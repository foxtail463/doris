// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.Collections;
import java.util.List;

/**
 * 在 Memo 中优化单个 {@link GroupExpression} 的任务。
 *
 * 角色：负责对一个 GroupExpression 依次应用“探索规则”和“实现规则”，
 *      生成等价逻辑计划与物理实现，再由后续 CostAndEnforcerJob 计算代价。
 */
public class OptimizeGroupExpressionJob extends Job {
    private final GroupExpression groupExpression;

    public OptimizeGroupExpressionJob(GroupExpression groupExpression, JobContext context) {
        super(JobType.OPTIMIZE_PLAN, context);
        this.groupExpression = groupExpression;
    }

    @Override
    public void execute() {
        // 无效（已被替换或删除）的表达式直接跳过，避免无用调度
        if (groupExpression.isUnused()) {
            return;
        }

        // 统计当前 GroupExpression 被调度的次数（用于诊断/限流）
        countJobExecutionTimesOfGroupExpressions(groupExpression);

        // 预先取出实现规则与探索规则
        List<Rule> implementationRules = getRuleSet().getImplementationRules();
        List<Rule> explorationRules = getExplorationRules(context.getCascadesContext());

        // 先跑探索规则（逻辑等价变换：Join 重排、谓词下推等）
        for (Rule rule : explorationRules) {
            // 规则被禁用或不适用则跳过
            if (rule.isInvalid(disableRules, groupExpression)) {
                continue;
            }
            // 每条规则作为一个 ApplyRuleJob 入队，由调度器执行
            pushJob(new ApplyRuleJob(groupExpression, rule, context));
        }

        // 再跑实现规则（逻辑 -> 物理，如 LogicalJoin -> PhysicalHashJoin）
        for (Rule rule : implementationRules) {
            if (rule.isInvalid(disableRules, groupExpression)) {
                continue;
            }
            pushJob(new ApplyRuleJob(groupExpression, rule, context));
        }
    }

    private List<Rule> getExplorationRules(CascadesContext cascadesContext) {
        // 默认添加 Join 类探索规则
        Builder<Rule> ruleBuilder = ImmutableList.<Rule>builder().addAll(getJoinRules());

        // MV Hook 不存在或已做过预重写，则仅返回 Join 规则
        // 如果 isPreMvRewritten() == true，说明 preMaterializedViewRewrite() 已经执行过，
        // MV 规则已经在 Pre-MV Rewrite 阶段应用过了，这里不再添加 MV 规则
        if (!MaterializedViewUtils.containMaterializedViewHook(cascadesContext.getStatementContext())
                || cascadesContext.getStatementContext().isPreMvRewritten()) {
            return ruleBuilder.build();
        }
        // 根据是否需要 Pre-MV Rewrite，选择不同的 MV 规则集合
        // 
        // 关键场景：isNeedPreMvRewrite() == true 但 isPreMvRewritten() == false
        // 这种情况发生在 Pre-MV Rewrite 阶段的独立 CascadesContext 中：
        // 1. preMaterializedViewRewrite() 执行时，会调用 PreMaterializedViewRewriter.rewrite()
        // 2. PreMaterializedViewRewriter.rewrite() 内部会创建独立的 CascadesContext（共享同一个 StatementContext）
        // 3. 在这个独立的 CascadesContext 中调用 Optimizer.execute() 时，会创建 OptimizeGroupExpressionJob
        // 4. 此时：
        //    - isNeedPreMvRewrite() == true（从主查询的 StatementContext 继承）
        //    - isPreMvRewritten() == false（因为主查询的 preMaterializedViewRewrite() 还没执行完，还没设置 setPreMvRewritten(true)）
        // 5. 因此会添加 MATERIALIZED_VIEW_IN_RBO_RULES，在 Pre-MV Rewrite 阶段应用
        // 
        // 执行流程总结：
        // - 如果 isNeedPreMvRewrite() == true：
        //   添加 MATERIALIZED_VIEW_IN_RBO_RULES（在 Pre-MV Rewrite 阶段的独立 CascadesContext 中应用）
        //   主查询的 CBO 阶段不会执行到这里（因为 isPreMvRewritten() 已经是 true）
        // - 如果 isNeedPreMvRewrite() == false：
        //   添加 MATERIALIZED_VIEW_IN_CBO_RULES（在主查询的 CBO 阶段应用）
        if (cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
            ruleBuilder.addAll(RuleSet.MATERIALIZED_VIEW_IN_RBO_RULES);
        } else {
            ruleBuilder.addAll(RuleSet.MATERIALIZED_VIEW_IN_CBO_RULES);
        }
        return ruleBuilder.build();
    }

    private List<Rule> getJoinRules() {
        // 是否禁用 Join 重排：用户配置、LEADING hint、生长过大的 Memo
        boolean isDisableJoinReorder = context.getCascadesContext().getConnectContext().getSessionVariable()
                .isDisableJoinReorder()
                || context.getCascadesContext().isLeadingDisableJoinReorder()
                || context.getCascadesContext().getMemo().getGroupExpressionsSize() > context.getCascadesContext()
                .getConnectContext().getSessionVariable().memoMaxGroupExpressionSize;

        // 是否使用 DPHyp（超图 DP）做 Join 顺序
        boolean isDpHyp = context.getCascadesContext().getStatementContext().isDpHyp();

        // 是否允许 Bushy 树（左右子树都可继续分裂）或 ZigZag 树（限制形状）
        boolean isEnableBushyTree = context.getCascadesContext().getConnectContext().getSessionVariable()
                .isEnableBushyTree();
        boolean isLeftZigZagTree = context.getCascadesContext().getConnectContext()
                .getSessionVariable().isEnableLeftZigZag()
                || (groupExpression.getOwnerGroup() != null && !groupExpression.getOwnerGroup().isStatsReliable());
        int joinNumBushyTree = context.getCascadesContext().getConnectContext()
                .getSessionVariable().getMaxJoinNumBushyTree();

        // 按优先级选择 Join 重排策略
        if (isDisableJoinReorder) {
            // 完全不做 Join 重排
            return Collections.emptyList();
        } else if (isDpHyp) {
            // DPHyp 高效枚举 Join 顺序
            return getRuleSet().getDPHypReorderRules();
        } else if (isLeftZigZagTree) {
            // 左 ZigZag 形状（偏左深）
            return getRuleSet().getLeftZigZagTreeJoinReorder();
        } else if (isEnableBushyTree) {
            // 允许 Bushy 树
            return getRuleSet().getBushyTreeJoinReorder();
        } else if (context.getCascadesContext().getStatementContext().getMaxNAryInnerJoin() <= joinNumBushyTree) {
            // 大型 N-ary Join 时也放宽为 Bushy
            return getRuleSet().getBushyTreeJoinReorder();
        } else {
            // 默认 ZigZag（限制形状，搜索空间更小）
            return getRuleSet().getZigZagTreeJoinReorder();
        }
    }
}
