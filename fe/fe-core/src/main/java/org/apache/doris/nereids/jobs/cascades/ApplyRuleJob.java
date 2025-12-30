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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.TransformEvent;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;

/**
 * Job to apply rule on {@link GroupExpression}.
 */
public class ApplyRuleJob extends Job {
    private static final EventProducer APPLY_RULE_TRACER = new EventProducer(TransformEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(TransformEvent.class, EventChannel.LOG)));
    private final GroupExpression groupExpression;
    private final Rule rule;

    /**
     * Constructor of ApplyRuleJob.
     *
     * @param groupExpression apply rule on this {@link GroupExpression}
     * @param rule rule to be applied
     * @param context context of current job
     */
    public ApplyRuleJob(GroupExpression groupExpression, Rule rule, JobContext context) {
        super(JobType.APPLY_RULE, context);
        this.groupExpression = groupExpression;
        this.rule = rule;
        super.cteIdToStats = new HashMap<>();
    }

    @Override
    public final void execute() throws AnalysisException {
        // 同一规则已作用过，或表达式已无效，直接返回
        if (groupExpression.hasApplied(rule)
                || groupExpression.isUnused()) {
            return;
        }
        // 记录调度次数（用于诊断/限流）
        countJobExecutionTimesOfGroupExpressions(groupExpression);

        // 延迟推导统计：先收集，最后统一入队，避免合并 Group 后遗漏
        List<DeriveStatsJob> deriveStatsJobs = Lists.newArrayList();

        // 按规则的 pattern 在当前 GroupExpression 上做匹配，可能得到多棵匹配树
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(rule.getPattern(), groupExpression);
        for (Plan plan : groupExpressionMatching) {
            // 规则是探索类且 Memo 体量已达上限，提前停止，防止爆炸
            if (rule.isExploration()
                    && context.getCascadesContext().getMemo().getGroupExpressionsSize() > context.getCascadesContext()
                    .getConnectContext().getSessionVariable().memoMaxGroupExpressionSize) {
                break;
            }
            // 应用规则，生成新计划（可能 0..n 个）
            List<Plan> newPlans = rule.transform(plan, context.getCascadesContext());
            for (Plan newPlan : newPlans) {
                // 没有变化就跳过（避免自环）
                if (newPlan == plan) {
                    continue;
                }
                // 将新计划插入 Memo（可能复用已有表达式或生成新表达式）
                CopyInResult result = context.getCascadesContext()
                        .getMemo()
                        .copyIn(newPlan, groupExpression.getOwnerGroup(), false);
                // 若未产生新表达式（完全等价已存在），跳过
                if (!result.generateNewExpression) {
                    continue;
                }
                GroupExpression newGroupExpression = result.correspondingExpression;
                newGroupExpression.setFromRule(rule);

                // 逻辑计划：继续做逻辑/实现优化；必要时推导统计
                if (newPlan instanceof LogicalPlan) {
                    pushJob(new OptimizeGroupExpressionJob(newGroupExpression, context));
                    // Join 交换（commute）不改孩子与运算符，统计不变，无需推导
                    if (!rule.getRuleType().equals(RuleType.LOGICAL_JOIN_COMMUTE)) {
                        deriveStatsJobs.add(new DeriveStatsJob(newGroupExpression, context));
                    } else {
                        // The Join Commute rule preserves the operator's expression and children,
                        // thereby not altering the statistics. Hence, there is no need to derive statistics for it.
                        newGroupExpression.setStatDerived(true);
                    }
                // 物理计划：计算代价/Enforcer；若生成新子 Group 需补充统计
                } else {
                    pushJob(new CostAndEnforcerJob(newGroupExpression, context));
                    if (newGroupExpression.children().stream().anyMatch(g -> g.getLogicalExpressions().isEmpty())) {
                        // If a rule creates a new group when generating a physical plan,
                        // then we need to derive statistics for it, e.g., logicalTopToPhysicalTopN rule:
                        // logicalTopN ==> GlobalPhysicalTopN
                        //                   -> localPhysicalTopN
                        // These implementation rules integrate rules for plan shape transformation.
                        deriveStatsJobs.add(new DeriveStatsJob(newGroupExpression, context));
                    } else {
                        newGroupExpression.setStatDerived(true);
                    }
                }

                // 记录规则应用事件（trace/日志）
                NereidsTracer.logApplyRuleEvent(rule.toString(), plan, newGroupExpression.getPlan());
                APPLY_RULE_TRACER.log(TransformEvent.of(groupExpression, plan, newPlans, rule.getRuleType()),
                        rule::isRewrite);
            }
            // we do derive stats job eager to avoid un derive stats due to merge group and optimize group
            // consider:
            //   we have two groups burned by order: G1 and G2
            //   then we have job by order derive G2, optimize group expression in G2,
            //     derive G1, optimize group expression in G1
            //   if G1 merged into G2, then we maybe generated job optimize group G2 before derive G1
            //   in this case, we will do get stats from G1's child before derive G1's child stats
            //   then we will meet NPE in CostModel.
            // 立刻把需要的推导统计任务入队，防止 Group 合并导致遗漏或顺序错乱
            for (DeriveStatsJob deriveStatsJob : deriveStatsJobs) {
                pushJob(deriveStatsJob);
            }
        }
        // 标记该规则已作用于当前 GroupExpression，避免重复
        groupExpression.setApplied(rule);
    }
}
