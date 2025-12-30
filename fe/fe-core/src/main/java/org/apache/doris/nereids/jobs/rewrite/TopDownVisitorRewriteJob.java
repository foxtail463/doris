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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlanProcess;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/** TopDownVisitorRewriteJob */
public class TopDownVisitorRewriteJob implements RewriteJob {
    private final Rules rules;
    private final Predicate<Plan> isTraverseChildren;

    public TopDownVisitorRewriteJob(Rules rules, Predicate<Plan> isTraverseChildren) {
        this.rules = rules;
        this.isTraverseChildren = isTraverseChildren;
    }

    public Rules getRules() {
        return rules;
    }

    @Override
    public void execute(JobContext jobContext) {
        Plan originPlan = jobContext.getCascadesContext().getRewritePlan();
        Optional<Rules> relateRules = getRelatedRules(originPlan, rules, jobContext.getCascadesContext());
        if (!relateRules.isPresent()) {
            return;
        }

        Plan root = rewrite(
                null, -1, originPlan, jobContext, rules, false, new ProcessState(originPlan)
        );
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    /**
     * 自顶向下重写计划节点
     * 
     * 这是核心的重写方法，实现了自顶向下的重写策略：
     * 1. 首先对当前节点应用重写规则（doRewrite）
     * 2. 然后递归处理所有子节点
     * 3. 如果子节点发生变化，更新当前节点
     * 
     * 与 BottomUpVisitorRewriteJob 的区别：
     * - TopDown：先重写父节点，再重写子节点（适合需要父节点信息来指导子节点重写的规则）
     * - BottomUp：先重写子节点，再重写父节点（适合需要子节点信息来指导父节点重写的规则）
     * 
     * @param parent 父节点（用于更新父节点中的子节点引用）
     * @param childIndex 当前节点在父节点中的索引（-1 表示根节点）
     * @param plan 当前要重写的计划节点
     * @param jobContext 任务上下文
     * @param rules 重写规则集合
     * @param fastReturn 是否快速返回（当没有相关规则时，直接返回原计划，避免不必要的遍历）
     * @param processState 处理状态，用于跟踪计划变化（用于 EXPLAIN 展示）
     * @return 重写后的计划节点
     */
    private Plan rewrite(Plan parent, int childIndex, Plan plan, JobContext jobContext, Rules rules, boolean fastReturn,
            ProcessState processState) {
        // 快速返回优化：如果没有相关规则，直接返回原计划，避免不必要的遍历
        if (fastReturn && rules.getCurrentAndChildrenRules(plan).isEmpty()) {
            return plan;
        }
        // 检查是否应该遍历当前节点的子节点（某些节点类型可能不需要遍历子节点）
        if (!isTraverseChildren.test(plan)) {
            return plan;
        }

        // 检查超时（避免优化器运行时间过长）
        checkTimeout(jobContext);

        // 步骤1：自顶向下策略 - 先重写当前节点
        // 这里会尝试所有匹配的规则，直到找到一个能成功转换的规则（doRewrite 内部有循环）
        plan = doRewrite(parent, childIndex, plan, jobContext, rules, processState);

        // 步骤2：递归重写所有子节点
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean changed = false;
        for (int i = 0; i < plan.children().size(); i++) {
            Plan child = plan.children().get(i);
            // 递归重写子节点（fastReturn=true 表示子节点如果没有相关规则就直接返回）
            Plan newChild = rewrite(plan, i, child, jobContext, rules, true, processState);
            newChildren.add(newChild);
            // 检查子节点是否发生变化
            changed |= !newChild.deepEquals(child);
        }

        // 步骤3：如果子节点发生变化，更新当前节点
        if (changed) {
            // 用新的子节点列表重新构造当前节点
            plan = plan.withChildren(newChildren.build());
            // 更新 processState，用于 EXPLAIN 展示计划变化过程
            processState.updateChild(parent, childIndex, plan);
        }

        return plan;
    }

    /**
     * 对单个节点做“自顶向下”的规则尝试（可多轮，直到不再变化）
     *
     * 流程：
     * - 取出当前节点 applicable 的规则列表，并过滤禁用规则
     * - 按顺序尝试 transform，拿到第一个真正改写 plan 的结果就停止本轮
     * - 若本轮有改写（changed=true），以新 plan 继续下一轮，直到无规则能改写为止
     */
    private static Plan doRewrite(
            Plan originParent, int childIndex, Plan plan, JobContext jobContext,
            Rules rules, ProcessState processState) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        Plan originPlan = plan; // 当前正在尝试改写的 plan
        while (true) {
            // 取当前节点的规则 + 获取禁用规则 bitset
            List<Rule> currentRules = rules.getCurrentRules(originPlan);
            BitSet forbidRules = cascadesContext.getAndCacheDisableRules();

            boolean changed = false;
            for (Rule currentRule : currentRules) {
                // 跳过禁用规则或 pattern 不匹配的规则
                if (forbidRules.get(currentRule.getRuleType().ordinal())
                        || !currentRule.getPattern().matchPlanTree(originPlan)) {
                    continue;
                }
                // 尝试改写
                List<Plan> transform = currentRule.transform(originPlan, cascadesContext);
                if (!transform.isEmpty() && !transform.get(0).deepEquals(originPlan)) {
                    Plan newPlan = transform.get(0);
                    // 记录规则已接受旧 plan（供规则内部做统计/引用）
                    currentRule.acceptPlan(originPlan);
                    // 展示改写前后形状
                    if (cascadesContext.showPlanProcess()) {
                        String beforeShape = processState.getNewestPlan().treeString(true, originPlan);
                        String afterShape = processState.updateChildAndGetNewest(originParent, childIndex, newPlan)
                                .treeString(true, newPlan);
                        cascadesContext.addPlanProcess(
                                new PlanProcess(currentRule.getRuleType().name(), beforeShape, afterShape)
                        );
                    }
                    // 记录规则命中
                    cascadesContext.getStatementContext().ruleSetApplied(currentRule.getRuleType());
                    originPlan = newPlan; // 继续下一轮，以新 plan 作为输入
                    changed = true;
                    break; // 本轮找到一个改写就停，进入下一轮
                }
            }
            // 没有任何规则改写，收敛，返回最终 plan
            if (!changed) {
                return originPlan;
            }
        }
    }

    /** getRelateRules */
    public static Optional<Rules> getRelatedRules(Plan plan, Rules originRules, CascadesContext context) {
        List<Rule> validRules = originRules.filterValidRules(context);
        if (validRules.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(originRules);
    }
}
