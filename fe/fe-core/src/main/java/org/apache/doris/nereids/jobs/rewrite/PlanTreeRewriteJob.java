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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/** PlanTreeRewriteJob */
public abstract class PlanTreeRewriteJob extends Job {
    protected final Predicate<Plan> isTraverseChildren;

    public PlanTreeRewriteJob(JobType type, JobContext context, Predicate<Plan> isTraverseChildren) {
        super(type, context);
        this.isTraverseChildren = Objects.requireNonNull(isTraverseChildren, "isTraverseChildren can not be null");
    }

    protected final RewriteResult rewrite(Plan plan, Rules rules, RewriteJobContext rewriteJobContext) {
        CascadesContext cascadesContext = context.getCascadesContext();
        // 标记本次重写是否作用于根节点（影响日志/展示）
        cascadesContext.setIsRewriteRoot(rewriteJobContext.isRewriteRoot());

        boolean showPlanProcess = cascadesContext.showPlanProcess();
        // 遍历当前节点可用的规则（按优先级顺序）
        for (Rule rule : rules.getCurrentRules(plan)) {
            // 规则被禁用则跳过
            if (disableRules.get(rule.getRuleType().type())) {
                continue;
            }
            Pattern<Plan> pattern = (Pattern<Plan>) rule.getPattern();
            // 先匹配 pattern，避免无谓 transform
            if (pattern.matchPlanTree(plan)) {
                List<Plan> newPlans = rule.transform(plan, cascadesContext);
                // rewrite 规则约定必须产生且只产生 1 个新 plan
                if (newPlans.size() != 1) {
                    throw new AnalysisException("Rewrite rule should generate one plan: " + rule.getRuleType());
                }
                Plan newPlan = newPlans.get(0);
                // 无变化则继续尝试下一条规则
                if (!newPlan.deepEquals(plan)) {
                    // 记录 trace（用于 minidump / 调试）
                    NereidsTracer.logRewriteEvent(rule.toString(), pattern, plan, newPlan);
                    String traceBefore = null;
                    if (showPlanProcess) {
                        // 展示前后的形状，用于 explain 过程展示
                        traceBefore = getCurrentPlanTreeString(rewriteJobContext, null);
                    }
                    // 写回重写结果并打上“已改写”标记
                    rewriteJobContext.result = newPlan;
                    context.setRewritten(true);
                    rule.acceptPlan(newPlan);
                    if (showPlanProcess) {
                        String traceAfter = getCurrentPlanTreeString(rewriteJobContext, newPlan);
                        PlanProcess planProcess = new PlanProcess(rule.getRuleType().name(), traceBefore, traceAfter);
                        cascadesContext.addPlanProcess(planProcess);
                    }
                    // 成功改写后记录被应用的规则类型（用于统计/控制）
                    context.getCascadesContext().getStatementContext().ruleSetApplied(rule.getRuleType());
                    return new RewriteResult(true, newPlan);
                }
            }
        }
        // 没有规则命中或未改写成功，返回原 plan
        return new RewriteResult(false, plan);
    }

    /**
     * 根据子节点的重写结果，重新拼装当前 plan 的 children。
     *
     * - 输入：
     *   - {@code plan}：当前要处理的父节点；
     *   - {@code childrenContext}：与 {@code plan.children()} 一一对应的子节点重写上下文数组，
     *     每个元素中可能带有该子节点的重写结果 {@code result}。
     *
     * - 输出：
     *   - 如果所有子节点都没变，直接返回原始 {@code plan}；
     *   - 如果有任意子节点被重写，则用新的子节点列表调用 {@code plan.withChildren(...)} 构造一个新的父节点。
     */
    protected static Plan linkChildren(Plan plan, RewriteJobContext[] childrenContext) {
        List<Plan> children = plan.children();
        // 针对 children.size() 不同的情况做简单的 loop unrolling 优化，
        // 避免在最常见的 0/1/2 子节点场景下创建多余的集合对象。
        switch (children.size()) {
            case 0: {
                // 没有子节点，直接返回原 plan。
                return plan;
            }
            case 1: {
                // 只有一个子节点：看该子节点是否有重写结果，如果没有或结果等于原节点，则不变；
                // 否则，用新的 child 构造一个带单子节点的 plan。
                RewriteJobContext child = childrenContext[0];
                Plan firstResult = child == null ? plan.child(0) : child.result;
                return firstResult == null || firstResult == children.get(0)
                        ? plan : plan.withChildren(ImmutableList.of(firstResult));
            }
            case 2: {
                // 有两个子节点：分别取出左右子节点的重写结果，与原节点做对比。
                RewriteJobContext left = childrenContext[0];
                Plan firstResult = left == null ? plan.child(0) : left.result;
                RewriteJobContext right = childrenContext[1];
                Plan secondResult = right == null ? plan.child(1) : right.result;
                Plan firstOrigin = children.get(0);
                Plan secondOrigin = children.get(1);
                boolean firstChanged = firstResult != null && firstResult != firstOrigin;
                boolean secondChanged = secondResult != null && secondResult != secondOrigin;
                if (firstChanged || secondChanged) {
                    // 至少有一个子节点发生变化，重新构造 children 列表。
                    ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(2);
                    newChildren.add(firstChanged ? firstResult : firstOrigin);
                    newChildren.add(secondChanged ? secondResult : secondOrigin);
                    return plan.withChildren(newChildren.build());
                } else {
                    // 两个子节点都没变，直接返回原 plan。
                    return plan;
                }
            }
            default: {
                // 子节点数大于 2 的通用情况。
                boolean anyChanged = false;
                int i = 0;
                Plan[] newChildren = new Plan[childrenContext.length];
                for (Plan oldChild : children) {
                    // 对于第 i 个子节点，取出其重写结果（可能为 null），判断是否发生变化。
                    Plan result = childrenContext[i].result;
                    boolean changed = result != null && result != oldChild;
                    newChildren[i] = changed ? result : oldChild;
                    anyChanged |= changed;
                    i++;
                }
                // 只有当至少一个子节点发生变化时，才调用 withChildren 构造新 plan；
                // 否则直接返回原 plan，避免创建不必要的新对象。
                return anyChanged ? plan.withChildren(newChildren) : plan;
            }
        }
    }

    private String getCurrentPlanTreeString(RewriteJobContext rewriteJobContext, Plan rewrittenPlan) {
        Plan newestPlan = context.getCascadesContext()
                .getCurrentRootRewriteJobContext().get()
                .getNewestPlan();
        return newestPlan.treeString(true, rewrittenPlan == null ? rewriteJobContext.tmpPlan : rewrittenPlan);
    }

    static class RewriteResult {
        final boolean hasNewPlan;
        final Plan plan;

        public RewriteResult(boolean hasNewPlan, Plan plan) {
            this.hasNewPlan = hasNewPlan;
            this.plan = plan;
        }
    }
}
