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
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteBottomUpJob.RewriteState;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * 自底向上访问者重写任务
 * 
 * 该类实现了基于访问者模式的自底向上计划树重写任务。
 * 它会从叶子节点开始，逐层向上遍历计划树，对每个节点应用相应的重写规则。
 * 适用于浅层计划树的重写优化，避免了深层递归可能导致的栈溢出问题。
 */
public class BottomUpVisitorRewriteJob implements RewriteJob {
    /** 
     * 批次ID生成器，用于跟踪重写批次
     * 
     * 作用：
     * 1. **批次隔离**：每次执行 execute() 方法时，都会通过 incrementAndGet() 生成一个新的唯一批次ID
     * 2. **状态重置**：在重写过程中，每个 Plan 节点会记录自己的重写状态（REWRITTEN/REWRITE_THIS等）和对应的批次ID
     *    当 getState(plan, currentBatchId) 发现节点上的状态来自上一个批次时，会重置状态，让节点重新开始重写流程
     * 3. **避免重复重写**：在同一批次中，如果节点已经被标记为 REWRITTEN，就不会再次重写，提高效率
     * 
     * 为什么需要批次ID？
     * - Plan 节点可能会被多个规则共享（通过 withChildren 创建新节点时，子节点可能被复用）
     * - 如果没有批次ID，一个节点在之前的重写批次中被标记为 REWRITTEN 后，在新的批次中可能不会被重新考虑
     * - 有了批次ID，每个新的重写批次都会重新检查所有节点，确保规则能够正确应用到新的计划树结构上
     * 
     * 示例：
     * - 批次1：节点A被规则R1重写，标记为 REWRITTEN(batchId=1)
     * - 批次2：节点A的父节点被规则R2重写，创建了新的节点A'（但可能复用A作为子节点）
     *          此时 getState(A, batchId=2) 发现 A 的状态是 batchId=1，会重置状态，让 A 在新批次中重新考虑重写
     */
    private static AtomicInteger batchId = new AtomicInteger(0);
    
    /** 重写规则集合 */
    private final Rules rules;
    
    /** 遍历子节点的谓词，用于控制是否遍历特定类型节点的子节点 */
    private final Predicate<Plan> isTraverseChildren;

    /**
     * 构造函数
     * 
     * @param rules 重写规则集合
     * @param isTraverseChildren 遍历子节点的谓词，用于控制是否遍历特定类型节点的子节点
     */
    public BottomUpVisitorRewriteJob(Rules rules, Predicate<Plan> isTraverseChildren) {
        this.rules = rules;
        this.isTraverseChildren = isTraverseChildren;
    }

    /**
     * 获取重写规则集合
     * 
     * @return 重写规则集合
     */
    public Rules getRules() {
        return rules;
    }

    /**
     * 执行自底向上重写任务
     * 
     * 从根节点开始，自底向上遍历整个计划树，对每个节点应用相应的重写规则。
     * 重写过程会持续进行，直到没有更多的规则可以应用为止。
     * 
     * @param jobContext 任务上下文，包含级联上下文等信息
     */
    @Override
    public void execute(JobContext jobContext) {
        // 获取原始计划树
        Plan originPlan = jobContext.getCascadesContext().getRewritePlan();
        
        // 获取与当前计划相关的规则
        Optional<Rules> relateRules
                = TopDownVisitorRewriteJob.getRelatedRules(originPlan, rules, jobContext.getCascadesContext());
        
        // 如果没有相关规则，直接返回
        if (!relateRules.isPresent()) {
            return;
        }

        // 执行自底向上重写，生成新的批次ID
        Plan root = rewrite(
                null, -1, originPlan, jobContext, rules, batchId.incrementAndGet(), false, new ProcessState(originPlan)
        );
        
        // 将重写后的计划设置回级联上下文
        jobContext.getCascadesContext().setRewritePlan(root);
    }

    /**
     * 判断是否为一次性任务
     * 
     * @return false，表示这是一个持续性的重写任务，会重复执行直到收敛
     */
    @Override
    public boolean isOnce() {
        return false;
    }

    /**
     * 自底向上重写计划节点
     * 
     * 这是核心的重写方法，实现了自底向上的重写策略：
     * 1. 首先递归处理所有子节点
     * 2. 然后对当前节点应用重写规则
     * 3. 如果节点发生变化，继续重写直到收敛
     * 
     * @param parent 父节点
     * @param childIndex 当前节点在父节点中的索引
     * @param plan 当前要重写的计划节点
     * @param jobContext 任务上下文
     * @param rules 重写规则集合
     * @param batchId 批次ID，用于跟踪重写状态
     * @param fastReturn 是否快速返回（当没有相关规则时）
     * @param processState 处理状态，用于跟踪计划变化
     * @return 重写后的计划节点
     */
    private Plan rewrite(
            Plan parent, int childIndex, Plan plan, JobContext jobContext, Rules rules,
            int batchId, boolean fastReturn, ProcessState processState) {
        
        // 检查当前节点是否已经在当前批次中被重写过
        RewriteState state = PlanTreeRewriteBottomUpJob.getState(plan, batchId);
        if (state == RewriteState.REWRITTEN) {
            return plan; // 已经重写过，直接返回
        }
        
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        boolean showPlanProcess = cascadesContext.showPlanProcess();

        Plan currentPlan = plan;

        // 持续重写循环，直到没有更多规则可以应用
        while (true) {
            // 快速返回：如果没有相关规则，直接返回当前计划
            if (fastReturn && rules.getCurrentAndChildrenRules(currentPlan).isEmpty()) {
                return currentPlan;
            }
            
            // 检查是否应该遍历当前节点的子节点
            if (!isTraverseChildren.test(currentPlan)) {
                return currentPlan; // 跳过子节点遍历
            }

            // 检查超时
            checkTimeout(jobContext);

            // 步骤1：自底向上重写所有子节点
            ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(currentPlan.arity());
            boolean changed = false;

            for (int i = 0; i < currentPlan.children().size(); i++) {
                Plan child = currentPlan.children().get(i);
                // 递归重写子节点
                Plan rewrite = rewrite(currentPlan, i, child, jobContext, rules, batchId, true, processState);
                newChildren.add(rewrite);
                changed |= !rewrite.deepEquals(child); // 检查子节点是否发生变化
            }
            
            // 如果子节点发生变化，更新当前节点
            if (changed) {
                currentPlan = currentPlan.withChildren(newChildren.build());
                if (showPlanProcess) {
                    parent = processState.updateChild(parent, childIndex, currentPlan);
                }
            }
            
            // 步骤2：对当前节点应用重写规则
            Plan rewrittenPlan = doRewrite(parent, childIndex, currentPlan, jobContext, rules, processState);
            
            if (!rewrittenPlan.deepEquals(currentPlan)) {
                // 当前节点发生变化，继续重写循环
                currentPlan = rewrittenPlan;
                if (showPlanProcess) {
                    parent = processState.updateChild(parent, childIndex, currentPlan);
                }
            } else {
                // 当前节点没有变化，标记为重写完成并返回
                PlanTreeRewriteBottomUpJob.setState(rewrittenPlan, RewriteState.REWRITTEN, batchId);
                return rewrittenPlan;
            }
        }
    }

    /**
     * 对单个计划节点执行重写规则
     * 
     * 遍历所有适用于当前节点的重写规则，找到第一个能够成功转换的规则并应用。
     * 如果多个规则都匹配，只应用第一个成功的规则。
     * 
     * @param originParent 原始父节点
     * @param childIndex 当前节点在父节点中的索引
     * @param plan 要重写的计划节点
     * @param jobContext 任务上下文
     * @param rules 重写规则集合
     * @param processState 处理状态
     * @return 重写后的计划节点，如果没有规则匹配则返回原节点
     */
    private static Plan doRewrite(
            Plan originParent, int childIndex, Plan plan, JobContext jobContext,
            Rules rules, ProcessState processState) {
        
        // 获取适用于当前节点的规则列表
        List<Rule> currentRules = rules.getCurrentRules(plan);
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        
        // 获取被禁用的规则集合
        BitSet forbidRules = cascadesContext.getAndCacheDisableRules();
        
        // 遍历所有规则，找到第一个可以应用的规则
        for (Rule currentRule : currentRules) {
            // 跳过被禁用的规则或不匹配模式的规则
            if (forbidRules.get(currentRule.getRuleType().ordinal()) 
                || !currentRule.getPattern().matchPlanTree(plan)) {
                continue;
            }
            
            // 尝试应用规则进行转换
            List<Plan> transform = currentRule.transform(plan, cascadesContext);
            
            // 检查转换是否成功（有结果且与原计划不同）
            if (!transform.isEmpty() && !transform.get(0).deepEquals(plan)) {
                Plan result = transform.get(0);
                
                // 通知规则已接受新的计划
                currentRule.acceptPlan(result);

                // 如果启用了计划过程显示，记录转换前后的计划形状
                if (cascadesContext.showPlanProcess()) {
                    String beforeShape = processState.getNewestPlan().treeString(true, plan);
                    String afterShape = processState.updateChildAndGetNewest(originParent, childIndex, result)
                            .treeString(true, result);
                    cascadesContext.addPlanProcess(
                        new PlanProcess(currentRule.getRuleType().name(), beforeShape, afterShape)
                    );
                }
                
                // 记录成功应用的规则类型
                cascadesContext.getStatementContext().ruleSetApplied(currentRule.getRuleType());
                
                return result; // 返回转换后的计划
            }
        }
        
        // 没有规则匹配，返回原计划
        return plan;
    }
}
