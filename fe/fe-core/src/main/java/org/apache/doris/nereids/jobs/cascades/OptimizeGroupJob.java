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

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.metrics.CounterType;
import org.apache.doris.nereids.metrics.event.CounterEvent;

import java.util.List;

/**
 * Job to optimize {@link Group} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class OptimizeGroupJob extends Job {
    private final Group group;

    public OptimizeGroupJob(Group group, JobContext context) {
        super(JobType.OPTIMIZE_PLAN_SET, context);
        this.group = group;
    }

    /**
     * 执行 Group 的优化任务
     * 
     * 这是 Cascades 优化器的核心方法之一，负责优化一个 Group（等价计划组）。
     * 
     * 执行流程：
     * 1. 快速检查：如果已有满足要求的计划，直接返回（避免重复优化）
     * 2. 探索阶段（如果未探索）：为所有逻辑表达式创建优化任务，探索等价计划
     * 3. 实现阶段：为所有物理表达式创建代价计算任务，计算代价并添加 Enforcer
     * 4. 标记为已探索：避免重复探索同一个 Group
     * 
     * 优化策略：
     * - 逻辑表达式优化：通过 OptimizeGroupExpressionJob 应用探索规则和实现规则
     *   - 探索规则：生成等价的逻辑计划（如 Join 重排序、谓词下推）
     *   - 实现规则：将逻辑计划转换为物理计划（如 LogicalJoin -> PhysicalHashJoin）
     * - 物理表达式优化：通过 CostAndEnforcerJob 计算代价并满足物理属性要求
     *   - 计算代价：基于统计信息和代价模型
     *   - 添加 Enforcer：如果输出属性不满足要求，添加 Enforcer（如 PhysicalDistribute、PhysicalQuickSort）
     * 
     * 遍历顺序：从后往前遍历（i--），这样可以优先处理新添加的表达式
     */
    @Override
    public void execute() {
        // 记录性能追踪：记录 Job 执行事件，用于性能分析和调试
        COUNTER_TRACER.log(CounterEvent.of(Memo.getStateId(), CounterType.JOB_EXECUTION, group, null, null));
        
        // 步骤1：快速检查 - 如果已有满足要求的计划，直接返回
        // getLowestCostPlan 返回满足 requiredProperties 的最低代价计划
        // 如果存在，说明该 Group 已经优化过，无需重复优化
        // 这是 Cascades 优化器的剪枝优化，避免重复计算
        if (group.getLowestCostPlan(context.getRequiredProperties()).isPresent()) {
            return;
        }
        
        // 步骤2：探索阶段 - 如果 Group 还未被探索，优化所有逻辑表达式
        // isExplored 标记该 Group 是否已经被探索过
        // 探索阶段的目标是：
        // - 应用探索规则（exploration rules）生成等价的逻辑计划
        // - 应用实现规则（implementation rules）将逻辑计划转换为物理计划
        // 
        // 例如：
        // - LogicalJoin(A, B) 可能通过探索规则生成 LogicalJoin(B, A)（Join 重排序）
        // - LogicalJoin 可能通过实现规则生成 PhysicalHashJoin、PhysicalSortMergeJoin 等
        if (!group.isExplored()) {
            List<GroupExpression> logicalExpressions = group.getLogicalExpressions();
            // 从后往前遍历，优先处理新添加的表达式
            for (int i = logicalExpressions.size() - 1; i >= 0; i--) {
                // 为每个逻辑表达式创建优化任务
                // OptimizeGroupExpressionJob 会：
                // - 应用探索规则，生成等价的逻辑计划
                // - 应用实现规则，生成物理计划
                context.getCascadesContext().pushJob(
                        new OptimizeGroupExpressionJob(logicalExpressions.get(i), context));
            }
        }

        // 步骤3：实现阶段 - 为所有物理表达式计算代价并添加 Enforcer
        // 物理表达式是已经转换为物理操作符的计划（如 PhysicalHashJoin、PhysicalOlapScan）
        // CostAndEnforcerJob 会：
        // - 计算物理计划的代价（基于统计信息和代价模型）
        // - 检查输出属性是否满足要求（如分布、排序）
        // - 如果不满足，添加 Enforcer（如 PhysicalDistribute、PhysicalQuickSort）
        // - 记录最低代价计划到 Group 的 lowestCostPlans 中
        List<GroupExpression> physicalExpressions = group.getPhysicalExpressions();
        // 从后往前遍历，优先处理新添加的表达式
        for (int i = physicalExpressions.size() - 1; i >= 0; i--) {
            // 为每个物理表达式创建代价计算和 Enforcer 任务
            context.getCascadesContext().pushJob(new CostAndEnforcerJob(physicalExpressions.get(i), context));
        }
        
        // 步骤4：标记 Group 为已探索
        // 避免重复探索同一个 Group，提高优化效率
        // 注意：即使探索阶段被跳过（已有满足要求的计划），这里也会标记为已探索
        group.setExplored(true);
    }
}
