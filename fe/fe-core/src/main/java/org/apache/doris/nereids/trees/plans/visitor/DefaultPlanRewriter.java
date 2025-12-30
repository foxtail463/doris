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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;

import com.google.common.collect.ImmutableList;

/**
 * Default implementation for plan rewriting, delegating to child plans and rewrite current root
 * when any one of its children changed.
 */
public abstract class DefaultPlanRewriter<C> extends PlanVisitor<Plan, C> {

    @Override
    public Plan visit(Plan plan, C context) {
        return visitChildren(this, plan, context);
    }

    @Override
    public Plan visitPhysicalStorageLayerAggregate(PhysicalStorageLayerAggregate storageLayerAggregate, C context) {
        if (storageLayerAggregate.getRelation() instanceof PhysicalOlapScan) {
            PhysicalOlapScan olapScan = (PhysicalOlapScan) storageLayerAggregate.getRelation().accept(this, context);
            if (olapScan != storageLayerAggregate.getRelation()) {
                return storageLayerAggregate.withPhysicalOlapScan(olapScan);
            }
        }
        return storageLayerAggregate;
    }

    /**
     * 递归访问计划节点的所有子节点，并根据子节点的变化决定是否重建当前节点
     * 
     * 这是访问者模式中的核心辅助方法，主要功能：
     * 1. 遍历当前节点的所有子节点
     * 2. 对每个子节点应用重写器（rewriter）
     * 3. 收集重写后的子节点
     * 4. 如果有子节点发生变化，创建新的父节点替换旧的
     * 5. 对于物理计划，保留统计信息和组ID
     * 
     * 为什么需要这个方法：
     * - 递归遍历：实现树形结构的递归访问
     * - 不可变性：Nereids的计划树是不可变的，需要创建新节点
     * - 性能优化：只有当子节点确实变化时才创建新父节点
     * - 统计信息保留：物理计划的统计信息需要在重写过程中保留
     * 
     * 执行流程示例：
     * 输入：Filter(a > 1) -> Project(a, b) -> Scan(t)
     * 步骤1：访问Filter的子节点（Project）
     *   调用 Project.accept(rewriter)
     *   -> Project访问自己的子节点（Scan）
     *     -> Scan.accept(rewriter) 返回 Scan'（可能被优化）
     *   -> Project使用Scan'创建新的Project'
     *   返回 Project'
     * 步骤2：如果Project' != Project，创建新的Filter'
     *   使用Filter.withChildren(Project')
     * 步骤3：如果Filter是物理计划，保留统计信息和组ID
     *   调用 copyStatsAndGroupIdFrom(原始Filter)
     * 输出：Filter' -> Project' -> Scan'
     * 
     * 为什么需要 copyStatsAndGroupIdFrom：
     * - 统计信息（Statistics）：物理计划的统计信息（如行数、基数）是在优化过程中计算出来的。
     *   当子节点变化时，只是结构变化，统计信息不应该丢失。
     *   例如：PhysicalFilter的子节点被替换，但Filter的统计信息应该保持
     * - 组ID（Group ID）：在Cascades优化器中，相同逻辑的等价计划被组织在同一个Group中。
     *   GroupExpression存储了计划与Group的关联关系。
     *   当计划被重写时，需要保留这个关联关系，以便Memo能够正确管理等价计划
     * 
     * 性能考虑：
     * - 使用引用相等判断（newChild != child）快速检测变化，避免深度比较
     * - 只有当子节点确实变化时才创建新父节点，避免不必要的对象创建
     * - 预分配ImmutableList.Builder的大小（plan.arity()）避免扩容
     * 
     * 使用场景：
     * - 常量传播（ConstantPropagation）：visitLogicalFilter中调用，递归优化子节点
     * - 谓词下推（PredicatePushdown）：递归访问子节点，收集和下推谓词
     * - 任何需要遍历和重写子节点的优化规则
     * 
     * @param <P> 计划节点的类型（必须是Plan的子类）
     * @param <C> 上下文类型
     * @param rewriter 计划重写器（访问者）
     * @param plan 要访问的计划节点
     * @param context 访问上下文
     * @return 如果子节点有变化，返回新的计划节点；否则返回原计划节点
     */
    public static <P extends Plan, C> P visitChildren(DefaultPlanRewriter<C> rewriter, P plan, C context) {
        // 步骤1：预分配子节点列表的容量（优化内存分配）
        // plan.arity() 返回计划节点的子节点数量
        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        
        // 步骤2：标志是否有子节点发生变化
        // 使用简单的引用比较，避免深度相等比较的开销
        boolean hasNewChildren = false;
        
        // 步骤3：遍历所有子节点，递归应用重写器
        for (Plan child : plan.children()) {
            // 递归调用：child.accept(rewriter, context)
            // 这会触发双分派机制，根据child的实际类型和rewriter的类型调用相应方法
            Plan newChild = child.accept(rewriter, context);
            
            // 检测子节点是否变化（使用引用比较，性能最优）
            if (newChild != child) {
                hasNewChildren = true;
            }
            
            // 收集重写后的子节点（无论是否变化）
            newChildren.add(newChild);
        }

        // 步骤4：如果有子节点变化，创建新的父节点
        if (hasNewChildren) {
            // 保存原始计划节点的引用（用于后续复制统计信息）
            P originPlan = plan;
            
            // 使用新的子节点列表创建新的计划节点
            // withChildren 会创建一个新的计划实例，保持不可变性
            plan = (P) plan.withChildren(newChildren.build());
            
            // 步骤5：特殊处理物理计划 - 保留统计信息和组ID
            // 为什么需要保留？
            //   - 统计信息（Statistics）：子节点变化不应该丢失当前节点的统计信息
            //   - 组ID（Group ID）：保持与Memo中Group的关联关系
            // 例如：
            //   PhysicalFilter(stats: 1000 rows) -> PhysicalProject -> PhysicalScan
            //   如果PhysicalProject被优化，PhysicalFilter应该保持其统计信息
            if (originPlan instanceof AbstractPhysicalPlan) {
                plan = (P) ((AbstractPhysicalPlan) plan).copyStatsAndGroupIdFrom((AbstractPhysicalPlan) originPlan);
            }
        }
        
        // 步骤6：返回结果（如果有变化则返回新节点，否则返回原节点）
        return plan;
    }
}
