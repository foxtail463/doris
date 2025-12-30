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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import java.util.Objects;

/**
 * Cascades style optimize:
 * Perform equivalent logical plan exploration and physical implementation enumeration,
 * try to find best plan under the guidance of statistic information and cost model.
 */
public class Optimizer {

    private final CascadesContext cascadesContext;

    public Optimizer(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null");
    }

    /**
     * 执行 CBO（基于代价的优化）的核心方法
     * 
     * 根据 Join 数量和会话变量，选择使用 DPHyp 或 Cascades 优化器。
     * 
     * 执行流程：
     * 1. 初始化 Memo：将逻辑计划转换为 Memo 结构（Cascades 优化器的核心数据结构）
     * 2. 推导统计信息：为所有逻辑表达式推导统计信息（行数、基数等），用于后续代价计算
     * 3. DPHyp 优化（可选）：如果满足条件，使用 DPHyp 算法优化 Join 顺序
     * 4. Cascades 优化：使用 Cascades 框架进行完整的优化（探索等价计划、枚举物理实现、计算代价）
     * 
     * 优化器选择逻辑：
     * - DPHyp：适用于 Join 数量较多（> maxTableCount）或用户显式启用的情况
     *   - 限制：最多只能优化 64 个 Join 操作符
     * - Cascades：默认优化器，适用于所有场景
     * 
     * MoreFieldsThread.keepFunctionSignature 用于保持函数签名信息（用于调试和追踪）
     */
    public void execute() {
        MoreFieldsThread.keepFunctionSignature(() -> {
            // 步骤1：初始化 Memo
            // 将逻辑计划树转换为 Memo 结构
            // Memo 是 Cascades 优化器的核心数据结构，用于存储等价计划组（Group）
            // 每个 Group 包含多个逻辑等价的计划（GroupExpression）
            // 这样可以避免重复存储相同的子计划，实现计划共享
            cascadesContext.toMemo();
            
            // 步骤2：推导统计信息
            // 统计信息是代价计算的基础，包括：
            // - 表行数（row count）
            // - 列基数（cardinality）
            // - 数据分布（distribution）
            // - 选择率（selectivity）
            // 
            // 为根 Group 的所有逻辑表达式创建 DeriveStatsJob
            // 这些 Job 会递归地推导整个计划树的统计信息
            cascadesContext.getMemo().getRoot().getLogicalExpressions().forEach(groupExpression ->
                    cascadesContext.pushJob(
                            new DeriveStatsJob(groupExpression, cascadesContext.getCurrentJobContext())));
            
            // 执行统计信息推导任务池
            // 所有 DeriveStatsJob 会被调度执行，完成统计信息的推导
            cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            
            // 步骤3：DPHyp 优化（可选）
            // DPHyp（Dynamic Programming Hypergraph）是一种高效的 Join 顺序优化算法
            // 适用于 Join 数量较多的场景
            // 
            // 判断是否使用 DPHyp：
            // - 用户显式启用（isDpHyp() 返回 true）
            // - 或者通过 isDpHyp() 方法自动判断（基于 Join 数量、统计信息等）
            // 
            // 限制：DPHyp 目前最多只能优化 64 个 Join 操作符
            if (cascadesContext.getStatementContext().isDpHyp() || isDpHyp(cascadesContext)) {
                // RightNow, dp hyper can only order 64 join operators
                // 执行 DPHyp 优化，优化 Join 顺序
                dpHypOptimize();
            }
            
            // 步骤4：Cascades 优化
            // 使用 Cascades 框架进行完整的优化：
            // - 探索等价逻辑计划（通过应用转换规则）
            // - 枚举物理实现（如 HashJoin、SortMergeJoin、NestedLoopJoin）
            // - 计算代价（基于统计信息和代价模型）
            // - 选择最优计划（代价最低的物理计划）
            // 
            // OptimizeGroupJob 会递归地优化整个计划树
            // 从根 Group 开始，逐步优化每个子 Group
            cascadesContext.pushJob(
                    new OptimizeGroupJob(cascadesContext.getMemo().getRoot(), cascadesContext.getCurrentJobContext()));
            
            // 执行 Cascades 优化任务池
            // 所有优化任务会被调度执行，直到找到最优计划或达到优化目标
            cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            return null;
        });
    }

    /**
     * This method calc the result that if use dp hyper or not
     */
    public static boolean isDpHyp(CascadesContext cascadesContext) {
        boolean optimizeWithUnknownColStats = false;
        if (ConnectContext.get() != null && ConnectContext.get().getStatementContext() != null) {
            if (ConnectContext.get().getStatementContext().isHasUnknownColStats()) {
                optimizeWithUnknownColStats = true;
            }
        }
        // DPHyp optimize
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        int maxTableCount = sessionVariable.getMaxTableCountUseCascadesJoinReorder();
        if (optimizeWithUnknownColStats) {
            // if column stats are unknown, 10~20 table-join is optimized by cascading framework
            maxTableCount = 2 * maxTableCount;
        }
        int continuousJoinNum = Memo.countMaxContinuousJoin(cascadesContext.getRewritePlan());
        cascadesContext.getStatementContext().setMaxContinuousJoin(continuousJoinNum);
        boolean isDpHyp = sessionVariable.enableDPHypOptimizer || continuousJoinNum > maxTableCount;
        boolean finalEnableDpHyp = !sessionVariable.isDisableJoinReorder()
                && !cascadesContext.isLeadingDisableJoinReorder()
                && continuousJoinNum <= sessionVariable.getMaxJoinNumberOfReorder()
                && isDpHyp;
        cascadesContext.getStatementContext().setDpHyp(finalEnableDpHyp);
        return finalEnableDpHyp;
    }

    private void dpHypOptimize() {
        Group root = cascadesContext.getMemo().getRoot();
        // Due to EnsureProjectOnTopJoin, root group can't be Join Group, so DPHyp doesn't change the root group
        cascadesContext.pushJob(new JoinOrderJob(root, cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    private SessionVariable getSessionVariable() {
        return cascadesContext.getConnectContext().getSessionVariable();
    }
}
