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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 记录临时计划，用于后续的物化视图预重写。
 * 
 * 该规则在 RBO（基于规则的优化）阶段执行，用于保存临时计划到 StatementContext 中，
 * 供后续 Pre-MV Rewrite 阶段使用。
 * 
 * 执行流程：
 * 1. 检查是否需要记录临时计划（needRecordTmpPlanForRewrite）
 * 2. 如果需要，对计划进行预处理（CTE 重写、表达式规范化等）
 * 3. 将处理后的计划添加到 tmpPlanForMvRewrite 列表中
 * 4. 返回原始计划（不修改计划树）
 * 
 * 注意：该方法不会修改输入的计划树，只是保存一个处理后的副本供后续使用。
 */
public class RecordPlanForMvPreRewrite extends DefaultPlanRewriter<Void> implements CustomRewriter {

    public static final Logger LOG = LogManager.getLogger(RecordPlanForMvPreRewrite.class);

    /**
     * 重写计划根节点，记录临时计划用于后续物化视图预重写。
     * 
     * 该方法在 RBO 阶段被调用，用于：
     * 1. 检查是否需要记录临时计划（基于物化视图候选、预重写策略等）
     * 2. 如果需要，对计划进行预处理（CTE 重写、表达式规范化等）
     * 3. 将处理后的计划保存到 StatementContext.tmpPlanForMvRewrite 中
     * 4. 返回原始计划（不修改计划树）
     * 
     * ==================== 执行流程 ====================
     * 
     * 示例：查询包含 CTE 和物化视图候选
     * 
     * 步骤1：检查是否需要记录临时计划
     * - 调用 needRecordTmpPlanForRewrite() 检查：
     *   - 是否有物化视图候选（getCandidateMVs() 或 getCandidateMTMVs() 不为空）
     *   - 预重写策略是否为 NOT_IN_RBO
     *   - 是否存在物化视图 hook
     * - 如果不需要，直接返回原始计划
     * 
     * 步骤2：对计划进行预处理
     * - 创建独立的 CascadesContext，对计划应用 CTE 重写规则
     * - 使用 CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED 规则集合，包括：
     *   - 计划规范化（Plan Normalization）
     *   - 表达式规范化（ExpressionNormalizationAndOptimization）
     *   - 子查询去嵌套（Subquery unnesting）
     *   - 其他 RBO 规则
     * - 目的是在 Pre-MV Rewrite 之前，先对计划进行规范化处理
     * 
     * 步骤3：保存临时计划
     * - 将处理后的计划添加到 StatementContext.tmpPlanForMvRewrite
     * - 这个计划会在后续的 preMaterializedViewRewrite() 中使用
     * 
     * 步骤4：返回原始计划
     * - 注意：返回的是原始计划，不是处理后的计划
     * - 原因：这个规则只是记录临时计划，不应该修改当前的计划树
     * 
     * ==================== 为什么需要预处理 ====================
     * 
     * 1. CTE 处理：如果查询包含 CTE，需要先进行 CTE 内联或重写，才能正确匹配物化视图
     * 2. 表达式规范化：确保表达式格式统一，便于后续的物化视图匹配
     * 3. 子查询去嵌套：将子查询转换为 Join，便于物化视图匹配
     * 
     * ==================== 使用场景 ====================
     * 
     * 临时计划会在以下场景使用：
     * - preMaterializedViewRewrite()：遍历 tmpPlansForMvRewrite，对每个临时计划进行物化视图预重写
     * - InitMaterializationContextHook：用于初始化物化视图上下文
     * 
     * @param plan 当前计划树的根节点
     * @param jobContext 任务上下文，包含 CascadesContext 等信息
     * @return 原始计划（不修改计划树）
     */
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        StatementContext statementContext = cascadesContext.getStatementContext();
        
        // 步骤1：检查是否需要记录临时计划
        // 判断条件：
        // - 是否有物化视图候选（getCandidateMVs() 或 getCandidateMTMVs() 不为空）
        // - 预重写策略是否为 NOT_IN_RBO
        // - 是否存在物化视图 hook
        // - 是否强制记录（forceRecordTmpPlan）
        if (!PreMaterializedViewRewriter.needRecordTmpPlanForRewrite(cascadesContext)) {
            return plan;
        }
        
        // 步骤2：对计划进行预处理（CTE 重写、表达式规范化等）
        // 目的：在 Pre-MV Rewrite 之前，先对计划进行规范化处理，便于后续的物化视图匹配
        Plan finalPlan;
        try {
            // 在独立的 CascadesContext 中对计划应用 CTE 重写规则
            // CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED 包含：
            // - 计划规范化（Plan Normalization）
            // - 表达式规范化（ExpressionNormalizationAndOptimization）
            // - 子查询去嵌套（Subquery unnesting）
            // - 其他 RBO 规则
            finalPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        // 获取 CTE 子计划重写器，应用 CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED 规则集合
                        Rewriter.getCteChildrenRewriter(childContext,
                                        ImmutableList.of(Rewriter.custom(RuleType.REWRITE_CTE_CHILDREN,
                                                () -> new RewriteCteChildren(
                                                        Rewriter.CTE_CHILDREN_REWRITE_JOBS_MV_REWRITE_USED,
                                                        false))))
                                .execute();
                        // 返回重写后的计划
                        return childContext.getRewritePlan();
                    }, plan, plan, false);
            
            // 步骤3：将处理后的计划添加到临时计划列表
            // 这个计划会在后续的 preMaterializedViewRewrite() 中使用
            statementContext.addTmpPlanForMvRewrite(finalPlan);
        } catch (Exception e) {
            // 异常处理：如果预处理失败，记录错误日志，但不中断整个流程
            // 这样可以提高容错性，即使临时计划生成失败，主查询仍可以继续执行
            LOG.error("mv rewrite in rbo rewrite pre normalize fail, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier(), e);
        }
        
        // 步骤4：返回原始计划（不修改计划树）
        // 注意：返回的是原始计划，不是处理后的计划
        // 原因：这个规则只是记录临时计划，不应该修改当前的计划树
        return plan;
    }
}
