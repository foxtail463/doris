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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Individual materialized view rewriter based CBO
 */
public class PreMaterializedViewRewriter {
    public static BitSet NEED_PRE_REWRITE_RULE_TYPES = new BitSet();
    private static final Logger LOG = LogManager.getLogger(PreMaterializedViewRewriter.class);

    static {
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_PROJECT_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_PROJECT_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.ELIMINATE_CONST_JOIN_CONDITION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.MERGE_PERCENTILE_TO_ARRAY.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.SUM_LITERAL_REWRITE.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.DISTINCT_AGG_STRATEGY_SELECTOR.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.CONSTANT_PROPAGATION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.DISTINCT_AGGREGATE_SPLIT.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PROCESS_SCALAR_AGG_MUST_USE_MULTI_DISTINCT.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.ELIMINATE_GROUP_BY_KEY_BY_UNIFORM.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.SALT_JOIN.ordinal());
    }

    /**
     * 物化视图预重写
     * 
     * 该方法在 RBO 阶段之后、CBO 阶段之前执行，用于：
     * 1. 对临时计划进行优化（应用 CBO 规则）
     * 2. 选择最佳物理计划
     * 3. 从物理计划中提取被选中的物化视图和使用的表
     * 4. 根据使用的表集合，提取对应的逻辑计划结构信息
     * 5. 返回原始逻辑计划，用于后续的 CBO 优化
     * 
     * 流程说明：
     * - 预重写的目的是：在 CBO 阶段之前，先通过优化器找到使用物化视图的最佳物理计划，
     *   然后提取该计划对应的逻辑计划结构，作为后续 CBO 优化的输入
     * - 这样可以在 CBO 阶段更好地利用物化视图进行查询优化
     * 
     * @param cascadesContext 级联上下文，包含优化器状态、物化视图上下文等
     * @return 如果成功提取到逻辑计划，返回原始逻辑计划；否则返回 null
     */
    public static Plan rewrite(CascadesContext cascadesContext) {
        // 前置检查：如果没有物化视图上下文，或者不需要预重写，直接返回 null
        if (cascadesContext.getMaterializationContexts().isEmpty()
                || !cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
            return null;
        }
        
        // 步骤1：执行优化器，对临时计划进行优化
        // 这会应用 CBO 规则，生成多个候选物理计划，并选择最优的
        new Optimizer(cascadesContext).execute();
        
        // 步骤2：从 memo 的根节点选择最佳物理计划
        // chooseBestPlan 会根据代价模型选择最优的物理计划
        Group root = cascadesContext.getMemo().getRoot();
        PhysicalPlan physicalPlan = NereidsPlanner.chooseBestPlan(root,
                cascadesContext.getCurrentJobContext().getRequiredProperties(), cascadesContext);
        
        // 步骤3：从最佳物理计划中提取被选中的物化视图和使用的表
        // chosenMaterializationAndUsedTable.key() 是被选中的物化视图标识符列表
        // chosenMaterializationAndUsedTable.value() 是物理计划使用的表ID集合（BitSet）
        Pair<Map<List<String>, MaterializationContext>, BitSet> chosenMaterializationAndUsedTable
                = MaterializedViewUtils.getChosenMaterializationAndUsedTable(physicalPlan,
                cascadesContext.getAllMaterializationContexts());
        
        // 步骤4：刷新 memo 版本，确保结构信息的版本一致性
        // 当 memo 结构发生变化时，需要更新版本号，以便后续的结构信息提取能正确工作
        cascadesContext.getMemo().incrementAndGetRefreshVersion();
        
        // 步骤5：根据物理计划使用的表ID集合，从根节点的 StructInfoMap 中提取对应的逻辑计划结构信息
        // getStructInfo 会根据 tableBitSet（chosenMaterializationAndUsedTable.value()）查找或构造对应的 StructInfo
        // StructInfo 包含了逻辑计划的结构信息，用于后续的物化视图重写匹配
        StructInfo structInfo = root.getStructInfoMap().getStructInfo(cascadesContext,
                chosenMaterializationAndUsedTable.value(), root, null, true);
        
        // 错误检查：如果无法提取结构信息，记录错误日志
        if (structInfo == null) {
            LOG.error("preMaterializedViewRewriter rewrite structInfo is null, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
        }
        
        // 步骤6：如果成功提取到结构信息，并且有物化视图被选中，返回原始逻辑计划
        // structInfo.getOriginalPlan() 返回的是与物理计划对应的原始逻辑计划
        // 这个逻辑计划会被用作后续 CBO 优化的输入，以便更好地利用物化视图
        if (structInfo != null && !chosenMaterializationAndUsedTable.key().isEmpty()) {
            return structInfo.getOriginalPlan();
        }
        
        // 如果没有物化视图被选中，或无法提取结构信息，返回 null
        return null;
    }

    public static BitSet getNeedPreRewriteRule() {
        return NEED_PRE_REWRITE_RULE_TYPES;
    }

    /**
     * 判断是否需要记录临时计划用于后续的物化视图预重写。
     * 
     * 该方法在 RBO（基于规则的优化）阶段被调用，用于决定是否需要在 RecordPlanForMvPreRewrite 规则中
     * 记录临时计划。临时计划会在后续的 Pre-MV Rewrite 阶段使用。
     * 
     * ==================== 判断条件 ====================
     * 
     * 满足以下任一条件时，返回 true（需要记录临时计划）：
     * 1. 强制记录标志为 true（forceRecordTmpPlan）
     *    - 用于特殊场景，强制记录临时计划
     * 
     * 2. 满足以下所有条件：
     *    a) 预重写策略不是 NOT_IN_RBO
     *       - NOT_IN_RBO：不在 RBO 阶段进行预重写，因此不需要记录临时计划
     *       - TRY_IN_RBO：尝试在 RBO 阶段进行预重写
     *       - FORCE_IN_RBO：强制在 RBO 阶段进行预重写
     *    b) 存在物化视图 hook
     *       - hook 是物化视图重写的关键组件，没有 hook 无法进行重写
     *    c) 有物化视图候选
     *       - candidateMVs：同步物化视图（Rollup）候选集合，在分析阶段由 CollectRelation 收集
     *       - candidateMTMVs：异步物化视图（MTMV）候选集合，在分析阶段由 CollectRelation 收集
     * 
     * ==================== 执行时机 ====================
     * 
     * 该方法在以下场景被调用：
     * 1. RecordPlanForMvPreRewrite.rewriteRoot()：判断是否需要记录临时计划
     * 2. PreMaterializedViewRewriter.needPreRewrite()：判断是否需要预重写（前置检查）
     * 
     * ==================== 示例 ====================
     * 
     * 示例1：有物化视图候选
     * - 查询：SELECT * FROM orders WHERE id = 5
     * - 存在 MTMV：CREATE MATERIALIZED VIEW mv_orders AS SELECT * FROM orders WHERE id > 0
     * - candidateMTMVs 不为空 → 返回 true
     * 
     * 示例2：没有物化视图候选
     * - 查询：SELECT * FROM orders WHERE id = 5
     * - 不存在任何相关的物化视图
     * - candidateMVs 和 candidateMTMVs 都为空 → 返回 false
     * 
     * 示例3：预重写策略为 NOT_IN_RBO
     * - 预重写策略设置为 NOT_IN_RBO
     * - 即使有物化视图候选，也返回 false（不在 RBO 阶段进行预重写）
     * 
     * @param cascadesContext 级联上下文，包含优化器状态和物化视图信息
     * @return true 表示需要记录临时计划，false 表示不需要
     */
    public static boolean needRecordTmpPlanForRewrite(CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        
        // 获取预重写策略（FORCE_IN_RBO / TRY_IN_RBO / NOT_IN_RBO）
        PreRewriteStrategy preRewriteStrategy = PreRewriteStrategy.getEnum(
                cascadesContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy());
        
        // 条件1：强制记录标志
        // 如果强制记录标志为 true，直接返回 true（用于特殊场景）
        if (statementContext.isForceRecordTmpPlan()) {
            return true;
        }
        
        // 条件2：预重写策略检查
        // 如果预重写策略为 NOT_IN_RBO，不在 RBO 阶段进行预重写，因此不需要记录临时计划
        if (PreRewriteStrategy.NOT_IN_RBO.equals(preRewriteStrategy)) {
            return false;
        }
        
        // 条件3：物化视图 hook 检查
        // hook 是物化视图重写的关键组件，没有 hook 无法进行重写
        // 如果不存在 hook，说明当前查询不支持物化视图重写，不需要记录临时计划
        if (!MaterializedViewUtils.containMaterializedViewHook(statementContext)) {
            // current statement context doesn't have hook, doesn't use pre RBO materialized view rewrite
            return false;
        }
        
        // 条件4：物化视图候选检查
        // candidateMVs：同步物化视图（Rollup）候选集合，在分析阶段由 CollectRelation 收集
        // candidateMTMVs：异步物化视图（MTMV）候选集合，在分析阶段由 CollectRelation 收集
        // 如果两个候选集合都为空，说明没有可用的物化视图，不需要记录临时计划
        return !statementContext.getCandidateMVs().isEmpty() || !statementContext.getCandidateMTMVs().isEmpty();
    }

    /**
     * 判断是否需要在 RBO（Rule-Based Optimization）阶段进行物化视图预重写
     * 
     * 该方法在 RBO 阶段结束后被调用，用于决定是否执行物化视图的预重写优化。
     * 预重写的目的是：在 CBO 阶段之前，先对查询计划进行一些关键的重写规则应用，
     * 以便更好地利用物化视图进行查询优化。
     * 
     * 判断条件（必须全部满足）：
     * 1. 需要记录临时计划（needRecordTmpPlanForRewrite）
     * 2. 临时计划不为空（RBO 阶段已经生成了临时计划）
     * 3. 存在物化视图 hook（有物化视图相关的上下文）
     * 4. 不是 dp hyper 优化器（dp hyper 优化器在初始化时每个 group 只支持一个 group expression）
     * 5. 满足以下任一条件：
     *    - 有需要预重写的规则被应用（规则交集不为空）
     *    - 预重写策略为 FORCE_IN_RBO（强制在 RBO 阶段重写）
     * 
     * @param cascadesContext 级联上下文，包含优化器状态和物化视图信息
     * @return true 表示需要进行预重写，false 表示不需要
     */
    public static boolean needPreRewrite(CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        
        // 检查1：是否需要记录临时计划
        // 如果不需要记录，说明没有物化视图候选或策略不允许，直接返回 false
        if (!needRecordTmpPlanForRewrite(cascadesContext)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("needPreRewrite found not need record tmp plan, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        
        // 检查2：临时计划是否为空
        // 如果 RBO 阶段没有生成临时计划，无法进行预重写
        if (statementContext.getTmpPlanForMvRewrite().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("does not need pre rewrite, because TmpPlanForMvRewrite is empty, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        
        // 检查3：是否存在物化视图 hook
        // hook 是物化视图重写的关键组件，没有 hook 无法进行重写
        if (!MaterializedViewUtils.containMaterializedViewHook(statementContext)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("does not need pre rewrite, because no hook exists, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        
        // 检查4：是否使用了 dp hyper 优化器
        // dp hyper 优化器在初始化时每个 group 只支持一个 group expression，
        // 这与预重写机制不兼容，因此需要跳过预重写
        if (Optimizer.isDpHyp(cascadesContext)) {
            // dp hyper only support one group expression in each group when init
            if (LOG.isDebugEnabled()) {
                LOG.debug("does not need pre rewrite, because is dp hyper optimize, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        
        // 检查5：判断是否有需要预重写的规则被应用，或者策略强制预重写
        // 获取在 RBO 阶段被应用的规则集合
        BitSet appliedRules = statementContext.getNeedPreMvRewriteRuleMasks();
        // 获取需要预重写的规则集合（如 PUSH_DOWN_TOP_N_THROUGH_JOIN 等）
        BitSet needPreRewriteRuleSet = (BitSet) getNeedPreRewriteRule().clone();
        // 计算交集：哪些需要预重写的规则在 RBO 阶段被实际应用了
        needPreRewriteRuleSet.and(appliedRules);
        
        // 获取预重写策略（FORCE_IN_RBO / TRY_IN_RBO / NOT_IN_RBO）
        PreRewriteStrategy preRewriteStrategy = PreRewriteStrategy.getEnum(
                statementContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy());
        
        // 判断是否需要预重写：
        // - 如果有需要预重写的规则被应用（交集不为空），则需要预重写
        // - 或者策略强制在 RBO 阶段重写（FORCE_IN_RBO）
        boolean shouldPreRewrite = !needPreRewriteRuleSet.isEmpty()
                || PreRewriteStrategy.FORCE_IN_RBO.equals(preRewriteStrategy);
        
        if (!shouldPreRewrite && LOG.isDebugEnabled()) {
            LOG.debug("does not need pre rewrite, because needPreRewriteRuleSet is empty or "
                            + "preRewriteStrategy is not FORCE_IN_RBO, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
        }
        return shouldPreRewrite;
    }

    /**
     * convert millis to ceiling seconds
     */
    public static int convertMillisToCeilingSeconds(long milliseconds) {
        if (milliseconds <= 0) {
            return 0;
        }
        double secondsAsDouble = (double) milliseconds / 1000.0;
        double ceilingSeconds = Math.ceil(secondsAsDouble);
        return (int) ceilingSeconds;
    }

    /**
     * PreRewriteStrategy from materialized view rewrite
     */
    public enum PreRewriteStrategy {
        // Force transparent rewriting in the RBO phase
        FORCE_IN_RBO,
        // Attempt transparent rewriting in the RBO phase
        TRY_IN_RBO,
        // Do not attempt rewriting in the RBO phase; apply only during the CBO phase
        NOT_IN_RBO;

        public static PreRewriteStrategy getEnum(String name) {
            return EnumUtils.getEnum(PreRewriteStrategy.class, name);
        }
    }
}
