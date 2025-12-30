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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.StructInfoMap;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer.PartitionIncrementCheckContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer.PartitionIncrementChecker;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.rules.rewrite.QueryPartitionCollector;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.NondeterministicFunctionCollector;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The common util for materialized view
 */
public class MaterializedViewUtils {

    /**
     * Get related base table info which materialized view plan column reference,
     * input param plan should be rewritten plan that sub query should be eliminated
     *
     * @param materializedViewPlan this should be rewritten or analyzed plan, should not be physical plan.
     * @param column ref column name.
     */
    @Deprecated
    public static RelatedTableInfo getRelatedTableInfo(String column, String timeUnit,
            Plan materializedViewPlan, CascadesContext cascadesContext) {
        List<Slot> outputExpressions = materializedViewPlan.getOutput();
        NamedExpression columnExpr = null;
        // get column slot
        for (Slot outputSlot : outputExpressions) {
            if (outputSlot.getName().equalsIgnoreCase(column)) {
                columnExpr = outputSlot;
                break;
            }
        }
        if (columnExpr == null) {
            return RelatedTableInfo.failWith("partition column can not find from sql select column");
        }
        materializedViewPlan = PartitionIncrementMaintainer.removeSink(materializedViewPlan);
        Expression dateTrunc = null;
        if (timeUnit != null) {
            dateTrunc = new DateTrunc(columnExpr, new VarcharLiteral(timeUnit));
            columnExpr = new Alias(dateTrunc);
            materializedViewPlan = new LogicalProject<>(ImmutableList.of(columnExpr), materializedViewPlan);
        }
        // Check sql pattern
        Map<CTEId, Plan> producerCteIdToPlanMap = collectProducerCtePlans(materializedViewPlan);
        PartitionIncrementCheckContext checkContext = new PartitionIncrementCheckContext(
                columnExpr, dateTrunc, producerCteIdToPlanMap, materializedViewPlan, cascadesContext);
        checkContext.getPartitionAndRefExpressionMap().put(columnExpr,
                RelatedTableColumnInfo.of(columnExpr, null, true, false));
        materializedViewPlan.accept(PartitionIncrementChecker.INSTANCE, checkContext);
        List<RelatedTableColumnInfo> checkedTableColumnInfos =
                PartitionIncrementMaintainer.getRelatedTableColumnInfosWithCheck(checkContext, tableColumnInfo ->
                        tableColumnInfo.isOriginalPartition() && tableColumnInfo.isFromTablePartitionColumn());
        if (checkedTableColumnInfos == null) {
            return RelatedTableInfo.failWith("multi partition column data types are different");
        }
        if (checkContext.isFailFast()) {
            return RelatedTableInfo.failWith("partition column is not in group by or window partition by, "
                    + checkContext.getFailReasons());
        }
        if (!checkedTableColumnInfos.isEmpty()) {
            return RelatedTableInfo.successWith(checkedTableColumnInfos);
        }
        return RelatedTableInfo.failWith(String.format("can't not find valid partition track column, because %s",
                String.join(",", checkContext.getFailReasons())));
    }

    /**
     * Get related base table info which materialized view plan column reference,
     * input param plan should be rewritten plan that sub query should be eliminated
     *
     * @param materializedViewPlan this should be rewritten or analyzed plan, should not be physical plan.
     * @param column ref column name.
     */
    public static RelatedTableInfo getRelatedTableInfos(String column, String timeUnit,
            Plan materializedViewPlan, CascadesContext cascadesContext) {
        List<Slot> outputExpressions = materializedViewPlan.getOutput();
        NamedExpression columnExpr = null;
        // get column slot
        for (Slot outputSlot : outputExpressions) {
            if (outputSlot.getName().equalsIgnoreCase(column)) {
                columnExpr = outputSlot;
                break;
            }
        }
        if (columnExpr == null) {
            return RelatedTableInfo.failWith("partition column can not find from sql select column");
        }
        materializedViewPlan = PartitionIncrementMaintainer.removeSink(materializedViewPlan);
        Expression dateTrunc = null;
        if (timeUnit != null) {
            dateTrunc = new DateTrunc(columnExpr, new VarcharLiteral(timeUnit));
            columnExpr = new Alias(dateTrunc);
            materializedViewPlan = new LogicalProject<>(ImmutableList.of(columnExpr), materializedViewPlan);
        }
        // Check sql pattern
        Map<CTEId, Plan> producerCteIdToPlanMap = collectProducerCtePlans(materializedViewPlan);
        PartitionIncrementCheckContext checkContext = new PartitionIncrementCheckContext(
                columnExpr, dateTrunc, producerCteIdToPlanMap, materializedViewPlan, cascadesContext);
        checkContext.getPartitionAndRefExpressionMap().put(columnExpr,
                RelatedTableColumnInfo.of(columnExpr, null, true, false));
        materializedViewPlan.accept(PartitionIncrementChecker.INSTANCE, checkContext);
        if (!checkPartitionRefExpression(checkContext.getPartitionAndRefExpressionMap().values())) {
            return RelatedTableInfo.failWith(String.format(
                    "partition ref expressions is not consistent, partition ref expressions map is %s",
                    checkContext.getPartitionAndRefExpressionMap()));
        }
        List<RelatedTableColumnInfo> checkedTableColumnInfos =
                PartitionIncrementMaintainer.getRelatedTableColumnInfosWithCheck(checkContext,
                        RelatedTableColumnInfo::isFromTablePartitionColumn);
        if (checkedTableColumnInfos == null) {
            return RelatedTableInfo.failWith("multi partition column data types are different");
        }
        if (checkContext.isFailFast()) {
            return RelatedTableInfo.failWith("partition column is not in group by or window partition by, "
                    + checkContext.getFailReasons());
        }
        if (!checkedTableColumnInfos.isEmpty()) {
            return RelatedTableInfo.successWith(checkedTableColumnInfos);
        }
        return RelatedTableInfo.failWith(String.format("can't not find valid partition track column, because %s",
                String.join(",", checkContext.getFailReasons())));
    }

    private static Map<CTEId, Plan> collectProducerCtePlans(Plan plan) {
        Map<CTEId, Plan> collectProducerCtePlans = new HashMap<>();
        plan.accept(new DefaultPlanVisitor<Void, Map<CTEId, Plan>>() {
            @Override
            public Void visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
                    Map<CTEId, Plan> context) {
                context.put(cteProducer.getCteId(), cteProducer);
                return super.visitLogicalCTEProducer(cteProducer, context);
            }
        }, collectProducerCtePlans);
        return collectProducerCtePlans;
    }

    /**
     * Check the partition expression date_trunc num is valid or not
     */
    private static boolean checkPartitionRefExpression(Collection<RelatedTableColumnInfo> refExpressions) {
        for (RelatedTableColumnInfo tableColumnInfo : refExpressions) {
            if (tableColumnInfo.getPartitionExpression().isPresent()) {
                // If partition ref up expression is empty, return false directly
                List<DateTrunc> dateTruncs =
                        tableColumnInfo.getPartitionExpression().get().collectToList(DateTrunc.class::isInstance);
                if (dateTruncs.size() > 1) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * This method check the select query plan is contain the stmt as following or not
     * <p>
     * SELECT
     * [hint_statement, ...]
     * [ALL | DISTINCT | DISTINCTROW | ALL EXCEPT ( col_name1 [, col_name2, col_name3, ...] )]
     * select_expr [, select_expr ...]
     * [FROM table_references
     * [PARTITION partition_list]
     * [TABLET tabletid_list]
     * [TABLESAMPLE sample_value [ROWS | PERCENT]
     * [REPEATABLE pos_seek]]
     * [WHERE where_condition]
     * [GROUP BY [GROUPING SETS | ROLLUP | CUBE] {col_name | expr | position}]
     * [HAVING where_condition]
     * [ORDER BY {col_name | expr | position}
     * [ASC | DESC], ...]
     * [LIMIT {[offset,] row_count | row_count OFFSET offset}]
     * [INTO OUTFILE 'file_name']
     * <p>
     * if analyzedPlan contains the stmt as following:
     * [PARTITION partition_list]
     * [TABLET tabletid_list] or
     * [TABLESAMPLE sample_value [ROWS | PERCENT]
     * *         [REPEATABLE pos_seek]]
     * this method will return true.
     */
    public static boolean containTableQueryOperator(Plan analyzedPlan) {
        return analyzedPlan.accept(TableQueryOperatorChecker.INSTANCE, null);
    }

    /**
     * Transform to common table id, this is used by get query struct info, maybe little err when same table occur
     * more than once, this is not a problem because the process of query rewrite by mv would consider more
     */
    public static BitSet transformToCommonTableId(BitSet relationIdSet, Map<Integer, Integer> relationIdToTableIdMap) {
        BitSet transformedBitset = new BitSet();
        for (int i = relationIdSet.nextSetBit(0); i >= 0; i = relationIdSet.nextSetBit(i + 1)) {
            Integer commonTableId = relationIdToTableIdMap.get(i);
            if (commonTableId != null) {
                transformedBitset.set(commonTableId);
            }
        }
        return transformedBitset;
    }

    /**
     * 从计划中提取 StructInfo。
     * 支持从逻辑计划或 Group 中的计划提取 StructInfo。
     * 
     * @param plan 计划（可能已经移除了不必要的计划节点，逻辑输出可能不正确）
     * @param originalPlan 原始计划（逻辑输出是正确的）
     * @param cascadesContext 优化器上下文
     * @param materializedViewTableSet 物化视图使用的表的位集合，用于过滤匹配的 StructInfo
     * @return StructInfo 列表
     */
    public static List<StructInfo> extractStructInfo(Plan plan, Plan originalPlan, CascadesContext cascadesContext,
            BitSet materializedViewTableSet) {
        // 情况1：如果计划属于某个 Group（在 Memo 结构中的计划）
        // 需要从 Group 的 StructInfoMap 中获取已缓存的 StructInfo
        if (plan.getGroupExpression().isPresent()) {
            Group ownerGroup = plan.getGroupExpression().get().getOwnerGroup();
            StructInfoMap structInfoMap = ownerGroup.getStructInfoMap();
            
            // 刷新 StructInfoMap：从顶向下重新计算当前层级的 StructInfo
            // 确保 StructInfo 与最新的计划结构同步
            SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
            structInfoMap.refresh(ownerGroup, cascadesContext, new BitSet(), new HashSet<>(),
                    sessionVariable.isEnableMaterializedViewNestRewrite());
            structInfoMap.setRefreshVersion(cascadesContext.getMemo().getRefreshVersion());
            
            // 获取 Group 中所有不同表组合对应的 StructInfo 表集合
            // queryTableSets 中的每个 BitSet 代表一个表组合（每个 bit 表示一个表是否被使用）
            Set<BitSet> queryTableSets = structInfoMap.getTableMaps();
            ImmutableList.Builder<StructInfo> structInfosBuilder = ImmutableList.builder();
            
            if (!queryTableSets.isEmpty()) {
                for (BitSet queryTableSet : queryTableSets) {
                    // 将 RelationId 转换为 CommonTableId（统一表标识符）
                    // 这样可以在不同的计划上下文中正确比较表的使用情况
                    BitSet queryCommonTableSet = MaterializedViewUtils.transformToCommonTableId(queryTableSet,
                            cascadesContext.getStatementContext().getRelationIdToCommonTableIdMap());
                    
                    // 如果指定了 materializedViewTableSet（物化视图使用的表集合），
                    // 则只保留包含所有 MV 表的 StructInfo（用于过滤匹配的 StructInfo）
                    if (!materializedViewTableSet.isEmpty()
                            && !containsAll(materializedViewTableSet, queryCommonTableSet)) {
                        continue;
                    }
                    
                    // 从 StructInfoMap 中获取对应的 StructInfo
                    StructInfo structInfo = structInfoMap.getStructInfo(cascadesContext, queryTableSet, ownerGroup,
                            originalPlan, sessionVariable.isEnableMaterializedViewNestRewrite());
                    if (structInfo != null) {
                        structInfosBuilder.add(structInfo);
                    }
                }
            }
            return structInfosBuilder.build();
        }
        
        // 情况2：如果计划不属于任何 Group（独立的逻辑计划，不在 Memo 中）
        // 直接构造 StructInfo，不需要从缓存中获取
        return ImmutableList.of(StructInfo.of(plan, originalPlan, cascadesContext));
    }

    /**
     * Generate scan plan for materialized view
     * if MaterializationContext is already rewritten by materialized view, then should generate in real time
     * when query rewrite, because one plan may hit the materialized view repeatedly and the mv scan output
     * should be different
     */
    public static Plan generateMvScanPlan(OlapTable table, long indexId,
            List<Long> partitionIds,
            PreAggStatus preAggStatus,
            CascadesContext cascadesContext) {
        LogicalOlapScan olapScan = new LogicalOlapScan(
                cascadesContext.getStatementContext().getNextRelationId(),
                table,
                ImmutableList.of(table.getQualifiedDbName()),
                ImmutableList.of(),
                partitionIds,
                indexId,
                preAggStatus,
                ImmutableList.of(),
                // this must be empty, or it will be used to sample
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of());
        return BindRelation.checkAndAddDeleteSignFilter(olapScan, cascadesContext.getConnectContext(),
                olapScan.getTable());
    }

    /**
     * 通过规则优化计划，专门用于物化视图重写
     * 
     * 该方法支持通过定义不同的重写器来应用不同的规则进行优化。
     * 它在独立的 CascadesContext 中执行规则优化，确保不会影响原始上下文。
     * 
     * 主要功能：
     * 1. 在独立的上下文中对重写后的计划应用规则优化
     * 2. 处理输出列顺序的变化（RBO 规则可能会改变列顺序）
     * 3. 临时禁用某些规则（如 ADD_DEFAULT_LIMIT）
     * 4. 处理物化视图 hooks 的添加/移除
     * 5. 如果输出列顺序改变，重新排序以保持与原始计划一致
     * 
     * @param cascadesContext 原始的级联上下文
     * @param planRewriter 计划重写函数，接受 CascadesContext 并返回优化后的计划
     * @param rewrittenPlan 重写后的计划（物化视图重写后的计划）
     * @param originPlan 原始计划（用于对比输出列数量）
     * @param mvRewrite 是否为物化视图重写阶段
     *                    - true: 物化视图重写阶段，需要添加 MaterializationContext
     *                    - false: 规则优化阶段，需要移除物化视图 hooks
     * @return 优化后的计划，如果优化失败或输出列数量不匹配则返回 null 或原始计划
     */
    public static Plan rewriteByRules(
            CascadesContext cascadesContext, Function<CascadesContext, Plan> planRewriter,
            Plan rewrittenPlan, Plan originPlan, boolean mvRewrite) {
        // 前置检查：如果原始计划或重写计划为 null，返回 null
        if (originPlan == null || rewrittenPlan == null) {
            return null;
        }
        
        // 如果输出列数量不匹配，直接返回重写计划（不进行优化）
        // 这种情况通常发生在物化视图重写改变了输出列数量时
        if (originPlan.getOutputSet().size() != rewrittenPlan.getOutputSet().size()) {
            return rewrittenPlan;
        }
        
        // 保存原始重写计划，用于后续如果优化失败时回退
        Plan tmpRewrittenPlan = rewrittenPlan;
        
        // 步骤1：记录优化前的输出列顺序
        // 在 RBO 规则优化后，槽（slot）的顺序可能会改变
        // 需要记录原始顺序，以便后续恢复正确的列顺序
        List<ExprId> rewrittenPlanOutputsBeforeOptimize =
                rewrittenPlan.getOutput().stream().map(Slot::getExprId).collect(Collectors.toList());
        
        // 步骤2：为重写后的计划创建独立的 CascadesContext
        // 这样可以确保优化过程不会影响原始的上下文
        CascadesContext rewrittenPlanContext = CascadesContext.initContext(
                cascadesContext.getStatementContext(), rewrittenPlan,
                cascadesContext.getCurrentJobContext().getRequiredProperties());
        
        // 步骤3：临时禁用某些规则
        // 保存原始的禁用规则列表，用于后续恢复
        Set<String> oldDisableRuleNames = rewrittenPlanContext.getStatementContext().getConnectContext()
                .getSessionVariable()
                .getDisableNereidsRuleNames();
        
        // 临时禁用 ADD_DEFAULT_LIMIT 规则
        // 这个规则会在没有 LIMIT 的查询中添加默认的 LIMIT，但在物化视图重写中不需要
        rewrittenPlanContext.getStatementContext().getConnectContext().getSessionVariable()
                .setDisableNereidsRules(String.join(",", ImmutableSet.of(RuleType.ADD_DEFAULT_LIMIT.name())));
        rewrittenPlanContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        
        // 步骤4：处理物化视图 hooks
        // 用于记录被移除的 hooks，以便后续恢复
        List<PlannerHook> removedMaterializedViewHooks = new ArrayList<>();
        
        try {
            if (!mvRewrite) {
                // 如果不是物化视图重写阶段（规则优化阶段），移除物化视图 hooks
                // 避免在规则优化时再次触发物化视图重写，导致循环
                removedMaterializedViewHooks = removeMaterializedViewHooks(rewrittenPlanContext.getStatementContext());
            } else {
                // 如果是物化视图重写阶段，添加 MaterializationContext 到新的上下文
                // 这样在优化过程中可以使用物化视图信息
                cascadesContext.getMaterializationContexts().forEach(rewrittenPlanContext::addMaterializationContext);
            }
            
            // 步骤5：跳过权限检查
            // 在物化视图重写优化过程中，不需要进行权限检查，提高性能
            rewrittenPlanContext.getConnectContext().setSkipAuth(true);
            
            // 步骤6：执行计划重写
            // 使用 AtomicReference 来在 lambda 中保存结果
            AtomicReference<Plan> rewriteResult = new AtomicReference<>();
            
            // 在计划处理上下文中执行重写
            // withPlanProcess 用于控制是否记录计划变化过程（用于 EXPLAIN 展示）
            rewrittenPlanContext.withPlanProcess(cascadesContext.showPlanProcess(), () -> {
                rewriteResult.set(planRewriter.apply(rewrittenPlanContext));
            });
            
            // 将重写过程中的计划变化添加到原始上下文中
            cascadesContext.addPlanProcesses(rewrittenPlanContext.getPlanProcesses());
            rewrittenPlan = rewriteResult.get();
        } finally {
            // 步骤7：恢复原始设置
            // 无论成功还是失败，都要恢复原始配置，避免影响后续处理
            
            // 恢复权限检查
            rewrittenPlanContext.getConnectContext().setSkipAuth(false);
            
            // 恢复原始的禁用规则列表
            rewrittenPlanContext.getStatementContext().getConnectContext().getSessionVariable()
                    .setDisableNereidsRules(String.join(",", oldDisableRuleNames));
            rewrittenPlanContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            
            // 恢复物化视图 hooks
            rewrittenPlanContext.getStatementContext().getPlannerHooks().addAll(removedMaterializedViewHooks);
        }
        
        // 如果优化失败（返回 null），直接返回 null
        if (rewrittenPlan == null) {
            return null;
        }
        
        // 步骤8：处理特殊类型的计划（Sink）
        // Sink 类型的计划（如 INSERT INTO）可以保持正确的列顺序，不需要调整
        if (rewrittenPlan instanceof Sink) {
            // 可以保持正确的列顺序，不需要调整
            return rewrittenPlan;
        }
        
        // 步骤9：检查输出列顺序是否改变
        // 创建优化后的输出列映射（ExprId -> Slot）
        Map<ExprId, Slot> rewrittenPlanAfterOptimizedExprIdToOutputMap = Maps.newLinkedHashMap();
        for (Slot slot : rewrittenPlan.getOutput()) {
            rewrittenPlanAfterOptimizedExprIdToOutputMap.put(slot.getExprId(), slot);
        }
        
        // 获取优化后的输出列顺序
        List<ExprId> rewrittenPlanOutputsAfterOptimized = rewrittenPlan.getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toList());
        
        // 如果输出列顺序没有改变，直接返回优化后的计划
        if (rewrittenPlanOutputsBeforeOptimize.equals(rewrittenPlanOutputsAfterOptimized)) {
            return rewrittenPlan;
        }
        
        // 步骤10：调整输出列顺序
        // 如果输出列顺序改变了，需要重新排序以保持与原始计划一致
        // 某些规则可能会改变表达式的 ID，一旦发生这种情况，无法检查列顺序
        List<NamedExpression> adjustedOrderProjects = new ArrayList<>();
        for (ExprId exprId : rewrittenPlanOutputsBeforeOptimize) {
            Slot output = rewrittenPlanAfterOptimizedExprIdToOutputMap.get(exprId);
            if (output == null) {
                // 某些规则改变了输出槽的 ID，会导致错误
                // 在这种情况下，不进行优化，返回原始的重写计划
                return tmpRewrittenPlan;
            }
            adjustedOrderProjects.add(output);
        }
        
        // 如果输出列顺序改变了，返回带有重新排序的 Project 节点的计划
        // 这样可以确保输出列顺序与原始计划一致
        return new LogicalProject<>(adjustedOrderProjects, rewrittenPlan);
    }

    /**
     * Normalize expression such as nullable property and output slot id when plan in the plan tree
     */
    public static Plan normalizeExpressions(Plan rewrittenPlan, Plan originPlan) {
        if (rewrittenPlan.getOutput().size() != originPlan.getOutput().size()) {
            return null;
        }
        // normalize nullable
        List<NamedExpression> normalizeProjects = new ArrayList<>();
        for (int i = 0; i < originPlan.getOutput().size(); i++) {
            normalizeProjects.add(normalizeExpression(originPlan.getOutput().get(i),
                    rewrittenPlan.getOutput().get(i), false));
        }
        return new LogicalProject<>(normalizeProjects, rewrittenPlan);
    }

    /**
     * Normalize expression such as nullable property and output slot id when plan is on the top of tree
     */
    public static Plan normalizeSinkExpressions(Plan rewrittenPlan, Plan originPlan) {
        return rewrittenPlan.accept(new DefaultPlanRewriter<Void>() {
            @Override
            public Plan visitLogicalSink(LogicalSink<? extends Plan> rewrittenPlan, Void context) {
                if (rewrittenPlan.getOutput().size() != originPlan.getOutput().size()) {
                    return null;
                }
                if (rewrittenPlan.getLogicalProperties().equals(originPlan.getLogicalProperties())) {
                    return rewrittenPlan;
                }
                List<NamedExpression> rewrittenPlanOutputExprList = rewrittenPlan.getOutputExprs();
                List<? extends NamedExpression> originPlanOutputExprList = originPlan.getOutput();
                if (rewrittenPlanOutputExprList.size() != originPlanOutputExprList.size()) {
                    return null;
                }
                List<NamedExpression> normalizedOutputExprList = new ArrayList<>();
                for (int i = 0; i < rewrittenPlanOutputExprList.size(); i++) {
                    NamedExpression rewrittenExpression = rewrittenPlanOutputExprList.get(i);
                    NamedExpression originalExpression = originPlanOutputExprList.get(i);
                    normalizedOutputExprList.add(normalizeExpression(originalExpression,
                            rewrittenExpression, true));
                }
                LogicalProject<Plan> project = new LogicalProject<>(normalizedOutputExprList,
                        rewrittenPlan.child());
                return rewrittenPlan.withChildren(project);
            }
        }, null);

    }

    /**
     * Normalize expression with query, keep the consistency of exprId and nullable props with
     * query
     * Keep the replacedExpression slot property is the same as the sourceExpression
     */
    public static NamedExpression normalizeExpression(
            NamedExpression sourceExpression, NamedExpression replacedExpression, boolean isSink) {
        Expression innerExpression = replacedExpression;
        boolean isExprEquals = replacedExpression.getExprId().equals(sourceExpression.getExprId());
        boolean isNullableEquals = replacedExpression.nullable() == sourceExpression.nullable();
        if (isExprEquals && isNullableEquals) {
            return replacedExpression;
        }
        if (isExprEquals && isSink && replacedExpression instanceof SlotReference) {
            // for sink, if expr id is the same, but nullable is different, should keep the same expr id
            return ((SlotReference) replacedExpression).withNullable(sourceExpression.nullable());
        }
        if (!isNullableEquals) {
            // if enable join eliminate, query maybe inner join and mv maybe outer join.
            // If the slot is at null generate side, the nullable maybe different between query and view
            // So need to force to consistent.
            innerExpression = sourceExpression.nullable()
                    ? new Nullable(replacedExpression) : new NonNullable(replacedExpression);
        }
        return new Alias(sourceExpression.getExprId(), innerExpression, sourceExpression.getName());
    }

    /**
     * removeMaterializedViewHooks
     *
     * @return removed materialized view hooks
     */
    public static List<PlannerHook> removeMaterializedViewHooks(StatementContext statementContext) {
        List<PlannerHook> tmpMaterializedViewHooks = new ArrayList<>();
        Set<PlannerHook> otherHooks = new HashSet<>();
        for (PlannerHook hook : statementContext.getPlannerHooks()) {
            if (hook instanceof InitMaterializationContextHook) {
                tmpMaterializedViewHooks.add(hook);
            } else {
                otherHooks.add(hook);
            }
        }
        statementContext.clearMaterializedHooksBy(otherHooks);
        return tmpMaterializedViewHooks;
    }

    /**
     * Extract nondeterministic function form plan, if the function is in whiteExpressionSet,
     * the function would be considered as deterministic function and will not return
     * in the result expression result
     */
    public static List<Expression> extractNondeterministicFunction(Plan plan) {
        List<Expression> nondeterministicFunctions = new ArrayList<>();
        plan.accept(NondeterministicFunctionCollector.INSTANCE, nondeterministicFunctions);
        return nondeterministicFunctions;
    }

    /**
     * 收集查询中使用的表分区信息。
     *
     * 该方法用于物化视图重写时的分区匹配和补偿。它会遍历计划树，收集每个表在查询中实际使用的分区，
     * 并将这些信息存储到 StatementContext.tableUsedPartitionNameMap 中。
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：遍历计划树
     * - 使用 QueryPartitionCollector 访问计划树中的所有 LogicalCatalogRelation 节点
     * - 对于每个表（LogicalOlapScan 或 LogicalFileScan），收集其使用的分区信息
     *
     * 步骤2：收集分区信息
     * - LogicalOlapScan：从 getSelectedPartitionIds() 获取选中的分区 ID，转换为分区名称
     * - LogicalFileScan：从 getSelectedPartitions() 获取选中的分区名称（仅支持分区剪枝的外部表）
     * - 其他表：标记为查询所有分区（ALL_PARTITIONS）
     *
     * 步骤3：存储分区信息
     * - 将分区信息存储到 StatementContext.tableUsedPartitionNameMap
     * - Key：表的完整限定名（catalog.db.table）
     * - Value：Pair<RelationId, Set<String>>，包含关系 ID 和分区名称集合
     *
     * ==================== 使用场景 ====================
     *
     * 示例：查询指定分区
     * ```sql
     * SELECT * FROM orders PARTITION (p202401, p202402) WHERE amount > 100;
     * ```
     *
     * 执行过程：
     * 1. collectTableUsedPartitions 收集 orders 表使用的分区：{p202401, p202402}
     * 2. 存储到 tableUsedPartitionNameMap：orders -> (relationId, {p202401, p202402})
     * 3. 后续在物化视图重写时，PartitionCompensator 会使用这些信息判断：
     *    - 物化视图是否包含查询所需的所有分区
     *    - 如果物化视图缺少某些分区，需要进行分区补偿
     *
     * 示例：分区剪枝
     * ```sql
     * SELECT * FROM orders WHERE create_time >= '2024-01-01' AND create_time < '2024-02-01';
     * ```
     *
     * 执行过程：
     * 1. 优化器进行分区剪枝，只选择满足条件的分区（如 p202401）
     * 2. collectTableUsedPartitions 收集剪枝后的分区：{p202401}
     * 3. 物化视图重写时，只检查物化视图是否包含 p202401 分区
     *
     * ==================== 注意事项 ====================
     *
     * 1. 不支持累积：该方法不支持累积收集，如果多次调用，需要先清理 tableUsedPartitionNameMap
     * 2. 调用时机：通常在 InitMaterializationContextHook.afterRewrite() 中调用，
     *    在初始化 MaterializationContext 之前收集分区信息
     * 3. 分区信息基于 RelationId：同一个表在不同位置出现时，会分别记录其 RelationId 和使用的分区
     * 4. 分区补偿：收集的分区信息用于后续的 PartitionCompensator 进行分区匹配和补偿计算
     * 5. 外部表支持：仅支持支持分区剪枝的外部表（supportInternalPartitionPruned()），
     *    其他外部表会被标记为查询所有分区
     *
     * @param plan 要收集分区信息的计划树
     * @param cascadesContext 优化器上下文，用于访问 StatementContext
     */
    public static void collectTableUsedPartitions(Plan plan, CascadesContext cascadesContext) {
        // 使用 QueryPartitionCollector 访问计划树，收集每个表使用的分区信息
        // 收集的分区信息基于 RelationId，同一个表在不同位置出现时会分别记录
        plan.accept(new QueryPartitionCollector(), cascadesContext);
    }

    /**
     * Decide the statementContext if contain materialized view hook or not
     */
    public static boolean containMaterializedViewHook(StatementContext statementContext) {
        for (PlannerHook plannerHook : statementContext.getPlannerHooks()) {
            // only collect when InitMaterializationContextHook exists in planner hooks
            if (plannerHook instanceof InitMaterializationContextHook) {
                return true;
            }
        }
        return false;
    }

    /**
     * 计算最终物理计划中被选中的物化视图和使用的表
     * 
     * 该方法从物理计划树中提取以下信息：
     * 1. 被最终选中的物化视图（通过 isFinalChosen 判断）
     * 2. 所有使用的表ID集合（BitSet）
     * 
     * 执行流程：
     * 1. 使用访问者模式遍历物理计划树
     * 2. 对于每个 PhysicalRelation，记录其表ID到 usedRelation
     * 3. 对于 PhysicalOlapScan，检查是否是物化视图
     * 4. 如果是物化视图且被最终选中，添加到 chosenMaterializationMap
     * 
     * 物化视图识别：
     * - 通过 generateMaterializationIdentifierByIndexId 生成物化视图标识符
     * - 如果选中的索引ID是基表索引，标识符为 null（不是物化视图）
     * - 如果选中的索引ID是物化视图索引，生成对应的标识符
     * 
     * 最终选中判断（isFinalChosen）：
     * - 同步物化视图：检查选中的索引ID是否匹配物化视图的索引ID
     * - 异步物化视图：检查表的全限定名是否匹配物化视图的标识符
     * 
     * 使用场景：
     * - 预重写阶段：从最佳物理计划中提取使用的物化视图和表
     * - 结构信息提取：根据使用的表ID集合提取对应的结构信息
     * - 物化视图统计：统计哪些物化视图被最终使用
     * 
     * @param physicalPlan 物理计划树（通常是 CBO 阶段选择的最佳计划）
     * @param materializationContexts 所有物化视图上下文的映射
     *                                Key: 物化视图标识符（如 [db, table, indexId]）
     *                                Value: 对应的 MaterializationContext
     * @return Pair，包含：
     *         - 第一个元素：被最终选中的物化视图映射（Key: 标识符, Value: MaterializationContext）
     *         - 第二个元素：所有使用的表ID集合（BitSet），用于后续的结构信息提取
     */
    public static Pair<Map<List<String>, MaterializationContext>, BitSet> getChosenMaterializationAndUsedTable(
            Plan physicalPlan, Map<List<String>, MaterializationContext> materializationContexts) {
        // 用于收集被最终选中的物化视图
        final Map<List<String>, MaterializationContext> chosenMaterializationMap = new HashMap<>();
        // 用于收集所有使用的表ID（BitSet 用于高效存储和操作）
        BitSet usedRelation = new BitSet();
        
        // 使用访问者模式遍历物理计划树
        physicalPlan.accept(new DefaultPlanVisitor<Void, Map<List<String>, MaterializationContext>>() {
            /**
             * 访问 PhysicalCatalogRelation（目录关系，如表扫描）
             * 
             * 处理逻辑：
             * 1. 记录表ID到 usedRelation
             * 2. 如果是 PhysicalOlapScan，检查是否是物化视图
             * 3. 如果是物化视图且被最终选中，添加到 chosenMaterializationMap
             */
            @Override
            public Void visitPhysicalCatalogRelation(PhysicalCatalogRelation catalogRelation,
                    Map<List<String>, MaterializationContext> chosenMaterializationMap) {
                // 步骤1：记录表ID到 usedRelation
                // 无论是否是物化视图，都需要记录使用的表ID
                usedRelation.set(catalogRelation.getRelationId().asInt());
                
                // 步骤2：检查是否是 PhysicalOlapScan
                // 只有 PhysicalOlapScan 才可能是物化视图（其他类型如 JDBC、ES 等不是）
                if (!(catalogRelation instanceof PhysicalOlapScan)) {
                    return null;
                }
                
                PhysicalOlapScan physicalOlapScan = (PhysicalOlapScan) catalogRelation;
                OlapTable table = physicalOlapScan.getTable();
                
                // 步骤3：生成物化视图标识符
                // 如果选中的索引ID是基表索引，标识符为 null（不是物化视图）
                // 如果选中的索引ID是物化视图索引，生成对应的标识符
                List<String> materializationIdentifier
                        = MaterializationContext.generateMaterializationIdentifierByIndexId(table,
                        physicalOlapScan.getSelectedIndexId() == table.getBaseIndexId()
                                ? null : physicalOlapScan.getSelectedIndexId());
                
                // 步骤4：从物化视图上下文中查找对应的 MaterializationContext
                MaterializationContext materializationContext = materializationContexts.get(materializationIdentifier);
                if (materializationContext == null) {
                    // 如果找不到对应的上下文，说明不是物化视图或物化视图未注册
                    return null;
                }
                
                // 步骤5：检查物化视图是否被最终选中
                // isFinalChosen 用于判断该关系是否最终被选中作为物化视图
                // - 同步物化视图：检查选中的索引ID是否匹配
                // - 异步物化视图：检查表的全限定名是否匹配
                if (materializationContext.isFinalChosen(catalogRelation)) {
                    // 如果被最终选中，添加到结果映射中
                    chosenMaterializationMap.put(materializationIdentifier, materializationContext);
                }
                return null;
            }

            /**
             * 访问 PhysicalRelation（物理关系，通用接口）
             * 
             * 处理逻辑：
             * - 只记录表ID到 usedRelation
             * - 不检查是否是物化视图（因为不是 PhysicalCatalogRelation）
             */
            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation,
                    Map<List<String>, MaterializationContext> context) {
                // 记录表ID到 usedRelation
                // 用于后续的结构信息提取（根据表ID集合提取对应的结构信息）
                usedRelation.set(physicalRelation.getRelationId().asInt());
                return null;
            }
        }, chosenMaterializationMap);
        
        // 返回被选中的物化视图映射和使用的表ID集合
        return Pair.of(chosenMaterializationMap, usedRelation);
    }

    /**
     * Checks if the superset contains all of the set bits from the subset.
     *
     * @param superset The BitSet expected to contain the bits.
     * @param subset   The BitSet whose set bits are to be checked.
     * @return true if all bits set in the subset are also set in the superset, false otherwise.
     */
    public static boolean containsAll(BitSet superset, BitSet subset) {
        // Clone the subset to avoid modifying the original instance.
        BitSet temp = (BitSet) subset.clone();
        // Remove all bits from temp that are also present in the superset.
        // temp.andNot(superset) is equivalent to the operation: temp = temp AND (NOT superset)
        temp.andNot(superset);
        return temp.isEmpty();
    }

    /**
     * Check the query if Contains query operator
     * Such sql as following should return true
     * select * from orders TABLET(10098) because TABLET(10098) should return true
     * select * from orders_partition PARTITION (day_2) because PARTITION (day_2)
     * select * from orders index query_index_test because index query_index_test
     * select * from orders TABLESAMPLE(20 percent) because TABLESAMPLE(20 percent)
     * */
    public static final class TableQueryOperatorChecker extends DefaultPlanVisitor<Boolean, Void> {
        public static final TableQueryOperatorChecker INSTANCE = new TableQueryOperatorChecker();

        @Override
        public Boolean visitLogicalRelation(LogicalRelation relation, Void context) {
            if (relation instanceof LogicalFileScan && ((LogicalFileScan) relation).getTableSample().isPresent()) {
                return true;
            }
            if (relation instanceof LogicalOlapScan) {
                LogicalOlapScan logicalOlapScan = (LogicalOlapScan) relation;
                if (logicalOlapScan.getTableSample().isPresent()) {
                    // Contain sample, select * from orders TABLESAMPLE(20 percent)
                    return true;
                }
                if (!logicalOlapScan.getManuallySpecifiedTabletIds().isEmpty()) {
                    // Contain tablets, select * from orders TABLET(10098) because TABLET(10098)
                    return true;
                }
                if (!logicalOlapScan.getManuallySpecifiedPartitions().isEmpty()) {
                    // Contain specified partitions, select * from orders_partition PARTITION (day_2)
                    return true;
                }
                if (logicalOlapScan.getSelectedIndexId() != logicalOlapScan.getTable().getBaseIndexId()) {
                    // Contains select index or use sync mv in rbo rewrite
                    // select * from orders index query_index_test
                    return true;
                }
            }
            return visit(relation, context);
        }

        @Override
        public Boolean visit(Plan plan, Void context) {
            for (Plan child : plan.children()) {
                Boolean checkResult = child.accept(this, context);
                if (checkResult) {
                    return checkResult;
                }
            }
            return false;
        }
    }

    /**
     * Check the prefix of two order key list is same from start
     */
    public static boolean isPrefixSameFromStart(List<OrderKey> queryShuttledOrderKeys,
                                                 List<OrderKey> viewShuttledOrderKeys) {
        if (queryShuttledOrderKeys == null || viewShuttledOrderKeys == null) {
            return false;
        }
        if (queryShuttledOrderKeys.size() > viewShuttledOrderKeys.size()) {
            return false;
        }
        for (int i = 0; i < queryShuttledOrderKeys.size(); i++) {
            if (!java.util.Objects.equals(queryShuttledOrderKeys.get(i), viewShuttledOrderKeys.get(i))) {
                return false;
            }
        }
        return true;
    }
}
