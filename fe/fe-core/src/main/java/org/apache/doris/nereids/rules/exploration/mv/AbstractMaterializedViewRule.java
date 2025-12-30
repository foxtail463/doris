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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.ExpressionInfo;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.PartitionRemover;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.TableId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The abstract class for all materialized view rules
 */
public abstract class AbstractMaterializedViewRule implements ExplorationRuleFactory {

    public static final Logger LOG = LogManager.getLogger(AbstractMaterializedViewRule.class);
    public static final Set<JoinType> SUPPORTED_JOIN_TYPE_SET = ImmutableSet.of(
            JoinType.INNER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.RIGHT_OUTER_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_SEMI_JOIN,
            JoinType.RIGHT_SEMI_JOIN,
            JoinType.LEFT_ANTI_JOIN,
            JoinType.RIGHT_ANTI_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN);

    /**
     * 物化视图重写的模板方法，包含主要的重写逻辑。
     * 
     * 该方法会遍历所有可用的物化视图上下文，尝试将查询重写为使用物化视图。
     * 如果发生异常，会捕获异常并记录到物化视图上下文中，不会中断整个重写流程。
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：前置检查
     * - 检查是否有可用的物化视图上下文（MaterializationContext）
     * - 检查重写耗时是否超过阈值（避免重写时间过长）
     *
     * 步骤2：遍历每个物化视图上下文
     * - 对每个 MaterializationContext 执行以下检查：
     *   a) 检查是否已经重写过（checkIfRewritten）
     *      - 如果该 Group 已经使用过该物化视图重写，跳过
     *   b) 检查物化视图是否有效（isMaterializationValid）
     *      - 检查物化视图的 StructInfo 是否有效
     *      - 使用缓存机制避免重复检查
     *   c) 获取有效的查询 StructInfo（getValidQueryStructInfos）
     *      - 从查询计划中提取 StructInfo
     *      - 验证查询模式是否支持（checkQueryPattern）
     *      - 过滤掉无效的 StructInfo
     *
     * 步骤3：对每个有效的查询 StructInfo 执行重写
     * - 检查重写耗时是否超过阈值（每次重写前检查）
     * - 检查已重写的计划数量是否达到上限（materializedViewRewriteSuccessCandidateNum）
     * - 调用 doRewrite() 执行实际的重写逻辑
     * - 捕获异常并记录失败原因，不中断其他物化视图的重写
     *
     * 步骤4：性能统计
     * - 记录每个物化视图的重写耗时
     * - 累计总的重写耗时
     * - 更新性能分析 Profile
     *
     * ==================== 使用场景 ====================
     *
     * 示例：查询有多个物化视图候选
     * ```sql
     * -- 查询
     * SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
     * 
     * -- 物化视图1：mv1 (user_id, SUM(amount))
     * -- 物化视图2：mv2 (user_id, SUM(amount), COUNT(*))
     * ```
     *
     * 执行过程：
     * 1. 遍历 mv1 和 mv2 的 MaterializationContext
     * 2. 对每个物化视图：
     *    - 检查是否已经重写过（避免重复）
     *    - 检查物化视图是否有效（StructInfo 是否有效）
     *    - 提取查询的 StructInfo（从查询计划中提取结构信息）
     *    - 执行重写（doRewrite），生成使用物化视图的计划
     * 3. 返回所有重写后的计划列表
     *
     * ==================== 注意事项 ====================
     *
     * 1. 性能控制：
     *    - 通过 materializedViewRewriteDurationThresholdMs 限制总重写时间
     *    - 通过 materializedViewRewriteSuccessCandidateNum 限制重写计划数量
     *    - 避免重写时间过长或生成过多计划
     *
     * 2. 异常处理：
     *    - 捕获异常并记录失败原因，不中断其他物化视图的重写
     *    - 失败原因记录到 MaterializationContext，用于后续分析和调试
     *
     * 3. 缓存机制：
     *    - isMaterializationValid 使用 Memo 缓存检查结果
     *    - 避免对同一个物化视图重复检查有效性
     *
     * 4. 去重机制：
     *    - checkIfRewritten 检查 Group 是否已经使用过该物化视图
     *    - 避免对同一个 Group 重复应用同一个物化视图
     *
     * @param queryPlan 要重写的查询计划
     * @param cascadesContext 优化器上下文，包含物化视图上下文列表
     * @return 重写后的计划列表（可能为空，表示没有成功重写）
     */
    public List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<Plan> rewrittenPlans = new ArrayList<>();
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        StatementContext statementContext = cascadesContext.getStatementContext();
        
        // 步骤1：前置检查 - 如果没有可用的物化视图上下文，直接返回
        if (cascadesContext.getMaterializationContexts().isEmpty()) {
            return rewrittenPlans;
        }
        
        // 步骤1：前置检查 - 如果重写耗时已经超过阈值，停止重写
        if (statementContext.getMaterializedViewRewriteDuration()
                > sessionVariable.materializedViewRewriteDurationThresholdMs) {
            LOG.warn("materialized view rewrite duration is exceeded, the query queryId is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            MaterializationContext.makeFailWithDurationExceeded(queryPlan, cascadesContext.getMaterializationContexts(),
                    statementContext.getMaterializedViewRewriteDuration());
            return rewrittenPlans;
        }
        
        // 步骤2：遍历每个物化视图上下文，尝试重写查询
        for (MaterializationContext context : cascadesContext.getMaterializationContexts()) {
            statementContext.getMaterializedViewStopwatch().reset().start();
            
            // 检查2a：如果该 Group 已经使用过该物化视图重写，跳过
            if (checkIfRewritten(queryPlan, context)) {
                continue;
            }
            
            // 检查2b：检查物化视图是否有效（StructInfo 是否有效，使用缓存避免重复检查）
            if (!isMaterializationValid(queryPlan, cascadesContext, context)) {
                continue;
            }
            
            // 检查2c：获取有效的查询 StructInfo
            // 从查询计划中提取 StructInfo，验证查询模式是否支持，过滤掉无效的 StructInfo
            List<StructInfo> queryStructInfos = getValidQueryStructInfos(queryPlan, cascadesContext,
                    context.getCommonTableIdSet(statementContext));
            if (queryStructInfos.isEmpty()) {
                continue;
            }
            
            // 记录获取 StructInfo 的耗时
            long elapsed = statementContext.getMaterializedViewStopwatch().elapsed(TimeUnit.MILLISECONDS);
            statementContext.addMaterializedViewRewriteDuration(elapsed);
            SummaryProfile profile = SummaryProfile.getSummaryProfile(cascadesContext.getConnectContext());
            if (profile != null) {
                profile.addNereidsMvRewriteTime(elapsed);
            }
            
            // 步骤3：对每个有效的查询 StructInfo 执行重写
            for (StructInfo queryStructInfo : queryStructInfos) {
                statementContext.getMaterializedViewStopwatch().reset().start();
                
                // 检查重写耗时是否超过阈值（每次重写前检查）
                if (statementContext.getMaterializedViewRewriteDuration()
                        > sessionVariable.materializedViewRewriteDurationThresholdMs) {
                    statementContext.getMaterializedViewStopwatch().stop();
                    LOG.warn("materialized view rewrite duration is exceeded, the queryId is {}",
                            cascadesContext.getConnectContext().getQueryIdentifier());
                    MaterializationContext.makeFailWithDurationExceeded(queryStructInfo.getOriginalPlan(),
                            cascadesContext.getMaterializationContexts(),
                            statementContext.getMaterializedViewRewriteDuration());
                    return rewrittenPlans;
                }
                
                try {
                    // 检查已重写的计划数量是否达到上限
                    // 如果达到上限，不再继续重写，避免生成过多计划
                    if (rewrittenPlans.size() < sessionVariable.getMaterializedViewRewriteSuccessCandidateNum()) {
                        // 执行实际的重写逻辑，生成使用物化视图的计划
                        rewrittenPlans.addAll(doRewrite(queryStructInfo, cascadesContext, context));
                    }
                } catch (Exception exception) {
                    // 异常处理：捕获异常并记录失败原因，不中断其他物化视图的重写
                    LOG.warn("Materialized view rule exec fail", exception);
                    context.recordFailReason(queryStructInfo,
                            "Materialized view rule exec fail", exception::toString);
                } finally {
                    // 记录重写耗时
                    elapsed = statementContext.getMaterializedViewStopwatch().elapsed(TimeUnit.MILLISECONDS);
                    statementContext.addMaterializedViewRewriteDuration(elapsed);
                    if (profile != null) {
                        profile.addNereidsMvRewriteTime(elapsed);
                    }
                }
            }
        }
        return rewrittenPlans;
    }

    /**
     * Get valid query struct infos, if invalid record the invalid reason
     */
    protected List<StructInfo> getValidQueryStructInfos(Plan queryPlan, CascadesContext cascadesContext,
            BitSet materializedViewTableSet) {
        List<StructInfo> validStructInfos = new ArrayList<>();
        // For every materialized view we should trigger refreshing struct info map
        List<StructInfo> uncheckedStructInfos = MaterializedViewUtils.extractStructInfo(queryPlan, queryPlan,
                cascadesContext, materializedViewTableSet);
        uncheckedStructInfos.forEach(queryStructInfo -> {
            boolean valid = checkQueryPattern(queryStructInfo, cascadesContext) && queryStructInfo.isValid();
            if (!valid) {
                cascadesContext.getMaterializationContexts().forEach(ctx ->
                        ctx.recordFailReason(queryStructInfo, "Query struct info is invalid",
                                () -> String.format("query table bitmap is %s, plan is %s",
                                        queryStructInfo.getTableBitSet(), queryPlan.treeString())
                        ));
            } else {
                validStructInfos.add(queryStructInfo);
            }
        });
        return validStructInfos;
    }

    /**
     * 物化视图重写的核心模板方法，尝试将查询重写为使用物化视图。
     * 
     * 该方法包含物化视图重写的完整流程，每次只处理一个物化视图。
     * 不同的查询模式（Join、Aggregate、Scan 等）可以重写子逻辑来定制化处理。
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：决定匹配模式（MatchMode）
     * - COMPLETE：查询和物化视图的表集合完全相同
     * - QUERY_PARTIAL：物化视图的表集合包含查询的表集合（物化视图更宽）
     * - VIEW_PARTIAL：查询的表集合包含物化视图的表集合（查询更宽，不支持）
     * - NOT_MATCH：表集合不匹配，无法重写
     *
     * 步骤2：生成表映射关系（RelationMapping）
     * - 为查询中的每个表找到物化视图中对应的表
     * - 支持一对多映射（一个查询表对应多个物化视图表）
     * - 如果无法建立映射关系，重写失败
     *
     * 步骤3：生成列映射关系（SlotMapping）
     * - 基于表映射关系，生成查询列到物化视图列的映射
     * - 使用缓存机制避免重复计算
     * - 如果无法建立列映射关系，跳过当前表映射组合
     *
     * 步骤4：检查图逻辑等价性
     * - 比较查询和物化视图的逻辑计划图结构是否等价
     * - 检查 Join 类型、Join 条件、聚合函数等是否一致
     * - 如果图逻辑不一致，跳过当前映射组合
     *
     * 步骤5：谓词补偿（Predicates Compensate）
     * - 计算查询比物化视图更严格的谓词，需要在物化视图扫描后添加过滤
     * - 包括等值谓词补偿、范围谓词补偿、残差谓词补偿
     * - 如果无法补偿（查询比物化视图更宽松），重写失败
     *
     * 步骤6：生成重写计划
     * - 获取物化视图的扫描计划（mvScan）
     * - 如果补偿谓词为空（isAlwaysTrue），直接使用 mvScan
     * - 否则，将补偿谓词重写为基于物化视图扫描的表达式，并添加 LogicalFilter
     *
     * 步骤7：重写前检查（Pre-check）
     * - 检查重写计划是否满足各种约束条件
     * - 如果检查失败，跳过当前映射组合
     *
     * 步骤8：执行重写（rewriteQueryByView）
     * - 根据匹配模式，将查询计划重写为使用物化视图
     * - 处理 Project、Aggregate、Join 等节点的重写
     *
     * 步骤9：分区裁剪（Partition Prune）
     * - 对重写后的计划执行分区裁剪规则
     * - 确保后续分区补偿计算时使用正确的分区信息
     *
     * 步骤10：分区补偿（Partition Compensate，仅异步 MV）
     * - 检查物化视图是否有无效分区（数据过期或未刷新）
     * - 如果需要 Union Rewrite：
     *   a) 计算无效分区（invalidPartitions）
     *   b) 检查是否需要 Union（物化视图部分分区无效）
     *   c) 如果需要 Union：
     *      - 从物化视图中移除无效分区
     *      - 在基表上添加过滤条件，只查询无效分区的数据
     *      - 使用 LogicalUnion 合并物化视图和基表的结果
     *
     * 步骤11：表达式规范化（Normalize Expressions）
     * - 规范化重写计划中的表达式，确保与查询计划的表达式格式一致
     * - 如果规范化失败（输出不匹配），跳过当前映射组合
     *
     * 步骤12：合并 Project（Merge Project）
     * - 合并重写计划中多余的 Project 节点，优化计划结构
     *
     * 步骤13：验证输出（Validate Output）
     * - 检查重写计划的输出逻辑属性是否与查询计划一致
     * - 如果输出不匹配，跳过当前映射组合
     *
     * 步骤14：收集表分区信息
     * - 重新收集重写计划中涉及的表分区信息
     * - 用于后续的嵌套重写（nest rewrite）和分区匹配
     *
     * 步骤15：派生操作列（Derive Operative Column）
     * - 为物化视图扫描派生操作列信息，用于后续优化
     *
     * 步骤16：记录重写成功
     * - 记录物化视图重写成功的信息
     * - 清理物化视图扫描计划缓存（可能被其他重写复用）
     *
     * ==================== 使用场景 ====================
     *
     * 示例1：简单查询重写
     * ```sql
     * -- 查询
     * SELECT user_id, SUM(amount) FROM orders WHERE create_time > '2024-01-01' GROUP BY user_id;
     * 
     * -- 物化视图
     * CREATE MATERIALIZED VIEW mv AS
     * SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
     * ```
     * 执行过程：
     * 1. 匹配模式：COMPLETE（表集合相同）
     * 2. 表映射：orders -> orders
     * 3. 列映射：user_id -> user_id, amount -> amount
     * 4. 图逻辑等价：检查通过（都是 Aggregate）
     * 5. 谓词补偿：create_time > '2024-01-01'（查询更严格）
     * 6. 重写计划：LogicalFilter(create_time > '2024-01-01', LogicalOlapScan(mv))
     *
     * 示例2：分区补偿（Union Rewrite）
     * ```sql
     * -- 查询
     * SELECT * FROM orders WHERE ds = '2024-01-01';
     * 
     * -- 物化视图（分区：2024-01-01, 2024-01-02, 2024-01-03）
     * -- 其中 2024-01-01 分区数据过期（invalid）
     * ```
     * 执行过程：
     * 1. 重写计划：LogicalOlapScan(mv, partition=2024-01-01)
     * 2. 分区补偿：检测到 2024-01-01 分区无效
     * 3. Union Rewrite：
     *    - 物化视图：移除 2024-01-01 分区，保留 2024-01-02, 2024-01-03
     *    - 基表：添加过滤 ds = '2024-01-01'，查询无效分区数据
     *    - Union：合并物化视图和基表的结果
     *
     * ==================== 注意事项 ====================
     *
     * 1. 表映射组合：一个查询可能有多个表映射组合（例如多表 Join），需要遍历所有组合
     * 2. 谓词补偿：查询的谓词必须比物化视图更严格或相等，否则无法重写
     * 3. 分区补偿：仅异步 MV（MTMV）支持分区补偿和 Union Rewrite
     * 4. 表达式规范化：确保重写计划的表达式格式与查询计划一致，避免后续优化失败
     * 5. 嵌套重写：重写后的计划可能包含新的物化视图，需要记录表分区信息用于嵌套重写
     *
     * @param queryStructInfo 查询的结构信息
     * @param cascadesContext 优化器上下文
     * @param materializationContext 物化视图上下文
     * @return 重写后的计划列表（可能为空，表示没有成功重写）
     * @throws AnalysisException 分析异常
     */
    protected List<Plan> doRewrite(StructInfo queryStructInfo, CascadesContext cascadesContext,
            MaterializationContext materializationContext) throws AnalysisException {
        List<Plan> rewriteResults = new ArrayList<>();
        StructInfo viewStructInfo = materializationContext.getStructInfo();
        
        // 步骤1：决定匹配模式（COMPLETE 或 QUERY_PARTIAL）
        MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations(),
                cascadesContext);
        if (MatchMode.COMPLETE != matchMode && MatchMode.QUERY_PARTIAL != matchMode) {
            materializationContext.recordFailReason(queryStructInfo, "Match mode is invalid",
                    () -> String.format("matchMode is %s", matchMode));
            return rewriteResults;
        }
        
        // 步骤2：生成表映射关系（RelationMapping）
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        int materializedViewRelationMappingMaxCount = sessionVariable.getMaterializedViewRelationMappingMaxCount();
        List<RelationMapping> queryToViewTableMappings = RelationMapping.generate(queryStructInfo.getRelations(),
                viewStructInfo.getRelations(), materializedViewRelationMappingMaxCount);
        // 如果查询和物化视图中的表无法映射，重写失败
        if (queryToViewTableMappings == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "Query to view table mapping is null", () -> "");
            return rewriteResults;
        }
        
        // 遍历每个表映射组合（一个查询可能有多个表映射组合）
        for (RelationMapping queryToViewTableMapping : queryToViewTableMappings) {
            // 步骤3：生成列映射关系（SlotMapping）
            SlotMapping queryToViewSlotMapping =
                    materializationContext.getSlotMappingFromCache(queryToViewTableMapping);
            if (queryToViewSlotMapping == null) {
                queryToViewSlotMapping = SlotMapping.generate(queryToViewTableMapping);
                materializationContext.addSlotMappingToCache(queryToViewTableMapping, queryToViewSlotMapping);
            }
            if (queryToViewSlotMapping == null) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Query to view slot mapping is null", () ->
                                String.format("queryToViewTableMapping relation mapping is %s",
                                        queryToViewTableMapping));
                continue;
            }
            SlotMapping viewToQuerySlotMapping = queryToViewSlotMapping.inverse();
            
            // 步骤4：检查图逻辑等价性
            LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                    queryToViewTableMapping, viewToQuerySlotMapping, queryStructInfo, viewStructInfo);
            ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(queryStructInfo, viewStructInfo,
                    compatibilityContext);
            if (comparisonResult.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "The graph logic between query and view is not consistent",
                        comparisonResult::getErrorMessage);
                continue;
            }
            
            // 步骤5：谓词补偿（计算查询比物化视图更严格的谓词）
            SplitPredicate compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                    viewToQuerySlotMapping, comparisonResult, cascadesContext);
            // 如果无法补偿（查询比物化视图更宽松），跳过当前映射组合
            if (compensatePredicates.isInvalid()) {
                materializationContext.recordFailReason(queryStructInfo,
                        "Predicate compensate fail",
                        () -> String.format("query predicates = %s,\n query equivalenceClass = %s, \n"
                                        + "view predicates = %s,\n query equivalenceClass = %s\n"
                                        + "comparisonResult = %s ", queryStructInfo.getPredicates(),
                                queryStructInfo.getEquivalenceClass(), viewStructInfo.getPredicates(),
                                viewStructInfo.getEquivalenceClass(), comparisonResult));
                continue;
            }
            
            // 步骤6：生成重写计划
            Plan rewrittenPlan;
            Plan mvScan = materializationContext.getScanPlan(queryStructInfo, cascadesContext);
            Plan queryPlan = queryStructInfo.getTopPlan();
            if (compensatePredicates.isAlwaysTrue()) {
                // 如果补偿谓词为空（查询和物化视图的谓词完全匹配），直接使用物化视图扫描
                rewrittenPlan = mvScan;
            } else {
                // 否则，将补偿谓词重写为基于物化视图扫描的表达式，并添加 LogicalFilter
                // Try to rewrite compensate predicates by using mv scan
                List<Expression> rewriteCompensatePredicates = rewriteExpression(compensatePredicates.toList(),
                        queryPlan, materializationContext.getShuttledExprToScanExprMapping(),
                        viewToQuerySlotMapping, queryStructInfo.getTableBitSet(),
                        compensatePredicates.getRangePredicateMap(),
                        cascadesContext);
                if (rewriteCompensatePredicates.isEmpty()) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Rewrite compensate predicate by view fail",
                            () -> String.format("compensatePredicates = %s,\n mvExprToMvScanExprMapping = %s,\n"
                                            + "viewToQuerySlotMapping = %s",
                                    compensatePredicates, materializationContext.getShuttledExprToScanExprMapping(),
                                    viewToQuerySlotMapping));
                    continue;
                }
                rewrittenPlan = new LogicalFilter<>(Sets.newLinkedHashSet(rewriteCompensatePredicates), mvScan);
            }
            
            // 步骤7：重写前检查（Pre-check）
            boolean checkResult = rewriteQueryByViewPreCheck(matchMode, queryStructInfo,
                    viewStructInfo, viewToQuerySlotMapping, rewrittenPlan, materializationContext,
                    comparisonResult);
            if (!checkResult) {
                continue;
            }
            
            // 步骤8：执行重写（rewriteQueryByView）
            // Rewrite query by view
            rewrittenPlan = rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    rewrittenPlan, materializationContext, cascadesContext);
            
            // 步骤9：分区裁剪（Partition Prune）
            // 必须在分区补偿之前执行分区裁剪，确保后续计算使用正确的分区信息
            // 例如：物化视图有分区 17, 18, 19，查询只使用分区 18
            // 如果不在重写计划上执行分区裁剪，calcInvalidPartitions 可能会错误地将 17 和 19 添加到基表的分区集合中
            rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getWholeTreeRewriter(childContext).execute();
                        return childContext.getRewritePlan();
                    }, rewrittenPlan, queryPlan, false);
            if (rewrittenPlan == null) {
                continue;
            }
            
            // 步骤10：分区补偿（Partition Compensate，仅异步 MV）
            Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> invalidPartitions;
            if (PartitionCompensator.needUnionRewrite(materializationContext, cascadesContext.getStatementContext())
                    && sessionVariable.isEnableMaterializedViewUnionRewrite()) {
                MTMV mtmv = ((AsyncMaterializationContext) materializationContext).getMtmv();
                Map<List<String>, Set<String>> queryUsedPartitions = PartitionCompensator.getQueryUsedPartitions(
                        cascadesContext.getStatementContext(), queryStructInfo.getTableBitSet());
                Set<MTMVRelatedTableIf> pctTables = mtmv.getMvPartitionInfo().getPctTables();
                
                // 检查查询使用的分区信息是否有效
                boolean relateTableUsedPartitionsAnyNull = false;
                boolean relateTableUsedPartitionsAllEmpty = true;
                for (MTMVRelatedTableIf tableIf : pctTables) {
                    Set<String> queryUsedPartition = queryUsedPartitions.get(tableIf.getFullQualifiers());
                    relateTableUsedPartitionsAnyNull |= queryUsedPartition == null;
                    if (queryUsedPartition == null) {
                        continue;
                    }
                    relateTableUsedPartitionsAllEmpty &= queryUsedPartition.isEmpty();
                }
                if (relateTableUsedPartitionsAnyNull || relateTableUsedPartitionsAllEmpty) {
                    materializationContext.recordFailReason(queryStructInfo,
                            String.format("queryUsedPartition is all null or empty but needUnionRewrite, "
                                            + "queryUsedPartitions is %s, queryId is %s",
                                    queryUsedPartitions,
                                    cascadesContext.getConnectContext().getQueryIdentifier()),
                            () -> String.format(
                                    "queryUsedPartition is all null or empty but needUnionRewrite, "
                                            + "queryUsedPartitions is %s, queryId is %s",
                                    queryUsedPartitions,
                                    cascadesContext.getConnectContext().getQueryIdentifier()));
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format(
                                "queryUsedPartition is all null or empty but needUnionRewrite, "
                                        + "queryUsedPartitions is %s, queryId is %s",
                                queryUsedPartitions,
                                cascadesContext.getConnectContext().getQueryIdentifier()));
                    }
                    return rewriteResults;
                }
                
                // 计算无效分区（数据过期或未刷新的分区）
                try {
                    invalidPartitions = calcInvalidPartitions(queryUsedPartitions, rewrittenPlan,
                            cascadesContext, (AsyncMaterializationContext) materializationContext);
                } catch (AnalysisException e) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "Calc invalid partitions fail",
                            () -> String.format("Calc invalid partitions fail, mv partition names are %s",
                                    mtmv.getPartitions()));
                    LOG.warn("Calc invalid partitions fail", e);
                    continue;
                }
                if (invalidPartitions == null) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "mv can not offer any partition for query",
                            () -> String.format("mv partition info %s", mtmv.getMvPartitionInfo()));
                    // 如果物化视图无法为查询提供任何分区，重写失败，避免循环执行
                    return rewriteResults;
                }
                
                // 检查是否需要 Union Rewrite（物化视图部分分区无效）
                boolean partitionNeedUnion = PartitionCompensator.needUnionRewrite(invalidPartitions, cascadesContext);
                boolean canUnionRewrite = canUnionRewrite(queryPlan,
                        (AsyncMaterializationContext) materializationContext, cascadesContext);
                if (partitionNeedUnion && !canUnionRewrite) {
                    materializationContext.recordFailReason(queryStructInfo,
                            "need compensate union all, but can not, because the query structInfo",
                            () -> String.format("mv partition info is %s, and the query plan is %s",
                                    mtmv.getMvPartitionInfo(), queryPlan.treeString()));
                    return rewriteResults;
                }
                
                // 如果需要 Union Rewrite，执行分区补偿
                if (partitionNeedUnion) {
                    // 在基表上添加过滤条件，只查询无效分区的数据
                    Pair<Plan, Boolean> planAndNeedAddFilterPair =
                            StructInfo.addFilterOnTableScan(queryPlan, invalidPartitions.value(), cascadesContext);
                    if (planAndNeedAddFilterPair == null) {
                        materializationContext.recordFailReason(queryStructInfo,
                                "Add filter to base table fail when union rewrite",
                                () -> String.format("invalidPartitions are %s, queryPlan is %s",
                                        invalidPartitions, queryPlan.treeString()));
                        continue;
                    }
                    if (invalidPartitions.value().isEmpty() || !planAndNeedAddFilterPair.value()) {
                        // 如果无效基表过滤为空或不需要在基表上添加过滤，只需要从物化视图中移除无效分区
                        rewrittenPlan = rewrittenPlan.accept(new PartitionRemover(), invalidPartitions.key());
                    } else {
                        // 对于包含物化视图的重写计划，需要移除无效分区 ID
                        List<Plan> children = Lists.newArrayList(
                                rewrittenPlan.accept(new PartitionRemover(), invalidPartitions.key()),
                                planAndNeedAddFilterPair.key());
                        // Union 查询物化视图和基表的结果
                        rewrittenPlan = new LogicalUnion(Qualifier.ALL,
                                queryPlan.getOutput().stream().map(NamedExpression.class::cast)
                                        .collect(Collectors.toList()),
                                children.stream()
                                        .map(plan -> plan.getOutput().stream()
                                                .map(slot -> (SlotReference) slot.toSlot())
                                                .collect(Collectors.toList()))
                                        .collect(Collectors.toList()),
                                ImmutableList.of(),
                                false,
                                children);
                    }
                }
            }
            
            // 步骤11：表达式规范化（Normalize Expressions）
            List<Slot> rewrittenPlanOutput = rewrittenPlan.getOutput();
            rewrittenPlan = MaterializedViewUtils.normalizeExpressions(rewrittenPlan, queryPlan);
            if (rewrittenPlan == null) {
                // 可能自动添加了虚拟 slot reference
                materializationContext.recordFailReason(queryStructInfo,
                        "RewrittenPlan output logical properties is different with target group",
                        () -> String.format("materialized view rule normalizeExpressions, output size between "
                                        + "origin and rewritten plan is different, rewritten output is %s, "
                                        + "origin output is %s",
                                rewrittenPlanOutput, queryPlan.getOutput()));
                continue;
            }
            
            // 步骤12：合并 Project（Merge Project）
            rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                    childContext -> {
                        Rewriter.getCteChildrenRewriter(childContext,
                                ImmutableList.of(Rewriter.bottomUp(new MergeProjectable()))
                        ).execute();
                        return childContext.getRewritePlan();
                    }, rewrittenPlan, queryPlan, false);
            
            // 步骤13：验证输出（Validate Output）
            if (!isOutputValid(queryPlan, rewrittenPlan)) {
                LogicalProperties logicalProperties = rewrittenPlan.getLogicalProperties();
                materializationContext.recordFailReason(queryStructInfo,
                        "RewrittenPlan output logical properties is different with target group",
                        () -> String.format("planOutput logical"
                                        + " properties = %s,\n groupOutput logical properties = %s",
                                logicalProperties, queryPlan.getLogicalProperties()));
                continue;
            }
            
            // 步骤14：收集表分区信息
            // 需要重新收集，因为重写计划可能包含新的关系（物化视图）
            // 重写计划可能在后续被再次重写，表分区信息用于后续重写
            // 记录新的物化视图关系 ID 到表的映射，用于嵌套重写
            // 在嵌套重写中，会通过物化视图的 StructInfo 获取查询 StructInfo
            // 如果不记录，MaterializedViewUtils.transformToCommonTableId 会失败
            long startTimeMs = TimeUtils.getStartTimeMs();
            try {
                MaterializedViewUtils.collectTableUsedPartitions(rewrittenPlan, cascadesContext);
            } finally {
                SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(cascadesContext.getConnectContext());
                if (summaryProfile != null) {
                    summaryProfile.addCollectTablePartitionTime(TimeUtils.getElapsedTimeMs(startTimeMs));
                }
            }
            
            trySetStatistics(materializationContext, cascadesContext);
            
            // 步骤15：派生操作列（Derive Operative Column）
            // 为物化视图扫描派生操作列信息，用于后续优化
            rewrittenPlan = deriveOperativeColumn(rewrittenPlan, queryStructInfo,
                    materializationContext.getShuttledExprToScanExprMapping(), viewToQuerySlotMapping,
                    materializationContext);
            
            rewriteResults.add(rewrittenPlan);
            
            // 步骤16：记录重写成功
            recordIfRewritten(queryStructInfo.getOriginalPlan(), materializationContext, cascadesContext);
            
            // 如果重写成功，尝试清理物化视图扫描计划缓存，因为它可能被其他重写复用
            materializationContext.clearScanPlan(cascadesContext);
        }
        return rewriteResults;
    }

    // Set materialization context statistics to statementContext for cost estimate later
    // this should be called before MaterializationContext.clearScanPlan because clearScanPlan change the
    // mv scan plan relation id
    private static void trySetStatistics(MaterializationContext context, CascadesContext cascadesContext) {
        Optional<Pair<Id, Statistics>> materializationPlanStatistics = context.getPlanStatistics(cascadesContext);
        if (materializationPlanStatistics.isPresent() && materializationPlanStatistics.get().key() != null) {
            cascadesContext.getStatementContext().addStatistics(materializationPlanStatistics.get().key(),
                    materializationPlanStatistics.get().value());
        }
    }

    /**
     * Not all query after rewritten successfully can compensate union all
     * Such as:
     * mv def sql is as following, partition column is a
     * select a, b, count(*) from t1 group by a, b
     * Query is as following:
     * select b, count(*) from t1 group by b, after rewritten by materialized view successfully
     * If mv part partition is invalid, can not compensate union all, because result is wrong after
     * compensate union all.
     */
    protected boolean canUnionRewrite(Plan queryPlan, AsyncMaterializationContext context,
            CascadesContext cascadesContext) {
        return true;
    }

    /**
     * Check the logical properties of rewritten plan by mv is the same with source plan
     * if same return true, if different return false
     */
    protected boolean isOutputValid(Plan sourcePlan, Plan rewrittenPlan) {
        if (sourcePlan.getGroupExpression().isPresent() && !rewrittenPlan.getLogicalProperties()
                .equals(sourcePlan.getGroupExpression().get().getOwnerGroup().getLogicalProperties())) {
            return false;
        }
        return sourcePlan.getLogicalProperties().equals(rewrittenPlan.getLogicalProperties());
    }

    /**
     * Partition will be pruned in query then add the pruned partitions to select partitions field of
     * catalog relation.
     * Maybe only some partitions is invalid in materialized view, or base table maybe add new partition
     * So we should calc the invalid partition used in query
     * @return the key in pair is mvNeedRemovePartitionNameSet, the value in pair is baseTableNeedUnionPartitionNameSet
     */
    protected Pair<Map<BaseTableInfo, Set<String>>, Map<BaseColInfo, Set<String>>> calcInvalidPartitions(
            Map<List<String>, Set<String>> queryUsedBaseTablePartitionMap,
            Plan rewrittenPlan,
            CascadesContext cascadesContext,
            AsyncMaterializationContext materializationContext)
            throws AnalysisException {
        return PartitionCompensator.calcInvalidPartitions(queryUsedBaseTablePartitionMap, rewrittenPlan,
                materializationContext, cascadesContext);
    }

    /**
    * Query rewrite result may output origin plan , this will cause loop.
    * if return origin plan, need add check hear.
    */
    protected boolean rewriteQueryByViewPreCheck(MatchMode matchMode, StructInfo queryStructInfo,
            StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan,
            MaterializationContext materializationContext, ComparisonResult comparisonResult) {
        if (materializationContext instanceof SyncMaterializationContext
                && queryStructInfo.getBottomPlan() instanceof LogicalOlapScan) {
            LogicalOlapScan olapScan = (LogicalOlapScan) queryStructInfo.getBottomPlan();
            if (olapScan.getSelectedIndexId() != olapScan.getTable().getBaseIndexId()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Rewrite query by view, for aggregate or join rewriting should be different inherit class implementation
     */
    protected Plan rewriteQueryByView(MatchMode matchMode, StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, Plan tempRewritedPlan, MaterializationContext materializationContext,
            CascadesContext cascadesContext) {
        return tempRewritedPlan;
    }

    /**
     * 将查询表达式重写为 MV scan 表达式。
     * 核心流程：1) shuttle 表达式到基表列形式 2) 尝试直接替换 3) 失败时尝试 date_trunc 分区对齐重写
     * 
     * @param targetExpressionMapping MV 表达式映射（MV slot 空间）
     *        key: MV 计划输出的 shuttle 后的表达式（如 user_id#10, date_trunc(create_time#11, 'day')）
     *        value: MV scan 的输出 slot（如 user_id#10, dt#12）
     *        作用：表示 MV 计划中的表达式对应到 MV scan 的哪个输出列
     * 
     * @param targetToSourceMapping MV slot 到查询 slot 的映射（基表列空间）
     *        key: MV 中的 slot（如 user_id#10, create_time#11）
     *        value: 查询中对应的 slot（如 user_id#0, create_time#0），都 shuttle 到了基表列空间
     *        作用：将 MV slot 空间映射到查询 slot 空间（统一到基表列），用于表达式重写时的空间转换
     */
    protected List<Expression> rewriteExpression(List<? extends Expression> sourceExpressionsToWrite, Plan sourcePlan,
            ExpressionMapping targetExpressionMapping, SlotMapping targetToSourceMapping, BitSet sourcePlanBitSet,
            Map<Expression, ExpressionInfo> queryExprToInfoMap, CascadesContext cascadesContext) {
        /* ==================== 示例场景 ====================
         * 基表: orders(order_id, user_id, create_time, amount)
         * MV: SELECT user_id, date_trunc(create_time, 'day') as dt, sum(amount) FROM orders GROUP BY ...
         * 查询: SELECT user_id, sum(amount) FROM orders WHERE create_time >= '2024-01-15 00:00:00' GROUP BY user_id
         * 
         * sourceExpressionsToWrite: [create_time#0 >= '2024-01-15 00:00:00']
         * 
         * targetExpressionMapping (MV 表达式映射，MV slot 空间):
         *   {user_id#10 -> user_id#10, date_trunc(create_time#11, 'day') -> dt#12}
         *   含义：MV 计划中的 user_id#10 对应 MV scan 的 user_id#10，
         *         MV 计划中的 date_trunc(create_time#11, 'day') 对应 MV scan 的 dt#12
         * 
         * targetToSourceMapping (MV slot -> 查询 slot，基表列空间):
         *   {user_id#10 -> user_id#0, create_time#11 -> create_time#0}
         *   含义：将 MV 的 slot 映射到查询的 slot（都统一到基表列空间）
         */
        
        // 步骤1：将查询表达式 shuttle 到基表列形式，便于与 MV 表达式匹配
        // 
        // Shuttle 的作用：将查询计划中的表达式（可能包含别名、中间列）回溯到基表列引用
        // 
        // 例子：
        // 查询计划可能是：
        //   LogicalFilter(create_time_alias#5 >= '2024-01-15')
        //     LogicalProject(create_time as create_time_alias#5)
        //       LogicalOlapScan(orders, create_time#0)
        // 
        // shuttle 前: create_time_alias#5 >= '2024-01-15'  ← 使用的是别名列
        // shuttle 后: create_time#0 >= '2024-01-15'        ← 回溯到基表的原始列
        // 
        // 为什么需要 shuttle：
        // 1. MV 的表达式映射是基于基表列的（如 date_trunc(create_time#11, 'day')）
        // 2. 只有统一到基表列空间，才能正确匹配和替换
        // 3. 查询的表达式可能有别名、JOIN 后的列等，需要回溯到基表列
        // 
        // sourceShuttledExpressions: [create_time#0 >= '2024-01-15 00:00:00']
        List<? extends Expression> sourceShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                sourceExpressionsToWrite, sourcePlan, sourcePlanBitSet);
        
        // 步骤2：构建替换映射，将 targetExpressionMapping 的 key 从 MV slot 空间转换到查询 slot 空间
        // 
        // expressionMappingKeySourceBased（key 已转换到查询 slot 空间的 ExpressionMapping）:
        // 
        // 原始 targetExpressionMapping（MV slot 空间 -> MV scan 输出）:
        //   {user_id#10 -> user_id#10, date_trunc(create_time#11, 'day') -> dt#12}
        // 
        // 经过 keyPermute(targetToSourceMapping) 后，key 中的 slot 被替换：
        //   - user_id#10 -> user_id#0（根据 targetToSourceMapping）
        //   - create_time#11 -> create_time#0（根据 targetToSourceMapping）
        // 
        // 结果 expressionMappingKeySourceBased（查询 slot 空间 -> MV scan 输出）:
        //   {user_id#0 -> user_id#10, date_trunc(create_time#0, 'day') -> dt#12}
        // 
        // 含义：
        //   - key: 查询中的表达式（使用查询的 slot，已统一到基表列空间，如 user_id#0）
        //   - value: MV scan 的输出 slot（如 user_id#10, dt#12）
        // 
        // 作用：用于将查询表达式中的列引用替换为 MV scan 的输出列
        // 例如：查询中的 user_id#0 可以用 MV scan 的 user_id#10 替换
        ExpressionMapping expressionMappingKeySourceBased = targetExpressionMapping.keyPermute(targetToSourceMapping);
        // 将 Multimap 展开为多个 Map（因为一个 key 可能对应多个 value）
        List<Map<Expression, Expression>> flattenExpressionMap = expressionMappingKeySourceBased.flattenMap();
        // 取第一个 Map 作为替换映射（MV 通常是一对一的，所以取第一个即可）
        Map<Expression, Expression> targetToTargetReplacementMappingQueryBased =
                flattenExpressionMap.get(0);

        // 步骤3：收集 MV 中的 date_trunc 表达式，用于分区对齐重写
        // viewExprParamToDateTruncMap: {create_time#0 -> date_trunc(create_time#0, 'day')}
        Map<Expression, DateTrunc> viewExprParamToDateTruncMap = new HashMap<>();
        targetToTargetReplacementMappingQueryBased.keySet().forEach(expr -> {
            if (expr instanceof DateTrunc) {
                viewExprParamToDateTruncMap.put(expr.child(0), (DateTrunc) expr);
            }
        });

        List<Expression> rewrittenExpressions = new ArrayList<>();
        for (int exprIndex = 0; exprIndex < sourceShuttledExpressions.size(); exprIndex++) {
            Expression expressionShuttledToRewrite = sourceShuttledExpressions.get(exprIndex);
            // 字面量直接保留
            if (expressionShuttledToRewrite instanceof Literal) {
                rewrittenExpressions.add(expressionShuttledToRewrite);
                continue;
            }
            // 收集需要重写的 slot
            // 例: {create_time#0}
            final Set<Expression> slotsToRewrite =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof Slot);

            // 处理 Variant 类型的扩展映射
            final Set<SlotReference> variants =
                    expressionShuttledToRewrite.collectToSet(expression -> expression instanceof SlotReference
                            && ((SlotReference) expression).getDataType() instanceof VariantType);
            extendMappingByVariant(variants, targetToTargetReplacementMappingQueryBased);
            
            // 步骤4：尝试直接替换
            // 示例: create_time#0 >= '2024-01-15' 
            // 结果: 替换失败！因为映射中没有 create_time#0，只有 date_trunc(create_time#0, 'day')
            Expression replacedExpression = ExpressionUtils.replace(expressionShuttledToRewrite,
                    targetToTargetReplacementMappingQueryBased);
            Set<Expression> replacedExpressionSlotQueryUsed = replacedExpression.collect(slotsToRewrite::contains);
            
            // 步骤5：如果直接替换失败（还有未替换的 slot），尝试 date_trunc 分区对齐重写
            // 核心思想：检查查询的日期字面量是否恰好在 MV 的 date_trunc 分区边界上
            // 例如: date_trunc('2024-01-15 00:00:00', 'day') = '2024-01-15 00:00:00' ✓ 可以重写
            //       date_trunc('2024-01-15 08:30:00', 'day') = '2024-01-15 00:00:00' ✗ 不相等，无法重写
            if (!replacedExpressionSlotQueryUsed.isEmpty()) {
                // 前置检查：MV 必须有 date_trunc，且表达式必须是比较谓词
                if (viewExprParamToDateTruncMap.isEmpty()
                        || expressionShuttledToRewrite.children().isEmpty()
                        || !(expressionShuttledToRewrite instanceof ComparisonPredicate)) {
                    return ImmutableList.of();
                }
                // queryShuttledExprParam = create_time#0 (查询表达式的左操作数)
                Expression queryShuttledExprParam = expressionShuttledToRewrite.child(0);
                // queryOriginalExpr = create_time#0 >= '2024-01-15 00:00:00' (原始查询表达式)
                Expression queryOriginalExpr = sourceExpressionsToWrite.get(exprIndex);
                // 检查：查询表达式是否有字面量信息，且 MV 有对应的 date_trunc
                if (!queryExprToInfoMap.containsKey(queryOriginalExpr)
                        || !viewExprParamToDateTruncMap.containsKey(queryShuttledExprParam)) {
                    return ImmutableList.of();
                }
                
                // 步骤5.1：从 ExpressionInfo 中获取字面量
                // queryUsedLiteral = '2024-01-15 00:00:00'
                Map<Expression, Expression> datetruncMap = new HashMap<>();
                Literal queryUsedLiteral = queryExprToInfoMap.get(queryOriginalExpr).literal;
                if (!(queryUsedLiteral instanceof DateLiteral)) {
                    return ImmutableList.of();
                }
                // 步骤5.2：将字面量代入 date_trunc 表达式并常量折叠
                // date_trunc('2024-01-15 00:00:00', 'day') -> '2024-01-15 00:00:00'
                datetruncMap.put(queryShuttledExprParam, queryUsedLiteral);
                Expression dateTruncWithLiteral = ExpressionUtils.replace(
                        viewExprParamToDateTruncMap.get(queryShuttledExprParam), datetruncMap);
                Expression foldedExpressionWithLiteral = FoldConstantRuleOnFE.evaluate(dateTruncWithLiteral,
                        new ExpressionRewriteContext(cascadesContext));
                if (!(foldedExpressionWithLiteral instanceof DateLiteral)) {
                    return ImmutableList.of();
                }
                // 步骤5.3：检查 date_trunc 结果是否与原字面量相等（即处于分区边界）
                // '2024-01-15 00:00:00' == '2024-01-15 00:00:00' -> 相等！可以重写
                if (((DateLiteral) foldedExpressionWithLiteral).getDouble() == queryUsedLiteral.getDouble()) {
                    // 步骤5.4：重写成功！create_time#0 >= '2024-01-15' -> dt#12 >= '2024-01-15'
                    replacedExpression = ExpressionUtils.replace(expressionShuttledToRewrite,
                            targetToTargetReplacementMappingQueryBased,
                            viewExprParamToDateTruncMap);
                }
                // 最终检查：是否所有 slot 都已被替换
                if (replacedExpression.anyMatch(slotsToRewrite::contains)) {
                    return ImmutableList.of();
                }
            }
            rewrittenExpressions.add(replacedExpression);
        }
        return rewrittenExpressions;
    }

    /**
     * if query contains variant slot reference, extend the expression mapping for rewrite
     * such as targetToTargetReplacementMappingQueryBased is
     * id#0 -> id#8
     * type#1 -> type#9
     * payload#4 -> payload#10
     * query variants is payload['issue']['number']#20
     * then we can add payload['issue']['number']#20 -> element_at(element_at(payload#10, 'issue'), 'number')
     * to targetToTargetReplacementMappingQueryBased
     * */
    private void extendMappingByVariant(Set<SlotReference> queryVariants,
            Map<Expression, Expression> targetToTargetReplacementMappingQueryBased) {
        if (queryVariants.isEmpty()) {
            return;
        }
        Map<List<String>, Expression> viewNameToExprMap = new HashMap<>();
        for (Map.Entry<Expression, Expression> targetExpressionEntry :
                targetToTargetReplacementMappingQueryBased.entrySet()) {
            if (targetExpressionEntry.getKey() instanceof SlotReference
                    && ((SlotReference) targetExpressionEntry.getKey()).getDataType() instanceof VariantType) {
                SlotReference targetSlotReference = (SlotReference) targetExpressionEntry.getKey();
                List<String> nameIdentifier = new ArrayList<>(targetSlotReference.getQualifier());
                nameIdentifier.add(targetSlotReference.getName());
                nameIdentifier.addAll(targetSlotReference.getSubPath());
                viewNameToExprMap.put(nameIdentifier, targetExpressionEntry.getValue());
            }
        }
        if (viewNameToExprMap.isEmpty()) {
            return;
        }
        Map<List<String>, SlotReference> queryNameAndExpressionMap = new HashMap<>();
        for (SlotReference slotReference : queryVariants) {
            List<String> nameIdentifier = new ArrayList<>(slotReference.getQualifier());
            nameIdentifier.add(slotReference.getName());
            nameIdentifier.addAll(slotReference.getSubPath());
            queryNameAndExpressionMap.put(nameIdentifier, slotReference);
        }
        for (Map.Entry<List<String>, ? extends Expression> queryNameEntry : queryNameAndExpressionMap.entrySet()) {
            Expression minExpr = null;
            List<String> minCompensateName = null;
            for (Map.Entry<List<String>, Expression> entry : viewNameToExprMap.entrySet()) {
                if (!containsAllWithOrder(queryNameEntry.getKey(), entry.getKey())) {
                    continue;
                }
                List<String> removedQueryName = new ArrayList<>(queryNameEntry.getKey());
                removedQueryName.removeAll(entry.getKey());
                if (minCompensateName == null) {
                    minCompensateName = removedQueryName;
                    minExpr = entry.getValue();
                }
                if (removedQueryName.size() < minCompensateName.size()) {
                    minCompensateName = removedQueryName;
                    minExpr = entry.getValue();
                }
            }
            if (minExpr != null) {
                targetToTargetReplacementMappingQueryBased.put(queryNameEntry.getValue(),
                        constructElementAt(minExpr, minCompensateName));
            }
        }
    }

    /**
     * Derive the operative column for materialized view scan, if the operative column in query can be
     * represented by the operative column in materialized view, then set the operative column in
     * materialized view scan, otherwise return the materialized view scan without operative column
     */
    private static Plan deriveOperativeColumn(Plan rewrittenPlan, StructInfo queryStructInfo,
            ExpressionMapping targetExpressionMapping, SlotMapping targetToSourceMapping,
            MaterializationContext materializationContext) {
        ExpressionMapping expressionMappingKeySourceBased = targetExpressionMapping.keyPermute(targetToSourceMapping);
        // target to target replacement expression mapping, because mv is 1:1 so get first element
        List<Map<Expression, Expression>> flattenExpressionMap = expressionMappingKeySourceBased.flattenMap();
        Map<Expression, Expression> targetToTargetReplacementMappingQueryBased =
                flattenExpressionMap.get(0);
        final Multimap<NamedExpression, Slot> slotMapping = ArrayListMultimap.create();
        for (Map.Entry<Expression, Expression> entry : targetToTargetReplacementMappingQueryBased.entrySet()) {
            if (entry.getValue() instanceof Slot) {
                entry.getKey().collect(NamedExpression.class::isInstance).forEach(
                        namedExpression -> slotMapping.put(
                                (NamedExpression) namedExpression, (Slot) entry.getValue()));
            }
        }
        Set<Slot> operativeSlots = new HashSet<>();
        for (CatalogRelation relation : queryStructInfo.getRelations()) {
            List<Slot> relationOperativeSlots = relation.getOperativeSlots();
            if (relationOperativeSlots.isEmpty()) {
                continue;
            }
            for (Slot slot : relationOperativeSlots) {
                Collection<Slot> mvOutputSlots = slotMapping.get(slot);
                if (!mvOutputSlots.isEmpty()) {
                    operativeSlots.addAll(mvOutputSlots);
                }
            }
        }
        return rewrittenPlan.accept(new DefaultPlanRewriter<MaterializationContext>() {
            @Override
            public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, MaterializationContext context) {
                if (context.generateMaterializationIdentifier().equals(olapScan.getTable().getFullQualifiers())) {
                    return olapScan.withOperativeSlots(operativeSlots);
                }
                return super.visitLogicalOlapScan(olapScan, context);
            }
        }, materializationContext);
    }

    private static Expression constructElementAt(Expression target, List<String> atList) {
        Expression elementAt = target;
        for (String at : atList) {
            elementAt = new ElementAt(elementAt, new VarcharLiteral(at));
        }
        return elementAt;
    }

    // source names is contain all target with order or not
    private static boolean containsAllWithOrder(List<String> sourceNames, List<String> targetNames) {
        if (sourceNames.size() < targetNames.size()) {
            return false;
        }
        for (int index = 0; index < targetNames.size(); index++) {
            String sourceName = sourceNames.get(index);
            String targetName = targetNames.get(index);
            if (sourceName == null || targetName == null) {
                return false;
            }
            if (!sourceName.equals(targetName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据查询谓词补偿 MV 谓词，补偿结果基于查询的 slot 空间。
     * 
     * 补偿原则：查询比 MV 更严格时，需要补偿额外的过滤条件
     * 
     * 示例1 - 范围谓词：
     *   MV:  a > 5
     *   查询: a > 10
     *   补偿: a > 10（查询更严格，需要在 MV 结果上加过滤）
     * 
     * 示例2 - 等值谓词：
     *   MV:  a = b
     *   查询: a = b AND c = d
     *   补偿: c = d（查询多了一个等值条件）
     */
    protected SplitPredicate predicatesCompensate(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext
    ) {
        // 步骤1：合并从 join 条件上拉的谓词到 queryStructInfo
        List<Expression> queryPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getQueryExpressions());
        if (!queryPulledUpExpressions.isEmpty()) {
            queryStructInfo = queryStructInfo.withPredicates(
                    queryStructInfo.getPredicates().mergePulledUpPredicates(queryPulledUpExpressions));
        }
        // 步骤2：合并从 join 条件上拉的谓词到 viewStructInfo
        List<Expression> viewPulledUpExpressions = ImmutableList.copyOf(comparisonResult.getViewExpressions());
        if (!viewPulledUpExpressions.isEmpty()) {
            viewStructInfo = viewStructInfo.withPredicates(
                    viewStructInfo.getPredicates().mergePulledUpPredicates(viewPulledUpExpressions));
        }
        // 步骤3：处理 join 类型不同的情况
        // 例如 MV 是 LEFT JOIN，查询是 INNER JOIN，需要检查查询是否有拒绝 NULL 的过滤条件
        Set<Set<Slot>> requireNoNullableViewSlot = comparisonResult.getViewNoNullableSlot();
        if (!requireNoNullableViewSlot.isEmpty()) {
            SlotMapping queryToViewMapping = viewToQuerySlotMapping.inverse();
            // 检查查询谓词是否包含 null reject 条件
            boolean valid = containsNullRejectSlot(requireNoNullableViewSlot,
                    queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping, queryStructInfo,
                    viewStructInfo, cascadesContext);
            if (!valid) {
                // 第一次检查失败，尝试合并所有上拉谓词后再检查
                queryStructInfo = queryStructInfo.withPredicates(queryStructInfo.getPredicates()
                        .mergePulledUpPredicates(comparisonResult.getQueryAllPulledUpExpressions()));
                valid = containsNullRejectSlot(requireNoNullableViewSlot,
                        queryStructInfo.getPredicates().getPulledUpPredicates(), queryToViewMapping,
                        queryStructInfo, viewStructInfo, cascadesContext);
            }
            if (!valid) {
                return SplitPredicate.INVALID_INSTANCE;
            }
        }
        // 步骤4：补偿无法上拉的谓词（如子查询中的谓词）
        Map<Expression, ExpressionInfo> couldNotPulledUpCompensateConjunctions =
                Predicates.compensateCouldNotPullUpPredicates(queryStructInfo, viewStructInfo,
                viewToQuerySlotMapping, comparisonResult);
        if (couldNotPulledUpCompensateConjunctions == null) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        // 步骤5：等值谓词补偿（处理等价类差异）
        final Map<Expression, ExpressionInfo> equalCompensateConjunctions = Predicates.compensateEquivalence(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult);
        // 步骤6：范围谓词补偿（如 a > 5 vs a > 10）
        final Map<Expression, ExpressionInfo> rangeCompensatePredicates =
                Predicates.compensateRangePredicate(queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                comparisonResult, cascadesContext);
        // 步骤7：残差谓词补偿（包括 OR 条件等复杂谓词）
        final Map<Expression, ExpressionInfo> residualCompensatePredicates = Predicates.compensateResidualPredicate(
                queryStructInfo, viewStructInfo, viewToQuerySlotMapping, comparisonResult);
        // 任一补偿失败则整体失败
        if (equalCompensateConjunctions == null || rangeCompensatePredicates == null
                || residualCompensatePredicates == null) {
            return SplitPredicate.INVALID_INSTANCE;
        }
        return SplitPredicate.of(equalCompensateConjunctions, rangeCompensatePredicates, residualCompensatePredicates);
    }

    /**
     * Check the queryPredicates contains the required nullable slot
     */
    private boolean containsNullRejectSlot(Set<Set<Slot>> requireNoNullableViewSlot,
            Set<Expression> queryPredicates,
            SlotMapping queryToViewMapping,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            CascadesContext cascadesContext) {
        Set<Expression> queryPulledUpPredicates = queryPredicates.stream()
                .flatMap(expr -> ExpressionUtils.extractConjunction(expr).stream())
                .map(expr -> {
                    // NOTICE inferNotNull generate Not with isGeneratedIsNotNull = false,
                    //  so, we need set this flag to false before comparison.
                    if (expr instanceof Not) {
                        return ((Not) expr).withGeneratedIsNotNull(false);
                    }
                    return expr;
                })
                .collect(Collectors.toSet());
        Set<Expression> queryNullRejectPredicates =
                ExpressionUtils.inferNotNull(queryPulledUpPredicates, cascadesContext);
        if (queryPulledUpPredicates.containsAll(queryNullRejectPredicates)) {
            // Query has no null reject predicates, return
            return false;
        }
        // Get query null reject predicate slots
        Set<Expression> queryNullRejectSlotSet = new HashSet<>();
        for (Expression queryNullRejectPredicate : queryNullRejectPredicates) {
            Optional<Slot> notNullSlot = TypeUtils.isNotNull(queryNullRejectPredicate);
            if (!notNullSlot.isPresent()) {
                continue;
            }
            queryNullRejectSlotSet.add(notNullSlot.get());
        }
        // query slot need shuttle to use table slot, avoid alias influence
        Set<Expression> queryUsedNeedRejectNullSlotsViewBased = ExpressionUtils.shuttleExpressionWithLineage(
                        new ArrayList<>(queryNullRejectSlotSet), queryStructInfo.getTopPlan(), new BitSet()).stream()
                .map(expr -> ExpressionUtils.replace(expr, queryToViewMapping.toSlotReferenceMap()))
                .collect(Collectors.toSet());
        // view slot need shuttle to use table slot, avoid alias influence
        Set<Set<Slot>> shuttledRequireNoNullableViewSlot = new HashSet<>();
        for (Set<Slot> requireNullableSlots : requireNoNullableViewSlot) {
            shuttledRequireNoNullableViewSlot.add(
                    ExpressionUtils.shuttleExpressionWithLineage(new ArrayList<>(requireNullableSlots),
                                    viewStructInfo.getTopPlan(), new BitSet()).stream().map(Slot.class::cast)
                            .collect(Collectors.toSet()));
        }
        // query pulledUp predicates should have null reject predicates and contains any require noNullable slot
        return shuttledRequireNoNullableViewSlot.stream().noneMatch(viewRequiredNullSlotSet ->
                Sets.intersection(viewRequiredNullSlotSet, queryUsedNeedRejectNullSlotsViewBased).isEmpty());
    }

    /**
     * Decide the match mode
     *
     * @see MatchMode
     */
    private MatchMode decideMatchMode(List<CatalogRelation> queryRelations, List<CatalogRelation> viewRelations,
            CascadesContext cascadesContext) {
        Set<TableId> queryTables = new HashSet<>();
        for (CatalogRelation catalogRelation : queryRelations) {
            queryTables.add(cascadesContext.getStatementContext().getTableId(catalogRelation.getTable()));
        }
        Set<TableId> viewTables = new HashSet<>();
        for (CatalogRelation catalogRelation : viewRelations) {
            viewTables.add(cascadesContext.getStatementContext().getTableId(catalogRelation.getTable()));
        }
        if (queryTables.equals(viewTables)) {
            return MatchMode.COMPLETE;
        }
        if (queryTables.containsAll(viewTables)) {
            return MatchMode.VIEW_PARTIAL;
        }
        if (viewTables.containsAll(queryTables)) {
            return MatchMode.QUERY_PARTIAL;
        }
        return MatchMode.NOT_MATCH;
    }

    /**
     * Check the pattern of query or materializedView is supported or not.
     */
    protected boolean checkQueryPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        if (structInfo.getRelations().isEmpty()) {
            return false;
        }
        return true;
    }

    protected boolean checkMaterializationPattern(StructInfo structInfo, CascadesContext cascadesContext) {
        return checkQueryPattern(structInfo, cascadesContext);
    }

    protected void recordIfRewritten(Plan plan, MaterializationContext context, CascadesContext cascadesContext) {
        context.setSuccess(true);
        cascadesContext.getStatementContext().addMaterializationRewrittenSuccess(
                context.generateMaterializationIdentifier());
        if (plan.getGroupExpression().isPresent()) {
            context.addMatchedGroup(plan.getGroupExpression().get().getOwnerGroup().getGroupId(), true);
        }
    }

    protected boolean checkIfRewritten(Plan plan, MaterializationContext context) {
        return plan.getGroupExpression().isPresent()
                && context.alreadyRewrite(plan.getGroupExpression().get().getOwnerGroup().getGroupId());
    }

    // check mv plan is valid or not, this can use cache for performance
    private boolean isMaterializationValid(Plan queryPlan, CascadesContext cascadesContext,
            MaterializationContext context) {
        if (!context.getStructInfo().isValid()) {
            context.recordFailReason(context.getStructInfo(),
                    "View original struct info is invalid", () -> String.format("view plan is %s",
                            context.getStructInfo().getOriginalPlan().treeString()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("View struct info is invalid, mv identifier is %s,  query plan is %s,"
                                + "view plan is %s",
                        context.generateMaterializationIdentifier(), queryPlan.treeString(),
                        context.getStructInfo().getTopPlan().treeString()));
            }
            return false;
        }
        long materializationId = context.generateMaterializationIdentifier().hashCode();
        Boolean cachedCheckResult = cascadesContext.getMemo().materializationHasChecked(this.getClass(),
                materializationId);
        if (cachedCheckResult == null) {
            // need check in real time
            boolean checkResult = checkMaterializationPattern(context.getStructInfo(), cascadesContext);
            if (!checkResult) {
                context.recordFailReason(context.getStructInfo(),
                        "View struct info is invalid", () -> String.format("view plan is %s",
                                context.getStructInfo().getOriginalPlan().treeString()));
                // tmp to location question
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("View struct info is invalid, mv identifier is %s, query plan is %s,"
                                    + "view plan is %s",
                            context.generateMaterializationIdentifier(), queryPlan.treeString(),
                            context.getStructInfo().getTopPlan().treeString()));
                }
                cascadesContext.getMemo().recordMaterializationCheckResult(this.getClass(), materializationId,
                        false);
                return false;
            } else {
                cascadesContext.getMemo().recordMaterializationCheckResult(this.getClass(),
                        materializationId, true);
            }
        } else if (!cachedCheckResult) {
            context.recordFailReason(context.getStructInfo(),
                    "View struct info is invalid", () -> String.format("view plan is %s",
                            context.getStructInfo().getOriginalPlan().treeString()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("View struct info is invalid, mv identifier is %s, query plan is %s,"
                                + "view plan is %s",
                        context.generateMaterializationIdentifier(), queryPlan.treeString(),
                        context.getStructInfo().getTopPlan().treeString()));
            }
            return false;
        }
        return true;
    }

    /**
     * Query and mv match node
     */
    protected enum MatchMode {
        /**
         * The tables in query are same to the tables in view
         */
        COMPLETE,
        /**
         * The tables in query contains all the tables in view
         */
        VIEW_PARTIAL,
        /**
         * The tables in view contains all the tables in query
         */
        QUERY_PARTIAL,
        /**
         * Except for COMPLETE and VIEW_PARTIAL and QUERY_PARTIAL
         */
        NOT_MATCH
    }

    /**
     * Try rewrite topN node
     */
    protected Plan tryRewriteTopN(LogicalTopN<Plan> queryTopNode, LogicalTopN<Plan> viewTopNode,
            SlotMapping viewToQuerySlotMapping, Plan tmpRwritePlan, StructInfo queryStructInfo,
            StructInfo viewStructInfo, MaterializationContext materializationContext, CascadesContext cascadesContext) {
        if (queryTopNode == null || viewTopNode == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN rewrite fail, queryLimitNode or viewLimitNode is null",
                    () -> String.format("queryTopNode = %s,\n viewTopNode = %s,\n",
                            queryTopNode, viewTopNode));
            return null;
        }
        Pair<Long, Long> limitAndOffset = AbstractMaterializedViewLimitOrTopNRule.rewriteLimitAndOffset(
                Pair.of(queryTopNode.getLimit(), queryTopNode.getOffset()),
                Pair.of(viewTopNode.getLimit(), viewTopNode.getOffset()));
        if (limitAndOffset == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN limit and offset rewrite fail, query topN is not consistent with view topN",
                    () -> String.format("query topN = %s,\n view topN = %s,\n",
                            queryTopNode.treeString(),
                            viewTopNode.treeString()));
            return null;
        }
        // check the order keys of TopN between query and view is consistent
        List<OrderKey> queryOrderKeys = queryTopNode.getOrderKeys();
        List<OrderKey> viewOrderKeys = viewTopNode.getOrderKeys();
        if (queryOrderKeys.size() > viewOrderKeys.size()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN order keys size is bigger than view topN order keys size",
                    () -> String.format("query topN order keys = %s,\n view topN order keys = %s,\n",
                            queryOrderKeys, viewOrderKeys));
            return null;
        }
        List<Expression> queryOrderKeysExpressions = queryOrderKeys.stream()
                .map(OrderKey::getExpr).collect(Collectors.toList());
        List<? extends Expression> queryOrderByExpressionsShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                queryOrderKeysExpressions, queryStructInfo.getTopPlan(), queryStructInfo.getTableBitSet());

        List<OrderKey> queryShuttledOrderKeys = new ArrayList<>();
        for (int i = 0; i < queryOrderKeys.size(); i++) {
            OrderKey queryOrderKey = queryOrderKeys.get(i);
            queryShuttledOrderKeys.add(new OrderKey(queryOrderByExpressionsShuttled.get(i), queryOrderKey.isAsc(),
                    queryOrderKey.isNullFirst()));
        }
        List<OrderKey> viewShuttledOrderKeys = new ArrayList<>();
        List<? extends Expression> viewOrderByExpressionsShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                viewOrderKeys.stream().map(OrderKey::getExpr).collect(Collectors.toList()),
                viewStructInfo.getTopPlan(), new BitSet());
        List<Expression> viewOrderByExpressionsQueryBasedSet = ExpressionUtils.replace(
                viewOrderByExpressionsShuttled.stream().map(Expression.class::cast).collect(Collectors.toList()),
                viewToQuerySlotMapping.toSlotReferenceMap());
        for (int j = 0; j < viewOrderKeys.size(); j++) {
            OrderKey viewOrderKey = viewOrderKeys.get(j);
            viewShuttledOrderKeys.add(new OrderKey(viewOrderByExpressionsQueryBasedSet.get(j), viewOrderKey.isAsc(),
                    viewOrderKey.isNullFirst()));
        }
        if (!MaterializedViewUtils.isPrefixSameFromStart(queryShuttledOrderKeys, viewShuttledOrderKeys)) {
            materializationContext.recordFailReason(queryStructInfo,
                    "view topN order key doesn't match query order key",
                    () -> String.format("queryShuttledOrderKeys = %s,\n viewShuttledOrderKeys = %s,\n",
                            queryShuttledOrderKeys, viewShuttledOrderKeys));
            return null;
        }

        // try to rewrite the order by expressions using the mv scan slot
        List<Expression> rewrittenExpressions = rewriteExpression(queryOrderKeysExpressions,
                queryStructInfo.getTopPlan(), materializationContext.shuttledExprToScanExprMapping,
                viewToQuerySlotMapping, queryStructInfo.getTableBitSet(), ImmutableMap.of(), cascadesContext);
        if (rewrittenExpressions.isEmpty()) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query topN order keys rewrite fail, query topN order keys is not consistent "
                            + "with view topN order keys",
                    () -> String.format("query topN order keys = %s,\n shuttledExprToScanExprMapping = %s,\n",
                            queryOrderKeysExpressions, materializationContext.shuttledExprToScanExprMapping));
            return null;
        }
        List<OrderKey> rewrittenOrderKeys = new ArrayList<>();
        for (int i = 0; i < rewrittenExpressions.size(); i++) {
            OrderKey queryOrderKey = queryOrderKeys.get(i);
            rewrittenOrderKeys.add(new OrderKey(rewrittenExpressions.get(i), queryOrderKey.isAsc(),
                    queryOrderKey.isNullFirst()));
        }
        return new LogicalTopN<>(rewrittenOrderKeys, limitAndOffset.key(), limitAndOffset.value(), tmpRwritePlan);
    }
}
