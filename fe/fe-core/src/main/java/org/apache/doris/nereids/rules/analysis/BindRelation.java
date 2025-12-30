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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.SchemaTable.SchemaColumn;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalView;
import org.apache.doris.datasource.doris.RemoteDorisExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.TableFrom;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.SqlDialectHelper;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.QuantileUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEsScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHudiScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSchemaScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTestScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.AutoCloseSessionVariable;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 绑定关系规则：将未绑定的关系（UnboundRelation）绑定到实际的表对象。
 * <p>
 * 该规则是分析阶段的核心规则之一，主要功能包括：
 * <ol>
 *   <li><b>表名解析</b>：解析表名（支持 catalog.db.table、db.table、table 三种格式）</li>
 *   <li><b>表对象查找</b>：从 Catalog 中查找对应的表对象</li>
 *   <li><b>CTE 处理</b>：检查是否为 CTE（Common Table Expression）</li>
 *   <li><b>逻辑计划创建</b>：根据表类型创建相应的逻辑计划节点：
 *     <ul>
 *       <li>OLAP 表 → LogicalOlapScan</li>
 *       <li>视图 → LogicalView</li>
 *       <li>外部表 → LogicalFileScan / LogicalJdbcScan / LogicalEsScan 等</li>
 *       <li>CTE → LogicalCTEConsumer</li>
 *     </ul>
 *   </li>
 *   <li><b>特殊处理</b>：
 *     <ul>
 *       <li>随机分布聚合表：自动添加 LogicalAggregate</li>
 *       <li>删除标记：自动添加删除标记过滤器</li>
 *       <li>分区：处理分区裁剪</li>
 *       <li>物化视图：支持直接扫描物化视图</li>
 *     </ul>
 *   </li>
 * </ol>
 * <p>
 * 执行时机：在分析阶段的最早阶段执行，因为后续的表达式绑定（BindExpression）需要知道表的列信息。
 *
 * @see UnboundRelation 未绑定的关系节点
 * @see LogicalOlapScan OLAP 表扫描节点
 * @see LogicalView 视图节点
 * @see LogicalFileScan 文件扫描节点（外部表）
 */
public class BindRelation extends OneAnalysisRuleFactory {
    private static final Logger LOG = LogManager.getLogger(StatementContext.class);

    /**
     * 构建绑定关系的规则。
     * <p>
     * 规则匹配模式：匹配所有 UnboundRelation 节点
     * <p>
     * 规则执行逻辑：
     * <ol>
     *   <li>调用 doBindRelation() 执行绑定</li>
     *   <li>如果绑定成功（不再是 Unbound），立即初始化输出和分配 Slot ID</li>
     *   <li>Slot ID 按照表在 SQL 中出现的顺序递增，确保 ID 的一致性</li>
     * </ol>
     *
     * @return 绑定关系的规则
     */
    @Override
    public Rule build() {
        return unboundRelation().thenApply(ctx -> {
            Plan plan = doBindRelation(ctx);
            if (!(plan instanceof Unbound) && plan instanceof Relation) {
                // 立即初始化输出并分配 Slot ID，确保 Slot ID 按照表在 SQL 中出现的顺序递增
                // init output and allocate slot id immediately, so that the slot id increase
                // in the order in which the table appears.
                LogicalProperties logicalProperties = plan.getLogicalProperties();
                logicalProperties.getOutput();
            }
            return plan;
        }).toRule(RuleType.BINDING_RELATION);
    }

    /**
     * 执行关系绑定。
     * <p>
     * 根据表名的组成部分数量，选择不同的绑定策略：
     * <ul>
     *   <li><b>1 部分</b>（如 "t1"）：使用当前数据库，调用 bindWithCurrentDb()</li>
     *   <li><b>2 部分</b>（如 "db1.t1"）：使用指定的数据库，调用 bind()</li>
     *   <li><b>3 部分</b>（如 "ctl1.db1.t1"）：使用指定的 Catalog 和数据库，调用 bind()</li>
     * </ul>
     *
     * @param ctx 匹配上下文，包含 UnboundRelation 节点和 CascadesContext
     * @return 绑定后的逻辑计划节点
     * @throws IllegalStateException 如果表名格式无效（不是 1、2 或 3 部分）
     */
    private Plan doBindRelation(MatchingContext<UnboundRelation> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1: {
                // 单部分表名：table
                // 使用当前数据库名称
                // table
                // Use current database name from catalog.
                return bindWithCurrentDb(ctx.cascadesContext, ctx.root);
            }
            case 2:
                // 两部分表名：db.table
                // 使用表名中的数据库名称
                // db.table
                // Use database name from table name parts.
            case 3: {
                // 三部分表名：catalog.db.table
                // 使用表名中的 Catalog 和数据库名称
                // catalog.db.table
                // Use catalog and database name from name parts.
                return bind(ctx.cascadesContext, ctx.root);
            }
            default:
                throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
        }
    }

    /**
     * 使用当前数据库绑定关系。
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>检查 CTE</b>：首先检查表名是否为 CTE（Common Table Expression）名称
     *     <ul>
     *       <li>如果是 CTE，创建 LogicalCTEConsumer 节点</li>
     *       <li>如果 CTE 已分析，使用已分析的 CTE 计划</li>
     *     </ul>
     *   </li>
     *   <li><b>查找表对象</b>：从当前数据库的 Catalog 中查找表对象</li>
     *   <li><b>创建逻辑计划</b>：根据表类型调用 getLogicalPlan() 创建相应的逻辑计划节点</li>
     *   <li><b>处理 Leading Join</b>：如果启用了 Leading Join 提示，记录关系 ID 和表名的映射</li>
     * </ol>
     *
     * @param cascadesContext 级联上下文
     * @param unboundRelation 未绑定的关系节点
     * @return 绑定后的逻辑计划节点（LogicalCTEConsumer 或各种 Scan 节点）
     */
    private LogicalPlan bindWithCurrentDb(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        String tableName = unboundRelation.getNameParts().get(0);
        // 检查是否为 CTE 名称
        // check if it is a CTE's name
        CTEContext cteContext = cascadesContext.getCteContext().findCTEContext(tableName).orElse(null);
        if (cteContext != null) {
            Optional<LogicalPlan> analyzedCte = cteContext.getAnalyzedCTEPlan(tableName);
            if (analyzedCte.isPresent()) {
                // 创建 CTE Consumer 节点
                LogicalCTEConsumer consumer = new LogicalCTEConsumer(unboundRelation.getRelationId(),
                        cteContext.getCteId(), tableName, analyzedCte.get());
                // 处理 Leading Join 提示
                if (cascadesContext.isLeadingJoin()) {
                    LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
                    leading.putRelationIdAndTableName(Pair.of(consumer.getRelationId(), tableName));
                    leading.getRelationIdToScanMap().put(consumer.getRelationId(), consumer);
                }
                return consumer;
            }
        }
        // 获取表的限定名（catalog.db.table）
        List<String> tableQualifier = RelationUtil.getQualifierName(
                cascadesContext.getConnectContext(), unboundRelation.getNameParts());
        // 从 StatementContext 中获取并缓存表对象
        TableIf table = cascadesContext.getStatementContext().getAndCacheTable(tableQualifier, TableFrom.QUERY,
                Optional.of(unboundRelation));

        // 根据表类型创建逻辑计划
        LogicalPlan scan = getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
        // 处理 Leading Join 提示
        if (cascadesContext.isLeadingJoin()) {
            LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
            leading.putRelationIdAndTableName(Pair.of(unboundRelation.getRelationId(), tableName));
            leading.getRelationIdToScanMap().put(unboundRelation.getRelationId(), scan);
        }
        return scan;
    }

    /**
     * 使用指定的 Catalog 和数据库绑定关系。
     * <p>
     * 与 bindWithCurrentDb() 的区别：
     * <ul>
     *   <li>不需要检查 CTE（因为 CTE 只能在当前查询作用域内）</li>
     *   <li>使用表名中指定的 Catalog 和数据库，而不是当前数据库</li>
     * </ul>
     *
     * @param cascadesContext 级联上下文
     * @param unboundRelation 未绑定的关系节点
     * @return 绑定后的逻辑计划节点
     */
    private LogicalPlan bind(CascadesContext cascadesContext, UnboundRelation unboundRelation) {
        // 获取表的限定名（从表名中提取 catalog.db.table）
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                unboundRelation.getNameParts());
        // 从 StatementContext 中获取并缓存表对象
        TableIf table = cascadesContext.getStatementContext().getAndCacheTable(tableQualifier, TableFrom.QUERY,
                Optional.of(unboundRelation));
        // 根据表类型创建逻辑计划
        return getLogicalPlan(table, unboundRelation, tableQualifier, cascadesContext);
    }

    /**
     * 创建 OLAP 表的扫描节点。
     * <p>
     * 该方法根据不同的场景创建不同类型的 LogicalOlapScan 节点，并可能添加额外的处理节点。
     * <p>
     * 主要处理流程：
     * <ol>
     *   <li><b>获取分区和 Tablet 信息</b>：从 UnboundRelation 中提取分区 ID 和 Tablet ID</li>
     *   <li><b>创建扫描节点</b>：根据是否有分区 ID 和物化视图名称创建不同的扫描节点</li>
     *   <li><b>处理手动指定的 Tablet</b>：如果指定了 Tablet ID，标记为手动指定</li>
     *   <li><b>处理预聚合状态</b>：根据 Hint 强制开启预聚合</li>
     *   <li><b>处理随机分布聚合表</b>：如果是随机分布聚合表，添加 LogicalAggregate 节点</li>
     *   <li><b>处理删除标记</b>：对于其他类型的表，添加删除标记过滤器（如果需要）</li>
     * </ol>
     *
     * @param table OLAP 表对象
     * @param unboundRelation 未绑定的关系节点，包含分区、Tablet、Hint 等信息
     * @param qualifier 表的限定名（catalog.db）
     * @param cascadesContext 级联上下文
     * @return OLAP 扫描节点，可能包含上层的 LogicalAggregate 或 LogicalFilter
     */
    private LogicalPlan makeOlapScan(TableIf table, UnboundRelation unboundRelation, List<String> qualifier,
            CascadesContext cascadesContext) {
        LogicalOlapScan scan;
        
        // 步骤 1：获取分区 ID 列表（如果 SQL 中指定了分区）
        List<Long> partIds = getPartitionIds(table, unboundRelation, qualifier);
        // 步骤 2：获取 Tablet ID 列表（如果 SQL 中指定了 Tablet）
        List<Long> tabletIds = unboundRelation.getTabletIds();
        
        // 步骤 3：根据是否有分区 ID 和物化视图名称创建不同的扫描节点
        if (!CollectionUtils.isEmpty(partIds) && !unboundRelation.getIndexName().isPresent()) {
            // 场景 3.1：指定了分区，但没有指定物化视图
            // 创建带分区裁剪的扫描节点
            scan = new LogicalOlapScan(unboundRelation.getRelationId(),
                    (OlapTable) table, qualifier, partIds,
                    tabletIds, unboundRelation.getHints(),
                    unboundRelation.getTableSample(),
                    ImmutableList.of());
        } else {
            // 场景 3.2：没有指定分区，或指定了物化视图
            Optional<String> indexName = unboundRelation.getIndexName();
            // For direct mv scan.
            if (indexName.isPresent()) {
                // 场景 3.2.1：指定了物化视图名称，直接扫描物化视图
                OlapTable olapTable = (OlapTable) table;
                // 根据物化视图名称查找物化视图 ID
                Long indexId = olapTable.getIndexIdByName(indexName.get());
                if (indexId == null) {
                    throw new AnalysisException("Table " + olapTable.getName()
                        + " doesn't have materialized view " + indexName.get());
                }
                // 对于 Merge-On-Write 或聚合表，直接扫描物化视图时需要关闭预聚合
                // 因为物化视图本身已经是聚合后的结果
                PreAggStatus preAggStatus = olapTable.isDupKeysOrMergeOnWrite() ? PreAggStatus.unset()
                        : PreAggStatus.off("For direct index scan on mor/agg.");

                // 创建直接扫描物化视图的节点
                // 如果没有指定分区，使用表的所有分区；否则使用指定的分区
                scan = new LogicalOlapScan(unboundRelation.getRelationId(),
                    (OlapTable) table, qualifier, tabletIds,
                    CollectionUtils.isEmpty(partIds) ? ((OlapTable) table).getPartitionIds() : partIds, indexId,
                    preAggStatus, CollectionUtils.isEmpty(partIds) ? ImmutableList.of() : partIds,
                    unboundRelation.getHints(), unboundRelation.getTableSample(), ImmutableList.of());
            } else {
                // 场景 3.2.2：没有指定分区，也没有指定物化视图
                // 创建普通的全表扫描节点
                scan = new LogicalOlapScan(unboundRelation.getRelationId(),
                    (OlapTable) table, qualifier, tabletIds, unboundRelation.getHints(),
                    unboundRelation.getTableSample(), ImmutableList.of());
            }
        }
        
        // 步骤 4：处理手动指定的 Tablet ID
        // 如果 SQL 中明确指定了 Tablet ID（如用于调试或特定场景），需要标记为手动指定
        if (!tabletIds.isEmpty()) {
            // This tabletIds is set manually, so need to set specifiedTabletIds
            scan = scan.withManuallySpecifiedTabletIds(tabletIds);
        }
        
        // 步骤 5：处理预聚合状态（通过 Hint 强制开启）
        // 如果 SQL 中使用了 Hint 强制开启预聚合，直接返回开启预聚合的扫描节点
        if (cascadesContext.getStatementContext().isHintForcePreAggOn()) {
            return scan.withPreAggStatus(PreAggStatus.on());
        }
        
        // 步骤 6：处理随机分布聚合表
        // 随机分布聚合表（AGG_KEYS + RANDOM 分布）需要添加 LogicalAggregate 节点来实现预聚合
        if (needGenerateLogicalAggForRandomDistAggTable(scan)) {
            // it's a random distribution agg table
            // add agg on olap scan
            return preAggForRandomDistribution(scan);
        } else {
            // 步骤 7：处理其他类型的表（Duplicate、Unique、Hash 分布）
            // 对于这些表，如果表有删除标记列，需要添加删除标记过滤器
            // it's a duplicate, unique or hash distribution agg table
            // add delete sign filter on olap scan if needed
            return checkAndAddDeleteSignFilter(scan, ConnectContext.get(), (OlapTable) table);
        }
    }

    private boolean needGenerateLogicalAggForRandomDistAggTable(LogicalOlapScan olapScan) {
        if (ConnectContext.get() != null && ConnectContext.get().getState() != null
                && ConnectContext.get().getState().isQuery()) {
            // we only need to add an agg node for query, and should not do it for deleting
            // from random distributed table. see https://github.com/apache/doris/pull/37985 for more info
            OlapTable olapTable = olapScan.getTable();
            KeysType keysType = olapTable.getKeysType();
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            return keysType == KeysType.AGG_KEYS
                    && distributionInfo.getType() == DistributionInfo.DistributionInfoType.RANDOM;
        } else {
            return false;
        }
    }

    /**
     * 为随机分布聚合表添加 LogicalAggregate 节点以实现预聚合。
     * <p>
     * 随机分布聚合表的特点：
     * <ul>
     *   <li>KeysType 为 AGG_KEYS</li>
     *   <li>DistributionInfo 类型为 RANDOM</li>
     * </ul>
     * <p>
     * 处理逻辑：
     * <ol>
     *   <li>遍历表的所有列</li>
     *   <li>对于 Key 列：添加到 GROUP BY 和输出表达式</li>
     *   <li>对于非 Key 列：根据聚合类型（SUM、MAX、MIN 等）生成聚合函数</li>
     *   <li>创建 LogicalAggregate 节点，包含 GROUP BY 表达式和聚合函数</li>
     * </ol>
     * <p>
     * 注意：此方法只在查询场景下使用，删除操作不会添加聚合节点。
     *
     * @param olapScan OLAP 扫描计划
     * @return 重写后的计划（LogicalAggregate(LogicalOlapScan)）
     */
    private LogicalPlan preAggForRandomDistribution(LogicalOlapScan olapScan) {
        OlapTable olapTable = olapScan.getTable();
        List<Slot> childOutputSlots = olapScan.computeOutput();
        List<Expression> groupByExpressions = new ArrayList<>();
        List<NamedExpression> outputExpressions = new ArrayList<>();
        List<Column> columns = olapScan.isIndexSelected()
                ? olapTable.getSchemaByIndexId(olapScan.getSelectedIndexId())
                : olapTable.getBaseSchema();

        IdGenerator<ExprId> exprIdGenerator = StatementScopeIdGenerator.getExprIdGenerator();
        for (Column col : columns) {
            // use exist slot in the plan
            SlotReference slot = SlotReference.fromColumn(
                    exprIdGenerator.getNextId(), olapTable, col, olapScan.qualified()
            );
            ExprId exprId = slot.getExprId();
            for (Slot childSlot : childOutputSlots) {
                if (childSlot instanceof SlotReference && childSlot.getName().equals(col.getName())) {
                    exprId = childSlot.getExprId();
                    slot = slot.withExprId(exprId);
                    break;
                }
            }
            if (col.isKey()) {
                groupByExpressions.add(slot);
                outputExpressions.add(slot);
            } else {
                Expression function = generateAggFunction(slot, col);
                // DO NOT rewrite
                if (function == null) {
                    return olapScan;
                }
                Alias alias = new Alias(exprIdGenerator.getNextId(), ImmutableList.of(function),
                        col.getName(), olapScan.qualified(), true);
                outputExpressions.add(alias);
            }
        }
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(groupByExpressions, outputExpressions,
                olapScan);
        return aggregate;
    }

    /**
     * 根据列的聚合类型生成聚合函数。
     * <p>
     * 支持的聚合类型：
     * <ul>
     *   <li>SUM → Sum(slot)</li>
     *   <li>MAX → Max(slot)</li>
     *   <li>MIN → Min(slot)</li>
     *   <li>HLL_UNION → HllUnion(slot)</li>
     *   <li>BITMAP_UNION → BitmapUnion(slot)</li>
     *   <li>QUANTILE_UNION → QuantileUnion(slot)</li>
     *   <li>GENERIC → 根据 AggStateType 生成对应的 UNION 函数</li>
     * </ul>
     * <p>
     * 如果列类型不是 AggStateType，返回 null（表示不需要聚合）。
     *
     * @param slot 列的 Slot 引用
     * @param column 列对象，包含聚合类型信息
     * @return 生成的聚合函数表达式，如果不需要聚合则返回 null
     */
    private Expression generateAggFunction(SlotReference slot, Column column) {
        AggregateType aggregateType = column.getAggregationType();
        switch (aggregateType) {
            case SUM:
                return new Sum(slot);
            case MAX:
                return new Max(slot);
            case MIN:
                return new Min(slot);
            case HLL_UNION:
                return new HllUnion(slot);
            case BITMAP_UNION:
                return new BitmapUnion(slot);
            case QUANTILE_UNION:
                return new QuantileUnion(slot);
            case GENERIC:
                Type type = column.getType();
                if (!type.isAggStateType()) {
                    return null;
                }
                AggStateType aggState = (AggStateType) type;
                // use AGGREGATE_FUNCTION_UNION to aggregate multiple agg_state into one
                String funcName = aggState.getFunctionName() + AggCombinerFunctionBuilder.UNION_SUFFIX;
                FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
                FunctionBuilder builder = functionRegistry.findFunctionBuilder(funcName, slot);
                return builder.build(funcName, ImmutableList.of(slot)).first;
            default:
                return null;
        }
    }

    /**
     * 如果需要，在 OLAP 扫描节点上添加删除标记过滤器。
     * <p>
     * 删除标记（DELETE_SIGN）是 Doris 用于标记删除行的隐藏列。
     * <p>
     * 添加条件：
     * <ul>
     *   <li>表有删除标记列（hasDeleteSign()）</li>
     *   <li>未跳过删除标记（!skipDeleteSign()）</li>
     *   <li>未显示隐藏列（!showHiddenColumns()）</li>
     * </ul>
     * <p>
     * 处理逻辑：
     * <ol>
     *   <li>从扫描节点的输出中找到删除标记 Slot</li>
     *   <li>创建过滤条件：DELETE_SIGN = 0（只保留未删除的行）</li>
     *   <li>对于非 Unique Key Merge-On-Write 表，关闭预聚合（因为删除标记作为过滤条件）</li>
     *   <li>创建 LogicalFilter 节点包装扫描节点</li>
     * </ol>
     *
     * @param scan OLAP 扫描节点
     * @param connectContext 连接上下文
     * @param olapTable OLAP 表对象
     * @return 如果添加了过滤器，返回 LogicalFilter(LogicalOlapScan)，否则返回原扫描节点
     */
    public static LogicalPlan checkAndAddDeleteSignFilter(LogicalOlapScan scan, ConnectContext connectContext,
            OlapTable olapTable) {
        if (!Util.showHiddenColumns() && scan.getTable().hasDeleteSign()
                && !connectContext.getSessionVariable().skipDeleteSign()) {
            // table qualifier is catalog.db.table, we make db.table.column
            Slot deleteSlot = null;
            for (Slot slot : scan.getOutput()) {
                if (slot.getName().equals(Column.DELETE_SIGN)) {
                    deleteSlot = slot;
                    break;
                }
            }
            Preconditions.checkArgument(deleteSlot != null);
            Expression conjunct = new EqualTo(deleteSlot, new TinyIntLiteral((byte) 0));
            if (!olapTable.getEnableUniqueKeyMergeOnWrite()) {
                scan = scan.withPreAggStatus(PreAggStatus.off(
                        Column.DELETE_SIGN + " is used as conjuncts."));
            }
            return new LogicalFilter<>(ImmutableSet.of(conjunct), scan);
        }
        return scan;
    }

    private Optional<LogicalPlan> handleMetaTable(TableIf table, UnboundRelation unboundRelation,
            List<String> qualifiedTableName) {
        Optional<TableValuedFunction> tvf = table.getSysTableFunction(
                qualifiedTableName.get(0), qualifiedTableName.get(1), qualifiedTableName.get(2));
        if (tvf.isPresent()) {
            return Optional.of(new LogicalTVFRelation(unboundRelation.getRelationId(), tvf.get(), ImmutableList.of()));
        }
        return Optional.empty();
    }

    /**
     * 根据表类型创建相应的逻辑计划节点。
     * <p>
     * 这是绑定关系的核心方法，根据不同的表类型创建不同的逻辑计划节点：
     * <ul>
     *   <li><b>OLAP / MATERIALIZED_VIEW</b> → LogicalOlapScan</li>
     *   <li><b>VIEW</b> → LogicalView（需要解析视图 SQL）</li>
     *   <li><b>HMS_EXTERNAL_TABLE</b> → LogicalFileScan / LogicalHudiScan（如果是 Hudi 表）</li>
     *   <li><b>ICEBERG_EXTERNAL_TABLE</b> → LogicalFileScan</li>
     *   <li><b>JDBC_EXTERNAL_TABLE / JDBC</b> → LogicalJdbcScan</li>
     *   <li><b>ODBC</b> → LogicalOdbcScan</li>
     *   <li><b>ES_EXTERNAL_TABLE / ELASTICSEARCH</b> → LogicalEsScan</li>
     *   <li><b>SCHEMA</b> → LogicalSchemaScan（可能需要添加聚合）</li>
     *   <li><b>其他外部表</b> → LogicalFileScan</li>
     * </ul>
     * <p>
     * 特殊处理：
     * <ul>
     *   <li><b>视图</b>：解析视图 SQL，创建子上下文进行分析</li>
     *   <li><b>Schema 表</b>：某些 Schema 表需要添加聚合节点</li>
     *   <li><b>SQL 缓存</b>：记录使用的表，用于 SQL 缓存判断</li>
     * </ul>
     *
     * @param table 表对象
     * @param unboundRelation 未绑定的关系节点
     * @param qualifiedTableName 表的限定名（catalog.db.table）
     * @param cascadesContext 级联上下文
     * @return 对应的逻辑计划节点
     * @throws AnalysisException 如果表类型不支持
     */
    private LogicalPlan getLogicalPlan(TableIf table, UnboundRelation unboundRelation,
                                       List<String> qualifiedTableName, CascadesContext cascadesContext) {
        // 为 CREATE VIEW 语句记录表名在 SQL 中的位置，用于替换为 ctl.db.tableName
        // for create view stmt replace tableName to ctl.db.tableName
        unboundRelation.getIndexInSqlString().ifPresent(pair -> {
            StatementContext statementContext = cascadesContext.getStatementContext();
            statementContext.addIndexInSqlToString(pair,
                    Utils.qualifiedNameWithBackquote(qualifiedTableName));
        });

        // 处理元表（如 "table_name$partitions"）
        // 限定名格式应为 "ctl.db.tbl$partitions"
        // Handle meta table like "table_name$partitions"
        // qualifiedTableName should be like "ctl.db.tbl$partitions"
        Optional<LogicalPlan> logicalPlan = handleMetaTable(table, unboundRelation, qualifiedTableName);
        if (logicalPlan.isPresent()) {
            return logicalPlan.get();
        }

        List<String> qualifierWithoutTableName = qualifiedTableName.subList(0, qualifiedTableName.size() - 1);
        cascadesContext.getStatementContext().loadSnapshots(
                unboundRelation.getTableSnapshot(),
                Optional.ofNullable(unboundRelation.getScanParams()));
        boolean isView = false;
        try {
            switch (table.getType()) {
                case OLAP:
                case MATERIALIZED_VIEW:
                    return makeOlapScan(table, unboundRelation, qualifierWithoutTableName, cascadesContext);
                case VIEW:
                    View view = (View) table;
                    isView = true;
                    Plan viewBody = parseAndAnalyzeDorisView(view, qualifiedTableName, cascadesContext);
                    LogicalView<Plan> logicalView = new LogicalView<>(view, viewBody);
                    return new LogicalSubQueryAlias<>(qualifiedTableName, logicalView);
                case HMS_EXTERNAL_TABLE:
                    HMSExternalTable hmsTable = (HMSExternalTable) table;
                    if (Config.enable_query_hive_views && hmsTable.isView()) {
                        isView = true;
                        String hiveCatalog = hmsTable.getCatalog().getName();
                        String hiveDb = hmsTable.getDatabase().getFullName();
                        String ddlSql = hmsTable.getViewText();
                        Plan hiveViewPlan = parseAndAnalyzeExternalView(
                                hmsTable, hiveCatalog, hiveDb, ddlSql, cascadesContext);
                        return new LogicalSubQueryAlias<>(qualifiedTableName, hiveViewPlan);
                    }
                    if (hmsTable.getDlaType() == DLAType.HUDI) {
                        LogicalHudiScan hudiScan = new LogicalHudiScan(unboundRelation.getRelationId(), hmsTable,
                                qualifierWithoutTableName, ImmutableList.of(), Optional.empty(),
                                unboundRelation.getTableSample(), unboundRelation.getTableSnapshot(),
                                Optional.empty());
                        hudiScan = hudiScan.withScanParams(
                                hmsTable, Optional.ofNullable(unboundRelation.getScanParams()));
                        return hudiScan;
                    } else {
                        return new LogicalFileScan(unboundRelation.getRelationId(), (HMSExternalTable) table,
                                qualifierWithoutTableName,
                                ImmutableList.of(),
                                unboundRelation.getTableSample(),
                                unboundRelation.getTableSnapshot(),
                                Optional.ofNullable(unboundRelation.getScanParams()), Optional.empty());
                    }
                case ICEBERG_EXTERNAL_TABLE:
                    IcebergExternalTable icebergExternalTable = (IcebergExternalTable) table;
                    if (Config.enable_query_iceberg_views && icebergExternalTable.isView()) {
                        Optional<TableSnapshot> tableSnapshot = unboundRelation.getTableSnapshot();
                        if (tableSnapshot.isPresent()) {
                            // iceberg view not supported with snapshot time/version travel
                            // note that enable_fallback_to_original_planner should be set with false
                            // or else this exception will not be thrown
                            // because legacy planner will retry and thrown other exception
                            throw new UnsupportedOperationException(
                                "iceberg view not supported with snapshot time/version travel");
                        }
                        isView = true;
                        String icebergCatalog = icebergExternalTable.getCatalog().getName();
                        String icebergDb = icebergExternalTable.getDatabase().getFullName();
                        String ddlSql = icebergExternalTable.getViewText();
                        Plan icebergViewPlan = parseAndAnalyzeExternalView(icebergExternalTable,
                                icebergCatalog, icebergDb, ddlSql, cascadesContext);
                        return new LogicalSubQueryAlias<>(qualifiedTableName, icebergViewPlan);
                    }
                    if (icebergExternalTable.isView()) {
                        throw new UnsupportedOperationException(
                            "please set enable_query_iceberg_views=true to enable query iceberg views");
                    }
                    return new LogicalFileScan(unboundRelation.getRelationId(), (ExternalTable) table,
                        qualifierWithoutTableName, ImmutableList.of(),
                        unboundRelation.getTableSample(),
                        unboundRelation.getTableSnapshot(),
                        Optional.ofNullable(unboundRelation.getScanParams()), Optional.empty());
                case PAIMON_EXTERNAL_TABLE:
                case MAX_COMPUTE_EXTERNAL_TABLE:
                case TRINO_CONNECTOR_EXTERNAL_TABLE:
                case LAKESOUl_EXTERNAL_TABLE:
                    return new LogicalFileScan(unboundRelation.getRelationId(), (ExternalTable) table,
                            qualifierWithoutTableName, ImmutableList.of(),
                            unboundRelation.getTableSample(),
                            unboundRelation.getTableSnapshot(),
                            Optional.ofNullable(unboundRelation.getScanParams()), Optional.empty());
                case DORIS_EXTERNAL_TABLE:
                    ConnectContext ctx = cascadesContext.getConnectContext();
                    RemoteDorisExternalTable externalTable = (RemoteDorisExternalTable) table;
                    if (!externalTable.useArrowFlight()) {
                        if (!ctx.getSessionVariable().isEnableNereidsDistributePlanner()) {
                            // use isEnableNereidsDistributePlanner instead of canUseNereidsDistributePlanner
                            // because it cannot work in explain command
                            throw new AnalysisException("query remote doris only support NereidsDistributePlanner"
                                    + " when catalog use_arrow_flight is false");
                        }
                        OlapTable olapTable = externalTable.getOlapTable();
                        return makeOlapScan(olapTable, unboundRelation, qualifierWithoutTableName, cascadesContext);
                    }
                    return new LogicalFileScan(unboundRelation.getRelationId(), (ExternalTable) table,
                            qualifierWithoutTableName, ImmutableList.of(),
                            unboundRelation.getTableSample(),
                            unboundRelation.getTableSnapshot(),
                            Optional.ofNullable(unboundRelation.getScanParams()), Optional.empty());
                case SCHEMA:
                    LogicalSchemaScan schemaScan = new LogicalSchemaScan(unboundRelation.getRelationId(), table,
                            qualifierWithoutTableName);
                    LogicalSubQueryAlias<LogicalSchemaScan> subQueryAlias = new LogicalSubQueryAlias<>(
                            qualifiedTableName, schemaScan);
                    TableIf tableIf = schemaScan.getTable();
                    // may be ExternalInfoSchemaTable
                    if (!(tableIf instanceof SchemaTable)) {
                        return subQueryAlias;
                    }
                    SchemaTable schemaTable = (SchemaTable) tableIf;
                    if (!schemaTable.shouldAddAgg()) {
                        return subQueryAlias;
                    }
                    List<Expression> groupByExpressions = new ArrayList<>();
                    List<NamedExpression> outputExpressions = new ArrayList<>();
                    List<Slot> output = subQueryAlias.getOutput();
                    for (Slot slot : output) {
                        SchemaColumn column = (SchemaColumn) schemaTable.getColumn(slot.getName());
                        if (column.isKey()) {
                            groupByExpressions.add(slot);
                            outputExpressions.add(slot);
                        } else {
                            Expression function = SchemaTable.generateAggBySchemaAggType(slot,
                                    column.getSchemaTableAggregateType());
                            Alias alias = new Alias(StatementScopeIdGenerator.newExprId(),
                                    ImmutableList.of(function),
                                    slot.getName(), qualifiedTableName, true);
                            outputExpressions.add(alias);
                        }
                    }
                    return new LogicalAggregate<>(groupByExpressions, outputExpressions, subQueryAlias);
                case JDBC_EXTERNAL_TABLE:
                case JDBC:
                    return new LogicalJdbcScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case ODBC:
                    return new LogicalOdbcScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case ES_EXTERNAL_TABLE:
                case ELASTICSEARCH:
                    return new LogicalEsScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                case TEST_EXTERNAL_TABLE:
                    return new LogicalTestScan(unboundRelation.getRelationId(), table, qualifierWithoutTableName);
                default:
                    throw new AnalysisException("Unsupported tableType " + table.getType());
            }
        } finally {
            if (!isView) {
                Optional<SqlCacheContext> sqlCacheContextOpt
                        = cascadesContext.getStatementContext().getSqlCacheContext();
                if (sqlCacheContextOpt.isPresent()) {
                    SqlCacheContext sqlCacheContext = sqlCacheContextOpt.get();
                    try {
                        if (table.isTemporary()) {
                            sqlCacheContext.setHasUnsupportedTables(true);
                        } else if (table instanceof OlapTable) {
                            sqlCacheContext.addUsedTable(table);
                        } else if (table instanceof HMSExternalTable
                                && cascadesContext.getConnectContext().getSessionVariable().enableHiveSqlCache) {
                            sqlCacheContext.addUsedTable(table);
                        } else {
                            sqlCacheContext.setHasUnsupportedTables(true);
                        }
                    } catch (Throwable t) {
                        sqlCacheContext.setHasUnsupportedTables(true);
                    }
                }
            }
        }
    }

    private Plan parseAndAnalyzeExternalView(
            ExternalTable table, String externalCatalog, String externalDb,
            String ddlSql, CascadesContext cascadesContext) {
        ConnectContext ctx = cascadesContext.getConnectContext();
        String previousCatalog = ctx.getCurrentCatalog().getName();
        String previousDb = ctx.getDatabase();
        String convertedSql = SqlDialectHelper.convertSqlByDialect(ddlSql, ctx.getSessionVariable());
        // change catalog and db to external catalog and db,
        // so that we can parse and analyze the view sql in external context.
        ctx.changeDefaultCatalog(externalCatalog);
        ctx.setDatabase(externalDb);
        try {
            return new LogicalView<>(new ExternalView(table, ddlSql),
                    parseAndAnalyzeView(table, convertedSql, cascadesContext));
        } finally {
            // restore catalog and db in connect context
            ctx.changeDefaultCatalog(previousCatalog);
            ctx.setDatabase(previousDb);
        }
    }

    private Plan parseAndAnalyzeDorisView(View view, List<String> tableQualifier, CascadesContext parentContext) {
        Pair<String, Map<String, String>> viewInfo = parentContext.getStatementContext()
                .getAndCacheViewInfo(tableQualifier, view);
        Plan analyzedPlan;
        Map<String, String> currentSessionVars =
                parentContext.getConnectContext().getSessionVariable().getAffectQueryResultInPlanVariables();
        try (AutoCloseSessionVariable autoClose = new AutoCloseSessionVariable(parentContext.getConnectContext(),
                viewInfo.second)) {
            analyzedPlan = parseAndAnalyzeView(view, viewInfo.first, parentContext);
            if (!SessionVarGuardRewriter.checkSessionVariablesMatch(currentSessionVars, viewInfo.second)) {
                SessionVarGuardRewriter exprRewriter = new SessionVarGuardRewriter(viewInfo.second, parentContext);
                analyzedPlan = SessionVarGuardRewriter.rewritePlanTree(exprRewriter, analyzedPlan);
            }
        }
        return analyzedPlan;
    }

    /**
     * 解析和分析视图 SQL。
     * <p>
     * 处理流程：
     * <ol>
     *   <li><b>记录视图 SQL</b>：将视图 DDL SQL 添加到 StatementContext</li>
     *   <li><b>SQL 缓存处理</b>：如果是临时视图，标记为不支持 SQL 缓存；否则记录使用的视图</li>
     *   <li><b>解析视图 SQL</b>：使用 NereidsParser 解析视图 SQL</li>
     *   <li><b>创建子上下文</b>：为视图创建独立的 CascadesContext</li>
     *   <li><b>分析视图</b>：在子上下文中执行分析（只绑定关系，不进行完整分析）</li>
     *   <li><b>返回视图计划</b>：返回分析后的视图计划</li>
     * </ol>
     * <p>
     * 注意：
     * <ul>
     *   <li>视图的分析是递归的，视图内部可能引用其他表或视图</li>
     *   <li>视图使用独立的上下文，避免与外部查询的计划冲突</li>
     *   <li>需要移除其他 Memo 中的 Group Expression，避免 Group ID 冲突</li>
     * </ul>
     *
     * @param view 视图对象
     * @param ddlSql 视图的 DDL SQL
     * @param parentContext 父级联上下文
     * @return 分析后的视图逻辑计划
     */
    private Plan parseAndAnalyzeView(TableIf view, String ddlSql, CascadesContext parentContext) {
        parentContext.getStatementContext().addViewDdlSql(ddlSql);
        Optional<SqlCacheContext> sqlCacheContextOpt = parentContext.getStatementContext().getSqlCacheContext();
        if (sqlCacheContextOpt.isPresent()) {
            SqlCacheContext sqlCacheContext = sqlCacheContextOpt.get();
            try {
                if (view.isTemporary()) {
                    sqlCacheContext.setHasUnsupportedTables(true);
                } else {
                    try {
                        sqlCacheContext.addUsedView(view, ddlSql);
                    } catch (Throwable t) {
                        sqlCacheContext.setHasUnsupportedTables(true);
                    }
                }
            } catch (Throwable t) {
                sqlCacheContext.setHasUnsupportedTables(true);
            }
        }
        LogicalPlan parsedViewPlan = new NereidsParser().parseSingle(ddlSql);
        // TODO: use a good to do this, such as eliminate UnboundResultSink
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContext = CascadesContext.initContext(
                parentContext.getStatementContext(), parsedViewPlan, PhysicalProperties.ANY);
        viewContext.keepOrShowPlanProcess(parentContext.showPlanProcess(), () -> {
            viewContext.newAnalyzer().analyze();
        });
        parentContext.addPlanProcesses(viewContext.getPlanProcesses());
        // we should remove all group expression of the plan which in other memo, so the groupId would not conflict
        return viewContext.getRewritePlan();
    }

    /**
     * 根据分区名称获取分区 ID 列表。
     * <p>
     * 处理流程：
     * <ol>
     *   <li>如果未指定分区，返回空列表</li>
     *   <li>检查表是否为托管表（只有 OLAP 表支持分区查询）</li>
     *   <li>遍历分区名称，从表中查找对应的分区对象</li>
     *   <li>如果分区不存在，抛出 AnalysisException</li>
     *   <li>返回分区 ID 列表</li>
     * </ol>
     * <p>
     * 注意：
     * <ul>
     *   <li>只有 OLAP 表支持按分区查询</li>
     *   <li>支持临时分区（isTempPart）</li>
     * </ul>
     *
     * @param t 表对象
     * @param unboundRelation 未绑定的关系节点（包含分区名称列表）
     * @param qualifier 表的限定名（用于错误消息）
     * @return 分区 ID 列表
     * @throws AnalysisException 如果表不是 OLAP 表，或分区不存在
     */
    private List<Long> getPartitionIds(TableIf t, UnboundRelation unboundRelation, List<String> qualifier) {
        List<String> parts = unboundRelation.getPartNames();
        if (CollectionUtils.isEmpty(parts)) {
            return ImmutableList.of();
        }
        if (!t.isManagedTable()) {
            throw new AnalysisException(String.format(
                    "Only OLAP table is support select by partition for now,"
                            + "Table: %s is not OLAP table", t.getName()));
        }
        return parts.stream().map(name -> {
            Partition part = ((OlapTable) t).getPartition(name, unboundRelation.isTempPart());
            if (part == null) {
                List<String> qualified = Lists.newArrayList();
                if (!CollectionUtils.isEmpty(qualifier)) {
                    qualified.addAll(qualifier);
                }
                qualified.add(unboundRelation.getTableName());
                throw new AnalysisException(String.format("Partition: %s is not exists on table %s",
                        name, String.join(".", qualified)));
            }
            return part.getId();
        }).collect(ImmutableList.toImmutableList());
    }

}
