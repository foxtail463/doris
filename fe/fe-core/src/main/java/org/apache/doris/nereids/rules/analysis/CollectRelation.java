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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.TableFrom;
import org.apache.doris.nereids.analyzer.UnboundDictionarySink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.AutoCloseSessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Rule to bind relations in query plan.
 */
public class CollectRelation implements AnalysisRuleFactory {

    private static final Logger LOG = LogManager.getLogger(CollectRelation.class);

    private boolean firstLevel;

    public CollectRelation(boolean firstLevel) {
        this.firstLevel = firstLevel;
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // should collect table from cte first to fill collect all cte name to avoid collect wrong table.
                logicalCTE()
                        .thenApply(ctx -> {
                            ctx.cascadesContext.setCteContext(collectFromCte(ctx.root, ctx.cascadesContext));
                            return null;
                        })
                        .toRule(RuleType.COLLECT_TABLE_FROM_CTE),
                unboundRelation()
                        .thenApply(this::collectFromUnboundRelation)
                        .toRule(RuleType.COLLECT_TABLE_FROM_RELATION),
                unboundLogicalSink()
                        .thenApply(this::collectFromUnboundSink)
                        .toRule(RuleType.COLLECT_TABLE_FROM_SINK),
                any().whenNot(UnboundRelation.class::isInstance)
                        .whenNot(UnboundTableSink.class::isInstance)
                        .thenApply(this::collectFromAny)
                        .toRule(RuleType.COLLECT_TABLE_FROM_OTHER)
        );
    }

    /**
     * register and store CTEs in CTEContext
     */
    private CTEContext collectFromCte(
            LogicalCTE<Plan> logicalCTE, CascadesContext cascadesContext) {
        CTEContext outerCteCtx = cascadesContext.getCteContext();
        List<LogicalSubQueryAlias<Plan>> aliasQueries = logicalCTE.getAliasQueries();
        for (LogicalSubQueryAlias<Plan> aliasQuery : aliasQueries) {
            // we should use a chain to ensure visible of cte
            LogicalPlan parsedCtePlan = (LogicalPlan) aliasQuery.child();
            CascadesContext innerCascadesCtx = CascadesContext.newContextWithCteContext(
                    cascadesContext, parsedCtePlan, outerCteCtx);
            innerCascadesCtx.newTableCollector(true).collect();
            LogicalPlan analyzedCtePlan = (LogicalPlan) innerCascadesCtx.getRewritePlan();
            // cteId is not used in CollectTable stage
            CTEId cteId = new CTEId(0);
            LogicalSubQueryAlias<Plan> logicalSubQueryAlias =
                    aliasQuery.withChildren(ImmutableList.of(analyzedCtePlan));
            outerCteCtx = new CTEContext(cteId, logicalSubQueryAlias, outerCteCtx);
            outerCteCtx.setAnalyzedPlan(logicalSubQueryAlias);
        }
        return outerCteCtx;
    }

    private Plan collectFromAny(MatchingContext<Plan> ctx) {
        for (Expression expression : ctx.root.getExpressions()) {
            expression.foreach(e -> {
                if (e instanceof SubqueryExpr) {
                    SubqueryExpr subqueryExpr = (SubqueryExpr) e;
                    CascadesContext subqueryContext = CascadesContext.newContextWithCteContext(
                            ctx.cascadesContext, subqueryExpr.getQueryPlan(), ctx.cteContext);
                    subqueryContext.keepOrShowPlanProcess(ctx.cascadesContext.showPlanProcess(),
                            () -> subqueryContext.newTableCollector(true).collect());
                    ctx.cascadesContext.addPlanProcesses(subqueryContext.getPlanProcesses());
                }
            });
        }
        return null;
    }

    private Plan collectFromUnboundSink(MatchingContext<UnboundLogicalSink<Plan>> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1:
                // table
                // Use current database name from catalog.
            case 2:
                // db.table
                // Use database name from table name parts.
            case 3:
                // catalog.db.table
                // Use catalog and database name from name parts.
                collectFromUnboundRelation(ctx.cascadesContext, nameParts, TableFrom.INSERT_TARGET, Optional.empty());
                return null;
            default:
                throw new IllegalStateException("Insert target name is invalid.");
        }
    }

    private Plan collectFromUnboundRelation(MatchingContext<UnboundRelation> ctx) {
        List<String> nameParts = ctx.root.getNameParts();
        switch (nameParts.size()) {
            case 1:
                // table
                // Use current database name from catalog.
            case 2:
                // db.table
                // Use database name from table name parts.
            case 3:
                // catalog.db.table
                // Use catalog and database name from name parts.
                collectFromUnboundRelation(ctx.cascadesContext, nameParts, TableFrom.QUERY, Optional.of(ctx.root));
                return null;
            default:
                throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
        }
    }

    /**
     * 从未绑定的关系（UnboundRelation）中收集表信息。
     *
     * 该方法是表收集的核心逻辑，负责：
     * 1. 识别并跳过已分析的 CTE
     * 2. 获取表的元数据并缓存到 StatementContext
     * 3. 根据表的来源（查询、插入目标、MTMV）执行不同的收集逻辑
     * 4. 递归处理视图，收集视图中的表
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：检查是否是 CTE 名称
     * - 如果表名是单部分名称（如 "cte1"），检查是否是 CTE
     * - 如果是 CTE 且已分析过，直接返回（CTE 的表收集在 collectFromCte 中完成）
     *
     * 步骤2：获取表的限定名并解析表对象
     * - 使用 RelationUtil.getQualifierName() 将 nameParts 转换为完整的限定名
     *   （格式：catalog.db.table 或 db.table 或 table）
     * - 特殊处理：如果是 UnboundDictionarySink，直接从 sink 获取字典表
     * - 否则：从 StatementContext 获取并缓存表对象
     * - 如果是第一级表（firstLevel == true），添加到 oneLevelTables
     *
     * 步骤3：根据表的来源执行不同的收集逻辑
     * - TableFrom.QUERY（查询中直接出现的表）：
     *   * 收集同步物化视图候选（collectMVCandidates）
     *   * 收集异步物化视图候选（collectMTMVCandidates）
     * - TableFrom.INSERT_TARGET（INSERT 语句的目标表）：
     *   * 收集插入目标表的 schema，用于后续的插入操作
     * - TableFrom.MTMV（MTMV 相关的表）：
     *   * 在 collectMTMVCandidates 中处理，收集 MTMV 的基表
     *
     * 步骤4：递归处理视图
     * - 如果表是视图（View），解析视图定义并递归收集视图中的表
     * - 视图中的表会通过 parseAndCollectFromView 递归处理
     *
     * ==================== 使用场景 ====================
     *
     * 示例1：查询中的表
     * ```sql
     * SELECT * FROM orders o JOIN users u ON o.user_id = u.id
     * ```
     * - orders 和 users 都是 TableFrom.QUERY
     * - 会收集这两个表的同步和异步物化视图候选
     *
     * 示例2：INSERT 语句
     * ```sql
     * INSERT INTO target_table SELECT * FROM source_table
     * ```
     * - target_table 是 TableFrom.INSERT_TARGET，收集其 schema
     * - source_table 是 TableFrom.QUERY，收集物化视图候选
     *
     * 示例3：视图
     * ```sql
     * SELECT * FROM my_view
     * ```
     * - my_view 是 View，会解析视图定义，递归收集视图中的表
     *
     * 示例4：MTMV 相关表
     * - 当收集到 MTMV 候选时，会递归收集 MTMV 的基表（TableFrom.MTMV）
     *
     * ==================== 注意事项 ====================
     *
     * 1. CTE 的处理：CTE 的表收集在 collectFromCte 中完成，这里只检查是否已分析
     * 2. 表的缓存：通过 StatementContext.getAndCacheTable() 缓存表对象，避免重复解析
     * 3. 一级表 vs 多级表：
     *    - firstLevel == true：只收集直接出现在查询中的表（一级表）
     *    - firstLevel == false：递归收集所有表（包括视图中的表、MTMV 的基表等）
     * 4. 物化视图候选收集：只有在存在 MaterializedViewHook 时才收集
     * 5. 视图的递归处理：视图定义会被解析为逻辑计划，然后递归收集其中的表
     *
     * @param cascadesContext 优化器上下文
     * @param nameParts 表名的各个部分（如 ["catalog", "db", "table"] 或 ["table"]）
     * @param tableFrom 表的来源（QUERY、INSERT_TARGET、MTMV）
     * @param unboundRelation 未绑定的关系对象（可选，用于查询中的表）
     */
    private void collectFromUnboundRelation(CascadesContext cascadesContext,
            List<String> nameParts, TableFrom tableFrom, Optional<UnboundRelation> unboundRelation) {
        // 步骤1：检查是否是 CTE 名称
        // 如果表名是单部分名称（如 "cte1"），检查是否是 CTE
        // 如果是 CTE 且已分析过，直接返回（CTE 的表收集在 collectFromCte 中完成）
        if (nameParts.size() == 1) {
            String tableName = nameParts.get(0);
            CTEContext cteContext = cascadesContext.getCteContext().findCTEContext(tableName).orElse(null);
            if (cteContext != null) {
                Optional<LogicalPlan> analyzedCte = cteContext.getAnalyzedCTEPlan(tableName);
                if (analyzedCte.isPresent()) {
                    return;
                }
            }
        }

        // 步骤2：获取表的限定名并解析表对象
        // 将 nameParts 转换为完整的限定名（格式：catalog.db.table 或 db.table 或 table）
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(), nameParts);
        TableIf table;
        
        // 特殊处理：如果是 UnboundDictionarySink，直接从 sink 获取字典表
        if (cascadesContext.getRewritePlan() instanceof UnboundDictionarySink) {
            table = ((UnboundDictionarySink) cascadesContext.getRewritePlan()).getDictionary();
        } else {
            // 从 StatementContext 获取并缓存表对象
            StatementContext statementContext = cascadesContext.getConnectContext().getStatementContext();
            table = statementContext.getAndCacheTable(tableQualifier, tableFrom, unboundRelation);
            
            // 如果是第一级表（直接出现在查询中的表），添加到 oneLevelTables
            if (firstLevel) {
                statementContext.getOneLevelTables().put(tableQualifier, table);
            }
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("collect table {} from {}", nameParts, tableFrom);
        }
        
        // 步骤3：根据表的来源执行不同的收集逻辑
        // TableFrom.QUERY：收集物化视图候选
        if (tableFrom == TableFrom.QUERY) {
            // 收集同步物化视图候选（OlapTable 的物化索引）
            collectMVCandidates(table, cascadesContext);
            // 收集异步物化视图候选（MTMV）
            collectMTMVCandidates(table, cascadesContext);
        }
        
        // TableFrom.INSERT_TARGET：收集插入目标表的 schema
        if (tableFrom == TableFrom.INSERT_TARGET) {
            if (!cascadesContext.getStatementContext().getInsertTargetSchema().isEmpty()) {
                LOG.warn("collect insert target table '{}' more than once.", tableQualifier);
            }
            // 清空并重新设置插入目标表的 schema
            cascadesContext.getStatementContext().getInsertTargetSchema().clear();
            cascadesContext.getStatementContext().getInsertTargetSchema().addAll(table.getFullSchema());
        }
        
        // 步骤4：递归处理视图
        // 如果表是视图，解析视图定义并递归收集视图中的表
        if (table instanceof View) {
            parseAndCollectFromView(tableQualifier, (View) table, cascadesContext);
        }
    }

    /**
     * 收集同步物化视图候选（MaterializedIndexMeta）。
     *
     * 同步物化视图是 OlapTable 的物化索引（Materialized Index），与基表数据同步更新。
     * 该方法收集表中所有非基础索引的物化索引，作为后续物化视图重写的候选。
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：检查是否需要收集
     * - 检查是否存在 MaterializedViewHook（通过 MaterializedViewUtils.containMaterializedViewHook）
     * - 只有存在 Hook 时才收集（Hook 会在后续阶段初始化 MaterializationContext）
     *
     * 步骤2：收集物化索引
     * - 获取 OlapTable 的所有可见索引（getVisibleIndexIdToMeta）
     * - 排除基础索引（baseIndexId），基础索引是表的原始数据，不是物化视图
     * - 将所有非基础索引添加到 candidateMVs
     *
     * ==================== 使用场景 ====================
     *
     * 示例：创建同步物化视图
     * ```sql
     * CREATE TABLE orders (id INT, user_id INT, amount DECIMAL);
     * CREATE MATERIALIZED VIEW mv1 AS SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
     * ```
     *
     * 查询时：
     * ```sql
     * SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
     * ```
     *
     * 执行过程：
     * 1. 收集 orders 表时，发现 orders 有物化索引 mv1
     * 2. 将 mv1 的 MaterializedIndexMeta 添加到 candidateMVs
     * 3. 后续在物化视图重写阶段，会尝试使用 mv1 来重写查询
     *
     * ==================== 注意事项 ====================
     *
     * 1. 同步物化视图 vs 异步物化视图：
     *    - 同步物化视图（MaterializedIndexMeta）：与基表数据同步，通过 CREATE MATERIALIZED VIEW 创建，
     *      作为 OlapTable 的物化索引存在，数据与基表同步更新
     *    - 异步物化视图（MTMV）：独立表，通过 CREATE MATERIALIZED VIEW 创建，数据异步刷新
     * 2. 基础索引不会被收集：baseIndexId 是表的原始数据，不是物化视图
     * 3. 只有存在 MaterializedViewHook 时才收集：避免不必要的开销
     *
     * @param tableIf 要收集物化视图候选的表
     * @param cascadesContext 优化器上下文
     */
    private void collectMVCandidates(TableIf tableIf, CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        // 检查是否需要收集物化视图候选（只有存在 MaterializedViewHook 时才收集）
        boolean shouldCollect = MaterializedViewUtils.containMaterializedViewHook(statementContext);
        
        // 只有 OlapTable 才有物化索引
        if (shouldCollect && tableIf instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) tableIf;
            long baseIndexId = olapTable.getBaseIndexId();
            
            // 遍历所有可见索引，排除基础索引，收集物化索引
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getVisibleIndexIdToMeta().entrySet()) {
                if (entry.getKey() != baseIndexId) {
                    // 将非基础索引的物化索引添加到候选列表
                    statementContext.getCandidateMVs().add(entry.getValue());
                }
            }
        }
    }

    /**
     * 收集异步物化视图候选（MTMV）。
     *
     * 异步物化视图是独立的表，通过 CREATE MATERIALIZED VIEW 创建，数据异步刷新。
     * 该方法收集与当前表相关的所有 MTMV 候选，并递归收集 MTMV 的基表。
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：检查是否需要收集
     * - 检查是否存在 MaterializedViewHook
     * - 只有存在 Hook 时才收集
     *
     * 步骤2：查找相关的 MTMV 候选
     * - 通过 MTMVRelationManager.getCandidateMTMVs() 查找与当前表相关的 MTMV
     * - 该方法会查找所有以当前表为基表的 MTMV
     *
     * 步骤3：收集 MTMV 并递归收集基表
     * - 将 MTMV 添加到 candidateMTMVs
     * - 将 MTMV 添加到 mtmvRelatedTables（标记为 MTMV 相关表）
     * - 获取 MTMV 的读锁（readMvLock），保护元数据访问
     * - 遍历 MTMV 的所有基表（getRelation().getBaseTables()）
     * - 递归收集每个基表（通过 getAndCacheTable，标记为 TableFrom.MTMV）
     * - 释放读锁（readMvUnlock）
     *
     * ==================== 使用场景 ====================
     *
     * 示例：创建异步物化视图
     * ```sql
     * CREATE MATERIALIZED VIEW mv1 AS
     * SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
     * ```
     *
     * 查询时：
     * ```sql
     * SELECT user_id, SUM(amount) FROM orders GROUP BY user_id;
     * ```
     *
     * 执行过程：
     * 1. 收集 orders 表时，发现 mv1 以 orders 为基表
     * 2. 将 mv1 添加到 candidateMTMVs 和 mtmvRelatedTables
     * 3. 递归收集 mv1 的基表 orders（标记为 TableFrom.MTMV）
     * 4. 后续在物化视图重写阶段，会尝试使用 mv1 来重写查询
     *
     * 示例：多层 MTMV
     * ```sql
     * CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM orders;
     * CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM mv1;
     * ```
     *
     * 查询 orders 时：
     * 1. 收集到 mv1（以 orders 为基表）
     * 2. 收集到 mv2（以 mv1 为基表，间接以 orders 为基表）
     * 3. 递归收集 mv1 和 mv2 的所有基表
     *
     * ==================== 注意事项 ====================
     *
     * 1. MTMV 的基表需要递归收集：
     *    - MTMV 可能依赖其他表，这些表也需要被收集和锁定
     *    - 基表标记为 TableFrom.MTMV，表示它们不属于查询本身，但可能被 MTMV 重写使用
     * 2. 使用读锁保护元数据访问：
     *    - readMvLock() / readMvUnlock() 保护 MTMV 的元数据（如基表列表）不被并发修改
     * 3. 异常处理：
     *    - 如果基表收集失败，记录警告但不中断流程
     *    - 确保 finally 块中释放锁
     * 4. 基表的有效性检查：
     *    - 只收集有效的基表（baseTableInfo.isValid()）
     * 5. MTMV 候选 vs 可用 MTMV：
     *    - 这里收集的是候选 MTMV（可能与查询相关）
     *    - 后续在 InitMaterializationContextHook 中会进一步筛选可用的 MTMV（考虑分区、一致性等）
     *
     * @param table 要收集物化视图候选的表
     * @param cascadesContext 优化器上下文
     */
    private void collectMTMVCandidates(TableIf table, CascadesContext cascadesContext) {
        // 检查是否需要收集物化视图候选（只有存在 MaterializedViewHook 时才收集）
        boolean shouldCollect = MaterializedViewUtils.containMaterializedViewHook(
                cascadesContext.getStatementContext());
        
        if (shouldCollect) {
            boolean isDebugEnabled = LOG.isDebugEnabled();
            
            // 步骤2：查找与当前表相关的 MTMV 候选
            // 通过 MTMVRelationManager 查找所有以当前表为基表的 MTMV
            Set<MTMV> mtmvSet = Env.getCurrentEnv().getMtmvService().getRelationManager()
                    .getCandidateMTMVs(Lists.newArrayList(new BaseTableInfo(table)));
            
            if (isDebugEnabled) {
                LOG.debug("table {} related mv set is {}", new BaseTableInfo(table), mtmvSet);
            }
            
            // 步骤3：收集 MTMV 并递归收集基表
            for (MTMV mtmv : mtmvSet) {
                // 将 MTMV 添加到候选列表
                cascadesContext.getStatementContext().getCandidateMTMVs().add(mtmv);
                // 将 MTMV 添加到相关表列表（标记为 MTMV 相关表）
                cascadesContext.getStatementContext().getMtmvRelatedTables().put(mtmv.getFullQualifiers(), mtmv);
                
                // 获取 MTMV 的读锁，保护元数据访问
                mtmv.readMvLock();
                try {
                    // 遍历 MTMV 的所有基表，递归收集
                    for (BaseTableInfo baseTableInfo : mtmv.getRelation().getBaseTables()) {
                        // 只收集有效的基表
                        if (!baseTableInfo.isValid()) {
                            continue;
                        }
                        
                        if (isDebugEnabled) {
                            LOG.debug("mtmv {} related base table include {}", new BaseTableInfo(mtmv), baseTableInfo);
                        }
                        
                        try {
                            // 递归收集基表，标记为 TableFrom.MTMV
                            // 这些表不属于查询本身，但可能被 MTMV 重写使用
                            cascadesContext.getStatementContext().getAndCacheTable(baseTableInfo.toList(),
                                    TableFrom.MTMV, Optional.empty());
                        } catch (AnalysisException exception) {
                            // 如果基表收集失败，记录警告但不中断流程
                            LOG.warn("mtmv related base table get err, related table is {}",
                                    baseTableInfo.toList(), exception);
                        }
                    }
                } finally {
                    // 确保释放读锁
                    mtmv.readMvUnlock();
                }
            }
        }
    }

    protected void parseAndCollectFromView(List<String> tableQualifier, View view, CascadesContext parentContext) {
        Pair<String, Map<String, String>> viewInfo = parentContext.getStatementContext()
                .getAndCacheViewInfo(tableQualifier, view);
        LogicalPlan parsedViewPlan;
        try (AutoCloseSessionVariable autoClose = new AutoCloseSessionVariable(parentContext.getConnectContext(),
                viewInfo.second)) {
            parsedViewPlan = new NereidsParser().parseSingle(viewInfo.first);
        }
        if (parsedViewPlan instanceof UnboundResultSink) {
            parsedViewPlan = (LogicalPlan) ((UnboundResultSink<?>) parsedViewPlan).child();
        }
        CascadesContext viewContext = CascadesContext.initContext(
                parentContext.getStatementContext(), parsedViewPlan, PhysicalProperties.ANY);
        viewContext.keepOrShowPlanProcess(parentContext.showPlanProcess(),
                () -> viewContext.newTableCollector(false).collect());
        parentContext.addPlanProcesses(viewContext.getPlanProcesses());
    }
}
