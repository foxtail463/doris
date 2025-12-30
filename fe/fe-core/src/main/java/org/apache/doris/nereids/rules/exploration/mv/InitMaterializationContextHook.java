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

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContextUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * If enable query rewrite with mv, should init materialization context after analyze
 */
public class InitMaterializationContextHook implements PlannerHook {

    public static final Logger LOG = LogManager.getLogger(InitMaterializationContextHook.class);
    public static final InitMaterializationContextHook INSTANCE = new InitMaterializationContextHook();

    /**
     * 在 rewrite 阶段之后调用。收集查询表的分区信息用于 MV 匹配，
     * 然后为每个可用的 MV 初始化 MaterializationContext。
     */
    @Override
    public void afterRewrite(CascadesContext cascadesContext) {
        // 收集查询表使用的分区信息，必须在 initMaterializationContext 之前执行，
        // 因为 MV 匹配时需要比较分区版本
        if (cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
            // Pre-MV rewrite 模式：从临时计划中收集分区信息
            // 
            // 为什么需要从临时计划收集？
            // 1. 临时计划（tmpPlanForMvRewrite）是在 RBO 阶段保存的，经过了预处理：
            //    - CTE 重写（CTE 内联或展开）
            //    - 表达式规范化（ExpressionNormalizationAndOptimization）
            //    - 子查询去嵌套（Subquery unnesting）
            //    这些预处理使得计划结构更稳定，更适合进行物化视图匹配
            // 
            // 2. Pre-MV Rewrite 阶段会使用临时计划进行物化视图重写：
            //    - preMaterializedViewRewrite() 会遍历 tmpPlanForMvRewrite
            //    - 对每个临时计划进行物化视图重写
            //    - 因此分区信息也应该从临时计划中收集，以保持一致性
            // 
            // 3. 如果从 rewrite plan 收集，可能会收集到错误的分区信息：
            //    - rewrite plan 可能还没有经过 Pre-MV Rewrite 的优化
            //    - 计划结构可能与最终用于 MV 匹配的计划不一致
            //    - 导致分区匹配失败或错误
            // 
            // 示例场景：
            // - 查询包含 CTE：WITH cte AS (SELECT * FROM orders) SELECT * FROM cte
            // - 临时计划：CTE 已内联，结构为 SELECT * FROM orders
            // - rewrite plan：可能还包含 CTE 节点，结构不一致
            // - 从临时计划收集：能正确识别 orders 表的分区
            // - 从 rewrite plan 收集：可能无法正确识别分区（因为 CTE 节点）
            for (Plan plan : cascadesContext.getStatementContext().getTmpPlanForMvRewrite()) {
                MaterializedViewUtils.collectTableUsedPartitions(plan, cascadesContext);
            }
        } else {
            // 普通模式：从当前 rewrite plan 中收集分区信息
            // 
            // 为什么可以直接从 rewrite plan 收集？
            // 1. 普通模式下没有 Pre-MV Rewrite，不需要临时计划
            // 2. rewrite plan 就是最终用于 MV 匹配的计划
            // 3. 分区信息直接从 rewrite plan 收集即可
            MaterializedViewUtils.collectTableUsedPartitions(cascadesContext.getRewritePlan(), cascadesContext);
        }
        // 记录耗时用于性能分析
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsCollectTablePartitionFinishTime(TimeUtils.getStartTimeMs());
        }
        // 为每个候选 MV 构建 MaterializationContext
        initMaterializationContext(cascadesContext);
    }

    @VisibleForTesting
    public void initMaterializationContext(CascadesContext cascadesContext) {
        if (!cascadesContext.getConnectContext().getSessionVariable().isEnableMaterializedViewRewrite()) {
            return;
        }
        doInitMaterializationContext(cascadesContext);
    }

    /**
     * 初始化物化视图上下文，为后续 MV 重写做准备。
     * 会分别创建同步 MV 和异步 MV 的 MaterializationContext。
     */
    protected void doInitMaterializationContext(CascadesContext cascadesContext) {
        // 调试模式下跳过 MV 重写
        if (cascadesContext.getConnectContext().getSessionVariable().isInDebugMode()) {
            LOG.info("MaterializationContext init return because is in debug mode, current queryId is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return;
        }
        // 短路查询（如点查）跳过 MV 重写
        if (cascadesContext.getStatementContext().isShortCircuitQuery()) {
            LOG.debug("MaterializationContext init return because isShortCircuitQuery, current queryId is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return;
        }
        // 收集查询涉及的所有表
        Set<TableIf> collectedTables = Sets.newHashSet(cascadesContext.getStatementContext().getTables().values());
        if (collectedTables.isEmpty()) {
            return;
        }
        // 为每个 OlapTable 创建同步 MV（Rollup）的上下文
        for (TableIf tableIf : collectedTables) {
            if (tableIf instanceof OlapTable) {
                for (MaterializationContext context : createSyncMvContexts(
                        (OlapTable) tableIf, cascadesContext)) {
                    cascadesContext.addMaterializationContext(context);
                }
            }
        }

        // 创建异步 MV（MTMV）的上下文
        for (MaterializationContext context : createAsyncMaterializationContext(cascadesContext,
                collectedTables)) {
            cascadesContext.addMaterializationContext(context);
        }
    }

    private List<MaterializationContext> getMvIdWithUseMvHint(List<MaterializationContext> mtmvCtxs,
                                                                UseMvHint useMvHint) {
        List<MaterializationContext> hintMTMVs = new ArrayList<>();
        for (MaterializationContext mtmvCtx : mtmvCtxs) {
            List<String> mvQualifier = mtmvCtx.generateMaterializationIdentifier();
            if (useMvHint.getUseMvTableColumnMap().containsKey(mvQualifier)) {
                hintMTMVs.add(mtmvCtx);
            }
        }
        return hintMTMVs;
    }

    private List<MaterializationContext> getMvIdWithNoUseMvHint(List<MaterializationContext> mtmvCtxs,
                                                                    UseMvHint useMvHint) {
        List<MaterializationContext> hintMTMVs = new ArrayList<>();
        if (useMvHint.isAllMv()) {
            useMvHint.setStatus(Hint.HintStatus.SUCCESS);
            return hintMTMVs;
        }
        for (MaterializationContext mtmvCtx : mtmvCtxs) {
            List<String> mvQualifier = mtmvCtx.generateMaterializationIdentifier();
            if (useMvHint.getNoUseMvTableColumnMap().containsKey(mvQualifier)) {
                useMvHint.setStatus(Hint.HintStatus.SUCCESS);
                useMvHint.getNoUseMvTableColumnMap().put(mvQualifier, true);
            } else {
                hintMTMVs.add(mtmvCtx);
            }
        }
        return hintMTMVs;
    }

    /**
     * get mtmvs by hint
     * @param mtmvCtxs input mtmvs which could be used to rewrite sql
     * @return set of mtmvs which pass the check of useMvHint
     */
    public List<MaterializationContext> getMaterializationContextByHint(List<MaterializationContext> mtmvCtxs) {
        Optional<UseMvHint> useMvHint = ConnectContext.get().getStatementContext().getUseMvHint("USE_MV");
        Optional<UseMvHint> noUseMvHint = ConnectContext.get().getStatementContext().getUseMvHint("NO_USE_MV");
        if (!useMvHint.isPresent() && !noUseMvHint.isPresent()) {
            return mtmvCtxs;
        }
        List<MaterializationContext> result = mtmvCtxs;
        if (noUseMvHint.isPresent()) {
            result = getMvIdWithNoUseMvHint(result, noUseMvHint.get());
        }
        if (useMvHint.isPresent()) {
            result = getMvIdWithUseMvHint(result, useMvHint.get());
        }
        return result;
    }

    protected Set<MTMV> getAvailableMTMVs(Set<TableIf> usedTables, CascadesContext cascadesContext) {
        return Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getAvailableMTMVs(cascadesContext.getStatementContext().getCandidateMTMVs(),
                        cascadesContext.getConnectContext(),
                        false, ((connectContext, mtmv) -> {
                            return MTMVUtil.mtmvContainsExternalTable(mtmv) && (!connectContext.getSessionVariable()
                                    .isEnableMaterializedViewRewriteWhenBaseTableUnawareness());
                        }));
    }

    /**
     * 为异步物化视图（MTMV）创建 MaterializationContext。
     * 1. 获取查询表关联的所有可用 MTMV
     * 2. 为每个 MTMV 获取或生成缓存（包含已重写的计划和 StructInfo）
     * 3. 根据是否需要 Pre-MV rewrite 选择不同的 plan 和 StructInfo
     */
    private List<MaterializationContext> createAsyncMaterializationContext(CascadesContext cascadesContext,
            Set<TableIf> usedTables) {
        // 获取与查询表相关的所有可用 MTMV
        Set<MTMV> availableMTMVs;
        try {
            availableMTMVs = getAvailableMTMVs(usedTables, cascadesContext);
        } catch (Exception e) {
            LOG.warn(String.format("MaterializationContext getAvailableMTMVs generate fail, current queryId is %s",
                    cascadesContext.getConnectContext().getQueryIdentifier()), e);
            return ImmutableList.of();
        }
        if (CollectionUtils.isEmpty(availableMTMVs)) {
            LOG.info("Enable materialized view rewrite but availableMTMVs is empty, query id "
                    + "is {}", cascadesContext.getConnectContext().getQueryIdentifier());
            return ImmutableList.of();
        }
        List<MaterializationContext> asyncMaterializationContext = new ArrayList<>();
        for (MTMV materializedView : availableMTMVs) {
            MTMVCache mtmvCache = null;
            try {
                // 获取或生成 MTMV 的缓存，包含解析、分析、重写后的计划
                mtmvCache = materializedView.getOrGenerateCache(cascadesContext.getConnectContext());
                if (mtmvCache == null) {
                    continue;
                }
                // 异步 MV 的 StructInfo 可能是在不同的 CascadesContext 中构建的，
                // 需要根据当前上下文重新生成 table bitset
                if (!cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
                    // 普通模式：使用完整重写后的计划
                    asyncMaterializationContext.add(doCreateAsyncMaterializationContext(materializedView,
                            mtmvCache, mtmvCache.getAllRulesRewrittenPlanAndStructInfo(), cascadesContext
                    ));
                } else {
                    // Pre-MV rewrite 模式：使用部分重写的计划（多个 StructInfo）
                    for (Pair<Plan, StructInfo> planAndStructInfo
                            : mtmvCache.getPartRulesRewrittenPlanAndStructInfos()) {
                        asyncMaterializationContext.add(doCreateAsyncMaterializationContext(materializedView,
                                mtmvCache, planAndStructInfo, cascadesContext
                        ));
                    }
                }
            } catch (Exception e) {
                LOG.warn(String.format("MaterializationContext init mv cache generate fail, current queryId is %s",
                        cascadesContext.getConnectContext().getQueryIdentifier()), e);
            }
        }
        // 根据 hint 过滤，只保留用户指定的 MV
        return getMaterializationContextByHint(asyncMaterializationContext);
    }

    private static AsyncMaterializationContext doCreateAsyncMaterializationContext(MTMV mtmv, MTMVCache cache,
            Pair<Plan, StructInfo> planAndStructInfo,
            CascadesContext cascadesContext) {
        return new AsyncMaterializationContext(mtmv, planAndStructInfo.key(), cache.getOriginalFinalPlan(),
                ImmutableList.of(), ImmutableList.of(), cascadesContext, planAndStructInfo.value());
    }

    private List<MaterializationContext> createSyncMvContexts(OlapTable olapTable,
            CascadesContext cascadesContext) {
        int indexNumber = olapTable.getIndexNumber();
        List<MaterializationContext> contexts = new ArrayList<>(indexNumber);
        long baseIndexId = olapTable.getBaseIndexId();
        int keyCount = 0;
        for (Column column : olapTable.getFullSchema()) {
            keyCount += column.isKey() ? 1 : 0;
        }
        for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getVisibleIndexIdToMeta().entrySet()) {
            long indexId = entry.getKey();
            String indexName = olapTable.getIndexNameById(indexId);
            try {
                if (indexId != baseIndexId) {
                    MaterializedIndexMeta meta = entry.getValue();
                    String createMvSql;
                    if (meta.getDefineStmt() != null) {
                        // get the original create mv sql
                        createMvSql = meta.getDefineStmt().originStmt;
                    } else {
                        // it's rollup, need assemble create mv sql manually
                        if (olapTable.getKeysType() == KeysType.AGG_KEYS) {
                            createMvSql = assembleCreateMvSqlForAggTable(olapTable.getQualifiedName(),
                                    indexName, meta.getSchema(false), keyCount);
                        } else {
                            createMvSql =
                                    assembleCreateMvSqlForDupOrUniqueTable(olapTable.getQualifiedName(),
                                            indexName, meta.getSchema(false));
                        }
                    }
                    if (createMvSql != null) {
                        Optional<String> querySql =
                                new NereidsParser().parseForSyncMv(createMvSql);
                        if (!querySql.isPresent()) {
                            LOG.warn(String.format("can't parse %s ", createMvSql));
                            continue;
                        }
                        ConnectContext basicMvContext = MTMVPlanUtil.createBasicMvContext(
                                cascadesContext.getConnectContext(),
                                MTMVPlanUtil.DISABLE_RULES_WHEN_GENERATE_MTMV_CACHE, meta.getSessionVariables());
                        basicMvContext.setDatabase(meta.getDbName());

                        boolean sessionVarMatch = SessionVarGuardRewriter.checkSessionVariablesMatch(
                                ConnectContextUtil.getAffectQueryResultInPlanVariables(
                                        cascadesContext.getConnectContext()), meta.getSessionVariables());
                        MTMVCache mtmvCache = MTMVCache.from(querySql.get(),
                                basicMvContext, true,
                                false, cascadesContext.getConnectContext(), !sessionVarMatch);
                        if (!cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
                            contexts.add(new SyncMaterializationContext(
                                    mtmvCache.getAllRulesRewrittenPlanAndStructInfo().key(),
                                    mtmvCache.getOriginalFinalPlan(), olapTable, meta.getIndexId(), indexName,
                                    cascadesContext, mtmvCache.getStatistics()));
                        } else {
                            for (Pair<Plan, StructInfo> planAndStructInfo
                                    : mtmvCache.getPartRulesRewrittenPlanAndStructInfos()) {
                                contexts.add(new SyncMaterializationContext(planAndStructInfo.key(),
                                        planAndStructInfo.key(), olapTable, meta.getIndexId(), indexName,
                                        cascadesContext, mtmvCache.getStatistics()));
                            }
                        }
                    } else {
                        LOG.warn(String.format("can't assemble create mv sql for index ", indexName));
                    }
                }
            } catch (Exception exception) {
                LOG.warn(String.format("createSyncMvContexts exception, index id is %s, index name is %s, "
                                + "table name is %s", entry.getValue(), indexName, olapTable.getQualifiedName()),
                        exception);
            }
        }
        return getMaterializationContextByHint(contexts);
    }

    private boolean checkSessionVariablesMatch(Map<String, String> var1, Map<String, String> var2) {
        if (var1 == null && var2 == null) {
            return true;
        } else if (var1 == null || var2 == null) {
            return false;
        }
        // Check if all saved session variables match current ones
        for (Map.Entry<String, String> entry : var1.entrySet()) {
            String varName = entry.getKey();
            String savedValue = entry.getValue();
            String currentValue = var2.get(varName);

            if (currentValue == null || !currentValue.equals(savedValue)) {
                return false;
            }
        }

        return true;
    }

    private String assembleCreateMvSqlForDupOrUniqueTable(String baseTableName, String mvName, List<Column> columns) {
        StringBuilder createMvSqlBuilder = new StringBuilder();
        createMvSqlBuilder.append(String.format("create materialized view %s as select ", mvName));
        for (Column col : columns) {
            createMvSqlBuilder.append(String.format("%s, ", getIdentSql(col.getName())));
        }
        removeLastTwoChars(createMvSqlBuilder);
        createMvSqlBuilder.append(String.format(" from %s", baseTableName));
        return createMvSqlBuilder.toString();
    }

    private String assembleCreateMvSqlForAggTable(String baseTableName, String mvName,
            List<Column> columns, int keyCount) {
        StringBuilder createMvSqlBuilder = new StringBuilder();
        createMvSqlBuilder.append(String.format("create materialized view %s as select ", mvName));
        int mvKeyCount = 0;
        for (Column column : columns) {
            mvKeyCount += column.isKey() ? 1 : 0;
        }
        if (mvKeyCount < keyCount) {
            StringBuilder keyColumnsStringBuilder = new StringBuilder();
            StringBuilder aggColumnsStringBuilder = new StringBuilder();
            for (Column col : columns) {
                AggregateType aggregateType = col.getAggregationType();
                if (aggregateType != null) {
                    switch (aggregateType) {
                        case SUM:
                        case MAX:
                        case MIN:
                        case HLL_UNION:
                        case BITMAP_UNION:
                        case QUANTILE_UNION: {
                            aggColumnsStringBuilder
                                    .append(String.format("%s(%s), ", aggregateType, getIdentSql(col.getName())));
                            break;
                        }
                        case GENERIC: {
                            AggStateType aggStateType = (AggStateType) col.getType();
                            aggColumnsStringBuilder.append(String.format("%s_union(%s), ",
                                    aggStateType.getFunctionName(), getIdentSql(col.getName())));
                            break;
                        }
                        default: {
                            // mv agg columns mustn't be NONE, REPLACE, REPLACE_IF_NOT_NULL agg type
                            LOG.warn(String.format("mv agg column %s mustn't be %s type",
                                    col.getName(), aggregateType));
                            return null;
                        }
                    }
                } else {
                    // use column name for key
                    Preconditions.checkState(col.isKey(),
                            String.format("%s must be key", col.getName()));
                    keyColumnsStringBuilder.append(String.format("%s, ", getIdentSql(col.getName())));
                }
            }
            Preconditions.checkState(keyColumnsStringBuilder.length() > 0,
                    "must contain at least one key column in rollup");
            if (aggColumnsStringBuilder.length() > 0) {
                removeLastTwoChars(aggColumnsStringBuilder);
            } else {
                removeLastTwoChars(keyColumnsStringBuilder);
            }
            createMvSqlBuilder.append(keyColumnsStringBuilder);
            createMvSqlBuilder.append(aggColumnsStringBuilder);
            if (aggColumnsStringBuilder.length() > 0) {
                // all key columns should be group by keys, so remove the last ", " characters
                removeLastTwoChars(keyColumnsStringBuilder);
            }
            createMvSqlBuilder.append(
                    String.format(" from %s group by %s", baseTableName, keyColumnsStringBuilder));
        } else {
            for (Column col : columns) {
                createMvSqlBuilder.append(String.format("%s, ", getIdentSql(col.getName())));
            }
            removeLastTwoChars(createMvSqlBuilder);
            createMvSqlBuilder.append(String.format(" from %s", baseTableName));
        }

        return createMvSqlBuilder.toString();
    }

    private void removeLastTwoChars(StringBuilder stringBuilder) {
        if (stringBuilder.length() >= 2) {
            stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        }
    }

    private static String getIdentSql(String ident) {
        StringBuilder sb = new StringBuilder();
        sb.append('`');
        for (char ch : ident.toCharArray()) {
            if (ch == '`') {
                sb.append("``");
            } else {
                sb.append(ch);
            }
        }
        sb.append('`');
        return sb.toString();
    }
}
