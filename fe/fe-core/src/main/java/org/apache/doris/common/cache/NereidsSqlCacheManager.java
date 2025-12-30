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

package org.apache.doris.common.cache;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.SqlCacheContext.CacheKeyType;
import org.apache.doris.nereids.SqlCacheContext.FullColumnName;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.SqlCacheContext.ScanTable;
import org.apache.doris.nereids.SqlCacheContext.TableVersion;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.SqlCache;
import org.apache.doris.rpc.RpcException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Nereids SQL 缓存管理器。
 * <p>
 * 负责维护前端（FE）中 Nereids 引擎的 SQL 缓存，包括：缓存创建、命中检查、失效处理以及配置热更新。
 * 缓存底层使用 Caffeine，支持基于访问时间的过期和容量限制，同时使用软引用配合 JVM 内存回收。
 */
public class NereidsSqlCacheManager {
    private static final Logger LOG = LogManager.getLogger(NereidsSqlCacheManager.class);
    /** 缓存键格式：<catalog.database>:<user>:<sql 或 md5>；值为对应的上下文信息。 */
    // value: SqlCacheContext
    private volatile Cache<String, SqlCacheContext> sqlCaches;

    /**
     * 构造函数：根据当前配置初始化 SQL 缓存。
     */
    public NereidsSqlCacheManager() {
        sqlCaches = buildSqlCaches(
                Config.sql_cache_manage_num,
                Config.expire_sql_cache_in_fe_second
        );
    }

    /** 仅用于测试场景的访问入口。 */
    @VisibleForTesting
    public Cache<String, SqlCacheContext> getSqlCaches() {
        return sqlCaches;
    }

    /**
     * 使引用指定表的所有缓存失效。
     */
    public void invalidateAboutTable(TableIf tableIf) {
        Set<String> invalidateKeys = new LinkedHashSet<>();
        FullTableName invalidateTableName = null;
        DatabaseIf database = tableIf.getDatabase();
        if (database != null) {
            CatalogIf catalog = database.getCatalog();
            if (catalog != null) {
                invalidateTableName = new FullTableName(
                        database.getCatalog().getName(), database.getFullName(), tableIf.getName()
                );
            }
        }

        for (Entry<String, SqlCacheContext> kv : sqlCaches.asMap().entrySet()) {
            String key = kv.getKey();
            SqlCacheContext context = kv.getValue();
            // 缓存中记录了查询涉及的表及其版本，逐个比对是否命中当前待失效的表
            for (Entry<FullTableName, TableVersion> nameToVersion : context.getUsedTables().entrySet()) {
                FullTableName tableName = nameToVersion.getKey();
                TableVersion tableVersion = nameToVersion.getValue();
                if (tableVersion.id == tableIf.getId()) {
                    invalidateKeys.add(key);
                    break;
                }
                if (tableName.equals(invalidateTableName)) {
                    invalidateKeys.add(key);
                    break;
                }
            }
        }

        for (String invalidateKey : invalidateKeys) {
            sqlCaches.invalidate(invalidateKey);
        }
    }

    public void invalidateAll() {
        sqlCaches.invalidateAll();
    }

    /**
     * 在动态修改 SQL Cache 相关配置时调用，基于最新参数重建缓存实例并迁移旧数据。
     */
    public static synchronized void updateConfig() {
        Env currentEnv = Env.getCurrentEnv();
        if (currentEnv == null) {
            return;
        }
        NereidsSqlCacheManager sqlCacheManager = currentEnv.getSqlCacheManager();
        if (sqlCacheManager == null) {
            return;
        }

        Cache<String, SqlCacheContext> sqlCaches = buildSqlCaches(
                Config.sql_cache_manage_num,
                Config.expire_sql_cache_in_fe_second
        );
        sqlCaches.putAll(sqlCacheManager.sqlCaches.asMap());
        sqlCaches.cleanUp();
        sqlCacheManager.sqlCaches = sqlCaches;
    }

    /**
     * 根据配置构建 SQL 缓存实例。
     *
     * @param sqlCacheNum            最大缓存条数（小于等于 0 表示不限制）
     * @param expireAfterAccessSeconds 访问超时秒数（小于等于 0 表示不过期）
     */
    private static Cache<String, SqlCacheContext> buildSqlCaches(int sqlCacheNum, long expireAfterAccessSeconds) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                // auto evict cache when jvm memory too low
                .softValues();
        if (sqlCacheNum > 0) {
            cacheBuilder.maximumSize(sqlCacheNum);
        }
        if (expireAfterAccessSeconds > 0) {
            cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSeconds));
        }

        return cacheBuilder.build();
    }

    /**
     * 尝试将 FE 端直接产生的结果集加入缓存。
     * <p>
     * 当查询在前端即可返回结果（例如常量语句、EXPLAIN 等）时，缓存结果方便后续重用。
     */
    public void tryAddFeSqlCache(ConnectContext connectContext, String sql) {
        switch (connectContext.getCommand()) {
            case COM_STMT_EXECUTE:
            case COM_STMT_PREPARE:
                return;
            default: { }
        }

        Optional<SqlCacheContext> sqlCacheContextOpt = connectContext.getStatementContext().getSqlCacheContext();
        if (!sqlCacheContextOpt.isPresent()) {
            return;
        }
        SqlCacheContext sqlCacheContext = sqlCacheContextOpt.get();
        if (!sqlCacheContext.supportSqlCache()) {
            return;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sqlCacheContext.setQueryId(connectContext.queryId());
        String key = sqlCacheContext.getCacheKeyType() == CacheKeyType.SQL
                ? generateCacheKey(connectContext, normalizeSql(sql))
                : generateCacheKey(connectContext,
                        DebugUtil.printId(sqlCacheContext.getOrComputeCacheKeyMd5(sessionVariable)));
        if (sqlCaches.getIfPresent(key) == null && sqlCacheContext.getOrComputeCacheKeyMd5(sessionVariable) != null
                && sqlCacheContext.getResultSetInFe().isPresent()) {
            sqlCacheContext.setAffectQueryResultVariables(sessionVariable);
            sqlCaches.put(key, sqlCacheContext);
        }
    }

    /**
     * 尝试将 BE 端执行完毕的查询加入缓存。
     * <p>
     * 在结果返回 FE 后，将查询使用到的表、分区等信息记录在上下文中，以便后续校验。
     */
    public void tryAddBeCache(ConnectContext connectContext, String sql, CacheAnalyzer analyzer) {
        switch (connectContext.getCommand()) {
            case COM_STMT_EXECUTE:
            case COM_STMT_PREPARE:
                return;
            default: { }
        }
        Optional<SqlCacheContext> sqlCacheContextOpt = connectContext.getStatementContext().getSqlCacheContext();
        if (!sqlCacheContextOpt.isPresent()) {
            return;
        }
        if (!(analyzer.getCache() instanceof SqlCache)) {
            return;
        }
        SqlCacheContext sqlCacheContext = sqlCacheContextOpt.get();
        if (!sqlCacheContext.supportSqlCache()) {
            return;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sqlCacheContext.setQueryId(connectContext.queryId());
        String key = sqlCacheContext.getCacheKeyType() == CacheKeyType.SQL
                ? generateCacheKey(connectContext, normalizeSql(sql))
                : generateCacheKey(connectContext,
                        DebugUtil.printId(sqlCacheContext.getOrComputeCacheKeyMd5(sessionVariable))
                );
        if (sqlCaches.getIfPresent(key) == null && sqlCacheContext.getOrComputeCacheKeyMd5(sessionVariable) != null) {
            SqlCache cache = (SqlCache) analyzer.getCache();
            // 记录分区相关元信息，命中时用于校验数据是否变化
            sqlCacheContext.setSumOfPartitionNum(cache.getSumOfPartitionNum());
            sqlCacheContext.setLatestPartitionId(cache.getLatestId());
            sqlCacheContext.setLatestPartitionVersion(cache.getLatestVersion());
            sqlCacheContext.setLatestPartitionTime(cache.getLatestTime());
            sqlCacheContext.setCacheProxy(cache.getProxy());
            sqlCacheContext.setAffectQueryResultVariables(sessionVariable);

            // 将分析阶段收集到的扫描表信息写入上下文，用于后续一致性校验
            for (Pair<ScanTable, TableIf> scanTable : analyzer.getScanTables()) {
                sqlCacheContext.addScanTable(scanTable.first, scanTable.second);
            }
            // add scan tables maybe find some mtmv which can not support because grace_period > 0,
            // we will not add sql cache
            if (!sqlCacheContext.supportSqlCache()) {
                return;
            }

            sqlCaches.put(key, sqlCacheContext);
        }
    }

    /**
     * 尝试命中 SQL 缓存。
     * <p>
     * 步骤：先根据规范化 SQL 生成缓存键；若命中，则解析用户变量，判断是否需要改用带 MD5 的缓存键；
     * 最后调用底层重载方法执行权限、元数据等详细校验。
     *
     * @return 命中缓存时返回封装好的逻辑缓存信息，否则返回 empty
     */
    public Optional<LogicalSqlCache> tryParseSql(ConnectContext connectContext, String sql) {
        switch (connectContext.getCommand()) {
            case COM_STMT_EXECUTE:
            case COM_STMT_PREPARE:
                return Optional.empty();
            default: { }
        }
        // 1) 依据原始 SQL 生成缓存键，并尝试命中
        String key = generateCacheKey(connectContext, normalizeSql(sql.trim()));
        SqlCacheContext sqlCacheContext = sqlCaches.getIfPresent(key);
        if (sqlCacheContext == null) {
            return Optional.empty();
        }

        // LOG.info("Total size: " + GraphLayout.parseInstance(sqlCacheContext).totalSize());
        UserIdentity currentUserIdentity = connectContext.getCurrentUserIdentity();
        List<Variable> currentVariables = resolveUserVariables(sqlCacheContext);
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (usedVariablesChanged(currentVariables, sqlCacheContext, sessionVariable)) {
            // 2) 若缓存涉及用户变量且值发生变化，改用 MD5 形式的 cache key
            String md5 = DebugUtil.printId(
                    sqlCacheContext.doComputeCacheKeyMd5(Utils.fastToImmutableSet(currentVariables), sessionVariable));
            String md5CacheKey = generateCacheKey(connectContext, md5);
            SqlCacheContext sqlCacheContextWithVariable = sqlCaches.getIfPresent(md5CacheKey);

            // already exist cache in the fe, but the variable is different to this query,
            // we should create another cache context in fe, use another cache key
            connectContext.getStatementContext()
                    .getSqlCacheContext().ifPresent(ctx -> ctx.setCacheKeyType(CacheKeyType.MD5));

            if (sqlCacheContextWithVariable != null) {
                return tryParseSql(
                        connectContext, md5CacheKey, sqlCacheContextWithVariable, currentUserIdentity, true
                );
            } else {
                return Optional.empty();
            }
        } else {
            // 3) 否则直接使用原始 key 进入后续校验流程
            return tryParseSql(connectContext, key, sqlCacheContext, currentUserIdentity, false);
        }
    }

    /** 基于当前 Catalog、数据库、用户及 SQL（或 MD5）生成缓存键。 */
    private String generateCacheKey(ConnectContext connectContext, String sqlOrMd5) {
        CatalogIf<?> currentCatalog = connectContext.getCurrentCatalog();
        String currentCatalogName = currentCatalog != null ? currentCatalog.getName() : "";
        String currentDatabase = connectContext.getDatabase();
        String currentDatabaseName = currentDatabase != null ? currentDatabase : "";
        return currentCatalogName + "." + currentDatabaseName + ":" + connectContext.getCurrentUserIdentity().toString()
                + ":" + sqlOrMd5;
    }

    /** 规范化 SQL：去掉注释并裁剪首尾空白。 */
    private String normalizeSql(String sql) {
        return NereidsParser.removeCommentAndTrimBlank(sql);
    }

    private Optional<LogicalSqlCache> tryParseSql(
            ConnectContext connectContext, String key, SqlCacheContext sqlCacheContext,
            UserIdentity currentUserIdentity, boolean checkUserVariable) {
        try {
            Env env = connectContext.getEnv();

            // 1) 锁定本次查询涉及的表/视图，避免校验过程中元数据被并发修改
            if (!tryLockTables(connectContext, env, sqlCacheContext)) {
                return invalidateCache(key);
            }

            // 2) 权限检查：若用户权限发生变化则不能复用缓存
            if (privilegeChanged(connectContext, env, sqlCacheContext)) {
                return invalidateCache(key);
            }
            // 3) 校验表、数据、视图、外部 Catalog 等是否发生变化
            IsChanged tableIsChanged = tablesOrDataChanged(env, sqlCacheContext);
            if (tableIsChanged.changed) {
                if (tableIsChanged.invalidCache) {
                    return invalidateCache(key);
                } else {
                    return Optional.empty();
                }
            }
            IsChanged viewIsChanged = viewsChanged(env, sqlCacheContext);
            if (viewIsChanged.changed) {
                if (viewIsChanged.invalidCache) {
                    return invalidateCache(key);
                } else {
                    return Optional.empty();
                }
            }
            if (externalCatalogChanged(env, sqlCacheContext)) {
                return invalidateCache(key);
            }

            LogicalEmptyRelation whateverPlan = new LogicalEmptyRelation(new RelationId(0), ImmutableList.of());
            if (nondeterministicFunctionChanged(whateverPlan, connectContext, sqlCacheContext)) {
                return invalidateCache(key);
            }

            // table structure and data not changed, now check policy
            // 4) 校验行级、列级安全策略是否发生变化
            if (rowPoliciesChanged(currentUserIdentity, env, sqlCacheContext)) {
                return invalidateCache(key);
            }
            if (dataMaskPoliciesChanged(currentUserIdentity, env, sqlCacheContext)) {
                return invalidateCache(key);
            }
            Optional<ResultSet> resultSetInFe = sqlCacheContext.getResultSetInFe();

            List<Variable> currentVariables = ImmutableList.of();
            if (checkUserVariable) {
                currentVariables = resolveUserVariables(sqlCacheContext);
            }
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            boolean usedVariablesChanged
                    = checkUserVariable && usedVariablesChanged(currentVariables, sqlCacheContext, sessionVariable);
            if (resultSetInFe.isPresent() && !usedVariablesChanged) {
                // 优先复用 FE 保存的结果集（无需访问 BE）
                String cachedPlan = sqlCacheContext.getPhysicalPlan();
                LogicalSqlCache logicalSqlCache = new LogicalSqlCache(
                        sqlCacheContext.getQueryId(), sqlCacheContext.getColLabels(), sqlCacheContext.getFieldInfos(),
                        sqlCacheContext.getResultExprs(), resultSetInFe, ImmutableList.of(),
                        "none", cachedPlan
                );
                return Optional.of(logicalSqlCache);
            }

            Status status = new Status();

            PUniqueId cacheKeyMd5;
            if (usedVariablesChanged) {
                invalidateCache(key);
                cacheKeyMd5 = sqlCacheContext.doComputeCacheKeyMd5(
                        Utils.fastToImmutableSet(currentVariables), sessionVariable
                );
            } else {
                cacheKeyMd5 = sqlCacheContext.getOrComputeCacheKeyMd5(sessionVariable);
            }

            InternalService.PFetchCacheResult cacheData =
                    SqlCache.getCacheData(sqlCacheContext.getCacheProxy(),
                            cacheKeyMd5, sqlCacheContext.getLatestPartitionId(),
                            sqlCacheContext.getLatestPartitionVersion(), sqlCacheContext.getLatestPartitionTime(),
                            sqlCacheContext.getSumOfPartitionNum(), status);

            if (status.ok() && cacheData != null && cacheData.getStatus() == InternalService.PCacheStatus.CACHE_OK) {
                List<InternalService.PCacheValue> cacheValues = cacheData.getValuesList();
                String cachedPlan = sqlCacheContext.getPhysicalPlan();
                String backendAddress = SqlCache.findCacheBe(cacheKeyMd5).getAddress();
                LogicalSqlCache logicalSqlCache = new LogicalSqlCache(
                        sqlCacheContext.getQueryId(), sqlCacheContext.getColLabels(), sqlCacheContext.getFieldInfos(),
                        sqlCacheContext.getResultExprs(), Optional.empty(),
                        cacheValues, backendAddress, cachedPlan
                );
                return Optional.of(logicalSqlCache);
            }
            return Optional.empty();
        } catch (Throwable t) {
            return invalidateCache(key);
        }
    }

    /**
     * 检查缓存记录的表/分区是否发生变化。
     * <p>
     * 包含：表 ID、版本号、分区集合、外部表更新时间等。
     */
    private IsChanged tablesOrDataChanged(Env env, SqlCacheContext sqlCacheContext) {
        if (sqlCacheContext.hasUnsupportedTables()) {
            return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
        }

        // the query maybe scan empty partition of the table, we should check these table version too,
        // but the table not exists in sqlCacheContext.getScanTables(), so we need check here.
        // check table type and version
        // 遍历缓存记录的所有表，逐一对比表类型、ID、版本等信息是否一致
        for (Entry<FullTableName, TableVersion> scanTable : sqlCacheContext.getUsedTables().entrySet()) {
            TableVersion tableVersion = scanTable.getValue();
            if (tableVersion.type != TableType.OLAP && tableVersion.type != TableType.MATERIALIZED_VIEW
                    && tableVersion.type != TableType.HMS_EXTERNAL_TABLE) {
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            }
            TableIf tableIf = findTableIf(env, scanTable.getKey());
            if (tableIf == null) {
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            } else if (tableIf.isTemporary()) {
                return IsChanged.CHANGED_BUT_NOT_INVALIDATE_CACHE;
            } else if (tableVersion.id != tableIf.getId()) { // should after isTemporary condition
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            }
            if (tableIf instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) tableIf;
                long currentTableVersion = 0L;
                try {
                    currentTableVersion = olapTable.getVisibleVersion();
                } catch (RpcException e) {
                    LOG.warn("table {}, in cloud getVisibleVersion exception", olapTable.getName(), e);
                    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                }
                long cacheTableVersion = tableVersion.version;
                // 表的版本号不一致，说明分区被新增/删除或数据被更新
                if (currentTableVersion != cacheTableVersion) {
                    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                }
                if (tableIf instanceof MTMV) {
                    // mtmv maybe access old data when grace_period > 0, we should disable cache at this case
                    long gracePeriod = ((MTMV) tableIf).getGracePeriod();
                    if (gracePeriod > 0) {
                        return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                    }
                }
            } else if (tableIf instanceof HMSExternalTable) {
                HMSExternalTable hiveTable = (HMSExternalTable) tableIf;
                if (tableVersion.version != hiveTable.getUpdateTime()) {
                    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                }
            } else {
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            }
        }

        // 检查缓存记录的具体分区是否仍然存在，防止使用已被删除的分区
        for (ScanTable scanTable : sqlCacheContext.getScanTables()) {
            FullTableName fullTableName = scanTable.fullTableName;
            TableIf tableIf = findTableIf(env, fullTableName);
            if (tableIf instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) tableIf;
                Collection<Long> partitionIds = scanTable.getScanPartitions();
                try {
                    olapTable.getVersionInBatchForCloudMode(partitionIds);
                } catch (RpcException e) {
                    LOG.warn("failed to get version in batch for table {}", fullTableName, e);
                    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                }

                for (Long scanPartitionId : scanTable.getScanPartitions()) {
                    Partition partition = olapTable.getPartition(scanPartitionId);
                    // partition == null: is this partition truncated?
                    if (partition == null) {
                        return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                    }
                }
            } else if (!(tableIf instanceof HMSExternalTable)) {
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            }
        }
        return IsChanged.NOT_CHANGED;
    }

    /** 检查缓存依赖的视图定义是否发生变化。 */
    private IsChanged viewsChanged(Env env, SqlCacheContext sqlCacheContext) {
        // 遍历缓存使用到的每个视图，校验视图是否仍存在且定义未变
        for (Entry<FullTableName, String> cacheView : sqlCacheContext.getUsedViews().entrySet()) {
            TableIf currentView = findTableIf(env, cacheView.getKey());
            if (currentView == null) {
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            } else if (currentView.isTemporary()) {
                return IsChanged.CHANGED_BUT_NOT_INVALIDATE_CACHE;
            }

            String cacheValueDdlSql = cacheView.getValue();
            if (currentView instanceof View) {
                if (!((View) currentView).getInlineViewDef().equals(cacheValueDdlSql)) {
                    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
                }
            } else {
                return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
            }
        }
        return IsChanged.NOT_CHANGED;
    }

    /** 检查行级安全策略是否变化。 */
    private boolean rowPoliciesChanged(UserIdentity currentUserIdentity, Env env, SqlCacheContext sqlCacheContext) {
        for (Entry<FullTableName, List<RowFilterPolicy>> kv : sqlCacheContext.getRowPolicies().entrySet()) {
            FullTableName qualifiedTable = kv.getKey();
            List<? extends RowFilterPolicy> cachedPolicies = kv.getValue();

            List<? extends RowFilterPolicy> rowPolicies = env.getAccessManager().evalRowFilterPolicies(
                    currentUserIdentity, qualifiedTable.catalog, qualifiedTable.db, qualifiedTable.table);
            if (!CollectionUtils.isEqualCollection(cachedPolicies, rowPolicies)) {
                return true;
            }
        }
        return false;
    }

    /** 检查列脱敏策略是否变化。 */
    private boolean dataMaskPoliciesChanged(
            UserIdentity currentUserIdentity, Env env, SqlCacheContext sqlCacheContext) {
        for (Entry<FullColumnName, Optional<DataMaskPolicy>> kv : sqlCacheContext.getDataMaskPolicies().entrySet()) {
            FullColumnName qualifiedColumn = kv.getKey();
            Optional<DataMaskPolicy> cachedPolicy = kv.getValue();

            Optional<DataMaskPolicy> dataMaskPolicy = env.getAccessManager()
                    .evalDataMaskPolicy(currentUserIdentity, qualifiedColumn.catalog,
                            qualifiedColumn.db, qualifiedColumn.table, qualifiedColumn.column);
            if (!Objects.equals(cachedPolicy, dataMaskPolicy)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Execute table locking operations in ascending order of table IDs.
     *
     * @return true if obtain all tables lock.
     */
    /**
     * 按照查询涉及的对象加锁，保证缓存校验期间元数据一致。
     *
     * @return 是否成功锁定全部对象
     */
    private boolean tryLockTables(ConnectContext connectContext, Env env, SqlCacheContext sqlCacheContext) {
        StatementContext currentStatementContext = connectContext.getStatementContext();
        for (FullTableName fullTableName : sqlCacheContext.getUsedTables().keySet()) {
            TableIf tableIf = findTableIf(env, fullTableName);
            if (tableIf == null) {
                return false;
            }
            currentStatementContext.getTables().put(fullTableName.toList(), tableIf);
        }
        for (FullTableName fullTableName : sqlCacheContext.getUsedViews().keySet()) {
            TableIf tableIf = findTableIf(env, fullTableName);
            if (tableIf == null) {
                return false;
            }
            currentStatementContext.getTables().put(fullTableName.toList(), tableIf);
        }
        currentStatementContext.lock();
        return true;
    }

    /** 校验缓存记录的权限信息是否仍旧有效。 */
    private boolean privilegeChanged(ConnectContext connectContext, Env env, SqlCacheContext sqlCacheContext) {
        for (Entry<FullTableName, Set<String>> kv : sqlCacheContext.getCheckPrivilegeTablesOrViews().entrySet()) {
            Set<String> usedColumns = kv.getValue();
            TableIf tableIf = findTableIf(env, kv.getKey());
            if (tableIf == null) {
                return true;
            }
            try {
                UserAuthentication.checkPermission(tableIf, connectContext, usedColumns);
            } catch (Throwable t) {
                return true;
            }
        }
        return false;
    }

    /** 重新解析缓存中记录的用户变量，获取当前实际值。 */
    private List<Variable> resolveUserVariables(SqlCacheContext sqlCacheContext) {
        List<Variable> cachedUsedVariables = sqlCacheContext.getUsedVariables();
        List<Variable> currentVariables = Lists.newArrayListWithCapacity(cachedUsedVariables.size());
        for (Variable cachedVariable : cachedUsedVariables) {
            Variable currentVariable = ExpressionAnalyzer.resolveUnboundVariable(
                    new UnboundVariable(cachedVariable.getName(), cachedVariable.getType()));
            currentVariables.add(currentVariable);
        }
        return currentVariables;
    }

    /** 检查缓存涉及的用户变量是否发生变化。 */
    private boolean usedVariablesChanged(
            List<Variable> currentVariables, SqlCacheContext sqlCacheContext, SessionVariable sessionVariable) {
        List<Variable> cachedUsedVariables = sqlCacheContext.getUsedVariables();
        for (int i = 0; i < cachedUsedVariables.size(); i++) {
            Variable currentVariable = currentVariables.get(i);
            Variable cachedVariable = cachedUsedVariables.get(i);
            if (!Objects.equals(currentVariable, cachedVariable)
                    || cachedVariable.getRealExpression().anyMatch(
                        expr -> !((ExpressionTrait) expr).isDeterministic())) {
                return true;
            }
        }
        String cachedAffectQueryResultVariables = sqlCacheContext.getAffectQueryResultVariables();
        String currentAffectQueryResultVariables = SqlCacheContext.computeAffectQueryResultVariables(sessionVariable);
        return !StringUtils.equals(cachedAffectQueryResultVariables, currentAffectQueryResultVariables);
    }

    /** 判断缓存中存在的非确定性函数结果是否仍可复用。 */
    private boolean nondeterministicFunctionChanged(
            Plan plan, ConnectContext connectContext, SqlCacheContext sqlCacheContext) {
        if (sqlCacheContext.containsCannotProcessExpression()) {
            return true;
        }

        List<Pair<Expression, Expression>> nondeterministicFunctions
                = sqlCacheContext.getFoldFullNondeterministicPairs();
        if (nondeterministicFunctions.isEmpty()) {
            return false;
        }

        CascadesContext tempCascadeContext = CascadesContext.initContext(
                connectContext.getStatementContext(), plan, PhysicalProperties.ANY);
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(tempCascadeContext);
        for (Pair<Expression, Expression> foldPair : nondeterministicFunctions) {
            Expression nondeterministic = foldPair.first;
            Expression deterministic = foldPair.second;
            Expression fold = nondeterministic.accept(FoldConstantRuleOnFE.VISITOR_INSTANCE, rewriteContext);
            if (!Objects.equals(deterministic, fold)) {
                return true;
            }
        }
        return false;
    }

    /** 检查外部 Catalog 的配置是否变化（例如连接信息）。 */
    private boolean externalCatalogChanged(Env env, SqlCacheContext sqlCacheContext) {
        Map<String, String> externalCatalogConfigs = sqlCacheContext.getExternalCatalogConfigs();
        CatalogMgr catalogMgr = env.getCatalogMgr();
        for (Entry<String, String> kv : externalCatalogConfigs.entrySet()) {
            CatalogIf catalog = catalogMgr.getCatalog(kv.getKey());
            if (catalog == null) {
                return true;
            }
            String catalogConfigs = SqlCacheContext.getCatalogConfigs(catalog);
            if (!StringUtils.equals(catalogConfigs, kv.getValue())) {
                return true;
            }
        }
        return false;
    }

    /** 清除指定 key 的缓存，并返回 empty，便于链式调用。 */
    private Optional<LogicalSqlCache> invalidateCache(String key) {
        sqlCaches.invalidate(key);
        return Optional.empty();
    }

    /** 根据全限定名查找表对象，找不到返回 null。 */
    private TableIf findTableIf(Env env, FullTableName fullTableName) {
        CatalogIf<DatabaseIf<TableIf>> catalog = env.getCatalogMgr().getCatalog(fullTableName.catalog);
        if (catalog == null) {
            return null;
        }
        Optional<DatabaseIf<TableIf>> db = catalog.getDb(fullTableName.db);
        if (!db.isPresent()) {
            return null;
        }
        return db.get().getTable(fullTableName.table).orElse(null);
    }

    // NOTE: used in Config.sql_cache_manage_num.callbackClassString and
    //       Config.cache_last_version_interval_second.callbackClassString,
    //       don't remove it!
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            NereidsSqlCacheManager.updateConfig();
        }
    }

    /** 返回当前缓存条目估算数量。 */
    public long getSqlCacheNum() {
        return sqlCaches.estimatedSize();
    }

    /** 表示对象是否发生变化以及是否需要直接失效缓存的返回值封装。 */
    private static class IsChanged {
        public static final IsChanged NOT_CHANGED = new IsChanged(false, false);
        public static final IsChanged CHANGED_AND_INVALIDATE_CACHE = new IsChanged(true, true);
        public static final IsChanged CHANGED_BUT_NOT_INVALIDATE_CACHE = new IsChanged(true, false);

        public final boolean changed;
        public final boolean invalidCache;

        private IsChanged(boolean changed, boolean invalidCache) {
            this.changed = changed;
            this.invalidCache = invalidCache;
        }
    }
}
