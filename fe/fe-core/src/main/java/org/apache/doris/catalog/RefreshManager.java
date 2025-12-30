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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.persist.OperationType;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Manager for refresh database and table action
public class RefreshManager {
    private static final Logger LOG = LogManager.getLogger(RefreshManager.class);
    private ScheduledThreadPoolExecutor refreshScheduler = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "catalog-refresh-timer-pool", true);
    // Unit:SECONDS
    private static final int REFRESH_TIME_SEC = 5;
    // key is the id of a catalog, value is an array of length 2, used to store
    // the original refresh time and the current remaining time of the catalog
    private Map<Long, Integer[]> refreshMap = Maps.newConcurrentMap();

    // Refresh catalog
    public void handleRefreshCatalog(String catalogName, boolean invalidCache) throws UserException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogName);
        refreshCatalogInternal(catalog, invalidCache);
        CatalogLog log = CatalogLog.createForRefreshCatalog(catalog.getId(), invalidCache);
        Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_REFRESH_CATALOG, log);
    }

    public void replayRefreshCatalog(CatalogLog log) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog replaying refresh catalog {}", log.getCatalogId());
            return;
        }
        refreshCatalogInternal(catalog, log.isInvalidCache());
    }

    private void refreshCatalogInternal(CatalogIf catalog, boolean invalidCache) {
        if (catalog.isInternalCatalog()) {
            return;
        }
        ((ExternalCatalog) catalog).onRefreshCache(invalidCache);
        LOG.info("refresh catalog {} with invalidCache {}", catalog.getName(), invalidCache);
    }

    // Refresh database
    public void handleRefreshDb(String catalogName, String dbName) throws DdlException {
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();
        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh database in external catalog");
        }
        DatabaseIf db = catalog.getDbOrDdlException(dbName);
        refreshDbInternal((ExternalDatabase) db);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(catalog.getId(), db.getFullName());
        Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
    }

    public void replayRefreshDb(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh db: {}", log.debugForRefreshDb());
        }
        Optional<ExternalDatabase<? extends ExternalTable>> db;
        if (!Strings.isNullOrEmpty(log.getDbName())) {
            db = catalog.getDbForReplay(log.getDbName());
        } else {
            db = catalog.getDbForReplay(log.getDbId());
        }

        if (!db.isPresent()) {
            LOG.warn("failed to find db when replaying refresh db: {}", log.debugForRefreshDb());
        } else {
            refreshDbInternal(db.get());
        }
    }

    private void refreshDbInternal(ExternalDatabase db) {
        db.resetMetaToUninitialized();
        LOG.info("refresh database {} in catalog {}", db.getFullName(), db.getCatalog().getName());
    }

    // Refresh table
    public void handleRefreshTable(String catalogName, String dbName, String tableName, boolean ignoreIfNotExists)
            throws DdlException {
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();
        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }

        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, 0);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(catalog.getId(), db.getFullName(),
                table.getName());
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    public void replayRefreshTable(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        Optional<ExternalDatabase<? extends ExternalTable>> db;
        if (!Strings.isNullOrEmpty(log.getDbName())) {
            db = catalog.getDbForReplay(log.getDbName());
        } else {
            db = catalog.getDbForReplay(log.getDbId());
        }
        // See comment in refreshDbInternal for why db and table may be null.
        if (!db.isPresent()) {
            LOG.warn("failed to find db when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        Optional<? extends ExternalTable> table;
        if (!Strings.isNullOrEmpty(log.getTableName())) {
            table = db.get().getTableForReplay(log.getTableName());
        } else {
            table = db.get().getTableForReplay(log.getTableId());
        }
        if (!table.isPresent()) {
            LOG.warn("failed to find table when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        if (!Strings.isNullOrEmpty(log.getNewTableName())) {
            // this is a rename table op
            db.get().unregisterTable(log.getTableName());
            db.get().resetMetaCacheNames();
        } else {
            List<String> modifiedPartNames = log.getPartitionNames();
            List<String> newPartNames = log.getNewPartitionNames();
            if (catalog instanceof HMSExternalCatalog
                    && ((modifiedPartNames != null && !modifiedPartNames.isEmpty())
                    || (newPartNames != null && !newPartNames.isEmpty()))) {
                // Partition-level cache invalidation, only for hive catalog
                HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                        .getMetaStoreCache((HMSExternalCatalog) catalog);
                cache.refreshAffectedPartitionsCache((HMSExternalTable) table.get(), modifiedPartNames, newPartNames);
                LOG.info("replay refresh partitions for table {}, "
                                + "modified partitions count: {}, "
                                + "new partitions count: {}",
                        table.get().getName(), modifiedPartNames == null ? 0 : modifiedPartNames.size(),
                        newPartNames == null ? 0 : newPartNames.size());
            } else {
                // Full table cache invalidation
                refreshTableInternal(db.get(), table.get(), log.getLastUpdateTime());
            }
        }
    }

    public void refreshExternalTableFromEvent(String catalogName, String dbName, String tableName,
            long updateTime) throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            return;
        }
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, updateTime);
    }

    /**
     * 刷新外部表的元数据缓存
     * 
     * 该方法执行外部表的刷新操作，主要包括：
     * 1. 清除表的创建标志，标记表需要重新加载元数据
     * 2. 对于 Hive 表，设置事件更新时间戳（用于增量刷新）
     * 3. 使表级别的元数据缓存失效，强制下次访问时重新从外部系统加载
     * 
     * 使用场景：
     * - 手动执行 REFRESH TABLE 命令时调用（updateTime=0）
     * - 通过 Hive Metastore 事件监听器自动刷新时调用（updateTime>0）
     * - 主从同步 replay 时调用
     * 
     * @param db 外部数据库对象
     * @param table 待刷新的外部表对象
     * @param updateTime 表元数据的更新时间戳（毫秒）
     *                    - updateTime > 0: 表示来自事件驱动的刷新，记录具体的更新时间
     *                    - updateTime = 0: 表示手动刷新，不记录具体时间戳
     *                    对于 HMS 表，该时间戳用于增量刷新，可以避免全量扫描
     */
    public void refreshTableInternal(ExternalDatabase db, ExternalTable table, long updateTime) {
        // 步骤1: 清除表的对象创建标志
        // unsetObjectCreated() 会清除表的初始化标志，使得下次访问时认为表未创建
        // 这样在后续访问时会触发重新加载表结构、分区等元数据信息
        table.unsetObjectCreated();
        
        // 步骤2: 对于 Hive Metastore 表，记录事件更新时间戳
        // 这个时间戳主要用于增量刷新场景：
        // - 当来自 HMS 事件通知时，updateTime 是事件发生的时间
        // - 后续可以通过比较时间戳，只刷新变更的分区或表结构
        // - 避免每次刷新都需要全量扫描所有分区元数据，提升性能
        if (table instanceof HMSExternalTable && updateTime > 0) {
            ((HMSExternalTable) table).setEventUpdateTime(updateTime);
        }
        
        // 步骤3: 使表级别的元数据缓存失效
        // 表元数据缓存包括：
        // - 表结构（列信息、类型等）
        // - 分区信息（分区列表、分区列等）
        // - 表属性（存储格式、压缩方式等）
        // 失效缓存后，下次查询时将从外部系统（如 HMS、Iceberg Catalog）重新加载最新元数据
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(table);
        
        LOG.info("refresh table {}, id {} from db {} in catalog {}",
                table.getName(), table.getId(), db.getFullName(), db.getCatalog().getName());
    }

    // Refresh partition
    public void refreshPartitions(String catalogName, String dbName, String tableName,
            List<String> partitionNames, long updateTime, boolean ignoreIfNotExists)
            throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("No catalog found with name: " + catalogName);
            }
            return;
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }

        Env.getCurrentEnv().getExtMetaCacheMgr().invalidatePartitionsCache((ExternalTable) table, partitionNames);
        ((HMSExternalTable) table).setEventUpdateTime(updateTime);
    }

    /**
     * 将 Catalog 添加到自动刷新列表中
     * 
     * @param catalogId Catalog 的唯一标识符
     * @param sec 时间数组，长度为2：
     *            - sec[0]: 原始刷新间隔（秒），用户配置的值，用于重置计时器
     *            - sec[1]: 当前剩余时间（秒），倒计时用，每次检查会递减
     * 
     * 示例：如果配置了 metadata_refresh_interval_sec = 3600（1小时）
     *       sec = {3600, 3600}  // 初始时剩余时间等于原始间隔
     */
    public void addToRefreshMap(long catalogId, Integer[] sec) {
        refreshMap.put(catalogId, sec);
    }

    /**
     * 从自动刷新列表中移除 Catalog
     * 当 Catalog 被删除或移除此属性时调用
     */
    public void removeFromRefreshMap(long catalogId) {
        refreshMap.remove(catalogId);
    }

    /**
     * 启动自动刷新定时任务
     * 
     * 使用 ScheduledThreadPoolExecutor 创建一个固定频率的定时任务：
     * - 初始延迟：0秒（立即开始）
     * - 执行频率：每 REFRESH_TIME_SEC（5秒）执行一次检查
     * - 任务内容：遍历所有配置了自动刷新的 Catalog，检查是否需要刷新
     */
    public void start() {
        RefreshTask refreshTask = new RefreshTask();
        this.refreshScheduler.scheduleAtFixedRate(refreshTask, 0, REFRESH_TIME_SEC,
                TimeUnit.SECONDS);
    }

    /**
     * 自动刷新检查任务
     * 
     * 核心逻辑：倒计时机制
     * 
     * 工作流程：
     * 1. 每5秒执行一次检查（由 ScheduledThreadPoolExecutor 控制）
     * 2. 遍历所有配置了自动刷新的 Catalog
     * 3. 对每个 Catalog 的剩余时间进行倒计时（减去5秒）
     * 4. 当剩余时间 <= 0 时，执行刷新操作
     * 5. 刷新完成后，重置剩余时间为原始间隔
     * 
     * 倒计时逻辑说明：
     * - 假设配置了 metadata_refresh_interval_sec = 3600 秒（1小时）
     * - 初始状态：{3600, 3600}
     * - 每5秒：{3600, 3595} → {3600, 3590} → ... → {3600, 5}
     * - 当 {3600, 5} 时，current(5) - REFRESH_TIME_SEC(5) = 0，触发刷新
     * - 刷新后重置：{3600, 3600}，开始新一轮倒计时
     * 
     * 为什么使用倒计时而不是定时器？
     * - 更灵活：每个 Catalog 可以有不同的刷新间隔
     * - 更统一：所有 Catalog 共享同一个定时任务线程，减少线程开销
     * - 更可控：刷新失败不影响其他 Catalog 的刷新计划
     */
    private class RefreshTask implements Runnable {
        @Override
        public void run() {
            // 遍历所有配置了自动刷新的 Catalog
            for (Map.Entry<Long, Integer[]> entry : refreshMap.entrySet()) {
                Long catalogId = entry.getKey();
                Integer[] timeGroup = entry.getValue();
                
                // 从数组中提取原始刷新间隔和当前剩余时间
                Integer original = timeGroup[0];  // 原始间隔，用于重置（例如：3600秒）
                Integer current = timeGroup[1];   // 当前剩余时间（倒计时用）
                
                // ==================== 核心判断逻辑 ====================
                // 触发条件：current - REFRESH_TIME_SEC <= 0
                // 即：current <= 5（当剩余时间 <= 5秒时触发刷新）
                //
                // 为什么是 <= 5秒而不是 <= 0？
                // - 因为检查间隔是固定的5秒（REFRESH_TIME_SEC = 5）
                // - 当剩余时间为5秒时，下次检查（5秒后）刚好到达刷新时间
                // - 这样可以确保在配置的刷新间隔到达时及时触发
                //
                // 示例（配置为3600秒刷新一次）：
                // - {3600, 3600} → 不触发，减5 → {3600, 3595}
                // - {3600, 3595} → 不触发，减5 → {3600, 3590}
                // - ...
                // - {3600, 10}   → 不触发，减5 → {3600, 5}
                // - {3600, 5}    → 触发刷新！因为 5 - 5 = 0 <= 0
                // - {3600, 0}    → 触发刷新！因为 0 - 5 = -5 <= 0
                // ===================================================
                
                if (current - REFRESH_TIME_SEC > 0) {
                    // 情况1：不触发刷新，继续倒计时
                    // 条件：current > 5（剩余时间大于5秒）
                    // 操作：剩余时间减去5秒，继续等待下次检查
                    // 示例：3595 - 5 = 3590，继续等待
                    timeGroup[1] = current - REFRESH_TIME_SEC;
                    refreshMap.put(catalogId, timeGroup);
                } else {
                    // 情况2：触发刷新操作
                    // 条件：current <= 5（剩余时间小于等于5秒）
                    // 说明：已达到或接近配置的刷新间隔，需要执行刷新
                    
                    // 前置检查：验证 Catalog 是否仍然存在（可能已被删除）
                    CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
                    if (catalog != null) {
                        String catalogName = catalog.getName();
                        
                        // 执行刷新操作
                        // invalidCache = true：清空缓存，强制重新加载元数据
                        try {
                            Env.getCurrentEnv().getRefreshManager().handleRefreshCatalog(catalogName, true);
                        } catch (Exception e) {
                            // 刷新失败不阻塞其他 Catalog 的刷新，只记录警告日志
                            LOG.warn("failed to refresh catalog {}", catalogName, e);
                        }

                        // 刷新完成后，重置倒计时器
                        // 将剩余时间重置为原始间隔，开始新一轮倒计时
                        // 示例：{3600, 5} 刷新后 → {3600, 3600}，1小时后再刷新
                        timeGroup[1] = original;
                        refreshMap.put(catalogId, timeGroup);
                    }
                    // 如果 Catalog 已被删除，跳过刷新（refreshMap 会在删除时清理）
                }
            }
        }
    }
}
