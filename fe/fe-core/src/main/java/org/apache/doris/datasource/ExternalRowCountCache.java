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

package org.apache.doris.datasource;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.BasicAsyncCacheLoader;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class ExternalRowCountCache {

    private static final Logger LOG = LogManager.getLogger(ExternalRowCountCache.class);
    private final AsyncLoadingCache<RowCountKey, Optional<Long>> rowCountCache;

    // 用途：缓存外部表（含 Hive 等）的行数，降低频繁访问外部元数据源的开销。
    // 策略：使用 Caffeine AsyncLoadingCache，支持过期与刷新策略，行数缺失时异步加载。
    public ExternalRowCountCache(ExecutorService executor) {
        // 1. set expireAfterWrite to 1 day, avoid too many entries
        // 2. set refreshAfterWrite to 10min(default), so that the cache will be refreshed after 10min
        CacheFactory rowCountCacheFactory = new CacheFactory(
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                OptionalLong.of(Config.external_cache_refresh_time_minutes * 60),
                Config.max_external_table_row_count_cache_num,
                false,
                null);
        rowCountCache = rowCountCacheFactory.buildAsyncCache(new RowCountCacheLoader(), executor);
    }

    @Getter
    public static class RowCountKey {
        private final long catalogId;
        private final long dbId;
        private final long tableId;

        public RowCountKey(long catalogId, long dbId, long tableId) {
            this.catalogId = catalogId;
            this.dbId = dbId;
            this.tableId = tableId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof RowCountKey)) {
                return false;
            }
            // 仅基于 tableId 判等/哈希：意味着同一 tableId 在不同 catalog/db 下会被视为同一个键。
            // 注意：这依赖 tableId 在全局唯一（通常满足）。
            return ((RowCountKey) obj).tableId == this.tableId;
        }

        @Override
        public int hashCode() {
            // 与 equals 一致，仅基于 tableId。
            return (int) tableId;
        }
    }

    public static class RowCountCacheLoader extends BasicAsyncCacheLoader<RowCountKey, Optional<Long>> {
        @Override
        protected Optional<Long> doLoad(RowCountKey rowCountKey) {
            try {
                // 通过 catalogId/dbId/tableId 定位表并获取行数，包装为 Optional
                TableIf table = StatisticsUtil.findTable(rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId);
                return Optional.of(table.fetchRowCount());
            } catch (Exception e) {
                String message = String.format("Failed to get table row count with catalogId %s, dbId %s, tableId %s. "
                                + "Reason %s",
                        rowCountKey.catalogId, rowCountKey.dbId, rowCountKey.tableId, e.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(message, e);
                } else {
                    LOG.warn(message);
                }

                // Return Optional.empty() will cache this empty value in memory,
                // so we can't try to load the row count until the cache expire.
                // Throw an exception here will cause too much stack log in fe.out.
                // So we return null when exception happen.
                // Null may raise NPE in caller, but that is expected.
                // We catch that NPE and return a default value -1 without keep the value in cache,
                // so we can trigger the load function to fetch row count again next time in this exception case.
                // 中文补充：返回 null（而非 Optional.empty）可避免错误结果被缓存，允许下次再次尝试加载。
                return null;
            }
        }
    }

    /**
     * Get cached row count for the given table. Return -1 if cached not loaded or table not exists.
     * Cached will be loaded async.
     * @return Cached row count or -1 if not exist
     */
    public long getCachedRowCount(long catalogId, long dbId, long tableId) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<Long>> f = rowCountCache.get(key);
            // Get row count synchronously by default.
            if (ConnectContext.get() == null
                    || ConnectContext.get().getSessionVariable().fetchHiveRowCountSync) {
                // 同步模式：阻塞等待结果，未命中则触发异步加载并等待其完成
                return f.get().orElse(TableIf.UNKNOWN_ROW_COUNT);
            } else {
                // 异步模式：如果已完成则返回，否则不阻塞，返回默认值
                if (f.isDone()) {
                    return f.get().orElse(TableIf.UNKNOWN_ROW_COUNT);
                }
                LOG.info("Row count for table {}.{}.{} is still processing.", catalogId, dbId, tableId);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count", e);
        }
        return TableIf.UNKNOWN_ROW_COUNT;
    }

    /**
     * Get cached row count for the given table if present. Return -1 if cached not loaded.
     * This method will not trigger async loading if cache is missing.
     * @return Cached row count or -1 if not exist
     */
    public long getCachedRowCountIfPresent(long catalogId, long dbId, long tableId) {
        RowCountKey key = new RowCountKey(catalogId, dbId, tableId);
        try {
            CompletableFuture<Optional<Long>> f = rowCountCache.getIfPresent(key);
            if (f == null) {
                // 缓存不存在：不触发加载，直接返回 -1 表示未知
                return -1;
            } else if (f.isDone()) {
                // 已计算完成：返回值（缺失则为 -1）
                return f.get().orElse(-1L);
            }
        } catch (Exception e) {
            LOG.warn("Unexpected exception while returning row count if present", e);
        }
        // 仍在计算中或异常：返回 -1 表示当前不可用
        return -1;
    }

}
