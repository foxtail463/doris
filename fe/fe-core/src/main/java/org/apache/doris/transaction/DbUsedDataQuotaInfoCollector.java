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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlCompatibleDatabase;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DbUsedDataQuotaInfoCollector extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DbUsedDataQuotaInfoCollector.class);

    public DbUsedDataQuotaInfoCollector() {
        super("DbUsedDataQuotaInfoCollector", Config.db_used_data_quota_update_interval_secs * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        updateAllDatabaseUsedDataQuota();
    }

    /**
     * 更新所有数据库的已使用数据配额信息
     * 该方法会遍历所有数据库，获取每个数据库的已使用数据配额，并更新到全局事务管理器中
     * 用于在事务开始时快速检查配额，避免每次都实时计算
     */
    private void updateAllDatabaseUsedDataQuota() {
        // 获取当前环境对象
        Env env = Env.getCurrentEnv();
        // 获取所有数据库 ID 列表
        List<Long> dbIdList = env.getInternalCatalog().getDbIds();
        // 获取全局事务管理器，用于更新配额信息
        GlobalTransactionMgrIface globalTransactionMgr = env.getGlobalTransactionMgr();
        
        // 遍历所有数据库
        for (Long dbId : dbIdList) {
            // 获取数据库对象（可能为 null）
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                // 如果数据库不存在，记录警告并跳过
                LOG.warn("Database [" + dbId + "] does not exist, skip to update database used data quota");
                continue;
            }
            // 跳过 MySQL 兼容数据库（不需要配额管理）
            if (db instanceof MysqlCompatibleDatabase) {
                continue;
            }
            try {
                // 获取数据库已使用的数据配额（字节数）
                long usedDataQuotaBytes = db.getUsedDataQuota();
                // 更新全局事务管理器中的配额信息，供事务开始时快速检查使用
                globalTransactionMgr.updateDatabaseUsedQuotaData(dbId, usedDataQuotaBytes);
                // 如果启用调试日志，记录更新信息
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update database[{}] used data quota bytes : {}.", db.getFullName(), usedDataQuotaBytes);
                }
            } catch (AnalysisException e) {
                // 如果更新失败，记录警告日志但不中断其他数据库的更新
                LOG.warn("Update database[" + db.getFullName() + "] used data quota failed", e);
            }
        }
    }
}
