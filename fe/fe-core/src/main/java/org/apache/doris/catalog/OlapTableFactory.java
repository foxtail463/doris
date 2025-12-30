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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

public class OlapTableFactory {

    public static class BuildParams {
        public long tableId;
        public String tableName;
        public boolean isTemporary;
        public List<Column> schema;
        public KeysType keysType;
        public PartitionInfo partitionInfo;
        public DistributionInfo distributionInfo;
    }

    public static class OlapTableParams extends BuildParams {
        public TableIndexes indexes;

        public OlapTableParams(boolean isTemporary) {
            this.isTemporary = isTemporary;
        }
    }

    public static class MTMVParams extends BuildParams {
        public MTMVRefreshInfo refreshInfo;
        public String querySql;
        public Map<String, String> mvProperties;
        public MTMVPartitionInfo mvPartitionInfo;
        public MTMVRelation relation;
        public Map<String, String> sessionVariables;
    }

    private BuildParams params;


    public static TableType getTableType(CreateTableInfo createTableInfo) {
        if (createTableInfo instanceof CreateMTMVInfo) {
            return TableType.MATERIALIZED_VIEW;
        } else if (createTableInfo instanceof CreateTableInfo) {
            return TableType.OLAP;
        } else {
            throw new IllegalArgumentException("Invalid DDL statement: " + createTableInfo.toSql());
        }
    }

    public OlapTableFactory init(TableType type, boolean isTemporary) {
        if (type == TableType.OLAP) {
            params = new OlapTableParams(isTemporary);
        } else {
            params = new MTMVParams();
        }
        return this;
    }

    /**
     * 构建并返回表对象
     * 
     * 这是工厂模式的核心方法，根据不同的参数类型创建相应的表对象：
     * 1. OlapTableParams -> 创建标准的OLAP表
     * 2. MTMVParams -> 创建物化视图表
     * 
     * @return 返回创建的表对象（OlapTable或MTMV）
     * @throws IllegalStateException 如果工厂未初始化（params为null）
     */
    public Table build() {
        // 验证工厂是否已初始化：params不能为null
        Preconditions.checkNotNull(params, "The factory isn't initialized.");

        // 根据参数类型选择创建策略
        if (params instanceof OlapTableParams) {
            // 分支1：创建标准OLAP表
            OlapTableParams olapTableParams = (OlapTableParams) params;
            
            // 使用参数创建新的OlapTable实例
            // 包含：表ID、表名、临时表标志、列Schema、键类型、分区信息、分布信息、索引信息
            return new OlapTable(
                    olapTableParams.tableId,                    // 表唯一标识符
                    olapTableParams.tableName,                  // 表名
                    olapTableParams.isTemporary,                // 是否为临时表
                    olapTableParams.schema,                     // 列定义Schema
                    olapTableParams.keysType,                   // 键类型（DUP/UNIQUE/AGG）
                    olapTableParams.partitionInfo,              // 分区策略信息
                    olapTableParams.distributionInfo,           // 数据分布策略
                    olapTableParams.indexes                     // 索引信息（包括物化视图）
            );
        } else {
            // 分支2：创建物化视图表（MTMV）
            MTMVParams mtmvParams = (MTMVParams) params;
            
            // 使用MTMV参数创建物化视图表
            // MTMV是Materialized Table Materialized View的缩写
            return new MTMV(mtmvParams);
        }
    }

    public OlapTableFactory withTableId(long tableId) {
        params.tableId = tableId;
        return this;
    }

    public OlapTableFactory withTableName(String tableName) {
        params.tableName = tableName;
        return this;
    }

    public OlapTableFactory withSchema(List<Column> schema) {
        params.schema = schema;
        return this;
    }

    public OlapTableFactory withKeysType(KeysType keysType) {
        params.keysType = keysType;
        return this;
    }

    public OlapTableFactory withPartitionInfo(PartitionInfo partitionInfo) {
        params.partitionInfo = partitionInfo;
        return this;
    }

    public OlapTableFactory withDistributionInfo(DistributionInfo distributionInfo) {
        params.distributionInfo = distributionInfo;
        return this;
    }

    public OlapTableFactory withIndexes(TableIndexes indexes) {
        Preconditions.checkState(params instanceof OlapTableParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        OlapTableParams olapTableParams = (OlapTableParams) params;
        olapTableParams.indexes = indexes;
        return this;
    }

    public OlapTableFactory withQuerySql(String querySql) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.querySql = querySql;
        return this;
    }

    public OlapTableFactory withMvProperties(Map<String, String> mvProperties) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.mvProperties = mvProperties;
        return this;
    }

    private OlapTableFactory withRefreshInfo(MTMVRefreshInfo refreshInfo) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.refreshInfo = refreshInfo;
        return this;
    }

    private OlapTableFactory withMvPartitionInfo(MTMVPartitionInfo mvPartitionInfo) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.mvPartitionInfo = mvPartitionInfo;
        return this;
    }

    private OlapTableFactory withMvRelation(MTMVRelation relation) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.relation = relation;
        return this;
    }

    private OlapTableFactory withSessionVariables(Map<String, String> sessionVariables) {
        Preconditions.checkState(params instanceof MTMVParams, "Invalid argument for "
                + params.getClass().getSimpleName());
        MTMVParams mtmvParams = (MTMVParams) params;
        mtmvParams.sessionVariables = sessionVariables;
        return this;
    }

    public OlapTableFactory withExtraParams(CreateTableInfo createTableInfo) {
        boolean isMaterializedView = createTableInfo instanceof CreateMTMVInfo;
        if (!isMaterializedView) {
            return withIndexes(new TableIndexes(createTableInfo.getIndexes()));
        } else {
            CreateMTMVInfo createMTMVInfo = (CreateMTMVInfo) createTableInfo;
            return withRefreshInfo(createMTMVInfo.getRefreshInfo())
                .withQuerySql(createMTMVInfo.getQuerySql())
                .withMvProperties(createMTMVInfo.getMvProperties())
                .withMvPartitionInfo(createMTMVInfo.getMvPartitionInfo())
                .withSessionVariables(createMTMVInfo.getSessionVariables())
                .withMvRelation(createMTMVInfo.getRelation());
        }
    }
}
