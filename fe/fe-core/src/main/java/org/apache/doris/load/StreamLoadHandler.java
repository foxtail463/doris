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

package org.apache.doris.load;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.nereids.load.NereidsCloudStreamLoadPlanner;
import org.apache.doris.nereids.load.NereidsStreamLoadPlanner;
import org.apache.doris.nereids.load.NereidsStreamLoadTask;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class StreamLoadHandler {
    private static final Logger LOG = LogManager.getLogger(StreamLoadHandler.class);

    private TStreamLoadPutRequest request;
    private Boolean isMultiTableRequest;
    private AtomicInteger multiTableFragmentInstanceIdIndex;
    private TStreamLoadPutResult result;
    private String clientAddr;
    private Database db;
    private List<OlapTable> tables = Lists.newArrayList();
    private long timeoutMs;
    private List fragmentParams = Lists.newArrayList();

    public StreamLoadHandler(TStreamLoadPutRequest request, AtomicInteger indexId,
            TStreamLoadPutResult result, String clientAddr) {
        this.request = request;
        this.timeoutMs = request.isSetThriftRpcTimeoutMs() ? request.getThriftRpcTimeoutMs() : 5000;
        this.isMultiTableRequest = indexId != null;
        this.multiTableFragmentInstanceIdIndex = indexId;
        this.result = result;
        this.clientAddr = clientAddr;
    }

    /**
     * Select a random backend in the given cloud cluster.
     *
     * @param clusterName cloud cluster name
     * @throws LoadException if there is no available backend
     */
    public static Backend selectBackend(String clusterName) throws LoadException {
        List<Backend> backends = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getBackendsByClusterName(clusterName)
                .stream().filter(Backend::isLoadAvailable)
                .collect(Collectors.toList());

        if (backends.isEmpty()) {
            LOG.warn("No available backend for stream load redirect, cluster name {}", clusterName);
            throw new LoadException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", cluster: " + clusterName);
        }

        // TODO: add a more sophisticated algorithm to select backend
        SecureRandom rand = new SecureRandom();
        int randomIndex = rand.nextInt(backends.size());
        return backends.get(randomIndex);
    }

    public void setCloudCluster() throws UserException {
        if (ConnectContext.get() != null) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("stream load put request: {}", request);
        }
        // create connect context
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setQueryId(request.getLoadId());
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(request.getUser(), "%"));
        ctx.setBackendId(request.getBackendId());
        ctx.setThreadLocalInfo();

        if (!Config.isCloudMode()) {
            return;
        }

        ctx.setRemoteIP(request.isSetAuthCode() ? clientAddr : request.getUserIp());
        String userName = ClusterNamespace.getNameFromFullName(request.getUser());
        if (!request.isSetToken() && !request.isSetAuthCode() && !Strings.isNullOrEmpty(userName)) {
            List<UserIdentity> currentUser = Lists.newArrayList();
            try {
                Env.getCurrentEnv().getAuth().checkPlainPassword(userName,
                        request.getUserIp(), request.getPasswd(), currentUser);
            } catch (AuthenticationException e) {
                throw new UserException(e.formatErrMsg());
            }
            Preconditions.checkState(currentUser.size() == 1);
            ctx.setCurrentUserIdentity(currentUser.get(0));
        }
        if ((request.isSetToken() || request.isSetAuthCode()) && request.isSetBackendId()) {
            long backendId = request.getBackendId();
            Backend backend = Env.getCurrentSystemInfo().getBackend(backendId);
            Preconditions.checkNotNull(backend);
            ctx.setCloudCluster(backend.getCloudClusterName());
            return;
        }
        if (!Strings.isNullOrEmpty(request.getCloudCluster())) {
            if (Strings.isNullOrEmpty(request.getUser())) {
                // mysql load
                ctx.setCloudCluster(request.getCloudCluster());
            } else {
                // stream load
                ((CloudEnv) Env.getCurrentEnv()).changeCloudCluster(request.getCloudCluster(), ctx);
            }
        }
    }

    private void setDbAndTable() throws UserException, MetaNotFoundException {
        Env env = Env.getCurrentEnv();
        String fullDbName = request.getDb();
        db = env.getInternalCatalog().getDbNullable(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        List<String> tableNames = null;
        if (isMultiTableRequest) {
            tableNames = request.getTableNames();
        } else {
            tableNames = Lists.newArrayList();
            tableNames.add(request.getTbl());
        }

        for (String tableName : tableNames) {
            Table table = db.getTableOrMetaException(tableName, TableType.OLAP);
            if (request.getGroupCommitMode() != null
                    && !request.getGroupCommitMode().equals("off_mode")) {
                if (!((OlapTable) table).getTableProperty().getUseSchemaLightChange()) {
                    throw new UserException(
                            "table light_schema_change is false, can't do stream load with group commit mode");
                }
                if (Env.getCurrentEnv().getGroupCommitManager().isBlock(table.getId())) {
                    String msg = "insert table " + table.getId() + GroupCommitPlanner.SCHEMA_CHANGE;
                    LOG.info(msg);
                    throw new AnalysisException(msg);
                }
            }
            if (table.isTemporary()) {
                throw new UserException("Do not support load for temporary table " + tableName);
            }
            tables.add((OlapTable) table);
        }

        if (tables.isEmpty()) {
            throw new MetaNotFoundException("table not found");
        }

        if (result != null) {
            OlapTable olapTable = tables.get(0);
            result.setDbId(db.getId());
            result.setTableId(olapTable.getId());
            result.setBaseSchemaVersion(olapTable.getBaseSchemaVersion());
        }
    }

    /**
     * For first-class multi-table scenarios, we should store the mapping between Txn and data source type in a common
     * place. Since there is only Kafka now, we should do this first.
     */
    private void buildMultiTableStreamLoadTask(NereidsStreamLoadTask baseTaskInfo, long txnId) {
        try {
            RoutineLoadJob routineLoadJob = Env.getCurrentEnv().getRoutineLoadManager()
                    .getRoutineLoadJobByMultiLoadTaskTxnId(txnId);
            if (routineLoadJob == null) {
                return;
            }
            baseTaskInfo.setMultiTableBaseTaskInfo(routineLoadJob);
        } catch (Exception e) {
            LOG.warn("failed to build multi table stream load task: {}", e.getMessage());
        }
    }

    /**
     * 为单个表生成Stream Load执行计划
     * 这是Stream Load的核心入口方法，负责创建数据加载的执行计划
     * 
     * @param table 目标OLAP表
     * @throws UserException 当获取表锁超时或创建计划时出现错误
     */
    public void generatePlan(OlapTable table) throws UserException {
        // ==================== 第一步：获取表读锁 ====================
        // 尝试获取表的读锁，确保在规划过程中表结构不会发生变化
        if (!table.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException(
                    "get table read lock timeout, database=" + request.getDb() + ",table=" + table.getName());
        }
        try {
            // ==================== 第二步：创建Stream Load任务 ====================
            // 从Thrift请求中创建Nereids Stream Load任务对象
            NereidsStreamLoadTask streamLoadTask = NereidsStreamLoadTask.fromTStreamLoadPutRequest(request);
            
            // 如果是多表请求，需要构建多表Stream Load任务信息
            if (isMultiTableRequest) {
                buildMultiTableStreamLoadTask(streamLoadTask, request.getTxnId());
            }

            // ==================== 第三步：创建规划器 ====================
            // 根据运行模式选择不同的规划器
            NereidsStreamLoadPlanner planner = null;
            if (Config.isCloudMode()) {
                // 云模式：使用云版本的Stream Load规划器
                planner = new NereidsCloudStreamLoadPlanner(db, table, streamLoadTask, request.getCloudCluster());
            } else {
                // 本地模式：使用标准版本的Stream Load规划器
                planner = new NereidsStreamLoadPlanner(db, table, streamLoadTask);
            }
            
            // ==================== 第四步：生成执行计划 ====================
            // 获取片段实例ID索引（用于多表场景）
            int index = multiTableFragmentInstanceIdIndex != null
                    ? multiTableFragmentInstanceIdIndex.getAndIncrement() : 0;
            
            // 调用规划器的plan方法生成执行计划
            // 这里就是调用我们之前分析的NereidsStreamLoadPlanner.plan()方法
            TPipelineFragmentParams result = null;
            result = planner.plan(streamLoadTask.getId(), index);
            
            // ==================== 第五步：设置计划参数 ====================
            // 设置表名
            result.setTableName(table.getName());
            // 设置FE进程UUID
            result.query_options.setFeProcessUuid(ExecuteEnv.getInstance().getProcessUUID());
            // 设置是否为MOW（Merge On Write）表
            result.setIsMowTable(table.getEnableUniqueKeyMergeOnWrite());
            // 将生成的计划添加到片段参数列表中
            fragmentParams.add(result);

            // ==================== 第六步：处理事务状态 ====================
            // 如果不是组提交模式，需要更新事务状态
            if (StringUtils.isEmpty(streamLoadTask.getGroupCommit())) {
                // 获取事务状态
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(db.getId(), request.getTxnId());
                if (txnState == null) {
                    throw new UserException("txn does not exist: " + request.getTxnId());
                }
                // 添加表索引到事务状态
                txnState.addTableIndexes(table);
                
                // 如果是部分更新模式，需要设置部分更新的schema
                TUniqueKeyUpdateMode uniqueKeyUpdateMode = request.getUniqueKeyUpdateMode();
                if (uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                        || uniqueKeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS) {
                    txnState.setSchemaForPartialUpdate(table);
                }
            }
        } finally {
            // ==================== 第七步：释放表锁 ====================
            // 确保在任何情况下都释放表读锁
            table.readUnlock();
        }
    }

    public void generatePlan() throws UserException, MetaNotFoundException {
        setCloudCluster();
        setDbAndTable();
        for (OlapTable table : tables) {
            generatePlan(table);
        }
    }

    public List getFragmentParams() {
        return fragmentParams;
    }
}
