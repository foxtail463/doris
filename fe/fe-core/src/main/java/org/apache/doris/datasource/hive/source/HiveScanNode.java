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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.AcidInfo;
import org.apache.doris.datasource.hive.AcidInfo.DeleteDeltaInfo;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.HiveProperties;
import org.apache.doris.datasource.hive.HiveTransaction;
import org.apache.doris.datasource.hive.source.HiveSplit.HiveSplitCreator;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TTransactionalHiveDeleteDeltaDesc;
import org.apache.doris.thrift.TTransactionalHiveDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Setter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HiveScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(HiveScanNode.class);

    protected final HMSExternalTable hmsTable;
    private HiveTransaction hiveTransaction = null;

    // will only be set in Nereids, for lagency planner, it should be null
    @Setter
    protected SelectedPartitions selectedPartitions = null;

    private DirectoryLister directoryLister;

    private boolean partitionInit = false;
    private final AtomicReference<UserException> batchException = new AtomicReference<>(null);
    private List<HivePartition> prunedPartitions;
    private final Semaphore splittersOnFlight = new Semaphore(NUM_SPLITTERS_ON_FLIGHT);
    private final AtomicInteger numSplitsPerPartition = new AtomicInteger(NUM_SPLITS_PER_PARTITION);

    private boolean skipCheckingAcidVersionFile = false;

    /**
     * * External file scan node for Query Hive table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv,
            DirectoryLister directoryLister) {
        this(id, desc, "HIVE_SCAN_NODE", needCheckColumnPriv, sv, directoryLister);
    }

    public HiveScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
            boolean needCheckColumnPriv, SessionVariable sv,
            DirectoryLister directoryLister) {
        super(id, desc, planNodeName, needCheckColumnPriv, sv);
        hmsTable = (HMSExternalTable) desc.getTable();
        brokerName = hmsTable.getCatalog().bindBrokerName();
        this.directoryLister = directoryLister;
    }

    @Override
    protected void doInitialize() throws UserException {
        super.doInitialize();

        if (hmsTable.isHiveTransactionalTable()) {
            this.hiveTransaction = new HiveTransaction(DebugUtil.printId(ConnectContext.get().queryId()),
                    ConnectContext.get().getQualifiedUser(), hmsTable, hmsTable.isFullAcidTable());
            Env.getCurrentHiveTransactionMgr().register(hiveTransaction);
            skipCheckingAcidVersionFile = sessionVariable.skipCheckingAcidVersionFile;
        }
    }

    /**
     * 获取Hive表的分区信息
     * 对于分区表，从缓存中获取已选择的分区；对于非分区表，创建虚拟分区以统一接口
     * @return 分区列表
     * @throws AnalysisException 分析异常
     */
    protected List<HivePartition> getPartitions() throws AnalysisException {
        // 初始化分区结果列表
        List<HivePartition> resPartitions = Lists.newArrayList();
        // 获取Hive元数据存储缓存管理器
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        // 获取分区列的类型信息
        List<Type> partitionColumnTypes = hmsTable.getPartitionColumnTypes(MvccUtil.getSnapshotFromContext(hmsTable));
        
        if (!partitionColumnTypes.isEmpty()) {
            // 分区表处理逻辑
            Collection<PartitionItem> partitionItems;
            // 分区已经被Nereids优化器在PruneFileScanPartition阶段进行了裁剪，
            // 因此直接使用已选择的分区即可
            this.totalPartitionNum = selectedPartitions.totalPartitionNum;
            partitionItems = selectedPartitions.selectedPartitions.values();
            Preconditions.checkNotNull(partitionItems);
            this.selectedPartitionNum = partitionItems.size();

            // 从缓存中获取分区信息
            // 构建分区值列表，用于从缓存中查询分区信息
            List<List<String>> partitionValuesList = Lists.newArrayListWithCapacity(partitionItems.size());
            for (PartitionItem item : partitionItems) {
                // 将分区项转换为字符串列表格式，供Hive使用
                partitionValuesList.add(
                        ((ListPartitionItem) item).getItems().get(0).getPartitionValuesAsStringListForHive());
            }
            // 从缓存中批量获取分区信息
            resPartitions = cache.getAllPartitionsWithCache(hmsTable, partitionValuesList);
        } else {
            // 非分区表处理逻辑
            // 创建虚拟分区来保存位置和输入格式信息，
            // 这样可以统一接口处理
            HivePartition dummyPartition = new HivePartition(hmsTable.getOrBuildNameMapping(), true,
                    hmsTable.getRemoteTable().getSd().getInputFormat(),
                    hmsTable.getRemoteTable().getSd().getLocation(), null, Maps.newHashMap());
            // 设置分区统计信息
            this.totalPartitionNum = 1;
            this.selectedPartitionNum = 1;
            resPartitions.add(dummyPartition);
        }
        
        // 记录分区获取完成时间，用于查询性能统计
        if (ConnectContext.get().getExecutor() != null) {
            ConnectContext.get().getExecutor().getSummaryProfile().setGetPartitionsFinishTime();
        }
        return resPartitions;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        // 获取待扫描的文件切分（Split）列表的主入口
        // 整体流程：
        // 1) 若分区尚未初始化，则先从元数据缓存中获取并裁剪分区列表；
        // 2) 通过 HiveMetaStoreCache 获取各分区下的文件列表；
        // 3) 根据文件与格式等信息切分为多个 Split；
        // 4) 返回所有可调度的 Split 列表。
        long start = System.currentTimeMillis();
        try {
            if (!partitionInit) {
                // 首次调用时初始化分区（已由 Nereids 可能完成分区裁剪）
                prunedPartitions = getPartitions();
                partitionInit = true;
            }
            HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                    .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
            String bindBrokerName = hmsTable.getCatalog().bindBrokerName();
            // 收集所有文件对应的 Split
            List<Split> allFiles = Lists.newArrayList();
            // 根据分区列表生成文件 Split；内部会根据文件大小、格式等决定是否切分
            getFileSplitByPartitions(cache, prunedPartitions, allFiles, bindBrokerName, numBackends);
            if (ConnectContext.get().getExecutor() != null) {
                // 记录获取分区文件完成时间，用于查询概要统计
                ConnectContext.get().getExecutor().getSummaryProfile().setGetPartitionFilesFinishTime();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get #{} files for table: {}.{}, cost: {} ms",
                        allFiles.size(), hmsTable.getDbName(), hmsTable.getName(),
                        (System.currentTimeMillis() - start));
            }
            return allFiles;
        } catch (Throwable t) {
            // 捕获并封装异常，便于上层处理
            LOG.warn("get file split failed for table: {}", hmsTable.getName(), t);
            throw new UserException(
                "get file split failed for table: " + hmsTable.getName() + ", err: " + Util.getRootCauseMessage(t),
                t);
        }
    }

    @Override
    public void startSplit(int numBackends) {
        if (prunedPartitions.isEmpty()) {
            splitAssignment.finishSchedule();
            return;
        }
        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hmsTable.getCatalog());
        Executor scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
        String bindBrokerName = hmsTable.getCatalog().bindBrokerName();
        AtomicInteger numFinishedPartitions = new AtomicInteger(0);
        CompletableFuture.runAsync(() -> {
            for (HivePartition partition : prunedPartitions) {
                if (batchException.get() != null || splitAssignment.isStop()) {
                    break;
                }
                try {
                    splittersOnFlight.acquire();
                    CompletableFuture.runAsync(() -> {
                        try {
                            List<Split> allFiles = Lists.newArrayList();
                            getFileSplitByPartitions(
                                    cache, Collections.singletonList(partition), allFiles, bindBrokerName, numBackends);
                            if (allFiles.size() > numSplitsPerPartition.get()) {
                                numSplitsPerPartition.set(allFiles.size());
                            }
                            if (splitAssignment.needMoreSplit()) {
                                splitAssignment.addToQueue(allFiles);
                            }
                        } catch (Exception e) {
                            batchException.set(new UserException(e.getMessage(), e));
                        } finally {
                            splittersOnFlight.release();
                            if (batchException.get() != null) {
                                splitAssignment.setException(batchException.get());
                            }
                            if (numFinishedPartitions.incrementAndGet() == prunedPartitions.size()) {
                                splitAssignment.finishSchedule();
                            }
                        }
                    }, scheduleExecutor);
                } catch (Exception e) {
                    // When submitting a task, an exception will be thrown if the task pool(scheduleExecutor) is full
                    batchException.set(new UserException(e.getMessage(), e));
                    break;
                }
            }
            if (batchException.get() != null) {
                splitAssignment.setException(batchException.get());
            }
        });
    }

    @Override
    public boolean isBatchMode() {
        if (!partitionInit) {
            try {
                prunedPartitions = getPartitions();
            } catch (Exception e) {
                return false;
            }
            partitionInit = true;
        }
        int numPartitions = sessionVariable.getNumPartitionsInBatchMode();
        return numPartitions >= 0 && prunedPartitions.size() >= numPartitions;
    }

    @Override
    public int numApproximateSplits() {
        return numSplitsPerPartition.get() * prunedPartitions.size();
    }

    private void getFileSplitByPartitions(HiveMetaStoreCache cache, List<HivePartition> partitions,
            List<Split> allFiles, String bindBrokerName, int numBackends) throws IOException, UserException {
        // 根据分区列表获取文件缓存信息，支持事务表和非事务表两种模式
        List<FileCacheValue> fileCaches;
        if (hiveTransaction != null) {
            // 事务表模式：通过事务管理器获取有效的文件列表
            try {
                fileCaches = getFileSplitByTransaction(cache, partitions, bindBrokerName);
            } catch (Exception e) {
                // 释放共享锁（getValidWriteIds 会获取锁）
                // 如果没有异常，锁会在 `finalizeQuery()` 时释放
                Env.getCurrentHiveTransactionMgr().deregister(hiveTransaction.getQueryId());
                throw e;
            }
        } else {
            // 非事务表模式：直接从缓存获取文件列表
            boolean withCache = Config.max_external_file_cache_num > 0;
            fileCaches = cache.getFilesByPartitions(partitions, withCache, partitions.size() > 1,
                    directoryLister, hmsTable);
        }
        
        // 如果设置了表采样，则先选择文件再进行切分
        if (tableSample != null) {
            List<HiveMetaStoreCache.HiveFileStatus> hiveFileStatuses = selectFiles(fileCaches);
            splitAllFiles(allFiles, hiveFileStatuses);
            return;
        }

        /**
         * 针对 COUNT 聚合下推的优化：
         * 对于 parquet/orc 格式，只需要读取元数据，不需要切分文件
         * 如果切分文件，会多次读取同一个文件的元数据，效率低下
         *
         * - Hive Full ACID 事务表可能需要合并读取，因此不应用此优化
         * - 如果文件格式不是 parquet/orc（如 text），需要切分文件以提高并行度
         */
        boolean needSplit = true;
        if (getPushDownAggNoGroupingOp() == TPushAggOp.COUNT
                && !(hmsTable.isHiveTransactionalTable() && hmsTable.isFullAcidTable())) {
            // 计算总文件数量，用于判断是否需要切分
            int totalFileNum = 0;
            for (FileCacheValue fileCacheValue : fileCaches) {
                if (fileCacheValue.getFiles() != null) {
                    totalFileNum += fileCacheValue.getFiles().size();
                }
            }
            int parallelNum = sessionVariable.getParallelExecInstanceNum();
            // 根据并行度、后端数量和文件数量决定是否需要切分
            needSplit = FileSplitter.needSplitForCountPushdown(parallelNum, numBackends, totalFileNum);
        }
        
        // 遍历所有文件缓存，为每个文件生成对应的 Split
        for (HiveMetaStoreCache.FileCacheValue fileCacheValue : fileCaches) {
            if (fileCacheValue.getFiles() != null) {
                boolean isSplittable = fileCacheValue.isSplittable();
                for (HiveMetaStoreCache.HiveFileStatus status : fileCacheValue.getFiles()) {
                    // 使用 FileSplitter 将文件切分为多个 Split
                    allFiles.addAll(FileSplitter.splitFile(status.getPath(),
                            // 如果不需要切分，设置块大小为 Long.MAX_VALUE 避免文件切分
                            getRealFileSplitSize(needSplit ? status.getBlockSize() : Long.MAX_VALUE),
                            status.getBlockLocations(), status.getLength(), status.getModificationTime(),
                            isSplittable, fileCacheValue.getPartitionValues(),
                            new HiveSplitCreator(fileCacheValue.getAcidInfo())));
                }
            }
        }
    }

    private void splitAllFiles(List<Split> allFiles,
                               List<HiveMetaStoreCache.HiveFileStatus> hiveFileStatuses) throws IOException {
        for (HiveMetaStoreCache.HiveFileStatus status : hiveFileStatuses) {
            allFiles.addAll(FileSplitter.splitFile(status.getPath(), getRealFileSplitSize(status.getBlockSize()),
                    status.getBlockLocations(), status.getLength(), status.getModificationTime(),
                    status.isSplittable(), status.getPartitionValues(),
                    new HiveSplitCreator(status.getAcidInfo())));
        }
    }

    private List<HiveMetaStoreCache.HiveFileStatus> selectFiles(List<FileCacheValue> inputCacheValue) {
        List<HiveMetaStoreCache.HiveFileStatus> fileList = Lists.newArrayList();
        long totalSize = 0;
        for (FileCacheValue value : inputCacheValue) {
            for (HiveMetaStoreCache.HiveFileStatus file : value.getFiles()) {
                file.setSplittable(value.isSplittable());
                file.setPartitionValues(value.getPartitionValues());
                file.setAcidInfo(value.getAcidInfo());
                fileList.add(file);
                totalSize += file.getLength();
            }
        }
        long sampleSize = 0;
        if (tableSample.isPercent()) {
            sampleSize = totalSize * tableSample.getSampleValue() / 100;
        } else {
            long estimatedRowSize = 0;
            for (Column column : hmsTable.getFullSchema()) {
                estimatedRowSize += column.getDataType().getSlotSize();
            }
            sampleSize = estimatedRowSize * tableSample.getSampleValue();
        }
        long selectedSize = 0;
        Collections.shuffle(fileList, new Random(tableSample.getSeek()));
        int index = 0;
        for (HiveMetaStoreCache.HiveFileStatus file : fileList) {
            selectedSize += file.getLength();
            index += 1;
            if (selectedSize >= sampleSize) {
                break;
            }
        }
        return fileList.subList(0, index);
    }

    private List<FileCacheValue> getFileSplitByTransaction(HiveMetaStoreCache cache, List<HivePartition> partitions,
                                                           String bindBrokerName) {
        for (HivePartition partition : partitions) {
            if (partition.getPartitionValues() == null || partition.getPartitionValues().isEmpty()) {
                // this is unpartitioned table.
                continue;
            }
            hiveTransaction.addPartition(partition.getPartitionName(hmsTable.getPartitionColumns()));
        }
        Map<String, String> txnValidIds = hiveTransaction.getValidWriteIds(
                ((HMSExternalCatalog) hmsTable.getCatalog()).getClient());

        return cache.getFilesByTransaction(partitions, txnValidIds, hiveTransaction.isFullAcid(), bindBrokerName);
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return hmsTable.getRemoteTable().getPartitionKeys().stream()
                .map(FieldSchema::getName).filter(partitionKey -> !"".equals(partitionKey))
                .map(String::toLowerCase).collect(Collectors.toList());
    }

    @Override
    public TableIf getTargetTable() {
        return hmsTable;
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        return hmsTable.getFileFormatType(sessionVariable);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof  HiveSplit) {
            HiveSplit hiveSplit = (HiveSplit) split;
            if (hiveSplit.isACID()) {
                hiveSplit.setTableFormatType(TableFormatType.TRANSACTIONAL_HIVE);
                TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
                tableFormatFileDesc.setTableFormatType(hiveSplit.getTableFormatType().value());
                AcidInfo acidInfo = (AcidInfo) hiveSplit.getInfo();
                TTransactionalHiveDesc transactionalHiveDesc = new TTransactionalHiveDesc();
                transactionalHiveDesc.setPartition(acidInfo.getPartitionLocation());
                List<TTransactionalHiveDeleteDeltaDesc> deleteDeltaDescs = new ArrayList<>();
                for (DeleteDeltaInfo deleteDeltaInfo : acidInfo.getDeleteDeltas()) {
                    TTransactionalHiveDeleteDeltaDesc deleteDeltaDesc = new TTransactionalHiveDeleteDeltaDesc();
                    deleteDeltaDesc.setDirectoryLocation(deleteDeltaInfo.getDirectoryLocation());
                    deleteDeltaDesc.setFileNames(deleteDeltaInfo.getFileNames());
                    deleteDeltaDescs.add(deleteDeltaDesc);
                }
                transactionalHiveDesc.setDeleteDeltas(deleteDeltaDescs);
                tableFormatFileDesc.setTransactionalHiveParams(transactionalHiveDesc);
                tableFormatFileDesc.setTableLevelRowCount(-1);
                rangeDesc.setTableFormatParams(tableFormatFileDesc);
            } else {
                TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
                tableFormatFileDesc.setTableFormatType(TableFormatType.HIVE.value());
                tableFormatFileDesc.setTableLevelRowCount(-1);
                rangeDesc.setTableFormatParams(tableFormatFileDesc);
            }
        }
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        return hmsTable.getBackendStorageProperties();
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        TFileAttributes fileAttributes = new TFileAttributes();
        Table table = hmsTable.getRemoteTable();
        // set skip header count
        // TODO: support skip footer count
        fileAttributes.setSkipLines(HiveProperties.getSkipHeaderCount(table));
        String serDeLib = table.getSd().getSerdeInfo().getSerializationLib();
        if (serDeLib.equals(HiveMetaStoreClientHelper.HIVE_TEXT_SERDE)
                || serDeLib.equals(HiveMetaStoreClientHelper.HIVE_MULTI_DELIMIT_SERDE)) {
            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            // set properties of LazySimpleSerDe and MultiDelimitSerDe
            // 1. set column separator (MultiDelimitSerDe supports multi-character delimiters)
            boolean supportMultiChar = serDeLib.equals(HiveMetaStoreClientHelper.HIVE_MULTI_DELIMIT_SERDE);
            textParams.setColumnSeparator(HiveProperties.getFieldDelimiter(table, supportMultiChar));
            // 2. set line delimiter
            textParams.setLineDelimiter(HiveProperties.getLineDelimiter(table));
            // 3. set mapkv delimiter
            textParams.setMapkvDelimiter(HiveProperties.getMapKvDelimiter(table));
            // 4. set collection delimiter
            textParams.setCollectionDelimiter(HiveProperties.getCollectionDelimiter(table));
            // 5. set escape delimiter
            HiveProperties.getEscapeDelimiter(table).ifPresent(d -> textParams.setEscape(d.getBytes()[0]));
            // 6. set null format
            textParams.setNullFormat(HiveProperties.getNullFormat(table));
            fileAttributes.setTextParams(textParams);
            fileAttributes.setHeaderType("");
            fileAttributes.setEnableTextValidateUtf8(
                    sessionVariable.enableTextValidateUtf8);
        } else if (serDeLib.equals(HiveMetaStoreClientHelper.HIVE_OPEN_CSV_SERDE)) {
            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            // set set properties of OpenCSVSerde
            // 1. set column separator
            textParams.setColumnSeparator(HiveProperties.getSeparatorChar(table));
            // 2. set line delimiter
            textParams.setLineDelimiter(HiveProperties.getLineDelimiter(table));
            // 3. set enclose char
            textParams.setEnclose(HiveProperties.getQuoteChar(table).getBytes()[0]);
            // 4. set escape char
            textParams.setEscape(HiveProperties.getEscapeChar(table).getBytes()[0]);
            // 5. set null format with empty string to make csv reader not use "\\N" to represent null
            textParams.setNullFormat("");
            fileAttributes.setTextParams(textParams);
            fileAttributes.setHeaderType("");
            if (textParams.isSetEnclose()) {
                fileAttributes.setTrimDoubleQuotes(true);
            }
            fileAttributes.setEnableTextValidateUtf8(
                    sessionVariable.enableTextValidateUtf8);
        } else if (serDeLib.equals(HiveMetaStoreClientHelper.HIVE_JSON_SERDE)) {
            TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
            textParams.setColumnSeparator("\t");
            textParams.setLineDelimiter("\n");
            fileAttributes.setTextParams(textParams);

            fileAttributes.setJsonpaths("");
            fileAttributes.setJsonRoot("");
            fileAttributes.setNumAsString(true);
            fileAttributes.setFuzzyParse(false);
            fileAttributes.setReadJsonByLine(true);
            fileAttributes.setStripOuterArray(false);
            fileAttributes.setHeaderType("");
        } else if (serDeLib.equals(HiveMetaStoreClientHelper.OPENX_JSON_SERDE)) {
            if (!sessionVariable.isReadHiveJsonInOneColumn()) {
                TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
                textParams.setColumnSeparator("\t");
                textParams.setLineDelimiter("\n");
                fileAttributes.setTextParams(textParams);

                fileAttributes.setJsonpaths("");
                fileAttributes.setJsonRoot("");
                fileAttributes.setNumAsString(true);
                fileAttributes.setFuzzyParse(false);
                fileAttributes.setReadJsonByLine(true);
                fileAttributes.setStripOuterArray(false);
                fileAttributes.setHeaderType("");

                fileAttributes.setOpenxJsonIgnoreMalformed(
                        Boolean.parseBoolean(HiveProperties.getOpenxJsonIgnoreMalformed(table)));
            } else if (sessionVariable.isReadHiveJsonInOneColumn()
                    && hmsTable.firstColumnIsString()) {
                TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
                textParams.setLineDelimiter("\n");
                textParams.setColumnSeparator("\n");
                //First, perform row splitting according to `\n`. When performing column splitting,
                // since there is no `\n`, only one column of data will be generated.
                fileAttributes.setTextParams(textParams);
                fileAttributes.setHeaderType("");
            } else {
                throw new UserException("You set read_hive_json_in_one_column = true, but the first column of table "
                        + hmsTable.getName()
                        + " is not a string column.");
            }
        } else {
            throw new UserException(
                    "unsupported hive table serde: " + serDeLib);
        }

        return fileAttributes;
    }

    @Override
    protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
        TFileCompressType compressType = super.getFileCompressType(fileSplit);
        // hadoop use lz4 blocked codec
        if (compressType == TFileCompressType.LZ4FRAME) {
            compressType = TFileCompressType.LZ4BLOCK;
        }
        return compressType;
    }
}


