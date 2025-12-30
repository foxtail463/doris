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

package org.apache.doris.qe.cache;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.planner.PartitionColumnFilter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convert the range of the partition to the list
 * all partition by day/week/month split to day list
 */
public class PartitionRange {
    private static final Logger LOG = LogManager.getLogger(PartitionRange.class);

    public class PartitionSingle {
        private Partition partition;
        private PartitionKey partitionKey;
        private long partitionId;
        private PartitionKeyType cacheKey;
        private boolean fromCache;
        private boolean tooNew;

        public Partition getPartition() {
            return partition;
        }

        public void setPartition(Partition partition) {
            this.partition = partition;
        }

        public PartitionKey getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(PartitionKey key) {
            this.partitionKey = key;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(long partitionId) {
            this.partitionId = partitionId;
        }

        public PartitionKeyType getCacheKey() {
            return cacheKey;
        }

        public void setCacheKey(PartitionKeyType cacheKey) {
            this.cacheKey.clone(cacheKey);
        }

        public boolean isFromCache() {
            return fromCache;
        }

        public void setFromCache(boolean fromCache) {
            this.fromCache = fromCache;
        }

        public boolean isTooNew() {
            return tooNew;
        }

        public void setTooNew(boolean tooNew) {
            this.tooNew = tooNew;
        }

        public PartitionSingle() {
            this.partitionId = 0;
            this.cacheKey = new PartitionKeyType();
            this.fromCache = false;
            this.tooNew = false;
        }

        public void debug() {
            if (partition != null) {
                LOG.info("partition id {}, cacheKey {}, version {}, time {}, fromCache {}, tooNew {} ",
                        partitionId, cacheKey.realValue(),
                        partition.getVisibleVersion(), partition.getVisibleVersionTime(),
                        fromCache, tooNew);
            } else {
                LOG.info("partition id {}, cacheKey {}, fromCache {}, tooNew {} ", partitionId,
                        cacheKey.realValue(), fromCache, tooNew);
            }
        }
    }

    public enum KeyType {
        DEFAULT,
        LONG,
        DATE,
        DATETIME,
        TIME
    }

    public static class PartitionKeyType {
        private DateTimeFormatter df8 = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());
        private DateTimeFormatter df10 = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

        public KeyType keyType = KeyType.DEFAULT;
        public long value;
        public Date date;

        public boolean init(Type type, String str) {
            switch (type.getPrimitiveType()) {
                case DATE:
                case DATEV2:
                    try {
                        date = Date.from(
                                LocalDate.parse(str, df10).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
                    } catch (Exception e) {
                        LOG.warn("parse error str{}.", str);
                        return false;
                    }
                    keyType = KeyType.DATE;
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    value = Long.parseLong(str);
                    keyType = KeyType.LONG;
                    break;
                default:
                    LOG.info("PartitionCache not support such key type {}", type.toSql());
                    return false;
            }
            return true;
        }

        public boolean init(Type type, LiteralExpr expr) {
            switch (type.getPrimitiveType()) {
                case BOOLEAN:
                case TIMEV2:
                case DATETIME:
                case DATETIMEV2:
                case TIMESTAMPTZ:
                case FLOAT:
                case DOUBLE:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                case CHAR:
                case VARCHAR:
                case STRING:
                case LARGEINT:
                    LOG.info("PartitionCache not support such key type {}", type.toSql());
                    return false;
                case DATE:
                case DATEV2:
                    date = getDateValue(expr);
                    keyType = KeyType.DATE;
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    value = expr.getLongValue();
                    keyType = KeyType.LONG;
                    break;
                default:
                    return true;
            }
            return true;
        }

        public void clone(PartitionKeyType key) {
            keyType = key.keyType;
            value = key.value;
            date = key.date;
        }

        public void add(int num) {
            if (keyType == KeyType.DATE) {
                date = new Date(date.getTime() + num * 3600 * 24 * 1000);
            } else {
                value += num;
            }
        }

        public String toString() {
            if (keyType == KeyType.DEFAULT) {
                return "";
            } else if (keyType == KeyType.DATE) {
                return df10.format(LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));
            } else {
                return String.valueOf(value);
            }
        }

        public long realValue() {
            if (keyType == KeyType.DATE) {
                return Long.parseLong(df8.format(LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault())));
            } else {
                return value;
            }
        }

        private Date getDateValue(LiteralExpr expr) {
            Preconditions.checkArgument(expr.getType() == Type.DATE || expr.getType() == Type.DATEV2);
            value = expr.getLongValue();
            Date dt = null;
            try {
                dt = Date.from(LocalDate.parse(String.valueOf(value), df8).atStartOfDay().atZone(ZoneId.systemDefault())
                        .toInstant());
            } catch (Exception e) {
                // CHECKSTYLE IGNORE THIS LINE
            }
            return dt;
        }
    }

    private CompoundPredicate partitionKeyPredicate;
    private OlapTable olapTable;
    private RangePartitionInfo rangePartitionInfo;
    private Column partitionColumn;
    private List<PartitionSingle> partitionSingleList;

    public CompoundPredicate getPartitionKeyPredicate() {
        return partitionKeyPredicate;
    }

    public void setPartitionKeyPredicate(CompoundPredicate partitionKeyPredicate) {
        this.partitionKeyPredicate = partitionKeyPredicate;
    }

    public RangePartitionInfo getRangePartitionInfo() {
        return rangePartitionInfo;
    }

    public void setRangePartitionInfo(RangePartitionInfo rangePartitionInfo) {
        this.rangePartitionInfo = rangePartitionInfo;
    }

    public Column getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(Column partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public List<PartitionSingle> getPartitionSingleList() {
        return partitionSingleList;
    }

    public PartitionRange() {
    }

    public PartitionRange(CompoundPredicate partitionKeyPredicate, OlapTable olapTable,
                          RangePartitionInfo rangePartitionInfo) {
        this.partitionKeyPredicate = partitionKeyPredicate;
        this.olapTable = olapTable;
        this.rangePartitionInfo = rangePartitionInfo;
        this.partitionSingleList = Lists.newArrayList();
    }

    /**
     * analytics PartitionKey and PartitionInfo
     *
     * @return
     */
    public boolean analytics() {
        if (rangePartitionInfo.getPartitionColumns().size() != 1) {
            return false;
        }
        partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        PartitionColumnFilter filter = createPartitionFilter(this.partitionKeyPredicate, partitionColumn);
        try {
            if (!buildPartitionKeyRange(filter, partitionColumn)) {
                return false;
            }
            getTablePartitionList(olapTable);
        } catch (AnalysisException e) {
            LOG.warn("get partition range failed, because:", e);
            return false;
        }
        return true;
    }

    public boolean setCacheFlag(long cacheKey) {
        boolean find = false;
        for (PartitionSingle single : partitionSingleList) {
            if (single.getCacheKey().realValue() == cacheKey) {
                single.setFromCache(true);
                find = true;
                break;
            }
        }
        return find;
    }

    public boolean setTooNewByID(long partitionId) {
        boolean find = false;
        for (PartitionSingle single : partitionSingleList) {
            if (single.getPartition().getId() == partitionId) {
                single.setTooNew(true);
                find = true;
                break;
            }
        }
        return find;
    }

    public boolean setTooNewByKey(long cacheKey) {
        boolean find = false;
        for (PartitionSingle single : partitionSingleList) {
            if (single.getCacheKey().realValue() == cacheKey) {
                single.setTooNew(true);
                find = true;
                break;
            }
        }
        return find;
    }

    /**
     * Only the range query of the key of the partition is supported, and the separated partition key query is not
     * supported.
     * Because a query can only be divided into two parts, part1 get data from cache, part2 fetch_data by scan node
     * from BE.
     * Partition cache : 20191211-20191215
     * Hit cache parameter : [20191211 - 20191215], [20191212 - 20191214], [20191212 - 20191216],[20191210 - 20191215]
     * Miss cache parameter: [20191210 - 20191216]
     * So hit range is full, left or right, not support middle now
     */
    public Cache.HitRange buildDiskPartitionRange(List<PartitionSingle> rangeList) {
        Cache.HitRange hitRange = Cache.HitRange.None;
        if (partitionSingleList.size() == 0) {
            return hitRange;
        }
        int begin = partitionSingleList.size() - 1;
        int end = 0;
        for (int i = 0; i < partitionSingleList.size(); i++) {
            if (!partitionSingleList.get(i).isFromCache()) {
                if (begin > i) {
                    begin = i;
                }
                if (end < i) {
                    end = i;
                }
            }
        }
        if (end < begin) {
            hitRange = Cache.HitRange.Full;
            return hitRange;
        } else if (begin > 0 && end == partitionSingleList.size() - 1) {
            hitRange = Cache.HitRange.Left;
        } else if (begin == 0 && end < partitionSingleList.size() - 1) {
            hitRange = Cache.HitRange.Right;
        } else if (begin > 0 && end < partitionSingleList.size() - 1) {
            hitRange = Cache.HitRange.Middle;
        } else {
            hitRange = Cache.HitRange.None;
        }
        rangeList.add(partitionSingleList.get(begin));
        rangeList.add(partitionSingleList.get(end));
        LOG.info("the new range for scan be is [{},{}], hit range", rangeList.get(0).getCacheKey().realValue(),
                rangeList.get(1).getCacheKey().realValue(), hitRange);
        return hitRange;
    }

    /**
     * Gets the partition range that needs to be updated
     * @return
     */
    public List<PartitionSingle> buildUpdatePartitionRange() {
        List<PartitionSingle> updateList = Lists.newArrayList();
        for (PartitionSingle single : partitionSingleList) {
            if (!single.isFromCache() && !single.isTooNew()) {
                updateList.add(single);
            }
        }
        return updateList;
    }

    /**
     * 重写谓词以利用分区缓存
     * 
     * 该函数根据分区范围信息重写查询谓词，将原始谓词转换为可以利用分区缓存的形式。
     * 通过将谓词中的字面量值调整为分区边界值，可以最大化缓存命中率。
     * 
     * 重写策略：
     * 1. 只处理 AND 连接的复合谓词
     * 2. 将范围谓词调整为分区边界值
     * 3. 保持 IN 谓词不变（直接利用缓存）
     * 
     * 示例转换：
     * 原始谓词：date_col >= '2023-01-15' AND date_col < '2023-01-20'
     * 分区范围：['2023-01-01', '2023-01-31'] (对应分区 p1)
     * 重写后：date_col >= '2023-01-01' AND date_col < '2023-02-01'
     * 
     * @param predicate 要重写的复合谓词
     * @param rangeList 分区范围列表，包含分区的边界信息
     * @return 是否成功重写谓词
     */
    public boolean rewritePredicate(CompoundPredicate predicate, List<PartitionSingle> rangeList) {
        // 1. 检查谓词操作符：只处理 AND 连接的谓词
        if (predicate.getOp() != CompoundPredicate.Operator.AND) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("predicate op {}", predicate.getOp().toString());
            }
            return false;
        }
        
        // 2. 遍历谓词中的每个子表达式
        for (Expr expr : predicate.getChildren()) {
            if (expr instanceof BinaryPredicate) {
                // 处理二元谓词（>, <, >=, <=, =, !=）
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                BinaryPredicate.Operator op = binPredicate.getOp();
                
                // 2.1 检查谓词格式：必须是二元谓词
                if (binPredicate.getChildren().size() != 2) {
                    LOG.info("binary predicate children size {}", binPredicate.getChildren().size());
                    continue;
                }
                
                // 2.2 跳过不等谓词：!= 操作符不支持缓存优化
                if (op == BinaryPredicate.Operator.NE) {
                    LOG.info("binary predicate op {}", op.toString());
                    continue;
                }
                
                // 2.3 根据操作符类型调整分区边界值
                PartitionKeyType key = new PartitionKeyType();
                switch (op) {
                    case LE: // <= 操作符：使用分区上边界
                        key.clone(rangeList.get(1).getCacheKey());
                        break;
                    case LT: // < 操作符：使用分区上边界减1
                        key.clone(rangeList.get(1).getCacheKey());
                        key.add(1);
                        break;
                    case GE: // >= 操作符：使用分区下边界
                        key.clone(rangeList.get(0).getCacheKey());
                        break;
                    case GT: // > 操作符：使用分区下边界加1
                        key.clone(rangeList.get(0).getCacheKey());
                        key.add(-1);
                        break;
                    default:
                        break;
                }
                
                // 2.4 创建新的字面量表达式
                LiteralExpr newLiteral;
                if (key.keyType == KeyType.DATE) {
                    // 日期类型：创建日期字面量
                    try {
                        newLiteral = new DateLiteral(key.toString(), Type.DATE);
                    } catch (Exception e) {
                        LOG.warn("Date's format is error {},{}", key.toString(), e);
                        continue;
                    }
                } else if (key.keyType == KeyType.LONG) {
                    // 长整型：创建整数字面量
                    newLiteral = new IntLiteral(key.realValue());
                } else {
                    // 不支持的类型：跳过
                    LOG.warn("Partition cache not support type {}", key.keyType);
                    continue;
                }

                // 2.5 替换谓词中的字面量值
                if (binPredicate.getChild(1) instanceof LiteralExpr) {
                    // 右操作数是字面量：替换右操作数
                    binPredicate.removeNode(1);
                    binPredicate.addChild(newLiteral);
                } else if (binPredicate.getChild(0) instanceof LiteralExpr) {
                    // 左操作数是字面量：替换左操作数
                    binPredicate.removeNode(0);
                    binPredicate.setChild(0, newLiteral);
                } else {
                    // 没有字面量操作数：跳过
                    continue;
                }
            } else if (expr instanceof InPredicate) {
                // 处理 IN 谓词：检查是否可以直接利用缓存
                InPredicate inPredicate = (InPredicate) expr;
                if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                    // 不是字面量 IN 谓词或是否定 IN 谓词：跳过
                    continue;
                }
            }
        }
        return true;
    }

    /**
     * Get partition info from SQL Predicate and OlapTable
     * Pair<PartitionID, PartitionKey>
     * PARTITION BY RANGE(`olap_date`)
     * ( PARTITION p20200101 VALUES [("20200101"), ("20200102")),
     * PARTITION p20200102 VALUES [("20200102"), ("20200103")) )
     */
    private void getTablePartitionList(OlapTable table) {
        Map<Long, PartitionItem> range = rangePartitionInfo.getIdToItem(false);
        for (Map.Entry<Long, PartitionItem> entry : range.entrySet()) {
            Long partId = entry.getKey();
            for (PartitionSingle single : partitionSingleList) {
                if (((RangePartitionItem) entry.getValue()).getItems().contains(single.getPartitionKey())) {
                    if (single.getPartitionId() == 0) {
                        single.setPartitionId(partId);
                    }
                }
            }
        }

        for (PartitionSingle single : partitionSingleList) {
            single.setPartition(table.getPartition(single.getPartitionId()));
        }

        // filter the partitions in predicate but not in OlapTable
        partitionSingleList =
                partitionSingleList.stream().filter(p -> p.getPartition() != null).collect(Collectors.toList());
    }

    /**
     * Get value range of partition column from predicate
     */
    private boolean buildPartitionKeyRange(PartitionColumnFilter partitionColumnFilter,
                                           Column partitionColumn) throws AnalysisException {
        if (partitionColumnFilter.lowerBound == null || partitionColumnFilter.upperBound == null) {
            LOG.info("filter is null");
            return false;
        }
        PartitionKeyType begin = new PartitionKeyType();
        PartitionKeyType end = new PartitionKeyType();
        begin.init(partitionColumn.getType(), partitionColumnFilter.lowerBound);
        end.init(partitionColumn.getType(), partitionColumnFilter.upperBound);

        if (!partitionColumnFilter.lowerBoundInclusive) {
            begin.add(1);
        }
        if (!partitionColumnFilter.upperBoundInclusive) {
            end.add(-1);
        }
        if (begin.realValue() > end.realValue()) {
            LOG.info("partition range begin {}, end {}", begin, end);
            return false;
        }

        if (end.realValue() - begin.realValue() > Config.cache_result_max_row_count) {
            LOG.info("partition key range is too large, begin {}, end {}", begin.realValue(), end.realValue());
            return false;
        }

        while (begin.realValue() <= end.realValue()) {
            PartitionKey key = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue(begin.toString())),
                    Lists.newArrayList(partitionColumn));
            PartitionSingle single = new PartitionSingle();
            single.setCacheKey(begin);
            single.setPartitionKey(key);
            partitionSingleList.add(single);
            begin.add(1);
        }
        return true;
    }

    private PartitionColumnFilter createPartitionFilter(CompoundPredicate partitionKeyPredicate,
                                                        Column partitionColumn) {
        if (partitionKeyPredicate.getOp() != CompoundPredicate.Operator.AND) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not and op");
            }
            return null;
        }
        PartitionColumnFilter partitionColumnFilter = new PartitionColumnFilter();
        for (Expr expr : partitionKeyPredicate.getChildren()) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binPredicate = (BinaryPredicate) expr;
                BinaryPredicate.Operator op = binPredicate.getOp();
                if (binPredicate.getChildren().size() != 2) {
                    LOG.warn("child size {}", binPredicate.getChildren().size());
                    continue;
                }
                if (binPredicate.getOp() == BinaryPredicate.Operator.NE) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("not support NE operator");
                    }
                    continue;
                }
                Expr slotBinding;
                if (binPredicate.getChild(1) instanceof LiteralExpr) {
                    slotBinding = binPredicate.getChild(1);
                } else if (binPredicate.getChild(0) instanceof LiteralExpr) {
                    slotBinding = binPredicate.getChild(0);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("not find LiteralExpr");
                    }
                    continue;
                }

                LiteralExpr literal = (LiteralExpr) slotBinding;
                switch (op) {
                    case EQ: //=
                        partitionColumnFilter.setLowerBound(literal, true);
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LE: //<=
                        partitionColumnFilter.setUpperBound(literal, true);
                        break;
                    case LT: //<
                        partitionColumnFilter.setUpperBound(literal, false);
                        break;
                    case GE: //>=
                        partitionColumnFilter.setLowerBound(literal, true);

                        break;
                    case GT: //>
                        partitionColumnFilter.setLowerBound(literal, false);
                        break;
                    default:
                        break;
                }
            } else if (expr instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) expr;
                if (!inPredicate.isLiteralChildren() || inPredicate.isNotIn()) {
                    continue;
                }
                partitionColumnFilter.setInPredicate(inPredicate);
            }
        }
        return partitionColumnFilter;
    }
}
