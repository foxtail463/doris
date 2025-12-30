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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndId;
import org.apache.doris.nereids.rules.expression.rules.SortedPartitionRanges.PartitionItemAndRange;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * PartitionPruner
 */
public class PartitionPruner extends DefaultExpressionRewriter<Void> {
    private final List<OnePartitionEvaluator<?>> partitions;
    private final Expression partitionPredicate;

    /** Different type of table may have different partition prune behavior. */
    public enum PartitionTableType {
        OLAP,
        EXTERNAL
    }

    private PartitionPruner(List<OnePartitionEvaluator<?>> partitions, Expression partitionPredicate) {
        this.partitions = Objects.requireNonNull(partitions, "partitions cannot be null");
        this.partitionPredicate = Objects.requireNonNull(partitionPredicate.accept(this, null),
                "partitionPredicate cannot be null");
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, Void context) {
        // Date cp Date is not supported in BE storage engine. So cast to DateTime in SimplifyComparisonPredicate
        // for easy process partition prune, we convert back to date compare date here
        // see more info in SimplifyComparisonPredicate
        Expression left = cp.left();
        Expression right = cp.right();
        if (left.getDataType() != DateTimeType.INSTANCE || right.getDataType() != DateTimeType.INSTANCE) {
            return cp;
        }
        if (!(left instanceof DateTimeLiteral) && !(right instanceof DateTimeLiteral)) {
            return cp;
        }
        if (left instanceof DateTimeLiteral && ((DateTimeLiteral) left).isMidnight()
                && right instanceof Cast
                && ((Cast) right).child() instanceof SlotReference
                && ((Cast) right).child().getDataType().isDateType()) {
            DateTimeLiteral dt = (DateTimeLiteral) left;
            Cast cast = (Cast) right;
            return cp.withChildren(
                    ImmutableList.of(new DateLiteral(dt.getYear(), dt.getMonth(), dt.getDay()), cast.child())
            );
        } else if (right instanceof DateTimeLiteral && ((DateTimeLiteral) right).isMidnight()
                && left instanceof Cast
                && ((Cast) left).child() instanceof SlotReference
                && ((Cast) left).child().getDataType().isDateType()) {
            DateTimeLiteral dt = (DateTimeLiteral) right;
            Cast cast = (Cast) left;
            return cp.withChildren(ImmutableList.of(
                    cast.child(),
                    new DateLiteral(dt.getYear(), dt.getMonth(), dt.getDay()))
            );
        } else {
            return cp;
        }
    }

    /** prune */
    public <K extends Comparable<K>> Pair<List<K>, Boolean> prune() {
        Builder<K> scanPartitionIdents = ImmutableList.builder();
        boolean canPredicatePruned = true;
        for (OnePartitionEvaluator partition : partitions) {
            Pair<Boolean, Boolean> res = canBePrunedOut(partitionPredicate, partition);
            if (!res.first) {
                canPredicatePruned = canPredicatePruned && res.second;
                scanPartitionIdents.add((K) partition.getPartitionIdent());
            }
        }
        return Pair.of(scanPartitionIdents.build(), canPredicatePruned);
    }

    public static <K extends Comparable<K>> Pair<List<K>, Optional<Expression>> prune(List<Slot> partitionSlots,
            Expression partitionPredicate,
            Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType) {
        return prune(partitionSlots, partitionPredicate, idToPartitions,
                cascadesContext, partitionTableType, Optional.empty());
    }

    /**
     * 分区裁剪的公共入口方法
     * 
     * 该方法负责根据分区谓词条件裁剪不必要的分区，是分区裁剪功能的核心入口。
     * 主要功能包括：
     * 1. 性能监控：记录分区裁剪的执行时间
     * 2. 异常处理：确保性能统计的准确性
     * 3. 内部调用：调用实际的分区裁剪逻辑
     * 4. 统计收集：将执行时间添加到性能统计中
     * 
     * 分区裁剪的作用：
     * - 减少扫描的分区数量，提高查询性能
     * - 根据WHERE条件过滤掉不相关的分区
     * - 支持多种分区类型（RANGE、LIST、HASH等）
     * - 优化查询执行计划
     * 
     * @param partitionSlots 分区列槽位列表，用于确定分区列
     * @param partitionPredicate 分区谓词表达式，用于过滤分区
     * @param idToPartitions 分区ID到分区项的映射，包含所有分区信息
     * @param cascadesContext 级联上下文，提供优化上下文信息
     * @param partitionTableType 分区表类型，确定分区裁剪策略
     * @param sortedPartitionRanges 排序后的分区范围，用于优化裁剪算法
     * @return 裁剪后需要扫描的分区ID列表
     */
    public static <K extends Comparable<K>> Pair<List<K>, Optional<Expression>> prune(List<Slot> partitionSlots,
            Expression partitionPredicate,
            Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType, Optional<SortedPartitionRanges<K>> sortedPartitionRanges) {
        // 记录开始时间：用于性能监控和统计
        long startAt = System.currentTimeMillis();
        
        try {
            // 调用内部裁剪方法：执行实际的分区裁剪逻辑
            return pruneInternal(partitionSlots, partitionPredicate, idToPartitions, cascadesContext,
                    partitionTableType,
                    sortedPartitionRanges);
        } finally {
            // 性能统计：无论是否发生异常，都要记录执行时间
            SummaryProfile profile = SummaryProfile.getSummaryProfile(cascadesContext.getConnectContext());
            if (profile != null) {
                // 添加Nereids分区裁剪时间到性能统计中
                profile.addNereidsPartitiionPruneTime(System.currentTimeMillis() - startAt);
            }
        }
    }

    private static <K extends Comparable<K>> Pair<List<K>, Optional<Expression>> pruneInternal(
            List<Slot> partitionSlots,
            Expression partitionPredicate,
            Map<K, PartitionItem> idToPartitions, CascadesContext cascadesContext,
            PartitionTableType partitionTableType, Optional<SortedPartitionRanges<K>> sortedPartitionRanges) {
        
        // 1. 谓词提取：从原始谓词中提取分区相关的谓词
        // 示例：原始查询 "SELECT * FROM table WHERE date_col >= '2023-01-01' AND date_col < '2023-02-01' AND name = 'Alice'"
        // 提取结果：只保留分区列相关谓词 "date_col >= '2023-01-01' AND date_col < '2023-02-01'"
        partitionPredicate = PartitionPruneExpressionExtractor.extract(
                partitionPredicate, ImmutableSet.copyOf(partitionSlots), cascadesContext);
        Expression originalPartitionPredicate = partitionPredicate;
        partitionPredicate = PredicateRewriteForPartitionPrune.rewrite(partitionPredicate, cascadesContext);
        int expandThreshold = cascadesContext.getAndCacheSessionVariable(
                "partitionPruningExpandThreshold",
                10, sessionVariable -> sessionVariable.partitionPruningExpandThreshold);

        // 4. OR到IN转换：将OR条件转换为IN条件以优化裁剪
        // 示例：将 "date_col = '2023-01-01' OR date_col = '2023-01-02'" 转换为 "date_col IN ('2023-01-01', '2023-01-02')"
        partitionPredicate = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(
                partitionPredicate, new ExpressionRewriteContext(cascadesContext));
        
        // 5. 常量谓词处理：处理常量谓词的特殊情况
        if (BooleanLiteral.TRUE.equals(partitionPredicate)) {
            // The partition column predicate is always true and can be deleted, the partition cannot be pruned
            return Pair.of(Utils.fastToImmutableList(idToPartitions.keySet()), Optional.of(originalPartitionPredicate));
        } else if (BooleanLiteral.FALSE.equals(partitionPredicate) || partitionPredicate.isNullLiteral()) {
            // The partition column predicate is always false, and all partitions can be pruned.
            return Pair.of(ImmutableList.of(), Optional.empty());
        }

        // 6. 二分查找策略：当有排序分区范围时，使用二分查找提高效率
        if (sortedPartitionRanges.isPresent()) {
            // 将谓词转换为范围集合：用于二分查找
            // 示例：将 "date_col >= '2023-01-01' AND date_col < '2023-02-01'" 转换为 RangeSet([2023-01-01, 2023-02-01))
            RangeSet<MultiColumnBound> predicateRanges = partitionPredicate.accept(
                    new PartitionPredicateToRange(partitionSlots), null);
            
            if (predicateRanges != null) {
                Pair<List<K>, Boolean> res = binarySearchFiltering(
                        sortedPartitionRanges.get(), partitionSlots, partitionPredicate, cascadesContext,
                        expandThreshold, predicateRanges
                );
                if (res.second) {
                    return Pair.of(res.first, Optional.of(originalPartitionPredicate));
                } else {
                    return Pair.of(res.first, Optional.empty());
                }
            }
        }

        Pair<List<K>, Boolean> res = sequentialFiltering(
                idToPartitions, partitionSlots, partitionPredicate, cascadesContext, expandThreshold
        );
        if (res.second) {
            return Pair.of(res.first, Optional.of(originalPartitionPredicate));
        } else {
            return Pair.of(res.first, Optional.empty());
        }
    }

    /**
     * convert partition item to partition evaluator
     */
    public static <K> OnePartitionEvaluator<K> toPartitionEvaluator(K id, PartitionItem partitionItem,
            List<Slot> partitionSlots, CascadesContext cascadesContext, int expandThreshold) {
        if (partitionItem instanceof ListPartitionItem) {
            return new OneListPartitionEvaluator<>(
                    id, partitionSlots, (ListPartitionItem) partitionItem, cascadesContext);
        } else if (partitionItem instanceof RangePartitionItem) {
            return new OneRangePartitionEvaluator<>(
                    id, partitionSlots, (RangePartitionItem) partitionItem, cascadesContext, expandThreshold);
        } else {
            return new UnknownPartitionEvaluator<>(id, partitionItem);
        }
    }

    private static <K extends Comparable<K>> Pair<List<K>, Boolean> binarySearchFiltering(
            SortedPartitionRanges<K> sortedPartitionRanges, List<Slot> partitionSlots,
            Expression partitionPredicate, CascadesContext cascadesContext, int expandThreshold,
            RangeSet<MultiColumnBound> predicateRanges) {
        List<PartitionItemAndRange<K>> sortedPartitions = sortedPartitionRanges.sortedPartitions;

        Set<K> selectedIdSets = Sets.newTreeSet();
        int leftIndex = 0;
        boolean canPredicatePruned = true;
        for (Range<MultiColumnBound> predicateRange : predicateRanges.asRanges()) {
            int rightIndex = sortedPartitions.size();
            if (leftIndex >= rightIndex) {
                break;
            }

            int midIndex;
            MultiColumnBound predicateUpperBound = predicateRange.upperEndpoint();
            MultiColumnBound predicateLowerBound = predicateRange.lowerEndpoint();

            while (leftIndex + 1 < rightIndex) {
                midIndex = (leftIndex + rightIndex) / 2;
                PartitionItemAndRange<K> partition = sortedPartitions.get(midIndex);
                Range<MultiColumnBound> partitionSpan = partition.range;

                if (predicateUpperBound.compareTo(partitionSpan.lowerEndpoint()) < 0) {
                    rightIndex = midIndex;
                } else if (predicateLowerBound.compareTo(partitionSpan.upperEndpoint()) > 0) {
                    leftIndex = midIndex;
                } else {
                    break;
                }
            }

            for (; leftIndex < sortedPartitions.size(); leftIndex++) {
                PartitionItemAndRange<K> partition = sortedPartitions.get(leftIndex);

                K partitionId = partition.id;
                // list partition will expand to multiple PartitionItemAndRange, we should skip evaluate it again
                if (selectedIdSets.contains(partitionId)) {
                    continue;
                }

                Range<MultiColumnBound> partitionSpan = partition.range;
                if (predicateUpperBound.compareTo(partitionSpan.lowerEndpoint()) < 0) {
                    break;
                }

                OnePartitionEvaluator<K> partitionEvaluator = toPartitionEvaluator(
                        partitionId, partition.partitionItem, partitionSlots, cascadesContext, expandThreshold);
                Pair<Boolean, Boolean> res = canBePrunedOut(partitionPredicate, partitionEvaluator);
                if (!res.first) {
                    selectedIdSets.add(partitionId);
                    canPredicatePruned = canPredicatePruned && res.second;
                }
            }
        }

        for (PartitionItemAndId<K> defaultPartition : sortedPartitionRanges.defaultPartitions) {
            K partitionId = defaultPartition.id;
            OnePartitionEvaluator<K> partitionEvaluator = toPartitionEvaluator(
                    partitionId, defaultPartition.partitionItem, partitionSlots, cascadesContext, expandThreshold);
            Pair<Boolean, Boolean> res = canBePrunedOut(partitionPredicate, partitionEvaluator);
            if (!res.first) {
                selectedIdSets.add(partitionId);
                canPredicatePruned = canPredicatePruned && res.second;
            }
        }

        return Pair.of(Utils.fastToImmutableList(selectedIdSets), canPredicatePruned);
    }

    private static <K extends Comparable<K>> Pair<List<K>, Boolean> sequentialFiltering(
            Map<K, PartitionItem> idToPartitions, List<Slot> partitionSlots,
            Expression partitionPredicate, CascadesContext cascadesContext, int expandThreshold) {
        
        // 1. 创建分区评估器列表：为每个分区创建评估器，用于判断分区是否匹配谓词
        // 示例：假设有3个分区 p1, p2, p3，创建3个评估器 evaluator1, evaluator2, evaluator3
        List<OnePartitionEvaluator<?>> evaluators = Lists.newArrayListWithCapacity(idToPartitions.size());
        
        // 2. 遍历所有分区：为每个分区创建对应的评估器
        for (Entry<K, PartitionItem> kv : idToPartitions.entrySet()) {
            // 为每个分区创建评估器：评估器包含分区信息和谓词匹配逻辑
            // 示例：p1 -> evaluator1, p2 -> evaluator2, p3 -> evaluator3
            evaluators.add(toPartitionEvaluator(
                    kv.getKey(), kv.getValue(), partitionSlots, cascadesContext, expandThreshold));
        }
        
        // 3. 构建分区裁剪器：使用评估器列表和谓词构建裁剪器
        // 示例：PartitionPruner([evaluator1, evaluator2, evaluator3], "date_col >= '2023-01-01'")
        PartitionPruner partitionPruner = new PartitionPruner(evaluators, partitionPredicate);
        
        // 4. 执行分区裁剪：调用裁剪器进行实际的分区裁剪
        // 示例：检查每个分区是否匹配谓词，返回匹配的分区列表
        //TODO: we keep default partition because it's too hard to prune it, we return false in canPrune().
        return partitionPruner.prune();
    }

    /**
     * return Pair
     *     pair.first is true if partition can be pruned
     *     pair.second is true if partitionPredicate is always true in this partition
     */
    private static <K> Pair<Boolean, Boolean> canBePrunedOut(Expression partitionPredicate,
            OnePartitionEvaluator<K> evaluator) {
        List<Map<Slot, PartitionSlotInput>> onePartitionInputs = evaluator.getOnePartitionInputs();
        if (evaluator instanceof OneListPartitionEvaluator) {
            // if a table has default partition, the predicate should not be pruned,
            // because evaluateWithDefaultPartition always return true in default partition
            // e.g. PARTITION BY LIST(k1) (
            //     PARTITION p1 VALUES IN ("1","2","3","4"),
            //     PARTITION p2 VALUES IN ("5","6","7","8"),
            //     PARTITION p3 )  p3 is default partition
            boolean notDefaultPartition = !evaluator.isDefaultPartition();
            Pair<Boolean, Boolean> res = Pair.of(notDefaultPartition, notDefaultPartition);
            for (Map<Slot, PartitionSlotInput> currentInputs : onePartitionInputs) {
                // evaluate whether there's possible for this partition to accept this predicate
                Expression result = evaluator.evaluateWithDefaultPartition(partitionPredicate, currentInputs);
                if (result.equals(BooleanLiteral.FALSE) || (result instanceof NullLiteral)) {
                    // Indicates that there is a partition value that does not satisfy the predicate
                    res.second = false;
                } else if (result.equals(BooleanLiteral.TRUE)) {
                    // Indicates that there is a partition value that satisfies the predicate
                    res.first = false;
                } else {
                    // Indicates that this partition value may or may not satisfy the predicate
                    res.second = false;
                    res.first = false;
                }
                if (!res.first && !res.second) {
                    break;
                }
            }
            return res;
        } else {
            // only prune partition predicates in list partition, therefore set pair.second always be false,
            // meaning not to prune partition predicates in range partition
            for (Map<Slot, PartitionSlotInput> currentInputs : onePartitionInputs) {
                Expression result = evaluator.evaluateWithDefaultPartition(partitionPredicate, currentInputs);
                if (!result.equals(BooleanLiteral.FALSE) && !(result instanceof NullLiteral)) {
                    return Pair.of(false, false);
                }
            }
            // only have false result: Can be pruned out. have other exprs: CanNot be pruned out
            return Pair.of(true, false);
        }
    }

    /** remove predicates that are always true*/
    public static Plan prunePredicate(boolean skipPrunePredicate, Optional<Expression> prunedPredicates,
            LogicalFilter<? extends Plan> filter, LogicalRelation scan) {
        if (!skipPrunePredicate && prunedPredicates.isPresent()) {
            Set<Expression> conjuncts = new LinkedHashSet<>(filter.getConjuncts());
            Expression deletedPredicate = prunedPredicates.get();
            Set<Expression> deletedPredicateSet = ExpressionUtils.extractConjunctionToSet(deletedPredicate);
            conjuncts.removeAll(deletedPredicateSet);
            if (conjuncts.isEmpty()) {
                return scan;
            } else {
                return filter.withConjunctsAndChild(conjuncts, scan);
            }
        } else {
            return filter.withChildren(ImmutableList.of(scan));
        }
    }
}
