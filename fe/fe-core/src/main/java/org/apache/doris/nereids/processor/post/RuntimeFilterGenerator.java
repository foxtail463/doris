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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostProcessor {

    public static final ImmutableSet<JoinType> DENIED_JOIN_TYPES = ImmutableSet.of(
            JoinType.LEFT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN
    );

    private static final Set<Class<? extends PhysicalPlan>> SPJ_PLAN = ImmutableSet.of(
            PhysicalRelation.class,
            PhysicalProject.class,
            PhysicalFilter.class,
            PhysicalDistribute.class,
            PhysicalHashJoin.class
    );

    public RuntimeFilterGenerator() {
    }

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        if (!plan.containsType(Join.class)) {
            return plan;
        }

        // 先正常生成并下推 RF
        Plan result = plan.accept(this, ctx);
        // 尝试将 RF 继续下推到 CTE Producer 内部
        // 收集所有 CTEProducer
        RuntimeFilterContext rfCtx = ctx.getRuntimeFilterContext();
        Map<CTEId, PhysicalCTEProducer> cteProducerMap = plan.collect(PhysicalCTEProducer.class::isInstance)
                .stream().collect(Collectors.toMap(p -> ((PhysicalCTEProducer) p).getCteId(),
                        p -> (PhysicalCTEProducer) p));
        // 收集含有 RF 目标的 CTEConsumer 以及其关联的 RF/源表达式
        Map<CTEId, Set<PhysicalCTEConsumer>> cteIdToConsumersWithRF = Maps.newHashMap();
        Map<PhysicalCTEConsumer, Set<RuntimeFilter>> consumerToRFs = Maps.newHashMap();
        Map<PhysicalCTEConsumer, Set<Expression>> consumerToSrcExpression = Maps.newHashMap();
        List<RuntimeFilter> allRFs = rfCtx.getNereidsRuntimeFilter();
        for (RuntimeFilter rf : allRFs) {
            for (PhysicalRelation rel : rf.getTargetScans()) {
                if (rel instanceof PhysicalCTEConsumer) {
                    PhysicalCTEConsumer consumer = (PhysicalCTEConsumer) rel;
                    CTEId cteId = consumer.getCteId();
                    cteIdToConsumersWithRF.computeIfAbsent(cteId, key -> Sets.newHashSet()).add(consumer);
                    consumerToRFs.computeIfAbsent(consumer, key -> Sets.newHashSet()).add(rf);
                    consumerToSrcExpression.computeIfAbsent(consumer, key -> Sets.newHashSet())
                            .add(rf.getSrcExpr());
                }
            }
        }
        for (CTEId cteId : cteIdToConsumersWithRF.keySet()) {
            // if any consumer does not have RF, RF cannot be pushed down.
            // cteIdToConsumersWithRF.get(cteId).size() can not be 1, o.w. this cte will be inlined.
            // 只有所有消费者都拥有 RF 且消费者数量≥2 时，才有必要向 Producer 下推
            if (ctx.getCteIdToConsumers().get(cteId).size() == cteIdToConsumersWithRF.get(cteId).size()
                        && cteIdToConsumersWithRF.get(cteId).size() >= 2) {
                // check if there is a common srcExpr among all the consumers
                Set<PhysicalCTEConsumer> consumers = cteIdToConsumersWithRF.get(cteId);
                PhysicalCTEConsumer consumer0 = consumers.iterator().next();
                Set<Expression> candidateSrcExpressions = consumerToSrcExpression.get(consumer0);
                for (PhysicalCTEConsumer currentConsumer : consumers) {
                    Set<Expression> srcExpressionsOnCurrentConsumer = consumerToSrcExpression.get(currentConsumer);
                    candidateSrcExpressions.retainAll(srcExpressionsOnCurrentConsumer);
                    if (candidateSrcExpressions.isEmpty()) {
                        break;
                    }
                }
                if (!candidateSrcExpressions.isEmpty()) {
                    // find RFs to push down
                    for (Expression srcExpr : candidateSrcExpressions) {
                        List<RuntimeFilter> rfsToPushDown = Lists.newArrayList();
                        for (PhysicalCTEConsumer consumer : cteIdToConsumersWithRF.get(cteId)) {
                            for (RuntimeFilter rf : consumerToRFs.get(consumer)) {
                                if (rf.getSrcExpr().equals(srcExpr)) {
                                    rfsToPushDown.add(rf);
                                }
                            }
                        }
                        if (rfsToPushDown.isEmpty()) {
                            break;
                        }

                        // the most right deep buildNode from rfsToPushDown is used as buildNode for pushDown rf
                        // since the srcExpr are the same, all buildNodes of rfToPushDown are in the same tree path
                        // the longest ancestors means its corresponding rf build node is the most right deep one.
                        // 找到最“右深”的构建端 RF（相同源表达式意味着在同一路径上）
                        List<RuntimeFilter> rightDeepRfs = Lists.newArrayList();
                        List<Plan> rightDeepAncestors = rfsToPushDown.get(0).getBuilderNode().getAncestors();
                        int rightDeepAncestorsSize = rightDeepAncestors.size();
                        RuntimeFilter leftTop = rfsToPushDown.get(0);
                        int leftTopAncestorsSize = rightDeepAncestorsSize;
                        for (RuntimeFilter rf : rfsToPushDown) {
                            List<Plan> ancestors = rf.getBuilderNode().getAncestors();
                            int currentAncestorsSize = ancestors.size();
                            if (currentAncestorsSize >= rightDeepAncestorsSize) {
                                if (currentAncestorsSize == rightDeepAncestorsSize) {
                                    rightDeepRfs.add(rf);
                                } else {
                                    rightDeepAncestorsSize = currentAncestorsSize;
                                    rightDeepAncestors = ancestors;
                                    rightDeepRfs.clear();
                                    rightDeepRfs.add(rf);
                                }
                            }
                            if (currentAncestorsSize < leftTopAncestorsSize) {
                                leftTopAncestorsSize = currentAncestorsSize;
                                leftTop = rf;
                            }
                        }
                        Preconditions.checkArgument(rightDeepAncestors.contains(leftTop.getBuilderNode()));
                        // check nodes between right deep and left top are SPJ and not denied join and not mark join
                        // 检查右深构建端到左上构建端路径上的节点是否合法（仅 SPJ 且不包含被禁用的 Join）
                        boolean valid = true;
                        for (Plan cursor : rightDeepAncestors) {
                            if (cursor.equals(leftTop.getBuilderNode())) {
                                break;
                            }
                            // valid = valid && SPJ_PLAN.contains(cursor.getClass());
                            if (cursor instanceof AbstractPhysicalJoin) {
                                AbstractPhysicalJoin cursorJoin = (AbstractPhysicalJoin) cursor;
                                valid = (!RuntimeFilterGenerator.DENIED_JOIN_TYPES
                                        .contains(cursorJoin.getJoinType())
                                        || cursorJoin.isMarkJoin()) && valid;
                            }
                            if (!valid) {
                                break;
                            }
                        }

                        if (!valid) {
                            break;
                        }

                        for (RuntimeFilter rfToPush : rightDeepRfs) {
                            Expression rightDeepTargetExpressionOnCTE = null;
                            int targetCount = rfToPush.getTargetExpressions().size();
                            // 找到该 RF 在 CTEConsumer 上对应的目标表达式
                            for (int i = 0; i < targetCount; i++) {
                                PhysicalRelation rel = rfToPush.getTargetScans().get(i);
                                if (rel instanceof PhysicalCTEConsumer
                                        && ((PhysicalCTEConsumer) rel).getCteId().equals(cteId)) {
                                    rightDeepTargetExpressionOnCTE = rfToPush.getTargetExpressions().get(i);
                                    break;
                                }
                            }

                            boolean pushedDown = doPushDownIntoCTEProducerInternal(
                                    rfToPush,
                                    rightDeepTargetExpressionOnCTE,
                                    rfCtx,
                                    cteProducerMap.get(cteId)
                            );
                            if (pushedDown) {
                                // 已下推到 Producer，移除 Consumer 侧原有 RF
                                rfCtx.removeFilter(
                                        rfToPush,
                                        rightDeepTargetExpressionOnCTE.getInputSlotExprIds().iterator().next());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * 访问 PhysicalHashJoin 节点，为其生成运行时过滤器
     * 
     * 运行时过滤器生成器在 Nereids 优化器的后处理和计划翻译阶段运行。
     * 
     * 后处理阶段：
     * 第一步：如果遇到支持的 join 类型，为所有 hash join 条件生成 Nereids 运行时过滤器，
     * 并建立从目标 slot 引用的 exprId 到运行时过滤器的关联。或者删除那些目标 slot 引用
     * 是物理 join 左子节点输出 slot 引用的运行时过滤器。
     * 第二步：如果遇到 project 节点，收集其子节点和自身的关联，以便通过 project 节点下推。
     * 
     * 计划翻译阶段：
     * 第三步：在扫描节点 fragment 生成 Nereids 运行时过滤器目标。
     * 第四步：在 hash join 节点 fragment 生成传统运行时过滤器目标和运行时过滤器。
     * 
     * 注意：自底向上遍历计划树！！！
     * 
     * TODO: 当前支持 inner join、cross join、right outer join，未来将支持更多 join 类型。
     */
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        // 自底向上遍历：先访问右子节点（build side），再访问左子节点（probe side）
        join.right().accept(this, context);
        join.left().accept(this, context);
        
        // 检查是否应该为这个 join 生成运行时过滤器
        // 如果 join 类型在拒绝列表中，或者是 mark join，则不生成 RF
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            return join;
        }
        
        // 获取运行时过滤器上下文
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        // 获取会话变量允许的运行时过滤器类型列表
        List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
                .filter(type -> ctx.getSessionVariable().allowedRuntimeFilterType(type))
                .collect(Collectors.toList());

        // 获取 hash join 的等值连接条件列表
        List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts();

        // 遍历每个 hash join 条件，为每个条件生成运行时过滤器
        for (int i = 0; i < hashJoinConjuncts.size(); i++) {
            // 调整等值条件的左右顺序，确保左边是 probe side（左子节点），右边是 build side（右子节点）
            EqualPredicate equalTo = JoinUtils.swapEqualToForChildrenOrder(
                    (EqualPredicate) hashJoinConjuncts.get(i), join.left().getOutputSet());
            
            // 如果等值条件的两边都是唯一值，则不需要生成运行时过滤器
            // 例如：T1 join T2 on T1.a=T2.a where T1.a=1 and T2.a=1
            // 因为两边都是常量，运行时过滤器没有意义
            if (isUniqueValueEqualTo(join, equalTo)) {
                continue;
            }
            
            // 为每个允许的运行时过滤器类型生成过滤器
            for (TRuntimeFilterType type : legalTypes) {
                // BITMAP 类型的运行时过滤器由嵌套循环 join 生成，这里跳过
                if (type == TRuntimeFilterType.BITMAP) {
                    continue;
                }
                
                // 获取 build side（右子节点）的 NDV（不同值数量），用于评估过滤器效果
                long buildSideNdv = getBuildSideNdv(join, equalTo);
                
                // 只有当等值条件左边（probe side）只包含一个 slot 时才生成运行时过滤器
                // 这是因为运行时过滤器只能应用到单个列上
                if (equalTo.left().getInputSlots().size() == 1) {
                    // 创建下推上下文，包含生成运行时过滤器所需的所有信息
                    RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                            RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContextForHashJoin(
                                    equalTo.right(),  // build side 表达式（源）
                                    equalTo.left(),   // probe side 表达式（目标）
                                    ctx,              // 运行时过滤器上下文
                                    ctx.getRuntimeFilterIdGen(), // 运行时过滤器 ID 生成器
                                    type,             // 过滤器类型
                                    join,             // join 节点
                                    context.getStatementContext().isHasUnknownColStats(), // 是否有未知列统计信息
                                    buildSideNdv,     // build side 的 NDV
                                    i);               // 连接条件的索引
                    
                    // 检查下推上下文是否有效
                    // 如果目标是聚合结果，则下推上下文无效
                    // 目前我们只在 PhysicalScan 上应用运行时过滤器，所以跳过这种情况
                    // 例如：(select sum(x) as s from A) T join B on T.s=B.s
                    // 在这种情况下，T.s 是聚合结果，不能应用运行时过滤器
                    if (pushDownContext.isValid()) {
                        // 使用 RuntimeFilterPushDownVisitor 将运行时过滤器下推到扫描节点
                        join.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
                    }
                }
            }
        }
        return join;
    }

    /**
     * 检查等值条件的两边是否都是唯一值（uniform value）且相等
     * 
     * 如果等值条件的两边都是唯一值且相等，则不需要生成运行时过滤器，因为：
     * 1. 唯一值意味着该列在整个表中只有一个值
     * 2. 如果两边的唯一值相等，连接条件总是为真，运行时过滤器没有过滤效果
     * 
     * 示例1（不需要生成 RF）：
     * T1 join T2 on T1.a=T2.a where T1.a=1 and T2.a=1
     * 如果 T1.a 和 T2.a 都是唯一值且相等（都是1），则不需要为 "T1.a=T2.a" 生成运行时过滤器
     * 
     * 示例2（需要生成 RF）：
     * T1 join T2 on T1.a=T2.a where T1.a in (1, 2) and T2.a in (1, 2)
     * 在上面的情况下，由于常量传播的限制，T1.a 和 T2.a 可能不是唯一值，
     * 所以仍然会生成运行时过滤器 RF: T2.a->T1.a
     * 
     * @param join hash join 节点
     * @param equalTo 等值条件表达式
     * @return 如果两边都是唯一值且相等，返回 true，表示不需要生成运行时过滤器；否则返回 false
     */
    private boolean isUniqueValueEqualTo(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
                                         EqualPredicate equalTo) {
        // 只有当等值条件的两边都是 Slot 类型时，才能检查唯一值
        if (equalTo.left() instanceof Slot && equalTo.right() instanceof Slot) {
            // 获取左子节点（probe side）对应 slot 的唯一值
            // uniform value 表示该列在整个表中只有一个值（例如：常量、唯一键等）
            Optional<Expression> leftValue = join.left().getLogicalProperties()
                    .getTrait().getUniformValue((Slot) equalTo.left());
            // 获取右子节点（build side）对应 slot 的唯一值
            Optional<Expression> rightValue = join.right().getLogicalProperties()
                    .getTrait().getUniformValue((Slot) equalTo.right());
            
            // 检查是否成功获取到两边的唯一值
            if (leftValue != null && rightValue != null) {
                // 如果两边都有唯一值（Optional.isPresent() 为 true）
                if (leftValue.isPresent() && rightValue.isPresent()) {
                    // 如果两边的唯一值相等，则不需要生成运行时过滤器
                    if (leftValue.get().equals(rightValue.get())) {
                        return true;
                    }
                }
            }
        }
        // 如果两边不是 Slot，或者没有唯一值，或者唯一值不相等，则需要生成运行时过滤器
        return false;
    }

    @Override
    public PhysicalCTEConsumer visitPhysicalCTEConsumer(PhysicalCTEConsumer scan, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.aliasTransferMapPut(slot, Pair.of(scan, slot)));
        return scan;
    }

    private void generateBitMapRuntimeFilterForNLJ(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
                                                   RuntimeFilterContext ctx) {
        if (join.getJoinType() != JoinType.LEFT_SEMI_JOIN && join.getJoinType() != JoinType.CROSS_JOIN) {
            return;
        }
        List<Slot> leftSlots = join.left().getOutput();
        List<Slot> rightSlots = join.right().getOutput();
        List<Expression> bitmapRuntimeFilterConditions = JoinUtils.extractBitmapRuntimeFilterConditions(leftSlots,
                rightSlots, join.getOtherJoinConjuncts());
        if (!JoinUtils.extractExpressionForHashTable(leftSlots, rightSlots, join.getOtherJoinConjuncts())
                .first.isEmpty()) {
            return;
        }
        int bitmapRFCount = bitmapRuntimeFilterConditions.size();
        for (int i = 0; i < bitmapRFCount; i++) {
            Expression bitmapRuntimeFilterCondition = bitmapRuntimeFilterConditions.get(i);
            boolean isNot = bitmapRuntimeFilterCondition instanceof Not;
            BitmapContains bitmapContains;
            if (bitmapRuntimeFilterCondition instanceof Not) {
                bitmapContains = (BitmapContains) bitmapRuntimeFilterCondition.child(0);
            } else {
                bitmapContains = (BitmapContains) bitmapRuntimeFilterCondition;
            }
            RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                    RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContextForBitMapFilter(
                            bitmapContains.child(0), bitmapContains.child(1),
                            ctx, ctx.getRuntimeFilterIdGen(), join,
                            -1, i, isNot);
            if (pushDownContext.isValid()) {
                join.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
            }
        }
    }

    /**
     * A join B on B.x < A.x
     * transform B.x < A.x to A.x > B.x,
     * otherwise return null
     */
    private ComparisonPredicate normalizeNonEqual(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
                                                  Expression expr) {
        if (!(expr instanceof ComparisonPredicate)) {
            return null;
        }
        if (!(expr instanceof LessThan) && !(expr instanceof LessThanEqual)
                && !(expr instanceof GreaterThanEqual) && !(expr instanceof GreaterThan)) {
            return null;
        }
        if (!(expr.child(0) instanceof SlotReference)) {
            return null;
        }
        if (!(expr.child(1) instanceof SlotReference)) {
            return null;
        }
        if (! join.left().getOutput().contains(expr.child(0))
                || ! join.right().getOutput().contains(expr.child(1))) {
            if (join.left().getOutput().contains(expr.child(1))
                    && join.right().getOutput().contains(expr.child(0))) {
                return ((ComparisonPredicate) expr).commute();
            }
        } else {
            return (ComparisonPredicate) expr;
        }
        return null;
    }

    private TMinMaxRuntimeFilterType getMinMaxType(ComparisonPredicate compare) {
        if (compare instanceof LessThan || compare instanceof LessThanEqual) {
            return TMinMaxRuntimeFilterType.MAX;
        }
        if (compare instanceof GreaterThan || compare instanceof GreaterThanEqual) {
            return TMinMaxRuntimeFilterType.MIN;
        }
        return TMinMaxRuntimeFilterType.MIN_MAX;
    }

    /**
     * A join B on A.x < B.y
     * min-max filter (A.x < N, N=max(B.y)) could be applied to A.x
     */
    private void generateMinMaxRuntimeFilter(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
                                                   RuntimeFilterContext ctx) {
        int hashCondionSize = join.getHashJoinConjuncts().size();
        for (int idx = 0; idx < join.getOtherJoinConjuncts().size(); idx++) {
            int exprOrder = idx + hashCondionSize;
            Expression expr = join.getOtherJoinConjuncts().get(exprOrder);
            ComparisonPredicate compare = normalizeNonEqual(join, expr);
            if (compare != null) {
                Slot unwrappedSlot = checkTargetChild(compare.child(0));
                if (unwrappedSlot == null) {
                    continue;
                }
                Pair<PhysicalRelation, Slot> pair = ctx.getAliasTransferPair(unwrappedSlot);
                if (pair == null) {
                    continue;
                }
                Slot olapScanSlot = pair.second;
                PhysicalRelation scan = pair.first;
                Preconditions.checkState(olapScanSlot != null && scan != null);
                RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                        RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContextForNljMinMaxFilter(
                                compare.child(1), compare.child(0),
                                ctx, ctx.getRuntimeFilterIdGen(), join,
                                exprOrder, getMinMaxType(compare));
                if (pushDownContext.isValid()) {
                    join.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
                }
            }
        }
    }

    @Override
    public PhysicalPlan visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        // TODO: we need to support all type join
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            // do not generate RF on this join
            return join;
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();

        if (ctx.getSessionVariable().allowedRuntimeFilterType(TRuntimeFilterType.BITMAP)) {
            generateBitMapRuntimeFilterForNLJ(join, ctx);
        }

        if (ctx.getSessionVariable().allowedRuntimeFilterType(TRuntimeFilterType.MIN_MAX)) {
            generateMinMaxRuntimeFilter(join, ctx);
        }

        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        // change key when encounter alias.
        // TODO: same action will be taken for set operation
        for (Expression expression : project.getProjects()) {
            if (expression.children().isEmpty()) {
                continue;
            }
            Expression expr = ExpressionUtils.getSingleNumericSlotOrExpressionCoveredByCast(expression.child(0));
            if (expr instanceof NamedExpression
                    && ctx.aliasTransferMapContains((NamedExpression) expr)) {
                if (expression instanceof Alias) {
                    Alias alias = ((Alias) expression);
                    ctx.aliasTransferMapPut(alias.toSlot(), ctx.getAliasTransferPair((NamedExpression) expr));
                }
            }
        }
        return project;
    }

    @Override
    public Plan visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, CascadesContext context) {
        // TODO: OneRowRelation will be translated to union. Union node cannot apply runtime filter now
        //  so, just return itself now, until runtime filter could apply on any node.
        return oneRowRelation;
    }

    @Override
    public PhysicalRelation visitPhysicalRelation(PhysicalRelation relation, CascadesContext context) {
        if (relation instanceof PhysicalSchemaScan) {
            return relation;
        }
        // add all the slots in map.
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        relation.getOutput().forEach(slot -> ctx.aliasTransferMapPut(slot, Pair.of(relation, slot)));
        return relation;
    }

    @Override
    public Plan visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan scan, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.aliasTransferMapPut(slot, Pair.of(scan, slot)));
        return scan;
    }

    @Override
    public PhysicalSetOperation visitPhysicalSetOperation(PhysicalSetOperation setOperation, CascadesContext context) {
        setOperation.children().forEach(child -> child.accept(this, context));
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        if (!setOperation.getRegularChildrenOutputs().isEmpty()) {
            // example: RegularChildrenOutputs is empty
            // "select 1 a, 2 b union all select 3, 4 union all select 10 e, 20 f;"
            for (int i = 0; i < setOperation.getOutput().size(); i++) {
                Pair childSlotPair = ctx.getAliasTransferPair(setOperation.getRegularChildOutput(0).get(i));
                if (childSlotPair != null) {
                    ctx.aliasTransferMapPut(setOperation.getOutput().get(i), childSlotPair);
                }
            }
        }
        return setOperation;
    }

    // runtime filter build side ndv
    private long getBuildSideNdv(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
                                 ComparisonPredicate compare) {
        AbstractPlan right = (AbstractPlan) join.right();
        //make ut test friendly
        if (right.getStats() == null) {
            return -1L;
        }
        ExpressionEstimation estimator = new ExpressionEstimation();
        ColumnStatistic buildColStats = compare.right().accept(estimator, right.getStats());
        return buildColStats.isUnKnown
                ? Math.max(1, (long) right.getStats().getRowCount()) : Math.max(1, (long) buildColStats.ndv);
    }

    public static Slot checkTargetChild(Expression leftChild) {
        Expression expression = ExpressionUtils.getSingleNumericSlotOrExpressionCoveredByCast(leftChild);
        return expression instanceof Slot ? ((Slot) expression) : null;
    }

    private boolean doPushDownIntoCTEProducerInternal(RuntimeFilter rf, Expression targetExpression,
                                                   RuntimeFilterContext ctx, PhysicalCTEProducer cteProducer) {
        PhysicalPlan inputPlanNode = (PhysicalPlan) cteProducer.child(0);
        Slot unwrappedSlot = checkTargetChild(targetExpression);
        // aliasTransMap doesn't contain the key, means that the path from the scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!checkProbeSlot(ctx, unwrappedSlot)) {
            return false;
        }
        Slot consumerOutputSlot = ctx.getAliasTransferPair(unwrappedSlot).second;
        PhysicalRelation cteNode = ctx.getAliasTransferPair(unwrappedSlot).first;
        long buildSideNdv = rf.getBuildSideNdv();
        if (!(cteNode instanceof PhysicalCTEConsumer) || !(inputPlanNode instanceof PhysicalProject)) {
            return false;
        }
        Slot cteSlot = ((PhysicalCTEConsumer) cteNode).getProducerSlot(consumerOutputSlot);

        PhysicalProject<Plan> project = (PhysicalProject<Plan>) inputPlanNode;
        NamedExpression targetExpr = null;
        for (NamedExpression ne : project.getProjects()) {
            if (cteSlot.getExprId().equals(ne.getExprId())) {
                targetExpr = ne;
                break;
            }
        }
        Preconditions.checkState(targetExpr != null,
                "cannot find runtime filter cte.target: "
                        + cteSlot + "in project " + project.toString());
        if (targetExpr instanceof SlotReference && checkCanPushDownIntoBasicTable(project)) {
            Map<Slot, PhysicalRelation> pushDownBasicTableInfos = getPushDownBasicTablesInfos(project,
                    (SlotReference) targetExpr, ctx);
            if (!pushDownBasicTableInfos.isEmpty()) {
                List<Slot> targetList = new ArrayList<>();
                List<Expression> targetExpressions = new ArrayList<>();
                List<PhysicalRelation> targetNodes = new ArrayList<>();
                for (Map.Entry<Slot, PhysicalRelation> entry : pushDownBasicTableInfos.entrySet()) {
                    Slot targetSlot = entry.getKey();
                    PhysicalRelation scan = entry.getValue();
                    if (!RuntimeFilterGenerator.checkPushDownPreconditionsForRelation(project, scan)) {
                        continue;
                    }
                    targetList.add(targetSlot);
                    targetExpressions.add(targetSlot);
                    targetNodes.add(scan);
                    ctx.addJoinToTargetMap(rf.getBuilderNode(), targetSlot.getExprId());
                    ctx.setTargetsOnScanNode(scan, targetSlot);
                }
                if (targetList.isEmpty()) {
                    return false;
                }
                RuntimeFilter filter = new RuntimeFilter(ctx.getRuntimeFilterIdGen().getNextId(),
                        rf.getSrcExpr(), targetList, targetExpressions, rf.getType(), rf.getExprOrder(),
                        rf.getBuilderNode(), buildSideNdv, rf.isBloomFilterSizeCalculatedByNdv(),
                        rf.gettMinMaxType(), cteNode);
                targetNodes.forEach(node -> node.addAppliedRuntimeFilter(filter));
                for (Slot slot : targetList) {
                    ctx.setTargetExprIdToFilter(slot.getExprId(), filter);
                }
                ctx.setRuntimeFilterIdentityToFilter(rf.getSrcExpr(), rf.getType(), rf.getBuilderNode(), filter);
                return true;
            }
        }
        return false;
    }

    /**
     * check if slot is in ctx.aliasTransferMap
     */
    public static boolean checkProbeSlot(RuntimeFilterContext ctx, Slot slot) {
        if (slot == null || !ctx.aliasTransferMapContains(slot)) {
            return false;
        }
        return true;
    }

    /**
     * Check runtime filter push down relation related pre-conditions.
     */
    public static boolean checkPushDownPreconditionsForRelation(PhysicalPlan root, PhysicalRelation relation) {
        Preconditions.checkState(relation != null, "relation is null");
        // check if the relation supports runtime filter push down
        if (!relation.canPushDownRuntimeFilter()) {
            return false;
        }
        // check if the plan root can cover the push down candidate relation
        Set<PhysicalRelation> relations = new HashSet<>();
        RuntimeFilterGenerator.getAllScanInfo(root, relations);
        return relations.contains(relation);
    }

    private boolean checkCanPushDownIntoBasicTable(PhysicalPlan root) {
        // only support spj currently
        List<PhysicalPlan> plans = Lists.newArrayList();
        plans.addAll(root.collect(PhysicalPlan.class::isInstance));
        return plans.stream().allMatch(p -> SPJ_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    private Map<Slot, PhysicalRelation> getPushDownBasicTablesInfos(PhysicalPlan root, SlotReference slot,
            RuntimeFilterContext ctx) {
        Map<Slot, PhysicalRelation> basicTableInfos = new HashMap<>();
        Set<PhysicalHashJoin> joins = new HashSet<>();
        ExprId exprId = slot.getExprId();
        if (ctx.getAliasTransferPair(slot) != null) {
            basicTableInfos.put(slot, ctx.getAliasTransferPair(slot).first);
        }
        // try to find propagation condition from join
        getAllJoinInfo(root, joins);
        for (PhysicalHashJoin join : joins) {
            List<Expression> conditions = join.getHashJoinConjuncts();
            for (Expression equalTo : conditions) {
                if (equalTo instanceof EqualTo) {
                    SlotReference leftSlot = (SlotReference) ((EqualTo) equalTo).left();
                    SlotReference rightSlot = (SlotReference) ((EqualTo) equalTo).right();
                    if (leftSlot.getExprId() == exprId && ctx.getAliasTransferPair(rightSlot) != null) {
                        PhysicalRelation rightTable = ctx.getAliasTransferPair(rightSlot).first;
                        if (rightTable != null) {
                            basicTableInfos.put(rightSlot, rightTable);
                        }
                    } else if (rightSlot.getExprId() == exprId && ctx.getAliasTransferPair(leftSlot) != null) {
                        PhysicalRelation leftTable = ctx.getAliasTransferPair(leftSlot).first;
                        if (leftTable != null) {
                            basicTableInfos.put(leftSlot, leftTable);
                        }
                    }
                }
            }
        }
        return basicTableInfos;
    }

    private void getAllJoinInfo(PhysicalPlan root, Set<PhysicalHashJoin> joins) {
        if (root instanceof PhysicalHashJoin) {
            joins.add((PhysicalHashJoin) root);
        } else {
            for (Object child : root.children()) {
                getAllJoinInfo((PhysicalPlan) child, joins);
            }
        }
    }

    /**
     * Get all relation node from current root plan.
     */
    public static void getAllScanInfo(Plan root, Set<PhysicalRelation> scans) {
        if (root instanceof PhysicalRelation) {
            scans.add((PhysicalRelation) root);
            // if (root instanceof PhysicalLazyMaterializeOlapScan) {
            //     scans.add(((PhysicalLazyMaterializeOlapScan) root).getScan());
            // } else {
            //     scans.add((PhysicalRelation) root);
            // }
        } else {
            for (Plan child : root.children()) {
                getAllScanInfo(child, scans);
            }
        }
    }
}
