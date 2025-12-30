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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**SplitAggRule*/
public abstract class SplitAggBaseRule {
    /**
     * 拆分去重聚合的第一阶段（phase1）
     * 
     * 这个方法用于将包含 DISTINCT 的聚合函数拆分为多个阶段。
     * 第一阶段（phase1）负责对 DISTINCT 列和 GROUP BY 列进行去重和预聚合。
     * 
     * 示例：select count(distinct a) group by b
     * 拆分后的计划结构：
     *   agg(group by b, count(a); distinct global) ---phase2（第二阶段）
     *     +--agg(group by a,b; global) ---phase1（第一阶段，本方法生成）
     *       +--hashShuffle(b)
     * 
     * @param aggregate 逻辑聚合节点
     * @param localAggGroupBySet 本地聚合的 GROUP BY 集合（包含 DISTINCT 列和原始 GROUP BY 列）
     * @param inputToBufferParam 输入到缓冲区的聚合参数（用于非 DISTINCT 聚合函数）
     * @param paramForAggFunc 聚合函数的参数（用于非 DISTINCT 聚合函数）
     * @param localAggFunctionToAlias 本地聚合函数到别名的映射（输出参数，用于存储生成的别名）
     * @param child 子计划
     * @param partitionExpressions 分区表达式（用于数据分布）
     * @return 第一阶段（phase1）的物理聚合节点
     */
    protected PhysicalHashAggregate<? extends Plan> splitDeduplicateOnePhase(LogicalAggregate<? extends Plan> aggregate,
            Set<NamedExpression> localAggGroupBySet, AggregateParam inputToBufferParam, AggregateParam paramForAggFunc,
            Map<AggregateFunction, Alias> localAggFunctionToAlias, Plan child, List<Expression> partitionExpressions) {
        // 步骤1: 处理非 DISTINCT 的聚合函数
        // 对于非 DISTINCT 的聚合函数（如 sum、avg 等），在第一阶段也需要进行预聚合
        // 创建 AggregateExpression 并包装为 Alias，存储到 localAggFunctionToAlias 中
        aggregate.getAggregateFunctions().stream()
                .filter(aggFunc -> !aggFunc.isDistinct())  // 只处理非 DISTINCT 的聚合函数
                .collect(Collectors.toMap(
                        expr -> expr,  // key: 原始聚合函数
                        expr -> {
                            // value: 创建本地聚合表达式（使用 paramForAggFunc 参数）
                            AggregateExpression localAggExpr = new AggregateExpression(expr, paramForAggFunc);
                            return new Alias(localAggExpr);
                        },
                        (existing, replacement) -> existing,  // 如果 key 已存在，保留现有的
                        () -> localAggFunctionToAlias  // 使用提供的 map 作为结果容器
                ));
        
        // 步骤2: 构建第一阶段聚合的输出表达式列表
        // 输出包括：GROUP BY 的列 + 非 DISTINCT 聚合函数的别名
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)  // 添加 GROUP BY 列
                .addAll(localAggFunctionToAlias.values())  // 添加非 DISTINCT 聚合函数的别名
                .build();
        
        // 步骤3: 将 GROUP BY 集合转换为表达式列表
        List<Expression> localAggGroupBy = Utils.fastToImmutableList(localAggGroupBySet);
        
        // 步骤4: 处理空聚合的特殊情况
        // 检查是否同时满足：GROUP BY 为空 且 输出表达式为空
        boolean isGroupByEmptySelectEmpty = localAggGroupBy.isEmpty() && localAggOutput.isEmpty();
        // 不推荐生成空的聚合节点（GROUP BY 和输出都为空），因为：
        // 1. 空的聚合节点没有实际意义
        // 2. 可能导致后续处理出现问题（如 computeUniform 中的类型转换错误）
        // 所以添加一个 NullLiteral 作为占位符到 GROUP BY 和输出中
        if (isGroupByEmptySelectEmpty) {
            localAggGroupBy = ImmutableList.of(new NullLiteral(TinyIntType.INSTANCE));
            localAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }
        
        // 步骤5: 创建并返回第一阶段的物理聚合节点
        return new PhysicalHashAggregate<>(localAggGroupBy, localAggOutput, Optional.ofNullable(partitionExpressions),
                inputToBufferParam, AggregateUtils.maybeUsingStreamAgg(localAggGroupBy, inputToBufferParam),
                null, child);
    }

    /**
     * This functions is used to split bottom deduplicate Aggregate(phase1 and phase2):
     * e.g.select count(distinct a) group by b
     *   agg(group by b, count(a); distinct global) ---phase3
     *     +--agg(group by a,b; global) ---phase2
     *       +--hashShuffle(b)
     *         +--agg(group by a,b; local) ---phase1
     * */
    protected PhysicalHashAggregate<? extends Plan> splitDeduplicateTwoPhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> middleAggFunctionToAlias, List<Expression> partitionExpressions,
            Set<NamedExpression> localAggGroupBySet) {
        // first phase
        AggregateParam inputToBufferParam = AggregateParam.LOCAL_BUFFER;
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        PhysicalHashAggregate<? extends Plan> localAgg = splitDeduplicateOnePhase(aggregate, localAggGroupBySet,
                inputToBufferParam, inputToBufferParam, localAggFunctionToAlias, aggregate.child(), ImmutableList.of());

        // second phase
        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        localAggFunctionToAlias.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey,
                        kv -> {
                            AggregateExpression middleAggExpr = new AggregateExpression(kv.getKey(),
                                    bufferToBufferParam, kv.getValue().toSlot());
                            return new Alias(middleAggExpr);
                        },
                        (existing, replacement) -> existing,
                        () -> middleAggFunctionToAlias));
        List<NamedExpression> middleAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(middleAggFunctionToAlias.values())
                .build();
        if (middleAggOutput.isEmpty()) {
            middleAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }
        return new PhysicalHashAggregate<>(localAgg.getGroupByExpressions(), middleAggOutput,
                Optional.ofNullable(partitionExpressions), bufferToResultParam,
                AggregateUtils.maybeUsingStreamAgg(localAgg.getGroupByExpressions(), bufferToResultParam),
                null, localAgg);
    }

    /**
     * This functions is used to split distinct phase Aggregate(phase2 and phase3):
     * e.g. select count(distinct a) group by b
     *   agg(group by b, count(a); distinct global) --phase3
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local) --phase2
     *         +--agg(group by a,b; global) --phase1
     *           +--hashShuffle(a)
     * */
    protected PhysicalHashAggregate<? extends Plan> splitDistinctTwoPhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> middleAggFunctionToAlias, Plan child) {
        AggregateParam thirdParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> aggFuncToAliasThird = new LinkedHashMap<>();
        middleAggFunctionToAlias.entrySet().stream().collect(
                Collectors.toMap(Entry::getKey,
                        entry -> new Alias(new AggregateExpression(entry.getKey(),
                                new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_BUFFER),
                                entry.getValue().toSlot())),
                        (k1, k2) -> k1,
                        () -> aggFuncToAliasThird
                )
        );
        for (AggregateFunction func : aggregate.getAggregateFunctions()) {
            if (!func.isDistinct()) {
                continue;
            }
            if (func instanceof Count && func.arity() > 1) {
                Expression countIf = AggregateUtils.countDistinctMultiExprToCountIf((Count) func);
                aggFuncToAliasThird.put(func, new Alias(new AggregateExpression((Count) countIf, thirdParam)));
            } else {
                aggFuncToAliasThird.put(func, new Alias(new AggregateExpression(
                        func.withDistinctAndChildren(false, func.children()), thirdParam)));
            }
        }
        List<NamedExpression> thirdAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(aggFuncToAliasThird.values())
                .build();
        Plan thirdAgg = new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(), thirdAggOutput, thirdParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), thirdParam), null, child);

        // fourth phase
        AggregateParam fourthParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            Alias alias = aggFuncToAliasThird.get(aggFunc);
                            if (aggFunc instanceof Count && aggFunc.arity() > 1) {
                                return new AggregateExpression(((AggregateExpression) alias.child()).getFunction(),
                                        fourthParam, alias.toSlot());
                            } else {
                                return new AggregateExpression(
                                        aggFunc.withDistinctAndChildren(false, aggFunc.children()),
                                        fourthParam, alias.toSlot());
                            }
                        } else {
                            return new AggregateExpression(aggFunc,
                                    new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                                    aggFuncToAliasThird.get(aggFunc).toSlot());
                        }
                    }
                    return expr;
                }
        );
        return new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(), globalOutput,
                Optional.ofNullable(aggregate.getGroupByExpressions()), fourthParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), fourthParam),
                aggregate.getLogicalProperties(), thirdAgg);
    }
}
