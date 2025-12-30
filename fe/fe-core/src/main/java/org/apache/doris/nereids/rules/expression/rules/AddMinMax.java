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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.rules.expression.rules.AddMinMax.MinMaxValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.CompoundValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.DiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.EmptyValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.IsNotNullValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.IsNullValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.NotDiscreteValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.RangeValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.UnknownValue;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDesc;
import org.apache.doris.nereids.rules.expression.rules.RangeInference.ValueDescVisitor;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ComparableLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 该类实现了为 OR 表达式添加最小最大值范围约束的功能。
 * 
 * 该规则会分析 OR 表达式中各个分支的范围，推断出整个表达式的整体最小最大值范围，
 * 然后将这些范围约束作为额外的 AND 条件添加到原表达式中。
 * 
 * 示例：
 * 
 * a > 10 and a < 20 or a > 30 and a < 40 or a > 50 and a < 60
 *   => (a < 20 or a > 30 and a < 40 or a > 50) and a > 10 and a < 60
 * 
 * 说明：从三个 OR 分支中推断出 a 的最小值是 10，最大值是 60，添加约束 a > 10 and a < 60
 *
 * a between 10 and 20 and b between 10 and 20 or a between 100 and 200 and b between 100 and 200
 *   => (a <= 20 and b <= 20 or a >= 100 and b >= 100) and a >= 10 and a <= 200 and b >= 10 and b <= 200
 * 
 * 说明：从两个 OR 分支中推断出 a 的范围是 [10, 200]，b 的范围是 [10, 200]，添加相应约束
 */
public class AddMinMax implements ExpressionPatternRuleFactory, ValueDescVisitor<Map<Expression, MinMaxValue>, Void> {
    public static final AddMinMax INSTANCE = new AddMinMax();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CompoundPredicate.class)
                        .whenCtx(ctx -> PlanUtils.isConditionExpressionPlan(ctx.rewriteContext.plan.orElse(null)))
                        .thenApply(ctx -> rewrite(ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.ADD_MIN_MAX)
        );
    }

    /**
     * 重写复合谓词表达式，为其添加最小最大值范围约束
     * 
     * 处理流程：
     * 1. 使用 RangeInference 分析表达式，推断出每个表达式的值范围描述（ValueDesc）
     * 2. 通过访问者模式遍历 ValueDesc，提取每个表达式的最小最大值信息
     * 3. 移除不必要的 min/max 值（例如原表达式中已存在的约束条件）
     * 4. 如果存在有效的 min/max 值，则添加范围约束条件（如 id >= 2）并返回新表达式
     * 5. 否则返回原表达式
     * 
     * 示例：
     * OR[(id#0 = 5),(id#0 = 2),(id#0 > 10)]
     * => AND[OR[id#0 IN (5, 2),(id#0 > 10)],(id#0 >= 2)]
     * 
     * @param expr 待重写的复合谓词表达式（AND 或 OR）
     * @param context 表达式重写上下文
     * @return 添加了范围约束的新表达式，如果没有需要添加的约束则返回原表达式
     */
    public Expression rewrite(CompoundPredicate expr, ExpressionRewriteContext context) {
        // 使用 RangeInference 分析表达式，获取值的范围描述
        ValueDesc valueDesc = (new RangeInference()).getValue(expr, context);
        // 通过访问者模式提取每个表达式的最小最大值信息
        Map<Expression, MinMaxValue> exprMinMaxValues = valueDesc.accept(this, null);
        // 移除不必要的 min/max 值（例如原表达式中已存在的约束）
        removeUnnecessaryMinMaxValues(expr, exprMinMaxValues);
        // 如果存在有效的 min/max 值，添加范围约束并返回新表达式
        if (!exprMinMaxValues.isEmpty()) {
            return addExprMinMaxValues(expr, context, exprMinMaxValues);
        }
        // 没有需要添加的约束，返回原表达式
        return expr;
    }

    private enum MatchMinMax {
        MATCH_MIN,
        MATCH_MAX,
        MATCH_NONE,
    }

    /** record each expression's min and max value */
    public static class MinMaxValue {
        // min max range, if range = null means empty
        Range<ComparableLiteral> range;

        // expression in range is discrete value
        boolean isDiscrete;

        // expr relative order, for keep order after add min-max to the expression
        int exprOrderIndex;

        public MinMaxValue(Range<ComparableLiteral> range, boolean isDiscrete, int exprOrderIndex) {
            this.range = range;
            this.isDiscrete = isDiscrete;
            this.exprOrderIndex = exprOrderIndex;
        }
    }

    private void removeUnnecessaryMinMaxValues(Expression expr, Map<Expression, MinMaxValue> exprMinMaxValues) {
        exprMinMaxValues.entrySet().removeIf(entry -> entry.getValue().isDiscrete || entry.getValue().range == null
                || (!entry.getValue().range.hasLowerBound() && !entry.getValue().range.hasUpperBound()));
        if (exprMinMaxValues.isEmpty()) {
            return;
        }

        // keep original expression order, don't rewrite a sub expression if it's in original conjunctions.
        // example: if original expression is:  '(a >= 100) AND (...)',  and after visiting got a's range is [100, 200],
        // because 'a >= 100' is in expression's conjunctions, don't add 'a >= 100' to expression,
        // then the rewritten expression is '((a >= 100) AND (...)) AND (a <= 200)'
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(expr);
        for (Expression conjunct : conjuncts) {
            List<Expression> disjunctions = ExpressionUtils.extractDisjunction(conjunct);
            if (disjunctions.isEmpty() || !(disjunctions.get(0) instanceof ComparisonPredicate)) {
                continue;
            }
            Expression targetExpr = disjunctions.get(0).child(0);
            boolean matchMin = false;
            boolean matchMax = false;
            for (Expression disjunction : disjunctions) {
                MatchMinMax match = getExprMatchMinMax(disjunction, exprMinMaxValues);
                if (match == MatchMinMax.MATCH_NONE || !disjunction.child(0).equals(targetExpr)) {
                    matchMin = false;
                    matchMax = false;
                    break;
                }
                if (match == MatchMinMax.MATCH_MIN) {
                    matchMin = true;
                } else if (match == MatchMinMax.MATCH_MAX) {
                    matchMax = true;
                }
            }
            MinMaxValue targetValue = exprMinMaxValues.get(targetExpr);
            if (matchMin) {
                // remove targetValue's lower bound
                if (targetValue.range.hasUpperBound()) {
                    targetValue.range = Range.upTo(targetValue.range.upperEndpoint(),
                        targetValue.range.upperBoundType());
                } else {
                    exprMinMaxValues.remove(targetExpr);
                }
            }
            if (matchMax) {
                // remove targetValue's upper bound
                if (targetValue.range.hasLowerBound()) {
                    targetValue.range = Range.downTo(targetValue.range.lowerEndpoint(),
                        targetValue.range.lowerBoundType());
                } else {
                    exprMinMaxValues.remove(targetExpr);
                }
            }
        }
    }

    /**
     * 为表达式添加最小最大值范围约束
     * 
     * 处理流程：
     * 1. 按照表达式的相对顺序（exprOrderIndex）排序，保持表达式的原始顺序
     * 2. 遍历每个表达式的范围信息，生成相应的比较谓词：
     *    - 如果范围是单点值（如 [2, 2]），生成等值谓词（id = 2）
     *    - 如果有下界，根据边界类型生成 >= 或 > 谓词（如 id >= 2 或 id > 2）
     *    - 如果有上界，根据边界类型生成 <= 或 < 谓词（如 id <= 10 或 id < 10）
     * 3. 移除原表达式中已存在的重复约束（避免重复添加）
     * 4. 将新生成的约束表达式与原表达式合并（使用 AND 连接）
     * 5. 执行常量折叠评估，简化表达式
     * 6. 如果结果与原表达式相同，返回原表达式；否则返回新表达式
     * 
     * 示例：
     * 输入：OR[id#0 IN (5, 2),(id#0 > 10)]，exprMinMaxValues = {id#0: [2, +∞)}
     * 输出：AND[OR[id#0 IN (5, 2),(id#0 > 10)],(id#0 >= 2)]
     * 
     * @param expr 原始表达式
     * @param context 表达式重写上下文
     * @param exprMinMaxValues 每个表达式对应的最小最大值信息映射
     * @return 添加了范围约束的新表达式，如果结果与原表达式相同则返回原表达式
     */
    private Expression addExprMinMaxValues(Expression expr, ExpressionRewriteContext context,
            Map<Expression, MinMaxValue> exprMinMaxValues) {
        // 按照表达式的相对顺序排序，保持表达式的原始顺序
        List<Map.Entry<Expression, MinMaxValue>> minMaxExprs = exprMinMaxValues.entrySet().stream()
                .sorted((a, b) -> Integer.compare(a.getValue().exprOrderIndex, b.getValue().exprOrderIndex))
                .collect(Collectors.toList());
        // 预分配空间：每个表达式最多生成 2 个约束（上界和下界）
        List<Expression> addExprs = Lists.newArrayListWithExpectedSize(minMaxExprs.size() * 2);
        
        // 遍历每个表达式，根据范围信息生成比较谓词
        for (Map.Entry<Expression, MinMaxValue> entry : minMaxExprs) {
            Expression targetExpr = entry.getKey();
            Range<ComparableLiteral> range = entry.getValue().range;
            
            // 特殊情况：如果范围是单点值（如 [2, 2]），生成等值谓词
            if (range.hasLowerBound() && range.hasUpperBound()
                    && range.lowerEndpoint().equals(range.upperEndpoint())
                    && range.lowerBoundType() == BoundType.CLOSED
                    && range.upperBoundType() == BoundType.CLOSED) {
                Expression cmp = new EqualTo(targetExpr, (Literal) range.lowerEndpoint());
                addExprs.add(cmp);
                continue;
            }
            
            // 如果有下界，生成 >= 或 > 谓词
            if (range.hasLowerBound()) {
                ComparableLiteral literal = range.lowerEndpoint();
                Expression cmp = range.lowerBoundType() == BoundType.CLOSED
                        ? new GreaterThanEqual(targetExpr, (Literal) literal)
                        : new GreaterThan(targetExpr, (Literal) literal);
                addExprs.add(cmp);
            }
            
            // 如果有上界，生成 <= 或 < 谓词
            if (range.hasUpperBound()) {
                ComparableLiteral literal = range.upperEndpoint();
                Expression cmp = range.upperBoundType() == BoundType.CLOSED
                        ? new LessThanEqual(targetExpr, (Literal) literal)
                        : new LessThan(targetExpr, (Literal) literal);
                addExprs.add(cmp);
            }
        }

        // 在将新约束添加到原表达式之前，先移除原表达式中已存在的重复约束
        // 避免重复添加相同的约束条件（如原表达式中已有 id >= 2，就不需要再添加）
        Expression replaceOriginExpr = replaceCmpMinMax(expr, Sets.newHashSet(addExprs));

        // 将原表达式（已移除重复约束）放在最前面，然后添加新的约束表达式
        addExprs.add(0, replaceOriginExpr);
        // 使用 AND 连接所有表达式，并执行常量折叠评估
        Expression result = FoldConstantRuleOnFE.evaluate(ExpressionUtils.and(addExprs), context);
        
        // 如果结果与原表达式相同，返回原表达式（避免不必要的对象创建）
        if (result.equals(expr)) {
            return expr;
        }

        return result;
    }

    private Expression replaceCmpMinMax(Expression expr, Set<Expression> cmpMinMaxExprs) {
        // even if expr is nullable, replace it to true is ok because expression will 'AND' it later
        if (cmpMinMaxExprs.contains(expr)) {
            return BooleanLiteral.TRUE;
        }

        // only replace those expression whose all its ancestors are AND / OR
        if (!(expr instanceof CompoundPredicate)) {
            return expr;
        }

        ImmutableList.Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(expr.arity());
        boolean changed = false;
        for (Expression child : expr.children()) {
            Expression newChild = replaceCmpMinMax(child, cmpMinMaxExprs);
            if (child != newChild) {
                changed = true;
            }
            newChildren.add(newChild);
        }

        if (changed) {
            return expr.withChildren(newChildren.build());
        } else {
            return expr;
        }
    }

    private MatchMinMax getExprMatchMinMax(Expression expr,
            Map<Expression, MinMaxValue> exprMinMaxValues) {
        if (!(expr instanceof ComparisonPredicate)) {
            return MatchMinMax.MATCH_NONE;
        }

        ComparisonPredicate cp = (ComparisonPredicate) expr;
        Expression left = cp.left();
        Expression right = cp.right();
        if (!(right instanceof ComparableLiteral)) {
            return MatchMinMax.MATCH_NONE;
        }

        MinMaxValue value = exprMinMaxValues.get(left);
        if (value == null) {
            return MatchMinMax.MATCH_NONE;
        }

        if (cp instanceof GreaterThan || cp instanceof GreaterThanEqual) {
            if (value.range.hasLowerBound() && value.range.lowerEndpoint().equals(right)) {
                BoundType boundType = value.range.lowerBoundType();
                if ((boundType == BoundType.CLOSED && expr instanceof GreaterThanEqual)
                        || (boundType == BoundType.OPEN && expr instanceof GreaterThan)) {
                    return MatchMinMax.MATCH_MIN;
                }
            }
        } else if (expr instanceof LessThan || expr instanceof LessThanEqual) {
            if (value.range.hasUpperBound() && value.range.upperEndpoint().equals(right)) {
                BoundType boundType = value.range.upperBoundType();
                if ((boundType == BoundType.CLOSED && expr instanceof LessThanEqual)
                        || (boundType == BoundType.OPEN && expr instanceof LessThan)) {
                    return MatchMinMax.MATCH_MAX;
                }
            }
        }

        return MatchMinMax.MATCH_NONE;
    }

    private boolean isExprNeedAddMinMax(Expression expr) {
        return (expr instanceof SlotReference) && ((SlotReference) expr).getOriginalColumn().isPresent();
    }

    @Override
    public Map<Expression, MinMaxValue> visitEmptyValue(EmptyValue value, Void context) {
        Expression reference = value.getReference();
        Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
        if (isExprNeedAddMinMax(reference)) {
            exprMinMaxValues.put(reference, new MinMaxValue(null, true, 0));
        }
        return exprMinMaxValues;
    }

    @Override
    public Map<Expression, MinMaxValue> visitDiscreteValue(DiscreteValue value, Void context) {
        Expression reference = value.getReference();
        Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
        if (isExprNeedAddMinMax(reference)) {
            exprMinMaxValues.put(reference, new MinMaxValue(Range.encloseAll(value.getValues()), true, 0));
        }
        return exprMinMaxValues;
    }

    @Override
    public Map<Expression, MinMaxValue> visitNotDiscreteValue(NotDiscreteValue value, Void context) {
        return Maps.newHashMap();
    }

    @Override
    public Map<Expression, MinMaxValue> visitIsNullValue(IsNullValue value, Void context) {
        return Maps.newHashMap();
    }

    @Override
    public Map<Expression, MinMaxValue> visitIsNotNullValue(IsNotNullValue value, Void context) {
        return Maps.newHashMap();
    }

    @Override
    public Map<Expression, MinMaxValue> visitRangeValue(RangeValue value, Void context) {
        Expression reference = value.getReference();
        Map<Expression, MinMaxValue> exprMinMaxValues = Maps.newHashMap();
        if (isExprNeedAddMinMax(reference)) {
            exprMinMaxValues.put(reference, new MinMaxValue(value.getRange(), false, 0));
        }
        return exprMinMaxValues;
    }

    /**
     * 访问复合值（CompoundValue），合并多个 ValueDesc 中的范围信息
     * 
     * 这是生成范围约束的关键方法。对于 OR 表达式，会使用 span 操作合并范围，
     * 从而推断出表达式的整体最小最大值范围。
     * 
     * 处理逻辑：
     * 1. 对于 AND 表达式（isAnd = true）：
     *    - 使用 intersection（交集）合并范围，取最严格的约束
     *    - 例如：[2, 5] ∩ (10, +∞) = 空集（不相连）或交集区间
     * 
     * 2. 对于 OR 表达式（isAnd = false）：
     *    - 使用 span（并集的最小覆盖范围）合并范围，取最宽松的约束
     *    - 例如：[2, 5] span (10, +∞) = [2, +∞)
     *    - 这是生成 id >= 2 的关键步骤：通过合并 OR 中的各个分支范围，
     *      推断出整个 OR 表达式的最小值范围
     * 
     * 示例：
     * 输入：OR[id#0 IN (5, 2),(id#0 > 10)]
     * - sourceValues[0]: DiscreteValue(id#0, {5, 2}) → MinMaxValue(id#0, [2, 5])
     * - sourceValues[1]: RangeValue(id#0, (10, +∞)) → MinMaxValue(id#0, (10, +∞))
     * - 第 451 行执行：[2, 5].span((10, +∞)) = [2, +∞)
     * - 返回：{id#0: MinMaxValue([2, +∞), false)}
     * 
     * 后续在 addExprMinMaxValues 中，会根据 [2, +∞) 生成 id#0 >= 2 约束
     * 
     * @param valueDesc 复合值描述，包含多个子 ValueDesc（来自 AND 或 OR 表达式）
     * @param context 访问上下文
     * @return 每个表达式对应的合并后的最小最大值信息映射
     */
    @Override
    public Map<Expression, MinMaxValue> visitCompoundValue(CompoundValue valueDesc, Void context) {
        List<ValueDesc> sourceValues = valueDesc.getSourceValues();
        // 处理第一个 sourceValue，作为初始结果
        Map<Expression, MinMaxValue> result = Maps.newHashMap(sourceValues.get(0).accept(this, context));
        int nextExprOrderIndex = result.values().stream().mapToInt(k -> k.exprOrderIndex).max().orElse(0);
        
        // 遍历后续的 sourceValues，逐个合并到 result 中
        for (int i = 1; i < sourceValues.size(); i++) {
            // 处理 sourceValues[i] 中的表达式
            Map<Expression, MinMaxValue> minMaxValues = sourceValues.get(i).accept(this, context);
            // 合并 sourceValues[i] 中的值到 result 中
            // 同时保持值的相对顺序
            // 例如：如果 a 和 b 在 sourceValues[i] 中但不在 result 中，则在合并时
            // a 和 b 会被分配新的 exprOrderIndex（使用 nextExprOrderIndex）
            // 如果在 sourceValues[i] 中，a 的 exprOrderIndex < b 的 exprOrderIndex，
            // 则确保在 result 中，a 的新 exprOrderIndex < b 的新 exprOrderIndex
            // 这样它们的相对顺序得以保持
            List<Map.Entry<Expression, MinMaxValue>> minMaxValueList = minMaxValues.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(a.getValue().exprOrderIndex, b.getValue().exprOrderIndex))
                    .collect(Collectors.toList());
            
            for (Map.Entry<Expression, MinMaxValue> entry : minMaxValueList) {
                Expression expr = entry.getKey();
                MinMaxValue value = result.get(expr);
                MinMaxValue otherValue = entry.getValue();
                
                if (valueDesc.isAnd()) {
                    // AND 表达式：使用交集（intersection）合并范围，取最严格的约束
                    if (value == null) { // value = null 表示范围为全部
                        nextExprOrderIndex++;
                        value = otherValue;
                        value.exprOrderIndex = nextExprOrderIndex;
                        result.put(expr, value);
                    } else if (otherValue.range == null) { // range = null 表示空范围
                        value.range = null;
                    } else if (value.range != null) {
                        if (value.range.isConnected(otherValue.range)) {
                            // 如果两个范围相连，计算交集
                            Range<ComparableLiteral> newRange = value.range.intersection(otherValue.range);
                            if (!newRange.isEmpty()) {
                                value.range = newRange;
                                // 如果 newRange.lowerEndpoint().equals(newRange.upperEndpoint())，
                                // 那么 isDiscrete 应该为 true
                                // 但不需要这样做，因为 AddMinMax 不会处理离散值情况
                                value.isDiscrete = value.isDiscrete && otherValue.isDiscrete;
                            } else {
                                value.range = null;
                            }
                        } else {
                            // 两个范围不相连，交集为空
                            value.range = null;
                        }
                    }
                } else {
                    // OR 表达式：使用 span（并集的最小覆盖范围）合并范围，取最宽松的约束
                    // 这是生成 id >= 2 的关键步骤！
                    if (value == null) { // value = null 表示范围为全部
                        nextExprOrderIndex++;
                        value = new MinMaxValue(Range.all(), false, nextExprOrderIndex);
                        result.put(expr, value);
                    } else if (value.range == null) { // range = null 表示空范围
                        value.range = otherValue.range;
                        value.isDiscrete = otherValue.isDiscrete;
                    } else if (otherValue.range != null) {
                        // 关键步骤：使用 span 合并两个范围
                        // 例如：[2, 5].span((10, +∞)) = [2, +∞)
                        // 这样就能推断出 OR 表达式的整体最小值范围
                        value.range = value.range.span(otherValue.range);
                        value.isDiscrete = value.isDiscrete && otherValue.isDiscrete;
                    }
                }
            }

            // 处理不在 sourceValues[i] 中的表达式（仅对 OR 表达式）
            if (!valueDesc.isAnd()) {
                for (Map.Entry<Expression, MinMaxValue> entry : result.entrySet()) {
                    Expression expr = entry.getKey();
                    MinMaxValue value = entry.getValue();
                    // 如果某个表达式不在当前 sourceValue 中，说明它可能出现在其他分支
                    // 对于 OR 表达式，需要将范围扩展为全部（因为其他分支可能满足）
                    if (!minMaxValues.containsKey(expr)) {
                        value.range = Range.all();
                        value.isDiscrete = false;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Map<Expression, MinMaxValue> visitUnknownValue(UnknownValue valueDesc, Void context) {
        return Maps.newHashMap();
    }
}
