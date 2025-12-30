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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 窗口表达式提取和规范化规则
 * 
 * 主要功能：
 * 1. 从 LogicalProject 的 projects 中提取窗口表达式
 * 2. 规范化 LogicalWindow 的结构
 * 3. 优化窗口函数的执行计划，将相关表达式下推到合适的层级
 * 
 * 处理流程：
 * - 检测包含窗口表达式的投影操作
 * - 将窗口表达式从投影中提取出来，创建独立的 LogicalWindow 节点
 * - 优化表达式下推，提高查询性能
 */
public class ExtractAndNormalizeWindowExpression extends OneRewriteRuleFactory implements NormalizeToSlot {

    @Override
    public Rule build() {
        return logicalProject()
                // 只有当投影中包含窗口表达式时才应用此规则
                .when(project -> ExpressionUtils.containsWindowExpression(project.getProjects()))
                .then(this::normalize)
                .toRule(RuleType.EXTRACT_AND_NORMALIZE_WINDOW_EXPRESSIONS);
    }

    /**
     * 规范化窗口表达式的核心方法
     * 
     * 处理步骤：
     * 1. 重写窗口表达式，移除常量分区键和排序键
     * 2. 处理可空聚合函数
     * 3. 创建三层结构：底层投影 -> 窗口操作 -> 顶层投影
     */
    private Plan normalize(LogicalProject<Plan> project) {
        // 第一步：重写窗口表达式，移除常量分区键和排序键
        List<NamedExpression> outputs =
                ExpressionUtils.rewriteDownShortCircuit(project.getProjects(), output -> {
                    if (output instanceof WindowExpression) {
                        WindowExpression windowExpression = (WindowExpression) output;
                        Expression expression = ((WindowExpression) output).getFunction();
                        
                        // 检查窗口函数中是否包含 ORDER BY 表达式（不支持）
                        if (expression.children().stream().anyMatch(OrderExpression.class::isInstance)) {
                            throw new AnalysisException("order by is not supported in " + expression);
                        }
                        
                        // 处理可空聚合函数：窗口函数中的聚合函数应该始终可空
                        // 因为窗口框架中可能没有数据，会生成空值
                        if (expression instanceof NullableAggregateFunction) {
                            windowExpression = ((WindowExpression) output).withFunction(
                                    ((NullableAggregateFunction) expression).withAlwaysNullable(true));
                        }

                        // 收集非字面量的分区键
                        ImmutableList.Builder<Expression> nonLiteralPartitionKeys =
                                ImmutableList.builderWithExpectedSize(windowExpression.getPartitionKeys().size());
                        for (Expression partitionKey : windowExpression.getPartitionKeys()) {
                            if (!partitionKey.isConstant()) {
                                nonLiteralPartitionKeys.add(partitionKey);
                            }
                        }

                        // 收集非字面量的排序表达式
                        ImmutableList.Builder<OrderExpression> nonLiteralOrderExpressions =
                                ImmutableList.builderWithExpectedSize(windowExpression.getOrderKeys().size());
                        for (OrderExpression orderExpr : windowExpression.getOrderKeys()) {
                            if (!orderExpr.getOrderKey().getExpr().isConstant()) {
                                nonLiteralOrderExpressions.add(orderExpr);
                            }
                        }

                        // 移除字面量的分区键和排序键，返回优化后的窗口表达式
                        return windowExpression.withPartitionKeysOrderKeys(
                                nonLiteralPartitionKeys.build(),
                                nonLiteralOrderExpressions.build()
                        );
                    }
                    return output;
                });

        // 第二步：处理底层投影
        // 收集需要下推的表达式，创建底层投影节点
        Set<Alias> existedAlias = ExpressionUtils.collect(outputs, Alias.class::isInstance);
        Set<Expression> toBePushedDown = collectExpressionsToBePushedDown(outputs);
        NormalizeToSlotContext context = NormalizeToSlotContext.buildContext(existedAlias, toBePushedDown);
        
        // 将需要下推的表达式转换为 NamedExpression，例如 (a+1) -> Alias(a+1)
        Set<NamedExpression> bottomProjects = context.pushDownToNamedExpression(toBePushedDown);
        Plan normalizedChild;
        if (bottomProjects.isEmpty()) {
            // 如果没有需要下推的表达式，直接使用原计划的子节点
            normalizedChild = project.child();
        } else {
            // 创建包含下推表达式的底层投影节点
            normalizedChild = project.withProjectsAndChild(
                    ImmutableList.copyOf(bottomProjects), project.child());
        }

        // 第三步：处理窗口表达式的输出和窗口规范
        // 需要将 WindowSpec 中的表达式替换为 SlotReference，因为 LogicalWindow.getExpressions() 需要
        
        // 由于别名被下推到底层投影，我们需要在输出中将别名的子表达式替换为对应的别名槽位
        // 创建自定义规范化映射：别名的子表达式 -> 别名对应的槽位
        Map<Expression, Slot> customNormalizeMap = toBePushedDown.stream()
                .filter(expr -> expr instanceof Alias && !(expr.child(0) instanceof Literal))
                .collect(Collectors.toMap(expr -> ((Alias) expr).child(), expr -> ((Alias) expr).toSlot(),
                        (oldExpr, newExpr) -> oldExpr));

        // 自定义规范化映射仅用于别名，所以我们只规范化输出中的别名
        List<NamedExpression> normalizedOutputs = context.normalizeToUseSlotRef(outputs,
                (ctx, expr) -> expr instanceof Alias ? customNormalizeMap.getOrDefault(expr, null) : null);
        // 使用自定义规范化映射替换 normalizedOutputs 中的子表达式
        normalizedOutputs = ExpressionUtils.replaceNamedExpressions(normalizedOutputs, customNormalizeMap);
        Set<WindowExpression> normalizedWindows =
                ExpressionUtils.collect(normalizedOutputs, WindowExpression.class::isInstance);

        // 为窗口表达式创建新的规范化上下文
        existedAlias = ExpressionUtils.collect(normalizedOutputs, Alias.class::isInstance);
        NormalizeToSlotContext ctxForWindows = NormalizeToSlotContext.buildContext(
                existedAlias, Sets.newHashSet(normalizedWindows));

        Set<NamedExpression> normalizedWindowWithAlias = ctxForWindows.pushDownToNamedExpression(normalizedWindows);
        // 只需要规范化的窗口表达式
        LogicalWindow normalizedLogicalWindow =
                new LogicalWindow<>(ImmutableList.copyOf(normalizedWindowWithAlias), normalizedChild);

        // 第四步：处理顶层投影
        // 创建最终的顶层投影，将窗口表达式的结果映射回原始输出
        List<NamedExpression> topProjects = ctxForWindows.normalizeToUseSlotRef(normalizedOutputs);
        return project.withProjectsAndChild(topProjects, normalizedLogicalWindow);
    }

    /**
     * 收集需要下推的表达式
     * 
     * 下推的表达式包括：
     * 1. 窗口函数和 WindowSpec 的 partitionKeys 和 orderKeys 中的表达式
     * 2. 输出表达式中的其他槽位
     * 
     * 示例：
     * avg(c) / sum(a+1) over (order by avg(b))  group by a
     * win(x/sum(z) over y)
     *     prj(x, y, a+1 as z)
     *         agg(avg(c) x, avg(b) y, a)
     *             proj(a b c)
     * toBePushDown = {avg(c), a+1, avg(b)}
     */
    private Set<Expression> collectExpressionsToBePushedDown(List<NamedExpression> expressions) {
        return expressions.stream()
            .flatMap(expression -> {
                if (expression.anyMatch(WindowExpression.class::isInstance)) {
                    Set<Slot> inputSlots = Sets.newHashSet(expression.getInputSlots());
                    Set<WindowExpression> collects = expression.collect(WindowExpression.class::isInstance);
                    
                    // 示例：substr(ref_1.cp_type, sum(CASE WHEN ref_1.cp_type = 0 THEN 3 ELSE 2 END) OVER (), 1)
                    // 在这种情况下，ref_1.cp_type 和 CASE WHEN ref_1.cp_type = 0 THEN 3 ELSE 2 END 应该被下推
                    // inputSlots = {ref_1.cp_type}
                    return Stream.concat(
                            collects.stream().flatMap(windowExpression ->
                                    windowExpression.getExpressionsInWindowSpec().stream()
                                    // 常量参数可能出现在窗口函数中（如 Lead, Lag）
                                    // 这些不应该被下推
                                    .filter(expr -> !expr.isConstant())
                            ),
                            inputSlots.stream()
                    ).distinct();
                }

                // 对于以下 SQL：
                //   select
                //     SUBSTR(orderdate,1,10) AS dt,
                //     ROW_NUMBER() OVER(PARTITION BY orderdate ORDER BY orderid DESC) AS rn
                //   from lineorders
                //   having dt = '2025-01-01'
                //
                // 我们不会将 `dt` 槽位下推到 LogicalWindow 下，而是将 [orderdate, orderid] 下推到底层投影
                // 因为如果我们下推 `dt`，计划树将是：
                //
                //             LogicalFilter(substr(dt#3, 1, 10) = '2025-01-01')
                //                                     |
                //      LogicalWindow(rowNumber(partition by orderdate#2, order by orderid#1))
                //                                     |
                //   LogicalProject(orderid#1, orderdate#2, substr(orderdate#1, 1, 10) as dt#3)
                //
                // 这样无法通过 `PushDownFilterThroughWindow` 下推过滤器，导致效率低下
                // 因为 LogicalFilter 中的 dt#3 不包含在 LogicalWindow 的分区键中：[orderdate#2]
                //
                // 所以我们只在 LogicalFilter 中下推 orderdate，不下推 `dt`：
                //
                //      LogicalFilter(substr(orderdate#2, 1, 10) = '2025-01-01')
                //                               |
                //      LogicalWindow(rowNumber(partition by orderdate#2, order by orderid#1))
                //                               |
                //             LogicalProject(orderid#1, orderdate#2)
                //
                // 然后，`PushDownFilterThroughWindow` 发现 LogicalFilter 的 `orderdate#2` 包含在
                // LogicalWindow 的分区键中：[orderdate#2]，可以将过滤器下推到：
                //
                //   LogicalWindow(rowNumber(partition by orderdate#2, order by orderid#1))
                //                               |
                //             LogicalProject(orderid#1, orderdate#2)
                //                              |
                //     LogicalFilter(substr(orderdate#2, 1, 10) = '2025-01-01')
                if (expression instanceof Alias) {
                    // 对于别名表达式，只下推其输入槽位
                    return expression.getInputSlots().stream();
                }
                // 对于其他表达式，直接返回表达式本身
                return ImmutableList.of(expression).stream();
            })
            .collect(ImmutableSet.toImmutableSet());
    }
}
