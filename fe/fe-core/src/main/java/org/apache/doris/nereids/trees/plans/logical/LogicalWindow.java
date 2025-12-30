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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Window;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * logical node to deal with window functions;
 */
public class LogicalWindow<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Window {

    // List<Alias<WindowExpression>>
    private final List<NamedExpression> windowExpressions;

    private final boolean isChecked;

    public LogicalWindow(List<NamedExpression> windowExpressions, CHILD_TYPE child) {
        this(windowExpressions, false, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> windowExpressions, boolean isChecked, CHILD_TYPE child) {
        this(windowExpressions, isChecked, Optional.empty(), Optional.empty(), child);
    }

    public LogicalWindow(List<NamedExpression> windowExpressions, boolean isChecked,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_WINDOW, groupExpression, logicalProperties, child);
        this.windowExpressions = ImmutableList.copyOf(Objects.requireNonNull(windowExpressions, "output expressions"
                + "in LogicalWindow cannot be null"));
        this.isChecked = isChecked;
    }

    public boolean isChecked() {
        return isChecked;
    }

    @Override
    public List<NamedExpression> getWindowExpressions() {
        return windowExpressions;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return windowExpressions;
    }

    public List<WindowExpression> getActualWindowExpressions() {
        List<WindowExpression> actualWindowExpressions = new ArrayList<>();
        for (NamedExpression expr : windowExpressions) {
            actualWindowExpressions.add((WindowExpression) (expr.child(0)));
        }
        return actualWindowExpressions;
    }

    public LogicalWindow<Plan> withExpressionsAndChild(List<NamedExpression> windowExpressions, Plan child) {
        return new LogicalWindow<>(windowExpressions, isChecked, child);
    }

    public LogicalWindow<Plan> withChecked(List<NamedExpression> windowExpressions, Plan child) {
        return new LogicalWindow<>(windowExpressions, true, Optional.empty(),
                Optional.of(getLogicalProperties()), child);
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(windowExpressions, isChecked, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalWindow(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalWindow<>(windowExpressions, isChecked,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalWindow<>(windowExpressions, isChecked, groupExpression, logicalProperties, children.get(0));
    }

    /**
     * LogicalWindow need to add child().getOutput() as its outputs, to resolve patterns like the following
     * after the implementation rule LogicalWindowToPhysicalWindow:
     * <p>
     * origin:
     * LogicalProject( projects = [row_number as `row_number`, rank as `rank`]
     * +--LogicalWindow( windowExpressions = [row_number() over(order by c1), rank() over(order by c2)]
     * <p>
     * after(not show PhysicalLogicalQuickSort generated by enforcer):
     * PhysicalProject( projects = [row_number as `row_number`, rank as `rank`]
     * +--PhysicalWindow( windowExpressions = [row_number() over(order by c1)])
     * +----PhysicalWindow( windowExpressions = [rank() over(order by c2)])
     * <p>
     * if we don't add child().getOutput(), the top-PhysicalProject cannot find rank()
     */
    @Override
    public List<Slot> computeOutput() {
        return new ImmutableList.Builder<Slot>()
            .addAll(child().getOutput())
            .addAll(windowExpressions.stream()
            .map(NamedExpression::toSlot)
            .collect(ImmutableList.toImmutableList()))
            .build();
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalWindow",
                "windowExpressions", windowExpressions,
                "isChecked", isChecked,
                "stats", statistics
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalWindow<?> that = (LogicalWindow<?>) o;
        return Objects.equals(windowExpressions, that.windowExpressions)
                && isChecked == that.isChecked;
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowExpressions, isChecked);
    }

    /**
     * 获取可以下推到 PartitionTopN 的窗口函数候选和对应的分区限制值。
     * 该方法用于判断窗口函数是否可以转换为 PartitionTopN 优化，并返回选定的窗口函数和分区限制。
     *
     * @param filter
     *              对于分区 TopN 过滤场景（如 WHERE row_number <= 100），表示过滤条件；
     *              对于分区限制场景（如通过 TopN 算子下推），该参数为 null。
     * @param partitionLimit
     *              对于分区 TopN 过滤场景，表示过滤边界值，例如 row_number <= 100 中的 100；
     *              对于分区限制场景，表示限制值。
     * @return
     *              如果返回 null，表示无效的情况或优化选项被禁用；
     *              否则返回选定的窗口函数和对应的分区限制值。
     *              特殊返回值：limit 值为 -1 表示该情况可以优化为空关系（空结果集）。
     * 
     * Get push down window function candidate and corresponding partition limit.
     *
     * @param filter
     *              For partition topN filter cases, it means the topN filter;
     *              For partition limit cases, it will be null.
     * @param partitionLimit
     *              For partition topN filter cases, it means the filter boundary,
     *                  e.g, 100 for the case rn <= 100;
     *              For partition limit cases, it means the limit.
     * @return
     *              Return null means invalid cases or the opt option is disabled,
     *              else return the chosen window function and the chosen partition limit.
     *              A special limit -1 means the case can be optimized as empty relation.
     */
    public Pair<WindowExpression, Long> getPushDownWindowFuncAndLimit(LogicalFilter<?> filter, long partitionLimit) {
        // 检查会话变量是否启用了 PartitionTopN 优化，如果未启用则直接返回 null
        if (!ConnectContext.get().getSessionVariable().isEnablePartitionTopN()) {
            return null;
        }
        
        // 检查是否已经进行过此优化：如果窗口算子的子节点已经是 LogicalPartitionTopN，
        // 或者在子节点和 LogicalPartitionTopN 之间存在 LogicalFilter，
        // 则说明已经优化过了，避免重复优化
        if (child(0) instanceof LogicalPartitionTopN
                || (child(0) instanceof LogicalFilter
                && child(0).child(0) != null
                && child(0).child(0) instanceof LogicalPartitionTopN)) {
            return null;
        }

        // 检查窗口函数的限制条件。窗口函数必须满足以下条件才能进行 PartitionTopN 优化：
        // 1. 窗口函数必须是 'row_number()'、'rank()' 或 'dense_rank()' 之一
        // 2. 窗口帧必须是 'UNBOUNDED PRECEDING' 到 'CURRENT ROW'
        // 3. 'PARTITION' 键和 'ORDER' 键不能同时为空
        // 4. 所有窗口表达式的排序键必须兼容（相同或为空）
        WindowExpression chosenWindowFunc = null;
        long chosenPartitionLimit = Long.MAX_VALUE;
        long chosenRowNumberPartitionLimit = Long.MAX_VALUE;
        boolean hasRowNumber = false;
        
        // 遍历所有窗口表达式，找到满足条件的窗口函数
        for (NamedExpression windowExpr : windowExpressions) {
            // 验证窗口表达式的结构：必须存在且只有一个子节点，且该子节点是 WindowExpression
            if (windowExpr == null || windowExpr.children().size() != 1
                    || !(windowExpr.child(0) instanceof WindowExpression)) {
                return null;
            }
            WindowExpression windowFunc = (WindowExpression) windowExpr.child(0);

            // 检查窗口函数名称：只支持 ROW_NUMBER、RANK 和 DENSE_RANK 三种函数
            if (!(windowFunc.getFunction() instanceof RowNumber
                    || windowFunc.getFunction() instanceof Rank
                    || windowFunc.getFunction() instanceof DenseRank)) {
                return null;
            }

            // 检查分区键和排序键：两者不能同时为空
            // 如果都为空，无法进行分区级别的 TopN 优化
            if (windowFunc.getPartitionKeys().isEmpty() && windowFunc.getOrderKeys().isEmpty()) {
                return null;
            }

            // 检查窗口类型和窗口帧：窗口帧必须是 'UNBOUNDED PRECEDING' 到 'CURRENT ROW'
            // 这是 PartitionTopN 优化的前提条件，其他窗口帧类型不支持此优化
            Optional<WindowFrame> windowFrame = windowFunc.getWindowFrame();
            if (windowFrame.isPresent()) {
                WindowFrame frame = windowFrame.get();
                if (!(frame.getLeftBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.UNBOUNDED_PRECEDING
                        && frame.getRightBoundary().getFrameBoundType() == WindowFrame.FrameBoundType.CURRENT_ROW)) {
                    return null;
                }
            } else {
                // 如果没有窗口帧定义，也不支持此优化
                return null;
            }

            // 处理过滤条件（filter 不为 null 的情况）
            if (filter != null) {
                // 目前只支持简单条件：'窗口函数列 </ <=/ = 常量'
                // 例如：row_number <= 100, rank < 50 等
                boolean hasPartitionLimit = false;
                long curPartitionLimit = Long.MAX_VALUE;
                Set<Expression> conjuncts = filter.getConjuncts();
                // 提取与当前窗口表达式相关的过滤条件（通过 ExprId 关联）
                Set<Expression> relatedConjuncts = extractRelatedConjuncts(conjuncts, windowExpr.getExprId());
                
                // 遍历所有相关的过滤条件，提取 limit 值
                for (Expression conjunct : relatedConjuncts) {
                    // 前置检查已在提取阶段完成
                    BinaryOperator op = (BinaryOperator) conjunct;
                    Expression rightChild = op.children().get(1);
                    long limitVal = ((IntegerLikeLiteral) rightChild).getLongValue();
                    
                    // 根据比较操作符调整 limit 值
                    // 例如：row_number < 100 应该转换为 limit 99（因为 < 不包含边界值）
                    if (conjunct instanceof LessThan) {
                        limitVal--;
                    }
                    
                    // 如果调整后的 limit 值小于 0，表示结果为空关系，可以直接优化
                    if (limitVal < 0) {
                        // 设置返回 limit 值为 -1，表示可以优化为空关系
                        chosenPartitionLimit = -1;
                        chosenRowNumberPartitionLimit = -1;
                        break;
                    }
                    
                    // 如果有多个过滤条件，取最小的 limit 值（最严格的限制）
                    if (hasPartitionLimit) {
                        curPartitionLimit = Math.min(curPartitionLimit, limitVal);
                    } else {
                        curPartitionLimit = limitVal;
                        hasPartitionLimit = true;
                    }
                }
                
                // 如果检测到空关系情况，直接选择当前窗口函数并退出循环
                if (chosenPartitionLimit == -1) {
                    chosenWindowFunc = windowFunc;
                    break;
                } else if (windowFunc.getFunction() instanceof RowNumber) {
                    // 优先选择 row_number 函数（性能最好）
                    // 如果存在多个 row_number，选择 limit 值最小的那个
                    if (curPartitionLimit < chosenRowNumberPartitionLimit) {
                        chosenRowNumberPartitionLimit = curPartitionLimit;
                        chosenWindowFunc = windowFunc;
                        hasRowNumber = true;
                    }
                } else if (!hasRowNumber) {
                    // 如果没有 row_number 函数，选择 limit 值最小的窗口函数
                    if (curPartitionLimit < chosenPartitionLimit) {
                        chosenPartitionLimit = curPartitionLimit;
                        chosenWindowFunc = windowFunc;
                    }
                }
            } else {
                // filter 为 null 的情况：直接使用传入的 partitionLimit
                // 这种情况通常是从 TopN 算子下推 limit 值
                chosenWindowFunc = windowFunc;
                chosenPartitionLimit = partitionLimit;
                // 如果当前是 row_number 函数，优先选择它并退出循环
                if (windowFunc.getFunction() instanceof RowNumber) {
                    break;
                }
            }
        }

        // 如果未找到合适的窗口函数，或 limit 值无效（仍为初始值 Long.MAX_VALUE），返回 null
        if (chosenWindowFunc == null || (chosenPartitionLimit == Long.MAX_VALUE
                && chosenRowNumberPartitionLimit == Long.MAX_VALUE)) {
            return null;
        } else {
            // 最后一步：验证所有窗口表达式的排序键是否兼容
            // 要求：所有窗口表达式的 ORDER BY 键要么为空，要么与选定的窗口函数的 ORDER BY 键相同
            // 这是为了确保可以安全地应用 PartitionTopN 优化
            for (NamedExpression windowExpr : windowExpressions) {
                if (windowExpr != null && windowExpr instanceof Alias
                        && windowExpr.child(0) instanceof WindowExpression) {
                    WindowExpression windowFunc = (WindowExpression) windowExpr.child(0);
                    // 如果当前窗口函数的排序键为空，或与选定窗口函数的排序键相同，则兼容
                    if (windowFunc.getOrderKeys().isEmpty()
                            || windowFunc.getOrderKeys().equals(chosenWindowFunc.getOrderKeys())) {
                        continue;
                    } else {
                        // 如果存在不兼容的排序键，无法进行优化
                        return null;
                    }
                }
            }
            // 返回选定的窗口函数和对应的分区限制值
            // 优先返回 row_number 的限制值（如果存在），否则返回其他窗口函数的限制值
            return Pair.of(chosenWindowFunc, hasRowNumber ? chosenRowNumberPartitionLimit : chosenPartitionLimit);
        }
    }

    /**
     * pushPartitionLimitThroughWindow is used to push the partitionLimit through the window
     * and generate the partitionTopN. If the window can not meet the requirement,
     * it will return null. So when we use this function, we need check the null in the outside.
     */
    public Plan pushPartitionLimitThroughWindow(WindowExpression windowFunc,
            long partitionLimit, boolean hasGlobalLimit) {
        LogicalWindow<?> window = (LogicalWindow<?>) withChildren(new LogicalPartitionTopN<>(windowFunc, hasGlobalLimit,
                partitionLimit, child(0)));
        return window;
    }

    private Set<Expression> extractRelatedConjuncts(Set<Expression> conjuncts, ExprId slotRefID) {
        Predicate<Expression> condition = conjunct -> {
            if (!(conjunct instanceof BinaryOperator)) {
                return false;
            }
            BinaryOperator op = (BinaryOperator) conjunct;
            Expression leftChild = op.children().get(0);
            Expression rightChild = op.children().get(1);

            if (!(conjunct instanceof LessThan || conjunct instanceof LessThanEqual || conjunct instanceof EqualTo)) {
                return false;
            }

            // TODO: Now, we only support the column on the left side.
            if (!(leftChild instanceof SlotReference) || !(rightChild instanceof IntegerLikeLiteral)) {
                return false;
            }
            return ((SlotReference) leftChild).getExprId() == slotRefID;
        };

        return conjuncts.stream()
                .filter(condition)
                .collect(ImmutableSet.toImmutableSet());
    }

    private boolean isUnique(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return false;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();
        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return false;
        }
        ImmutableSet<Slot> slotSet = partitionKeys.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // if partition by keys are uniform or empty, output is unique
        if (child(0).getLogicalProperties().getTrait().isUniformAndNotNull(slotSet)
                || slotSet.isEmpty()) {
            if (windowExpr.getFunction() instanceof RowNumber) {
                return true;
            }
        }
        return false;
    }

    private boolean isUniform(NamedExpression namedExpression) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return false;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();
        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return false;
        }
        ImmutableSet<Slot> slotSet = partitionKeys.stream()
                .map(s -> (Slot) s)
                .collect(ImmutableSet.toImmutableSet());
        // if partition by keys are unique, output is uniform
        if (child(0).getLogicalProperties().getTrait().isUniqueAndNotNull(slotSet)) {
            if (windowExpr.getFunction() instanceof RowNumber
                    || windowExpr.getFunction() instanceof Rank
                    || windowExpr.getFunction() instanceof DenseRank) {
                return true;
            }
        }
        return false;
    }

    private void updateFuncDepsByWindowExpr(NamedExpression namedExpression, ImmutableSet.Builder<FdItem> builder) {
        if (namedExpression.children().size() != 1 || !(namedExpression.child(0) instanceof WindowExpression)) {
            return;
        }
        WindowExpression windowExpr = (WindowExpression) namedExpression.child(0);
        List<Expression> partitionKeys = windowExpr.getPartitionKeys();

        // Now we only support slot type keys
        if (!partitionKeys.stream().allMatch(Slot.class::isInstance)) {
            return;
        }
        //ImmutableSet<Slot> slotSet = partitionKeys.stream()
        //        .map(s -> (Slot) s)
        //        .collect(ImmutableSet.toImmutableSet());
        // TODO: if partition by keys are unique, output is uniform
        // TODO: if partition by keys are uniform, output is unique
    }

    @Override
    public void computeUnique(Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
        for (NamedExpression namedExpression : windowExpressions) {
            if (isUnique(namedExpression)) {
                builder.addUniqueSlot(namedExpression.toSlot());
            }
        }
    }

    @Override
    public void computeUniform(Builder builder) {
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
        for (NamedExpression namedExpression : windowExpressions) {
            if (isUniform(namedExpression)) {
                builder.addUniformSlot(namedExpression.toSlot());
            }
        }
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
