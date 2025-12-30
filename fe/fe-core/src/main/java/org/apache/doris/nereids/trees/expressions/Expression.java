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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.AnalysisException.ErrorCode;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UniqueFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition.MaxValue;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.LazyCompute;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 表达式的抽象基类。
 * Nereids 优化器中所有表达式的基类，包括字面量、函数、谓词、算术运算等。
 * 提供了表达式的通用功能：类型检查、SQL 生成、常量判断、哈希计算等。
 */
public abstract class Expression extends AbstractTreeNode<Expression> implements ExpressionTrait {
    /** 默认表达式名称 */
    public static final String DEFAULT_EXPRESSION_NAME = "expression";
    
    /** 表达式名称，用于自动生成列名（当没有别名时） */
    protected Optional<String> exprName = Optional.empty();
    
    /** 是否支持通过宽度和深度进行快速比较，用于优化 equals 方法 */
    protected final boolean compareWidthAndDepth;
    
    /** 表达式树的深度（从根到最深叶节点的路径长度） */
    private final int depth;
    
    /** 表达式树的宽度（所有子表达式的宽度之和） */
    private final int width;
    
    /** 标记表达式是否由谓词推断或其他推断生成 */
    private final boolean inferred;
    
    /** 标记表达式是否包含未绑定的符号 */
    private final boolean hasUnbound;
    
    /** 输入槽（列引用）的懒加载缓存，收集表达式中所有的 Slot，但不包括 ArrayItemSlot */
    private final Supplier<Set<Slot>> inputSlots = LazyCompute.of(
            () -> collect(e -> e instanceof Slot && !(e instanceof ArrayItemSlot)));
    
    /** 快速子节点哈希码，用于优化 equals 和 hashCode 计算 */
    private final int fastChildrenHashCode;
    
    /** SQL 字符串的懒加载缓存 */
    private final Supplier<String> toSqlCache = LazyCompute.of(this::computeToSql);
    
    /** 哈希码的懒加载缓存 */
    private final Supplier<Integer> hashCodeCache = LazyCompute.of(this::computeHashCode);

    /**
     * 构造函数：使用可变参数列表。
     * 初始化表达式的深度、宽度、快速哈希码等属性，并检查表达式树的限制。
     *
     * @param children 子表达式数组
     */
    protected Expression(Expression... children) {
        super(children);

        boolean hasUnbound = false;
        switch (children.length) {
            case 0:
                this.depth = 1;
                this.width = 1;
                this.compareWidthAndDepth = supportCompareWidthAndDepth();
                this.fastChildrenHashCode = 0;
                break;
            case 1:
                Expression child = children[0];
                this.depth = child.depth + 1;
                this.width = child.width;
                this.compareWidthAndDepth = child.compareWidthAndDepth && supportCompareWidthAndDepth();
                this.fastChildrenHashCode = child.fastChildrenHashCode() + 1;
                break;
            case 2:
                Expression left = children[0];
                Expression right = children[1];
                this.depth = Math.max(left.depth, right.depth) + 1;
                this.width = left.width + right.width;
                this.compareWidthAndDepth =
                        left.compareWidthAndDepth && right.compareWidthAndDepth && supportCompareWidthAndDepth();
                this.fastChildrenHashCode = left.fastChildrenHashCode() + right.fastChildrenHashCode() + 2;
                break;
            default:
                int maxChildDepth = 0;
                int sumChildWidth = 0;
                boolean compareWidthAndDepth = true;
                int fastChildrenHashCode = 0;
                for (Expression expression : children) {
                    child = expression;
                    maxChildDepth = Math.max(child.depth, maxChildDepth);
                    sumChildWidth += child.width;
                    hasUnbound |= child.hasUnbound;
                    compareWidthAndDepth &= child.compareWidthAndDepth;
                    fastChildrenHashCode = fastChildrenHashCode + expression.fastChildrenHashCode() + 1;
                }
                this.depth = maxChildDepth + 1;
                this.width = sumChildWidth;
                this.compareWidthAndDepth = compareWidthAndDepth;
                this.fastChildrenHashCode = fastChildrenHashCode;
        }
        checkLimit();
        this.inferred = false;
        this.hasUnbound = hasUnbound || this instanceof Unbound;
    }

    /**
     * 构造函数：使用列表参数。
     * 便捷构造函数，内部调用三参数构造函数，inferred 默认为 false。
     *
     * @param children 子表达式列表
     */
    protected Expression(List<Expression> children) {
        this(children, false);
    }

    /**
     * 构造函数：使用列表参数和推断标志。
     * 初始化表达式的深度、宽度、快速哈希码等属性，并检查表达式树的限制。
     *
     * @param children 子表达式列表
     * @param inferred 是否由推断生成
     */
    protected Expression(List<Expression> children, boolean inferred) {
        super(children);

        boolean hasUnbound = false;
        switch (children.size()) {
            case 0:
                this.depth = 1;
                this.width = 1;
                this.compareWidthAndDepth = supportCompareWidthAndDepth();
                this.fastChildrenHashCode = 0;
                break;
            case 1:
                Expression child = children.get(0);
                this.depth = child.depth + 1;
                this.width = child.width;
                this.compareWidthAndDepth = child.compareWidthAndDepth && supportCompareWidthAndDepth();
                this.fastChildrenHashCode = child.fastChildrenHashCode() + 1;
                break;
            case 2:
                Expression left = children.get(0);
                Expression right = children.get(1);
                this.depth = Math.max(left.depth, right.depth) + 1;
                this.width = left.width + right.width;
                this.compareWidthAndDepth =
                        left.compareWidthAndDepth && right.compareWidthAndDepth && supportCompareWidthAndDepth();
                this.fastChildrenHashCode = left.fastChildrenHashCode() + right.fastChildrenHashCode() + 2;
                break;
            default:
                int maxChildDepth = 0;
                int sumChildWidth = 0;
                boolean compareWidthAndDepth = true;
                int fastChildrenhashCode = 0;
                for (Expression expression : children) {
                    child = expression;
                    maxChildDepth = Math.max(child.depth, maxChildDepth);
                    sumChildWidth += child.width;
                    hasUnbound |= child.hasUnbound;
                    compareWidthAndDepth &= child.compareWidthAndDepth;
                    fastChildrenhashCode = fastChildrenhashCode + expression.fastChildrenHashCode() + 1;
                }
                this.depth = maxChildDepth + 1;
                this.width = sumChildWidth;
                this.compareWidthAndDepth = compareWidthAndDepth && supportCompareWidthAndDepth();
                this.fastChildrenHashCode = fastChildrenhashCode;
        }
        checkLimit();
        this.inferred = inferred;
        this.hasUnbound = hasUnbound || this instanceof Unbound;
    }

    /**
     * 检查表达式树的限制。
     * 验证表达式的深度和宽度是否超过配置的最大值，如果超过则抛出异常。
     */
    private void checkLimit() {
        if (depth > Config.expr_depth_limit) {
            throw new AnalysisException(ErrorCode.EXPRESSION_EXCEEDS_LIMIT,
                    String.format("Exceeded the maximum depth of an expression tree (%s).", Config.expr_depth_limit));
        }
        if (width > Config.expr_children_limit) {
            throw new AnalysisException(ErrorCode.EXPRESSION_EXCEEDS_LIMIT,
                    String.format("Exceeded the maximum children of an expression tree (%s).",
                            Config.expr_children_limit));
        }
    }

    /**
     * 为表达式创建别名。
     *
     * @param alias 别名
     * @return 带别名的表达式
     */
    public Alias alias(String alias) {
        return new Alias(this, alias);
    }

    /**
     * 获取表达式名称。
     * 用于在没有别名时自动生成列名。如果名称尚未计算，则根据类名生成规范化名称。
     *
     * @return 表达式名称
     */
    public String getExpressionName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(this.getClass().getSimpleName(), DEFAULT_EXPRESSION_NAME));
        }
        return this.exprName.get();
    }

    /**
     * 检查输入数据类型。
     * 递归检查所有子表达式的类型，如果表达式实现了 ExpectsInputTypes，则检查输入类型是否符合预期。
     *
     * @return 类型检查结果
     */
    public TypeCheckResult checkInputDataTypes() {
        // check all of its children recursively.
        for (Expression child : this.children) {
            TypeCheckResult childResult = child.checkInputDataTypes();
            if (childResult.failed()) {
                return childResult;
            }
        }
        if (this instanceof ExpectsInputTypes) {
            ExpectsInputTypes expectsInputTypes = (ExpectsInputTypes) this;
            TypeCheckResult commonCheckResult = checkInputDataTypesWithExpectTypes(
                    children, expectsInputTypes.expectedInputTypes());
            if (commonCheckResult.failed()) {
                return commonCheckResult;
            }
        }
        return checkInputDataTypesInternal();
    }

    /**
     * 获取快速子节点哈希码。
     * 用于优化 equals 和 hashCode 计算，避免递归遍历整个表达式树。
     *
     * @return 快速子节点哈希码
     */
    public int fastChildrenHashCode() {
        return fastChildrenHashCode;
    }

    /**
     * 计算 SQL 字符串。
     * 子类应该重写此方法以提供实际的 SQL 生成逻辑。
     * 默认实现抛出 UnboundException。
     *
     * @return SQL 字符串
     * @throws UnboundException 如果表达式尚未绑定
     */
    protected String computeToSql() {
        throw new UnboundException("sql");
    }

    /**
     * 检查输入数据类型的内部实现。
     * 子类可以重写此方法以提供自定义的类型检查逻辑。
     * 默认返回成功。
     *
     * @return 类型检查结果
     */
    protected TypeCheckResult checkInputDataTypesInternal() {
        return TypeCheckResult.SUCCESS;
    }

    private boolean checkInputDataTypesWithExpectType(DataType input, DataType expected) {
        if (input instanceof ArrayType && expected instanceof ArrayType) {
            return checkInputDataTypesWithExpectType(
                    ((ArrayType) input).getItemType(), ((ArrayType) expected).getItemType());
        } else if (input instanceof MapType && expected instanceof MapType) {
            return checkInputDataTypesWithExpectType(
                    ((MapType) input).getKeyType(), ((MapType) expected).getKeyType())
                    && checkInputDataTypesWithExpectType(
                    ((MapType) input).getValueType(), ((MapType) expected).getValueType());
        } else if (input instanceof StructType && expected instanceof StructType) {
            List<StructField> inputFields = ((StructType) input).getFields();
            List<StructField> expectedFields = ((StructType) expected).getFields();
            if (inputFields.size() != expectedFields.size()) {
                return false;
            }
            for (int i = 0; i < inputFields.size(); i++) {
                if (!checkInputDataTypesWithExpectType(
                        inputFields.get(i).getDataType(),
                        expectedFields.get(i).getDataType())) {
                    return false;
                }
            }
            return true;
        } else {
            return checkPrimitiveInputDataTypesWithExpectType(input, expected);
        }
    }

    /**
     * 检查基本数据类型是否匹配预期类型。
     * 支持快速检查，例如：input=TinyIntType, expected=NumericType（如 `1 + 1`）。
     * 如果没有此检查，在调用 NumericType.toCatalogDataType 时会抛出异常，
     * 当表达式很多时，异常会成为瓶颈，因为异常需要记录整个堆栈帧。
     *
     * @param input 输入数据类型
     * @param expected 预期数据类型
     * @return true 如果类型匹配，false 否则
     */
    private boolean checkPrimitiveInputDataTypesWithExpectType(DataType input, DataType expected) {
        // 快速检查：如果预期类型接受输入类型，直接返回 true
        if (expected.acceptsType(input)) {
            return true;
        }

        // TODO: 完善类型转换逻辑，类似 FunctionCallExpr.analyzeImpl
        try {
            return input.toCatalogDataType().matchesType(expected.toCatalogDataType());
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * 检查多个输入表达式的类型是否匹配预期类型列表。
     *
     * @param inputs 输入表达式列表
     * @param expectedTypes 预期类型列表
     * @return 类型检查结果，包含错误信息（如果有）
     */
    private TypeCheckResult checkInputDataTypesWithExpectTypes(
            List<Expression> inputs, List<DataType> expectedTypes) {
        Preconditions.checkArgument(inputs.size() == expectedTypes.size());
        List<String> errorMessages = Lists.newArrayList();
        for (int i = 0; i < inputs.size(); i++) {
            Expression input = inputs.get(i);
            DataType expected = expectedTypes.get(i);
            if (!checkInputDataTypesWithExpectType(input.getDataType(), expected)) {
                errorMessages.add(String.format("argument %d requires %s type, however '%s' is of %s type",
                        i + 1, expected.simpleString(), input.toSql(), input.getDataType().simpleString()));
            }
        }
        if (!errorMessages.isEmpty()) {
            return new TypeCheckResult(false, StringUtils.join(errorMessages, ", "));
        }
        return TypeCheckResult.SUCCESS;
    }

    /**
     * 接受访问者模式的访问。
     * 子类必须实现此方法以支持访问者模式。
     *
     * @param visitor 表达式访问者
     * @param context 访问者上下文
     * @param <R> 访问者的返回类型
     * @param <C> 上下文类型
     * @return 访问此表达式的结果
     */
    public abstract <R, C> R accept(ExpressionVisitor<R, C> visitor, C context);

    /**
     * 获取所有子表达式。
     *
     * @return 子表达式列表
     */
    @Override
    public List<Expression> children() {
        return children;
    }

    /**
     * 获取指定索引位置的子表达式。
     *
     * @param index 子表达式的索引位置（从 0 开始）
     * @return 指定索引位置的子表达式
     */
    @Override
    public Expression child(int index) {
        return children.get(index);
    }

    /**
     * 获取表达式树的宽度（所有子表达式的宽度之和）。
     *
     * @return 表达式树的宽度
     */
    public int getWidth() {
        return width;
    }

    /**
     * 获取表达式树的深度（从根到最深叶节点的路径长度）。
     *
     * @return 表达式树的深度
     */
    public int getDepth() {
        return depth;
    }

    /**
     * 判断表达式是否由推断生成。
     *
     * @return true 如果表达式由推断生成，false 否则
     */
    public boolean isInferred() {
        return inferred;
    }

    /**
     * 将表达式转换为 SQL 字符串。
     * 使用懒加载缓存，首次调用时计算并缓存结果。
     *
     * @return 表达式的 SQL 字符串表示
     */
    public final String toSql() {
        return toSqlCache.get();
    }

    /**
     * 使用新的子表达式创建新的表达式实例。
     * 子类应该重写此方法以提供实际的实现。
     *
     * @param children 新的子表达式列表
     * @return 新的表达式实例
     */
    @Override
    public Expression withChildren(List<Expression> children) {
        throw new RuntimeException();
    }

    /**
     * 使用新的推断标志创建新的表达式实例。
     * 子类应该重写此方法以提供实际的实现。
     *
     * @param inferred 新的推断标志
     * @return 新的表达式实例
     */
    public Expression withInferred(boolean inferred) {
        throw new RuntimeException("current expression has not impl the withInferred method");
    }

    /**
     * 判断表达式是否为常量。
     * 常量表达式是指在编译时就可以确定值的表达式。
     * 聚合函数、Lambda、子查询、变量等不是常量。
     * 对于叶子表达式，只有字面量是常量；对于非叶子表达式，需要是确定性的且所有子表达式都是常量。
     *
     * @return true 如果表达式是常量，false 否则
     */
    public boolean isConstant() {
        if (this instanceof AggregateFunction
                || this instanceof Lambda
                || this instanceof MaxValue
                || this instanceof OrderExpression
                || this instanceof Properties
                || this instanceof SubqueryExpr
                || this instanceof UnboundVariable
                || this instanceof Variable
                || this instanceof VariableDesc
                || this instanceof WindowExpression
                || this instanceof WindowFrame) {
            // agg_fun(literal) is not constant, the result depends on the group by keys
            return false;
        }
        if (this instanceof LeafExpression) {
            return this instanceof Literal;
        } else {
            return this.isDeterministic() && ExpressionUtils.allMatch(children(), Expression::isConstant);
        }
    }

    /**
     * 判断表达式是否为零值字面量。
     *
     * @return true 如果表达式是零值字面量，false 否则
     */
    public boolean isZeroLiteral() {
        return this instanceof Literal && ((Literal) this).isZero();
    }

    /**
     * 将表达式转换为目标类型。
     * 执行类型转换，如果转换失败会抛出异常。
     *
     * @param targetType 目标数据类型
     * @return 转换后的表达式
     * @throws AnalysisException 如果类型转换失败
     */
    public final Expression castTo(DataType targetType) throws AnalysisException {
        return uncheckedCastTo(targetType);
    }

    /**
     * 将表达式转换为目标类型（带检查）。
     * 执行类型转换，如果转换失败会抛出异常。
     *
     * @param targetType 目标数据类型
     * @return 转换后的表达式
     * @throws AnalysisException 如果类型转换失败
     */
    public Expression checkedCastTo(DataType targetType) throws AnalysisException {
        return castTo(targetType);
    }

    /**
     * 将表达式转换为目标类型（不检查）。
     * 子类应该重写此方法以提供实际的类型转换逻辑。
     *
     * @param targetType 目标数据类型
     * @return 转换后的表达式
     * @throws AnalysisException 如果类型转换失败
     */
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        throw new RuntimeException("Do not implement uncheckedCastTo");
    }

    /**
     * 获取表达式中所有的输入槽（列引用）。
     * 注意：子查询内部计划的输入槽不包含在内。
     * 使用懒加载缓存，首次调用时计算并缓存结果。
     *
     * @return 输入槽的集合
     */
    public final Set<Slot> getInputSlots() {
        return inputSlots.get();
    }

    /**
     * 获取表达式中所有输入槽的 ID。
     * 注意：子查询内部计划的输入槽不包含在内。
     *
     * @return 输入槽 ID 的集合
     */
    public final Set<ExprId> getInputSlotExprIds() {
        Set<Slot> inputSlots = getInputSlots();
        Builder<ExprId> exprIds = ImmutableSet.builderWithExpectedSize(inputSlots.size());
        for (Slot inputSlot : inputSlots) {
            exprIds.add(inputSlot.getExprId());
        }
        return exprIds.build();
    }

    /**
     * 判断表达式是否为字面量。
     *
     * @return true 如果表达式是字面量，false 否则
     */
    public boolean isLiteral() {
        return this instanceof Literal;
    }

    /**
     * 判断表达式是否为 NULL 字面量。
     *
     * @return true 如果表达式是 NULL 字面量，false 否则
     */
    public boolean isNullLiteral() {
        return this instanceof NullLiteral;
    }

    /**
     * 判断表达式是否为槽（列引用）。
     *
     * @return true 如果表达式是槽，false 否则
     */
    public boolean isSlot() {
        return this instanceof Slot;
    }

    /**
     * 判断表达式是否为来自表的列。
     * 检查是否为 SlotReference 且具有原始列信息。
     *
     * @return true 如果表达式是来自表的列，false 否则
     */
    public boolean isColumnFromTable() {
        return (this instanceof SlotReference) && ((SlotReference) this).getOriginalColumn().isPresent();
    }

    /**
     * 判断表达式是否为来自表的主键列。
     * 检查是否为 SlotReference、具有原始列信息且该列是主键。
     *
     * @return true 如果表达式是来自表的主键列，false 否则
     */
    public boolean isKeyColumnFromTable() {
        return (this instanceof SlotReference) && ((SlotReference) this).getOriginalColumn().isPresent()
                && ((SlotReference) this).getOriginalColumn().get().isKey();
    }

    /**
     * 判断表达式是否包含唯一函数（如 rand(), uuid() 等）。
     *
     * @return true 如果表达式包含唯一函数，false 否则
     */
    public boolean containsUniqueFunction() {
        return containsType(UniqueFunction.class);
    }

    /**
     * 判断表达式是否包含 NULL 字面量子表达式。
     * 使用可变状态缓存结果，避免重复计算。
     *
     * @return true 如果表达式包含 NULL 字面量子表达式，false 否则
     */
    public boolean containsNullLiteralChildren() {
        return getOrInitMutableState("CONTAINS_NULL_LITERAL_CHILDREN", () -> {
            for (Expression child : children) {
                if (child instanceof NullLiteral) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * 判断表达式的所有子表达式是否都是字面量。
     * 使用可变状态缓存结果，避免重复计算。
     *
     * @return true 如果所有子表达式都是字面量，false 否则
     */
    public boolean allChildrenAreLiteral() {
        return getOrInitMutableState("ALL_CHILDREN_ARE_LITERAL", () -> {
            boolean allLiteral = true;
            for (Expression child : getArguments()) {
                if (!(child instanceof Literal)) {
                    allLiteral = false;
                    break;
                }
            }
            return allLiteral;
        });
    }

    /**
     * 判断两个表达式是否相等。
     * 使用快速路径优化：先比较宽度、深度和快速哈希码，如果不同则直接返回 false。
     * 然后比较子节点数量和额外相等性，最后递归比较所有子节点。
     *
     * @param o 要比较的对象
     * @return true 如果表达式相等，false 否则
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Expression that = (Expression) o;
        if ((compareWidthAndDepth
                && (this.width != that.width || this.depth != that.depth
                    || this.fastChildrenHashCode != that.fastChildrenHashCode))
                || arity() != that.arity() || !extraEquals(that)) {
            return false;
        }
        return equalsChildren(that);
    }

    /**
     * 比较两个表达式的子节点是否相等。
     * 递归比较所有子节点。
     *
     * @param that 要比较的表达式
     * @return true 如果所有子节点都相等，false 否则
     */
    protected boolean equalsChildren(Expression that) {
        List<Expression> children = children();
        List<Expression> thatChildren = that.children();
        for (int i = 0; i < children.size(); i++) {
            if (!children.get(i).equals(thatChildren.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 额外的相等性检查。
     * 子类可以重写此方法以提供额外的相等性判断逻辑。
     *
     * @param that 要比较的表达式
     * @return true 如果额外条件满足，false 否则
     */
    protected boolean extraEquals(Expression that) {
        return true;
    }

    /**
     * 计算表达式的哈希码。
     * 使用懒加载缓存，首次调用时计算并缓存结果。
     *
     * @return 表达式的哈希码
     */
    @Override
    public int hashCode() {
        return hashCodeCache.get();
    }

    /**
     * 计算表达式的哈希码。
     * 基于类名和快速子节点哈希码计算。
     * 子类可以重写此方法以提供自定义的哈希码计算逻辑。
     *
     * @return 表达式的哈希码
     */
    protected int computeHashCode() {
        return getClass().hashCode() + fastChildrenHashCode();
    }

    /**
     * 判断表达式是否包含未绑定的符号。
     * 未绑定的符号是指在语义分析阶段尚未解析的标识符。
     *
     * @return true 如果表达式包含未绑定的符号，false 否则
     */
    public boolean hasUnbound() {
        return this.hasUnbound;
    }

    /**
     * 获取表达式的形状信息。
     * 返回表达式的 SQL 字符串表示，用于调试和可视化。
     *
     * @return 表达式的形状信息（SQL 字符串）
     */
    public String shapeInfo() {
        return toSql();
    }

    /**
     * 判断是否支持通过宽度和深度进行快速比较。
     * 子类可以重写此方法以禁用快速比较优化。
     *
     * @return true 如果支持快速比较，false 否则
     */
    protected boolean supportCompareWidthAndDepth() {
        return true;
    }

    /**
     * 获取表达式的指纹（fingerprint）。
     * 用于唯一标识表达式，目前未实现。
     *
     * @return 表达式的指纹
     */
    public String getFingerprint() {
        return "NOT_IMPLEMENTED_EXPR_FP";
    }
}
