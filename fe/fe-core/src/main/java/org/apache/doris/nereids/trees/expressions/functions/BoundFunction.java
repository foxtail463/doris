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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.LazyCompute;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 已绑定函数（Bound Function）的基类。
 * <p>
 * 在 Nereids 优化器中，函数解析分为两个阶段：
 * <ul>
 *   <li><b>UnboundFunction（未绑定函数）</b>：解析阶段，只有函数名和参数表达式，
 *       还没有确定具体的函数实现和类型签名</li>
 *   <li><b>BoundFunction（已绑定函数）</b>：语义分析阶段，已经确定了具体的函数实现类
 *       和完整的函数签名（包括参数类型和返回类型）</li>
 * </ul>
 * <p>
 * BoundFunction 的主要特性：
 * <ul>
 *   <li>实现了 {@link ComputeSignature} 接口，可以根据参数类型计算并确定函数签名</li>
 *   <li>使用懒加载机制缓存函数签名，避免重复计算</li>
 *   <li>支持函数签名的重用，在修改子表达式时保持签名一致性</li>
 * </ul>
 * <p>
 * 函数签名计算流程：
 * <ol>
 *   <li>在候选签名列表（SIGNATURES）中根据实参类型匹配"粗签名"</li>
 *   <li>对匹配的签名进行加工，例如 Decimal/Datetime 类型的精度补全</li>
 *   <li>将计算好的签名缓存起来，后续直接使用</li>
 * </ol>
 * <p>
 * 所有具体的函数实现类（如 {@link org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction}、
 * {@link org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction} 等）
 * 都继承自 BoundFunction。
 */
public abstract class BoundFunction extends Function implements ComputeSignature {
    /** 函数签名的懒加载缓存，避免重复计算 */
    private final Supplier<FunctionSignature> signatureCache;

    /**
     * 构造函数：使用函数名和可变参数列表。
     *
     * @param name 函数名（如 "abs", "sum", "json_extract"）
     * @param arguments 函数的参数表达式（可变参数）
     */
    public BoundFunction(String name, Expression... arguments) {
        super(name, arguments);
        this.signatureCache = buildSignatureCache(null);
    }

    /**
     * 构造函数：使用函数名和参数列表。
     * <p>
     * 这是一个便捷构造函数，内部调用三参数构造函数，并将 {@code inferred} 设置为 false。
     *
     * @param name 函数名
     * @param children 函数的参数表达式列表
     */
    public BoundFunction(String name, List<Expression> children) {
        this(name, children, false);
    }

    /**
     * 构造函数：使用函数名、参数列表和推断标志。
     *
     * @param name 函数名
     * @param children 函数的参数表达式列表
     * @param inferred 是否从参数推断签名（true 表示推断，false 表示显式指定）
     */
    public BoundFunction(String name, List<Expression> children, boolean inferred) {
        super(name, children, inferred);
        this.signatureCache = buildSignatureCache(null);
    }

    /**
     * 构造函数：用于 withChildren 方法和签名重用。
     * <p>
     * 当需要创建新的函数实例并修改子表达式时，此构造函数可以重用原有的函数签名，
     * 保证签名计算的幂等性和一致性。这通常由 {@link #withChildren(List)} 方法调用。
     *
     * @param functionParams 函数参数对象，包含函数引用、函数名、参数列表和推断标志
     */
    public BoundFunction(FunctionParams functionParams) {
        super(functionParams.functionName, functionParams.arguments, functionParams.inferred);
        this.signatureCache = buildSignatureCache(functionParams.getOriginSignature());
    }

    /**
     * 获取表达式的规范化名称。
     * <p>
     * 表达式名称用于表达式树的序列化和比较。如果名称尚未计算，则根据函数名生成规范化名称。
     *
     * @return 表达式的规范化名称
     */
    @Override
    public String getExpressionName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(getName(), DEFAULT_EXPRESSION_NAME));
        }
        return this.exprName.get();
    }

    /**
     * 获取函数的签名。
     * <p>
     * 函数签名包含参数类型和返回类型信息。签名通过懒加载机制计算并缓存，
     * 首次调用时会触发签名计算，后续调用直接返回缓存结果。
     *
     * @return 函数的签名对象
     */
    public FunctionSignature getSignature() {
        return signatureCache.get();
    }

    /**
     * 接受访问者模式的访问。
     * <p>
     * 允许表达式访问者遍历和处理已绑定函数节点。访问者模式在 Nereids 中广泛用于
     * 表达式分析、优化和代码生成。
     *
     * @param visitor 表达式访问者
     * @param context 访问者上下文
     * @param <R> 访问者的返回类型
     * @param <C> 上下文类型
     * @return 访问此已绑定函数的结果
     */
    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBoundFunction(this, context);
    }

    @Override
    protected boolean extraEquals(Expression that) {
        return Objects.equals(getName(), ((BoundFunction) that).getName());
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(getName(), children);
    }

    @Override
    public String computeToSql() throws UnboundException {
        StringBuilder sql = new StringBuilder(getName()).append("(");
        int arity = arity();
        for (int i = 0; i < arity; i++) {
            Expression arg = child(i);
            sql.append(arg.toSql());
            if (i + 1 < arity) {
                sql.append(", ");
            }
        }
        return sql.append(")").toString();
    }

    @Override
    public String toString() {
        String args = children()
                .stream()
                .map(Expression::toString)
                .collect(Collectors.joining(", "));
        return getName() + "(" + args + ")";
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append(getName().toUpperCase());
        sb.append(
                children().stream().map(Expression::toDigest)
                        .collect(Collectors.joining(", ", "(", ")"))
        );
        return sb.toString();
    }

    /**
     * 使用新的子表达式创建新的函数实例。
     * <p>
     * 此方法在 BoundFunction 中不支持直接调用，子类应该通过 FunctionParams 构造函数
     * 来实现 withChildren 方法，以保证函数签名的正确重用。
     *
     * @param children 新的子表达式列表
     * @return 不支持的操作异常
     * @throws UnsupportedOperationException 总是抛出此异常，提示使用 FunctionParams 方式
     */
    @Override
    public Expression withChildren(List<Expression> children) {
        throw new UnsupportedOperationException(
                "Please implement withChildren by create new function with FunctionParams");
    }

    /**
     * 创建函数参数对象。
     * <p>
     * 此方法由父类在创建新函数实例时调用，用于将当前函数的元数据（函数引用、名称、参数、推断标志）
     * 打包成 FunctionParams 对象，供构造函数使用。
     *
     * @param arguments 新的参数表达式列表
     * @return 包含函数元数据的 FunctionParams 对象
     */
    protected FunctionParams getFunctionParams(List<Expression> arguments) {
        return new FunctionParams(this, getName(), arguments, isInferred());
    }

    /**
     * 检查函数是否支持 ORDER BY 表达式。
     * <p>
     * 只有特定的聚合函数（如 GroupConcat、MultiDistinctGroupConcat）支持 ORDER BY 子句。
     * 如果函数参数中包含 OrderExpression，但函数本身不支持，则抛出异常。
     *
     * @throws AnalysisException 如果函数不支持 ORDER BY 表达式但参数中包含 OrderExpression
     */
    public void checkOrderExprIsValid() {
        for (Expression child : children) {
            if (child instanceof OrderExpression
                    && !(this instanceof GroupConcat || this instanceof MultiDistinctGroupConcat)) {
                throw new AnalysisException(
                        String.format("%s doesn't support order by expression", getName()));
            }
        }
    }

    /**
     * 构建函数签名的懒加载缓存。
     * <p>
     * 函数签名计算分为两个步骤：
     * <ol>
     *   <li><b>匹配粗签名</b>：在候选签名列表（SIGNATURES）中根据实参类型匹配最合适的签名</li>
     *   <li><b>加工签名</b>：对匹配的签名进行进一步处理，例如：
     *       <ul>
     *         <li>Decimal 类型的精度补全（如 sum 函数调用 ComputePrecisionForSum）</li>
     *         <li>Datetime 类型的精度处理</li>
     *         <li>其他类型相关的调整</li>
     *       </ul>
     *   </li>
     * </ol>
     * <p>
     * 如果外部指定了签名（如重用原签名），则直接使用指定的签名，保证签名计算的幂等性。
     *
     * @param specifiedSignature 外部指定的签名（可选，用于签名重用）
     * @return 函数签名的懒加载 Supplier，首次调用时计算并缓存结果
     */
    private Supplier<FunctionSignature> buildSignatureCache(Supplier<FunctionSignature> specifiedSignature) {
        if (specifiedSignature != null) {
            // 若外部指定了签名（如重用原签名），直接使用，保证签名计算的幂等
            return specifiedSignature;
        } else {
            return LazyCompute.of(() -> {
                // 第一步：在 SIGNATURES 列表中根据实参类型选择"粗签名"
                FunctionSignature matchedSignature = searchSignature(getSignatures());
                // 第二步：对签名进行加工，例如 Decimal/Datetime 精度补全（sum 在此调用 ComputePrecisionForSum）
                return computeSignature(matchedSignature);
            });
        }
    }
}
