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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.List;

/**
 * 标量函数（Scalar Function）的抽象基类。
 * <p>
 * 标量函数逐行读取零个或多个参数，并为每一行返回一个结果值，
 * 与需要跨多行聚合的聚合函数不同，标量函数仅关注当前行。
 * <p>
 * 典型的标量函数示例：
 * <ul>
 *   <li>数学函数：abs、sqrt、sin、cos 等</li>
 *   <li>字符串函数：concat、substring、length 等</li>
 *   <li>日期时间函数：year、month、day 等</li>
 *   <li>类型转换函数：cast、convert 等</li>
 *   <li>JSON 函数：json_extract、json_parse 等</li>
 * </ul>
 * <p>
 * 该类继承自 {@link BoundFunction} 并实现 {@link ComputeSignature}，
 * 是 Nereids 中所有标量函数实现的基础。
 */
public abstract class ScalarFunction extends BoundFunction implements ComputeSignature {
    /**
     * 以函数名称和可变参数构造标量函数。
     *
     * @param name 函数名称（例如 "abs"、"concat"、"json_extract"）
     * @param arguments 函数参数列表，可为空或包含多个表达式
     */
    public ScalarFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    /**
     * 用于 withChildren 逻辑的构造函数。
     * <p>
     * 在生成一个仅子节点发生变化的新函数实例时，
     * 会复用原函数的元信息并替换子表达式，该构造函数即负责承载该流程。
     *
     * @param functionParams 函数参数封装，包含当前函数引用、名称、参数以及是否为推断签名
     */
    public ScalarFunction(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * 以函数名和参数列表构造标量函数。
     * <p>
     * 该构造函数会默认传入 {@code inferred = false}，是三参构造的便捷封装。
     *
     * @param name 函数名称
     * @param arguments 函数参数列表
     */
    public ScalarFunction(String name, List<Expression> arguments) {
        this(name, arguments, false);
    }

    /**
     * 以函数名、参数列表与推断标记构造标量函数。
     *
     * @param name 函数名称
     * @param arguments 函数参数列表
     * @param inferred 是否由参数推断函数签名，true 表示推断，false 表示显式指定
     */
    public ScalarFunction(String name, List<Expression> arguments, boolean inferred) {
        super(name, arguments, inferred);
    }

    /**
     * 接收表达式访问者（Visitor 模式）。
     * <p>
     * 访问者模式广泛应用于 Nereids 的表达式分析、优化与代码生成流程，
     * 通过该方法可以在表达式树中遍历并处理标量函数节点。
     *
     * @param visitor 表达式访问者
     * @param context 访问上下文
     * @param <R> 访问结果类型
     * @param <C> 上下文类型
     * @return 访问该标量函数后的处理结果
     */
    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }

    /**
     * 为 withChildren 方法构造新的参数封装。
     * <p>
     * 当父类需要基于新的子表达式生成函数实例时，会调用该方法，
     * 将当前函数的元信息与新的参数列表打包成 {@link ScalarFunctionParams} 对象。
     *
     * @param arguments 新的函数参数列表
     * @return 携带函数引用、名称、参数以及推断标记的参数对象
     */
    @Override
    protected ScalarFunctionParams getFunctionParams(List<Expression> arguments) {
        return new ScalarFunctionParams(this, getName(), arguments, isInferred());
    }
}
