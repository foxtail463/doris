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

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList; 

import java.util.List;

/**
 * 表达式特征接口。
 * 定义了表达式的基本特征和行为，包括可空性、类型信息、可折叠性、确定性等。
 * 继承自 TreeNode，使表达式成为树形结构中的节点。
 */
public interface ExpressionTrait extends TreeNode<Expression> {

    /**
     * 判断表达式的结果是否可能为 NULL。
     * 用于类型系统、优化器和执行器的 NULL 值处理。
     *
     * @return true 如果表达式结果可能为 NULL，false 如果表达式结果不会为 NULL
     */
    boolean nullable();

    /**
     * 判断表达式的结果是否不会为 NULL。
     * nullable() 的反向方法。
     *
     * @return true 如果表达式结果不会为 NULL，false 如果表达式结果可能为 NULL
     */
    default boolean notNullable() {
        return !nullable();
    }

    /**
     * 在类型转换之前检查表达式的合法性。
     * 检查参数类型兼容性、参数数量、特殊约束等。
     * 注意：此方法正在开发中，未来可能与 checkInputDataTypes 合并。
     */
    @Developing
    default void checkLegalityBeforeTypeCoercion() {}

    /**
     * 在表达式重写之后检查表达式的合法性。
     * 确保重写后的表达式仍然满足语义要求。
     * 注意：此方法正在开发中。
     */
    @Developing
    default void checkLegalityAfterRewrite() {}

    /**
     * 获取表达式的所有参数（子表达式）。
     * 对于函数表达式，参数就是函数的输入参数；对于其他表达式，参数就是子表达式。
     *
     * @return 表达式的参数列表
     */
    default List<Expression> getArguments() {
        return children();
    }

    /**
     * 获取指定索引位置的参数（子表达式）。
     *
     * @param index 参数的索引位置（从 0 开始）
     * @return 指定索引位置的参数表达式
     */
    default Expression getArgument(int index) {
        return child(index);
    }

    /**
     * 获取所有参数的数据类型列表。
     * 用于类型推导、类型检查和函数签名匹配。
     *
     * @return 参数数据类型的不可变列表
     */
    default List<DataType> getArgumentsTypes() {
        return getArguments()
                .stream()
                .map(Expression::getDataType)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * 获取指定索引位置参数的数据类型。
     *
     * @param index 参数的索引位置（从 0 开始）
     * @return 指定参数的数据类型
     */
    default DataType getArgumentType(int index) {
        return child(index).getDataType();
    }

    /**
     * 获取表达式的数据类型。
     * 用于类型检查、类型推导和执行计划生成。
     * 默认实现抛出 UnboundException，具体表达式类应该重写此方法。
     *
     * @return 表达式的数据类型
     * @throws UnboundException 如果表达式尚未绑定类型
     */
    default DataType getDataType() throws UnboundException {
        throw new UnboundException(toSql() + ".getDataType()");
    }

    /**
     * 将表达式转换为 SQL 字符串。
     * 用于查询计划可视化、错误消息显示、调试和日志记录。
     * 默认实现抛出 UnboundException，具体表达式类应该重写此方法。
     *
     * @return 表达式的 SQL 字符串表示
     * @throws UnboundException 如果表达式尚未绑定
     */
    default String toSql() throws UnboundException {
        throw new UnboundException("sql");
    }

    /**
     * 判断表达式是否可折叠（foldable）。
     * 可折叠表达式可以在编译时计算出结果，进行常量折叠优化。
     * 例如：1 + 2 可以折叠为 3。
     * UDF 和 UniqueFunction（如 rand(), uuid()）通常不可折叠。
     * 注意：检查非幂等函数应使用 Expression::containsUniqueFunction()。
     * 默认返回 true，具体表达式类应该重写此方法。
     *
     * @return true 如果表达式可折叠，false 如果表达式不可折叠
     */
    default boolean foldable() {
        return true;
    }

    /**
     * 判断表达式是否具有确定性（deterministic）。
     * 确定性表达式对于相同输入总是产生相同输出。
     * 例如：1 + 2（确定性）、col1 + col2（确定性）、rand()（非确定性）、now()（非确定性）。
     * 用于查询优化、缓存策略和执行计划生成。
     * 默认返回 true，具体表达式类应该重写此方法。
     *
     * @return true 如果表达式具有确定性，false 如果表达式不具有确定性
     */
    default boolean isDeterministic() {
        return true;
    }

    /**
     * 判断表达式是否包含非确定性子表达式。
     * 递归检查所有子表达式，例如：col1 + rand() 包含非确定性表达式 rand()。
     * 用于查询优化和执行计划生成。
     *
     * @return true 如果表达式包含非确定性子表达式，false 如果所有子表达式都是确定性的
     */
    default boolean containsNondeterministic() {
        return anyMatch(expr -> !((ExpressionTrait) expr).isDeterministic());
    }
}
