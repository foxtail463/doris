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

/**
 * 函数特征接口（Function Trait）。
 * <p>
 * 此接口定义了函数的基本特征，用于标识和描述函数的行为特性。
 * 它继承自 {@link ExpressionTrait}，提供了表达式相关的通用功能，同时增加了函数特有的特征。
 * <p>
 * <b>设计目的：</b>
 * <ul>
 *   <li>提供函数的基本标识信息（函数名）</li>
 *   <li>标识函数是否支持可变参数（variadic arguments）</li>
 *   <li>作为函数类型系统的标记接口，用于类型检查和模式匹配</li>
 * </ul>
 * <p>
 * <b>实现关系：</b>
 * <ul>
 *   <li>{@link org.apache.doris.nereids.trees.expressions.functions.Function} 类及其子类
 *       （如 {@link org.apache.doris.nereids.trees.expressions.functions.BoundFunction}）
 *       通过实现此接口来提供函数特征</li>
 *   <li>{@link org.apache.doris.nereids.trees.expressions.functions.ComputeSignature} 接口
 *       提供了 {@link #hasVarArguments()} 的默认实现，基于函数签名判断</li>
 * </ul>
 * <p>
 * <b>使用场景：</b>
 * <ul>
 *   <li>函数解析和类型推导时检查函数特征</li>
 *   <li>函数重载匹配时判断参数数量是否可变</li>
 *   <li>表达式优化和重写时识别函数类型</li>
 * </ul>
 * <p>
 * <b>可变参数函数示例：</b>
 * <ul>
 *   <li>{@code concat(str1, str2, ...)} - 可以接受任意多个字符串参数</li>
 *   <li>{@code json_extract(json, path1, path2, ...)} - 可以接受多个路径参数</li>
 *   <li>{@code coalesce(val1, val2, ...)} - 可以接受任意多个参数</li>
 * </ul>
 */
public interface FunctionTrait extends ExpressionTrait {
    /**
     * 获取函数名称。
     * <p>
     * 函数名用于标识函数，在 SQL 解析、函数查找和错误报告中使用。
     * 例如："abs", "sum", "json_extract", "concat" 等。
     *
     * @return 函数的名称，不能为 null
     */
    String getName();

    /**
     * 判断函数是否支持可变参数（variadic arguments）。
     * <p>
     * 可变参数函数是指可以接受可变数量参数的函数。例如：
     * <ul>
     *   <li>{@code concat(str1, str2, str3, ...)} - 可以连接任意多个字符串</li>
     *   <li>{@code json_extract(json, path1, path2, ...)} - 可以提取多个路径的值</li>
     * </ul>
     * <p>
     * 对于非可变参数函数，参数数量是固定的，例如：
     * <ul>
     *   <li>{@code abs(x)} - 固定 1 个参数</li>
     *   <li>{@code add(x, y)} - 固定 2 个参数</li>
     * </ul>
     * <p>
     * <b>默认实现：</b>
     * {@link ComputeSignature} 接口提供了默认实现，通过检查函数签名中的
     * {@code hasVarArgs} 标志来判断。具体函数类可以重写此方法以提供自定义逻辑。
     *
     * @return true 如果函数支持可变参数，false 如果参数数量固定
     */
    boolean hasVarArguments();
}
