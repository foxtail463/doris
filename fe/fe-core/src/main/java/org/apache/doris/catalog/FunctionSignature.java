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

package org.apache.doris.catalog;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 函数签名类。
 * <p>
 * 表示一个函数的签名信息，包括返回类型、参数类型列表、是否支持可变参数等。
 * 用于函数重载匹配和类型推导。
 */
public class FunctionSignature {
    /** 函数的返回类型 */
    public final DataType returnType;
    /** 是否支持可变参数（varargs） */
    public final boolean hasVarArgs;
    /** 参数类型列表 */
    public final List<DataType> argumentsTypes;
    /** 参数数量（arity） */
    public final int arity;

    /**
     * 构造函数。
     *
     * @param returnType 返回类型
     * @param hasVarArgs 是否支持可变参数
     * @param argumentsTypes 参数类型列表
     */
    private FunctionSignature(DataType returnType, boolean hasVarArgs,
            List<? extends DataType> argumentsTypes) {
        this.returnType = Objects.requireNonNull(returnType, "returnType is not null");
        this.argumentsTypes = Utils.fastToImmutableList(
                Objects.requireNonNull(argumentsTypes, "argumentsTypes is not null"));
        this.hasVarArgs = hasVarArgs;
        this.arity = argumentsTypes.size();
    }

    /**
     * 获取可变参数的类型。
     * 如果函数支持可变参数，返回最后一个参数的类型（可变参数类型）。
     *
     * @return 可变参数类型，如果不支持可变参数则返回 empty
     */
    public Optional<DataType> getVarArgType() {
        return hasVarArgs ? Optional.of(argumentsTypes.get(arity - 1)) : Optional.empty();
    }

    /**
     * 获取指定索引位置的参数类型。
     * 如果支持可变参数且索引超出参数数量，返回可变参数类型。
     *
     * @param index 参数索引
     * @return 参数类型
     */
    public DataType getArgType(int index) {
        if (hasVarArgs && index >= arity) {
            return argumentsTypes.get(arity - 1);
        }
        return argumentsTypes.get(index);
    }

    /**
     * 创建新的函数签名，修改返回类型。
     *
     * @param returnType 新的返回类型
     * @return 新的函数签名
     */
    public FunctionSignature withReturnType(DataType returnType) {
        return new FunctionSignature(returnType, hasVarArgs, argumentsTypes);
    }

    /**
     * 创建新的函数签名，修改指定索引位置的参数类型。
     *
     * @param index 参数索引
     * @param argumentType 新的参数类型
     * @return 新的函数签名
     */
    public FunctionSignature withArgumentType(int index, DataType argumentType) {
        ImmutableList.Builder<DataType> builder = ImmutableList.builder();
        for (int i = 0; i < argumentsTypes.size(); i++) {
            if (i == index) {
                builder.add(argumentType);
            } else {
                builder.add(argumentsTypes.get(i));
            }
        }
        return new FunctionSignature(returnType, hasVarArgs, builder.build());
    }

    /**
     * 创建新的函数签名，修改参数类型列表和可变参数标志。
     *
     * @param hasVarArgs 是否支持可变参数
     * @param argumentsTypes 新的参数类型列表
     * @return 新的函数签名
     */
    public FunctionSignature withArgumentTypes(boolean hasVarArgs, List<? extends DataType> argumentsTypes) {
        return new FunctionSignature(returnType, hasVarArgs, argumentsTypes);
    }

    /**
     * 根据签名类型和实际参数类型，通过转换函数修改参数类型。
     * <p>
     * 转换函数接收三个参数：
     * <ul>
     *   <li>参数索引</li>
     *   <li>签名中的类型</li>
     *   <li>实际参数表达式</li>
     * </ul>
     * 返回新的参数类型。
     *
     * @param arguments 实际参数表达式列表
     * @param transform 转换函数，参数：索引、签名类型、实际参数，返回：新类型
     * @return 新的函数签名
     */
    public FunctionSignature withArgumentTypes(List<Expression> arguments,
            TripleFunction<Integer, DataType, Expression, DataType> transform) {
        List<DataType> newTypes = Lists.newArrayList();
        for (int i = 0; i < argumentsTypes.size(); i++) {
            newTypes.add(transform.apply(i, argumentsTypes.get(i), arguments.get(i)));
        }
        return withArgumentTypes(hasVarArgs, newTypes);
    }

    /**
     * 创建函数签名的静态工厂方法（不支持可变参数）。
     *
     * @param returnType 返回类型
     * @param argumentsTypes 参数类型列表
     * @return 函数签名
     */
    public static FunctionSignature of(DataType returnType, List<? extends DataType> argumentsTypes) {
        return of(returnType, false, argumentsTypes);
    }

    /**
     * 创建函数签名的静态工厂方法。
     *
     * @param returnType 返回类型
     * @param hasVarArgs 是否支持可变参数
     * @param argumentsTypes 参数类型列表
     * @return 函数签名
     */
    public static FunctionSignature of(DataType returnType, boolean hasVarArgs,
            List<? extends DataType> argumentsTypes) {
        return new FunctionSignature(returnType, hasVarArgs, argumentsTypes);
    }

    /**
     * 创建函数签名的静态工厂方法（不支持可变参数，使用可变参数列表）。
     *
     * @param returnType 返回类型
     * @param argumentsTypes 参数类型（可变参数）
     * @return 函数签名
     */
    public static FunctionSignature of(DataType returnType, DataType... argumentsTypes) {
        return of(returnType, false, argumentsTypes);
    }

    /**
     * 创建函数签名的静态工厂方法（使用可变参数列表）。
     *
     * @param returnType 返回类型
     * @param hasVarArgs 是否支持可变参数
     * @param argumentsTypes 参数类型（可变参数）
     * @return 函数签名
     */
    public static FunctionSignature of(DataType returnType,
            boolean hasVarArgs, DataType... argumentsTypes) {
        return new FunctionSignature(returnType, hasVarArgs, Arrays.asList(argumentsTypes));
    }

    /**
     * 创建函数签名构建器，指定返回类型。
     * <p>
     * 使用示例：
     * <pre>{@code
     * FunctionSignature.ret(StringType.INSTANCE)
     *     .args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE)
     * }</pre>
     *
     * @param returnType 返回类型
     * @return 函数签名构建器
     */
    public static FuncSigBuilder ret(DataType returnType) {
        return new FuncSigBuilder(returnType);
    }

    /**
     * 创建函数签名构建器，返回类型跟随指定参数的类型。
     * <p>
     * 用于返回类型需要跟随某个参数类型的函数（如 if/least/greatest）。
     *
     * @param argIndex 参数索引，返回类型将跟随此参数的类型
     * @return 函数签名构建器
     */
    public static FuncSigBuilder retArgType(int argIndex) {
        return new FuncSigBuilder(new FollowToArgumentType(argIndex));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("returnType", returnType)
                .add("hasVarArgs", hasVarArgs)
                .add("argumentsTypes", argumentsTypes)
                .add("arity", arity)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionSignature signature = (FunctionSignature) o;
        return hasVarArgs == signature.hasVarArgs && arity == signature.arity && Objects.equals(returnType,
                signature.returnType) && Objects.equals(argumentsTypes, signature.argumentsTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnType, hasVarArgs, argumentsTypes, arity);
    }

    /**
     * 函数签名构建器。
     * <p>
     * 提供链式调用的方式构建函数签名，使代码更简洁易读。
     */
    public static class FuncSigBuilder {
        /** 返回类型 */
        public final DataType returnType;

        /**
         * 构造函数。
         *
         * @param returnType 返回类型
         */
        public FuncSigBuilder(DataType returnType) {
            this.returnType = returnType;
        }

        /**
         * 设置参数类型（不支持可变参数）。
         *
         * @param argTypes 参数类型（可变参数）
         * @return 函数签名
         */
        public FunctionSignature args(DataType...argTypes) {
            return FunctionSignature.of(returnType, false, argTypes);
        }

        /**
         * 设置参数类型（支持可变参数）。
         *
         * @param argTypes 参数类型（可变参数），最后一个类型作为可变参数类型
         * @return 函数签名
         */
        public FunctionSignature varArgs(DataType...argTypes) {
            return FunctionSignature.of(returnType, true, argTypes);
        }
    }

    /**
     * 三参数函数接口。
     * <p>
     * 用于函数签名的参数类型转换等场景。
     *
     * @param <P1> 第一个参数类型
     * @param <P2> 第二个参数类型
     * @param <P3> 第三个参数类型
     * @param <R> 返回类型
     */
    public interface TripleFunction<P1, P2, P3, R> {
        /**
         * 应用函数。
         *
         * @param p1 第一个参数
         * @param p2 第二个参数
         * @param p3 第三个参数
         * @return 函数结果
         */
        R apply(P1 p1, P2 p2, P3 p3);
    }
}
