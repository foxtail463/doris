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
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignatureHelper.ComputeSignatureChain;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.function.BiFunction;

/**
 * 计算函数签名的接口。
 * <p>
 * 用于根据参数类型计算函数的返回类型。大多数情况下，应该继承 BoundFunction 并实现
 * ComputeSignature 的子接口（通常是 ExplicitlyCastableSignature），然后提供函数签名列表。
 * <p>
 * 此接口继承自 FunctionTrait 和 ImplicitCastInputTypes，提供了函数签名匹配、类型计算、
 * 隐式类型转换等功能。
 */
@Developing
public interface ComputeSignature extends FunctionTrait, ImplicitCastInputTypes {
    ///// current interface's methods /////

    /**
     * 获取函数的所有签名列表。
     * 子类需要提供函数支持的所有签名，用于签名匹配。
     *
     * @return 函数签名列表
     */
    List<FunctionSignature> getSignatures();

    /**
     * 获取当前匹配的函数签名。
     * 此方法是 searchSignature 的缓存版本，由 BoundFunction.getSignature() 实现。
     * 通常返回已经匹配好的签名，避免重复查找。
     *
     * @return 当前匹配的函数签名
     */
    FunctionSignature getSignature();

    /**
     * 根据参数类型查找匹配的函数签名。
     * 此方法会在 BoundFunction.getSignature() 中被调用（当 BoundFunction 实现了 ComputeSignature 时）。
     *
     * @param signatures 候选签名列表
     * @return 匹配的签名
     */
    FunctionSignature searchSignature(List<FunctionSignature> signatures);

    ///// re-defined other interface's methods, so we can mixin this interfaces like a trait /////

    /**
     * 获取函数名称。
     * 重新定义 BoundFunction 中的 getName 方法，默认返回类名。
     *
     * @return 函数名称
     */
    default String getName() {
        return getClass().getSimpleName();
    }

    ///// override expressions trait methods, so we can compute some properties by the signature /////

    /**
     * 从函数签名中计算期望的输入类型。
     * 如果函数支持可变参数（varargs），会为额外的参数填充可变参数类型。
     *
     * @return 期望的输入类型列表
     */
    @Override
    default List<DataType> expectedInputTypes() {
        FunctionSignature signature = getSignature();
        int arity = arity();
        // 处理可变参数：如果实际参数数量大于签名定义的参数数量，用可变参数类型填充
        if (signature.hasVarArgs && arity > signature.arity) {
            Builder<DataType> varTypes = ImmutableList.<DataType>builder()
                    .addAll(signature.argumentsTypes);
            DataType varType = signature.getVarArgType().get();
            for (int i = signature.arity; i < arity; ++i) {
                varTypes.add(varType);
            }
            return varTypes.build();
        }
        return signature.argumentsTypes;
    }

    /**
     * 从函数签名中获取返回类型。
     *
     * @return 函数的返回类型
     */
    @Override
    default DataType getDataType() {
        return getSignature().returnType;
    }

    /**
     * 判断函数是否支持可变参数。
     *
     * @return 如果支持可变参数返回 true，否则返回 false
     */
    @Override
    default boolean hasVarArguments() {
        return getSignature().hasVarArgs;
    }

    /**
     * 计算函数签名的默认实现。
     * <p>
     * 该处理链用于"签名后处理"，覆盖常见场景：
     * <ul>
     *   <li>Any 类型实现：将 Any 类型按真实类型实现</li>
     *   <li>精度补全：如果实现了 ComputePrecision，调用其 computePrecision()</li>
     *   <li>返回类型跟随参数：某些函数（如 if/least/greatest）的返回类型需要跟随某个参数类型</li>
     *   <li>DecimalV2 归一化：将 DecimalV2 的返回类型规范化为系统默认精度</li>
     *   <li>数组嵌套可空性：确保数组嵌套类型的可空性正确</li>
     *   <li>动态计算可变参数：处理可变参数的类型计算</li>
     * </ul>
     * <p>
     * 特殊函数若需要自定义流程，可在具体类中重写本方法。
     *
     * @param signature 初始函数签名
     * @return 处理后的函数签名
     */
    default FunctionSignature computeSignature(FunctionSignature signature) {
        // NOTE:
        // 该处理链用于"签名后处理"，覆盖常见场景：Any 类型实现、精度补全、ReturnType 跟随参数、DecimalV2 归一化等。
        // 特殊函数若需要自定义流程，可在具体类中重写本方法。
        return ComputeSignatureChain.from(this, signature, getArguments())
                // 将 Any 类型在参数位置按真实类型实现（不带索引/带索引两种场景）
                .then(ComputeSignatureHelper::implementAnyDataTypeWithOutIndex)
                .then(ComputeSignatureHelper::implementAnyDataTypeWithIndex)
                // 精度补全：若实现了 ComputePrecision（如 Sum 实现了 ComputePrecisionForSum），在此调用其 computePrecision()
                .then(ComputeSignatureHelper::computePrecision)
                // 某些函数的返回类型需要"跟随某个参数类型"（如 if/least/greatest 等）的通用实现
                .then(ComputeSignatureHelper::implementFollowToArgumentReturnType)
                // 将 DecimalV2 的返回类型规范化为系统默认（避免携带用户定义的非默认精度）
                .then(ComputeSignatureHelper::normalizeDecimalV2)
                .then(ComputeSignatureHelper::ensureNestedNullableOfArray)
                .get();
    }

    /**
     * 递归处理复杂类型（Array、Map、Struct）。
     * <p>
     * 对复杂类型递归应用处理器函数，用于类型匹配、类型转换等场景。
     * 例如：Array 类型会递归处理元素类型，Map 类型会递归处理 key 和 value 类型。
     *
     * @param signatureType 签名中的类型
     * @param realType 实际类型
     * @param processor 处理函数，接收两个类型参数，返回处理结果
     * @return 处理结果
     */
    static boolean processComplexType(DataType signatureType, DataType realType,
            BiFunction<DataType, DataType, Boolean> processor) {
        // 处理 Array 类型：递归处理元素类型
        if (signatureType instanceof ArrayType && realType instanceof ArrayType) {
            return processComplexType(((ArrayType) signatureType).getItemType(),
                    ((ArrayType) realType).getItemType(), processor);
        } 
        // 处理 Map 类型：递归处理 key 和 value 类型
        else if (signatureType instanceof MapType && realType instanceof MapType) {
            return processComplexType(((MapType) signatureType).getKeyType(),
                    ((MapType) realType).getKeyType(), processor)
                    && processComplexType(((MapType) signatureType).getValueType(),
                    ((MapType) realType).getValueType(), processor);
        } 
        // 处理 Struct 类型：目前不支持
        else if (signatureType instanceof StructType && realType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
            return true;
        } 
        // 基础类型：直接应用处理器
        else {
            return processor.apply(signatureType, realType);
        }
    }
}
