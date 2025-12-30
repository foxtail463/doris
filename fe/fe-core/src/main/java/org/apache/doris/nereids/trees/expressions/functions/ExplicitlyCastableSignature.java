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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.ComplexDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.List;

/**
 * Explicitly castable signature. This class equals to 'CompareMode.IS_NONSTRICT_SUPERTYPE_OF'.
 *
 * Non-strict supertypes broaden the definition of supertype to accept implicit casts
 * of arguments that may result in loss of precision - e.g. decimal to float.
 */
public interface ExplicitlyCastableSignature extends ComputeSignature {

    static boolean isExplicitlyCastable(DataType signatureType, DataType realType) {
        return ComputeSignature.processComplexType(
                signatureType, realType, ExplicitlyCastableSignature::isPrimitiveExplicitlyCastable);
    }

    /** isExplicitlyCastable */
    static boolean isPrimitiveExplicitlyCastable(DataType signatureType, DataType realType) {
        if (signatureType instanceof AnyDataType
                || signatureType instanceof FollowToAnyDataType
                || signatureType.isAssignableFrom(realType)) {
            return true;
        }
        if (realType instanceof NullType) {
            return true;
        }
        if (signatureType instanceof ComplexDataType && !(realType instanceof ComplexDataType)) {
            return false;
        }
        try {
            return TypeCoercionUtils.canCastTo(realType, signatureType);
        } catch (Throwable t) {
            // the signatureType maybe DataType and can not cast to catalog data type.
            return false;
        }
    }

    /**
     * 多轮函数签名匹配策略。
     *
     * 该方法实现了从最严格到最宽松的渐进式签名匹配策略，依次尝试四轮匹配：
     * 1. 精确匹配（Identical）：参数类型完全一致
     * 2. NULL 或精确匹配（NullOrIdentical）：允许 NULL 类型或类型完全一致
     * 3. 隐式转换匹配（ImplicitlyCastable）：允许隐式类型转换（不损失精度）
     * 4. 显式转换匹配（ExplicitlyCastable）：允许显式类型转换（可能损失精度）
     *
     * 匹配策略说明：
     * - 每轮匹配都会遍历所有候选签名，找到第一个匹配的签名即返回
     * - 如果某一轮找到匹配，则不再尝试后续轮次
     * - 如果所有轮次都未找到匹配，则抛出异常
     *
     * 示例：
     * - 函数签名：JSON_EXTRACT_STRING(JsonType, StringType)
     * - 实际调用：JSON_EXTRACT_STRING(StringType, StringType)
     *   - 第 1 轮（精确匹配）：失败（StringType != JsonType）
     *   - 第 2 轮（NULL 或精确匹配）：失败
     *   - 第 3 轮（隐式转换）：失败（StringType 不能隐式转换为 JsonType）
     *   - 第 4 轮（显式转换）：成功（StringType 可以显式转换为 JsonType）
     *   - 结果：匹配成功，自动插入 CAST(StringType, JsonType)
     *
     * @param signatures 候选函数签名列表
     * @return 匹配的函数签名
     * @throws AnalysisException 如果所有轮次都未找到匹配的签名
     */
    @Override
    default FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return SearchSignature.from(this, signatures, getArguments())
                // 第 1 轮：精确匹配策略
                // 要求参数类型与签名类型完全一致，不允许任何类型转换
                // 例如：函数签名是 (IntType, IntType)，实际参数是 (IntType, IntType)，则匹配成功
                .orElseSearch(IdenticalSignature::isIdentical)
                // 第 2 轮：NULL 或精确匹配策略
                // 允许参数为 NULL 类型，或者类型完全一致
                // 例如：函数签名是 (IntType, IntType)，实际参数是 (NullType, IntType)，则匹配成功
                .orElseSearch(NullOrIdenticalSignature::isNullOrIdentical)
                // 第 3 轮：隐式转换匹配策略
                // 允许隐式类型转换（不损失精度的转换，如 IntType → BigIntType）
                // 例如：函数签名是 (BigIntType, BigIntType)，实际参数是 (IntType, IntType)，则匹配成功
                .orElseSearch(ImplicitlyCastableSignature::isImplicitlyCastable)
                // 第 4 轮：显式转换匹配策略（最宽松）
                // 允许显式类型转换（可能损失精度的转换，如 DecimalType → FloatType，StringType → JsonType）
                // 例如：函数签名是 (JsonType, StringType)，实际参数是 (StringType, StringType)，则匹配成功
                // 此时会自动插入 CAST(StringType, JsonType) 表达式
                .orElseSearch(ExplicitlyCastableSignature::isExplicitlyCastable)
                // 执行多轮匹配，如果所有轮次都未找到匹配，则抛出异常
                .resultOrException(getName());
    }
}
