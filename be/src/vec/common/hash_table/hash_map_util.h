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

#pragma once

#include "vec/common/hash_table/hash_key_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

inline std::vector<vectorized::DataTypePtr> get_data_types(
        const vectorized::VExprContextSPtrs& expr_contexts) {
    std::vector<vectorized::DataTypePtr> data_types;
    for (const auto& ctx : expr_contexts) {
        data_types.emplace_back(ctx->root()->data_type());
    }
    return data_types;
}

/**
 * 初始化 Hash 表方法
 * 
 * 【功能说明】
 * 根据数据类型和阶段信息，确定合适的 Hash 键类型，并初始化 Hash 表变体。
 * 这是 Hash 表初始化的核心函数，负责选择最优的 Hash 表实现。
 * 
 * 【Hash 键类型选择规则】
 * 1. 根据数据类型确定基础 Hash 键类型：
 *    - 单列整数类型（UInt8/16/32/64/128/256）→ int8_key/int16_key/int32_key/int64_key/int128_key/int256_key
 *    - 单列字符串类型 → string_key
 *    - 多列键 → fixed64/fixed72/fixed96/fixed104/fixed128/fixed136/fixed256（根据总大小）
 *    - 复杂类型或超大键 → serialized
 *    - 无键 → without_key
 * 
 * 2. 根据阶段信息调整类型：
 *    - is_first_phase = true：使用基础类型（int32_key, int64_key）
 *    - is_first_phase = false：使用 phase2 类型（int32_key_phase2, int64_key_phase2）
 *    - phase2 类型用于某些聚合场景的优化
 * 
 * 【Hash 表变体初始化】
 * data->init() 会根据 HashKeyType 选择具体的 Hash 表实现：
 * - int8_key → PrimaryTypeHashTableContext<UInt8>
 * - int64_key → PrimaryTypeHashTableContext<UInt64>
 * - string_key → MethodOneString 或 SerializedHashTableContext
 * - fixed64 → FixedKeyHashTableContext<UInt64>
 * - serialized → SerializedHashTableContext
 * 
 * 【异常处理】
 * - 如果初始化过程中抛出异常，std::variant 可能处于 valueless_by_exception 状态
 * - 需要显式设置为 monostate，避免后续访问时崩溃
 * 
 * 【参数说明】
 * @param data Hash 表变体指针（JoinDataVariants 或 AggDataVariants）
 * @param data_types Join/Agg 键的数据类型列表
 * @param is_first_phase 是否是第一阶段（用于决定是否使用 phase2 类型）
 * @return Status
 */
template <typename DataVariants>
Status init_hash_method(DataVariants* data, const std::vector<vectorized::DataTypePtr>& data_types,
                        bool is_first_phase) {
    auto type = HashKeyType::EMPTY;
    try {
        // ===== 步骤 1：确定 Hash 键类型 =====
        // 1.1 根据数据类型确定基础 Hash 键类型
        //     - 单列整数类型 → int8_key/int16_key/int32_key/int64_key/int128_key/int256_key
        //     - 单列字符串类型 → string_key
        //     - 多列键 → fixed64/fixed72/.../fixed256（根据总大小）
        //     - 复杂类型或超大键 → serialized
        //     - 无键 → without_key
        auto base_type = get_hash_key_type(data_types);
        
        // 1.2 根据阶段信息调整类型
        //     - is_first_phase = true → 使用基础类型（int32_key, int64_key）
        //     - is_first_phase = false → 使用 phase2 类型（int32_key_phase2, int64_key_phase2）
        //     - phase2 类型用于某些聚合场景的优化（如两阶段聚合）
        type = get_hash_key_type_with_phase(base_type, !is_first_phase);
        
        // ===== 步骤 2：初始化 Hash 表变体 =====
        // 根据 HashKeyType，在 method_variant 中创建对应的 Hash 表实现
        // 例如：type = int64_key → method_variant.emplace<PrimaryTypeHashTableContext<UInt64>>()
        data->init(data_types, type);
    } catch (const Exception& e) {
        // ===== 异常处理 =====
        // method_variant may meet valueless_by_exception, so we set it to monostate
        // 如果初始化过程中抛出异常，std::variant 可能处于 valueless_by_exception 状态
        // 这种情况下，variant 无法访问任何值，需要显式设置为 monostate
        // 否则后续访问 variant 时会崩溃
        data->method_variant.template emplace<std::monostate>();
        return e.to_status();
    }

    // ===== 步骤 3：验证 variant 状态 =====
    // 确保 variant 不处于 valueless_by_exception 状态
    // 如果处于该状态，说明初始化过程中发生了异常，但异常被捕获了
    CHECK(!data->method_variant.valueless_by_exception());

    // ===== 步骤 4：验证初始化结果 =====
    // 如果 Hash 键类型不是 without_key 和 EMPTY，但 variant 的索引是 0（monostate），
    // 说明初始化失败（应该创建了具体的 Hash 表实现，但实际是 monostate）
    if (type != HashKeyType::without_key && type != HashKeyType::EMPTY &&
        data->method_variant.index() == 0) { // index is 0 means variant is monostate
        return Status::InternalError("method_variant init failed");
    }
    return Status::OK();
}

// DataVariants 是一个轻量的“方法选择器”与承载体：
// - 用一个 std::variant (MethodVariants) 存放当前选中的聚合 Key 方法实现类型
// - 提供 emplace_single()，根据是否可空(nullable)为“单数值键类型”放入对应的方法类型
//
// 模板参数说明：
// - MethodVariants: 一个 std::variant<...>，列举所有可能的方法实现类型
// - MethodNullable<Inner>: 将单列方法包装为“可空单列方法”的外层模板(例如 MethodSingleNullableColumn<Inner>)。
// - MethodOneNumber<T, DataT>: 针对单数值键 T 的方法模板(例如 MethodOneNumber<UInt64, AggData<UInt64>>)。
// - DataNullable<TT>: 将数据容器类型 TT 适配为“可空”版本(例如 AggDataNullable<TT>)。
template <typename MethodVariants, template <typename> typename MethodNullable,
          template <typename, typename> typename MethodOneNumber,
          template <typename> typename DataNullable>
struct DataVariants {
    DataVariants() = default;
    DataVariants(const DataVariants&) = delete;
    DataVariants& operator=(const DataVariants&) = delete;
    MethodVariants method_variant;

    // 将“单数值键”的方法按可空性放入 method_variant：
    // - 非空: 直接放入 MethodOneNumber<T, TT>
    // - 可空: 先将数据容器 TT 适配为 DataNullable<TT>，再用 MethodOneNumber<T, DataNullable<TT>>，
    //         最后再用 MethodNullable<...> 包一层，得到“可空单列键”的方法类型
    template <typename T, typename TT>
    void emplace_single(bool nullable) {
        if (nullable) {
            method_variant.template emplace<MethodNullable<MethodOneNumber<T, DataNullable<TT>>>>();
        } else {
            method_variant.template emplace<MethodOneNumber<T, TT>>();
        }
    }
};

} // namespace doris