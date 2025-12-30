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

#include <variant>

#include "vec/common/hash_table/hash_key_type.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/join_hash_table.h"

namespace doris {
using JoinOpVariants =
        std::variant<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>>;

template <class T>
using PrimaryTypeHashTableContext =
        vectorized::MethodOneNumber<T, JoinHashMap<T, HashCRC32<T>, false>>;

template <class T>
using DirectPrimaryTypeHashTableContext =
        vectorized::MethodOneNumberDirect<T, JoinHashMap<T, HashCRC32<T>, true>>;

template <class Key>
using FixedKeyHashTableContext =
        vectorized::MethodKeysFixed<JoinHashMap<Key, HashCRC32<Key>, false>>;

using SerializedHashTableContext =
        vectorized::MethodSerialized<JoinHashMap<StringRef, DefaultHash<StringRef>, false>>;
using MethodOneString =
        vectorized::MethodStringNoCache<JoinHashMap<StringRef, DefaultHash<StringRef>, false>>;

/**
 * Hash 表变体类型定义
 * 
 * 【三种主要的 Hash 表类型】
 * 
 * 1. PrimaryTypeHashTableContext（标准 Hash 表）
 *    - 定义：MethodOneNumber<T, JoinHashMap<T, HashCRC32<T>, false>>
 *    - 用途：单列整数类型键（UInt8/16/32/64/128/256）
 *    - 工作原理：
 *      * 使用 CRC32 Hash 函数计算键的 Hash 值
 *      * Hash 值取模得到桶编号：bucket_num = hash(key) & (bucket_size - 1)
 *      * 使用链式哈希处理冲突
 *    - 适用场景：
 *      * 单列整数键
 *      * 键值范围较大（无法使用 Direct Mapping）
 *      * 例如：用户 ID（1-1000000）、订单 ID 等
 *    - 性能：O(1) 平均查找时间，需要 Hash 计算开销
 * 
 * 2. DirectPrimaryTypeHashTableContext（直接映射 Hash 表）
 *    - 定义：MethodOneNumberDirect<T, JoinHashMap<T, HashCRC32<T>, true>>
 *    - 用途：单列整数类型键，键值范围小
 *    - 工作原理：
 *      * 不使用 Hash 函数，键值直接作为数组索引
 *      * bucket_num = key - min_key + 1（直接映射）
 *      * 如果 key < min_key 或 key > max_key，bucket_num = 0（表示未匹配）
 *    - 转换条件：
 *      * 从 PrimaryTypeHashTableContext 转换而来
 *      * 条件：max_key - min_key < MAX_MAPPING_RANGE (8,388,607)
 *      * 例如：键值范围 [100, 200] 可以使用 Direct Mapping
 *    - 适用场景：
 *      * 单列整数键
 *      * 键值范围小（差值 < 8M）
 *      * 例如：状态码（0-10）、类型 ID（1-100）等
 *    - 性能：O(1) 查找时间，无需 Hash 计算，性能最优
 *    - 内存：需要分配 (max_key - min_key + 2) 个桶的空间
 * 
 * 3. FixedKeyHashTableContext（固定长度键 Hash 表）
 *    - 定义：MethodKeysFixed<JoinHashMap<Key, HashCRC32<Key>, false>>
 *    - 用途：多列键或固定长度的复合键
 *    - 工作原理：
 *      * 将多个列打包成固定长度的键（UInt64/UInt72/UInt96/UInt104/UInt128/UInt136/UInt256）
 *      * 根据总大小选择键类型：
 *        - <= 8 bytes → UInt64
 *        - <= 9 bytes → UInt72
 *        - <= 12 bytes → UInt96
 *        - <= 13 bytes → UInt104
 *        - <= 16 bytes → UInt128
 *        - <= 17 bytes → UInt136
 *        - <= 32 bytes → UInt256
 *        - > 32 bytes → SerializedHashTableContext
 *      * 使用 CRC32 Hash 函数计算打包后的键的 Hash 值
 *    - 打包方式：
 *      * 如果有多列，按顺序打包到固定长度的整数中
 *      * 如果有 NULL 值，使用 bitmap 标记（每个键一个 bit）
 *      * 例如：两列 (INT32, INT32) → 打包成 UInt64
 *    - 适用场景：
 *      * 多列 Join 键（如 ON a.id = b.id AND a.type = b.type）
 *      * 键的总大小 <= 32 bytes
 *      * 例如：(user_id, order_id)、(date, region, product_id) 等
 *    - 性能：O(1) 平均查找时间，需要打包/解包开销
 * 
 * 【类型选择流程】
 * 
 * 1. 单列键：
 *    - 整数类型 → PrimaryTypeHashTableContext
 *      - 如果键值范围小 → 转换为 DirectPrimaryTypeHashTableContext
 *    - 字符串类型 → MethodOneString
 *    - 复杂类型 → SerializedHashTableContext
 * 
 * 2. 多列键：
 *    - 计算总大小（包括 NULL bitmap）
 *    - 根据大小选择 FixedKeyHashTableContext 的键类型
 *    - 如果总大小 > 32 bytes → SerializedHashTableContext
 * 
 * 【性能对比】
 * 
 * | Hash 表类型 | 查找时间 | Hash 计算 | 内存占用 | 适用场景 |
 * |------------|---------|----------|---------|---------|
 * | DirectPrimaryTypeHashTableContext | O(1) | 无 | 高（需要连续空间） | 键值范围小 |
 * | PrimaryTypeHashTableContext | O(1) | 有 | 中 | 单列整数键 |
 * | FixedKeyHashTableContext | O(1) | 有 | 中 | 多列键 |
 * 
 * 【示例】
 * 
 * 示例 1：单列整数键，范围大
 * - 键：user_id (1-1000000)
 * - 选择：PrimaryTypeHashTableContext<UInt64>
 * - 原因：范围太大，无法使用 Direct Mapping
 * 
 * 示例 2：单列整数键，范围小
 * - 键：status_code (0-10)
 * - 选择：DirectPrimaryTypeHashTableContext<UInt8>
 * - 原因：范围小（10-0=10 < 8M），可以使用 Direct Mapping
 * 
 * 示例 3：多列键
 * - 键：(user_id INT64, order_id INT64)
 * - 选择：FixedKeyHashTableContext<UInt128>
 * - 原因：两列 INT64 = 16 bytes，打包成 UInt128
 */
using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContext, PrimaryTypeHashTableContext<vectorized::UInt8>,
        PrimaryTypeHashTableContext<vectorized::UInt16>,
        PrimaryTypeHashTableContext<vectorized::UInt32>,
        PrimaryTypeHashTableContext<vectorized::UInt64>,
        PrimaryTypeHashTableContext<vectorized::UInt128>,
        PrimaryTypeHashTableContext<vectorized::UInt256>,
        DirectPrimaryTypeHashTableContext<vectorized::UInt8>,
        DirectPrimaryTypeHashTableContext<vectorized::UInt16>,
        DirectPrimaryTypeHashTableContext<vectorized::UInt32>,
        DirectPrimaryTypeHashTableContext<vectorized::UInt64>,
        DirectPrimaryTypeHashTableContext<vectorized::UInt128>,
        FixedKeyHashTableContext<vectorized::UInt64>, FixedKeyHashTableContext<vectorized::UInt72>,
        FixedKeyHashTableContext<vectorized::UInt96>, FixedKeyHashTableContext<vectorized::UInt104>,
        FixedKeyHashTableContext<vectorized::UInt128>,
        FixedKeyHashTableContext<vectorized::UInt136>,
        FixedKeyHashTableContext<vectorized::UInt256>, MethodOneString>;

struct JoinDataVariants {
    HashTableVariants method_variant;

    void init(const std::vector<vectorized::DataTypePtr>& data_types, HashKeyType type) {
        switch (type) {
        case HashKeyType::serialized:
            method_variant.emplace<SerializedHashTableContext>();
            break;
        case HashKeyType::int8_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt8>>();
            break;
        case HashKeyType::int16_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt16>>();
            break;
        case HashKeyType::int32_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt32>>();
            break;
        case HashKeyType::int64_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt64>>();
            break;
        case HashKeyType::int128_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt128>>();
            break;
        case HashKeyType::int256_key:
            method_variant.emplace<PrimaryTypeHashTableContext<vectorized::UInt256>>();
            break;
        case HashKeyType::string_key:
            method_variant.emplace<MethodOneString>();
            break;
        case HashKeyType::fixed64:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt64>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed72:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt72>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed96:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt96>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed104:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt104>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed128:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt128>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed136:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt136>>(
                    get_key_sizes(data_types));
            break;
        case HashKeyType::fixed256:
            method_variant.emplace<FixedKeyHashTableContext<vectorized::UInt256>>(
                    get_key_sizes(data_types));
            break;
        default:
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "JoinDataVariants meet invalid key type, type={}", type);
        }
    }
};

template <typename Method>
void primary_to_direct_mapping(Method* context, const vectorized::ColumnRawPtrs& key_columns,
                               const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    using FieldType = typename Method::Base::Key;
    FieldType max_key = std::numeric_limits<FieldType>::min();
    FieldType min_key = std::numeric_limits<FieldType>::max();

    size_t num_rows = key_columns[0]->size();
    if (key_columns[0]->is_nullable()) {
        const FieldType* input_keys =
                (FieldType*)assert_cast<const vectorized::ColumnNullable*>(key_columns[0])
                        ->get_nested_column_ptr()
                        ->get_raw_data()
                        .data;
        const vectorized::NullMap& null_map =
                assert_cast<const vectorized::ColumnNullable*>(key_columns[0])->get_null_map_data();
        // skip first mocked row
        for (size_t i = 1; i < num_rows; i++) {
            if (null_map[i]) {
                continue;
            }
            max_key = std::max(max_key, input_keys[i]);
            min_key = std::min(min_key, input_keys[i]);
        }
    } else {
        const FieldType* input_keys = (FieldType*)key_columns[0]->get_raw_data().data;
        // skip first mocked row
        for (size_t i = 1; i < num_rows; i++) {
            max_key = std::max(max_key, input_keys[i]);
            min_key = std::min(min_key, input_keys[i]);
        }
    }

    constexpr auto MAX_MAPPING_RANGE = 1 << 23;
    bool allow_direct_mapping = (max_key >= min_key && max_key - min_key < MAX_MAPPING_RANGE - 1);
    if (allow_direct_mapping) {
        for (const auto& variant_ptr : variant_ptrs) {
            variant_ptr->method_variant.emplace<DirectPrimaryTypeHashTableContext<FieldType>>(
                    max_key, min_key);
        }
    }
}

template <typename Method>
void try_convert_to_direct_mapping(
        Method* method, const vectorized::ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<vectorized::UInt8>* context,
        const vectorized::ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<vectorized::UInt16>* context,
        const vectorized::ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<vectorized::UInt32>* context,
        const vectorized::ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<vectorized::UInt64>* context,
        const vectorized::ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

inline void try_convert_to_direct_mapping(
        PrimaryTypeHashTableContext<vectorized::UInt128>* context,
        const vectorized::ColumnRawPtrs& key_columns,
        const std::vector<std::shared_ptr<JoinDataVariants>>& variant_ptrs) {
    primary_to_direct_mapping(context, key_columns, variant_ptrs);
}

} // namespace doris
