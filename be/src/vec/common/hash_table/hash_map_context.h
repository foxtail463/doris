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

#include <cstdint>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/custom_allocator.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
constexpr auto BITSIZE = 8;

template <typename Base>
struct DataWithNullKey;

/**
 * Hash 表方法的抽象基类
 * 
 * 【抽象的核心】
 * `MethodBaseInner` 抽象的是"不同键类型的 Hash 表操作方法"的统一接口。
 * 它提供了一个模板框架，用于处理不同类型的 Hash 表（JoinHashMap、AggHashMap 等）
 * 和不同类型的键（数值键、字符串键、序列化键、固定长度键等）。
 * 
 * 【设计模式】
 * - 模板方法模式：定义算法骨架，子类实现具体步骤
 * - 策略模式：不同的子类实现不同的键处理策略
 * - CRTP（Curiously Recurring Template Pattern）：通过模板参数实现多态
 * 
 * 【核心职责】
 * 1. 类型提取：从 HashMap 模板参数中提取 Key、Mapped、Value 类型
 * 2. 统一接口：定义所有 Hash 表方法必须实现的虚函数接口
 * 3. 通用实现：提供 Join 和 Aggregation 场景的通用实现（bucket_nums、hash_values）
 * 4. 键管理：管理键的存储、序列化、Hash 计算等
 * 
 * 【继承层次】
 * ```
 * MethodBaseInner<HashMap>
 *     ↓
 * MethodBase<HashMap>  (添加迭代器支持)
 *     ↓
 * MethodOneNumber<T, HashMap>      // 单列数值键（零拷贝）
 * MethodSerialized<HashMap>        // 序列化键（复杂类型）
 * MethodKeysFixed<HashMap>         // 固定长度键（多列打包）
 * MethodStringNoCache<HashMap>     // 字符串键（无缓存）
 * ```
 * 
 * 【子类实现的不同策略】
 * 
 * 1. MethodOneNumber（单列数值键）
 *    - 策略：零拷贝，直接使用列的原始数据指针
 *    - 适用：UInt8/16/32/64/128/256 类型的单列键
 *    - 示例：user_id (UInt64) → 直接使用 UInt64* keys
 * 
 * 2. MethodSerialized（序列化键）
 *    - 策略：将多列键序列化为连续的字节流（StringRef）
 *    - 适用：复杂类型、多列键、超大键（>32 bytes）
 *    - 示例：(user_id, order_id, timestamp) → 序列化为 StringRef
 * 
 * 3. MethodKeysFixed（固定长度键）
 *    - 策略：将多列键打包成固定长度的整数（UInt64/UInt128/UInt256 等）
 *    - 适用：多列键，总大小 <= 32 bytes
 *    - 示例：(INT32, INT32) → 打包成 UInt64
 * 
 * 4. MethodStringNoCache（字符串键）
 *    - 策略：直接使用 StringRef，不缓存字符串数据
 *    - 适用：字符串类型的单列键
 *    - 示例：product_name (String) → 使用 StringRef
 * 
 * 【关键成员变量】
 * 
 * - hash_table: Hash 表的智能指针（JoinHashMap 或 AggHashMap）
 * - keys: 键的数组指针（Key*），不同子类有不同的初始化方式
 * - arena: 内存池，用于分配临时内存（如序列化键）
 * - hash_values: Hash 值数组（Aggregation 场景使用）
 * - bucket_nums: Hash 桶编号数组（Join 场景使用）
 * 
 * 【虚函数接口（子类必须实现）】
 * 
 * 1. init_serialized_keys(): 初始化键
 *    - MethodOneNumber: 零拷贝，直接使用列的数据指针
 *    - MethodSerialized: 序列化键到 arena
 *    - MethodKeysFixed: 打包键到固定长度整数
 * 
 * 2. estimated_size(): 估算内存使用大小
 *    - 不同子类有不同的内存占用模式
 * 
 * 3. insert_keys_into_columns(): 将键插入到列中
 *    - 用于某些场景下需要将 Hash 表的键输出到结果列
 * 
 * 【通用实现（基类提供）】
 * 
 * 1. init_join_bucket_num(): 计算 Join 场景的 Hash 桶编号
 *    - bucket_nums[i] = hash(keys[i]) & (bucket_size - 1)
 * 
 * 2. init_hash_values(): 计算 Aggregation 场景的 Hash 值
 *    - hash_values[i] = hash(keys[i])
 * 
 * 3. find() / lazy_emplace(): Hash 表查找和插入的模板方法
 *    - 使用 prefetch 优化缓存性能
 * 
 * 【使用示例】
 * 
 * ```cpp
 * // 示例 1：单列 UInt64 键的 Join
 * using HashMap = JoinHashMap<UInt64, HashCRC32<UInt64>, false>;
 * using Method = MethodOneNumber<UInt64, HashMap>;
 * Method method;
 * method.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
 * // keys 直接指向列的数据，零拷贝
 * 
 * // 示例 2：多列键的序列化
 * using HashMap = JoinHashMap<StringRef, DefaultHash<StringRef>, false>;
 * using Method = MethodSerialized<HashMap>;
 * Method method;
 * method.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
 * // keys 是序列化后的 StringRef 数组
 * 
 * // 示例 3：固定长度键的打包
 * using HashMap = JoinHashMap<UInt128, HashCRC32<UInt128>, false>;
 * using Method = MethodKeysFixed<HashMap>;
 * Method method;
 * method.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
 * // keys 是打包后的 UInt128 数组
 * ```
 * 
 * 【设计优势】
 * 
 * 1. 类型安全：通过模板参数确保类型一致性
 * 2. 性能优化：不同子类使用最适合的键处理策略
 * 3. 代码复用：通用实现（bucket_nums、hash_values）在基类中实现
 * 4. 扩展性：新增键类型只需添加新的子类
 * 5. 统一接口：上层代码通过统一的接口操作不同的 Hash 表方法
 */
template <typename HashMap>
struct MethodBaseInner {
    /**
     * Hash 表的三个核心类型别名
     * 
     * 【类型说明】
     * 这三个类型别名从 HashMap 模板参数中提取，用于统一访问不同类型的 Hash 表。
     * 
     * 【具体例子】
     * 
     * 例子 1：JoinHashMap（Hash Join 场景）
     * ```cpp
     * // 定义：JoinHashMap<UInt64, HashCRC32<UInt64>, false>
     * // 用于：SELECT * FROM orders o JOIN users u ON o.user_id = u.id
     * //      其中 user_id 是 UInt64 类型
     * 
     * using HashMap = JoinHashMap<UInt64, HashCRC32<UInt64>, false>;
     * 
     * // 从 JoinHashTable 中提取：
     * // template <typename Key, typename Hash, bool DirectMapping>
     * // class JoinHashTable {
     * //     using key_type = Key;        // UInt64
     * //     using mapped_type = void*;    // void*（Join 不需要存储值）
     * //     using value_type = void*;     // void*（Join 不需要存储值）
     * // };
     * 
     * Key    = UInt64   // Join 键的类型（user_id）
     * Mapped = void*    // Join 中不需要存储值，只需要匹配键
     * Value  = void*    // Join 中不需要存储值，只需要匹配键
     * ```
     * 
     * 例子 2：AggHashMap（Aggregation 场景）
     * ```cpp
     * // 定义：PHHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>
     * // 用于：SELECT user_id, SUM(amount) FROM orders GROUP BY user_id
     * //      其中 user_id 是 UInt64 类型，SUM(amount) 是聚合结果
     * 
     * using HashMap = PHHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
     * 
     * // 从 PHHashMap 中提取：
     * // template <typename Key, typename Mapped, typename Hash>
     * // class PHHashMap {
     * //     using key_type = Key;                    // UInt64
     * //     using mapped_type = Mapped;               // AggregateDataPtr
     * //     using value_type = std::pair<const Key, Mapped>;  // 键值对
     * // };
     * 
     * Key    = UInt64           // GROUP BY 键的类型（user_id）
     * Mapped = AggregateDataPtr // 聚合数据的指针（存储 SUM(amount) 的结果）
     * Value  = std::pair<const UInt64, AggregateDataPtr>  // 键值对（user_id -> 聚合结果）
     * ```
     * 
     * 例子 3：字符串键的 JoinHashMap
     * ```cpp
     * // 定义：JoinHashMap<StringRef, DefaultHash<StringRef>, false>
     * // 用于：SELECT * FROM orders o JOIN products p ON o.product_name = p.name
     * 
     * using HashMap = JoinHashMap<StringRef, DefaultHash<StringRef>, false>;
     * 
     * Key    = StringRef  // Join 键的类型（product_name，字符串）
     * Mapped = void*      // Join 不需要存储值
     * Value  = void*      // Join 不需要存储值
     * ```
     * 
     * 【使用场景对比】
     * 
     * | 场景 | Key 类型 | Mapped 类型 | Value 类型 | 用途 |
     * |------|---------|------------|-----------|------|
     * | Hash Join | Join 键类型（UInt64/StringRef 等） | void* | void* | 只匹配键，不存储值 |
     * | Aggregation | GROUP BY 键类型（UInt64/StringRef 等） | AggregateDataPtr | std::pair<const Key, Mapped> | 存储聚合结果 |
     * 
     * 【关键区别】
     * - Join：只需要匹配键，不需要存储值，所以 Mapped 和 Value 都是 void*
     * - Aggregation：需要存储每个键对应的聚合结果，所以 Mapped 是 AggregateDataPtr
     * 
     * 【实际使用】
     * - Key* keys：存储键的数组指针（例如：UInt64* keys 存储所有 user_id）
     * - Mapped：在 Aggregation 中用于访问聚合数据
     * - Value：在 Aggregation 中用于遍历 Hash 表的键值对
     */
    using Key = typename HashMap::key_type;        // Hash 表的键类型
    using Mapped = typename HashMap::mapped_type;  // Hash 表的映射值类型
    using Value = typename HashMap::value_type;    // Hash 表的键值对类型
    using HashMapType = HashMap;                   // Hash 表的完整类型

    std::shared_ptr<HashMap> hash_table = nullptr;
    Key* keys = nullptr;
    Arena arena;
    DorisVector<size_t> hash_values;

    // use in join case
    DorisVector<uint32_t> bucket_nums;

    MethodBaseInner() { hash_table.reset(new HashMap()); }
    virtual ~MethodBaseInner() = default;

    virtual void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                      const uint8_t* null_map = nullptr, bool is_join = false,
                                      bool is_build = false, uint32_t bucket_size = 0) = 0;

    [[nodiscard]] virtual size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                                bool is_join = false, bool is_build = false,
                                                uint32_t bucket_size = 0) = 0;

    virtual size_t serialized_keys_size(bool is_build) const { return 0; }

    void init_join_bucket_num(uint32_t num_rows, uint32_t bucket_size, const uint8_t* null_map) {
        bucket_nums.resize(num_rows);

        if (null_map == nullptr) {
            init_join_bucket_num(num_rows, bucket_size);
            return;
        }
        for (uint32_t k = 0; k < num_rows; ++k) {
            bucket_nums[k] =
                    null_map[k] ? bucket_size : hash_table->hash(keys[k]) & (bucket_size - 1);
        }
    }

    void init_join_bucket_num(uint32_t num_rows, uint32_t bucket_size) {
        for (uint32_t k = 0; k < num_rows; ++k) {
            bucket_nums[k] = hash_table->hash(keys[k]) & (bucket_size - 1);
        }
    }

    void init_hash_values(uint32_t num_rows, const uint8_t* null_map) {
        if (null_map == nullptr) {
            init_hash_values(num_rows);
            return;
        }
        hash_values.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k) {
            if (null_map[k]) {
                continue;
            }

            hash_values[k] = hash_table->hash(keys[k]);
        }
    }

    void init_hash_values(uint32_t num_rows) {
        hash_values.resize(num_rows);
        for (size_t k = 0; k < num_rows; ++k) {
            hash_values[k] = hash_table->hash(keys[k]);
        }
    }

    template <bool read>
    ALWAYS_INLINE void prefetch(size_t i) {
        if (LIKELY(i + HASH_MAP_PREFETCH_DIST < hash_values.size())) {
            hash_table->template prefetch<read>(keys[i + HASH_MAP_PREFETCH_DIST],
                                                hash_values[i + HASH_MAP_PREFETCH_DIST]);
        }
    }

    template <typename State>
    ALWAYS_INLINE auto find(State& state, size_t i) {
        if constexpr (!is_string_hash_map()) {
            prefetch<true>(i);
        }
        return state.find_key_with_hash(*hash_table, i, keys[i], hash_values[i]);
    }

    template <typename State, typename F, typename FF>
    ALWAYS_INLINE auto lazy_emplace(State& state, size_t i, F&& creator,
                                    FF&& creator_for_null_key) {
        if constexpr (!is_string_hash_map()) {
            prefetch<false>(i);
        }
        return state.lazy_emplace_key(*hash_table, i, keys[i], hash_values[i], creator,
                                      creator_for_null_key);
    }

    static constexpr bool is_string_hash_map() {
        return std::is_same_v<StringHashMap<Mapped>, HashMap> ||
               std::is_same_v<DataWithNullKey<StringHashMap<Mapped>>, HashMap>;
    }

    template <typename Key, typename Origin>
    static void try_presis_key(Key& key, Origin& origin, Arena& arena) {
        if constexpr (std::is_same_v<Key, StringRef>) {
            key.data = arena.insert(key.data, key.size);
        }
    }

    template <typename Key, typename Origin>
    static void try_presis_key_and_origin(Key& key, Origin& origin, Arena& arena) {
        if constexpr (std::is_same_v<Origin, StringRef>) {
            origin.data = arena.insert(origin.data, origin.size);
            if constexpr (!is_string_hash_map()) {
                key = origin;
            }
        }
    }

    virtual void insert_keys_into_columns(std::vector<Key>& keys, MutableColumns& key_columns,
                                          uint32_t num_rows) = 0;

    virtual uint32_t direct_mapping_range() { return 0; }
};

template <typename T>
concept IteratoredMap = requires(T* map) { typename T::iterator; };

template <typename HashMap>
struct MethodBase : public MethodBaseInner<HashMap> {
    using Iterator = void*;
    void init_iterator() {}
};

template <IteratoredMap HashMap>
struct MethodBase<HashMap> : public MethodBaseInner<HashMap> {
    using Iterator = typename HashMap::iterator;
    using Base = MethodBaseInner<HashMap>;
    Iterator begin;
    Iterator end;
    bool inited_iterator = false;
    void init_iterator() {
        if (!inited_iterator) {
            inited_iterator = true;
            begin = Base::hash_table->begin();
            end = Base::hash_table->end();
        }
    }
};

template <typename TData>
struct MethodSerialized : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using State = ColumnsHashing::HashMethodSerialized<typename Base::Value, typename Base::Mapped>;
    using Base::try_presis_key;
    // need keep until the hash probe end.
    DorisVector<StringRef> build_stored_keys;
    Arena build_arena;
    // refresh each time probe
    DorisVector<StringRef> stored_keys;

    StringRef serialize_keys_to_pool_contiguous(size_t i, size_t keys_size,
                                                const ColumnRawPtrs& key_columns, Arena& pool) {
        const char* begin = nullptr;

        size_t sum_size = 0;
        for (size_t j = 0; j < keys_size; ++j) {
            sum_size += key_columns[j]->serialize_value_into_arena(i, pool, begin).size;
        }

        return {begin, sum_size};
    }

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        for (const auto& column : key_columns) {
            size += column->byte_size();
        }

        size += sizeof(StringRef) * num_rows; // stored_keys
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys_impl(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                   DorisVector<StringRef>& input_keys, Arena& input_arena) {
        input_arena.clear();
        input_keys.resize(num_rows);

        size_t max_one_row_byte_size = 0;
        for (const auto& column : key_columns) {
            max_one_row_byte_size += column->get_max_row_byte_size();
        }
        size_t total_bytes = max_one_row_byte_size * num_rows;
        if (total_bytes > config::pre_serialize_keys_limit_bytes) {
            // reach mem limit, don't serialize in batch
            size_t keys_size = key_columns.size();
            for (size_t i = 0; i < num_rows; ++i) {
                input_keys[i] =
                        serialize_keys_to_pool_contiguous(i, keys_size, key_columns, input_arena);
            }
        } else {
            auto* serialized_key_buffer =
                    reinterpret_cast<uint8_t*>(input_arena.alloc(total_bytes));

            for (size_t i = 0; i < num_rows; ++i) {
                input_keys[i].data =
                        reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
                input_keys[i].size = 0;
            }

            for (const auto& column : key_columns) {
                column->serialize(input_keys.data(), num_rows);
            }
        }
        Base::keys = input_keys.data();
    }

    size_t serialized_keys_size(bool is_build) const override {
        if (is_build) {
            return build_stored_keys.size() * sizeof(StringRef) + build_arena.size();
        } else {
            return stored_keys.size() * sizeof(StringRef) + Base::arena.size();
        }
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        init_serialized_keys_impl(key_columns, num_rows, is_build ? build_stored_keys : stored_keys,
                                  is_build ? build_arena : Base::arena);
        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
        }
    }

    void insert_keys_into_columns(std::vector<StringRef>& input_keys, MutableColumns& key_columns,
                                  const uint32_t num_rows) override {
        for (auto& column : key_columns) {
            column->deserialize(input_keys.data(), num_rows);
        }
    }
};

inline size_t get_bitmap_size(size_t key_number) {
    return (key_number + BITSIZE - 1) / BITSIZE;
}

template <typename TData>
struct MethodStringNoCache : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State =
            ColumnsHashing::HashMethodString<typename Base::Value, typename Base::Mapped, true>;

    // need keep until the hash probe end.
    DorisVector<StringRef> _build_stored_keys;
    // refresh each time probe
    DorisVector<StringRef> _stored_keys;

    size_t serialized_keys_size(bool is_build) const override {
        return is_build ? (_build_stored_keys.size() * sizeof(StringRef))
                        : (_stored_keys.size() * sizeof(StringRef));
    }

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        size += sizeof(StringRef) * num_rows; // stored_keys
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys_impl(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                                   DorisVector<StringRef>& stored_keys) {
        const IColumn& column = *key_columns[0];
        const auto& nested_column =
                column.is_nullable()
                        ? assert_cast<const ColumnNullable&>(column).get_nested_column()
                        : column;
        auto serialized_str = [](const auto& column_string, DorisVector<StringRef>& stored_keys) {
            const auto& offsets = column_string.get_offsets();
            const auto* chars = column_string.get_chars().data();
            stored_keys.resize(column_string.size());
            for (size_t row = 0; row < column_string.size(); row++) {
                stored_keys[row] =
                        StringRef(chars + offsets[row - 1], offsets[row] - offsets[row - 1]);
            }
        };
        if (nested_column.is_column_string64()) {
            const auto& column_string = assert_cast<const ColumnString64&>(nested_column);
            serialized_str(column_string, stored_keys);
        } else {
            const auto& column_string = assert_cast<const ColumnString&>(nested_column);
            serialized_str(column_string, stored_keys);
        }
        Base::keys = stored_keys.data();
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        init_serialized_keys_impl(key_columns, num_rows,
                                  is_build ? _build_stored_keys : _stored_keys);
        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
        }
    }

    void insert_keys_into_columns(std::vector<StringRef>& input_keys, MutableColumns& key_columns,
                                  const uint32_t num_rows) override {
        key_columns[0]->reserve(num_rows);
        key_columns[0]->insert_many_strings(input_keys.data(), num_rows);
    }
};

/**
 * 单列数值键的 Hash 表方法实现
 * 
 * 【功能说明】
 * 这是处理单列数值类型键（UInt8/16/32/64/128/256）的 Hash 表方法实现。
 * 用于 PrimaryTypeHashTableContext，是 Hash Join 和 Aggregation 中常用的 Hash 表类型。
 * 
 * 【核心特点】
 * - 零拷贝：直接使用列的原始数据指针，无需序列化
 * - 高效：数值类型可以直接进行 Hash 计算，无需额外处理
 * - 支持 Nullable：自动处理 Nullable 列，提取嵌套列的数据
 * 
 * 【模板参数】
 * @param FieldType 键的数值类型（UInt8/16/32/64/128/256）
 * @param TData Hash 表数据类型（如 JoinHashMap、AggHashMap 等）
 * 
 * 【使用场景】
 * - Hash Join：单列整数类型的 Join 键
 * - Aggregation：单列整数类型的 GROUP BY 键
 * 
 * 【示例】
 * - UInt64 类型的 user_id → MethodOneNumber<UInt64, JoinHashMap<UInt64, ...>>
 * - UInt32 类型的 order_id → MethodOneNumber<UInt32, JoinHashMap<UInt32, ...>>
 */
/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData>
struct MethodOneNumber : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using Base::init_iterator;
    using Base::hash_table;
    // State 类型：用于 Hash 查找和插入的状态机
    // HashMethodOneNumber 提供了单列数值键的 Hash 方法实现
    using State = ColumnsHashing::HashMethodOneNumber<typename Base::Value, typename Base::Mapped,
                                                      FieldType>;

    /**
     * 估算内存使用大小
     * 
     * 【功能说明】
     * 估算 Hash 表操作所需的内存大小，用于内存预分配和限制检查。
     * 
     * 【内存组成】
     * - Join 场景：bucket_nums 数组（存储每个键的 Hash 桶编号）
     * - Aggregation 场景：hash_values 数组（存储每个键的 Hash 值）
     * 
     * 【参数说明】
     * @param key_columns 键列指针数组（单列）
     * @param num_rows 行数
     * @param is_join 是否是 Join 操作
     * @param is_build 是否是 Build 阶段（Join 专用）
     * @param bucket_size Hash 桶大小（Join 专用）
     * @return 估算的内存大小（字节）
     */
    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        if (is_join) {
            // Join 场景：需要 bucket_nums 数组
            // bucket_nums[i] 存储键 i 的 Hash 桶编号，用于快速定位
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            // Aggregation 场景：需要 hash_values 数组
            // hash_values[i] 存储键 i 的 Hash 值，用于 Hash 表查找
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    /**
     * 初始化键（零拷贝方式）
     * 
     * 【功能说明】
     * 对于数值类型，不需要序列化，直接使用列的原始数据指针。
     * 这是零拷贝优化：避免数据复制，直接使用列的内部存储。
     * 
     * 【工作流程】
     * 1. 获取键列的原始数据指针
     *    - 如果是 Nullable 列：提取嵌套列的数据指针
     *    - 如果是非 Nullable 列：直接使用列的数据指针
     * 2. 根据操作类型初始化：
     *    - Join：计算 bucket_nums（Hash 桶编号）
     *    - Aggregation：计算 hash_values（Hash 值）
     * 
     * 【关键设计】
     * - 零拷贝：Base::keys 直接指向列的内部数据，不复制
     * - Nullable 处理：自动提取嵌套列，跳过 NULL Map
     * - 类型转换：将列的数据指针转换为 FieldType* 类型
     * 
     * 【参数说明】
     * @param key_columns 键列指针数组（单列）
     * @param num_rows 行数
     * @param null_map NULL Map（如果列是 Nullable，用于标记 NULL 值）
     * @param is_join 是否是 Join 操作
     * @param is_build 是否是 Build 阶段（Join 专用）
     * @param bucket_size Hash 桶大小（Join 专用）
     */
    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        // ===== 步骤 1：获取键列的原始数据指针（零拷贝） =====
        // 对于数值类型，不需要序列化，直接使用列的原始数据
        // 这样可以避免数据复制，提高性能
        Base::keys = (FieldType*)(key_columns[0]->is_nullable()
                                          // ===== 情况 A：Nullable 列 =====
                                          // 提取嵌套列的数据指针（跳过 NULL Map）
                                          // 例如：ColumnNullable<ColumnUInt64> → ColumnUInt64 的数据指针
                                          ? assert_cast<const ColumnNullable*>(key_columns[0])
                                                    ->get_nested_column_ptr()
                                                    ->get_raw_data()
                                                    .data
                                          // ===== 情况 B：非 Nullable 列 =====
                                          // 直接使用列的数据指针
                                          // 例如：ColumnUInt64 → 数据指针
                                          : key_columns[0]->get_raw_data().data);
        
        // ===== 步骤 2：根据操作类型初始化 =====
        if (is_join) {
            // ===== Join 场景：计算 Hash 桶编号 =====
            // bucket_nums[i] = hash(keys[i]) & (bucket_size - 1)
            // 用于快速定位键在 Hash 表中的位置
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            // ===== Aggregation 场景：计算 Hash 值 =====
            // hash_values[i] = hash(keys[i])
            // 用于 Hash 表查找和插入
            Base::init_hash_values(num_rows, null_map);
        }
    }

    /**
     * 将键插入到列中
     * 
     * 【功能说明】
     * 将 Hash 表中的键数据插入到输出列中。
     * 主要用于某些场景下需要将 Hash 表的键输出到结果列。
     * 
     * 【参数说明】
     * @param input_keys 输入的键数组
     * @param key_columns 输出的键列（可变列）
     * @param num_rows 行数
     */
    void insert_keys_into_columns(std::vector<typename Base::Key>& input_keys,
                                  MutableColumns& key_columns, const uint32_t num_rows) override {
        if (!input_keys.empty()) {
            // If size() is ​0​, data() may or may not return a null pointer.
            // 批量插入原始数据（避免逐行插入的开销）
            // 将 input_keys 数组的数据直接复制到 key_columns[0] 中
            key_columns[0]->insert_many_raw_data((char*)input_keys.data(), num_rows);
        }
    }
};

template <typename FieldType, typename TData>
struct MethodOneNumberDirect : public MethodOneNumber<FieldType, TData> {
    using Base = MethodOneNumber<FieldType, TData>;
    using Base::init_iterator;
    using Base::hash_table;
    using State = ColumnsHashing::HashMethodOneNumber<typename Base::Value, typename Base::Mapped,
                                                      FieldType>;
    FieldType _max_key;
    FieldType _min_key;

    MethodOneNumberDirect(FieldType max_key, FieldType min_key)
            : _max_key(max_key), _min_key(min_key) {}

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        Base::keys = (FieldType*)(key_columns[0]->is_nullable()
                                          ? assert_cast<const ColumnNullable*>(key_columns[0])
                                                    ->get_nested_column_ptr()
                                                    ->get_raw_data()
                                                    .data
                                          : key_columns[0]->get_raw_data().data);
        CHECK(is_join);
        CHECK_EQ(bucket_size, direct_mapping_range());
        Base::bucket_nums.resize(num_rows);

        if (null_map == nullptr) {
            if (is_build) {
                for (uint32_t k = 1; k < num_rows; ++k) {
                    Base::bucket_nums[k] = uint32_t(Base::keys[k] - _min_key + 1);
                }
            } else {
                for (uint32_t k = 0; k < num_rows; ++k) {
                    Base::bucket_nums[k] = (Base::keys[k] >= _min_key && Base::keys[k] <= _max_key)
                                                   ? uint32_t(Base::keys[k] - _min_key + 1)
                                                   : 0;
                }
            }
        } else {
            if (is_build) {
                for (uint32_t k = 1; k < num_rows; ++k) {
                    Base::bucket_nums[k] =
                            null_map[k] ? bucket_size : uint32_t(Base::keys[k] - _min_key + 1);
                }
            } else {
                for (uint32_t k = 0; k < num_rows; ++k) {
                    Base::bucket_nums[k] =
                            null_map[k] ? bucket_size
                            : (Base::keys[k] >= _min_key && Base::keys[k] <= _max_key)
                                    ? uint32_t(Base::keys[k] - _min_key + 1)
                                    : 0;
                }
            }
        }
    }

    uint32_t direct_mapping_range() override {
        // +2 to include max_key and one slot for out of range value
        return static_cast<uint32_t>(_max_key - _min_key + 2);
    }
};

template <typename TData>
struct MethodKeysFixed : public MethodBase<TData> {
    using Base = MethodBase<TData>;
    using typename Base::Key;
    using typename Base::Mapped;
    using Base::keys;
    using Base::hash_table;

    using State = ColumnsHashing::HashMethodKeysFixed<typename Base::Value, Key, Mapped>;

    // need keep until the hash probe end. use only in join
    DorisVector<Key> build_stored_keys;
    // refresh each time probe hash table
    DorisVector<Key> stored_keys;
    Sizes key_sizes;

    MethodKeysFixed(Sizes key_sizes_) : key_sizes(std::move(key_sizes_)) {}

    template <typename T>
    void pack_fixeds(size_t row_numbers, const ColumnRawPtrs& key_columns,
                     const ColumnRawPtrs& nullmap_columns, DorisVector<T>& result) {
        size_t bitmap_size = get_bitmap_size(nullmap_columns.size());
        // set size to 0 at first, then use resize to call default constructor on index included from [0, row_numbers) to reset all memory
        result.clear();
        result.resize(row_numbers);

        size_t offset = 0;
        if (bitmap_size > 0) {
            for (size_t j = 0; j < nullmap_columns.size(); j++) {
                if (!nullmap_columns[j]) {
                    continue;
                }
                size_t bucket = j / BITSIZE;
                size_t local_offset = j % BITSIZE;
                const auto& data =
                        assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                for (size_t i = 0; i < row_numbers; ++i) {
                    *((char*)(&result[i]) + bucket) |= data[i] << local_offset;
                }
            }
            offset += bitmap_size;
        }

        for (size_t j = 0; j < key_columns.size(); ++j) {
            const char* data = key_columns[j]->get_raw_data().data;

            auto foo = [&]<typename Fixed>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), key_sizes[j]);
                if (!nullmap_columns.empty() && nullmap_columns[j]) {
                    const auto& nullmap =
                            assert_cast<const ColumnUInt8&>(*nullmap_columns[j]).get_data().data();
                    for (size_t i = 0; i < row_numbers; ++i) {
                        // make sure null cell is filled by 0x0
                        memcpy_fixed<Fixed, true>(
                                (char*)(&result[i]) + offset,
                                nullmap[i] ? (char*)&zero : data + i * sizeof(Fixed));
                    }
                } else {
                    for (size_t i = 0; i < row_numbers; ++i) {
                        memcpy_fixed<Fixed, true>((char*)(&result[i]) + offset,
                                                  data + i * sizeof(Fixed));
                    }
                }
            };

            if (key_sizes[j] == sizeof(uint8_t)) {
                foo(uint8_t());
            } else if (key_sizes[j] == sizeof(uint16_t)) {
                foo(uint16_t());
            } else if (key_sizes[j] == sizeof(uint32_t)) {
                foo(uint32_t());
            } else if (key_sizes[j] == sizeof(uint64_t)) {
                foo(uint64_t());
            } else if (key_sizes[j] == sizeof(UInt128)) {
                foo(UInt128());
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "pack_fixeds input invalid key size, key_size={}", key_sizes[j]);
            }
            offset += key_sizes[j];
        }
    }

    size_t serialized_keys_size(bool is_build) const override {
        return (is_build ? build_stored_keys.size() : stored_keys.size()) *
               sizeof(typename Base::Key);
    }

    size_t estimated_size(const ColumnRawPtrs& key_columns, uint32_t num_rows, bool is_join,
                          bool is_build, uint32_t bucket_size) override {
        size_t size = 0;
        size += sizeof(StringRef) * num_rows; // stored_keys
        if (is_join) {
            size += sizeof(uint32_t) * num_rows; // bucket_nums
        } else {
            size += sizeof(size_t) * num_rows; // hash_values
        }
        return size;
    }

    void init_serialized_keys(const ColumnRawPtrs& key_columns, uint32_t num_rows,
                              const uint8_t* null_map = nullptr, bool is_join = false,
                              bool is_build = false, uint32_t bucket_size = 0) override {
        ColumnRawPtrs actual_columns;
        ColumnRawPtrs null_maps;
        actual_columns.reserve(key_columns.size());
        null_maps.reserve(key_columns.size());
        bool has_nullable_key = false;

        for (const auto& col : key_columns) {
            if (const auto* nullable_col = check_and_get_column<ColumnNullable>(col)) {
                actual_columns.push_back(&nullable_col->get_nested_column());
                null_maps.push_back(&nullable_col->get_null_map_column());
                has_nullable_key = true;
            } else {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
        if (!has_nullable_key) {
            null_maps.clear();
        }

        if (is_build) {
            pack_fixeds<Key>(num_rows, actual_columns, null_maps, build_stored_keys);
            Base::keys = build_stored_keys.data();
        } else {
            pack_fixeds<Key>(num_rows, actual_columns, null_maps, stored_keys);
            Base::keys = stored_keys.data();
        }

        if (is_join) {
            Base::init_join_bucket_num(num_rows, bucket_size, null_map);
        } else {
            Base::init_hash_values(num_rows, null_map);
        }
    }

    void insert_keys_into_columns(std::vector<typename Base::Key>& input_keys,
                                  MutableColumns& key_columns, const uint32_t num_rows) override {
        // In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = 0;
        for (size_t i = 0; i < key_columns.size(); ++i) {
            if (key_columns[i]->is_nullable()) {
                pos = get_bitmap_size(key_columns.size());
                break;
            }
        }

        for (size_t i = 0; i < key_columns.size(); ++i) {
            size_t size = key_sizes[i];
            char* data = nullptr;
            key_columns[i]->resize(num_rows);
            // If we have a nullable column, get its nested column and its null map.
            if (is_column_nullable(*key_columns[i])) {
                auto& nullable_col = assert_cast<ColumnNullable&>(*key_columns[i]);

                // nullable_col is obtained via key_columns and is itself a mutable element. However, when accessed
                // through get_raw_data().data, it yields a const char*, necessitating the use of const_cast.
                data = const_cast<char*>(nullable_col.get_nested_column().get_raw_data().data);
                UInt8* nullmap = assert_cast<ColumnUInt8*>(&nullable_col.get_null_map_column())
                                         ->get_data()
                                         .data();

                // The current column is nullable. Check if the value of the
                // corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / BITSIZE;
                size_t offset = i % BITSIZE;
                for (size_t j = 0; j < num_rows; j++) {
                    nullmap[j] =
                            (reinterpret_cast<const UInt8*>(&input_keys[j])[bucket] >> offset) & 1;
                }
            } else {
                // key_columns is a mutable element. However, when accessed through get_raw_data().data,
                // it yields a const char*, necessitating the use of const_cast.
                data = const_cast<char*>(key_columns[i]->get_raw_data().data);
            }

            auto foo = [&]<typename Fixed>(Fixed zero) {
                CHECK_EQ(sizeof(Fixed), size);
                for (size_t j = 0; j < num_rows; j++) {
                    memcpy_fixed<Fixed, true>(data + j * sizeof(Fixed),
                                              (char*)(&input_keys[j]) + pos);
                }
            };

            if (size == sizeof(uint8_t)) {
                foo(uint8_t());
            } else if (size == sizeof(uint16_t)) {
                foo(uint16_t());
            } else if (size == sizeof(uint32_t)) {
                foo(uint32_t());
            } else if (size == sizeof(uint64_t)) {
                foo(uint64_t());
            } else if (size == sizeof(UInt128)) {
                foo(UInt128());
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "pack_fixeds input invalid key size, key_size={}", size);
            }

            pos += size;
        }
    }
};

template <typename Base>
struct DataWithNullKeyImpl : public Base {
    bool& has_null_key_data() { return has_null_key; }
    bool has_null_key_data() const { return has_null_key; }
    template <typename MappedType>
    MappedType& get_null_key_data() const {
        return (MappedType&)null_key_data;
    }
    size_t size() const { return Base::size() + has_null_key; }
    bool empty() const { return Base::empty() && !has_null_key; }

    void clear() {
        Base::clear();
        has_null_key = false;
    }

    void clear_and_shrink() {
        Base::clear_and_shrink();
        has_null_key = false;
    }

protected:
    bool has_null_key = false;
    Base::Value null_key_data;
};

template <typename Base>
struct DataWithNullKey : public DataWithNullKeyImpl<Base> {};

template <IteratoredMap Base>
struct DataWithNullKey<Base> : public DataWithNullKeyImpl<Base> {
    using DataWithNullKeyImpl<Base>::null_key_data;
    using DataWithNullKeyImpl<Base>::has_null_key;

    struct Iterator {
        typename Base::iterator base_iterator = {};
        bool current_null = false;
        Base::Value* null_key_data = nullptr;

        Iterator() = default;
        Iterator(typename Base::iterator it, bool null, Base::Value* null_key)
                : base_iterator(it), current_null(null), null_key_data(null_key) {}
        bool operator==(const Iterator& rhs) const {
            return current_null == rhs.current_null && base_iterator == rhs.base_iterator;
        }

        bool operator!=(const Iterator& rhs) const { return !(*this == rhs); }

        Iterator& operator++() {
            if (current_null) {
                current_null = false;
            } else {
                ++base_iterator;
            }
            return *this;
        }

        Base::Value& get_second() {
            if (current_null) {
                return *null_key_data;
            } else {
                return base_iterator->get_second();
            }
        }
    };

    Iterator begin() { return {Base::begin(), has_null_key, &null_key_data}; }

    Iterator end() { return {Base::end(), false, &null_key_data}; }

    void insert(const Iterator& other_iter) {
        if (other_iter.current_null) {
            has_null_key = true;
            null_key_data = *other_iter.null_key_data;
        } else {
            Base::insert(other_iter.base_iterator);
        }
    }

    using iterator = Iterator;
};

/// Single low cardinality column.
template <typename SingleColumnMethod>
struct MethodSingleNullableColumn : public SingleColumnMethod {
    using Base = SingleColumnMethod;
    using State = ColumnsHashing::HashMethodSingleLowNullableColumn<typename Base::State,
                                                                    typename Base::Mapped>;
    void insert_keys_into_columns(std::vector<typename Base::Key>& input_keys,
                                  MutableColumns& key_columns, const uint32_t num_rows) override {
        auto* col = key_columns[0].get();
        col->reserve(num_rows);
        if (input_keys.empty()) {
            // If size() is ​0​, data() may or may not return a null pointer.
            return;
        }
        if constexpr (std::is_same_v<typename Base::Key, StringRef>) {
            col->insert_many_strings(input_keys.data(), num_rows);
        } else {
            col->insert_many_raw_data(reinterpret_cast<char*>(input_keys.data()), num_rows);
        }
    }
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized