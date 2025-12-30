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
//
// This file is copied from
// https://github.com/apache/impala/blob/master/be/src/util/lru-multi-cache.h
// and modified by Doris

#pragma once

#include <boost/intrusive/list.hpp>  // 侵入式链表，用于高效的 LRU 操作
#include <list>                      // 标准链表，用于存储同键的多个值
#include <mutex>                     // 互斥锁，保证线程安全
#include <unordered_map>             // 哈希表，用于 O(1) 的键查找

namespace doris {

/// LruMultiCache 是一个基于 std::unordered_map 与 boost::intrusive::list 的线程安全 LRU 缓存。
///
/// 特性
///   - 同一个键可以存储多个对象
///   - 为最新可用的对象提供"唯一访问"
///   - 通过软容量限制缓存对象数量
///   - 提供 EvictOlderThan()，按时间淘汰长时间未使用的对象
///   - O(1) 操作（EvictOlderThan() 例外，可能触发多次驱逐）
///   - 通过 RAII 的 Accessor 自动归还对象
///   - 也支持通过 Accessor 显式 release/destroy
///   - 通过移除空的拥有链表避免老化问题
///
/// 限制
///   - 需要确保缓存的生命周期长于所有 Accessor
///   - 缓存无法强制严格容量限制：若对象都在使用中且继续 emplace，会暂时超出容量；
///   释放时会触发驱逐
///   - 对于简单场景可能略显"重"：例如无需线程安全、无需唯一访问、无需多值存储、
///   无需基于时间戳的淘汰。它最初用于存储 HdfsFileHandle
///   - 仅支持对象的 emplace（构造即插入），构造期间会对缓存加锁
///   - 内存利用效率一般
///
/// 设计
///          _ _ _ _ _
///         |_|_|_|_|_|     unordered_map 的桶
///          |       |
///       _ _|    _ _|_
///      |0|1|   |0|0|1|    拥有链表，0 表示可用，1 表示在用
///        \\     / /
///       (LRU 计算)
///               |
///        _    _ V  _
///       |0|->|0|->|0|     LRU 链表
///
///
///
///   Cache
///     - std::unordered_map 保存扩展过内部信息的 std::list，这些 list 是"拥有者"
///     - 拥有链表只对"可用对象"维护 LRU 顺序（"在用对象"的顺序无关紧要），
///     以支持 O(1) 的 get；若存在可用对象，最久未使用的对象位于链表头
///     - boost::intrusive::list 维护"可用对象"的 LRU 链表，这是一个非拥有的侵入式链表，
///     用于驱逐
///
///   Accessor
///     - RAII 对象，提供对缓存对象的唯一访问，并在析构时自动归还
///     - 也支持显式 release 与 destroy
///

template <typename KeyType, typename ValueType>
class LruMultiCache {
public:
    /// 定义在底部，因为它使用了私有作用域的类型定义
    class Accessor;

    /// 构造函数，初始化缓存容量
    /// @param capacity 缓存的最大容量，0 表示无限制
    explicit LruMultiCache(size_t capacity = 0);

    /// 禁用移动构造，确保缓存位置固定，因为引用用于 release/destroy
    LruMultiCache(LruMultiCache&&) = delete;
    LruMultiCache& operator=(LruMultiCache&&) = delete;

    /// 禁用拷贝构造和赋值，避免浅拷贝问题
    LruMultiCache(const LruMultiCache&) = delete;
    const LruMultiCache& operator=(const LruMultiCache&) = delete;

    /// 返回存储的对象数量，O(1) 时间复杂度
    size_t size();

    /// 返回拥有的链表数量，O(1) 时间复杂度
    size_t number_of_keys();

    /// 设置新的缓存容量
    /// @param new_capacity 新的容量限制
    void set_capacity(size_t new_capacity);

    /// 获取指定键的最新可用对象的唯一访问器
    /// @param key 要查找的键
    /// @return 如果找到可用对象则返回 Accessor，否则返回空的 Accessor
    [[nodiscard]] Accessor get(const KeyType& key);

    /// 构造并插入新对象，返回其唯一访问器
    /// 使用变参模板转发其余参数到存储对象的构造函数
    /// @param key 要存储的键
    /// @param args 传递给 ValueType 构造函数的参数
    /// @return 新创建对象的 Accessor
    template <typename... Args>
    [[nodiscard]] Accessor emplace_and_get(const KeyType& key, Args&&... args);

    /// 驱逐释放时间过旧的可用对象
    /// @param oldest_allowed_timestamp 允许的最旧时间戳（秒）
    void evict_older_than(uint64_t oldest_allowed_timestamp);

    /// 返回可用对象数量，O(n) 复杂度，仅用于测试
    size_t number_of_available_objects();

    /// 强制重新哈希，增加桶数量，仅用于测试
    void rehash();

private:
    /// 双向链表钩子，使用 auto_unlink 模式实现 O(1) 的 LRU 链表移除操作
    /// 在 get 和 evict 操作中使用
    using link_type = boost::intrusive::list_member_hook<
            boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

    /// 内部类型，存储 O(1) 操作所需的所有信息
    struct ValueType_internal {
        using Container_internal = std::list<ValueType_internal>;

        /// 构造函数，使用变参模板支持 emplace 操作
        /// @param cache 缓存引用
        /// @param key 键的引用
        /// @param container 容器引用
        /// @param args 传递给 ValueType 构造函数的参数
        template <typename... Args>
        explicit ValueType_internal(LruMultiCache& cache, const KeyType& key,
                                    Container_internal& container, Args&&... args);

        /// 检查对象是否可用（未被访问器持有）
        bool is_available();

        /// LRU 链表的成员钩子
        link_type member_hook;

        /// 缓存引用，用于 release/destroy 操作
        /// std::unordered_map::iterators 在 rehash 时失效，但引用是稳定的
        LruMultiCache& cache;

        /// 键的引用，用于在驱逐或销毁最后一个元素时移除空的拥有链表
        const KeyType& key;

        /// 容器引用和迭代器，用于 release/destroy 操作
        /// 可以通过键来避免容器引用，但这样更快，避免了一次哈希调用
        Container_internal& container;
        typename Container_internal::iterator it;

        /// 用户想要缓存的实际对象
        ValueType value;

        /// 对象最后释放的时间戳（秒），仅用于 EvictOlderThan()
        uint64_t timestamp_seconds;
    };

    /// 拥有链表的类型定义
    using Container = std::list<ValueType_internal>;

    /// 哈希表类型定义
    using HashTableType = std::unordered_map<KeyType, Container>;

    /// 成员钩子选项，用于侵入式链表
    using MemberHookOption = boost::intrusive::member_hook<ValueType_internal, link_type,
                                                           &ValueType_internal::member_hook>;

    /// LRU 链表类型，不使用常量时间大小以支持自取消链接，缓存大小由类跟踪
    using LruListType = boost::intrusive::list<ValueType_internal, MemberHookOption,
                                               boost::intrusive::constant_time_size<false>>;

    /// 释放对象，将其标记为可用并重新加入 LRU 链表
    /// @param p_value_internal 要释放的内部值对象指针
    void release(ValueType_internal* p_value_internal);
    
    /// 销毁对象，从缓存中完全移除
    /// @param p_value_internal 要销毁的内部值对象指针
    void destroy(ValueType_internal* p_value_internal);

    /// 内部函数：如果对象变为可用但缓存超出容量，则移除一个对象
    void _evict_one_if_needed();

    /// 内部函数：用于 EvictOlderThan() 的单个对象驱逐
    /// @param value_internal 要驱逐的内部值对象引用
    void _evict_one(ValueType_internal& value_internal);

    /// 哈希表，存储键到拥有链表的映射
    HashTableType _hash_table;
    
    /// LRU 链表，维护可用对象的最近最少使用顺序
    LruListType _lru_list;

    /// 缓存容量限制
    size_t _capacity;
    
    /// 当前存储的对象数量
    size_t _size;

    /// 保护缓存访问的互斥锁
    /// 不需要读写锁，因为没有昂贵的纯读操作
    std::mutex _lock;

public:
    /// RAII 访问器，提供对缓存对象的唯一访问
    class Accessor {
    public:
        /// 默认构造，用于支持作为输入/输出参数使用
        /// @param p_value_internal 内部值对象指针，默认为 nullptr
        Accessor(ValueType_internal* p_value_internal = nullptr);

        /// 移动构造，类似 unique_ptr 的接口，只能移动
        Accessor(Accessor&&);
        Accessor& operator=(Accessor&&);

        /// 禁用拷贝构造和赋值
        Accessor(const Accessor&) = delete;
        const Accessor& operator=(const Accessor&) = delete;

        /// 析构函数中自动释放对象
        ~Accessor();

        /// 返回指向用户对象的指针
        /// @return 如果是空访问器则返回 nullptr
        ValueType* get();

        /// Returns a pointer to the stored key;
        /// Returns nullptr if it's an empty accessor;
        const KeyType* get_key() const;

        /// 显式释放对象
        void release();

        /// 显式销毁对象
        void destroy();

    private:
        /// 内部值对象指针
        ValueType_internal* _p_value_internal = nullptr;
    };
};

} // namespace doris