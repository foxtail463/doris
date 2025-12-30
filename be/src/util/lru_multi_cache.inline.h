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
// https://github.com/apache/impala/blob/master/be/src/util/lru-multi-cache.inline.h
// and modified by Doris

#pragma once

#include <glog/logging.h>

#include "util/hash_util.hpp" // IWYU pragma: keep
#include "util/lru_multi_cache.h"
#include "util/time.h"

namespace doris {

// ============================================================================
// ValueType_internal 构造函数实现
// ============================================================================

/// 构造函数，使用变参模板支持 emplace 操作
/// @param cache 缓存引用，用于后续的释放和销毁操作
/// @param key 键的引用，用于在驱逐时找到对应的哈希表条目
/// @param container 容器引用，用于快速定位和删除
/// @param args 传递给 ValueType 构造函数的参数，使用完美转发
template <typename KeyType, typename ValueType>
template <typename... Args>
LruMultiCache<KeyType, ValueType>::ValueType_internal::ValueType_internal(
        LruMultiCache& cache, const KeyType& key, Container_internal& container, Args&&... args)
        : cache(cache),                                    // 初始化缓存引用
          key(key),                                        // 初始化键引用
          container(container),                            // 初始化容器引用
          value(std::forward<Args>(args)...),             // 使用完美转发构造用户对象
          timestamp_seconds(MonotonicSeconds()) {}        // 初始化时间戳为当前时间

// ============================================================================
// ValueType_internal 方法实现
// ============================================================================

/// 检查对象是否可用（未被访问器持有）
/// @return true 如果对象在 LRU 链表中（可用），false 如果被访问器持有（在用）
template <typename KeyType, typename ValueType>
bool LruMultiCache<KeyType, ValueType>::ValueType_internal::is_available() {
    return member_hook.is_linked();
}

// ============================================================================
// Accessor 类实现
// ============================================================================

/// 构造函数，创建访问器
/// @param p_value_internal 内部值对象指针，默认为 nullptr
template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::Accessor(ValueType_internal* p_value_internal)
        : _p_value_internal(p_value_internal) {}

/// 移动构造函数，类似 unique_ptr 的接口
/// @param rhs 要移动的访问器
template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::Accessor(Accessor&& rhs) {
    _p_value_internal = std::move(rhs._p_value_internal);
    rhs._p_value_internal = nullptr;  // 清空源对象的指针
}

/// 移动赋值操作符
/// @param rhs 要移动的访问器
/// @return 当前访问器的引用
template <typename KeyType, typename ValueType>
auto LruMultiCache<KeyType, ValueType>::Accessor::operator=(Accessor&& rhs) -> Accessor& {
    _p_value_internal = std::move(rhs._p_value_internal);
    rhs._p_value_internal = nullptr;  // 清空源对象的指针
    return (*this);
}

/// 析构函数，自动释放对象
template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::Accessor::~Accessor() {
    release();
}

/// 获取指向用户对象的指针
/// @return 如果访问器有效则返回用户对象指针，否则返回 nullptr
template <typename KeyType, typename ValueType>
ValueType* LruMultiCache<KeyType, ValueType>::Accessor::get() {
    if (_p_value_internal) {
        return &(_p_value_internal->value);
    }
    return nullptr;
}

/// 获取指向存储键的指针
/// @return 如果访问器有效则返回键指针，否则返回 nullptr
template <typename KeyType, typename ValueType>
const KeyType* LruMultiCache<KeyType, ValueType>::Accessor::get_key() const {
    if (_p_value_internal) {
        return &(_p_value_internal->key);
    }
    return nullptr;
}

/// 显式释放对象，将其标记为可用并重新加入 LRU 链表
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Accessor::release() {
    /// 空指针检查，因为需要解引用获取缓存引用
    /// LruMultiCache::Release() 内部不需要空指针检查
    if (_p_value_internal) {
        LruMultiCache& cache = _p_value_internal->cache;
        cache.release(_p_value_internal);
        _p_value_internal = nullptr;  // 清空指针，避免重复释放
    }
}

/// 显式销毁对象，从缓存中完全移除
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::Accessor::destroy() {
    /// 空指针检查，因为需要解引用获取缓存引用
    /// LruMultiCache::destroy() 内部不需要空指针检查
    if (_p_value_internal) {
        LruMultiCache& cache = _p_value_internal->cache;
        cache.destroy(_p_value_internal);
        _p_value_internal = nullptr;  // 清空指针，避免重复销毁
    }
}

// ============================================================================
// LruMultiCache 主要方法实现
// ============================================================================

/// 构造函数，初始化缓存容量
/// @param capacity 缓存的最大容量，0 表示无限制
template <typename KeyType, typename ValueType>
LruMultiCache<KeyType, ValueType>::LruMultiCache(size_t capacity) : _capacity(capacity), _size(0) {}

/// 返回存储的对象数量，O(1) 时间复杂度
/// @return 当前缓存中的对象总数
template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::size() {
    std::lock_guard<std::mutex> g(_lock);
    return _size;
}

/// 返回拥有的链表数量，O(1) 时间复杂度
/// @return 哈希表中的键数量
template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::number_of_keys() {
    std::lock_guard<std::mutex> g(_lock);
    return _hash_table.size();
}

/// 设置新的缓存容量
/// @param new_capacity 新的容量限制
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::set_capacity(size_t new_capacity) {
    std::lock_guard<std::mutex> g(_lock);
    _capacity = new_capacity;
}

/// 获取指定键的最新可用对象的唯一访问器
/// @param key 要查找的键
/// @return 如果找到可用对象则返回 Accessor，否则返回空的 Accessor
template <typename KeyType, typename ValueType>
auto LruMultiCache<KeyType, ValueType>::get(const KeyType& key) -> Accessor {
    // 获取互斥锁，保证线程安全
    std::lock_guard<std::mutex> g(_lock);
    
    // 在哈希表中查找指定键对应的拥有链表
    auto hash_table_it = _hash_table.find(key);

    // 没有找到对应的拥有链表，调用者需要使用 EmplaceAndGet() 创建新对象
    if (hash_table_it == _hash_table.end()) {
        return Accessor();
    }

    // 获取该键对应的拥有链表引用
    Container& container = hash_table_it->second;

    // 空的容器会被自动删除，所以这里应该不为空
    DCHECK(!container.empty());

    // 所有可用元素都在链表前面，只需要检查第一个元素
    // 这是因为可用对象会按 LRU 顺序排列，最久未使用的在链表头
    auto container_it = container.begin();

    // 如果第一个对象不可用（正在被使用），说明没有可用对象
    // 调用者需要使用 EmplaceAndGet() 创建新对象
    if (!container_it->is_available()) {
        return Accessor();
    }

    // 将对象移动到拥有链表的末尾，因为它不再可用（即将被访问器持有）
    // splice 操作是 O(1) 的，不会复制对象，只是改变指针
    container.splice(container.end(), container, container_it);

    // 从 LRU 链表中移除该元素，因为它不再可用
    // unlink() 是 O(1) 操作，会自动从侵入式链表中移除
    container_it->member_hook.unlink();

    // 返回指向该对象的访问器，访问器会持有该对象直到被释放
    return Accessor(&(*container_it));
}

/// 构造并插入新对象，返回其唯一访问器
/// @param key 要存储的键
/// @param args 传递给 ValueType 构造函数的参数
/// @return 新创建对象的 Accessor
template <typename KeyType, typename ValueType>
template <typename... Args>
auto LruMultiCache<KeyType, ValueType>::emplace_and_get(const KeyType& key, Args&&... args)
        -> Accessor {
    std::lock_guard<std::mutex> g(_lock);

    // 如果不存在则创建默认容器
    Container& container = _hash_table[key];

    // 获取存储在 unordered_map 中的键的引用，参数可能是临时对象
    // 但 std::unordered_map 有稳定的引用
    const KeyType& stored_key = _hash_table.find(key)->first;

    // 将其作为拥有链表的最后一个条目，因为它刚刚被保留
    auto container_it = container.emplace(container.end(), (*this), stored_key, container,
                                          std::forward<Args>(args)...);

    // 只能在 emplace 之后设置这个
    container_it->it = container_it;

    _size++;

    // 如果缓存超出容量，需要移除最旧的可用对象
    _evict_one_if_needed();

    return Accessor(&(*container_it));
}

/// 释放对象，将其标记为可用并重新加入 LRU 链表
/// @param p_value_internal 要释放的内部值对象指针
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::release(ValueType_internal* p_value_internal) {
    std::lock_guard<std::mutex> g(_lock);

    // 这只能由访问器使用，访问器已经检查了空指针
    DCHECK(p_value_internal);

    // 必须当前不可用
    DCHECK(!p_value_internal->is_available());

    // 释放时更新时间戳
    // 因为我们将在一定时间后驱逐缓存值
    p_value_internal->timestamp_seconds = MonotonicSeconds();

    Container& container = p_value_internal->container;

    // 将对象移动到前面，在拥有链表中也保持 LRU 关系
    // 以便能够老化未使用的对象
    container.splice(container.begin(), container, p_value_internal->it);

    // 将对象添加到 LRU 链表中，因为它现在可用于使用
    _lru_list.push_front(container.front());

    // 以防我们已经超出容量，缓存可以驱逐最旧的对象
    _evict_one_if_needed();
}

/// 销毁对象，从缓存中完全移除
/// @param p_value_internal 要销毁的内部值对象指针
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::destroy(ValueType_internal* p_value_internal) {
    std::lock_guard<std::mutex> g(_lock);

    // 这只能由访问器使用，访问器已经检查了空指针
    DCHECK(p_value_internal);

    // 必须当前不可用
    DCHECK(!p_value_internal->is_available());

    Container& container = p_value_internal->container;

    if (container.size() == 1) {
        // 最后一个元素，可以移除拥有链表以防止老化
        _hash_table.erase(p_value_internal->key);
    } else {
        // 从拥有链表中移除
        container.erase(p_value_internal->it);
    }

    _size--;
}

/// 返回可用对象数量，O(n) 复杂度，仅用于测试
/// @return LRU 链表中可用对象的数量
template <typename KeyType, typename ValueType>
size_t LruMultiCache<KeyType, ValueType>::number_of_available_objects() {
    std::lock_guard<std::mutex> g(_lock);
    return _lru_list.size();
}

/// 强制重新哈希，增加桶数量，仅用于测试
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::rehash() {
    std::lock_guard<std::mutex> g(_lock);
    _hash_table.rehash(_hash_table.bucket_count() + 1);
}

// ============================================================================
// 内部辅助方法实现
// ============================================================================

/// 内部函数：用于 EvictOlderThan() 的单个对象驱逐
/// @param value_internal 要驱逐的内部值对象引用
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::_evict_one(ValueType_internal& value_internal) {
    // std::mutex 由调用驱逐函数的调用者锁定
    // _lock.DCheckLocked();

    // 必须可用才能驱逐
    DCHECK(value_internal.is_available());

    // 从 LRU 缓存中移除
    value_internal.member_hook.unlink();

    Container& container = value_internal.container;

    if (container.size() == 1) {
        // 最后一个元素，可以移除拥有链表以防止老化
        _hash_table.erase(value_internal.key);
    } else {
        // 从拥有链表中移除
        container.erase(value_internal.it);
    }

    _size--;
}

/// 内部函数：如果对象变为可用但缓存超出容量，则移除一个对象
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::_evict_one_if_needed() {
    // std::mutex 由调用公共函数的调用者锁定
    // _lock.DCheckLocked();

    if (!_lru_list.empty() && _size > _capacity) {
        _evict_one(_lru_list.back());  // 驱逐最旧的对象
    }
}

/// 驱逐释放时间过旧的可用对象
/// @param oldest_allowed_timestamp 允许的最旧时间戳（秒）
template <typename KeyType, typename ValueType>
void LruMultiCache<KeyType, ValueType>::evict_older_than(uint64_t oldest_allowed_timestamp) {
    std::lock_guard<std::mutex> g(_lock);

    // 在以下情况下停止驱逐：
    //   - 没有更多可用（即可驱逐）对象
    //   - 缓存大小低于容量且最旧对象不早于限制
    while (!_lru_list.empty() &&
           (_size > _capacity || _lru_list.back().timestamp_seconds < oldest_allowed_timestamp)) {
        _evict_one(_lru_list.back());
    }
}

} // namespace doris