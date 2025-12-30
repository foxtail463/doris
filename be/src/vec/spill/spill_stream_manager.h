
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
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "olap/options.h"
#include "util/metrics.h"
#include "util/threadpool.h"
#include "vec/spill/spill_stream.h"
namespace doris {
#include "common/compile_check_begin.h"
class RuntimeProfile;
template <typename T>
class AtomicCounter;
using IntAtomicCounter = AtomicCounter<int64_t>;
template <typename T>
class AtomicGauge;
using UIntGauge = AtomicGauge<uint64_t>;
class MetricEntity;
struct MetricPrototype;

namespace vectorized {

class SpillStreamManager;

/**
 * SpillDataDir - Spill 数据目录管理器
 * 
 * 负责管理单个磁盘目录的 spill 数据存储，包括：
 * - 磁盘容量监控和限制检查
 * - Spill 数据使用量统计
 * - 提供 spill 数据路径和 GC 路径
 * - 监控指标管理
 */
class SpillDataDir {
public:
    /**
     * 构造函数
     * @param path 磁盘路径
     * @param capacity_bytes 磁盘容量（字节）
     * @param storage_medium 存储介质类型（SSD/HDD），默认为 HDD
     */
    SpillDataDir(std::string path, int64_t capacity_bytes,
                 TStorageMedium::type storage_medium = TStorageMedium::HDD);

    /**
     * 初始化 SpillDataDir
     * - 检查路径是否存在
     * - 更新磁盘容量信息
     * - 注册监控指标
     */
    Status init();

    /**
     * 获取磁盘路径
     */
    const std::string& path() const { return _path; }

    /**
     * 获取 spill 数据路径
     * @param query_id 查询 ID，如果为空则返回根路径
     * @return 路径格式: {path}/spill/{query_id}
     */
    std::string get_spill_data_path(const std::string& query_id = "") const;

    /**
     * 获取 spill GC 路径
     * @param sub_dir_name 子目录名称，如果为空则返回根路径
     * @return 路径格式: {path}/spill_gc/{sub_dir_name}
     */
    std::string get_spill_data_gc_path(const std::string& sub_dir_name = "") const;

    /**
     * 获取存储介质类型（SSD/HDD）
     */
    TStorageMedium::type storage_medium() const { return _storage_medium; }

    /**
     * 检查在添加指定大小的数据后是否会达到容量限制
     * 检查双重限制：
     * 1. 磁盘容量限制（使用率超过阈值或剩余容量不足）
     * 2. Spill 数据限制（spill 数据总量超过配置限制）
     * 
     * @param incoming_data_size 即将添加的数据大小（字节）
     * @return true 表示达到限制，false 表示未达到限制
     */
    bool reach_capacity_limit(int64_t incoming_data_size);

    /**
     * 更新磁盘容量信息
     * - 获取当前磁盘空间信息
     * - 重新计算 spill 数据限制
     * - 更新监控指标
     */
    Status update_capacity();

    /**
     * 更新 spill 数据使用量
     * 线程安全，使用 mutex 保护
     * 
     * @param incoming_data_size 新增的数据大小（字节），可以为负数（表示删除）
     */
    void update_spill_data_usage(int64_t incoming_data_size) {
        std::lock_guard<std::mutex> l(_mutex);
        _spill_data_bytes += incoming_data_size;
        spill_disk_data_size->set_value(_spill_data_bytes);
    }

    /**
     * 获取当前 spill 数据使用量（字节）
     */
    int64_t get_spill_data_bytes() {
        std::lock_guard<std::mutex> l(_mutex);
        return _spill_data_bytes;
    }

    /**
     * 获取 spill 数据限制（字节）
     */
    int64_t get_spill_data_limit() {
        std::lock_guard<std::mutex> l(_mutex);
        return _spill_data_limit_bytes;
    }

    /**
     * 生成调试信息字符串
     * 包含路径、容量、限制、使用量、可用空间等信息
     */
    std::string debug_string();

private:
    /**
     * 检查磁盘容量限制（不考虑 spill 数据限制）
     * 检查条件：
     * - 磁盘使用率 >= storage_flood_stage_usage_percent
     * - 且剩余容量 <= storage_flood_stage_left_capacity_bytes
     * 
     * @param incoming_data_size 即将添加的数据大小（字节）
     * @return true 表示达到磁盘容量限制
     */
    bool _reach_disk_capacity_limit(int64_t incoming_data_size);
    
    /**
     * 计算磁盘使用率
     * @param incoming_data_size 即将添加的数据大小（字节）
     * @return 使用率（0.0 - 1.0）
     */
    double _get_disk_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

    friend class SpillStreamManager;
    
    std::string _path;  // 磁盘路径

    // 保护以下字段的并发访问：_disk_capacity_bytes, _available_bytes, 
    // _spill_data_limit_bytes, _spill_data_bytes
    std::mutex _mutex;
    
    size_t _disk_capacity_bytes;           // 磁盘总容量（字节）
    int64_t _spill_data_limit_bytes = 0;  // Spill 数据限制（字节）
    size_t _available_bytes = 0;            // 磁盘可用容量（字节）
    int64_t _spill_data_bytes = 0;         // 当前 spill 数据使用量（字节）
    TStorageMedium::type _storage_medium;  // 存储介质类型（SSD/HDD）

    // 监控指标相关
    std::shared_ptr<MetricEntity> spill_data_dir_metric_entity;
    IntGauge* spill_disk_capacity = nullptr;        // 磁盘总容量指标
    IntGauge* spill_disk_limit = nullptr;          // Spill 数据限制指标
    IntGauge* spill_disk_avail_capacity = nullptr; // 磁盘可用容量指标
    IntGauge* spill_disk_data_size = nullptr;      // Spill 数据使用量指标
    IntGauge* spill_disk_has_spill_data = nullptr;      // 是否有活跃 spill 数据（0/1）
    IntGauge* spill_disk_has_spill_gc_data = nullptr; // 是否有待 GC 数据（0/1）
};

/**
 * SpillStreamManager - Spill 流管理器
 * 
 * 全局单例，负责管理所有 spill 相关的操作：
 * - 管理多个 SpillDataDir（磁盘目录）
 * - 创建和注册 SpillStream
 * - 后台 GC 线程清理过期文件
 * - 监控 spill 读写统计
 * - 磁盘选择策略（优先 SSD，使用率低优先）
 */
class SpillStreamManager {
public:
    ~SpillStreamManager();
    
    /**
     * 构造函数
     * @param spill_store_map spill 存储目录映射表（路径 -> SpillDataDir）
     */
    SpillStreamManager(std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>>&&
                               spill_store_map);

    /**
     * 初始化 SpillStreamManager
     * - 初始化所有 SpillDataDir
     * - 创建 spill 和 spill_gc 目录
     * - 启动 GC 后台线程
     * - 初始化监控指标
     */
    Status init();

    /**
     * 停止 SpillStreamManager
     * - 停止 GC 线程
     * - 等待 GC 线程结束
     */
    void stop() {
        _stop_background_threads_latch.count_down();
        if (_spill_gc_thread) {
            _spill_gc_thread->join();
        }
    }

    /**
     * 创建 SpillStream 并登记
     * 
     * 流程：
     * 1. 选择可用磁盘（优先 SSD，使用率低优先）
     * 2. 创建 spill 目录: {spill_root}/{query_id}/{operator_name}-{node_id}-{task_id}-{stream_id}
     * 3. 创建 SpillStream 对象
     * 4. 调用 SpillStream::prepare() 初始化
     * 
     * @param state 运行时状态
     * @param spill_stream 输出的 SpillStream 智能指针
     * @param query_id 查询 ID
     * @param operator_name 操作符名称（如 "hash_build_sink_0", "sort"）
     * @param node_id Plan Node ID
     * @param batch_rows 批次行数
     * @param batch_bytes 批次字节数
     * @param operator_profile 操作符性能分析器
     */
    Status register_spill_stream(RuntimeState* state, SpillStreamSPtr& spill_stream,
                                 const std::string& query_id, const std::string& operator_name,
                                 int32_t node_id, int32_t batch_rows, size_t batch_bytes,
                                 RuntimeProfile* operator_profile);

    /**
     * 标记 SpillStream 需要被删除
     * 实际删除操作在 GC 线程中异步执行
     * 
     * @param spill_stream 要删除的 SpillStream
     */
    void delete_spill_stream(SpillStreamSPtr spill_stream);

    /**
     * 执行垃圾回收
     * 清理 spill_gc 目录下的过期文件
     * 
     * @param max_work_time_ms 最大工作时间（毫秒），避免长时间阻塞
     */
    void gc(int32_t max_work_time_ms);

    /**
     * 更新 spill 写入字节数统计
     * @param bytes 写入的字节数
     */
    void update_spill_write_bytes(int64_t bytes) { _spill_write_bytes_counter->increment(bytes); }

    /**
     * 更新 spill 读取字节数统计
     * @param bytes 读取的字节数
     */
    void update_spill_read_bytes(int64_t bytes) { _spill_read_bytes_counter->increment(bytes); }

private:
    /**
     * 初始化监控指标
     * 注册 spill_write_bytes 和 spill_read_bytes 指标
     */
    void _init_metrics();
    
    /**
     * 初始化 spill 存储目录映射表
     * 调用每个 SpillDataDir 的 init() 方法
     */
    Status _init_spill_store_map();
    
    /**
     * GC 线程回调函数
     * 定期执行 GC 和容量更新
     */
    void _spill_gc_thread_callback();
    
    /**
     * 获取可用于 spill 的存储目录列表
     * 
     * 选择策略：
     * 1. 过滤指定存储介质类型（SSD/HDD）
     * 2. 排除已达到容量限制的目录
     * 3. 按使用率排序，优先返回使用率低的目录
     * 
     * @param storage_medium 存储介质类型
     * @return 可用目录列表（按使用率从低到高排序）
     */
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);

    // Spill 存储目录映射表：路径 -> SpillDataDir
    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;

    // 用于控制后台线程停止的信号量
    CountDownLatch _stop_background_threads_latch;
    
    // GC 后台线程
    std::shared_ptr<Thread> _spill_gc_thread;

    // SpillStream ID 生成器（原子递增）
    std::atomic_uint64_t id_ = 0;

    // 监控指标实体
    std::shared_ptr<MetricEntity> _entity {nullptr};

    // Spill 写入字节数指标原型
    std::unique_ptr<doris::MetricPrototype> _spill_write_bytes_metric {nullptr};
    // Spill 读取字节数指标原型
    std::unique_ptr<doris::MetricPrototype> _spill_read_bytes_metric {nullptr};

    // Spill 写入字节数计数器
    IntAtomicCounter* _spill_write_bytes_counter {nullptr};
    // Spill 读取字节数计数器
    IntAtomicCounter* _spill_read_bytes_counter {nullptr};
};
} // namespace vectorized
} // namespace doris
#include "common/compile_check_end.h"
