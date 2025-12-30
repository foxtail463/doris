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
#include <future>
#include <memory>

#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeProfile;
class ThreadPool;

namespace vectorized {

class Block;
class SpillDataDir;

/**
 * SpillStream - Spill 流
 * 
 * 封装单个 spill 流的读写操作，是操作符与磁盘存储之间的桥梁。
 * 
 * 主要职责：
 * - 管理 SpillWriter 和 SpillReader 的生命周期
 * - 提供 spill_block() 接口写入数据到磁盘
 * - 提供 read_next_block_sync() 接口从磁盘读取数据
 * - 管理 spill 目录的创建和清理
 * - 维护写入字节数统计
 * 
 * 使用流程：
 * 1. 创建 SpillStream（由 SpillStreamManager::register_spill_stream() 创建）
 * 2. 调用 prepare() 初始化（创建 Writer 和 Reader）
 * 3. 调用 spill_block() 写入数据
 * 4. 调用 spill_eof() 结束写入
 * 5. 调用 read_next_block_sync() 读取数据
 * 6. 析构时自动调用 gc() 清理文件
 */
class SpillStream {
public:
    /**
     * 最小 spill 写入批次内存大小（32KB）
     * 用于避免频繁的小文件写入，提高 I/O 效率
     * 只有当 block 大小 >= MIN_SPILL_WRITE_BATCH_MEM 时才会 spill
     */
    static constexpr size_t MIN_SPILL_WRITE_BATCH_MEM = 32 * 1024;
    
    /**
     * 最大 spill 写入批次内存大小（32MB）
     * 用于限制单次写入的最大数据量，避免内存占用过高
     */
    static constexpr size_t MAX_SPILL_WRITE_BATCH_MEM = 32 * 1024 * 1024;
    
    /**
     * 构造函数
     * @param state 运行时状态
     * @param stream_id 流 ID（全局唯一）
     * @param data_dir SpillDataDir 指针（不拥有所有权）
     * @param spill_dir spill 目录路径
     * @param batch_rows 批次行数
     * @param batch_bytes 批次字节数
     * @param profile 性能分析器
     */
    SpillStream(RuntimeState* state, int64_t stream_id, SpillDataDir* data_dir,
                std::string spill_dir, size_t batch_rows, size_t batch_bytes,
                RuntimeProfile* profile);

    /**
     * 禁止默认构造函数
     */
    SpillStream() = delete;

    /**
     * 析构函数
     * 自动调用 gc() 清理 spill 文件
     */
    ~SpillStream();

    /**
     * 垃圾回收
     * 将 spill_dir_ 移动到 spill_gc 目录，由后台 GC 线程异步删除
     * 同时更新 SpillDataDir 的使用量统计
     */
    void gc();

    /**
     * 获取流 ID
     */
    int64_t id() const { return stream_id_; }

    /**
     * 获取 SpillDataDir 指针
     */
    SpillDataDir* get_data_dir() const { return data_dir_; }
    
    /**
     * 获取 spill 根目录路径（SpillDataDir 的路径）
     */
    const std::string& get_spill_root_dir() const;

    /**
     * 获取 spill 目录路径
     * 格式: storage_root/spill/query_id/operator_name-node_id-task_id-stream_id
     */
    const std::string& get_spill_dir() const { return spill_dir_; }

    /**
     * 获取已写入的字节数
     */
    int64_t get_written_bytes() const { return total_written_bytes_; }

    /**
     * 将 Block 数据写入磁盘
     * 
     * 流程：
     * 1. 调用 SpillWriter::write() 写入数据
     * 2. 如果 eof=true，调用 spill_eof() 结束写入
     * 3. 更新 total_written_bytes_
     * 
     * @param state 运行时状态
     * @param block 要写入的 Block
     * @param eof 是否为最后一个 block
     * @return Status
     */
    Status spill_block(RuntimeState* state, const Block& block, bool eof);

    /**
     * 结束写入（EOF）
     * 
     * 流程：
     * 1. 调用 SpillWriter::close() 关闭文件并写入 meta
     * 2. 释放 writer_
     * 3. 设置 _ready_for_reading = true，标记可以读取
     * 
     * @return Status
     */
    Status spill_eof();

    /**
     * 同步读取下一个 Block
     * 
     * 流程：
     * 1. 检查是否正在读取（防止并发读取）
     * 2. 调用 SpillReader::open() 打开文件（如果未打开）
     * 3. 调用 SpillReader::read() 读取数据
     * 
     * @param block 输出的 Block
     * @param eos 输出参数，是否到达文件末尾
     * @return Status
     */
    Status read_next_block_sync(Block* block, bool* eos);

    /**
     * 设置读取相关的性能计数器
     * @param operator_profile 操作符性能分析器
     */
    void set_read_counters(RuntimeProfile* operator_profile) {
        reader_->set_counters(operator_profile);
    }

    /**
     * 更新共享的性能分析器
     * 用于在多个 SpillStream 之间共享某些计数器
     * @param source_op_profile 源操作符性能分析器
     */
    void update_shared_profiles(RuntimeProfile* source_op_profile);

    /**
     * 创建一个独立的 Reader
     * 用于需要多个 Reader 同时读取的场景（如 merge sort）
     * @return SpillReader 智能指针
     */
    SpillReaderUPtr create_separate_reader() const;

    /**
     * 获取查询 ID
     */
    const TUniqueId& query_id() const;

    /**
     * 检查是否准备好读取
     * 只有在调用 spill_eof() 成功后才会返回 true
     */
    bool ready_for_reading() const { return _ready_for_reading; }

private:
    /**
     * SpillStreamManager 是友元类，可以访问 prepare() 方法
     */
    friend class SpillStreamManager;

    /**
     * 准备 SpillStream
     * 
     * 流程：
     * 1. 创建 SpillWriter
     * 2. 创建 SpillReader（使用 writer 的文件路径）
     * 3. 设置写入计数器
     * 4. 调用 SpillWriter::open() 打开文件
     * 
     * 注意：此方法由 SpillStreamManager::register_spill_stream() 调用
     * 
     * @return Status
     */
    Status prepare();

    /**
     * 设置写入相关的性能计数器
     * @param profile 性能分析器
     */
    void _set_write_counters(RuntimeProfile* profile) { writer_->set_counters(profile); }

    RuntimeState* state_ = nullptr;        // 运行时状态
    int64_t stream_id_;                    // 流 ID（全局唯一）
    SpillDataDir* data_dir_ = nullptr;     // SpillDataDir 指针（不拥有所有权）
    
    /**
     * Spill 目录路径
     * 格式: storage_root/spill/query_id/operator_name-node_id-task_id-stream_id
     * 由 SpillStreamManager::register_spill_stream() 指定
     */
    std::string spill_dir_;
    
    size_t batch_rows_;                    // 批次行数
    size_t batch_bytes_;                   // 批次字节数
    int64_t total_written_bytes_ = 0;      // 已写入的总字节数

    /**
     * 是否准备好读取（原子变量）
     * 只有在调用 spill_eof() 成功后才会设置为 true
     */
    std::atomic_bool _ready_for_reading = false;
    
    /**
     * 是否正在读取（原子变量）
     * 用于防止并发读取，确保 read_next_block_sync() 的线程安全
     */
    std::atomic_bool _is_reading = false;

    SpillWriterUPtr writer_;              // Spill 写入器
    SpillReaderUPtr reader_;               // Spill 读取器

    TUniqueId query_id_;                   // 查询 ID

    RuntimeProfile* profile_ = nullptr;   // 性能分析器
    RuntimeProfile::Counter* _current_file_count = nullptr;   // 当前文件数量计数器
    RuntimeProfile::Counter* _total_file_count = nullptr;      // 总文件数量计数器
    RuntimeProfile::Counter* _current_file_size = nullptr;      // 当前文件大小计数器
};

/**
 * SpillStream 智能指针类型别名
 * 使用 shared_ptr 管理 SpillStream 的生命周期
 */
using SpillStreamSPtr = std::shared_ptr<SpillStream>;
} // namespace vectorized
} // namespace doris
#include "common/compile_check_end.h"
