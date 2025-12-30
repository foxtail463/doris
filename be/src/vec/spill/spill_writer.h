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
#include <string>

#include "io/fs/file_writer.h"
#include "runtime/workload_management/resource_context.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {
class SpillDataDir;
class SpillWriter {
public:
    SpillWriter(std::shared_ptr<ResourceContext> resource_context, RuntimeProfile* profile,
                int64_t id, size_t batch_size, SpillDataDir* data_dir, const std::string& dir)
            : data_dir_(data_dir),
              stream_id_(id),
              batch_size_(batch_size),
              _resource_ctx(std::move(resource_context)) {
        // Directory path format specified in SpillStreamManager::register_spill_stream:
        // storage_root/spill/query_id/partitioned_hash_join-node_id-task_id-stream_id/0
        file_path_ = dir + "/0";
        RuntimeProfile* common_profile = profile->get_child("CommonCounters");
        DCHECK(common_profile != nullptr);
        _memory_used_counter = common_profile->get_counter("MemoryUsage");
    }

    Status open();

    Status close();

    Status write(RuntimeState* state, const Block& block, size_t& written_bytes);

    int64_t get_id() const { return stream_id_; }

    int64_t get_written_bytes() const { return total_written_bytes_; }

    const std::string& get_file_path() const { return file_path_; }

    void set_counters(RuntimeProfile* operator_profile) {
        RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
        _write_file_timer = custom_profile->get_counter("SpillWriteFileTime");
        _serialize_timer = custom_profile->get_counter("SpillWriteSerializeBlockTime");
        _write_block_counter = custom_profile->get_counter("SpillWriteBlockCount");
        _write_block_bytes_counter = custom_profile->get_counter("SpillWriteBlockBytes");
        _write_file_total_size = custom_profile->get_counter("SpillWriteFileBytes");
        _write_file_current_size = custom_profile->get_counter("SpillWriteFileCurrentBytes");
        _write_rows_counter = custom_profile->get_counter("SpillWriteRows");
    }

private:
    Status _write_internal(const Block& block, size_t& written_bytes);

    // ===== 核心成员变量 =====
    
    /**
     * SpillDataDir 指针（不拥有所有权）
     * 
     * 【用途】
     * - 检查磁盘容量限制（`reach_capacity_limit()`）
     * - 更新 spill 数据使用量统计（`update_spill_data_usage()`）
     * - 获取 spill 数据目录路径（`path()`）
     * 
     * 【生命周期】
     * - 由 SpillStream 传入，在 SpillStream 生命周期内有效
     * - SpillWriter 不负责释放，由 SpillStream 管理
     */
    SpillDataDir* data_dir_ = nullptr;
    
    /**
     * 标记 Writer 是否已关闭（原子布尔值）
     * 
     * 【用途】
     * - 防止重复关闭文件（`close()` 中检查）
     * - 线程安全的状态标记
     * 
     * 【状态转换】
     * - false（默认）：Writer 打开状态，可以写入
     * - true：Writer 已关闭，不能再写入
     * 
     * 【设置时机】
     * - 在 `close()` 方法中设置为 true
     */
    std::atomic_bool closed_ = false;
    
    /**
     * 流 ID（全局唯一）
     * 
     * 【用途】
     * - 标识该 SpillWriter 所属的 SpillStream
     * - 用于日志记录和调试
     * - 传递给 SpillReader，用于读取对应的文件
     * 
     * 【格式】
     * - 由 SpillStreamManager 分配，全局唯一
     * - 通常为递增的整数
     */
    int64_t stream_id_;
    
    /**
     * 批次大小（行数）
     * 
     * 【用途】
     * - 当 Block 的行数超过 `batch_size_` 时，将 Block 分割成多个子块写入
     * - 避免单次写入过大的 Block，控制内存占用
     * 
     * 【使用场景】
     * ```cpp
     * if (block.rows() <= batch_size_) {
     *     // 直接写入整个 Block
     *     return _write_internal(block, written_bytes);
     * } else {
     *     // 分割成多个子块，逐个写入
     *     for (size_t row_idx = 0; row_idx < rows; row_idx += batch_size_) {
     *         // 提取 batch_size_ 行数据，写入子块
     *     }
     * }
     * ```
     * 
     * 【默认值】
     * - 由 SpillStream 传入，通常为 4096 或 8192
     */
    size_t batch_size_;
    
    /**
     * 最大子块大小（字节）
     * 
     * 【用途】
     * - 记录写入的所有子块中的最大大小
     * - 用于元数据（meta_），帮助 SpillReader 分配读取缓冲区
     * 
     * 【更新时机】
     * - 每次写入子块后，更新为 `max(max_sub_block_size_, buff_size)`
     * - 在 `close()` 时写入 meta_ 的末尾
     * 
     * 【文件格式】
     * - 文件格式：block1, block2, ..., blockn, meta
     * - meta 格式：block1_offset, block2_offset, ..., blockn_offset, max_sub_block_size, n
     */
    size_t max_sub_block_size_ = 0;
    
    /**
     * 文件路径
     * 
     * 【格式】
     * - 格式：storage_root/spill/query_id/operator_name-node_id-task_id-stream_id/0
     * - 例如：/data/doris/storage/spill/query_123/partitioned_hash_join-1-2-3/0
     * 
     * 【用途】
     * - 创建文件写入器（`file_writer_`）
     * - 传递给 SpillReader，用于读取文件
     * - 用于日志记录和错误信息
     * 
     * 【初始化】
     * - 在构造函数中设置：`file_path_ = dir + "/0"`
     * - dir 由 SpillStream 传入，格式为：storage_root/spill/query_id/operator_name-node_id-task_id-stream_id
     */
    std::string file_path_;
    
    /**
     * 文件写入器（智能指针）
     * 
     * 【用途】
     * - 实际执行文件 I/O 操作（`append()`, `close()`）
     * - 管理文件句柄的生命周期
     * 
     * 【生命周期】
     * - 在 `open()` 中创建：`create_file(file_path_, &file_writer_)`
     * - 在 `close()` 中关闭并释放：`file_writer_->close(); file_writer_.reset()`
     * 
     * 【线程安全】
     * - FileWriter 本身是线程安全的，可以在多线程环境中使用
     */
    std::unique_ptr<doris::io::FileWriter> file_writer_;

    // ===== 统计信息 =====
    
    /**
     * 已写入的块数
     * 
     * 【用途】
     * - 统计写入的 Block 数量（包括分割后的子块）
     * - 用于元数据（meta_），帮助 SpillReader 知道文件中有多少个块
     * 
     * 【更新时机】
     * - 每次成功写入一个块后，`++written_blocks_`
     * - 在 `close()` 时写入 meta_ 的末尾
     * 
     * 【文件格式】
     * - meta 格式：block1_offset, block2_offset, ..., blockn_offset, max_sub_block_size, n
     * - 其中 n = written_blocks_
     */
    size_t written_blocks_ = 0;
    
    /**
     * 总写入字节数
     * 
     * 【用途】
     * - 统计写入文件的总字节数（包括所有块和元数据）
     * - 用于监控和性能分析
     * - 用于计算每个块在文件中的偏移量（meta_）
     * 
     * 【更新时机】
     * - 每次成功写入一个块后，`total_written_bytes_ += buff_size`
     * - 在 `close()` 时，`total_written_bytes_ += meta_.size()`
     * 
     * 【注意】
     * - 不包括文件系统开销（如文件头、索引等）
     * - 只统计实际写入的数据大小
     */
    int64_t total_written_bytes_ = 0;
    
    /**
     * 元数据（二进制格式）
     * 
     * 【格式】
     * - 元数据格式：block1_offset, block2_offset, ..., blockn_offset, max_sub_block_size, n
     * - 每个块偏移量：`sizeof(size_t)` 字节（8 字节）
     * - max_sub_block_size：`sizeof(size_t)` 字节（8 字节）
     * - n（块数）：`sizeof(size_t)` 字节（8 字节）
     * 
     * 【内容】
     * - block1_offset：第一个块在文件中的偏移量（通常为 0）
     * - block2_offset：第二个块在文件中的偏移量（block1_offset + block1_size）
     * - ...
     * - blockn_offset：第 n 个块在文件中的偏移量
     * - max_sub_block_size：所有块中的最大大小
     * - n：块的总数（written_blocks_）
     * 
     * 【构建过程】
     * - 每次写入块时，将 `total_written_bytes_`（块偏移量）追加到 meta_
     * - 在 `close()` 时，将 `max_sub_block_size_` 和 `written_blocks_` 追加到 meta_
     * - 最后将 meta_ 写入文件末尾
     * 
     * 【用途】
     * - SpillReader 读取文件时，先读取 meta_，获取所有块的偏移量
     * - 然后根据偏移量随机访问任意块，实现高效的读取
     * 
     * 【文件格式】
     * - 文件格式：block1, block2, ..., blockn, meta
     * - meta 位于文件末尾，方便读取
     */
    std::string meta_;

    RuntimeProfile::Counter* _write_file_timer = nullptr;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    RuntimeProfile::Counter* _write_block_counter = nullptr;
    RuntimeProfile::Counter* _write_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _write_file_total_size = nullptr;
    RuntimeProfile::Counter* _write_file_current_size = nullptr;
    RuntimeProfile::Counter* _write_rows_counter = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};
using SpillWriterUPtr = std::unique_ptr<SpillWriter>;
} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
