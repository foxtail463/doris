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

#include "vec/spill/spill_stream.h"

#include <glog/logging.h>

#include <memory>
#include <mutex>
#include <utility>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_stream_manager.h"
#include "vec/spill/spill_writer.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
SpillStream::SpillStream(RuntimeState* state, int64_t stream_id, SpillDataDir* data_dir,
                         std::string spill_dir, size_t batch_rows, size_t batch_bytes,
                         RuntimeProfile* operator_profile)
        : state_(state),
          stream_id_(stream_id),
          data_dir_(data_dir),
          spill_dir_(std::move(spill_dir)),
          batch_rows_(batch_rows),
          batch_bytes_(batch_bytes),
          query_id_(state->query_id()),
          profile_(operator_profile) {
    RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
    DCHECK(custom_profile != nullptr);
    _total_file_count = custom_profile->get_counter("SpillWriteFileTotalCount");
    _current_file_count = custom_profile->get_counter("SpillWriteFileCurrentCount");
    _current_file_size = custom_profile->get_counter("SpillWriteFileCurrentBytes");
}

void SpillStream::update_shared_profiles(RuntimeProfile* source_op_profile) {
    _current_file_count = source_op_profile->get_counter("SpillWriteFileCurrentCount");
    _current_file_size = source_op_profile->get_counter("SpillWriteFileCurrentBytes");
}

SpillStream::~SpillStream() {
    gc();
}

/**
 * 垃圾回收（Garbage Collection）
 * 
 * 【功能说明】
 * 当 SpillStream 析构时，将 spill 目录移动到 GC 目录，由后台 GC 线程异步删除。
 * 不直接删除文件，而是通过重命名操作原子性地移动到 GC 目录，避免在析构函数中执行耗时的删除操作。
 * 
 * 【目录结构】
 * 
 * 原始 spill 目录：
 *   spill_dir_ = storage_root/spill/query_id/operator_name-node_id-task_id-stream_id
 * 
 * GC 目录结构：
 *   query_gc_dir = storage_root/spill_gc/query_id/
 *   gc_dir = storage_root/spill_gc/query_id/operator_name-node_id-task_id-stream_id
 * 
 * 【工作流程】
 * 
 * 1. **更新性能计数器**：
 *    - 减少当前文件大小计数器（`_current_file_size`）
 *    - 减少当前文件数量计数器（`_current_file_count`）
 *    - 这些计数器用于监控 spill 文件的使用情况
 * 
 * 2. **检查 spill 目录是否存在**：
 *    - 如果目录不存在，说明已经被移动或删除，跳过后续操作
 *    - 如果目录存在，继续执行 GC 流程
 * 
 * 3. **创建查询 GC 目录**：
 *    - 创建 `storage_root/spill_gc/query_id/` 目录（如果不存在）
 *    - 这个目录用于存放该查询的所有待清理 spill 文件
 * 
 * 4. **移动 spill 目录到 GC 目录**：
 *    - 使用 `rename` 操作原子性地将 spill_dir_ 移动到 gc_dir
 *    - rename 操作是原子的，不会出现中间状态
 *    - 移动后，spill_dir_ 路径下的文件会被移动到 spill_gc 目录
 * 
 * 5. **更新 SpillDataDir 使用量统计**：
 *    - 减少 SpillDataDir 的已使用空间（`update_spill_data_usage(-total_written_bytes_)`）
 *    - 即使目录移动失败，也要更新统计，因为数据已经不再使用
 * 
 * 6. **重置写入字节数**：
 *    - 将 `total_written_bytes_` 重置为 0
 *    - 表示该 SpillStream 不再占用空间
 * 
 * 【设计原因】
 * 
 * 1. **异步删除**：
 *    - 不直接删除文件，而是移动到 GC 目录
 *    - 由后台 GC 线程定期清理 GC 目录下的过期文件
 *    - 避免在析构函数中执行耗时的 I/O 操作，影响性能
 * 
 * 2. **原子性操作**：
 *    - 使用 `rename` 操作移动目录，保证原子性
 *    - 不会出现目录部分移动的情况
 * 
 * 3. **容错处理**：
 *    - 如果 QueryContext 先于 PipelineFragmentContext 析构，spill_dir_ 可能已经被移动
 *    - 通过检查目录是否存在来处理这种情况
 *    - 即使移动失败，也会更新使用量统计，确保统计准确性
 * 
 * 4. **最终清理保证**：
 *    - 即使 GC 失败，QueryContext 析构时也会清理该查询的所有 spill 数据
 *    - 这是最后的清理保障，确保不会泄露磁盘空间
 * 
 * 【调用时机】
 * 
 * - SpillStream 析构时自动调用（`~SpillStream()`）
 * - SpillStreamManager::delete_spill_stream() 中显式调用
 * 
 * 【示例】
 * 
 * 示例 1：正常 GC
 * ```cpp
 * // SpillStream 析构
 * ~SpillStream() {
 *     gc();
 *     // → 更新性能计数器：-100MB
 *     // → 检查目录存在：spill_dir_ 存在
 *     // → 创建 GC 目录：storage_root/spill_gc/query_123/
 *     // → 移动目录：spill_dir_ → gc_dir
 *     // → 更新使用量：-100MB
 *     // → 重置 total_written_bytes_ = 0
 * }
 * ```
 * 
 * 示例 2：目录已被移动
 * ```cpp
 * // 如果 QueryContext 先于 PipelineFragmentContext 析构
 * // spill_dir_ 可能已经被移动到 GC 目录
 * 
 * gc();
 * // → 更新性能计数器：-100MB
 * // → 检查目录存在：spill_dir_ 不存在
 * // → 跳过移动操作
 * // → 更新使用量：-100MB（仍然更新，确保统计准确）
 * // → 重置 total_written_bytes_ = 0
 * ```
 * 
 * 【注意事项】
 * 
 * 1. **性能计数器**：
 *    - 只有在计数器存在时才更新（`if (_current_file_size)`）
 *    - 避免空指针访问
 * 
 * 2. **错误处理**：
 *    - 如果移动失败，只记录警告日志，不抛出异常
 *    - 因为析构函数中不应该抛出异常
 *    - 最终清理会在 QueryContext 析构时执行
 * 
 * 3. **统计更新**：
 *    - 无论目录移动是否成功，都会更新使用量统计
 *    - 确保 SpillDataDir 的使用量统计准确
 * 
 * 4. **后台 GC 线程**：
 *    - SpillStreamManager 会启动后台 GC 线程
 *    - 定期清理 spill_gc 目录下的过期文件
 *    - 避免磁盘空间被占用
 */
void SpillStream::gc() {
    // ===== 步骤 1：更新性能计数器（文件大小） =====
    // 减少当前文件大小计数器，表示该 SpillStream 不再占用空间
    // 只有在计数器存在时才更新，避免空指针访问
    if (_current_file_size) {
        COUNTER_UPDATE(_current_file_size, -total_written_bytes_);
    }
    
    // ===== 步骤 2：检查 spill 目录是否存在 =====
    // 如果目录不存在，说明已经被移动或删除，跳过后续操作
    // 这种情况可能发生在 QueryContext 先于 PipelineFragmentContext 析构时
    bool exists = false;
    auto status = io::global_local_filesystem()->exists(spill_dir_, &exists);
    
    if (status.ok() && exists) {
        // ===== 步骤 3：更新性能计数器（文件数量） =====
        // 减少当前文件数量计数器，表示该 SpillStream 不再占用文件数
        if (_current_file_count) {
            COUNTER_UPDATE(_current_file_count, -1);
        }
        
        // ===== 步骤 4：创建查询 GC 目录 =====
        // query_gc_dir = storage_root/spill_gc/query_id/
        // 这个目录用于存放该查询的所有待清理 spill 文件
        auto query_gc_dir = data_dir_->get_spill_data_gc_path(print_id(query_id_));
        status = io::global_local_filesystem()->create_directory(query_gc_dir);
        
        // ===== 调试钩子：模拟 GC 失败（用于测试） =====
        DBUG_EXECUTE_IF("fault_inject::spill_stream::gc", {
            status = Status::Error<INTERNAL_ERROR>("fault_inject spill_stream gc failed");
        });
        
        // ===== 步骤 5：移动 spill 目录到 GC 目录 =====
        // 如果创建 GC 目录成功，执行移动操作
        if (status.ok()) {
            // gc_dir = storage_root/spill_gc/query_id/operator_name-node_id-task_id-stream_id
            // 使用 spill_dir_ 的文件名作为 gc_dir 的子目录名
            auto gc_dir = fmt::format("{}/{}", query_gc_dir,
                                      std::filesystem::path(spill_dir_).filename().string());
            
            // 使用 rename 操作原子性地移动目录
            // rename 操作是原子的，不会出现中间状态
            status = io::global_local_filesystem()->rename(spill_dir_, gc_dir);
        }
        
        // ===== 步骤 6：错误处理 =====
        // 如果移动失败，记录警告日志，但不抛出异常
        // 原因：析构函数中不应该抛出异常，最终清理会在 QueryContext 析构时执行
        if (!status.ok()) {
            LOG_EVERY_T(WARNING, 1) << fmt::format("failed to gc spill data, dir {}, error: {}",
                                                   query_gc_dir, status.to_string());
        }
    }
    
    // ===== 步骤 7：更新 SpillDataDir 使用量统计 =====
    // 无论目录移动是否成功，都要更新使用量统计
    // 
    // 【原因 1】如果 QueryContext 先于 PipelineFragmentContext 析构，
    //          spill_dir_ 可能已经被移动到 spill_gc 目录，但统计仍然需要更新
    // 
    // 【原因 2】即使移动失败，数据也已经不再使用，应该从统计中移除
    //          最终清理会在 QueryContext 析构时执行，确保数据被清理
    data_dir_->update_spill_data_usage(-total_written_bytes_);
    
    // ===== 步骤 8：重置写入字节数 =====
    // 将 total_written_bytes_ 重置为 0，表示该 SpillStream 不再占用空间
    total_written_bytes_ = 0;
}

Status SpillStream::prepare() {
    writer_ = std::make_unique<SpillWriter>(state_->get_query_ctx()->resource_ctx(), profile_,
                                            stream_id_, batch_rows_, data_dir_, spill_dir_);
    _set_write_counters(profile_);

    reader_ = std::make_unique<SpillReader>(state_->get_query_ctx()->resource_ctx(), stream_id_,
                                            writer_->get_file_path());

    DBUG_EXECUTE_IF("fault_inject::spill_stream::prepare_spill", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream prepare_spill failed");
    });
    COUNTER_UPDATE(_total_file_count, 1);
    if (_current_file_count) {
        COUNTER_UPDATE(_current_file_count, 1);
    }
    return writer_->open();
}

SpillReaderUPtr SpillStream::create_separate_reader() const {
    return std::make_unique<SpillReader>(state_->get_query_ctx()->resource_ctx(), stream_id_,
                                         writer_->get_file_path());
}

const TUniqueId& SpillStream::query_id() const {
    return query_id_;
}

const std::string& SpillStream::get_spill_root_dir() const {
    return data_dir_->path();
}

Status SpillStream::spill_block(RuntimeState* state, const Block& block, bool eof) {
    size_t written_bytes = 0;
    DBUG_EXECUTE_IF("fault_inject::spill_stream::spill_block", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream spill_block failed");
    });
    RETURN_IF_ERROR(writer_->write(state, block, written_bytes));
    if (eof) {
        RETURN_IF_ERROR(spill_eof());
    } else {
        total_written_bytes_ = writer_->get_written_bytes();
    }
    return Status::OK();
}

Status SpillStream::spill_eof() {
    DBUG_EXECUTE_IF("fault_inject::spill_stream::spill_eof", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream spill_eof failed");
    });
    auto status = writer_->close();
    total_written_bytes_ = writer_->get_written_bytes();
    writer_.reset();

    if (status.ok()) {
        _ready_for_reading = true;
    }
    return status;
}

Status SpillStream::read_next_block_sync(Block* block, bool* eos) {
    DCHECK(reader_ != nullptr);
    DCHECK(!_is_reading);
    _is_reading = true;
    Defer defer([this] { _is_reading = false; });

    DBUG_EXECUTE_IF("fault_inject::spill_stream::read_next_block", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream read_next_block failed");
    });
    RETURN_IF_ERROR(reader_->open());
    return reader_->read(block, eos);
}

} // namespace doris::vectorized
