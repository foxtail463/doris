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

#include "partitioned_hash_join_sink_operator.h"

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status PartitionedHashJoinSinkLocalState::init(doris::RuntimeState* state,
                                               doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    _shared_state->partitioned_build_blocks.resize(p._partition_count);
    _shared_state->spilled_streams.resize(p._partition_count);

    _rows_in_partitions.assign(p._partition_count, 0);

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    _partition_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillPartitionTime", 1);
    _partition_shuffle_timer =
            ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillPartitionShuffleTime", 1);
    _spill_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillBuildTime", 1);
    _in_mem_rows_counter =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "SpillInMemRow", TUnit::UNIT, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);
    RETURN_IF_ERROR(_setup_internal_operator(state));
    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _shared_state->setup_shared_profile(custom_profile());
    RETURN_IF_ERROR(PipelineXSpillSinkLocalState::open(state));
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    for (uint32_t i = 0; i != p._partition_count; ++i) {
        auto& spilling_stream = _shared_state->spilled_streams[i];
        RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                state, spilling_stream, print_id(state->query_id()),
                fmt::format("hash_build_sink_{}", i), _parent->node_id(),
                std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
                operator_profile()));
    }
    return p._partitioner->clone(state, _partitioner);
}

Status PartitionedHashJoinSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(PipelineXSpillSinkLocalState::exec_time_counter());
    SCOPED_TIMER(PipelineXSpillSinkLocalState::_close_timer);
    if (PipelineXSpillSinkLocalState::_closed) {
        return Status::OK();
    }
    DCHECK(_shared_state->inner_runtime_state != nullptr);
    VLOG_DEBUG << "Query:" << print_id(state->query_id())
               << ", hash join sink:" << _parent->node_id() << ", task:" << state->task_id()
               << ", close";
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    if (!_shared_state->is_spilled && _shared_state->inner_runtime_state) {
        RETURN_IF_ERROR(p._inner_sink_operator->close(_shared_state->inner_runtime_state.get(),
                                                      exec_status));
    }
    return PipelineXSpillSinkLocalState::close(state, exec_status);
}

/**
 * @brief 计算可回收内存大小（可以 Spill 到磁盘的内存大小）
 *
 * 【功能说明】
 * 返回当前 Sink 算子中可以 Spill 到磁盘的内存总量。
 * 这个值用于判断是否有足够的数据可以 Spill，以及是否需要触发 Spill。
 *
 * 【两种状态的处理】
 * 1. 未 Spill 状态（`!is_spilled`）：
 *    - 数据还没有分区，全部存储在内部的 HashJoinBuildSinkOperatorX 中
 *    - 返回内部 Sink 算子的 Build 块内存使用量
 *    - 这个值表示如果现在 Spill，可以释放多少内存
 *
 * 2. 已 Spill 状态（`is_spilled`）：
 *    - 数据已经分区，存储在 `partitioned_build_blocks` 中
 *    - 遍历所有分区的块，累加大于等于最小批次大小的块的内存
 *    - 只计算 >= MIN_SPILL_WRITE_BATCH_MEM（32KB）的块，避免频繁 Spill 小数据
 *
 * 【使用场景】
 * - 在 `_try_to_reserve_memory()` 中调用，判断是否有足够的可回收内存
 * - 在 `revoke_memory()` 中调用，判断是否需要创建 RevokableTask
 * - 在 `is_revocable_mem_high_watermark()` 中调用，判断是否超过高水位线
 *
 * 【返回值】
 * - 未 Spill：返回内部 Sink 算子的 Build 块内存使用量（字节）
 * - 已 Spill：返回所有分区块的内存总和（字节），只计算 >= 32KB 的块
 * - 如果无法获取（例如内部状态不存在），返回 0
 *
 * @param state 运行时状态（当前未使用，保留用于未来扩展）
 * @return 可回收内存大小（字节）
 *
 * 【示例】
 * ```cpp
 * // 情况 1：未 Spill
 * // 内部 HashJoinBuildSinkLocalState 的 _build_blocks_memory_usage = 5GB
 * // 返回：5GB（表示可以 Spill 5GB 的数据）
 *
 * // 情况 2：已 Spill，有 4 个分区
 * // Partition 0: 100MB（>= 32KB，计入）
 * // Partition 1: 20KB（< 32KB，不计入）
 * // Partition 2: 200MB（>= 32KB，计入）
 * // Partition 3: 50MB（>= 32KB，计入）
 * // 返回：100MB + 200MB + 50MB = 350MB
 * ```
 */
size_t PartitionedHashJoinSinkLocalState::revocable_mem_size(RuntimeState* state) const {
    // ===== 情况 1：未 Spill，数据存储在内部 Sink 算子中 =====
    // 如果还没有进行过 Spill，说明数据还没有分区
    // 所有数据都存储在内部的 HashJoinBuildSinkOperatorX 中
    // 此时需要返回内部 Sink 算子的内存使用量
    if (!_shared_state->is_spilled) {
        // 检查内部共享状态是否存在
        // inner_shared_state 是 HashJoinBuildSinkOperatorX 的共享状态
        if (_shared_state->inner_shared_state) {
            // 获取内部 Sink 算子的本地状态
            // inner_runtime_state 是内部 HashJoinBuildSinkOperatorX 的 RuntimeState
            auto* inner_sink_state_ = _shared_state->inner_runtime_state->get_sink_local_state();
            if (inner_sink_state_) {
                // 转换为 HashJoinBuildSinkLocalState 类型
                // 这个类型包含了 Build 块的内存使用统计
                auto* inner_sink_state =
                        assert_cast<HashJoinBuildSinkLocalState*>(inner_sink_state_);
                // 返回 Build 块的内存使用量
                // _build_blocks_memory_usage 是一个 RuntimeProfile::Counter，记录了所有 Build 块的总内存
                // 例如：如果 Build 表有 3 个块，每个 1GB，则返回 3GB
                return inner_sink_state->_build_blocks_memory_usage->value();
            }
        }
        // 如果内部状态不存在，返回 0（表示没有可回收的内存）
        return 0;
    }

    // ===== 情况 2：已 Spill，数据存储在分区块中 =====
    // 如果已经进行过 Spill，说明数据已经分区
    // 数据存储在 partitioned_build_blocks 数组中，每个元素对应一个分区
    size_t mem_size = 0;
    auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
    // 遍历所有分区的块
    for (auto& block : partitioned_blocks) {
        if (block) {
            // 获取块的实际内存大小（allocated_bytes）
            // 这个值包括块中所有列的内存占用
            auto block_bytes = block->allocated_bytes();
            // ===== 只计算大于等于最小批次大小的块 =====
            // MIN_SPILL_WRITE_BATCH_MEM 通常为 32KB
            // 原因：
            // 1. 避免频繁 Spill 小数据，提高性能
            // 2. Spill 操作本身有开销（文件创建、序列化、压缩等）
            // 3. 太小的数据不值得 Spill，直接保留在内存中更高效
            // 例如：
            // - 块大小 100MB >= 32KB → 计入 mem_size
            // - 块大小 20KB < 32KB → 不计入 mem_size
            if (block_bytes >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                mem_size += block_bytes;
            }
        }
    }
    // 返回所有符合条件的分区块的内存总和
    return mem_size;
}

void PartitionedHashJoinSinkLocalState::update_memory_usage() {
    if (!_shared_state->is_spilled) {
        if (_shared_state->inner_shared_state) {
            auto* inner_sink_state_ = _shared_state->inner_runtime_state->get_sink_local_state();
            if (inner_sink_state_) {
                auto* inner_sink_state =
                        assert_cast<HashJoinBuildSinkLocalState*>(inner_sink_state_);
                COUNTER_SET(_memory_used_counter, inner_sink_state->_memory_used_counter->value());
            }
        }
        return;
    }

    int64_t mem_size = 0;
    auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
    for (auto& block : partitioned_blocks) {
        if (block) {
            mem_size += block->allocated_bytes();
        }
    }
    COUNTER_SET(_memory_used_counter, mem_size);
}

size_t PartitionedHashJoinSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    size_t size_to_reserve = 0;
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    if (_shared_state->is_spilled) {
        size_to_reserve = p._partition_count * vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM;
    } else {
        if (_shared_state->inner_runtime_state) {
            size_to_reserve = p._inner_sink_operator->get_reserve_mem_size(
                    _shared_state->inner_runtime_state.get(), eos);
        }
    }

    COUNTER_SET(_memory_usage_reserved, int64_t(size_to_reserve));
    return size_to_reserve;
}

Dependency* PartitionedHashJoinSinkLocalState::finishdependency() {
    return _finish_dependency.get();
}

/**
 * 处理未分区的数据块（首次 Spill）
 * 
 * 【功能说明】
 * 这是首次 Spill 时调用的方法，负责处理未分区的数据块。
 * 当首次触发 Spill 时，数据可能还在内部的 HashJoinBuildSinkOperatorX 中，还没有分区。
 * 此方法会：
 * 1. 从内部 Sink 算子获取 Build 数据块
 * 2. 清理内部 Hash 表（释放内存）
 * 3. 分批处理数据（每次 4096 行），对每批数据进行分区
 * 4. 将分区后的数据 Spill 到对应的分区文件中
 * 
 * 【为什么需要分批处理】
 * 1. **内存控制**：避免一次性处理大量数据导致内存峰值
 * 2. **性能优化**：分批处理可以更好地利用缓存，提高分区效率
 * 3. **及时 Spill**：当分区块达到阈值时，可以立即 Spill，不需要等待所有数据处理完
 * 
 * 【分批大小选择】
 * - `reserved_size = 4096`：每批处理 4096 行
 * - 这个大小是一个平衡点：
 *   * 足够大：减少批次数量，提高处理效率
 *   * 足够小：避免内存峰值，及时释放内存
 * 
 * 【工作流程】
 * 
 * 阶段 1：准备阶段（在主线程）
 * 1. 获取内部 Sink 算子的状态
 * 2. 清理内部 Hash 表（释放内存）
 * 3. 从内部 Sink 算子获取 Build 数据块
 * 4. 禁用 Runtime Filter（Spill 时不需要）
 * 5. 清理无用列（只保留必要的列）
 * 
 * 阶段 2：分批处理阶段（在后台线程，spill_func）
 * 对 Build 数据块进行分批处理：
 * 1. 每次切取 4096 行（或剩余行数，取较小值）
 * 2. 对每批数据进行分区计算（do_partitioning）
 * 3. 收集每个分区的行索引（partitions_indexes）
 * 4. 将数据写入对应的分区块（partitioned_build_blocks）
 * 5. 如果分区块达到阈值（4096 行）或最后一个批次，Spill 到磁盘
 * 
 * 阶段 3：完成阶段（在后台线程，spill_func）
 * 如果数据流结束（EOS）：
 * 1. 统计内存中剩余的行数
 * 2. 调用 `_finish_spilling()` 标记所有 SpillStream 为 EOF
 * 3. 设置 `_dependency->set_ready_to_read()`，通知 Probe 算子可以开始读取
 * 
 * 【关键设计】
 * 
 * 1. **分批切取**：
 *    - 使用 `column->cut(offset, this_run)` 切取子块
 *    - 避免复制整个数据块，只复制需要的部分
 * 
 * 2. **分区索引收集**：
 *    - `partitions_indexes[i]` 存储属于分区 i 的所有行在 sub_block 中的索引
 *    - 例如：partitions_indexes[5] = [0, 15, 32] 表示 sub_block 的第 0、15、32 行属于分区 5
 * 
 * 3. **分批 Spill**：
 *    - 当分区块达到 `reserved_size`（4096 行）时，立即 Spill
 *    - 如果是最后一个批次（`is_last_block`），即使未达到阈值也 Spill
 *    - 这样可以及时释放内存，避免内存峰值
 * 
 * 4. **内存管理**：
 *    - 使用 `Defer` 和 `COUNTER_UPDATE` 跟踪内存使用
 *    - 每次 Spill 后，更新内存计数器
 * 
 * 【参数说明】
 * @param state 运行时状态
 * @param spill_context Spill 上下文（用于跟踪 Spill 进度和资源管理）
 * @return Status
 * 
 * 【示例】
 * 
 * 假设 Build 表有 10000 行数据，16 个分区：
 * 
 * 1. 准备阶段：
 *    - 从内部 Sink 算子获取 10000 行数据
 *    - 清理内部 Hash 表（释放内存）
 * 
 * 2. 分批处理：
 *    - 批次 1：处理第 1-4096 行（索引 1-4096）
 *      * 分区计算：第 1-4096 行分配到各个分区
 *      * 写入分区块：partitioned_build_blocks[0-15]
 *      * 如果某个分区块达到 4096 行，Spill 到磁盘
 * 
 *    - 批次 2：处理第 4097-8192 行（索引 4097-8192）
 *      * 分区计算：第 4097-8192 行分配到各个分区
 *      * 写入分区块：partitioned_build_blocks[0-15]
 *      * 如果某个分区块达到 4096 行，Spill 到磁盘
 * 
 *    - 批次 3：处理第 8193-10000 行（索引 8193-10000，最后一批）
 *      * 分区计算：第 8193-10000 行分配到各个分区
 *      * 写入分区块：partitioned_build_blocks[0-15]
 *      * 因为是最后一批（is_last_block = true），所有分区块都 Spill 到磁盘
 * 
 * 3. 完成阶段：
 *    - 统计内存中剩余的行数（应该为 0）
 *    - 标记所有 SpillStream 为 EOF
 *    - 通知 Probe 算子可以开始读取
 * 
 * 【注意事项】
 * 1. 索引从 1 开始（offset = 1），因为索引 0 是虚拟行
 * 2. Spill 操作是异步的，不会阻塞主流程
 * 3. 分批处理可以更好地控制内存使用，避免内存峰值
 * 4. 如果 Build 表为空（rows <= 1），直接返回，不进行 Spill
 */
Status PartitionedHashJoinSinkLocalState::_revoke_unpartitioned_block(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    // ===== 阶段 1：获取内部 Sink 算子的状态 =====
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    HashJoinBuildSinkLocalState* inner_sink_state {nullptr};
    if (auto* tmp_sink_state = _shared_state->inner_runtime_state->get_sink_local_state()) {
        inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(tmp_sink_state);
    }
    
    // ===== 阶段 2：清理内部 Hash 表（释放内存） =====
    // 检查内部 Hash 表只有一个变体（Broadcast Join）
    DCHECK_EQ(_shared_state->inner_shared_state->hash_table_variant_vector.size(), 1);
    // 重置 Hash 表变体，释放 Hash 表占用的内存
    _shared_state->inner_shared_state->hash_table_variant_vector.front().reset();
    
    // 更新内存计数器：减去 Hash 表和 Arena 的内存占用
    if (inner_sink_state) {
        COUNTER_UPDATE(_memory_used_counter,
                       -(inner_sink_state->_hash_table_memory_usage->value() +
                         inner_sink_state->_build_arena_memory_usage->value()));
    }
    
    // ===== 阶段 3：准备获取 Build 数据块 =====
    const auto& row_desc = p._child->row_desc();
    const auto num_slots = row_desc.num_slots();
    vectorized::Block build_block;
    int64_t block_old_mem = 0;
    
    if (inner_sink_state) {
        // ===== 子阶段 3A：从内部 Sink 算子获取 Build 数据块 =====
        // 将 MutableBlock 转换为 Block（获取实际数据）
        build_block = inner_sink_state->_build_side_mutable_block.to_block();
        block_old_mem = build_block.allocated_bytes();
        
        // ===== 子阶段 3B：禁用 Runtime Filter =====
        // If spilling was triggered, constructing runtime filters is meaningless,
        // therefore, all runtime filters are temporarily disabled.
        // 如果已经触发 Spill，构建 Runtime Filter 没有意义，因此暂时禁用所有 Runtime Filter
        RETURN_IF_ERROR(inner_sink_state->_runtime_filter_producer_helper->skip_process(
                _shared_state->inner_runtime_state.get()));
        
        // ===== 子阶段 3C：设置完成依赖 =====
        // 通知依赖的算子，Build 阶段已完成
        _finish_dependency->set_ready();
    }

    // ===== 阶段 4：检查数据是否为空 =====
    // 如果 Build 表为空（只有虚拟行 0），直接返回
    if (build_block.rows() <= 1) {
        LOG(WARNING) << fmt::format(
                "Query:{}, hash join sink:{}, task:{},"
                " has no data to revoke",
                print_id(state->query_id()), _parent->node_id(), state->task_id());
        if (spill_context) {
            spill_context->on_task_finished();
        }
        return Status::OK();
    }

    // ===== 阶段 5：清理无用列 =====
    // 如果 Build 数据块的列数超过需要的列数，删除多余的列
    // 这样可以减少内存占用和 Spill 的数据量
    if (build_block.columns() > num_slots) {
        vectorized::Block::erase_useless_column(&build_block, num_slots);
        COUNTER_UPDATE(_memory_used_counter, build_block.allocated_bytes() - block_old_mem);
    }

    // ===== 阶段 6：定义 Spill 执行函数（lambda） =====
    // 这个函数会在后台线程中执行，负责分批处理数据并 Spill
    auto spill_func = [build_block = std::move(build_block), state, this]() mutable {
        // 使用 Defer 确保在函数返回时更新内存使用统计
        Defer defer1 {[&]() { update_memory_usage(); }};
        
        auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
        auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
        
        // ===== 子阶段 6A：初始化分区索引数组 =====
        // partitions_indexes[i] 存储属于分区 i 的所有行在 sub_block 中的索引
        // 例如：partitions_indexes[5] = [0, 15, 32] 表示 sub_block 的第 0、15、32 行属于分区 5
        std::vector<std::vector<uint32_t>> partitions_indexes(p._partition_count);

        // ===== 子阶段 6B：设置分批大小和预分配内存 =====
        const size_t reserved_size = 4096;  // 每批处理 4096 行
        // 为每个分区的索引数组预分配内存，减少内存重新分配的开销
        std::ranges::for_each(partitions_indexes, [](std::vector<uint32_t>& indices) {
            indices.reserve(reserved_size);
        });

        // ===== 子阶段 6C：分批处理数据 =====
        size_t total_rows = build_block.rows();
        size_t offset = 1;  // 从索引 1 开始（索引 0 是虚拟行）
        
        while (offset < total_rows) {
            // ===== 步骤 1：创建子块并切取数据 =====
            // 创建一个空的子块（与 build_block 结构相同）
            auto sub_block = build_block.clone_empty();
            // 计算本次处理的行数（取 reserved_size 和剩余行数的较小值）
            size_t this_run = std::min(reserved_size, total_rows - offset);

            // 对每一列，使用 cut() 方法切取子块的数据
            // cut(offset, this_run) 表示从 offset 开始切取 this_run 行
            // 例如：cut(1, 4096) 表示切取第 1-4096 行
            for (size_t i = 0; i != build_block.columns(); ++i) {
                sub_block.get_by_position(i).column =
                        build_block.get_by_position(i).column->cut(offset, this_run);
            }
            
            // ===== 步骤 2：更新内存统计 =====
            int64_t sub_blocks_memory_usage = sub_block.allocated_bytes();
            COUNTER_UPDATE(_memory_used_counter, sub_blocks_memory_usage);
            // 使用 Defer 确保在退出循环时释放子块的内存
            Defer defer2 {
                    [&]() { COUNTER_UPDATE(_memory_used_counter, -sub_blocks_memory_usage); }};

            // ===== 步骤 3：更新偏移量并判断是否是最后一个批次 =====
            offset += this_run;
            const auto is_last_block = offset == total_rows;

            // ===== 步骤 4：对子块进行分区计算 =====
            {
                SCOPED_TIMER(_partition_timer);
                // 计算每行数据属于哪个分区（0 到 partition_count-1）
                // 结果存储在 _partitioner->get_channel_ids() 中
                (void)_partitioner->do_partitioning(state, &sub_block);
            }

            // ===== 步骤 5：收集每个分区的行索引 =====
            const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();
            // channel_ids[i] 表示 sub_block 的第 i 行属于哪个分区
            // 将行索引 i 添加到对应分区的索引列表中
            for (size_t i = 0; i != sub_block.rows(); ++i) {
                partitions_indexes[channel_ids[i]].emplace_back(i);
            }

            // ===== 步骤 6：将数据写入对应的分区块并 Spill =====
            for (uint32_t partition_idx = 0; partition_idx != p._partition_count; ++partition_idx) {
                // 获取分区 partition_idx 的行索引数组的起始和结束指针
                auto* begin = partitions_indexes[partition_idx].data();
                auto* end = begin + partitions_indexes[partition_idx].size();
                
                // 获取分区 partition_idx 的数据块和 SpillStream
                auto& partition_block = partitioned_blocks[partition_idx];
                vectorized::SpillStreamSPtr& spilling_stream =
                        _shared_state->spilled_streams[partition_idx];
                
                // ===== 步骤 6A：如果分区块不存在，创建它 =====
                if (UNLIKELY(!partition_block)) {
                    partition_block =
                            vectorized::MutableBlock::create_unique(build_block.clone_empty());
                }

                // ===== 步骤 6B：将数据写入分区块 =====
                int64_t old_mem = partition_block->allocated_bytes();
                {
                    SCOPED_TIMER(_partition_shuffle_timer);
                    // 将 sub_block 中属于分区 partition_idx 的所有行复制到 partition_block
                    // begin 和 end 指向行索引数组的起始和结束位置
                    RETURN_IF_ERROR(partition_block->add_rows(&sub_block, begin, end));
                    // 清空分区索引数组，准备下一批数据
                    partitions_indexes[partition_idx].clear();
                }
                int64_t new_mem = partition_block->allocated_bytes();

                // ===== 步骤 6C：判断是否需要 Spill =====
                // 如果分区块达到阈值（4096 行）或者是最后一个批次，Spill 到磁盘
                if (partition_block->rows() >= reserved_size || is_last_block) {
                    // 将 MutableBlock 转换为 Block
                    auto block = partition_block->to_block();
                    // Spill 到磁盘
                    RETURN_IF_ERROR(spilling_stream->spill_block(state, block, false));
                    // 清空分区块（释放内存），创建一个新的空块
                    partition_block =
                            vectorized::MutableBlock::create_unique(build_block.clone_empty());
                    // 更新内存计数器：减去 Spill 前的内存占用
                    COUNTER_UPDATE(_memory_used_counter, -new_mem);
                } else {
                    // 如果不需要 Spill，只更新内存增量
                    COUNTER_UPDATE(_memory_used_counter, new_mem - old_mem);
                }
            }
        }

        // ===== 阶段 7：如果数据流结束，完成 Spill =====
        Status status;
        if (_child_eos) {
            // 统计内存中剩余的行数（用于监控和调试）
            std::ranges::for_each(_shared_state->partitioned_build_blocks, [&](auto& block) {
                if (block) {
                    COUNTER_UPDATE(_in_mem_rows_counter, block->rows());
                }
            });
            // 完成 Spill：标记所有 SpillStream 为 EOF
            status = _finish_spilling();
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join sink:{}, task:{}, _revoke_unpartitioned_block, "
                    "set_ready_to_read",
                    print_id(state->query_id()), _parent->node_id(), state->task_id());
            // 设置依赖关系为 ready_to_read，通知 Probe 算子可以开始读取数据
            _dependency->set_ready_to_read();
        }

        return status;
    };

    // ===== 阶段 8：定义异常捕获函数 =====
    // 捕获 Spill 执行过程中的所有异常，并转换为 Status
    auto exception_catch_func = [spill_func]() mutable {
        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION(return spill_func()); }();
        return status;
    };

    // ===== 阶段 9：创建 SpillSinkRunnable 并执行 Spill =====
    // SpillSinkRunnable 会在后台线程中执行 Spill 操作
    SpillSinkRunnable spill_runnable(state, spill_context, operator_profile(),
                                     exception_catch_func);

    // ===== 调试钩子：模拟提交函数失败（用于测试） =====
    DBUG_EXECUTE_IF(
            "fault_inject::partitioned_hash_join_sink::revoke_unpartitioned_block_submit_func", {
                return Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_sink "
                        "revoke_unpartitioned_block submit_func failed");
            });
    
    // ===== 执行 Spill =====
    // SpillSinkRunnable::run() 会：
    // 1. 将 Spill 任务提交到后台线程池
    // 2. 等待 Spill 完成
    // 3. 返回执行结果
    return spill_runnable.run();
}

Status PartitionedHashJoinSinkLocalState::terminate(RuntimeState* state) {
    if (_terminated) {
        return Status::OK();
    }
    HashJoinBuildSinkLocalState* inner_sink_state {nullptr};
    if (auto* tmp_sink_state = _shared_state->inner_runtime_state->get_sink_local_state()) {
        inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(tmp_sink_state);
    }
    if (_parent->cast<PartitionedHashJoinSinkOperatorX>()._inner_sink_operator) {
        RETURN_IF_ERROR(inner_sink_state->_runtime_filter_producer_helper->skip_process(state));
    }
    inner_sink_state->_terminated = true;
    return PipelineXSpillSinkLocalState<PartitionedHashJoinSharedState>::terminate(state);
}

/**
 * 回收内存（执行 Spill 操作）
 * 
 * 【功能说明】
 * 这是分区 Hash Join Sink 算子的内存回收方法，负责将内存中的数据 Spill 到磁盘。
 * 当内存不足时，PipelineTask 会调用此方法来释放内存。
 * 
 * 【两种处理路径】
 * 
 * 1. **首次 Spill（未分区数据）**：
 *    - 条件：`!is_spilled`（还没有进行过 Spill）
 *    - 处理：调用 `_revoke_unpartitioned_block()` 处理未分区的数据
 *    - 原因：首次 Spill 时，数据可能还在内部的 HashJoinBuildSinkOperatorX 中，还没有分区
 *    - 流程：
 *      * 从内部 Sink 算子获取 Build 数据块
 *      * 对数据进行分区
 *      * 将分区的数据 Spill 到磁盘
 * 
 * 2. **后续 Spill（已分区数据）**：
 *    - 条件：`is_spilled = true`（已经进行过 Spill，数据已经分区）
 *    - 处理：遍历所有分区块，对超过阈值的分区进行 Spill
 *    - 原因：数据已经分区，存储在 `partitioned_build_blocks[i]` 中
 *    - 流程：
 *      * 遍历所有分区（partitioned_build_blocks）
 *      * 检查每个分区的内存占用
 *      * 如果 >= MIN_SPILL_WRITE_BATCH_MEM（32KB），调用 `_spill_to_disk()` 写入磁盘
 * 
 * 【异步执行】
 * 使用 `SpillSinkRunnable` 异步执行 Spill 操作：
 * - Spill 操作在后台线程执行，不阻塞主流程
 * - `SpillSinkRunnable` 会处理异常、统计、资源清理等
 * - 执行完成后，调用 `spill_fin_cb` 回调函数
 * 
 * 【完成回调（spill_fin_cb）】
 * 当 Spill 操作完成时，会执行以下操作：
 * - 如果数据流结束（EOS）：
 *   * 统计内存中剩余的行数（`_in_mem_rows_counter`）
 *   * 调用 `_finish_spilling()` 标记所有 SpillStream 为 EOF
 *   * 设置 `_dependency->set_ready_to_read()`，通知 Probe 算子可以开始读取
 * - 通知 `SpillContext` 任务完成（`spill_context->on_task_finished()`）
 * 
 * 【参数说明】
 * @param state 运行时状态
 * @param spill_context Spill 上下文（用于跟踪 Spill 进度和资源管理）
 * @return Status
 * 
 * 【调用链】
 * PipelineTask::do_revoke_memory()
 *   └─→ PartitionedHashJoinSinkLocalState::revoke_memory() (当前方法)
 *       └─→ SpillSinkRunnable::run() (异步执行)
 *           ├─→ 执行 Spill 函数（遍历分区，调用 _spill_to_disk）
 *           └─→ 执行完成回调（spill_fin_cb）
 * 
 * 【示例】
 * 
 * 示例 1：首次 Spill
 * ```cpp
 * // 情况：内存不足，首次触发 Spill
 * // 状态：is_spilled = false，数据在内部 HashJoinBuildSinkOperatorX 中
 * 
 * revoke_memory(state, spill_context);
 * // → 调用 _revoke_unpartitioned_block()
 * // → 从内部 Sink 算子获取数据
 * // → 对数据进行分区
 * // → 将分区数据 Spill 到磁盘
 * // → 设置 is_spilled = true
 * ```
 * 
 * 示例 2：后续 Spill
 * ```cpp
 * // 情况：内存不足，再次触发 Spill
 * // 状态：is_spilled = true，数据已经分区，存储在 partitioned_build_blocks 中
 * 
 * revoke_memory(state, spill_context);
 * // → 遍历 partitioned_build_blocks[0..15]
 * // → Partition 0: 100MB >= 32KB → Spill 到磁盘
 * // → Partition 1: 20KB < 32KB → 跳过（保留在内存中）
 * // → Partition 2: 200MB >= 32KB → Spill 到磁盘
 * // → ...
 * ```
 * 
 * 【注意事项】
 * 1. Spill 操作是异步的，不会阻塞主流程
 * 2. 只有 >= MIN_SPILL_WRITE_BATCH_MEM（32KB）的分区才会 Spill
 * 3. Spill 后，分区的数据会被清空（释放内存），但分区结构保留
 * 4. 如果数据流结束（EOS），会调用 `_finish_spilling()` 标记所有 SpillStream 为 EOF
 */
Status PartitionedHashJoinSinkLocalState::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    // ===== 阶段 1：开始计时和日志记录 =====
    SCOPED_TIMER(_spill_total_timer);
    VLOG_DEBUG << fmt::format("Query:{}, hash join sink:{}, task:{}, revoke_memory, eos:{}",
                              print_id(state->query_id()), _parent->node_id(), state->task_id(),
                              _child_eos);

    // ===== 阶段 2：判断是首次 Spill 还是后续 Spill =====
    if (!_shared_state->is_spilled) {
        // ===== 情况 A：首次 Spill（未分区数据） =====
        // 如果还没有进行过 Spill，说明数据可能还在内部的 HashJoinBuildSinkOperatorX 中
        // 需要先对数据进行分区，然后再 Spill
        
        // 标记已 Spill（后续数据都需要分区处理）
        custom_profile()->add_info_string("Spilled", "true");
        _shared_state->is_spilled = true;
        
        // 处理未分区的数据块
        // _revoke_unpartitioned_block 会：
        // 1. 从内部 Sink 算子获取 Build 数据块
        // 2. 对数据进行分区
        // 3. 将分区的数据 Spill 到磁盘
        return _revoke_unpartitioned_block(state, spill_context);
    }

    // ===== 情况 B：后续 Spill（已分区数据） =====
    // 如果已经进行过 Spill，说明数据已经分区，存储在 partitioned_build_blocks 中
    // 只需要遍历所有分区，对超过阈值的分区进行 Spill
    
    const auto query_id = state->query_id();
    
    // ===== 阶段 3：定义完成回调函数 =====
    // 当 Spill 操作完成时，会调用此回调函数
    auto spill_fin_cb = [this, state, query_id, spill_context]() {
        Status status;
        
        // ===== 子阶段 3A：如果数据流结束（EOS），完成 Spill =====
        if (_child_eos) {
            LOG(INFO) << fmt::format(
                    "Query:{}, hash join sink:{}, task:{}, finish spilling, set_ready_to_read",
                    print_id(query_id), _parent->node_id(), state->task_id());
            
            // 统计内存中剩余的行数（用于监控和调试）
            std::ranges::for_each(_shared_state->partitioned_build_blocks, [&](auto& block) {
                if (block) {
                    COUNTER_UPDATE(_in_mem_rows_counter, block->rows());
                }
            });
            
            // 完成 Spill：标记所有 SpillStream 为 EOF
            // 这会通知 Probe 算子，所有数据都已经 Spill 完成，可以开始读取
            status = _finish_spilling();
            
            // 设置依赖关系为 ready_to_read，通知 Probe 算子可以开始读取数据
            _dependency->set_ready_to_read();
        }

        // ===== 子阶段 3B：通知 SpillContext 任务完成 =====
        // 如果提供了 spill_context，通知它任务已完成
        // 用于跟踪 Spill 进度和资源管理
        if (spill_context) {
            spill_context->on_task_finished();
        }

        return status;
    };

    // ===== 阶段 4：创建 SpillSinkRunnable 并执行 Spill =====
    // SpillSinkRunnable 是一个包装类，用于异步执行 Spill 操作
    // 它会：
    // - 在后台线程执行 Spill（不阻塞主流程）
    // - 处理异常和错误
    // - 更新性能统计（SpillTotalTime、SpillBuildTime 等）
    // - 执行完成回调（spill_fin_cb）
    SpillSinkRunnable spill_runnable(
            state, nullptr, operator_profile(),
            // ===== Spill 执行函数（lambda） =====
            // 这个函数会在后台线程中执行，负责实际的 Spill 操作
            [this, state, query_id] {
                // ===== 调试钩子：模拟取消操作（用于测试） =====
                DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::revoke_memory_cancel", {
                    auto status = Status::InternalError(
                            "fault_inject partitioned_hash_join_sink "
                            "revoke_memory canceled");
                    state->get_query_ctx()->cancel(status);
                    return status;
                });
                
                // ===== 开始计时 =====
                SCOPED_TIMER(_spill_build_timer);

                // ===== 遍历所有分区，对超过阈值的分区进行 Spill =====
                for (size_t i = 0; i != _shared_state->partitioned_build_blocks.size(); ++i) {
                    // 获取分区 i 的 SpillStream（用于写入磁盘）
                    vectorized::SpillStreamSPtr& spilling_stream =
                            _shared_state->spilled_streams[i];
                    DCHECK(spilling_stream != nullptr);
                    
                    // 获取分区 i 的数据块
                    auto& mutable_block = _shared_state->partitioned_build_blocks[i];

                    // ===== 跳过条件检查 =====
                    // 如果分区块不存在，或者内存占用小于阈值，跳过该分区
                    // MIN_SPILL_WRITE_BATCH_MEM 通常为 32KB
                    // 原因：
                    // 1. 避免频繁 Spill 小数据，提高性能
                    // 2. Spill 操作本身有开销（文件创建、序列化、压缩等）
                    // 3. 太小的数据不值得 Spill，直接保留在内存中更高效
                    if (!mutable_block ||
                        mutable_block->allocated_bytes() <
                                vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                        continue;
                    }

                    // ===== 执行 Spill =====
                    // 调用 _spill_to_disk() 将分区 i 的数据写入磁盘
                    // 使用异常捕获确保错误能正确传播
                    auto status = [&]() {
                        RETURN_IF_CATCH_EXCEPTION(
                                return _spill_to_disk(static_cast<uint32_t>(i), spilling_stream));
                    }();

                    // 如果 Spill 失败，立即返回错误
                    RETURN_IF_ERROR(status);
                }
                return Status::OK();
            },
            spill_fin_cb);

    // ===== 执行 Spill =====
    // SpillSinkRunnable::run() 会：
    // 1. 将 Spill 任务提交到后台线程池
    // 2. 等待 Spill 完成
    // 3. 执行完成回调（spill_fin_cb）
    // 4. 返回执行结果
    return spill_runnable.run();
}

Status PartitionedHashJoinSinkLocalState::_finish_spilling() {
    for (auto& stream : _shared_state->spilled_streams) {
        if (stream) {
            RETURN_IF_ERROR(stream->spill_eof());
        }
    }
    return Status::OK();
}

/**
 * 对数据块进行分区
 * 
 * 【功能说明】
 * 将输入数据块（in_block）中的行数据根据 Join Key 的 Hash 值分配到不同的分区中。
 * 这是分区 Hash Join 的核心操作，确保相同 Join Key 的数据被分配到同一个分区。
 * 
 * 【工作流程】
 * 1. 检查数据块是否为空
 * 2. 使用 Partitioner 计算每行数据属于哪个分区（do_partitioning）
 * 3. 收集每行数据的分区索引（partition_indexes）
 * 4. 将数据按分区写入对应的 partitioned_build_blocks[i]
 * 5. 更新每个分区的行数统计
 * 
 * 【示例】
 * 假设有 1000 行数据，分成 16 个分区：
 * - 输入：in_block（1000 行）
 * - 分区计算：每行根据 Join Key 的 Hash 值计算分区号（0-15）
 * - 结果：
 *   * Partition 0: 第 0, 15, 32, ... 行（共 62 行）
 *   * Partition 1: 第 1, 16, 33, ... 行（共 63 行）
 *   * ...
 *   * Partition 15: 第 14, 29, 46, ... 行（共 63 行）
 * 
 * 【关键设计】
 * - 分区函数：使用 Join Key 的 Hash 值，确保相同 Key 的数据在同一分区
 * - 延迟创建：只有当分区有数据时才创建对应的 MutableBlock
 * - 内存管理：使用 Defer 确保内存统计正确更新
 * 
 * @param state RuntimeState
 * @param in_block 输入数据块（待分区的数据）
 * @param begin 起始行索引（包含），用于处理部分数据
 * @param end 结束行索引（不包含），用于处理部分数据
 * @return Status
 */
Status PartitionedHashJoinSinkLocalState::_partition_block(RuntimeState* state,
                                                           vectorized::Block* in_block,
                                                           size_t begin, size_t end) {
    // ===== 阶段 1：基本检查和初始化 =====
    // 检查数据块是否为空，如果为空则直接返回
    const auto rows = in_block->rows();
    if (!rows) {
        return Status::OK();
    }
    
    // 使用 Defer 确保在函数返回时更新内存使用统计
    // 无论函数正常返回还是异常返回，都会执行 update_memory_usage()
    Defer defer {[&]() { update_memory_usage(); }};
    
    // ===== 阶段 2：计算分区索引 =====
    // 使用 Partitioner 对数据块进行分区计算
    // 这会计算每行数据属于哪个分区（0 到 partition_count-1）
    {
        /// TODO: DO NOT execute build exprs twice(when partition and building hash table)
        // 注意：这里会计算 Join Key 的 Hash 值，后续构建 Hash 表时可能会重复计算
        // TODO: 优化以避免重复计算
        SCOPED_TIMER(_partition_timer);
        RETURN_IF_ERROR(_partitioner->do_partitioning(state, in_block));
    }

    // ===== 阶段 3：收集每行数据的分区信息 =====
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    SCOPED_TIMER(_partition_shuffle_timer);
    
    // 获取每行数据的分区 ID（channel_ids[i] 表示第 i 行属于哪个分区）
    // 例如：channel_ids[0] = 5 表示第 0 行属于 Partition 5
    const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();
    
    // partition_indexes[i] 存储属于分区 i 的所有行在 in_block 中的索引
    // 例如：partition_indexes[5] = [0, 15, 32, ...] 表示第 0, 15, 32 行属于 Partition 5
    std::vector<std::vector<uint32_t>> partition_indexes(p._partition_count);
    
    // 确保 begin < end（用于处理部分数据的情况）
    DCHECK_LT(begin, end);
    
    // 遍历指定范围内的每一行，将其索引添加到对应的分区索引列表中
    for (size_t i = begin; i != end; ++i) {
        // channel_ids[i] 是第 i 行所属的分区号
        // 将行索引 i 添加到对应分区的索引列表中
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    // ===== 阶段 4：将数据写入对应的分区块 =====
    // 获取共享的分区块数组（所有 Task 共享）
    auto& partitioned_blocks = _shared_state->partitioned_build_blocks;
    
    // 遍历每个分区，将属于该分区的数据写入对应的块
    for (uint32_t i = 0; i != p._partition_count; ++i) {
        // 获取属于分区 i 的行数
        const auto count = partition_indexes[i].size();
        
        // 如果该分区没有数据，跳过
        if (UNLIKELY(count == 0)) {
            continue;
        }

        // 如果该分区的块还未创建，先创建一个空的 MutableBlock
        // 使用 clone_empty() 创建相同结构但无数据的块
        if (UNLIKELY(!partitioned_blocks[i])) {
            partitioned_blocks[i] =
                    vectorized::MutableBlock::create_unique(in_block->clone_empty());
        }
        
        // 将属于分区 i 的所有行从 in_block 复制到 partitioned_blocks[i]
        // partition_indexes[i].data() 指向该分区行索引数组的起始位置
        // partition_indexes[i].data() + count 指向结束位置（不包含）
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(in_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));
        
        // 更新该分区的行数统计（用于监控和调试）
        _rows_in_partitions[i] += count;
    }

    // ===== 阶段 5：更新统计信息 =====
    // 更新最大最小行数计数器（用于性能分析和调试）
    update_max_min_rows_counter();

    return Status::OK();
}

Status PartitionedHashJoinSinkLocalState::_spill_to_disk(
        uint32_t partition_index, const vectorized::SpillStreamSPtr& spilling_stream) {
    auto& partitioned_block = _shared_state->partitioned_build_blocks[partition_index];

    if (!_state->is_cancelled()) {
        auto block = partitioned_block->to_block();
        int64_t block_mem_usage = block.allocated_bytes();
        Defer defer {[&]() { COUNTER_UPDATE(memory_used_counter(), -block_mem_usage); }};
        partitioned_block = vectorized::MutableBlock::create_unique(block.clone_empty());
        return spilling_stream->spill_block(state(), block, false);
    } else {
        return _state->cancel_reason();
    }
}

PartitionedHashJoinSinkOperatorX::PartitionedHashJoinSinkOperatorX(ObjectPool* pool,
                                                                   int operator_id, int dest_id,
                                                                   const TPlanNode& tnode,
                                                                   const DescriptorTbl& descs,
                                                                   uint32_t partition_count)
        : JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>(pool, operator_id, dest_id,
                                                                    tnode, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[1]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {
    _spillable = true;
}

Status PartitionedHashJoinSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    _name = "PARTITIONED_HASH_JOIN_SINK_OPERATOR";
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<TExpr> partition_exprs;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_exprs.emplace_back(eq_join_conjunct.right);
        partition_exprs.emplace_back(eq_join_conjunct.right);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_build_exprs));

    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>::prepare(state));
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_child));
    RETURN_IF_ERROR(_partitioner->prepare(state, _child->row_desc()));
    RETURN_IF_ERROR(_partitioner->open(state));
    return _inner_sink_operator->prepare(state);
}

Status PartitionedHashJoinSinkLocalState::_setup_internal_operator(RuntimeState* state) {
    auto inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    inner_runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    inner_runtime_state->set_be_number(state->be_number());

    inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    inner_runtime_state->resize_op_id_to_local_state(-1);
    inner_runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    auto inner_shared_state = std::dynamic_pointer_cast<HashJoinSharedState>(
            p._inner_sink_operator->create_shared_state());
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = inner_shared_state.get(),
                             .shared_state_map = {},
                             .tsink = {}};

    RETURN_IF_ERROR(p._inner_sink_operator->setup_local_state(inner_runtime_state.get(), info));
    auto* sink_local_state = inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = inner_shared_state.get(),
                               .shared_state_map = {},
                               .task_idx = 0};

    RETURN_IF_ERROR(
            p._inner_probe_operator->setup_local_state(inner_runtime_state.get(), state_info));
    auto* probe_local_state =
            inner_runtime_state->get_local_state(p._inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));
    RETURN_IF_ERROR(sink_local_state->open(state));

    _finish_dependency = sink_local_state->finishdependency()->shared_from_this();

    /// Set these two values after all the work is ready.
    _shared_state->inner_shared_state = std::move(inner_shared_state);
    _shared_state->inner_runtime_state = std::move(inner_runtime_state);
    return Status::OK();
}

#define UPDATE_COUNTER_FROM_INNER(name) \
    update_profile_from_inner_profile<false>(name, custom_profile(), inner_profile)

void PartitionedHashJoinSinkLocalState::update_profile_from_inner() {
    auto* sink_local_state = _shared_state->inner_runtime_state->get_sink_local_state();
    if (sink_local_state) {
        auto* inner_sink_state = assert_cast<HashJoinBuildSinkLocalState*>(sink_local_state);
        auto* inner_profile = inner_sink_state->custom_profile();
        UPDATE_COUNTER_FROM_INNER("BuildHashTableTime");
        UPDATE_COUNTER_FROM_INNER("MergeBuildBlockTime");
        UPDATE_COUNTER_FROM_INNER("BuildTableInsertTime");
        UPDATE_COUNTER_FROM_INNER("BuildExprCallTime");
        UPDATE_COUNTER_FROM_INNER("MemoryUsageBuildBlocks");
        UPDATE_COUNTER_FROM_INNER("MemoryUsageHashTable");
        UPDATE_COUNTER_FROM_INNER("MemoryUsageBuildKeyArena");
    }
}

#undef UPDATE_COUNTER_FROM_INNER

/**
 * @brief 检查可回收内存是否超过高水位线
 *
 * 【功能说明】
 * 在构建 Hash 表之前，检查可回收内存是否超过配置的高水位线。
 * 如果超过，需要触发 Spill，避免构建 Hash 表后无法 Spill 导致查询被取消。
 *
 * 【为什么需要这个检查】
 * 1. Hash 表构建后无法 Spill：
 *    - 一旦调用内部的 HashJoinBuildSinkOperatorX 构建 Hash 表，数据就被组织成 Hash 表结构
 *    - Hash 表结构复杂，无法直接 Spill 到磁盘
 *    - 如果此时内存不足，查询只能被取消
 *
 * 2. 预防性检查：
 *    - 在构建 Hash 表之前，检查内存使用情况
 *    - 如果可回收内存（Build 表数据）超过高水位线，提前触发 Spill
 *    - 这样可以避免构建 Hash 表后内存不足的问题
 *
 * 【高水位线计算】
 * 高水位线 = 查询内存限制 × 高水位百分比
 * 例如：
 * - 查询内存限制：10GB
 * - 高水位百分比：80%
 * - 高水位线：10GB × 80% = 8GB
 * - 如果可回收内存 >= 8GB，返回 true（需要 Spill）
 *
 * 【使用场景】
 * - 在 sink() 方法的 EOS 处理中调用
 * - 在构建 Hash 表之前检查
 * - 如果返回 true，调用 revoke_memory() 触发 Spill
 *
 * @param state 运行时状态，用于获取高水位百分比配置
 * @param revocable_size 可回收内存大小（字节），即 Build 表中可以 Spill 的数据大小
 * @param query_mem_limit 查询内存限制（字节），即查询级别的最大内存使用量
 * @return true 如果可回收内存超过高水位线，需要触发 Spill
 * @return false 如果可回收内存未超过高水位线，可以继续构建 Hash 表
 *
 * 【示例】
 * ```cpp
 * // 查询内存限制：10GB，高水位百分比：80%
 * // 可回收内存：8.5GB
 * // 高水位线：10GB × 80% = 8GB
 * // 8.5GB >= 8GB → 返回 true，需要 Spill
 *
 * // 查询内存限制：10GB，高水位百分比：80%
 * // 可回收内存：7GB
 * // 高水位线：10GB × 80% = 8GB
 * // 7GB < 8GB → 返回 false，可以继续构建 Hash 表
 * ```
 */
static bool is_revocable_mem_high_watermark(RuntimeState* state, size_t revocable_size,
                                            int64_t query_mem_limit) {
    // ===== 阶段 1：获取高水位百分比配置 =====
    // 从 RuntimeState 获取可回收内存高水位百分比配置
    // 这个配置通常通过查询选项或系统配置设置，例如：80% 表示查询内存限制的 80%
    // 如果配置为 0，表示禁用高水位检查（总是返回 false）
    auto revocable_memory_high_watermark_percent =
            state->spill_revocable_memory_high_watermark_percent();

    // ===== 阶段 2：检查是否超过高水位线 =====
    // 条件 1：高水位百分比 > 0（启用高水位检查）
    // 条件 2：可回收内存 >= 查询内存限制 × 高水位百分比
    //
    // 计算示例：
    // - query_mem_limit = 10GB = 10,737,418,240 字节
    // - revocable_memory_high_watermark_percent = 80
    // - 高水位线 = 10,737,418,240 × 80 / 100 = 8,589,934,592 字节 = 8GB
    // - revocable_size = 8.5GB = 9,126,805,504 字节
    // - 9,126,805,504 >= 8,589,934,592 → 返回 true
    return revocable_memory_high_watermark_percent > 0 &&
           static_cast<double>(revocable_size) >=
                   (double)query_mem_limit / 100.0 * revocable_memory_high_watermark_percent;
}

Status PartitionedHashJoinSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                              bool eos) {
    // ===== 功能说明：接收输入数据块，根据是否已 Spill 决定处理方式 =====
    //
    // 【工作流程】
    // 1. 获取本地状态，更新 EOS 标志
    // 2. 如果数据流结束（EOS），计算可回收内存和查询内存限制
    // 3. 处理空数据块（rows == 0）的情况
    // 4. 处理非空数据块：
    //    - 如果已 Spill：进行分区处理，检查是否需要继续 Spill
    //    - 如果未 Spill：调用内部 Sink 算子，检查内存水位决定是否 Spill
    //
    // 【关键设计】
    // - 分区 Hash Join：将 Build 表数据分成多个分区，降低单个 Hash 表的内存占用
    // - 延迟 Spill：只有在内存不足时才 Spill，避免不必要的磁盘 I/O
    // - 内存水位检查：在 EOS 时检查内存水位，防止构建 Hash 表后无法 Spill

    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());

    // ===== 阶段 1：更新本地状态和获取基本信息 =====
    // 更新子节点 EOS 标志（表示上游数据流是否结束）
    local_state._child_eos = eos;
    // 获取输入数据块的行数
    const auto rows = in_block->rows();
    // 检查是否已经进行过 Spill（一旦 Spill，后续数据都需要分区处理）
    const auto is_spilled = local_state._shared_state->is_spilled;

    // ===== 阶段 2：如果数据流结束（EOS），计算内存相关信息 =====
    // 这些信息用于判断是否需要触发 Spill
    size_t revocable_size = 0;      // 可回收内存大小（可以 Spill 的内存）
    int64_t query_mem_limit = 0;    // 查询内存限制
    if (eos) {
        // 计算当前可回收的内存大小（Build 表中可以 Spill 的数据大小）
        revocable_size = revocable_mem_size(state);
        // 获取查询级别的内存限制
        query_mem_limit = state->get_query_ctx()->resource_ctx()->memory_context()->mem_limit();
        // 记录日志，便于调试和监控
        LOG(INFO) << fmt::format(
                "Query:{}, hash join sink:{}, task:{}, eos, need spill:{}, query mem limit:{}, "
                "revocable memory:{}",
                print_id(state->query_id()), node_id(), state->task_id(), is_spilled,
                PrettyPrinter::print_bytes(query_mem_limit),
                PrettyPrinter::print_bytes(revocable_size));
    }

    // ===== 阶段 3：处理空数据块（rows == 0）的情况 =====
    // 空数据块通常表示上游数据流结束或没有数据
    if (rows == 0) {
        // 只有在数据流结束（EOS）时才需要处理
        if (eos) {
            if (is_spilled) {
                // ===== 情况 A：已经 Spill，需要完成最后的 Spill 操作 =====
                // 即使没有新数据，也需要调用 revoke_memory 来完成 Spill 流程
                // 例如：标记所有 SpillStream 为 EOF，通知下游可以读取等
                return revoke_memory(state, nullptr);
            } else {
                // ===== 情况 B：未 Spill，需要检查内存水位并决定是否 Spill =====

                // 调试钩子：用于测试错误处理
                DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::sink_eos", {
                    return Status::Error<INTERNAL_ERROR>(
                            "fault_inject partitioned_hash_join_sink "
                            "sink_eos failed");
                });

                // ===== 检查内存水位 =====
                // 如果可回收内存超过高水位线，触发 Spill
                // 原因：构建 Hash 表后无法再 Spill，必须在构建前检查
                // 例如：查询内存限制 10GB，高水位 80%，可回收内存 8.5GB → 触发 Spill
                if (is_revocable_mem_high_watermark(state, revocable_size, query_mem_limit)) {
                    LOG(INFO) << fmt::format(
                            "Query:{}, hash join sink:{}, task:{} eos, revoke_memory "
                            "because revocable memory is high",
                            print_id(state->query_id()), node_id(), state->task_id());
                    return revoke_memory(state, nullptr);
                }

                // ===== 调用内部 Sink 算子处理数据 =====
                // 使用 Defer 确保在函数退出时更新内存使用统计
                Defer defer {[&]() { local_state.update_memory_usage(); }};
                // 调用内部的 HashJoinBuildSinkOperatorX 处理数据
                // 这会构建 Hash 表，用于后续的 Probe 阶段
                RETURN_IF_ERROR(_inner_sink_operator->sink(
                        local_state._shared_state->inner_runtime_state.get(), in_block, eos));

                // 更新性能统计（从内部算子复制）
                local_state.update_profile_from_inner();

                // 记录日志：未 Spill 情况下的内存使用
                LOG(INFO) << fmt::format(
                        "Query:{}, hash join sink:{}, task:{}, eos, set_ready_to_read, nonspill "
                        "memory usage:{}",
                        print_id(state->query_id()), node_id(), state->task_id(),
                        _inner_sink_operator->get_memory_usage_debug_str(
                                local_state._shared_state->inner_runtime_state.get()));
            }

            // ===== 统计内存中的行数 =====
            // 遍历所有分区的 Build 块，统计总行数（用于性能监控）
            std::ranges::for_each(
                    local_state._shared_state->partitioned_build_blocks, [&](auto& block) {
                        if (block) {
                            COUNTER_UPDATE(local_state._in_mem_rows_counter, block->rows());
                        }
                    });
            // ===== 通知下游可以开始读取 =====
            // 设置依赖关系为就绪状态，允许 Probe 阶段开始执行
            local_state._dependency->set_ready_to_read();
        }
        return Status::OK();
    }

    // ===== 阶段 4：处理非空数据块（rows > 0） =====
    // 更新输入行数计数器（用于性能监控）
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (is_spilled) {
        // ===== 情况 A：已经 Spill，进行分区处理 =====
        // 一旦开始 Spill，后续所有数据都需要分区处理
        // 将输入数据块根据分区表达式分配到不同的分区中
        RETURN_IF_ERROR(local_state._partition_block(state, in_block, 0, rows));

        if (eos) {
            // ===== 数据流结束，完成最后的 Spill =====
            // 将所有分区的数据 Spill 到磁盘，并标记 SpillStream 为 EOF
            return revoke_memory(state, nullptr);
        } else if (revocable_mem_size(state) > vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
            // ===== 可回收内存超过最大批次大小，触发 Spill =====
            // MAX_SPILL_WRITE_BATCH_MEM 通常为 512MB
            // 这样可以批量 Spill，提高磁盘 I/O 效率
            // 例如：可回收内存 600MB > 512MB → 触发 Spill
            return revoke_memory(state, nullptr);
        }
    } else {
        // ===== 情况 B：未 Spill，调用内部 Sink 算子 =====

        // 调试钩子：用于测试错误处理
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_sink::sink", {
            return Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_sink "
                    "sink failed");
        });

        if (eos) {
            // ===== 数据流结束，检查内存水位 =====
            // 如果可回收内存超过高水位线，触发 Spill
            // 原因：构建 Hash 表后无法再 Spill，必须在构建前检查
            if (is_revocable_mem_high_watermark(state, revocable_size, query_mem_limit)) {
                LOG(INFO) << fmt::format(
                        "Query:{}, hash join sink:{}, task:{}, eos, revoke_memory "
                        "because revocable memory is high",
                        print_id(state->query_id()), node_id(), state->task_id());
                return revoke_memory(state, nullptr);
            }
        }
        // ===== 调用内部 Sink 算子处理数据 =====
        // 将数据传递给内部的 HashJoinBuildSinkOperatorX
        // 这会累积数据，准备构建 Hash 表
        RETURN_IF_ERROR(_inner_sink_operator->sink(
                local_state._shared_state->inner_runtime_state.get(), in_block, eos));
        // 更新内存使用统计
        local_state.update_memory_usage();
        // 更新性能统计（从内部算子复制）
        local_state.update_profile_from_inner();
        if (eos) {
            // ===== 数据流结束，通知下游可以开始读取 =====
            LOG(INFO) << fmt::format(
                    "Query:{}, hash join sink:{}, task:{}, eos, set_ready_to_read, nonspill memory "
                    "usage:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    _inner_sink_operator->get_memory_usage_debug_str(
                            local_state._shared_state->inner_runtime_state.get()));
            // 设置依赖关系为就绪状态，允许 Probe 阶段开始执行
            local_state._dependency->set_ready_to_read();
        }
    }

    return Status::OK();
}

size_t PartitionedHashJoinSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revocable_mem_size(state);
}

Status PartitionedHashJoinSinkOperatorX::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    return local_state.revoke_memory(state, spill_context);
}

size_t PartitionedHashJoinSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}

bool PartitionedHashJoinSinkLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
