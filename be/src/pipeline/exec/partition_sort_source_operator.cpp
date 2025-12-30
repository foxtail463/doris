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

#include "partition_sort_source_operator.h"

#include "pipeline/exec/operator.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

Status PartitionSortSourceLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<PartitionSortNodeSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _get_sorted_timer = ADD_TIMER(custom_profile(), "GetSortedTime");
    _sorted_partition_output_rows_counter =
            ADD_COUNTER(custom_profile(), "SortedPartitionOutputRows", TUnit::UNIT);
    return Status::OK();
}

Status PartitionSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* output_block,
                                               bool* eos) {
    RETURN_IF_CANCELLED(state);
    // 获取当前线程绑定的本地状态（包含 shared_state / 依赖 / profile 等）
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    // 每次读取前先清空输出 Block 的数据（只保留列结构）
    output_block->clear_column_data();
    auto get_data_from_blocks_buffer = false;
    {
        // 先尝试从 shared_state 的 blocks_buffer 中读取“旁路（passthrough）”的数据
        std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
        get_data_from_blocks_buffer = !local_state._shared_state->blocks_buffer.empty();
        if (get_data_from_blocks_buffer) {
            // 为了避免拷贝，直接 swap 出队首 Block 到 output_block
            local_state._shared_state->blocks_buffer.front().swap(*output_block);
            local_state._shared_state->blocks_buffer.pop();

            if (local_state._shared_state->blocks_buffer.empty() &&
                !local_state._shared_state->sink_eos) {
                // 这里再持有 sink_eos_lock 检查一次，避免出现：
                // - Source 正准备 block()，而 Sink 恰好在设置 sink_eos 的竞态
                // - 必须在同一把锁下判断 / 设置，保证不会“多 block 一次”
                std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
                // 如果此时缓冲区已经空了且 Sink 还没 eos，则真正进入阻塞，等待 Sink 再次唤醒
                if (!local_state._shared_state->sink_eos) {
                    local_state._dependency->block();
                }
            }
        }
    }

    if (!get_data_from_blocks_buffer) {
        // 如果没有从 blocks_buffer 里拿到数据，说明没有旁路数据可读：
        // 此时从 partition_sorts 中按分区顺序读取排序/TopN 后的结果。
        //
        // 注：ready_for_read 标志由 Sink 通过 local_state._dependency->set_ready_for_read() 设置，
        // 这里必须先消费掉 blocks_buffer 再读 sorted block，避免出现：
        // - child 已经 eos 且 _partition_sorts 已经 push_back 完成
        // - 但 buffer 里还有未输出的数据，如果先读 sorted 再读 buffer，可能导致部分数据永远不输出。
        RETURN_IF_ERROR(get_sorted_block(state, output_block, local_state));
        // 当 _sort_idx >= partition_sorts.size() 时，说明所有分区均已输出完毕，可以设置 eos
        *eos = local_state._sort_idx >= local_state._shared_state->partition_sorts.size();
    }

    if (!output_block->empty()) {
        // 对当前 Block 应用下推到本节点的谓词过滤，并更新返回行数统计
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, output_block,
                                                 output_block->columns()));
        local_state._num_rows_returned += output_block->rows();
    }
    return Status::OK();
}

Status PartitionSortSourceOperatorX::get_sorted_block(RuntimeState* state,
                                                      vectorized::Block* output_block,
                                                      PartitionSortSourceLocalState& local_state) {
    SCOPED_TIMER(local_state._get_sorted_timer);
    // 从当前分区对应的 PartitionSorter 中读取数据，一次输出一批（一个 batch）
    bool current_eos = false;
    // 所有分区对应的 sorter 列表（每个 partition 一个 PartitionSorter）
    auto& sorters = local_state._shared_state->partition_sorts;
    auto sorter_size = sorters.size();
    // 只有在当前分区下标还在有效范围内时，才尝试读取
    if (local_state._sort_idx < sorter_size) {
        // 调用当前分区的 PartitionSorter::get_next，将该分区的一部分有序数据写入 output_block
        RETURN_IF_ERROR(
                sorters[local_state._sort_idx]->get_next(state, output_block, &current_eos));
        // 统计当前调用输出的行数，用于 profile
        COUNTER_UPDATE(local_state._sorted_partition_output_rows_counter, output_block->rows());
    }
    if (current_eos) {
        // 当前分区的数据已经读完，切换到下一个分区
        local_state._sort_idx++;
        // 为了与 Sink 侧设置 prepared_finish 的操作同步，这里持有 prepared_finish_lock
        std::unique_lock<std::mutex> lc(local_state._shared_state->prepared_finish_lock);
        if (local_state._sort_idx < sorter_size &&
            // 如果下一个分区的 sorter 还没准备好（Sink 还未调用 set_prepared_finish），
            // 则通过 dependency 阻塞当前 Source，等待 Sink 侧准备完再继续读取
            !sorters[local_state._sort_idx]->prepared_finish()) {
            local_state._dependency->block();
        }
    }

    return Status::OK();
}

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris