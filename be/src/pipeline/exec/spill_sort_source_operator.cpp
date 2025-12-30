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

#include "spill_sort_source_operator.h"

#include <glog/logging.h>

#include <cstdint>
#include <limits>

#include "common/status.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "sort_source_operator.h"
#include "util/runtime_profile.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
SpillSortLocalState::SpillSortLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status SpillSortLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    init_spill_write_counters();
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _spill_merge_sort_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillMergeSortTime", 1);
    return Status::OK();
}

Status SpillSortLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }

    RETURN_IF_ERROR(setup_in_memory_sort_op(state));
    return Base::open(state);
}

Status SpillSortLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    return Base::close(state);
}

int SpillSortLocalState::_calc_spill_blocks_to_merge(RuntimeState* state) const {
    auto count = state->spill_sort_mem_limit() / state->spill_sort_batch_bytes();
    if (count > std::numeric_limits<int>::max()) [[unlikely]] {
        return std::numeric_limits<int>::max();
    }
    return std::max(2, static_cast<int32_t>(count));
}

Status SpillSortLocalState::initiate_merge_sort_spill_streams(RuntimeState* state) {
    auto& parent = Base::_parent->template cast<Parent>();
    VLOG_DEBUG << fmt::format("Query:{}, sort source:{}, task:{}, merge spill data",
                              print_id(state->query_id()), _parent->node_id(), state->task_id());

    auto query_id = state->query_id();

    auto spill_func = [this, state, query_id, &parent] {
        SCOPED_TIMER(_spill_merge_sort_timer);
        Status status;
        Defer defer {[&]() {
            if (!status.ok() || state->is_cancelled()) {
                if (!status.ok()) {
                    LOG(WARNING) << fmt::format(
                            "Query:{}, sort source:{}, task:{}, merge spill data error:{}",
                            print_id(query_id), _parent->node_id(), state->task_id(), status);
                }
                for (auto& stream : _current_merging_streams) {
                    ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                }
                _current_merging_streams.clear();
            } else {
                VLOG_DEBUG << fmt::format(
                        "Query:{}, sort source:{}, task:{}, merge spill data finish",
                        print_id(query_id), _parent->node_id(), state->task_id());
            }
        }};
        vectorized::Block merge_sorted_block;
        vectorized::SpillStreamSPtr tmp_stream;
        while (!state->is_cancelled()) {
            int max_stream_count = _calc_spill_blocks_to_merge(state);
            VLOG_DEBUG << fmt::format(
                    "Query:{}, sort source:{}, task:{}, merge spill streams, streams count:{}, "
                    "curren merge max stream count:{}",
                    print_id(query_id), _parent->node_id(), state->task_id(),
                    _shared_state->sorted_streams.size(), max_stream_count);
            {
                SCOPED_TIMER(Base::_spill_recover_time);
                status = _create_intermediate_merger(
                        max_stream_count,
                        parent._sort_source_operator->get_sort_description(_runtime_state.get()));
            }
            RETURN_IF_ERROR(status);

            // all the remaining streams can be merged in a run
            if (_shared_state->sorted_streams.empty()) {
                return Status::OK();
            }

            {
                int32_t batch_size =
                        _shared_state->spill_block_batch_row_count >
                                        std::numeric_limits<int32_t>::max()
                                ? std::numeric_limits<int32_t>::max()
                                : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
                status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, tmp_stream, print_id(state->query_id()), "sort", _parent->node_id(),
                        batch_size, state->spill_sort_batch_bytes(), operator_profile());
                RETURN_IF_ERROR(status);

                _shared_state->sorted_streams.emplace_back(tmp_stream);

                bool eos = false;
                while (!eos && !state->is_cancelled()) {
                    merge_sorted_block.clear_column_data();
                    {
                        SCOPED_TIMER(Base::_spill_recover_time);
                        DBUG_EXECUTE_IF("fault_inject::spill_sort_source::recover_spill_data", {
                            status = Status::Error<INTERNAL_ERROR>(
                                    "fault_inject spill_sort_source "
                                    "recover_spill_data failed");
                        });
                        if (status.ok()) {
                            status = _merger->get_next(&merge_sorted_block, &eos);
                        }
                    }
                    RETURN_IF_ERROR(status);
                    status = tmp_stream->spill_block(state, merge_sorted_block, eos);
                    if (status.ok()) {
                        DBUG_EXECUTE_IF("fault_inject::spill_sort_source::spill_merged_data", {
                            status = Status::Error<INTERNAL_ERROR>(
                                    "fault_inject spill_sort_source "
                                    "spill_merged_data failed");
                        });
                    }
                    RETURN_IF_ERROR(status);
                }
            }
            for (auto& stream : _current_merging_streams) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            _current_merging_streams.clear();
        }
        return Status::OK();
    };

    auto exception_catch_func = [spill_func]() {
        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::spill_sort_source::merge_sort_spill_data_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject spill_sort_source "
                "merge_sort_spill_data submit_func failed");
    });

    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

Status SpillSortLocalState::_create_intermediate_merger(
        int num_blocks, const vectorized::SortDescription& sort_description) {
    std::vector<vectorized::BlockSupplier> child_block_suppliers;
    int64_t limit = -1;
    int64_t offset = 0;
    if (num_blocks >= _shared_state->sorted_streams.size()) {
        // final round use real limit and offset
        limit = Base::_shared_state->limit;
        offset = Base::_shared_state->offset;
    }

    _merger = std::make_unique<vectorized::VSortedRunMerger>(
            sort_description, _runtime_state->batch_size(), limit, offset, custom_profile());

    _current_merging_streams.clear();
    for (int i = 0; i < num_blocks && !_shared_state->sorted_streams.empty(); ++i) {
        auto stream = _shared_state->sorted_streams.front();
        stream->set_read_counters(operator_profile());
        _current_merging_streams.emplace_back(stream);
        child_block_suppliers.emplace_back([stream](vectorized::Block* block, bool* eos) {
            return stream->read_next_block_sync(block, eos);
        });

        _shared_state->sorted_streams.pop_front();
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

bool SpillSortLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

Status SpillSortLocalState::setup_in_memory_sort_op(RuntimeState* state) {
    // ===== 阶段 1：创建独立的 RuntimeState 用于内部 SortSourceOperatorX =====
    // 注意：这里创建一个新的 RuntimeState，而不是直接使用参数 state
    // 原因：
    // 1. SortSourceOperatorX 需要独立的 RuntimeState 来管理其本地状态（LocalState）
    // 2. 避免与外部 RuntimeState 的状态冲突，确保资源隔离
    // 3. 便于独立管理 SortSourceOperatorX 的生命周期和资源释放
    // 新创建的 RuntimeState 会复制参数 state 的关键信息，但保持独立
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    
    // ===== 阶段 2：从参数 state 复制必要的配置信息到新的 RuntimeState =====
    // 设置任务执行上下文，让新 RuntimeState 可以访问 PipelineFragmentContext
    // 这对于算子之间的依赖关系和共享状态管理很重要
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    // 设置后端节点编号，用于标识该 RuntimeState 运行在哪个 BE 节点上
    _runtime_state->set_be_number(state->be_number());

    // 设置描述符表（DescriptorTable），包含表结构、列信息等
    // 注意：这里传递的是引用，因为 DescriptorTable 是共享的，不需要复制
    _runtime_state->set_desc_tbl(&state->desc_tbl());
    // 为所有算子预留本地状态的存储空间
    // max_operator_id() 表示该 Fragment 中最大的算子 ID，用于确定数组大小
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    // 设置运行时过滤器管理器，用于接收和推送运行时过滤器
    // 运行时过滤器可以在查询执行过程中动态生成和推送，用于优化数据扫描
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    // ===== 阶段 3：准备 LocalStateInfo，用于初始化 SortSourceOperatorX 的本地状态 =====
    // 确保共享状态中存在内存排序的共享状态（in_mem_shared_state）
    // 这个共享状态包含了内存中已排序的数据，SortSourceOperatorX 需要从中读取
    DCHECK(_shared_state->in_mem_shared_state);
    // 构建 LocalStateInfo，包含：
    // - parent_profile: 父级性能监控 Profile（使用内部的 _internal_runtime_profile）
    // - scan_ranges: 扫描范围（空，因为数据已经在内存中）
    // - shared_state: 内存排序的共享状态（包含已排序的数据）
    // - shared_state_map: 共享状态映射（空，因为只有一个共享状态）
    // - task_idx: 任务索引（0，因为这是单个任务的操作）
    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = _shared_state->in_mem_shared_state,
                               .shared_state_map = {},
                               .task_idx = 0};

    // ===== 阶段 4：为内部的 SortSourceOperatorX 设置本地状态 =====
    // 获取父算子（SpillSortSourceOperatorX）的引用
    auto& parent = Base::_parent->template cast<Parent>();
    // 调用 SortSourceOperatorX 的 setup_local_state 方法
    // 这会创建 SortSourceOperatorX 的本地状态（SortLocalState），并初始化相关资源
    // 使用新创建的 _runtime_state，确保 SortSourceOperatorX 有独立的运行时环境
    RETURN_IF_ERROR(
            parent._sort_source_operator->setup_local_state(_runtime_state.get(), state_info));

    // ===== 阶段 5：打开 SortSourceOperatorX 的本地状态 =====
    // 从新创建的 RuntimeState 中获取 SortSourceOperatorX 的本地状态
    // 这个本地状态是在上一步 setup_local_state 中创建的
    auto* source_local_state =
            _runtime_state->get_local_state(parent._sort_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    // 打开本地状态，进行最后的初始化工作（如打开排序器、准备读取等）
    // 注意：这里传入的是参数 state，而不是 _runtime_state.get()
    // 这是因为某些操作（如内存追踪）可能需要使用原始的 RuntimeState
    return source_local_state->open(state);
}
SpillSortSourceOperatorX::SpillSortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                   int operator_id, const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {
    _sort_source_operator = std::make_unique<SortSourceOperatorX>(pool, tnode, operator_id, descs);
}
Status SpillSortSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    _op_name = "SPILL_SORT_SOURCE_OPERATOR";
    return _sort_source_operator->init(tnode, state);
}

Status SpillSortSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _sort_source_operator->prepare(state);
}

Status SpillSortSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _sort_source_operator->close(state);
}

Status SpillSortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) {
    // 获取当前 PipelineTask 对应的本地状态
    // 每个 PipelineTask 都有独立的本地状态实例，用于管理该 task 的排序结果读取
    auto& local_state = get_local_state(state);
    // 复制共享的 spill profile 信息到本地状态，用于性能监控
    local_state.copy_shared_spill_profile();
    Status status;
    // 使用 Defer 确保在函数退出时（无论成功还是失败）清理资源：
    // - 关闭 shared_state（释放排序相关的共享资源）
    // - 删除当前正在合并的 spill streams（释放磁盘文件句柄）
    // - 重置 merger（释放多路归并器的资源）
    // 这样可以避免资源泄漏，特别是在异常情况下
    Defer defer {[&]() {
        if (!status.ok() || *eos) {
            local_state._shared_state->close();
            for (auto& stream : local_state._current_merging_streams) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
            local_state._current_merging_streams.clear();
            local_state._merger.reset();
        }
    }};
    // 记录本次 get_block 的执行时间，用于性能分析
    SCOPED_TIMER(local_state.exec_time_counter());

    // 判断数据是否已经溢出到磁盘
    // is_spilled == true 表示 SortSink 在执行排序时内存不足，已将部分数据写入磁盘
    if (local_state._shared_state->is_spilled) {
        // 情况 1：数据已溢出，但 merger 还未初始化
        // 需要先初始化多路归并器，将多个 spill streams 合并成一个有序流
        // initiate_merge_sort_spill_streams 会：
        // - 从 shared_state 中获取所有已排序的 spill streams
        // - 创建 VSortedRunMerger 多路归并器
        // - 将多个 spill streams 作为输入，准备进行多路归并
        if (!local_state._merger) {
            status = local_state.initiate_merge_sort_spill_streams(state);
            return status;
        } else {
            // 情况 2：数据已溢出，merger 已初始化
            // 从多路归并器中读取下一个有序的数据块
            // merger 会将多个已排序的 spill streams 合并成一个全局有序的数据流
            SCOPED_TIMER(local_state._spill_total_timer);
            status = local_state._merger->get_next(block, eos);
            RETURN_IF_ERROR(status);
        }
    } else {
        // 情况 3：数据仍在内存中（未溢出）
        // 直接从内存中的 SortSourceOperatorX 读取排序结果
        // _sort_source_operator 是包装的普通排序 Source 算子，用于处理全内存排序场景
        // 它从 shared_state 中的内存排序器（如 FullSorter）读取已排序的数据
        status = _sort_source_operator->get_block(local_state._runtime_state.get(), block, eos);
        RETURN_IF_ERROR(status);
    }
    // 处理 LIMIT 子句：如果查询设置了 LIMIT，需要检查是否已达到限制行数
    // reached_limit 会：
    // - 检查当前已返回的行数是否达到 LIMIT 限制
    // - 如果达到，截断 block 并设置 eos = true，表示不再需要更多数据
    local_state.reached_limit(block, eos);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline