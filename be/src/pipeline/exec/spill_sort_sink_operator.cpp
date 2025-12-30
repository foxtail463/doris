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

#include "spill_sort_sink_operator.h"

#include "common/status.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
SpillSortSinkLocalState::SpillSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
        : Base(parent, state) {}

Status SpillSortSinkLocalState::init(doris::RuntimeState* state,
                                     doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _init_counters();
    RETURN_IF_ERROR(setup_in_memory_sort_op(state));

    Base::_shared_state->in_mem_shared_state->sorter->set_enable_spill();
    return Status::OK();
}

Status SpillSortSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    _shared_state->setup_shared_profile(custom_profile());
    return Base::open(state);
}

void SpillSortSinkLocalState::_init_counters() {
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    _spill_merge_sort_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillMergeSortTime", 1);
}

#define UPDATE_PROFILE(name) \
    update_profile_from_inner_profile<true>(name, custom_profile(), child_profile)

void SpillSortSinkLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_PROFILE("PartialSortTime");
    UPDATE_PROFILE("MergeBlockTime");
    UPDATE_PROFILE("MemoryUsageSortBlocks");
}
#undef UPDATE_PROFILE

Status SpillSortSinkLocalState::close(RuntimeState* state, Status execsink_status) {
    return Base::close(state, execsink_status);
}

Status SpillSortSinkLocalState::setup_in_memory_sort_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->in_mem_shared_state_sptr =
            parent._sort_sink_operator->create_shared_state();
    Base::_shared_state->in_mem_shared_state =
            static_cast<SortSharedState*>(Base::_shared_state->in_mem_shared_state_sptr.get());

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = _internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = Base::_shared_state->in_mem_shared_state,
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(parent._sort_sink_operator->setup_local_state(_runtime_state.get(), info));
    auto* sink_local_state = _runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    RETURN_IF_ERROR(sink_local_state->open(state));

    custom_profile()->add_info_string(
            "TOP-N", *sink_local_state->custom_profile()->get_info_string("TOP-N"));
    return Status::OK();
}

bool SpillSortSinkLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

SpillSortSinkOperatorX::SpillSortSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                               const TPlanNode& tnode, const DescriptorTbl& descs,
                                               bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode.node_id, dest_id) {
    _spillable = true;
    _sort_sink_operator = std::make_unique<SortSinkOperatorX>(pool, operator_id, dest_id, tnode,
                                                              descs, require_bucket_distribution);
}

Status SpillSortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    _name = "SPILL_SORT_SINK_OPERATOR";
    return _sort_sink_operator->init(tnode, state);
}

Status SpillSortSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::prepare(state));
    return _sort_sink_operator->prepare(state);
}

size_t SpillSortSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}
Status SpillSortSinkOperatorX::revoke_memory(RuntimeState* state,
                                             const std::shared_ptr<SpillContext>& spill_context) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state, spill_context);
}

size_t SpillSortSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return _sort_sink_operator->get_revocable_mem_size(local_state._runtime_state.get());
}

Status SpillSortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                                    bool eos) {
    // ===== 功能说明：SpillSortSinkOperatorX 的核心方法，支持磁盘溢写的排序 =====
    // 
    // 【与 SortSinkOperatorX 的区别】
    // - SortSinkOperatorX：全内存排序，不支持 spill
    // - SpillSortSinkOperatorX：支持 spill 到磁盘，当内存不足时可以将数据写入磁盘
    //
    // 【工作流程】
    // 1. 接收上游数据块，更新 spill 相关统计
    // 2. 调用内部的 SortSinkOperatorX 进行排序（可能触发 spill）
    // 3. 更新内存使用统计
    // 4. 当数据流结束时（eos=true），根据是否已 spill 决定处理方式
    
    // ===== 阶段 1：初始化并更新统计信息 =====
    // 获取当前 PipelineTask 对应的本地状态
    auto& local_state = get_local_state(state);
    // 记录本次 sink 操作的执行时间
    SCOPED_TIMER(local_state.exec_time_counter());
    // 更新输入行数计数器
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    
    // ===== 阶段 2：更新 Spill 相关统计信息 =====
    // 如果输入块中有数据，更新 spill 块批次行数
    // 这个值用于计算每次 spill 时写入磁盘的行数，影响 spill 的性能和内存使用
    // 【举例】
    // 如果输入块有 1000 行，平均每行 100 字节，则总大小约 100KB
    // 系统会根据这个信息计算合适的批次大小，用于后续的 spill 操作
    if (in_block->rows() > 0) {
        local_state._shared_state->update_spill_block_batch_row_count(state, in_block);
    }
    // 保存 EOS 标志，用于后续判断数据流是否结束
    local_state._eos = eos;
    
    // ===== 阶段 3：调试钩子（用于测试） =====
    // 用于测试场景，模拟 sink 操作失败的情况
    DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::sink",
                    { return Status::InternalError("fault_inject spill_sort_sink sink failed"); });
    
    // ===== 阶段 4：调用内部 SortSinkOperatorX 进行排序 =====
    // 将数据传递给内部的 SortSinkOperatorX 进行排序
    // 注意：这里传入的是 local_state._runtime_state（内部创建的 RuntimeState），
    // 而不是外部的 state，因为 SortSinkOperatorX 需要独立的运行时环境
    // 
    // 【重要】传入的 eos 参数是 false，而不是外部的 eos
    // 原因：SpillSortSinkOperatorX 需要控制排序的完成时机
    // - 如果传入 true，SortSinkOperatorX 会立即完成排序并准备读取
    // - 但 SpillSortSinkOperatorX 可能还需要处理 spill，所以传入 false
    // - 真正的 EOS 处理在下面的代码中进行
    RETURN_IF_ERROR(_sort_sink_operator->sink(local_state._runtime_state.get(), in_block, false));

    // ===== 阶段 5：更新内存使用统计 =====
    // 获取排序器当前使用的内存大小（包括已排序的数据块）
    // 注意：这里访问的是 in_mem_shared_state->sorter，因为数据可能还在内存中
    // 如果已经 spill，部分数据可能在磁盘上，但内存中可能还有未 spill 的数据
    int64_t data_size = local_state._shared_state->in_mem_shared_state->sorter->data_size();
    // 更新内存使用计数器，用于查询级别的内存追踪和限制
    COUNTER_SET(local_state._memory_used_counter, data_size);

    // ===== 阶段 6：处理数据流结束（EOS） =====
    // 当上游数据流结束时，需要完成排序并通知下游可以开始读取
    if (eos) {
        // ===== 情况 1：数据已经溢出到磁盘（is_spilled = true） =====
        // 说明在排序过程中内存不足，部分数据已经写入磁盘
        // 此时需要：
        // 1. 检查是否还有可回收的内存（未 spill 的数据）
        // 2. 如果有，继续 spill 剩余数据
        // 3. 如果没有，通知下游可以开始读取（从磁盘读取并合并）
        if (local_state._shared_state->is_spilled) {
            // 检查是否还有可回收的内存
            // revocable_mem_size 返回可以 spill 到磁盘的内存大小
            // 例如：如果排序器内存中还有 100MB 数据未 spill，返回 100MB
            if (revocable_mem_size(state) > 0) {
                // 还有未 spill 的数据，继续 spill 到磁盘
                // revoke_memory 会将内存中的数据写入磁盘，释放内存
                // 例如：将剩余的排序数据写入 spill stream，释放内存
                RETURN_IF_ERROR(revoke_memory(state, nullptr));
                // 注意：revoke_memory 完成后，会异步触发下游读取
                // 不需要在这里调用 set_ready_to_read()
            } else {
                // 所有数据都已经 spill 到磁盘，没有可回收的内存
                // 通知下游 Pipeline（SpillSortSourceOperatorX）可以开始读取
                // 下游会从磁盘读取数据并进行多路归并排序
                local_state._dependency->set_ready_to_read();
            }
        } else {
            // ===== 情况 2：数据仍在内存中（is_spilled = false） =====
            // 说明排序过程中内存充足，所有数据都在内存中
            // 此时的处理方式与 SortSinkOperatorX 相同：
            // 1. 完成排序，准备读取
            // 2. 通知下游可以开始读取
            RETURN_IF_ERROR(
                    local_state._shared_state->in_mem_shared_state->sorter->prepare_for_read(
                            false));
            // 通知下游 Pipeline（SortSourceOperatorX）可以开始读取排序结果
            local_state._dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}

size_t SpillSortSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& parent = Base::_parent->template cast<Parent>();
    return parent._sort_sink_operator->get_reserve_mem_size_for_next_sink(_runtime_state.get(),
                                                                          eos);
}

Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state,
                                              const std::shared_ptr<SpillContext>& spill_context) {
    // ===== 功能说明：执行内存回收（Spill），将排序数据写入磁盘 =====
    //
    // 【调用位置】
    // PipelineTask::do_revoke_memory()
    //   └─→ Sink::revoke_memory() (当前函数)
    //       └─→ SpillSinkRunnable::run() (异步执行 Spill)
    //
    // 【工作流程】
    // 1. 标记已 Spill，保存 LIMIT 和 OFFSET
    // 2. 注册 SpillStream（选择磁盘、创建文件）
    // 3. 创建 Spill 执行函数（lambda）
    // 4. 准备排序器供读取
    // 5. 循环读取已排序的数据并写入磁盘
    // 6. 使用 SpillSinkRunnable 异步执行 Spill
    //
    // 【异步执行】
    // Spill 操作在后台线程执行，不阻塞主流程
    // SpillSinkRunnable 会处理异常、统计、资源清理等
    
    auto& parent = Base::_parent->template cast<Parent>();
    
    // ===== 阶段 1：标记已 Spill 并保存查询参数 =====
    // 第一次 Spill 时，设置 is_spilled = true
    // 保存 LIMIT 和 OFFSET，用于后续读取时的数据过滤
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        // 保存 LIMIT 值（用于后续读取时只返回前 N 行）
        _shared_state->limit = parent._sort_sink_operator->limit();
        // 保存 OFFSET 值（用于后续读取时跳过前 M 行）
        _shared_state->offset = parent._sort_sink_operator->offset();
        // 在 Profile 中标记已 Spill（用于监控和调试）
        custom_profile()->add_info_string("Spilled", "true");
    }

    // 记录调试日志
    VLOG_DEBUG << fmt::format("Query:{}, sort sink:{}, task:{}, revoke_memory, eos:{}",
                              print_id(state->query_id()), _parent->node_id(), state->task_id(),
                              _eos);

    // ===== 阶段 2：计算批次大小并注册 SpillStream =====
    // 批次大小用于控制每次写入磁盘的数据量
    // 如果 spill_block_batch_row_count 超过 int32_t 最大值，使用最大值
    int32_t batch_size =
            _shared_state->spill_block_batch_row_count > std::numeric_limits<int32_t>::max()
                    ? std::numeric_limits<int32_t>::max()
                    : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
    
    // 注册 SpillStream
    // SpillStreamManager 会：
    // - 选择可用磁盘（优先 SSD，排除已满的磁盘）
    // - 创建 spill 目录：storage_root/spill/query_id/sort-node_id-task_id-stream_id
    // - 创建 SpillWriter（用于写入）和 SpillReader（用于读取）
    auto status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            state, _spilling_stream, print_id(state->query_id()), "sort", _parent->node_id(),
            batch_size, state->spill_sort_batch_bytes(), operator_profile());
    RETURN_IF_ERROR(status);

    // 将 SpillStream 添加到 sorted_streams 列表
    // 后续 Source 阶段会从这个列表读取数据并进行多路归并
    _shared_state->sorted_streams.emplace_back(_spilling_stream);

    auto query_id = state->query_id();

    // ===== 阶段 3：创建 Spill 执行函数（Lambda） =====
    // spill_func 是实际执行 Spill 的逻辑
    // 它会在后台线程中执行，不阻塞当前线程
    auto spill_func = [this, state, query_id, &parent] {
        Status status;
        
        // ===== 阶段 3.1：设置收尾逻辑（Defer 机制） =====
        // defer 在函数退出时（正常返回或异常）自动执行
        // 用于：
        // 1. 错误处理和资源清理
        // 2. 更新 Spill 任务计数
        // 3. 通知下游可以读取（如果 EOS）
        Defer defer {[&]() {
            // 3.1.1 处理错误或取消的情况
            if (!status.ok() || state->is_cancelled()) {
                if (!status.ok()) {
                    LOG(WARNING) << fmt::format(
                            "Query:{}, sort sink:{}, task:{}, revoke memory error:{}",
                            print_id(query_id), _parent->node_id(), state->task_id(), status);
                }
                // 关闭共享状态，清理资源
                _shared_state->close();
            } else {
                // 3.1.2 Spill 成功完成
                VLOG_DEBUG << fmt::format("Query:{}, sort sink:{}, task:{}, revoke memory finish",
                                          print_id(query_id), _parent->node_id(), state->task_id());
            }

            // 3.1.3 如果状态错误，再次关闭（确保资源清理）
            if (!status.ok()) {
                _shared_state->close();
            }

            // 3.1.4 释放 SpillStream（文件已写入完成）
            _spilling_stream.reset();
            
            // 3.1.5 减少正在 Spill 的任务计数
            // 当所有任务完成时，QueryTaskController 会恢复查询执行
            state->get_query_ctx()
                    ->resource_ctx()
                    ->task_controller()
                    ->decrease_revoking_tasks_count();
            
            // 3.1.6 如果数据流已结束（EOS），通知下游可以读取
            // 下游 Pipeline（SpillSortSourceOperatorX）会开始读取排序结果
            if (_eos) {
                _dependency->set_ready_to_read();
            }
        }};

        // ===== 阶段 3.2：准备排序器供读取 =====
        // prepare_for_spill 内部调用 prepare_for_read(true)
        // 这会完成所有未排序数据的排序，并准备输出
        // 例如：
        // - 完成 unsorted_block 的排序
        // - 将排序后的数据添加到 sorted_blocks
        // - 准备多路归并（如果有多个已排序的块）
        status = parent._sort_sink_operator->prepare_for_spill(_runtime_state.get());
        RETURN_IF_ERROR(status);

        // ===== 阶段 3.3：更新性能 Profile =====
        // 从内部的 SortSinkOperatorX 获取性能统计信息
        // 用于监控和性能分析
        auto* sink_local_state = _runtime_state->get_sink_local_state();
        update_profile(sink_local_state->custom_profile());

        // ===== 阶段 3.4：循环读取已排序的数据并写入磁盘 =====
        bool eos = false;
        vectorized::Block block;

        // 批次大小（每次读取的行数）
        int32_t batch_size =
                _shared_state->spill_block_batch_row_count > std::numeric_limits<int32_t>::max()
                        ? std::numeric_limits<int32_t>::max()
                        : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
        
        // 循环读取直到所有数据都写入磁盘
        while (!eos && !state->is_cancelled()) {
            // 3.4.1 从排序器读取已排序的数据块
            // merge_sort_read_for_spill 会：
            // - 从 sorted_blocks 读取数据
            // - 如果有多个已排序的块，进行多路归并
            // - 返回 batch_size 行数据（或更少，如果已读完）
            {
                SCOPED_TIMER(_spill_merge_sort_timer);  // 记录归并排序时间
                status = parent._sort_sink_operator->merge_sort_read_for_spill(
                        _runtime_state.get(), &block, batch_size, &eos);
            }
            RETURN_IF_ERROR(status);
            
            // 3.4.2 将数据块写入 SpillStream
            // spill_block 内部会：
            // 1. 序列化 Block（转换为 PBlock 格式）
            // 2. 压缩（使用 ZSTD 压缩算法）
            // 3. 写入磁盘文件（追加模式）
            status = _spilling_stream->spill_block(state, block, eos);
            RETURN_IF_ERROR(status);
            
            // 3.4.3 清空 Block，释放内存
            // 数据已写入磁盘，可以释放内存
            block.clear_column_data();
        }
        
        // ===== 阶段 3.5：重置排序器 =====
        // 排序器中的数据已全部写入磁盘，重置内部状态
        // 释放内存，准备接收新的数据（如果有）
        parent._sort_sink_operator->reset(_runtime_state.get());

        return Status::OK();
    };

    // ===== 阶段 4：创建异常捕获函数 =====
    // exception_catch_func 用于捕获 Spill 过程中的异常
    // 包括：
    // - C++ 异常（std::exception）
    // - 调试注入的故障（用于测试）
    auto exception_catch_func = [query_id, state, spill_func]() {
        // 4.1 调试钩子：模拟取消操作（用于测试）
        DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::revoke_memory_cancel", {
            auto status = Status::InternalError(
                    "fault_inject spill_sort_sink "
                    "revoke_memory canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        // 4.2 执行 Spill 函数，捕获所有异常
        // RETURN_IF_CATCH_EXCEPTION 会捕获 C++ 异常并转换为 Status
        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();

        return status;
    };

    // ===== 阶段 5：调试钩子（用于测试） =====
    // 模拟提交函数失败的情况
    DBUG_EXECUTE_IF("fault_inject::spill_sort_sink::revoke_memory_submit_func", {
        status = Status::Error<INTERNAL_ERROR>(
                "fault_inject spill_sort_sink "
                "revoke_memory submit_func failed");
    });

    RETURN_IF_ERROR(status);
    
    // ===== 阶段 6：增加正在 Spill 的任务计数 =====
    // 通知 QueryTaskController 有一个任务正在 Spill
    // 用于：
    // - 跟踪 Spill 进度
    // - 当所有任务完成时，恢复查询执行
    state->get_query_ctx()->resource_ctx()->task_controller()->increase_revoking_tasks_count();

    // ===== 阶段 7：使用 SpillSinkRunnable 执行 Spill =====
    // SpillSinkRunnable 是一个包装类，用于：
    // - 异步执行 Spill（在后台线程）
    // - 处理异常和错误
    // - 更新性能统计（SpillTotalTime、SpillWriteTime 等）
    // - 通知 SpillContext 任务完成
    //
    // 【执行流程】
    // SpillSinkRunnable::run()
    //   ├─→ 更新统计（SpillWriteTaskWaitInQueueCount、SpillWriteTaskCount）
    //   ├─→ 执行 exception_catch_func()
    //   │     └─→ 执行 spill_func()
    //   │           ├─→ 准备排序器
    //   │           ├─→ 循环读取并写入磁盘
    //   │           └─→ 清理资源
    //   └─→ 通知 SpillContext 任务完成
    return SpillSinkRunnable(state, spill_context, operator_profile(), exception_catch_func).run();
}
#include "common/compile_check_end.h"
} // namespace doris::pipeline