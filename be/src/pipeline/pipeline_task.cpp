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

#include "pipeline_task.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/scan_operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "revokable_task.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {
#include "common/compile_check_begin.h"

PipelineTask::PipelineTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                           std::shared_ptr<PipelineFragmentContext> fragment_context,
                           RuntimeProfile* parent_profile,
                           std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                                   std::vector<std::shared_ptr<Dependency>>>>
                                   shared_state_map,
                           int task_idx)
        :
#ifdef BE_TEST
          _query_id(fragment_context ? fragment_context->get_query_id() : TUniqueId()),
#else
          _query_id(fragment_context->get_query_id()),
#endif
          _index(task_id),
          _pipeline(pipeline),
          _opened(false),
          _state(state),
          _fragment_context(fragment_context),
          _parent_profile(parent_profile),
          _operators(pipeline->operators()),
          _source(_operators.front().get()),
          _root(_operators.back().get()),
          _sink(pipeline->sink_shared_pointer()),
          _shared_state_map(std::move(shared_state_map)),
          _task_idx(task_idx),
          _memory_sufficient_dependency(state->get_query_ctx()->get_memory_sufficient_dependency()),
          _pipeline_name(_pipeline->name()) {
#ifndef BE_TEST
    _query_mem_tracker = fragment_context->get_query_ctx()->query_mem_tracker();
#endif
    _execution_dependencies.push_back(state->get_query_ctx()->get_execution_dependency());
    if (!_shared_state_map.contains(_sink->dests_id().front())) {
        auto shared_state = _sink->create_shared_state();
        if (shared_state) {
            _sink_shared_state = shared_state;
        }
    }
}

PipelineTask::~PipelineTask() {
    auto reset_member = [&]() {
        _shared_state_map.clear();
        _sink_shared_state.reset();
        _op_shared_states.clear();
        _sink.reset();
        _operators.clear();
        _block.reset();
        _pipeline.reset();
    };
// PipelineTask is also hold by task queue( https://github.com/apache/doris/pull/49753),
// so that it maybe the last one to be destructed.
// But pipeline task hold some objects, like operators, shared state, etc. So that should release
// memory manually.
#ifndef BE_TEST
    if (_query_mem_tracker) {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_mem_tracker);
        reset_member();
        return;
    }
#endif
    reset_member();
}

Status PipelineTask::prepare(const std::vector<TScanRangeParams>& scan_range, const int sender_id,
                             const TDataSink& tsink) {
    DCHECK(_sink);
    _init_profile();
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_prepare_timer);
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::prepare", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task prepare failed");
        return status;
    });
    {
        // set sink local state
        LocalSinkStateInfo info {_task_idx,         _task_profile.get(),
                                 sender_id,         get_sink_shared_state().get(),
                                 _shared_state_map, tsink};
        RETURN_IF_ERROR(_sink->setup_local_state(_state, info));
    }

    _scan_ranges = scan_range;
    auto* parent_profile = _state->get_sink_local_state()->operator_profile();

    for (int op_idx = cast_set<int>(_operators.size() - 1); op_idx >= 0; op_idx--) {
        auto& op = _operators[op_idx];
        LocalStateInfo info {parent_profile, _scan_ranges, get_op_shared_state(op->operator_id()),
                             _shared_state_map, _task_idx};
        RETURN_IF_ERROR(op->setup_local_state(_state, info));
        parent_profile = _state->get_local_state(op->operator_id())->operator_profile();
    }
    {
        const auto& deps =
                _state->get_local_state(_source->operator_id())->execution_dependencies();
        std::unique_lock<std::mutex> lc(_dependency_lock);
        std::copy(deps.begin(), deps.end(),
                  std::inserter(_execution_dependencies, _execution_dependencies.end()));
    }
    if (auto fragment = _fragment_context.lock()) {
        if (fragment->get_query_ctx()->is_cancelled()) {
            terminate();
            return fragment->get_query_ctx()->exec_status();
        }
    } else {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }
    _block = doris::vectorized::Block::create_unique();
    return _state_transition(State::RUNNABLE);
}

Status PipelineTask::_extract_dependencies() {
    std::vector<std::vector<Dependency*>> read_dependencies;
    std::vector<Dependency*> write_dependencies;
    std::vector<Dependency*> finish_dependencies;
    read_dependencies.resize(_operators.size());
    size_t i = 0;
    for (auto& op : _operators) {
        auto* local_state = _state->get_local_state(op->operator_id());
        DCHECK(local_state);
        read_dependencies[i] = local_state->dependencies();
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            finish_dependencies.push_back(fin_dep);
        }
        i++;
    }
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::_extract_dependencies", {
        Status status = Status::Error<INTERNAL_ERROR>(
                "fault_inject pipeline_task _extract_dependencies failed");
        return status;
    });
    {
        auto* local_state = _state->get_sink_local_state();
        write_dependencies = local_state->dependencies();
        auto* fin_dep = local_state->finishdependency();
        if (fin_dep) {
            finish_dependencies.push_back(fin_dep);
        }
    }
    {
        std::unique_lock<std::mutex> lc(_dependency_lock);
        read_dependencies.swap(_read_dependencies);
        write_dependencies.swap(_write_dependencies);
        finish_dependencies.swap(_finish_dependencies);
    }
    return Status::OK();
}

bool PipelineTask::inject_shared_state(std::shared_ptr<BasicSharedState> shared_state) {
    if (!shared_state) {
        return false;
    }
    // Shared state is created by upstream task's sink operator and shared by source operator of
    // this task.
    for (auto& op : _operators) {
        if (shared_state->related_op_ids.contains(op->operator_id())) {
            _op_shared_states.insert({op->operator_id(), shared_state});
            return true;
        }
    }
    // Shared state is created by the first sink operator and shared by sink operator of this task.
    // For example, Set operations.
    if (shared_state->related_op_ids.contains(_sink->dests_id().front())) {
        DCHECK_EQ(_sink_shared_state, nullptr)
                << " Sink: " << _sink->get_name() << " dest id: " << _sink->dests_id().front();
        _sink_shared_state = shared_state;
        return true;
    }
    return false;
}

void PipelineTask::_init_profile() {
    _task_profile = std::make_unique<RuntimeProfile>(fmt::format("PipelineTask(index={})", _index));
    _parent_profile->add_child(_task_profile.get(), true, nullptr);
    _task_cpu_timer = ADD_TIMER(_task_profile, "TaskCpuTime");

    static const char* exec_time = "ExecuteTime";
    _exec_timer = ADD_TIMER(_task_profile, exec_time);
    _prepare_timer = ADD_CHILD_TIMER(_task_profile, "PrepareTime", exec_time);
    _open_timer = ADD_CHILD_TIMER(_task_profile, "OpenTime", exec_time);
    _get_block_timer = ADD_CHILD_TIMER(_task_profile, "GetBlockTime", exec_time);
    _get_block_counter = ADD_COUNTER(_task_profile, "GetBlockCounter", TUnit::UNIT);
    _sink_timer = ADD_CHILD_TIMER(_task_profile, "SinkTime", exec_time);
    _close_timer = ADD_CHILD_TIMER(_task_profile, "CloseTime", exec_time);

    _wait_worker_timer = ADD_TIMER_WITH_LEVEL(_task_profile, "WaitWorkerTime", 1);

    _schedule_counts = ADD_COUNTER(_task_profile, "NumScheduleTimes", TUnit::UNIT);
    _yield_counts = ADD_COUNTER(_task_profile, "NumYieldTimes", TUnit::UNIT);
    _core_change_times = ADD_COUNTER(_task_profile, "CoreChangeTimes", TUnit::UNIT);
    _memory_reserve_times = ADD_COUNTER(_task_profile, "MemoryReserveTimes", TUnit::UNIT);
    _memory_reserve_failed_times =
            ADD_COUNTER(_task_profile, "MemoryReserveFailedTimes", TUnit::UNIT);
}

void PipelineTask::_fresh_profile_counter() {
    COUNTER_SET(_schedule_counts, (int64_t)_schedule_time);
    COUNTER_SET(_wait_worker_timer, (int64_t)_wait_worker_watcher.elapsed_time());
}

Status PipelineTask::_open() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    SCOPED_TIMER(_open_timer);
    _dry_run = _sink->should_dry_run(_state);
    for (auto& o : _operators) {
        RETURN_IF_ERROR(_state->get_local_state(o->operator_id())->open(_state));
    }
    RETURN_IF_ERROR(_state->get_sink_local_state()->open(_state));
    RETURN_IF_ERROR(_extract_dependencies());
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::open", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task open failed");
        return status;
    });
    _opened = true;
    return Status::OK();
}

Status PipelineTask::_prepare() {
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_CPU_TIMER(_task_cpu_timer);
    for (auto& o : _operators) {
        RETURN_IF_ERROR(_state->get_local_state(o->operator_id())->prepare(_state));
    }
    RETURN_IF_ERROR(_state->get_sink_local_state()->prepare(_state));
    return Status::OK();
}

bool PipelineTask::_wait_to_start() {
    // Before task starting, we should make sure
    // 1. Execution dependency is ready (which is controlled by FE 2-phase commit)
    // 2. Runtime filter dependencies are ready
    // 3. All tablets are loaded into local storage
    return std::any_of(
            _execution_dependencies.begin(), _execution_dependencies.end(),
            [&](Dependency* dep) -> bool { return dep->is_blocked_by(shared_from_this()); });
}

bool PipelineTask::_is_pending_finish() {
    // Spilling may be in progress if eos is true.
    return std::ranges::any_of(_finish_dependencies, [&](Dependency* dep) -> bool {
        return dep->is_blocked_by(shared_from_this());
    });
}

bool PipelineTask::is_blockable() const {
    // Before task starting, we should make sure
    // 1. Execution dependency is ready (which is controlled by FE 2-phase commit)
    // 2. Runtime filter dependencies are ready
    // 3. All tablets are loaded into local storage

    if (_state->enable_fuzzy_blockable_task()) {
        if ((_schedule_time + _task_idx) % 2 == 0) {
            return true;
        }
    }

    return std::ranges::any_of(_operators,
                               [&](OperatorPtr op) -> bool { return op->is_blockable(_state); }) ||
           _sink->is_blockable(_state);
}

bool PipelineTask::_is_blocked() {
    // `_dry_run = true` means we do not need data from source operator.
    if (!_dry_run) {
        for (int i = cast_set<int>(_read_dependencies.size() - 1); i >= 0; i--) {
            // `_read_dependencies` is organized according to operators. For each operator, running condition is met iff all dependencies are ready.
            for (auto* dep : _read_dependencies[i]) {
                if (dep->is_blocked_by(shared_from_this())) {
                    return true;
                }
            }
            // If all dependencies are ready for this operator, we can execute this task if no datum is needed from upstream operators.
            if (!_operators[i]->need_more_input_data(_state)) {
                break;
            }
        }
    }
    return _memory_sufficient_dependency->is_blocked_by(shared_from_this()) ||
           std::ranges::any_of(_write_dependencies, [&](Dependency* dep) -> bool {
               return dep->is_blocked_by(shared_from_this());
           });
}

void PipelineTask::terminate() {
    // We use a lock to assure all dependencies are not deconstructed here.
    std::unique_lock<std::mutex> lc(_dependency_lock);
    auto fragment = _fragment_context.lock();
    if (!is_finalized() && fragment) {
        try {
            DCHECK(_wake_up_early || fragment->is_canceled());
            std::ranges::for_each(_write_dependencies,
                                  [&](Dependency* dep) { dep->set_always_ready(); });
            std::ranges::for_each(_finish_dependencies,
                                  [&](Dependency* dep) { dep->set_always_ready(); });
            std::ranges::for_each(_read_dependencies, [&](std::vector<Dependency*>& deps) {
                std::ranges::for_each(deps, [&](Dependency* dep) { dep->set_always_ready(); });
            });
            // All `_execution_deps` will never be set blocking from ready. So we just set ready here.
            std::ranges::for_each(_execution_dependencies,
                                  [&](Dependency* dep) { dep->set_ready(); });
            _memory_sufficient_dependency->set_ready();
        } catch (const doris::Exception& e) {
            LOG(WARNING) << "Terminate failed: " << e.code() << ", " << e.to_string();
        }
    }
}

/**
 * Pipeline Task 的核心执行函数
 *
 * 该函数是 Pipeline 执行引擎的核心，负责在一个调度时间片内执行 Pipeline Task。
 * Pipeline Task 代表一个 Pipeline 的一个实例（Instance），包含完整的算子链：
 * Source Operator → ... → Root Operator → Sink Operator
 *
 * 执行流程：
 * 1. 状态检查：确保任务处于可执行状态（RUNNABLE 且未被依赖阻塞）
 * 2. 初始化：首次执行时打开算子（_open），设置 CPU 计时器和资源跟踪
 * 3. 主执行循环：在当前时间片内，不断从 root 拉取数据并交给 sink 处理
 *    - 从 root 算子开始向上游 pull 数据（pull-based 执行模型）
 *    - 数据经过算子链处理，最终到达 sink
 *    - 如果时间片用完、被阻塞或到达 eos，退出循环
 * 4. 收尾：如果任务未完成，重新提交到调度器队列，等待下次调度
 *
 * 关键概念：
 * - 时间片（time slice）：每次 execute 调用的最大执行时间，用于公平调度
 * - EOS（End of Stream）：数据流结束标志，表示上游不再有数据
 * - Spilling：内存不足时将数据落盘，任务会被阻塞，唤醒后继续处理残留数据
 * - Block：列式存储的数据块，是算子之间传递数据的基本单位
 * - Pull-based：从下游算子向上游算子拉取数据，而非上游主动推送
 *
 * @param done 输出参数，指示任务是否已完成（true 表示任务已完成，可以关闭）
 * @return Status 返回执行状态，成功返回 OK，失败返回相应的错误状态
 */
Status PipelineTask::execute(bool* done) {
    // ========== 第一阶段：状态和前置条件检查 ==========
    
    // 1. 检查任务执行状态
    // 只有在 State::RUNNABLE 且没有被某个 Dependency block 住时，task 才允许真正执行。
    // 如果任务处于其他状态（如 BLOCKED、FINISHED），说明任务不应该被执行，返回错误。
    if (_exec_state != State::RUNNABLE || _blocked_dep != nullptr) [[unlikely]] {
#ifdef BE_TEST
        return Status::InternalError("Pipeline task is not runnable! Task info: {}",
                                     debug_string());
#else
        return Status::FatalError("Pipeline task is not runnable! Task info: {}", debug_string());
#endif
    }

    // 2. 检查 Fragment Context 是否还存在
    // fragment_context 持有整个 fragment 的全局上下文，如果已经被释放说明 fragment 已结束。
    // 使用 weak_ptr 的 lock() 方法获取 shared_ptr，如果对象已被释放则返回空指针。
    auto fragment_context = _fragment_context.lock();
    if (!fragment_context) {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }

    // ========== 第二阶段：初始化和资源跟踪设置 ==========
    
    // 3. 初始化 CPU 计时器和资源跟踪
    // time_spent 用于记录本次执行循环中消耗的时间，用于时间片控制
    int64_t time_spent = 0;
    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    // 将当前线程附加到查询的资源上下文，用于内存跟踪和资源管理
    SCOPED_ATTACH_TASK(_state);
    
    // 4. 设置收尾逻辑（Defer 机制，函数退出时自动执行）
    // running_defer 在本次 execute 结束时统一做收尾工作（更新 CPU 统计、检查是否可以标记 done 等）
    Defer running_defer {[&]() {
        // 更新 CPU 时间统计
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        _task_cpu_timer->update(delta_cpu_time);
        fragment_context->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                delta_cpu_time);

        // 如果 task 被 "wake_up_early"（例如上层决定提前终止、sink 报告 finished 等），
        // 需要终止所有算子并立即将 task 标记为 done。
        if (_wake_up_early) {
            terminate();
            THROW_IF_ERROR(_root->terminate(_state));
            THROW_IF_ERROR(_sink->terminate(_state));
            _eos = true;
            *done = true;
        } else if (_eos && !_spilling &&
                   // eos 且没有 pending 的 finish 依赖（或 fragment 已被取消），说明可以关闭 task
                   // _is_pending_finish() 检查是否还有未完成的 finish 依赖（如等待其他 fragment 完成）
                   (fragment_context->is_canceled() || !_is_pending_finish())) {
            *done = true;
        }
    }};
    
    const auto query_id = _state->query_id();
    
    // ========== 第三阶段：快速路径检查 ==========
    
    // 5. 快速路径：如果已经 eos 且当前没有未输出的 block，说明之前已经完成过本轮执行，直接返回。
    // 这种情况通常发生在任务已经完成，但调度器又调度了一次（可能是并发问题或状态不一致）。
    if (_eos && !_spilling) {
        return Status::OK();
    }
    
    // 6. 处理 Spilling 恢复场景
    // 如果之前被"内存回收/落盘(spilling)"请求阻塞，且刚被唤醒，_block 里可能已经有残留数据。
    // Spilling 是内存不足时的处理机制：将部分数据落盘，释放内存，任务被阻塞。
    // 唤醒后，_block 中可能还有未处理完的数据，此时不会再次被 spilling 依赖 block，直接继续执行。
    if (!_block->empty()) {
        LOG(INFO) << "Query: " << print_id(query_id) << " has pending block, size: "
                  << PrettyPrinter::print_bytes(_block->allocated_bytes());
        DCHECK(_spilling);
    }

    // ========== 第四阶段：执行前准备 ==========
    
    // 7. 设置性能统计计时器
    SCOPED_TIMER(_task_profile->total_time_counter());
    SCOPED_TIMER(_exec_timer);

    // 8. 执行前的准备逻辑（提取依赖、检查是否需要 block 等），首次执行或被唤醒时需要跑一遍。
    // _prepare() 会提取任务的依赖关系，检查是否需要等待某些条件（如 runtime filter 就绪）。
    // 如果 _wake_up_early 为 true，说明任务需要提前终止，跳过 prepare。
    if (!_wake_up_early) {
        RETURN_IF_ERROR(_prepare());
    }
    
    // 调试代码：用于测试错误注入
    DBUG_EXECUTE_IF("fault_inject::PipelineXTask::execute", {
        Status status = Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task execute failed");
        return status;
    });
    
    // 9. 等待启动条件满足
    // `_wake_up_early` must be after `_wait_to_start()`
    // `_wait_to_start()` 主要用于"启动前依赖"（比如 runtime filter、两阶段提交等）检查。
    // 如果启动条件还没满足（如 runtime filter 还未构建完成），或者已经被要求 wake_up_early，
    // 就先返回，等待下次调度。任务会被重新提交到调度器队列。
    if (_wait_to_start() || _wake_up_early) {
        return Status::OK();
    }
    
    // 10. 再次调用 _prepare()（在某些场景下需要）
    // 注意：这里可能会重复调用 _prepare()，但 _prepare() 内部应该有幂等性保证。
    RETURN_IF_ERROR(_prepare());

    // ========== 第五阶段：首次执行时的算子初始化 ==========
    
    // 11. 首次执行时打开算子
    // The status must be runnable
    if (!_opened && !fragment_context->is_canceled()) {
        // 调试代码：用于测试 open 阶段的延迟
        DBUG_EXECUTE_IF("PipelineTask::execute.open_sleep", {
            auto required_pipeline_id =
                    DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                            "PipelineTask::execute.open_sleep", "pipeline_id", -1);
            auto required_task_id = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                    "PipelineTask::execute.open_sleep", "task_id", -1);
            if (required_pipeline_id == pipeline_id() && required_task_id == task_id()) {
                LOG(WARNING) << "PipelineTask::execute.open_sleep sleep 5s";
                sleep(5);
            }
        });

        SCOPED_RAW_TIMER(&time_spent);
        // 首次执行时调用 _open()，对应算子的 open 生命周期（打开 scan range、建结构等）
        // _open() 会依次调用算子链上所有算子的 open() 方法，进行初始化工作：
        // - Source Operator：打开数据源（如文件、网络连接等）
        // - 其他 Operator：初始化内部状态（如 Hash Join 构建 hash table）
        // - Sink Operator：初始化输出目标（如打开文件、建立网络连接等）
        RETURN_IF_ERROR(_open());
    }

    // ========== 第六阶段：主执行循环 ==========
    
    // 12. 主执行循环：在当前调度时间片内，不断从 root 拉数据并喂给 sink
    // 这是 Pipeline 执行的核心循环，采用 pull-based 执行模型：
    // - 从 root 算子开始向上游 pull 数据
    // - 数据经过算子链处理，最终到达 sink
    // - 循环直到：时间片用完、被阻塞、到达 eos 或 fragment 被取消
    while (!fragment_context->is_canceled()) {
        SCOPED_RAW_TIMER(&time_spent);
        
        // 12.1 设置循环内的清理逻辑
        // 如果本轮因为 spilling 请求而中断，那么 _block 可能会在下一轮继续使用，不清空。
        // 否则，正常清空 block 的列数据，为下一轮复用内存（避免重复分配内存）。
        Defer defer {[&]() {
            if (!_spilling) {
                _block->clear_column_data(_root->row_desc().num_materialized_slots());
            }
        }};
        
        // 12.2 检查是否被阻塞或需要提前终止
        // `_wake_up_early` 必须在 `_is_blocked()` 之后检查，以避免刚刚从阻塞状态恢复就被提前终止。
        // _is_blocked() 检查任务是否被某个依赖阻塞（如等待数据、等待内存等）。
        if (_is_blocked() || _wake_up_early) {
            return Status::OK();
        }

        // 12.3 再次检查 fragment 是否被取消
        // 当任务被取消时，其阻塞状态会被清除，任务会转换到 ready 状态（虽然实际上并不 ready）。
        // 这里再次检查是否被取消，以防止处于阻塞状态的任务被重新执行。
        if (fragment_context->is_canceled()) {
            break;
        }

        // 12.4 检查时间片是否用完
        // 如果本次执行已经消耗的时间超过分配的时间片（_exec_time_slice），则退出循环，
        // 让出 CPU 给其他任务，实现公平调度。任务会被重新提交到调度器队列。
        if (time_spent > _exec_time_slice) {
            COUNTER_UPDATE(_yield_counts, 1);
            break;
        }
        
        auto* block = _block.get();

        // 调试代码：用于测试执行阶段的错误注入
        DBUG_EXECUTE_IF("fault_inject::PipelineXTask::executing", {
            Status status =
                    Status::Error<INTERNAL_ERROR>("fault_inject pipeline_task executing failed");
            return status;
        });

        // 12.5 检查 sink 是否已经完成
        // `_sink->is_finished(_state)` 表示 sink operator 已经完成（如已写入足够的数据）。
        // 如果 sink 自己报告已经 finished，则不再拉数据，设置 early wake-up，交回调度器做收尾。
        if (_sink->is_finished(_state)) {
            set_wake_up_early();
            return Status::OK();
        }

        // 12.6 更新 EOS 状态
        // `_dry_run` 表示 sink 不再需要数据（例如某些 limit 场景，已经达到 limit 限制），
        // 此时可以直接认为 eos，不再从上游拉取数据。
        _eos = _dry_run || _eos;
        _spilling = false;
        auto workload_group = _state->workload_group();
        
        // 12.7 从上游拉取数据（Pull-based 执行模型）
        // If last run is pended by a spilling request, `_block` is produced with some rows in last
        // run, so we will resume execution using the block.
        // 如果还没到 eos 且当前缓冲 block 为空，则需要从算子链上游（root）拉一批新数据。
        if (!_eos && _block->empty()) {
            SCOPED_TIMER(_get_block_timer);
            
            // 12.7.1 低内存模式处理
            // 如果系统处于低内存模式，通知 sink 和 root 算子，让它们采用更节省内存的策略。
            if (_state->low_memory_mode()) {
                _sink->set_low_memory_mode(_state);
                _root->set_low_memory_mode(_state);
            }
            
            // 12.7.2 内存预留和释放
            // DEFER_RELEASE_RESERVED() 确保在函数退出时释放预留的内存。
            DEFER_RELEASE_RESERVED();
            _get_block_counter->update(1);
            
            // 12.7.3 获取 root 算子需要预留的内存大小
            // root 算子可能会在 get_block 过程中需要分配内存（如 Hash Join 构建 hash table），
            // 提前预留内存可以避免在 get_block 过程中因内存不足而失败。
            const auto reserve_size = _root->get_reserve_mem_size(_state);
            _root->reset_reserve_mem_size(_state);

            // 12.7.4 尝试预留内存（如果启用了内存预留机制）
            // 如果预留失败（内存不足），则跳过本次循环，等待下次调度时再尝试。
            if (workload_group &&
                _state->get_query_ctx()
                        ->resource_ctx()
                        ->task_controller()
                        ->is_enable_reserve_memory() &&
                reserve_size > 0) {
                if (!_try_to_reserve_memory(reserve_size, _root)) {
                    continue;
                }
            }

            // 12.7.5 从 root 算子开始向上游 pull 数据
            // 这是 pull-based 执行模型的核心：从 root（算子链最末尾的 OperatorX）开始往上游 pull 数据。
            // get_block_after_projects 内部会依次调用各个算子的 get_block，形成调用链：
            // root->get_block() -> child->get_block() -> ... -> source->get_block()
            // 最终到达 Source，从而完成一次"从 Source 到 root 的数据流动"。
            // 数据在流动过程中会被各个算子处理（如过滤、投影、聚合、连接等）。
            bool eos = false;
            RETURN_IF_ERROR(_root->get_block_after_projects(_state, block, &eos));
            RETURN_IF_ERROR(block->check_type_and_column());
            _eos = eos;
        }

        // 12.8 将数据交给 sink 处理
        // 如果当前缓冲 block 非空，或刚好到达 eos（即便 block 为空，也要通知 sink 完成），
        // 则将数据交给 sink 处理。
        if (!_block->empty() || _eos) {
            SCOPED_TIMER(_sink_timer);
            Status status = Status::OK();
            DEFER_RELEASE_RESERVED();
            
            // 12.8.1 为 sink 预留内存（如果启用了内存预留机制）
            // sink 在处理数据时可能需要分配内存（如排序、聚合等），提前预留可以避免失败。
            if (_state->get_query_ctx()
                        ->resource_ctx()
                        ->task_controller()
                        ->is_enable_reserve_memory() &&
                workload_group && !(_wake_up_early || _dry_run)) {
                const auto sink_reserve_size = _sink->get_reserve_mem_size(_state, _eos);
                if (sink_reserve_size > 0 &&
                    !_try_to_reserve_memory(sink_reserve_size, _sink.get())) {
                    continue;
                }
            }

            // 12.8.2 如果到达 EOS，关闭算子链
            // eos 时先关闭算子链上的各个算子（不一定立刻退出 execute 循环）。
            // close() 会依次调用各个算子的 close() 方法，进行清理工作（如关闭文件、释放资源等）。
            if (_eos) {
                RETURN_IF_ERROR(close(Status::OK(), false));
            }

            // 调试代码：用于测试 sink eos 阶段的延迟
            DBUG_EXECUTE_IF("PipelineTask::execute.sink_eos_sleep", {
                auto required_pipeline_id =
                        DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                "PipelineTask::execute.sink_eos_sleep", "pipeline_id", -1);
                auto required_task_id =
                        DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                "PipelineTask::execute.sink_eos_sleep", "task_id", -1);
                if (required_pipeline_id == pipeline_id() && required_task_id == task_id()) {
                    LOG(WARNING) << "PipelineTask::execute.sink_eos_sleep sleep 10s";
                    sleep(10);
                }
            });

            // 调试代码：用于测试任务终止逻辑
            DBUG_EXECUTE_IF("PipelineTask::execute.terminate", {
                if (_eos) {
                    auto required_pipeline_id =
                            DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                    "PipelineTask::execute.terminate", "pipeline_id", -1);
                    auto required_task_id =
                            DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                    "PipelineTask::execute.terminate", "task_id", -1);
                    auto required_fragment_id =
                            DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                                    "PipelineTask::execute.terminate", "fragment_id", -1);
                    if (required_pipeline_id == pipeline_id() && required_task_id == task_id() &&
                        fragment_context->get_fragment_id() == required_fragment_id) {
                        _wake_up_early = true;
                        terminate();
                    } else if (required_pipeline_id == pipeline_id() &&
                               fragment_context->get_fragment_id() == required_fragment_id) {
                        LOG(WARNING) << "PipelineTask::execute.terminate sleep 5s";
                        sleep(5);
                    }
                }
            });
            
            RETURN_IF_ERROR(block->check_type_and_column());
            
            // 12.8.3 将当前 block 交给 sink 处理
            // 将当前 block 交给 sink 处理，sink 的具体行为取决于其类型：
            // - ResultSink：将数据写回客户端或文件
            // - PartitionSortSink：对数据进行分区 hash + 排序，写入 shared_state
            // - OlapTableSink：将数据写入 OLAP 表（如 Stream Load 场景）
            // `status` 为 END_OF_FILE 时说明 sink 不再需要更多数据，可以提前结束。
            status = _sink->sink(_state, block, _eos);

            // 12.8.4 处理 sink 返回的状态
            if (status.is<ErrorCode::END_OF_FILE>()) {
                // sink 返回 END_OF_FILE，表示不再需要更多数据（如 limit 已满足），提前结束。
                set_wake_up_early();
                return Status::OK();
            } else if (!status) {
                // sink 返回错误，直接返回错误状态。
                return status;
            }

            // 12.8.5 如果已经 EOS，退出循环
            // 若已经 eos，直接返回，让调度器在外层基于 `done` 决定是否完全收尾。
            // 调度器会在 running_defer 中检查 _eos 和 _is_pending_finish()，决定是否标记 done。
            if (_eos) { // just return, the scheduler will do finish work
                return Status::OK();
            }
        }
    }

    // ========== 第七阶段：重新提交到调度器 ==========
    
    // 13. 本次时间片结束或被打断后，将自己重新提交回调度器队列，等待下次被调度执行
    // 任务可能因为以下原因退出主循环：
    // - 时间片用完（公平调度）
    // - 被阻塞（等待依赖）
    // - Fragment 被取消
    // 无论哪种原因，只要任务未完成（done 为 false），都需要重新提交到调度器队列，
    // 等待下次被调度执行，直到任务完成。
    RETURN_IF_ERROR(_state->get_query_ctx()->get_pipe_exec_scheduler()->submit(shared_from_this()));
    return Status::OK();
}

Status PipelineTask::do_revoke_memory(const std::shared_ptr<SpillContext>& spill_context) {
    // ===== 功能说明：实际执行内存回收（Spill）操作 =====
    //
    // 【调用位置】
    // RevokableTask::execute()
    //   └─→ PipelineTask::do_revoke_memory() (当前函数)
    //       └─→ Sink::revoke_memory() (实际执行 Spill)
    //
    // 【工作流程】
    // 1. 检查 Fragment Context 是否有效
    // 2. 设置线程资源上下文（用于内存追踪）
    // 3. 启动 CPU 计时器
    // 4. 调用 Sink 的 revoke_memory 执行实际的 Spill 操作
    // 5. 在函数退出时更新 CPU 统计和处理提前唤醒的情况
    //
    // 【与 revoke_memory() 的区别】
    // - revoke_memory()：创建 RevokableTask 并提交到调度器（异步）
    // - do_revoke_memory()：实际执行 Spill 操作（同步，在调度器线程中执行）
    
    // ===== 阶段 1：检查 Fragment Context =====
    // 使用 weak_ptr 的 lock() 方法获取 shared_ptr
    // 如果 Fragment 已经被释放，说明查询已结束，不需要执行 Spill
    auto fragment_context = _fragment_context.lock();
    if (!fragment_context) {
        return Status::InternalError("Fragment already finished! Query: {}", print_id(_query_id));
    }

    // ===== 阶段 2：设置线程资源上下文 =====
    // SCOPED_ATTACH_TASK 会将当前线程附加到查询的资源上下文
    // 作用：
    // - 设置线程的内存追踪器（MemTrackerLimiter），用于追踪内存使用
    // - 设置查询 ID、任务 ID 等上下文信息
    // - 确保该线程的所有内存分配都会记录到查询的内存追踪器中
    //
    // 【为什么需要】
    // - Spill 操作可能会分配内存（如创建 SpillStream、序列化数据等）
    // - 这些内存分配需要记录到查询的内存追踪器中
    // - 如果不设置，内存分配可能无法正确追踪，导致内存泄漏或统计错误
    SCOPED_ATTACH_TASK(_state);
    
    // ===== 阶段 3：启动 CPU 计时器 =====
    // ThreadCpuStopWatch 用于测量 CPU 时间（不包括等待时间）
    // 用于性能监控和资源统计
    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    
    // ===== 阶段 4：设置收尾逻辑（Defer 机制） =====
    // running_defer 在函数退出时（正常返回或异常）自动执行
    // 用于：
    // 1. 更新 CPU 时间统计
    // 2. 处理提前唤醒的情况（如果任务被提前终止）
    Defer running_defer {[&]() {
        // 4.1 计算 CPU 时间增量
        int64_t delta_cpu_time = cpu_time_stop_watch.elapsed_time();
        
        // 4.2 更新任务的 CPU 时间统计
        // _task_cpu_timer 用于记录该任务消耗的 CPU 时间
        _task_cpu_timer->update(delta_cpu_time);
        
        // 4.3 更新查询级别的 CPU 时间统计
        // 用于 WorkloadGroup 的资源管理和限制
        fragment_context->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                delta_cpu_time);

        // ===== 阶段 5：处理提前唤醒的情况 =====
        // _wake_up_early 标志表示任务被提前唤醒（可能是查询被取消或终止）
        // 如果任务被提前唤醒，需要：
        // 1. 终止任务
        // 2. 终止所有算子（Root 和 Sink）
        // 3. 设置 EOS 标志，表示数据流结束
        //
        // 【为什么需要】
        // - Spill 操作可能需要较长时间
        // - 如果查询被取消，需要快速响应，终止 Spill 操作
        // - 避免浪费资源继续执行无用的 Spill
        if (_wake_up_early) {
            // 终止任务状态
            terminate();
            // 终止 Root 算子（清理资源）
            THROW_IF_ERROR(_root->terminate(_state));
            // 终止 Sink 算子（清理资源，停止 Spill）
            THROW_IF_ERROR(_sink->terminate(_state));
            // 设置 EOS 标志，表示数据流结束
            _eos = true;
        }
    }};

    // ===== 阶段 6：执行实际的 Spill 操作 =====
    // 调用 Sink 算子的 revoke_memory 方法
    // 不同的 Sink 有不同的实现：
    // - SortSink：将排序数据写入磁盘
    // - HashJoinSink：将 hash table 分区并写入磁盘
    // - AggSink：将聚合数据分区并写入磁盘
    //
    // 【Spill 过程】
    // 1. 准备 SpillStream（选择磁盘、创建文件）
    // 2. 从内存中读取数据（已排序/已分区）
    // 3. 序列化、压缩数据
    // 4. 写入磁盘文件
    // 5. 释放内存
    //
    // 【异步执行】
    // Spill 操作可能在后台线程执行，但 revoke_memory 会等待完成
    // 如果 Spill 失败，会返回错误状态
    return _sink->revoke_memory(_state, spill_context);
}

bool PipelineTask::_try_to_reserve_memory(const size_t reserve_size, OperatorBase* op) {
    // ===== 功能说明：尝试预留内存，如果失败则触发 Spill =====
    //
    // 【工作流程】
    // 1. 尝试从内存追踪器预留内存
    // 2. 检查是否启用强制 Spill（用于测试）
    // 3. 如果预留失败，检查是否有可回收内存
    // 4. 如果有足够的可回收内存，触发 Spill 并将查询加入暂停列表
    // 5. 如果可回收内存不足，设置低内存模式
    //
    // 【返回值】
    // - true：内存预留成功，可以继续执行
    // - false：内存预留失败，需要 Spill，查询将被暂停
    
    // ===== 阶段 1：尝试预留内存 =====
    // try_reserve 会检查查询级别的内存限制（query_mem_limit）
    // 如果查询内存使用超过限制，返回错误
    // 例如：如果查询限制是 10GB，当前已使用 9.5GB，尝试预留 1GB 会失败
    auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(reserve_size);
    // 更新内存预留次数计数器（用于性能监控）
    COUNTER_UPDATE(_memory_reserve_times, 1);
    
    // ===== 阶段 2：获取 Sink 的可回收内存大小 =====
    // revocable_mem_size 返回可以 spill 到磁盘的内存大小
    // 对于不同的 Sink 算子：
    // - SortSink：返回 sorter->data_size()（排序数据的大小）
    // - HashJoinSink：返回 hash table 的大小
    // - AggSink：返回聚合数据的大小
    // 这个值用于判断是否有足够的数据可以 spill 到磁盘
    auto sink_revocable_mem_size = _sink->revocable_mem_size(_state);
    
    // ===== 阶段 3：检查强制 Spill 模式（用于测试） =====
    // 如果满足以下条件，强制触发 Spill（即使内存预留成功）：
    // 1. 内存预留成功（st.ok()）
    // 2. 启用了强制 Spill（enable_force_spill = true）
    // 3. Sink 支持 Spill（is_spillable() = true）
    // 4. 有足够的可回收内存（>= 32KB）
    // 
    // 【用途】
    // - 用于测试 Spill 功能，即使在内存充足的情况下也触发 Spill
    // - 可以验证 Spill 逻辑的正确性
    if (st.ok() && _state->enable_force_spill() && _sink->is_spillable() &&
        sink_revocable_mem_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
        // 强制设置为内存超限错误，触发 Spill 流程
        st = Status(ErrorCode::QUERY_MEMORY_EXCEEDED, "Force Spill");
    }
    
    // ===== 阶段 4：处理内存预留失败的情况 =====
    if (!st.ok()) {
        // 4.1 更新内存预留失败次数计数器
        COUNTER_UPDATE(_memory_reserve_failed_times, 1);
        
        // 4.2 构建调试信息
        // 包含查询 ID、预留大小、算子信息、可回收内存大小等
        // 用于排查内存问题和性能分析
        auto debug_msg = fmt::format(
                "Query: {} , try to reserve: {}, operator name: {}, operator "
                "id: {}, task id: {}, root revocable mem size: {}, sink revocable mem"
                "size: {}, failed: {}",
                print_id(_query_id), PrettyPrinter::print_bytes(reserve_size), op->get_name(),
                op->node_id(), _state->task_id(),
                PrettyPrinter::print_bytes(op->revocable_mem_size(_state)),
                PrettyPrinter::print_bytes(sink_revocable_mem_size), st.to_string());
        
        // PROCESS_MEMORY_EXCEEDED 错误消息已经包含了进程内存日志
        // 其他错误需要额外添加调试信息
        if (!st.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
            debug_msg +=
                    fmt::format(", debug info: {}", GlobalMemoryArbitrator::process_mem_log_str());
        }
        // 每 100 次失败记录一次日志，避免日志过多
        LOG_EVERY_N(INFO, 100) << debug_msg;
        
        // ===== 阶段 5：检查是否有足够的可回收内存触发 Spill =====
        // MIN_SPILL_WRITE_BATCH_MEM = 32KB
        // 只有当可回收内存 >= 32KB 时才触发 Spill
        // 
        // 【原因】
        // - 避免频繁创建小文件，提高性能
        // - Spill 操作本身有开销，太小的数据不值得 Spill
        // - 32KB 是一个平衡点，既能有效释放内存，又不会产生太多小文件
        if (sink_revocable_mem_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            // 5.1 记录 Spill 触发日志
            LOG(INFO) << fmt::format(
                    "Query: {} sink: {}, node id: {}, task id: "
                    "{}, revocable mem size: {}",
                    print_id(_query_id), _sink->get_name(), _sink->node_id(), _state->task_id(),
                    PrettyPrinter::print_bytes(sink_revocable_mem_size));
            
            // 5.2 将查询添加到暂停列表
            // add_paused_query 会：
            // - 将查询添加到 WorkloadGroup 的暂停队列
            // - 设置查询为低内存模式
            // - 标记内存不足
            // - 等待 WorkloadGroupManager 处理（触发 Spill 或释放缓存）
            //
            // 【暂停机制】
            // - 查询会被暂停执行，等待 Spill 完成或内存释放
            // - WorkloadGroupManager 会定期检查暂停列表，尝试恢复查询
            // - 如果 Spill 成功，查询会从暂停列表移除并继续执行
            ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                    _state->get_query_ctx()->resource_ctx()->shared_from_this(), reserve_size, st);
            
            // 5.3 标记正在 Spill
            // 这个标志用于：
            // - 避免重复触发 Spill
            // - 跟踪 Spill 状态
            _spilling = true;
            
            // 5.4 返回 false，表示内存分配失败，需要 Spill
            // 调用者会检查这个返回值，决定是否继续执行
            return false;
        } else {
            // ===== 阶段 6：可回收内存不足的情况 =====
            // 如果可回收内存 < 32KB，说明：
            // - 数据量太小，不值得 Spill
            // - 或者 Sink 不支持 Spill（revocable_mem_size 返回 0）
            //
            // 【处理策略】
            // - 不将查询添加到暂停列表（因为数据很小，不会消耗太多内存）
            // - 设置低内存模式，限制后续的内存使用
            // - 让查询继续执行，但会限制内存分配
            //
            // 【低内存模式的作用】
            // - 算子会采用更保守的内存策略
            // - 例如：减少批次大小、提前释放内存等
            // - 避免进一步加剧内存压力
            _state->get_query_ctx()->set_low_memory_mode();
        }
    }
    
    // ===== 阶段 7：内存预留成功 =====
    // 返回 true，表示可以继续执行
    return true;
}

void PipelineTask::stop_if_finished() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return;
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker());
    if (auto sink = _sink) {
        if (sink->is_finished(_state)) {
            set_wake_up_early();
            terminate();
        }
    }
}

Status PipelineTask::finalize() {
    auto fragment = _fragment_context.lock();
    if (!fragment) {
        return Status::OK();
    }
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(fragment->get_query_ctx()->query_mem_tracker());
    RETURN_IF_ERROR(_state_transition(State::FINALIZED));
    std::unique_lock<std::mutex> lc(_dependency_lock);
    _sink_shared_state.reset();
    _op_shared_states.clear();
    _shared_state_map.clear();
    _block.reset();
    _operators.clear();
    _sink.reset();
    _pipeline.reset();
    return Status::OK();
}

Status PipelineTask::close(Status exec_status, bool close_sink) {
    int64_t close_ns = 0;
    Status s;
    {
        SCOPED_RAW_TIMER(&close_ns);
        if (close_sink) {
            s = _sink->close(_state, exec_status);
        }
        for (auto& op : _operators) {
            auto tem = op->close(_state);
            if (!tem.ok() && s.ok()) {
                s = tem;
            }
        }
    }
    if (_opened) {
        COUNTER_UPDATE(_close_timer, close_ns);
        COUNTER_UPDATE(_task_profile->total_time_counter(), close_ns);
    }

    if (close_sink && _opened) {
        _task_profile->add_info_string("WakeUpEarly", std::to_string(_wake_up_early.load()));
        _fresh_profile_counter();
    }

    if (close_sink) {
        RETURN_IF_ERROR(_state_transition(State::FINISHED));
    }
    return s;
}

std::string PipelineTask::debug_string() {
    fmt::memory_buffer debug_string_buffer;

    fmt::format_to(debug_string_buffer, "QueryId: {}\n", print_id(_query_id));
    fmt::format_to(debug_string_buffer, "InstanceId: {}\n",
                   print_id(_state->fragment_instance_id()));

    fmt::format_to(debug_string_buffer,
                   "PipelineTask[id = {}, open = {}, eos = {}, state = {}, dry run = "
                   "{}, _wake_up_early = {}, _wake_up_by = {}, time elapsed since last state "
                   "changing = {}s, spilling = {}, is running = {}]",
                   _index, _opened, _eos, _to_string(_exec_state), _dry_run, _wake_up_early.load(),
                   _wake_by, _state_change_watcher.elapsed_time() / NANOS_PER_SEC, _spilling,
                   is_running());
    std::unique_lock<std::mutex> lc(_dependency_lock);
    auto* cur_blocked_dep = _blocked_dep;
    auto fragment = _fragment_context.lock();
    if (is_finalized() || !fragment) {
        fmt::format_to(debug_string_buffer, " pipeline name = {}", _pipeline_name);
        return fmt::to_string(debug_string_buffer);
    }
    auto elapsed = fragment->elapsed_time() / NANOS_PER_SEC;
    fmt::format_to(debug_string_buffer, " elapse time = {}s, block dependency = [{}]\n", elapsed,
                   cur_blocked_dep && !is_finalized() ? cur_blocked_dep->debug_string() : "NULL");

    if (_state && _state->local_runtime_filter_mgr()) {
        fmt::format_to(debug_string_buffer, "local_runtime_filter_mgr: [{}]\n",
                       _state->local_runtime_filter_mgr()->debug_string());
    }

    fmt::format_to(debug_string_buffer, "operators: ");
    for (size_t i = 0; i < _operators.size(); i++) {
        fmt::format_to(debug_string_buffer, "\n{}",
                       _opened && !is_finalized()
                               ? _operators[i]->debug_string(_state, cast_set<int>(i))
                               : _operators[i]->debug_string(cast_set<int>(i)));
    }
    fmt::format_to(debug_string_buffer, "\n{}\n",
                   _opened && !is_finalized()
                           ? _sink->debug_string(_state, cast_set<int>(_operators.size()))
                           : _sink->debug_string(cast_set<int>(_operators.size())));

    fmt::format_to(debug_string_buffer, "\nRead Dependency Information: \n");

    size_t i = 0;
    for (; i < _read_dependencies.size(); i++) {
        for (size_t j = 0; j < _read_dependencies[i].size(); j++) {
            fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                           _read_dependencies[i][j]->debug_string(cast_set<int>(i) + 1));
        }
    }

    fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                   _memory_sufficient_dependency->debug_string(cast_set<int>(i++)));

    fmt::format_to(debug_string_buffer, "\nWrite Dependency Information: \n");
    for (size_t j = 0; j < _write_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _write_dependencies[j]->debug_string(cast_set<int>(j) + 1));
    }

    fmt::format_to(debug_string_buffer, "\nExecution Dependency Information: \n");
    for (size_t j = 0; j < _execution_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _execution_dependencies[j]->debug_string(cast_set<int>(i) + 1));
    }

    fmt::format_to(debug_string_buffer, "Finish Dependency Information: \n");
    for (size_t j = 0; j < _finish_dependencies.size(); j++, i++) {
        fmt::format_to(debug_string_buffer, "{}. {}\n", i,
                       _finish_dependencies[j]->debug_string(cast_set<int>(i) + 1));
    }
    return fmt::to_string(debug_string_buffer);
}

size_t PipelineTask::get_revocable_size() const {
    if (!_opened || is_finalized() || _running || (_eos && !_spilling)) {
        return 0;
    }

    return _sink->revocable_mem_size(_state);
}

Status PipelineTask::revoke_memory(const std::shared_ptr<SpillContext>& spill_context) {
    // ===== 功能说明：触发内存回收（Spill），将任务提交到调度器执行 =====
    //
    // 【完整调用链】
    // 1. 内存分配失败触发 Spill
    //    PipelineTask::_try_to_reserve_memory() 失败
    //      └─→ 将查询添加到暂停列表
    //
    // 2. WorkloadGroupManager 处理暂停的查询
    //    WorkloadGroupManager::handle_paused_queries()
    //      └─→ QueryTaskController::revoke_memory()
    //          ├─→ 收集所有可回收的 Task（按可回收内存大小排序）
    //          ├─→ 选择需要 Spill 的 Task（目标：释放 20% 的内存）
    //          ├─→ 创建 SpillContext（用于跟踪多个任务的 Spill 进度）
    //          └─→ 对每个选中的 Task 调用 revoke_memory() ← 当前函数
    //
    // 3. 当前函数：创建 RevokableTask 并提交到调度器
    //    PipelineTask::revoke_memory() (当前函数)
    //      └─→ 创建 RevokableTask 并提交到调度器
    //
    // 4. 调度器执行 RevokableTask
    //    RevokableTask::execute()
    //      └─→ PipelineTask::do_revoke_memory()
    //          └─→ Sink::revoke_memory() (实际执行 Spill)
    //              ├─→ SortSink::revoke_memory()
    //              ├─→ HashJoinSink::revoke_memory()
    //              └─→ AggSink::revoke_memory()
    //
    // 5. Spill 完成后
    //    SpillContext::on_task_finished()
    //      └─→ 当所有任务完成时，恢复查询执行
    //
    // 【工作流程】
    // 1. 检查任务是否已结束
    // 2. 检查是否有足够的可回收内存
    // 3. 如果有，创建 RevokableTask 并提交到调度器（异步执行 Spill）
    // 4. 如果没有，直接通知 SpillContext 任务完成
    //
    // 【设计说明】
    // - 不直接执行 Spill，而是创建 RevokableTask 提交到调度器
    // - 这样可以异步执行 Spill，不阻塞当前线程
    // - SpillContext 用于跟踪多个任务的 Spill 进度
    
    DCHECK(spill_context);
    
    // ===== 阶段 1：检查任务是否已结束 =====
    // 如果任务已经结束（finalized），不需要执行 Spill
    // 直接通知 SpillContext 任务完成
    if (is_finalized()) {
        spill_context->on_task_finished();
        VLOG_DEBUG << "Query: " << print_id(_state->query_id()) << ", task: " << ((void*)this)
                   << " finalized";
        return Status::OK();
    }

    // ===== 阶段 2：检查是否有足够的可回收内存 =====
    // 获取 Sink 的可回收内存大小
    // 对于不同的 Sink：
    // - SortSink：返回 sorter->data_size()（排序数据的大小）
    // - HashJoinSink：返回 hash table 的大小
    // - AggSink：返回聚合数据的大小
    const auto revocable_size = _sink->revocable_mem_size(_state);
    
    // MIN_SPILL_WRITE_BATCH_MEM = 32KB
    // 只有当可回收内存 >= 32KB 时才执行 Spill
    // 原因：
    // - 避免频繁创建小文件，提高性能
    // - Spill 操作本身有开销，太小的数据不值得 Spill
    if (revocable_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
        // ===== 阶段 3：创建 RevokableTask 并提交到调度器 =====
        // RevokableTask 是一个包装类，用于异步执行 Spill
        // 
        // 【RevokableTask 的作用】
        // - 包装原始的 PipelineTask，提供统一的执行接口
        // - 在 execute() 中调用 do_revoke_memory()，实际执行 Spill
        // - 可以被调度器调度，异步执行
        //
        // 【异步执行的好处】
        // - 不阻塞当前线程（可能是 WorkloadGroupManager 的线程）
        // - 可以在调度器的线程池中执行，充分利用 CPU
        // - 可以控制并发度，避免同时执行太多 Spill 操作
        auto revokable_task = std::make_shared<RevokableTask>(shared_from_this(), spill_context);
        
        // 提交到调度器执行
        // 调度器会在合适的时机（有空闲线程时）执行 RevokableTask
        // RevokableTask::execute() 会调用 do_revoke_memory()
        RETURN_IF_ERROR(_state->get_query_ctx()->get_pipe_exec_scheduler()->submit(revokable_task));
    } else {
        // ===== 阶段 4：可回收内存不足 =====
        // 如果可回收内存 < 32KB，说明：
        // - 数据量太小，不值得 Spill
        // - 或者 Sink 不支持 Spill（revocable_mem_size 返回 0）
        //
        // 【处理策略】
        // - 直接通知 SpillContext 任务完成（因为没有数据需要 Spill）
        // - 记录日志，便于排查问题
        spill_context->on_task_finished();
        LOG(INFO) << "Query: " << print_id(_state->query_id()) << ", task: " << ((void*)this)
                  << " has not enough data to revoke: " << revocable_size;
    }
    return Status::OK();
}

Status PipelineTask::wake_up(Dependency* dep, std::unique_lock<std::mutex>& /* dep_lock */) {
    // call by dependency
    DCHECK_EQ(_blocked_dep, dep) << "dep : " << dep->debug_string(0) << "task: " << debug_string();
    _blocked_dep = nullptr;
    auto holder = std::dynamic_pointer_cast<PipelineTask>(shared_from_this());
    RETURN_IF_ERROR(_state_transition(PipelineTask::State::RUNNABLE));
    RETURN_IF_ERROR(_state->get_query_ctx()->get_pipe_exec_scheduler()->submit(holder));
    return Status::OK();
}

Status PipelineTask::_state_transition(State new_state) {
    if (_exec_state != new_state) {
        _state_change_watcher.reset();
        _state_change_watcher.start();
    }
    _task_profile->add_info_string("TaskState", _to_string(new_state));
    _task_profile->add_info_string("BlockedByDependency", _blocked_dep ? _blocked_dep->name() : "");
    if (!LEGAL_STATE_TRANSITION[(int)new_state].contains(_exec_state)) {
        return Status::InternalError(
                "Task state transition from {} to {} is not allowed! Task info: {}",
                _to_string(_exec_state), _to_string(new_state), debug_string());
    }
    _exec_state = new_state;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
