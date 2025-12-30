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

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <utility>

#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/runtime/partitioner.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
using SpillPartitionerType = vectorized::Crc32HashPartitioner<vectorized::SpillPartitionChannelIds>;

struct SpillContext {
    std::atomic_int running_tasks_count;
    TUniqueId query_id;
    std::function<void(SpillContext*)> all_tasks_finished_callback;

    SpillContext(int running_tasks_count_, TUniqueId query_id_,
                 std::function<void(SpillContext*)> all_tasks_finished_callback_)
            : running_tasks_count(running_tasks_count_),
              query_id(std::move(query_id_)),
              all_tasks_finished_callback(std::move(all_tasks_finished_callback_)) {}

    ~SpillContext() {
        if (running_tasks_count.load() != 0) {
            LOG(WARNING) << "Query: " << print_id(query_id)
                         << " not all spill tasks finished, remaining tasks: "
                         << running_tasks_count.load();
        }
    }

    void on_task_finished() {
        auto count = running_tasks_count.fetch_sub(1);
        if (count == 1) {
            all_tasks_finished_callback(this);
        }
    }
};

class SpillRunnable {
protected:
    SpillRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                  RuntimeProfile* operator_profile, bool is_write,
                  std::function<Status()> spill_exec_func,
                  std::function<Status()> spill_fin_cb = {})
            : _state(state),
              _custom_profile(operator_profile->get_child("CustomCounters")),
              _spill_context(std::move(spill_context)),
              _is_write_task(is_write),
              _spill_exec_func(std::move(spill_exec_func)),
              _spill_fin_cb(std::move(spill_fin_cb)) {
        RuntimeProfile* common_profile = operator_profile->get_child("CommonCounters");
        DCHECK(common_profile != nullptr);
        DCHECK(_custom_profile != nullptr);
        _spill_total_timer = _custom_profile->get_counter("SpillTotalTime");

        if (is_write) {
            _write_wait_in_queue_task_count =
                    _custom_profile->get_counter("SpillWriteTaskWaitInQueueCount");
            _writing_task_count = _custom_profile->get_counter("SpillWriteTaskCount");
            COUNTER_UPDATE(_write_wait_in_queue_task_count, 1);
        }
    }

public:
    virtual ~SpillRunnable() = default;

    /**
     * 执行 Spill 操作
     * 
     * 【功能说明】
     * 这是 SpillRunnable 的核心方法，负责执行 Spill 操作的完整生命周期。
     * 该方法会被提交到后台线程池执行，不阻塞主流程。
     * 
     * 【执行流程】
     * 
     * 1. **计时统计**：
     *    - 开始 SpillTotalTime 计时（总时间）
     *    - 开始 SpillWriteTime/SpillReadTime 计时（具体操作时间）
     * 
     * 2. **任务开始**：
     *    - 调用 `_on_task_started()` 通知任务开始
     *    - 更新统计计数器（等待队列计数、执行中计数等）
     * 
     * 3. **资源清理准备**：
     *    - 使用 Defer 确保函数返回时清理 `_spill_exec_func` 和 `_spill_fin_cb`
     *    - 通过 swap 将函数对象置空，释放捕获的资源（避免循环引用）
     * 
     * 4. **取消检查**：
     *    - 如果查询已取消，直接返回取消原因
     * 
     * 5. **执行 Spill**：
     *    - 调用 `_spill_exec_func()` 执行实际的 Spill 操作
     *    - 如果执行失败，立即返回错误
     * 
     * 6. **任务完成**：
     *    - 调用 `_on_task_finished()` 通知任务完成
     *    - 如果提供了 `_spill_fin_cb`，执行完成回调
     * 
     * 【关键设计】
     * 
     * 1. **资源管理**：
     *    - 使用 Defer 确保资源清理（无论正常返回还是异常返回）
     *    - 通过 swap 将函数对象置空，避免循环引用导致的内存泄漏
     * 
     * 2. **统计跟踪**：
     *    - SpillTotalTime：Spill 操作的总时间（包括等待和执行）
     *    - SpillWriteTime/SpillReadTime：实际 Spill 操作的时间
     *    - SpillWriteTaskWaitInQueueCount：等待队列中的任务数
     *    - SpillWriteTaskCount：正在执行的任务数
     * 
     * 3. **取消支持**：
     *    - 在执行前检查查询是否已取消
     *    - 如果已取消，立即返回，不执行 Spill 操作
     * 
     * 4. **回调机制**：
     *    - `_spill_exec_func`：执行实际的 Spill 操作（必须）
     *    - `_spill_fin_cb`：Spill 完成后的回调（可选）
     *    - 完成回调可以用于：
     *      * 标记 SpillStream 为 EOF
     *      * 通知依赖的算子可以开始读取
     *      * 更新共享状态
     * 
     * 【使用示例】
     * 
     * 示例 1：基本使用
     * ```cpp
     * SpillSinkRunnable runnable(
     *     state, spill_context, operator_profile(),
     *     [this, state]() {
     *         // Spill 执行函数：将数据写入磁盘
     *         for (auto& block : blocks_to_spill) {
     *             RETURN_IF_ERROR(spill_stream->spill_block(state, block, false));
     *         }
     *         return Status::OK();
     *     },
     *     [this, state]() {
     *         // 完成回调：标记 EOF，通知 Probe 算子
     *         RETURN_IF_ERROR(spill_stream->spill_eof());
     *         _dependency->set_ready_to_read();
     *         return Status::OK();
     *     }
     * );
     * 
     * // 提交到后台线程池执行
     * RETURN_IF_ERROR(runnable.run());
     * ```
     * 
     * 示例 2：执行流程
     * ```
     * run() 开始
     *   ├─→ 开始计时（SpillTotalTime、SpillWriteTime）
     *   ├─→ _on_task_started()
     *   │     ├─→ 记录日志
     *   │     └─→ 更新计数器（等待队列 -1，执行中 +1）
     *   ├─→ 设置 Defer（资源清理）
     *   ├─→ 检查取消状态
     *   ├─→ 执行 _spill_exec_func()
     *   │     └─→ 实际的 Spill 操作（写入/读取磁盘）
     *   ├─→ _on_task_finished()
     *   │     └─→ 通知 SpillContext 任务完成
     *   ├─→ 执行 _spill_fin_cb()（如果提供）
     *   │     └─→ 完成回调（标记 EOF、通知依赖等）
     *   └─→ Defer 执行（清理资源）
     * ```
     * 
     * 【注意事项】
     * 1. 该方法通常在后台线程中执行，不阻塞主流程
     * 2. 使用 Defer 确保资源清理，避免内存泄漏
     * 3. 如果查询已取消，会立即返回，不执行 Spill 操作
     * 4. `_spill_exec_func` 和 `_spill_fin_cb` 会在执行后被置空（通过 swap）
     * 5. 如果 `_spill_exec_func` 执行失败，不会执行 `_spill_fin_cb`
     * 
     * @return Status 执行结果
     */
    [[nodiscard]] Status run() {
        // ===== 阶段 1：开始计时统计 =====
        // SpillTotalTime：Spill 操作的总时间（包括等待和执行）
        SCOPED_TIMER(_spill_total_timer);

        // ===== 阶段 2：获取并开始具体操作的计时器 =====
        // 对于写操作：SpillWriteTime
        // 对于读操作：SpillReadTime
        auto* spill_timer = _get_spill_timer();
        DCHECK(spill_timer != nullptr);
        SCOPED_TIMER(spill_timer);

        // ===== 阶段 3：通知任务开始 =====
        // _on_task_started() 会：
        // - 记录调试日志
        // - 更新统计计数器（等待队列计数 -1，执行中计数 +1）
        _on_task_started();

        // ===== 阶段 4：设置资源清理（Defer） =====
        // 使用 Defer 确保在函数返回时（无论正常返回还是异常返回）清理资源
        // 通过 swap 将函数对象置空，释放捕获的资源（避免循环引用导致的内存泄漏）
        Defer defer([&] {
            {
                // 清理 _spill_exec_func
                // swap 会将函数对象的内容交换到临时变量 tmp 中
                // tmp 在作用域结束时自动析构，释放捕获的资源
                std::function<Status()> tmp;
                std::swap(tmp, _spill_exec_func);
            }
            {
                // 清理 _spill_fin_cb
                std::function<Status()> tmp;
                std::swap(tmp, _spill_fin_cb);
            }
        });

        // ===== 阶段 5：检查查询是否已取消 =====
        // 如果查询已取消，立即返回取消原因，不执行 Spill 操作
        // 这样可以避免在查询已取消时继续执行无意义的 Spill 操作
        if (_state->is_cancelled()) {
            return _state->cancel_reason();
        }

        // ===== 阶段 6：执行 Spill 操作 =====
        // 调用 _spill_exec_func() 执行实际的 Spill 操作
        // 这个函数通常包含：
        // - 遍历数据块
        // - 将数据写入磁盘（SpillSinkRunnable）
        // - 从磁盘读取数据（SpillRecoverRunnable）
        // 如果执行失败，立即返回错误
        RETURN_IF_ERROR(_spill_exec_func());
        
        // ===== 阶段 7：通知任务完成 =====
        // _on_task_finished() 会：
        // - 通知 SpillContext 任务完成（如果提供了 spill_context）
        // - SpillContext 会减少 running_tasks_count
        // - 如果所有任务都完成，会调用 all_tasks_finished_callback
        _on_task_finished();
        
        // ===== 阶段 8：执行完成回调（如果提供） =====
        // 如果提供了 _spill_fin_cb，执行完成回调
        // 完成回调通常用于：
        // - 标记 SpillStream 为 EOF
        // - 通知依赖的算子可以开始读取
        // - 更新共享状态
        // 注意：只有在 _spill_exec_func 成功执行后才会执行完成回调
        if (_spill_fin_cb) {
            return _spill_fin_cb();
        }

        return Status::OK();
    }

protected:
    virtual void _on_task_finished() {
        if (_spill_context) {
            _spill_context->on_task_finished();
        }
    }

    virtual RuntimeProfile::Counter* _get_spill_timer() {
        return _custom_profile->get_counter("SpillWriteTime");
    }

    virtual void _on_task_started() {
        VLOG_DEBUG << "Query: " << print_id(_state->query_id())
                   << " spill task started, pipeline task id: " << _state->task_id();
        if (_is_write_task) {
            COUNTER_UPDATE(_write_wait_in_queue_task_count, -1);
            COUNTER_UPDATE(_writing_task_count, 1);
        }
    }

    RuntimeState* _state;
    RuntimeProfile* _custom_profile;
    std::shared_ptr<SpillContext> _spill_context;
    bool _is_write_task;

private:
    RuntimeProfile::Counter* _spill_total_timer;

    RuntimeProfile::Counter* _write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _writing_task_count = nullptr;

    std::function<Status()> _spill_exec_func;
    std::function<Status()> _spill_fin_cb;
};

class SpillSinkRunnable : public SpillRunnable {
public:
    SpillSinkRunnable(RuntimeState* state, std::shared_ptr<SpillContext> spill_context,
                      RuntimeProfile* operator_profile, std::function<Status()> spill_exec_func,
                      std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, spill_context, operator_profile, true, spill_exec_func,
                            spill_fin_cb) {}
};

class SpillNonSinkRunnable : public SpillRunnable {
public:
    SpillNonSinkRunnable(RuntimeState* state, RuntimeProfile* operator_profile,
                         std::function<Status()> spill_exec_func,
                         std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, nullptr, operator_profile, true, spill_exec_func, spill_fin_cb) {
    }
};

class SpillRecoverRunnable : public SpillRunnable {
public:
    SpillRecoverRunnable(RuntimeState* state, RuntimeProfile* operator_profile,
                         std::function<Status()> spill_exec_func,
                         std::function<Status()> spill_fin_cb = {})
            : SpillRunnable(state, nullptr, operator_profile, false, spill_exec_func,
                            spill_fin_cb) {
        RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
        DCHECK(custom_profile != nullptr);
        _spill_revover_timer = custom_profile->get_counter("SpillRecoverTime");
        _read_wait_in_queue_task_count =
                custom_profile->get_counter("SpillReadTaskWaitInQueueCount");
        _reading_task_count = custom_profile->get_counter("SpillReadTaskCount");

        COUNTER_UPDATE(_read_wait_in_queue_task_count, 1);
    }

protected:
    RuntimeProfile::Counter* _get_spill_timer() override {
        return _custom_profile->get_counter("SpillRecoverTime");
    }

    void _on_task_started() override {
        VLOG_DEBUG << "SpillRecoverRunnable, Query: " << print_id(_state->query_id())
                   << " spill task started, pipeline task id: " << _state->task_id();
        COUNTER_UPDATE(_read_wait_in_queue_task_count, -1);
        COUNTER_UPDATE(_reading_task_count, 1);
    }

private:
    RuntimeProfile::Counter* _spill_revover_timer;
    RuntimeProfile::Counter* _read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _reading_task_count = nullptr;
};

template <bool accumulating>
inline void update_profile_from_inner_profile(const std::string& name,
                                              RuntimeProfile* runtime_profile,
                                              RuntimeProfile* inner_profile) {
    auto* inner_counter = inner_profile->get_counter(name);
    DCHECK(inner_counter != nullptr) << "inner counter " << name << " not found";
    if (inner_counter == nullptr) [[unlikely]] {
        return;
    }
    auto* counter = runtime_profile->get_counter(name);
    if (counter == nullptr) [[unlikely]] {
        counter = runtime_profile->add_counter(name, inner_counter->type(), "",
                                               inner_counter->level());
    }
    if constexpr (accumulating) {
        // Memory usage should not be accumulated.
        if (counter->type() == TUnit::BYTES) {
            counter->set(inner_counter->value());
        } else {
            counter->update(inner_counter->value());
        }
    } else {
        counter->set(inner_counter->value());
    }
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline