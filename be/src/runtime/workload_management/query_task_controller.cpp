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

#include "runtime/workload_management/query_task_controller.h"

#include "pipeline/pipeline_fragment_context.h"
#include "runtime/query_context.h"
#include "runtime/workload_management/task_controller.h"

namespace doris {
#include "common/compile_check_begin.h"

std::unique_ptr<TaskController> QueryTaskController::create(
        std::shared_ptr<QueryContext> query_ctx) {
    return QueryTaskController::create_unique(query_ctx);
}

bool QueryTaskController::is_cancelled() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    return query_ctx->is_cancelled();
}

bool QueryTaskController::cancel_impl(const Status& reason, int fragment_id) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    query_ctx->cancel(reason, fragment_id);
    return true;
}

bool QueryTaskController::is_pure_load_task() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    return query_ctx->is_pure_load_task();
}

int32_t QueryTaskController::get_slot_count() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return 0;
    }
    return query_ctx->get_slot_count();
}

bool QueryTaskController::is_enable_reserve_memory() const {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return false;
    }
    return query_ctx->query_options().__isset.enable_reserve_memory &&
           query_ctx->query_options().enable_reserve_memory && enable_reserve_memory_;
}

void QueryTaskController::set_memory_sufficient(bool sufficient) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return;
    }
    query_ctx->set_memory_sufficient(sufficient);
}

int64_t QueryTaskController::memory_sufficient_time() {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return 0;
    }
    return query_ctx->get_memory_sufficient_dependency()->watcher_elapse_time();
}

void QueryTaskController::get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                                             bool* has_running_task) {
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return;
    }
    *revocable_size = 0;
    std::lock_guard<std::mutex> lock(query_ctx->_pipeline_map_write_lock);
    for (auto&& [fragment_id, fragment_wptr] : query_ctx->_fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }

        *revocable_size += fragment_ctx->get_revocable_size(has_running_task);

        // Should wait for all tasks are not running before revoking memory.
        if (*has_running_task) {
            break;
        }
    }

    *memory_usage = query_ctx->query_mem_tracker()->consumption();
}

size_t QueryTaskController::get_revocable_size() {
    size_t revocable_size = 0;
    size_t memory_usage = 0;
    bool has_running_task;
    get_revocable_info(&revocable_size, &memory_usage, &has_running_task);
    return revocable_size;
}

/**
 * 触发查询的内存回收（Spill）操作
 * 
 * 【功能说明】
 * 这是查询级别的内存回收方法，负责收集查询中所有可回收的 Task，选择需要 Spill 的 Task，
 * 创建 SpillContext 来跟踪 Spill 进度，然后对每个选中的 Task 触发 Spill。
 * 
 * 【工作流程】
 * 1. 收集所有 Fragment 中可回收的 Task（按可回收内存大小排序）
 * 2. 计算目标回收内存大小（当前内存使用的 20%）
 * 3. 选择需要 Spill 的 Task（直到达到目标回收大小）
 * 4. 创建 SpillContext（用于跟踪多个 Task 的 Spill 进度）
 * 5. 对每个选中的 Task 调用 revoke_memory()，触发 Spill
 * 
 * 【调用链】
 * WorkloadGroupManager::handle_single_query_()
 *   └─→ QueryTaskController::revoke_memory() (当前方法)
 *       └─→ PipelineTask::revoke_memory()
 *           └─→ 创建 RevokableTask 并提交到调度器
 *               └─→ Sink::revoke_memory() (实际执行 Spill)
 * 
 * 【返回值】
 * - Status::OK(): Spill 任务已成功提交
 * - 其他: Spill 过程中出现错误
 */
Status QueryTaskController::revoke_memory() {
    // 获取查询上下文（使用 weak_ptr 避免循环引用）
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        // 查询已结束，直接返回
        return Status::OK();
    }
    
    // 阶段 1：收集所有可回收的 Task
    // tasks: 存储 <可回收内存大小, Task 指针> 的配对
    std::vector<std::pair<size_t, pipeline::PipelineTask*>> tasks;
    // fragments: 存储 Fragment 上下文（保持引用，防止被释放）
    std::vector<std::shared_ptr<pipeline::PipelineFragmentContext>> fragments;
    
    // 加锁保护 Fragment 映射表的访问
    std::lock_guard<std::mutex> lock(query_ctx->_pipeline_map_write_lock);
    // 遍历查询的所有 Fragment
    for (auto&& [fragment_id, fragment_wptr] : query_ctx->_fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            // Fragment 已释放，跳过
            continue;
        }

        // 获取该 Fragment 中所有可回收的 Task
        auto tasks_of_fragment = fragment_ctx->get_revocable_tasks();
        // 将每个 Task 及其可回收内存大小添加到列表中
        for (auto* task : tasks_of_fragment) {
            tasks.emplace_back(task->get_revocable_size(), task);
        }
        // 保存 Fragment 上下文引用，防止在处理过程中被释放
        fragments.emplace_back(std::move(fragment_ctx));
    }

    // 阶段 2：按可回收内存大小降序排序（优先 Spill 内存占用大的 Task）
    std::sort(tasks.begin(), tasks.end(), [](auto&& l, auto&& r) { return l.first > r.first; });

    // 阶段 3：计算目标回收内存大小
    // 不使用内存限制，使用当前内存使用量
    // 例如：如果当前限制是 1.6GB，但当前使用是 1GB，如果预留失败
    // 应该释放 200MB 内存（1GB * 20%），而不是 300MB（1.6GB * 20%）
    const auto target_revoking_size = static_cast<int64_t>(
            static_cast<double>(query_ctx->query_mem_tracker()->consumption()) * 0.2);
    size_t revoked_size = 0;           // 已选择的 Task 的可回收内存总和
    size_t total_revokable_size = 0;    // 所有 Task 的可回收内存总和（用于日志）

    // 阶段 4：选择需要 Spill 的 Task
    std::vector<pipeline::PipelineTask*> chosen_tasks;
    for (auto&& [revocable_size, task] : tasks) {
        // 只回收最大的 Task，确保内存尽可能被使用
        // 注释掉的 break 表示：如果只想回收最大的 Task，可以在这里 break
        // 当前实现：回收多个 Task，直到达到目标回收大小
        if (revoked_size < target_revoking_size) {
            chosen_tasks.emplace_back(task);
            revoked_size += revocable_size;
        }
        total_revokable_size += revocable_size;
    }

    // 阶段 5：创建 SpillContext（用于跟踪多个 Task 的 Spill 进度）
    std::weak_ptr<QueryContext> this_ctx = query_ctx;
    auto spill_context = std::make_shared<pipeline::SpillContext>(
            chosen_tasks.size(), query_ctx->query_id(),
            // Spill 完成回调：当所有 Task 的 Spill 完成时调用
            [this_ctx, this](pipeline::SpillContext* context) {
                auto query_context = this_ctx.lock();
                if (!query_context) {
                    // 查询已结束，不需要恢复
                    return;
                }

                // 所有 Spill 任务完成，恢复查询执行
                LOG(INFO) << debug_string() << ", context: " << ((void*)context)
                          << " all spill tasks done, resume it.";
                query_context->set_memory_sufficient(true);
            });

    // 记录日志：Spill 上下文信息、可回收内存、Task 数量等
    LOG(INFO) << fmt::format(
            "{}, spill context: {}, revokable mem: {}/{}, tasks count: {}/{}", debug_string(),
            ((void*)spill_context.get()), PrettyPrinter::print_bytes(revoked_size),
            PrettyPrinter::print_bytes(total_revokable_size), chosen_tasks.size(), tasks.size());

    // 阶段 6：对每个选中的 Task 触发 Spill
    // 这会创建 RevokableTask 并提交到调度器异步执行
    for (auto* task : chosen_tasks) {
        RETURN_IF_ERROR(task->revoke_memory(spill_context));
    }
    return Status::OK();
}

std::vector<pipeline::PipelineTask*> QueryTaskController::get_revocable_tasks() {
    std::vector<pipeline::PipelineTask*> tasks;
    auto query_ctx = query_ctx_.lock();
    if (query_ctx == nullptr) {
        return tasks;
    }
    std::lock_guard<std::mutex> lock(query_ctx->_pipeline_map_write_lock);
    for (auto&& [fragment_id, fragment_wptr] : query_ctx->_fragment_id_to_pipeline_ctx) {
        auto fragment_ctx = fragment_wptr.lock();
        if (!fragment_ctx) {
            continue;
        }
        auto tasks_of_fragment = fragment_ctx->get_revocable_tasks();
        tasks.insert(tasks.end(), tasks_of_fragment.cbegin(), tasks_of_fragment.cend());
    }
    return tasks;
}

#include "common/compile_check_end.h"
} // namespace doris
