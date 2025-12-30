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

#include "workload_group_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "pipeline/task_scheduler.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_metrics.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"
#include "util/threadpool.h"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

#include "common/compile_check_begin.h"

const static std::string INTERNAL_NORMAL_WG_NAME = "normal";
const static uint64_t INTERNAL_NORMAL_WG_ID = 1;

PausedQuery::PausedQuery(std::shared_ptr<ResourceContext> resource_ctx, double cache_ratio,
                         int64_t reserve_size)
        : resource_ctx_(resource_ctx),
          cache_ratio_(cache_ratio),
          reserve_size_(reserve_size),
          query_id_(print_id(resource_ctx->task_controller()->task_id())) {
    enqueue_at = std::chrono::system_clock::now();
}

WorkloadGroupMgr::~WorkloadGroupMgr() = default;

WorkloadGroupMgr::WorkloadGroupMgr() = default;

WorkloadGroupPtr WorkloadGroupMgr::get_or_create_workload_group(
        const WorkloadGroupInfo& fe_wg_info) {
    std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
    // 1. update internal wg's id
    if (fe_wg_info.name == INTERNAL_NORMAL_WG_NAME) {
        // normal wg's id maybe not equal to BE's id, so that need update it
        reset_workload_group_id(INTERNAL_NORMAL_WG_NAME, fe_wg_info.id);
    }

    // 2. check and update wg
    if (LIKELY(_workload_groups.count(fe_wg_info.id))) {
        auto workload_group = _workload_groups[fe_wg_info.id];
        workload_group->check_and_update(fe_wg_info);
        return workload_group;
    }

    auto new_task_group = WorkloadGroup::create_shared(fe_wg_info);
    _workload_groups[fe_wg_info.id] = new_task_group;
    return new_task_group;
}

WorkloadGroupPtr WorkloadGroupMgr::get_group(std::vector<uint64_t>& id_list) {
    WorkloadGroupPtr ret_wg = nullptr;
    int wg_cout = 0;
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& wg_id : id_list) {
            if (_workload_groups.find(wg_id) != _workload_groups.end()) {
                wg_cout++;
                ret_wg = _workload_groups.at(wg_id);
            }
        }
    }

    if (wg_cout > 1) {
        std::stringstream ss;
        ss << "Unexpected error: find too much wg in BE; input id=";

        for (auto& id : id_list) {
            ss << id << ",";
        }

        ss << " be wg: ";
        for (auto& wg_pair : _workload_groups) {
            ss << wg_pair.second->debug_string() << ", ";
        }
        LOG(ERROR) << ss.str();
    }
    DCHECK(wg_cout <= 1);

    if (ret_wg == nullptr) {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& wg_pair : _workload_groups) {
            if (wg_pair.second->name() == INTERNAL_NORMAL_WG_NAME) {
                ret_wg = wg_pair.second;
                break;
            }
        }
    }

    if (ret_wg == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "not even find normal wg in BE");
    }
    return ret_wg;
}

void WorkloadGroupMgr::reset_workload_group_id(std::string workload_group_name, uint64_t new_id) {
    WorkloadGroupPtr wg_ptr = nullptr;
    uint64_t old_wg_id = -1;
    for (auto& wg_pair : _workload_groups) {
        uint64_t wg_id = wg_pair.first;
        WorkloadGroupPtr wg = wg_pair.second;
        if (workload_group_name == wg->name() && wg_id != new_id) {
            wg_ptr = wg_pair.second;
            old_wg_id = wg_id;
            break;
        }
    }
    if (wg_ptr) {
        _workload_groups.erase(old_wg_id);
        wg_ptr->set_id(new_id);
        _workload_groups[wg_ptr->id()] = wg_ptr;
        LOG(INFO) << "workload group's id changed, before: " << old_wg_id
                  << ", after:" << wg_ptr->id();
    }
}

void WorkloadGroupMgr::delete_workload_group_by_ids(std::set<uint64_t> used_wg_id) {
    int64_t begin_time = MonotonicMillis();
    // 1 get delete group without running queries
    std::vector<WorkloadGroupPtr> deleted_task_groups;
    size_t old_wg_size = 0;
    size_t new_wg_size = 0;
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        old_wg_size = _workload_groups.size();
        for (auto& _workload_group : _workload_groups) {
            uint64_t wg_id = _workload_group.first;
            auto workload_group_ptr = _workload_group.second;
            if (used_wg_id.find(wg_id) == used_wg_id.end()) {
                workload_group_ptr->shutdown();
                LOG(INFO) << "[topic_publish_wg] shutdown wg:" << wg_id;
            }
            // wg is shutdown and running rum = 0, its resource can be released in BE
            if (workload_group_ptr->can_be_dropped()) {
                LOG(INFO) << "[topic_publish_wg]There is no query in wg " << wg_id
                          << ", delete it.";
                deleted_task_groups.push_back(workload_group_ptr);
            }
        }
    }

    // 2 stop active thread
    for (auto& wg : deleted_task_groups) {
        // There is not lock here, but the tg may be released by another
        // thread, so that we should use shared ptr here, not use wg_id
        wg->try_stop_schedulers();
    }

    // 3 release resource in memory
    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto& wg : deleted_task_groups) {
            _workload_groups.erase(wg->id());
        }
        new_wg_size = _workload_groups.size();
    }

    // 4 clear cgroup dir
    // NOTE(wb) currently we use rmdir to delete cgroup path,
    // this action may be failed until task file is cleared which means all thread are stopped.
    // So the first time to rmdir a cgroup path may failed.
    // Using cgdelete has no such issue.
    {
        if (!config::doris_cgroup_cpu_path.empty()) {
            std::lock_guard<std::shared_mutex> write_lock(_clear_cgroup_lock);
            Status ret = CgroupCpuCtl::delete_unused_cgroup_path(used_wg_id);
            if (!ret.ok()) {
                LOG(WARNING) << "[topic_publish_wg]" << ret.to_string();
            }
        }
    }
    int64_t time_cost_ms = MonotonicMillis() - begin_time;
    if (deleted_task_groups.size() > 0) {
        LOG(INFO) << "[topic_publish_wg]finish clear unused workload group, time cost: "
                  << time_cost_ms << " ms, deleted group size:" << deleted_task_groups.size()
                  << ", before wg size=" << old_wg_size << ", after wg size=" << new_wg_size;
    }
}

void WorkloadGroupMgr::do_sweep() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (auto& [wg_id, wg] : _workload_groups) {
        wg->do_sweep();
    }
}

struct WorkloadGroupMemInfo {
    int64_t total_mem_used = 0;
    std::list<std::shared_ptr<MemTrackerLimiter>> tracker_snapshots =
            std::list<std::shared_ptr<MemTrackerLimiter>>();
};

void WorkloadGroupMgr::refresh_workload_group_memory_state() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);

    // 1. make all workload groups memory snapshots(refresh workload groups total memory used at the same time)
    // and calculate total memory used of all queries.
    int64_t all_workload_groups_mem_usage = 0;
    for (auto& [wg_id, wg] : _workload_groups) {
        all_workload_groups_mem_usage += wg->refresh_memory_usage();
    }
    if (all_workload_groups_mem_usage <= 0) {
        return;
    }

    std::string debug_msg =
            fmt::format("\nProcess Memory Summary: {}, {}, all workload groups memory usage: {}",
                        doris::GlobalMemoryArbitrator::process_memory_used_details_str(),
                        doris::GlobalMemoryArbitrator::sys_mem_available_details_str(),
                        PrettyPrinter::print(all_workload_groups_mem_usage, TUnit::BYTES));
    LOG_EVERY_T(INFO, 60) << debug_msg;
    for (auto& wg : _workload_groups) {
        update_queries_limit_(wg.second, false);
    }
}

void WorkloadGroupMgr::get_wg_resource_usage(vectorized::Block* block) {
    int64_t be_id = ExecEnv::GetInstance()->cluster_info()->backend_id;
    int cpu_num = CpuInfo::num_cores();
    cpu_num = cpu_num <= 0 ? 1 : cpu_num;
    uint64_t total_cpu_time_ns_per_second = cpu_num * 1000000000LL;

    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    block->reserve(_workload_groups.size());
    for (const auto& [id, wg] : _workload_groups) {
        SchemaScannerHelper::insert_int64_value(0, be_id, block);
        SchemaScannerHelper::insert_int64_value(1, wg->id(), block);
        SchemaScannerHelper::insert_int64_value(2, wg->get_metrics()->get_memory_used(), block);

        double cpu_usage_p = (double)wg->get_metrics()->get_cpu_time_nanos_per_second() /
                             (double)total_cpu_time_ns_per_second * 100;
        cpu_usage_p = std::round(cpu_usage_p * 100.0) / 100.0;

        SchemaScannerHelper::insert_double_value(3, cpu_usage_p, block);
        SchemaScannerHelper::insert_int64_value(
                4, wg->get_metrics()->get_local_scan_bytes_per_second(), block);
        SchemaScannerHelper::insert_int64_value(
                5, wg->get_metrics()->get_remote_scan_bytes_per_second(), block);
    }
}

void WorkloadGroupMgr::refresh_workload_group_metrics() {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, wg] : _workload_groups) {
        wg->get_metrics()->refresh_metrics();
    }
}

void WorkloadGroupMgr::add_paused_query(const std::shared_ptr<ResourceContext>& resource_ctx,
                                        int64_t reserve_size, const Status& status) {
    DCHECK(resource_ctx != nullptr);
    resource_ctx->task_controller()->update_paused_reason(status);
    resource_ctx->task_controller()->set_low_memory_mode(true);
    resource_ctx->task_controller()->set_memory_sufficient(false);
    std::lock_guard<std::mutex> lock(_paused_queries_lock);
    auto wg = resource_ctx->workload_group();
    auto&& [it, inserted] = _paused_queries_list[wg].emplace(
            resource_ctx,
            doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted,
            reserve_size);
    // Check if this is an invalid reserve, for example, if the reserve size is too large, larger than the query limit
    // if hard limit is enabled, then not need enable other queries hard limit.
    if (inserted) {
        LOG(INFO) << "Insert one new paused query: "
                  << resource_ctx->task_controller()->debug_string()
                  << ", workload group: " << wg->debug_string();
    }
}

/**
 * 处理暂停的查询（由 Daemon 线程定期调用）
 * 
 * 【内存管理策略】
 * Strategy 1: 可回收的查询不应该有任何正在运行的 Task（PipelineTask）
 * Strategy 2: 如果 Workload Group 有任务超过 Workload Group 内存限制，则设置所有查询的内存限制
 * Strategy 3: 如果任何查询超过进程内存限制，应该清理所有缓存
 * Strategy 4: 如果任何查询超过查询的内存限制，则进行 Spill 或取消查询
 * Strategy 5: 如果任何查询超过进程内存限制且缓存为零，则执行以下操作
 */
void WorkloadGroupMgr::handle_paused_queries() {
    // 初始化暂停查询列表：为所有 Workload Group 创建空列表（如果不存在）
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& [wg_id, wg] : _workload_groups) {
            std::unique_lock<std::mutex> lock(_paused_queries_lock);
            if (_paused_queries_list[wg].empty()) {
                // 为没有暂停查询的 Workload Group 添加空集合
            }
        }
    }

    // 第一阶段：清理无效查询和检查取消状态
    std::unique_lock<std::mutex> lock(_paused_queries_lock);
    for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
        auto& queries_list = it->second;
        for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
            auto resource_ctx = query_it->resource_ctx_.lock();
            // 查询在执行过程中已完成，从暂停列表中移除
            if (resource_ctx == nullptr) {
                LOG(INFO) << "Query: " << query_it->query_id() << " is nullptr, erase it.";
                query_it = queries_list.erase(query_it);
                continue;
            }
            // 如果有查询正在取消且取消时间小于 15 秒，直接返回
            // 原因：取消操作会释放内存，其他查询可能不需要取消或 Spill
            if (resource_ctx->task_controller()->is_cancelled() &&
                resource_ctx->task_controller()->cancel_elapsed_millis() <
                        config::wait_cancel_release_memory_ms) {
                return;
            }
            ++query_it;
        }
        if (queries_list.empty()) {
            it = _paused_queries_list.erase(it);
            continue;
        } else {
            // 处理完一个 Workload Group，继续处理下一个
            ++it;
        }
    }

    // 第二阶段：根据暂停原因处理每个查询
    bool has_query_exceed_process_memlimit = false;
    for (auto it = _paused_queries_list.begin(); it != _paused_queries_list.end();) {
        auto& queries_list = it->second;
        auto query_count = queries_list.size();
        const auto& wg = it->first;

        if (query_count != 0) {
            LOG_EVERY_T(INFO, 1) << "Paused queries count of wg " << wg->name() << ": "
                                 << query_count;
        }

        bool has_changed_hard_limit = false;
        bool exceed_low_watermark = false;
        bool exceed_high_watermark = false;
        // 检查 Workload Group 的内存使用情况（低水位和高水位）
        wg->check_mem_used(&exceed_low_watermark, &exceed_high_watermark);
        
        // 遍历该 Workload Group 的所有暂停查询
        // 如果查询因为超过自身内存限制而暂停，则进行 Spill
        // 查询的内存限制通过 slot 机制设置，值是用户配置的，不是加权值
        // 如果预留失败，说明确实超过了限制
        for (auto query_it = queries_list.begin(); query_it != queries_list.end();) {
            auto resource_ctx = query_it->resource_ctx_.lock();
            // 查询在执行过程中已完成，从暂停列表中移除
            if (resource_ctx == nullptr) {
                LOG(INFO) << "Query: " << query_it->query_id() << " is nullptr, erase it.";
                query_it = queries_list.erase(query_it);
                continue;
            }

            // 情况 1：查询内存超限（QUERY_MEMORY_EXCEEDED）
            // Streamload、kafka load、group commit 不会出现此错误，因为它们的查询限制很大
            if (resource_ctx->task_controller()
                        ->paused_reason()
                        .is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
                // 处理单个查询：尝试 Spill 或取消
                bool spill_res = handle_single_query_(
                        resource_ctx, query_it->reserve_size_, query_it->elapsed_time(),
                        resource_ctx->task_controller()->paused_reason());
                if (!spill_res) {
                    // Spill 失败，继续处理下一个查询
                    ++query_it;
                    continue;
                } else {
                    // Spill 成功，从暂停列表中移除
                    VLOG_DEBUG << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                               << " remove from paused list";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
            } else if (resource_ctx->task_controller()
                               ->paused_reason()
                               .is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
                // 情况 2：Workload Group 内存超限（WORKLOAD_GROUP_MEMORY_EXCEEDED）
                // 当前查询的 Workload Group 可能并未真正超过限制，只是（WG 消耗 + 当前查询预期预留内存 > WG 内存限制）
                // 如果当前查询内存消耗 + 预期预留内存超过限制，可能是预期预留内存过大
                // 此时 WG 内存不足，当前查询应该尝试释放内存
                // 但这里不直接尝试 Spill 该查询，而是只设置查询的限制，然后唤醒查询继续执行
                //
                // 如果预期预留内存估算正确，大概率查询会再次进入暂停状态，原因变为 QUERY_MEMORY_EXCEEDED
                // 然后会调用 handle_single_query_ 进行 Spill
                //
                // 当然，如果实际所需内存小于预留内存，或继续执行时有足够内存，查询会成功运行而不需要 Spill
                if (resource_ctx->memory_context()->adjusted_mem_limit() <
                    resource_ctx->memory_context()->current_memory_bytes() +
                            query_it->reserve_size_) {
                    // 查询未超过查询限制，但在 Workload Group 内存不足时超过了预期查询限制
                    // 使用较小的内存限制让查询超过查询限制，触发 Spill
                    resource_ctx->memory_context()->set_mem_limit(
                            resource_ctx->memory_context()->adjusted_mem_limit());
                    resource_ctx->task_controller()->set_memory_sufficient(true);
                    LOG(INFO) << "Workload group memory reserve failed because "
                              << resource_ctx->task_controller()->debug_string() << " reserve size "
                              << PrettyPrinter::print_bytes(query_it->reserve_size_)
                              << " is too large, set hard limit to "
                              << PrettyPrinter::print_bytes(
                                         resource_ctx->memory_context()->adjusted_mem_limit())
                              << " and resume running.";
                    query_it = queries_list.erase(query_it);
                    continue;
                }

                // 执行到这里，说明当前查询的 adjusted_mem_limit < 查询内存消耗 + reserve_size
                // 这意味着当前查询本身没有超过内存限制
                //
                // 这意味着当前查询的 Workload Group 中一定有查询的内存超过了 adjusted_mem_limit
                // 但这些查询可能没有进入暂停状态，所以它们可能不会修改内存限制并继续执行
                // 当上面判断 (adjusted_mem_limit < consumption + reserve_size_) 时
                //
                // 因此调用 update_queries_limit_ 强制更新当前查询的 Workload Group 中所有查询的内存限制为 adjusted_mem_limit
                // 希望那些超过限制的查询会释放内存
                if (!has_changed_hard_limit) {
                    update_queries_limit_(wg, true);
                    has_changed_hard_limit = true;
                    LOG(INFO) << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                              << " reserve memory("
                              << PrettyPrinter::print_bytes(query_it->reserve_size_)
                              << ") failed due to workload group memory exceed, "
                                 "should set the workload group work in memory insufficent mode, "
                                 "so that other query will reduce their memory."
                              << " Query mem limit: "
                              << PrettyPrinter::print_bytes(
                                         resource_ctx->memory_context()->mem_limit())
                              << " mem usage: "
                              << PrettyPrinter::print_bytes(
                                         resource_ctx->memory_context()->current_memory_bytes())
                              << ", wg: " << wg->debug_string();
                }
                
                // 如果未启用 slot 内存策略，直接进行 Spill
                // 不鼓励不启用 slot 内存策略
                // 可能有其他查询使用了太多内存，如果这些查询超过内存限制，它们会进入暂停状态
                // 原因变为 QUERY_MEMORY_EXCEEDED，也会尝试 Spill
                // TODO: 应该取消超过限制的查询
                if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::NONE) {
                    bool spill_res = handle_single_query_(
                            resource_ctx, query_it->reserve_size_, query_it->elapsed_time(),
                            resource_ctx->task_controller()->paused_reason());
                    if (!spill_res) {
                        ++query_it;
                        continue;
                    } else {
                        VLOG_DEBUG
                                << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                                << " remove from paused list";
                        query_it = queries_list.erase(query_it);
                        continue;
                    }
                } else {
                    // 不应该立即将查询放回任务调度器，因为当 WG 内存不足时，设置 WG 标志后
                    // 其他查询可能不会很快释放内存
                    // 如果 Workload Group 的内存使用低于低水位，则调度查询运行
                    if (query_it->elapsed_time() > config::spill_in_paused_queue_timeout_ms ||
                        !exceed_low_watermark) {
                        // 设置 WG 的内存为充足，然后将其添加回任务调度器运行
                        LOG(INFO) << "Query: "
                                  << print_id(resource_ctx->task_controller()->task_id())
                                  << " has waited in paused query queue for "
                                  << query_it->elapsed_time() << " ms. Resume it.";
                        resource_ctx->task_controller()->set_memory_sufficient(true);
                        query_it = queries_list.erase(query_it);
                        continue;
                    } else {
                        ++query_it;
                        continue;
                    }
                }
            } else {
                // 情况 3：进程内存超限（PROCESS_MEMORY_EXCEEDED）或其他原因
                // 如果之前已经从其他查询回收了内存，且取消阶段已完成，则恢复所有查询
                if (revoking_memory_from_other_query_) {
                    resource_ctx->task_controller()->set_memory_sufficient(true);
                    VLOG_DEBUG << "Query " << print_id(resource_ctx->task_controller()->task_id())
                               << " is blocked due to process memory not enough, but already "
                                  "cancelled some queries, resumt it now.";
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                has_query_exceed_process_memlimit = true;
                
                // 如果 WG 的内存限制未超过，但进程内存超过，说明缓存或其他元数据使用了太多内存
                // 应该在这里清理所有缓存
                // 清理所有缓存不是缓存的一部分，因为缓存线程已经尝试逐步释放缓存，但效果不佳
                //
                // 这里查询因为 PROCESS_MEMORY_EXCEEDED 而暂停
                // 通常，在进程内存超过之前，daemon 线程 refresh_cache_capacity 会将缓存容量调整为 0
                // 但此时进程可能并未真正超过限制，只是（进程内存 + 当前查询预期预留内存 > 进程内存限制）
                // 所以此时的行为与进程内存限制超过相同，清理所有缓存
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted >
                            0.05 &&
                    doris::GlobalMemoryArbitrator::
                                    last_memory_exceeded_cache_capacity_adjust_weighted > 0.05) {
                    doris::GlobalMemoryArbitrator::
                            last_memory_exceeded_cache_capacity_adjust_weighted = 0.04;
                    doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
                    LOG(INFO) << "There are some queries need process memory, so that set cache "
                                 "capacity to 0 now";
                }

                // cache_ratio_ < 0.05 表示在查询进入暂停状态之前缓存已被清理
                // 但查询仍然因为进程内存超过而暂停，所以这里会尝试继续释放其他内存
                //
                // 需要在这里检查 config::disable_memory_gc，如果不检查，当 config::disable_memory_gc == true 时
                // 缓存不会被调整，query_it->cache_ratio_ 将始终为 1，这个 if 分支永远不会执行
                // 查询永远不会恢复，会在这里死锁
                if (query_it->cache_ratio_ < 0.05 || config::disable_memory_gc) {
                    // 如果 Workload Group 的内存使用 > 最小内存，说明 Workload Group 在内存竞争状态下使用了太多内存
                    // 应该进行 Spill
                    if (wg->total_mem_used() > wg->min_memory_limit()) {
                        auto revocable_tasks =
                                resource_ctx->task_controller()->get_revocable_tasks();
                        if (revocable_tasks.empty()) {
                            // 没有可回收的 Task，取消查询
                            Status status = Status::MemoryLimitExceeded(
                                    "Workload group memory usage {} > min memory {}, but no "
                                    "revocable tasks",
                                    wg->total_mem_used(), wg->min_memory_limit());
                            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                                    resource_ctx->task_controller()->task_id(), status);
                            revoking_memory_from_other_query_ = true;
                            // 如果任何查询被取消，跳过其他查询，因为会释放大量内存，其他查询可能不需要释放内存
                            return;
                        } else {
                            // 有可回收的 Task，触发 Spill
                            SCOPED_ATTACH_TASK(resource_ctx);
                            auto status = resource_ctx->task_controller()->revoke_memory();
                            if (!status.ok()) {
                                ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                                        resource_ctx->task_controller()->task_id(), status);
                                revoking_memory_from_other_query_ = true;
                                return;
                            }
                            query_it = queries_list.erase(query_it);
                            continue;
                        }
                    }

                    // 其他 Workload Group 可能使用了很多内存，应该通过取消它们的查询来从其他 Workload Group 回收内存
                    int64_t revoked_size = revoke_memory_from_other_groups_();
                    if (revoked_size > 0) {
                        // 从其他 Workload Group 回收内存会取消一些查询，等待它们取消完成后再检查
                        revoking_memory_from_other_query_ = true;
                        return;
                    }

                    // TODO: 从 memtable 回收内存
                }
                
                // cache_ratio_ > 0.05 表示在查询进入暂停状态时缓存未被清理
                // last_affected_cache_capacity_adjust_weighted < 0.05 表示此时缓存已被清理
                // 这意味着在查询进入暂停状态后缓存已被清理
                // 假设已经释放了一些内存，唤醒查询继续执行
                if (doris::GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted <
                            0.05 &&
                    query_it->cache_ratio_ > 0.05) {
                    LOG(INFO) << "Query: " << print_id(resource_ctx->task_controller()->task_id())
                              << " will be resume after cache adjust.";
                    resource_ctx->task_controller()->set_memory_sufficient(true);
                    query_it = queries_list.erase(query_it);
                    continue;
                }
                ++query_it;
            }
        }

        // 即使 Workload Group 没有处于暂停状态的查询，以下代码仍会执行
        // 因为 handle_paused_queries 在开始时会将 <wg, empty set> 添加到 _paused_queries_list
        if (queries_list.empty()) {
            it = _paused_queries_list.erase(it);
            continue;
        } else {
            // 处理完一个 Workload Group，继续处理下一个
            ++it;
        }
    }
    
    // 注意：必须在这里。这意味着没有查询处于取消状态，所有因进程内存不足而阻塞的查询都已恢复
    revoking_memory_from_other_query_ = false;

    // 如果没有查询因进程超过限制而暂停，且缓存容量调整权重 < 0.05，则启用缓存
    if (!has_query_exceed_process_memlimit &&
        doris::GlobalMemoryArbitrator::last_memory_exceeded_cache_capacity_adjust_weighted < 0.05) {
        // 没有查询因进程超过限制而暂停，因此现在启用缓存
        doris::GlobalMemoryArbitrator::last_memory_exceeded_cache_capacity_adjust_weighted =
                doris::GlobalMemoryArbitrator::
                        last_periodic_refreshed_cache_capacity_adjust_weighted.load(
                                std::memory_order_relaxed);
        doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
        LOG(INFO) << "No query was paused due to insufficient process memory, so that set cache "
                     "capacity to last_periodic_refreshed_cache_capacity_adjust_weighted now";
    }
}

// Find the workload group that could revoke lot of memory:
// 1. workload group = max(total used memory - min memory that should reserved for it)
// 2. revoke 10% memory of the workload group that exceeded. For example, if the workload group exceed 10g,
//    then revoke 1g memory.
// 3. After revoke memory, go to the loop and wait for the query to be cancelled and check again.
int64_t WorkloadGroupMgr::revoke_memory_from_other_groups_() {
    MonotonicStopWatch watch;
    watch.start();
    std::unique_ptr<RuntimeProfile> profile =
            std::make_unique<RuntimeProfile>("RevokeMemoryFromOtherGroups");

    WorkloadGroupPtr max_wg = nullptr;
    int64_t max_exceeded_memory = 0;
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        for (auto& workload_group : _workload_groups) {
            int64_t min_memory_limit = workload_group.second->min_memory_limit();
            int64_t total_used_memory = workload_group.second->total_mem_used();
            if (total_used_memory <= min_memory_limit) {
                // min memory is reserved for this workload group, if it used less than min memory,
                // then not revoke memory from it.
                continue;
            }
            if (total_used_memory - min_memory_limit > max_exceeded_memory) {
                max_wg = workload_group.second;
                max_exceeded_memory = total_used_memory - min_memory_limit;
            }
        }
    }
    if (max_wg == nullptr) {
        return 0;
    }
    if (max_exceeded_memory < 1 << 27) {
        LOG(INFO) << "The workload group that exceed most memory is :"
                  << max_wg->memory_debug_string() << ", max_exceeded_memory: "
                  << PrettyPrinter::print(max_exceeded_memory, TUnit::BYTES)
                  << " less than 128MB, no need to revoke memory";
        return 0;
    }
    int64_t freed_mem = static_cast<int64_t>((double)max_exceeded_memory * 0.1);
    // Revoke 10% of memory from the workload group that exceed most memory
    max_wg->revoke_memory(freed_mem, "exceed_memory", profile.get());
    std::stringstream ss;
    profile->pretty_print(&ss);
    LOG(INFO) << fmt::format(
            "[MemoryGC] process memory not enough, revoke memory from workload_group: {}, "
            "free memory {}. cost(us): {}, details: {}",
            max_wg->memory_debug_string(), PrettyPrinter::print_bytes(freed_mem),
            watch.elapsed_time() / 1000, ss.str());
    return freed_mem;
}

/**
 * 处理单个暂停查询
 * 
 * 【支持的查询类型】
 * - streamload, kafka routine load, group commit
 * - insert into select
 * - select
 * 
 * 【返回值说明】
 * - true: 查询可以释放内存（如 Spill）或取消查询
 * - false: 查询尚未准备好执行这些任务，需要继续等待
 * 
 * 【参数说明】
 * @param requestor 请求查询的资源上下文
 * @param size_to_reserve 需要预留的内存大小
 * @param time_in_queue 在暂停队列中等待的时间（毫秒）
 * @param paused_reason 暂停原因（QUERY_MEMORY_EXCEEDED、WORKLOAD_GROUP_MEMORY_EXCEEDED、PROCESS_MEMORY_EXCEEDED）
 * @return true 如果查询已处理（Spill 或恢复），false 如果需要继续等待
 */
bool WorkloadGroupMgr::handle_single_query_(const std::shared_ptr<ResourceContext>& requestor,
                                            size_t size_to_reserve, int64_t time_in_queue,
                                            Status paused_reason) {
    // 获取查询的可回收内存信息
    size_t revocable_size = 0;      // 可回收内存大小（可以 Spill 的内存）
    size_t memory_usage = 0;       // 当前内存使用量
    bool has_running_task = false; // 是否有正在运行的 Task
    const auto query_id = print_id(requestor->task_controller()->task_id());
    requestor->task_controller()->get_revocable_info(&revocable_size, &memory_usage,
                                                     &has_running_task);
    
    // Strategy 1: 可回收的查询不应该有任何正在运行的 Task
    // 如果有正在运行的 Task，跳过处理，等待 Task 完成
    if (has_running_task) {
        LOG(INFO) << "Query: " << print_id(requestor->task_controller()->task_id())
                  << " is paused, but still has running task, skip it.";
        return false;
    }

    const auto wg = requestor->workload_group();
    // 获取查询中所有可回收的 Task（可以 Spill 的 Task）
    auto revocable_tasks = requestor->task_controller()->get_revocable_tasks();
    
    // 情况 A：没有可回收的 Task（无法通过 Spill 释放内存）
    if (revocable_tasks.empty()) {
        const auto limit = requestor->memory_context()->mem_limit();
        const auto reserved_size = requestor->memory_context()->reserved_consumption();
        
        // 子情况 A1：查询内存超限（QUERY_MEMORY_EXCEEDED）
        if (paused_reason.is<ErrorCode::QUERY_MEMORY_EXCEEDED>()) {
            // 在等待期间，查询中的其他算子可能已完成并释放了大量内存，现在可以运行
            if ((memory_usage + size_to_reserve) < limit) {
                LOG(INFO) << "Query: " << query_id << ", usage("
                          << PrettyPrinter::print_bytes(memory_usage) << " + " << size_to_reserve
                          << ") less than limit(" << PrettyPrinter::print_bytes(limit)
                          << "), resume it.";
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue >= config::spill_in_paused_queue_timeout_ms) {
                // 超时处理：如果无法找到任何可以释放或 Spill 的内存，让查询尽可能继续运行
                // 禁用预留内存后，查询不会再进入暂停状态
                // 如果内存真的不足，Allocator 会抛出查询内存限制超过的异常，查询会被取消
                // 或者当进程内存超过限制时，会被内存 GC 取消
                auto log_str = fmt::format(
                        "Query {} memory limit is exceeded, but could "
                        "not find memory that could release or spill to disk, disable reserve "
                        "memory and resume it. Query memory usage: "
                        "{}, limit: {}, reserved "
                        "size: {}, try to reserve: {}, wg info: {}. {}",
                        query_id, PrettyPrinter::print_bytes(memory_usage),
                        PrettyPrinter::print_bytes(limit),
                        PrettyPrinter::print_bytes(reserved_size),
                        PrettyPrinter::print_bytes(size_to_reserve), wg->memory_debug_string(),
                        doris::ProcessProfile::instance()
                                ->memory_profile()
                                ->process_memory_detail_str());
                LOG_LONG_STRING(INFO, log_str);
                // 禁用预留内存会启用查询级别的内存检查，如果查询需要的内存超过内存限制，会被杀死
                // 不需要设置 memlimit = adjusted_mem_limit，因为 Workload Group 刷新线程会自动更新
                requestor->task_controller()->disable_reserve_memory();
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else {
                // 未超时，继续等待
                return false;
            }
        } else if (paused_reason.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>()) {
            // 子情况 A2：Workload Group 内存超限（WORKLOAD_GROUP_MEMORY_EXCEEDED）
            // 如果 Workload Group 现在没有超过限制，恢复查询
            if (!wg->exceed_limit()) {
                LOG(INFO) << "Query: " << query_id
                          << " paused caused by WORKLOAD_GROUP_MEMORY_EXCEEDED, now resume it.";
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue > config::spill_in_paused_queue_timeout_ms) {
                // 超时处理：如果无法找到任何可以释放的内存，让查询尽可能继续运行
                // 或者如果内存真的不足，会被 GC 取消
                auto log_str = fmt::format(
                        "Query {} workload group memory is exceeded"
                        ", and there is no cache now. And could not find task to spill, disable "
                        "reserve memory and resume it. "
                        "Query memory usage: {}, limit: {}, reserved "
                        "size: {}, try to reserve: {}, wg info: {}."
                        " Maybe you should set the workload group's limit to a lower value. {}",
                        query_id, PrettyPrinter::print_bytes(memory_usage),
                        PrettyPrinter::print_bytes(limit),
                        PrettyPrinter::print_bytes(reserved_size),
                        PrettyPrinter::print_bytes(size_to_reserve), wg->memory_debug_string(),
                        doris::ProcessProfile::instance()
                                ->memory_profile()
                                ->process_memory_detail_str());
                LOG_LONG_STRING(INFO, log_str);
                requestor->task_controller()->disable_reserve_memory();
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else {
                // 未超时，继续等待
                return false;
            }
        } else {
            // 子情况 A3：进程内存超限（PROCESS_MEMORY_EXCEEDED）或其他原因
            // 不应该考虑进程内存。例如：查询限制 100GB，Workload Group 限制 10GB，进程内存 20GB
            // 查询预留会在 WG 限制处失败，进程总是有内存，所以会恢复并再次失败预留
            const size_t test_memory_size = std::max<size_t>(size_to_reserve, 32L * 1024 * 1024);
            // 检查进程内存是否超过软限制
            if (!GlobalMemoryArbitrator::is_exceed_soft_mem_limit(test_memory_size)) {
                // 进程限制现在未超过，恢复查询
                LOG(INFO) << "Query: " << query_id
                          << ", process limit not exceeded now, resume this query"
                          << ", process memory info: "
                          << GlobalMemoryArbitrator::process_memory_used_details_str()
                          << ", wg info: " << wg->debug_string();
                requestor->task_controller()->set_memory_sufficient(true);
                return true;
            } else if (time_in_queue > config::spill_in_paused_queue_timeout_ms) {
                // 超时处理：如果无法找到任何可以释放的内存，让查询尽可能继续运行
                // 或者如果内存真的不足，会被 GC 取消
                auto log_str = fmt::format(
                        "Query {} process memory is exceeded"
                        ", and there is no cache now. And could not find task to spill, disable "
                        "reserve memory and resume it. "
                        "Query memory usage: {}, limit: {}, reserved "
                        "size: {}, try to reserve: {}, wg info: {}."
                        " Maybe you should set the workload group's limit to a lower value. {}",
                        query_id, PrettyPrinter::print_bytes(memory_usage),
                        PrettyPrinter::print_bytes(limit),
                        PrettyPrinter::print_bytes(reserved_size),
                        PrettyPrinter::print_bytes(size_to_reserve), wg->memory_debug_string(),
                        doris::ProcessProfile::instance()
                                ->memory_profile()
                                ->process_memory_detail_str());
                LOG_LONG_STRING(INFO, log_str);
                requestor->task_controller()->disable_reserve_memory();
                requestor->task_controller()->set_memory_sufficient(true);
            } else {
                // 未超时，继续等待
                return false;
            }
        }
    } else {
        // 情况 B：有可回收的 Task（可以通过 Spill 释放内存）
        // 设置线程资源上下文（用于内存追踪）
        SCOPED_ATTACH_TASK(requestor);
        // 触发内存回收（Spill）：收集可回收的 Task，创建 SpillContext，提交 RevokableTask 到调度器
        auto status = requestor->task_controller()->revoke_memory();
        if (!status.ok()) {
            // Spill 失败，取消查询
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    requestor->task_controller()->task_id(), status);
        }
    }
    return true;
}

void WorkloadGroupMgr::update_queries_limit_(WorkloadGroupPtr wg, bool enable_hard_limit) {
    auto wg_mem_limit = wg->memory_limit();
    auto all_resource_ctxs = wg->resource_ctxs();
    bool exceed_low_watermark = false;
    bool exceed_high_watermark = false;
    wg->check_mem_used(&exceed_low_watermark, &exceed_high_watermark);
    int64_t wg_high_water_mark_limit =
            (int64_t)(static_cast<double>(wg_mem_limit) * wg->memory_high_watermark() * 1.0 / 100);
    int64_t wg_high_water_mark_except_load = wg_high_water_mark_limit;
    std::string debug_msg;
    if (exceed_high_watermark || exceed_low_watermark) {
        debug_msg = fmt::format(
                "\nWorkload Group {}: mem limit: {}, mem used: {}, "
                "high water mark mem limit: {}, used ratio: {}",
                wg->name(), PrettyPrinter::print(wg->memory_limit(), TUnit::BYTES),
                PrettyPrinter::print(wg->total_mem_used(), TUnit::BYTES),
                PrettyPrinter::print(wg_high_water_mark_limit, TUnit::BYTES),
                (double)(wg->total_mem_used()) / static_cast<double>(wg_mem_limit));
    }

    int32_t total_used_slot_count = 0;
    int32_t total_slot_count = wg->total_query_slot_count();
    // calculate total used slot count
    for (const auto& resource_ctx_pair : all_resource_ctxs) {
        auto resource_ctx = resource_ctx_pair.second.lock();
        if (!resource_ctx) {
            continue;
        }
        // Streamload kafka load group commit, not modify slot
        if (!resource_ctx->task_controller()->is_pure_load_task()) {
            total_used_slot_count += resource_ctx->task_controller()->get_slot_count();
        }
    }
    // calculate per query weighted memory limit
    debug_msg += "\nQuery Memory Summary: \n";
    for (const auto& resource_ctx_pair : all_resource_ctxs) {
        auto resource_ctx = resource_ctx_pair.second.lock();
        if (!resource_ctx) {
            continue;
        }
        if (exceed_low_watermark) {
            resource_ctx->task_controller()->set_low_memory_mode(true);
        }
        int64_t query_weighted_mem_limit = 0;
        int64_t expected_query_weighted_mem_limit = 0;
        if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::NONE) {
            query_weighted_mem_limit = resource_ctx->memory_context()->user_set_mem_limit();
            // If the policy is NONE, we use the query's memory limit. but the query's memory limit
            // should not be greater than the workload group's memory limit.
            if (query_weighted_mem_limit > wg_mem_limit) {
                query_weighted_mem_limit = wg_mem_limit;
            }
            expected_query_weighted_mem_limit = query_weighted_mem_limit;
        } else if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::FIXED) {
            // TODO, `Policy::FIXED` expects `all_query_used_slot_count < wg_total_slot_count`,
            // which is controlled when query is submitted
            // DCEHCK(total_used_slot_count <= total_slot_count);
            if (total_slot_count < 1) {
                LOG(WARNING)
                        << "Query " << print_id(resource_ctx->task_controller()->task_id())
                        << " enabled hard limit, but the slot count < 1, could not take affect";
                continue;
            } else {
                // If the query enable hard limit, then not use weighted info any more, just use the settings limit.
                query_weighted_mem_limit =
                        (int64_t)((static_cast<double>(wg_high_water_mark_except_load) *
                                   resource_ctx->task_controller()->get_slot_count() * 1.0) /
                                  total_slot_count);
                expected_query_weighted_mem_limit = query_weighted_mem_limit;
            }
        } else if (wg->slot_memory_policy() == TWgSlotMemoryPolicy::DYNAMIC) {
            // If low water mark is not reached, then use process memory limit as query memory limit.
            // It means it will not take effect.
            // If there are some query in paused list, then limit should take effect.
            // numerator `+ total_used_slot_count` ensures that the result is greater than 1.
            expected_query_weighted_mem_limit =
                    total_used_slot_count > 0
                            ? (int64_t)(static_cast<double>(wg_high_water_mark_except_load +
                                                            total_used_slot_count) *
                                        resource_ctx->task_controller()->get_slot_count() * 1.0 /
                                        total_used_slot_count)
                            : wg_high_water_mark_except_load;
            if (!exceed_low_watermark && !enable_hard_limit) {
                query_weighted_mem_limit = wg_high_water_mark_except_load;
            } else {
                query_weighted_mem_limit = expected_query_weighted_mem_limit;
            }
        }
        debug_msg += resource_ctx->task_controller()->debug_string() + "\n";
        // If the query is a pure load task, then should not modify its limit. Or it will reserve
        // memory failed and we did not hanle it.
        if (!resource_ctx->task_controller()->is_pure_load_task()) {
            // If user's set mem limit is less than query weighted mem limit, then should not modify its limit.
            // Use user settings.
            if (resource_ctx->memory_context()->user_set_mem_limit() > query_weighted_mem_limit) {
                resource_ctx->memory_context()->set_mem_limit(query_weighted_mem_limit);
            }
            resource_ctx->memory_context()->set_adjusted_mem_limit(
                    expected_query_weighted_mem_limit);
        }
    }
    LOG_EVERY_T(INFO, 60) << debug_msg;
}

void WorkloadGroupMgr::stop() {
    for (auto iter = _workload_groups.begin(); iter != _workload_groups.end(); iter++) {
        iter->second->try_stop_schedulers();
    }
}

Status WorkloadGroupMgr::create_internal_wg() {
    TWorkloadGroupInfo twg_info;
    twg_info.__set_id(INTERNAL_NORMAL_WG_ID);
    twg_info.__set_name(INTERNAL_NORMAL_WG_NAME);
    twg_info.__set_max_memory_percent(100); // The normal wg will occupy all memory by default.
    twg_info.__set_min_memory_percent(0);   // The normal wg will occupy all memory by default.
    twg_info.__set_max_cpu_percent(100);
    twg_info.__set_min_cpu_percent(0);
    twg_info.__set_version(0);

    WorkloadGroupInfo wg_info = WorkloadGroupInfo::parse_topic_info(twg_info);
    auto normal_wg = std::make_shared<WorkloadGroup>(wg_info);

    RETURN_IF_ERROR(normal_wg->upsert_task_scheduler(&wg_info));

    {
        std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
        _workload_groups[normal_wg->id()] = normal_wg;
    }

    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris
