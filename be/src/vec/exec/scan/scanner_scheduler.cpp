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

#include "scanner_scheduler.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "file_scanner.h"
#include "olap/tablet.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/async_io.h" // IWYU pragma: keep
#include "util/cpu_info.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "vec/columns/column_nothing.h"
#include "vec/core/block.h"
#include "vec/exec/scan/olap_scanner.h" // IWYU pragma: keep
#include "vec/exec/scan/scan_node.h"
#include "vec/exec/scan/scanner.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris::vectorized {

// 提交scanner任务到调度器执行
// ctx: scanner上下文，包含查询状态和scanner管理信息
// scan_task: 待执行的扫描任务
Status ScannerScheduler::submit(std::shared_ptr<ScannerContext> ctx,
                                std::shared_ptr<ScanTask> scan_task) {
    // 如果上下文已完成，直接返回
    if (ctx->done()) {
        return Status::OK();
    }
    // 尝试获取任务执行上下文锁，确保查询未被取消
    auto task_lock = ctx->task_exec_ctx();
    if (task_lock == nullptr) {
        LOG(INFO) << "could not lock task execution context, query " << ctx->debug_string()
                  << " maybe finished";
        return Status::OK();
    }
    // 获取scanner代理，如果已被释放则直接返回
    std::shared_ptr<ScannerDelegate> scanner_delegate = scan_task->scanner.lock();
    if (scanner_delegate == nullptr) {
        return Status::OK();
    }

    // 开始计时等待worker的时间
    scanner_delegate->_scanner->start_wait_worker_timer();
    TabletStorageType type = scanner_delegate->_scanner->get_storage_type();
    
    // 构造提交任务的lambda
    auto sumbit_task = [&]() {
        // work_func是实际在线程池中执行的函数
        auto work_func = [scanner_ref = scan_task, ctx]() {
            // 调用_scanner_scan执行实际的扫描操作
            auto status = [&] {
                RETURN_IF_CATCH_EXCEPTION(_scanner_scan(ctx, scanner_ref));
                return Status::OK();
            }();

            // 如果执行出错，设置错误状态并将任务放回队列
            if (!status.ok()) {
                scanner_ref->set_status(status);
                ctx->push_back_scan_task(scanner_ref);
                return true;
            }
            // 返回是否扫描结束
            return scanner_ref->is_eos();
        };
        // 封装成SimplifiedScanTask并提交到线程池
        SimplifiedScanTask simple_scan_task = {work_func, ctx, scan_task};
        return this->submit_scan_task(simple_scan_task);
    };

    // 执行提交
    Status submit_status = sumbit_task();
    if (!submit_status.ok()) {
        // 提交失败时返回TooManyTasks错误，便于用户理解
        Status scan_task_status = Status::TooManyTasks(
                "Failed to submit scanner to scanner pool reason:" +
                std::string(submit_status.msg()) + "|type:" + std::to_string(type));
        scan_task->set_status(scan_task_status);
        return scan_task_status;
    }

    return Status::OK();
}

void handle_reserve_memory_failure(RuntimeState* state, std::shared_ptr<ScannerContext> ctx,
                                   const Status& st, size_t reserve_size) {
    ctx->clear_free_blocks();
    auto* local_state = ctx->local_state();

    auto debug_msg = fmt::format(
            "Query: {} , scanner try to reserve: {}, operator name {}, "
            "operator "
            "id: {}, "
            "task id: "
            "{}, failed: {}",
            print_id(state->query_id()), PrettyPrinter::print_bytes(reserve_size),
            local_state->get_name(), local_state->parent()->node_id(), state->task_id(),
            st.to_string());
    // PROCESS_MEMORY_EXCEEDED error msg alread contains process_mem_log_str
    if (!st.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>()) {
        debug_msg += fmt::format(", debug info: {}", GlobalMemoryArbitrator::process_mem_log_str());
    }
    VLOG_DEBUG << debug_msg;

    state->get_query_ctx()->set_low_memory_mode();
}

// 实际执行扫描操作的核心函数，在线程池worker中被调用
// ctx: scanner上下文，管理scanner生命周期和数据传递
// scan_task: 当前执行的扫描任务，包含scanner和缓存的数据块
void ScannerScheduler::_scanner_scan(std::shared_ptr<ScannerContext> ctx,
                                     std::shared_ptr<ScanTask> scan_task) {
    // 获取任务执行上下文锁，确保查询未被取消
    auto task_lock = ctx->task_exec_ctx();
    if (task_lock == nullptr) {
        return;
    }
    // 将当前线程附加到任务上下文，用于内存跟踪等
    SCOPED_ATTACH_TASK(ctx->state());

    // 更新正在运行的scanner计数（用于统计峰值）
    ctx->update_peak_running_scanner(1);
    Defer defer([&] { ctx->update_peak_running_scanner(-1); });

    // 获取scanner代理
    std::shared_ptr<ScannerDelegate> scanner_delegate = scan_task->scanner.lock();
    if (scanner_delegate == nullptr) {
        return;
    }

    ScannerSPtr& scanner = scanner_delegate->_scanner;
    // 对于CPU硬限制场景，不重置线程名
    if (ctx->_should_reset_thread_name) {
        Thread::set_self_name("_scanner_scan");
    }

#ifndef __APPLE__
    // 降低scanner线程优先级，确保写操作的CPU调度优先
    if (config::scan_thread_nice_value != 0 && scanner->get_name() != FileScanner::NAME) {
        Thread::set_thread_nice_value();
    }
#endif
    // 计时器：用于限制单次扫描的最大运行时间
    MonotonicStopWatch max_run_time_watch;
    max_run_time_watch.start();
    // 更新等待worker的时间统计
    scanner->update_wait_worker_timer();
    // 开始计时CPU使用时间
    scanner->start_scan_cpu_timer();
    // 延迟执行：在函数退出时更新各种计时器和计数器
    Defer defer_scanner(
            [&] { // WorkloadGroup策略会实时检查CPU时间，需要尽快更新计数器
                if (scanner->has_prepared()) {
                    // 只有prepare成功后才能更新计数器，否则可能崩溃
                    // 例如olap scanner在prepare时打开tablet reader，未成功则reader为空
                    scanner->update_scan_cpu_timer();
                    scanner->update_realtime_counters();
                    scanner->start_wait_worker_timer();
                }
            });
    Status status = Status::OK();
    bool eos = false;  // end of stream标志
    ASSIGN_STATUS_IF_CATCH_EXCEPTION(
            RuntimeState* state = ctx->state(); DCHECK(nullptr != state);
            // scanner->open可能分配大量内存，低内存模式下先清理空闲块
            if (ctx->low_memory_mode()) { ctx->clear_free_blocks(); }

            // ========== Scanner初始化阶段 ==========
            // 首次调度时执行prepare
            if (!scanner->has_prepared()) {
                status = scanner->prepare();
                if (!status.ok()) {
                    eos = true;
                }
            }

            // 首次调度时执行open，初始化scanner资源
            if (!eos && !scanner->is_open()) {
                status = scanner->open(state);
                if (!status.ok()) {
                    eos = true;
                }
                scanner->set_opened();
            }

            // 尝试应用延迟到达的runtime filter
            Status rf_status = scanner->try_append_late_arrival_runtime_filter();
            if (!rf_status.ok()) {
                LOG(WARNING) << "Failed to append late arrival runtime filter: "
                             << rf_status.to_string();
            }

            // ========== 扫描参数设置 ==========
            // 单次扫描的字节数阈值
            size_t raw_bytes_threshold = config::doris_scanner_row_bytes;
            if (ctx->low_memory_mode()) {
                ctx->clear_free_blocks();
                // 低内存模式下降低阈值
                if (raw_bytes_threshold > ctx->low_memory_mode_scan_bytes_per_scanner()) {
                    raw_bytes_threshold = ctx->low_memory_mode_scan_bytes_per_scanner();
                }
            }

            size_t raw_bytes_read = 0;  // 已读取的字节数
            bool first_read = true;
            int64_t limit = scanner->limit();
            // 标记第一个block是否已满（用于控制低内存模式下的block数量）
            bool has_first_full_block = false;

            // ========== 主扫描循环 ==========
            // 循环条件：未结束 && 未达到字节阈值 && 内存检查通过
            // 低内存模式下每次最多返回2个block以减少内存使用
            while (!eos && raw_bytes_read < raw_bytes_threshold &&
                   (!ctx->low_memory_mode() || !has_first_full_block) &&
                   (!has_first_full_block || doris::thread_context()
                                                     ->thread_mem_tracker_mgr->limiter_mem_tracker()
                                                     ->check_limit(1))) {
                // 检查查询是否已取消
                if (UNLIKELY(ctx->done())) {
                    eos = true;
                    break;
                }
                // 检查是否超过最大运行时间，避免单个scanner长时间占用线程
                if (max_run_time_watch.elapsed_time() >
                    config::doris_scanner_max_run_time_ms * 1e6) {
                    break;
                }
                DEFER_RELEASE_RESERVED();
                
                // 获取空闲block用于存储扫描结果
                BlockUPtr free_block;
                if (first_read) {
                    free_block = ctx->get_free_block(first_read);
                } else {
                    // 非首次读取时尝试预留内存
                    if (state->get_query_ctx()
                                ->resource_ctx()
                                ->task_controller()
                                ->is_enable_reserve_memory()) {
                        size_t block_avg_bytes = scanner->get_block_avg_bytes();
                        auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(
                                block_avg_bytes);
                        if (!st.ok()) {
                            // 内存预留失败，进入低内存处理
                            handle_reserve_memory_failure(state, ctx, st, block_avg_bytes);
                            break;
                        }
                    }
                    free_block = ctx->get_free_block(first_read);
                }
                if (free_block == nullptr) {
                    break;
                }
                
                // ========== 核心：执行扫描读取数据 ==========
                // 调用scanner读取一个block的数据（包含投影操作）
                status = scanner->get_block_after_projects(state, free_block.get(), &eos);
                first_read = false;
                if (!status.ok()) {
                    LOG(WARNING) << "Scan thread read Scanner failed: " << status.to_string();
                    break;
                }
                
                // 确保虚拟列已物化
                _make_sure_virtual_col_is_materialized(scanner, free_block.get());
                
                // 投影后block大小可能变化，重新计算
                auto free_block_bytes = free_block->allocated_bytes();
                raw_bytes_read += free_block_bytes;
                
                // ========== Block合并逻辑 ==========
                // 如果当前block可以与上一个block合并（行数未超过batch_size）
                if (!scan_task->cached_blocks.empty() &&
                    scan_task->cached_blocks.back().first->rows() + free_block->rows() <=
                            ctx->batch_size()) {
                    // 合并到上一个block中，减少小block数量
                    size_t block_size = scan_task->cached_blocks.back().first->allocated_bytes();
                    vectorized::MutableBlock mutable_block(
                            scan_task->cached_blocks.back().first.get());
                    status = mutable_block.merge(*free_block);
                    if (!status.ok()) {
                        LOG(WARNING) << "Block merge failed: " << status.to_string();
                        break;
                    }
                    scan_task->cached_blocks.back().second = mutable_block.allocated_bytes();
                    scan_task->cached_blocks.back().first.get()->set_columns(
                            std::move(mutable_block.mutable_columns()));

                    // 归还空闲block以便复用
                    ctx->return_free_block(std::move(free_block));
                    ctx->inc_block_usage(scan_task->cached_blocks.back().first->allocated_bytes() -
                                         block_size);
                } else {
                    // 无法合并，作为新block添加
                    if (!scan_task->cached_blocks.empty()) {
                        has_first_full_block = true;
                    }
                    ctx->inc_block_usage(free_block->allocated_bytes());
                    scan_task->cached_blocks.emplace_back(std::move(free_block), free_block_bytes);
                }

                // 如果有小limit（小于batch_size），立即返回避免过度扫描
                // 例如 "select * from tbl where id=1 limit 10" 这种查询
                if (limit > 0 && limit < ctx->batch_size()) {
                    break;
                }

                // 更新block平均大小统计，用于内存预留估算
                if (scan_task->cached_blocks.back().first->rows() > 0) {
                    auto block_avg_bytes = (scan_task->cached_blocks.back().first->bytes() +
                                            scan_task->cached_blocks.back().first->rows() - 1) /
                                           scan_task->cached_blocks.back().first->rows() *
                                           ctx->batch_size();
                    scanner->update_block_avg_bytes(block_avg_bytes);
                }
                // 低内存模式下动态调整阈值
                if (ctx->low_memory_mode()) {
                    ctx->clear_free_blocks();
                    if (raw_bytes_threshold > ctx->low_memory_mode_scan_bytes_per_scanner()) {
                        raw_bytes_threshold = ctx->low_memory_mode_scan_bytes_per_scanner();
                    }
                }
            } // end for while

            // 处理扫描过程中的错误
            if (UNLIKELY(!status.ok())) {
                scan_task->set_status(status);
                eos = true;
            },
            status);

    // 异常捕获后的错误处理
    if (UNLIKELY(!status.ok())) {
        scan_task->set_status(status);
        eos = true;
    }

    // 如果扫描结束，标记scanner需要关闭
    if (eos) {
        scanner->mark_to_need_to_close();
    }
    scan_task->set_eos(eos);

    VLOG_DEBUG << fmt::format(
            "Scanner context {} has finished task, cached_block {} current scheduled task is "
            "{}, eos: {}, status: {}",
            ctx->ctx_id, scan_task->cached_blocks.size(), ctx->num_scheduled_scanners(), eos,
            status.to_string());

    // 将完成的scan_task放回队列，供ScanNode消费
    ctx->push_back_scan_task(scan_task);
}
int ScannerScheduler::default_local_scan_thread_num() {
    return config::doris_scanner_thread_pool_thread_num > 0
                   ? config::doris_scanner_thread_pool_thread_num
                   : std::max(48, CpuInfo::num_cores() * 2);
}
int ScannerScheduler::default_remote_scan_thread_num() {
    int num = config::doris_max_remote_scanner_thread_pool_thread_num > 0
                      ? config::doris_max_remote_scanner_thread_pool_thread_num
                      : std::max(512, CpuInfo::num_cores() * 10);
    return std::max(num, default_local_scan_thread_num());
}

int ScannerScheduler::get_remote_scan_thread_queue_size() {
    return config::doris_remote_scanner_thread_pool_queue_size;
}

int ScannerScheduler::default_min_active_scan_threads() {
    return config::min_active_scan_threads > 0
                   ? config::min_active_scan_threads
                   : config::min_active_scan_threads = CpuInfo::num_cores() * 2;
}

int ScannerScheduler::default_min_active_file_scan_threads() {
    return config::min_active_file_scan_threads > 0
                   ? config::min_active_file_scan_threads
                   : config::min_active_file_scan_threads = CpuInfo::num_cores() * 8;
}

void ScannerScheduler::_make_sure_virtual_col_is_materialized(
        const std::shared_ptr<Scanner>& scanner, vectorized::Block* free_block) {
#ifndef NDEBUG
    // Currently, virtual column can only be used on olap table.
    std::shared_ptr<OlapScanner> olap_scanner = std::dynamic_pointer_cast<OlapScanner>(scanner);
    if (olap_scanner == nullptr) {
        return;
    }

    size_t idx = 0;
    for (const auto& entry : *free_block) {
        // Virtual column must be materialized on the end of SegmentIterator's next batch method.
        const vectorized::ColumnNothing* column_nothing =
                vectorized::check_and_get_column<vectorized::ColumnNothing>(entry.column.get());
        if (column_nothing == nullptr) {
            idx++;
            continue;
        }

        std::vector<std::string> vcid_to_idx;

        for (const auto& pair : olap_scanner->_vir_cid_to_idx_in_block) {
            vcid_to_idx.push_back(fmt::format("{}-{}", pair.first, pair.second));
        }

        std::string error_msg = fmt::format(
                "Column in idx {} is nothing, block columns {}, normal_columns "
                "{}, "
                "vir_cid_to_idx_in_block_msg {}",
                idx, free_block->columns(), olap_scanner->_return_columns.size(),
                fmt::format("_vir_cid_to_idx_in_block:[{}]", fmt::join(vcid_to_idx, ",")));
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, error_msg);
    }
#endif
}

Result<SharedListenableFuture<Void>> ScannerSplitRunner::process_for(std::chrono::nanoseconds) {
    _started = true;
    bool is_completed = _scan_func();
    if (is_completed) {
        _completion_future.set_value(Void {});
    }
    return SharedListenableFuture<Void>::create_ready(Void {});
}

bool ScannerSplitRunner::is_finished() {
    return _completion_future.is_done();
}

Status ScannerSplitRunner::finished_status() {
    return _completion_future.get_status();
}

bool ScannerSplitRunner::is_started() const {
    return _started.load();
}

bool ScannerSplitRunner::is_auto_reschedule() const {
    return false;
}

} // namespace doris::vectorized
