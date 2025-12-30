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

#include "olap/memtable_flush_executor.h"

#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "olap/memtable.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/storage_engine.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/pretty_printer.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<int64_t> g_flush_task_num("memtable_flush_task_num");

class MemtableFlushTask final : public Runnable {
    ENABLE_FACTORY_CREATOR(MemtableFlushTask);

public:
    MemtableFlushTask(std::shared_ptr<FlushToken> flush_token, std::shared_ptr<MemTable> memtable,
                      int32_t segment_id, int64_t submit_task_time)
            : _flush_token(flush_token),
              _memtable(memtable),
              _segment_id(segment_id),
              _submit_task_time(submit_task_time) {
        g_flush_task_num << 1;
    }

    ~MemtableFlushTask() override { g_flush_task_num << -1; }

    void run() override {
        auto token = _flush_token.lock();
        if (token) {
            token->_flush_memtable(_memtable, _segment_id, _submit_task_time);
        } else {
            LOG(WARNING) << "flush token is deconstructed, ignore the flush task";
        }
    }

private:
    std::weak_ptr<FlushToken> _flush_token;
    std::shared_ptr<MemTable> _memtable;
    int32_t _segment_id;
    int64_t _submit_task_time;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / NANOS_PER_MILLIS
       << ", flush wait time(ms)=" << stat.flush_wait_time_ns / NANOS_PER_MILLIS
       << ", flush submit count=" << stat.flush_submit_count
       << ", running flush count=" << stat.flush_running_count
       << ", finish flush count=" << stat.flush_finish_count
       << ", flush bytes: " << stat.flush_size_bytes
       << ", flush disk bytes: " << stat.flush_disk_size_bytes << ")";
    return os;
}

Status FlushToken::submit(std::shared_ptr<MemTable> mem_table) {
    {
        std::shared_lock rdlk(_flush_status_lock);
        DBUG_EXECUTE_IF("FlushToken.submit_flush_error", {
            _flush_status = Status::IOError<false>("dbug_be_memtable_submit_flush_error");
        });
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }

    if (mem_table == nullptr || mem_table->empty()) {
        return Status::OK();
    }
    int64_t submit_task_time = MonotonicNanos();
    auto task = MemtableFlushTask::create_shared(
            shared_from_this(), mem_table, _rowset_writer->allocate_segment_id(), submit_task_time);
    // NOTE: we should guarantee WorkloadGroup is not deconstructed when submit memtable flush task.
    // because currently WorkloadGroup's can only be destroyed when all queries in the group is finished,
    // but not consider whether load channel is finish.
    std::shared_ptr<WorkloadGroup> wg_sptr = _wg_wptr.lock();
    ThreadPool* wg_thread_pool = nullptr;
    if (wg_sptr) {
        wg_thread_pool = wg_sptr->get_memtable_flush_pool();
    }
    Status ret = wg_thread_pool ? wg_thread_pool->submit(std::move(task))
                                : _thread_pool->submit(std::move(task));
    if (ret.ok()) {
        // _wait_running_task_finish was executed after this function, so no need to notify _cond here
        _stats.flush_submit_count++;
    }
    return ret;
}

// NOTE: FlushToken's submit/cancel/wait run in one thread,
// so we don't need to make them mutually exclusive, std::atomic is enough.
void FlushToken::_wait_submit_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _submit_task_finish_cond.wait(lock, [&]() { return _stats.flush_submit_count.load() == 0; });
}

void FlushToken::_wait_running_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _running_task_finish_cond.wait(lock, [&]() { return _stats.flush_running_count.load() == 0; });
}

void FlushToken::cancel() {
    _shutdown_flush_token();
    _wait_running_task_finish();
}

Status FlushToken::wait() {
    _wait_submit_task_finish();
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }
    return Status::OK();
}

Status FlushToken::_try_reserve_memory(const std::shared_ptr<ResourceContext>& resource_context,
                                       int64_t size) {
    auto* thread_context = doris::thread_context();
    auto* memtable_flush_executor =
            ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    Status st;
    int32_t max_waiting_time = config::memtable_wait_for_memory_sleep_time_s;
    do {
        // only try to reserve process memory
        st = thread_context->thread_mem_tracker_mgr->try_reserve(
                size, ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
        if (st.ok()) {
            memtable_flush_executor->inc_flushing_task();
            break;
        }
        if (_is_shutdown() || resource_context->task_controller()->is_cancelled()) {
            st = Status::Cancelled("flush memtable already cancelled");
            break;
        }
        // Make sure at least one memtable is flushing even reserve memory failed.
        if (memtable_flush_executor->check_and_inc_has_any_flushing_task()) {
            // If there are already any flushing task, Wait for some time and retry.
            LOG_EVERY_T(INFO, 60) << fmt::format(
                    "Failed to reserve memory {} for flush memtable, retry after 100ms",
                    PrettyPrinter::print_bytes(size));
            std::this_thread::sleep_for(std::chrono::seconds(1));
            max_waiting_time -= 1;
        } else {
            st = Status::OK();
            break;
        }
    } while (max_waiting_time > 0);
    return st;
}

Status FlushToken::_do_flush_memtable(MemTable* memtable, int32_t segment_id, int64_t* flush_size) {
    // 记录刷新开始的详细信息：包含tablet ID、内存大小和行数
    // 这些信息用于性能监控和问题诊断，便于追踪特定tablet的刷新性能
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << memtable->tablet_id()
                  << ", memsize: " << PrettyPrinter::print_bytes(memtable->memory_usage())
                  << ", rows: " << memtable->stat().raw_rows;
    
    // 更新内存表状态为"刷新中"：表示内存表正在进行刷新操作
    // 这个状态变化用于跟踪内存表的生命周期，防止在刷新过程中进行其他操作
    memtable->update_mem_type(MemType::FLUSH);
    
    // 声明持续时间变量：用于记录刷新操作的实际执行时间
    // 这个时间不包含等待时间，只包含实际的刷新操作耗时
    int64_t duration_ns = 0;
    
    // 性能监控和资源管理的作用域块
    {
        // 开始性能计时：记录刷新操作的精确执行时间
        // 使用纳秒级精度，提供高精度的性能数据
        SCOPED_RAW_TIMER(&duration_ns);
        
        // 附加任务上下文：将当前刷新任务与资源上下文关联
        // 用于资源跟踪和限制，确保任务在正确的资源上下文中执行
        SCOPED_ATTACH_TASK(memtable->resource_ctx());
        
        // 切换线程内存跟踪器：使用写入跟踪器监控内存使用
        // 这有助于区分刷新操作和其他操作的内存使用情况
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
        
        // 消费内存跟踪器：记录当前刷新操作的内存消耗
        // 用于内存使用统计和限制，防止内存过度使用
        SCOPED_CONSUME_MEM_TRACKER(memtable->mem_tracker());

        // 延迟释放预留内存：确保在刷新过程中有足够的内存
        // 这是一个防御性措施，防止内存不足导致的刷新失败
        DEFER_RELEASE_RESERVED();

        // 获取刷新所需的内存大小：计算刷新操作需要预留的内存
        // 这个大小通常基于内存表的数据量和刷新算法的需求
        auto reserve_size = memtable->get_flush_reserve_memory_size();
        
        // 内存预留检查：如果启用了内存预留且需要预留内存，则尝试预留
        // 内存预留机制确保刷新操作有足够的内存资源，提高成功率
        if (memtable->resource_ctx()->task_controller()->is_enable_reserve_memory() &&
            reserve_size > 0) {
            RETURN_IF_ERROR(_try_reserve_memory(memtable->resource_ctx(), reserve_size));
        }

        // 延迟清理函数：在刷新完成后减少刷新任务计数
        // 使用RAII模式确保计数正确更新，即使发生异常也能执行清理
        Defer defer {[&]() {
            ExecEnv::GetInstance()->storage_engine().memtable_flush_executor()->dec_flushing_task();
        }};
        
        // 数据转换：将内存表转换为数据块格式
        // 这是刷新过程的关键步骤，将内存中的数据结构转换为可序列化的格式
        std::unique_ptr<vectorized::Block> block;
        RETURN_IF_ERROR(memtable->to_block(&block));
        
        // 核心刷新操作：调用RowsetWriter将数据块写入磁盘
        // 这是真正的磁盘写入操作，包含数据序列化、文件写入和索引构建
        RETURN_IF_ERROR(_rowset_writer->flush_memtable(block.get(), segment_id, flush_size));
        
        // 标记刷新成功：更新内存表状态，表示刷新操作已完成
        // 这个状态变化用于后续的资源清理和内存表重用
        memtable->set_flush_success();
    }
    
    // 统计信息更新：累加内存表的统计信息到总统计中
    // 这些统计信息用于系统性能分析和监控
    _memtable_stat += memtable->stat();
    
    // 全局指标更新：增加刷新总数计数
    // 用于监控系统的整体刷新性能和负载情况
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    
    // 全局指标更新：累加刷新持续时间（微秒）
    // 用于分析刷新操作的性能趋势和瓶颈
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    
    // 记录刷新完成的详细信息：包含tablet ID和刷新后的磁盘大小
    // 这些信息用于验证刷新结果和性能分析
    VLOG_CRITICAL << "after flush memtable for tablet: " << memtable->tablet_id()
                  << ", flushsize: " << PrettyPrinter::print_bytes(*flush_size);
    
    // 返回成功状态：表示刷新操作已成功完成
    return Status::OK();
}

void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable_ptr, int32_t segment_id,
                                 int64_t submit_task_time) {
    // 设置信号任务ID：将当前刷新任务与特定的加载任务关联
    // 用于任务跟踪和调试，便于识别刷新任务属于哪个加载操作
    signal::set_signal_task_id(_rowset_writer->load_id());
    
    // 设置tablet ID：记录当前刷新的tablet标识
    // 用于日志记录和性能监控，便于追踪特定tablet的刷新性能
    signal::tablet_id = memtable_ptr->tablet_id();
    
    // 延迟清理函数：在函数退出时自动执行清理操作
    // 使用RAII模式确保资源正确释放，即使发生异常也能执行清理
    Defer defer {[&]() {
        std::lock_guard<std::mutex> lock(_mutex);
        
        // 减少提交任务计数：表示一个提交的任务开始执行
        _stats.flush_submit_count--;
        // 如果所有提交的任务都开始执行，通知等待线程
        if (_stats.flush_submit_count == 0) {
            _submit_task_finish_cond.notify_one();
        }
        
        // 减少运行任务计数：表示一个运行的任务完成
        _stats.flush_running_count--;
        // 如果所有运行的任务都完成，通知等待线程
        if (_stats.flush_running_count == 0) {
            _running_task_finish_cond.notify_one();
        }
    }};
    
    // 调试模式：在第一次关闭检查前等待10秒
    // 用于测试关闭逻辑和超时机制
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_before_first_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    
    // 第一次关闭检查：如果刷新令牌已关闭，直接返回
    // 避免在已关闭的令牌上执行刷新操作
    if (_is_shutdown()) {
        return;
    }
    
    // 调试模式：在第一次关闭检查后等待10秒
    // 用于测试关闭检查的时序
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_after_first_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    
    // 增加运行任务计数：表示当前任务开始运行
    _stats.flush_running_count++;
    
    // 双重关闭检查：再次检查是否已关闭，确保计数准确性
    // 这是因为在第一次检查和增加计数之间可能发生关闭操作
    if (_is_shutdown()) {
        return;
    }
    
    // 调试模式：在第二次关闭检查后等待10秒
    // 用于测试双重关闭检查的逻辑
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_after_second_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    
    // 计算刷新等待时间：从任务提交到开始执行的时间间隔
    // 用于监控刷新任务的调度延迟，识别系统瓶颈
    uint64_t flush_wait_time_ns = MonotonicNanos() - submit_task_time;
    _stats.flush_wait_time_ns += flush_wait_time_ns;
    
    // 检查刷新状态：如果之前的刷新已失败，直接返回
    // 使用共享锁读取状态，避免阻塞其他读取操作
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }

    // 开始性能计时：记录刷新操作的执行时间
    MonotonicStopWatch timer;
    timer.start();
    
    // 记录内存使用量：用于统计和监控内存使用情况
    size_t memory_usage = memtable_ptr->memory_usage();

    // 执行实际的刷新操作：将内存表数据写入磁盘
    // 这是整个刷新过程的核心，包含数据序列化和文件写入
    int64_t flush_size;
    Status s = _do_flush_memtable(memtable_ptr.get(), segment_id, &flush_size);

    // 再次检查刷新状态：确保在刷新过程中没有发生状态变化
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }
    
    // 错误处理：如果刷新失败，更新状态并记录错误日志
    if (!s.ok()) {
        std::lock_guard wrlk(_flush_status_lock);  // 使用写锁更新状态
        LOG(WARNING) << "Flush memtable failed with res = " << s
                     << ", load_id: " << print_id(_rowset_writer->load_id());
        _flush_status = s;  // 更新刷新状态为失败状态
        return;
    }

    // 记录详细的刷新统计信息：包含等待时间、执行时间、各种计数和大小信息
    // 这些信息用于性能分析和系统监控
    VLOG_CRITICAL << "flush memtable wait time: "
                  << PrettyPrinter::print(flush_wait_time_ns, TUnit::TIME_NS)
                  << ", flush memtable cost: "
                  << PrettyPrinter::print(timer.elapsed_time(), TUnit::TIME_NS)
                  << ", submit count: " << _stats.flush_submit_count
                  << ", running count: " << _stats.flush_running_count
                  << ", finish count: " << _stats.flush_finish_count
                  << ", mem size: " << PrettyPrinter::print_bytes(memory_usage)
                  << ", disk size: " << PrettyPrinter::print_bytes(flush_size);
    
    // 更新统计信息：累加各种性能指标
    _stats.flush_time_ns += timer.elapsed_time();           // 累加刷新执行时间
    _stats.flush_finish_count++;                           // 增加完成刷新计数
    _stats.flush_size_bytes += memtable_ptr->memory_usage(); // 累加刷新内存大小
    _stats.flush_disk_size_bytes += flush_size;            // 累加刷新磁盘大小
}

void MemTableFlushExecutor::init(int num_disk) {
    num_disk = std::max(1, num_disk);
    int num_cpus = std::thread::hardware_concurrency();
    int min_threads = std::max(1, config::flush_thread_num_per_store);
    int max_threads = num_cpus == 0 ? num_disk * min_threads
                                    : std::min(num_disk * min_threads,
                                               num_cpus * config::max_flush_thread_num_per_cpu);
    static_cast<void>(ThreadPoolBuilder("MemTableFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_flush_pool));

    min_threads = std::max(1, config::high_priority_flush_thread_num_per_store);
    max_threads = num_cpus == 0 ? num_disk * min_threads
                                : std::min(num_disk * min_threads,
                                           num_cpus * config::max_flush_thread_num_per_cpu);
    static_cast<void>(ThreadPoolBuilder("MemTableHighPriorityFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_high_prio_flush_pool));
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                                                 std::shared_ptr<RowsetWriter> rowset_writer,
                                                 bool is_high_priority,
                                                 std::shared_ptr<WorkloadGroup> wg_sptr) {
    switch (rowset_writer->type()) {
    case ALPHA_ROWSET:
        // alpha rowset do not support flush in CONCURRENT.  and not support alpha rowset now.
        return Status::InternalError<false>("not support alpha rowset load now.");
    case BETA_ROWSET: {
        // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
        ThreadPool* pool = is_high_priority ? _high_prio_flush_pool.get() : _flush_pool.get();
        flush_token = FlushToken::create_shared(pool, wg_sptr);
        flush_token->set_rowset_writer(rowset_writer);
        return Status::OK();
    }
    default:
        return Status::InternalError<false>("unknown rowset type.");
    }
}

} // namespace doris
