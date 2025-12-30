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

#include "async_result_writer.h"

#include "common/status.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class TExpr;

namespace vectorized {
#include "common/compile_check_begin.h"

AsyncResultWriter::AsyncResultWriter(const doris::vectorized::VExprContextSPtrs& output_expr_ctxs,
                                     std::shared_ptr<pipeline::Dependency> dep,
                                     std::shared_ptr<pipeline::Dependency> fin_dep)
        : _vec_output_expr_ctxs(output_expr_ctxs), _dependency(dep), _finish_dependency(fin_dep) {}

Status AsyncResultWriter::sink(Block* block, bool eos) {
    // 1. 获取当前 block 的行数，如果有数据则从空闲块池获取一个块并复制数据
    // 这样做是为了避免在锁内进行数据复制，减少锁持有时间
    auto rows = block->rows();
    std::unique_ptr<Block> add_block;
    if (rows) {
        add_block = _get_free_block(block, rows);
    }

    // 2. 加锁保护共享状态（_writer_status、_data_queue、_eos、_dependency 等）
    std::lock_guard l(_m);
    // 3. 如果后台 IO 任务已经失败，直接返回错误状态，终止查询
    // if io task failed, just return error status to
    // end the query
    if (!_writer_status.ok()) {
        return _writer_status.status();
    }

    // 4. 如果写入器已经完成（后台线程已处理完所有数据），设置依赖为 ready
    // 这样下游算子可以继续执行
    DCHECK(_dependency);
    if (_is_finished()) {
        _dependency->set_ready();
    }
    // 5. 如果有数据，将块加入队列供后台线程处理
    if (rows) {
        // 更新内存使用计数器（记录队列中数据占用的内存）
        _memory_used_counter->update(add_block->allocated_bytes());
        // 将块加入队列（后台 process_block 线程会从队列中取出并写入）
        _data_queue.emplace_back(std::move(add_block));
        // 6. 流控：如果队列已满（不可用）且写入器未完成，阻塞依赖
        // 这会暂停上游算子的执行，直到队列有空间（后台线程消费数据后）
        if (!_data_queue_is_available() && !_is_finished()) {
            _dependency->block();
        }
    }
    // 7. 设置 eos 标志（必须在修改 _data_queue 之后设置）
    // 原因：后台 process_block 线程在检查时先检查 _eos，再检查 _data_queue
    // 如果先设置 _eos=true 但队列还有数据，可能导致线程误判为已完成而退出
    // 因此必须保证：先修改队列，再修改 eos，避免多线程逻辑错误
    // in 'process block' we check _eos first and _data_queue second so here
    // in the lock. must modify the _eos after change _data_queue to make sure
    // not lead the logic error in multi thread
    _eos = eos;

    // 8. 通知等待的后台线程（process_block）：有新数据到达或已到达 eos
    _cv.notify_one();
    return Status::OK();
}

std::unique_ptr<Block> AsyncResultWriter::_get_block_from_queue() {
    std::lock_guard l(_m);
    DCHECK(!_data_queue.empty());
    auto block = std::move(_data_queue.front());
    _data_queue.pop_front();
    DCHECK(_dependency);
    if (_data_queue_is_available()) {
        _dependency->set_ready();
    }
    _memory_used_counter->update(-block->allocated_bytes());
    return block;
}

Status AsyncResultWriter::start_writer(RuntimeState* state, RuntimeProfile* operator_profile) {
    // Attention!!!
    // AsyncResultWriter::open is called asynchronously,
    // so we need to setupt the operator_profile and memory counter here,
    // or else the counter can be nullptr when AsyncResultWriter::sink is called.
    _operator_profile = operator_profile;
    DCHECK(_operator_profile->get_child("CommonCounters") != nullptr);
    _memory_used_counter =
            _operator_profile->get_child("CommonCounters")->get_counter("MemoryUsage");
    DCHECK(_memory_used_counter != nullptr);
    // Should set to false here, to
    DCHECK(_finish_dependency);
    _finish_dependency->block();
    // This is a async thread, should lock the task ctx, to make sure runtimestate and operator_profile
    // not deconstructed before the thread exit.
    auto task_ctx = state->get_task_execution_context();
    RETURN_IF_ERROR(ExecEnv::GetInstance()->fragment_mgr()->get_thread_pool()->submit_func(
            [this, state, operator_profile, task_ctx]() {
                SCOPED_ATTACH_TASK(state);
                auto task_lock = task_ctx.lock();
                if (task_lock == nullptr) {
                    return;
                }
                this->process_block(state, operator_profile);
                task_lock.reset();
            }));
    return Status::OK();
}

/**
 * AsyncResultWriter::process_block：异步写入器的后台处理线程主循环函数
 * 
 * 功能：
 * - 从数据队列中取出 Block，调用 write() 写入下游（如 RPC 发送到 BE、写入文件等）
 * - 处理写入完成后的收尾工作（finish、close）
 * 
 * 执行流程：
 * 1. 初始化：调用 open() 打开写入器
 * 2. 设置工作负载组：将线程加入 cgroup，设置线程名
 * 3. 主循环：不断从队列取数据并写入，直到队列为空且到达 eos
 * 4. 完成写入：调用 finish() 完成写入（如提交事务）
 * 5. 关闭写入器：调用 close() 清理资源
 * 6. 设置完成依赖：通知上游写入已完成
 */
void AsyncResultWriter::process_block(RuntimeState* state, RuntimeProfile* operator_profile) {
    // 1. 初始化写入器（打开连接、初始化资源等）
    // 如果初始化失败，强制关闭并返回
    if (auto status = open(state, operator_profile); !status.ok()) {
        force_close(status);
    }

    // 2. 设置工作负载组（WorkloadGroup）的 CPU 控制
    // 如果查询属于某个工作负载组，将当前线程加入该组的 cgroup，并设置线程名
    // 这样可以实现资源隔离和监控
    if (state && state->get_query_ctx() && state->get_query_ctx()->workload_group()) {
        if (auto cg_ctl_sptr =
                    state->get_query_ctx()->workload_group()->get_cgroup_cpu_ctl_wptr().lock()) {
            Status ret = cg_ctl_sptr->add_thread_to_cgroup();
            if (ret.ok()) {
                std::string wg_tname =
                        "asyc_wr_" + state->get_query_ctx()->workload_group()->name();
                Thread::set_self_name(wg_tname);
            }
        }
    }

    // 3. 主循环：不断从队列中取出数据并写入下游
    DCHECK(_dependency);
    while (_writer_status.ok()) {
        // 3.1 启动 CPU 时间统计（用于资源监控）
        ThreadCpuStopWatch cpu_time_stop_watch;
        cpu_time_stop_watch.start();
        // 使用 Defer 确保在退出作用域时更新 CPU 时间统计
        Defer defer {[&]() {
            if (state && state->get_query_ctx()) {
                state->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(
                        cpu_time_stop_watch.elapsed_time());
            }
        }};

        // 3.2 等待上游算子（如 ScanOperator）将数据写入队列
        {
            std::unique_lock l(_m);
            // 等待条件：未到达 eos 且队列为空且写入器状态正常且 FragmentMgr 未关闭
            // 当查询被取消时，_writer_status 可能在 force_close 中被设置为错误状态
            // 当 BE 进程优雅退出时，FragmentMgr 的线程池会被关闭，异步线程也会退出
            while (!_eos && _data_queue.empty() && _writer_status.ok() &&
                   !ExecEnv::GetInstance()->fragment_mgr()->shutting_down()) {
                // 使用 1 秒超时等待，避免信号丢失（定期检查条件）
                // Add 1s to check to avoid lost signal
                _cv.wait_for(l, std::chrono::seconds(1));
            }
            // 如果写入器状态不是 ok，则不应该改变其状态，避免丢失实际的错误状态
            // If writer status is not ok, then we should not change its status to avoid lost the actual error status.
            if (ExecEnv::GetInstance()->fragment_mgr()->shutting_down() && _writer_status.ok()) {
                _writer_status.update(Status::InternalError<false>("FragmentMgr is shutting down"));
            }

            // 3.3 检查是否应该退出循环：到达 eos 且队列为空，或写入器出错
            // check if eos or writer error
            if ((_eos && _data_queue.empty()) || !_writer_status.ok()) {
                _data_queue.clear();
                break;
            }
        }

        // 3.4 从队列中取出 Block 并写入下游
        // get the block from data queue and write to downstream
        auto block = _get_block_from_queue();
        auto status = write(state, *block);
        if (!status.ok()) [[unlikely]] {
            // 如果写入失败，更新写入器状态并退出循环
            std::unique_lock l(_m);
            _writer_status.update(status);
            if (_is_finished()) {
                _dependency->set_ready();
            }
            break;
        }

        // 3.5 将 Block 归还到空闲块池，供后续复用
        _return_free_block(std::move(block));
    }

    // 4. 判断是否需要调用 finish() 完成写入
    // finish() 的作用：清空缓冲区、提交事务等收尾工作
    bool need_finish = false;
    {
        // 如果最后一个 Block 发送成功，则调用 finish 清空缓冲区或提交事务
        // If the last block is sent successfuly, then call finish to clear the buffer or commit transactions.
        // 使用锁确保写入器状态不会被修改
        // Using lock to make sure the writer status is not modified
        // Status 中有 unique_ptr err_msg，如果被修改，unique_ptr 可能被释放，导致 use after free
        // There is a unique ptr err_msg in Status, if it is modified, the unique ptr
        // maybe released. And it will core because use after free.
        std::lock_guard l(_m);
        if (_writer_status.ok() && _eos) {
            need_finish = true;
        }
    }
    // eos 只意味着最后一个 Block 已输入到队列且不会有更多 Block 被添加
    // 但不保证 Block 已经写入到流中
    // eos only means the last block is input to the queue and there is no more block to be added,
    // it is not sure that the block is written to stream.
    if (need_finish) {
        // 不应该在锁内调用 finish，因为它可能会挂起，导致锁持有时间过长
        // 而且 get_writer_status 也需要这个锁，会阻塞 pipeline 执行线程
        // Should not call finish in lock because it may hang, and it will lock _m too long.
        // And get_writer_status will also need this lock, it will block pipeline exec thread.
        Status st = finish(state);
        _writer_status.update(st);
    }
    
    // 5. 获取最终的写入状态（用于 close）
    Status st = Status::OK();
    { st = _writer_status.status(); }

    // 6. 关闭写入器（清理资源、关闭连接等）
    Status close_st = close(st);
    {
        // 如果之前已经失败，则不更新写入状态，以便获取真实的原因
        // If it is already failed before, then not update the write status so that we could get the real reason.
        std::lock_guard l(_m);
        if (_writer_status.ok()) {
            _writer_status.update(close_st);
        }
    }
    
    // 7. 设置完成依赖为 ready（通知上游写入已完成）
    // 应该在 close 之前设置，因为 close 函数可能被 execution_timeout 的 wait_close 阻塞
    // should set _finish_dependency first, as close function maybe blocked by wait_close of execution_timeout
    _set_ready_to_finish();
}

void AsyncResultWriter::_set_ready_to_finish() {
    DCHECK(_finish_dependency);
    _finish_dependency->set_ready();
}

Status AsyncResultWriter::_projection_block(doris::vectorized::Block& input_block,
                                            doris::vectorized::Block* output_block) {
    Status status = Status::OK();
    if (input_block.rows() == 0) {
        return status;
    }
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, input_block, output_block));
    materialize_block_inplace(*output_block);
    return status;
}

void AsyncResultWriter::force_close(Status s) {
    std::lock_guard l(_m);
    _writer_status.update(s);
    DCHECK(_dependency);
    if (_is_finished()) {
        _dependency->set_ready();
    }
    _cv.notify_one();
}

void AsyncResultWriter::_return_free_block(std::unique_ptr<Block> b) {
    if (_low_memory_mode) {
        return;
    }

    const auto allocated_bytes = b->allocated_bytes();
    if (_free_blocks.enqueue(std::move(b))) {
        _memory_used_counter->update(allocated_bytes);
    }
}

std::unique_ptr<Block> AsyncResultWriter::_get_free_block(doris::vectorized::Block* block,
                                                          size_t rows) {
    std::unique_ptr<Block> b;
    if (!_free_blocks.try_dequeue(b)) {
        b = block->create_same_struct_block(rows, true);
    } else {
        _memory_used_counter->update(-b->allocated_bytes());
    }
    b->swap(*block);
    return b;
}

template <typename T>
void clear_blocks(moodycamel::ConcurrentQueue<T>& blocks,
                  RuntimeProfile::Counter* memory_used_counter = nullptr);
void AsyncResultWriter::set_low_memory_mode() {
    _low_memory_mode = true;
    clear_blocks(_free_blocks, _memory_used_counter);
}
} // namespace vectorized
} // namespace doris
