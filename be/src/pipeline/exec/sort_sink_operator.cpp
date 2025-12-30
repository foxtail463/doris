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

#include "sort_sink_operator.h"

#include <string>

#include "pipeline/exec/operator.h"
#include "runtime/query_context.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status SortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _sort_blocks_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageSortBlocks", TUnit::BYTES, 1);
    _append_blocks_timer = ADD_TIMER(custom_profile(), "AppendBlockTime");
    _update_runtime_predicate_timer = ADD_TIMER(custom_profile(), "UpdateRuntimePredicateTime");
    return Status::OK();
}

Status SortSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<SortSinkOperatorX>();

    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    switch (p._algorithm) {
    case TSortAlgorithm::HEAP_SORT: {
        _shared_state->sorter = vectorized::HeapSorter::create_shared(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child->row_desc(), state->get_query_ctx()->has_runtime_predicate(p._node_id));
        break;
    }
    case TSortAlgorithm::TOPN_SORT: {
        _shared_state->sorter = vectorized::TopNSorter::create_shared(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child->row_desc(), state, custom_profile());
        break;
    }
    case TSortAlgorithm::FULL_SORT: {
        auto sorter = vectorized::FullSorter::create_shared(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child->row_desc(), state, custom_profile());
        if (p._max_buffered_bytes > 0) {
            sorter->set_max_buffered_block_bytes(p._max_buffered_bytes);
        }
        _shared_state->sorter = std::move(sorter);
        break;
    }
    default: {
        return Status::InvalidArgument("Invalid sort algorithm!");
    }
    }

    _shared_state->sorter->init_profile(custom_profile());

    custom_profile()->add_info_string("TOP-N", p._limit == -1 ? "false" : "true");
    custom_profile()->add_info_string(
            "SortAlgorithm",
            p._algorithm == TSortAlgorithm::HEAP_SORT
                    ? "HEAP_SORT"
                    : (p._algorithm == TSortAlgorithm::TOPN_SORT ? "TOPN_SORT" : "FULL_SORT"));
    return Status::OK();
}

SortSinkOperatorX::SortSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                     const TPlanNode& tnode, const DescriptorTbl& descs,
                                     const bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode, dest_id),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _pool(pool),
          _limit(tnode.limit),
          _row_descriptor(descs, tnode.row_tuples),
          _merge_by_exchange(tnode.sort_node.merge_by_exchange),
          _is_colocate(tnode.sort_node.__isset.is_colocate && tnode.sort_node.is_colocate),
          _require_bucket_distribution(require_bucket_distribution),
          _is_analytic_sort(tnode.sort_node.__isset.is_analytic_sort
                                    ? tnode.sort_node.is_analytic_sort
                                    : false),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _algorithm(tnode.sort_node.__isset.algorithm ? tnode.sort_node.algorithm
                                                       : TSortAlgorithm::FULL_SORT),
          _reuse_mem(_algorithm != TSortAlgorithm::HEAP_SORT),
          _max_buffered_bytes(tnode.sort_node.__isset.full_sort_max_buffered_bytes
                                      ? tnode.sort_node.full_sort_max_buffered_bytes
                                      : -1) {}

Status SortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;

    auto* query_ctx = state->get_query_ctx();
    // init runtime predicate
    if (query_ctx->has_runtime_predicate(_node_id) && _algorithm == TSortAlgorithm::HEAP_SORT) {
        query_ctx->get_runtime_predicate(_node_id).set_detected_source();
    }
    return Status::OK();
}

Status SortSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<SortSinkLocalState>::prepare(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _child->row_desc(), _row_descriptor));
    return _vsort_exec_exprs.open(state);
}

Status SortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block, bool eos) {
    // ===== 功能说明：SortSinkOperatorX 的核心方法，接收上游数据并进行排序 =====
    // 
    // 【工作流程】
    // 1. 接收上游 Pipeline 传来的数据块（in_block）
    // 2. 将数据追加到排序器（sorter）中进行排序
    // 3. 更新内存使用统计和运行时谓词
    // 4. 当数据流结束时（eos=true），准备排序器供下游读取，并通知依赖关系
    
    // ===== 阶段 1：初始化并记录统计信息 =====
    // 获取当前 PipelineTask 对应的本地状态
    // 每个 PipelineTask 都有独立的本地状态实例，用于管理该 task 的排序操作
    auto& local_state = get_local_state(state);
    // 记录本次 sink 操作的执行时间，用于性能分析
    SCOPED_TIMER(local_state.exec_time_counter());
    // 更新输入行数计数器，记录已处理的输入数据行数
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    
    // ===== 阶段 2：处理输入数据块 =====
    // 如果输入块中有数据（行数 > 0），进行排序处理
    if (in_block->rows() > 0) {
        // ===== 阶段 2.1：将数据追加到排序器 =====
        {
            // 记录追加数据块的时间，用于性能分析
            SCOPED_TIMER(local_state._append_blocks_timer);
            // 将输入块追加到排序器中
            // sorter 会根据排序算法（HEAP_SORT、TOPN_SORT、FULL_SORT）进行排序
            // 例如：
            // - HeapSorter：使用堆排序，维护 TopN 结果
            // - TopNSorter：使用 TopN 排序算法
            // - FullSorter：使用全排序，支持 spill 到磁盘
            RETURN_IF_ERROR(local_state._shared_state->sorter->append_block(in_block));
        }
        
        // ===== 阶段 2.2：更新内存使用统计 =====
        // 获取排序器当前使用的内存大小（包括已排序的数据块）
        int64_t data_size = local_state._shared_state->sorter->data_size();
        // 更新排序块的内存使用统计，用于监控和内存管理
        COUNTER_SET(local_state._sort_blocks_memory_usage, data_size);
        // 更新总内存使用计数器，用于查询级别的内存追踪
        COUNTER_SET(local_state._memory_used_counter, data_size);

        // ===== 阶段 2.3：检查查询是否被取消 =====
        // 如果查询被取消（如用户执行了 CANCEL 命令），立即返回错误
        // 这样可以快速响应取消请求，避免继续处理无用的数据
        RETURN_IF_CANCELLED(state);

        // ===== 阶段 2.4：更新运行时谓词（Runtime Predicate） =====
        // 运行时谓词是一种查询优化技术，用于在查询执行过程中动态生成过滤条件
        // 
        // 【工作原理】
        // 1. 在 TopN 查询中，排序后的"最小值"（top value）可以作为过滤条件
        // 2. 将这个值推送给上游的扫描算子，用于过滤不需要的数据
        // 3. 例如：如果当前 TopN 的最小值是 100，上游可以跳过 < 100 的数据
        //
        // 【举例】
        // SQL: SELECT * FROM t ORDER BY id LIMIT 10
        // 当排序器收集到足够的数据后，top_value 可能是 100
        // 运行时谓词会告诉扫描算子：只扫描 id >= 100 的数据，跳过 id < 100 的数据
        if (state->get_query_ctx()->has_runtime_predicate(_node_id)) {
            // 记录更新运行时谓词的时间
            SCOPED_TIMER(local_state._update_runtime_predicate_timer);
            // 获取该节点的运行时谓词
            auto& predicate = state->get_query_ctx()->get_runtime_predicate(_node_id);
            // 如果运行时谓词已启用
            if (predicate.enable()) {
                // 获取排序器当前的"最小值"（top value）
                // 对于 TopN 查询，这是当前 TopN 结果中的最小值
                vectorized::Field new_top = local_state._shared_state->sorter->get_top_value();
                // 如果新值有效且与上次不同，更新运行时谓词
                // 这样可以避免重复更新相同的值
                if (!new_top.is_null() && new_top != local_state.old_top) {
                    auto* query_ctx = state->get_query_ctx();
                    // 更新运行时谓词，将新值推送给上游算子
                    RETURN_IF_ERROR(query_ctx->get_runtime_predicate(_node_id).update(new_top));
                    // 保存当前值，用于下次比较
                    local_state.old_top = std::move(new_top);
                }
            }
        }
        
        // ===== 阶段 2.5：清理输入块（可选） =====
        // 如果不需要重用输入块的内存，清空输入块
        // _reuse_mem 标志控制是否重用内存：
        // - true：保留输入块的内存，供后续使用（减少内存分配）
        // - false：清空输入块，释放内存（减少内存占用）
        if (!_reuse_mem) {
            in_block->clear();
        }
    }

    // ===== 阶段 3：处理数据流结束（EOS） =====
    // 当上游数据流结束时（eos = true），需要：
    // 1. 准备排序器供下游读取（完成排序，准备输出）
    // 2. 通知依赖关系，让下游 Pipeline 可以开始读取数据
    if (eos) {
        // 准备排序器供读取
        // prepare_for_read(false) 表示不使用 spill（全内存排序）
        // 这会完成最后的排序工作，准备输出数据
        // 例如：
        // - HeapSorter：完成堆排序，准备输出 TopN 结果
        // - TopNSorter：完成 TopN 排序，准备输出结果
        // - FullSorter：完成全排序，准备输出结果
        RETURN_IF_ERROR(local_state._shared_state->sorter->prepare_for_read(false));
        // 设置依赖关系为"可读"状态
        // 这会通知下游 Pipeline（包含 SortSourceOperatorX）可以开始读取排序结果
        // 依赖关系确保下游 Pipeline 在排序完成之前不会开始执行
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t SortSinkOperatorX::get_reserve_mem_size_for_next_sink(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->get_reserve_mem_size(state, eos);
}

size_t SortSinkOperatorX::get_revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->data_size();
}

Status SortSinkOperatorX::prepare_for_spill(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->prepare_for_read(true);
}

Status SortSinkOperatorX::merge_sort_read_for_spill(RuntimeState* state,
                                                    doris::vectorized::Block* block, int batch_size,
                                                    bool* eos) {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->merge_sort_read_for_spill(state, block, batch_size,
                                                                        eos);
}
void SortSinkOperatorX::reset(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    local_state._shared_state->sorter->reset();
}
#include "common/compile_check_end.h"
} // namespace doris::pipeline
