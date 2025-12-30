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

#include "partition_sort_sink_operator.h"

#include <glog/logging.h>

#include <cstdint>

#include "common/status.h"
#include "partition_sort_source_operator.h"
#include "vec/common/hash_table/hash.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status PartitionSortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<PartitionSortNodeSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<PartitionSortSinkOperatorX>();
    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    _partition_expr_ctxs.resize(p._partition_expr_ctxs.size());
    _partition_columns.resize(p._partition_expr_ctxs.size());
    for (size_t i = 0; i < p._partition_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._partition_expr_ctxs[i]->clone(state, _partition_expr_ctxs[i]));
    }
    _topn_phase = p._topn_phase;
    _partition_exprs_num = p._partition_exprs_num;
    _hash_table_size_counter = ADD_COUNTER(custom_profile(), "HashTableSize", TUnit::UNIT);
    _serialize_key_arena_memory_usage = custom_profile()->AddHighWaterMarkCounter(
            "MemoryUsageSerializeKeyArena", TUnit::BYTES, "", 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _build_timer = ADD_TIMER(custom_profile(), "HashTableBuildTime");
    _selector_block_timer = ADD_TIMER(custom_profile(), "SelectorBlockTime");
    _emplace_key_timer = ADD_TIMER(custom_profile(), "EmplaceKeyTime");
    _sorted_data_timer = ADD_TIMER(custom_profile(), "SortedDataTime");
    _passthrough_rows_counter =
            ADD_COUNTER(custom_profile(), "PassThroughRowsCounter", TUnit::UNIT);
    _sorted_partition_input_rows_counter =
            ADD_COUNTER(custom_profile(), "SortedPartitionInputRows", TUnit::UNIT);
    _partition_sort_info = std::make_shared<PartitionSortInfo>(
            &_vsort_exec_exprs, p._limit, 0, p._pool, p._is_asc_order, p._nulls_first,
            p._child->row_desc(), state, custom_profile(), p._has_global_limit,
            p._partition_inner_limit, p._top_n_algorithm, p._topn_phase);
    custom_profile()->add_info_string("PartitionTopNPhase", to_string(p._topn_phase));
    custom_profile()->add_info_string("PartitionTopNLimit",
                                      std::to_string(p._partition_inner_limit));
    RETURN_IF_ERROR(_init_hash_method());
    return Status::OK();
}

PartitionSortSinkOperatorX::PartitionSortSinkOperatorX(ObjectPool* pool, int operator_id,
                                                       int dest_id, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id, dest_id),
          _pool(pool),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _limit(tnode.limit),
          _partition_exprs_num(static_cast<int>(tnode.partition_sort_node.partition_exprs.size())),
          _topn_phase(tnode.partition_sort_node.ptopn_phase),
          _has_global_limit(tnode.partition_sort_node.has_global_limit),
          _top_n_algorithm(tnode.partition_sort_node.top_n_algorithm),
          _partition_inner_limit(tnode.partition_sort_node.partition_inner_limit),
          _distribute_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                                : std::vector<TExpr> {}) {}

Status PartitionSortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));

    //order by key
    if (tnode.partition_sort_node.__isset.sort_info) {
        RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.partition_sort_node.sort_info, _pool));
        _is_asc_order = tnode.partition_sort_node.sort_info.is_asc_order;
        _nulls_first = tnode.partition_sort_node.sort_info.nulls_first;
    }
    //partition by key
    if (tnode.partition_sort_node.__isset.partition_exprs) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.partition_sort_node.partition_exprs, _partition_expr_ctxs));
    }

    return Status::OK();
}

Status PartitionSortSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<PartitionSortSinkLocalState>::prepare(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _child->row_desc(), _row_descriptor));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_partition_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_expr_ctxs, state));
    return Status::OK();
}

Status PartitionSortSinkOperatorX::sink(RuntimeState* state, vectorized::Block* input_block,
                                        bool eos) {
    // 获取当前线程对应的本地状态（包含分区哈希表、PartitionBlocks、profile 等）
    auto& local_state = get_local_state(state);
    auto current_rows = input_block->rows();
    SCOPED_TIMER(local_state.exec_time_counter());
    if (current_rows > 0) {
        // 统计本次 sink 进入的行数
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
        // 情况一：没有分区键，所有行都属于一个“默认分区”
        if (UNLIKELY(_partition_exprs_num == 0)) {
            if (UNLIKELY(local_state._value_places.empty())) {
                // 为默认分区创建唯一一个 PartitionBlocks，用于收集所有行
                local_state._value_places.push_back(_pool->add(new PartitionBlocks(
                        local_state._partition_sort_info, local_state._value_places.empty())));
            }
            // 整块追加：直接把当前 Block 的全部数据移动到默认分区中存放
            local_state._value_places[0]->append_whole_block(input_block, _child->row_desc());
        } else {
            // 情况二：存在分区键，需要按 partition_exprs 对行进行划分
            if (local_state._is_need_passthrough) {
                // 已经判断需要“旁路”（passthrough）：不再往分区哈希表中写入，而是直接透传给 Source
                {
                    COUNTER_UPDATE(local_state._passthrough_rows_counter, (int64_t)current_rows);
                    std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
                    // 将整个 Block 放入 shared_state 的 blocks_buffer 队列中
                    local_state._shared_state->blocks_buffer.push(std::move(*input_block));
                    // 队列中已有数据，通知 Source 侧可以读取
                    local_state._dependency->set_ready_to_read();
                }
            } else {
                // 正常分区 TopN 路径：把 Block 拆分到不同 PartitionBlocks
                RETURN_IF_ERROR(_split_block_by_partition(input_block, local_state, eos));
                RETURN_IF_CANCELLED(state);
                // input_block 的数据已经全部转移到各个 PartitionBlocks，可清空以复用内存
                input_block->clear_column_data();
            }
        }
    }

    if (eos) {
        // 上游 child 已经 eos：不会再有新的行进来，可以释放分区哈希表相关内存
        local_state._agg_arena_pool.reset(nullptr);
        local_state._partitioned_data.reset(nullptr);
        SCOPED_TIMER(local_state._sorted_data_timer);
        // 第一轮遍历：为每个 PartitionBlocks 创建/重置对应的 PartitionSorter，
        // 并将其移动到 shared_state->partition_sorts 中
        for (auto& _value_place : local_state._value_places) {
            _value_place->create_or_reset_sorter_state();
            local_state._shared_state->partition_sorts.emplace_back(
                    std::move(_value_place->_partition_topn_sorter));
        }
        // 注意：需要分两轮循环：
        //  1）先把所有 PartitionSorter 对象创建好，放入 partition_sorts；
        //  2）再依次往每个 sorter 里 append block 并 prepare_for_read，
        //     这样 Source 侧在有些场景下可以更早地检测到 sorter 是否存在。
        for (int i = 0; i < local_state._value_places.size(); ++i) {
            auto& sorter = local_state._shared_state->partition_sorts[i];
            // 将该分区累积的所有 Block 追加到对应的 PartitionSorter 中
            for (const auto& block : local_state._value_places[i]->_blocks) {
                RETURN_IF_ERROR(sorter->append_block(block.get()));
            }
            // 原始 Block 已经全部交给 sorter，清空缓存
            local_state._value_places[i]->_blocks.clear();
            // 准备读取（构建 merge 队列等）
            RETURN_IF_ERROR(sorter->prepare_for_read(false));
            INJECT_MOCK_SLEEP(std::unique_lock<std::mutex> lc(
                    local_state._shared_state->prepared_finish_lock));
            // 标记该分区的 sorter 已准备完毕，Source 可安全读取
            sorter->set_prepared_finish();
            // 只要有一个 sorter 就绪，就可以唤醒 Source 尝试读取
            local_state._dependency->set_ready_to_read();
        }

        // 更新 profile 中的 hash 表大小和参与排序的总输入行数
        COUNTER_SET(local_state._hash_table_size_counter, int64_t(local_state._num_partition));
        COUNTER_SET(local_state._sorted_partition_input_rows_counter,
                    local_state._sorted_partition_input_rows);
        // 至此，来自 child 的所有数据已经全部写入并完成分区排序准备
        {
            std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
            // 标记 Sink 侧整体 eos（包括构建 sorter 的阶段）
            local_state._shared_state->sink_eos = true;
            // 再次设置 ready，确保 Source 若因自身逻辑 block 住，也能被唤醒检查 eos
            local_state._dependency->set_ready_to_read();
        }
        // 记录是否发生过 passthrough 行为，便于调试和分析
        local_state.custom_profile()->add_info_string(
                "HasPassThrough", local_state._is_need_passthrough ? "Yes" : "No");
    }

    return Status::OK();
}

/**
 * 按窗口 partition key（PARTITION BY 表达式）将输入 Block 的行分组
 * 
 * 这个函数的核心作用是：
 * 1. 对输入 Block 的每一行，计算窗口 partition key（例如 PARTITION BY city，则 key = city 列的值）
 * 2. 根据 partition key 将行 hash 到不同的 PartitionBlocks（每个窗口分区对应一个 PartitionBlocks）
 * 
 * @param input_block 输入的 Block，包含待分区排序的原始数据
 * @param local_state 本地状态，包含分区表达式上下文、分区列指针数组、哈希表等
 * @param eos 是否到达输入流的末尾
 */
Status PartitionSortSinkOperatorX::_split_block_by_partition(
        vectorized::Block* input_block, PartitionSortSinkLocalState& local_state, bool eos) {
    // 步骤 1：对每个 partition_expr（PARTITION BY 的表达式，例如 city 列），执行表达式计算
    // 例如：如果 SQL 是 PARTITION BY city，则 _partition_exprs_num = 1，_partition_expr_ctxs[0] 就是 city 列的表达式上下文
    for (int i = 0; i < _partition_exprs_num; ++i) {
        int result_column_id = -1;
        // 执行表达式，得到分区列在 input_block 中的列索引
        // 例如：如果 partition_expr 是 city 列，执行后 result_column_id 就是 city 列在 input_block 中的位置
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(input_block, &result_column_id));
        DCHECK(result_column_id != -1);
        // 将分区列的指针保存到 local_state._partition_columns[i]
        // 这样后续可以直接用这些列指针来计算 hash key，而不需要每次都重新执行表达式
        local_state._partition_columns[i] =
                input_block->get_by_position(result_column_id).column.get();
    }
    // 步骤 2：根据分区列，将 input_block 中的每一行 hash 到对应的 PartitionBlocks
    // _emplace_into_hash_table 会：
    //   - 根据 _partition_columns 计算每一行的 partition key（hash 值）
    //   - 在 _partitioned_data（哈希表）中查找或创建对应的 PartitionBlocks
    //   - 将行号记录到对应 PartitionBlocks 的 _selector 中
    //   - 后续通过 PartitionBlocks::append_block_by_selector 把行数据复制到对应分区的 _blocks 中
    RETURN_IF_ERROR(_emplace_into_hash_table(local_state._partition_columns, input_block,
                                             local_state, eos));
    return Status::OK();
}

size_t PartitionSortSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    auto rows = state->batch_size();
    size_t reserve_mem_size = std::visit(
            vectorized::Overload {[&](std::monostate&) -> size_t { return 0; },
                                  [&](auto& agg_method) -> size_t {
                                      return agg_method.hash_table->estimate_memory(rows);
                                  }},
            local_state._partitioned_data->method_variant);
    reserve_mem_size += rows * sizeof(size_t); // hash values
    return reserve_mem_size;
}

/**
 * 将输入 Block 的每一行根据 partition key hash 到对应的 PartitionBlocks
 * 
 * 核心流程：
 * 1. 对每一行计算 partition key（hash 值）
 * 2. 在哈希表中查找或创建对应的 PartitionBlocks（每个窗口分区一个）
 * 3. 将行号记录到对应 PartitionBlocks 的 _selector 中
 * 4. 最后通过 append_block_by_selector 把行数据复制到对应分区的 _blocks 中
 * 
 * @param key_columns 分区列指针数组（例如 [city 列指针]），用于计算 partition key
 * @param input_block 输入的 Block，包含待分区排序的原始数据
 * @param local_state 本地状态，包含哈希表、PartitionBlocks 列表等
 * @param eos 是否到达输入流的末尾
 */
Status PartitionSortSinkOperatorX::_emplace_into_hash_table(
        const vectorized::ColumnRawPtrs& key_columns, vectorized::Block* input_block,
        PartitionSortSinkLocalState& local_state, bool eos) {
    // 使用 std::visit 根据哈希方法类型（method_variant）分发到对应的处理逻辑
    // method_variant 可能是不同的哈希表实现（例如 UInt64 key、String key、Fixed key 等）
    return std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> Status {
                        // 哈希表未初始化，返回错误
                        return Status::InternalError("Unit hash table");
                    },
                    [&](auto& agg_method) -> Status {
                        SCOPED_TIMER(local_state._build_timer);
                        using HashMethodType = std::decay_t<decltype(agg_method)>;
                        using AggState = typename HashMethodType::State;

                        // 步骤 1：初始化哈希状态和序列化 key
                        // AggState 用于在计算 hash 时缓存中间状态，提高性能
                        AggState state(key_columns);
                        uint32_t num_rows = (uint32_t)input_block->rows();
                        // 预序列化所有行的 partition key，避免在循环中重复计算
                        agg_method.init_serialized_keys(key_columns, num_rows);

                        // 步骤 2：定义 creator lambda，当哈希表中遇到新的 partition key 时调用
                        // 作用：为新的窗口分区创建一个 PartitionBlocks 对象
                        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                            // 持久化 key（如果是字符串等复杂类型，需要拷贝到 arena 中）
                            HashMethodType::try_presis_key(key, origin,
                                                           *local_state._agg_arena_pool);
                            // 创建新的 PartitionBlocks，表示一个新的窗口分区
                            // 例如：如果 partition key 是 "Beijing"，则创建一个 PartitionBlocks 来存储所有 city="Beijing" 的行
                            auto* aggregate_data = _pool->add(
                                    new PartitionBlocks(local_state._partition_sort_info,
                                                        local_state._value_places.empty()));
                            local_state._value_places.push_back(aggregate_data);
                            // 将 key 和 PartitionBlocks 的映射关系插入哈希表
                            ctor(key, aggregate_data);
                            local_state._num_partition++;
                        };
                        // 定义 creator_for_null_key lambda，处理 partition key 为 NULL 的情况
                        auto creator_for_null_key = [&](auto& mapped) {
                            mapped = _pool->add(
                                    new PartitionBlocks(local_state._partition_sort_info,
                                                        local_state._value_places.empty()));
                            local_state._value_places.push_back(mapped);
                            local_state._num_partition++;
                        };

                        // 步骤 3：遍历 input_block 的每一行，计算 partition key 并 hash 到对应的 PartitionBlocks
                        SCOPED_TIMER(local_state._emplace_key_timer);
                        int64_t row = num_rows;
                        // 从后往前遍历（row = num_rows - 1 到 0），这样可以在 passthrough 时方便截断
                        for (row = row - 1; row >= 0 && !local_state._is_need_passthrough; --row) {
                            // lazy_emplace：如果 partition key 已存在，返回对应的 PartitionBlocks*；
                            //             如果不存在，调用 creator/creator_for_null_key 创建新的 PartitionBlocks
                            auto& mapped = *agg_method.lazy_emplace(state, row, creator,
                                                                    creator_for_null_key);
                            // 将当前行的行号（row）记录到对应 PartitionBlocks 的 _selector 中
                            // 例如：如果第 5 行的 city="Beijing"，则 mapped（Beijing 的 PartitionBlocks）的 _selector 会 push_back(5)
                            mapped->add_row_idx(row);
                            local_state._sorted_partition_input_rows++;
                            // 检查是否需要 passthrough（分区数太多时，直接透传剩余数据，不做分区排序）
                            local_state._is_need_passthrough =
                                    local_state.check_whether_need_passthrough();
                        }
                        // 步骤 4：对每个 PartitionBlocks，根据 _selector 从 input_block 中选择对应的行，复制到 _blocks 中
                        // 例如：Beijing 的 PartitionBlocks 的 _selector = [5, 12, 23]，则会把 input_block 的第 5、12、23 行复制到 Beijing 的 _blocks 中
                        for (auto* place : local_state._value_places) {
                            SCOPED_TIMER(local_state._selector_block_timer);
                            RETURN_IF_ERROR(place->append_block_by_selector(input_block, eos));
                        }
                        // 步骤 5：如果检测到需要 passthrough（分区数超过阈值），将剩余行 [0, row] 直接放到 blocks_buffer
                        // 这样 Source 可以直接读取这些数据，跳过分区排序逻辑
                        //Perform passthrough for the range [0, row] of input_block
                        if (local_state._is_need_passthrough && row >= 0) {
                            {
                                COUNTER_UPDATE(local_state._passthrough_rows_counter,
                                               (int64_t)(row + 1));
                                std::lock_guard<std::mutex> lock(
                                        local_state._shared_state->buffer_mutex);
                                // have emplace (num_rows - row) to hashtable, and now have row remaining needed in block;
                                // set_num_rows(x) retains the range [0, x - 1], so row + 1 is needed here.
                                // 截断 input_block，只保留 [0, row] 范围的行（row + 1 行）
                                input_block->set_num_rows(row + 1);
                                // 将剩余行直接放到 shared_state 的 blocks_buffer 中，供 Source 读取
                                local_state._shared_state->blocks_buffer.push(
                                        std::move(*input_block));
                                // buffer have data, source could read this.
                                // 唤醒 Source，告诉它有数据可以读取了
                                local_state._dependency->set_ready_to_read();
                            }
                        }
                        // 步骤 6：更新 profile 计数器，记录内存使用情况
                        local_state._serialize_key_arena_memory_usage->set(
                                (int64_t)local_state._agg_arena_pool->size());
                        COUNTER_SET(local_state._hash_table_memory_usage,
                                    (int64_t)agg_method.hash_table->get_buffer_size_in_bytes());
                        return Status::OK();
                    }},
            local_state._partitioned_data->method_variant);
}

constexpr auto init_partition_hash_method = init_hash_method<PartitionedHashMapVariants>;

Status PartitionSortSinkLocalState::_init_hash_method() {
    RETURN_IF_ERROR(init_partition_hash_method(_partitioned_data.get(),
                                               get_data_types(_partition_expr_ctxs), true));
    return Status::OK();
}

// NOLINTBEGIN(readability-simplify-boolean-expr)
// just simply use partition num to check
// but if is TWO_PHASE_GLOBAL, must be sort all data thought partition num threshold have been exceeded.
// partition_topn_max_partitions     default is : 1024
// partition_topn_per_partition_rows default is : 1000
bool PartitionSortSinkLocalState::check_whether_need_passthrough() {
    if (_topn_phase != TPartTopNPhase::TWO_PHASE_GLOBAL &&
        _num_partition > _state->partition_topn_max_partitions() &&
        _sorted_partition_input_rows <
                _state->partition_topn_per_partition_rows() * _num_partition) {
        return true;
    }
    return false;
}
// NOLINTEND(readability-simplify-boolean-expr)

#include "common/compile_check_end.h"
} // namespace doris::pipeline
