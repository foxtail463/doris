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

#include "hashjoin_build_sink.h"

#include <cstdlib>
#include <string>
#include <variant>

#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
HashJoinBuildSinkLocalState::HashJoinBuildSinkLocalState(DataSinkOperatorXBase* parent,
                                                         RuntimeState* state)
        : JoinBuildSinkLocalState(parent, state) {
    _finish_dependency = std::make_shared<CountedFinishDependency>(
            parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY");
}

Status HashJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(JoinBuildSinkLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _task_idx = info.task_idx;
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    _shared_state->join_op_variants = p._join_op_variants;

    _build_expr_ctxs.resize(p._build_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._build_expr_ctxs[i]->clone(state, _build_expr_ctxs[i]));
    }
    _shared_state->build_exprs_size = _build_expr_ctxs.size();

    _should_build_hash_table = true;
    custom_profile()->add_info_string("BroadcastJoin", std::to_string(p._is_broadcast_join));
    if (p._use_shared_hash_table) {
        _should_build_hash_table = info.task_idx == 0;
    }
    custom_profile()->add_info_string("BuildShareHashTable",
                                      std::to_string(_should_build_hash_table));
    custom_profile()->add_info_string("ShareHashTableEnabled",
                                      std::to_string(p._use_shared_hash_table));
    if (!_should_build_hash_table) {
        _dependency->block();
        _finish_dependency->block();
        {
            std::lock_guard<std::mutex> guard(p._mutex);
            p._finish_dependencies.push_back(_finish_dependency);
        }
    } else {
        _dependency->set_ready();
    }

    _build_blocks_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageBuildBlocks", TUnit::BYTES, 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _build_arena_memory_usage =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageBuildKeyArena", TUnit::BYTES, 1);

    // Build phase
    auto* record_profile = _should_build_hash_table ? custom_profile() : faker_runtime_profile();
    _build_table_timer = ADD_TIMER(custom_profile(), "BuildHashTableTime");
    _build_side_merge_block_timer = ADD_TIMER(custom_profile(), "MergeBuildBlockTime");
    _build_table_insert_timer = ADD_TIMER(record_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(record_profile, "BuildExprCallTime");

    _runtime_filter_producer_helper = std::make_shared<RuntimeFilterProducerHelper>(
            _should_build_hash_table, p._is_broadcast_join);
    RETURN_IF_ERROR(_runtime_filter_producer_helper->init(state, _build_expr_ctxs,
                                                          p._runtime_filter_descs));
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinBuildSinkLocalState::open(state));
    return Status::OK();
}

Status HashJoinBuildSinkLocalState::terminate(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    if (_terminated) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_runtime_filter_producer_helper->skip_process(state));
    return JoinBuildSinkLocalState::terminate(state);
}

size_t HashJoinBuildSinkLocalState::get_reserve_mem_size(RuntimeState* state, bool eos) {
    if (!_should_build_hash_table) {
        return 0;
    }

    if (_shared_state->build_block) {
        return 0;
    }

    size_t size_to_reserve = 0;

    const size_t build_block_rows = _build_side_mutable_block.rows();
    if (build_block_rows != 0) {
        const auto bytes = _build_side_mutable_block.bytes();
        const auto allocated_bytes = _build_side_mutable_block.allocated_bytes();
        const auto bytes_per_row = bytes / build_block_rows;
        const auto estimated_size_of_next_block = bytes_per_row * state->batch_size();
        // If the new size is greater than 85% of allocalted bytes, it maybe need to realloc.
        if (((estimated_size_of_next_block + bytes) * 100 / allocated_bytes) >= 85) {
            size_to_reserve += static_cast<size_t>(static_cast<double>(allocated_bytes) * 1.15);
        }
    }

    if (eos) {
        const size_t rows = build_block_rows + state->batch_size();
        const auto bucket_size = hash_join_table_calc_bucket_size(rows);

        size_to_reserve += bucket_size * sizeof(uint32_t); // JoinHashTable::first
        size_to_reserve += rows * sizeof(uint32_t);        // JoinHashTable::next

        auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
        if (p._join_op == TJoinOp::FULL_OUTER_JOIN || p._join_op == TJoinOp::RIGHT_OUTER_JOIN ||
            p._join_op == TJoinOp::RIGHT_ANTI_JOIN || p._join_op == TJoinOp::RIGHT_SEMI_JOIN) {
            size_to_reserve += rows * sizeof(uint8_t); // JoinHashTable::visited
        }
        size_to_reserve += _evaluate_mem_usage;

        vectorized::ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

        if (build_block_rows > 0) {
            auto block = _build_side_mutable_block.to_block();
            std::vector<uint16_t> converted_columns;
            Defer defer([&]() {
                for (auto i : converted_columns) {
                    auto& data = block.get_by_position(i);
                    data.column = vectorized::remove_nullable(data.column);
                    data.type = vectorized::remove_nullable(data.type);
                }
                _build_side_mutable_block = vectorized::MutableBlock(std::move(block));
            });
            vectorized::ColumnUInt8::MutablePtr null_map_val;
            if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN) {
                converted_columns = _convert_block_to_null(block);
                // first row is mocked
                for (int i = 0; i < block.columns(); i++) {
                    auto [column, is_const] = unpack_if_const(block.safe_get_by_position(i).column);
                    assert_cast<vectorized::ColumnNullable*>(column->assume_mutable().get())
                            ->get_null_map_column()
                            .get_data()
                            .data()[0] = 1;
                }
            }

            null_map_val = vectorized::ColumnUInt8::create();
            null_map_val->get_data().assign(build_block_rows, (uint8_t)0);

            // Get the key column that needs to be built
            Status st = _extract_join_column(block, null_map_val, raw_ptrs, _build_col_ids);
            if (!st.ok()) {
                throw Exception(st);
            }

            std::visit(vectorized::Overload {[&](std::monostate& arg) {},
                                             [&](auto&& hash_map_context) {
                                                 size_to_reserve += hash_map_context.estimated_size(
                                                         raw_ptrs, (uint32_t)block.rows(), true,
                                                         true, bucket_size);
                                             }},
                       _shared_state->hash_table_variant_vector.front()->method_variant);
        }
    }
    return size_to_reserve;
}

Status HashJoinBuildSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    Defer defer {[&]() {
        if (!_should_build_hash_table) {
            return;
        }
        // The build side hash key column maybe no need output, but we need to keep the column in block
        // because it is used to compare with probe side hash key column

        if (p._should_keep_hash_key_column && _build_col_ids.size() == 1) {
            // when key column from build side tuple, we should keep it
            // if key column belong to intermediate tuple, it means _build_col_ids[0] >= _should_keep_column_flags.size(),
            // key column still kept too.
            if (_build_col_ids[0] < p._should_keep_column_flags.size()) {
                p._should_keep_column_flags[_build_col_ids[0]] = true;
            }
        }

        if (_shared_state->build_block) {
            // release the memory of unused column in probe stage
            _shared_state->build_block->clear_column_mem_not_keep(p._should_keep_column_flags,
                                                                  p._use_shared_hash_table);
        }

        if (p._use_shared_hash_table) {
            std::unique_lock lock(p._mutex);
            p._signaled = true;
            for (auto& dep : _shared_state->sink_deps) {
                dep->set_ready();
            }
            for (auto& dep : p._finish_dependencies) {
                dep->set_ready();
            }
        }
    }};

    try {
        if (!_terminated && _runtime_filter_producer_helper && !state->is_cancelled()) {
            RETURN_IF_ERROR(_runtime_filter_producer_helper->build(
                    state, _shared_state->build_block.get(), p._use_shared_hash_table,
                    p._runtime_filters));
            RETURN_IF_ERROR(_runtime_filter_producer_helper->publish(state));
        }
    } catch (Exception& e) {
        bool blocked_by_shared_hash_table_signal =
                !_should_build_hash_table && p._use_shared_hash_table && !p._signaled;

        return Status::InternalError(
                "rf process meet error: {}, _terminated: {}, should_build_hash_table: "
                "{}, _finish_dependency: {}, "
                "blocked_by_shared_hash_table_signal: "
                "{}",
                e.to_string(), _terminated, _should_build_hash_table,
                _finish_dependency ? _finish_dependency->debug_string() : "null",
                blocked_by_shared_hash_table_signal);
    }
    if (_runtime_filter_producer_helper) {
        _runtime_filter_producer_helper->collect_realtime_profile(custom_profile());
    }
    return Base::close(state, exec_status);
}

bool HashJoinBuildSinkLocalState::build_unique() const {
    return _parent->cast<HashJoinBuildSinkOperatorX>()._build_unique;
}

void HashJoinBuildSinkLocalState::init_short_circuit_for_probe() {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    bool empty_block =
            !_shared_state->build_block ||
            !(_shared_state->build_block->rows() > 1); // build size always mock a row into block
    _shared_state->short_circuit_for_probe =
            ((_shared_state->_has_null_in_build_side &&
              p._join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) ||
             (empty_block &&
              (p._join_op == TJoinOp::INNER_JOIN || p._join_op == TJoinOp::LEFT_SEMI_JOIN ||
               p._join_op == TJoinOp::RIGHT_OUTER_JOIN || p._join_op == TJoinOp::RIGHT_SEMI_JOIN ||
               p._join_op == TJoinOp::RIGHT_ANTI_JOIN))) &&
            !p._is_mark_join;

    //when build table rows is 0 and not have other_join_conjunct and not _is_mark_join and join type is one of LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
    //we could get the result is probe table + null-column(if need output)
    _shared_state->empty_right_table_need_probe_dispose =
            (empty_block && !p._have_other_join_conjunct && !p._is_mark_join) &&
            (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN ||
             p._join_op == TJoinOp::LEFT_ANTI_JOIN);
}

Status HashJoinBuildSinkLocalState::_do_evaluate(vectorized::Block& block,
                                                 vectorized::VExprContextSPtrs& exprs,
                                                 RuntimeProfile::Counter& expr_call_timer,
                                                 std::vector<int>& res_col_ids) {
    auto origin_size = block.allocated_bytes();
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        res_col_ids[i] = result_col_id;
    }

    _evaluate_mem_usage = block.allocated_bytes() - origin_size;
    return Status::OK();
}

std::vector<uint16_t> HashJoinBuildSinkLocalState::_convert_block_to_null(
        vectorized::Block& block) {
    std::vector<uint16_t> results;
    for (int i = 0; i < block.columns(); ++i) {
        if (auto& column_type = block.safe_get_by_position(i); !column_type.type->is_nullable()) {
            DCHECK(!column_type.column->is_nullable());
            column_type.column = make_nullable(column_type.column);
            column_type.type = make_nullable(column_type.type);
            results.emplace_back(i);
        }
    }
    return results;
}

/**
 * 从 Block 中提取 Join 键列
 * 
 * 【功能说明】
 * 从 Build Block 中提取 Join 键对应的列，并根据 NULL 值处理策略进行转换。
 * 主要处理两种 NULL 值表示方式：
 * 1. 序列化 NULL 到键中（serialize_null_into_key = true）
 * 2. 使用外部 NULL Map 表示 NULL（serialize_null_into_key = false）
 * 
 * 【NULL 值处理策略】
 * - serialize_null_into_key = true：
 *   - 用于多列 NULL Safe Equal Join（<=>）
 *   - NULL 值被序列化到键中，Hash 表可以直接比较 NULL
 *   - 例如：多列键 (a, b)，如果 a=NULL, b=1，NULL 会被序列化到键中
 * 
 * - serialize_null_into_key = false：
 *   - 用于单列键或普通 Join（=）
 *   - NULL 值通过外部 NULL Map 表示，键中不包含 NULL
 *   - 例如：单列键 a，如果 a=NULL，通过 null_map[i]=1 表示，键中存储默认值
 * 
 * 【三种处理情况】
 * 1. 列不是 Nullable，但需要序列化 NULL 到键中：
 *    - 转换为 Nullable 列（为 NULL Safe Equal Join 做准备）
 * 
 * 2. 列是 Nullable，但不序列化 NULL 到键中：
 *    - 提取嵌套列（去掉 Nullable 包装）
 *    - 更新外部 NULL Map（合并列的 NULL Map）
 * 
 * 3. 其他情况：
 *    - 直接使用原列（不需要转换）
 * 
 * 【参数说明】
 * @param block Build 数据块
 * @param null_map 外部 NULL Map（用于情况 2，如果 serialize_null_into_key = false）
 * @param raw_ptrs 输出参数：存储提取的 Join 键列指针
 * @param res_col_ids Join 键列在 Block 中的列索引
 * @return Status
 */
Status HashJoinBuildSinkLocalState::_extract_join_column(
        vectorized::Block& block, vectorized::ColumnUInt8::MutablePtr& null_map,
        vectorized::ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids) {
    DCHECK(_should_build_hash_table);
    auto& shared_state = *_shared_state;
    
    // ===== 遍历所有 Join 键列 =====
    for (size_t i = 0; i < shared_state.build_exprs_size; ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        
        // ===== 情况 1：列不是 Nullable，但需要序列化 NULL 到键中 =====
        // 场景：多列 NULL Safe Equal Join（<=>）
        // 原因：即使列本身不是 Nullable，但在多列 NULL Safe Equal Join 中，
        //       需要将 NULL 值序列化到键中，以便 Hash 表可以直接比较 NULL
        // 处理：将列转换为 Nullable 类型
        if (!column->is_nullable() &&
            _parent->cast<HashJoinBuildSinkOperatorX>()._serialize_null_into_key[i]) {
            // 创建 Nullable 版本的列，存储到 _key_columns_holder 中（保持生命周期）
            _key_columns_holder.emplace_back(
                    vectorized::make_nullable(block.get_by_position(res_col_ids[i]).column));
            // 使用 Nullable 版本的列
            raw_ptrs[i] = _key_columns_holder.back().get();
        } 
        // ===== 情况 2：列是 Nullable，但不序列化 NULL 到键中 =====
        // 场景：单列键或普通 Join（=）
        // 原因：单列键必须使用 NULL Map 表示 NULL（性能优化）
        //       普通 Join 中 NULL != NULL，可以直接用 NULL Map 表示
        // 处理：提取嵌套列（去掉 Nullable 包装），更新外部 NULL Map
        else if (const auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column);
                   !_parent->cast<HashJoinBuildSinkOperatorX>()._serialize_null_into_key[i] &&
                   nullable) {
            // update nulllmap and split nested out of ColumnNullable when serialize_null_into_key is false and column is nullable
            // 提取嵌套列（去掉 Nullable 包装）
            const auto& col_nested = nullable->get_nested_column();
            // 获取列的 NULL Map
            const auto& col_nullmap = nullable->get_null_map_data();
            // 确保外部 NULL Map 已创建
            DCHECK(null_map);
            // 更新外部 NULL Map：合并列的 NULL Map（使用 OR 操作）
            // 这样，如果任何一列是 NULL，外部 NULL Map 对应位置就是 1
            vectorized::VectorizedUtils::update_null_map(null_map->get_data(), col_nullmap);
            // 使用嵌套列（不包含 NULL Map）
            raw_ptrs[i] = &col_nested;
        } 
        // ===== 情况 3：其他情况（直接使用原列） =====
        // 场景：
        // - 列不是 Nullable，且不序列化 NULL 到键中
        // - 列是 Nullable，且序列化 NULL 到键中（保持 Nullable 类型）
        else {
            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

/**
 * 处理 Build 数据块并构建 Hash 表
 * 
 * 【功能说明】
 * 这是 Hash Join Build 阶段的核心方法，负责：
 * 1. 预处理 Build 数据块（处理溢出、finalize variant column）
 * 2. 对于 OUTER JOIN，转换 Block 为 Nullable（支持未匹配行的 NULL 填充）
 * 3. 提取 Join 键列并初始化 Hash 表
 * 4. 调用 ProcessHashTableBuild 构建链式哈希表
 * 
 * 【工作流程】
 * 1. 预处理 Block：处理 ColumnString 溢出、finalize ColumnVariant
 * 2. OUTER JOIN 处理：转换 Block 为 Nullable，创建虚拟行（索引 0）
 * 3. NULL Map 处理：设置外部 NULL Map（用于某些 Join 类型）
 * 4. 提取 Join 键列：从 Block 中提取 Join 键对应的列
 * 5. 初始化 Hash 表：根据键类型选择 Hash 表变体
 * 6. 构建 Hash 表：调用 ProcessHashTableBuild 构建链式哈希表
 * 
 * 【关键设计】
 * - 虚拟行（索引 0）：用于 OUTER JOIN 的未匹配行处理
 * - Hash 表变体：根据键类型选择不同的 Hash 表实现（std::variant）
 * - 模板特化：使用 std::visit 根据 Join 类型和 Hash 表类型生成不同的实现
 * 
 * @param state 运行时状态
 * @param block Build 数据块
 * @return Status
 */
Status HashJoinBuildSinkLocalState::process_build_block(RuntimeState* state,
                                                        vectorized::Block& block) {
    // ===== 前置检查 =====
    // 确保当前 Task 负责构建 Hash 表（Broadcast Join 模式下只有 Task 0 构建）
    DCHECK(_should_build_hash_table);
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    SCOPED_TIMER(_build_table_timer);
    auto rows = (uint32_t)block.rows();
    
    // ===== 阶段 1：预处理 Block =====
    // 1. Dispose the overflow of ColumnString
    //    处理 ColumnString 的溢出（如果字符串太长，需要特殊处理）
    // 2. Finalize the ColumnVariant to speed up
    //    完成 ColumnVariant 的 finalize（将变体类型转换为具体类型，提高后续处理速度）
    for (auto& data : block) {
        data.column = std::move(*data.column).mutate()->convert_column_if_overflow();
        if (p._need_finalize_variant_column) {
            std::move(*data.column).mutate()->finalize();
        }
    }

    // ===== 阶段 2：准备 Join 键列和 NULL Map =====
    vectorized::ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());
    vectorized::ColumnUInt8::MutablePtr null_map_val;
    
    // ===== 阶段 3：OUTER JOIN 特殊处理 =====
    // 对于 LEFT OUTER JOIN 和 FULL OUTER JOIN，需要支持未匹配行的 NULL 填充
    // 因此需要将 Block 转换为 Nullable 类型
    if (p._join_op == TJoinOp::LEFT_OUTER_JOIN || p._join_op == TJoinOp::FULL_OUTER_JOIN) {
        // 将所有列转换为 Nullable 类型
        _convert_block_to_null(block);
        // first row is mocked
        // 创建虚拟行（索引 0），用于表示未匹配的行
        // 虚拟行的所有列都是 NULL（null_map[0] = 1）
        // 这样，在 Probe 阶段如果 Probe 行未匹配，可以使用虚拟行填充 Build 侧（全 NULL）
        for (int i = 0; i < block.columns(); i++) {
            auto [column, is_const] = unpack_if_const(block.safe_get_by_position(i).column);
            assert_cast<vectorized::ColumnNullable*>(column->assume_mutable().get())
                    ->get_null_map_column()
                    .get_data()
                    .data()[0] = 1;
        }
    }

    // ===== 阶段 4：设置外部 NULL Map =====
    // 检查是否需要外部 NULL Map（用于某些 Join 类型的 NULL 值处理）
    // 例如：NULL Aware Join、short_circuit_for_null_in_build_side 等场景
    _set_build_side_has_external_nullmap(block, _build_col_ids);
    if (_build_side_has_external_nullmap) {
        // 创建 NULL Map，初始化为全 0（表示没有 NULL 值）
        null_map_val = vectorized::ColumnUInt8::create();
        null_map_val->get_data().assign((size_t)rows, (uint8_t)0);
    }

    // ===== 阶段 5：提取 Join 键列 =====
    // Get the key column that needs to be built
    // 从 Block 中提取 Join 键对应的列，存储到 raw_ptrs 中
    // 同时处理 NULL 值的序列化（根据 _serialize_null_into_key 配置）
    RETURN_IF_ERROR(_extract_join_column(block, null_map_val, raw_ptrs, _build_col_ids));

    // ===== 阶段 6：初始化 Hash 表 =====
    // 根据 Join 键的类型，选择并初始化合适的 Hash 表变体
    // Hash 表变体包括：
    // - PrimaryTypeHashTableContext（整数类型）
    // - MethodOneString（字符串类型）
    // - SerializedHashTableContext（序列化类型）
    // - FixedKeyHashTableContext（固定长度键）
    RETURN_IF_ERROR(_hash_table_init(state, raw_ptrs));

    // ===== 阶段 7：构建 Hash 表 =====
    // 使用 std::visit 根据 Hash 表类型和 Join 类型，调用相应的构建方法
    // 这是一个多态分发机制，根据实际的类型组合生成不同的代码
    Status st = std::visit(
            vectorized::Overload {
                    // ===== 情况 1：Hash 表未初始化 =====
                    // 如果 Hash 表变体是 std::monostate（未初始化），抛出错误
                    [&](std::monostate& arg, auto join_op,
                        auto short_circuit_for_null_in_build_side,
                        auto with_other_conjuncts) -> Status {
                        throw Exception(Status::FatalError("FATAL: uninited hash table"));
                    },
                    // ===== 情况 2：正常构建 Hash 表 =====
                    [&](auto&& arg, auto&& join_op, auto short_circuit_for_null_in_build_side,
                        auto with_other_conjuncts) -> Status {
                        // 推导 Hash 表上下文类型和 Join 操作类型
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        using JoinOpType = std::decay_t<decltype(join_op)>;
                        
                        // 创建 Hash 表构建处理器
                        // ProcessHashTableBuild 负责实际的 Hash 表构建逻辑
                        ProcessHashTableBuild<HashTableCtxType> hash_table_build_process(
                                rows, raw_ptrs, this, state->batch_size(), state);
                        
                        // 调用构建方法
                        // 参数说明：
                        // - JoinOpType::value: Join 操作类型（INNER_JOIN、LEFT_OUTER_JOIN 等）
                        // - short_circuit_for_null_in_build_side: 是否对 Build 侧的 NULL 值短路
                        // - with_other_conjuncts: 是否有其他 Join 条件
                        // - arg: Hash 表上下文（包含 Hash 表实例和键值）
                        // - null_map_val: NULL Map（如果存在）
                        // - _has_null_in_build_side: 输出参数，标记 Build 侧是否有 NULL 值
                        auto st = hash_table_build_process.template run<
                                JoinOpType::value, short_circuit_for_null_in_build_side,
                                with_other_conjuncts>(
                                arg, null_map_val ? &null_map_val->get_data() : nullptr,
                                &_shared_state->_has_null_in_build_side);
                        
                        // ===== 更新内存使用统计 =====
                        // 计算总内存使用 = Build Block 内存 + Hash 表内存 + 序列化键内存
                        COUNTER_SET(_memory_used_counter,
                                    _build_blocks_memory_usage->value() +
                                            (int64_t)(arg.hash_table->get_byte_size() +
                                                      arg.serialized_keys_size(true)));
                        return st;
                    }},
            // ===== std::visit 的参数 =====
            // 1. Hash 表变体：根据键类型选择的具体 Hash 表实现
            _shared_state->hash_table_variant_vector.front()->method_variant,
            // 2. Join 操作变体：Join 操作类型（INNER_JOIN、LEFT_OUTER_JOIN 等）
            _shared_state->join_op_variants,
            // 3. short_circuit_for_null_in_build_side 变体：是否对 Build 侧的 NULL 值短路
            vectorized::make_bool_variant(p._short_circuit_for_null_in_build_side),
            // 4. with_other_conjuncts 变体：是否有其他 Join 条件
            vectorized::make_bool_variant((p._have_other_join_conjunct)));
    return st;
}

void HashJoinBuildSinkLocalState::_set_build_side_has_external_nullmap(
        vectorized::Block& block, const std::vector<int>& res_col_ids) {
    DCHECK(_should_build_hash_table);
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    if (p._short_circuit_for_null_in_build_side) {
        _build_side_has_external_nullmap = true;
        return;
    }
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (column->is_nullable() && !p._serialize_null_into_key[i]) {
            _build_side_has_external_nullmap = true;
            return;
        }
    }
}

/**
 * 初始化 Hash 表
 * 
 * 【功能说明】
 * 根据 Join 键的类型和 NULL 值处理策略，初始化合适的 Hash 表变体。
 * 主要步骤：
 * 1. 确定数据类型（Nullable 或非 Nullable）
 * 2. 选择要初始化的 Hash 表变体（根据 Broadcast Join 和共享 Hash 表配置）
 * 3. 初始化 Hash 表方法（根据键类型选择具体的 Hash 表实现）
 * 4. 尝试转换为 Direct Mapping（性能优化）
 * 
 * 【Hash 表类型选择】
 * 根据键类型，`init_hash_method` 会选择不同的 Hash 表实现：
 * - 整数类型（UInt8/16/32/64/128/256）→ PrimaryTypeHashTableContext
 * - 字符串类型 → MethodOneString 或 SerializedHashTableContext
 * - 固定长度键（64/72/96/104/128/136/256 bits）→ FixedKeyHashTableContext
 * 
 * 【Direct Mapping 优化】
 * 对于整数类型的键，如果键值范围小（max_key - min_key < MAX_MAPPING_RANGE），
 * 可以使用 Direct Mapping（直接映射），避免 Hash 计算，提高性能。
 * 
 * 【Broadcast Join 和共享 Hash 表】
 * - Broadcast Join + 共享 Hash 表：所有 Task 共享同一个 Hash 表（variant_ptrs 包含所有变体）
 * - 其他情况：每个 Task 使用自己的 Hash 表（variant_ptrs 只包含一个变体）
 * 
 * @param state 运行时状态
 * @param raw_ptrs Join 键列的原始指针（用于 Direct Mapping 优化）
 * @return Status
 */
Status HashJoinBuildSinkLocalState::_hash_table_init(RuntimeState* state,
                                                     const vectorized::ColumnRawPtrs& raw_ptrs) {
    auto& p = _parent->cast<HashJoinBuildSinkOperatorX>();
    std::vector<vectorized::DataTypePtr> data_types;
    
    // ===== 阶段 1：确定数据类型 =====
    // 根据 NULL 值处理策略，决定数据类型是 Nullable 还是非 Nullable
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        auto& ctx = _build_expr_ctxs[i];
        auto data_type = ctx->root()->data_type();

        /// For 'null safe equal' join,
        /// the build key column maybe be converted to nullable from non-nullable.
        // ===== 情况 1：序列化 NULL 到键中 =====
        // 用于多列 NULL Safe Equal Join（<=>）
        // 数据类型需要是 Nullable，以便 Hash 表可以直接比较 NULL
        if (p._serialize_null_into_key[i]) {
            data_types.emplace_back(vectorized::make_nullable(data_type));
        } 
        // ===== 情况 2：使用 NULL Map 表示 NULL =====
        // 用于单列键或普通 Join（=）
        // 数据类型不需要是 Nullable，NULL 值通过外部 NULL Map 表示
        else {
            // in this case, we use nullmap to represent null value
            data_types.emplace_back(vectorized::remove_nullable(data_type));
        }
    }
    
    // ===== 阶段 2：单列键优化 =====
    // 如果是单列键，标记需要保留 Hash 键列
    // 原因：单列键可以直接用于比较，不需要重新计算 Hash 值
    if (_build_expr_ctxs.size() == 1) {
        p._should_keep_hash_key_column = true;
    }

    // ===== 阶段 3：选择要初始化的 Hash 表变体 =====
    std::vector<std::shared_ptr<JoinDataVariants>> variant_ptrs;
    
    // ===== 情况 A：Broadcast Join + 共享 Hash 表 =====
    // 所有 Task 共享同一个 Hash 表
    // variant_ptrs 包含所有变体（每个 Task 一个槽位）
    if (p._is_broadcast_join && p._use_shared_hash_table) {
        variant_ptrs = _shared_state->hash_table_variant_vector;
    } 
    // ===== 情况 B：其他情况 =====
    // 每个 Task 使用自己的 Hash 表
    // variant_ptrs 只包含一个变体（当前 Task 的槽位）
    else {
        // 如果使用共享 Hash 表（但不是 Broadcast Join），使用 task_idx 对应的槽位
        // 否则使用索引 0 的槽位
        variant_ptrs.emplace_back(
                _shared_state->hash_table_variant_vector[p._use_shared_hash_table ? _task_idx : 0]);
    }

    // ===== 阶段 4：初始化 Hash 表方法 =====
    // 对每个 Hash 表变体，根据数据类型选择具体的 Hash 表实现
    // init_hash_method 会根据键类型（整数、字符串、固定长度等）选择：
    // - PrimaryTypeHashTableContext（整数类型）
    // - MethodOneString（字符串类型）
    // - SerializedHashTableContext（序列化类型）
    // - FixedKeyHashTableContext（固定长度键）
    for (auto& variant_ptr : variant_ptrs) {
        RETURN_IF_ERROR(init_hash_method<JoinDataVariants>(variant_ptr.get(), data_types, true));
    }
    
    // ===== 阶段 5：尝试转换为 Direct Mapping =====
    // Direct Mapping 优化：对于整数类型的键，如果键值范围小，
    // 可以使用直接映射（键值直接作为数组索引），避免 Hash 计算
    // 
    // 条件：max_key - min_key < MAX_MAPPING_RANGE - 1
    // 
    // 示例：
    // - 键值范围 [100, 200]：可以使用 Direct Mapping（范围小）
    // - 键值范围 [1, 1000000]：不能使用 Direct Mapping（范围太大）
    // 
    // 使用 std::visit 根据实际的 Hash 表类型，调用相应的转换函数
    // 只有 PrimaryTypeHashTableContext（整数类型）支持 Direct Mapping
    std::visit([&](auto&& arg) { try_convert_to_direct_mapping(&arg, raw_ptrs, variant_ptrs); },
               variant_ptrs[0]->method_variant);
    return Status::OK();
}

HashJoinBuildSinkOperatorX::HashJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                       int dest_id, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : JoinBuildSinkOperatorX(pool, operator_id, dest_id, tnode, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join),
          _partition_exprs(tnode.__isset.distribute_expr_lists && !_is_broadcast_join
                                   ? tnode.distribute_expr_lists[1]
                                   : std::vector<TExpr> {}) {}

Status HashJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);

    if (tnode.hash_join_node.__isset.hash_output_slot_ids) {
        _hash_output_slot_ids = tnode.hash_join_node.hash_output_slot_ids;
    }

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr build_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, build_ctx));
        {
            // for type check
            vectorized::VExprContextSPtr probe_ctx;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.left, probe_ctx));
            auto build_side_expr_type = build_ctx->root()->data_type();
            auto probe_side_expr_type = probe_ctx->root()->data_type();
            if (!vectorized::make_nullable(build_side_expr_type)
                         ->equals(*vectorized::make_nullable(probe_side_expr_type))) {
                return Status::InternalError(
                        "build side type {}, not match probe side type {} , node info "
                        "{}",
                        build_side_expr_type->get_name(), probe_side_expr_type->get_name(),
                        this->debug_string(0));
            }
        }
        _build_expr_ctxs.push_back(build_ctx);

        const auto vexpr = _build_expr_ctxs.back()->root();

        /// null safe equal means null = null is true, the operator in SQL should be: <=>.
        const bool is_null_safe_equal =
                eq_join_conjunct.__isset.opcode &&
                (eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) &&
                // For a null safe equal join, FE may generate a plan that
                // both sides of the conjuct are not nullable, we just treat it
                // as a normal equal join conjunct.
                (eq_join_conjunct.right.nodes[0].is_nullable ||
                 eq_join_conjunct.left.nodes[0].is_nullable);

        _is_null_safe_eq_join.push_back(is_null_safe_equal);

        if (eq_join_conjuncts.size() == 1) {
            // single column key serialize method must use nullmap for represent null to instead serialize null into key
            _serialize_null_into_key.emplace_back(false);
        } else if (is_null_safe_equal) {
            // use serialize null into key to represent multi column null value
            _serialize_null_into_key.emplace_back(true);
        } else {
            // on normal conditions, because null!=null, it can be expressed directly with nullmap.
            _serialize_null_into_key.emplace_back(false);
        }
    }

    return Status::OK();
}

Status HashJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinBuildSinkOperatorX<HashJoinBuildSinkLocalState>::prepare(state));
    _use_shared_hash_table =
            _is_broadcast_join && state->enable_share_hash_table_for_broadcast_join();
    auto init_keep_column_flags = [&](auto& tuple_descs, auto& output_slot_flags) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
                if (output_slot_flags.back() &&
                    slot_desc->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
                    _need_finalize_variant_column = true;
                }
            }
        }
    };
    init_keep_column_flags(row_desc().tuple_descriptors(), _should_keep_column_flags);
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_build_expr_ctxs, state, _child->row_desc()));
    return vectorized::VExpr::open(_build_expr_ctxs, state);
}

Status HashJoinBuildSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (local_state._should_build_hash_table) {
        // If eos or have already met a null value using short-circuit strategy, we do not need to pull
        // data from probe side.

        if (local_state._build_side_mutable_block.empty()) {
            auto tmp_build_block = vectorized::VectorizedUtils::create_empty_columnswithtypename(
                    _child->row_desc());
            tmp_build_block = *(tmp_build_block.create_same_struct_block(1, false));
            local_state._build_col_ids.resize(_build_expr_ctxs.size());
            RETURN_IF_ERROR(local_state._do_evaluate(tmp_build_block, local_state._build_expr_ctxs,
                                                     *local_state._build_expr_call_timer,
                                                     local_state._build_col_ids));
            local_state._build_side_mutable_block =
                    vectorized::MutableBlock::build_mutable_block(&tmp_build_block);
        }

        if (!in_block->empty()) {
            std::vector<int> res_col_ids(_build_expr_ctxs.size());
            RETURN_IF_ERROR(local_state._do_evaluate(*in_block, local_state._build_expr_ctxs,
                                                     *local_state._build_expr_call_timer,
                                                     res_col_ids));
            local_state._build_side_rows += in_block->rows();
            if (local_state._build_side_rows > std::numeric_limits<uint32_t>::max()) {
                return Status::NotSupported(
                        "Hash join do not support build table rows over: {}, you should enable "
                        "join spill to avoid this issue",
                        std::to_string(std::numeric_limits<uint32_t>::max()));
            }

            SCOPED_TIMER(local_state._build_side_merge_block_timer);
            RETURN_IF_ERROR(local_state._build_side_mutable_block.merge_ignore_overflow(
                    std::move(*in_block)));
            int64_t blocks_mem_usage = local_state._build_side_mutable_block.allocated_bytes();
            COUNTER_SET(local_state._memory_used_counter, blocks_mem_usage);
            COUNTER_SET(local_state._build_blocks_memory_usage, blocks_mem_usage);
        }
    }

    if (local_state._should_build_hash_table && eos) {
        DCHECK(!local_state._build_side_mutable_block.empty());
        local_state._shared_state->build_block = std::make_shared<vectorized::Block>(
                local_state._build_side_mutable_block.to_block());

        RETURN_IF_ERROR(local_state._runtime_filter_producer_helper->send_filter_size(
                state, local_state._shared_state->build_block->rows(),
                local_state._finish_dependency));

        RETURN_IF_ERROR(
                local_state.process_build_block(state, (*local_state._shared_state->build_block)));
        local_state.init_short_circuit_for_probe();
    } else if (!local_state._should_build_hash_table) {
        // ===== 情况 2：不构建 Hash 表的 Task（共享 Hash 表模式下的非 Task 0） =====
        //
        // 【背景】
        // 在 Broadcast Join 且启用共享 Hash 表（_use_shared_hash_table）时：
        // - Task 0：负责构建 Hash 表（_should_build_hash_table = true）
        // - Task 1-N：不构建 Hash 表（_should_build_hash_table = false），等待 Task 0 完成并共享
        //
        // 【工作流程】
        // 1. 检查是否收到构建完成的信号（_signaled）
        // 2. 如果未收到信号或任务已终止，返回 EOF
        // 3. 从 Task 0 的 Hash 表（hash_table_variant_vector[0]）复制到当前 Task 的槽位
        //
        // 【为什么需要复制而不是直接共享指针】
        // - Probe 阶段可能需要写入一些状态（如 visited flags）
        // - 每个 Task 需要独立的 Hash 表副本，避免并发写入冲突
        // - 但 Hash 表的核心数据（buckets、keys）是共享的

        // ===== 阶段 1：检查构建完成信号 =====
        // 正常情况下，不构建 Hash 表的 Task 应该等待 Task 0 构建完成并发出信号
        // 但如果任务正在运行且 signaled == false，可能是以下情况：
        // 1. Source 算子因为短路策略（short circuit）提前关闭
        // 2. 查询被取消或出错
        // 3. 其他异常情况
        //
        // 返回 EOF 会让任务被标记为 wake_up_early（提前唤醒），正常结束
        // TODO: 在能够保证 wake_up_early 总是准确设置后，移除 signaled 检查
        if (!_signaled || local_state._terminated) {
            return Status::Error<ErrorCode::END_OF_FILE>("source have closed");
        }

        // ===== 阶段 2：验证 Task 索引有效性 =====
        // 确保当前 Task 的索引在 hash_table_variant_vector 的有效范围内
        // hash_table_variant_vector 的大小应该等于 num_instances（实例数量）
        // 例如：如果有 4 个 Task，hash_table_variant_vector.size() = 4，task_idx 应该在 [0, 3] 范围内
        DCHECK_LE(local_state._task_idx,
                  local_state._shared_state->hash_table_variant_vector.size());

        // ===== 阶段 3：复制 Hash 表 =====
        // 使用 std::visit 进行类型安全的 Hash 表复制
        // 从 hash_table_variant_vector[0]（Task 0 构建的 Hash 表）复制到当前 Task 的槽位
        //
        // 【复制过程】
        // - dst：目标 Hash 表（hash_table_variant_vector[task_idx]）
        // - src：源 Hash 表（hash_table_variant_vector[0]）
        // - 使用类型检查确保两个 Hash 表类型匹配
        // - 复制 hash_table 指针（共享底层数据）
        //
        // 【示例】
        // 假设有 4 个 Task，Task 0 构建了 Hash 表：
        // - Task 1: 复制 hash_table_variant_vector[0] → hash_table_variant_vector[1]
        // - Task 2: 复制 hash_table_variant_vector[0] → hash_table_variant_vector[2]
        // - Task 3: 复制 hash_table_variant_vector[0] → hash_table_variant_vector[3]
        std::visit(
                [](auto&& dst, auto&& src) {
                    // 类型检查：确保目标不是 monostate（未初始化），且源和目标类型匹配
                    if constexpr (!std::is_same_v<std::monostate, std::decay_t<decltype(dst)>> &&
                                  std::is_same_v<std::decay_t<decltype(src)>,
                                                 std::decay_t<decltype(dst)>>) {
                        // 复制 Hash 表指针（共享底层数据）
                        // 注意：这里复制的是指针，多个 Task 共享同一个 Hash 表实例
                        // 但每个 Task 有自己的 method_variant 包装，可以独立管理状态
                        dst.hash_table = src.hash_table;
                    } else {
                        // Hash 表类型不匹配，抛出异常
                        // 这通常不应该发生，如果发生说明代码逻辑有问题
                        throw Exception(Status::InternalError(
                                "Hash table type mismatch when share hash table"));
                    }
                },
                // 目标：当前 Task 的 Hash 表槽位
                local_state._shared_state->hash_table_variant_vector[local_state._task_idx]
                        ->method_variant,
                // 源：Task 0 构建的 Hash 表（第一个槽位）
                local_state._shared_state->hash_table_variant_vector.front()->method_variant);
    }

    if (eos) {
        // If a shared hash table is used, states are shared by all tasks.
        // Sink and source has n-n relationship If a shared hash table is used otherwise 1-1 relationship.
        // So we should notify the `_task_idx` source task if a shared hash table is used.
        local_state._dependency->set_ready_to_read(_use_shared_hash_table ? local_state._task_idx
                                                                          : 0);
    }

    return Status::OK();
}

size_t HashJoinBuildSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos) {
    auto& local_state = get_local_state(state);
    return local_state.get_reserve_mem_size(state, eos);
}

size_t HashJoinBuildSinkOperatorX::get_memory_usage(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._memory_used_counter->value();
}

std::string HashJoinBuildSinkOperatorX::get_memory_usage_debug_str(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return fmt::format("build block: {}, hash table: {}, build key arena: {}",
                       PrettyPrinter::print_bytes(local_state._build_blocks_memory_usage->value()),
                       PrettyPrinter::print_bytes(local_state._hash_table_memory_usage->value()),
                       PrettyPrinter::print_bytes(local_state._build_arena_memory_usage->value()));
}

} // namespace doris::pipeline
