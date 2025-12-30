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

#include "hashjoin_probe_operator.h"

#include <gen_cpp/PlanNodes_types.h>

#include <string>

#include "common/cast_set.h"
#include "common/logging.h"
#include "pipeline/exec/operator.h"
#include "runtime/descriptors.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
HashJoinProbeLocalState::HashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
        : JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>(state, parent),
          _process_hashtable_ctx_variants(std::make_unique<HashTableCtxVariants>()) {}

Status HashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(JoinProbeLocalState::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _task_idx = info.task_idx;
    auto& p = _parent->cast<HashJoinProbeOperatorX>();
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }
    _other_join_conjuncts.resize(p._other_join_conjuncts.size());
    for (size_t i = 0; i < _other_join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._other_join_conjuncts[i]->clone(state, _other_join_conjuncts[i]));
    }

    _mark_join_conjuncts.resize(p._mark_join_conjuncts.size());
    for (size_t i = 0; i < _mark_join_conjuncts.size(); i++) {
        RETURN_IF_ERROR(p._mark_join_conjuncts[i]->clone(state, _mark_join_conjuncts[i]));
    }

    _construct_mutable_join_block();
    _probe_column_disguise_null.reserve(_probe_expr_ctxs.size());
    _probe_arena_memory_usage = custom_profile()->AddHighWaterMarkCounter(
            "MemoryUsageProbeKeyArena", TUnit::BYTES, "", 1);
    // Probe phase
    _probe_expr_call_timer = ADD_TIMER(custom_profile(), "ProbeExprCallTime");
    _search_hashtable_timer = ADD_TIMER(custom_profile(), "ProbeWhenSearchHashTableTime");
    _build_side_output_timer = ADD_TIMER(custom_profile(), "ProbeWhenBuildSideOutputTime");
    _probe_side_output_timer = ADD_TIMER(custom_profile(), "ProbeWhenProbeSideOutputTime");
    _non_equal_join_conjuncts_timer =
            ADD_TIMER(custom_profile(), "NonEqualJoinConjunctEvaluationTime");
    _init_probe_side_timer = ADD_TIMER(custom_profile(), "InitProbeSideTime");
    return Status::OK();
}

Status HashJoinProbeLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(JoinProbeLocalState::open(state));

    auto& p = _parent->cast<HashJoinProbeOperatorX>();
    Status res;
    std::visit(
            [&](auto&& join_op_variants, auto have_other_join_conjunct) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                if constexpr (JoinOpType::value == TJoinOp::CROSS_JOIN) {
                    res = Status::InternalError("hash join do not support cross join");
                } else {
                    _process_hashtable_ctx_variants
                            ->emplace<ProcessHashTableProbe<JoinOpType::value>>(
                                    this, state->batch_size());
                }
            },
            _shared_state->join_op_variants,
            vectorized::make_bool_variant(p._have_other_join_conjunct));
    return res;
}

void HashJoinProbeLocalState::prepare_for_next() {
    _probe_index = 0;
    _build_index = 0;
    _ready_probe = false;
    _last_probe_match = -1;
    _last_probe_null_mark = -1;
    _prepare_probe_block();
}

Status HashJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    if (_process_hashtable_ctx_variants) {
        std::visit(vectorized::Overload {[&](std::monostate&) {},
                                         [&](auto&& process_hashtable_ctx) {
                                             if (process_hashtable_ctx._arena) {
                                                 process_hashtable_ctx._arena.reset();
                                             }
                                         }},
                   *_process_hashtable_ctx_variants);
    }
    _process_hashtable_ctx_variants = nullptr;
    _null_map_column = nullptr;
    _probe_block.clear();
    return JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>::close(state);
}

bool HashJoinProbeLocalState::_need_probe_null_map(vectorized::Block& block,
                                                   const std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (column->is_nullable() &&
            !_parent->cast<HashJoinProbeOperatorX>()._serialize_null_into_key[i]) {
            return true;
        }
    }
    return false;
}

void HashJoinProbeLocalState::_prepare_probe_block() {
    // clear_column_data of _probe_block
    if (!_probe_column_disguise_null.empty()) {
        for (int i = 0; i < _probe_column_disguise_null.size(); ++i) {
            auto column_to_erase = _probe_column_disguise_null[i];
            _probe_block.erase(column_to_erase - i);
        }
        _probe_column_disguise_null.clear();
    }

    // remove add nullmap of probe columns
    for (auto index : _probe_column_convert_to_null) {
        auto& column_type = _probe_block.safe_get_by_position(index);
        DCHECK(column_type.column->is_nullable() || is_column_const(*(column_type.column.get())));
        DCHECK(column_type.type->is_nullable());

        column_type.column = remove_nullable(column_type.column);
        column_type.type = remove_nullable(column_type.type);
    }
    _key_columns_holder.clear();
    _probe_block.clear_column_data(_parent->get_child()->row_desc().num_materialized_slots());
}

HashJoinProbeOperatorX::HashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                               int operator_id, const DescriptorTbl& descs)
        : JoinProbeOperatorX<HashJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _is_broadcast_join(tnode.hash_join_node.__isset.is_broadcast_join &&
                             tnode.hash_join_node.is_broadcast_join),
          _hash_output_slot_ids(tnode.hash_join_node.__isset.hash_output_slot_ids
                                        ? tnode.hash_join_node.hash_output_slot_ids
                                        : std::vector<SlotId> {}),
          _partition_exprs(tnode.__isset.distribute_expr_lists && !_is_broadcast_join
                                   ? tnode.distribute_expr_lists[0]
                                   : std::vector<TExpr> {}) {}

/**
 * 从 Hash 表中拉取 Join 结果
 * 
 * 【功能说明】
 * 这是 Hash Join Probe 算子的核心方法，负责：
 * 1. 处理 Probe 数据与 Build 表的 Hash Join
 * 2. 处理右表未匹配的数据（RIGHT_SEMI_ANTI、OUTER JOIN）
 * 3. 应用过滤条件并构建输出数据块
 * 
 * 【工作流程】
 * 1. 检查短路策略（short_circuit_for_probe）：如果启用，直接返回空结果
 * 2. 检查空右表短路（empty_right_table_shortcut）：如果右表为空且是特定 Join 类型，直接返回 Probe 数据 + NULL 列
 * 3. 处理 Probe 数据：
 *    - 如果还有未处理的 Probe 数据：调用 process() 进行 Hash Join
 *    - 如果 Probe 数据已处理完：调用 finish_probing() 处理右表未匹配的数据
 * 4. 过滤和构建输出：应用其他 Join 条件和过滤条件，构建最终输出
 * 
 * 【关键设计】
 * - 短路优化：当 Build 表为空或满足特定条件时，跳过 Hash Join 计算
 * - 分阶段处理：先处理 Probe 数据，再处理右表未匹配数据
 * - 多态 Hash 表：使用 std::visit 支持不同类型的 Hash 表
 * 
 * 【示例】
 * 假设 INNER JOIN，Probe 表有 1000 行，Build 表有 500 行：
 * 
 * 1. 处理 Probe 数据：
 *    - 对每行 Probe 数据，在 Hash 表中查找匹配的 Build 数据
 *    - 如果匹配，生成 Join 结果行
 *    - 结果存储在 temp_block 中
 * 
 * 2. 处理右表未匹配数据（如果是 RIGHT_OUTER_JOIN）：
 *    - 查找 Build 表中未被匹配的行
 *    - 生成 NULL + Build 数据的行
 * 
 * 3. 过滤和输出：
 *    - 应用其他 Join 条件（如 WHERE 子句）
 *    - 构建最终输出数据块
 * 
 * @param state RuntimeState
 * @param output_block 输出数据块（存储 Join 结果）
 * @param eos 是否数据流结束（输出参数）
 * @return Status
 */
Status HashJoinProbeOperatorX::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                    bool* eos) const {
    auto& local_state = get_local_state(state);
    
    // ===== 阶段 1：检查短路策略 =====
    // short_circuit_for_probe：当 Build 表为空且是特定 Join 类型时，直接返回空结果
    // 例如：INNER JOIN 且 Build 表为空 → 结果为空，无需处理 Probe 数据
    if (local_state._shared_state->short_circuit_for_probe) {
        // 如果使用短路策略，直接返回空块
        *eos = true;
        return Status::OK();
    }

    // ===== 阶段 2：检查空右表短路 =====
    // empty_right_table_shortcut：当 Build 表为空且是特定 Join 类型时，返回 Probe 数据 + NULL 列
    // 例如：LEFT_OUTER_JOIN 且 Build 表为空 → 返回 Probe 数据 + Build 表的 NULL 列
    //TODO: this short circuit maybe could refactor, no need to check at here.
    if (local_state.empty_right_table_shortcut()) {
        // 当 Build 表行数为 0 且没有其他 Join 条件，且 Join 类型是以下之一：
        // LEFT_OUTER_JOIN/FULL_OUTER_JOIN/LEFT_ANTI_JOIN
        // 我们可以直接返回 Probe 表 + NULL 列（如果需要输出）
        // 如果使用短路策略，直接返回块，添加额外的 NULL 数据
        
        auto block_rows = local_state._probe_block.rows();
        // 如果 Probe 数据已结束且块为空，返回 EOS
        if (local_state._probe_eos && block_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        // ===== 创建 Build 侧的 NULL 列（如果需要输出） =====
        // 对于 LEFT_OUTER_JOIN/FULL_OUTER_JOIN，需要输出 Build 表的列（填充 NULL）
        // 对于 LEFT_ANTI_JOIN，不需要输出 Build 表的列
        for (int i = 0;
             (_join_op != TJoinOp::LEFT_ANTI_JOIN) && i < _right_output_slot_flags.size(); ++i) {
            // 创建非 NULL 类型的列
            auto type = remove_nullable(_right_table_data_types[i]);
            auto column = type->create_column();
            column->resize(block_rows);
            
            // 创建 NULL Map（所有行都是 NULL）
            auto null_map_column = vectorized::ColumnUInt8::create(block_rows, 1);
            // 创建可 NULL 列
            auto nullable_column = vectorized::ColumnNullable::create(std::move(column),
                                                                      std::move(null_map_column));
            // 插入到 Probe 块中
            local_state._probe_block.insert({std::move(nullable_column), make_nullable(type),
                                             _right_table_column_names[i]});
        }

        // 不需要在 `filter_data_and_build_output` 中检查块大小，因为这里不会增加输出行数
        // （输出行数等于 `_probe_block` 的行数）
        RETURN_IF_ERROR(local_state.filter_data_and_build_output(state, output_block, eos,
                                                                 &local_state._probe_block, false));
        // 清空 Probe 块的数据（保留列结构）
        local_state._probe_block.clear_column_data(_child->row_desc().num_materialized_slots());
        return Status::OK();
    }

    // ===== 阶段 3：正常的 Hash Join 处理 =====
    // 清空 Join 块（用于存储 Join 结果）
    local_state._join_block.clear_column_data();

    // 创建可变的 Join 块（用于写入 Join 结果）
    vectorized::MutableBlock mutable_join_block(&local_state._join_block);
    // 临时块（用于存储过滤后的结果）
    vectorized::Block temp_block;

    Status st;
    
    // ===== 情况 A：还有未处理的 Probe 数据 =====
    // _probe_index：当前处理的 Probe 数据行索引
    // 如果 _probe_index < _probe_block.rows()，说明还有数据未处理
    if (local_state._probe_index < local_state._probe_block.rows()) {
        // 确保已经设置了 NULL Map（用于处理 NULL 值）
        DCHECK(local_state._has_set_need_null_map_for_probe);
        
        // ===== 使用 std::visit 处理不同类型的 Hash 表 =====
        // 
        // 【为什么 std::visit 有三个参数？】
        // std::visit 可以接受多个 variant 参数，当有多个 variant 时，它会遍历所有 variant 的组合。
        // 
        // 这里有两个独立的 variant：
        // 1. Hash 表类型 variant（method_variant）：
        //    - 可能的值：UInt8、UInt16、UInt32、UInt64、UInt128、UInt256、String、FixedKey 等
        //    - 决定了如何查找：不同的键类型有不同的 Hash 计算和查找方法
        //    - 例如：UInt64 键使用 CRC32 Hash，String 键使用字符串 Hash
        // 
        // 2. Join 操作类型 variant（_process_hashtable_ctx_variants）：
        //    - 可能的值：INNER_JOIN、LEFT_OUTER_JOIN、RIGHT_OUTER_JOIN、LEFT_SEMI_JOIN 等
        //    - 决定了如何处理匹配结果：不同的 Join 类型有不同的输出逻辑
        //    - 例如：INNER JOIN 只输出匹配的行，LEFT OUTER JOIN 还要输出未匹配的左表行
        // 
        // 【为什么需要两个 variant？】
        // Hash 表类型和 Join 操作类型是两个独立的维度，需要组合才能确定最终的处理逻辑。
        // 例如：
        // - UInt64 Hash 表 + INNER JOIN → 使用 UInt64 的查找方法 + INNER JOIN 的输出逻辑
        // - String Hash 表 + LEFT OUTER JOIN → 使用 String 的查找方法 + LEFT OUTER JOIN 的输出逻辑
        // 
        // 【std::visit 的工作原理】
        // 1. std::visit 会检查两个 variant 中实际存储的类型
        // 2. 根据实际类型组合，调用 Lambda 函数
        // 3. Lambda 函数中的 arg 和 process_hashtable_ctx 会被设置为对应的实际类型
        // 4. 编译器会根据这两个类型，选择对应的 process() 方法实现（通过模板特化）
        // 
        // 【示例】
        // 假设：
        // - method_variant 存储的是 PrimaryTypeHashTableContext<UInt64>
        // - _process_hashtable_ctx_variants 存储的是 ProcessHashTableProbe<TJoinOp::INNER_JOIN>
        // 
        // 那么：
        // - arg 的类型是 PrimaryTypeHashTableContext<UInt64>&
        // - process_hashtable_ctx 的类型是 ProcessHashTableProbe<TJoinOp::INNER_JOIN>&
        // - 编译器会选择 ProcessHashTableProbe<TJoinOp::INNER_JOIN>::process(PrimaryTypeHashTableContext<UInt64>&, ...) 的实现
        // 
        std::visit(
                // ===== Lambda 函数：处理 Hash Join =====
                // [&]：捕获所有外部变量（按引用）
                // auto&& arg：第一个 variant 的实际类型（Hash 表类型，如 PrimaryTypeHashTableContext<UInt64>）
                // auto&& process_hashtable_ctx：第二个 variant 的实际类型（Join 操作类型，如 ProcessHashTableProbe<TJoinOp::INNER_JOIN>）
                [&](auto&& arg, auto&& process_hashtable_ctx) {
                    // ===== 步骤 1：获取处理上下文的类型 =====
                    // std::decay_t：去除引用和 const 修饰符，获取核心类型
                    // 例如：const PrimaryTypeHashTableContext<UInt64>& → PrimaryTypeHashTableContext<UInt64>
                    using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                    
                    // ===== 步骤 2：检查处理上下文是否已初始化 =====
                    // if constexpr：编译时条件判断（不是运行时判断）
                    // std::monostate：表示 variant 未初始化（空状态）
                    // 如果处理上下文未初始化，说明 Hash Join 还未准备好
                    if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                        // ===== 步骤 3：获取 Hash 表的类型 =====
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        
                        // ===== 步骤 4：检查 Hash 表是否已初始化 =====
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            // ===== 步骤 5：调用 process() 方法进行 Hash Join =====
                            // 
                            // 【类型组合决定方法选择】
                            // 编译器会根据 arg 和 process_hashtable_ctx 的实际类型组合，
                            // 选择对应的 process() 方法实现（通过模板特化）。
                            // 
                            // 【示例】
                            // 假设：
                            // - arg 是 PrimaryTypeHashTableContext<UInt64>（UInt64 键的 Hash 表）
                            // - process_hashtable_ctx 是 ProcessHashTableProbe<TJoinOp::INNER_JOIN>（INNER JOIN 处理逻辑）
                            // 
                            // 那么编译器会选择：
                            // ProcessHashTableProbe<TJoinOp::INNER_JOIN>::process(
                            //     PrimaryTypeHashTableContext<UInt64>& hash_table_ctx,
                            //     ...)
                            // 
                            // 这个方法会：
                            // 1. 使用 UInt64 的 Hash 计算方法查找匹配的行
                            // 2. 使用 INNER JOIN 的输出逻辑处理匹配结果（只输出匹配的行）
                            // 
                            // 【参数说明】
                            // - arg：Hash 表上下文（包含 Hash 表实例），决定了如何查找
                            // - process_hashtable_ctx：Join 处理上下文，决定了如何处理匹配结果
                            // - _null_map_column：NULL Map 列（用于处理 NULL 值）
                            // - mutable_join_block：可变的 Join 块（写入 Join 结果）
                            // - temp_block：临时块（存储 Join 结果）
                            // - _probe_block.rows()：Probe 块的行数
                            // - _is_mark_join：是否是 Mark Join
                            st = process_hashtable_ctx.process(
                                    arg,
                                    local_state._null_map_column
                                            ? local_state._null_map_column->get_data().data()
                                            : nullptr,
                                    mutable_join_block, &temp_block,
                                    cast_set<uint32_t>(local_state._probe_block.rows()),
                                    _is_mark_join);
                        } else {
                            // Hash 表未初始化，返回错误
                            st = Status::InternalError("uninited hash table");
                        }
                    } else {
                        // 处理上下文未初始化，返回错误
                        st = Status::InternalError("uninited hash table probe");
                    }
                },
                // ===== std::visit 的第二个参数：Hash 表类型 variant =====
                // 类型：HashTableVariants（std::variant）
                // 可能的值：PrimaryTypeHashTableContext<UInt8/16/32/64/128/256>、String、FixedKey 等
                // 作用：决定如何查找（Hash 计算、桶定位、键比较等）
                // 
                // 根据任务数量选择对应的 Hash 表：
                // - Broadcast Join（size == 1）：所有任务共享 hash_table_variant_vector[0]
                // - Partitioned Join（size > 1）：每个任务使用 hash_table_variant_vector[task_idx]
                // 
                // 这个 variant 对应 Lambda 函数中的 arg 参数
                local_state._shared_state->hash_table_variant_vector.size() == 1
                        ? local_state._shared_state->hash_table_variant_vector[0]->method_variant
                        : local_state._shared_state
                                  ->hash_table_variant_vector[local_state._task_idx]
                                  ->method_variant,
                // ===== std::visit 的第三个参数：Join 操作类型 variant =====
                // 类型：HashTableCtxVariants（std::variant）
                // 可能的值：ProcessHashTableProbe<TJoinOp::INNER_JOIN>、ProcessHashTableProbe<TJoinOp::LEFT_OUTER_JOIN> 等
                // 作用：决定如何处理匹配结果（输出逻辑、未匹配行的处理等）
                // 
                // 这个 variant 对应 Lambda 函数中的 process_hashtable_ctx 参数
                // 它包含了对应 Join 类型的 process() 方法实现
                *local_state._process_hashtable_ctx_variants);
    } 
    // ===== 情况 B：Probe 数据已处理完，处理右表未匹配数据 =====
    // 如果 Probe 数据已结束（_probe_eos == true），需要处理右表未匹配的数据
    // 这适用于 RIGHT_SEMI_ANTI、RIGHT_OUTER_JOIN、FULL_OUTER_JOIN 等 Join 类型
    else if (local_state._probe_eos) {
        // 如果是 RIGHT_SEMI_ANTI 或 OUTER JOIN（除了 LEFT_OUTER_JOIN），需要处理右表未匹配数据
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            // 调用 finish_probing() 方法处理右表未匹配的数据
            std::visit(
                    [&](auto&& arg, auto&& process_hashtable_ctx) {
                        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                            using HashTableCtxType = std::decay_t<decltype(arg)>;
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                // finish_probing()：处理右表未匹配的数据
                                // 例如：RIGHT_OUTER_JOIN 中，查找 Build 表中未被匹配的行
                                st = process_hashtable_ctx.finish_probing(
                                        arg, mutable_join_block, &temp_block, eos, _is_mark_join);
                            } else {
                                st = Status::InternalError("uninited hash table");
                            }
                        } else {
                            st = Status::InternalError("uninited hash table probe");
                        }
                    },
                    // 选择 Hash 表变体（同上）
                    local_state._shared_state->hash_table_variant_vector.size() == 1
                            ? local_state._shared_state->hash_table_variant_vector[0]
                                      ->method_variant
                            : local_state._shared_state
                                      ->hash_table_variant_vector[local_state._task_idx]
                                      ->method_variant,
                    *local_state._process_hashtable_ctx_variants);
        } else {
            // 其他 Join 类型（如 LEFT_OUTER_JOIN），不需要处理右表未匹配数据
            *eos = true;
            return Status::OK();
        }
    } else {
        // ===== 情况 C：Probe 数据未结束，但当前块已处理完 =====
        // 等待更多 Probe 数据
        return Status::OK();
    }
    
    // ===== 阶段 4：检查处理结果 =====
    // 如果处理失败，返回错误
    if (!st) {
        return st;
    }

    // ===== 阶段 5：过滤和构建输出 =====
    // 更新内存使用估计（加上临时块的内存）
    local_state._estimate_memory_usage += temp_block.allocated_bytes();
    
    // 应用过滤条件并构建输出数据块
    // filter_data_and_build_output() 会：
    // 1. 应用其他 Join 条件（如 WHERE 子句）
    // 2. 构建最终输出数据块（选择需要的列）
    RETURN_IF_ERROR(
            local_state.filter_data_and_build_output(state, output_block, eos, &temp_block));
    
    // ===== 阶段 6：清理资源 =====
    // 释放 _join_block 中的列指针（避免内存泄漏）
    local_state._join_block.set_columns(local_state._join_block.clone_empty_columns());
    // 清空可变块
    mutable_join_block.clear();
    return Status::OK();
}

std::string HashJoinProbeLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}, short_circuit_for_probe: {}",
                   JoinProbeLocalState<HashJoinSharedState, HashJoinProbeLocalState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL");
    return fmt::to_string(debug_string_buffer);
}

Status HashJoinProbeLocalState::_extract_join_column(vectorized::Block& block,
                                                     const std::vector<int>& res_col_ids) {
    if (empty_right_table_shortcut()) {
        return Status::OK();
    }

    _probe_columns.resize(_probe_expr_ctxs.size());

    if (!_has_set_need_null_map_for_probe) {
        _has_set_need_null_map_for_probe = true;
        _need_null_map_for_probe = _need_probe_null_map(block, res_col_ids);
    }
    if (_need_null_map_for_probe) {
        if (!_null_map_column) {
            _null_map_column = vectorized::ColumnUInt8::create();
        }
        _null_map_column->get_data().assign(block.rows(), (uint8_t)0);
    }

    auto& shared_state = *_shared_state;
    for (size_t i = 0; i < shared_state.build_exprs_size; ++i) {
        const auto* column = block.get_by_position(res_col_ids[i]).column.get();
        if (!column->is_nullable() &&
            _parent->cast<HashJoinProbeOperatorX>()._serialize_null_into_key[i]) {
            _key_columns_holder.emplace_back(
                    vectorized::make_nullable(block.get_by_position(res_col_ids[i]).column));
            _probe_columns[i] = _key_columns_holder.back().get();
        } else if (const auto* nullable = check_and_get_column<vectorized::ColumnNullable>(*column);
                   nullable &&
                   !_parent->cast<HashJoinProbeOperatorX>()._serialize_null_into_key[i]) {
            // update nulllmap and split nested out of ColumnNullable when serialize_null_into_key is false and column is nullable
            const auto& col_nested = nullable->get_nested_column();
            const auto& col_nullmap = nullable->get_null_map_data();
            DCHECK(_null_map_column);
            vectorized::VectorizedUtils::update_null_map(_null_map_column->get_data(), col_nullmap);
            _probe_columns[i] = &col_nested;
        } else {
            _probe_columns[i] = column;
        }
    }
    return Status::OK();
}

std::vector<uint16_t> HashJoinProbeLocalState::_convert_block_to_null(vectorized::Block& block) {
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

Status HashJoinProbeLocalState::filter_data_and_build_output(RuntimeState* state,
                                                             vectorized::Block* output_block,
                                                             bool* eos,
                                                             vectorized::Block* temp_block,
                                                             bool check_rows_count) {
    auto output_rows = temp_block->rows();
    if (check_rows_count) {
        DCHECK(output_rows <= state->batch_size());
    }
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(filter_block(_conjuncts, temp_block, temp_block->columns()));
    }

    RETURN_IF_ERROR(_build_output_block(temp_block, output_block));
    reached_limit(output_block, eos);
    return Status::OK();
}

bool HashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = state->get_local_state(operator_id())->cast<HashJoinProbeLocalState>();
    return (local_state._probe_block.rows() == 0 ||
            local_state._probe_index == local_state._probe_block.rows()) &&
           !local_state._probe_eos && !local_state._shared_state->short_circuit_for_probe;
}

Status HashJoinProbeOperatorX::_do_evaluate(vectorized::Block& block,
                                            vectorized::VExprContextSPtrs& exprs,
                                            RuntimeProfile::Counter& expr_call_timer,
                                            std::vector<int>& res_col_ids) const {
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
    return Status::OK();
}

/**
 * 接收 Probe 侧的输入数据块，并进行预处理
 * 
 * 【功能说明】
 * 这是 Hash Join Probe 算子的输入接口，负责接收来自上游算子的 Probe 数据块。
 * 主要完成以下工作：
 * 1. 重置 Probe 状态，准备处理新的数据块
 * 2. 执行 Probe 侧的表达式，计算 Join 键的值
 * 3. 对于 RIGHT/FULL OUTER JOIN，将 Probe 侧列转换为 Nullable
 * 4. 提取 Join 键列，准备用于 Hash 表查找
 * 5. 统计内存使用情况
 * 
 * 【处理流程】
 * 1. 状态重置：
 *    - 调用 `prepare_for_next()` 重置 `_probe_index`、`_build_index`、`_ready_probe` 等状态
 *    - 设置 `_probe_eos` 标志，表示 Probe 数据是否结束
 * 
 * 2. 表达式计算（如果数据块非空）：
 *    - 调用 `_do_evaluate()` 执行 `_probe_expr_ctxs` 中的表达式
 *    - 这些表达式通常是 Join 键的计算表达式（如 `a + b`、`func(c)` 等）
 *    - 结果存储在 `res_col_ids` 中，表示计算出的列在 Block 中的索引
 *    - 例如：如果 Join 键是 `t1.a = t2.b`，则 `_probe_expr_ctxs[0]` 计算 `t1.a` 的值
 * 
 * 3. NULL 值处理（仅 RIGHT/FULL OUTER JOIN）：
 *    - 对于 RIGHT OUTER JOIN 和 FULL OUTER JOIN，需要输出未匹配的 Build 侧行
 *    - 当 Probe 侧行未匹配时，Probe 侧的列需要填充为 NULL
 *    - 调用 `_convert_block_to_null()` 将 Probe 侧的所有列转换为 Nullable 类型
 *    - 例如：RIGHT JOIN 中，如果 Build 侧行未匹配，Probe 侧列需要输出 NULL
 * 
 * 4. Join 键列提取：
 *    - 调用 `_extract_join_column()` 从 Block 中提取 Join 键列
 *    - 根据 NULL 值处理策略（`_serialize_null_into_key`）进行列转换：
 *      * 如果 `serialize_null_into_key = true`：将 NULL 序列化到键中（用于 NULL Safe Equal Join <=>）
 *      * 如果 `serialize_null_into_key = false`：使用外部 NULL Map 表示 NULL（用于普通 Join =）
 *    - 提取的列存储在 `_probe_columns` 中，后续用于 Hash 表查找
 * 
 * 5. 内存管理：
 *    - 统计表达式计算和列转换后的内存增长
 *    - 如果 `input_block` 和 `_probe_block` 不是同一个对象，则交换数据
 *    - 更新内存使用计数器
 * 
 * 【参数说明】
 * @param state 运行时状态
 * @param input_block Probe 侧的输入数据块（可能为空）
 * @param eos 是否表示 Probe 数据结束（End of Stream）
 * @return Status
 * 
 * 【使用示例】
 * 假设有一个 Hash Join：`SELECT * FROM t1 JOIN t2 ON t1.a = t2.b`
 * 1. 上游算子（如 Scan）调用 `push(state, block, false)` 推送 `t1` 的数据块
 * 2. `push` 方法执行表达式 `t1.a`，得到 Join 键列
 * 3. 提取 Join 键列到 `_probe_columns`，准备用于 Hash 表查找
 * 4. 后续 `pull` 方法会使用 `_probe_columns` 在 Hash 表中查找匹配的 Build 侧行
 * 
 * 【注意事项】
 * - 如果 `input_block` 为空（`rows == 0`），只更新 `_probe_eos` 标志，不进行其他处理
 * - `_probe_block` 是 Probe 算子的内部缓冲区，用于存储预处理后的 Probe 数据
 * - 对于 Broadcast Join，所有 Probe 算子共享同一个 Hash 表，但每个算子有独立的 `_probe_block`
 */
Status HashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                    bool eos) const {
    auto& local_state = get_local_state(state);
    // ===== 步骤 1：重置 Probe 状态，准备处理新的数据块 =====
    // 重置 _probe_index、_build_index、_ready_probe 等状态变量
    local_state.prepare_for_next();
    // 设置 Probe 数据结束标志
    local_state._probe_eos = eos;

    const auto rows = input_block->rows();
    size_t origin_size = input_block->allocated_bytes();

    if (rows > 0) {
        // ===== 步骤 2：统计 Probe 行数 =====
        COUNTER_UPDATE(local_state._probe_rows_counter, rows);
        
        // ===== 步骤 3：执行 Probe 侧的表达式，计算 Join 键的值 =====
        // res_col_ids 存储表达式计算结果的列索引
        // 例如：如果 Join 键是 t1.a，则 _probe_expr_ctxs[0] 计算 t1.a 的值
        std::vector<int> res_col_ids(local_state._probe_expr_ctxs.size());
        RETURN_IF_ERROR(_do_evaluate(*input_block, local_state._probe_expr_ctxs,
                                     *local_state._probe_expr_call_timer, res_col_ids));
        
        // ===== 步骤 4：对于 RIGHT/FULL OUTER JOIN，将 Probe 侧列转换为 Nullable =====
        // 原因：当 Build 侧行未匹配时，Probe 侧列需要输出 NULL
        // 例如：RIGHT JOIN 中，如果 Build 侧行未匹配，Probe 侧列需要填充为 NULL
        if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
            // _probe_column_convert_to_null 记录哪些列被转换了（用于后续输出）
            local_state._probe_column_convert_to_null =
                    local_state._convert_block_to_null(*input_block);
        }

        // ===== 步骤 5：提取 Join 键列，准备用于 Hash 表查找 =====
        // 根据 NULL 值处理策略（_serialize_null_into_key）进行列转换：
        // - serialize_null_into_key = true：将 NULL 序列化到键中（用于 NULL Safe Equal Join <=>）
        // - serialize_null_into_key = false：使用外部 NULL Map 表示 NULL（用于普通 Join =）
        // 提取的列存储在 _probe_columns 中，后续用于 Hash 表查找
        RETURN_IF_ERROR(local_state._extract_join_column(*input_block, res_col_ids));

        // ===== 步骤 6：统计内存使用情况 =====
        // 计算表达式计算和列转换后的内存增长
        local_state._estimate_memory_usage += (input_block->allocated_bytes() - origin_size);

        // ===== 步骤 7：交换数据到内部缓冲区 =====
        // 如果 input_block 和 _probe_block 不是同一个对象，则交换数据
        // _probe_block 是 Probe 算子的内部缓冲区，用于存储预处理后的 Probe 数据
        if (&local_state._probe_block != input_block) {
            input_block->swap(local_state._probe_block);
            // 更新内存使用计数器
            COUNTER_SET(local_state._memory_used_counter,
                        (int64_t)local_state._probe_block.allocated_bytes());
        }
    }
    return Status::OK();
}

Status HashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _probe_expr_ctxs.push_back(ctx);

        /// null safe equal means null = null is true, the operator in SQL should be: <=>.
        const bool is_null_safe_equal =
                eq_join_conjunct.__isset.opcode &&
                (eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) &&
                // For a null safe equal join, FE may generate a plan that
                // both sides of the conjuct are not nullable, we just treat it
                // as a normal equal join conjunct.
                (eq_join_conjunct.right.nodes[0].is_nullable ||
                 eq_join_conjunct.left.nodes[0].is_nullable);

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

    if (tnode.hash_join_node.__isset.other_join_conjuncts &&
        !tnode.hash_join_node.other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.other_join_conjuncts, _other_join_conjuncts));

        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    } else if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _other_join_conjuncts.resize(1);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                tnode.hash_join_node.vother_join_conjunct, _other_join_conjuncts[0]));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    if (tnode.hash_join_node.__isset.mark_join_conjuncts &&
        !tnode.hash_join_node.mark_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.mark_join_conjuncts, _mark_join_conjuncts));
        DCHECK(_is_mark_join);

        /// We make mark join conjuncts as equal conjuncts for null aware join,
        /// so `_mark_join_conjuncts` should be empty if this is null aware join.
        DCHECK_EQ(_mark_join_conjuncts.empty(),
                  _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                          _join_op == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN);
    }

    return Status::OK();
}

Status HashJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX<HashJoinProbeLocalState>::prepare(state));
    // init left/right output slots flags, only column of slot_id in _hash_output_slot_ids need
    // insert to output block of hash join.
    // _left_output_slots_flags : column of left table need to output set flag = true
    // _rgiht_output_slots_flags : column of right table need to output set flag = true
    // if _hash_output_slot_ids is empty, means all column of left/right table need to output.
    auto init_output_slots_flags = [&](auto& tuple_descs, auto& output_slot_flags,
                                       bool init_finalize_flag = false) {
        for (const auto& tuple_desc : tuple_descs) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                output_slot_flags.emplace_back(
                        std::find(_hash_output_slot_ids.begin(), _hash_output_slot_ids.end(),
                                  slot_desc->id()) != _hash_output_slot_ids.end());
                if (init_finalize_flag && output_slot_flags.back() &&
                    slot_desc->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
                    _need_finalize_variant_column = true;
                }
            }
        }
    };
    init_output_slots_flags(_child->row_desc().tuple_descriptors(), _left_output_slot_flags, true);
    init_output_slots_flags(_build_side_child->row_desc().tuple_descriptors(),
                            _right_output_slot_flags);
    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
        conjunct->root()->collect_slot_column_ids(_should_not_lazy_materialized_column_ids);
    }

    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, *_intermediate_row_desc));
        conjunct->root()->collect_slot_column_ids(_should_not_lazy_materialized_column_ids);
    }

    RETURN_IF_ERROR(vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    DCHECK(_build_side_child != nullptr);
    // right table data types
    _right_table_data_types =
            vectorized::VectorizedUtils::get_data_types(_build_side_child->row_desc());
    _left_table_data_types = vectorized::VectorizedUtils::get_data_types(_child->row_desc());
    _right_table_column_names =
            vectorized::VectorizedUtils::get_column_names(_build_side_child->row_desc());

    std::vector<const SlotDescriptor*> slots_to_check;
    for (const auto& tuple_descriptor : _intermediate_row_desc->tuple_descriptors()) {
        for (const auto& slot : tuple_descriptor->slots()) {
            slots_to_check.emplace_back(slot);
        }
    }

    if (_is_mark_join) {
        const auto* last_one = slots_to_check.back();
        slots_to_check.pop_back();
        auto data_type = last_one->get_data_type_ptr();
        if (!data_type->is_nullable()) {
            return Status::InternalError(
                    "The last column for mark join should be Nullable(UInt8), not {}",
                    data_type->get_name());
        }

        const auto& null_data_type = assert_cast<const vectorized::DataTypeNullable&>(*data_type);
        if (null_data_type.get_nested_type()->get_primitive_type() != PrimitiveType::TYPE_BOOLEAN) {
            return Status::InternalError(
                    "The last column for mark join should be Nullable(UInt8), not {}",
                    data_type->get_name());
        }
    }

    _right_col_idx = (_is_right_semi_anti && !_have_other_join_conjunct &&
                      (!_is_mark_join || _mark_join_conjuncts.empty()))
                             ? 0
                             : _left_table_data_types.size();

    size_t idx = 0;
    for (const auto* slot : slots_to_check) {
        auto data_type = slot->get_data_type_ptr();
        const auto slot_on_left = idx < _right_col_idx;

        if (slot_on_left) {
            if (idx >= _left_table_data_types.size()) {
                return Status::InternalError(
                        "Join node(id={}, OP={}) intermediate slot({}, #{})'s on left table "
                        "idx out bound of _left_table_data_types: {} vs {}",
                        _node_id, _join_op, slot->col_name(), slot->id(), idx,
                        _left_table_data_types.size());
            }
        } else if (idx - _right_col_idx >= _right_table_data_types.size()) {
            return Status::InternalError(
                    "Join node(id={}, OP={}) intermediate slot({}, #{})'s on right table "
                    "idx out bound of _right_table_data_types: {} vs {}(idx = {}, _right_col_idx = "
                    "{})",
                    _node_id, _join_op, slot->col_name(), slot->id(), idx - _right_col_idx,
                    _right_table_data_types.size(), idx, _right_col_idx);
        }

        auto target_data_type = slot_on_left ? _left_table_data_types[idx]
                                             : _right_table_data_types[idx - _right_col_idx];
        ++idx;
        if (data_type->equals(*target_data_type)) {
            continue;
        }

        /// For outer join(left/right/full), the non-nullable columns may be converted to nullable.
        const auto accept_nullable_not_match =
                _join_op == TJoinOp::FULL_OUTER_JOIN ||
                (slot_on_left ? _join_op == TJoinOp::RIGHT_OUTER_JOIN
                              : _join_op == TJoinOp::LEFT_OUTER_JOIN);

        if (accept_nullable_not_match) {
            auto data_type_non_nullable = vectorized::remove_nullable(data_type);
            if (data_type_non_nullable->equals(*target_data_type)) {
                continue;
            }
        } else if (data_type->equals(*target_data_type)) {
            continue;
        }

        return Status::InternalError(
                "Join node(id={}, OP={}) intermediate slot({}, #{})'s on {} table data type not "
                "match: '{}' vs '{}'",
                _node_id, _join_op, slot->col_name(), slot->id(), (slot_on_left ? "left" : "right"),
                data_type->get_name(), target_data_type->get_name());
    }

    _build_side_child.reset();
    RETURN_IF_ERROR(vectorized::VExpr::open(_probe_expr_ctxs, state));
    for (auto& conjunct : _other_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }

    for (auto& conjunct : _mark_join_conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }

    return Status::OK();
}

} // namespace doris::pipeline
