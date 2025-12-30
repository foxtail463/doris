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

#include "aggregation_source_operator.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "pipeline/exec/operator.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

AggLocalState::AggLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}

Status AggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _get_results_timer = ADD_TIMER(custom_profile(), "GetResultsTime");
    _hash_table_iterate_timer = ADD_TIMER(custom_profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(custom_profile(), "InsertKeysToColumnTime");
    _insert_values_to_column_timer = ADD_TIMER(custom_profile(), "InsertValuesToColumnTime");

    _merge_timer = ADD_TIMER(Base::custom_profile(), "MergeTime");
    _deserialize_data_timer = ADD_TIMER(Base::custom_profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
    _hash_table_input_counter =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT, 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "MemoryUsageHashTable", TUnit::BYTES, 1);
    _hash_table_size_counter =
            ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "HashTableSize", TUnit::UNIT, 1);

    _memory_usage_container = ADD_COUNTER(custom_profile(), "MemoryUsageContainer", TUnit::BYTES);
    _memory_usage_arena = ADD_COUNTER(custom_profile(), "MemoryUsageArena", TUnit::BYTES);

    // 绑定导出策略：根据“是否有 key / 是否需要 finalize”绑定到不同的 get_result 实现
    // 无 key: _get_results_without_key / _get_without_key_result
    // 有 key: _get_results_with_serialized_key / _get_with_serialized_key_result
    auto& p = _parent->template cast<AggSourceOperatorX>();
    if (p._without_key) {
        if (p._needs_finalize) {
            _executor.get_result = [this](RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
                return _get_without_key_result(state, block, eos);
            };
        } else {
            _executor.get_result = [this](RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
                return _get_results_without_key(state, block, eos);
            };
        }
    } else {
        if (p._needs_finalize) {
            _executor.get_result = [this](RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
                return _get_with_serialized_key_result(state, block, eos);
            };
        } else {
            _executor.get_result = [this](RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
                return _get_results_with_serialized_key(state, block, eos);
            };
        }
    }

    return Status::OK();
}

Status AggLocalState::_create_agg_status(vectorized::AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        try {
            shared_state.aggregate_evaluators[i]->create(
                    data + shared_state.offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                shared_state.aggregate_evaluators[j]->destroy(
                        data + shared_state.offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

/**
 * 有键聚合结果获取函数（不需要最终化处理）
 * 
 * 功能说明：
 * 1. 从哈希表中遍历聚合结果，批量获取数据
 * 2. 将GROUP BY键和聚合值序列化到输出列中
 * 3. 处理NULL键值和内存优化
 * 4. 构建最终的数据块输出
 * 
 * 适用场景：
 * - 有GROUP BY子句的聚合查询（如 SELECT COUNT(*) FROM table GROUP BY id）
 * - 单级聚合，不需要最终化处理
 * - 最常见的OLAP聚合场景
 * 
 * @param state 运行时状态对象
 * @param block 输出数据块，用于存储聚合结果
 * @param eos 结束标志，指示是否已处理完所有数据
 * @return Status 操作状态
 */
Status AggLocalState::_get_results_with_serialized_key(RuntimeState* state,
                                                       vectorized::Block* block, bool* eos) {
    // 第一步：设置性能监控计时器
    SCOPED_TIMER(_get_results_timer);
    
    // 第二步：获取共享状态和基本参数
    auto& shared_state = *_shared_state;
    size_t key_size = _shared_state->probe_expr_ctxs.size();      // GROUP BY列的数量
    size_t agg_size = _shared_state->aggregate_evaluators.size(); // 聚合函数的数量
    
    // 第三步：准备输出列的数据结构
    vectorized::MutableColumns value_columns(agg_size);  // 聚合值列
    vectorized::DataTypes value_data_types(agg_size);    // 聚合值的数据类型

    // 第四步：判断是否可以复用内存
    // 当没有需要转换为可空的键且输出块支持内存复用时，可以复用内存以提高性能
    bool mem_reuse = shared_state.make_nullable_keys.empty() && block->mem_reuse();

    // 第五步：准备GROUP BY键列
    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            // 内存复用：直接使用输出块中现有的列内存
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            // 创建新列：根据表达式类型创建新的列
            key_columns.emplace_back(
                    shared_state.probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    // 第六步：使用访问者模式处理不同类型的聚合方法
    std::visit(
            vectorized::Overload {
                    // 处理未初始化的哈希表（错误情况）
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    // 处理具体的聚合方法（模板函数）
                    [&](auto& agg_method) -> void {
                        // 6.1 初始化哈希表迭代器
                        agg_method.init_iterator();
                        auto& data = *agg_method.hash_table;
                        
                        // 6.2 计算本次处理的数据量（批量处理，避免内存过大）
                        const auto size = std::min(data.size(), size_t(state->batch_size()));
                        using KeyType = std::decay_t<decltype(agg_method)>::Key;
                        std::vector<KeyType> keys(size);  // 存储键值的临时数组
                        
                        // 6.3 确保聚合数据容器有足够的空间
                        if (shared_state.values.size() < size + 1) {
                            shared_state.values.resize(size + 1);
                        }

                        uint32_t num_rows = 0;  // 实际处理的行数
                        shared_state.aggregate_data_container->init_once();
                        auto& iter = shared_state.aggregate_data_container->iterator;

                        // 6.4 遍历哈希表，收集键值和聚合数据
                        {
                            SCOPED_TIMER(_hash_table_iterate_timer);
                            while (iter != shared_state.aggregate_data_container->end() &&
                                   num_rows < state->batch_size()) {
                                // 获取当前迭代器位置的键值
                                keys[num_rows] = iter.template get_key<KeyType>();
                                // 获取当前迭代器位置的聚合数据
                                shared_state.values[num_rows] = iter.get_aggregate_data();
                                ++iter;      // 移动到下一个位置
                                ++num_rows;  // 增加处理行数
                            }
                        }

                        // 6.5 将键值插入到输出列中
                        {
                            SCOPED_TIMER(_insert_keys_to_column_timer);
                            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                        }

                        // 6.6 处理哈希表遍历结束后的特殊情况
                        if (iter == shared_state.aggregate_data_container->end()) {
                            if (agg_method.hash_table->has_null_key_data()) {
                                // 处理GROUP BY中的NULL键值
                                // 注意：只有单个GROUP BY列才支持NULL键包装
                                // 这里需要额外的NULL键/值处理逻辑
                                DCHECK(key_columns.size() == 1);
                                DCHECK(key_columns[0]->is_nullable());
                                if (agg_method.hash_table->has_null_key_data()) {
                                    // 插入NULL键值
                                    key_columns[0]->insert_data(nullptr, 0);
                                    // 获取NULL键对应的聚合数据
                                    shared_state.values[num_rows] =
                                            agg_method.hash_table->template get_null_key_data<
                                                    vectorized::AggregateDataPtr>();
                                    ++num_rows;
                                    *eos = true;  // 标记数据流结束
                                }
                            } else {
                                *eos = true;  // 没有更多数据，标记结束
                            }
                        }

                        // 6.7 将聚合值序列化到输出列中
                        {
                            SCOPED_TIMER(_insert_values_to_column_timer);
                            for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
                                // 获取聚合函数的序列化类型
                                value_data_types[i] = shared_state.aggregate_evaluators[i]
                                                              ->function()
                                                              ->get_serialized_type();
                                
                                // 准备聚合值列
                                if (mem_reuse) {
                                    // 内存复用：使用输出块中现有的列
                                    value_columns[i] =
                                            std::move(*block->get_by_position(i + key_size).column)
                                                    .mutate();
                                } else {
                                    // 创建新的序列化列
                                    value_columns[i] = shared_state.aggregate_evaluators[i]
                                                               ->function()
                                                               ->create_serialize_column();
                                }
                                
                                // 将聚合数据序列化到列中
                                shared_state.aggregate_evaluators[i]
                                        ->function()
                                        ->serialize_to_column(
                                                shared_state.values,                    // 聚合数据数组
                                                shared_state.offsets_of_aggregate_states[i], // 数据偏移量
                                                value_columns[i],                       // 目标列
                                                num_rows);                              // 行数
                            }
                        }
                    }},
            shared_state.agg_data->method_variant);  // 聚合方法的变体类型

    // 第七步：构建最终的数据块（仅在非内存复用模式下）
    if (!mem_reuse) {
        vectorized::ColumnsWithTypeAndName columns_with_schema;
        
        // 7.1 添加GROUP BY键列
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             shared_state.probe_expr_ctxs[i]->root()->data_type(),
                                             shared_state.probe_expr_ctxs[i]->root()->expr_name());
        }
        
        // 7.2 添加聚合值列
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
        }
        
        // 7.3 构建最终的数据块
        *block = vectorized::Block(columns_with_schema);
    }

    return Status::OK();
}

Status AggLocalState::_get_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                                      bool* eos) {
    auto& shared_state = *_shared_state;
    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = shared_state.make_nullable_keys.empty() && block->mem_reuse();

    auto columns_with_schema = vectorized::VectorizedUtils::create_columns_with_type_and_name(
            _parent->cast<AggSourceOperatorX>().row_descriptor());
    size_t key_size = shared_state.probe_expr_ctxs.size();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (!mem_reuse) {
            key_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }
    vectorized::MutableColumns value_columns;
    for (size_t i = key_size; i < columns_with_schema.size(); ++i) {
        if (!mem_reuse) {
            value_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            value_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    // 执行期统一分发：对不同键方法(method_variant)做零开销分发
    // 两阶段：
    // 1) 遍历容器，批量收集 (key, state) → keys[] / values[]
    // 2) 写出 key 列(insert_keys_into_columns) 与 序列化聚合值(serialize_to_column)
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        auto& data = *agg_method.hash_table;
                        agg_method.init_iterator();
                        const auto size = std::min(data.size(), size_t(state->batch_size()));
                        using KeyType = std::decay_t<decltype(agg_method)>::Key;
                        std::vector<KeyType> keys(size);
                        if (shared_state.values.size() < size) {
                            shared_state.values.resize(size);
                        }

                        uint32_t num_rows = 0;
                        shared_state.aggregate_data_container->init_once();
                        auto& iter = shared_state.aggregate_data_container->iterator;

                        {
                            SCOPED_TIMER(_hash_table_iterate_timer);
                            while (iter != shared_state.aggregate_data_container->end() &&
                                   num_rows < state->batch_size()) {
                                keys[num_rows] = iter.template get_key<KeyType>();
                                shared_state.values[num_rows] = iter.get_aggregate_data();
                                ++iter;
                                ++num_rows;
                            }
                        }

                        {
                            SCOPED_TIMER(_insert_keys_to_column_timer);
                            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                        }

                        for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
                            shared_state.aggregate_evaluators[i]->insert_result_info_vec(
                                    shared_state.values,
                                    shared_state.offsets_of_aggregate_states[i],
                                    value_columns[i].get(), num_rows);
                        }

                        if (iter == shared_state.aggregate_data_container->end()) {
                            if (agg_method.hash_table->has_null_key_data()) {
                                // only one key of group by support wrap null key
                                // here need additional processing logic on the null key / value
                                DCHECK(key_columns.size() == 1);
                                DCHECK(key_columns[0]->is_nullable());
                                if (key_columns[0]->size() < state->batch_size()) {
                                    key_columns[0]->insert_data(nullptr, 0);
                                    auto mapped = agg_method.hash_table->template get_null_key_data<
                                            vectorized::AggregateDataPtr>();
                                    for (size_t i = 0; i < shared_state.aggregate_evaluators.size();
                                         ++i)
                                        shared_state.aggregate_evaluators[i]->insert_result_info(
                                                mapped +
                                                        shared_state.offsets_of_aggregate_states[i],
                                                value_columns[i].get());
                                    *eos = true;
                                }
                            } else {
                                *eos = true;
                            }
                        }
                    }},
            shared_state.agg_data->method_variant);

    if (!mem_reuse) {
        *block = columns_with_schema;
        vectorized::MutableColumns columns(block->columns());
        for (int i = 0; i < block->columns(); ++i) {
            if (i < key_size) {
                columns[i] = std::move(key_columns[i]);
            } else {
                columns[i] = std::move(value_columns[i - key_size]);
            }
        }
        block->set_columns(std::move(columns));
    }

    return Status::OK();
}

Status AggLocalState::_get_results_without_key(RuntimeState* state, vectorized::Block* block,
                                               bool* eos) {
    SCOPED_TIMER(_get_results_timer);
    auto& shared_state = *_shared_state;
    // 1. `child(0)->rows_returned() == 0` mean not data from child
    // in level two aggregation node should return NULL result
    //    level one aggregation node set `eos = true` return directly
    if (UNLIKELY(_shared_state->input_num_rows == 0)) {
        *eos = true;
        return Status::OK();
    }
    block->clear();

    DCHECK(shared_state.agg_data->without_key != nullptr);
    size_t agg_size = shared_state.aggregate_evaluators.size();

    vectorized::MutableColumns value_columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    // will serialize data to string column
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        data_types[i] = shared_state.aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] =
                shared_state.aggregate_evaluators[i]->function()->create_serialize_column();
    }

    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        shared_state.aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                shared_state.agg_data->without_key + shared_state.offsets_of_aggregate_states[i],
                *value_columns[i]);
    }

    {
        vectorized::ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
            vectorized::ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = vectorized::Block(data_with_schema);
    }

    block->set_columns(std::move(value_columns));
    *eos = true;
    return Status::OK();
}

Status AggLocalState::_get_without_key_result(RuntimeState* state, vectorized::Block* block,
                                              bool* eos) {
    auto& shared_state = *_shared_state;
    DCHECK(_shared_state->agg_data->without_key != nullptr);
    block->clear();

    auto& p = _parent->cast<AggSourceOperatorX>();
    *block = vectorized::VectorizedUtils::create_empty_columnswithtypename(p.row_descriptor());
    size_t agg_size = shared_state.aggregate_evaluators.size();

    vectorized::MutableColumns columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        data_types[i] = shared_state.aggregate_evaluators[i]->function()->get_return_type();
        columns[i] = data_types[i]->create_column();
    }

    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        auto column = columns[i].get();
        shared_state.aggregate_evaluators[i]->insert_result_info(
                shared_state.agg_data->without_key + shared_state.offsets_of_aggregate_states[i],
                column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (column_type->get_primitive_type() != TYPE_ARRAY) {
                if (!column_type->is_nullable() || data_types[i]->is_nullable() ||
                    !remove_nullable(column_type)->equals(*data_types[i])) {
                    return Status::InternalError(
                            "node id = {}, column_type not match data_types, column_type={}, "
                            "data_types={}",
                            _parent->node_id(), column_type->get_name(), data_types[i]->get_name());
                }
            }

            // Result of operator is nullable, but aggregate function result is not nullable
            // this happens when:
            // 1. no group by
            // 2. input of aggregate function is empty
            // 3. all of input columns are not nullable
            if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                vectorized::ColumnPtr ptr = std::move(columns[i]);
                // unless `count`, other aggregate function dispose empty set should be null
                // so here check the children row return
                ptr = make_nullable(ptr, shared_state.input_num_rows == 0);
                columns[i] = ptr->assume_mutable();
            }
        }
    }

    block->set_columns(std::move(columns));
    *eos = true;
    return Status::OK();
}

AggSourceOperatorX::AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                       const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _needs_finalize(tnode.agg_node.need_finalize),
          _without_key(tnode.agg_node.grouping_exprs.empty()) {}

/**
 * 聚合源操作符获取数据块的核心方法
 * 
 * 功能说明：
 * 1. 从聚合执行器中获取聚合计算结果
 * 2. 处理输出键的可空性
 * 3. 应用HAVING子句过滤条件
 * 4. 应用LIMIT限制和分页处理
 * 
 * 在Push流程中的作用：
 * - 作为Source操作符，从共享聚合状态中拉取最终结果
 * - 与AggSinkOperatorX配合，实现完整的聚合Push流程
 * - 负责将聚合结果转换为用户可见的最终输出
 * 
 * @param state 运行时状态对象，包含查询执行的上下文信息
 * @param block 输出数据块，用于存储聚合后的最终结果
 * @param eos 结束标志，指示是否已处理完所有聚合数据
 * @return Status 操作状态，成功返回OK，失败返回错误状态
 */
Status AggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    // 第一步：获取当前操作符的本地状态
    // 每个PipelineTask都有独立的本地状态实例，包含聚合执行器、内存统计等信息
    auto& local_state = get_local_state(state);
    
    // 第二步：设置性能监控计时器
    // 记录函数执行时间，用于性能分析和优化
    SCOPED_TIMER(local_state.exec_time_counter());
    
    // 第三步：设置内存使用监控
    // 记录内存使用峰值，用于内存管理和溢出检测
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    
    // 第四步：从聚合执行器获取聚合结果
    // 这是核心步骤，从共享聚合状态中获取计算完成的聚合数据
    // _executor是聚合执行器，负责实际的聚合计算（GROUP BY, COUNT, SUM等）
    RETURN_IF_ERROR(local_state._executor.get_result(state, block, eos));
    
    // 第五步：处理输出键的可空性
    // 将GROUP BY列设置为可空类型，处理可能为NULL的键值
    // 确保输出格式符合SQL语义和用户预期
    local_state.make_nullable_output_key(block);
    
    // 第六步：应用HAVING子句过滤条件
    // 根据HAVING条件过滤聚合结果，只保留满足条件的分组
    // 注意：HAVING子句不应该在预流式聚合中执行，因为此时聚合还未完成
    RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block, block->columns()));
    
    // 第七步：应用LIMIT限制和分页处理
    // 处理聚合查询的LIMIT子句，实现分页查询功能
    // 可能修改eos标志，表示是否还有更多数据需要处理
    local_state.do_agg_limit(block, eos);
    
    return Status::OK();
}

void AggLocalState::do_agg_limit(vectorized::Block* block, bool* eos) {
    if (_shared_state->reach_limit) {
        if (_shared_state->do_sort_limit && _shared_state->do_limit_filter(block, block->rows())) {
            vectorized::Block::filter_block_internal(block, _shared_state->need_computes);
            if (auto rows = block->rows()) {
                _num_rows_returned += rows;
            }
        } else {
            reached_limit(block, eos);
        }
    } else {
        if (auto rows = block->rows()) {
            _num_rows_returned += rows;
        }
    }
}

void AggLocalState::make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _shared_state->make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

Status AggLocalState::merge_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);
    SCOPED_PEAK_MEM(&_estimate_memory_usage);

    size_t key_size = Base::_shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        key_columns[i] = block->get_by_position(i).column.get();
    }

    uint32_t rows = (uint32_t)block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    _emplace_into_hash_table(_places.data(), key_columns, rows);

    for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
        auto col_id = Base::_shared_state->probe_expr_ctxs.size() + i;
        auto column = block->get_by_position(col_id).column;
        if (column->is_nullable()) {
            column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
        }

        size_t buffer_size =
                Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() * rows;
        if (_deserialize_buffer.size() < buffer_size) {
            _deserialize_buffer.resize(buffer_size);
        }

        {
            SCOPED_TIMER(_deserialize_data_timer);
            Base::_shared_state->aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                    _places.data(), _shared_state->offsets_of_aggregate_states[i],
                    _deserialize_buffer.data(), column.get(), _agg_arena_pool, rows);
        }
    }

    return Status::OK();
}

Status AggSourceOperatorX::merge_with_serialized_key_helper(RuntimeState* state,
                                                            vectorized::Block* block) {
    auto& local_state = get_local_state(state);
    return local_state.merge_with_serialized_key_helper(block);
}

size_t AggSourceOperatorX::get_estimated_memory_size_for_merging(RuntimeState* state,
                                                                 size_t rows) const {
    auto& local_state = get_local_state(state);
    size_t size = std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> size_t {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                        return 0;
                    },
                    [&](auto& agg_method) { return agg_method.hash_table->estimate_memory(rows); }},
            local_state._shared_state->agg_data->method_variant);
    size += local_state._shared_state->aggregate_data_container->estimate_memory(rows);
    return size;
}

void AggLocalState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                             vectorized::ColumnRawPtrs& key_columns,
                                             uint32_t num_rows) {
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        SCOPED_TIMER(_hash_table_compute_timer);
                        using HashMethodType = std::decay_t<decltype(agg_method)>;
                        using AggState = typename HashMethodType::State;
                        AggState state(key_columns);
                        agg_method.init_serialized_keys(key_columns, num_rows);

                        auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                            HashMethodType::try_presis_key_and_origin(key, origin, _agg_arena_pool);
                            auto mapped =
                                    Base::_shared_state->aggregate_data_container->append_data(
                                            origin);
                            auto st = _create_agg_status(mapped);
                            if (!st) {
                                throw Exception(st.code(), st.to_string());
                            }
                            ctor(key, mapped);
                        };

                        auto creator_for_null_key = [&](auto& mapped) {
                            mapped = _agg_arena_pool.aligned_alloc(
                                    _shared_state->total_size_of_aggregate_states,
                                    _shared_state->align_aggregate_states);
                            auto st = _create_agg_status(mapped);
                            if (!st) {
                                throw Exception(st.code(), st.to_string());
                            }
                        };

                        SCOPED_TIMER(_hash_table_emplace_timer);
                        for (size_t i = 0; i < num_rows; ++i) {
                            places[i] = *agg_method.lazy_emplace(state, i, creator,
                                                                 creator_for_null_key);
                        }

                        COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                        COUNTER_SET(_hash_table_memory_usage,
                                    static_cast<int64_t>(
                                            agg_method.hash_table->get_buffer_size_in_bytes()));
                        COUNTER_SET(_hash_table_size_counter,
                                    static_cast<int64_t>(agg_method.hash_table->size()));
                        COUNTER_SET(
                                _memory_usage_container,
                                static_cast<int64_t>(
                                        _shared_state->aggregate_data_container->memory_usage()));
                        COUNTER_SET(_memory_usage_arena,
                                    static_cast<int64_t>(_agg_arena_pool.size()));
                    }},
            _shared_state->agg_data->method_variant);
}

Status AggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);
    return Base::close(state);
}

} // namespace doris::pipeline
