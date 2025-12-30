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

#include "vec/exec/scan/scanner.h"

#include <glog/logging.h>

#include "common/config.h"
#include "common/status.h"
#include "pipeline/exec/scan_operator.h"
#include "runtime/descriptors.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "vec/columns/column_nothing.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exec/scan/scan_node.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

Scanner::Scanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state, int64_t limit,
                 RuntimeProfile* profile)
        : _state(state),
          _local_state(local_state),
          _limit(limit),
          _profile(profile),
          _output_tuple_desc(_local_state->output_tuple_desc()),
          _output_row_descriptor(_local_state->_parent->output_row_descriptor()),
          _has_prepared(false) {
    DorisMetrics::instance()->scanner_cnt->increment(1);
}

Status Scanner::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    if (!conjuncts.empty()) {
        _conjuncts.resize(conjuncts.size());
        for (size_t i = 0; i != conjuncts.size(); ++i) {
            RETURN_IF_ERROR(conjuncts[i]->clone(state, _conjuncts[i]));
        }
    }

    const auto& projections = _local_state->_projections;
    if (!projections.empty()) {
        _projections.resize(projections.size());
        for (size_t i = 0; i != projections.size(); ++i) {
            RETURN_IF_ERROR(projections[i]->clone(state, _projections[i]));
        }
    }

    const auto& intermediate_projections = _local_state->_intermediate_projections;
    if (!intermediate_projections.empty()) {
        _intermediate_projections.resize(intermediate_projections.size());
        for (int i = 0; i < intermediate_projections.size(); i++) {
            _intermediate_projections[i].resize(intermediate_projections[i].size());
            for (int j = 0; j < intermediate_projections[i].size(); j++) {
                RETURN_IF_ERROR(intermediate_projections[i][j]->clone(
                        state, _intermediate_projections[i][j]));
            }
        }
    }

    return Status::OK();
}

Status Scanner::get_block_after_projects(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& row_descriptor = _local_state->_parent->row_descriptor();
    if (_output_row_descriptor) {
        _origin_block.clear_column_data(row_descriptor.num_materialized_slots());
        RETURN_IF_ERROR(get_block(state, &_origin_block, eos));
        return _do_projections(&_origin_block, block);
    } else {
        return get_block(state, block, eos);
    }
}

// Scanner读取数据的核心入口函数
// state: 运行时状态
// block: 输出数据块，用于存储读取的数据
// eof: 输出参数，标记是否读取结束
Status Scanner::get_block(RuntimeState* state, Block* block, bool* eof) {
    // 输入的block必须是空的
    DCHECK(block->rows() == 0);
    // 计时：scanner整体运行时间
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    // 单次调度最多读取的行数阈值，避免单个scanner长时间占用线程
    int64_t rows_read_threshold = _num_rows_read + config::doris_scanner_row_num;
    // 如果block不能复用内存，则按输出schema初始化列
    if (!block->mem_reuse()) {
        for (auto* const slot_desc : _output_tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
    }

    {
        // 循环读取数据，直到满足退出条件
        do {
            // 1. 从底层存储读取数据到block
            {
                SCOPED_TIMER(_local_state->_scan_timer);
                // 调用子类实现的_get_block_impl读取数据（如OlapScanner从tablet读取）
                RETURN_IF_ERROR(_get_block_impl(state, block, eof));
                if (*eof) {
                    DCHECK(block->rows() == 0);
                    break;
                }
                // 累计已读取的行数和字节数
                _num_rows_read += block->rows();
                _num_byte_read += block->allocated_bytes();
            }

            // 2. 对读取的数据进行过滤（如WHERE条件过滤）
            {
                SCOPED_TIMER(_local_state->_filter_timer);
                RETURN_IF_ERROR(_filter_output_block(block));
            }
            // 记录过滤后返回的行数，用于limit检查
            _num_rows_return += block->rows();
        } while (!_should_stop &&              // 未被标记停止
                 !state->is_cancelled() &&     // 查询未被取消
                 block->rows() == 0 &&         // 当前block为空（被过滤掉了），继续读
                 !(*eof) &&                    // 未到文件末尾
                 _num_rows_read < rows_read_threshold);  // 未超过单次读取行数阈值
    }

    // 检查查询是否被取消
    if (state->is_cancelled()) {
        return Status::Cancelled("cancelled");
    }
    *eof = *eof || _should_stop;
    // 检查是否达到scanner级别的limit限制
    // 用于 ORDER BY key LIMIT n 这类查询的优化
    *eof = *eof || (_limit > 0 && _num_rows_return >= _limit);

    return Status::OK();
}

Status Scanner::_filter_output_block(Block* block) {
    auto old_rows = block->rows();
    Status st = VExprContext::filter_block(_conjuncts, block, block->columns());
    _counter.num_rows_unselected += old_rows - block->rows();
    return st;
}

// 执行投影操作，将origin_block中的数据投影到output_block
// 
// 例如SQL: SELECT a, (b+c)*d AS result FROM table WHERE e > 10
// 表有列: a, b, c, d, e
// 
// origin_block: 存储层读取的原始数据，包含列 [a, b, c, d]（e用于过滤已处理）
// output_block: 最终输出，只包含 [a, result] 两列
// 
// _intermediate_projections: 中间投影表达式列表，用于分步计算复杂表达式
//   例如第一步: [a, b+c, d] -> 先计算b+c得到临时列tmp
// _projections: 最终投影表达式列表，生成SELECT中的输出列
//   例如: [a, tmp*d] -> 输出 [a, result]
Status Scanner::_do_projections(vectorized::Block* origin_block, vectorized::Block* output_block) {
    SCOPED_RAW_TIMER(&_per_scanner_timer);
    SCOPED_RAW_TIMER(&_projection_timer);

    const size_t rows = origin_block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    // 复制原始block作为输入
    // 例如: input_block = [a, b, c, d]
    vectorized::Block input_block = *origin_block;

    // ========== 执行中间投影 ==========
    // 处理复杂表达式的分步计算
    // 例如 (b+c)*d 需要先算 b+c，再乘 d
    std::vector<int> result_column_ids;
    for (auto& projections : _intermediate_projections) {
        result_column_ids.resize(projections.size());
        // 执行每个投影表达式，结果列ID存入result_column_ids
        // 例如: 执行 [a, b+c, d]，b+c的结果追加到block末尾
        for (int i = 0; i < projections.size(); i++) {
            RETURN_IF_ERROR(projections[i]->execute(&input_block, &result_column_ids[i]));
        }
        // 重排列顺序，只保留投影结果列
        // 例如: [a, b, c, d, tmp] -> shuffle后变成 [a, tmp, d]
        input_block.shuffle_columns(result_column_ids);
    }

    DCHECK_EQ(rows, input_block.rows());
    // 构建可复用的输出block，按照输出schema初始化列
    // 例如: output_block的schema是 [a, result]
    MutableBlock mutable_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_output_row_descriptor);

    auto& mutable_columns = mutable_block.mutable_columns();

    DCHECK_EQ(mutable_columns.size(), _projections.size());

    // ========== 执行最终投影 ==========
    // _projections[0] = a (直接引用列a)
    // _projections[1] = tmp*d (计算表达式得到result)
    for (int i = 0; i < mutable_columns.size(); ++i) {
        ColumnPtr column_ptr;
        // 执行投影表达式计算
        // 例如: _projections[1]->execute 计算 tmp*d
        RETURN_IF_ERROR(_projections[i]->execute(&input_block, column_ptr));
        // 如果是常量列（如 SELECT 1），转换为完整列
        column_ptr = column_ptr->convert_to_full_column_if_const();
        // 检查nullable属性一致性
        if (mutable_columns[i]->is_nullable() != column_ptr->is_nullable()) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Nullable mismatch");
        }
        // 将计算结果插入到输出列
        // 例如: mutable_columns[1]插入result列的所有行
        mutable_columns[i]->insert_range_from(*column_ptr, 0, rows);
    }
    DCHECK(mutable_block.rows() == rows);
    // 设置输出block的列，最终 output_block = [a, result]
    output_block->set_columns(std::move(mutable_columns));

    return Status::OK();
}

Status Scanner::try_append_late_arrival_runtime_filter() {
    if (_applied_rf_num == _total_rf_num) {
        return Status::OK();
    }
    DCHECK(_applied_rf_num < _total_rf_num);

    int arrived_rf_num = 0;
    RETURN_IF_ERROR(_local_state->_helper.try_append_late_arrival_runtime_filter(
            _state, &arrived_rf_num, _local_state->_conjuncts,
            _local_state->_parent->row_descriptor()));

    if (arrived_rf_num == _applied_rf_num) {
        // No newly arrived runtime filters, just return;
        return Status::OK();
    }

    // There are newly arrived runtime filters,
    // renew the _conjuncts
    if (!_conjuncts.empty()) {
        _discard_conjuncts();
    }
    // Notice that the number of runtime filters may be larger than _applied_rf_num.
    // But it is ok because it will be updated at next time.
    RETURN_IF_ERROR(_local_state->clone_conjunct_ctxs(_conjuncts));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

Status Scanner::close(RuntimeState* state) {
#ifndef BE_TEST
    COUNTER_UPDATE(_local_state->_scanner_wait_worker_timer, _scanner_wait_worker_timer);
#endif
    return Status::OK();
}

bool Scanner::_try_close() {
    bool expected = false;
    return _is_closed.compare_exchange_strong(expected, true);
}

void Scanner::_collect_profile_before_close() {
    COUNTER_UPDATE(_local_state->_scan_cpu_timer, _scan_cpu_timer);
    COUNTER_UPDATE(_local_state->_rows_read_counter, _num_rows_read);

    // Update stats for load
    _state->update_num_rows_load_filtered(_counter.num_rows_filtered);
    _state->update_num_rows_load_unselected(_counter.num_rows_unselected);
}

void Scanner::update_scan_cpu_timer() {
    int64_t cpu_time = _cpu_watch.elapsed_time();
    _scan_cpu_timer += cpu_time;
    if (_state && _state->get_query_ctx()) {
        _state->get_query_ctx()->resource_ctx()->cpu_context()->update_cpu_cost_ms(cpu_time);
    }
}

} // namespace doris::vectorized
