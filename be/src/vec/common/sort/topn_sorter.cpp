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

#include "vec/common/sort/topn_sorter.h"

#include <glog/logging.h>

#include <algorithm>
#include <queue>

#include "common/object_pool.h"
#include "vec/core/block.h"
#include "vec/core/sort_cursor.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class VSortExecExprs;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

TopNSorter::TopNSorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       RuntimeState* state, RuntimeProfile* profile)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset)),
          _row_desc(row_desc) {}

Status TopNSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    RETURN_IF_ERROR(_do_sort(block));
    return Status::OK();
}

Status TopNSorter::prepare_for_read(bool is_spill) {
    if (is_spill) {
        return Status::InternalError("TopN sorter does not support spill");
    }
    return _state->build_merge_tree(_sort_description);
}

Status TopNSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(block, state->batch_size(), eos);
}

Status TopNSorter::_do_sort(Block* block) {
    // 创建一个空的排序后数据块，用于存储排序结果
    // 使用行描述符创建具有正确列类型的空数据块
    Block sorted_block = VectorizedUtils::create_empty_columnswithtypename(_row_desc);
    
    // 对输入数据块进行局部排序
    // partial_sort 函数会对当前数据块内的数据进行排序，但不保证全局有序
    RETURN_IF_ERROR(partial_sort(*block, sorted_block));

    // 处理 TOP-N 逻辑
    // 这里实现了一个内存优化策略，通过构建最大堆来管理排序后的数据块
    if (_limit != -1) {
        // 优化说明：为了减少内存使用，我们构建一个最大堆来管理 _block_priority_queue 中的数据块
        // 如果某个数据块完全大于堆顶元素，我们可以直接丢弃该数据块
        
        // 检查当前已排序的行数是否小于 offset + limit
        // 如果小于，说明还需要更多数据，直接添加当前排序后的数据块
        if (_state->num_rows() < _offset + _limit) {
            // 将排序后的数据块添加到状态中
            _state->add_sorted_block(Block::create_shared(std::move(sorted_block)));
            
            // 创建合并排序游标实现，并将其添加到优先队列中
            // 优先队列会根据排序描述符维护数据块的顺序
            _block_priority_queue.emplace(MergeSortCursorImpl::create_shared(
                    _state->last_sorted_block(), _sort_description));
        } else {
            // 当前已排序的行数已达到或超过 offset + limit
            // 需要判断是否应该保留当前数据块
            
            // 创建临时游标实现，用于比较当前数据块与堆顶元素
            auto tmp_cursor_impl = MergeSortCursorImpl::create_shared(
                    Block::create_shared(std::move(sorted_block)), _sort_description);
            
            // 创建合并排序块游标，用于进行数据块级别的比较
            MergeSortBlockCursor block_cursor(tmp_cursor_impl);
            
            // 检查当前数据块是否完全大于堆顶元素
            // 如果不是完全大于，说明可能包含我们需要的前N个元素，需要保留
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                // 将数据块添加到状态中
                _state->add_sorted_block(block_cursor.impl->block);
                
                // 将游标实现添加到优先队列中
                _block_priority_queue.emplace(tmp_cursor_impl);
            }
            // 如果当前数据块完全大于堆顶元素，直接丢弃，不进行任何操作
            // 这样可以有效减少内存使用
        }
    } else {
        // 如果没有设置 limit，不应该使用 TopN 排序器
        // 应该使用 FullSorter 来处理全排序查询
        return Status::InternalError("Should not reach TopN sorter for full sort query");
    }
    
    return Status::OK();
}

size_t TopNSorter::data_size() const {
    return _state->data_size();
}

} // namespace doris::vectorized
