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

#include "vec/common/sort/sorter.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {

// When doing spillable sorting, each sorted block is spilled into a single file.
//
// In order to decrease memory pressure when merging
// multiple spilled blocks into one bigger sorted block, only part
// of each spilled blocks are read back into memory at a time.
//
// Currently the spilled blocks are splitted into small sub blocks,
// each sub block is serialized in PBlock format and appended
// to the spill file.
//

void MergeSorterState::reset() {
    std::vector<std::shared_ptr<MergeSortCursorImpl>> empty_cursors(0);
    std::vector<std::shared_ptr<Block>> empty_blocks(0);
    _sorted_blocks.swap(empty_blocks);
    unsorted_block() = Block::create_unique(unsorted_block()->clone_empty());
    _in_mem_sorted_bocks_size = 0;
}

void MergeSorterState::add_sorted_block(std::shared_ptr<Block> block) {
    auto rows = block->rows();
    if (0 == rows) {
        return;
    }
    _in_mem_sorted_bocks_size += block->bytes();
    _sorted_blocks.emplace_back(block);
    _num_rows += rows;
}

Status MergeSorterState::build_merge_tree(const SortDescription& sort_description) {
    std::vector<MergeSortCursor> cursors;
    for (auto& block : _sorted_blocks) {
        cursors.emplace_back(
                MergeSortCursorImpl::create_shared(std::move(block), sort_description));
    }
    _queue = MergeSorterQueue(cursors);

    _sorted_blocks.clear();
    return Status::OK();
}

Status MergeSorterState::merge_sort_read(doris::vectorized::Block* block, int batch_size,
                                         bool* eos) {
    DCHECK(_sorted_blocks.empty());
    DCHECK(unsorted_block()->empty());
    _merge_sort_read_impl(batch_size, block, eos);
    return Status::OK();
}

void MergeSorterState::_merge_sort_read_impl(int batch_size, doris::vectorized::Block* block,
                                             bool* eos) {
    // ===== 功能说明：多路归并排序（K-way Merge Sort） =====
    // 
    // 【核心功能】
    // 将多个已经排序好的数据块（Block）合并成一个全局有序的结果。
    // 
    // 【场景举例】
    // 假设我们有 3 个已经排序好的数据块，需要合并成一个全局有序的结果：
    // 
    // Block1: [1, 3, 5, 7, 9]      (已排序，按升序)
    // Block2: [2, 4, 6, 8, 10]     (已排序，按升序)
    // Block3: [1, 2, 11, 12]        (已排序，按升序)
    // 
    // 期望输出：全局有序的结果 [1, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    // 
    // 【工作原理】
    // 1. 使用优先队列（最小堆）维护每个块的"当前最小值"
    // 2. 每次从队列顶部取出最小值，写入输出
    // 3. 将该块的游标向前移动，如果块未读完，重新加入队列
    // 4. 重复直到所有块都读完
    //
    // 【执行过程示例】
    // 初始状态：队列顶部是 Block1[1], Block2[2], Block3[1] 中的最小值
    // 第1步：取出 Block1[1] 或 Block3[1]（都是1），假设取 Block1[1]
    //        输出：[1]，Block1 游标移动到 [3]
    // 第2步：取出 Block3[1]（最小值）
    //        输出：[1, 1]，Block3 游标移动到 [2]
    // 第3步：取出 Block2[2] 或 Block3[2]（都是2），假设取 Block2[2]
    //        输出：[1, 1, 2]，Block2 游标移动到 [4]
    // ... 以此类推

    // ===== 阶段 1：初始化输出 Block =====
    // 获取未排序 Block 的列数（用于确定输出 Block 的列结构）
    // 【举例】如果 Block 有 3 列（id, name, age），输出 Block 也需要 3 列
    size_t num_columns = unsorted_block()->columns();

    // 构建可变的 Block，重用内存以提高性能
    // build_mutable_mem_reuse_block 会：
    // 1. 如果 block 为空或结构不匹配，创建新的 MutableBlock
    // 2. 如果 block 结构匹配，重用现有内存，避免重复分配
    MutableBlock m_block = VectorizedUtils::build_mutable_mem_reuse_block(block, *unsorted_block());
    // 获取可变的列引用，用于写入合并后的数据
    MutableColumns& merged_columns = m_block.mutable_columns();

    // ===== 阶段 2：从归并排序队列中按顺序读取数据并合并 =====
    // merged_rows 记录已合并的行数，用于控制批次大小
    // 【举例】batch_size=1000，表示每次最多读取 1000 行
    size_t merged_rows = 0;
    
    // 循环条件：
    // - _queue.is_valid()：队列中还有数据（还有块未读完）
    // - merged_rows < batch_size：还未达到批次大小限制
    // 
    // 【举例说明循环过程】
    // 假设 batch_size=5，当前状态：
    //   Block1: [5, 7, 9] (游标在位置0，指向5)
    //   Block2: [6, 8, 10] (游标在位置0，指向6)
    //   Block3: [11, 12] (游标在位置0，指向11)
    //   队列顶部：Block1[5]（最小值）
    //
    // 第1次循环：
    //   current = Block1, current_rows = 3 (Block1还有3行)
    //   current_rows = min(3, 5-0) = 3
    //   读取 Block1 的 [5, 7, 9]，merged_rows = 3
    //   输出：[5, 7, 9]
    //   Block1 读完，从队列移除
    //
    // 第2次循环：
    //   current = Block2, current_rows = 3
    //   current_rows = min(3, 5-3) = 2 (只能再读2行)
    //   读取 Block2 的 [6, 8]，merged_rows = 5
    //   输出：[5, 7, 9, 6, 8]
    //   达到 batch_size，退出循环
    while (_queue.is_valid() && merged_rows < batch_size) {
        // 从优先队列中获取当前最小元素（已排序的块）
        // current：指向当前块的游标（MergeSortCursor），指向该块的"当前位置"
        // current_rows：当前块中从"当前位置"到"块末尾"的可用行数
        auto [current, current_rows] = _queue.current();
        
        // 限制本次处理的行数，确保不超过批次大小
        // 【举例】
        // batch_size=1000, merged_rows=800, current_rows=500
        // 则 current_rows = min(500, 1000-800) = 200
        // 说明：虽然当前块有500行可用，但只能读200行（因为批次限制）
        current_rows = std::min(current_rows, batch_size - merged_rows);

        // ===== 阶段 3：处理 OFFSET（跳过前面的行） =====
        // step：本次需要跳过的行数
        // 如果 _offset > 0，说明需要跳过一些行（对应 SQL 的 OFFSET 子句）
        // 
        // 【举例：OFFSET 的作用】
        // SQL: SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5
        // 含义：排序后，跳过前5行，然后取10行
        // 
        // 假设当前块 Block1: [1, 3, 5, 7, 9]，_offset=3
        // step = min(3, 5) = 3，跳过前3行 [1, 3, 5]
        // current_rows = 5 - 3 = 2，实际读取 [7, 9]
        // _offset = 3 - 3 = 0，offset 已处理完
        size_t step = std::min(_offset, current_rows);
        _offset -= step;  // 更新剩余的 offset
        current_rows -= step;  // 减去跳过的行数，得到实际需要读取的行数

        // ===== 阶段 4：将数据从当前块复制到合并后的列中 =====
        // 如果还有需要读取的行（current_rows > 0）
        if (current_rows) {
            // 遍历每一列，将数据从当前块的指定位置复制到合并列中
            // 
            // 【举例：数据复制过程】
            // 假设 Block 有 2 列：id 和 name
            // Block1: id=[5, 7, 9], name=['a', 'b', 'c']
            // current->impl->pos = 0（游标位置）
            // step = 0（没有 offset）
            // current_rows = 3（读取3行）
            // 
            // 第1列（id）：
            //   merged_columns[0]->insert_range_from(Block1.id列, 0, 3)
            //   结果：merged_columns[0] = [5, 7, 9]
            // 
            // 第2列（name）：
            //   merged_columns[1]->insert_range_from(Block1.name列, 0, 3)
            //   结果：merged_columns[1] = ['a', 'b', 'c']
            // 
            // 最终输出 Block: {id=[5,7,9], name=['a','b','c']}
            for (size_t i = 0; i < num_columns; ++i) {
                merged_columns[i]->insert_range_from(*current->impl->columns[i],
                                                     current->impl->pos + step, current_rows);
            }
            // 更新已合并的行数
            merged_rows += current_rows;
        }

        // ===== 阶段 5：更新队列状态 =====
        // 判断当前游标是否已经到达块的末尾
        // current_rows + step：本次处理的总行数（包括跳过的和读取的）
        // 
        // 【举例：队列更新】
        // 情况1：块未读完
        //   Block1: [5, 7, 9]，pos=0，current_rows=2，step=0
        //   读取了 [5, 7]，还剩 [9]
        //   is_last(2) = false，调用 _queue.next(2)
        //   游标移动到 pos=2（指向9），重新加入队列
        //
        // 情况2：块已读完
        //   Block1: [5, 7]，pos=0，current_rows=2，step=0
        //   读取了 [5, 7]，块已读完
        //   is_last(2) = true，调用 _queue.remove_top()
        //   从队列中移除 Block1，队列自动调整，下一个最小值移到顶部
        if (!current->impl->is_last(current_rows + step)) {
            // 如果还有剩余数据，将游标向前移动
            // 移动的距离是本次处理的总行数（包括跳过的 offset）
            _queue.next(current_rows + step);
        } else {
            // 如果当前块已经处理完毕，从队列中移除
            // 优先队列会自动调整，将下一个最小元素移到顶部
            _queue.remove_top();
        }
    }

    // ===== 阶段 6：设置输出 Block =====
    // 将合并后的列移动到输出 Block 中
    // 使用 std::move 避免不必要的拷贝，提高性能
    block->set_columns(std::move(merged_columns));
    // 设置结束标志：
    // - merged_rows == 0：说明没有读取到任何数据，表示数据已全部读取完毕
    // - merged_rows > 0：说明还有数据，需要继续读取
    // 
    // 【举例】
    // 如果 merged_rows = 0，说明队列已空，所有块都读完了，*eos = true
    // 如果 merged_rows = 1000，说明还有数据，*eos = false，下次继续调用
    *eos = merged_rows == 0;
}

Status Sorter::merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                         int batch_size, bool* eos) {
    return get_next(state, block, eos);
}

Status Sorter::partial_sort(Block& src_block, Block& dest_block, bool reversed) {
    size_t num_cols = src_block.columns();
    RETURN_IF_ERROR(_prepare_sort_columns(src_block, dest_block, reversed));
    {
        SCOPED_TIMER(_partial_sort_timer);
        uint64_t limit = reversed ? 0 : (_offset + _limit);
        sort_block(_materialize_sort_exprs ? dest_block : src_block, dest_block, _sort_description,
                   limit);
    }

    src_block.clear_column_data(num_cols);
    return Status::OK();
}

Status Sorter::_prepare_sort_columns(Block& src_block, Block& dest_block, bool reversed) {
    if (_materialize_sort_exprs) {
        auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
        ColumnsWithTypeAndName columns_data(output_tuple_expr_ctxs.size());
        for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&src_block, columns_data[i]));
        }

        Block new_block {columns_data};
        dest_block.swap(new_block);
    }

    _sort_description.resize(_vsort_exec_exprs.ordering_expr_ctxs().size());
    Block* result_block = _materialize_sort_exprs ? &dest_block : &src_block;
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(result_block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
        if (reversed) {
            _sort_description[i].direction *= -1;
        }
    }
    return Status::OK();
}

FullSorter::FullSorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       RuntimeState* state, RuntimeProfile* profile)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset)) {}

// check whether the unsorted block can hold more data from input block and no need to alloc new memory
bool FullSorter::has_enough_capacity(Block* input_block, Block* unsorted_block) const {
    DCHECK_EQ(input_block->columns(), unsorted_block->columns());
    for (auto i = 0; i < input_block->columns(); ++i) {
        if (!unsorted_block->get_by_position(i).column->has_enough_capacity(
                    *input_block->get_by_position(i).column)) {
            return false;
        }
    }
    return true;
}

size_t FullSorter::get_reserve_mem_size(RuntimeState* state, bool eos) const {
    size_t size_to_reserve = 0;
    const auto rows = _state->unsorted_block()->rows();
    if (rows != 0) {
        const auto bytes = _state->unsorted_block()->bytes();
        const auto allocated_bytes = _state->unsorted_block()->allocated_bytes();
        const auto bytes_per_row = bytes / rows;
        const auto estimated_size_of_next_block = bytes_per_row * state->batch_size();
        auto new_block_bytes = estimated_size_of_next_block + bytes;
        auto new_rows = rows + state->batch_size();
        // If the new size is greater than 85% of allocalted bytes, it maybe need to realloc.
        if ((new_block_bytes * 100 / allocated_bytes) >= 85) {
            size_to_reserve += (size_t)(allocated_bytes * 1.15);
        }
        auto sort = new_rows > _buffered_block_size || new_block_bytes > _buffered_block_bytes;
        if (sort) {
            // new column is created when doing sort, reserve average size of one column
            // for estimation
            size_to_reserve += new_block_bytes / _state->unsorted_block()->columns();

            // helping data structures used during sorting
            size_to_reserve += new_rows * sizeof(IColumn::Permutation::value_type);

            auto sort_columns_count = _vsort_exec_exprs.ordering_expr_ctxs().size();
            if (1 != sort_columns_count) {
                size_to_reserve += new_rows * sizeof(EqualRangeIterator);
            }
        }
    }
    return size_to_reserve;
}

Status FullSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);

    // iff have reach limit and the unsorted block capacity can't hold the block data size
    if (_reach_limit() && !has_enough_capacity(block, _state->unsorted_block().get())) {
        RETURN_IF_ERROR(_do_sort());
    }

    {
        SCOPED_TIMER(_merge_block_timer);
        const auto& data = _state->unsorted_block()->get_columns_with_type_and_name();
        const auto& arrival_data = block->get_columns_with_type_and_name();
        auto sz = block->rows();
        for (int i = 0; i < data.size(); ++i) {
            DCHECK(data[i].type->equals(*(arrival_data[i].type)))
                    << " type1: " << data[i].type->get_name()
                    << " type2: " << arrival_data[i].type->get_name() << " i: " << i;
            if (is_column_const(*arrival_data[i].column)) {
                data[i].column->assume_mutable()->insert_many_from(
                        assert_cast<const ColumnConst*>(arrival_data[i].column.get())
                                ->get_data_column(),
                        0, sz);
            } else {
                data[i].column->assume_mutable()->insert_range_from(*arrival_data[i].column, 0, sz);
            }
        }
        block->clear_column_data();
    }
    return Status::OK();
}

Status FullSorter::prepare_for_read(bool is_spill) {
    if (is_spill) {
        _limit += _offset;
        _offset = 0;
        _state->ignore_offset();
    }
    if (_state->unsorted_block()->rows() > 0) {
        RETURN_IF_ERROR(_do_sort());
    }
    return _state->build_merge_tree(_sort_description);
}

Status FullSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(block, state->batch_size(), eos);
}

Status FullSorter::merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                             int batch_size, bool* eos) {
    return _state->merge_sort_read(block, batch_size, eos);
}

Status FullSorter::_do_sort() {
    Block* src_block = _state->unsorted_block().get();
    Block desc_block = src_block->clone_without_columns();
    COUNTER_UPDATE(_partial_sort_counter, 1);
    RETURN_IF_ERROR(partial_sort(*src_block, desc_block));

    // dispose TOP-N logic
    if (_limit != -1 && !_enable_spill) {
        // Here is a little opt to reduce the mem usage, we build a max heap
        // to order the block in _block_priority_queue.
        // if one block totally greater the heap top of _block_priority_queue
        // we can throw the block data directly.
        if (_state->num_rows() < _offset + _limit) {
            _state->add_sorted_block(Block::create_shared(std::move(desc_block)));
            _block_priority_queue.emplace(MergeSortCursorImpl::create_shared(
                    _state->last_sorted_block(), _sort_description));
        } else {
            auto tmp_cursor_impl = MergeSortCursorImpl::create_shared(
                    Block::create_shared(std::move(desc_block)), _sort_description);
            MergeSortBlockCursor block_cursor(tmp_cursor_impl);
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                _state->add_sorted_block(tmp_cursor_impl->block);
                _block_priority_queue.emplace(MergeSortCursorImpl::create_shared(
                        _state->last_sorted_block(), _sort_description));
            }
        }
    } else {
        // dispose normal sort logic
        _state->add_sorted_block(Block::create_shared(std::move(desc_block)));
    }
    return Status::OK();
}

size_t FullSorter::data_size() const {
    return _state->data_size();
}

void FullSorter::reset() {
    _state->reset();
}

} // namespace doris::vectorized
