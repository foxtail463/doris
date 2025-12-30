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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/sortBlock.h
// and modified by Doris

#pragma once
#include <glog/logging.h>
#include <pdqsort.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <utility>
#include <vector>

#include "common/compare.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_varbinary.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"

namespace doris::vectorized {
template <PrimitiveType T>
class ColumnDecimal;
template <PrimitiveType T>
class ColumnVector;
} // namespace doris::vectorized

namespace doris::vectorized {
#include "common/compile_check_begin.h"
/// Sort one block by `description`. If limit != 0, then the partial sort of the first `limit` rows is produced.
void sort_block(Block& src_block, Block& dest_block, const SortDescription& description,
                UInt64 limit = 0);

using ColumnWithSortDescription = std::pair<const IColumn*, SortColumnDescription>;

using ColumnsWithSortDescriptions = std::vector<ColumnWithSortDescription>;

ColumnsWithSortDescriptions get_columns_with_sort_description(const Block& block,
                                                              const SortDescription& description);

/**
 * 相等范围迭代器 - 用于多列排序中的相等范围遍历
 * 
 * 功能说明：
 * 在多列排序中，当第一列排序完成后，只有相等范围内的行才需要在第二列上排序
 * 这个迭代器帮助找到所有需要排序的相等范围
 * 
 * 示例：
 * 假设按年龄、姓名排序，年龄列排序后：
 * 年龄: [22, 25, 25, 30, 30, 30]
 * 相等标志: [0, 0, 1, 0, 1, 1]
 * 相等范围: [0,1), [1,3), [3,6)
 * 只有[1,3)和[3,6)范围内的行需要在姓名列上排序
 */
struct EqualRangeIterator {
    int range_begin;  // 当前相等范围的起始位置
    int range_end;    // 当前相等范围的结束位置

    EqualRangeIterator(const EqualFlags& flags) : EqualRangeIterator(flags, 0, (int)flags.size()) {}

    EqualRangeIterator(const EqualFlags& flags, int begin, int end) : _flags(flags), _end(end) {
        range_begin = begin;
        range_end = end;
        _cur_range_begin = begin;
        _cur_range_end = end;
    }

    /**
     * 移动到下一个相等范围
     * @return true 如果还有更多范围需要处理
     */
    bool next() {
        if (_cur_range_begin >= _end) {
            return false;
        }

        // `_flags[i]=1` indicates that the i-th row is equal to the previous row, which means we
        // should continue to sort this row according to current column. Using the first non-zero
        // value and first zero value after first non-zero value as two bounds, we can get an equal range here
        if (!(_cur_range_begin == 0) || !(_flags[_cur_range_begin] == 1)) {
            _cur_range_begin = (int)simd::find_one(_flags, _cur_range_begin + 1);
            if (_cur_range_begin >= _end) {
                return false;
            }
            _cur_range_begin--;
        }

        _cur_range_end = (int)simd::find_zero(_flags, _cur_range_begin + 1);
        DCHECK(_cur_range_end <= _end);

        if (_cur_range_begin >= _cur_range_end) {
            return false;
        }

        range_begin = _cur_range_begin;
        range_end = _cur_range_end;
        _cur_range_begin = _cur_range_end;
        return true;
    }

private:
    int _cur_range_begin;
    int _cur_range_end;

    const EqualFlags& _flags;
    const int _end;
};

struct ColumnPartialSortingLess {
    const ColumnWithSortDescription& _column_with_sort_desc;

    explicit ColumnPartialSortingLess(const ColumnWithSortDescription& column)
            : _column_with_sort_desc(column) {}

    bool operator()(size_t a, size_t b) const {
        int res = _column_with_sort_desc.second.direction *
                  _column_with_sort_desc.first->compare_at(
                          a, b, *_column_with_sort_desc.first,
                          _column_with_sort_desc.second.nulls_direction);
        if (res < 0) {
            return true;
        } else if (res > 0) {
            return false;
        }
        return false;
    }
};

template <PrimitiveType T>
struct PermutationWithInlineValue {
    using ValueType = std::conditional_t<is_string_type(T), StringRef,
                                         typename PrimitiveTypeTraits<T>::ColumnItemType>;
    ValueType inline_value;
    uint32_t row_id;
};

template <PrimitiveType T>
using PermutationForColumn = std::vector<PermutationWithInlineValue<T>>;

/**
 * 列排序器 - 负责对单个列进行排序
 * 
 * 功能说明：
 * 1. 封装单个列的排序逻辑，包括排序方向、NULL值处理等
 * 2. 支持多列排序中的分层排序策略
 * 3. 通过相等标志数组优化排序性能
 */
class ColumnSorter {
public:
    /**
     * 构造函数
     * @param column 列对象和排序描述符的配对
     * @param limit 排序限制行数，用于TOP-N优化
     */
    explicit ColumnSorter(const ColumnWithSortDescription& column, const size_t limit)
            : _column_with_sort_desc(column),
              _limit(limit),
              _nulls_direction(column.second.nulls_direction),
              _direction(column.second.direction) {}

    /**
     * 执行列排序操作
     * 
     * 多列排序的核心逻辑：
     * 1. 只对相等范围内的行进行排序
     * 2. 更新相等标志数组，标记哪些行在当前列上相等
     * 3. 为下一列的排序提供更精确的排序范围
     * 
     * @param flags 相等标志数组，标记哪些行在排序键上相等
     * @param perms 置换数组，记录排序后的行顺序
     * @param range 当前排序范围，只对相等范围内的行进行排序
     * @param last_column 是否为最后一列，影响排序优化策略
     */
    void operator()(EqualFlags& flags, IColumn::Permutation& perms, EqualRange& range,
                    bool last_column) const {
        // 调用列对象的sort_column方法，执行实际的排序逻辑
        _column_with_sort_desc.first->sort_column(this, flags, perms, range, last_column);
    }

    /**
     * 通用列排序方法 - 适用于所有列类型的排序
     * 
     * 功能说明：
     * 1. 使用列自己的比较方法，支持所有列类型
     * 2. 支持TOP-N优化和相等范围排序
     * 3. 作为复杂列类型（Array、Map、Struct等）的兜底方案
     * 
     * 适用场景：
     * - 复杂列类型：ColumnArray、ColumnMap、ColumnStruct等
     * - 当模板版本无法匹配时的兜底方案
     * - 需要通用比较逻辑的场景
     * 
     * @param column 要排序的列对象（基类引用）
     * @param flags 相等标志数组，标记哪些行在排序键上相等
     * @param perms 置换数组，记录排序后的行顺序
     * @param range 当前排序范围
     * @param last_column 是否为最后一列
     */
    void sort_column(const IColumn& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        size_t new_limit = _limit;
        
        // 创建比较器：使用列自己的比较方法，支持所有列类型
        auto comparator = [&](const size_t a, const size_t b) {
            return column.compare_at(a, b, *_column_with_sort_desc.first, _nulls_direction);
        };
        
        // 创建部分排序比较器：用于TOP-N优化
        ColumnPartialSortingLess less(_column_with_sort_desc);
        
        // 定义排序函数：支持TOP-N优化
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto begin = perms.begin() + first_iter;
            auto end = perms.begin() + last_iter;

            // TOP-N优化：如果当前范围跨越limit边界，使用partial_sort
            if (UNLIKELY(_limit > 0 && first_iter < _limit && _limit <= last_iter)) {
                size_t n = _limit - first_iter;
                // 只排序前n个元素
                std::partial_sort(begin, begin + n, end, less);

                // 处理相等元素：将与第n个元素相等的元素也包含进来
                auto nth = perms[_limit - 1];
                size_t equal_count = 0;
                for (auto iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = _limit + equal_count;
            } else {
                // 普通排序：使用pdqsort算法
                pdqsort(begin, end, less);
            }
        };

        // 遍历相等范围，对每个范围进行排序
        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            // 如果当前范围已经超过limit，提前退出
            if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                break;
            }
            
            // 只对包含多行的范围进行排序
            if (LIKELY(range_end - range_begin > 1)) {
                do_sort(range_begin, range_end);
                
                // 如果不是最后一列，更新相等标志数组
                if (!last_column) {
                    flags[range_begin] = 0;
                    // 标记哪些行在当前列上相等
                    for (int i = range_begin + 1; i < range_end; i++) {
                        flags[i] &= comparator(perms[i - 1], perms[i]) == 0;
                    }
                }
            }
        }
        
        // 根据新的limit调整数组大小
        _shrink_to_fit(perms, flags, new_limit);
    }

    /**
     * 列排序的核心方法 - 根据列类型选择最优的排序策略
     * 
     * 功能说明：
     * 1. 根据列类型和大小选择不同的排序算法
     * 2. 支持两种排序模式：默认排序和内联置换排序
     * 3. 内联置换排序对小数据量有更好的性能
     * 
     * 排序策略选择：
     * - 小数据量 + 简单类型：使用内联置换排序（_sort_by_inlined_permutation）
     * - 大数据量 + 复杂类型：使用默认排序（_sort_by_default）
     * 
     * @param column 要排序的列对象
     * @param flags 相等标志数组，标记哪些行在排序键上相等
     * @param perms 置换数组，记录排序后的行顺序
     * @param range 当前排序范围
     * @param last_column 是否为最后一列
     */
    template <template <typename type> typename ColumnType, typename T>
    void sort_column(const ColumnType<T>& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        // 判断是否应该使用内联置换排序
        // 内联置换排序适用于小数据量，将值直接内联到置换数组中，减少内存访问
        if (!_should_inline_value(perms)) {
            // 使用默认排序策略：标准的置换数组排序
            // 适用于大数据量或复杂数据类型
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            // 使用内联置换排序：将值内联到置换数组中
            // 适用于小数据量，可以减少内存访问次数，提高缓存效率
            _sort_by_inlined_permutation<T::PType>(column, flags, perms, range, last_column);
        }
    }

    template <template <PrimitiveType type> typename ColumnType, PrimitiveType T>
    void sort_column(const ColumnType<T>& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!_should_inline_value(perms)) {
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            _sort_by_inlined_permutation<T>(column, flags, perms, range, last_column);
        }
    }

    void sort_column(const ColumnString& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!_should_inline_value(perms)) {
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            _sort_by_inlined_permutation<TYPE_STRING>(column, flags, perms, range, last_column);
        }
    }

    void sort_column(const ColumnArray& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        _sort_by_default(column, flags, perms, range, last_column);
    }
    void sort_column(const ColumnMap& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        _sort_by_default(column, flags, perms, range, last_column);
    }
    void sort_column(const ColumnStruct& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        _sort_by_default(column, flags, perms, range, last_column);
    }
    void sort_column(const ColumnVarbinary& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        _sort_by_default(column, flags, perms, range, last_column);
    }

    void sort_column(const ColumnString64& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!_should_inline_value(perms)) {
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            _sort_by_inlined_permutation<TYPE_STRING>(column, flags, perms, range, last_column);
        }
    }

    void sort_column(const ColumnNullable& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!column.has_null()) {
            column.get_nested_column().sort_column(this, flags, perms, range, last_column);
        } else {
            const auto& null_map = column.get_null_map_data();
            size_t limit = _limit;
            std::vector<std::pair<size_t, size_t>> is_null_ranges;
            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                    break;
                }
                bool null_first = _nulls_direction * _direction < 0;
                if (LIKELY(range_end - range_begin > 1)) {
                    size_t range_split = 0;
                    if (null_first) {
                        range_split = std::partition(perms.begin() + range_begin,
                                                     perms.begin() + range_end,
                                                     [&](size_t row_id) -> bool {
                                                         return null_map[row_id] != 0;
                                                     }) -
                                      perms.begin();
                    } else {
                        range_split = std::partition(perms.begin() + range_begin,
                                                     perms.begin() + range_end,
                                                     [&](size_t row_id) -> bool {
                                                         return null_map[row_id] == 0;
                                                     }) -
                                      perms.begin();
                    }
                    std::pair<size_t, size_t> is_null_range = {range_begin, range_split};
                    std::pair<size_t, size_t> not_null_range = {range_split, range_end};
                    if (!null_first) {
                        std::swap(is_null_range, not_null_range);
                    }

                    if (not_null_range.first < not_null_range.second) {
                        flags[not_null_range.first] = 0;
                    }
                    if (is_null_range.first < is_null_range.second) {
                        // do not sort null values
                        std::fill(flags.begin() + is_null_range.first,
                                  flags.begin() + is_null_range.second, 0);

                        if (UNLIKELY(_limit > is_null_range.first &&
                                     _limit <= is_null_range.second)) {
                            _limit = is_null_range.second;
                        }
                        is_null_ranges.push_back(std::move(is_null_range));
                    }
                }
            }

            column.get_nested_column().sort_column(this, flags, perms, range, last_column);
            _limit = limit;
            if (!last_column) {
                for (const auto& nr : is_null_ranges) {
                    std::fill(flags.begin() + nr.first + 1, flags.begin() + nr.second, 1);
                }
            }
        }
    }

private:
    /**
     * 判断是否应该使用内联置换排序
     * 
     * 内联置换排序示例：
     * 
     * 原始数据：年龄列 [25, 30, 22, 35, 28]
     * 置换数组：[2, 0, 4, 1, 3] (排序后的索引)
     * 
     * 标准排序：只存储索引，比较时需要 column.get_data()[perms[i]]
     * 内联排序：存储 {值, 索引}，直接比较值，无需访问原始列
     * 
     * 判断条件：
     * - _limit == 0 或 _limit > perms.size()/5 时使用内联排序
     * - 否则使用标准排序
     * 
     * @param perms 置换数组，用于判断数据量大小
     * @return true 如果应该使用内联置换排序，false 否则
     */
    bool _should_inline_value(const IColumn::Permutation& perms) const {
        // 如果没有排序限制，或者排序限制大于总行数的1/5，使用内联排序
        // 这样可以减少内存访问次数，提高缓存效率
        return _limit == 0 || _limit > (perms.size() / 5);
    }

    template <PrimitiveType T>
    void _shrink_to_fit(PermutationForColumn<T>& permutation_for_column,
                        IColumn::Permutation& perms, EqualFlags& flags, size_t limit) const {
        if (limit < perms.size() && limit != 0) {
            permutation_for_column.resize(limit);
            perms.resize(limit);
            flags.resize(limit);
        }
    }

    void _shrink_to_fit(IColumn::Permutation& perms, EqualFlags& flags, size_t limit) const {
        if (_limit < perms.size() && limit != 0) {
            perms.resize(limit);
            flags.resize(limit);
        }
    }

    template <typename ColumnType>
    static constexpr bool always_false_v = false;

    /**
     * 创建内联置换数组 - 将列的值直接内联到置换数组中
     * 
     * 内联置换数组创建示例：
     * 
     * 原始数据：年龄列 [25, 30, 22, 35, 28]
     * 置换数组：[2, 0, 4, 1, 3] (已排序的索引)
     * 
     * 创建过程：
     * i=0: row_id=2, inline_value=22 → {inline_value: 22, row_id: 2}
     * i=1: row_id=0, inline_value=25 → {inline_value: 25, row_id: 0}
     * i=2: row_id=4, inline_value=28 → {inline_value: 28, row_id: 4}
     * i=3: row_id=1, inline_value=30 → {inline_value: 30, row_id: 1}
     * i=4: row_id=3, inline_value=35 → {inline_value: 35, row_id: 3}
     * 
     * 结果内联置换数组：
     * [
     *   {inline_value: 22, row_id: 2},
     *   {inline_value: 25, row_id: 0}, 
     *   {inline_value: 28, row_id: 4},
     *   {inline_value: 30, row_id: 1},
     *   {inline_value: 35, row_id: 3}
     * ]
     * 
     * @param column 原始列对象
     * @param permutation_for_column 输出的内联置换数组
     * @param perms 输入的置换数组，包含已排序的索引
     */
    template <typename ColumnType, PrimitiveType T>
    void _create_permutation(const ColumnType& column,
                             PermutationWithInlineValue<T>* __restrict permutation_for_column,
                             const IColumn::Permutation& perms) const {
        for (size_t i = 0; i < perms.size(); i++) {
            size_t row_id = perms[i];
            // 根据列类型获取值：数值列直接访问数据，字符串列调用get_data_at
            if constexpr (std::is_same_v<ColumnType, ColumnVector<T>> ||
                          std::is_same_v<ColumnType, ColumnDecimal<T>>) {
                permutation_for_column[i].inline_value = column.get_data()[row_id];
            } else if constexpr (std::is_same_v<ColumnType, ColumnString> ||
                                 std::is_same_v<ColumnType, ColumnString64>) {
                permutation_for_column[i].inline_value = column.get_data_at(row_id);
            } else {
                static_assert(always_false_v<ColumnType>);
            }
            // 保存原始行ID，用于后续数据重排
            permutation_for_column[i].row_id = (uint32_t)row_id;
        }
    }

    /**
     * 默认排序方法 - 使用标准的置换数组排序
     * 
     * 功能说明：
     * 1. 使用标准的置换数组进行排序，通过索引访问原始数据
     * 2. 适用于大数据量或复杂数据类型
     * 3. 支持TOP-N优化和相等范围排序
     * 
     * 排序流程：
     * 1. 创建比较器，用于比较两个行的大小
     * 2. 遍历相等范围，对每个范围进行排序
     * 3. 更新相等标志数组，标记哪些行在当前列上相等
     * 4. 为下一列的排序提供更精确的排序范围
     * 
     * @param column 要排序的列对象
     * @param flags 相等标志数组，标记哪些行在排序键上相等
     * @param perms 置换数组，记录排序后的行顺序
     * @param range 当前排序范围
     * @param last_column 是否为最后一列
     */
    template <typename ColumnType>
    void _sort_by_default(const ColumnType& column, EqualFlags& flags, IColumn::Permutation& perms,
                          EqualRange& range, bool last_column) const {
        size_t new_limit = _limit;
        
        // 创建比较器：根据列类型选择不同的比较方式
        auto comparator = [&](const size_t a, const size_t b) {
            // 对于数值类型列，直接比较数据值
            if constexpr (!std::is_same_v<ColumnType, ColumnString> &&
                          !std::is_same_v<ColumnType, ColumnString64> &&
                          !std::is_same_v<ColumnType, ColumnArray> &&
                          !std::is_same_v<ColumnType, ColumnVarbinary> &&
                          !std::is_same_v<ColumnType, ColumnMap> &&
                          !std::is_same_v<ColumnType, ColumnStruct>) {
                auto value_a = column.get_data()[a];
                auto value_b = column.get_data()[b];
                return Compare::compare(value_a, value_b);
            } else {
                // 对于复杂类型列，使用列自己的比较方法
                return column.compare_at(a, b, column, _nulls_direction);
            }
        };

        // 创建排序比较器：考虑排序方向（升序/降序）
        auto sort_comparator = [&](const size_t a, const size_t b) {
            return comparator(a, b) * _direction < 0;
        };
        
        // 定义排序函数：支持TOP-N优化
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto* begin = perms.begin() + first_iter;
            auto* end = perms.begin() + last_iter;

            // TOP-N优化：如果当前范围跨越limit边界，使用partial_sort
            if (UNLIKELY(_limit > 0 && first_iter < _limit && _limit <= last_iter)) {
                size_t n = _limit - first_iter;
                // 只排序前n个元素
                std::partial_sort(begin, begin + n, end, sort_comparator);

                // 处理相等元素：将与第n个元素相等的元素也包含进来
                auto nth = perms[_limit - 1];
                size_t equal_count = 0;
                for (auto* iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = _limit + equal_count;
            } else {
                // 普通排序：使用pdqsort算法
                pdqsort(begin, end, sort_comparator);
            }
        };

        // 遍历相等范围，对每个范围进行排序
        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            // 如果当前范围已经超过limit，提前退出
            if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                break;
            }
            
            // 只对包含多行的范围进行排序
            if (LIKELY(range_end - range_begin > 1)) {
                do_sort(range_begin, range_end);
                
                // 如果不是最后一列，更新相等标志数组
                if (!last_column) {
                    flags[range_begin] = 0;
                    // 标记哪些行在当前列上相等
                    for (int i = range_begin + 1; i < range_end; i++) {
                        flags[i] &= comparator(perms[i - 1], perms[i]) == 0;
                    }
                }
            }
        }
        
        // 根据新的limit调整数组大小
        _shrink_to_fit(perms, flags, new_limit);
    }

    /**
     * 内联置换排序方法 - 将值直接内联到置换数组中
     * 
     * 内联置换排序示例：
     * 
     * 原始数据：年龄列 [25, 30, 22, 35, 28]
     * 置换数组：[2, 0, 4, 1, 3] (排序后的索引)
     * 
     * 标准排序：只存储索引，比较时需要 column.get_data()[perms[i]]
     * 内联排序：存储 {值, 索引}，直接比较值，无需访问原始列
     * 
     * 内联置换数组结果：
     * [
     *   {inline_value: 22, row_id: 2},
     *   {inline_value: 25, row_id: 0}, 
     *   {inline_value: 28, row_id: 4},
     *   {inline_value: 30, row_id: 1},
     *   {inline_value: 35, row_id: 3}
     * ]
     * 
     * @param column 要排序的列对象
     * @param flags 相等标志数组，标记哪些行在排序键上相等
     * @param perms 置换数组，记录排序后的行顺序
     * @param range 当前排序范围
     * @param last_column 是否为最后一列
     */
    template <PrimitiveType InlineType, typename ColumnType>
    void _sort_by_inlined_permutation(const ColumnType& column, EqualFlags& flags,
                                      IColumn::Permutation& perms, EqualRange& range,
                                      bool last_column) const {
        size_t new_limit = _limit;
        
        // 创建内联置换数组：将列的值直接内联到置换数组中
        PermutationForColumn<InlineType> permutation_for_column(perms.size());
        _create_permutation(column, permutation_for_column.data(), perms);
        
        // 创建比较器：直接比较内联值，无需访问原始列
        auto comparator = [&](const PermutationWithInlineValue<InlineType>& a,
                              const PermutationWithInlineValue<InlineType>& b) {
            // 对于非字符串类型，直接比较内联值
            if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
                return Compare::compare(a.inline_value, b.inline_value);
            } else {
                // 对于字符串类型，使用内存比较
                return memcmp_small_allow_overflow15(
                        reinterpret_cast<const UInt8*>(a.inline_value.data), a.inline_value.size,
                        reinterpret_cast<const UInt8*>(b.inline_value.data), b.inline_value.size);
            }
        };

        // 创建排序比较器：考虑排序方向（升序/降序）
        auto sort_comparator = [&](const PermutationWithInlineValue<InlineType>& a,
                                   const PermutationWithInlineValue<InlineType>& b) {
            return comparator(a, b) * _direction < 0;
        };
        
        // 定义排序函数：支持TOP-N优化
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto begin = permutation_for_column.begin() + first_iter;
            auto end = permutation_for_column.begin() + last_iter;

            // TOP-N优化：如果当前范围跨越limit边界，使用partial_sort
            if (UNLIKELY(_limit > 0 && first_iter < _limit && _limit <= last_iter)) {
                size_t n = _limit - first_iter;
                // 只排序前n个元素
                std::partial_sort(begin, begin + n, end, sort_comparator);

                // 处理相等元素：将与第n个元素相等的元素也包含进来
                auto nth = permutation_for_column[_limit - 1];
                size_t equal_count = 0;
                for (auto iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = _limit + equal_count;
            } else {
                // 普通排序：使用pdqsort算法
                pdqsort(begin, end, sort_comparator);
            }
        };

        // 遍历相等范围，对每个范围进行排序
        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            // 如果当前范围已经超过limit，提前退出
            if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                break;
            }
            
            // 只对包含多行的范围进行排序
            if (LIKELY(range_end - range_begin > 1)) {
                do_sort(range_begin, range_end);
                
                // 如果不是最后一列，更新相等标志数组
                if (!last_column) {
                    flags[range_begin] = 0;
                    // 标记哪些行在当前列上相等
                    for (int i = range_begin + 1; i < range_end; i++) {
                        flags[i] &= comparator(permutation_for_column[i - 1],
                                               permutation_for_column[i]) == 0;
                    }
                }
            }
        }
        
        // 根据新的limit调整数组大小
        _shrink_to_fit(permutation_for_column, perms, flags, new_limit);
        
        // 从内联置换数组恢复原始置换数组
        _restore_permutation(permutation_for_column, perms.data());
    }

    template <PrimitiveType T>
    void _restore_permutation(const PermutationForColumn<T>& permutation_for_column,
                              size_t* __restrict perms) const {
        for (size_t i = 0; i < permutation_for_column.size(); i++) {
            perms[i] = permutation_for_column[i].row_id;
        }
    }

    const ColumnWithSortDescription& _column_with_sort_desc;
    mutable size_t _limit;
    const int _nulls_direction;
    const int _direction;
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
