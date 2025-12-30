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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Block.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <list>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "common/exception.h"
#include "common/factory_creator.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

class SipHash;

namespace doris {

class TupleDescriptor;
class PBlock;
class SlotDescriptor;

namespace segment_v2 {
enum CompressionTypePB : int;
} // namespace segment_v2

namespace vectorized {

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitrary position, to change order of columns.
  */
class MutableBlock;

class Block {
    ENABLE_FACTORY_CREATOR(Block);

private:
    using Container = ColumnsWithTypeAndName;
    Container data;

public:
    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(ColumnsWithTypeAndName data_);
    Block(const std::vector<SlotDescriptor*>& slots, size_t block_size);
    Block(const std::vector<SlotDescriptor>& slots, size_t block_size);

    MOCK_FUNCTION ~Block() = default;
    Block(const Block& block) = default;
    Block& operator=(const Block& p) = default;
    Block(Block&& block) = default;
    Block& operator=(Block&& other) = default;

    void reserve(size_t count);
    // Make sure the nammes is useless when use block
    void clear_names();

    /// insert the column at the specified position
    void insert(size_t position, const ColumnWithTypeAndName& elem);
    void insert(size_t position, ColumnWithTypeAndName&& elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName& elem);
    void insert(ColumnWithTypeAndName&& elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the column at the [start, end)
    void erase_tail(size_t start);
    /// remove the columns at the specified positions
    void erase(const std::set<size_t>& positions);
    // T was std::set<int>, std::vector<int>, std::list<int>
    template <class T>
    void erase_not_in(const T& container) {
        Container new_data;
        for (auto pos : container) {
            new_data.emplace_back(std::move(data[pos]));
        }
        std::swap(data, new_data);
    }

    std::unordered_map<std::string, uint32_t> get_name_to_pos_map() const {
        std::unordered_map<std::string, uint32_t> name_to_index_map;
        for (uint32_t i = 0; i < data.size(); ++i) {
            name_to_index_map[data[i].name] = i;
        }
        return name_to_index_map;
    }

    /**
     * 根据位置索引获取 Block 中的列
     * 
     * 此函数通过列在 Block 中的位置索引（position）获取对应的列（ColumnWithTypeAndName），
     * 并返回该列的可变引用。位置索引从 0 开始，表示列在 Block 中的顺序。
     * 
     * 使用场景：
     * - 在向量化执行中，函数调用时需要根据参数列索引获取输入列
     * - 表达式执行时需要获取结果列以便写入数据
     * - 查询执行计划中通过列索引访问列数据
     * 
     * 注意事项：
     * - 位置索引必须在有效范围内（0 <= position < data.size()），否则在 DEBUG 模式下会触发断言
     * - 返回的是列的引用，可以直接修改列的内容
     * - 如果在调用此函数后调用了 insert、erase 等修改 Block 结构的函数，返回的引用可能会失效
     *   （因为这些函数可能改变列的存储位置或导致内存重新分配）
     * 
     * @param position 列的位置索引（从 0 开始）
     * @return 对应位置列的引用（ColumnWithTypeAndName&），包含列数据、类型和名称信息
     * 
     * @see get_by_name() 通过名称获取列
     * @see safe_get_by_position() 带边界检查的安全版本
     */
    /// References are invalidated after calling functions above.
    ColumnWithTypeAndName& get_by_position(size_t position) {
        DCHECK(data.size() > position)
                << ", data.size()=" << data.size() << ", position=" << position;
        return data[position];
    }
    const ColumnWithTypeAndName& get_by_position(size_t position) const { return data[position]; }

    void replace_by_position(size_t position, ColumnPtr&& res) {
        this->get_by_position(position).column = std::move(res);
    }

    void replace_by_position(size_t position, const ColumnPtr& res) {
        this->get_by_position(position).column = res;
    }

    void replace_by_position_if_const(size_t position) {
        auto& element = this->get_by_position(position);
        element.column = element.column->convert_to_full_column_if_const();
    }

    ColumnWithTypeAndName& safe_get_by_position(size_t position);
    const ColumnWithTypeAndName& safe_get_by_position(size_t position) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    // Get position of column by name. Returns -1 if there is no column with that name.
    // ATTN: this method is O(N). better maintain name -> position map in caller if you need to call it frequently.
    int get_position_by_name(const std::string& name) const;

    const ColumnsWithTypeAndName& get_columns_with_type_and_name() const;

    std::vector<std::string> get_names() const;
    DataTypes get_data_types() const;

    DataTypePtr get_data_type(size_t index) const {
        CHECK(index < data.size());
        return data[index].type;
    }

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    // Cut the rows in block, use in LIMIT operation
    void set_num_rows(size_t length);

    // Skip the rows in block, use in OFFSET, LIMIT operation
    void skip_num_rows(int64_t& offset);

    /// As the assumption we used around, the number of columns won't exceed int16 range. so no need to worry when we
    ///  assign it to int32.
    uint32_t columns() const { return static_cast<uint32_t>(data.size()); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void check_number_of_rows(bool allow_null_columns = false) const;

    Status check_type_and_column() const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    std::string columns_bytes() const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    MOCK_FUNCTION size_t allocated_bytes() const;

    /** Get a list of column names separated by commas. */
    std::string dump_names() const;

    std::string dump_types() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
    std::string dump_structure() const;

    /** Get the same block, but empty. */
    Block clone_empty() const;

    Columns get_columns() const;
    Columns get_columns_and_convert();

    Block clone_without_columns(const std::vector<int>* column_offset = nullptr) const;

    /** Get empty columns with the same types as in block. */
    MutableColumns clone_empty_columns() const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutate_columns();

    /** Replace columns in a block */
    void set_columns(MutableColumns&& columns);
    Block clone_with_columns(MutableColumns&& columns) const;

    void clear();
    void swap(Block& other) noexcept;
    void swap(Block&& other) noexcept;

    // Shuffle columns in place based on the result_column_ids
    void shuffle_columns(const std::vector<int>& result_column_ids);

    // Default column size = -1 means clear all column in block
    // Else clear column [0, column_size) delete column [column_size, data.size)
    void clear_column_data(int64_t column_size = -1) noexcept;

    MOCK_FUNCTION bool mem_reuse() { return !data.empty(); }

    bool is_empty_column() { return data.empty(); }

    bool empty() const { return rows() == 0; }

    /** 
      * Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
    void update_hash(SipHash& hash) const;

    /** 
     *  Get block data in string. 
     *  If code is in default_implementation_for_nulls or something likely, type and column's nullity could
     *   temporarily be not same. set allow_null_mismatch to true to dump it correctly.
    */
    std::string dump_data(size_t begin = 0, size_t row_limit = 100,
                          bool allow_null_mismatch = false) const;

    std::string dump_data_json(size_t begin = 0, size_t row_limit = 100,
                               bool allow_null_mismatch = false) const;

    /** Get one line data from block, only use in load data */
    std::string dump_one_line(size_t row, int column_end) const;

    Status append_to_block_by_selector(MutableBlock* dst, const IColumn::Selector& selector) const;

    // need exception safety
    static void filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                      const IColumn::Filter& filter);
    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter,
                                      uint32_t column_to_keep);
    // need exception safety
    static void filter_block_internal(Block* block, const IColumn::Filter& filter);

    static Status filter_block(Block* block, const std::vector<uint32_t>& columns_to_filter,
                               size_t filter_column_id, size_t column_to_keep);

    static Status filter_block(Block* block, size_t filter_column_id, size_t column_to_keep);

    static void erase_useless_column(Block* block, size_t column_to_keep) {
        block->erase_tail(column_to_keep);
    }

    // serialize block to PBlock
    Status serialize(int be_exec_version, PBlock* pblock, size_t* uncompressed_bytes,
                     size_t* compressed_bytes, int64_t* compress_time,
                     segment_v2::CompressionTypePB compression_type,
                     bool allow_transfer_large_data = false) const;

    Status deserialize(const PBlock& pblock, size_t* uncompressed_bytes, int64_t* decompress_time);

    std::unique_ptr<Block> create_same_struct_block(size_t size, bool is_reserve = false) const;

    /** Compares (*this) n-th row and rhs m-th row.
      * Returns negative number, 0, or positive number  (*this) n-th row is less, equal, greater than rhs m-th row respectively.
      * Is used in sortings.
      *
      * If one of element's value is NaN or NULLs, then:
      * - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
      * - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
      * For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
      *
      * For non Nullable and non floating point types, nan_direction_hint is ignored.
      */
    int compare_at(size_t n, size_t m, const Block& rhs, int nan_direction_hint) const {
        DCHECK_EQ(columns(), rhs.columns());
        return compare_at(n, m, columns(), rhs, nan_direction_hint);
    }

    int compare_at(size_t n, size_t m, size_t num_columns, const Block& rhs,
                   int nan_direction_hint) const {
        DCHECK_GE(columns(), num_columns);
        DCHECK_GE(rhs.columns(), num_columns);

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (size_t i = 0; i < num_columns; ++i) {
            DCHECK(get_by_position(i).type->equals(*rhs.get_by_position(i).type));
            auto res = get_by_position(i).column->compare_at(n, m, *(rhs.get_by_position(i).column),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    int compare_at(size_t n, size_t m, const std::vector<uint32_t>* compare_columns,
                   const Block& rhs, int nan_direction_hint) const {
        DCHECK_GE(columns(), compare_columns->size());
        DCHECK_GE(rhs.columns(), compare_columns->size());

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (auto i : *compare_columns) {
            DCHECK(get_by_position(i).type->equals(*rhs.get_by_position(i).type));
            auto res = get_by_position(i).column->compare_at(n, m, *(rhs.get_by_position(i).column),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    //note(wb) no DCHECK here, because this method is only used after compare_at now, so no need to repeat check here.
    // If this method is used in more places, you can add DCHECK case by case.
    int compare_column_at(size_t n, size_t m, size_t col_idx, const Block& rhs,
                          int nan_direction_hint) const {
        auto res = get_by_position(col_idx).column->compare_at(
                n, m, *(rhs.get_by_position(col_idx).column), nan_direction_hint);
        return res;
    }

    // for String type or Array<String> type
    void shrink_char_type_column_suffix_zero(const std::vector<size_t>& char_type_idx);

    void clear_column_mem_not_keep(const std::vector<bool>& column_keep_flags,
                                   bool need_keep_first);

private:
    void erase_impl(size_t position);
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;
using BlocksPtrs = std::shared_ptr<std::vector<BlocksPtr>>;

class MutableBlock {
    ENABLE_FACTORY_CREATOR(MutableBlock);

private:
    MutableColumns _columns;
    DataTypes _data_types;
    std::vector<std::string> _names;

public:
    static MutableBlock build_mutable_block(Block* block) {
        return block == nullptr ? MutableBlock() : MutableBlock(block);
    }
    MutableBlock() = default;
    ~MutableBlock() = default;

    MutableBlock(Block* block)
            : _columns(block->mutate_columns()),
              _data_types(block->get_data_types()),
              _names(block->get_names()) {}
    MutableBlock(Block&& block)
            : _columns(block.mutate_columns()),
              _data_types(block.get_data_types()),
              _names(block.get_names()) {}

    void operator=(MutableBlock&& m_block) {
        _columns = std::move(m_block._columns);
        _data_types = std::move(m_block._data_types);
        _names = std::move(m_block._names);
    }

    size_t rows() const;
    size_t columns() const { return _columns.size(); }

    bool empty() const { return rows() == 0; }

    MutableColumns& mutable_columns() { return _columns; }

    void set_mutable_columns(MutableColumns&& columns) { _columns = std::move(columns); }

    DataTypes& data_types() { return _data_types; }

    MutableColumnPtr& get_column_by_position(size_t position) { return _columns[position]; }
    const MutableColumnPtr& get_column_by_position(size_t position) const {
        return _columns[position];
    }

    DataTypePtr& get_datatype_by_position(size_t position) { return _data_types[position]; }
    const DataTypePtr& get_datatype_by_position(size_t position) const {
        return _data_types[position];
    }

    int compare_one_column(size_t n, size_t m, size_t column_id, int nan_direction_hint) const {
        DCHECK_LE(column_id, columns());
        DCHECK_LE(n, rows());
        DCHECK_LE(m, rows());
        auto& column = get_column_by_position(column_id);
        return column->compare_at(n, m, *column, nan_direction_hint);
    }

    int compare_at(size_t n, size_t m, size_t num_columns, const MutableBlock& rhs,
                   int nan_direction_hint) const {
        DCHECK_GE(columns(), num_columns);
        DCHECK_GE(rhs.columns(), num_columns);

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (size_t i = 0; i < num_columns; ++i) {
            DCHECK(get_datatype_by_position(i)->equals(*rhs.get_datatype_by_position(i)));
            auto res = get_column_by_position(i)->compare_at(n, m, *(rhs.get_column_by_position(i)),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    int compare_at(size_t n, size_t m, const std::vector<uint32_t>* compare_columns,
                   const MutableBlock& rhs, int nan_direction_hint) const {
        DCHECK_GE(columns(), compare_columns->size());
        DCHECK_GE(rhs.columns(), compare_columns->size());

        DCHECK_LE(n, rows());
        DCHECK_LE(m, rhs.rows());
        for (auto i : *compare_columns) {
            DCHECK(get_datatype_by_position(i)->equals(*rhs.get_datatype_by_position(i)));
            auto res = get_column_by_position(i)->compare_at(n, m, *(rhs.get_column_by_position(i)),
                                                             nan_direction_hint);
            if (res) {
                return res;
            }
        }
        return 0;
    }

    std::string dump_types() const {
        std::string res;
        for (auto type : _data_types) {
            if (!res.empty()) {
                res += ", ";
            }
            res += type->get_name();
        }
        return res;
    }

    template <typename T>
    [[nodiscard]] Status merge(T&& block) {
        RETURN_IF_CATCH_EXCEPTION(return merge_impl(block););
    }

    template <typename T>
    [[nodiscard]] Status merge_ignore_overflow(T&& block) {
        RETURN_IF_CATCH_EXCEPTION(return merge_impl_ignore_overflow(block););
    }

    // only use for join. call ignore_overflow to prevent from throw exception in join
    template <typename T>
    [[nodiscard]] Status merge_impl_ignore_overflow(T&& block) {
        if (_columns.size() != block.columns()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Merge block not match, self column count: {}, [columns: {}, types: {}], "
                    "input column count: {}, [columns: {}, "
                    "types: {}], ",
                    _columns.size(), dump_names(), dump_types(), block.columns(),
                    block.dump_names(), block.dump_types());
        }
        for (int i = 0; i < _columns.size(); ++i) {
            if (!_data_types[i]->equals(*block.get_by_position(i).type)) {
                throw doris::Exception(doris::ErrorCode::FATAL_ERROR,
                                       "Merge block not match, self:[columns: {}, types: {}], "
                                       "input:[columns: {}, types: {}], ",
                                       dump_names(), dump_types(), block.dump_names(),
                                       block.dump_types());
            }
            _columns[i]->insert_range_from_ignore_overflow(
                    *block.get_by_position(i).column->convert_to_full_column_if_const().get(), 0,
                    block.rows());
        }
        return Status::OK();
    }

    /**
     * 合并另一个 Block 的数据到当前 MutableBlock
     * 
     * 【功能说明】
     * 将源 Block 的所有行数据追加到当前 MutableBlock 的末尾。
     * 支持两种情况：
     * 1. 当前 Block 为空：直接复制源 Block 的结构和数据
     * 2. 当前 Block 不为空：检查列数和类型匹配，然后追加数据
     * 
     * 【类型处理】
     * - 如果目标列是 Nullable 类型，源列是非 Nullable 类型，会自动转换为 Nullable
     * - 如果类型完全匹配，直接追加
     * - 不支持动态 Block（Dynamic Block）的合并
     * 
     * 【参数说明】
     * @param block 源 Block（使用完美转发，支持左值和右值引用）
     * @return Status::OK() 如果成功，否则返回错误状态
     * 
     * 【使用场景】
     * - Hash Join：合并多个 Build 数据块
     * - 数据恢复：将从磁盘恢复的数据块合并到内存中的 Block
     * - 数据聚合：合并多个分区的数据
     */
    template <typename T>
    [[nodiscard]] Status merge_impl(T&& block) {
        // 情况 1：当前 Block 为空（动态 Block 不支持合并）
        // 如果当前 Block 的列和数据类型都为空，说明是首次合并
        // 此时直接复制源 Block 的结构和数据
        if (_columns.empty() && _data_types.empty()) {
            // 复制数据类型和列名
            _data_types = block.get_data_types();
            _names = block.get_names();
            // 调整列数
            _columns.resize(block.columns());
            
            // 遍历每一列，复制列数据
            for (size_t i = 0; i < block.columns(); ++i) {
                if (block.get_by_position(i).column) {
                    // 如果源列存在，转换为完整列（非常量列）并移动数据
                    // convert_to_full_column_if_const：如果是常量列，转换为完整列
                    // mutate()：获取可变的列引用
                    _columns[i] = (*std::move(block.get_by_position(i)
                                                      .column->convert_to_full_column_if_const()))
                                          .mutate();
                } else {
                    // 如果源列为空，创建空列
                    _columns[i] = _data_types[i]->create_column();
                }
            }
        } else {
            // 情况 2：当前 Block 不为空，需要追加数据
            // 检查列数是否匹配
            if (_columns.size() != block.columns()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "Merge block not match, self column count: {}, [columns: {}, types: {}], "
                        "input column count: {}, [columns: {}, "
                        "types: {}], ",
                        _columns.size(), dump_names(), dump_types(), block.columns(),
                        block.dump_names(), block.dump_types());
            }
            
            // 遍历每一列，追加数据
            for (int i = 0; i < _columns.size(); ++i) {
                // 检查类型是否匹配
                if (!_data_types[i]->equals(*block.get_by_position(i).type)) {
                    // 类型不匹配，但可能是 Nullable 类型转换的情况
                    // 目标列必须是 Nullable 类型
                    DCHECK(_data_types[i]->is_nullable())
                            << " target type: " << _data_types[i]->get_name()
                            << " src type: " << block.get_by_position(i).type->get_name();
                    // 目标列的嵌套类型必须与源列类型匹配
                    DCHECK(((DataTypeNullable*)_data_types[i].get())
                                   ->get_nested_type()
                                   ->equals(*block.get_by_position(i).type));
                    // 源列必须是非 Nullable 类型
                    DCHECK(!block.get_by_position(i).type->is_nullable());
                    
                    // 将源列转换为 Nullable 类型，然后追加数据
                    // make_nullable：将非 Nullable 列包装为 Nullable 列
                    // convert_to_full_column_if_const：转换为完整列
                    // insert_range_from：从索引 0 开始，追加 block.rows() 行数据
                    _columns[i]->insert_range_from(*make_nullable(block.get_by_position(i).column)
                                                            ->convert_to_full_column_if_const(),
                                                   0, block.rows());
                } else {
                    // 类型完全匹配，直接追加数据
                    // convert_to_full_column_if_const：转换为完整列（如果是常量列）
                    // insert_range_from：从索引 0 开始，追加 block.rows() 行数据
                    _columns[i]->insert_range_from(
                            *block.get_by_position(i)
                                     .column->convert_to_full_column_if_const()
                                     .get(),
                            0, block.rows());
                }
            }
        }
        return Status::OK();
    }

    // move to columns' data to a Block. this will invalidate
    Block to_block(int start_column = 0);
    Block to_block(int start_column, int end_column);

    void swap(MutableBlock& other) noexcept;

    void add_row(const Block* block, int row);
    // Batch add row should return error status if allocate memory failed.
    Status add_rows(const Block* block, const uint32_t* row_begin, const uint32_t* row_end,
                    const std::vector<int>* column_offset = nullptr);
    Status add_rows(const Block* block, size_t row_begin, size_t length);
    Status add_rows(const Block* block, const std::vector<int64_t>& rows);

    std::string dump_data(size_t row_limit = 100) const;
    std::string dump_data_json(size_t row_limit = 100) const;

    void clear() {
        _columns.clear();
        _data_types.clear();
        _names.clear();
    }

    // columns resist. columns' inner data removed.
    void clear_column_data() noexcept;

    size_t allocated_bytes() const;

    size_t bytes() const {
        size_t res = 0;
        for (const auto& elem : _columns) {
            res += elem->byte_size();
        }

        return res;
    }

    std::vector<std::string>& get_names() { return _names; }

    /** Get a list of column names separated by commas. */
    std::string dump_names() const;
};

struct IteratorRowRef {
    std::shared_ptr<Block> block;
    int row_pos;
    bool is_same;

    template <typename T>
    int compare(const IteratorRowRef& rhs, const T& compare_arguments) const {
        return block->compare_at(row_pos, rhs.row_pos, compare_arguments, *rhs.block, -1);
    }

    void reset() {
        block = nullptr;
        row_pos = -1;
        is_same = false;
    }
};

using BlockView = std::vector<IteratorRowRef>;
using BlockUPtr = std::unique_ptr<Block>;

} // namespace vectorized
} // namespace doris
