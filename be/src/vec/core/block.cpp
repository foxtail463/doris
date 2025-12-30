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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Block.cpp
// and modified by Doris

#include "vec/core/block.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <snappy.h>
#include <streamvbyte.h>
#include <sys/types.h>

#include <algorithm>
#include <cassert>
#include <iomanip>
#include <iterator>
#include <limits>
#include <ranges>

#include "agent/be_exec_version_manager.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/thread_context.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "util/simd/bits.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/serde/data_type_serde.h"

class SipHash;

namespace doris::segment_v2 {
enum CompressionTypePB : int;
} // namespace doris::segment_v2
#include "common/compile_check_begin.h"
namespace doris::vectorized {
template <typename T>
void clear_blocks(moodycamel::ConcurrentQueue<T>& blocks,
                  RuntimeProfile::Counter* memory_used_counter = nullptr) {
    T block;
    while (blocks.try_dequeue(block)) {
        if (memory_used_counter) {
            if constexpr (std::is_same_v<T, Block>) {
                memory_used_counter->update(-block.allocated_bytes());
            } else {
                memory_used_counter->update(-block->allocated_bytes());
            }
        }
    }
}

template void clear_blocks<Block>(moodycamel::ConcurrentQueue<Block>&,
                                  RuntimeProfile::Counter* memory_used_counter);
template void clear_blocks<BlockUPtr>(moodycamel::ConcurrentQueue<BlockUPtr>&,
                                      RuntimeProfile::Counter* memory_used_counter);

Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data {il} {}

Block::Block(ColumnsWithTypeAndName data_) : data {std::move(data_)} {}

Block::Block(const std::vector<SlotDescriptor*>& slots, size_t block_size) {
    for (auto* const slot_desc : slots) {
        auto column_ptr = slot_desc->get_empty_mutable_column();
        column_ptr->reserve(block_size);
        insert(ColumnWithTypeAndName(std::move(column_ptr), slot_desc->get_data_type_ptr(),
                                     slot_desc->col_name()));
    }
}

Block::Block(const std::vector<SlotDescriptor>& slots, size_t block_size) {
    std::vector<SlotDescriptor*> slot_ptrs(slots.size());
    for (size_t i = 0; i < slots.size(); ++i) {
        // Slots remain unmodified and are used to read column information; const_cast can be employed.
        // used in src/exec/rowid_fetcher.cpp
        slot_ptrs[i] = const_cast<SlotDescriptor*>(&slots[i]);
    }
    *this = Block(slot_ptrs, block_size);
}

Status Block::deserialize(const PBlock& pblock, size_t* uncompressed_bytes,
                          int64_t* decompress_time) {
    swap(Block());
    int be_exec_version = pblock.has_be_exec_version() ? pblock.be_exec_version() : 0;
    RETURN_IF_ERROR(BeExecVersionManager::check_be_exec_version(be_exec_version));

    const char* buf = nullptr;
    std::string compression_scratch;
    if (pblock.compressed()) {
        // Decompress
        SCOPED_RAW_TIMER(decompress_time);
        const char* compressed_data = pblock.column_values().c_str();
        size_t compressed_size = pblock.column_values().size();
        size_t uncompressed_size = 0;
        if (pblock.has_compression_type() && pblock.has_uncompressed_size()) {
            BlockCompressionCodec* codec;
            RETURN_IF_ERROR(get_block_compression_codec(pblock.compression_type(), &codec));
            uncompressed_size = pblock.uncompressed_size();
            // Should also use allocator to allocate memory here.
            compression_scratch.resize(uncompressed_size);
            Slice decompressed_slice(compression_scratch);
            RETURN_IF_ERROR(codec->decompress(Slice(compressed_data, compressed_size),
                                              &decompressed_slice));
            DCHECK(uncompressed_size == decompressed_slice.size);
        } else {
            bool success = snappy::GetUncompressedLength(compressed_data, compressed_size,
                                                         &uncompressed_size);
            DCHECK(success) << "snappy::GetUncompressedLength failed";
            compression_scratch.resize(uncompressed_size);
            success = snappy::RawUncompress(compressed_data, compressed_size,
                                            compression_scratch.data());
            DCHECK(success) << "snappy::RawUncompress failed";
        }
        *uncompressed_bytes = uncompressed_size;
        buf = compression_scratch.data();
    } else {
        buf = pblock.column_values().data();
    }

    for (const auto& pcol_meta : pblock.column_metas()) {
        DataTypePtr type = DataTypeFactory::instance().create_data_type(pcol_meta);
        MutableColumnPtr data_column = type->create_column();
        // Here will try to allocate large memory, should return error if failed.
        RETURN_IF_CATCH_EXCEPTION(
                buf = type->deserialize(buf, &data_column, pblock.be_exec_version()));
        data.emplace_back(data_column->get_ptr(), type, pcol_meta.name());
    }

    return Status::OK();
}

void Block::reserve(size_t count) {
    data.reserve(count);
}

void Block::insert(size_t position, const ColumnWithTypeAndName& elem) {
    if (position > data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }

    data.emplace(data.begin() + position, elem);
}

void Block::insert(size_t position, ColumnWithTypeAndName&& elem) {
    if (position > data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }

    data.emplace(data.begin() + position, std::move(elem));
}

void Block::clear_names() {
    for (auto& entry : data) {
        entry.name.clear();
    }
}

void Block::insert(const ColumnWithTypeAndName& elem) {
    data.emplace_back(elem);
}

void Block::insert(ColumnWithTypeAndName&& elem) {
    data.emplace_back(std::move(elem));
}

void Block::erase(const std::set<size_t>& positions) {
    for (unsigned long position : std::ranges::reverse_view(positions)) {
        erase(position);
    }
}

void Block::erase_tail(size_t start) {
    DCHECK(start <= data.size()) << fmt::format(
            "Position out of bound in Block::erase(), max position = {}", data.size());
    data.erase(data.begin() + start, data.end());
}

void Block::erase(size_t position) {
    DCHECK(!data.empty()) << "Block is empty";
    DCHECK_LT(position, data.size()) << fmt::format(
            "Position out of bound in Block::erase(), max position = {}", data.size() - 1);

    erase_impl(position);
}

void Block::erase_impl(size_t position) {
    data.erase(data.begin() + position);
}

ColumnWithTypeAndName& Block::safe_get_by_position(size_t position) {
    if (position >= data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }
    return data[position];
}

const ColumnWithTypeAndName& Block::safe_get_by_position(size_t position) const {
    if (position >= data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }
    return data[position];
}

int Block::get_position_by_name(const std::string& name) const {
    for (int i = 0; i < data.size(); i++) {
        if (data[i].name == name) {
            return i;
        }
    }
    return -1;
}

void Block::check_number_of_rows(bool allow_null_columns) const {
    ssize_t rows = -1;
    for (const auto& elem : data) {
        if (!elem.column && allow_null_columns) {
            continue;
        }

        if (!elem.column) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Column {} in block is nullptr, in method check_number_of_rows.",
                            elem.name);
        }

        ssize_t size = elem.column->size();

        if (rows == -1) {
            rows = size;
        } else if (rows != size) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Sizes of columns doesn't match, block={}",
                            dump_structure());
        }
    }
}

Status Block::check_type_and_column() const {
#ifndef NDEBUG
    for (const auto& elem : data) {
        if (!elem.column) {
            continue;
        }
        if (!elem.type) {
            continue;
        }

        // ColumnNothing is a special column type, it is used to represent a column that
        // is not materialized, so we don't need to check it.
        if (check_and_get_column<ColumnNothing>(elem.column.get())) {
            continue;
        }

        const auto& type = elem.type;
        const auto& column = elem.column;

        RETURN_IF_ERROR(column->column_self_check());
        auto st = type->check_column(*column);
        if (!st.ok()) {
            return Status::InternalError(
                    "Column {} in block is not compatible with its column type :{}, data type :{}, "
                    "error: {}",
                    elem.name, column->get_name(), type->get_name(), st.msg());
        }
    }
#endif
    return Status::OK();
}

size_t Block::rows() const {
    for (const auto& elem : data) {
        if (elem.column) {
            return elem.column->size();
        }
    }

    return 0;
}

void Block::set_num_rows(size_t length) {
    if (rows() > length) {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->shrink(length);
            }
        }
    }
}

void Block::skip_num_rows(int64_t& length) {
    auto origin_rows = rows();
    if (origin_rows <= length) {
        clear();
        length -= origin_rows;
    } else {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->cut(length, origin_rows - length);
            }
        }
    }
}

size_t Block::bytes() const {
    size_t res = 0;
    for (const auto& elem : data) {
        if (!elem.column) {
            std::stringstream ss;
            for (const auto& e : data) {
                ss << e.name + " ";
            }
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Column {} in block is nullptr, in method bytes. All Columns are {}",
                            elem.name, ss.str());
        }
        res += elem.column->byte_size();
    }

    return res;
}

std::string Block::columns_bytes() const {
    std::stringstream res;
    res << "column bytes: [";
    for (const auto& elem : data) {
        if (!elem.column) {
            std::stringstream ss;
            for (const auto& e : data) {
                ss << e.name + " ";
            }
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Column {} in block is nullptr, in method bytes. All Columns are {}",
                            elem.name, ss.str());
        }
        res << ", " << elem.column->byte_size();
    }
    res << "]";
    return res.str();
}

size_t Block::allocated_bytes() const {
    size_t res = 0;
    for (const auto& elem : data) {
        if (!elem.column) {
            // Sometimes if expr failed, then there will be a nullptr
            // column left in the block.
            continue;
        }
        res += elem.column->allocated_bytes();
    }

    return res;
}

std::string Block::dump_names() const {
    std::string out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out += ", ";
        }
        out += it->name;
    }
    return out;
}

std::string Block::dump_types() const {
    std::string out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out += ", ";
        }
        out += it->type->get_name();
    }
    return out;
}

std::string Block::dump_data_json(size_t begin, size_t row_limit, bool allow_null_mismatch) const {
    std::stringstream ss;

    std::vector<std::string> headers;
    headers.reserve(columns());
    for (const auto& it : data) {
        // fmt::format is from the {fmt} library, you might be using std::format in C++20
        // If not, you can build the string with a stringstream as a fallback.
        headers.push_back(fmt::format("{}({})", it.name, it.type->get_name()));
    }

    size_t start_row = std::min(begin, rows());
    size_t end_row = std::min(rows(), begin + row_limit);

    auto format_options = DataTypeSerDe::get_default_format_options();
    auto time_zone = cctz::utc_time_zone();
    format_options.timezone = &time_zone;

    ss << "[";
    for (size_t row_num = start_row; row_num < end_row; ++row_num) {
        if (row_num > start_row) {
            ss << ",";
        }
        ss << "{";
        for (size_t i = 0; i < columns(); ++i) {
            if (i > 0) {
                ss << ",";
            }
            ss << "\"" << headers[i] << "\":";
            std::string s;

            // This value-extraction logic is preserved from your original function
            // to maintain consistency, especially for handling nullability mismatches.
            if (data[i].column && data[i].type->is_nullable() &&
                !data[i].column->is_concrete_nullable()) {
                // This branch handles a specific internal representation of nullable columns.
                // The original code would assert here if allow_null_mismatch is false.
                assert(allow_null_mismatch);
                s = assert_cast<const DataTypeNullable*>(data[i].type.get())
                            ->get_nested_type()
                            ->to_string(*data[i].column, row_num, format_options);
            } else {
                // This is the standard path. The to_string method is expected to correctly
                // handle all cases, including when the column is null (e.g., by returning "NULL").
                s = data[i].to_string(row_num, format_options);
            }
            ss << "\"" << s << "\"";
        }
        ss << "}";
    }
    ss << "]";
    return ss.str();
}

std::string Block::dump_data(size_t begin, size_t row_limit, bool allow_null_mismatch) const {
    std::vector<std::string> headers;
    std::vector<int> headers_size;
    for (const auto& it : data) {
        std::string s = fmt::format("{}({})", it.name, it.type->get_name());
        headers_size.push_back(s.size() > 15 ? (int)s.size() : 15);
        headers.emplace_back(s);
    }

    std::stringstream out;
    // header upper line
    auto line = [&]() {
        for (size_t i = 0; i < columns(); ++i) {
            out << std::setfill('-') << std::setw(1) << "+" << std::setw(headers_size[i]) << "-";
        }
        out << std::setw(1) << "+" << std::endl;
    };
    line();
    // header text
    for (size_t i = 0; i < columns(); ++i) {
        out << std::setfill(' ') << std::setw(1) << "|" << std::left << std::setw(headers_size[i])
            << headers[i];
    }
    out << std::setw(1) << "|" << std::endl;
    // header bottom line
    line();
    if (rows() == 0) {
        return out.str();
    }

    auto format_options = DataTypeSerDe::get_default_format_options();
    auto time_zone = cctz::utc_time_zone();
    format_options.timezone = &time_zone;

    // content
    for (size_t row_num = begin; row_num < rows() && row_num < row_limit + begin; ++row_num) {
        for (size_t i = 0; i < columns(); ++i) {
            if (!data[i].column || data[i].column->empty()) {
                out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                    << std::right;
                continue;
            }
            std::string s;
            if (data[i].column) { // column may be const
                // for code inside `default_implementation_for_nulls`, there's could have: type = null, col != null
                if (data[i].type->is_nullable() && !data[i].column->is_concrete_nullable()) {
                    assert(allow_null_mismatch);
                    s = assert_cast<const DataTypeNullable*>(data[i].type.get())
                                ->get_nested_type()
                                ->to_string(*data[i].column, row_num, format_options);
                } else {
                    s = data[i].to_string(row_num, format_options);
                }
            }
            if (s.length() > headers_size[i]) {
                s = s.substr(0, headers_size[i] - 3) + "...";
            }
            out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                << std::right << s;
        }
        out << std::setw(1) << "|" << std::endl;
    }
    // bottom line
    line();
    if (row_limit < rows()) {
        out << rows() << " rows in block, only show first " << row_limit << " rows." << std::endl;
    }
    return out.str();
}

std::string Block::dump_one_line(size_t row, int column_end) const {
    assert(column_end <= columns());
    fmt::memory_buffer line;

    auto format_options = DataTypeSerDe::get_default_format_options();
    auto time_zone = cctz::utc_time_zone();
    format_options.timezone = &time_zone;

    for (int i = 0; i < column_end; ++i) {
        if (LIKELY(i != 0)) {
            // TODO: need more effective function of to string. now the impl is slow
            fmt::format_to(line, " {}", data[i].to_string(row, format_options));
        } else {
            fmt::format_to(line, "{}", data[i].to_string(row, format_options));
        }
    }
    return fmt::to_string(line);
}

std::string Block::dump_structure() const {
    std::string out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out += ", \n";
        }
        out += it->dump_structure();
    }
    return out;
}

Block Block::clone_empty() const {
    Block res;
    for (const auto& elem : data) {
        res.insert(elem.clone_empty());
    }
    return res;
}

MutableColumns Block::clone_empty_columns() const {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column ? data[i].column->clone_empty() : data[i].type->create_column();
    }
    return columns;
}

Columns Block::get_columns() const {
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column->convert_to_full_column_if_const();
    }
    return columns;
}

Columns Block::get_columns_and_convert() {
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        data[i].column = data[i].column->convert_to_full_column_if_const();
        columns[i] = data[i].column;
    }
    return columns;
}

MutableColumns Block::mutate_columns() {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        DCHECK(data[i].type);
        columns[i] = data[i].column ? (*std::move(data[i].column)).mutate()
                                    : data[i].type->create_column();
    }
    return columns;
}

void Block::set_columns(MutableColumns&& columns) {
    DCHECK_GE(columns.size(), data.size())
            << fmt::format("Invalid size of columns, columns size: {}, data size: {}",
                           columns.size(), data.size());
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        data[i].column = std::move(columns[i]);
    }
}

Block Block::clone_with_columns(MutableColumns&& columns) const {
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        res.insert({std::move(columns[i]), data[i].type, data[i].name});
    }

    return res;
}

Block Block::clone_without_columns(const std::vector<int>* column_offset) const {
    Block res;

    if (column_offset != nullptr) {
        size_t num_columns = column_offset->size();
        for (size_t i = 0; i < num_columns; ++i) {
            res.insert({nullptr, data[(*column_offset)[i]].type, data[(*column_offset)[i]].name});
        }
    } else {
        size_t num_columns = data.size();
        for (size_t i = 0; i < num_columns; ++i) {
            res.insert({nullptr, data[i].type, data[i].name});
        }
    }
    return res;
}

const ColumnsWithTypeAndName& Block::get_columns_with_type_and_name() const {
    return data;
}

std::vector<std::string> Block::get_names() const {
    std::vector<std::string> res;
    res.reserve(columns());

    for (const auto& elem : data) {
        res.push_back(elem.name);
    }

    return res;
}

DataTypes Block::get_data_types() const {
    DataTypes res;
    res.reserve(columns());

    for (const auto& elem : data) {
        res.push_back(elem.type);
    }

    return res;
}

void Block::clear() {
    data.clear();
}

void Block::clear_column_data(int64_t column_size) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    // data.size() greater than column_size, means here have some
    // function exec result in block, need erase it here
    if (column_size != -1 and data.size() > column_size) {
        for (int64_t i = data.size() - 1; i >= column_size; --i) {
            erase(i);
        }
    }
    for (auto& d : data) {
        if (d.column) {
            // Temporarily disable reference count check because a column might be referenced multiple times within a block.
            // Queries like this: `select c, c from t1;`
            (*std::move(d.column)).assume_mutable()->clear();
        }
    }
}

void Block::clear_column_mem_not_keep(const std::vector<bool>& column_keep_flags,
                                      bool need_keep_first) {
    if (data.size() >= column_keep_flags.size()) {
        auto origin_rows = rows();
        for (size_t i = 0; i < column_keep_flags.size(); ++i) {
            if (!column_keep_flags[i]) {
                data[i].column = data[i].column->clone_empty();
            }
        }

        if (need_keep_first && !column_keep_flags[0]) {
            auto first_column = data[0].column->clone_empty();
            first_column->resize(origin_rows);
            data[0].column = std::move(first_column);
        }
    }
}

void Block::swap(Block& other) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    data.swap(other.data);
}

void Block::swap(Block&& other) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    data = std::move(other.data);
}

void Block::shuffle_columns(const std::vector<int>& result_column_ids) {
    Container tmp_data;
    tmp_data.reserve(result_column_ids.size());
    for (const int result_column_id : result_column_ids) {
        tmp_data.push_back(data[result_column_id]);
    }
    data = std::move(tmp_data);
}

void Block::update_hash(SipHash& hash) const {
    for (size_t row_no = 0, num_rows = rows(); row_no < num_rows; ++row_no) {
        for (const auto& col : data) {
            col.column->update_hash_with_value(row_no, hash);
        }
    }
}

/**
 * 根据过滤器对 Block 中指定的列进行过滤（内部方法）
 * 
 * @param block 要过滤的 Block 指针
 * @param columns_to_filter 需要过滤的列索引列表
 * @param filter 过滤器，每个元素为 0（过滤掉）或 1（保留）
 * 
 * 过滤逻辑：
 * 1. 计算过滤后应保留的行数（filter 中 1 的个数）
 * 2. 对每个需要过滤的列：
 *    - 如果列大小已等于过滤后的行数，跳过（已过滤过）
 *    - 如果过滤后行数为 0，清空列
 *    - 如果列是独占的（exclusive），原地过滤（修改原列）
 *    - 否则创建新的过滤后的列（不修改原列）
 */
void Block::filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                  const IColumn::Filter& filter) {
    // 计算过滤后应保留的行数：总行数 - filter 中 0 的个数
    // 使用 SIMD 优化计算 0 的个数，提高性能
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    
    // 遍历需要过滤的每一列
    for (const auto& col : columns_to_filter) {
        auto& column = block->get_by_position(col).column;
        
        // 如果列的大小已经等于过滤后的行数，说明已经过滤过了，跳过
        if (column->size() == count) {
            continue;
        }
        
        // 如果过滤后没有行需要保留，清空该列
        if (count == 0) {
            block->get_by_position(col).column->assume_mutable()->clear();
            continue;
        }
        
        // 如果列是独占的（exclusive），可以直接原地修改，避免创建新列
        // 这种方式更高效，因为不需要分配新内存
        if (column->is_exclusive()) {
            // 原地过滤，返回实际过滤后的行数
            const auto result_size = column->assume_mutable()->filter(filter);
            // 验证过滤后的行数是否与预期一致，不一致则抛出异常
            if (result_size != count) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "result_size not equal with filter_size, result_size={}, "
                                "filter_size={}",
                                result_size, count);
            }
        } else {
            // 列不是独占的，创建新的过滤后的列（不修改原列）
            // 这种方式适用于列被多个地方共享的情况
            column = column->filter(filter, count);
        }
    }
}

void Block::filter_block_internal(Block* block, const IColumn::Filter& filter,
                                  uint32_t column_to_keep) {
    std::vector<uint32_t> columns_to_filter;
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }
    filter_block_internal(block, columns_to_filter, filter);
}

void Block::filter_block_internal(Block* block, const IColumn::Filter& filter) {
    const size_t count =
            filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    for (int i = 0; i < block->columns(); ++i) {
        auto& column = block->get_by_position(i).column;
        if (column->is_exclusive()) {
            column->assume_mutable()->filter(filter);
        } else {
            column = column->filter(filter, count);
        }
    }
}

Status Block::append_to_block_by_selector(MutableBlock* dst,
                                          const IColumn::Selector& selector) const {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_EQ(data.size(), dst->mutable_columns().size());
        for (size_t i = 0; i < data.size(); i++) {
            // FIXME: this is a quickfix. we assume that only partition functions make there some
            if (!is_column_const(*data[i].column)) {
                data[i].column->append_data_by_selector(dst->mutable_columns()[i], selector);
            }
        }
    });
    return Status::OK();
}

Status Block::filter_block(Block* block, const std::vector<uint32_t>& columns_to_filter,
                           size_t filter_column_id, size_t column_to_keep) {
    const auto& filter_column = block->get_by_position(filter_column_id).column;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
        const auto& nested_column = nullable_column->get_nested_column_ptr();

        MutableColumnPtr mutable_holder =
                nested_column->use_count() == 1
                        ? nested_column->assume_mutable()
                        : nested_column->clone_resized(nested_column->size());

        auto* concrete_column = assert_cast<ColumnUInt8*>(mutable_holder.get());
        const auto* __restrict null_map = nullable_column->get_null_map_data().data();
        IColumn::Filter& filter = concrete_column->get_data();
        auto* __restrict filter_data = filter.data();

        const size_t size = filter.size();
        for (size_t i = 0; i < size; ++i) {
            filter_data[i] &= !null_map[i];
        }
        RETURN_IF_CATCH_EXCEPTION(filter_block_internal(block, columns_to_filter, filter));
    } else if (const auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (!ret) {
            for (const auto& col : columns_to_filter) {
                std::move(*block->get_by_position(col).column).assume_mutable()->clear();
            }
        }
    } else {
        const IColumn::Filter& filter =
                assert_cast<const doris::vectorized::ColumnUInt8&>(*filter_column).get_data();
        RETURN_IF_CATCH_EXCEPTION(filter_block_internal(block, columns_to_filter, filter));
    }

    erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status Block::filter_block(Block* block, size_t filter_column_id, size_t column_to_keep) {
    std::vector<uint32_t> columns_to_filter;
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }
    return filter_block(block, columns_to_filter, filter_column_id, column_to_keep);
}

Status Block::serialize(int be_exec_version, PBlock* pblock,
                        /*std::string* compressed_buffer,*/ size_t* uncompressed_bytes,
                        size_t* compressed_bytes, int64_t* compress_time,
                        segment_v2::CompressionTypePB compression_type,
                        bool allow_transfer_large_data) const {
    RETURN_IF_ERROR(BeExecVersionManager::check_be_exec_version(be_exec_version));
    pblock->set_be_exec_version(be_exec_version);

    // calc uncompressed size for allocation
    size_t content_uncompressed_size = 0;
    for (const auto& c : *this) {
        PColumnMeta* pcm = pblock->add_column_metas();
        c.to_pb_column_meta(pcm);
        DCHECK(pcm->type() != PGenericType::UNKNOWN) << " forget to set pb type";
        // get serialized size
        content_uncompressed_size +=
                c.type->get_uncompressed_serialized_bytes(*(c.column), pblock->be_exec_version());
    }

    // serialize data values
    // when data type is HLL, content_uncompressed_size maybe larger than real size.
    std::string column_values;
    try {
        // TODO: After support c++23, we should use resize_and_overwrite to replace resize
        column_values.resize(content_uncompressed_size);
    } catch (...) {
        std::string msg = fmt::format("Try to alloc {} bytes for pblock column values failed.",
                                      content_uncompressed_size);
        LOG(WARNING) << msg;
        return Status::BufferAllocFailed(msg);
    }
    char* buf = column_values.data();

    for (const auto& c : *this) {
        buf = c.type->serialize(*(c.column), buf, pblock->be_exec_version());
    }
    *uncompressed_bytes = content_uncompressed_size;
    const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
    *compressed_bytes = serialize_bytes;
    column_values.resize(serialize_bytes);

    // compress
    if (compression_type != segment_v2::NO_COMPRESSION && content_uncompressed_size > 0) {
        SCOPED_RAW_TIMER(compress_time);
        pblock->set_compression_type(compression_type);
        pblock->set_uncompressed_size(serialize_bytes);

        BlockCompressionCodec* codec;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));

        faststring buf_compressed;
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
                codec->compress(Slice(column_values.data(), serialize_bytes), &buf_compressed));
        size_t compressed_size = buf_compressed.size();
        if (LIKELY(compressed_size < serialize_bytes)) {
            // TODO: rethink the logic here may copy again ?
            pblock->set_column_values(buf_compressed.data(), buf_compressed.size());
            pblock->set_compressed(true);
            *compressed_bytes = compressed_size;
        } else {
            pblock->set_column_values(std::move(column_values));
        }

        VLOG_ROW << "uncompressed size: " << content_uncompressed_size
                 << ", compressed size: " << compressed_size;
    } else {
        pblock->set_column_values(std::move(column_values));
    }
    if (!allow_transfer_large_data && *compressed_bytes >= std::numeric_limits<int32_t>::max()) {
        return Status::InternalError("The block is large than 2GB({}), can not send by Protobuf.",
                                     *compressed_bytes);
    }
    return Status::OK();
}

size_t MutableBlock::rows() const {
    for (const auto& column : _columns) {
        if (column) {
            return column->size();
        }
    }

    return 0;
}

void MutableBlock::swap(MutableBlock& another) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    _columns.swap(another._columns);
    _data_types.swap(another._data_types);
    _names.swap(another._names);
}

void MutableBlock::add_row(const Block* block, int row) {
    const auto& block_data = block->get_columns_with_type_and_name();
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->insert_from(*block_data[i].column.get(), row);
    }
}

/**
 * 批量添加行数据（按索引数组）
 * 
 * 【功能说明】
 * 从源 Block 中按照指定的行索引数组，批量复制行数据到当前 MutableBlock。
 * 支持列重映射（通过 column_offset 参数），可以从源 Block 的不同列位置复制数据。
 * 
 * 【核心特点】
 * 1. 按索引复制：不是连续复制，而是按照 row_begin 到 row_end 之间的索引数组复制
 * 2. 列重映射：可以通过 column_offset 参数重新映射列的位置
 * 3. 批量操作：一次性复制多行数据，比逐行复制更高效
 * 4. 类型检查：确保源列和目标列的类型匹配
 * 
 * 【使用场景】
 * - Hash Join 分区：将数据按照分区索引复制到不同的分区块中
 * - 数据过滤：按照过滤后的行索引复制数据
 * - 列重排：通过 column_offset 重新排列列的顺序
 * 
 * 【参数说明】
 * @param block 源 Block，从中复制数据
 * @param row_begin 行索引数组的起始指针（指向 uint32_t 数组）
 * @param row_end 行索引数组的结束指针（指向 uint32_t 数组的末尾，不包含）
 * @param column_offset 列偏移映射（可选）
 *   - nullptr：按顺序复制列（第 i 列复制到第 i 列）
 *   - 非空：column_offset[i] 表示目标第 i 列对应源 Block 的第 column_offset[i] 列
 * 
 * 【工作流程】
 * 1. 参数验证：检查列数是否匹配，column_offset 大小是否匹配
 * 2. 获取源 Block 的列数据
 * 3. 遍历目标 Block 的每一列：
 *    - 根据 column_offset 确定源列位置（如果有 column_offset，使用 column_offset[i]，否则使用 i）
 *    - 检查源列和目标列的类型是否匹配
 *    - 调用 insert_indices_from 按索引数组复制数据
 * 
 * 【示例】
 * 
 * 示例 1：按顺序复制列
 * ```cpp
 * // 源 Block：3 列，10 行
 * Block src_block;
 * 
 * // 目标 MutableBlock：3 列，0 行
 * MutableBlock dst_block;
 * 
 * // 行索引数组：复制第 2、5、7 行
 * uint32_t indices[] = {2, 5, 7};
 * 
 * // 复制数据（按顺序复制列）
 * dst_block.add_rows(&src_block, indices, indices + 3, nullptr);
 * // 结果：dst_block 有 3 行数据（来自 src_block 的第 2、5、7 行）
 * ```
 * 
 * 示例 2：列重映射
 * ```cpp
 * // 源 Block：5 列，10 行
 * Block src_block;  // 列：A, B, C, D, E
 * 
 * // 目标 MutableBlock：3 列，0 行
 * MutableBlock dst_block;  // 列：X, Y, Z
 * 
 * // 列映射：X ← C, Y ← A, Z ← E
 * std::vector<int> column_offset = {2, 0, 4};
 * 
 * // 行索引数组：复制第 1、3、5 行
 * uint32_t indices[] = {1, 3, 5};
 * 
 * // 复制数据（列重映射）
 * dst_block.add_rows(&src_block, indices, indices + 3, &column_offset);
 * // 结果：dst_block 有 3 行数据
 * //   - 第 0 列（X）：来自 src_block 的第 2 列（C）的第 1、3、5 行
 * //   - 第 1 列（Y）：来自 src_block 的第 0 列（A）的第 1、3、5 行
 * //   - 第 2 列（Z）：来自 src_block 的第 4 列（E）的第 1、3、5 行
 * ```
 * 
 * 示例 3：Hash Join 分区
 * ```cpp
 * // 在 PartitionedHashJoinSink 中，将数据按照分区索引复制到不同的分区块
 * 
 * // 源 Block：待分区的数据
 * Block* in_block;
 * 
 * // 分区索引数组：partition_indexes[i] 存储属于分区 i 的行索引
 * std::vector<std::vector<uint32_t>> partition_indexes(partition_count);
 * 
 * // 计算每行属于哪个分区
 * for (size_t i = 0; i < rows; ++i) {
 *     partition_indexes[channel_ids[i]].emplace_back(i);
 * }
 * 
 * // 将数据复制到各个分区块
 * for (uint32_t i = 0; i < partition_count; ++i) {
 *     if (!partition_indexes[i].empty()) {
 *         partitioned_blocks[i]->add_rows(
 *             in_block,
 *             partition_indexes[i].data(),
 *             partition_indexes[i].data() + partition_indexes[i].size(),
 *             nullptr  // 按顺序复制列
 *         );
 *     }
 * }
 * ```
 * 
 * 【注意事项】
 * 1. row_begin 和 row_end 必须指向有效的行索引数组
 * 2. 行索引必须在源 Block 的有效范围内（0 到 rows()-1）
 * 3. 如果使用 column_offset，必须确保 column_offset[i] 在源 Block 的有效列范围内
 * 4. 源列和目标列的类型必须匹配（通过类型名称检查）
 * 5. 该方法会捕获异常并返回 Status，确保内存分配失败时能正确处理
 */
Status MutableBlock::add_rows(const Block* block, const uint32_t* row_begin,
                              const uint32_t* row_end, const std::vector<int>* column_offset) {
    RETURN_IF_CATCH_EXCEPTION({
        // ===== 步骤 1：参数验证 =====
        // 检查目标 Block 的列数不能超过源 Block 的列数
        DCHECK_LE(columns(), block->columns());
        
        // 如果提供了 column_offset，检查其大小必须等于目标 Block 的列数
        if (column_offset != nullptr) {
            DCHECK_EQ(columns(), column_offset->size());
        }
        
        // ===== 步骤 2：获取源 Block 的列数据 =====
        const auto& block_data = block->get_columns_with_type_and_name();
        
        // ===== 步骤 3：遍历目标 Block 的每一列，复制数据 =====
        for (size_t i = 0; i < _columns.size(); ++i) {
            // ===== 步骤 3A：确定源列位置 =====
            // 如果提供了 column_offset，使用 column_offset[i] 作为源列索引
            // 否则，使用 i 作为源列索引（按顺序复制）
            const auto& src_col = column_offset ? block_data[(*column_offset)[i]] : block_data[i];
            
            // ===== 步骤 3B：类型检查 =====
            // 确保源列和目标列的类型匹配（通过类型名称检查）
            DCHECK_EQ(_data_types[i]->get_name(), src_col.type->get_name());
            
            // ===== 步骤 3C：获取列引用 =====
            auto& dst = _columns[i];              // 目标列（MutableColumn）
            const auto& src = *src_col.column.get();  // 源列（IColumn）
            
            // ===== 步骤 3D：范围检查 =====
            // 确保源列的行数足够（至少要有 row_end - row_begin 行）
            DCHECK_GE(src.size(), row_end - row_begin);
            
            // ===== 步骤 3E：按索引数组复制数据 =====
            // insert_indices_from 会按照 row_begin 到 row_end 之间的索引数组，
            // 从源列中复制对应的行数据到目标列
            // 例如：如果 row_begin 指向 [2, 5, 7]，则复制源列的第 2、5、7 行
            dst->insert_indices_from(src, row_begin, row_end);
        }
    });
    return Status::OK();
}

Status MutableBlock::add_rows(const Block* block, size_t row_begin, size_t length) {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_LE(columns(), block->columns());
        const auto& block_data = block->get_columns_with_type_and_name();
        for (size_t i = 0; i < _columns.size(); ++i) {
            DCHECK_EQ(_data_types[i]->get_name(), block_data[i].type->get_name());
            auto& dst = _columns[i];
            const auto& src = *block_data[i].column.get();
            dst->insert_range_from(src, row_begin, length);
        }
    });
    return Status::OK();
}

Status MutableBlock::add_rows(const Block* block, const std::vector<int64_t>& rows) {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_LE(columns(), block->columns());
        const auto& block_data = block->get_columns_with_type_and_name();
        const size_t length = std::ranges::distance(rows);
        for (size_t i = 0; i < _columns.size(); ++i) {
            DCHECK_EQ(_data_types[i]->get_name(), block_data[i].type->get_name());
            auto& dst = _columns[i];
            const auto& src = *block_data[i].column.get();
            dst->reserve(dst->size() + length);
            for (auto row : rows) {
                // we can introduce a new function like `insert_assume_reserved` for IColumn.
                dst->insert_from(src, row);
            }
        }
    });
    return Status::OK();
}

Block MutableBlock::to_block(int start_column) {
    return to_block(start_column, (int)_columns.size());
}

Block MutableBlock::to_block(int start_column, int end_column) {
    ColumnsWithTypeAndName columns_with_schema;
    columns_with_schema.reserve(end_column - start_column);
    for (size_t i = start_column; i < end_column; ++i) {
        columns_with_schema.emplace_back(std::move(_columns[i]), _data_types[i], _names[i]);
    }
    return {columns_with_schema};
}

std::string MutableBlock::dump_data_json(size_t row_limit) const {
    std::stringstream ss;
    std::vector<std::string> headers;

    headers.reserve(columns());
    for (size_t i = 0; i < columns(); ++i) {
        headers.push_back(_data_types[i]->get_name());
    }
    size_t num_rows_to_dump = std::min(rows(), row_limit);
    ss << "[";

    auto format_options = DataTypeSerDe::get_default_format_options();
    auto time_zone = cctz::utc_time_zone();
    format_options.timezone = &time_zone;

    for (size_t row_num = 0; row_num < num_rows_to_dump; ++row_num) {
        if (row_num > 0) {
            ss << ",";
        }
        ss << "{";
        for (size_t i = 0; i < columns(); ++i) {
            if (i > 0) {
                ss << ",";
            }
            ss << "\"" << headers[i] << "\":";
            std::string s = _data_types[i]->to_string(*_columns[i].get(), row_num, format_options);
            ss << "\"" << s << "\"";
        }
        ss << "}";
    }
    ss << "]";
    return ss.str();
}

std::string MutableBlock::dump_data(size_t row_limit) const {
    std::vector<std::string> headers;
    std::vector<int> headers_size;
    for (size_t i = 0; i < columns(); ++i) {
        std::string s = _data_types[i]->get_name();
        headers_size.push_back(s.size() > 15 ? (int)s.size() : 15);
        headers.emplace_back(s);
    }

    std::stringstream out;
    // header upper line
    auto line = [&]() {
        for (size_t i = 0; i < columns(); ++i) {
            out << std::setfill('-') << std::setw(1) << "+" << std::setw(headers_size[i]) << "-";
        }
        out << std::setw(1) << "+" << std::endl;
    };
    line();
    // header text
    for (size_t i = 0; i < columns(); ++i) {
        out << std::setfill(' ') << std::setw(1) << "|" << std::left << std::setw(headers_size[i])
            << headers[i];
    }
    out << std::setw(1) << "|" << std::endl;
    // header bottom line
    line();
    if (rows() == 0) {
        return out.str();
    }

    auto format_options = DataTypeSerDe::get_default_format_options();
    auto time_zone = cctz::utc_time_zone();
    format_options.timezone = &time_zone;

    // content
    for (size_t row_num = 0; row_num < rows() && row_num < row_limit; ++row_num) {
        for (size_t i = 0; i < columns(); ++i) {
            if (_columns[i].get()->empty()) {
                out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                    << std::right;
                continue;
            }
            std::string s = _data_types[i]->to_string(*_columns[i].get(), row_num, format_options);
            if (s.length() > headers_size[i]) {
                s = s.substr(0, headers_size[i] - 3) + "...";
            }
            out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                << std::right << s;
        }
        out << std::setw(1) << "|" << std::endl;
    }
    // bottom line
    line();
    if (row_limit < rows()) {
        out << rows() << " rows in block, only show first " << row_limit << " rows." << std::endl;
    }
    return out.str();
}

std::unique_ptr<Block> Block::create_same_struct_block(size_t size, bool is_reserve) const {
    auto temp_block = Block::create_unique();
    for (const auto& d : data) {
        auto column = d.type->create_column();
        if (is_reserve) {
            column->reserve(size);
        } else {
            column->insert_many_defaults(size);
        }
        temp_block->insert({std::move(column), d.type, d.name});
    }
    return temp_block;
}

void Block::shrink_char_type_column_suffix_zero(const std::vector<size_t>& char_type_idx) {
    for (auto idx : char_type_idx) {
        if (idx < data.size()) {
            auto& col_and_name = this->get_by_position(idx);
            col_and_name.column->assume_mutable()->shrink_padding_chars();
        }
    }
}

size_t MutableBlock::allocated_bytes() const {
    size_t res = 0;
    for (const auto& col : _columns) {
        if (col) {
            res += col->allocated_bytes();
        }
    }

    return res;
}

void MutableBlock::clear_column_data() noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    for (auto& col : _columns) {
        if (col) {
            col->clear();
        }
    }
}

std::string MutableBlock::dump_names() const {
    std::string out;
    for (auto it = _names.begin(); it != _names.end(); ++it) {
        if (it != _names.begin()) {
            out += ", ";
        }
        out += *it;
    }
    return out;
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
