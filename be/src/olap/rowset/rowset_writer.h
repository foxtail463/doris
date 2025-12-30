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

#pragma once

#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>

#include <functional>
#include <memory>
#include <optional>

#include "common/factory_creator.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/column_mapping.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"
#include "vec/core/block.h"

namespace doris {

struct SegmentStatistics {
    int64_t row_num;
    int64_t data_size;
    int64_t index_size;
    KeyBoundsPB key_bounds;

    SegmentStatistics() = default;

    SegmentStatistics(SegmentStatisticsPB pb)
            : row_num(pb.row_num()),
              data_size(pb.data_size()),
              index_size(pb.index_size()),
              key_bounds(pb.key_bounds()) {}

    void to_pb(SegmentStatisticsPB* segstat_pb) const {
        segstat_pb->set_row_num(row_num);
        segstat_pb->set_data_size(data_size);
        segstat_pb->set_index_size(index_size);
        segstat_pb->mutable_key_bounds()->CopyFrom(key_bounds);
    }

    std::string to_string() {
        std::stringstream ss;
        ss << "row_num: " << row_num << ", data_size: " << data_size
           << ", index_size: " << index_size << ", key_bounds: " << key_bounds.ShortDebugString();
        return ss.str();
    }
};
using SegmentStatisticsSharedPtr = std::shared_ptr<SegmentStatistics>;

/**
 * 行集写入器抽象基类
 * 定义了行集写入的通用接口，包括数据添加、刷新、构建等操作
 * 具体的写入器实现类需要继承此类并实现相应的虚函数
 */
class RowsetWriter {
public:
    RowsetWriter() = default;
    virtual ~RowsetWriter() = default;

    /**
     * 初始化行集写入器
     * @param rowset_writer_context 行集写入上下文
     * @return 初始化状态
     */
    virtual Status init(const RowsetWriterContext& rowset_writer_context) = 0;

    /**
     * 添加数据块到行集
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param block 向量化的数据块
     * @return 操作状态
     */
    virtual Status add_block(const vectorized::Block* block) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support add_block");
    }
    
    /**
     * 添加指定列的数据到行集
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param block 数据块
     * @param col_ids 列ID列表
     * @param is_key 是否为键列
     * @param max_rows_per_segment 每个段的最大行数
     * @param has_cluster_key 是否有集群键
     * @return 操作状态
     */
    virtual Status add_columns(const vectorized::Block* block, const std::vector<uint32_t>& col_ids,
                               bool is_key, uint32_t max_rows_per_segment, bool has_cluster_key) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support add_columns");
    }

    /**
     * 添加现有行集到当前行集
     * 前置条件：输入的rowset应该与正在构建的行集具有相同的类型
     * @param rowset 要添加的行集
     * @return 操作状态
     */
    virtual Status add_rowset(RowsetSharedPtr rowset) = 0;

    /**
     * 为链接的schema变更添加行集
     * 前置条件：输入的rowset应该与正在构建的行集具有相同的类型
     * @param rowset 要添加的行集
     * @return 操作状态
     */
    virtual Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) = 0;

    /**
     * 创建段文件写入器
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param segment_id 段ID
     * @param writer 文件写入器指针
     * @param file_type 文件类型，默认为段文件
     * @return 操作状态
     */
    virtual Status create_file_writer(uint32_t segment_id, io::FileWriterPtr& writer,
                                      FileType file_type = FileType::SEGMENT_FILE) {
        return Status::NotSupported("RowsetWriter does not support create_file_writer");
    }

    /**
     * 创建倒排索引文件写入器
     * 为倒排索引格式v2创建文件写入器
     * @param segment_id 段ID
     * @param index_file_writer 索引文件写入器指针
     * @return 操作状态
     */
    virtual Status create_index_file_writer(uint32_t segment_id,
                                            IndexFileWriterPtr* index_file_writer) {
        // 为倒排索引格式v2创建文件写入器
        io::FileWriterPtr idx_file_v2_ptr;
        if (_context.tablet_schema->get_inverted_index_storage_format() !=
            InvertedIndexStorageFormatPB::V1) {
            RETURN_IF_ERROR(
                    create_file_writer(segment_id, idx_file_v2_ptr, FileType::INVERTED_INDEX_FILE));
        }
        
        // 获取段路径前缀
        std::string segment_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                _context.segment_path(segment_id))};
        
        // 默认可以使用RAM目录，仅在基础压缩时需要检查配置
        bool can_use_ram_dir = true;
        if (_context.compaction_type == ReaderType::READER_BASE_COMPACTION) {
            can_use_ram_dir = config::inverted_index_ram_dir_enable_when_base_compaction;
        }
        
        // 创建索引文件写入器
        *index_file_writer = std::make_unique<IndexFileWriter>(
                _context.fs(), segment_prefix, _context.rowset_id.to_string(), segment_id,
                _context.tablet_schema->get_inverted_index_storage_format(),
                std::move(idx_file_v2_ptr), can_use_ram_dir);
        return Status::OK();
    }

    /**
     * 显式刷新所有缓冲的行到段文件
     * 注意：当满足某些条件时，add_row也可能触发刷新
     * @return 操作状态
     */
    virtual Status flush() = 0;
    
    /**
     * 刷新指定列的数据
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param is_key 是否为键列
     * @return 操作状态
     */
    virtual Status flush_columns(bool is_key) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support flush_columns");
    }
    
    /**
     * 最终刷新操作
     * 默认实现返回不支持错误，子类需要重写此方法
     * @return 操作状态
     */
    virtual Status final_flush() {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support final_flush");
    }

    /**
     * 刷新内存表数据到指定段
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param block 数据块
     * @param segment_id 段ID
     * @param flush_size 刷新到磁盘的文件大小
     * @return 操作状态
     */
    virtual Status flush_memtable(vectorized::Block* block, int32_t segment_id,
                                  int64_t* flush_size) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support flush_memtable");
    }

    /**
     * 刷新单个数据块
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param block 数据块
     * @return 操作状态
     */
    virtual Status flush_single_block(const vectorized::Block* block) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support flush_single_block");
    }

    /**
     * 添加段统计信息
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param segment_id 段ID
     * @param segstat 段统计信息
     * @return 操作状态
     */
    virtual Status add_segment(uint32_t segment_id, const SegmentStatistics& segstat) {
        return Status::NotSupported("RowsetWriter does not support add_segment");
    }

    /**
     * 完成构建并设置行集指针到构建的行集（保证已初始化）
     * 如果返回的Status不是OK，则rowset无效
     * @param rowset 输出的行集指针
     * @return 构建状态
     */
    virtual Status build(RowsetSharedPtr& rowset) = 0;

    /**
     * 手动构建行集（用于有序行集压缩）
     * @param rowset_meta 行集元数据
     * @return 构建的行集指针
     */
    virtual RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) = 0;

    /**
     * 获取加载ID
     * @return 加载ID
     */
    virtual PUniqueId load_id() = 0;

    /**
     * 获取版本信息
     * @return 版本对象
     */
    virtual Version version() = 0;

    /**
     * 获取已写入的行数
     * @return 行数
     */
    virtual int64_t num_rows() const = 0;

    /**
     * 部分更新相关的统计信息
     */
    virtual int64_t num_rows_updated() const = 0;      // 更新的行数
    virtual int64_t num_rows_deleted() const = 0;      // 删除的行数
    virtual int64_t num_rows_new_added() const = 0;    // 新添加的行数
    virtual int64_t num_rows_filtered() const = 0;     // 过滤的行数

    /**
     * 获取行集ID
     * @return 行集ID
     */
    virtual RowsetId rowset_id() = 0;

    /**
     * 获取行集类型
     * @return 行集类型
     */
    virtual RowsetTypePB type() const = 0;

    /**
     * 获取每个段的行数统计
     * 默认实现返回不支持错误，子类需要重写此方法
     * @param segment_num_rows 段行数统计列表
     * @return 操作状态
     */
    virtual Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const {
        return Status::NotSupported("to be implemented");
    }

    /**
     * 分配段ID
     * @return 分配的段ID
     */
    virtual int32_t allocate_segment_id() = 0;

    /**
     * 设置段起始ID
     * 默认实现抛出异常，子类需要重写此方法
     * @param num_segment 段数量
     */
    virtual void set_segment_start_id(int num_segment) {
        throw Exception(Status::FatalError("not supported!"));
    }

    /**
     * 获取删除位图计算耗时（纳秒）
     * 默认实现返回0，子类需要重写此方法
     * @return 耗时
     */
    virtual int64_t delete_bitmap_ns() { return 0; }

    /**
     * 获取段写入耗时（纳秒）
     * 默认实现返回0，子类需要重写此方法
     * @return 耗时
     */
    virtual int64_t segment_writer_ns() { return 0; }

    /**
     * 获取部分更新信息
     * @return 部分更新信息指针
     */
    virtual std::shared_ptr<PartialUpdateInfo> get_partial_update_info() = 0;

    /**
     * 判断是否为部分更新
     * @return 是否为部分更新
     */
    virtual bool is_partial_update() = 0;

    /**
     * 获取行集写入上下文
     * @return 上下文引用
     */
    const RowsetWriterContext& context() { return _context; }

    /**
     * 获取行集元数据
     * @return 元数据引用
     */
    const RowsetMetaSharedPtr& rowset_meta() { return _rowset_meta; }

private:
    DISALLOW_COPY_AND_ASSIGN(RowsetWriter);  // 禁止拷贝和赋值

protected:
    RowsetWriterContext _context;      // 行集写入上下文
    RowsetMetaSharedPtr _rowset_meta;  // 行集元数据
};

} // namespace doris
