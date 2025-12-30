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

#include "vec/exec/format/orc/orc_file_reader.h"

#include "util/runtime_profile.h"
#include "vec/common/custom_allocator.h"

namespace doris {
namespace vectorized {

/**
 * ORC 合并范围文件读取器构造函数
 * 用于将多个小的连续读取请求合并成一个大请求，减少网络IO次数
 * 
 * @param profile 运行时性能分析器
 * @param inner_reader 底层文件读取器（如HDFS、S3等）
 * @param range 需要合并读取的范围
 */
OrcMergeRangeFileReader::OrcMergeRangeFileReader(RuntimeProfile* profile,
                                                 io::FileReaderSPtr inner_reader,
                                                 io::PrefetchRange range)
        : _profile(profile), _inner_reader(std::move(inner_reader)), _range(std::move(range)) {
    // 设置文件大小（与底层读取器相同）
    _size = _inner_reader->size();
    // 统计应用字节数（范围大小）
    _statistics.apply_bytes += range.end_offset - range.start_offset;
    
    // 初始化性能分析计数器
    if (_profile != nullptr) {
        const char* random_profile = "MergedSmallIO";
        ADD_TIMER_WITH_LEVEL(_profile, random_profile, 1);
        _copy_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "CopyTime", random_profile, 1);
        _read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ReadTime", random_profile, 1);
        _request_io =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RequestIO", TUnit::UNIT, random_profile, 1);
        _merged_io =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MergedIO", TUnit::UNIT, random_profile, 1);
        _request_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RequestBytes", TUnit::BYTES,
                                                      random_profile, 1);
        _merged_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MergedBytes", TUnit::BYTES,
                                                     random_profile, 1);
        _apply_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "ApplyBytes", TUnit::BYTES,
                                                    random_profile, 1);
    }
}

/**
 * 实现合并范围读取的核心函数
 * 采用"一次读取整个范围，多次从缓存拷贝"的策略来优化小IO
 * 
 * @param offset 请求的起始偏移
 * @param result 输出缓冲区
 * @param bytes_read 实际读取的字节数
 * @param io_ctx IO上下文
 * @return 操作状态
 */
Status OrcMergeRangeFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                             const io::IOContext* io_ctx) {
    auto request_size = result.size;

    // 统计请求IO次数和字节数
    _statistics.request_io++;
    _statistics.request_bytes += request_size;

    // 处理空请求
    if (request_size == 0) {
        *bytes_read = 0;
        return Status::OK();
    }

    // 如果缓存为空，说明是第一次读取，需要从底层读取整个范围
    if (_cache == nullptr) {
        // 计算范围大小
        auto range_size = _range.end_offset - _range.start_offset;
        // 分配范围大小的缓存
        _cache = make_unique_buffer<char>(range_size);

        {
            // 计时：从底层读取整个范围的时间
            SCOPED_RAW_TIMER(&_statistics.read_time);
            Slice cache_slice = {_cache.get(), range_size};
            // 一次读取整个范围到缓存
            RETURN_IF_ERROR(
                    _inner_reader->read_at(_range.start_offset, cache_slice, bytes_read, io_ctx));
            // 统计合并IO次数和字节数
            _statistics.merged_io++;
            _statistics.merged_bytes += *bytes_read;
        }

        // 验证读取的字节数是否与期望一致
        if (*bytes_read != range_size) [[unlikely]] {
            return Status::InternalError(
                    "OrcMergeRangeFileReader use inner reader read bytes {} not eq expect size {}",
                    *bytes_read, range_size);
        }

        // 记录当前范围的起始偏移
        _current_start_offset = _range.start_offset;
    }

    // 计时：从缓存拷贝数据的时间
    SCOPED_RAW_TIMER(&_statistics.copy_time);
    // 计算在缓存中的偏移位置
    int64_t buffer_offset = offset - _current_start_offset;
    // 从缓存拷贝请求的数据到输出缓冲区
    memcpy(result.data, _cache.get() + buffer_offset, request_size);
    *bytes_read = request_size;
    return Status::OK();
}

/**
 * 收集性能分析数据
 * 在文件读取器关闭前调用，将统计信息更新到性能分析器中
 */
void OrcMergeRangeFileReader::_collect_profile_before_close() {
    if (_profile != nullptr) {
        // 更新各种性能计数器
        COUNTER_UPDATE(_copy_time, _statistics.copy_time);           // 拷贝时间
        COUNTER_UPDATE(_read_time, _statistics.read_time);           // 读取时间
        COUNTER_UPDATE(_request_io, _statistics.request_io);         // 请求IO次数
        COUNTER_UPDATE(_merged_io, _statistics.merged_io);            // 合并IO次数
        COUNTER_UPDATE(_request_bytes, _statistics.request_bytes);   // 请求字节数
        COUNTER_UPDATE(_merged_bytes, _statistics.merged_bytes);    // 合并字节数
        COUNTER_UPDATE(_apply_bytes, _statistics.apply_bytes);       // 应用字节数
        
        // 递归收集底层读取器的性能数据
        if (_inner_reader != nullptr) {
            _inner_reader->collect_profile_before_close();
        }
    }
}

} // namespace vectorized
} // namespace doris