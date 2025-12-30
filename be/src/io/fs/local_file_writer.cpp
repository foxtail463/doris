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

#include "io/fs/local_file_writer.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <ostream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/macros.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/data_dir.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"

namespace doris::io {
#include "common/compile_check_begin.h"
namespace {

Status sync_dir(const io::Path& dirname) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("sync_dir", Status::IOError(""));
    int fd;
    RETRY_ON_EINTR(fd, ::open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
    if (-1 == fd) {
        return localfs_error(errno, fmt::format("failed to open {}", dirname.native()));
    }
    Defer defer {[fd] { ::close(fd); }};
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) < 0) {
        return localfs_error(errno, fmt::format("failed to sync {}", dirname.native()));
    }
#else
    if (0 != ::fdatasync(fd)) {
        return localfs_error(errno, fmt::format("failed to sync {}", dirname.native()));
    }
#endif
    return Status::OK();
}

} // namespace

LocalFileWriter::LocalFileWriter(Path path, int fd, bool sync_data)
        : _path(std::move(path)), _fd(fd), _sync_data(sync_data) {
    DorisMetrics::instance()->local_file_open_writing->increment(1);
    DorisMetrics::instance()->local_file_writer_total->increment(1);
}

size_t LocalFileWriter::bytes_appended() const {
    return _bytes_appended;
}

LocalFileWriter::~LocalFileWriter() {
    if (_state == State::OPENED) {
        _abort();
    }
    DorisMetrics::instance()->local_file_open_writing->increment(-1);
    DorisMetrics::instance()->file_created_total->increment(1);
    DorisMetrics::instance()->local_bytes_written_total->increment(_bytes_appended);
}

Status LocalFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::InternalError("LocalFileWriter already closed, file path {}",
                                     _path.native());
    }
    if (_state == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        // Actucally the first time call to close(true) would return the value of _finalize, if it returned one
        // error status then the code would never call the second close(true)
        _state = State::CLOSED;
        return Status::OK();
    }
    if (non_block) {
        _state = State::ASYNC_CLOSING;
    } else {
        _state = State::CLOSED;
    }
    return _close(_sync_data);
}

void LocalFileWriter::_abort() {
    auto st = _close(false);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "close file failed when abort file writer: " << st;
    }
    st = io::global_local_filesystem()->delete_file(_path);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "delete file failed when abort file writer: " << st;
    }
}

/**
 * 向量化写入多个数据片段到本地文件
 * 使用系统调用writev实现高效的批量写入，支持部分写入和重试机制
 * 
 * @param data 数据片段数组指针，每个Slice包含一段要写入的数据
 * @param data_cnt 数据片段的数量
 * @return 写入结果状态
 */
Status LocalFileWriter::appendv(const Slice* data, size_t data_cnt) {
    // 测试同步点：用于注入IO错误进行测试
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileWriter::appendv",
                                      Status::IOError("inject io error"));
    
    // 检查文件状态：只能向已打开的文件写入数据
    if (_state != State::OPENED) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }
    
    // 标记文件为脏状态，表示有未同步的数据
    _dirty = true;

    // 将Slice数组转换为iovec向量，并计算总字节数
    // iovec是系统调用writev使用的数据结构，包含数据指针和长度
    size_t bytes_req = 0;                    // 总请求字节数
    std::vector<iovec> iov(data_cnt);        // iovec向量，用于writev系统调用
    
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];        // 获取第i个数据片段
        bytes_req += result.size;             // 累加总字节数
        iov[i] = {result.data, result.size}; // 构造iovec结构：数据指针 + 长度
    }

    // 写入循环：处理部分写入的情况
    size_t completed_iov = 0;                 // 已完成的iovec数量
    size_t n_left = bytes_req;                // 剩余待写入的字节数
    
    while (n_left > 0) {
        // 单次请求的iovec数量不能超过系统限制IOV_MAX
        size_t iov_count = std::min(data_cnt - completed_iov, static_cast<size_t>(IOV_MAX));
        
        ssize_t res;  // 实际写入的字节数
        
        // 调用writev系统调用进行向量化写入
        // RETRY_ON_EINTR：在EINTR错误时自动重试
        // SYNC_POINT_HOOK_RETURN_VALUE：用于测试的同步点钩子
        RETRY_ON_EINTR(res, SYNC_POINT_HOOK_RETURN_VALUE(::writev(_fd, iov.data() + completed_iov,
                                                                  cast_set<int32_t>(iov_count)),
                                                         "LocalFileWriter::writev", _fd));
        
        // 调试执行点：用于测试时注入IO错误
        DBUG_EXECUTE_IF("LocalFileWriter::appendv.io_error", {
            auto sub_path = dp->param<std::string>("sub_path", "");
            if ((sub_path.empty() && _path.filename().compare(kTestFilePath)) ||
                (!sub_path.empty() && _path.native().find(sub_path) != std::string::npos)) {
                res = -1;                    // 模拟写入失败
                errno = EIO;                 // 设置IO错误
                LOG(WARNING) << Status::IOError("debug write io error: {}", _path.native());
            }
        });
        
        // 检查写入错误：如果返回负值表示写入失败
        if (UNLIKELY(res < 0)) {
            return localfs_error(errno, fmt::format("failed to write {}", _path.native()));
        }

        // 检查是否完全写入：如果写入字节数等于剩余字节数，说明全部写入完成
        if (LIKELY(res == n_left)) {
            // 所有请求的字节都已写入，这是最常见的情况
            n_left = 0;
            break;
        }
        
        // 处理部分写入：调整iovec向量，为下一次写入请求做准备
        ssize_t bytes_rem = res;  // 已写入的字节数
        
        for (size_t i = completed_iov; i < data_cnt; i++) {
            if (bytes_rem >= iov[i].iov_len) {
                // 这个iovec完全写入：移动到下一个iovec
                completed_iov++;
                bytes_rem -= iov[i].iov_len;
            } else {
                // 这个iovec部分写入：调整数据指针和长度，只写入剩余部分
                iov[i].iov_base = static_cast<uint8_t*>(iov[i].iov_base) + bytes_rem;  // 调整数据指针
                iov[i].iov_len -= bytes_rem;                                            // 调整剩余长度
                break; // 不需要调整剩余的iovec
            }
        }
        
        // 更新剩余待写入的字节数
        n_left -= res;
    }
    
    // 断言检查：确保所有数据都已写入
    DCHECK_EQ(0, n_left);
    
    // 更新已追加的字节数统计
    _bytes_appended += bytes_req;
    
    return Status::OK();
}

// TODO(ByteYue): Refactor this function as FileWriter::flush()
Status LocalFileWriter::_finalize() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileWriter::finalize",
                                      Status::IOError("inject io error"));
    if (_state == State::OPENED) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}", _path.native());
    }

    if (_dirty) {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return localfs_error(errno, fmt::format("failed to finalize {}", _path.native()));
        }
#endif
    }
    return Status::OK();
}

Status LocalFileWriter::_close(bool sync) {
    auto fd_reclaim_func = [&](Status st) {
        if (_fd > 0 && 0 != ::close(_fd)) {
            return localfs_error(errno, fmt::format("failed to {}, along with failed to close {}",
                                                    st, _path.native()));
        }
        _fd = -1;
        return st;
    };
    if (sync && config::sync_file_on_close) {
        if (_dirty) {
#ifdef __APPLE__
            if (fcntl(_fd, F_FULLFSYNC) < 0) [[unlikely]] {
                return fd_reclaim_func(
                        localfs_error(errno, fmt::format("failed to sync {}", _path.native())));
            }
#else
            if (0 != ::fdatasync(_fd)) [[unlikely]] {
                return fd_reclaim_func(
                        localfs_error(errno, fmt::format("failed to sync {}", _path.native())));
            }
#endif
            _dirty = false;
        }
        RETURN_IF_ERROR(fd_reclaim_func(sync_dir(_path.parent_path())));
    }

    DBUG_EXECUTE_IF("LocalFileWriter.close.failed", {
        // spare '.testfile' to make bad disk checker happy
        if (_path.filename().compare(kTestFilePath)) {
            return fd_reclaim_func(
                    Status::IOError("cannot close {}: {}", _path.native(), std::strerror(errno)));
        }
    });

    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileWriter::close", Status::IOError("inject io error"));
    return fd_reclaim_func(Status::OK());
}
#include "common/compile_check_end.h"

} // namespace doris::io
