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
//
// This file is copied from
// https://github.com/apache/impala/blob/master/be/src/runtime/io/handle-cache.h
// and modified by Doris

#pragma once

#include <array>
#include <list>
#include <map>
#include <memory>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs.h"
#include "util/aligned_new.h"
#include "util/lru_multi_cache.inline.h"
#include "util/thread.h"

namespace doris::io {

/// This abstract class is a small wrapper around the hdfsFile handle and the file system
/// instance which is needed to close the file handle. The handle incorporates
/// the last modified time of the file when it was opened. This is used to distinguish
/// between file handles for files that can be updated or overwritten.
/// This is used only through its subclasses, CachedHdfsFileHandle and
/// ExclusiveHdfsFileHandle.
class HdfsFileHandle {
public:
    /// Destructor will close the file handle
    ~HdfsFileHandle();

    /// Init opens the file handle
    Status init(int64_t file_size);

    hdfsFS fs() const { return _fs; }
    hdfsFile file() const { return _hdfs_file; }
    int64_t mtime() const { return _mtime; }
    int64_t file_size() const { return _file_size; }

protected:
    HdfsFileHandle(const hdfsFS& fs, const std::string& fname, int64_t mtime)
            : _fs(fs), _fname(fname), _mtime(mtime) {}

private:
    hdfsFS _fs;
    const std::string _fname;
    hdfsFile _hdfs_file = nullptr;
    int64_t _mtime;
    int64_t _file_size;
};

/// CachedHdfsFileHandles are owned by the file handle cache and are used for no
/// other purpose.
class CachedHdfsFileHandle : public HdfsFileHandle {
public:
    CachedHdfsFileHandle(const hdfsFS& fs, const std::string& fname, int64_t mtime);
    ~CachedHdfsFileHandle();
};

/// ExclusiveHdfsFileHandles are used for all purposes where a CachedHdfsFileHandle
/// is not appropriate.
class ExclusiveHdfsFileHandle : public HdfsFileHandle {
public:
    ExclusiveHdfsFileHandle(const hdfsFS& fs, const std::string& fname, int64_t mtime)
            : HdfsFileHandle(fs, fname, mtime) {}
};

/// FileHandleCache 是一个数据结构，用于在线程间共享 HdfsFileHandle 的所有权。
/// HdfsFileHandles 通过哈希分区分布在 NUM_PARTITIONS 个分区中。
/// 每个分区独立运行，拥有自己的锁，减少了并发线程之间的竞争。
/// `capacity` 在分区之间分配，并独立执行。
///
/// 线程通过 RAII 访问器签出文件句柄以获得独占访问权限，访问器会自动释放。
/// 如果文件句柄在缓存中不存在，或者该文件的所有文件句柄都已被签出，
/// 则会在缓存中构造文件句柄。缓存可以包含同一文件的多个文件句柄。
/// 如果文件句柄被签出，它就不能从缓存中驱逐。在这种情况下，缓存可能会超过指定的容量。
///
/// 远程文件系统可能会将连接作为文件句柄的一部分，而不支持解缓冲。
/// 文件句柄缓存不适用于这些系统，因为缓存大小可能会超过并发连接数的限制。
/// HDFS 不在文件句柄中维护连接，S3A 客户端自 IMPALA-8428 以来支持解缓冲，
/// 所以这些系统没有这个限制。
///
/// 如果缓存中有文件句柄，而底层文件被删除，文件句柄可能会阻止文件在操作系统级别被删除。
/// 这可能会占用磁盘空间并影响正确性。为了避免这种情况，缓存将驱逐任何
/// 未使用时间超过 `unused_handle_timeout_secs` 指定阈值的文件句柄。
/// 当阈值为 0 时，驱逐被禁用。
///
/// TODO: 如果文件句柄的 mtime 比文件的当前 mtime 更旧，
/// 缓存还应该更积极地驱逐文件句柄。
class FileHandleCache {
private:
    /// Each partition operates independently, and thus has its own thread-safe cache.
    /// To avoid contention on the lock_ due to false sharing the partitions are
    /// aligned to cache line boundaries.
    struct FileHandleCachePartition : public CacheLineAligned {
        // Cache key is a pair of filename and mtime
        // Using std::pair to spare boilerplate of hash function
        typedef LruMultiCache<std::pair<std::string, int64_t>, CachedHdfsFileHandle> CacheType;
        CacheType cache;
    };

public:
    /// RAII accessor built over LruMultiCache::Accessor to handle metrics and unbuffering.
    /// Composition is used instead of inheritance to support the usage as in/out parameter
    class Accessor {
    public:
        Accessor();
        Accessor(FileHandleCachePartition::CacheType::Accessor&& cache_accessor);
        Accessor(Accessor&&) = default;
        Accessor& operator=(Accessor&&) = default;

        DISALLOW_COPY_AND_ASSIGN(Accessor);

        /// Handles metrics and unbuffering
        ~Accessor();

        /// Set function can be used if the Accessor is used as in/out parameter.
        void set(FileHandleCachePartition::CacheType::Accessor&& cache_accessor);

        /// Interface mimics LruMultiCache::Accessor's interface, handles metrics
        CachedHdfsFileHandle* get();
        void release();
        void destroy();

    private:
        FileHandleCachePartition::CacheType::Accessor _cache_accessor;
    };

    /// Instantiates the cache with `capacity` split evenly across NUM_PARTITIONS
    /// partitions. If the capacity does not split evenly, then the capacity is rounded
    /// up. The cache will age out any file handle that is unused for
    /// `unused_handle_timeout_secs` seconds. Age out is disabled if this is set to zero.
    FileHandleCache(size_t capacity, size_t num_partitions, uint64_t unused_handle_timeout_secs);

    /// Destructor is only called for backend tests
    ~FileHandleCache();

    /// Starts up a thread that monitors the age of file handles and evicts any that
    /// exceed the limit.
    Status init() WARN_UNUSED_RESULT;

    /// Get a file handle accessor from the cache for the specified filename (fname) and
    /// last modification time (mtime). This will hash the filename to determine
    /// which partition to use for this file handle.
    ///
    /// If 'require_new_handle' is false and the partition contains an available handle,
    /// an accessor is returned and cache_hit is set to true. Otherwise, the partition will
    /// emplace file handle, an accessor to it will be returned with cache_hit set to false.
    /// On failure, empty accessor will be returned. In either case, the partition may evict
    /// a file handle to make room for the new file handle.
    ///
    /// This obtains exclusive control over the returned file handle.
    Status get_file_handle(const hdfsFS& fs, const std::string& fname, int64_t mtime,
                           int64_t file_size, bool require_new_handle, Accessor* accessor,
                           bool* cache_hit) WARN_UNUSED_RESULT;

private:
    /// Periodic check to evict unused file handles. Only executed by _eviction_thread.
    void _evict_handles_loop();

    std::vector<FileHandleCachePartition> _cache_partitions;

    /// Maximum time before an unused file handle is aged out of the cache.
    /// Aging out is disabled if this is set to 0.
    uint64_t _unused_handle_timeout_secs;

    /// Thread to check for unused file handles to evict. This thread will exit when
    /// the _shut_down_promise is set.
    std::shared_ptr<Thread> _eviction_thread;
    std::atomic<bool> _is_shut_down = {false};
};

} // namespace doris::io
