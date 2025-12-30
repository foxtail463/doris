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

#include "io/fs/hdfs/hdfs_mgr.h"

#include <bthread/bthread.h>
#include <bthread/butex.h>

#include <chrono>
#include <thread>

#include "common/kerberos/kerberos_ticket_mgr.h"
#include "common/logging.h"
#include "io/fs/err_utils.h"
#include "io/hdfs_builder.h"
#include "io/hdfs_util.h"
#include "runtime/exec_env.h"
#include "vec/common/string_ref.h"

namespace doris::io {

HdfsMgr::HdfsMgr() : _should_stop_cleanup_thread(false) {
    _start_cleanup_thread();
}

HdfsMgr::~HdfsMgr() {
    _stop_cleanup_thread();
}

void HdfsMgr::_start_cleanup_thread() {
    _cleanup_thread = std::make_unique<std::thread>(&HdfsMgr::_cleanup_loop, this);
}

void HdfsMgr::_stop_cleanup_thread() {
    if (_cleanup_thread) {
        _should_stop_cleanup_thread = true;
        _cleanup_thread->join();
        _cleanup_thread.reset();
    }
}

void HdfsMgr::_cleanup_loop() {
#ifdef BE_TEST
    static constexpr int64_t CHECK_INTERVAL_SECONDS = 1; // For testing purpose
#else
    static constexpr int64_t CHECK_INTERVAL_SECONDS = 5; // Check stop flag every 5 seconds
#endif
    uint64_t last_cleanup_time = std::time(nullptr);

    while (!_should_stop_cleanup_thread) {
        uint64_t current_time = std::time(nullptr);

        // Only perform cleanup if enough time has passed
        if (current_time - last_cleanup_time >= _cleanup_interval_seconds) {
            // Collect expired handlers under lock
            std::vector<std::shared_ptr<HdfsHandler>> handlers_to_cleanup;
            {
                std::lock_guard<std::mutex> lock(_mutex);
                std::vector<uint64_t> to_remove;

                // Find expired handlers
                for (const auto& entry : _fs_handlers) {
                    bool is_expired = current_time - entry.second->last_access_time >=
                                      _instance_timeout_seconds;
                    // bool is_krb_expired =
                    //         entry.second->is_kerberos_auth &&
                    //         (current_time - entry.second->create_time >=
                    //          entry.second->ticket_cache->get_ticket_lifetime_sec() / 2);
                    if (is_expired) {
                        LOG(INFO) << "Found expired HDFS handler, hash_code=" << entry.first
                                  << ", last_access_time=" << entry.second->last_access_time
                                  << ", is_kerberos=" << entry.second->is_kerberos_auth
                                  << ", principal=" << entry.second->principal
                                  << ", fs_name=" << entry.second->fs_name
                                  << ", is_expired=" << is_expired;
                        // << ", is_krb_expire=" << is_krb_expired;
                        to_remove.push_back(entry.first);
                        handlers_to_cleanup.push_back(entry.second);
                    }
                }

                // Remove expired handlers from map under lock
                for (uint64_t hash_code : to_remove) {
                    _fs_handlers.erase(hash_code);
                }
            }

            // Cleanup handlers outside lock
            for (const auto& handler : handlers_to_cleanup) {
                LOG(INFO) << "Start to cleanup HDFS handler"
                          << ", is_kerberos=" << handler->is_kerberos_auth
                          << ", principal=" << handler->principal
                          << ", fs_name=" << handler->fs_name;

                // The kerberos ticket cache will be automatically cleaned up when the last reference is gone
                // DO NOT call hdfsDisconnect(), or we will meet "Filesystem closed"
                // even if we create a new one
                // hdfsDisconnect(handler->hdfs_fs);

                LOG(INFO) << "Finished cleanup HDFS handler"
                          << ", fs_name=" << handler->fs_name;
            }

            handlers_to_cleanup.clear();
            last_cleanup_time = current_time;
        }

        // Sleep for a short interval to check stop flag more frequently
        std::this_thread::sleep_for(std::chrono::seconds(CHECK_INTERVAL_SECONDS));
    }
}

// 根据 hdfs 参数与 fs_name 计算唯一键，优先从本地缓存复用 hdfsFS 句柄，缺失则创建并放入缓存。
// 采用“先查再创 + 双重检查”策略：
// 1) 持锁查询缓存，命中则直接返回；
// 2) 未命中则在锁外创建新 handler（避免长时间持锁）；
// 3) 创建完成后再次加锁检查，若期间有其他线程已插入，则复用其结果，否则插入新建的 handler。
Status HdfsMgr::get_or_create_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                                 std::shared_ptr<HdfsHandler>* fs_handler) {
    uint64_t hash_code = _hdfs_hash_code(hdfs_params, fs_name);

    // 第一次在持锁条件下检查缓存，若命中则复用并返回
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _fs_handlers.find(hash_code);
        if (it != _fs_handlers.end()) {
            LOG(INFO) << "Reuse existing HDFS handler, hash_code=" << hash_code
                      << ", is_kerberos=" << it->second->is_kerberos_auth
                      << ", principal=" << it->second->principal << ", fs_name=" << fs_name;
            it->second->update_access_time();
            *fs_handler = it->second;
            return Status::OK();
        }
    }

    // 未命中：在锁外创建新的 hdfsFS 句柄，避免长时间占用互斥锁
    LOG(INFO) << "Start to create new HDFS handler, hash_code=" << hash_code
              << ", fs_name=" << fs_name;

    std::shared_ptr<HdfsHandler> new_fs_handler;
    RETURN_IF_ERROR(_create_hdfs_fs(hdfs_params, fs_name, &new_fs_handler));

    // 插入前的第二次检查（双重检查），防止并发场景下重复创建
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _fs_handlers.find(hash_code);
        if (it != _fs_handlers.end()) {
            // 期间已有其他线程创建并写入缓存：复用之
            LOG(INFO) << "Another thread created HDFS handler, reuse it, hash_code=" << hash_code
                      << ", is_kerberos=" << it->second->is_kerberos_auth
                      << ", principal=" << it->second->principal << ", fs_name=" << fs_name;
            it->second->update_access_time();
            *fs_handler = it->second;
            return Status::OK();
        }

        // 缓存未命中：存入新创建的 handler
        *fs_handler = new_fs_handler;
        _fs_handlers[hash_code] = new_fs_handler;

        LOG(INFO) << "Finished create new HDFS handler, hash_code=" << hash_code
                  << ", is_kerberos=" << new_fs_handler->is_kerberos_auth
                  << ", principal=" << new_fs_handler->principal << ", fs_name=" << fs_name;
    }

    return Status::OK();
}

Status HdfsMgr::_create_hdfs_fs_impl(const THdfsParams& hdfs_params, const std::string& fs_name,
                                     std::shared_ptr<HdfsHandler>* fs_handler) {
    HDFSCommonBuilder builder;
    RETURN_IF_ERROR(create_hdfs_builder(hdfs_params, fs_name, &builder));
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        return Status::InternalError("failed to connect to hdfs {}: {}", fs_name, hdfs_error());
    }

    bool is_kerberos = builder.is_kerberos();
    *fs_handler = std::make_shared<HdfsHandler>(
            hdfs_fs, is_kerberos, is_kerberos ? hdfs_params.hdfs_kerberos_principal : "",
            is_kerberos ? hdfs_params.hdfs_kerberos_keytab : "", fs_name);
    // builder.get_ticket_cache());
    return Status::OK();
}

// 参考 brpc 文档：https://brpc.apache.org/docs/server/basics/
// 背景：Hadoop 的 JNI 代码会检查线程栈布局，不能在 bthread（协程线程）中运行。
// 因此：如果当前线程是 bthread，则临时切换到一个原生 pthread 中执行
// _create_hdfs_fs_impl（真正的 hdfs 连接创建），创建完成后再唤醒 bthread 继续。
Status HdfsMgr::_create_hdfs_fs(const THdfsParams& hdfs_params, const std::string& fs_name,
                                std::shared_ptr<HdfsHandler>* fs_handler) {
    bool is_pthread = bthread_self() == 0;
    LOG(INFO) << "create hdfs fs, is_pthread=" << is_pthread << " fs_name=" << fs_name;
    if (is_pthread) { // 已经运行在原生 pthread 中，直接创建
        return _create_hdfs_fs_impl(hdfs_params, fs_name, fs_handler);
    }

    // 当前运行在 bthread 中：切换到 pthread 并等待其完成
    Status st;
    // butex: brpc 的用户态同步原语，用于在 bthread 与 pthread 间同步
    auto btx = bthread::butex_create();
    *(int*)btx = 0;
    std::thread t([&] {
        // 在 pthread 中执行真正的 hdfs 连接创建
        st = _create_hdfs_fs_impl(hdfs_params, fs_name, fs_handler);
        // 创建完成后置位并唤醒等待方
        *(int*)btx = 1;
        bthread::butex_wake_all(btx);
    });
    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&t, &btx](...) {
        if (t.joinable()) t.join();
        bthread::butex_destroy(btx);
    });
    // 等待最长 60 秒（butex 超时返回后做错误处理）
    timespec tmout {.tv_sec = std::chrono::system_clock::now().time_since_epoch().count() + 60,
                    .tv_nsec = 0};
    if (int ret = bthread::butex_wait(btx, 1, &tmout); ret != 0) {
        std::string msg = "failed to wait create_hdfs_fs finish. fs_name=" + fs_name;
        LOG(WARNING) << msg << " error=" << std::strerror(errno);
        st = Status::Error<ErrorCode::INTERNAL_ERROR, false>(msg);
    }
    return st;
}

uint64_t HdfsMgr::_hdfs_hash_code(const THdfsParams& hdfs_params, const std::string& fs_name) {
    uint64_t hash_code = 0;
    // The specified fsname is used first.
    // If there is no specified fsname, the default fsname is used
    if (!fs_name.empty()) {
        hash_code ^= crc32_hash(fs_name);
    } else if (hdfs_params.__isset.fs_name) {
        hash_code ^= crc32_hash(hdfs_params.fs_name);
    }

    if (hdfs_params.__isset.user) {
        hash_code ^= crc32_hash(hdfs_params.user);
    }
    if (hdfs_params.__isset.hdfs_kerberos_principal) {
        hash_code ^= crc32_hash(hdfs_params.hdfs_kerberos_principal);
    }
    if (hdfs_params.__isset.hdfs_kerberos_keytab) {
        hash_code ^= crc32_hash(hdfs_params.hdfs_kerberos_keytab);
    }
    if (hdfs_params.__isset.hdfs_conf) {
        std::map<std::string, std::string> conf_map;
        for (const auto& conf : hdfs_params.hdfs_conf) {
            conf_map[conf.key] = conf.value;
        }
        for (auto& conf : conf_map) {
            hash_code ^= crc32_hash(conf.first);
            hash_code ^= crc32_hash(conf.second);
        }
    }
    return hash_code;
}

} // namespace doris::io
