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

#include "agent/agent_server.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <stdint.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <filesystem>
#include <memory>
#include <ostream>
#include <string>

#include "agent/task_worker_pool.h"
#include "agent/topic_subscriber.h"
#include "agent/utils.h"
#include "agent/workload_group_listener.h"
#include "agent/workload_sched_policy_listener.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/work_thread_pool.hpp"

namespace doris {

AgentServer::AgentServer(ExecEnv* exec_env, const ClusterInfo* cluster_info)
        : _cluster_info(cluster_info), _topic_subscriber(new TopicSubscriber()) {
    MasterServerClient::create(cluster_info);

#if !defined(BE_TEST) && !defined(__APPLE__)
    // Add subscriber here and register listeners
    std::unique_ptr<TopicListener> wg_listener = std::make_unique<WorkloadGroupListener>(exec_env);
    LOG(INFO) << "Register workload group listener";
    _topic_subscriber->register_listener(doris::TTopicInfoType::type::WORKLOAD_GROUP,
                                         std::move(wg_listener));

    std::unique_ptr<TopicListener> policy_listener =
            std::make_unique<WorkloadschedPolicyListener>(exec_env);
    LOG(INFO) << "Register workload scheduler policy listener";
    _topic_subscriber->register_listener(doris::TTopicInfoType::type::WORKLOAD_SCHED_POLICY,
                                         std::move(policy_listener));

#endif
}

AgentServer::~AgentServer() = default;

class PushTaskWorkerPool final : public TaskWorkerPoolIf {
public:
    PushTaskWorkerPool(StorageEngine& engine)
            : _push_delete_workers(
                      TaskWorkerPool("DELETE", config::delete_worker_count,
                                     [&engine](auto&& task) { push_callback(engine, task); })),
              _push_load_workers(PriorTaskWorkerPool(
                      "PUSH", config::push_worker_count_normal_priority,
                      config::push_worker_count_high_priority,
                      [&engine](auto&& task) { push_callback(engine, task); })) {}

    ~PushTaskWorkerPool() override { stop(); }

    void stop() {
        _push_delete_workers.stop();
        _push_load_workers.stop();
    }

    Status submit_task(const TAgentTaskRequest& task) override {
        if (task.push_req.push_type == TPushType::LOAD_V2) {
            return _push_load_workers.submit_task(task);
        } else if (task.push_req.push_type == TPushType::DELETE) {
            return _push_delete_workers.submit_task(task);
        } else {
            return Status::InvalidArgument(
                    "task(signature={}, type={}, push_type={}) has wrong push_type", task.signature,
                    task.task_type, task.push_req.push_type);
        }
    }

private:
    TaskWorkerPool _push_delete_workers;
    PriorTaskWorkerPool _push_load_workers;
};

void AgentServer::start_workers(StorageEngine& engine, ExecEnv* exec_env) {
    for (const auto& path : exec_env->store_paths()) {
        try {
            std::string dpp_download_path_str = path.path + "/" + DPP_PREFIX;
            std::filesystem::path dpp_download_path(dpp_download_path_str);
            if (std::filesystem::exists(dpp_download_path)) {
                std::filesystem::remove_all(dpp_download_path);
            }
        } catch (...) {
            LOG(WARNING) << "boost exception when remove dpp download path. path=" << path.path;
        }
    }

    // clang-format off
    _workers[TTaskType::ALTER_INVERTED_INDEX] = std::make_unique<TaskWorkerPool>(
        "ALTER_INVERTED_INDEX", config::alter_index_worker_count, [&engine](auto&& task) { return alter_inverted_index_callback(engine, task); });

    _workers[TTaskType::CHECK_CONSISTENCY] = std::make_unique<TaskWorkerPool>(
        "CHECK_CONSISTENCY", config::check_consistency_worker_count, [&engine](auto&& task) { return check_consistency_callback(engine, task); });

    _workers[TTaskType::UPLOAD] = std::make_unique<TaskWorkerPool>(
            "UPLOAD", config::upload_worker_count, [&engine, exec_env](auto&& task) { return upload_callback(engine, exec_env, task); });

    _workers[TTaskType::DOWNLOAD] = std::make_unique<TaskWorkerPool>(
            "DOWNLOAD", config::download_worker_count, [&engine, exec_env](auto&& task) { return download_callback(engine, exec_env, task); });

    _workers[TTaskType::MAKE_SNAPSHOT] = std::make_unique<TaskWorkerPool>(
            "MAKE_SNAPSHOT", config::make_snapshot_worker_count, [&engine](auto&& task) { return make_snapshot_callback(engine, task); });

    _workers[TTaskType::RELEASE_SNAPSHOT] = std::make_unique<TaskWorkerPool>(
            "RELEASE_SNAPSHOT", config::release_snapshot_worker_count, [&engine](auto&& task) { return release_snapshot_callback(engine, task); });

    _workers[TTaskType::MOVE] = std::make_unique<TaskWorkerPool>(
            "MOVE", 1, [&engine, exec_env](auto&& task) { return move_dir_callback(engine, exec_env, task); });

    _workers[TTaskType::COMPACTION] = std::make_unique<TaskWorkerPool>(
            "SUBMIT_TABLE_COMPACTION", 1, [&engine](auto&& task) { return submit_table_compaction_callback(engine, task); });

    _workers[TTaskType::PUSH_STORAGE_POLICY] = std::make_unique<TaskWorkerPool>(
            "PUSH_STORAGE_POLICY", 1, [&engine](auto&& task) { return push_storage_policy_callback(engine, task); });

    _workers[TTaskType::PUSH_INDEX_POLICY] = std::make_unique<TaskWorkerPool>(
            "PUSH_INDEX_POLICY", 1, [](auto&& task) { return push_index_policy_callback(task); });

    _workers[TTaskType::PUSH_COOLDOWN_CONF] = std::make_unique<TaskWorkerPool>(
            "PUSH_COOLDOWN_CONF", 1, [&engine](auto&& task) { return push_cooldown_conf_callback(engine, task); });

    _workers[TTaskType::CREATE] = std::make_unique<TaskWorkerPool>(
            "CREATE_TABLE", config::create_tablet_worker_count, [&engine](auto&& task) { return create_tablet_callback(engine, task); });

    _workers[TTaskType::DROP] = std::make_unique<TaskWorkerPool>(
            "DROP_TABLE", config::drop_tablet_worker_count, [&engine](auto&& task) { return drop_tablet_callback(engine, task); });

    _workers[TTaskType::PUBLISH_VERSION] = std::make_unique<PublishVersionWorkerPool>(engine);

    _workers[TTaskType::CLEAR_TRANSACTION_TASK] = std::make_unique<TaskWorkerPool>(
            "CLEAR_TRANSACTION_TASK", config::clear_transaction_task_worker_count, [&engine](auto&& task) { return clear_transaction_task_callback(engine, task); });

    _workers[TTaskType::PUSH] = std::make_unique<PushTaskWorkerPool>(engine);

    _workers[TTaskType::UPDATE_TABLET_META_INFO] = std::make_unique<TaskWorkerPool>(
            "UPDATE_TABLET_META_INFO", 1, [&engine](auto&& task) { return update_tablet_meta_callback(engine, task); });

    _workers[TTaskType::ALTER] = std::make_unique<TaskWorkerPool>(
            "ALTER_TABLE", config::alter_tablet_worker_count, [&engine](auto&& task) { return alter_tablet_callback(engine, task); });

    _workers[TTaskType::CLONE] = std::make_unique<PriorTaskWorkerPool>(
            "CLONE", config::clone_worker_count,config::clone_worker_count, [&engine, &cluster_info = _cluster_info](auto&& task) { return clone_callback(engine, cluster_info, task); });

    _workers[TTaskType::STORAGE_MEDIUM_MIGRATE] = std::make_unique<TaskWorkerPool>(
            "STORAGE_MEDIUM_MIGRATE", config::storage_medium_migrate_count, [&engine](auto&& task) { return storage_medium_migrate_callback(engine, task); });

    _workers[TTaskType::GC_BINLOG] = std::make_unique<TaskWorkerPool>(
            "GC_BINLOG", 1, [&engine](auto&& task) { return gc_binlog_callback(engine, task); });

    _workers[TTaskType::CLEAN_TRASH] = std::make_unique<TaskWorkerPool>(
            "CLEAN_TRASH", 1, [&engine](auto&& task) {return clean_trash_callback(engine, task); });

    _workers[TTaskType::CLEAN_UDF_CACHE] = std::make_unique<TaskWorkerPool>(
            "CLEAN_UDF_CACHE", 1, [](auto&& task) {return clean_udf_cache_callback(task); });

    _workers[TTaskType::UPDATE_VISIBLE_VERSION] = std::make_unique<TaskWorkerPool>(
            "UPDATE_VISIBLE_VERSION", 1, [&engine](auto&& task) { return visible_version_callback(engine, task); });

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_TASK", _cluster_info, config::report_task_interval_seconds, [&cluster_info = _cluster_info] { report_task_callback(cluster_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_DISK_STATE", _cluster_info, config::report_disk_state_interval_seconds, [&engine, &cluster_info = _cluster_info] { report_disk_callback(engine, cluster_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_OLAP_TABLET", _cluster_info, config::report_tablet_interval_seconds,[&engine, &cluster_info = _cluster_info] { report_tablet_callback(engine, cluster_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_INDEX_POLICY", _cluster_info, config::report_index_policy_interval_seconds,[&cluster_info = _cluster_info] { report_index_policy_callback(cluster_info); }));
    // clang-format on

    exec_env->storage_engine().to_local().workers = &_workers;
}

void AgentServer::cloud_start_workers(CloudStorageEngine& engine, ExecEnv* exec_env) {
    _workers[TTaskType::PUSH] = std::make_unique<TaskWorkerPool>(
            "PUSH", config::delete_worker_count,
            [&engine](auto&& task) { cloud_push_callback(engine, task); });
    // TODO(plat1ko): SUBMIT_TABLE_COMPACTION worker

    _workers[TTaskType::ALTER] = std::make_unique<TaskWorkerPool>(
            "ALTER_TABLE", config::alter_tablet_worker_count,
            [&engine](auto&& task) { return alter_cloud_tablet_callback(engine, task); });

    _workers[TTaskType::CALCULATE_DELETE_BITMAP] = std::make_unique<TaskWorkerPool>(
            "CALC_DBM_TASK", config::calc_delete_bitmap_worker_count,
            [&engine](auto&& task) { return calc_delete_bitmap_callback(engine, task); });

    // cloud, drop tablet just clean clear_cache, so just one thread do it
    _workers[TTaskType::DROP] = std::make_unique<TaskWorkerPool>(
            "DROP_TABLE", 1, [&engine](auto&& task) { return drop_tablet_callback(engine, task); });

    _workers[TTaskType::PUSH_INDEX_POLICY] = std::make_unique<TaskWorkerPool>(
            "PUSH_INDEX_POLICY", 1, [](auto&& task) { return push_index_policy_callback(task); });

    _workers[TTaskType::DOWNLOAD] = std::make_unique<TaskWorkerPool>(
            "DOWNLOAD", config::download_worker_count,
            [&engine, exec_env](auto&& task) { return download_callback(engine, exec_env, task); });

    _workers[TTaskType::MOVE] = std::make_unique<TaskWorkerPool>(
            "MOVE", 1,
            [&engine, exec_env](auto&& task) { return move_dir_callback(engine, exec_env, task); });

    _workers[TTaskType::RELEASE_SNAPSHOT] = std::make_unique<TaskWorkerPool>(
            "RELEASE_SNAPSHOT", config::release_snapshot_worker_count,
            [&engine](auto&& task) { return release_snapshot_callback(engine, task); });

    _workers[TTaskType::ALTER_INVERTED_INDEX] = std::make_unique<TaskWorkerPool>(
            "ALTER_INVERTED_INDEX", config::alter_index_worker_count,
            [&engine](auto&& task) { return alter_cloud_index_callback(engine, task); });

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_TASK", _cluster_info, config::report_task_interval_seconds,
            [&cluster_info = _cluster_info] { report_task_callback(cluster_info); }));

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_DISK_STATE", _cluster_info, config::report_disk_state_interval_seconds,
            [&engine, &cluster_info = _cluster_info] {
                report_disk_callback(engine, cluster_info);
            }));

    if (config::enable_cloud_tablet_report) {
        _report_workers.push_back(std::make_unique<ReportWorker>(
                "REPORT_OLAP_TABLET", _cluster_info, config::report_tablet_interval_seconds,
                [&engine, &cluster_info = _cluster_info] {
                    report_tablet_callback(engine, cluster_info);
                }));
    }

    _report_workers.push_back(std::make_unique<ReportWorker>(
            "REPORT_INDEX_POLICY", _cluster_info, config::report_index_policy_interval_seconds,
            [&cluster_info = _cluster_info] { report_index_policy_callback(cluster_info); }));
}

// TODO(lingbin): 批处理中的每个任务可能有自己的状态，或者FE必须检查
// 当出现问题时重新发送请求（BE可能需要一些逻辑来保证幂等性）
void AgentServer::submit_tasks(TAgentResult& agent_result,
                               const std::vector<TAgentTaskRequest>& tasks) {
    Status ret_st;  // 返回状态

    // TODO: 在这里检查cluster_info是否与心跳RPC中的相同
    // 检查是否已经收到FE Master的心跳信息
    if (_cluster_info->master_fe_addr.hostname.empty() || _cluster_info->master_fe_addr.port == 0) {
        Status st = Status::Cancelled("Have not get FE Master heartbeat yet");
        st.to_thrift(&agent_result.status);
        return;
    }

    // 遍历所有任务，逐个提交
    for (auto&& task : tasks) {
        // 记录RPC日志，打印任务详情
        VLOG_RPC << "submit one task: " << apache::thrift::ThriftDebugString(task).c_str();
        
        auto task_type = task.task_type;
        // 特殊处理：将REALTIME_PUSH类型转换为PUSH类型
        if (task_type == TTaskType::REALTIME_PUSH) {
            task_type = TTaskType::PUSH;
        }
        
        int64_t signature = task.signature;  // 获取任务签名
        
        // 根据任务类型查找对应的工作池
        if (auto it = _workers.find(task_type); it != _workers.end()) {
            auto& worker = it->second;  // 获取工作池引用
            ret_st = worker->submit_task(task);  // 提交任务到工作池
        } else {
            // 如果找不到对应的工作池，返回错误
            ret_st = Status::InvalidArgument("task(signature={}, type={}) has wrong task type",
                                             signature, task.task_type);
        }

        // 处理任务提交失败的情况
        if (!ret_st.ok()) {
            LOG_WARNING("failed to submit task").tag("task", task).error(ret_st);
            
            // 当前设计说明：
            // 1. 批处理中的所有任务共享一个状态，所以如果任何任务提交失败，
            //    我们只能向FE返回错误（即使某些任务已经成功提交）
            // 2. 但是，FE目前不检查submit_tasks()的返回状态，
            //    也不确定FE在出现问题时是否会重试
            // 3. 因此这里只打印警告日志并继续执行（不中断当前循环），
            //    确保每个任务都能被提交一次
            // 4. 目前这样是可以的，因为ret_st只有在遇到错误的task_type和
            //    TAgentTaskRequest中的req-member时才会出错，这基本上是不可能的
            // TODO(lingbin): 稍后再次检查FE中的逻辑
        }
    }

    // 将最终状态转换为Thrift格式并设置到结果中
    ret_st.to_thrift(&agent_result.status);
}

void AgentServer::publish_cluster_state(TAgentResult& t_agent_result,
                                        const TAgentPublishRequest& request) {
    Status status = Status::NotSupported("deprecated method(publish_cluster_state) was invoked");
    status.to_thrift(&t_agent_result.status);
}

void AgentServer::stop_report_workers() {
    for (auto& work : _report_workers) {
        work->stop();
    }
}

} // namespace doris
