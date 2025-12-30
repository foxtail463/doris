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

#include "pipeline_fragment_context.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <pthread.h>

#include <algorithm>
#include <cstdlib>
// IWYU pragma: no_include <bits/chrono.h>
#include <fmt/format.h>

#include <chrono> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>
#include <utility>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/stream_load_pipe.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/analytic_sink_operator.h"
#include "pipeline/exec/analytic_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/blackhole_sink_operator.h"
#include "pipeline/exec/cache_sink_operator.h"
#include "pipeline/exec/cache_source_operator.h"
#include "pipeline/exec/datagen_operator.h"
#include "pipeline/exec/dict_sink_operator.h"
#include "pipeline/exec/distinct_streaming_aggregation_operator.h"
#include "pipeline/exec/empty_set_operator.h"
#include "pipeline/exec/es_scan_operator.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/file_scan_operator.h"
#include "pipeline/exec/group_commit_block_sink_operator.h"
#include "pipeline/exec/group_commit_scan_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/hive_table_sink_operator.h"
#include "pipeline/exec/iceberg_table_sink_operator.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "pipeline/exec/jdbc_table_sink_operator.h"
#include "pipeline/exec/local_merge_sort_source_operator.h"
#include "pipeline/exec/materialization_opertor.h"
#include "pipeline/exec/memory_scratch_sink_operator.h"
#include "pipeline/exec/meta_scan_operator.h"
#include "pipeline/exec/multi_cast_data_stream_sink.h"
#include "pipeline/exec/multi_cast_data_stream_source.h"
#include "pipeline/exec/nested_loop_join_build_operator.h"
#include "pipeline/exec/nested_loop_join_probe_operator.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "pipeline/exec/olap_table_sink_operator.h"
#include "pipeline/exec/olap_table_sink_v2_operator.h"
#include "pipeline/exec/partition_sort_sink_operator.h"
#include "pipeline/exec/partition_sort_source_operator.h"
#include "pipeline/exec/partitioned_aggregation_sink_operator.h"
#include "pipeline/exec/partitioned_aggregation_source_operator.h"
#include "pipeline/exec/partitioned_hash_join_probe_operator.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/exec/repeat_operator.h"
#include "pipeline/exec/result_file_sink_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline/exec/schema_scan_operator.h"
#include "pipeline/exec/select_operator.h"
#include "pipeline/exec/set_probe_sink_operator.h"
#include "pipeline/exec/set_sink_operator.h"
#include "pipeline/exec/set_source_operator.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "pipeline/exec/sort_source_operator.h"
#include "pipeline/exec/spill_sort_sink_operator.h"
#include "pipeline/exec/spill_sort_source_operator.h"
#include "pipeline/exec/streaming_aggregation_operator.h"
#include "pipeline/exec/table_function_operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/exec/union_source_operator.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "pipeline/local_exchange/local_exchange_source_operator.h"
#include "pipeline/local_exchange/local_exchanger.h"
#include "pipeline/task_scheduler.h"
#include "pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_block_buffer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/thread_context.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "service/backend_options.h"
#include "util/countdown_latch.h"
#include "util/debug_util.h"
#include "util/uid_util.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/spill/spill_stream.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
PipelineFragmentContext::PipelineFragmentContext(
        TUniqueId query_id, const TPipelineFragmentParams& request,
        std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
        const std::function<void(RuntimeState*, Status*)>& call_back,
        report_status_callback report_status_cb)
        : _query_id(std::move(query_id)),
          _fragment_id(request.fragment_id),
          _exec_env(exec_env),
          _query_ctx(std::move(query_ctx)),
          _call_back(call_back),
          _is_report_on_cancel(true),
          _report_status_cb(std::move(report_status_cb)),
          _params(request),
          _parallel_instances(_params.__isset.parallel_instances ? _params.parallel_instances : 0) {
    _fragment_watcher.start();
}

PipelineFragmentContext::~PipelineFragmentContext() {
    LOG_INFO("PipelineFragmentContext::~PipelineFragmentContext")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id);
    // The memory released by the query end is recorded in the query mem tracker.
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_ctx->query_mem_tracker());
    auto st = _query_ctx->exec_status();
    for (size_t i = 0; i < _tasks.size(); i++) {
        if (!_tasks[i].empty()) {
            _call_back(_tasks[i].front().first->runtime_state(), &st);
        }
    }
    _tasks.clear();
    _dag.clear();
    _pip_id_to_pipeline.clear();
    _pipelines.clear();
    _sink.reset();
    _root_op.reset();
    _runtime_state.reset();
    _runtime_filter_mgr_map.clear();
    _op_id_to_shared_state.clear();
    _query_ctx.reset();
}

bool PipelineFragmentContext::is_timeout(timespec now) const {
    if (_timeout <= 0) {
        return false;
    }
    return _fragment_watcher.elapsed_time_seconds(now) > _timeout;
}

// Must not add lock in this method. Because it will call query ctx cancel. And
// QueryCtx cancel will call fragment ctx cancel. And Also Fragment ctx's running
// Method like exchange sink buffer will call query ctx cancel. If we add lock here
// There maybe dead lock.
void PipelineFragmentContext::cancel(const Status reason) {
    LOG_INFO("PipelineFragmentContext::cancel")
            .tag("query_id", print_id(_query_id))
            .tag("fragment_id", _fragment_id)
            .tag("reason", reason.to_string());
    {
        std::lock_guard<std::mutex> l(_task_mutex);
        if (_closed_tasks >= _total_tasks) {
            // All tasks in this PipelineXFragmentContext already closed.
            return;
        }
    }
    // Timeout is a special error code, we need print current stack to debug timeout issue.
    if (reason.is<ErrorCode::TIMEOUT>()) {
        auto dbg_str = fmt::format("PipelineFragmentContext is cancelled due to timeout:\n{}",
                                   debug_string());
        LOG_LONG_STRING(WARNING, dbg_str);
    }

    // `ILLEGAL_STATE` means queries this fragment belongs to was not found in FE (maybe finished)
    if (reason.is<ErrorCode::ILLEGAL_STATE>()) {
        LOG_WARNING("PipelineFragmentContext is cancelled due to illegal state : {}",
                    debug_string());
    }

    if (reason.is<ErrorCode::MEM_LIMIT_EXCEEDED>() || reason.is<ErrorCode::MEM_ALLOC_FAILED>()) {
        print_profile("cancel pipeline, reason: " + reason.to_string());
    }

    if (auto error_url = get_load_error_url(); !error_url.empty()) {
        _query_ctx->set_load_error_url(error_url);
    }

    if (auto first_error_msg = get_first_error_msg(); !first_error_msg.empty()) {
        _query_ctx->set_first_error_msg(first_error_msg);
    }

    _query_ctx->cancel(reason, _fragment_id);
    if (reason.is<ErrorCode::LIMIT_REACH>()) {
        _is_report_on_cancel = false;
    } else {
        for (auto& id : _fragment_instance_ids) {
            LOG(WARNING) << "PipelineFragmentContext cancel instance: " << print_id(id);
        }
    }
    // Get pipe from new load stream manager and send cancel to it or the fragment may hang to wait read from pipe
    // For stream load the fragment's query_id == load id, it is set in FE.
    auto stream_load_ctx = _exec_env->new_load_stream_mgr()->get(_query_id);
    if (stream_load_ctx != nullptr) {
        stream_load_ctx->pipe->cancel(reason.to_string());
        // Set error URL here because after pipe is cancelled, stream load execution may return early.
        // We need to set the error URL at this point to ensure error information is properly
        // propagated to the client.
        stream_load_ctx->error_url = get_load_error_url();
        stream_load_ctx->first_error_msg = get_first_error_msg();
    }

    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            task.first->terminate();
        }
    }
}

PipelinePtr PipelineFragmentContext::add_pipeline(PipelinePtr parent, int idx) {
    // 每次新增 pipeline 都需要分配一个唯一的 PipelineId
    PipelineId id = _next_pipeline_id++;
    // 需要根据父 pipeline 的任务数和当前实例数决定新 pipeline 的任务数/并发度
    auto pipeline = std::make_shared<Pipeline>(
            id, parent ? std::min(parent->num_tasks(), _num_instances) : _num_instances,
            parent ? parent->num_tasks() : _num_instances);
    if (idx >= 0) {
        // 指定插入位置时，将 pipeline 插入到对应下标
        _pipelines.insert(_pipelines.begin() + idx, pipeline);
    } else {
        // 未指定位置，默认插入到末尾
        _pipelines.emplace_back(pipeline);
    }
    if (parent) {
        // 建立父子 pipeline 的拓扑关系
        parent->set_children(pipeline);
    }
    return pipeline;
}

Status PipelineFragmentContext::prepare(ThreadPool* thread_pool) {
    // 已经执行过 prepare 的上下文无需重复执行，直接返回错误
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    // 读取查询级别的超时配置，后续调度需要用到
    if (_params.__isset.query_options && _params.query_options.__isset.execution_timeout) {
        _timeout = _params.query_options.execution_timeout;
    }

    // 构建片段级 profile，并挂载准备阶段需要的计时器，便于排查性能
    _fragment_level_profile = std::make_unique<RuntimeProfile>("PipelineContext");
    _prepare_timer = ADD_TIMER(_fragment_level_profile, "PrepareTime");
    SCOPED_TIMER(_prepare_timer);
    _build_pipelines_timer = ADD_TIMER(_fragment_level_profile, "BuildPipelinesTime");
    _init_context_timer = ADD_TIMER(_fragment_level_profile, "InitContextTime");
    _plan_local_exchanger_timer = ADD_TIMER(_fragment_level_profile, "PlanLocalLocalExchangerTime");
    _build_tasks_timer = ADD_TIMER(_fragment_level_profile, "BuildTasksTime");
    _prepare_all_pipelines_timer = ADD_TIMER(_fragment_level_profile, "PrepareAllPipelinesTime");
    {
        // ===== 阶段 1：初始化全局运行时状态 =====
        SCOPED_TIMER(_init_context_timer);
        cast_set(_num_instances, _params.local_params.size());
        _total_instances =
                _params.__isset.total_instances ? _params.total_instances : _num_instances;

        auto* fragment_context = this;

        if (_params.query_options.__isset.is_report_success) {
            fragment_context->set_is_report_success(_params.query_options.is_report_success);
        }

        // 创建 RuntimeState，并将 query 级别的各种属性注入进去
        _runtime_state = RuntimeState::create_unique(
                _params.query_id, _params.fragment_id, _params.query_options,
                _query_ctx->query_globals, _exec_env, _query_ctx.get());
        _runtime_state->set_task_execution_context(shared_from_this());
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_runtime_state->query_mem_tracker());
        if (_params.__isset.backend_id) {
            _runtime_state->set_backend_id(_params.backend_id);
        }
        if (_params.__isset.import_label) {
            _runtime_state->set_import_label(_params.import_label);
        }
        if (_params.__isset.db_name) {
            _runtime_state->set_db_name(_params.db_name);
        }
        if (_params.__isset.load_job_id) {
            _runtime_state->set_load_job_id(_params.load_job_id);
        }

        if (_params.is_simplified_param) {
            _desc_tbl = _query_ctx->desc_tbl;
        } else {
            DCHECK(_params.__isset.desc_tbl);
            RETURN_IF_ERROR(DescriptorTbl::create(_runtime_state->obj_pool(), _params.desc_tbl,
                                                  &_desc_tbl));
        }
        _runtime_state->set_desc_tbl(_desc_tbl);
        _runtime_state->set_num_per_fragment_instances(_params.num_senders);
        _runtime_state->set_load_stream_per_node(_params.load_stream_per_node);
        _runtime_state->set_total_load_streams(_params.total_load_streams);
        _runtime_state->set_num_local_sink(_params.num_local_sink);

        // 初始化 fragment_instance_ids，后续调度/汇报需要
        const auto target_size = _params.local_params.size();
        _fragment_instance_ids.resize(target_size);
        for (size_t i = 0; i < _params.local_params.size(); i++) {
            auto fragment_instance_id = _params.local_params[i].fragment_instance_id;
            _fragment_instance_ids[i] = fragment_instance_id;
        }
    }

    {
        // ===== 阶段 2：构建算子 Pipeline 并创建 Sink =====
        SCOPED_TIMER(_build_pipelines_timer);
        // 2. Build pipelines with operators in this fragment.
        auto root_pipeline = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(_runtime_state->obj_pool(), *_query_ctx->desc_tbl,
                                         &_root_op, root_pipeline));

        // 3. Create sink operator
        if (!_params.fragment.__isset.output_sink) {
            return Status::InternalError("No output sink in this fragment!");
        }
        RETURN_IF_ERROR(_create_data_sink(_runtime_state->obj_pool(), _params.fragment.output_sink,
                                          _params.fragment.output_exprs, _params,
                                          root_pipeline->output_row_desc(), _runtime_state.get(),
                                          *_desc_tbl, root_pipeline->id()));
        RETURN_IF_ERROR(_sink->init(_params.fragment.output_sink));
        RETURN_IF_ERROR(root_pipeline->set_sink(_sink));

        for (PipelinePtr& pipeline : _pipelines) {
            DCHECK(pipeline->sink() != nullptr) << pipeline->operators().size();
            RETURN_IF_ERROR(pipeline->sink()->set_child(pipeline->operators().back()));
        }
    }
    // ===== 阶段 3：根据需要规划本地交换器 =====
    // 仅当开启 local shuffle 时才需要构建 local exchanger
    if (_runtime_state->enable_local_shuffle()) {
        SCOPED_TIMER(_plan_local_exchanger_timer);
        RETURN_IF_ERROR(_plan_local_exchange(_params.num_buckets,
                                             _params.bucket_seq_to_instance_idx,
                                             _params.shuffle_idx_to_instance_idx));
    }

    // ===== 阶段 4：prepare 所有 pipeline，并清理 children 关系 =====
    for (PipelinePtr& pipeline : _pipelines) {
        SCOPED_TIMER(_prepare_all_pipelines_timer);
        pipeline->children().clear();
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    {
        // ===== 阶段 5：创建 pipeline task，并做任务级初始化 =====
        SCOPED_TIMER(_build_tasks_timer);
        // 6. Build pipeline tasks and initialize local state.
        RETURN_IF_ERROR(_build_pipeline_tasks(thread_pool));
    }

    // 初始化下一次状态上报时间，确保心跳/汇报机制可用
    _init_next_report_time();

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(ThreadPool* thread_pool) {
    // ===== 阶段 1：初始化任务相关的数据结构 =====
    // 重置任务计数器，准备开始创建任务
    _total_tasks = 0;
    // target_size 表示该 fragment 需要创建的实例数量（通常对应多个 BE 节点或并发实例）
    const auto target_size = _params.local_params.size();
    // 为每个实例预留任务存储空间：_tasks[i] 存储第 i 个实例的所有 PipelineTask
    _tasks.resize(target_size);
    // 为每个实例预留运行时过滤器管理器存储空间
    _runtime_filter_mgr_map.resize(target_size);
    // 构建 Pipeline ID 到 Pipeline 指针的映射，方便后续快速查找
    for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
        _pip_id_to_pipeline[_pipelines[pip_idx]->id()] = _pipelines[pip_idx].get();
    }
    // 为每个 Pipeline 创建性能监控 Profile，用于记录执行统计信息
    auto pipeline_id_to_profile = _runtime_state->build_pipeline_profile(_pipelines.size());

    // ===== 阶段 2：定义 lambda 函数，用于为每个 fragment instance 创建和准备 tasks =====
    // pre_and_submit 函数负责：
    // 1. 为第 i 个实例创建所有 PipelineTask
    // 2. 建立 Pipeline 之间的依赖关系（DAG）
    // 3. 准备每个 task（初始化本地状态、设置扫描范围等）
    auto pre_and_submit = [&](int i, PipelineFragmentContext* ctx) {
        // 获取第 i 个实例的本地参数（包含 fragment_instance_id、sender_id、scan_ranges 等）
        const auto& local_params = _params.local_params[i];
        auto fragment_instance_id = local_params.fragment_instance_id;
        // 为当前实例创建运行时过滤器管理器，用于处理运行时过滤器的推送和接收
        auto runtime_filter_mgr = std::make_unique<RuntimeFilterMgr>(false);
        // Pipeline ID 到 PipelineTask 指针的映射，用于后续建立依赖关系
        std::map<PipelineId, PipelineTask*> pipeline_id_to_task;
        
        // 定义 lambda 函数：获取指定 Pipeline 的共享状态映射
        // 共享状态用于在不同 Pipeline 之间共享数据（如 Hash Join 的 Build 端数据）
        auto get_shared_state = [&](PipelinePtr pipeline)
                -> std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                           std::vector<std::shared_ptr<Dependency>>>> {
            std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                    std::vector<std::shared_ptr<Dependency>>>>
                    shared_state_map;
            // 遍历 Pipeline 中的所有算子，查找需要共享状态的 Source 算子
            // Source 算子（如 JoinProbeSource）需要从上游 Pipeline 的 Sink（如 JoinBuildSink）获取共享状态
            for (auto& op : pipeline->operators()) {
                auto source_id = op->operator_id();
                if (auto iter = _op_id_to_shared_state.find(source_id);
                    iter != _op_id_to_shared_state.end()) {
                    shared_state_map.insert({source_id, iter->second});
                }
            }
            // 遍历 Pipeline 的 Sink，查找需要共享状态的 Sink 算子
            // Sink 算子（如 JoinBuildSink）会创建共享状态，供下游 Pipeline 的 Source 使用
            for (auto sink_to_source_id : pipeline->sink()->dests_id()) {
                if (auto iter = _op_id_to_shared_state.find(sink_to_source_id);
                    iter != _op_id_to_shared_state.end()) {
                    shared_state_map.insert({sink_to_source_id, iter->second});
                }
            }
            return shared_state_map;
        };

        // ===== 阶段 2.1：为每个 Pipeline 创建对应的 PipelineTask =====
        // 注意：这个循环是在 pre_and_submit lambda 内部，而 pre_and_submit 会被调用 target_size 次
        // （每个 fragment instance 一次，见下面的 621-630 行或 653-655 行）
        // 因此，一个 Pipeline 创建多个 Task 的逻辑是：
        // - 如果 pipeline->num_tasks() > 1：每个 fragment instance (i) 都会创建一个 Task
        //   例如：target_size=8, num_tasks=8，则总共创建 8 个 Task（每个 instance 一个）
        // - 如果 pipeline->num_tasks() == 1：只有第一个 fragment instance (i==0) 创建 Task
        //   例如：target_size=8, num_tasks=1，则总共创建 1 个 Task（只有 instance 0）
        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            auto& pipeline = _pipelines[pip_idx];
            // 判断是否需要为当前实例（第 i 个 fragment instance）创建该 Pipeline 的 Task：
            // - pipeline->num_tasks() > 1：该 Pipeline 需要多个并发任务（如并行扫描）
            //   此时每个 fragment instance 都会创建一个 Task，实现并行执行
            // - i == 0：第一个实例总是需要创建（即使 num_tasks == 1）
            //   这样可以确保至少有一个实例执行该 Pipeline（串行算子场景）
            if (pipeline->num_tasks() > 1 || i == 0) {
                // 创建该 Task 的运行时状态（RuntimeState）
                // RuntimeState 包含查询执行所需的所有上下文信息（如内存追踪器、描述符表等）
                auto task_runtime_state = RuntimeState::create_unique(
                        local_params.fragment_instance_id, _params.query_id, _params.fragment_id,
                        _params.query_options, _query_ctx->query_globals, _exec_env,
                        _query_ctx.get());
                {
                    // 初始化运行时状态的各种属性
                    // 设置查询级别的内存追踪器，用于监控和限制该 Task 的内存使用
                    task_runtime_state->set_query_mem_tracker(_query_ctx->query_mem_tracker());

                    // 设置任务执行上下文，让 Task 可以访问 FragmentContext
                    task_runtime_state->set_task_execution_context(shared_from_this());
                    // 设置后端节点编号，用于标识该 Task 运行在哪个 BE 节点上
                    task_runtime_state->set_be_number(local_params.backend_num);

                    // 以下为可选参数的设置（根据 Thrift 参数是否设置来决定）
                    if (_params.__isset.backend_id) {
                        task_runtime_state->set_backend_id(_params.backend_id);
                    }
                    if (_params.__isset.import_label) {
                        task_runtime_state->set_import_label(_params.import_label);
                    }
                    if (_params.__isset.db_name) {
                        task_runtime_state->set_db_name(_params.db_name);
                    }
                    if (_params.__isset.load_job_id) {
                        task_runtime_state->set_load_job_id(_params.load_job_id);
                    }
                    if (_params.__isset.wal_id) {
                        task_runtime_state->set_wal_id(_params.wal_id);
                    }
                    if (_params.__isset.content_length) {
                        task_runtime_state->set_content_length(_params.content_length);
                    }

                    // 设置描述符表（包含表结构、列信息等），算子需要用它来解析数据
                    task_runtime_state->set_desc_tbl(_desc_tbl);
                    // 设置当前实例在 fragment 中的索引和总数，用于数据分布和 shuffle
                    task_runtime_state->set_per_fragment_instance_idx(local_params.sender_id);
                    task_runtime_state->set_num_per_fragment_instances(_params.num_senders);
                    // 为所有算子预留本地状态的存储空间
                    task_runtime_state->resize_op_id_to_local_state(max_operator_id());
                    task_runtime_state->set_max_operator_id(max_operator_id());
                    // 设置 Load 相关的流参数（用于 Stream Load 场景）
                    task_runtime_state->set_load_stream_per_node(_params.load_stream_per_node);
                    task_runtime_state->set_total_load_streams(_params.total_load_streams);
                    task_runtime_state->set_num_local_sink(_params.num_local_sink);

                    // 设置运行时过滤器管理器，用于接收和推送运行时过滤器
                    task_runtime_state->set_runtime_filter_mgr(runtime_filter_mgr.get());
                }
                // 分配全局唯一的 Task ID，用于标识和追踪该 Task
                auto cur_task_id = _total_tasks++;
                task_runtime_state->set_task_id(cur_task_id);
                // 设置该 Pipeline 的总任务数（用于并发控制）
                task_runtime_state->set_task_num(pipeline->num_tasks());
                // 创建 PipelineTask 对象
                // PipelineTask 是 Pipeline 执行的最小单元，封装了算子的执行逻辑
                auto task = std::make_shared<PipelineTask>(
                        pipeline, cur_task_id, task_runtime_state.get(),
                        std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()),
                        pipeline_id_to_profile[pip_idx].get(), get_shared_state(pipeline), i);
                // 通知 Pipeline 已创建了一个 Task，更新 Pipeline 的任务计数
                pipeline->incr_created_tasks(i, task.get());
                // 记录 Pipeline ID 到 Task 的映射，用于后续建立依赖关系
                pipeline_id_to_task.insert({pipeline->id(), task.get()});
                // 将 Task 和对应的 RuntimeState 存储到 _tasks 容器中
                // 注意：Task 和 RuntimeState 的生命周期绑定在一起
                _tasks[i].emplace_back(
                        std::pair<std::shared_ptr<PipelineTask>, std::unique_ptr<RuntimeState>> {
                                std::move(task), std::move(task_runtime_state)});
            }
        }

        // ===== 阶段 2.2：构建 Pipeline 之间的依赖关系（DAG） =====
        /**
         * Build DAG for pipeline tasks.
         * For example, we have
         *
         *   ExchangeSink (Pipeline1)     JoinBuildSink (Pipeline2)
         *            \                      /
         *          JoinProbeOperator1 (Pipeline1)    JoinBuildSink (Pipeline3)
         *                 \                          /
         *               JoinProbeOperator2 (Pipeline1)
         *
         * In this fragment, we have three pipelines and pipeline 1 depends on pipeline 2 and pipeline 3.
         * To build this DAG, `_dag` manage dependencies between pipelines by pipeline ID and
         * `pipeline_id_to_task` is used to find the task by a unique pipeline ID.
         *
         * Finally, we have two upstream dependencies in Pipeline1 corresponding to JoinProbeOperator1
         * and JoinProbeOperator2.
         */
        // 遍历所有 Pipeline，建立它们之间的依赖关系
        // 依赖关系表示：下游 Pipeline（如 JoinProbe）需要等待上游 Pipeline（如 JoinBuild）完成
        for (auto& _pipeline : _pipelines) {
            if (pipeline_id_to_task.contains(_pipeline->id())) {
                auto* task = pipeline_id_to_task[_pipeline->id()];
                DCHECK(task != nullptr);

                // 如果当前 Pipeline 有上游依赖（在 _dag 中记录），则注入共享状态
                // 共享状态是上游 Pipeline 的 Sink 创建，供下游 Pipeline 的 Source 使用
                if (_dag.contains(_pipeline->id())) {
                    auto& deps = _dag[_pipeline->id()];
                    // 遍历所有依赖的上游 Pipeline
                    for (auto& dep : deps) {
                        if (pipeline_id_to_task.contains(dep)) {
                            // 尝试从上游 Pipeline 的 Sink 获取共享状态
                            // 例如：JoinBuildSink 创建的 HashTable 共享状态
                            auto ss = pipeline_id_to_task[dep]->get_sink_shared_state();
                            if (ss) {
                                // 如果上游有 Sink 共享状态，注入到当前 Task
                                // 这样当前 Task 的 Source（如 JoinProbeSource）就可以访问上游的数据
                                task->inject_shared_state(ss);
                            } else {
                                // 如果上游没有 Sink 共享状态，可能是反向依赖
                                // 将当前 Task 的 Source 共享状态注入到上游 Task
                                // 这种情况较少见，通常用于特殊的数据交换场景
                                pipeline_id_to_task[dep]->inject_shared_state(
                                        task->get_source_shared_state());
                            }
                        }
                    }
                }
            }
        }
        // ===== 阶段 2.3：准备每个 Task（初始化本地状态、设置扫描范围等） =====
        for (size_t pip_idx = 0; pip_idx < _pipelines.size(); pip_idx++) {
            if (pipeline_id_to_task.contains(_pipelines[pip_idx]->id())) {
                auto* task = pipeline_id_to_task[_pipelines[pip_idx]->id()];
                DCHECK(pipeline_id_to_profile[pip_idx]);
                // 获取该 Pipeline 的扫描范围（ScanRange）
                // ScanRange 定义了需要扫描的数据范围（如表的分区、文件块等）
                std::vector<TScanRangeParams> scan_ranges;
                // 获取 Pipeline 的第一个算子（通常是 Source 算子，如 ScanOperator）的节点 ID
                auto node_id = _pipelines[pip_idx]->operators().front()->node_id();
                // 从本地参数中查找该节点对应的扫描范围
                if (local_params.per_node_scan_ranges.contains(node_id)) {
                    scan_ranges = local_params.per_node_scan_ranges.find(node_id)->second;
                }
                // 准备 Task：初始化算子的本地状态、设置扫描范围、初始化 Sink 等
                // 这是 Task 执行前的最后一步准备工作
                RETURN_IF_ERROR_OR_CATCH_EXCEPTION(task->prepare(
                        scan_ranges, local_params.sender_id, _params.fragment.output_sink));
            }
        }
        // 将运行时过滤器管理器存储到映射中，供后续使用
        {
            std::lock_guard<std::mutex> l(_state_map_lock);
            _runtime_filter_mgr_map[i] = std::move(runtime_filter_mgr);
        }
        return Status::OK();
    };
    
    // ===== 阶段 3：根据实例数量决定并行准备还是串行准备 =====
    // 如果实例数量足够大（超过 parallel_prepare_threshold），使用多线程并行准备
    // 这样可以加快大量实例的准备工作，提高查询启动速度
    if (target_size > 1 &&
        (_runtime_state->query_options().__isset.parallel_prepare_threshold &&
         target_size > _runtime_state->query_options().parallel_prepare_threshold)) {
        // 并行准备模式：使用线程池并行执行 pre_and_submit
        std::vector<Status> prepare_status(target_size);
        int submitted_tasks = 0;
        Status submit_status;
        // 使用 CountDownLatch 等待所有并行任务完成
        CountDownLatch latch((int)target_size);
        for (int i = 0; i < target_size; i++) {
            // 将每个实例的准备工作提交到线程池
            submit_status = thread_pool->submit_func([&, i]() {
                // 附加查询上下文，确保内存追踪和线程本地存储正确
                SCOPED_ATTACH_TASK(_query_ctx.get());
                // 执行准备工作
                prepare_status[i] = pre_and_submit(i, this);
                // 计数减一，表示该任务完成
                latch.count_down();
            });
            if (LIKELY(submit_status.ok())) {
                submitted_tasks++;
            } else {
                // 如果提交失败，停止提交更多任务
                break;
            }
        }
        // 等待所有已提交的任务完成（包括失败的任务）
        latch.arrive_and_wait(target_size - submitted_tasks);
        // 检查提交状态
        if (UNLIKELY(!submit_status.ok())) {
            return submit_status;
        }
        // 检查所有任务的执行状态，如果有失败则返回错误
        for (int i = 0; i < submitted_tasks; i++) {
            if (!prepare_status[i].ok()) {
                return prepare_status[i];
            }
        }
    } else {
        // 串行准备模式：顺序执行每个实例的准备工作
        // 适用于实例数量较少的情况，避免线程切换开销
        for (int i = 0; i < target_size; i++) {
            RETURN_IF_ERROR(pre_and_submit(i, this));
        }
    }
    
    // ===== 阶段 4：清理临时数据结构 =====
    // 清理 Pipeline 父子关系映射（仅在构建阶段需要）
    _pipeline_parent_map.clear();
    // 清理算子 ID 到共享状态的映射（已注入到 Task 中，不再需要）
    _op_id_to_shared_state.clear();

    return Status::OK();
}

void PipelineFragmentContext::_init_next_report_time() {
    auto interval_s = config::pipeline_status_report_interval;
    if (_is_report_success && interval_s > 0 && _timeout > interval_s) {
        VLOG_FILE << "enable period report: fragment id=" << _fragment_id;
        uint64_t report_fragment_offset = (uint64_t)(rand() % interval_s) * NANOS_PER_SEC;
        // We don't want to wait longer than it takes to run the entire fragment.
        _previous_report_time =
                MonotonicNanos() + report_fragment_offset - (uint64_t)(interval_s)*NANOS_PER_SEC;
        _disable_period_report = false;
    }
}

void PipelineFragmentContext::refresh_next_report_time() {
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    DCHECK(disable == true);
    _previous_report_time.store(MonotonicNanos(), std::memory_order_release);
    _disable_period_report.compare_exchange_strong(disable, false);
}

void PipelineFragmentContext::trigger_report_if_necessary() {
    if (!_is_report_success) {
        return;
    }
    auto disable = _disable_period_report.load(std::memory_order_acquire);
    if (disable) {
        return;
    }
    int32_t interval_s = config::pipeline_status_report_interval;
    if (interval_s <= 0) {
        LOG(WARNING)
                << "config::status_report_interval is equal to or less than zero, do not trigger "
                   "report.";
    }
    uint64_t next_report_time = _previous_report_time.load(std::memory_order_acquire) +
                                (uint64_t)(interval_s)*NANOS_PER_SEC;
    if (MonotonicNanos() > next_report_time) {
        if (!_disable_period_report.compare_exchange_strong(disable, true,
                                                            std::memory_order_acq_rel)) {
            return;
        }
        if (VLOG_FILE_IS_ON) {
            VLOG_FILE << "Reporting "
                      << "profile for query_id " << print_id(_query_id)
                      << ", fragment id: " << _fragment_id;

            std::stringstream ss;
            _runtime_state->runtime_profile()->compute_time_in_profile();
            _runtime_state->runtime_profile()->pretty_print(&ss);
            if (_runtime_state->load_channel_profile()) {
                _runtime_state->load_channel_profile()->pretty_print(&ss);
            }

            VLOG_FILE << "Query " << print_id(get_query_id()) << " fragment " << get_fragment_id()
                      << " profile:\n"
                      << ss.str();
        }
        auto st = send_report(false);
        if (!st.ok()) {
            disable = true;
            _disable_period_report.compare_exchange_strong(disable, false,
                                                           std::memory_order_acq_rel);
        }
    }
}

Status PipelineFragmentContext::_build_pipelines(ObjectPool* pool, const DescriptorTbl& descs,
                                                 OperatorPtr* root, PipelinePtr cur_pipe) {
    if (_params.fragment.plan.nodes.empty()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid plan which has no plan node!");
    }

    int node_idx = 0;

    RETURN_IF_ERROR(_create_tree_helper(pool, _params.fragment.plan.nodes, descs, nullptr,
                                        &node_idx, root, cur_pipe, 0, false));

    if (node_idx + 1 != _params.fragment.plan.nodes.size()) {
        return Status::InternalError(
                "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_tree_helper(ObjectPool* pool,
                                                    const std::vector<TPlanNode>& tnodes,
                                                    const DescriptorTbl& descs, OperatorPtr parent,
                                                    int* node_idx, OperatorPtr* root,
                                                    PipelinePtr& cur_pipe, int child_idx,
                                                    const bool followed_by_shuffled_operator) {
    // 遍历 tnodes 时需要保证 node_idx 没越界，否则说明 Thrift 计划树有问题
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        return Status::InternalError(
                "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                *node_idx, tnodes.size());
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    int num_children = tnodes[*node_idx].num_children;
    bool current_followed_by_shuffled_operator = followed_by_shuffled_operator;
    OperatorPtr op = nullptr;
    RETURN_IF_ERROR(_create_operator(pool, tnodes[*node_idx], descs, op, cur_pipe,
                                     parent == nullptr ? -1 : parent->node_id(), child_idx,
                                     followed_by_shuffled_operator));
    // 某些算子在 init 阶段会读取表达式/参数（例如聚合的 group by 列），用于后续局部 shuffle 规划
    // Initialization must be done here. For example, group by expressions in agg will be used to
    // decide if a local shuffle should be planed, so it must be initialized here.
    RETURN_IF_ERROR(op->init(tnode, _runtime_state.get()));
    // assert(parent != nullptr || (node_idx == 0 && root_expr != nullptr));
    if (parent != nullptr) {
        // add to parent's child(s)
        RETURN_IF_ERROR(parent->set_child(op));
    } else {
        *root = op;
    }
    /**
     * `ExchangeType::HASH_SHUFFLE` should be used if an operator is followed by a shuffled operator (shuffled hash join, union operator followed by co-located operators).
     *
     * For plan:
     * LocalExchange(id=0) -> Aggregation(id=1) -> ShuffledHashJoin(id=2)
     *                           Exchange(id=3) -> ShuffledHashJoinBuild(id=2)
     * We must ensure data distribution of `LocalExchange(id=0)` is same as Exchange(id=3).
     *
     * If an operator's is followed by a local exchange without shuffle (e.g. passthrough), a
     * shuffled local exchanger will be used before join so it is not followed by shuffle join.
     *
     * 上述注释强调：如果当前算子后面接的是需要 shuffle 的算子（比如 Shuffle Hash Join），
     * 就需要保证本地 LocalExchange 的数据分布与远端 Exchange 一致。
     */
    auto require_shuffled_data_distribution =
            cur_pipe->operators().empty()
                    ? cur_pipe->sink()->require_shuffled_data_distribution(_runtime_state.get())
                    : op->require_shuffled_data_distribution(_runtime_state.get());
    current_followed_by_shuffled_operator =
            (followed_by_shuffled_operator || op->is_shuffled_operator()) &&
            require_shuffled_data_distribution;

    // 如果是叶子节点，需要记录该算子是否串行，后续在构建 Source 时避免并发
    if (num_children == 0) {
        _use_serial_source = op->is_serial_operator();
    }
    // rely on that tnodes is preorder of the plan
    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(_create_tree_helper(pool, tnodes, descs, op, node_idx, nullptr, cur_pipe, i,
                                            current_followed_by_shuffled_operator));

        // 遍历完子节点后立即检查越界，避免访问非法节点
        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            return Status::InternalError(
                    "Failed to reconstruct plan tree from thrift. Node id: {}, number of nodes: {}",
                    *node_idx, tnodes.size());
        }
    }

    return Status::OK();
}

void PipelineFragmentContext::_inherit_pipeline_properties(
        const DataDistribution& data_distribution, PipelinePtr pipe_with_source,
        PipelinePtr pipe_with_sink) {
    pipe_with_sink->set_num_tasks(pipe_with_source->num_tasks());
    pipe_with_source->set_num_tasks(_num_instances);
    pipe_with_source->set_data_distribution(data_distribution);
}

Status PipelineFragmentContext::_add_local_exchange_impl(
        int idx, ObjectPool* pool, PipelinePtr cur_pipe, PipelinePtr new_pip,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    auto& operators = cur_pipe->operators();
    const auto downstream_pipeline_id = cur_pipe->id();
    auto local_exchange_id = next_operator_id();
    // 1. Create a new pipeline with local exchange sink.
    DataSinkOperatorPtr sink;
    auto sink_id = next_sink_operator_id();

    /**
     * `bucket_seq_to_instance_idx` is empty if no scan operator is contained in this fragment.
     * So co-located operators(e.g. Agg, Analytic) should use `HASH_SHUFFLE` instead of `BUCKET_HASH_SHUFFLE`.
     */
    const bool followed_by_shuffled_operator =
            operators.size() > idx ? operators[idx]->followed_by_shuffled_operator()
                                   : cur_pipe->sink()->followed_by_shuffled_operator();
    const bool use_global_hash_shuffle = bucket_seq_to_instance_idx.empty() &&
                                         !shuffle_idx_to_instance_idx.contains(-1) &&
                                         followed_by_shuffled_operator && !_use_serial_source;
    sink = std::make_shared<LocalExchangeSinkOperatorX>(
            sink_id, local_exchange_id, use_global_hash_shuffle ? _total_instances : _num_instances,
            data_distribution.partition_exprs, bucket_seq_to_instance_idx);
    if (bucket_seq_to_instance_idx.empty() &&
        data_distribution.distribution_type == ExchangeType::BUCKET_HASH_SHUFFLE) {
        data_distribution.distribution_type = ExchangeType::HASH_SHUFFLE;
    }
    RETURN_IF_ERROR(new_pip->set_sink(sink));
    RETURN_IF_ERROR(new_pip->sink()->init(_runtime_state.get(), data_distribution.distribution_type,
                                          num_buckets, use_global_hash_shuffle,
                                          shuffle_idx_to_instance_idx));

    // 2. Create and initialize LocalExchangeSharedState.
    std::shared_ptr<LocalExchangeSharedState> shared_state =
            LocalExchangeSharedState::create_shared(_num_instances);
    switch (data_distribution.distribution_type) {
    case ExchangeType::HASH_SHUFFLE:
        shared_state->exchanger = ShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances,
                use_global_hash_shuffle ? _total_instances : _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        shared_state->exchanger = BucketShuffleExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances, num_buckets,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::PASSTHROUGH:
        shared_state->exchanger = PassthroughExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::BROADCAST:
        shared_state->exchanger = BroadcastExchanger::create_unique(
                cur_pipe->num_tasks(), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    case ExchangeType::PASS_TO_ONE:
        if (_runtime_state->enable_share_hash_table_for_broadcast_join()) {
            // If shared hash table is enabled for BJ, hash table will be built by only one task
            shared_state->exchanger = PassToOneExchanger::create_unique(
                    cur_pipe->num_tasks(), _num_instances,
                    _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                            ? cast_set<int>(_runtime_state->query_options()
                                                    .local_exchange_free_blocks_limit)
                            : 0);
        } else {
            shared_state->exchanger = BroadcastExchanger::create_unique(
                    cur_pipe->num_tasks(), _num_instances,
                    _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                            ? cast_set<int>(_runtime_state->query_options()
                                                    .local_exchange_free_blocks_limit)
                            : 0);
        }
        break;
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        shared_state->exchanger = AdaptivePassthroughExchanger::create_unique(
                std::max(cur_pipe->num_tasks(), _num_instances), _num_instances,
                _runtime_state->query_options().__isset.local_exchange_free_blocks_limit
                        ? cast_set<int>(
                                  _runtime_state->query_options().local_exchange_free_blocks_limit)
                        : 0);
        break;
    default:
        return Status::InternalError("Unsupported local exchange type : " +
                                     std::to_string((int)data_distribution.distribution_type));
    }
    shared_state->create_source_dependencies(_num_instances, local_exchange_id, local_exchange_id,
                                             "LOCAL_EXCHANGE_OPERATOR");
    shared_state->create_sink_dependency(sink_id, local_exchange_id, "LOCAL_EXCHANGE_SINK");
    _op_id_to_shared_state.insert({local_exchange_id, {shared_state, shared_state->sink_deps}});

    // 3. Set two pipelines' operator list. For example, split pipeline [Scan - AggSink] to
    // pipeline1 [Scan - LocalExchangeSink] and pipeline2 [LocalExchangeSource - AggSink].

    // 3.1 Initialize new pipeline's operator list.
    std::copy(operators.begin(), operators.begin() + idx,
              std::inserter(new_pip->operators(), new_pip->operators().end()));

    // 3.2 Erase unused operators in previous pipeline.
    operators.erase(operators.begin(), operators.begin() + idx);

    // 4. Initialize LocalExchangeSource and insert it into this pipeline.
    OperatorPtr source_op;
    source_op = std::make_shared<LocalExchangeSourceOperatorX>(pool, local_exchange_id);
    RETURN_IF_ERROR(source_op->set_child(new_pip->operators().back()));
    RETURN_IF_ERROR(source_op->init(data_distribution.distribution_type));
    if (!operators.empty()) {
        RETURN_IF_ERROR(operators.front()->set_child(nullptr));
        RETURN_IF_ERROR(operators.front()->set_child(source_op));
    }
    operators.insert(operators.begin(), source_op);

    // 5. Set children for two pipelines separately.
    std::vector<std::shared_ptr<Pipeline>> new_children;
    std::vector<PipelineId> edges_with_source;
    for (auto child : cur_pipe->children()) {
        bool found = false;
        for (auto op : new_pip->operators()) {
            if (child->sink()->node_id() == op->node_id()) {
                new_pip->set_children(child);
                found = true;
            };
        }
        if (!found) {
            new_children.push_back(child);
            edges_with_source.push_back(child->id());
        }
    }
    new_children.push_back(new_pip);
    edges_with_source.push_back(new_pip->id());

    // 6. Set DAG for new pipelines.
    if (!new_pip->children().empty()) {
        std::vector<PipelineId> edges_with_sink;
        for (auto child : new_pip->children()) {
            edges_with_sink.push_back(child->id());
        }
        _dag.insert({new_pip->id(), edges_with_sink});
    }
    cur_pipe->set_children(new_children);
    _dag[downstream_pipeline_id] = edges_with_source;
    RETURN_IF_ERROR(new_pip->sink()->set_child(new_pip->operators().back()));
    RETURN_IF_ERROR(cur_pipe->sink()->set_child(nullptr));
    RETURN_IF_ERROR(cur_pipe->sink()->set_child(cur_pipe->operators().back()));

    // 7. Inherit properties from current pipeline.
    _inherit_pipeline_properties(data_distribution, cur_pipe, new_pip);
    return Status::OK();
}

Status PipelineFragmentContext::_add_local_exchange(
        int pip_idx, int idx, int node_id, ObjectPool* pool, PipelinePtr cur_pipe,
        DataDistribution data_distribution, bool* do_local_exchange, int num_buckets,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    if (_num_instances <= 1 || cur_pipe->num_tasks_of_parent() <= 1) {
        return Status::OK();
    }

    if (!cur_pipe->need_to_local_exchange(data_distribution, idx)) {
        return Status::OK();
    }
    *do_local_exchange = true;

    auto& operators = cur_pipe->operators();
    auto total_op_num = operators.size();
    auto new_pip = add_pipeline(cur_pipe, pip_idx + 1);
    RETURN_IF_ERROR(_add_local_exchange_impl(
            idx, pool, cur_pipe, new_pip, data_distribution, do_local_exchange, num_buckets,
            bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));

    CHECK(total_op_num + 1 == cur_pipe->operators().size() + new_pip->operators().size())
            << "total_op_num: " << total_op_num
            << " cur_pipe->operators().size(): " << cur_pipe->operators().size()
            << " new_pip->operators().size(): " << new_pip->operators().size();

    // There are some local shuffles with relatively heavy operations on the sink.
    // If the local sink concurrency is 1 and the local source concurrency is n, the sink becomes a bottleneck.
    // Therefore, local passthrough is used to increase the concurrency of the sink.
    // op -> local sink(1) -> local source (n)
    // op -> local passthrough(1) -> local passthrough(n) ->  local sink(n) -> local source (n)
    if (cur_pipe->num_tasks() > 1 && new_pip->num_tasks() == 1 &&
        Pipeline::heavy_operations_on_the_sink(data_distribution.distribution_type)) {
        RETURN_IF_ERROR(_add_local_exchange_impl(
                cast_set<int>(new_pip->operators().size()), pool, new_pip,
                add_pipeline(new_pip, pip_idx + 2), DataDistribution(ExchangeType::PASSTHROUGH),
                do_local_exchange, num_buckets, bucket_seq_to_instance_idx,
                shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

/**
 * 规划本地数据交换（Local Exchange）
 * 
 * 本地交换用于在同一个 BE 节点内的不同 Pipeline 之间重新分布数据，主要场景包括：
 * 1. 数据重新分区：当后续算子需要不同的数据分布时（如 Hash Join 需要按 join key 分区）
 * 2. 并行度调整：调整 Pipeline 的并行度，提高执行效率
 * 3. 数据倾斜处理：通过重新分区缓解数据倾斜问题
 * 
 * 该函数是规划本地交换的入口函数，遍历所有 Pipeline 并为每个 Pipeline 规划本地交换节点。
 * 采用从后往前的遍历顺序，确保在规划当前 Pipeline 时，其子 Pipeline 的数据分布已经确定。
 * 
 * @param num_buckets 分桶数量，用于 BUCKET_HASH_SHUFFLE 类型的数据分布
 * @param bucket_seq_to_instance_idx bucket 序列号到实例索引的映射，用于确定数据路由
 * @param shuffle_idx_to_instance_idx shuffle 索引到实例索引的映射，用于全局 shuffle
 * @return Status 返回规划状态，成功返回 OK
 */
Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    // 从后往前遍历所有 Pipeline（从最后一个到第一个）
    // 这样在规划当前 Pipeline 时，其子 Pipeline 的数据分布已经确定，可以正确继承
    for (int pip_idx = cast_set<int>(_pipelines.size()) - 1; pip_idx >= 0; pip_idx--) {
        // 1. 初始化当前 Pipeline 的数据分布属性
        // 数据分布描述了数据在 Pipeline 中的分布方式（如哈希分区、轮询、广播等）
        _pipelines[pip_idx]->init_data_distribution(_runtime_state.get());
        
        // 2. 从子 Pipeline 继承数据分布（如果适用）
        // 如果当前 Pipeline 有子 Pipeline，并且子 Pipeline 的 sink 节点 ID 等于
        // 当前 Pipeline 的第一个 operator 节点 ID，说明它们是直接连接的（不是 Join 的子节点），
        // 此时可以继承子 Pipeline 的数据分布，避免不必要的本地交换
        if (!_pipelines[pip_idx]->children().empty()) {
            for (auto& child : _pipelines[pip_idx]->children()) {
                // 检查子 Pipeline 的 sink 节点是否直接连接到当前 Pipeline 的第一个 operator
                // 如果是，说明数据流是连续的，可以继承数据分布
                if (child->sink()->node_id() ==
                    _pipelines[pip_idx]->operators().front()->node_id()) {
                    // 继承子 Pipeline 的数据分布，避免重复的本地交换
                    _pipelines[pip_idx]->set_data_distribution(child->data_distribution());
                }
            }
        }

        // 3. 为当前 Pipeline 规划本地交换节点
        // 调用重载版本的 _plan_local_exchange，它会检查 Pipeline 中的每个 operator 和 sink
        // 是否需要本地交换，并在需要时插入 LocalExchangeSink 和 LocalExchangeSource
        // 
        // 注意：如果 num_buckets == 0，说明 Fragment 是通过 Exchange 节点进行 co-located 的，
        // 而不是通过 Scan 节点。此时使用 _num_instance 替代 num_buckets 以避免除零错误，
        // 同时保持 co-locate 计划在本地 shuffle 之后仍然有效
        RETURN_IF_ERROR(_plan_local_exchange(num_buckets, pip_idx, _pipelines[pip_idx],
                                             bucket_seq_to_instance_idx,
                                             shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

/**
 * 为单个 Pipeline 规划本地数据交换（Local Exchange）
 *
 * 该函数负责在 Pipeline 内部规划本地数据交换，用于在同一个 BE 节点内的多个 Task 之间重新分布数据。
 * Local Exchange 主要用于：
 * 1. 满足下游 Operator 的数据分布要求（如 Hash Join Build 需要按 Join Key 分桶）
 * 2. 实现数据广播（Broadcast）或重新分区（Shuffle）
 * 3. 调整 Pipeline 的并行度或处理数据倾斜
 *
 * 工作原理：
 * - 遍历 Pipeline 中的每个 Operator，检查其数据分布要求
 * - 如果某个 Operator 需要特定的数据分布（如 HASH_SHUFFLE、BROADCAST），
 *   则在该 Operator 之前插入 LocalExchangeSink 和 LocalExchangeSource
 * - 插入 Local Exchange 后，Pipeline 会被分割成两个 Pipeline：
 *   * 原 Pipeline：在 LocalExchangeSink 处结束
 *   * 新 Pipeline：从 LocalExchangeSource 开始，包含剩余的 Operator
 * - 使用 do-while 循环确保所有需要 Local Exchange 的位置都被处理
 *
 * @param num_buckets 分桶数量，用于哈希分区
 * @param pip_idx Pipeline 索引
 * @param pip 当前 Pipeline 指针
 * @param bucket_seq_to_instance_idx 桶序列到实例索引的映射
 * @param shuffle_idx_to_instance_idx shuffle 索引到实例索引的映射
 * @return Status 返回执行状态
 */
Status PipelineFragmentContext::_plan_local_exchange(
        int num_buckets, int pip_idx, PipelinePtr pip,
        const std::map<int, int>& bucket_seq_to_instance_idx,
        const std::map<int, int>& shuffle_idx_to_instance_idx) {
    // 从索引 1 开始遍历 Operator（索引 0 通常是 Source Operator，不需要 Local Exchange）
    int idx = 1;
    bool do_local_exchange = false;
    
    // 使用 do-while 循环处理 Pipeline 分割后的情况
    // 当添加 Local Exchange 后，Pipeline 会被分割，需要重新处理新 Pipeline 中的剩余 Operator
    do {
        auto& ops = pip->operators();
        do_local_exchange = false;
        
        // 遍历 Pipeline 中的每个 Operator，检查是否需要 Local Exchange
        for (; idx < ops.size();) {
            // 检查当前 Operator 是否需要特定的数据分布（如 HASH_SHUFFLE、BROADCAST）
            if (ops[idx]->required_data_distribution(_runtime_state.get()).need_local_exchange()) {
                // 在当前 Operator 之前添加 LocalExchangeSink 和 LocalExchangeSource
                // 这会将 Pipeline 分割成两个 Pipeline：
                // - 原 Pipeline：在 LocalExchangeSink 处结束
                // - 新 Pipeline：从 LocalExchangeSource 开始，包含当前 Operator 及后续 Operator
                RETURN_IF_ERROR(_add_local_exchange(
                        pip_idx, idx, ops[idx]->node_id(), _runtime_state->obj_pool(), pip,
                        ops[idx]->required_data_distribution(_runtime_state.get()),
                        &do_local_exchange, num_buckets, bucket_seq_to_instance_idx,
                        shuffle_idx_to_instance_idx));
            }
            
            // 如果添加了 Local Exchange，Pipeline 已被分割
            if (do_local_exchange) {
                // Pipeline 分割后的结构：
                // [原 Pipeline: ... Operator(idx-1) -> LocalExchangeSink]
                // [新 Pipeline: LocalExchangeSource -> Operator(idx) -> ...]
                // 
                // 设置 idx = 2，因为：
                // - 索引 0：新 Pipeline 中的 LocalExchangeSource（已添加）
                // - 索引 1：当前 Operator（已处理，但需要在新 Pipeline 中重新检查）
                // - 索引 2：下一个需要检查的 Operator
                // 
                // 跳出当前循环，重新进入 do-while 循环处理新 Pipeline
                idx = 2;
                break;
            }
            idx++;
        }
    } while (do_local_exchange);
    
    // 最后检查 Sink Operator 是否需要 Local Exchange
    // Sink 通常需要特定的数据分布（如按分桶写入、广播结果等）
    if (pip->sink()->required_data_distribution(_runtime_state.get()).need_local_exchange()) {
        RETURN_IF_ERROR(_add_local_exchange(
                pip_idx, idx, pip->sink()->node_id(), _runtime_state->obj_pool(), pip,
                pip->sink()->required_data_distribution(_runtime_state.get()), &do_local_exchange,
                num_buckets, bucket_seq_to_instance_idx, shuffle_idx_to_instance_idx));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                                                  const std::vector<TExpr>& output_exprs,
                                                  const TPipelineFragmentParams& params,
                                                  const RowDescriptor& row_desc,
                                                  RuntimeState* state, DescriptorTbl& desc_tbl,
                                                  PipelineId cur_pipeline_id) {
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        _sink = std::make_shared<ExchangeSinkOperatorX>(
                state, row_desc, next_sink_operator_id(), thrift_sink.stream_sink,
                params.destinations, _fragment_instance_ids);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        _sink = std::make_shared<ResultSinkOperatorX>(next_sink_operator_id(), row_desc,
                                                      output_exprs, thrift_sink.result_sink);
        break;
    }
    case TDataSinkType::DICTIONARY_SINK: {
        if (!thrift_sink.__isset.dictionary_sink) {
            return Status::InternalError("Missing dict sink.");
        }

        _sink = std::make_shared<DictSinkOperatorX>(next_sink_operator_id(), row_desc, output_exprs,
                                                    thrift_sink.dictionary_sink);
        break;
    }
    case TDataSinkType::GROUP_COMMIT_OLAP_TABLE_SINK:
    case TDataSinkType::OLAP_TABLE_SINK: {
        if (state->query_options().enable_memtable_on_sink_node &&
            !_has_inverted_index_v1_or_partial_update(thrift_sink.olap_table_sink) &&
            !config::is_cloud_mode()) {
            _sink = std::make_shared<OlapTableSinkV2OperatorX>(pool, next_sink_operator_id(),
                                                               row_desc, output_exprs);
        } else {
            _sink = std::make_shared<OlapTableSinkOperatorX>(pool, next_sink_operator_id(),
                                                             row_desc, output_exprs);
        }
        break;
    }
    case TDataSinkType::GROUP_COMMIT_BLOCK_SINK: {
        DCHECK(thrift_sink.__isset.olap_table_sink);
        DCHECK(state->get_query_ctx() != nullptr);
        state->get_query_ctx()->query_mem_tracker()->is_group_commit_load = true;
        _sink = std::make_shared<GroupCommitBlockSinkOperatorX>(next_sink_operator_id(), row_desc,
                                                                output_exprs);
        break;
    }
    case TDataSinkType::HIVE_TABLE_SINK: {
        if (!thrift_sink.__isset.hive_table_sink) {
            return Status::InternalError("Missing hive table sink.");
        }
        _sink = std::make_shared<HiveTableSinkOperatorX>(pool, next_sink_operator_id(), row_desc,
                                                         output_exprs);
        break;
    }
    case TDataSinkType::ICEBERG_TABLE_SINK: {
        if (!thrift_sink.__isset.iceberg_table_sink) {
            return Status::InternalError("Missing hive table sink.");
        }
        _sink = std::make_shared<IcebergTableSinkOperatorX>(pool, next_sink_operator_id(), row_desc,
                                                            output_exprs);
        break;
    }
    case TDataSinkType::JDBC_TABLE_SINK: {
        if (!thrift_sink.__isset.jdbc_table_sink) {
            return Status::InternalError("Missing data jdbc sink.");
        }
        if (config::enable_java_support) {
            _sink = std::make_shared<JdbcTableSinkOperatorX>(row_desc, next_sink_operator_id(),
                                                             output_exprs);
        } else {
            return Status::InternalError(
                    "Jdbc table sink is not enabled, you can change be config "
                    "enable_java_support to true and restart be.");
        }
        break;
    }
    case TDataSinkType::MEMORY_SCRATCH_SINK: {
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        _sink = std::make_shared<MemoryScratchSinkOperatorX>(row_desc, next_sink_operator_id(),
                                                             output_exprs);
        break;
    }
    case TDataSinkType::RESULT_FILE_SINK: {
        if (!thrift_sink.__isset.result_file_sink) {
            return Status::InternalError("Missing result file sink.");
        }

        // Result file sink is not the top sink
        if (params.__isset.destinations && !params.destinations.empty()) {
            _sink = std::make_shared<ResultFileSinkOperatorX>(
                    next_sink_operator_id(), row_desc, thrift_sink.result_file_sink,
                    params.destinations, output_exprs, desc_tbl);
        } else {
            _sink = std::make_shared<ResultFileSinkOperatorX>(next_sink_operator_id(), row_desc,
                                                              output_exprs);
        }
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink);
        DCHECK_GT(thrift_sink.multi_cast_stream_sink.sinks.size(), 0);
        auto sink_id = next_sink_operator_id();
        const int multi_cast_node_id = sink_id;
        auto sender_size = thrift_sink.multi_cast_stream_sink.sinks.size();
        // one sink has multiple sources.
        std::vector<int> sources;
        for (int i = 0; i < sender_size; ++i) {
            auto source_id = next_operator_id();
            sources.push_back(source_id);
        }

        _sink = std::make_shared<MultiCastDataStreamSinkOperatorX>(
                sink_id, multi_cast_node_id, sources, pool, thrift_sink.multi_cast_stream_sink);
        for (int i = 0; i < sender_size; ++i) {
            auto new_pipeline = add_pipeline();
            // use to exchange sink
            RowDescriptor* exchange_row_desc = nullptr;
            {
                const auto& tmp_row_desc =
                        !thrift_sink.multi_cast_stream_sink.sinks[i].output_exprs.empty()
                                ? RowDescriptor(state->desc_tbl(),
                                                {thrift_sink.multi_cast_stream_sink.sinks[i]
                                                         .output_tuple_id})
                                : row_desc;
                exchange_row_desc = pool->add(new RowDescriptor(tmp_row_desc));
            }
            auto source_id = sources[i];
            OperatorPtr source_op;
            // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
            source_op = std::make_shared<MultiCastDataStreamerSourceOperatorX>(
                    /*node_id*/ source_id, /*consumer_id*/ i, pool,
                    thrift_sink.multi_cast_stream_sink.sinks[i], row_desc,
                    /*operator_id=*/source_id);
            RETURN_IF_ERROR(new_pipeline->add_operator(
                    source_op, params.__isset.parallel_instances ? params.parallel_instances : 0));
            // 2. create and set sink operator of data stream sender for new pipeline

            DataSinkOperatorPtr sink_op;
            sink_op = std::make_shared<ExchangeSinkOperatorX>(
                    state, *exchange_row_desc, next_sink_operator_id(),
                    thrift_sink.multi_cast_stream_sink.sinks[i],
                    thrift_sink.multi_cast_stream_sink.destinations[i], _fragment_instance_ids);

            RETURN_IF_ERROR(new_pipeline->set_sink(sink_op));
            {
                TDataSink* t = pool->add(new TDataSink());
                t->stream_sink = thrift_sink.multi_cast_stream_sink.sinks[i];
                RETURN_IF_ERROR(sink_op->init(*t));
            }

            // 3. set dependency dag
            _dag[new_pipeline->id()].push_back(cur_pipeline_id);
        }
        if (sources.empty()) {
            return Status::InternalError("size of sources must be greater than 0");
        }
        break;
    }
    case TDataSinkType::BLACKHOLE_SINK: {
        if (!thrift_sink.__isset.blackhole_sink) {
            return Status::InternalError("Missing blackhole sink.");
        }

        _sink.reset(new BlackholeSinkOperatorX(next_sink_operator_id()));
        break;
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return Status::OK();
}

// NOLINTBEGIN(readability-function-size)
// NOLINTBEGIN(readability-function-cognitive-complexity)
Status PipelineFragmentContext::_create_operator(ObjectPool* pool, const TPlanNode& tnode,
                                                 const DescriptorTbl& descs, OperatorPtr& op,
                                                 PipelinePtr& cur_pipe, int parent_idx,
                                                 int child_idx,
                                                 const bool followed_by_shuffled_operator) {
    // We directly construct the operator from Thrift because the given array is in the order of preorder traversal.
    // Therefore, here we need to use a stack-like structure.
    _pipeline_parent_map.pop(cur_pipe, parent_idx, child_idx);
    std::stringstream error_msg;
    bool enable_query_cache = _params.fragment.__isset.query_cache_param;

    bool fe_with_old_version = false;
    switch (tnode.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE: {
        op = std::make_shared<OlapScanOperatorX>(
                pool, tnode, next_operator_id(), descs, _num_instances,
                enable_query_cache ? _params.fragment.query_cache_param : TQueryCacheParam {});
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::GROUP_COMMIT_SCAN_NODE: {
        DCHECK(_query_ctx != nullptr);
        _query_ctx->query_mem_tracker()->is_group_commit_load = true;
        op = std::make_shared<GroupCommitOperatorX>(pool, tnode, next_operator_id(), descs,
                                                    _num_instances);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::JDBC_SCAN_NODE: {
        if (config::enable_java_support) {
            op = std::make_shared<JDBCScanOperatorX>(pool, tnode, next_operator_id(), descs,
                                                     _num_instances);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        } else {
            return Status::InternalError(
                    "Jdbc scan node is disabled, you can change be config enable_java_support "
                    "to true and restart be.");
        }
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::FILE_SCAN_NODE: {
        op = std::make_shared<FileScanOperatorX>(pool, tnode, next_operator_id(), descs,
                                                 _num_instances);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::ES_SCAN_NODE:
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        op = std::make_shared<EsScanOperatorX>(pool, tnode, next_operator_id(), descs,
                                               _num_instances);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        int num_senders = _params.per_exch_num_senders.contains(tnode.node_id)
                                  ? _params.per_exch_num_senders.find(tnode.node_id)->second
                                  : 0;
        DCHECK_GT(num_senders, 0);
        op = std::make_shared<ExchangeSourceOperatorX>(pool, tnode, next_operator_id(), descs,
                                                       num_senders);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        if (tnode.agg_node.grouping_exprs.empty() &&
            descs.get_tuple_descriptor(tnode.agg_node.output_tuple_id)->slots().empty()) {
            return Status::InternalError("Illegal aggregate node " + std::to_string(tnode.node_id) +
                                         ": group by and output is empty");
        }
        bool need_create_cache_op =
                enable_query_cache && tnode.node_id == _params.fragment.query_cache_param.node_id;
        auto create_query_cache_operator = [&](PipelinePtr& new_pipe) {
            auto cache_node_id = _params.local_params[0].per_node_scan_ranges.begin()->first;
            auto cache_source_id = next_operator_id();
            op = std::make_shared<CacheSourceOperatorX>(pool, cache_node_id, cache_source_id,
                                                        _params.fragment.query_cache_param);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            new_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(new_pipe->id());

            DataSinkOperatorPtr cache_sink(new CacheSinkOperatorX(
                    next_sink_operator_id(), cache_source_id, op->operator_id()));
            RETURN_IF_ERROR(new_pipe->set_sink(cache_sink));
            return Status::OK();
        };
        const bool group_by_limit_opt =
                tnode.agg_node.__isset.agg_sort_info_by_group_key && tnode.limit > 0;

        /// PartitionedAggSourceOperatorX does not support "group by limit opt(#29641)" yet.
        /// If `group_by_limit_opt` is true, then it might not need to spill at all.
        const bool enable_spill = _runtime_state->enable_spill() &&
                                  !tnode.agg_node.grouping_exprs.empty() && !group_by_limit_opt;
        const bool is_streaming_agg = tnode.agg_node.__isset.use_streaming_preaggregation &&
                                      tnode.agg_node.use_streaming_preaggregation &&
                                      !tnode.agg_node.grouping_exprs.empty();
        const bool can_use_distinct_streaming_agg =
                tnode.agg_node.aggregate_functions.empty() &&
                !tnode.agg_node.__isset.agg_sort_info_by_group_key &&
                _params.query_options.__isset.enable_distinct_streaming_aggregation &&
                _params.query_options.enable_distinct_streaming_aggregation;

        if (can_use_distinct_streaming_agg) {
            if (need_create_cache_op) {
                PipelinePtr new_pipe;
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));

                op = std::make_shared<DistinctStreamingAggOperatorX>(
                        pool, next_operator_id(), tnode, descs, _require_bucket_distribution);
                op->set_followed_by_shuffled_operator(false);
                _require_bucket_distribution = true;
                RETURN_IF_ERROR(new_pipe->add_operator(op, _parallel_instances));
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                cur_pipe = new_pipe;
            } else {
                op = std::make_shared<DistinctStreamingAggOperatorX>(
                        pool, next_operator_id(), tnode, descs, _require_bucket_distribution);
                op->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
                _require_bucket_distribution =
                        _require_bucket_distribution || op->require_data_distribution();
                RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
            }
        } else if (is_streaming_agg) {
            if (need_create_cache_op) {
                PipelinePtr new_pipe;
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));

                op = std::make_shared<StreamingAggOperatorX>(pool, next_operator_id(), tnode, descs,
                                                             _require_bucket_distribution);
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                RETURN_IF_ERROR(new_pipe->add_operator(op, _parallel_instances));
                cur_pipe = new_pipe;
            } else {
                op = std::make_shared<StreamingAggOperatorX>(pool, next_operator_id(), tnode, descs,
                                                             _require_bucket_distribution);
                RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
            }
        } else {
            // create new pipeline to add query cache operator
            PipelinePtr new_pipe;
            if (need_create_cache_op) {
                RETURN_IF_ERROR(create_query_cache_operator(new_pipe));
            }

            if (enable_spill) {
                op = std::make_shared<PartitionedAggSourceOperatorX>(pool, tnode,
                                                                     next_operator_id(), descs);
            } else {
                op = std::make_shared<AggSourceOperatorX>(pool, tnode, next_operator_id(), descs);
            }
            if (need_create_cache_op) {
                RETURN_IF_ERROR(cur_pipe->operators().front()->set_child(op));
                RETURN_IF_ERROR(new_pipe->add_operator(op, _parallel_instances));
                cur_pipe = new_pipe;
            } else {
                RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
            }

            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            cur_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(cur_pipe->id());

            DataSinkOperatorPtr sink;
            if (enable_spill) {
                sink = std::make_shared<PartitionedAggSinkOperatorX>(
                        pool, next_sink_operator_id(), op->operator_id(), tnode, descs,
                        _require_bucket_distribution);
            } else {
                sink = std::make_shared<AggSinkOperatorX>(pool, next_sink_operator_id(),
                                                          op->operator_id(), tnode, descs,
                                                          _require_bucket_distribution);
            }
            sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
            _require_bucket_distribution =
                    _require_bucket_distribution || sink->require_data_distribution();
            RETURN_IF_ERROR(cur_pipe->set_sink(sink));
            RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        }
        break;
    }
    case TPlanNodeType::HASH_JOIN_NODE: {
        // 1. 判断 Join 类型和执行策略
        // is_broadcast_join: 是否为广播 Join（小表广播到所有节点）
        // enable_spill: 是否启用溢出到磁盘功能（当内存不足时）
        const auto is_broadcast_join = tnode.hash_join_node.__isset.is_broadcast_join &&
                                       tnode.hash_join_node.is_broadcast_join;
        const auto enable_spill = _runtime_state->enable_spill();
        
        // 2. 如果启用 Spill 且不是广播 Join，创建分区 Hash Join（支持溢出到磁盘）
        // 分区 Hash Join 可以将数据分成多个分区，当某个分区的内存不足时，可以溢出到磁盘
        if (enable_spill && !is_broadcast_join) {
            // 2.1 准备 TPlanNode（清除 runtime filters，因为溢出场景下不需要）
            auto tnode_ = tnode;
            tnode_.runtime_filters.clear();
            uint32_t partition_count = _runtime_state->spill_hash_join_partition_count();
            
            // 2.2 创建内部 Probe Operator（用于处理溢出到磁盘的数据）
            // 当 Probe 端的数据溢出时，需要重新构建 Hash 表，此时使用内部 Probe Operator
            auto inner_probe_operator =
                    std::make_shared<HashJoinProbeOperatorX>(pool, tnode_, 0, descs);

            // 2.3 创建 Probe 端的内部 Build Sink Operator
            // 当 Probe 端数据溢出时，需要在 Probe 端重新构建 Hash 表
            // 使用 `tnode_`（没有 runtime filters），因为溢出场景下不需要运行时过滤
            auto probe_side_inner_sink_operator =
                    std::make_shared<HashJoinBuildSinkOperatorX>(pool, 0, 0, tnode_, descs);

            RETURN_IF_ERROR(inner_probe_operator->init(tnode_, _runtime_state.get()));
            RETURN_IF_ERROR(probe_side_inner_sink_operator->init(tnode_, _runtime_state.get()));

            // 2.4 创建分区 Hash Join Probe Operator（主 Probe Operator）
            // PartitionedHashJoinProbeOperatorX 负责：
            // - 将 Probe 端数据分区
            // - 处理内存中的分区数据
            // - 当内存不足时，将数据溢出到磁盘
            // - 从磁盘恢复数据并执行 Join
            auto probe_operator = std::make_shared<PartitionedHashJoinProbeOperatorX>(
                    pool, tnode_, next_operator_id(), descs, partition_count);
            // 设置内部 Operator，用于处理溢出数据
            probe_operator->set_inner_operators(probe_side_inner_sink_operator,
                                                inner_probe_operator);
            op = std::move(probe_operator);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

            // 2.5 创建 Build 端 Pipeline（用于构建 Hash 表）
            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            // 2.6 创建 Build 端的内部和外部 Sink Operator
            // inner_sink_operator: 用于处理溢出数据的内部 Build Sink
            // sink_operator: 分区 Hash Join Sink Operator（主 Build Sink）
            // PartitionedHashJoinSinkOperatorX 负责：
            // - 将 Build 端数据分区
            // - 在内存中构建 Hash 表
            // - 当内存不足时，将分区数据溢出到磁盘
            auto inner_sink_operator =
                    std::make_shared<HashJoinBuildSinkOperatorX>(pool, 0, 0, tnode, descs);
            auto sink_operator = std::make_shared<PartitionedHashJoinSinkOperatorX>(
                    pool, next_sink_operator_id(), op->operator_id(), tnode_, descs,
                    partition_count);
            RETURN_IF_ERROR(inner_sink_operator->init(tnode, _runtime_state.get()));

            // 设置内部 Operator，用于处理溢出数据
            sink_operator->set_inner_operators(inner_sink_operator, inner_probe_operator);
            DataSinkOperatorPtr sink = std::move(sink_operator);
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode_, _runtime_state.get()));

            // 2.7 记录 Pipeline 父子关系，用于后续的依赖管理
            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
            // 标记是否后续有 Shuffle Operator，用于优化数据分布
            sink->set_followed_by_shuffled_operator(sink->is_shuffled_operator());
            op->set_followed_by_shuffled_operator(op->is_shuffled_operator());
        } else {
            // 3. 创建普通 Hash Join（不支持溢出到磁盘）
            // 适用于内存充足或广播 Join 的场景
            
            // 3.1 创建 Probe Operator（用于探测 Hash 表）
            op = std::make_shared<HashJoinProbeOperatorX>(pool, tnode, next_operator_id(), descs);
            RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

            // 3.2 创建 Build 端 Pipeline（用于构建 Hash 表）
            const auto downstream_pipeline_id = cur_pipe->id();
            if (!_dag.contains(downstream_pipeline_id)) {
                _dag.insert({downstream_pipeline_id, {}});
            }
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

            // 3.3 创建 Build Sink Operator（用于构建 Hash 表）
            DataSinkOperatorPtr sink;
            sink = std::make_shared<HashJoinBuildSinkOperatorX>(pool, next_sink_operator_id(),
                                                                op->operator_id(), tnode, descs);
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));

            // 3.4 记录 Pipeline 父子关系
            _pipeline_parent_map.push(op->node_id(), cur_pipe);
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
            sink->set_followed_by_shuffled_operator(sink->is_shuffled_operator());
            op->set_followed_by_shuffled_operator(op->is_shuffled_operator());
        }
        
        // 4. 如果是广播 Join 且启用共享 Hash 表，创建共享状态
        // 共享 Hash 表允许多个 Probe Operator 共享同一个 Build 端构建的 Hash 表，
        // 减少内存使用和提高执行效率
        if (is_broadcast_join && _runtime_state->enable_share_hash_table_for_broadcast_join()) {
            // 4.1 创建共享状态，管理多个实例之间的 Hash 表共享
            std::shared_ptr<HashJoinSharedState> shared_state =
                    HashJoinSharedState::create_shared(_num_instances);
            
            // 4.2 为每个实例创建 Build 依赖
            // Build 依赖确保 Probe 端在所有 Build 端完成之前不会开始执行
            for (int i = 0; i < _num_instances; i++) {
                auto sink_dep = std::make_shared<Dependency>(op->operator_id(), op->node_id(),
                                                             "HASH_JOIN_BUILD_DEPENDENCY");
                sink_dep->set_shared_state(shared_state.get());
                shared_state->sink_deps.push_back(sink_dep);
            }
            
            // 4.3 创建 Probe 端的依赖关系
            // 确保 Probe 端可以正确等待 Build 端完成
            shared_state->create_source_dependencies(_num_instances, op->operator_id(),
                                                     op->node_id(), "HASH_JOIN_PROBE");
            
            // 4.4 将共享状态注册到全局映射中
            _op_id_to_shared_state.insert(
                    {op->operator_id(), {shared_state, shared_state->sink_deps}});
        }
        
        // 5. 更新数据分布要求
        // 如果 Hash Join 需要特定的数据分布（如按 Join Key 分区），则标记需要 bucket 分布
        _require_bucket_distribution =
                _require_bucket_distribution || op->require_data_distribution();
        break;
    }
    case TPlanNodeType::CROSS_JOIN_NODE: {
        op = std::make_shared<NestedLoopJoinProbeOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(build_side_pipe->id());

        DataSinkOperatorPtr sink;
        sink = std::make_shared<NestedLoopJoinBuildSinkOperatorX>(pool, next_sink_operator_id(),
                                                                  op->operator_id(), tnode, descs);
        RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));
        _pipeline_parent_map.push(op->node_id(), cur_pipe);
        _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        break;
    }
    case TPlanNodeType::UNION_NODE: {
        int child_count = tnode.num_children;
        op = std::make_shared<UnionSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        op->set_followed_by_shuffled_operator(_require_bucket_distribution);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        for (int i = 0; i < child_count; i++) {
            PipelinePtr build_side_pipe = add_pipeline(cur_pipe);
            _dag[downstream_pipeline_id].push_back(build_side_pipe->id());
            DataSinkOperatorPtr sink;
            sink = std::make_shared<UnionSinkOperatorX>(i, next_sink_operator_id(),
                                                        op->operator_id(), pool, tnode, descs);
            sink->set_followed_by_shuffled_operator(_require_bucket_distribution);
            RETURN_IF_ERROR(build_side_pipe->set_sink(sink));
            RETURN_IF_ERROR(build_side_pipe->sink()->init(tnode, _runtime_state.get()));
            // preset children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
            _pipeline_parent_map.push(op->node_id(), build_side_pipe);
        }
        break;
    }
    case TPlanNodeType::SORT_NODE: {
        // 1）根据 Plan 决定这次排序是否需要 spill 以及是否启用本地 merge
        const auto should_spill = _runtime_state->enable_spill() &&
                                  tnode.sort_node.algorithm == TSortAlgorithm::FULL_SORT;
        const bool use_local_merge =
                tnode.sort_node.__isset.use_local_merge && tnode.sort_node.use_local_merge;

        // 2）在当前 pipeline 尾部添加一个“排序结果 Source” 算子
        //    - SpillSortSourceOperatorX：对应支持外排（磁盘溢写）的排序
        //    - LocalMergeSortSourceOperatorX：本地多路 merge sort
        //    - SortSourceOperatorX：普通全内存排序
        if (should_spill) {
            op = std::make_shared<SpillSortSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        } else if (use_local_merge) {
            op = std::make_shared<LocalMergeSortSourceOperatorX>(pool, tnode, next_operator_id(),
                                                                 descs);
        } else {
            op = std::make_shared<SortSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        }
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        // 3）记录当前 pipeline（包含 SortSource）的 id，并基于它创建一条新的 pipeline（包含 SortSink）
        //    新 pipeline 作为当前 pipeline 的依赖（必须先执行），负责排序工作
        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        // 4）在新建的 pipeline（数据流上游）上创建对应的 SortSink：
        //    - SpillSortSinkOperatorX：写入可溢写的 sort buffer
        //    - SortSinkOperatorX：普通 sort sink
        //    SortSink 会从数据流上游拉数据、做排序并将结果写入 shared_state，
        //    供前面创建的 SortSource（数据流下游）读取并输出给更下游的算子。
        DataSinkOperatorPtr sink;
        if (should_spill) {
            sink = std::make_shared<SpillSortSinkOperatorX>(pool, next_sink_operator_id(),
                                                            op->operator_id(), tnode, descs,
                                                            _require_bucket_distribution);
        } else {
            sink = std::make_shared<SortSinkOperatorX>(pool, next_sink_operator_id(),
                                                       op->operator_id(), tnode, descs,
                                                       _require_bucket_distribution);
        }
        // 标记排序 sink 的下游是否会再接 shuffle 类算子，影响部分分布要求
        sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
        _require_bucket_distribution =
                _require_bucket_distribution || sink->require_data_distribution();
        // 将新 pipeline 的 sink 设置为排序 sink，并用 TPlanNode 初始化之
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::PARTITION_SORT_NODE: {
        // 1）在当前 pipeline 的末尾添加一个 Source 算子：PartitionSortSourceOperatorX
        //    这条“当前 pipeline”后面不会再设置 sink，而是作为上游管道（child pipeline）存在，
        //    专门给后面构建的新 pipeline 的 PartitionSortSink 提供数据。
        op = std::make_shared<PartitionSortSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        // 2）记录当前（上游）pipeline 的 id，作为后面新建下游 pipeline 的“父节点”
        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }

        // 3）基于当前 pipeline 新建一条下游 pipeline（cur_pipe 重新指向新建的这条）
        //    新建的 pipeline 会被设置 sink = PartitionSortSinkOperatorX，构成完整的 sink pipeline。
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        // 4）在新建的下游 pipeline 上创建 PartitionSortSinkOperatorX：
        //    - 它的 child 就是上面那条（带 PartitionSortSource 的）上游 pipeline；
        //    - 它负责从 child 拉数据、按 PARTITION BY 分区并在每个分区内部排序/TopN；
        //    - 结果写入 shared_state，供 PartitionSortSource 再作为 Source 输出给更下游。
        DataSinkOperatorPtr sink;
        sink = std::make_shared<PartitionSortSinkOperatorX>(pool, next_sink_operator_id(),
                                                            op->operator_id(), tnode, descs);
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::ANALYTIC_EVAL_NODE: {
        op = std::make_shared<AnalyticSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

        const auto downstream_pipeline_id = cur_pipe->id();
        if (!_dag.contains(downstream_pipeline_id)) {
            _dag.insert({downstream_pipeline_id, {}});
        }
        cur_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(cur_pipe->id());

        DataSinkOperatorPtr sink;
        sink = std::make_shared<AnalyticSinkOperatorX>(pool, next_sink_operator_id(),
                                                       op->operator_id(), tnode, descs,
                                                       _require_bucket_distribution);
        sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
        _require_bucket_distribution =
                _require_bucket_distribution || sink->require_data_distribution();
        RETURN_IF_ERROR(cur_pipe->set_sink(sink));
        RETURN_IF_ERROR(cur_pipe->sink()->init(tnode, _runtime_state.get()));
        break;
    }
    case TPlanNodeType::MATERIALIZATION_NODE: {
        op = std::make_shared<MaterializationOperator>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::INTERSECT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<true>(
                pool, tnode, descs, op, cur_pipe, parent_idx, child_idx,
                followed_by_shuffled_operator));
        break;
    }
    case TPlanNodeType::EXCEPT_NODE: {
        RETURN_IF_ERROR(_build_operators_for_set_operation_node<false>(
                pool, tnode, descs, op, cur_pipe, parent_idx, child_idx,
                followed_by_shuffled_operator));
        break;
    }
    case TPlanNodeType::REPEAT_NODE: {
        op = std::make_shared<RepeatOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::TABLE_FUNCTION_NODE: {
        op = std::make_shared<TableFunctionOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE: {
        op = std::make_shared<AssertNumRowsOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::EMPTY_SET_NODE: {
        op = std::make_shared<EmptySetSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::DATA_GEN_SCAN_NODE: {
        op = std::make_shared<DataGenSourceOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        fe_with_old_version = !tnode.__isset.is_serial_operator;
        break;
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE: {
        op = std::make_shared<SchemaScanOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::META_SCAN_NODE: {
        op = std::make_shared<MetaScanOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    case TPlanNodeType::SELECT_NODE: {
        op = std::make_shared<SelectOperatorX>(pool, tnode, next_operator_id(), descs);
        RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipeline: {}",
                                     print_plan_node_type(tnode.node_type));
    }
    if (_params.__isset.parallel_instances && fe_with_old_version) {
        cur_pipe->set_num_tasks(_params.parallel_instances);
        op->set_serial_operator();
    }

    return Status::OK();
}
// NOLINTEND(readability-function-cognitive-complexity)
// NOLINTEND(readability-function-size)

template <bool is_intersect>
Status PipelineFragmentContext::_build_operators_for_set_operation_node(
        ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs, OperatorPtr& op,
        PipelinePtr& cur_pipe, int parent_idx, int child_idx, bool followed_by_shuffled_operator) {
    op.reset(new SetSourceOperatorX<is_intersect>(pool, tnode, next_operator_id(), descs));
    op->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
    RETURN_IF_ERROR(cur_pipe->add_operator(op, _parallel_instances));

    const auto downstream_pipeline_id = cur_pipe->id();
    if (!_dag.contains(downstream_pipeline_id)) {
        _dag.insert({downstream_pipeline_id, {}});
    }

    for (int child_id = 0; child_id < tnode.num_children; child_id++) {
        PipelinePtr probe_side_pipe = add_pipeline(cur_pipe);
        _dag[downstream_pipeline_id].push_back(probe_side_pipe->id());

        DataSinkOperatorPtr sink;
        if (child_id == 0) {
            sink.reset(new SetSinkOperatorX<is_intersect>(child_id, next_sink_operator_id(),
                                                          op->operator_id(), pool, tnode, descs));
        } else {
            sink.reset(new SetProbeSinkOperatorX<is_intersect>(
                    child_id, next_sink_operator_id(), op->operator_id(), pool, tnode, descs));
        }
        sink->set_followed_by_shuffled_operator(followed_by_shuffled_operator);
        RETURN_IF_ERROR(probe_side_pipe->set_sink(sink));
        RETURN_IF_ERROR(probe_side_pipe->sink()->init(tnode, _runtime_state.get()));
        // prepare children pipelines. if any pipeline found this as its father, will use the prepared pipeline to build.
        _pipeline_parent_map.push(op->node_id(), probe_side_pipe);
    }

    return Status::OK();
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }
    _submitted = true;

    int submit_tasks = 0;
    Status st;
    auto* scheduler = _query_ctx->get_pipe_exec_scheduler();
    for (auto& task : _tasks) {
        for (auto& t : task) {
            st = scheduler->submit(t.first);
            DBUG_EXECUTE_IF("PipelineFragmentContext.submit.failed",
                            { st = Status::Aborted("PipelineFragmentContext.submit.failed"); });
            if (!st) {
                cancel(Status::InternalError("submit context to executor fail"));
                std::lock_guard<std::mutex> l(_task_mutex);
                _total_tasks = submit_tasks;
                break;
            }
            submit_tasks++;
        }
    }
    if (!st.ok()) {
        std::lock_guard<std::mutex> l(_task_mutex);
        if (_closed_tasks >= _total_tasks) {
            _close_fragment_instance();
        }
        return Status::InternalError("Submit pipeline failed. err = {}, BE: {}", st.to_string(),
                                     BackendOptions::get_localhost());
    } else {
        return st;
    }
}

void PipelineFragmentContext::print_profile(const std::string& extra_info) {
    if (_runtime_state->enable_profile()) {
        std::stringstream ss;
        for (auto runtime_profile_ptr : _runtime_state->pipeline_id_to_profile()) {
            runtime_profile_ptr->pretty_print(&ss);
        }

        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        auto profile_str =
                fmt::format("Query {} fragment {} {}, profile, {}", print_id(this->_query_id),
                            this->_fragment_id, extra_info, ss.str());
        LOG_LONG_STRING(INFO, profile_str);
    }
}
// If all pipeline tasks binded to the fragment instance are finished, then we could
// close the fragment instance.
void PipelineFragmentContext::_close_fragment_instance() {
    if (_is_fragment_instance_closed) {
        return;
    }
    Defer defer_op {[&]() { _is_fragment_instance_closed = true; }};
    _fragment_level_profile->total_time_counter()->update(_fragment_watcher.elapsed_time());
    static_cast<void>(send_report(true));
    // Print profile content in info log is a tempoeray solution for stream load and external_connector.
    // Since stream load does not have someting like coordinator on FE, so
    // backend can not report profile to FE, ant its profile can not be shown
    // in the same way with other query. So we print the profile content to info log.

    if (_runtime_state->enable_profile() &&
        (_query_ctx->get_query_source() == QuerySource::STREAM_LOAD ||
         _query_ctx->get_query_source() == QuerySource::EXTERNAL_CONNECTOR ||
         _query_ctx->get_query_source() == QuerySource::GROUP_COMMIT_LOAD)) {
        std::stringstream ss;
        // Compute the _local_time_percent before pretty_print the runtime_profile
        // Before add this operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 00.00%)
        // After add the operation, the print out like that:
        // UNION_NODE (id=0):(Active: 56.720us, non-child: 82.53%)
        // We can easily know the exec node execute time without child time consumed.
        for (auto runtime_profile_ptr : _runtime_state->pipeline_id_to_profile()) {
            runtime_profile_ptr->pretty_print(&ss);
        }

        if (_runtime_state->load_channel_profile()) {
            _runtime_state->load_channel_profile()->pretty_print(&ss);
        }

        LOG_INFO("Query {} fragment {} profile:\n {}", print_id(_query_id), _fragment_id, ss.str());
    }

    if (_query_ctx->enable_profile()) {
        _query_ctx->add_fragment_profile(_fragment_id, collect_realtime_profile(),
                                         collect_realtime_load_channel_profile());
    }

    // all submitted tasks done
    _exec_env->fragment_mgr()->remove_pipeline_context({_query_id, _fragment_id});
}

void PipelineFragmentContext::decrement_running_task(PipelineId pipeline_id) {
    // If all tasks of this pipeline has been closed, upstream tasks is never needed, and we just make those runnable here
    DCHECK(_pip_id_to_pipeline.contains(pipeline_id));
    if (_pip_id_to_pipeline[pipeline_id]->close_task()) {
        if (_dag.contains(pipeline_id)) {
            for (auto dep : _dag[pipeline_id]) {
                _pip_id_to_pipeline[dep]->make_all_runnable(pipeline_id);
            }
        }
    }
    std::lock_guard<std::mutex> l(_task_mutex);
    ++_closed_tasks;
    if (_closed_tasks >= _total_tasks) {
        _close_fragment_instance();
    }
}

std::string PipelineFragmentContext::get_load_error_url() {
    if (const auto& str = _runtime_state->get_error_log_file_path(); !str.empty()) {
        return to_load_error_http_path(str);
    }
    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            if (const auto& str = task.second->get_error_log_file_path(); !str.empty()) {
                return to_load_error_http_path(str);
            }
        }
    }
    return "";
}

std::string PipelineFragmentContext::get_first_error_msg() {
    if (const auto& str = _runtime_state->get_first_error_msg(); !str.empty()) {
        return str;
    }
    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            if (const auto& str = task.second->get_first_error_msg(); !str.empty()) {
                return str;
            }
        }
    }
    return "";
}

Status PipelineFragmentContext::send_report(bool done) {
    Status exec_status = _query_ctx->exec_status();
    // If plan is done successfully, but _is_report_success is false,
    // no need to send report.
    // Load will set _is_report_success to true because load wants to know
    // the process.
    if (!_is_report_success && done && exec_status.ok()) {
        return Status::NeedSendAgain("");
    }

    // If both _is_report_success and _is_report_on_cancel are false,
    // which means no matter query is success or failed, no report is needed.
    // This may happen when the query limit reached and
    // a internal cancellation being processed
    // When limit is reached the fragment is also cancelled, but _is_report_on_cancel will
    // be set to false, to avoid sending fault report to FE.
    if (!_is_report_success && !_is_report_on_cancel) {
        return Status::NeedSendAgain("");
    }

    std::vector<RuntimeState*> runtime_states;

    for (auto& tasks : _tasks) {
        for (auto& task : tasks) {
            runtime_states.push_back(task.second.get());
        }
    }

    std::string load_eror_url = _query_ctx->get_load_error_url().empty()
                                        ? get_load_error_url()
                                        : _query_ctx->get_load_error_url();
    std::string first_error_msg = _query_ctx->get_first_error_msg().empty()
                                          ? get_first_error_msg()
                                          : _query_ctx->get_first_error_msg();

    ReportStatusRequest req {.status = exec_status,
                             .runtime_states = runtime_states,
                             .done = done || !exec_status.ok(),
                             .coord_addr = _query_ctx->coord_addr,
                             .query_id = _query_id,
                             .fragment_id = _fragment_id,
                             .fragment_instance_id = TUniqueId(),
                             .backend_num = -1,
                             .runtime_state = _runtime_state.get(),
                             .load_error_url = load_eror_url,
                             .first_error_msg = first_error_msg,
                             .cancel_fn = [this](const Status& reason) { cancel(reason); }};

    return _report_status_cb(
            req, std::dynamic_pointer_cast<PipelineFragmentContext>(shared_from_this()));
}

size_t PipelineFragmentContext::get_revocable_size(bool* has_running_task) const {
    size_t res = 0;
    // _tasks will be cleared during ~PipelineFragmentContext, so that it's safe
    // here to traverse the vector.
    for (const auto& task_instances : _tasks) {
        for (const auto& task : task_instances) {
            if (task.first->is_running()) {
                LOG_EVERY_N(INFO, 50) << "Query: " << print_id(_query_id)
                                      << " is running, task: " << (void*)task.first.get()
                                      << ", is_running: " << task.first->is_running();
                *has_running_task = true;
                return 0;
            }

            size_t revocable_size = task.first->get_revocable_size();
            if (revocable_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                res += revocable_size;
            }
        }
    }
    return res;
}

std::vector<PipelineTask*> PipelineFragmentContext::get_revocable_tasks() const {
    std::vector<PipelineTask*> revocable_tasks;
    for (const auto& task_instances : _tasks) {
        for (const auto& task : task_instances) {
            size_t revocable_size_ = task.first->get_revocable_size();
            if (revocable_size_ >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                revocable_tasks.emplace_back(task.first.get());
            }
        }
    }
    return revocable_tasks;
}

std::string PipelineFragmentContext::debug_string() {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "PipelineFragmentContext Info: _closed_tasks={}, _total_tasks={}\n",
                   _closed_tasks, _total_tasks);
    for (size_t j = 0; j < _tasks.size(); j++) {
        fmt::format_to(debug_string_buffer, "Tasks in instance {}:\n", j);
        for (size_t i = 0; i < _tasks[j].size(); i++) {
            fmt::format_to(debug_string_buffer, "Task {}: {}\n", i,
                           _tasks[j][i].first->debug_string());
        }
    }

    return fmt::to_string(debug_string_buffer);
}

std::vector<std::shared_ptr<TRuntimeProfileTree>>
PipelineFragmentContext::collect_realtime_profile() const {
    std::vector<std::shared_ptr<TRuntimeProfileTree>> res;

    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!_prepared) {
        std::string msg =
                "Query " + print_id(_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return res;
    }

    // Make sure first profile is fragment level profile
    auto fragment_profile = std::make_shared<TRuntimeProfileTree>();
    _fragment_level_profile->to_thrift(fragment_profile.get(), _runtime_state->profile_level());
    res.push_back(fragment_profile);

    // pipeline_id_to_profile is initialized in prepare stage
    for (auto pipeline_profile : _runtime_state->pipeline_id_to_profile()) {
        auto profile_ptr = std::make_shared<TRuntimeProfileTree>();
        pipeline_profile->to_thrift(profile_ptr.get(), _runtime_state->profile_level());
        res.push_back(profile_ptr);
    }

    return res;
}

std::shared_ptr<TRuntimeProfileTree>
PipelineFragmentContext::collect_realtime_load_channel_profile() const {
    // we do not have mutex to protect pipeline_id_to_profile
    // so we need to make sure this funciton is invoked after fragment context
    // has already been prepared.
    if (!_prepared) {
        std::string msg =
                "Query " + print_id(_query_id) + " collecting profile, but its not prepared";
        DCHECK(false) << msg;
        LOG_ERROR(msg);
        return nullptr;
    }

    for (const auto& tasks : _tasks) {
        for (const auto& task : tasks) {
            if (task.second->load_channel_profile() == nullptr) {
                continue;
            }

            auto tmp_load_channel_profile = std::make_shared<TRuntimeProfileTree>();

            task.second->load_channel_profile()->to_thrift(tmp_load_channel_profile.get(),
                                                           _runtime_state->profile_level());
            _runtime_state->load_channel_profile()->update(*tmp_load_channel_profile);
        }
    }

    auto load_channel_profile = std::make_shared<TRuntimeProfileTree>();
    _runtime_state->load_channel_profile()->to_thrift(load_channel_profile.get(),
                                                      _runtime_state->profile_level());
    return load_channel_profile;
}
#include "common/compile_check_end.h"
} // namespace doris::pipeline
