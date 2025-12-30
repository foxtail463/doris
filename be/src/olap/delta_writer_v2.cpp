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

#include "olap/delta_writer_v2.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <filesystem>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer_v2.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "service/backend_options.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "util/debug_points.h"
#include "util/mem_info.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "vec/core/block.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

DeltaWriterV2::DeltaWriterV2(WriteRequest* req,
                             const std::vector<std::shared_ptr<LoadStreamStub>>& streams,
                             RuntimeState* state)
        : _state(state),
          _req(*req),
          _tablet_schema(new TabletSchema),
          _memtable_writer(new MemTableWriter(*req)),
          _streams(streams) {}

void DeltaWriterV2::_update_profile(RuntimeProfile* profile) {
    auto child = profile->create_child(fmt::format("DeltaWriterV2 {}", _req.tablet_id), true, true);
    auto write_memtable_timer = ADD_TIMER(child, "WriteMemTableTime");
    auto wait_flush_limit_timer = ADD_TIMER(child, "WaitFlushLimitTime");
    auto close_wait_timer = ADD_TIMER(child, "CloseWaitTime");
    COUNTER_SET(write_memtable_timer, _write_memtable_time);
    COUNTER_SET(wait_flush_limit_timer, _wait_flush_limit_time);
    COUNTER_SET(close_wait_timer, _close_wait_time);
}

DeltaWriterV2::~DeltaWriterV2() {
    if (!_is_init) {
        return;
    }

    // cancel and wait all memtables in flush queue to be finished
    static_cast<void>(_memtable_writer->cancel());
}

Status DeltaWriterV2::init() {
    if (_is_init) {
        return Status::OK();
    }
    // build tablet schema in request level
    DBUG_EXECUTE_IF("DeltaWriterV2.init.stream_size", { _streams.clear(); });
    if (_streams.size() == 0 || _streams[0]->tablet_schema(_req.index_id) == nullptr) {
        return Status::InternalError("failed to find tablet schema for {}", _req.index_id);
    }
    RETURN_IF_ERROR(_build_current_tablet_schema(_req.index_id, _req.table_schema_param.get(),
                                                 *_streams[0]->tablet_schema(_req.index_id)));
    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.load_id = _req.load_id;
    context.index_id = _req.index_id;
    context.partition_id = _req.partition_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet = nullptr;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.tablet_id = _req.tablet_id;
    context.partition_id = _req.partition_id;
    context.tablet_schema_hash = _req.schema_hash;
    context.enable_unique_key_merge_on_write = _streams[0]->enable_unique_mow(_req.index_id);
    context.rowset_type = RowsetTypePB::BETA_ROWSET;
    context.rowset_id = ExecEnv::GetInstance()->storage_engine().next_rowset_id();
    context.data_dir = nullptr;
    context.partial_update_info = _partial_update_info;
    context.memtable_on_sink_support_index_v2 = true;
    context.encrypt_algorithm = EncryptionAlgorithmPB::PLAINTEXT;

    _rowset_writer = std::make_shared<BetaRowsetWriterV2>(_streams);
    RETURN_IF_ERROR(_rowset_writer->init(context));
    std::shared_ptr<WorkloadGroup> wg_sptr = nullptr;
    if (_state->get_query_ctx()) {
        wg_sptr = _state->get_query_ctx()->workload_group();
    }
    RETURN_IF_ERROR(_memtable_writer->init(_rowset_writer, _tablet_schema, _partial_update_info,
                                           wg_sptr, _streams[0]->enable_unique_mow(_req.index_id)));
    ExecEnv::GetInstance()->memtable_memory_limiter()->register_writer(_memtable_writer);
    _is_init = true;
    _streams.clear();
    return Status::OK();
}

Status DeltaWriterV2::write(const vectorized::Block* block, const DorisVector<uint32_t>& row_idxs) {
    // 快速路径检查：如果行索引为空，直接返回成功
    // 避免不必要的锁竞争和初始化开销
    if (UNLIKELY(row_idxs.empty())) {
        return Status::OK();
    }
    
    // 锁竞争监控：记录获取锁的耗时
    // 用于性能分析和锁竞争诊断
    _lock_watch.start();
    
    // 获取写锁：确保同一时间只有一个线程可以写入
    // 这是保证数据一致性的关键机制
    std::lock_guard<std::mutex> l(_lock);
    
    // 停止锁竞争计时：记录实际获取锁的耗时
    _lock_watch.stop();
    
    // 延迟初始化检查：如果DeltaWriter还未初始化且未被取消，则进行初始化
    // 这种设计避免了在构造函数中进行昂贵的初始化操作
    if (!_is_init && !_is_cancelled) {
        RETURN_IF_ERROR(init());
    }
    
    // 内存表刷新限制检查：等待刷新任务数量降到限制以下
    // 这是背压控制机制，防止内存表刷新任务过多导致系统过载
    {
        SCOPED_RAW_TIMER(&_wait_flush_limit_time);  // 记录等待刷新限制的耗时
        
        // 获取配置的刷新任务数量限制
        auto memtable_flush_running_count_limit = config::memtable_flush_running_count_limit;
        
        // 调试模式：模拟背压情况，强制等待10秒
        // 用于测试背压处理逻辑和超时机制
        DBUG_EXECUTE_IF("DeltaWriterV2.write.back_pressure",
                        { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
        
        // 等待循环：当运行中的刷新任务数量达到限制时，等待并重试
        // 每次等待10毫秒，避免过度消耗CPU资源
        while (_memtable_writer->flush_running_count() >= memtable_flush_running_count_limit) {
            // 检查操作是否被取消：如果被取消，立即返回取消原因
            if (_state->is_cancelled()) {
                return _state->cancel_reason();
            }
            
            // 短暂休眠：避免忙等待，减少CPU占用
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    // 实际写入阶段：记录写入内存表的耗时
    SCOPED_RAW_TIMER(&_write_memtable_time);
    
    // 调用内存表写入器的write方法，执行实际的数据写入操作
    // 传入数据块指针和行索引，将指定行写入到内存表中
    return _memtable_writer->write(block, row_idxs);
}

Status DeltaWriterV2::close() {
    _lock_watch.start();
    std::lock_guard<std::mutex> l(_lock);
    _lock_watch.stop();
    if (!_is_init && !_is_cancelled) {
        // if this delta writer is not initialized, but close() is called.
        // which means this tablet has no data loaded, but at least one tablet
        // in same partition has data loaded.
        // so we have to also init this DeltaWriterV2, so that it can create an empty rowset
        // for this tablet when being closed.
        RETURN_IF_ERROR(init());
    }
    return _memtable_writer->close();
}

Status DeltaWriterV2::close_wait(int32_t& num_segments, RuntimeProfile* profile) {
    SCOPED_RAW_TIMER(&_close_wait_time);
    std::lock_guard<std::mutex> l(_lock);
    DCHECK(_is_init)
            << "delta writer is supposed be to initialized before close_wait() being called";

    if (profile != nullptr) {
        _update_profile(profile);
    }
    RETURN_IF_ERROR(_memtable_writer->close_wait(profile));
    num_segments = _rowset_writer->next_segment_id();

    _delta_written_success = true;
    return Status::OK();
}

Status DeltaWriterV2::cancel() {
    return cancel_with_status(Status::Cancelled("already cancelled"));
}

Status DeltaWriterV2::cancel_with_status(const Status& st) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_memtable_writer->cancel_with_status(st));
    _is_cancelled = true;
    return Status::OK();
}

Status DeltaWriterV2::_build_current_tablet_schema(int64_t index_id,
                                                   const OlapTableSchemaParam* table_schema_param,
                                                   const TabletSchema& ori_tablet_schema) {
    _tablet_schema->copy_from(ori_tablet_schema);
    // find the right index id
    int i = 0;
    auto indexes = table_schema_param->indexes();
    for (; i < indexes.size(); i++) {
        if (indexes[i]->index_id == index_id) {
            break;
        }
    }

    if (!indexes.empty() && !indexes[i]->columns.empty() &&
        indexes[i]->columns[0]->unique_id() >= 0) {
        _tablet_schema->build_current_tablet_schema(
                index_id, static_cast<int32_t>(table_schema_param->version()), indexes[i],
                ori_tablet_schema);
    }

    _tablet_schema->set_table_id(table_schema_param->table_id());
    _tablet_schema->set_db_id(table_schema_param->db_id());
    if (table_schema_param->is_partial_update()) {
        _tablet_schema->set_auto_increment_column(table_schema_param->auto_increment_coulumn());
    }
    // set partial update columns info
    _partial_update_info = std::make_shared<PartialUpdateInfo>();
    RETURN_IF_ERROR(_partial_update_info->init(
            _req.tablet_id, _req.txn_id, *_tablet_schema,
            table_schema_param->unique_key_update_mode(),
            table_schema_param->partial_update_new_key_policy(),
            table_schema_param->partial_update_input_columns(),
            table_schema_param->is_strict_mode(), table_schema_param->timestamp_ms(),
            table_schema_param->nano_seconds(), table_schema_param->timezone(),
            table_schema_param->auto_increment_coulumn()));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
