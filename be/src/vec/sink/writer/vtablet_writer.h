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
#include <brpc/controller.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <bthread/mutex.h>

#include <atomic>
#include <chrono> // IWYU pragma: keep
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "util/brpc_closure.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TDataSink;
class TExpr;
class Thread;
class ThreadPoolToken;
class TupleDescriptor;

namespace vectorized {

// The counter of add_batch rpc of a single node
struct AddBatchCounter {
    // total execution time of a add_batch rpc
    int64_t add_batch_execution_time_us = 0;
    // lock waiting time in a add_batch rpc
    int64_t add_batch_wait_execution_time_us = 0;
    // number of add_batch call
    int64_t add_batch_num = 0;
    // time passed between marked close and finish close
    int64_t close_wait_time_ms = 0;

    AddBatchCounter& operator+=(const AddBatchCounter& rhs) {
        add_batch_execution_time_us += rhs.add_batch_execution_time_us;
        add_batch_wait_execution_time_us += rhs.add_batch_wait_execution_time_us;
        add_batch_num += rhs.add_batch_num;
        close_wait_time_ms += rhs.close_wait_time_ms;
        return *this;
    }
    friend AddBatchCounter operator+(const AddBatchCounter& lhs, const AddBatchCounter& rhs) {
        AddBatchCounter sum = lhs;
        sum += rhs;
        return sum;
    }
};

struct WriteBlockCallbackContext {
    std::atomic<bool> _is_last_rpc {false};
};

// It's very error-prone to guarantee the handler capture vars' & this closure's destruct sequence.
// So using create() to get the closure pointer is recommended. We can delete the closure ptr before the capture vars destruction.
// Delete this point is safe, don't worry about RPC callback will run after WriteBlockCallback deleted.
// "Ping-Pong" between sender and receiver, `try_set_in_flight` when send, `clear_in_flight` after rpc failure or callback,
// then next send will start, and it will wait for the rpc callback to complete when it is destroyed.
template <typename T>
class WriteBlockCallback final : public ::doris::DummyBrpcCallback<T> {
    ENABLE_FACTORY_CREATOR(WriteBlockCallback);

public:
    WriteBlockCallback() : cid(INVALID_BTHREAD_ID) {}
    ~WriteBlockCallback() override = default;

    void addFailedHandler(const std::function<void(const WriteBlockCallbackContext&)>& fn) {
        failed_handler = fn;
    }
    void addSuccessHandler(
            const std::function<void(const T&, const WriteBlockCallbackContext&)>& fn) {
        success_handler = fn;
    }

    void join() override {
        // We rely on in_flight to assure one rpc is running,
        // while cid is not reliable due to memory order.
        // in_flight is written before getting callid,
        // so we can not use memory fence to synchronize.
        while (_packet_in_flight) {
            // cid here is complicated
            if (cid != INVALID_BTHREAD_ID) {
                // actually cid may be the last rpc call id.
                brpc::Join(cid);
            }
            if (_packet_in_flight) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    // plz follow this order: reset() -> set_in_flight() -> send brpc batch
    void reset() {
        ::doris::DummyBrpcCallback<T>::cntl_->Reset();
        cid = ::doris::DummyBrpcCallback<T>::cntl_->call_id();
    }

    // if _packet_in_flight == false, set it to true. Return true.
    // if _packet_in_flight == true, Return false.
    bool try_set_in_flight() {
        bool value = false;
        return _packet_in_flight.compare_exchange_strong(value, true);
    }

    void clear_in_flight() { _packet_in_flight = false; }

    bool is_packet_in_flight() { return _packet_in_flight; }

    void end_mark() {
        DCHECK(_ctx._is_last_rpc == false);
        _ctx._is_last_rpc = true;
    }

    void call() override {
        DCHECK(_packet_in_flight);
        if (::doris::DummyBrpcCallback<T>::cntl_->Failed()) {
            LOG(WARNING) << "failed to send brpc batch, error="
                         << berror(::doris::DummyBrpcCallback<T>::cntl_->ErrorCode())
                         << ", error_text=" << ::doris::DummyBrpcCallback<T>::cntl_->ErrorText();
            failed_handler(_ctx);
        } else {
            success_handler(*(::doris::DummyBrpcCallback<T>::response_), _ctx);
        }
        clear_in_flight();
    }

private:
    brpc::CallId cid;
    std::atomic<bool> _packet_in_flight {false};
    WriteBlockCallbackContext _ctx;
    std::function<void(const WriteBlockCallbackContext&)> failed_handler;
    std::function<void(const T&, const WriteBlockCallbackContext&)> success_handler;
};

class IndexChannel;
class VTabletWriter;

class VNodeChannelStat {
public:
    VNodeChannelStat& operator+=(const VNodeChannelStat& stat) {
        mem_exceeded_block_ns += stat.mem_exceeded_block_ns;
        where_clause_ns += stat.where_clause_ns;
        append_node_channel_ns += stat.append_node_channel_ns;
        return *this;
    };

    int64_t mem_exceeded_block_ns = 0;
    int64_t where_clause_ns = 0;
    int64_t append_node_channel_ns = 0;
};

struct WriterStats {
    int64_t serialize_batch_ns = 0;
    int64_t queue_push_lock_ns = 0;
    int64_t actual_consume_ns = 0;
    int64_t total_add_batch_exec_time_ns = 0;
    int64_t max_add_batch_exec_time_ns = 0;
    int64_t total_wait_exec_time_ns = 0;
    int64_t max_wait_exec_time_ns = 0;
    int64_t total_add_batch_num = 0;
    int64_t num_node_channels = 0;
    int64_t load_back_pressure_version_time_ms = 0;
    VNodeChannelStat channel_stat;
};

// pair<row_id,tablet_id>
using Payload = std::pair<std::unique_ptr<vectorized::IColumn::Selector>, std::vector<int64_t>>;

// every NodeChannel keeps a data transmission channel with one BE. for multiple times open, it has a dozen of requests and corresponding closures.
class VNodeChannel {
public:
    VNodeChannel(VTabletWriter* parent, IndexChannel* index_channel, int64_t node_id,
                 bool is_incremental = false);

    ~VNodeChannel();

    // called before open, used to add tablet located in this backend. called by IndexChannel::init
    void add_tablet(const TTabletWithPartition& tablet) { _tablets_wait_open.emplace_back(tablet); }
    std::string debug_tablets() const {
        std::stringstream ss;
        for (const auto& tab : _all_tablets) {
            tab.printTo(ss);
            ss << '\n';
        }
        return ss.str();
    }

    void add_slave_tablet_nodes(int64_t tablet_id, const std::vector<int64_t>& slave_nodes) {
        _slave_tablet_nodes[tablet_id] = slave_nodes;
    }

    // this function is NON_REENTRANT
    Status init(RuntimeState* state);
    /// these two functions will call open_internal. should keep that clear --- REENTRANT
    // build corresponding connect to BE. NON-REENTRANT
    void open();
    // for auto partition, we use this to open more tablet. KEEP IT REENTRANT
    void incremental_open();
    // this will block until all request transmission which were opened or incremental opened finished.
    // this function will called multi times. NON_REENTRANT
    Status open_wait();

    Status add_block(vectorized::Block* block, const Payload* payload);

    // @return: 1 if running, 0 if finished.
    // @caller: VOlapTabletSink::_send_batch_process. it's a continual asynchronous process.
    int try_send_and_fetch_status(RuntimeState* state,
                                  std::unique_ptr<ThreadPoolToken>& thread_pool_token);
    // when there's pending block found by try_send_and_fetch_status(), we will awake a thread to send it.
    void try_send_pending_block(RuntimeState* state);

    void clear_all_blocks();

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    // hang_wait = true will make reciever hang until all sender mark_closed.
    void mark_close(bool hang_wait = false);

    bool is_closed() const { return _is_closed; }
    bool is_cancelled() const { return _cancelled; }
    std::string get_cancel_msg() {
        std::lock_guard<std::mutex> l(_cancel_msg_lock);
        if (!_cancel_msg.empty()) {
            return _cancel_msg;
        }
        return fmt::format("{} is cancelled", channel_info());
    }

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    Status close_wait(RuntimeState* state, bool* is_closed);

    Status after_close_handle(
            RuntimeState* state, WriterStats* writer_stats,
            std::unordered_map<int64_t, AddBatchCounter>* node_add_batch_counter_map);

    Status check_status();

    void cancel(const std::string& cancel_msg);

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map,
                     WriterStats* writer_stats) const {
        if (add_batch_counter_map != nullptr) {
            (*add_batch_counter_map)[_node_id] += _add_batch_counter;
            (*add_batch_counter_map)[_node_id].close_wait_time_ms = _close_time_ms;
        }
        if (writer_stats != nullptr) {
            writer_stats->serialize_batch_ns += _serialize_batch_ns;
            writer_stats->channel_stat += _stat;
            writer_stats->queue_push_lock_ns += _queue_push_lock_ns;
            writer_stats->actual_consume_ns += _actual_consume_ns;
            writer_stats->total_add_batch_exec_time_ns +=
                    (_add_batch_counter.add_batch_execution_time_us * 1000);
            writer_stats->total_wait_exec_time_ns +=
                    (_add_batch_counter.add_batch_wait_execution_time_us * 1000);
            writer_stats->total_add_batch_num += _add_batch_counter.add_batch_num;
            writer_stats->load_back_pressure_version_time_ms +=
                    _load_back_pressure_version_block_ms;
        }
    }

    int64_t node_id() const { return _node_id; }
    std::string host() const { return _node_info.host; }
    std::string name() const { return _name; }

    std::string channel_info() const {
        return fmt::format("{}, {}, node={}:{}", _name, _load_info, _node_info.host,
                           _node_info.brpc_port);
    }

    size_t get_pending_bytes() { return _pending_batches_bytes; }

    bool is_incremental() const { return _is_incremental; }

    int64_t write_bytes() const { return _write_bytes.load(); }

protected:
    // make a real open request for relative BE's load channel.
    void _open_internal(bool is_incremental);

    void _close_check();
    void _cancel_with_msg(const std::string& msg);

    void _add_block_success_callback(const PTabletWriterAddBlockResult& result,
                                     const WriteBlockCallbackContext& ctx);
    void _add_block_failed_callback(const WriteBlockCallbackContext& ctx);

    void _refresh_back_pressure_version_wait_time(
            const ::google::protobuf::RepeatedPtrField<::doris::PTabletLoadRowsetInfo>&
                    tablet_load_infos);

    VTabletWriter* _parent = nullptr;
    IndexChannel* _index_channel = nullptr;
    int64_t _node_id = -1;
    std::string _load_info;
    std::string _name;

    std::shared_ptr<MemTracker> _node_channel_tracker;
    int64_t _load_mem_limit = -1;

    TupleDescriptor* _tuple_desc = nullptr;
    NodeInfo _node_info;

    // this should be set in init() using config
    int _rpc_timeout_ms = 60000;
    int64_t _next_packet_seq = 0;
    MonotonicStopWatch _timeout_watch;

    // the timestamp when this node channel be marked closed and finished closed
    uint64_t _close_time_ms = 0;

    // user cancel or get some errors
    std::atomic<bool> _cancelled {false};
    std::mutex _cancel_msg_lock;
    std::string _cancel_msg;

    // send finished means the consumer thread which send the rpc can exit
    std::atomic<bool> _send_finished {false};

    // add batches finished means the last rpc has be response, used to check whether this channel can be closed
    std::atomic<bool> _add_batches_finished {false}; // reuse for vectorized

    bool _eos_is_produced {false}; // only for restricting producer behaviors

    std::unique_ptr<RowDescriptor> _row_desc;
    int _batch_size = 0;

    // limit _pending_batches size
    std::atomic<size_t> _pending_batches_bytes {0};
    size_t _max_pending_batches_bytes {(size_t)config::nodechannel_pending_queue_max_bytes};
    std::mutex _pending_batches_lock;          // reuse for vectorized
    std::atomic<int> _pending_batches_num {0}; // reuse for vectorized

    std::shared_ptr<PBackendService_Stub> _stub;
    // because we have incremantal open, we should keep one relative closure for one request. it's similarly for adding block.
    std::vector<std::shared_ptr<DummyBrpcCallback<PTabletWriterOpenResult>>> _open_callbacks;

    std::vector<TTabletWithPartition> _all_tablets;
    std::vector<TTabletWithPartition> _tablets_wait_open;
    // map from tablet_id to node_id where slave replicas locate in
    std::unordered_map<int64_t, std::vector<int64_t>> _slave_tablet_nodes;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    AddBatchCounter _add_batch_counter;
    std::atomic<int64_t> _serialize_batch_ns {0};
    std::atomic<int64_t> _queue_push_lock_ns {0};
    std::atomic<int64_t> _actual_consume_ns {0};
    std::atomic<int64_t> _load_back_pressure_version_block_ms {0};

    VNodeChannelStat _stat;
    // lock to protect _is_closed.
    // The methods in the IndexChannel are called back in the RpcClosure in the NodeChannel.
    // However, this rpc callback may occur after the whole task is finished (e.g. due to network latency),
    // and by that time the IndexChannel may have been destructured, so we should not call the
    // IndexChannel methods anymore, otherwise the BE will crash.
    // Therefore, we use the _is_closed and _closed_lock to ensure that the RPC callback
    // function will not call the IndexChannel method after the NodeChannel is closed.
    // The IndexChannel is definitely accessible until the NodeChannel is closed.
    std::mutex _closed_lock;
    bool _is_closed = false;
    bool _inited = false;

    RuntimeState* _state = nullptr;
    // A context lock for callbacks, the callback has to lock the ctx, to avoid
    // the object is deleted during callback is running.
    std::weak_ptr<TaskExecutionContext> _task_exec_ctx;
    // rows number received per tablet, tablet_id -> rows_num
    std::vector<std::pair<int64_t, int64_t>> _tablets_received_rows;
    // rows number filtered per tablet, tablet_id -> filtered_rows_num
    std::vector<std::pair<int64_t, int64_t>> _tablets_filtered_rows;

    // build a _cur_mutable_block and push into _pending_blocks. when not building, this block is empty.
    std::unique_ptr<vectorized::MutableBlock> _cur_mutable_block;
    std::shared_ptr<PTabletWriterAddBlockRequest> _cur_add_block_request;

    using AddBlockReq = std::pair<std::unique_ptr<vectorized::MutableBlock>,
                                  std::shared_ptr<PTabletWriterAddBlockRequest>>;
    std::queue<AddBlockReq> _pending_blocks;
    // send block to slave BE rely on this. dont reconstruct it.
    std::shared_ptr<WriteBlockCallback<PTabletWriterAddBlockResult>> _send_block_callback = nullptr;

    int64_t _wg_id = -1;

    bool _is_incremental;

    std::atomic<int64_t> _write_bytes {0};
    std::atomic<int64_t> _load_back_pressure_version_wait_time_ms {0};
};

// an IndexChannel is related to specific table and its rollup and mv
class IndexChannel {
public:
    IndexChannel(VTabletWriter* parent, int64_t index_id, vectorized::VExprContextSPtr where_clause)
            : _parent(parent), _index_id(index_id), _where_clause(std::move(where_clause)) {
        _index_channel_tracker =
                std::make_unique<MemTracker>("IndexChannel:indexID=" + std::to_string(_index_id));
    }
    ~IndexChannel() = default;

    // allow to init multi times, for incremental open more tablets for one index(table)
    Status init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets,
                bool incremental = false);

    void for_each_node_channel(
            const std::function<void(const std::shared_ptr<VNodeChannel>&)>& func) {
        for (auto& it : _node_channels) {
            func(it.second);
        }
    }

    void for_init_node_channel(
            const std::function<void(const std::shared_ptr<VNodeChannel>&)>& func) {
        for (auto& it : _node_channels) {
            if (!it.second->is_incremental()) {
                func(it.second);
            }
        }
    }

    void for_inc_node_channel(
            const std::function<void(const std::shared_ptr<VNodeChannel>&)>& func) {
        for (auto& it : _node_channels) {
            if (it.second->is_incremental()) {
                func(it.second);
            }
        }
    }

    std::unordered_set<int64_t> init_node_channel_ids() {
        std::unordered_set<int64_t> node_channel_ids;
        for (auto& it : _node_channels) {
            if (!it.second->is_incremental()) {
                node_channel_ids.insert(it.first);
            }
        }
        return node_channel_ids;
    }

    std::unordered_set<int64_t> inc_node_channel_ids() {
        std::unordered_set<int64_t> node_channel_ids;
        for (auto& it : _node_channels) {
            if (it.second->is_incremental()) {
                node_channel_ids.insert(it.first);
            }
        }
        return node_channel_ids;
    }

    std::unordered_set<int64_t> each_node_channel_ids() {
        std::unordered_set<int64_t> node_channel_ids;
        for (auto& it : _node_channels) {
            node_channel_ids.insert(it.first);
        }
        return node_channel_ids;
    }

    bool has_incremental_node_channel() const { return _has_inc_node; }

    void mark_as_failed(const VNodeChannel* node_channel, const std::string& err,
                        int64_t tablet_id = -1);
    Status check_intolerable_failure();

    Status close_wait(RuntimeState* state, WriterStats* writer_stats,
                      std::unordered_map<int64_t, AddBatchCounter>* node_add_batch_counter_map,
                      std::unordered_set<int64_t> unfinished_node_channel_ids,
                      bool need_wait_after_quorum_success);

    Status check_each_node_channel_close(
            std::unordered_set<int64_t>* unfinished_node_channel_ids,
            std::unordered_map<int64_t, AddBatchCounter>* node_add_batch_counter_map,
            WriterStats* writer_stats, Status status);

    // set error tablet info in runtime state, so that it can be returned to FE.
    void set_error_tablet_in_state(RuntimeState* state);

    size_t num_node_channels() const { return _node_channels.size(); }

    size_t get_pending_bytes() const {
        size_t mem_consumption = 0;
        for (const auto& kv : _node_channels) {
            mem_consumption += kv.second->get_pending_bytes();
        }
        return mem_consumption;
    }

    void set_tablets_received_rows(
            const std::vector<std::pair<int64_t, int64_t>>& tablets_received_rows, int64_t node_id);

    void set_tablets_filtered_rows(
            const std::vector<std::pair<int64_t, int64_t>>& tablets_filtered_rows, int64_t node_id);

    int64_t num_rows_filtered() {
        // the Unique table has no roll up or materilized view
        // we just add up filtered rows from all partitions
        return std::accumulate(_tablets_filtered_rows.cbegin(), _tablets_filtered_rows.cend(), 0,
                               [](int64_t sum, const auto& a) { return sum + a.second[0].second; });
    }

    // check whether the rows num written by different replicas is consistent
    Status check_tablet_received_rows_consistency();

    // check whether the rows num filtered by different replicas is consistent
    Status check_tablet_filtered_rows_consistency();

    void set_start_time(const int64_t& start_time) { _start_time = start_time; }

    vectorized::VExprContextSPtr get_where_clause() { return _where_clause; }

private:
    friend class VNodeChannel;
    friend class VTabletWriter;
    friend class VRowDistribution;

    int _max_failed_replicas(int64_t tablet_id);

    int _load_required_replicas_num(int64_t tablet_id);

    bool _quorum_success(const std::unordered_set<int64_t>& unfinished_node_channel_ids,
                         const std::unordered_set<int64_t>& need_finish_tablets);

    int64_t _calc_max_wait_time_ms(const std::unordered_set<int64_t>& unfinished_node_channel_ids);

    VTabletWriter* _parent = nullptr;
    int64_t _index_id;
    vectorized::VExprContextSPtr _where_clause;

    // from backend channel to tablet_id
    // ATTN: must be placed before `_node_channels` and `_channels_by_tablet`.
    // Because the destruct order of objects is opposite to the creation order.
    // So NodeChannel will be destructured first.
    // And the destructor function of NodeChannel waits for all RPCs to finish.
    // This ensures that it is safe to use `_tablets_by_channel` in the callback function for the end of the RPC.
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _tablets_by_channel;
    // BeId -> channel
    std::unordered_map<int64_t, std::shared_ptr<VNodeChannel>> _node_channels;
    // from tablet_id to backend channel
    std::unordered_map<int64_t, std::vector<std::shared_ptr<VNodeChannel>>> _channels_by_tablet;
    bool _has_inc_node = false;

    // lock to protect _failed_channels and _failed_channels_msgs
    mutable std::mutex _fail_lock;
    // key is tablet_id, value is a set of failed node id
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _failed_channels;
    // key is tablet_id, value is error message
    std::unordered_map<int64_t, std::string> _failed_channels_msgs;
    Status _intolerable_failure_status = Status::OK();

    std::unique_ptr<MemTracker> _index_channel_tracker;
    // rows num received by DeltaWriter per tablet, tablet_id -> <node_Id, rows_num>
    // used to verify whether the rows num received by different replicas is consistent
    std::map<int64_t, std::vector<std::pair<int64_t, int64_t>>> _tablets_received_rows;

    // rows num filtered by DeltaWriter per tablet, tablet_id -> <node_Id, filtered_rows_num>
    // used to verify whether the rows num filtered by different replicas is consistent
    std::map<int64_t, std::vector<std::pair<int64_t, int64_t>>> _tablets_filtered_rows;

    int64_t _start_time = 0;
};
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
/**
 * VTabletWriter：负责将数据写入 OLAP 表的异步写入器
 * 
 * 核心功能：
 * 1. 接收上游算子的 Block，进行数据分区和路由
 * 2. 将数据分发到各个 BE 节点的 NodeChannel
 * 3. 通过 RPC 将数据发送到目标 BE，最终写入 MemTable
 * 4. 支持多副本写入、自动分区创建、流控等特性
 * 
 * 架构：
 * - VTabletWriter（顶层写入器）
 *   - IndexChannel（每个表/索引对应一个，包含多个 NodeChannel）
 *     - VNodeChannel（每个 BE 节点对应一个，负责 RPC 通信）
 *       - DeltaWriter（目标 BE 上的写入器，写入 MemTable）
 */
class VTabletWriter final : public AsyncResultWriter {
public:
    /**
     * 构造函数
     * @param t_sink FE 下发的数据写入配置
     * @param output_exprs 输出表达式上下文列表（用于列转换）
     * @param dep 异步写入依赖（用于流控）
     * @param fin_dep 完成依赖（用于等待写入完成）
     */
    VTabletWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                  std::shared_ptr<pipeline::Dependency> dep,
                  std::shared_ptr<pipeline::Dependency> fin_dep);

    /**
     * 写入一个 Block 到目标表
     * 主要流程：
     * 1. 数据分区和路由（根据分区键计算每个行属于哪个 tablet）
     * 2. 数据转换（Block 格式转换、列映射、过滤等）
     * 3. 将数据分发到对应的 NodeChannel（按 tablet 分组）
     * 4. NodeChannel 异步发送数据到目标 BE
     * 
     * @param state 运行时状态
     * @param block 待写入的数据块
     * @return 写入状态
     */
    Status write(RuntimeState* state, Block& block) override;

    /**
     * 关闭写入器，等待所有数据写入完成
     * 主要流程：
     * 1. 标记所有 NodeChannel 为关闭状态
     * 2. 等待所有 RPC 完成
     * 3. 收集写入结果和统计信息
     * 
     * @param exec_status 执行状态（如果出错，会取消写入）
     * @return 关闭状态
     */
    Status close(Status) override;

    /**
     * 初始化写入器
     * 主要流程：
     * 1. 解析表结构、分区信息、副本信息等
     * 2. 创建 IndexChannel 和 NodeChannel
     * 3. 初始化行分布器（用于数据路由）
     * 4. 启动后台发送线程（_send_batch_process）
     * 
     * @param state 运行时状态
     * @param profile 性能分析器
     * @return 初始化状态
     */
    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    /**
     * 后台发送线程的主循环函数
     * 
     * 功能：
     * - 轮询所有 NodeChannel，检查是否有待发送的数据块
     * - 调用 NodeChannel::try_send_and_fetch_status() 非阻塞发送
     * - 只关注待发送批次和通道状态，NodeChannel 的内部错误由生产者处理
     * 
     * 实现方式：
     * - 使用轮询 + NodeChannel::try_send_and_fetch_status() 实现非阻塞发送
     * - 避免阻塞主线程，提高并发性能
     */
    void _send_batch_process();

    /**
     * 处理自动分区创建的结果
     * 当检测到新分区需要创建时，会调用 FE 创建分区，然后通过此函数处理结果
     * 
     * @param result 分区创建结果
     * @return 处理状态
     */
    Status on_partitions_created(TCreatePartitionResult* result);

    /**
     * 发送新创建分区的数据批次
     * 在分区创建成功后，将之前缓存的数据发送到新分区
     * 
     * @return 发送状态
     */
    Status _send_new_partition_batch();

private:
    friend class VNodeChannel;
    friend class IndexChannel;

    // 通道分发负载：将数据按 NodeChannel 分组，每个 NodeChannel 对应一个 Payload（行选择器和 tablet_id 列表）
    using ChannelDistributionPayload = std::unordered_map<VNodeChannel*, Payload>;
    // 多个索引通道的分发负载（用于支持物化视图等）
    using ChannelDistributionPayloadVec = std::vector<std::unordered_map<VNodeChannel*, Payload>>;

    /**
     * 初始化行分布器（VRowDistribution）
     * 行分布器负责根据分区键计算每行数据属于哪个分区和 tablet
     */
    Status _init_row_distribution();

    /**
     * 内部初始化函数，由 open() 调用
     * 负责解析配置、创建通道、初始化各种组件
     */
    Status _init(RuntimeState* state, RuntimeProfile* profile);

    /**
     * 为单个索引通道生成分发负载
     * 将行分区 tablet 信息转换为按 NodeChannel 分组的数据负载
     * 
     * @param row_part_tablet_tuple 行对应的分区和 tablet 信息
     * @param index_idx 索引索引（0 为主表，>0 为物化视图）
     * @param channel_payload 输出的通道负载（按 NodeChannel 分组）
     */
    void _generate_one_index_channel_payload(RowPartTabletIds& row_part_tablet_tuple,
                                             int32_t index_idx,
                                             ChannelDistributionPayload& channel_payload);

    /**
     * 为所有索引通道生成分发负载
     * 批量处理多行数据，生成每个索引通道对应的负载
     * 
     * @param row_part_tablet_ids 多行数据的分区 tablet 信息
     * @param payload 输出的负载向量（每个元素对应一个索引通道）
     */
    void _generate_index_channels_payloads(std::vector<RowPartTabletIds>& row_part_tablet_ids,
                                           ChannelDistributionPayloadVec& payload);

    /**
     * 取消所有通道的写入操作
     * 当发生错误时，通知所有 NodeChannel 停止写入
     * 
     * @param status 取消原因
     */
    void _cancel_all_channel(Status status);

    /**
     * 增量打开 NodeChannel（用于自动分区场景）
     * 当检测到新分区时，需要为新分区创建对应的 NodeChannel 并打开连接
     * 
     * @param partitions 新创建的分区列表
     * @return 打开状态
     */
    Status _incremental_open_node_channel(const std::vector<TOlapTablePartition>& partitions);

    /**
     * 尝试关闭写入器（内部实现）
     * 标记所有通道为关闭状态，但不等待完成
     * 
     * @param state 运行时状态
     * @param exec_status 执行状态
     */
    void _do_try_close(RuntimeState* state, const Status& exec_status);

    /**
     * 构建 tablet 的副本信息
     * 记录每个 tablet 的总副本数和写入所需的副本数（用于 quorum 判断）
     * 
     * @param tablet_id tablet ID
     * @param partition 分区信息
     */
    void _build_tablet_replica_info(const int64_t tablet_id, VOlapTablePartition* partition);

    // FE 下发的数据写入配置（包含表信息、分区信息、副本信息等）
    TDataSink _t_sink;

    // 内存跟踪器，用于监控写入过程中的内存使用
    std::shared_ptr<MemTracker> _mem_tracker;

    // 对象池，用于管理临时对象的生命周期
    ObjectPool* _pool = nullptr;

    // 后台发送线程 ID（用于发送待发送的数据批次）
    bthread_t _sender_thread = 0;

    // 本次导入的唯一标识（由 FE 生成，用于标识一次导入任务）
    PUniqueId _load_id;
    // 事务 ID（用于 2PC 事务提交）
    int64_t _txn_id = -1;
    // 副本数量（每个 tablet 的副本数）
    int _num_replicas = -1;
    // 输出元组的描述符 ID
    int _tuple_desc_id = -1;

    // 目标 OLAP 表的元组描述符（定义表的结构）
    TupleDescriptor* _output_tuple_desc = nullptr;
    // 输出行描述符（用于行格式转换）
    RowDescriptor* _output_row_desc = nullptr;

    // 发送者 ID（用于多发送者场景，每个发送者负责一部分数据）
    // 如果只支持单节点插入，所有数据需要先收集再发送
    // 为了支持多发送者，为每个发送者维护一个通道
    int _sender_id = -1;
    // 发送者总数
    int _num_senders = -1;
    // 是否为高优先级任务
    bool _is_high_priority = false;

    // 表结构参数（包含列信息、索引信息等）
    // TODO(zc): think about cache this data
    std::shared_ptr<OlapTableSchemaParam> _schema;
    // 主副本位置参数（用于路由数据到正确的 BE）
    OlapTableLocationParam* _location = nullptr;
    // 是否只写入单个副本（性能优化选项）
    bool _write_single_replica = false;
    // 从副本位置参数（用于单副本写入时的备份）
    OlapTableLocationParam* _slave_location = nullptr;
    // BE 节点信息（包含所有 BE 的地址、端口等）
    DorisNodesInfo* _nodes_info = nullptr;

    // Tablet 查找器（根据分区键快速定位 tablet）
    std::unique_ptr<OlapTabletFinder> _tablet_finder;

    // 索引通道相关
    // 保护通道操作的互斥锁（防止并发修改通道状态）
    bthread::Mutex _stop_check_channel;
    // 所有索引通道列表（每个表/物化视图对应一个 IndexChannel）
    std::vector<std::shared_ptr<IndexChannel>> _channels;
    // 索引 ID 到通道的映射（快速查找）
    std::unordered_map<int64_t, std::shared_ptr<IndexChannel>> _index_id_to_channel;

    // 发送批次的线程池令牌（用于控制并发发送）
    std::unique_ptr<ThreadPoolToken> _send_batch_thread_pool_token;

    // 需要创建的分区列表（用于自动分区场景）
    // 目前只支持单个分区列
    std::vector<std::vector<TStringLiteral>> _partitions_need_create;

    // Block 转换器（负责 Block 格式转换、列映射、数据验证等）
    std::unique_ptr<OlapTableBlockConvertor> _block_convertor;
    // 统计信息
    int64_t _send_data_ns = 0;              // 发送数据耗时（纳秒）
    int64_t _number_input_rows = 0;        // 输入行数
    int64_t _number_output_rows = 0;        // 输出行数（过滤后）
    int64_t _filter_ns = 0;                 // 过滤耗时（纳秒）

    // 行分布计时器（用于性能分析）
    MonotonicStopWatch _row_distribution_watch;

    // 性能计数器（RuntimeProfile）
    RuntimeProfile::Counter* _input_rows_counter = nullptr;                    // 输入行数计数器
    RuntimeProfile::Counter* _output_rows_counter = nullptr;                    // 输出行数计数器
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;                  // 过滤行数计数器
    RuntimeProfile::Counter* _send_data_timer = nullptr;                        // 发送数据耗时
    RuntimeProfile::Counter* _row_distribution_timer = nullptr;                 // 行分布耗时
    RuntimeProfile::Counter* _append_node_channel_timer = nullptr;              // 追加到 NodeChannel 耗时
    RuntimeProfile::Counter* _filter_timer = nullptr;                           // 过滤耗时
    RuntimeProfile::Counter* _where_clause_timer = nullptr;                     // WHERE 子句过滤耗时
    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;            // 添加分区请求耗时
    RuntimeProfile::Counter* _wait_mem_limit_timer = nullptr;                   // 等待内存限制耗时
    RuntimeProfile::Counter* _validate_data_timer = nullptr;                    // 数据验证耗时
    RuntimeProfile::Counter* _open_timer = nullptr;                              // 打开耗时
    RuntimeProfile::Counter* _close_timer = nullptr;                             // 关闭耗时
    RuntimeProfile::Counter* _non_blocking_send_timer = nullptr;                 // 非阻塞发送总耗时
    RuntimeProfile::Counter* _non_blocking_send_work_timer = nullptr;            // 非阻塞发送实际工作耗时
    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;                  // 序列化批次耗时
    RuntimeProfile::Counter* _total_add_batch_exec_timer = nullptr;              // 总 add_batch 执行耗时
    RuntimeProfile::Counter* _max_add_batch_exec_timer = nullptr;                // 最大 add_batch 执行耗时
    RuntimeProfile::Counter* _total_wait_exec_timer = nullptr;                   // 总等待执行耗时
    RuntimeProfile::Counter* _max_wait_exec_timer = nullptr;                     // 最大等待执行耗时
    RuntimeProfile::Counter* _add_batch_number = nullptr;                        // add_batch 调用次数
    RuntimeProfile::Counter* _num_node_channels = nullptr;                       // NodeChannel 数量
    RuntimeProfile::Counter* _load_back_pressure_version_time_ms = nullptr;      // 负载背压版本等待时间

    // 本次 tablet sink 打开的 load channel 的超时时间（秒）
    int64_t _load_channel_timeout_s = 0;
    // 导入事务的绝对过期时间（时间戳）
    int64_t _txn_expiration = 0;

    // 发送批次的并行度（控制同时发送的批次数量）
    int32_t _send_batch_parallelism = 1;
    // 保存 try_close() 和 close() 方法的状态
    Status _close_status;
    // 如果调用了 try_close()，对于自动分区场景，周期性发送线程应该停止等待首次打开的 node channels
    bool _try_close = false;
    // 对于非 pipeline 模式，如果 close() 做了某些操作，close_wait() 应该等待它完成
    bool _close_wait = false;
    // 是否已初始化
    bool _inited = false;
    // 是否写入文件缓存
    bool _write_file_cache = false;

    // 是否通过 brpc 传输大数据（用户可以在运行时修改此配置，避免在查询或加载过程中被修改）
    bool _transfer_large_data_by_brpc = false;

    // OLAP 表分区参数（包含分区信息、分区键等）
    VOlapTablePartitionParam* _vpartition = nullptr;

    // 运行时状态（不拥有所有权，在 open 时设置）
    RuntimeState* _state = nullptr; // not owned, set when open

    // 行分布器（根据分区键计算每行数据属于哪个分区和 tablet）
    VRowDistribution _row_distribution;
    // 复用向量，避免频繁的内存分配和释放
    // 存储每行数据对应的分区和 tablet ID 信息
    std::vector<RowPartTabletIds> _row_part_tablet_ids;

    // Tablet 副本信息映射：tablet_id -> <总副本数, 写入所需副本数>
    // 用于判断是否达到 quorum，决定是否可以提交事务
    std::unordered_map<int64_t, std::pair<int, int>> _tablet_replica_info;
};
} // namespace doris::vectorized
