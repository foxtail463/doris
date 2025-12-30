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

#ifdef __APPLE__
#include <netinet/in.h>
#include <sys/_types/_u_int.h>
#endif

#include <concurrentqueue.h>
#include <sqltypes.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/internal_service.pb.h"
#include "pipeline/common/agg_utils.h"
#include "pipeline/common/join_utils.h"
#include "pipeline/common/set_utils.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/join/process_hash_table_probe.h"
#include "util/brpc_closure.h"
#include "util/stack_util.h"
#include "vec/common/sort/partition_sorter.h"
#include "vec/common/sort/sorter.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/spill/spill_stream.h"

namespace doris::vectorized {
class AggFnEvaluator;
class VSlotRef;
} // namespace doris::vectorized

namespace doris::pipeline {
#include "common/compile_check_begin.h"
class Dependency;
class PipelineTask;
struct BasicSharedState;
using DependencySPtr = std::shared_ptr<Dependency>;
class LocalExchangeSourceLocalState;

static constexpr auto SLOW_DEPENDENCY_THRESHOLD = 60 * 1000L * 1000L * 1000L;
static constexpr auto TIME_UNIT_DEPENDENCY_LOG = 30 * 1000L * 1000L * 1000L;
static_assert(TIME_UNIT_DEPENDENCY_LOG < SLOW_DEPENDENCY_THRESHOLD);

struct BasicSharedState {
    ENABLE_FACTORY_CREATOR(BasicSharedState)

    template <class TARGET>
    TARGET* cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET*>(this);
    }
    template <class TARGET>
    const TARGET* cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET*>(this);
    }
    std::vector<DependencySPtr> source_deps;
    std::vector<DependencySPtr> sink_deps;
    int id = 0;
    std::set<int> related_op_ids;

    virtual ~BasicSharedState() = default;

    void create_source_dependencies(int num_sources, int operator_id, int node_id,
                                    const std::string& name);
    Dependency* create_source_dependency(int operator_id, int node_id, const std::string& name);

    Dependency* create_sink_dependency(int dest_id, int node_id, const std::string& name);
    std::vector<DependencySPtr> get_dep_by_channel_id(int channel_id) {
        DCHECK_LT(channel_id, source_deps.size());
        return {source_deps[channel_id]};
    }
};

class Dependency : public std::enable_shared_from_this<Dependency> {
public:
    ENABLE_FACTORY_CREATOR(Dependency);
    Dependency(int id, int node_id, std::string name, bool ready = false)
            : _id(id), _node_id(node_id), _name(std::move(name)), _ready(ready) {}
    virtual ~Dependency() = default;

    [[nodiscard]] int id() const { return _id; }
    [[nodiscard]] virtual std::string name() const { return _name; }
    BasicSharedState* shared_state() { return _shared_state; }
    void set_shared_state(BasicSharedState* shared_state) { _shared_state = shared_state; }
    virtual std::string debug_string(int indentation_level = 0);
    bool ready() const { return _ready; }

    // Start the watcher. We use it to count how long this dependency block the current pipeline task.
    void start_watcher() { _watcher.start(); }
    [[nodiscard]] int64_t watcher_elapse_time() { return _watcher.elapsed_time(); }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] Dependency* is_blocked_by(std::shared_ptr<PipelineTask> task = nullptr);
    // Notify downstream pipeline tasks this dependency is ready.
    void set_ready();
    void set_ready_to_read(int channel_id = 0) {
        DCHECK_LT(channel_id, _shared_state->source_deps.size()) << debug_string();
        _shared_state->source_deps[channel_id]->set_ready();
    }
    void set_ready_to_write() {
        DCHECK_EQ(_shared_state->sink_deps.size(), 1) << debug_string();
        _shared_state->sink_deps.front()->set_ready();
    }

    // Notify downstream pipeline tasks this dependency is blocked.
    void block() {
        if (_always_ready) {
            return;
        }
        std::unique_lock<std::mutex> lc(_always_ready_lock);
        if (_always_ready) {
            return;
        }
        _ready = false;
    }

    void set_always_ready() {
        if (_always_ready) {
            return;
        }
        std::unique_lock<std::mutex> lc(_always_ready_lock);
        if (_always_ready) {
            return;
        }
        _always_ready = true;
        set_ready();
    }

protected:
    void _add_block_task(std::shared_ptr<PipelineTask> task);

    const int _id;
    const int _node_id;
    const std::string _name;
    std::atomic<bool> _ready;

    BasicSharedState* _shared_state = nullptr;
    MonotonicStopWatch _watcher;

    std::mutex _task_lock;
    std::vector<std::weak_ptr<PipelineTask>> _blocked_task;

    // If `_always_ready` is true, `block()` will never block tasks.
    std::atomic<bool> _always_ready = false;
    std::mutex _always_ready_lock;
};

struct FakeSharedState final : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(FakeSharedState)
};

class CountedFinishDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    CountedFinishDependency(int id, int node_id, std::string name)
            : Dependency(id, node_id, std::move(name), true) {}

    void add(uint32_t count = 1) {
        std::unique_lock<std::mutex> l(_mtx);
        if (!_counter) {
            block();
        }
        _counter += count;
    }

    void sub() {
        std::unique_lock<std::mutex> l(_mtx);
        _counter--;
        if (!_counter) {
            set_ready();
        }
    }

    std::string debug_string(int indentation_level = 0) override;

private:
    std::mutex _mtx;
    uint32_t _counter = 0;
};

struct RuntimeFilterTimerQueue;
class RuntimeFilterTimer {
public:
    RuntimeFilterTimer(int64_t registration_time, int32_t wait_time_ms,
                       std::shared_ptr<Dependency> parent, bool force_wait_timeout = false)
            : _parent(std::move(parent)),
              _registration_time(registration_time),
              _wait_time_ms(wait_time_ms),
              _force_wait_timeout(force_wait_timeout) {}

    // Called by runtime filter producer.
    void call_ready();

    // Called by RuntimeFilterTimerQueue which is responsible for checking if this rf is timeout.
    void call_timeout();

    int64_t registration_time() const { return _registration_time; }
    int32_t wait_time_ms() const { return _wait_time_ms; }

    void set_local_runtime_filter_dependencies(
            const std::vector<std::shared_ptr<Dependency>>& deps) {
        _local_runtime_filter_dependencies = deps;
    }

    bool should_be_check_timeout();

    bool force_wait_timeout() { return _force_wait_timeout; }

private:
    friend struct RuntimeFilterTimerQueue;
    std::shared_ptr<Dependency> _parent = nullptr;
    std::vector<std::shared_ptr<Dependency>> _local_runtime_filter_dependencies;
    std::mutex _lock;
    int64_t _registration_time;
    const int32_t _wait_time_ms;
    // true only for group_commit_scan_operator
    bool _force_wait_timeout;
};

struct RuntimeFilterTimerQueue {
    constexpr static int64_t interval = 10;
    void run() { _thread.detach(); }
    void start();

    void stop() {
        _stop = true;
        cv.notify_all();
        wait_for_shutdown();
    }

    void wait_for_shutdown() const {
        while (!_shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }

    ~RuntimeFilterTimerQueue() = default;
    RuntimeFilterTimerQueue() { _thread = std::thread(&RuntimeFilterTimerQueue::start, this); }
    void push_filter_timer(std::vector<std::shared_ptr<pipeline::RuntimeFilterTimer>>&& filter) {
        std::unique_lock<std::mutex> lc(_que_lock);
        _que.insert(_que.end(), filter.begin(), filter.end());
        cv.notify_all();
    }

    std::thread _thread;
    std::condition_variable cv;
    std::mutex cv_m;
    std::mutex _que_lock;
    std::atomic_bool _stop = false;
    std::atomic_bool _shutdown = false;
    std::list<std::shared_ptr<pipeline::RuntimeFilterTimer>> _que;
};

struct AggSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(AggSharedState)
public:
    AggSharedState() { agg_data = std::make_unique<AggregatedDataVariants>(); }
    ~AggSharedState() override {
        if (!probe_expr_ctxs.empty()) {
            _close_with_serialized_key();
        } else {
            _close_without_key();
        }
    }

    Status reset_hash_table();

    bool do_limit_filter(vectorized::Block* block, size_t num_rows,
                         const std::vector<int>* key_locs = nullptr);
    void build_limit_heap(size_t hash_table_size);

    // We should call this function only at 1st phase.
    // 1st phase: is_merge=true, only have one SlotRef.
    // 2nd phase: is_merge=false, maybe have multiple exprs.
    static int get_slot_column_id(const vectorized::AggFnEvaluator* evaluator);

    AggregatedDataVariantsUPtr agg_data = nullptr;
    std::unique_ptr<AggregateDataContainer> aggregate_data_container;
    std::vector<vectorized::AggFnEvaluator*> aggregate_evaluators;
    // group by k1,k2
    vectorized::VExprContextSPtrs probe_expr_ctxs;
    size_t input_num_rows = 0;
    std::vector<vectorized::AggregateDataPtr> values;
    /// The total size of the row from the aggregate functions.
    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes offsets_of_aggregate_states;
    std::vector<size_t> make_nullable_keys;

    bool agg_data_created_without_key = false;
    bool enable_spill = false;
    bool reach_limit = false;

    int64_t limit = -1;
    bool do_sort_limit = false;
    vectorized::MutableColumns limit_columns;
    int limit_columns_min = -1;
    vectorized::PaddedPODArray<uint8_t> need_computes;
    std::vector<uint8_t> cmp_res;
    std::vector<int> order_directions;
    std::vector<int> null_directions;

    struct HeapLimitCursor {
        HeapLimitCursor(int row_id, vectorized::MutableColumns& limit_columns,
                        std::vector<int>& order_directions, std::vector<int>& null_directions)
                : _row_id(row_id),
                  _limit_columns(limit_columns),
                  _order_directions(order_directions),
                  _null_directions(null_directions) {}

        HeapLimitCursor(const HeapLimitCursor& other) = default;

        HeapLimitCursor(HeapLimitCursor&& other) noexcept
                : _row_id(other._row_id),
                  _limit_columns(other._limit_columns),
                  _order_directions(other._order_directions),
                  _null_directions(other._null_directions) {}

        HeapLimitCursor& operator=(const HeapLimitCursor& other) noexcept {
            _row_id = other._row_id;
            return *this;
        }

        HeapLimitCursor& operator=(HeapLimitCursor&& other) noexcept {
            _row_id = other._row_id;
            return *this;
        }

        bool operator<(const HeapLimitCursor& rhs) const {
            for (int i = 0; i < _limit_columns.size(); ++i) {
                const auto& _limit_column = _limit_columns[i];
                auto res = _limit_column->compare_at(_row_id, rhs._row_id, *_limit_column,
                                                     _null_directions[i]) *
                           _order_directions[i];
                if (res < 0) {
                    return true;
                } else if (res > 0) {
                    return false;
                }
            }
            return false;
        }

        int _row_id;
        vectorized::MutableColumns& _limit_columns;
        std::vector<int>& _order_directions;
        std::vector<int>& _null_directions;
    };

    std::priority_queue<HeapLimitCursor> limit_heap;

    // Refresh the top limit heap with a new row
    void refresh_top_limit(size_t row_id, const vectorized::ColumnRawPtrs& key_columns);

private:
    vectorized::MutableColumns _get_keys_hash_table();

    void _close_with_serialized_key() {
        std::visit(
                vectorized::Overload {[&](std::monostate& arg) -> void {
                                          // Do nothing
                                      },
                                      [&](auto& agg_method) -> void {
                                          auto& data = *agg_method.hash_table;
                                          data.for_each_mapped([&](auto& mapped) {
                                              if (mapped) {
                                                  _destroy_agg_status(mapped);
                                                  mapped = nullptr;
                                              }
                                          });
                                          if (data.has_null_key_data()) {
                                              _destroy_agg_status(data.template get_null_key_data<
                                                                  vectorized::AggregateDataPtr>());
                                          }
                                      }},
                agg_data->method_variant);
    }

    void _close_without_key() {
        //because prepare maybe failed, and couldn't create agg data.
        //but finally call close to destory agg data, if agg data has bitmapValue
        //will be core dump, it's not initialized
        if (agg_data_created_without_key) {
            _destroy_agg_status(agg_data->without_key);
            agg_data_created_without_key = false;
        }
    }
    void _destroy_agg_status(vectorized::AggregateDataPtr data);
};

struct BasicSpillSharedState {
    virtual ~BasicSpillSharedState() = default;

    // These two counters are shared to spill source operators as the initial value
    // of 'SpillWriteFileCurrentBytes' and 'SpillWriteFileCurrentCount'.
    // Total bytes of spill data written to disk file(after serialized)
    RuntimeProfile::Counter* _spill_write_file_total_size = nullptr;
    RuntimeProfile::Counter* _spill_file_total_count = nullptr;

    void setup_shared_profile(RuntimeProfile* sink_profile) {
        _spill_file_total_count =
                ADD_COUNTER_WITH_LEVEL(sink_profile, "SpillWriteFileTotalCount", TUnit::UNIT, 1);
        _spill_write_file_total_size =
                ADD_COUNTER_WITH_LEVEL(sink_profile, "SpillWriteFileBytes", TUnit::BYTES, 1);
    }

    virtual void update_spill_stream_profiles(RuntimeProfile* source_profile) = 0;
};

struct AggSpillPartition;
struct PartitionedAggSharedState : public BasicSharedState,
                                   public BasicSpillSharedState,
                                   public std::enable_shared_from_this<PartitionedAggSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionedAggSharedState)

    PartitionedAggSharedState() = default;
    ~PartitionedAggSharedState() override = default;

    void update_spill_stream_profiles(RuntimeProfile* source_profile) override;

    void init_spill_params(size_t spill_partition_count);

    void close();

    AggSharedState* in_mem_shared_state = nullptr;
    std::shared_ptr<BasicSharedState> in_mem_shared_state_sptr;

    size_t partition_count;
    size_t max_partition_index;
    bool is_spilled = false;
    std::atomic_bool is_closed = false;
    std::deque<std::shared_ptr<AggSpillPartition>> spill_partitions;

    size_t get_partition_index(size_t hash_value) const { return hash_value % partition_count; }
};

struct AggSpillPartition {
    static constexpr int64_t AGG_SPILL_FILE_SIZE = 1024 * 1024 * 1024; // 1G

    AggSpillPartition() = default;

    void close();

    Status get_spill_stream(RuntimeState* state, int node_id, RuntimeProfile* profile,
                            vectorized::SpillStreamSPtr& spilling_stream);

    Status flush_if_full() {
        DCHECK(spilling_stream_);
        Status status;
        // avoid small spill files
        if (spilling_stream_->get_written_bytes() >= AGG_SPILL_FILE_SIZE) {
            status = spilling_stream_->spill_eof();
            spilling_stream_.reset();
        }
        return status;
    }

    Status finish_current_spilling(bool eos = false) {
        if (spilling_stream_) {
            if (eos || spilling_stream_->get_written_bytes() >= AGG_SPILL_FILE_SIZE) {
                auto status = spilling_stream_->spill_eof();
                spilling_stream_.reset();
                return status;
            }
        }
        return Status::OK();
    }

    std::deque<vectorized::SpillStreamSPtr> spill_streams_;
    vectorized::SpillStreamSPtr spilling_stream_;
};
using AggSpillPartitionSPtr = std::shared_ptr<AggSpillPartition>;
struct SortSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(SortSharedState)
public:
    std::shared_ptr<vectorized::Sorter> sorter;
};

struct SpillSortSharedState : public BasicSharedState,
                              public BasicSpillSharedState,
                              public std::enable_shared_from_this<SpillSortSharedState> {
    ENABLE_FACTORY_CREATOR(SpillSortSharedState)

    SpillSortSharedState() = default;
    ~SpillSortSharedState() override = default;

    void update_spill_block_batch_row_count(RuntimeState* state, const vectorized::Block* block) {
        auto rows = block->rows();
        if (rows > 0 && 0 == avg_row_bytes) {
            avg_row_bytes = std::max((std::size_t)1, block->bytes() / rows);
            spill_block_batch_row_count =
                    (state->spill_sort_batch_bytes() + avg_row_bytes - 1) / avg_row_bytes;
            LOG(INFO) << "spill sort block batch row count: " << spill_block_batch_row_count;
        }
    }

    void update_spill_stream_profiles(RuntimeProfile* source_profile) override;

    void close();

    SortSharedState* in_mem_shared_state = nullptr;
    bool enable_spill = false;
    bool is_spilled = false;
    int64_t limit = -1;
    int64_t offset = 0;
    std::atomic_bool is_closed = false;
    std::shared_ptr<BasicSharedState> in_mem_shared_state_sptr;

    std::deque<vectorized::SpillStreamSPtr> sorted_streams;
    size_t avg_row_bytes = 0;
    size_t spill_block_batch_row_count;
};

struct UnionSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(UnionSharedState)

public:
    UnionSharedState(int child_count = 1) : data_queue(child_count), _child_count(child_count) {};
    int child_count() const { return _child_count; }
    DataQueue data_queue;
    const int _child_count;
};

struct DataQueueSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(DataQueueSharedState)
public:
    DataQueue data_queue;
};

class MultiCastDataStreamer;

struct MultiCastSharedState : public BasicSharedState,
                              public BasicSpillSharedState,
                              public std::enable_shared_from_this<MultiCastSharedState> {
    MultiCastSharedState(ObjectPool* pool, int cast_sender_count, int node_id);
    std::unique_ptr<pipeline::MultiCastDataStreamer> multi_cast_data_streamer;

    void update_spill_stream_profiles(RuntimeProfile* source_profile) override;
};

struct AnalyticSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(AnalyticSharedState)

public:
    AnalyticSharedState() = default;
    std::queue<vectorized::Block> blocks_buffer;
    std::mutex buffer_mutex;
    bool sink_eos = false;
    std::mutex sink_eos_lock;
};

struct JoinSharedState : public BasicSharedState {
    // For some join case, we can apply a short circuit strategy
    // 1. _has_null_in_build_side = true
    // 2. build side rows is empty, Join op is: inner join/right outer join/left semi/right semi/right anti
    bool _has_null_in_build_side = false;
    bool short_circuit_for_probe = false;
    // for some join, when build side rows is empty, we could return directly by add some additional null data in probe table.
    bool empty_right_table_need_probe_dispose = false;
    JoinOpVariants join_op_variants;
};

struct HashJoinSharedState : public JoinSharedState {
    ENABLE_FACTORY_CREATOR(HashJoinSharedState)
    HashJoinSharedState() {
        hash_table_variant_vector.push_back(std::make_shared<JoinDataVariants>());
    }
    HashJoinSharedState(int num_instances) {
        source_deps.resize(num_instances, nullptr);
        hash_table_variant_vector.resize(num_instances, nullptr);
        for (int i = 0; i < num_instances; i++) {
            hash_table_variant_vector[i] = std::make_shared<JoinDataVariants>();
        }
    }
    std::shared_ptr<vectorized::Arena> arena = std::make_shared<vectorized::Arena>();

    const std::vector<TupleDescriptor*> build_side_child_desc;
    size_t build_exprs_size = 0;
    std::shared_ptr<vectorized::Block> build_block;
    std::shared_ptr<std::vector<uint32_t>> build_indexes_null;

    /**
     * @brief Hash 表变体向量，用于共享 Hash 表机制
     *
     * 【功能说明】
     * 存储多个 Hash 表变体（JoinDataVariants），支持 Broadcast Join 的共享 Hash 表机制。
     * 每个元素对应一个 PipelineTask 的 Hash 表槽位。
     *
     * 【共享机制】
     * 1. Broadcast Join + 共享 Hash 表模式：
     *    - Task 0 构建 Hash 表，存储到 hash_table_variant_vector[0]
     *    - Task 1-N 复制 Task 0 的 Hash 表到各自的槽位
     *    - 所有 Task 共享同一个 Hash 表实例（共享底层数据）
     *
     * 2. 非共享模式：
     *    - 每个 Task 构建自己的 Hash 表
     *    - hash_table_variant_vector 大小为 1，所有 Task 使用同一个槽位
     *
     * 【读写特性】
     * 1. Probe 算子的读取特性：
     *    - 如果未使用 visited flags（visited flags 仅在 RIGHT/FULL OUTER JOIN 中使用），
     *      Hash 表是只读的，多个 Probe Task 可以安全地并发读取
     *    - 如果使用了 visited flags，每个 Probe Task 需要独立的 visited flags 状态
     *
     * 2. Broadcast Join 的特殊情况：
     *    - 虽然 Hash 表的核心数据（buckets、keys）是只读的，
     *      但 `JoinDataVariants` 中的某些状态仍然可以写入
     *    - 例如：序列化的 keys（serialized keys）会被写入到连续的内存区域
     *    - 这些状态写入发生在 `_hash_table_variants` 中，而不是 Hash 表本身
     *
     * 【为什么需要本地副本】
     * 在执行前，应该使用本地的 `_hash_table_variants`，其中包含共享的 Hash 表。
     * 原因：
     * 1. 避免并发写入冲突：每个 Task 有独立的状态空间，可以安全地写入序列化 keys 等状态
     * 2. 支持 visited flags：RIGHT/FULL OUTER JOIN 需要每个 Task 独立的 visited flags
     * 3. 性能优化：本地副本可以减少锁竞争，提高并发性能
     *
     * 【示例】
     * ```cpp
     * // Broadcast Join，4 个 Task，启用共享 Hash 表
     * hash_table_variant_vector.size() = 4
     * 
     * // Task 0 构建 Hash 表
     * hash_table_variant_vector[0]->method_variant = HashTable (构建完成)
     * 
     * // Task 1-3 复制 Hash 表
     * hash_table_variant_vector[1]->method_variant.hash_table = hash_table_variant_vector[0]->method_variant.hash_table
     * hash_table_variant_vector[2]->method_variant.hash_table = hash_table_variant_vector[0]->method_variant.hash_table
     * hash_table_variant_vector[3]->method_variant.hash_table = hash_table_variant_vector[0]->method_variant.hash_table
     * 
     * // 每个 Task 的 method_variant 是独立的，但 hash_table 指针指向同一个实例
     * // 这样既共享了 Hash 表数据，又避免了状态写入冲突
     * ```
     */
    std::vector<std::shared_ptr<JoinDataVariants>> hash_table_variant_vector;
};

struct PartitionedHashJoinSharedState
        : public HashJoinSharedState,
          public BasicSpillSharedState,
          public std::enable_shared_from_this<PartitionedHashJoinSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinSharedState)

    void update_spill_stream_profiles(RuntimeProfile* source_profile) override {
        for (auto& stream : spilled_streams) {
            if (stream) {
                stream->update_shared_profiles(source_profile);
            }
        }
    }

    std::unique_ptr<RuntimeState> inner_runtime_state;
    std::shared_ptr<HashJoinSharedState> inner_shared_state;
    std::vector<std::unique_ptr<vectorized::MutableBlock>> partitioned_build_blocks;
    std::vector<vectorized::SpillStreamSPtr> spilled_streams;
    bool is_spilled = false;
};

struct NestedLoopJoinSharedState : public JoinSharedState {
    ENABLE_FACTORY_CREATOR(NestedLoopJoinSharedState)
    // if true, left child has no more rows to process
    bool left_side_eos = false;
    // Visited flags for each row in build side.
    vectorized::MutableColumns build_side_visited_flags;
    // List of build blocks, constructed in prepare()
    vectorized::Blocks build_blocks;
};

struct PartitionSortNodeSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(PartitionSortNodeSharedState)
public:
    std::queue<vectorized::Block> blocks_buffer;
    std::mutex buffer_mutex;
    std::vector<std::unique_ptr<vectorized::PartitionSorter>> partition_sorts;
    bool sink_eos = false;
    std::mutex sink_eos_lock;
    std::mutex prepared_finish_lock;
};

struct SetSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(SetSharedState)
public:
    /// default init
    vectorized::Block build_block; // build to source
    //record element size in hashtable
    int64_t valid_element_in_hash_tbl = 0;
    //first: idx mapped to column types
    //second: column_id, could point to origin column or cast column
    std::unordered_map<int, int> build_col_idx;

    //// shared static states (shared, decided in prepare/open...)

    /// init in setup_local_state
    std::unique_ptr<SetDataVariants> hash_table_variants =
            std::make_unique<SetDataVariants>(); // the real data HERE.
    std::vector<bool> build_not_ignore_null;

    // The SET operator's child might have different nullable attributes.
    // If a calculation involves both nullable and non-nullable columns, the final output should be a nullable column
    Status update_build_not_ignore_null(const vectorized::VExprContextSPtrs& ctxs);

    size_t get_hash_table_size() const;
    /// init in both upstream side.
    //The i-th result expr list refers to the i-th child.
    std::vector<vectorized::VExprContextSPtrs> child_exprs_lists;

    /// init in build side
    size_t child_quantity;
    vectorized::VExprContextSPtrs build_child_exprs;
    std::vector<Dependency*> probe_finished_children_dependency;

    /// init in probe side
    std::vector<vectorized::VExprContextSPtrs> probe_child_exprs_lists;

    std::atomic<bool> ready_for_read = false;

    /// called in setup_local_state
    Status hash_table_init();
};

enum class ExchangeType : uint8_t {
    NOOP = 0,
    // Shuffle data by Crc32CHashPartitioner
    HASH_SHUFFLE = 1,
    // Round-robin passthrough data blocks.
    PASSTHROUGH = 2,
    // Shuffle data by Crc32HashPartitioner<ShuffleChannelIds> (e.g. same as storage engine).
    BUCKET_HASH_SHUFFLE = 3,
    // Passthrough data blocks to all channels.
    BROADCAST = 4,
    // Passthrough data to channels evenly in an adaptive way.
    ADAPTIVE_PASSTHROUGH = 5,
    // Send all data to the first channel.
    PASS_TO_ONE = 6,
};

inline std::string get_exchange_type_name(ExchangeType idx) {
    switch (idx) {
    case ExchangeType::NOOP:
        return "NOOP";
    case ExchangeType::HASH_SHUFFLE:
        return "HASH_SHUFFLE";
    case ExchangeType::PASSTHROUGH:
        return "PASSTHROUGH";
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        return "BUCKET_HASH_SHUFFLE";
    case ExchangeType::BROADCAST:
        return "BROADCAST";
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        return "ADAPTIVE_PASSTHROUGH";
    case ExchangeType::PASS_TO_ONE:
        return "PASS_TO_ONE";
    }
    throw Exception(Status::FatalError("__builtin_unreachable"));
}

struct DataDistribution {
    DataDistribution(ExchangeType type) : distribution_type(type) {}
    DataDistribution(ExchangeType type, const std::vector<TExpr>& partition_exprs_)
            : distribution_type(type), partition_exprs(partition_exprs_) {}
    DataDistribution(const DataDistribution& other) = default;
    bool need_local_exchange() const { return distribution_type != ExchangeType::NOOP; }
    DataDistribution& operator=(const DataDistribution& other) = default;
    ExchangeType distribution_type;
    std::vector<TExpr> partition_exprs;
};

class ExchangerBase;

/**
 * 本地交换共享状态：继承自 BasicSharedState，提供本地数据交换功能
 * 
 * 设计理念：
 * 1. 继承复用：继承 BasicSharedState 的基础功能（统计、配置、依赖等）
 * 2. 功能扩展：添加本地交换特有的功能（数据交换、内存管理、通道协调等）
 * 3. 内存控制：提供细粒度的内存使用监控和限制机制
 * 4. 通道管理：管理多个数据通道的依赖关系和内存使用
 * 
 * 在 Stream Load 中的作用：
 * - 当数据需要在本地节点内重新分布时使用
 * - 支持数据重新分区、并行度调整、数据倾斜处理等场景
 * - 提供内存使用监控，防止内存溢出
 */
struct LocalExchangeSharedState : public BasicSharedState {
public:
    ENABLE_FACTORY_CREATOR(LocalExchangeSharedState);
    
    /**
     * 构造函数：初始化本地交换共享状态
     * @param num_instances 实例数量，用于初始化内存计数器和依赖关系
     */
    LocalExchangeSharedState(int num_instances);
    
    /**
     * 析构函数：清理资源
     */
    ~LocalExchangeSharedState() override;
    
    /**
     * 数据交换器：负责实际的数据交换逻辑
     * 支持不同类型的数据交换策略（如哈希分区、轮询等）
     */
    std::unique_ptr<ExchangerBase> exchanger {};
    
    /**
     * 内存计数器：每个通道一个，用于监控各个通道的内存使用情况
     * 索引对应通道ID，便于定位内存使用热点
     */
    std::vector<RuntimeProfile::Counter*> mem_counters;
    
    /**
     * 总内存使用量：所有通道的总内存使用统计
     * 原子操作，线程安全，用于全局内存控制
     */
    std::atomic<int64_t> mem_usage = 0;
    
    /**
     * 缓冲区内存限制：本地交换缓冲区的最大内存使用量
     * 可配置，支持低内存模式下的动态调整
     */
    std::atomic<size_t> _buffer_mem_limit = config::local_exchange_buffer_mem_limit;
    
    /**
     * 本地交换锁：保护共享状态的并发访问
     * 主要用于内存使用统计的原子性操作
     */
    std::mutex le_lock;
    
    /**
     * 减少运行中的 Sink 操作符计数
     * 当 Sink 操作符完成时调用，用于协调操作符的生命周期
     */
    void sub_running_sink_operators();
    
    /**
     * 减少运行中的 Source 操作符计数
     * 当 Source 操作符完成时调用，用于协调操作符的生命周期
     */
    void sub_running_source_operators();
    
    /**
     * 设置所有依赖关系为始终就绪状态
     * 用于测试或特殊场景，确保数据流不会阻塞
     */
    void _set_always_ready() {
        // 设置所有 Source 依赖为始终就绪
        for (auto& dep : source_deps) {
            DCHECK(dep);
            dep->set_always_ready();
        }
        // 设置所有 Sink 依赖为始终就绪
        for (auto& dep : sink_deps) {
            DCHECK(dep);
            dep->set_always_ready();
        }
    }

    /**
     * 根据通道ID获取Sink依赖关系
     * 本地交换不需要网络通道，所以返回 nullptr
     * @param channel_id 通道ID（本地交换中不使用）
     * @return Dependency* 总是返回 nullptr
     */
    Dependency* get_sink_dep_by_channel_id(int channel_id) { return nullptr; }

    /**
     * 设置指定通道为可读状态
     * 当通道有数据可读时调用，通知依赖该通道的操作符
     * @param channel_id 通道ID
     */
    void set_ready_to_read(int channel_id) {
        auto& dep = source_deps[channel_id];
        DCHECK(dep) << channel_id;
        dep->set_ready();
    }

    /**
     * 增加指定通道的内存使用量
     * 更新通道级别的内存计数器，用于细粒度监控
     * @param channel_id 通道ID
     * @param delta 增加的内存量
     */
    void add_mem_usage(int channel_id, size_t delta) { 
        mem_counters[channel_id]->update(delta); 
    }

    /**
     * 减少指定通道的内存使用量
     * 更新通道级别的内存计数器，用于细粒度监控
     * @param channel_id 通道ID
     * @param delta 减少的内存量
     */
    void sub_mem_usage(int channel_id, size_t delta) {
        mem_counters[channel_id]->update(-(int64_t)delta);
    }

    /**
     * 增加总内存使用量
     * 关键方法：需要先增加内存使用量，再入队数据
     * 否则并发出队时可能导致内存使用量为负数
     * 
     * 内存控制逻辑：
     * - 如果总内存使用量超过限制，阻塞第一个 Sink 依赖
     * - 防止内存溢出，实现背压控制
     * 
     * @param delta 增加的内存量
     */
    void add_total_mem_usage(size_t delta) {
        if (cast_set<int64_t>(mem_usage.fetch_add(delta) + delta) > _buffer_mem_limit) {
            // 内存超限，阻塞 Sink 操作符，实现背压控制
            sink_deps.front()->block();
        }
    }

    /**
     * 减少总内存使用量
     * 当数据被消费后，释放相应的内存
     * 
     * 内存控制逻辑：
     * - 如果内存使用量降到限制以下，唤醒第一个 Sink 依赖
     * - 恢复数据流，避免不必要的阻塞
     * 
     * @param delta 减少的内存量
     */
    void sub_total_mem_usage(size_t delta) {
        auto prev_usage = mem_usage.fetch_sub(delta);
        // 断言检查：内存使用量不能为负数
        DCHECK_GE(prev_usage - delta, 0) << "prev_usage: " << prev_usage << " delta: " << delta;
        
        if (cast_set<int64_t>(prev_usage - delta) <= _buffer_mem_limit) {
            // 内存释放到限制以下，唤醒 Sink 操作符
            sink_deps.front()->set_ready();
        }
    }

    /**
     * 设置低内存模式
     * 在内存紧张时动态调整缓冲区限制
     * 
     * 调整策略：
     * - 取配置的本地交换缓冲区限制和低内存模式限制的较小值
     * - 确保在内存紧张时不会过度使用内存
     * 
     * @param state 运行时状态，包含低内存模式的配置信息
     */
    void set_low_memory_mode(RuntimeState* state) {
        _buffer_mem_limit = std::min<int64_t>(config::local_exchange_buffer_mem_limit,
                                              state->low_memory_mode_buffer_limit());
    }
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
