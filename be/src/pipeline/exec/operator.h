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

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/local_exchange/local_exchanger.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris {
#include "common/compile_check_begin.h"
class RowDescriptor;
class RuntimeState;
class TDataSink;
namespace vectorized {
class AsyncResultWriter;
class ScoreRuntime;
class AnnTopNRuntime;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

class OperatorBase;
class OperatorXBase;
class DataSinkOperatorXBase;

using OperatorPtr = std::shared_ptr<OperatorXBase>;
using Operators = std::vector<OperatorPtr>;

using DataSinkOperatorPtr = std::shared_ptr<DataSinkOperatorXBase>;

// This suffix will be added back to the name of sink operator
// when we creating runtime profile.
const std::string exchange_sink_name_suffix = "(dest_id={})";

const std::string operator_name_suffix = "(id={})";

// This struct is used only for initializing local state.
struct LocalStateInfo {
    RuntimeProfile* parent_profile = nullptr;
    const std::vector<TScanRangeParams>& scan_ranges;
    BasicSharedState* shared_state;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>& shared_state_map;
    const int task_idx;
};

// This struct is used only for initializing local sink state.
struct LocalSinkStateInfo {
    const int task_idx;
    RuntimeProfile* parent_profile = nullptr;
    const int sender_id;
    BasicSharedState* shared_state;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>& shared_state_map;
    const TDataSink& tsink;
};

class OperatorBase {
public:
    explicit OperatorBase() : _child(nullptr), _is_closed(false) {}
    explicit OperatorBase(bool is_serial_operator)
            : _child(nullptr), _is_closed(false), _is_serial_operator(is_serial_operator) {}
    virtual ~OperatorBase() = default;

    virtual bool is_sink() const { return false; }

    virtual bool is_source() const { return false; }

    [[nodiscard]] virtual const RowDescriptor& row_desc() const;

    [[nodiscard]] virtual Status init(const TDataSink& tsink) { return Status::OK(); }

    [[nodiscard]] virtual std::string get_name() const = 0;
    [[nodiscard]] virtual Status prepare(RuntimeState* state) = 0;
    [[nodiscard]] virtual Status terminate(RuntimeState* state) = 0;
    [[nodiscard]] virtual Status close(RuntimeState* state);
    [[nodiscard]] virtual int node_id() const = 0;
    [[nodiscard]] virtual int parallelism(RuntimeState* state) const {
        return _is_serial_operator ? 1 : state->query_parallel_instance_num();
    }

    [[nodiscard]] virtual Status set_child(OperatorPtr child) {
        if (_child && child != nullptr) {
            return Status::InternalError("Child is already set in node name={}", get_name());
        }
        _child = child;
        return Status::OK();
    }

    // Operators need to be executed serially. (e.g. finalized agg without key)
    [[nodiscard]] virtual bool is_serial_operator() const { return _is_serial_operator; }

    [[nodiscard]] bool is_closed() const { return _is_closed; }

    virtual size_t revocable_mem_size(RuntimeState* state) const { return 0; }

    virtual Status revoke_memory(RuntimeState* state,
                                 const std::shared_ptr<SpillContext>& spill_context) {
        return Status::OK();
    }

    /**
     * Pipeline task is blockable means it will be blocked in the next run. So we should put the
     * pipeline task into the blocking task scheduler.
     */
    virtual bool is_blockable(RuntimeState* state) const = 0;

    virtual void set_low_memory_mode(RuntimeState* state) {}

    [[nodiscard]] virtual bool require_data_distribution() const { return false; }
    OperatorPtr child() { return _child; }
    [[nodiscard]] bool followed_by_shuffled_operator() const {
        return _followed_by_shuffled_operator;
    }
    void set_followed_by_shuffled_operator(bool followed_by_shuffled_operator) {
        _followed_by_shuffled_operator = followed_by_shuffled_operator;
    }
    [[nodiscard]] virtual bool is_shuffled_operator() const { return false; }
    [[nodiscard]] virtual DataDistribution required_data_distribution(
            RuntimeState* /*state*/) const;
    [[nodiscard]] virtual bool require_shuffled_data_distribution(RuntimeState* /*state*/) const;

protected:
    OperatorPtr _child = nullptr;

    bool _is_closed;
    bool _followed_by_shuffled_operator = false;
    bool _is_serial_operator = false;
};

class PipelineXLocalStateBase {
public:
    PipelineXLocalStateBase(RuntimeState* state, OperatorXBase* parent);
    virtual ~PipelineXLocalStateBase() = default;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    virtual Status init(RuntimeState* state, LocalStateInfo& info) = 0;
    // Make sure all resources are ready before execution. For example, remote tablets should be
    // loaded to local storage.
    // This is called by execution pthread and different from `Operator::prepare` which is called
    // by bthread.
    virtual Status prepare(RuntimeState* state) = 0;
    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state) = 0;
    virtual Status terminate(RuntimeState* state) = 0;

    // If use projection, we should clear `_origin_block`.
    void clear_origin_block();

    void reached_limit(vectorized::Block* block, bool* eos);
    RuntimeProfile* operator_profile() { return _operator_profile.get(); }
    RuntimeProfile* common_profile() { return _common_profile.get(); }
    RuntimeProfile* custom_profile() { return _custom_profile.get(); }

    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }
    OperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    vectorized::VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] int64_t num_rows_returned() const { return _num_rows_returned; }
    void add_num_rows_returned(int64_t delta) { _num_rows_returned += delta; }
    void set_num_rows_returned(int64_t value) { _num_rows_returned = value; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level = 0) const = 0;
    [[nodiscard]] virtual bool is_blockable() const;

    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in Scan
    virtual Dependency* finishdependency() { return nullptr; }
    //  override in Scan  MultiCastSink
    virtual std::vector<Dependency*> execution_dependencies() { return {}; }

    Status filter_block(const vectorized::VExprContextSPtrs& expr_contexts,
                        vectorized::Block* block, size_t column_to_keep);

    int64_t& estimate_memory_usage() { return _estimate_memory_usage; }

    void reset_estimate_memory_usage() { _estimate_memory_usage = 0; }

    bool low_memory_mode() {
#ifdef BE_TEST
        return false;
#else
        return _state->low_memory_mode();
#endif
    }

protected:
    friend class OperatorXBase;
    template <typename LocalStateType>
    friend class ScanOperatorX;

    ObjectPool* _pool = nullptr;
    int64_t _num_rows_returned {0};
    int64_t _estimate_memory_usage {0};

    /*
    Each operator has its profile like this:
    XXXX_OPERATOR:
        CommonCounters:
            ...
        CustomCounters:
            ...
    */
    // Profile of this operator.
    // Should not modify this profile usually.
    std::unique_ptr<RuntimeProfile> _operator_profile;
    // CommonCounters of this operator.
    // CommonCounters are counters that will be used by all operators.
    std::unique_ptr<RuntimeProfile> _common_profile;
    // CustomCounters of this operator.
    // CustomCounters are counters that will be used by this operator only.
    std::unique_ptr<RuntimeProfile> _custom_profile;

    RuntimeProfile::Counter* _rows_returned_counter = nullptr;
    RuntimeProfile::Counter* _blocks_returned_counter = nullptr;
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    // Account for current memory and peak memory used by this node
    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _projection_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;

    OperatorXBase* _parent = nullptr;
    RuntimeState* _state = nullptr;
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    std::shared_ptr<vectorized::ScoreRuntime> _score_runtime;
    std::shared_ptr<segment_v2::AnnTopNRuntime> _ann_topn_runtime;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<vectorized::VExprContextSPtrs> _intermediate_projections;

    bool _closed = false;
    std::atomic<bool> _terminated = false;
    vectorized::Block _origin_block;
};

template <typename SharedStateArg = FakeSharedState>
class PipelineXLocalState : public PipelineXLocalStateBase {
public:
    using SharedStateType = SharedStateArg;
    PipelineXLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalStateBase(state, parent) {}
    ~PipelineXLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override;

    virtual std::string name_suffix() const;

    Status close(RuntimeState* state) override;
    Status terminate(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }

    virtual bool must_set_shared_state() const {
        return !std::is_same_v<SharedStateArg, FakeSharedState>;
    }

protected:
    Dependency* _dependency = nullptr;
    SharedStateArg* _shared_state = nullptr;
};

template <typename SharedStateArg>
class PipelineXSpillLocalState : public PipelineXLocalState<SharedStateArg> {
public:
    using Base = PipelineXLocalState<SharedStateArg>;
    PipelineXSpillLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<SharedStateArg>(state, parent) {}
    ~PipelineXSpillLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXLocalState<SharedStateArg>::init(state, info));

        init_spill_read_counters();

        return Status::OK();
    }

    void init_spill_write_counters() {
        _spill_write_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTime", 1);

        _spill_write_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteTaskWaitInQueueCount", TUnit::UNIT, 1);
        _spill_writing_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillWriteTaskCount", TUnit::UNIT, 1);
        _spill_write_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTaskWaitInQueueTime", 1);

        _spill_write_file_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteFileTime", 1);

        _spill_write_serialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteSerializeBlockTime", 1);
        _spill_write_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                          "SpillWriteBlockCount", TUnit::UNIT, 1);
        _spill_write_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
        _spill_write_file_total_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileBytes", TUnit::BYTES, 1);
        _spill_write_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillWriteRows", TUnit::UNIT, 1);
        _spill_file_total_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileTotalCount", TUnit::UNIT, 1);
    }

    void init_spill_read_counters() {
        _spill_total_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillTotalTime", 1);

        // Spill read counters
        _spill_recover_time = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillRecoverTime", 1);

        _spill_read_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillReadTaskWaitInQueueCount", TUnit::UNIT, 1);
        _spill_reading_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillReadTaskCount", TUnit::UNIT, 1);
        _spill_read_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadTaskWaitInQueueTime", 1);

        _spill_read_file_time =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileTime", 1);
        _spill_read_derialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadDerializeBlockTime", 1);

        _spill_read_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                         "SpillReadBlockCount", TUnit::UNIT, 1);
        _spill_read_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillReadBlockBytes", TUnit::BYTES, 1);
        _spill_read_file_size = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileBytes",
                                                       TUnit::BYTES, 1);
        _spill_read_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadRows", TUnit::UNIT, 1);
        _spill_read_file_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                        "SpillReadFileCount", TUnit::UNIT, 1);

        _spill_file_current_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileCurrentBytes", TUnit::BYTES, 1);
        _spill_file_current_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileCurrentCount", TUnit::UNIT, 1);
    }

    // These two counters are shared to spill source operators as the initial value
    // Initialize values of counters 'SpillWriteFileCurrentBytes' and 'SpillWriteFileCurrentCount'
    // from spill sink operators' "SpillWriteFileTotalCount" and "SpillWriteFileBytes"
    void copy_shared_spill_profile() {
        if (_copy_shared_spill_profile) {
            _copy_shared_spill_profile = false;
            const auto* spill_shared_state = (const BasicSpillSharedState*)Base::_shared_state;
            COUNTER_UPDATE(_spill_file_current_size,
                           spill_shared_state->_spill_write_file_total_size->value());
            COUNTER_UPDATE(_spill_file_current_count,
                           spill_shared_state->_spill_file_total_count->value());
            Base::_shared_state->update_spill_stream_profiles(Base::custom_profile());
        }
    }

    // Total time of spill, including spill task scheduling time,
    // serialize block time, write disk file time,
    // and read disk file time, deserialize block time etc.
    RuntimeProfile::Counter* _spill_total_timer = nullptr;

    // Spill write counters
    // Total time of spill write, including serialize block time, write disk file,
    // and wait in queue time, etc.
    RuntimeProfile::Counter* _spill_write_timer = nullptr;

    RuntimeProfile::Counter* _spill_write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _spill_writing_task_count = nullptr;
    RuntimeProfile::Counter* _spill_write_wait_in_queue_timer = nullptr;

    // Total time of writing file
    RuntimeProfile::Counter* _spill_write_file_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_serialize_block_timer = nullptr;
    // Original count of spilled Blocks
    // One Big Block maybe split into multiple small Blocks when actually written to disk file.
    RuntimeProfile::Counter* _spill_write_block_count = nullptr;
    // Total bytes of spill data in Block format(in memory format)
    RuntimeProfile::Counter* _spill_write_block_data_size = nullptr;
    // Total bytes of spill data written to disk file(after serialized)
    RuntimeProfile::Counter* _spill_write_file_total_size = nullptr;
    RuntimeProfile::Counter* _spill_write_rows_count = nullptr;
    RuntimeProfile::Counter* _spill_file_total_count = nullptr;
    RuntimeProfile::Counter* _spill_file_current_count = nullptr;
    // Spilled file total size
    RuntimeProfile::Counter* _spill_file_total_size = nullptr;
    // Current spilled file size
    RuntimeProfile::Counter* _spill_file_current_size = nullptr;

    // Spill read counters
    // Total time of recovring spilled data, including read file time, deserialize time, etc.
    RuntimeProfile::Counter* _spill_recover_time = nullptr;

    RuntimeProfile::Counter* _spill_read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _spill_reading_task_count = nullptr;
    RuntimeProfile::Counter* _spill_read_wait_in_queue_timer = nullptr;

    RuntimeProfile::Counter* _spill_read_file_time = nullptr;
    RuntimeProfile::Counter* _spill_read_derialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_read_block_count = nullptr;
    // Total bytes of read data in Block format(in memory format)
    RuntimeProfile::Counter* _spill_read_block_data_size = nullptr;
    // Total bytes of spill data read from disk file
    RuntimeProfile::Counter* _spill_read_file_size = nullptr;
    RuntimeProfile::Counter* _spill_read_rows_count = nullptr;
    RuntimeProfile::Counter* _spill_read_file_count = nullptr;

    bool _copy_shared_spill_profile = true;
};

class DataSinkOperatorXBase;

/**
 * Pipeline 执行引擎中 Sink 操作符的本地状态基类
 * 
 * 设计理念：
 * 1. 分离关注点：将操作符的逻辑（OperatorX）与本地执行状态（LocalState）分离
 * 2. 并发安全：每个执行实例都有独立的本地状态，避免线程间竞争
 * 3. 生命周期管理：提供完整的初始化、执行、清理生命周期管理
 * 4. 性能监控：内置完整的性能监控和统计功能
 * 
 * 在 Stream Load 中的作用：
 * - OlapTableSinkOperator 会创建对应的 LocalState 实例
 * - 每个执行线程都有独立的 LocalState，管理自己的内存、依赖关系等
 * - 通过 _parent 指针访问操作符的配置信息
 */
class PipelineXSinkLocalStateBase {
public:
    /**
     * 构造函数：初始化本地状态
     * @param parent_ 指向父操作符的指针，用于访问操作符配置
     * @param state_ 运行时状态，包含查询执行的上下文信息
     */
    PipelineXSinkLocalStateBase(DataSinkOperatorXBase* parent_, RuntimeState* state_);
    virtual ~PipelineXSinkLocalStateBase() = default;

    /**
     * 初始化方法：只执行一次，在 bthread 中执行
     * 可以执行轻量级或非幂等操作（如初始化 profile、从 operatorX 克隆表达式上下文）
     * @param state 运行时状态
     * @param info 本地状态信息
     * @return Status 初始化状态
     */
    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    /**
     * 准备方法：可以执行多次，必须是幂等的
     * 通常用于等待运行时过滤器等
     * @param state 运行时状态
     * @return Status 准备状态
     */
    virtual Status prepare(RuntimeState* state) = 0;
    
    /**
     * 打开方法：可以执行多次，必须是幂等的
     * 通常用于等待运行时过滤器等
     * @param state 运行时状态
     * @return Status 打开状态
     */
    virtual Status open(RuntimeState* state) = 0;
    
    /**
     * 终止方法：优雅地终止执行
     * @param state 运行时状态
     * @return Status 终止状态
     */
    virtual Status terminate(RuntimeState* state) = 0;
    
    /**
     * 关闭方法：清理资源
     * @param state 运行时状态
     * @param exec_status 执行状态
     * @return Status 关闭状态
     */
    virtual Status close(RuntimeState* state, Status exec_status) = 0;
    
    /**
     * 检查是否已完成
     * @return bool 是否已完成
     */
    [[nodiscard]] virtual bool is_finished() const { return false; }
    [[nodiscard]] virtual bool is_blockable() const { return false; }

    /**
     * 调试字符串：用于调试和日志记录
     * @param indentation_level 缩进级别
     * @return std::string 调试信息
     */
    [[nodiscard]] virtual std::string debug_string(int indentation_level) const = 0;

    /**
     * 类型转换模板：将当前对象转换为目标类型
     * 包含运行时类型检查，确保类型安全
     * @tparam TARGET 目标类型
     * @return TARGET& 转换后的引用
     */
    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    
    /**
     * 常量类型转换模板：const 版本
     */
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    // ========== 访问器方法 ==========
    
    /**
     * 获取父操作符指针
     * @return DataSinkOperatorXBase* 父操作符指针
     */
    DataSinkOperatorXBase* parent() { return _parent; }
    
    /**
     * 获取运行时状态
     * @return RuntimeState* 运行时状态指针
     */
    RuntimeState* state() { return _state; }
    
    /**
     * 获取操作符性能分析器
     * @return RuntimeProfile* 操作符性能分析器
     */
    RuntimeProfile* operator_profile() { return _operator_profile; }
    
    /**
     * 获取通用性能分析器
     * @return RuntimeProfile* 通用性能分析器
     */
    RuntimeProfile* common_profile() { return _common_profile; }
    
    /**
     * 获取自定义性能分析器
     * @return RuntimeProfile* 自定义性能分析器
     */
    RuntimeProfile* custom_profile() { return _custom_profile; }

    /**
     * 获取假性能分析器（用于不重要的计数器/计时器）
     * @return RuntimeProfile* 假性能分析器指针
     */
    [[nodiscard]] RuntimeProfile* faker_runtime_profile() const {
        return _faker_runtime_profile.get();
    }

    // ========== 性能计数器访问器 ==========
    
    /**
     * 获取输入行数计数器
     * @return RuntimeProfile::Counter* 输入行数计数器
     */
    RuntimeProfile::Counter* rows_input_counter() { return _rows_input_counter; }
    
    /**
     * 获取执行时间计数器
     * @return RuntimeProfile::Counter* 执行时间计数器
     */
    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    
    /**
     * 获取内存使用计数器
     * @return RuntimeProfile::Counter* 内存使用计数器
     */
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }

    // ========== 依赖关系管理 ==========
    
    /**
     * 获取依赖关系列表
     * @return std::vector<Dependency*> 依赖关系列表
     */
    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    /**
     * 获取完成依赖关系（在 exchange sink 和 AsyncWriterSink 中重写）
     * @return Dependency* 完成依赖关系指针
     */
    virtual Dependency* finishdependency() { return nullptr; }

    /**
     * 检查是否处于低内存模式
     * @return bool 是否处于低内存模式
     */
    bool low_memory_mode() { return _state->low_memory_mode(); }

protected:
    // ========== 核心成员变量 ==========
    
    /**
     * 父操作符指针：指向当前 LocalState 所属的 DataSink 操作符
     * 用于访问操作符的配置信息、输出表达式等
     */
    DataSinkOperatorXBase* _parent = nullptr;
    
    /**
     * 运行时状态：包含查询执行的上下文信息
     * 包括查询选项、资源限制、运行时过滤器等
     */
    RuntimeState* _state = nullptr;
    
    // ========== 性能分析器 ==========
    
    /**
     * 操作符性能分析器：记录操作符级别的性能指标
     */
    RuntimeProfile* _operator_profile = nullptr;
    
    /**
     * 通用性能分析器：记录通用的性能指标
     */
    RuntimeProfile* _common_profile = nullptr;
    
    /**
     * 自定义性能分析器：记录自定义的性能指标
     */
    RuntimeProfile* _custom_profile = nullptr;
    
    // ========== 状态标志 ==========
    
    /**
     * 关闭标志：在 close() 被调用后设置为 true
     * 子类应该在 close() 中检查并设置此标志
     */
    bool _closed = false;
    
    /**
     * 终止标志：表示是否已被终止
     */
    bool _terminated = false;
    
    /**
     * 假性能分析器：用于不重要的计数器/计时器
     * 
     * 注意：现在添加一个假 profile，因为有时 profile 记录是无用的
     * 所以我们要移除一些计数器/计时器，例如：
     * - 在 join 节点中，如果是 broadcast_join 且共享哈希表
     * - 一些关于构建哈希表的计数器/计时器是无用的
     * - 所以我们可以将这些计数器/计时器添加到假 profile 中
     * - 这些不会在 web profile 中显示
     */
    std::unique_ptr<RuntimeProfile> _faker_runtime_profile =
            std::make_unique<RuntimeProfile>("faker profile");

    // ========== 性能计数器 ==========
    
    /**
     * 输入行数计数器：记录输入的数据行数
     */
    RuntimeProfile::Counter* _rows_input_counter = nullptr;
    
    /**
     * 初始化计时器：记录初始化耗时
     */
    RuntimeProfile::Counter* _init_timer = nullptr;
    
    /**
     * 打开计时器：记录打开耗时
     */
    RuntimeProfile::Counter* _open_timer = nullptr;
    
    /**
     * 关闭计时器：记录关闭耗时
     */
    RuntimeProfile::Counter* _close_timer = nullptr;
    
    /**
     * 等待依赖关系计时器：记录等待依赖关系就绪的耗时
     */
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    
    /**
     * 等待完成依赖关系计时器：记录等待完成依赖关系就绪的耗时
     */
    RuntimeProfile::Counter* _wait_for_finish_dependency_timer = nullptr;
    
    /**
     * 执行计时器：记录总执行耗时
     */
    RuntimeProfile::Counter* _exec_timer = nullptr;
    
    /**
     * 内存使用计数器：记录内存使用情况（高水位标记）
     */
    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;
};

/**
 * Pipeline 执行引擎中 Sink 操作符的本地状态模板类
 * 
 * 设计理念：
 * 1. 模板化设计：通过模板参数 SharedStateArg 支持不同类型的共享状态
 * 2. 继承扩展：继承自 PipelineXSinkLocalStateBase，添加共享状态管理功能
 * 3. 依赖管理：提供依赖关系管理，支持操作符间的协调执行
 * 4. 生命周期：实现完整的初始化、准备、打开、终止、关闭生命周期
 * 
 * 模板参数说明：
 * @tparam SharedStateArg 共享状态类型，默认为 FakeSharedState
 * - BasicSharedState: 基本的共享状态，用于需要共享数据的场景
 * - FakeSharedState: 假共享状态，用于不需要共享数据的场景
 * - 其他类型: 特定操作符可能需要特殊的共享状态类型
 * 
 * 在 Stream Load 中的作用：
 * - OlapTableSinkLocalState 继承此类，使用 BasicSharedState
 * - 支持多个 LocalState 实例共享某些数据（如计数器、配置等）
 * - 通过依赖关系协调多个实例的执行顺序
 */
template <typename SharedStateArg = FakeSharedState>
class PipelineXSinkLocalState : public PipelineXSinkLocalStateBase {
public:
    /**
     * 共享状态类型别名：使用模板参数作为共享状态类型
     * 在编译时确定，提供类型安全
     */
    using SharedStateType = SharedStateArg;
    
    /**
     * 构造函数：初始化本地状态
     * @param parent 指向父操作符的指针，用于访问操作符配置
     * @param state 运行时状态，包含查询执行的上下文信息
     */
    PipelineXSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalStateBase(parent, state) {}
    
    /**
     * 析构函数：默认析构，自动清理资源
     */
    ~PipelineXSinkLocalState() override = default;

    /**
     * 初始化方法：执行一次性的初始化操作
     * 包括设置依赖关系、创建共享状态等
     * @param state 运行时状态
     * @param info 本地状态信息
     * @return Status 初始化状态
     */
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    /**
     * 准备方法：可以执行多次，必须是幂等的
     * 默认实现返回成功，子类可以重写以添加特定逻辑
     * @param state 运行时状态
     * @return Status 准备状态
     */
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    
    /**
     * 打开方法：可以执行多次，必须是幂等的
     * 默认实现返回成功，子类可以重写以添加特定逻辑
     * @param state 运行时状态
     * @return Status 打开状态
     */
    Status open(RuntimeState* state) override { return Status::OK(); }

    /**
     * 终止方法：优雅地终止执行
     * 子类必须实现，用于清理资源和设置终止状态
     * @param state 运行时状态
     * @return Status 终止状态
     */
    Status terminate(RuntimeState* state) override;
    
    /**
     * 关闭方法：清理资源和设置关闭状态
     * 子类必须实现，确保资源正确释放
     * @param state 运行时状态
     * @param exec_status 执行状态
     * @return Status 关闭状态
     */
    Status close(RuntimeState* state, Status exec_status) override;

    /**
     * 调试字符串：生成调试信息
     * 子类必须实现，用于调试和日志记录
     * @param indentation_level 缩进级别
     * @return std::string 调试信息
     */
    [[nodiscard]] std::string debug_string(int indentation_level) const override;

    /**
     * 名称后缀：返回操作符特定的名称后缀
     * 子类可以重写以提供更有意义的名称
     * @return std::string 名称后缀
     */
    virtual std::string name_suffix();

    /**
     * 获取依赖关系列表
     * 返回当前实例的所有依赖关系，用于协调执行顺序
     * @return std::vector<Dependency*> 依赖关系列表
     */
    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }

    /**
     * 检查是否必须设置共享状态
     * 根据模板参数类型判断是否需要设置共享状态
     * - 如果 SharedStateArg 是 FakeSharedState，返回 false
     * - 否则返回 true，表示必须设置共享状态
     * @return bool 是否必须设置共享状态
     */
    virtual bool must_set_shared_state() const {
        return !std::is_same_v<SharedStateArg, FakeSharedState>;
    }

protected:
    // ========== 依赖关系管理 ==========
    
    /**
     * 主要依赖关系：管理当前实例的主要执行依赖
     * 用于协调与其他操作符或实例的执行顺序
     */
    Dependency* _dependency = nullptr;
    SharedStateType* _shared_state = nullptr;
};

class DataSinkOperatorXBase : public OperatorBase {
public:
    DataSinkOperatorXBase(const int operator_id, const int node_id, const int dest_id)
            : _operator_id(operator_id), _node_id(node_id), _dests_id({dest_id}) {}
    DataSinkOperatorXBase(const int operator_id, const TPlanNode& tnode, const int dest_id)
            : OperatorBase(tnode.__isset.is_serial_operator && tnode.is_serial_operator),
              _operator_id(operator_id),
              _node_id(tnode.node_id),
              _dests_id({dest_id}) {}

    DataSinkOperatorXBase(const int operator_id, const int node_id, std::vector<int>& dests)
            : _operator_id(operator_id), _node_id(node_id), _dests_id(dests) {}

#ifdef BE_TEST
    DataSinkOperatorXBase() : _operator_id(-1), _node_id(0), _dests_id({-1}) {};
#endif

    ~DataSinkOperatorXBase() override = default;

    // For agg/sort/join sink.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);

    Status init(const TDataSink& tsink) override;
    [[nodiscard]] virtual Status init(RuntimeState* state, ExchangeType type, const int num_buckets,
                                      const bool use_global_hash_shuffle,
                                      const std::map<int, int>& shuffle_idx_to_instance_idx) {
        return Status::InternalError("init() is only implemented in local exchange!");
    }

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status terminate(RuntimeState* state) override;
    [[nodiscard]] bool is_finished(RuntimeState* state) const {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->is_finished();
    }

    [[nodiscard]] virtual Status sink(RuntimeState* state, vectorized::Block* block, bool eos) = 0;

    [[nodiscard]] virtual Status setup_local_state(RuntimeState* state,
                                                   LocalSinkStateInfo& info) = 0;

    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state, bool eos) {
        return state->minimum_operator_memory_required_bytes();
    }
    bool is_blockable(RuntimeState* state) const override {
        return state->get_sink_local_state()->is_blockable();
    }

    [[nodiscard]] bool is_spillable() const { return _spillable; }

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    [[nodiscard]] virtual std::shared_ptr<BasicSharedState> create_shared_state() const = 0;

    Status close(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const;

    [[nodiscard]] virtual std::string debug_string(RuntimeState* state,
                                                   int indentation_level) const;

    [[nodiscard]] bool is_sink() const override { return true; }

    static Status close(RuntimeState* state, Status exec_status) {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->close(state, exec_status);
    }

    [[nodiscard]] int operator_id() const { return _operator_id; }

    [[nodiscard]] const std::vector<int>& dests_id() const { return _dests_id; }

    [[nodiscard]] int nereids_id() const { return _nereids_id; }

    [[nodiscard]] int node_id() const override { return _node_id; }

    [[nodiscard]] std::string get_name() const override { return _name; }

    virtual bool should_dry_run(RuntimeState* state) { return false; }

    [[nodiscard]] virtual bool count_down_destination() { return true; }

protected:
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;
    // _operator_id : the current Operator's ID, which is not visible to the user.
    // _node_id : the plan node ID corresponding to the Operator, which is visible on the profile.
    // _dests_id : the target _operator_id of the sink, for example, in the case of a multi-sink, there are multiple targets.
    const int _operator_id;
    const int _node_id;
    int _nereids_id = -1;
    bool _spillable = false;
    std::vector<int> _dests_id;
    std::string _name;
};

template <typename LocalStateType>
class DataSinkOperatorX : public DataSinkOperatorXBase {
public:
    DataSinkOperatorX(const int id, const int node_id, const int dest_id)
            : DataSinkOperatorXBase(id, node_id, dest_id) {}
    DataSinkOperatorX(const int id, const TPlanNode& tnode, const int dest_id)
            : DataSinkOperatorXBase(id, tnode, dest_id) {}

    DataSinkOperatorX(const int id, const int node_id, std::vector<int> dest_ids)
            : DataSinkOperatorXBase(id, node_id, dest_ids) {}
#ifdef BE_TEST
    DataSinkOperatorX() = default;
#endif
    ~DataSinkOperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;
    std::shared_ptr<BasicSharedState> create_shared_state() const override;

    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_sink_local_state()->template cast<LocalState>();
    }
};

/**
 * PipelineXSpillSinkLocalState：支持 Spill 的 Sink 操作符本地状态基类
 * 
 * 【核心功能】
 * 1. Spill 支持：为支持 Spill 的 Sink 操作符提供统一的性能监控和统计
 * 2. 性能监控：提供完整的 Spill 相关性能计数器（时间、数据量、任务数等）
 * 3. 分区统计：跟踪每个分区的行数，支持最大最小行数统计
 * 
 * 【设计理念】
 * - 继承扩展：继承自 PipelineXSinkLocalState，添加 Spill 相关功能
 * - 统一接口：为所有支持 Spill 的 Sink 操作符提供统一的性能监控接口
 * - 模板化：通过模板参数 SharedStateArg 支持不同类型的共享状态
 * 
 * 【与基类的区别】
 * - PipelineXSinkLocalState：普通的 Sink 本地状态，不支持 Spill
 * - PipelineXSpillSinkLocalState：支持 Spill 的 Sink 本地状态，添加了 Spill 性能计数器
 * 
 * 【使用场景】
 * - PartitionedHashJoinSinkLocalState：分区 Hash Join，支持按分区 Spill
 * - SpillSortSinkLocalState：排序操作，支持 Spill 到磁盘
 * - PartitionedAggregationSinkLocalState：分区聚合，支持 Spill
 * 
 * 【模板参数】
 * @tparam SharedStateArg 共享状态类型，例如：
 *   - PartitionedHashJoinSharedState：分区 Hash Join 的共享状态
 *   - SpillSortSharedState：排序 Spill 的共享状态
 */
template <typename SharedStateArg>
class PipelineXSpillSinkLocalState : public PipelineXSinkLocalState<SharedStateArg> {
public:
    using Base = PipelineXSinkLocalState<SharedStateArg>;
    
    /**
     * 构造函数：初始化支持 Spill 的 Sink 本地状态
     * 
     * @param parent 指向父操作符的指针
     * @param state 运行时状态
     */
    PipelineXSpillSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    ~PipelineXSpillSinkLocalState() override = default;

    /**
     * 初始化：调用基类初始化，并初始化 Spill 性能计数器
     * 
     * 【工作流程】
     * 1. 调用基类的 init 方法（初始化基础功能）
     * 2. 初始化所有 Spill 相关的性能计数器
     * 
     * @param state RuntimeState
     * @param info 本地状态信息
     * @return Status
     */
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(Base::init(state, info));
        init_spill_counters();
        return Status::OK();
    }

    /**
     * 初始化 Spill 性能计数器
     * 
     * 【计数器分类】
     * 1. 总体时间：SpillTotalTime（总时间）
     * 2. 写入时间：SpillWriteTime（写入总时间）
     * 3. 任务统计：等待队列任务数、正在写入任务数
     * 4. 文件操作：写入文件时间、序列化 Block 时间
     * 5. 数据统计：Block 数量、数据大小、行数
     * 6. 分区统计：最大最小行数
     * 
     * 【用途】
     * 这些计数器用于性能监控和调试，帮助识别 Spill 性能瓶颈
     */
    void init_spill_counters() {
        _spill_total_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillTotalTime", 1);

        _spill_write_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTime", 1);

        _spill_write_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteTaskWaitInQueueCount", TUnit::UNIT, 1);
        _spill_writing_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillWriteTaskCount", TUnit::UNIT, 1);
        _spill_write_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTaskWaitInQueueTime", 1);

        _spill_write_file_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteFileTime", 1);

        _spill_write_serialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteSerializeBlockTime", 1);
        _spill_write_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                          "SpillWriteBlockCount", TUnit::UNIT, 1);
        _spill_write_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
        _spill_write_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillWriteRows", TUnit::UNIT, 1);

        // ===== 总体时间统计 =====
        // SpillTotalTime：Spill 操作的总时间，包括任务调度、序列化、写入、读取等所有时间
        _spill_total_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillTotalTime", 1);

        // ===== 写入时间统计 =====
        // SpillWriteTime：Spill 写入的总时间，包括序列化、写入文件、等待队列等时间
        _spill_write_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTime", 1);

        // ===== 任务统计 =====
        // SpillWriteTaskWaitInQueueCount：等待在队列中的 Spill 写入任务数量
        // 用于监控 Spill 任务的调度情况，如果这个值很大，说明 Spill 任务积压
        _spill_write_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteTaskWaitInQueueCount", TUnit::UNIT, 1);
        // SpillWriteTaskCount：正在执行的 Spill 写入任务数量
        // 用于监控并发执行的 Spill 任务数
        _spill_writing_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillWriteTaskCount", TUnit::UNIT, 1);
        // SpillWriteTaskWaitInQueueTime：Spill 写入任务在队列中等待的时间
        // 用于监控 Spill 任务的调度延迟
        _spill_write_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTaskWaitInQueueTime", 1);

        // ===== 文件操作时间统计 =====
        // SpillWriteFileTime：实际写入磁盘文件的时间
        // 不包括序列化、等待队列等时间，只包括磁盘 I/O 时间
        _spill_write_file_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteFileTime", 1);
        // SpillWriteSerializeBlockTime：序列化 Block 的时间
        // 将内存中的 Block 序列化为可写入磁盘的格式（如 PBlock）的时间
        _spill_write_serialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteSerializeBlockTime", 1);
        
        // ===== 数据统计 =====
        // SpillWriteBlockCount：写入的 Block 数量（原始 Block 数量）
        // 注意：一个大 Block 在写入磁盘时可能会被拆分成多个小 Block
        _spill_write_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                          "SpillWriteBlockCount", TUnit::UNIT, 1);
        // SpillWriteBlockBytes：写入的 Block 数据大小（内存格式，未压缩）
        // 这是序列化前的数据大小，用于评估数据量
        _spill_write_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
        // SpillWriteRows：写入的行数
        // 用于统计 Spill 的数据量
        _spill_write_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillWriteRows", TUnit::UNIT, 1);

        // ===== 分区统计 =====
        // SpillMaxRowsOfPartition：所有分区中最大的行数
        // 用于评估数据分布的均匀性，如果最大值和最小值差距很大，说明数据分布不均匀
        _spill_max_rows_of_partition = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillMaxRowsOfPartition", TUnit::UNIT, 1);
        // SpillMinRowsOfPartition：所有分区中最小的行数
        // 与最大值一起，用于评估数据分布的均匀性
        _spill_min_rows_of_partition = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillMinRowsOfPartition", TUnit::UNIT, 1);
    }

    /**
     * 获取依赖关系列表
     * 
     * 【用途】
     * 返回当前本地状态的所有依赖关系
     * 默认实现返回基类的依赖关系，子类可以重写以添加额外的依赖关系
     * 
     * @return std::vector<Dependency*> 依赖关系列表
     */
    std::vector<Dependency*> dependencies() const override {
        auto dependencies = Base::dependencies();
        return dependencies;
    }

    /**
     * 更新最大最小行数计数器
     * 
     * 【功能】
     * 遍历所有分区的行数（_rows_in_partitions），计算最大值和最小值
     * 并更新对应的性能计数器
     * 
     * 【用途】
     * 1. 评估数据分布的均匀性
     * 2. 识别数据倾斜问题
     * 3. 性能监控和优化
     * 
     * 【调用时机】
     * 通常在分区操作完成后调用，例如在 _partition_block 方法中
     * 
     * 【示例】
     * 假设有 16 个分区，行数分布如下：
     * Partition 0: 1000 行
     * Partition 1: 500 行
     * Partition 2: 2000 行
     * ...
     * Partition 15: 800 行
     * 
     * 调用后：
     * _spill_max_rows_of_partition = 2000
     * _spill_min_rows_of_partition = 500
     */
    void update_max_min_rows_counter() {
        int64_t max_rows = 0;
        int64_t min_rows = std::numeric_limits<int64_t>::max();

        for (auto rows : _rows_in_partitions) {
            if (rows > max_rows) {
                max_rows = rows;
            }
            if (rows < min_rows) {
                min_rows = rows;
            }
        }

        COUNTER_SET(_spill_max_rows_of_partition, max_rows);
        COUNTER_SET(_spill_min_rows_of_partition, min_rows);
    }

    // ===== 成员变量 =====
    
    /**
     * 每个分区的行数统计
     * 
     * 【用途】
     * 记录每个分区（Partition）中的行数，用于：
     * 1. 评估数据分布的均匀性
     * 2. 计算最大最小行数（update_max_min_rows_counter）
     * 3. 性能监控和调试
     * 
     * 【示例】
     * 假设有 16 个分区：
     * _rows_in_partitions[0] = 1000  // Partition 0 有 1000 行
     * _rows_in_partitions[1] = 500   // Partition 1 有 500 行
     * ...
     * _rows_in_partitions[15] = 800  // Partition 15 有 800 行
     * 
     * 【更新时机】
     * 在 _partition_block 方法中，每次分区操作后更新对应分区的行数
     */
    std::vector<int64_t> _rows_in_partitions;

    // ===== Spill 性能计数器 =====
    
    /**
     * Spill 总时间：包括 Spill 任务调度时间、序列化 Block 时间、写入磁盘文件时间、
     * 读取磁盘文件时间、反序列化 Block 时间等所有 Spill 相关操作的总时间
     * 
     * 【用途】
     * 监控 Spill 操作的整体性能，用于性能分析和优化
     */
    RuntimeProfile::Counter* _spill_total_timer = nullptr;

    /**
     * Spill 写入时间：包括序列化 Block 时间、写入磁盘文件时间、等待队列时间等
     * 
     * 【与 _spill_total_timer 的区别】
     * - _spill_total_timer：包括所有 Spill 操作（写入 + 读取）
     * - _spill_write_timer：只包括写入操作
     */
    RuntimeProfile::Counter* _spill_write_timer = nullptr;

    /**
     * Spill 写入任务等待队列计数：等待在队列中的 Spill 写入任务数量
     * 
     * 【用途】
     * 监控 Spill 任务的调度情况，如果这个值很大，说明 Spill 任务积压
     */
    RuntimeProfile::Counter* _spill_write_wait_in_queue_task_count = nullptr;
    
    /**
     * Spill 写入任务计数：正在执行的 Spill 写入任务数量
     * 
     * 【用途】
     * 监控并发执行的 Spill 任务数，用于评估 Spill 的并发度
     */
    RuntimeProfile::Counter* _spill_writing_task_count = nullptr;
    
    /**
     * Spill 写入任务等待队列时间：Spill 写入任务在队列中等待的时间
     * 
     * 【用途】
     * 监控 Spill 任务的调度延迟，如果这个值很大，说明 Spill 调度器繁忙
     */
    RuntimeProfile::Counter* _spill_write_wait_in_queue_timer = nullptr;

    /**
     * Spill 写入文件时间：实际写入磁盘文件的时间
     * 
     * 【用途】
     * 监控磁盘 I/O 性能，不包括序列化、等待队列等时间
     */
    RuntimeProfile::Counter* _spill_write_file_timer = nullptr;
    
    /**
     * Spill 写入序列化 Block 时间：序列化 Block 的时间
     * 
     * 【用途】
     * 监控序列化性能，将内存中的 Block 序列化为可写入磁盘的格式（如 PBlock）的时间
     */
    RuntimeProfile::Counter* _spill_write_serialize_block_timer = nullptr;
    
    /**
     * Spill 写入 Block 数量：写入的 Block 数量（原始 Block 数量）
     * 
     * 【注意】
     * 一个大 Block 在写入磁盘时可能会被拆分成多个小 Block
     * 这个计数器记录的是原始 Block 的数量，不是实际写入磁盘的 Block 数量
     * 
     * 【用途】
     * 统计 Spill 的数据块数量，用于评估数据量
     */
    RuntimeProfile::Counter* _spill_write_block_count = nullptr;
    
    /**
     * Spill 写入 Block 数据大小：写入的 Block 数据大小（内存格式，未压缩）
     * 
     * 【用途】
     * 统计 Spill 的数据量（字节），这是序列化前的数据大小
     * 用于评估 Spill 的数据量和内存释放效果
     */
    RuntimeProfile::Counter* _spill_write_block_data_size = nullptr;
    
    /**
     * Spill 写入行数：写入的行数
     * 
     * 【用途】
     * 统计 Spill 的数据行数，用于评估 Spill 的数据量
     */
    RuntimeProfile::Counter* _spill_write_rows_count = nullptr;
    
    /**
     * Spill 文件总大小：Spill 到磁盘的文件总大小（压缩后）
     * 
     * 【用途】
     * 统计实际写入磁盘的文件大小（压缩后），用于评估磁盘使用情况
     * 
     * 【与 _spill_write_block_data_size 的区别】
     * - _spill_write_block_data_size：内存格式的数据大小（未压缩）
     * - _spill_file_total_size：磁盘文件大小（压缩后）
     */
    RuntimeProfile::Counter* _spill_file_total_size = nullptr;

    /**
     * Spill 最大行数分区：所有分区中最大的行数
     * 
     * 【用途】
     * 评估数据分布的均匀性，如果最大值和最小值差距很大，说明数据分布不均匀
     * 与 _spill_min_rows_of_partition 一起使用，用于识别数据倾斜问题
     */
    RuntimeProfile::Counter* _spill_max_rows_of_partition = nullptr;
    
    /**
     * Spill 最小行数分区：所有分区中最小的行数
     * 
     * 【用途】
     * 评估数据分布的均匀性，与 _spill_max_rows_of_partition 一起使用
     * 用于识别数据倾斜问题和性能优化
     */
    RuntimeProfile::Counter* _spill_min_rows_of_partition = nullptr;
};

class OperatorXBase : public OperatorBase {
public:
    OperatorXBase(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                  const DescriptorTbl& descs)
            : OperatorBase(tnode.__isset.is_serial_operator && tnode.is_serial_operator),
              _operator_id(operator_id),
              _node_id(tnode.node_id),
              _type(tnode.node_type),
              _pool(pool),
              _tuple_ids(tnode.row_tuples),
              _row_descriptor(descs, tnode.row_tuples),
              _resource_profile(tnode.resource_profile),
              _limit(tnode.limit) {
        if (tnode.__isset.output_tuple_id) {
            _output_row_descriptor.reset(new RowDescriptor(descs, {tnode.output_tuple_id}));
            _output_row_descriptor =
                    std::make_unique<RowDescriptor>(descs, std::vector {tnode.output_tuple_id});
        }
        if (!tnode.intermediate_output_tuple_id_list.empty()) {
            // common subexpression elimination
            _intermediate_output_row_descriptor.reserve(
                    tnode.intermediate_output_tuple_id_list.size());
            for (auto output_tuple_id : tnode.intermediate_output_tuple_id_list) {
                _intermediate_output_row_descriptor.push_back(
                        RowDescriptor(descs, std::vector {output_tuple_id}));
            }
        }
    }

    OperatorXBase(ObjectPool* pool, int node_id, int operator_id)
            : OperatorBase(),
              _operator_id(operator_id),
              _node_id(node_id),
              _pool(pool),
              _limit(-1) {}

#ifdef BE_TEST
    OperatorXBase() : _operator_id(-1), _node_id(0), _limit(-1) {};
#endif
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);
    Status init(const TDataSink& tsink) override {
        throw Exception(Status::FatalError("should not reach here!"));
    }
    virtual Status init(ExchangeType type) {
        throw Exception(Status::FatalError("should not reach here!"));
    }
    [[noreturn]] virtual const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, _op_name);
    }
    [[nodiscard]] std::string get_name() const override { return _op_name; }
    [[nodiscard]] virtual bool need_more_input_data(RuntimeState* state) const { return true; }
    bool is_blockable(RuntimeState* state) const override {
        return state->get_sink_local_state()->is_blockable() || _blockable;
    }

    Status prepare(RuntimeState* state) override;

    Status terminate(RuntimeState* state) override;
    [[nodiscard]] virtual Status get_block(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) = 0;

    Status close(RuntimeState* state) override;

    [[nodiscard]] virtual const RowDescriptor& intermediate_row_desc() const {
        return _row_descriptor;
    }

    [[nodiscard]] const RowDescriptor& intermediate_row_desc(int idx) {
        if (idx == 0) {
            return intermediate_row_desc();
        }
        DCHECK((idx - 1) < _intermediate_output_row_descriptor.size());
        return _intermediate_output_row_descriptor[idx - 1];
    }

    [[nodiscard]] const RowDescriptor& projections_row_desc() const {
        if (_intermediate_output_row_descriptor.empty()) {
            return intermediate_row_desc();
        } else {
            return _intermediate_output_row_descriptor.back();
        }
    }

    size_t revocable_mem_size(RuntimeState* state) const override {
        return (_child and !is_source()) ? _child->revocable_mem_size(state) : 0;
    }

    // If this method is not overwrite by child, its default value is 1MB
    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state) {
        return state->minimum_operator_memory_required_bytes();
    }

    virtual std::string debug_string(int indentation_level = 0) const;

    virtual std::string debug_string(RuntimeState* state, int indentation_level = 0) const;

    virtual Status setup_local_state(RuntimeState* state, LocalStateInfo& info) = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    [[nodiscard]] OperatorPtr get_child() { return _child; }

    [[nodiscard]] vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    [[nodiscard]] vectorized::VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] virtual RowDescriptor& row_descriptor() { return _row_descriptor; }

    [[nodiscard]] int operator_id() const { return _operator_id; }
    [[nodiscard]] int node_id() const override { return _node_id; }
    [[nodiscard]] int nereids_id() const { return _nereids_id; }

    [[nodiscard]] int64_t limit() const { return _limit; }

    [[nodiscard]] const RowDescriptor& row_desc() const override {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }

    [[nodiscard]] const RowDescriptor* output_row_descriptor() {
        return _output_row_descriptor.get();
    }

    bool has_output_row_desc() const { return _output_row_descriptor != nullptr; }

    [[nodiscard]] virtual Status get_block_after_projects(RuntimeState* state,
                                                          vectorized::Block* block, bool* eos);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    Status do_projections(RuntimeState* state, vectorized::Block* origin_block,
                          vectorized::Block* output_block) const;
    void set_parallel_tasks(int parallel_tasks) { _parallel_tasks = parallel_tasks; }
    int parallel_tasks() const { return _parallel_tasks; }

    // To keep compatibility with older FE
    void set_serial_operator() { _is_serial_operator = true; }

    virtual void reset_reserve_mem_size(RuntimeState* state) {}

protected:
    template <typename Dependency>
    friend class PipelineXLocalState;
    friend class PipelineXLocalStateBase;
    friend class Scanner;
    const int _operator_id;
    const int _node_id; // unique w/in single plan tree
    int _nereids_id = -1;
    TPlanNodeType::type _type;
    ObjectPool* _pool = nullptr;
    std::vector<TupleId> _tuple_ids;

private:
    // The expr of operator set to private permissions, as cannot be executed concurrently,
    // should use local state's expr.
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<vectorized::VExprContextSPtrs> _intermediate_projections;

protected:
    RowDescriptor _row_descriptor;
    std::unique_ptr<RowDescriptor> _output_row_descriptor = nullptr;
    std::vector<RowDescriptor> _intermediate_output_row_descriptor;

    /// Resource information sent from the frontend.
    const TBackendResourceProfile _resource_profile;

    int64_t _limit; // -1: no limit

    uint32_t _debug_point_count = 0;
    std::atomic_uint32_t _bytes_per_row = 0;

    std::string _op_name;
    int _parallel_tasks = 0;

    //_keep_origin is used to avoid copying during projection,
    // currently set to false only in the nestloop join.
    bool _keep_origin = true;

    // _blockable is true if the operator contains expressions that may block execution
    bool _blockable = false;
};

template <typename LocalStateType>
class OperatorX : public OperatorXBase {
public:
    OperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
              const DescriptorTbl& descs)
            : OperatorXBase(pool, tnode, operator_id, descs) {}
    OperatorX(ObjectPool* pool, int node_id, int operator_id)
            : OperatorXBase(pool, node_id, operator_id) {};

#ifdef BE_TEST
    OperatorX() = default;
#endif

    ~OperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;
    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_local_state(operator_id())->template cast<LocalState>();
    }

    size_t get_reserve_mem_size(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        auto estimated_size = local_state.estimate_memory_usage();
        if (estimated_size < state->minimum_operator_memory_required_bytes()) {
            estimated_size = state->minimum_operator_memory_required_bytes();
        }
        if (!is_source() && _child) {
            auto child_reserve_size = _child->get_reserve_mem_size(state);
            estimated_size +=
                    std::max(state->minimum_operator_memory_required_bytes(), child_reserve_size);
        }
        return estimated_size;
    }

    void reset_reserve_mem_size(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        local_state.reset_estimate_memory_usage();

        if (!is_source() && _child) {
            _child->reset_reserve_mem_size(state);
        }
    }
};

/**
 * StreamingOperatorX indicates operators which always processes block in streaming way (one-in-one-out).
 */
template <typename LocalStateType>
class StreamingOperatorX : public OperatorX<LocalStateType> {
public:
    StreamingOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, operator_id, descs) {}

#ifdef BE_TEST
    StreamingOperatorX() = default;
#endif

    virtual ~StreamingOperatorX() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    virtual Status pull(RuntimeState* state, vectorized::Block* block, bool* eos) = 0;
};

/**
 * StatefulOperatorX indicates the operators with some states inside.
 *
 * Specifically, we called an operator stateful if an operator can determine its output by itself.
 * For example, hash join probe operator is a typical StatefulOperator. When it gets a block from probe side, it will hold this block inside (e.g. _child_block).
 * If there are still remain rows in probe block, we can get output block by calling `get_block` without any data from its child.
 * In a nutshell, it is a one-to-many relation between input blocks and output blocks for StatefulOperator.
 */
template <typename LocalStateType>
class StatefulOperatorX : public OperatorX<LocalStateType> {
public:
    StatefulOperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                      const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, operator_id, descs) {}
#ifdef BE_TEST
    StatefulOperatorX() = default;
#endif
    virtual ~StatefulOperatorX() = default;

    using OperatorX<LocalStateType>::get_local_state;

    [[nodiscard]] Status get_block(RuntimeState* state, vectorized::Block* block,
                                   bool* eos) override;

    [[nodiscard]] virtual Status pull(RuntimeState* state, vectorized::Block* block,
                                      bool* eos) const = 0;
    [[nodiscard]] virtual Status push(RuntimeState* state, vectorized::Block* input_block,
                                      bool eos) const = 0;
    bool need_more_input_data(RuntimeState* state) const override { return true; }
};

/**
 * 异步写入器接收器：专门为异步写入设计的中间层类
 * 
 * 设计理念：
 * 1. 模板化设计：通过模板参数 Writer 和 Parent 支持不同类型的写入器和操作符
 * 2. 异步执行：将同步的数据接收和异步的数据写入分离，提高系统吞吐量
 * 3. 依赖管理：管理异步写入和完成两个阶段的依赖关系
 * 4. 资源协调：协调多个写入操作，确保数据正确写入和资源正确释放
 * 
 * 模板参数说明：
 * @tparam Writer 必须是 AsyncResultWriter 的子类，用于实际的数据写入操作
 * @tparam Parent 父类类型，通常是 DataSinkOperatorX 的子类，用于访问操作符配置
 * 
 * 在 Stream Load 中的作用：
 * - OlapTableSinkOperator 使用此类，Writer 为 VTabletWriter
 * - 实现数据接收和写入的异步分离，提高性能
 * - 管理写入器的生命周期和依赖关系
 */
template <typename Writer, typename Parent>
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
class AsyncWriterSink : public PipelineXSinkLocalState<BasicSharedState> {
public:
    /**
     * 基类类型别名：使用 PipelineXSinkLocalState<BasicSharedState> 作为基类
     * 提供共享状态管理和基础依赖关系功能
     */
    using Base = PipelineXSinkLocalState<BasicSharedState>;
    
    /**
     * 构造函数：初始化异步写入器接收器
     * @param parent 指向父操作符的指针，用于访问操作符配置和元信息
     * @param state 运行时状态，包含查询执行的上下文信息
     */
    AsyncWriterSink(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _async_writer_dependency(nullptr) {
        // 创建完成依赖关系，用于协调写入完成阶段
        // 依赖关系名称包含操作符名称，便于调试和监控
        _finish_dependency =
                std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                             parent->get_name() + "_FINISH_DEPENDENCY", true);
    }

    /**
     * 初始化方法：执行一次性的初始化操作
     * 包括设置依赖关系、创建共享状态、初始化异步写入器等
     * @param state 运行时状态
     * @param info 本地状态信息，包含共享状态映射和父级 Profile
     * @return Status 初始化状态，成功返回 OK，失败返回错误状态
     */
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    /**
     * 打开方法：启动异步写入器，开始后台写入线程
     * 这是异步写入的关键步骤，会创建后台线程执行实际写入
     * @param state 运行时状态
     * @return Status 打开状态，成功返回 OK，失败返回错误状态
     */
    Status open(RuntimeState* state) override;

    /**
     * 数据接收方法：接收数据块并传递给异步写入器
     * 这是同步接口，但内部会异步处理数据写入
     * @param state 运行时状态
     * @param block 输入的数据块
     * @param eos 是否为最后一个数据块（End of Stream）
     * @return Status 接收状态，成功返回 OK，失败返回错误状态
     */
    Status sink(RuntimeState* state, vectorized::Block* block, bool eos);

    /**
     * 获取依赖关系列表：返回当前实例的所有依赖关系
     * 用于协调执行顺序，确保异步写入器正确启动
     * @return std::vector<Dependency*> 依赖关系列表
     */
    std::vector<Dependency*> dependencies() const override {
        return {_async_writer_dependency.get()};
    }
    
    /**
     * 关闭方法：清理资源和设置关闭状态
     * 确保异步写入器正确关闭，资源得到释放
     * @param state 运行时状态
     * @param exec_status 执行状态，用于判断是否需要回滚等操作
     * @return Status 关闭状态，成功返回 OK，失败返回错误状态
     */
    Status close(RuntimeState* state, Status exec_status) override;

    /**
     * 获取完成依赖关系：用于协调写入完成阶段
     * 当所有数据写入完成后，此依赖关系会被设置为就绪状态
     * @return Dependency* 完成依赖关系指针
     */
    Dependency* finishdependency() override { return _finish_dependency.get(); }

protected:
    // ========== 表达式上下文管理 ==========
    
    /**
     * 输出表达式上下文：存储输出表达式的执行上下文
     * 每个表达式都需要一个独立的上下文，避免并发访问冲突
     * 在 init 方法中会从父操作符克隆这些表达式上下文
     */
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    
    // ========== 异步写入器管理 ==========
    
    /**
     * 异步写入器：负责实际的数据写入操作
     * 类型由模板参数 Writer 决定，必须是 AsyncResultWriter 的子类
     * 在后台线程中执行，不会阻塞主线程的数据接收
     * 
     * 在 Stream Load 中的具体实现：
     * - Writer = VTabletWriter：负责将数据写入 Tablet
     * - 支持批量写入、内存表、文件写入等多种写入策略
     */
    std::unique_ptr<Writer> _writer;

    // ========== 依赖关系管理 ==========
    
    /**
     * 异步写入器依赖关系：管理异步写入器的启动和执行
     * 确保异步写入器在正确的时机启动，避免过早或过晚启动
     * 当依赖关系就绪时，异步写入器开始处理数据
     */
    std::shared_ptr<Dependency> _async_writer_dependency;
    
    /**
     * 完成依赖关系：管理写入完成阶段的协调
     * 当所有数据写入完成后，此依赖关系被设置为就绪状态
     * 用于通知上层操作符写入已完成，可以进行后续操作（如提交事务）
     * 
     * 在 Stream Load 中的作用：
     * - 协调多个写入实例的完成状态
     * - 确保所有数据都写入完成后才提交事务
     * - 支持事务的原子性提交
     */
    std::shared_ptr<Dependency> _finish_dependency;
};

#ifdef BE_TEST
class DummyOperatorLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    ENABLE_FACTORY_CREATOR(DummyOperatorLocalState);

    DummyOperatorLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<FakeSharedState>(state, parent) {
        _tmp_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                    "DummyOperatorDependency", true);
        _finish_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
        _filter_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
    }
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    ~DummyOperatorLocalState() = default;

    std::vector<Dependency*> dependencies() const override { return {_tmp_dependency.get()}; }
    std::vector<Dependency*> execution_dependencies() override {
        return {_filter_dependency.get()};
    }

private:
    std::shared_ptr<Dependency> _tmp_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
    std::shared_ptr<Dependency> _filter_dependency;
};

class DummyOperator final : public OperatorX<DummyOperatorLocalState> {
public:
    DummyOperator() : OperatorX<DummyOperatorLocalState>(nullptr, 0, 0) {}

    [[nodiscard]] bool is_source() const override { return true; }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        *eos = _eos;
        return Status::OK();
    }
    void set_low_memory_mode(RuntimeState* state) override { _low_memory_mode = true; }
    Status terminate(RuntimeState* state) override {
        _terminated = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState* state) const override { return _revocable_mem_size; }
    size_t get_reserve_mem_size(RuntimeState* state) override {
        return _disable_reserve_mem
                       ? 0
                       : OperatorX<DummyOperatorLocalState>::get_reserve_mem_size(state);
    }

private:
    friend class AssertNumRowsLocalState;
    bool _eos = false;
    bool _low_memory_mode = false;
    bool _terminated = false;
    size_t _revocable_mem_size = 0;
    bool _disable_reserve_mem = false;
};

class DummySinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
public:
    using Base = PipelineXSinkLocalState<BasicSharedState>;
    ENABLE_FACTORY_CREATOR(DummySinkLocalState);
    DummySinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {
        _tmp_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                    "DummyOperatorDependency", true);
        _finish_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
    }

    std::vector<Dependency*> dependencies() const override { return {_tmp_dependency.get()}; }
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    bool is_finished() const override { return _is_finished; }

private:
    std::shared_ptr<Dependency> _tmp_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
    std::atomic_bool _is_finished = false;
};

class DummySinkOperatorX final : public DataSinkOperatorX<DummySinkLocalState> {
public:
    DummySinkOperatorX(int op_id, int node_id, int dest_id)
            : DataSinkOperatorX<DummySinkLocalState>(op_id, node_id, dest_id) {}
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        return _return_eof ? Status::Error<ErrorCode::END_OF_FILE>("source have closed")
                           : Status::OK();
    }
    void set_low_memory_mode(RuntimeState* state) override { _low_memory_mode = true; }
    Status terminate(RuntimeState* state) override {
        _terminated = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState* state) const override { return _revocable_mem_size; }
    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override {
        return _disable_reserve_mem
                       ? 0
                       : DataSinkOperatorX<DummySinkLocalState>::get_reserve_mem_size(state, eos);
    }

private:
    bool _low_memory_mode = false;
    bool _terminated = false;
    std::atomic_bool _return_eof = false;
    size_t _revocable_mem_size = 0;
    bool _disable_reserve_mem = false;
};
#endif

#include "common/compile_check_end.h"
} // namespace doris::pipeline
