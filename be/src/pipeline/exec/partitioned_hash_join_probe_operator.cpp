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

#include "partitioned_hash_join_probe_operator.h"

#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : PipelineXSpillLocalState(state, parent),
          _child_block(vectorized::Block::create_unique()) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::init(state, info));
    init_spill_write_counters();

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _partitioned_blocks.resize(p._partition_count);
    _probe_spilling_streams.resize(p._partition_count);
    init_counters();
    return Status::OK();
}

void PartitionedHashJoinProbeLocalState::init_counters() {
    _partition_timer = ADD_TIMER(custom_profile(), "SpillPartitionTime");
    _partition_shuffle_timer = ADD_TIMER(custom_profile(), "SpillPartitionShuffleTime");
    _spill_build_rows = ADD_COUNTER(custom_profile(), "SpillBuildRows", TUnit::UNIT);
    _spill_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillBuildTime", 1);
    _recovery_build_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildRows", TUnit::UNIT);
    _recovery_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryBuildTime", 1);
    _spill_probe_rows = ADD_COUNTER(custom_profile(), "SpillProbeRows", TUnit::UNIT);
    _recovery_probe_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeRows", TUnit::UNIT);
    _spill_build_blocks = ADD_COUNTER(custom_profile(), "SpillBuildBlocks", TUnit::UNIT);
    _recovery_build_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildBlocks", TUnit::UNIT);
    _spill_probe_blocks = ADD_COUNTER(custom_profile(), "SpillProbeBlocks", TUnit::UNIT);
    _spill_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillProbeTime", 1);
    _recovery_probe_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeBlocks", TUnit::UNIT);
    _recovery_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryProbeTime", 1);
    _get_child_next_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "GetChildNextTime", 1);

    _probe_blocks_bytes =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "ProbeBloksBytesInMem", TUnit::BYTES, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_build_custom_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("BuildHashTableTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MergeBuildBlockTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("BuildTableInsertTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("BuildExprCallTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageBuildBlocks", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageHashTable", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageBuildKeyArena", custom_profile(),
                                               child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_build_common_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("MemoryUsage", common_profile(), child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_probe_custom_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("JoinFilterTimer", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("BuildOutputBlock", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("ProbeRows", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("ProbeExprCallTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenSearchHashTableTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenBuildSideOutputTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenProbeSideOutputTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("NonEqualJoinConjunctEvaluationTime",
                                               custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("InitProbeSideTime", custom_profile(),
                                               child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_probe_common_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("MemoryUsage", common_profile(), child_profile);
}

void PartitionedHashJoinProbeLocalState::update_profile_from_inner() {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    if (_shared_state->inner_runtime_state) {
        auto* sink_local_state = _shared_state->inner_runtime_state->get_sink_local_state();
        auto* probe_local_state = _shared_state->inner_runtime_state->get_local_state(
                p._inner_probe_operator->operator_id());
        if (_shared_state->is_spilled) {
            update_build_custom_profile<true>(sink_local_state->custom_profile());
            update_probe_custom_profile<true>(probe_local_state->custom_profile());
            update_build_common_profile<true>(sink_local_state->common_profile());
            update_probe_common_profile<true>(probe_local_state->common_profile());
        } else {
            update_build_custom_profile<false>(sink_local_state->custom_profile());
            update_probe_custom_profile<false>(probe_local_state->custom_profile());
            update_build_common_profile<false>(sink_local_state->common_profile());
            update_probe_common_profile<false>(probe_local_state->common_profile());
        }
    }
}

Status PartitionedHashJoinProbeLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::open(state));
    return _parent->cast<PartitionedHashJoinProbeOperatorX>()._partitioner->clone(state,
                                                                                  _partitioner);
}
Status PartitionedHashJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(PipelineXSpillLocalState::close(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::spill_probe_blocks(RuntimeState* state) {
    auto query_id = state->query_id();

    auto spill_func = [query_id, state, this] {
        SCOPED_TIMER(_spill_probe_timer);

        size_t not_revoked_size = 0;
        auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
        for (uint32_t partition_index = 0; partition_index != p._partition_count;
             ++partition_index) {
            auto& blocks = _probe_blocks[partition_index];
            auto& partitioned_block = _partitioned_blocks[partition_index];
            if (partitioned_block) {
                const auto size = partitioned_block->allocated_bytes();
                if (size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                    blocks.emplace_back(partitioned_block->to_block());
                    partitioned_block.reset();
                } else {
                    not_revoked_size += size;
                }
            }

            if (blocks.empty()) {
                continue;
            }

            auto& spilling_stream = _probe_spilling_streams[partition_index];
            if (!spilling_stream) {
                RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                        state, spilling_stream, print_id(state->query_id()), "hash_probe",
                        _parent->node_id(), std::numeric_limits<int32_t>::max(),
                        std::numeric_limits<size_t>::max(), operator_profile()));
            }

            auto merged_block = vectorized::MutableBlock::create_unique(std::move(blocks.back()));
            blocks.pop_back();

            while (!blocks.empty() && !state->is_cancelled()) {
                auto block = std::move(blocks.back());
                blocks.pop_back();

                RETURN_IF_ERROR(merged_block->merge(std::move(block)));
                DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks", {
                    return Status::Error<INTERNAL_ERROR>(
                            "fault_inject partitioned_hash_join_probe "
                            "spill_probe_blocks failed");
                });
            }

            if (!merged_block->empty()) [[likely]] {
                COUNTER_UPDATE(_spill_probe_rows, merged_block->rows());
                RETURN_IF_ERROR(
                        spilling_stream->spill_block(state, merged_block->to_block(), false));
                COUNTER_UPDATE(_spill_probe_blocks, 1);
            }
        }

        COUNTER_SET(_probe_blocks_bytes, int64_t(not_revoked_size));

        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " spill_probe_blocks done",
                print_id(query_id), p.node_id(), state->task_id());
        return Status::OK();
    };

    auto exception_catch_func = [query_id, state, spill_func]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "spill_probe_blocks canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION({ return spill_func(); }); }();
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe spill_probe_blocks "
                "submit_func failed");
    });

    SpillNonSinkRunnable spill_runnable(state, operator_profile(), exception_catch_func);
    return spill_runnable.run();
}

Status PartitionedHashJoinProbeLocalState::finish_spilling(uint32_t partition_index) {
    auto& probe_spilling_stream = _probe_spilling_streams[partition_index];

    if (probe_spilling_stream) {
        RETURN_IF_ERROR(probe_spilling_stream->spill_eof());
        probe_spilling_stream->set_read_counters(operator_profile());
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_disk(RuntimeState* state,
                                                                          uint32_t partition_index,
                                                                          bool& has_data) {
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " partition:{}, recover_build_blocks_from_disk",
            print_id(state->query_id()), _parent->node_id(), state->task_id(), partition_index);
    auto& spilled_stream = _shared_state->spilled_streams[partition_index];
    has_data = false;
    if (!spilled_stream) {
        return Status::OK();
    }
    spilled_stream->set_read_counters(operator_profile());

    auto query_id = state->query_id();

    auto read_func = [this, query_id, state, spilled_stream = spilled_stream, partition_index] {
        SCOPED_TIMER(_recovery_build_timer);

        bool eos = false;
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, recoverying build data",
                print_id(state->query_id()), _parent->node_id(), state->task_id(), partition_index);
        Status status;
        while (!eos) {
            vectorized::Block block;
            DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks", {
                status = Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_probe "
                        "recover_build_blocks failed");
            });
            if (status.ok()) {
                status = spilled_stream->read_next_block_sync(&block, &eos);
            }
            if (!status.ok()) {
                break;
            }
            COUNTER_UPDATE(_recovery_build_rows, block.rows());
            COUNTER_UPDATE(_recovery_build_blocks, 1);

            if (block.empty()) {
                continue;
            }

            if (UNLIKELY(state->is_cancelled())) {
                LOG(INFO) << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, recovery build data canceled",
                        print_id(state->query_id()), _parent->node_id(), state->task_id(),
                        partition_index);
                break;
            }

            if (!_recovered_build_block) {
                _recovered_build_block = vectorized::MutableBlock::create_unique(std::move(block));
            } else {
                DCHECK_EQ(_recovered_build_block->columns(), block.columns());
                status = _recovered_build_block->merge(std::move(block));
                if (!status.ok()) {
                    break;
                }
            }

            if (_recovered_build_block->allocated_bytes() >=
                vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }

        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            _shared_state->spilled_streams[partition_index].reset();
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery build data eos",
                    print_id(state->query_id()), _parent->node_id(), state->task_id(),
                    partition_index);
        }
        return status;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_build_blocks canceled");

            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    has_data = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_build_blocks submit_func failed");
                    });

    SpillRecoverRunnable spill_runnable(state, operator_profile(), exception_catch_func);
    return spill_runnable.run();
}

std::string PartitionedHashJoinProbeLocalState::debug_string(int indentation_level) const {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    bool need_more_input_data;
    if (_shared_state->is_spilled) {
        need_more_input_data = !_child_eos;
    } else if (_shared_state->inner_runtime_state) {
        need_more_input_data = p._inner_probe_operator->need_more_input_data(
                _shared_state->inner_runtime_state.get());
    } else {
        need_more_input_data = true;
    }
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, short_circuit_for_probe: {}, is_spilled: {}, child_eos: {}, "
                   "_shared_state->inner_runtime_state: {}, need_more_input_data: {}",
                   PipelineXSpillLocalState<PartitionedHashJoinSharedState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL",
                   _shared_state->is_spilled, _child_eos,
                   _shared_state->inner_runtime_state != nullptr, need_more_input_data);
    return fmt::to_string(debug_string_buffer);
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_disk(RuntimeState* state,
                                                                          uint32_t partition_index,
                                                                          bool& has_data) {
    auto& spilled_stream = _probe_spilling_streams[partition_index];
    has_data = false;
    if (!spilled_stream) {
        return Status::OK();
    }

    spilled_stream->set_read_counters(operator_profile());
    auto& blocks = _probe_blocks[partition_index];

    auto query_id = state->query_id();

    auto read_func = [this, query_id, partition_index, &spilled_stream, &blocks] {
        SCOPED_TIMER(_recovery_probe_timer);

        vectorized::Block block;
        bool eos = false;
        Status st;
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_probe recover_probe_blocks failed");
        });

        size_t read_size = 0;
        while (!eos && !_state->is_cancelled() && st.ok()) {
            st = spilled_stream->read_next_block_sync(&block, &eos);
            if (!st.ok()) {
                break;
            } else if (!block.empty()) {
                COUNTER_UPDATE(_recovery_probe_rows, block.rows());
                COUNTER_UPDATE(_recovery_probe_blocks, 1);
                read_size += block.allocated_bytes();
                blocks.emplace_back(std::move(block));
            }

            if (read_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }
        if (eos) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery probe data done",
                    print_id(query_id), _parent->node_id(), _state->task_id(), partition_index);
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            spilled_stream.reset();
        }
        return st;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_probe_blocks canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    has_data = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_probe_blocks submit_func failed");
                    });
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

bool PartitionedHashJoinProbeLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

PartitionedHashJoinProbeOperatorX::PartitionedHashJoinProbeOperatorX(ObjectPool* pool,
                                                                     const TPlanNode& tnode,
                                                                     int operator_id,
                                                                     const DescriptorTbl& descs,
                                                                     uint32_t partition_count)
        : JoinProbeOperatorX<PartitionedHashJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[0]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {}

Status PartitionedHashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX::init(tnode, state));
    _op_name = "PARTITIONED_HASH_JOIN_PROBE_OPERATOR";
    auto tnode_ = _tnode;
    tnode_.runtime_filters.clear();

    for (const auto& conjunct : tnode.hash_join_node.eq_join_conjuncts) {
        _probe_exprs.emplace_back(conjunct.left);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_probe_exprs));

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::prepare(RuntimeState* state) {
    // to avoid open _child twice
    auto child = std::move(_child);
    RETURN_IF_ERROR(JoinProbeOperatorX::prepare(state));
    RETURN_IF_ERROR(_inner_probe_operator->set_child(child));
    DCHECK(_build_side_child != nullptr);
    _inner_probe_operator->set_build_side_child(_build_side_child);
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_build_side_child));
    RETURN_IF_ERROR(_inner_probe_operator->prepare(state));
    RETURN_IF_ERROR(_inner_sink_operator->prepare(state));
    _child = std::move(child);
    RETURN_IF_ERROR(_partitioner->prepare(state, _child->row_desc()));
    RETURN_IF_ERROR(_partitioner->open(state));
    return Status::OK();
}

/**
 * 推送 Probe 数据并进行分区处理
 * 
 * 【功能说明】
 * 这是分区 Hash Join Probe 算子的 push 方法，负责接收 Probe 数据并进行分区处理。
 * 当 Build 侧已经 Spill 时，Probe 侧的数据也需要按照相同的分区规则进行分区，
 * 确保相同 Join Key 的数据在同一个分区中进行 Join。
 * 
 * 【核心工作】
 * 1. 对 Probe 数据进行分区（使用与 Build 侧相同的分区函数）
 * 2. 将分区后的数据存储到 `_partitioned_blocks[i]` 中
 * 3. 当分区块达到阈值（2M 行）或数据流结束（EOS）时，将数据移动到 `_probe_blocks[i]` 中
 * 4. 统计 Probe 数据的内存占用（用于内存管理和 Spill 决策）
 * 
 * 【数据结构】
 * 
 * - `_partitioned_blocks[i]`：临时存储分区后的数据（MutableBlock）
 *   - 用于累积分区数据，直到达到阈值或 EOS
 *   - 达到阈值后，转换为 Block 并移动到 `_probe_blocks[i]`
 * 
 * - `_probe_blocks[i]`：存储准备进行 Join 的数据块（Block 列表）
 *   - 每个分区可以有多个 Block（因为分批处理）
 *   - 这些 Block 会在 `pull()` 方法中用于与 Build 侧数据进行 Join
 * 
 * 【阈值设计】
 * 
 * 阈值：`2 * 1024 * 1024 = 2,097,152` 行
 * 
 * 当分区块达到阈值时，会立即移动到 `_probe_blocks[i]`：
 * - **原因 1**：控制内存占用，避免单个分区块过大
 * - **原因 2**：及时处理数据，避免延迟过高
 * - **原因 3**：便于后续的 Spill 操作（分批 Spill）
 * 
 * 【工作流程】
 * 
 * 1. **空数据处理**：
 *    - 如果输入数据为空（rows == 0）且 EOS，将 `_partitioned_blocks` 中剩余的数据移动到 `_probe_blocks`
 * 
 * 2. **分区计算**：
 *    - 使用 Partitioner 计算每行数据属于哪个分区（do_partitioning）
 *    - 收集每行数据的分区索引（partition_indexes）
 * 
 * 3. **数据写入**：
 *    - 将数据按分区写入 `_partitioned_blocks[i]`
 *    - 如果分区块达到阈值或 EOS，移动到 `_probe_blocks[i]`
 * 
 * 4. **内存统计**：
 *    - 统计所有 Probe 数据块的内存占用
 *    - 用于内存管理和 Spill 决策
 * 
 * 【参数说明】
 * @param state 运行时状态
 * @param input_block 输入的 Probe 数据块
 * @param eos 是否数据流结束
 * @return Status
 * 
 * 【示例】
 * 
 * 假设有 1000 行 Probe 数据，16 个分区，阈值 2M 行：
 * 
 * 1. 分区计算：
 *    - 第 0-62 行 → Partition 0
 *    - 第 63-125 行 → Partition 1
 *    - ...
 *    - 第 937-999 行 → Partition 15
 * 
 * 2. 数据写入：
 *    - Partition 0: 写入 `_partitioned_blocks[0]`（62 行 < 2M，保留）
 *    - Partition 1: 写入 `_partitioned_blocks[1]`（63 行 < 2M，保留）
 *    - ...
 * 
 * 3. 如果后续数据导致 Partition 0 达到 2M+1 行：
 *    - 将 `_partitioned_blocks[0]` 转换为 Block，移动到 `_probe_blocks[0]`
 *    - 清空 `_partitioned_blocks[0]`，准备接收新数据
 * 
 * 4. 如果 EOS：
 *    - 将所有 `_partitioned_blocks[i]` 中剩余的数据移动到 `_probe_blocks[i]`
 * 
 * 【注意事项】
 * 1. 分区函数必须与 Build 侧相同，确保相同 Join Key 的数据在同一分区
 * 2. 阈值（2M 行）是一个平衡点，既控制内存又避免过度分批
 * 3. `_probe_blocks[i]` 中的 Block 会在 `pull()` 方法中用于 Join
 * 4. 内存统计用于后续的 Spill 决策（`_should_revoke_memory()`）
 */
Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                               bool eos) const {
    // ===== 阶段 1：初始化和获取基本信息 =====
    auto& local_state = get_local_state(state);
    const auto rows = input_block->rows();
    auto& partitioned_blocks = local_state._partitioned_blocks;
    
    // ===== 阶段 2：处理空数据的情况 =====
    // 如果输入数据为空（rows == 0）
    if (rows == 0) {
        // 如果数据流结束（EOS），需要将 _partitioned_blocks 中剩余的数据移动到 _probe_blocks
        // 这样可以确保所有数据都被处理，不会遗漏
        if (eos) {
            for (uint32_t i = 0; i != _partition_count; ++i) {
                // 如果分区块存在且不为空，将其移动到 _probe_blocks
                if (partitioned_blocks[i] && !partitioned_blocks[i]->empty()) {
                    // 将 MutableBlock 转换为 Block，添加到 _probe_blocks[i]
                    local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
                    // 清空分区块（释放内存）
                    partitioned_blocks[i].reset();
                }
            }
        }
        return Status::OK();
    }
    
    // ===== 阶段 3：对 Probe 数据进行分区计算 =====
    // 使用与 Build 侧相同的 Partitioner，确保相同 Join Key 的数据在同一分区
    {
        SCOPED_TIMER(local_state._partition_timer);
        // do_partitioning 会计算每行数据属于哪个分区（0 到 partition_count-1）
        // 结果存储在 _partitioner->get_channel_ids() 中
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block));
    }

    // ===== 阶段 4：收集每个分区的行索引 =====
    // partition_indexes[i] 存储属于分区 i 的所有行在 input_block 中的索引
    // 例如：partition_indexes[5] = [0, 15, 32] 表示第 0、15、32 行属于分区 5
    std::vector<std::vector<uint32_t>> partition_indexes(_partition_count);
    const auto* channel_ids = local_state._partitioner->get_channel_ids().get<uint32_t>();
    // channel_ids[i] 表示 input_block 的第 i 行属于哪个分区
    // 将行索引 i 添加到对应分区的索引列表中
    for (uint32_t i = 0; i != rows; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    // ===== 阶段 5：将数据写入对应的分区块 =====
    SCOPED_TIMER(local_state._partition_shuffle_timer);
    int64_t bytes_of_blocks = 0;  // 用于统计 Probe 数据的内存占用
    
    for (uint32_t i = 0; i != _partition_count; ++i) {
        // ===== 步骤 5A：获取属于分区 i 的行数 =====
        const auto count = partition_indexes[i].size();
        
        // 如果该分区没有数据，跳过
        if (UNLIKELY(count == 0)) {
            continue;
        }

        // ===== 步骤 5B：如果分区块不存在，创建它 =====
        // 延迟创建：只有当分区有数据时才创建对应的 MutableBlock
        if (!partitioned_blocks[i]) {
            partitioned_blocks[i] =
                    vectorized::MutableBlock::create_unique(input_block->clone_empty());
        }
        
        // ===== 步骤 5C：将数据写入分区块 =====
        // 将 input_block 中属于分区 i 的所有行复制到 partitioned_blocks[i]
        // partition_indexes[i].data() 指向该分区行索引数组的起始位置
        // partition_indexes[i].data() + count 指向结束位置（不包含）
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(input_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));

        // ===== 步骤 5D：判断是否需要移动到 _probe_blocks =====
        // 条件 1：分区块达到阈值（2M 行）
        //   原因：控制内存占用，避免单个分区块过大
        // 条件 2：数据流结束（EOS）且分区块不为空
        //   原因：确保所有数据都被处理，不会遗漏
        if (partitioned_blocks[i]->rows() > 2 * 1024 * 1024 ||
            (eos && partitioned_blocks[i]->rows() > 0)) {
            // ===== 情况 A：移动到 _probe_blocks =====
            // 将 MutableBlock 转换为 Block，添加到 _probe_blocks[i]
            // _probe_blocks[i] 中的 Block 会在 pull() 方法中用于 Join
            local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
            // 清空分区块（释放内存），准备接收新数据
            partitioned_blocks[i].reset();
        } else {
            // ===== 情况 B：保留在 _partitioned_blocks =====
            // 如果未达到阈值且未 EOS，保留在 _partitioned_blocks 中
            // 继续累积数据，直到达到阈值或 EOS
            bytes_of_blocks += partitioned_blocks[i]->allocated_bytes();
        }

        // ===== 步骤 5E：统计 _probe_blocks 中的内存占用 =====
        // 遍历 _probe_blocks[i] 中的所有 Block，累加内存占用
        // 这些 Block 已经准备好进行 Join，需要统计其内存占用
        for (auto& block : local_state._probe_blocks[i]) {
            bytes_of_blocks += block.allocated_bytes();
        }
    }

    // ===== 阶段 6：更新内存统计 =====
    // 设置 Probe 数据块的内存占用统计（用于内存管理和 Spill 决策）
    // 这个值会被用于 _should_revoke_memory() 判断是否需要 Spill Probe 数据
    COUNTER_SET(local_state._probe_blocks_bytes, bytes_of_blocks);

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    local_state._shared_state->inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._shared_state->inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._shared_state->inner_runtime_state->set_be_number(state->be_number());

    local_state._shared_state->inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._shared_state->inner_runtime_state->resize_op_id_to_local_state(-1);
    local_state._shared_state->inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    // set sink local state
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = local_state._internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = local_state._in_mem_shared_state_sptr.get(),
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), info));

    LocalStateInfo state_info {.parent_profile = local_state._internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = local_state._in_mem_shared_state_sptr.get(),
                               .shared_state_map = {},
                               .task_idx = 0};
    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), state_info));

    auto* sink_local_state = local_state._shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state = local_state._shared_state->inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    auto& partitioned_block =
            local_state._shared_state->partitioned_build_blocks[local_state._partition_cursor];
    vectorized::Block block;
    if (partitioned_block && partitioned_block->rows() > 0) {
        block = partitioned_block->to_block();
        partitioned_block.reset();
    }
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::sink", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe sink failed");
    });

    RETURN_IF_ERROR(_inner_sink_operator->sink(local_state._shared_state->inner_runtime_state.get(),
                                               &block, true));
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " internal build operator finished, partition:{}, rows:{}, memory usage:{}",
            print_id(state->query_id()), node_id(), state->task_id(), local_state._partition_cursor,
            block.rows(),
            _inner_sink_operator->get_memory_usage(
                    local_state._shared_state->inner_runtime_state.get()));
    return Status::OK();
}

/**
 * 拉取 Join 结果（按分区处理）
 * 
 * 【功能说明】
 * 这是分区 Hash Join Probe 算子的 pull 方法，负责按分区顺序处理 Join 并返回结果。
 * 该方法采用"一个分区一个分区"的处理方式，确保每个分区的 Build 和 Probe 数据都准备好后再进行 Join。
 * 
 * 【核心工作】
 * 1. 按分区顺序处理（从分区 0 开始，依次处理到分区 partition_count-1）
 * 2. 恢复 Build 数据（从磁盘或内存）
 * 3. 恢复 Probe 数据（从磁盘或内存）
 * 4. 使用内部 Probe 算子进行 Join
 * 5. 返回 Join 结果
 * 
 * 【分区处理流程】
 * 
 * 每个分区的处理分为三个阶段：
 * 
 * 阶段 1：初始化阶段（`_need_to_setup_internal_operators == true`）
 * - 从磁盘恢复 Build 数据块（`recover_build_blocks_from_disk`）
 * - 完成 Spill（`finish_spilling`）
 * - 设置内部算子（`_setup_internal_operators`）
 * - 将 `_partitioned_blocks` 中的数据移动到 `_probe_blocks`
 * 
 * 阶段 2：Join 阶段（循环处理 Probe 数据）
 * - 如果 `_probe_blocks` 为空，从磁盘恢复 Probe 数据（`recover_probe_blocks_from_disk`）
 * - 从 `_probe_blocks` 中取出 Block，推送到内部 Probe 算子
 * - 内部 Probe 算子使用 Build 侧的 Hash 表进行 Join
 * 
 * 阶段 3：输出阶段
 * - 从内部 Probe 算子拉取 Join 结果
 * - 如果当前分区完成（`in_mem_eos == true`），移动到下一个分区
 * 
 * 【关键数据结构】
 * 
 * - `_partition_cursor`：当前处理的分区索引（0 到 partition_count-1）
 * - `_recovered_build_block`：从磁盘恢复的 Build 数据块（临时存储）
 * - `_probe_blocks[partition_index]`：当前分区的 Probe 数据块列表
 * - `_need_to_setup_internal_operators`：是否需要设置内部算子（每个分区开始时为 true）
 * 
 * 【数据恢复策略】
 * 
 * 1. **Build 数据恢复**：
 *    - 如果 `_recovered_build_block` 存在，先合并到 `partitioned_build_blocks[partition_index]`
 *    - 如果 `_need_to_setup_internal_operators == true`，从磁盘恢复 Build 数据
 *    - 恢复的数据用于构建 Hash 表
 * 
 * 2. **Probe 数据恢复**：
 *    - 如果 `_probe_blocks` 为空，从磁盘恢复 Probe 数据
 *    - 恢复的数据用于与 Build 侧进行 Join
 * 
 * 【分区切换逻辑】
 * 
 * 当当前分区完成（`in_mem_eos == true`）时：
 * - `_partition_cursor++`：移动到下一个分区
 * - `_need_to_setup_internal_operators = true`：标记需要设置内部算子
 * - 如果 `_partition_cursor == _partition_count`：所有分区处理完成，`*eos = true`
 * 
 * 【参数说明】
 * @param state 运行时状态
 * @param output_block 输出数据块（存储 Join 结果）
 * @param eos 是否数据流结束（输出参数）
 * @return Status
 * 
 * 【示例】
 * 
 * 假设有 16 个分区，当前处理分区 5：
 * 
 * 1. 初始化阶段（第一次调用）：
 *    - `_partition_cursor = 5`
 *    - `_need_to_setup_internal_operators = true`
 *    - 从磁盘恢复 Build 数据（分区 5）
 *    - 设置内部算子，构建 Hash 表
 *    - 将 `_partitioned_blocks[5]` 中的数据移动到 `_probe_blocks[5]`
 * 
 * 2. Join 阶段（循环调用）：
 *    - 从 `_probe_blocks[5]` 中取出 Block
 *    - 推送到内部 Probe 算子进行 Join
 *    - 从内部 Probe 算子拉取结果
 *    - 如果 `_probe_blocks[5]` 为空，从磁盘恢复更多 Probe 数据
 * 
 * 3. 分区完成：
 *    - `in_mem_eos = true`（当前分区完成）
 *    - `_partition_cursor++`（移动到分区 6）
 *    - `_need_to_setup_internal_operators = true`（标记需要设置内部算子）
 * 
 * 4. 所有分区完成：
 *    - `_partition_cursor == 16`（所有分区处理完成）
 *    - `*eos = true`（数据流结束）
 * 
 * 【注意事项】
 * 1. 分区按顺序处理（0, 1, 2, ..., partition_count-1）
 * 2. 每个分区独立处理，互不干扰
 * 3. Build 数据在分区开始时恢复，Probe 数据按需恢复
 * 4. 内部 Probe 算子会在每个分区开始时重新初始化
 */
Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state,
                                               vectorized::Block* output_block, bool* eos) const {
    // ===== 阶段 1：获取当前分区信息 =====
    auto& local_state = get_local_state(state);

    // 获取当前处理的分区索引（从 0 开始，依次处理到 partition_count-1）
    const auto partition_index = local_state._partition_cursor;
    // 获取当前分区的 Probe 数据块列表
    auto& probe_blocks = local_state._probe_blocks[partition_index];

    // ===== 阶段 2：处理恢复的 Build 数据块 =====
    // 如果之前从磁盘恢复了 Build 数据块（存储在 _recovered_build_block 中），
    // 需要将其合并到 partitioned_build_blocks[partition_index] 中
    // 这样后续构建 Hash 表时可以使用这些数据
    if (local_state._recovered_build_block && !local_state._recovered_build_block->empty()) {
        // 更新内存使用统计
        local_state._estimate_memory_usage += local_state._recovered_build_block->allocated_bytes();
        
        // 获取当前分区的 Build 数据块
        auto& mutable_block = local_state._shared_state->partitioned_build_blocks[partition_index];
        
        if (!mutable_block) {
            // ===== 情况 A：分区块不存在，直接移动 =====
            // 将恢复的 Build 数据块移动到 partitioned_build_blocks[partition_index]
            mutable_block = std::move(local_state._recovered_build_block);
        } else {
            // ===== 情况 B：分区块已存在，合并数据 =====
            // 将恢复的 Build 数据块合并到现有的分区块中
            RETURN_IF_ERROR(mutable_block->merge(local_state._recovered_build_block->to_block()));
            // 清空恢复的 Build 数据块（已合并，不再需要）
            local_state._recovered_build_block.reset();
        }
    }

    // ===== 阶段 3：初始化阶段（设置内部算子） =====
    // 如果 `_need_to_setup_internal_operators == true`，说明这是当前分区的第一次处理
    // 需要：
    // 1. 从磁盘恢复 Build 数据块
    // 2. 完成 Spill（标记 SpillStream 为 EOF）
    // 3. 设置内部算子（构建 Hash 表）
    // 4. 将 _partitioned_blocks 中的数据移动到 _probe_blocks
    if (local_state._need_to_setup_internal_operators) {
        // ===== 步骤 3A：从磁盘恢复 Build 数据块 =====
        // 如果 Build 数据已经 Spill 到磁盘，需要先恢复
        // recover_build_blocks_from_disk 会：
        // - 从 SpillStream 读取 Build 数据块
        // - 存储到 _recovered_build_block 中（后续会合并到 partitioned_build_blocks）
        bool has_data = false;
        RETURN_IF_ERROR(local_state.recover_build_blocks_from_disk(
                state, local_state._partition_cursor, has_data));
        
        // 如果还有数据需要恢复（has_data == true），直接返回
        // 下次调用时会继续恢复数据
        if (has_data) {
            return Status::OK();
        }

        // ===== 步骤 3B：完成 Spill =====
        // 标记当前分区的 Probe SpillStream 为 EOF（如果存在）
        // 这样可以确保后续恢复 Probe 数据时知道数据流已结束
        *eos = false;
        RETURN_IF_ERROR(local_state.finish_spilling(partition_index));
        
        // ===== 步骤 3C：设置内部算子 =====
        // _setup_internal_operators 会：
        // - 创建内部 RuntimeState
        // - 初始化内部 Sink 算子（构建 Hash 表）
        // - 初始化内部 Probe 算子（执行 Join）
        RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
        
        // 标记内部算子已设置（后续调用不再需要设置）
        local_state._need_to_setup_internal_operators = false;
        
        // ===== 步骤 3D：将 _partitioned_blocks 中的数据移动到 _probe_blocks =====
        // 如果 _partitioned_blocks[partition_index] 中有数据，将其移动到 _probe_blocks
        // 这样可以确保所有 Probe 数据都在 _probe_blocks 中，便于后续处理
        auto& mutable_block = local_state._partitioned_blocks[partition_index];
        if (mutable_block && !mutable_block->empty()) {
            probe_blocks.emplace_back(mutable_block->to_block());
        }
    }
    
    // ===== 阶段 4：Join 阶段（循环处理 Probe 数据） =====
    // 使用内部 Probe 算子进行 Join
    // 循环条件：内部 Probe 算子需要更多输入数据（need_more_input_data == true）
    bool in_mem_eos = false;
    auto* runtime_state = local_state._shared_state->inner_runtime_state.get();
    
    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        // ===== 步骤 4A：检查 Probe 数据是否为空 =====
        // 如果 _probe_blocks 为空，需要从磁盘恢复 Probe 数据
        if (probe_blocks.empty()) {
            *eos = false;
            bool has_data = false;
            
            // ===== 从磁盘恢复 Probe 数据 =====
            // recover_probe_blocks_from_disk 会：
            // - 从 SpillStream 读取 Probe 数据块
            // - 存储到 _probe_blocks[partition_index] 中
            RETURN_IF_ERROR(
                    local_state.recover_probe_blocks_from_disk(state, partition_index, has_data));
            
            if (!has_data) {
                // ===== 情况 A：没有更多数据 =====
                // 如果磁盘中没有更多数据（has_data == false），
                // 推送空 Block 和 EOS 标志给内部 Probe 算子
                // 这样可以通知内部 Probe 算子当前分区的 Probe 数据已结束
                vectorized::Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, has no data to recovery",
                        print_id(state->query_id()), node_id(), state->task_id(), partition_index);
                // 退出循环，准备输出结果
                break;
            } else {
                // ===== 情况 B：有数据需要恢复 =====
                // 如果磁盘中有数据（has_data == true），直接返回
                // 下次调用时会继续处理恢复的数据
                return Status::OK();
            }
        }

        // ===== 步骤 4B：从 _probe_blocks 中取出 Block =====
        // 从 _probe_blocks 的末尾取出一个 Block（使用 back() 和 pop_back()）
        // 这样可以按照 LIFO（后进先出）的顺序处理数据
        auto block = std::move(probe_blocks.back());
        probe_blocks.pop_back();
        
        // ===== 步骤 4C：推送 Probe 数据到内部 Probe 算子 =====
        // 如果 Block 不为空，推送到内部 Probe 算子进行 Join
        // 内部 Probe 算子会使用 Build 侧的 Hash 表进行 Join
        if (!block.empty()) {
            RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, false));
        }
    }

    // ===== 阶段 5：从内部 Probe 算子拉取 Join 结果 =====
    // 内部 Probe 算子已经处理完当前的 Probe 数据，可以拉取 Join 结果
    RETURN_IF_ERROR(_inner_probe_operator->pull(
            local_state._shared_state->inner_runtime_state.get(), output_block, &in_mem_eos));

    // ===== 阶段 6：处理分区完成逻辑 =====
    // 初始化 EOS 为 false（当前分区可能还未完成）
    *eos = false;
    
    // 如果内部 Probe 算子返回 in_mem_eos == true，说明当前分区的 Join 已完成
    if (in_mem_eos) {
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, probe done",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._partition_cursor);
        
        // ===== 步骤 6A：移动到下一个分区 =====
        // 当前分区处理完成，移动到下一个分区
        local_state._partition_cursor++;
        
        // ===== 步骤 6B：更新性能统计 =====
        // 从内部算子更新性能统计信息
        local_state.update_profile_from_inner();
        
        // ===== 步骤 6C：判断是否所有分区都处理完成 =====
        if (local_state._partition_cursor == _partition_count) {
            // ===== 情况 A：所有分区处理完成 =====
            // 如果 _partition_cursor == _partition_count，说明所有分区都已处理完成
            // 设置 *eos = true，表示数据流结束
            *eos = true;
        } else {
            // ===== 情况 B：还有更多分区需要处理 =====
            // 如果还有更多分区，标记需要设置内部算子
            // 下次调用时会处理下一个分区
            local_state._need_to_setup_internal_operators = true;
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->is_spilled) {
        return !local_state._child_eos;
    } else if (local_state._shared_state->inner_runtime_state) {
        return _inner_probe_operator->need_more_input_data(
                local_state._shared_state->inner_runtime_state.get());
    } else {
        return true;
    }
}

size_t PartitionedHashJoinProbeOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._child_eos) {
        return 0;
    }

    auto revocable_size = _revocable_mem_size(state, true);
    if (_child) {
        revocable_size += _child->revocable_mem_size(state);
    }
    return revocable_size;
}

size_t PartitionedHashJoinProbeOperatorX::_revocable_mem_size(RuntimeState* state,
                                                              bool force) const {
    const auto spill_size_threshold = force ? vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM
                                            : vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
    auto& local_state = get_local_state(state);
    size_t mem_size = 0;
    auto& probe_blocks = local_state._probe_blocks;
    for (uint32_t i = 0; i < _partition_count; ++i) {
        for (auto& block : probe_blocks[i]) {
            mem_size += block.allocated_bytes();
        }

        auto& partitioned_block = local_state._partitioned_blocks[i];
        if (partitioned_block) {
            auto block_bytes = partitioned_block->allocated_bytes();
            if (block_bytes >= spill_size_threshold) {
                mem_size += block_bytes;
            }
        }
    }
    return mem_size;
}

size_t PartitionedHashJoinProbeOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    const auto is_spilled = local_state._shared_state->is_spilled;
    if (!is_spilled || local_state._child_eos) {
        return Base::get_reserve_mem_size(state);
    }

    size_t size_to_reserve = vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;

    if (local_state._need_to_setup_internal_operators) {
        const size_t rows =
                (local_state._recovered_build_block ? local_state._recovered_build_block->rows()
                                                    : 0) +
                state->batch_size();
        size_t bucket_size = hash_join_table_calc_bucket_size(rows);

        size_to_reserve += bucket_size * sizeof(uint32_t); // JoinHashTable::first
        size_to_reserve += rows * sizeof(uint32_t);        // JoinHashTable::next

        if (_join_op == TJoinOp::FULL_OUTER_JOIN || _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
            _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN) {
            size_to_reserve += rows * sizeof(uint8_t); // JoinHashTable::visited
        }
    }

    COUNTER_SET(local_state._memory_usage_reserved, int64_t(size_to_reserve));
    return size_to_reserve;
}

Status PartitionedHashJoinProbeOperatorX::_revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    VLOG_DEBUG << fmt::format("Query:{}, hash join probe:{}, task:{}, revoke_memory",
                              print_id(state->query_id()), node_id(), state->task_id());

    RETURN_IF_ERROR(local_state.spill_probe_blocks(state));
    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::_should_revoke_memory(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->is_spilled) {
        const auto revocable_size = _revocable_mem_size(state);

        if (local_state.low_memory_mode()) {
            return revocable_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM;
        } else {
            return revocable_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
        }
    }
    return false;
}

/**
 * 获取 Probe 阶段的输出数据块
 * 
 * 【功能说明】
 * 这是分区 Hash Join Probe 算子的核心方法，负责：
 * 1. 从子节点（通常是 Scan 或 Filter）获取 Probe 数据
 * 2. 根据是否 Spill 选择不同的处理路径
 * 3. 执行 Hash Join 并返回结果
 * 
 * 【工作流程】
 * 这是一个拉取式（Pull-based）算子，采用两阶段处理：
 * 
 * 阶段 1：输入阶段（need_more_input_data == true）
 *   - 从子节点获取 Probe 数据块
 *   - 如果已 Spill：调用 push() 进行分区处理，检查是否需要 Spill Probe 数据
 *   - 如果未 Spill：直接调用内部 Probe 算子处理
 * 
 * 阶段 2：输出阶段（need_more_input_data == false）
 *   - 如果已 Spill：调用 pull() 从分区结果中获取数据
 *   - 如果未 Spill：从内部 Probe 算子获取结果
 *   - 更新统计信息
 * 
 * 【关键设计】
 * - 拉取式执行：只有当内部算子准备好输出时才拉取数据
 * - 双路径处理：根据是否 Spill 选择不同的处理路径
 * - 内存管理：在 Spill 模式下，检查内存水位并触发 Probe 数据的 Spill
 * 
 * 【示例】
 * 假设有 1000 行 Probe 数据，16 个分区：
 * 
 * 1. 输入阶段：
 *    - 从子节点获取 100 行数据
 *    - 如果已 Spill：调用 push()，将数据分区到 _probe_blocks[0-15]
 *    - 如果内存不足：触发 _revoke_memory()，Spill Probe 数据到磁盘
 * 
 * 2. 输出阶段：
 *    - 如果已 Spill：调用 pull()，从当前分区读取 Join 结果
 *    - 如果未 Spill：从内部 Probe 算子获取结果
 *    - 返回结果给上游算子
 * 
 * @param state RuntimeState
 * @param block 输出数据块（存储 Join 结果）
 * @param eos 是否数据流结束（输出参数）
 * @return Status
 */
Status PartitionedHashJoinProbeOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                    bool* eos) {
    // ===== 阶段 1：初始化和状态检查 =====
    // 初始化 EOS 标志为 false
    *eos = false;
    // 获取本地状态
    auto& local_state = get_local_state(state);
    // 复制共享的 Spill Profile（用于性能统计）
    local_state.copy_shared_spill_profile();
    // 检查是否已经进行过 Spill（决定处理路径）
    const auto is_spilled = local_state._shared_state->is_spilled;
    
#ifndef NDEBUG
    // 调试模式：在 EOS 时记录日志
    Defer eos_check_defer([&] {
        if (*eos) {
            LOG(INFO) << fmt::format(
                    "Query:{}, hash join probe:{}, task:{}, child eos:{}, need spill:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._child_eos, is_spilled);
        }
    });
#endif

    // 使用 Defer 确保在函数返回时更新内存使用统计
    Defer defer([&]() {
        COUNTER_SET(local_state._memory_usage_reserved,
                    int64_t(local_state.estimate_memory_usage()));
    });

    // ===== 阶段 2：输入阶段（需要更多输入数据） =====
    // need_more_input_data() 判断是否需要从子节点获取更多数据
    // - 已 Spill：如果子节点未结束（!_child_eos），需要更多数据
    // - 未 Spill：如果内部 Probe 算子需要更多数据，需要更多数据
    if (need_more_input_data(state)) {
        // 从子节点获取 Probe 数据块
        {
            SCOPED_TIMER(local_state._get_child_next_timer);
            RETURN_IF_ERROR(_child->get_block_after_projects(state, local_state._child_block.get(),
                                                             &local_state._child_eos));
        }

        SCOPED_TIMER(local_state.exec_time_counter());
        // 如果数据块为空且子节点未结束，直接返回（等待更多数据）
        if (local_state._child_block->rows() == 0 && !local_state._child_eos) {
            return Status::OK();
        }

        // 使用 Defer 确保在处理完成后清空数据块（释放内存）
        Defer clear_defer([&] { local_state._child_block->clear_column_data(); });
        
        // 根据是否 Spill 选择不同的处理路径
        if (is_spilled) {
            // ===== 路径 A：已 Spill，进行分区处理 =====
            // 调用 push() 方法，将 Probe 数据分区到不同的 _probe_blocks[i]
            // 这会根据 Join Key 的 Hash 值将数据分配到对应分区
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
            
            // 检查是否需要 Spill Probe 数据到磁盘
            // 如果 Probe 数据的内存使用超过阈值，触发 Spill
            if (_should_revoke_memory(state)) {
                return _revoke_memory(state);
            }
        } else {
            // ===== 路径 B：未 Spill，使用内部 Probe 算子 =====
            // 直接调用内部的 HashJoinProbeOperatorX 处理数据
            // 这会使用内存中的 Hash 表进行 Join
            DCHECK(local_state._shared_state->inner_runtime_state);
            RETURN_IF_ERROR(_inner_probe_operator->push(
                    local_state._shared_state->inner_runtime_state.get(),
                    local_state._child_block.get(), local_state._child_eos));
        }
    }

    // ===== 阶段 3：输出阶段（可以输出结果） =====
    // 如果不需要更多输入数据，说明内部算子已经准备好输出结果
    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
        
        // 根据是否 Spill 选择不同的输出路径
        if (is_spilled) {
            // ===== 路径 A：已 Spill，从分区结果中拉取数据 =====
            // 调用 pull() 方法，从当前分区的 Join 结果中获取数据
            // 这会处理一个分区的 Probe 数据，返回 Join 结果
            RETURN_IF_ERROR(pull(state, block, eos));
        } else {
            // ===== 路径 B：未 Spill，从内部 Probe 算子拉取结果 =====
            // 从内部的 HashJoinProbeOperatorX 获取 Join 结果
            RETURN_IF_ERROR(_inner_probe_operator->pull(
                    local_state._shared_state->inner_runtime_state.get(), block, eos));
            // 更新性能统计（从内部算子复制）
            local_state.update_profile_from_inner();
        }

        // ===== 阶段 4：更新统计信息 =====
        // 更新返回的行数统计
        local_state.add_num_rows_returned(block->rows());
        // 更新返回的块数统计
        COUNTER_UPDATE(local_state._blocks_returned_counter, 1);
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
