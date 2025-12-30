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

#include <cstdint>

#include "common/be_mock_util.h"
#include "common/status.h"
#include "operator.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/exec/spill_utils.h"
#include "vec/core/block.h"
#include "vec/runtime/partitioner.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class PartitionedHashJoinSinkOperatorX;

/**
 * PartitionedHashJoinSinkLocalState：分区 Hash Join Sink 的本地状态
 * 
 * 【核心功能】
 * 1. 分区管理：将 Build 表的数据分成多个分区（Partition），每个分区独立管理
 * 2. Spill 支持：当内存不足时，可以将部分分区 Spill 到磁盘
 * 3. 内存管理：跟踪每个分区的内存使用，支持按分区回收内存
 * 
 * 【设计理念】
 * - 分区（Partitioned）：将数据分成多个分区，降低单个 Hash 表的内存占用
 * - 独立 Spill：每个分区可以独立 Spill，而不是整个 Hash 表一起 Spill
 * - 按需处理：只有超过内存限制的分区才需要 Spill
 * 
 * 【与普通 HashJoin 的区别】
 * - HashJoinBuildSinkLocalState：所有数据在一个 Hash 表中，无法部分 Spill
 * - PartitionedHashJoinSinkLocalState：数据分成多个分区，支持按分区 Spill
 * 
 * 【工作流程】
 * 1. 接收 Build 表数据（sink）
 * 2. 使用 Partitioner 将数据分区（_partition_block）
 * 3. 每个分区存储在 partitioned_build_blocks[i] 中
 * 4. 当内存不足时，选择部分分区 Spill 到磁盘（revoke_memory）
 * 5. Probe 阶段从内存和磁盘读取对应分区的数据
 */
class PartitionedHashJoinSinkLocalState
        : public PipelineXSpillSinkLocalState<PartitionedHashJoinSharedState> {
public:
    using Parent = PartitionedHashJoinSinkOperatorX;
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinSinkLocalState);
    ~PartitionedHashJoinSinkLocalState() override = default;
    
    // ===== 生命周期管理方法 =====
    
    /**
     * 初始化：设置分区相关的数据结构和性能计数器
     * - 初始化 partitioned_build_blocks 和 spilled_streams 数组
     * - 创建性能计数器（分区时间、Spill 时间等）
     */
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    
    /**
     * 打开：注册每个分区的 SpillStream，克隆 Partitioner
     * - 为每个分区注册 SpillStream（用于后续 Spill）
     * - 克隆 Partitioner（用于数据分区）
     */
    Status open(RuntimeState* state) override;
    
    /**
     * 关闭：清理资源，关闭内部算子
     */
    Status close(RuntimeState* state, Status exec_status) override;
    
    // ===== Spill 相关方法 =====
    
    /**
     * 回收内存：将部分分区 Spill 到磁盘
     * 
     * 【工作流程】
     * 1. 检查是否首次 Spill（处理未分区数据）
     * 2. 遍历所有分区，检查大小
     * 3. 对超过内存限制的分区进行 Spill
     * 4. 使用 SpillSinkRunnable 异步执行 Spill
     * 
     * @param state RuntimeState
     * @param spill_context Spill 上下文，用于协调多个任务的 Spill
     * @return Status
     */
    Status revoke_memory(RuntimeState* state, const std::shared_ptr<SpillContext>& spill_context);
    
    /**
     * 获取可回收的内存大小
     * 
     * 【返回值】
     * - 如果未 Spill：返回内部 HashJoin 的内存使用
     * - 如果已 Spill：返回所有分区中可 Spill 的数据大小（大于 MIN_SPILL_WRITE_BATCH_MEM）
     * 
     * @param state RuntimeState
     * @return 可回收的内存大小（字节）
     */
    size_t revocable_mem_size(RuntimeState* state) const;
    
    /**
     * 终止：优雅地终止执行
     */
    Status terminate(RuntimeState* state) override;
    
    /**
     * 获取需要预留的内存大小
     * 
     * 【用途】
     * 用于估算查询需要的内存，帮助内存管理决策
     * 
     * @param state RuntimeState
     * @param eos 是否为最后一个数据块
     * @return 需要预留的内存大小（字节）
     */
    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state, bool eos);
    
    /**
     * 更新内存使用统计
     * 
     * 【用途】
     * 更新性能计数器中的内存使用信息，用于监控和调试
     */
    void update_memory_usage();
    
    /**
     * 从内部算子更新性能 Profile
     * 
     * 【用途】
     * 将内部 HashJoin 算子的性能统计同步到当前 Profile
     */
    MOCK_FUNCTION void update_profile_from_inner();

    // ===== 依赖关系管理 =====
    
    /**
     * 获取完成依赖关系
     * 
     * 【用途】
     * 用于协调多个 Pipeline 的执行顺序
     * 当所有数据都处理完成后，通知下游 Pipeline 可以开始执行
     * 
     * @return Dependency* 完成依赖关系指针
     */
    Dependency* finishdependency() override;

    /**
     * 检查是否可以被阻塞
     * 
     * 【用途】
     * 返回 true 表示当前算子可以被阻塞（等待依赖关系）
     * 用于 Pipeline 调度器的调度决策
     * 
     * @return bool 是否可以被阻塞
     */
    bool is_blockable() const override;

protected:
    /**
     * 构造函数：初始化本地状态
     * 
     * @param parent 指向父操作符的指针
     * @param state 运行时状态
     */
    PartitionedHashJoinSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSpillSinkLocalState<PartitionedHashJoinSharedState>(parent, state) {}

    // ===== 内部方法（分区和 Spill 相关） =====
    
    /**
     * 将指定分区 Spill 到磁盘
     * 
     * 【工作流程】
     * 1. 将 MutableBlock 转换为 Block
     * 2. 通过 SpillStream 写入磁盘
     * 3. 清空原分区的数据（释放内存）
     * 
     * @param partition_index 分区索引（0 到 partition_count-1）
     * @param spilling_stream SpillStream 指针，用于写入磁盘
     * @return Status
     */
    Status _spill_to_disk(uint32_t partition_index,
                          const vectorized::SpillStreamSPtr& spilling_stream);

    /**
     * 对数据块进行分区
     * 
     * 【工作流程】
     * 1. 使用 Partitioner 计算每行数据属于哪个分区
     * 2. 根据分区结果，将数据分配到对应的 partitioned_build_blocks[i]
     * 3. 更新每个分区的行数统计
     * 
     * 【示例】
     * 假设有 1000 行数据，分成 16 个分区：
     * - 第 0-62 行 → Partition 0
     * - 第 63-125 行 → Partition 1
     * - ...
     * - 第 937-999 行 → Partition 15
     * 
     * @param state RuntimeState
     * @param in_block 输入数据块
     * @param begin 起始行索引（包含）
     * @param end 结束行索引（不包含）
     * @return Status
     */
    Status _partition_block(RuntimeState* state, vectorized::Block* in_block, size_t begin,
                            size_t end);

    /**
     * 处理未分区的数据块（首次 Spill 时）
     * 
     * 【用途】
     * 当首次触发 Spill 时，数据可能还没有分区
     * 需要先对数据进行分区，然后再 Spill
     * 
     * @param state RuntimeState
     * @param spill_context Spill 上下文
     * @return Status
     */
    Status _revoke_unpartitioned_block(RuntimeState* state,
                                       const std::shared_ptr<SpillContext>& spill_context);

    /**
     * 完成 Spill 操作
     * 
     * 【工作流程】
     * 对所有 SpillStream 调用 spill_eof，表示数据写入完成
     * 
     * @return Status
     */
    Status _finish_spilling();

    /**
     * 设置内部算子（HashJoinBuildSinkOperatorX 和 HashJoinProbeOperatorX）
     * 
     * 【用途】
     * PartitionedHashJoin 内部使用普通的 HashJoin 算子来处理每个分区
     * 这里创建内部算子的 RuntimeState 和 LocalState
     * 
     * @param state RuntimeState
     * @return Status
     */
    Status _setup_internal_operator(RuntimeState* state);

    friend class PartitionedHashJoinSinkOperatorX;

    // ===== 成员变量 =====
    
    /**
     * 子数据流是否结束（End of Stream）
     * 
     * 【用途】
     * 标记上游数据流是否已经结束
     * 当 EOS 为 true 时，表示不会再接收新数据，可以开始 Spill
     */
    bool _child_eos {false};

    /**
     * 分区器（Partitioner）
     * 
     * 【功能】
     * 根据 Join Key 的 Hash 值，将数据分配到不同的分区
     * 
     * 【分区算法】
     * - 使用 Crc32HashPartitioner
     * - 对 Join Key 计算 Hash 值
     * - Hash 值 % partition_count 得到分区索引
     * 
     * 【示例】
     * 假设 partition_count = 16，Join Key = "user_id"
     * - Hash("user_id_1") % 16 = 5 → Partition 5
     * - Hash("user_id_2") % 16 = 12 → Partition 12
     */
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;

    /**
     * 内部性能 Profile
     * 
     * 【用途】
     * 用于内部 HashJoin 算子的性能统计
     * 与外部 Profile 分离，便于管理和调试
     */
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    
    /**
     * 完成依赖关系
     * 
     * 【用途】
     * 用于协调内部算子的完成状态
     * 当内部算子完成时，通知外部可以继续执行
     */
    std::shared_ptr<Dependency> _finish_dependency;

    // ===== 性能计数器 =====
    
    /**
     * 分区时间：执行 do_partitioning 的时间
     * 
     * 【用途】
     * 监控分区操作的性能，用于性能优化
     */
    RuntimeProfile::Counter* _partition_timer = nullptr;
    
    /**
     * 分区 Shuffle 时间：将数据分配到不同分区的时间
     * 
     * 【用途】
     * 监控数据分配的性能，包括内存拷贝等操作
     */
    RuntimeProfile::Counter* _partition_shuffle_timer = nullptr;
    
    /**
     * Spill 构建时间：执行 Spill 操作的时间
     * 
     * 【用途】
     * 监控 Spill 操作的性能，包括数据序列化、压缩、写入磁盘等
     */
    RuntimeProfile::Counter* _spill_build_timer = nullptr;
    
    /**
     * 内存中的行数：Spill 完成后，仍然在内存中的行数
     * 
     * 【用途】
     * 统计有多少数据在内存中，有多少数据 Spill 到磁盘
     * 用于评估 Spill 的效果
     */
    RuntimeProfile::Counter* _in_mem_rows_counter = nullptr;
    
    /**
     * 内存使用预留：预留的内存大小
     * 
     * 【用途】
     * 跟踪为查询预留的内存，用于内存管理和监控
     */
    RuntimeProfile::Counter* _memory_usage_reserved = nullptr;
};

class PartitionedHashJoinSinkOperatorX
        : public JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState> {
public:
    PartitionedHashJoinSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id,
                                     const TPlanNode& tnode, const DescriptorTbl& descs,
                                     uint32_t partition_count);

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     PartitionedHashJoinSinkOperatorX::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    bool should_dry_run(RuntimeState* state) override { return false; }

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state,
                         const std::shared_ptr<SpillContext>& spill_context) override;

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }

        return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                               _join_distribution == TJoinDistributionType::COLOCATE
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE,
                                          _distribution_partition_exprs)
                       : DataDistribution(ExchangeType::HASH_SHUFFLE,
                                          _distribution_partition_exprs);
    }

    bool is_shuffled_operator() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }

    void set_inner_operators(const std::shared_ptr<HashJoinBuildSinkOperatorX>& sink_operator,
                             const std::shared_ptr<HashJoinProbeOperatorX>& probe_operator) {
        _inner_sink_operator = sink_operator;
        _inner_probe_operator = probe_operator;
    }

    bool require_data_distribution() const override {
        return _inner_probe_operator->require_data_distribution();
    }

private:
    friend class PartitionedHashJoinSinkLocalState;
#ifdef BE_TEST
    friend class PartitionedHashJoinSinkOperatorTest;
#endif

    /**
     * @brief Join 分布类型
     *
     * 【功能说明】
     * 决定 Join 操作的数据分布策略，影响数据如何在多个节点间分布。
     *
     * 【可能的值】
     * - PARTITIONED：分区分布，数据按 Join Key 的 Hash 值分区
     * - BUCKET_SHUFFLE：Bucket Shuffle，使用表的分桶信息进行 Shuffle
     * - COLOCATE：Colocate Join，数据已经在同一节点，无需 Shuffle
     * - BROADCAST：广播 Join，小表广播到所有节点
     * - NONE：无特殊分布要求
     *
     * 【使用场景】
     * - 在 `required_data_distribution()` 中使用，决定数据交换类型
     * - 在 `is_shuffled_operator()` 中使用，判断是否需要 Shuffle
     *
     * 【初始化】
     * 从 `tnode.hash_join_node.dist_type` 获取，如果不存在则默认为 NONE
     */
    const TJoinDistributionType::type _join_distribution;

    /**
     * @brief Build 表的 Join Key 表达式列表
     *
     * 【功能说明】
     * 存储 Build 表（右表）的 Join Key 表达式，用于：
     * 1. 初始化 Partitioner，将数据分配到不同分区
     * 2. 构建 Hash 表时的 Key 计算
     *
     * 【示例】
     * 如果 Join 条件是 `t1.id = t2.id`，则：
     * - `_build_exprs` 包含 `t2.id` 的表达式
     * - 用于计算每行数据的 Hash 值，决定数据属于哪个分区
     *
     * 【初始化】
     * 在 `init()` 方法中，从 `eq_join_conjuncts` 的 `right` 部分提取
     */
    std::vector<TExpr> _build_exprs;

    /**
     * @brief 内部的 HashJoinBuildSinkOperatorX
     *
     * 【功能说明】
     * 用于构建 Hash 表的内部 Sink 算子。
     * 当数据未 Spill 时，所有数据都传递给这个算子构建 Hash 表。
     *
     * 【使用场景】
     * - 未 Spill 时：调用 `_inner_sink_operator->sink()` 处理数据
     * - 已 Spill 时：不再使用此算子，数据直接分区处理
     *
     * 【初始化】
     * 通过 `set_inner_operators()` 方法设置，由外部创建并传入
     */
    std::shared_ptr<HashJoinBuildSinkOperatorX> _inner_sink_operator;

    /**
     * @brief 内部的 HashJoinProbeOperatorX
     *
     * 【功能说明】
     * 用于 Probe 阶段的内部算子。
     * 在 Probe 阶段，使用此算子对 Probe 表（左表）进行 Hash Join。
     *
     * 【使用场景】
     * - 在 Probe 阶段使用，对每个分区分别进行 Hash Join
     * - 每个分区有独立的 Hash 表，使用对应的 Probe 算子进行 Join
     *
     * 【初始化】
     * 通过 `set_inner_operators()` 方法设置，由外部创建并传入
     */
    std::shared_ptr<HashJoinProbeOperatorX> _inner_probe_operator;

    /**
     * @brief 数据分布分区表达式
     *
     * 【功能说明】
     * 用于数据分布（Shuffle）的表达式列表。
     * 决定数据如何在多个节点间分布，通常与 Join Key 相关但不完全相同。
     *
     * 【使用场景】
     * - 在 `required_data_distribution()` 中使用
     * - 决定数据交换（Exchange）的类型和分区表达式
     * - 例如：BUCKET_HASH_SHUFFLE 或 HASH_SHUFFLE
     *
     * 【初始化】
     * 从 `tnode.distribute_expr_lists[1]` 获取，如果不存在则为空
     *
     * 【与 _build_exprs 的区别】
     * - `_build_exprs`：用于本地分区（Partition），将数据分配到本地多个分区
     * - `_distribution_partition_exprs`：用于跨节点分布（Shuffle），将数据分布到不同节点
     */
    const std::vector<TExpr> _distribution_partition_exprs;

    /**
     * @brief 计划节点（Plan Node）
     *
     * 【功能说明】
     * 存储查询计划节点的信息，包含：
     * - Join 类型、Join 条件
     * - 数据分布类型
     * - 运行时过滤器配置
     * - 其他查询计划相关的信息
     *
     * 【使用场景】
     * - 在初始化时使用，提取配置信息
     * - 传递给内部算子，用于初始化
     * - 用于调试和日志记录
     */
    const TPlanNode _tnode;

    /**
     * @brief 描述符表（Descriptor Table）
     *
     * 【功能说明】
     * 包含查询中所有表的结构信息，包括：
     * - 表结构（列名、类型、位置等）
     * - 表元数据
     * - 列描述符
     *
     * 【使用场景】
     * - 在算子初始化时使用，获取表结构信息
     * - 用于数据序列化和反序列化
     * - 传递给内部算子，确保数据格式一致
     */
    const DescriptorTbl _descriptor_tbl;

    /**
     * @brief 分区数量
     *
     * 【功能说明】
     * 指定将 Build 表数据分成多少个分区。
     * 每个分区独立管理，可以单独 Spill 到磁盘。
     *
     * 【使用场景】
     * - 初始化 `partitioned_build_blocks` 和 `spilled_streams` 数组大小
     * - 初始化 Partitioner，设置分区数量
     * - 用于计算每行数据属于哪个分区（Hash 值 % partition_count）
     *
     * 【典型值】
     * - 通常为 2 的幂次，如 16、32、64
     * - 分区数量越多，单个分区的数据越少，但管理开销越大
     *
     * 【初始化】
     * 构造函数参数传入，由查询优化器（FE）根据数据量和内存限制计算
     */
    const uint32_t _partition_count;

    /**
     * @brief 分区器（Partitioner）
     *
     * 【功能说明】
     * 根据 Join Key 的 Hash 值，将数据分配到不同的分区。
     * 使用 Crc32HashPartitioner，对 `_build_exprs` 计算 Hash 值。
     *
     * 【分区算法】
     * 1. 对每行数据的 Join Key 计算 Hash 值
     * 2. Hash 值 % `_partition_count` 得到分区索引
     * 3. 将数据分配到对应的分区
     *
     * 【示例】
     * 假设 `_partition_count = 16`，Join Key = "user_id"：
     * - Hash("user_id_1") % 16 = 5 → Partition 5
     * - Hash("user_id_2") % 16 = 12 → Partition 12
     *
     * 【使用场景】
     * - 在 `_partition_block()` 中使用，对输入数据块进行分区
     * - 在首次 Spill 时使用，对未分区数据进行分区
     *
     * 【初始化】
     * 在 `init()` 方法中创建，使用 `_build_exprs` 初始化
     * 在 `prepare()` 方法中准备和打开
     */
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
