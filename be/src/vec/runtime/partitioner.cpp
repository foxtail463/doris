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

#include "partitioner.h"

#include "common/cast_set.h"
#include "pipeline/local_exchange/local_exchange_sink_operator.h"
#include "runtime/thread_context.h"
#include "vec/columns/column_const.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

/**
 * 对数据块进行分区计算
 * 
 * 【功能说明】
 * 对输入数据块（block）中的每一行数据，根据分区表达式（Join Key）计算 Hash 值，
 * 然后根据 Hash 值计算分区索引，将结果存储在 `_hash_vals` 中。
 * 
 * 【工作流程】
 * 1. 执行分区表达式，获取每行数据的 Join Key 列
 * 2. 对每行数据的 Join Key 计算 CRC32 Hash 值
 * 3. 使用 ChannelIds 策略（位旋转 + 取模）计算分区索引
 * 4. 清理临时列，保留原始列结构
 * 
 * 【示例】
 * 假设：
 * - Join Key: user_id（值为 "user_123"）
 * - partition_count = 16
 * - 数据块有 1000 行
 * 
 * 处理过程：
 * 1. 执行表达式，获取 user_id 列
 * 2. 对每行计算 CRC32("user_123") = 0x8A3B2C1D
 * 3. 位旋转 + 取模：((0x8A3B2C1D >> 16) | (0x8A3B2C1D << 16)) % 16 = 11
 * 4. _hash_vals[i] = 11（表示第 i 行属于 Partition 11）
 * 
 * 【关键设计】
 * - 使用 CRC32 Hash：快速且分布均匀
 * - ChannelIds 策略：SpillPartitionChannelIds 使用位旋转改善分布
 * - 多列 Join Key：依次对每列计算 Hash，最终合并
 * - 常量列优化：跳过常量列（所有行值相同，不影响分区）
 * 
 * @param state RuntimeState
 * @param block 输入数据块（会被修改，临时列会被删除）
 * @param eos 是否数据流结束（当前未使用）
 * @param already_sent 是否已发送（当前未使用）
 * @return Status
 */
template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::do_partitioning(RuntimeState* state, Block* block,
                                                         bool eos, bool* already_sent) const {
    // ===== 阶段 1：基本检查和初始化 =====
    // 获取数据块的行数
    size_t rows = block->rows();

    // 如果数据块为空，直接返回
    if (rows > 0) {
        // 记录需要保留的列数（用于后续清理临时列）
        auto column_to_keep = block->columns();

        // ===== 阶段 2：准备分区表达式执行结果 =====
        // result_size：分区表达式的数量（通常是 Join Key 的列数）
        // 例如：如果 Join Key 是 (user_id, order_id)，则 result_size = 2
        int result_size = cast_set<int>(_partition_expr_ctxs.size());
        // result：存储每个表达式执行后在 block 中的列索引
        // 例如：result[0] = 5 表示第一个表达式的结果在第 5 列
        std::vector<int> result(result_size);

        // ===== 阶段 3：初始化分区索引数组 =====
        // 为每行数据分配一个分区索引存储位置
        // 注意：虽然变量名叫 _hash_vals，但最终存储的是分区索引（0 到 partition_count-1），而不是原始 Hash 值
        _hash_vals.resize(rows);
        // 初始化为 0（后续会先存储 CRC32 Hash 值，最后转换为分区索引）
        std::fill(_hash_vals.begin(), _hash_vals.end(), 0);
        // 获取数组的指针（使用 __restrict 提示编译器优化）
        // 注意：hashes 在阶段 5 存储 Hash 值，在阶段 6 被覆盖为分区索引
        auto* __restrict hashes = _hash_vals.data();
        
        // ===== 阶段 4：执行分区表达式，获取 Join Key 列 =====
        // 执行所有分区表达式（通常是 Join Key 表达式）
        // 结果存储在 result 中，表示每个表达式对应的列索引
        // 例如：如果 Join Key 是 user_id，则执行表达式后，result[0] 是 user_id 列的索引
        { RETURN_IF_ERROR(_get_partition_column_result(block, result)); }
        
        // ===== 阶段 5：对每列计算 Hash 值 =====
        // 遍历每个分区表达式对应的列，依次计算 Hash 值
        // 对于多列 Join Key，每列的 Hash 值会累积到 hashes 数组中
        for (int j = 0; j < result_size; ++j) {
            // 获取第 j 个表达式对应的列
            // unpack_if_const：如果是常量列，返回常量值和标志
            const auto& [col, is_const] = unpack_if_const(block->get_by_position(result[j]).column);
            
            // 如果是常量列（所有行的值相同），跳过 Hash 计算
            // 原因：常量列不影响分区结果，所有行都会分到同一分区
            if (is_const) {
                continue;
            }
            
            // 对当前列的所有行计算 CRC32 Hash 值
            // hashes：存储每行的 Hash 值（会被更新）
            // j：列索引（用于多列 Join Key 的 Hash 累积）
            _do_hash(col, hashes, j);
        }

        // ===== 阶段 6：计算分区索引（覆盖 Hash 值） =====
        // 对每行的 Hash 值应用 ChannelIds 策略，计算最终的分区索引
        // ChannelIds()：调用 ChannelIds 的 operator()，例如：
        // - SpillPartitionChannelIds：((hash >> 16) | (hash << 16)) % partition_count
        // - ShuffleChannelIds：hash % partition_count
        // 
        // 【重要】这里会覆盖 hashes[i] 中的 Hash 值，替换为分区索引
        // 结果：hashes[i]（即 _hash_vals[i]）存储第 i 行所属的分区索引（0 到 partition_count-1）
        // 
        // 【示例】
        // 假设 hashes[0] = 0x8A3B2C1D（CRC32 Hash 值）
        // 经过 SpillPartitionChannelIds：((0x8A3B2C1D >> 16) | (0x8A3B2C1D << 16)) % 16 = 11
        // 最终：_hash_vals[0] = 11（分区索引）
        for (size_t i = 0; i < rows; i++) {
            hashes[i] = ChannelIds()(hashes[i], _partition_count);
        }

        // ===== 阶段 7：清理临时列 =====
        // 删除执行分区表达式时产生的临时列，保留原始列结构
        // 这些临时列是表达式执行的结果，不需要保留在原始 block 中
        { Block::erase_useless_column(block, column_to_keep); }
    }
    return Status::OK();
}

/**
 * 对列数据计算 CRC32 Hash 值
 * 
 * 【功能说明】
 * 对列（column）中的每一行数据计算 CRC32 Hash 值，并累积到 result 数组中。
 * 对于多列 Join Key，每列的 Hash 值会累积，最终得到组合 Hash 值。
 * 
 * 【工作流程】
 * 1. 调用列的 update_crcs_with_value 方法
 * 2. 对列中的每一行数据计算 CRC32 Hash
 * 3. 将 Hash 值累积到 result 数组中（result[i] 对应第 i 行的 Hash 值）
 * 
 * 【示例】
 * 假设列有 3 行数据：[100, 200, 300]
 * - 第 0 行：CRC32(100) → 累积到 result[0]
 * - 第 1 行：CRC32(200) → 累积到 result[1]
 * - 第 2 行：CRC32(300) → 累积到 result[2]
 * 
 * 【多列 Hash 累积】
 * 对于多列 Join Key（如 user_id, order_id）：
 * 1. 第一列：result[i] = CRC32(user_id[i])
 * 2. 第二列：result[i] = CRC32(result[i] + order_id[i])（累积）
 * 
 * @param column 要计算 Hash 的列
 * @param result Hash 值数组（会被更新，每行一个 Hash 值）
 * @param idx 列索引（用于多列 Join Key 的 Hash 累积）
 */
template <typename ChannelIds>
void Crc32HashPartitioner<ChannelIds>::_do_hash(const ColumnPtr& column,
                                                uint32_t* __restrict result, int idx) const {
    // 调用列的 update_crcs_with_value 方法计算 CRC32 Hash
    // result：Hash 值数组，每行一个元素（会被更新）
    // _partition_expr_ctxs[idx]->root()->data_type()->get_primitive_type()：列的数据类型
    // column->size()：列的行数
    column->update_crcs_with_value(
            result, _partition_expr_ctxs[idx]->root()->data_type()->get_primitive_type(),
            cast_set<uint32_t>(column->size()));
}

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32HashPartitioner<ChannelIds>(cast_set<int>(_partition_count));

    partitioner.reset(new_partitioner);
    new_partitioner->_partition_expr_ctxs.resize(_partition_expr_ctxs.size());
    for (size_t i = 0; i < _partition_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(
                _partition_expr_ctxs[i]->clone(state, new_partitioner->_partition_expr_ctxs[i]));
    }
    return Status::OK();
}

template class Crc32HashPartitioner<ShuffleChannelIds>;
template class Crc32HashPartitioner<SpillPartitionChannelIds>;

} // namespace doris::vectorized
