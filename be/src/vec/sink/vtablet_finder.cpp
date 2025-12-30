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

#include "vec/sink/vtablet_finder.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <glog/logging.h>

#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
Status OlapTabletFinder::find_tablets(RuntimeState* state, Block* block, int rows,
                                      std::vector<VOlapTablePartition*>& partitions,
                                      std::vector<uint32_t>& tablet_index, std::vector<bool>& skip,
                                      std::vector<int64_t>* miss_rows) {
    // 第一步：为每一行数据查找对应的分区
    // 遍历所有行，调用分区查找器为每行数据确定所属的分区
    for (int index = 0; index < rows; index++) {
        _vpartition->find_partition(block, index, partitions[index]);
    }

    // 准备存储合格行索引的向量，预分配内存以提高性能
    std::vector<uint32_t> qualified_rows;
    qualified_rows.reserve(rows);

    // 第二步：验证每一行的分区有效性，过滤掉不合格的行
    for (int row_index = 0; row_index < rows; row_index++) {
        // 检查分区是否为空（nullptr）
        if (partitions[row_index] == nullptr) [[unlikely]] {
            // 自动分区表的处理：如果提供了miss_rows参数，记录缺失分区的行索引
            if (miss_rows != nullptr) {          // auto partition table
                miss_rows->push_back(row_index); // already reserve memory outside
                skip[row_index] = true;
                continue;
            }

            // 非自动分区表的处理：记录过滤行数，设置过滤位图，并记录错误信息
            _num_filtered_rows++;
            _filter_bitmap.Set(row_index, true);
            skip[row_index] = true;

            // 将错误信息写入错误日志文件，包含具体的行数据内容
            RETURN_IF_ERROR(state->append_error_msg_to_file(
                    []() -> std::string { return ""; },
                    [&]() -> std::string {
                        fmt::memory_buffer buf;
                        fmt::format_to(buf, "no partition for this tuple. tuple={}",
                                       block->dump_data_json(row_index, 1));
                        return fmt::to_string(buf);
                    }));
            continue;
        }

        // 检查分区是否可变：不可变的分区（如历史分区）不允许写入
        if (!partitions[row_index]->is_mutable) [[unlikely]] {
            _num_immutable_partition_filtered_rows++;
            skip[row_index] = true;
            continue;
        }

        // 检查分区的桶数：桶数必须大于0，否则说明分区配置有问题
        if (partitions[row_index]->num_buckets <= 0) [[unlikely]] {
            std::stringstream ss;
            ss << "num_buckets must be greater than 0, num_buckets="
               << partitions[row_index]->num_buckets;
            return Status::InternalError(ss.str());
        }

        // 记录分区ID：将当前分区ID添加到分区ID集合中，用于统计和去重
        _partition_ids.emplace(partitions[row_index]->id);

        // 将合格的行索引添加到合格行向量中
        qualified_rows.push_back(row_index);
    }

    // 第三步：根据查找模式选择不同的tablet查找策略
    if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_ROW) {
        // 逐行查找模式：为每一行单独查找对应的tablet
        // 这种模式适用于需要精确控制每行数据分布的场景
        _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index);
    } else {
        // 批量查找模式：为整个批次查找tablet，支持随机分布策略
        // 使用分区到tablet的映射表来优化查找性能
        _vpartition->find_tablets(block, qualified_rows, partitions, tablet_index,
                                  &_partition_to_tablet_map);

        // 如果是每批次查找模式，需要更新分区负载均衡状态
        if (_find_tablet_mode == FindTabletMode::FIND_TABLET_EVERY_BATCH) {
            for (auto it : _partition_to_tablet_map) {
                // 为下一批次执行轮询负载均衡：递增每个分区的tablet索引
                if (it.first->load_tablet_idx != -1) {
                    it.first->load_tablet_idx++;
                }
            }
            // 清空分区到tablet的映射表，为下一批次做准备
            _partition_to_tablet_map.clear();
        }
    }

    // 返回成功状态
    return Status::OK();
}

} // namespace doris::vectorized
