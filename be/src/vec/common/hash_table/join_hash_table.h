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

#include <gen_cpp/PlanNodes_types.h>

#include <limits>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/common/custom_allocator.h"

namespace doris {
#include "common/compile_check_begin.h"

inline uint32_t hash_join_table_calc_bucket_size(size_t num_elem) {
    size_t expect_bucket_size = num_elem + (num_elem - 1) / 7;
    return (uint32_t)std::min(phmap::priv::NormalizeCapacity(expect_bucket_size) + 1,
                              static_cast<size_t>(std::numeric_limits<int32_t>::max()) + 1);
}

template <typename Key, typename Hash, bool DirectMapping>
class JoinHashTable {
public:
    using key_type = Key;
    using mapped_type = void*;
    using value_type = void*;
    size_t hash(const Key& x) const { return Hash()(x); }

    size_t get_byte_size() const {
        auto cal_vector_mem = [](const auto& vec) { return vec.capacity() * sizeof(vec[0]); };
        return cal_vector_mem(visited) + cal_vector_mem(first) + cal_vector_mem(next);
    }

    /**
     * 准备构建 Hash 表
     * 
     * 【功能说明】
     * 这是 Hash Join Build 阶段的准备工作，负责根据 Join 类型和参数分配和初始化 Hash 表的数据结构。
     * 在调用 `build()` 方法之前，必须先调用此方法进行初始化。
     * 
     * 【核心工作】
     * 1. 设置 Hash 表的基本参数（NULL 键标志、空表标志、批次大小）
     * 2. 计算 Hash 桶大小（根据 DirectMapping 模式选择不同的计算方式）
     * 3. 分配 Hash 表的核心数据结构（first、next 数组）
     * 4. 根据 Join 类型决定是否需要 visited 数组（用于跟踪 Build 行的匹配状态）
     * 
     * 【数据结构分配】
     * - first 数组：大小为 bucket_size + 1，用于存储每个 Hash 桶的第一个 Build 行索引
     *   - first[0..bucket_size-1]：正常 Hash 桶
     *   - first[bucket_size]：NULL 键的特殊 Hash 桶
     * - next 数组：大小为 num_elem，用于链式哈希（存储每个 Build 行的下一个 Build 行索引）
     * - visited 数组：大小为 num_elem（仅某些 Join 类型需要），用于标记 Build 行是否被匹配
     * 
     * 【visited 数组的作用】
     * visited 数组仅在以下 Join 类型中需要：
     * - RIGHT OUTER JOIN：需要知道哪些 Build 行被匹配了，后续输出未匹配的 Build 行
     * - FULL OUTER JOIN：需要同时跟踪 Build 行和 Probe 行的匹配状态
     * - RIGHT SEMI JOIN：只返回匹配的 Build 行，需要标记哪些 Build 行被匹配了
     * - RIGHT ANTI JOIN：只返回未匹配的 Build 行，需要标记哪些 Build 行被匹配了
     * 
     * 对于其他 Join 类型（INNER JOIN、LEFT OUTER JOIN、LEFT SEMI JOIN、LEFT ANTI JOIN），
     * 不需要 visited 数组，因为不需要输出未匹配的 Build 行。
     * 
     * 【Hash 桶大小计算】
     * - DirectMapping 模式：直接使用 force_bucket_size（由外部计算好的桶大小）
     * - 普通模式：使用 hash_join_table_calc_bucket_size(num_elem + 1) 计算
     *   - num_elem + 1：+1 是因为索引 0 是虚拟行，实际数据从索引 1 开始
     *   - 计算公式：expect_bucket_size = num_elem + (num_elem - 1) / 7
     *   - 目的是保持 Hash 表的负载因子在合理范围内，减少冲突
     * 
     * 【参数说明】
     * @param num_elem Build 数据的总行数（包括虚拟行 0，实际数据从索引 1 开始）
     * @param batch_size 批次大小（用于控制批量处理的规模）
     * @param has_null_key Build 数据中是否包含 NULL 键
     * @param force_bucket_size 强制指定的 Hash 桶大小（仅 DirectMapping 模式使用）
     * 
     * 【示例】
     * 
     * 示例 1：INNER JOIN
     * ```cpp
     * JoinHashTable<UInt64, HashCRC32<UInt64>, false> hash_table;
     * hash_table.prepare_build<TJoinOp::INNER_JOIN>(1000, 4064, false, 0);
     * // 结果：
     * // - first 数组：大小为 bucket_size + 1
     * // - next 数组：大小为 1000
     * // - visited 数组：不分配（INNER JOIN 不需要）
     * ```
     * 
     * 示例 2：RIGHT OUTER JOIN
     * ```cpp
     * hash_table.prepare_build<TJoinOp::RIGHT_OUTER_JOIN>(1000, 4064, false, 0);
     * // 结果：
     * // - first 数组：大小为 bucket_size + 1
     * // - next 数组：大小为 1000
     * // - visited 数组：大小为 1000（RIGHT OUTER JOIN 需要跟踪 Build 行的匹配状态）
     * ```
     * 
     * 【注意事项】
     * 1. 索引 0 是虚拟行（Mocked Row），不参与实际的 Hash 表构建
     * 2. _empty_build_side = true 表示 Build 表为空（只有虚拟行）
     * 3. visited 数组初始值为 0，表示所有 Build 行都未被匹配
     * 4. 在 Probe 阶段，如果匹配到 Build 行，会设置 visited[build_idx] = 1
     * 5. 在 finish_probing 阶段，会遍历 visited 数组，输出未匹配的 Build 行（visited[build_idx] == 0）
     */
    template <int JoinOpType>
    void prepare_build(size_t num_elem, int batch_size, bool has_null_key,
                       uint32_t force_bucket_size) {
        // ===== 步骤 1：设置基本参数 =====
        _has_null_key = has_null_key;

        // ===== 步骤 2：判断 Build 表是否为空 =====
        // the first row in build side is not really from build side table
        // 索引 0 是虚拟行，不参与实际的 Hash 表构建
        // 如果 num_elem <= 1，说明只有虚拟行，Build 表为空
        _empty_build_side = num_elem <= 1;
        
        // ===== 步骤 3：设置批次大小 =====
        max_batch_size = batch_size;
        
        // ===== 步骤 4：计算 Hash 桶大小 =====
        if constexpr (DirectMapping) {
            // DirectMapping 模式：直接使用外部计算好的桶大小
            // 桶大小 = max_key - min_key + 2（+2 是因为需要额外的空间）
            bucket_size = force_bucket_size;
        } else {
            // 普通模式：根据 Build 数据行数计算桶大小
            // num_elem + 1：+1 是因为索引 0 是虚拟行，实际数据从索引 1 开始
            // 计算公式：expect_bucket_size = num_elem + (num_elem - 1) / 7
            // 目的是保持 Hash 表的负载因子在合理范围内，减少冲突
            bucket_size = hash_join_table_calc_bucket_size(num_elem + 1);
        }
        
        // ===== 步骤 5：分配 Hash 表的核心数据结构 =====
        // first 数组：存储每个 Hash 桶的第一个 Build 行索引
        // 大小为 bucket_size + 1：
        // - first[0..bucket_size-1]：正常 Hash 桶
        // - first[bucket_size]：NULL 键的特殊 Hash 桶
        first.resize(bucket_size + 1);
        
        // next 数组：链式哈希，存储每个 Build 行的下一个 Build 行索引
        // 大小为 num_elem（包括虚拟行 0）
        next.resize(num_elem);

        // ===== 步骤 6：根据 Join 类型决定是否需要 visited 数组 =====
        // visited 数组用于跟踪 Build 行的匹配状态，仅在以下 Join 类型中需要：
        // - RIGHT OUTER JOIN：需要输出未匹配的 Build 行（Probe 侧填充 NULL）
        // - FULL OUTER JOIN：需要同时输出未匹配的 Build 行和 Probe 行
        // - RIGHT SEMI JOIN：只返回匹配的 Build 行，需要标记哪些 Build 行被匹配了
        // - RIGHT ANTI JOIN：只返回未匹配的 Build 行，需要标记哪些 Build 行被匹配了
        //
        // 对于其他 Join 类型（INNER JOIN、LEFT OUTER JOIN、LEFT SEMI JOIN、LEFT ANTI JOIN），
        // 不需要 visited 数组，因为不需要输出未匹配的 Build 行。
        if constexpr (JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
            // 分配 visited 数组，初始值为 0（表示所有 Build 行都未被匹配）
            // 在 Probe 阶段，如果匹配到 Build 行，会设置 visited[build_idx] = 1
            // 在 finish_probing 阶段，会遍历 visited 数组，输出未匹配的 Build 行（visited[build_idx] == 0）
            visited.resize(num_elem);
        }
    }

    uint32_t get_bucket_size() const { return bucket_size; }

    size_t size() const { return next.size(); }

    DorisVector<uint8_t>& get_visited() { return visited; }

    bool empty_build_side() const { return _empty_build_side; }

    /**
     * 构建 Hash 表的数据结构
     * 
     * 【功能说明】
     * 这是 Hash Join Build 阶段的核心方法，负责将 Build 数据插入到 Hash 表中。
     * 使用链式哈希（Chained Hashing）来处理 Hash 冲突。
     * 
     * 【数据结构】
     * Hash 表使用两个数组来实现链式哈希：
     * - first[bucket_num]：Hash 桶 bucket_num 中第一个 Build 行的索引
     * - next[build_idx]：Build 行 build_idx 在同一 Hash 桶中下一个 Build 行的索引
     * 
     * 【构建过程】
     * 1. 遍历所有 Build 行（从索引 1 开始，索引 0 是虚拟行）
     * 2. 对每个 Build 行，根据其 Hash 桶编号，插入到对应的 Hash 桶链表中
     * 3. 使用头插法：新插入的行成为链表的头节点
     * 
     * 【示例】
     * 假设 Build 表有 5 行数据（索引 1-5），Hash 值计算后：
     * - Build 行 1 → Hash 桶 3
     * - Build 行 2 → Hash 桶 1
     * - Build 行 3 → Hash 桶 3（与行 1 冲突）
     * - Build 行 4 → Hash 桶 1（与行 2 冲突）
     * - Build 行 5 → Hash 桶 2
     * 
     * 构建过程：
     * i=1: bucket_num=3 → next[1]=first[3]=0, first[3]=1
     *      Hash 桶 3: 1 → 0
     * 
     * i=2: bucket_num=1 → next[2]=first[1]=0, first[1]=2
     *      Hash 桶 1: 2 → 0
     * 
     * i=3: bucket_num=3 → next[3]=first[3]=1, first[3]=3
     *      Hash 桶 3: 3 → 1 → 0
     * 
     * i=4: bucket_num=1 → next[4]=first[1]=2, first[1]=4
     *      Hash 桶 1: 4 → 2 → 0
     * 
     * i=5: bucket_num=2 → next[5]=first[2]=0, first[2]=5
     *      Hash 桶 2: 5 → 0
     * 
     * 最终结果：
     * first[0] = 0
     * first[1] = 4  → next[4] = 2  → next[2] = 0
     * first[2] = 5  → next[5] = 0
     * first[3] = 3  → next[3] = 1  → next[1] = 0
     * 
     * 【参数说明】
     * @param keys Build 键值数组（已计算 Hash 值）
     * @param bucket_nums Build 行的 Hash 桶编号数组（bucket_nums[i] 表示 Build 行 i 的 Hash 桶编号）
     * @param num_elem Build 数据的总行数（包括虚拟行 0）
     * @param keep_null_key 是否保留 NULL 键（用于某些特殊的 Join 类型）
     * 
     * 【注意事项】
     * 1. 索引 0 是虚拟行，不参与构建（循环从 i=1 开始）
     * 2. 使用头插法插入，后插入的行在链表前面
     * 3. bucket_size 是特殊的 Hash 桶编号，用于存储 NULL 键
     * 4. 如果 keep_null_key=false，清空 NULL 键的 Hash 桶（first[bucket_size] = 0）
     */
    void build(const Key* __restrict keys, const uint32_t* __restrict bucket_nums,
               uint32_t num_elem, bool keep_null_key) {
        // ===== 步骤 1：保存 Build 键值数组的指针 =====
        // build_keys 用于后续的键值比较（在 Probe 阶段）
        build_keys = keys;
        
        // ===== 步骤 2：构建链式哈希表 =====
        // 遍历所有 Build 行（从索引 1 开始，索引 0 是虚拟行）
        for (uint32_t i = 1; i < num_elem; i++) {
            // ===== 步骤 2A：获取当前 Build 行的 Hash 桶编号 =====
            // bucket_nums[i] 是在调用 build 之前计算好的 Hash 桶编号
            // 计算方式：bucket_num = hash(keys[i]) & (bucket_size - 1)
            uint32_t bucket_num = bucket_nums[i];
            
            // ===== 步骤 2B：使用头插法插入到 Hash 桶链表中 =====
            // next[i] = first[bucket_num]：将当前 Build 行链接到链表的头部
            // 如果 first[bucket_num] == 0，表示 Hash 桶为空，next[i] = 0（链表结束）
            // 如果 first[bucket_num] != 0，表示 Hash 桶已有数据，next[i] 指向原来的头节点
            next[i] = first[bucket_num];
            
            // first[bucket_num] = i：更新 Hash 桶的头节点为当前 Build 行
            // 这样，新插入的行总是成为链表的头节点（头插法）
            first[bucket_num] = i;
        }
        
        // ===== 步骤 3：处理 NULL 键 =====
        // bucket_size 是特殊的 Hash 桶编号，用于存储 NULL 键
        // 如果 keep_null_key=false，清空 NULL 键的 Hash 桶
        // 这样，Probe 阶段的 NULL 键将无法匹配到任何 Build 行
        if (!keep_null_key) {
            first[bucket_size] = 0; // index = bucket_size means null
        }
        
        // ===== 步骤 4：保存 keep_null_key 标志 =====
        // 用于后续判断是否允许 NULL 键匹配
        _keep_null_key = keep_null_key;
    }

    /**
     * 批量查找匹配行的核心方法
     * 
     * 【功能说明】
     * 这是 Hash Join 的核心查找方法，负责在 Hash 表中批量查找 Probe 数据的匹配行。
     * 根据不同的 Join 类型和条件，路由到不同的内部查找方法。
     * 
     * 【工作流程】
     * 1. 检查特殊情况（NULL Aware Join + 空 Build 表）
     * 2. 检查是否有其他 Join 条件 → 调用 _find_batch_conjunct
     * 3. 检查是否是 Mark Join → 调用 _find_batch_conjunct（带优化）
     * 4. 根据 Join 类型路由：
     *    - INNER/OUTER JOIN → _find_batch_inner_outer_join
     *    - LEFT SEMI/ANTI JOIN → _find_batch_left_semi_anti
     *    - RIGHT SEMI/ANTI JOIN → _find_batch_right_semi_anti
     * 
     * 【参数说明】
     * @param keys Probe 键值数组（已计算 Hash 值）
     * @param build_idx_map Build 索引映射表（build_idx_map[probe_idx] 表示 Probe 行对应的 Hash 桶中的第一个 Build 行索引）
     * @param probe_idx 当前处理的 Probe 行索引（输入/输出参数）
     * @param build_idx 当前处理的 Build 行索引（用于一对多匹配，输入/输出参数）
     * @param probe_rows Probe 数据的总行数
     * @param probe_idxs 输出参数：存储匹配的 Probe 行索引数组
     * @param probe_visited 输出参数：Probe 行是否已访问（用于 OUTER JOIN）
     * @param build_idxs 输出参数：存储匹配的 Build 行索引数组
     * @param null_map NULL Map（标记哪些 Probe 行的键是 NULL）
     * @param with_other_conjuncts 是否有其他 Join 条件（如 WHERE 子句中的条件）
     * @param is_mark_join 是否是 Mark Join
     * @param has_mark_join_conjunct 是否有 Mark Join 条件
     * 
     * 【返回值】
     * 返回 std::tuple<probe_idx, build_idx, matched_cnt>
     * - probe_idx：处理到的 Probe 行索引（可能已处理多行）
     * - build_idx：处理到的 Build 行索引（用于一对多匹配，支持分批次处理）
     * - matched_cnt：当前批次匹配的行数
     * 
     * 【示例】
     * 假设 INNER JOIN，Probe 表有键值 ["A", "B", "C"]，Build 表有键值 ["A", "A", "B"]
     * 
     * 第一次调用：probe_idx=0, build_idx=0
     * - 查找键值 "A"，找到 Build 行 1、2（都是 "A"）
     * - 返回：probe_idx=1, build_idx=0, matched_cnt=2
     * - probe_idxs=[0, 0], build_idxs=[1, 2]
     * 
     * 第二次调用：probe_idx=1, build_idx=0
     * - 查找键值 "B"，找到 Build 行 3
     * - 返回：probe_idx=2, build_idx=0, matched_cnt=1
     * - probe_idxs=[1], build_idxs=[3]
     */
    template <int JoinOpType>
    auto find_batch(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                    int probe_idx, uint32_t build_idx, int probe_rows,
                    uint32_t* __restrict probe_idxs, bool& probe_visited,
                    uint32_t* __restrict build_idxs, const uint8_t* null_map,
                    bool with_other_conjuncts, bool is_mark_join, bool has_mark_join_conjunct) {
        // ===== 情况 1：NULL Aware Join + 空 Build 表 =====
        // NULL Aware LEFT ANTI/SEMI JOIN 的特殊情况：如果 Build 表为空
        // 对于 LEFT SEMI JOIN：所有 Probe 行都不匹配（返回空结果）
        // 对于 LEFT ANTI JOIN：所有 Probe 行都匹配（返回所有 Probe 行）
        if ((JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
             JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) &&
            _empty_build_side) {
            return _process_null_aware_left_half_join_for_empty_build_side<JoinOpType>(
                    probe_idx, probe_rows, probe_idxs, build_idxs);
        }

        // ===== 情况 2：有其他 Join 条件 =====
        // 如果有其他 Join 条件（如 WHERE 子句中的条件），需要先找到匹配的行
        // 然后再应用其他条件进行过滤
        // only_need_to_match_one=false：需要找到所有匹配的行（因为后续还要过滤）
        if (with_other_conjuncts) {
            return _find_batch_conjunct<JoinOpType, false>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs);
        }

        // ===== 情况 3：Mark Join =====
        // Mark Join：只返回是否匹配，不返回匹配的数据
        if (is_mark_join) {
            bool is_null_aware_join = JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                                      JoinOpType == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN;
            bool is_left_half_join =
                    JoinOpType == TJoinOp::LEFT_SEMI_JOIN || JoinOpType == TJoinOp::LEFT_ANTI_JOIN;

            /// For null aware join or left half(semi/anti) join without other conjuncts and without
            /// mark join conjunct.
            /// If one row on probe side has one match in build side, we should stop searching the
            /// hash table for this row.
            // ===== 子情况 3A：优化路径（只需要匹配一个） =====
            // 对于 NULL Aware Join 或 LEFT SEMI/ANTI JOIN（没有其他条件和 Mark Join 条件）
            // 如果 Probe 行匹配到一个 Build 行，就可以停止查找（因为只需要知道是否匹配）
            // only_need_to_match_one=true：优化，找到第一个匹配就停止
            if (is_null_aware_join || (is_left_half_join && !has_mark_join_conjunct)) {
                return _find_batch_conjunct<JoinOpType, true>(keys, build_idx_map, probe_idx,
                                                              build_idx, probe_rows, probe_idxs,
                                                              build_idxs);
            }

            // ===== 子情况 3B：需要找到所有匹配 =====
            // 对于其他 Mark Join 情况，可能需要找到所有匹配的行
            // only_need_to_match_one=false：找到所有匹配的行
            return _find_batch_conjunct<JoinOpType, false>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs);
        }

        // ===== 情况 4：INNER JOIN / OUTER JOIN =====
        // INNER JOIN：只返回匹配的行
        // LEFT OUTER JOIN：返回匹配的行 + 未匹配的 Probe 行（Build 侧填充 NULL）
        // RIGHT OUTER JOIN：返回匹配的行 + 未匹配的 Build 行（Probe 侧填充 NULL）
        // FULL OUTER JOIN：返回所有匹配的行 + 未匹配的 Probe 行 + 未匹配的 Build 行
        if (JoinOpType == TJoinOp::INNER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
            JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
            return _find_batch_inner_outer_join<JoinOpType>(keys, build_idx_map, probe_idx,
                                                            build_idx, probe_rows, probe_idxs,
                                                            probe_visited, build_idxs);
        }
        
        // ===== 情况 5：LEFT SEMI/ANTI JOIN =====
        // LEFT SEMI JOIN：只返回匹配的 Probe 行（不返回 Build 侧数据）
        // LEFT ANTI JOIN：只返回未匹配的 Probe 行
        // NULL_AWARE_LEFT_ANTI_JOIN：特殊处理 NULL 值
        if (JoinOpType == TJoinOp::LEFT_ANTI_JOIN || JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
            JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            // 根据是否有 NULL Map 选择不同的实现
            if (null_map) {
                return _find_batch_left_semi_anti<JoinOpType, true>(
                        keys, build_idx_map, probe_idx, probe_rows, probe_idxs, null_map);
            } else {
                return _find_batch_left_semi_anti<JoinOpType, false>(
                        keys, build_idx_map, probe_idx, probe_rows, probe_idxs, nullptr);
            }
        }
        
        // ===== 情况 6：RIGHT SEMI/ANTI JOIN =====
        // RIGHT SEMI JOIN：只返回匹配的 Build 行（不返回 Probe 侧数据）
        // RIGHT ANTI JOIN：只返回未匹配的 Build 行
        if (JoinOpType == TJoinOp::RIGHT_ANTI_JOIN || JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
            return _find_batch_right_semi_anti(keys, build_idx_map, probe_idx, probe_rows);
        }
        
        // ===== 错误情况 =====
        // 如果遇到不支持的 Join 类型，抛出异常
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid hash join input");
    }

    /**
     * Because the equality comparison result of null with any value is null,
     * in null aware join, if the probe key of a row in the left table(probe side) is null,
     * then this row will match all rows on the right table(build side) (the match result is null).
     * If the probe key of a row in the left table does not match any row in right table,
     * this row will match all rows with null key in the right table.
     * select 'a' in ('b', null) => 'a' = 'b' or 'a' = null => false or null => null
     * select 'a' in ('a', 'b', null) => true
     * select 'a' not in ('b', null) => null => 'a' != 'b' and 'a' != null => true and null => null
     * select 'a' not in ('a', 'b', null) => false
     */
    auto find_null_aware_with_other_conjuncts(const Key* __restrict keys,
                                              const uint32_t* __restrict build_idx_map,
                                              int probe_idx, uint32_t build_idx, int probe_rows,
                                              uint32_t* __restrict probe_idxs,
                                              uint32_t* __restrict build_idxs,
                                              uint8_t* __restrict null_flags,
                                              bool picking_null_keys, const uint8_t* null_map) {
        if (null_map) {
            return _find_null_aware_with_other_conjuncts_impl<true>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs,
                    null_flags, picking_null_keys, null_map);
        } else {
            return _find_null_aware_with_other_conjuncts_impl<false>(
                    keys, build_idx_map, probe_idx, build_idx, probe_rows, probe_idxs, build_idxs,
                    null_flags, picking_null_keys, nullptr);
        }
    }

    template <int JoinOpType, bool is_mark_join>
    bool iterate_map(vectorized::ColumnOffset32& build_idxs,
                     vectorized::ColumnFilterHelper* mark_column_helper) const {
        const auto batch_size = max_batch_size;
        const auto elem_num = visited.size();
        int count = 0;
        build_idxs.resize(batch_size);

        while (count < batch_size && iter_idx < elem_num) {
            const auto matched = visited[iter_idx];
            build_idxs.get_element(count) = iter_idx;
            if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN) {
                if constexpr (is_mark_join) {
                    mark_column_helper->insert_value(matched);
                    ++count;
                } else {
                    count += matched;
                }
            } else {
                count += !matched;
            }
            iter_idx++;
        }

        build_idxs.resize(count);
        return iter_idx >= elem_num;
    }

    bool has_null_key() { return _has_null_key; }

    bool keep_null_key() { return _keep_null_key; }

    void pre_build_idxs(DorisVector<uint32_t>& buckets) const {
        for (unsigned int& bucket : buckets) {
            bucket = first[bucket];
        }
    }

private:
    bool _eq(const Key& lhs, const Key& rhs) const {
        if (DirectMapping) {
            return true;
        }
        return lhs == rhs;
    }

    template <int JoinOpType>
    auto _process_null_aware_left_half_join_for_empty_build_side(int probe_idx, int probe_rows,
                                                                 uint32_t* __restrict probe_idxs,
                                                                 uint32_t* __restrict build_idxs) {
        if (JoinOpType != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN &&
            JoinOpType != TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "process_null_aware_left_half_join_for_empty_build_side meet invalid "
                            "hash join input");
        }
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            probe_idxs[matched_cnt] = probe_idx++;
            build_idxs[matched_cnt] = 0;
            ++matched_cnt;
        }

        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    auto _find_batch_right_semi_anti(const Key* __restrict keys,
                                     const uint32_t* __restrict build_idx_map, int probe_idx,
                                     int probe_rows) {
        while (probe_idx < probe_rows) {
            auto build_idx = build_idx_map[probe_idx];

            if constexpr (DirectMapping) {
                if (!visited[build_idx]) {
                    while (build_idx) {
                        visited[build_idx] = 1;
                        build_idx = next[build_idx];
                    }
                }
            } else {
                while (build_idx) {
                    if (!visited[build_idx] && _eq(keys[probe_idx], build_keys[build_idx])) {
                        visited[build_idx] = 1;
                    }
                    build_idx = next[build_idx];
                }
            }
            probe_idx++;
        }
        return std::tuple {probe_idx, 0U, 0U};
    }

    template <int JoinOpType, bool has_null_map>
    auto _find_batch_left_semi_anti(const Key* __restrict keys,
                                    const uint32_t* __restrict build_idx_map, int probe_idx,
                                    int probe_rows, uint32_t* __restrict probe_idxs,
                                    const uint8_t* null_map) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            if constexpr (JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && has_null_map) {
                if (null_map[probe_idx]) {
                    probe_idx++;
                    continue;
                }
            }

            auto build_idx = build_idx_map[probe_idx];

            while (build_idx && keys[probe_idx] != build_keys[build_idx]) {
                build_idx = next[build_idx];
            }
            bool matched = JoinOpType == TJoinOp::LEFT_SEMI_JOIN ? build_idx != 0 : build_idx == 0;
            probe_idxs[matched_cnt] = probe_idx++;
            matched_cnt += matched;
        }
        return std::tuple {probe_idx, 0U, matched_cnt};
    }

    template <int JoinOpType, bool only_need_to_match_one>
    auto _find_batch_conjunct(const Key* __restrict keys, const uint32_t* __restrict build_idx_map,
                              int probe_idx, uint32_t build_idx, int probe_rows,
                              uint32_t* __restrict probe_idxs, uint32_t* __restrict build_idxs) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            while (build_idx && matched_cnt < batch_size) {
                if (_eq(keys[probe_idx], build_keys[build_idx])) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    matched_cnt++;

                    if constexpr (only_need_to_match_one) {
                        build_idx = 0;
                        break;
                    }
                }
                build_idx = next[build_idx];
            }

            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                          JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                          JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                          JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN) {
                // may over batch_size when emplace 0 into build_idxs
                if (!build_idx) {
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = 0;
                    matched_cnt++;
                }
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];
            do_the_probe();
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    /**
     * 处理 INNER JOIN 和 OUTER JOIN 的批量查找方法
     * 
     * 【功能说明】
     * 这是处理 INNER JOIN、LEFT OUTER JOIN、RIGHT OUTER JOIN 和 FULL OUTER JOIN 的内部方法。
     * 核心逻辑：
     * 1. 对每个 Probe 行，在 Hash 表中查找所有匹配的 Build 行（一对多匹配）
     * 2. 对于 OUTER JOIN，处理未匹配的行：
     *    - LEFT OUTER JOIN：未匹配的 Probe 行需要输出（Build 侧填充 NULL）
     *    - RIGHT OUTER JOIN：标记已匹配的 Build 行（后续输出未匹配的 Build 行）
     *    - FULL OUTER JOIN：同时处理上述两种情况
     * 
     * 【关键设计】
     * - 使用 lambda 函数 `do_the_probe` 封装单个 Probe 行的查找逻辑
     * - 支持一对多匹配：一个 Probe 行可以匹配多个 Build 行
     * - 支持分批次处理：通过 `build_idx` 跟踪当前匹配进度
     * - 最后调整 `probe_idx`：如果当前 Probe 行还有未处理的 Build 行，回退 `probe_idx`
     * 
     * 【参数说明】
     * @param keys Probe 键值数组
     * @param build_idx_map Build 索引映射表（build_idx_map[probe_idx] 表示 Probe 行对应的 Hash 桶中的第一个 Build 行索引）
     * @param probe_idx 当前处理的 Probe 行索引（输入/输出参数）
     * @param build_idx 当前处理的 Build 行索引（用于一对多匹配，输入/输出参数）
     * @param probe_rows Probe 数据的总行数
     * @param probe_idxs 输出参数：存储匹配的 Probe 行索引数组
     * @param probe_visited 输出参数：Probe 行是否已访问（用于 LEFT OUTER JOIN）
     * @param build_idxs 输出参数：存储匹配的 Build 行索引数组
     * 
     * 【返回值】
     * 返回 std::tuple<probe_idx, build_idx, matched_cnt>
     * - probe_idx：处理到的 Probe 行索引（如果 build_idx != 0，表示当前 Probe 行还有未处理的 Build 行）
     * - build_idx：处理到的 Build 行索引（如果 != 0，表示当前 Probe 行还有未处理的 Build 行）
     * - matched_cnt：当前批次匹配的行数
     * 
     * 【示例】
     * 假设 LEFT OUTER JOIN，Probe 表有键值 ["A", "B", "C"]，Build 表有键值 ["A", "A"]
     * 
     * 第一次调用：probe_idx=0, build_idx=0
     * - Probe 行 0（键值 "A"）匹配 Build 行 1、2（都是 "A"）
     * - 返回：probe_idx=1, build_idx=0, matched_cnt=2
     * - probe_idxs=[0, 0], build_idxs=[1, 2]
     * 
     * 第二次调用：probe_idx=1, build_idx=0
     * - Probe 行 1（键值 "B"）未匹配
     * - 返回：probe_idx=2, build_idx=0, matched_cnt=1（包含未匹配的 Probe 行）
     * - probe_idxs=[1], build_idxs=[0]（0 表示未匹配）
     */
    template <int JoinOpType>
    auto _find_batch_inner_outer_join(const Key* __restrict keys,
                                      const uint32_t* __restrict build_idx_map, int probe_idx,
                                      uint32_t build_idx, int probe_rows,
                                      uint32_t* __restrict probe_idxs, bool& probe_visited,
                                      uint32_t* __restrict build_idxs) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        /**
         * Lambda 函数：处理单个 Probe 行的查找逻辑
         * 
         * 【工作流程】
         * 1. 遍历当前 Probe 行对应的所有 Build 行（通过 next 链表）
         * 2. 对每个 Build 行，检查是否匹配（键值相等）
         * 3. 如果匹配，记录到输出数组
         * 4. 对于 RIGHT OUTER JOIN 和 FULL OUTER JOIN，标记 Build 行已访问
         * 5. 对于 LEFT OUTER JOIN 和 FULL OUTER JOIN，处理未匹配的 Probe 行
         * 6. 移动到下一个 Probe 行
         */
        auto do_the_probe = [&]() {
            // ===== 步骤 1：遍历 Hash 桶中的 Build 行链表 =====
            // build_idx != 0 表示还有 Build 行需要检查
            // matched_cnt < batch_size 确保不超过批次大小限制
            while (build_idx && matched_cnt < batch_size) {
                // ===== 步骤 2：检查键值是否匹配 =====
                // _eq 是比较函数，检查 Probe 键和 Build 键是否相等
                if (_eq(keys[probe_idx], build_keys[build_idx])) {
                    // ===== 步骤 3：记录匹配的行 =====
                    // 将匹配的 Probe 行索引和 Build 行索引存储到输出数组
                    probe_idxs[matched_cnt] = probe_idx;
                    build_idxs[matched_cnt] = build_idx;
                    matched_cnt++;
                    
                    // ===== 步骤 4：标记 Build 行已访问（RIGHT OUTER JOIN / FULL OUTER JOIN） =====
                    // 对于 RIGHT OUTER JOIN 和 FULL OUTER JOIN，需要知道哪些 Build 行被匹配了
                    // 后续需要输出未匹配的 Build 行（Probe 侧填充 NULL）
                    if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                                  JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                        if (!visited[build_idx]) {
                            visited[build_idx] = 1;
                        }
                    }
                }
                // ===== 步骤 5：移动到链表中的下一个 Build 行 =====
                // next[build_idx] 指向 Hash 桶中下一个 Build 行（处理 Hash 冲突）
                build_idx = next[build_idx];
            }

            // ===== 步骤 6：处理未匹配的 Probe 行（LEFT OUTER JOIN / FULL OUTER JOIN） =====
            // 对于 LEFT OUTER JOIN 和 FULL OUTER JOIN，如果 Probe 行未匹配任何 Build 行，
            // 需要输出该 Probe 行（Build 侧填充 NULL）
            if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                // ===== 子步骤 6A：更新 probe_visited 标志 =====
                // probe_visited 表示当前 Probe 行是否已匹配（用于跨批次处理）
                // 如果当前批次有匹配（matched_cnt > 0）且最后一个匹配来自当前 Probe 行，
                // 说明当前 Probe 行已匹配，设置 probe_visited = true
                // `(!matched_cnt || probe_idxs[matched_cnt - 1] != probe_idx)` means not match one build side
                probe_visited |= (matched_cnt && probe_idxs[matched_cnt - 1] == probe_idx);
                
                // ===== 子步骤 6B：处理未匹配的情况 =====
                // build_idx == 0 表示已遍历完所有 Build 行，没有更多匹配
                if (!build_idx) {
                    // 如果当前 Probe 行未匹配（probe_visited == false），输出未匹配的行
                    if (!probe_visited) {
                        probe_idxs[matched_cnt] = probe_idx;
                        build_idxs[matched_cnt] = 0;  // 0 表示未匹配（Build 侧填充 NULL）
                        matched_cnt++;
                    }
                    // 重置 probe_visited，准备处理下一个 Probe 行
                    probe_visited = false;
                }
            }
            // ===== 步骤 7：移动到下一个 Probe 行 =====
            probe_idx++;
        };

        // ===== 阶段 1：处理当前 Probe 行的剩余 Build 行 =====
        // 如果 build_idx != 0，说明当前 Probe 行还有未处理的 Build 行（可能是上次调用未完成的）
        // 继续处理这些 Build 行
        if (build_idx) {
            do_the_probe();
        }

        // ===== 阶段 2：批量处理后续 Probe 行 =====
        // 继续处理后续的 Probe 行，直到：
        // 1. 处理完所有 Probe 行（probe_idx >= probe_rows）
        // 2. 达到批次大小限制（matched_cnt >= batch_size）
        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            // 获取当前 Probe 行对应的 Hash 桶中的第一个 Build 行索引
            build_idx = build_idx_map[probe_idx];
            // 处理当前 Probe 行
            do_the_probe();
        }

        // ===== 阶段 3：调整 probe_idx（处理一对多匹配） =====
        // 【关键逻辑】如果 build_idx != 0，说明当前 Probe 行还有未处理的 Build 行
        // 需要回退 probe_idx，以便下次调用时继续处理当前 Probe 行的剩余 Build 行
        // 
        // 示例：
        // - Probe 行 0 匹配 Build 行 1、2、3、4、5（共 5 个）
        // - 当前批次只处理了 Build 行 1、2（matched_cnt=2，达到批次限制）
        // - build_idx = 3（还有 Build 行 3、4、5 未处理）
        // - probe_idx 已递增到 1，需要回退到 0，以便下次继续处理 Probe 行 0 的剩余 Build 行
        probe_idx -= (build_idx != 0);
        
        return std::tuple {probe_idx, build_idx, matched_cnt};
    }

    template <bool has_null_map>
    auto _find_null_aware_with_other_conjuncts_impl(
            const Key* __restrict keys, const uint32_t* __restrict build_idx_map, int probe_idx,
            uint32_t build_idx, int probe_rows, uint32_t* __restrict probe_idxs,
            uint32_t* __restrict build_idxs, uint8_t* __restrict null_flags, bool picking_null_keys,
            const uint8_t* null_map) {
        uint32_t matched_cnt = 0;
        const auto batch_size = max_batch_size;

        auto do_the_probe = [&]() {
            /// If no any rows match the probe key, here start to handle null keys in build side.
            /// The result of "Any = null" is null.
            if (build_idx == 0 && !picking_null_keys) {
                build_idx = first[bucket_size];
                picking_null_keys = true; // now pick null from build side
            }

            while (build_idx && matched_cnt < batch_size) {
                if (picking_null_keys || _eq(keys[probe_idx], build_keys[build_idx])) {
                    build_idxs[matched_cnt] = build_idx;
                    probe_idxs[matched_cnt] = probe_idx;
                    null_flags[matched_cnt] = picking_null_keys;
                    matched_cnt++;
                }

                build_idx = next[build_idx];

                // If `build_idx` is 0, all matched keys are handled,
                // now need to handle null keys in build side.
                if (!build_idx && !picking_null_keys) {
                    build_idx = first[bucket_size];
                    picking_null_keys = true; // now pick null keys from build side
                }
            }

            // may over batch_size when emplace 0 into build_idxs
            if (!build_idx) {
                probe_idxs[matched_cnt] = probe_idx;
                build_idxs[matched_cnt] = 0;
                picking_null_keys = false;
                matched_cnt++;
            }

            probe_idx++;
        };

        if (build_idx) {
            do_the_probe();
        }

        while (probe_idx < probe_rows && matched_cnt < batch_size) {
            build_idx = build_idx_map[probe_idx];

            /// If the probe key is null
            if constexpr (has_null_map) {
                if (null_map[probe_idx]) {
                    probe_idx++;
                    break;
                }
            }
            do_the_probe();
            if (picking_null_keys) {
                break;
            }
        }

        probe_idx -= (build_idx != 0);
        return std::tuple {probe_idx, build_idx, matched_cnt, picking_null_keys};
    }

    const Key* __restrict build_keys;
    DorisVector<uint8_t> visited;

    uint32_t bucket_size = 1;
    int max_batch_size = 4064;

    DorisVector<uint32_t> first = {0};
    DorisVector<uint32_t> next = {0};

    // use in iter hash map
    mutable uint32_t iter_idx = 1;
    bool _has_null_key = false;
    bool _keep_null_key = false;
    bool _empty_build_side = true;
};

template <typename Key, typename Hash, bool DirectMapping>
using JoinHashMap = JoinHashTable<Key, Hash, DirectMapping>;
#include "common/compile_check_end.h"
} // namespace doris