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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PartitionTopnPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Implementation rule that convert logical partition-top-n to physical partition-top-n.
 */
public class LogicalPartitionTopNToPhysicalPartitionTopN extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalPartitionTopN().thenApplyMulti(ctx -> generatePhysicalPartitionTopn(ctx.root))
                .toRule(RuleType.LOGICAL_PARTITION_TOP_N_TO_PHYSICAL_PARTITION_TOP_N_RULE);
    }

    /**
     * 将逻辑的 PartitionTopN 转换为物理的 PartitionTopN 计划。
     * 根据分区键的情况和统计信息，生成不同的物理执行计划候选。
     * 
     * PartitionTopN 的执行阶段（Phase）说明：
     * - ONE_PHASE_GLOBAL_PTOPN: 单阶段全局分区TopN，在单个节点上完成所有分区的TopN计算
     * - TWO_PHASE_LOCAL_PTOPN: 两阶段本地分区TopN，第一阶段在各个BE节点本地进行分区TopN
     * - TWO_PHASE_GLOBAL_PTOPN: 两阶段全局分区TopN，第二阶段在全局进行合并和最终TopN
     * 
     * @param logicalPartitionTopN 逻辑的 PartitionTopN 算子
     * @return 物理的 PartitionTopN 计划列表，优化器会选择成本最低的计划
     */
    private List<PhysicalPartitionTopN<? extends Plan>> generatePhysicalPartitionTopn(
            LogicalPartitionTopN<? extends Plan> logicalPartitionTopN) {
        // 情况1：如果没有分区键，或者统计信息显示不适合使用两阶段全局分区TopN
        // （例如分区键的基数（NDV）太大，接近总行数，意味着每个分区数据量很小，不需要两阶段）
        if (logicalPartitionTopN.getPartitionKeys().isEmpty()
                || !checkTwoPhaseGlobalPartitionTopn(logicalPartitionTopN)) {
            // 如果没有分区键，使用本地分区TopN，后续可能需要全局排序来满足上层窗口算子的排序要求
            // 转换 OrderExpression 列表为 OrderKey 列表
            List<OrderKey> orderKeys = !logicalPartitionTopN.getOrderKeys().isEmpty()
                    ? logicalPartitionTopN.getOrderKeys().stream()
                    .map(OrderExpression::getOrderKey)
                    .collect(ImmutableList.toImmutableList()) :
                    ImmutableList.of();

            // 创建一个两阶段本地分区TopN物理计划
            // TWO_PHASE_LOCAL_PTOPN 表示在各个BE节点上独立进行分区TopN计算
            PhysicalPartitionTopN<Plan> onePhaseLocalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    orderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    logicalPartitionTopN.child(0));

            // 只返回一个物理计划候选
            return ImmutableList.of(onePhaseLocalPartitionTopN);
        } else {
            // 情况2：有分区键且适合两阶段全局分区TopN
            // 构建完整的排序键：分区键 + 排序键
            // 这样做的原因是上层窗口算子需要按照 [分区键, 排序键] 的顺序来处理数据
            // 例如：PARTITION BY department ORDER BY salary 需要按 (department, salary) 排序
            ImmutableList<OrderKey> fullOrderKeys = getAllOrderKeys(logicalPartitionTopN);
            
            // 生成第一个物理计划候选：单阶段全局分区TopN
            // ONE_PHASE_GLOBAL_PTOPN 适用于数据量较小或可以在单个节点处理的情况
            // 优点：执行简单，不需要数据shuffle；缺点：单节点资源限制
            PhysicalPartitionTopN<Plan> onePhaseGlobalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    fullOrderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.ONE_PHASE_GLOBAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    logicalPartitionTopN.child(0));

            // 生成两阶段执行计划的第一个阶段：本地分区TopN
            // TWO_PHASE_LOCAL_PTOPN 在各个BE节点上并行执行，每个节点处理部分分区
            // 这是两阶段计划的第一个阶段
            PhysicalPartitionTopN<Plan> twoPhaseLocalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    fullOrderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.TWO_PHASE_LOCAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    logicalPartitionTopN.child(0));

            // 生成两阶段执行计划的第二个阶段：全局分区TopN
            // TWO_PHASE_GLOBAL_PTOPN 在所有BE节点完成本地TopN后，进行全局合并
            // 子节点是 twoPhaseLocalPartitionTopN，形成两阶段执行链
            // 优点：可以处理大数据量，并行度高；缺点：需要数据shuffle，网络开销
            PhysicalPartitionTopN<Plan> twoPhaseGlobalPartitionTopN = new PhysicalPartitionTopN<>(
                    logicalPartitionTopN.getFunction(),
                    logicalPartitionTopN.getPartitionKeys(),
                    fullOrderKeys,
                    logicalPartitionTopN.hasGlobalLimit(),
                    logicalPartitionTopN.getPartitionLimit(),
                    PartitionTopnPhase.TWO_PHASE_GLOBAL_PTOPN,
                    logicalPartitionTopN.getLogicalProperties(),
                    twoPhaseLocalPartitionTopN);

            // 返回两个物理计划候选：
            // 1. 单阶段全局计划：适合小数据量
            // 2. 两阶段计划（本地+全局）：适合大数据量，并行处理
            // 优化器会根据统计信息和成本模型选择最优计划
            return ImmutableList.of(onePhaseGlobalPartitionTopN, twoPhaseGlobalPartitionTopN);
        }
    }

    /**
     * 检查是否适合使用两阶段全局分区TopN。
     * 
     * 判断逻辑：
     * - 如果分区键的 NDV（不同值的数量）接近总行数，说明每个分区的数据量很小
     * - 在这种情况下，使用两阶段全局分区TopN（需要数据shuffle和网络传输）的开销会超过收益
     * - 因此，只有当每个分区的平均数据量足够大时（>= 阈值），才适合使用两阶段全局分区TopN
     * 
     * 判断公式：rowCount / maxNdv >= globalPartitionTopnThreshold
     * - rowCount: 总行数
     * - maxNdv: 分区键中的最大NDV值（不同值的数量）
     * - rowCount / maxNdv: 每个分区的平均行数
     * - globalPartitionTopnThreshold: 阈值（默认100），表示每个分区至少要有多少行才适合两阶段
     * 
     * 示例：
     * - 如果总行数 = 10000，分区键的NDV = 10，则平均每个分区 = 1000 行（>= 100），适合两阶段
     * - 如果总行数 = 10000，分区键的NDV = 9000，则平均每个分区 ≈ 1.1 行（< 100），不适合两阶段
     * 
     * @param logicalPartitionTopN 逻辑的 PartitionTopN 算子
     * @return true 表示适合使用两阶段全局分区TopN，false 表示不适合
     * 
     * check if partition keys' ndv is almost near the total row count.
     * if yes, it is not suitable for two phase global partition topn.
     */
    private boolean checkTwoPhaseGlobalPartitionTopn(LogicalPartitionTopN<? extends Plan> logicalPartitionTopN) {
        // 获取全局分区TopN阈值，默认为100，表示每个分区平均至少要有100行才适合两阶段
        double globalPartitionTopnThreshold = ConnectContext.get().getSessionVariable()
                .getGlobalPartitionTopNThreshold();
        
        // 检查是否有 GroupExpression（用于获取统计信息）
        if (logicalPartitionTopN.getGroupExpression().isPresent()) {
            Group group = logicalPartitionTopN.getGroupExpression().get().getOwnerGroup();
            
            // 检查 Group 和统计信息是否存在
            if (group != null && group.getStatistics() != null) {
                Statistics stats = group.getStatistics();
                double rowCount = stats.getRowCount();  // 获取总行数
                List<Expression> partitionKeys = logicalPartitionTopN.getPartitionKeys();
                
                // 检查分区键是否是基本表的列（必须是 SlotReference 且来自原始表）
                // 如果不是，则不支持两阶段全局分区TopN
                if (!checkPartitionKeys(partitionKeys)) {
                    return false;
                }
                
                // 获取所有分区键的列统计信息
                // 过滤掉 null 和未知的统计信息
                List<ColumnStatistic> partitionByKeyStats = partitionKeys.stream()
                        .map(partitionKey -> stats.findColumnStatistics(partitionKey))
                        .filter(Objects::nonNull)
                        .filter(e -> !e.isUnKnown)
                        .collect(Collectors.toList());
                
                // 如果无法获取所有分区键的统计信息，则不适合两阶段
                if (partitionByKeyStats.size() != partitionKeys.size()) {
                    return false;
                } else {
                    // 提取所有分区键的 NDV（不同值的数量）
                    // 过滤掉无效值（<= 0 或无穷大）
                    List<Double> ndvs = partitionByKeyStats.stream().map(s -> s.ndv)
                            .filter(e -> e > 0 && !Double.isInfinite(e))
                            .collect(Collectors.toList());
                    
                    // 如果无法获取有效的 NDV 值，则不适合两阶段
                    if (ndvs.size() != partitionByKeyStats.size()) {
                        return false;
                    } else {
                        // 取所有分区键中的最大 NDV 值
                        // 对于多列分区键，使用最大值来保守估计（避免高估每个分区的数据量）
                        double maxNdv = ndvs.stream().max(Double::compare).get();
                        
                        // 判断每个分区的平均行数是否 >= 阈值
                        // rowCount / maxNdv 表示每个分区的平均行数
                        // 如果 >= threshold，说明每个分区数据量足够大，适合使用两阶段全局分区TopN
                        // 如果 < threshold，说明每个分区数据量很小，两阶段的网络开销不划算
                        return rowCount / maxNdv >= globalPartitionTopnThreshold;
                    }
                }
            } else {
                // 没有统计信息，无法判断，返回 false（不适合两阶段）
                return false;
            }
        } else {
            // 没有 GroupExpression，无法获取统计信息，返回 false（不适合两阶段）
            return false;
        }
    }

    /**
     * global partition topn only take effect if partition keys are columns from basic table
     */
    private boolean checkPartitionKeys(List<Expression> partitionKeys) {
        for (Expression expr : partitionKeys) {
            if (!(expr instanceof SlotReference)) {
                return false;
            } else {
                SlotReference slot = (SlotReference) expr;
                if (!slot.getOriginalColumn().isPresent() || !slot.getOriginalTable().isPresent()) {
                    return false;
                }
            }
        }
        return true;
    }

    private ImmutableList<OrderKey> getAllOrderKeys(LogicalPartitionTopN<? extends Plan> logicalPartitionTopN) {
        ImmutableList.Builder<OrderKey> builder = ImmutableList.builder();

        if (!logicalPartitionTopN.getPartitionKeys().isEmpty()) {
            builder.addAll(logicalPartitionTopN.getPartitionKeys().stream().map(partitionKey -> {
                return new OrderKey(partitionKey, true, false);
            }).collect(ImmutableList.toImmutableList()));
        }

        if (!logicalPartitionTopN.getOrderKeys().isEmpty()) {
            builder.addAll(logicalPartitionTopN.getOrderKeys().stream()
                    .map(OrderExpression::getOrderKey)
                    .collect(ImmutableList.toImmutableList())
            );
        }

        return builder.build();
    }
}
