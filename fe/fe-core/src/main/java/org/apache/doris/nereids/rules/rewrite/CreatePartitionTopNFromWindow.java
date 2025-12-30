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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

/**
 * 将 'partitionTopN' 下推到 'window' 算子中。
 * 该规则会将过滤条件转换为 'limit value'，并将其下推到 'window' 算子下方。
 * 但存在一些限制条件，具体说明如下。
 * 
 * 示例：
 * 'SELECT * FROM (
 *     SELECT *, ROW_NUMBER() OVER (ORDER BY b) AS row_number
 *     FROM t
 * ) AS tt WHERE row_number <= 100;'
 * 过滤条件 'row_number <= 100' 可以被下推到窗口算子中。
 * 
 * 以下展示了计划树的变化过程：
 * 原始逻辑计划树：
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                 any_node
 * 
 * 转换后的逻辑计划树：
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                partition_topn(PARTITION BY: a, ORDER BY b, Partition Limit: 100)
 *                   |
 *                 any_node
 * 
 * Push down the 'partitionTopN' into the 'window'.
 * It will convert the filter condition to the 'limit value' and push down below the 'window'.
 * But there are some restrictions, the details are explained below.
 * For example:
 * 'SELECT * FROM (
 *     SELECT *, ROW_NUMBER() OVER (ORDER BY b) AS row_number
 *     FROM t
 * ) AS tt WHERE row_number <= 100;'
 * The filter 'row_number <= 100' can be pushed down into the window operator.
 * The following will demonstrate how the plan changes:
 * Logical plan tree:
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                 any_node
 * transformed to:
 *                 any_node
 *                   |
 *                filter (row_number <= 100)
 *                   |
 *                window (PARTITION BY a ORDER BY b)
 *                   |
 *                partition_topn(PARTITION BY: a, ORDER BY b, Partition Limit: 100)
 *                   |
 *                 any_node
 */

public class CreatePartitionTopNFromWindow extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        // 匹配模式：Filter -> Window 的结构
        return logicalFilter(logicalWindow()).thenApply(ctx -> {
            LogicalFilter<LogicalWindow<Plan>> filter = ctx.root;
            LogicalWindow<Plan> window = filter.child();

            // 检查是否已经进行过此优化：如果 window 的子节点已经是 LogicalPartitionTopN，
            // 或者在 window 和 LogicalPartitionTopN 之间存在 LogicalFilter，
            // 则说明已经优化过了，直接返回原计划，避免重复优化
            if (window.child(0) instanceof LogicalPartitionTopN
                    || (window.child(0) instanceof LogicalFilter
                    && window.child(0).child(0) != null
                    && window.child(0).child(0) instanceof LogicalPartitionTopN)) {
                return filter;
            }

            // 尝试从 filter 条件中提取可以下推的窗口函数和对应的 limit 值
            // 如果 filter 条件可以转换为窗口函数的 limit（如 row_number <= 100），
            // 则返回对应的 WindowExpression 和 limit 值；否则返回 null
            // Long.MAX_VALUE 表示没有额外的 limit 限制
            Pair<WindowExpression, Long> windowFuncPair = window.getPushDownWindowFuncAndLimit(filter, Long.MAX_VALUE);
            
            // 如果无法提取窗口函数和 limit 值，说明当前 filter 条件不适合此优化，返回原计划
            if (windowFuncPair == null) {
                return filter;
            } else if (windowFuncPair.second == -1) {
                // limit 值为 -1 表示空关系的情况（例如 limit 0 或 limit < 0）
                // 直接返回空的逻辑关系，避免执行不必要的计算
                return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(), filter.getOutput());
            } else {
                // 成功提取窗口函数和 limit 值，执行下推优化
                // 在 window 算子下方插入 partition_topn 算子，实现分区级别的 TopN 优化
                // false 参数表示不是强制下推
                Plan newWindow = window.pushPartitionLimitThroughWindow(windowFuncPair.first,
                        windowFuncPair.second, false);
                // 返回新的 filter，其子节点为优化后的 window
                return filter.withChildren(newWindow);
            }
        }).toRule(RuleType.CREATE_PARTITION_TOPN_FOR_WINDOW);
    }
}
