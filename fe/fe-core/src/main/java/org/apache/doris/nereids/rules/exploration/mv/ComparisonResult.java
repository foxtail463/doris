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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 查询和物化视图的比较结果。
 * 
 * 该类用于存储查询和物化视图在逻辑计划图（HyperGraph）比较过程中产生的信息，
 * 包括从 Join 条件中上拉的谓词、从 Filter 条件中上拉的谓词，以及 Join 类型推断信息。
 * 
 * ==================== 核心作用 ====================
 * 
 * 在物化视图匹配过程中，需要比较查询和物化视图的逻辑计划图结构是否等价。
 * 比较过程会：
 * 1. 检查查询和物化视图的表结构、Join 类型、Join 条件是否兼容
 * 2. 收集从 Join Edge 中上拉的谓词（这些谓词原本在 Join 条件中，但可以上拉到 Filter）
 * 3. 收集从 Filter Edge 中上拉的谓词（这些谓词原本在 Filter 中）
 * 4. 记录 Join 类型推断信息（如 MV 是 LEFT JOIN，查询是 INNER JOIN 时的 NULL 拒绝条件）
 * 
 * ==================== 字段说明 ====================
 * 
 * 1. valid：比较结果是否有效
 *    - true：查询和物化视图的逻辑计划图结构兼容，可以继续重写
 *    - false：结构不兼容，无法重写（如 Join 类型不匹配、表结构不一致等）
 * 
 * 2. queryExpressions：从查询的 Join Edge 中上拉的谓词
 *    - 这些谓词原本在查询的 Join 条件中，但可以上拉到 Filter
 *    - 示例：查询 `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t1.status = 'active'`
 *      → `t1.status = 'active'` 可以从 Join 条件上拉到 Filter
 * 
 * 3. viewExpressions：从物化视图的 Join Edge 中上拉的谓词
 *    - 这些谓词原本在物化视图的 Join 条件中，但可以上拉到 Filter
 *    - 示例：物化视图 `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t1.status = 'active'`
 *      → `t1.status = 'active'` 可以从 Join 条件上拉到 Filter
 * 
 * 4. queryAllPulledUpExpressions：从查询的 Filter Edge 中上拉的所有谓词
 *    - 这些谓词原本在查询的 Filter 中
 *    - **重要**：在 build() 方法中会过滤掉推断表达式（inferred expressions）
 *    - 用于后续的谓词补偿（predicatesCompensate）
 * 
 * 5. viewNoNullableSlot：物化视图中需要拒绝 NULL 的 Slot 集合
 *    - 用于处理 Join 类型不同的情况
 *    - 示例：MV 是 LEFT JOIN，查询是 INNER JOIN
 *      → 需要检查查询是否有过滤条件能拒绝 NULL，使得 LEFT JOIN 的行为等价于 INNER JOIN
 * 
 * 6. errorMessage：如果比较失败，记录失败原因
 * 
 * ==================== 使用场景 ====================
 * 
 * 示例1：Join 条件上拉
 * ```sql
 * -- 查询
 * SELECT * FROM orders o
 * JOIN users u ON o.user_id = u.id AND o.status = 'active'
 * WHERE o.create_time > '2024-01-01'
 * 
 * -- 物化视图
 * CREATE MATERIALIZED VIEW mv AS
 * SELECT * FROM orders o
 * JOIN users u ON o.user_id = u.id AND o.status = 'active'
 * WHERE o.create_time > '2024-01-01'
 * ```
 * 
 * 比较结果：
 * - queryExpressions: [o.status = 'active']（从 Join 条件上拉）
 * - viewExpressions: [o.status = 'active']（从 Join 条件上拉）
 * - queryAllPulledUpExpressions: [o.create_time > '2024-01-01']（从 Filter 上拉）
 * - valid: true（结构兼容）
 * 
 * 示例2：Join 类型推断
 * ```sql
 * -- 查询（INNER JOIN）
 * SELECT * FROM orders o
 * INNER JOIN users u ON o.user_id = u.id
 * WHERE u.name IS NOT NULL
 * 
 * -- 物化视图（LEFT JOIN）
 * CREATE MATERIALIZED VIEW mv AS
 * SELECT * FROM orders o
 * LEFT JOIN users u ON o.user_id = u.id
 * ```
 * 
 * 比较结果：
 * - viewNoNullableSlot: [{u.name}]（查询的 `u.name IS NOT NULL` 可以拒绝 NULL，使得 LEFT JOIN 等价于 INNER JOIN）
 * - valid: true（结构兼容，可以通过 NULL 拒绝条件推断）
 * 
 * ==================== 工作流程 ====================
 * 
 * 1. 在 `HyperGraphComparator.isLogicCompatible()` 中比较查询和物化视图的逻辑计划图
 * 2. 收集从 Join Edge 和 Filter Edge 中上拉的谓词
 * 3. 检查 Join 类型是否兼容，记录需要 NULL 拒绝的 Slot
 * 4. 使用 `ComparisonResult.Builder` 构建 `ComparisonResult` 对象
 * 5. 在 `build()` 方法中过滤掉推断表达式
 * 6. 在 `AbstractMaterializedViewRule.predicatesCompensate()` 中使用比较结果进行谓词补偿
 * 
 * ==================== 注意事项 ====================
 * 
 * 1. 推断表达式的过滤：
 *    - `queryAllPulledUpExpressions` 在 build() 时会过滤掉推断表达式
 *    - 确保物化视图匹配时只使用用户原始查询的谓词，而不是优化器推导的谓词
 * 
 * 2. Join 条件上拉：
 *    - 从 Join 条件中上拉的谓词需要满足一定的条件（如 canPullUp）
 *    - 如果无法上拉，比较结果会标记为 invalid
 * 
 * 3. Join 类型推断：
 *    - 某些 Join 类型可以通过 NULL 拒绝条件进行推断（如 LEFT JOIN → INNER JOIN）
 *    - 这需要在 `viewNoNullableSlot` 中记录相关信息
 */
public class ComparisonResult {
    /** 比较结果是否有效。true 表示查询和物化视图的逻辑计划图结构兼容，可以继续重写；false 表示结构不兼容，无法重写 */
    private final boolean valid;
    
    /** 从物化视图的 Join Edge 中上拉的谓词。这些谓词原本在物化视图的 Join 条件中，但可以上拉到 Filter */
    private final List<Expression> viewExpressions;
    
    /** 从查询的 Join Edge 中上拉的谓词。这些谓词原本在查询的 Join 条件中，但可以上拉到 Filter */
    private final List<Expression> queryExpressions;
    
    /** 从查询的 Filter Edge 中上拉的所有谓词。这些谓词原本在查询的 Filter 中，用于后续的谓词补偿。
     *  注意：在 build() 方法中会过滤掉推断表达式（inferred expressions） */
    private final List<Expression> queryAllPulledUpExpressions;
    
    /** 物化视图中需要拒绝 NULL 的 Slot 集合。用于处理 Join 类型不同的情况（如 MV 是 LEFT JOIN，查询是 INNER JOIN） */
    private final Set<Set<Slot>> viewNoNullableSlot;
    
    /** 如果比较失败，记录失败原因 */
    private final String errorMessage;

    ComparisonResult(List<Expression> queryExpressions, List<Expression> queryAllPulledUpExpressions,
            List<Expression> viewExpressions, Set<Set<Slot>> viewNoNullableSlot, boolean valid, String message) {
        this.viewExpressions = ImmutableList.copyOf(viewExpressions);
        this.queryExpressions = ImmutableList.copyOf(queryExpressions);
        this.queryAllPulledUpExpressions = ImmutableList.copyOf(queryAllPulledUpExpressions);
        this.viewNoNullableSlot = ImmutableSet.copyOf(viewNoNullableSlot);
        this.valid = valid;
        this.errorMessage = message;
    }

    /**
     * 创建一个无效的比较结果，并记录错误信息。
     * 
     * @param errorMessage 错误信息，说明比较失败的原因
     * @return 无效的 ComparisonResult 对象（valid=false）
     */
    public static ComparisonResult newInvalidResWithErrorMessage(String errorMessage) {
        return new ComparisonResult(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                ImmutableSet.of(), false, errorMessage);
    }

    /**
     * 获取从物化视图的 Join Edge 中上拉的谓词列表。
     * 
     * @return 物化视图表达式列表
     */
    public List<Expression> getViewExpressions() {
        return viewExpressions;
    }

    /**
     * 获取从查询的 Join Edge 中上拉的谓词列表。
     * 
     * @return 查询表达式列表
     */
    public List<Expression> getQueryExpressions() {
        return queryExpressions;
    }

    /**
     * 获取从查询的 Filter Edge 中上拉的所有谓词列表（已过滤推断表达式）。
     * 
     * 这些谓词用于后续的谓词补偿（predicatesCompensate），确保只使用用户原始查询的谓词。
     * 
     * @return 查询所有上拉表达式列表
     */
    public List<Expression> getQueryAllPulledUpExpressions() {
        return queryAllPulledUpExpressions;
    }

    /**
     * 获取物化视图中需要拒绝 NULL 的 Slot 集合。
     * 
     * 用于处理 Join 类型不同的情况（如 MV 是 LEFT JOIN，查询是 INNER JOIN）。
     * 
     * @return 视图非空 Slot 集合
     */
    public Set<Set<Slot>> getViewNoNullableSlot() {
        return viewNoNullableSlot;
    }

    /**
     * 检查比较结果是否无效。
     * 
     * @return true 如果比较结果无效（结构不兼容），false 如果比较结果有效（结构兼容）
     */
    public boolean isInvalid() {
        return !valid;
    }

    /**
     * 获取错误信息（如果比较失败）。
     * 
     * @return 错误信息，说明比较失败的原因
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * ComparisonResult 的构建器。
     * 
     * 用于逐步构建 ComparisonResult 对象，支持添加查询表达式、视图表达式、上拉谓词等信息。
     * 在 build() 方法中会过滤掉推断表达式，确保物化视图匹配时只使用用户原始查询的谓词。
     */
    public static class Builder {
        /** 构建查询表达式的列表（从 Join Edge 中上拉的谓词） */
        ImmutableList.Builder<Expression> queryBuilder = new ImmutableList.Builder<>();
        
        /** 构建视图表达式的列表（从 Join Edge 中上拉的谓词） */
        ImmutableList.Builder<Expression> viewBuilder = new ImmutableList.Builder<>();
        
        /** 构建视图非空 Slot 集合（用于 Join 类型推断） */
        ImmutableSet.Builder<Set<Slot>> viewNoNullableSlotBuilder = new ImmutableSet.Builder<>();
        
        /** 构建查询所有上拉表达式的列表（从 Filter Edge 中上拉的谓词，会在 build() 中过滤推断表达式） */
        ImmutableList.Builder<Expression> queryAllPulledUpExpressionsBuilder = new ImmutableList.Builder<>();
        
        /** 构建器状态是否有效。如果添加了无效的比较结果，会设置为 false */
        boolean valid = true;

        /**
         * 添加一个 ComparisonResult 对象的内容到构建器中。
         * 
         * 如果添加的比较结果无效，会将构建器状态设置为无效。
         * 
         * @param comparisonResult 要添加的比较结果
         * @return 当前构建器实例（支持链式调用）
         */
        public Builder addComparisonResult(ComparisonResult comparisonResult) {
            if (comparisonResult.isInvalid()) {
                valid = false;
                return this;
            }
            queryBuilder.addAll(comparisonResult.getQueryExpressions());
            viewBuilder.addAll(comparisonResult.getViewExpressions());
            return this;
        }

        /**
         * 添加查询表达式（从查询的 Join Edge 中上拉的谓词）。
         * 
         * @param expressions 要添加的查询表达式集合
         * @return 当前构建器实例（支持链式调用）
         */
        public Builder addQueryExpressions(Collection<? extends Expression> expressions) {
            queryBuilder.addAll(expressions);
            return this;
        }

        /**
         * 添加视图表达式（从物化视图的 Join Edge 中上拉的谓词）。
         * 
         * @param expressions 要添加的视图表达式集合
         * @return 当前构建器实例（支持链式调用）
         */
        public Builder addViewExpressions(Collection<? extends Expression> expressions) {
            viewBuilder.addAll(expressions);
            return this;
        }

        /**
         * 添加物化视图中需要拒绝 NULL 的 Slot 集合。
         * 
         * 用于处理 Join 类型不同的情况（如 MV 是 LEFT JOIN，查询是 INNER JOIN）。
         * 
         * @param viewNoNullableSlotsPair 包含左右两侧需要拒绝 NULL 的 Slot 集合的 Pair
         * @return 当前构建器实例（支持链式调用）
         */
        public Builder addViewNoNullableSlot(Pair<Set<Slot>, Set<Slot>> viewNoNullableSlotsPair) {
            if (!viewNoNullableSlotsPair.first.isEmpty()) {
                viewNoNullableSlotBuilder.add(viewNoNullableSlotsPair.first);
            }
            if (!viewNoNullableSlotsPair.second.isEmpty()) {
                viewNoNullableSlotBuilder.add(viewNoNullableSlotsPair.second);
            }
            return this;
        }

        /**
         * 添加查询所有上拉表达式（从查询的 Filter Edge 中上拉的谓词）。
         * 
         * 注意：这些表达式会在 build() 方法中过滤掉推断表达式。
         * 
         * @param expressions 要添加的查询所有上拉表达式集合
         * @return 当前构建器实例（支持链式调用）
         */
        public Builder addQueryAllPulledUpExpressions(Collection<? extends Expression> expressions) {
            queryAllPulledUpExpressionsBuilder.addAll(expressions);
            return this;
        }

        /**
         * 检查构建器状态是否无效。
         * 
         * @return true 如果构建器状态无效，false 如果构建器状态有效
         */
        public boolean isInvalid() {
            return !valid;
        }

        /**
         * 构建 ComparisonResult 对象。
         * 
         * 该方法会过滤掉推断表达式（inferred expressions），确保物化视图匹配时只使用用户原始查询的谓词。
         * 
         * ==================== 执行流程 ====================
         * 
         * 步骤1：验证构建器状态
         * - 检查 valid 标志，确保所有添加的比较结果都是有效的
         * 
         * 步骤2：构建查询表达式列表（queryExpressions）
         * - 从 queryBuilder 构建查询表达式列表
         * - 这些表达式是从查询的 Join Edge 中上拉的谓词
         * 
         * 步骤3：构建查询所有上拉表达式列表（queryAllPulledUpExpressions）
         * - 从 queryAllPulledUpExpressionsBuilder 构建表达式列表
         * - **关键步骤**：过滤掉推断表达式（`filter(expr -> !expr.isInferred())`）
         * - 这些表达式是从查询的 Filter Edge 中上拉的谓词
         * - 过滤推断表达式的原因：
         *   a) 推断表达式是优化器推导的，不是用户原始查询的一部分
         *   b) 物化视图匹配应该基于用户原始谓词，而不是推断谓词
         *   c) 避免推断谓词影响匹配结果，导致匹配失败或错误匹配
         * 
         * 步骤4：构建视图表达式列表（viewExpressions）
         * - 从 viewBuilder 构建视图表达式列表
         * - 这些表达式是从物化视图的 Join Edge 中上拉的谓词
         * 
         * 步骤5：构建视图非空 Slot 集合（viewNoNullableSlot）
         * - 从 viewNoNullableSlotBuilder 构建 Slot 集合
         * - 用于处理 Join 类型不同的情况（如 MV 是 LEFT JOIN，查询是 INNER JOIN）
         * 
         * ==================== 使用场景 ====================
         * 
         * 示例：查询和物化视图的谓词比较
         * ```sql
         * -- 用户查询
         * SELECT * FROM orders o
         * JOIN users u ON o.user_id = u.id
         * WHERE o.create_time > '2024-01-01' AND o.status = 'active'
         * 
         * -- 优化器可能推断出：o.create_time >= '2024-01-01'（inferred=true）
         * -- 物化视图
         * CREATE MATERIALIZED VIEW mv AS
         * SELECT * FROM orders o
         * JOIN users u ON o.user_id = u.id
         * WHERE o.create_time > '2024-01-01'
         * ```
         * 
         * 执行过程：
         * 1. queryAllPulledUpExpressionsBuilder 收集：
         *    - o.create_time > '2024-01-01'（原始谓词，inferred=false）
         *    - o.status = 'active'（原始谓词，inferred=false）
         *    - o.create_time >= '2024-01-01'（推断谓词，inferred=true）
         * 
         * 2. build() 方法过滤后：
         *    - queryAllPulledUpExpressions: [o.create_time > '2024-01-01', o.status = 'active']
         *    - 推断谓词 o.create_time >= '2024-01-01' 被过滤掉
         * 
         * 3. 后续谓词补偿时，只使用原始谓词进行匹配，确保匹配结果正确
         * 
         * ==================== 注意事项 ====================
         * 
         * 1. 推断表达式的识别：
         *    - 推断表达式由优化器规则（如 AddMinMax、UnequalPredicateInfer）生成
         *    - 这些表达式的 `inferred` 标志应该被设置为 `true`
         *    - 如果推断表达式的 `inferred` 标志为 `false`，可能导致匹配失败
         * 
         * 2. 过滤的必要性：
         *    - 如果不过滤推断表达式，物化视图匹配可能会失败
         *    - 例如：查询没有 `id >= 2`，但物化视图有（由推断生成），匹配时会认为查询缺少这个谓词
         * 
         * 3. queryExpressions vs queryAllPulledUpExpressions：
         *    - queryExpressions：从 Join Edge 中上拉的谓词（已在上游过滤推断表达式）
         *    - queryAllPulledUpExpressions：从 Filter Edge 中上拉的谓词（需要在这里过滤推断表达式）
         * 
         * @return ComparisonResult 对象，包含查询和物化视图的比较结果
         * @throws IllegalArgumentException 如果构建器状态无效（valid=false）
         */
        public ComparisonResult build() {
            Preconditions.checkArgument(valid, "Comparison result must be valid");
            return new ComparisonResult(queryBuilder.build(),
                    // 过滤掉推断表达式，确保物化视图匹配时只使用用户原始查询的谓词
                    // 推断表达式是优化器推导的，不应该用于匹配比较
                    queryAllPulledUpExpressionsBuilder.build().stream()
                            .filter(expr -> !expr.isInferred()).collect(Collectors.toList()),
                    viewBuilder.build(), viewNoNullableSlotBuilder.build(), valid, "");
        }
    }

    @Override
    public String toString() {
        return String.format("valid: %s \n "
                        + "viewExpressions: %s \n "
                        + "queryExpressions :%s \n "
                        + "viewNoNullableSlot :%s \n"
                        + "queryAllPulledUpExpressions :%s \n", valid, viewExpressions, queryExpressions,
                viewNoNullableSlot, queryAllPulledUpExpressions);
    }
}
