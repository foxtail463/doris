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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * 将 InApply 转换为 LogicalJoin 的重写规则。
 * <p>
 * 转换规则：
 * Not In -> NULL_AWARE_LEFT_ANTI_JOIN (空值感知的左反连接)
 * In -> LEFT_SEMI_JOIN (左半连接)
 * 
 * 这个规则的作用是将子查询中的 IN/NOT IN 谓词转换为等价的 JOIN 操作，
 * 这样可以更好地利用优化器的连接优化能力。
 */
public class InApplyToJoin extends OneRewriteRuleFactory {
    /**
     * 构建重写规则。
     * 该规则匹配所有 LogicalApply 节点，且该节点必须是 IN 类型的子查询。
     * 
     * @return 重写规则
     */
    @Override
    public Rule build() {
        // 匹配所有 IN 类型的 LogicalApply 节点
        return logicalApply().when(LogicalApply::isIn).then(apply -> {
            // ========== 情况1：处理 Bitmap 类型的 IN 子查询 ==========
            // 当子查询返回的是 Bitmap 类型，而比较表达式不是 Bitmap 类型时，需要使用 BitmapUnion
            if (needBitmapUnion(apply)) {
                // Bitmap 类型的 IN 子查询不支持相关子查询（correlated subquery）
                if (apply.isCorrelated()) {
                    throw new AnalysisException("In bitmap does not support correlated subquery");
                }
                /*
                 * Bitmap 优化的转换示例：
                 * 
                 * 示例1：IN 查询
                 * 原始SQL: select t1.k1 from bigtable t1 where t1.k1 in (select t2.k2 from bitmap_table t2);
                 * 第一步: select t1.k1 from bigtable t1 where t1.k1 in (select bitmap_union(k2) from bitmap_table t2);
                 * 最终: select t1.k1 from bigtable t1 left semi join (select bitmap_union(k2) x from bitmap_table ) t2
                 *       on bitmap_contains(x, t1.k1);
                 *
                 * 示例2：NOT IN 查询
                 * 原始SQL: select t1.k1 from bigtable t1 where t1.k1 not in (select t2.k2 from bitmap_table t2);
                 * 第一步: select t1.k1 from bigtable t1 where t1.k1 not in (select bitmap_union(k2) from bitmap_table t2);
                 * 最终: select t1.k1 from bigtable t1 left semi join (select bitmap_union(k2) x from bitmap_table ) t2
                 *       on not bitmap_contains(x, t1.k1);
                 */
                // 创建聚合操作：对 Bitmap 列进行 bitmap_union 聚合
                List<Expression> groupExpressions = ImmutableList.of(); // 无分组表达式，表示全局聚合
                Expression bitmapCol = apply.right().getOutput().get(0); // 获取子查询输出的第一个 Bitmap 列
                BitmapUnion union = new BitmapUnion(bitmapCol); // 创建 BitmapUnion 聚合函数
                Alias alias = new Alias(union); // 为聚合结果创建别名
                List<NamedExpression> outputExpressions = Lists.newArrayList(alias); // 聚合的输出表达式列表

                // 创建逻辑聚合节点，对子查询的右子树进行 BitmapUnion 聚合
                LogicalAggregate agg = new LogicalAggregate(groupExpressions, outputExpressions, apply.right());
                
                // 获取 IN 子查询中的比较表达式（即 t1.k1）
                Expression compareExpr = apply.getCompareExpr().get();
                // 创建 BitmapContains 表达式：检查聚合后的 Bitmap 是否包含比较表达式的值
                Expression expr = new BitmapContains(agg.getOutput().get(0), compareExpr);
                
                // 如果是 NOT IN，则对 BitmapContains 结果取反
                if (apply.isNot()) {
                    expr = new Not(expr);
                }
                
                // 创建左半连接（LEFT_SEMI_JOIN），连接条件为 BitmapContains 表达式
                return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                        Lists.newArrayList(expr), // 连接条件列表
                        new DistributeHint(DistributeType.NONE), // 不进行数据分布提示
                        apply.getMarkJoinSlotReference(), // Mark Join 的槽引用
                        apply.left(), agg, null); // 左子节点和右子节点（聚合后的子查询）
            }

            // ========== 情况2：处理普通的 IN/NOT IN 子查询 ==========
            // 将 IN 谓词转换为等值连接
            Expression predicate; // 连接谓词
            Expression left = apply.getCompareExpr().get(); // 获取 IN 子查询左侧的比较表达式
            // TODO: 这里的技巧：当深度复制逻辑计划时，apply 的右子节点
            //  与子查询表达式中的查询计划不同，因为扫描节点被复制了两次
            Expression right = apply.getSubqueryOutput(); // 获取子查询的输出表达式
            
            // ========== 情况2.1：Mark Join 模式 ==========
            // Mark Join 是一种特殊的连接方式，用于标记匹配的行
            if (apply.isMarkJoin()) {
                // 提取关联过滤条件（如果有的话），并将其分解为合取（AND）的表达式列表
                List<Expression> joinConjuncts = apply.getCorrelationFilter().isPresent()
                        ? ExpressionUtils.extractConjunction(apply.getCorrelationFilter().get())
                        : Lists.newArrayList();
                
                // 创建等值比较谓词：left = right
                predicate = new EqualTo(left, right);
                List<Expression> markConjuncts = Lists.newArrayList(predicate); // Mark 连接条件列表
                
                // 在两种情况下，可以将 mark conjuncts 合并到 hash conjuncts 中：
                // 1. mark join 谓词不可为空，因此不会产生 null 值
                // 2. 半连接（semi join）且 mark slot 不可为空
                // 因为半连接只关心 mark slot 为 true 的值，会丢弃 false 和 null
                // 在这种情况下，使用 false 代替 null 是安全的
                if (!predicate.nullable() || (apply.isMarkJoinSlotNotNull() && !apply.isNot())) {
                    joinConjuncts.addAll(markConjuncts); // 将 mark 条件合并到连接条件中
                    markConjuncts.clear(); // 清空 mark 条件列表
                }
                
                // 创建逻辑连接节点
                // 如果是 NOT IN，使用 LEFT_ANTI_JOIN；否则使用 LEFT_SEMI_JOIN
                return new LogicalJoin<>(
                        apply.isNot() ? JoinType.LEFT_ANTI_JOIN : JoinType.LEFT_SEMI_JOIN,
                        Lists.newArrayList(), // 空的分区表达式列表
                        joinConjuncts, // 连接条件（hash 连接条件）
                        markConjuncts, // Mark 连接条件
                        new DistributeHint(DistributeType.NONE), // 不进行数据分布提示
                        apply.getMarkJoinSlotReference(), // Mark Join 的槽引用
                        apply.children(), null); // 子节点列表
            } else {
                // ========== 情况2.2：非 Mark Join 模式 ==========
                // apply.isCorrelated() 只检查是否存在关联槽（correlated slot）
                // 但是关联过滤器可能被 SimplifyConflictCompound 规则消除
                // 所以在创建 LogicalJoin 节点之前，需要同时检查关联槽和关联过滤器是否存在
                if (apply.isCorrelated() && apply.getCorrelationFilter().isPresent()) {
                    // 存在关联条件的情况
                    if (apply.isNot()) {
                        // NOT IN 且有关联条件：
                        // 需要处理 NULL 值的情况，因为 NOT IN 对 NULL 值敏感
                        // 条件为：(left = right OR left IS NULL OR right IS NULL) AND correlation_filter
                        predicate = ExpressionUtils.and(ExpressionUtils.or(new EqualTo(left, right),
                                        new IsNull(left), new IsNull(right)),
                                apply.getCorrelationFilter().get());
                    } else {
                        // IN 且有关联条件：
                        // 条件为：(left = right) AND correlation_filter
                        predicate = ExpressionUtils.and(new EqualTo(left, right),
                                apply.getCorrelationFilter().get());
                    }
                } else {
                    // 无关联条件的情况，直接使用等值比较
                    predicate = new EqualTo(left, right);
                }

                // 将谓词分解为合取（AND）的表达式列表
                List<Expression> conjuncts = ExpressionUtils.extractConjunction(predicate);
                
                if (apply.isNot()) {
                    // NOT IN 的情况
                    // 如果谓词可能为空值且不是相关子查询，使用 NULL_AWARE_LEFT_ANTI_JOIN
                    // 否则使用普通的 LEFT_ANTI_JOIN
                    return new LogicalJoin<>(
                            predicate.nullable() && !apply.isCorrelated()
                                    ? JoinType.NULL_AWARE_LEFT_ANTI_JOIN // 空值感知的左反连接
                                    : JoinType.LEFT_ANTI_JOIN, // 普通的左反连接
                            Lists.newArrayList(), // 空的分区表达式列表
                            conjuncts, // 连接条件
                            new DistributeHint(DistributeType.NONE), // 不进行数据分布提示
                            apply.getMarkJoinSlotReference(), // Mark Join 的槽引用
                            apply.children(), null); // 子节点列表
                } else {
                    // IN 的情况，使用左半连接（LEFT_SEMI_JOIN）
                    return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, Lists.newArrayList(),
                            conjuncts, // 连接条件
                            new DistributeHint(DistributeType.NONE), // 不进行数据分布提示
                            apply.getMarkJoinSlotReference(), // Mark Join 的槽引用
                            apply.children(), null); // 子节点列表
                }
            }
        }).toRule(RuleType.IN_APPLY_TO_JOIN);
    }

    /**
     * 判断是否需要使用 BitmapUnion 优化。
     * 
     * 需要 BitmapUnion 的条件：
     * 1. 子查询输出的第一个列是 Bitmap 类型
     * 2. 比较表达式（IN 左侧的表达式）不是 Bitmap 类型
     * 
     * @param apply 待检查的 LogicalApply 节点
     * @return 如果需要 BitmapUnion 优化，返回 true；否则返回 false
     */
    private boolean needBitmapUnion(LogicalApply<Plan, Plan> apply) {
        return apply.right().getOutput().get(0).getDataType().isBitmapType()
                && !apply.getCompareExpr().get().getDataType().isBitmapType();
    }
}
