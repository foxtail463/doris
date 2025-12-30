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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Expression mapping, maybe one expression map to multi expression
 */
public class ExpressionMapping extends Mapping {
    private final Multimap<Expression, Expression> expressionMapping;

    public ExpressionMapping(Multimap<Expression, Expression> expressionMapping) {
        this.expressionMapping = expressionMapping;
    }

    public Multimap<Expression, Expression> getExpressionMapping() {
        return expressionMapping;
    }

    /**
     * ExpressionMapping flatten
     */
    public List<Map<Expression, Expression>> flattenMap() {
        List<List<Pair<Expression, Expression>>> tmpExpressionPairs = new ArrayList<>(this.expressionMapping.size());
        Map<? extends Expression, ? extends Collection<? extends Expression>> expressionMappingMap =
                expressionMapping.asMap();
        for (Map.Entry<? extends Expression, ? extends Collection<? extends Expression>> entry
                : expressionMappingMap.entrySet()) {
            List<Pair<Expression, Expression>> targetExpressionList = new ArrayList<>(entry.getValue().size());
            for (Expression valueExpression : entry.getValue()) {
                targetExpressionList.add(Pair.of(entry.getKey(), valueExpression));
            }
            tmpExpressionPairs.add(targetExpressionList);
        }
        List<List<Pair<Expression, Expression>>> cartesianExpressionMap = Lists.cartesianProduct(tmpExpressionPairs);

        final List<Map<Expression, Expression>> flattenedMap = new ArrayList<>();
        for (List<Pair<Expression, Expression>> listPair : cartesianExpressionMap) {
            final Map<Expression, Expression> expressionMap = new HashMap<>();
            listPair.forEach(pair -> expressionMap.put(pair.key(), pair.value()));
            flattenedMap.add(expressionMap);
        }
        return flattenedMap;
    }

    /**
     * 将 ExpressionMapping 的 key（表达式）从一个 slot 空间转换到另一个 slot 空间。
     * 
     * 核心作用：通过 slotMapping 替换 key 表达式中的 slot，实现 slot 空间的转换。
     * 
     * ==================== 示例场景（MV 重写）====================
     * 
     * 原始 ExpressionMapping（MV slot 空间 -> MV scan 输出）:
     *   {user_id#10 -> user_id#10, date_trunc(create_time#11, 'day') -> dt#12}
     *   key: MV 计划中的表达式（使用 MV 的 slot，如 user_id#10）
     *   value: MV scan 的输出 slot（如 user_id#10, dt#12）
     * 
     * SlotMapping（MV slot -> 查询 slot，基表列空间）:
     *   {user_id#10 -> user_id#0, create_time#11 -> create_time#0}
     * 
     * Permute 后（查询 slot 空间 -> MV scan 输出）:
     *   {user_id#0 -> user_id#10, date_trunc(create_time#0, 'day') -> dt#12}
     *   key: 查询中的表达式（使用查询的 slot，如 user_id#0），都统一到基表列空间
     *   value: MV scan 的输出 slot（不变）
     * 
     * 处理过程：
     * 1. user_id#10 -> user_id#10
     *    替换 key 中的 user_id#10 -> user_id#0（根据 slotMapping）
     *    结果：{user_id#0 -> user_id#10}
     * 
     * 2. date_trunc(create_time#11, 'day') -> dt#12
     *    替换 key 中的 create_time#11 -> create_time#0（根据 slotMapping）
     *    结果：{date_trunc(create_time#0, 'day') -> dt#12}
     * 
     * 为什么需要 permute：
     * - 查询表达式使用的是查询的 slot（如 user_id#0），而原始 mapping 的 key 使用的是 MV 的 slot（如 user_id#10）
     * - 需要统一到同一个 slot 空间（基表列空间），才能匹配和替换
     * - 这样当查询中有 user_id#0 时，可以用映射找到对应的 MV scan 输出 user_id#10
     * 
     * @param slotMapping slot 映射关系，用于替换 key 表达式中的 slot
     * @return 新的 ExpressionMapping，key 已经被 permute 到目标 slot 空间
     */
    public ExpressionMapping keyPermute(SlotMapping slotMapping) {
        Multimap<Expression, Expression> permutedExpressionMapping = ArrayListMultimap.create();
        Map<? extends Expression, ? extends Collection<? extends Expression>> expressionMap =
                this.getExpressionMapping().asMap();
        for (Map.Entry<? extends Expression, ? extends Collection<? extends Expression>> entry :
                expressionMap.entrySet()) {
            // 将 slotMapping 转换为 SlotReference 到 SlotReference 的映射，用于替换表达式中的 slot
            Map<SlotReference, SlotReference> slotReferenceMap = slotMapping.toSlotReferenceMap();
            // 尝试替换 key 表达式中的 slot：将 MV slot 替换为查询 slot（基表列空间）
            // 例如：将 user_id#10 替换为 user_id#0
            Expression replacedExpr = ExpressionUtils.replaceNullAware(entry.getKey(), slotReferenceMap);
            // 如果替换失败（replacedExpr == null），说明该表达式中包含的 slot 不在 slotMapping 中
            // 在部分重写场景中，MV 计划的输出可能多于查询输出，需要丢弃不相关的表达式
            if (replacedExpr == null) {
                continue;
            }
            // 将替换后的 key 和原来的 value 放入新的 mapping
            // replacedExpr: 替换后的 key（查询 slot 空间）
            // entry.getValue(): 原来的 value（MV scan 输出），保持不变
            permutedExpressionMapping.putAll(replacedExpr, entry.getValue());
        }
        return new ExpressionMapping(permutedExpressionMapping);
    }

    /**
     * ExpressionMapping generate
     */
    public static ExpressionMapping generate(
            List<? extends Expression> sourceExpressions,
            List<? extends Expression> targetExpressions) {
        final Multimap<Expression, Expression> expressionMultiMap =
                ArrayListMultimap.create();
        for (int i = 0; i < sourceExpressions.size(); i++) {
            expressionMultiMap.put(sourceExpressions.get(i), targetExpressions.get(i));
        }
        return new ExpressionMapping(expressionMultiMap);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("ExpressionMapping", "expressionMapping", expressionMapping);
    }
}
