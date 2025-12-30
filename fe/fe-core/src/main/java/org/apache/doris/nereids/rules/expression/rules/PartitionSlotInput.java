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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * PartitionSlotInput, the input of the partition slot.
 * We will replace the partition slot to PartitionSlotInput#result in the partition predicate,
 * so that we can evaluate the expression tree.
 *
 * 核心作用：为每个分区创建输入，将抽象的分区列替换为具体值，然后计算查询条件是否匹配该分区
 *
 * 直观例子1 - 列表分区：
 * 表定义：
 * CREATE TABLE orders (id INT, name VARCHAR) PARTITION BY LIST(region) (
 *     PARTITION p_north VALUES IN ('north'),
 *     PARTITION p_south VALUES IN ('south'),
 *     PARTITION p_east VALUES IN ('east'),
 *     PARTITION p_west VALUES IN ('west')
 * );
 * 
 * 查询：SELECT * FROM orders WHERE region = 'north';
 * 
 * 为每个分区创建输入：
 * 分区p_north: PartitionSlotInput(result = Literal('north'))  -> 'north' = 'north' = true  ✅ 扫描
 * 分区p_south: PartitionSlotInput(result = Literal('south'))  -> 'south' = 'north' = false ❌ 跳过
 * 分区p_east:  PartitionSlotInput(result = Literal('east'))   -> 'east' = 'north' = false  ❌ 跳过
 * 分区p_west:  PartitionSlotInput(result = Literal('west'))   -> 'west' = 'north' = false  ❌ 跳过
 *
 * 直观例子2 - 范围分区：
 * 表定义：
 * CREATE TABLE sales (id INT, amount DECIMAL) PARTITION BY RANGE(year) (
 *     PARTITION p2022 VALUES LESS THAN (2023),
 *     PARTITION p2023 VALUES LESS THAN (2024),
 *     PARTITION p2024 VALUES LESS THAN (2025)
 * );
 * 
 * 查询：SELECT * FROM sales WHERE year > 2022;
 * 
 * 为每个分区创建输入：
 * 分区p2022: PartitionSlotInput(result = Slot(year), columnRanges = {year: [0,2023)})  -> 部分匹配
 * 分区p2023: PartitionSlotInput(result = Slot(year), columnRanges = {year: [2023,2024)}) -> 完全匹配 ✅ 扫描
 * 分区p2024: PartitionSlotInput(result = Slot(year), columnRanges = {year: [2024,2025)}) -> 完全匹配 ✅ 扫描
 *
 * 原始例子 - 列表分区的值替换：
 * for example, the partition predicate: `part_column1 > 1`, and exist a partition range is [('1'), ('4')),
 * and part_column1 is int type.
 *
 *             GreaterThen                                                 GreaterThen
 *      /                      \                  ->                 /                    \
 * Slot(part_column1)       IntegerLiteral(1)                IntegerLiteral(n)       IntegerLiteral(1)
 *     |                                                           ^
 *     |                                                           |
 *     +------------------------------------------------------------
 *                             |
 *                        replace by
 *      PartitionSlotInput(result = IntegerLiteral(1))
 *      PartitionSlotInput(result = IntegerLiteral(2))
 *      PartitionSlotInput(result = IntegerLiteral(3))
 *
 *
 * if the partition slot is not enumerable(some RANGE / all OTHER partition slot type), we will stay slot:
 * PartitionSlotInput(result = Slot(part_column1))
 */
public class PartitionSlotInput {
    // the partition slot will be replaced to this result
    public final Expression result;

    // all partition slot's range map, the example in the class comment, it will be `{Slot(part_column1): [1, 4)}`.
    // this range will use as the initialized partition slot range, every expression has a related columnRange map.
    // as the expression executes, the upper expression' columnRange map will be computed.
    // for example, the predicate `part_column1 > 100 or part_column1 < 0`.
    //
    // the [1, 10000) is too much we default not expand to IntLiterals.
    // this are the process steps:
    //
    //                                               Or
    //                     /                                                        \
    //                 GreaterThen                                                LessThen
    //         /                         \                                  /                    \
    //   part_column1                  IntegerLiteral(100)             part_column1          IntegerLiteral(0)
    // (part_column1: [1,10000))    (part_column1: [1,10000))     (part_column1: [1,10000))  (part_column1: [1,10000))
    //
    //                                                |
    //                                                v
    //
    //                                               Or
    //                     /                                                        \
    //                 GreaterThen                                                LessThen
    //       (part_column1: [1,10000) and (100, +∞))                  (part_column1: [1,10000) and (-∞, 0))
    //         /                    \                                      /                    \
    //   part_column1               IntegerLiteral(100)                 part_column1          IntegerLiteral(0)
    //
    //                                                |
    //                                                v
    //
    //                                               Or
    //                     /                                                        \
    //                 GreaterThen                                                LessThen
    //          (part_column1: (100,10000))                               (part_column1: empty range)
    //         /                    \                                      /                    \
    //   part_column1               IntegerLiteral(100)               part_column1          IntegerLiteral(0)
    //
    //                                                |
    //                                                v
    //
    //                                               Or
    //                     /                                                        \
    //                 GreaterThen                                      BooleanLiteral.FALSE    <- empty set to false
    //        (part_column1: (100,10000))
    //         /                    \
    //   part_column1               IntegerLiteral(100)
    //
    //                                                |
    //                                                v
    //
    //                                               Or
    //                               (part_column1: (100,10000) or empty range)
    //                     /                                                        \
    //                 GreaterThen                                              BooleanLiteral.FALSE
    //         /                    \
    //   part_column1               IntegerLiteral(100)
    //
    //                                                |
    //                                                v
    //
    //                                         GreaterThen                <- fold `expr or false` to expr
    //                                  (part_column1: (100,10000))       <- merge columnRanges
    //                                 /                         \
    //                          part_column1                    IntegerLiteral(100)
    //
    // because we can't fold this predicate to BooleanLiteral.FALSE, so we should scan the partition.
    public final Map<Expression, ColumnRange> columnRanges;

    public PartitionSlotInput(Expression result, Map<Expression, ColumnRange> columnRanges) {
        this.result = result;
        this.columnRanges = ImmutableMap.copyOf(columnRanges);
    }
}
