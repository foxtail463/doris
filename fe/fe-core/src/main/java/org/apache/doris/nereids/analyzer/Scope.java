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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Suppliers;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 表达式分析所需的 Slot 作用域。
 *
 * slots: 当前层级使用的符号（列名）存储在 slots 中。
 * outerScope: 父级对应的作用域信息存储在 outerScope 中（用于相关子查询）。
 * ownerSubquery: 对应的子查询。
 * subqueryToOuterCorrelatedSlots: 子查询中相关的 Slot（相关列），
 *                                 只包含在当前层级无法解析的 Slot（即来自外层查询的列）。
 *
 * 示例：
 * t1(k1, v1) / t2(k2, v2)
 * select * from t1 where t1.k1 = (select sum(k2) from t2 where t1.v1 = t2.v2);
 *
 * 当分析子查询时：
 *
 * slots: k2, v2;  // 子查询中的列
 * outerScope:
 *      slots: k1, v1;  // 外层查询的列
 *      outerScope: Optional.empty();
 *      ownerSubquery: Optional.empty();
 *      subqueryToOuterCorrelatedSlots: empty();
 * ownerSubquery: subquery((select sum(k2) from t2 where t1.v1 = t2.v2));  // 当前子查询
 * subqueryToOuterCorrelatedSlots: (subquery, v1);  // 子查询中引用的外层列 v1
 */
public class Scope {

    // 外层作用域，用于相关子查询。当子查询引用外层查询的列时，可以通过 outerScope 查找
    private final Optional<Scope> outerScope;
    
    // 当前作用域中的所有 Slot（列）。这些 Slot 可以直接在当前查询中使用
    private final List<Slot> slots;
    
    // 用于 SELECT * 展开的 Slot。当处理 SELECT * 时，会从这些 Slot 中展开所有列
    private final List<Slot> asteriskSlots;
    
    // 相关子查询中引用的外层 Slot。当子查询引用外层查询的列时，这些列会被添加到这个集合中
    private final Set<Slot> correlatedSlots;
    
    // 是否构建名称到 Slot 的映射。当 Slot 数量大于 500 时，使用映射可以提高查找性能
    private final boolean buildNameToSlot;
    
    // 名称到 Slot 的映射（延迟计算）。键是列名（大写），值是对应的 Slot 列表
    // 使用 Supplier 延迟计算，只有在需要时才构建映射
    private final Supplier<ListMultimap<String, Slot>> nameToSlot;
    
    // 名称到 Asterisk Slot 的映射（延迟计算）。用于 SELECT * 的快速查找
    private final Supplier<ListMultimap<String, Slot>> nameToAsteriskSlot;

    public Scope(List<Slot> slots) {
        this(Optional.empty(), slots);
    }

    public Scope(Optional<Scope> outerScope, List<Slot> slots) {
        this(outerScope, slots, slots);
    }

    public Scope(List<Slot> slots, List<Slot> asteriskSlots) {
        this(Optional.empty(), slots, asteriskSlots);
    }

    /** Scope */
    public Scope(Optional<Scope> outerScope, List<Slot> slots, List<Slot> asteriskSlots) {
        this.outerScope = Objects.requireNonNull(outerScope, "outerScope can not be null");
        this.slots = Utils.fastToImmutableList(Objects.requireNonNull(slots, "slots can not be null"));
        this.correlatedSlots = Sets.newLinkedHashSet();
        this.buildNameToSlot = slots.size() > 500;
        this.nameToSlot = buildNameToSlot ? Suppliers.memoize(this::buildNameToSlot) : null;
        this.nameToAsteriskSlot = buildNameToSlot ? Suppliers.memoize(this::buildNameToAsteriskSlot) : null;
        this.asteriskSlots = Utils.fastToImmutableList(
                Objects.requireNonNull(asteriskSlots, "asteriskSlots can not be null"));
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public List<Slot> getAsteriskSlots() {
        return asteriskSlots;
    }

    public Optional<Scope> getOuterScope() {
        return outerScope;
    }

    public Set<Slot> getCorrelatedSlots() {
        return correlatedSlots;
    }

    /** findSlotIgnoreCase */
    public List<Slot> findSlotIgnoreCase(String slotName, boolean all) {
        List<Slot> slots = all ? this.slots : this.asteriskSlots;
        Supplier<ListMultimap<String, Slot>> nameToSlot = all ? this.nameToSlot : this.nameToAsteriskSlot;
        if (!buildNameToSlot) {
            Slot[] array = new Slot[slots.size()];
            int filterIndex = 0;
            for (Slot slot : slots) {
                if (slot.getName().equalsIgnoreCase(slotName)) {
                    array[filterIndex++] = slot;
                }
            }
            return Arrays.asList(array).subList(0, filterIndex);
        } else {
            return nameToSlot.get().get(slotName.toUpperCase(Locale.ROOT));
        }
    }

    private ListMultimap<String, Slot> buildNameToSlot() {
        ListMultimap<String, Slot> map = LinkedListMultimap.create(slots.size());
        for (Slot slot : slots) {
            map.put(slot.getName().toUpperCase(Locale.ROOT), slot);
        }
        return map;
    }

    private ListMultimap<String, Slot> buildNameToAsteriskSlot() {
        ListMultimap<String, Slot> map = LinkedListMultimap.create(asteriskSlots.size());
        for (Slot slot : asteriskSlots) {
            map.put(slot.getName().toUpperCase(Locale.ROOT), slot);
        }
        return map;
    }
}
