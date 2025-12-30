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

import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * EquivalenceClass, this is used for equality propagation when predicate compensation
 */
public class EquivalenceClass {

    /**
     * eg: column a = b
     * this will be
     * {
     * a: [a, b],
     * b: [a, b]
     * }
     * or column a = a,
     * this would be
     * {
     * a: [a, a]
     * }
     */
    private Map<SlotReference, List<SlotReference>> equivalenceSlotMap = new LinkedHashMap<>();
    private List<List<SlotReference>> equivalenceSlotList;

    public EquivalenceClass() {
    }

    public EquivalenceClass(Map<SlotReference, List<SlotReference>> equivalenceSlotMap) {
        this.equivalenceSlotMap = equivalenceSlotMap;
    }

    /**
     * 添加等价关系：将两个 slot 添加到同一个等价类中。
     * 
     * 该方法实现了类似并查集（Union-Find）的数据结构，用于维护列之间的等价关系。
     * 通过传递性自动合并等价类：如果 a = b 且 b = c，则自动推导出 a = c。
     * 
     * ==================== 数据结构说明 ====================
     * 
     * equivalenceSlotMap: Map<SlotReference, List<SlotReference>>
     *   - key: 等价类中的任意一个 slot
     *   - value: 该 slot 所属的等价类（包含所有等价的 slot 的列表）
     *   - 同一个等价类中的所有 slot 都指向同一个 List 对象（共享引用）
     * 
     * 示例：
     *   初始状态：{}
     *   添加 a = b: {a -> [a, b], b -> [a, b]}
     *   添加 b = c: {a -> [a, b, c], b -> [a, b, c], c -> [a, b, c]}
     * 
     * ==================== 处理逻辑（四种情况）====================
     * 
     * 【情况1：两个 slot 都已存在于不同的等价类中】
     *   需要合并两个等价类（传递性）
     *   例如：已有 a = b（等价类 [a, b]）和 c = d（等价类 [c, d]），现在添加 b = c
     *   结果：合并为 [a, b, c, d]
     *   
     *   优化：选择较大的等价类作为目标，将较小的等价类合并到较大的中（减少更新次数）
     * 
     * 【情况2：leftSlot 已存在，rightSlot 不存在】
     *   将 rightSlot 添加到 leftSlot 的等价类中
     *   例如：已有 a = b（等价类 [a, b]），现在添加 a = c
     *   结果：[a, b, c]
     * 
     * 【情况3：rightSlot 已存在，leftSlot 不存在】
     *   将 leftSlot 添加到 rightSlot 的等价类中
     *   例如：已有 b = c（等价类 [b, c]），现在添加 a = b
     *   结果：[a, b, c]
     * 
     * 【情况4：两个 slot 都不存在】
     *   创建新的等价类，包含这两个 slot
     *   例如：添加 a = b（之前没有任何等价关系）
     *   结果：创建新等价类 [a, b]
     * 
     * ==================== 示例流程 ====================
     * 
     * 步骤1：addEquivalenceClass(a, b)
     *   情况4：两个都不存在
     *   结果：{a -> [a, b], b -> [a, b]}
     * 
     * 步骤2：addEquivalenceClass(b, c)
     *   情况3：b 已存在（在 [a, b] 中），c 不存在
     *   结果：{a -> [a, b, c], b -> [a, b, c], c -> [a, b, c]}
     * 
     * 步骤3：addEquivalenceClass(d, e)
     *   情况4：两个都不存在
     *   结果：{a -> [a, b, c], b -> [a, b, c], c -> [a, b, c], d -> [d, e], e -> [d, e]}
     * 
     * 步骤4：addEquivalenceClass(c, d)
     *   情况1：c 在 [a, b, c] 中，d 在 [d, e] 中，需要合并
     *   选择较大的等价类 [a, b, c]（size=3）作为目标
     *   将 [d, e] 合并到 [a, b, c] 中
     *   结果：{a -> [a, b, c, d, e], b -> [a, b, c, d, e], c -> [a, b, c, d, e],
     *         d -> [a, b, c, d, e], e -> [a, b, c, d, e]}
     * 
     * ==================== 用途 ====================
     * 
     * 在物化视图重写中，用于：
     * 1. 记录查询和MV中的等值关系（如 a = b, b = c）
     * 2. 通过传递性自动推导出隐含的等值关系（如 a = c）
     * 3. 在等值谓词补偿时，判断查询和MV的等价类差异，生成补偿谓词
     * 
     * @param leftSlot 等值关系的左操作数（列引用）
     * @param rightSlot 等值关系的右操作数（列引用）
     */
    public void addEquivalenceClass(SlotReference leftSlot, SlotReference rightSlot) {
        // 查找两个 slot 当前所属的等价类
        List<SlotReference> leftSlotSet = equivalenceSlotMap.get(leftSlot);
        List<SlotReference> rightSlotSet = equivalenceSlotMap.get(rightSlot);
        
        // 情况1：两个 slot 都已存在于不同的等价类中，需要合并两个等价类
        if (leftSlotSet != null && rightSlotSet != null) {
            // 优化：选择较大的等价类作为目标，将较小的合并到较大的中（减少更新次数）
            if (leftSlotSet.size() < rightSlotSet.size()) {
                // 交换引用，确保 leftSlotSet 是较大的等价类
                List<SlotReference> tmp = rightSlotSet;
                rightSlotSet = leftSlotSet;
                leftSlotSet = tmp;
            }
            // 将较小等价类中的所有 slot 合并到较大等价类中
            for (SlotReference newRef : rightSlotSet) {
                leftSlotSet.add(newRef);
                // 更新较小等价类中每个 slot 的映射，指向合并后的等价类
                equivalenceSlotMap.put(newRef, leftSlotSet);
            }
        } 
        // 情况2：leftSlot 已存在，rightSlot 不存在，将 rightSlot 添加到 leftSlot 的等价类中
        else if (leftSlotSet != null) {
            leftSlotSet.add(rightSlot);
            equivalenceSlotMap.put(rightSlot, leftSlotSet);
        } 
        // 情况3：rightSlot 已存在，leftSlot 不存在，将 leftSlot 添加到 rightSlot 的等价类中
        else if (rightSlotSet != null) {
            rightSlotSet.add(leftSlot);
            equivalenceSlotMap.put(leftSlot, rightSlotSet);
        } 
        // 情况4：两个 slot 都不存在，创建新的等价类
        else {
            List<SlotReference> equivalenceClass = new ArrayList<>();
            equivalenceClass.add(leftSlot);
            equivalenceClass.add(rightSlot);
            // 两个 slot 都指向同一个等价类列表（共享引用）
            equivalenceSlotMap.put(leftSlot, equivalenceClass);
            equivalenceSlotMap.put(rightSlot, equivalenceClass);
        }
    }

    public Map<SlotReference, List<SlotReference>> getEquivalenceSlotMap() {
        return equivalenceSlotMap;
    }

    public boolean isEmpty() {
        return equivalenceSlotMap.isEmpty();
    }

    /**
     * EquivalenceClass permute
     */
    public EquivalenceClass permute(Map<SlotReference, SlotReference> mapping) {

        Map<SlotReference, List<SlotReference>> permutedEquivalenceSlotMap = new HashMap<>();
        for (Map.Entry<SlotReference, List<SlotReference>> slotReferenceSetEntry : equivalenceSlotMap.entrySet()) {
            SlotReference mappedSlotReferenceKey = mapping.get(slotReferenceSetEntry.getKey());
            if (mappedSlotReferenceKey == null) {
                // can not permute then need to return null
                return null;
            }
            List<SlotReference> equivalenceValueSet = slotReferenceSetEntry.getValue();
            final List<SlotReference> mappedSlotReferenceSet = new ArrayList<>();
            for (SlotReference target : equivalenceValueSet) {
                SlotReference mappedSlotReferenceValue = mapping.get(target);
                if (mappedSlotReferenceValue == null) {
                    return null;
                }
                mappedSlotReferenceSet.add(mappedSlotReferenceValue);
            }
            permutedEquivalenceSlotMap.put(mappedSlotReferenceKey, mappedSlotReferenceSet);
        }
        return new EquivalenceClass(permutedEquivalenceSlotMap);
    }

    /**
     * Return the list of equivalence list, remove duplicate
     */
    public List<List<SlotReference>> getEquivalenceSetList() {
        if (equivalenceSlotList != null) {
            return equivalenceSlotList;
        }
        List<List<SlotReference>> equivalenceSets = new ArrayList<>();
        List<List<SlotReference>> visited = new ArrayList<>();
        equivalenceSlotMap.values().forEach(slotSet -> {
            if (!visited.contains(slotSet)) {
                equivalenceSets.add(slotSet);
            }
            visited.add(slotSet);
        });
        this.equivalenceSlotList = equivalenceSets;
        return this.equivalenceSlotList;
    }

    @Override
    public String toString() {
        return "EquivalenceClass{" + "equivalenceSlotMap=" + equivalenceSlotMap + '}';
    }
}
