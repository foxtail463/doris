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

import org.apache.doris.nereids.rules.exploration.mv.EquivalenceClass;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EquivalenceClassSetMapping
 * This will extract the equivalence class set in EquivalenceClass and mapping set in
 * two different EquivalenceClass.
 */
public class EquivalenceClassMapping extends Mapping {

    private final Map<List<SlotReference>, List<SlotReference>> equivalenceClassSetMap;

    public EquivalenceClassMapping(Map<List<SlotReference>,
            List<SlotReference>> equivalenceClassSetMap) {
        this.equivalenceClassSetMap = equivalenceClassSetMap;
    }

    public static EquivalenceClassMapping of(Map<List<SlotReference>, List<SlotReference>> equivalenceClassSetMap) {
        return new EquivalenceClassMapping(equivalenceClassSetMap);
    }

    /**
     * 生成从 source 等价类到 target 等价类的映射关系。
     * 
     * 该方法用于物化视图重写中的等值谓词补偿，建立查询等价类和MV等价类之间的对应关系。
     * 
     * ==================== 映射规则 ====================
     * 
     * 对于 source 中的每个等价类，如果它包含 target 中某个等价类的所有元素，
     * 则建立映射关系：source等价类 -> target等价类
     * 
     * 条件：sourceSet.containsAll(targetSet)
     *   - sourceSet 必须包含 targetSet 的所有元素
     *   - sourceSet 可以包含额外的元素（查询的等价类可以比MV的更大）
     * 
     * ==================== 示例场景 ====================
     * 
     * 【示例1：查询等价类包含MV等价类】
     * Source（查询等价类）: [[a, b, c]]  （查询有 a=b, b=c）
     * Target（MV等价类）:   [[a, b]]     （MV有 a=b）
     * 
     * 处理：
     *   - sourceSet = {a, b, c}
     *   - targetSet = {a, b}
     *   - sourceSet.containsAll(targetSet) = true ✓
     *   - 建立映射：{[a, b, c] -> [a, b]}
     * 
     * 含义：查询的等价类 [a, b, c] 包含了MV的等价类 [a, b]，
     *       说明查询有更多的等值关系（b=c），需要补偿。
     * 
     * 【示例2：查询等价类等于MV等价类】
     * Source: [[a, b]]  （查询有 a=b）
     * Target: [[a, b]]  （MV有 a=b）
     * 
     * 处理：
     *   - sourceSet = {a, b}
     *   - targetSet = {a, b}
     *   - sourceSet.containsAll(targetSet) = true ✓
     *   - 建立映射：{[a, b] -> [a, b]}
     * 
     * 含义：查询和MV的等价类相同，无需补偿。
     * 
     * 【示例3：查询等价类不包含MV等价类】
     * Source: [[a, b]]     （查询有 a=b）
     * Target: [[a, b, c]]  （MV有 a=b, b=c）
     * 
     * 处理：
     *   - sourceSet = {a, b}
     *   - targetSet = {a, b, c}
     *   - sourceSet.containsAll(targetSet) = false ✗
     *   - 不建立映射
     * 
     * 含义：查询的等价类不包含MV的等价类，说明MV有更多的等值关系，
     *       这种情况在物化视图重写中通常会导致匹配失败。
     * 
     * 【示例4：多个等价类】
     * Source: [[a, b], [c, d]]
     * Target: [[a, b], [c]]
     * 
     * 处理：
     *   - [a, b] 包含 [a, b] → 映射 {[a, b] -> [a, b]}
     *   - [c, d] 包含 [c] → 映射 {[c, d] -> [c]}
     *   - 结果：{[a, b] -> [a, b], [c, d] -> [c]}
     * 
     * ==================== 在物化视图重写中的使用 ====================
     * 
     * 在 Predicates.compensateEquivalence() 中：
     * 1. source = queryEquivalenceClass（查询等价类）
     * 2. target = viewEquivalenceClassQueryBased（MV等价类，已映射到查询侧）
     * 3. 生成映射后，用于判断：
     *    - 如果查询等价类没有映射到MV等价类 → 需要补偿整个查询等价类
     *    - 如果查询等价类映射到MV等价类，但查询更大 → 需要补偿查询多出的部分
     *    - 如果MV等价类无法全部映射到查询等价类 → 匹配失败
     * 
     * @param source 源等价类（通常是查询等价类）
     * @param target 目标等价类（通常是MV等价类，已映射到查询侧）
     * @return EquivalenceClassMapping 对象，包含 source等价类 -> target等价类 的映射关系
     */
    public static EquivalenceClassMapping generate(EquivalenceClass source, EquivalenceClass target) {
        Map<List<SlotReference>, List<SlotReference>> equivalenceClassSetMap = new HashMap<>();
        List<List<SlotReference>> sourceSets = source.getEquivalenceSetList();
        List<List<SlotReference>> targetSets = target.getEquivalenceSetList();

        // 遍历 source 中的每个等价类
        for (List<SlotReference> sourceList : sourceSets) {
            Set<SlotReference> sourceSet = new HashSet<>(sourceList);
            // 遍历 target 中的每个等价类，查找 source 是否包含 target
            for (List<SlotReference> targetList : targetSets) {
                Set<SlotReference> targetSet = new HashSet<>(targetList);
                // 如果 source 等价类包含 target 等价类的所有元素，建立映射关系
                // 注意：source 可以包含额外的元素（查询的等价类可以比MV的更大）
                if (sourceSet.containsAll(targetSet)) {
                    equivalenceClassSetMap.put(sourceList, targetList);
                }
            }
        }
        return EquivalenceClassMapping.of(equivalenceClassSetMap);
    }

    public Map<List<SlotReference>, List<SlotReference>> getEquivalenceClassSetMap() {
        return equivalenceClassSetMap;
    }
}
