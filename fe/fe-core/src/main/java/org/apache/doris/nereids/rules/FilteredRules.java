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

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.trees.SuperClassId;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** FilteredRules */
public class FilteredRules extends Rules {
    private BitSet typePatternIds;
    private Map<Class<?>, List<Rule>> classToRules = new ConcurrentHashMap<>();
    private List<Rule> typeFilterRules;
    private List<Rule> otherRules;
    private List<Rule> allRules;

    /** FilteredRules */
    public FilteredRules(List<Rule> rules) {
        super(rules);
        this.allRules = Utils.fastToImmutableList(rules);

        this.typeFilterRules = new ArrayList<>();
        this.otherRules = new ArrayList<>();

        BitSet typePatternIds = new BitSet();
        for (Rule rule : rules) {
            Pattern<? extends Plan> pattern = rule.getPattern();
            Class<? extends Plan> topType = pattern.getMatchedType();
            if (topType != null) {
                typePatternIds.set(SuperClassId.getClassId(topType));
                typeFilterRules.add(rule);
            } else {
                otherRules.add(rule);
            }
        }
        this.typePatternIds = typePatternIds;
    }

    @Override
    public List<Rule> getCurrentAndChildrenRules(TreeNode<?> treeNode) {
        // 如果本组规则中没有“无类型约束”的规则（otherRules 为空），
        // 就可以先用类型位图快速判断：当前节点及其子树的所有节点类型集合（classTypes）
        // 是否与本组规则感兴趣的类型集合（typePatternIds）有交集。
        if (otherRules.isEmpty()) {
            BitSet classTypes = treeNode.getAllChildrenTypes();
            // 如果两者没有任何交集，说明这棵子树不可能匹配到本组规则中的任何一条，
            // 直接返回空列表，后续就不会再尝试对这棵树应用这些规则，减少无效匹配开销。
            if (!typePatternIds.intersects(classTypes)) {
                return ImmutableList.of();
            }
        }
        // 存在交集（或者本身有不依赖类型过滤的规则）时，直接返回整组规则，
        // 由上层再根据具体模式去做精确匹配。
        return rules;
    }

    /**
     * 获取适用于当前树节点的规则列表
     * 
     * 该方法是FilteredRules的核心逻辑，负责根据树节点的类型和规则模式
     * 筛选出适用的重写规则。主要流程包括：
     * 1. 其他规则检查：如果存在其他规则，直接返回所有规则
     * 2. 类型匹配检查：检查树节点的类型是否与规则模式匹配
     * 3. 缓存查找：从缓存中查找已匹配的规则
     * 4. 规则匹配：动态匹配规则并缓存结果
     * 5. 性能优化：使用缓存避免重复计算
     * 
     * 规则匹配的特点：
     * - 基于类型继承关系进行匹配
     * - 使用缓存提高性能
     * - 支持类型模式匹配
     * - 避免不必要的规则应用
     * 
     * @param treeNode 当前树节点，用于确定适用的规则
     * @return 适用于当前树节点的规则列表
     */
    @Override
    public List<Rule> getCurrentRules(TreeNode<?> treeNode) {
        // 其他规则检查：如果存在其他规则，直接返回所有规则（不进行过滤）
        if (otherRules.isEmpty()) {
            // 获取树节点的超类类型：用于快速类型匹配检查
            BitSet classTypes = treeNode.getSuperClassTypes();
            
            // 类型模式匹配检查：检查树节点类型是否与规则模式匹配
            if (!typePatternIds.intersects(classTypes)) {
                // 没有匹配的类型模式，返回空规则列表
                return ImmutableList.of();
            } else {
                // 获取树节点的具体类型：用于精确规则匹配
                Class<?> treeClass = treeNode.getClass();
                
                // 缓存查找：从缓存中查找已匹配的规则，提高性能
                List<Rule> rulesCache = classToRules.get(treeClass);
                if (rulesCache != null) {
                    return rulesCache;
                }
                
                // 动态规则匹配：根据类型继承关系匹配规则并缓存结果
                return classToRules.computeIfAbsent(treeClass, c -> {
                    ImmutableList.Builder<Rule> matchedTopTypeRules = ImmutableList.builder();
                    
                    // 遍历所有规则：检查每个规则是否适用于当前类型
                    for (Rule rule : this.rules) {
                        Class<? extends Plan> type = rule.getPattern().getMatchedType();
                        // 类型继承检查：检查当前类型是否是指定类型的子类或实现类
                        if (type.isAssignableFrom(c)) {
                            matchedTopTypeRules.add(rule);
                        }
                    }
                    return matchedTopTypeRules.build();
                });
            }
        }
        
        // 返回所有规则：如果存在其他规则，不进行过滤，直接返回所有规则
        return this.rules;
    }

    @Override
    public List<Rule> getAllRules() {
        return allRules;
    }

    @Override
    public List<Rule> filterValidRules(CascadesContext cascadesContext) {
        BitSet disableRules = cascadesContext.getAndCacheDisableRules();
        if (disableRules.isEmpty()) {
            return allRules;
        }
        List<Rule> validRules = new ArrayList<>(allRules);
        for (Rule rule : allRules) {
            if (!disableRules.get(rule.getRuleType().type())) {
                validRules.add(rule);
            }
        }
        return validRules;
    }

    @Override
    public String toString() {
        return "rules: " + rules.stream()
                .map(r -> r.getRuleType().name())
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
