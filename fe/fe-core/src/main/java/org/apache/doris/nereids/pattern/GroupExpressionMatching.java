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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * 在一个 GroupExpression 上，枚举所有与给定 Pattern 匹配的子树。
 * 结果以迭代器形式输出，每个元素是一棵具体的 plan 树（用真实子计划替换 GroupPlan）。
 */
public class GroupExpressionMatching implements Iterable<Plan> {
    private final Pattern<Plan> pattern;
    private final GroupExpression groupExpression;

    public GroupExpressionMatching(Pattern<? extends Plan> pattern, GroupExpression groupExpression) {
        this.pattern = (Pattern<Plan>) Objects.requireNonNull(pattern, "pattern can not be null");
        this.groupExpression = Objects.requireNonNull(groupExpression, "groupExpression can not be null");
    }

    @Override
    public GroupExpressionIterator iterator() {
        return new GroupExpressionIterator(pattern, groupExpression);
    }

    /** 迭代器：产出所有匹配到的子树 plan。 */
    public static class GroupExpressionIterator implements Iterator<Plan> {
        private final List<Plan> results = Lists.newArrayList();
        private int resultIndex = 0;
        private int resultsSize;

        /**
         * Constructor.
         *
         * @param pattern pattern to match
         * @param groupExpression group expression to be matched
         */
        public GroupExpressionIterator(Pattern<Plan> pattern, GroupExpression groupExpression) {
            // 1) 根节点形状必须先匹配上（只看 operator / predicates 不展开孩子）
            if (!pattern.matchRoot(groupExpression.getPlan())) {
                return;
            }

            int childrenGroupArity = groupExpression.arity(); // 实际子 Group 数
            int patternArity = pattern.arity();                // pattern 的子模式数

            // 2) 非子树模式下，子数目要兼容（考虑 multi/multiGroup 作为“0 个或多个”）
            if (!(pattern instanceof SubTreePattern)) {
                // 形如 (filter, multi()) 可匹配 (filter)；但 (filter, filter, multi()) 不能匹配 (filter)
                boolean extraMulti = patternArity == childrenGroupArity + 1
                        && (pattern.hasMultiChild() || pattern.hasMultiGroupChild());
                if (patternArity > childrenGroupArity && !extraMulti) {
                    return;
                }

                // 形如 (multi()) 可匹配 (filter, filter)；但 (filter) 不能匹配 (filter, filter)
                if (!pattern.isAny() && patternArity < childrenGroupArity
                        && !pattern.hasMultiChild() && !pattern.hasMultiGroupChild()) {
                    return;
                }
            }

            // 3) GROUP/MULTI/MULTI_GROUP 这些“占位”模式不能直接匹配 GroupExpression 自身
            if (pattern.isGroup() || pattern.isMulti() || pattern.isMultiGroup()) {
                return;
            }

            // 4) 取出当前 root 计划（子节点还是 GroupPlan 占位）
            Plan root = groupExpression.getPlan();

            // 5) 无子模式（如 ANY）且非 SubTreePattern：只要根谓词匹配即可命中
            if (patternArity == 0 && !(pattern instanceof SubTreePattern)) {
                if (pattern.matchPredicates(root)) {
                    // 没有子模式时，默认把所有孩子视作 GROUP，占位即可
                    results.add(root);
                }

            // 6) 有子 Group 时，逐子匹配并做笛卡尔组合
            } else if (childrenGroupArity > 0) {
                // childrenPlans[i]：第 i 个子 Group 匹配到的所有 plan
                List<Plan>[] childrenPlans = new List[childrenGroupArity];
                for (int i = 0; i < childrenGroupArity; ++i) {
                    Group childGroup = groupExpression.child(i);
                    List<Plan> childrenPlan = matchingChildGroup(pattern, childGroup, i);

                    if (childrenPlan.isEmpty()) {
                        if (pattern instanceof SubTreePattern) {
                            // SubTreePattern 允许直接把子 Group 占位成 GroupPlan
                            childrenPlan = ImmutableList.of(new GroupPlan(childGroup));
                        } else {
                            // 根匹配但某个子匹配失败，整体失败
                            return;
                        }
                    }
                    childrenPlans[i] = childrenPlan;
                }
                // 将根与各子候选做笛卡尔积，拼出所有匹配的具体 plan 树
                assembleAllCombinationPlanTree(root, pattern, groupExpression, childrenPlans);

            // 7) 叶子 Group 但 pattern 允许 multi()/multiGroup（零个或多个孩子）
            } else if (patternArity == 1 && (pattern.hasMultiChild() || pattern.hasMultiGroupChild())) {
                // 如 logicalPlan(multi()) 可匹配 LogicalOlapScan（把“零个孩子”视为匹配）
                if (pattern.matchPredicates(root)) {
                    results.add(root);
                }
            }
            this.resultsSize = results.size();
        }

        // 针对第 childIndex 个子 Group，用对应子模式去匹配，返回该子 Group 的所有匹配 plan
        private List<Plan> matchingChildGroup(Pattern<? extends Plan> parentPattern,
                Group childGroup, int childIndex) {
            Pattern<? extends Plan> childPattern;
            if (parentPattern instanceof SubTreePattern) {
                childPattern = parentPattern;
            } else {
                boolean isLastPattern = childIndex + 1 >= parentPattern.arity();
                int patternChildIndex = isLastPattern ? parentPattern.arity() - 1 : childIndex;

                childPattern = parentPattern.child(patternChildIndex);
                // 把 MULTI/MULTI_GROUP 翻译成 ANY/GROUP（多余的孩子都归入最后一个子模式）
                if (isLastPattern) {
                    if (childPattern.isMulti()) {
                        childPattern = Pattern.ANY;
                    } else if (childPattern.isMultiGroup()) {
                        childPattern = Pattern.GROUP;
                    }
                }
            }

            // 递归调用 GroupMatching，在子 Group 内枚举所有匹配当前子模式的 plan
            List<Plan> matchingChildren = GroupMatching.getAllMatchingPlans(childPattern, childGroup);
            return matchingChildren;
        }

        // 把根 + 各子匹配结果做笛卡尔积，生成所有匹配的具体 plan 树
        private void assembleAllCombinationPlanTree(Plan root, Pattern<Plan> rootPattern,
                GroupExpression groupExpression, List<Plan>[] childrenPlans) {
            int childrenPlansSize = childrenPlans.length;
            int[] childrenPlanIndex = new int[childrenPlansSize];
            int offset = 0;
            LogicalProperties logicalProperties = groupExpression.getOwnerGroup().getLogicalProperties();

            // assemble all combination of plan tree by current root plan and children plan
            Optional<GroupExpression> groupExprOption = Optional.of(groupExpression);
            Optional<LogicalProperties> logicalPropOption = Optional.of(logicalProperties);
            while (offset < childrenPlansSize) {
                // 取当前索引组合的一组子计划
                ImmutableList.Builder<Plan> childrenBuilder = ImmutableList.builderWithExpectedSize(childrenPlansSize);
                for (int i = 0; i < childrenPlansSize; i++) {
                    childrenBuilder.add(childrenPlans[i].get(childrenPlanIndex[i]));
                }
                List<Plan> children = childrenBuilder.build();

                // assemble children: replace GroupPlan to real plan,
                // withChildren will erase groupExpression, so we must
                // withGroupExpression too.
                // 将 GroupPlan 占位替换为真实子 plan，并带上 groupExpr / logicalProp 信息
                Plan rootWithChildren = root.withGroupExprLogicalPropChildren(groupExprOption,
                        logicalPropOption, children);
                // 最终再用 rootPattern 的谓词校验一遍
                if (rootPattern.matchPredicates(rootWithChildren)) {
                    results.add(rootWithChildren);
                }

                // 递增“混合基数”计数器，遍历下一组子计划组合
                for (offset = 0; offset < childrenPlansSize; offset++) {
                    childrenPlanIndex[offset]++;
                    if (childrenPlanIndex[offset] == childrenPlans[offset].size()) {
                        // Reset the index when it reaches the size of the current child plan list
                        childrenPlanIndex[offset] = 0;
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return resultIndex < resultsSize;
        }

        @Override
        public Plan next() {
            if (!hasNext()) {
                throw new NoSuchElementException("GroupExpressionIterator is empty");
            }
            return results.get(resultIndex++);
        }
    }
}
