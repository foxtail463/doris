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

package org.apache.doris.nereids.memo;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.TreeStringUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Representation for group in cascades optimizer.
 */
public class Group {
    /** 
     * Group 的唯一标识符
     * 用于在 Memo 中唯一标识一个 Group，便于查找和引用
     */
    private final GroupId groupId;
    
    /** 
     * 保存所有父 GroupExpression 的映射
     * 
     * 作用：
     * - 记录所有将当前 Group 作为子节点的父 GroupExpression
     * - 避免在需要查找父节点时遍历整个 Memo，提高性能
     * 
     * 使用场景：
     * - Group 合并时，需要更新所有父 GroupExpression 的子节点引用
     * - 自底向上优化时，需要知道哪些父节点使用了当前 Group
     * - 统计信息更新时，需要通知所有父节点
     * 
     * 为什么使用 IdentityHashMap？
     * - 使用对象身份（==）而不是 equals() 进行比较
     * - 因为同一个 GroupExpression 对象可能被多次引用，需要区分不同的引用
     */
    private final IdentityHashMap<GroupExpression, Void> parentExpressions = new IdentityHashMap<>();

    /** 
     * 逻辑表达式列表
     * 存储所有逻辑等价的逻辑计划表达式（LogicalPlan）
     * 例如：LogicalJoin、LogicalFilter、LogicalProject 等
     */
    private final List<GroupExpression> logicalExpressions = Lists.newArrayList();
    
    /** 
     * 物理表达式列表
     * 存储所有物理实现表达式（PhysicalPlan）
     * 例如：PhysicalHashJoin、PhysicalNestedLoopJoin、PhysicalOlapScan 等
     * 这些是逻辑计划的具体物理实现，用于执行
     */
    private final List<GroupExpression> physicalExpressions = Lists.newArrayList();
    
    /** 
     * 强制器（Enforcer）映射
     * 
     * 作用：
     * - 存储用于强制物理属性的特殊 GroupExpression
     * - 当计划不满足所需的物理属性时，需要添加 Enforcer 来强制属性
     * 
     * 示例：
     * - 如果需要数据分布（Distribution），但计划没有，可以添加 Exchange 节点
     * - 如果需要排序（Order），但计划没有，可以添加 Sort 节点
     * 
     * Key: 需要强制属性的 GroupExpression
     * Value: 对应的 Enforcer GroupExpression
     */
    private final Map<GroupExpression, GroupExpression> enforcers = Maps.newHashMap();
    
    /** 
     * 统计信息是否可靠
     * 
     * 作用：
     * - 标记当前 Group 的统计信息是否准确可靠
     * - 如果统计信息不可靠，优化器可能需要重新计算或使用保守估计
     * 
     * 何时设置为 false：
     * - 当 Group 被合并时，统计信息可能需要重新计算
     * - 当应用了某些规则后，统计信息可能不再准确
     */
    private boolean isStatsReliable = true;
    
    /** 
     * 逻辑属性
     * 
     * 包含：
     * - 输出列（Output slots）
     * - 统计信息（Statistics）
     * - 等价类（Equivalence classes）
     * - 函数依赖（Functional dependencies）
     * 
     * 作用：
     * - 描述 Group 中所有计划的逻辑特征
     * - 用于等价性检测：同一个 Group 中的所有计划必须有相同的逻辑属性
     * - 用于规则匹配：规则需要检查逻辑属性是否匹配
     */
    private LogicalProperties logicalProperties;

    /** 
     * 代价下界映射
     * 
     * 作用：
     * - 存储不同物理属性要求下的最低代价计划
     * - Key: 所需的物理属性（PhysicalProperties），如数据分布、排序等
     * - Value: Pair<代价, GroupExpression>，包含最低代价和对应的 GroupExpression
     * 
     * 用途：
     * - CBO（基于代价的优化）：根据物理属性要求选择代价最低的计划
     * - 剪枝优化：如果某个计划的代价已经超过下界，可以剪枝
     * - 计划选择：最终选择代价最低的计划作为执行计划
     * 
     * 示例：
     * - PhysicalProperties.GATHER -> (Cost: 100, PhysicalHashJoin)
     * - PhysicalProperties.REPLICATED -> (Cost: 200, PhysicalBroadcastJoin)
     */
    private final Map<PhysicalProperties, Pair<Cost, GroupExpression>> lowestCostPlans = Maps.newLinkedHashMap();

    /** 
     * 是否已探索
     * 
     * 作用：
     * - 标记当前 Group 是否已经被完全探索（所有规则都已应用）
     * - 用于优化器的探索阶段，避免重复探索
     * 
     * 何时设置为 true：
     * - 当所有适用的规则都已应用到当前 Group 时
     * - 表示当前 Group 已经探索完毕，不需要再次探索
     */
    private boolean isExplored = false;

    /** 
     * 统计信息
     * 
     * 包含：
     * - 行数（Row count）
     * - 列统计信息（Column statistics）
     * - 数据分布信息（Data distribution）
     * 
     * 作用：
     * - 用于代价估算：计算计划的执行代价
     * - 用于规则匹配：某些规则需要统计信息来判断是否适用
     * - 用于优化决策：根据统计信息选择最优计划
     */
    private Statistics statistics;

    /** 
     * 选择的物理属性
     * 
     * 作用：
     * - 记录最终选择的物理计划的物理属性
     * - 用于计划生成：根据选择的物理属性生成最终的执行计划
     * 
     * 包含：
     * - 数据分布（Distribution）：如 GATHER、HASH、BROADCAST 等
     * - 排序（Order）：如按某列排序
     * - 分区（Partition）：如按某列分区
     */
    private PhysicalProperties chosenProperties;

    /** 
     * 选择的 GroupExpression ID
     * 
     * 作用：
     * - 记录最终选择的 GroupExpression 的索引
     * - 用于计划生成：根据 ID 找到对应的 GroupExpression
     * 
     * 值：
     * - -1: 尚未选择
     * - >= 0: 选择的 GroupExpression 在列表中的索引
     */
    private int chosenGroupExpressionId = -1;

    /** 
     * 选择的强制器属性列表
     * 
     * 作用：
     * - 记录最终选择的执行计划中使用的强制器（Enforcer）的物理属性
     * - 用于计划生成：知道需要哪些强制器来满足物理属性要求
     * 
     * 示例：
     * - 如果需要在某个节点强制排序，这里会记录排序属性
     * - 如果需要在某个节点强制数据分布，这里会记录分布属性
     */
    private List<PhysicalProperties> chosenEnforcerPropertiesList = new ArrayList<>();
    
    /** 
     * 选择的强制器 ID 列表
     * 
     * 作用：
     * - 记录最终选择的执行计划中使用的强制器（Enforcer）的 ID
     * - 与 chosenEnforcerPropertiesList 一一对应
     * - 用于计划生成：知道在哪些位置需要添加哪些强制器
     */
    private List<Integer> chosenEnforcerIdList = new ArrayList<>();

    /** 
     * 结构信息映射
     * 
     * 作用：
     * - 存储物化视图重写相关的结构信息（StructInfo）
     * - Key: 表 ID 集合（BitSet），表示使用了哪些表
     * - Value: 对应的结构信息，包含计划的图结构、谓词、等价类等
     * 
     * 用途：
     * - 物化视图匹配：用于判断物化视图是否可以匹配查询
     * - 结构信息缓存：避免重复计算结构信息，提高性能
     * - 嵌套重写：支持物化视图的嵌套重写
     */
    private StructInfoMap structInfoMap = new StructInfoMap();

    /**
     * Constructor for Group.
     *
     * @param groupExpression first {@link GroupExpression} in this Group
     */
    public Group(GroupId groupId, GroupExpression groupExpression, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        addGroupExpression(groupExpression);
        this.logicalProperties = logicalProperties;
    }

    /**
     * Construct a Group without any group expression
     *
     * @param groupId the groupId in memo
     */
    public Group(GroupId groupId, LogicalProperties logicalProperties) {
        this.groupId = groupId;
        this.logicalProperties = logicalProperties;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public List<PhysicalProperties> getAllProperties() {
        return new ArrayList<>(lowestCostPlans.keySet());
    }

    /**
     * Add new {@link GroupExpression} into this group.
     *
     * @param groupExpression {@link GroupExpression} to be added
     * @return added {@link GroupExpression}
     */
    public GroupExpression addGroupExpression(GroupExpression groupExpression) {
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            logicalExpressions.add(groupExpression);
        } else {
            physicalExpressions.add(groupExpression);
        }
        groupExpression.setOwnerGroup(this);
        return groupExpression;
    }

    public void setStatsReliable(boolean statsReliable) {
        this.isStatsReliable = statsReliable;
    }

    public boolean isStatsReliable() {
        return isStatsReliable;
    }

    public void addLogicalExpression(GroupExpression groupExpression) {
        groupExpression.setOwnerGroup(this);
        logicalExpressions.add(groupExpression);
    }

    public void addPhysicalExpression(GroupExpression groupExpression) {
        groupExpression.setOwnerGroup(this);
        physicalExpressions.add(groupExpression);
    }

    public List<GroupExpression> getLogicalExpressions() {
        return logicalExpressions;
    }

    public GroupExpression logicalExpressionsAt(int index) {
        return logicalExpressions.get(index);
    }

    /**
     * Get the first logical group expression in this group.
     * If there is no logical group expression or more than one, throw an exception.
     *
     * @return the first logical group expression in this group
     */
    public GroupExpression getLogicalExpression() {
        Preconditions.checkArgument(logicalExpressions.size() == 1,
                "There should be only one Logical Expression in Group");
        return logicalExpressions.get(0);
    }

    public GroupExpression getFirstLogicalExpression() {
        Preconditions.checkArgument(!logicalExpressions.isEmpty(),
                "There should be more than one Logical Expression in Group");
        return logicalExpressions.get(0);
    }

    public List<GroupExpression> getPhysicalExpressions() {
        return physicalExpressions;
    }

    /**
     * Remove groupExpression from this group.
     *
     * @param groupExpression to be removed
     * @return removed {@link GroupExpression}
     */
    public GroupExpression removeGroupExpression(GroupExpression groupExpression) {
        // use identityRemove to avoid equals() method
        if (groupExpression.getPlan() instanceof LogicalPlan) {
            Utils.identityRemove(logicalExpressions, groupExpression);
        } else {
            Utils.identityRemove(physicalExpressions, groupExpression);
        }
        groupExpression.setOwnerGroup(null);
        return groupExpression;
    }

    public List<GroupExpression> clearLogicalExpressions() {
        List<GroupExpression> move = logicalExpressions.stream()
                .peek(groupExpr -> groupExpr.setOwnerGroup(null))
                .collect(Collectors.toList());
        logicalExpressions.clear();
        return move;
    }

    public List<GroupExpression> clearPhysicalExpressions() {
        List<GroupExpression> move = physicalExpressions.stream()
                .peek(groupExpr -> groupExpr.setOwnerGroup(null))
                .collect(Collectors.toList());
        physicalExpressions.clear();
        return move;
    }

    public void clearLowestCostPlans() {
        lowestCostPlans.clear();
    }

    public double getCostLowerBound() {
        return -1D;
    }

    /**
     * Get the lowest cost {@link org.apache.doris.nereids.trees.plans.physical.PhysicalPlan}
     * which meeting the physical property constraints in this Group.
     *
     * @param physicalProperties the physical property constraints
     * @return {@link Optional} of cost and {@link GroupExpression} of physical plan pair.
     */
    public Optional<Pair<Cost, GroupExpression>> getLowestCostPlan(PhysicalProperties physicalProperties) {
        if (physicalProperties == null || lowestCostPlans.isEmpty()) {
            return Optional.empty();
        }
        Optional<Pair<Cost, GroupExpression>> costAndGroupExpression =
                Optional.ofNullable(lowestCostPlans.get(physicalProperties));
        return costAndGroupExpression;
    }

    public Map<PhysicalProperties, Cost> getLowestCosts() {
        return lowestCostPlans.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(Entry::getKey, kv -> kv.getValue().first));
    }

    public GroupExpression getBestPlan(PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            return lowestCostPlans.get(properties).second;
        }
        return null;
    }

    public void addEnforcer(GroupExpression enforcer) {
        enforcer.setOwnerGroup(this);
        enforcers.put(enforcer, enforcer);
    }

    public Map<GroupExpression, GroupExpression> getEnforcers() {
        return enforcers;
    }

    /**
     * Set or update lowestCostPlans: properties --> Pair.of(cost, expression)
     */
    public void setBestPlan(GroupExpression expression, Cost cost, PhysicalProperties properties) {
        if (lowestCostPlans.containsKey(properties)) {
            if (lowestCostPlans.get(properties).first.getValue() > cost.getValue()) {
                lowestCostPlans.put(properties, Pair.of(cost, expression));
            }
        } else {
            lowestCostPlans.put(properties, Pair.of(cost, expression));
        }
    }

    /**
     * replace best plan with new properties
     */
    public void replaceBestPlanProperty(PhysicalProperties oldProperty,
            PhysicalProperties newProperty, Cost cost) {
        Pair<Cost, GroupExpression> pair = lowestCostPlans.get(oldProperty);
        GroupExpression lowestGroupExpr = pair.second;
        lowestGroupExpr.updateLowestCostTable(newProperty,
                lowestGroupExpr.getInputPropertiesList(oldProperty), cost);
        lowestCostPlans.remove(oldProperty);
        lowestCostPlans.put(newProperty, pair);
    }

    /**
     * replace oldGroupExpression with newGroupExpression in lowestCostPlans.
     */
    public void replaceBestPlanGroupExpr(GroupExpression oldGroupExpression, GroupExpression newGroupExpression) {
        Map<PhysicalProperties, Pair<Cost, GroupExpression>> needReplaceBestExpressions = Maps.newHashMap();
        for (Iterator<Entry<PhysicalProperties, Pair<Cost, GroupExpression>>> iterator =
                lowestCostPlans.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry = iterator.next();
            Pair<Cost, GroupExpression> pair = entry.getValue();
            if (pair.second.equals(oldGroupExpression)) {
                needReplaceBestExpressions.put(entry.getKey(), Pair.of(pair.first, newGroupExpression));
                iterator.remove();
            }
        }
        lowestCostPlans.putAll(needReplaceBestExpressions);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public LogicalProperties getLogicalProperties() {
        return logicalProperties;
    }

    public void setLogicalProperties(LogicalProperties logicalProperties) {
        this.logicalProperties = logicalProperties;
    }

    public boolean isExplored() {
        return isExplored;
    }

    public void setExplored(boolean explored) {
        isExplored = explored;
    }

    public List<GroupExpression> getParentGroupExpressions() {
        return ImmutableList.copyOf(parentExpressions.keySet());
    }

    public void addParentExpression(GroupExpression parent) {
        parentExpressions.put(parent, null);
    }

    /**
     * remove the reference to parent groupExpression
     *
     * @param parent group expression
     * @return parentExpressions's num
     */
    public int removeParentExpression(GroupExpression parent) {
        parentExpressions.remove(parent);
        return parentExpressions.size();
    }

    public void removeParentPhysicalExpressions() {
        parentExpressions.entrySet().removeIf(entry -> entry.getKey().getPlan() instanceof PhysicalPlan);
    }

    /**
     * move the ownerGroup to target group.
     *
     * @param target the new owner group of expressions
     */
    public void mergeTo(Group target) {
        // move parentExpressions Ownership
        parentExpressions.keySet().forEach(parent -> target.addParentExpression(parent));

        // move enforcers Ownership
        enforcers.forEach((k, v) -> k.children().set(0, target));
        // TODO: dedup?
        enforcers.forEach((k, v) -> target.addEnforcer(k));
        enforcers.clear();

        // move LogicalExpression PhysicalExpression Ownership
        Map<GroupExpression, GroupExpression> logicalSet = target.getLogicalExpressions().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        for (GroupExpression logicalExpression : logicalExpressions) {
            GroupExpression existGroupExpr = logicalSet.get(logicalExpression);
            if (existGroupExpr != null) {
                Preconditions.checkState(logicalExpression != existGroupExpr, "must not equals");
                // lowCostPlans must be physical GroupExpression, don't need to replaceBestPlanGroupExpr
                logicalExpression.mergeToNotOwnerRemove(existGroupExpr);
            } else {
                target.addLogicalExpression(logicalExpression);
            }
        }
        logicalExpressions.clear();
        // movePhysicalExpressionOwnership
        Map<GroupExpression, GroupExpression> physicalSet = target.getPhysicalExpressions().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        for (GroupExpression physicalExpression : physicalExpressions) {
            GroupExpression existGroupExpr = physicalSet.get(physicalExpression);
            if (existGroupExpr != null) {
                Preconditions.checkState(physicalExpression != existGroupExpr, "must not equals");
                physicalExpression.getOwnerGroup().replaceBestPlanGroupExpr(physicalExpression, existGroupExpr);
                physicalExpression.mergeToNotOwnerRemove(existGroupExpr);
            } else {
                target.addPhysicalExpression(physicalExpression);
            }
        }
        physicalExpressions.clear();

        // Above we already replaceBestPlanGroupExpr, but we still need to moveLowestCostPlansOwnership.
        lowestCostPlans.forEach((physicalProperties, costAndGroupExpr) -> {
            // move lowestCostPlans Ownership
            if (!target.lowestCostPlans.containsKey(physicalProperties)) {
                target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
            } else {
                if (costAndGroupExpr.first.getValue()
                        < target.lowestCostPlans.get(physicalProperties).first.getValue()) {
                    target.lowestCostPlans.put(physicalProperties, costAndGroupExpr);
                }
            }
        });
        lowestCostPlans.clear();

        // If statistics is null, use other statistics
        if (target.statistics == null) {
            target.statistics = this.statistics;
        }
    }

    /**
     * This function used to check whether the group is an end node in DPHyp
     */
    public boolean isValidJoinGroup() {
        Plan plan = getLogicalExpression().getPlan();
        if (plan instanceof LogicalJoin
                && ((LogicalJoin) plan).getJoinType() == JoinType.INNER_JOIN
                && !((LogicalJoin) plan).isMarkJoin()) {
            Preconditions.checkArgument(!((LogicalJoin) plan).getExpressions().isEmpty(),
                    "inner join must have join conjuncts");
            if (((LogicalJoin) plan).getHashJoinConjuncts().isEmpty()
                    && ((LogicalJoin) plan).getOtherJoinConjuncts().get(0) instanceof Literal) {
                return false;
            } else {
                // Right now, we only support inner join with some conjuncts referencing any side of the child's output
                return true;
            }
        }
        return false;
    }

    public StructInfoMap getStructInfoMap() {
        return structInfoMap;
    }

    public boolean isProjectGroup() {
        return getFirstLogicalExpression().getPlan() instanceof LogicalProject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Group group = (Group) o;
        return groupId.equals(group.groupId);
    }

    @Override
    public int hashCode() {
        return 31 * groupId.asInt();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("Group[" + groupId + "]\n");
        str.append("  logical expressions:\n");
        for (GroupExpression logicalExpression : logicalExpressions) {
            str.append("    ").append(logicalExpression).append("\n");
        }
        str.append("  physical expressions:\n");
        for (GroupExpression physicalExpression : physicalExpressions) {
            str.append("    ").append(physicalExpression).append("\n");
        }
        str.append("  enforcers:\n");
        List<GroupExpression> enforcerList = enforcers.keySet().stream()
                .sorted(java.util.Comparator.comparing(e1 -> e1.getId().asInt()))
                .collect(Collectors.toList());

        for (GroupExpression enforcer : enforcerList) {
            str.append("    ").append(enforcer).append("\n");
        }
        if (!chosenEnforcerIdList.isEmpty()) {
            str.append("  chosen enforcer(id, requiredProperties):\n");
            for (int i = 0; i < chosenEnforcerIdList.size(); i++) {
                str.append("      (").append(i).append(")").append(chosenEnforcerIdList.get(i)).append(",  ")
                        .append(chosenEnforcerPropertiesList.get(i)).append("\n");
            }
        }
        if (chosenGroupExpressionId != -1) {
            str.append("  chosen expression id: ").append(chosenGroupExpressionId).append("\n");
            str.append("  chosen properties: ").append(chosenProperties).append("\n");
        }
        str.append("  stats").append("\n");
        str.append(getStatistics() == null ? "" : getStatistics().detail("    "));

        str.append("  lowest Plan(cost, properties, plan, childrenRequires)");
        DecimalFormat format = new DecimalFormat("#,###.##");
        for (Map.Entry<PhysicalProperties, Pair<Cost, GroupExpression>> entry : lowestCostPlans.entrySet()) {
            PhysicalProperties prop = entry.getKey();
            Pair<Cost, GroupExpression> costGroupExpressionPair = entry.getValue();
            Cost cost = costGroupExpressionPair.first;
            GroupExpression child = costGroupExpressionPair.second;
            str.append("\n\n    ").append(format.format(cost.getValue())).append(" ").append(prop)
                .append("\n     ").append(child).append("\n     ")
                .append(child.getInputPropertiesListOrEmpty(prop));
        }
        str.append("\n").append("  struct info map").append("\n");
        str.append(structInfoMap);

        return str.toString();
    }

    /**
     * Get tree like string describing group.
     *
     * @return tree like string describing group
     */
    public String treeString() {
        Function<Object, String> toString = obj -> {
            if (obj instanceof Group) {
                Group group = (Group) obj;
                Map<PhysicalProperties, Cost> lowestCosts = group.getLowestCosts();
                return "Group[" + group.groupId + ", lowestCosts: " + lowestCosts + "]";
            } else if (obj instanceof GroupExpression) {
                GroupExpression groupExpression = (GroupExpression) obj;
                Map<PhysicalProperties, Pair<Cost, List<PhysicalProperties>>> lowestCostTable
                        = groupExpression.getLowestCostTable();
                Map<PhysicalProperties, PhysicalProperties> requestPropertiesMap
                        = groupExpression.getRequestPropertiesMap();
                Cost cost = groupExpression.getCost();
                return groupExpression.getPlan().toString() + " [cost: " + cost + ", lowestCostTable: "
                        + lowestCostTable + ", requestPropertiesMap: " + requestPropertiesMap + "]";
            } else if (obj instanceof Pair) {
                // print logicalExpressions or physicalExpressions
                // first is name, second is group expressions
                return ((Pair<?, ?>) obj).first.toString();
            } else {
                return obj.toString();
            }
        };

        Function<Object, List<Object>> getChildren = obj -> {
            if (obj instanceof Group) {
                Group group = (Group) obj;
                List children = new ArrayList<>();

                // to <name, children> pair
                if (!group.getLogicalExpressions().isEmpty()) {
                    children.add(Pair.of("logicalExpressions", group.getLogicalExpressions()));
                }
                if (!group.getPhysicalExpressions().isEmpty()) {
                    children.add(Pair.of("physicalExpressions", group.getPhysicalExpressions()));
                }
                return children;
            } else if (obj instanceof GroupExpression) {
                return (List) ((GroupExpression) obj).children();
            } else if (obj instanceof Pair) {
                return (List) ((Pair<String, List<GroupExpression>>) obj).second;
            } else {
                return ImmutableList.of();
            }
        };

        Function<Object, List<Object>> getExtraPlans = obj -> {
            if (obj instanceof Plan) {
                return (List) ((Plan) obj).extraPlans();
            } else {
                return ImmutableList.of();
            }
        };

        Function<Object, Boolean> displayExtraPlan = obj -> {
            if (obj instanceof Plan) {
                return ((Plan) obj).displayExtraPlanFirst();
            } else {
                return false;
            }
        };

        return TreeStringUtils.treeString(this, toString, getChildren, getExtraPlans, displayExtraPlan);
    }

    public PhysicalProperties getChosenProperties() {
        return chosenProperties;
    }

    public void setChosenProperties(PhysicalProperties chosenProperties) {
        this.chosenProperties = chosenProperties;
    }

    public void setChosenGroupExpressionId(int chosenGroupExpressionId) {
        Preconditions.checkArgument(this.chosenGroupExpressionId == -1,
                "chosenGroupExpressionId is already set");
        this.chosenGroupExpressionId = chosenGroupExpressionId;
    }

    public void addChosenEnforcerProperties(PhysicalProperties chosenEnforcerProperties) {
        this.chosenEnforcerPropertiesList.add(chosenEnforcerProperties);
    }

    public void addChosenEnforcerId(int chosenEnforcerId) {
        this.chosenEnforcerIdList.add(chosenEnforcerId);
    }
}
