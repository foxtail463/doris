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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.event.GroupMergeEvent;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;
import org.apache.doris.nereids.rules.exploration.mv.AbstractMaterializedViewRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Cascades 优化器中的 Memo 数据结构
 * 
 * Memo 是 Cascades 优化器的核心数据结构，用于存储和管理优化过程中的所有候选计划。
 * 它通过 Group 和 GroupExpression 来组织计划，实现计划的共享和等价性检测。
 * 
 * 核心概念：
 * 1. **Group**: 表示逻辑等价的计划集合，所有在同一个 Group 中的计划在逻辑上等价
 * 2. **GroupExpression**: 表示一个具体的计划表达式，包含计划节点和其子 Group 的引用
 * 3. **等价性检测**: 通过比较 GroupExpression 的哈希值来判断计划是否等价
 * 4. **Group 合并**: 当发现两个 Group 等价时，会合并它们，减少搜索空间
 * 
 * 主要功能：
 * - 计划的添加和查找（copyIn/copyOut）
 * - Group 的合并（mergeGroup）
 * - 计划的排名和选择（rank/unrank）
 * - 物化视图检查结果的缓存
 * 
 * 工作流程：
 * 1. 初始化：将初始逻辑计划转换为 Memo 结构
 * 2. 规则应用：应用重写规则，生成新的计划并添加到 Memo
 * 3. 等价性检测：检测新计划是否与已有计划等价，决定是否合并 Group
 * 4. 代价计算：为每个物理计划计算代价
 * 5. 计划选择：根据代价选择最优计划
 */
public class Memo {
    public static final Logger LOG = LogManager.getLogger(Memo.class);
    
    /** 
     * Group 合并事件追踪器
     * 用于记录和追踪 Group 合并事件，便于调试和性能分析
     * 在测试中生成 group id 更好，因为可以完全重现相同的 Memo
     */
    private static final EventProducer GROUP_MERGE_TRACER = new EventProducer(GroupMergeEvent.class,
            EventChannel.getDefaultChannel().addConsumers(new LogConsumer(GroupMergeEvent.class, EventChannel.LOG)));
    
    /** 
     * 状态 ID，用于追踪 Memo 的状态变化
     * 每次生成新的 GroupExpression 时会递增，用于调试和测试
     */
    private static long stateId = 0;
    
    /** 
     * 连接上下文，包含会话变量、查询信息等
     * 用于访问会话配置和查询相关的上下文信息
     */
    private final ConnectContext connectContext;
    
    /** 
     * 刷新版本号，用于追踪 Memo 的刷新次数
     * 当 Memo 结构发生变化时（如添加新计划、合并 Group），版本号会递增
     * 用于缓存失效和结构信息的版本控制
     */
    private final AtomicLong refreshVersion = new AtomicLong(1);
    
    /** 
     * 物化视图检查成功的结果缓存
     * Key: 物化视图规则类型
     * Value: 成功检查的物化视图 ID 集合
     * 用于避免重复检查相同的物化视图，提高性能
     */
    private final Map<Class<? extends AbstractMaterializedViewRule>, Set<Long>> materializationCheckSuccessMap =
            new LinkedHashMap<>();
    
    /** 
     * 物化视图检查失败的结果缓存
     * Key: 物化视图规则类型
     * Value: 检查失败的物化视图 ID 集合
     * 用于避免重复检查已知失败的物化视图，提高性能
     */
    private final Map<Class<? extends AbstractMaterializedViewRule>, Set<Long>> materializationCheckFailMap =
            new LinkedHashMap<>();
    
    /** 
     * Group ID 生成器
     * 为每个新创建的 Group 生成唯一的 ID
     */
    private final IdGenerator<GroupId> groupIdGenerator = GroupId.createGenerator();
    
    /** 
     * Group 集合，存储 Memo 中所有的 Group
     * Key: GroupId
     * Value: Group 对象
     * 使用 LinkedHashMap 保持插入顺序，便于调试和展示
     */
    private final Map<GroupId, Group> groups = Maps.newLinkedHashMap();
    
    /** 
     * GroupExpression 集合，存储 Memo 中所有的 GroupExpression
     * Key: GroupExpression（用于快速查找）
     * Value: GroupExpression（实际对象引用）
     * 
     * 注意：不能使用 Set，因为 Set 没有 get 方法，无法快速查找
     * 这个 Map 用于快速检测 GroupExpression 是否已存在（等价性检测）
     */
    private final Map<GroupExpression, GroupExpression> groupExpressions = Maps.newHashMap();
    
    /** 
     * 根 Group，表示查询计划的根节点
     * 所有优化操作都从根 Group 开始
     */
    private Group root;

    // FOR TEST ONLY
    public Memo() {
        this.root = null;
        this.connectContext = null;
    }

    public Memo(ConnectContext connectContext, Plan plan) {
        this.root = init(plan);
        this.connectContext = connectContext;
    }

    public static long getStateId() {
        return stateId;
    }

    public Group getRoot() {
        return root;
    }

    /**
     * This function used to update the root group when DPHyp change the root Group
     * Note it only used in DPHyp
     */
    public void setRoot(Group root) {
        this.root = root;
    }

    public List<Group> getGroups() {
        return ImmutableList.copyOf(groups.values());
    }

    public Group getGroup(GroupId groupId) {
        return groups.get(groupId);
    }

    public Map<GroupExpression, GroupExpression> getGroupExpressions() {
        return groupExpressions;
    }

    public int getGroupExpressionsSize() {
        return groupExpressions.size();
    }

    /**
     * 获取当前的刷新版本号
     * 
     * 刷新版本号用于追踪 Memo 结构的变化。当 Memo 结构发生变化时
     * （如添加新计划、合并 Group），版本号会递增。
     * 
     * 用途：
     * - 缓存失效：当 Memo 结构变化时，相关的缓存需要失效
     * - 结构信息版本控制：StructInfo 等结构信息需要知道 Memo 的版本
     * - 调试和追踪：可以追踪 Memo 的变化历史
     * 
     * @return 当前的刷新版本号
     */
    public long getRefreshVersion() {
        return refreshVersion.get();
    }

    /**
     * 递增并获取刷新版本号
     * 
     * 当 Memo 结构发生变化时（如添加新计划、合并 Group），调用此方法
     * 来更新版本号。这通常发生在：
     * - 添加新的 GroupExpression 时
     * - 合并 Group 时
     * - 物化视图嵌套重写时
     * 
     * @return 递增后的刷新版本号
     */
    public long incrementAndGetRefreshVersion() {
        return refreshVersion.incrementAndGet();
    }

    /**
     * 记录物化视图检查结果，用于性能优化
     * 
     * 在物化视图重写过程中，需要检查物化视图是否可以被查询使用。
     * 这个检查过程可能比较耗时，因此将检查结果缓存起来，避免重复检查。
     * 
     * 缓存策略：
     * - 成功的结果存储在 materializationCheckSuccessMap 中
     * - 失败的结果存储在 materializationCheckFailMap 中
     * - 通过规则类型和物化视图 ID 来唯一标识检查结果
     * 
     * @param target 物化视图规则类型（如 AbstractMaterializedViewJoinRule）
     * @param checkedMaterializationId 被检查的物化视图 ID
     * @param isSuccess 检查是否成功
     *                  - true: 物化视图可以被使用
     *                  - false: 物化视图不能被使用
     */
    public void recordMaterializationCheckResult(Class<? extends AbstractMaterializedViewRule> target,
            Long checkedMaterializationId, boolean isSuccess) {
        if (isSuccess) {
            // 记录成功的结果
            Set<Long> checkedSet = materializationCheckSuccessMap.get(target);
            if (checkedSet == null) {
                checkedSet = new HashSet<>();
                materializationCheckSuccessMap.put(target, checkedSet);
            }
            checkedSet.add(checkedMaterializationId);
        } else {
            // 记录失败的结果
            Set<Long> checkResultSet = materializationCheckFailMap.get(target);
            if (checkResultSet == null) {
                checkResultSet = new HashSet<>();
                materializationCheckFailMap.put(target, checkResultSet);
            }
            checkResultSet.add(checkedMaterializationId);
        }
    }

    /**
     * 检查物化视图是否已经被检查过
     * 
     * 用于快速判断物化视图是否已经被检查过，避免重复检查。
     * 
     * @param target 物化视图规则类型
     * @param materializationId 物化视图 ID
     * @return 检查结果：
     *         - true: 检查成功，物化视图可以被使用
     *         - false: 检查失败，物化视图不能被使用
     *         - null: 尚未检查过
     */
    public Boolean materializationHasChecked(Class<? extends AbstractMaterializedViewRule> target,
            long materializationId) {
        // 检查成功的结果缓存
        Set<Long> checkSuccessSet = materializationCheckSuccessMap.get(target);
        if (checkSuccessSet != null && checkSuccessSet.contains(materializationId)) {
            return true;
        }
        
        // 检查失败的结果缓存
        Set<Long> checkFailSet = materializationCheckFailMap.get(target);
        if (checkFailSet != null && checkFailSet.contains(materializationId)) {
            return false;
        }
        
        // 尚未检查过
        return null;
    }

    public int countMaxContinuousJoin() {
        return countGroupJoin(root).second;
    }

    public static int countMaxContinuousJoin(Plan plan) {
        return countLogicalJoin(plan).second;
    }

    /**
     * return the max continuous join operator
     */
    public Pair<Integer, Integer> countGroupJoin(Group group) {
        GroupExpression logicalExpr = group.getFirstLogicalExpression();
        List<Pair<Integer, Integer>> children = new ArrayList<>();
        for (Group child : logicalExpr.children()) {
            children.add(countGroupJoin(child));
        }

        if (group.isProjectGroup()) {
            return children.get(0);
        }

        int maxJoinCount = 0;
        int continuousJoinCount = 0;
        for (Pair<Integer, Integer> child : children) {
            maxJoinCount = Math.max(maxJoinCount, child.second);
        }
        if (group.getFirstLogicalExpression().getPlan() instanceof LogicalJoin) {
            for (Pair<Integer, Integer> child : children) {
                continuousJoinCount += child.first;
            }
            continuousJoinCount += 1;
        } else if (group.isProjectGroup()) {
            return children.get(0);
        }
        return Pair.of(continuousJoinCount, Math.max(continuousJoinCount, maxJoinCount));
    }

    private static Pair<Integer, Integer> countLogicalJoin(Plan plan) {
        List<Pair<Integer, Integer>> children = new ArrayList<>();
        for (Plan child : plan.children()) {
            children.add(countLogicalJoin(child));
        }
        if (plan instanceof LogicalProject) {
            return children.get(0);
        }
        int maxJoinCount = 0;
        int continuousJoinCount = 0;
        for (Pair<Integer, Integer> child : children) {
            maxJoinCount = Math.max(maxJoinCount, child.second);
        }
        if (plan instanceof LogicalJoin) {
            for (Pair<Integer, Integer> child : children) {
                continuousJoinCount += child.first;
            }
            continuousJoinCount += 1;
        }
        return Pair.of(continuousJoinCount, Math.max(continuousJoinCount, maxJoinCount));
    }

    /**
     * 将计划添加到 Memo 中
     * 
     * 这是将计划添加到 Memo 的主要入口方法。根据 rewrite 参数决定是重写还是复制。
     * 
     * @param plan 要添加的计划
     * @param target 目标 Group，如果为 null 则创建新的 Group
     * @param rewrite 是否为重写模式
     *                - true: 重写模式，会替换目标 Group 中的现有计划
     *                - false: 复制模式，将计划添加到目标 Group 或创建新 Group
     * @param planTable 计划表映射，用于快速查找计划对应的 Group（可选）
     * @return CopyInResult，包含是否生成了新的 GroupExpression 和对应的 GroupExpression
     */
    public CopyInResult copyIn(Plan plan, @Nullable Group target, boolean rewrite, HashMap<Long, Group> planTable) {
        CopyInResult result;
        if (rewrite) {
            // 重写模式：替换目标 Group 中的计划
            result = doRewrite(plan, target);
        } else {
            // 复制模式：添加计划到目标 Group 或创建新 Group
            result = doCopyIn(plan, target, planTable);
        }
        // 如果生成了新的 GroupExpression，更新状态 ID（用于调试和测试）
        maybeAddStateId(result);
        return result;
    }

    /**
     * 将计划添加到 Memo 中（简化版本，不包含 planTable）
     * 
     * @param plan 要添加的计划（Plan 或 Expression）
     * @param target 目标 Group，如果为 null 则生成新的 Group
     * @param rewrite 是否为重写模式
     *                - true: 重写模式，会替换目标 Group 中的现有计划
     *                - false: 复制模式，将计划添加到目标 Group 或创建新 Group
     * @return CopyInResult，其中：
     *         - generateNewExpression: true 表示新生成的 GroupExpression 被添加到 Memo
     *         - correspondingExpression: 计划对应的 GroupExpression
     */
    public CopyInResult copyIn(Plan plan, @Nullable Group target, boolean rewrite) {
        CopyInResult result;
        if (rewrite) {
            result = doRewrite(plan, target);
        } else {
            result = doCopyIn(plan, target, null);
        }
        maybeAddStateId(result);
        return result;
    }

    private void maybeAddStateId(CopyInResult result) {
        if (connectContext != null && connectContext.getSessionVariable().isEnableNereidsTrace()
                && result.generateNewExpression) {
            stateId++;
        }
    }

    public List<Plan> copyOutAll() {
        return copyOutAll(root);
    }

    private List<Plan> copyOutAll(Group group) {
        List<GroupExpression> logicalExpressions = group.getLogicalExpressions();
        return logicalExpressions.stream()
                .flatMap(groupExpr -> copyOutAll(groupExpr).stream())
                .collect(Collectors.toList());
    }

    private List<Plan> copyOutAll(GroupExpression logicalExpression) {
        if (logicalExpression.arity() == 0) {
            return Lists.newArrayList(logicalExpression.getPlan().withChildren(ImmutableList.of()));
        } else if (logicalExpression.arity() == 1) {
            List<Plan> multiChild = copyOutAll(logicalExpression.child(0));
            return multiChild.stream()
                    .map(children -> logicalExpression.getPlan().withChildren(children))
                    .collect(Collectors.toList());
        } else if (logicalExpression.arity() == 2) {
            int leftCount = logicalExpression.child(0).getLogicalExpressions().size();
            int rightCount = logicalExpression.child(1).getLogicalExpressions().size();
            int count = leftCount * rightCount;

            List<Plan> leftChildren = copyOutAll(logicalExpression.child(0));
            List<Plan> rightChildren = copyOutAll(logicalExpression.child(1));

            List<Plan> result = new ArrayList<>(count);
            for (Plan leftChild : leftChildren) {
                for (Plan rightChild : rightChildren) {
                    result.add(logicalExpression.getPlan().withChildren(leftChild, rightChild));
                }
            }
            return result;
        } else {
            throw new RuntimeException("arity > 2");
        }
    }

    /**
     * 从 Memo 中提取根 Group 的计划
     * 
     * 这是从 Memo 中提取计划的便捷方法，默认从根 Group 开始提取。
     * 
     * @return 提取的计划（不包含 GroupExpression 信息）
     */
    public Plan copyOut() {
        return copyOut(root, false);
    }

    /**
     * 从 Memo 中提取指定 Group 的计划
     * 
     * 递归地将 Group 结构转换为计划树。从 Group 的第一个逻辑表达式开始，
     * 递归处理所有子 Group，最终得到完整的计划树。
     * 
     * @param group 要提取的 Group
     * @param includeGroupExpression 是否在计划中包含 GroupExpression 信息
     *                                - true: 计划中会包含 GroupExpression，用于调试和追踪
     *                                - false: 计划中不包含 GroupExpression，是纯计划树
     * @return 提取的计划树
     */
    public Plan copyOut(Group group, boolean includeGroupExpression) {
        // 获取 Group 的第一个逻辑表达式（通常是最优的或默认的）
        GroupExpression logicalExpression = group.getFirstLogicalExpression();
        return copyOut(logicalExpression, includeGroupExpression);
    }

    /**
     * 从 Memo 中提取指定 GroupExpression 的计划
     * 
     * 递归地将 GroupExpression 及其子 Group 转换为计划树。
     * 对于每个子 Group，递归调用 copyOut 方法，最终构建完整的计划树。
     * 
     * @param logicalExpression 要提取的 GroupExpression
     * @param includeGroupExpression 是否在计划中包含 GroupExpression 信息
     *                                - true: 计划中会包含 GroupExpression，用于调试和追踪
     *                                - false: 计划中不包含 GroupExpression，是纯计划树
     * @return 提取的计划树
     */
    public Plan copyOut(GroupExpression logicalExpression, boolean includeGroupExpression) {
        // 步骤1：递归提取所有子 Group 的计划
        List<Plan> children = Lists.newArrayList();
        for (Group child : logicalExpression.children()) {
            children.add(copyOut(child, includeGroupExpression));
        }
        
        // 步骤2：使用提取的子计划构建当前计划节点
        Plan planWithChildren = logicalExpression.getPlan().withChildren(children);

        // 步骤3：根据参数决定是否包含 GroupExpression 信息
        Optional<GroupExpression> groupExpression = includeGroupExpression
                ? Optional.of(logicalExpression)
                : Optional.empty();

        // 步骤4：将 GroupExpression 信息附加到计划中（如果需要）
        return planWithChildren.withGroupExpression(groupExpression);
    }

    /**
     * 使用初始计划初始化 Memo
     * 
     * 这是 Memo 的初始化方法，将逻辑计划树转换为 Memo 结构。
     * 它会递归处理所有子节点，为每个计划节点创建对应的 Group 和 GroupExpression。
     * 
     * 流程：
     * 1. 递归初始化所有子节点，获取子节点的 Group
     * 2. 将计划节点的子节点替换为 GroupPlan（引用子 Group）
     * 3. 创建 GroupExpression，包含计划节点和子 Group 的引用
     * 4. 创建新的 Group，包含 GroupExpression 和逻辑属性
     * 5. 将 Group 和 GroupExpression 添加到 Memo 中
     * 
     * @param plan 初始逻辑计划（不能是 GroupPlan）
     * @return 计划对应的 Group
     */
    private Group init(Plan plan) {
        Preconditions.checkArgument(!(plan instanceof GroupPlan), "Cannot init memo by a GroupPlan");

        // 步骤1：递归初始化所有子节点
        // 为每个子计划创建对应的 Group，并收集这些 Group
        List<Group> childrenGroups = new ArrayList<>(plan.arity());
        for (Plan child : plan.children()) {
            childrenGroups.add(init(child));
        }

        // 步骤2：将计划节点的子节点替换为 GroupPlan
        // GroupPlan 是一个占位符，用于引用子 Group，而不是实际的计划节点
        plan = replaceChildrenToGroupPlan(plan, childrenGroups);
        
        // 步骤3：创建 GroupExpression
        // GroupExpression 包含计划节点和子 Group 的引用
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);
        
        // 步骤4：创建新的 Group
        // Group 包含 GroupExpression 和逻辑属性（输出列、统计信息等）
        Group group = new Group(groupIdGenerator.getNextId(), newGroupExpression, plan.getLogicalProperties());

        // 步骤5：将 Group 和 GroupExpression 添加到 Memo 中
        groups.put(group.getGroupId(), group);
        
        // 检查 GroupExpression 是否已存在（理论上不应该存在，因为这是初始化）
        if (groupExpressions.containsKey(newGroupExpression)) {
            throw new IllegalStateException("groupExpression already exists in memo, maybe a bug");
        }
        groupExpressions.put(newGroupExpression, newGroupExpression);
        return group;
    }

    /**
     * add or replace the plan into the target group.
     * <p>
     * the result truth table:
     * <pre>
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case                                  | is generated new group expression | corresponding group expression |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 1:                               |                                   |                                |
     * | if plan is GroupPlan                  |              false                |    existed group expression    |
     * | or plan has groupExpression           |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 2:                               |                                   |                                |
     * | if targetGroup is null                |              true                 |      new group expression      |
     * | and same group expression not exist   |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 3:                               |                                   |                                |
     * | if targetGroup is not null            |              true                 |      new group expression      |
     * | and same group expression not exist   |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 4:                               |                                   |                                |
     * | if targetGroup is not null and not    |              true                 |      new group expression      |
     * | equal to the existed group            |                                   |                                |
     * | expression's owner group              |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * | case 5:                               |                                   |                                |
     * | if targetGroup is null or equal to    |              false                |    existed group expression    |
     * | the existed group expression's owner  |                                   |                                |
     * | group                                 |                                   |                                |
     * +---------------------------------------+-----------------------------------+--------------------------------+
     * </pre>
     *
     * @param plan the plan which want to rewrite or added
     * @param targetGroup target group to replace plan. null to generate new Group. It should be the ancestors
     *                    of the plan's group, or equals to the plan's group, we do not check this constraint
     *                    completely because of performance.
     * @return a pair, in which the first element is true if a newly generated groupExpression added into memo,
     *         and the second element is a reference of node in Memo
     */
    /**
     * 重写计划到目标 Group 中（重写模式）
     * 
     * 这是将计划添加到 Memo 的核心方法之一，用于重写模式。
     * 重写模式与复制模式的区别：
     * - 重写模式：会替换目标 Group 中的现有计划
     * - 复制模式：将计划添加到目标 Group 或创建新 Group
     * 
     * 处理的情况（参考方法注释中的真值表）：
     * - Case 1: 计划已经是 GroupPlan 或已有 GroupExpression（快速检查）
     * - Case 2: 目标 Group 为 null，且相同的 GroupExpression 不存在
     * - Case 3: 目标 Group 不为 null，且相同的 GroupExpression 不存在
     * - Case 4: 目标 Group 不为 null，且与已存在的 GroupExpression 的 OwnerGroup 不同
     * - Case 5: 目标 Group 为 null 或等于已存在的 GroupExpression 的 OwnerGroup
     * 
     * 等价性检测：
     * - 通过 groupExpressions Map 快速检测 GroupExpression 是否已存在
     * - 如果已存在，说明找到了等价的计划，可以合并 Group 以减少搜索空间
     * 
     * @param plan 要重写的计划（必须是逻辑计划）
     * @param targetGroup 目标 Group，如果为 null 则创建新的 Group
     *                    目标 Group 应该是计划的祖先 Group，或等于计划的 Group
     *                    为了性能考虑，不完整检查这个约束
     * @return CopyInResult，其中：
     *         - generateNewExpression: true 表示新生成的 GroupExpression 被添加到 Memo
     *         - correspondingExpression: 对应的 GroupExpression（可能是新创建的或已存在的）
     */
    private CopyInResult doRewrite(Plan plan, @Nullable Group targetGroup) {
        Preconditions.checkArgument(plan != null, "plan can not be null");
        Preconditions.checkArgument(plan instanceof LogicalPlan, "only logical plan can be rewrite");

        // 情况1：快速检查 - 计划是否已经在 Memo 中
        // 如果计划是 GroupPlan 或已有 GroupExpression，说明它已经在 Memo 中
        // 直接使用已存在的计划，不需要重新创建
        if (plan instanceof GroupPlan || plan.getGroupExpression().isPresent()) {
            return rewriteByExistedPlan(targetGroup, plan);
        }

        // 步骤1：递归处理所有子节点，将子计划转换为 Group
        // 对于重写模式，需要验证子 Group 与目标 Group 的关系（避免循环）
        List<Group> childrenGroups = rewriteChildrenPlansToGroups(plan, targetGroup);
        
        // 步骤2：将计划节点的子节点替换为 GroupPlan
        // GroupPlan 是占位符，用于引用子 Group
        plan = replaceChildrenToGroupPlan(plan, childrenGroups);

        // 步骤3：尝试创建新的 GroupExpression
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);

        // 步骤4：慢速检查 - GroupExpression 是否已存在于 Memo 中
        // 通过 groupExpressions Map 进行等价性检测
        GroupExpression existedExpression = groupExpressions.get(newGroupExpression);
        if (existedExpression == null) {
            // 情况2 或 情况3：GroupExpression 不存在，需要创建新的
            return rewriteByNewGroupExpression(targetGroup, plan, newGroupExpression);
        } else {
            // 情况4 或 情况5：GroupExpression 已存在，需要处理合并
            return rewriteByExistedGroupExpression(targetGroup, plan, existedExpression, newGroupExpression);
        }
    }

    /**
     * 将计划添加到目标 Group 中（复制模式）
     * 
     * 这是将计划添加到 Memo 的核心方法之一，用于复制模式（非重写模式）。
     * 它会递归处理所有子节点，为每个子节点创建或查找对应的 Group，然后创建
     * GroupExpression 并插入到目标 Group 中。
     * 
     * 流程：
     * 1. 检查计划是否已经是 GroupPlan（不能是）
     * 2. 检查逻辑属性是否匹配（同一个 Group 中的计划必须有相同的逻辑属性）
     * 3. 处理物化视图嵌套重写（如果启用）
     * 4. 如果计划已经有 GroupExpression，直接返回
     * 5. 递归处理所有子节点，获取或创建子节点的 Group
     * 6. 将计划节点的子节点替换为 GroupPlan
     * 7. 创建 GroupExpression 并插入到目标 Group
     * 
     * 逻辑属性检查：
     * - 同一个 Group 中的所有计划必须在逻辑上等价
     * - 逻辑属性包括：输出列、统计信息、等价类等
     * - 如果逻辑属性不匹配，说明计划不等价，不能放在同一个 Group 中
     * 
     * @param plan 要添加的计划（不能是 GroupPlan）
     * @param targetGroup 目标 Group，如果为 null 则创建新的 Group
     *                    目标 Group 应该是计划的祖先 Group，或等于计划的 Group
     *                    为了性能考虑，不完整检查这个约束
     * @param planTable 计划表映射，用于快速查找计划对应的 Group（可选）
     * @return CopyInResult，其中：
     *         - generateNewExpression: true 表示新生成的 GroupExpression 被添加到 Memo
     *         - correspondingExpression: 对应的 GroupExpression（可能是新创建的或已存在的）
     */
    private CopyInResult doCopyIn(Plan plan, @Nullable Group targetGroup, @Nullable HashMap<Long, Group> planTable) {
        // 前置检查：计划不能是 GroupPlan（GroupPlan 是占位符，不是实际计划）
        Preconditions.checkArgument(!(plan instanceof GroupPlan), "plan can not be GroupPlan");
        
        // 步骤1：检查逻辑属性是否匹配
        // 同一个 Group 中的所有计划必须有相同的逻辑属性（输出列、统计信息等）
        // 如果逻辑属性不匹配，说明计划不等价，不能放在同一个 Group 中
        if (targetGroup != null && !plan.getLogicalProperties().equals(targetGroup.getLogicalProperties())) {
            LOG.info("Insert a plan into targetGroup but differ in logicalproperties."
                            + "\nPlan logicalproperties: {}\n targetGroup logicalproperties: {}",
                    plan.getLogicalProperties(), targetGroup.getLogicalProperties());
            throw new IllegalStateException("Insert a plan into targetGroup but differ in logicalproperties");
        }
        
        // 步骤2：处理物化视图嵌套重写
        // 如果启用了物化视图嵌套重写，且计划是异步物化视图（MTMV），
        // 需要更新刷新版本号，以便后续的结构信息提取能正确工作
        // TODO: 未来支持同步物化视图
        if (connectContext != null
                && connectContext.getSessionVariable().isEnableMaterializedViewNestRewrite()
                && plan instanceof LogicalCatalogRelation
                && ((CatalogRelation) plan).getTable() instanceof MTMV
                && !plan.getGroupExpression().isPresent()) {
            incrementAndGetRefreshVersion();
        }
        
        // 步骤3：如果计划已经有 GroupExpression，直接返回
        // 这种情况发生在计划已经被添加到 Memo 中时
        Optional<GroupExpression> groupExpr = plan.getGroupExpression();
        if (groupExpr.isPresent()) {
            Preconditions.checkState(groupExpressions.containsKey(groupExpr.get()));
            return CopyInResult.of(false, groupExpr.get());
        }
        
        // 步骤4：递归处理所有子节点，获取或创建子节点的 Group
        List<Group> childrenGroups = Lists.newArrayList();
        for (int i = 0; i < plan.children().size(); i++) {
            // 跳过无用的 Project（优化：某些 Project 节点可能被优化掉）
            Plan child = plan.child(i);
            if (child instanceof GroupPlan) {
                // 如果子节点是 GroupPlan，直接获取其 Group
                childrenGroups.add(((GroupPlan) child).getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                // 如果子节点已经有 GroupExpression，获取其 OwnerGroup
                childrenGroups.add(child.getGroupExpression().get().getOwnerGroup());
            } else {
                // 如果子节点还没有 GroupExpression，递归调用 doCopyIn 创建
                childrenGroups.add(doCopyIn(child, null, planTable).correspondingExpression.getOwnerGroup());
            }
        }
        
        // 步骤5：将计划节点的子节点替换为 GroupPlan
        // GroupPlan 是占位符，用于引用子 Group，而不是实际的计划节点
        plan = replaceChildrenToGroupPlan(plan, childrenGroups);
        
        // 步骤6：创建 GroupExpression 并插入到目标 Group
        GroupExpression newGroupExpression = new GroupExpression(plan, childrenGroups);
        return insertGroupExpression(newGroupExpression, targetGroup, plan.getLogicalProperties(), planTable);
        // TODO: 如果生成新 Group，需要推导逻辑属性。目前我们不将逻辑计划复制到 Memo 中
    }

    /**
     * 将计划的子节点转换为 Group（重写模式）
     * 
     * 这是 doRewrite 方法中用于处理子节点的辅助方法。它会递归处理所有子节点，
     * 为每个子节点获取或创建对应的 Group，并验证子 Group 与目标 Group 的关系。
     * 
     * 与 doCopyIn 中的子节点处理的主要区别：
     * 1. 需要验证子 Group 与目标 Group 的关系，避免循环引用
     * 2. 对于没有 GroupExpression 的子节点，调用 doRewrite 而不是 doCopyIn
     * 
     * 处理逻辑：
     * 1. 如果子节点是 GroupPlan，直接获取其 Group，并验证与目标 Group 的关系
     * 2. 如果子节点已有 GroupExpression，获取其 OwnerGroup，并验证与目标 Group 的关系
     * 3. 如果子节点还没有 GroupExpression，递归调用 doRewrite 创建，并获取其 OwnerGroup
     * 
     * 循环检测：
     * - 通过 validateRewriteChildGroup 验证子 Group 是否等于目标 Group
     * - 如果相等，说明存在循环引用（如 A => B(A)），会导致死循环
     * - 这种情况是不允许的，会抛出异常
     * 
     * 为什么需要循环检测？
     * - 在重写模式下，目标 Group 可能是计划的祖先 Group
     * - 如果子 Group 等于目标 Group，意味着计划引用了自己的祖先，形成循环
     * - 例如：A => B(A) 这种等价变换会导致死循环，因为 B 依赖于 A，而 A 又依赖于 B
     * 
     * 示例场景：
     * <pre>
     * 计划结构：
     *   LogicalJoin
     *     - LogicalOlapScan (child 1)
     *     - LogicalFilter (child 2)
     *       - LogicalOlapScan (child 2.1)
     * 
     * 处理过程：
     * 1. child 1 是 GroupPlan，获取其 Group，验证通过
     * 2. child 2 没有 GroupExpression，递归调用 doRewrite(child 2, null)
     * 3. child 2.1 是 GroupPlan，获取其 Group，验证通过
     * 4. 返回所有子节点的 Group 列表
     * </pre>
     * 
     * @param plan 要处理的计划（其子节点需要转换为 Group）
     * @param targetGroup 目标 Group（用于循环检测）
     * @return 子节点对应的 Group 列表
     */
    private List<Group> rewriteChildrenPlansToGroups(Plan plan, Group targetGroup) {
        List<Group> childrenGroups = Lists.newArrayList();
        
        // 遍历所有子节点，将每个子节点转换为对应的 Group
        for (int i = 0; i < plan.children().size(); i++) {
            Plan child = plan.children().get(i);
            
            if (child instanceof GroupPlan) {
                // 情况1：子节点是 GroupPlan
                // GroupPlan 是占位符，直接获取其引用的 Group
                GroupPlan childGroupPlan = (GroupPlan) child;
                
                // 验证子 Group 与目标 Group 的关系，避免循环引用
                // 如果子 Group 等于目标 Group，说明存在循环，会抛出异常
                validateRewriteChildGroup(childGroupPlan.getGroup(), targetGroup);
                
                // 将子 Group 添加到列表中
                childrenGroups.add(childGroupPlan.getGroup());
            } else if (child.getGroupExpression().isPresent()) {
                // 情况2：子节点已有 GroupExpression
                // 获取子节点的 GroupExpression 的 OwnerGroup
                Group childGroup = child.getGroupExpression().get().getOwnerGroup();
                
                // 验证子 Group 与目标 Group 的关系，避免循环引用
                validateRewriteChildGroup(childGroup, targetGroup);
                
                // 将子 Group 添加到列表中
                childrenGroups.add(childGroup);
            } else {
                // 情况3：子节点还没有 GroupExpression
                // 递归调用 doRewrite 将子节点添加到 Memo，并获取其 OwnerGroup
                // 注意：这里传入 null 作为目标 Group，因为子节点应该创建新的 Group
                childrenGroups.add(doRewrite(child, null).correspondingExpression.getOwnerGroup());
            }
        }
        
        return childrenGroups;
    }

    private void validateRewriteChildGroup(Group childGroup, Group targetGroup) {
        /*
         * 'A => B(A)' is invalid equivalent transform because of dead loop.
         * see 'MemoTest.a2ba()'
         */
        if (childGroup == targetGroup) {
            throw new IllegalStateException("Can not add plan which is ancestor of the target plan");
        }
    }

    /**
     * 将 GroupExpression 插入到目标 Group 中
     * 
     * 这是 Memo 中插入 GroupExpression 的核心方法，处理以下情况：
     * 1. 如果 GroupExpression 已存在于 Memo 中，且目标 Group 不为 null，则合并两个 Group
     * 2. 如果目标 Group 为 null，则创建新的 Group
     * 3. 如果目标 Group 不为 null，则将 GroupExpression 添加到目标 Group
     * 
     * 等价性检测：
     * - 通过 groupExpressions Map 快速检测 GroupExpression 是否已存在
     * - 如果已存在，说明找到了等价的计划，可以合并 Group 以减少搜索空间
     * 
     * @param groupExpression 要插入的 GroupExpression
     * @param target 目标 Group，如果为 null 则创建新的 Group
     * @param logicalProperties 逻辑属性（输出列、统计信息等）
     * @param planTable 计划表映射，用于快速查找计划对应的 Group（可选）
     * @return CopyInResult，其中：
     *         - generateNewExpression: true 表示新生成的 GroupExpression 被添加到 Memo
     *         - correspondingExpression: 对应的 GroupExpression（可能是新创建的或已存在的）
     */
    private CopyInResult insertGroupExpression(GroupExpression groupExpression, Group target,
            LogicalProperties logicalProperties, HashMap<Long, Group> planTable) {
        // 步骤1：检查 GroupExpression 是否已存在于 Memo 中
        GroupExpression existedGroupExpression = groupExpressions.get(groupExpression);
        
        if (existedGroupExpression != null) {
            // 情况1：GroupExpression 已存在
            // 如果目标 Group 不为 null，且与已存在的 GroupExpression 的 OwnerGroup 不同，则合并两个 Group
            if (target != null && !target.getGroupId().equals(existedGroupExpression.getOwnerGroup().getGroupId())) {
                mergeGroup(target, existedGroupExpression.getOwnerGroup(), planTable);
            }
            
            // 当我们创建 GroupExpression 时，会将其添加到子 Group 的 ParentExpression 中
            // 但如果它已存在，我们应该从子 Group 的 ParentExpression 中移除它
            // 因为这是临时创建的 GroupExpression，不应该影响已存在的 GroupExpression 的父子关系
            groupExpression.children().forEach(childGroup -> childGroup.removeParentExpression(groupExpression));
            
            // 返回已存在的 GroupExpression，表示没有生成新的
            return CopyInResult.of(false, existedGroupExpression);
        }
        
        // 情况2：GroupExpression 不存在，需要添加
        if (target != null) {
            // 如果目标 Group 不为 null，将 GroupExpression 添加到目标 Group
            target.addGroupExpression(groupExpression);
        } else {
            // 如果目标 Group 为 null，创建新的 Group
            Group group = new Group(groupIdGenerator.getNextId(), groupExpression, logicalProperties);
            groups.put(group.getGroupId(), group);
        }
        
        // 将 GroupExpression 添加到 Memo 的 groupExpressions Map 中
        groupExpressions.put(groupExpression, groupExpression);
        
        // 返回新创建的 GroupExpression
        return CopyInResult.of(true, groupExpression);
    }

    /**
     * 合并两个 Group
     * 
     * 当发现两个 Group 在逻辑上等价时，需要合并它们以减少搜索空间。
     * 合并过程包括：
     * 1. 找到所有以 source 作为子节点的 GroupExpression
     * 2. 将这些 GroupExpression 的子节点替换为 destination
     * 3. 替换后可能产生重复的 GroupExpression，需要移除冗余的
     * 4. 将 source 中的所有 GroupExpression 移动到 destination
     * 5. 更新 planTable 中的引用（如果提供）
     * 6. 如果 source 是根 Group，更新根 Group 为 destination
     * 7. 从 groups Map 中移除 source
     * 
     * 循环检测：
     * - 在合并前检查是否存在循环引用，如果存在则放弃合并
     * - 例如：如果 destination 的父 GroupExpression 的 OwnerGroup 是 source，则存在循环
     * 
     * 示例场景：
     * <pre>
     * 合并前：
     *   Group 1 (source)          Group 2 (destination)
     *   - LogicalOlapScan         - LogicalOlapScan
     * 
     * 合并后：
     *   Group 2 (destination)
     *   - LogicalOlapScan (来自 source)
     *   - LogicalOlapScan (原有的)
     * </pre>
     * 
     * @param source 源 Group（要被合并的 Group）
     * @param destination 目标 Group（合并后的 Group）
     * @param planTable 计划表映射，用于更新计划对应的 Group 引用（可选）
     */
    public void mergeGroup(Group source, Group destination, HashMap<Long, Group> planTable) {
        // 前置检查：如果 source 和 destination 是同一个 Group，直接返回
        if (source.equals(destination)) {
            return;
        }
        
        // 步骤1：循环检测 - 检查是否存在循环引用
        // 需要检查两个方向的循环：
        // 1. destination 的父 GroupExpression 的 OwnerGroup 是否是 source
        // 2. source 的父 GroupExpression 的 OwnerGroup 是否是 destination
        // 如果存在循环，不能合并，否则会导致死循环
        List<GroupExpression> needReplaceChild = Lists.newArrayList();
        
        // 检查 destination -> source 的循环
        for (GroupExpression dstParent : destination.getParentGroupExpressions()) {
            if (dstParent.getOwnerGroup().equals(source)) {
                // 存在循环，不能合并
                return;
            }
        }
        
        // 检查 source -> destination 的循环，并收集需要替换子节点的 GroupExpression
        for (GroupExpression srcParent : source.getParentGroupExpressions()) {
            if (srcParent.getOwnerGroup().equals(destination)) {
                // 存在循环，不能合并
                return;
            }
            
            // 跳过 Enforcer（强制器）类型的 GroupExpression
            // Enforcer 是用于强制物理属性的特殊 GroupExpression，不应该被合并
            Group parentOwnerGroup = srcParent.getOwnerGroup();
            if (parentOwnerGroup.getEnforcers().containsKey(srcParent)) {
                continue;
            }
            
            // 收集需要替换子节点的 GroupExpression
            // 这些 GroupExpression 的子节点中包含 source，需要替换为 destination
            needReplaceChild.add(srcParent);
        }
        
        // 记录 Group 合并事件（用于调试和性能分析）
        GROUP_MERGE_TRACER.log(GroupMergeEvent.of(source, destination, needReplaceChild));

        // 步骤2：替换所有父 GroupExpression 中的子节点引用
        // 将所有以 source 作为子节点的 GroupExpression 的子节点替换为 destination
        for (GroupExpression reinsertGroupExpr : needReplaceChild) {
            // 由于修改了 GroupExpression 的子节点，其 hashCode 会改变
            // 需要先从 groupExpressions Map 中移除，替换后再重新插入
            groupExpressions.remove(reinsertGroupExpr);
            
            // 将 source 替换为 destination
            reinsertGroupExpr.replaceChild(source, destination);

            // 检查替换后的 GroupExpression 是否已存在
            GroupExpression existGroupExpr = groupExpressions.get(reinsertGroupExpr);
            if (existGroupExpr != null) {
                // 情况1：替换后的 GroupExpression 已存在
                Preconditions.checkState(existGroupExpr.getOwnerGroup() != null);
                
                // 将 reinsertGroupExpr 标记为未使用，避免重复添加到 existGroupExpr 的 OwnerGroup
                // 因为 existGroupExpr 的 OwnerGroup 已经包含这个 GroupExpression
                reinsertGroupExpr.setUnused(true);
                
                if (existGroupExpr.getOwnerGroup().equals(reinsertGroupExpr.getOwnerGroup())) {
                    // 情况1.1：reinsertGroupExpr 和 existGroupExpr 在同一个 Group 中
                    // 需要合并它们的状态（如代价信息、物理属性等）
                    if (reinsertGroupExpr.getPlan() instanceof PhysicalPlan) {
                        // 如果是物理计划，需要替换最优计划
                        reinsertGroupExpr.getOwnerGroup().replaceBestPlanGroupExpr(reinsertGroupExpr, existGroupExpr);
                    }
                    // 将 reinsertGroupExpr 的状态合并到 existGroupExpr
                    reinsertGroupExpr.mergeTo(existGroupExpr);
                } else {
                    // 情况1.2：reinsertGroupExpr 和 existGroupExpr 不在同一个 Group 中
                    // 需要合并它们的 OwnerGroup（递归合并）
                    mergeGroup(reinsertGroupExpr.getOwnerGroup(), existGroupExpr.getOwnerGroup(), planTable);
                }
            } else {
                // 情况2：替换后的 GroupExpression 不存在，直接添加到 groupExpressions Map
                groupExpressions.put(reinsertGroupExpr, reinsertGroupExpr);
            }
        }
        
        // 步骤3：更新 planTable 中的引用
        // 将 planTable 中所有指向 source 的引用替换为 destination
        if (planTable != null) {
            planTable.forEach((bitset, group) -> {
                if (group.equals(source)) {
                    planTable.put(bitset, destination);
                }
            });
        }

        // 步骤4：将 source 的所有内容合并到 destination
        // 包括 GroupExpression、逻辑属性、物理属性等
        source.mergeTo(destination);
        
        // 步骤5：如果 source 是根 Group，更新根 Group 为 destination
        if (source == root) {
            root = destination;
        }
        
        // 步骤6：从 groups Map 中移除 source
        groups.remove(source.getGroupId());
    }

    /**
     * 使用已存在的计划进行重写（情况1：快速检查）
     * 
     * 这是 doRewrite 方法中处理情况1的方法：当计划已经是 GroupPlan 或已有 GroupExpression 时，
     * 说明计划已经在 Memo 中，不需要重新创建，直接使用已存在的计划。
     * 
     * 处理逻辑：
     * 1. 获取已存在的 GroupExpression
     *    - 如果计划是 GroupPlan，从 Group 中获取第一个逻辑 GroupExpression
     *    - 如果计划已有 GroupExpression，直接获取
     * 2. 如果目标 Group 不为 null，需要将已存在的 Group 中的所有内容移动到目标 Group
     *    - 清空目标 Group 的现有内容
     *    - 从已存在的 Group 中移动所有逻辑 GroupExpression 到目标 Group
     *    - 更新目标 Group 的逻辑属性
     * 3. 返回已存在的 GroupExpression（表示没有生成新的）
     * 
     * 为什么需要移动 GroupExpression？
     * - 在重写模式下，目标 Group 可能已经存在，但需要被新的计划替换
     * - 通过移动已存在的 GroupExpression 到目标 Group，可以保持 Memo 结构的一致性
     * - 同时避免创建重复的 GroupExpression
     * 
     * 示例场景：
     * <pre>
     * 重写前：
     *   targetGroup (空)
     *   existedGroup
     *     - LogicalOlapScan (existedLogicalExpression)
     * 
     * 重写后：
     *   targetGroup
     *     - LogicalOlapScan (从 existedGroup 移动过来)
     *   existedGroup (被回收)
     * </pre>
     * 
     * @param targetGroup 目标 Group，如果为 null 则不需要移动
     * @param existedPlan 已存在的计划（GroupPlan 或已有 GroupExpression 的计划）
     * @return CopyInResult，其中：
     *         - generateNewExpression: false（表示没有生成新的 GroupExpression）
     *         - correspondingExpression: 已存在的 GroupExpression
     */
    private CopyInResult rewriteByExistedPlan(Group targetGroup, Plan existedPlan) {
        // 步骤1：获取已存在的 GroupExpression
        GroupExpression existedLogicalExpression = existedPlan instanceof GroupPlan
                ? ((GroupPlan) existedPlan).getGroup().getLogicalExpression() // 从 GroupPlan 的 Group 中获取第一个逻辑 GroupExpression
                : existedPlan.getGroupExpression().get(); // 从计划中直接获取 GroupExpression
        
        // 步骤2：如果目标 Group 不为 null，需要移动已存在的 Group 的内容到目标 Group
        if (targetGroup != null) {
            Group existedGroup = existedLogicalExpression.getOwnerGroup();
            
            // 清空目标 Group，从已存在的 Group 中移动所有逻辑 GroupExpression 和逻辑属性到目标 Group
            // 这个过程包括：
            // 1. 清空目标 Group 的现有逻辑和物理 GroupExpression
            // 2. 从已存在的 Group 中移除所有逻辑 GroupExpression
            // 3. 将这些逻辑 GroupExpression 添加到目标 Group
            // 4. 更新目标 Group 的逻辑属性
            // 5. 回收已存在的 Group（如果为空）
            eliminateFromGroupAndMoveToTargetGroup(existedGroup, targetGroup, existedPlan.getLogicalProperties());
        }
        
        // 步骤3：返回已存在的 GroupExpression（表示没有生成新的）
        return CopyInResult.of(false, existedLogicalExpression);
    }

    public Group newGroup(LogicalProperties logicalProperties) {
        Group group = new Group(groupIdGenerator.getNextId(), logicalProperties);
        groups.put(group.getGroupId(), group);
        return group;
    }

    private CopyInResult rewriteByNewGroupExpression(Group targetGroup, Plan newPlan,
            GroupExpression newGroupExpression) {
        if (targetGroup == null) {
            // case 2:
            // if not exist target group and not exist the same group expression,
            // then create new group with the newGroupExpression
            Group newGroup = new Group(groupIdGenerator.getNextId(), newGroupExpression,
                    newPlan.getLogicalProperties());
            groups.put(newGroup.getGroupId(), newGroup);
            groupExpressions.put(newGroupExpression, newGroupExpression);
        } else {
            // case 3:
            // if exist the target group, clear all origin group expressions in the
            // existedExpression's owner group and reset logical properties, the
            // newGroupExpression is the init logical group expression.
            reInitGroup(targetGroup, newGroupExpression, newPlan.getLogicalProperties());

            // note: put newGroupExpression must behind recycle existedExpression(reInitGroup method),
            //       because existedExpression maybe equal to the newGroupExpression and recycle
            //       existedExpression will recycle newGroupExpression
            groupExpressions.put(newGroupExpression, newGroupExpression);
        }
        return CopyInResult.of(true, newGroupExpression);
    }

    private CopyInResult rewriteByExistedGroupExpression(Group targetGroup, Plan transformedPlan,
            GroupExpression existedExpression, GroupExpression newExpression) {
        if (targetGroup != null && !targetGroup.equals(existedExpression.getOwnerGroup())) {
            // case 4:
            existedExpression.propagateApplied(newExpression);
            moveParentExpressionsReference(existedExpression.getOwnerGroup(), targetGroup);
            recycleGroup(existedExpression.getOwnerGroup());
            reInitGroup(targetGroup, newExpression, transformedPlan.getLogicalProperties());

            // note: put newGroupExpression must behind recycle existedExpression(reInitGroup method),
            //       because existedExpression maybe equal to the newGroupExpression and recycle
            //       existedExpression will recycle newGroupExpression
            groupExpressions.put(newExpression, newExpression);
            return CopyInResult.of(true, newExpression);
        } else {
            // case 5:
            // if targetGroup is null or targetGroup equal to the existedExpression's ownerGroup,
            // then recycle the temporary new group expression
            // No ownerGroup, don't need ownerGroup.removeChild()
            recycleExpression(newExpression);
            return CopyInResult.of(false, existedExpression);
        }
    }

    /**
     * eliminate fromGroup, clear targetGroup, then move the logical group expressions in the fromGroup to the toGroup.
     * <p>
     * the scenario is:
     * <pre>
     *  Group 1(project, the targetGroup)                  Group 1(logicalOlapScan, the targetGroup)
     *               |                             =>
     *  Group 0(logicalOlapScan, the fromGroup)
     * </pre>
     * we should recycle the group 0, and recycle all group expressions in group 1, then move the logicalOlapScan to
     * the group 1, and reset logical properties of the group 1.
     */
    private void eliminateFromGroupAndMoveToTargetGroup(Group fromGroup, Group targetGroup,
            LogicalProperties logicalProperties) {
        if (fromGroup == targetGroup) {
            return;
        }
        // simple check targetGroup is the ancestors of the fromGroup, not check completely because of performance
        if (fromGroup == root) {
            throw new IllegalStateException(
                    "TargetGroup should be ancestors of fromGroup, but fromGroup is root. Maybe a bug");
        }

        List<GroupExpression> logicalExpressions = fromGroup.clearLogicalExpressions();
        recycleGroup(fromGroup);

        recycleLogicalAndPhysicalExpressions(targetGroup);

        for (GroupExpression logicalExpression : logicalExpressions) {
            targetGroup.addLogicalExpression(logicalExpression);
        }
        targetGroup.setLogicalProperties(logicalProperties);
    }

    private void reInitGroup(Group group, GroupExpression initLogicalExpression, LogicalProperties logicalProperties) {
        recycleLogicalAndPhysicalExpressions(group);

        group.setLogicalProperties(logicalProperties);
        group.addLogicalExpression(initLogicalExpression);
    }

    private Plan replaceChildrenToGroupPlan(Plan plan, List<Group> childrenGroups) {
        if (childrenGroups.isEmpty()) {
            return plan;
        }

        ImmutableList.Builder<Plan> groupPlanChildren = ImmutableList.builderWithExpectedSize(childrenGroups.size());
        for (Group childrenGroup : childrenGroups) {
            groupPlanChildren.add(new GroupPlan(childrenGroup));
        }
        return plan.withChildren(groupPlanChildren.build());
    }

    /*
     * the scenarios that 'parentGroupExpression == toGroup': eliminate the root group.
     * the fromGroup is group 1, the toGroup is group 2, we can not replace group2's
     * groupExpressions reference the child group which is group 2 (reference itself).
     *
     *   A(group 2)            B(group 2)
     *   |                     |
     *   B(group 1)      =>    C(group 0)
     *   |
     *   C(group 0)
     *
     *
     * note: the method don't save group and groupExpression to the memo, so you need
     *       save group and groupExpression to the memo at other place.
     */
    private void moveParentExpressionsReference(Group fromGroup, Group toGroup) {
        for (GroupExpression parentGroupExpression : fromGroup.getParentGroupExpressions()) {
            if (parentGroupExpression.getOwnerGroup() != toGroup) {
                parentGroupExpression.replaceChild(fromGroup, toGroup);
            }
        }
    }

    /**
     * Notice: this func don't replace { Parent GroupExpressions -> this Group }.
     */
    private void recycleGroup(Group group) {
        // recycle in memo.
        if (groups.get(group.getGroupId()) == group) {
            groups.remove(group.getGroupId());
        }

        // recycle children GroupExpression
        recycleLogicalAndPhysicalExpressions(group);
    }

    private void recycleLogicalAndPhysicalExpressions(Group group) {
        group.getLogicalExpressions().forEach(this::recycleExpression);
        group.clearLogicalExpressions();

        group.getPhysicalExpressions().forEach(this::recycleExpression);
        group.clearPhysicalExpressions();
    }

    /**
     * Notice: this func don't clear { OwnerGroup() -> this GroupExpression }.
     */
    private void recycleExpression(GroupExpression groupExpression) {
        // recycle in memo.
        if (groupExpressions.get(groupExpression) == groupExpression) {
            groupExpressions.remove(groupExpression);
        }

        // recycle parentGroupExpr in childGroup
        groupExpression.children().forEach(childGroup -> {
            // if not any groupExpression reference child group, then recycle the child group
            if (childGroup.removeParentExpression(groupExpression) == 0) {
                recycleGroup(childGroup);
            }
        });

        groupExpression.setOwnerGroup(null);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("root:").append(getRoot()).append("\n");
        for (Group group : groups.values()) {
            builder.append("\n\n").append(group).append("\n");
        }
        return builder.toString();
    }

    /**
     * 对所有计划进行排名并选择第 n 个计划
     * 
     * 该算法基于论文《Counting, Enumerating, and Sampling of Execution Plans in a Cost-Based Query Optimizer》。
     * 
     * 算法原理：
     * 1. 在 rank() 函数中，为 Memo 中的每个物理计划分配一个唯一的 ID
     * 2. 根据代价对计划进行排序
     * 3. 选择第 n 个计划（代价第 n 小的计划）
     * 
     * 注意：
     * - 在 rank() 函数中不会生成任何物理计划，只是分配 ID 和计算代价
     * - 在 unrank() 函数中，会根据唯一 ID 提取实际的物理计划
     * 
     * 使用场景：
     * - 多计划优化：当需要选择多个候选计划时
     * - 计划采样：当需要随机采样计划时
     * - 计划对比：当需要对比不同代价的计划时
     * 
     * @param n 要选择的计划排名（从 1 开始，1 表示代价最小的计划）
     * @return Pair，包含：
     *         - 第一个元素：计划的唯一 ID（用于 unrank 时提取计划）
     *         - 第二个元素：计划的代价
     */
    public Pair<Long, Double> rank(long n) {
        double threshold = 0.000000001;
        Preconditions.checkArgument(n > 0, "the n %d must be greater than 0 in nthPlan", n);
        List<Pair<Long, Cost>> plans = rankGroup(root, PhysicalProperties.GATHER);
        plans = plans.stream()
                .filter(p -> Double.isFinite(p.second.getValue()))
                .collect(Collectors.toList());
        // This is big heap, it always pops the element with larger cost or larger id.
        PriorityQueue<Pair<Long, Cost>> pq = new PriorityQueue<>((l, r) ->
                Math.abs(l.second.getValue() - r.second.getValue()) < threshold
                        ? -Long.compare(l.first, r.first)
                        : -Double.compare(l.second.getValue(), r.second.getValue()));
        for (Pair<Long, Cost> p : plans) {
            pq.add(p);
            if (pq.size() > n) {
                pq.poll();
            }
        }
        Preconditions.checkArgument(pq.peek() != null, "rank error because there is no valid plan");
        return Pair.of(pq.peek().first, pq.peek().second.getValue());
    }

    /**
     * 获取可以排名的计划数量
     * 
     * 返回 Memo 中所有有效的物理计划数量（代价为有限值的计划）。
     * 用于了解优化器生成了多少个候选计划。
     * 
     * 有效计划的条件：
     * - 代价不是 NaN（Not a Number）
     * - 代价不是正无穷
     * - 代价不是负无穷
     * 
     * 使用场景：
     * - 调试：了解优化器生成了多少个候选计划
     * - 性能分析：评估优化器的搜索空间大小
     * - 计划选择：在选择计划前了解有多少个候选
     * 
     * @return 可以排名的计划数量
     */
    public int getRankSize() {
        // 获取所有计划的排名和代价
        List<Pair<Long, Cost>> plans = rankGroup(root, PhysicalProperties.GATHER);
        
        // 过滤出有效的计划（代价为有限值）
        plans = plans.stream().filter(
                        p -> !p.second.equals(Double.NaN)
                                && !p.second.equals(Double.POSITIVE_INFINITY)
                                && !p.second.equals(Double.NEGATIVE_INFINITY))
                .collect(Collectors.toList());
        
        return plans.size();
    }

    private List<Pair<Long, Cost>> rankGroup(Group group, PhysicalProperties prop) {
        List<Pair<Long, Cost>> res = new ArrayList<>();
        int prefix = 0;
        List<GroupExpression> validGroupExprList = extractGroupExpressionSatisfyProp(group, prop);
        for (GroupExpression groupExpression : validGroupExprList) {
            for (Pair<Long, Cost> idCostPair : rankGroupExpression(groupExpression, prop)) {
                res.add(Pair.of(idCostPair.first + prefix, idCostPair.second));
            }
            prefix = res.size();
            // avoid ranking all plans
            if (res.size() > 1e2) {
                break;
            }
        }
        return res;
    }

    private List<Pair<Long, Cost>> rankGroupExpression(GroupExpression groupExpression,
            PhysicalProperties prop) {
        if (!groupExpression.getLowestCostTable().containsKey(prop)) {
            return new ArrayList<>();
        }
        List<Pair<Long, Cost>> res = new ArrayList<>();
        if (groupExpression.getPlan() instanceof LeafPlan) {
            res.add(Pair.of(0L, groupExpression.getCostValueByProperties(prop)));
            return res;
        }

        List<List<PhysicalProperties>> inputPropertiesList = extractInputProperties(groupExpression, prop);
        for (List<PhysicalProperties> inputProperties : inputPropertiesList) {
            int prefix = res.size();
            List<List<Pair<Long, Cost>>> children = new ArrayList<>();
            for (int i = 0; i < inputProperties.size(); i++) {
                // To avoid reach a circle, we don't allow ranking the same group with the same physical properties.
                Preconditions.checkArgument(!groupExpression.child(i).equals(groupExpression.getOwnerGroup())
                        || !prop.equals(inputProperties.get(i)));
                List<Pair<Long, Cost>> idCostPair
                        = rankGroup(groupExpression.child(i), inputProperties.get(i));
                children.add(idCostPair);
            }

            List<Pair<Long, List<Integer>>> childrenId = new ArrayList<>();
            permute(children, 0, childrenId, new ArrayList<>());
            Cost cost = CostCalculator.calculateCost(connectContext, groupExpression, inputProperties);
            for (Pair<Long, List<Integer>> c : childrenId) {
                Cost totalCost = cost;
                for (int i = 0; i < children.size(); i++) {
                    totalCost = CostCalculator.addChildCost(connectContext,
                            groupExpression.getPlan(),
                            totalCost,
                            children.get(i).get(c.second.get(i)).second,
                            i);
                }
                if (res.isEmpty()) {
                    Preconditions.checkArgument(
                            Math.abs(totalCost.getValue() - groupExpression.getCostByProperties(prop)) < 0.0001,
                            "Please check operator %s, expected cost %s but found %s",
                            groupExpression.getPlan().shapeInfo(), totalCost.getValue(),
                            groupExpression.getCostByProperties(prop));
                }
                res.add(Pair.of(prefix + c.first, totalCost));
            }
        }

        return res;
    }

    /**
     * we permute all children, e.g.,
     * for children [1, 2] [1, 2, 3]
     * we can get: 0: [1,1] 1:[1, 2] 2:[1, 3] 3:[2, 1] 4:[2, 2] 5:[2, 3]
     */
    private void permute(List<List<Pair<Long, Cost>>> children, int index,
            List<Pair<Long, List<Integer>>> result, List<Integer> current) {
        if (index == children.size()) {
            result.add(Pair.of(getUniqueId(children, current), current));
            return;
        }
        for (int i = 0; i < children.get(index).size(); i++) {
            List<Integer> next = new ArrayList<>(current);
            next.add(i);
            permute(children, index + 1, result, next);
        }
    }

    /**
     * This method is used to calculate the unique ID for one combination,
     * The current is used to represent the index of the child in lists e.g.,
     * for children [1], [1, 2], The possible indices and IDs are:
     * [0, 0]: 0*1 + 0*1*2
     * [0, 1]: 0*1 + 1*1*2
     */
    private static long getUniqueId(List<List<Pair<Long, Cost>>> lists, List<Integer> current) {
        long id = 0;
        long factor = 1;
        for (int i = 0; i < lists.size(); i++) {
            id += factor * current.get(i);
            factor *= lists.get(i).size();
        }
        return id;
    }

    private List<GroupExpression> extractGroupExpressionSatisfyProp(Group group, PhysicalProperties prop) {
        GroupExpression bestExpr = group.getLowestCostPlan(prop).get().second;
        List<GroupExpression> exprs = Lists.newArrayList(bestExpr);
        Set<GroupExpression> hasVisited = new HashSet<>();
        hasVisited.add(bestExpr);
        Stream.concat(group.getPhysicalExpressions().stream(), group.getEnforcers().keySet().stream())
                .forEach(groupExpression -> {
                    if (!groupExpression.getInputPropertiesListOrEmpty(prop).isEmpty()
                            && !groupExpression.equals(bestExpr) && !hasVisited.contains(groupExpression)) {
                        hasVisited.add(groupExpression);
                        exprs.add(groupExpression);
                    }
                });
        return exprs;
    }

    // ----------------------------------------------------------------
    // extract input properties for a given group expression and required output properties
    // There are three cases:
    // 1. If group expression is enforcer, return the input properties of the best expression
    // 2. If group expression require any, return any input properties
    // 3. Otherwise, return all input properties that satisfies the required output properties
    private List<List<PhysicalProperties>> extractInputProperties(GroupExpression groupExpression,
            PhysicalProperties prop) {
        List<List<PhysicalProperties>> res = new ArrayList<>();
        res.add(groupExpression.getInputPropertiesList(prop));

        // return optimized input for enforcer
        if (groupExpression.getOwnerGroup().getEnforcers().containsKey(groupExpression)) {
            return res;
        }

        // return any if exits except RequirePropertiesSupplier and SetOperators
        // Because PropRegulator could change their input properties
        RequestPropertyDeriver requestPropertyDeriver = new RequestPropertyDeriver(connectContext, prop);
        List<List<PhysicalProperties>> requestList = requestPropertyDeriver
                .getRequestChildrenPropertyList(groupExpression);
        Optional<List<PhysicalProperties>> any = requestList.stream()
                .filter(e -> e.stream().allMatch(PhysicalProperties.ANY::equals))
                .findAny();
        if (any.isPresent()
                && !(groupExpression.getPlan() instanceof SetOperation)) {
            res.clear();
            res.add(any.get());
            return res;
        }

        // return all optimized inputs
        Set<List<PhysicalProperties>> inputProps = groupExpression.getLowestCostTable().keySet().stream()
                .filter(physicalProperties -> physicalProperties.satisfy(prop))
                .map(groupExpression::getInputPropertiesList)
                .collect(Collectors.toSet());
        res.addAll(inputProps);
        return res;
    }

    private int getGroupSize(Group group, PhysicalProperties prop,
            Map<GroupExpression, List<List<Integer>>> exprSizeCache) {
        List<GroupExpression> validGroupExprs = extractGroupExpressionSatisfyProp(group, prop);
        int groupCount = 0;
        for (GroupExpression groupExpression : validGroupExprs) {
            int exprCount = getExprSize(groupExpression, prop, exprSizeCache);
            groupCount += exprCount;
            if (groupCount > 1e2) {
                break;
            }
        }
        return groupCount;
    }

    // return size for each input properties
    private int getExprSize(GroupExpression groupExpression, PhysicalProperties properties,
            Map<GroupExpression, List<List<Integer>>> exprChildSizeCache) {
        List<List<Integer>> exprCount = new ArrayList<>();
        if (!groupExpression.getLowestCostTable().containsKey(properties)) {
            exprCount.add(Lists.newArrayList(0));
        } else if (groupExpression.getPlan() instanceof LeafPlan) {
            exprCount.add(Lists.newArrayList(1));
        } else {
            List<List<PhysicalProperties>> inputPropertiesList = extractInputProperties(groupExpression, properties);
            for (List<PhysicalProperties> inputProperties : inputPropertiesList) {
                List<Integer> groupExprSize = new ArrayList<>();
                for (int i = 0; i < inputProperties.size(); i++) {
                    groupExprSize.add(
                            getGroupSize(groupExpression.child(i), inputProperties.get(i), exprChildSizeCache));
                }
                exprCount.add(groupExprSize);
            }
        }
        exprChildSizeCache.put(groupExpression, exprCount);
        return exprCount.stream()
                .mapToInt(s -> s.stream().reduce(1, (a, b) -> a * b))
                .sum();
    }

    private PhysicalPlan unrankGroup(Group group, PhysicalProperties prop, long rank,
            Map<GroupExpression, List<List<Integer>>> exprSizeCache) {
        int prefix = 0;
        for (GroupExpression groupExpression : extractGroupExpressionSatisfyProp(group, prop)) {
            int exprCount = exprSizeCache.get(groupExpression).stream()
                    .mapToInt(s -> s.stream().reduce(1, (a, b) -> a * b))
                    .sum();
            // rank is start from 0
            if (exprCount != 0 && rank + 1 - prefix <= exprCount) {
                return unrankGroupExpression(groupExpression, prop, rank - prefix,
                        exprSizeCache);
            }
            prefix += exprCount;
        }
        throw new RuntimeException("the group has no plan for prop %s in rank job");
    }

    private PhysicalPlan unrankGroupExpression(GroupExpression groupExpression, PhysicalProperties prop, long rank,
            Map<GroupExpression, List<List<Integer>>> exprSizeCache) {
        if (groupExpression.getPlan() instanceof LeafPlan) {
            Preconditions.checkArgument(rank == 0,
                    "leaf plan's %s rank must be 0 but is %d", groupExpression, rank);
            return ((PhysicalPlan) groupExpression.getPlan()).withPhysicalPropertiesAndStats(
                    groupExpression.getOutputProperties(prop),
                    groupExpression.getOwnerGroup().getStatistics());
        }

        List<List<PhysicalProperties>> inputPropertiesList = extractInputProperties(groupExpression, prop);
        for (int i = 0; i < inputPropertiesList.size(); i++) {
            List<PhysicalProperties> properties = inputPropertiesList.get(i);
            List<Integer> childrenSize = exprSizeCache.get(groupExpression).get(i);
            int count = childrenSize.stream().reduce(1, (a, b) -> a * b);
            if (rank >= count) {
                rank -= count;
                continue;
            }
            List<Long> childrenRanks = extractChildRanks(rank, childrenSize);
            List<Plan> childrenPlan = new ArrayList<>();
            for (int j = 0; j < properties.size(); j++) {
                Plan plan = unrankGroup(groupExpression.child(j), properties.get(j),
                        childrenRanks.get(j), exprSizeCache);
                Preconditions.checkArgument(plan != null, "rank group get null");
                childrenPlan.add(plan);
            }

            Plan plan = groupExpression.getPlan().withChildren(childrenPlan);
            return ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                    groupExpression.getOutputProperties(prop),
                    groupExpression.getOwnerGroup().getStatistics());
        }
        throw new RuntimeException("the groupExpr has no plan for prop in rank job");
    }

    /**
     * This method is used to decode ID for each child, which is the opposite of getUniqueID method, e.g.,
     * 0: [0%1, 0%(1*2)]
     * 1: [1%1, 1%(1*2)]
     * 2: [2%1, 2%(1*2)]
     */
    private List<Long> extractChildRanks(long rank, List<Integer> childrenSize) {
        Preconditions.checkArgument(!childrenSize.isEmpty(), "children should not empty in extractChildRanks");
        List<Long> indices = new ArrayList<>();
        for (int i = 0; i < childrenSize.size(); i++) {
            int factor = childrenSize.get(i);
            indices.add(rank % factor);
            rank = rank / factor;
        }
        return indices;
    }

    /**
     * 根据唯一 ID 提取物理计划（unrank）
     * 
     * 这是 rank/unrank 机制中的 unrank 部分，用于根据 rank() 方法返回的唯一 ID
     * 提取对应的物理计划。
     * 
     * 工作流程：
     * 1. 计算每个 Group 和 GroupExpression 的计划数量（缓存）
     * 2. 根据唯一 ID 递归查找对应的计划
     * 3. 构建完整的物理计划树
     * 
     * ID 编码规则：
     * - 每个 GroupExpression 的子计划组合都有一个唯一的 ID
     * - ID 是通过 getUniqueId 方法计算的，基于子计划的索引组合
     * - 例如：对于子计划 [1], [1, 2]，ID 计算为：0*1 + index*1*2
     * 
     * 使用场景：
     * - 多计划优化：当需要选择多个候选计划时
     * - 计划采样：当需要随机采样计划时
     * - 计划对比：当需要对比不同代价的计划时
     * 
     * @param id 计划的唯一 ID（由 rank() 方法返回）
     * @return 对应的物理计划
     */
    public PhysicalPlan unrank(long id) {
        // 步骤1：计算每个 Group 和 GroupExpression 的计划数量（缓存）
        // 这个缓存用于快速查找指定 ID 对应的计划
        Map<GroupExpression, List<List<Integer>>> exprSizeCache = new HashMap<>();
        getGroupSize(getRoot(), PhysicalProperties.GATHER, exprSizeCache);
        
        // 步骤2：从根 Group 开始，根据 ID 递归查找对应的计划
        return unrankGroup(getRoot(), PhysicalProperties.GATHER, id, exprSizeCache);
    }
}
