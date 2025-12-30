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

import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperElement;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils.TableQueryOperatorChecker;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughWindow;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand.PredicateAddContext;
import org.apache.doris.nereids.trees.plans.commands.UpdateMvByPartitionCommand.PredicateAdder;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * StructInfo for plan, this contains necessary info for query rewrite by materialized view
 * the struct info is used by all materialization, so it's struct info should only get, should not
 * modify, if wanting to modify, should copy and then modify
 */
public class StructInfo {
    public static final PlanPatternChecker PLAN_PATTERN_CHECKER = new PlanPatternChecker();
    public static final ScanPlanPatternChecker SCAN_PLAN_PATTERN_CHECKER = new ScanPlanPatternChecker();
    // struct info splitter
    public static final PlanSplitter PLAN_SPLITTER = new PlanSplitter();
    public static final PredicateCollector PREDICATE_COLLECTOR = new PredicateCollector();
    // source data
    private final Plan originalPlan;
    private final ObjectId originalPlanId;
    private final HyperGraph hyperGraph;
    private final boolean valid;
    // derived data following
    // top plan which may include project or filter, except for join and scan
    private final Plan topPlan;
    // bottom plan which top plan only contain join or scan. this is needed by hyper graph
    private final Plan bottomPlan;
    private final List<CatalogRelation> relations;
    // This is generated by cascadesContext, this may be different in different cascadesContext
    // So if the cascadesContext currently is different form the cascadesContext which generated it.
    // Should regenerate the tableBitSet by current cascadesContext and call withTableBitSet method
    private final BitSet tableBitSet;
    // this is for LogicalCompatibilityContext later
    private final Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap;
    // this recorde the predicates which can pull up, not shuttled
    private final Predicates predicates;
    // this record the grouping id generated by repeat node
    private final Optional<SlotReference> groupingId;
    // split predicates is shuttled
    private SplitPredicate splitPredicate;
    private EquivalenceClass equivalenceClass;
    // For value of Map, the key is the position of expression
    // the value is the expressions and the hyper element of expression pair
    // Key of pair is the expression shuttled and the value is the origin expression and the hyper element it belonged
    // Sometimes origin expressions are different and shuttled expression is same
    // Such as origin expressions are l_partkey#0 > 1 and l_partkey#10 > 1 and shuttled expression is l_partkey#10 > 1
    // this is for building LogicalCompatibilityContext later.
    private final Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
            shuttledExpressionsToExpressionsMap;
    // For value of Map, the key is the position of expression
    // the value is the original expression and shuttled expression map
    // Such as origin expressions are l_partkey#0 > 1 and shuttled expression is l_partkey#10 > 1
    // the map would be {ExpressionPosition.FILTER, {
    //     l_partkey#0 > 1 : l_partkey#10 > 1
    // }}
    // this is for building LogicalCompatibilityContext later.
    private final Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap;

    // planOutputShuttledExpressions: 计划输出表达式经过 shuttle 后的结果（统一到基表列空间）
    // 
    // 构建过程（以查询计划为例）：
    // 1. originalPlan.getOutput(): 计划输出的 NamedExpression 列表
    //    例如：SELECT user_id, date_trunc(create_time, 'day') as dt, sum(amount) as total
    //    输出：[user_id#5, Alias(dt#6, date_trunc(create_time#7, 'day')), Alias(total#8, sum(amount#9))]
    // 
    // 2. ExpressionUtils.shuttleExpressionWithLineage(originalPlan.getOutput(), originalPlan, new BitSet())
    //    将输出表达式 shuttle 到基表列空间，展开 Alias，统一 ExprId 到基表列
    //    结果：[user_id#0, date_trunc(create_time#0, 'day'), sum(amount#0)]
    //    （所有列引用都统一到基表列空间，ExprId 变为基表列的 ExprId，如 user_id#0）
    // 
    // 作用：
    // - 用于物化视图匹配：将查询和 MV 的输出表达式都统一到基表列空间后，才能正确匹配
    // - 用于表达式重写：MaterializationContext 使用它构建 shuttledExprToScanExprMapping，
    //   将查询表达式（基表列空间）映射到 MV scan 的输出 slot
    // 
    // 注意：
    // - shuttle 过程会展开 Alias，只保留底层表达式（如 date_trunc(create_time#0, 'day')），不再有别名
    // - 必须使用 originalPlan.getOutput() 而不是 derivedPlan.getOutput()，以确保输出顺序正确
    private final List<? extends Expression> planOutputShuttledExpressions;

    /**
     * The construct method for StructInfo
     */
    private StructInfo(Plan originalPlan, ObjectId originalPlanId, HyperGraph hyperGraph, boolean valid, Plan topPlan,
            Plan bottomPlan, List<CatalogRelation> relations,
            Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap,
            @Nullable Predicates predicates,
            Optional<SlotReference> groupingId,
            Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                    shuttledExpressionsToExpressionsMap,
            Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap,
            BitSet tableIdSet,
            SplitPredicate splitPredicate,
            EquivalenceClass equivalenceClass,
            List<? extends Expression> planOutputShuttledExpressions) {
        this.originalPlan = originalPlan;
        this.originalPlanId = originalPlanId;
        this.hyperGraph = hyperGraph;
        this.valid = valid;
        this.topPlan = topPlan;
        this.bottomPlan = bottomPlan;
        this.relations = relations;
        this.tableBitSet = tableIdSet;
        this.relationIdStructInfoNodeMap = relationIdStructInfoNodeMap;
        this.predicates = predicates;
        this.groupingId = groupingId;
        this.splitPredicate = splitPredicate;
        this.equivalenceClass = equivalenceClass;
        this.shuttledExpressionsToExpressionsMap = shuttledExpressionsToExpressionsMap;
        this.expressionToShuttledExpressionToMap = expressionToShuttledExpressionToMap;
        this.planOutputShuttledExpressions = planOutputShuttledExpressions;
    }

    /**
     * Construct StructInfo with new predicates
     */
    public StructInfo withPredicates(Predicates predicates) {
        return new StructInfo(this.originalPlan, this.originalPlanId, this.hyperGraph, this.valid, this.topPlan,
                this.bottomPlan, this.relations, this.relationIdStructInfoNodeMap, predicates, this.groupingId,
                this.shuttledExpressionsToExpressionsMap, this.expressionToShuttledExpressionToMap,
                this.tableBitSet, null, null, this.planOutputShuttledExpressions);
    }

    private static boolean collectStructInfoFromGraph(HyperGraph hyperGraph,
            Plan topPlan,
            Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                    shuttledExpressionsToExpressionsMap,
            Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap,
            List<CatalogRelation> relations,
            Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap,
            BitSet hyperTableBitSet,
            CascadesContext cascadesContext) {

        // Collect relations from hyper graph which in the bottom plan firstly
        hyperGraph.getNodes().forEach(node -> {
            // plan relation collector and set to map
            StructInfoNode structInfoNode = (StructInfoNode) node;
            // plan relation collector and set to map
            Set<CatalogRelation> nodeRelations = structInfoNode.getCatalogRelation();
            relations.addAll(structInfoNode.getCatalogRelation());
            nodeRelations.forEach(relation -> hyperTableBitSet.set(relation.getRelationId().asInt()));
            // record expressions in node
            List<Expression> nodeExpressions = structInfoNode.getCouldMoveExpressions();
            if (nodeExpressions != null) {
                List<? extends Expression> shuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                        nodeExpressions, structInfoNode.getPlan(),
                        new BitSet());
                for (int index = 0; index < nodeExpressions.size(); index++) {
                    putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                            expressionToShuttledExpressionToMap, ExpressionPosition.NODE_COULD_MOVE,
                            shuttledExpressions.get(index), nodeExpressions.get(index), node);
                }
            }
            nodeExpressions = structInfoNode.getCouldNotMoveExpressions();
            if (nodeExpressions != null) {
                List<? extends Expression> shuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                        nodeExpressions, structInfoNode.getPlan(),
                        new BitSet());
                for (int index = 0; index < nodeExpressions.size(); index++) {
                    putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                            expressionToShuttledExpressionToMap, ExpressionPosition.NODE_COULD_NOT_MOVE,
                            shuttledExpressions.get(index), nodeExpressions.get(index), node);
                }
            }
            // every node should only have one relation, this is for LogicalCompatibilityContext
            if (!nodeRelations.isEmpty()) {
                relationIdStructInfoNodeMap.put(nodeRelations.iterator().next().getRelationId(), structInfoNode);
            }
        });
        // Collect expression from join condition in hyper graph
        for (JoinEdge edge : hyperGraph.getJoinEdges()) {
            List<? extends Expression> joinConjunctExpressions = edge.getExpressions();
            // shuttle expression in edge for the build of LogicalCompatibilityContext later.
            // Record the exprId to expr map in the processing to strut info
            // Replace expressions by expression map
            List<? extends Expression> shuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                    joinConjunctExpressions, topPlan, new BitSet());
            for (int i = 0; i < shuttledExpressions.size(); i++) {
                putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                        expressionToShuttledExpressionToMap,
                        ExpressionPosition.JOIN_EDGE, shuttledExpressions.get(i),
                        joinConjunctExpressions.get(i), edge);
            }
        }
        // Collect expression from where in hyper graph
        hyperGraph.getFilterEdges().forEach(filterEdge -> {
            List<? extends Expression> filterExpressions = filterEdge.getExpressions();
            List<? extends Expression> shuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                    filterExpressions, topPlan, new BitSet());
            for (int i = 0; i < shuttledExpressions.size(); i++) {
                putShuttledExpressionToExpressionsMap(shuttledExpressionsToExpressionsMap,
                        expressionToShuttledExpressionToMap,
                        ExpressionPosition.FILTER_EDGE, shuttledExpressions.get(i),
                        filterExpressions.get(i), filterEdge);
            }
        });
        return true;
    }

    /**
     * 从谓词中推导出分割谓词（SplitPredicate）和等价类（EquivalenceClass）。
     * 
     * 该方法用于物化视图重写，将原始谓词转换为便于匹配和补偿的形式。
     * 
     * ==================== 处理流程 ====================
     * 
     * 【步骤1：收集可上提的谓词】
     * 从 predicates.getPulledUpPredicates() 获取所有可以上提到 HyperGraph 的谓词
     * 例如：{a = b, b = c, id = 5, score > 10, (x = 1 OR y = 2)}
     * 
     * 【步骤2：Shuttle 到基表列空间】
     * 使用 ExpressionUtils.shuttleExpressionWithLineage() 将谓词中的表达式回溯到基表列空间
     * 目的：统一表达式到基表列空间，便于后续匹配
     * 例如：
     *   - 如果 a#5 是别名，对应基表列 a#0，则 a#5 = b#6 → a#0 = b#0
     *   - 如果 id#10 是中间列，对应基表列 id#0，则 id#10 = 5 → id#0 = 5
     * 
     * 【步骤3：合并为 AND 表达式】
     * 使用 ExpressionUtils.and() 将所有 shuttled 表达式合并为一个 AND 表达式
     * 例如：AND(a#0 = b#0, b#0 = c#0, id#0 = 5, score#0 > 10, OR(x#0 = 1, y#0 = 2))
     * 
     * 【步骤4：分割谓词】
     * 使用 Predicates.splitPredicates() 将合并后的表达式分割为三类：
     * - 等值谓词（Equal Predicate）：列 = 列，如 a#0 = b#0, b#0 = c#0
     * - 范围谓词（Range Predicate）：列 op 字面量，如 id#0 = 5, score#0 > 10
     * - 残差谓词（Residual Predicate）：其他复杂表达式，如 OR(x#0 = 1, y#0 = 2)
     * 
     * 【步骤5：构建等价类】
     * 遍历等值谓词（splitPredicate.getEqualPredicateMap()），提取列之间的等值关系：
     * - a#0 = b#0 → 调用 equivalenceClass.addEquivalenceClass(a#0, b#0)
     * - b#0 = c#0 → 调用 equivalenceClass.addEquivalenceClass(b#0, c#0)
     * 
     * EquivalenceClass.addEquivalenceClass() 会自动合并等价类（传递性）：
     * - 第一次 addEquivalenceClass(a#0, b#0)：创建等价类 [a#0, b#0]
     * - 第二次 addEquivalenceClass(b#0, c#0)：发现 b#0 已存在，合并为 [a#0, b#0, c#0]
     * 
     * 最终结果：
     * EquivalenceClass {
     *   equivalenceSlotMap = {
     *     a#0 -> [a#0, b#0, c#0],
     *     b#0 -> [a#0, b#0, c#0],
     *     c#0 -> [a#0, b#0, c#0]
     *   }
     * }
     * 
     * ==================== 返回值 ====================
     * 
     * @param predicates 原始谓词集合（包含可上提和不可上提的谓词）
     * @param originalPlan 原始计划（用于 shuttle 表达式到基表列空间）
     * @return Pair<SplitPredicate, EquivalenceClass>
     *         - SplitPredicate: 分割后的谓词（等值/范围/残差）
     *         - EquivalenceClass: 等价类（列之间的等值关系）
     * 
     * ==================== 使用场景 ====================
     * 
     * 在 StructInfo.getEquivalenceClass() 和 StructInfo.getSplitPredicate() 中调用，
     * 用于物化视图重写时的谓词匹配和补偿：
     * - SplitPredicate 用于比较查询和MV的谓词差异
     * - EquivalenceClass 用于等值谓词补偿（compensateEquivalence）
     */
    private static Pair<SplitPredicate, EquivalenceClass> predicatesDerive(Predicates predicates, Plan originalPlan) {
        // 步骤1-2：收集可上提的谓词并 shuttle 到基表列空间
        List<Expression> shuttledExpression = ExpressionUtils.shuttleExpressionWithLineage(
                        new ArrayList<>(predicates.getPulledUpPredicates()), originalPlan, new BitSet()).stream()
                .map(Expression.class::cast)
                .collect(Collectors.toList());
        
        // 步骤3-4：合并为 AND 表达式并分割为等值/范围/残差三类
        SplitPredicate splitPredicate = Predicates.splitPredicates(ExpressionUtils.and(shuttledExpression));
        
        // 步骤5：构建等价类（从等值谓词中提取列之间的等值关系）
        EquivalenceClass equivalenceClass = new EquivalenceClass();
        for (Expression expression : splitPredicate.getEqualPredicateMap().keySet()) {
            // 跳过字面量（等值谓词应该是列 = 列的形式）
            if (expression instanceof Literal) {
                continue;
            }
            // 提取等值关系：列 = 列 → 添加到等价类
            if (expression instanceof EqualTo) {
                EqualTo equalTo = (EqualTo) expression;
                equivalenceClass.addEquivalenceClass(
                        (SlotReference) equalTo.getArguments().get(0),
                        (SlotReference) equalTo.getArguments().get(1));
            }
        }
        return Pair.of(splitPredicate, equivalenceClass);
    }

    /**
     * Build Struct info from plan.
     * Maybe return multi structInfo when original plan already be rewritten by mv
     */
    public static StructInfo of(Plan originalPlan, CascadesContext cascadesContext) {
        return of(originalPlan, originalPlan, cascadesContext);
    }

    /**
     * 从计划中构建 StructInfo。
     * 这是 StructInfo 的静态工厂方法，用于分析计划结构并提取物化视图重写所需的信息。
     * 
     * @param derivedPlan 派生计划（可能已经经过优化，移除了不必要的节点）
     * @param originalPlan 原始计划（逻辑输出是正确的），如果为 null 则使用 derivedPlan
     * @param cascadesContext 优化器上下文
     * @return 构建的 StructInfo 对象
     */
    public static StructInfo of(Plan derivedPlan, Plan originalPlan, CascadesContext cascadesContext) {
        // 如果 originalPlan 为 null，使用 derivedPlan 作为 originalPlan
        if (originalPlan == null) {
            originalPlan = derivedPlan;
        }
        
        // 步骤1：分割计划树
        // 将计划按照包含多子节点的边界（如 Join）分割成 topPlan 和 bottomPlan
        // bottomPlan 是包含表扫描和 Join 的部分（用于构建 HyperGraph）
        // topPlan 是包含聚合、排序等操作的部分（用于收集谓词）
        LinkedHashSet<Class<? extends Plan>> set = Sets.newLinkedHashSet();
        set.add(LogicalJoin.class);  // 以 Join 作为分割边界
        PlanSplitContext planSplitContext = new PlanSplitContext(set);
        derivedPlan.accept(PLAN_SPLITTER, planSplitContext);
        Plan topPlan = planSplitContext.getTopPlan();
        Plan bottomPlan = planSplitContext.getBottomPlan();
        
        // 步骤2：构建 HyperGraph
        // HyperGraph 表示计划中的表连接关系，用于后续的表匹配和连接顺序优化
        HyperGraph hyperGraph = HyperGraph.builderForMv(planSplitContext.getBottomPlan()).build();
        
        // 步骤3：获取原始计划的 ObjectId（用于标识计划）
        ObjectId originalPlanId = originalPlan.getGroupExpression()
                .map(GroupExpression::getId).orElseGet(() -> new ObjectId(-1));
        
        // 步骤4：从 HyperGraph 中收集结构信息
        // 收集表关系、表达式映射、谓词映射等信息
        List<CatalogRelation> relationList = new ArrayList<>();
        Map<RelationId, StructInfoNode> relationIdStructInfoNodeMap = new LinkedHashMap<>();
        Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                shuttledHashConjunctsToConjunctsMap = new LinkedHashMap<>();
        BitSet tableBitSet = new BitSet();
        Map<ExpressionPosition, Map<Expression, Expression>> expressionToShuttledExpressionToMap = new HashMap<>();
        boolean valid = collectStructInfoFromGraph(hyperGraph, topPlan, shuttledHashConjunctsToConjunctsMap,
                expressionToShuttledExpressionToMap,
                relationList,
                relationIdStructInfoNodeMap,
                tableBitSet,
                cascadesContext);
        
        // 步骤5：验证有效性
        // 检查 HyperGraph 中所有节点都有有效的表达式
        valid = valid
                && hyperGraph.getNodes().stream().allMatch(n -> ((StructInfoNode) n).getExpressions() != null);
        
        // 步骤6：检查是否包含不支持的表操作符
        // 如果关系列表中包含表操作符（如 sample、index、table 等），则标记为无效
        // 例如：查询中包含采样（sample）操作，不支持物化视图重写
        boolean invalid = relationList.stream().anyMatch(relation ->
                ((AbstractPlan) relation).accept(TableQueryOperatorChecker.INSTANCE, null));
        valid = valid && !invalid;
        
        // 步骤7：收集 topPlan 中的谓词（不在 HyperGraph 中的谓词）
        // 这些谓词是从 topPlan 中提取的，用于后续的谓词匹配和补偿
        PredicateCollectorContext predicateCollectorContext = new PredicateCollectorContext();
        topPlan.accept(PREDICATE_COLLECTOR, predicateCollectorContext);
        Predicates predicates = Predicates.of(predicateCollectorContext.getCouldPullUpPredicates(),
                predicateCollectorContext.getCouldNotPullUpPredicates());
        
        // 步骤8：Shuttle 原始计划的输出表达式
        // 将原始计划的输出表达式 shuttle 到基表列空间，用于后续的表达式匹配和重写
        // 注意：必须使用 originalPlan.getOutput() 而不是 derivedPlan.getOutput()，以确保输出顺序正确
        List<? extends Expression> planOutputShuttledExpressions =
                ExpressionUtils.shuttleExpressionWithLineage(originalPlan.getOutput(), originalPlan, new BitSet());
        
        // 步骤9：构建并返回 StructInfo 对象
        return new StructInfo(originalPlan, originalPlanId, hyperGraph, valid, topPlan, bottomPlan,
                relationList, relationIdStructInfoNodeMap, predicates, planSplitContext.getGroupingId(),
                shuttledHashConjunctsToConjunctsMap, expressionToShuttledExpressionToMap,
                tableBitSet, null, null, planOutputShuttledExpressions);
    }

    public List<CatalogRelation> getRelations() {
        return relations;
    }

    public Predicates getPredicates() {
        return predicates;
    }

    public Optional<SlotReference> getGroupingId() {
        return groupingId;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public HyperGraph getHyperGraph() {
        return hyperGraph;
    }

    /**
     * lazy init for performance
     */
    public SplitPredicate getSplitPredicate() {
        if (this.splitPredicate == null && this.predicates != null) {
            Pair<SplitPredicate, EquivalenceClass> derivedPredicates = predicatesDerive(this.predicates, topPlan);
            this.splitPredicate = derivedPredicates.key();
            this.equivalenceClass = derivedPredicates.value();
        }
        return this.splitPredicate;
    }

    /**
     * lazy init for performance
     * 
     * ==================== 调用链 ====================
     * 
     * 【入口：物化视图重写流程】
     * AbstractMaterializedViewRule.predicatesCompensate()
     *   └─> Predicates.compensateEquivalence()
     *       ├─> queryStructInfo.getEquivalenceClass()  ← 这里（查询等价类）
     *       └─> viewStructInfo.getEquivalenceClass()  ← 这里（MV等价类）
     * 
     * 【等价类生成流程】
     * StructInfo.getEquivalenceClass()
     *   └─> StructInfo.predicatesDerive(predicates, topPlan)
     *       ├─> predicates.getPulledUpPredicates()           // 获取可上提的谓词
     *       ├─> ExpressionUtils.shuttleExpressionWithLineage()  // Shuttle到基表列空间
     *       ├─> ExpressionUtils.and(shuttledExpression)       // 合并为AND表达式
     *       ├─> Predicates.splitPredicates()                  // 分割为等值/范围/残差
     *       │   └─> PredicatesSplitter.getSplitPredicate()
     *       ├─> new EquivalenceClass()                       // 创建空等价类
     *       └─> EquivalenceClass.addEquivalenceClass()       // 添加等值关系（循环）
     *           └─> 自动合并等价类（传递性）
     * 
     * 【使用场景】
     * - 物化视图重写时，比较查询和MV的等价类差异
     * - 如果查询有 a=b，MV有 b=c，通过等价类可以推导出需要补偿 a=c
     * 
     * @return 等价类对象，如果 predicates 为 null 则返回 null
     */
    public EquivalenceClass getEquivalenceClass() {
        if (this.equivalenceClass == null && this.predicates != null) {
            Pair<SplitPredicate, EquivalenceClass> derivedPredicates = predicatesDerive(this.predicates, topPlan);
            this.splitPredicate = derivedPredicates.key();
            this.equivalenceClass = derivedPredicates.value();
        }
        return this.equivalenceClass;
    }

    public boolean isValid() {
        return valid;
    }

    public Plan getTopPlan() {
        return topPlan;
    }

    public Plan getBottomPlan() {
        return bottomPlan;
    }

    public Map<RelationId, StructInfoNode> getRelationIdStructInfoNodeMap() {
        return relationIdStructInfoNodeMap;
    }

    public Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
            getShuttledExpressionsToExpressionsMap() {
        return shuttledExpressionsToExpressionsMap;
    }

    public Map<ExpressionPosition, Map<Expression, Expression>> getExpressionToShuttledExpressionToMap() {
        return expressionToShuttledExpressionToMap;
    }

    private static void putShuttledExpressionToExpressionsMap(
            Map<ExpressionPosition, Multimap<Expression, Pair<Expression, HyperElement>>>
                    shuttledExpressionsToExpressionsMap,
            Map<ExpressionPosition, Map<Expression, Expression>> expressionPositionToExpressionToMap,
            ExpressionPosition expressionPosition,
            Expression shuttledExpression, Expression originalExpression, HyperElement valueBelongedElement) {
        Multimap<Expression, Pair<Expression, HyperElement>> shuttledExpressionToExpressionMap =
                shuttledExpressionsToExpressionsMap.get(expressionPosition);
        if (shuttledExpressionToExpressionMap == null) {
            shuttledExpressionToExpressionMap = HashMultimap.create();
            shuttledExpressionsToExpressionsMap.put(expressionPosition, shuttledExpressionToExpressionMap);
        }
        shuttledExpressionToExpressionMap.put(shuttledExpression, Pair.of(originalExpression, valueBelongedElement));

        Map<Expression, Expression> originalExprToShuttledExprMap =
                expressionPositionToExpressionToMap.get(expressionPosition);
        if (originalExprToShuttledExprMap == null) {
            originalExprToShuttledExprMap = new HashMap<>();
            expressionPositionToExpressionToMap.put(expressionPosition, originalExprToShuttledExprMap);
        }
        originalExprToShuttledExprMap.put(originalExpression, shuttledExpression);
    }

    public List<? extends Expression> getExpressions() {
        return topPlan instanceof LogicalProject
                ? ((LogicalProject<Plan>) topPlan).getProjects() : topPlan.getOutput();
    }

    public ObjectId getOriginalPlanId() {
        return originalPlanId;
    }

    public BitSet getTableBitSet() {
        return tableBitSet;
    }

    public List<? extends Expression> getPlanOutputShuttledExpressions() {
        return planOutputShuttledExpressions;
    }

    /**
     * Judge the source graph logical is whether the same as target
     * For inner join should judge only the join tables,
     * for other join type should also judge the join direction, it's input filter that can not be pulled up etc.
     */
    public static ComparisonResult isGraphLogicalEquals(StructInfo queryStructInfo, StructInfo viewStructInfo,
            LogicalCompatibilityContext compatibilityContext) {
        return HyperGraphComparator
                .isLogicCompatible(queryStructInfo.hyperGraph, viewStructInfo.hyperGraph, compatibilityContext);
    }

    @Override
    public String toString() {
        return "StructInfo{ originalPlanId = " + originalPlanId + ", relations = " + relations + '}';
    }

    private static class RelationCollector extends DefaultPlanVisitor<Void, List<CatalogRelation>> {
        @Override
        public Void visit(Plan plan, List<CatalogRelation> collectedRelations) {
            if (plan instanceof CatalogRelation) {
                collectedRelations.add((CatalogRelation) plan);
            }
            return super.visit(plan, collectedRelations);
        }
    }

    private static class PredicateCollector extends DefaultPlanVisitor<Void, PredicateCollectorContext> {
        @Override
        public Void visit(Plan plan, PredicateCollectorContext collectorContext) {
            // Just collect the filter in top plan, if meet other node except the following node, return
            if (!(plan instanceof LogicalSink)
                    && !(plan instanceof LogicalProject)
                    && !(plan instanceof LogicalFilter)
                    && !(plan instanceof LogicalAggregate)
                    && !(plan instanceof LogicalWindow)
                    && !(plan instanceof LogicalSort)
                    && !(plan instanceof LogicalRepeat)
                    && !(plan instanceof LogicalLimit)
                    && !(plan instanceof LogicalTopN)) {
                return null;
            }
            return super.visit(plan, collectorContext);
        }

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, PredicateCollectorContext context) {
            if (context.getWindowCommonPartitionKeys().isEmpty()) {
                context.getCouldPullUpPredicates().addAll(filter.getConjuncts());
            } else {
                // if the filter contains the partition key of window, it can be pulled up
                for (Expression conjunct : filter.getConjuncts()) {
                    if (PushDownFilterThroughWindow.canPushDown(conjunct, context.getWindowCommonPartitionKeys())) {
                        context.getCouldPullUpPredicates().add(conjunct);
                    } else {
                        context.getCouldNotPullUpPredicates().add(conjunct);
                    }
                }
            }
            return super.visit(filter, context);
        }

        @Override
        public Void visitLogicalWindow(LogicalWindow<? extends Plan> window, PredicateCollectorContext context) {
            Set<SlotReference> commonPartitionKeys = window.getCommonPartitionKeyFromWindowExpressions();
            context.getWindowCommonPartitionKeys().addAll(commonPartitionKeys);
            return super.visit(window, context);
        }
    }

    /**
     * PredicateCollectorContext
     */
    public static class PredicateCollectorContext {
        private Set<Expression> couldPullUpPredicates = new HashSet<>();
        private Set<Expression> couldNotPullUpPredicates = new HashSet<>();
        private Set<SlotReference> windowCommonPartitionKeys = new HashSet<>();

        public Set<Expression> getCouldPullUpPredicates() {
            return couldPullUpPredicates;
        }

        public Set<Expression> getCouldNotPullUpPredicates() {
            return couldNotPullUpPredicates;
        }

        public Set<SlotReference> getWindowCommonPartitionKeys() {
            return windowCommonPartitionKeys;
        }
    }

    /**
     * Split the plan into bottom and up, the boundary is given by context,
     * the bottom contains the boundary, and top plan doesn't contain the boundary.
     */
    public static class PlanSplitter extends DefaultPlanVisitor<Void, PlanSplitContext> {
        @Override
        public Void visit(Plan plan, PlanSplitContext context) {
            if (context.getTopPlan() == null) {
                context.setTopPlan(plan);
            }
            if (plan instanceof LogicalRepeat) {
                context.setGroupingId(((LogicalRepeat<?>) plan).getGroupingId());
            }
            if (plan.children().isEmpty() && context.getBottomPlan() == null) {
                context.setBottomPlan(plan);
                return null;
            }
            if (context.isBoundary(plan)) {
                context.setBottomPlan(plan);
                return null;
            }
            return super.visit(plan, context);
        }
    }

    /**
     * Judge if source contains all target
     */
    public static boolean containsAll(BitSet source, BitSet target) {
        if (source.size() < target.size()) {
            return false;
        }
        for (int i = target.nextSetBit(0); i >= 0; i = target.nextSetBit(i + 1)) {
            boolean contains = source.get(i);
            if (!contains) {
                return false;
            }
        }
        return true;
    }

    /**
     * Plan split context, this hold bottom and top plan, and boundary plan setting
     */
    public static class PlanSplitContext {
        private Plan bottomPlan;
        private Plan topPlan;
        private Set<Class<? extends Plan>> boundaryPlanClazzSet;
        private Optional<SlotReference> groupingId = Optional.empty();

        public PlanSplitContext(Set<Class<? extends Plan>> boundaryPlanClazzSet) {
            this.boundaryPlanClazzSet = boundaryPlanClazzSet;
        }

        public Plan getBottomPlan() {
            return bottomPlan;
        }

        public void setBottomPlan(Plan bottomPlan) {
            this.bottomPlan = bottomPlan;
        }

        public Plan getTopPlan() {
            return topPlan;
        }

        public void setTopPlan(Plan topPlan) {
            this.topPlan = topPlan;
        }

        public Optional<SlotReference> getGroupingId() {
            return groupingId;
        }

        public void setGroupingId(Optional<SlotReference> groupingId) {
            this.groupingId = groupingId;
        }

        /**
         * isBoundary
         */
        public boolean isBoundary(Plan plan) {
            for (Class<? extends Plan> boundaryPlanClazz : boundaryPlanClazzSet) {
                if (boundaryPlanClazz.isAssignableFrom(plan.getClass())) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * The context for plan check context, make sure that the plan in query and mv is valid or not
     */
    public static class PlanCheckContext {
        // Record if aggregate is above join or not
        private boolean containsTopAggregate = false;
        private int topAggregateNum = 0;
        // This indicates whether the operators above the join contain any window operator.
        private boolean containsTopWindow = false;
        // This records the number of window operators above the join.
        private int topWindowNum = 0;
        private boolean alreadyMeetJoin = false;
        // Records whether an Aggregate operator has been meet when check window operator
        private boolean alreadyMeetAggregate = false;
        // Indicates if a window operator is under an Aggregate operator, because the window operator under
        // aggregate is not supported now.
        private boolean windowUnderAggregate = false;
        private final Set<JoinType> supportJoinTypes;
        // This field indicates whether a node that does not allow limit rewriting is encountered in the plan node.
        // Currently, limit is only allowed to traverse through Project and Filter nodes;
        // rewriting is not supported in any other stages.
        private boolean alreadyMeetLimitForbiddenNode = false;
        // This indicates whether the operators above the join contain a limit operator.
        private boolean containsTopLimit = false;
        // This records the number of limit operators above the join.
        private int topLimitNum = 0;
        // This indicates whether the operators above the join contain a topN operator.
        private boolean containsTopTopN = false;
        // This records the number of topN operators above the join.
        private int topTopNNum = 0;

        public PlanCheckContext(Set<JoinType> supportJoinTypes) {
            this.supportJoinTypes = supportJoinTypes;
        }

        public boolean isContainsTopAggregate() {
            return containsTopAggregate;
        }

        public void setContainsTopAggregate(boolean containsTopAggregate) {
            this.containsTopAggregate = containsTopAggregate;
        }

        public int getTopLimitNum() {
            return topLimitNum;
        }

        public void plusTopLimitNum() {
            this.topLimitNum += 1;
        }

        public boolean isContainsTopLimit() {
            return containsTopLimit;
        }

        public void setContainsTopLimit(boolean containsTopLimit) {
            this.containsTopLimit = containsTopLimit;
        }

        public int getTopTopNNum() {
            return topTopNNum;
        }

        public void plusTopTopNNum() {
            this.topTopNNum += 1;
        }

        public boolean isContainsTopTopN() {
            return containsTopTopN;
        }

        public void setContainsTopTopN(boolean containsTopTopN) {
            this.containsTopTopN = containsTopTopN;
        }

        public boolean isContainsTopWindow() {
            return containsTopWindow;
        }

        public void setContainsTopWindow(boolean containsTopWindow) {
            this.containsTopWindow = containsTopWindow;
        }

        public int getTopWindowNum() {
            return topWindowNum;
        }

        public void plusTopWindowNum() {
            this.topWindowNum += 1;
        }

        public boolean isAlreadyMeetJoin() {
            return alreadyMeetJoin;
        }

        public void setAlreadyMeetJoin(boolean alreadyMeetJoin) {
            this.alreadyMeetJoin = alreadyMeetJoin;
        }

        public Set<JoinType> getSupportJoinTypes() {
            return supportJoinTypes;
        }

        public int getTopAggregateNum() {
            return topAggregateNum;
        }

        public void plusTopAggregateNum() {
            this.topAggregateNum += 1;
        }

        public boolean isAlreadyMeetAggregate() {
            return alreadyMeetAggregate;
        }

        public void setAlreadyMeetAggregate(boolean alreadyMeetAggregate) {
            this.alreadyMeetAggregate = alreadyMeetAggregate;
        }

        public boolean isWindowUnderAggregate() {
            return windowUnderAggregate;
        }

        public void setWindowUnderAggregate(boolean windowUnderAggregate) {
            this.windowUnderAggregate = windowUnderAggregate;
        }

        public boolean isAlreadyMeetLimitForbiddenNode() {
            return alreadyMeetLimitForbiddenNode;
        }

        public void setAlreadyMeetLimitForbiddenNode(boolean alreadyMeetLimitForbiddenNode) {
            this.alreadyMeetLimitForbiddenNode = alreadyMeetLimitForbiddenNode;
        }

        public static PlanCheckContext of(Set<JoinType> supportJoinTypes) {
            return new PlanCheckContext(supportJoinTypes);
        }
    }

    /**
     * PlanPatternChecker, this is used to check the plan pattern is valid or not
     */
    public static class PlanPatternChecker extends DefaultPlanVisitor<Boolean, PlanCheckContext> {
        @Override
        public Boolean visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                PlanCheckContext checkContext) {
            checkContext.setAlreadyMeetJoin(true);
            if (!checkContext.getSupportJoinTypes().contains(join.getJoinType())) {
                return false;
            }
            return visit(join, checkContext);
        }

        @Override
        public Boolean visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                PlanCheckContext checkContext) {
            if (!checkContext.isAlreadyMeetJoin()) {
                checkContext.setContainsTopAggregate(true);
                checkContext.setAlreadyMeetAggregate(true);
                checkContext.plusTopAggregateNum();
            }
            return visit(aggregate, checkContext);
        }

        @Override
        public Boolean visitLogicalWindow(LogicalWindow<? extends Plan> window, PlanCheckContext checkContext) {
            if (!checkContext.isAlreadyMeetJoin()) {
                if (checkContext.isAlreadyMeetAggregate()) {
                    checkContext.setWindowUnderAggregate(true);
                    return false;
                }
                checkContext.setContainsTopWindow(true);
                checkContext.plusTopWindowNum();
            }
            return visit(window, checkContext);
        }

        @Override
        public Boolean visitGroupPlan(GroupPlan groupPlan, PlanCheckContext checkContext) {
            return groupPlan.getGroup().getLogicalExpressions().stream()
                    .anyMatch(logicalExpression -> logicalExpression.getPlan().accept(this, checkContext));
        }

        @Override
        public Boolean visitLogicalLimit(LogicalLimit<? extends Plan> limit, PlanCheckContext context) {
            if (context.isAlreadyMeetLimitForbiddenNode() && limit.getPhase() == LimitPhase.GLOBAL) {
                return false;
            }
            if (limit.getPhase() == LimitPhase.GLOBAL) {
                context.setContainsTopLimit(true);
                context.plusTopLimitNum();
            }
            return visit(limit, context);
        }

        @Override
        public Boolean visitLogicalTopN(LogicalTopN<? extends Plan> topN, PlanCheckContext context) {
            if (context.isAlreadyMeetLimitForbiddenNode()) {
                return false;
            }
            context.setContainsTopTopN(true);
            context.plusTopTopNNum();
            return visit(topN, context);
        }

        @Override
        public Boolean visit(Plan plan, PlanCheckContext checkContext) {
            if (!(plan instanceof Sink) && !(plan instanceof LogicalProject) && !(plan instanceof LogicalFilter)) {
                checkContext.setAlreadyMeetLimitForbiddenNode(true);
            }
            if (plan instanceof Filter
                    || plan instanceof Project
                    || plan instanceof CatalogRelation
                    || plan instanceof Join
                    || plan instanceof LogicalSort
                    || plan instanceof LogicalAggregate
                    || plan instanceof GroupPlan
                    || plan instanceof LogicalWindow
                    || plan instanceof LogicalRepeat
                    || plan instanceof LogicalLimit
                    || plan instanceof LogicalTopN) {
                return doVisit(plan, checkContext);
            }
            return false;
        }

        private Boolean doVisit(Plan plan, PlanCheckContext checkContext) {
            for (Plan child : plan.children()) {
                boolean valid = child.accept(this, checkContext);
                if (!valid) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * ScanPlanPatternChecker, this is used to check the plan pattern is valid or not
     */
    public static class ScanPlanPatternChecker extends DefaultPlanVisitor<Boolean, PlanCheckContext> {

        @Override
        public Boolean visitGroupPlan(GroupPlan groupPlan, PlanCheckContext checkContext) {
            return groupPlan.getGroup().getLogicalExpressions().stream()
                    .anyMatch(logicalExpression -> logicalExpression.getPlan().accept(this, checkContext));
        }

        @Override
        public Boolean visitLogicalWindow(LogicalWindow<? extends Plan> window, PlanCheckContext checkContext) {
            checkContext.setContainsTopWindow(true);
            checkContext.plusTopWindowNum();
            return visit(window, checkContext);
        }

        @Override
        public Boolean visitLogicalLimit(LogicalLimit<? extends Plan> limit, PlanCheckContext context) {
            if (context.isAlreadyMeetLimitForbiddenNode() && limit.getPhase() == LimitPhase.GLOBAL) {
                return false;
            }
            if (limit.getPhase() == LimitPhase.GLOBAL) {
                context.setContainsTopLimit(true);
                context.plusTopLimitNum();
            }
            return visit(limit, context);
        }

        @Override
        public Boolean visitLogicalTopN(LogicalTopN<? extends Plan> topN, PlanCheckContext context) {
            if (context.isAlreadyMeetLimitForbiddenNode()) {
                return false;
            }
            context.setContainsTopTopN(true);
            context.plusTopTopNNum();
            return visit(topN, context);
        }

        @Override
        public Boolean visit(Plan plan, PlanCheckContext checkContext) {
            if (plan instanceof Filter
                    || plan instanceof Project
                    || plan instanceof CatalogRelation
                    || plan instanceof GroupPlan
                    || plan instanceof LogicalRepeat
                    || plan instanceof LogicalWindow
                    || plan instanceof LogicalLimit
                    || plan instanceof LogicalTopN) {
                return doVisit(plan, checkContext);
            }
            return false;
        }

        private Boolean doVisit(Plan plan, PlanCheckContext checkContext) {
            for (Plan child : plan.children()) {
                boolean valid = child.accept(this, checkContext);
                if (!valid) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Add or remove partition on base table and mv when materialized view scan contains invalid partitions
     */
    public static class PartitionRemover extends DefaultPlanRewriter<Map<BaseTableInfo, Set<String>>> {
        @Override
        public Plan visitLogicalOlapScan(LogicalOlapScan olapScan,
                Map<BaseTableInfo, Set<String>> context) {
            // todo Support other partition table
            BaseTableInfo tableInfo = new BaseTableInfo(olapScan.getTable());
            if (!context.containsKey(tableInfo)) {
                return olapScan;
            }
            Set<String> targetPartitionNameSet = context.get(tableInfo);
            List<Long> selectedPartitionIds = new ArrayList<>(olapScan.getSelectedPartitionIds());
            // need remove partition
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(partitionId -> !targetPartitionNameSet.contains(
                            olapScan.getTable().getPartition(partitionId).getName()))
                    .collect(Collectors.toList());
            return olapScan.withSelectedPartitionIds(selectedPartitionIds);
        }
    }

    /**
     * Add filter on table scan according to table filter map
     *
     * @return Pair(Plan, Boolean) first is the added filter plan, value is the identifier that represent whether
     *         need to add filter.
     *         return null if add filter fail.
     */
    public static Pair<Plan, Boolean> addFilterOnTableScan(Plan queryPlan,
            Map<BaseColInfo, Set<String>> partitionOnBaseTableMap, CascadesContext parentCascadesContext) {
        // Firstly, construct filter form invalid partition, this filter should be added on origin plan
        PredicateAddContext predicateAddContext = new PredicateAddContext(null, partitionOnBaseTableMap);
        Plan queryPlanWithUnionFilter = queryPlan.accept(new PredicateAdder(), predicateAddContext);
        if (!predicateAddContext.isHandleSuccess()) {
            return null;
        }
        if (!predicateAddContext.isNeedAddFilter()) {
            return Pair.of(queryPlan, false);
        }
        // Deep copy the plan to avoid the plan output is the same with the later union output, this may cause
        // exec by mistake
        queryPlanWithUnionFilter = new LogicalPlanDeepCopier().deepCopy(
                (LogicalPlan) queryPlanWithUnionFilter, new DeepCopierContext());
        // rbo rewrite after adding filter on origin plan
        return Pair.of(MaterializedViewUtils.rewriteByRules(parentCascadesContext, context -> {
            Rewriter.getWholeTreeRewriter(context).execute();
            return context.getRewritePlan();
        }, queryPlanWithUnionFilter, queryPlan, false), true);
    }

    /**
     * Check the tempRewrittenPlan is valid, should only contain project, scan
     */
    public static boolean checkLimitTmpRewrittenPlanIsValid(Plan tempRewrittenPlan) {
        if (tempRewrittenPlan == null) {
            return false;
        }
        return tempRewrittenPlan.accept(new DefaultPlanVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(Plan plan, Void context) {
                if (plan instanceof LogicalProject || plan instanceof CatalogRelation) {
                    boolean isValid;
                    for (Plan child : plan.children()) {
                        isValid = child.accept(this, context);
                        if (!isValid) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }
        }, null);
    }

    /**
     * Expressions may appear in three place in hype graph, this identifies the position where
     * expression appear in hyper graph
     */
    public static enum ExpressionPosition {
        JOIN_EDGE,
        NODE_COULD_MOVE,
        NODE_COULD_NOT_MOVE,
        FILTER_EDGE
    }

    /**
     * Check the tempRewrittenPlan is valid, should only contain project, scan or filter
     */
    public static boolean checkWindowTmpRewrittenPlanIsValid(Plan tempRewrittenPlan) {
        if (tempRewrittenPlan == null) {
            return false;
        }
        return tempRewrittenPlan.accept(new DefaultPlanVisitor<Boolean, Void>() {
            @Override
            public Boolean visit(Plan plan, Void context) {
                if (plan instanceof LogicalProject || plan instanceof CatalogRelation
                        || plan instanceof LogicalFilter) {
                    boolean isValid;
                    for (Plan child : plan.children()) {
                        isValid = child.accept(this, context);
                        if (!isValid) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }
        }, null);
    }
}
