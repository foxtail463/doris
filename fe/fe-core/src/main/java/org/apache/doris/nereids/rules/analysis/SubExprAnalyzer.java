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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Use the visitor to iterate sub expression.
 */
class SubExprAnalyzer<T> extends DefaultExpressionRewriter<T> {
    private final Scope scope;
    private final CascadesContext cascadesContext;

    public SubExprAnalyzer(Scope scope, CascadesContext cascadesContext) {
        this.scope = scope;
        this.cascadesContext = cascadesContext;
    }

    @Override
    public Expression visitNot(Not not, T context) {
        Expression child = not.child();
        if (child instanceof Exists) {
            return visitExistsSubquery(
                    new Exists(((Exists) child).getQueryPlan(), true), context);
        } else if (child instanceof InSubquery) {
            return visitInSubquery(new InSubquery(((InSubquery) child).getCompareExpr(),
                    ((InSubquery) child).getQueryPlan(), true), context);
        }
        return visit(not, context);
    }

    @Override
    public Expression visitExistsSubquery(Exists exists, T context) {
        if (!exists.getCorrelateSlots().isEmpty()) {
            return exists;
        }
        LogicalPlan queryPlan = exists.getQueryPlan();
        // distinct is useless, remove it
        if (queryPlan instanceof LogicalProject && ((LogicalProject) queryPlan).isDistinct()) {
            exists = exists.withSubquery(((LogicalProject) queryPlan).withDistinct(false));
        }
        AnalyzedResult analyzedResult = analyzeSubquery(exists);
        if (analyzedResult.rootIsLimitZero()) {
            return BooleanLiteral.of(exists.isNot());
        }
        if (analyzedResult.isCorrelated() && analyzedResult.rootIsLimitWithOffset()) {
            throw new AnalysisException("Unsupported correlated subquery with a LIMIT clause with offset > 0 "
                    + analyzedResult.getLogicalPlan());
        }
        return new Exists(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots(), exists.isNot());
    }

    @Override
    public Expression visitInSubquery(InSubquery expr, T context) {
        if (!expr.getCorrelateSlots().isEmpty()) {
            return expr;
        }
        LogicalPlan queryPlan = expr.getQueryPlan();
        // distinct is useless, remove it
        if (queryPlan instanceof LogicalProject && ((LogicalProject) queryPlan).isDistinct()) {
            expr = expr.withSubquery(((LogicalProject) queryPlan).withDistinct(false));
        }
        AnalyzedResult analyzedResult = analyzeSubquery(expr);

        checkOutputColumn(analyzedResult.getLogicalPlan());
        checkNoCorrelatedSlotsUnderAgg(analyzedResult);
        checkRootIsLimit(analyzedResult);

        return new InSubquery(
                expr.getCompareExpr().accept(this, context),
                analyzedResult.getLogicalPlan(),
                analyzedResult.getCorrelatedSlots(), expr.isNot());
    }

    @Override
    public Expression visitScalarSubquery(ScalarSubquery scalar, T context) {
        if (!scalar.getCorrelateSlots().isEmpty()) {
            return scalar;
        }
        // 对标量子查询进行独立分析：产出规范化后的子查询逻辑计划与相关列
        AnalyzedResult analyzedResult = analyzeSubquery(scalar);
        boolean isCorrelated = analyzedResult.isCorrelated();
        LogicalPlan analyzedSubqueryPlan = analyzedResult.logicalPlan;
        // 标量子查询必须只返回一列
        checkOutputColumn(analyzedSubqueryPlan);
        // 用 limitOneIsEliminated 标记是否存在 "LIMIT 1"（且偏移为 0）
        // 若存在，该约束可保证“至多一行”，这里将其消除并把标记传递给后续规则
        // 后续在构造 LogicalApply 时可据此避免再插入行数断言
        boolean limitOneIsEliminated = false;
        if (isCorrelated) {
            // 相关子查询上的 LIMIT 仅支持 offset=0 且 limit=1，其他场景直接报错
            if (analyzedSubqueryPlan instanceof LogicalLimit) {
                LogicalLimit limit = (LogicalLimit) analyzedSubqueryPlan;
                if (limit.getOffset() == 0 && limit.getLimit() == 1) {
                    // skip useless limit node
                    analyzedResult = new AnalyzedResult((LogicalPlan) analyzedSubqueryPlan.child(0),
                            analyzedResult.correlatedSlots);
                    limitOneIsEliminated = true;
                } else {
                    throw new AnalysisException("limit is not supported in correlated subquery "
                            + analyzedResult.getLogicalPlan());
                }
            }
            // 顶层 Sort 对标量语义无影响，可直接消除
            if (analyzedSubqueryPlan instanceof LogicalSort) {
                // skip useless sort node
                analyzedResult = new AnalyzedResult((LogicalPlan) analyzedSubqueryPlan.child(0),
                        analyzedResult.correlatedSlots);
            }
            // 校验：相关列不能出现在非法位置（如聚合、Join、Window 等敏感节点之前）
            CorrelatedSlotsValidator validator =
                    new CorrelatedSlotsValidator(ImmutableSet.copyOf(analyzedResult.correlatedSlots));
            List<PlanNodeCorrelatedInfo> nodeInfoList = new ArrayList<>(16);
            Set<LogicalAggregate> topAgg = new HashSet<>();
            validateSubquery(analyzedResult.logicalPlan, validator, nodeInfoList, topAgg);
        }

        if (analyzedResult.getLogicalPlan() instanceof LogicalOneRowRelation) {
            LogicalOneRowRelation oneRowRelation = (LogicalOneRowRelation) analyzedResult.getLogicalPlan();
            if (oneRowRelation.getProjects().size() == 1 && oneRowRelation.getProjects().get(0) instanceof Alias) {
                // if scalar subquery is like select '2024-02-02 00:00:00'
                // we can just return the constant expr '2024-02-02 00:00:00'
                // 常量标量子查询：直接折叠为常量表达式
                Alias alias = (Alias) oneRowRelation.getProjects().get(0);
                if (alias.isConstant()) {
                    return alias.child();
                }
            }
        } else if (analyzedResult.getLogicalPlan() instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) analyzedResult.getLogicalPlan();
            if (project.child() instanceof LogicalOneRowRelation
                    && project.getProjects().size() == 1
                    && project.getProjects().get(0) instanceof Alias) {
                // if scalar subquery is like select '2024-02-02 00:00:00'
                // we can just return the constant expr '2024-02-02 00:00:00'
                // 仍属于常量折叠场景
                Alias alias = (Alias) project.getProjects().get(0);
                if (alias.isConstant()) {
                    return alias.child();
                }
            } else if (isCorrelated) {
                // 校验：相关列不能泄露到子查询的输出列中
                Set<Slot> correlatedSlots = new HashSet<>(analyzedResult.getCorrelatedSlots());
                if (!Sets.intersection(ExpressionUtils.getInputSlotSet(project.getProjects()),
                        correlatedSlots).isEmpty()) {
                    throw new AnalysisException(
                            "outer query's column is not supported in subquery's output "
                                    + analyzedResult.getLogicalPlan());
                }
            }
        }

        // 返回新的 ScalarSubquery，携带分析后的子查询计划、相关槽位，以及是否消除了 LIMIT 1 的标记
        return new ScalarSubquery(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots(),
                limitOneIsEliminated);
    }

    private void checkOutputColumn(LogicalPlan plan) {
        if (plan.getOutput().size() != 1) {
            throw new AnalysisException("Multiple columns returned by subquery are not yet supported. Found "
                    + plan.getOutput().size());
        }
    }

    private void checkNoCorrelatedSlotsUnderAgg(AnalyzedResult analyzedResult) {
        if (analyzedResult.hasCorrelatedSlotsUnderAgg()) {
            throw new AnalysisException(
                    "Unsupported correlated subquery with grouping and/or aggregation "
                            + analyzedResult.getLogicalPlan());
        }
    }

    private void checkRootIsLimit(AnalyzedResult analyzedResult) {
        if (!analyzedResult.isCorrelated()) {
            return;
        }
        if (analyzedResult.rootIsLimit()) {
            throw new AnalysisException("Unsupported correlated subquery with a LIMIT clause "
                    + analyzedResult.getLogicalPlan());
        }
    }

    private AnalyzedResult analyzeSubquery(SubqueryExpr expr) {
        if (cascadesContext == null) {
            throw new IllegalStateException("Missing CascadesContext");
        }
        // 为子查询创建独立的 CascadesContext：
        // - 以当前表达式携带的 queryPlan 作为子查询的根计划
        // - 复用外层的 CTE 上下文，保证子查询能解析/引用同一批 CTE 定义
        CascadesContext subqueryContext = CascadesContext.newContextWithCteContext(
                cascadesContext, expr.getQueryPlan(), cascadesContext.getCteContext());
        // 注意：不要直接使用当前 getScope()，这里只需要外层作用域与已知槽位集合，
        // 以便在子查询内识别并收集“相关列”（来自外层查询的列引用），避免引入不必要的上下文状态
        Scope subqueryScope = new Scope(getScope().getOuterScope(),
                getScope().getSlots(), getScope().getAsteriskSlots());
        // 设置子查询的外部作用域，允许在分析阶段做相关性检测与规则校验
        subqueryContext.setOuterScope(subqueryScope);
        // 运行分析器：对该子查询进行名称解析、类型推导、规则重写等，得到规范化的子查询计划
        subqueryContext.newAnalyzer().analyze();
        // 返回规范化后的子查询逻辑计划与“相关列”集合：
        // - 逻辑计划用于后续改写（如 SubqueryToApply）
        // - 相关列用于后续的相关子查询校验与 Join 改写前提判断
        return new AnalyzedResult((LogicalPlan) subqueryContext.getRewritePlan(),
                subqueryScope.getCorrelatedSlots());
    }

    public Scope getScope() {
        return scope;
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    private static class AnalyzedResult {
        private final LogicalPlan logicalPlan;
        private final List<Slot> correlatedSlots;

        public AnalyzedResult(LogicalPlan logicalPlan, Collection<Slot> correlatedSlots) {
            this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan can not be null");
            this.correlatedSlots = correlatedSlots == null ? new ArrayList<>() : ImmutableList.copyOf(correlatedSlots);
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public List<Slot> getCorrelatedSlots() {
            return correlatedSlots;
        }

        public boolean isCorrelated() {
            return !correlatedSlots.isEmpty();
        }

        public boolean hasCorrelatedSlotsUnderAgg() {
            return correlatedSlots.isEmpty() ? false
                    : hasCorrelatedSlotsUnderNode(logicalPlan,
                            ImmutableSet.copyOf(correlatedSlots), LogicalAggregate.class);
        }

        private static <T> boolean hasCorrelatedSlotsUnderNode(Plan rootPlan,
                                                               ImmutableSet<Slot> slots, Class<T> clazz) {
            ArrayDeque<Plan> planQueue = new ArrayDeque<>();
            planQueue.add(rootPlan);
            while (!planQueue.isEmpty()) {
                Plan plan = planQueue.poll();
                if (plan.getClass().equals(clazz)) {
                    if (plan.containsSlots(slots)) {
                        return true;
                    }
                } else {
                    for (Plan child : plan.children()) {
                        planQueue.add(child);
                    }
                }
            }
            return false;
        }

        public boolean rootIsLimit() {
            return logicalPlan instanceof LogicalLimit;
        }

        public boolean rootIsLimitWithOffset() {
            return logicalPlan instanceof LogicalLimit && ((LogicalLimit<?>) logicalPlan).getOffset() != 0;
        }

        public boolean rootIsLimitZero() {
            return logicalPlan instanceof LogicalLimit && ((LogicalLimit<?>) logicalPlan).getLimit() == 0;
        }
    }

    private static class PlanNodeCorrelatedInfo {
        private PlanType planType;
        private boolean containCorrelatedSlots;
        private boolean hasGroupBy;
        private LogicalAggregate aggregate;

        public PlanNodeCorrelatedInfo(PlanType planType, boolean containCorrelatedSlots) {
            this(planType, containCorrelatedSlots, null);
        }

        public PlanNodeCorrelatedInfo(PlanType planType, boolean containCorrelatedSlots,
                LogicalAggregate aggregate) {
            this.planType = planType;
            this.containCorrelatedSlots = containCorrelatedSlots;
            this.aggregate = aggregate;
            this.hasGroupBy = aggregate != null ? !aggregate.getGroupByExpressions().isEmpty() : false;
        }
    }

    private static class CorrelatedSlotsValidator
            extends PlanVisitor<PlanNodeCorrelatedInfo, Void> {
        private final ImmutableSet<Slot> correlatedSlots;

        public CorrelatedSlotsValidator(ImmutableSet<Slot> correlatedSlots) {
            this.correlatedSlots = correlatedSlots;
        }

        @Override
        public PlanNodeCorrelatedInfo visit(Plan plan, Void context) {
            return new PlanNodeCorrelatedInfo(plan.getType(), findCorrelatedSlots(plan));
        }

        public PlanNodeCorrelatedInfo visitLogicalProject(LogicalProject plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in project is not supported",
                                correlatedSlots));
            } else {
                PlanType planType = ExpressionUtils.containsWindowExpression(
                        ((LogicalProject<?>) plan).getProjects()) ? PlanType.LOGICAL_WINDOW : plan.getType();
                return new PlanNodeCorrelatedInfo(planType, false);
            }
        }

        public PlanNodeCorrelatedInfo visitLogicalAggregate(LogicalAggregate plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in aggregate is not supported",
                                correlatedSlots, plan));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false, plan);
            }
        }

        public PlanNodeCorrelatedInfo visitLogicalJoin(LogicalJoin plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in join is not supported",
                                correlatedSlots, plan));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false);
            }
        }

        public PlanNodeCorrelatedInfo visitLogicalSort(LogicalSort plan, Void context) {
            boolean containCorrelatedSlots = findCorrelatedSlots(plan);
            if (containCorrelatedSlots) {
                throw new AnalysisException(
                        String.format("access outer query's column in order by is not supported",
                                correlatedSlots, plan));
            } else {
                return new PlanNodeCorrelatedInfo(plan.getType(), false);
            }
        }

        private boolean findCorrelatedSlots(Plan plan) {
            return plan.getExpressions().stream().anyMatch(expression -> !Sets
                    .intersection(correlatedSlots, expression.getInputSlots()).isEmpty());
        }
    }

    private LogicalAggregate validateNodeInfoList(List<PlanNodeCorrelatedInfo> nodeInfoList) {
        LogicalAggregate topAggregate = null;
        int size = nodeInfoList.size();
        if (size > 0) {
            List<PlanNodeCorrelatedInfo> correlatedNodes = new ArrayList<>(4);
            boolean checkNodeTypeAfterCorrelatedNode = false;
            boolean checkAfterAggNode = false;
            for (int i = size - 1; i >= 0; --i) {
                PlanNodeCorrelatedInfo nodeInfo = nodeInfoList.get(i);
                if (checkNodeTypeAfterCorrelatedNode) {
                    switch (nodeInfo.planType) {
                        case LOGICAL_LIMIT:
                            throw new AnalysisException(
                                    "limit is not supported in correlated subquery");
                        case LOGICAL_GENERATE:
                            throw new AnalysisException(
                                    "access outer query's column before lateral view is not supported");
                        case LOGICAL_AGGREGATE:
                            if (checkAfterAggNode) {
                                throw new AnalysisException(
                                        "access outer query's column before two agg nodes is not supported");
                            }
                            if (nodeInfo.hasGroupBy) {
                                // TODO support later
                                throw new AnalysisException(
                                        "access outer query's column before agg with group by is not supported");
                            }
                            checkAfterAggNode = true;
                            topAggregate = nodeInfo.aggregate;
                            break;
                        case LOGICAL_WINDOW:
                            throw new AnalysisException(
                                    "access outer query's column before window function is not supported");
                        case LOGICAL_JOIN:
                            throw new AnalysisException(
                                    "access outer query's column before join is not supported");
                        case LOGICAL_SORT:
                            // allow any sort node, the sort node will be removed by ELIMINATE_ORDER_BY_UNDER_SUBQUERY
                            break;
                        case LOGICAL_PROJECT:
                            // allow any project node
                            break;
                        case LOGICAL_SUBQUERY_ALIAS:
                            // allow any subquery alias
                            break;
                        default:
                            if (checkAfterAggNode) {
                                throw new AnalysisException(
                                        "only project, sort and subquery alias node is allowed after agg node");
                            }
                            break;
                    }
                }
                if (nodeInfo.containCorrelatedSlots) {
                    correlatedNodes.add(nodeInfo);
                    checkNodeTypeAfterCorrelatedNode = true;
                }
            }

            // only support 1 correlated node for now
            if (correlatedNodes.size() > 1) {
                throw new AnalysisException(
                        "access outer query's column in two places is not supported");
            }
        }
        return topAggregate;
    }

    /**
     * 递归验证子查询 plan 树的结构，检查相关列（correlated slots）的使用是否符合限制。
     *
     * 验证逻辑：
     * 1. 使用回溯法（DFS）遍历整个 plan 树，从根到叶子节点收集每个节点的 PlanNodeCorrelatedInfo；
     * 2. 当到达叶子节点时，调用 validateNodeInfoList 验证从根到该叶子的整条路径是否符合规则；
     * 3. 如果路径上存在聚合节点，将其记录到 topAgg 集合中，供后续使用。
     *
     * 限制规则（详见 validateNodeInfoList）：
     * 
     * 关于"之前"和"之后"的定义：
     * - "之前" = 在 plan 树中更靠近根节点（上层），执行时先执行
     * - "之后" = 在 plan 树中更靠近叶子节点（下层），执行时后执行
     * 
     * 例如 plan 树结构（从上到下执行）：
     *     LogicalProject (根节点，先执行)
     *         |
     *     LogicalAggregate (中间层)
     *         |
     *     LogicalFilter (相关列在这里，t1.c = t2.c)
     *         |
     *     LogicalOlapScan (叶子节点，后执行)
     * 
     * 在这个例子中：
     * - Filter 在 Aggregate "之后"（更靠近叶子，后执行）
     * - Project 在 Aggregate "之前"（更靠近根，先执行）
     *
     * 具体限制：
     * - 如果相关列出现在聚合节点"之后"（如 WHERE 子句），则必须有一个 scalar aggregate（无 GROUP BY）；
     * - 聚合节点"之后"只能有 PROJECT、SORT、SUBQUERY_ALIAS 节点；
     * - 相关列不能出现在聚合、Join、Window 等敏感节点的表达式中。
     *
     * 具体例子：
     *
     * ✅ 允许的情况：
     *   1) SELECT (SELECT MAX(t2.b) FROM t2 WHERE t2.c = t1.c) FROM t1;
     *      Plan 树结构（从上到下）：
     *        LogicalProject
     *            |
     *        LogicalAggregate(MAX)  ← 聚合节点
     *            |
     *        LogicalFilter(WHERE t2.c = t1.c)  ← 相关列在这里（在聚合"之后"）
     *            |
     *        LogicalOlapScan(t2)
     *      相关列 t1.c 在 WHERE（Filter）中，Filter 在聚合节点"之后"（下层），符合规则。
     *
     *   2) SELECT (SELECT COUNT(*) FROM t2 WHERE t2.d = t1.d) FROM t1;
     *      相关列 t1.d 在 WHERE 中（在聚合"之后"），之后有聚合 COUNT，符合规则。
     *
     *   3) SELECT (SELECT t2.x FROM t2 WHERE t2.y = t1.y LIMIT 1) FROM t1;
     *      相关列 t1.y 在 WHERE 中，但子查询有 LIMIT 1（会被消除），
     *      后续在 SubqueryToApply 中会自动添加 ANY_VALUE 聚合，符合规则。
     *
     * ❌ 不允许的情况：
     *   1) SELECT (SELECT t2.b FROM t2 WHERE t2.c = t1.c) FROM t1;
     *      Plan 树结构：
     *        LogicalProject
     *            |
     *        LogicalFilter(WHERE t2.c = t1.c)  ← 相关列在这里
     *            |
     *        LogicalOlapScan(t2)  ← 没有聚合节点
     *      相关列 t1.c 在 WHERE 中，但整个路径上没有聚合节点，会抛异常：
     *      "only project, sort and subquery alias node is allowed after agg node"
     *      （因为相关列出现在 plan 树中，但路径上没有聚合节点来"消化"相关列）
     *
     *   2) SELECT (SELECT MAX(t2.b) FROM t2 WHERE t2.c = t1.c GROUP BY t2.d) FROM t1;
     *      相关列 t1.c 在 WHERE 中，但聚合有 GROUP BY，会抛异常：
     *      "access outer query's column before agg with group by is not supported"
     *
     *   3) SELECT (SELECT t2.b FROM t2 JOIN t3 ON t2.id = t3.id WHERE t3.c = t1.c) FROM t1;
     *      Plan 树结构（从上到下）：
     *        LogicalProject
     *            |
     *        LogicalFilter(WHERE t3.c = t1.c)  ← 相关列在这里（在 JOIN "之后"）
     *            |
     *        LogicalJoin(t2 JOIN t3)  ← JOIN 节点（在相关列"之前"）
     *            /              \
     *      LogicalOlapScan(t2)  LogicalOlapScan(t3)
     *      相关列 t1.c 出现在 JOIN 之后（WHERE 子句中），会抛异常：
     *      "access outer query's column before join is not supported"
     *      
     *      原因：
     *      - 相关列的解相关需要将相关谓词（t3.c = t1.c）提取到 Apply 的 correlationFilter 中
     *      - 但相关列在 JOIN 之后使用，意味着相关性跨越了 JOIN 节点，使得解相关变得复杂：
     *        * JOIN 会改变数据的形状和分布（两表 join 后的行数、列数都变了）
     *        * 相关列 t1.c 需要与 JOIN 后的结果（t3.c）进行比较，这需要特殊的处理逻辑
     *        * 当前的解相关算法（CorrelateApplyToUnCorrelateApply）假设相关列直接出现在
     *          聚合节点之前（如 WHERE 子句），然后通过聚合来"消化"相关性
     *      - 如果允许这种情况，需要在 JOIN 之后、聚合之前处理相关性，这会大大增加
     *        解相关和 Apply→Join 转换的复杂度，目前暂不支持
     *
     *   4) SELECT (SELECT MAX(t2.b) FROM t2 WHERE t2.c = t1.c) FROM t1
     *      UNION ALL
     *      SELECT (SELECT t2.x FROM t2 WHERE t2.y = t1.y) FROM t1;
     *      如果子查询中有两个地方使用相关列，会抛异常：
     *      "access outer query's column in two places is not supported"
     *
     * @param plan 当前要验证的 plan 节点
     * @param validator 用于检查节点是否包含相关列的访问器
     * @param nodeInfoList 当前路径上所有节点的 PlanNodeCorrelatedInfo 列表（用于回溯）
     * @param topAgg 收集到的顶层聚合节点集合（用于后续处理）
     */
    private void validateSubquery(Plan plan, CorrelatedSlotsValidator validator,
            List<PlanNodeCorrelatedInfo> nodeInfoList, Set<LogicalAggregate> topAgg) {
        // 将当前节点的 PlanNodeCorrelatedInfo 加入路径列表
        // PlanNodeCorrelatedInfo 包含：节点类型、是否包含相关列、是否有聚合等信息
        nodeInfoList.add(plan.accept(validator, null));
        // 递归处理所有子节点
        for (Plan child : plan.children()) {
            validateSubquery(child, validator, nodeInfoList, topAgg);
        }
        // 当到达叶子节点时（没有子节点），验证从根到该叶子的整条路径
        if (plan.children().isEmpty()) {
            // validateNodeInfoList 会检查路径上相关列的使用是否符合限制，
            // 并返回路径上的顶层聚合节点（如果存在）
            LogicalAggregate topAggNode = validateNodeInfoList(nodeInfoList);
            if (topAggNode != null) {
                // 将找到的顶层聚合节点加入集合，供后续规则使用
                topAgg.add(topAggNode);
            }
        }
        // 回溯：移除当前节点，恢复 nodeInfoList 到进入当前节点前的状态
        // 这样在返回到父节点时，nodeInfoList 只包含从根到父节点的路径信息
        nodeInfoList.remove(nodeInfoList.size() - 1);
    }
}
