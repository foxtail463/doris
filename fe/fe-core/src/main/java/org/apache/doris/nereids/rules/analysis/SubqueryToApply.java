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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.TrySimplifyPredicateWithMarkJoinSlot;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.NotNullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SubqueryToApply：负责在分析阶段把表达式中的子查询改写成 LogicalApply。
 *
 * 整体分两步：
 * 1. 在 Filter / Project / Join 等节点中，先把包含子查询的谓词/投影表达式里的 SubqueryExpr
 *    替换成对应的「标记列（mark join slot）」或「子查询输出列」，得到一个不再直接依赖 SubqueryExpr 的表达式；
 * 2. 再基于这些 SubqueryExpr 构造 LogicalApply 节点，并把上一步中的表达式更新为依赖 LogicalApply 输出。
 *
 * 这样做的好处是，将「语法上的子查询」统一降解为「主查询 + Apply 结构」，为后续的解相关、Apply→Join 等重写打基础。
 */
public class SubqueryToApply implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        // 本规则工厂一共注册 4 类 “subquery → apply” 规则：
        // 1）Filter 中的子查询；2）Project 中的子查询；3）一行常量表上的子查询；4）Join 条件中的子查询。
        // 这些规则都是 Analysis 阶段的逻辑改写，为后续 Rewrite/Implementation 做准备。
        return ImmutableList.of(
            // 场景一：WHERE / HAVING 等 Filter 谓词中包含子查询时，将其改写为 Apply。
            // 典型例子：
            //   1）WHERE 子查询：
            //      SELECT *
            //      FROM t1
            //      WHERE t1.a > (SELECT MAX(t2.a) FROM t2 WHERE t2.b = t1.b);
            //      —— 这里的 "(SELECT MAX(t2.a) ...)" 就是在 Filter 中的 scalar subquery。
            //   2）HAVING 子查询：
            //      SELECT k1, SUM(v)
            //      FROM t1
            //      GROUP BY k1
            //      HAVING SUM(v) > (SELECT AVG(v2) FROM t2 WHERE t2.k2 = t1.k1);
            //      —— 这里的 "(SELECT AVG(v2) ...)" 是 HAVING 中的 scalar subquery。
            RuleType.FILTER_SUBQUERY_TO_APPLY.build(
                logicalFilter().thenApply(ctx -> {
                    // 当前匹配到的根节点是 LogicalFilter
                    LogicalFilter<Plan> filter = ctx.root;

                    // 取出 Filter 的所有谓词（已按 AND 拆分为一组 conjunct）
                    Set<Expression> conjuncts = filter.getConjuncts();
                    // 收集每个谓词中出现的 SubqueryExpr，便于后续逐个处理
                    CollectSubquerys collectSubquerys = collectSubquerys(conjuncts);
                    if (!collectSubquerys.hasSubquery) {
                        // 当前 filter 根本没有子查询，直接返回原节点
                        return filter;
                    }

                    // 针对每个 conjunct 判断：是否需要把 mark join slot 输出到上层（例如 OR 里含子查询的场景）
                    List<Boolean> shouldOutputMarkJoinSlot = shouldOutputMarkJoinSlot(conjuncts);

                    // 将 Set 转成 List，方便通过下标对齐访问
                    List<Expression> oldConjuncts = Utils.fastToImmutableList(conjuncts);
                    // 用 builder 收集改写后的新谓词（不再直接包含 SubqueryExpr）
                    ImmutableSet.Builder<Expression> newConjuncts = new ImmutableSet.Builder<>();
                    // applyPlan 记录挂上所有 LogicalApply 之后的子树
                    LogicalPlan applyPlan = null;
                    // tmpPlan 表示当前循环中最新的 child plan（每处理一个子查询都会更新）
                    LogicalPlan tmpPlan = (LogicalPlan) filter.child();

                    // subqueryExprsList 与 oldConjuncts 按下标一一对应，表示每个谓词里有哪些子查询
                    List<Set<SubqueryExpr>> subqueryExprsList = collectSubquerys.subqueies;
                    // 以 AND 的每个 conjunct 为粒度依次处理其中的子查询
                    for (int i = 0; i < subqueryExprsList.size(); ++i) {
                        // 当前这个谓词中包含的所有子查询
                        Set<SubqueryExpr> subqueryExprs = subqueryExprsList.get(i);
                        if (subqueryExprs.isEmpty()) {
                            // 如果当前谓词中没有子查询，直接原样放入 newConjuncts
                            newConjuncts.add(oldConjuncts.get(i));
                            continue;
                        }

                        // first step: 用 ReplaceSubquery 把表达式中的子查询替换成 mark slot / 标量输出
                        // second step: 基于这些子查询构造 LogicalApply，并更新表达式中对其的引用
                        ReplaceSubquery replaceSubquery = new ReplaceSubquery(
                                ctx.statementContext, shouldOutputMarkJoinSlot.get(i));
                        // SubqueryContext 用于记录每个子查询对应的 markJoinSlot
                        SubqueryContext context = new SubqueryContext(subqueryExprs);
                        // 在当前谓词表达式上，先把 SubqueryExpr 替换成 mark / 标量列
                        Expression conjunct = replaceSubquery.replace(oldConjuncts.get(i), context);
                        /*
                         * 这里额外做一次基于 mark join slot 的「可推断非空」分析：
                         * - 思路是：把 mark join slot 替换成 null/false，跑一次常量折叠；
                         * - 如果结果要么恒 true，要么只含 null/false（Filter 会丢掉 null/false），
                         *   则可以认为 mark slot 实际上是 non-nullable boolean。
                         * 这个信息会存入 LogicalApply，在之后 InApplyToJoin 规则中，
                         * 若是 semi join 且 mark slot 非空，就可以把 mark 谓词安全地转成 hash 谓词。
                         */
                        // 构造表达式重写上下文，用于在当前谓词上做 mark slot 非空性推断
                        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx.cascadesContext);
                        boolean isMarkSlotNotNull = conjunct.containsType(MarkJoinSlotReference.class)
                                        ? ExpressionUtils.canInferNotNullForMarkSlot(
                                                TrySimplifyPredicateWithMarkJoinSlot.INSTANCE.rewrite(conjunct,
                                                        rewriteContext), rewriteContext)
                                        : false;
                        // 将当前谓词中的子查询统一转换为 Apply，并返回新的 plan +（可能已改写的）谓词表达式
                        Pair<LogicalPlan, Optional<Expression>> result = subqueryToApply(subqueryExprs.stream()
                                    .collect(ImmutableList.toImmutableList()), tmpPlan,
                                context.getSubqueryToMarkJoinSlot(),
                                ctx.cascadesContext,
                                Optional.of(conjunct), isMarkSlotNotNull);
                        // 更新最新的含 Apply 的子树
                        applyPlan = result.first;
                        // tmpPlan 也更新为最新子树，供后续子查询继续在此基础上改写
                        tmpPlan = applyPlan;
                        // 如果返回的表达式被进一步改写，则使用改写后的；否则使用原 conjunct
                        newConjuncts.add(result.second.isPresent() ? result.second.get() : conjunct);
                    }
                    // 最终在新的 Apply 之上再挂一个 LogicalFilter，并用 LogicalProject 恢复原来的输出列顺序
                    Plan newFilter = new LogicalFilter<>(newConjuncts.build(), applyPlan);
                    return new LogicalProject<>(filter.getOutput().stream().collect(ImmutableList.toImmutableList()),
                        newFilter);
                })
            ),
            // 场景二：SELECT 列表中包含子查询的情况（投影表达式中的 subquery）
            // 典型例子（包含多个子查询，展示循环过程）：
            //   SELECT
            //       t1.a,
            //       (SELECT MAX(t2.b) FROM t2 WHERE t2.c = t1.c) AS max_b,
            //       t1.d,
            //       (SELECT COUNT(*) FROM t3 WHERE t3.e = t1.e) AS cnt
            //   FROM t1;
            //
            // 在分析阶段，循环处理每个投影表达式，逐步将子查询转换为 Apply。树的变化过程如下：
            //
            // ========== 初始状态 ==========
            // before:
            //      LogicalProject(projects=[
            //          t1.a,
            //          ScalarSubquery(subqueryPlan1: t2 ... MAX(t2.b)) AS max_b,
            //          t1.d,
            //          ScalarSubquery(subqueryPlan2: t3 ... COUNT(*)) AS cnt
            //      ])
            //              |
            //         childPlan(t1)
            //
            // ========== 循环开始 ==========
            // childPlan = t1（初始值）
            // newProjects = []（空列表）
            //
            // ---------- 循环 i=0：处理 t1.a（无子查询） ----------
            // newProjects.add(t1.a)  // 直接复用，无变化
            // childPlan 仍为 t1
            //
            // ---------- 循环 i=1：处理 max_b（含子查询1） ----------
            // Step 1: ReplaceSubquery 替换表达式
            //      newProject = "subqueryOutputSlot1 AS max_b"  // 表达式层面替换
            //
            // Step 2: subqueryToApply → addApply，在 childPlan(t1) 上挂第一个 Apply
            //      childPlan 更新为：
            //          LogicalProject(projects=[ t1.*, subqueryOutputSlot1 ])
            //                  |
            //          LogicalApply(subQueryType=SCALAR_SUBQUERY,
            //                      correlationSlots=[t1.c], ...)
            //              /                               \
            //       childPlan(t1)                    subqueryPlan1(t2 ... MAX(t2.b))
            //
            // newProjects.add(subqueryOutputSlot1 AS max_b)
            //
            // ---------- 循环 i=2：处理 t1.d（无子查询） ----------
            // newProjects.add(t1.d)  // 直接复用，无变化
            // childPlan 仍为上次循环的结果（已包含第一个 Apply）
            //
            // ---------- 循环 i=3：处理 cnt（含子查询2） ----------
            // Step 1: ReplaceSubquery 替换表达式
            //      newProject = "subqueryOutputSlot2 AS cnt"  // 表达式层面替换
            //
            // Step 2: subqueryToApply → addApply，在 childPlan（已含第一个 Apply）上再挂第二个 Apply
            //      childPlan 更新为（嵌套结构）：
            //          LogicalProject(projects=[ t1.*, subqueryOutputSlot1, subqueryOutputSlot2 ])
            //                  |
            //          LogicalApply(subQueryType=SCALAR_SUBQUERY,
            //                      correlationSlots=[t1.e], ...)
            //              /                               \
            //       LogicalProject(projects=[ t1.*, subqueryOutputSlot1 ])
            //                  |                                    subqueryPlan2(t3 ... COUNT(*))
            //          LogicalApply(subQueryType=SCALAR_SUBQUERY, ...)
            //              /                               \
            //       childPlan(t1)                    subqueryPlan1(t2 ... MAX(t2.b))
            //
            // newProjects.add(subqueryOutputSlot2 AS cnt)
            //
            // ========== 循环结束，最终结果 ==========
            //      LogicalProject(projects=[
            //          t1.a,
            //          subqueryOutputSlot1 AS max_b,
            //          t1.d,
            //          subqueryOutputSlot2 AS cnt
            //      ])
            //              |
            //      LogicalProject(projects=[ t1.*, subqueryOutputSlot1, subqueryOutputSlot2 ])
            //              |
            //      LogicalApply(subQueryType=SCALAR_SUBQUERY, correlationSlots=[t1.e], ...)
            //          /                               \
            //   LogicalProject(projects=[ t1.*, subqueryOutputSlot1 ])
            //          |                                    subqueryPlan2(t3 ... COUNT(*))
            //      LogicalApply(subQueryType=SCALAR_SUBQUERY, correlationSlots=[t1.c], ...)
            //          /                               \
            //   childPlan(t1)                    subqueryPlan1(t2 ... MAX(t2.b))
            //
            // 关键点：
            //   1）循环按投影顺序逐个处理，childPlan 每次循环都会更新（嵌套挂 Apply）；
            //   2）每个含子查询的投影，都会在 childPlan 上挂一层 LogicalApply + LogicalProject；
            //   3）外层 Project 的投影表达式只引用 slot，真正的子查询逻辑都在 Apply 子树中。
            RuleType.PROJECT_SUBQUERY_TO_APPLY.build(logicalProject().thenApply(ctx -> {
                // 当前匹配到的根节点是 LogicalProject
                LogicalProject<Plan> project = ctx.root;

                // 当前 Project 的所有投影表达式
                List<NamedExpression> projects = project.getProjects();
                // 收集每个投影表达式中出现的 SubqueryExpr
                CollectSubquerys collectSubquerys = collectSubquerys(projects);
                if (!collectSubquerys.hasSubquery) {
                    // 没有子查询，直接返回原 Project
                    return project;
                }

                // 每个投影表达式对应的一组子查询（与 projects 下标一一对应）
                List<Set<SubqueryExpr>> subqueryExprsList = collectSubquerys.subqueies;
                // 保存原始投影列表，后续按下标访问和复用
                List<NamedExpression> oldProjects = ImmutableList.copyOf(projects);
                // 构造新的投影列表（其中的 SubqueryExpr 将被替换）
                ImmutableList.Builder<NamedExpression> newProjects = new ImmutableList.Builder<>();
                // childPlan 表示 Project 的子节点 plan，随着 Apply 的挂载不断更新
                LogicalPlan childPlan = (LogicalPlan) project.child();
                // applyPlan 暂存 subqueryToApply 返回的最新 plan
                LogicalPlan applyPlan;
                // 逐个 project 表达式处理其中的子查询：先替换表达式，再构造 Apply
                for (int i = 0; i < subqueryExprsList.size(); ++i) {
                    Set<SubqueryExpr> subqueryExprs = subqueryExprsList.get(i);
                    if (subqueryExprs.isEmpty()) {
                        // 当前投影中没有子查询，直接复用原投影表达式
                        newProjects.add(oldProjects.get(i));
                        continue;
                    }

                    // first step: 替换 project 列中的 SubqueryExpr
                    // second step: 基于子查询构造 LogicalApply，并把输出更新为 mark / 标量列
                    ReplaceSubquery replaceSubquery =
                            new ReplaceSubquery(ctx.statementContext, true);
                    // SubqueryContext 记录该投影中每个子查询对应的 markJoinSlot
                    SubqueryContext context = new SubqueryContext(subqueryExprs);
                    // newProject 为将 SubqueryExpr 替换成 mark / 标量列后的新投影表达式
                    Expression newProject =
                            replaceSubquery.replace(oldProjects.get(i), context);

                    // 基于该投影中的所有子查询，在 childPlan 上挂上 Apply，得到新的 plan
                    Pair<LogicalPlan, Optional<Expression>> result =
                            subqueryToApply(Utils.fastToImmutableList(subqueryExprs), childPlan,
                                    context.getSubqueryToMarkJoinSlot(), ctx.cascadesContext,
                                    Optional.of(newProject), false);
                    // 更新最新的 Apply plan
                    applyPlan = result.first;
                    // childPlan 也更新为最新 plan，供下一个投影继续使用
                    childPlan = applyPlan;
                    // 使用可能被进一步改写的表达式（若无改写则直接使用 newProject）
                    newProjects.add(
                            result.second.isPresent() ? (NamedExpression) result.second.get()
                                    : (NamedExpression) newProject);
                }

                // 使用新的投影列表 + 最新的 childPlan 构造新的 Project
                return project.withProjectsAndChild(newProjects.build(), childPlan);
            })),
            // 场景三：OneRowRelation（如 VALUES(1) 这类一行常量表）上的子查询
            // 这里先人为包一层 LogicalProject，统一复用 PROJECT_SUBQUERY_TO_APPLY 规则处理
            RuleType.ONE_ROW_RELATION_SUBQUERY_TO_APPLY.build(logicalOneRowRelation()
                .when(ctx -> ctx.getProjects().stream()
                        .anyMatch(project -> project.containsType(SubqueryExpr.class)))
                .thenApply(ctx -> {
                    // 当前匹配到的根节点是 LogicalOneRowRelation
                    LogicalOneRowRelation oneRowRelation = ctx.root;
                    // create a LogicalProject node with the same project lists above LogicalOneRowRelation
                    // create a LogicalOneRowRelation with a dummy output column
                    // so PROJECT_SUBQUERY_TO_APPLY rule can handle the subquery unnest thing
                    // 这里人为在 OneRowRelation 上面加一层 LogicalProject，
                    // 并在 child 的 OneRowRelation 中补一个 always-true 的虚拟列，
                    // 这样 PROJECT_SUBQUERY_TO_APPLY 规则就能统一处理其中的子查询。
                    return new LogicalProject<Plan>(oneRowRelation.getProjects(),
                            oneRowRelation.withProjects(
                                    ImmutableList.of(new Alias(BooleanLiteral.of(true),
                                            ctx.statementContext.generateColumnName()))));
                })),
            // 场景四：Join 条件（ON 子句）中包含子查询的情况
            // 典型例子（以 LEFT JOIN 为例）：
            //   SELECT *
            //   FROM t1
            //   LEFT JOIN t2
            //     ON t1.k1 = t2.k1
            //    AND t1.a IN (SELECT s.x FROM s WHERE s.y = t2.y);
            //
            // 该规则会把 “ON 谓词中的 IN/EXISTS/标量子查询” 抽出来，挂成 t1 或 t2 侧的 LogicalApply。
            // 树的大致变化（以右表相关的 IN 子查询为例）：
            //
            // before:
            //      LogicalJoin(type=LEFT_JOIN,
            //                  hashJoinConjuncts=[t1.k1 = t2.k1],
            //                  otherJoinConjuncts=[t1.a IN (SELECT s.x FROM s WHERE s.y = t2.y)])
            //          /                               \
            //      leftChild(t1 ...)              rightChild(t2 ...)
            //
            // after（右表相关的 IN 子查询被改写为 mark join 语义的 Apply 挂在右子树上）:
            //      LogicalJoin(type=LEFT_JOIN,
            //                  hashJoinConjuncts=[t1.k1 = t2.k1],
            //                  otherJoinConjuncts=[markSlot 或 nvl(markSlot, FALSE) ...])
            //          /                                   \
            //      leftChild(t1 ...)                 LogicalProject([t2.* ... markSlot])
            //                                               |
            //                                      LogicalFilter(predicate: markSlot 或 TRUE)
            //                                               |
            //                                      LogicalApply(subQueryType=IN_SUBQUERY,
            //                                                  markJoinSlotReference=markSlot,
            //                                                  compareExpr=t1.a 或 t2.col)
            //                                           /                     \
            //                                   t2(原 rightChild)          subqueryPlan(s ...)
            //
            // 对 EXISTS 子查询同理，只是 Apply 的 subQueryType=EXISTS_SUBQUERY，谓词是 nvl(markSlot,FALSE)。
            RuleType.JOIN_SUBQUERY_TO_APPLY
                .build(logicalJoin()
                .when(join -> join.getHashJoinConjuncts().isEmpty() && !join.getOtherJoinConjuncts().isEmpty())
                .thenApply(ctx -> {
                    // 当前匹配到的根节点是 LogicalJoin
                    LogicalJoin<Plan, Plan> join = ctx.root;
                    // 按是否包含 SubqueryExpr，将 otherJoinConjuncts 分组：true=含子查询，false=不含
                    Map<Boolean, List<Expression>> joinConjuncts = join.getOtherJoinConjuncts().stream()
                            .collect(Collectors.groupingBy(conjunct -> conjunct.containsType(SubqueryExpr.class),
                                    Collectors.toList()));
                    // 所有包含子查询的 join 条件
                    List<Expression> subqueryConjuncts = joinConjuncts.get(true);
                    if (subqueryConjuncts == null || subqueryConjuncts.stream()
                            .anyMatch(expr -> !isValidSubqueryConjunct(expr))) {
                        // join 条件里没有子查询，或者子查询形态不受支持，直接返回
                        return join;
                    }

                    // 判定这些子查询分别与左表/右表/都不相关，用于确定 Apply 应挂在左还是右子树
                    List<RelatedInfo> relatedInfoList = collectRelatedInfo(
                            subqueryConjuncts, join.left(), join.right());
                    if (relatedInfoList.stream().anyMatch(info -> info == RelatedInfo.UnSupported)) {
                        // 如发现不受支持的关联形式，则整个 join 不做改写
                        return join;
                    }

                    // 对每个子查询 conjunct，收集其中所有 SubqueryExpr，便于后续构造 Apply
                    ImmutableList<Set<SubqueryExpr>> subqueryExprsList = subqueryConjuncts.stream()
                            .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                            .collect(ImmutableList.toImmutableList());
                    // 用 builder 构建新的 join 条件（包含改写后的表达式）
                    ImmutableList.Builder<Expression> newConjuncts = new ImmutableList.Builder<>();
                    // applyPlan 暂存每次 subqueryToApply 返回的新 plan
                    LogicalPlan applyPlan;
                    // leftChildPlan/rightChildPlan 分别表示可能被挂上 Apply 的左右子树
                    LogicalPlan leftChildPlan = (LogicalPlan) join.left();
                    LogicalPlan rightChildPlan = (LogicalPlan) join.right();

                    // 仍然以 AND 的每个 conjunct 为粒度处理 join 条件中的子查询
                    for (int i = 0; i < subqueryExprsList.size(); ++i) {
                        Set<SubqueryExpr> subqueryExprs = subqueryExprsList.get(i);
                        if (subqueryExprs.size() > 1) {
                            // only support the conjunct contains one subquery expr
                            return join;
                        }

                        // first step: 用 ReplaceSubquery 把 join 条件中的子查询替换成 mark / 标量
                        // second step: 基于子查询构造 LogicalApply，并更新 join 条件
                        ReplaceSubquery replaceSubquery = new ReplaceSubquery(ctx.statementContext, true);
                        SubqueryContext context = new SubqueryContext(subqueryExprs);
                        // 先在 join 条件里把 SubqueryExpr 替换为 mark / 标量列
                        Expression conjunct = replaceSubquery.replace(subqueryConjuncts.get(i), context);
                        /*
                         * 与 FILTER_SUBQUERY_TO_APPLY 中相同的逻辑：基于 mark join slot 做非空性推断，
                         * 把「mark slot 一定不会是 null」的信息传递给 LogicalApply，供之后 InApplyToJoin 使用。
                        */
                        ExpressionRewriteContext rewriteContext
                                = new ExpressionRewriteContext(join, ctx.cascadesContext);
                        boolean isMarkSlotNotNull = conjunct.containsType(MarkJoinSlotReference.class)
                                ? ExpressionUtils.canInferNotNullForMarkSlot(
                                    TrySimplifyPredicateWithMarkJoinSlot.INSTANCE.rewrite(conjunct, rewriteContext),
                                    rewriteContext)
                                : false;
                        // 将当前子查询转成 Apply，并返回新的左右子树 plan 和（可能被改写的）join 条件
                        Pair<LogicalPlan, Optional<Expression>> result = subqueryToApply(
                                subqueryExprs.stream().collect(ImmutableList.toImmutableList()),
                                relatedInfoList.get(i) == RelatedInfo.RelatedToLeft ? leftChildPlan : rightChildPlan,
                                context.getSubqueryToMarkJoinSlot(),
                                ctx.cascadesContext, Optional.of(conjunct), isMarkSlotNotNull);
                        // 更新最新的 Apply plan
                        applyPlan = result.first;
                        if (relatedInfoList.get(i) == RelatedInfo.RelatedToLeft) {
                            // 若与左表相关，则把新 plan 赋给 leftChildPlan
                            leftChildPlan = applyPlan;
                        } else {
                            // 否则赋给 rightChildPlan
                            rightChildPlan = applyPlan;
                        }
                        // 使用可能被改写后的表达式加入 newConjuncts
                        newConjuncts.add(result.second.isPresent() ? result.second.get() : conjunct);
                    }
                    // simpleConjuncts 是原来就不含子查询的 join 条件，需要一并保留
                    List<Expression> simpleConjuncts = joinConjuncts.get(false);
                    if (simpleConjuncts != null) {
                        newConjuncts.addAll(simpleConjuncts);
                    }
                    // 使用新的 join 条件和左右子树，构造一个更新后的 LogicalJoin
                    Plan newJoin = join.withConjunctsChildren(join.getHashJoinConjuncts(),
                            newConjuncts.build(), leftChildPlan, rightChildPlan, null);
                    return newJoin;
                }))
        );
    }

    private static boolean isValidSubqueryConjunct(Expression expression) {
        // only support 1 subquery expr in the expression
        // don't support expression like subquery1 or subquery2
        return expression
                .collectToList(SubqueryExpr.class::isInstance)
                .size() == 1;
    }

    private enum RelatedInfo {
        // both subquery and its output don't related to any child. like (select sum(t.a) from t) > 1
        Unrelated,
        // either subquery or its output only related to left child. like bellow:
        // tableLeft.a in (select t.a from t)
        // 3 in (select t.b from t where t.a = tableLeft.a)
        // tableLeft.a > (select sum(t.a) from t where tableLeft.b = t.b)
        RelatedToLeft,
        // like above, but related to right child
        RelatedToRight,
        // subquery related to both left and child is not supported:
        // tableLeft.a > (select sum(t.a) from t where t.b = tableRight.b)
        UnSupported
    }

    private ImmutableList<RelatedInfo> collectRelatedInfo(List<Expression> subqueryConjuncts,
            Plan leftChild, Plan rightChild) {
        int size = subqueryConjuncts.size();
        ImmutableList.Builder<RelatedInfo> correlatedInfoList = new ImmutableList.Builder<>();
        Set<Slot> leftOutputSlots = leftChild.getOutputSet();
        Set<Slot> rightOutputSlots = rightChild.getOutputSet();
        for (int i = 0; i < size; ++i) {
            Expression expression = subqueryConjuncts.get(i);
            List<SubqueryExpr> subqueryExprs = expression.collectToList(SubqueryExpr.class::isInstance);
            RelatedInfo relatedInfo = RelatedInfo.UnSupported;
            if (subqueryExprs.size() == 1) {
                SubqueryExpr subqueryExpr = subqueryExprs.get(0);
                List<Slot> correlatedSlots = subqueryExpr.getCorrelateSlots();
                if (subqueryExpr instanceof ScalarSubquery) {
                    Set<Slot> inputSlots = subqueryExpr.getInputSlots();
                    if (correlatedSlots.isEmpty() && inputSlots.isEmpty()) {
                        relatedInfo = RelatedInfo.Unrelated;
                    } else if (leftOutputSlots.containsAll(inputSlots)
                            && leftOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToLeft;
                    } else if (rightOutputSlots.containsAll(inputSlots)
                            && rightOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToRight;
                    }
                } else if (subqueryExpr instanceof InSubquery) {
                    InSubquery inSubquery = (InSubquery) subqueryExpr;
                    Set<Slot> compareSlots = inSubquery.getCompareExpr().getInputSlots();
                    if (compareSlots.isEmpty()) {
                        relatedInfo = RelatedInfo.UnSupported;
                    } else if (leftOutputSlots.containsAll(compareSlots)
                            && leftOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToLeft;
                    } else if (rightOutputSlots.containsAll(compareSlots)
                            && rightOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToRight;
                    }
                } else if (subqueryExpr instanceof Exists) {
                    if (correlatedSlots.isEmpty()) {
                        relatedInfo = RelatedInfo.Unrelated;
                    } else if (leftOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToLeft;
                    } else if (rightOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToRight;
                    }
                }
            }
            correlatedInfoList.add(relatedInfo);
        }
        return correlatedInfoList.build();
    }

    /**
     * 把当前节点上收集到的一批子查询依次转换为 Apply。
     *
     * subqueryExprs    ：当前待处理的子查询列表（顺序与原表达式中出现顺序一致）
     * childPlan        ：当前的输入 plan（每次创建新的 Apply 后都会更新）
     * subqueryToMarkJoinSlot：记录每个子查询对应的 mark join slot（若存在）
     * correlatedOuterExpr   ：外层依赖该子查询输出的表达式（如 t1.a <= scalarSubquery）
     * isMarkJoinSlotNotNull ：上层推断出来的 mark join slot 是否一定非空
     *
     * 返回：
     *  - first  ：挂上所有 Apply 之后的新 LogicalPlan
     *  - second ：根据子查询改写后的外层表达式（可能为空）
     */
    private Pair<LogicalPlan, Optional<Expression>> subqueryToApply(
            List<SubqueryExpr> subqueryExprs, LogicalPlan childPlan,
            Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
            CascadesContext ctx, Optional<Expression> correlatedOuterExpr, boolean isMarkJoinSlotNotNull) {
        Pair<LogicalPlan, Optional<Expression>> tmpPlan = Pair.of(childPlan, correlatedOuterExpr);
        for (int i = 0; i < subqueryExprs.size(); ++i) {
            SubqueryExpr subqueryExpr = subqueryExprs.get(i);
            if (subqueryExpr instanceof Exists && hasTopLevelScalarAgg(subqueryExpr.getQueryPlan())) {
                // 对于 EXISTS + 顶层标量聚合的情况：
                // 顶层标量聚合对空输入返回 null，对非空输入返回一个值，
                // 因此 EXISTS/NOT EXISTS 的结果在上层可直接简化为 TRUE/FALSE，
                // 这里就不再为它创建 Apply 了（在 ReplaceSubquery 里已经替换成常量）。
                continue;
            }

            if (!ctx.subqueryIsAnalyzed(subqueryExpr)) {
                tmpPlan = addApply(subqueryExpr, tmpPlan.first,
                    subqueryToMarkJoinSlot, ctx, tmpPlan.second, isMarkJoinSlotNotNull);
            }
        }
        return tmpPlan;
    }

    private static boolean hasTopLevelScalarAgg(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            return ((LogicalAggregate) plan).getGroupByExpressions().isEmpty();
        } else if (plan instanceof LogicalProject || plan instanceof LogicalSort) {
            return hasTopLevelScalarAgg(plan.child(0));
        }
        return false;
    }

    private Pair<LogicalPlan, Optional<Expression>> addApply(SubqueryExpr subquery, LogicalPlan childPlan,
            Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
            CascadesContext ctx, Optional<Expression> correlatedOuterExpr, boolean isMarkJoinSlotNotNull) {
        // 标记该 Subquery 已被处理，避免重复构建 Apply
        ctx.setSubqueryExprIsAnalyzed(subquery, true);
        Optional<MarkJoinSlotReference> markJoinSlot = subqueryToMarkJoinSlot.get(subquery);
        boolean needAddScalarSubqueryOutputToProjects = isScalarSubqueryOutputUsedInOuterScope(
                subquery, correlatedOuterExpr);
        // for scalar subquery, we need ensure it output at most 1 row
        // by doing that, we add an aggregate function any_value() to the project list
        // we use needRuntimeAnyValue to indicate if any_value() is needed
        // if needRuntimeAnyValue is true, we will add it to the project list
        boolean needRuntimeAnyValue = false;
        NamedExpression subqueryOutput = subquery.getQueryPlan().getOutput().get(0);
        if (subquery instanceof ScalarSubquery) {
            // scalar sub query may adjust output slot's nullable.
            subqueryOutput = ((ScalarSubquery) subquery).getOutputSlotAdjustNullable();
        }
        Slot countSlot = null;
        Slot anyValueSlot = null;
        Optional<Expression> newCorrelatedOuterExpr = correlatedOuterExpr;
        // 对需要输出到外层的相关标量子查询做特殊处理：要么复用已有聚合，要么人为补一个 count/any_value
        if (needAddScalarSubqueryOutputToProjects && !subquery.getCorrelateSlots().isEmpty()) {
            if (((ScalarSubquery) subquery).hasTopLevelScalarAgg()) {
                // consider sql: SELECT * FROM t1 WHERE t1.a <= (SELECT COUNT(t2.a) FROM t2 WHERE (t1.b = t2.b));
                // when unnest correlated subquery, we create a left join node.
                // outer query is left table and subquery is right one
                // if there is no match, the row from right table is filled with nulls
                // but COUNT function is always not nullable.
                // so wrap COUNT with Nvl to ensure its result is 0 instead of null to get the correct result
                if (correlatedOuterExpr.isPresent()) {
                    List<NamedExpression> aggFunctions = ScalarSubquery.getTopLevelScalarAggFunctions(
                            subquery.getQueryPlan(), subquery.getCorrelateSlots());
                    SubQueryRewriteResult result = addNvlForScalarSubqueryOutput(aggFunctions, subqueryOutput,
                            subquery, correlatedOuterExpr);
                    subqueryOutput = result.subqueryOutput;
                    subquery = result.subquery;
                    newCorrelatedOuterExpr = result.correlatedOuterExpr;
                }
            } else {
                // if scalar subquery doesn't have top level scalar agg we will create one, for example
                // select (select t2.c1 from t2 where t2.c2 = t1.c2) from t1;
                // the original output of the correlate subquery is t2.c1, after adding a scalar agg, it will be
                // select (select count(*), any_value(t2.c1) from t2 where t2.c2 = t1.c2) from t1;
                Alias anyValueAlias = new Alias(new AnyValue(subqueryOutput));
                LogicalAggregate<Plan> aggregate;
                if (((ScalarSubquery) subquery).limitOneIsEliminated()) {
                    aggregate = new LogicalAggregate<>(ImmutableList.of(),
                            ImmutableList.of(anyValueAlias), subquery.getQueryPlan());
                } else {
                    Alias countAlias = new Alias(new Count());
                    countSlot = countAlias.toSlot();
                    aggregate = new LogicalAggregate<>(ImmutableList.of(),
                            ImmutableList.of(countAlias, anyValueAlias), subquery.getQueryPlan());
                }
                anyValueSlot = anyValueAlias.toSlot();
                subquery = subquery.withSubquery(aggregate);
                if (correlatedOuterExpr.isPresent()) {
                    Map<Expression, Expression> replaceMap = new HashMap<>();
                    replaceMap.put(subqueryOutput, anyValueSlot);
                    newCorrelatedOuterExpr = Optional.of(ExpressionUtils.replace(correlatedOuterExpr.get(),
                            replaceMap));
                }
                needRuntimeAnyValue = true;
            }
        }
        // 根据不同 SubqueryExpr 类型（IN/EXISTS/SCALAR）设置 Apply 的行为
        LogicalApply.SubQueryType subQueryType;
        boolean isNot = false;
        Optional<Expression> compareExpr = Optional.empty();
        if (subquery instanceof InSubquery) {
            subQueryType = LogicalApply.SubQueryType.IN_SUBQUERY;
            isNot = ((InSubquery) subquery).isNot();
            compareExpr = Optional.of(((InSubquery) subquery).getCompareExpr());
        } else if (subquery instanceof Exists) {
            subQueryType = LogicalApply.SubQueryType.EXITS_SUBQUERY;
            isNot = ((Exists) subquery).isNot();
        } else if (subquery instanceof ScalarSubquery) {
            subQueryType = LogicalApply.SubQueryType.SCALAR_SUBQUERY;
        } else {
            throw new AnalysisException(String.format("Unsupported subquery : %s", subquery.toString()));
        }
        LogicalApply newApply = new LogicalApply(
                subquery.getCorrelateSlots(),
                subQueryType, isNot, compareExpr, subquery.getTypeCoercionExpr(), Optional.empty(),
                markJoinSlot,
                needAddScalarSubqueryOutputToProjects, isMarkJoinSlotNotNull,
                childPlan, subquery.getQueryPlan());

        // 构造最终 Project 的列：左子树输出 + mark 列 + 子查询产物（any_value / count / assert_true 等）
        ImmutableList.Builder<NamedExpression> projects =
                ImmutableList.builderWithExpectedSize(childPlan.getOutput().size() + 3);
        // left child
        projects.addAll(childPlan.getOutput());
        // markJoinSlotReference
        markJoinSlot.map(projects::add);
        LogicalProject logicalProject;
        if (needAddScalarSubqueryOutputToProjects) {
            if (needRuntimeAnyValue) {
                // if we create a new subquery in previous step, we need add the any_value() and assert_true()
                // into the project list. So BE will use assert_true to check if the subquery return only 1 row
                projects.add(anyValueSlot);
                if (countSlot != null) {
                    List<NamedExpression> upperProjects = new ArrayList<>();
                    upperProjects.addAll(projects.build());
                    projects.add(new Alias(new AssertTrue(
                            ExpressionUtils.or(new IsNull(countSlot),
                                    new LessThanEqual(countSlot, new BigIntLiteral(1))),
                            new VarcharLiteral("correlate scalar subquery must return only 1 row"))));
                    logicalProject = new LogicalProject(projects.build(), newApply);
                } else {
                    logicalProject = new LogicalProject(projects.build(), newApply);
                }
            } else {
                projects.add(subqueryOutput);
                logicalProject = new LogicalProject(projects.build(), newApply);
            }
        } else {
            logicalProject = new LogicalProject(projects.build(), newApply);
        }

        return Pair.of(logicalProject, newCorrelatedOuterExpr);
    }

    /**
     * SubQueryRewriteResult
     */
    @VisibleForTesting
    protected class SubQueryRewriteResult {
        public SubqueryExpr subquery;
        public NamedExpression subqueryOutput;
        public Optional<Expression> correlatedOuterExpr;

        public SubQueryRewriteResult(SubqueryExpr subquery, NamedExpression subqueryOutput,
                                     Optional<Expression> correlatedOuterExpr) {
            this.subquery = subquery;
            this.subqueryOutput = subqueryOutput;
            this.correlatedOuterExpr = correlatedOuterExpr;
        }
    }

    /**
     * for correlated scalar subquery like select c1, (select count(c1) from t2 where t1.c2 = t2.c2) as c from t1
     * if we don't add extra nvl for not nullable agg functions, the plan will be like bellow:
     * +--LogicalProject(projects=[c1#0, count(c1)#4 AS `c`#5])
     *    +--LogicalJoin(type=LEFT_OUTER_JOIN, hashJoinConjuncts=[(c2#1 = c2#3)])
     *       |--LogicalOlapScan (t1)
     *       +--LogicalAggregate[108] (groupByExpr=[c2#3], outputExpr=[c2#3, count(c1#2) AS `count(c1)`#4])
     *          +--LogicalOlapScan (t2)
     *
     * the count(c1)#4 may be null because of unmatched row of left outer join, but count is not nullable agg function,
     * it should never be null, we need use nvl to wrap it and change the plan like bellow:
     * +--LogicalProject(projects=[c1#0, ifnull(count(c1)#4, 0) AS `c`#5])
     *    +--LogicalJoin(type=LEFT_OUTER_JOIN, hashJoinConjuncts=[(c2#1 = c2#3)])
     *       |--LogicalOlapScan (t1)
     *       +--LogicalAggregate[108] (groupByExpr=[c2#3], outputExpr=[c2#3, count(c1#2) AS `count(c1)`#4])
     *          +--LogicalOlapScan (t2)
     *
     * in order to do that, we need change subquery's output and replace the correlated outer expr
     */
    @VisibleForTesting
    protected SubQueryRewriteResult addNvlForScalarSubqueryOutput(List<NamedExpression> aggFunctions,
                                                                NamedExpression subqueryOutput,
                                                                SubqueryExpr subquery,
                                                                Optional<Expression> correlatedOuterExpr) {
        SubQueryRewriteResult result = new SubQueryRewriteResult(subquery, subqueryOutput, correlatedOuterExpr);
        Map<Expression, Expression> replaceMapForSubqueryProject = new HashMap<>();
        Map<Expression, Expression> replaceMapForCorrelatedOuterExpr = new HashMap<>();
        for (NamedExpression agg : aggFunctions) {
            if (agg instanceof Alias && ((Alias) agg).child() instanceof NotNullableAggregateFunction) {
                NotNullableAggregateFunction notNullableAggFunc =
                        (NotNullableAggregateFunction) ((Alias) agg).child();
                if (subquery.getQueryPlan() instanceof LogicalProject) {
                    // if the top node of subquery is LogicalProject, we need replace the agg slot in
                    // project list by nvl(agg), and this project will be placed above LogicalApply node
                    Slot aggSlot = agg.toSlot();
                    replaceMapForSubqueryProject.put(aggSlot, new Alias(new Nvl(aggSlot,
                            notNullableAggFunc.resultForEmptyInput())));
                } else {
                    replaceMapForCorrelatedOuterExpr.put(subqueryOutput, new Nvl(subqueryOutput,
                            notNullableAggFunc.resultForEmptyInput()));
                }
            }
        }
        if (!replaceMapForSubqueryProject.isEmpty()) {
            Preconditions.checkState(subquery.getQueryPlan() instanceof LogicalProject,
                    "Scalar subquery's top plan node should be LogicalProject");
            LogicalProject logicalProject =
                    (LogicalProject) subquery.getQueryPlan();
            Preconditions.checkState(logicalProject.getOutputs().size() == 1,
                    "Scalar subuqery's should only output 1 column");
            NamedExpression newOutput = (NamedExpression) ExpressionUtils
                    .replace((NamedExpression) logicalProject.getProjects().get(0),
                            replaceMapForSubqueryProject);
            replaceMapForCorrelatedOuterExpr.put(subqueryOutput, newOutput.toSlot());
            result.subqueryOutput = newOutput;
            // logicalProject will be placed above LogicalApply later, so we remove it from subquery
            result.subquery = subquery.withSubquery((LogicalPlan) logicalProject.child());
        }
        if (!replaceMapForCorrelatedOuterExpr.isEmpty()) {
            result.correlatedOuterExpr = Optional.of(ExpressionUtils.replace(correlatedOuterExpr.get(),
                    replaceMapForCorrelatedOuterExpr));
        }
        return result;
    }

    private boolean isScalarSubqueryOutputUsedInOuterScope(
            SubqueryExpr subqueryExpr, Optional<Expression> correlatedOuterExpr) {
        return subqueryExpr instanceof ScalarSubquery
            && ((correlatedOuterExpr.isPresent()
                && ((ImmutableSet) correlatedOuterExpr.get().collect(SlotReference.class::isInstance))
                    .contains(subqueryExpr.getQueryPlan().getOutput().get(0))));
    }

    /**
     * The Subquery in the LogicalFilter will change to LogicalApply, so we must replace the origin Subquery.
     * LogicalFilter(predicate(contain subquery)) -> LogicalFilter(predicate(not contain subquery)
     * Replace the subquery in logical with the relevant expression.
     *
     * The replacement rules are as follows (表达式层面的替换)：
     * before:
     *      1.filter(t1.a = scalarSubquery(output b));
     *      2.filter(inSubquery);   inSubquery = (t1.a in select ***);
     *      3.filter(exists);   exists = (select ***);
     *
     * after:
     *      1.filter(t1.a = b);
     *      2.isMarkJoin ? filter(MarkJoinSlotReference) : filter(True);
     *      3.isMarkJoin ? filter(MarkJoinSlotReference) : filter(True);
     *
     * 配合上层规则（FILTER_SUBQUERY_TO_APPLY）后，整个 plan 树的大致变化可以画成：
     *
     * 1）标量子查询（WHERE t1.a > (SELECT MAX(t2.a) FROM t2 WHERE t2.b = t1.b)）
     *
     *    before:
     *        LogicalFilter(predicate: t1.a > scalarSubquery)
     *              |
     *           childPlan
     *
     *    ReplaceSubquery 之后（表达式里不再有 SubqueryExpr）:
     *        LogicalFilter(predicate: t1.a > subqueryOutputSlot)
     *              |
     *           childPlan
     *
     *    再经过 subqueryToApply/addApply 之后（真正挂上 Apply）:
     *        LogicalProject([t1.* ... subqueryOutputSlot])
     *              |
     *        LogicalFilter(predicate: t1.a > subqueryOutputSlot)
     *              |
     *        LogicalApply(correlation: t1.b = t2.b, subQueryType=SCALAR_SUBQUERY)
     *            /                               \
     *       childPlan(t1)                   subqueryPlan(t2 ... MAX(t2.a))
     *
     * 2）IN 子查询（WHERE t1.a IN (SELECT t2.a FROM t2 WHERE ...)）
     *
     *    before:
     *        LogicalFilter(predicate: t1.a IN (SELECT ...))
     *              |
     *           childPlan
     *
     *    ReplaceSubquery 之后（若需要 mark join）:
     *        LogicalFilter(predicate: markSlot)   // 表达式中用 markSlot 替代表达式级的 IN 子查询
     *              |
     *           childPlan
     *
     *    addApply 之后:
     *        LogicalProject([t1.* ... markSlot])
     *              |
     *        LogicalFilter(predicate: markSlot)
     *              |
     *        LogicalApply(subQueryType=IN_SUBQUERY, markJoinSlotReference=markSlot, compareExpr=t1.a)
     *            /                         \
     *       childPlan(t1)             subqueryPlan(t2 ...)
     *
     * 3）EXISTS 子查询（WHERE EXISTS (SELECT ... FROM t2 WHERE t2.b = t1.b)）
     *
     *    before:
     *        LogicalFilter(predicate: EXISTS(subquery))
     *              |
     *           childPlan
     *
     *    ReplaceSubquery 之后（需要 mark 时）:
     *        LogicalFilter(predicate: nvl(markSlot, FALSE))
     *              |
     *           childPlan
     *
     *    addApply 之后:
     *        LogicalProject([t1.* ... markSlot])
     *              |
     *        LogicalFilter(predicate: nvl(markSlot, FALSE))
     *              |
     *        LogicalApply(subQueryType=EXITS_SUBQUERY, markJoinSlotReference=markSlot)
     *            /                         \
     *       childPlan(t1)             subqueryPlan(t2 ...)
     *
     * 也就是说：本类只负责“把表达式里的 SubqueryExpr 换成 slot/mark/常量”，
     * 真正的 plan 树结构变化（加 LogicalApply/LogicalProject）由 buildRules 里的规则完成。
     */
    private static class ReplaceSubquery extends DefaultExpressionRewriter<SubqueryContext> {
        // 用于生成列名等 SQL 级别上下文信息
        private final StatementContext statementContext;
        // 当前是否处在需要 mark join 语义的 OR 谓词中（由 visitCompoundPredicate 更新）
        private boolean isMarkJoin;

        // 外部传入：这一整条谓词/投影是否“应该输出” mark join slot
        private final boolean shouldOutputMarkJoinSlot;

        public ReplaceSubquery(StatementContext statementContext,
                               boolean shouldOutputMarkJoinSlot) {
            this.statementContext = Objects.requireNonNull(statementContext, "statementContext can't be null");
            this.shouldOutputMarkJoinSlot = shouldOutputMarkJoinSlot;
        }

        public Expression replace(Expression expression, SubqueryContext subqueryContext) {
            // 入口：对一棵表达式树做重写，具体逻辑在各个 visitXxx 中
            return expression.accept(this, subqueryContext);
        }

        @Override
        public Expression visitExistsSubquery(Exists exists, SubqueryContext context) {
            // The result set when NULL is specified in the subquery and still evaluates to TRUE by using EXISTS
            // When the number of rows returned is empty, agg will return null, so if there is more agg,
            // it will always consider the returned result to be true
            if (hasTopLevelScalarAgg(exists.getQueryPlan())) {
                /*
                top level scalar agg and always return a value or null for empty input
                so Exists and Not Exists conjunct are always evaluated to True and False literals respectively
                    SELECT *
                    FROM t1
                    WHERE EXISTS (
                            SELECT SUM(a)
                            FROM t2
                            WHERE t1.a = t2.b and t1.a = 1;
                        );
                 */
                // 情况一：子查询顶层为标量聚合（始终返回 1 行：值 or null），
                // 此时 EXISTS/NOT EXISTS 的结果可以直接折叠为 TRUE/FALSE 常量，不再生成 Apply。
                return exists.isNot() ? BooleanLiteral.FALSE : BooleanLiteral.TRUE;
            } else {
                // 情况二：普通 EXISTS 子查询，需要决定是否为其生成 mark 列
                // isMarkJoin 表示由 OR 语义推导需要 mark；shouldOutputMarkJoinSlot 表示上层需要用到 mark 列
                boolean needCreateMarkJoinSlot = isMarkJoin || shouldOutputMarkJoinSlot;
                if (needCreateMarkJoinSlot) {
                    // 为当前 EXISTS 创建一个新的 mark join 列
                    MarkJoinSlotReference markJoinSlotReference =
                            new MarkJoinSlotReference(statementContext.generateColumnName());
                    // 记录“这个 SubqueryExpr 对应哪个 mark 列”，供后续构造 Apply 使用
                    context.setSubqueryToMarkJoinSlot(exists, Optional.of(markJoinSlotReference));
                    // 在表达式里用 NVL(markSlot, FALSE) 替换掉 EXISTS(...)，
                    // 防止外连接下 markSlot 变 NULL 时语义出错。
                    return new Nvl(markJoinSlotReference, BooleanLiteral.FALSE);
                } else {
                    // 不需要 mark 列时，EXISTS 在当前 Filter 谓词中直接当 TRUE 使用，
                    // 真正的存在性语义由 Apply/Join 结构保证。
                    return BooleanLiteral.TRUE;
                }
            }
        }

        @Override
        public Expression visitInSubquery(InSubquery in, SubqueryContext context) {
            // 先创建一个潜在要用到的 mark 列
            MarkJoinSlotReference markJoinSlotReference =
                    new MarkJoinSlotReference(statementContext.generateColumnName());
            // 只要 OR 语义或上层要求输出 mark 列，则需要为该 IN 子查询创建 mark slot
            boolean needCreateMarkJoinSlot = isMarkJoin || shouldOutputMarkJoinSlot;
            if (needCreateMarkJoinSlot) {
                // 记录该 IN 子查询对应的 mark 列
                context.setSubqueryToMarkJoinSlot(in, Optional.of(markJoinSlotReference));
            }
            // 表达式中用 mark 列或 TRUE 替换 IN 子查询：
            // - 需要 mark：返回 markSlot；
            // - 不需要 mark：返回 TRUE（由 Apply/Join 负责真正的 IN 语义）。
            return needCreateMarkJoinSlot ? markJoinSlotReference : BooleanLiteral.TRUE;
        }

        @Override
        public Expression visitScalarSubquery(ScalarSubquery scalar, SubqueryContext context) {
            // 标量子查询直接替换为其输出 slot（内部已处理 nullable/any_value 等细节）
            return scalar.getSubqueryOutput();
        }

        @Override
        public Expression visitCompoundPredicate(CompoundPredicate compound, SubqueryContext context) {
            // update isMarkJoin flag
            if (compound instanceof Or) {
                // 若当前为 OR 谓词，且任一子表达式中包含子查询，
                // 则后续 IN/EXISTS 替换时需要开启 mark join 语义。
                for (Expression child : compound.children()) {
                    if (child.anyMatch(SubqueryExpr.class::isInstance)) {
                        isMarkJoin = true;
                        break;
                    }
                }
            }
            // 对 OR/AND 的每个子表达式递归调用 replace，保持结构不变，仅替换内部的 SubqueryExpr
            return compound.withChildren(
                    compound.children().stream().map(c -> replace(c, context)).collect(Collectors.toList())
            );
        }
    }

    /**
     * subqueryToMarkJoinSlot: The markJoinSlot corresponding to each subquery.
     * rule:
     * For inSubquery and exists: it will be directly replaced by markSlotReference
     *  e.g.
     *  logicalFilter(predicate=exists) ---> logicalFilter(predicate=$c$1)
     * For scalarSubquery: it will be replaced by scalarSubquery's output slot
     *  e.g.
     *  logicalFilter(predicate=k1 > scalarSubquery) ---> logicalFilter(predicate=k1 > $c$1)
     *
     * subqueryCorrespondingConjunct: Record the conject corresponding to the subquery.
     * rule:
     *
     *
     */
    private static class SubqueryContext {
        private final Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot;

        public SubqueryContext(Set<SubqueryExpr> subqueryExprs) {
            this.subqueryToMarkJoinSlot = new LinkedHashMap<>(subqueryExprs.size());
            subqueryExprs.forEach(subqueryExpr -> subqueryToMarkJoinSlot.put(subqueryExpr, Optional.empty()));
        }

        private Map<SubqueryExpr, Optional<MarkJoinSlotReference>> getSubqueryToMarkJoinSlot() {
            return subqueryToMarkJoinSlot;
        }

        private void setSubqueryToMarkJoinSlot(SubqueryExpr subquery,
                                              Optional<MarkJoinSlotReference> markJoinSlotReference) {
            subqueryToMarkJoinSlot.put(subquery, markJoinSlotReference);
        }

    }

    private List<Boolean> shouldOutputMarkJoinSlot(Collection<Expression> conjuncts) {
        ImmutableList.Builder<Boolean> result = ImmutableList.builderWithExpectedSize(conjuncts.size());
        for (Expression expr : conjuncts) {
            result.add(!(expr instanceof SubqueryExpr) && expr.containsType(SubqueryExpr.class));
        }
        return result.build();
    }

    // 遍历一组表达式，收集每个表达式中出现的 SubqueryExpr 集合，并标记整体上是否存在子查询
    private CollectSubquerys collectSubquerys(Collection<? extends Expression> exprs) {
        // 标记：整组表达式中是否至少出现过一个 SubqueryExpr
        boolean hasSubqueryExpr = false;
        // subqueryExprsListBuilder：按顺序记录每个表达式里的 SubqueryExpr 集合
        ImmutableList.Builder<Set<SubqueryExpr>> subqueryExprsListBuilder = ImmutableList.builder();
        for (Expression expression : exprs) {
            // 找出当前 expression 中所有的 SubqueryExpr
            Set<SubqueryExpr> subqueries = expression.collect(SubqueryExpr.class::isInstance);
            // 只要某个表达式中非空，就说明全局含有子查询
            hasSubqueryExpr |= !subqueries.isEmpty();
            // 把当前表达式的子查询集合加入列表（可能为空集合，但位置要占住以保证下标对齐）
            subqueryExprsListBuilder.add(subqueries);
        }
        // 返回封装好的 CollectSubquerys：包含每个表达式对应的子查询集合 + 是否存在子查询的总标记
        return new CollectSubquerys(subqueryExprsListBuilder.build(), hasSubqueryExpr);
    }

    private static class CollectSubquerys {
        final List<Set<SubqueryExpr>> subqueies;
        final boolean hasSubquery;

        public CollectSubquerys(List<Set<SubqueryExpr>> subqueies, boolean hasSubquery) {
            this.subqueies = subqueies;
            this.hasSubquery = hasSubquery;
        }
    }
}
