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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer.ExpressionReplaceContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ExpressionLineageReplacer：表达式血缘追踪和替换器
 * 
 * 核心功能：将表达式中的 NamedExpression（别名、中间列）回溯到底层表达式（基表列或完整表达式），
 * 实现表达式的 lineage（血缘）追踪。
 * 
 * ==================== 完整 SQL 示例 ====================
 * 
 * SQL:
 *   SELECT a + 10 as a1, d FROM (
 *     SELECT b - 5 as a, d FROM orders
 *   );
 * 
 * 计划树：
 *   LogicalProject(a1#7 = a#5 + 10, d#8 = d#6)
 *     LogicalProject(a#5 = b#1 - 5, d#6 = d#0)
 *       LogicalOlapScan(orders: b#1, d#0)
 * 
 * 假设我们要 shuttle 表达式: [a#5 + 10, d#8]
 * 
 * 【阶段1：初始化】
 *   targetExpressions = [a#5 + 10, d#8]
 *   从目标表达式树中递归收集 NamedExpression：
 *     - a#5 + 10 中包含 a#5
 *     - d#8 就是 d#8
 *   usedExprIdSet = {a#5, d#8}
 *   exprIdExpressionMap = {}
 * 
 * 【阶段2：收集映射关系】（遍历计划树，从根向下）
 * 
 *   遍历到 LogicalProject(a#5 = b#1 - 5, d#6 = d#0):
 *     - 发现 a#5 在 usedExprIdSet 中
 *       → visitAlias: 建立映射 {a#5 -> b#1 - 5}
 *       → visitNamedExpression: 将 a#5 添加到 usedExprIdSet（已存在）
 *       → 遍历 b#1 - 5 表达式，发现其中的 b#1
 *       → visitNamedExpression: 将 b#1 添加到 usedExprIdSet
 *     - 发现 d#8 不在当前节点，继续向下
 *     - usedExprIdSet = {a#5, d#8, b#1}
 *     - exprIdExpressionMap = {a#5 -> b#1 - 5}
 * 
 *   遍历到 LogicalProject(a1#7 = a#5 + 10, d#8 = d#6):
 *     - 发现 d#8 在 usedExprIdSet 中
 *       → visitAlias: 建立映射 {d#8 -> d#6}
 *       → 遍历 d#6，发现它是 SlotReference
 *       → visitNamedExpression: 将 d#6 添加到 usedExprIdSet
 *     - usedExprIdSet = {a#5, d#8, b#1, d#6}
 *     - exprIdExpressionMap = {a#5 -> b#1 - 5, d#8 -> d#6}
 * 
 *   继续向下，到 LogicalOlapScan:
 *     - d#6 在 usedExprIdSet 中，但 d#6 是中间 LogicalProject 的输出 SlotReference，
 *       而不是基表的 SlotReference（基表的是 d#0）
 *     - 由于 d#6 本身是 SlotReference（不是 Alias），不会建立 d#6 -> d#0 的映射
 *     - 最终 exprIdExpressionMap = {a#5 -> b#1 - 5, d#8 -> d#6}
 *       （注意：虽然 d#6 在逻辑上对应基表列 d#0，但由于 d#6 是 SlotReference 而不是 Alias，
 *        所以不会建立映射关系。shuttle 的结果是回溯到中间层级，而不是最底层的基表列）
 * 
 * 【阶段3：替换表达式】
 *   ExpressionReplacer 遍历目标表达式：
 *     - a#5 + 10:
 *       * 查找 a#5 → 找到映射 {a#5 -> b#1 - 5}
 *       * 替换为 (b#1 - 5) + 10
 *       * 递归处理 (b#1 - 5) + 10，b#1 没有映射（是基表列），保持不变
 *     - d#8:
 *       * 查找 d#8 → 找到映射 {d#8 -> d#6}
 *       * 替换为 d#6
 *       * d#6 是 SlotReference，无需进一步替换
 * 
 *   最终结果：
 *     replacedExpressions = [(b#1 - 5) + 10, d#6]
 * 
 *   说明：最终结果中的 slot 含义
 *     - b#1: 基表 orders 的列 b，ExprId 为 1（来自 LogicalOlapScan(orders: b#1, d#0)）
 *     - d#6: 中间 LogicalProject 的输出列 d，ExprId 为 6
 *            （在计划树中，d#6 = d#0 表示 LogicalProject 直接传递了基表列 d#0，
 *             但创建了一个新的 SlotReference d#6 作为输出）
 *            - 虽然 d#6 在逻辑上对应基表列 d#0，但由于 d#6 本身是 SlotReference（不是 Alias），
 *              所以没有建立 d#6 -> d#0 的映射关系
 *            - shuttle 的结果保留为 d#6，而不是继续回溯到 d#0
 *            - 这说明 shuttle 的目标是将表达式统一到某个层级（通常是子查询的输出层级），
 *              而不是必须回溯到最底层的基表列
 * 
 * ==================== 完整运作流程（抽象说明）====================
 * 
 * 入口：ExpressionUtils.shuttleExpressionWithLineage(expressions, plan)
 * 
 * 阶段1：初始化（ExpressionReplaceContext 构造函数）
 *   - 输入：目标表达式列表
 *   - 递归收集目标表达式树中的所有 NamedExpression 的 ExprId
 *   - 初始化空的映射表
 * 
 * 阶段2：收集映射关系（plan.accept(INSTANCE, replaceContext)）
 *   - ExpressionLineageReplacer.visit()：遍历计划树（从根节点向下）
 *   - NamedExpressionCollector.visitAlias()：建立别名到底层表达式的映射
 *   - NamedExpressionCollector.visitNamedExpression()：收集映射表达式中的 NamedExpression
 *   - 逐步填充 exprIdExpressionMap
 * 
 * 阶段3：替换表达式（replaceContext.getReplacedExpressions()）
 *   - ExpressionReplacer：根据映射表递归替换目标表达式中的 NamedExpression
 *   - 返回替换后的表达式列表
 * 
 * ==================== 关键组件说明 ====================
 * 
 * 1. ExpressionLineageReplacer（主访问器）
 *    - 继承 DefaultPlanVisitor，遍历计划树
 *    - visit() 方法：在计划节点中查找需要收集的 NamedExpression
 * 
 * 2. NamedExpressionCollector（表达式收集器）
 *    - visitNamedExpression()：收集 NamedExpression 的 ExprId，以便继续追踪
 *    - visitAlias()：专门处理 Alias，建立别名到底层表达式的映射
 * 
 * 3. ExpressionReplacer（表达式替换器）
 *    - visitNamedExpression()：根据映射表替换 NamedExpression
 *    - visit()：递归替换表达式树中的所有子表达式
 * 
 * 4. ExpressionReplaceContext（上下文）
 *    - targetExpressions：输入的目标表达式列表
 *    - usedExprIdSet：需要追踪的 NamedExpression 的 ExprId 集合（起点 + 路径中的中间节点）
 *    - exprIdExpressionMap：ExprId 到底层表达式的映射表
 *    - replacedExpressions：替换后的表达式列表（延迟计算）
 * 
 * ==================== 使用场景 ====================
 * 
 * 主要用于物化视图重写：
 * - 查询表达式可能包含别名或中间列引用（如 create_time_alias#5）
 * - MV 表达式是基于基表列的（如 create_time#0）
 * - 需要将查询表达式 shuttle 到基表列空间，才能与 MV 表达式匹配
 * 
 * Get from rewrite plan and can also get from plan struct info, if from plan struct info it depends on
 * the nodes from graph.
 */
public class ExpressionLineageReplacer extends DefaultPlanVisitor<Expression, ExpressionReplaceContext> {

    public static final Logger LOG = LogManager.getLogger(ExpressionLineageReplacer.class);
    public static final ExpressionLineageReplacer INSTANCE = new ExpressionLineageReplacer();

    /**
     * 遍历计划节点，收集目标表达式中使用的 NamedExpression 及其对应的底层表达式。
     * 
     * 处理流程：
     * 1. 获取目标表达式中使用的所有 NamedExpression 的 ExprId 集合（usedExprIdSet）
     * 2. 遍历当前计划节点中的所有表达式
     * 3. 如果表达式是 NamedExpression 且其 ExprId 在 usedExprIdSet 中（说明目标表达式使用了它）
     * 4. 调用 NamedExpressionCollector 收集该 NamedExpression 及其底层表达式，建立映射关系
     * 
     * 示例：
     * 目标表达式：a + 10（其中 a#5 是别名）
     * usedExprIdSet: {a#5}
     * 
     * 计划节点 LogicalProject(a#5 = b - 5, d#6 = d#0)：
     * - 发现 a#5 在 usedExprIdSet 中
     * - 收集映射：{a#5 -> b - 5}
     * 
     * 最终建立映射关系，用于将 a + 10 替换为 (b - 5) + 10
     */
    @Override
    public Expression visit(Plan plan, ExpressionReplaceContext context) {
        // 获取目标表达式中使用的所有 NamedExpression 的 ExprId 集合
        Set<ExprId> usedExprIdSet = context.getUsedExprIdSet();
        // 遍历当前计划节点中的所有表达式，找出目标表达式使用的 NamedExpression
        for (Expression expression : plan.getExpressions()) {
            // 只处理 NamedExpression（如别名、列引用等）
            if (!(expression instanceof NamedExpression)) {
                continue;
            }
            // 只收集目标表达式中实际使用的 NamedExpression（通过 ExprId 匹配）
            if (!usedExprIdSet.contains(((NamedExpression) expression).getExprId())) {
                continue;
            }
            // 收集该 NamedExpression 及其底层表达式，建立 ExprId -> 底层表达式 的映射
            // 例如：如果 expression 是 Alias(a#5, b - 5)，则建立 {a#5 -> b - 5} 的映射
            expression.accept(NamedExpressionCollector.INSTANCE, context);
        }
        // 继续遍历子计划节点
        return super.visit(plan, context);
    }

    @Override
    public Expression visitGroupPlan(GroupPlan groupPlan, ExpressionReplaceContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ExpressionLineageReplacer should not meet groupPlan, plan is {}", groupPlan.toString());
        }
        return null;
    }

    /**
     * Replace the expression with lineage according the exprIdExpressionMap
     */
    public static class ExpressionReplacer extends DefaultExpressionRewriter<Map<ExprId, Expression>> {

        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        @Override
        public Expression visitNamedExpression(NamedExpression namedExpression,
                Map<ExprId, Expression> exprIdExpressionMap) {
            Expression childExpr = exprIdExpressionMap.get(namedExpression.getExprId());
            // avoid loop when non_nullable(o_orderkey#0) AS `o_orderkey`#0 after join eliminate when
            // inner join
            if (childExpr != null && !childExpr.children().isEmpty()
                    && childExpr.child(0) instanceof NamedExpression
                    && ((NamedExpression) childExpr.child(0)).getExprId().equals(namedExpression.getExprId())) {
                return namedExpression;
            }
            if (childExpr != null) {
                // remove alias
                return visit(childExpr, exprIdExpressionMap);
            }
            return visit(namedExpression, exprIdExpressionMap);
        }

        @Override
        public Expression visit(Expression expr, Map<ExprId, Expression> exprIdExpressionMap) {
            if (expr instanceof NamedExpression
                    && expr.arity() == 0
                    && exprIdExpressionMap.containsKey(((NamedExpression) expr).getExprId())) {
                expr = exprIdExpressionMap.get(((NamedExpression) expr).getExprId());
            }
            List<Expression> newChildren = new ArrayList<>(expr.arity());
            boolean hasNewChildren = false;
            for (Expression child : expr.children()) {
                Expression newChild = child.accept(this, exprIdExpressionMap);
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            return hasNewChildren ? expr.withChildren(newChildren) : expr;
        }
    }

    /**
     * The Collector for named expressions in the whole plan, and will be used to
     * replace the target expression later
     */
    public static class NamedExpressionCollector
            extends DefaultExpressionVisitor<Void, ExpressionReplaceContext> {

        public static final NamedExpressionCollector INSTANCE = new NamedExpressionCollector();

        /**
         * 处理 NamedExpression（包括 Alias、SlotReference 等）。
         * 将当前 NamedExpression 的 ExprId 添加到 usedExprIdSet 中，用于追踪需要进一步收集底层表达式的 NamedExpression。
         * 
         * 例如：如果遇到 Alias(a#5, b - 5)，先将 a#5 添加到 usedExprIdSet，
         * 后续 visitAlias 方法会建立 {a#5 -> b - 5} 的映射。
         */
        @Override
        public Void visitNamedExpression(NamedExpression namedExpression, ExpressionReplaceContext context) {
            context.getUsedExprIdSet().add(namedExpression.getExprId());
            return super.visitNamedExpression(namedExpression, context);
        }

        /**
         * 专门处理 Alias（别名）表达式。
         * 如果该 Alias 的 ExprId 在 usedExprIdSet 中（说明目标表达式使用了它），
         * 则建立映射关系：alias 的 ExprId -> alias 的 child（去掉别名，获取底层表达式）。
         * 
         * 示例：
         * Alias: a#5 = b - 5（别名 a 对应表达式 b - 5）
         * 如果 a#5 在 usedExprIdSet 中（目标表达式使用了 a#5）
         * 则建立映射：{a#5 -> b - 5}
         * 
         * 后续替换时，可以将目标表达式中的 a#5 替换为 b - 5，实现回溯到底层表达式。
         */
        @Override
        public Void visitAlias(Alias alias, ExpressionReplaceContext context) {
            // 如果目标表达式使用了这个别名，建立映射关系：别名 -> 底层表达式
            if (context.getUsedExprIdSet().contains(alias.getExprId())) {
                // 建立映射：alias.getExprId() -> alias.child()（去掉别名，获取底层表达式）
                context.getExprIdExpressionMap().put(alias.getExprId(), alias.child());
            }
            return super.visitAlias(alias, context);
        }
    }

    /**
     * 表达式替换上下文，用于追踪和替换表达式的 lineage（血缘关系）。
     * 
     * ==================== 完整 SQL 示例 ====================
     * 
     * SQL:
     *   SELECT a + 10 as a1, d FROM (
     *     SELECT b - 5 as a, d FROM orders
     *   );
     * 
     * 计划树：
     *   LogicalProject(a1#7 = a + 10, d#8 = d#6)
     *     LogicalProject(a#5 = b - 5, d#6 = d#0)
     *       LogicalOlapScan(orders: b#1, d#0)
     * 
     * 假设我们要 shuttle 表达式: [a + 10, d#8]
     * 
     * 初始化阶段（构造函数）：
     *   targetExpressions = [a#5 + 10, d#8]
     *   usedExprIdSet = {a#5, d#8}  ← 提取表达式中的所有 NamedExpression 的 ExprId
     * 
     * 收集阶段（遍历计划树）：
     *   1. 遍历到 LogicalProject(a#5 = b - 5, d#6 = d#0)：
     *      - 发现 a#5 在 usedExprIdSet 中 → 建立映射 {a#5 -> b - 5}
     *      - 发现 d#8 不在当前节点，继续向下
     *   2. 遍历到 LogicalProject(a1#7 = a + 10, d#8 = d#6)：
     *      - 发现 d#8 在 usedExprIdSet 中 → 但 d#8 = d#6，需要继续追踪 d#6
     *   3. 最终建立映射：
     *      exprIdExpressionMap = {a#5 -> b - 5, d#8 -> d#6, d#6 -> d#0}
     * 
     * 替换阶段（getReplacedExpressions()）：
     *   - a#5 + 10 → (b - 5) + 10
     *   - d#8 → d#0
     *   replacedExpressions = [(b - 5) + 10, d#0]
     * 
     * ==================== 字段说明 ====================
     */
    public static class ExpressionReplaceContext {
        // targetExpressions: 需要 shuttle 的目标表达式列表（输入）
        // 示例：在 SQL "SELECT a + 10, d FROM ..." 中，如果 shuttle 的是 SELECT 列表的表达式
        // 则 targetExpressions = [a#5 + 10, d#8]
        private final List<? extends Expression> targetExpressions;
        
        // usedExprIdSet: 目标表达式中使用的所有 NamedExpression 的 ExprId 集合
        // 在构造函数中初始化：从 targetExpressions 中提取所有 NamedExpression 的 ExprId
        // 示例：targetExpressions = [a#5 + 10, d#8]
        //      usedExprIdSet = {a#5, d#8}  ← a#5 和 d#8 是表达式中的 NamedExpression
        // 作用：用于在遍历计划树时，只收集目标表达式实际使用的 NamedExpression（避免不必要的收集）
        private final Set<ExprId> usedExprIdSet = new HashSet<>();
        
        // exprIdExpressionMap: ExprId 到底层表达式的映射（收集阶段逐步构建）
        // key: NamedExpression 的 ExprId（如别名 a#5）
        // value: 对应的底层表达式（如 b - 5 或基表列 d#0）
        // 示例（基于上面的 SQL）：
        //   {a#5 -> b - 5}     ← 别名 a#5 对应的底层表达式是 b - 5
        //   {d#8 -> d#6}       ← 别名 d#8 对应的底层是 d#6
        //   {d#6 -> d#0}       ← d#6 对应的底层是基表列 d#0
        // 作用：用于后续将目标表达式中的 NamedExpression 替换为底层表达式
        private final Map<ExprId, Expression> exprIdExpressionMap = new HashMap<>();
        
        // replacedExpressions: shuttle 后的表达式列表（输出，延迟计算）
        // 在 getReplacedExpressions() 方法中首次调用时计算
        // 示例（基于上面的 SQL）：
        //   targetExpressions = [a#5 + 10, d#8]
        //   replacedExpressions = [(b - 5) + 10, d#0]  ← 替换后的结果
        private List<Expression> replacedExpressions;

        /**
         * ExpressionReplaceContext
         */
        public ExpressionReplaceContext(List<? extends Expression> targetExpressions) {
            this.targetExpressions = targetExpressions;
            // collect the named expressions used in target expression and will be replaced later
            for (Expression expression : targetExpressions) {
                for (Object namedExpression : expression.collectToList(NamedExpression.class::isInstance)) {
                    this.usedExprIdSet.add(((NamedExpression) namedExpression).getExprId());
                }
            }
        }

        public Map<ExprId, Expression> getExprIdExpressionMap() {
            return exprIdExpressionMap;
        }

        public Set<ExprId> getUsedExprIdSet() {
            return usedExprIdSet;
        }

        /**
         * getReplacedExpressions
         */
        public List<? extends Expression> getReplacedExpressions() {
            if (this.replacedExpressions == null) {
                this.replacedExpressions = targetExpressions.stream()
                        .map(original -> original.accept(ExpressionReplacer.INSTANCE, getExprIdExpressionMap()))
                        .collect(Collectors.toList());
            }
            return this.replacedExpressions;
        }
    }
}
