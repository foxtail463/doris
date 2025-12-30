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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.EquivalenceClassMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionOptimization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This record the predicates which can be pulled up or some other type predicates.
 * Also contains the necessary method for predicates process
 */
public class Predicates {

    // Predicates that can be pulled up
    private final Set<Expression> pulledUpPredicates;
    // Predicates that can not be pulled up, should be equals between query and view
    private final Set<Expression> couldNotPulledUpPredicates;

    public Predicates(Set<Expression> pulledUpPredicates, Set<Expression> couldNotPulledUpPredicates) {
        this.pulledUpPredicates = pulledUpPredicates;
        this.couldNotPulledUpPredicates = couldNotPulledUpPredicates;
    }

    public static Predicates of(Set<Expression> pulledUpPredicates, Set<Expression> predicatesUnderBreaker) {
        return new Predicates(pulledUpPredicates, predicatesUnderBreaker);
    }

    public Set<Expression> getPulledUpPredicates() {
        return pulledUpPredicates;
    }

    public Set<Expression> getCouldNotPulledUpPredicates() {
        return couldNotPulledUpPredicates;
    }

    public Predicates mergePulledUpPredicates(Collection<Expression> predicates) {
        Set<Expression> mergedPredicates = new HashSet<>(predicates);
        mergedPredicates.addAll(this.pulledUpPredicates);
        return new Predicates(mergedPredicates, this.couldNotPulledUpPredicates);
    }

    /**
     * Split the expression to equal, range and residual predicate.
     */
    public static SplitPredicate splitPredicates(Expression expression) {
        PredicatesSplitter predicatesSplit = new PredicatesSplitter(expression);
        return predicatesSplit.getSplitPredicate();
    }

    /**
     * 尝试处理“无法拉升”的谓词补偿。
     * - 逻辑：只有当 query 与 view 两侧都存在无法上提的谓词时才有比较意义；
     *   如果一侧为空、一侧非空，直接判定无法重写（返回 null）。
     * - 步骤：
     *   1) 取出两侧的 couldNotPulledUpPredicates。
     *   2) 若双方都为空，返回空补偿（成功）。
     *   3) 若仅一侧为空，另一侧非空，返回 null（失败）。
     *   4) 将 view 侧的不可上提谓词先做 shuttle（恢复到基表血缘），再用 slotMapping 映射到 query 列名，
     *      与 query 侧集合做等价性比较；完全相同则返回空补偿，否则返回 null。
     */
    public static Map<Expression, ExpressionInfo> compensateCouldNotPullUpPredicates(
            StructInfo queryStructInfo, StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping, ComparisonResult comparisonResult) {

        Predicates queryStructInfoPredicates = queryStructInfo.getPredicates();
        Predicates viewStructInfoPredicates = viewStructInfo.getPredicates();
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()
                && viewStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()) {
            return ImmutableMap.of();
        }
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()
                && !viewStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()) {
            return null;
        }
        if (!queryStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()
                && viewStructInfoPredicates.getCouldNotPulledUpPredicates().isEmpty()) {
            return null;
        }

        List<? extends Expression> viewPredicatesShuttled = ExpressionUtils.shuttleExpressionWithLineage(
                Lists.newArrayList(viewStructInfoPredicates.getCouldNotPulledUpPredicates()),
                viewStructInfo.getTopPlan(), new BitSet());
        List<Expression> viewPredicatesQueryBased = ExpressionUtils.replace((List<Expression>) viewPredicatesShuttled,
                viewToQuerySlotMapping.toSlotReferenceMap());
        // could not be pulled up predicates in query and view should be same
        if (queryStructInfoPredicates.getCouldNotPulledUpPredicates().equals(
                Sets.newHashSet(viewPredicatesQueryBased))) {
            return ImmutableMap.of();
        }
        return null;
    }

    /**
     * 等值谓词补偿：基于等价类校验/补齐查询缺少的等值关系。
     * 逻辑：
     * 1) 将视图等价类映射到查询侧 slot（permute），若映射失败返回 null
     * 2) 生成查询等价类 -> 视图等价类的映射，若视图等价类无法全部映射，返回 null
     * 3) 遍历查询等价类：
     *    - 如果视图没有对应等价类：补齐该查询等价类内部所有等值（如 {a,b,c} → a=b, a=c）
     *    - 如果视图有对应等价类但查询更大：补齐查询多出的等值（以视图等价类中的第一个 slot 为基准）
     * 4) 任意补偿含聚合则失败（返回 null）
     * 返回：需要补偿的等值谓词 map（表达式 -> EMPTY），为空表示无需补偿。
     */
    public static Map<Expression, ExpressionInfo> compensateEquivalence(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult) {
        EquivalenceClass queryEquivalenceClass = queryStructInfo.getEquivalenceClass(); // 查询等价类
        EquivalenceClass viewEquivalenceClass = viewStructInfo.getEquivalenceClass();   // 视图等价类
        Map<SlotReference, SlotReference> viewToQuerySlotMap = viewToQuerySlotMapping.toSlotReferenceMap(); // MV->Query 列映射
        EquivalenceClass viewEquivalenceClassQueryBased = viewEquivalenceClass.permute(viewToQuerySlotMap); // 视图等价类映射到查询侧
        if (viewEquivalenceClassQueryBased == null) {
            return null;
        }
        final Map<Expression, ExpressionInfo> equalCompensateConjunctions = new HashMap<>(); // 补偿结果
        if (queryEquivalenceClass.isEmpty() && viewEquivalenceClass.isEmpty()) { // 双空，直接返回空补偿
            return ImmutableMap.of();
        }
        if (queryEquivalenceClass.isEmpty() && !viewEquivalenceClass.isEmpty()) { // 视图有等价类，查询无，无法补偿
            return null;
        }
        EquivalenceClassMapping queryToViewEquivalenceMapping =
                EquivalenceClassMapping.generate(queryEquivalenceClass, viewEquivalenceClassQueryBased); // 生成查询->视图等价类映射
        // can not map all target equivalence class, can not compensate
        if (queryToViewEquivalenceMapping.getEquivalenceClassSetMap().size()
                < viewEquivalenceClass.getEquivalenceSetList().size()) {
            return null;
        }
        // ==================== 步骤3：执行等值谓词补偿 ====================
        // 获取能映射到视图的查询等价类集合（这些等价类在视图中也有对应的等价类）
        Set<List<SlotReference>> mappedQueryEquivalenceSet =
                queryToViewEquivalenceMapping.getEquivalenceClassSetMap().keySet();

        // 遍历查询中的每个等价类，判断是否需要补偿
        for (List<SlotReference> queryEquivalenceSet : queryEquivalenceClass.getEquivalenceSetList()) {
            
            // ==================== 情况1：查询等价类在视图中没有对应的等价类 ====================
            // 说明：查询有这个等价类，但视图没有，需要补偿整个查询等价类内部的所有等值关系
            // 
            // 示例：
            //   查询等价类：[a, b, c]（查询有 a=b, b=c）
            //   视图等价类：[]（视图没有等值关系）
            //   补偿：a=b, a=c（以第一个元素 a 为基准，与其他所有元素建立等值关系）
            if (!mappedQueryEquivalenceSet.contains(queryEquivalenceSet)) {
                Iterator<SlotReference> iterator = queryEquivalenceSet.iterator();
                SlotReference first = iterator.next(); // 选择第一个元素作为基准
                // 将第一个元素与其他所有元素建立等值关系
                while (iterator.hasNext()) {
                    Expression equals = new EqualTo(first, iterator.next());
                    // 如果补偿谓词包含聚合函数，无法补偿（返回 null）
                    if (equals.anyMatch(AggregateFunction.class::isInstance)) {
                        return null;
                    }
                    equalCompensateConjunctions.put(equals, ExpressionInfo.EMPTY);
                }
            } 
            // ==================== 情况2：查询等价类在视图中有对应的等价类，但查询的等价类更大 ====================
            // 说明：查询和视图都有这个等价类，但查询的等价类包含更多元素，需要补偿查询多出的部分
            // 
            // 示例：
            //   查询等价类：[a, b, c]（查询有 a=b, b=c）
            //   视图等价类：[a, b]（视图只有 a=b）
            //   补偿：b=c（查询多出的等值关系，以视图等价类中的第一个元素 a 为基准）
            //   注意：虽然查询等价类是 [a, b, c]，但补偿时使用视图等价类的第一个元素作为基准
            else {
                // 获取视图对应的等价类
                List<SlotReference> viewEquivalenceSet =
                        queryToViewEquivalenceMapping.getEquivalenceClassSetMap().get(queryEquivalenceSet);
                // 计算查询等价类中多出的元素（查询有但视图没有的）
                List<SlotReference> copiedQueryEquivalenceSet = new ArrayList<>(queryEquivalenceSet);
                copiedQueryEquivalenceSet.removeAll(viewEquivalenceSet);
                // 选择视图等价类中的第一个元素作为基准
                SlotReference first = viewEquivalenceSet.iterator().next();
                // 将视图等价类的第一个元素与查询多出的元素建立等值关系
                for (SlotReference slotReference : copiedQueryEquivalenceSet) {
                    Expression equals = new EqualTo(first, slotReference);
                    // 如果补偿谓词包含聚合函数，无法补偿（返回 null）
                    if (equals.anyMatch(AggregateFunction.class::isInstance)) {
                        return null;
                    }
                    equalCompensateConjunctions.put(equals, ExpressionInfo.EMPTY);
                }
            }
        }
        return equalCompensateConjunctions;
    }

    /**
     * 范围谓词补偿：查询与 MV 的 range 谓词差异对齐。
     * 步骤：
     * 1) 将 MV 的 range 谓词映射到查询侧 slot，去掉 TRUE
     * 2) 获取查询侧 range 谓词集合，去掉 TRUE
     * 3) 计算双向差集（查询缺的 + MV 缺的），若无差异直接返回空补偿
     * 4) 将差集 AND 后做规范化（rewrite），要求规范化结果必须被查询 range 集合包含，否则返回 null（视为无法补偿，避免放宽）
     * 5) 为规范化后的表达式构造补偿 map：Comparison 且唯一字面量时记录 literal，其余用 EMPTY；含聚合直接返回 null
     * 6) 返回补偿 map，空 map 表示无需补偿
     */
    public static Map<Expression, ExpressionInfo> compensateRangePredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult,
            CascadesContext cascadesContext) {
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        Set<Expression> viewRangeQueryBasedSet = new HashSet<>();
        for (Expression viewExpression : viewSplitPredicate.getRangePredicateMap().keySet()) {
            viewRangeQueryBasedSet.add(
                    ExpressionUtils.replace(viewExpression, viewToQuerySlotMapping.toSlotReferenceMap()));
        }
        viewRangeQueryBasedSet.remove(BooleanLiteral.TRUE);

        Set<Expression> queryRangeSet = querySplitPredicate.getRangePredicateMap().keySet();
        queryRangeSet.remove(BooleanLiteral.TRUE);

        Set<Expression> differentExpressions = new HashSet<>(); // 查询缺/视图缺的范围谓词
        Sets.difference(queryRangeSet, viewRangeQueryBasedSet).copyInto(differentExpressions);
        Sets.difference(viewRangeQueryBasedSet, queryRangeSet).copyInto(differentExpressions);
        // the range predicate in query and view is same, don't need to compensate
        if (differentExpressions.isEmpty()) {
            return ImmutableMap.of();
        }
        // try to normalize the different expressions（差集 AND 后 rewrite）
        Set<Expression> normalizedExpressions =
                normalizeExpression(ExpressionUtils.and(differentExpressions), cascadesContext);
        if (!queryRangeSet.containsAll(normalizedExpressions)) {
            // normalized expressions is not in query, can not compensate
            return null;
        }
         // 构造补偿表达式 map，key 是表达式，value 是 ExpressionInfo（包含字面量信息用于分区裁剪）
        Map<Expression, ExpressionInfo> normalizedExpressionsWithLiteral = new HashMap<>();
        for (Expression expression : normalizedExpressions) {
            // 收集表达式中的字面量
            Set<Literal> literalSet = expression.collect(expressionTreeNode -> expressionTreeNode instanceof Literal);
            // 以下情况不提取字面量信息（使用 EMPTY）：
            // 1. 不是比较谓词
            // 2. 是 GreaterThan 或 LessThanEqual（规范化后应该是 >= 或 <）
            // 3. 字面量数量不为 1（如 a BETWEEN 1 AND 10）
            if (!(expression instanceof ComparisonPredicate)
                    || (expression instanceof GreaterThan || expression instanceof LessThanEqual)
                    || literalSet.size() != 1) {
                if (expression.anyMatch(AggregateFunction.class::isInstance)) {
                    return null;
                }
                normalizedExpressionsWithLiteral.put(expression, ExpressionInfo.EMPTY);
                continue;
            }
            // 包含聚合函数的表达式不能作为补偿谓词
            if (expression.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            // 简单比较谓词（如 a >= 5），提取字面量用于分区裁剪优化
            normalizedExpressionsWithLiteral.put(expression, new ExpressionInfo(literalSet.iterator().next()));
        }
        return normalizedExpressionsWithLiteral;
    }

    private static Set<Expression> normalizeExpression(Expression expression, CascadesContext cascadesContext) {
        ExpressionNormalization expressionNormalization = new ExpressionNormalization();
        ExpressionOptimization expressionOptimization = new ExpressionOptimization();
        ExpressionRewriteContext context = new ExpressionRewriteContext(cascadesContext);
        expression = expressionNormalization.rewrite(expression, context);
        expression = expressionOptimization.rewrite(expression, context);
        return ExpressionUtils.extractConjunctionToSet(expression);
    }

    /**
     * 补偿残差谓词（Residual Predicates）
     * 
     * 【残差谓词的定义】
     * 在物化视图重写中，谓词被分为三类：
     * 
     * 1. 等值谓词（Equal Predicate）：
     *    - 列 = 列：如 a = b（两个列引用比较）
     *    - 列 = 字面量：如 status = 'active'（列与字面量比较）
     *    示例：WHERE a = b AND status = 'active'
     *          → equalPredicateMap: {a = b, status = 'active'}
     * 
     * 2. 范围谓词（Range Predicate）：
     *    - 列 > 字面量：如 age > 18
     *    - 列 < 字面量：如 price < 100
     *    - 列 >= 字面量：如 ds >= '202531'
     *    - 列 <= 字面量：如 score <= 100
     *    - 列 IN (字面量列表)：如 status IN ('active', 'pending')
     *    示例：WHERE age > 18 AND price < 100 AND status IN ('active', 'pending')
     *          → rangePredicateMap: {age > 18, price < 100, status IN ('active', 'pending')}
     * 
     * 3. 残差谓词（Residual Predicate）：
     *    所有无法归类为等值或范围的复杂表达式，包括：
     *    - OR 条件：如 ds >= '202531' OR ds = '202530'
     *    - 包含函数的表达式：如 col1 + col2 = 10, LENGTH(name) > 5
     *    - 复杂的比较：如 col1 + 1 > col2 * 2
     *    - 其他无法识别的表达式类型
     *    示例：WHERE (ds >= '202531' OR ds = '202530') AND col1 + col2 = 10
     *          → residualPredicateMap: {ds >= '202531' OR ds = '202530', col1 + col2 = 10}
     * 
     * 【分类规则】
     * 根据 PredicatesSplitter 的实现：
     * - 如果比较谓词的两边都是列引用，且是等值比较 → 等值谓词
     * - 如果比较谓词的一边是列引用，另一边是字面量 → 范围谓词
     * - 其他所有情况（OR、函数、复杂表达式等）→ 残差谓词
     * 
     * 【为什么需要残差谓词】
     * 残差谓词通常更复杂，难以进行简单的包含性检查。例如：
     * - OR 条件需要检查查询是否匹配 OR 的至少一个分支
     * - 包含函数的表达式需要检查函数是否等价
     * - 这些检查比简单的等值或范围比较更复杂
     * 
     * 该方法用于检查查询的残差谓词是否能够覆盖物化视图的残差谓词，并返回需要额外补偿的谓词。
     * 
     * 注意：当前实现是简化版本（TODO），存在以下限制：
     * 1. 要求查询的残差谓词集合必须完全包含物化视图的残差谓词集合
     * 2. 不支持 OR 条件的部分匹配（例如：物化视图有 OR 条件，查询只匹配 OR 条件中的部分分支）
     * 3. 如果物化视图包含 OR 条件，而查询只包含简单谓词，则无法重写
     * 
     * 示例场景 1（当前无法重写）：
     * - 物化视图：SELECT * FROM table1 WHERE ds >= '202531' OR ds = '202530'
     *   - 残差谓词：{ds >= '202531' OR ds = '202530'}
     * - 查询：SELECT * FROM table1 WHERE ds = '202530'
     *   - 残差谓词：{}（空，因为 ds = '202530' 被归类为等值或范围谓词）
     * - 结果：返回 null，无法重写
     *   - 原因：queryResidualSet.containsAll(viewResidualQueryBasedSet) 为 false
     *   - 查询的残差谓词集合为空，不包含物化视图的 OR 条件
     * 
     * 示例场景 2（可以重写）：
     * - 物化视图：SELECT * FROM table1 WHERE ds >= '202531' OR ds = '202530'
     *   - 残差谓词：{ds >= '202531' OR ds = '202530'}
     * - 查询：SELECT * FROM table1 WHERE (ds >= '202531' OR ds = '202530') AND status = 'active'
     *   - 残差谓词：{ds >= '202531' OR ds = '202530'}
     * - 结果：返回 {status = 'active'}，可以重写
     *   - 原因：查询的残差谓词完全包含物化视图的残差谓词
     *   - 需要补偿：status = 'active'（查询有但物化视图没有）
     *   - 重写后的查询：SELECT * FROM mv WHERE status = 'active'
     * 
     * 示例场景 3（无法重写 - 包含聚合函数）：
     * - 物化视图：SELECT * FROM table1 WHERE ds >= '202531' OR ds = '202530'
     * - 查询：SELECT * FROM table1 WHERE (ds >= '202531' OR ds = '202530') AND COUNT(*) > 100
     *   - 残差谓词：{ds >= '202531' OR ds = '202530', COUNT(*) > 100}
     * - 结果：返回 null，无法重写
     *   - 原因：补偿谓词 COUNT(*) > 100 包含聚合函数，无法在物化视图扫描后通过简单过滤补偿
     * 
     * @param queryStructInfo 查询的结构信息
     * @param viewStructInfo 物化视图的结构信息
     * @param viewToQuerySlotMapping 物化视图列到查询列的映射关系
     * @param comparisonResult 查询和物化视图的比较结果（当前未使用）
     * @return 需要补偿的谓词映射，key 为需要补偿的表达式，value 为表达式信息。
     *         如果无法重写（查询不包含物化视图的所有残差谓词，或包含聚合函数），返回 null
     */
    public static Map<Expression, ExpressionInfo> compensateResidualPredicate(StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            SlotMapping viewToQuerySlotMapping,
            ComparisonResult comparisonResult) {
        // TODO Residual predicates compensate, simplify implementation currently.
        
        // 步骤1：获取查询和物化视图的分割谓词
        // SplitPredicate 将谓词分为三类：
        //   - equalPredicateMap: 等值谓词，如 col1 = col2, col1 = 'value'
        //   - rangePredicateMap: 范围谓词，如 col1 > 100, col1 IN (1,2,3)
        //   - residualPredicateMap: 残差谓词，如 col1 >= '202531' OR col1 = '202530'
        // 示例：
        //   查询：WHERE ds = '202530' AND status = 'active'
        //   - equalPredicateMap: {ds = '202530', status = 'active'}
        //   - rangePredicateMap: {}
        //   - residualPredicateMap: {}
        SplitPredicate querySplitPredicate = queryStructInfo.getSplitPredicate();
        SplitPredicate viewSplitPredicate = viewStructInfo.getSplitPredicate();

        // 步骤2：将物化视图的残差谓词转换为基于查询的表达式
        // 使用 SlotMapping 将物化视图中的列引用替换为查询中对应的列引用
        // 示例：
        //   物化视图：SELECT * FROM table1 WHERE ds >= '202531' OR ds = '202530'
        //   - 物化视图的残差谓词：{ds >= '202531' OR ds = '202530'}
        //   - 转换后（基于查询的表达式）：{ds >= '202531' OR ds = '202530'}（如果列名相同）
        Set<Expression> viewResidualQueryBasedSet = new HashSet<>();
        for (Expression viewExpression : viewSplitPredicate.getResidualPredicateMap().keySet()) {
            viewResidualQueryBasedSet.add(
                    ExpressionUtils.replace(viewExpression, viewToQuerySlotMapping.toSlotReferenceMap()));
        }
        // 移除恒真的布尔字面量（TRUE），这些是冗余的
        viewResidualQueryBasedSet.remove(BooleanLiteral.TRUE);

        // 步骤3：获取查询的残差谓词集合
        // 示例：
        //   查询：WHERE ds = '202530'
        //   - queryResidualSet = {}（空，因为 ds = '202530' 被归类为等值谓词，不是残差谓词）
        //   查询：WHERE ds >= '202531' OR ds = '202530'
        //   - queryResidualSet = {ds >= '202531' OR ds = '202530'}
        Set<Expression> queryResidualSet = querySplitPredicate.getResidualPredicateMap().keySet();
        // 移除恒真的布尔字面量（TRUE），这些是冗余的
        queryResidualSet.remove(BooleanLiteral.TRUE);
        
        // 步骤4：检查查询的残差谓词是否包含物化视图的所有残差谓词
        // 当前实现的限制：要求查询必须完全包含物化视图的残差谓词
        // 如果不包含，说明查询的过滤条件不足以覆盖物化视图的过滤条件，无法使用物化视图重写
        // 示例（无法重写）：
        //   viewResidualQueryBasedSet = {ds >= '202531' OR ds = '202530'}
        //   queryResidualSet = {}
        //   queryResidualSet.containsAll(viewResidualQueryBasedSet) = false
        //   结果：返回 null，无法重写
        // 示例（可以重写）：
        //   viewResidualQueryBasedSet = {ds >= '202531' OR ds = '202530'}
        //   queryResidualSet = {ds >= '202531' OR ds = '202530'}
        //   queryResidualSet.containsAll(viewResidualQueryBasedSet) = true
        //   继续执行后续步骤
        if (!queryResidualSet.containsAll(viewResidualQueryBasedSet)) {
            return null;
        }
        
        // 步骤5：计算需要补偿的谓词（查询有但物化视图没有的残差谓词）
        // 这些谓词需要在重写后的查询中额外添加，以确保查询结果的正确性
        // 示例：
        //   物化视图：WHERE ds >= '202531' OR ds = '202530'
        //   查询：WHERE (ds >= '202531' OR ds = '202530') AND status = 'active'
        //   - viewResidualQueryBasedSet = {ds >= '202531' OR ds = '202530'}
        //   - queryResidualSet = {ds >= '202531' OR ds = '202530', status = 'active'}
        //   - removeAll 后：queryResidualSet = {status = 'active'}
        //   - 需要补偿：status = 'active'
        queryResidualSet.removeAll(viewResidualQueryBasedSet);
        Map<Expression, ExpressionInfo> expressionExpressionInfoMap = new HashMap<>();
        
        // 步骤6：检查需要补偿的谓词，并构建补偿映射
        for (Expression needCompensate : queryResidualSet) {
            // 如果补偿谓词中包含聚合函数，则无法重写
            // 因为聚合函数无法在物化视图扫描后通过简单的过滤来补偿
            // 示例：
            //   需要补偿的谓词：COUNT(*) > 100
            //   结果：返回 null，无法重写
            //   原因：聚合函数需要在物化视图构建时计算，无法在查询时通过过滤补偿
            if (needCompensate.anyMatch(AggregateFunction.class::isInstance)) {
                return null;
            }
            // 将需要补偿的谓词添加到映射中，ExpressionInfo.EMPTY 表示没有额外的表达式信息
            // 重写器会使用这个映射在重写后的查询中添加这些谓词
            // 示例：
            //   expressionExpressionInfoMap = {status = 'active' -> ExpressionInfo.EMPTY}
            //   重写后的查询：SELECT * FROM mv WHERE status = 'active'
            expressionExpressionInfoMap.put(needCompensate, ExpressionInfo.EMPTY);
        }
        
        // 返回需要补偿的谓词映射，重写器会在重写后的查询中添加这些谓词
        // 示例返回值：
        //   {status = 'active' -> ExpressionInfo.EMPTY}
        return expressionExpressionInfoMap;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("Predicates", "pulledUpPredicates", pulledUpPredicates,
                "predicatesUnderBreaker", couldNotPulledUpPredicates);
    }

    /**
     * The struct info for expression, such as the constant that it used
     */
    public static final class ExpressionInfo {

        public static final ExpressionInfo EMPTY = new ExpressionInfo(null);

        public final Literal literal;

        public ExpressionInfo(Literal literal) {
            this.literal = literal;
        }
    }

    /**
     * The split different representation for predicate expression, such as equal, range and residual predicate.
     */
    public static final class SplitPredicate {
        public static final SplitPredicate INVALID_INSTANCE =
                SplitPredicate.of(null, null, null);
        private final Map<Expression, ExpressionInfo> equalPredicateMap;
        private final Map<Expression, ExpressionInfo> rangePredicateMap;
        private final Map<Expression, ExpressionInfo> residualPredicateMap;

        public SplitPredicate(Map<Expression, ExpressionInfo> equalPredicateMap,
                Map<Expression, ExpressionInfo> rangePredicateMap,
                Map<Expression, ExpressionInfo> residualPredicateMap) {
            this.equalPredicateMap = equalPredicateMap;
            this.rangePredicateMap = rangePredicateMap;
            this.residualPredicateMap = residualPredicateMap;
        }

        public Map<Expression, ExpressionInfo> getEqualPredicateMap() {
            return equalPredicateMap;
        }

        public Map<Expression, ExpressionInfo> getRangePredicateMap() {
            return rangePredicateMap;
        }

        public Map<Expression, ExpressionInfo> getResidualPredicateMap() {
            return residualPredicateMap;
        }

        /**
         * SplitPredicate construct
         */
        public static SplitPredicate of(Map<Expression, ExpressionInfo> equalPredicateMap,
                Map<Expression, ExpressionInfo> rangePredicateMap,
                Map<Expression, ExpressionInfo> residualPredicateMap) {
            return new SplitPredicate(equalPredicateMap, rangePredicateMap, residualPredicateMap);
        }

        /**
         * Check the predicates are invalid or not. If any of the predicates is null, it is invalid.
         */
        public boolean isInvalid() {
            return Objects.equals(this, INVALID_INSTANCE);
        }

        /**
         * Get expression list in predicates
         */
        public List<Expression> toList() {
            if (isInvalid()) {
                return ImmutableList.of();
            }
            List<Expression> flattenExpressions = new ArrayList<>(getEqualPredicateMap().keySet());
            flattenExpressions.addAll(getRangePredicateMap().keySet());
            flattenExpressions.addAll(getResidualPredicateMap().keySet());
            return flattenExpressions;
        }

        /**
         * Check the predicates in SplitPredicate is whether all true or not
         */
        public boolean isAlwaysTrue() {
            return getEqualPredicateMap().isEmpty() && getRangePredicateMap().isEmpty()
                    && getResidualPredicateMap().isEmpty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SplitPredicate that = (SplitPredicate) o;
            return Objects.equals(equalPredicateMap, that.equalPredicateMap)
                    && Objects.equals(rangePredicateMap, that.residualPredicateMap)
                    && Objects.equals(residualPredicateMap, that.residualPredicateMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(equalPredicateMap, rangePredicateMap, residualPredicateMap);
        }

        @Override
        public String toString() {
            return Utils.toSqlString("SplitPredicate",
                    "equalPredicate", equalPredicateMap,
                    "rangePredicate", rangePredicateMap,
                    "residualPredicate", residualPredicateMap);
        }
    }
}
