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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.PredicateInferUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * this class do these things:
 * {@code
 * 1. t1.a=t2.b t2.b=t3.c -> t1.a=t2.b t2.b=t3.c (reserve all three condition)
 * 2. remove useless equal predicates(e.g. t1.a=t1.b t1.a=1 t1.b=1 -> t1.a=1 t1.b=1. t1.a=t1.b is removed)
 * 3. do unequalPredicateInfer(e.g. t1.a<t2.b and t2.b<1 -> t1.a<1 and t1.a<t2.b and t2.b<1)
 * 4. remove useless unequal predicates(e.g. t1.a<t1.b t1.a<1 t1.b<1 -> t1.a<t1.b t1.b<1)}
 * */
public class UnequalPredicateInfer {
    /**InferenceGraph*/
    public static class InferenceGraph {
        /** relation between inputExprs */
        public enum Relation {
            GT,
            GTE,
            EQ,
            UNDEFINED
        }

        private static class PairAndRelation {
            private final Pair<Expression, Expression> pair;
            private final Relation relation;

            private PairAndRelation(Pair<Expression, Expression> p, Relation r) {
                pair = p;
                relation = r;
            }
        }

        // Save and infer the relationship between inputExpressions
        private final Relation[][] graph;
        // slots or literal at both ends of the input predicate, and its index corresponds to the one in the graph.
        private final List<Expression> usedExprs = new ArrayList<>();
        // predicates used in derivation, this is used in chooseInputPredicates
        private final List<ComparisonPredicate> usedPredicates = new ArrayList<>();
        // usedPredicatesPairs has same length with usedPredicates,
        // usedPredicatesPairs[i] and usedPredicates[i] correspond to same predicates
        // usedPredicatesPairs is extracted from cast and used in graph
        private final List<PairAndRelation> usedPredicatesPairs = new ArrayList<>();
        // Elements and their indexes in usedExprs
        private final Map<Expression, Integer> usedExprPosition = new HashMap<>();
        // size of usedExprs
        private final int size;
        // not use input predicates
        private final List<Expression> otherPredicates = new ArrayList<>();

        /**Constructor*/
        public InferenceGraph(Set<Expression> inputs) {
            Set<Expression> inputExpressionSet = new HashSet<>();
            for (Expression input : inputs) {
                if (!(input instanceof ComparisonPredicate)) {
                    otherPredicates.add(input);
                    continue;
                }
                ComparisonPredicate comparison = (ComparisonPredicate) input;
                if (comparison.left().equals(comparison.right())) {
                    otherPredicates.add(comparison);
                    continue;
                }
                if (comparison.left() instanceof NullLiteral || comparison.right() instanceof NullLiteral) {
                    otherPredicates.add(comparison);
                    continue;
                }
                Set<Slot> leftSlots = comparison.left().getInputSlots();
                Set<Slot> rightSlots = comparison.right().getInputSlots();
                if (leftSlots.isEmpty() && rightSlots.isEmpty()) {
                    otherPredicates.add(comparison);
                    continue;
                }
                ComparisonPredicate commute;
                if (comparison instanceof LessThan || comparison instanceof LessThanEqual) {
                    commute = (ComparisonPredicate) comparison.commute().withInferred(comparison.isInferred());
                } else if (comparison instanceof GreaterThan || comparison instanceof GreaterThanEqual
                        || comparison instanceof EqualTo) {
                    commute = comparison;
                } else {
                    otherPredicates.add(comparison);
                    continue;
                }
                Optional<Pair<Expression, Expression>> optionalPair = PredicateInferUtils.getPairFromCast(commute);
                if (!optionalPair.isPresent()) {
                    otherPredicates.add(comparison);
                    continue;
                }
                Pair<Expression, Expression> pair = optionalPair.get();
                if (!PredicateInferUtils.isSlotOrLiteral(pair.first)
                        || !PredicateInferUtils.isSlotOrLiteral(pair.second)) {
                    otherPredicates.add(comparison);
                    continue;
                }
                inputExpressionSet.add(pair.first);
                inputExpressionSet.add(pair.second);
                usedPredicates.add(comparison);
                usedPredicatesPairs.add(new PairAndRelation(pair, getType(commute)));
            }
            usedExprs.addAll(inputExpressionSet);
            // Sorting is required to ensure the stability of the plan shape
            // and to ensure that the same results are output in the derivation of d>1 d=c and c>1 d=c
            usedExprs.sort(Comparator.comparing(ExpressionTrait::toSql));
            size = usedExprs.size();
            for (int i = 0; i < size; ++i) {
                usedExprPosition.put(usedExprs.get(i), i);
            }
            graph = new Relation[size][size];
            initGraph(graph);
            // Add edges to the graph.
            for (PairAndRelation predicatesPair : usedPredicatesPairs) {
                int l = usedExprPosition.get(predicatesPair.pair.first);
                int r = usedExprPosition.get(predicatesPair.pair.second);
                set(graph, l, r, predicatesPair.relation);
            }
        }

        public void initGraph(Relation[][] g) {
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    g[i][j] = Relation.UNDEFINED;
                }
            }
        }

        private void connect(Relation[][] graph, int left, int right, int mid) {
            if (graph[left][right] != Relation.EQ) {
                if (graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.EQ) {
                    graph[left][right] = Relation.EQ;
                }
            }
            if (graph[left][right] != Relation.GTE) {
                if (graph[left][mid] == Relation.GTE && graph[mid][right] == Relation.EQ
                        || graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.GTE) {
                    graph[left][right] = Relation.GTE;
                }
            }
            if (graph[left][right] != Relation.GT) {
                if (graph[left][mid] == Relation.GT && graph[mid][right] != Relation.UNDEFINED
                        || graph[left][mid] != Relation.UNDEFINED && graph[mid][right] == Relation.GT) {
                    graph[left][right] = Relation.GT;
                }
            }
        }

        // Calculate the relationship between left and right derived from mid
        private Relation connectInThisPath(final Relation[][] graph, int left, int right, int mid) {
            Relation deduceRelation = Relation.UNDEFINED;
            if (graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.EQ) {
                deduceRelation = Relation.EQ;
            }
            if (graph[left][mid] == Relation.GTE && graph[mid][right] == Relation.EQ
                    || graph[left][mid] == Relation.EQ && graph[mid][right] == Relation.GTE) {
                deduceRelation = Relation.GTE;
            }
            if (graph[left][mid] == Relation.GT && graph[mid][right] != Relation.UNDEFINED
                    || graph[left][mid] != Relation.UNDEFINED && graph[mid][right] == Relation.GT) {
                deduceRelation = Relation.GT;
            }
            return deduceRelation;
        }

        /** use Floyd algorithm to deduce the inequality */
        public void deduce(Relation[][] graph) {
            for (int mid = 0; mid < size; ++mid) {
                for (int left = 0; left < size; ++left) {
                    for (int right = 0; right < size; ++right) {
                        connect(graph, left, right, mid);
                    }
                }
            }
        }

        /**topoSort*/
        public List<Integer> topoSort() {
            ArrayList<Integer> order = new ArrayList<>();
            order.ensureCapacity(size);
            ArrayList<Boolean> visited = new ArrayList<>();
            visited.ensureCapacity(size);
            for (int i = 0; i < size; ++i) {
                visited.add(false);
            }
            for (int i = 0; i < size; ++i) {
                dfs(i, visited, order);
            }
            return order;
        }

        private void dfs(int node, List<Boolean> visited, List<Integer> order) {
            if (visited.get(node)) {
                return;
            }
            visited.set(node, true);
            for (int i = 0; i < size; ++i) {
                if (graph[node][i] == Relation.GT || graph[node][i] == Relation.GTE) {
                    dfs(i, visited, order);
                }
            }
            order.add(node);
        }

        /**Determine whether the slots in a predicate come from only one table*/
        private boolean isTableFilter(int left, int right) {
            Set<String> qualifiers = new HashSet<>();
            for (Slot slot : usedExprs.get(left).getInputSlots()) {
                qualifiers.add(String.join(".", slot.getQualifier()));
            }
            for (Slot slot : usedExprs.get(right).getInputSlots()) {
                qualifiers.add(String.join(".", slot.getQualifier()));
            }
            // TODO:
            // isTableFilter(abs(t1.a)#1 = abs(t1.b)#2) will return true
            // isTableFilter(abs(t1.a)#1 = abs(t2.b)#2) will also return true, which is wrong.
            // because expr(e.g. abs(a) #1) qualifiers is empty.
            // We cannot distinguish whether abs(t1.a)#1 = abs(t2.b)#2 is a TableFilter or not.
            // current code may lead to some useful predicates be removed
            return qualifiers.size() == 1;
        }

        private boolean hasIndexOrPartitionColumn(Expression left, Expression right) {
            SlotReference checkSlot;
            if (left instanceof SlotReference && right instanceof Literal) {
                checkSlot = (SlotReference) left;
            } else if (left instanceof Literal && right instanceof SlotReference) {
                checkSlot = (SlotReference) right;
            } else {
                return false;
            }
            if (!checkSlot.isColumnFromTable()) {
                return false;
            }
            Column column = checkSlot.getOriginalColumn().get();
            if (column.isKey()) {
                return true;
            }
            if (!checkSlot.getOriginalTable().isPresent()) {
                return false;
            }
            TableIf tableIf = checkSlot.getOriginalTable().get();
            if (tableIf.isPartitionedTable() && tableIf.isPartitionColumn(column)) {
                return true;
            }
            /* Indexes are seldom used and are not supported temporarily
            if (tableIf.getType() != TableType.OLAP) {
                return false;
            }
            TableIndexes tableIndexes = tableIf.getTableIndexes();
            for (Index index : tableIndexes.getIndexes()) {
                IndexDef.IndexType type = index.getIndexType();
                if (type == IndexType.NGRAM_BF || type == IndexType.BLOOMFILTER) {
                    continue;
                }
                Set<String> columns = new HashSet<>(index.getColumns());
                if (columns.contains(column.getName())) {
                    return true;
                }
            }*/
            return false;
        }

        // determine whether the comparison predicate of type between left right can be deduced by mid
        private boolean checkDeducible(final Relation[][] graph, int left, int right, int mid, Relation type) {
            Relation deduceType = connectInThisPath(graph, left, right, mid);
            return deduceType == type;
        }

        private List<Integer> removeExprEqualToConstant(List<Integer> order, Set<Integer> equalWithConstant) {
            // Remove expr equal to constant
            List<Integer> orderToInfer = new ArrayList<>();
            for (Integer integer : order) {
                if (equalWithConstant.contains(integer)) {
                    continue;
                }
                orderToInfer.add(integer);
            }
            return orderToInfer;
        }

        /**chooseUnequalPredicates*/
        public void chooseUnequalPredicates(Relation[][] chosen, Set<Integer> equalWithConstant) {
            List<Integer> order = topoSort();
            List<Integer> orderToInfer = removeExprEqualToConstant(order, equalWithConstant);
            //Select predicate:
            // 1. Do not select predicates that can be deduced from the intermediate expr
            // 2. If it is an index column or partition column, reserve the predicate
            for (int i = 1; i < orderToInfer.size(); ++i) {
                for (int j = 0; j < i; ++j) {
                    int left = orderToInfer.get(i);
                    int right = orderToInfer.get(j);
                    if (graph[left][right] == Relation.EQ || graph[left][right] == Relation.UNDEFINED) {
                        continue;
                    }
                    if (!isTableFilter(left, right)) {
                        continue;
                    }
                    boolean skip = hasIndexOrPartitionColumn(usedExprs.get(left), usedExprs.get(right));
                    boolean deducible = false;
                    for (int m = j + 1; !skip && !deducible && m < i; ++m) {
                        int mid = orderToInfer.get(m);
                        if (usedExprs.get(mid) instanceof Literal) {
                            deducible = checkDeducible(graph, left, right, mid, graph[left][right]);
                        } else if (isTableFilter(left, mid) && isTableFilter(right, mid)) {
                            deducible = checkDeducible(graph, left, right, mid, graph[left][right]);
                        }
                    }
                    if (!deducible) {
                        set(chosen, left, right, graph[left][right]);
                    }
                }
            }
        }

        /**
         * 根据 chosen 矩阵生成新的比较谓词。
         * 
         * 该方法遍历 chosen 矩阵，对于每个被选中的关系（GT, GTE, EQ），
         * 创建对应的比较谓词对象，并进行规范化处理。
         * 
         * ==================== 处理流程 ====================
         * 
         * 【步骤1：遍历关系矩阵】
         * 遍历 chosen 矩阵的所有位置 (i, j)，其中：
         * - i：左表达式的索引（usedExprs[i]）
         * - j：右表达式的索引（usedExprs[j]）
         * 
         * 【步骤2：跳过无效情况】
         * 以下情况会被跳过，不生成谓词：
         * - i == j：自己和自己比较（如 id = id），这是恒真的，不需要生成
         * - isAllLiteral(i, j)：两个字面量之间的比较（如 2 < 10），
         *   这种比较是常量比较，在查询优化中没有意义
         * 
         * 【步骤3：生成比较谓词】
         * 根据 chosen[i][j] 的关系类型，创建对应的比较谓词：
         * - Relation.GT → new GreaterThan(left, right)
         * - Relation.GTE → new GreaterThanEqual(left, right)
         * - Relation.EQ → new EqualTo(left, right)
         * 
         * 【步骤4：规范化谓词】
         * 调用 normalize() 方法对生成的谓词进行规范化：
         * - normalizePredicate()：确保常量在右边（如 2 < id → id > 2）
         * - TypeCoercionUtils.processComparisonPredicate()：进行类型转换
         * - withInferred(true)：设置 inferred 标志，标记为推导出的谓词
         * 
         * 【步骤5：处理等值关系】
         * 对于等值关系（EQ），生成谓词后需要清除 chosen 矩阵中的对应关系：
         * - 清除 chosen[i][j] = UNDEFINED
         * - 清除 chosen[j][i] = UNDEFINED（因为等值关系是对称的）
         * 这样可以避免重复生成相同的等值谓词
         * 
         * 【步骤6：异常处理】
         * 如果类型转换失败（AnalysisException），跳过该谓词，继续处理下一个
         * 
         * ==================== 示例 ====================
         * 
         * 输入：
         * usedExprs = [id#0, 2, 10]
         * chosen 矩阵：
         *        id#0    2     10
         * id#0    EQ     EQ    GT
         * 2       EQ     EQ    ?
         * 10      ?      ?     EQ
         * 
         * 处理过程：
         * 1. (0, 0)：i == j，跳过
         * 2. (0, 1)：chosen[0][1] = EQ → 生成 id#0 = 2，清除 chosen[0][1] 和 chosen[1][0]
         * 3. (0, 2)：chosen[0][2] = GT → 生成 id#0 > 10
         * 4. (1, 0)：chosen[1][0] = UNDEFINED（已被清除），跳过
         * 5. (1, 1)：i == j，跳过
         * 6. (1, 2)：isAllLiteral(1, 2) = true（2 和 10 都是字面量），跳过
         * 7. (2, 0)：chosen[2][0] = UNDEFINED，跳过
         * 8. (2, 1)：isAllLiteral(2, 1) = true，跳过
         * 9. (2, 2)：i == j，跳过
         * 
         * 结果：
         * newPredicates = {
         *     id#0 = 2,      // 规范化后，inferred=true
         *     id#0 > 10      // 规范化后，inferred=true
         * }
         * 
         * ==================== 关键点 ====================
         * 
         * 1. 生成的谓词都会设置 inferred=true，表示这是推导出的谓词
         * 2. 等值关系生成后会清除，避免重复
         * 3. 常量比较会被跳过，因为它们在查询优化中没有意义
         * 4. 类型转换失败不会中断整个流程，只是跳过该谓词
         * 
         * @param chosen 经过选择的关系矩阵，包含需要生成谓词的关系
         * @return 生成的新比较谓词集合，所有谓词的 inferred 标志都设置为 true
         */
        private Set<Expression> generatePredicates(Relation[][] chosen) {
            Set<Expression> newPredicates = new LinkedHashSet<>();
            // 遍历关系矩阵的所有位置
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    // 跳过自己和自己比较（恒真，无意义）
                    // 跳过两个字面量之间的比较（常量比较，在查询优化中无意义）
                    if (i == j || isAllLiteral(i, j)) {
                        continue;
                    }
                    try {
                        // 根据关系类型生成对应的比较谓词
                        if (chosen[i][j] == Relation.GT) {
                            // 生成大于谓词：usedExprs[i] > usedExprs[j]
                            // normalize() 会规范化谓词并设置 inferred=true
                            newPredicates.add(normalize(new GreaterThan(usedExprs.get(i), usedExprs.get(j))));
                        } else if (chosen[i][j] == Relation.GTE) {
                            // 生成大于等于谓词：usedExprs[i] >= usedExprs[j]
                            newPredicates.add(normalize(new GreaterThanEqual(usedExprs.get(i), usedExprs.get(j))));
                        } else if (chosen[i][j] == Relation.EQ) {
                            // 生成等值谓词：usedExprs[i] = usedExprs[j]
                            newPredicates.add(normalize(new EqualTo(usedExprs.get(i), usedExprs.get(j))));
                            // 等值关系是对称的，生成后清除 chosen 矩阵中的对应关系，避免重复生成
                            clear(chosen, i, j, Relation.EQ);
                        }
                    } catch (AnalysisException e) {
                        // 类型转换失败（如无法比较的类型），跳过该谓词，继续处理下一个
                        // 这种情况通常发生在类型不兼容的表达式之间（如字符串和数字）
                    }
                }
            }
            return newPredicates;
        }

        private ComparisonPredicate normalizePredicate(ComparisonPredicate expr) {
            return expr.left().isConstant() && !expr.right().isConstant() ? expr.commute() : expr;
        }

        private Relation getType(ComparisonPredicate comparisonPredicate) {
            if (comparisonPredicate instanceof GreaterThan) {
                return Relation.GT;
            } else if (comparisonPredicate instanceof GreaterThanEqual) {
                return Relation.GTE;
            } else if (comparisonPredicate instanceof EqualTo) {
                return Relation.EQ;
            }
            return Relation.UNDEFINED;
        }

        private void clear(Relation[][] graph, int left, int right, Relation type) {
            graph[left][right] = Relation.UNDEFINED;
            if (type == Relation.EQ) {
                graph[right][left] = Relation.UNDEFINED;
            }
        }

        private void set(Relation[][] graph, int left, int right, Relation type) {
            graph[left][right] = type;
            if (type == Relation.EQ) {
                graph[right][left] = type;
            }
        }

        // A new edge from hub1 to hub2 has been added to the graph.
        // Use this edge to extend the connectivity between the graph nodes
        private void expandGraph(Relation[][] graph, int hub1, int hub2) {
            //Update the path from all nodes to hub2 (use hub1->hub2)
            for (int left = 0; left < size; ++left) {
                connect(graph, left, hub2, hub1);
            }
            // Use hub2 as the transit node to update the path between any two nodes
            for (int l = 0; l < size; ++l) {
                for (int r = 0; r < size; ++r) {
                    connect(graph, l, r, hub2);
                }
            }
        }

        /**chooseInputPredicates*/
        public Set<Expression> chooseInputPredicates(Relation[][] chosen) {
            boolean[] keep = new boolean[usedPredicates.size()];
            Relation[][] deduced = new Relation[size][size];
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    deduced[i][j] = chosen[i][j];
                    if (i == j) {
                        deduced[i][j] = Relation.EQ;
                    }
                }
            }
            deduce(deduced);
            // If an input predicate is not chosen and can be deduced by chosen,
            // then the input predicate need not be retained (because it is a useless predicate)
            // And the predicates in inputs that cannot be deduced by chosen should be retained.
            for (int i = 0; i < usedPredicates.size(); ++i) {
                Relation type = usedPredicatesPairs.get(i).relation;
                int left = usedExprPosition.get(usedPredicatesPairs.get(i).pair.first);
                int right = usedExprPosition.get(usedPredicatesPairs.get(i).pair.second);
                if (chosen[left][right] == type) {
                    keep[i] = true;
                    clear(chosen, left, right, type);
                } else if (deduced[left][right] != type) {
                    keep[i] = true;
                    set(deduced, left, right, Relation.EQ);
                    expandGraph(deduced, left, right);
                    if (type == Relation.EQ) {
                        expandGraph(deduced, right, left);
                    }
                }
            }
            Set<Expression> chooseInputs = new LinkedHashSet<>();
            for (int i = 0; i < usedPredicates.size(); ++i) {
                if (!keep[i]) {
                    continue;
                }
                chooseInputs.add(normalizePredicate(usedPredicates.get(i))
                        .withInferred(usedPredicates.get(i).isInferred()));
            }
            return chooseInputs;
        }

        /**chooseEqualPredicates*/
        public Relation[][] chooseEqualPredicates(Set<Integer> equalWithConstant) {
            Relation[][] chosen = new Relation[size][size];
            initGraph(chosen);
            int[] equalToLiteral = new int[size];
            Arrays.fill(equalToLiteral, -1);
            // save equal predicates like a=b (no literal)
            List<Pair<Integer, Integer>> tableFilters = new ArrayList<>();
            // save equal predicates like t1.a=t2.b (no literal)
            List<Pair<Integer, Integer>> nonTableFilters = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                for (int j = i + 1; j < size; ++j) {
                    if (graph[i][j] != Relation.EQ) {
                        continue;
                    }
                    // choose predicate with one side literal or t1.a=t2.b(not table filter equal)
                    if (usedExprs.get(i) instanceof Literal && usedExprs.get(j) instanceof Literal) {
                        continue;
                    } else if (!(usedExprs.get(i) instanceof Literal) && !(usedExprs.get(j) instanceof Literal)) {
                        if (isTableFilter(i, j)) {
                            tableFilters.add(Pair.of(i, j));
                        } else {
                            nonTableFilters.add(Pair.of(i, j));
                        }
                    } else if (usedExprs.get(i) instanceof Literal
                            || usedExprs.get(j) instanceof Literal) {
                        set(chosen, i, j, Relation.EQ);
                        if (usedExprs.get(i) instanceof Literal) {
                            equalToLiteral[j] = i;
                            equalWithConstant.add(j);
                        } else {
                            equalToLiteral[i] = j;
                            equalWithConstant.add(i);
                        }
                    }
                }
            }
            // a=b a=c a=1 only infer a=1 b=1 c=1, not retain a=b a=c
            for (Pair<Integer, Integer> tableFilter : tableFilters) {
                int left = tableFilter.first;
                int right = tableFilter.second;
                if (equalToLiteral[left] == -1 || equalToLiteral[right] == -1) {
                    set(chosen, left, right, Relation.EQ);
                    equalToLiteral[left] = left;
                    equalToLiteral[right] = left;
                }
            }
            for (Pair<Integer, Integer> nonTableFilter : nonTableFilters) {
                int left = nonTableFilter.first;
                int right = nonTableFilter.second;
                if (!equalWithConstant.contains(left) && !equalWithConstant.contains(right)) {
                    set(chosen, left, right, Relation.EQ);
                }
            }
            return chosen;
        }

        private Expression normalize(ComparisonPredicate cmp) {
            return TypeCoercionUtils.processComparisonPredicate(normalizePredicate(cmp)).withInferred(true);
        }

        private boolean isAllLiteral(int i, int j) {
            Expression left = usedExprs.get(i);
            Expression right = usedExprs.get(j);
            return left instanceof Literal && right instanceof Literal;
        }

        /** for test */
        public Relation[][] getGraph() {
            return graph;
        }
    }

    /**
     * 推导不等值谓词：基于输入的比较谓词集合，推导出所有可能的传递关系和新谓词。
     * 
     * 该方法是不等值谓词推导的核心入口，通过构建推理图（InferenceGraph）并使用图算法
     * 来推导传递关系，最终生成新的比较谓词。
     * 
     * ==================== 处理流程 ====================
     * 
     * 【步骤1：构建推理图】
     * 创建 InferenceGraph 对象，解析输入谓词：
     * - 提取所有比较谓词（ComparisonPredicate）
     * - 提取谓词两边的表达式（Slot 或 Literal）
     * - 构建表达式索引映射（usedExprs, usedExprPosition）
     * - 构建关系矩阵（graph），记录表达式之间的比较关系（GT, GTE, EQ）
     * 
     * 【步骤2：推导传递关系】
     * 使用 Floyd-Warshall 算法推导传递闭包：
     * - 如果 a < b 且 b < c，则推导出 a < c
     * - 如果 a = b 且 b > c，则推导出 a > c
     * - 例如：id = 2 和 id > 10 可以推导出 2 < 10（但常量比较会被跳过）
     * 
     * 【步骤3：选择等值谓词】
     * 从推导后的关系矩阵中选择等值谓词：
     * - 优先选择包含字面量的等值谓词（如 id = 5）
     * - 选择表内等值谓词（如 t1.a = t1.b）
     * - 选择跨表等值谓词（如 t1.a = t2.b）
     * - 记录与常量相等的表达式集合（equalWithConstant）
     * 
     * 【步骤4：选择不等值谓词】
     * 从推导后的关系矩阵中选择不等值谓词：
     * - 使用拓扑排序确定表达式顺序
     * - 过滤掉可以通过中间表达式推导出的谓词（避免冗余）
     * - 保留索引列或分区列的谓词（即使可推导也保留，用于优化）
     * - 只选择表内过滤谓词（isTableFilter）
     * 
     * 【步骤5：选择输入谓词】
     * 从原始输入谓词中选择需要保留的：
     * - 如果输入谓词在 chosen 矩阵中被选中，则保留
     * - 如果输入谓词无法通过 chosen 矩阵推导出，则保留（避免丢失信息）
     * - 如果输入谓词可以通过 chosen 矩阵推导出，则移除（冗余谓词）
     * 
     * 【步骤6：生成新谓词】
     * 根据 chosen 矩阵生成新的比较谓词：
     * - 遍历 chosen 矩阵，对于每个关系（GT, GTE, EQ）
     * - 创建对应的比较谓词对象（GreaterThan, GreaterThanEqual, EqualTo）
     * - 规范化谓词（normalize）：确保常量在右边，进行类型转换
     * - 设置 inferred=true 标志，标记为推导出的谓词
     * 
     * 【步骤7：合并结果】
     * 合并所有谓词：
     * - 选择的输入谓词（chooseInputPredicates）
     * - 生成的新谓词（generatePredicates）
     * - 其他无法处理的谓词（otherPredicates，如包含函数的谓词）
     * 
     * ==================== 示例 ====================
     * 
     * 输入：{id = 2, id > 10}
     * 
     * 1. 构建推理图：
     *    usedExprs = [id#0, 2, 10]
     *    graph[0][1] = EQ  (id = 2)
     *    graph[0][2] = GT  (id > 10)
     * 
     * 2. 推导传递关系：
     *    deduce() 推导出 graph[1][2] = LT  (2 < 10，但会被跳过，因为是常量比较)
     * 
     * 3. 选择等值谓词：
     *    chosen[0][1] = EQ  (id = 2)
     *    equalWithConstant = {0}  (id 与常量 2 相等)
     * 
     * 4. 选择不等值谓词：
     *    chosen[0][2] = GT  (id > 10)
     * 
     * 5. 选择输入谓词：
     *    id = 2 和 id > 10 都在 chosen 中，都保留
     * 
     * 6. 生成新谓词：
     *    根据 id = 2 和 id > 10，可能推导出 id >= 2
     *    （具体推导逻辑在 chooseUnequalPredicates 中）
     * 
     * 7. 返回结果：
     *    {id = 2, id > 10, id >= 2}  (id >= 2 的 inferred=true)
     * 
     * ==================== 关键点 ====================
     * 
     * 1. 推导出的新谓词会设置 inferred=true 标志，用于后续过滤
     * 2. 冗余谓词会被移除，只保留最小集合
     * 3. 索引列和分区列的谓词会被特殊处理，即使可推导也保留
     * 4. 包含函数的谓词会被放入 otherPredicates，不参与推导
     * 
     * @param inputs 输入的表达式集合，通常包含比较谓词（ComparisonPredicate）
     * @return 推导后的表达式集合，包含原始谓词和推导出的新谓词
     */
    public static Set<? extends Expression> inferUnequalPredicates(Set<Expression> inputs) {
        // 如果输入少于2个谓词，无法进行推导，直接返回
        if (inputs.size() < 2) {
            return inputs;
        }
        // 步骤1：构建推理图，解析输入谓词并构建关系矩阵
        InferenceGraph inferGraph = new InferenceGraph(inputs);
        // 如果没有可用的表达式（所有谓词都无法处理），直接返回输入
        if (inferGraph.usedExprs.isEmpty()) {
            return inputs;
        }
        // 步骤2：使用 Floyd-Warshall 算法推导传递关系
        // 例如：a < b 且 b < c → a < c
        inferGraph.deduce(inferGraph.graph);
        // 记录与常量相等的表达式索引集合（用于后续选择策略）
        Set<Integer> equalWithConstant = new HashSet<>();
        // 步骤3：从推导后的关系矩阵中选择等值谓词
        // 优先选择包含字面量的等值谓词，记录 equalWithConstant
        InferenceGraph.Relation[][] chosen = inferGraph.chooseEqualPredicates(equalWithConstant);
        // 步骤4：从推导后的关系矩阵中选择不等值谓词
        // 过滤掉可推导的冗余谓词，保留必要的谓词
        inferGraph.chooseUnequalPredicates(chosen, equalWithConstant);
        // 步骤5：从原始输入谓词中选择需要保留的谓词
        // 移除可以通过 chosen 矩阵推导出的冗余谓词
        Set<Expression> newPredicates = inferGraph.chooseInputPredicates(chosen);
        // 步骤6：根据 chosen 矩阵生成新的比较谓词
        // 新生成的谓词会设置 inferred=true 标志
        newPredicates.addAll(inferGraph.generatePredicates(chosen));
        // 步骤7：添加其他无法处理的谓词（如包含函数的谓词）
        newPredicates.addAll(inferGraph.otherPredicates);
        return newPredicates;
    }

    /** deduce predicates and generate all predicates without choosing*/
    public static Set<? extends Expression> inferAllPredicates(Set<Expression> inputs) {
        if (inputs.size() < 2) {
            return inputs;
        }
        InferenceGraph inferGraph = new InferenceGraph(inputs);
        if (inferGraph.usedExprs.isEmpty()) {
            return inputs;
        }
        inferGraph.deduce(inferGraph.graph);
        Set<Expression> newPredicates = new LinkedHashSet<>();
        newPredicates.addAll(inferGraph.generatePredicates(inferGraph.graph));
        newPredicates.addAll(inferGraph.otherPredicates);
        return newPredicates;
    }
}
