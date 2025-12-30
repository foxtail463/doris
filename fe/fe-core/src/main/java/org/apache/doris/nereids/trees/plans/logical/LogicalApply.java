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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalApply 表示关联型子查询（Apply Operator）。
 * 主要用于关系代数树中显式表示 SQL 子查询（包括标量子查询、IN/EXISTS 等）。
 *
 * 主要设计：
 * - 提供一种二元（左/右孩子）结构，其中：
 *   - left (输入)：主查询
 *   - right (subquery)：子查询部分
 * - 子查询类型支持 IN、EXISTS、SCALAR，详见 SubQueryType
 * - 记录关联列（correlationSlot）及关联谓词（correlationFilter），用于处理相关子查询
 * - 支持“mark join”类型语义标记（如半连接的标记列 MarkJoinSlotReference）
 *
 * 实例化泛型：LEFT_CHILD_TYPE/RIGHT_CHILD_TYPE 一般为 Plan 类型。
 */
public class LogicalApply<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    /**
     * 子查询类型，用于区分 SQL 表达式语义。
     * IN_SUBQUERY 对应 IN 语法，EXISTS_SUBQUERY 对应 EXISTS，SCALAR_SUBQUERY 代表标量子查询。
     */
    public enum SubQueryType {
        IN_SUBQUERY,
        EXITS_SUBQUERY,
        SCALAR_SUBQUERY
    }

    /** 子查询类型。如 IN/EXISTS/SCALAR。*/
    private final SubQueryType subqueryType;
    /** SQL NOT IN/NOT EXISTS/!= 用；例如 a NOT IN (SELECT ...)。 */
    private final boolean isNot;

    /** 仅IN适用，对应 a IN (SELECT b...) 中的 compare 部分。*/
    private final Optional<Expression> compareExpr;
    /** IN适用，对应类型自动转换（如 int=double 时）。 */
    private final Optional<Expression> typeCoercionExpr;

    /**
     * 相关列集合：左表哪些字段作为相关列会被右表用到。
     * 例如：WHERE t1.a = (SELECT max(b) FROM t2 WHERE t2.c = t1.a)
     * 此时 t1.a 即为 correlationSlot。
     */
    private final List<Slot> correlationSlot;
    /** 相关filter复合条件（如 t1.x = t2.y AND ...）*/
    private final Optional<Expression> correlationFilter;
    /** Mark join语义时使用，标记连接输出的Slot。 */
    private final Optional<MarkJoinSlotReference> markJoinSlotReference;

    /** 是否需要将子查询结果列加到投影结果中（如标量相关子查询转换join时加右侧字段）。*/
    private final boolean needAddSubOutputToProjects;
    /** Mark join slot可否为null，仅部分子查询消解规则中用到。 */
    private final boolean isMarkJoinSlotNotNull;

    private LogicalApply(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            List<Slot> correlationSlot, SubQueryType subqueryType, boolean isNot,
            Optional<Expression> compareExpr, Optional<Expression> typeCoercionExpr,
            Optional<Expression> correlationFilter,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            boolean needAddSubOutputToProjects,
            boolean isMarkJoinSlotNotNull,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_APPLY, groupExpression, logicalProperties, leftChild, rightChild);
        if (subqueryType == SubQueryType.IN_SUBQUERY) {
            Preconditions.checkArgument(compareExpr.isPresent(), "InSubquery must have compareExpr");
        }
        this.correlationSlot = correlationSlot == null ? ImmutableList.of() : ImmutableList.copyOf(correlationSlot);
        this.subqueryType = subqueryType;
        this.isNot = isNot;
        this.compareExpr = compareExpr;
        this.typeCoercionExpr = typeCoercionExpr;
        this.correlationFilter = correlationFilter;
        this.markJoinSlotReference = markJoinSlotReference;
        this.needAddSubOutputToProjects = needAddSubOutputToProjects;
        this.isMarkJoinSlotNotNull = isMarkJoinSlotNotNull;
    }

    public LogicalApply(List<Slot> correlationSlot, SubQueryType subqueryType, boolean isNot,
            Optional<Expression> compareExpr, Optional<Expression> typeCoercionExpr,
            Optional<Expression> correlationFilter, Optional<MarkJoinSlotReference> markJoinSlotReference,
            boolean needAddSubOutputToProjects, boolean isMarkJoinSlotNotNull,
            LEFT_CHILD_TYPE input, RIGHT_CHILD_TYPE subquery) {
        this(Optional.empty(), Optional.empty(), correlationSlot, subqueryType, isNot, compareExpr, typeCoercionExpr,
                correlationFilter, markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull,
                input, subquery);
    }

    public Optional<Expression> getCompareExpr() {
        return compareExpr;
    }

    public Optional<Expression> getTypeCoercionExpr() {
        return typeCoercionExpr;
    }

    public Expression getSubqueryOutput() {
        return typeCoercionExpr.orElseGet(() -> right().getOutput().get(0));
    }

    /** 获取所有相关列slot（主查询与子查询有交集字段时才有）。 */
    public List<Slot> getCorrelationSlot() {
        return correlationSlot;
    }

    /**
     * 获取相关谓词复合表达式（如 t1.a = t2.a AND t1.b > t2.c），用于在 apply->join 转换时生成 Join 条件。
     */
    public Optional<Expression> getCorrelationFilter() {
        return correlationFilter;
    }

    /**
     * 判断是否为标量子查询。
     * 如 WHERE ... = (SELECT ...) 这种形式
     */
    public boolean isScalar() {
        return subqueryType == SubQueryType.SCALAR_SUBQUERY;
    }

    /**
     * 是否为 IN 子查询
     */
    public boolean isIn() {
        return subqueryType == SubQueryType.IN_SUBQUERY;
    }

    /**
     * 是否为 EXISTS 子查询
     */
    public boolean isExist() {
        return subqueryType == SubQueryType.EXITS_SUBQUERY;
    }

    public SubQueryType getSubqueryType() {
        return subqueryType;
    }

    public boolean isNot() {
        return isNot;
    }

    /**
     * 是否“相关”，判断依据为是否存在关联slot（典型如 t1.a = t2.a 这种 parent-child 相关子查询)。
     * 用于区分相关 vs 非相关 apply。
     */
    public boolean isCorrelated() {
        return !correlationSlot.isEmpty();
    }

    public boolean alreadyExecutedEliminateFilter() {
        return correlationFilter.isPresent();
    }

    public boolean isMarkJoin() {
        return markJoinSlotReference.isPresent();
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return markJoinSlotReference;
    }

    public boolean isNeedAddSubOutputToProjects() {
        return needAddSubOutputToProjects;
    }

    public boolean isMarkJoinSlotNotNull() {
        return isMarkJoinSlotNotNull;
    }

    @Override
    public List<Slot> computeOutput() {
        ImmutableList.Builder<Slot> builder = ImmutableList.builder();
        // 1. 先加入左孩子的所有输出slot（主查询字段必定有）
        builder.addAll(left().getOutput());

        // 2. 如果是mark join消解（如半连接/反连接判别），特殊加一个标记列
        if (markJoinSlotReference.isPresent()) {
            builder.add(markJoinSlotReference.get());
        }

        // 3. 仅当需要加子查询输出（如标量相关子查询、apply消解左外连接等场景）
        if (needAddSubOutputToProjects) {
            // 相关型apply（左/右孩子有关联字段）
            if (isCorrelated()) {
                // 例：
                // select t1.a, (select sum(b) from t2 where t2.a = t1.a) as k from t1
                //         |-- LogicalApply(correlationSlot = [t1.a])
                //                |-- LogicalOlapScan(t1)
                //                |-- LogicalAggregate(sum(b))
                // 相关apply右侧字段全部要加进来，且全部设为nullable（外连接时右表为null需返回null）
                for (Slot slot : right().getOutput()) {
                    // 注意：即便如count本身理论不可为null，但外连接时右表为null，需整体nullable。
                    builder.add(slot.toSlot().withNullable(true));
                }
            } else {
                // 非相关型apply（如标量/IN/EXISTS子查询，右孩子一般只有一个输出slot）
                // 例：select t1.a, (select sum(b) from t2) as k from t1
                // 输出只取一个slot，且自动判断是否nullable。
                builder.add(ScalarSubquery.getScalarQueryOutputAdjustNullable(right(), correlationSlot));
            }
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalApply",
                "correlationSlot", correlationSlot,
                "correlationFilter", correlationFilter,
                "isMarkJoin", markJoinSlotReference.isPresent(),
                "isMarkJoinSlotNotNull", isMarkJoinSlotNotNull,
                "MarkJoinSlotReference", markJoinSlotReference.isPresent() ? markJoinSlotReference.get() : "empty");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalApply<?, ?> that = (LogicalApply<?, ?>) o;
        return Objects.equals(correlationSlot, that.getCorrelationSlot())
                && Objects.equals(subqueryType, that.subqueryType)
                && Objects.equals(compareExpr, that.compareExpr)
                && Objects.equals(typeCoercionExpr, that.typeCoercionExpr)
                && Objects.equals(correlationFilter, that.getCorrelationFilter())
                && Objects.equals(markJoinSlotReference, that.getMarkJoinSlotReference())
                && needAddSubOutputToProjects == that.needAddSubOutputToProjects
                && isMarkJoinSlotNotNull == that.isMarkJoinSlotNotNull
                && isNot == that.isNot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                correlationSlot, subqueryType, compareExpr, typeCoercionExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull, isNot);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalApply(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        if (correlationFilter.isPresent()) {
            return new ImmutableList.Builder<Expression>()
                    .addAll(correlationSlot)
                    .add(correlationFilter.get())
                    .build();
        } else {
            return new ImmutableList.Builder<Expression>()
                    .addAll(correlationSlot)
                    .build();
        }
    }

    @Override
    public LogicalApply<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(correlationSlot, subqueryType, isNot, compareExpr, typeCoercionExpr,
                correlationFilter, markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull,
                children.get(0), children.get(1));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalApply<>(groupExpression, Optional.of(getLogicalProperties()),
                correlationSlot, subqueryType, isNot, compareExpr, typeCoercionExpr, correlationFilter,
                markJoinSlotReference, needAddSubOutputToProjects, isMarkJoinSlotNotNull, left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(groupExpression, logicalProperties, correlationSlot, subqueryType, isNot,
                compareExpr, typeCoercionExpr, correlationFilter, markJoinSlotReference,
                needAddSubOutputToProjects, isMarkJoinSlotNotNull, children.get(0), children.get(1));
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        builder.addUniqueSlot(left().getLogicalProperties().getTrait());
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        builder.addUniformSlot(left().getLogicalProperties().getTrait());
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(left().getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(left().getLogicalProperties().getTrait());
    }
}
