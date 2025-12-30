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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Expression for alias, such as col1 as c1.
 */
public class Alias extends NamedExpression implements UnaryExpression {

    private final ExprId exprId;
    private final Supplier<String> name;
    private final List<String> qualifier;
    private final boolean nameFromChild;

    /**
     * constructor of Alias.
     *
     * @param child expression that alias represents for
     * @param name alias name
     */
    public Alias(Expression child, String name) {
        this(child, name, false);
    }

    public Alias(Expression child, String name, boolean nameFromChild) {
        this(StatementScopeIdGenerator.newExprId(), child, name, nameFromChild);
    }

    public Alias(Expression child) {
        this(StatementScopeIdGenerator.newExprId(), ImmutableList.of(child),
                Suppliers.memoize(child::toSql), ImmutableList.of(), true);
    }

    public Alias(ExprId exprId, Expression child) {
        this(exprId, ImmutableList.of(child), Suppliers.memoize(child::toSql), ImmutableList.of(), true);
    }

    public Alias(ExprId exprId, Expression child, String name) {
        this(exprId, ImmutableList.of(child), name, ImmutableList.of(), false);
    }

    public Alias(Expression child, String name, List<String> qualifier) {
        this(StatementScopeIdGenerator.newExprId(), ImmutableList.of(child), name, qualifier, false);
    }

    public Alias(ExprId exprId, Expression child, String name, boolean nameFromChild) {
        this(exprId, ImmutableList.of(child), name, ImmutableList.of(), nameFromChild);
    }

    public Alias(ExprId exprId, List<Expression> child, String name, List<String> qualifier, boolean nameFromChild) {
        this(exprId, child, Suppliers.memoize(() -> name), qualifier, nameFromChild);
    }

    private Alias(ExprId exprId, List<Expression> child, Supplier<String> name,
            List<String> qualifier, boolean nameFromChild) {
        super(child);
        this.exprId = exprId;
        this.name = name;
        this.qualifier = qualifier;
        this.nameFromChild = nameFromChild;
    }

    /**
     * 将 Alias 转换为 SlotReference，用于表示计划节点的输出列。
     * 
     * 背景：在查询计划中，每个节点（如 LogicalAggregate、LogicalProject）都需要明确声明自己的输出列。
     * 这些输出列会被上层节点引用，形成一个"数据流"。
     * 
     * 具体例子：
     * ```sql
     * SELECT user_id, sum(amount) as total FROM orders GROUP BY user_id
     * ```
     * 
     * 计划结构：
     * LogicalProject(output: [user_id#10, total#13])
     *   └─ LogicalAggregate(output: [user_id#10, total#13])
     *       └─ LogicalOlapScan(orders)
     * 
     * 在 LogicalAggregate 中：
     * - 输出表达式列表 outputExpressions: [user_id#10, Alias(sum(amount), "total")#13]
     * - 调用 computeOutput() 时，需要返回输出列（List<Slot>）
     * - 对每个 NamedExpression 调用 toSlot()：
     *   * user_id#10.toSlot() → user_id#10（本身就是 SlotReference）
     *   * Alias(...)#13.toSlot() → total#13（转换为 SlotReference）
     * - 最终 output = [user_id#10, total#13]
     * 
     * 为什么需要 toSlot()：
     * - Alias 是表达式树（包含子表达式 sum(amount)），结构复杂
     * - SlotReference 是简单的列引用（无子节点），只表示"这是第几列输出"
     * - 上层节点只需要知道"第0列是user_id，第1列是total"，不需要知道total的计算过程
     * 
     * 类比：如果把计划节点比作函数：
     * - outputExpressions 是函数的"计算逻辑"（可能很复杂，包含表达式树）
     * - output slots 是函数的"返回值签名"（简单明了，告诉调用者返回了什么）
     * - toSlot() 就是提取"返回值签名"的过程
     * 
     * 如果 child() 是 SlotReference，会保留原始表和列的信息（originalTable, originalColumn），
     * 用于后续优化（如物化视图匹配时需要知道列的来源）。
     * 如果 child() 是聚合函数等，这些信息为 null。
     */
    @Override
    public Slot toSlot() throws UnboundException {
        SlotReference slotReference = child() instanceof SlotReference
                ? (SlotReference) child() : null;

        return new SlotReference(exprId, name, child().getDataType(), child().nullable(), qualifier,
                slotReference != null ? ((SlotReference) child()).getOriginalTable().orElse(null) : null,
                slotReference != null ? slotReference.getOriginalColumn().orElse(null) : null,
                slotReference != null ? ((SlotReference) child()).getOneLevelTable().orElse(null) : null,
                slotReference != null ? slotReference.getOriginalColumn().orElse(null) : null,
                slotReference != null ? slotReference.getSubPath() : ImmutableList.of(), Optional.empty()
        );
    }

    @Override
    public String getName() throws UnboundException {
        return name.get();
    }

    @Override
    public ExprId getExprId() throws UnboundException {
        return exprId;
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child().getDataType();
    }

    @Override
    public String computeToSql() {
        return child().toSql() + " AS `" + name.get() + "`";
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child().nullable();
    }

    @Override
    protected boolean extraEquals(Expression other) {
        Alias that = (Alias) other;
        if (!exprId.equals(that.exprId) || !qualifier.equals(that.qualifier)) {
            return false;
        }

        return nameFromChild || name.get().equals(that.name.get());
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(exprId, qualifier);
    }

    @Override
    public String toString() {
        return child().toString() + " AS `" + name.get() + "`#" + exprId;
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        if (child() instanceof Literal) {
            sb.append("?");
        } else {
            sb.append(child().toDigest()).append(" AS ").append(getName());
        }
        return sb.toString();
    }

    @Override
    public Alias withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Alias(exprId, children, name, qualifier, nameFromChild);
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAlias(this, context);
    }

    public boolean isNameFromChild() {
        return nameFromChild;
    }
}
