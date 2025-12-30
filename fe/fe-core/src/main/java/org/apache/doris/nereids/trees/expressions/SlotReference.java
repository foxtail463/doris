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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TColumnAccessPath;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Reference to slot in expression.
 */
public class SlotReference extends Slot {
    protected final ExprId exprId;
    protected final Supplier<String> name;
    protected final DataType dataType;
    protected final boolean nullable;
    protected final List<String> qualifier;

    // The sub column path to access type like struct or variant
    // e.g. For accessing variant["a"]["b"], the parsed paths is ["a", "b"]
    protected final List<String> subPath;

    // table and column from the original table, fall through views
    private final TableIf originalTable;
    private final Column originalColumn;

    // table and column from one level table/view, do not fall through views. use for compatible with MySQL protocol
    // that need return original table and name for view not its original table if u query a view
    private final TableIf oneLevelTable;
    private final Column oneLevelColumn;
    private final Optional<List<TColumnAccessPath>> allAccessPaths;
    private final Optional<List<TColumnAccessPath>> predicateAccessPaths;
    private final Optional<List<TColumnAccessPath>> displayAllAccessPaths;
    private final Optional<List<TColumnAccessPath>> displayPredicateAccessPaths;

    public SlotReference(String name, DataType dataType) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, true, ImmutableList.of(),
                null, null, null, null, ImmutableList.of());
    }

    public SlotReference(String name, DataType dataType, boolean nullable) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, nullable, ImmutableList.of(),
                null, null, null, null, ImmutableList.of());
    }

    public SlotReference(String name, DataType dataType, boolean nullable, List<String> qualifier) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, nullable,
                qualifier, null, null, null, null, ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable, List<String> qualifier) {
        this(exprId, name, dataType, nullable, qualifier, null, null, null, null, ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable, List<String> qualifier,
            @Nullable TableIf originalTable, @Nullable Column originalColumn,
            @Nullable TableIf oneLevelTable, @Nullable Column oneLevelColumn) {
        this(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn, ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable, List<String> qualifier,
            @Nullable TableIf originalTable, @Nullable Column originalColumn,
            @Nullable TableIf oneLevelTable, @Nullable Column oneLevelColumn,
            List<String> subPath) {
        this(exprId, () -> name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, Optional.empty());
    }

    public SlotReference(ExprId exprId, Supplier<String> name, DataType dataType, boolean nullable,
            List<String> qualifier, @Nullable TableIf originalTable, @Nullable Column originalColumn,
            @Nullable TableIf oneLevelTable, Column oneLevelColumn,
            List<String> subPath, Optional<Pair<Integer, Integer>> indexInSql) {
        this(exprId, name, dataType, nullable, qualifier, originalTable, originalColumn, oneLevelTable,
                oneLevelColumn, subPath, indexInSql, Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
    }

    /**
     * Constructor for SlotReference.
     *
     * @param exprId UUID for this slot reference
     * @param name slot reference name
     * @param dataType slot reference logical data type
     * @param nullable true if nullable
     * @param qualifier slot reference qualifier
     * @param originalColumn the column which this slot come from
     * @param subPath subColumn access labels
     */
    public SlotReference(ExprId exprId, Supplier<String> name, DataType dataType, boolean nullable,
            List<String> qualifier, @Nullable TableIf originalTable, @Nullable Column originalColumn,
            @Nullable TableIf oneLevelTable, Column oneLevelColumn,
            List<String> subPath, Optional<Pair<Integer, Integer>> indexInSql,
            Optional<List<TColumnAccessPath>> allAccessPaths, Optional<List<TColumnAccessPath>> predicateAccessPaths,
            Optional<List<TColumnAccessPath>> displayAllAccessPaths,
            Optional<List<TColumnAccessPath>> displayPredicateAccessPaths) {
        super(indexInSql);
        this.exprId = exprId;
        this.name = name;
        this.dataType = dataType;
        this.qualifier = Utils.fastToImmutableList(
                Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.nullable = nullable;
        this.originalTable = originalTable;
        this.originalColumn = originalColumn;
        this.oneLevelTable = oneLevelTable;
        this.oneLevelColumn = oneLevelColumn;
        this.subPath = Objects.requireNonNull(subPath, "subPath can not be null");
        this.allAccessPaths = allAccessPaths;
        this.predicateAccessPaths = predicateAccessPaths;
        this.displayAllAccessPaths = displayAllAccessPaths;
        this.displayPredicateAccessPaths = displayPredicateAccessPaths;
    }

    public static SlotReference of(String name, DataType type) {
        return new SlotReference(name, type);
    }

    /**
     * get SlotReference from a column
     * @param column the column which contains type info
     * @param qualifier the qualifier of SlotReference
     */
    public static SlotReference fromColumn(ExprId exprId, TableIf table, Column column, List<String> qualifier) {
        return fromColumn(exprId, table, column, column.getName(), qualifier);
    }

    /**
     * 从表的 Column 对象创建 SlotReference。
     * 
     * 这是 Slot 最开始创建的关键方法，将 Catalog 层的 Column 转换为 Nereids 层的 SlotReference。
     * 该方法在 LogicalOlapScan.computeOutput() 中被调用，为表的每个列创建对应的 Slot。
     * 
     * 转换过程：
     * 1. 类型转换：将 Catalog 的 Type 转换为 Nereids 的 DataType
     * 2. 信息提取：从 Column 中提取列名、可空性等信息
     * 3. 对象创建：创建 SlotReference 对象，包含完整的列信息
     * 
     * 创建的 SlotReference 包含的信息：
     * - exprId: 唯一标识符，在整个查询中唯一标识该 Slot
     * - name: 列名（可能与 Column 的名称不同，例如视图中的列名）
     * - dataType: 数据类型（从 Catalog Type 转换而来）
     * - nullable: 是否可空（从 Column.isAllowNull() 获取）
     * - qualifier: 表的限定符（如 [catalog, db, table]），用于区分不同表的同名列
     * - originalTable/originalColumn: 原始表和列对象（用于视图场景，可以追溯到实际的表）
     * - oneLevelTable/oneLevelColumn: 一级表和列（不穿透视图，用于 MySQL 协议兼容）
     * - subPath: 子路径（用于复杂类型，默认为空）
     * 
     * 使用场景：
     * - LogicalOlapScan: 从表的 Schema 创建 Slot
     * - LogicalView: 从视图的 Schema 创建 Slot
     * - 物化视图扫描: 从物化视图的 Schema 创建 Slot
     * 
     * @param exprId Slot 的唯一标识符，由 StatementScopeIdGenerator 生成
     * @param table 表对象，包含表的元数据信息
     * @param column 列的元数据对象，包含列的类型、可空性等信息
     * @param name Slot 的名称（可能与 column.getName() 不同，例如视图中的别名）
     * @param qualifier 表的限定符列表，例如 [catalog, db, table]
     * @return 创建的 SlotReference 对象，代表数据流中的一个位置
     */
    public static SlotReference fromColumn(
            ExprId exprId, TableIf table, Column column, String name, List<String> qualifier) {
        // 步骤 1: 类型转换
        // 将 Catalog 层的 Type（如 org.apache.doris.catalog.Type）转换为 Nereids 层的 DataType
        // 例如：Type.INT -> IntegerType.INSTANCE
        DataType dataType = DataType.fromCatalogType(column.getType());
        
        // 步骤 2: 创建 SlotReference
        // 参数说明：
        // - exprId: 唯一标识符
        // - name: 列名（可能与 column.getName() 不同）
        // - dataType: 转换后的数据类型
        // - column.isAllowNull(): 是否可空
        // - qualifier: 表的限定符
        // - table, column: 原始表和列（用于视图等场景）
        // - table, column: 一级表和列（不穿透视图）
        // - ImmutableList.of(): 子路径（默认为空，用于复杂类型）
        return new SlotReference(exprId, name, dataType,
            column.isAllowNull(), qualifier, table, column, table, column, ImmutableList.of());
    }

    @Override
    public String getName() {
        return name.get();
    }

    @Override
    public ExprId getExprId() {
        return exprId;
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return nullable;
    }

    public Optional<TableIf> getOriginalTable() {
        return Optional.ofNullable(originalTable);
    }

    public Optional<Column> getOriginalColumn() {
        return Optional.ofNullable(originalColumn);
    }

    public Optional<TableIf> getOneLevelTable() {
        return Optional.ofNullable(oneLevelTable);
    }

    public Optional<Column> getOneLevelColumn() {
        return Optional.ofNullable(oneLevelColumn);
    }

    @Override
    public String computeToSql() {
        if (subPath.isEmpty()) {
            return name.get();
        } else {
            return name.get() + "['" + String.join("']['", subPath) + "']";
        }
    }

    @Override
    public String toString() {
        if (subPath.isEmpty()) {
            // Just return name and exprId, add another method to show fully qualified name when it's necessary.
            return name.get() + "#" + exprId;
        }
        return name.get() + "['" + String.join("']['", subPath) + "']" + "#" + exprId;
    }

    @Override
    public String shapeInfo() {
        if (qualifier.isEmpty()) {
            return name.get();
        } else {
            return qualifier.get(qualifier.size() - 1) + "." + name.get();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotReference that = (SlotReference) o;
        // The equals of slotReference only compares exprId,
        // because in subqueries with aliases,
        // there will be scenarios where the same exprId but different qualifiers are used,
        // resulting in an error due to different qualifiers during comparison.
        // eg:
        // select * from t6 where t6.k1 < (select max(aa) from (select v1 as aa from t7 where t6.k2=t7.v2) t2 )
        //
        // For aa, the qualifier of aa in the subquery is empty, but in the output column of agg,
        // the qualifier of aa is t2. but both actually represent the same column.
        return exprId.equals(that.exprId);
    }

    // The contains method needs to use hashCode, so similar to equals, it only compares exprId
    @Override
    public int hashCode() {
        // direct return exprId to speed up
        return exprId.asInt();
    }

    @Override
    public int fastChildrenHashCode() {
        return exprId.asInt();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSlotReference(this, context);
    }

    @Override
    public SlotReference withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.isEmpty());
        return this;
    }

    @Override
    public SlotReference withNullable(boolean nullable) {
        if (this.nullable == nullable) {
            return this;
        }
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString);
    }

    @Override
    public Slot withNullableAndDataType(boolean nullable, DataType dataType) {
        if (this.nullable == nullable && this.dataType.equals(dataType)) {
            return this;
        }
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString);
    }

    @Override
    public SlotReference withQualifier(List<String> qualifier) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString);
    }

    @Override
    public SlotReference withName(String name) {
        if (this.name.get().equals(name)) {
            return this;
        }
        return new SlotReference(exprId, () -> name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString);
    }

    @Override
    public SlotReference withExprId(ExprId exprId) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString);
    }

    public SlotReference withSubPath(List<String> subPath) {
        return new SlotReference(exprId, name, dataType, !subPath.isEmpty() || nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString);
    }

    @Override
    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, Optional.ofNullable(index));
    }

    public SlotReference withColumn(Column column) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, column, oneLevelTable, column,
                subPath, indexInSqlString);
    }

    @Override
    public Slot withOneLevelTableAndColumnAndQualifier(TableIf oneLevelTable, Column column, List<String> qualifier) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, column, oneLevelTable, column,
                subPath, indexInSqlString);
    }

    public boolean isVisible() {
        return originalColumn == null || originalColumn.isVisible();
    }

    public List<String> getSubPath() {
        return subPath;
    }

    public boolean hasSubColPath() {
        return !subPath.isEmpty();
    }

    public String getQualifiedNameWithBackquote() throws UnboundException {
        return Utils.qualifiedNameWithBackquote(getQualifier(), getName());
    }

    public boolean hasAutoInc() {
        return originalColumn != null ? originalColumn.isAutoInc() : false;
    }

    public SlotReference withAccessPaths(
            List<TColumnAccessPath> allAccessPaths, List<TColumnAccessPath> predicateAccessPaths) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString, Optional.of(allAccessPaths), Optional.of(predicateAccessPaths),
                Optional.of(allAccessPaths), Optional.of(predicateAccessPaths));
    }

    public SlotReference withAccessPaths(
            List<TColumnAccessPath> allAccessPaths, List<TColumnAccessPath> predicateAccessPaths,
            List<TColumnAccessPath> displayAllAccessPaths, List<TColumnAccessPath> displayPredicateAccessPaths) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier,
                originalTable, originalColumn, oneLevelTable, oneLevelColumn,
                subPath, indexInSqlString, Optional.of(allAccessPaths), Optional.of(predicateAccessPaths),
                Optional.of(displayAllAccessPaths), Optional.of(displayPredicateAccessPaths));
    }

    public Optional<List<TColumnAccessPath>> getAllAccessPaths() {
        return allAccessPaths;
    }

    public Optional<List<TColumnAccessPath>> getPredicateAccessPaths() {
        return predicateAccessPaths;
    }

    public Optional<List<TColumnAccessPath>> getDisplayAllAccessPaths() {
        return displayAllAccessPaths;
    }

    public Optional<List<TColumnAccessPath>> getDisplayPredicateAccessPaths() {
        return displayPredicateAccessPaths;
    }
}
