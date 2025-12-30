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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract context for query rewrite by materialized view, this context is different by statement context
 * which means should be instanced by new query
 * 
 * ==================== 统一的 SQL 示例（用于理解字段含义）====================
 * 
 * 基表: orders(order_id, user_id, create_time, amount)
 * 
 * MV 定义:
 *   CREATE MATERIALIZED VIEW mv AS
 *   SELECT user_id, date_trunc(create_time, 'day') as dt, sum(amount) as total
 *   FROM orders
 *   GROUP BY user_id, date_trunc(create_time, 'day');
 * 
 * 用户查询:
 *   SELECT user_id, sum(amount) FROM orders
 *   WHERE create_time >= '2024-01-15 00:00:00' GROUP BY user_id;
 * 
 * 计划中的 ExprId（假设值，用于理解概念）:
 * 
 * ExprId 说明：# 后面的数字是 ExprId，是每个 NamedExpression 的唯一标识符。
 * 
 * ExprId 的生命周期和缓存机制：
 *   - ExprId 在 StatementContext 中生成，每个查询语句都会创建新的 StatementContext
 *   - ExprId 生成机制：IdGenerator 使用 nextId++（非原子操作，但在单线程环境下安全）
 *     * 每个查询的 ExprId 是独立的：每个查询创建独立的 StatementContext 和 IdGenerator
 *     * 单个查询在生成 ExprId 时没有并行：Nereids 优化器使用 SimpleJobScheduler（单线程串行调度器）
 *       所有优化任务（包括生成新表达式的任务）都在同一个线程中串行执行
 *     * 因此 nextId++ 虽然非原子，但在单线程环境下是安全的
 *   - MTMVCache 会缓存 MV 的 Plan 对象，如果缓存存在就直接复用（MTMV.getOrGenerateCache()）
 *   - 如果缓存被复用，Plan 中的 ExprId 保持不变（复用同一个 Plan 对象实例）
 *   - 只有当缓存失效（MV 刷新、schema 变化等）重新生成时，ExprId 才会变化
 *   - 因此：同一次 FE 重启周期内，如果缓存有效，ExprId 保持一致；缓存失效后重新生成，ExprId 会变化
 * 
 *   - 基表 orders 的列（基表列空间，shuttle 后的统一表示）:
 *     user_id#0, create_time#0, amount#0
 *     含义：在基表列空间中，这些列对应的 ExprId。
 *     说明：这里的 #0 表示在"基表列空间"中的统一表示。通过 shuttle 过程将不同位置的列引用统一到基表列。
 *     实际上，同一个逻辑列在不同计划位置可能有不同的 ExprId（如 MV 计划中的 user_id#10，查询中的 user_id#5），
 *     但 shuttle 后都会回溯到基表的原始列引用（如 user_id#0），这样便于匹配和比较。
 *   - MV 计划输出（originalPlan.getOutput()，实际包含 Alias）:
 *     [user_id#10, Alias(dt#12, date_trunc(create_time#11, 'day')), Alias(total#13, sum(amount#12))]
 *     （实际返回 NamedExpression 列表，有别名的表达式被 Alias 包装）
 *   - MV 计划输出展开后（用于说明，实际 shuttle 后会展开 Alias）:
 *     [user_id#10, date_trunc(create_time#11, 'day'), sum(amount#12)]
 *     （MV slot 空间：user_id#10, create_time#11, amount#12 是 MV 计划中的 slot）
 *   - MV scan 输出（scanPlan.getOutput()，MV 表的实际列）:
 *     [user_id#10, dt#12, sum(amount)#13]
 *     （dt#12 是 date_trunc 的别名 slot，sum(amount)#13 是聚合函数的输出 slot）
 *   - 查询中的 slot（查询 slot 空间）:
 *     user_id#5, create_time#6, amount#7
 *     含义：查询计划中分配的 ExprId，与 MV 计划中的 ExprId 不同（如 user_id#5 vs user_id#10）
 *     说明：查询和 MV 在不同的计划上下文中，同一个逻辑列会有不同的 ExprId。
 *           只有通过 shuttle 统一到基表列空间后，才能正确匹配（都变成 user_id#0）
 * 
 * ==================== Slot 空间（Slot Space）的概念说明 ====================
 * 
 * "Slot 空间"是指表达式所在的计划上下文，同一个逻辑列在不同计划中会有不同的 ExprId。
 * 
 * 1. 基表列空间（Base Table Column Space）:
 *    - 定义：所有列引用都回溯到基表的原始列引用，ExprId 统一
 *    - 示例：user_id#0, create_time#0, amount#0
 *    - 作用：作为统一的"参考坐标系"，用于不同计划之间的表达式匹配
 *    - 生成方式：通过 shuttle 过程将不同计划中的列引用统一到基表列
 * 
 * 2. MV slot 空间（MV Slot Space）:
 *    - 定义：MV 计划中的 slot，使用 MV 计划上下文中分配的 ExprId
 *    - 示例：user_id#10, create_time#11, amount#12（MV 计划中的 ExprId）
 *    - 特点：同一个逻辑列在 MV 计划中有独立的 ExprId（如 user_id#10）
 *    - 包含：MV 计划输出表达式中的 slot，可能包含 Alias 包装
 * 
 * 3. 查询 slot 空间（Query Slot Space）:
 *    - 定义：查询计划中的 slot，使用查询计划上下文中分配的 ExprId
 *    - 示例：user_id#5, create_time#6, amount#7（查询计划中的 ExprId）
 *    - 特点：同一个逻辑列在查询计划中有独立的 ExprId（如 user_id#5）
 * 
 * 为什么需要"slot 空间"的概念：
 *   - 不同计划上下文中的 ExprId 是独立的（查询计划中的 user_id#5 和 MV 计划中的 user_id#10 是不同的对象）
 *   - 无法直接通过 ExprId 比较来判断是否是同一个逻辑列
 *   - 需要通过 shuttle 统一到基表列空间（都变成 user_id#0），才能正确匹配
 *   - 这样，查询中的 user_id#5（shuttle 后变成 user_id#0）可以和 MV 中的 user_id#10（shuttle 后也变成 user_id#0）匹配
 */
public abstract class MaterializationContext {
    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);
    // 查询到物化视图的 slot 映射缓存，避免重复计算
    // 
    // 结构：Map<RelationMapping, SlotMapping>
    // - key: RelationMapping（查询表到 MV 表的映射关系）
    // - value: SlotMapping（查询 slot 到 MV slot 的映射关系）
    // 
    // 包含的 slot（基于统一的 SQL 示例）：
    // SlotMapping 按列名匹配，建立查询表的列 slot 到 MV 表的列 slot 的映射：
    //   {
    //     查询: user_id#5 -> MV: user_id#10,
    //     查询: create_time#6 -> MV: create_time#11,
    //     查询: amount#7 -> MV: amount#12
    //   }
    // 
    // 重要说明：并不是查询表达式中的所有 slot 都有映射
    // - 只包含基表的列 slot（从 getSlotNameToSlotMap() 获取）
    // - 只包含在查询表和 MV 表中都有相同列名的 slot
    // - 不包含：
    //   * 表达式产生的中间 slot（如 a + b 的结果）
    //   * 聚合函数的结果 slot（如 sum(amount)#13，虽然它可以通过 Alias.toSlot() 转换为 SlotReference，
    //     但它不是基表的列，而是聚合函数输出的表示，不会出现在基表的 getSlotNameToSlotMap() 中）
    //   * 别名产生的 slot（如 dt#12，虽然它也是 slot，但它是表达式的别名，不是基表列）
    // - 如果 MV 表中没有对应的列（如表结构变更新增列，或 variant 类型），该 slot 也不会有映射
    // 
    // 作用：用于表达式重写时，将查询表达式中的基表列 slot（如 user_id#5）替换为 MV 的列 slot（如 user_id#10）
    // 注意：一个查询可能有多个表组合（多个 RelationMapping），因此缓存中可能有多个映射
    public final Map<RelationMapping, SlotMapping> queryToMaterializationSlotMappingCache = new HashMap<>();
    // MV 依赖的基表列表
    protected List<Table> baseTables;
    // MV 依赖的基视图列表
    protected List<Table> baseViews;
    // 物化视图定义 SQL 的计划（已分析和优化）
    protected final Plan plan;
    // 物化视图 SQL 的原始计划（未优化）
    protected final Plan originalPlan;
    // scanPlan: MV 扫描计划，用于扫描物化视图表的实际数据
    // 
    // ==================== MV 计划 vs scanPlan 的区别 ====================
    // 
    // 1. MV 计划（plan/originalPlan）：MV 定义 SQL 对应的逻辑计划（定义了如何构建 MV）
    //    示例 SQL：CREATE MATERIALIZED VIEW mv AS
    //              SELECT user_id, date_trunc(create_time, 'day') as dt, sum(amount) as total
    //              FROM orders GROUP BY user_id, date_trunc(create_time, 'day')
    //    
    //    对应的计划树结构（简化）：
    //      LogicalAggregate(groupBy=[user_id, date_trunc(create_time, 'day')],
    //                       output=[user_id#10, Alias(dt#12, date_trunc(...)), Alias(total#13, sum(...))])
    //        └─ LogicalOlapScan(orders)
    //    
    //    作用：定义 MV 是如何从基表计算出来的（包含聚合、表达式计算等逻辑）
    //    输出：plan.getOutput() = [user_id#10, Alias(dt#12, ...), Alias(total#13, ...)]
    //         包含完整的表达式树（如 date_trunc 函数调用、聚合函数等）
    // 
    // 2. scanPlan：直接扫描已构建好的 MV 表的计划节点（读取已计算好的数据）
    //    本质：是一个 LogicalOlapScan 节点，直接扫描 MV 表（物化后的物理表）
    //    
    //    构建过程（通过 doGenerateScanPlan() 方法）:
    //      scanPlan = LogicalOlapScan(mv_table, indexId=xxx, partitionIds=[...])
    //      创建了一个扫描 MV 表的计划节点
    //    
    //    计划树结构（简化）：
    //      LogicalOlapScan(mv_table, indexId=xxx)
    //      只有一个扫描节点，没有聚合、计算等逻辑
    //    
    //    作用：用于查询重写，将查询改为读取已物化好的 MV 数据，而不是重新计算
    //    输出：scanPlan.getOutput() = [user_id#10, dt#12, sum(amount)#13]
    //         直接是 MV 表的列 slot，没有表达式树（因为数据已经计算好并存储在表中）
    //         注意：这里的 dt#12 和 sum(amount)#13 是 MV 表的列名对应的 slot
    // 
    // 3. scanPlanOutput = scanPlan.getOutput()：scanPlan 的输出 slot 列表
    //    基于统一的 SQL 示例：scanPlanOutput = [user_id#10, dt#12, sum(amount)#13]
    //    - user_id#10: MV 表中的 user_id 列对应的 slot
    //    - dt#12: MV 表中的 dt 列对应的 slot（存储的是 date_trunc(create_time, 'day') 的计算结果）
    //    - sum(amount)#13: MV 表中的 sum(amount) 列对应的 slot（存储的是聚合结果）
    // 
    // 核心区别总结：
    //   - MV 计划：描述"如何计算"（包含计算逻辑，如聚合、函数调用）
    //   - scanPlan：描述"如何读取"（只是一个表扫描，数据已经计算好）
    // 
    // 使用场景：
    //   1. 查询重写：将原始查询重写为使用 scanPlan（读取 MV 数据）而不是重新计算
    //   2. 补偿谓词：在 scanPlan 上应用补偿谓词（如 LogicalFilter(compensatePredicates, scanPlan)）
    // 
    // 注意：当一个查询可能多次命中同一个 MV 时，需要重新生成 scanPlan 以确保输出不同
    protected Plan scanPlan;
    // planOutputShuttledExpressions: MV 计划输出的 shuttle 后的表达式列表（统一到基表列空间）
    // 
    // 构建过程（基于统一的 SQL 示例）：
    // 1. MV 计划输出（originalPlan.getOutput()，实际包含 Alias）: 
    //    [user_id#10, Alias(dt#12, date_trunc(create_time#11, 'day')), Alias(total#13, sum(amount#12))]
    //    （originalPlan.getOutput() 返回 NamedExpression 列表，别名表达式被 Alias 包装）
    // 
    // 2. shuttle 过程（ExpressionUtils.shuttleExpressionWithLineage）:
    //    - 展开 Alias：Alias(dt#12, date_trunc(...)) -> date_trunc(create_time#11, 'day')
    //    - 替换 slot：将 MV slot 替换为基表列 slot
    //    - 结果：[user_id#0, date_trunc(create_time#0, 'day'), sum(amount#0)]
    //    转换为基表列的 slot（user_id#0, create_time#0, amount#0），并且去掉了 Alias 包装
    // 
    // 注意：shuttle 过程会展开 Alias，只保留底层表达式，所以这里不再有别名
    // 
    // 作用：用于生成 shuttledExprToScanExprMapping，建立基表列空间到 MV scan 输出的映射
    protected List<? extends Expression> planOutputShuttledExpressions;
    
    // exprToScanExprMapping: 从 MV 计划输出表达式（MV slot 空间，包含 Alias）到 MV scan 输出 slot 的映射
    // 
    // MV slot 空间说明：
    // - MV slot 空间是指 MV 计划中的 slot 所处的命名空间/上下文
    // - 使用 MV 计划上下文中分配的 ExprId（如 user_id#10, create_time#11, amount#12）
    // - 与查询 slot 空间不同：同一个逻辑列在查询中是 user_id#5，在 MV 中是 user_id#10
    // - 包含完整的表达式结构（可能包含 Alias 包装），如 Alias(dt#12, date_trunc(create_time#11, 'day'))
    // 
    // 构建过程（基于统一的 SQL 示例）：
    // 1. originalPlan.getOutput(): MV 计划输出的 NamedExpression（实际包含 Alias）
    //    [user_id#10, Alias(dt#12, date_trunc(create_time#11, 'day')), Alias(total#13, sum(amount#12))]
    //    （实际的计划输出包含 Alias 包装）
    // 
    // 2. scanPlanOutput: MV scan 的输出 slot（MV scan 的实际输出列）
    //    [user_id#10, dt#12, sum(amount)#13]
    //    dt#12 是 date_trunc(create_time, 'day') 的别名 slot，sum(amount)#13 是聚合函数的输出 slot
    // 
    // 3. exprToScanExprMapping: 按索引位置一一对应
    //    {
    //      user_id#10 -> user_id#10,
    //      Alias(dt#12, date_trunc(create_time#11, 'day')) -> dt#12,
    //      Alias(total#13, sum(amount#12)) -> sum(amount)#13
    //    }
    //    注意：这里 key 是包含 Alias 的表达式，value 是 MV scan 的别名 slot
    // 
    // 作用：用于统计信息列表达式的规范化（normalize statistics column expression）
    // 注意：与 shuttledExprToScanExprMapping 的区别是，这里的 key 是 MV slot 空间（包含 Alias），而不是基表列空间
    protected Map<Expression, Expression> exprToScanExprMapping = new HashMap<>();
    
    // shuttledExprToScanExprMapping: 从 shuttle 后的表达式（基表列空间）到 MV scan 输出 slot 的映射
    // 
    // 构建过程（基于统一的 SQL 示例）：
    // 1. planOutputShuttledExpressions: MV 计划输出的 shuttle 后的表达式（统一到基表列空间）
    //    [user_id#0, date_trunc(create_time#0, 'day'), sum(amount#0)]
    // 
    // 2. scanPlanOutput: MV scan 的输出 slot（MV scan 的实际输出列）
    //    [user_id#10, dt#12, sum(amount)#13]
    // 
    // 3. shuttledExprToScanExprMapping: 通过 ExpressionMapping.generate() 生成
    //    {
    //      user_id#0 -> user_id#10,
    //      date_trunc(create_time#0, 'day') -> dt#12,
    //      sum(amount#0) -> sum(amount)#13
    //    }
    //    key: shuttle 后的表达式（基表列空间）
    //    value: MV scan 的输出 slot
    // 
    // 作用：用于 rewriteExpression() 方法，将查询表达式（基表列空间）中的列引用替换为 MV scan 的输出列
    // 示例：查询表达式 create_time#0 >= '2024-01-15' 可以替换为 dt#12 >= '2024-01-15'
    protected ExpressionMapping shuttledExprToScanExprMapping;
    // 标记物化视图上下文是否可用。如果为 false，将不会用于查询透明重写
    protected boolean available = true;
    // 标记物化视图计划是否已经成功重写。用于判断是否需要重新生成 scanPlan
    protected boolean success = false;
    // 是否启用记录失败详细信息。记录失败详情会影响性能，所以需要开关控制
    protected final boolean enableRecordFailureDetail;
    // 物化视图计划的结构信息。构建 StructInfo 开销较大，因此只构建一次供所有查询复用
    protected final StructInfo structInfo;
    // 重写失败的 group id 集合，用于减少重复重写尝试
    protected final Set<GroupId> matchedFailGroups = new HashSet<>();
    // 重写成功的 group id 集合，用于减少重复重写尝试
    protected final Set<GroupId> matchedSuccessGroups = new HashSet<>();
    // 记录重写失败的原因。key 是查询所属的 group expression 的 objectId，
    // value 是失败原因。因为嵌套物化视图时，一个物化视图可能对应多个查询
    protected final Multimap<ObjectId, Pair<String, String>> failReason = HashMultimap.create();
    // MV 的标识符列表（用于日志和调试）
    protected List<String> identifier;

    /**
     * MaterializationContext, this contains necessary info for query rewriting by materialization
     */
    public MaterializationContext(Plan plan, Plan originalPlan,
            CascadesContext cascadesContext, StructInfo structInfo) {
        this.plan = plan;
        this.originalPlan = originalPlan;
        StatementBase parsedStatement = cascadesContext.getStatementContext().getParsedStatement();
        this.enableRecordFailureDetail = parsedStatement != null && parsedStatement.isExplain()
                && ExplainLevel.MEMO_PLAN == parsedStatement.getExplainOptions().getExplainLevel();
        // Construct materialization struct info, catch exception which may cause planner roll back
        this.structInfo = structInfo == null
                ? constructStructInfo(plan, originalPlan, cascadesContext, new BitSet()).orElseGet(() -> null)
                : structInfo;
        this.available = this.structInfo != null;
        if (available) {
            this.planOutputShuttledExpressions = this.structInfo.getPlanOutputShuttledExpressions();
        }
    }

    /**
     * 构建物化视图的 StructInfo。
     * 
     * StructInfo 用于存储物化视图计划的结构化信息，包括：
     * - 计划的拓扑结构
     * - 表的使用情况
     * - 谓词信息
     * - 聚合信息
     * 这些信息用于后续的 MV 匹配和重写。
     * 
     * @param plan 物化视图的计划（可能已经移除了不必要的计划节点，逻辑输出可能不正确）
     * @param originalPlan 物化视图的原始计划（逻辑输出是正确的）
     * @param cascadesContext 优化器上下文
     * @param expectedTableBitSet 期望的表位集合，用于过滤和验证表的使用
     * @return StructInfo 的 Optional，如果构建失败则返回 empty
     */
    public static Optional<StructInfo> constructStructInfo(Plan plan, Plan originalPlan,
            CascadesContext cascadesContext, BitSet expectedTableBitSet) {
        List<StructInfo> viewStructInfos;
        try {
            // 从计划中提取 StructInfo
            // MaterializedViewUtils.extractStructInfo 会分析计划结构，提取表、谓词、聚合等信息
            viewStructInfos = MaterializedViewUtils.extractStructInfo(plan, originalPlan,
                    cascadesContext, expectedTableBitSet);
            
            // 正常情况下，一个物化视图应该只有一个 StructInfo
            // 如果提取到多个，说明计划结构异常，记录警告但使用第一个
            if (viewStructInfos.size() > 1) {
                LOG.warn(String.format("view strut info is more than one, materialization plan is %s",
                        plan.treeString()));
            }
        } catch (Exception exception) {
            // 如果提取 StructInfo 失败（可能是计划结构不合法或其他异常），记录警告并返回 empty
            // 这会导致 MaterializationContext 的 available 标志为 false，不会用于查询重写
            LOG.warn(String.format("construct materialization struct info fail, materialization plan is %s",
                    plan.treeString()), exception);
            return Optional.empty();
        }
        // 返回第一个（也是唯一的）StructInfo
        return Optional.of(viewStructInfos.get(0));
    }

    public boolean alreadyRewrite(GroupId groupId) {
        return this.matchedFailGroups.contains(groupId) || this.matchedSuccessGroups.contains(groupId);
    }

    public void addMatchedGroup(GroupId groupId, boolean rewriteSuccess) {
        if (rewriteSuccess) {
            this.matchedSuccessGroups.add(groupId);
        } else {
            this.matchedFailGroups.add(groupId);
        }
    }

    /**
     * 生成物化视图的 scan plan 并构建相关的映射关系。
     * 如果 MaterializationContext 已经成功重写，在后续查询重写中需要重新生成 scan plan，
     * 因为一个查询计划可能多次命中同一个物化视图，而每次命中的 MV scan 输出可能不同。
     */
    public void tryGenerateScanPlan(CascadesContext cascadesContext) {
        if (!this.isAvailable()) {
            return;
        }
        // 步骤1：生成 MV 的 scan plan（LogicalOlapScan 节点）
        this.scanPlan = doGenerateScanPlan(cascadesContext);
        
        // 步骤2：获取 MV scan 的输出 slot 列表
        List<Slot> scanPlanOutput = this.scanPlan.getOutput();
        
        // 步骤3：构建 shuttledExprToScanExprMapping（从 shuttle 后的表达式到 MV scan 输出 slot 的映射）
        // 
        // 映射构建过程（基于统一的 SQL 示例）：
        // - planOutputShuttledExpressions: [user_id#0, date_trunc(create_time#0, 'day'), sum(amount#0)]
        //   （MV 计划输出的 shuttle 后的表达式，统一到基表列空间）
        // - scanPlanOutput: [user_id#10, dt#12, sum(amount)#13]（MV scan 的输出 slot）
        // - 按索引位置一一对应，生成映射：
        //   {
        //     user_id#0 -> user_id#10,
        //     date_trunc(create_time#0, 'day') -> dt#12,
        //     sum(amount#0) -> sum(amount)#13
        //   }
        // 
        // 作用：用于 rewriteExpression()，将查询表达式（基表列空间）替换为 MV scan 的输出列
        // 示例：查询表达式 create_time#0 >= '2024-01-15' 可以通过 date_trunc 映射替换为 dt#12 >= '2024-01-15'
        this.shuttledExprToScanExprMapping = ExpressionMapping.generate(this.planOutputShuttledExpressions,
                scanPlanOutput);
        
        // 步骤4：构建 exprToScanExprMapping（从 MV 计划输出到 MV scan 输出 slot 的映射）
        // 
        // 映射构建过程（基于统一的 SQL 示例）：
        // - originalPlanOutput: [user_id#10, Alias(dt#12, date_trunc(...)), Alias(total#13, sum(...))]
        //   （MV 计划输出的 NamedExpression，包含 Alias）
        // - scanPlanOutput: [user_id#10, dt#12, sum(amount)#13]（MV scan 的输出 slot）
        // - 按索引位置一一对应，生成映射：
        //   {
        //     user_id#10 -> user_id#10,
        //     Alias(dt#12, date_trunc(...)) -> dt#12,
        //     Alias(total#13, sum(...)) -> sum(amount)#13
        //   }
        // 
        // 作用：用于 normalize statistics column expression（规范化统计信息列表达式）
        // 注意：与 shuttledExprToScanExprMapping 的区别是，这里的 key 是 MV slot 空间（包含 Alias），而不是基表列空间
        Map<Expression, Expression> regeneratedMapping = new HashMap<>();
        List<Slot> originalPlanOutput = originalPlan.getOutput();
        // 只有当两者大小相等时才建立映射（确保索引位置一一对应）
        if (originalPlanOutput.size() == scanPlanOutput.size()) {
            for (int slotIndex = 0; slotIndex < originalPlanOutput.size(); slotIndex++) {
                regeneratedMapping.put(originalPlanOutput.get(slotIndex), scanPlanOutput.get(slotIndex));
            }
        }
        this.exprToScanExprMapping = regeneratedMapping;
    }

    /**
     * Should clear scan plan after materializationContext is already rewritten successfully,
     * Because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different.
     */
    public void clearScanPlan(CascadesContext cascadesContext) {
        this.scanPlan = null;
        this.shuttledExprToScanExprMapping = null;
        this.exprToScanExprMapping = null;
    }

    public void addSlotMappingToCache(RelationMapping relationMapping, SlotMapping slotMapping) {
        queryToMaterializationSlotMappingCache.put(relationMapping, slotMapping);
    }

    public SlotMapping getSlotMappingFromCache(RelationMapping relationMapping) {
        return queryToMaterializationSlotMappingCache.get(relationMapping);
    }

    /**
     * Try to generate scan plan for materialization
     * if MaterializationContext is already rewritten successfully, then should generate new scan plan in later
     * query rewrite, because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different
     */
    abstract Plan doGenerateScanPlan(CascadesContext cascadesContext);

    /**
     * Get materialization unique identifier which identify it
     */
    public abstract List<String> generateMaterializationIdentifier();

    /**
     * Common method for generating materialization identifier by index name
     */
    public static List<String> generateMaterializationIdentifier(OlapTable olapTable, String indexName) {
        return indexName == null
                ? ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                        ClusterNamespace.getNameFromFullName(olapTable.getDatabase().getFullName()),
                        olapTable.getName())
                : ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                        ClusterNamespace.getNameFromFullName(olapTable.getDatabase().getFullName()),
                        olapTable.getName(), indexName);
    }

    /**
     * Common method for generating materialization identifier by index id
     */
    public static List<String> generateMaterializationIdentifierByIndexId(OlapTable olapTable, Long indexId) {
        return indexId == null
                ? ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                ClusterNamespace.getNameFromFullName(olapTable.getDatabase().getFullName()),
                olapTable.getName())
                : ImmutableList.of(olapTable.getDatabase().getCatalog().getName(),
                        ClusterNamespace.getNameFromFullName(olapTable.getDatabase().getFullName()),
                        olapTable.getName(), olapTable.getIndexNameById(indexId));
    }

    /**
     * Get String info which is used for to string
     */
    abstract String getStringInfo();

    /**
     * Get materialization plan statistics,
     * the key is the identifier of statistics which is usual the scan plan relationId or something similar
     * the value is original plan statistics.
     * the statistics is used by cost estimation when the materialization is used
     * Which should be the materialization origin plan statistics
     */
    abstract Optional<Pair<Id, Statistics>> getPlanStatistics(CascadesContext cascadesContext);

    // original plan statistics is generated by origin plan, and the column expression in statistics
    // should be keep consistent to mv scan plan
    protected Statistics normalizeStatisticsColumnExpression(Statistics originalPlanStatistics) {
        Map<Expression, ColumnStatistic> normalizedExpressionMap = new HashMap<>();
        // this statistics column expression is materialization origin plan, should normalize it to
        // materialization scan plan
        for (Map.Entry<Expression, ColumnStatistic> entry : originalPlanStatistics.columnStatistics().entrySet()) {
            Expression targetExpression = entry.getKey();
            Expression sourceExpression = this.getExprToScanExprMapping().get(targetExpression);
            if (sourceExpression != null && targetExpression instanceof NamedExpression
                    && sourceExpression instanceof NamedExpression) {
                normalizedExpressionMap.put(MaterializedViewUtils.normalizeExpression(
                        (NamedExpression) sourceExpression, (NamedExpression) targetExpression, false).toSlot(),
                        entry.getValue());
            }
        }
        return originalPlanStatistics.withExpressionToColumnStats(normalizedExpressionMap);
    }

    /**
     * Calc the relation is chosen finally or not
     */
    abstract boolean isFinalChosen(Relation relation);

    public Plan getPlan() {
        return plan;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public Plan getScanPlan(StructInfo queryStructInfo, CascadesContext cascadesContext) {
        if (this.scanPlan == null || this.shuttledExprToScanExprMapping == null
                || this.exprToScanExprMapping == null) {
            tryGenerateScanPlan(cascadesContext);
        }
        return scanPlan;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public List<Table> getBaseViews() {
        return baseViews;
    }

    public Map<Expression, Expression> getExprToScanExprMapping() {
        return exprToScanExprMapping;
    }

    public ExpressionMapping getShuttledExprToScanExprMapping() {
        return shuttledExprToScanExprMapping;
    }

    public boolean isAvailable() {
        return available;
    }

    public Multimap<ObjectId, Pair<String, String>> getFailReason() {
        return failReason;
    }

    public boolean isEnableRecordFailureDetail() {
        return enableRecordFailureDetail;
    }

    public void setSuccess(boolean success) {
        this.success = success;
        // TODO clear the fail message by according planId ?
        this.failReason.clear();
    }

    public StructInfo getStructInfo() {
        return structInfo;
    }

    public boolean isSuccess() {
        return success;
    }

    /**
     * Record fail reason when in rewriting by struct info
     */
    public void recordFailReason(StructInfo structInfo, String summary, Supplier<String> failureReasonSupplier) {
        // record it's rewritten
        if (structInfo.getTopPlan().getGroupExpression().isPresent()) {
            this.addMatchedGroup(structInfo.getTopPlan().getGroupExpression().get().getOwnerGroup().getGroupId(),
                    false);
        }
        // once success, do not record the fail reason
        if (this.success) {
            return;
        }
        this.failReason.put(structInfo.getOriginalPlanId(),
                Pair.of(summary, this.isEnableRecordFailureDetail() ? failureReasonSupplier.get() : ""));
    }

    /**
     * Record fail reason when in rewriting by queryGroupPlan
     */
    public void recordFailReason(Plan queryGroupPlan, String summary, Supplier<String> failureReasonSupplier) {
        // record it's rewritten
        if (queryGroupPlan.getGroupExpression().isPresent()) {
            this.addMatchedGroup(queryGroupPlan.getGroupExpression().get().getOwnerGroup().getGroupId(),
                    false);
        }
        // once success, do not record the fail reason
        if (this.success) {
            return;
        }
        this.failReason.put(queryGroupPlan.getGroupExpression()
                        .map(GroupExpression::getId).orElseGet(() -> new ObjectId(-1)),
                Pair.of(summary, this.isEnableRecordFailureDetail() ? failureReasonSupplier.get() : ""));
    }

    /**
     * get materialization context common table id by current statementContext
     */
    public BitSet getCommonTableIdSet(StatementContext statementContext) {
        BitSet commonTableId = new BitSet();
        for (StructInfoNode node : structInfo.getRelationIdStructInfoNodeMap().values()) {
            for (CatalogRelation catalogRelation : node.getCatalogRelation()) {
                commonTableId.set(statementContext.getTableId(catalogRelation.getTable()).asInt());
            }
        }
        return commonTableId;
    }

    @Override
    public String toString() {
        return getStringInfo();
    }

    /**
     * get qualifiers for all mvs rewrite success and chosen by current query.
     *
     * @param materializationContexts all mv candidates context for current query
     * @param physicalPlan the chosen plan for current query
     * @return chosen mvs' qualifier set
     */
    public static Set<List<String>> getChosenMvsQualifiers(
            List<MaterializationContext> materializationContexts, Plan physicalPlan) {
        Set<MaterializationContext> rewrittenSuccessMaterializationSet = materializationContexts.stream()
                .filter(MaterializationContext::isSuccess)
                .collect(Collectors.toSet());
        Set<List<String>> chosenMaterializationQualifiers = new HashSet<>();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Void>() {
            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation, Void context) {
                for (MaterializationContext rewrittenContext : rewrittenSuccessMaterializationSet) {
                    if (rewrittenContext.isFinalChosen(physicalRelation)) {
                        chosenMaterializationQualifiers.add(rewrittenContext.generateMaterializationIdentifier());
                    }
                }
                return null;
            }

            @Override
            public Void visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan lazyScan, Void context) {
                PhysicalOlapScan physicalRelation = lazyScan.getScan();
                for (MaterializationContext rewrittenContext : rewrittenSuccessMaterializationSet) {
                    if (rewrittenContext.isFinalChosen(physicalRelation)) {
                        chosenMaterializationQualifiers.add(rewrittenContext.generateMaterializationIdentifier());
                    }
                }
                return null;
            }
        }, null);
        return chosenMaterializationQualifiers;
    }

    /**
     * ToSummaryString, this contains only summary info.
     */
    public static String toSummaryString(CascadesContext cascadesContext,
            Plan physicalPlan) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        if (materializationContexts.isEmpty()) {
            return "";
        }
        Set<List<String>> chosenMaterializationQualifiers = getChosenMvsQualifiers(
                materializationContexts, physicalPlan);

        StringBuilder builder = new StringBuilder();
        builder.append("\nMaterializedView");
        // rewrite success and chosen
        builder.append("\nMaterializedViewRewriteSuccessAndChose:\n");
        if (!chosenMaterializationQualifiers.isEmpty()) {
            chosenMaterializationQualifiers.forEach(materializationQualifier ->
                    builder.append(statementContext.isPreMvRewritten() ? " RBO." : " CBO.")
                            .append(generateIdentifierName(materializationQualifier)).append(" chose\n"));
        }
        // rewrite success but not chosen
        builder.append("\nMaterializedViewRewriteSuccessButNotChose:\n");
        Set<List<String>> rewriteSuccessButNotChoseQualifiers = materializationContexts.stream()
                .filter(MaterializationContext::isSuccess)
                .map(MaterializationContext::generateMaterializationIdentifier)
                .filter(materializationQualifier -> !chosenMaterializationQualifiers.contains(materializationQualifier))
                .collect(Collectors.toSet());
        if (!rewriteSuccessButNotChoseQualifiers.isEmpty()) {
            rewriteSuccessButNotChoseQualifiers.forEach(materializationQualifier ->
                    builder.append(statementContext.isPreMvRewritten() ? " RBO." : " CBO.")
                            .append(generateIdentifierName(materializationQualifier)).append(" not chose\n"));
        }
        // rewrite fail
        builder.append("\nMaterializedViewRewriteFail:");
        for (MaterializationContext ctx : materializationContexts) {
            if (!ctx.isSuccess()) {
                Set<String> failReasonSet =
                        ctx.getFailReason().values().stream().map(Pair::key).collect(ImmutableSet.toImmutableSet());
                if (ctx.isEnableRecordFailureDetail()) {
                    failReasonSet = ctx.getFailReason().values().stream()
                            .map(Pair::toString)
                            .collect(ImmutableSet.toImmutableSet());
                }
                builder.append("\n")
                        .append(statementContext.isPreMvRewritten() ? " RBO." : " CBO.")
                        .append(generateIdentifierName(ctx.generateMaterializationIdentifier())).append(" fail\n")
                        .append("  FailInfo: ").append(String.join(", ", failReasonSet));
            }
        }
        return builder.toString();
    }

    /**
     * If materialized view rewrite duration is exceeded, make all materializationContexts with reason
     * materialized view rewrite duration is exceeded
     * */
    public static void makeFailWithDurationExceeded(Plan queryPlan,
            List<MaterializationContext> materializationContexts, long duration) {
        for (MaterializationContext context : materializationContexts) {
            if (context.isSuccess()) {
                continue;
            }
            context.recordFailReason(queryPlan,
                    "materialized view rewrite duration is exceeded, the duration is " + duration,
                    () -> "materialized view rewrite duration is exceeded, the duration is " + duration);
        }
    }

    private static String generateIdentifierName(List<String> qualifiers) {
        return String.join(".", qualifiers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaterializationContext context = (MaterializationContext) o;
        return generateMaterializationIdentifier().equals(context.generateMaterializationIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(generateMaterializationIdentifier());
    }
}
