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

package org.apache.doris.nereids;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.FieldInfo;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.minidump.NereidsTracer;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.processor.pre.PlanPreprocessors;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.ComputeResultSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.distribute.DistributePlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDictionarySink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSqlCache;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TQueryCacheParam;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Planner to do query plan in Nereids.
 */
public class NereidsPlanner extends Planner {
    // private static final AtomicInteger executeCount = new AtomicInteger(0);
    public static final Logger LOG = LogManager.getLogger(NereidsPlanner.class);

    public static AtomicLong runningPlanNum = new AtomicLong(0L);
    protected Plan parsedPlan;
    protected Plan analyzedPlan;
    protected Plan rewrittenPlan;
    protected Plan optimizedPlan;
    protected PhysicalPlan physicalPlan;

    private CascadesContext cascadesContext;
    private final StatementContext statementContext;
    private final List<ScanNode> scanNodeList = Lists.newArrayList();
    private final List<PhysicalRelation> physicalRelations = Lists.newArrayList();
    private DescriptorTable descTable;

    private FragmentIdMapping<DistributedPlan> distributedPlans;
    // The cost of optimized plan
    private double cost = 0;
    private LogicalPlanAdapter logicalPlanAdapter;

    public NereidsPlanner(StatementContext statementContext) {
        this.statementContext = statementContext;
    }

    @Override
    public void plan(StatementBase queryStmt, org.apache.doris.thrift.TQueryOptions queryOptions) throws UserException {
        this.queryOptions = queryOptions;
        if (statementContext.getConnectContext().getSessionVariable().isEnableNereidsTrace()) {
            NereidsTracer.init();
        } else {
            NereidsTracer.disable();
        }
        if (!(queryStmt instanceof LogicalPlanAdapter)) {
            throw new RuntimeException("Wrong type of queryStmt, expected: <? extends LogicalPlanAdapter>");
        }

        logicalPlanAdapter = (LogicalPlanAdapter) queryStmt;

        ExplainLevel explainLevel = getExplainLevel(queryStmt.getExplainOptions());

        LogicalPlan parsedPlan = logicalPlanAdapter.getLogicalPlan();
        NereidsTracer.logImportantTime("EndParsePlan");
        setParsedPlan(parsedPlan);

        PhysicalProperties requireProperties = buildInitRequireProperties();
        statementContext.getStopwatch().reset().start();
        NereidsPlanner.runningPlanNum.incrementAndGet();
        try {
            boolean showPlanProcess = showPlanProcess(queryStmt.getExplainOptions());
            planWithLock(parsedPlan, requireProperties, explainLevel, showPlanProcess, plan -> {
                setOptimizedPlan(plan);
                if (plan instanceof PhysicalPlan) {
                    physicalPlan = (PhysicalPlan) plan;
                    distribute(physicalPlan, explainLevel);
                }
            });
        } finally {
            statementContext.getStopwatch().stop();
            NereidsPlanner.runningPlanNum.decrementAndGet();
        }

        if (LOG.isDebugEnabled()) {
            LOG.info(getExplainString(new ExplainOptions(ExplainLevel.SHAPE_PLAN, false)));
            LOG.info(getExplainString(new ExplainOptions(ExplainLevel.DISTRIBUTED_PLAN, false)));
        }
    }

    @VisibleForTesting
    public void plan(StatementBase queryStmt) {
        try {
            plan(queryStmt, statementContext.getConnectContext().getSessionVariable().toThrift());
        } catch (Exception e) {
            throw new NereidsException(e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public PhysicalPlan planWithLock(LogicalPlan plan, PhysicalProperties outputProperties) {
        return (PhysicalPlan) planWithLock(plan, outputProperties, ExplainLevel.NONE, false);
    }

    // TODO check all caller
    public Plan planWithLock(LogicalPlan plan, PhysicalProperties requireProperties, ExplainLevel explainLevel) {
        return planWithLock(plan, requireProperties, explainLevel, false);
    }

    @VisibleForTesting
    public Plan planWithLock(LogicalPlan plan, PhysicalProperties requireProperties,
            ExplainLevel explainLevel, boolean showPlanProcess) {
        Consumer<Plan> noCallback = p -> {};
        return planWithLock(plan, requireProperties, explainLevel, showPlanProcess, noCallback);
    }

    /**
     * 执行查询计划的分析和优化，并管理表锁。
     * 
     * 这是 Nereids 优化器的核心入口方法，负责执行从逻辑计划到物理计划的完整优化流程。
     * 方法会按顺序执行以下步骤：
     * 1. 特殊处理：SQL Cache 查询的快速路径
     * 2. Explain 级别处理：根据 explainLevel 决定是否记录解析计划
     * 3. 预处理：处理 SET_VAR hint 等逻辑计划预处理
     * 4. 初始化 CascadesContext：创建优化器上下文
     * 5. 收集并锁定表：按表 ID 顺序获取表锁，确保并发安全
     * 6. 执行优化：调用 planWithoutLock 执行完整的分析和优化流程
     * 7. 资源清理：在 finally 块中释放规划器资源
     * 
     * ==================== 执行流程 ====================
     * 
     * 示例：SELECT * FROM orders WHERE id = 5
     * 
     * 步骤1：检查是否为 SQL Cache 查询
     * - 如果是 LogicalSqlCache，直接转换为 PhysicalSqlCache，跳过优化
     * - 原因：SQL Cache 查询已经缓存了结果，无需重新优化
     * 
     * 步骤2：处理 Explain 级别
     * - PARSED_PLAN：只返回解析后的计划，不进行优化
     * - ALL_PLAN：记录解析计划，继续后续优化
     * 
     * 步骤3：预处理逻辑计划
     * - 处理 SET_VAR hint：设置会话变量
     * - 其他预处理逻辑
     * 
     * 步骤4：初始化 CascadesContext
     * - 创建优化器上下文，包含 Memo、统计信息等
     * - 设置物理属性要求（如数据分布、排序等）
     * 
     * 步骤5：收集并锁定表
     * - 收集查询涉及的所有表
     * - 按表 ID 顺序获取表锁，避免死锁
     * - 锁会在查询执行完成后释放
     * 
     * 步骤6：执行优化（planWithoutLock）
     * - 分析阶段：绑定表、列、函数等
     * - 重写阶段：应用 RBO 规则（如谓词下推、常量折叠）
     * - 优化阶段：应用 CBO 规则（如 Join 重排序、物化视图重写）
     * - 实现阶段：将逻辑计划转换为物理计划
     * 
     * 步骤7：资源清理
     * - 释放规划器占用的资源
     * - 记录 GC 时间（用于性能分析）
     * 
     * @param plan 待优化的逻辑计划
     * @param requireProperties 请求的物理属性约束（如数据分布、排序）
     * @param explainLevel Explain 级别，决定是否记录计划过程
     * @param showPlanProcess 是否记录计划过程到 CascadesContext
     * @param lockCallback 表锁回调函数，在获取表锁后调用
     * @return 优化后生成的计划（可能是逻辑计划或物理计划）
     * @throws AnalysisException 如果在任何阶段失败则抛出异常
     */
    private Plan planWithLock(LogicalPlan plan, PhysicalProperties requireProperties,
            ExplainLevel explainLevel, boolean showPlanProcess, Consumer<Plan> lockCallback) {
        try {
            // 记录优化前的 GC 时间，用于性能分析
            long beforePlanGcTime = getGarbageCollectionTime();
            
            // ==================== 特殊处理：SQL Cache 查询 ====================
            // SQL Cache 查询已经缓存了结果，无需重新优化，直接转换为物理计划
            if (plan instanceof LogicalSqlCache) {
                rewrittenPlan = analyzedPlan = plan;
                LogicalSqlCache logicalSqlCache = (LogicalSqlCache) plan;
                // 直接创建 PhysicalSqlCache，跳过优化流程
                optimizedPlan = physicalPlan = new PhysicalSqlCache(
                        logicalSqlCache.getQueryId(),
                        logicalSqlCache.getColumnLabels(), logicalSqlCache.getFieldInfos(),
                        logicalSqlCache.getResultExprs(), logicalSqlCache.getResultSetInFe(),
                        logicalSqlCache.getCacheValues(), logicalSqlCache.getBackendAddress(),
                        logicalSqlCache.getPlanBody()
                );
                // 如果需要记录计划过程（Explain），创建 CascadesContext 并记录
                if (explainLevel != ExplainLevel.NONE) {
                    this.cascadesContext = CascadesContext.initContext(
                            statementContext, parsedPlan, PhysicalProperties.ANY);
                    switch (explainLevel) {
                        case OPTIMIZED_PLAN:
                        case ALL_PLAN:
                            // 记录 SQL Cache 的实现过程
                            cascadesContext.addPlanProcess(
                                    new PlanProcess("ImplementSqlCache",
                                            parsedPlan.treeString(false, parsedPlan),
                                            physicalPlan.treeString(false, physicalPlan)
                                    )
                            );
                            break;
                        default: {
                        }
                    }
                }
                return physicalPlan;
            }
            
            // ==================== 处理 Explain 级别 ====================
            // 如果只需要解析计划（PARSED_PLAN），直接返回，不进行优化
            if (explainLevel == ExplainLevel.PARSED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
                parsedPlan = plan;
                if (explainLevel == ExplainLevel.PARSED_PLAN) {
                    return parsedPlan;
                }
                // ALL_PLAN 会继续后续优化流程
            }

            // ==================== 预处理逻辑计划 ====================
            // 在进入 Memo 之前，对逻辑计划进行预处理
            // 例如：处理 SET_VAR hint，设置会话变量
            plan = preprocess(plan);

            // ==================== 初始化 CascadesContext ====================
            // 创建优化器上下文，包含 Memo、统计信息、物理属性要求等
            // CascadesContext 是优化器的核心数据结构，用于存储优化过程中的状态
            initCascadesContext(plan, requireProperties);
            
            // ==================== 收集并锁定表 ====================
            // 收集查询涉及的所有表，并按表 ID 顺序获取表锁
            // 按顺序锁定可以避免死锁（多个查询同时访问多个表时）
            // showAnalyzeProcess 决定是否在分析阶段显示过程
            collectAndLockTable(showAnalyzeProcess(explainLevel, showPlanProcess));
            
            // ==================== 执行优化 ====================
            // 注意：表收集后应该使用新的上下文（因为表锁已经获取）
            // planWithoutLock 执行完整的分析和优化流程：
            // 1. 分析阶段：绑定表、列、函数等
            // 2. 重写阶段：应用 RBO 规则（谓词下推、常量折叠等）
            // 3. 优化阶段：应用 CBO 规则（Join 重排序、物化视图重写等）
            // 4. 实现阶段：将逻辑计划转换为物理计划
            Plan resultPlan = planWithoutLock(plan, requireProperties, explainLevel, showPlanProcess);
            
            // ==================== 执行表锁回调 ====================
            // 在获取表锁后调用回调函数，允许调用者执行额外的操作
            lockCallback.accept(resultPlan);
            
            // ==================== 记录 GC 时间 ====================
            // 记录优化过程中的 GC 时间，用于性能分析和调优
            if (statementContext.getConnectContext().getExecutor() != null) {
                statementContext.getConnectContext().getExecutor().getSummaryProfile()
                        .setNereidsGarbageCollectionTime(getGarbageCollectionTime() - beforePlanGcTime);
            }
            return resultPlan;
        } finally {
            // ==================== 资源清理 ====================
            // 无论成功还是失败，都要释放规划器占用的资源
            // 包括：释放表锁、清理临时数据结构、关闭连接等
            statementContext.releasePlannerResources();
        }
    }

    /**
     * 执行完整的查询计划过程，但不获取表锁
     * 
     * 这是 Nereids 优化器的核心方法，执行从逻辑计划到物理计划的完整优化流程。
     * 按顺序执行以下阶段：
     * 
     * 1. 输入序列化：将输入逻辑计划序列化到 minidump 文件，用于调试和回放
     * 2. 分析阶段：解析列、表、函数，对逻辑计划进行语义分析
     * 3. 重写阶段：应用启发式重写规则（如谓词下推、列裁剪、常量折叠）优化逻辑计划
     * 4. 物化视图预重写：如果适用，尝试使用物化视图重写查询
     * 5. 优化阶段：使用基于成本的优化，探索替代的物理计划并选择最优的
     * 6. 计划选择：根据会话变量配置选择第 N 个最优物理计划
     * 7. 后处理：应用后优化转换（如运行时过滤器生成、分布式计划生成）
     * 8. 输出序列化：将最终物理计划序列化到 minidump 文件
     * 
     * 注意：该方法不会获取表锁，调用者（通常是 planWithLock）需要在调用前锁定表
     * 
     * @param plan 待优化的输入逻辑计划
     * @param requireProperties 输出计划必须满足的物理属性要求（如分布、排序）
     * @param explainLevel 解释级别，决定执行多少优化过程以及返回什么计划
     *                    ANALYZED_PLAN: 分析阶段后返回
     *                    REWRITTEN_PLAN: 重写阶段后返回
     *                    OPTIMIZED_PLAN: 优化阶段后返回
     *                    ALL_PLAN: 执行所有阶段并返回最终计划
     *                    NONE: 执行所有阶段但不存储中间计划
     * @param showPlanProcess 是否记录并显示计划转换过程（用于调试和性能分析）
     * @return 优化后的物理计划，或者如果 explainLevel 要求则返回中间计划
     */
    private Plan planWithoutLock(
            LogicalPlan plan, PhysicalProperties requireProperties, ExplainLevel explainLevel,
            boolean showPlanProcess) {
        // 阶段1: 输入序列化 - 将输入逻辑计划序列化到 minidump 文件，用于调试和回放
        // 必须在最开始序列化，确保 minidump 字符串不为空
        try {
            MinidumpUtils.serializeInputsToDumpFile(plan, statementContext);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        // 阶段2: 分析阶段 - 解析列、表、函数，进行语义分析
        analyze(showAnalyzeProcess(explainLevel, showPlanProcess));
        // 如果只需要分析后的计划，直接返回
        if (explainLevel == ExplainLevel.ANALYZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
            analyzedPlan = cascadesContext.getRewritePlan();
            if (explainLevel == ExplainLevel.ANALYZED_PLAN) {
                return analyzedPlan;
            }
        }

        // 阶段3: 重写阶段 - 应用启发式重写规则优化逻辑计划
        rewrite(showRewriteProcess(explainLevel, showPlanProcess));
        // 阶段4: 物化视图预重写 - 尝试使用物化视图重写查询
        preMaterializedViewRewrite();
        // 如果只需要重写后的计划，直接返回
        if (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN) {
            rewrittenPlan = cascadesContext.getRewritePlan();
            if (explainLevel == ExplainLevel.REWRITTEN_PLAN) {
                return rewrittenPlan;
            }
        }

        // 阶段5: 优化阶段 - 使用基于成本的优化，探索替代物理计划
        optimize(showPlanProcess);
        // 如果启用了 memo 导出，在选择计划前打印 memo，用于调试
        // 如果 chooseNthPlan 失败，可以通过 memo 来调试
        if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
            Memo memo = cascadesContext.getMemo();
            if (memo != null) {
                LOG.info("{}\n{}", ConnectContext.get().getQueryIdentifier(), memo.toString());
            } else {
                LOG.info("{}\nMemo is null", ConnectContext.get().getQueryIdentifier());
            }
        }
        // 阶段6: 计划选择 - 根据会话变量选择第 N 个最优物理计划
        int nth = cascadesContext.getConnectContext().getSessionVariable().getNthOptimizedPlan();
        PhysicalPlan physicalPlan = chooseNthPlan(getRoot(), requireProperties, nth);

        // 阶段7: 后处理 - 应用后优化转换（运行时过滤器生成、分布式计划生成等）
        physicalPlan = postProcess(physicalPlan);
        // 如果启用了 memo 导出，打印最终物理计划树
        if (cascadesContext.getConnectContext().getSessionVariable().dumpNereidsMemo) {
            String tree = physicalPlan.treeString();
            LOG.info("{}\n{}", ConnectContext.get().getQueryIdentifier(), tree);
        }
        // 根据 explainLevel 保存优化后的计划
        if (explainLevel == ExplainLevel.OPTIMIZED_PLAN
                || explainLevel == ExplainLevel.ALL_PLAN
                || explainLevel == ExplainLevel.SHAPE_PLAN) {
            optimizedPlan = physicalPlan;
        }
        // 阶段8: 输出序列化 - 将优化后的物理计划序列化到 dumpfile
        // 如果 dumpfile 没有这部分内容，说明优化失败了
        MinidumpUtils.serializeOutputToDumpFile(physicalPlan);
        // 输出 Nereids 追踪信息
        NereidsTracer.output(statementContext.getConnectContext());
        return physicalPlan;
    }

    protected LogicalPlan preprocess(LogicalPlan logicalPlan) {
        return new PlanPreprocessors(statementContext).process(logicalPlan);
    }

    /**
     * config rf wait time if wait time is the same as default value
     * 1. local mode, config according to max table row count
     *     a. olap table:
     *       row < 1G: 1 sec
     *       1G <= row < 10G: 5 sec
     *       10G < row: 20 sec
     *     b. external table:
     *       row < 1G: 5 sec
     *       1G <= row < 10G: 10 sec
     *       10G < row: 50 sec
     * 2. cloud mode, config it as query time out
     */
    private void configRuntimeFilterWaitTime() {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null
                && ConnectContext.get().getSessionVariable().getRuntimeFilterWaitTimeMs()
                == VariableMgr.getDefaultSessionVariable().getRuntimeFilterWaitTimeMs()) {
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
            if (Config.isCloudMode()) {
                sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS,
                        String.valueOf(Math.max(VariableMgr.getDefaultSessionVariable().getRuntimeFilterWaitTimeMs(),
                                1000 * sessionVariable.getQueryTimeoutS())));
            } else {
                List<LogicalCatalogRelation> scans = cascadesContext.getRewritePlan()
                        .collectToList(LogicalCatalogRelation.class::isInstance);
                double maxRow = StatsCalculator.getMaxTableRowCount(scans, cascadesContext);
                boolean hasExternalTable = scans.stream().anyMatch(scan -> !(scan instanceof LogicalOlapScan));
                if (hasExternalTable) {
                    if (maxRow < 1_000_000_000L) {
                        sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "5000");
                    } else if (maxRow < 10_000_000_000L) {
                        sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "20000");
                    } else {
                        sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "50000");
                    }
                } else {
                    if (maxRow < 1_000_000_000L) {
                        sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "1000");
                    } else if (maxRow < 10_000_000_000L) {
                        sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "5000");
                    } else {
                        sessionVariable.setVarOnce(SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS, "20000");
                    }
                }
            }
        }
    }

    private void initCascadesContext(LogicalPlan plan, PhysicalProperties requireProperties) {
        cascadesContext = CascadesContext.initContext(statementContext, plan, requireProperties);
    }

    /**
     * 收集查询涉及的所有表并获取表锁。
     *
     * 该方法是查询规划过程中的关键步骤，在分析阶段执行，主要功能包括：
     *
     * ==================== 执行流程 ====================
     *
     * 步骤1：收集表信息并注册 Planner Hooks
     * - 调用 cascadesContext.newTableCollector(true).collect()，执行以下规则：
     *   - AddInitMaterializationHook：注册物化视图相关的 Planner Hook
     *     （InitMaterializationContextHook 或 InitConsistentMaterializationContextHook）
     *   - CollectRelation：收集查询涉及的所有表，包括：
     *     * 基表（tables）
     *     * 一级表（oneLevelTables，直接出现在查询中的表）
     *     * MTMV 相关表（mtmvRelatedTables，物化视图及其依赖的表）
     *     * 插入目标表（insertTargetTables）
     *     * 视图信息（viewInfos，缓存视图定义和 SQL 模式）
     *     * 物化视图候选（candidateMVs、candidateMTMVs）
     *
     * 步骤2：获取表读锁
     * - 调用 statementContext.lock()，按表 ID 顺序对所有表获取读锁
     * - 按顺序锁定可以避免死锁（多个查询同时访问多个表时）
     * - 锁资源保存在 plannerResources 栈中，查询结束后统一释放
     *
     * 步骤3：初始化 CTE 上下文
     * - 设置 cascadesContext 的 CTE 上下文，用于后续处理 CTE（Common Table Expression）
     *
     * 步骤4：记录性能追踪信息
     * - 记录表锁获取完成的时间点，用于性能分析和调优
     *
     * ==================== 使用场景 ====================
     *
     * 示例：查询涉及多个表和物化视图
     * ```sql
     * SELECT * FROM orders o
     * JOIN users u ON o.user_id = u.id
     * WHERE o.create_time > '2024-01-01'
     * ```
     *
     * 执行过程：
     * 1. CollectRelation 收集 orders、users 表，以及可能匹配的物化视图
     * 2. AddInitMaterializationHook 注册 InitMaterializationContextHook
     * 3. lock() 按表 ID 顺序获取 orders、users 的读锁
     * 4. 设置 CTE 上下文（如果查询包含 CTE）
     *
     * ==================== 注意事项 ====================
     *
     * 1. 表锁必须在分析阶段获取，确保后续优化过程中表结构不会发生变化
     * 2. 按表 ID 顺序锁定可以避免死锁，这是 Doris 的锁管理策略
     * 3. 锁资源会在查询结束后通过 releasePlannerResources() 统一释放
     * 4. Planner Hook 的注册时机很重要，必须在表收集之后、重写之前完成
     *
     * @param showPlanProcess 是否显示计划处理过程（用于调试和性能分析）
     */
    protected void collectAndLockTable(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start collect and lock table");
        }
        // 步骤1：收集表信息并注册 Planner Hooks
        // 执行 AddInitMaterializationHook 和 CollectRelation 规则
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newTableCollector(true).collect());
        
        // 步骤2：按表 ID 顺序获取表读锁，避免死锁
        statementContext.lock();
        
        // 步骤3：初始化 CTE 上下文
        cascadesContext.setCteContext(new CTEContext());
        
        // 记录表锁获取完成的时间点
        NereidsTracer.logImportantTime("EndCollectAndLockTables");
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("End collect and lock table");
        }
        
        // 步骤4：记录性能追踪信息
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsLockTableFinishTime(TimeUtils.getStartTimeMs());
        }
    }

    protected void analyze(boolean showPlanProcess) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start analyze plan");
        }
        keepOrShowPlanProcess(showPlanProcess, () -> cascadesContext.newAnalyzer().analyze());
        NereidsTracer.logImportantTime("EndAnalyzePlan");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End analyze plan");
        }

        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsAnalysisTime(TimeUtils.getStartTimeMs());
        }
    }

    /**
     * 基于一系列启发式规则对逻辑计划进行重写优化
     * 
     * 该方法是Nereids优化器中逻辑计划重写的核心入口，主要功能包括：
     * 1. 执行启发式规则重写：通过Rewriter执行各种重写规则，如谓词下推、列裁剪、常量折叠等
     * 2. 性能统计：记录重写阶段的时间消耗，用于性能分析和优化
     * 3. 物化视图预检查：检查是否需要物化视图重写，为后续MV优化做准备
     * 4. 钩子函数执行：执行重写后的回调函数，支持插件化扩展
     * 
     * @param showPlanProcess 是否显示计划处理过程（用于调试和性能分析）
     */
    protected void rewrite(boolean showPlanProcess) {
        // 调试日志：记录重写开始
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start rewrite plan");
        }
        
        // 核心重写逻辑：获取完整树重写器并执行所有重写规则
        keepOrShowPlanProcess(showPlanProcess, () -> {
            // 创建并执行完整树重写器，包含所有启发式重写规则
            Rewriter.getWholeTreeRewriter(cascadesContext).execute();
        });
        
        // 性能追踪：记录重写阶段结束时间点
        NereidsTracer.logImportantTime("EndRewritePlan");
        
        // 调试日志：记录重写结束z
        if (LOG.isDebugEnabled()) {
            LOG.debug("End rewrite plan");
        }
        
        // 性能统计：将重写时间记录到执行器摘要中，用于性能分析
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsRewriteTime(TimeUtils.getStartTimeMs());
        }
        
        // 物化视图预检查：判断当前查询是否需要物化视图重写优化
        statementContext.setNeedPreMvRewrite(PreMaterializedViewRewriter.needPreRewrite(cascadesContext));
        
        // 钩子函数执行：执行所有注册的重写后回调函数，支持插件化扩展
        cascadesContext.getStatementContext().getPlannerHooks().forEach(hook -> hook.afterRewrite(cascadesContext));
    }

    /**
     * 物化视图预重写
     * 
     * 该方法在 RBO 阶段之后执行，用于对临时计划进行物化视图预重写优化。
     * 预重写的目的是：在 CBO 阶段之前，先通过优化器找到使用物化视图的最佳计划，
     * 然后规范化输出表达式，生成包含物化视图的重写计划，供后续 CBO 优化使用。
     * 
     * 执行流程：
     * 1. 检查是否需要预重写（isNeedPreMvRewrite）
     * 2. 遍历临时计划列表（tmpPlansForMvRewrite）
     * 3. 对每个临时计划执行两步重写：
     *    a) 物化视图重写：调用 PreMaterializedViewRewriter.rewrite 进行物化视图重写
     *    b) 规则优化：应用 RBO 规则（不包含基于代价的作业）进一步优化计划
     * 4. 规范化输出表达式：确保重写后的计划与原始计划的输出表达式保持一致
     * 5. 收集成功重写的计划，并标记为已预重写
     * 
     * 注意事项：
     * - 使用独立的超时设置（materializedViewRewriteDurationThresholdMs），避免影响主查询超时
     * - 由于 tmpPlansForMvRewrite 通常只有一个计划，超时是累积的，这是可以接受的
     * - 如果预重写成功，后续 CBO 阶段不会再进行物化视图重写（setPreMvRewritten(true)）
     */
    protected void preMaterializedViewRewrite() {
        // 前置检查：如果不需要预重写，直接返回
        if (!cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start pre rewrite plan by mv");
        }
        
        // 获取临时计划列表（在 RBO 阶段保存的，用于物化视图重写的临时计划）
        List<Plan> tmpPlansForMvRewrite = cascadesContext.getStatementContext().getTmpPlanForMvRewrite();
        // 获取原始计划（RBO 阶段重写后的计划）
        Plan originalPlan = cascadesContext.getRewritePlan();
        // 用于收集成功重写且包含物化视图的计划
        List<Plan> plansWhichContainMv = new ArrayList<>();
        
        // 遍历每个临时计划，进行物化视图预重写
        // 注意：由于 tmpPlansForMvRewrite 通常只有一个计划，超时是累积的，这是可以接受的
        for (Plan planForRewrite : tmpPlansForMvRewrite) {
            SessionVariable sessionVariable = cascadesContext.getConnectContext()
                    .getSessionVariable();
            // 保存原始超时设置，用于 finally 块中恢复
            int timeoutSecond = sessionVariable.nereidsTimeoutSecond;
            boolean enableTimeout = sessionVariable.enableNereidsTimeout;
            try {
                // 设置物化视图重写的超时时间（独立于主查询超时）
                // 将毫秒转换为向上取整的秒数
                sessionVariable.nereidsTimeoutSecond = PreMaterializedViewRewriter.convertMillisToCeilingSeconds(
                                sessionVariable.materializedViewRewriteDurationThresholdMs);
                sessionVariable.enableNereidsTimeout = true;
                
                // 步骤1：物化视图重写
                // MaterializedViewUtils.rewriteByRules 会在独立的 CascadesContext 中执行重写逻辑
                // 参数说明：
                //   - cascadesContext: 当前的级联上下文
                //   - PreMaterializedViewRewriter::rewrite: 重写函数，执行优化、选择最佳物理计划、提取物化视图和表
                //   - planForRewrite: 输入计划（临时计划）
                //   - planForRewrite: 原始计划（用于对比）
                //   - true: 表示这是物化视图重写阶段
                // 返回：重写后的逻辑计划（如果成功找到使用物化视图的物理计划）
                Plan rewrittenPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                        PreMaterializedViewRewriter::rewrite, planForRewrite, planForRewrite, true);
                
                // 步骤2：规则优化（不包含基于代价的作业）
                // 对重写后的计划应用 RBO 规则，进一步优化计划结构
                // 这里使用 getWholeTreeRewriterWithoutCostBasedJobs 是因为：
                //   - 预重写阶段不应该执行基于代价的优化（CBO），避免重复优化
                //   - 只需要应用 RBO 规则来优化计划结构（如谓词下推、列裁剪等）
                // 参数说明：
                //   - rewrittenPlan: 物化视图重写后的计划
                //   - planForRewrite: 原始计划（用于对比）
                //   - false: 表示这不是物化视图重写阶段，而是规则优化阶段
                Plan ruleOptimizedPlan = MaterializedViewUtils.rewriteByRules(cascadesContext,
                        childOptContext -> {
                            // 获取不包含基于代价作业的完整树重写器，并执行重写
                            Rewriter.getWholeTreeRewriterWithoutCostBasedJobs(childOptContext).execute();
                            // 返回重写后的计划
                            return childOptContext.getRewritePlan();
                        }, rewrittenPlan, planForRewrite, false);
                
                // 如果规则优化失败（返回 null），跳过当前计划，继续处理下一个临时计划
                if (ruleOptimizedPlan == null) {
                    continue;
                }
                
                // 步骤3：规范化输出表达式
                // 在 RBO 阶段之后，计划可能发生了很大变化（如使用了物化视图），
                // 需要与原始计划进行规范化，确保输出表达式的一致性
                // normalizeSinkExpressions 的作用：
                //   - 对齐输出表达式的 exprId、nullable、dataType 等属性
                //   - 确保重写后的计划输出与原始计划在语义上等价
                //   - 这样后续处理（如 CBO、执行计划生成）能正确识别输出列
                // 参数说明：
                //   - ruleOptimizedPlan: 规则优化后的计划
                //   - originalPlan: 原始计划（RBO 阶段重写后的计划）
                Plan normalizedPlan = MaterializedViewUtils.normalizeSinkExpressions(
                        ruleOptimizedPlan, originalPlan);
                
                // 如果规范化成功（返回非 null），添加到结果列表
                // 这些计划将在后续的 CBO 阶段被使用，作为候选计划之一
                if (normalizedPlan != null) {
                    plansWhichContainMv.add(normalizedPlan);
                }
            } catch (Exception e) {
                // 异常处理：记录错误日志，但不中断整个流程
                // 如果某个临时计划的重写失败，继续处理其他临时计划
                // 这样可以提高容错性，即使部分计划重写失败，其他计划仍可以成功重写
                LOG.error("pre mv rewrite in rbo rewrite fail, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier(), e);

            } finally {
                // 恢复原始超时设置：无论成功还是失败，都要恢复原始的超时配置
                // 这样可以确保主查询的超时设置不受物化视图预重写的影响
                sessionVariable.nereidsTimeoutSecond = timeoutSecond;
                sessionVariable.enableNereidsTimeout = enableTimeout;
            }
        }
        
        // 步骤4：清理之前临时优化的重写计划
        // 这些计划是在之前的优化阶段临时保存的，现在应该被当前预重写的结果替换
        // 后续的完整优化（CBO）会基于这些新的计划进行
        statementContext.getRewrittenPlansByMv().clear();
        
        // 步骤5：标记为已预重写
        // 设置 setPreMvRewritten(true) 的作用：
        //   - 告诉后续的 CBO 阶段：物化视图预重写已经完成
        //   - CBO 阶段不会再进行物化视图重写，避免重复优化
        //   - 确保预重写的计划被正确使用，而不是被 CBO 阶段的新计划覆盖
        this.cascadesContext.getStatementContext().setPreMvRewritten(true);
        
        // 步骤6：检查是否有成功重写的计划
        // 如果没有任何计划成功重写（plansWhichContainMv 为空），直接返回
        // 这意味着预重写失败，后续 CBO 阶段会使用原始计划进行优化
        if (plansWhichContainMv.isEmpty()) {
            return;
        }
        
        // 步骤7：将成功重写的计划添加到 StatementContext 中
        // 这些计划会在后续的 CBO 阶段被使用，作为候选计划之一
        // CBO 阶段会从这些计划中选择代价最低的计划作为最终执行计划
        plansWhichContainMv.forEach(statementContext::addRewrittenPlanByMv);
        
        // 步骤8：记录性能追踪和日志
        // 记录预重写结束的时间点，用于性能分析和调试
        NereidsTracer.logImportantTime("EndPreRewritePlanByMv");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End pre rewrite plan by mv");
        }
        
        // 性能统计：将预重写完成时间记录到执行器摘要中，用于性能分析
        // 这可以用于查询性能分析和优化器调优
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsPreRewriteByMvFinishTime(TimeUtils.getStartTimeMs());
        }
    }

    /**
     * 执行 CBO（基于代价的优化）阶段
     * 
     * 这是优化器的核心方法，在 RBO（基于规则的优化）阶段之后执行。
     * 它会应用 CBO 规则，生成多个物理计划候选，计算代价，并选择最优计划。
     * 
     * 执行流程：
     * 1. 检查统计信息有效性，决定是否禁用 Join 重排序
     * 2. 配置运行时过滤器等待时间
     * 3. 执行优化器（应用 CBO 规则，生成物理计划，计算代价）
     * 4. 记录性能追踪和日志
     * 
     * 依赖规则：
     * - AddProjectForJoin：在 Join 之前添加 Project（如果需要）
     * 
     * @param showPlanProcess 是否显示计划变化过程（用于 EXPLAIN 展示）
     */
    protected void optimize(boolean showPlanProcess) {
        // 步骤1：检查统计信息有效性，决定是否禁用 Join 重排序
        // 
        // Join 重排序需要准确的统计信息（如表行数）来计算代价
        // 如果统计信息无效，Join 重排序可能会产生更差的计划
        // 
        // 禁用 Join 重排序的条件：
        // - 启用了内部模式数据库（enableInternalSchemaDb）
        // - 不是单元测试（runningUnitTest = false）
        // - 用户没有设置 LEADING hint（isLeadingDisableJoinReorder = false）
        // 
        // 不禁用的情况：
        // 1. 用户设置了 LEADING hint（强制指定 Join 顺序）
        // 2. 单元测试（在单元测试中，enableInternalSchemaDb 为 false 或 runningUnitTest 为 true）
        if (FeConstants.enableInternalSchemaDb && !FeConstants.runningUnitTest
                && !cascadesContext.isLeadingDisableJoinReorder()) {
            // 收集所有表扫描节点
            List<CatalogRelation> scans = cascadesContext.getRewritePlan()
                    .collectToList(CatalogRelation.class::isInstance);
            
            // 检查统计信息是否有效，如果无效则禁用 Join 重排序
            // 返回禁用原因（如果有）
            Optional<String> disableJoinReorderReason = StatsCalculator
                    .disableJoinReorderIfStatsInvalid(scans, cascadesContext);
            
            // 如果统计信息无效，记录禁用原因（用于日志和调试）
            disableJoinReorderReason.ifPresent(statementContext::setDisableJoinReorderReason);
        }
        
        // 步骤2：配置运行时过滤器（Runtime Filter）等待时间
        // 运行时过滤器是一种优化技术，用于在 Join 时提前过滤数据
        // 等待时间根据表的大小和查询超时时间动态调整
        configRuntimeFilterWaitTime();
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Start optimize plan");
        }
        
        // 步骤3：执行优化器（CBO 阶段）
        // keepOrShowPlanProcess 用于控制是否记录计划变化过程
        // - 如果 showPlanProcess 为 true，记录计划变化过程（用于 EXPLAIN 展示）
        // - 如果 showPlanProcess 为 false，只执行优化，不记录过程
        keepOrShowPlanProcess(showPlanProcess, () -> {
            // 创建并执行优化器
            // Optimizer 会应用 CBO 规则，生成物理计划，计算代价，选择最优计划
            new Optimizer(cascadesContext).execute();
        });
        
        // 步骤4：记录性能追踪和日志
        // 记录优化结束的时间点，用于性能分析和调试
        NereidsTracer.logImportantTime("EndOptimizePlan");
        if (LOG.isDebugEnabled()) {
            LOG.debug("End optimize plan");
        }
        
        // 性能统计：将优化时间记录到执行器摘要中，用于性能分析
        // 这可以用于查询性能分析和优化器调优
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsOptimizeTime(TimeUtils.getStartTimeMs());
        }
    }

    /**
     * Collect plan info for hbo usage.
     * @param queryId queryId
     * @param root physical plan
     * @param context PlanTranslatorContext
     */
    private void collectHboPlanInfo(String queryId, PhysicalPlan root, PlanTranslatorContext context) {
        for (Object child : root.children()) {
            collectHboPlanInfo(queryId, (PhysicalPlan) child, context);
        }
        if (root instanceof AbstractPlan) {
            int nodeId = ((AbstractPlan) root).getId();
            PlanNodeId planId = context.getNereidsIdToPlanNodeIdMap().get(nodeId);
            if (planId != null) {
                Map<Integer, PhysicalPlan> idToPlanMap = Env.getCurrentEnv().getHboPlanStatisticsManager()
                        .getHboPlanInfoProvider().getIdToPlanMap(queryId);
                if (idToPlanMap.isEmpty()) {
                    Env.getCurrentEnv().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().putIdToPlanMap(queryId, idToPlanMap);
                }
                idToPlanMap.put(planId.asInt(), root);
                Map<PhysicalPlan, Integer> planToIdMap = Env.getCurrentEnv().getHboPlanStatisticsManager()
                                .getHboPlanInfoProvider().getPlanToIdMap(queryId);
                if (planToIdMap.isEmpty()) {
                    Env.getCurrentEnv().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().putPlanToIdMap(queryId, planToIdMap);
                }
                planToIdMap.put(root, planId.asInt());
            }
        }
    }

    protected void splitFragments(PhysicalPlan resultPlan) {
        if (resultPlan instanceof PhysicalSqlCache) {
            return;
        }

        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator(planTranslatorContext,
                statementContext.getConnectContext().getStatsErrorEstimator());
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        if (sessionVariable.isEnableNereidsTrace()) {
            CounterEvent.clearCounter();
        }
        if (sessionVariable.isPlayNereidsDump()) {
            return;
        }
        PlanFragment root = physicalPlanTranslator.translatePlan(physicalPlan);
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsTranslateTime(TimeUtils.getStartTimeMs());
        }
        String queryId = DebugUtil.printId(cascadesContext.getConnectContext().queryId());
        if (StatisticsUtil.isEnableHboInfoCollection()) {
            collectHboPlanInfo(queryId, physicalPlan, planTranslatorContext);
        }

        scanNodeList.addAll(planTranslatorContext.getScanNodes());
        physicalRelations.addAll(planTranslatorContext.getPhysicalRelations());
        descTable = planTranslatorContext.getDescTable();
        fragments = new ArrayList<>(planTranslatorContext.getPlanFragments());

        boolean enableQueryCache = sessionVariable.getEnableQueryCache();
        for (int seq = 0; seq < fragments.size(); seq++) {
            PlanFragment fragment = fragments.get(seq);
            fragment.setFragmentSequenceNum(seq);
            if (enableQueryCache) {
                try {
                    QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
                    Optional<TQueryCacheParam> queryCacheParam =
                            normalizer.normalize(cascadesContext.getConnectContext());
                    if (queryCacheParam.isPresent()) {
                        fragment.queryCacheParam = queryCacheParam.get();
                        // after commons-codec 1.14 (include), Hex.encodeHexString will change ByteBuffer.pos,
                        // so we should copy a new byte buffer to print it
                        ByteBuffer digestCopy = fragment.queryCacheParam.digest.duplicate();
                        LOG.info("Use query cache for fragment {}, node id: {}, digest: {}, queryId: {}",
                                seq,
                                fragment.queryCacheParam.node_id,
                                Hex.encodeHexString(digestCopy), queryId);
                    }
                } catch (Throwable t) {
                    // do nothing
                }
            }
        }
        // set output exprs
        logicalPlanAdapter.setResultExprs(root.getOutputExprs());
        ArrayList<String> columnLabels = Lists.newArrayListWithExpectedSize(physicalPlan.getOutput().size());
        List<FieldInfo> fieldInfos = Lists.newArrayListWithExpectedSize(physicalPlan.getOutput().size());
        for (NamedExpression output : physicalPlan.getOutput()) {
            Optional<Column> column = Optional.empty();
            Optional<TableIf> table = Optional.empty();
            if (output instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) output;
                column = slotReference.getOneLevelColumn();
                table = slotReference.getOneLevelTable();
            }
            columnLabels.add(output.getName());
            FieldInfo fieldInfo = new FieldInfo(
                    table.isPresent() ? (table.get().getDatabase() != null
                            ? table.get().getDatabase().getFullName() : "") : "",
                    !output.getQualifier().isEmpty() ? output.getQualifier().get(output.getQualifier().size() - 1)
                            : (table.isPresent() ? Util.getTempTableDisplayName(table.get().getName()) : ""),
                    table.isPresent() ? Util.getTempTableDisplayName(table.get().getName()) : "",
                    output.getName(),
                    column.isPresent() ? column.get().getName() : ""
            );
            fieldInfos.add(fieldInfo);
        }
        logicalPlanAdapter.setColLabels(columnLabels);
        logicalPlanAdapter.setFieldInfos(fieldInfos);
        logicalPlanAdapter.setViewDdlSqls(statementContext.getViewDdlSqls());
        if (statementContext.getSqlCacheContext().isPresent()) {
            SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext().get();
            sqlCacheContext.setColLabels(columnLabels);
            sqlCacheContext.setFieldInfos(fieldInfos);
            sqlCacheContext.setResultExprs(root.getOutputExprs());
            sqlCacheContext.setPhysicalPlan(resultPlan.treeString());
        }

        cascadesContext.releaseMemo();

        // update scan nodes visible version at the end of plan phase for cloud mode.
        try {
            ScanNode.setVisibleVersionForOlapScanNodes(getScanNodes());
        } catch (UserException ue) {
            throw new NereidsException(ue.getMessage(), ue);
        }
    }

    protected void distribute(PhysicalPlan physicalPlan, ExplainLevel explainLevel) {
        boolean canUseNereidsDistributePlanner = SessionVariable.canUseNereidsDistributePlanner()
                || (physicalPlan instanceof PhysicalDictionarySink); // dic sink only supported in new Coordinator
        if ((!canUseNereidsDistributePlanner && explainLevel.isPlanLevel)) {
            return;
        } else if ((canUseNereidsDistributePlanner && explainLevel.isPlanLevel
                && (explainLevel != ExplainLevel.ALL_PLAN && explainLevel != ExplainLevel.DISTRIBUTED_PLAN))) {
            return;
        }

        splitFragments(physicalPlan);
        doDistribute(canUseNereidsDistributePlanner, explainLevel);
    }

    protected void doDistribute(boolean canUseNereidsDistributePlanner, ExplainLevel explainLevel) {
        if (!canUseNereidsDistributePlanner) {
            return;
        }
        switch (explainLevel) {
            case NONE:
            case ALL_PLAN:
            case DISTRIBUTED_PLAN:
                break;
            default: {
                return;
            }
        }

        boolean notNeedBackend = false;
        // if the query can compute without backend, we can skip check cluster privileges
        if (Config.isCloudMode()
                && cascadesContext.getConnectContext().supportHandleByFe()
                && physicalPlan instanceof ComputeResultSet) {
            Optional<ResultSet> resultSet = ((ComputeResultSet) physicalPlan).computeResultInFe(
                    cascadesContext, Optional.empty(), physicalPlan.getOutput());
            if (resultSet.isPresent()) {
                notNeedBackend = true;
            }
        }

        distributedPlans = new DistributePlanner(statementContext, fragments, notNeedBackend, false).plan();
        if (statementContext.getConnectContext().getExecutor() != null) {
            statementContext.getConnectContext().getExecutor().getSummaryProfile()
                    .setNereidsDistributeTime(TimeUtils.getStartTimeMs());
        }
    }

    protected PhysicalPlan postProcess(PhysicalPlan physicalPlan) {
        return new PlanPostProcessors(cascadesContext).process(physicalPlan);
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return scanNodeList;
    }

    public List<PhysicalRelation> getPhysicalRelations() {
        return physicalRelations;
    }

    public Group getRoot() {
        return cascadesContext.getMemo().getRoot();
    }

    protected PhysicalPlan chooseNthPlan(Group rootGroup, PhysicalProperties physicalProperties, int nthPlan) {
        if (nthPlan <= 1) {
            cost = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                    () -> new AnalysisException("lowestCostPlans with physicalProperties("
                            + physicalProperties + ") doesn't exist in root group")).first.getValue();
            return chooseBestPlan(rootGroup, physicalProperties, cascadesContext);
        }
        Memo memo = cascadesContext.getMemo();

        Pair<Long, Double> idCost = memo.rank(nthPlan);
        cost = idCost.second;
        return memo.unrank(idCost.first);
    }

    /**
     * 从 Memo 中选择代价最低的物理计划
     * 
     * 这是 CBO（基于代价的优化）阶段的核心方法，用于从 Memo 中选择最优的物理执行计划。
     * 该方法递归地从根 Group 开始，根据物理属性要求选择代价最低的计划，并构建完整的物理计划树。
     * 
     * 执行流程：
     * 1. 从 Group 中获取满足物理属性要求且代价最低的 GroupExpression
     * 2. 检查是否是 Enforcer（强制器），如果是则记录 Enforcer 信息
     * 3. 获取子节点所需的物理属性列表
     * 4. 递归选择所有子节点的最优计划
     * 5. 使用选择的子计划构建完整的物理计划树
     * 6. 添加物理属性和统计信息
     * 
     * 物理属性传播：
     * - 父节点对子节点有物理属性要求（如数据分布、排序等）
     * - 子节点需要满足这些要求，如果不满足则使用 Enforcer
     * - 通过 inputPropertiesList 将父节点的要求传递给子节点
     * 
     * 示例：
     * <pre>
     * 假设根 Group 需要 GATHER 分布：
     * 
     * Group 0 (根)
     *   - 选择: PhysicalHashJoin (代价: 100, 输出: GATHER)
     *     - 子节点1需要: HASH(id)
     *     - 子节点2需要: HASH(id)
     * 
     * Group 1 (表A)
     *   - 选择: PhysicalDistribute(HASH(id)) (Enforcer, 代价: 50)
     *     - 子节点需要: ANY
     * 
     * Group 2 (表B)
     *   - 选择: PhysicalDistribute(HASH(id)) (Enforcer, 代价: 50)
     *     - 子节点需要: ANY
     * </pre>
     * 
     * @param rootGroup 根 Group，从该 Group 开始选择最优计划
     * @param physicalProperties 所需的物理属性（如数据分布、排序等）
     * @param cascadesContext 级联上下文，包含优化器状态和配置
     * @return 代价最低的物理计划树
     * @throws AnalysisException 如果无法找到满足要求的计划或发生其他错误
     */
    public static PhysicalPlan chooseBestPlan(Group rootGroup, PhysicalProperties physicalProperties,
            CascadesContext cascadesContext)
            throws AnalysisException {
        try {
            // 步骤1：从 Group 中获取满足物理属性要求且代价最低的 GroupExpression
            // getLowestCostPlan 返回 Pair<Cost, GroupExpression>，包含最低代价和对应的 GroupExpression
            // 如果不存在满足要求的计划，抛出异常
            GroupExpression groupExpression = rootGroup.getLowestCostPlan(physicalProperties).orElseThrow(
                    () -> new AnalysisException("lowestCostPlans with physicalProperties("
                            + physicalProperties + ") doesn't exist in root group")).second;
            
            // 步骤2：检查选择的 GroupExpression 是否是 Enforcer（强制器）
            // Enforcer 是用于强制物理属性的特殊节点（如 PhysicalDistribute、PhysicalQuickSort）
            if (rootGroup.getEnforcers().containsKey(groupExpression)) {
                // 如果是 Enforcer，记录 Enforcer 的 ID 和物理属性
                // 这些信息用于后续的计划生成和调试
                rootGroup.addChosenEnforcerId(groupExpression.getId().asInt());
                rootGroup.addChosenEnforcerProperties(physicalProperties);
            } else {
                // 如果不是 Enforcer，记录选择的物理属性和 GroupExpression ID
                // 这些信息用于后续的计划生成和调试
                rootGroup.setChosenProperties(physicalProperties);
                rootGroup.setChosenGroupExpressionId(groupExpression.getId().asInt());
            }
            
            // 步骤3：获取子节点所需的物理属性列表
            // 父节点对每个子节点都有物理属性要求（如数据分布、排序等）
            // 这些要求会传递给子节点，子节点需要满足这些要求或使用 Enforcer
            List<PhysicalProperties> inputPropertiesList = groupExpression.getInputPropertiesList(physicalProperties);
            
            // 步骤4：递归选择所有子节点的最优计划
            // 为每个子节点递归调用 chooseBestPlan，传入子节点所需的物理属性
            List<Plan> planChildren = Lists.newArrayList();
            for (int i = 0; i < groupExpression.arity(); i++) {
                // 递归选择子节点的最优计划
                // 子节点需要满足 inputPropertiesList.get(i) 的物理属性要求
                planChildren.add(chooseBestPlan(groupExpression.child(i), inputPropertiesList.get(i), cascadesContext));
            }

            // 步骤5：使用选择的子计划构建完整的物理计划树
            // 将 GroupExpression 的计划节点与选择的子计划组合
            Plan plan = groupExpression.getPlan().withChildren(planChildren);
            
            // 验证：结果必须是物理计划
            if (!(plan instanceof PhysicalPlan)) {
                // TODO need add some log
                throw new AnalysisException("Result plan must be PhysicalPlan");
            }
            
            // 步骤6：将 GroupExpression 信息附加到计划中
            // 这样在打印计划树时（plan.treeString()）可以显示 Group ID，便于调试
            plan = plan.withGroupExpression(Optional.of(groupExpression));
            
            // 步骤7：添加物理属性和统计信息
            // 物理属性：数据分布、排序等
            // 统计信息：行数、列统计等，用于执行计划的生成和优化
            PhysicalPlan physicalPlan = ((PhysicalPlan) plan).withPhysicalPropertiesAndStats(
                    physicalProperties, groupExpression.getOwnerGroup().getStatistics());
            
            return physicalPlan;
        } catch (Exception e) {
            // 异常处理：如果是已知的 AnalysisException，直接抛出
            if (e instanceof AnalysisException && e.getMessage().contains("Failed to choose best plan")) {
                throw e;
            }
            // 记录警告日志，包含 Memo 结构信息，便于调试
            LOG.warn("Failed to choose best plan, memo structure:{}", cascadesContext.getMemo(), e);
            throw new AnalysisException("Failed to choose best plan: " + e.getMessage(), e);
        }
    }

    private long getGarbageCollectionTime() {
        if (!ConnectContext.get().getSessionVariable().enableProfile()) {
            return 0;
        }
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long initialGCTime = 0;
        for (GarbageCollectorMXBean gcBean : gcMxBeans) {
            initialGCTime += gcBean.getCollectionTime();
        }
        return initialGCTime;
    }

    /**
     * getting hints explain string, which specified by enumerate and show in lists
     * @param hints hint map recorded in statement context
     * @return explain string shows using of hint
     */
    public String getHintExplainString(List<Hint> hints) {
        String used = "";
        String unUsed = "";
        String syntaxError = "";
        int distributeHintIndex = 0;
        for (Hint hint : hints) {
            String distributeIndex = "";
            if (hint instanceof DistributeHint) {
                distributeHintIndex++;
                if (!hint.getExplainString().equals("")) {
                    distributeIndex = "_" + distributeHintIndex;
                }
            }
            switch (hint.getStatus()) {
                case UNUSED:
                    unUsed = unUsed + " " + hint.getExplainString() + distributeIndex;
                    break;
                case SYNTAX_ERROR:
                    syntaxError = syntaxError + " " + hint.getExplainString() + distributeIndex
                        + " Msg:" + hint.getErrorMessage();
                    break;
                case SUCCESS:
                    used = used + " " + hint.getExplainString() + distributeIndex;
                    break;
                default:
                    break;
            }
        }
        return "\nHint log:" + "\nUsed:" + used + "\nUnUsed:" + unUsed + "\nSyntaxError:" + syntaxError;
    }

    @Override
    public String getExplainString(ExplainOptions explainOptions) {
        if (getConnectContext().getSessionVariable().enableExplainNone) {
            return "";
        }
        ExplainLevel explainLevel = getExplainLevel(explainOptions);
        String plan = "";
        String mvSummary = "";
        if ((this.getPhysicalPlan() != null || this.getOptimizedPlan() != null) && cascadesContext != null) {
            mvSummary = cascadesContext.getMaterializationContexts().isEmpty() ? "" :
                    "\n\n========== MATERIALIZATIONS ==========\n"
                            + MaterializationContext.toSummaryString(cascadesContext,
                            this.getPhysicalPlan() == null ? this.getOptimizedPlan() : this.getPhysicalPlan());
        }
        switch (explainLevel) {
            case PARSED_PLAN:
                plan = parsedPlan.treeString();
                break;
            case ANALYZED_PLAN:
                plan = analyzedPlan.treeString();
                break;
            case REWRITTEN_PLAN:
                plan = rewrittenPlan.treeString();
                break;
            case OPTIMIZED_PLAN:
                plan = "cost = " + cost + "\n" + optimizedPlan.treeString() + mvSummary;
                break;
            case SHAPE_PLAN:
                plan = optimizedPlan.shape("");
                break;
            case MEMO_PLAN:
                Memo memo = cascadesContext.getMemo();
                if (memo == null) {
                    plan = "Memo is null";
                } else {
                    plan = memo.toString()
                        + "\n\n========== OPTIMIZED PLAN ==========\n"
                        + optimizedPlan.treeString()
                        + mvSummary;
                }
                break;
            case DISTRIBUTED_PLAN:
                StringBuilder distributedPlanStringBuilder = new StringBuilder();

                distributedPlanStringBuilder.append("========== DISTRIBUTED PLAN ==========\n");
                if (distributedPlans == null || distributedPlans.isEmpty()) {
                    plan = "Distributed plan not generated, please set enable_nereids_distribute_planner "
                            + "and enable_pipeline_x_engine to true";
                } else {
                    plan += DistributedPlan.toString(Lists.newArrayList(distributedPlans.values())) + "\n\n";
                }
                break;
            case ALL_PLAN:
                plan = "========== PARSED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyParseSqlTime) + " ==========\n"
                        + parsedPlan.treeString() + "\n\n"
                        + "========== LOCK TABLE "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsLockTableTime) + " ==========\n"
                        + "\n\n"
                        + "========== ANALYZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsAnalysisTime) + " ==========\n"
                        + analyzedPlan.treeString() + "\n\n"
                        + "========== REWRITTEN PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsRewriteTime) + " ==========\n"
                        + rewrittenPlan.treeString() + "\n\n"
                        + "========== PRE REWRITTEN BY MV "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsPreRewriteByMvTime) + " ==========\n"
                        + "========== OPTIMIZED PLAN "
                        + getTimeMetricString(SummaryProfile::getPrettyNereidsOptimizeTime) + " ==========\n"
                        + optimizedPlan.treeString() + "\n\n";
                if (cascadesContext != null && cascadesContext.getMemo() != null) {
                    plan += "========== MEMO " + cascadesContext.getMemo().toString() + "\n\n";
                }

                if (distributedPlans != null && !distributedPlans.isEmpty()) {
                    plan += "========== DISTRIBUTED PLAN "
                            + getTimeMetricString(SummaryProfile::getPrettyNereidsDistributeTime) + " ==========\n";
                    plan += DistributedPlan.toString(Lists.newArrayList(distributedPlans.values())) + "\n\n";
                }
                plan += mvSummary;
                break;
            default:
                plan = super.getExplainString(explainOptions);
                plan += mvSummary;
                plan += "\n\n\n========== STATISTICS ==========\n";
                if (statementContext != null) {
                    if (statementContext.isHasUnknownColStats()) {
                        plan += "planned with unknown column statistics\n";
                    }
                }
        }
        if (ConnectContext.get() != null && cascadesContext != null
                && cascadesContext.getGroupExpressionCount()
                        > ConnectContext.get().getSessionVariable().memoMaxGroupExpressionSize) {
            plan += "\n\n\n group expression count exceeds memo_max_group_expression_size("
                    + ConnectContext.get().getSessionVariable().memoMaxGroupExpressionSize + ")\n";
        }
        if (statementContext != null) {
            if (!statementContext.getHints().isEmpty()) {
                String hint = getHintExplainString(statementContext.getHints());
                return plan + hint;
            }
        }
        return plan;
    }

    @Override
    public DescriptorTable getDescTable() {
        return descTable;
    }

    @Override
    public void appendTupleInfo(StringBuilder str) {
        str.append(descTable.getExplainString());
    }

    @Override
    public List<RuntimeFilter> getRuntimeFilters() {
        ArrayList<RuntimeFilter> runtimeFilters = new ArrayList<>();
        runtimeFilters.addAll(cascadesContext.getRuntimeFilterContext().getLegacyFilters());
        runtimeFilters.addAll(cascadesContext.getRuntimeFilterV2Context().getLegacyFilters());
        return runtimeFilters;
    }

    @Override
    public Optional<ResultSet> handleQueryInFe(StatementBase parsedStmt) {
        if (!(parsedStmt instanceof LogicalPlanAdapter)) {
            return Optional.empty();
        }

        setFormatOptions();
        if (physicalPlan instanceof ComputeResultSet) {
            Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
            Optional<ResultSet> resultSet = ((ComputeResultSet) physicalPlan)
                    .computeResultInFe(cascadesContext, sqlCacheContext, physicalPlan.getOutput());
            if (resultSet.isPresent()) {
                return resultSet;
            }
        }

        return Optional.empty();
    }

    private void setFormatOptions() {
        ConnectContext ctx = statementContext.getConnectContext();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        switch (sessionVariable.serdeDialect) {
            case "presto":
            case "trino":
                statementContext.setFormatOptions(FormatOptions.getForPresto());
                break;
            case "hive":
                statementContext.setFormatOptions(FormatOptions.getForHive());
                break;
            case "doris":
                statementContext.setFormatOptions(FormatOptions.getDefault());
                break;
            default:
                throw new AnalysisException("Unsupported serde dialect: " + sessionVariable.serdeDialect);
        }
    }

    @VisibleForTesting
    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public ConnectContext getConnectContext() {
        return cascadesContext == null ? ConnectContext.get() : cascadesContext.getConnectContext();
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public static PhysicalProperties buildInitRequireProperties() {
        return PhysicalProperties.GATHER;
    }

    protected ExplainLevel getExplainLevel(ExplainOptions explainOptions) {
        if (explainOptions == null) {
            return ExplainLevel.NONE;
        }
        ExplainLevel explainLevel = explainOptions.getExplainLevel();
        return explainLevel == null ? ExplainLevel.NONE : explainLevel;
    }

    @VisibleForTesting
    public Plan getParsedPlan() {
        return parsedPlan;
    }

    @VisibleForTesting
    public void setParsedPlan(Plan parsedPlan) {
        this.parsedPlan = parsedPlan;
    }

    @VisibleForTesting
    public void setOptimizedPlan(Plan optimizedPlan) {
        this.optimizedPlan = optimizedPlan;
    }

    @VisibleForTesting
    public Plan getAnalyzedPlan() {
        return analyzedPlan;
    }

    @VisibleForTesting
    public Plan getRewrittenPlan() {
        return rewrittenPlan;
    }

    @VisibleForTesting
    public Plan getOptimizedPlan() {
        return optimizedPlan;
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public FragmentIdMapping<DistributedPlan> getDistributedPlans() {
        return distributedPlans;
    }

    public LogicalPlanAdapter getLogicalPlanAdapter() {
        return logicalPlanAdapter;
    }

    private String getTimeMetricString(Function<SummaryProfile, String> profileSupplier) {
        return getProfile(summaryProfile -> {
            String metricString = profileSupplier.apply(summaryProfile);
            return (metricString == null || "N/A".equals(metricString)) ? "" : "(time: " + metricString + ")";
        }, "");
    }

    private <T> T getProfile(Function<SummaryProfile, T> profileSupplier, T defaultMetric) {
        T metric = null;
        if (statementContext.getConnectContext().getExecutor() != null) {
            SummaryProfile summaryProfile = statementContext.getConnectContext().getExecutor().getSummaryProfile();
            if (summaryProfile != null) {
                metric = profileSupplier.apply(summaryProfile);
            }
        }
        return metric == null ? defaultMetric : metric;
    }

    private boolean showAnalyzeProcess(ExplainLevel explainLevel, boolean showPlanProcess) {
        return showPlanProcess
                && (explainLevel == ExplainLevel.ANALYZED_PLAN || explainLevel == ExplainLevel.ALL_PLAN);
    }

    private boolean showRewriteProcess(ExplainLevel explainLevel, boolean showPlanProcess) {
        return showPlanProcess
                && (explainLevel == ExplainLevel.REWRITTEN_PLAN || explainLevel == ExplainLevel.ALL_PLAN);
    }

    private boolean showPlanProcess(ExplainOptions explainOptions) {
        return explainOptions != null && explainOptions.showPlanProcess();
    }

    protected void keepOrShowPlanProcess(boolean showPlanProcess, Runnable task) {
        if (showPlanProcess) {
            cascadesContext.withPlanProcess(showPlanProcess, task);
        } else {
            task.run();
        }
    }

    @Override
    public List<TopnFilter> getTopnFilters() {
        return cascadesContext.getTopnFilterContext().getTopnFilters();
    }
}
