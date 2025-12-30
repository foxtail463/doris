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

package org.apache.doris.mtmv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContextUtil;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

/**
 * The cache for materialized view cache
 */
public class MTMVCache {

    public static final Logger LOG = LogManager.getLogger(MTMVCache.class);

    // The materialized view plan which should be optimized by the same rules to query
    // and will remove top sink and unused sort
    private final Pair<Plan, StructInfo> allRulesRewrittenPlanAndStructInfo;
    // The original rewritten plan of mv def sql
    private final Plan originalFinalPlan;
    private final Statistics statistics;
    private final List<Pair<Plan, StructInfo>> partRulesRewrittenPlanAndStructInfos;

    public MTMVCache(Pair<Plan, StructInfo> allRulesRewrittenPlanAndStructInfo, Plan originalFinalPlan,
            Statistics statistics, List<Pair<Plan, StructInfo>> partRulesRewrittenPlanAndStructInfos) {
        this.allRulesRewrittenPlanAndStructInfo = allRulesRewrittenPlanAndStructInfo;
        this.originalFinalPlan = originalFinalPlan;
        this.statistics = statistics;
        this.partRulesRewrittenPlanAndStructInfos = partRulesRewrittenPlanAndStructInfos;
    }

    public Pair<Plan, StructInfo> getAllRulesRewrittenPlanAndStructInfo() {
        return allRulesRewrittenPlanAndStructInfo;
    }

    public Plan getOriginalFinalPlan() {
        return originalFinalPlan;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public List<Pair<Plan, StructInfo>> getPartRulesRewrittenPlanAndStructInfos() {
        return partRulesRewrittenPlanAndStructInfos;
    }

    /**
     * 根据 MV 定义 SQL 构建 MTMVCache。
     * 
     * 核心流程：
     * 1. 解析 MV 定义 SQL 得到 unbound plan
     * 2. 通过 NereidsPlanner 进行分析、重写、优化（计划会被规范化）
     * 3. 可选地添加 SessionVarGuard 表达式（用于会话变量不匹配时阻止重写）
     * 4. 构建 StructInfo 用于后续 MV 匹配
     * 
     * @param defSql MV 定义 SQL
     * @param createCacheContext 用于创建缓存的上下文
     * @param needCost 是否需要计算代价（用于 MV 选择）
     * @param needLock 是否需要锁表
     * @param currentContext 当前上下文，缓存创建后恢复 ThreadLocal
     * @param addSessionVarGuard 是否添加会话变量 guard 表达式
     */
    public static MTMVCache from(String defSql,
            ConnectContext createCacheContext,
            boolean needCost, boolean needLock,
            ConnectContext currentContext,
            boolean addSessionVarGuard) throws AnalysisException {
        // 创建 MV SQL 的 StatementContext
        StatementContext mvSqlStatementContext = new StatementContext(createCacheContext,
                new OriginStatement(defSql, 0));
        if (!needLock) {
            mvSqlStatementContext.setNeedLockTables(false);
        }
        if (mvSqlStatementContext.getConnectContext().getStatementContext() == null) {
            mvSqlStatementContext.getConnectContext().setStatementContext(mvSqlStatementContext);
        }
        // 强制记录临时计划，用于 Pre-MV rewrite
        createCacheContext.getStatementContext().setForceRecordTmpPlan(true);
        mvSqlStatementContext.setForceRecordTmpPlan(true);
        // 禁用 MV 重写，避免递归重写
        boolean originalRewriteFlag = createCacheContext.getSessionVariable().enableMaterializedViewRewrite;
        createCacheContext.getSessionVariable().enableMaterializedViewRewrite = false;
        // 解析 MV 定义 SQL
        LogicalPlan unboundMvPlan = new NereidsParser().parseSingle(defSql);
        NereidsPlanner planner = new NereidsPlanner(mvSqlStatementContext);
        try {
            // 执行计划：分析 -> 重写（包含表达式规范化）-> 优化
            if (needCost) {
                // MV 重写时需要代价信息用于选择最优 MV
                planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN);
            } else {
                // 不需要代价，提升性能
                planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
            }
            CascadesContext cascadesContext = planner.getCascadesContext();
            Plan rewritePlan = cascadesContext.getRewritePlan();

            // 根据需要添加 SessionVarGuard 表达式
            // 当会话变量不匹配时，guard 表达式会阻止该 MV 被使用
            Optional<SessionVarGuardRewriter> exprRewriter = addSessionVarGuard
                    ? Optional.of(new SessionVarGuardRewriter(
                    ConnectContextUtil.getAffectQueryResultInPlanVariables(createCacheContext),
                    cascadesContext))
                    : Optional.empty();
            Plan addGuardRewritePlan = exprRewriter
                    .map(rewriter -> SessionVarGuardRewriter.rewritePlanTree(rewriter, rewritePlan))
                    .orElse(rewritePlan);
            // 构建最终的 Plan 和 StructInfo
            Pair<Plan, StructInfo> finalPlanStructInfoPair = constructPlanAndStructInfo(
                    addGuardRewritePlan, cascadesContext);
            // 处理临时计划（用于 Pre-MV rewrite 模式）
            List<Pair<Plan, StructInfo>> tmpPlanUsedForRewrite = new ArrayList<>();
            for (Plan plan : cascadesContext.getStatementContext().getTmpPlanForMvRewrite()) {
                Plan addGuardplan = exprRewriter
                        .map(rewriter -> SessionVarGuardRewriter.rewritePlanTree(rewriter, plan))
                        .orElse(plan);
                tmpPlanUsedForRewrite.add(constructPlanAndStructInfo(addGuardplan, cascadesContext));
            }
            return new MTMVCache(finalPlanStructInfoPair, addGuardRewritePlan, needCost
                    ? cascadesContext.getMemo().getRoot().getStatistics() : null, tmpPlanUsedForRewrite);
        } finally {
            // 恢复状态
            createCacheContext.getStatementContext().setForceRecordTmpPlan(false);
            mvSqlStatementContext.setForceRecordTmpPlan(false);
            createCacheContext.getSessionVariable().enableMaterializedViewRewrite = originalRewriteFlag;
            if (currentContext != null) {
                currentContext.setThreadLocalInfo();
            }
        }
    }

    // Eliminate result sink because sink operator is useless in query rewrite by materialized view
    // and the top sort can also be removed
    private static Pair<Plan, StructInfo> constructPlanAndStructInfo(Plan plan, CascadesContext cascadesContext) {
        Plan mvPlan = plan.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink,
                    Object context) {
                return new LogicalProject(logicalResultSink.getOutput(),
                        false, logicalResultSink.children());
            }
        }, null);
        // Optimize by rules to remove top sort
        mvPlan = MaterializedViewUtils.rewriteByRules(cascadesContext, childContext -> {
            Rewriter.getCteChildrenRewriter(childContext, ImmutableList.of(
                    Rewriter.custom(RuleType.ELIMINATE_SORT, EliminateSort::new),
                    Rewriter.bottomUp(new MergeProjectable()))).execute();
            return childContext.getRewritePlan();
        }, mvPlan, plan, false);
        // Construct structInfo once for use later
        Optional<StructInfo> structInfoOptional = MaterializationContext.constructStructInfo(mvPlan, plan,
                cascadesContext, new BitSet());
        return Pair.of(mvPlan, structInfoOptional.orElse(null));
    }
}
