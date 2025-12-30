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

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.rules.exploration.IntersectReorder;
import org.apache.doris.nereids.rules.exploration.MergeProjectsCBO;
import org.apache.doris.nereids.rules.exploration.TransposeAggSemiJoinProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLAsscomProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLeftAssociateProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinRightAssociateProject;
import org.apache.doris.nereids.rules.exploration.join.JoinCommute;
import org.apache.doris.nereids.rules.exploration.join.JoinExchangeBothProject;
import org.apache.doris.nereids.rules.exploration.join.LogicalJoinSemiJoinTransposeProject;
import org.apache.doris.nereids.rules.exploration.join.OuterJoinAssocProject;
import org.apache.doris.nereids.rules.exploration.join.OuterJoinLAsscomProject;
import org.apache.doris.nereids.rules.exploration.join.PushDownProjectThroughInnerOuterJoin;
import org.apache.doris.nereids.rules.exploration.join.PushDownProjectThroughSemiJoin;
import org.apache.doris.nereids.rules.exploration.join.SemiJoinSemiJoinTransposeProject;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewAggregateOnNoneAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterProjectAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterProjectJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterProjectScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewLimitAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewLimitJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewLimitScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewOnlyScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterProjectJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewTopNAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewTopNJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewTopNScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewWindowAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewWindowJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewWindowScanRule;
import org.apache.doris.nereids.rules.expression.ExpressionNormalizationAndOptimization;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.rules.implementation.LogicalAssertNumRowsToPhysicalAssertNumRows;
import org.apache.doris.nereids.rules.implementation.LogicalBlackholeSinkToPhysicalBlackholeSink;
import org.apache.doris.nereids.rules.implementation.LogicalCTEAnchorToPhysicalCTEAnchor;
import org.apache.doris.nereids.rules.implementation.LogicalCTEConsumerToPhysicalCTEConsumer;
import org.apache.doris.nereids.rules.implementation.LogicalCTEProducerToPhysicalCTEProducer;
import org.apache.doris.nereids.rules.implementation.LogicalDeferMaterializeOlapScanToPhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalDeferMaterializeResultSinkToPhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.rules.implementation.LogicalDeferMaterializeTopNToPhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.rules.implementation.LogicalDictionarySinkToPhysicalDictionarySink;
import org.apache.doris.nereids.rules.implementation.LogicalEmptyRelationToPhysicalEmptyRelation;
import org.apache.doris.nereids.rules.implementation.LogicalEsScanToPhysicalEsScan;
import org.apache.doris.nereids.rules.implementation.LogicalExceptToPhysicalExcept;
import org.apache.doris.nereids.rules.implementation.LogicalFileScanToPhysicalFileScan;
import org.apache.doris.nereids.rules.implementation.LogicalFileSinkToPhysicalFileSink;
import org.apache.doris.nereids.rules.implementation.LogicalFilterToPhysicalFilter;
import org.apache.doris.nereids.rules.implementation.LogicalGenerateToPhysicalGenerate;
import org.apache.doris.nereids.rules.implementation.LogicalHiveTableSinkToPhysicalHiveTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalHudiScanToPhysicalHudiScan;
import org.apache.doris.nereids.rules.implementation.LogicalIcebergTableSinkToPhysicalIcebergTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalIntersectToPhysicalIntersect;
import org.apache.doris.nereids.rules.implementation.LogicalJdbcScanToPhysicalJdbcScan;
import org.apache.doris.nereids.rules.implementation.LogicalJdbcTableSinkToPhysicalJdbcTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToHashJoin;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToNestedLoopJoin;
import org.apache.doris.nereids.rules.implementation.LogicalLimitToPhysicalLimit;
import org.apache.doris.nereids.rules.implementation.LogicalOdbcScanToPhysicalOdbcScan;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalOlapTableSinkToPhysicalOlapTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalOneRowRelationToPhysicalOneRowRelation;
import org.apache.doris.nereids.rules.implementation.LogicalPartitionTopNToPhysicalPartitionTopN;
import org.apache.doris.nereids.rules.implementation.LogicalProjectToPhysicalProject;
import org.apache.doris.nereids.rules.implementation.LogicalRepeatToPhysicalRepeat;
import org.apache.doris.nereids.rules.implementation.LogicalResultSinkToPhysicalResultSink;
import org.apache.doris.nereids.rules.implementation.LogicalSchemaScanToPhysicalSchemaScan;
import org.apache.doris.nereids.rules.implementation.LogicalSortToPhysicalQuickSort;
import org.apache.doris.nereids.rules.implementation.LogicalTVFRelationToPhysicalTVFRelation;
import org.apache.doris.nereids.rules.implementation.LogicalTopNToPhysicalTopN;
import org.apache.doris.nereids.rules.implementation.LogicalUnionToPhysicalUnion;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow;
import org.apache.doris.nereids.rules.implementation.SplitAggMultiPhase;
import org.apache.doris.nereids.rules.implementation.SplitAggMultiPhaseWithoutGbyKey;
import org.apache.doris.nereids.rules.implementation.SplitAggWithoutDistinct;
import org.apache.doris.nereids.rules.rewrite.CreatePartitionTopNFromWindow;
import org.apache.doris.nereids.rules.rewrite.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.EliminateOuterJoin;
import org.apache.doris.nereids.rules.rewrite.MaxMinFilterPushDown;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeGenerates;
import org.apache.doris.nereids.rules.rewrite.MergeLimits;
import org.apache.doris.nereids.rules.rewrite.MergeOneRowRelationIntoUnion;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.rules.rewrite.MergeSetOperations;
import org.apache.doris.nereids.rules.rewrite.MergeSetOperationsExcept;
import org.apache.doris.nereids.rules.rewrite.PushDownAliasThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownExpressionsInHashCondition;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughAggregation;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughGenerate;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughPartitionTopN;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughRepeat;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughSetOperation;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughSort;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughWindow;
import org.apache.doris.nereids.rules.rewrite.PushDownJoinOtherCondition;
import org.apache.doris.nereids.rules.rewrite.PushDownLimitDistinctThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownProject;
import org.apache.doris.nereids.rules.rewrite.PushDownProjectThroughLimit;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNDistinctThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownTopNThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushProjectThroughUnion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/**
 * Containers for set of different type rules.
 */
public class RuleSet {

    public static final List<Rule> EXPLORATION_RULES = planRuleFactories()
            .add(new MergeProjectsCBO())
            .add(IntersectReorder.INSTANCE)
            .build();

    public static final List<Rule> OTHER_REORDER_RULES = planRuleFactories()
            .addAll(EXPLORATION_RULES)
            .add(OuterJoinLAsscomProject.INSTANCE)
            .add(SemiJoinSemiJoinTransposeProject.INSTANCE)
            .add(LogicalJoinSemiJoinTransposeProject.INSTANCE)
            .add(PushDownProjectThroughInnerOuterJoin.INSTANCE)
            .add(PushDownProjectThroughSemiJoin.INSTANCE)
            .add(TransposeAggSemiJoinProject.INSTANCE)
            .addAll(new PushDownTopNThroughJoin().buildRules())
            .addAll(new PushDownLimitDistinctThroughJoin().buildRules())
            .addAll(new PushDownTopNDistinctThroughJoin().buildRules())
            .build();

    public static final List<RuleFactory> PUSH_DOWN_FILTERS = ImmutableList.of(
            new MaxMinFilterPushDown(),
            new CreatePartitionTopNFromWindow(),
            new PushDownFilterThroughProject(),
            new PushDownFilterThroughSort(),
            new PushDownJoinOtherCondition(),
            new PushDownFilterThroughJoin(),
            new PushDownExpressionsInHashCondition(),
            new PushDownFilterThroughAggregation(),
            new PushDownFilterThroughRepeat(),
            new MergeOneRowRelationIntoUnion(),
            // PushProjectThroughUnion should invoke before PushDownFilterThroughSetOperation
            new PushProjectThroughUnion(),
            new MergeSetOperations(),
            new MergeSetOperationsExcept(),
            new PushDownFilterThroughSetOperation(),
            new PushDownFilterThroughGenerate(),
            new PushDownProjectThroughLimit(),
            new PushDownProject(),
            new EliminateOuterJoin(),
            new MergeProjectable(),
            new MergeFilters(),
            new MergeGenerates(),
            new MergeLimits(),
            new PushDownAliasThroughJoin(),
            new PushDownFilterThroughWindow(),
            new PushDownFilterThroughPartitionTopN(),
            ExpressionNormalizationAndOptimization.NO_MIN_MAX_RANGE_INSTANCE,
            new EliminateFilter()
            );

    public static final List<Rule> IMPLEMENTATION_RULES = planRuleFactories()
            .add(new LogicalCTEProducerToPhysicalCTEProducer())
            .add(new LogicalCTEConsumerToPhysicalCTEConsumer())
            .add(new LogicalCTEAnchorToPhysicalCTEAnchor())
            .add(new LogicalRepeatToPhysicalRepeat())
            .add(new LogicalFilterToPhysicalFilter())
            .add(new LogicalJoinToHashJoin())
            .add(new LogicalJoinToNestedLoopJoin())
            .add(new LogicalOlapScanToPhysicalOlapScan())
            .add(new LogicalDeferMaterializeOlapScanToPhysicalDeferMaterializeOlapScan())
            .add(new LogicalSchemaScanToPhysicalSchemaScan())
            .add(new LogicalHudiScanToPhysicalHudiScan())
            .add(new LogicalFileScanToPhysicalFileScan())
            .add(new LogicalJdbcScanToPhysicalJdbcScan())
            .add(new LogicalOdbcScanToPhysicalOdbcScan())
            .add(new LogicalEsScanToPhysicalEsScan())
            .add(new LogicalProjectToPhysicalProject())
            .add(new LogicalLimitToPhysicalLimit())
            .add(new LogicalWindowToPhysicalWindow())
            .add(new LogicalSortToPhysicalQuickSort())
            .add(new LogicalTopNToPhysicalTopN())
            .add(new LogicalDeferMaterializeTopNToPhysicalDeferMaterializeTopN())
            .add(new LogicalPartitionTopNToPhysicalPartitionTopN())
            .add(new LogicalAssertNumRowsToPhysicalAssertNumRows())
            .add(new LogicalOneRowRelationToPhysicalOneRowRelation())
            .add(new LogicalEmptyRelationToPhysicalEmptyRelation())
            .add(new LogicalTVFRelationToPhysicalTVFRelation())
            .add(new AggregateStrategies())
            .add(SplitAggWithoutDistinct.INSTANCE)
            .add(SplitAggMultiPhase.INSTANCE)
            .add(SplitAggMultiPhaseWithoutGbyKey.INSTANCE)
            .add(new LogicalUnionToPhysicalUnion())
            .add(new LogicalExceptToPhysicalExcept())
            .add(new LogicalIntersectToPhysicalIntersect())
            .add(new LogicalGenerateToPhysicalGenerate())
            .add(new LogicalOlapTableSinkToPhysicalOlapTableSink())
            .add(new LogicalHiveTableSinkToPhysicalHiveTableSink())
            .add(new LogicalIcebergTableSinkToPhysicalIcebergTableSink())
            .add(new LogicalJdbcTableSinkToPhysicalJdbcTableSink())
            .add(new LogicalFileSinkToPhysicalFileSink())
            .add(new LogicalResultSinkToPhysicalResultSink())
            .add(new LogicalDeferMaterializeResultSinkToPhysicalDeferMaterializeResultSink())
            .add(new LogicalDictionarySinkToPhysicalDictionarySink())
            .add(new LogicalBlackholeSinkToPhysicalBlackholeSink())
            .build();

    // left-zig-zag tree is used when column stats are not available.
    public static final List<Rule> LEFT_ZIG_ZAG_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.LEFT_ZIG_ZAG)
            .add(InnerJoinLAsscomProject.LEFT_ZIG_ZAG)
            .addAll(OTHER_REORDER_RULES)
            .build();

    public static final List<Rule> ZIG_ZAG_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.ZIG_ZAG)
            .add(InnerJoinLAsscomProject.INSTANCE)
            .build();

    public static final List<Rule> BUSHY_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.BUSHY)
            .add(InnerJoinLeftAssociateProject.INSTANCE)
            .add(InnerJoinRightAssociateProject.INSTANCE)
            .add(JoinExchangeBothProject.INSTANCE)
            .add(OuterJoinAssocProject.INSTANCE)
            .build();

    public static final List<Rule> ZIG_ZAG_TREE_JOIN_REORDER_RULES = ImmutableList.<Rule>builder()
            .addAll(ZIG_ZAG_TREE_JOIN_REORDER)
            .addAll(OTHER_REORDER_RULES)
            .build();

    public static final List<Rule> BUSHY_TREE_JOIN_REORDER_RULES = ImmutableList.<Rule>builder()
            .addAll(BUSHY_TREE_JOIN_REORDER)
            .addAll(OTHER_REORDER_RULES)
            .build();

    /**
     * 物化视图重写规则集合，用于主查询的 CBO（基于成本的优化）阶段。
     * 
     * 这些规则用于将查询计划重写为使用物化视图，规则名称中的操作顺序表示从外到内的计划结构。
     * 例如：MaterializedViewProjectJoinRule 匹配 "Project on Join" 模式。
     */
    public static final List<Rule> MATERIALIZED_VIEW_IN_CBO_RULES = planRuleFactories()
            // ==================== Join 相关规则 ====================
            // 匹配包含 Join 操作的查询，尝试用物化视图替换 Join 及其上层的操作
            .add(MaterializedViewProjectJoinRule.INSTANCE)              // Project on Join：匹配 SELECT ... FROM t1 JOIN t2 模式
            .add(MaterializedViewFilterJoinRule.INSTANCE)               // Filter on Join：匹配 WHERE ... FROM t1 JOIN t2 模式
            .add(MaterializedViewFilterProjectJoinRule.INSTANCE)        // Filter on Project on Join：匹配 WHERE ... SELECT ... FROM t1 JOIN t2 模式
            .add(MaterializedViewProjectFilterJoinRule.INSTANCE)        // Project on Filter on Join：匹配 SELECT ... WHERE ... FROM t1 JOIN t2 模式
            
            // ==================== Aggregate 相关规则 ====================
            // 匹配包含聚合操作的查询，尝试用物化视图替换聚合及其上层的操作
            .add(MaterializedViewAggregateRule.INSTANCE)                // Aggregate：匹配 SELECT SUM(...) FROM ... GROUP BY ... 模式
            .add(MaterializedViewProjectAggregateRule.INSTANCE)         // Project on Aggregate：匹配 SELECT ... FROM (SELECT SUM(...) FROM ... GROUP BY ...) 模式
            .add(MaterializedViewFilterAggregateRule.INSTANCE)          // Filter on Aggregate：匹配 WHERE ... SELECT SUM(...) FROM ... GROUP BY ... 模式
            .add(MaterializedViewProjectFilterAggregateRule.INSTANCE)   // Project on Filter on Aggregate：匹配 SELECT ... WHERE ... SELECT SUM(...) FROM ... GROUP BY ... 模式
            .add(MaterializedViewFilterProjectAggregateRule.INSTANCE)   // Filter on Project on Aggregate：匹配 WHERE ... SELECT ... FROM (SELECT SUM(...) FROM ... GROUP BY ...) 模式
            
            // ==================== Scan 相关规则 ====================
            // 匹配包含表扫描的查询，尝试用物化视图替换扫描及其上层的操作
            .add(MaterializedViewFilterScanRule.INSTANCE)               // Filter on Scan：匹配 SELECT * FROM table WHERE ... 模式
            .add(MaterializedViewFilterProjectScanRule.INSTANCE)        // Filter on Project on Scan：匹配 WHERE ... SELECT ... FROM table 模式
            .add(MaterializedViewProjectScanRule.INSTANCE)              // Project on Scan：匹配 SELECT col1, col2 FROM table 模式
            .add(MaterializedViewProjectFilterScanRule.INSTANCE)        // Project on Filter on Scan：匹配 SELECT col1, col2 FROM table WHERE ... 模式
            .add(MaterializedViewAggregateOnNoneAggregateRule.INSTANCE) // Aggregate on non-aggregate：匹配在非聚合查询上应用聚合的模式
            .add(MaterializedViewOnlyScanRule.INSTANCE)                 // Only Scan：匹配直接扫描表的模式，用于直接使用物化视图替换基表
            
            // ==================== Limit 相关规则 ====================
            // 匹配包含 LIMIT 操作的查询，尝试用物化视图替换 Limit 及其下层的操作
            .add(MaterializedViewLimitScanRule.INSTANCE)                 // Limit on Scan：匹配 SELECT * FROM table LIMIT n 模式
            .add(MaterializedViewLimitJoinRule.INSTANCE)                 // Limit on Join：匹配 SELECT * FROM t1 JOIN t2 LIMIT n 模式
            .add(MaterializedViewLimitAggregateRule.INSTANCE)            // Limit on Aggregate：匹配 SELECT SUM(...) FROM ... GROUP BY ... LIMIT n 模式
            
            // ==================== TopN 相关规则 ====================
            // 匹配包含 TopN（ORDER BY ... LIMIT）操作的查询，尝试用物化视图替换 TopN 及其下层的操作
            .add(MaterializedViewTopNAggregateRule.INSTANCE)            // TopN on Aggregate：匹配 SELECT SUM(...) FROM ... GROUP BY ... ORDER BY ... LIMIT n 模式
            .add(MaterializedViewTopNJoinRule.INSTANCE)                  // TopN on Join：匹配 SELECT * FROM t1 JOIN t2 ORDER BY ... LIMIT n 模式
            .add(MaterializedViewTopNScanRule.INSTANCE)                  // TopN on Scan：匹配 SELECT * FROM table ORDER BY ... LIMIT n 模式
            
            // ==================== Window 相关规则 ====================
            // 匹配包含窗口函数的查询，尝试用物化视图替换 Window 及其下层的操作
            .add(MaterializedViewWindowScanRule.INSTANCE)                // Window on Scan：匹配 SELECT ROW_NUMBER() OVER(...) FROM table 模式
            .add(MaterializedViewWindowJoinRule.INSTANCE)                // Window on Join：匹配 SELECT ROW_NUMBER() OVER(...) FROM t1 JOIN t2 模式
            .add(MaterializedViewWindowAggregateRule.INSTANCE)          // Window on Aggregate：匹配 SELECT ROW_NUMBER() OVER(...) FROM (SELECT SUM(...) FROM ... GROUP BY ...) 模式
            .build();

    /**
     * 物化视图重写规则集合，用于 Pre-MV Rewrite 阶段（RBO 阶段相关的特殊优化阶段）。
     * 
     * 包含所有 MATERIALIZED_VIEW_IN_CBO_RULES 的规则，并额外添加一个规则用于处理
     * Project-Filter-Project-Join 这种复杂的嵌套模式。
     */
    public static final List<Rule> MATERIALIZED_VIEW_IN_RBO_RULES = planRuleFactories()
            .addAll(MATERIALIZED_VIEW_IN_CBO_RULES)
            .add(MaterializedViewProjectFilterProjectJoinRule.INSTANCE) // Project on Filter on Project on Join：匹配 SELECT ... WHERE ... SELECT ... FROM t1 JOIN t2 模式
            .build();

    public static final List<Rule> DPHYP_REORDER_RULES = ImmutableList.<Rule>builder()
            .add(JoinCommute.BUSHY.build())
            .build();

    public List<Rule> getDPHypReorderRules() {
        return DPHYP_REORDER_RULES;
    }

    public List<Rule> getZigZagTreeJoinReorder() {
        return ZIG_ZAG_TREE_JOIN_REORDER_RULES;
    }

    public List<Rule> getLeftZigZagTreeJoinReorder() {
        return LEFT_ZIG_ZAG_TREE_JOIN_REORDER;
    }

    public List<Rule> getBushyTreeJoinReorder() {
        return BUSHY_TREE_JOIN_REORDER_RULES;
    }

    public List<Rule> getImplementationRules() {
        return IMPLEMENTATION_RULES;
    }

    public List<Rule> getMaterializedViewInRBORules() {
        return MATERIALIZED_VIEW_IN_RBO_RULES;
    }

    public static RuleFactories planRuleFactories() {
        return new RuleFactories();
    }

    /**
     * generate rule factories.
     */
    public static class RuleFactories {
        final Builder<Rule> rules = ImmutableList.builder();

        public RuleFactories add(RuleFactory ruleFactory) {
            rules.addAll(ruleFactory.buildRules());
            return this;
        }

        public RuleFactories addAll(List<Rule> rules) {
            this.rules.addAll(rules);
            return this;
        }

        public List<Rule> build() {
            return rules.build();
        }
    }
}
