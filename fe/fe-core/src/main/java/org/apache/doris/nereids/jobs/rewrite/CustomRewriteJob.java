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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.PlanProcess;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import java.util.BitSet;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Custom rewrite the plan.
 * Just pass the plan node to the 'CustomRewriter', and the 'CustomRewriter' rule will handle it.
 * The 'CustomRewriter' rule use the 'Visitor' design pattern to implement the rule.
 * You can check the 'CustomRewriter' interface to see which rules use this way to do rewrite.
 */
public class CustomRewriteJob implements RewriteJob {

    private final RuleType ruleType;
    private final Supplier<CustomRewriter> customRewriter;

    /**
     * Constructor.
     */
    public CustomRewriteJob(Supplier<CustomRewriter> rewriter, RuleType ruleType) {
        this.ruleType = Objects.requireNonNull(ruleType, "ruleType cannot be null");
        this.customRewriter = Objects.requireNonNull(rewriter, "customRewriter cannot be null");
    }

    /**
     * 执行自定义重写作业
     * 
     * <p>该方法执行自定义重写器（CustomRewriter）对查询计划的重写。CustomRewriter 采用访问者模式
     * 实现，适用于复杂的重写逻辑，如常量传播、谓词推导等。
     * 
     * <p>执行流程：
     * <ol>
     *   <li>检查规则是否被禁用：如果规则在禁用列表中，直接返回</li>
     *   <li>获取当前重写计划：从 CascadesContext 中获取待重写的查询计划</li>
     *   <li>执行重写：调用 CustomRewriter.rewriteRoot() 对计划进行重写</li>
     *   <li>处理重写结果：如果计划发生变化，记录重写过程和规则应用情况</li>
     *   <li>更新计划：将重写后的计划设置回 CascadesContext</li>
     * </ol>
     * 
     * <p>与普通规则的区别：
     * <ul>
     *   <li>普通规则（Pattern-Based）：使用 PatternMatcher 匹配计划结构，调用 transform() 方法</li>
     *   <li>自定义规则（CustomRewriter）：采用访问者模式，通过 rewriteRoot() 实现复杂逻辑</li>
     *   <li>CustomRewriter 更适合需要遍历整个计划树的场景（如常量传播、谓词推导）</li>
     * </ul>
     * 
     * <p>典型的 CustomRewriter 实现：
     * <ul>
     *   <li>ConstantPropagation：常量传播优化</li>
     *   <li>InferPredicates：谓词推导</li>
     *   <li>ColumnPruning：列裁剪</li>
     *   <li>EliminateUnnecessaryProject：消除不必要的投影</li>
     * </ul>
     * 
     * @param context 作业上下文，包含 CascadesContext、StatementContext 等执行环境信息
     */
    @Override
    public void execute(JobContext context) {
        // 步骤1：检查规则是否被禁用
        BitSet disableRules = Job.getDisableRules(context);
        if (disableRules.get(ruleType.type())) {
            return;
        }
        
        // 步骤2：获取当前查询计划
        CascadesContext cascadesContext = context.getCascadesContext();
        Plan root = cascadesContext.getRewritePlan();
        
        // COUNTER_TRACER.log(CounterEvent.of(Memo.get=-StateId(), CounterType.JOB_EXECUTION, group, logicalExpression,
        //         root));
        
        // 步骤3：执行自定义重写器
        // 调用 CustomRewriter.rewriteRoot() 方法，该方法内部使用访问者模式遍历整个计划树
        // 例如 ConstantPropagation.rewriteRoot() 会调用 plan.accept(this, context) 触发访问者模式
        Plan rewrittenRoot = customRewriter.get().rewriteRoot(root, context);
        
        // 步骤4：处理空结果
        if (rewrittenRoot == null) {
            return;
        }

        // don't remove this comment, it can help us to trace some bug when developing.

        // 步骤5：检查计划是否发生变化并记录
        if (!root.deepEquals(rewrittenRoot)) {
            // 如果启用了计划过程显示，记录重写前后的计划形状
            if (cascadesContext.showPlanProcess()) {
                PlanProcess planProcess = new PlanProcess(
                        ruleType.name(),
                        root.treeString(true, root),
                        rewrittenRoot.treeString(true, rewrittenRoot));
                cascadesContext.addPlanProcess(planProcess);
            }
            // 记录规则已被应用，用于统计和监控
            context.getCascadesContext().getStatementContext().ruleSetApplied(ruleType);
        }
        
        // 步骤6：更新重写后的计划
        cascadesContext.setRewritePlan(rewrittenRoot);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    public RuleType getRuleType() {
        return ruleType;
    }

}
