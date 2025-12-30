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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.rewrite.AdaptiveBottomUpRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.AdaptiveTopDownRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.BottomUpVisitorRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.CostBasedRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.CustomRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteBottomUpJob;
import org.apache.doris.nereids.jobs.rewrite.PlanTreeRewriteTopDownJob;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.jobs.rewrite.RootPlanTreeRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.TopDownVisitorRewriteJob;
import org.apache.doris.nereids.jobs.rewrite.TopicRewriteJob;
import org.apache.doris.nereids.rules.FilteredRules;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Base class for executing all jobs.
 *
 * Each batch of rules will be uniformly executed.
 */
public abstract class AbstractBatchJobExecutor {
    private static final ThreadLocal<Set<Class<Plan>>> NOT_TRAVERSE_CHILDREN = new ThreadLocal<>();
    private static final Predicate<Plan> TRAVERSE_ALL_PLANS = plan -> true;

    protected CascadesContext cascadesContext;

    public AbstractBatchJobExecutor(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not null");
    }

    /**
    * 扁平化处理重写作业列表，将TopicRewriteJob中的作业展开为可执行的实际作业
    * 
    * 该方法是重写作业处理的核心工具方法，主要功能包括：
    * 1. 过滤空值：移除null作业，确保作业列表的完整性
    * 2. 扁平化处理：将TopicRewriteJob中的子作业展开为平铺的作业列表
    * 3. 条件处理：对于有条件的TopicRewriteJob，保持其包装结构；对于无条件的，直接展开子作业
    * 4. 类型转换：将处理后的作业流转换为不可变的列表结构
    * 
    * @param jobs 可变参数的重写作业数组
    * @return 扁平化后的重写作业列表
    */
    public static List<RewriteJob> jobs(RewriteJob... jobs) {
        return Arrays.stream(jobs)
                // 过滤空值：移除null作业，确保作业列表的完整性
                .filter(Objects::nonNull)
                // 扁平化处理：根据作业类型决定是否展开子作业
                .flatMap(job -> {
                    // 如果是TopicRewriteJob且没有条件，则展开其子作业列表
                    if (job instanceof TopicRewriteJob && !((TopicRewriteJob) job).condition.isPresent()) {
                        return ((TopicRewriteJob) job).jobs.stream();
                    } else {
                        // 其他情况（普通作业或有条件的TopicRewriteJob）直接返回单个作业
                        return Stream.of(job);
                    }
                })
                // 收集为不可变列表：确保返回的列表结构稳定且不可修改
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * 在一次批量执行（如一轮规则执行或一条优化流水线）中，临时指定：对某些 Plan 类型的节点，不再向下遍历其子节点。
     *
     * 使用方式：
     * <pre>
     * notTraverseChildrenOf(Set.of(LogicalSubquery.class, LogicalView.class), () -> {
     *     // 在这个 action 执行期间，Bottom-Up / Top-Down 遍历都会跳过这些类型节点的 children
     *     runSomeBatchOptimize();
     *     return null;
     * });
     * </pre>
     *
     * 实现细节：
     * - NOT_TRAVERSE_CHILDREN 是一个 ThreadLocal&lt;Set&lt;Class&lt;? extends Plan&gt;&gt;&gt;；
     * - 这里通过 try/finally 确保：在 action 执行完成后，一定会清理掉这个 ThreadLocal，避免污染后续任务。
     *
     * @param notTraverseClasses 在本次 action 中不需要继续向下遍历子节点的 Plan 类型集合
     * @param action             需要在该上下文中执行的逻辑（通常是一次完整的优化流程）
     * @return                   action.get() 的返回值，原样透传
     */
    public static <T> T notTraverseChildrenOf(
            Set<Class<? extends Plan>> notTraverseClasses, Supplier<T> action) {
        try {
            NOT_TRAVERSE_CHILDREN.set((Set) notTraverseClasses);
            return action.get();
        } finally {
            NOT_TRAVERSE_CHILDREN.remove();
        }
    }

    public static TopicRewriteJob topic(String topicName, RewriteJob... jobs) {
        return new TopicRewriteJob(topicName, Arrays.asList(jobs), null);
    }

    public static TopicRewriteJob topic(String topicName, Predicate<CascadesContext> condition, RewriteJob... jobs) {
        return new TopicRewriteJob(topicName, Arrays.asList(jobs), condition);
    }

    public static RewriteJob costBased(RewriteJob... jobs) {
        return new CostBasedRewriteJob(Arrays.asList(jobs));
    }

    public static RewriteJob bottomUp(RuleFactory... ruleFactories) {
        return bottomUp(Arrays.asList(ruleFactories));
    }

    /** bottomUp */
    public static RewriteJob bottomUp(List<RuleFactory> ruleFactories) {
        Rules rules = new FilteredRules(ruleFactories.stream()
                .map(RuleFactory::buildRules)
                .flatMap(List::stream)
                .collect(ImmutableList.toImmutableList()));
        Predicate<Plan> traversePredicate = getTraversePredicate();
        BottomUpVisitorRewriteJob visitorJob = new BottomUpVisitorRewriteJob(rules, traversePredicate);
        RootPlanTreeRewriteJob stackJob = new RootPlanTreeRewriteJob(rules,
                PlanTreeRewriteBottomUpJob::new, traversePredicate, true);
        return new AdaptiveBottomUpRewriteJob(visitorJob, stackJob);
    }

    public static RewriteJob topDown(RuleFactory... ruleFactories) {
        return topDown(Arrays.asList(ruleFactories));
    }

    public static RewriteJob topDown(List<RuleFactory> ruleFactories) {
        return topDown(ruleFactories, true);
    }

    /**
     * 创建自顶向下的重写作业
     * 
     * 该方法创建了一个自顶向下（Top-Down）的重写作业，采用双重策略确保重写规则的充分应用：
     * 1. 访问者模式：通过TopDownVisitorRewriteJob遍历计划树，应用重写规则
     * 2. 栈式处理：通过RootPlanTreeRewriteJob使用栈结构处理复杂的嵌套计划
     * 3. 自适应策略：结合两种策略，根据实际情况选择最优的执行方式
     * 
     * 自顶向下重写的特点：
     * - 先处理父节点，再处理子节点
     * - 适合需要从根节点开始应用规则的场景
     * - 能够处理复杂的嵌套计划结构
     * - 支持条件执行和一次性执行控制
     * 
     * @param ruleFactories 规则工厂列表，用于构建具体的重写规则
     * @param once 是否只执行一次，true表示规则只应用一次，false表示重复应用直到收敛
     * @return 自顶向下的重写作业
     */
    public static RewriteJob topDown(List<RuleFactory> ruleFactories, boolean once) {
        // 构建规则集合：将所有规则工厂的规则合并为统一的规则列表
        Rules rules = new FilteredRules(ruleFactories.stream()
                .map(RuleFactory::buildRules)  // 从每个工厂构建规则
                .flatMap(List::stream)         // 扁平化所有规则
                .collect(ImmutableList.toImmutableList()));  // 收集为不可变列表
        
        // 创建访问者模式的重写作业：使用访问者模式遍历计划树
        TopDownVisitorRewriteJob visitorJob = new TopDownVisitorRewriteJob(rules, getTraversePredicate());
        
        // 创建栈式处理的重写作业：使用栈结构处理复杂的嵌套计划
        RootPlanTreeRewriteJob stackJob
                = new RootPlanTreeRewriteJob(rules, PlanTreeRewriteTopDownJob::new, getTraversePredicate(), once);
        
        // 返回自适应重写作业：结合两种策略，根据实际情况选择最优执行方式
        return new AdaptiveTopDownRewriteJob(visitorJob, stackJob);
    }

    public static RewriteJob custom(RuleType ruleType, Supplier<CustomRewriter> planRewriter) {
        return new CustomRewriteJob(planRewriter, ruleType);
    }

    /**
     * 执行批量重写作业
     * 
     * <p>核心功能：
     * <ol>
     *   <li>获取并执行所有的重写作业</li>
     *   <li>处理条件化的 TopicRewriteJob（主题包装的作业）</li>
     *   <li>支持作业的重复执行直到收敛</li>
     * </ol>
     * 
     * <p>执行流程：
     * <pre>{@code
     *  1. 获取所有的重写作业列表
     *  2. 循环遍历作业列表（支持动态扩展）：
     *     a. 获取当前作业的上下文
     *     b. 判断作业类型：
     *        - TopicRewriteJob：检查条件，满足则展开子作业
     *        - 普通 RewriteJob：执行并可能重复应用
     *  3. 对于普通作业，根据是否满足执行条件来决定是否运行
     *  4. 对于可重复执行的作业（isOnce() == false），循环应用直到不再有修改
     * }</pre>
     * 
     * <p>TopicRewriteJob 处理逻辑：
     * <ul>
     *   <li>TopicRewriteJob 是一个包装作业，包含多个子作业和一个可选的条件</li>
     *   <li>如果条件存在，只有当条件为 true 时才会展开其子作业</li>
     *   <li>子作业会被插入到当前位置的下一个位置，保持执行顺序</li>
     * </ul>
     * 
     * <p>重复执行机制（Fixed Point）：
     * <ul>
     *   <li>对于 {@code isOnce() == false} 的作业，会被重复执行直到计划不再变化</li>
     *   <li>通过 {@code jobContext.isRewritten()} 来判断计划是否被修改</li>
     *   <li>这种方式确保规则能够被应用多次直到收敛（Fixed Point）</li>
     *   <li>典型的应用场景：常量传播、谓词合并等需要多次应用才能完全生效的规则</li>
     * </ul>
     * 
     * <p>性能优化：
     * <ul>
     *   <li>动态列表扩展：使用 ArrayList 支持作业列表的动态增长</li>
     *   <li>条件化执行：通过 shouldRun() 方法支持高级的执行控制逻辑</li>
     *   <li>上下文管理：通过 JobContext 管理作业执行状态</li>
     * </ul>
     * 
     * <p>典型应用场景：
     * <ul>
     *   <li>表达式规范化：常量折叠、函数签名计算</li>
     *   <li>查询优化：谓词下推、列裁剪、常量传播</li>
     *   <li>计划重写：join 顺序优化、子查询消除</li>
     * </ul>
     * 
     * @see TopicRewriteJob 主题包装的重写作业
     * @see JobContext 作业执行上下文
     * @see RewriteJob#isOnce() 作业是否只执行一次
     */
    public void execute() {
        // 获取所有的重写作业并转为可变列表（支持动态插入）
        List<RewriteJob> jobs = Lists.newArrayList(getJobs());
        
        // 循环处理所有作业（支持动态扩展）
        for (int i = 0; i < jobs.size(); i++) {
            // 获取当前作业的执行上下文
            JobContext jobContext = cascadesContext.getCurrentJobContext();
            RewriteJob currentJob = jobs.get(i);

            // 处理 TopicRewriteJob（主题包装的作业）
            if (currentJob instanceof TopicRewriteJob) {
                TopicRewriteJob topicRewriteJob = (TopicRewriteJob) currentJob;
                Optional<Predicate<CascadesContext>> condition = topicRewriteJob.condition;
                
                // 如果没有条件或条件满足，则展开其子作业
                if (!condition.isPresent() || condition.get().test(jobContext.getCascadesContext())) {
                    // 将子作业插入到当前位置的下一个位置
                    jobs.addAll(i + 1, topicRewriteJob.jobs);
                }
                continue;
            }

            // 执行普通作业
            if (shouldRun(currentJob, jobContext, jobs, i)) {
                // 重复执行直到计划不再变化（Fixed Point）
                do {
                    // 重置重写标志
                    jobContext.setRewritten(false);
                    // 执行当前作业
                    currentJob.execute(jobContext);
                    // 如果作业不是一次性执行（isOnce() == false）且计划被修改，则继续执行
                } while (!currentJob.isOnce() && jobContext.isRewritten());
            }
        }
    }

    public abstract List<RewriteJob> getJobs();

    protected boolean shouldRun(RewriteJob rewriteJob, JobContext jobContext, List<RewriteJob> jobs, int jobIndex) {
        return true;
    }

    private static Predicate<Plan> getTraversePredicate() {
        Set<Class<Plan>> notTraverseChildren = NOT_TRAVERSE_CHILDREN.get();
        return notTraverseChildren == null
                ? TRAVERSE_ALL_PLANS
                : new NotTraverseChildren(notTraverseChildren);
    }

    private static class NotTraverseChildren implements Predicate<Plan> {
        private final Set<Class<Plan>> notTraverseChildren;

        public NotTraverseChildren(Set<Class<Plan>> notTraverseChildren) {
            this.notTraverseChildren = Objects.requireNonNull(notTraverseChildren, "notTraversePlans can not be null");
        }

        @Override
        public boolean test(Plan plan) {
            return !notTraverseChildren.contains(plan.getClass());
        }
    }
}
