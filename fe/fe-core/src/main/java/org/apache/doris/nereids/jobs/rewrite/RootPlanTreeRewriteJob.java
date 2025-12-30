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
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.jobs.scheduler.JobStack;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * 根计划树重写任务
 * 
 * 该类实现了对根计划树的整体重写任务。它是重写框架的入口点，负责：
 * 1. 从CascadesContext中获取根计划
 * 2. 创建根重写任务上下文
 * 3. 构建具体的重写任务并推送到任务池
 * 4. 执行任务池中的所有任务
 * 5. 管理重写过程的批次跟踪
 */
public class RootPlanTreeRewriteJob implements RewriteJob {
    /** 批次ID生成器，用于跟踪重写批次，确保不同批次的重写可以区分 */
    private static final AtomicInteger BATCH_ID = new AtomicInteger();

    /** 重写规则集合 */
    private final Rules rules;
    
    /** 重写任务构建器，用于创建具体的重写任务 */
    private final RewriteJobBuilder rewriteJobBuilder;
    
    /** 是否只执行一次，true表示一次性任务，false表示可重复执行 */
    private final boolean once;
    
    /** 遍历子节点的谓词，用于控制是否遍历特定类型节点的子节点 */
    private final Predicate<Plan> isTraverseChildren;

    /**
     * 构造函数（简化版本）
     * 
     * @param rules 重写规则集合
     * @param rewriteJobBuilder 重写任务构建器
     * @param once 是否只执行一次
     */
    public RootPlanTreeRewriteJob(Rules rules, RewriteJobBuilder rewriteJobBuilder, boolean once) {
        this(rules, rewriteJobBuilder, plan -> true, once);
    }

    /**
     * 构造函数（完整版本）
     * 
     * @param rules 重写规则集合
     * @param rewriteJobBuilder 重写任务构建器
     * @param isTraverseChildren 遍历子节点的谓词，用于控制是否遍历特定类型节点的子节点
     * @param once 是否只执行一次
     */
    public RootPlanTreeRewriteJob(
            Rules rules, RewriteJobBuilder rewriteJobBuilder, Predicate<Plan> isTraverseChildren, boolean once) {
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.rewriteJobBuilder = Objects.requireNonNull(rewriteJobBuilder, "rewriteJobBuilder cannot be null");
        this.once = once;
        this.isTraverseChildren = isTraverseChildren;
    }

    /**
     * 执行根计划树重写任务
     * 
     * 这是重写框架的核心入口方法，负责：
     * 1. 获取当前的重写计划
     * 2. 创建根重写任务上下文
     * 3. 构建具体的重写任务
     * 4. 推送任务到任务池并执行
     * 5. 清理上下文状态
     * 
     * @param context 任务上下文，包含级联上下文等信息
     */
    @Override
    public void execute(JobContext context) {
        // 获取级联上下文
        CascadesContext cascadesContext = context.getCascadesContext();
        
        // 从级联上下文中获取当前的重写计划
        Plan root = cascadesContext.getRewritePlan();
        
        // 生成新的批次ID，用于跟踪这次重写批次
        int batchId = BATCH_ID.incrementAndGet();
        
        // 创建根重写任务上下文，负责管理整个重写过程
        RootRewriteJobContext rewriteJobContext = new RootRewriteJobContext(
                root, false, context, batchId);
        
        // 使用构建器创建具体的重写任务
        Job rewriteJob = rewriteJobBuilder.build(rewriteJobContext, context, isTraverseChildren, rules);

        // 将重写任务推送到任务池
        context.getScheduleContext().pushJob(rewriteJob);
        
        // 执行任务池中的所有任务
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);

        // 清理当前根重写任务上下文
        cascadesContext.setCurrentRootRewriteJobContext(null);
    }

    /**
     * 判断是否为一次性任务
     * 
     * @return true表示一次性任务，false表示可重复执行
     */
    @Override
    public boolean isOnce() {
        return once;
    }

    /**
     * 重写任务构建器接口
     * 
     * 该接口定义了如何根据重写上下文构建具体的重写任务。
     * 不同的重写策略（如自底向上、自顶向下）可以有不同的实现。
     */
    public interface RewriteJobBuilder {
        /**
         * 构建重写任务
         * 
         * @param rewriteJobContext 重写任务上下文
         * @param jobContext 任务上下文
         * @param isTraverseChildren 遍历子节点的谓词
         * @param rules 重写规则集合
         * @return 构建的重写任务
         */
        Job build(RewriteJobContext rewriteJobContext, JobContext jobContext,
                Predicate<Plan> isTraverseChildren, Rules rules);
    }

    /**
     * 根重写任务上下文
     * 
     * 该类继承自RewriteJobContext，专门用于管理根计划的重写过程。
     * 它负责：
     * 1. 标识自己是根重写上下文
     * 2. 将重写结果设置回CascadesContext
     * 3. 提供创建新上下文的方法
     * 4. 管理重写后的计划组装
     */
    public static class RootRewriteJobContext extends RewriteJobContext {

        /** 关联的任务上下文 */
        private final JobContext jobContext;

        /**
         * 构造函数
         * 
         * @param plan 要重写的计划
         * @param childrenVisited 子节点是否已访问
         * @param jobContext 任务上下文
         * @param batchId 批次ID
         */
        RootRewriteJobContext(Plan plan, boolean childrenVisited, JobContext jobContext, int batchId) {
            super(plan, null, -1, childrenVisited, batchId);
            this.jobContext = Objects.requireNonNull(jobContext, "jobContext cannot be null");
            // 将自己设置为当前根重写任务上下文
            jobContext.getCascadesContext().setCurrentRootRewriteJobContext(this);
        }

        /**
         * 判断是否为根重写上下文
         * 
         * @return 总是返回true，因为这是根重写上下文
         */
        @Override
        public boolean isRewriteRoot() {
            return true;
        }

        /**
         * 设置重写结果
         * 
         * 将重写后的计划设置回CascadesContext中，这样整个查询优化过程
         * 就能看到最新的计划状态。
         * 
         * @param result 重写后的计划
         */
        @Override
        public void setResult(Plan result) {
            jobContext.getCascadesContext().setRewritePlan(result);
        }

        /**
         * 创建带有新的子节点访问状态的上下文
         * 
         * @param childrenVisited 新的子节点访问状态
         * @return 新的根重写任务上下文
         */
        @Override
        public RewriteJobContext withChildrenVisited(boolean childrenVisited) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext, batchId);
        }

        /**
         * 创建带有新计划的上下文
         * 
         * @param plan 新的计划
         * @return 新的根重写任务上下文
         */
        @Override
        public RewriteJobContext withPlan(Plan plan) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext, batchId);
        }

        /**
         * 创建带有新计划和子节点访问状态的上下文
         * 
         * @param plan 新的计划
         * @param childrenVisited 新的子节点访问状态
         * @return 新的根重写任务上下文
         */
        @Override
        public RewriteJobContext withPlanAndChildrenVisited(Plan plan, boolean childrenVisited) {
            return new RootRewriteJobContext(plan, childrenVisited, jobContext, batchId);
        }

        /**
         * 获取最新的计划
         * 
         * 通过LinkPlanJob将重写后的各个子计划组装成完整的计划树。
         * 这个过程是自底向上的，确保所有子计划都已经正确组装。
         * 
         * @return 组装后的完整计划
         */
        public Plan getNewestPlan() {
            // 创建任务栈用于管理链接任务
            JobStack jobStack = new JobStack();
            
            // 创建根链接任务
            LinkPlanJob linkPlanJob = new LinkPlanJob(
                    jobContext, this, null, false, jobStack);
            
            // 将根链接任务推入栈中
            jobStack.push(linkPlanJob);
            
            // 执行所有链接任务，直到栈为空
            while (!jobStack.isEmpty()) {
                Job job = jobStack.pop();
                job.execute();
            }
            
            // 返回组装后的完整计划
            return linkPlanJob.result;
        }
    }

    /**
     * 获取当前和子节点的规则列表
     * 
     * @return 适用于当前节点及其子节点的规则列表
     */
    public List<Rule> getRules() {
        return rules.getCurrentAndChildrenRules();
    }

    /**
     * 返回字符串表示
     * 
     * @return 规则的字符串表示
     */
    @Override
    public String toString() {
        return rules.toString();
    }

    /**
     * 计划链接任务
     * 
     * 该类用于将重写后的各个子计划组装成完整的计划树。
     * 它使用栈来管理链接过程，确保自底向上地组装计划。
     */
    private static class LinkPlanJob extends Job {
        /** 父链接任务 */
        LinkPlanJob parentJob;
        
        /** 重写任务上下文 */
        RewriteJobContext rewriteJobContext;
        
        /** 子节点的结果计划数组 */
        Plan[] childrenResult;
        
        /** 当前节点的结果计划 */
        Plan result;
        
        /** 是否已经链接过 */
        boolean linked;
        
        /** 任务栈，用于管理链接任务 */
        JobStack jobStack;

        /**
         * 构造函数
         * 
         * @param context 任务上下文
         * @param rewriteJobContext 重写任务上下文
         * @param parentJob 父链接任务
         * @param linked 是否已经链接过
         * @param jobStack 任务栈
         */
        private LinkPlanJob(JobContext context, RewriteJobContext rewriteJobContext,
                LinkPlanJob parentJob, boolean linked, JobStack jobStack) {
            super(JobType.LINK_PLAN, context);
            this.rewriteJobContext = rewriteJobContext;
            this.parentJob = parentJob;
            this.linked = linked;
            // 根据当前计划的子节点数量创建结果数组
            this.childrenResult = new Plan[rewriteJobContext.plan.arity()];
            this.jobStack = jobStack;
        }

        /**
         * 执行链接任务
         * 
         * 该方法实现了自底向上的计划链接过程：
         * 1. 如果还没链接，则标记为已链接，并创建所有子节点的链接任务
         * 2. 如果当前节点已有结果，直接链接结果
         * 3. 否则，使用子节点的结果组装当前节点
         */
        @Override
        public void execute() {
            if (!linked) {
                // 第一次执行：标记为已链接，创建子节点的链接任务
                linked = true;
                jobStack.push(this); // 将自己重新推入栈，稍后处理
                
                // 从右到左遍历子节点，创建链接任务（逆序推入栈，正序执行）
                for (int i = rewriteJobContext.childrenContext.length - 1; i >= 0; i--) {
                    RewriteJobContext childContext = rewriteJobContext.childrenContext[i];
                    if (childContext != null) {
                        jobStack.push(new LinkPlanJob(context, childContext, this, false, jobStack));
                    }
                }
            } else if (rewriteJobContext.result != null) {
                // 当前节点已有重写结果，直接链接
                linkResult(rewriteJobContext.result);
            } else {
                // 当前节点没有重写结果，使用子节点的结果组装
                Plan[] newChildren = new Plan[childrenResult.length];
                for (int i = 0; i < newChildren.length; i++) {
                    Plan childResult = childrenResult[i];
                    if (childResult == null) {
                        // 如果子节点没有结果，使用原始子节点
                        childResult = rewriteJobContext.plan.child(i);
                    }
                    newChildren[i] = childResult;
                }
                // 使用新的子节点组装当前节点
                linkResult(rewriteJobContext.plan.withChildren(newChildren));
            }
        }

        /**
         * 链接结果计划
         * 
         * 将当前节点的结果传递给父节点或保存为根结果。
         * 
         * @param result 要链接的结果计划
         */
        private void linkResult(Plan result) {
            rewriteJobContext.tmpPlan = result;
            if (parentJob != null) {
                // 如果有父任务，将结果传递给父任务的子结果数组
                parentJob.childrenResult[rewriteJobContext.childIndexInParentContext] = result;
            } else {
                // 如果没有父任务，说明这是根节点，保存为最终结果
                this.result = result;
            }
        }
    }
}
