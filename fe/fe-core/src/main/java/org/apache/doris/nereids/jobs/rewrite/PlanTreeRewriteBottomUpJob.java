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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.rules.Rules;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * PlanTreeRewriteBottomUpJob
 * The job is used for bottom-up rewrite. If some rewrite rules can take effect,
 * we will process all the rules from the leaf node again. So there are some rules that can take effect interactively,
 * we should use the 'Bottom-Up' job to handle it.
 */
public class PlanTreeRewriteBottomUpJob extends PlanTreeRewriteJob {

    // REWRITE_STATE_KEY represents the key to store the 'RewriteState'. Each plan node has their own 'RewriteState'.
    // Different 'RewriteState' has different actions,
    // so we will do specified action for each node based on their 'RewriteState'.
    private static final String REWRITE_STATE_KEY = "rewrite_state";
    private final RewriteJobContext rewriteJobContext;
    private final Rules rules;
    private final int batchId;

    enum RewriteState {
        // 'REWRITE_THIS' means the current plan node can be handled immediately. If the plan state is 'REWRITE_THIS',
        // it means all of its children's state are 'REWRITTEN'. Because we handle the plan tree bottom up.
        REWRITE_THIS,
        // 'REWRITTEN' means the current plan have been handled already, we don't need to do anything else.
        REWRITTEN,
        // 'ENSURE_CHILDREN_REWRITTEN' means we need to check the children for the current plan node first.
        // It means some plans have changed after rewrite, so we need traverse the plan tree and reset their state.
        // All the plan nodes need to be handled again.
        ENSURE_CHILDREN_REWRITTEN
    }

    public PlanTreeRewriteBottomUpJob(
            RewriteJobContext rewriteJobContext, JobContext context,
            Predicate<Plan> isTraverseChildren, Rules rules) {
        super(JobType.BOTTOM_UP_REWRITE, context, isTraverseChildren);
        this.rewriteJobContext = Objects.requireNonNull(rewriteJobContext, "rewriteContext cannot be null");
        this.rules = Objects.requireNonNull(rules, "rules cannot be null");
        this.batchId = rewriteJobContext.batchId;
    }

    @Override
    public void execute() {
        // We'll do different actions based on their different states.
        // You can check the comment in 'RewriteState' structure for more details.
        Plan plan = rewriteJobContext.plan;
        RewriteState state = getState(plan, batchId);
        switch (state) {
            case REWRITE_THIS:
                rewriteThis();
                return;
            case ENSURE_CHILDREN_REWRITTEN:
                ensureChildrenRewritten();
                return;
            case REWRITTEN:
                rewriteJobContext.result = plan;
                return;
            default:
                throw new IllegalStateException("Unknown rewrite state: " + state);
        }
    }

    private void rewriteThis() {
        // 1) 先把“当前节点 + 已重写的子节点”重新 link 成一个完整的 plan，
        //    之后的规则匹配 / 执行都是基于这个 plan 展开的。
        Plan plan = linkChildren(rewriteJobContext.plan, rewriteJobContext.childrenContext);
        if (rules.getCurrentAndChildrenRules(plan).isEmpty()) {
            // 如果当前 plan 在本批次没有任何可用规则，直接标记为 REWRITTEN 并结束。
            setState(plan, RewriteState.REWRITTEN, batchId);
            rewriteJobContext.setResult(plan);
            return;
        }

        // 2) 尝试对当前 plan 应用规则，返回是否生成了新 plan。
        RewriteResult rewriteResult = rewrite(plan, rules, rewriteJobContext);
        if (rewriteResult.hasNewPlan) {
            RewriteJobContext newJobContext = rewriteJobContext.withPlan(rewriteResult.plan);
            RewriteState state = getState(rewriteResult.plan, batchId);
            // 某些“消除类”规则可能直接返回子节点（相当于当前节点被删掉），
            // 这类 plan 会被标记成 REWRITTEN，不需要再次入队。
            if (state == RewriteState.REWRITTEN) {
                newJobContext.setResult(rewriteResult.plan);
                return;
            }
            // 3) 对于真正产生了新 plan 的情况，需要把它重新放回队列，
            //    让它的子树再跑一遍 bottom-up 重写。
            pushJob(new PlanTreeRewriteBottomUpJob(newJobContext, context, isTraverseChildren, rules));
            setState(rewriteResult.plan, RewriteState.ENSURE_CHILDREN_REWRITTEN, batchId);
        } else {
            // rewrite() 没有生成新 plan，直接标记为 REWRITTEN 即可。
            setState(rewriteResult.plan, RewriteState.REWRITTEN, batchId);
            rewriteJobContext.setResult(rewriteResult.plan);
        }
    }

    private void ensureChildrenRewritten() {
        // 当前这一轮调度处理的节点
        Plan plan = rewriteJobContext.plan;
        if (rules.getCurrentAndChildrenRules(plan).isEmpty()) {
            // 如果当前节点及其子树在本 batch 下完全没有可用的 rewrite 规则，
            // 说明这一批对它来说是“无事可做”，直接标记为 REWRITTEN 并返回。
            setState(plan, RewriteState.REWRITTEN, batchId);
            rewriteJobContext.setResult(plan);
            return;
        }

        // 否则，当前节点/子树有规则可用：
        // 1）先把当前节点状态改成 REWRITE_THIS，表示“等孩子处理完后，该轮到我自己重写了”；
        int batchId = rewriteJobContext.batchId;
        setState(plan, RewriteState.REWRITE_THIS, batchId);
        // 2）把当前节点本身重新入队，再来一次 execute()，届时会走到 rewriteThis() 分支。
        pushJob(new PlanTreeRewriteBottomUpJob(rewriteJobContext, context, isTraverseChildren, rules));

        // 3）对需要遍历子节点的情况，为每个 child 也派发一个 BottomUp 重写任务，
        //    这样可以保证在“轮到自己重写”之前，所有子树都已经先跑完一轮规则。
        //    尤其是有些规则会一下子生成多个新节点（plan node 个数 > 1），
        //    这些新节点也需要递归应用本 batch 的规则。
        if (isTraverseChildren.test(plan)) {
            pushChildrenJobs(plan);
        }
    }

    /**
     * 为当前 plan 的每个子节点创建并推送对应的 Bottom-Up 重写任务。
     *
     * 注意：
     * - 这里不直接在当前方法中递归，而是为每个 child 创建一个新的 {@link PlanTreeRewriteBottomUpJob}，
     *   交给调度器（JobStack/JobPool）统一调度，保持遍历顺序统一；
     * - 使用 switch + 特化 0/1/2 个子节点的分支，是一种轻量的 loop unrolling 优化，
     *   常见的单子节点（如一元算子）和二元算子（如 Join）可以少一些分支和数组操作。
     */
    private void pushChildrenJobs(Plan plan) {
        List<Plan> children = plan.children();
        switch (children.size()) {
            case 0:
                // 没有子节点，直接返回。
                return;
            case 1:
                // 单子节点：为第 0 个子节点创建对应的 RewriteJobContext 并推入调度队列。
                Plan child = children.get(0);
                RewriteJobContext childRewriteJobContext = new RewriteJobContext(
                        child, rewriteJobContext, 0, false, batchId);
                pushJob(new PlanTreeRewriteBottomUpJob(childRewriteJobContext, context, isTraverseChildren, rules));
                return;
            case 2:
                // 二元节点（典型如 Join）：先推右孩子，再推左孩子，
                // 保持自底向上的处理顺序（右子树先被调度，之后才是左子树）。
                Plan right = children.get(1);
                RewriteJobContext rightRewriteJobContext = new RewriteJobContext(
                        right, rewriteJobContext, 1, false, batchId);
                pushJob(new PlanTreeRewriteBottomUpJob(rightRewriteJobContext, context, isTraverseChildren, rules));

                Plan left = children.get(0);
                RewriteJobContext leftRewriteJobContext = new RewriteJobContext(
                        left, rewriteJobContext, 0, false, batchId);
                pushJob(new PlanTreeRewriteBottomUpJob(leftRewriteJobContext, context, isTraverseChildren, rules));
                return;
            default:
                // 多子节点的情况：从后往前遍历子节点，依次为每个 child 创建重写任务。
                // 之所以从尾到头，是为了配合调度栈（后推先出），最终达到从左到右的遍历顺序。
                boolean ignore = false; // 占位，强调这里只是控制顺序，没有其他副作用。
                for (int i = children.size() - 1; i >= 0; i--) {
                    child = children.get(i);
                    childRewriteJobContext = new RewriteJobContext(
                            child, rewriteJobContext, i, false, batchId);
                    pushJob(new PlanTreeRewriteBottomUpJob(childRewriteJobContext, context, isTraverseChildren, rules));
                }
        }
    }

    /**
     * 读取某个 plan 节点在当前 batch 下的重写状态。
     *
     * - 如果节点从未记录过状态，或记录的 batchId 与当前 batch 不一致，
     *   说明这是“新的一轮重写”，默认状态为 ENSURE_CHILDREN_REWRITTEN（先确保子树被处理）。
     * - 否则返回之前写入的 {@code rewriteState}。
     */
    static RewriteState getState(Plan plan, int currentBatchId) {
        Optional<RewriteStateContext> state = plan.getMutableState(REWRITE_STATE_KEY);
        if (!state.isPresent()) {
            // 没有任何状态，视为需要先检查子节点。
            return RewriteState.ENSURE_CHILDREN_REWRITTEN;
        }
        RewriteStateContext context = state.get();
        if (context.batchId != currentBatchId) {
            // 状态是上一批规则留下的，本批需要重新从“确保子节点被重写”开始。
            return RewriteState.ENSURE_CHILDREN_REWRITTEN;
        }
        // 正常情况：直接返回本批次记录的状态。
        return context.rewriteState;
    }

    static void setState(Plan plan, RewriteState state, int batchId) {
        plan.setMutableState(REWRITE_STATE_KEY, new RewriteStateContext(state, batchId));
    }

    private static class RewriteStateContext {
        private final RewriteState rewriteState;
        private final int batchId;

        public RewriteStateContext(RewriteState rewriteState, int batchId) {
            this.rewriteState = rewriteState;
            this.batchId = batchId;
        }

        @Override
        public String toString() {
            return "RewriteStateContext{rewriteState=" + rewriteState + ", batchId=" + batchId + '}';
        }
    }
}
