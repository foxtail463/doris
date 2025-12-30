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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.processor.post.materialize.LazyMaterializeTopN;
import org.apache.doris.nereids.processor.post.runtimefilterv2.RuntimeFilterV2Generator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TRuntimeFilterMode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;

/**
 * PlanPostprocessors: after copy out the plan from the memo, we use this rewriter to rewrite plan by visitor.
 */
public class PlanPostProcessors {
    private final CascadesContext cascadesContext;

    public PlanPostProcessors(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext can not be null");
    }

    /**
     * 对物理计划进行后处理
     * 
     * 在从 memo 中复制出计划后，使用后处理器对物理计划进行优化和转换。
     * 按照 getProcessors() 返回的顺序，依次应用每个后处理器。
     * 
     * @param physicalPlan 输入的物理计划
     * @return 经过后处理优化后的物理计划
     */
    public PhysicalPlan process(PhysicalPlan physicalPlan) {
        // 使用 MoreFieldsThread.keepFunctionSignature 保持函数签名，用于调试和追踪
        return MoreFieldsThread.keepFunctionSignature(() -> {
            PhysicalPlan resultPlan = physicalPlan;
            // 按顺序应用所有后处理器，每个处理器都会对计划进行转换
            for (PlanPostProcessor processor : getProcessors()) {
                resultPlan = (PhysicalPlan) processor.processRoot(resultPlan, cascadesContext);
            }
            return resultPlan;
        });
    }

    /**
     * 获取所有后处理器列表
     * 
     * 按照特定的顺序组织后处理器，顺序很重要，因为后面的处理器可能依赖前面处理器的结果。
     * 后处理器的执行顺序：
     * 
     * 1. 基础优化阶段：
     *    - PushDownFilterThroughProject: 将过滤条件下推通过 Project 节点
     *    - RemoveUselessProjectPostProcessor: 移除无用的 Project 节点
     *    - RecomputeLogicalPropertiesProcessor: 重新计算逻辑属性
     * 
     * 2. TopN 延迟物化阶段（可选）：
     *    - LazyMaterializeTopN: TopN 延迟物化，必须在 MergeProjectPostProcessor 之前执行
     * 
     * 3. Project 合并阶段：
     *    - MergeProjectPostProcessor: 合并多个连续的 Project 节点
     * 
     * 4. 公共子表达式优化阶段：
     *    - ProjectAggregateExpressionsForCse: 为聚合表达式提取公共子表达式（可选）
     *    - CommonSubExpressionOpt: 公共子表达式优化
     * 
     * 5. TopN 优化阶段（注意：从这里开始不能替换 PLAN NODE）：
     *    - PushTopnToAgg: 将 TopN 下推到聚合节点（可选）
     *    - TopNScanOpt: TopN 扫描优化
     * 
     * 6. 分布式计划阶段：
     *    - FragmentProcessor: 将计划切分为多个 Fragment
     * 
     * 7. 运行时过滤器阶段（如果启用）：
     *    - RegisterParent: 注册父节点信息
     *    - RuntimeFilterGenerator: 生成运行时过滤器
     *    - RuntimeFilterPruner: 运行时过滤器裁剪（可选）
     *    - RuntimeFilterPrunerForExternalTable: 外部表运行时过滤器裁剪（可选）
     *    - RuntimeFilterV2Generator: 生成 V2 版本的运行时过滤器
     * 
     * 8. 验证阶段：
     *    - Validator: 验证计划的正确性
     * 
     * @return 按执行顺序排列的后处理器列表
     */
    public List<PlanPostProcessor> getProcessors() {
        Builder<PlanPostProcessor> builder = ImmutableList.builder();
        
        // 阶段1: 基础优化 - 过滤下推和 Project 清理
        builder.add(new PushDownFilterThroughProject());
        builder.add(new RemoveUselessProjectPostProcessor());
        builder.add(new RecomputeLogicalPropertiesProcessor());
        
        // 阶段2: TopN 延迟物化（可选）
        // 注意：LazyMaterializeTopN 必须在 MergeProjectPostProcessor 之前执行
        // 因为 PhysicalLazyMaterialize.materializedSlots 必须是 topN.getOutput() 的子序列
        if (cascadesContext.getConnectContext().getSessionVariable().enableTopnLazyMaterialization()) {
            builder.add(new LazyMaterializeTopN());
        }
        
        // 阶段3: Project 合并
        builder.add(new MergeProjectPostProcessor());

        // 阶段4: 公共子表达式优化
        if (cascadesContext.getConnectContext().getSessionVariable().enableAggregateCse) {
            builder.add(new ProjectAggregateExpressionsForCse());
        }
        builder.add(new CommonSubExpressionOpt());
        
        // 阶段5: TopN 优化
        // 注意：从这里开始不能替换 PLAN NODE，因为后续的 FragmentProcessor 等需要稳定的节点结构
        if (cascadesContext.getConnectContext().getSessionVariable().pushTopnToAgg) {
            builder.add(new PushTopnToAgg());
        }
        builder.add(new TopNScanOpt());
        
        // 阶段6: 分布式计划处理
        builder.add(new FragmentProcessor());
        
        // 阶段7: 运行时过滤器生成（如果启用）
        if (!cascadesContext.getConnectContext().getSessionVariable().getRuntimeFilterMode()
                        .toUpperCase().equals(TRuntimeFilterMode.OFF.name())) {
            builder.add(new RegisterParent());
            builder.add(new org.apache.doris.nereids.processor.post.RuntimeFilterGenerator());
            // 运行时过滤器裁剪（可选）
            if (ConnectContext.get().getSessionVariable().enableRuntimeFilterPrune) {
                builder.add(new RuntimeFilterPruner());
                // 外部表运行时过滤器裁剪（可选）
                if (ConnectContext.get().getSessionVariable().runtimeFilterPruneForExternal) {
                    builder.add(new RuntimeFilterPrunerForExternalTable());
                }
            }
            builder.add(new RuntimeFilterV2Generator());
        }
        
        // 阶段8: 计划验证
        builder.add(new Validator());
        
        return builder.build();
    }
}
