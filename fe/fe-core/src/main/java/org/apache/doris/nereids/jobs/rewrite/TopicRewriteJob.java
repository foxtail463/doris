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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** TopicRewriteJob */
public class TopicRewriteJob implements RewriteJob {

    public final String topicName;
    public final List<RewriteJob> jobs;
    public final Optional<Predicate<CascadesContext>> condition;

    /**
     * 构造函数：创建主题重写作业容器
     * 
     * 该构造函数负责创建TopicRewriteJob实例，并自动处理嵌套的TopicRewriteJob扁平化。
     * 主要功能包括：
     * 1. 设置主题名称：用于调试和日志记录
     * 2. 条件处理：将条件包装为Optional，支持空条件
     * 3. 自动扁平化：递归展开无条件的嵌套TopicRewriteJob，避免深层嵌套
     * 4. 保持结构：有条件的TopicRewriteJob保持包装结构，支持条件执行
     * 
     * @param topicName 主题名称，用于标识和调试
     * @param jobs 重写作业列表，可能包含嵌套的TopicRewriteJob
     * @param condition 执行条件，为null表示无条件执行
     */
    public TopicRewriteJob(String topicName, List<RewriteJob> jobs, Predicate<CascadesContext> condition) {
        // 设置主题名称：用于调试、日志记录和性能分析
        this.topicName = topicName;
        
        // 条件处理：将条件包装为Optional，支持空条件（无条件执行）
        this.condition = Optional.ofNullable(condition);
        
        // 自动扁平化处理：递归展开嵌套的TopicRewriteJob，优化执行结构
        this.jobs = jobs.stream()
                .flatMap(job -> {
                    // 如果是无条件的TopicRewriteJob，则展开其子作业列表
                    if (job instanceof TopicRewriteJob && !((TopicRewriteJob) job).condition.isPresent()) {
                        return ((TopicRewriteJob) job).jobs.stream();
                    } else {
                        // 其他情况（普通作业或有条件的TopicRewriteJob）保持原结构
                        return Stream.of(job);
                    }
                })
                // 收集为不可变列表：确保作业列表的稳定性和不可修改性
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public void execute(JobContext jobContext) {
        throw new AnalysisException("should not execute topic rewrite job " + topicName + " directly.");
    }

    @Override
    public boolean isOnce() {
        return true;
    }
}
