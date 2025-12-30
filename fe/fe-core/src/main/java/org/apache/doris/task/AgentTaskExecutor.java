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

package org.apache.doris.task;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class AgentTaskExecutor {

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
            Config.max_agent_task_threads_num, "agent-task-pool", true);

    public AgentTaskExecutor() {
    }

    public static void submit(AgentBatchTask task) {
        // 空任务保护：若为空则直接返回
        if (task == null) {
            return;
        }
        try {
            // 将批任务提交到 agent-task 线程池异步执行
            EXECUTOR.submit(task);
        } catch (RejectedExecutionException e) {
            // 线程池已满时的兜底处理：给任务标记失败并提示调大线程数配置
            String msg = "Task is rejected, because the agent-task-pool is full, "
                    + "consider increasing the max_agent_task_threads_num config";
            for (AgentTask t : task.getAllTasks()) {
                // 对需要重发的任务：若已在队列中存在，跳过（会由重试机制再次提交）
                if (t.isNeedResendType() && AgentTaskQueue.contains(t)) {
                    continue;
                }
                // 其他任务：直接标记失败，向上层反馈原因
                t.failedWithMsg(msg);
            }
        }
    }

}
