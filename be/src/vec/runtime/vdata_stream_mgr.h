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

#pragma once

#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "common/be_mock_util.h"
#include "common/global_types.h"
#include "common/status.h"
#include "util/runtime_profile.h"

namespace google {
#include "common/compile_check_begin.h"
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {
class RuntimeState;
class RowDescriptor;
class PTransmitDataParams;
namespace pipeline {
class ExchangeLocalState;
}

namespace vectorized {
class VDataStreamRecvr;

class VDataStreamMgr {
public:
    /**
     * VDataStreamMgr：向量化数据流管理器（Exchange 路由/目录服务）
     * 职责：
     * - 注册/查找/注销接收端 VDataStreamRecvr（按 fragment_instance_id + node_id 管理）
     * - 处理发送端的 RPC（transmit_block），将远端来的 PBlock 路由到对应接收器
     * - 提供并发安全的接入，配合内存/背压统计
     */
    VDataStreamMgr();
    MOCK_FUNCTION ~VDataStreamMgr();

    // 创建并注册一个接收器，返回共享指针（供 Exchange Source 使用）
    std::shared_ptr<VDataStreamRecvr> create_recvr(
            RuntimeState* state, RuntimeProfile::HighWaterMarkCounter* memory_used_counter,
            const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
            RuntimeProfile* profile, bool is_merging, size_t data_queue_capacity);

    // 查找接收器：按 (fragment_instance_id, node_id) 键；acquire_lock 控制是否加锁
    MOCK_FUNCTION Status find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id,
                                    std::shared_ptr<VDataStreamRecvr>* res,
                                    bool acquire_lock = true);

    // 注销接收器：从路由表移除
    Status deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

    // 处理远端发送的块（RPC 入口）：解析请求，定位接收器并投递数据块
    Status transmit_block(const PTransmitDataParams* request, ::google::protobuf::Closure** done,
                          const int64_t wait_for_worker);

    // 取消某个 fragment 的所有接收器（错误/超时/取消场景）
    void cancel(const TUniqueId& fragment_instance_id, Status exec_status);

private:
    // 全局读写锁，保护接收器路由表
    std::shared_mutex _lock;
    using StreamMap = std::unordered_multimap<uint32_t, std::shared_ptr<VDataStreamRecvr>>;
    StreamMap _receiver_map; // 哈希桶路由：hash(fragment_instance_id, node_id) -> recvr

    // 用于维护去重/遍历顺序的集合（按 fragment_id + node_id 有序）
    struct ComparisonOp {
        bool operator()(const std::pair<doris::TUniqueId, PlanNodeId>& a,
                        const std::pair<doris::TUniqueId, PlanNodeId>& b) const {
            if (a.first.hi < b.first.hi) {
                return true;
            } else if (a.first.hi > b.first.hi) {
                return false;
            } else if (a.first.lo < b.first.lo) {
                return true;
            } else if (a.first.lo > b.first.lo) {
                return false;
            }
            return a.second < b.second;
        }
    };
    using FragmentStreamSet = std::set<std::pair<TUniqueId, PlanNodeId>, ComparisonOp>;
    FragmentStreamSet _fragment_stream_set;

    // 计算路由哈希
    uint32_t get_hash_value(const TUniqueId& fragment_instance_id, PlanNodeId node_id);
};
} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
