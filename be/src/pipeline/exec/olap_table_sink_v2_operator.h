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

#include "operator.h"
#include "vec/sink/writer/vtablet_writer_v2.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class OlapTableSinkV2OperatorX;

class OlapTableSinkV2LocalState final
        : public AsyncWriterSink<vectorized::VTabletWriterV2, OlapTableSinkV2OperatorX> {
public:
    using Base = AsyncWriterSink<vectorized::VTabletWriterV2, OlapTableSinkV2OperatorX>;
    using Parent = OlapTableSinkV2OperatorX;
    ENABLE_FACTORY_CREATOR(OlapTableSinkV2LocalState);
    OlapTableSinkV2LocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {};
    friend class OlapTableSinkV2OperatorX;
};

class OlapTableSinkV2OperatorX final : public DataSinkOperatorX<OlapTableSinkV2LocalState> {
public:
    using Base = DataSinkOperatorX<OlapTableSinkV2LocalState>;
    OlapTableSinkV2OperatorX(ObjectPool* pool, int operator_id, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& t_output_expr)
            : Base(operator_id, 0, 0),
              _row_desc(row_desc),
              _t_output_expr(t_output_expr),
              _pool(pool) {};

    Status init(const TDataSink& thrift_sink) override {
        RETURN_IF_ERROR(Base::init(thrift_sink));
        // From the thrift expressions create the real exprs.
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
        return Status::OK();
    }

    /**
     * 准备阶段：初始化操作符的执行环境
     * 在查询执行前调用，负责表达式准备和资源分配
     * 
     * @param state 运行时状态对象，包含查询执行的上下文信息
     * @return 准备结果状态，成功返回OK，失败返回错误状态
     */
    Status prepare(RuntimeState* state) override {
        // 调用基类的准备方法，完成基础初始化工作
        RETURN_IF_ERROR(Base::prepare(state));
        
        // 准备输出表达式：将Thrift格式的表达式转换为可执行的表达式上下文
        // _output_vexpr_ctxs 包含了数据转换和过滤的表达式
        // 这些表达式用于在数据写入前进行必要的转换操作
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
        
        // 打开表达式：激活表达式上下文，使其可以开始执行
        // 这一步通常在prepare之后、open之前调用
        return vectorized::VExpr::open(_output_vexpr_ctxs, state);
    }

    /**
     * 数据接收：处理来自上游操作符的数据块
     * 这是Pipeline执行引擎中数据流动的核心方法，负责接收、处理和转发数据
     * 
     * @param state 运行时状态对象，包含查询执行的上下文信息
     * @param in_block 输入数据块指针，包含要处理的数据行
     * @param eos 是否到达数据流末尾的标志（End Of Stream）
     * @return 处理结果状态，成功返回OK，失败返回错误状态
     */
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        // 获取当前线程的本地状态对象
        // 每个线程都有独立的本地状态，避免并发访问冲突
        auto& local_state = get_local_state(state);
        
        // 性能监控：记录执行时间
        // SCOPED_TIMER 是一个RAII工具，自动记录从构造到析构的时间间隔
        SCOPED_TIMER(local_state.exec_time_counter());
        
        // 统计计数器更新：记录输入的行数
        // 用于性能分析和监控，了解数据处理的吞吐量
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
        
        // 调用本地状态的sink方法，执行实际的数据处理逻辑
        // 将数据块传递给本地状态，由具体的实现类处理数据写入
        return local_state.sink(state, in_block, eos);
    }

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        local_state._writer->set_low_memory_mode();
    }

private:
    friend class OlapTableSinkV2LocalState;
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;
    const RowDescriptor& _row_desc;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    const std::vector<TExpr>& _t_output_expr;
    ObjectPool* _pool = nullptr;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
