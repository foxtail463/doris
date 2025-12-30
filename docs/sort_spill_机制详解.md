# Doris Sort Spill 机制详解

## 目录
1. [问题背景](#1-问题背景)
2. [解决方案概述](#2-解决方案概述)
3. [核心组件](#3-核心组件)
4. [详细流程](#4-详细流程)
5. [代码实现分析](#5-代码实现分析)

---

## 1. 问题背景

### 1.1 为什么需要 Spill？

在 Doris 中，当执行 `ORDER BY` 查询时，如果数据量很大，排序操作可能会消耗大量内存：

**问题场景**：
```sql
-- 假设表有 1 亿行数据，每行 1KB
SELECT * FROM large_table ORDER BY id LIMIT 1000;
```

**内存挑战**：
- 如果全内存排序，需要约 100GB 内存
- 大多数服务器没有这么多内存
- 即使有，也会影响其他查询的性能

**传统方案的问题**：
- ❌ 限制查询大小（用户体验差）
- ❌ 增加内存（成本高）
- ❌ 查询失败（OOM）

### 1.2 Spill 的解决方案

**核心思想**：当内存不足时，将部分已排序的数据写入磁盘，释放内存，继续处理剩余数据。

**优势**：
- ✅ 可以处理任意大小的数据
- ✅ 内存使用可控
- ✅ 查询不会因内存不足而失败

---

## 2. 解决方案概述

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Sort 查询执行流程                          │
└─────────────────────────────────────────────────────────────┘

阶段 1: 数据收集和排序（SpillSortSinkOperatorX）
  │
  ├─ 接收上游数据块
  ├─ 追加到 FullSorter
  ├─ 内存不足时触发 Spill
  │   ├─ 将已排序的数据写入磁盘（SpillStream）
  │   └─ 释放内存，继续处理
  │
  └─ 数据流结束时（EOS）
      ├─ 如果已 Spill：继续 Spill 剩余数据
      └─ 通知下游可以读取

阶段 2: 数据读取和合并（SpillSortSourceOperatorX）
  │
  ├─ 如果未 Spill：直接从内存读取
  │
  └─ 如果已 Spill：
      ├─ 多轮归并：每次归并 N 个 streams
      ├─ 初始化多路归并器（VSortedRunMerger）
      ├─ 从多个 SpillStream 读取数据
      └─ 合并成全局有序结果
```

### 2.2 完整流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                     Sort Spill 完整流程                          │
└─────────────────────────────────────────────────────────────────┘

【Sink 阶段：数据收集和排序】
                                        
输入数据块 Block1
    │
    ├─→ SpillSortSinkOperatorX::sink()
    │       │
    │       ├─→ SortSinkOperatorX::sink()
    │       │       │
    │       │       └─→ FullSorter::append_block()
    │       │               │
    │       │               ├─ 追加到 unsorted_block
    │       │               │
    │       │               └─ 如果 unsorted_block >= 64MB
    │       │                       └─→ _do_sort()
    │       │                           └─ 排序后添加到 sorted_blocks
    │       │
    │       └─→ 更新内存统计
    │
    ├─→ 输入数据块 Block2
    │       └─→ ...（重复上述过程）
    │
    └─→ 内存不足（触发 Spill）
            │
            ├─→ PipelineTask::_try_to_reserve_memory() 失败
            │       │
            │       └─→ revocable_mem_size() >= 32KB
            │               │
            │               └─→ SpillSortSinkOperatorX::revoke_memory()
            │                       │
            │                       ├─→ 注册 SpillStream1
            │                       │       ├─ 选择磁盘（优先 SSD）
            │                       │       ├─ 创建目录
            │                       │       └─ 创建 SpillWriter/Reader
            │                       │
            │                       ├─→ FullSorter::prepare_for_read(true)
            │                       │       └─ 完成排序，准备输出
            │                       │
            │                       └─→ 后台线程执行 Spill
            │                               │
            │                               ├─→ 循环读取 sorted_blocks
            │                               │       └─→ FullSorter::merge_sort_read_for_spill()
            │                               │
            │                               └─→ 写入 SpillStream1
            │                                       ├─ 序列化（PBlock）
            │                                       ├─ 压缩（ZSTD）
            │                                       └─ 写入磁盘文件1
            │
            └─→ 继续接收数据块 Block3, Block4...
                    └─→ 如果再次内存不足，创建 SpillStream2...

【EOS 处理】
    │
    ├─→ 如果已 Spill
    │       │
    │       ├─→ 检查是否还有可回收内存
    │       │       │
    │       │       ├─→ 如果有：继续 Spill 剩余数据到 SpillStreamN
    │       │       │
    │       │       └─→ 如果没有：通知下游可以读取
    │       │
    │       └─→ sorted_streams = [SpillStream1, SpillStream2, ..., SpillStreamN]
    │
    └─→ 如果未 Spill
            └─→ 直接完成排序，通知下游读取

【Source 阶段：数据读取和合并】

SpillSortSourceOperatorX::get_block()
    │
    ├─→ 检查 is_spilled
    │       │
    │       ├─→ false：直接从内存读取
    │       │       └─→ SortSourceOperatorX::get_block()
    │       │               └─→ FullSorter::get_next()
    │       │
    │       └─→ true：从磁盘读取并归并
    │               │
    │               ├─→ 如果 merger 未初始化
    │               │       │
    │               │       └─→ initiate_merge_sort_spill_streams()
    │               │               │
    │               │               ├─→ 多轮归并循环
    │               │               │       │
    │               │               │       ├─→ 第1轮：归并 streams 1-16 → 临时 stream A
    │               │               │       ├─→ 第2轮：归并 streams 17-32 → 临时 stream B
    │               │               │       ├─→ ...
    │               │               │       └─→ 最后一轮：归并所有剩余 streams → 最终结果
    │               │               │
    │               │               └─→ 初始化最终 merger
    │               │                       ├─→ 创建 BlockSupplier（从 streams 读取）
    │               │                       └─→ VSortedRunMerger::prepare()
    │               │
    │               └─→ VSortedRunMerger::get_next()
    │                       │
    │                       ├─→ 从优先队列取出最小值
    │                       │       └─→ 队列顶部是最小的数据块
    │                       │
    │                       ├─→ SpillStream::read_next_block_sync()
    │                       │       ├─→ 从磁盘读取
    │                       │       ├─→ 解压缩（ZSTD）
    │                       │       └─→ 反序列化
    │                       │
    │                       └─→ 写入输出 Block
    │                               └─→ 返回给下游算子
```

### 2.2 关键设计

1. **延迟 Spill**：只有在内存真正不足时才 Spill
2. **分批 Spill**：每次 Spill 一定大小的数据，而不是一次性全部 Spill
3. **多路归并**：读取时使用多路归并排序，合并多个已排序的文件
4. **异步处理**：Spill 操作在后台线程执行，不阻塞主流程

---

## 3. 核心组件

### 3.1 主要类

| 类名 | 职责 |
|------|------|
| `SpillSortSinkOperatorX` | 排序 Sink 算子，支持 Spill |
| `SpillSortSourceOperatorX` | 排序 Source 算子，支持从磁盘读取 |
| `FullSorter` | 全排序器，执行实际排序 |
| `SpillStream` | Spill 流，管理磁盘文件的读写 |
| `SpillStreamManager` | Spill 流管理器，管理所有 Spill 流 |
| `VSortedRunMerger` | 多路归并器，合并多个已排序流 |

### 3.2 数据结构

```cpp
// 共享状态：在 Sink 和 Source 之间共享
struct SpillSortSharedState {
    bool is_spilled = false;                    // 是否已 Spill
    std::list<SpillStreamSPtr> sorted_streams; // 已排序的 Spill 流列表
    SortSharedState* in_mem_shared_state;      // 内存中的排序状态
    int64_t limit;                              // LIMIT 值
    int64_t offset;                             // OFFSET 值
    size_t spill_block_batch_row_count;       // Spill 批次行数
};
```

---

## 4. 详细流程

### 4.1 阶段 1：数据收集和排序（Sink 阶段）

#### 4.1.1 正常流程（内存充足）

```
输入数据块 → SpillSortSinkOperatorX::sink()
  │
  ├─ 调用 SortSinkOperatorX::sink()
  │   └─ FullSorter::append_block()
  │       ├─ 检查是否达到阈值（_reach_limit()）
  │       │   └─ unsorted_block 大小 >= _max_buffered_block_bytes (64MB)
  │       │
  │       ├─ 如果达到阈值且容量不足
  │       │   └─ _do_sort()：执行排序
  │       │       ├─ 对 unsorted_block 进行排序
  │       │       └─ 将排序后的数据添加到 sorted_blocks
  │       │
  │       └─ 将新数据追加到 unsorted_block
  │           └─ 继续累积数据
  │
  └─ 更新内存统计
      └─ 继续接收下一个数据块
```

**关键阈值**：
- `_max_buffered_block_bytes`：默认 64MB，控制何时触发排序
- 当 `unsorted_block` 达到此大小时，会执行排序并清空 `unsorted_block`

**代码示例**：
```cpp
// spill_sort_sink_operator.cpp:143-192
Status SpillSortSinkOperatorX::sink(RuntimeState* state, Block* in_block, bool eos) {
    // 1. 接收数据块
    if (in_block->rows() > 0) {
        // 2. 调用内部 SortSinkOperatorX 进行排序
        RETURN_IF_ERROR(_sort_sink_operator->sink(_runtime_state.get(), in_block, false));
        // 3. 更新内存统计
        int64_t data_size = _shared_state->in_mem_shared_state->sorter->data_size();
        COUNTER_SET(_memory_used_counter, data_size);
    }
    
    // 4. 数据流结束时
    if (eos) {
        if (!_shared_state->is_spilled) {
            // 内存充足，直接完成排序
            RETURN_IF_ERROR(_shared_state->in_mem_shared_state->sorter->prepare_for_read(false));
            _dependency->set_ready_to_read();
        }
    }
}
```

#### 4.1.2 Spill 触发流程（内存不足）

**触发条件**：
1. 查询内存使用超过限制
2. 操作符有可回收内存（`revocable_mem_size()` > `MIN_SPILL_WRITE_BATCH_MEM`）
3. 操作符支持 Spill（`is_spillable()` = true）

**触发调用链**：
```
PipelineTask::_try_to_reserve_memory()
  │
  ├─ 内存分配失败
  │
  └─ PipelineTask::revoke_memory()
      └─ SpillSortSinkOperatorX::revoke_memory()
          └─ SpillSortSinkLocalState::revoke_memory()
```

**详细流程**：

```
步骤 1: 检测内存不足
  │
  ├─ PipelineTask 尝试分配内存失败
  ├─ 检查是否有可回收内存
  │   └─ revocable_mem_size() >= MIN_SPILL_WRITE_BATCH_MEM (32KB)
  │
  └─ 触发 revoke_memory()

步骤 2: 注册 SpillStream
  │
  ├─ 创建 SpillStream
  ├─ 注册到 SpillStreamManager
  │   ├─ 选择可用磁盘（优先 SSD）
  │   ├─ 创建 spill 目录
  │   └─ 创建 SpillWriter 和 SpillReader
  │
  └─ 将 SpillStream 添加到 sorted_streams 列表

步骤 3: 执行 Spill（在后台线程）
  │
  ├─ 准备排序器供读取
  │   └─ FullSorter::prepare_for_read(true)
  │       └─ 完成排序，准备输出
  │
  ├─ 循环读取已排序的数据块
  │   ├─ FullSorter::merge_sort_read_for_spill()
  │   │   └─ 从 sorted_blocks 读取数据
  │   │
  │   └─ SpillStream::spill_block()
  │       ├─ 序列化数据块
  │       ├─ 压缩（ZSTD）
  │       └─ 写入磁盘文件
  │
  └─ 标记 Spill 完成
      └─ 释放内存
```

**代码示例**：
```cpp
// spill_sort_sink_operator.cpp:250-329
Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state, ...) {
    // 1. 标记已 Spill
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        _shared_state->limit = parent._sort_sink_operator->limit();
        _shared_state->offset = parent._sort_sink_operator->offset();
    }
    
    // 2. 注册 SpillStream
    auto status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
        state, _spilling_stream, query_id, "sort", node_id,
        batch_size, state->spill_sort_batch_bytes(), profile);
    RETURN_IF_ERROR(status);
    
    _shared_state->sorted_streams.emplace_back(_spilling_stream);
    
    // 3. 在后台线程执行 Spill
    auto spill_func = [this, state, query_id, &parent] {
        // 3.1 准备排序器供读取
        status = parent._sort_sink_operator->prepare_for_spill(_runtime_state.get());
        
        // 3.2 循环读取并写入磁盘
        bool eos = false;
        Block block;
        while (!eos && !state->is_cancelled()) {
            // 从排序器读取已排序的数据块
            status = parent._sort_sink_operator->merge_sort_read_for_spill(
                _runtime_state.get(), &block, batch_size, &eos);
            RETURN_IF_ERROR(status);
            
            // 写入 SpillStream（序列化、压缩、写入磁盘）
            status = _spilling_stream->spill_block(state, block, eos);
            RETURN_IF_ERROR(status);
            
            block.clear_column_data();
        }
    };
    
    // 提交到线程池执行
    ExecEnv::GetInstance()->spill_stream_mgr()->submit_spill_task(spill_func);
}
```

### 4.2 阶段 2：数据读取和合并（Source 阶段）

#### 4.2.1 未 Spill 的情况（全内存）

```
SpillSortSourceOperatorX::get_block()
  │
  └─ 调用 SortSourceOperatorX::get_block()
      └─ FullSorter::get_next()
          └─ 从内存中的 sorted_blocks 读取数据
```

#### 4.2.2 已 Spill 的情况（磁盘读取）

**多轮归并策略**：

由于可能有多个 SpillStream（例如 100 个），如果一次性全部归并，会占用大量内存。因此采用**多轮归并**策略：

1. **第一轮**：每次归并 N 个 streams（N = `spill_sort_mem_limit / spill_sort_batch_bytes`）
2. **中间轮**：将归并结果写入新的 stream，继续归并下一批
3. **最后一轮**：归并所有剩余的 streams，得到最终结果

**初始化阶段**：
```
SpillSortSourceOperatorX::get_block()
  │
  ├─ 检查 is_spilled
  │
  └─ 如果 merger 未初始化
      └─ initiate_merge_sort_spill_streams()
          │
          ├─ 循环：直到所有 streams 归并完成
          │   │
          │   ├─ 计算本次归并的 stream 数量
          │   │   └─ max_stream_count = spill_sort_mem_limit / spill_sort_batch_bytes
          │   │       └─ 例如：128MB / 8MB = 16 个 streams
          │   │
          │   ├─ _create_intermediate_merger()
          │   │   ├─ 从 sorted_streams 取出前 N 个
          │   │   ├─ 为每个创建 BlockSupplier
          │   │   └─ 创建 VSortedRunMerger
          │   │
          │   ├─ 创建临时 SpillStream（存储归并结果）
          │   │
          │   ├─ 循环读取归并结果
          │   │   ├─ _merger->get_next()：从多路归并器读取
          │   │   └─ tmp_stream->spill_block()：写入临时 stream
          │   │
          │   └─ 将临时 stream 加入 sorted_streams
          │       └─ 删除已归并的 streams
          │
          └─ 最后一轮：归并所有剩余 streams
              └─ 不写入新 stream，直接供读取
```

**读取阶段**：
```
SpillSortSourceOperatorX::get_block()
  │
  └─ VSortedRunMerger::get_next()
      │
      ├─ 从优先队列取出最小值
      │   └─ 队列顶部是最小的数据块
      │
      ├─ 读取该块的数据
      │   └─ SpillStream::read_next_block_sync()
      │       ├─ 从磁盘读取
      │       ├─ 解压缩（ZSTD）
      │       └─ 反序列化
      │
      ├─ 将数据写入输出 Block
      │
      └─ 如果该块未读完，重新加入队列
          └─ 队列自动调整，下一个最小值移到顶部
```

**多轮归并示例**：

假设有 50 个 SpillStream，每次归并 16 个：

```
初始：50 个 streams

第1轮：
  - 归并 streams 1-16 → 临时 stream A
  - 归并 streams 17-32 → 临时 stream B
  - 归并 streams 33-48 → 临时 stream C
  - streams 49-50 保留
  - 结果：4 个 streams（A, B, C, 49, 50）

第2轮（最后一轮）：
  - 归并 A, B, C, 49, 50 → 最终结果
  - 直接供读取，不写入新 stream
```

**代码示例**：
```cpp
// spill_sort_source_operator.cpp:304-364
Status SpillSortSourceOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    
    if (local_state._shared_state->is_spilled) {
        // 情况 1: 初始化多路归并器
        if (!local_state._merger) {
            RETURN_IF_ERROR(local_state.initiate_merge_sort_spill_streams(state));
            return Status::OK();
        }
        
        // 情况 2: 从多路归并器读取
        status = local_state._merger->get_next(block, eos);
    } else {
        // 情况 3: 从内存读取
        status = _sort_source_operator->get_block(
            local_state._runtime_state.get(), block, eos);
    }
    
    // 处理 LIMIT
    local_state.reached_limit(block, eos);
    return Status::OK();
}
```

---

## 5. 代码实现分析

### 5.1 内存检测和触发

**位置**：`be/src/pipeline/pipeline_task.cpp:782-823`

**完整调用链**：
```
PipelineTask::execute()
  └─→ Operator::open() / Operator::get_block()
      └─→ PipelineTask::_try_to_reserve_memory()
          │
          ├─→ 内存分配成功 → 继续执行
          │
          └─→ 内存分配失败
              │
              ├─→ 检查是否有可回收内存
              │       └─→ revocable_mem_size() >= 32KB
              │
              └─→ 触发 Spill
                  └─→ PipelineTask::revoke_memory()
                      └─→ SpillSortSinkOperatorX::revoke_memory()
```

**关键代码**：
```cpp
// pipeline_task.cpp:782-823
bool PipelineTask::_try_to_reserve_memory(size_t reserve_size, OperatorBase* op) {
    // 1. 尝试预留内存
    // try_reserve 会检查查询级别的内存限制
    // 如果查询内存使用超过限制，返回错误
    auto st = thread_context()->thread_mem_tracker_mgr->try_reserve(reserve_size);
    
    // 2. 检查是否有可回收内存
    // revocable_mem_size 返回可以 spill 到磁盘的内存大小
    // 对于 SortSink，返回 sorter->data_size()
    auto sink_revocable_mem_size = _sink->revocable_mem_size(_state);
    
    // 3. 如果预留失败且有可回收内存，触发 Spill
    if (!st.ok()) {
        // MIN_SPILL_WRITE_BATCH_MEM = 32KB
        // 只有当可回收内存 >= 32KB 时才触发 Spill
        // 这样可以避免频繁创建小文件，提高性能
        if (sink_revocable_mem_size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            // 标记正在 Spill
            _spilling = true;
            
            // 将查询添加到暂停列表，等待 Spill 完成
            ExecEnv::GetInstance()->workload_group_mgr()->add_paused_query(
                _state->get_query_ctx()->resource_ctx()->shared_from_this(),
                reserve_size, st);
            
            return false;  // 内存分配失败，需要 Spill
        } else {
            // 可回收内存太小，不触发 Spill
            // 设置低内存模式，限制内存使用
            _state->get_query_ctx()->set_low_memory_mode();
        }
    }
    
    return true;
}
```

**关键点**：
- `try_reserve()` 会检查查询内存限制（`query_mem_limit`）
- 如果失败，检查是否有可回收内存（`revocable_mem_size()`）
- 只有当可回收内存 >= 32KB 时才触发 Spill（避免频繁小文件）
- Spill 操作在后台线程执行，查询会被暂停，等待 Spill 完成

### 5.2 Spill 执行流程

**位置**：`be/src/pipeline/exec/spill_sort_sink_operator.cpp:250-329`

**完整代码**：
```cpp
Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state, ...) {
    auto& parent = Base::_parent->template cast<Parent>();
    
    // ===== 步骤 1：标记已 Spill =====
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        _shared_state->limit = parent._sort_sink_operator->limit();
        _shared_state->offset = parent._sort_sink_operator->offset();
        custom_profile()->add_info_string("Spilled", "true");
    }
    
    // ===== 步骤 2：注册 SpillStream =====
    // 计算批次大小（每次 Spill 的行数）
    int32_t batch_size = _shared_state->spill_block_batch_row_count > INT32_MAX
                         ? INT32_MAX
                         : static_cast<int32_t>(_shared_state->spill_block_batch_row_count);
    
    // 注册 SpillStream
    // SpillStreamManager 会：
    // - 选择可用磁盘（优先 SSD，排除已满的磁盘）
    // - 创建 spill 目录：storage_root/spill/query_id/sort-node_id-task_id-stream_id
    // - 创建 SpillWriter（用于写入）和 SpillReader（用于读取）
    auto status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
        state, _spilling_stream, print_id(state->query_id()), "sort",
        _parent->node_id(), batch_size, state->spill_sort_batch_bytes(),
        operator_profile());
    RETURN_IF_ERROR(status);
    
    // 将 SpillStream 添加到 sorted_streams 列表
    // 后续 Source 阶段会从这个列表读取数据
    _shared_state->sorted_streams.emplace_back(_spilling_stream);
    
    // ===== 步骤 3：在后台线程执行 Spill =====
    auto spill_func = [this, state, query_id, &parent] {
        // 3.1 准备排序器供读取
        // prepare_for_spill 内部调用 prepare_for_read(true)
        // 这会完成所有未排序数据的排序，并准备输出
        status = parent._sort_sink_operator->prepare_for_spill(_runtime_state.get());
        RETURN_IF_ERROR(status);
        
        // 3.2 循环读取已排序的数据并写入磁盘
        bool eos = false;
        Block block;
        while (!eos && !state->is_cancelled()) {
            // 从排序器读取已排序的数据块
            // merge_sort_read_for_spill 会从 sorted_blocks 读取数据
            // batch_size 控制每次读取的行数
            status = parent._sort_sink_operator->merge_sort_read_for_spill(
                _runtime_state.get(), &block, batch_size, &eos);
            RETURN_IF_ERROR(status);
            
            // 写入 SpillStream
            // spill_block 内部会：
            // 1. 序列化 Block（转换为 PBlock 格式）
            // 2. 压缩（使用 ZSTD 压缩算法）
            // 3. 写入磁盘文件（追加模式）
            status = _spilling_stream->spill_block(state, block, eos);
            RETURN_IF_ERROR(status);
            
            // 清空 Block，释放内存
            block.clear_column_data();
        }
        
        return Status::OK();
    };
    
    // 提交到线程池执行（异步）
    // Spill 操作在后台线程执行，不阻塞主流程
    ExecEnv::GetInstance()->spill_stream_mgr()->submit_spill_task(spill_func);
    
    // Spill 完成后（在 defer 中）
    if (_eos) {
        _dependency->set_ready_to_read();  // 通知下游可以读取
    }
    
    return Status::OK();
}
```

**关键点**：
1. **异步执行**：Spill 操作在后台线程执行，不阻塞主流程
2. **分批写入**：每次写入 `batch_size` 行数据，避免一次性写入过多
3. **数据压缩**：使用 ZSTD 压缩，减少磁盘占用
4. **错误处理**：如果 Spill 失败，会清理资源并返回错误

### 5.3 多路归并读取

**位置**：`be/src/pipeline/exec/spill_sort_source_operator.cpp:75-212`

**完整代码**：

#### 5.3.1 多轮归并初始化

```cpp
Status SpillSortLocalState::initiate_merge_sort_spill_streams(RuntimeState* state) {
    // 在后台线程执行多轮归并
    auto spill_func = [this, state, query_id, &parent] {
        Block merge_sorted_block;
        SpillStreamSPtr tmp_stream;
        
        // ===== 多轮归并循环 =====
        while (!state->is_cancelled()) {
            // 1. 计算本次归并的 stream 数量
            // max_stream_count = spill_sort_mem_limit / spill_sort_batch_bytes
            // 例如：128MB / 8MB = 16
            // 这样可以控制同时从磁盘读取的块数量，避免内存占用过高
            int max_stream_count = _calc_spill_blocks_to_merge(state);
            
            // 2. 创建中间归并器
            // 从 sorted_streams 取出前 max_stream_count 个 streams
            RETURN_IF_ERROR(_create_intermediate_merger(max_stream_count, sort_description));
            
            // 3. 如果所有 streams 都已归并，退出循环
            if (_shared_state->sorted_streams.empty()) {
                return Status::OK();  // 最后一轮归并完成
            }
            
            // 4. 创建临时 SpillStream（存储归并结果）
            RETURN_IF_ERROR(spill_stream_mgr()->register_spill_stream(
                state, tmp_stream, query_id, "sort", node_id,
                batch_size, state->spill_sort_batch_bytes(), profile));
            
            // 5. 循环读取归并结果并写入临时 stream
            bool eos = false;
            while (!eos && !state->is_cancelled()) {
                merge_sorted_block.clear_column_data();
                
                // 从多路归并器读取下一个有序的数据块
                status = _merger->get_next(&merge_sorted_block, &eos);
                RETURN_IF_ERROR(status);
                
                // 写入临时 stream
                status = tmp_stream->spill_block(state, merge_sorted_block, eos);
                RETURN_IF_ERROR(status);
            }
            
            // 6. 将临时 stream 加入 sorted_streams
            // 删除已归并的 streams（释放磁盘文件）
            _shared_state->sorted_streams.emplace_back(tmp_stream);
            for (auto& stream : _current_merging_streams) {
                spill_stream_mgr()->delete_spill_stream(stream);
            }
            _current_merging_streams.clear();
        }
        
        return Status::OK();
    };
    
    // 提交到线程池执行
    return SpillRecoverRunnable(state, operator_profile(), spill_func).run();
}
```

#### 5.3.2 创建中间归并器

```cpp
Status SpillSortLocalState::_create_intermediate_merger(
        int num_blocks, const SortDescription& sort_description) {
    std::vector<BlockSupplier> child_block_suppliers;
    
    // 确定 LIMIT 和 OFFSET
    // 如果是最后一轮归并，使用真实的 LIMIT 和 OFFSET
    // 否则不使用 LIMIT（归并所有数据）
    int64_t limit = -1;
    int64_t offset = 0;
    if (num_blocks >= _shared_state->sorted_streams.size()) {
        // 最后一轮，使用真实的 LIMIT 和 OFFSET
        limit = _shared_state->limit;
        offset = _shared_state->offset;
    }
    
    // 创建多路归并器
    _merger = std::make_unique<VSortedRunMerger>(
        sort_description, batch_size, limit, offset, profile);
    
    // 从 sorted_streams 取出前 num_blocks 个
    _current_merging_streams.clear();
    for (int i = 0; i < num_blocks && !_shared_state->sorted_streams.empty(); ++i) {
        auto stream = _shared_state->sorted_streams.front();
        _shared_state->sorted_streams.pop_front();
        
        // 设置读取计数器（用于性能监控）
        stream->set_read_counters(operator_profile());
        _current_merging_streams.emplace_back(stream);
        
        // 为每个 stream 创建 BlockSupplier
        // BlockSupplier 是一个函数对象，用于从 SpillStream 读取数据块
        child_block_suppliers.emplace_back([stream](Block* block, bool* eos) {
            return stream->read_next_block_sync(block, eos);
        });
    }
    
    // 准备归并器，传入所有 BlockSupplier
    // prepare 内部会：
    // 1. 为每个 BlockSupplier 创建一个 MergeSortCursor
    // 2. 初始化优先队列（最小堆）
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    
    return Status::OK();
}
```

#### 5.3.3 读取数据

```cpp
// spill_sort_source_operator.cpp:347
Status SpillSortSourceOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    
    if (local_state._shared_state->is_spilled) {
        if (!local_state._merger) {
            // 初始化多轮归并（在后台线程执行）
            RETURN_IF_ERROR(local_state.initiate_merge_sort_spill_streams(state));
            return Status::OK();
        } else {
            // 从多路归并器读取下一个有序的数据块
            // get_next 内部会：
            // 1. 从优先队列取出最小值（队列顶部）
            // 2. 调用对应的 BlockSupplier 读取数据
            //    └─→ SpillStream::read_next_block_sync()
            //        ├─→ 从磁盘读取
            //        ├─→ 解压缩（ZSTD）
            //        └─→ 反序列化
            // 3. 将数据写入输出 Block
            // 4. 如果该块未读完，重新加入队列
            // 5. 队列自动调整，下一个最小值移到顶部
            status = local_state._merger->get_next(block, eos);
            RETURN_IF_ERROR(status);
        }
    } else {
        // 未 Spill，直接从内存读取
        status = _sort_source_operator->get_block(
            local_state._runtime_state.get(), block, eos);
        RETURN_IF_ERROR(status);
    }
    
    // 处理 LIMIT
    local_state.reached_limit(block, eos);
    return Status::OK();
}
```

---

## 6. 完整示例

### 9.1 场景：大表排序查询

**SQL**：
```sql
SET enable_spill = true;
SELECT * FROM large_table ORDER BY id LIMIT 1000;
```

**执行流程**：

```
1. FE 规划查询计划
   └─ 选择 FULL_SORT 算法（因为数据量大）

2. BE 执行查询
   │
   ├─ Pipeline1: SpillSortSinkOperatorX
   │   ├─ 接收数据块 1-100（内存充足，正常排序）
   │   ├─ 接收数据块 101-200（内存不足，触发 Spill）
   │   │   ├─ 将已排序的数据写入 spill_file_1
   │   │   └─ 释放内存
   │   ├─ 接收数据块 201-300（继续排序）
   │   ├─ 接收数据块 301-400（再次 Spill）
   │   │   ├─ 将已排序的数据写入 spill_file_2
   │   │   └─ 释放内存
   │   └─ EOS：继续 Spill 剩余数据到 spill_file_3
   │
   └─ Pipeline2: SpillSortSourceOperatorX
       ├─ 初始化多路归并器
       │   ├─ 创建 3 个 BlockSupplier（对应 3 个 spill_file）
       │   └─ 初始化优先队列
       │
       └─ 循环读取
           ├─ 从优先队列取出最小值（来自 spill_file_1）
           ├─ 读取该块的数据
           ├─ 写入输出 Block
           └─ 重复直到所有文件读完
```

### 9.2 代码调用链

**Sink 阶段**：
```
PipelineTask::execute()
  └─ PipelineTask::_try_to_reserve_memory()  // 内存分配失败
      └─ PipelineTask::revoke_memory()
          └─ SpillSortSinkOperatorX::revoke_memory()
              └─ SpillSortSinkLocalState::revoke_memory()
                  ├─ SpillStreamManager::register_spill_stream()
                  ├─ FullSorter::prepare_for_read(true)
                  └─ FullSorter::merge_sort_read_for_spill()
                      └─ SpillStream::spill_block()
                          ├─ 序列化 Block
                          ├─ 压缩（ZSTD）
                          └─ 写入磁盘文件
```

**Source 阶段**：
```
PipelineTask::execute()
  └─ SpillSortSourceOperatorX::get_block()
      └─ SpillSortLocalState::initiate_merge_sort_spill_streams()
          ├─ 创建 BlockSupplier（从 SpillStream 读取）
          └─ VSortedRunMerger::prepare()
              └─ 初始化优先队列
      └─ VSortedRunMerger::get_next()
          ├─ 从优先队列取出最小值
          └─ SpillStream::read_next_block_sync()
              ├─ 从磁盘读取
              ├─ 解压缩
              └─ 反序列化
```

---

## 7. 总结

### 10.1 核心要点

1. **Spill 是延迟触发的**：只有在内存真正不足时才 Spill
2. **分批处理**：每次 Spill 一定大小的数据，而不是一次性全部 Spill
3. **多路归并**：读取时使用多路归并排序，合并多个已排序的文件
4. **异步执行**：Spill 操作在后台线程执行，不阻塞主流程

### 10.2 适用场景

✅ **适合使用 Spill**：
- 大表排序查询
- 内存有限的环境
- 需要处理任意大小数据的场景

❌ **不适合使用 Spill**：
- 小表查询（增加不必要的开销）
- 内存充足的环境（全内存排序更快）
- 对延迟敏感的场景（Spill 会增加延迟）

### 10.3 最佳实践

1. **合理配置参数**：根据硬件环境调整 Spill 参数
2. **监控性能**：关注 Spill 相关的 Profile 指标
3. **优化查询**：尽量使用 LIMIT 减少排序数据量
4. **使用 SSD**：如果可能，使用 SSD 作为 Spill 存储

---

## 附录：相关代码文件

- `be/src/pipeline/exec/spill_sort_sink_operator.cpp` - Spill Sort Sink 实现
- `be/src/pipeline/exec/spill_sort_source_operator.cpp` - Spill Sort Source 实现
- `be/src/vec/common/sort/sorter.cpp` - 排序器基类实现
- `be/src/vec/spill/spill_stream.cpp` - Spill 流实现
- `be/src/vec/spill/spill_stream_manager.cpp` - Spill 流管理器
- `be/src/pipeline/pipeline_task.cpp` - Pipeline Task 内存管理

---

**文档版本**：v1.0  
**最后更新**：2024年

