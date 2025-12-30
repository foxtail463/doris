# Stream Load 调用链条详解

## 概述

Stream Load 是 Apache Doris 中用于实时数据导入的核心功能，支持通过 HTTP 协议将数据流式导入到 Doris 表中。本文档详细描述了 Stream Load 从 FE（Frontend）到 BE（Backend）的完整调用链条，包括算子树结构、数据流动过程和部分更新等特殊场景。

**重要说明**：本文档假设使用 **V2 版本**（`OlapTableSinkV2OperatorX` + `VTabletWriterV2` + `DeltaWriterV2`）。V2 版本使用流式传输（LoadStreamStub），直接在本地写入 MemTable，不再通过 RPC 发送到目标 BE，性能更好。如果未启用 `enable_memtable_on_sink_node` 或满足其他条件，系统会使用旧版本（`OlapTableSinkOperatorX` + `VTabletWriter` + `DeltaWriter`），旧版本使用传统 RPC 方式。

## 目录

1. [整体架构](#整体架构)
2. [算子树结构](#算子树结构)
3. [完整调用链](#完整调用链)
4. [部分更新场景](#部分更新场景)
5. [关键函数调用位置](#关键函数调用位置)

---

## 整体架构

Stream Load 的整体架构包括：

- **HTTP 服务器层**：接收 HTTP 请求，处理数据流
- **FE 端**：解析请求参数，生成执行计划
- **BE 端**：执行计划，处理数据，写入存储

```
┌─────────────────────────────────────────────────────────────┐
│                      FE (Frontend)                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  StreamLoadHandler / NereidsStreamLoadPlanner        │   │
│  │  - 解析请求参数                                        │   │
│  │  - 生成执行计划                                        │   │
│  │  - 支持部分更新模式                                     │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Thrift RPC
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      BE (Backend)                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  StreamLoadExecutor / Pipeline 执行引擎               │   │ 
│  │  - 接收数据（StreamLoadPipe）                          │   │
│  │  - 处理数据（FileScanOperator）                        │   │
│  │  - 写入数据（OlapTableSinkV2Operator）                 │   │
│  │  - 写入 MemTable → Segment（使用 DeltaWriterV2）      │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 算子树结构

Stream Load 的算子树采用 **Pull-based 执行模型**，数据从 Source（FileScanOperator）开始，经过中间处理算子，最终到达 Sink（OlapTableSinkV2Operator，使用 V2 版本）。

### 数据流架构

```
HTTP 请求数据
    ↓
StreamLoadPipe (内存缓冲区)
    ↓
FileScanOperatorX (Source)
    ↓
[中间处理算子] (可选)
    ↓
OlapTableSinkV2OperatorX (Sink)
    ↓
VTabletWriterV2 → DeltaWriterV2 → MemTable → 磁盘
```

### FE 端逻辑计划树（Nereids）

在 FE 端，Stream Load 通过 `NereidsStreamLoadPlanner.plan()` 生成执行计划。

#### 逻辑计划构建流程

逻辑计划树从下往上构建（从数据源到目标表）：

```
LogicalOneRowRelation
    ↓ (如果有前置过滤)
LogicalPreFilter
    ↓ (类型转换)
LogicalProject (Cast Project)
    ↓ (列映射表达式)
LogicalLoadProject
    ↓ (目标表写入)
LogicalTableSink (UnboundTableSink)
```

#### 各节点说明

**LogicalOneRowRelation**
- **作用**：数据源占位符，表示从外部文件读取的数据
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadUtils.java:136`
- **说明**：包含 `context.scanSlots`，表示从文件中读取的列（通常是 VARCHAR 类型）

**LogicalPreFilter（可选）**
- **作用**：前置过滤，在类型转换之前进行过滤
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadUtils.java:140`
- **说明**：如果 `fileGroup.getPrecedingFilterExpr()` 不为空，则添加此节点

**LogicalProject (Cast Project)**
- **作用**：类型转换，将 VARCHAR 类型的扫描列转换为目标表的正确数据类型
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadUtils.java:187`
- **说明**：
  - 对于有对应表列的扫描列，进行类型转换
  - 对于临时列（没有对应表列），保持原样

**LogicalLoadProject**
- **作用**：列映射表达式计算
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadUtils.java:191`
- **说明**：
  - 处理 `columns` 参数中的表达式（如 `tmp_c1, tmp_c2, tmp_c3 => id, name, age`）
  - 计算函数表达式（如 `upper(name)`, `cast(id as int)` 等）

**LogicalTableSink (UnboundTableSink)**
- **作用**：目标表写入节点
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadUtils.java:195`
- **说明**：
  - 指定目标表、分区信息
  - 包含部分更新相关配置（`isPartialUpdate`, `partialUpdateNewKeyPolicy`）

### FE 端物理计划

逻辑计划经过优化和物理化后，生成物理计划：

```
FileLoadScanNode
    ↓
OlapTableSink
```

**FileLoadScanNode**
- **类型**：`TPlanNodeType::FILE_SCAN_NODE`
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsStreamLoadPlanner.java:267`
- **作用**：
  - 扫描外部文件（对于 Stream Load，文件类型为 `FILE_STREAM`）
  - 解析文件格式（CSV、JSON 等）
  - 处理压缩（GZIP、LZ4 等）

**OlapTableSink**
- **类型**：`TDataSinkType::OLAP_TABLE_SINK`
- **位置**：`fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsStreamLoadPlanner.java:275`
- **作用**：
  - 将数据写入 OLAP 表
  - 处理数据分区和路由
- **注意**：如果启用了 `enable_memtable_on_sink_node` 且满足条件，FE 会选择使用 `OlapTableSinkV2OperatorX`（V2 版本），否则使用 `OlapTableSinkOperatorX`（旧版本）。本文档假设使用 V2 版本。
  - 支持部分更新模式

### BE 端 Pipeline 算子

BE 端将物理计划转换为 Pipeline 算子，执行实际的数据处理。

#### Pipeline 结构

```
PipelineTask
    │
    ├─> FileScanOperatorX (Source)
    │   │
    │   └─> 从 StreamLoadPipe 读取数据
    │       - 解析 CSV/JSON 格式
    │       - 类型转换
    │       - 列映射
    │
    └─> OlapTableSinkOperatorX (Sink)
        │
        └─> 写入 OLAP 表
            - 数据分区和路由
            - 写入 MemTable
```

#### FileScanOperatorX

- **类型**：`TPlanNodeType::FILE_SCAN_NODE`
- **位置**：`be/src/pipeline/exec/file_scan_operator.h`
- **基类**：`ScanOperatorX<FileScanLocalState>`
- **数据源**：
  - **流式模式**：`StreamLoadPipe`（内存缓冲区）
    - 位置：`be/src/io/fs/stream_load_pipe.h`
    - 数据通过 HTTP `on_chunk_data` 回调写入
    - Pipeline 通过 `FileFactory::create_pipe_reader()` 获取
  - **非流式模式**：本地临时文件
    - 位置：`be/src/http/action/stream_load.cpp:522`
    - 数据先全部写入文件，再读取

**数据读取流程**：
```
FileScanOperatorX::get_block()
    ↓
ScanOperatorX::get_block()
    ↓
ScannerContext::get_block_from_queue()
    ↓
FileScanner::get_next()
    ↓
StreamLoadPipe::read() / LocalFileReader::read()
```

#### OlapTableSinkV2OperatorX（V2 版本）

- **类型**：`TDataSinkType::OLAP_TABLE_SINK`
- **位置**：`be/src/pipeline/exec/olap_table_sink_v2_operator.h`
- **基类**：`DataSinkOperatorX<OlapTableSinkV2LocalState>`
- **本地状态**：`OlapTableSinkV2LocalState`（继承自 `AsyncWriterSink<VTabletWriterV2, OlapTableSinkV2OperatorX>`）
- **使用条件**：
  - `enable_memtable_on_sink_node = true`
  - 没有倒排索引 v1 或部分更新
  - 不是云模式

**数据写入流程（V2 版本，使用流式传输）**：
```
OlapTableSinkV2OperatorX::sink()
    ↓
OlapTableSinkV2LocalState::sink()
    ↓
AsyncWriterSink::sink()
    ↓
VTabletWriterV2::sink() (异步队列)
    ↓
VTabletWriterV2::process_block() (后台线程)
    ↓
VTabletWriterV2::write()
    ↓
DeltaWriterV2Pool::get_or_create() (获取或创建 DeltaWriterV2)
    ↓
DeltaWriterV2::write()
    ↓
MemTableWriter::write()
    ↓
MemTable::insert()
```

**注意**：V2 版本使用流式传输（LoadStreamStub），不再通过 RPC 发送到目标 BE，而是在本地直接写入 MemTable。

### Pull-based 执行模型

Pipeline 采用 Pull-based 执行模型，数据从下游向上游拉取：

```
PipelineTask::execute()
    ↓
_root->get_block_after_projects()
    ↓
FileScanOperatorX::get_block()
    ↓
从 StreamLoadPipe 读取数据
    ↓
返回 Block 给 PipelineTask
    ↓
_sink->sink()
    ↓
OlapTableSinkV2OperatorX::sink()
    ↓
写入异步队列（VTabletWriterV2）
```

### StreamLoadPipe 数据流

```
HTTP on_chunk_data()
    ↓
StreamLoadPipe::append()
    ↓
[数据在内存缓冲区中]
    ↓
FileScanOperatorX::get_block()
    ↓
StreamLoadPipe::read()
    ↓
返回数据给 FileScanOperatorX
```

---

## HTTP 运行流程详解

本节详细说明 Stream Load 的 HTTP 请求处理流程，包括 libevent 的回调机制和各个回调函数的调用时机。

### HTTP 请求生命周期

Stream Load 使用 **libevent** 作为 HTTP 服务器框架。libevent 采用事件驱动模型，通过回调函数处理 HTTP 请求的不同阶段。

```
HTTP 客户端发送请求
    ↓
┌─────────────────────────────────────────────────────────┐
│ 1. 连接建立阶段                                          │
│    libevent 检测到新的 TCP 连接                         │
│    ↓                                                      │
│    on_connection() 回调                                  │
│    [be/src/http/ev_http_server.cpp:77]                  │
│    - 设置 on_header 回调函数                             │
│    - 准备接收 HTTP header                                │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Header 解析阶段                                       │
│    libevent 解析完所有 HTTP headers                      │
│    ↓                                                      │
│    on_header() 回调                                      │
│    [be/src/http/ev_http_server.cpp:240]                 │
│    - 创建 HttpRequest 对象                               │
│    - 查找对应的 Handler（StreamLoadAction）              │
│    - 调用 Handler::on_header()                          │
│    - 如果支持流式读取，设置 on_chunked 回调              │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ 3. Body 数据接收阶段（流式模式）                         │
│    libevent 检测到输入缓冲区（evbuffer）中有数据        │
│    ↓                                                      │
│    on_chunked() / on_chunk_data() 回调（多次调用）      │
│    [be/src/http/ev_http_server.cpp:52]                  │
│    - 每次有数据到达时调用                                │
│    - 从 evbuffer 读取数据（最多 128KB）                 │
│    - 写入 StreamLoadPipe 或 MessageBodyFileSink         │
└─────────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────────┐
│ 4. 请求完成阶段                                          │
│    libevent 检测到 HTTP 请求完成（所有数据接收完毕）     │
│    ↓                                                      │
│    handle() / on_request() 回调                         │
│    [be/src/http/ev_http_server.cpp:62]                  │
│    - 完成数据接收                                        │
│    - 执行计划（非流式模式）                              │
│    - 等待执行完成                                        │
│    - 提交事务                                            │
│    - 返回响应给客户端                                    │
└─────────────────────────────────────────────────────────┘
```

### 回调函数详解

#### 1. on_connection() - 连接建立回调

**调用时机**：当新的 HTTP 连接建立时，libevent 调用此回调。

**位置**：`be/src/http/ev_http_server.cpp:77`

**功能**：
- 设置 `on_header` 回调函数，用于处理 HTTP header
- 准备接收 HTTP 请求

**代码**：
```cpp
static int on_connection(struct evhttp_request* req, void* param) {
    evhttp_request_set_header_cb(req, on_header);  // 设置 header 回调
    evhttp_request_set_on_complete_cb(req, nullptr, param);
    return 0;
}
```

#### 2. on_header() - Header 解析回调

**调用时机**：当 libevent 解析完所有 HTTP headers 后，调用此回调。

**位置**：`be/src/http/ev_http_server.cpp:240`

**调用流程**：
```
libevent 解析完 HTTP headers
    ↓
EvHttpServer::on_header()
    ↓
创建 HttpRequest 对象
    ↓
查找对应的 Handler（StreamLoadAction）
    ↓
StreamLoadAction::on_header()
    [be/src/http/action/stream_load.cpp:202]
    ↓
创建 StreamLoadContext
    ↓
解析 URL 参数（db, table）
    ↓
解析 HTTP headers（label, two_phase_commit 等）
    ↓
StreamLoadAction::_on_header()
    [be/src/http/action/stream_load.cpp:272]
    ↓
解析认证信息、数据格式、大小限制等
    ↓
开始事务（如果不是组提交模式）
    ↓
StreamLoadAction::_process_put()
    [be/src/http/action/stream_load.cpp:453]
    ↓
构建 TStreamLoadPutRequest
    ↓
调用 FE 获取执行计划
    ↓
设置 body_sink（StreamLoadPipe 或 MessageBodyFileSink）
    ↓
如果支持流式模式，设置 on_chunked 回调
    evhttp_request_set_chunked_cb(ev_req, on_chunked);
```

**关键点**：
- **返回值**：返回 0 表示成功，返回 -1 表示失败（此时需要发送错误响应）
- **流式模式判断**：如果 `handler->request_will_be_read_progressively()` 返回 `true`，则设置 `on_chunked` 回调
- **StreamLoadAction** 的 `request_will_be_read_progressively()` 返回 `true`，因此会启用流式读取

#### 3. on_chunk_data() - 数据块接收回调

**调用时机**：当 libevent 检测到输入缓冲区（evbuffer）中有数据时，调用此回调。**多次调用**，每次有数据到达时都会调用。

**位置**：
- libevent 回调：`be/src/http/ev_http_server.cpp:52` (`on_chunked`)
- Handler 实现：`be/src/http/action/stream_load.cpp:397` (`on_chunk_data`)

**调用流程**：
```
libevent 检测到 evbuffer 中有数据
    ↓
on_chunked() 静态回调
    [be/src/http/ev_http_server.cpp:52]
    ↓
StreamLoadAction::on_chunk_data()
    [be/src/http/action/stream_load.cpp:397]
    ↓
从 evbuffer 读取数据（最多 128KB）
    evbuffer_remove(evbuf, bb->ptr, bb->capacity)
    ↓
写入 body_sink
    ctx->body_sink->append(bb)
    ↓
如果是 StreamLoadPipe：
    - 数据写入内存缓冲区
    - Pipeline 可以从 StreamLoadPipe 读取数据
如果是 MessageBodyFileSink：
    - 数据写入临时文件
    - 等待所有数据接收完成后读取
```

**关键点**：
- **多次调用**：每次有数据到达时都会调用，不是只调用一次
- **数据大小**：每次最多读取 128KB（`128 * 1024`）
- **流式处理**：数据边接收边处理，不需要等待全部数据到达
- **非流式模式**：如果使用 `MessageBodyFileSink`，数据先写入文件，等待全部接收完成后再处理

#### 4. handle() - 请求完成回调

**调用时机**：当 HTTP 请求完成时（所有数据接收完毕），libevent 调用此回调。

**位置**：
- libevent 回调：`be/src/http/ev_http_server.cpp:62` (`on_request`)
- Handler 实现：`be/src/http/action/stream_load.cpp:102` (`handle`)

**调用流程**：
```
libevent 检测到 HTTP 请求完成
    ↓
on_request() 静态回调
    [be/src/http/ev_http_server.cpp:62]
    ↓
StreamLoadAction::handle()
    [be/src/http/action/stream_load.cpp:102]
    ↓
StreamLoadAction::_handle()
    [be/src/http/action/stream_load.cpp:163]
    ↓
验证接收的数据大小
    ↓
完成数据接收
    ctx->body_sink->finish()
    ↓
如果非流式模式，执行计划
    StreamLoadExecutor::execute_plan_fragment()
    ↓
等待执行完成
    ctx->future.get()
    ↓
提交事务
    StreamLoadExecutor::commit_txn()
    ↓
返回响应给客户端
    HttpChannel::send_reply()
```

**关键点**：
- **流式模式**：计划在 `_process_put` 中已执行，这里只需要等待完成
- **非流式模式**：计划在这里执行，因为需要等待所有数据接收完成
- **事务提交**：执行完成后提交事务，使数据可见

### 流式模式 vs 非流式模式

#### 流式模式（Streaming Mode）

**特点**：
- 使用 `StreamLoadPipe` 作为内存缓冲区
- 在 `on_header` 阶段设置 `on_chunked` 回调
- 数据边接收边处理，`on_chunk_data` 多次调用
- 在 `_process_put` 中立即执行计划
- 适合实时导入，低延迟

**数据流**：
```
HTTP 客户端发送数据
    ↓
on_chunk_data() 多次调用
    ↓
数据写入 StreamLoadPipe
    ↓
Pipeline 从 StreamLoadPipe 读取数据
    ↓
边接收边处理
```

**流控机制（Backpressure）**：

当消费速度比生产慢时，系统通过多层流控机制防止内存溢出：

1. **StreamLoadPipe 层流控**（主要流控层）
   - 缓冲区大小限制：默认 `_max_buffered_bytes = 4MB`
   - 当缓冲区满时：`StreamLoadPipe::_append()` 阻塞在 `_put_cond.wait()`
   - 等待消费者读取数据后，调用 `_put_cond.notify_one()` 唤醒生产者
   - 代码位置：`be/src/io/fs/stream_load_pipe.cpp:210-213`
   ```cpp
   while (!_cancelled && !_buf_queue.empty() &&
          _buffered_bytes + buf->remaining() > _max_buffered_bytes) {
       _put_cond.wait(l);  // 阻塞等待，直到消费者读取数据
   }
   ```

2. **HTTP evbuffer 层流控**
   - libevent 的 evbuffer 有大小限制
   - 当 evbuffer 满时，TCP 层会停止接收数据
   - `on_chunk_data` 每次最多读取 128KB，清空 evbuffer

3. **TCP 层流控**
   - TCP 接收窗口控制：如果接收端处理慢，发送端会停止发送
   - 这是操作系统层面的流控，自动处理

**流控流程**：
```
消费速度 < 生产速度
    ↓
StreamLoadPipe 缓冲区逐渐填满
    ↓
缓冲区达到 _max_buffered_bytes (4MB)
    ↓
StreamLoadPipe::_append() 阻塞等待
    ↓
HTTP on_chunk_data() 阻塞（因为 append 阻塞）
    ↓
evbuffer 逐渐填满
    ↓
TCP 接收窗口满，停止接收数据
    ↓
客户端发送速度降低或停止
    ↓
Pipeline 消费数据，缓冲区释放
    ↓
StreamLoadPipe::read_at_impl() 调用 _put_cond.notify_one()
    ↓
唤醒等待的 on_chunk_data()，继续接收数据
```

**负责流控的层级**：
- **主要流控层**：`StreamLoadPipe`（应用层）
  - 通过 `_max_buffered_bytes` 限制内存使用
  - 通过 `_put_cond` 条件变量实现阻塞等待
- **辅助流控层**：HTTP evbuffer（网络层）
  - libevent 自动管理 evbuffer 大小
- **底层流控**：TCP 接收窗口（传输层）
  - 操作系统自动处理

#### 非流式模式（Non-Streaming Mode）

**特点**：
- 使用 `MessageBodyFileSink` 作为临时文件
- 数据先全部写入磁盘文件，然后再处理
- 在 `handle` 中数据接收完成后才执行计划
- 适合大文件批量导入，或需要读取文件元数据的格式（如 Parquet、ORC）

**数据流**：
```
HTTP 客户端发送数据
    ↓
on_chunk_data() 多次调用（每次最多 128KB）
    ↓
MessageBodyFileSink::append() 写入临时文件
    ↓
等待所有数据接收完成
    ↓
handle() 中调用 body_sink->finish() 关闭文件
    ↓
execute_plan_fragment() 执行计划
    ↓
FileScanOperatorX 从临时文件读取数据并处理
```

**内存管理**：
- 数据写入磁盘文件，不占用内存（除了系统缓冲区）
- `on_chunk_data` 每次只分配 128KB 的临时缓冲区用于读取
- 写入文件后立即释放缓冲区，内存占用可控

**触发条件**：
- 自动触发：当数据格式不支持流式处理时
  - 不支持流式的格式：Parquet、ORC 等（需要读取文件元数据）
  - 支持流式的格式：CSV、JSON、TEXT、WAL、ARROW 等
- 判断逻辑：`LoadUtil::is_format_support_streaming(format)`

**用户如何使用**：
- 通过 HTTP header 指定格式：`format: parquet` 或 `format: orc`
- 系统自动选择非流式模式
- 示例：
  ```http
  PUT /api/test_db/test_table/_stream_load HTTP/1.1
  Host: localhost:8040
  Authorization: Basic dXNlcjpwYXNz
  Content-Type: application/octet-stream
  Content-Length: 1048576
  format: parquet
  
  [Parquet 文件二进制数据]
  ```

**临时文件管理**：
- 文件路径：`{load_path}/{table}.{timestamp}.{microseconds}`
- 文件在 `handle` 完成后自动删除
- 如果处理失败，文件也会被清理

### HTTP 请求示例

以下是一个典型的 Stream Load HTTP 请求：

```http
PUT /api/test_db/test_table/_stream_load HTTP/1.1
Host: localhost:8040
Authorization: Basic dXNlcjpwYXNz
Content-Type: text/plain
Content-Length: 1024
label: my_load_label
format: csv
column_separator: ,
unique_key_update_mode: UPDATE_FIXED_COLUMNS
partial_columns: id,name

id,name,age
1,Alice,20
2,Bob,25
3,Charlie,30
```

**处理流程**：
1. **on_connection()**：连接建立，设置 `on_header` 回调
2. **on_header()**：解析 headers，创建 `StreamLoadContext`，调用 FE 获取计划，设置 `on_chunked` 回调
3. **on_chunk_data()**（多次调用）：
   - 第 1 次：接收 `id,name,age\n1,Alice,20\n2,`
   - 第 2 次：接收 `Bob,25\n3,Charlie,30`
   - 数据写入 `StreamLoadPipe`
4. **handle()**：所有数据接收完成，等待执行完成，提交事务，返回响应

---

## 完整调用链

本节展示从 FE 接收请求到 BE 最终写入数据的完整代码调用路径，包括所有关键函数、文件位置和详细步骤。

### 通用 Stream Load 调用链

```
┌─────────────────────────────────────────────────────────────────┐
│                    BE HTTP 服务器层                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ 0. HTTP 服务器启动和路由注册
                              ▼
HttpService::start()
  [be/src/service/http_service.cpp:122]
  │
  └─> EvHttpServer::register_handler()
      [be/src/service/http_service.cpp:132]
      └─> 注册路由：PUT /api/{db}/{table}/_stream_load -> StreamLoadAction
                              │
                              │ 1. HTTP 请求到达（libevent）
                              ▼
EvHttpServer::on_header(struct evhttp_request* ev_req)
  [be/src/http/ev_http_server.cpp:240]
  │
  ├─> 创建 HttpRequest 对象
  ├─> 查找对应的 Handler（StreamLoadAction）
  │
  └─> StreamLoadAction::on_header(HttpRequest* req)
      [be/src/http/action/stream_load.cpp:202]
      │
      ├─> 创建 StreamLoadContext
      ├─> 解析 URL 参数（db, table）
      ├─> 解析 HTTP headers（label, two_phase_commit 等）
      ├─> 处理组提交（_handle_group_commit）
      │
      └─> StreamLoadAction::_on_header(req, ctx)
          [be/src/http/action/stream_load.cpp:272]
          │
          ├─> 解析认证信息（Basic Auth）
          ├─> 解析数据格式（format, compress_type）
          ├─> 检查 Content-Length 和大小限制
          ├─> 检查 Transfer-Encoding（chunked）
          ├─> 解析超时和注释参数
          │
          ├─> 开始事务（如果不是组提交模式）
          │   └─> StreamLoadExecutor::begin_txn(ctx)
          │       [be/src/runtime/stream_load/stream_load_executor.cpp]
          │
          └─> StreamLoadAction::_process_put(http_req, ctx)
              [be/src/http/action/stream_load.cpp:453]
              │
              ├─> 确定是否使用流式模式
              │   └─> ctx->use_streaming = LoadUtil::is_format_support_streaming()
              │
              ├─> 构建 TStreamLoadPutRequest
              │   ├─> 设置认证信息
              │   ├─> 设置数据库和表名
              │   ├─> 设置事务 ID
              │   ├─> 设置数据格式和压缩类型
              │   ├─> 解析 HTTP headers 中的参数：
              │   │   ├─> columns（列映射）
              │   │   ├─> column_separator（列分隔符）
              │   │   ├─> line_delimiter（行分隔符）
              │   │   ├─> unique_key_update_mode（部分更新模式，可选）
              │   │   ├─> strict_mode（严格模式）
              │   │   └─> 其他参数...
              │   │
              │   └─> 设置 body_sink
              │       ├─> 流式模式：StreamLoadPipe（内存缓冲区）
              │       └─> 非流式模式：MessageBodyFileSink（临时文件）
              │
              ├─> 调用 FE 获取执行计划
              │   └─> ThriftRpcHelper::rpc<FrontendServiceClient>()
              │       [be/src/http/action/stream_load.cpp:816]
              │       │
              │       └─> FrontendService::streamLoadPut(request)
              │           [fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java:2190]
              │           │
              │           └─> StreamLoadHandler.handle()
              │               [fe/fe-core/src/main/java/org/apache/doris/load/StreamLoadHandler.java:232]
              │               │
              │               ├─> NereidsStreamLoadTask.fromTStreamLoadPutRequest()
              │               │   └─> 解析请求，构建 StreamLoadTask
              │               │
              │               └─> NereidsStreamLoadPlanner.plan()
              │                   [fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsStreamLoadPlanner.java:103]
              │                   │
              │                   ├─> 验证表类型和配置
              │                   ├─> 确定更新模式（UPSERT / 部分更新）
              │                   ├─> NereidsLoadUtils.createLoadPlan()
              │                   │   └─> 构建逻辑执行计划
              │                   │       - LogicalOneRowRelation
              │                   │       - LogicalPreFilter（可选）
              │                   │       - LogicalProject（类型转换）
              │                   │       - LogicalLoadProject（列映射）
              │                   │       - LogicalTableSink
              │                   │
              │                   └─> NereidsLoadPlanInfoCollector
              │                       └─> 收集计划信息，设置到 TPipelineFragmentParams
              │                           ├─> FileLoadScanNode
              │                           └─> OlapTableSink
              │
              └─> 如果使用流式模式，立即执行计划
                  └─> StreamLoadExecutor::execute_plan_fragment(ctx, mocked)
                      [be/src/runtime/stream_load/stream_load_executor.cpp:81]
                              │
                              │ 2. 接收 HTTP Body 数据（libevent 触发，多次调用）
                              ▼
StreamLoadAction::on_chunk_data(HttpRequest* req)
  [be/src/http/action/stream_load.cpp:397]
  │
  ├─> 从 evbuffer 读取数据（最多 128KB）
  └─> ctx->body_sink->append(bb)
      └─> 写入 StreamLoadPipe 或 MessageBodyFileSink
                              │
                              │ 3. HTTP 请求完成（libevent 触发）
                              ▼
StreamLoadAction::handle(HttpRequest* req)
  [be/src/http/action/stream_load.cpp:102]
  │
  └─> StreamLoadAction::_handle(ctx)
      [be/src/http/action/stream_load.cpp:163]
      │
      ├─> 验证接收的数据大小
      ├─> ctx->body_sink->finish()
      │   └─> 完成数据接收
      │
      ├─> 如果非流式模式，执行计划
      │   └─> StreamLoadExecutor::execute_plan_fragment(ctx, mocked)
      │       [be/src/runtime/stream_load/stream_load_executor.cpp:81]
      │
      ├─> 等待执行完成
      │   └─> ctx->future.get()
      │
      └─> 提交事务
          └─> StreamLoadExecutor::commit_txn(ctx)
              [be/src/runtime/stream_load/stream_load_executor.cpp:320]
              │
              ├─> 收集执行结果
              │   └─> 从 exec_fragment 回调中获取统计信息
              │       - total_rows, loaded_rows, filtered_rows
              │       - tablet_commit_infos
              │
              ├─> 验证数据质量
              │   └─> 检查过滤比例是否超过 max_filter_ratio
              │
              ├─> 预提交事务（2PC 模式）
              │   └─> StreamLoadExecutor::pre_commit_txn(ctx)
              │       [be/src/runtime/stream_load/stream_load_executor.cpp:251]
              │       │
              │       └─> ThriftRpcHelper::rpc<FrontendServiceClient>()
              │           └─> FrontendService::loadTxnPreCommit()
              │               [fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java:1401]
              │               │
              │               └─> DatabaseTransactionMgr::preCommitTransaction2PC()
              │                   [fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java:453]
              │                   │
              │                   ├─> 验证所有 BE 完成写入
              │                   │   └─> checkCommitStatus()
              │                   │
              │                   ├─> 设置事务状态为 PRECOMMITTED
              │                   │   └─> transactionState.setTransactionStatus(PRECOMMITTED)
              │                   │
              │                   └─> 持久化事务状态
              │                       └─> editLog.logInsertTransactionState()
              │                           - 写入 EditLog，确保 FE 重启后可恢复
              │
              └─> 提交事务
                  └─> StreamLoadExecutor::operate_txn_2pc(ctx)
                      [be/src/runtime/stream_load/stream_load_executor.cpp:289]
                      │
                      └─> ThriftRpcHelper::rpc<FrontendServiceClient>()
                          └─> FrontendService::loadTxn2PC()
                              [fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java:1544]
                              │
                              └─> DatabaseTransactionMgr::commitTransaction2PC()
                                  [fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java:823]
                                  │
                                  └─> DatabaseTransactionMgr::unprotectedCommitTransaction2PC()
                                      [fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java:1682]
                                      │
                                      ├─> 验证事务状态为 PRECOMMITTED
                                      │   └─> 确保事务已通过 pre-commit
                                      │
                                      ├─> 设置事务状态为 COMMITTED
                                      │   └─> transactionState.setTransactionStatus(COMMITTED)
                                      │
                                      ├─> 设置版本号
                                      │   └─> partitionCommitInfo.setVersion()
                                      │
                                      └─> 持久化事务状态
                                          └─> editLog.logInsertTransactionState()
                                              - 写入 EditLog，确保 FE 重启后可恢复
                                              │
                                              └─> 版本发布（异步，由 PublishVersionDaemon 负责）
                                                  [fe/fe-core/src/main/java/org/apache/doris/transaction/PublishVersionDaemon.java:90]
                                                  │
                                                  ├─> 定期检查 COMMITTED 状态的事务
                                                  │   └─> getCommittedTxnList()
                                                  │
                                                  ├─> 向所有 BE 发送 publish version 任务
                                                  │   └─> genPublishTask()
                                                  │       - 向所有 involved BE 发送任务
                                                  │       - 如果部分 BE 失败，会重试
                                                  │
                                                  └─> 等待所有 BE 完成版本发布
                                                      └─> tryFinishTxn()
                                                          - 检查所有 BE 的 publish version 任务状态
                                                          - 如果所有任务完成，设置事务为 VISIBLE
                              │
                              │ 4. 执行计划（流式模式在 _process_put 中已执行）
                              ▼
StreamLoadExecutor::execute_plan_fragment(ctx, parent)
  [be/src/runtime/stream_load/stream_load_executor.cpp:81]
  │
  ├─> 设置查询选项
  │   └─> ctx->put_result.pipeline_params.query_options.enable_strict_cast = false
  │       （允许更宽松的数据类型转换）
  │
  ├─> 定义执行完成回调函数 exec_fragment
  │   └─> 收集执行结果和统计数据
  │       - 收集 tablet_commit_infos
  │       - 收集行数统计（total_rows, loaded_rows, filtered_rows）
  │       - 收集字节数统计
  │       - 处理事务提交或回滚
  │
  └─> FragmentMgr::exec_plan_fragment()
      [be/src/runtime/fragment_mgr.cpp:847]
      │
      ├─> 创建 QueryContext
      │   └─> 管理查询生命周期和资源
      │
      ├─> 创建 PipelineFragmentContext
      │   └─> 管理 Pipeline 执行上下文
      │
      ├─> PipelineFragmentContext::prepare()
      │   └─> 构建 Pipeline、初始化 Operator、创建 Pipeline Tasks
      │       ├─> 创建 FileScanOperatorX（从 StreamLoadPipe 读取）
      │       └─> 创建 OlapTableSinkOperatorX（写入 OLAP 表）
      │
      └─> PipelineFragmentContext::submit()
          [be/src/pipeline/pipeline_fragment_context.cpp:1869]
          │
          └─> 提交 Pipeline Tasks 到调度器
              │
              └─> TaskScheduler::submit()
                  [be/src/pipeline/task_scheduler.cpp:72]
                  │
                  └─> 任务进入队列，等待工作线程调度
                      │
                      └─> TaskScheduler::_do_work()
                          [be/src/pipeline/task_scheduler.cpp:97]
                          │
                          └─> PipelineTask::execute()
                              [be/src/pipeline/pipeline_task.cpp:384]
                              │
                              ├─> PipelineTask::_open() (首次执行时)
                              │   └─> 打开所有算子（Source、Operator、Sink）
                              │       │
                              │       ├─> FileScanLocalState::init()
                              │       │   [be/src/pipeline/exec/file_scan_operator.cpp:154]
                              │       │   └─> 初始化 FileScanLocalState
                              │       │
                              │       ├─> FileScanLocalState::open()
                              │       │   [be/src/pipeline/exec/scan_operator.cpp:133]
                              │       │   │
                              │       │   ├─> 处理下推的过滤条件（conjuncts）
                              │       │   │
                              │       │   └─> ScanLocalState::_prepare_scanners()
                              │       │       [be/src/pipeline/exec/scan_operator.cpp:1091]
                              │       │       │
                              │       │       ├─> FileScanLocalState::_init_scanners()
                              │       │       │   [be/src/pipeline/exec/file_scan_operator.cpp:72]
                              │       │       │   │
                              │       │       │   └─> 创建 FileScanner 列表
                              │       │       │       ├─> FileScanner::create_unique()
                              │       │       │       │   - 创建 FileScanner 对象
                              │       │       │       │   - 设置参数（format, compress_type 等）
                              │       │       │       │
                              │       │       │       └─> FileScanner::init()
                              │       │       │           [be/src/vec/exec/scan/file_scanner.cpp:134]
                              │       │       │           └─> 初始化 Scanner，设置过滤条件
                              │       │       │
                              │       │       └─> ScanLocalState::_start_scanners()
                              │       │           [be/src/pipeline/exec/scan_operator.cpp:1109]
                              │       │           │
                              │       │           └─> ScannerContext::create_shared()
                              │       │               [be/src/vec/exec/scan/scanner_context.cpp:104]
                              │       │               │
                              │       │               ├─> ScannerContext::init()
                              │       │               │   - 初始化 Scanner 上下文
                              │       │               │   - 设置并发度、队列大小等
                              │       │               │
                              │       │               └─> 启动 Scanner 调度器
                              │       │                   - Scanner 在后台线程池中执行
                              │       │                   - 通过 ScannerContext 管理
                              │       │
                              │       └─> OlapTableSinkV2LocalState::open()
                              │           [be/src/pipeline/exec/olap_table_sink_v2_operator.h:29]
                              │           │
                              │           └─> AsyncWriterSink::open()
                              │               [be/src/pipeline/exec/operator.cpp:756]
                              │               │
                              │               ├─> 克隆输出表达式上下文
                              │               │
                              │               └─> VTabletWriterV2::start_writer()
                              │                   [be/src/vec/sink/writer/async_result_writer.cpp:89]
                              │                   │
                              │                   ├─> AsyncResultWriter::start_writer()
                              │                   │   - 设置 Profile 和内存计数器
                              │                   │   - 启动后台写入线程
                              │                   │
                              │                   └─> VTabletWriterV2::open()
                              │                       [be/src/vec/sink/writer/vtablet_writer_v2.cpp]
                              │                       │
                              │                       ├─> VTabletWriterV2::_init()
                              │                       │   │
                              │                       │   ├─> 初始化 Schema、Location、NodesInfo
                              │                       │   ├─> 初始化 RowDistribution（数据分区和路由）
                              │                       │   ├─> 初始化 BlockConvertor（数据转换）
                              │                       │   └─> 初始化 LoadStreamStub（流式传输）
                              │                       │
                              │                       └─> 打开流式传输通道（LoadStreamStub）
                              │                           - 建立流式连接
                              │                           - 不再使用 RPC，直接本地写入
                              │
                              └─> 主执行循环（Pull-based 执行模型）
                                  │
                                  ├─> PipelineTask::_root->get_block_after_projects()
                                  │   [be/src/pipeline/exec/operator.cpp:372]
                                  │   │
                                  │   └─> 从 root 算子开始向上游 pull 数据
                                  │       │
                                  │       └─> FileScanOperatorX::get_block()
                                  │           [be/src/pipeline/exec/scan_operator.cpp:1378]
                                  │           │
                                  │           └─> ScannerContext::get_block_from_queue()
                                  │               │
                                  │               └─> FileScanner::get_next()
                                  │                   [be/src/vec/exec/scan/file_scanner.cpp:408]
                                  │                   │
                                  │                   ├─> FileScanner::prepare() (首次调用时)
                                  │                   │   - 准备 Scanner，初始化 Reader
                                  │                   │
                                  │                   ├─> FileScanner::open() (首次调用时)
                                  │                   │   [be/src/vec/exec/scan/file_scanner.cpp:389]
                                  │                   │   │
                                  │                   │   └─> FileScanner::_get_next_reader()
                                  │                   │       │
                                  │                   │       └─> FileFactory::create_pipe_reader()
                                  │                   │           [be/src/io/file_factory.cpp:275]
                                  │                   │           │
                                  │                   │           └─> 从 StreamLoadManager 获取 StreamLoadPipe
                                  │                   │               - 通过 load_id 查找 StreamLoadContext
                                  │                   │               - 返回 StreamLoadPipe 的 FileReader 包装
                                  │                   │
                                  │                   └─> FileScanner::_get_block_impl()
                                  │                       │
                                  │                       └─> 根据格式调用对应的 Reader
                                  │                           - CSV: CsvReader::get_next_block()
                                  │                           - JSON: NewJsonReader::get_next_block()
                                  │                           - 解析数据、类型转换、列映射
                                  │                           - 返回 Block 给 ScannerContext
                                  │
                                  └─> PipelineTask::_sink->sink()
                                      │
                                      └─> OlapTableSinkV2OperatorX::sink()
                                          [be/src/pipeline/exec/olap_table_sink_v2_operator.h]
                                          │
                                          └─> OlapTableSinkV2LocalState::sink()
                                              [be/src/pipeline/exec/olap_table_sink_v2_operator.h:29]
                                              │
                                              └─> AsyncWriterSink::sink()
                                                  [be/src/pipeline/exec/operator.cpp:769]
                                                  │
                                                  └─> VTabletWriterV2::sink()
                                                      [be/src/vec/sink/writer/async_result_writer.cpp:42]
                                                      │
                                                      └─> 数据进入异步写入队列
                                                          │
                                                          └─> VTabletWriterV2::process_block() (后台线程)
                                                              │
                                                              └─> VTabletWriterV2::write()
                                                                  [be/src/vec/sink/writer/vtablet_writer_v2.cpp]
                                                                  │
                                                                  ├─> 数据分区和路由
                                                                  │   └─> _row_distribution.generate_rows_distribution()
                                                                  │
                                                                  └─> DeltaWriterV2Pool::get_or_create()
                                                                      [be/src/vec/sink/delta_writer_v2_pool.cpp]
                                                                      │
                                                                      └─> 获取或创建 DeltaWriterV2（本地，不再通过 RPC）
                                                                          │
                                                                          ├─> DeltaWriterV2::init() (首次创建时)
                                                                          │   [be/src/olap/delta_writer_v2.cpp:95]
                                                                          │   │
                                                                          │   ├─> _build_current_tablet_schema()
                                                                          │   │   [be/src/olap/delta_writer_v2.cpp:245]
                                                                          │   │   │
                                                                          │   │   └─> PartialUpdateInfo::init() (部分更新时)
                                                                          │   │       [be/src/olap/partial_update_info.cpp:42]
                                                                          │   │       - 设置更新模式、计算 update_cids 和 missing_cids
                                                                          │   │
                                                                          │   ├─> 创建 BetaRowsetWriterV2
                                                                          │   │
                                                                          │   └─> MemTableWriter::init()
                                                                          │       [be/src/olap/memtable_writer.cpp:66]
                                                                          │       - 传入已初始化的 _partial_update_info
                                                                          │
                                                                          └─> DeltaWriterV2::write()
                                                                              [be/src/olap/delta_writer_v2.cpp:142]
                                                                              │
                                                                              └─> MemTableWriter::write()
                                                                                  [be/src/olap/memtable_writer.cpp:89]
                                                                                  │
                                                                                  └─> MemTable::insert()
                                                                                      [be/src/olap/memtable.cpp]
                                                                                      │
                                                                                      └─> MemTable::_insert()
                                                                                                                          │
                                                                                                                          └─> 写入完整数据或部分数据
                                                                                                                              - 普通模式：写入所有列
                                                                                                                              - 部分更新模式：只写入部分列
                              │
                              │ 5. MemTable 刷新
                              ▼
MemTable::flush()
  [be/src/olap/memtable.cpp]
  │
  └─> VerticalSegmentWriter::write_batch()
      [be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp:945]
      │
      └─> 写入 Segment
          - 普通模式：直接写入所有列
          - 部分更新模式：读取历史数据，填充缺失列，然后写入
```

---

## 部分更新场景

部分列更新是 Stream Load 的一个特殊场景，允许用户只更新表中的部分列，而不是整行数据。部分更新支持两种模式：

- **固定部分更新（UPDATE_FIXED_COLUMNS）**：每次更新时，所有行都更新相同的列集合
- **灵活部分更新（UPDATE_FLEXIBLE_COLUMNS）**：每行可以更新不同的列集合，使用 skip bitmap 标记哪些列需要更新

### 固定部分更新调用链

固定部分更新的调用链与通用 Stream Load 调用链基本相同，主要区别在于：

1. **FE 端**：在 `NereidsStreamLoadPlanner.plan()` 中确定部分更新模式和列集合
2. **BE 端**：在 `DeltaWriterV2::init()` → `_build_current_tablet_schema()` 时调用 `PartialUpdateInfo::init()` 初始化部分更新信息（V2 版本）
3. **MemTable 写入**：使用 `MemTable::_insert_with_partial_content()` 只写入部分列
4. **Segment 写入**：使用 `VerticalSegmentWriter::_append_block_with_partial_content()` 读取历史数据并填充缺失列

#### PartialUpdateInfo 初始化（V2 版本）

```
DeltaWriterV2::init()
  [be/src/olap/delta_writer_v2.cpp:95]
  │
  ├─> _build_current_tablet_schema()
  │   [be/src/olap/delta_writer_v2.cpp:245]
  │   │
  │   └─> PartialUpdateInfo::init() (部分更新时)
  │       [be/src/olap/partial_update_info.cpp:42]
  │       │
  │       ├─> 设置更新模式
  │       │   └─> partial_update_mode = UPDATE_FIXED_COLUMNS
  │       │
  │       ├─> 计算 update_cids 和 missing_cids
  │       │   ├─> update_cids: 在 partial_update_input_columns 中的列
  │       │   └─> missing_cids: 不在 partial_update_input_columns 中的列
  │       │       （必须包含所有主键列）
  │       │
  │       └─> 生成默认值
  │           └─> _generate_default_values_for_missing_cids()
  │               - 为 missing_cids 生成默认值 block
  │
  ├─> 创建 BetaRowsetWriterV2
  │
  └─> MemTableWriter::init()
      [be/src/olap/memtable_writer.cpp:66]
      - 传入已初始化的 _partial_update_info（在 _build_current_tablet_schema() 中已初始化）
```

#### MemTable 写入部分数据

```
MemTable::_insert_with_partial_content()
  [be/src/olap/memtable.cpp]
  │
  └─> 只写入 update_cids 的列
      - 创建部分 block，只包含 update_cids
      - 写入 MemTable
```

#### Segment 写入时填充缺失列

```
VerticalSegmentWriter::_append_block_with_partial_content()
  [be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp:515]
  │
  ├─> 1. 写入 update_cids 的列
  │   └─> 使用 VerticalSegmentWriter 写入列数据
  │
  ├─> 2. 查找历史数据位置
  │   └─> FixedReadPlan.prepare_to_read()
  │       - 对每行数据编码主键
  │       - 查找历史数据中的位置（RowLocation）
  │
  ├─> 3. 读取缺失列
  │   └─> FixedReadPlan.read_columns_by_plan()
  │       - 批量读取 missing_cids 的列
  │
  └─> 4. 填充缺失列
      └─> FixedReadPlan.fill_missing_columns()
          [be/src/olap/partial_update_info.cpp:367]
          │
          ├─> 生成默认值 block
          ├─> 对每行数据：
          │   ├─> 如果新行有删除标记：使用默认值
          │   ├─> 如果旧行有删除标记：使用默认值
          │   └─> 否则：
          │       ├─> 如果启用 ignore_null_values：
          │       │   └─> BaseTablet::generate_new_block_for_partial_update()
          │       │       - 检查新值是否为 NULL
          │       │       - 如果新值为 NULL：使用旧值
          │       │       - 如果新值不为 NULL：使用新值
          │       └─> 否则：使用旧值
          │
          └─> 合并到完整 block
              └─> full_block 包含所有列（update_cids + missing_cids）
```

### 灵活部分更新调用链

灵活部分更新的调用链与固定部分更新类似，主要区别在于：

1. **MemTable 写入**：使用 `MemTable::_insert_with_flexible_partial_content()` 写入所有列，使用 skip bitmap 标记更新的列
2. **Segment 写入**：使用 `VerticalSegmentWriter::_append_block_with_flexible_partial_content()` 处理灵活部分更新

#### MemTable 写入（灵活模式）

```
MemTable::_insert_with_flexible_partial_content()
  [be/src/olap/memtable.cpp]
  │
  └─> 写入所有列，使用 skip bitmap 标记更新的列
      - skip bitmap: 标记哪些列未指定（需要保留旧值）
      - 写入所有列到 MemTable
```

#### Segment 写入（灵活模式）

```
VerticalSegmentWriter::_append_block_with_flexible_partial_content()
  [be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp:686]
  │
  ├─> 1. 聚合重复行
  │   └─> BlockAggregator::aggregate_for_flexible_partial_update()
  │       - 根据 skip bitmap 合并相同 key 的行
  │
  ├─> 2. 编码主键列
  ├─> 3. 查找历史数据位置
  │   └─> FlexibleReadPlan.prepare_to_read()
  │       - 根据 skip bitmap 确定需要读取的列
  │
  ├─> 4. 读取缺失列
  │   └─> FlexibleReadPlan.read_columns_by_plan()
  │
  └─> 5. 填充缺失列
      └─> FlexibleReadPlan.fill_non_primary_key_columns()
          [be/src/olap/partial_update_info.cpp:545]
          │
          └─> fill_one_cell() lambda 函数
              ├─> 如果列在 skip bitmap 中（未指定）：从旧值读取
              └─> 如果列不在 skip bitmap 中（已指定）：
                  ├─> 如果启用 ignore_null_values：
                  │   ├─> 如果新值为 NULL：使用旧值
                  │   └─> 如果新值不为 NULL：使用新值
                  └─> 否则：使用新值
```

---

## 关键概念说明

### 1. 流式模式 vs 非流式模式

**流式模式（Streaming Mode）**：
- **触发条件**：数据格式支持流式处理（如 CSV、JSON）
- **特点**：
  - 数据边接收边处理，不需要等待全部数据到达
  - 使用 `StreamLoadPipe` 作为内存缓冲区
  - 在 `_process_put` 中立即执行计划
- **优势**：低延迟，适合实时导入

**非流式模式（Non-Streaming Mode）**：
- **触发条件**：数据格式不支持流式处理（如 Parquet）
- **特点**：
  - 数据先全部写入临时文件
  - 使用 `MessageBodyFileSink` 保存数据
  - 在 `handle` 中数据接收完成后才执行计划
- **优势**：适合大文件批量导入

### 2. Pull-based 执行模型

Pipeline 采用 **Pull-based（拉取式）** 执行模型：
- **数据流向**：从下游（Sink）向上游（Source）拉取数据
- **执行流程**：
  1. `PipelineTask::execute()` 调用 `_sink->sink()` 需要数据
  2. `_sink` 调用 `_root->get_block_after_projects()` 拉取数据
  3. `_root` 向上游算子拉取数据，最终到达 `FileScanOperatorX`
  4. `FileScanOperatorX` 从 `StreamLoadPipe` 读取数据
  5. 数据沿调用链返回，最终到达 `_sink`
- **优势**：
  - 按需处理，避免不必要的数据处理
  - 支持流式处理，边读边写
  - 内存占用可控

### 3. 异步写入机制

`OlapTableSinkV2OperatorX` 使用异步写入机制：
- **同步层**：`AsyncWriterSink::sink()` 接收数据，放入队列
- **异步层**：后台线程 `VTabletWriterV2::process_block()` 处理队列中的数据
- **优势**：
  - 解耦数据接收和写入，提高吞吐量
  - 避免阻塞 Pipeline Task 的执行
  - 支持批量写入，提高效率
  - **V2 版本优势**：使用流式传输（LoadStreamStub），不再通过 RPC，直接在本地写入 MemTable，减少网络开销

### 4. 数据分区和路由

数据写入前需要进行分区和路由：
- **分区**：根据分区键确定数据属于哪个分区
- **路由**：根据分桶键确定数据属于哪个 tablet
- **实现**：`RowDistribution::generate_rows_distribution()`
  - 计算每行数据的分区 ID 和 tablet ID
  - 将数据按 tablet 分组
  - 发送到对应的 `VNodeChannel`

### 5. 数据传输机制

**V2 版本（本文档假设使用的版本）**：
- **流式传输**：使用 `LoadStreamStub` 进行流式数据传输
- **本地写入**：数据直接在本地写入 MemTable，不再通过 RPC 发送到目标 BE
- **管理**：`DeltaWriterV2Pool` 管理同一 `load_id` 的所有 `DeltaWriterV2`，按 `tablet_id` 路由
- **优势**：减少网络开销，提高写入性能

**旧版本（参考）**：
- **RPC 传输**：通过 RPC 发送到目标 BE
- **发送端**：`VNodeChannel::_send_batch()` 发送 `PTabletWriterAddBlockRequest`
- **接收端**：`PInternalService::tablet_writer_add_block()` 接收请求
- **管理**：`LoadChannelMgr` 管理所有 `LoadChannel`，按 `load_id` 组织

### 6. MemTable 和 Segment

数据写入分为两个阶段：
- **MemTable 阶段**：
  - 数据先写入内存中的 `MemTable`
  - 支持快速写入和部分更新
  - 当 MemTable 达到阈值时触发 flush
- **Segment 阶段**：
  - MemTable flush 时写入磁盘 Segment
  - 对于部分更新，需要读取历史数据填充缺失列
  - Segment 是列式存储格式，支持高效查询

### 7. 2PC 事务提交的可靠性机制

Doris 使用 2PC（两阶段提交）来保证事务的一致性。虽然经典的 2PC 存在一些问题（如协调者故障），但 Doris 通过以下机制提高了可靠性：

#### 2PC 的两个阶段

**阶段 1：Pre-commit（预提交）**
```
BE 端：StreamLoadExecutor::pre_commit_txn()
    ↓
FE 端：DatabaseTransactionMgr::preCommitTransaction2PC()
    ↓
1. 验证所有 BE 完成写入（checkCommitStatus）
2. 设置事务状态为 PRECOMMITTED
3. 持久化事务状态到 EditLog
   └─> editLog.logInsertTransactionState()
```

**阶段 2：Commit（提交）**
```
BE 端：StreamLoadExecutor::operate_txn_2pc()
    ↓
FE 端：DatabaseTransactionMgr::commitTransaction2PC()
    ↓
1. 验证事务状态为 PRECOMMITTED
2. 设置事务状态为 COMMITTED
3. 持久化事务状态到 EditLog
   └─> editLog.logInsertTransactionState()
4. 异步发布版本（PublishVersionDaemon）
   └─> 向所有 BE 发送 publish version 任务
```

#### 关键设计：Commit 阶段不直接向 BE 发送消息

**重要**：Doris 的 commit 阶段**不是**直接向 BE 发送 commit 消息，而是：
1. 改变事务状态为 COMMITTED（持久化到 EditLog）
2. 版本发布是**异步的**，由 `PublishVersionDaemon` 后台线程负责
3. `PublishVersionDaemon` 定期检查 COMMITTED 状态的事务，向所有 BE 发送 publish version 任务

#### FE 故障恢复机制

**场景 1：FE 在 pre-commit 阶段挂了**
- 事务状态已持久化为 PRECOMMITTED
- FE 重启后，通过 `replayUpsertTransactionState()` 恢复事务状态
- 事务保持在 PRECOMMITTED 状态，不会丢失
- 可以继续执行 commit 阶段

**场景 2：FE 在 commit 阶段挂了（只持久化了 COMMITTED 状态，还没发布版本）**
- 事务状态已持久化为 COMMITTED
- FE 重启后，通过 `replayUpsertTransactionState()` 恢复事务状态
- `PublishVersionDaemon` 会检测到 COMMITTED 状态的事务
- 继续向所有 BE 发送 publish version 任务
- **不会出现"只给一个 BE 发送了 commit"的情况**，因为版本发布是异步的，且会向所有 BE 发送

**场景 3：FE 在版本发布过程中挂了（部分 BE 已收到版本，部分 BE 未收到）**
- 事务状态已持久化为 COMMITTED
- FE 重启后，恢复 COMMITTED 状态
- `PublishVersionDaemon` 会检查所有 BE 的 publish version 任务状态
- 向未完成的 BE 重新发送 publish version 任务
- 直到所有 BE 完成版本发布，事务才变为 VISIBLE

#### 版本发布的可靠性保证

**PublishVersionDaemon 的工作机制**：
```java
// PublishVersionDaemon 定期执行
1. 获取所有 COMMITTED 状态的事务
   └─> getCommittedTxnList()
2. 向所有 involved BE 发送 publish version 任务
   └─> genPublishTask()
       - 如果任务已发送，跳过（避免重复发送）
       - 向所有 BE 发送任务（包括 dead backend）
3. 检查任务完成状态
   └─> tryFinishTxn()
       - 检查所有 BE 的任务状态
       - 如果所有任务完成，设置事务为 VISIBLE
       - 如果部分任务失败，会重试
```

**重试机制**：
- 如果部分 BE 的 publish version 任务失败，`PublishVersionDaemon` 会在下次循环中重试
- BE 端会检查版本连续性，如果版本不连续，会等待前序版本完成
- FE 端会等待所有 BE 完成版本发布，或超时后继续（基于 quorum 机制）

#### 2PC 的可靠性保证

**Doris 2PC 的可靠性保证**：
1. **状态持久化**：每个阶段的状态都持久化到 EditLog，FE 重启后可恢复
2. **异步版本发布**：commit 阶段不直接向 BE 发送消息，而是异步发布版本
3. **重试机制**：版本发布失败会重试，直到所有 BE 完成
4. **Quorum 机制**：即使部分 BE 失败，只要达到 quorum，事务仍可完成
5. **超时处理**：如果版本发布超时，FE 会基于 quorum 机制决定是否完成事务

**与传统 2PC 的区别**：
- **传统 2PC**：协调者直接向所有参与者发送 commit，如果协调者挂了，可能导致部分参与者已提交，部分未提交
- **Doris 2PC**：
  - commit 阶段只改变 FE 端状态（持久化）
  - 版本发布是异步的，由后台线程负责
  - FE 重启后可以继续版本发布，不会出现"部分 BE 已提交，部分未提交"的情况

**总结**：Doris 的 2PC 实现通过**状态持久化**和**异步版本发布**机制，避免了传统 2PC 的协调者故障问题，提高了可靠性。

---

## 关键函数调用位置

| 函数名 | 文件位置 | 说明 |
|--------|---------|------|
| **HTTP 服务器层** | | |
| `HttpService::start()` | `be/src/service/http_service.cpp:122` | HTTP 服务器启动，注册路由 |
| `EvHttpServer::on_header()` | `be/src/http/ev_http_server.cpp:240` | libevent HTTP header 回调 |
| `StreamLoadAction::on_header()` | `be/src/http/action/stream_load.cpp:202` | 处理 HTTP header |
| `StreamLoadAction::_on_header()` | `be/src/http/action/stream_load.cpp:272` | 解析 HTTP 参数，开始事务 |
| `StreamLoadAction::_process_put()` | `be/src/http/action/stream_load.cpp:453` | 构建请求，调用 FE 获取计划 |
| `StreamLoadAction::on_chunk_data()` | `be/src/http/action/stream_load.cpp:397` | 接收 HTTP body 数据 |
| `StreamLoadAction::handle()` | `be/src/http/action/stream_load.cpp:102` | HTTP 请求完成处理 |
| `StreamLoadAction::_handle()` | `be/src/http/action/stream_load.cpp:163` | 完成数据接收，执行计划，提交事务 |
| **FE 端** | | |
| `FrontendService.streamLoadPut()` | `fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java:2190` | FE 接收请求入口 |
| `StreamLoadHandler.handle()` | `fe/fe-core/src/main/java/org/apache/doris/load/StreamLoadHandler.java:232` | 处理 Stream Load 请求 |
| `NereidsStreamLoadPlanner.plan()` | `fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsStreamLoadPlanner.java:103` | 生成执行计划 |
| `NereidsLoadUtils.createLoadPlan()` | `fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadUtils.java:131` | 构建逻辑计划树 |
| **BE 执行层** | | |
| `StreamLoadExecutor::execute_plan_fragment()` | `be/src/runtime/stream_load/stream_load_executor.cpp:81` | BE 执行计划入口 |
| `StreamLoadExecutor::begin_txn()` | `be/src/runtime/stream_load/stream_load_executor.cpp:200` | 开始事务 |
| `StreamLoadExecutor::pre_commit_txn()` | `be/src/runtime/stream_load/stream_load_executor.cpp:251` | 预提交事务（2PC） |
| `StreamLoadExecutor::operate_txn_2pc()` | `be/src/runtime/stream_load/stream_load_executor.cpp:289` | 提交事务（2PC） |
| `StreamLoadExecutor::commit_txn()` | `be/src/runtime/stream_load/stream_load_executor.cpp:320` | 提交事务（统一入口） |
| `FragmentMgr::exec_plan_fragment()` | `be/src/runtime/fragment_mgr.cpp:847` | 执行计划片段 |
| `PipelineFragmentContext::prepare()` | `be/src/pipeline/pipeline_fragment_context.cpp` | 构建 Pipeline、初始化 Operator |
| `PipelineFragmentContext::submit()` | `be/src/pipeline/pipeline_fragment_context.cpp:1869` | 提交 Pipeline Tasks |
| `TaskScheduler::_do_work()` | `be/src/pipeline/task_scheduler.cpp:97` | 工作线程执行任务 |
| `PipelineTask::execute()` | `be/src/pipeline/pipeline_task.cpp:384` | Pipeline Task 执行 |
| **Pipeline 算子** | | |
| `FileScanLocalState::init()` | `be/src/pipeline/exec/file_scan_operator.cpp:154` | 初始化 FileScanLocalState |
| `FileScanLocalState::open()` | `be/src/pipeline/exec/scan_operator.cpp:133` | 打开 FileScanLocalState |
| `FileScanLocalState::_init_scanners()` | `be/src/pipeline/exec/file_scan_operator.cpp:72` | 创建 FileScanner 列表 |
| `FileScanner::init()` | `be/src/vec/exec/scan/file_scanner.cpp:134` | 初始化 FileScanner |
| `FileScanner::open()` | `be/src/vec/exec/scan/file_scanner.cpp:389` | 打开 FileScanner，获取 Reader |
| `FileFactory::create_pipe_reader()` | `be/src/io/file_factory.cpp:275` | 创建 StreamLoadPipe Reader |
| `FileScanner::get_next()` | `be/src/vec/exec/scan/file_scanner.cpp:408` | 读取数据块 |
| `ScannerContext::get_block_from_queue()` | `be/src/vec/exec/scan/scanner_context.cpp` | 从队列获取数据块 |
| `FileScanOperatorX::get_block()` | `be/src/pipeline/exec/scan_operator.cpp:1378` | 文件扫描算子读取数据 |
| `OlapTableSinkV2LocalState::open()` | `be/src/pipeline/exec/olap_table_sink_v2_operator.h:29` | 打开 OlapTableSinkV2LocalState（V2） |
| `AsyncWriterSink::open()` | `be/src/pipeline/exec/operator.cpp:756` | 打开 AsyncWriterSink |
| `VTabletWriterV2::start_writer()` | `be/src/vec/sink/writer/async_result_writer.cpp:89` | 启动后台写入线程（V2） |
| `VTabletWriterV2::_init()` | `be/src/vec/sink/writer/vtablet_writer_v2.cpp` | 初始化 VTabletWriterV2 |
| `VTabletWriterV2::open()` | `be/src/vec/sink/writer/vtablet_writer_v2.cpp` | 打开 VTabletWriterV2 |
| `OlapTableSinkV2OperatorX::sink()` | `be/src/pipeline/exec/olap_table_sink_v2_operator.h` | OLAP 表写入算子（V2） |
| `VTabletWriterV2::write()` | `be/src/vec/sink/writer/vtablet_writer_v2.cpp` | 数据写入和路由（V2） |
| `DeltaWriterV2Pool::get_or_create()` | `be/src/vec/sink/delta_writer_v2_pool.cpp` | 获取或创建 DeltaWriterV2 |
| `DeltaWriterV2::init()` | `be/src/olap/delta_writer_v2.cpp:95` | 初始化 DeltaWriterV2 |
| `DeltaWriterV2::write()` | `be/src/olap/delta_writer_v2.cpp:142` | 写入数据到 MemTable（V2） |
| `RowsetBuilder::init()` | `be/src/olap/rowset_builder.cpp:404` | 初始化 RowsetBuilder |
| **数据写入层** | | |
| `MemTable::insert()` | `be/src/olap/memtable.cpp` | 写入 MemTable |
| `MemTable::flush()` | `be/src/olap/memtable.cpp` | MemTable 刷新 |
| `VerticalSegmentWriter::write_batch()` | `be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp:945` | 写入 Segment |
| **部分更新相关** | | |
| `PartialUpdateInfo::init()` | `be/src/olap/partial_update_info.cpp:42` | 初始化部分更新信息 |
| `MemTable::_insert_with_partial_content()` | `be/src/olap/memtable.cpp` | 固定部分更新写入 |
| `MemTable::_insert_with_flexible_partial_content()` | `be/src/olap/memtable.cpp` | 灵活部分更新写入 |
| `VerticalSegmentWriter::_append_block_with_partial_content()` | `be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp:515` | 固定部分更新写入 Segment |
| `VerticalSegmentWriter::_append_block_with_flexible_partial_content()` | `be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp:686` | 灵活部分更新写入 Segment |
| `FixedReadPlan.fill_missing_columns()` | `be/src/olap/partial_update_info.cpp:367` | 固定部分更新填充缺失列 |
| `FlexibleReadPlan.fill_non_primary_key_columns()` | `be/src/olap/partial_update_info.cpp:545` | 灵活部分更新填充缺失列 |
| `BlockAggregator::merge_one_row()` | `be/src/olap/partial_update_info.cpp:801` | 灵活部分更新合并行 |
| `BaseTablet::generate_new_block_for_partial_update()` | `be/src/olap/base_tablet.cpp:1009` | 固定部分更新生成完整 block（支持 ignore_null_values） |

