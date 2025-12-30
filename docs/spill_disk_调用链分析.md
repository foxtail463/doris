# Doris Spill Disk 调用链分析文档

## 目录

1. [概述](#1-概述)
2. [核心组件架构](#2-核心组件架构)
3. [调用链详解](#3-调用链详解)
4. [目录结构与管理](#4-目录结构与管理)
5. [容量管理与限制](#5-容量管理与限制)
6. [使用 Spill 的操作符](#6-使用-spill-的操作符)
7. [配置参数详解](#7-配置参数详解)
8. [监控指标](#8-监控指标)
9. [性能优化建议](#9-性能优化建议)
10. [故障排查](#10-故障排查)
11. [总结](#11-总结)

---

## 1. 概述

### 1.1 什么是 Spill Disk

Spill Disk（溢出到磁盘）是 Doris 中用于处理大查询内存不足时的核心机制。当查询操作（如 Hash Join、Sort、Aggregation）的内存使用超过限制时，会将部分数据溢出到磁盘，以释放内存并继续执行查询。

### 1.2 核心价值

- **内存管理**: 避免 OOM，支持更大规模的查询
- **查询稳定性**: 在内存受限环境下仍能完成查询
- **资源隔离**: 通过磁盘容量限制保护系统稳定性
- **性能权衡**: 在内存和磁盘 I/O 之间取得平衡

### 1.3 适用场景

- 大表 Join 操作
- 大数据量排序
- 复杂聚合查询
- 内存受限的生产环境

---

## 2. 核心组件架构

### 2.1 组件层次结构

```
ExecEnv (全局环境)
  └── SpillStreamManager (全局 Spill 流管理器)
      ├── SpillDataDir (磁盘目录管理，多个)
      │   ├── 容量监控
      │   ├── 使用量统计
      │   └── 限制检查
      └── SpillStream (Spill 流，每个操作符分区一个)
          ├── SpillWriter (写入器)
          │   ├── Block 序列化
          │   ├── ZSTD 压缩
          │   └── 文件写入
          └── SpillReader (读取器)
              ├── 文件读取
              ├── 数据解压
              └── Block 反序列化
```

### 2.2 关键类说明

#### SpillStreamManager
- **位置**: `be/src/vec/spill/spill_stream_manager.h`
- **职责**: 
  - 管理所有 SpillDataDir（磁盘目录）
  - 创建和注册 SpillStream
  - 后台 GC 线程清理过期文件
  - 监控 spill 读写统计
  - 磁盘选择策略（SSD 优先，使用率低优先）

#### SpillDataDir
- **位置**: `be/src/vec/spill/spill_stream_manager.h`
- **职责**:
  - 管理单个磁盘目录的容量和限制
  - 检查磁盘容量是否足够（双重限制）
  - 更新 spill 数据使用量统计
  - 提供 spill 数据路径和 GC 路径
  - 定期更新磁盘空间信息

#### SpillStream
- **位置**: `be/src/vec/spill/spill_stream.h`
- **职责**:
  - 封装单个 spill 流的读写操作
  - 管理 SpillWriter 和 SpillReader 生命周期
  - 提供 `spill_block()`、`spill_eof()`、`read_next_block_sync()` 等接口
  - 管理 spill 目录的创建和清理

#### SpillWriter
- **位置**: `be/src/vec/spill/spill_writer.h`
- **职责**:
  - 将 Block 序列化为 PBlock（Protobuf 格式）
  - 使用 ZSTD 压缩（压缩比更好）
  - 写入本地文件系统
  - 维护文件元数据（block offset 列表）
  - 支持分批写入（避免大 Block 一次性写入）

#### SpillReader
- **位置**: `be/src/vec/spill/spill_reader.h`
- **职责**:
  - 从磁盘文件读取数据
  - 反序列化 PBlock 为 Block
  - 支持随机访问（seek 到指定 block）
  - 读取文件 meta 信息

### 2.3 组件交互关系

```
操作符 (Operator)
    │
    ├─→ SpillStreamManager::register_spill_stream()  [注册流]
    │
    ├─→ SpillStream::spill_block()                   [写入数据]
    │       └─→ SpillWriter::write()
    │
    ├─→ SpillStream::read_next_block_sync()          [读取数据]
    │       └─→ SpillReader::read()
    │
    └─→ SpillStreamManager::delete_spill_stream()    [清理流]
            └─→ SpillStream::gc()
```

---

## 3. 调用链详解

### 3.1 初始化流程

**调用链**:
```
doris_main()
  └── ExecEnv::init()
      └── ExecEnv::_init()
          ├── 创建 SpillStreamManager
          │   └── 传入 spill_store_paths 创建 SpillDataDir map
          └── SpillStreamManager::init()
              ├── _init_spill_store_map()
              │   └── 初始化所有 SpillDataDir
              │       └── SpillDataDir::init()
              │           └── SpillDataDir::update_capacity()
              │               ├── 获取磁盘空间信息
              │               ├── 计算 spill 数据限制
              │               └── 注册监控指标
              ├── 创建 spill 和 spill_gc 目录
              │   ├── 如果 spill 目录已存在，移动到 GC 目录（异常恢复）
              │   └── 重新创建空的 spill 目录
              └── 启动 GC 后台线程
                  └── _spill_gc_thread_callback()
```

**关键步骤**:
1. **初始化 SpillDataDir**: 检查路径、更新容量、注册指标
2. **目录管理**: 如果 spill 目录已存在（异常退出），移动到 GC 目录
3. **GC 线程**: 后台定期清理过期文件

### 3.2 Spill Stream 注册流程

**调用链**（以 PartitionedHashJoinSink 为例）:
```
PartitionedHashJoinSinkLocalState::open()
  └── SpillStreamManager::register_spill_stream()
      ├── _get_stores_for_spill() - 选择可用磁盘
      │   ├── 优先选择 SSD
      │   ├── 如果 SSD 不可用，选择 HDD
      │   ├── 排除已达到容量限制的磁盘
      │   └── 按使用率排序（使用率低优先）
      ├── 创建 spill 目录
      │   └── 格式: storage_root/spill/query_id/operator_name-node_id-task_id-stream_id
      ├── 创建 SpillStream 对象
      └── SpillStream::prepare()
          ├── 创建 SpillWriter
          ├── 创建 SpillReader（使用 writer 的文件路径）
          ├── 设置写入计数器
          └── SpillWriter::open() - 创建文件
```

**磁盘选择策略**:
1. **优先级**: SSD > HDD
2. **过滤条件**: 排除已达到容量限制的磁盘
3. **排序规则**: 在相同类型的磁盘中，优先选择使用率低的
4. **容错**: 如果所有磁盘都不可用，返回错误

### 3.3 内存触发 Spill 机制

**触发条件**:
1. 查询内存使用超过限制
2. 操作符有可回收内存（`revocable_mem_size()` > 0）
3. 操作符支持 spill（`is_spillable()` = true）

**调用链**:
```
PipelineTask::_try_to_reserve_memory()
  └── 内存分配失败
      └── PipelineTask::revoke_memory()
          └── Operator::revoke_memory()
              └── 操作符特定的 revoke_memory 实现
                  └── SpillStream::spill_block()
```

**最小 Spill 大小限制**:
- `MIN_SPILL_WRITE_BATCH_MEM = 32KB`: 避免频繁的小文件写入
- `MAX_SPILL_WRITE_BATCH_MEM = 32MB`: 限制单次写入的最大数据量

**操作符的 revocable_mem_size**:
- 只计算大于 `MIN_SPILL_WRITE_BATCH_MEM` 的 block
- 如果已经 spill，返回分区 block 的内存大小
- 如果未 spill，返回内部操作符的内存使用

### 3.4 数据写入流程（Spill）

**调用链**（以 PartitionedHashJoinSink 为例）:
```
PartitionedHashJoinSinkLocalState::revoke_memory()
  └── SpillSinkRunnable::run()
      └── PartitionedHashJoinSinkLocalState::_spill_to_disk()
          └── SpillStream::spill_block()
              └── SpillWriter::write()
                  ├── 如果 Block 太大，分批写入
                  └── SpillWriter::_write_internal()
                      ├── Block::serialize() - 序列化为 PBlock（Protobuf 格式）
                      ├── PBlock::SerializeToString() - Protobuf 序列化
                      ├── 检查磁盘容量限制
                      ├── FileWriter::append() - 写入文件
                      ├── 记录 block offset（用于后续读取）
                      └── 更新统计信息
```

**写入步骤**:
1. **序列化**: Block → PBlock（Protobuf 格式）
2. **压缩**: 使用 ZSTD 压缩（压缩比更好）
3. **容量检查**: 检查磁盘容量限制（双重限制）
4. **文件写入**: 追加写入文件
5. **元数据记录**: 记录 block offset（用于后续随机访问）
6. **统计更新**: 更新各种计数器

**文件格式**:
- **数据部分**: `[Block1 Data] [Block2 Data] ... [BlockN Data]`
- **Meta 部分**: `[Block1 Offset] [Block2 Offset] ... [BlockN Offset] [Max Sub Block Size] [Block Count]`

### 3.4.1 Hash Join Build 阶段的 JoinHashTable 构建流程

**调用链**（Hash Join Build 阶段构建 Hash 表）:
```
HashJoinBuildSinkOperatorX::sink()
  └── HashJoinBuildSinkLocalState::process_build_block()
      ├── 提取 Join 键列
      │   └── _extract_join_column()
      │       └── 从 Block 中提取 Join 键对应的列
      ├── 初始化 Hash 表
      │   └── _hash_table_init()
      │       └── 根据键类型选择 Hash 表变体（HashTableVariants）
      │           ├── 整数类型 → PrimaryTypeHashTableContext
      │           ├── 字符串类型 → MethodOneString 或 SerializedHashTableContext
      │           └── 固定长度键 → FixedKeyHashTableContext
      └── ProcessHashTableBuild::run()
          ├── JoinHashTable::prepare_build()
          │   ├── 计算 bucket_size = hash_join_table_calc_bucket_size(num_elem)
          │   ├── first.resize(bucket_size + 1) - 初始化 Hash 桶数组
          │   ├── next.resize(num_elem) - 初始化链表数组
          │   └── visited.resize(num_elem) - 初始化访问标志（OUTER JOIN 需要）
          ├── 序列化 Build 键
          │   └── hash_table_ctx.init_serialized_keys()
          │       ├── 序列化键值（如果是复杂类型）
          │       └── 计算 Hash 桶编号
          │           └── bucket_nums[i] = hash(keys[i]) & (bucket_size - 1)
          └── JoinHashTable::build() - 构建链式哈希表
              └── 对每个 Build 行（i = 1 到 num_elem-1）:
                  ├── bucket_num = bucket_nums[i]
                  ├── next[i] = first[bucket_num] - 链接到链表头部
                  └── first[bucket_num] = i - 更新链表头节点
```

**关键数据结构**:
- **first[]**: `first[bucket_num]` 存储 Hash 桶 `bucket_num` 中第一个 Build 行的索引
- **next[]**: `next[build_idx]` 存储 Build 行 `build_idx` 在同一 Hash 桶中下一个 Build 行的索引
- **build_keys**: 存储 Build 键值数组的指针，用于后续的键值比较

**构建示例**:
假设 Build 表有 5 行数据（索引 1-5），Hash 值计算后：
- Build 行 1 → Hash 桶 3
- Build 行 2 → Hash 桶 1
- Build 行 3 → Hash 桶 3（与行 1 冲突）
- Build 行 4 → Hash 桶 1（与行 2 冲突）
- Build 行 5 → Hash 桶 2

构建后的数据结构：
```
first[0] = 0
first[1] = 4  → next[4] = 2  → next[2] = 0  (Hash 桶 1: 行4 → 行2)
first[2] = 5  → next[5] = 0                 (Hash 桶 2: 行5)
first[3] = 3  → next[3] = 1  → next[1] = 0  (Hash 桶 3: 行3 → 行1)
```

### 3.4.2 Hash Join Probe 阶段的 JoinHashTable 查找流程

**调用链**（Hash Join Probe 阶段查找匹配行）:
```
HashJoinProbeOperatorX::pull()
  └── ProcessHashTableProbe::process()
      ├── 初始化 Probe 侧
      │   └── _init_probe_side()
      │       ├── 序列化 Probe 键
      │       │   └── hash_table_ctx.init_serialized_keys()
      │       │       └── 计算每个 Probe 行的 Hash 桶编号
      │       │           └── bucket_nums[i] = hash(keys[i]) & (bucket_size - 1)
      │       └── JoinHashTable::pre_build_idxs() - 转换 bucket_nums 为 build_idx_map
      │           └── build_idx_map[i] = first[bucket_nums[i]]
      │               (将 Hash 桶编号转换为该桶中第一个 Build 行的索引)
      ├── JoinHashTable::find_batch() - 批量查找匹配的行
      │   ├── 根据 Join 类型路由到不同的查找方法:
      │   │   - INNER/OUTER JOIN → _find_batch_inner_outer_join()
      │   │   - LEFT SEMI/ANTI JOIN → _find_batch_left_semi_anti()
      │   │   - RIGHT SEMI/ANTI JOIN → _find_batch_right_semi_anti()
      │   │   - 有额外 Join 条件 → _find_batch_conjunct()
      │   └── 查找过程（以 _find_batch_inner_outer_join 为例）:
      │       └── 对每个 Probe 行:
      │           ├── build_idx = build_idx_map[probe_idx]
      │           ├── 遍历 Hash 桶链表:
      │           │   └── while (build_idx != 0):
      │           │       ├── 检查键值是否匹配: _eq(keys[probe_idx], build_keys[build_idx])
      │           │       ├── 如果匹配，记录到输出数组:
      │           │       │   ├── probe_idxs[matched_cnt] = probe_idx
      │           │       │   └── build_idxs[matched_cnt] = build_idx
      │           │       └── build_idx = next[build_idx] - 移动到下一个 Build 行
      │           └── 返回匹配结果:
      │               - probe_idx: 处理到的 Probe 行索引
      │               - build_idx: 处理到的 Build 行索引（用于一对多匹配）
      │               - matched_cnt: 当前批次匹配的行数
      ├── 构建输出列
      │   ├── build_side_output_column() - 从 Build 表提取匹配行的列
      │   └── probe_side_output_column() - 从 Probe 表提取匹配行的列
      └── 处理特殊 Join 条件
          ├── Mark Join → do_mark_join_conjuncts()
          └── 其他 Join 条件 → do_other_join_conjuncts()
```

**关键概念**:
- **build_idx_map**: 存储每个 Probe 行对应的 Hash 桶中第一个 Build 行的索引
  - `build_idx_map[i] = first[bucket_nums[i]]`
  - 如果 `build_idx_map[i] == 0`，表示该 Probe 行没有匹配的 Build 行
- **一对多匹配**: 一个 Probe 行可以匹配多个 Build 行（通过 `next[]` 链表遍历）
- **分批次处理**: 通过 `build_idx` 跟踪当前匹配进度，支持分批次处理大量匹配
- **查找优化**: 
  - 对于某些 Join 类型（如 LEFT SEMI JOIN），找到第一个匹配就停止
  - 使用批量查找提高性能

**查找示例**:
假设 Probe 表有键值 ["A", "B", "C"]，Build 表有键值 ["A", "A", "B"]（已构建 Hash 表）

第一次调用：probe_idx=0, build_idx=0
- Probe 行 0（键值 "A"）→ bucket_nums[0] = 3 → build_idx_map[0] = 3
- 查找 Hash 桶 3 的链表：build_idx = 3 → 匹配 Build 行 3（键值 "A"）
- 继续遍历：build_idx = next[3] = 1 → 匹配 Build 行 1（键值 "A"）
- 继续遍历：build_idx = next[1] = 0 → 结束
- 返回：probe_idx=1, build_idx=0, matched_cnt=2
- probe_idxs=[0, 0], build_idxs=[3, 1]

### 3.5 结束写入（EOF）

**调用链**:
```
操作符完成 spill
  └── SpillStream::spill_eof()
      └── SpillWriter::close()
          ├── 写入 meta 数据
          ├── 关闭文件
          └── 释放 writer
```

**要点**:
- 写入 meta 数据后才能读取
- 关闭文件后释放 writer
- 设置 `_ready_for_reading = true` 标志

### 3.6 数据读取流程（Restore）

**调用链**:
```
操作符需要读取 spill 数据
  └── SpillStream::read_next_block_sync()
      ├── 检查 _is_reading 标志（防止并发读取）
      ├── SpillReader::open() - 打开文件并读取 meta
      └── SpillReader::read()
          ├── 读取 block 数据
          ├── PBlock::ParseFromString() - Protobuf 反序列化
          └── Block::deserialize() - 反序列化为 Block
```

**读取要点**:
- 使用 `_is_reading` 标志防止并发读取
- 先打开文件读取 meta
- 然后读取 block 数据
- 支持随机访问（seek 到指定 block）

### 3.7 垃圾回收（GC）流程

#### 3.7.1 GC 后台线程

**调用链**:
```
SpillStreamManager::init()
  └── 启动 GC 线程
      └── SpillStreamManager::_spill_gc_thread_callback()
          └── 循环执行:
              ├── SpillStreamManager::gc()
              │   └── 删除 spill_gc 目录下的过期文件
              └── SpillDataDir::update_capacity()
                  └── 更新所有磁盘的容量信息
```

**GC 执行逻辑**:
1. 遍历所有 SpillDataDir
2. 列出 `spill_gc` 目录下的文件
3. 删除过期文件（每次最多工作 2 秒，避免长时间阻塞）
4. 更新磁盘容量信息

**GC 要点**:
- 定期执行（默认每 2 秒）
- 每次最多工作 2 秒（避免长时间阻塞）
- 清理 `spill_gc` 目录下的文件
- 更新磁盘容量信息

#### 3.7.2 SpillStream 销毁时的 GC

**调用链**:
```
SpillStream 析构
  └── SpillStream::gc()
      ├── 将 spill_dir 移动到 spill_gc 目录
      │   └── 使用 rename 操作（原子性）
      └── 更新 SpillDataDir 的使用量统计
```

**GC 策略**:
1. **延迟删除**: 先移动到 GC 目录，后台线程异步删除
2. **原子操作**: 使用 rename 操作，避免文件正在使用时删除
3. **容错处理**: 即使移动失败，也会更新使用量统计

---

## 4. 目录结构与管理

### 4.1 Spill 目录布局

```
storage_root/
  ├── spill/                          # 活跃 spill 数据目录
  │   └── {query_id}/                 # 按查询 ID 组织
  │       └── {operator_name}-{node_id}-{task_id}-{stream_id}/  # 操作符 spill 目录
  │           └── 0                   # spill 文件（文件名固定为 0）
  └── spill_gc/                       # GC 目录（待清理）
      └── {query_id}/                 # 按查询 ID 组织
          └── {operator_name}-{node_id}-{task_id}-{stream_id}/
              └── 0
```

**目录命名规则**:
- `{operator_name}`: 操作符名称（如 `hash_build_sink_0`, `sort`）
- `{node_id}`: Plan Node ID
- `{task_id}`: Task ID
- `{stream_id}`: Stream ID（全局唯一）

**目录常量**:
- `SPILL_DIR_PREFIX`: `"spill"`
- `SPILL_GC_DIR_PREFIX`: `"spill_gc"`

---

## 5. 容量管理与限制

### 5.1 容量限制检查

**双重限制机制**:
1. **磁盘容量限制**: 
   - 磁盘使用率 >= `storage_flood_stage_usage_percent`
   - 且剩余容量 <= `storage_flood_stage_left_capacity_bytes`
2. **Spill 数据限制**: 
   - Spill 数据总量 > `spill_storage_limit` 配置

**检查调用链**:
```
SpillWriter::_write_internal()
  └── SpillDataDir::reach_capacity_limit()
      ├── _reach_disk_capacity_limit() - 检查磁盘容量限制
      └── 检查 Spill 数据限制
```

### 5.2 容量更新

**更新时机**:
- GC 线程定期更新
- SpillDataDir::init() 时更新

**更新调用链**:
```
SpillDataDir::update_capacity()
  ├── 获取磁盘空间信息 (get_space_info)
  ├── 计算 spill 数据限制
  │   ├── 解析 spill_storage_limit 配置
  │   ├── 如果是百分比，乘以 storage_flood_stage_usage_percent
  │   └── 不能超过磁盘使用上限
  └── 更新监控指标
```

**容量限制计算规则**:
1. 如果 `spill_storage_limit` 是百分比，最终限制 = `磁盘容量 × storage_flood_stage_usage_percent × spill_storage_limit`
2. 如果 `spill_storage_limit` 是绝对值，直接使用该值
3. 最终限制不能超过 `磁盘容量 × storage_flood_stage_usage_percent`

### 5.3 使用量统计

**更新调用链**:
```
SpillWriter::_write_internal()
  └── SpillDataDir::update_spill_data_usage()
      ├── 更新 _spill_data_bytes（线程安全）
      └── 更新监控指标

SpillStream::gc()
  └── SpillDataDir::update_spill_data_usage(-total_written_bytes_)
```

**要点**:
- 线程安全（使用 mutex）
- 实时更新监控指标
- 写入和删除时都会更新

---

## 6. 使用 Spill 的操作符

### 6.1 支持 Spill 的操作符

#### 1. PartitionedHashJoinSink - Hash Join Build 阶段
- **文件**: `be/src/pipeline/exec/partitioned_hash_join_sink_operator.cpp`
- **场景**: 当 build 数据超过内存限制时，按分区 spill
- **特点**: 支持分区 spill，每个分区一个 stream

**Hash Join Build 阶段的完整调用链**:
```
HashJoinBuildSinkOperatorX::sink()
  └── HashJoinBuildSinkLocalState::process_build_block()
      ├── 提取 Join 键列 (_extract_join_column)
      ├── 初始化 Hash 表 (_hash_table_init)
      └── ProcessHashTableBuild::run()
          ├── JoinHashTable::prepare_build() - 准备 Hash 表结构
          │   ├── 计算 bucket_size（Hash 桶大小）
          │   ├── 初始化 first[] 数组（每个 Hash 桶的第一个 Build 行索引）
          │   └── 初始化 next[] 数组（链式哈希的链表）
          ├── 序列化 Build 键 (init_serialized_keys)
          │   └── 计算每个 Build 行的 Hash 桶编号 (bucket_nums)
          └── JoinHashTable::build() - 构建链式哈希表
              └── 对每个 Build 行（从索引 1 开始）:
                  ├── 获取 Hash 桶编号 (bucket_num = bucket_nums[i])
                  ├── next[i] = first[bucket_num] - 链接到链表头部
                  └── first[bucket_num] = i - 更新链表头节点
```

**JoinHashTable 构建说明**:
- **数据结构**: 使用链式哈希（Chained Hashing）处理 Hash 冲突
  - `first[bucket_num]`: Hash 桶 `bucket_num` 中第一个 Build 行的索引
  - `next[build_idx]`: Build 行 `build_idx` 在同一 Hash 桶中下一个 Build 行的索引
- **构建方式**: 使用头插法，新插入的行成为链表的头节点
- **存储位置**: Hash 表存储在 `HashJoinSharedState::hash_table_variant_vector` 中
- **共享机制**: Broadcast Join 模式下，只有 Task 0 构建 Hash 表，其他 Task 共享

#### 2. PartitionedHashJoinProbe - Hash Join Probe 阶段
- **文件**: `be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp`
- **场景**: Probe 阶段也可能需要 spill probe 数据
- **特点**: 支持 probe 数据 spill

**Hash Join Probe 阶段的完整调用链**:
```
HashJoinProbeOperatorX::pull()
  └── ProcessHashTableProbe::process()
      ├── 初始化 Probe 侧 (_init_probe_side)
      │   ├── 序列化 Probe 键 (init_serialized_keys)
      │   ├── 计算 Hash 桶编号 (init_join_bucket_num)
      │   └── JoinHashTable::pre_build_idxs() - 转换 bucket_nums 为 build_idx_map
      │       └── build_idx_map[i] = first[bucket_nums[i]]
      │           (将 Hash 桶编号转换为该桶中第一个 Build 行的索引)
      ├── JoinHashTable::find_batch() - 批量查找匹配的行
      │   ├── 根据 Join 类型路由到不同的查找方法:
      │   │   - INNER/OUTER JOIN → _find_batch_inner_outer_join()
      │   │   - LEFT SEMI/ANTI JOIN → _find_batch_left_semi_anti()
      │   │   - RIGHT SEMI/ANTI JOIN → _find_batch_right_semi_anti()
      │   │   - 有额外 Join 条件 → _find_batch_conjunct()
      │   └── 返回匹配结果:
      │       - probe_idx: 处理到的 Probe 行索引
      │       - build_idx: 处理到的 Build 行索引（用于一对多匹配）
      │       - matched_cnt: 当前批次匹配的行数
      ├── 构建输出列
      │   ├── build_side_output_column() - 从 Build 表提取匹配行的列
      │   └── probe_side_output_column() - 从 Probe 表提取匹配行的列
      └── 处理特殊 Join 条件
          ├── Mark Join → do_mark_join_conjuncts()
          └── 其他 Join 条件 → do_other_join_conjuncts()
```

**JoinHashTable 查找说明**:
- **build_idx_map**: 存储每个 Probe 行对应的 Hash 桶中第一个 Build 行的索引
  - `build_idx_map[i] = first[bucket_nums[i]]`
  - 如果 `build_idx_map[i] == 0`，表示该 Probe 行没有匹配的 Build 行
- **一对多匹配**: 一个 Probe 行可以匹配多个 Build 行（通过 `next[]` 链表遍历）
- **分批次处理**: 通过 `build_idx` 跟踪当前匹配进度，支持分批次处理大量匹配
- **查找优化**: 
  - 对于某些 Join 类型（如 LEFT SEMI JOIN），找到第一个匹配就停止
  - 使用批量查找提高性能

#### 3. SpillSortSink - Sort 操作
- **文件**: `be/src/pipeline/exec/spill_sort_sink_operator.cpp`
- **场景**: 当排序数据超过内存限制时 spill
- **特点**: 支持多路归并排序

#### 4. PartitionedAggregationSink - Aggregation 操作
- **文件**: `be/src/pipeline/exec/partitioned_aggregation_sink_operator.cpp`
- **场景**: 当聚合数据超过内存限制时，按分区 spill
- **特点**: 支持分区 spill

### 6.2 操作符使用模式

**通用模式**:
1. **Open 阶段**: 
   - 注册 SpillStream（如果可能 spill）
   - 初始化内部状态

2. **执行阶段**: 
   - 检查内存使用
   - 如果超过限制，调用 `spill_block()` 写入磁盘
   - 释放内存

3. **读取阶段**: 
   - 调用 `read_next_block_sync()` 从磁盘读取
   - 处理数据

4. **Close 阶段**: 
   - 调用 `spill_eof()` 结束写入
   - GC 清理文件

---

## 7. 配置参数详解

### 7.1 BE 配置参数

#### spill_storage_root_path
- **类型**: String
- **默认值**: `""`
- **说明**: Spill 存储根路径，如果为空则使用数据目录
- **示例**: `"/data/spill"`

#### spill_storage_limit
- **类型**: String
- **默认值**: `"20%"`
- **说明**: Spill 存储限制，支持百分比或绝对值
  - 百分比格式: `"20%"`（相对于磁盘容量）
  - 绝对值格式: `"10G"`, `"1024M"`, `"1073741824"`（字节）
- **计算规则**: 
  - 如果是指百分比，最终限制 = `磁盘容量 × storage_flood_stage_usage_percent × spill_storage_limit`
  - 如果是绝对值，直接使用该值
  - 最终限制不能超过 `磁盘容量 × storage_flood_stage_usage_percent`

#### spill_gc_interval_ms
- **类型**: Int32
- **默认值**: `2000` (2 秒)
- **说明**: GC 线程执行间隔（毫秒）

#### spill_gc_work_time_ms
- **类型**: Int32
- **默认值**: `2000` (2 秒)
- **说明**: GC 单次最大工作时间（毫秒），避免长时间阻塞

#### storage_flood_stage_usage_percent
- **类型**: Int32
- **默认值**: `95`
- **说明**: 磁盘使用率告警阈值（百分比）

#### storage_flood_stage_left_capacity_bytes
- **类型**: Int64
- **默认值**: `1073741824` (1GB)
- **说明**: 磁盘剩余容量告警阈值（字节）

### 7.2 Session 变量

#### enable_spill
- **类型**: Boolean
- **默认值**: `false`
- **说明**: 是否启用查询算子落盘
- **设置**: `SET enable_spill = true;`

#### enable_force_spill
- **类型**: Boolean
- **默认值**: `false`
- **说明**: 是否开启强制落盘（即使在内存足够的情况）
- **用途**: 测试和调试

#### enable_reserve_memory
- **类型**: Boolean
- **默认值**: `true`
- **说明**: 是否启用分配内存前先 reserve memory 的功能

#### spill_min_revocable_mem
- **类型**: Long
- **默认值**: `33554432` (32MB)
- **说明**: 触发 spill 的最小可回收内存大小

#### spill_sort_mem_limit
- **类型**: Long
- **默认值**: `134217728` (128MB)
- **说明**: Sort 操作 merge 阶段的内存限制

#### spill_sort_batch_bytes
- **类型**: Long
- **默认值**: `8388608` (8MB)
- **说明**: Sort 操作 spill 时的 batch 大小

#### spill_aggregation_partition_count
- **类型**: Int
- **默认值**: `32`
- **说明**: Aggregation 操作 spill 时的分区数

#### spill_hash_join_partition_count
- **类型**: Int
- **默认值**: `32`
- **说明**: Hash Join 操作 spill 时的分区数

---

## 8. 监控指标

### 8.1 SpillStreamManager 指标

- `spill_write_bytes` - 总写入字节数（Counter）
- `spill_read_bytes` - 总读取字节数（Counter）

### 8.2 SpillDataDir 指标（每个磁盘目录）

- `spill_disk_capacity` - 磁盘总容量（Gauge）
- `spill_disk_limit` - Spill 数据限制（Gauge）
- `spill_disk_avail_capacity` - 可用容量（Gauge）
- `spill_disk_data_size` - Spill 数据大小（Gauge）
- `spill_disk_has_spill_data` - 是否有活跃 spill 数据（0/1）
- `spill_disk_has_spill_gc_data` - 是否有待 GC 数据（0/1）

### 8.3 操作符 Profile 指标

**写入相关**:
- `SpillWriteFileTime` - 文件写入时间
- `SpillWriteSerializeBlockTime` - Block 序列化时间
- `SpillWriteBlockCount` - 写入 Block 数量
- `SpillWriteBlockBytes` - 写入 Block 字节数（未压缩）
- `SpillWriteFileBytes` - 写入文件总字节数（压缩后）
- `SpillWriteFileCurrentBytes` - 当前文件字节数
- `SpillWriteRows` - 写入行数

**读取相关**:
- `SpillReadFileTime` - 文件读取时间
- `SpillReadDerializeBlockTime` - Block 反序列化时间
- `SpillReadBlockCount` - 读取 Block 数量
- `SpillReadBlockBytes` - 读取 Block 字节数（压缩后）
- `SpillReadFileBytes` - 读取文件总字节数
- `SpillReadRows` - 读取行数
- `SpillReadFileCount` - 读取文件数量

**查看方式**:
- 通过 `SHOW PROFILE` 查看查询 Profile
- 通过 Prometheus 监控指标（如果启用）

---

## 9. 性能优化建议

### 9.1 磁盘选择

1. **优先使用 SSD**: 配置 SSD 作为 spill 存储路径
2. **多磁盘负载均衡**: 配置多个 spill 路径，系统会自动选择使用率低的磁盘
3. **避免系统盘**: 不要使用系统盘作为 spill 路径

### 9.2 容量配置

1. **合理设置 spill_storage_limit**: 
   - 太小：容易触发容量限制，查询失败
   - 太大：可能影响其他系统组件
   - 建议：20%-30% 磁盘容量

2. **监控磁盘使用率**: 
   - 定期检查 `spill_disk_data_size` 指标
   - 确保有足够的磁盘空间

### 9.3 查询优化

1. **避免不必要的 Spill**: 
   - 增加查询内存限制
   - 优化查询计划，减少中间结果大小

2. **合理设置分区数**: 
   - Hash Join: `spill_hash_join_partition_count`（默认 32）
   - Aggregation: `spill_aggregation_partition_count`（默认 32）
   - 分区数太少：每个分区数据量大，spill 效率低
   - 分区数太多：管理开销大

3. **调整 Batch 大小**: 
   - Sort: `spill_sort_batch_bytes`（默认 8MB）
   - 太小：I/O 次数多
   - 太大：内存占用高

### 9.4 GC 优化

1. **调整 GC 间隔**: 
   - `spill_gc_interval_ms`（默认 2 秒）
   - 间隔太短：GC 开销大
   - 间隔太长：磁盘空间释放慢

2. **调整 GC 工作时间**: 
   - `spill_gc_work_time_ms`（默认 2 秒）
   - 控制单次 GC 的最大工作时间，避免长时间阻塞

### 9.5 监控和调优

1. **关注 Profile 指标**: 
   - `SpillWriteFileTime`: 写入时间过长可能表示磁盘 I/O 瓶颈
   - `SpillReadFileTime`: 读取时间过长可能影响查询性能

2. **压缩效果**: 
   - 关注 `SpillWriteBlockBytes` vs `SpillWriteFileBytes`
   - 压缩比 = `SpillWriteBlockBytes / SpillWriteFileBytes`
   - 压缩比低可能表示数据不可压缩（如已压缩的数据）

---

## 10. 故障排查

### 10.1 常见错误

#### 1. "no available disk can be used for spill"
**原因**:
- 所有磁盘都达到容量限制
- 没有配置 spill 存储路径

**解决方法**:
- 检查磁盘空间：`df -h`
- 检查 `spill_disk_data_size` 和 `spill_disk_limit` 指标
- 清理 spill_gc 目录
- 增加 `spill_storage_limit` 配置

#### 2. "spill data total size exceed limit"
**原因**:
- Spill 数据总量超过 `spill_storage_limit` 限制

**解决方法**:
- 检查 `spill_disk_data_size` 指标
- 等待 GC 清理完成
- 增加 `spill_storage_limit` 配置
- 优化查询，减少 spill 数据量

#### 3. "reach capacity limit"
**原因**:
- 磁盘使用率超过 `storage_flood_stage_usage_percent`
- 磁盘剩余容量小于 `storage_flood_stage_left_capacity_bytes`

**解决方法**:
- 清理磁盘空间
- 调整 `storage_flood_stage_usage_percent` 配置（不推荐）
- 增加磁盘容量

#### 4. "create spill dir failed"
**原因**:
- 磁盘权限问题
- 磁盘空间不足
- 文件系统错误

**解决方法**:
- 检查目录权限：`ls -ld /path/to/spill`
- 检查磁盘空间：`df -h`
- 检查文件系统：`dmesg | grep -i error`

### 10.2 调试方法

#### 1. 启用 Debug 日志
```sql
SET enable_debug_log = true;
```

#### 2. 查看 Spill 相关日志
```bash
grep -i "spill" be/log/be.INFO
```

#### 3. 检查 Spill 目录
```bash
# 查看活跃 spill 数据
ls -lh /path/to/storage/spill/

# 查看待 GC 数据
ls -lh /path/to/storage/spill_gc/

# 统计 spill 数据大小
du -sh /path/to/storage/spill/
du -sh /path/to/storage/spill_gc/
```

#### 4. 查看监控指标
```bash
# 通过 HTTP 接口查看指标（如果启用）
curl http://be_host:8040/metrics | grep spill
```

#### 5. 查看查询 Profile
```sql
SHOW PROFILE FOR QUERY <query_id>;
```

### 10.3 性能问题排查

#### 1. Spill 写入慢
- 检查磁盘 I/O：`iostat -x 1`
- 检查 `SpillWriteFileTime` 指标
- 考虑使用 SSD

#### 2. Spill 读取慢
- 检查磁盘 I/O：`iostat -x 1`
- 检查 `SpillReadFileTime` 指标
- 检查压缩比（压缩比低可能表示数据不可压缩）

#### 3. GC 清理慢
- 检查 `spill_gc_work_time_ms` 配置
- 检查 GC 线程是否正常运行
- 手动清理 `spill_gc` 目录（谨慎操作）

---

## 11. 总结

### 11.1 关键设计点

1. **分层设计**: SpillStreamManager -> SpillDataDir -> SpillStream -> SpillWriter/SpillReader
2. **异步 GC**: 后台线程定期清理过期文件，避免阻塞查询
3. **容量管理**: 双重限制（磁盘容量 + Spill 数据限制），防止磁盘写满
4. **目录隔离**: 按查询 ID 和操作符组织目录，便于管理和清理
5. **压缩存储**: 使用 ZSTD 压缩，节省磁盘空间
6. **随机访问**: 支持 seek，便于操作符按需读取
7. **线程安全**: 使用 mutex 保护共享数据
8. **容错处理**: 完善的错误处理和日志记录

### 11.2 完整调用链总结

**初始化**:
```
ExecEnv::_init() 
  → SpillStreamManager::init() 
    → SpillDataDir::init() 
      → SpillDataDir::update_capacity()
```

**注册流**:
```
操作符::open() 
  → SpillStreamManager::register_spill_stream() 
    → SpillStream::prepare() 
      → SpillWriter::open()
```

**写入数据**:
```
内存分配失败 
  → PipelineTask::revoke_memory() 
    → 操作符::revoke_memory() 
      → SpillStream::spill_block() 
        → SpillWriter::write() 
          → SpillWriter::_write_internal() 
            → FileWriter::append()
```

**读取数据**:
```
操作符::read() 
  → SpillStream::read_next_block_sync() 
    → SpillReader::open() 
      → SpillReader::read()
```

**Hash Join Build 阶段（构建 Hash 表）**:
```
HashJoinBuildSinkOperatorX::sink()
  → HashJoinBuildSinkLocalState::process_build_block()
    → ProcessHashTableBuild::run()
      → JoinHashTable::prepare_build() - 准备 Hash 表结构
      → hash_table_ctx.init_serialized_keys() - 序列化键并计算 Hash 桶编号
      → JoinHashTable::build() - 构建链式哈希表
        → 对每个 Build 行: next[i] = first[bucket_num], first[bucket_num] = i
```

**Hash Join Probe 阶段（查找匹配行）**:
```
HashJoinProbeOperatorX::pull()
  → ProcessHashTableProbe::process()
    → _init_probe_side() - 初始化 Probe 侧
      → hash_table_ctx.init_serialized_keys() - 序列化 Probe 键
      → JoinHashTable::pre_build_idxs() - 转换 bucket_nums 为 build_idx_map
    → JoinHashTable::find_batch() - 批量查找匹配行
      → _find_batch_inner_outer_join() / _find_batch_left_semi_anti() / ...
        → 遍历 Hash 桶链表查找匹配
    → build_side_output_column() - 构建 Build 侧输出列
    → probe_side_output_column() - 构建 Probe 侧输出列
```

**清理**:
```
SpillStream::~SpillStream() 
  → SpillStream::gc() 
    → 移动到 spill_gc 目录

后台 GC 线程 
  → SpillStreamManager::_spill_gc_thread_callback() 
    → SpillStreamManager::gc() 
      → 删除 spill_gc 目录
```

### 11.3 最佳实践

1. **配置**: 
   - 使用 SSD 作为 spill 存储
   - 合理设置 `spill_storage_limit`（20%-30%）
   - 配置多个 spill 路径实现负载均衡

2. **监控**: 
   - 定期检查磁盘使用率
   - 监控 spill 读写指标
   - 关注 GC 执行情况

3. **优化**: 
   - 优化查询计划，减少 spill 数据量
   - 合理设置分区数和 batch 大小
   - 避免不必要的 spill

4. **故障处理**: 
   - 及时清理 spill_gc 目录
   - 监控磁盘空间
   - 查看日志和 Profile 定位问题

---

## 12. 参考资料

### 12.1 核心文件

- **SpillStreamManager**: `be/src/vec/spill/spill_stream_manager.h/cpp`
- **SpillStream**: `be/src/vec/spill/spill_stream.h/cpp`
- **SpillWriter**: `be/src/vec/spill/spill_writer.h/cpp`
- **SpillReader**: `be/src/vec/spill/spill_reader.h/cpp`

### 12.2 操作符实现

- **PartitionedHashJoinSink**: `be/src/pipeline/exec/partitioned_hash_join_sink_operator.cpp`
- **PartitionedHashJoinProbe**: `be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp`
- **HashJoinBuildSink**: `be/src/pipeline/exec/hashjoin_build_sink.cpp` 和 `be/src/pipeline/exec/hashjoin_build_sink.h`
- **HashJoinProbe**: `be/src/pipeline/exec/hashjoin_probe_operator.cpp` 和 `be/src/pipeline/exec/hashjoin_probe_operator.h`
- **SpillSortSink**: `be/src/pipeline/exec/spill_sort_sink_operator.cpp`
- **PartitionedAggregationSink**: `be/src/pipeline/exec/partitioned_aggregation_sink_operator.cpp`

### 12.3 Hash Join 核心组件

- **JoinHashTable**: `be/src/vec/common/hash_table/join_hash_table.h`
  - 核心 Hash 表实现，使用链式哈希处理冲突
  - 提供 `build()` 方法构建 Hash 表
  - 提供 `find_batch()` 方法批量查找匹配行
- **ProcessHashTableBuild**: `be/src/pipeline/exec/hashjoin_build_sink.h`
  - Hash 表构建的模板结构体
  - 负责调用 `JoinHashTable::build()` 构建 Hash 表
- **ProcessHashTableProbe**: `be/src/pipeline/exec/join/process_hash_table_probe.h` 和 `be/src/pipeline/exec/join/process_hash_table_probe_impl.h`
  - Hash 表查找的模板结构体
  - 负责调用 `JoinHashTable::find_batch()` 查找匹配行
- **HashTableVariants**: `be/src/pipeline/common/join_utils.h`
  - Hash 表变体定义，根据键类型选择不同的 Hash 表实现

### 12.4 配置和初始化

- **ExecEnv**: `be/src/runtime/exec_env.h` 和 `be/src/runtime/exec_env_init.cpp`
- **配置参数**: `be/src/common/config.h` 和 `be/src/common/config.cpp`
- **Session 变量**: `fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java`

---

**文档版本**: v2.0  
**最后更新**: 2024  
**维护者**: Doris 开发团队
