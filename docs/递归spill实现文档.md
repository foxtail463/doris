# Doris 递归 Spill 实现文档

## 文档说明

本文档记录了递归 Spill 的完整实现思路、设计细节和代码结构，方便后续重新实现时参考。

**当前状态**：设计文档已完成，代码实现已撤销，等待重新实现。

---

## 目录

1. [问题背景](#1-问题背景)
2. [解决方案设计](#2-解决方案设计)
3. [核心数据结构](#3-核心数据结构)
4. [关键算法](#4-关键算法)
5. [集成方案](#5-集成方案)
6. [实现步骤](#6-实现步骤)
7. [注意事项](#7-注意事项)

---

## 1. 问题背景

### 1.1 Issue 描述

参考：[GitHub Issue #59194](https://github.com/apache/doris/issues/59194)

**核心问题**：
- 当前 Doris 的磁盘溢出机制通过将哈希表分割成固定数量的分区（例如16个）进行磁盘写入
- **没有实现递归机制**
- 当内存真正不足时，实际最大内存占用只能减少到 15/16
- **目标**：在只有 10GB 内存的情况下运行所有 TPC-DS 1TB 查询

### 1.2 问题示例

**场景**：
- 总数据量：100GB
- 分区数量：16 个
- 每个分区大小：约 6.25GB
- 可用内存：10GB

**当前实现的限制**：
```
Partition 0: 6.25GB → 无法放入 10GB 内存（需要构建 Hash 表）
Partition 1: 6.25GB → 无法放入 10GB 内存
...
Partition 15: 6.25GB → 无法放入 10GB 内存
```

**问题**：单个分区（6.25GB）仍然太大，无法放入内存进行 Hash Join 构建。

---

## 2. 解决方案设计

### 2.1 核心思想：递归分区树

**多层分区结构**：
```
输入数据（100GB）
  │
  ├─→ Level 0: 分成 16 个分区
  │   │
  │   ├─→ Partition 0 (6.25GB) - 仍然太大
  │   │   │
  │   │   ├─→ Level 1: 进一步分成 16 个子分区
  │   │   │   │
  │   │   │   ├─→ Sub-Partition 0-0 (390MB) ✅ 可以放入内存
  │   │   │   ├─→ Sub-Partition 0-1 (390MB) ✅ 可以放入内存
  │   │   │   ├─→ ...
  │   │   │   └─→ Sub-Partition 0-15 (390MB) ✅ 可以放入内存
  │   │   │
  │   │   └─→ Partition 0-1 (6.25GB) - 仍然太大
  │   │       └─→ Level 2: 进一步分割...
  │   │
  │   └─→ Partition 1 (6.25GB) - 仍然太大
  │       └─→ ...
```

### 2.2 设计要点

1. **递归分割**：当单个分区仍然太大时，继续分割为子分区
2. **分区树结构**：使用树结构管理多层级分区
3. **内存限制检查**：每个分区检查是否超过内存限制
4. **叶子节点 Spill**：只有叶子节点（足够小的分区）才进行 Spill
5. **向后兼容**：保留原有单层分区逻辑

---

## 3. 核心数据结构

### 3.1 PartitionNode（分区树节点）

**定义位置**：`be/src/pipeline/exec/recursive_partition_manager.h`

**关键成员**：
```cpp
class PartitionNode {
    uint32_t level;                    // 分区层级（0 为根层级）
    uint32_t index_in_level;          // 在当前层级中的索引
    PartitionNode* parent;            // 父节点指针
    bool is_leaf;                     // 是否为叶子节点
    uint64_t partition_id;            // 编码后的分区 ID
    size_t estimated_size;            // 预估大小（字节）
    size_t actual_size;               // 实际大小（字节）
    size_t row_count;                 // 行数
    std::vector<std::unique_ptr<PartitionNode>> sub_partitions;  // 子分区列表
    vectorized::SpillStreamSPtr spill_stream;  // 关联的 SpillStream
};
```

**关键方法**：
- `check_needs_recursive_split(size_t mem_limit)`: 检查是否需要递归分割
- `get_leaf_nodes(std::vector<PartitionNode*>& leaves)`: 获取所有叶子节点
- `get_path_string()`: 获取分区路径字符串（用于调试）

### 3.2 RecursivePartitionManager（递归分区管理器）

**定义位置**：
- 头文件：`be/src/pipeline/exec/recursive_partition_manager.h`
- 实现文件：`be/src/pipeline/exec/recursive_partition_manager.cpp`

**关键成员**：
```cpp
class RecursivePartitionManager {
    std::vector<std::unique_ptr<PartitionNode>> _root_partitions;  // 根分区列表
    uint32_t _partition_count;          // 每层的分区数（如 16）
    size_t _mem_limit;                   // 单个分区的最大大小（内存限制）
    uint32_t _max_level;                 // 最大递归层级（防止无限递归）
    std::vector<TExpr> _partition_exprs; // 分区表达式
};
```

**关键方法**：
- `init_root_partitions()`: 初始化根分区
- `split_partition_recursively()`: 递归分割分区
- `get_leaf_partitions()`: 获取所有叶子分区
- `find_partition()`: 根据分区 ID 查找节点
- `create_partitioner()`: 创建 Partitioner（不同层级使用不同 hash seed）

### 3.3 分区 ID 编码/解码

**编码方案**：多级编码，每个层级占用 8 位
```
Level 0: Partition 0  → ID = 0x00
         Partition 1  → ID = 0x01
         Partition 15 → ID = 0x0F

Level 1: Partition 0-0 → ID = 0x0000 (0 << 8 | 0)
         Partition 0-1 → ID = 0x0001 (0 << 8 | 1)
         Partition 1-0 → ID = 0x0100 (1 << 8 | 0)

Level 2: Partition 0-1-2 → ID = 0x000102 ((0 << 8 | 1) << 8 | 2)
```

**函数**：
```cpp
uint64_t encode_partition_id(const std::vector<uint32_t>& indices);
std::vector<uint32_t> decode_partition_id(uint64_t id, uint32_t level);
```

---

## 4. 关键算法

### 4.1 递归分割算法

**函数**：`RecursivePartitionManager::split_partition_recursively()`

**流程**：
1. **检查终止条件**
   - 达到最大层级 → 强制 Spill
   - 分区足够小 → 标记为叶子节点并处理

2. **创建子分区**
   - 创建 `_partition_count` 个子分区节点
   - 设置父子关系

3. **使用新的 Partitioner 分割数据**
   - 创建子层级的 Partitioner（使用不同的 hash seed）
   - 执行分区操作

4. **将数据分配到子分区**
   - 根据分区结果提取每个子分区的数据
   - 更新子分区的信息（大小、行数）

5. **递归处理每个子分区**
   - 对每个子分区递归调用 `split_partition_recursively()`

### 4.2 内存限制计算

**公式**：
```
单个分区内存限制 = 查询内存限制 / 分区数 / 2
```

**考虑因素**：
- Hash 表构建开销（通常数据大小的 1.5-2 倍）
- 预留安全边距
- 最小限制（如 32MB，避免过度分割）

**示例**：
- 查询限制：10GB
- 分区数：16
- 单个分区限制：10GB / 16 / 2 = 312.5MB

---

## 5. 集成方案

### 5.1 修改 PartitionedHashJoinSinkLocalState

**需要添加的成员**：
```cpp
std::unique_ptr<RecursivePartitionManager> _recursive_partition_manager;
bool _enable_recursive_spill {true};  // 可通过配置控制
```

**初始化位置**：`init()` 方法中
```cpp
// 计算内存限制
size_t query_mem_limit = state->query_options().mem_limit;
size_t partition_mem_limit = query_mem_limit / partition_count / 2;

// 创建递归分区管理器
_recursive_partition_manager = std::make_unique<RecursivePartitionManager>(
    partition_count, partition_mem_limit, 10);

// 初始化根分区
RETURN_IF_ERROR(_recursive_partition_manager->init_root_partitions(
    state, partition_exprs));
```

### 5.2 修改 revoke_memory 方法

**流程**：
1. 检查是否启用递归 Spill
2. 遍历所有根分区
3. 检查分区大小
4. 如果超过限制 → 调用 `split_partition_recursively()`
5. 否则 → 直接 Spill
6. 处理所有叶子分区

**代码结构**：
```cpp
if (_enable_recursive_spill && _recursive_partition_manager) {
    // 递归分区逻辑
    for (每个根分区) {
        if (需要递归分割) {
            split_partition_recursively();
        } else {
            spill_to_disk();
        }
    }
    // 处理叶子分区
} else {
    // 原有单层分区逻辑（向后兼容）
}
```

### 5.3 修改 Probe 阶段

**需要修改的文件**：`be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp`

**关键方法**：`recover_build_blocks_from_disk()`

**流程**：
1. 获取所有叶子分区
2. 对每个叶子分区：
   - 从 SpillStream 读取数据
   - 构建 Hash 表
   - 执行 Probe 操作

---

## 6. 实现步骤

### Phase 1: 基础数据结构（已完成设计）

1. ✅ 创建 `PartitionNode` 类
2. ✅ 创建 `RecursivePartitionManager` 类
3. ✅ 实现分区 ID 编码/解码函数

### Phase 2: 核心算法（已完成设计）

1. ✅ 实现 `split_partition_recursively()` 算法
2. ✅ 实现 `find_partition()` 查找算法
3. ✅ 实现 `create_partitioner()` 方法

### Phase 3: 集成到 Sink 阶段（待实现）

1. ⏳ 修改 `PartitionedHashJoinSinkLocalState`
   - 添加递归分区管理器成员
   - 在 `init()` 中初始化
   - 修改 `revoke_memory()` 使用递归分割

2. ⏳ 测试 Sink 阶段
   - 测试小数据量（不需要递归）
   - 测试大数据量（需要递归）

### Phase 4: 集成到 Probe 阶段（待实现）

1. ⏳ 修改 `PartitionedHashJoinProbeLocalState`
   - 支持从递归分区读取
   - 遍历所有叶子分区

2. ⏳ 测试 Probe 阶段
   - 测试数据读取正确性
   - 测试 Join 结果正确性

### Phase 5: 优化和完善（待实现）

1. ⏳ 实现 Partitioner 的 hash seed 支持
2. ⏳ 性能优化
3. ⏳ 添加配置参数
4. ⏳ 完善错误处理
5. ⏳ 添加单元测试和集成测试

---

## 7. 注意事项

### 7.1 技术难点

1. **Partitioner 的 Hash Seed**
   - 问题：不同层级需要使用不同的 hash seed，避免冲突
   - 状态：TODO，需要修改 Partitioner 以支持自定义 seed
   - 建议：在 `create_partitioner()` 中添加 seed 参数

2. **内存限制的确定**
   - 问题：如何准确计算单个分区的内存限制？
   - 建议：
     - 使用查询内存限制（query_mem_limit）
     - 考虑 Hash 表构建开销（1.5-2 倍）
     - 预留安全边距

3. **向后兼容**
   - 问题：如何保证原有单层分区逻辑仍然可用？
   - 建议：通过 `_enable_recursive_spill` 标志控制，默认启用

### 7.2 性能考虑

1. **递归深度**
   - 设置最大层级限制（如 10 层），防止无限递归
   - 监控递归深度，避免过度分割

2. **磁盘 I/O**
   - 递归分割会增加磁盘写入次数
   - 需要平衡内存使用和 I/O 开销

3. **内存使用**
   - 递归分割过程中会临时占用内存
   - 需要及时释放已分割的数据

### 7.3 测试要点

1. **功能测试**
   - 小数据量（不需要递归）
   - 中等数据量（需要一层递归）
   - 大数据量（需要多层递归）
   - 边界情况（达到最大层级）

2. **性能测试**
   - 对比单层分区和递归分区的性能
   - 测试内存使用情况
   - 测试磁盘 I/O 开销

3. **正确性测试**
   - 验证 Join 结果的正确性
   - 验证数据完整性

---

## 8. 参考文档

- [递归 Spill 实现思路](./递归spill实现思路.md) - 详细的设计文档
- [递归 Spill 实现进度](./递归spill实现进度.md) - 实现进度跟踪
- [GitHub Issue #59194](https://github.com/apache/doris/issues/59194) - 原始 Issue

---

## 9. 文件清单

### 需要创建的文件

1. `be/src/pipeline/exec/recursive_partition_manager.h` - 头文件
2. `be/src/pipeline/exec/recursive_partition_manager.cpp` - 实现文件

### 需要修改的文件

1. `be/src/pipeline/exec/partitioned_hash_join_sink_operator.h` - 添加成员变量
2. `be/src/pipeline/exec/partitioned_hash_join_sink_operator.cpp` - 修改 `init()` 和 `revoke_memory()`
3. `be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp` - 修改 Probe 阶段

---

**最后更新**：2024年（代码实现已撤销，保留设计文档）

