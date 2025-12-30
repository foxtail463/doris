# Doris 递归 Spill 实现思路

## 目录
1. [问题背景](#1-问题背景)
2. [当前实现分析](#2-当前实现分析)
3. [解决方案设计](#3-解决方案设计)
4. [核心数据结构](#4-核心数据结构)
5. [关键算法](#5-关键算法)
6. [实现步骤](#6-实现步骤)
7. [注意事项](#7-注意事项)
8. [参考链接](#8-参考链接)

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

## 2. 当前实现分析

### 2.1 现有架构

**单层分区结构**：
```
输入数据
  │
  ├─→ Partitioner（分成 16 个分区）
  │
  ├─→ Partition 0 → SpillStream 0
  ├─→ Partition 1 → SpillStream 1
  ├─→ ...
  └─→ Partition 15 → SpillStream 15
```

**关键代码位置**：
- `be/src/pipeline/exec/partitioned_hash_join_sink_operator.cpp`
- `be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp`

### 2.2 当前实现的限制

1. **固定分区数**：分区数量在初始化时确定（如 16 个），无法动态调整
2. **单层分割**：只能进行一次分区，无法递归分割
3. **内存限制**：如果单个分区仍然太大，无法进一步处理

**内存占用分析**：
```
总数据：100GB
分成 16 个分区：每个 6.25GB
最大内存占用：15/16 × 100GB = 93.75GB（仍然太大）
```

---

## 3. 解决方案设计

### 3.1 核心思想：递归分区树

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
  │   │   └─→ 如果某个子分区仍然太大，继续递归...
  │   │
  │   └─→ Partition 1-15（类似处理）
  │
  └─→ 递归直到所有叶子分区都能放入内存
```

### 3.2 设计原则

1. **动态层级**：根据数据大小和内存限制动态决定分区层级
2. **延迟分割**：只有在真正需要时才进行递归分割
3. **内存感知**：实时监控分区大小，确保不超过内存限制
4. **性能优化**：避免不必要的递归分割，减少开销

### 3.3 内存占用优化

**递归分割后的内存占用**：
```
Level 0: 16 个分区，每个 6.25GB
  └─→ Level 1: 16 × 16 = 256 个子分区，每个 390MB
      └─→ 最大内存占用：15/16 × 390MB ≈ 365MB（可以接受）
```

**理论最大内存占用**：
```
假设内存限制为 M，分区数为 P
Level 0: 最大占用 = (P-1)/P × 总数据
Level 1: 最大占用 = (P-1)/P × 总数据/P
Level N: 最大占用 = (P-1)/P × 总数据/(P^N)

当 Level N 的最大占用 ≤ M 时，可以停止递归
```

---

## 4. 核心数据结构

### 4.1 分区节点（PartitionNode）

```cpp
// 分区节点（支持递归）
struct PartitionNode {
    // 基本信息
    uint64_t partition_id;              // 分区 ID（多级编码，如 0x00010002 表示 0-1-2）
    uint32_t level;                     // 分区层级（0 = 根层，1 = 第一层子分区...）
    uint32_t partition_index;           // 在当前层的索引（0-15）
    
    // 数据信息
    size_t estimated_size;               // 预估大小（字节）
    size_t actual_size;                  // 实际大小（字节）
    size_t row_count;                    // 行数
    
    // 状态标志
    bool is_spilled;                     // 是否已 spill 到磁盘
    bool needs_recursive_split;          // 是否需要递归分割
    bool is_leaf;                        // 是否为叶子节点
    
    // 子分区（如果存在）
    std::vector<std::unique_ptr<PartitionNode>> sub_partitions;
    uint32_t sub_partition_count;       // 子分区数量
    
    // SpillStream（如果已 spill）
    vectorized::SpillStreamSPtr spill_stream;
    
    // 内存中的数据块（如果未 spill 或部分在内存）
    vectorized::MutableBlockPtr in_mem_block;
    
    // 父节点指针（用于回溯）
    PartitionNode* parent;
    
    // 构造函数
    PartitionNode(uint32_t level, uint32_t index, PartitionNode* parent = nullptr)
        : level(level), partition_index(index), parent(parent), 
          is_spilled(false), needs_recursive_split(false), is_leaf(true),
          sub_partition_count(0) {
        partition_id = calculate_partition_id(level, index, parent);
    }
    
    // 计算分区 ID（多级编码）
    static uint64_t calculate_partition_id(uint32_t level, uint32_t index, 
                                           PartitionNode* parent) {
        if (parent == nullptr) {
            return index;  // 根层
        }
        return (parent->partition_id << 8) | index;  // 每层用 8 位编码
    }
    
    // 检查是否需要递归分割
    bool check_needs_recursive_split(size_t mem_limit) const {
        return estimated_size > mem_limit * 0.8;  // 80% 阈值，留出安全余量
    }
    
    // 获取所有叶子节点
    void get_leaf_nodes(std::vector<PartitionNode*>& leaves) {
        if (is_leaf) {
            leaves.push_back(this);
        } else {
            for (auto& sub : sub_partitions) {
                if (sub) {
                    sub->get_leaf_nodes(leaves);
                }
            }
        }
    }
};
```

### 4.2 递归分区管理器（RecursivePartitionManager）

```cpp
class RecursivePartitionManager {
public:
    RecursivePartitionManager(uint32_t partition_count, 
                              size_t mem_limit,
                              uint32_t max_level = 10)
        : _partition_count(partition_count),
          _mem_limit(mem_limit),
          _max_level(max_level) {}
    
    // 初始化根分区
    Status init_root_partitions(RuntimeState* state, 
                                const vectorized::Block& data);
    
    // 递归分割分区
    Status split_partition_recursively(PartitionNode* node,
                                       RuntimeState* state,
                                       const vectorized::Block& data);
    
    // 获取所有需要 spill 的叶子分区
    std::vector<PartitionNode*> get_leaf_partitions() {
        std::vector<PartitionNode*> leaves;
        for (auto& root : _root_partitions) {
            if (root) {
                root->get_leaf_nodes(leaves);
            }
        }
        return leaves;
    }
    
    // 查找分区节点（根据分区 ID）
    PartitionNode* find_partition(uint64_t partition_id);
    
    // 计算最大递归层级
    uint32_t calculate_max_level(size_t total_data_size) {
        uint32_t level = 0;
        size_t current_size = total_data_size / _partition_count;
        while (current_size > _mem_limit && level < _max_level) {
            current_size /= _partition_count;
            level++;
        }
        return level;
    }
    
    // 预估分区大小（采样估算）
    size_t estimate_partition_size(const vectorized::Block& data, 
                                   uint32_t partition_id);
    
private:
    std::vector<std::unique_ptr<PartitionNode>> _root_partitions;  // 根分区
    uint32_t _partition_count;          // 每层的分区数（如16）
    size_t _mem_limit;                  // 单个分区的最大大小（内存限制）
    uint32_t _max_level;                // 最大递归层级（防止无限递归）
    
    // 创建 Partitioner（不同层级使用不同的 hash seed）
    std::unique_ptr<vectorized::PartitionerBase> create_partitioner(uint32_t level);
};
```

### 4.3 分区 ID 编码方案

**多级编码**：
```
Level 0: Partition 0  → ID = 0x00000000
         Partition 1  → ID = 0x00000001
         ...
         Partition 15 → ID = 0x0000000F

Level 1: Partition 0-0 → ID = 0x00000000 (0 << 8 | 0)
         Partition 0-1 → ID = 0x00000001 (0 << 8 | 1)
         ...
         Partition 0-15 → ID = 0x0000000F (0 << 8 | 15)
         Partition 1-0 → ID = 0x00000100 (1 << 8 | 0)
         ...

Level 2: Partition 0-1-2 → ID = 0x00000102 ((0 << 8 | 1) << 8 | 2)
```

**编码函数**：
```cpp
uint64_t encode_partition_id(const std::vector<uint32_t>& indices) {
    uint64_t id = 0;
    for (uint32_t idx : indices) {
        id = (id << 8) | (idx & 0xFF);
    }
    return id;
}

std::vector<uint32_t> decode_partition_id(uint64_t id, uint32_t level) {
    std::vector<uint32_t> indices;
    for (uint32_t i = 0; i < level; ++i) {
        indices.push_back(id & 0xFF);
        id >>= 8;
    }
    std::reverse(indices.begin(), indices.end());
    return indices;
}
```

---

## 5. 关键算法

### 5.1 递归分割算法

```cpp
Status RecursivePartitionManager::split_partition_recursively(
        PartitionNode* node,
        RuntimeState* state,
        const vectorized::Block& data) {
    
    // ===== 阶段 1：检查终止条件 =====
    
    // 1.1 检查是否达到最大层级
    if (node->level >= _max_level) {
        // 达到最大层级，强制 spill（即使可能失败）
        LOG(WARNING) << fmt::format(
            "Partition {} reached max level {}, force spilling",
            node->partition_id, _max_level);
        return _force_spill_partition(node, state, data);
    }
    
    // 1.2 检查分区大小是否足够小
    if (!node->check_needs_recursive_split(_mem_limit)) {
        // 分区足够小，可以直接处理
        node->is_leaf = true;
        return _process_partition(node, state, data);
    }
    
    // ===== 阶段 2：创建子分区 =====
    
    node->is_leaf = false;
    node->sub_partitions.resize(_partition_count);
    node->sub_partition_count = _partition_count;
    
    for (uint32_t i = 0; i < _partition_count; ++i) {
        node->sub_partitions[i] = std::make_unique<PartitionNode>(
            node->level + 1, i, node);
    }
    
    // ===== 阶段 3：使用新的 Partitioner 分割数据 =====
    
    // 3.1 创建子层级的 Partitioner（使用不同的 hash seed）
    auto sub_partitioner = create_partitioner(node->level + 1);
    RETURN_IF_ERROR(sub_partitioner->do_partitioning(state, &data));
    
    // 3.2 获取分区结果
    const auto* channel_ids = sub_partitioner->get_channel_ids().get<uint32_t>();
    
    // ===== 阶段 4：将数据分配到子分区 =====
    
    // 4.1 收集每个子分区的行索引
    std::vector<std::vector<uint32_t>> sub_partition_indices(_partition_count);
    for (size_t i = 0; i < data.rows(); ++i) {
        uint32_t sub_partition_idx = channel_ids[i];
        sub_partition_indices[sub_partition_idx].push_back(i);
    }
    
    // 4.2 为每个子分区提取数据并递归处理
    for (uint32_t i = 0; i < _partition_count; ++i) {
        if (sub_partition_indices[i].empty()) {
            // 子分区为空，跳过
            continue;
        }
        
        // 提取子分区的数据
        auto sub_block = data.clone_empty();
        for (size_t col_idx = 0; col_idx < data.columns(); ++col_idx) {
            auto& sub_col = sub_block.get_by_position(col_idx).column;
            for (uint32_t row_idx : sub_partition_indices[i]) {
                sub_col->insert_from(*data.get_by_position(col_idx).column, row_idx);
            }
        }
        
        // 更新子分区的信息
        auto* sub_node = node->sub_partitions[i].get();
        sub_node->estimated_size = sub_block.allocated_bytes();
        sub_node->actual_size = sub_block.allocated_bytes();
        sub_node->row_count = sub_block.rows();
        
        // 递归处理子分区
        RETURN_IF_ERROR(split_partition_recursively(sub_node, state, sub_block));
    }
    
    return Status::OK();
}
```

### 5.2 创建 Partitioner（不同层级使用不同 Hash Seed）

```cpp
std::unique_ptr<vectorized::PartitionerBase> 
RecursivePartitionManager::create_partitioner(uint32_t level) {
    // 不同层级使用不同的 hash seed，确保分布均匀
    uint32_t hash_seed = level * 0x9E3779B9;  // 黄金比例乘数
    
    // 创建 Partitioner（需要根据实际实现调整）
    auto partitioner = std::make_unique<vectorized::HashPartitioner>(
        _partition_count, hash_seed);
    
    return partitioner;
}
```

### 5.3 Spill 流程改造

```cpp
Status PartitionedHashJoinSinkLocalState::revoke_memory(
        RuntimeState* state, 
        const std::shared_ptr<SpillContext>& spill_context) {
    
    SCOPED_TIMER(_spill_total_timer);
    
    // ===== 阶段 1：初始化 =====
    
    if (!_shared_state->is_spilled) {
        _shared_state->is_spilled = true;
        custom_profile()->add_info_string("Spilled", "true");
        
        // 初始化递归分区管理器
        size_t mem_limit = state->get_query_ctx()
            ->resource_ctx()->memory_context()->mem_limit();
        
        _recursive_partition_manager = std::make_unique<RecursivePartitionManager>(
            _parent->_partition_count,  // 每层分区数（如16）
            mem_limit * 0.8,            // 内存限制（80%阈值）
            MAX_RECURSIVE_LEVEL         // 最大递归层级（如10）
        );
        
        // 创建根分区并初始化
        RETURN_IF_ERROR(_recursive_partition_manager->init_root_partitions(
            state, _shared_state->build_block));
    }
    
    // ===== 阶段 2：获取需要 spill 的叶子分区 =====
    
    auto leaf_partitions = _recursive_partition_manager->get_leaf_partitions();
    
    // ===== 阶段 3：对每个叶子分区执行 spill =====
    
    for (auto* partition : leaf_partitions) {
        if (partition->needs_recursive_split) {
            // 需要递归分割
            RETURN_IF_ERROR(_recursive_partition_manager->split_partition_recursively(
                partition, state, partition->in_mem_block->to_block()));
        } else {
            // 可以直接 spill
            RETURN_IF_ERROR(_spill_partition_to_disk(partition, state));
        }
    }
    
    return Status::OK();
}
```

### 5.4 Probe 阶段改造

```cpp
Status PartitionedHashJoinProbeLocalState::_probe_from_disk(
        RuntimeState* state,
        const vectorized::Block& probe_block) {
    
    // ===== 阶段 1：计算 probe 数据属于哪个根分区 =====
    
    RETURN_IF_ERROR(_partitioner->do_partitioning(state, &probe_block));
    const auto* channel_ids = _partitioner->get_channel_ids().get<uint32_t>();
    
    // ===== 阶段 2：按分区分组处理 =====
    
    std::vector<std::vector<uint32_t>> partition_indices(_parent->_partition_count);
    for (size_t i = 0; i < probe_block.rows(); ++i) {
        uint32_t partition_idx = channel_ids[i];
        partition_indices[partition_idx].push_back(i);
    }
    
    // ===== 阶段 3：递归处理每个分区 =====
    
    for (uint32_t partition_idx = 0; partition_idx < _parent->_partition_count; ++partition_idx) {
        if (partition_indices[partition_idx].empty()) {
            continue;
        }
        
        // 提取该分区的 probe 数据
        auto partition_probe_block = probe_block.clone_empty();
        for (size_t col_idx = 0; col_idx < probe_block.columns(); ++col_idx) {
            for (uint32_t row_idx : partition_indices[partition_idx]) {
                partition_probe_block.get_by_position(col_idx).column->insert_from(
                    *probe_block.get_by_position(col_idx).column, row_idx);
            }
        }
        
        // 查找对应的分区节点
        auto* partition_node = _recursive_partition_manager->find_partition(partition_idx);
        
        // 递归 probe
        RETURN_IF_ERROR(_probe_partition_recursively(
            state, partition_probe_block, partition_node));
    }
    
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::_probe_partition_recursively(
        RuntimeState* state,
        const vectorized::Block& probe_block,
        PartitionNode* partition_node) {
    
    // ===== 情况 1：叶子节点，直接 probe =====
    
    if (partition_node->is_leaf) {
        if (partition_node->is_spilled) {
            // 从磁盘读取 build 数据并 probe
            RETURN_IF_ERROR(_probe_from_leaf_partition(
                state, probe_block, partition_node));
        } else {
            // 从内存 probe
            RETURN_IF_ERROR(_probe_from_memory(
                state, probe_block, partition_node));
        }
        return Status::OK();
    }
    
    // ===== 情况 2：非叶子节点，需要递归分割 probe 数据 =====
    
    // 使用子层级的 Partitioner 进一步分割
    auto sub_partitioner = _recursive_partition_manager->create_partitioner(
        partition_node->level + 1);
    RETURN_IF_ERROR(sub_partitioner->do_partitioning(state, &probe_block));
    
    const auto* channel_ids = sub_partitioner->get_channel_ids().get<uint32_t>();
    
    // 将 probe 数据分配到子分区
    std::vector<std::vector<uint32_t>> sub_partition_indices(_parent->_partition_count);
    for (size_t i = 0; i < probe_block.rows(); ++i) {
        uint32_t sub_partition_idx = channel_ids[i];
        sub_partition_indices[sub_partition_idx].push_back(i);
    }
    
    // 递归处理每个子分区
    for (uint32_t i = 0; i < _parent->_partition_count; ++i) {
        if (sub_partition_indices[i].empty()) {
            continue;
        }
        
        // 提取子分区的 probe 数据
        auto sub_probe_block = probe_block.clone_empty();
        for (size_t col_idx = 0; col_idx < probe_block.columns(); ++col_idx) {
            for (uint32_t row_idx : sub_partition_indices[i]) {
                sub_probe_block.get_by_position(col_idx).column->insert_from(
                    *probe_block.get_by_position(col_idx).column, row_idx);
            }
        }
        
        // 递归 probe
        auto* sub_node = partition_node->sub_partitions[i].get();
        RETURN_IF_ERROR(_probe_partition_recursively(state, sub_probe_block, sub_node));
    }
    
    return Status::OK();
}
```

### 5.5 内存预估算法

```cpp
size_t RecursivePartitionManager::estimate_partition_size(
        const vectorized::Block& data,
        uint32_t partition_id) {
    
    // 使用采样估算
    const size_t SAMPLE_SIZE = 1000;
    size_t sample_size = std::min(SAMPLE_SIZE, data.rows());
    
    if (sample_size == 0) {
        return 0;
    }
    
    // 采样数据
    auto sample_block = data.clone_empty();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, data.rows() - 1);
    
    std::set<size_t> sampled_indices;
    while (sampled_indices.size() < sample_size) {
        sampled_indices.insert(dis(gen));
    }
    
    for (size_t col_idx = 0; col_idx < data.columns(); ++col_idx) {
        for (size_t idx : sampled_indices) {
            sample_block.get_by_position(col_idx).column->insert_from(
                *data.get_by_position(col_idx).column, idx);
        }
    }
    
    // 计算采样数据的大小
    size_t sample_size_bytes = sample_block.allocated_bytes();
    
    // 估算总大小
    size_t estimated_size = (data.rows() * sample_size_bytes) / sample_size;
    
    return estimated_size;
}
```

---

## 6. 实现步骤

### Phase 1: 基础数据结构（1-2 周）

**任务清单**：
1. ✅ 实现 `PartitionNode` 结构体
   - 支持多级分区 ID 编码
   - 支持子分区管理
   - 支持状态追踪

2. ✅ 实现 `RecursivePartitionManager` 类
   - 初始化根分区
   - 分区查找和遍历
   - 内存预估

3. ✅ 修改 `PartitionedHashJoinSharedState`
   - 添加 `RecursivePartitionManager` 成员
   - 支持多级分区状态管理

**文件清单**：
- `be/src/pipeline/exec/recursive_partition_manager.h`
- `be/src/pipeline/exec/recursive_partition_manager.cpp`
- `be/src/pipeline/exec/partitioned_hash_join_shared_state.h`（修改）

### Phase 2: 递归分割逻辑（2-3 周）

**任务清单**：
1. ✅ 实现 `split_partition_recursively` 方法
   - 递归终止条件检查
   - 子分区创建
   - 数据分割和分配

2. ✅ 实现多级 Partitioner
   - 不同层级使用不同的 hash seed
   - 确保分布均匀

3. ✅ 实现分区大小预估
   - 采样估算算法
   - 动态调整策略

**文件清单**：
- `be/src/pipeline/exec/recursive_partition_manager.cpp`（扩展）
- `be/src/vec/runtime/partitioner.h`（可能需要修改）

### Phase 3: Spill 改造（2-3 周）

**任务清单**：
1. ✅ 修改 `revoke_memory` 方法
   - 支持递归分割
   - 叶子节点 spill

2. ✅ 修改 SpillStream 命名
   - 支持多级路径（如 `partition_0_1_2/stream_0`）
   - 文件路径管理

3. ✅ 实现叶子分区 spill
   - 递归 spill 流程
   - 错误处理

**文件清单**：
- `be/src/pipeline/exec/partitioned_hash_join_sink_operator.cpp`（修改）
- `be/src/vec/spill/spill_stream.h`（可能需要修改）

### Phase 4: Probe 改造（2-3 周）

**任务清单**：
1. ✅ 修改 Probe 阶段
   - 支持多级分区查找
   - 递归 Probe 逻辑

2. ✅ 实现递归 Probe
   - 子分区数据提取
   - 递归调用

3. ✅ 性能优化
   - 避免重复计算
   - 缓存优化

**文件清单**：
- `be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp`（修改）

### Phase 5: 测试和优化（2-3 周）

**任务清单**：
1. ✅ 单元测试
   - 递归分割测试
   - 内存预估测试
   - 边界情况测试

2. ✅ 集成测试
   - TPC-DS 1TB 测试
   - 内存限制测试
   - 性能测试

3. ✅ 性能调优
   - 内存使用优化
   - 递归层级优化
   - 采样算法优化

**文件清单**：
- `be/test/pipeline/exec/recursive_partition_test.cpp`（新建）
- `be/test/integration-test/spill/recursive_spill_test.cpp`（新建）

---

## 7. 注意事项

### 7.1 分区 ID 编码

**问题**：如何高效编码多级分区 ID？

**方案**：
- 使用位编码：每层用 8 位（支持 256 个分区）
- 最大层级：8 层（64 位 / 8 = 8）
- 编码格式：`(level_0 << 56) | (level_1 << 48) | ... | level_7`

**示例**：
```cpp
// Level 0: Partition 5
ID = 0x0500000000000000

// Level 1: Partition 5-3
ID = 0x0503000000000000

// Level 2: Partition 5-3-12
ID = 0x05030C0000000000
```

### 7.2 Hash Seed 选择

**问题**：如何确保不同层级的分布均匀？

**方案**：
- 使用不同的 hash seed 对每一层
- 使用黄金比例乘数：`seed = level * 0x9E3779B9`
- 或者使用质数：`seed = level * 2654435761`

**验证**：
- 测试不同 seed 的分布均匀性
- 确保没有冲突

### 7.3 内存限制检查

**问题**：如何确保不会 OOM？

**方案**：
- 严格检查每层分区大小
- 使用 80% 阈值（`estimated_size > mem_limit * 0.8`）
- 达到最大层级时强制 spill（即使可能失败）

**监控**：
- 记录每层的内存使用
- 记录递归深度
- 记录 spill 次数

### 7.4 文件路径管理

**问题**：如何组织多级分区的文件路径？

**方案**：
```
spill_root/
  └─ query_id/
      └─ hash_join_sink_node_id/
          ├─ partition_0/
          │   ├─ stream_0
          │   ├─ stream_1
          │   └─ ...
          ├─ partition_0_1/
          │   ├─ stream_0
          │   └─ ...
          └─ partition_0_1_2/
              └─ stream_0
```

**实现**：
```cpp
std::string get_partition_path(uint64_t partition_id, uint32_t level) {
    std::string path = "partition_";
    auto indices = decode_partition_id(partition_id, level);
    for (size_t i = 0; i < indices.size(); ++i) {
        if (i > 0) path += "_";
        path += std::to_string(indices[i]);
    }
    return path;
}
```

### 7.5 性能考虑

**问题**：递归会增加多少开销？

**优化策略**：
1. **延迟分割**：只有在真正需要时才递归分割
2. **采样估算**：使用采样快速估算分区大小
3. **缓存优化**：缓存 Partitioner 和计算结果
4. **并行处理**：并行处理多个分区

**性能目标**：
- 递归分割开销 < 5%
- 内存使用减少 > 80%
- 查询延迟增加 < 10%

### 7.6 错误处理

**问题**：如何处理递归过程中的错误？

**策略**：
1. **层级限制**：达到最大层级时强制 spill
2. **回退机制**：如果递归失败，回退到单层分区
3. **错误报告**：详细记录错误信息，便于调试

**错误场景**：
- 磁盘空间不足
- 内存分配失败
- 文件写入失败
- 递归层级过深

---

## 8. 参考链接

- [GitHub Issue #59194](https://github.com/apache/doris/issues/59194) - 原始 Issue
- `be/src/pipeline/exec/partitioned_hash_join_sink_operator.cpp` - 当前实现
- `be/src/pipeline/exec/partitioned_hash_join_probe_operator.cpp` - Probe 实现
- `be/src/vec/runtime/partitioner.h` - Partitioner 接口
- `be/src/vec/spill/spill_stream.h` - SpillStream 实现

---

## 9. 预期效果

### 9.1 内存使用

**当前实现**：
- 100GB 数据，16 个分区
- 最大内存占用：93.75GB（15/16）

**递归实现**：
- 100GB 数据，16 个分区，2 层递归
- 最大内存占用：约 365MB（15/16 × 390MB）
- **内存减少：99.6%**

### 9.2 性能影响

**预期开销**：
- 递归分割：< 5% 额外时间
- 文件 I/O：增加约 10-20%（更多小文件）
- 总体延迟：增加 < 10%

**性能优化后**：
- 并行处理：减少 50% 时间
- 批量操作：减少 I/O 次数
- 总体延迟：增加 < 5%

### 9.3 适用场景

**适合**：
- ✅ 大表 Join（> 100GB）
- ✅ 内存受限环境（< 10GB）
- ✅ TPC-DS 等基准测试

**不适合**：
- ❌ 小表 Join（< 1GB）
- ❌ 内存充足环境（> 100GB）
- ❌ 延迟敏感查询

---

**文档版本**：v1.0  
**最后更新**：2024年  
**作者**：基于 GitHub Issue #59194 分析

