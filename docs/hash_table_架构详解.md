# Doris Hash 表架构详解

## 目录

1. [概述](#概述)
2. [整体架构](#整体架构)
3. [核心组件](#核心组件)
4. [Hash 表类型](#hash-表类型)
5. [Hash 表方法（Method）](#hash-表方法method)
6. [类型选择流程](#类型选择流程)
7. [使用流程](#使用流程)
8. [性能优化](#性能优化)
9. [代码示例](#代码示例)

---

## 概述

Doris 的 Hash 表架构是一个高度优化的、多层次的系统，用于支持 Hash Join 和 Aggregation 操作。该架构通过模板元编程和策略模式，实现了对不同键类型的自动适配和性能优化。

### 核心设计目标

1. **类型安全**：通过模板参数确保类型一致性
2. **性能优化**：针对不同键类型选择最优的实现策略
3. **代码复用**：通过继承和模板实现代码复用
4. **扩展性**：易于添加新的键类型和处理方法
5. **统一接口**：上层代码通过统一接口操作不同的 Hash 表

### 应用场景

- **Hash Join**：用于匹配两个表的行
- **Aggregation**：用于 GROUP BY 和聚合函数计算

---

## 整体架构

### 架构层次

```
┌─────────────────────────────────────────────────────────────┐
│                    上层应用代码                              │
│  (HashJoinBuildSinkOperatorX, HashJoinProbeOperatorX)       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              HashTableVariants (std::variant)               │
│  统一接口，包含所有可能的 Hash 表实现类型                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ PrimaryType  │ │ FixedKey     │ │ Serialized   │
│ HashTable    │ │ HashTable    │ │ HashTable    │
│ Context      │ │ Context      │ │ Context      │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                 │
       ▼                ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│              MethodBase / MethodBaseInner                    │
│  抽象基类，定义统一的接口和通用实现                           │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ MethodOne    │ │ MethodKeys   │ │ Method       │
│ Number       │ │ Fixed        │ │ Serialized   │
│ (零拷贝)     │ │ (打包)       │ │ (序列化)     │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                 │
       ▼                ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│              JoinHashMap / PHHashMap                        │
│  底层 Hash 表实现（链式哈希）                                │
└─────────────────────────────────────────────────────────────┘
```

### 关键组件关系

```
HashTableVariants (std::variant)
    ├── PrimaryTypeHashTableContext<T>
    │   └── MethodOneNumber<T, JoinHashMap<T, ...>>
    │       └── MethodBase<JoinHashMap<T, ...>>
    │           └── MethodBaseInner<JoinHashMap<T, ...>>
    │               └── JoinHashMap<T, HashCRC32<T>, false>
    │
    ├── FixedKeyHashTableContext<UInt128>
    │   └── MethodKeysFixed<JoinHashMap<UInt128, ...>>
    │       └── MethodBase<JoinHashMap<UInt128, ...>>
    │           └── MethodBaseInner<JoinHashMap<UInt128, ...>>
    │               └── JoinHashMap<UInt128, HashCRC32<UInt128>, false>
    │
    └── SerializedHashTableContext
        └── MethodSerialized<JoinHashMap<StringRef, ...>>
            └── MethodBase<JoinHashMap<StringRef, ...>>
                └── MethodBaseInner<JoinHashMap<StringRef, ...>>
                    └── JoinHashMap<StringRef, DefaultHash<StringRef>, false>
```

---

## 核心组件

### 1. HashTableVariants (std::variant)

**定义位置**：`be/src/pipeline/common/join_utils.h`

**作用**：使用 `std::variant` 实现类型安全的联合类型，包含所有可能的 Hash 表实现。

**包含的类型**：
- `PrimaryTypeHashTableContext<T>`：单列整数键
- `DirectPrimaryTypeHashTableContext<T>`：单列整数键（直接映射）
- `FixedKeyHashTableContext<Key>`：固定长度键（多列打包）
- `SerializedHashTableContext`：序列化键（复杂类型）
- `MethodOneString`：字符串键

**使用方式**：
```cpp
HashTableVariants method_variant;
// 初始化
method_variant.emplace<PrimaryTypeHashTableContext<UInt64>>();
// 访问（使用 std::visit）
std::visit([&](auto&& arg) {
    // 操作 arg
}, method_variant);
```

### 2. MethodBaseInner

**定义位置**：`be/src/vec/common/hash_table/hash_map_context.h`

**作用**：Hash 表方法的抽象基类，定义统一的接口和通用实现。

**核心职责**：
1. **类型提取**：从 HashMap 模板参数中提取 Key、Mapped、Value 类型
2. **统一接口**：定义所有 Hash 表方法必须实现的虚函数接口
3. **通用实现**：提供 Join 和 Aggregation 场景的通用实现
4. **键管理**：管理键的存储、序列化、Hash 计算等

**关键成员变量**：
```cpp
std::shared_ptr<HashMap> hash_table;  // Hash 表的智能指针
Key* keys;                            // 键的数组指针
Arena arena;                          // 内存池
DorisVector<size_t> hash_values;      // Hash 值数组（Aggregation）
DorisVector<uint32_t> bucket_nums;   // Hash 桶编号数组（Join）
```

**虚函数接口**：
- `init_serialized_keys()`：初始化键（子类必须实现）
- `estimated_size()`：估算内存使用大小（子类必须实现）
- `insert_keys_into_columns()`：将键插入到列中（子类必须实现）

**通用实现**：
- `init_join_bucket_num()`：计算 Join 场景的 Hash 桶编号
- `init_hash_values()`：计算 Aggregation 场景的 Hash 值
- `find()` / `lazy_emplace()`：Hash 表查找和插入的模板方法

### 3. MethodBase

**定义位置**：`be/src/vec/common/hash_table/hash_map_context.h`

**作用**：继承自 `MethodBaseInner`，添加迭代器支持。

**特化版本**：
- **通用版本**：`Iterator = void*`，不支持迭代
- **特化版本**（`IteratoredMap` 概念）：`Iterator = HashMap::iterator`，支持迭代

### 4. JoinHashTable

**定义位置**：`be/src/vec/common/hash_table/join_hash_table.h`

**作用**：底层 Hash 表实现，使用链式哈希处理冲突。

**核心数据结构**：
```cpp
DorisVector<uint32_t> first;   // first[bucket_num] = 第一个行的索引
DorisVector<uint32_t> next;     // next[row_idx] = 下一个行的索引
DorisVector<uint8_t> visited;   // visited[row_idx] = 是否已访问（OUTER JOIN）
const Key* build_keys;          // Build 侧的键数组
```

**链式哈希原理**：
```
Hash 桶 0: first[0] = 3 → next[3] = 7 → next[7] = 0 (结束)
Hash 桶 1: first[1] = 5 → next[5] = 0 (结束)
Hash 桶 2: first[2] = 0 (空)
```

**关键方法**：
- `build()`：构建 Hash 表（链式哈希）
- `find_batch()`：批量查找（根据 Join 类型选择不同的查找策略）
- `prepare_build()`：准备构建（分配内存、计算桶大小）

---

## Hash 表类型

### 1. PrimaryTypeHashTableContext（标准 Hash 表）

**定义**：`MethodOneNumber<T, JoinHashMap<T, HashCRC32<T>, false>>`

**用途**：单列整数类型键（UInt8/16/32/64/128/256）

**工作原理**：
1. 使用 CRC32 Hash 函数计算键的 Hash 值
2. Hash 值取模得到桶编号：`bucket_num = hash(key) & (bucket_size - 1)`
3. 使用链式哈希处理冲突

**适用场景**：
- 单列整数键
- 键值范围较大（无法使用 Direct Mapping）
- 例如：用户 ID（1-1000000）、订单 ID 等

**性能**：O(1) 平均查找时间，需要 Hash 计算开销

**示例**：
```cpp
// 键：user_id (UInt64)
using HashMap = JoinHashMap<UInt64, HashCRC32<UInt64>, false>;
using Method = MethodOneNumber<UInt64, HashMap>;
Method method;
method.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
// keys 直接指向列的数据，零拷贝
```

### 2. DirectPrimaryTypeHashTableContext（直接映射 Hash 表）

**定义**：`MethodOneNumberDirect<T, JoinHashMap<T, HashCRC32<T>, true>>`

**用途**：单列整数类型键，键值范围小

**工作原理**：
1. 不使用 Hash 函数，键值直接作为数组索引
2. `bucket_num = key - min_key + 1`（直接映射）
3. 如果 `key < min_key` 或 `key > max_key`，`bucket_num = 0`（表示未匹配）

**转换条件**：
- 从 `PrimaryTypeHashTableContext` 转换而来
- 条件：`max_key - min_key < MAX_MAPPING_RANGE` (8,388,607)
- 例如：键值范围 [100, 200] 可以使用 Direct Mapping

**适用场景**：
- 单列整数键
- 键值范围小（差值 < 8M）
- 例如：状态码（0-10）、类型 ID（1-100）等

**性能**：O(1) 查找时间，无需 Hash 计算，性能最优

**内存**：需要分配 `(max_key - min_key + 2)` 个桶的空间

**示例**：
```cpp
// 键：status_code (UInt8, 范围 0-10)
// 转换后使用 Direct Mapping
// bucket_num = status_code - 0 + 1 = status_code + 1
// 查找时直接访问 first[bucket_num]，无需 Hash 计算
```

### 3. FixedKeyHashTableContext（固定长度键 Hash 表）

**定义**：`MethodKeysFixed<JoinHashMap<Key, HashCRC32<Key>, false>>`

**用途**：多列键或固定长度的复合键

**工作原理**：
1. 将多个列打包成固定长度的键（UInt64/UInt72/UInt96/UInt104/UInt128/UInt136/UInt256）
2. 根据总大小选择键类型：
   - <= 8 bytes → UInt64
   - <= 9 bytes → UInt72
   - <= 12 bytes → UInt96
   - <= 13 bytes → UInt104
   - <= 16 bytes → UInt128
   - <= 17 bytes → UInt136
   - <= 32 bytes → UInt256
   - > 32 bytes → SerializedHashTableContext
3. 使用 CRC32 Hash 函数计算打包后的键的 Hash 值

**打包方式**：
- 如果有多列，按顺序打包到固定长度的整数中
- 如果有 NULL 值，使用 bitmap 标记（每个键一个 bit）
- 例如：两列 (INT32, INT32) → 打包成 UInt64

**适用场景**：
- 多列 Join 键（如 `ON a.id = b.id AND a.type = b.type`）
- 键的总大小 <= 32 bytes
- 例如：(user_id, order_id)、(date, region, product_id) 等

**性能**：O(1) 平均查找时间，需要打包/解包开销

**示例**：
```cpp
// 键：(user_id INT64, order_id INT64)
// 打包成 UInt128
using HashMap = JoinHashMap<UInt128, HashCRC32<UInt128>, false>;
using Method = MethodKeysFixed<HashMap>;
Method method;
method.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
// keys 是打包后的 UInt128 数组
```

### 4. SerializedHashTableContext（序列化 Hash 表）

**定义**：`MethodSerialized<JoinHashMap<StringRef, DefaultHash<StringRef>, false>>`

**用途**：复杂类型或超大键（>32 bytes）

**工作原理**：
1. 将多列键序列化为连续的字节流（StringRef）
2. 使用 DefaultHash 计算 StringRef 的 Hash 值
3. Hash 值取模得到桶编号

**适用场景**：
- 复杂类型（Array、Map、JSON 等）
- 超大键（>32 bytes）
- 无法使用 FixedKey 打包的多列键

**性能**：O(1) 平均查找时间，需要序列化/反序列化开销

**示例**：
```cpp
// 键：复杂类型或超大键
using HashMap = JoinHashMap<StringRef, DefaultHash<StringRef>, false>;
using Method = MethodSerialized<HashMap>;
Method method;
method.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
// keys 是序列化后的 StringRef 数组
```

### 5. MethodOneString（字符串键 Hash 表）

**定义**：`MethodStringNoCache<JoinHashMap<StringRef, DefaultHash<StringRef>, false>>`

**用途**：字符串类型的单列键

**工作原理**：
1. 直接使用 StringRef，不缓存字符串数据
2. 使用 DefaultHash 计算 StringRef 的 Hash 值
3. Hash 值取模得到桶编号

**适用场景**：
- 字符串类型的单列键
- 例如：product_name、user_name 等

**性能**：O(1) 平均查找时间，需要字符串 Hash 计算开销

---

## Hash 表方法（Method）

### 1. MethodOneNumber（单列数值键方法）

**定义位置**：`be/src/vec/common/hash_table/hash_map_context.h`

**策略**：零拷贝，直接使用列的原始数据指针

**关键实现**：
```cpp
void init_serialized_keys(...) {
    // 零拷贝：直接使用列的数据指针
    Base::keys = (FieldType*)(key_columns[0]->is_nullable()
        ? assert_cast<const ColumnNullable*>(key_columns[0])
            ->get_nested_column_ptr()->get_raw_data().data
        : key_columns[0]->get_raw_data().data);
    
    if (is_join) {
        Base::init_join_bucket_num(num_rows, bucket_size, null_map);
    } else {
        Base::init_hash_values(num_rows, null_map);
    }
}
```

**优势**：
- 零拷贝：避免数据复制
- 高效：直接使用列的内部存储
- 内存友好：不占用额外内存

### 2. MethodSerialized（序列化键方法）

**定义位置**：`be/src/vec/common/hash_table/hash_map_context.h`

**策略**：将多列键序列化为连续的字节流（StringRef）

**关键实现**：
```cpp
void init_serialized_keys(...) {
    // 序列化键到 arena
    input_arena.clear();
    input_keys.resize(num_rows);
    
    for (size_t i = 0; i < num_rows; ++i) {
        input_keys[i] = serialize_keys_to_pool_contiguous(
            i, keys_size, key_columns, input_arena);
    }
    
    Base::keys = input_keys.data();
    // ...
}
```

**优势**：
- 支持复杂类型
- 支持超大键
- 统一处理多列键

### 3. MethodKeysFixed（固定长度键方法）

**定义位置**：`be/src/vec/common/hash_table/hash_map_context.h`

**策略**：将多列键打包成固定长度的整数

**关键实现**：
```cpp
void init_serialized_keys(...) {
    // 打包键到固定长度整数
    keys.resize(num_rows);
    
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i] = pack_keys(i, key_columns, null_map);
    }
    
    Base::keys = keys.data();
    // ...
}
```

**优势**：
- 高效：固定长度，便于 Hash 计算
- 内存友好：避免序列化开销
- 支持多列键（<=32 bytes）

### 4. MethodStringNoCache（字符串键方法）

**定义位置**：`be/src/vec/common/hash_table/hash_map_context.h`

**策略**：直接使用 StringRef，不缓存字符串数据

**关键实现**：
```cpp
void init_serialized_keys(...) {
    // 直接使用 StringRef
    Base::keys = (StringRef*)(key_columns[0]->get_raw_data().data);
    // ...
}
```

**优势**：
- 简单：直接使用字符串引用
- 内存友好：不缓存字符串数据

---

## 类型选择流程

### 流程图

```
开始
  │
  ▼
检查键数量
  │
  ├─→ 无键 → without_key
  │
  ├─→ 单列键 ──→ 检查数据类型
  │              │
  │              ├─→ 整数类型 → PrimaryTypeHashTableContext
  │              │              │
  │              │              └─→ 检查键值范围
  │              │                 │
  │              │                 ├─→ 范围小 → DirectPrimaryTypeHashTableContext
  │              │                 │
  │              │                 └─→ 范围大 → PrimaryTypeHashTableContext
  │              │
  │              ├─→ 字符串类型 → MethodOneString
  │              │
  │              └─→ 复杂类型 → SerializedHashTableContext
  │
  └─→ 多列键 ──→ 计算总大小（包括 NULL bitmap）
                 │
                 ├─→ <= 8 bytes → FixedKeyHashTableContext<UInt64>
                 ├─→ <= 9 bytes → FixedKeyHashTableContext<UInt72>
                 ├─→ <= 12 bytes → FixedKeyHashTableContext<UInt96>
                 ├─→ <= 13 bytes → FixedKeyHashTableContext<UInt104>
                 ├─→ <= 16 bytes → FixedKeyHashTableContext<UInt128>
                 ├─→ <= 17 bytes → FixedKeyHashTableContext<UInt136>
                 ├─→ <= 32 bytes → FixedKeyHashTableContext<UInt256>
                 └─→ > 32 bytes → SerializedHashTableContext
```

### 选择规则

#### 1. 单列键

**整数类型**：
- 根据数据类型大小选择对应的 `PrimaryTypeHashTableContext`
  - UInt8 → `PrimaryTypeHashTableContext<UInt8>`
  - UInt16 → `PrimaryTypeHashTableContext<UInt16>`
  - UInt32 → `PrimaryTypeHashTableContext<UInt32>`
  - UInt64 → `PrimaryTypeHashTableContext<UInt64>`
  - UInt128 → `PrimaryTypeHashTableContext<UInt128>`
  - UInt256 → `PrimaryTypeHashTableContext<UInt256>`
- 如果键值范围小（`max_key - min_key < MAX_MAPPING_RANGE`），转换为 `DirectPrimaryTypeHashTableContext`

**字符串类型**：
- 选择 `MethodOneString`

**复杂类型**：
- 选择 `SerializedHashTableContext`

#### 2. 多列键

**计算总大小**：
```cpp
size_t key_byte_size = 0;
for (const auto& data_type : data_types) {
    key_byte_size += data_type->get_size_of_value_in_memory();
    if (data_type->is_nullable()) {
        key_byte_size--;  // NULL 值不占用空间
    }
}
size_t bitmap_size = has_null ? get_bitmap_size(data_types.size()) : 0;
size_t total_size = bitmap_size + key_byte_size;
```

**根据总大小选择**：
- `total_size <= 8` → `FixedKeyHashTableContext<UInt64>`
- `total_size <= 9` → `FixedKeyHashTableContext<UInt72>`
- `total_size <= 12` → `FixedKeyHashTableContext<UInt96>`
- `total_size <= 13` → `FixedKeyHashTableContext<UInt104>`
- `total_size <= 16` → `FixedKeyHashTableContext<UInt128>`
- `total_size <= 17` → `FixedKeyHashTableContext<UInt136>`
- `total_size <= 32` → `FixedKeyHashTableContext<UInt256>`
- `total_size > 32` → `SerializedHashTableContext`

### 代码实现

**位置**：`be/src/vec/common/hash_table/hash_key_type.h`

```cpp
inline HashKeyType get_hash_key_type(const std::vector<DataTypePtr>& data_types) {
    if (data_types.size() > 1) {
        return get_hash_key_type_fixed(data_types);  // 多列键
    }
    if (data_types.empty()) {
        return HashKeyType::without_key;
    }
    if (is_complex_type(data_types[0]->get_primitive_type())) {
        return HashKeyType::serialized;  // 复杂类型
    }
    
    auto t = remove_nullable(data_types[0]);
    if (!t->have_maximum_size_of_value()) {
        if (is_string_type(t->get_primitive_type())) {
            return HashKeyType::string_key;  // 字符串类型
        }
        return HashKeyType::serialized;
    }
    
    // 整数类型：根据大小选择
    size_t size = t->get_size_of_value_in_memory();
    if (size == sizeof(UInt8)) return HashKeyType::int8_key;
    if (size == sizeof(UInt16)) return HashKeyType::int16_key;
    if (size == sizeof(UInt32)) return HashKeyType::int32_key;
    if (size == sizeof(UInt64)) return HashKeyType::int64_key;
    if (size == sizeof(UInt128)) return HashKeyType::int128_key;
    if (size == sizeof(UInt256)) return HashKeyType::int256_key;
    return HashKeyType::serialized;
}
```

---

## 使用流程

### Hash Join 流程

#### 1. Build 阶段

```
初始化 Hash 表
  │
  ▼
选择 Hash 表类型（根据键类型）
  │
  ▼
初始化 Hash 表方法（init_hash_method）
  │
  ▼
处理 Build 数据块
  │
  ├─→ 提取 Join 键列
  ├─→ 初始化键（init_serialized_keys）
  │   ├─→ MethodOneNumber: 零拷贝
  │   ├─→ MethodKeysFixed: 打包
  │   └─→ MethodSerialized: 序列化
  │
  ├─→ 计算 Hash 值（init_join_bucket_num）
  │   └─→ bucket_nums[i] = hash(keys[i]) & (bucket_size - 1)
  │
  └─→ 构建 Hash 表（build）
      └─→ 链式哈希：first[bucket_num] = row_idx, next[row_idx] = ...
```

#### 2. Probe 阶段

```
处理 Probe 数据块
  │
  ▼
提取 Join 键列
  │
  ▼
初始化键（init_serialized_keys）
  │
  ▼
计算 Hash 值（init_join_bucket_num）
  │
  ▼
查找 Hash 表（find_batch）
  │
  ├─→ 根据 Join 类型选择查找策略
  │   ├─→ INNER JOIN: _find_batch_inner_outer_join
  │   ├─→ LEFT OUTER JOIN: _find_batch_inner_outer_join
  │   ├─→ RIGHT OUTER JOIN: _find_batch_inner_outer_join + visited
  │   └─→ ...
  │
  └─→ 输出匹配结果
```

### Aggregation 流程

```
初始化 Hash 表
  │
  ▼
选择 Hash 表类型（根据 GROUP BY 键类型）
  │
  ▼
初始化 Hash 表方法（init_hash_method）
  │
  ▼
处理数据块
  │
  ├─→ 提取 GROUP BY 键列
  ├─→ 初始化键（init_serialized_keys）
  │
  ├─→ 计算 Hash 值（init_hash_values）
  │   └─→ hash_values[i] = hash(keys[i])
  │
  └─→ 查找/插入 Hash 表（find / lazy_emplace）
      ├─→ 找到：更新聚合结果
      └─→ 未找到：创建新的聚合结果
```

---

## 性能优化

### 1. Direct Mapping 优化

**原理**：对于整数类型的键，如果键值范围小，可以使用直接映射，避免 Hash 计算。

**条件**：
- 单列整数键
- `max_key - min_key < MAX_MAPPING_RANGE` (8,388,607)

**实现**：
```cpp
// 转换前：PrimaryTypeHashTableContext
bucket_num = hash(key) & (bucket_size - 1);

// 转换后：DirectPrimaryTypeHashTableContext
bucket_num = key - min_key + 1;  // 直接映射，无需 Hash 计算
```

**性能提升**：
- 消除 Hash 计算开销
- 减少内存访问次数
- 提高缓存局部性

### 2. 零拷贝优化

**原理**：对于数值类型的键，直接使用列的原始数据指针，避免数据复制。

**实现**：
```cpp
// MethodOneNumber::init_serialized_keys
Base::keys = (FieldType*)(key_columns[0]->get_raw_data().data);
// 直接使用列的数据指针，零拷贝
```

**性能提升**：
- 消除数据复制开销
- 减少内存占用
- 提高缓存命中率

### 3. 固定长度键打包

**原理**：将多列键打包成固定长度的整数，便于 Hash 计算和比较。

**实现**：
```cpp
// MethodKeysFixed::init_serialized_keys
keys[i] = pack_keys(i, key_columns, null_map);
// 将多列键打包成 UInt64/UInt128/UInt256 等
```

**性能提升**：
- 固定长度，便于 Hash 计算
- 减少内存占用（相比序列化）
- 提高比较效率

### 4. Prefetch 优化

**原理**：在处理当前键时，预取下一个键的数据，提高缓存命中率。

**实现**：
```cpp
template <bool read>
ALWAYS_INLINE void prefetch(size_t i) {
    if (LIKELY(i + HASH_MAP_PREFETCH_DIST < hash_values.size())) {
        hash_table->template prefetch<read>(
            keys[i + HASH_MAP_PREFETCH_DIST],
            hash_values[i + HASH_MAP_PREFETCH_DIST]);
    }
}
```

**性能提升**：
- 减少缓存未命中
- 提高内存访问效率

### 5. 批量处理优化

**原理**：批量处理多个键，减少函数调用开销。

**实现**：
```cpp
// JoinHashTable::find_batch
// 批量查找多个键，减少函数调用开销
auto [probe_idx, build_idx, matched_cnt] = 
    hash_table->find_batch<JoinOpType>(...);
```

**性能提升**：
- 减少函数调用开销
- 提高指令缓存命中率

---

## 代码示例

### 示例 1：单列 UInt64 键的 Hash Join

```cpp
// 1. 初始化 Hash 表
HashTableVariants method_variant;
method_variant.emplace<PrimaryTypeHashTableContext<UInt64>>();

// 2. 初始化 Hash 表方法
std::visit([&](auto&& arg) {
    using MethodType = std::decay_t<decltype(arg)>;
    if constexpr (!std::is_same_v<MethodType, std::monostate>) {
        arg.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
    }
}, method_variant);

// 3. 构建 Hash 表
std::visit([&](auto&& arg) {
    using MethodType = std::decay_t<decltype(arg)>;
    if constexpr (!std::is_same_v<MethodType, std::monostate>) {
        arg.hash_table->build(arg.keys, arg.bucket_nums.data(), num_rows, false);
    }
}, method_variant);

// 4. Probe 阶段
std::visit([&](auto&& arg) {
    using MethodType = std::decay_t<decltype(arg)>;
    if constexpr (!std::is_same_v<MethodType, std::monostate>) {
        auto [probe_idx, build_idx, matched_cnt] = 
            arg.hash_table->find_batch<TJoinOp::INNER_JOIN>(
                probe_keys, probe_bucket_nums.data(), 
                probe_idx, build_idx, probe_rows, ...);
    }
}, method_variant);
```

### 示例 2：多列键的 Hash Join

```cpp
// 键：(user_id INT64, order_id INT64)
// 总大小：8 + 8 = 16 bytes → FixedKeyHashTableContext<UInt128>

// 1. 初始化 Hash 表
HashTableVariants method_variant;
method_variant.emplace<FixedKeyHashTableContext<UInt128>>();

// 2. 初始化 Hash 表方法（打包键）
std::visit([&](auto&& arg) {
    using MethodType = std::decay_t<decltype(arg)>;
    if constexpr (!std::is_same_v<MethodType, std::monostate>) {
        // 将两列 INT64 打包成 UInt128
        arg.init_serialized_keys(key_columns, num_rows, nullptr, true, true, bucket_size);
    }
}, method_variant);

// 3. 构建 Hash 表
std::visit([&](auto&& arg) {
    using MethodType = std::decay_t<decltype(arg)>;
    if constexpr (!std::is_same_v<MethodType, std::monostate>) {
        arg.hash_table->build(arg.keys, arg.bucket_nums.data(), num_rows, false);
    }
}, method_variant);
```

### 示例 3：Direct Mapping 优化

```cpp
// 键：status_code (UInt8, 范围 0-10)
// 条件：max_key - min_key = 10 - 0 = 10 < MAX_MAPPING_RANGE (8,388,607)
// → 转换为 DirectPrimaryTypeHashTableContext

// 1. 初始化为 PrimaryTypeHashTableContext
HashTableVariants method_variant;
method_variant.emplace<PrimaryTypeHashTableContext<UInt8>>();

// 2. 构建 Hash 表并收集键值范围
std::visit([&](auto&& arg) {
    // ... 构建 Hash 表 ...
    // 收集 min_key 和 max_key
    UInt8 min_key = ...;
    UInt8 max_key = ...;
    
    // 3. 检查是否可以转换为 Direct Mapping
    if (max_key - min_key < MAX_MAPPING_RANGE) {
        // 转换为 DirectPrimaryTypeHashTableContext
        method_variant.emplace<DirectPrimaryTypeHashTableContext<UInt8>>();
        // 重新构建 Hash 表（使用直接映射）
    }
}, method_variant);
```

---

## 总结

Doris 的 Hash 表架构通过多层次的设计，实现了：

1. **类型安全**：通过模板参数和 `std::variant` 确保类型一致性
2. **性能优化**：针对不同键类型选择最优的实现策略
3. **代码复用**：通过继承和模板实现代码复用
4. **扩展性**：易于添加新的键类型和处理方法
5. **统一接口**：上层代码通过统一接口操作不同的 Hash 表

该架构在 Hash Join 和 Aggregation 场景中都表现出色，通过 Direct Mapping、零拷贝、固定长度键打包等优化技术，显著提升了性能。

---

## 参考资料

- `be/src/vec/common/hash_table/join_hash_table.h`：JoinHashTable 实现
- `be/src/vec/common/hash_table/hash_map_context.h`：Hash 表方法实现
- `be/src/pipeline/common/join_utils.h`：Hash 表变体定义
- `be/src/vec/common/hash_table/hash_key_type.h`：Hash 键类型选择
- `be/src/vec/common/hash_table/hash_map_util.h`：Hash 表工具函数

