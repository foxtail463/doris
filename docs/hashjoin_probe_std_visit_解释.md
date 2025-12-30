# Hash Join Probe 中 std::visit 代码解释

## 问题：这段代码在做什么？

这段代码的核心目的是：**根据 Hash 表的实际类型，调用对应的 process() 方法进行 Hash Join**。

## 为什么需要这么复杂？

因为 Doris 支持多种类型的 Hash 表：
- `SerializedHashTableContext` - 序列化 Hash 表（用于字符串等复杂类型）
- `PrimaryTypeHashTableContext<UInt8>` - UInt8 类型的 Hash 表
- `PrimaryTypeHashTableContext<UInt16>` - UInt16 类型的 Hash 表
- `PrimaryTypeHashTableContext<UInt32>` - UInt32 类型的 Hash 表
- `PrimaryTypeHashTableContext<UInt64>` - UInt64 类型的 Hash 表
- ... 等等

每种 Hash 表类型都有不同的 `process()` 方法实现，但接口相同。

## 语言特性解释

### 1. std::variant（变体类型）

```cpp
// 这是一个"类型安全的联合体"
// 可以存储多种不同类型的值，但同一时刻只能存储一种类型
std::variant<int, std::string, double> v;

v = 42;           // 存储 int
v = "hello";      // 存储 string
v = 3.14;         // 存储 double

// 但不知道当前存储的是哪种类型，需要 std::visit 来访问
```

**类比**：就像一个"智能盒子"，可以装不同类型的物品，但一次只能装一种。

### 2. std::visit（访问 variant）

```cpp
// std::visit 会根据 variant 中实际存储的类型，调用对应的处理函数
std::visit([](auto&& value) {
    // 这里的 value 类型会根据 variant 中实际存储的类型自动推导
    if constexpr (std::is_same_v<decltype(value), int>) {
        std::cout << "这是 int: " << value << std::endl;
    } else if constexpr (std::is_same_v<decltype(value), std::string>) {
        std::cout << "这是 string: " << value << std::endl;
    }
}, v);
```

**类比**：就像一个"智能识别器"，能识别盒子里的物品类型，并调用对应的处理函数。

### 3. Lambda 表达式（匿名函数）

```cpp
// Lambda 表达式就是一个匿名函数
[&](auto&& arg, auto&& process_hashtable_ctx) {
    // 函数体
}

// 等价于：
void lambda_function(auto&& arg, auto&& process_hashtable_ctx) {
    // 函数体
}
```

**`[&]`**：捕获所有外部变量（按引用）
**`auto&&`**：万能引用，可以接受任何类型

### 4. if constexpr（编译时条件判断）

```cpp
if constexpr (condition) {
    // 如果 condition 为 true，这段代码会被编译
    // 如果 condition 为 false，这段代码不会被编译（而不是运行时跳过）
}

// 普通 if：运行时判断
if (condition) {
    // 无论 condition 是 true 还是 false，这段代码都会被编译
    // 只是运行时根据 condition 决定是否执行
}
```

**类比**：
- **普通 if**：编译所有代码，运行时选择执行
- **if constexpr**：编译时选择编译哪些代码

### 5. std::decay_t（类型转换）

```cpp
// std::decay_t 会去除引用、const、volatile 等修饰符
std::decay_t<int&>        → int
std::decay_t<const int>   → int
std::decay_t<int&&>       → int
```

**类比**：就像"去除包装"，只保留核心类型。

### 6. std::monostate（空状态）

```cpp
// std::monostate 表示 variant 未初始化或为空
std::variant<std::monostate, int, std::string> v;

v = std::monostate{};  // 设置为空状态
```

**类比**：就像"空盒子"，表示还没有装任何东西。

## 代码逐步解释

### 原始代码

```cpp
std::visit(
    [&](auto&& arg, auto&& process_hashtable_ctx) {
        using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
        if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
            using HashTableCtxType = std::decay_t<decltype(arg)>;
            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                st = process_hashtable_ctx.process(arg, ...);
            } else {
                st = Status::InternalError("uninited hash table");
            }
        } else {
            st = Status::InternalError("uninited hash table probe");
        }
    },
    hash_table_variant,  // 第一个参数：Hash 表变体
    process_ctx_variant  // 第二个参数：处理上下文变体
);
```

### 简化理解

```cpp
// 伪代码，帮助理解
void process_hash_join(HashTableVariant hash_table, ProcessCtxVariant process_ctx) {
    // 根据 hash_table 的实际类型，调用对应的 process() 方法
    
    if (hash_table 是 SerializedHashTableContext) {
        SerializedHashTableContext::process(...);
    } else if (hash_table 是 PrimaryTypeHashTableContext<UInt8>) {
        PrimaryTypeHashTableContext<UInt8>::process(...);
    } else if (hash_table 是 PrimaryTypeHashTableContext<UInt16>) {
        PrimaryTypeHashTableContext<UInt16>::process(...);
    } else if (hash_table 是 PrimaryTypeHashTableContext<UInt32>) {
        PrimaryTypeHashTableContext<UInt32>::process(...);
    } else if (hash_table 是 PrimaryTypeHashTableContext<UInt64>) {
        PrimaryTypeHashTableContext<UInt64>::process(...);
    }
    // ... 等等
}
```

### 实际执行流程

1. **std::visit 识别类型**：
   - 检查 `hash_table_variant` 中实际存储的是什么类型的 Hash 表
   - 检查 `process_ctx_variant` 中实际存储的是什么类型的处理上下文

2. **Lambda 函数被调用**：
   - `arg` 被设置为实际的 Hash 表类型（如 `SerializedHashTableContext`）
   - `process_hashtable_ctx` 被设置为实际的处理上下文类型

3. **类型检查**：
   - `if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>)`：
     - 检查处理上下文是否已初始化（不是空状态）
   - `if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>)`：
     - 检查 Hash 表是否已初始化（不是空状态）

4. **调用 process() 方法**：
   - 如果都初始化了，调用 `process_hashtable_ctx.process(arg, ...)`
   - 编译器会根据实际类型，选择对应的 `process()` 实现

## 为什么使用这种方式？

### 传统方式（不使用 variant）

```cpp
// 需要写很多 if-else
if (hash_table_type == SERIALIZED) {
    SerializedHashTableContext::process(...);
} else if (hash_table_type == UINT8) {
    PrimaryTypeHashTableContext<UInt8>::process(...);
} else if (hash_table_type == UINT16) {
    PrimaryTypeHashTableContext<UInt16>::process(...);
}
// ... 需要写很多代码
```

**问题**：
- 容易出错（类型不匹配）
- 性能差（运行时判断）
- 代码冗长

### 使用 variant + visit 方式

```cpp
// 类型安全，编译器自动选择正确的实现
std::visit([&](auto&& arg, auto&& ctx) {
    ctx.process(arg, ...);
}, hash_table_variant, process_ctx_variant);
```

**优势**：
- 类型安全（编译时检查）
- 性能好（编译时优化）
- 代码简洁

## 实际例子

假设 Hash 表类型是 `PrimaryTypeHashTableContext<UInt64>`：

```cpp
// 1. std::visit 识别出类型
arg = PrimaryTypeHashTableContext<UInt64> 实例
process_hashtable_ctx = ProcessHashTableProbe<PrimaryTypeHashTableContext<UInt64>> 实例

// 2. 类型检查通过（都不是 monostate）

// 3. 调用 process() 方法
process_hashtable_ctx.process(
    arg,  // PrimaryTypeHashTableContext<UInt64> 实例
    null_map_column,
    mutable_join_block,
    &temp_block,
    probe_block_rows,
    is_mark_join
);

// 4. 编译器会选择 PrimaryTypeHashTableContext<UInt64> 的 process() 实现
```

## 总结

这段代码的核心思想是：
1. **使用 std::variant 存储多种类型的 Hash 表**
2. **使用 std::visit 根据实际类型自动选择处理函数**
3. **使用 if constexpr 进行编译时类型检查**
4. **最终调用对应类型的 process() 方法进行 Hash Join**

这是一种**类型安全的多态实现**，避免了运行时类型判断的开销。

