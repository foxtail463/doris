# 部分更新忽略 NULL 值功能实现计划

## 1. 需求概述

### 1.1 问题描述
在 Unique Key 模型的部分更新场景中，当新插入的行包含 NULL 值时，当前实现会用 NULL 覆盖表中已有的值。用户希望添加一个选项，使得当新值为 NULL 时，保留表中已有的值而不是用 NULL 覆盖。

### 1.2 使用场景示例
**现有数据：**
```
id = 1, name = "Alice", age = 30
```

**新插入数据：**
```
id = 1, name = NULL, age = 31
```

**期望行为（启用 ignore_null_values）：**
```
id = 1, name = "Alice", age = 31  // name 保留原值，因为新值是 NULL
```

**当前行为（未启用 ignore_null_values）：**
```
id = 1, name = NULL, age = 31  // name 被 NULL 覆盖
```

### 1.3 参考 Issue
- GitHub Issue: https://github.com/apache/doris/issues/59195

### 1.4 实现复杂性分析

**关键问题：为什么 PMC 说"太复杂，不是简单开关的事"？**

部分更新确实需要读取历史数据，但**只读取缺失列（missing_cids）**，而**部分更新中指定的列（update_cids）是直接写入的，不读历史数据**。

**当前流程（固定部分更新）：**
1. `update_cids`（部分更新中指定的列）：**直接写入 Segment**，不读历史数据
2. `missing_cids`（缺失列）：**从历史数据读取**并填充

**如果要实现"忽略 NULL"：**
- `update_cids` 的列：如果新值是 NULL，需要**读历史数据**来保留旧值
- 这改变了 `update_cids` 的处理逻辑：从"直接写入"变成"条件性读取+写入"

**复杂性在于：**
1. **写入顺序问题**：`update_cids` 的列已经在写入流程中直接写入了，如果要忽略 NULL，需要在写入前判断，如果为 NULL 就不写入新值，而是读历史数据写入旧值
2. **读取计划问题**：原来只为 `missing_cids` 建立 read plan，现在需要为 `update_cids` 中值为 NULL 的列也建立 read plan
3. **条件性读取**：需要判断哪些行的哪些列是 NULL，然后只读这些列的历史数据（不是所有行都需要读）
4. **写入逻辑**：如果新值是 NULL，需要读历史数据，然后写入旧值（而不是新值），这改变了原有的写入流程

**示例：**
```
现有数据：id=1, name="Alice", age=30
新插入：  id=1, name=NULL, age=31

当前流程：
- update_cids = [name, age]
- missing_cids = []（假设没有其他列）
- name=NULL 直接写入 → name 变成 NULL ❌
- age=31 直接写入 → age 变成 31 ✅

如果要忽略 NULL：
- name=NULL → 需要读历史数据 → name="Alice" ✅
- age=31 → 直接写入 → age=31 ✅
- 但 name 列需要额外的读取逻辑，增加了复杂性
```

---

## 2. 技术方案

### 2.1 架构设计

部分更新在 Doris 中有两个主要路径：

1. **固定部分更新（UPDATE_FIXED_COLUMNS）**
   - 在 `FixedReadPlan::fill_missing_columns()` 中填充缺失列
   - 位置：`be/src/olap/partial_update_info.cpp:367`

2. **灵活部分更新（UPDATE_FLEXIBLE_COLUMNS）**
   - 在 `FlexibleReadPlan::fill_non_primary_key_columns()` 中填充缺失列
   - 在 `BlockAggregator::merge_one_row()` 中合并行
   - 位置：`be/src/olap/partial_update_info.cpp:573, 797`

### 2.2 数据流

```
Stream Load HTTP Request
  ↓
StreamLoadAction::on_header()
  ↓
解析 HTTP header: ignore_null_values=true/false
  ↓
TStreamLoadPutRequest
  ↓
DeltaWriterV2::_build_current_tablet_schema()
  ↓
PartialUpdateInfo::init()
  ↓ (设置 ignore_null_values 标志)
  ↓
MemTableWriter::write()
  ↓
MemTable::insert()
  ↓
MemTable::flush()
  ↓
VerticalSegmentWriter::write_batch()
  ↓
填充缺失列（检查 NULL 值）
  ├─> FixedReadPlan::fill_missing_columns()      (固定部分更新)
  └─> FlexibleReadPlan::fill_non_primary_key_columns()  (灵活部分更新)
      └─> BlockAggregator::merge_one_row()       (灵活部分更新聚合)
```

---

## 3. 实现步骤

### 3.1 步骤 1：添加配置字段

#### 3.1.1 修改 `PartialUpdateInfo` 结构体

**文件：** `be/src/olap/partial_update_info.h`

```cpp
struct PartialUpdateInfo {
    // ... 现有字段 ...
    
    // 是否忽略 NULL 值：当新值为 NULL 时，保留旧值而不是用 NULL 覆盖
    bool ignore_null_values {false};
    
    // ... 其他字段 ...
};
```

#### 3.1.2 修改 `PartialUpdateInfo::init()` 方法

**文件：** `be/src/olap/partial_update_info.cpp`

在 `init()` 方法中添加 `ignore_null_values` 参数：

```cpp
Status PartialUpdateInfo::init(
    int64_t tablet_id, 
    int64_t txn_id, 
    const TabletSchema& tablet_schema,
    UniqueKeyUpdateModePB unique_key_update_mode, 
    PartialUpdateNewRowPolicyPB policy,
    const std::set<std::string>& partial_update_cols, 
    bool is_strict_mode,
    int64_t timestamp_ms, 
    int32_t nano_seconds, 
    const std::string& timezone,
    const std::string& auto_increment_column, 
    int32_t sequence_map_col_uid = -1,
    int64_t cur_max_version = -1,
    bool ignore_null_values = false  // 新增参数
);
```

#### 3.1.3 修改 Protobuf 定义（如果需要持久化）

**文件：** `gensrc/proto/olap_file.proto`（如果存在）

在 `PartialUpdateInfoPB` 中添加字段：

```protobuf
message PartialUpdateInfoPB {
    // ... 现有字段 ...
    optional bool ignore_null_values = XX;  // 新增字段
}
```

### 3.2 步骤 2：HTTP 接口支持

#### 2.1 添加 HTTP Header 解析

**文件：** `be/src/http/action/stream_load.cpp`

在 `StreamLoadAction::on_header()` 中添加：

```cpp
// 解析 ignore_null_values 参数
if (!http_req->header(HTTP_IGNORE_NULL_VALUES).empty()) {
    std::string ignore_null_str = http_req->header(HTTP_IGNORE_NULL_VALUES);
    bool ignore_null_values = false;
    if (iequal(ignore_null_str, "true") || ignore_null_str == "1") {
        ignore_null_values = true;
    }
    request.__set_ignore_null_values(ignore_null_values);
}
```

**文件：** `be/src/http/http_common.h`

添加常量定义：

```cpp
#define HTTP_IGNORE_NULL_VALUES "ignore_null_values"
```

#### 2.2 修改 Thrift 定义

**文件：** `gensrc/thrift/FrontendService.thrift`

在 `TStreamLoadPutRequest` 中添加字段：

```thrift
struct TStreamLoadPutRequest {
    // ... 现有字段 ...
    255: optional bool ignore_null_values  // 新增字段
}
```

#### 2.3 FE 端传递参数

**文件：** `fe/fe-core/src/main/java/org/apache/doris/load/streamload/StreamLoadHandler.java`

在 `StreamLoadHandler` 中，从 `TStreamLoadPutRequest` 读取 `ignore_null_values` 参数，并传递到 `TableSchemaParam`：

```java
// 在 generatePlan() 或相关方法中
if (request.isSetIgnore_null_values()) {
    tableSchemaParam.setIgnoreNullValues(request.isIgnore_null_values());
}
```

**文件：** `fe/fe-core/src/main/java/org/apache/doris/load/streamload/TableSchemaParam.java`

添加 `ignore_null_values` 字段和 getter/setter：

```java
private boolean ignoreNullValues = false;

public boolean ignoreNullValues() {
    return ignoreNullValues;
}

public void setIgnoreNullValues(boolean ignoreNullValues) {
    this.ignoreNullValues = ignoreNullValues;
}
```

### 3.3 步骤 3：传递配置到 PartialUpdateInfo

#### 3.1 修改 `DeltaWriterV2::_build_current_tablet_schema()`

**文件：** `be/src/olap/delta_writer_v2.cpp`

```cpp
Status DeltaWriterV2::_build_current_tablet_schema(...) {
    // ... 现有代码 ...
    
    _partial_update_info = std::make_shared<PartialUpdateInfo>();
    RETURN_IF_ERROR(_partial_update_info->init(
        _req.tablet_id, 
        _req.txn_id, 
        *_tablet_schema,
        table_schema_param->unique_key_update_mode(),
        table_schema_param->partial_update_new_key_policy(),
        table_schema_param->partial_update_input_columns(),
        table_schema_param->is_strict_mode(), 
        table_schema_param->timestamp_ms(),
        table_schema_param->nano_seconds(), 
        table_schema_param->timezone(),
        table_schema_param->auto_increment_coulumn(),
        table_schema_param->sequence_map_col_uid(),
        -1,  // cur_max_version
        _req.ignore_null_values  // 新增参数
    ));
    
    return Status::OK();
}
```

#### 3.2 修改 `RowsetBuilder::init()`（如果需要支持旧版本）

**文件：** `be/src/olap/rowset_builder.cpp`

类似地传递 `ignore_null_values` 参数。

### 3.4 步骤 4：实现固定部分更新的 NULL 值处理

**关键问题**：`FixedReadPlan::fill_missing_columns()` 只处理 `missing_cids`（缺失列），而 `update_cids`（部分更新中指定的列）是在 `VerticalSegmentWriter::_append_block_with_partial_content()` 中直接写入的。

**需要修改的地方：**
1. **`VerticalSegmentWriter::_append_block_with_partial_content()`**：在写入 `update_cids` 的列之前，需要判断新值是否为 NULL
2. **`FixedReadPlan::fill_missing_columns()`**：需要同时处理 `update_cids` 中值为 NULL 的列

#### 4.1 修改 `VerticalSegmentWriter::_append_block_with_partial_content()`

**文件：** `be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp`

在写入 `update_cids` 的列之前，需要：
1. 判断新值是否为 NULL
2. 如果为 NULL 且启用了 `ignore_null_values`，需要读历史数据
3. 写入旧值（而不是新值）

**注意**：这需要修改写入流程，因为原来 `update_cids` 的列是直接写入的，现在需要条件性地读取历史数据。

**关键实现点：**
1. 在写入 `update_cids` 的列之前，需要先判断哪些行的哪些列是 NULL
2. 对于值为 NULL 的列，需要建立 read plan 来读取历史数据
3. 需要处理新键（不存在旧值）的情况：如果旧值不存在，NULL 值应该使用默认值（如果设置了默认值）
4. 序列列和删除标记列不应该受 `ignore_null_values` 影响

#### 4.2 修改 `FixedReadPlan::fill_missing_columns()`

**文件：** `be/src/olap/partial_update_info.cpp`

**注意**：`FixedReadPlan::fill_missing_columns()` 目前只处理 `missing_cids`（缺失列），如果要在这里处理 `update_cids` 中值为 NULL 的列，需要：
1. 扩展 `fill_missing_columns()` 的参数，传入 `update_cids` 和输入 Block
2. 或者创建一个新的方法来处理 `update_cids` 中值为 NULL 的列

**方案 A：在 `fill_missing_columns()` 中同时处理 `update_cids` 中值为 NULL 的列**

在 `fill_missing_columns()` 方法中，当填充 `update_cids` 的列时，检查新值是否为 NULL：

```cpp
Status FixedReadPlan::fill_missing_columns(...) {
    // ... 现有代码 ...
    
    // 填充 update_cids 的列（部分更新中指定的列）
    for (auto cid : update_cids) {
        const auto& tablet_column = tablet_schema.column(cid);
        auto& new_col = mutable_full_columns[cid];
        auto& cur_col = block->get_by_position(cid).column;
        
        for (size_t idx = 0; idx < block->rows(); idx++) {
            bool use_new_value = true;
            
            // 如果启用了 ignore_null_values，检查新值是否为 NULL
            if (info->ignore_null_values && tablet_column.is_nullable()) {
                const auto* nullable_col = 
                    assert_cast<const vectorized::ColumnNullable*>(cur_col.get());
                if (nullable_col->is_null_at(idx)) {
                    // 新值为 NULL，使用旧值
                    use_new_value = false;
                }
            }
            
            if (use_new_value) {
                new_col->insert_from(*cur_col, idx);
            } else {
                // 从 old_value_block 读取旧值
                auto pos_in_old_block = read_index[idx];
                new_col->insert_from(*old_value_block.get_by_position(cid).column, 
                                    pos_in_old_block);
            }
        }
    }
    
    // ... 现有代码 ...
}
```

### 3.5 步骤 5：实现灵活部分更新的 NULL 值处理

**关键问题**：灵活部分更新有两个写入路径：
1. `VerticalSegmentWriter::_append_block_with_flexible_partial_content()` - 灵活部分更新的写入路径
2. `FlexibleReadPlan::fill_non_primary_key_columns()` - 填充缺失列
3. `BlockAggregator::merge_one_row()` - 行聚合

#### 5.1 修改 `VerticalSegmentWriter::_append_block_with_flexible_partial_content()`

**文件：** `be/src/olap/rowset/segment_v2/vertical_segment_writer.cpp`

在灵活部分更新的写入路径中，也需要处理 `ignore_null_values`。与固定部分更新类似，需要在写入前判断新值是否为 NULL。

#### 5.2 修改 `FlexibleReadPlan::fill_one_cell` Lambda

**文件：** `be/src/olap/partial_update_info.cpp`

在 `fill_non_primary_key_columns_for_column_store()` 和 `fill_non_primary_key_columns_for_row_store()` 中的 `fill_one_cell` lambda 中：

```cpp
auto fill_one_cell = [&tablet_schema, &read_index, info](
    const TabletColumn& tablet_column, 
    uint32_t cid,
    vectorized::MutableColumnPtr& new_col,
    const vectorized::IColumn& default_value_col,
    const vectorized::IColumn& old_value_col,
    const vectorized::IColumn& cur_col, 
    std::size_t block_pos,
    uint32_t segment_pos, 
    bool skipped, 
    bool row_has_sequence_col,
    bool use_default, 
    const signed char* delete_sign_column_data
) {
    if (skipped) {
        // ... 现有逻辑（未指定的列，使用旧值）...
    } else {
        // 列已指定（在 skip_bitmap 中不存在）
        // 检查是否需要忽略 NULL 值
        if (info->ignore_null_values && tablet_column.is_nullable()) {
            const auto* nullable_col = 
                assert_cast<const vectorized::ColumnNullable*>(&cur_col);
            if (nullable_col->is_null_at(block_pos)) {
                // 新值为 NULL，使用旧值
                auto pos_in_old_block = read_index.at(cid).at(segment_pos);
                new_col->insert_from(old_value_col, pos_in_old_block);
                return;
            }
        }
        // 新值不为 NULL，使用新值
        new_col->insert_from(cur_col, block_pos);
    }
};
```

**注意**：在 `fill_one_cell` lambda 中，需要处理以下情况：
1. 如果列已指定（skipped = false）且新值为 NULL，需要从历史数据读取旧值
2. 需要处理新键（不存在旧值）的情况：如果旧值不存在，NULL 值应该使用默认值
3. 序列列和删除标记列不应该受 `ignore_null_values` 影响

#### 5.3 修改 `BlockAggregator::merge_one_row()`

**文件：** `be/src/olap/partial_update_info.cpp`

在 `merge_one_row()` 方法中，当合并行时检查 NULL 值：

```cpp
void BlockAggregator::merge_one_row(
    vectorized::MutableBlock& dst_block,
    vectorized::Block* src_block, 
    int rid,
    BitmapValue& skip_bitmap
) {
    auto* info = _writer._rowset_ctx->partial_update_info.get();
    
    for (size_t cid {_tablet_schema.num_key_columns()}; 
         cid < _tablet_schema.num_columns(); 
         cid++) {
        
        if (cid == _tablet_schema.skip_bitmap_col_idx()) {
            // ... 现有逻辑 ...
            continue;
        }
        
        if (!skip_bitmap.contains(_tablet_schema.column(cid).unique_id())) {
            // 列已指定
            const auto& tablet_column = _tablet_schema.column(cid);
            auto& src_col = src_block->get_by_position(cid).column;
            auto& dst_col = dst_block.mutable_columns()[cid];
            
            // 检查是否需要忽略 NULL 值
            if (info->ignore_null_values && tablet_column.is_nullable()) {
                const auto* nullable_col = 
                    assert_cast<const vectorized::ColumnNullable*>(src_col.get());
                if (nullable_col->is_null_at(rid)) {
                    // 新值为 NULL，保留旧值（不更新）
                    continue;
                }
            }
            
            // 使用新值
            dst_col->pop_back(1);
            dst_col->insert_from(*src_col, rid);
        }
    }
}
```

**注意**：在 `merge_one_row()` 中，需要处理以下情况：
1. 如果新值为 NULL，保留旧值（不更新），使用 `continue` 跳过
2. 需要确保序列列和删除标记列不受 `ignore_null_values` 影响
3. 需要处理 skip_bitmap 列的特殊逻辑

### 3.6 步骤 6：特殊列的处理

#### 6.1 序列列（Sequence Column）

序列列不应该受 `ignore_null_values` 影响，因为序列列用于去重和版本控制。

**处理方式：**
- 在检查 `ignore_null_values` 之前，先判断当前列是否为序列列
- 如果是序列列，直接使用新值（即使为 NULL）

#### 6.2 删除标记列（Delete Sign Column）

删除标记列不应该受 `ignore_null_values` 影响，因为删除标记列用于标记行的删除状态。

**处理方式：**
- 在检查 `ignore_null_values` 之前，先判断当前列是否为删除标记列
- 如果是删除标记列，直接使用新值（即使为 NULL）

#### 6.3 新键（不存在旧值）的处理

如果主键不存在（新键），且新值为 NULL，应该使用默认值（如果设置了默认值），而不是保留 NULL。

**处理方式：**
- 在 `use_default_or_null_flag` 为 `true` 时（新键），如果新值为 NULL，使用默认值
- 在 `use_default_or_null_flag` 为 `false` 时（旧键），如果新值为 NULL，使用旧值

### 3.7 步骤 7：序列化和反序列化支持

#### 7.1 修改 `PartialUpdateInfo::to_pb()`

**文件：** `be/src/olap/partial_update_info.cpp`

```cpp
void PartialUpdateInfo::to_pb(PartialUpdateInfoPB* partial_update_info) const {
    // ... 现有代码 ...
    partial_update_info->set_ignore_null_values(ignore_null_values);
}
```

#### 7.2 修改 `PartialUpdateInfo::from_pb()`

**文件：** `be/src/olap/partial_update_info.cpp`

```cpp
void PartialUpdateInfo::from_pb(PartialUpdateInfoPB* partial_update_info) {
    // ... 现有代码 ...
    if (partial_update_info->has_ignore_null_values()) {
        ignore_null_values = partial_update_info->ignore_null_values();
    }
}
```

---

## 4. 测试计划

### 4.1 单元测试

1. **固定部分更新测试**
   - 测试启用 `ignore_null_values` 时，NULL 值保留旧值
   - 测试未启用 `ignore_null_values` 时，NULL 值覆盖旧值
   - 测试非 NULL 值正常更新

2. **灵活部分更新测试**
   - 测试启用 `ignore_null_values` 时，NULL 值保留旧值
   - 测试未启用 `ignore_null_values` 时，NULL 值覆盖旧值
   - 测试 skip bitmap 和 NULL 值的组合场景

3. **边界情况测试**
   - 测试所有列都是 NULL 的情况
   - 测试部分列是 NULL 的情况
   - 测试非 nullable 列的处理（应该不受影响）

### 4.2 集成测试

1. **Stream Load 测试**
   ```bash
   # 测试用例 1：启用 ignore_null_values
   curl --location-trusted -u user:passwd \
     -H "label:test_ignore_null_1" \
     -H "format:json" \
     -H "unique_key_update_mode:UPDATE_FIXED_COLUMNS" \
     -H "partial_columns:id,name,age" \
     -H "ignore_null_values:true" \
     -T test_data.json \
     http://fe_host:8030/api/test_db/test_table/_stream_load
   ```

2. **数据验证**
   - 验证 NULL 值被正确保留
   - 验证非 NULL 值正常更新
   - 验证查询结果正确

### 4.3 性能测试

- 测试启用 `ignore_null_values` 的性能影响
- 对比启用前后的内存使用和 CPU 使用

---

## 5. 实现优先级

### 5.1 Phase 1：核心功能（必须）
1. ✅ 添加 `ignore_null_values` 字段到 `PartialUpdateInfo`
2. ✅ HTTP 接口支持
3. ✅ 固定部分更新的 NULL 值处理
4. ✅ 灵活部分更新的 NULL 值处理（fill_one_cell）
5. ✅ 基本单元测试

### 5.2 Phase 2：完善功能（重要）
1. ✅ 灵活部分更新的聚合处理（merge_one_row）
2. ✅ 序列化/反序列化支持
3. ✅ 集成测试
4. ✅ 文档更新

### 5.3 Phase 3：优化（可选）
1. ⬜ 性能优化
2. ⬜ 更多边界情况处理
3. ⬜ 监控指标添加

---

## 6. 风险评估

### 6.1 兼容性风险
- **低风险**：新增功能默认关闭，不影响现有行为
- **缓解措施**：默认值为 `false`，保持向后兼容

### 6.2 性能风险（重复，删除）
- **低风险**：仅在启用时增加 NULL 值检查
- **缓解措施**：性能测试验证影响

### 6.3 功能风险
- **高风险**：需要正确处理各种边界情况，特别是 `update_cids` 中值为 NULL 的列需要读历史数据，改变了原有的写入流程
- **缓解措施**：充分的单元测试和集成测试

### 6.4 语义风险
- **高风险**：改变了 Unique Key 模型的核心语义（"相同主键的行会被覆盖"），引入了条件性覆盖的语义
- **缓解措施**：
  - 默认关闭该功能，保持向后兼容
  - 在文档中明确说明语义变化
  - 建议用户优先考虑使用 Aggregate Key 模型的 `REPLACE_IF_NOT_NULL` 聚合类型

### 6.5 性能风险
- **中风险**：`update_cids` 中值为 NULL 的列需要读历史数据，增加了 I/O 开销
- **缓解措施**：
  - 性能测试验证影响
  - 优化读取逻辑，批量读取而不是逐行读取
  - 考虑缓存机制

---

## 7. 相关文件清单

### 7.1 核心文件
- `be/src/olap/partial_update_info.h`
- `be/src/olap/partial_update_info.cpp`
- `be/src/http/action/stream_load.cpp`
- `be/src/http/http_common.h`
- `be/src/olap/delta_writer_v2.cpp`
- `be/src/olap/rowset_builder.cpp`

### 7.2 Thrift/Protobuf 文件
- `gensrc/proto/InternalService.thrift`（或相应文件）
- `gensrc/proto/olap_file.proto`（如果存在）

### 7.3 测试文件
- `be/test/olap/partial_update_test.cpp`（新建或修改）
- `regression-test/suites/stream_load/test_stream_load_partial_update.groovy`（新建或修改）

---

## 8. 参考资源

- [GitHub Issue #59195](https://github.com/apache/doris/issues/59195)
- Doris 部分更新文档
- Stream Load 文档

---

## 9. 总结

本实现计划通过以下步骤实现部分更新忽略 NULL 值功能：

1. **配置层**：在 `PartialUpdateInfo` 中添加 `ignore_null_values` 标志
2. **接口层**：在 HTTP Stream Load 接口中添加参数支持
3. **实现层**：在固定部分更新和灵活部分更新的填充逻辑中检查 NULL 值
4. **测试层**：添加单元测试和集成测试
5. **文档层**：更新用户文档和代码注释

该功能默认关闭，保持向后兼容，不影响现有行为。实现后，用户可以通过设置 `ignore_null_values=true` 来启用该功能，避免 NULL 值覆盖已有数据。


