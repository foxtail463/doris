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

#include "olap/rowset/rowset_factory.h"

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "beta_rowset.h"
#include "cloud/cloud_rowset_writer.h"
#include "cloud/config.h"
#include "io/fs/file_writer.h" // IWYU pragma: keep
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/vertical_beta_rowset_writer.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {
using namespace ErrorCode;

Status RowsetFactory::create_rowset(const TabletSchemaSPtr& schema, const std::string& tablet_path,
                                    const RowsetMetaSharedPtr& rowset_meta,
                                    RowsetSharedPtr* rowset) {
    if (rowset_meta->rowset_type() == ALPHA_ROWSET) {
        return Status::Error<ROWSET_INVALID>("invalid rowset_type");
    }
    if (rowset_meta->rowset_type() == BETA_ROWSET) {
        rowset->reset(new BetaRowset(schema, rowset_meta, tablet_path));
        return (*rowset)->init();
    }
    return Status::Error<ROWSET_TYPE_NOT_FOUND>("invalid rowset_type"); // should never happen
}

/**
 * 创建行集写入器的工厂方法
 * 根据行集类型和写入模式创建相应的行集写入器
 * @param engine 存储引擎引用，用于行集写入器的初始化
 * @param context 行集写入上下文，包含行集类型、schema等信息
 * @param is_vertical 是否为垂直写入模式（列式写入）
 * @return 成功返回行集写入器指针，失败返回错误状态
 */
Result<std::unique_ptr<RowsetWriter>> RowsetFactory::create_rowset_writer(
        StorageEngine& engine, const RowsetWriterContext& context, bool is_vertical) {
    
    // 检查行集类型：ALPHA_ROWSET已被废弃，不再支持
    if (context.rowset_type == ALPHA_ROWSET) {
        return ResultError(Status::Error<ROWSET_INVALID>("invalid rowset_type"));
    }

    // 处理BETA_ROWSET类型：这是当前支持的主要行集类型
    if (context.rowset_type == BETA_ROWSET) {
        std::unique_ptr<RowsetWriter> writer;
        
        if (is_vertical) {
            // 垂直写入模式：创建垂直Beta行集写入器
            // 垂直写入器按列组织数据，适合列式存储和查询优化
            writer = std::make_unique<VerticalBetaRowsetWriter<BetaRowsetWriter>>(engine);
        } else {
            // 水平写入模式：创建标准Beta行集写入器
            // 水平写入器按行组织数据，适合传统的行式存储
            writer = std::make_unique<BetaRowsetWriter>(engine);
        }
        
        // 初始化行集写入器，传入上下文信息
        RETURN_IF_ERROR_RESULT(writer->init(context));
        
        // 返回初始化成功的写入器
        return writer;
    }

    // 不支持的行集类型，返回错误
    return ResultError(Status::Error<ROWSET_TYPE_NOT_FOUND>("invalid rowset_type"));
}

Result<std::unique_ptr<RowsetWriter>> RowsetFactory::create_rowset_writer(
        CloudStorageEngine& engine, const RowsetWriterContext& context, bool is_vertical) {
    DCHECK_EQ(context.rowset_type, BETA_ROWSET);
    // TODO(plat1ko): cloud vertical rowset writer
    std::unique_ptr<RowsetWriter> writer;
    if (is_vertical) {
        writer = std::make_unique<VerticalBetaRowsetWriter<CloudRowsetWriter>>(engine);
    } else {
        writer = std::make_unique<CloudRowsetWriter>(engine);
    }

    RETURN_IF_ERROR_RESULT(writer->init(context));
    return writer;
}

} // namespace doris
