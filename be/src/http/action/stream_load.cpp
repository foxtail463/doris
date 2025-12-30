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

#include "http/action/stream_load.h"

// use string iequal
#include <event2/buffer.h>
#include <event2/http.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <sys/time.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <future>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "cloud/config.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "io/fs/stream_load_pipe.h"
#include "olap/storage_engine.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/group_commit_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/message_body_sink.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/byte_buffer.h"
#include "util/doris_metrics.h"
#include "util/load_util.h"
#include "util/metrics.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace doris {
using namespace ErrorCode;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_requests_total, MetricUnit::REQUESTS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(streaming_load_duration_ms, MetricUnit::MILLISECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(streaming_load_current_processing, MetricUnit::REQUESTS);

bvar::LatencyRecorder g_stream_load_receive_data_latency_ms("stream_load_receive_data_latency_ms");
bvar::LatencyRecorder g_stream_load_commit_and_publish_latency_ms("stream_load",
                                                                  "commit_and_publish_ms");

static constexpr size_t MIN_CHUNK_SIZE = 64 * 1024;
static const std::string CHUNK = "chunked";

#ifdef BE_TEST
TStreamLoadPutResult k_stream_load_put_result;
#endif

StreamLoadAction::StreamLoadAction(ExecEnv* exec_env) : _exec_env(exec_env) {
    _stream_load_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("stream_load");
    INT_COUNTER_METRIC_REGISTER(_stream_load_entity, streaming_load_requests_total);
    INT_COUNTER_METRIC_REGISTER(_stream_load_entity, streaming_load_duration_ms);
    INT_GAUGE_METRIC_REGISTER(_stream_load_entity, streaming_load_current_processing);
}

StreamLoadAction::~StreamLoadAction() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_stream_load_entity);
}

void StreamLoadAction::handle(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr) {
        return;
    }

    // status already set to fail
    if (ctx->status.ok()) {
        ctx->status = _handle(ctx);
        if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
            LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status;
        }
    }
    ctx->load_cost_millis = UnixMillis() - ctx->start_millis;

    if (!ctx->status.ok() && !ctx->status.is<PUBLISH_TIMEOUT>()) {
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx.get());
            ctx->need_rollback = false;
        }
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
    }

    auto str = ctx->to_json();
    // add new line at end
    str = str + '\n';
    HttpChannel::send_reply(req, str);
#ifndef BE_TEST
    if (config::enable_stream_load_record || config::enable_stream_load_record_to_audit_log_table) {
        if (req->header(HTTP_SKIP_RECORD_TO_AUDIT_LOG_TABLE).empty()) {
            str = ctx->prepare_stream_load_record(str);
            _save_stream_load_record(ctx, str);
        }
    }
#endif

    LOG(INFO) << "finished to execute stream load. label=" << ctx->label
              << ", txn_id=" << ctx->txn_id << ", query_id=" << ctx->id
              << ", load_cost_ms=" << ctx->load_cost_millis << ", receive_data_cost_ms="
              << (ctx->receive_and_read_data_cost_nanos - ctx->read_data_cost_nanos) / 1000000
              << ", read_data_cost_ms=" << ctx->read_data_cost_nanos / 1000000
              << ", write_data_cost_ms=" << ctx->write_data_cost_nanos / 1000000
              << ", commit_and_publish_txn_cost_ms="
              << ctx->commit_and_publish_txn_cost_nanos / 1000000
              << ", number_total_rows=" << ctx->number_total_rows
              << ", number_loaded_rows=" << ctx->number_loaded_rows
              << ", receive_bytes=" << ctx->receive_bytes << ", loaded_bytes=" << ctx->loaded_bytes
              << ", error_url=" << ctx->error_url;

    // update statistics
    streaming_load_requests_total->increment(1);
    streaming_load_duration_ms->increment(ctx->load_cost_millis);
    if (!ctx->data_saved_path.empty()) {
        _exec_env->load_path_mgr()->clean_tmp_files(ctx->data_saved_path);
    }
}

Status StreamLoadAction::_handle(std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes=" << ctx->body_bytes
                     << ", receive_bytes=" << ctx->receive_bytes << ", id=" << ctx->id;
        return Status::Error<ErrorCode::NETWORK_ERROR>("receive body don't equal with body bytes");
    }

    // if we use non-streaming, MessageBodyFileSink.finish will close the file
    RETURN_IF_ERROR(ctx->body_sink->finish());
    if (!ctx->use_streaming) {
        // we need to close file first, then execute_plan_fragment here
        ctx->body_sink.reset();
        TPipelineFragmentParamsList mocked;
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx, mocked));
    }

    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());

    if (ctx->group_commit) {
        LOG(INFO) << "skip commit because this is group commit, pipe_id=" << ctx->id.to_string();
        return Status::OK();
    }

    if (ctx->two_phase_commit) {
        int64_t pre_commit_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->pre_commit_txn(ctx.get()));
        ctx->pre_commit_txn_cost_nanos = MonotonicNanos() - pre_commit_start_time;
    } else {
        // If put file success we need commit this load
        int64_t commit_and_publish_start_time = MonotonicNanos();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx.get()));
        ctx->commit_and_publish_txn_cost_nanos = MonotonicNanos() - commit_and_publish_start_time;
        g_stream_load_commit_and_publish_latency_ms
                << ctx->commit_and_publish_txn_cost_nanos / 1000000;
    }
    return Status::OK();
}

int StreamLoadAction::on_header(HttpRequest* req) {
    streaming_load_current_processing->increment(1);

    std::shared_ptr<StreamLoadContext> ctx = std::make_shared<StreamLoadContext>(_exec_env);
    req->set_handler_ctx(ctx);

    ctx->load_type = TLoadType::MANUL_LOAD;
    ctx->load_src_type = TLoadSourceType::RAW;

    url_decode(req->param(HTTP_DB_KEY), &ctx->db);
    url_decode(req->param(HTTP_TABLE_KEY), &ctx->table);
    ctx->label = req->header(HTTP_LABEL_KEY);
    ctx->two_phase_commit = req->header(HTTP_TWO_PHASE_COMMIT) == "true";
    Status st = _handle_group_commit(req, ctx);
    if (!ctx->group_commit && ctx->label.empty()) {
        ctx->label = generate_uuid_string();
    }

    LOG(INFO) << "new income streaming load request." << ctx->brief() << ", db=" << ctx->db
              << ", tbl=" << ctx->table << ", group_commit=" << ctx->group_commit
              << ", HTTP headers=" << req->get_all_headers();
    ctx->begin_receive_and_read_data_cost_nanos = MonotonicNanos();

    if (st.ok()) {
        st = _on_header(req, ctx);
        LOG(INFO) << "finished to handle HTTP header, " << ctx->brief();
    }
    if (!st.ok()) {
        ctx->status = std::move(st);
        if (ctx->need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(ctx.get());
            ctx->need_rollback = false;
        }
        if (ctx->body_sink != nullptr) {
            ctx->body_sink->cancel(ctx->status.to_string());
        }
        auto str = ctx->to_json();
        // add new line at end
        str = str + '\n';
        HttpChannel::send_reply(req, str);
#ifndef BE_TEST
        if (config::enable_stream_load_record ||
            config::enable_stream_load_record_to_audit_log_table) {
            if (req->header(HTTP_SKIP_RECORD_TO_AUDIT_LOG_TABLE).empty()) {
                str = ctx->prepare_stream_load_record(str);
                _save_stream_load_record(ctx, str);
            }
        }
#endif
        return -1;
    }
    return 0;
}

/**
 * 处理 Stream Load 请求的 HTTP Header
 * 
 * 该函数在 HTTP header 解析完成后被调用，负责：
 * 1. 解析和验证认证信息
 * 2. 解析数据格式和压缩类型
 * 3. 检查请求体大小限制
 * 4. 验证传输编码方式
 * 5. 解析超时和注释参数
 * 6. 开始事务（如果不是组提交模式）
 * 7. 调用 _process_put 处理 PUT 请求
 * 
 * @param http_req HTTP 请求对象，包含所有 HTTP headers
 * @param ctx Stream Load 上下文，用于存储解析后的参数和状态
 * @return Status 返回执行状态，成功返回 OK，失败返回相应的错误状态
 */
Status StreamLoadAction::_on_header(HttpRequest* http_req, std::shared_ptr<StreamLoadContext> ctx) {
    // 1. 解析和验证认证信息（Basic Auth）
    // 从 HTTP Authorization header 中提取用户名和密码
    if (!parse_basic_auth(*http_req, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        return Status::NotAuthorized("no valid Basic authorization");
    }

    // 2. 解析数据格式
    // 从 HTTP header 中获取 format 参数（如 csv、json 等）
    std::string format_str = http_req->header(HTTP_FORMAT_KEY);
    // 处理 CSV 格式的特殊变体（带列名或带列名和类型）
    if (iequal(format_str, BeConsts::CSV_WITH_NAMES) ||
        iequal(format_str, BeConsts::CSV_WITH_NAMES_AND_TYPES)) {
        ctx->header_type = format_str;  // 保存原始格式类型
        // 统一按 CSV 格式处理
        format_str = BeConsts::CSV;
    }
    // 解析格式字符串和压缩类型，设置到 ctx->format 和 ctx->compress_type
    LoadUtil::parse_format(format_str, http_req->header(HTTP_COMPRESS_TYPE), &ctx->format,
                           &ctx->compress_type);
    // 验证格式是否有效
    if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
        return Status::Error<ErrorCode::DATA_FILE_TYPE_ERROR>("unknown data format, format={}",
                                                              http_req->header(HTTP_FORMAT_KEY));
    }

    // 3. 检查请求体大小（Content-Length）
    ctx->body_bytes = 0;
    // 获取配置的最大 body 大小限制
    size_t csv_max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    size_t json_max_body_bytes = config::streaming_load_json_max_mb * 1024 * 1024;
    // 检查是否启用按行读取 JSON（用于大文件）
    bool read_json_by_line = false;
    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (iequal(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            read_json_by_line = true;
        }
    }
    // 如果指定了 Content-Length，解析并验证大小
    if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
        try {
            // 将 Content-Length 字符串转换为整数
            ctx->body_bytes = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
        } catch (const std::exception& e) {
            return Status::InvalidArgument("invalid HTTP header CONTENT_LENGTH={}: {}",
                                           http_req->header(HttpHeaders::CONTENT_LENGTH), e.what());
        }
        // JSON 格式的大小检查（如果未启用按行读取）
        if ((ctx->format == TFileFormatType::FORMAT_JSON) &&
            (ctx->body_bytes > json_max_body_bytes) && !read_json_by_line) {
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "json body size {} exceed BE's conf `streaming_load_json_max_mb` {}. increase "
                    "it if you are sure this load is reasonable",
                    ctx->body_bytes, json_max_body_bytes);
        }
        // CSV 或其他格式的大小检查
        else if (ctx->body_bytes > csv_max_body_bytes) {
            LOG(WARNING) << "body exceed max size." << ctx->brief();
            return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                    "body size {} exceed BE's conf `streaming_load_max_mb` {}. increase it if you "
                    "are sure this load is reasonable",
                    ctx->body_bytes, csv_max_body_bytes);
        }
    } else {
        // 如果未指定 Content-Length，设置 libevent 的最大 body 大小限制
#ifndef BE_TEST
        evhttp_connection_set_max_body_size(
                evhttp_request_get_connection(http_req->get_evhttp_request()), csv_max_body_bytes);
#endif
    }

    // 4. 检查传输编码方式（Transfer-Encoding）
    // 检查是否使用分块传输编码（chunked transfer encoding）
    if (!http_req->header(HttpHeaders::TRANSFER_ENCODING).empty()) {
        if (http_req->header(HttpHeaders::TRANSFER_ENCODING).find(CHUNK) != std::string::npos) {
            ctx->is_chunked_transfer = true;
        }
    }
    // 验证 Content-Length 和 Transfer-Encoding 的互斥性
    // 如果两者都未设置，返回错误
    if (UNLIKELY((http_req->header(HttpHeaders::CONTENT_LENGTH).empty() &&
                  !ctx->is_chunked_transfer))) {
        LOG(WARNING) << "content_length is empty and transfer-encoding!=chunked, please set "
                        "content_length or transfer-encoding=chunked";
        return Status::InvalidArgument(
                "content_length is empty and transfer-encoding!=chunked, please set content_length "
                "or transfer-encoding=chunked");
    } 
    // 如果两者都设置了，返回错误（HTTP 协议不允许同时设置）
    else if (UNLIKELY(!http_req->header(HttpHeaders::CONTENT_LENGTH).empty() &&
                        ctx->is_chunked_transfer)) {
        LOG(WARNING) << "please do not set both content_length and transfer-encoding";
        return Status::InvalidArgument(
                "please do not set both content_length and transfer-encoding");
    }

    // 5. 解析其他可选参数
    // 解析超时时间（秒）
    if (!http_req->header(HTTP_TIMEOUT).empty()) {
        ctx->timeout_second = DORIS_TRY(safe_stoi(http_req->header(HTTP_TIMEOUT), HTTP_TIMEOUT));
    }
    // 解析注释信息
    if (!http_req->header(HTTP_COMMENT).empty()) {
        ctx->load_comment = http_req->header(HTTP_COMMENT);
    }
    
    // 6. 开始事务（如果不是组提交模式）
    // 组提交模式下，事务由组提交管理器统一管理，不需要在这里开始
    if (!ctx->group_commit) {
        int64_t begin_txn_start_time = MonotonicNanos();
        // 调用 StreamLoadExecutor 开始事务
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx.get()));
        // 记录开始事务的耗时
        ctx->begin_txn_cost_nanos = MonotonicNanos() - begin_txn_start_time;
    }

    // 7. 处理 PUT 请求
    // 调用 _process_put 继续处理请求，包括：
    // - 构建 TStreamLoadPutRequest
    // - 调用 FE 的 streamLoadPut RPC 获取执行计划
    // - 设置 body_sink（StreamLoadPipe 或 MessageBodyFileSink）
    return _process_put(http_req, ctx);
}

void StreamLoadAction::on_chunk_data(HttpRequest* req) {
    std::shared_ptr<StreamLoadContext> ctx =
            std::static_pointer_cast<StreamLoadContext>(req->handler_ctx());
    if (ctx == nullptr || !ctx->status.ok()) {
        return;
    }

    struct evhttp_request* ev_req = req->get_evhttp_request();
    auto evbuf = evhttp_request_get_input_buffer(ev_req);

    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->stream_load_pipe_tracker());

    int64_t start_read_data_time = MonotonicNanos();
    while (evbuffer_get_length(evbuf) > 0) {
        ByteBufferPtr bb;
        Status st = ByteBuffer::allocate(128 * 1024, &bb);
        if (!st.ok()) {
            ctx->status = st;
            return;
        }
        auto remove_bytes = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        bb->pos = remove_bytes;
        bb->flip();
        st = ctx->body_sink->append(bb);
        if (!st.ok()) {
            LOG(WARNING) << "append body content failed. errmsg=" << st << ", " << ctx->brief();
            ctx->status = st;
            return;
        }
        ctx->receive_bytes += remove_bytes;
    }
    int64_t read_data_time = MonotonicNanos() - start_read_data_time;
    int64_t last_receive_and_read_data_cost_nanos = ctx->receive_and_read_data_cost_nanos;
    ctx->read_data_cost_nanos += read_data_time;
    ctx->receive_and_read_data_cost_nanos =
            MonotonicNanos() - ctx->begin_receive_and_read_data_cost_nanos;
    g_stream_load_receive_data_latency_ms
            << (ctx->receive_and_read_data_cost_nanos - last_receive_and_read_data_cost_nanos -
                read_data_time) /
                       1000000;
}

void StreamLoadAction::free_handler_ctx(std::shared_ptr<void> param) {
    std::shared_ptr<StreamLoadContext> ctx = std::static_pointer_cast<StreamLoadContext>(param);
    if (ctx == nullptr) {
        return;
    }
    // sender is gone, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel("sender is gone");
    }
    // remove stream load context from stream load manager and the resource will be released
    ctx->exec_env()->new_load_stream_mgr()->remove(ctx->id);
    streaming_load_current_processing->increment(-1);
}

/**
 * 处理 Stream Load PUT 请求
 * 
 * 该函数负责：
 * 1. 确定是否使用流式模式（根据数据格式）
 * 2. 构建 TStreamLoadPutRequest，设置所有请求参数
 * 3. 设置 body_sink（StreamLoadPipe 或 MessageBodyFileSink）
 * 4. 解析 HTTP headers 中的所有参数（列映射、分隔符、部分更新模式等）
 * 5. 调用 FE 的 streamLoadPut RPC 获取执行计划
 * 6. 如果使用流式模式，立即执行计划
 * 
 * @param http_req HTTP 请求对象，包含所有 HTTP headers
 * @param ctx Stream Load 上下文，用于存储解析后的参数和状态
 * @return Status 返回执行状态，成功返回 OK，失败返回相应的错误状态
 */
Status StreamLoadAction::_process_put(HttpRequest* http_req,
                                      std::shared_ptr<StreamLoadContext> ctx) {
    // 1. 确定是否使用流式模式
    // 根据数据格式判断是否支持流式处理（如 CSV、JSON 支持，Parquet 不支持）
    ctx->use_streaming = LoadUtil::is_format_support_streaming(ctx->format);

    // 2. 构建 TStreamLoadPutRequest
    // 创建请求对象，用于调用 FE 的 streamLoadPut RPC
    TStreamLoadPutRequest request;
    
    // 设置基本认证信息
    set_request_auth(&request, ctx->auth);
    
    // 设置数据库和表名
    request.db = ctx->db;
    request.tbl = ctx->table;
    
    // 设置事务 ID（在 _on_header 中已开始事务）
    request.txnId = ctx->txn_id;
    
    // 设置数据格式和压缩类型
    request.formatType = ctx->format;
    request.__set_compress_type(ctx->compress_type);
    request.__set_header_type(ctx->header_type);
    
    // 设置 Load ID（用于标识本次导入）
    request.__set_loadId(ctx->id.to_thrift());
    
    // 3. 设置 body_sink（数据接收器）
    if (ctx->use_streaming) {
        // 流式模式：使用 StreamLoadPipe（内存缓冲区）
        // 数据会边接收边处理，不需要等待全部数据到达
        std::shared_ptr<io::StreamLoadPipe> pipe;
        if (ctx->is_chunked_transfer) {
            // 分块传输编码（chunked transfer encoding）
            // 不知道总大小，使用无限制的 pipe
            pipe = std::make_shared<io::StreamLoadPipe>(
                    io::kMaxPipeBufferedBytes /* max_buffered_bytes */);
            pipe->set_is_chunked_transfer(true);
        } else {
            // 普通传输（Content-Length 已知）
            // 知道总大小，可以优化内存分配
            pipe = std::make_shared<io::StreamLoadPipe>(
                    io::kMaxPipeBufferedBytes /* max_buffered_bytes */,
                    MIN_CHUNK_SIZE /* min_chunk_size */, ctx->body_bytes /* total_length */);
        }
        request.fileType = TFileType::FILE_STREAM;
        ctx->body_sink = pipe;
        ctx->pipe = pipe;
        // 将 context 注册到 StreamLoadManager，供 Pipeline 读取数据
        RETURN_IF_ERROR(_exec_env->new_load_stream_mgr()->put(ctx->id, ctx));
    } else {
        // 非流式模式：使用 MessageBodyFileSink（临时文件）
        // 需要先接收全部数据到文件，然后再处理
        RETURN_IF_ERROR(_data_saved_path(http_req, &request.path, ctx->body_bytes));
        auto file_sink = std::make_shared<MessageBodyFileSink>(request.path);
        RETURN_IF_ERROR(file_sink->open());
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        request.__set_file_size(ctx->body_bytes);
        ctx->body_sink = file_sink;
        ctx->data_saved_path = request.path;
    }
    // 4. 解析 HTTP headers 中的参数，设置到 request
    
    // 列映射（columns）：指定如何将输入列映射到表列
    // 例如：columns: id,name,age 或 columns: tmp_c1,tmp_c2,tmp_c3 => id,name,age
    if (!http_req->header(HTTP_COLUMNS).empty()) {
        request.__set_columns(http_req->header(HTTP_COLUMNS));
    }
    
    // WHERE 条件：过滤导入的数据
    if (!http_req->header(HTTP_WHERE).empty()) {
        request.__set_where(http_req->header(HTTP_WHERE));
    }
    
    // 列分隔符（column_separator）：CSV 格式的列分隔符，默认是 \t
    if (!http_req->header(HTTP_COLUMN_SEPARATOR).empty()) {
        request.__set_columnSeparator(http_req->header(HTTP_COLUMN_SEPARATOR));
    }
    
    // 行分隔符（line_delimiter）：CSV 格式的行分隔符，默认是 \n
    if (!http_req->header(HTTP_LINE_DELIMITER).empty()) {
        request.__set_line_delimiter(http_req->header(HTTP_LINE_DELIMITER));
    }
    // 引号字符（enclose）：CSV 格式中用于包围字段的字符，默认是 "
    if (!http_req->header(HTTP_ENCLOSE).empty() && !http_req->header(HTTP_ENCLOSE).empty()) {
        const auto& enclose_str = http_req->header(HTTP_ENCLOSE);
        if (enclose_str.length() != 1) {
            return Status::InvalidArgument("enclose must be single-char, actually is {}",
                                           enclose_str);
        }
        request.__set_enclose(http_req->header(HTTP_ENCLOSE)[0]);
    }
    
    // 转义字符（escape）：CSV 格式中用于转义特殊字符的字符，默认是反斜杠
    if (!http_req->header(HTTP_ESCAPE).empty() && !http_req->header(HTTP_ESCAPE).empty()) {
        const auto& escape_str = http_req->header(HTTP_ESCAPE);
        if (escape_str.length() != 1) {
            return Status::InvalidArgument("escape must be single-char, actually is {}",
                                           escape_str);
        }
        request.__set_escape(http_req->header(HTTP_ESCAPE)[0]);
    }
    // 分区信息（partitions）：指定要导入的分区
    if (!http_req->header(HTTP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_PARTITIONS));
        request.__set_isTempPartition(false);
        // 不能同时指定普通分区和临时分区
        if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    
    // 临时分区信息（temp_partitions）：指定要导入的临时分区
    if (!http_req->header(HTTP_TEMP_PARTITIONS).empty()) {
        request.__set_partitions(http_req->header(HTTP_TEMP_PARTITIONS));
        request.__set_isTempPartition(true);
        // 不能同时指定普通分区和临时分区
        if (!http_req->header(HTTP_PARTITIONS).empty()) {
            return Status::InvalidArgument(
                    "Can not specify both partitions and temporary partitions");
        }
    }
    
    // 负数导入（negative）：是否导入负数（用于数据回滚）
    if (!http_req->header(HTTP_NEGATIVE).empty() && http_req->header(HTTP_NEGATIVE) == "true") {
        request.__set_negative(true);
    } else {
        request.__set_negative(false);
    }
    // 严格模式（strict_mode）：是否启用严格模式（数据类型转换更严格）
    bool strictMode = false;
    if (!http_req->header(HTTP_STRICT_MODE).empty()) {
        if (iequal(http_req->header(HTTP_STRICT_MODE), "false")) {
            strictMode = false;
        } else if (iequal(http_req->header(HTTP_STRICT_MODE), "true")) {
            strictMode = true;
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
        request.__set_strictMode(strictMode);
    }
    
    // 时区（timezone）：用于时间类型数据的解析
    // 优先使用 timezone，如果没有则使用 time_zone
    if (!http_req->header(HTTP_TIMEZONE).empty()) {
        request.__set_timezone(http_req->header(HTTP_TIMEZONE));
    } else if (!http_req->header(HTTP_TIME_ZONE).empty()) {
        request.__set_timezone(http_req->header(HTTP_TIME_ZONE));
    }
    
    // 执行内存限制（exec_mem_limit）：限制执行计划的内存使用
    if (!http_req->header(HTTP_EXEC_MEM_LIMIT).empty()) {
        try {
            request.__set_execMemLimit(std::stoll(http_req->header(HTTP_EXEC_MEM_LIMIT)));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid mem limit format, {}", e.what());
        }
    }
    // JSON 格式相关参数
    // JSON 路径（jsonpaths）：指定 JSON 数据的路径表达式
    if (!http_req->header(HTTP_JSONPATHS).empty()) {
        request.__set_jsonpaths(http_req->header(HTTP_JSONPATHS));
    }
    
    // JSON 根节点（json_root）：指定 JSON 数据的根节点
    if (!http_req->header(HTTP_JSONROOT).empty()) {
        request.__set_json_root(http_req->header(HTTP_JSONROOT));
    }
    
    // 去除外层数组（strip_outer_array）：是否去除 JSON 的最外层数组
    if (!http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        if (iequal(http_req->header(HTTP_STRIP_OUTER_ARRAY), "true")) {
            request.__set_strip_outer_array(true);
        } else {
            request.__set_strip_outer_array(false);
        }
    } else {
        request.__set_strip_outer_array(false);
    }

    // 按行读取 JSON（read_json_by_line）：是否按行读取 JSON（每行一个 JSON 对象）
    if (!http_req->header(HTTP_READ_JSON_BY_LINE).empty()) {
        if (iequal(http_req->header(HTTP_READ_JSON_BY_LINE), "true")) {
            request.__set_read_json_by_line(true);
        } else {
            request.__set_read_json_by_line(false);
        }
    } else {
        request.__set_read_json_by_line(false);
    }

    // 默认行为：如果两个参数都未指定，默认按行读取 JSON
    if (http_req->header(HTTP_READ_JSON_BY_LINE).empty() &&
        http_req->header(HTTP_STRIP_OUTER_ARRAY).empty()) {
        request.__set_read_json_by_line(true);
        request.__set_strip_outer_array(false);
    }

    // 数字作为字符串（num_as_string）：是否将 JSON 中的数字解析为字符串
    if (!http_req->header(HTTP_NUM_AS_STRING).empty()) {
        if (iequal(http_req->header(HTTP_NUM_AS_STRING), "true")) {
            request.__set_num_as_string(true);
        } else {
            request.__set_num_as_string(false);
        }
    } else {
        request.__set_num_as_string(false);
    }
    
    // 模糊解析（fuzzy_parse）：是否启用 JSON 的模糊解析模式
    if (!http_req->header(HTTP_FUZZY_PARSE).empty()) {
        if (iequal(http_req->header(HTTP_FUZZY_PARSE), "true")) {
            request.__set_fuzzy_parse(true);
        } else {
            request.__set_fuzzy_parse(false);
        }
    } else {
        request.__set_fuzzy_parse(false);
    }

    // 序列列（sequence_col）：用于解决相同 key 的行的冲突
    if (!http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL).empty()) {
        request.__set_sequence_col(
                http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL));
    }

    // 发送批次并行度（send_batch_parallelism）：控制发送数据的并行度
    if (!http_req->header(HTTP_SEND_BATCH_PARALLELISM).empty()) {
        int parallelism = DORIS_TRY(safe_stoi(http_req->header(HTTP_SEND_BATCH_PARALLELISM),
                                              HTTP_SEND_BATCH_PARALLELISM));
        request.__set_send_batch_parallelism(parallelism);
    }

    // 加载到单个 Tablet（load_to_single_tablet）：是否将所有数据加载到单个 Tablet
    if (!http_req->header(HTTP_LOAD_TO_SINGLE_TABLET).empty()) {
        if (iequal(http_req->header(HTTP_LOAD_TO_SINGLE_TABLET), "true")) {
            request.__set_load_to_single_tablet(true);
        } else {
            request.__set_load_to_single_tablet(false);
        }
    }

    // 超时时间（timeout）：导入操作的超时时间（秒）
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    
    // Thrift RPC 超时时间
    request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);
    
    // 合并类型（merge_type）：指定数据的合并方式
    // APPEND: 追加模式（默认）
    // DELETE: 删除模式
    // MERGE: 合并模式（需要指定 DELETE ON 条件）
    TMergeType::type merge_type = TMergeType::APPEND;
    StringCaseMap<TMergeType::type> merge_type_map = {{"APPEND", TMergeType::APPEND},
                                                      {"DELETE", TMergeType::DELETE},
                                                      {"MERGE", TMergeType::MERGE}};
    if (!http_req->header(HTTP_MERGE_TYPE).empty()) {
        std::string merge_type_str = http_req->header(HTTP_MERGE_TYPE);
        auto iter = merge_type_map.find(merge_type_str);
        if (iter != merge_type_map.end()) {
            merge_type = iter->second;
        } else {
            return Status::InvalidArgument("Invalid merge type {}", merge_type_str);
        }
        // MERGE 模式必须指定 DELETE ON 条件
        if (merge_type == TMergeType::MERGE && http_req->header(HTTP_DELETE_CONDITION).empty()) {
            return Status::InvalidArgument("Excepted DELETE ON clause when merge type is MERGE.");
        } 
        // 非 MERGE 模式不能指定 DELETE ON 条件
        else if (merge_type != TMergeType::MERGE &&
                   !http_req->header(HTTP_DELETE_CONDITION).empty()) {
            return Status::InvalidArgument(
                    "Not support DELETE ON clause when merge type is not MERGE.");
        }
    }
    request.__set_merge_type(merge_type);
    
    // DELETE 条件（delete_condition）：MERGE 模式下，指定哪些行应该被删除
    if (!http_req->header(HTTP_DELETE_CONDITION).empty()) {
        request.__set_delete_condition(http_req->header(HTTP_DELETE_CONDITION));
    }

    // 最大过滤比例（max_filter_ratio）：允许过滤的最大行数比例
    if (!http_req->header(HTTP_MAX_FILTER_RATIO).empty()) {
        ctx->max_filter_ratio = strtod(http_req->header(HTTP_MAX_FILTER_RATIO).c_str(), nullptr);
        request.__set_max_filter_ratio(ctx->max_filter_ratio);
    }

    // 隐藏列（hidden_columns）：指定隐藏列（用于部分列更新等场景）
    if (!http_req->header(HTTP_HIDDEN_COLUMNS).empty()) {
        request.__set_hidden_columns(http_req->header(HTTP_HIDDEN_COLUMNS));
    }
    
    // 去除双引号（trim_double_quotes）：是否去除 CSV 字段两端的双引号
    if (!http_req->header(HTTP_TRIM_DOUBLE_QUOTES).empty()) {
        if (iequal(http_req->header(HTTP_TRIM_DOUBLE_QUOTES), "true")) {
            request.__set_trim_double_quotes(true);
        } else {
            request.__set_trim_double_quotes(false);
        }
    }
    
    // 跳过行数（skip_lines）：CSV 格式中跳过的行数（通常用于跳过表头）
    if (!http_req->header(HTTP_SKIP_LINES).empty()) {
        int skip_lines = DORIS_TRY(safe_stoi(http_req->header(HTTP_SKIP_LINES), HTTP_SKIP_LINES));
        if (skip_lines < 0) {
            return Status::InvalidArgument("Invalid 'skip_lines': {}", skip_lines);
        }
        request.__set_skip_lines(skip_lines);
    }
    
    // 启用 Profile（enable_profile）：是否启用性能分析
    if (!http_req->header(HTTP_ENABLE_PROFILE).empty()) {
        if (iequal(http_req->header(HTTP_ENABLE_PROFILE), "true")) {
            request.__set_enable_profile(true);
        } else {
            request.__set_enable_profile(false);
        }
    }

    // 5. 部分列更新相关参数
    
    // Unique Key 更新模式（unique_key_update_mode）：指定 Unique Key 表的更新模式
    // UPSERT: 完整更新（默认）
    // UPDATE_FIXED_COLUMNS: 固定部分更新（所有行更新相同的列集合）
    // UPDATE_FLEXIBLE_COLUMNS: 灵活部分更新（每行可以更新不同的列集合）
    if (!http_req->header(HTTP_UNIQUE_KEY_UPDATE_MODE).empty()) {
        static const StringCaseMap<TUniqueKeyUpdateMode::type> unique_key_update_mode_map = {
                {"UPSERT", TUniqueKeyUpdateMode::UPSERT},
                {"UPDATE_FIXED_COLUMNS", TUniqueKeyUpdateMode::UPDATE_FIXED_COLUMNS},
                {"UPDATE_FLEXIBLE_COLUMNS", TUniqueKeyUpdateMode::UPDATE_FLEXIBLE_COLUMNS}};
        std::string unique_key_update_mode_str = http_req->header(HTTP_UNIQUE_KEY_UPDATE_MODE);
        auto iter = unique_key_update_mode_map.find(unique_key_update_mode_str);
        if (iter != unique_key_update_mode_map.end()) {
            TUniqueKeyUpdateMode::type unique_key_update_mode = iter->second;
            
            // 灵活部分更新的约束检查
            if (unique_key_update_mode == TUniqueKeyUpdateMode::UPDATE_FLEXIBLE_COLUMNS) {
                // 灵活部分更新只支持 JSON 格式
                if (ctx->format != TFileFormatType::FORMAT_JSON) {
                    return Status::InvalidArgument(
                            "flexible partial update only support json format as input file "
                            "currently");
                }
                // 不能启用模糊解析
                if (!http_req->header(HTTP_FUZZY_PARSE).empty() &&
                    iequal(http_req->header(HTTP_FUZZY_PARSE), "true")) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'fuzzy_parse' is enabled");
                }
                // 不能指定列映射
                if (!http_req->header(HTTP_COLUMNS).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'columns' is specified");
                }
                // 不能指定 JSON 路径
                if (!http_req->header(HTTP_JSONPATHS).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'jsonpaths' is specified");
                }
                // 不能指定隐藏列
                if (!http_req->header(HTTP_HIDDEN_COLUMNS).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when 'hidden_columns' is "
                            "specified");
                }
                // 不能指定序列列
                if (!http_req->header(HTTP_FUNCTION_COLUMN + "." + HTTP_SEQUENCE_COL).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when "
                            "'function_column.sequence_col' is specified");
                }
                // 不能指定合并类型
                if (!http_req->header(HTTP_MERGE_TYPE).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when "
                            "'merge_type' is specified");
                }
                // 不能指定 WHERE 条件
                if (!http_req->header(HTTP_WHERE).empty()) {
                    return Status::InvalidArgument(
                            "Don't support flexible partial update when "
                            "'where' is specified");
                }
            }
            request.__set_unique_key_update_mode(unique_key_update_mode);
        } else {
            return Status::InvalidArgument(
                    "Invalid unique_key_partial_mode {}, must be one of 'UPSERT', "
                    "'UPDATE_FIXED_COLUMNS' or 'UPDATE_FLEXIBLE_COLUMNS'",
                    unique_key_update_mode_str);
        }
    }

    // 兼容旧版本：partial_columns 参数（等同于 UPDATE_FIXED_COLUMNS）
    // 只有在 unique_key_update_mode 未设置时才考虑此参数
    if (http_req->header(HTTP_UNIQUE_KEY_UPDATE_MODE).empty() &&
        !http_req->header(HTTP_PARTIAL_COLUMNS).empty()) {
        if (iequal(http_req->header(HTTP_PARTIAL_COLUMNS), "true")) {
            request.__set_unique_key_update_mode(TUniqueKeyUpdateMode::UPDATE_FIXED_COLUMNS);
            // 为了向后兼容，同时设置 partial_update 标志
            request.__set_partial_update(true);
        }
    }

    // 部分更新新行策略（partial_update_new_row_policy）：当部分更新遇到新行时的处理策略
    // APPEND: 追加新行（默认）
    // ERROR: 报错（要求所有行都必须存在）
    if (!http_req->header(HTTP_PARTIAL_UPDATE_NEW_ROW_POLICY).empty()) {
        static const std::map<std::string, TPartialUpdateNewRowPolicy::type> policy_map {
                {"APPEND", TPartialUpdateNewRowPolicy::APPEND},
                {"ERROR", TPartialUpdateNewRowPolicy::ERROR}};

        auto policy_name = http_req->header(HTTP_PARTIAL_UPDATE_NEW_ROW_POLICY);
        // 转换为大写以便匹配
        std::transform(policy_name.begin(), policy_name.end(), policy_name.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        auto it = policy_map.find(policy_name);
        if (it == policy_map.end()) {
            return Status::InvalidArgument(
                    "Invalid partial_update_new_key_behavior {}, must be one of {'APPEND', "
                    "'ERROR'}",
                    policy_name);
        }
        request.__set_partial_update_new_key_policy(it->second);
    }

    // 其他高级参数
    // MemTable 在 SinkNode（memtable_on_sink_node）：是否在 SinkNode 创建 MemTable
    if (!http_req->header(HTTP_MEMTABLE_ON_SINKNODE).empty()) {
        bool value = iequal(http_req->header(HTTP_MEMTABLE_ON_SINKNODE), "true");
        request.__set_memtable_on_sink_node(value);
    }
    
    // 每个节点的流数量（load_stream_per_node）：控制每个节点的数据流数量
    if (!http_req->header(HTTP_LOAD_STREAM_PER_NODE).empty()) {
        int stream_per_node = DORIS_TRY(
                safe_stoi(http_req->header(HTTP_LOAD_STREAM_PER_NODE), HTTP_LOAD_STREAM_PER_NODE));
        request.__set_stream_per_node(stream_per_node);
    }
    
    // 组提交模式（group_commit）：指定组提交的模式（sync_mode 或 async_mode）
    if (ctx->group_commit) {
        if (!http_req->header(HTTP_GROUP_COMMIT).empty()) {
            request.__set_group_commit_mode(http_req->header(HTTP_GROUP_COMMIT));
        } else {
            // 默认使用同步模式（用于等待内部组提交完成）
            request.__set_group_commit_mode("sync_mode");
        }
    }

    // 云集群（cloud_cluster）：指定云模式下的计算集群
    if (!http_req->header(HTTP_COMPUTE_GROUP).empty()) {
        request.__set_cloud_cluster(http_req->header(HTTP_COMPUTE_GROUP));
    } else if (!http_req->header(HTTP_CLOUD_CLUSTER).empty()) {
        request.__set_cloud_cluster(http_req->header(HTTP_CLOUD_CLUSTER));
    }

    // 空字段作为 NULL（empty_field_as_null）：是否将空字段解析为 NULL
    if (!http_req->header(HTTP_EMPTY_FIELD_AS_NULL).empty()) {
        if (iequal(http_req->header(HTTP_EMPTY_FIELD_AS_NULL), "true")) {
            request.__set_empty_field_as_null(true);
        }
    }

    // 6. 调用 FE 获取执行计划
#ifndef BE_TEST
    // 获取 FE 主节点地址
    TNetworkAddress master_addr = _exec_env->cluster_info()->master_fe_addr;
    int64_t stream_load_put_start_time = MonotonicNanos();
    
    // 通过 Thrift RPC 调用 FE 的 streamLoadPut 方法
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx](FrontendServiceConnection& client) {
                // 调用 FE 的 streamLoadPut RPC
                // FE 会生成执行计划并返回 TPipelineFragmentParams
                client->streamLoadPut(ctx->put_result, request);
            }));
    
    // 记录调用 FE 的耗时
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - stream_load_put_start_time;
#else
    // 测试模式下，使用模拟结果
    ctx->put_result = k_stream_load_put_result;
#endif
    
    // 7. 检查执行计划的状态
    Status plan_status(Status::create(ctx->put_result.status));
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status << ctx->brief();
        return plan_status;
    }
    
    // 确保返回了 pipeline_params
    DCHECK(ctx->put_result.__isset.pipeline_params);
    
    // 设置查询选项
    // 禁用严格类型转换（允许更宽松的数据类型转换）
    ctx->put_result.pipeline_params.query_options.__set_enable_strict_cast(false);
    // 设置插入严格模式（根据用户指定的 strict_mode）
    ctx->put_result.pipeline_params.query_options.__set_enable_insert_strict(strictMode);
    
    // 云模式下，MOW 表不支持两阶段提交
    if (config::is_cloud_mode() && ctx->two_phase_commit && ctx->is_mow_table()) {
        return Status::NotSupported("stream load 2pc is unsupported for mow table");
    }
    
    // 异步组提交模式：设置内容长度（用于 WAL 大小估算）
    if (http_req->header(HTTP_GROUP_COMMIT) == "async_mode") {
        // TODO: 找到避免分块流式加载写入大 WAL 的方法
        size_t content_length = 0;
        if (!http_req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
            try {
                content_length = std::stol(http_req->header(HttpHeaders::CONTENT_LENGTH));
            } catch (const std::exception& e) {
                return Status::InvalidArgument("invalid HTTP header CONTENT_LENGTH={}: {}",
                                               http_req->header(HttpHeaders::CONTENT_LENGTH),
                                               e.what());
            }
            // 压缩格式的数据，估算解压后的大小（乘以 3）
            if (ctx->format == TFileFormatType::FORMAT_CSV_GZ ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZO ||
                ctx->format == TFileFormatType::FORMAT_CSV_BZ2 ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZ4FRAME ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZOP ||
                ctx->format == TFileFormatType::FORMAT_CSV_LZ4BLOCK ||
                ctx->format == TFileFormatType::FORMAT_CSV_SNAPPYBLOCK) {
                content_length *= 3;
            }
        }
        ctx->put_result.pipeline_params.__set_content_length(content_length);
    }

    // 记录执行计划参数（用于调试）
    VLOG_NOTICE << "params is "
                << apache::thrift::ThriftDebugString(ctx->put_result.pipeline_params);
    
    // 8. 执行计划
    // 非流式模式：需要先下载全部内容，然后在 _handle 中执行计划
    if (!ctx->use_streaming) {
        return Status::OK();
    }
    
    // 流式模式：立即执行计划
    // Pipeline 会从 StreamLoadPipe 中读取数据并处理
    TPipelineFragmentParamsList mocked;
    return _exec_env->stream_load_executor()->execute_plan_fragment(ctx, mocked);
}

Status StreamLoadAction::_data_saved_path(HttpRequest* req, std::string* file_path,
                                          int64_t file_bytes) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(req->param(HTTP_DB_KEY), "", &prefix,
                                                             file_bytes));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << "/" << req->param(HTTP_TABLE_KEY) << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}

void StreamLoadAction::_save_stream_load_record(std::shared_ptr<StreamLoadContext> ctx,
                                                const std::string& str) {
    std::shared_ptr<StreamLoadRecorder> stream_load_recorder =
            ExecEnv::GetInstance()->storage_engine().get_stream_load_recorder();

    if (stream_load_recorder != nullptr) {
        std::string key =
                std::to_string(ctx->start_millis + ctx->load_cost_millis) + "_" + ctx->label;
        auto st = stream_load_recorder->put(key, str);
        if (st.ok()) {
            LOG(INFO) << "put stream_load_record rocksdb successfully. label: " << ctx->label
                      << ", key: " << key;
        }
    } else {
        LOG(WARNING) << "put stream_load_record rocksdb failed. stream_load_recorder is null.";
    }
}

Status StreamLoadAction::_handle_group_commit(HttpRequest* req,
                                              std::shared_ptr<StreamLoadContext> ctx) {
    std::string group_commit_mode = req->header(HTTP_GROUP_COMMIT);
    if (!group_commit_mode.empty() && !iequal(group_commit_mode, "sync_mode") &&
        !iequal(group_commit_mode, "async_mode") && !iequal(group_commit_mode, "off_mode")) {
        return Status::InvalidArgument(
                "group_commit can only be [async_mode, sync_mode, off_mode]");
    }
    if (config::wait_internal_group_commit_finish) {
        group_commit_mode = "sync_mode";
    }
    int64_t content_length = req->header(HttpHeaders::CONTENT_LENGTH).empty()
                                     ? 0
                                     : std::stoll(req->header(HttpHeaders::CONTENT_LENGTH));
    if (content_length < 0) {
        std::stringstream ss;
        ss << "This stream load content length <0 (" << content_length
           << "), please check your content length.";
        LOG(WARNING) << ss.str();
        return Status::InvalidArgument(ss.str());
    }
    // allow chunked stream load in flink
    auto is_chunk = !req->header(HttpHeaders::TRANSFER_ENCODING).empty() &&
                    req->header(HttpHeaders::TRANSFER_ENCODING).find(CHUNK) != std::string::npos;
    if (group_commit_mode.empty() || iequal(group_commit_mode, "off_mode") ||
        (content_length == 0 && !is_chunk)) {
        // off_mode and empty
        ctx->group_commit = false;
        return Status::OK();
    }
    if (is_chunk) {
        ctx->label = "";
    }

    auto partial_columns = !req->header(HTTP_PARTIAL_COLUMNS).empty() &&
                           iequal(req->header(HTTP_PARTIAL_COLUMNS), "true");
    auto temp_partitions = !req->header(HTTP_TEMP_PARTITIONS).empty();
    auto partitions = !req->header(HTTP_PARTITIONS).empty();
    auto update_mode =
            !req->header(HTTP_UNIQUE_KEY_UPDATE_MODE).empty() &&
            (iequal(req->header(HTTP_UNIQUE_KEY_UPDATE_MODE), "UPDATE_FIXED_COLUMNS") ||
             iequal(req->header(HTTP_UNIQUE_KEY_UPDATE_MODE), "UPDATE_FLEXIBLE_COLUMNS"));
    if (!partial_columns && !partitions && !temp_partitions && !ctx->two_phase_commit &&
        !update_mode) {
        if (!config::wait_internal_group_commit_finish && !ctx->label.empty()) {
            return Status::InvalidArgument("label and group_commit can't be set at the same time");
        }
        ctx->group_commit = true;
        if (iequal(group_commit_mode, "async_mode")) {
            if (!load_size_smaller_than_wal_limit(content_length)) {
                std::stringstream ss;
                ss << "There is no space for group commit stream load async WAL. This stream load "
                      "size is "
                   << content_length << ". WAL dir info: "
                   << ExecEnv::GetInstance()->wal_mgr()->get_wal_dirs_info_string();
                LOG(WARNING) << ss.str();
                return Status::Error<EXCEEDED_LIMIT>(ss.str());
            }
        }
    }
    return Status::OK();
}

} // namespace doris
