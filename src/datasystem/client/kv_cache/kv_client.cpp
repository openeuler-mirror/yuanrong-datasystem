/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Data system State Cache Client management.
 */
#include "datasystem/kv_client.h"

#include <climits>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
KVClient::KVClient(const ConnectOptions &connectOptions)
{
    impl_ = std::make_unique<object_cache::ObjectClientImpl>(connectOptions);
}

KVClient::~KVClient()
{
    metrics::PrintSummary();
    if (impl_) {
        impl_.reset();
    }
}

Status KVClient::ShutDown()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    if (impl_) {
        bool needRollbackState;
        auto rc = impl_->ShutDown(needRollbackState);
        impl_->CompleteHandler(rc.IsError(), needRollbackState);
        return rc;
    }
    return Status::OK();
}

Status KVClient::Init()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    (void)metrics::InitKvMetrics();
    bool needRollbackState;
    auto rc = impl_->Init(needRollbackState, true);
    impl_->CompleteHandler(rc.IsError(), needRollbackState);
    return rc;
}

KVClient &KVClient::EmbeddedInstance()
{
    ConnectOptions opts;
    opts.port = -1;
    static KVClient instance(opts);
    return instance;
}

Status KVClient::InitEmbedded(const EmbeddedConfig &config)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    (void)metrics::InitKvMetrics();
    bool needRollbackState;
    auto &instance = KVClient::EmbeddedInstance();
    auto rc = instance.impl_->InitEmbedded(config, needRollbackState);
    instance.impl_->CompleteHandler(rc.IsError(), needRollbackState);
    return rc;
}

Status KVClient::Create(const std::string &key, uint64_t size, const SetParam &param, std::shared_ptr<Buffer> &buffer)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_CREATE_BUFFER);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_CREATE);
    object_cache::FullParam creatParam;
    creatParam.writeMode = param.writeMode;
    creatParam.ttlSecond = param.ttlSecond;
    creatParam.consistencyType = ConsistencyType::CAUSAL;
    creatParam.cacheType = param.cacheType;
    creatParam.existence = param.existence;
    Status rc = impl_->Create(key, size, creatParam, buffer);
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(param.writeMode));
    reqParam.ttlSecond = std::to_string(param.ttlSecond);
    reqParam.existence = std::to_string(static_cast<int>(param.existence));
    reqParam.cacheType = std::to_string(static_cast<int>(param.cacheType));
    accessPoint.Record(rc.GetCode(), std::to_string(size), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
const SetParam &param, std::vector<std::shared_ptr<Buffer>> &buffers)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_MCREATE_BUFFERS);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_MCREATE);
    object_cache::FullParam creatParam;
    creatParam.writeMode = param.writeMode;
    creatParam.ttlSecond = param.ttlSecond;
    creatParam.consistencyType = ConsistencyType::CAUSAL;
    creatParam.cacheType = param.cacheType;
    creatParam.existence = param.existence;
    Status rc = impl_->MCreate(keys, sizes, creatParam, buffers);
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    RequestParam reqParam;
    std::string key = (keys.empty() ? "" : keys[0]);
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(param.writeMode));
    reqParam.ttlSecond = std::to_string(param.ttlSecond);
    reqParam.existence = std::to_string(static_cast<int>(param.existence));
    reqParam.cacheType = std::to_string(static_cast<int>(param.cacheType));
    accessPoint.Record(rc.GetCode(), std::to_string(sizes.size()), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Set(const std::shared_ptr<Buffer> &buffer)
{
    CHECK_FAIL_RETURN_STATUS(buffer != nullptr, K_INVALID, "Buffer must not be null.");
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_SET_BUFFER);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_SET);
    Status rc = impl_->Set(buffer);
    RequestParam reqParam;
    reqParam.objectKey = buffer->bufferInfo_->objectKey;
    accessPoint.Record(rc.GetCode(), std::to_string(buffer->GetSize()), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::MSet(const std::vector<std::shared_ptr<Buffer>> &buffers)
{
    CHECK_FAIL_RETURN_STATUS(!buffers.empty(), K_INVALID, "Buffer list should not be empty.");
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_MSET_BUFFERS);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_MSET);
    Status rc = impl_->MSet(buffers);
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    RequestParam reqParam;
    reqParam.objectKey = buffers[0]->bufferInfo_->objectKey;
    accessPoint.Record(rc.GetCode(), std::to_string(buffers.size()), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Get(const std::string &key, Optional<Buffer> &buffer, int32_t subTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_BUFFER);
    std::vector<Optional<Buffer>> buffers;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    Status rc = impl_->Get({ key }, subTimeoutMs, buffers);
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    size_t dataSize = rc.IsOk() ? buffers[0]->GetSize() : 0;
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.timeout = std::to_string(subTimeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(dataSize), reqParam, rc.GetMsg());
    RETURN_IF_NOT_OK(rc);
    buffer = std::move(buffers[0]);
    return rc;
}

Status KVClient::Get(const std::vector<std::string> &keys,
                     std::vector<Optional<Buffer>> &buffers, int32_t subTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_MUL_BUFFERS);
    std::vector<Optional<Buffer>> tmpBuffers;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    Status rc = impl_->Get(keys, subTimeoutMs, buffers);
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    reqParam.timeout = std::to_string(subTimeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(buffers.size()), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Set(const std::string &key, const StringView &val, const SetParam &setParam)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_SET_OBJECT);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_SET);
    Status rc = impl_->Set(key, val, setParam);
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(setParam.writeMode));
    reqParam.ttlSecond = std::to_string(setParam.ttlSecond);
    reqParam.existence = std::to_string(static_cast<int>(setParam.existence));
    reqParam.cacheType = std::to_string(static_cast<int>(setParam.cacheType));
    accessPoint.Record(rc.GetCode(), std::to_string(val.size()), reqParam, rc.GetMsg());
    return rc;
}

std::string KVClient::Set(const StringView &val, const SetParam &setParam)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_SET_OBJECT);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_SET);
    std::string key;
    auto rc = impl_->Set(val, setParam, key);
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(setParam.writeMode));
    reqParam.ttlSecond = std::to_string(setParam.ttlSecond);
    reqParam.cacheType = std::to_string(static_cast<int>(setParam.cacheType));
    accessPoint.Record(rc.GetCode(), std::to_string(val.size()), reqParam);
    return key;
}

Status KVClient::UpdateToken(SensitiveValue token)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->UpdateToken(token);
}

Status KVClient::UpdateAkSk(const std::string accesskey, SensitiveValue secretkey)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->UpdateAkSk(accesskey, secretkey);
}

Status KVClient::MSetTx(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                           const MSetParam &param)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_MSET_OBJECT);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_MSETNX);
    Status rc = impl_->MSet(keys, vals, param);
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    RequestParam reqParam;
    std::string key = (keys.empty() ? "" : keys[0]);
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(param.writeMode));
    reqParam.ttlSecond = std::to_string(param.ttlSecond);
    reqParam.existence = std::to_string(static_cast<int>(param.existence));
    reqParam.cacheType = std::to_string(static_cast<int>(param.cacheType));
    accessPoint.Record(rc.GetCode(), std::to_string(vals.size()), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::MGetH2D(const std::vector<std::string> &keys,
                         const std::vector<std::pair<void *, size_t>> &devShmChunk,
                         std::vector<std::string> &outFailedKeys, int32_t subTimeoutMs)
{
    PerfPoint point(PerfKey::KV_CLIENT_MGET_H2D);
    LOG(ERROR) << "RH2D:MGetH2D start";
    std::shared_future<AsyncResult> future = AsyncMGetH2D(keys, devShmChunk, subTimeoutMs);
    auto result = future.get();
    LOG(ERROR) << "RH2D:MGetH2D end";
    outFailedKeys = std::move(result.failedList);
    return result.status;
}

std::shared_future<AsyncResult> KVClient::AsyncMGetH2D(const std::vector<std::string> &keys,
                                                       const std::vector<std::pair<void *, size_t>> &devShmChunk,
                                                       int32_t subTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->GetWithOsTransportPipeline(keys, devShmChunk, subTimeoutMs);
}

Status KVClient::Get(const std::string &key, std::string &val, int32_t timeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_OBJECT);
    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> vals;
    size_t dataSize = 0;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    Status rc = impl_->GetWithLatch({ key }, vals, timeoutMs, buffers, dataSize);
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.timeout = std::to_string(timeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(dataSize), reqParam, rc.GetMsg());
    if (rc.IsOk()) {
        val = std::move(vals[0]);
    }
    return rc;
}

Status KVClient::Get(const std::vector<std::string> &keys, std::vector<std::string> &vals, int32_t subTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_MUL_OBJECTS);
    std::vector<Optional<Buffer>> buffers;
    size_t dataSize = 0;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    Status rc = impl_->GetWithLatch(keys, vals, subTimeoutMs, buffers, dataSize);
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    reqParam.timeout = std::to_string(subTimeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(dataSize), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Get(const std::string &key, Optional<ReadOnlyBuffer> &readOnlyBuffer, int32_t subTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_BUFFER);
    std::vector<Optional<Buffer>> buffers;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    Status rc = impl_->Get({ key }, subTimeoutMs, buffers);
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    size_t dataSize = rc.IsOk() ? buffers[0]->GetSize() : 0;
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.timeout = std::to_string(subTimeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(dataSize), reqParam, rc.GetMsg());
    RETURN_IF_NOT_OK(rc);
    auto bufferSharedPtr = std::make_shared<Buffer>(std::move(buffers[0].value()));
    readOnlyBuffer = Optional<ReadOnlyBuffer>(ReadOnlyBuffer(bufferSharedPtr));
    return rc;
}

Status KVClient::MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                         std::vector<std::string> &outFailedKeys, const MSetParam &param)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_MSET_OBJECT);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_MSETNX);
    Status rc = impl_->MSet(keys, vals, param, outFailedKeys);
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    RequestParam reqParam;
    std::string key = (keys.empty() ? "" : keys[0]);
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.writeMode = std::to_string(static_cast<int>(param.writeMode));
    reqParam.ttlSecond = std::to_string(param.ttlSecond);
    reqParam.existence = std::to_string(static_cast<int>(param.existence));
    reqParam.cacheType = std::to_string(static_cast<int>(param.cacheType));
    accessPoint.Record(rc.GetCode(), std::to_string(vals.size()), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Get(const std::vector<std::string> &keys, std::vector<Optional<ReadOnlyBuffer>> &readOnlyBuffers,
                     int32_t subTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_MUL_BUFFERS);
    std::vector<Optional<Buffer>> buffers;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    Status rc = impl_->Get(keys, subTimeoutMs, buffers);
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    METRIC_ERROR_IF(rc.IsError(), metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    int64_t dataSize = 0;
    if (rc.IsOk()) {
        readOnlyBuffers.clear();
        for (auto &buffer : buffers) {
            if (buffer) {
                dataSize += buffer->GetSize();
                auto bufferSharedPtr = std::make_shared<Buffer>(std::move(buffer.value()));
                readOnlyBuffers.emplace_back(ReadOnlyBuffer(bufferSharedPtr));
            } else {
                readOnlyBuffers.emplace_back();
            }
        }
    }
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    reqParam.timeout = std::to_string(subTimeoutMs);
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(dataSize), reqParam, rc.GetMsg());
    return rc;
}

static std::string ReadParamToString(const std::vector<ReadParam> &params)
{
    std::string ret = "[";
    uint64_t len = 0;
    for (const auto &param : params) {
        std::string msg = FormatString("[%s, off:%ld, size:%ld],", param.key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
                                       param.offset, param.size);
        uint64_t size = msg.size();
        if (size <= LOG_TOTAL_KEYS_SIZE_LIMIT && len > LOG_TOTAL_KEYS_SIZE_LIMIT - size) {
            ret.append("total:").append(std::to_string(params.size())).append("]");
            return ret;
        }
        len += size;
        ret.append(msg);
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("]");
    return ret;
}

Status KVClient::Read(const std::vector<ReadParam> &readParams, std::vector<Optional<ReadOnlyBuffer>> &readOnlyBuffers)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_GET_MUL_BUFFERS);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(readParams.size()), K_INVALID,
                                         FormatString("The objectKeys size exceed %d.", OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::vector<Optional<Buffer>> buffers;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);
    std::unordered_set<std::string> keys;
    RequestParam reqParam;
    reqParam.objectKey = ReadParamToString(readParams);
    reqParam.timeout = std::to_string(0);
    int64_t dataSize = 0;
    for (const auto &param : readParams) {
        if (keys.find(param.key) != keys.end()) {
            auto status = Status(K_INVALID, FormatString("The input parameter contains duplicate key %s. Keys: %s",
                                                         param.key, VectorToString(keys)));
            accessPoint.Record(K_INVALID, std::to_string(dataSize), reqParam, status.GetMsg());
            return status;
        }
        if (UINT64_MAX - param.size < param.offset) {
            auto status =
                Status(K_INVALID, FormatString("The %s's offset: %llu + size: %llu overflow",
                                               param.key, param.offset, param.size));
            accessPoint.Record(K_INVALID, std::to_string(dataSize), reqParam, status.GetMsg());
            return status;
        }
        keys.insert(param.key);
    }
    Status rc = impl_->Read(readParams, buffers);
    if (rc.IsOk()) {
        readOnlyBuffers.clear();
        for (auto &buffer : buffers) {
            if (buffer) {
                dataSize += buffer->GetSize();
                auto bufferSharedPtr = std::make_shared<Buffer>(std::move(buffer.value()));
                readOnlyBuffers.emplace_back(ReadOnlyBuffer(bufferSharedPtr));
            } else {
                readOnlyBuffers.emplace_back();
            }
        }
    }
    StatusCode code = rc.GetCode() == K_NOT_FOUND ? K_OK : rc.GetCode();
    accessPoint.Record(code, std::to_string(dataSize), reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Del(const std::string &key)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_DEL_OBJECT);
    std::vector<std::string> failedKeys;
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_DELETE);
    Status rc = impl_->Delete({ key }, failedKeys);
    RequestParam reqParam;
    reqParam.objectKey = key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    accessPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Del(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_DEL_MUL_OBJECTS);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_DELETE);
    Status rc = impl_->Delete(keys, failedKeys);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    accessPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    return rc;
}

std::string KVClient::GenerateKey(const std::string &prefixKey)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::string key;
    (void)impl_->GenerateKey(key, prefixKey);
    return key;
}

Status KVClient::GenerateKey(const std::string &prefixKey, std::string &key)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->GenerateKey(key, prefixKey);
}

Status KVClient::QuerySize(const std::vector<std::string> &objectKeys, std::vector<uint64_t> &outSizes)
{
    return impl_->QuerySize(objectKeys, outSizes);
}

Status KVClient::HealthCheck()
{
    ServerState state;
    return impl_->HealthCheck(state);
}

Status KVClient::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_EXIST);
    Status rc = impl_->Exist(keys, exists, true, false);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    accessPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    return rc;
}

Status KVClient::Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::KV_CLIENT_EXPIRE_OBJECT);
    AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_EXPIRE);
    auto rc = impl_->Expire(keys, ttlSeconds, failedKeys);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(keys);
    accessPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    return rc;
}
}  // namespace datasystem
