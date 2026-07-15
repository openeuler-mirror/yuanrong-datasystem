/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Implements request builders and result mapping for transport-layer MSet. */

#include "datasystem/client/transport/rpc/mset_request_builder.h"

#include <unordered_set>

#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
namespace client {

Status ValidateMSetRequest(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                           const TransportSetParam &param)
{
    CHECK_FAIL_RETURN_STATUS(!buffers.empty(), K_INVALID, "MSet buffer list must not be empty");
    CHECK_FAIL_RETURN_STATUS(Validator::IsBatchSizeUnderLimit(buffers.size()), K_INVALID,
                             "MSet buffer count exceeds the batch limit");
    CHECK_FAIL_RETURN_STATUS(param.nestedKeys.empty() && !param.isSeal && !param.keep, K_NOT_SUPPORTED,
                             "MSet does not support nested keys, seal, or keep");

    const ObjectBufferInfo *first = nullptr;
    std::unordered_set<std::string> uniqueKeys;
    uniqueKeys.reserve(buffers.size());
    for (const auto &buffer : buffers) {
        CHECK_FAIL_RETURN_STATUS(buffer != nullptr && buffer->GetSize() > 0, K_INVALID,
                                 "MSet buffer must be initialized and non-empty");
        const auto &info = ObjectBufferInternal::GetInfo(*buffer);
        RETURN_IF_NOT_OK(ValidateSetRequest(info, param));
        CHECK_FAIL_RETURN_STATUS(info.pointer != nullptr, K_INVALID, "MSet buffer data pointer is null");
        CHECK_FAIL_RETURN_STATUS(uniqueKeys.emplace(info.objectKey).second, K_INVALID,
                                 "MSet object keys must be unique");
        if (first == nullptr) {
            first = &info;
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(info.workerAddr == first->workerAddr, K_INVALID,
                                 "MSet buffers must target the same worker");
        CHECK_FAIL_RETURN_STATUS(info.objectMode.GetWriteMode() == first->objectMode.GetWriteMode()
                                     && info.objectMode.GetConsistencyType() == first->objectMode.GetConsistencyType()
                                     && info.objectMode.GetCacheType() == first->objectMode.GetCacheType(),
                                 K_INVALID, "MSet buffers must use the same object mode");
    }
    return Status::OK();
}

namespace {
void AppendObjectInfo(const ObjectBufferInfo &info, bool useTcpPayload, MultiPublishReqPb &request)
{
    auto *objectInfo = request.add_object_info();
    objectInfo->set_object_key(info.objectKey);
    objectInfo->set_data_size(info.dataSize);
    if (!useTcpPayload) {
        objectInfo->set_shm_id(info.shmId);
    }
}

}  // namespace

void TransportMSetResult::Clear()
{
    failedKeys.clear();
    lastRc = Status::OK();
    actualKind = AccessTransportKind::UNKNOWN;
    publishAttempted = false;
}

Status ValidateMultiCreateRequest(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                                  const TransportCreateParam &param)
{
    CHECK_FAIL_RETURN_STATUS(!keys.empty(), K_INVALID, "MCreate key list must not be empty");
    CHECK_FAIL_RETURN_STATUS(keys.size() == sizes.size(), K_INVALID,
                             "MCreate key and size counts must match");
    CHECK_FAIL_RETURN_STATUS(Validator::IsBatchSizeUnderLimit(keys.size()), K_INVALID,
                             "MCreate key count exceeds the batch limit");
    std::unordered_set<std::string> uniqueKeys;
    uniqueKeys.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        RETURN_IF_NOT_OK(ValidateCreateRequest(keys[i], sizes[i], param));
        CHECK_FAIL_RETURN_STATUS(uniqueKeys.emplace(keys[i]).second, K_INVALID,
                                 "MCreate object keys must be unique");
    }
    return Status::OK();
}

Status BuildMultiCreateRequest(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                               const TransportCreateParam &param, MultiCreateReqPb &request)
{
    RETURN_IF_NOT_OK(ValidateMultiCreateRequest(keys, sizes, param));

    request.Clear();
    request.set_client_id(param.requestContext.clientId);
    request.set_token(param.requestContext.token);
    request.set_tenant_id(param.requestContext.tenantId);
    request.set_skip_check_existence(true);
    request.set_request_timeout(TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs()));
    request.set_is_routed(true);
    request.mutable_object_key()->Add(keys.begin(), keys.end());
    request.mutable_data_size()->Add(sizes.begin(), sizes.end());
    return Status::OK();
}

Status BuildMultiPublishRequest(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                const std::vector<bool> &tcpPayload, const TransportSetParam &param,
                                MultiPublishReqPb &request, std::vector<MemView> &payloads)
{
    CHECK_FAIL_RETURN_STATUS(!buffers.empty(), K_INVALID, "MSet buffer list must not be empty");
    CHECK_FAIL_RETURN_STATUS(buffers.size() == tcpPayload.size(), K_INVALID,
                             "MSet payload mode count does not match buffer count");
    request.Clear();
    payloads.clear();
    request.set_client_id(param.requestContext.clientId);
    request.set_token(param.requestContext.token);
    request.set_tenant_id(param.requestContext.tenantId);
    request.set_ttl_second(param.ttlSecond);
    request.set_existence(static_cast<::datasystem::ExistenceOptPb>(param.existence));
    request.set_is_replica(false);
    request.set_auto_release_memory_ref(false);
    request.set_is_routed(true);

    const auto &first = ObjectBufferInternal::GetInfo(*buffers.front());
    request.set_write_mode(static_cast<uint32_t>(first.objectMode.GetWriteMode()));
    request.set_consistency_type(static_cast<uint32_t>(first.objectMode.GetConsistencyType()));
    request.set_cache_type(static_cast<uint32_t>(first.objectMode.GetCacheType()));
    request.mutable_object_info()->Reserve(static_cast<int>(buffers.size()));
    payloads.reserve(buffers.size());

    // Worker indexes payloads by object_info index. Keep fallback objects first so compact payloads remain positional.
    for (size_t i = 0; i < buffers.size(); ++i) {
        if (!tcpPayload[i]) {
            continue;
        }
        const auto &info = ObjectBufferInternal::GetInfo(*buffers[i]);
        AppendObjectInfo(info, true, request);
        payloads.emplace_back(info.pointer + info.metadataSize, info.dataSize);
    }
    for (size_t i = 0; i < buffers.size(); ++i) {
        if (tcpPayload[i]) {
            continue;
        }
        AppendObjectInfo(ObjectBufferInternal::GetInfo(*buffers[i]), false, request);
    }
    return Status::OK();
}

Status SetMSetResponseResult(const MultiPublishRspPb &response, size_t objectCount,
                             AccessTransportKind kind, TransportMSetResult &result)
{
    result.Clear();
    result.failedKeys.assign(response.failed_object_keys().begin(), response.failed_object_keys().end());
    result.lastRc = Status(static_cast<StatusCode>(response.last_rc().error_code()), response.last_rc().error_msg());
    CHECK_FAIL_RETURN_STATUS(result.failedKeys.size() <= objectCount, K_RUNTIME_ERROR,
                             "MSet response contains more failed keys than requested objects");
    const bool responseHasSuccess = result.failedKeys.size() < objectCount
                                    && !(result.failedKeys.empty() && result.lastRc.IsError());
    if (responseHasSuccess) {
        result.actualKind = kind;
        AccessTransportTracker::Record(kind);
    }
    if (result.failedKeys.empty() && result.lastRc.IsError()) {
        return result.lastRc;
    }
    if (result.failedKeys.size() != objectCount) {
        return Status::OK();
    }
    return result.lastRc.IsError() ? result.lastRc
                                   : Status(K_RUNTIME_ERROR, "All MSet objects failed without an error status");
}

}  // namespace client
}  // namespace datasystem
