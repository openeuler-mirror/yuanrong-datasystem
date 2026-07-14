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

/** Description: Implements request builders for transport-layer Create and Set (Publish) RPCs. */

#include "datasystem/client/transport/rpc/set_request_builder.h"

#include <climits>

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
namespace {

static constexpr uint64_t MAX_PUB_SIZE = 256 * 1024 * 1024 * 1024uL;

}  // namespace

Status BuildCreateRequest(const std::string &key, uint64_t size, const TransportCreateParam &param,
                          CreateReqPb &request)
{
    RETURN_IF_NOT_OK(ValidateCreateRequest(key, size, param));
    request.Clear();
    request.set_client_id(param.requestContext.clientId);
    request.set_object_key(key);
    request.set_data_size(size);
    request.set_token(param.requestContext.token);
    request.set_tenant_id(param.requestContext.tenantId);
    request.set_cache_type(static_cast<uint32_t>(param.cacheType));
    request.set_request_timeout(TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs()));
    request.set_is_routed(true);
    return Status::OK();
}

Status ValidateSetRequest(const ObjectBufferInfo &info, const TransportSetParam &param)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsInNonNegativeInt32(param.subTimeoutMs), K_INVALID,
                             "Set sub-timeout must fit a non-negative int32");
    CHECK_FAIL_RETURN_STATUS(!info.objectKey.empty(), K_INVALID, "Set object key must not be empty");
    CHECK_FAIL_RETURN_STATUS(!param.requestContext.clientId.empty(), K_INVALID, "Set client ID must not be empty");
    return Status::OK();
}

Status BuildSetRequest(const ObjectBufferInfo &info, const TransportSetParam &param, PublishReqPb &request)
{
    request.Clear();
    *request.mutable_nested_keys() = { param.nestedKeys.begin(), param.nestedKeys.end() };
    request.set_client_id(param.requestContext.clientId);
    request.set_object_key(info.objectKey);
    request.set_token(param.requestContext.token);
    request.set_tenant_id(param.requestContext.tenantId);
    // WorkerRpcClient adds the AK/SK signature after all request fields are finalized.
    request.set_ttl_second(param.ttlSecond);
    request.set_existence(static_cast<::datasystem::ExistenceOptPb>(param.existence));

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(UINT64_MAX - info.dataSize >= info.metadataSize,
                                         StatusCode::K_RUNTIME_ERROR,
                                         FormatString("Data size[%llu] + meta size[%llu] > UINT64_MAX",
                                                      info.dataSize, info.metadataSize));
    auto bufferSize = info.dataSize + info.metadataSize;
    CHECK_FAIL_RETURN_STATUS(
        bufferSize < MAX_PUB_SIZE, K_INVALID,
        FormatString("Buffer size should not be too large, curr: %llu, max: %llu", bufferSize, MAX_PUB_SIZE));

    request.set_data_size(info.dataSize);
    request.set_write_mode(static_cast<uint32_t>(info.objectMode.GetWriteMode()));
    request.set_consistency_type(static_cast<uint32_t>(info.objectMode.GetConsistencyType()));
    request.set_cache_type(static_cast<uint32_t>(info.objectMode.GetCacheType()));
    request.set_is_seal(param.isSeal);
    request.set_shm_id(info.shmId);
    request.set_keep(param.keep);
    request.set_is_retry(param.isRetry);
    request.set_is_routed(true);
    return Status::OK();
}

Status SetTransportResponseStatus(const PublishRspPb &response, AccessTransportKind kind,
                                  bool isSeal, bool isRetry)
{
    // PublishRspPb carries only latency trace fields (latency_phase_us, latency_tick_dropped_count).
    // Set errors are in the RPC return Status, handled by TransportLayer::Set's rebuild ladder.
    // Seal-retry K_OC_ALREADY_SEALED is normalized by WorkerRpcClient::InvokeSet, where the RPC Status is available.
    (void)response;
    AccessTransportTracker::Record(kind);
    (void)isSeal;
    (void)isRetry;
    return Status::OK();
}

Status ValidateCreateRequest(const std::string &key, uint64_t size, const TransportCreateParam &param)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsInNonNegativeInt32(param.subTimeoutMs), K_INVALID,
                             "Create sub-timeout must fit a non-negative int32");
    CHECK_FAIL_RETURN_STATUS(!key.empty(), K_INVALID, "Create object key must not be empty");
    CHECK_FAIL_RETURN_STATUS(!param.requestContext.clientId.empty(), K_INVALID,
                             "Create client ID must not be empty");
    CHECK_FAIL_RETURN_STATUS(size > 0, K_INVALID, "Create data size must be positive");
    CHECK_FAIL_RETURN_STATUS(
        size < MAX_PUB_SIZE, K_INVALID,
        FormatString("Create data size should not be too large, curr: %llu, max: %llu", size, MAX_PUB_SIZE));
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
