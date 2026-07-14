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

/** Description: Request builders for transport-layer Create and Set (Publish) RPCs. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SET_REQUEST_BUILDER_H
#define DATASYSTEM_CLIENT_TRANSPORT_SET_REQUEST_BUILDER_H

#include <cstdint>
#include <string>
#include <unordered_set>

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

struct TransportRequestContext {
    std::string clientId;
    std::string token;
    std::string tenantId;
};

struct TransportCreateParam {
    TransportRequestContext requestContext;
    CacheType cacheType = CacheType::MEMORY;
    ConsistencyType consistencyType = ConsistencyType::PRAM;
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;
    int64_t subTimeoutMs = 0;
};

struct TransportSetParam {
    TransportRequestContext requestContext;
    std::unordered_set<std::string> nestedKeys;
    uint32_t ttlSecond = 0;
    ExistenceOpt existence = ExistenceOpt::NONE;
    bool isSeal = false;
    bool keep = false;
    int64_t subTimeoutMs = 0;
    bool isRetry = false;
};

Status ValidateCreateRequest(const std::string &key, uint64_t size, const TransportCreateParam &param);
Status BuildCreateRequest(const std::string &key, uint64_t size, const TransportCreateParam &param,
                          CreateReqPb &request);

Status ValidateSetRequest(const ObjectBufferInfo &info, const TransportSetParam &param);
Status BuildSetRequest(const ObjectBufferInfo &info, const TransportSetParam &param, PublishReqPb &request);

/** Maps the Publish response payload to a transport-level status. */
Status SetTransportResponseStatus(const PublishRspPb &response, AccessTransportKind kind,
                                  bool isSeal, bool isRetry);

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_SET_REQUEST_BUILDER_H
