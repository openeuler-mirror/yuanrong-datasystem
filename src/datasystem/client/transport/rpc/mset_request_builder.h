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

/** Description: Request builders and result mapping for transport-layer MSet. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_MSET_REQUEST_BUILDER_H
#define DATASYSTEM_CLIENT_TRANSPORT_MSET_REQUEST_BUILDER_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/object/object_buffer.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

struct TransportMSetResult {
    std::vector<std::string> failedKeys;
    Status lastRc = Status::OK();
    AccessTransportKind actualKind = AccessTransportKind::UNKNOWN;
    // True once InvokeMultiSet starts; connection errors after this point are ambiguous and must not be replayed.
    bool publishAttempted = false;

    void Clear();
};

Status ValidateMultiCreateRequest(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                                  const TransportCreateParam &param);

Status ValidateMSetRequest(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                           const TransportSetParam &param);

Status BuildMultiCreateRequest(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                               const TransportCreateParam &param, MultiCreateReqPb &request);

/**
 * Build one MultiPublish request and its positional payload vector after validation by TransportLayer.
 * tcpPayload[i] marks an object whose bytes must be carried by RPC rather than worker SHM/UB memory.
 */
Status BuildMultiPublishRequest(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                                const std::vector<bool> &tcpPayload, const TransportSetParam &param,
                                MultiPublishReqPb &request, std::vector<MemView> &payloads);

Status SetMSetResponseResult(const MultiPublishRspPb &response, size_t objectCount,
                             AccessTransportKind kind, TransportMSetResult &result);

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_MSET_REQUEST_BUILDER_H
