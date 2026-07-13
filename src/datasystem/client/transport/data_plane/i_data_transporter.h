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

/** Description: Defines the object Get transporter contract and result ownership. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_I_DATA_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_I_DATA_TRANSPORTER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

/** @brief Owns a transport receive buffer referenced by a zero-copy result. */
class IReceiveBufferOwner {
public:
    virtual ~IReceiveBufferOwner() = default;
};

/** @brief One data read against a transporter already bound to a data-worker endpoint. */
struct DataGetRequest {
    std::string objectKey;
    uint64_t expectedSize = 0;
};

/** @brief Owned result returned by one data-worker transporter. */
struct DataGetResult {
    GetObjectRemoteRspPb response;
    std::vector<RpcMessage> rpcPayloads;
    const uint8_t *externalData = nullptr;
    uint64_t externalSize = 0;
    std::shared_ptr<IReceiveBufferOwner> externalOwner;
    AccessTransportKind kind = AccessTransportKind::TCP;
};

class IDataTransporter {
public:
    virtual ~IDataTransporter() = default;

    /**
     * @brief Read one object from this transporter's data-worker endpoint.
     * @param[in] input Object identity and expected size.
     * @param[out] output Owned RPC payloads or an owner-backed external buffer.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Get(const DataGetRequest &input, DataGetResult &output) = 0;

    virtual AccessTransportKind Kind() const = 0;

    virtual bool IsAlive() const = 0;

    /** @brief Close transport-specific data-plane resources without closing the shared RPC client. */
    virtual void CloseDataPlane()
    {
    }
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_I_DATA_TRANSPORTER_H
