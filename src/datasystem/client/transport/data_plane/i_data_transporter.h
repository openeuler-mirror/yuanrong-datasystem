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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

/** @brief Owns a transport receive buffer referenced by zero-copy payload spans. */
class IReceiveBufferOwner {
public:
    virtual ~IReceiveBufferOwner() = default;
};

struct TransportGetRequest {
    TransportGetRequest(const std::vector<std::string> &keys, const std::vector<ReadParam> &params,
                        int64_t timeoutMs, bool queryL2)
        : objectKeys(keys), readParams(params), subTimeoutMs(timeoutMs), queryL2Cache(queryL2)
    {
    }

    const std::vector<std::string> &objectKeys;
    const std::vector<ReadParam> &readParams;
    int64_t subTimeoutMs;
    bool queryL2Cache;
};

/**
 * @brief Non-owning view of one zero-copy payload in a transport receive buffer.
 *
 * The data remains valid only while the externalOwner in the same TransportGetBatchResult is retained.
 */
struct ExternalPayloadSpan {
    size_t payloadInfoIndex = 0;
    const uint8_t *data = nullptr;
    uint64_t size = 0;
};

/** @brief One transport batch whose response and payload storage remain owned by this result. */
struct TransportGetBatchResult {
    std::vector<size_t> requestIndices;
    GetRspPb response;
    std::vector<RpcMessage> rpcPayloads;
    std::vector<ExternalPayloadSpan> externalPayloads;
    std::shared_ptr<IReceiveBufferOwner> externalOwner;
    uint32_t workerVersion = 0;
    AccessTransportKind kind = AccessTransportKind::TCP;
};

struct TransportGetResult {
    void Clear()
    {
        batches.clear();
        actualKind = AccessTransportKind::TCP;
    }

    std::vector<TransportGetBatchResult> batches;
    AccessTransportKind actualKind = AccessTransportKind::TCP;
};

class IDataTransporter {
public:
    virtual ~IDataTransporter() = default;

    /**
     * @brief Execute an object Get using this transport.
     * @param[in] input Logical Get request.
     * @param[out] output Transport batches with owned RPC payloads or external buffer references.
     * @return K_OK when transport completes; worker business status remains in each batch response. Returns a
     *         retry/rebuild error when the existing object-client Get flow requires one.
     */
    virtual Status Get(const TransportGetRequest &input, TransportGetResult &output) = 0;

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
