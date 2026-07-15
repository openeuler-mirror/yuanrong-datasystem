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

/** Description: Defines the object data transporter contract and result ownership. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_I_DATA_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_I_DATA_TRANSPORTER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_buffer.h"
#include "datasystem/protos/object_posix.pb.h"
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

    /**
     * @brief Allocate transport-native memory and create an ObjectBuffer.
     * @param[in] workerAddr Worker address from routing.
     * @param[in] key Object key.
     * @param[in] size Data capacity in bytes.
     * @param[in] param Create parameters (cache type, consistency, timeout).
     * @param[out] buffer Created ObjectBuffer with transport-specific memory.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                          const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer) = 0;

    /**
     * @brief Commit an ObjectBuffer via transport (URMA write + Publish RPC / TCP payload).
     * @param[in] buffer ObjectBuffer created by this transporter's Create.
     * @param[in] param Set parameters (nested keys, TTL, existence, seal, keep, timeout, isRetry).
     * @return K_OK on success; rebuild-trigger errors (K_URMA_NEED_CONNECT, K_RPC_UNAVAILABLE) on failure.
     */
    virtual Status Set(ObjectBuffer &buffer, const TransportSetParam &param) = 0;

    /**
     * @brief Create caller-owned transport-native buffers for one same-worker MSet batch.
     * @param[in] workerAddr Target worker to which every returned buffer is bound.
     * @param[in] keys Unique object keys in the batch.
     * @param[in] sizes Object sizes corresponding positionally to keys.
     * @param[in] param Create parameters and client identity.
     * @param[out] buffers Caller-owned buffers consumed by MSet; partial output is not returned on failure.
     * @return K_OK on success. Ambiguous K_RPC_UNAVAILABLE is not replayed because MultiCreate has no idempotency
     *         marker and the worker may already have allocated memory.
     */
    virtual Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                           const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                           std::vector<std::shared_ptr<ObjectBuffer>> &buffers) = 0;

    /**
     * @brief Commit one validated same-worker, same-mode MSet batch and report per-object failures.
     * @param[in] buffers Buffers returned by one same-worker MCreate operation. The caller retains ownership.
     * @param[in] param Publish parameters and client identity.
     * @param[out] result Failed keys, worker status, and actual transport when at least one object succeeds.
     * @return K_OK if at least one object succeeds; K_URMA_NEED_CONNECT requests one UB data-plane rebuild. A
     *  pre-Publish K_RPC_UNAVAILABLE may be replayed once; an attempted publish is ambiguous and is not replayed.
     *  TransportLayer schedules release of every worker allocation before returning.
     */
    virtual Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                        const TransportSetParam &param, TransportMSetResult &result) = 0;

    /**
     * @brief Release worker-side resources allocated for a transport buffer.
     * @param[in] shmId Worker allocation identifier.
     * @param[in] context Client identity and tenant context used by the release RPC.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Release(const ShmKey &shmId, const TransportRequestContext &context) = 0;

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
