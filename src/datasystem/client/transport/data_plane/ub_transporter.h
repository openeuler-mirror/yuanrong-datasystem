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

/** Description: Defines the UB object data transporter. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_UB_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_UB_TRANSPORTER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/data_plane/ub_connection.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/object_cache/object_base.h"

namespace datasystem {
namespace client {

/** @brief Owner-backed UB receive buffer and its transport identity. */
struct UbReceiveBuffer {
    uint8_t *data = nullptr;
    uint64_t size = 0;
    UrmaRemoteAddrPb remoteAddr;
    std::shared_ptr<IReceiveBufferOwner> owner;
    std::string transportInstanceId;
};

class IUbReceiveBufferProvider {
public:
    virtual ~IUbReceiveBufferProvider() = default;

    /** @return Maximum size supported by one UB Get receive buffer. */
    virtual uint64_t MaxGetSize() const = 0;

    /**
     * @brief Allocate and describe a UB receive buffer.
     * @param[in] requiredSize Required payload capacity.
     * @param[out] buffer Prepared receive buffer and transport identity.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Allocate(uint64_t requiredSize, UbReceiveBuffer &buffer) = 0;
};

/**
 * @brief Create the default process-wide UB receive-buffer provider.
 * @return Default UB receive-buffer provider implementation.
 */
std::shared_ptr<IUbReceiveBufferProvider> CreateDefaultUbReceiveBufferProvider();

class UbTransporter : public IDataTransporter {
public:
    explicit UbTransporter(std::shared_ptr<WorkerRpcClient> rpcClient, std::shared_ptr<UbConnection> conn,
                           std::shared_ptr<IUbReceiveBufferProvider> bufferProvider = nullptr);
    ~UbTransporter() override = default;

    AccessTransportKind Kind() const override
    {
        return AccessTransportKind::UB;
    }

    bool IsAlive() const override;

    Status Get(const DataGetRequest &input, DataGetResult &output) override;

    Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                  const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer) override;
    Status Set(ObjectBuffer &buffer, const TransportSetParam &param) override;
    Status Release(const ShmKey &shmId, const TransportRequestContext &context) override;

    void CloseDataPlane() override;

protected:
    /** @brief Write one ObjectBuffer payload through UB. Overridable for deterministic transport tests. */
    virtual Status WritePayload(ObjectBufferInfo &info);

private:
    Status GetOnce(const DataGetRequest &input, uint64_t expectedSize, DataGetResult &output, uint64_t &actualSize);

    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::shared_ptr<UbConnection> conn_;
    std::shared_ptr<IUbReceiveBufferProvider> bufferProvider_;
    mutable std::shared_mutex lifecycleMutex_;
    std::atomic<uint64_t> urmaFallbackTcpPendingBytes_{0};
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_UB_TRANSPORTER_H
