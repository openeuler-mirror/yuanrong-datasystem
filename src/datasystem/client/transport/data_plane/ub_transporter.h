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

/** Description: Defines the UB object Get transporter. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_UB_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_UB_TRANSPORTER_H

#include <memory>
#include <shared_mutex>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/data_plane/ub_connection.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"

namespace datasystem {
namespace client {
class IUbReceiveBufferProvider {
public:
    virtual ~IUbReceiveBufferProvider() = default;

    /** @return Maximum size supported by one UB Get receive buffer. */
    virtual uint64_t MaxGetSize() const = 0;

    /**
     * @brief Allocate and describe a UB receive buffer.
     * @param[in] requiredSize Required payload capacity.
     * @param[out] data Receive-buffer address.
     * @param[out] size Actual receive-buffer size.
     * @param[out] remoteAddr Remote address sent to the worker.
     * @param[out] owner Owner that keeps data alive.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Allocate(uint64_t requiredSize, uint8_t *&data, uint64_t &size,
                            UrmaRemoteAddrPb &remoteAddr, std::shared_ptr<IReceiveBufferOwner> &owner) = 0;
};

/** @brief Indices and UB feasibility for one Get batch. */
struct GetBatchPlan {
    std::vector<size_t> requestIndices;
    uint64_t totalSize = 0;
    bool useUb = true;
};

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

    Status Get(const TransportGetRequest &input, TransportGetResult &output) override;

    void CloseDataPlane() override;

private:
    Status ExecutePlan(const TransportGetRequest &input, const GetBatchPlan &plan, TransportGetBatchResult &batch);

    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::shared_ptr<UbConnection> conn_;
    std::shared_ptr<IUbReceiveBufferProvider> bufferProvider_;
    mutable std::shared_mutex lifecycleMutex_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_UB_TRANSPORTER_H
