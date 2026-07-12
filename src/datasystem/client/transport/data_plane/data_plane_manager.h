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

/** Description: Defines endpoint-scoped data-plane transporter management. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_MANAGER_H
#define DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_MANAGER_H

#include <cstdint>
#include <memory>
#include <string>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/rpc/client_request_auth.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/client/transport/worker_snapshot.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

class DataPlaneManager {
public:
    static constexpr uint64_t DEFAULT_FAST_TRANSPORT_MEM_SIZE = 256UL * 1024UL * 1024UL;

    explicit DataPlaneManager(std::shared_ptr<ClientRequestAuth> auth,
                              uint64_t fastTransportMemSize = DEFAULT_FAST_TRANSPORT_MEM_SIZE,
                              BrpcChannelConfig channelConfig = {});
    virtual ~DataPlaneManager();

    /** @brief Initialize process-level resources required by UB data-plane transport. */
    Status Init();

    /**
     * @brief Get or lazily create a transporter for the worker.
     * @param[in] workerAddr Target worker address.
     * @param[in] hint Transport suggestion from the advisor.
     * @param[out] out The cached or newly built transporter.
     * @return K_OK when out is ready, or the error code.
     */
    Status GetOrCreate(const HostPort &workerAddr, TransportHint hint, std::shared_ptr<IDataTransporter> &out);

    /** @brief Drop only the selected data-plane transporter while retaining the brpc control connection. */
    void ResetDataPlane(const HostPort &workerAddr);

    /** @brief Drop the complete worker entry, including its brpc control connection. */
    void Teardown(const HostPort &workerAddr);

    /**
     * @brief Remove cached worker entries that are absent from the current snapshot.
     * @param[in] snapshot Current reachable-worker snapshot.
     */
    void ReconcileWithSnapshot(const WorkerSnapshot &snapshot);

    void Shutdown();

protected:
    virtual Status CreateWorkerRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out);

    virtual Status BuildTransporter(const HostPort &workerAddr, TransportHint hint,
                                    const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                    std::shared_ptr<IDataTransporter> &out);

private:
    struct WorkerTransportEntry;

    Status GetOrCreateEntry(const std::string &workerKey, std::shared_ptr<WorkerTransportEntry> &entry);

    Status GetOrBuildTransporter(const HostPort &workerAddr, TransportHint hint, AccessTransportKind expectedKind,
                                 const std::shared_ptr<WorkerTransportEntry> &entry,
                                 std::shared_ptr<IDataTransporter> &out);

    Status EnsureRpcClientLocked(const HostPort &workerAddr, const std::shared_ptr<WorkerTransportEntry> &entry);

    Status EnsureTransporterLocked(const HostPort &workerAddr, TransportHint hint, AccessTransportKind expectedKind,
                                   const std::shared_ptr<WorkerTransportEntry> &entry);

    Status BuildUbTransporter(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                              const std::string &errorPrefix, std::shared_ptr<IDataTransporter> &out);

    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_MANAGER_H
