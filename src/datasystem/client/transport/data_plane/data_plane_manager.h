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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/client/transport/worker_snapshot.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

class DataPlaneManager {
public:
    explicit DataPlaneManager(std::shared_ptr<Signature> signature, uint64_t fastTransportMemSize,
                              BrpcChannelConfig channelConfig = {},
                              std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider = nullptr);
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

    /**
     * @brief Run an operation while the selected data plane cannot be torn down.
     * @param[in] workerAddr Target worker address.
     * @param[in] hint Transport suggestion from the advisor.
     * @param[in] operation Operation executed with the endpoint data-plane lease held.
     * @return K_OK when the operation succeeds; the connection or operation error otherwise.
     */
    Status WithDataPlaneLease(const HostPort &workerAddr, TransportHint hint,
                              const std::function<Status(const std::shared_ptr<IDataTransporter> &,
                                                         const std::shared_ptr<WorkerRpcClient> &)> &operation);

    /**
     * @brief Get or lazily create the shared RPC client for an endpoint without creating a data transporter.
     * @param[in] workerAddr Target endpoint address.
     * @param[out] out Cached or newly initialized RPC client.
     * @return K_OK when out is ready; the error code otherwise.
     */
    virtual Status GetOrCreateRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out);

    /** @brief Drop only the selected data-plane transporter while retaining the shared RPC connection. */
    void ResetDataPlane(const HostPort &workerAddr);

    /** @brief Drop the complete worker entry, including its shared RPC connection. */
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
    struct WorkerTransportEntry {
        bool HasAliveTransporter(AccessTransportKind expectedKind) const;
        void ResetDataPlaneLocked();
        void ResetDataPlane();

        std::shared_mutex mutex;
        std::shared_ptr<WorkerRpcClient> rpcClient;
        std::shared_ptr<IDataTransporter> transporter;
        AccessTransportKind kind = AccessTransportKind::TCP;
    };

    using EntryMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<WorkerTransportEntry>>;

    Status GetOrCreateEntry(const std::string &workerKey, std::shared_ptr<WorkerTransportEntry> &entry);

    Status GetOrBuildTransporter(const HostPort &workerAddr, TransportHint hint, AccessTransportKind expectedKind,
                                 const std::shared_ptr<WorkerTransportEntry> &entry,
                                 std::shared_ptr<IDataTransporter> &out);

    Status EnsureRpcClientLocked(const HostPort &workerAddr, const std::shared_ptr<WorkerTransportEntry> &entry);

    Status EnsureTransporterLocked(const HostPort &workerAddr, TransportHint hint, AccessTransportKind expectedKind,
                                   const std::shared_ptr<WorkerTransportEntry> &entry);

    Status BuildUbTransporter(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                              std::shared_ptr<IDataTransporter> &out);

    EntryMap entries_;
    std::shared_mutex mutex_;
    std::atomic<bool> shutdown_{ false };
    std::shared_ptr<Signature> signature_;
    BrpcChannelConfig channelConfig_;
    std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider_;
    uint64_t fastTransportMemSize_ = 0;
    std::atomic<bool> initialized_{ false };
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_MANAGER_H
