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
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/data_plane/shm_transporter.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/client/transport/transport_phase_latency_recorder.h"
#include "datasystem/client/transport/worker_snapshot.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"

#include <bthread/mutex.h>
#include <bthread/rwlock.h>

namespace datasystem {
namespace client {

class DataPlaneManager {
private:
    struct WorkerTransportEntry;

public:
    class DataPlaneLease {
    public:
        DataPlaneLease(const DataPlaneLease &) = delete;
        DataPlaneLease &operator=(const DataPlaneLease &) = delete;
        ~DataPlaneLease();

        const std::shared_ptr<IDataTransporter> &GetTransporter() const;
        const std::shared_ptr<WorkerRpcClient> &GetRpcClient() const;

    private:
        friend class DataPlaneManager;
        DataPlaneLease() = default;

        std::shared_ptr<WorkerTransportEntry> entry_;
        std::shared_ptr<IDataTransporter> transporter_;
        std::shared_ptr<WorkerRpcClient> rpcClient_;
        std::unique_ptr<bthread::RWLockRdGuard> entryLock_;
    };

    explicit DataPlaneManager(std::shared_ptr<Signature> signature, uint64_t fastTransportMemSize,
                              BrpcChannelConfig channelConfig = {},
                              std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider = nullptr,
                              bool enableClientDirectPipelineH2D = false, int32_t pipelineThreadNum = 64,
                              std::shared_ptr<ThreadPool> releasePool = nullptr, bool initializeUbRuntime = true,
                              bool allowUbRuntimeFailure = false);
    virtual ~DataPlaneManager();

    /** @brief Initialize manager lifecycle and, when requested, process-level UB resources. */
    Status Init();

    /**
     * @brief Get or lazily create a transporter for the worker.
     * @param[in] workerAddr Target worker address.
     * @param[in] hint Transport suggestion from the advisor.
     * @param[out] out The cached or newly built transporter.
     * @param[in] recorder Optional request-scoped phase recorder.
     * @return K_OK when out is ready, or the error code.
     */
    Status GetOrCreate(const HostPort &workerAddr, TransportHint hint, std::shared_ptr<IDataTransporter> &out,
                       TransportPhaseLatencyRecorder *recorder = nullptr);

    /**
     * @brief Acquire an endpoint lease that prevents its data plane from being torn down.
     * @param[in] workerAddr Target worker address.
     * @param[in] hint Transport suggestion from the advisor.
     * @param[out] lease Lease owning the selected transporter and RPC client.
     * @return K_OK when the endpoint is ready and leased; the error code otherwise.
     */
    Status AcquireDataPlaneLease(const HostPort &workerAddr, TransportHint hint,
                                 std::unique_ptr<DataPlaneLease> &lease);

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

    /** @brief Validate a UB connection and commit recovery while the worker snapshot remains admitted. */
    Status ProbeUbConnection(const HostPort &workerAddr, const std::function<void()> &commitRecovery = {});

    /** @brief Validate the Client-to-Worker UB path to one exact live worker endpoint. */
    virtual Status ProbeUbWriteTarget(const HostPort &workerAddr);

    /**
     * @brief Pull one Provider's UB health and verify its outbound Worker-to-Client UB path within a bounded timeout.
     */
    virtual Status ProbeProviderUbRecovery(const HostPort &workerAddr, const std::string &expectedIncarnation,
                                           int32_t timeoutMs, UbHealthSummary &summary);

    /** @brief Drop only the selected data-plane transporter while retaining the shared RPC connection. */
    void ResetDataPlane(const HostPort &workerAddr);

    /** @brief Drop the complete worker entry, including its shared RPC connection. */
    void Teardown(const HostPort &workerAddr);

    /**
     * @brief Atomically publish the latest worker admission set before route publication.
     * @param[in] snapshot Validated current worker snapshot.
     * @return K_OK on success; K_INVALID for a regressing version; K_SHUTTING_DOWN during shutdown.
     */
    Status UpdateWorkerSnapshot(const WorkerSnapshot &snapshot);

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
                                    TransportPhaseLatencyRecorder *recorder,
                                    std::shared_ptr<IDataTransporter> &out);

    virtual Status EstablishUbProbe(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient);

private:
    struct WorkerTransportEntry {
        bool HasAliveTransporter(AccessTransportKind expectedKind) const;
        void ResetDataPlaneLocked();
        void ResetDataPlane();

        bthread::RWLock mutex;
        std::shared_ptr<WorkerRpcClient> rpcClient;
        std::shared_ptr<IDataTransporter> transporter;
        AccessTransportKind kind = AccessTransportKind::TCP;
    };

    using EntryMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<WorkerTransportEntry>>;

    struct TransportBuildContext {
        const HostPort &workerAddr;
        TransportHint hint;
        AccessTransportKind expectedKind;
        TransportPhaseLatencyRecorder *recorder;
    };

    Status GetOrCreateEntry(const std::string &workerKey, std::shared_ptr<WorkerTransportEntry> &entry);

    Status GetOrBuildTransporter(const TransportBuildContext &context,
                                 const std::shared_ptr<WorkerTransportEntry> &entry,
                                 std::shared_ptr<IDataTransporter> &out);

    Status EnsureRpcClientLocked(const HostPort &workerAddr, const std::shared_ptr<WorkerTransportEntry> &entry,
                                 TransportPhaseLatencyRecorder *recorder);

    Status EnsureTransporterLocked(const TransportBuildContext &context,
                                   const std::shared_ptr<WorkerTransportEntry> &entry);

    Status BuildUbTransporter(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                              TransportPhaseLatencyRecorder *recorder, std::shared_ptr<IDataTransporter> &out);

    EntryMap entries_;
    std::shared_ptr<const std::unordered_set<std::string>> liveWorkers_;
    bthread::Mutex probeMutex_;
    std::vector<std::string> writeProbeWorkers_;
    std::unordered_map<std::string, size_t> writeProbeWorkerIndices_;
    std::string probePreferredWorker_;
    std::string lastProbeWorker_;
    std::atomic<uint64_t> workerSnapshotVersion_{ 0 };
    std::atomic<bool> hasWorkerSnapshot_{ false };
    std::atomic<bool> shutdown_{ false };
    std::shared_ptr<Signature> signature_;
    BrpcChannelConfig channelConfig_;
    std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider_;
    uint64_t fastTransportMemSize_ = 0;
    bool initializeUbRuntime_ = true;
    bool allowUbRuntimeFailure_ = false;
    bool enableClientDirectPipelineH2D_ = false;
    int32_t pipelineThreadNum_ = 64;
    bthread::Mutex lifecycleMutex_;
    std::atomic<bool> initialized_{ false };
    std::weak_ptr<ThreadPool> releasePool_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_MANAGER_H
