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

/** Description: Defines the client transport facade. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
#define DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "datasystem/client/routing/ub_health_filter.h"
#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/transport_advisor.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/object/object_buffer.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

namespace datasystem {

namespace client {

struct TransportLayerOptions {
    BrpcChannelConfig channelConfig;
    std::shared_ptr<ThreadPool> releasePool;
    bool enableClientDirectPipelineH2D = false;
    int32_t pipelineThreadNum = 64;
    std::shared_ptr<UbHealthFilter> readSourceFilter;
    // Synchronous client-lifecycle admission checked around transport retry backoff.
    std::function<Status()> retryAdmissionCheck;
};

class TransportLayer {
public:
    explicit TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                            uint64_t fastTransportMemSize, TransportLayerOptions options = {});
    ~TransportLayer();

    /** @brief Initialize transport runtime resources before data-plane connections are created. */
    Status Init();

    /** @brief Reject a new client-local UB write while the process-local sender is quarantined. */
    Status CheckLocalUbSenderAdmission() const;

    /**
     * @brief Run a client-local UB write under the shared sender admission and classify its raw failure evidence.
     * @param[in] workerAddr Worker endpoint used by the write.
     * @param[in,out] bufferInfo Buffer state populated with the raw provider/CQE failure detail.
     * @param[in] write Actual UB write operation.
     * @return The write result, or K_URMA_WORKER_UNAVAILABLE when the sender is quarantined.
     */
    Status RunClientLocalUbWrite(const HostPort &workerAddr, ObjectBufferInfo &bufferInfo,
                                 const std::function<Status()> &write);

    /**
     * @brief Execute an object read through metadata lookup and direct data-worker access.
     * @param[in] input Routed object read request.
     * @param[out] output Owned object read results.
     * @return K_OK on success; the error code otherwise.
     */
    Status Get(const ObjectReadRequest &input, ObjectReadResult &output);

    Status ResolveMetadata(const ObjectReadRequest &input, std::vector<ObjectMetadataItem> &metadata);

    Status AcquireDirectUbEndpointLease(const HostPort &workerAddr,
                                        std::unique_ptr<DataPlaneManager::DataPlaneLease> &lease);

    /**
     * @brief Execute Exist and rebuild the RPC connection once when the channel is unavailable.
     * @param[in] workerAddr Address returned by the routing layer.
     * @param[in] input Logical Exist request.
     * @param[out] output Exist results.
     * @return K_OK on success; the error code otherwise.
     */
    Status Exist(const HostPort &workerAddr, const TransportExistRequest &input, TransportExistResult &output);

    /**
     * @brief Create an ObjectBuffer with transport-native memory.
     * @param[in] workerAddr Address returned by the routing layer.
     * @param[in] objectKey Object key.
     * @param[in] dataSize Data capacity in bytes.
     * @param[in] param Create parameters.
     * @param[out] buffer Created ObjectBuffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status Create(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                  const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer);

    /**
     * @brief Commit an ObjectBuffer through the selected transport.
     * @param[in] buffer ObjectBuffer created through Create.
     * @param[in] param Publish parameters.
     * @return K_OK on success; the error code otherwise.
     */
    Status Set(ObjectBuffer &buffer, const TransportSetParam &param);

    /** @brief Create transport-native buffers for a same-worker MSet batch. */
    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                   const std::vector<uint64_t> &dataSizes, const TransportCreateParam &param,
                   std::vector<std::shared_ptr<ObjectBuffer>> &buffers);

    /** @brief Commit a same-worker MSet batch and return per-object failures. */
    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers, const TransportSetParam &param,
                TransportMSetResult &result);

    /** @brief Release an unfinished worker allocation after a local copy failure. */
    Status Release(ObjectBuffer &buffer, const TransportRequestContext &context);

    /** @brief Fetch a versioned hash-ring snapshot through the cached worker channel. */
    Status GetHashRing(const HostPort &workerAddr, uint64_t currentVersion, GetHashRingRspPb &response);

    /**
     * @brief Publish worker admission synchronously and schedule latest-wins connection cleanup asynchronously.
     * @param[in] snapshot Validated worker snapshot associated with the pending route update.
     * @return K_OK when admitted and queued; the error code otherwise.
     */
    Status ApplyWorkerSnapshot(WorkerSnapshot snapshot);

    void Shutdown();

protected:
    /** @brief Construct the facade with injected collaborators for focused orchestration tests. */
    TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager, std::shared_ptr<TransportAdvisor> advisor);
    TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager, std::shared_ptr<TransportAdvisor> advisor,
                   std::chrono::milliseconds localUbProbeBaseDelay,
                   std::shared_ptr<UbHealthFilter> readSourceFilter = nullptr);
    bool ReportProviderUbFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail);

private:
    struct LocalUbSenderFailureView {
        const HostPort &workerAddr;
        AccessTransportKind kind;
        const Status &status;
        std::optional<int> providerStatus;
        std::optional<int> cqeStatus;
    };

    Status CheckLocalUbSenderAdmission(TransportHint hint) const;
    Status AcquireLocalUbSenderAdmission(TransportHint hint, std::shared_lock<std::shared_mutex> &admission) const;
    bool ReportLocalUbSenderFailure(const LocalUbSenderFailureView &failure,
                                    std::shared_lock<std::shared_mutex> &admission);
    std::optional<std::chrono::steady_clock::time_point> GetLocalUbProbeDeadline() const;
    void TryRecoverLocalUbSender();
    std::optional<std::chrono::steady_clock::time_point> GetProviderUbProbeDeadline() const;
    void TryRecoverProviderUbSource();
    void NotifyReconcile();
    void ReconcileLoop();
    // Waits (under reconcileMutex_) for a snapshot, stop signal, or either UB recovery deadline.
    // Returns true if the caller should process/apply a snapshot, false if it should stop or re-loop.
    // Extracted from ReconcileLoop to keep that function within the codecheck nesting-depth limit.
    bool WaitForSnapshotOrStop(std::unique_lock<bthread::Mutex> &lock);
    // Post-publish Set processing: UB failure reporting + sender quarantine, routed SHM owner-managed
    // release decision, rebuild/retry, and async reference release. Extracted from Set to keep Set within
    // the codecheck function-size limit.
    Status FinalizeSetPublish(const HostPort &workerAddr, ObjectBuffer &buffer, const TransportSetParam &param,
                              TransportHint hint, std::shared_ptr<IDataTransporter> &transporter,
                              const Status &publishRc, std::shared_lock<std::shared_mutex> &admission,
                              std::chrono::steady_clock::time_point setStart);
    Status RetrySet(const HostPort &workerAddr, ObjectBuffer &buffer, const TransportSetParam &param,
                    TransportHint hint);
    // Rebuilds the data plane after a Set failure (K_URMA_NEED_CONNECT -> ResetDataPlane;
    // K_RPC_UNAVAILABLE -> Teardown). A non-retryable RPC error (dead peer) tears down the stale
    // connection but returns false so the caller does not retry. Returns true if rebuilt (caller
    // retries), false otherwise.
    bool RebuildPlaneOnSetFailure(const Status &rc, const HostPort &workerAddr);
    // Sampled triage log for the routed Set hot path: transport kind (SHM/UB/TCP) + result + latency,
    // so operators can localize which transport a write used and how long it took. Sampled (every N) to
    // avoid flooding; aggregate kind/byte counters are in the metrics.
    void LogSetResult(const HostPort &workerAddr, TransportHint hint, const Status &rc,
                      std::chrono::steady_clock::time_point start);
    Status RetryMSet(const HostPort &workerAddr, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                     const TransportSetParam &param, TransportHint hint, TransportMSetResult &result);

    void ScheduleRelease(const HostPort &workerAddr, const ShmKey &shmId, const TransportRequestContext &context,
                         std::optional<TransportHint> transportHint = std::nullopt);
    void ScheduleMSetReleases(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                              const TransportRequestContext &context, const TransportMSetResult &result,
                              std::optional<TransportHint> transportHint = std::nullopt);

    // Retry InvokeDecreaseReference up to 3 times with exponential backoff; rebuilds the transporter
    // if it dies mid-retry. Used by Release and ScheduleRelease to avoid permanent shm-ref leaks.
    Status InvokeReleaseWithRetry(const HostPort &workerAddr, const ShmKey &shmId,
                                  const TransportRequestContext &context,
                                  std::shared_ptr<IDataTransporter> &transporter);
    static Status InvokeReleaseWithRetryOnAliveTransporter(
        const HostPort &workerAddr, const ShmKey &shmId, const TransportRequestContext &context,
        std::shared_ptr<IDataTransporter> &transporter, const std::shared_ptr<DataPlaneManager> &manager,
        const std::shared_ptr<TransportAdvisor> &advisor);

    std::shared_ptr<DataPlaneManager> manager_;
    std::shared_ptr<TransportAdvisor> advisor_;
    std::shared_ptr<ThreadPool> releasePool_;
    std::shared_ptr<UbHealthFilter> healthFilter_;
    std::unique_ptr<ObjectReadFlow> objectRead_;
    mutable std::shared_mutex localUbSenderMutex_;
    std::atomic<bool> localUbSenderUnavailable_{ false };
    Status localUbSenderFailure_ = Status::OK();
    std::optional<HostPort> localUbProbeWorker_;
    uint64_t localUbSenderGeneration_{ 0 };
    uint32_t localUbProbeBackoffLevel_{ 0 };
    std::chrono::steady_clock::time_point localUbProbeDeadline_;
    std::chrono::milliseconds localUbProbeBaseDelay_{ std::chrono::seconds(1) };
    // ApplyWorkerSnapshot serializes admission publication with shutdown through reconcileMutex_.
    bthread::Mutex reconcileMutex_;
    bthread::ConditionVariable reconcileCv_;
    std::optional<WorkerSnapshot> pendingSnapshot_;
    Thread reconcileThread_;
    bool reconcileStarted_{ false };
    bool reconcileStopping_{ false };
    std::atomic<bool> shutdownRequested_{ false };
    // Serializes complete Shutdown calls while reconcileMutex_ remains available to the worker.
    bthread::Mutex shutdownMutex_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
