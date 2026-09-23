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
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "datasystem/client/mmap_manager/host_memory_pin_manager.h"
#include "datasystem/client/object_cache/routing/ub_health_filter.h"
#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/client/object_cache/transport/object_read/object_read_flow.h"
#include "datasystem/client/object_cache/transport/object_read/object_read_types.h"
#include "datasystem/client/object_cache/transport/rpc/mset_request_builder.h"
#include "datasystem/client/object_cache/transport/rpc/set_request_builder.h"
#include "datasystem/client/object_cache/transport/transport_advisor.h"
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
    std::shared_ptr<HostMemoryPinManager> hostMemoryPinManager;
    bool enableClientDirectPipelineH2D = false;
    int32_t pipelineThreadNum = 64;
    // Keep eager UB setup by default; non-pipeline callers that have not negotiated UB may opt out explicitly.
    bool initializeUbRuntime = true;
    // A same-host endpoint remains usable through SHM when optional UB prewarming fails.
    bool allowUbRuntimeFailure = false;
    std::shared_ptr<UbHealthFilter> readSourceFilter;
    UbHealthSummaryObserveHook ubHealthSummaryHook;
    UbHealthSummaryApplyHook verifiedUbHealthSummaryHook;
    // Synchronous client-lifecycle admission checked around transport retry backoff.
    std::function<Status()> retryAdmissionCheck;
    std::function<void(const HostPort &, const Status &)> metadataFailureHandler;
    std::function<void(const HostPort &, const Status &)> drainingFallbackHandler;
    // Optional consumer of every client-local port-health change (for example logging or routing hand-off).
    std::weak_ptr<IUbPortHealthObserver> localPortHealthObserver;
};

class TransportLayer {
public:
    explicit TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                            uint64_t fastTransportMemSize, TransportLayerOptions options = {});
    ~TransportLayer();

    /** @brief Initialize transport runtime resources before data-plane connections are created. */
    Status Init();

    /**
     * @brief Get a weak reference to the data-plane manager owned by this layer.
     * Returned as a weak_ptr rather than a shared_ptr so the "callers only observe it weakly" contract is
     * expressed by the type: the only legitimate use is registering standby drain hooks, and a caller that
     * extended the manager's lifetime past a client shutdown would let the heartbeat thread reach a manager
     * that has already been shut down (ShutDown() resets transportLayer_ before draining those threads).
     * Expiry is the signal to fall back to the legacy drain behaviour.
     */
    std::weak_ptr<DataPlaneManager> GetDataPlaneManager() const
    {
        return manager_;
    }

    /** @brief Reject a new client-local UB write when every process-local UB port is confirmed BAD. */
    Status CheckLocalUbSenderAdmission() const;

    /** @brief Reject all Host object data APIs when every client-local UB port is confirmed BAD. */
    Status CheckLocalNodeAdmission() const;

    /** @brief E4/E9 local path evidence: requests a merged monitor refresh; does not isolate by itself. */
    void ReportLocalPortHealthTrigger();

    /** Observe complete Provider UB failure evidence from a Get response. */
    bool ReportProviderUbFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail);

    std::optional<UbPortHealthSummary> GetLocalPortHealthSummary() const;

    /**
     * @brief Run a client-local UB write under the shared sender admission and classify its raw failure evidence.
     * @param[in] workerAddr Worker endpoint used by the write.
     * @param[in,out] bufferInfo Buffer state populated with the raw provider/CQE failure detail.
     * @param[in] write Actual UB write operation.
     * @return The write result, or K_URMA_WORKER_UNAVAILABLE when every local UB port is confirmed BAD.
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
                  TransportCreateParam param, std::shared_ptr<ObjectBuffer> &buffer);

    /**
     * @brief Commit an ObjectBuffer through the selected transport.
     * @param[in] buffer ObjectBuffer created through Create.
     * @param[in] param Publish parameters.
     * @return K_OK on success; the error code otherwise.
     */
    Status Set(ObjectBuffer &buffer, const TransportSetParam &param);
    Status Set(ObjectBuffer &buffer, const TransportSetParam &param, TransportSetResult &result);

    /** @brief Create transport-native buffers for a same-worker MSet batch. */
    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                   const std::vector<uint64_t> &dataSizes, TransportCreateParam param,
                   std::vector<std::shared_ptr<ObjectBuffer>> &buffers);

    /** @brief Commit a same-worker MSet batch and return per-object failures. */
    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers, const TransportSetParam &param,
                TransportMSetResult &result);

    /** @brief Release an unfinished worker allocation after a local copy failure. */
    Status Release(ObjectBuffer &buffer, const TransportRequestContext &context);

    /** @brief Fetch a versioned hash-ring snapshot through the cached worker channel. */
    Status GetHashRing(const HostPort &workerAddr, uint64_t currentVersion, GetHashRingRspPb &response);

    /**
     * @brief Report whether the target worker is same-host (eligible for SHM fd-passing) per the
     * routing topology. Local-cache Create/Set(buffer) use this to route cross-host writes through
     * the transport layer (UB/TCP) instead of the bound-worker SHM path, matching Set(string).
     * @param[in] workerAddr Target worker address.
     * @return true when the advisor treats the worker as same-host (SHM_CANDIDATE).
     */
    bool IsSameHostWorker(const HostPort &workerAddr) const;

    /**
     * @brief Publish worker admission synchronously and schedule latest-wins connection cleanup asynchronously.
     * @param[in] snapshot Validated worker snapshot associated with the pending route update.
     * @return K_OK when admitted and queued; the error code otherwise.
     */
    Status ApplyWorkerSnapshot(WorkerSnapshot snapshot);

    /** @brief Schedule the existing Provider recovery probe after accepting a non-writable global summary. */
    bool ScheduleProviderRecoveryFromGlobalSummary(const HostPort &provider);
    std::function<void(const HostPort &)> MakeProviderRecoveryCallback() const;
    void RecordRoutingRefresh(uint64_t ringVersion);
    void ObserveUbHealthSummary(const UbHealthSummary &summary);
    UbHealthSummaryApplyHook GetUbHealthSummaryApplyHook() const;

    void Shutdown();

protected:
    void ConfigureUbHealthTriggers();

    /** @brief Construct the facade with injected collaborators for focused orchestration tests. */
    TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager, std::shared_ptr<TransportAdvisor> advisor);
    TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager, std::shared_ptr<TransportAdvisor> advisor,
                   std::shared_ptr<UbHealthFilter> readSourceFilter,
                   std::shared_ptr<ThreadPool> releasePool = nullptr);
    Status CheckUbReadSource(const HostPort &workerAddr, AccessTransportKind &deniedKind) const;

private:
    Status ConfigureLocalPortHealth();

    struct LocalUbSenderState;
    struct LocalUbSenderOperation {
        LocalUbSenderOperation() = default;
        ~LocalUbSenderOperation();
        LocalUbSenderOperation(const LocalUbSenderOperation &) = delete;
        LocalUbSenderOperation &operator=(const LocalUbSenderOperation &) = delete;

        LocalUbSenderState *state{ nullptr };
    };

    struct LocalUbSenderFailureView {
        const HostPort &workerAddr;
        AccessTransportKind kind;
        const Status &status;
        std::optional<int> providerStatus;
        std::optional<int> cqeStatus;
    };

    Status CheckLocalUbSenderAdmission(TransportHint hint) const;
    Status AcquireLocalUbSenderAdmission(TransportHint hint, LocalUbSenderOperation &operation) const;
    bool ReportWriteTargetUbFailure(const LocalUbSenderFailureView &failure);
    bool ReportLocalUbSenderFailure(const LocalUbSenderFailureView &failure);
    void PrepareLocalUbLateCompletion(ObjectBufferInfo &bufferInfo, AccessTransportKind kind,
                                      const HostPort *explicitWorker = nullptr) const;
    std::optional<std::chrono::steady_clock::time_point> GetProviderUbProbeDeadline() const;
    void TryRecoverProviderUbSource();
    std::optional<std::chrono::steady_clock::time_point> GetWriteTargetUbProbeDeadline() const;
    void TryRecoverWriteTargetUbSource();
    void NotifyReconcile();
    void ReconcileLoop();
    // Waits (under reconcileMutex_) for a snapshot, stop signal, or either UB recovery deadline.
    // Returns true if the caller should process/apply a snapshot, false if it should stop or re-loop.
    // Extracted from ReconcileLoop to keep that function within the codecheck nesting-depth limit.
    bool WaitForSnapshotOrStop(std::unique_lock<bthread::Mutex> &lock);
    // Post-publish Set processing: UB failure reporting, routed SHM owner-managed
    // release decision, rebuild/retry, and async reference release. Extracted from Set to keep Set within
    // the codecheck function-size limit.
    Status FinalizeSetPublish(const HostPort &workerAddr, ObjectBuffer &buffer, const TransportSetParam &param,
                              TransportHint hint, std::shared_ptr<IDataTransporter> &transporter,
                              const Status &publishRc, TransportSetResult &result,
                              std::chrono::steady_clock::time_point setStart);
    Status RetrySet(const HostPort &workerAddr, ObjectBuffer &buffer, const TransportSetParam &param,
                    TransportHint hint, TransportSetResult &result);
    // Rebuilds the data plane after a Set failure (K_URMA_NEED_CONNECT -> ResetDataPlane;
    // K_RPC_UNAVAILABLE -> Teardown). A non-retryable RPC error (dead peer) tears down the stale
    // connection but returns false so the caller does not retry. Returns true if rebuilt (caller
    // retries), false otherwise.
    bool RebuildPlaneOnSetFailure(const Status &rc, const HostPort &workerAddr,
                                  const std::shared_ptr<IDataTransporter> &stale);
    // Sampled triage log for the routed Set hot path: transport kind (SHM/UB/TCP) + result + latency,
    // so operators can localize which transport a write used and how long it took. Sampled (every N) to
    // avoid flooding; aggregate kind/byte counters are in the metrics.
    void LogSetResult(const HostPort &workerAddr, TransportHint hint, const Status &rc,
                      std::chrono::steady_clock::time_point start);
    Status RetryMSet(const HostPort &workerAddr, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                     const TransportSetParam &param, TransportHint hint, TransportMSetResult &result);
    // Rebuild/retry decision after an MSet failure that did not isolate the write target. Extracted
    // from MSet to keep MSet within the codecheck function-size limit.
    Status RetryOrReplayMSet(const HostPort &workerAddr, const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                             const TransportSetParam &param, TransportHint hint, TransportMSetResult &result,
                             const Status &rc, const std::shared_ptr<IDataTransporter> &stale);

    void ScheduleRelease(const HostPort &workerAddr, const ShmKey &shmId, const TransportRequestContext &context,
                         std::optional<TransportHint> transportHint = std::nullopt);
    void ScheduleAmbiguousCreateCleanup(const HostPort &workerAddr, const std::unordered_set<ShmKey> &shmIds,
                                        const TransportRequestContext &context);
    Status TryCreate(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                     const TransportCreateParam &param, TransportHint hint,
                     std::shared_ptr<ObjectBuffer> &buffer, std::unordered_set<ShmKey> &ambiguousShmIds);
    Status TryMCreate(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                      const std::vector<uint64_t> &dataSizes, const TransportCreateParam &param,
                      TransportHint hint, std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                      std::unordered_set<ShmKey> &ambiguousShmIds);
    Status TryMCreateFallbacks(const HostPort &workerAddr, const std::vector<std::string> &objectKeys,
                               const std::vector<uint64_t> &dataSizes, const TransportCreateParam &param,
                               TransportHint hint, std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                               std::unordered_set<ShmKey> &ambiguousShmIds);
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
    std::shared_ptr<ThreadPool> ambiguousCreateCleanupPool_;
    std::shared_ptr<UbHealthFilter> healthFilter_;
    std::unique_ptr<ObjectReadFlow> objectRead_;
    std::shared_ptr<UbPortHealthMonitor> localPortHealthMonitor_;
    std::weak_ptr<IUbPortHealthObserver> localPortHealthObserver_;
    bool allowUbRuntimeFailure_{ false };
    std::shared_ptr<LocalUbSenderState> localUbSenderState_;
    // ApplyWorkerSnapshot serializes admission publication with shutdown through reconcileMutex_.
    std::shared_ptr<bthread::Mutex> reconcileMutex_{ std::make_shared<bthread::Mutex>() };
    std::shared_ptr<bthread::ConditionVariable> reconcileCv_{ std::make_shared<bthread::ConditionVariable>() };
    std::optional<WorkerSnapshot> pendingSnapshot_;
    Thread reconcileThread_;
    bool reconcileStarted_{ false };
    bool reconcileStopping_{ false };
    // Serializes complete Shutdown calls while reconcileMutex_ remains available to the worker.
    bthread::Mutex shutdownMutex_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
