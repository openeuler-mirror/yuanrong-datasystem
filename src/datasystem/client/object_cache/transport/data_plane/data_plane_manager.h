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
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/mmap_manager/host_memory_pin_manager.h"
#include "datasystem/client/object_cache/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/object_cache/transport/data_plane/shm_transporter.h"
#include "datasystem/client/object_cache/transport/data_plane/ub_transporter.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/client/object_cache/transport/transport_kind.h"
#include "datasystem/client/object_cache/transport/transport_phase_latency_recorder.h"
#include "datasystem/client/object_cache/transport/worker_snapshot.h"
#include "datasystem/cluster/ub_health/remote_ub_port_health_verifier.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>

namespace datasystem {
namespace client {

class ObjectMetadataClient;

class DataPlaneManager {
private:
    struct WorkerTransportEntry;
    class UbHealthCallbackState {
    public:
        explicit UbHealthCallbackState(DataPlaneManager *manager);
        ~UbHealthCallbackState() = default;

        void Detach();
        void ObserveSummary(const UbHealthSummary &summary);

      private:
        class Lease {
        public:
            Lease(UbHealthCallbackState *owner, DataPlaneManager *manager) : owner_(owner), manager_(manager) {}
            ~Lease();
            Lease(const Lease &) = delete;
            Lease &operator=(const Lease &) = delete;
            explicit operator bool() const { return manager_ != nullptr; }

        private:
            friend class UbHealthCallbackState;
            UbHealthCallbackState *owner_;
            DataPlaneManager *manager_;
        };

        Lease Acquire();
        bthread::Mutex mutex_;
        bthread::ConditionVariable drained_;
        size_t activeCallbacks_ = 0;
        DataPlaneManager *manager_;
    };

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
                              bool allowUbRuntimeFailure = false,
                              std::shared_ptr<HostMemoryPinManager> hostMemoryPinManager = nullptr,
                              UbHealthSummaryObserveHook ubHealthSummaryHook = {},
                              UbHealthSummaryApplyHook verifiedUbHealthSummaryHook = {},
                              std::function<void()> ubHealthWakeHook = {},
                              std::function<bool(const HostPort &)> ubPortHealthCapabilityCheck = {});
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

    Status GetOrCreateForDataLocation(const HostPort &workerAddr, TransportHint hint, uint64_t locationTopologyVersion,
                                      std::shared_ptr<IDataTransporter> &out,
                                      TransportPhaseLatencyRecorder *recorder = nullptr);

    /**
     * @brief Acquire an endpoint lease that prevents its data plane from being torn down.
     * @param[in] workerAddr Target worker address.
     * @param[in] hint Transport suggestion from the advisor.
     * @param[out] lease Lease owning the selected transporter and RPC client.
     * @param[in] recorder Optional request-scoped phase recorder.
     * @param[in] respectUbReadRecovery Whether a metadata UB read must honor the endpoint recovery gates.
     * @return K_OK when the endpoint is ready and leased; the error code otherwise.
     */
    Status AcquireDataPlaneLease(const HostPort &workerAddr, TransportHint hint,
                                 std::unique_ptr<DataPlaneLease> &lease,
                                 TransportPhaseLatencyRecorder *recorder = nullptr,
                                 bool respectUbReadRecovery = false);

    /**
     * @brief Run an operation while the selected data plane cannot be torn down.
     * @param[in] workerAddr Target worker address.
     * @param[in] hint Transport suggestion from the advisor.
     * @param[in] operation Operation executed with the endpoint data-plane lease held.
     * @param[in] recorder Optional request-scoped phase recorder.
     * @param[in] respectUbReadRecovery Whether a metadata UB read must honor the endpoint recovery gates.
     * @return K_OK when the operation succeeds; the connection or operation error otherwise.
     */
    Status WithDataPlaneLease(const HostPort &workerAddr, TransportHint hint,
                              const std::function<Status(const std::shared_ptr<IDataTransporter> &,
                                                         const std::shared_ptr<WorkerRpcClient> &)> &operation,
                              TransportPhaseLatencyRecorder *recorder = nullptr,
                              bool respectUbReadRecovery = false);

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

    /** @brief Query one Worker's cached UB port health without establishing or probing a data plane. */
    virtual Status QueryUbPortHealth(const HostPort &workerAddr, const std::string &expectedIncarnation,
                                     int32_t timeoutMs, UbHealthSummary &summary);

    bool RequestUbPortHealthVerification(const HostPort &workerAddr);
    void ObserveUbHealthSummary(const UbHealthSummary &summary);
    void RunDueUbPortHealthVerification();
    std::optional<std::chrono::steady_clock::time_point> GetUbPortHealthQueryDeadline() const;

    /** @brief Drop every data-plane transporter while retaining the shared RPC connection. */
    void ResetDataPlane(const HostPort &workerAddr);

    /**
     * @brief Drop one transport kind while retaining other transporters and the shared RPC connection.
     * @param[in] workerAddr Target worker address.
     * @param[in] kind Transport kind to reset.
     */
    void ResetTransporter(const HostPort &workerAddr, AccessTransportKind kind);

    /**
     * @brief Drop a UB data plane that the peer no longer recognizes.
     * @param[in] workerAddr Target worker address.
     * @param[in] stale Drop only when the entry still holds this transporter; nullptr drops unconditionally.
     *                   Guards against discarding a data plane that a concurrent writer has just rebuilt.
     * @param[in] markCooldown Whether to arm the read-path rebuild cooldown before dropping the plane.
     */
    virtual void ResetStaleUbDataPlane(const HostPort &workerAddr, const std::shared_ptr<IDataTransporter> &stale,
                                       bool markCooldown = false);

    /**
     * @brief Build and atomically publish a replacement for a stale UB data plane.
     * @param[in] workerAddr Target worker address.
     * @param[in] stale Transporter that triggered recovery; a newer healthy transporter is preserved.
     * @param[in] recorder Optional request-scoped phase recorder.
     * @return K_TRY_AGAIN when another request owns the rebuild slot or the endpoint generation changes.
     */
    Status RebuildStaleUbDataPlane(const HostPort &workerAddr, const std::shared_ptr<IDataTransporter> &stale,
                                   TransportPhaseLatencyRecorder *recorder = nullptr);

    /**
     * @brief Suppress read-path UB rebuild attempts for an endpoint until the cooldown elapses.
     * Read path only: the cooldown is never enforced in GetOrCreate, so writers, direct leases and
     * replica reads keep their own behaviour.
     */
    virtual void MarkUbRebuildCooldown(const HostPort &workerAddr);

    /**
     * @brief True while the read path must skip UB rebuild attempts for workerAddr.
     * AdmitUbRead() applies the same predicate (WorkerTransportEntry::UbRebuildCoolingDown) from the entry it
     * already holds, so admission pays only one lookup; this accessor is the observable form of the cooldown
     * (diagnostics and tests). Not virtual: nothing overrides it, and a test seam without consumers only
     * suggests that the production path can diverge.
     */
    bool IsUbRebuildCoolingDown(const HostPort &workerAddr) const;

    /** @brief How a read-path request should obtain the UB data plane for an endpoint. */
    enum class UbReadAdmission {
        PROCEED,  ///< The UB data plane is already usable; no rebuild slot is held by this request.
        REBUILD,  ///< This request owns the per-endpoint rebuild slot and must release it when the handshake ends.
        DEGRADE,  ///< Skip the handshake and serve this request over TCP instead of queueing for it.
    };

    /**
     * @brief Decide how a read-path request should obtain the UB data plane, and coordinate rebuilds.
     * Two read-path-private gates are evaluated from one entry lookup:
     * - the rebuild cooldown (see MarkUbRebuildCooldown), which skips a handshake the peer is likely to
     *   reject again;
     * - a per-endpoint rebuild slot, because a wave of concurrent readers can all pass the cooldown before
     *   the first handshake fails and arms it, so without coordination every queued request pays for its own
     *   doomed handshake (N x handshake latency, and N failed handshakes when the peer keeps rejecting).
     * This grants the slot to exactly one request; the others are told to DEGRADE and serve the request over
     * TCP inline. Waiters never block and no lock is held across the RPC.
     *
     * Read path only: GetOrCreate enforces no gate of its own, so the coordination stays private to readers
     * and leaves writers, direct leases and replica reads untouched.
     *
     * @param[in] workerAddr Target worker address.
     * @param[out] slotOwner Non-zero when the return value is REBUILD; pass it to ReleaseUbRebuildSlot().
     * @param[out] degradedByCooldown Optional; set to true when DEGRADE was decided by the cooldown rather
     *             than by another reader holding the slot. Only used for diagnostics.
     * @return The admission decision for this request.
     */
    UbReadAdmission AdmitUbRead(const HostPort &workerAddr, uint64_t &slotOwner, bool *degradedByCooldown = nullptr);

    /**
     * @brief Release a per-endpoint UB rebuild slot granted by AdmitUbRead().
     * @param[in] workerAddr Target worker address.
     * @param[in] slotOwner Owner id returned by the matching AdmitUbRead() call; 0 releases nothing.
     */
    void ReleaseUbRebuildSlot(const HostPort &workerAddr, uint64_t slotOwner);

    /**
     * @brief RAII owner of a per-endpoint UB rebuild slot granted by AdmitUbRead().
     * Nested in DataPlaneManager and declared next to the API that hands the slot out, because "take a slot,
     * release it" is the obligation that comes with UbReadAdmission::REBUILD: a request that never releases
     * leaves ubRebuildSlotOwner non-zero, so every later reader of that endpoint degrades to TCP silently
     * until the entry is reconciled or torn down. Keeping the type on this class makes the obligation
     * discoverable from AdmitUbRead() instead of hiding it in the .cpp of one caller.
     * @param[in] manager Manager that granted the slot; may be null when slotOwner is 0.
     * @param[in] address Endpoint the slot belongs to.
     * @param[in] slotOwner Owner id returned by AdmitUbRead(); 0 releases nothing.
     */
    class UbRebuildSlotGuard {
    public:
        UbRebuildSlotGuard(std::shared_ptr<DataPlaneManager> manager, HostPort address, uint64_t slotOwner);
        ~UbRebuildSlotGuard();

        UbRebuildSlotGuard(const UbRebuildSlotGuard &) = delete;
        UbRebuildSlotGuard &operator=(const UbRebuildSlotGuard &) = delete;

    private:
        std::shared_ptr<DataPlaneManager> manager_;
        const HostPort address_;
        uint64_t slotOwner_;
    };

    /**
     * @brief Check whether an endpoint's data plane has been unused for at least quietMs.
     * @param[in] workerAddr Target worker address.
     * @param[in] quietMs Idle window in milliseconds; 0 means "do not check".
     * @return True when the endpoint has no entry, no recorded non-TCP use, or has been quiet for quietMs.
     */
    bool IsEndpointDataPlaneQuiet(const HostPort &workerAddr, uint64_t quietMs);

    /** @brief Permanently reject SHM rebuilds for the current endpoint entry after scale-in is observed. */
    void MarkShmDraining(const HostPort &workerAddr);

    /** @brief Drop endpoint connections while preserving any observed scale-in SHM rejection. */
    void Teardown(const HostPort &workerAddr);

    /**
     * @brief Atomically publish the latest worker admission set before route publication.
     * @param[in] snapshot Validated current worker snapshot.
     * @return K_OK on success; K_INVALID for a regressing version; K_SHUTTING_DOWN during shutdown.
     */
    Status UpdateWorkerSnapshot(const WorkerSnapshot &snapshot);

    /** @brief Renew matching confirmed-ring health without scheduling topology reconciliation. */
    void RecordRoutingRefresh(uint64_t ringVersion);

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
    friend class ObjectMetadataClient;
    friend class DataPlaneManagerAdmissionTestPeer;
    friend class DataPlaneManagerQuietTestPeer;

    struct EndpointAdmissionSnapshot {
        EndpointAdmissionSnapshot(uint64_t version, std::shared_ptr<const std::unordered_set<std::string>> workers,
                                  bool isProvisional, int64_t confirmedMs,
                                  std::shared_ptr<const std::unordered_map<HostPort, std::string>> incarnations = {})
            : ringVersion(version), liveWorkers(std::move(workers)), provisional(isProvisional),
              lastConfirmedMs(confirmedMs), workerIncarnations(std::move(incarnations))
        {
        }

        uint64_t ringVersion;
        std::shared_ptr<const std::unordered_set<std::string>> liveWorkers;
        bool provisional{ false };
        int64_t lastConfirmedMs{ 0 };
        // One grace window per confirmed-refresh generation; readers never rearm an expired window.
        mutable std::atomic<int64_t> degradedDeadlineMs{ 0 };
        std::shared_ptr<const std::unordered_map<HostPort, std::string>> workerIncarnations;
    };

    struct WorkerTransportEntry {
        bool HasAliveTransporter(AccessTransportKind expectedKind) const;
        std::shared_ptr<IDataTransporter> GetTransporter(AccessTransportKind expectedKind) const;
        std::shared_ptr<IDataTransporter> &GetTransporterSlot(AccessTransportKind expectedKind);
        void ResetDataPlaneLocked();
        void ResetTransporterLocked(AccessTransportKind expectedKind);
        void ResetDataPlane();
        void ResetTransporter(AccessTransportKind expectedKind);

        /**
         * @brief Single source of the read-path rebuild-cooldown predicate.
         * Both AdmitUbRead() (which already holds the entry) and the diagnostic accessor
         * IsUbRebuildCoolingDown() evaluate the cooldown through this, so the observable form cannot drift
         * from the one that actually gates reads.
         * @param[in] nowMs Current steady-clock milliseconds.
         */
        bool UbRebuildCoolingDown(int64_t nowMs) const
        {
            return nowMs < ubRebuildAllowedAfterMs.load(std::memory_order_relaxed);
        }

        bthread::RWLock mutex;
        std::shared_ptr<WorkerRpcClient> rpcClient;
        std::shared_ptr<IDataTransporter> shmTransporter;
        std::shared_ptr<IDataTransporter> fallbackTransporter;
        AccessTransportKind fallbackKind = AccessTransportKind::TCP;
        bool shmDraining = false;
        // Steady-clock ms before which the read path skips UB rebuild attempts (see MarkUbRebuildCooldown).
        std::atomic<int64_t> ubRebuildAllowedAfterMs{ 0 };
        // Read-path per-endpoint UB rebuild slot: id of the request that currently owns the right to run a UB
        // handshake for this endpoint, or 0 when the slot is free. Passing the owner id back on release keeps
        // the release a no-op if this entry was dropped and recreated while the handshake was in flight.
        // Read path only — GetOrCreate never consults it, so writers and direct leases are unaffected.
        std::atomic<uint64_t> ubRebuildSlotOwner{ 0 };
        // Approximate last time a non-TCP data plane was selected for this endpoint (steady-clock ms).
        // Relaxed atomic: a monotonic best-effort signal for standby drain decisions, not a precise counter.
        std::atomic<int64_t> lastDataPlaneUseMs{ 0 };
        // Access under the EntryMap accessor so location admission is ordered with reconcile deletion.
        uint64_t locationAdmissionVersion = 0;
    };

    using EntryMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<WorkerTransportEntry>>;

    struct TransportBuildContext {
        const HostPort &workerAddr;
        TransportHint hint;
        AccessTransportKind expectedKind;
        TransportPhaseLatencyRecorder *recorder;
        bool respectUbReadRecovery = false;
    };

    Status GetOrCreateEntry(const std::string &workerKey, std::shared_ptr<WorkerTransportEntry> &entry,
                            bool requireSnapshotAdmission = true);
    bool AllowDegradedEndpointAdmission(const EndpointAdmissionSnapshot &snapshot);

    Status GetOrCreateLocationEntry(const std::string &workerKey, uint64_t topologyVersion,
                                    std::shared_ptr<WorkerTransportEntry> &entry);

    void DetachRejectedLocationEntry(const std::string &workerKey,
                                     const std::shared_ptr<WorkerTransportEntry> &entry,
                                     uint64_t topologyVersion);

    Status GetOrCreateRpcClientImpl(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out,
                                    bool requireSnapshotAdmission);

    Status GetOrCreateRedirectMetadataRpcClient(const HostPort &workerAddr, uint64_t redirectTopologyVersion,
                                                std::shared_ptr<WorkerRpcClient> &out);

    Status ValidateRedirectMetadataAdmission(const HostPort &workerAddr,
                                             uint64_t redirectTopologyVersion) const;

    Status ValidateVersionedEndpointAdmission(const std::string &workerKey, uint64_t topologyVersion,
                                              bool &bypassedSnapshot) const;

    Status GetOrBuildTransporter(const TransportBuildContext &context,
                                 const std::shared_ptr<WorkerTransportEntry> &entry,
                                 std::shared_ptr<IDataTransporter> &out);

    Status EnsureRpcClientLocked(const HostPort &workerAddr, const std::shared_ptr<WorkerTransportEntry> &entry,
                                 TransportPhaseLatencyRecorder *recorder);

    Status EnsureTransporterLocked(const TransportBuildContext &context,
                                   const std::shared_ptr<WorkerTransportEntry> &entry,
                                   bool &cachedFallbackAlongsideShm);

    void CompleteUbPortHealthVerification(const cluster::RemoteUbQueryTicket &ticket) noexcept;
    void FailUbPortHealthQueryDispatch(const cluster::RemoteUbQueryTicket &ticket,
                                       const std::string &message) noexcept;
    void ApplyUbPortHealthCapabilityCheck(const cluster::RemoteUbQueryTicket &ticket, Status &rc) const;

    /**
     * @brief Record that a non-TCP data plane actually served a request for this endpoint, for drain decisions.
     * @param[in] entry Endpoint entry that served the data plane.
     * @param[in] kind Kind of the transporter that served the request — the actual one (out->Kind()), not the
     *                 requested one: a UB candidate served by the cached TCP fallback must not hold the standby
     *                 drain back, since a worker-side URMA deletion cannot invalidate a TCP transporter.
     */
    void MarkDataPlaneUse(const std::shared_ptr<WorkerTransportEntry> &entry, AccessTransportKind kind);

    Status BuildUbTransporter(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                              TransportPhaseLatencyRecorder *recorder, std::shared_ptr<IDataTransporter> &out);

    // Monotonic ids handed out to read-path UB rebuild slots; 0 is reserved for "slot free".
    std::atomic<uint64_t> nextUbRebuildSlotId_{ 1 };
    EntryMap entries_;
    std::shared_ptr<const EndpointAdmissionSnapshot> endpointAdmissionSnapshot_;
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
    UbHealthSummaryObserveHook ubHealthSummaryHook_;
    UbHealthSummaryApplyHook verifiedUbHealthSummaryHook_;
    std::function<void()> ubHealthWakeHook_;
    std::function<bool(const HostPort &)> ubPortHealthCapabilityCheck_;
    std::shared_ptr<UbHealthCallbackState> ubHealthCallbackState_;
    cluster::RemoteUbPortHealthVerifier ubPortHealthVerifier_;
    std::shared_ptr<ThreadPool> ubPortHealthQueryPool_{ std::make_shared<ThreadPool>(
        0, cluster::REMOTE_UB_PORT_HEALTH_MAX_CONCURRENT_QUERIES, "client-ub-health") };
    // Counts submitted tasks, including RPCs whose topology ticket has been retired.
    std::atomic<size_t> ubPortHealthQueriesInFlight_{ 0 };
    UbHealthSummaryCache observedUbHealthSummaries_;
    bool enableClientDirectPipelineH2D_ = false;
    int32_t pipelineThreadNum_ = 64;
    bthread::Mutex lifecycleMutex_;
    std::atomic<bool> initialized_{ false };
    std::weak_ptr<ThreadPool> releasePool_;
    std::shared_ptr<ThreadPool> shmMaintenancePool_;
    std::shared_ptr<HostMemoryPinManager> hostMemoryPinManager_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_MANAGER_H
