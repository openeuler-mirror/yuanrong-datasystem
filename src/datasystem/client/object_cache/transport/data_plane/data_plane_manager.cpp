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

/** Description: Implements endpoint-scoped data-plane transporter management. */

#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/client/object_cache/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/object_cache/transport/data_plane/ub_connection.h"
#include "datasystem/client/object_cache/transport/data_plane/ub_transporter.h"
#include "datasystem/client/object_cache/transport/object_read/object_read_types.h"
#include "datasystem/client/object_cache/transport/transport_phase_latency_recorder.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

DataPlaneManager::UbHealthCallbackState::UbHealthCallbackState(DataPlaneManager *manager) : manager_(manager)
{
}

void DataPlaneManager::UbHealthCallbackState::Detach()
{
    std::unique_lock<bthread::Mutex> lock(mutex_);
    manager_ = nullptr;
    while (activeCallbacks_ != 0) {
        drained_.wait(lock);
    }
}

DataPlaneManager::UbHealthCallbackState::Lease DataPlaneManager::UbHealthCallbackState::Acquire()
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    if (manager_ != nullptr) {
        ++activeCallbacks_;
    }
    return Lease(this, manager_);
}

DataPlaneManager::UbHealthCallbackState::Lease::~Lease()
{
    if (manager_ != nullptr) {
        std::lock_guard<bthread::Mutex> lock(owner_->mutex_);
        if (--owner_->activeCallbacks_ == 0) {
            owner_->drained_.notify_all();
        }
    }
}

void DataPlaneManager::UbHealthCallbackState::ObserveSummary(const UbHealthSummary &summary)
{
    auto lease = Acquire();
    if (!lease) {
        return;
    }
    try {
        lease.manager_->ObserveUbHealthSummary(summary);
    } catch (const std::exception &error) {
        LOG(ERROR) << "Client UB health observation callback threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Client UB health observation callback threw";
    }
}

namespace {

constexpr uint32_t TRANSPORT_STATE_LOG_RATE = 100;
// Ring-health grace: admission keeps rejecting unknown endpoints while a confirmed publish is this
// recent; a longer gap means the ring refresh itself is lost.
constexpr int64_t SNAPSHOT_REFRESH_GRACE_MS = 60'000;
// Upper bound on how long admission may degrade (allow unknown endpoints) while the ring is lost.
constexpr int64_t DEGRADED_ADMISSION_TTL_MS = 120'000;
// Minimum gap between two recordings of "this endpoint's data plane was just used". The standby drain gate
// only needs multi-second granularity, so refreshing at most this often keeps the hot path from dirtying the
// timestamp on every read. The recorded value therefore lags the true last use by up to this interval:
// FLAGS_standby_drain_data_plane_quiet_ms is a dynamic uint32 with no non-zero lower bound, so
// IsEndpointDataPlaneQuiet() must discount this bound instead of assuming it is negligible.
constexpr int64_t DATA_PLANE_USE_REFRESH_INTERVAL_MS = 100;
constexpr size_t SHM_MAINTENANCE_THREAD_COUNT = 4;

int64_t SteadyNowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

AccessTransportKind KindForHint(TransportHint hint)
{
    if (hint == TransportHint::SHM_CANDIDATE) {
        return AccessTransportKind::SHM;
    }
    return hint == TransportHint::TCP_ONLY ? AccessTransportKind::TCP : AccessTransportKind::UB;
}

void LogTransporterReady(const HostPort &workerAddr, AccessTransportKind kind, bool retainedShm)
{
    if (retainedShm) {
        LOG_EVERY_N(INFO, TRANSPORT_STATE_LOG_RATE)
            << "[TransportGet][Connection] Cached fallback while retaining SHM, endpoint: "
            << workerAddr.ToString() << ", fallback: " << AccessTransportTracker::KindToName(kind);
    }
    VLOG(1) << "[TransportGet][Connection] Data transporter ready, endpoint: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(kind) << ", retained_shm: " << retainedShm;
}

Status InitClientUbRuntime(uint64_t fastTransportMemSize, bool enablePipelineH2D)
{
#ifdef USE_URMA
    static std::once_flag initOnce;
    static Status initStatus;
    SetClientFastTransportMode(FastTransportMode::UB, fastTransportMemSize, enablePipelineH2D);
    std::call_once(initOnce, []() {
        initStatus = InitializeFastTransportManager(GetClientFastTransportLocalAddr());
        if (initStatus.IsError()) {
            initStatus.AppendMsg("Fast transport init failed");
        }
    });
    return initStatus;
#else
    (void)fastTransportMemSize;
    (void)enablePipelineH2D;
    return Status::OK();
#endif
}

std::unordered_set<std::string> BuildLiveWorkerSet(const WorkerSnapshot &snapshot)
{
    std::unordered_set<std::string> liveWorkers;
    liveWorkers.reserve(snapshot.shmCandidateAddrs.size() + snapshot.remoteTransportAddrs.size());
    for (const auto &worker : snapshot.shmCandidateAddrs) {
        liveWorkers.insert(worker.ToString());
    }
    for (const auto &worker : snapshot.remoteTransportAddrs) {
        liveWorkers.insert(worker.ToString());
    }
    return liveWorkers;
}

std::vector<std::string> BuildWriteProbeWorkers(const WorkerSnapshot &snapshot,
                                                const std::unordered_set<std::string> &liveWorkers)
{
    std::vector<std::string> workers;
    workers.reserve(snapshot.writeProbeAddrs.size());
    for (const auto &worker : snapshot.writeProbeAddrs) {
        auto key = worker.ToString();
        if (liveWorkers.count(key) != 0) {
            workers.emplace_back(std::move(key));
        }
    }
    std::sort(workers.begin(), workers.end());
    workers.erase(std::unique(workers.begin(), workers.end()), workers.end());
    return workers;
}

}  // namespace

bool DataPlaneManager::WorkerTransportEntry::HasAliveTransporter(AccessTransportKind expectedKind) const
{
    if (expectedKind == AccessTransportKind::SHM) {
        return !shmDraining && shmTransporter != nullptr && shmTransporter->IsAlive();
    }
    return fallbackKind == expectedKind && fallbackTransporter != nullptr && fallbackTransporter->IsAlive();
}

std::shared_ptr<IDataTransporter> DataPlaneManager::WorkerTransportEntry::GetTransporter(
    AccessTransportKind expectedKind) const
{
    if (expectedKind == AccessTransportKind::SHM) {
        return shmTransporter;
    }
    return fallbackKind == expectedKind ? fallbackTransporter : nullptr;
}

std::shared_ptr<IDataTransporter> &DataPlaneManager::WorkerTransportEntry::GetTransporterSlot(
    AccessTransportKind expectedKind)
{
    return expectedKind == AccessTransportKind::SHM ? shmTransporter : fallbackTransporter;
}

void DataPlaneManager::WorkerTransportEntry::ResetTransporterLocked(AccessTransportKind expectedKind)
{
    auto staleTransporter = std::move(GetTransporterSlot(expectedKind));
    if (staleTransporter != nullptr) {
        staleTransporter->CloseDataPlane();
    }
}

void DataPlaneManager::WorkerTransportEntry::ResetDataPlaneLocked()
{
    ResetTransporterLocked(AccessTransportKind::SHM);
    ResetTransporterLocked(fallbackKind);
}

void DataPlaneManager::WorkerTransportEntry::ResetDataPlane()
{
    bthread::RWLockWrGuard lock(mutex);
    ResetDataPlaneLocked();
}

void DataPlaneManager::WorkerTransportEntry::ResetTransporter(AccessTransportKind expectedKind)
{
    bthread::RWLockWrGuard lock(mutex);
    if (expectedKind != AccessTransportKind::SHM && fallbackKind != expectedKind) {
        return;
    }
    ResetTransporterLocked(expectedKind);
}

DataPlaneManager::DataPlaneManager(std::shared_ptr<Signature> signature, uint64_t fastTransportMemSize,
                                   BrpcChannelConfig channelConfig,
                                   std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider,
                                   bool enableClientDirectPipelineH2D, int32_t pipelineThreadNum,
                                   std::shared_ptr<ThreadPool> releasePool, bool initializeUbRuntime,
                                   bool allowUbRuntimeFailure,
                                   std::shared_ptr<HostMemoryPinManager> hostMemoryPinManager,
                                   UbHealthSummaryApplyHook ubHealthSummaryHook,
                                   UbHealthSummaryApplyHook verifiedUbHealthSummaryHook,
                                   std::function<void()> ubHealthWakeHook,
                                   std::function<bool(const HostPort &)> ubPortHealthCapabilityCheck)
    : signature_(std::move(signature)), channelConfig_(std::move(channelConfig)),
      ubBufferProvider_(std::move(ubBufferProvider)), fastTransportMemSize_(fastTransportMemSize),
      initializeUbRuntime_(initializeUbRuntime), allowUbRuntimeFailure_(allowUbRuntimeFailure),
      ubHealthSummaryHook_(std::move(ubHealthSummaryHook)),
      verifiedUbHealthSummaryHook_(std::move(verifiedUbHealthSummaryHook)),
      ubHealthWakeHook_(std::move(ubHealthWakeHook)),
      ubPortHealthCapabilityCheck_(std::move(ubPortHealthCapabilityCheck)),
      enableClientDirectPipelineH2D_(enableClientDirectPipelineH2D), pipelineThreadNum_(pipelineThreadNum),
      releasePool_(std::move(releasePool)),
      shmMaintenancePool_(
          std::make_shared<ThreadPool>(0, SHM_MAINTENANCE_THREAD_COUNT, "shm_maintenance")),
      hostMemoryPinManager_(std::move(hostMemoryPinManager))
{
    ubHealthCallbackState_ = std::make_shared<UbHealthCallbackState>(this);
}

DataPlaneManager::DataPlaneLease::~DataPlaneLease() = default;

const std::shared_ptr<IDataTransporter> &DataPlaneManager::DataPlaneLease::GetTransporter() const
{
    return transporter_;
}

const std::shared_ptr<WorkerRpcClient> &DataPlaneManager::DataPlaneLease::GetRpcClient() const
{
    return rpcClient_;
}

DataPlaneManager::~DataPlaneManager()
{
    Shutdown();
}

Status DataPlaneManager::Init()
{
    std::lock_guard<bthread::Mutex> lock(lifecycleMutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    if (enableClientDirectPipelineH2D_) {
        RETURN_IF_NOT_OK(OsXprtPipln::SetClientPipelineThreadNum(pipelineThreadNum_));
    }
    if (initializeUbRuntime_ || enableClientDirectPipelineH2D_) {
        Status rc = InitClientUbRuntime(fastTransportMemSize_, enableClientDirectPipelineH2D_);
        if (rc.IsError()) {
            if (!allowUbRuntimeFailure_) {
                return rc;
            }
            LOG(WARNING) << "Optional client UB runtime initialization failed; continue with SHM/TCP. Detail: "
                         << rc.ToString();
        }
    }
#ifdef USE_URMA
    if (enableClientDirectPipelineH2D_) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().EnsureClientPipelineH2DEnv());
    }
#endif
    initialized_.store(true, std::memory_order_release);
    return Status::OK();
}

Status DataPlaneManager::CreateWorkerRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out)
{
    auto rpcClient = std::make_shared<WorkerRpcClient>(workerAddr, signature_, channelConfig_);
    RETURN_IF_NOT_OK(rpcClient->Init());
    auto callbackState = ubHealthCallbackState_;
    rpcClient->SetUbHealthSummaryCallback(
        [callbackState](const UbHealthSummary &summary) { callbackState->ObserveSummary(summary); });
    out = std::move(rpcClient);
    VLOG(1) << "[TransportGet][Connection] RPC connection ready, endpoint: " << workerAddr.ToString();
    return Status::OK();
}

Status DataPlaneManager::GetOrCreate(const HostPort &workerAddr, TransportHint hint,
                                     std::shared_ptr<IDataTransporter> &out,
                                     TransportPhaseLatencyRecorder *recorder)
{
    out.reset();
    std::shared_ptr<WorkerTransportEntry> entry;
    const auto lookupBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                 : recorder->StartPhase();
    Status status = GetOrCreateEntry(workerAddr.ToString(), entry);
    if (recorder != nullptr) {
        recorder->RecordPhase("connection_entry_lookup", lookupBegin, TransportLatencyThreshold::PROCESS);
    }
    RETURN_IF_NOT_OK(status);
    const TransportBuildContext context{ workerAddr, hint, KindForHint(hint), recorder };
    return GetOrBuildTransporter(context, entry, out);
}

Status DataPlaneManager::GetOrCreateForDataLocation(const HostPort &workerAddr, TransportHint hint,
                                                    uint64_t locationTopologyVersion,
                                                    std::shared_ptr<IDataTransporter> &out,
                                                    TransportPhaseLatencyRecorder *recorder)
{
    out.reset();
    const std::string workerKey = workerAddr.ToString();
    bool bypassedSnapshot = false;
    if (hasWorkerSnapshot_.load(std::memory_order_acquire)) {
        RETURN_IF_NOT_OK(ValidateVersionedEndpointAdmission(workerKey, locationTopologyVersion, bypassedSnapshot));
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    RETURN_IF_NOT_OK(bypassedSnapshot ? GetOrCreateLocationEntry(workerKey, locationTopologyVersion, entry)
                                      : GetOrCreateEntry(workerKey, entry, false));
    const TransportBuildContext context{ workerAddr, hint, KindForHint(hint), recorder };
    RETURN_IF_NOT_OK(GetOrBuildTransporter(context, entry, out));
    if (bypassedSnapshot) {
        bool ignored = false;
        Status rc = ValidateVersionedEndpointAdmission(workerKey, locationTopologyVersion, ignored);
        if (rc.IsError()) {
            out.reset();
            DetachRejectedLocationEntry(workerKey, entry, locationTopologyVersion);
            return rc;
        }
    }
    return Status::OK();
}

Status DataPlaneManager::AcquireDataPlaneLease(const HostPort &workerAddr, TransportHint hint,
                                               std::unique_ptr<DataPlaneLease> &lease,
                                               TransportPhaseLatencyRecorder *recorder,
                                               bool respectUbReadRecovery)
{
    lease.reset();
    const AccessTransportKind expectedKind = KindForHint(hint);
    std::shared_ptr<WorkerTransportEntry> entry;
    const auto lookupBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                 : recorder->StartPhase();
    Status status = GetOrCreateEntry(workerAddr.ToString(), entry);
    if (recorder != nullptr) {
        recorder->RecordPhase("connection_entry_lookup", lookupBegin, TransportLatencyThreshold::PROCESS);
    }
    RETURN_IF_NOT_OK(status);
    std::shared_ptr<IDataTransporter> transporter;
    const TransportBuildContext context{ workerAddr, hint, expectedKind, recorder, respectUbReadRecovery };
    RETURN_IF_NOT_OK(GetOrBuildTransporter(context, entry, transporter));

    auto acquired = std::unique_ptr<DataPlaneLease>(new DataPlaneLease());
    acquired->entry_ = entry;
    const auto leaseBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                : recorder->StartPhase();
    acquired->entryLock_ = std::make_unique<bthread::RWLockRdGuard>(entry->mutex);
    if (recorder != nullptr) {
        recorder->RecordPhase("connection_lease_lock_wait", leaseBegin, TransportLatencyThreshold::PROCESS);
    }
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(!(respectUbReadRecovery && expectedKind == AccessTransportKind::UB
                               && entry->ubRebuildSlotOwner.load(std::memory_order_acquire) != 0),
                             K_TRY_AGAIN, "UB data plane rebuild started before lease acquisition");
    if (entry->GetTransporter(expectedKind) != transporter || !entry->HasAliveTransporter(expectedKind)) {
        RETURN_STATUS(respectUbReadRecovery ? K_TRY_AGAIN : K_URMA_NEED_CONNECT,
                      "Data-plane transporter changed before lease acquisition");
    }
    CHECK_FAIL_RETURN_STATUS(entry->rpcClient != nullptr && entry->rpcClient->IsAlive(), K_RPC_UNAVAILABLE,
                             "RPC client is unavailable while acquiring lease");
    acquired->transporter_ = std::move(transporter);
    acquired->rpcClient_ = entry->rpcClient;
    lease = std::move(acquired);
    return Status::OK();
}

Status DataPlaneManager::WithDataPlaneLease(
    const HostPort &workerAddr, TransportHint hint,
    const std::function<Status(const std::shared_ptr<IDataTransporter> &,
                               const std::shared_ptr<WorkerRpcClient> &)> &operation,
    TransportPhaseLatencyRecorder *recorder, bool respectUbReadRecovery)
{
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(operation), K_INVALID, "Data-plane lease operation is empty");
    std::unique_ptr<DataPlaneLease> lease;
    RETURN_IF_NOT_OK(AcquireDataPlaneLease(workerAddr, hint, lease, recorder, respectUbReadRecovery));
    return operation(lease->GetTransporter(), lease->GetRpcClient());
}

Status DataPlaneManager::GetOrCreateRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out)
{
    return GetOrCreateRpcClientImpl(workerAddr, out, true);
}

Status DataPlaneManager::GetOrCreateRedirectMetadataRpcClient(const HostPort &workerAddr,
                                                              uint64_t redirectTopologyVersion,
                                                              std::shared_ptr<WorkerRpcClient> &out)
{
    RETURN_IF_NOT_OK(ValidateRedirectMetadataAdmission(workerAddr, redirectTopologyVersion));
    RETURN_IF_NOT_OK(GetOrCreateRpcClientImpl(workerAddr, out, false));
    Status rc = ValidateRedirectMetadataAdmission(workerAddr, redirectTopologyVersion);
    if (rc.IsError()) {
        out.reset();
    }
    return rc;
}

Status DataPlaneManager::ValidateRedirectMetadataAdmission(const HostPort &workerAddr,
                                                           uint64_t redirectTopologyVersion) const
{
    bool bypassedSnapshot = false;
    return ValidateVersionedEndpointAdmission(workerAddr.ToString(), redirectTopologyVersion, bypassedSnapshot);
}

Status DataPlaneManager::ValidateVersionedEndpointAdmission(const std::string &workerKey,
                                                            uint64_t topologyVersion,
                                                            bool &bypassedSnapshot) const
{
    bypassedSnapshot = false;
    auto snapshot = std::atomic_load(&endpointAdmissionSnapshot_);
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr && snapshot->liveWorkers != nullptr, K_NOT_READY,
                             STALE_TRANSPORT_SNAPSHOT_MESSAGE);
    if (snapshot->liveWorkers->find(workerKey) != snapshot->liveWorkers->end()) {
        return Status::OK();
    }
    // A non-zero location admission version means the Master verified this location's endpoints
    // against its immutable membership snapshot and placed that snapshot's version on the result,
    // so an equal version carries the same provenance as a newer one (e.g. a Worker re-joining
    // under a same-version hostId-only ring republish). Version 0 (older peer, no evidence) and
    // strictly older versions stay rejected.
    CHECK_FAIL_RETURN_STATUS(topologyVersion > 0 && topologyVersion >= snapshot->ringVersion, K_NOT_READY,
                             std::string(STALE_TRANSPORT_SNAPSHOT_MESSAGE) + ": observed version "
                                 + std::to_string(topologyVersion) + ", snapshot version "
                                 + std::to_string(snapshot->ringVersion));
    bypassedSnapshot = true;
    return Status::OK();
}

Status DataPlaneManager::GetOrCreateRpcClientImpl(const HostPort &workerAddr,
                                                  std::shared_ptr<WorkerRpcClient> &out,
                                                  bool requireSnapshotAdmission)
{
    out.reset();
    std::shared_ptr<WorkerTransportEntry> entry;
    RETURN_IF_NOT_OK(GetOrCreateEntry(workerAddr.ToString(), entry, requireSnapshotAdmission));
    {
        bthread::RWLockRdGuard lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        if (entry->rpcClient != nullptr && entry->rpcClient->IsAlive()) {
            out = entry->rpcClient;
            return Status::OK();
        }
    }
    bthread::RWLockWrGuard lock(entry->mutex);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    RETURN_IF_NOT_OK(EnsureRpcClientLocked(workerAddr, entry, nullptr));
    out = entry->rpcClient;
    return Status::OK();
}

Status DataPlaneManager::ProbeUbConnection(const HostPort &workerAddr, const std::function<void()> &commitRecovery)
{
    HostPort probeWorker;
    {
        std::lock_guard<bthread::Mutex> lock(probeMutex_);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        CHECK_FAIL_RETURN_STATUS(hasWorkerSnapshot_.load(std::memory_order_acquire), K_NOT_READY,
                                 "Worker snapshot is not published for UB recovery probe");
        CHECK_FAIL_RETURN_STATUS(!writeProbeWorkers_.empty(), K_NOT_FOUND,
                                 "No admitted Worker endpoint is available for UB recovery probe");

        const std::string preferredWorker = workerAddr.ToString();
        size_t selectedIndex = 0;
        if (probePreferredWorker_ != preferredWorker || lastProbeWorker_.empty()) {
            auto preferred = writeProbeWorkerIndices_.find(preferredWorker);
            selectedIndex = preferred == writeProbeWorkerIndices_.end() ? 0 : preferred->second;
        } else {
            auto last = writeProbeWorkerIndices_.find(lastProbeWorker_);
            selectedIndex = last == writeProbeWorkerIndices_.end() ? 0 : (last->second + 1) % writeProbeWorkers_.size();
        }
        const std::string &selectedWorker = writeProbeWorkers_[selectedIndex];
        probePreferredWorker_ = preferredWorker;
        lastProbeWorker_ = selectedWorker;
        RETURN_IF_NOT_OK(probeWorker.ParseString(selectedWorker));
    }
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(GetOrCreateRpcClient(probeWorker, rpcClient));
    RETURN_IF_NOT_OK(EstablishUbProbe(probeWorker, rpcClient));

    std::lock_guard<bthread::Mutex> lock(probeMutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(
        hasWorkerSnapshot_.load(std::memory_order_acquire)
            && writeProbeWorkerIndices_.count(probeWorker.ToString()) != 0,
        K_NOT_FOUND,
        "Worker endpoint is absent from latest writable transport snapshot: " + probeWorker.ToString());
    probePreferredWorker_.clear();
    lastProbeWorker_.clear();
    if (commitRecovery) {
        commitRecovery();
    }
    return Status::OK();
}

Status DataPlaneManager::ProbeUbWriteTarget(const HostPort &workerAddr)
{
    {
        std::lock_guard<bthread::Mutex> lock(probeMutex_);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        CHECK_FAIL_RETURN_STATUS(hasWorkerSnapshot_.load(std::memory_order_acquire), K_NOT_READY,
                                 "Worker snapshot is not published for UB write-target probe");
        CHECK_FAIL_RETURN_STATUS(writeProbeWorkerIndices_.count(workerAddr.ToString()) != 0, K_NOT_FOUND,
                                 "Worker endpoint is absent from latest writable transport snapshot: "
                                     + workerAddr.ToString());
    }
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(GetOrCreateRpcClient(workerAddr, rpcClient));
    RETURN_IF_NOT_OK(EstablishUbProbe(workerAddr, rpcClient));
    std::lock_guard<bthread::Mutex> lock(probeMutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(writeProbeWorkerIndices_.count(workerAddr.ToString()) != 0, K_NOT_FOUND,
                             "Worker endpoint left the writable transport snapshot during probe: "
                                 + workerAddr.ToString());
    return Status::OK();
}

Status DataPlaneManager::ProbeProviderUbRecovery(const HostPort &workerAddr, const std::string &expectedIncarnation,
                                                 int32_t timeoutMs, UbHealthSummary &summary)
{
    summary = UbHealthSummary{};
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(GetOrCreateRpcClient(workerAddr, rpcClient));
    ProviderUbRecoveryProbeRspPb response;
    Status rpcRc = rpcClient->ProbeProviderUbRecovery(expectedIncarnation, timeoutMs, response);
    if (!response.has_health_summary()) {
        if (rpcRc.IsError()) {
            return rpcRc;
        }
        RETURN_STATUS(K_INVALID, "Provider UB recovery response has no health summary");
    }

    UbHealthSummary decodedSummary;
    Status decodeRc = DecodeUbHealthSummary(response.health_summary(), decodedSummary);
    if (rpcRc.IsError()) {
        if (decodeRc.IsOk() && decodedSummary.worker == workerAddr) {
            summary = std::move(decodedSummary);
        }
        return rpcRc;
    }
    RETURN_IF_NOT_OK(decodeRc);
    CHECK_FAIL_RETURN_STATUS(decodedSummary.worker == workerAddr, K_INVALID,
                             "Provider UB recovery response Worker does not match RPC endpoint");
    summary = std::move(decodedSummary);
    CHECK_FAIL_RETURN_STATUS(expectedIncarnation.empty() || summary.incarnation == expectedIncarnation, K_NOT_READY,
                             "Provider UB recovery response has a different Worker incarnation");
    CHECK_FAIL_RETURN_STATUS(summary.writable, K_NOT_READY, "Provider UB admission is not writable");
    CHECK_FAIL_RETURN_STATUS(response.probe_performed(), K_NOT_READY,
                             "Provider did not perform the Worker-to-Client UB recovery probe");
    return Status::OK();
}

Status DataPlaneManager::QueryUbPortHealth(const HostPort &workerAddr, const std::string &expectedIncarnation,
                                           int32_t timeoutMs, UbHealthSummary &summary)
{
    summary = UbHealthSummary{};
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(GetOrCreateRpcClient(workerAddr, rpcClient));
    QueryUbPortHealthRspPb response;
    RETURN_IF_NOT_OK(rpcClient->QueryUbPortHealth(expectedIncarnation, timeoutMs, response));
    CHECK_FAIL_RETURN_STATUS(response.has_health_summary(), K_INVALID,
                             "UB port health query response has no summary");
    RETURN_IF_NOT_OK(DecodeUbHealthSummary(response.health_summary(), summary));
    CHECK_FAIL_RETURN_STATUS(summary.worker == workerAddr, K_INVALID,
                             "UB port health query response Worker does not match RPC endpoint");
    CHECK_FAIL_RETURN_STATUS(expectedIncarnation.empty() || summary.incarnation == expectedIncarnation,
                             K_NOT_READY, "UB port health query response has a different Worker incarnation");
    return Status::OK();
}

bool DataPlaneManager::RequestUbPortHealthVerification(const HostPort &workerAddr)
{
    if (!IsClientUbFaultIsolationEnabled() || shutdown_.load(std::memory_order_acquire)) {
        return false;
    }
    auto snapshot = std::atomic_load(&endpointAdmissionSnapshot_);
    if (snapshot == nullptr || snapshot->workerIncarnations == nullptr) {
        return false;
    }
    auto incarnation = snapshot->workerIncarnations->find(workerAddr);
    if (incarnation == snapshot->workerIncarnations->end()) {
        return false;
    }
    const bool scheduled = ubPortHealthVerifier_.RequestVerification(
        workerAddr, incarnation->second,
        static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    if (scheduled && ubHealthWakeHook_) {
        ubHealthWakeHook_();
    }
    return scheduled;
}

void DataPlaneManager::ObserveUbHealthSummary(const UbHealthSummary &summary)
{
    if (!IsClientUbFaultIsolationEnabled() || shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    auto topology = std::atomic_load(&endpointAdmissionSnapshot_);
    if (topology == nullptr || topology->workerIncarnations == nullptr) {
        return;
    }
    auto incarnation = topology->workerIncarnations->find(summary.worker);
    UbHealthSummary accepted;
    if (incarnation == topology->workerIncarnations->end()
        || incarnation->second != summary.incarnation
        || !observedUbHealthSummaries_.Apply(summary, incarnation->second, accepted)) {
        return;
    }
    if (ubHealthSummaryHook_) {
        try {
            ubHealthSummaryHook_(accepted);
        } catch (const std::exception &error) {
            LOG(ERROR) << "Client passive UB health hook threw: " << error.what();
        } catch (...) {
            LOG(ERROR) << "Client passive UB health hook threw";
        }
    }
    if (!accepted.portHealth.has_value()) {
        return;
    }
    const bool hinted = ubPortHealthVerifier_.NotifySummaryHint(
        accepted, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    if (ShouldIsolateForUbPortHealth(*accepted.portHealth)) {
        const bool requested = RequestUbPortHealthVerification(accepted.worker);
        if (hinted && !requested && ubHealthWakeHook_) {
            ubHealthWakeHook_();
        }
        return;
    }
    if (hinted && ubHealthWakeHook_) {
        ubHealthWakeHook_();
    }
}

void DataPlaneManager::RunDueUbPortHealthVerification()
{
    std::lock_guard<bthread::Mutex> lock(lifecycleMutex_);
    if (!IsClientUbFaultIsolationEnabled() || shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    // Bound each dispatch pass even when completions or submission failures are immediate.
    for (size_t dispatched = 0; dispatched < cluster::REMOTE_UB_PORT_HEALTH_MAX_CONCURRENT_QUERIES; ++dispatched) {
        if (ubPortHealthQueriesInFlight_.load(std::memory_order_acquire) >=
            cluster::REMOTE_UB_PORT_HEALTH_MAX_CONCURRENT_QUERIES) {
            break;
        }
        auto ticket = ubPortHealthVerifier_.TryBeginDue(nowMs);
        if (!ticket.has_value()) {
            break;
        }
        ubPortHealthQueriesInFlight_.fetch_add(1, std::memory_order_acq_rel);
        std::shared_ptr<std::atomic<bool>> unclaimed;
        try {
            // Submit may enqueue before throwing: only the task or catch path owns completion.
            unclaimed = std::make_shared<std::atomic<bool>>(true);
            INJECT_POINT_NO_RETURN("DataPlaneManager.DispatchUbPortHealthQuery", [] {
                throw std::runtime_error("Injected query dispatch failure");
            });
            (void)ubPortHealthQueryPool_->Submit([this, ticket = *ticket, unclaimed] {
                if (unclaimed->exchange(false)) {
                    CompleteUbPortHealthVerification(ticket);
                }
            });
        } catch (const std::exception &error) {
            LOG(ERROR) << "Failed to dispatch Client UB port-health query: " << error.what();
            if (unclaimed == nullptr || unclaimed->exchange(false)) {
                ubPortHealthQueriesInFlight_.fetch_sub(1, std::memory_order_acq_rel);
                FailUbPortHealthQueryDispatch(*ticket, error.what());
            }
        } catch (...) {
            LOG(ERROR) << "Failed to dispatch Client UB port-health query";
            if (unclaimed == nullptr || unclaimed->exchange(false)) {
                ubPortHealthQueriesInFlight_.fetch_sub(1, std::memory_order_acq_rel);
                FailUbPortHealthQueryDispatch(*ticket, "Failed to dispatch Client UB port-health query");
            }
        }
    }
}

void DataPlaneManager::FailUbPortHealthQueryDispatch(const cluster::RemoteUbQueryTicket &ticket,
                                                     const std::string &message) noexcept
{
    try {
        auto completion = ubPortHealthVerifier_.Complete(
            ticket, std::nullopt, Status(K_RUNTIME_ERROR, message),
            static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
        if (completion.retryScheduled && ubHealthWakeHook_) {
            ubHealthWakeHook_();
        }
    } catch (const std::exception &error) {
        LOG(ERROR) << "Failed to finish Client UB health query dispatch: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Failed to finish Client UB health query dispatch";
    }
}

void DataPlaneManager::CompleteUbPortHealthVerification(
    const cluster::RemoteUbQueryTicket &ticket) noexcept
{
    try {
        UbHealthSummary summary;
        Status rc;
        try {
            rc = QueryUbPortHealth(ticket.peer, ticket.incarnation,
                                   static_cast<int32_t>(UB_REMOTE_PORT_HEALTH_QUERY_INTERVAL.count()), summary);
        } catch (const std::exception &error) {
            rc = Status(K_RUNTIME_ERROR, error.what());
        } catch (...) {
            rc = Status(K_RUNTIME_ERROR, "Client UB port-health query threw");
        }
        ApplyUbPortHealthCapabilityCheck(ticket, rc);
        std::optional<UbHealthSummary> response;
        if (rc.IsOk()) {
            response = summary;
        }
        auto completion = ubPortHealthVerifier_.Complete(
            ticket, response, rc, static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
        if (completion.evidenceAccepted && response.has_value() && verifiedUbHealthSummaryHook_) {
            verifiedUbHealthSummaryHook_(*response);
        }
    } catch (const std::exception &error) {
        LOG(ERROR) << "Failed to complete Client UB port-health query: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Failed to complete Client UB port-health query";
    }
    ubPortHealthQueriesInFlight_.fetch_sub(1, std::memory_order_acq_rel);
    // Every completion frees a slot, even a healthy result or a retired ticket.
    if (ubHealthWakeHook_) {
        try {
            ubHealthWakeHook_();
        } catch (const std::exception &error) {
            LOG(ERROR) << "Client UB health wake hook threw: " << error.what();
        } catch (...) {
            LOG(ERROR) << "Client UB health wake hook threw";
        }
    }
}

void DataPlaneManager::ApplyUbPortHealthCapabilityCheck(const cluster::RemoteUbQueryTicket &ticket, Status &rc) const
{
    if (!rc.IsError() || ubPortHealthCapabilityCheck_ == nullptr) {
        return;
    }
    try {
        if (!ubPortHealthCapabilityCheck_(ticket.peer)) {
            rc = Status(K_NOT_SUPPORTED, "Worker has not advertised UB port-health query capability");
        }
    } catch (const std::exception &error) {
        LOG(ERROR) << "Client UB port-health capability check threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Client UB port-health capability check threw";
    }
}

std::optional<std::chrono::steady_clock::time_point> DataPlaneManager::GetUbPortHealthQueryDeadline() const
{
    if (ubPortHealthQueriesInFlight_.load(std::memory_order_acquire) >=
        cluster::REMOTE_UB_PORT_HEALTH_MAX_CONCURRENT_QUERIES) {
        return std::nullopt;
    }
    auto deadlineMs = ubPortHealthVerifier_.NextQueryDeadlineMs();
    if (!deadlineMs.has_value()) {
        return std::nullopt;
    }
    const auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    return std::chrono::steady_clock::now()
           + std::chrono::milliseconds(*deadlineMs > nowMs ? *deadlineMs - nowMs : 0);
}

Status DataPlaneManager::EstablishUbProbe(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient)
{
    (void)workerAddr;
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    UrmaHandshakeRspPb response;
    RETURN_IF_NOT_OK(rpcClient->ExchangeUrmaConnectInfo(response));
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        auto &manager = UrmaManager::Instance();
        std::shared_ptr<UrmaConnection> probeOwner;
        RETURN_IF_NOT_OK(
            manager.FinalizeOutboundConnection(response, UrmaManager::ConnectionOwnership::CLIENT_REF, &probeOwner));
        Raii releaseProbe([&] { manager.ReleaseClientConnection(workerAddr.ToString(), probeOwner); });
        return ProbeUbDataPlane(response);
    }
#endif
    RETURN_IF_NOT_OK(FinalizeOutboundConnection(response));
#ifdef USE_URMA
    RETURN_IF_NOT_OK(ProbeUbDataPlane(response));
#endif
    return Status::OK();
}

Status DataPlaneManager::GetOrCreateEntry(const std::string &workerKey,
                                          std::shared_ptr<WorkerTransportEntry> &entry,
                                          bool requireSnapshotAdmission)
{
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    auto admission = std::atomic_load(&endpointAdmissionSnapshot_);
    if (requireSnapshotAdmission && admission != nullptr && !admission->provisional) {
        const auto &live = admission->liveWorkers;
        if (live == nullptr || live->find(workerKey) == live->end()) {
            const bool degraded = AllowDegradedEndpointAdmission(*admission);
            INJECT_POINT_NO_RETURN("DataPlaneManager.GetOrCreateEntry.afterDegradedAdmission");
            if (!degraded || std::atomic_load(&endpointAdmissionSnapshot_) != admission) {
                return Status(K_NOT_READY, "Worker endpoint is absent from latest transport snapshot: " + workerKey);
            }
        }
    }
    EntryMap::const_accessor constAccessor;
    if (entries_.find(constAccessor, workerKey)) {
        entry = constAccessor->second;
        return Status::OK();
    }
    EntryMap::accessor accessor;
    (void)entries_.insert(accessor, workerKey);
    if (accessor->second == nullptr) {
        accessor->second = std::make_shared<WorkerTransportEntry>();
    }
    entry = accessor->second;
    return Status::OK();
}

Status DataPlaneManager::GetOrCreateLocationEntry(const std::string &workerKey, uint64_t topologyVersion,
                                                  std::shared_ptr<WorkerTransportEntry> &entry)
{
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    EntryMap::accessor accessor;
    (void)entries_.insert(accessor, workerKey);
    if (accessor->second == nullptr) {
        accessor->second = std::make_shared<WorkerTransportEntry>();
    }
    accessor->second->locationAdmissionVersion =
        std::max(accessor->second->locationAdmissionVersion, topologyVersion);
    entry = accessor->second;
    return Status::OK();
}

void DataPlaneManager::DetachRejectedLocationEntry(const std::string &workerKey,
                                                   const std::shared_ptr<WorkerTransportEntry> &entry,
                                                   uint64_t topologyVersion)
{
    EntryMap::accessor accessor;
    if (!entries_.find(accessor, workerKey) || accessor->second != entry
        || entry->locationAdmissionVersion > topologyVersion) {
        return;
    }
    entries_.erase(accessor);
}

Status DataPlaneManager::GetOrBuildTransporter(const TransportBuildContext &context,
                                               const std::shared_ptr<WorkerTransportEntry> &entry,
                                               std::shared_ptr<IDataTransporter> &out)
{
    auto *recorder = context.recorder;
    bool cachedFallbackAlongsideShm = false;
    {
        const auto lockBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                   : recorder->StartPhase();
        bthread::RWLockRdGuard lock(entry->mutex);
        if (recorder != nullptr) {
            recorder->RecordPhase("connection_read_lock_wait", lockBegin, TransportLatencyThreshold::PROCESS);
        }
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        CHECK_FAIL_RETURN_STATUS(!(context.respectUbReadRecovery
                                   && context.expectedKind == AccessTransportKind::UB
                                   && entry->UbRebuildCoolingDown(SteadyNowMs())),
                                 K_URMA_NEED_CONNECT, "UB data plane rebuild is cooling down");
        CHECK_FAIL_RETURN_STATUS(!(context.respectUbReadRecovery
                                   && context.expectedKind == AccessTransportKind::UB
                                   && entry->ubRebuildSlotOwner.load(std::memory_order_acquire) != 0),
                                 K_TRY_AGAIN, "UB data plane rebuild is already in flight");
        if (entry->HasAliveTransporter(context.expectedKind)) {
            out = entry->GetTransporter(context.expectedKind);
            MarkDataPlaneUse(entry, out->Kind());
            return Status::OK();
        }
    }
    {
        const auto lockBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                   : recorder->StartPhase();
        bthread::RWLockWrGuard entryLock(entry->mutex);
        if (recorder != nullptr) {
            recorder->RecordPhase("connection_write_lock_wait", lockBegin, TransportLatencyThreshold::PROCESS);
        }
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        CHECK_FAIL_RETURN_STATUS(!(context.respectUbReadRecovery
                                   && context.expectedKind == AccessTransportKind::UB
                                   && entry->UbRebuildCoolingDown(SteadyNowMs())),
                                 K_URMA_NEED_CONNECT, "UB data plane rebuild is cooling down");
        CHECK_FAIL_RETURN_STATUS(!(context.respectUbReadRecovery
                                   && context.expectedKind == AccessTransportKind::UB
                                   && entry->ubRebuildSlotOwner.load(std::memory_order_acquire) != 0),
                                 K_TRY_AGAIN, "UB data plane rebuild is already in flight");
        if (entry->HasAliveTransporter(context.expectedKind)) {
            out = entry->GetTransporter(context.expectedKind);
            MarkDataPlaneUse(entry, out->Kind());
            return Status::OK();
        }
        Status status = EnsureRpcClientLocked(context.workerAddr, entry, context.recorder);
        RETURN_IF_NOT_OK(status);
        status = EnsureTransporterLocked(context, entry, cachedFallbackAlongsideShm);
        RETURN_IF_NOT_OK(status);
        if (shutdown_.load(std::memory_order_acquire)) {
            entry->ResetDataPlaneLocked();
            return Status(K_SHUTTING_DOWN, __LINE__, __FILE__, "DataPlaneManager is shutting down");
        }
        out = entry->GetTransporter(context.expectedKind);
        MarkDataPlaneUse(entry, out->Kind());
    }
    LogTransporterReady(context.workerAddr, out->Kind(), cachedFallbackAlongsideShm);
    return Status::OK();
}

Status DataPlaneManager::EnsureRpcClientLocked(const HostPort &workerAddr,
                                               const std::shared_ptr<WorkerTransportEntry> &entry,
                                               TransportPhaseLatencyRecorder *recorder)
{
    if (entry->rpcClient != nullptr && entry->rpcClient->IsAlive()) {
        return Status::OK();
    }
    entry->ResetDataPlaneLocked();
    std::shared_ptr<WorkerRpcClient> rpcClient;
    const auto phaseBegin = recorder == nullptr ? TransportPhaseLatencyRecorder::TimePoint{}
                                                : recorder->StartPhase();
    Status status = CreateWorkerRpcClient(workerAddr, rpcClient);
    if (recorder != nullptr) {
        recorder->RecordPhase("rpc_client_create", phaseBegin, TransportLatencyThreshold::PROCESS);
    }
    RETURN_IF_NOT_OK(status);
    entry->rpcClient = std::move(rpcClient);
    return Status::OK();
}

Status DataPlaneManager::EnsureTransporterLocked(const TransportBuildContext &context,
                                                 const std::shared_ptr<WorkerTransportEntry> &entry,
                                                 bool &cachedFallbackAlongsideShm)
{
    cachedFallbackAlongsideShm = false;
    if (entry->HasAliveTransporter(context.expectedKind)) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!(entry->shmDraining && context.expectedKind == AccessTransportKind::SHM), K_NOT_READY,
                             WORKER_DRAINING_FOR_SCALE_IN_MESSAGE);
    entry->ResetTransporterLocked(context.expectedKind);
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(
        BuildTransporter(context.workerAddr, context.hint, entry->rpcClient, context.recorder, transporter));
    CHECK_FAIL_RETURN_STATUS(transporter != nullptr, K_RUNTIME_ERROR, "Transporter missing after build");
    auto &slot = entry->GetTransporterSlot(context.expectedKind);
    slot = std::move(transporter);
    if (context.expectedKind != AccessTransportKind::SHM) {
        entry->fallbackKind = slot->Kind();
    }
    cachedFallbackAlongsideShm = context.expectedKind != AccessTransportKind::SHM
                                 && entry->shmTransporter != nullptr;
    return Status::OK();
}

void DataPlaneManager::ResetDataPlane(const HostPort &workerAddr)
{
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        // Same lifecycle boundary as Shutdown() and IsEndpointDataPlaneQuiet(). The drain hook runs on the
        // heartbeat thread and can outlive the request path, so this keeps teardown single-pass: either this
        // reset completes before Shutdown()'s pass, or it observes the shutdown flag and no-ops. Without the
        // boundary a drain could reset an entry after Shutdown() had already finished, i.e. a teardown that
        // escapes the shutdown pass. Low-frequency path only; the read hot path stays lock-free.
        std::lock_guard<bthread::Mutex> lifecycleLock(lifecycleMutex_);
        if (shutdown_.load(std::memory_order_acquire)) {
            return;
        }
        EntryMap::const_accessor accessor;
        if (entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
        }
    }
    // Resetting the entry is safe outside the boundary: the shared_ptr keeps it alive even after Shutdown()
    // drops it from the map, and the entry lock is never held while acquiring lifecycleMutex_ (lock order
    // stays lifecycle -> entry, matching Shutdown()).
    if (entry != nullptr) {
        entry->ResetDataPlane();
    }
}

void DataPlaneManager::ResetTransporter(const HostPort &workerAddr, AccessTransportKind kind)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    std::shared_ptr<WorkerTransportEntry> entry;
    {
        EntryMap::const_accessor accessor;
        if (entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
        }
    }
    if (entry != nullptr) {
        entry->ResetTransporter(kind);
    }
}

void DataPlaneManager::ResetStaleUbDataPlane(const HostPort &workerAddr,
                                             const std::shared_ptr<IDataTransporter> &stale,
                                             bool markCooldown)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        EntryMap::const_accessor accessor;
        if (!entries_.find(accessor, workerAddr.ToString())) {
            return;
        }
        entry = accessor->second;
    }
    if (entry == nullptr) {
        return;
    }
    std::shared_ptr<IDataTransporter> retired;
    {
        bthread::RWLockWrGuard lock(entry->mutex);
        // A concurrent caller may have rebuilt the data plane after this request failed; dropping that
        // instance would undo its recovery, so only drop the one that served the request.
        if (stale != nullptr && entry->GetTransporter(AccessTransportKind::UB) != stale) {
            return;
        }
        if (markCooldown && FLAGS_ub_rebuild_cooldown_ms != 0) {
            entry->ubRebuildAllowedAfterMs.store(SteadyNowMs() + static_cast<int64_t>(FLAGS_ub_rebuild_cooldown_ms),
                                                 std::memory_order_relaxed);
        }
        retired = std::move(entry->GetTransporterSlot(AccessTransportKind::UB));
    }
    if (retired != nullptr) {
        retired->CloseDataPlane();
    }
}

Status DataPlaneManager::RebuildStaleUbDataPlane(const HostPort &workerAddr,
                                                 const std::shared_ptr<IDataTransporter> &stale,
                                                 TransportPhaseLatencyRecorder *recorder)
{
    CHECK_FAIL_RETURN_STATUS(stale != nullptr, K_INVALID, "Stale UB data plane is missing");
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    std::shared_ptr<WorkerTransportEntry> entry;
    const std::string workerKey = workerAddr.ToString();
    RETURN_IF_NOT_OK(GetOrCreateEntry(workerKey, entry));

    std::shared_ptr<WorkerRpcClient> rpcClient;
    int64_t observedCooldown = 0;
    uint64_t slotOwner = 0;
    {
        bthread::RWLockRdGuard lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        observedCooldown = entry->ubRebuildAllowedAfterMs.load(std::memory_order_relaxed);
        CHECK_FAIL_RETURN_STATUS(SteadyNowMs() >= observedCooldown, K_URMA_NEED_CONNECT,
                                 "UB data plane rebuild is cooling down");
        if (entry->GetTransporter(AccessTransportKind::UB) != stale
            && entry->HasAliveTransporter(AccessTransportKind::UB)) {
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS(entry->ubRebuildSlotOwner.load(std::memory_order_acquire) == 0, K_TRY_AGAIN,
                                 "UB data plane rebuild is already in flight");
        slotOwner = nextUbRebuildSlotId_.fetch_add(1, std::memory_order_relaxed);
        uint64_t expected = 0;
        CHECK_FAIL_RETURN_STATUS(entry->ubRebuildSlotOwner.compare_exchange_strong(
                                     expected, slotOwner, std::memory_order_acq_rel, std::memory_order_relaxed),
                                 K_TRY_AGAIN, "UB data plane rebuild is already in flight");
        rpcClient = entry->rpcClient;
    }
    auto releaseSlot = [entry, slotOwner] {
        uint64_t owned = slotOwner;
        (void)entry->ubRebuildSlotOwner.compare_exchange_strong(owned, 0, std::memory_order_release,
                                                                std::memory_order_relaxed);
    };
    Raii releaseSlotOnReturn(releaseSlot);
    auto finishFailedRebuild = [&](const Status &failure,
                                   const std::shared_ptr<WorkerRpcClient> &expectedRpcClient) -> Status {
        std::shared_ptr<IDataTransporter> retired;
        {
            EntryMap::const_accessor accessor;
            CHECK_FAIL_RETURN_STATUS(entries_.find(accessor, workerKey) && accessor->second == entry, K_TRY_AGAIN,
                                     "UB data-plane entry changed during rebuild");
            bthread::RWLockWrGuard lock(entry->mutex);
            CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                     "DataPlaneManager is shutting down");
            if (entry->GetTransporter(AccessTransportKind::UB) != stale
                && entry->HasAliveTransporter(AccessTransportKind::UB)) {
                return Status::OK();
            }
            CHECK_FAIL_RETURN_STATUS(entry->rpcClient == expectedRpcClient, K_TRY_AGAIN,
                                     "RPC client changed during UB data-plane rebuild");
            if (FLAGS_ub_rebuild_cooldown_ms != 0) {
                const int64_t cooldownUntil =
                    SteadyNowMs() + static_cast<int64_t>(FLAGS_ub_rebuild_cooldown_ms);
                auto currentCooldown = entry->ubRebuildAllowedAfterMs.load(std::memory_order_relaxed);
                while (currentCooldown < cooldownUntil
                       && !entry->ubRebuildAllowedAfterMs.compare_exchange_weak(
                           currentCooldown, cooldownUntil, std::memory_order_relaxed)) {
                }
            }
            if (entry->GetTransporter(AccessTransportKind::UB) == stale) {
                retired = std::move(entry->fallbackTransporter);
            }
        }
        releaseSlot();
        if (retired != nullptr) {
            retired->CloseDataPlane();
        }
        return failure;
    };
    if (rpcClient == nullptr) {
        Status rpcRc = GetOrCreateRpcClient(workerAddr, rpcClient);
        if (rpcRc.IsError()) {
            return finishFailedRebuild(rpcRc, nullptr);
        }
        EntryMap::const_accessor accessor;
        CHECK_FAIL_RETURN_STATUS(entries_.find(accessor, workerKey) && accessor->second == entry, K_TRY_AGAIN,
                                 "UB data-plane entry changed while rebuilding RPC client");
        bthread::RWLockRdGuard lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(entry->rpcClient == rpcClient && rpcClient->IsAlive(), K_TRY_AGAIN,
                                 "RPC client changed during UB data-plane rebuild");
    } else if (!rpcClient->IsAlive()) {
        return finishFailedRebuild(Status(K_RPC_UNAVAILABLE, "RPC client is unavailable while rebuilding UB data plane"),
                                   rpcClient);
    }

    std::shared_ptr<IDataTransporter> candidate;
    Status buildRc = BuildTransporter(workerAddr, TransportHint::UB_CANDIDATE, rpcClient, recorder, candidate);
    if (buildRc.IsError() || candidate == nullptr) {
        const Status failure =
            buildRc.IsError() ? buildRc : Status(K_RUNTIME_ERROR, "UB transporter missing after rebuild");
        return finishFailedRebuild(failure, rpcClient);
    }

    std::shared_ptr<IDataTransporter> retired;
    {
        EntryMap::const_accessor accessor;
        CHECK_FAIL_RETURN_STATUS(entries_.find(accessor, workerKey) && accessor->second == entry,
                                 K_TRY_AGAIN, "UB data-plane entry changed during rebuild");
        bthread::RWLockWrGuard lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        if (entry->GetTransporter(AccessTransportKind::UB) != stale
            && entry->HasAliveTransporter(AccessTransportKind::UB)) {
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS(entry->rpcClient == rpcClient && rpcClient->IsAlive(), K_TRY_AGAIN,
                                 "RPC client changed during UB data-plane rebuild");
        retired = std::move(entry->fallbackTransporter);
        entry->fallbackTransporter = std::move(candidate);
        entry->fallbackKind = AccessTransportKind::UB;
        (void)entry->ubRebuildAllowedAfterMs.compare_exchange_strong(observedCooldown, 0,
                                                                     std::memory_order_relaxed);
    }
    releaseSlot();
    if (retired != nullptr) {
        retired->CloseDataPlane();
    }
    return Status::OK();
}

void DataPlaneManager::MarkUbRebuildCooldown(const HostPort &workerAddr)
{
    if (shutdown_.load(std::memory_order_acquire) || FLAGS_ub_rebuild_cooldown_ms == 0) {
        return;
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        EntryMap::accessor accessor;
        (void)entries_.insert(accessor, workerAddr.ToString());
        if (accessor->second == nullptr) {
            accessor->second = std::make_shared<WorkerTransportEntry>();
        }
        entry = accessor->second;
    }
    entry->ubRebuildAllowedAfterMs.store(SteadyNowMs() + static_cast<int64_t>(FLAGS_ub_rebuild_cooldown_ms),
                                         std::memory_order_relaxed);
}

bool DataPlaneManager::IsUbRebuildCoolingDown(const HostPort &workerAddr) const
{
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        EntryMap::const_accessor accessor;
        if (!entries_.find(accessor, workerAddr.ToString())) {
            return false;
        }
        entry = accessor->second;
    }
    if (entry == nullptr) {
        return false;
    }
    return entry->UbRebuildCoolingDown(SteadyNowMs());
}

DataPlaneManager::UbReadAdmission DataPlaneManager::AdmitUbRead(const HostPort &workerAddr, uint64_t &slotOwner,
                                                                bool *degradedByCooldown)
{
    slotOwner = 0;
    if (degradedByCooldown != nullptr) {
        *degradedByCooldown = false;
    }
    if (shutdown_.load(std::memory_order_acquire)) {
        return UbReadAdmission::DEGRADE;
    }
    // This is the metadata-owner read hot path, so it deliberately runs outside lifecycleMutex_ and touches
    // entries_ only through the concurrency-safe find()/insert(). Shutdown() must therefore keep using
    // operations that are safe against them (see the note there) instead of entries_.clear().
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        // Read accessor first: the steady state must not take a write lock on the hash bucket.
        EntryMap::const_accessor constAccessor;
        if (entries_.find(constAccessor, workerAddr.ToString())) {
            entry = constAccessor->second;
        }
    }
    if (entry == nullptr) {
        // No entry yet: create it, because the entry is also the storage for the rebuild slot. Concurrent
        // creators get the same entry object, so the slot below still elects exactly one rebuilder.
        EntryMap::accessor accessor;
        (void)entries_.insert(accessor, workerAddr.ToString());
        if (accessor->second == nullptr) {
            accessor->second = std::make_shared<WorkerTransportEntry>();
        }
        entry = accessor->second;
    }
    if (entry == nullptr) {
        return UbReadAdmission::PROCEED;
    }
    {
        bthread::RWLockRdGuard lock(entry->mutex);
        if (entry->UbRebuildCoolingDown(SteadyNowMs())) {
            if (degradedByCooldown != nullptr) {
                *degradedByCooldown = true;
            }
            return UbReadAdmission::DEGRADE;
        }
        if (entry->ubRebuildSlotOwner.load(std::memory_order_acquire) != 0) {
            return UbReadAdmission::DEGRADE;
        }
        if (entry->HasAliveTransporter(AccessTransportKind::UB)) {
            return UbReadAdmission::PROCEED;
        }
        const uint64_t owner = nextUbRebuildSlotId_.fetch_add(1, std::memory_order_relaxed);
        uint64_t expected = 0;
        if (!entry->ubRebuildSlotOwner.compare_exchange_strong(expected, owner, std::memory_order_acq_rel,
                                                               std::memory_order_relaxed)) {
            return UbReadAdmission::DEGRADE;
        }
        slotOwner = owner;
    }
    return UbReadAdmission::REBUILD;
}

void DataPlaneManager::ReleaseUbRebuildSlot(const HostPort &workerAddr, uint64_t slotOwner)
{
    if (slotOwner == 0) {
        return;
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        EntryMap::const_accessor accessor;
        if (!entries_.find(accessor, workerAddr.ToString())) {
            return;
        }
        entry = accessor->second;
    }
    if (entry == nullptr) {
        return;
    }
    // Clear the slot only while this caller still owns it: the entry may have been dropped and recreated
    // (reconcile or teardown) during the handshake, in which case a new owner must keep its slot.
    uint64_t expected = slotOwner;
    (void)entry->ubRebuildSlotOwner.compare_exchange_strong(expected, 0, std::memory_order_release,
                                                            std::memory_order_relaxed);
}

DataPlaneManager::UbRebuildSlotGuard::UbRebuildSlotGuard(std::shared_ptr<DataPlaneManager> manager, HostPort address,
                                                         uint64_t slotOwner)
    : manager_(std::move(manager)), address_(std::move(address)), slotOwner_(slotOwner)
{
}

DataPlaneManager::UbRebuildSlotGuard::~UbRebuildSlotGuard()
{
    if (slotOwner_ != 0 && manager_ != nullptr) {
        manager_->ReleaseUbRebuildSlot(address_, slotOwner_);
    }
}

bool DataPlaneManager::IsEndpointDataPlaneQuiet(const HostPort &workerAddr, uint64_t quietMs)
{
    if (quietMs == 0) {
        return true;
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    {
        // Share the lifecycle boundary with Shutdown() and ResetDataPlane() so the gate never reports on, or
        // tears down, a manager whose shutdown pass has already completed: weak_ptr::lock() only keeps the
        // manager object alive, it does not stop an explicit Shutdown(). Hold the accessor inside this
        // boundary. Low-frequency drain path only; the read hot path must not take this lock.
        std::lock_guard<bthread::Mutex> lifecycleLock(lifecycleMutex_);
        if (shutdown_.load(std::memory_order_acquire)) {
            return true;
        }
        EntryMap::const_accessor accessor;
        if (!entries_.find(accessor, workerAddr.ToString())) {
            return true;
        }
        entry = accessor->second;
    }
    if (entry == nullptr) {
        return true;
    }
    const int64_t lastUse = entry->lastDataPlaneUseMs.load(std::memory_order_relaxed);
    if (lastUse == 0) {
        return true;
    }
    const int64_t elapsed = SteadyNowMs() - lastUse;
    // Err towards keeping the data plane: an unusable reading (steady_clock is monotonic, so this can only
    // be a corrupt recorded value) counts as "recently used" rather than "quiet", because the gate exists to
    // avoid tearing down a plane that is still in use.
    if (elapsed < 0) {
        return false;
    }
    // MarkDataPlaneUse() refreshes the timestamp at most once per DATA_PLANE_USE_REFRESH_INTERVAL_MS, so a
    // read inside that interval does not move it: the recorded value can lag the true last use by up to one
    // interval. Discount that bound before comparing — by subtraction and after an explicit lower-bound
    // check, so neither side can overflow — otherwise a quietMs below the interval would let the gate report
    // "quiet" for an endpoint that served a read milliseconds ago, and the standby would be torn down while
    // still in use (the exact 1006 this series removes). The gate is a "must not tear down too early"
    // guarantee, so over-estimating the age is the unsafe direction and under-estimating it is safe.
    const auto elapsedMs = static_cast<uint64_t>(elapsed);
    constexpr uint64_t refreshMs = static_cast<uint64_t>(DATA_PLANE_USE_REFRESH_INTERVAL_MS);
    return elapsedMs >= refreshMs && elapsedMs - refreshMs >= quietMs;
}

void DataPlaneManager::MarkDataPlaneUse(const std::shared_ptr<WorkerTransportEntry> &entry, AccessTransportKind kind)
{
    // TCP data planes cannot be invalidated by a worker-side teardown. Callers pass the kind that actually
    // served the request, so a UB candidate that fell back to the cached TCP transporter does not keep the
    // endpoint "in use": otherwise an endpoint stuck on the fallback (URMA persistently degraded) would never
    // go quiet and its standby connection would never be reclaimed.
    if (entry == nullptr || kind == AccessTransportKind::TCP) {
        return;
    }
    // Refresh at most once per DATA_PLANE_USE_REFRESH_INTERVAL_MS. The drain gate only needs to know whether
    // the plane was used inside a multi-second window, while an unconditional store on every read would dirty
    // this cache line (and invalidate it on every other core reading the same entry) on the hot path.
    const int64_t now = SteadyNowMs();
    const int64_t last = entry->lastDataPlaneUseMs.load(std::memory_order_relaxed);
    if (last != 0 && now >= last && now - last < DATA_PLANE_USE_REFRESH_INTERVAL_MS) {
        return;
    }
    entry->lastDataPlaneUseMs.store(now, std::memory_order_relaxed);
}

void DataPlaneManager::MarkShmDraining(const HostPort &workerAddr)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    std::shared_ptr<IDataTransporter> staleShm;
    {
        EntryMap::accessor accessor;
        (void)entries_.insert(accessor, workerAddr.ToString());
        if (accessor->second == nullptr) {
            accessor->second = std::make_shared<WorkerTransportEntry>();
        }
        entry = accessor->second;
        bthread::RWLockWrGuard lock(entry->mutex);
        entry->shmDraining = true;
        staleShm = std::move(entry->shmTransporter);
    }
    if (staleShm != nullptr) {
        staleShm->CloseDataPlane();
    }
}

void DataPlaneManager::Teardown(const HostPort &workerAddr)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<WorkerTransportEntry> entry;
    std::shared_ptr<IDataTransporter> staleTransporter;
    std::shared_ptr<WorkerRpcClient> staleRpcClient;
    bool preserveEntry = false;
    {
        EntryMap::accessor accessor;
        if (entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
            bthread::RWLockWrGuard lock(entry->mutex);
            preserveEntry = entry->shmDraining;
            if (preserveEntry) {
                staleTransporter = std::move(entry->fallbackTransporter);
                staleRpcClient = std::move(entry->rpcClient);
            } else {
                entries_.erase(accessor);
            }
        }
    }
    if (preserveEntry && staleTransporter != nullptr) {
        staleTransporter->CloseDataPlane();
    } else if (entry != nullptr) {
        entry->ResetDataPlane();
    }
    staleRpcClient.reset();
}

bool DataPlaneManager::AllowDegradedEndpointAdmission(const EndpointAdmissionSnapshot &snapshot)
{
    const auto nowMs = SteadyNowMs();
    const auto lastConfirmedMs = snapshot.lastConfirmedMs;
    if (lastConfirmedMs > 0 && nowMs - lastConfirmedMs < SNAPSHOT_REFRESH_GRACE_MS) {
        // Ring is healthy: an absent endpoint was removed by a confirmed ring, keep rejecting.
        return false;
    }
    int64_t expected = 0;
    if (snapshot.degradedDeadlineMs.compare_exchange_strong(expected, nowMs + DEGRADED_ADMISSION_TTL_MS)) {
        LOG(WARNING) << "[TransportGet][Reconcile] Ring refresh is lost, degrade endpoint admission for "
                     << DEGRADED_ADMISSION_TTL_MS << "ms";
        return true;
    }
    return nowMs < expected;
}

Status DataPlaneManager::UpdateWorkerSnapshot(const WorkerSnapshot &snapshot)
{
    auto liveWorkers = BuildLiveWorkerSet(snapshot);
    auto writeProbeWorkers = BuildWriteProbeWorkers(snapshot, liveWorkers);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    // A version regression is legal only for a cross-confirmed epoch reset (rebuilt membership
    // table); see HashRingRefresher lower-version cross confirmation.
    CHECK_FAIL_RETURN_STATUS(!hasWorkerSnapshot_.load(std::memory_order_acquire)
                                 || snapshot.epochResetConfirmed
                                 || snapshot.ringVersion >= workerSnapshotVersion_.load(std::memory_order_acquire),
                             K_INVALID,
                             "Transport worker snapshot version regressed from "
                                 + std::to_string(workerSnapshotVersion_.load()) + " to "
                                 + std::to_string(snapshot.ringVersion));
    auto newLiveWorkers = std::make_shared<const std::unordered_set<std::string>>(std::move(liveWorkers));
    auto workerIncarnations =
        std::make_shared<const std::unordered_map<HostPort, std::string>>(snapshot.workerIncarnations);
    auto endpointAdmission = std::make_shared<const EndpointAdmissionSnapshot>(
        snapshot.ringVersion, newLiveWorkers, snapshot.provisional,
        snapshot.provisional ? 0 : SteadyNowMs(), workerIncarnations);
    std::atomic_store(&endpointAdmissionSnapshot_, std::move(endpointAdmission));
    ubPortHealthVerifier_.ReconcileTopology(snapshot.workerIncarnations);
    std::unordered_set<HostPort> workers;
    workers.reserve(snapshot.workerIncarnations.size());
    for (const auto &[worker, incarnation] : snapshot.workerIncarnations) {
        (void)incarnation;
        workers.emplace(worker);
    }
    observedUbHealthSummaries_.ReconcileWorkers(workers);
    {
        std::lock_guard<bthread::Mutex> lock(probeMutex_);
        writeProbeWorkers_ = std::move(writeProbeWorkers);
        writeProbeWorkerIndices_.clear();
        writeProbeWorkerIndices_.reserve(writeProbeWorkers_.size());
        for (size_t index = 0; index < writeProbeWorkers_.size(); ++index) {
            writeProbeWorkerIndices_.emplace(writeProbeWorkers_[index], index);
        }
    }
    workerSnapshotVersion_.store(snapshot.ringVersion, std::memory_order_release);
    hasWorkerSnapshot_.store(true, std::memory_order_release);
    VLOG(1) << "[TransportGet][Reconcile] Published worker snapshot, version: " << workerSnapshotVersion_.load()
            << ", worker count: " << newLiveWorkers->size() << ", provisional: " << snapshot.provisional;
    return Status::OK();
}

void DataPlaneManager::RecordRoutingRefresh(uint64_t ringVersion)
{
    auto current = std::atomic_load(&endpointAdmissionSnapshot_);
    if (current == nullptr || current->provisional || current->ringVersion != ringVersion
        || shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    auto refreshed = std::make_shared<const EndpointAdmissionSnapshot>(
        current->ringVersion, current->liveWorkers, false, SteadyNowMs(), current->workerIncarnations);
    // A concurrent publication wins; old refresh evidence must not rearm the new generation.
    (void)std::atomic_compare_exchange_strong(&endpointAdmissionSnapshot_, &current, std::move(refreshed));
}

void DataPlaneManager::ReconcileWithSnapshot(const WorkerSnapshot &snapshot)
{
    auto liveWorkers = BuildLiveWorkerSet(snapshot);

    std::vector<std::shared_ptr<WorkerTransportEntry>> goneEntries;
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    if (hasWorkerSnapshot_.load(std::memory_order_acquire)
        && snapshot.ringVersion != workerSnapshotVersion_.load(std::memory_order_acquire)) {
        VLOG(1) << "[TransportGet][Reconcile] Skip superseded worker snapshot, version: "
                << snapshot.ringVersion << ", latest version: " << workerSnapshotVersion_.load();
        return;
    }

    std::vector<std::string> goneWorkers;
    for (auto iter = entries_.begin(); iter != entries_.end(); ++iter) {
        if (liveWorkers.find(iter->first) == liveWorkers.end()) {
            goneWorkers.emplace_back(iter->first);
        }
    }

    goneEntries.reserve(goneWorkers.size());
    for (const auto &worker : goneWorkers) {
        EntryMap::accessor accessor;
        if (entries_.find(accessor, worker)) {
            if (accessor->second != nullptr
                && accessor->second->locationAdmissionVersion > snapshot.ringVersion) {
                continue;
            }
            if (accessor->second != nullptr) {
                goneEntries.emplace_back(accessor->second);
            }
            entries_.erase(accessor);
        }
    }
    VLOG(1) << "[TransportGet][Reconcile] Detached absent worker entries, version: "
            << snapshot.ringVersion << ", removed count: " << goneEntries.size();
    for (auto &entry : goneEntries) {
        entry->ResetDataPlane();
    }
}

void DataPlaneManager::Shutdown()
{
    std::lock_guard<bthread::Mutex> lifecycleLock(lifecycleMutex_);
    if (shutdown_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    ubHealthCallbackState_->Detach();

    // Dispatch shares lifecycleMutex_. The non-dropping four-slot pool drains raw-this tasks before dependencies;
    // each RPC is capped at one second, so shutdown waits for at most the outstanding query wave plus pool join.
    ubPortHealthQueryPool_.reset();

    std::vector<std::shared_ptr<WorkerTransportEntry>> entries;
    entries.reserve(entries_.size());
    for (auto iter = entries_.begin(); iter != entries_.end(); ++iter) {
        if (iter->second != nullptr) {
            entries.emplace_back(iter->second);
        }
    }
    // Deliberately no entries_.clear() here. clear() unlinks and frees every node and then releases whole
    // segments without taking any bucket lock (tbb::concurrent_hash_map even asserts "concurrent or
    // unexpectedly terminated operation during clear() execution"), so it is not concurrency-safe with the
    // find()/insert() that request threads perform on the read hot path outside this mutex. Leaving the
    // (now reset) husks in the map keeps those lookups safe; the nodes are released with the manager, whose
    // destructor cannot run while a request still holds a reference to it.
    //
    // The per-entry reset below stays inside this boundary and must not call back out of the manager: it
    // only drops the data planes that were already handed out, and any future work added here has to remain
    // callable while lifecycleMutex_ is held (no re-entering a public DataPlaneManager method that takes it).
    for (auto &entry : entries) {
        entry->ResetDataPlane();
    }
    shmMaintenancePool_.reset();
}

Status DataPlaneManager::BuildUbTransporter(const HostPort &workerAddr,
                                            const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                            TransportPhaseLatencyRecorder *recorder,
                                            std::shared_ptr<IDataTransporter> &out)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(std::memory_order_acquire), K_NOT_READY,
                             "Call DataPlaneManager::Init before creating UB data-plane transport");

    auto ubConnection = std::make_shared<UbConnection>(rpcClient);
    Status rc = ubConnection->Establish(workerAddr, recorder);
    if (rc.IsOk() && ubConnection->IsAlive()) {
        out = std::make_shared<UbTransporter>(rpcClient, ubConnection, ubBufferProvider_, releasePool_);
        return Status::OK();
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        return rc;
    }
    return Status(K_URMA_CONNECT_FAILED, "UB establish failed: " + rc.GetMsg());
}

Status DataPlaneManager::BuildTransporter(const HostPort &workerAddr, TransportHint hint,
                                          const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                          TransportPhaseLatencyRecorder *recorder,
                                          std::shared_ptr<IDataTransporter> &out)
{
    if (hint == TransportHint::SHM_CANDIDATE) {
        // Same-host routing selects an endpoint-scoped SHM candidate. The initial bound Worker's
        // IsShmEnable state says nothing about this target Worker; ShmConnection probes the target
        // through GetSocketPath and RegisterClient when the first request supplies its auth context.
        CHECK_FAIL_RETURN_STATUS(rpcClient != nullptr && rpcClient->IsAlive(), K_RPC_UNAVAILABLE,
                                 "SHM_CANDIDATE worker RPC client is unavailable");
        RETURN_RUNTIME_ERROR_IF_NULL(hostMemoryPinManager_);
        out = std::make_shared<ShmTransporter>(workerAddr, rpcClient, releasePool_, hostMemoryPinManager_,
                                               shmMaintenancePool_);
        return Status::OK();
    }
    if (hint != TransportHint::TCP_ONLY) {
        return BuildUbTransporter(workerAddr, rpcClient, recorder, out);
    }
    out = std::make_shared<TcpTransporter>(rpcClient);
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
