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

#include "datasystem/client/transport/data_plane/data_plane_manager.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_connection.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {

AccessTransportKind KindForHint(TransportHint hint)
{
    if (hint == TransportHint::SHM_CANDIDATE) {
        return AccessTransportKind::SHM;
    }
    return hint == TransportHint::TCP_ONLY ? AccessTransportKind::TCP : AccessTransportKind::UB;
}

Status InitClientUbRuntime(uint64_t fastTransportMemSize, bool enablePipelineH2D)
{
#ifdef USE_URMA
    static std::once_flag initOnce;
    static Status initStatus;
    SetClientFastTransportMode(FastTransportMode::UB, fastTransportMemSize, enablePipelineH2D);
    std::call_once(initOnce, []() {
        initStatus = InitializeFastTransportManager();
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

#ifdef USE_URMA
Status ProbeUbDataPlane(const UrmaHandshakeRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(response.has_recovery_probe_addr(), K_NOT_SUPPORTED,
                             "Worker handshake has no dedicated URMA WRITE recovery probe address");

    constexpr uint64_t probeSize = 1;
    std::shared_ptr<UrmaManager::BufferHandle> localBuffer;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetMemoryBufferHandle(localBuffer, probeSize));
    RETURN_RUNTIME_ERROR_IF_NULL(localBuffer);
    auto *probeBuffer = static_cast<uint8_t *>(localBuffer->GetPointer());
    RETURN_RUNTIME_ERROR_IF_NULL(probeBuffer);
    *probeBuffer = 0;

    std::vector<uint64_t> eventKeys;
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaWritePayload(
        response.recovery_probe_addr(), localBuffer->GetSegmentAddress(), localBuffer->GetSegmentSize(),
        reinterpret_cast<uint64_t>(probeBuffer), 0, probeSize, 0, INVALID_CHIP_ID, INVALID_CHIP_ID, false,
        eventKeys));

    constexpr auto probeTimeout = std::chrono::milliseconds(500);
    const auto deadline = std::chrono::steady_clock::now() + probeTimeout;
    auto remainingTime = [deadline]() {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        return std::max<int64_t>(0, remaining.count());
    };
    auto preserveError = [](Status &status) { return status; };
    RETURN_IF_NOT_OK(WaitFastTransportEvent(eventKeys, remainingTime, preserveError));
    INJECT_POINT("DataPlaneManager.ProbeUbDataPlane.AfterCompletion");
    return Status::OK();
}
#endif

std::unordered_set<std::string> BuildLiveWorkerSet(const WorkerSnapshot &snapshot)
{
    std::unordered_set<std::string> liveWorkers;
    liveWorkers.reserve(snapshot.sameHostAddrs.size() + snapshot.otherAddrs.size());
    for (const auto &worker : snapshot.sameHostAddrs) {
        liveWorkers.insert(worker.ToString());
    }
    for (const auto &worker : snapshot.otherAddrs) {
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
    return transporter != nullptr && kind == expectedKind && transporter->IsAlive();
}

void DataPlaneManager::WorkerTransportEntry::ResetDataPlaneLocked()
{
    auto staleTransporter = std::move(transporter);
    if (staleTransporter != nullptr) {
        staleTransporter->CloseDataPlane();
    }
}

void DataPlaneManager::WorkerTransportEntry::ResetDataPlane()
{
    bthread::RWLockWrGuard lock(mutex);
    ResetDataPlaneLocked();
}

DataPlaneManager::DataPlaneManager(std::shared_ptr<Signature> signature, uint64_t fastTransportMemSize,
                                   BrpcChannelConfig channelConfig,
                                   std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider,
                                   bool enableClientDirectPipelineH2D, int32_t pipelineThreadNum,
                                   std::shared_ptr<ThreadPool> releasePool)
    : signature_(std::move(signature)), channelConfig_(std::move(channelConfig)),
      ubBufferProvider_(std::move(ubBufferProvider)), fastTransportMemSize_(fastTransportMemSize),
      enableClientDirectPipelineH2D_(enableClientDirectPipelineH2D), pipelineThreadNum_(pipelineThreadNum),
      releasePool_(std::move(releasePool))
{
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
    RETURN_IF_NOT_OK(InitClientUbRuntime(fastTransportMemSize_, enableClientDirectPipelineH2D_));
#ifdef USE_URMA
    if (enableClientDirectPipelineH2D_) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().EnsureClientPipelineH2DEnv());
        RETURN_IF_NOT_OK(UrmaManager::Instance().RegisterClientTransportMemoryForH2D());
        h2dMemoryRegistered_ = true;
    }
#endif
    initialized_.store(true, std::memory_order_release);
    return Status::OK();
}

Status DataPlaneManager::CreateWorkerRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out)
{
    auto rpcClient = std::make_shared<WorkerRpcClient>(workerAddr, signature_, channelConfig_);
    RETURN_IF_NOT_OK(rpcClient->Init());
    out = std::move(rpcClient);
    VLOG(1) << "[TransportGet][Connection] RPC connection ready, endpoint: " << workerAddr.ToString();
    return Status::OK();
}

Status DataPlaneManager::GetOrCreate(const HostPort &workerAddr, TransportHint hint,
                                     std::shared_ptr<IDataTransporter> &out)
{
    out.reset();
    std::shared_ptr<WorkerTransportEntry> entry;
    RETURN_IF_NOT_OK(GetOrCreateEntry(workerAddr.ToString(), entry));
    return GetOrBuildTransporter(workerAddr, hint, KindForHint(hint), entry, out);
}

Status DataPlaneManager::GetOrCreateEndpoint(const HostPort &workerAddr, TransportHint hint,
                                             std::shared_ptr<IDataTransporter> &transporter,
                                             std::shared_ptr<WorkerRpcClient> &rpcClient)
{
    transporter.reset();
    rpcClient.reset();
    const AccessTransportKind expectedKind = KindForHint(hint);
    std::shared_ptr<WorkerTransportEntry> entry;
    RETURN_IF_NOT_OK(GetOrCreateEntry(workerAddr.ToString(), entry));
    RETURN_IF_NOT_OK(GetOrBuildTransporter(workerAddr, hint, expectedKind, entry, transporter));

    bthread::RWLockRdGuard lock(entry->mutex);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(entry->transporter == transporter && entry->HasAliveTransporter(expectedKind),
                             K_URMA_NEED_CONNECT, "Data-plane transporter changed before endpoint acquisition");
    CHECK_FAIL_RETURN_STATUS(entry->rpcClient != nullptr && entry->rpcClient->IsAlive(), K_RPC_UNAVAILABLE,
                             "RPC client is unavailable while acquiring endpoint");
    rpcClient = entry->rpcClient;
    return Status::OK();
}

Status DataPlaneManager::WithDataPlaneLease(
    const HostPort &workerAddr, TransportHint hint,
    const std::function<Status(const std::shared_ptr<IDataTransporter> &,
                               const std::shared_ptr<WorkerRpcClient> &)> &operation)
{
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(operation), K_INVALID, "Data-plane lease operation is empty");
    const AccessTransportKind expectedKind = KindForHint(hint);
    std::shared_ptr<WorkerTransportEntry> entry;
    RETURN_IF_NOT_OK(GetOrCreateEntry(workerAddr.ToString(), entry));
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(GetOrBuildTransporter(workerAddr, hint, expectedKind, entry, transporter));

    bthread::RWLockRdGuard lock(entry->mutex);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(entry->transporter == transporter && entry->HasAliveTransporter(expectedKind),
                             K_URMA_NEED_CONNECT, "Data-plane transporter changed before lease acquisition");
    CHECK_FAIL_RETURN_STATUS(entry->rpcClient != nullptr && entry->rpcClient->IsAlive(), K_RPC_UNAVAILABLE,
                             "RPC client is unavailable while the data-plane lease is held");
    return operation(transporter, entry->rpcClient);
}

Status DataPlaneManager::GetOrCreateRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out)
{
    out.reset();
    std::shared_ptr<WorkerTransportEntry> entry;
    RETURN_IF_NOT_OK(GetOrCreateEntry(workerAddr.ToString(), entry));
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
    RETURN_IF_NOT_OK(EnsureRpcClientLocked(workerAddr, entry));
    out = entry->rpcClient;
    return Status::OK();
}

Status DataPlaneManager::ProbeUbConnection(const HostPort &workerAddr, const std::function<void()> &commitRecovery)
{
    HostPort probeWorker;
    {
        bthread::RWLockWrGuard lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        CHECK_FAIL_RETURN_STATUS(hasWorkerSnapshot_, K_NOT_READY,
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

    bthread::RWLockWrGuard lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(
        hasWorkerSnapshot_ && writeProbeWorkerIndices_.count(probeWorker.ToString()) != 0, K_NOT_FOUND,
        "Worker endpoint is absent from latest writable transport snapshot: " + probeWorker.ToString());
    probePreferredWorker_.clear();
    lastProbeWorker_.clear();
    if (commitRecovery) {
        commitRecovery();
    }
    return Status::OK();
}

Status DataPlaneManager::EstablishUbProbe(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient)
{
    (void)workerAddr;
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    UrmaHandshakeRspPb response;
    RETURN_IF_NOT_OK(rpcClient->ExchangeUrmaConnectInfo(response));
    RETURN_IF_NOT_OK(FinalizeOutboundConnection(response));
#ifdef USE_URMA
    RETURN_IF_NOT_OK(ProbeUbDataPlane(response));
#endif
    return Status::OK();
}

Status DataPlaneManager::GetOrCreateEntry(const std::string &workerKey,
                                          std::shared_ptr<WorkerTransportEntry> &entry)
{
    bthread::RWLockRdGuard lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(!hasWorkerSnapshot_ || liveWorkers_.find(workerKey) != liveWorkers_.end(), K_NOT_READY,
                             "Worker endpoint is absent from latest transport snapshot: " + workerKey);
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

Status DataPlaneManager::GetOrBuildTransporter(const HostPort &workerAddr, TransportHint hint,
                                               AccessTransportKind expectedKind,
                                               const std::shared_ptr<WorkerTransportEntry> &entry,
                                               std::shared_ptr<IDataTransporter> &out)
{
    {
        bthread::RWLockRdGuard lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        if (entry->HasAliveTransporter(expectedKind)) {
            out = entry->transporter;
            return Status::OK();
        }
    }
    bthread::RWLockWrGuard entryLock(entry->mutex);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    if (entry->HasAliveTransporter(expectedKind)) {
        out = entry->transporter;
        return Status::OK();
    }
    RETURN_IF_NOT_OK(EnsureRpcClientLocked(workerAddr, entry));
    RETURN_IF_NOT_OK(EnsureTransporterLocked(workerAddr, hint, expectedKind, entry));
    if (shutdown_.load(std::memory_order_acquire)) {
        entry->ResetDataPlaneLocked();
        return Status(K_SHUTTING_DOWN, "DataPlaneManager is shutting down");
    }
    out = entry->transporter;
    return Status::OK();
}

Status DataPlaneManager::EnsureRpcClientLocked(const HostPort &workerAddr,
                                               const std::shared_ptr<WorkerTransportEntry> &entry)
{
    if (entry->rpcClient != nullptr && entry->rpcClient->IsAlive()) {
        return Status::OK();
    }
    entry->ResetDataPlaneLocked();
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(CreateWorkerRpcClient(workerAddr, rpcClient));
    entry->rpcClient = std::move(rpcClient);
    return Status::OK();
}

Status DataPlaneManager::EnsureTransporterLocked(const HostPort &workerAddr, TransportHint hint,
                                                 AccessTransportKind expectedKind,
                                                 const std::shared_ptr<WorkerTransportEntry> &entry)
{
    if (entry->HasAliveTransporter(expectedKind)) {
        return Status::OK();
    }
    entry->ResetDataPlaneLocked();
    std::shared_ptr<IDataTransporter> transporter;
    RETURN_IF_NOT_OK(BuildTransporter(workerAddr, hint, entry->rpcClient, transporter));
    CHECK_FAIL_RETURN_STATUS(transporter != nullptr, K_RUNTIME_ERROR, "Transporter missing after build");
    entry->kind = transporter->Kind();
    entry->transporter = std::move(transporter);
    VLOG(1) << "[TransportGet][Connection] Data transporter ready, endpoint: " << workerAddr.ToString()
            << ", transport: " << AccessTransportTracker::KindToName(entry->kind);
    return Status::OK();
}

void DataPlaneManager::ResetDataPlane(const HostPort &workerAddr)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    std::shared_ptr<WorkerTransportEntry> entry;
    {
        bthread::RWLockRdGuard lock(mutex_);
        EntryMap::const_accessor accessor;
        if (entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
        }
    }
    if (entry != nullptr) {
        entry->ResetDataPlane();
    }
}

void DataPlaneManager::Teardown(const HostPort &workerAddr)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    std::shared_ptr<WorkerTransportEntry> entry;
    {
        bthread::RWLockRdGuard lock(mutex_);
        EntryMap::accessor accessor;
        if (entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
            entries_.erase(accessor);
        }
    }
    if (entry != nullptr) {
        entry->ResetDataPlane();
    }
}

Status DataPlaneManager::UpdateWorkerSnapshot(const WorkerSnapshot &snapshot)
{
    auto liveWorkers = BuildLiveWorkerSet(snapshot);
    auto writeProbeWorkers = BuildWriteProbeWorkers(snapshot, liveWorkers);
    bthread::RWLockWrGuard lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(!hasWorkerSnapshot_ || snapshot.ringVersion >= workerSnapshotVersion_, K_INVALID,
                             "Transport worker snapshot version regressed from "
                                 + std::to_string(workerSnapshotVersion_) + " to "
                                 + std::to_string(snapshot.ringVersion));
    liveWorkers_ = std::move(liveWorkers);
    writeProbeWorkers_ = std::move(writeProbeWorkers);
    writeProbeWorkerIndices_.clear();
    writeProbeWorkerIndices_.reserve(writeProbeWorkers_.size());
    for (size_t index = 0; index < writeProbeWorkers_.size(); ++index) {
        writeProbeWorkerIndices_.emplace(writeProbeWorkers_[index], index);
    }
    workerSnapshotVersion_ = snapshot.ringVersion;
    hasWorkerSnapshot_ = true;
    VLOG(1) << "[TransportGet][Reconcile] Published worker snapshot, version: " << workerSnapshotVersion_
            << ", worker count: " << liveWorkers_.size();
    return Status::OK();
}

void DataPlaneManager::ReconcileWithSnapshot(const WorkerSnapshot &snapshot)
{
    auto liveWorkers = BuildLiveWorkerSet(snapshot);

    std::vector<std::shared_ptr<WorkerTransportEntry>> goneEntries;
    {
        bthread::RWLockWrGuard lock(mutex_);
        if (shutdown_.load(std::memory_order_acquire)) {
            return;
        }
        if (hasWorkerSnapshot_ && snapshot.ringVersion != workerSnapshotVersion_) {
            VLOG(1) << "[TransportGet][Reconcile] Skip superseded worker snapshot, version: "
                    << snapshot.ringVersion << ", latest version: " << workerSnapshotVersion_;
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
                if (accessor->second != nullptr) {
                    goneEntries.emplace_back(accessor->second);
                }
                entries_.erase(accessor);
            }
        }
        VLOG(1) << "[TransportGet][Reconcile] Detached absent worker entries, version: "
                << snapshot.ringVersion << ", removed count: " << goneEntries.size();
    }
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

    std::vector<std::shared_ptr<WorkerTransportEntry>> entries;
    {
        bthread::RWLockWrGuard lock(mutex_);
        entries.reserve(entries_.size());
        for (auto iter = entries_.begin(); iter != entries_.end(); ++iter) {
            if (iter->second != nullptr) {
                entries.emplace_back(iter->second);
            }
        }
        entries_.clear();
    }
    for (auto &entry : entries) {
        entry->ResetDataPlane();
    }
#ifdef USE_URMA
    if (h2dMemoryRegistered_) {
        UrmaManager::Instance().UnregisterClientTransportMemoryForH2D();
        h2dMemoryRegistered_ = false;
    }
#endif
}

Status DataPlaneManager::BuildUbTransporter(const HostPort &workerAddr,
                                            const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                            std::shared_ptr<IDataTransporter> &out)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(std::memory_order_acquire), K_NOT_READY,
                             "Call DataPlaneManager::Init before creating UB data-plane transport");

    auto ubConnection = std::make_shared<UbConnection>(rpcClient);
    Status rc = ubConnection->Establish(workerAddr);
    if (rc.IsOk() && ubConnection->IsAlive()) {
        out = std::make_shared<UbTransporter>(rpcClient, ubConnection, ubBufferProvider_);
        return Status::OK();
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        return rc;
    }
    return Status(K_URMA_CONNECT_FAILED, "UB establish failed: " + rc.GetMsg());
}

Status DataPlaneManager::BuildTransporter(const HostPort &workerAddr, TransportHint hint,
                                          const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                          std::shared_ptr<IDataTransporter> &out)
{
    if (hint == TransportHint::SHM_CANDIDATE) {
        // Same-host routing selects an endpoint-scoped SHM candidate. The initial bound Worker's
        // IsShmEnable state says nothing about this target Worker; ShmConnection probes the target
        // through GetSocketPath and RegisterClient when the first request supplies its auth context.
        CHECK_FAIL_RETURN_STATUS(rpcClient != nullptr && rpcClient->IsAlive(), K_RPC_UNAVAILABLE,
                                 "SHM_CANDIDATE worker RPC client is unavailable");
        out = std::make_shared<ShmTransporter>(workerAddr, rpcClient, releasePool_);
        return Status::OK();
    }
    if (hint != TransportHint::TCP_ONLY) {
        return BuildUbTransporter(workerAddr, rpcClient, out);
    }
    out = std::make_shared<TcpTransporter>(rpcClient);
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
