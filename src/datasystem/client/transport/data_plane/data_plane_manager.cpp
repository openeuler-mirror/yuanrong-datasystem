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

#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_connection.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
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

Status InitClientUbRuntime(uint64_t fastTransportMemSize)
{
#ifdef USE_URMA
    static std::once_flag initOnce;
    static Status initStatus;
    std::call_once(initOnce, [fastTransportMemSize]() {
        SetClientFastTransportMode(FastTransportMode::UB, fastTransportMemSize);
        initStatus = InitializeFastTransportManager();
        if (initStatus.IsError()) {
            initStatus.AppendMsg("Fast transport init failed");
        }
    });
    return initStatus;
#else
    (void)fastTransportMemSize;
    return Status::OK();
#endif
}

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
    std::unique_lock<std::shared_mutex> lock(mutex);
    ResetDataPlaneLocked();
}

DataPlaneManager::DataPlaneManager(std::shared_ptr<Signature> signature, uint64_t fastTransportMemSize,
                                   BrpcChannelConfig channelConfig,
                                   std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider)
    : signature_(std::move(signature)), channelConfig_(std::move(channelConfig)),
      ubBufferProvider_(std::move(ubBufferProvider)),
      fastTransportMemSize_(fastTransportMemSize)
{
}

DataPlaneManager::~DataPlaneManager()
{
    Shutdown();
}

Status DataPlaneManager::Init()
{
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");

    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    if (initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(InitClientUbRuntime(fastTransportMemSize_));
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
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

    std::shared_lock<std::shared_mutex> lock(entry->mutex);
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
        std::shared_lock<std::shared_mutex> lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        if (entry->rpcClient != nullptr && entry->rpcClient->IsAlive()) {
            out = entry->rpcClient;
            return Status::OK();
        }
    }
    std::unique_lock<std::shared_mutex> lock(entry->mutex);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    RETURN_IF_NOT_OK(EnsureRpcClientLocked(workerAddr, entry));
    out = entry->rpcClient;
    return Status::OK();
}

Status DataPlaneManager::GetOrCreateEntry(const std::string &workerKey,
                                          std::shared_ptr<WorkerTransportEntry> &entry)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
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
        std::shared_lock<std::shared_mutex> lock(entry->mutex);
        CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        if (entry->HasAliveTransporter(expectedKind)) {
            out = entry->transporter;
            return Status::OK();
        }
    }
    std::unique_lock<std::shared_mutex> entryLock(entry->mutex);
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
            << ", transport kind: " << static_cast<int>(entry->kind);
    return Status::OK();
}

void DataPlaneManager::ResetDataPlane(const HostPort &workerAddr)
{
    if (shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    std::shared_ptr<WorkerTransportEntry> entry;
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
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
        std::shared_lock<std::shared_mutex> lock(mutex_);
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    CHECK_FAIL_RETURN_STATUS(!hasWorkerSnapshot_ || snapshot.ringVersion >= workerSnapshotVersion_, K_INVALID,
                             "Transport worker snapshot version regressed from "
                                 + std::to_string(workerSnapshotVersion_) + " to "
                                 + std::to_string(snapshot.ringVersion));
    liveWorkers_ = std::move(liveWorkers);
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
        std::unique_lock<std::shared_mutex> lock(mutex_);
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
    if (shutdown_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    std::vector<std::shared_ptr<WorkerTransportEntry>> entries;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
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
        // Defensive: if the same-host worker's RPC client is not alive here (normally guaranteed
        // alive by EnsureRpcClientLocked just before, but a race can tear it down), prefer building a
        // TcpTransporter over a ShmTransporter. Both wrap the same dead rpcClient and will return
        // K_RPC_UNAVAILABLE, but the TCP path returns the standard RPC error and lets the caller's
        // rebuild/Teardown path re-establish the channel, instead of attempting shm fd-passing
        // setup against a dead worker. This does not itself heal the connection.
        if (rpcClient == nullptr || !rpcClient->IsAlive()) {
            LOG(WARNING) << "SHM_CANDIDATE worker " << workerAddr.ToString()
                         << " has no alive RPC client; building TCP transporter so the caller rebuilds.";
            out = std::make_shared<TcpTransporter>(rpcClient);
            return Status::OK();
        }
        out = std::make_shared<ShmTransporter>(rpcClient, workerApi_, mmapManager_);
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
