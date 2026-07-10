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

#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/client/transport/data_plane/shm_connection.h"
#include "datasystem/client/transport/data_plane/tcp_transporter.h"
#include "datasystem/client/transport/data_plane/ub_connection.h"
#include "datasystem/client/transport/data_plane/ub_transporter.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {

AccessTransportKind KindForHint(TransportHint hint)
{
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

}  // namespace

struct DataPlaneManager::WorkerTransportEntry {
    bool HasAliveTransporter(AccessTransportKind expectedKind) const
    {
        return transporter != nullptr && kind == expectedKind && transporter->IsAlive();
    }

    void ResetDataPlaneLocked()
    {
        auto staleTransporter = std::move(transporter);
        if (staleTransporter != nullptr) {
            staleTransporter->CloseDataPlane();
        }
    }

    void ResetDataPlane()
    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        ResetDataPlaneLocked();
    }

    std::shared_mutex mutex;
    std::shared_ptr<WorkerRpcClient> rpcClient;
    std::shared_ptr<IDataTransporter> transporter;
    AccessTransportKind kind = AccessTransportKind::TCP;
};

class DataPlaneManager::Impl {
public:
    using EntryMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<WorkerTransportEntry>>;

    ~Impl()
    {
        Shutdown();
    }

    void Shutdown()
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

    EntryMap entries_;
    std::shared_mutex mutex_;
    std::atomic<bool> shutdown_{ false };
    std::shared_ptr<ClientRequestAuth> auth_;
    BrpcChannelConfig channelConfig_;
    uint64_t fastTransportMemSize_ = DataPlaneManager::DEFAULT_FAST_TRANSPORT_MEM_SIZE;
    std::atomic<bool> initialized_{ false };
};

DataPlaneManager::DataPlaneManager(std::shared_ptr<ClientRequestAuth> auth, uint64_t fastTransportMemSize,
                                   BrpcChannelConfig channelConfig)
    : impl_(new Impl())
{
    impl_->auth_ = std::move(auth);
    impl_->fastTransportMemSize_ = fastTransportMemSize;
    impl_->channelConfig_ = std::move(channelConfig);
}

DataPlaneManager::~DataPlaneManager()
{
    Shutdown();
}

Status DataPlaneManager::Init()
{
    CHECK_FAIL_RETURN_STATUS(!impl_->shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");

    RETURN_RUNTIME_ERROR_IF_NULL(impl_->auth_);
    if (impl_->initialized_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(InitClientUbRuntime(impl_->fastTransportMemSize_));
    CHECK_FAIL_RETURN_STATUS(!impl_->shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    impl_->initialized_.store(true, std::memory_order_release);
    return Status::OK();
}

Status DataPlaneManager::CreateWorkerRpcClient(const HostPort &workerAddr, std::shared_ptr<WorkerRpcClient> &out)
{
    auto rpcClient = std::make_shared<WorkerRpcClient>(workerAddr, impl_->auth_, impl_->channelConfig_);
    RETURN_IF_NOT_OK(rpcClient->Init());
    out = std::move(rpcClient);
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

Status DataPlaneManager::GetOrCreateEntry(const std::string &workerKey,
                                          std::shared_ptr<WorkerTransportEntry> &entry)
{
    std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
    CHECK_FAIL_RETURN_STATUS(!impl_->shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    {
        Impl::EntryMap::const_accessor accessor;
        if (impl_->entries_.find(accessor, workerKey)) {
            entry = accessor->second;
        }
    }
    if (entry != nullptr) {
        return Status::OK();
    }
    Impl::EntryMap::accessor accessor;
    (void)impl_->entries_.insert(accessor, workerKey);
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
        CHECK_FAIL_RETURN_STATUS(!impl_->shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                                 "DataPlaneManager is shutting down");
        if (entry->HasAliveTransporter(expectedKind)) {
            out = entry->transporter;
            return Status::OK();
        }
    }
    std::unique_lock<std::shared_mutex> entryLock(entry->mutex);
    CHECK_FAIL_RETURN_STATUS(!impl_->shutdown_.load(std::memory_order_acquire), K_SHUTTING_DOWN,
                             "DataPlaneManager is shutting down");
    if (entry->HasAliveTransporter(expectedKind)) {
        out = entry->transporter;
        return Status::OK();
    }
    RETURN_IF_NOT_OK(EnsureRpcClientLocked(workerAddr, entry));
    RETURN_IF_NOT_OK(EnsureTransporterLocked(workerAddr, hint, expectedKind, entry));
    if (impl_->shutdown_.load(std::memory_order_acquire)) {
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
    return Status::OK();
}

void DataPlaneManager::ResetDataPlane(const HostPort &workerAddr)
{
    if (impl_->shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    std::shared_ptr<WorkerTransportEntry> entry;
    {
        std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
        Impl::EntryMap::const_accessor accessor;
        if (impl_->entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
        }
    }
    if (entry != nullptr) {
        entry->ResetDataPlane();
    }
}

void DataPlaneManager::Teardown(const HostPort &workerAddr)
{
    if (impl_->shutdown_.load(std::memory_order_acquire)) {
        return;
    }

    std::shared_ptr<WorkerTransportEntry> entry;
    {
        std::shared_lock<std::shared_mutex> lock(impl_->mutex_);
        Impl::EntryMap::accessor accessor;
        if (impl_->entries_.find(accessor, workerAddr.ToString())) {
            entry = accessor->second;
            impl_->entries_.erase(accessor);
        }
    }
    if (entry != nullptr) {
        entry->ResetDataPlane();
    }
}

void DataPlaneManager::ReconcileWithSnapshot(const WorkerSnapshot &snapshot)
{
    std::unordered_set<std::string> liveWorkers;
    for (const auto &worker : snapshot.sameHostAddrs) {
        liveWorkers.insert(worker.ToString());
    }
    for (const auto &worker : snapshot.otherAddrs) {
        liveWorkers.insert(worker.ToString());
    }

    std::vector<std::shared_ptr<WorkerTransportEntry>> goneEntries;
    {
        std::unique_lock<std::shared_mutex> lock(impl_->mutex_);
        if (impl_->shutdown_.load(std::memory_order_acquire)) {
            return;
        }

        std::vector<std::string> goneWorkers;
        for (auto iter = impl_->entries_.begin(); iter != impl_->entries_.end(); ++iter) {
            if (liveWorkers.find(iter->first) == liveWorkers.end()) {
                goneWorkers.push_back(iter->first);
            }
        }

        goneEntries.reserve(goneWorkers.size());
        for (const auto &worker : goneWorkers) {
            Impl::EntryMap::accessor accessor;
            if (impl_->entries_.find(accessor, worker)) {
                if (accessor->second != nullptr) {
                    goneEntries.emplace_back(accessor->second);
                }
                impl_->entries_.erase(accessor);
            }
        }
    }
    for (auto &entry : goneEntries) {
        entry->ResetDataPlane();
    }
}

void DataPlaneManager::Shutdown()
{
    impl_->Shutdown();
}

Status DataPlaneManager::BuildUbTransporter(const HostPort &workerAddr,
                                            const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                            const std::string &errorPrefix, std::shared_ptr<IDataTransporter> &out)
{
    CHECK_FAIL_RETURN_STATUS(impl_->initialized_.load(std::memory_order_acquire), K_NOT_READY,
                             "Call DataPlaneManager::Init before creating UB data-plane transport");

    auto ubConnection = std::make_shared<UbConnection>(rpcClient);
    Status rc = ubConnection->Establish(workerAddr);
    if (rc.IsOk() && ubConnection->IsAlive()) {
        out = std::make_shared<UbTransporter>(rpcClient, ubConnection);
        return Status::OK();
    }
    if (rc.GetCode() == K_NOT_SUPPORTED) {
        return rc;
    }
    return Status(K_URMA_CONNECT_FAILED, errorPrefix + rc.GetMsg());
}

Status DataPlaneManager::BuildTransporter(const HostPort &workerAddr, TransportHint hint,
                                          const std::shared_ptr<WorkerRpcClient> &rpcClient,
                                          std::shared_ptr<IDataTransporter> &out)
{
    if (hint == TransportHint::SHM_CANDIDATE) {
        // Reserve the SHM establishment entry for the SHM transporter; data transfer currently degrades to UB.
        auto shmConnection = std::make_shared<ShmConnection>();
        (void)shmConnection->Establish(workerAddr);
        return BuildUbTransporter(workerAddr, rpcClient, "UB establish failed after SHM degrade: ", out);
    }
    if (hint == TransportHint::UB_CANDIDATE) {
        return BuildUbTransporter(workerAddr, rpcClient, "UB establish failed: ", out);
    }
    out = std::make_shared<TcpTransporter>(rpcClient);
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
