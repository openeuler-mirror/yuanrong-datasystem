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

/**
 * Description: Defines the worker failover split of the object client.
 */
#include "datasystem/client/object_cache/worker_failover.h"

#include <algorithm>
#include <random>
#include <bthread/mutex.h>

#include "datasystem/client/object_cache/client_memory_ref_table.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/random_data.h"

namespace datasystem {
namespace object_cache {

constexpr size_t MIN_SHUFFLE_CANDIDATE_COUNT = 2;

void ShuffleWorkerCandidates(std::vector<HostPort> &candidates)
{
    if (candidates.size() < MIN_SHUFFLE_CANDIDATE_COUNT) {
        return;
    }
    std::mt19937 generator(static_cast<uint32_t>(RandomData::GetRandomSeed()));
    std::shuffle(candidates.begin(), candidates.end(), generator);
}

namespace {

void NotifySwitchToExpectedWorker(const HostPort &target)
{
    const std::string targetAddress = target.ToString();
    INJECT_POINT_NO_RETURN("client.switch_worker_expected_1", [&targetAddress](const std::string &expectedAddress) {
        if (targetAddress == expectedAddress) {
            INJECT_POINT_NO_RETURN("client.switch_worker_expected_1.matched", []() { return true; });
        }
        return true;
    });
    INJECT_POINT_NO_RETURN("client.switch_worker_expected_2", [&targetAddress](const std::string &expectedAddress) {
        if (targetAddress == expectedAddress) {
            INJECT_POINT_NO_RETURN("client.switch_worker_expected_2.matched", []() { return true; });
        }
        return true;
    });
}

}  // namespace

WorkerFailover::WorkerFailover(ObjectClientImpl &owner) : owner_(owner) {}

void WorkerFailover::ConfigureUrmaDataPlaneFailureCallback(WorkerNode node,
                                                           const std::shared_ptr<IClientWorkerApi> &workerApi)
{
    if (workerApi == nullptr) {
        return;
    }
    if (owner_.ubHealthFilter_ == nullptr) {
        owner_.ubHealthFilter_ = std::make_shared<client::UbHealthFilter>();
    }
    std::weak_ptr<client::UbHealthFilter> weakUbHealthFilter(owner_.ubHealthFilter_);
    workerApi->SetUbHealthSummaryCallback([weakUbHealthFilter](const UbHealthSummary &summary) {
        auto filter = weakUbHealthFilter.lock();
        if (filter != nullptr) {
            (void)filter->ApplySummary(summary, summary.incarnation);
        }
    });
    if (!owner_.enableCrossNodeConnection_) {
        return;
    }
    std::weak_ptr<client::IClientWorkerCommonApi> weakWorkerApi(workerApi);
    workerApi->SetUrmaDataPlaneFailureCallback([this, node, weakWorkerApi]() {
        return SubmitUrmaDataPlaneSwitch(node, weakWorkerApi);
    });
}

bool WorkerFailover::SubmitUrmaDataPlaneSwitch(WorkerNode node,
                                               std::weak_ptr<client::IClientWorkerCommonApi> weakWorkerApi)
{
    auto traceId = Trace::Instance().GetTraceID();
    auto task = [this, node, weakWorkerApi, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        auto workerApi = weakWorkerApi.lock();
        if (workerApi == nullptr) {
            return;
        }
        if (!IsCurrentWorkerSwitchTrigger(node, workerApi)) {
            LOG(INFO) << "[Switch] Ignore stale URMA data-plane failure callback, client id: " << workerApi->clientId_
                      << ", worker address: " << workerApi->hostPort_.ToString()
                      << ", source node: " << static_cast<int>(node);
            workerApi->FinishUrmaDataPlaneSwitchAttempt(false);
            return;
        }
        LOG(INFO) << "[Switch] URMA data-plane failure triggers worker switch, client id: " << workerApi->clientId_
                  << ", worker address: " << workerApi->hostPort_.ToString();
        bool switched = SwitchWorkerNode(node, client::SwitchTriggerReason::URMA_DATA_PLANE_FAILURE);
        if (switched) {
            LOG(INFO) << "[Switch] URMA data-plane failure worker switch finished successfully, client id: "
                      << workerApi->clientId_ << ", source worker address: " << workerApi->hostPort_.ToString()
                      << ", source node: " << static_cast<int>(node);
        } else {
            LOG(ERROR) << "[Switch] URMA data-plane failure worker switch failed, client id: " << workerApi->clientId_
                       << ", source worker address: " << workerApi->hostPort_.ToString()
                       << ", source node: " << static_cast<int>(node);
        }
        workerApi->FinishUrmaDataPlaneSwitchAttempt(switched);
    };
    try {
        std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
        if (owner_.asyncSwitchWorkerPool_ == nullptr) {
            return false;
        }
        owner_.asyncSwitchWorkerPool_->Execute(std::move(task));
    } catch (const std::exception &error) {
        LOG(ERROR) << "[Switch] Failed to submit URMA data-plane worker switch: " << error.what();
        return false;
    } catch (...) {
        LOG(ERROR) << "[Switch] Failed to submit URMA data-plane worker switch: unknown exception";
        return false;
    }
    return true;
}

bool WorkerFailover::SubmitUnavailableWorkerSwitch(const std::shared_ptr<IClientWorkerApi> &workerApi)
{
    if (!owner_.enableLocalCache_ || !owner_.enableCrossNodeConnection_ || workerApi == nullptr) {
        return false;
    }
    WorkerNode node;
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        node = owner_.currentNode_;
        if (owner_.workerApi_[node] == nullptr || owner_.workerApi_[node].get() != workerApi.get()) {
            return false;
        }
    }
    try {
        std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
        if (owner_.asyncSwitchWorkerPool_ == nullptr) {
            return false;
        }
        if (!owner_.unavailableWorkerSwitchPending_.emplace(workerApi.get()).second) {
            return false;
        }
        auto traceId = Trace::Instance().GetTraceID();
        owner_.asyncSwitchWorkerPool_->Execute([this, node, workerApi, traceId]() {
            Raii clearPending([this, worker = workerApi.get()]() {
                std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
                owner_.unavailableWorkerSwitchPending_.erase(worker);
            });
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            if (!IsCurrentWorkerSwitchTrigger(node, workerApi)) {
                return;
            }
            LOG(WARNING) << "[Switch] Bound worker unavailable, trigger worker switch, client id: "
                         << workerApi->clientId_ << ", worker address: " << workerApi->hostPort_.ToString();
            (void)SwitchWorkerNode(node, client::SwitchTriggerReason::WORKER_UNAVAILABLE);
        });
    } catch (const std::exception &error) {
        std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
        owner_.unavailableWorkerSwitchPending_.erase(workerApi.get());
        LOG(ERROR) << "[Switch] Failed to submit unavailable worker switch: " << error.what();
        return false;
    } catch (...) {
        std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
        owner_.unavailableWorkerSwitchPending_.erase(workerApi.get());
        LOG(ERROR) << "[Switch] Failed to submit unavailable worker switch: unknown exception";
        return false;
    }
    return true;
}

void WorkerFailover::DrainAsyncSwitchWorkerPool()
{
    std::shared_ptr<ThreadPool> asyncSwitchWorkerPool;
    {
        std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
        asyncSwitchWorkerPool = std::move(owner_.asyncSwitchWorkerPool_);
    }
    asyncSwitchWorkerPool.reset();
    owner_.asyncSwitchWorkerPoolHandle_ = nullptr;
    std::lock_guard<std::mutex> lock(owner_.asyncSwitchWorkerMutex_);
    owner_.unavailableWorkerSwitchPending_.clear();
}

bool WorkerFailover::IsCurrentWorkerSwitchTrigger(WorkerNode node,
                                                  const std::shared_ptr<client::IClientWorkerCommonApi> &workerApi)
{
    std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
    return owner_.currentNode_ == node && owner_.workerApi_[node] != nullptr
           && owner_.workerApi_[node].get() == workerApi.get();
}

Status WorkerFailover::ProcessWorkerLost(client::WorkerRecoveryReason reason)
{
    if (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return Status::OK();
    }
    auto &recovery = *owner_.shmRecoveryState_;
    std::lock_guard<bthread::Mutex> recoveryLock(recovery.mutex);

    if (reason != client::WorkerRecoveryReason::RETRY_PENDING) {
        recovery.stage = ShmRecoveryState::Stage::CLEANUP_REQUIRED;
    }
    if (recovery.stage == ShmRecoveryState::Stage::IDLE) {
        recovery.stage = ShmRecoveryState::Stage::CLEANUP_REQUIRED;
    }

    if (recovery.stage == ShmRecoveryState::Stage::CLEANUP_REQUIRED) {
        CleanupWorkerShmAfterWorkerLost();
        recovery.stage = ShmRecoveryState::Stage::REGISTER_REQUIRED;
    }

    if (recovery.stage == ShmRecoveryState::Stage::REGISTER_REQUIRED) {
        RETURN_IF_NOT_OK(RegisterWorkerAfterWorkerLost(reason));
        recovery.stage = ShmRecoveryState::Stage::REBUILD_REQUIRED;
    }

    RETURN_IF_NOT_OK(RebuildWorkerShm());
    recovery.stage = ShmRecoveryState::Stage::IDLE;
    if (reason == client::WorkerRecoveryReason::CONNECTION_BROKEN) {
        owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER]->SetWorkerAvailable(true);
    }
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (owner_.currentNode_ == ObjectClientImpl::LOCAL_WORKER) {
            MarkWorkerAvailableLocked();
        }
    }
    LOG(INFO) << "[Reconnect] Reconnect to local worker success.";
    INJECT_POINT("ObjectClientImpl.ProcessWorkerLost", []() { return Status::OK(); });
    return Status::OK();
}

Status WorkerFailover::RegisterWorkerAfterWorkerLost(client::WorkerRecoveryReason reason)
{
    if (reason == client::WorkerRecoveryReason::RETRY_PENDING) {
        VLOG(1) << "[Reconnect] Retry reconnect to " << owner_.ipAddress_.ToString();
    } else {
        LOG(INFO) << "[Reconnect] Clear meta and try reconnect to " << owner_.ipAddress_.ToString();
    }
    std::vector<std::string> ids;
    {
        std::lock_guard<std::shared_timed_mutex> l(owner_.globalRefMutex_);
        ids.reserve(owner_.globalRefCount_.size());
        for (const auto &entry : owner_.globalRefCount_) {
            ids.emplace_back(entry.first);
        }
    }
    auto &workerApi = owner_.workerApi_[ObjectClientImpl::LOCAL_WORKER];
    Status rc = workerApi->ReconnectWorker(ids);
    if (rc.IsError()) {
        constexpr int logInterval = 10;
        LOG_EVERY_T(ERROR, logInterval)
            << "[Reconnect] Reconnect local worker failed, error message: " << rc.ToString();
        return rc;
    }
    owner_.memoryRefCount_.SetSupportMultiShmRefCount(workerApi->workerSupportMultiShmRefCount_);
    return Status::OK();
}

Status WorkerFailover::RebuildWorkerShm()
{
    auto &workerApi = owner_.workerApi_[ObjectClientImpl::LOCAL_WORKER];
    (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
    workerApi->CleanUpForPipelineRH2DQueueAfterWorkerLost();
    owner_.mmapManager_->CleanInvalidMmapTable();

    auto rc = workerApi->PrepareForDecreaseShmRef(std::bind(
        &client::MmapManager::LookupUnitsAndMmapFd, owner_.mmapManager_.get(), std::placeholders::_1,
            std::placeholders::_2));
    if (rc.IsError()) {
        constexpr int logInterval = 10;
        LOG_EVERY_T(ERROR, logInterval) << "[Reconnect] Failed to prepair for DecreaseShmRef:" << rc.ToString();
        return rc;
    }
    rc = workerApi->InitPipelineRH2DQueue([this](std::shared_ptr<ShmUnitInfo> &shmUnitInfo) {
        return owner_.mmapManager_->LookupUnitsAndMmapFd("", shmUnitInfo);
    });
    if (rc.IsError()) {
        constexpr int logInterval = 10;
        LOG_EVERY_T(ERROR, logInterval) << PIPLN_LOG_PREFIX "Reconnect: InitQueue failed: " << rc.ToString();
        (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
        workerApi->CleanUpForPipelineRH2DQueueAfterWorkerLost();
        owner_.mmapManager_->CleanInvalidMmapTable();
        return rc;
    }
    return Status::OK();
}

void WorkerFailover::CleanupWorkerShmAfterWorkerLost()
{
    auto &workerApi = owner_.workerApi_[ObjectClientImpl::LOCAL_WORKER];
    (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
    (void)workerApi->CleanUpForPipelineRH2DQueueAfterWorkerLost();
    owner_.mmapManager_->CleanInvalidMmapTable();
    // Only shm object would record reference count, and they are
    // unrecoverable after timeout until worker reconnects, so clear them directly.
    owner_.memoryRefCount_.Clear();
}

void WorkerFailover::ProcessWorkerTimeout()
{
    if (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return;
    }
    auto &recovery = *owner_.shmRecoveryState_;
    std::lock_guard<bthread::Mutex> recoveryLock(recovery.mutex);
    CleanupWorkerShmAfterWorkerLost();
    // If the same worker recovers, its registration is still valid and only the local SHM resources need rebuilding.
    recovery.stage = ShmRecoveryState::Stage::REBUILD_REQUIRED;
}

Status WorkerFailover::ProcessStandbyWorkerLost(WorkerNode node, client::WorkerRecoveryReason reason)
{
    if (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return Status::OK();
    }
    if (owner_.workerApi_[node] == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("[Reconnect] client %d is null", node));
    }
    if (reason == client::WorkerRecoveryReason::RETRY_PENDING) {
        VLOG(1) << FormatString("[Reconnect] Client[%d] %s retry reconnect to %s", node,
                                owner_.workerApi_[node]->clientId_, owner_.workerApi_[node]->hostPort_.ToString());
    } else {
        LOG(INFO) << FormatString("[Reconnect] Client[%d] %s try to reconnect to %s", node,
                                  owner_.workerApi_[node]->clientId_, owner_.workerApi_[node]->hostPort_.ToString());
    }
    Status s = owner_.workerApi_[node]->ReconnectWorker({});
    if (s.IsError()) {
        constexpr int logInterval = 10;
        LOG_EVERY_T(ERROR, logInterval)
            << FormatString("[Reconnect] client[%d] %s reconnect to worker failed: %s", node,
                            owner_.workerApi_[node]->clientId_, s.ToString());
        return s;
    }
    if (reason == client::WorkerRecoveryReason::CONNECTION_BROKEN && owner_.listenWorker_[node] != nullptr) {
        owner_.listenWorker_[node]->SetWorkerAvailable(true);
    }
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (owner_.currentNode_ == node) {
            MarkWorkerAvailableLocked();
        }
    }
    LOG(INFO) << FormatString("[Reconnect] Client[%d] %s reconnect to worker %s success.", node,
                              owner_.workerApi_[node]->clientId_, owner_.workerApi_[node]->hostPort_.ToString());
    return Status::OK();
}

WorkerFailover::WorkerNode WorkerFailover::GetNextWorkerNode(WorkerNode current)
{
    switch (current) {
        case ObjectClientImpl::LOCAL_WORKER:
        case ObjectClientImpl::STANDBY2_WORKER:
            return ObjectClientImpl::STANDBY1_WORKER;
        case ObjectClientImpl::STANDBY1_WORKER:
            return ObjectClientImpl::STANDBY2_WORKER;
        default:
            return ObjectClientImpl::STANDBY1_WORKER;
    }
}

void WorkerFailover::StopStandbyWorkerListen(WorkerNode id)
{
    if (id == ObjectClientImpl::LOCAL_WORKER || owner_.listenWorker_[id] == nullptr) {
        return;
    }
    owner_.listenWorker_[id]->StopListenWorker(false);
}

void WorkerFailover::MarkWorkerAvailableLocked()
{
    owner_.workerSwitchState_ = WorkerSwitchState::AVAILABLE;
    owner_.switchInProgress_ = false;
    ++owner_.switchGeneration_;
}

void WorkerFailover::MarkNoSwitchableWorkerLocked()
{
    LOG(WARNING) << "[Switch] No switchable worker available, enable fail-fast.";
    owner_.workerSwitchState_ = WorkerSwitchState::NO_SWITCHABLE_WORKER;
    owner_.switchInProgress_ = false;
    ++owner_.switchGeneration_;
}

Status WorkerFailover::NoSwitchableWorkerStatus() const
{
    return { K_RPC_UNAVAILABLE, "no switchable worker available" };
}

bool WorkerFailover::SwitchWorkerNode(WorkerNode node, client::SwitchTriggerReason reason)
{
    if (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED) {
        return true;
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    std::shared_ptr<IClientWorkerApi> nextWorkerApi;
    std::shared_ptr<client::ListenWorker> nextListenWorker;
    WorkerNode current;
    WorkerNode next = ObjectClientImpl::LOCAL_WORKER;
    uint64_t switchGeneration = 0;
    bool switchBackToLocal = false;
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        current = owner_.currentNode_;
        if (current != node && node != ObjectClientImpl::LOCAL_WORKER) {
            LOG(INFO) << FormatString("[Switch] Current node is %d, not %d, just ignore...", current, node);
            return true;
        }

        if (current != node && node == ObjectClientImpl::LOCAL_WORKER) {
            switchBackToLocal = true;
        } else {
            if (owner_.switchInProgress_) {
                VLOG(1) << "[Switch] Worker switch is already in progress";
                return false;
            }
            workerApi = owner_.workerApi_[current];
            if (workerApi == nullptr) {
                LOG(ERROR) << "[Switch] current worker is null pointer";
                return false;
            }
            next = GetNextWorkerNode(current);
            nextWorkerApi = owner_.workerApi_[next];
            nextListenWorker = owner_.listenWorker_[next];
            owner_.workerSwitchState_ = WorkerSwitchState::SWITCHING;
            owner_.switchInProgress_ = true;
            switchGeneration = ++owner_.switchGeneration_;
        }
    }

    if (switchBackToLocal) {
        return TrySwitchBackToLocalWorker();
    }
    // If next stub still has requests to be processed, wait for next time.
    if (!ReadyToExit(next, nextWorkerApi, nextListenWorker)) {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (owner_.switchInProgress_ && owner_.switchGeneration_ == switchGeneration
        && owner_.currentNode_ == current) {
            MarkWorkerAvailableLocked();
        }
        return false;
    }
    return SwitchToStandbyWorkerImpl(workerApi, current, next, switchGeneration, reason);
}

bool WorkerFailover::SwitchToStandbyWorkerImpl(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                               WorkerNode current, WorkerNode next, uint64_t switchGeneration,
                                               client::SwitchTriggerReason reason)
{
    PerfPoint perfPoint(PerfKey::CLIENT_SWITCH_STANDBY_WORKER);
    Raii switchEndNotifier([]() { INJECT_POINT_NO_RETURN("client.switch_worker_end", []() { return true; }); });
    const bool keepCurrentWorker = reason == client::SwitchTriggerReason::VOLUNTARY_SCALE_DOWN
                                   || reason == client::SwitchTriggerReason::URMA_DATA_PLANE_FAILURE;
    std::vector<HostPort> sameHost;
    std::vector<HostPort> others;
    GetStandbyWorkersForSwitch(currentApi, sameHost, others);
    if (sameHost.empty() && others.empty()) {
        LOG(ERROR) << "[Switch] standby worker list is empty";
        if (keepCurrentWorker) {
            RestoreWorkerAvailableIfNeeded(current, switchGeneration);
        } else {
            MarkNoSwitchableWorkerIfNeeded(current, switchGeneration);
        }
        return false;
    }

    // Same-host candidates replace the LOCAL_WORKER slot; others go into a standby slot.
    auto result = TrySwitchToCandidateList(currentApi, current, next, switchGeneration, sameHost, true);
    if (result == StandbySwitchAttemptResult::SWITCHED) {
        return true;
    }
    if (result == StandbySwitchAttemptResult::ABORT) {
        return false;
    }
    result = TrySwitchToCandidateList(currentApi, current, next, switchGeneration, others, false);
    if (result == StandbySwitchAttemptResult::SWITCHED) {
        return true;
    }
    if (result == StandbySwitchAttemptResult::ABORT) {
        return false;
    }
    if (keepCurrentWorker) {
        RestoreWorkerAvailableIfNeeded(current, switchGeneration);
    } else {
        MarkNoSwitchableWorkerIfNeeded(current, switchGeneration);
    }
    return false;
}

WorkerFailover::StandbySwitchAttemptResult WorkerFailover::TrySwitchToCandidateList(
    const std::shared_ptr<IClientWorkerApi> &currentApi, WorkerNode current, WorkerNode next, uint64_t switchGeneration,
    const std::vector<HostPort> &candidates, bool isSameHost)
{
    for (const auto &addr : candidates) {
        if (addr.Empty()) {
            if (!isSameHost) {
                LOG(INFO) << "[Switch] Current worker has not standby worker.";
            }
            continue;
        }
        LOG(INFO) << FormatString("[Switch] Switch worker to %s", addr.ToString());
        // The TrySwitchBackToLocalWorker short-circuit only works on the standby path: with service
        // discovery CommitStandbySwitch stops the old LOCAL_WORKER listener, so its CheckWorkerAvailable
        // will report unavailable. Same-host candidates must go through TrySwitchToLocalSameHost below,
        // which builds a fresh listener.
        if (!isSameHost && addr == owner_.ipAddress_) {
            if (TrySwitchBackToLocalWorker()) {
                return StandbySwitchAttemptResult::SWITCHED;
            }
            continue;
        }
        auto attemptResult = isSameHost ? TrySwitchToLocalSameHost(current, switchGeneration, addr)
                                        : TrySwitchToStandbyWorker(currentApi, current, next, switchGeneration, addr);
        if (attemptResult != StandbySwitchAttemptResult::CONTINUE) {
            return attemptResult;
        }
    }
    return StandbySwitchAttemptResult::CONTINUE;
}

void WorkerFailover::GetStandbyWorkersForSwitch(const std::shared_ptr<IClientWorkerApi> &currentApi,
                                                std::vector<HostPort> &sameHost, std::vector<HostPort> &others) const
{
    sameHost.clear();
    others.clear();
    if (owner_.serviceDiscovery_ != nullptr) {
        std::vector<std::string> sdSameHost;
        std::vector<std::string> sdOthers;
        Status rc = owner_.serviceDiscovery_->GetAllWorkers(sdSameHost, sdOthers);
        if (rc.IsError()) {
            LOG(WARNING) << "[Switch] Service discovery failed, falling back to heartbeat standby list: "
                         << rc.ToString();
            others = currentApi->GetStandbyWorkers();
        } else {
            const HostPort &selfAddr = currentApi->hostPort_;
            auto append = [&selfAddr](const std::vector<std::string> &addrs, std::vector<HostPort> &out) {
                for (const auto &addr : addrs) {
                    HostPort hp;
                    if (hp.ParseString(addr).IsError() || hp == selfAddr) {
                        continue;
                    }
                    out.emplace_back(std::move(hp));
                }
            };
            append(sdSameHost, sameHost);
            append(sdOthers, others);
        }
    } else {
        others = currentApi->GetStandbyWorkers();
    }
    INJECT_POINT_NO_RETURN("client.standby_worker", [&sameHost, &others](const std::string &addr) {
        HostPort hostPort;
        hostPort.ParseString(addr);
        sameHost.clear();
        others.clear();
        others.emplace_back(hostPort);
        return true;
    });
    ShuffleWorkerCandidates(sameHost);
    ShuffleWorkerCandidates(others);
}

bool WorkerFailover::CommitStandbySwitch(WorkerNode current, WorkerNode next, uint64_t switchGeneration,
                                         const std::shared_ptr<IClientWorkerApi> &candidateWorkerApi,
                                         const std::shared_ptr<client::ListenWorker> &candidateListenWorker)
{
    std::shared_ptr<client::ListenWorker> retiredLocalListenWorker;
    std::shared_ptr<IClientWorkerApi> previousWorkerApi;
    client::MmapManager *mmapManagerToClean = nullptr;
    std::vector<int64_t> mmapFdsToClean;

    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (!owner_.switchInProgress_ || owner_.switchGeneration_ != switchGeneration || owner_.currentNode_ != current
            || (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return false;
        }
        previousWorkerApi = owner_.workerApi_[current];
        owner_.workerApi_[next] = candidateWorkerApi;
        owner_.listenWorker_[next] = candidateListenWorker;
        owner_.currentNode_ = next;
        if (owner_.mmapManager_ != nullptr) {
            mmapManagerToClean = owner_.mmapManager_.get();
            mmapFdsToClean = owner_.mmapManager_->GetFds();
        }
        // Stop the LOCAL_WORKER listener only when standby-side rediscovery can take over;
        // otherwise it is still the only recovery path.
        if (owner_.serviceDiscovery_ != nullptr && owner_.serviceDiscovery_->HasHostAffinity()
            && owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER] != nullptr) {
            retiredLocalListenWorker = owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER];
        }
        MarkWorkerAvailableLocked();
    }
    if (retiredLocalListenWorker != nullptr) {
        retiredLocalListenWorker->StopListenWorker(false);
        retiredLocalListenWorker->JoinListenWorker();
        LOG_IF_ERROR(retiredLocalListenWorker->NotifyClientRemovable(), "[Switch] Notify old local client removable");
    }
    if (previousWorkerApi != nullptr && mmapManagerToClean != nullptr && !mmapFdsToClean.empty()) {
        auto weakThis = owner_.weak_from_this();
        std::weak_ptr<IClientWorkerApi> weakPreviousWorkerApi = previousWorkerApi;
        auto func = [weakThis, weakPreviousWorkerApi, current, mmapManagerToClean,
                     mmapFdsToClean = std::move(mmapFdsToClean)]() {
            auto client = weakThis.lock();
            auto previousApi = weakPreviousWorkerApi.lock();
            if (client == nullptr || previousApi == nullptr) {
                return;
            }
            std::lock_guard<std::mutex> lock(client->switchNodeMutex_);
            if (client->currentNode_ != current && client->workerApi_[current] == previousApi
                && client->mmapManager_.get() == mmapManagerToClean) {
                client->mmapManager_->ClearExpiredFds(mmapFdsToClean);
            }
        };
        previousWorkerApi->RunWhenInvokeCountZero(std::move(func));
    }
    return true;
}

WorkerFailover::StandbySwitchAttemptResult WorkerFailover::TrySwitchToStandbyWorker(
    const std::shared_ptr<IClientWorkerApi> &currentApi, WorkerNode current, WorkerNode next, uint64_t switchGeneration,
    const HostPort &standbyWorker)
{
    auto candidateWorkerApi =
        currentApi->CloneWith(standbyWorker, currentApi->heartbeatType_, owner_.token_, owner_.signature_.get(),
                              owner_.tenantId_,
                              owner_.enableCrossNodeConnection_, owner_.embeddedClientWorkerApi_, owner_.worker_);
    candidateWorkerApi->SetMayAccessNonBoundWorker(
        ClientMayAccessNonBoundWorker(owner_.enableLocalCache_, owner_.enableCrossNodeConnection_));
    candidateWorkerApi->isUseStandbyWorker_ = true;
    ConfigureUrmaDataPlaneFailureCallback(next, candidateWorkerApi);
    Status rc = candidateWorkerApi->Init(owner_.requestTimeoutMs_, owner_.connectTimeoutMs_,
                                         owner_.fastTransportMemSize_);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Switch] Worker(%s) init failed, error msg: %s", standbyWorker.ToString(),
                                   rc.ToString());
        return StandbySwitchAttemptResult::CONTINUE;
    }

    auto candidateListenWorker = std::make_shared<client::ListenWorker>(
        candidateWorkerApi, currentApi->heartbeatType_, next, owner_.asyncSwitchWorkerPoolHandle_);
    candidateListenWorker->SetSwitchWorkerHandle([this](uint32_t index, client::SwitchTriggerReason reason) {
        return SwitchWorkerNode(static_cast<WorkerNode>(index), reason);
    });
    candidateListenWorker->SetIsLocalWorker(false);
    if (owner_.serviceDiscovery_ != nullptr && owner_.serviceDiscovery_->HasHostAffinity()) {
        candidateListenWorker->SetRecoverLocalWorkerHandle([this]() { return RecoverPreferredLocalWorker(); });
    }
    candidateListenWorker->AddRecoveryCallback(
        &owner_,
        [this, next](client::WorkerRecoveryReason reason) { return ProcessStandbyWorkerLost(next, reason); });
    rc = candidateListenWorker->StartListenWorker();
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[Switch] Listen worker(%s) failed, with status: %s", standbyWorker.ToString(),
                                   rc.ToString());
        return StandbySwitchAttemptResult::CONTINUE;
    }

    rc = candidateWorkerApi->TryFastTransportAfterHeartbeat();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[Switch] Fast transport init failed for worker(%s), with status: %s",
                                     standbyWorker.ToString(), rc.ToString());
    }

    if (!WaitStandbyWorkerReady(candidateWorkerApi)) {
        LOG(ERROR) << FormatString("[Switch] client %s wait for worker %s ready failed", owner_.GetClientId(),
                                   candidateWorkerApi->hostPort_.ToString());
        candidateListenWorker->StopListenWorker(true);
        return StandbySwitchAttemptResult::CONTINUE;
    }
    if (!CommitStandbySwitch(current, next, switchGeneration, candidateWorkerApi, candidateListenWorker)) {
        candidateListenWorker->StopListenWorker(true);
        return StandbySwitchAttemptResult::ABORT;
    }
    NotifySwitchToExpectedWorker(candidateWorkerApi->hostPort_);
    LOG(INFO) << FormatString("[Switch] client %s wait for worker %s ready success", owner_.GetClientId(),
                              candidateWorkerApi->hostPort_.ToString());
    return StandbySwitchAttemptResult::SWITCHED;
}

WorkerFailover::StandbySwitchAttemptResult WorkerFailover::TrySwitchToLocalSameHost(WorkerNode current,
                                                                                    uint64_t switchGeneration,
                                                                                    const HostPort &localAddress)
{
    HeartbeatType heartbeatType = owner_.workerApi_[current]->heartbeatType_;
    std::shared_ptr<ClientWorkerRemoteApi> localWorkerApi;
    std::unique_ptr<client::MmapManager> localMmapManager;
    std::shared_ptr<client::ListenWorker> localListenWorker;
    if (PreparePreferredLocalWorker(localAddress, heartbeatType, localWorkerApi, localMmapManager, localListenWorker)
            .IsError()) {
        return StandbySwitchAttemptResult::CONTINUE;
    }
    Status rc = localWorkerApi->TryFastTransportAfterHeartbeat();
    if (rc.IsError()) {
        LOG(WARNING) << "[Switch] URMA handshake failed: " << rc.ToString();
    }
    // Declared outside the lock so the old listener's destructor (which joins its heartbeat
    // thread) runs after owner_.switchNodeMutex_ is released; otherwise it can deadlock against
    // ProcessWorkerLost waiting on the same mutex.
    std::shared_ptr<client::ListenWorker> oldLocalListener;
    std::unique_ptr<client::MmapManager> oldMmapManager;
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (!owner_.switchInProgress_ || owner_.switchGeneration_ != switchGeneration || owner_.currentNode_ != current
            || (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return StandbySwitchAttemptResult::ABORT;
        }
        owner_.ipAddress_ = localAddress;
        owner_.workerApi_[ObjectClientImpl::LOCAL_WORKER] = localWorkerApi;
        ReplacePreferredLocalWorkerLocked(localMmapManager, oldLocalListener, oldMmapManager);
        owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER] = localListenWorker;
        owner_.clientEnableP2Ptransfer_ = localWorkerApi->workerEnableP2Ptransfer_;
        owner_.memoryRefCount_.SetSupportMultiShmRefCount(localWorkerApi->workerSupportMultiShmRefCount_);
        owner_.currentNode_ = ObjectClientImpl::LOCAL_WORKER;
        if (current != ObjectClientImpl::LOCAL_WORKER && owner_.listenWorker_[current] != nullptr) {
            owner_.listenWorker_[current]->SetSwitched();
        }
        MarkWorkerAvailableLocked();
    }
    NotifySwitchToExpectedWorker(localAddress);
    LOG(INFO) << "[Switch] LOCAL_WORKER replaced with same-host worker at " << localAddress.ToString();
    return StandbySwitchAttemptResult::SWITCHED;
}

void WorkerFailover::MarkNoSwitchableWorkerIfNeeded(WorkerNode current, uint64_t switchGeneration)
{
    std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
    if (owner_.switchInProgress_ && owner_.switchGeneration_ == switchGeneration && owner_.currentNode_ == current) {
        MarkNoSwitchableWorkerLocked();
    }
}

void WorkerFailover::RestoreWorkerAvailableIfNeeded(WorkerNode current, uint64_t switchGeneration)
{
    std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
    if (owner_.switchInProgress_ && owner_.switchGeneration_ == switchGeneration && owner_.currentNode_ == current) {
        MarkWorkerAvailableLocked();
    }
}

void WorkerFailover::ReplacePreferredLocalWorkerLocked(std::unique_ptr<client::MmapManager> &localMmapManager,
                                                       std::shared_ptr<client::ListenWorker> &oldLocalListener,
                                                       std::unique_ptr<client::MmapManager> &oldMmapManager)
{
    oldLocalListener = std::move(owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER]);
    owner_.mmapManager_.swap(localMmapManager);
    oldMmapManager = std::move(localMmapManager);
}

bool WorkerFailover::TrySwitchBackToLocalWorker()
{
    WorkerNode current;
    std::shared_ptr<IClientWorkerApi> localWorkerApi;
    std::shared_ptr<client::ListenWorker> localListenWorker;
    std::shared_ptr<client::ListenWorker> currentListenWorker;
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        current = owner_.currentNode_;
        if (current == ObjectClientImpl::LOCAL_WORKER) {
            return false;
        }
        localWorkerApi = owner_.workerApi_[ObjectClientImpl::LOCAL_WORKER];
        localListenWorker = owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER];
        currentListenWorker = owner_.listenWorker_[current];
    }

    if (localWorkerApi == nullptr || localListenWorker == nullptr) {
        LOG(ERROR) << "[Switch] Local worker is not ready for switch back";
        return false;
    }
    auto s = localListenWorker->CheckWorkerAvailable();
    bool scaleDown = localListenWorker->IsWorkerVoluntaryScaleDown();
    bool healthy = localWorkerApi->healthy_;
    if (s.IsOk() && !scaleDown && healthy) {
        {
            std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
            if (owner_.currentNode_ == ObjectClientImpl::LOCAL_WORKER) {
                return true;
            }
            if (owner_.currentNode_ != current
        || (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
                return false;
            }
            LOG(INFO) << "[Switch] Restore local worker success.";
            if (currentListenWorker != nullptr) {
                currentListenWorker->SetSwitched();
            }
            owner_.currentNode_ = ObjectClientImpl::LOCAL_WORKER;
            MarkWorkerAvailableLocked();
        }
        NotifySwitchToExpectedWorker(localWorkerApi->hostPort_);
        return true;
    } else {
        constexpr int times = 10;
        LOG_EVERY_T(INFO, times) << FormatString(
            "[Switch] Restore local worker failed, connection status: %s, is scale down: %d, is healthy: %d",
            s.ToString(), scaleDown, healthy);
        return false;
    }
}

bool WorkerFailover::GetPreferredLocalWorkerToRecover(WorkerNode &oldNode, HostPort &localAddress,
                                                      HeartbeatType &heartbeatType)
{
    if (owner_.serviceDiscovery_ == nullptr || !owner_.serviceDiscovery_->HasHostAffinity()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (owner_.currentNode_ == ObjectClientImpl::LOCAL_WORKER
        || (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return false;
        }
        oldNode = owner_.currentNode_;
        if (owner_.workerApi_[oldNode] == nullptr) {
            return false;
        }
        heartbeatType = owner_.workerApi_[oldNode]->heartbeatType_;
    }

    std::string workerIp;
    int workerPort;
    Status rc = owner_.serviceDiscovery_->SelectSameNodeWorker(workerIp, workerPort);
    if (rc.IsError()) {
        constexpr int times = 10;
        LOG_EVERY_T(INFO, times) << "[Switch] Same-node worker is not ready yet: " << rc.ToString();
        return false;
    }
    localAddress = HostPort(workerIp, workerPort);
    return true;
}

Status WorkerFailover::PreparePreferredLocalWorker(const HostPort &localAddress, HeartbeatType heartbeatType,
                                                   std::shared_ptr<ClientWorkerRemoteApi> &localWorkerApi,
                                                   std::unique_ptr<client::MmapManager> &localMmapManager,
                                                   std::shared_ptr<client::ListenWorker> &localListenWorker)
{
    localWorkerApi =
        std::make_shared<ClientWorkerRemoteApi>(localAddress, heartbeatType, owner_.token_, owner_.signature_.get(),
                                           owner_.tenantId_,
                                                owner_.enableCrossNodeConnection_, owner_.deviceId_);
    localWorkerApi->SetMayAccessNonBoundWorker(
        ClientMayAccessNonBoundWorker(owner_.enableLocalCache_, owner_.enableCrossNodeConnection_));
    Status rc = localWorkerApi->Init(owner_.requestTimeoutMs_, owner_.connectTimeoutMs_, owner_.fastTransportMemSize_);
    if (rc.IsError()) {
        LOG(ERROR) << "[Switch] Init preferred same-node worker " << localAddress.ToString()
                   << " failed: " << rc.ToString();
        return rc;
    }
    ConfigureUrmaDataPlaneFailureCallback(ObjectClientImpl::LOCAL_WORKER, localWorkerApi);

    localMmapManager = std::make_unique<client::MmapManager>(localWorkerApi, false);
    rc = localWorkerApi->PrepareForDecreaseShmRef(std::bind(&client::MmapManager::LookupUnitsAndMmapFd,
                                                            localMmapManager.get(), std::placeholders::_1,
                                                            std::placeholders::_2));
    if (rc.IsError()) {
        LOG(ERROR) << "[Switch] PrepareForDecreaseShmRef for preferred same-node worker failed: " << rc.ToString();
        return rc;
    }

    localListenWorker = std::make_shared<client::ListenWorker>(localWorkerApi, localWorkerApi->heartbeatType_,
                                                    ObjectClientImpl::LOCAL_WORKER,
                                                    owner_.asyncSwitchWorkerPoolHandle_);
    localListenWorker->AddRecoveryCallback(
        &owner_, [this](client::WorkerRecoveryReason reason) { return ProcessWorkerLost(reason); });
    localListenWorker->SetWorkerTimeoutHandle([this] { ProcessWorkerTimeout(); });
    localListenWorker->SetReleaseFdCallBack(
        [this](const std::vector<int64_t> &fds) { owner_.mmapManager_->ClearExpiredFds(fds); });
    if (owner_.enableCrossNodeConnection_) {
        localListenWorker->SetSwitchWorkerHandle([this](uint32_t index, client::SwitchTriggerReason reason) {
            return SwitchWorkerNode(static_cast<WorkerNode>(index), reason);
        });
    }
    localListenWorker->SetIsLocalWorker(true);
    rc = localListenWorker->StartListenWorker();
    if (rc.IsError()) {
        LOG(ERROR) << "[Switch] Start preferred same-node worker listener failed: " << rc.ToString();
        return rc;
    }
    return Status::OK();
}

bool WorkerFailover::CommitPreferredLocalWorker(WorkerNode oldNode, const HostPort &localAddress,
                                                const std::shared_ptr<ClientWorkerRemoteApi> &localWorkerApi,
                                                std::unique_ptr<client::MmapManager> localMmapManager,
                                                const std::shared_ptr<client::ListenWorker> &localListenWorker)
{
    // See TrySwitchToLocalSameHost for why the old listener must destruct outside the lock.
    std::shared_ptr<client::ListenWorker> oldLocalListener;
    std::unique_ptr<client::MmapManager> oldMmapManager;
    {
        std::lock_guard<std::mutex> lock(owner_.switchNodeMutex_);
        if (owner_.currentNode_ == ObjectClientImpl::LOCAL_WORKER || owner_.currentNode_ != oldNode
            || (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            return false;
        }
        owner_.ipAddress_ = localAddress;
        owner_.workerApi_[ObjectClientImpl::LOCAL_WORKER] = localWorkerApi;
        ReplacePreferredLocalWorkerLocked(localMmapManager, oldLocalListener, oldMmapManager);
        owner_.listenWorker_[ObjectClientImpl::LOCAL_WORKER] = localListenWorker;
        owner_.clientEnableP2Ptransfer_ = localWorkerApi->workerEnableP2Ptransfer_;
        owner_.memoryRefCount_.SetSupportMultiShmRefCount(localWorkerApi->workerSupportMultiShmRefCount_);
        owner_.currentNode_ = ObjectClientImpl::LOCAL_WORKER;
        if (owner_.listenWorker_[oldNode] != nullptr) {
            owner_.listenWorker_[oldNode]->SetSwitched();
        }
        MarkWorkerAvailableLocked();
    }
    return true;
}

bool WorkerFailover::RecoverPreferredLocalWorker()
{
    WorkerNode oldNode;
    HostPort localAddress;
    HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT;
    if (!GetPreferredLocalWorkerToRecover(oldNode, localAddress, heartbeatType)) {
        return false;
    }

    std::shared_ptr<ClientWorkerRemoteApi> localWorkerApi;
    std::unique_ptr<client::MmapManager> localMmapManager;
    std::shared_ptr<client::ListenWorker> localListenWorker;
    auto rc =
        PreparePreferredLocalWorker(localAddress, heartbeatType, localWorkerApi, localMmapManager, localListenWorker);
    if (rc.IsError()) {
        return false;
    }
    if (!WaitStandbyWorkerReady(localWorkerApi)) {
        LOG(ERROR) << FormatString("[Switch] client %s wait for preferred local worker %s ready failed, keep fallback",
                                   owner_.GetClientId(), localAddress.ToString());
        localListenWorker->StopListenWorker(true);
        return false;
    }
    if (!CommitPreferredLocalWorker(oldNode, localAddress, localWorkerApi, std::move(localMmapManager),
                                    localListenWorker)) {
        return false;
    }

    NotifySwitchToExpectedWorker(localAddress);
    LOG(INFO) << "[Switch] Preferred same-node worker recovered at " << localAddress.ToString();
    return true;
}

bool WorkerFailover::ReadyToExit(WorkerNode node, const std::shared_ptr<IClientWorkerApi> &workerApi,
                                 const std::shared_ptr<client::ListenWorker> &listenWorker)
{
    if (!workerApi || !listenWorker) {
        return true;
    }

    auto count = workerApi->InvokeCount();
    auto status = listenWorker->CheckWorkerAvailable();
    if (status.IsOk() && count > 0) {
        LOG(INFO) << FormatString("[Switch] Client %d Still have %d invoke count need to process", node, count);
        return false;
    }
    if (status.IsOk()) {
        (void)workerApi->Disconnect(false);
    }
    listenWorker->StopListenWorker(true);
    return true;
}

bool WorkerFailover::WaitStandbyWorkerReady(const std::shared_ptr<IClientWorkerApi> &clientWorkerApi)
{
    if (clientWorkerApi == nullptr) {
        LOG(WARNING) << "[Switch] client worker api is nullptr";
        return false;
    }
    LOG(INFO) << FormatString("[Switch] client %s wait for worker %s ready", owner_.GetClientId(),
                              clientWorkerApi->hostPort_.ToString());
    constexpr uint64_t maxWaitMilliseconds = 10000;
    constexpr uint64_t waitIntervalMs = 500;
    uint64_t waitMilliseconds = std::min<uint64_t>(clientWorkerApi->heartBeatIntervalMs_ * 2, maxWaitMilliseconds);
    Timer timer;
    bool success = false;
    do {
        success = clientWorkerApi->healthy_;
        if (success || (owner_.clientStateManager_->GetState() & (uint16_t)ClientState::EXITED)) {
            break;
        }
        owner_.switchPost_.WaitFor(waitIntervalMs);
    } while (timer.ElapsedMilliSecond() <= waitMilliseconds && !success);
    return success;
}

}  // namespace object_cache
}  // namespace datasystem
