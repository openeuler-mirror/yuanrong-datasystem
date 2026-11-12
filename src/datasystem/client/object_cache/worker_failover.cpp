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
 * Description: Object client registration and local resource recovery.
 */
#include "datasystem/client/object_cache/worker_failover.h"

#include <atomic>
#include <functional>

#include <bthread/mutex.h>

#include "datasystem/client/object_cache/client_memory_ref_table.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace object_cache {

WorkerFailover::WorkerFailover(ObjectClientImpl &owner) : owner_(owner)
{
}

void WorkerFailover::ConfigureUbHealthCallback(const std::shared_ptr<IClientWorkerApi> &workerApi)
{
    if (workerApi == nullptr) {
        return;
    }
    if (owner_.ubHealthRegistry_ == nullptr) {
        owner_.ubHealthRegistry_ = std::make_shared<client::WorkerUbHealthRegistry>();
    }
    if (owner_.ubHealthFilter_ == nullptr) {
        owner_.ubHealthFilter_ = std::make_shared<client::UbHealthFilter>(owner_.ubHealthRegistry_);
    }
    if (owner_.transportLayer_ != nullptr) {
        workerApi->SetUbHealthSummaryCallback(owner_.transportLayer_->GetUbHealthSummaryApplyHook());
        return;
    }
    std::weak_ptr<client::UbHealthFilter> weakFilter(owner_.ubHealthFilter_);
    workerApi->SetUbHealthSummaryCallback([weakFilter](const UbHealthSummary &summary) {
        auto filter = weakFilter.lock();
        if (filter == nullptr) {
            return;
        }
        if (summary.portHealth.has_value()) {
            bool recovered = false;
            (void)filter->ObserveSummary(summary, summary.incarnation, recovered);
        } else {
            (void)filter->ApplySummary(summary, summary.incarnation);
        }
    });
}

Status WorkerFailover::ProcessWorkerLost(client::WorkerRecoveryReason reason)
{
    if (owner_.clientStateManager_->GetState() & static_cast<uint16_t>(ClientState::EXITED)) {
        return Status::OK();
    }
    if (owner_.transportLayer_ != nullptr && owner_.workerApi_ != nullptr) {
        owner_.transportLayer_->UpdateBoundWorkerShmCapability(false);
    }
    auto &recovery = *owner_.shmRecoveryState_;
    std::lock_guard<bthread::Mutex> recoveryLock(recovery.mutex);
    if (reason != client::WorkerRecoveryReason::RETRY_PENDING || recovery.stage == ShmRecoveryState::Stage::IDLE) {
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
    if (owner_.transportLayer_ != nullptr && owner_.workerApi_ != nullptr) {
        owner_.transportLayer_->UpdateBoundWorkerShmCapability(
            owner_.workerApi_->IsShmEnable() && !owner_.workerApi_->DidShmLocalityProbeFail());
    }
    recovery.stage = ShmRecoveryState::Stage::IDLE;
    if (reason == client::WorkerRecoveryReason::CONNECTION_BROKEN && owner_.listenWorker_ != nullptr) {
        owner_.listenWorker_->SetWorkerAvailable(true);
    }
    LOG(INFO) << "[Reconnect] Recovered client registration and local worker resources.";
    auto routing = std::atomic_load(&owner_.routing_);
    if (routing != nullptr) {
        (void)routing->ForceRefresh();
    }
    INJECT_POINT("ObjectClientImpl.ProcessWorkerLost", []() { return Status::OK(); });
    return Status::OK();
}

Status WorkerFailover::RegisterWorkerAfterWorkerLost(client::WorkerRecoveryReason reason)
{
    if (reason == client::WorkerRecoveryReason::RETRY_PENDING) {
        VLOG(1) << "[Reconnect] Retry reconnect to " << owner_.ipAddress_.ToString();
    } else {
        LOG(INFO) << "[Reconnect] Clear local resources and reconnect to " << owner_.ipAddress_.ToString();
    }
    std::vector<std::string> ids;
    {
        std::lock_guard<std::shared_timed_mutex> lock(owner_.globalRefMutex_);
        ids.reserve(owner_.globalRefCount_.size());
        for (const auto &entry : owner_.globalRefCount_) {
            ids.emplace_back(entry.first);
        }
    }
    auto &workerApi = owner_.workerApi_;
    RETURN_RUNTIME_ERROR_IF_NULL(workerApi);
    auto rc = workerApi->ReconnectWorker(ids);
    if (rc.IsError()) {
        constexpr int LOG_INTERVAL_SECONDS = 10;
        LOG_EVERY_T(ERROR, LOG_INTERVAL_SECONDS) << "[Reconnect] Reconnect worker failed: " << rc.ToString();
        return rc;
    }
    owner_.memoryRefCount_.SetSupportMultiShmRefCount(workerApi->workerSupportMultiShmRefCount_);
    return Status::OK();
}

Status WorkerFailover::RebuildWorkerShm()
{
    auto &workerApi = owner_.workerApi_;
    RETURN_RUNTIME_ERROR_IF_NULL(workerApi);
    (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
    (void)workerApi->CleanUpForPipelineRH2DQueueAfterWorkerLost();
    owner_.mmapManager_->CleanInvalidMmapTable();
    auto rc = workerApi->PrepareForDecreaseShmRef(std::bind(&client::MmapManager::LookupUnitsAndMmapFd,
                                                            owner_.mmapManager_.get(), std::placeholders::_1,
                                                            std::placeholders::_2));
    if (rc.IsError()) {
        constexpr int LOG_INTERVAL_SECONDS = 10;
        LOG_EVERY_T(ERROR, LOG_INTERVAL_SECONDS) << "[Reconnect] Prepare DecreaseShmRef failed: " << rc.ToString();
        return rc;
    }
    rc = workerApi->InitPipelineRH2DQueue([this](std::shared_ptr<ShmUnitInfo> &shmUnitInfo) {
        return owner_.mmapManager_->LookupUnitsAndMmapFd("", shmUnitInfo);
    });
    if (rc.IsError()) {
        constexpr int LOG_INTERVAL_SECONDS = 10;
        LOG_EVERY_T(ERROR, LOG_INTERVAL_SECONDS)
            << PIPLN_LOG_PREFIX << "Reconnect: InitQueue failed: " << rc.ToString();
        (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
        (void)workerApi->CleanUpForPipelineRH2DQueueAfterWorkerLost();
        owner_.mmapManager_->CleanInvalidMmapTable();
    }
    return rc;
}

void WorkerFailover::CleanupWorkerShmAfterWorkerLost()
{
    auto &workerApi = owner_.workerApi_;
    if (workerApi != nullptr) {
        (void)workerApi->CleanUpForDecreaseShmRefAfterWorkerLost();
        (void)workerApi->CleanUpForPipelineRH2DQueueAfterWorkerLost();
    }
    if (owner_.mmapManager_ != nullptr) {
        owner_.mmapManager_->CleanInvalidMmapTable();
    }
    owner_.memoryRefCount_.Clear();
}

void WorkerFailover::ProcessWorkerTimeout()
{
    if (owner_.clientStateManager_->GetState() & static_cast<uint16_t>(ClientState::EXITED)) {
        return;
    }
    if (owner_.transportLayer_ != nullptr && owner_.workerApi_ != nullptr) {
        owner_.transportLayer_->UpdateBoundWorkerShmCapability(false);
    }
    auto &recovery = *owner_.shmRecoveryState_;
    std::lock_guard<bthread::Mutex> recoveryLock(recovery.mutex);
    CleanupWorkerShmAfterWorkerLost();
    recovery.stage = ShmRecoveryState::Stage::REBUILD_REQUIRED;
    auto routing = std::atomic_load(&owner_.routing_);
    if (routing != nullptr && owner_.workerApi_ != nullptr) {
        routing->UpdateState(owner_.workerApi_->hostPort_, K_RPC_UNAVAILABLE);
    }
}

}  // namespace object_cache
}  // namespace datasystem
