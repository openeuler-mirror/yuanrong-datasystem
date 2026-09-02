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
 * Description: The resource manager implement.
 */

#include "datasystem/master/resource_manager.h"

#include <algorithm>
#include <chrono>
#include <mutex>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/eviction_policy_common.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/timer.h"

DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
namespace {
constexpr uint32_t FULL_COHORT_PERCENT = 100;

constexpr uint64_t SNAPSHOT_CLEAR_MIN_S = 60;

master::EvictionPolicyWorkerProgressPb BuildEvictionPolicyWorkerProgress(const master::WorkerStat &stat)
{
    master::EvictionPolicyWorkerProgressPb progress;
    progress.set_active_policy(stat.eviction_policy());
    progress.set_epoch(stat.eviction_policy_control_epoch());
    progress.set_phase(stat.eviction_policy_update_phase());
    progress.set_status(stat.eviction_policy_worker_status());
    progress.set_total_objects(stat.eviction_policy_total_objects());
    progress.set_migrated_objects(stat.eviction_policy_migrated_objects());
    progress.set_failure_code(stat.eviction_policy_failure_code());
    progress.set_failure_reason(stat.eviction_policy_failure_reason());
    return progress;
}

Status ValidateEvictionPolicyRollout(const master::EvictionPolicyRolloutPb &rollout)
{
    CHECK_FAIL_RETURN_STATUS(rollout.has_update(), K_INVALID, "Eviction policy rollout has no update");
    const auto &update = rollout.update();
    CHECK_FAIL_RETURN_STATUS(update.target_policy() == master::EVICTION_POLICY_CLOCK
                                 || update.target_policy() == master::EVICTION_POLICY_HEAT,
                             K_INVALID, "Unsupported eviction policy");
    CHECK_FAIL_RETURN_STATUS(update.epoch() > 0, K_INVALID, "Eviction policy update epoch must be positive");
    CHECK_FAIL_RETURN_STATUS(update.migration_batch_size() > 0, K_INVALID,
                             "Eviction policy migration batch size must be positive");
    CHECK_FAIL_RETURN_STATUS(update.migration_batch_size() <= EVICTION_POLICY_MAX_MIGRATION_BATCH_SIZE, K_INVALID,
                             "Eviction policy migration batch size exceeds the control-path limit");
    CHECK_FAIL_RETURN_STATUS(update.command() == master::EVICTION_POLICY_PRECHECK
                                 || update.command() == master::EVICTION_POLICY_COMMIT_CONVERT,
                             K_INVALID, "Unsupported eviction policy rollout command");
    CHECK_FAIL_RETURN_STATUS(
        rollout.cohort_percent() > 0 && rollout.cohort_percent() <= FULL_COHORT_PERCENT, K_INVALID,
        "Eviction policy cohort percent must be in [1, 100]");
    return Status::OK();
}

NodeInfo BuildNodeInfo(const master::ResourceReportReqPb &req, uint64_t timestamp)
{
    const auto policy = req.stat().eviction_policy() == master::EVICTION_POLICY_UNSPECIFIED
                            ? master::EVICTION_POLICY_CLOCK
                            : req.stat().eviction_policy();
    return NodeInfo(req.stat().address(), req.stat().available_memory(), req.stat().is_ready(), timestamp,
                    req.stat().used_memory(), req.stat().memory_capacity(), req.stat().memory_limit(),
                    req.stat().hot_primary_copy_count(), req.stat().total_primary_copy_count(),
                    req.stat().hot_primary_copy_bytes(), static_cast<uint32_t>(policy),
                    req.stat().eviction_policy_epoch());
}

Status PrepareCommittedRollout(const master::EvictionPolicyRolloutPb &requested, const std::string &oldValue,
                               std::unique_ptr<std::string> &newValue, bool &retry,
                               master::EvictionPolicyRolloutPb &committed)
{
    retry = false;
    if (oldValue.empty()) {
        committed = requested;
    } else {
        master::EvictionPolicyRolloutPb existing;
        CHECK_FAIL_RETURN_STATUS(existing.ParseFromString(oldValue), K_INVALID,
                                 "Persisted eviction policy rollout is malformed");
        RETURN_IF_NOT_OK(ValidateEvictionPolicyRollout(existing));
        CHECK_FAIL_RETURN_STATUS(requested.update().epoch() >= existing.update().epoch(), K_INVALID,
                                 "Eviction policy update epoch is stale");
        if (requested.update().epoch() == existing.update().epoch()) {
            CHECK_FAIL_RETURN_STATUS(
                requested.update().target_policy() == existing.update().target_policy()
                    && requested.update().migration_batch_size() == existing.update().migration_batch_size()
                    && requested.update().minimum_available_memory_bytes()
                           == existing.update().minimum_available_memory_bytes()
                    && requested.update().maximum_source_objects() == existing.update().maximum_source_objects()
                    && requested.update().deadline_unix_ms() == existing.update().deadline_unix_ms(),
                K_INVALID, "Eviction policy update epoch conflicts with an existing intent");
            CHECK_FAIL_RETURN_STATUS(requested.cohort_percent() >= existing.cohort_percent(), K_INVALID,
                                     "Eviction policy cohort can not shrink within an epoch");
            CHECK_FAIL_RETURN_STATUS(!(existing.update().command() == master::EVICTION_POLICY_COMMIT_CONVERT
                                       && requested.update().command() == master::EVICTION_POLICY_PRECHECK),
                                     K_INVALID, "Eviction policy rollout can not return to PRECHECK after COMMIT");
            if (requested.cohort_percent() == existing.cohort_percent()
                && requested.update().command() == existing.update().command()) {
                committed = std::move(existing);
                return Status::OK();
            }
        }
        committed = requested;
    }
    std::string serialized;
    CHECK_FAIL_RETURN_STATUS(committed.SerializeToString(&serialized), K_RUNTIME_ERROR,
                             "Serialize eviction policy rollout failed");
    newValue = std::make_unique<std::string>(std::move(serialized));
    return Status::OK();
}

void FillWorkerStat(const NodeInfo &nodeInfo, master::WorkerStat &stat)
{
    stat.set_address(nodeInfo.nodeId);
    stat.set_available_memory(nodeInfo.availableMemory);
    stat.set_is_ready(nodeInfo.isReady);
    stat.set_used_memory(nodeInfo.usedMemory);
    stat.set_memory_capacity(nodeInfo.memoryCapacity);
    stat.set_memory_limit(nodeInfo.memoryLimit);
    stat.set_hot_primary_copy_count(nodeInfo.hotPrimaryCopyCount);
    stat.set_total_primary_copy_count(nodeInfo.totalPrimaryCopyCount);
    stat.set_hot_primary_copy_bytes(nodeInfo.hotPrimaryCopyBytes);
}
}  // namespace

ResourceManager::ResourceManager()
{
    rebalanceScheduler_ = GetRebalanceStrategy() == "heat"
                              ? std::unique_ptr<RebalanceScheduler>(std::make_unique<HeatRebalanceScheduler>())
                              : std::unique_ptr<RebalanceScheduler>(std::make_unique<MemoryRebalanceScheduler>());
    workerThread_ = Thread(&ResourceManager::WorkerThread, this);
    workerThread_.set_name("ResourceManager");
    LOG(INFO) << "ResourceManager initialized with double-buffered snapshot, rebalance strategy: "
              << GetRebalanceStrategy();
}

ResourceManager::~ResourceManager()
{
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        running_.store(false);
    }
    taskCv_.notify_all();
    if (workerThread_.joinable()) {
        workerThread_.join();
    }
}

void ResourceManager::SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership)
{
    rebalanceScheduler_->SetTopologyMembership(topologyMembership);
}

Status ResourceManager::ReportResource(const master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp)
{
    const auto currentTimestamp = GetSteadyClockTimeStampMs();
    const std::string address = req.stat().address();
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_INVALID, "The address can not be empty");
    NodeInfo newInfo = BuildNodeInfo(req, currentTimestamp);
    ApplyEvictionPolicyRolloutToReport(req, newInfo, rsp);
    {
        // Update the write snapshot after applying control-plane admission so all
        // schedulers observe a converting worker as cordoned.
        std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
        auto result = writeSnapshot_.emplace(address, newInfo);
        if (!result.second) {
            result.first->second = newInfo;
        }
    }

    bool needScheduleSnapshot = rebalanceScheduler_->NeedSnapshotForSchedule(req, newInfo, rsp);
    if (needScheduleSnapshot) {
        std::unordered_map<std::string, NodeInfo> snapshot;
        BuildLatestSnapshot(snapshot);
        auto reportingWorker = snapshot.find(address);
        if (reportingWorker == snapshot.end() || reportingWorker->second.timestamp < newInfo.timestamp) {
            snapshot.insert_or_assign(address, newInfo);
        }
        auto *stats = rsp.mutable_stats();
        for (const auto &[worker, nodeInfo] : snapshot) {
            (void)worker;
            FillWorkerStat(nodeInfo, *stats->Add());
        }
        return rebalanceScheduler_->Schedule(req, snapshot, rsp);
    }
    auto *stats = rsp.mutable_stats();
    {
        std::shared_lock<SharedMutex> lock(readSnapshotMutex_);
        bool hasReportingWorker = false;
        for (const auto &[worker, nodeInfoInSnapshot] : readSnapshot_) {
            const NodeInfo &nodeInfo = worker == address ? newInfo : nodeInfoInSnapshot;
            hasReportingWorker = hasReportingWorker || worker == address;
            auto *stat = stats->Add();
            FillWorkerStat(nodeInfo, *stat);
        }
        if (!hasReportingWorker) {
            auto *stat = stats->Add();
            FillWorkerStat(newInfo, *stat);
        }
    }
    return Status::OK();
}

void ResourceManager::ApplyEvictionPolicyRolloutToReport(const master::ResourceReportReqPb &req, NodeInfo &nodeInfo,
                                                         master::ResourceReportRspPb &rsp)
{
    auto rollout = std::atomic_load_explicit(&evictionPolicyRollout_, std::memory_order_acquire);
    if (rollout == nullptr) {
        return;
    }
    const bool supportsPolicyRollout = req.stat().eviction_policy() != master::EVICTION_POLICY_UNSPECIFIED;
    const bool selected = supportsPolicyRollout
                          && MurmurHash3_32(req.stat().address()) % FULL_COHORT_PERCENT < rollout->cohort_percent();
    if (selected) {
        auto workerProgress = BuildEvictionPolicyWorkerProgress(req.stat());
        std::lock_guard<std::mutex> lock(evictionPolicyMutex_);
        auto current = std::atomic_load_explicit(&evictionPolicyRollout_, std::memory_order_relaxed);
        if (current != rollout) {
            return;
        }
        evictionPolicyWorkerProgress_.insert_or_assign(req.stat().address(), std::move(workerProgress));
    }
    const bool converged = req.stat().eviction_policy_epoch() >= rollout->update().epoch()
                           && req.stat().eviction_policy() == rollout->update().target_policy()
                           && req.stat().eviction_policy_update_phase() == master::EVICTION_POLICY_STABLE;
    const bool precheckReady = req.stat().eviction_policy_control_epoch() == rollout->update().epoch()
                               && req.stat().eviction_policy_worker_status() == master::EVICTION_POLICY_WORKER_READY;
    const bool shouldDispatch =
        rollout->update().command() == master::EVICTION_POLICY_PRECHECK ? !precheckReady : !converged;
    if (selected && shouldDispatch) {
        rsp.mutable_eviction_policy_update()->CopyFrom(rollout->update());
        if (rollout->update().command() == master::EVICTION_POLICY_COMMIT_CONVERT) {
            nodeInfo.isReady = false;
        }
    }
}

Status ResourceManager::SetEvictionPolicyUpdate(const master::EvictionPolicyUpdatePb &update, uint32_t cohortPercent)
{
    master::EvictionPolicyRolloutPb requested;
    requested.mutable_update()->CopyFrom(update);
    requested.set_cohort_percent(cohortPercent);
    RETURN_IF_NOT_OK(ValidateEvictionPolicyRollout(requested));

    RolloutCas cas;
    {
        std::lock_guard<std::mutex> lock(evictionPolicyMutex_);
        cas = evictionPolicyRolloutCas_;
    }
    CHECK_FAIL_RETURN_STATUS(cas != nullptr, K_NOT_READY, "Eviction policy rollout store is not initialized");

    master::EvictionPolicyRolloutPb committed;
    auto process = [&requested, &committed](const std::string &oldValue, std::unique_ptr<std::string> &newValue,
                                            bool &retry) {
        return PrepareCommittedRollout(requested, oldValue, newValue, retry, committed);
    };
    RETURN_IF_NOT_OK(cas(process));
    RETURN_IF_NOT_OK(ApplyEvictionPolicyRollout(committed));
    LOG(INFO) << "Committed eviction policy rollout epoch=" << committed.update().epoch()
              << " target=" << committed.update().target_policy() << " command=" << committed.update().command()
              << " cohort_percent=" << committed.cohort_percent();
    return Status::OK();
}

Status ResourceManager::GetEvictionPolicyUpdateProgress(uint64_t epoch,
                                                        master::GetEvictionPolicyUpdateProgressRspPb &rsp)
{
    std::shared_ptr<const master::EvictionPolicyRolloutPb> rollout;
    std::vector<std::pair<std::string, master::EvictionPolicyWorkerProgressPb>> progress;
    {
        std::lock_guard<std::mutex> rolloutLock(evictionPolicyMutex_);
        rollout = std::atomic_load_explicit(&evictionPolicyRollout_, std::memory_order_relaxed);
        CHECK_FAIL_RETURN_STATUS(rollout != nullptr, K_NOT_FOUND, "Eviction policy rollout does not exist");
        CHECK_FAIL_RETURN_STATUS(epoch == 0 || epoch == rollout->update().epoch(), K_NOT_FOUND,
                                 "Eviction policy rollout epoch does not match");
        progress.reserve(evictionPolicyWorkerProgress_.size());
        for (const auto &entry : evictionPolicyWorkerProgress_) {
            progress.emplace_back(entry);
        }
    }
    rsp.mutable_rollout()->CopyFrom(*rollout);
    for (const auto &[address, stat] : progress) {
        if (MurmurHash3_32(address) % FULL_COHORT_PERCENT >= rollout->cohort_percent()) {
            continue;
        }
        auto *worker = rsp.add_workers();
        worker->CopyFrom(stat);
        worker->set_address(address);
        if (stat.epoch() != rollout->update().epoch()) {
            continue;
        }
        switch (stat.status()) {
            case master::EVICTION_POLICY_WORKER_READY:
                rsp.set_ready_workers(rsp.ready_workers() + 1);
                break;
            case master::EVICTION_POLICY_WORKER_CONVERTING:
                rsp.set_converting_workers(rsp.converting_workers() + 1);
                break;
            case master::EVICTION_POLICY_WORKER_ACTIVE:
                rsp.set_active_workers(rsp.active_workers() + 1);
                break;
            case master::EVICTION_POLICY_WORKER_FAILED:
                rsp.set_failed_workers(rsp.failed_workers() + 1);
                break;
            default:
                break;
        }
    }
    rsp.set_selected_workers(rsp.workers_size());
    return Status::OK();
}

Status ResourceManager::InitEvictionPolicyRolloutStore(RolloutLoader loader, RolloutCas cas)
{
    CHECK_FAIL_RETURN_STATUS(loader != nullptr, K_INVALID, "Eviction policy rollout loader is null");
    CHECK_FAIL_RETURN_STATUS(cas != nullptr, K_INVALID, "Eviction policy rollout CAS is null");
    {
        std::lock_guard<std::mutex> lock(evictionPolicyMutex_);
        CHECK_FAIL_RETURN_STATUS(evictionPolicyRolloutLoader_ == nullptr && evictionPolicyRolloutCas_ == nullptr,
                                 K_INVALID, "Eviction policy rollout store is already initialized");
        evictionPolicyRolloutLoader_ = std::move(loader);
        evictionPolicyRolloutCas_ = std::move(cas);
    }
    RETURN_IF_NOT_OK(RefreshEvictionPolicyRollout());
    return Status::OK();
}

Status ResourceManager::RefreshEvictionPolicyRollout()
{
    RolloutLoader loader;
    {
        std::lock_guard<std::mutex> lock(evictionPolicyMutex_);
        loader = evictionPolicyRolloutLoader_;
    }
    CHECK_FAIL_RETURN_STATUS(loader != nullptr, K_NOT_READY, "Eviction policy rollout store is not initialized");
    std::string serialized;
    auto rc = loader(serialized);
    if (rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    master::EvictionPolicyRolloutPb rollout;
    CHECK_FAIL_RETURN_STATUS(rollout.ParseFromString(serialized), K_INVALID,
                             "Persisted eviction policy rollout is malformed");
    return ApplyEvictionPolicyRollout(rollout);
}

Status ResourceManager::ApplyEvictionPolicyRollout(const master::EvictionPolicyRolloutPb &rollout)
{
    RETURN_IF_NOT_OK(ValidateEvictionPolicyRollout(rollout));
    auto nextRollout = std::make_shared<const master::EvictionPolicyRolloutPb>(rollout);
    std::lock_guard<std::mutex> lock(evictionPolicyMutex_);
    auto current = std::atomic_load_explicit(&evictionPolicyRollout_, std::memory_order_relaxed);
    if (current != nullptr) {
        if (rollout.update().epoch() < current->update().epoch()) {
            return Status::OK();
        }
        if (rollout.update().epoch() == current->update().epoch()) {
            CHECK_FAIL_RETURN_STATUS(
                rollout.update().target_policy() == current->update().target_policy()
                    && rollout.update().migration_batch_size()
                           == current->update().migration_batch_size()
                    && rollout.update().minimum_available_memory_bytes()
                           == current->update().minimum_available_memory_bytes()
                    && rollout.update().maximum_source_objects()
                           == current->update().maximum_source_objects()
                    && rollout.update().deadline_unix_ms() == current->update().deadline_unix_ms(),
                K_INVALID, "Persisted eviction policy rollout conflicts with local intent");
            CHECK_FAIL_RETURN_STATUS(
                !(current->update().command() == master::EVICTION_POLICY_COMMIT_CONVERT
                  && rollout.update().command() == master::EVICTION_POLICY_PRECHECK),
                K_INVALID, "Persisted eviction policy rollout regressed to PRECHECK");
            const bool commandAdvanced = current->update().command() == master::EVICTION_POLICY_PRECHECK
                                         && rollout.update().command() == master::EVICTION_POLICY_COMMIT_CONVERT;
            if (!commandAdvanced && rollout.cohort_percent() <= current->cohort_percent()) {
                return Status::OK();
            }
        }
    }
    const bool epochChanged = current == nullptr || rollout.update().epoch() != current->update().epoch();
    if (epochChanged) {
        evictionPolicyWorkerProgress_.clear();
    }
    std::atomic_store_explicit(&evictionPolicyRollout_, std::move(nextRollout), std::memory_order_release);
    return Status::OK();
}

Status ResourceManager::ReportRebalanceResult(const master::ReportRebalanceResultReqPb &req,
                                              master::ReportRebalanceResultRspPb &rsp)
{
    return rebalanceScheduler_->ReportResult(req, rsp);
}

void ResourceManager::WorkerThread()
{
    int switchNumber = 0;
    int switchClearRatio = 3;
    int64_t intervalMs = WORKER_THREAD_INTERVAL_MS;
    INJECT_POINT_NO_RETURN("ResourceManager.setInterval", [&intervalMs](int64_t interval) { intervalMs = interval; });
    while (running_) {
        auto refreshStatus = RefreshEvictionPolicyRollout();
        if (refreshStatus.IsError() && refreshStatus.GetCode() != K_NOT_READY) {
            LOG(WARNING) << "Refresh eviction policy rollout failed: " << refreshStatus.ToString();
        }
        // Testability hook: skip background mutation so a UT can place reports in each buffer deterministically.
        bool skipBackgroundSwap = false;
        INJECT_POINT_NO_RETURN("ResourceManager.skipBackgroundSwap",
                               [&skipBackgroundSwap]() { skipBackgroundSwap = true; });
        if (!skipBackgroundSwap) {
            // Clear once every switchClearRatio switches
            if (++switchNumber % switchClearRatio == 0) {
                ClearWriteSnapshot();
            }
            SwitchSnapshots();
        }
        std::unique_lock<std::mutex> lock(taskMutex_);
        if (!running_.load()) {
            break;
        }
        (void)taskCv_.wait_for(lock, std::chrono::milliseconds(intervalMs), [this]() { return !running_.load(); });
    }
}

void ResourceManager::ClearWriteSnapshot()
{
    const uint64_t deadTimeoutS =
        std::max(static_cast<uint64_t>(FLAGS_node_dead_timeout_s), SNAPSHOT_CLEAR_MIN_S);
    auto deadTimestamp = GetSteadyClockTimeStampMs() - deadTimeoutS * SECS_TO_MS;
    std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
    for (auto it = writeSnapshot_.begin(); it != writeSnapshot_.end();) {
        if (it->second.timestamp < deadTimestamp) {
            it = writeSnapshot_.erase(it);
        } else {
            ++it;
        }
    }
}

void ResourceManager::SwitchSnapshots()
{
    std::unique_lock<SharedMutex> swapLock(snapshotSwapMutex_);
    std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
    std::unique_lock<SharedMutex> lockRead(readSnapshotMutex_);
    std::swap(readSnapshot_, writeSnapshot_);
}
void ResourceManager::BuildLatestSnapshot(std::unordered_map<std::string, NodeInfo> &snapshot)
{
    std::shared_lock<SharedMutex> swapLock(snapshotSwapMutex_);
    size_t readSnapshotSize;
    {
        std::shared_lock<SharedMutex> readLock(readSnapshotMutex_);
        readSnapshotSize = readSnapshot_.size();
    }
    size_t writeSnapshotSize;
    {
        std::lock_guard<std::mutex> writeLock(writeSnapshotMutex_);
        writeSnapshotSize = writeSnapshot_.size();
    }
    snapshot.reserve(readSnapshotSize + writeSnapshotSize);
    auto merge = [&snapshot](const std::unordered_map<std::string, NodeInfo> &source) {
        for (const auto &[worker, nodeInfo] : source) {
            auto iter = snapshot.find(worker);
            if (iter == snapshot.end() || iter->second.timestamp <= nodeInfo.timestamp) {
                snapshot.insert_or_assign(worker, nodeInfo);
            }
        }
    };
    {
        std::shared_lock<SharedMutex> readLock(readSnapshotMutex_);
        INJECT_POINT_NO_RETURN("ResourceManager.BuildLatestSnapshot.beforeReadMerge");
        merge(readSnapshot_);
    }
    {
        std::lock_guard<std::mutex> writeLock(writeSnapshotMutex_);
        merge(writeSnapshot_);
    }
}
}  // namespace master
}  // namespace datasystem
