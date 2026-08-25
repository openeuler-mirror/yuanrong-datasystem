/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Eviction policy hot-update state machine and heat telemetry.
 */
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <new>
#include <vector>

#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/eviction_policy_common.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr uint32_t POLICY_UPDATE_EVICTION_DRAIN_TIMEOUT_S = 30;
constexpr int POLICY_UPDATE_PROGRESS_LOG_INTERVAL_S = 10;
constexpr double COUNTER_P50_QUANTILE = 0.50;
constexpr double COUNTER_P90_QUANTILE = 0.90;
constexpr double COUNTER_P99_QUANTILE = 0.99;

double CounterPercentile(const std::vector<double> &counters, double quantile)
{
    const auto index = static_cast<size_t>(std::ceil(quantile * static_cast<double>(counters.size())) - 1.0);
    return counters[std::min(index, counters.size() - 1)];
}

std::vector<std::pair<std::string, std::shared_ptr<SafeObjType>>> TakeObjectSnapshot(
    const std::shared_ptr<ObjectTable> &objectTable)
{
    std::vector<std::pair<std::string, std::shared_ptr<SafeObjType>>> snapshot;
    snapshot.reserve(objectTable->GetSize());
    for (auto &entry : *objectTable) {
        snapshot.emplace_back(entry.first, entry.second);
    }
    return snapshot;
}

void FillCounterPercentiles(WorkerOcEvictionManager::CopyWatermarkStats &stats, std::vector<double> &counters)
{
    std::sort(counters.begin(), counters.end());
    stats.counterP50 = CounterPercentile(counters, COUNTER_P50_QUANTILE);
    stats.counterP90 = CounterPercentile(counters, COUNTER_P90_QUANTILE);
    stats.counterP99 = CounterPercentile(counters, COUNTER_P99_QUANTILE);
}

void RecordPrimaryCopyTemperature(WorkerOcEvictionManager::CopyWatermarkStats &stats,
                                  const EvictionList::HeatNodeMetadata &metadata, bool cold, bool hot)
{
    ++stats.totalPrimaryCopyCount;
    stats.totalPrimaryCopyBytes += metadata.primaryBytes;
    if (cold) {
        ++stats.coldPrimaryCopyCount;
        stats.coldPrimaryCopyBytes += metadata.primaryBytes;
    } else if (hot) {
        ++stats.hotPrimaryCopyCount;
        stats.hotPrimaryCopyBytes += metadata.primaryBytes;
    } else {
        ++stats.warmPrimaryCopyCount;
        stats.warmPrimaryCopyBytes += metadata.primaryBytes;
    }
}
}  // namespace

Status WorkerOcEvictionManager::PrecheckPolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch,
                                                     size_t migrationBatchSize, uint64_t minimumAvailableMemoryBytes,
                                                     uint64_t maximumSourceObjects, uint64_t deadlineUnixMs,
                                                     uint64_t &sourceObjects)
{
    sourceObjects = 0;
    CHECK_FAIL_RETURN_STATUS(epoch > 0, K_INVALID, "Eviction policy update epoch must be positive");
    CHECK_FAIL_RETURN_STATUS(migrationBatchSize > 0, K_INVALID, "Migration batch size must be positive");
    CHECK_FAIL_RETURN_STATUS(migrationBatchSize <= EVICTION_POLICY_MAX_MIGRATION_BATCH_SIZE, K_INVALID,
                             "Migration batch size exceeds the control-path limit");
    CHECK_FAIL_RETURN_STATUS(targetPolicy == EvictionPolicy::CLOCK || targetPolicy == EvictionPolicy::HEAT, K_INVALID,
                             "Unsupported eviction policy");
    CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::STABLE,
                             K_NOT_READY, "Another eviction policy update is in progress");
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        if (recoveredTransitionIntent_) {
            CHECK_FAIL_RETURN_STATUS(epoch == recoveredTransitionEpoch_ && targetPolicy == recoveredTargetPolicy_,
                                     K_INVALID, "A persisted eviction policy update must recover forward");
        } else {
            CHECK_FAIL_RETURN_STATUS(epoch > policyRoute_.epoch, K_INVALID, "Eviction policy update epoch is stale");
        }
        sourceObjects = policyRoute_.sourceList->Size();
    }
    if (deadlineUnixMs != 0) {
        const auto nowUnixMs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        CHECK_FAIL_RETURN_STATUS(nowUnixMs < deadlineUnixMs, K_RPC_DEADLINE_EXCEEDED,
                                 "Eviction policy update deadline has expired");
    }
    CHECK_FAIL_RETURN_STATUS(maximumSourceObjects == 0 || sourceObjects <= maximumSourceObjects, K_NO_SPACE,
                             "Eviction policy source object count exceeds the precheck limit");
    const auto availableMemory = memory::Allocator::Instance()->GetMemoryAvailToHighWater();
    CHECK_FAIL_RETURN_STATUS(availableMemory >= minimumAvailableMemoryBytes, K_NO_SPACE,
                             "Eviction policy update does not have the required memory headroom");
    policyUpdateTotalObjects_.store(sourceObjects, std::memory_order_release);
    policyUpdateRemainingObjects_.store(sourceObjects, std::memory_order_release);
    return Status::OK();
}

void WorkerOcEvictionManager::GetPolicyUpdateProgress(uint64_t &totalObjects, uint64_t &migratedObjects) const
{
    totalObjects = policyUpdateTotalObjects_.load(std::memory_order_acquire);
    const auto remaining = policyUpdateRemainingObjects_.load(std::memory_order_acquire);
    migratedObjects = totalObjects - std::min(totalObjects, remaining);
}

Status WorkerOcEvictionManager::BeginPolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch)
{
    bool phaseAcquired = false;
    Status rc = PreparePolicyUpdate(targetPolicy, epoch, phaseAcquired);
    if (rc.IsOk()) {
        rc = DrainPolicyUpdateActivity();
    }
    if (rc.IsOk()) {
        rc = InitializePolicyUpdateTarget(targetPolicy, epoch);
    }
    if (rc.IsError() && phaseAcquired) {
        ResetPolicyUpdatePhase();
    }
    return rc;
}

Status WorkerOcEvictionManager::PreparePolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch, bool &phaseAcquired)
{
    auto expected = PolicyUpdatePhase::STABLE;
    if (!policyUpdatePhase_.compare_exchange_strong(expected, PolicyUpdatePhase::DRAINING, std::memory_order_seq_cst)) {
        RETURN_STATUS(K_NOT_READY, "Another eviction policy update is in progress");
    }
    phaseAcquired = true;
    EvictionPolicy activePolicy;
    uint64_t activeEpoch;
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        if (recoveredTransitionIntent_) {
            if (epoch != recoveredTransitionEpoch_ || targetPolicy != recoveredTargetPolicy_) {
                RETURN_STATUS(K_INVALID, "A persisted eviction policy update must recover forward");
            }
        } else if (epoch <= policyRoute_.epoch) {
            RETURN_STATUS(K_INVALID, "Eviction policy update epoch is stale");
        }
        activePolicy = policyRoute_.sourcePolicy;
        activeEpoch = policyRoute_.epoch;
    }
    needsMigratableSize_.store(activePolicy == EvictionPolicy::HEAT || targetPolicy == EvictionPolicy::HEAT,
                               std::memory_order_release);
    RETURN_IF_NOT_OK(PersistTransitionIntent(activePolicy, activeEpoch, targetPolicy, epoch));
    {
        std::unique_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        recoveredTransitionIntent_ = true;
        recoveredTargetPolicy_ = targetPolicy;
        recoveredTransitionEpoch_ = epoch;
    }
    evictionCancelRequested_.store(true, std::memory_order_release);
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.BeginPolicyUpdate.afterCancelRequested", []() {});
    return Status::OK();
}

Status WorkerOcEvictionManager::DrainPolicyUpdateActivity()
{
    {
        std::unique_lock<std::mutex> lock(cvMutex_);
        if (!evictionStoppedCv_.wait_for(lock, std::chrono::seconds(POLICY_UPDATE_EVICTION_DRAIN_TIMEOUT_S),
                                         [this]() { return isDone_.load(std::memory_order_acquire); })) {
            RETURN_STATUS(K_TRY_AGAIN, "Timed out waiting for eviction to drain");
        }
    }
    {
        std::unique_lock<std::mutex> lock(primaryEndLifeMutex_);
        if (!primaryEndLifeDrainedCv_.wait_for(
            lock, std::chrono::seconds(POLICY_UPDATE_EVICTION_DRAIN_TIMEOUT_S), [this]() {
                const bool queuesDrained = primaryEndLifeReadyQueue_.empty() && delayedPrimaryEndLifeQueue_.empty();
                const bool ownersDrained = primaryEndLifeOwnerLanes_.empty() && activeDrainWorkers_ == 0;
                return queuesDrained && ownersDrained && pendingPrimaryEndLifeObjects_.empty();
            })) {
            RETURN_STATUS(K_TRY_AGAIN, "Timed out waiting for primary end-life tasks to drain");
        }
    }
    {
        std::unique_lock<std::mutex> lock(cvMutex_);
        if (!stableRouteReadersCv_.wait_for(lock, std::chrono::seconds(POLICY_UPDATE_EVICTION_DRAIN_TIMEOUT_S),
                                            [this]() { return StableRouteReadersDrained(); })) {
            RETURN_STATUS(K_TRY_AGAIN, "Timed out waiting for active eviction route readers");
        }
    }
    return Status::OK();
}

Status WorkerOcEvictionManager::InitializePolicyUpdateTarget(EvictionPolicy targetPolicy, uint64_t epoch)
{
    std::unique_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    if (epoch <= policyRoute_.epoch) {
        RETURN_STATUS(K_INVALID, "Eviction policy update epoch is stale");
    }
    if (targetPolicy == policyRoute_.sourcePolicy) {
        routeLock.unlock();
        Status rc = PersistLastGood(targetPolicy, epoch);
        if (rc.IsError()) {
            return rc;
        }
        routeLock.lock();
        recoveredTransitionIntent_ = false;
        recoveredTransitionEpoch_ = 0;
        policyRoute_.epoch = epoch;
        policyUpdateTotalObjects_.store(0, std::memory_order_release);
        policyUpdateRemainingObjects_.store(0, std::memory_order_release);
        evictionCancelRequested_.store(false, std::memory_order_release);
        policyUpdatePhase_.store(PolicyUpdatePhase::STABLE, std::memory_order_release);
        needsMigratableSize_.store(targetPolicy == EvictionPolicy::HEAT, std::memory_order_release);
        return Status::OK();
    }
    auto *targetList = policyRoute_.sourceList == &memEvictionList_ ? &alternateEvictionList_ : &memEvictionList_;
    if (targetList->Size() != 0) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Inactive eviction list is not empty");
    }
    const auto targetHeatConfig = GetCurrentHeatPolicyConfig();
    std::shared_ptr<EvictionStrategy> targetStrategy;
    try {
        targetStrategy = MakeEvictionStrategy(targetPolicy, *targetList, objectTable_, gRefTable_, targetHeatConfig);
    } catch (const std::exception &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Failed to create target eviction policy: %s", e.what()));
    } catch (...) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Failed to create target eviction policy");
    }
    policyRoute_.epoch = epoch;
    policyRoute_.targetPolicy = targetPolicy;
    policyRoute_.targetList = targetList;
    policyRoute_.targetStrategy = std::move(targetStrategy);
    policyRoute_.targetHeatConfig = targetHeatConfig;
    const auto sourceSize = policyRoute_.sourceList->Size();
    policyUpdateTotalObjects_.store(sourceSize, std::memory_order_release);
    policyUpdateRemainingObjects_.store(sourceSize, std::memory_order_release);
    policyUpdatePhase_.store(PolicyUpdatePhase::MIGRATING, std::memory_order_release);
    LOG(INFO) << "Started eviction policy update, epoch: " << epoch
              << ", source policy: " << static_cast<int>(policyRoute_.sourcePolicy)
              << ", target policy: " << static_cast<int>(targetPolicy)
              << ", source nodes: " << policyRoute_.sourceList->Size();
    return Status::OK();
}

void WorkerOcEvictionManager::ResetPolicyUpdatePhase()
{
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        needsMigratableSize_.store(policyRoute_.sourcePolicy == EvictionPolicy::HEAT, std::memory_order_release);
    }
    evictionCancelRequested_.store(false, std::memory_order_release);
    policyUpdatePhase_.store(PolicyUpdatePhase::STABLE, std::memory_order_release);
}

Status WorkerOcEvictionManager::MoveOnePolicyNode(const std::string &objectKey)
{
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    std::lock_guard<std::mutex> keyLock(GetPolicyMigrationLock(objectKey));
    EvictionList::Node sourceNode;
    Status rc = policyRoute_.sourceList->Extract(objectKey, sourceNode);
    if (rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    EvictionList::Node targetNode;
    try {
        INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.MoveOnePolicyNode.beforeConvert",
                               [] { throw std::bad_alloc(); });
        rc = ConvertPolicySnapshot(sourceNode, policyRoute_.sourcePolicy, policyRoute_.targetPolicy,
                                   policyRoute_.targetHeatConfig, targetNode);
    } catch (const std::exception &e) {
        rc = Status(K_RUNTIME_ERROR, FormatString("Failed to convert eviction policy snapshot: %s", e.what()));
    } catch (...) {
        rc = Status(K_RUNTIME_ERROR, "Failed to convert eviction policy snapshot");
    }
    if (rc.IsOk()) {
        bool inserted = false;
        const auto heatMergeMode =
            policyRoute_.sourcePolicy == EvictionPolicy::CLOCK && policyRoute_.targetPolicy == EvictionPolicy::HEAT
                ? EvictionList::HeatMergeMode::ADD_CAPPED
                : EvictionList::HeatMergeMode::PRESERVE_MAX;
        rc = policyRoute_.targetList->InsertOrMerge(targetNode,
                                                    policyRoute_.targetPolicy == EvictionPolicy::HEAT
                                                        ? EvictionList::StateKind::HEAT
                                                        : EvictionList::StateKind::CLOCK,
                                                    policyRoute_.targetHeatConfig.maxCounter, heatMergeMode,
                                                    inserted);
    }
    if (rc.IsError()) {
        Status restoreRc = policyRoute_.sourceList->Restore(sourceNode);
        if (restoreRc.IsError()) {
            LOG(ERROR) << "Failed to roll back source eviction node after target insertion failed: "
                       << restoreRc.ToString();
            return restoreRc;
        }
    }
    return rc;
}

Status WorkerOcEvictionManager::MigratePolicyBatch(size_t maxKeys, bool &done)
{
    done = false;
    CHECK_FAIL_RETURN_STATUS(maxKeys > 0, K_INVALID, "Migration batch size must be positive");
    CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::MIGRATING,
                             K_NOT_READY, "Eviction policy update is not migrating");
    std::vector<EvictionList::Node> nodes;
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        RETURN_IF_NOT_OK(policyRoute_.sourceList->GetObjectsInfoFromOldest(maxKeys, nodes));
    }
    for (const auto &node : nodes) {
        RETURN_IF_NOT_OK(MoveOnePolicyNode(node.objectKey));
    }
    size_t sourceRemaining = 0;
    size_t targetSize = 0;
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        sourceRemaining = policyRoute_.sourceList->Size();
        targetSize = policyRoute_.targetList->Size();
        done = sourceRemaining == 0;
    }
    policyUpdateRemainingObjects_.store(sourceRemaining, std::memory_order_release);
    if (done) {
        policyUpdatePhase_.store(PolicyUpdatePhase::VERIFYING, std::memory_order_release);
    }
    LOG_EVERY_T(INFO, POLICY_UPDATE_PROGRESS_LOG_INTERVAL_S)
        << "Eviction policy update progress, epoch: " << GetPolicyUpdateEpoch()
                          << ", source remaining: " << sourceRemaining << ", target nodes: " << targetSize;
    return Status::OK();
}

bool WorkerOcEvictionManager::IsEligibleEvictionMembership(const ObjectInterface &object)
{
    if (object.stateInfo.IsCacheInvalid() || object.stateInfo.IsIncomplete() || object.stateInfo.IsNeedToDelete()
        || object.IsInvalid()) {
        return false;
    }
    return object.IsBinary() ? object.GetShmUnit() != nullptr : object.HasL2Cache();
}

Status WorkerOcEvictionManager::AuditPolicyUpdateMembership(uint64_t epoch, uint64_t &auditedGeneration)
{
    CHECK_FAIL_RETURN_STATUS(objectTable_ != nullptr, K_RUNTIME_ERROR,
                             "Object table is unavailable for eviction policy audit");
    CHECK_FAIL_RETURN_STATUS(policyMutationWriters_.load(std::memory_order_acquire) == 0, K_TRY_AGAIN,
                             "Eviction policy membership changed during audit");
    auditedGeneration = policyMutationGeneration_.load(std::memory_order_acquire);

    std::pair<EvictionList *, EvictionList *> auditLists{ nullptr, nullptr };
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::VERIFYING,
                                 K_NOT_READY, "Eviction policy update is not ready for audit");
        CHECK_FAIL_RETURN_STATUS(epoch == policyRoute_.epoch, K_INVALID, "Eviction policy update epoch mismatch");
        CHECK_FAIL_RETURN_STATUS(policyRoute_.sourceList->Size() == 0, K_NOT_READY,
                                 "Source eviction list is not empty");
        auditLists = { policyRoute_.sourceList, policyRoute_.targetList };
    }
    CHECK_FAIL_RETURN_STATUS(auditLists.second != nullptr, K_NOT_READY,
                             "Target eviction list is unavailable for audit");
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.AuditPolicyUpdateMembership.afterRouteSnapshot", []() {});

    // SafeTable iteration fences insert/erase. Hold that table lock only while taking shared ownership of the current
    // rows; object latches and policy-list checks can then run without blocking unrelated object-table mutations.
    auto objectSnapshot = TakeObjectSnapshot(objectTable_);
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.AuditPolicyUpdateMembership.afterObjectSnapshot", []() {});

    size_t eligibleObjects = 0;
    // Use a non-blocking row lock: concurrent lifecycle changes invalidate this audit through the row status or the
    // policy-mutation generation fence instead of stalling the whole verification pass behind a foreground writer.
    for (const auto &[objectKey, entry] : objectSnapshot) {
        if (entry == nullptr || entry->TryRLock(true).IsError()) {
            RETURN_STATUS(K_TRY_AGAIN, "Object changed while auditing eviction policy membership");
        }
        Raii entryUnlock([&entry]() { entry->RUnlock(); });
        const auto *object = entry->Get();
        if (object == nullptr || !IsEligibleEvictionMembership(*object)) {
            continue;
        }
        std::lock_guard<std::mutex> keyLock(GetPolicyMigrationLock(objectKey));
        CHECK_FAIL_RETURN_STATUS(auditLists.second->Exist(objectKey), K_NOT_READY,
                                 "Eligible object is missing from target eviction list");
        ++eligibleObjects;
    }
    const size_t targetObjects = auditLists.second->Size();

    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::VERIFYING
                                     && epoch == policyRoute_.epoch && auditLists.first == policyRoute_.sourceList
                                     && auditLists.second == policyRoute_.targetList && auditLists.first->Size() == 0,
                                 K_TRY_AGAIN, "Eviction policy route changed during audit");
        CHECK_FAIL_RETURN_STATUS(policyMutationWriters_.load(std::memory_order_acquire) == 0
                                     && policyMutationGeneration_.load(std::memory_order_acquire) == auditedGeneration,
                                 K_TRY_AGAIN, "Eviction policy membership changed during audit");
    }
    CHECK_FAIL_RETURN_STATUS(targetObjects == eligibleObjects, K_NOT_READY,
                             FormatString("Target eviction membership mismatch, target: %zu, eligible: %zu",
                                          targetObjects, eligibleObjects));
    return Status::OK();
}

Status WorkerOcEvictionManager::CommitPolicyUpdate(uint64_t epoch)
{
    uint64_t auditedGeneration = 0;
    RETURN_IF_NOT_OK(AuditPolicyUpdateMembership(epoch, auditedGeneration));
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.CommitPolicyUpdate.afterAudit", []() {});
    std::unique_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::VERIFYING,
                             K_NOT_READY, "Eviction policy update is not ready to commit");
    CHECK_FAIL_RETURN_STATUS(epoch == policyRoute_.epoch, K_INVALID, "Eviction policy update epoch mismatch");
    CHECK_FAIL_RETURN_STATUS(policyMutationGeneration_.load(std::memory_order_acquire) == auditedGeneration,
                             K_TRY_AGAIN, "Eviction policy membership changed after audit");
    CHECK_FAIL_RETURN_STATUS(policyRoute_.sourceList->Size() == 0, K_NOT_READY, "Source eviction list is not empty");
    RETURN_IF_NOT_OK(policyRoute_.sourceList->ValidateMembership(policyRoute_.sourcePolicy == EvictionPolicy::HEAT
                                                                     ? EvictionList::StateKind::HEAT
                                                                     : EvictionList::StateKind::CLOCK));
    RETURN_IF_NOT_OK(policyRoute_.targetList->ValidateMembership(policyRoute_.targetPolicy == EvictionPolicy::HEAT
                                                                     ? EvictionList::StateKind::HEAT
                                                                     : EvictionList::StateKind::CLOCK));
    policyUpdatePhase_.store(PolicyUpdatePhase::ACTIVATING, std::memory_order_release);
    const auto targetPolicy = policyRoute_.targetPolicy;
    routeLock.unlock();
    Status rc = PersistLastGood(targetPolicy, epoch);
    routeLock.lock();
    if (rc.IsError()) {
        policyUpdatePhase_.store(PolicyUpdatePhase::VERIFYING, std::memory_order_release);
        return rc;
    }
    CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::ACTIVATING,
                             K_NOT_READY, "Eviction policy update activation was interrupted");
    CHECK_FAIL_RETURN_STATUS(epoch == policyRoute_.epoch && targetPolicy == policyRoute_.targetPolicy, K_INVALID,
                             "Eviction policy update changed during activation");
    recoveredTransitionIntent_ = false;
    recoveredTransitionEpoch_ = 0;
    policyRoute_.sourcePolicy = policyRoute_.targetPolicy;
    needsMigratableSize_.store(policyRoute_.sourcePolicy == EvictionPolicy::HEAT, std::memory_order_release);
    policyRoute_.sourceList = policyRoute_.targetList;
    policyRoute_.sourceStrategy = std::move(policyRoute_.targetStrategy);
    policyRoute_.sourceHeatConfig = policyRoute_.targetHeatConfig;
    policyRoute_.targetList = nullptr;
    policyUpdateRemainingObjects_.store(0, std::memory_order_release);
    evictionCancelRequested_.store(false, std::memory_order_release);
    policyUpdatePhase_.store(PolicyUpdatePhase::STABLE, std::memory_order_release);
    LOG(INFO) << "Committed eviction policy update, epoch: " << epoch
              << ", active policy: " << static_cast<int>(policyRoute_.sourcePolicy)
              << ", active nodes: " << policyRoute_.sourceList->Size();
    return Status::OK();
}

Status WorkerOcEvictionManager::HandlePolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch, size_t maxKeys,
                                                   bool &complete)
{
    complete = false;
    auto state = GetPolicyStateSnapshot();
    if (state.phase != PolicyUpdatePhase::STABLE) {
        CHECK_FAIL_RETURN_STATUS(epoch == state.epoch && targetPolicy == state.targetPolicy, K_INVALID,
                                 "Eviction policy update does not match the active transition");
    }
    if (state.phase == PolicyUpdatePhase::STABLE) {
        if (state.activePolicy == targetPolicy) {
            if (state.epoch < epoch) {
                RETURN_IF_NOT_OK(BeginPolicyUpdate(targetPolicy, epoch));
                state = GetPolicyStateSnapshot();
            }
            complete = state.epoch >= epoch;
            return Status::OK();
        }
        RETURN_IF_NOT_OK(BeginPolicyUpdate(targetPolicy, epoch));
        state = GetPolicyStateSnapshot();
    }
    if (state.phase == PolicyUpdatePhase::MIGRATING) {
        bool migrated = false;
        RETURN_IF_NOT_OK(MigratePolicyBatch(maxKeys, migrated));
        state = GetPolicyStateSnapshot();
    }
    if (state.phase == PolicyUpdatePhase::VERIFYING) {
        RETURN_IF_NOT_OK(CommitPolicyUpdate(epoch));
        state = GetPolicyStateSnapshot();
    }
    complete = state.phase == PolicyUpdatePhase::STABLE && state.activePolicy == targetPolicy && state.epoch == epoch;
    return Status::OK();
}

Status WorkerOcEvictionManager::MaintainHeatAndCollectHotPrimaryStats(CopyWatermarkStats &result)
{
    result = {};
    result.policy = EvictionPolicy::HEAT;
    StableRouteReadGuard stableRoute(*this);
    if (!stableRoute || policyRoute_.sourcePolicy != EvictionPolicy::HEAT || objectTable_ == nullptr) {
        return Status::OK();
    }
    auto stats = policyRoute_.sourceList->DecayAllAndCollect(
        policyRoute_.sourceHeatConfig.halfLifePrimaryS, policyRoute_.sourceHeatConfig.halfLifeLocalS,
        GetEvictionHeatConfig().threshold, GetRebalanceHeatConfig().hotCounterThreshold,
        [this](const std::string &objectKey) { return ResolveHeatNodeMetadata(objectKey); });
    if (policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE) {
        return Status::OK();
    }
    result.coldPrimaryCopyCount = stats.coldPrimaryCopyCount;
    result.warmPrimaryCopyCount = stats.warmPrimaryCopyCount;
    result.hotPrimaryCopyCount = stats.hotPrimaryCopyCount;
    result.totalPrimaryCopyCount = stats.totalPrimaryCopyCount;
    result.coldPrimaryCopyBytes = stats.coldPrimaryCopyBytes;
    result.warmPrimaryCopyBytes = stats.warmPrimaryCopyBytes;
    result.hotPrimaryCopyBytes = stats.hotPrimaryCopyBytes;
    result.totalPrimaryCopyBytes = stats.totalPrimaryCopyBytes;
    return Status::OK();
}

Status WorkerOcEvictionManager::CollectPrimaryCopyStats(uint64_t &totalPrimaryCopyCount,
                                                        uint64_t &totalPrimaryCopyBytes)
{
    CopyWatermarkStats stats;
    RETURN_IF_NOT_OK(CollectCopyWatermarkStats(stats, false));
    totalPrimaryCopyCount = stats.totalPrimaryCopyCount;
    totalPrimaryCopyBytes = stats.totalPrimaryCopyBytes;
    return Status::OK();
}

void WorkerOcEvictionManager::SetCopyWatermarkObserver(CopyWatermarkObserver observer)
{
    auto snapshot = observer ? std::make_shared<const CopyWatermarkObserver>(std::move(observer)) : nullptr;
    std::atomic_store_explicit(&copyWatermarkObserver_, std::move(snapshot), std::memory_order_release);
}

void WorkerOcEvictionManager::SetHotPrimaryReportObserver(CopyWatermarkObserver observer)
{
    auto snapshot = observer ? std::make_shared<const CopyWatermarkObserver>(std::move(observer)) : nullptr;
    std::atomic_store_explicit(&hotPrimaryReportObserver_, std::move(snapshot), std::memory_order_release);
}

Status WorkerOcEvictionManager::CollectCopyWatermarkStats(CopyWatermarkStats &stats, bool collectCounterDistribution)
{
    stats = {};
    if (objectTable_ == nullptr) {
        return Status::OK();
    }
    StableRouteReadGuard stableRoute(*this);
    if (!stableRoute) {
        RETURN_STATUS(K_NOT_READY, "Copy watermark is unavailable during eviction policy update");
    }
    CopyWatermarkStats collected;
    collected.policy = policyRoute_.sourcePolicy;
    const double heatCap = policyRoute_.sourceHeatConfig.maxCounter;
    std::vector<EvictionList::Node> nodes;
    EvictionList::Node oldest;
    RETURN_IF_NOT_OK(policyRoute_.sourceList->GetAllObjectsInfo(nodes, oldest));

    std::vector<double> primaryCounters;
    if (collectCounterDistribution) {
        primaryCounters.reserve(nodes.size());
    }
    for (const auto &node : nodes) {
        const auto metadata = ResolveHeatNodeMetadata(node.objectKey);
        if (!metadata.includeInPrimaryStats) {
            continue;
        }
        const double counter = collected.policy == EvictionPolicy::HEAT ? node.heat : node.curCounter;
        const bool cold = collected.policy == EvictionPolicy::HEAT
                              ? counter < GetEvictionHeatConfig().threshold
                              : node.curCounter == 0;
        const bool hot = collected.policy == EvictionPolicy::HEAT
                             ? counter > GetRebalanceHeatConfig().hotCounterThreshold
                             : node.curCounter >= Q2;
        RecordPrimaryCopyTemperature(collected, metadata, cold, hot);
        if (collectCounterDistribution) {
            primaryCounters.emplace_back(counter);
        }
        if (collectCounterDistribution && collected.policy == EvictionPolicy::HEAT && counter >= heatCap) {
            ++collected.cappedPrimaryCopyCount;
        }
    }
    CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::STABLE,
                             K_NOT_READY, "Copy watermark changed during eviction policy update");
    if (collectCounterDistribution && !primaryCounters.empty()) {
        FillCounterPercentiles(collected, primaryCounters);
    }
    stats = std::move(collected);
    return Status::OK();
}

void WorkerOcEvictionManager::NotifyCopyWatermarkObserver()
{
    auto observer = std::atomic_load_explicit(&copyWatermarkObserver_, std::memory_order_acquire);
    if (!observer) {
        return;
    }
    CopyWatermarkStats stats;
    auto rc = CollectCopyWatermarkStats(stats, true);
    if (rc.IsError()) {
        VLOG(1) << "Skip copy watermark refresh: " << rc.ToString();
        return;
    }
    (*observer)(stats);
}

void WorkerOcEvictionManager::RefreshCopyWatermarkSnapshot()
{
    NotifyCopyWatermarkObserver();
}

void WorkerOcEvictionManager::RefreshHotPrimaryReport()
{
    auto observer = std::atomic_load_explicit(&hotPrimaryReportObserver_, std::memory_order_acquire);
    if (!observer) {
        return;
    }
    CopyWatermarkStats stats;
    auto rc = CollectCopyWatermarkStats(stats, false);
    if (rc.IsError()) {
        VLOG(1) << "Skip post-rebalance hot-primary refresh: " << rc.ToString();
        return;
    }
    if (stats.policy == EvictionPolicy::HEAT) {
        (*observer)(stats);
    }
}

EvictionList::HeatNodeMetadata WorkerOcEvictionManager::ResolveHeatNodeMetadata(const std::string &objectKey) const
{
    EvictionList::HeatNodeMetadata metadata;
    std::shared_ptr<SafeObjType> entry;
    if (objectTable_ == nullptr || objectTable_->Get(objectKey, entry).IsError() || entry == nullptr
        || entry->TryRLock(true).IsError()) {
        return metadata;
    }
    Raii unlock([&entry]() { entry->RUnlock(); });
    auto *object = entry->Get();
    if (object == nullptr) {
        return metadata;
    }
    metadata.resolved = true;
    metadata.isPrimary = object->stateInfo.IsPrimaryCopy();
    const bool stablePrimary = metadata.isPrimary && !object->stateInfo.IsCacheInvalid()
                               && !object->stateInfo.IsIncomplete() && !object->stateInfo.IsNeedToDelete()
                               && !object->IsInvalid();
    if (stablePrimary) {
        auto shmUnit = object->GetShmUnit();
        metadata.primaryBytes = shmUnit == nullptr ? 0 : shmUnit->GetMigratableSize();
        metadata.includeInPrimaryStats = metadata.primaryBytes > 0;
    }
    return metadata;
}

}  // namespace object_cache
}  // namespace datasystem
