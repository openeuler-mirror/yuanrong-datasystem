/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Implementation of EvictionList and EvictionManager.
 */
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"

#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/flags/eviction_watermark.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <exception>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/object_cache/eviction_policy_common.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/eviction_strategy.h"
#include "datasystem/worker/object_cache/kv_event/kv_event_publisher.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/object_endpoint_policy.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

DS_DECLARE_uint32(eviction_reserve_mem_threshold_mb);
DS_DECLARE_uint32(spill_thread_num);

#ifdef WITH_TESTS
constexpr uint32_t MASTER_TASK_THREAD_NUM = 4;
#else
constexpr uint32_t MASTER_TASK_THREAD_NUM = 8;
#endif

constexpr uint32_t SPILL_EVICT_THREAD_NUM = 1;
constexpr uint32_t MEM_EVICT_THREAD_NUM = 1;
// Number of concurrent drain workers for primary end-life tasks. End-life involves
// master RPC (DeleteAllCopyMeta) which is the eviction throughput bottleneck under
// high write load (issue #750). Multiple workers drain the queue in parallel so the
// pending set (limit 64) turns over fast enough to keep up with EvictionTask.
constexpr uint32_t PRIMARY_END_LIFE_THREAD_NUM = 4;

namespace datasystem {
namespace object_cache {
namespace {
EvictionCandidate MakeEvictionCandidate(EvictionPolicy policy, const EvictionList::Node &snapshot)
{
    EvictionCandidate candidate;
    candidate.objectKey = snapshot.objectKey;
    candidate.policy = policy;
    candidate.heat = snapshot.heat;
    candidate.generation = snapshot.generation;
    candidate.heatUpdateSeq = snapshot.heatUpdateSeq;
    return candidate;
}
}  // namespace
static constexpr int DEBUG_LOG_LEVEL = 1;
static constexpr int BATCH_DELETE_META_THRESHOLD = 300;
static constexpr int BATCH_DELETE_META_MAX_DELAY_MS = 10;
static constexpr size_t PRIMARY_END_LIFE_PENDING_LIMIT = 1024;
static constexpr size_t PRIMARY_END_LIFE_BATCH_LIMIT = 64;
static constexpr int PRIMARY_END_LIFE_BATCH_MAX_DELAY_MS = 10;
static constexpr int64_t PRIMARY_END_LIFE_DELETE_ALL_COPY_TIMEOUT_MS = 1000;
static constexpr int64_t PRIMARY_END_LIFE_SLOW_LOG_THRESHOLD_MS = 100;
static constexpr uint32_t PRIMARY_END_LIFE_DELETE_ALL_COPY_RETRY_TIMES = 3;
static constexpr uint32_t PRIMARY_END_LIFE_LOCK_RETRY_TIMES = 3;
static constexpr int PRIMARY_END_LIFE_LOCK_RETRY_INTERVAL_MS = 1;

constexpr uint64_t EVICTION_TRACE_SUMMARY_INTERVAL_MS = 60 * SECS_TO_MS;
constexpr size_t EVICTION_TRACE_SUMMARY_THRESHOLD = 32;
namespace {
std::string JoinKeys(const std::vector<std::string> &keys)
{
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << keys[i];
    }
    ss << "]";
    return ss.str();
}
}  // namespace

WorkerOcEvictionManager::EvictionTrace::~EvictionTrace()
{
    if (aggregator_ != nullptr) {
        aggregator_->Add(*this);
    } else {
        // Fallback for code paths that don't set aggregator_.
        static thread_local EvictionTraceAggregator aggregator;
        aggregator.Add(*this);
    }
}

WorkerOcEvictionManager::EvictionTraceAggregator::~EvictionTraceAggregator()
{
    for (auto &item : summaries_) {
        Flush(item.first, item.second);
    }
}

void WorkerOcEvictionManager::EvictionTraceAggregator::Add(const EvictionTrace &trace)
{
    auto elapsed = trace.timer.ElapsedMilliSecond();
    auto buildTraceLog = [&trace, elapsed]() {
        auto actionName = GetActionName(trace.action);
        std::stringstream ss;
        ss << "[TaskId " << trace.taskId << "] ";
        if (!trace.info.empty()) {
            ss << trace.info << ", ";
        }
        ss << "evict action " << actionName << ", total cost " << elapsed << " ms, "
           << "obj size: " << trace.objectSize;
        if (trace.action == Action::SPILL) {
            ss << "spill cost " << trace.spillCost << " ms, ";
        }
        ss << "status:" << (trace.rc.IsOk() ? "OK" : trace.rc.GetMsg());
        return ss.str();
    };

    auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto &summary = summaries_[trace.action];
    if (summary.lastLogTimeMs == 0) {
        summary.lastLogTimeMs = nowMs;
    }
    auto statusCode = trace.rc.GetCode();
    if (statusCode == K_TRY_AGAIN || statusCode == K_NOT_READY) {
        if (statusCode == K_TRY_AGAIN) {
            summary.tryAgainCount += trace.objectKeySizeMap.size();
        } else {
            summary.notReadyCount += trace.objectKeySizeMap.size();
        }
        LOG_EVERY_T(WARNING, LOG_TIME_LIMIT_LEVEL2) << buildTraceLog();
        FlushIfNeeded(trace.action, summary, nowMs);
        return;
    }

    if (elapsed > 1 || trace.rc.IsError()) {
        LOG(INFO) << buildTraceLog();
    }
    if (!trace.objectKeySizeMap.empty()) {
        auto &keys = trace.rc.IsOk() ? summary.successKeys : summary.failedKeys;
        keys.reserve(keys.size() + trace.objectKeySizeMap.size());
        for (const auto &item : trace.objectKeySizeMap) {
            keys.emplace_back(item.first);
        }
    } else {
        if (trace.rc.IsOk()) {
            summary.successKeys.emplace_back(trace.taskId);
        } else {
            summary.failedKeys.emplace_back(trace.taskId);
        }
    }
    FlushIfNeeded(trace.action, summary, nowMs);
}

void WorkerOcEvictionManager::EvictionTraceAggregator::FlushIfNeeded(Action action, ActionSummary &summary,
                                                                     uint64_t nowMs)
{
    // Exclude tryAgainCount and notReadyCount from keyCount to avoid
    // frequent summary log flushing caused by transient failures.
    auto keyCount = summary.successKeys.size() + summary.failedKeys.size();
    if (keyCount >= EVICTION_TRACE_SUMMARY_THRESHOLD
        || nowMs - summary.lastLogTimeMs >= EVICTION_TRACE_SUMMARY_INTERVAL_MS) {
        Flush(action, summary);
    }
}

void WorkerOcEvictionManager::EvictionTraceAggregator::Flush(Action action, ActionSummary &summary)
{
    if (summary.successKeys.empty() && summary.failedKeys.empty()) {
        return;
    }
    LOG(INFO) << "Evict summary, action " << GetActionName(action)
              << ", success key count: " << summary.successKeys.size()
              << ", success keys: " << JoinKeys(summary.successKeys)
              << ", failed key count: " << summary.failedKeys.size()
              << ", failed keys: " << JoinKeys(summary.failedKeys)
              << ", try again count: " << summary.tryAgainCount << ", not ready count: " << summary.notReadyCount;
    summary.successKeys.clear();
    summary.failedKeys.clear();
    summary.tryAgainCount = 0;
    summary.notReadyCount = 0;
    summary.lastLogTimeMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
}

const std::unordered_map<WorkerOcEvictionManager::Action, WorkerOcEvictionManager::ActionSummary> &
WorkerOcEvictionManager::EvictionTraceAggregator::GetSummaries() const
{
    return summaries_;
}

WorkerOcEvictionManager::WorkerOcEvictionManager(std::shared_ptr<ObjectTable> objectTable, HostPort localAddress,
                                                 HostPort masterAddress,
                                                 const worker::MetadataRouteResolver &metadataRoute,
                                                 master::MasterOCServiceImpl *masterOc)
    : objectTable_(std::move(objectTable)),
      localAddress_(std::move(localAddress)),
      masterAddress_(std::move(masterAddress)),
      isDone_(true),
      masterOc_(masterOc),
      metadataRoute_(metadataRoute)
{
    const auto policy = GetEvictionStrategy() == "heat" ? EvictionPolicy::HEAT : EvictionPolicy::CLOCK;
    policyRoute_.sourceHeatConfig = GetCurrentHeatPolicyConfig();
    policyRoute_.sourcePolicy = policy;
    needsMigratableSize_.store(policy == EvictionPolicy::HEAT, std::memory_order_release);
    policyRoute_.sourceList = &memEvictionList_;
    policyRoute_.sourceStrategy = MakeEvictionStrategy(
        policy, memEvictionList_, objectTable_, gRefTable_, policyRoute_.sourceHeatConfig);
}

WorkerOcEvictionManager::~WorkerOcEvictionManager()
{
    LOG(INFO) << "WorkerOcEvictionManager exit";
    // The scheduler calls into the eviction pools, so stop and join it before
    // releasing any dependency it may still access.
    scheduleEvictionRunning_.store(false, std::memory_order_release);
    scheduleEvictThreadPool_.reset();
    memEvictTaskThreadPool_.reset();
    primaryEndLifeThreadPool_.reset();
    spillEvictTaskThreadPool_.reset();
    spillTaskThreadPool_.reset();
    masterTaskThreadPool_.reset();
}

Status WorkerOcEvictionManager::InitPolicyStateStore(PolicyStateLoader loader, PolicyStateStorer storer)
{
    CHECK_FAIL_RETURN_STATUS(loader != nullptr && storer != nullptr, K_INVALID,
                             "Eviction policy state store callbacks must be configured");
    CHECK_FAIL_RETURN_STATUS(memEvictionList_.Size() == 0 && alternateEvictionList_.Size() == 0, K_NOT_READY,
                             "Eviction policy state must be restored before memberships are added");

    PersistedPolicyState state;
    bool found = false;
    RETURN_IF_NOT_OK(loader(state, found));
    if (found) {
        CHECK_FAIL_RETURN_STATUS(
            state.activePolicy == EvictionPolicy::CLOCK || state.activePolicy == EvictionPolicy::HEAT, K_INVALID,
            "Persisted active eviction policy is invalid");
        CHECK_FAIL_RETURN_STATUS(
            !state.hasTransitionIntent
                || (state.transitionEpoch > state.activeEpoch
                    && (state.targetPolicy == EvictionPolicy::CLOCK || state.targetPolicy == EvictionPolicy::HEAT)),
            K_INVALID, "Persisted eviction policy transition intent is invalid");
    } else {
        state.activePolicy = policyRoute_.sourcePolicy;
    }

    {
        std::unique_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        CHECK_FAIL_RETURN_STATUS(policyStateLoader_ == nullptr && policyStateStorer_ == nullptr, K_INVALID,
                                 "Eviction policy state store is already initialized");
        policyStateLoader_ = std::move(loader);
        policyStateStorer_ = std::move(storer);
        policyRoute_.sourcePolicy = state.activePolicy;
        needsMigratableSize_.store(state.activePolicy == EvictionPolicy::HEAT, std::memory_order_release);
        policyRoute_.epoch = state.activeEpoch;
        policyRoute_.sourceHeatConfig = GetCurrentHeatPolicyConfig();
        policyRoute_.sourceStrategy = MakeEvictionStrategy(
            state.activePolicy, *policyRoute_.sourceList, objectTable_, gRefTable_, policyRoute_.sourceHeatConfig);
        recoveredTransitionIntent_ = state.hasTransitionIntent;
        recoveredTargetPolicy_ = state.targetPolicy;
        recoveredTransitionEpoch_ = state.transitionEpoch;
    }
    if (!found) {
        RETURN_IF_NOT_OK(PersistPolicyState(state));
    }
    if (state.hasTransitionIntent) {
        LOG(WARNING) << "Recovered unfinished eviction policy update intent, active epoch: " << state.activeEpoch
                     << ", transition epoch: " << recoveredTransitionEpoch_
                     << ", target policy: " << static_cast<int>(recoveredTargetPolicy_);
    }
    return Status::OK();
}

Status WorkerOcEvictionManager::PersistPolicyState(const PersistedPolicyState &state) const
{
    if (policyStateStorer_ == nullptr) {
        return Status::OK();
    }
    return policyStateStorer_(state);
}

Status WorkerOcEvictionManager::PersistTransitionIntent(EvictionPolicy activePolicy, uint64_t activeEpoch,
                                                        EvictionPolicy targetPolicy, uint64_t epoch)
{
    PersistedPolicyState state;
    state.activePolicy = activePolicy;
    state.activeEpoch = activeEpoch;
    state.hasTransitionIntent = true;
    state.targetPolicy = targetPolicy;
    state.transitionEpoch = epoch;
    return PersistPolicyState(state);
}

Status WorkerOcEvictionManager::PersistLastGood(EvictionPolicy activePolicy, uint64_t epoch)
{
    PersistedPolicyState state;
    state.activePolicy = activePolicy;
    state.activeEpoch = epoch;
    return PersistPolicyState(state);
}

void WorkerOcEvictionManager::BeginTrackedPolicyMutation()
{
    policyMutationWriters_.fetch_add(1, std::memory_order_acq_rel);
    policyMutationGeneration_.fetch_add(1, std::memory_order_acq_rel);
}

void WorkerOcEvictionManager::EndTrackedPolicyMutation()
{
    policyMutationWriters_.fetch_sub(1, std::memory_order_acq_rel);
}

Status WorkerOcEvictionManager::Init(const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &gRefTable,
                                     std::shared_ptr<AkSkManager> akSkManager)
{
    RETURN_IF_EXCEPTION_OCCURS(memEvictTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(MEM_EVICT_THREAD_NUM, 0, "MemEvictionThread"));
    RETURN_IF_EXCEPTION_OCCURS(primaryEndLifeThreadPool_ = std::make_unique<ThreadPool>(PRIMARY_END_LIFE_THREAD_NUM, 0,
                                                                                        "PrimaryEndLifeThread"));
    RETURN_IF_EXCEPTION_OCCURS(spillEvictTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(SPILL_EVICT_THREAD_NUM, 0, "SpillEvictionThread"));
    RETURN_IF_EXCEPTION_OCCURS(masterTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(MASTER_TASK_THREAD_NUM, 0, "MasterTaskThread"));
    RETURN_IF_EXCEPTION_OCCURS(spillTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(FLAGS_spill_thread_num, 0, "SpillThread"));
    RETURN_IF_EXCEPTION_OCCURS(scheduleEvictThreadPool_ = std::make_unique<ThreadPool>(1, 0, "scheduleEvictThread"));
    // reduce warn log output when thread pool is almost full
    spillTaskThreadPool_->SetWarnLevel(ThreadPool::WarnLevel::LOW);
    RETURN_IF_NOT_OK(WorkerOcSpill::Instance()->Init());
    gRefTable_ = gRefTable;
    akSkManager_ = std::move(akSkManager);
    scheduleEvictionRunning_.store(true, std::memory_order_release);
    scheduleEvictThreadPool_->Submit([this]() {
        Trace::Instance().SetTraceNewID("EvictionTimer;" + GetStringUuid(), true);
        Timer timer;
        while (scheduleEvictionRunning_.load(std::memory_order_acquire) && !IsTermSignalReceived()) {
            auto evictInterval = 10;
            if (timer.ElapsedSecond() > evictInterval) {
                EvictWhenMemoryExceedThrehold("", 0, shared_from_this(), ServiceType::OBJECT);
                EvictWhenMemoryExceedThrehold("", 0, shared_from_this(), ServiceType::STREAM);
                timer.Reset();
            }
            auto checkIntervalMs = 10;
            std::this_thread::sleep_for(std::chrono::milliseconds(checkIntervalMs));
        }
    });
    return Status::OK();
}

#ifdef WITH_TESTS
void WorkerOcEvictionManager::AddHeatNodeForTest(const std::string &objectKey, double heat, uint64_t nowMs)
{
    memEvictionList_.AddHeatNode(objectKey, heat, nowMs);
}

Status WorkerOcEvictionManager::CollectCopyWatermarkStatsForTest(CopyWatermarkStats &stats)
{
    return CollectCopyWatermarkStats(stats);
}

void WorkerOcEvictionManager::NotifyCopyWatermarkObserverForTest()
{
    NotifyCopyWatermarkObserver();
}

void WorkerOcEvictionManager::MarkPrimaryEndLifeTaskActiveForTest(const std::string &objectKey, uint64_t version)
{
    std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
    pendingPrimaryEndLifeObjects_[objectKey] = version;
    ++activeDrainWorkers_;
}

void WorkerOcEvictionManager::FinishPrimaryEndLifeTaskAndWorkerForTest(const std::string &objectKey, uint64_t version)
{
    PrimaryEndLifeTask task;
    task.objectKey = objectKey;
    task.version = version;
    FinishPrimaryEndLifeTask(task, true);
    {
        std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
        --activeDrainWorkers_;
    }
    primaryEndLifeDrainedCv_.notify_all();
}

void WorkerOcEvictionManager::HoldStableRouteReaderForTest(const std::function<void()> &callback)
{
    StableRouteReadGuard guard(*this);
    if (guard) {
        callback();
    }
}
#endif

bool WorkerOcEvictionManager::TryMarkRebalancingObject(const std::string &objectKey)
{
    RebalancingObjectTable::accessor accessor;
    return rebalancingObjects_.insert(accessor, objectKey);
}

void WorkerOcEvictionManager::UnmarkRebalancingObject(const std::string &objectKey)
{
    (void)rebalancingObjects_.erase(objectKey);
}

bool WorkerOcEvictionManager::IsObjectBeingRebalanced(const std::string &objectKey) const
{
    RebalancingObjectTable::const_accessor accessor;
    return rebalancingObjects_.find(accessor, objectKey);
}

void WorkerOcEvictionManager::Add(const std::string &objectKey)
{
    VLOG(DEBUG_LOG_LEVEL) << FormatString("[ObjectKey %s] EvictionManager add start.", objectKey);
    Status rc = RoutePolicyMutation(objectKey, PolicyMutationKind::ADD);
    LOG_IF_ERROR(rc, "Failed to update eviction metadata");
}

Status WorkerOcEvictionManager::ApplyMigratedHeat(const std::string &objectKey, double heat, bool mergeExisting)
{
    bool trackMutation = policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE;
    if (trackMutation) {
        BeginTrackedPolicyMutation();
    }
    Raii mutationDone([this, &trackMutation]() {
        if (trackMutation) {
            EndTrackedPolicyMutation();
        }
    });
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    const auto phase = policyUpdatePhase_.load(std::memory_order_acquire);
    if (!trackMutation && phase != PolicyUpdatePhase::STABLE) {
        BeginTrackedPolicyMutation();
        trackMutation = true;
    }
    if (phase == PolicyUpdatePhase::STABLE || phase == PolicyUpdatePhase::DRAINING) {
        if (policyRoute_.sourcePolicy != EvictionPolicy::HEAT) {
            return Status::OK();
        }
        return policyRoute_.sourceList->ApplyMigratedHeat(objectKey, heat, policyRoute_.sourceHeatConfig.maxCounter,
                                                          static_cast<uint64_t>(GetSteadyClockTimeStampMs()),
                                                          mergeExisting);
    }

    std::lock_guard<std::mutex> keyLock(GetPolicyMigrationLock(objectKey));
    if (policyRoute_.targetPolicy == EvictionPolicy::HEAT) {
        RETURN_IF_NOT_OK(EnsureMigratedHeatTarget(objectKey));
        return policyRoute_.targetList->ApplyMigratedHeat(objectKey, heat, policyRoute_.targetHeatConfig.maxCounter,
                                                          static_cast<uint64_t>(GetSteadyClockTimeStampMs()),
                                                          mergeExisting);
    }
    if (!std::isfinite(heat) || heat < 0.0) {
        RETURN_STATUS(K_INVALID, "Invalid migrated heat");
    }
    const auto now = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    EvictionList::Node heatNode(objectKey, heat, now, now);
    EvictionList::Node clockNode;
    RETURN_IF_NOT_OK(ConvertPolicySnapshot(heatNode, EvictionPolicy::HEAT, EvictionPolicy::CLOCK,
                                           policyRoute_.targetHeatConfig, clockNode));
    bool inserted = false;
    Status rc = policyRoute_.targetList->InsertOrMerge(clockNode, EvictionList::StateKind::CLOCK, 0.0,
                                                       EvictionList::HeatMergeMode::PRESERVE_MAX, inserted);
    if (rc.IsError()) {
        return rc;
    }
    // Clock state has no exact heat representation. InsertOrMerge preserves the
    // more protected counter instead of destructively lowering a local access.
    return Status::OK();
}

Status WorkerOcEvictionManager::EnsureMigratedHeatTarget(const std::string &objectKey)
{
    if (policyRoute_.targetList->Exist(objectKey)) {
        return Status::OK();
    }
    if (!policyRoute_.sourceList->Exist(objectKey)) {
        policyRoute_.targetStrategy->OnAdd(objectKey);
        CHECK_FAIL_RETURN_STATUS(policyRoute_.targetList->Exist(objectKey), K_RUNTIME_ERROR,
                                 "Failed to create target Heat membership for migrated object");
        return Status::OK();
    }
    EvictionList::Node sourceNode;
    RETURN_IF_NOT_OK(policyRoute_.sourceList->GetObjectInfo(objectKey, sourceNode));
    EvictionList::Node targetNode;
    RETURN_IF_NOT_OK(ConvertPolicySnapshot(sourceNode, policyRoute_.sourcePolicy, policyRoute_.targetPolicy,
                                           policyRoute_.targetHeatConfig, targetNode));
    bool inserted = false;
    return policyRoute_.targetList->InsertOrMerge(targetNode, EvictionList::StateKind::HEAT,
                                                  policyRoute_.targetHeatConfig.maxCounter,
                                                  EvictionList::HeatMergeMode::PRESERVE_MAX, inserted);
}

void WorkerOcEvictionManager::OnCacheHit(const std::string &objectKey, uint64_t migratableSize)
{
    Status rc = RoutePolicyMutation(objectKey, PolicyMutationKind::CACHE_HIT, migratableSize);
    LOG_IF_ERROR(rc, "Failed to update eviction metadata on cache hit");
}

bool WorkerOcEvictionManager::TryOnCacheHitWithoutSize(const std::string &objectKey)
{
    return TryApplyClockMutationWithoutSize(objectKey, PolicyMutationKind::CACHE_HIT);
}

void WorkerOcEvictionManager::OnRefill(const std::string &objectKey, uint64_t migratableSize)
{
    Status rc = RoutePolicyMutation(objectKey, PolicyMutationKind::REFILL, migratableSize);
    LOG_IF_ERROR(rc, "Failed to update eviction metadata on cache refill");
}

bool WorkerOcEvictionManager::TryOnRefillWithoutSize(const std::string &objectKey)
{
    return TryApplyClockMutationWithoutSize(objectKey, PolicyMutationKind::REFILL);
}

EvictionPolicy WorkerOcEvictionManager::GetActiveEvictionPolicy() const
{
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    return policyRoute_.sourcePolicy;
}

uint64_t WorkerOcEvictionManager::GetPolicyUpdateEpoch() const
{
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    return policyRoute_.epoch;
}

WorkerOcEvictionManager::PolicyStateSnapshot WorkerOcEvictionManager::GetPolicyStateSnapshot() const
{
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    return { policyUpdatePhase_.load(std::memory_order_acquire), policyRoute_.sourcePolicy, policyRoute_.epoch,
             policyRoute_.targetPolicy };
}

Status WorkerOcEvictionManager::ValidateRebalancePolicy(uint32_t policy, uint64_t epoch) const
{
    CHECK_FAIL_RETURN_STATUS(policy == static_cast<uint32_t>(master::EVICTION_POLICY_CLOCK)
                                 || policy == static_cast<uint32_t>(master::EVICTION_POLICY_HEAT),
                             K_INVALID, "Rebalance task has an invalid eviction policy");
    CHECK_FAIL_RETURN_STATUS(policyUpdatePhase_.load(std::memory_order_acquire) == PolicyUpdatePhase::STABLE,
                             K_NOT_READY, "Eviction policy update is in progress");
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    const auto expected = policyRoute_.sourcePolicy == EvictionPolicy::CLOCK
                              ? static_cast<uint32_t>(master::EVICTION_POLICY_CLOCK)
                              : static_cast<uint32_t>(master::EVICTION_POLICY_HEAT);
    CHECK_FAIL_RETURN_STATUS(policy == expected && epoch == policyRoute_.epoch, K_NOT_READY,
                             "Rebalance task eviction policy fence is stale");
    return Status::OK();
}

std::mutex &WorkerOcEvictionManager::GetPolicyMigrationLock(const std::string &objectKey)
{
    return policyMigrationLocks_[std::hash<std::string>{}(objectKey) % policyMigrationLocks_.size()].mutex;
}

WorkerOcEvictionManager::StableRouteReadGuard::StableRouteReadGuard(WorkerOcEvictionManager &manager)
    : manager_(manager), acquired_(manager_.TryAcquireStableRouteReader(slot_))
{
}

WorkerOcEvictionManager::StableRouteReadGuard::~StableRouteReadGuard()
{
    if (acquired_) {
        manager_.ReleaseStableRouteReader(slot_);
    }
}

WorkerOcEvictionManager::StableRouteReadGuard::operator bool() const
{
    return acquired_;
}

bool WorkerOcEvictionManager::TryAcquireStableRouteReader(size_t &slot)
{
    if (policyUpdatePhase_.load(std::memory_order_seq_cst) != PolicyUpdatePhase::STABLE) {
        return false;
    }
    static thread_local const size_t readerSlot =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % STABLE_ROUTE_READER_SLOT_COUNT;
    slot = readerSlot;
    stableRouteReaderSlots_[slot].count.fetch_add(1, std::memory_order_seq_cst);
    if (policyUpdatePhase_.load(std::memory_order_seq_cst) == PolicyUpdatePhase::STABLE) {
        return true;
    }
    ReleaseStableRouteReader(slot);
    return false;
}

void WorkerOcEvictionManager::ReleaseStableRouteReader(size_t slot)
{
    const bool slotDrained = stableRouteReaderSlots_[slot].count.fetch_sub(1, std::memory_order_seq_cst) == 1;
    if (slotDrained && policyUpdatePhase_.load(std::memory_order_seq_cst) != PolicyUpdatePhase::STABLE) {
        // Synchronize with the predicate-to-wait handoff so the final draining notification cannot be lost. This
        // mutex is never touched by the STABLE hot path; only readers leaving during a policy transition take it.
        std::lock_guard<std::mutex> lock(cvMutex_);
        stableRouteReadersCv_.notify_all();
    }
}

bool WorkerOcEvictionManager::StableRouteReadersDrained() const
{
    return std::all_of(stableRouteReaderSlots_.begin(), stableRouteReaderSlots_.end(),
                       [](const auto &slot) { return slot.count.load(std::memory_order_seq_cst) == 0; });
}

Status WorkerOcEvictionManager::RoutePolicyMutation(const std::string &objectKey, PolicyMutationKind kind,
                                                    uint64_t migratableSize, EvictionList::Node *snapshot)
{
    bool trackMutation = policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE;
    if (trackMutation) {
        BeginTrackedPolicyMutation();
    }
    Raii mutationDone([this, &trackMutation]() {
        if (trackMutation) {
            EndTrackedPolicyMutation();
        }
    });
    StableRouteReadGuard stableRoute(*this);
    if (stableRoute) {
        return ApplyPolicyMutation(policyRoute_, objectKey, kind, migratableSize, snapshot);
    }
    if (!trackMutation) {
        BeginTrackedPolicyMutation();
        trackMutation = true;
    }

    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    const auto phase = policyUpdatePhase_.load(std::memory_order_acquire);
    if (phase == PolicyUpdatePhase::STABLE || phase == PolicyUpdatePhase::DRAINING) {
        return ApplyPolicyMutation(policyRoute_, objectKey, kind, migratableSize, snapshot);
    }

    return RoutePolicyMutationDuringUpdate(objectKey, kind, migratableSize, snapshot);
}

bool WorkerOcEvictionManager::TryApplyClockMutationWithoutSize(const std::string &objectKey,
                                                               PolicyMutationKind kind)
{
    // The atomic is only a fast rejection for stable Heat. Correctness comes from acquiring the same stable-route
    // reader that policy rollout drains: either this CLOCK mutation completes before the transition, or admission
    // fails and the caller resolves size for the Heat-capable route.
    if (needsMigratableSize_.load(std::memory_order_acquire)) {
        return false;
    }
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.TryClockMutation.afterSizeCheck", []() {});
    StableRouteReadGuard stableRoute(*this);
    if (!stableRoute || policyRoute_.sourcePolicy != EvictionPolicy::CLOCK) {
        return false;
    }
    Status rc = ApplyPolicyMutation(policyRoute_, objectKey, kind, 0, nullptr);
    LOG_IF_ERROR(rc, "Failed to update CLOCK eviction metadata without object-size lookup");
    return true;
}

Status WorkerOcEvictionManager::RoutePolicyMutationDuringUpdate(
    const std::string &objectKey, PolicyMutationKind kind, uint64_t migratableSize, EvictionList::Node *snapshot)
{
    std::lock_guard<std::mutex> keyLock(GetPolicyMigrationLock(objectKey));
    if (kind == PolicyMutationKind::ERASE) {
        (void)policyRoute_.targetList->Erase(objectKey);
        (void)policyRoute_.sourceList->Erase(objectKey);
        return Status::OK();
    }
    if (kind == PolicyMutationKind::EXTRACT) {
        CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_INVALID, "Eviction snapshot output is required");
        Status rc = policyRoute_.targetList->Extract(objectKey, *snapshot);
        if (rc.IsOk()) {
            (void)policyRoute_.sourceList->Erase(objectKey);
            return rc;
        }
        RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
        return policyRoute_.sourceList->Extract(objectKey, *snapshot);
    }
    Status rc = EnsureTargetMembership(objectKey);
    if (rc.IsError()) {
        LOG(WARNING) << "Eviction policy target creation failed; applied the access to source instead. Detail: "
                     << rc.ToString();
        (void)ApplyPolicyMutation(policyRoute_, objectKey, kind, migratableSize, snapshot);
        return rc;
    }
    PolicyRoute targetRoute;
    targetRoute.sourceList = policyRoute_.targetList;
    targetRoute.sourceStrategy = policyRoute_.targetStrategy;
    RETURN_IF_NOT_OK(ApplyPolicyMutation(targetRoute, objectKey, kind, migratableSize, snapshot));
    CHECK_FAIL_RETURN_STATUS(policyRoute_.targetList->Exist(objectKey), K_RUNTIME_ERROR,
                             "Target eviction policy did not create the requested membership");
    return Status::OK();
}

Status WorkerOcEvictionManager::ApplyPolicyMutation(const PolicyRoute &route, const std::string &objectKey,
                                                    PolicyMutationKind kind, uint64_t migratableSize,
                                                    EvictionList::Node *snapshot)
{
    switch (kind) {
        case PolicyMutationKind::ADD:
            route.sourceStrategy->OnAdd(objectKey);
            break;
        case PolicyMutationKind::CACHE_HIT:
            route.sourceStrategy->OnCacheHit(objectKey, migratableSize);
            break;
        case PolicyMutationKind::REFILL:
            route.sourceStrategy->OnRefill(objectKey, migratableSize);
            break;
        case PolicyMutationKind::ERASE:
            (void)route.sourceList->Erase(objectKey);
            break;
        case PolicyMutationKind::EXTRACT:
            CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_INVALID, "Eviction snapshot output is required");
            return route.sourceList->Extract(objectKey, *snapshot);
    }
    return Status::OK();
}

Status WorkerOcEvictionManager::EnsureTargetMembership(const std::string &objectKey)
{
    if (policyRoute_.targetList->Exist(objectKey) || !policyRoute_.sourceList->Exist(objectKey)) {
        return Status::OK();
    }
    EvictionList::Node sourceNode;
    RETURN_IF_NOT_OK(policyRoute_.sourceList->GetObjectInfo(objectKey, sourceNode));
    EvictionList::Node targetNode;
    RETURN_IF_NOT_OK(ConvertPolicySnapshot(sourceNode, policyRoute_.sourcePolicy, policyRoute_.targetPolicy,
                                           policyRoute_.targetHeatConfig, targetNode));
    bool inserted = false;
    return policyRoute_.targetList->InsertOrMerge(targetNode,
                                                  policyRoute_.targetPolicy == EvictionPolicy::HEAT
                                                      ? EvictionList::StateKind::HEAT
                                                      : EvictionList::StateKind::CLOCK,
                                                  policyRoute_.targetHeatConfig.maxCounter,
                                                  EvictionList::HeatMergeMode::PRESERVE_MAX, inserted);
}

Status WorkerOcEvictionManager::ConvertPolicySnapshot(const EvictionList::Node &source, EvictionPolicy sourcePolicy,
                                                      EvictionPolicy targetPolicy,
                                                      const HeatPolicyConfig &targetHeatConfig,
                                                      EvictionList::Node &target)
{
    if (sourcePolicy == targetPolicy) {
        target = source;
        return Status::OK();
    }
    if (targetPolicy == EvictionPolicy::HEAT) {
        static constexpr std::array<double, 6> COUNTER_TO_HEAT{ 0.0, 2.0, 4.0, 8.0, 12.0, 16.0 };
        const auto counter = std::min<size_t>(source.curCounter, COUNTER_TO_HEAT.size() - 1);
        const auto now = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
        target = EvictionList::Node(source.objectKey, std::min(COUNTER_TO_HEAT[counter], targetHeatConfig.maxCounter),
                                    now, now);
        return Status::OK();
    }
    const uint8_t counter = source.heat < 2.0 ? 0 : (source.heat < 4.0 ? 1 : 2);
    target = EvictionList::Node(source.objectKey, counter);
    target.maxCounter = std::max(counter, ComputeClockAddCounter(gRefTable_, source.objectKey));
    return Status::OK();
}

void WorkerOcEvictionManager::Erase(const std::string &objectKey)
{
    VLOG(DEBUG_LOG_LEVEL) << FormatString("[ObjectKey %s] EvictionManager erase start.", objectKey);
    (void)RoutePolicyMutation(objectKey, PolicyMutationKind::ERASE);
}

Status WorkerOcEvictionManager::ExtractEvictionNode(const std::string &objectKey, EvictionList::Node &snapshot)
{
    return RoutePolicyMutation(objectKey, PolicyMutationKind::EXTRACT, 0, &snapshot);
}

Status WorkerOcEvictionManager::RemoveMetaFromMasterForEviction(EvictDeletedObjects &objectKeyVersions)
{
    RETURN_OK_IF_TRUE(objectKeyVersions.empty());
    VLOG(DEBUG_LOG_LEVEL) << "RemoveMetaFromMasterForEviction start. Object count: " << objectKeyVersions.size();
    EvictDeletedObjects failedObjects;
    Status lastRc;
    std::vector<std::string> objectKeys;
    objectKeys.reserve(objectKeyVersions.size());
    for (const auto &item : objectKeyVersions) {
        objectKeys.emplace_back(item.first);
    }
    auto grouped = metadataRoute_.GroupOwners(objectKeys);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.moveToEmptyMaster",
                           [&objKeysGrpByMaster](const std::string &objectKey) {
                               for (auto &item : objKeysGrpByMaster) {
                                   auto &objectKeys = item.second;
                                   objectKeys.erase(std::remove(objectKeys.begin(), objectKeys.end(), objectKey),
                                                    objectKeys.end());
                               }
                               objKeysGrpByMaster[HostPort()].emplace_back(objectKey);
                           });
    for (const auto &item : objKeysGrpByMaster) {
        RemoveEvictionMetaGroup(item.first, item.second, objectKeyVersions, failedObjects, lastRc);
    }
    objectKeyVersions = std::move(failedObjects);
    return objectKeyVersions.empty() ? Status::OK() : lastRc;
}

master::RemoveMetaReqPb WorkerOcEvictionManager::BuildEvictionRemoveMetaReq(
    const std::vector<std::string> &objectKeys, const EvictDeletedObjects &objectKeyVersions,
    const HostPort &localAddress, bool redirect)
{
    master::RemoveMetaReqPb req;
    req.set_address(localAddress.ToString());
    req.set_cause(master::RemoveMetaReqPb::EVICTION);
    req.set_version(UINT64_MAX);
    req.set_redirect(redirect);
    *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
    for (const auto &objectKey : objectKeys) {
        auto *objectVersion = req.add_id_with_version();
        objectVersion->set_id(objectKey);
        objectVersion->set_version(objectKeyVersions.at(objectKey));
    }
    return req;
}

Status WorkerOcEvictionManager::RemoveEvictionMetaOnce(const HostPort &masterAddr,
                                                        const std::vector<std::string> &objectKeys,
                                                        const EvictDeletedObjects &objectKeyVersions,
                                                        bool allowRedirect, master::RemoveMetaRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(!masterAddr.Empty(), K_NOT_FOUND, "Cannot find master for eviction remove-meta.");
    auto workerMasterApi =
        worker::WorkerMasterOCApi::CreateWorkerMasterOCApi(masterAddr, localAddress_, akSkManager_, masterOc_);
    RETURN_IF_NOT_OK(workerMasterApi->Init());
    auto req = BuildEvictionRemoveMetaReq(objectKeys, objectKeyVersions, localAddress_, allowRedirect);
    return workerMasterApi->RemoveMeta(req, rsp);
}

void WorkerOcEvictionManager::RemoveEvictionMetaGroup(
    const HostPort &masterAddr, const std::vector<std::string> &objectKeys,
    const EvictDeletedObjects &objectKeyVersions, EvictDeletedObjects &failedObjects, Status &lastRc)
{
    auto addFailedObjects = [&objectKeyVersions, &failedObjects, &lastRc](const std::vector<std::string> &failedKeys,
                                                                         const Status &rc) {
        lastRc = rc;
        for (const auto &objectKey : failedKeys) {
            failedObjects[objectKey] = objectKeyVersions.at(objectKey);
        }
    };
    if (objectKeys.empty()) {
        return;
    }
    master::RemoveMetaRspPb rsp;
    auto rc = RemoveEvictionMetaOnce(masterAddr, objectKeys, objectKeyVersions, true, rsp);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("RemoveMeta failed, object count %zu, status: %s.", objectKeys.size(),
                                   rc.ToString());
        addFailedObjects(objectKeys, rc);
        return;
    }
    if (rsp.meta_is_moving()) {
        addFailedObjects(objectKeys, { K_TRY_AGAIN, "Meta is moving." });
        return;
    }
    for (const auto &redirectInfo : rsp.info()) {
        std::vector<std::string> redirectKeys(redirectInfo.change_meta_ids().begin(),
                                              redirectInfo.change_meta_ids().end());
        if (redirectKeys.empty()) {
            continue;
        }
        HostPort redirectMasterAddr;
        rc = redirectMasterAddr.ParseString(redirectInfo.redirect_meta_address());
        if (rc.IsError()) {
            addFailedObjects(redirectKeys, rc);
            continue;
        }
        master::RemoveMetaRspPb redirectRsp;
        rc = RemoveEvictionMetaOnce(redirectMasterAddr, redirectKeys, objectKeyVersions, false, redirectRsp);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Forwarded RemoveMeta failed, object count %zu, status: %s.",
                                       redirectKeys.size(), rc.ToString());
            addFailedObjects(redirectKeys, rc);
            continue;
        }
        if (redirectRsp.meta_is_moving()) {
            addFailedObjects(redirectKeys, { K_TRY_AGAIN, "Forwarded meta is moving." });
            continue;
        }
        if (!redirectRsp.info().empty()) {
            LOG(WARNING) << FormatString("Forwarded RemoveMeta returned another redirect, object count %zu.",
                                         redirectKeys.size());
            addFailedObjects(redirectKeys, { K_TRY_AGAIN, "Forwarded RemoveMeta returned another redirect." });
            continue;
        }
        if (!redirectRsp.failed_ids().empty()) {
            std::vector<std::string> failedIds(redirectRsp.failed_ids().begin(), redirectRsp.failed_ids().end());
            addFailedObjects(failedIds, { K_TRY_AGAIN, "Forwarded RemoveMeta returned failed ids." });
        }
    }
    if (!rsp.failed_ids().empty()) {
        std::vector<std::string> failedIds(rsp.failed_ids().begin(), rsp.failed_ids().end());
        addFailedObjects(failedIds, { K_TRY_AGAIN, "RemoveMeta returned failed ids." });
    }
}

void WorkerOcEvictionManager::GetObjectNextAction(SafeObjType &entry, std::unique_ptr<EvictionTrace> &trace,
                                                  size_t pendingSpillSize)
{
    Action nextAction;
    std::string info;
    bool hasL2Cache = IsObjectExistInL2Cache(entry);
    size_t needSpillSize = pendingSpillSize + entry->GetDataSize();
    if (!entry->stateInfo.IsPrimaryCopy()) {
        info = "not primary copy";
        nextAction = Action::DELETE;
    } else if (entry->modeInfo.GetCacheType() == CacheType::DISK) {
        if (hasL2Cache) {
            info = "object has L2 cache, which cache type is DISK";
            nextAction = Action::DELETE;
        } else if (entry->IsNoneL2CacheEvictMode()) {
            info = "object could be evict, which cache type is DISK";
            nextAction = Action::END_LIFE;
        } else {
            info = "object don't have L2 cache, which cache type is DISK";
            nextAction = Action::RETAIN;
        }
    } else if (entry->IsSpilled()) {
        info = "already spilled";
        nextAction = Action::FREE_MEMORY;
    } else if (WorkerOcSpill::Instance()->IsEnabled()) {
        const double ratio = 0.95;
        bool spaceFull = WorkerOcSpill::Instance()->IsSpaceExceed(ratio, needSpillSize);
        info =
            FormatString("space is %sfull and object has %sL2 cache", spaceFull ? "" : "not ", hasL2Cache ? "" : "no ");
        if (spaceFull && hasL2Cache) {
            nextAction = Action::DELETE;
        } else {
            nextAction = Action::SPILL;
        }
    } else if (entry->IsWriteBackL2CacheEvictMode()) {
        info = "object is WRITE_BACK_L2_CACHE_EVICT mode";
        nextAction = Action::END_LIFE;
    } else if (hasL2Cache) {
        info = "object has L2 cache but no spill directory";
        nextAction = Action::DELETE;
    } else if (entry->IsNoneL2CacheEvictMode()) {
        info = "object could be evict";
        nextAction = Action::END_LIFE;
    } else {
        info = "no spill directories are configured";
        nextAction = Action::RETAIN;
    }
    INJECT_POINT("evictAction.setDelete", [&nextAction]() { nextAction = Action::DELETE; });
    trace->info = info;
    trace->action = nextAction;
}

Status WorkerOcEvictionManager::EvictObject(ObjectKV &objectKV, Action nextAction, EvictDeletedObjects *deletedObjects,
                                            CacheType cacheType, uint64_t needSize, const EvictionCandidate *candidate)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    if (nextAction == Action::END_LIFE) {
        bool accepted = false;
        Status rc = SubmitPrimaryEndLifeTask(objectKV, cacheType, needSize, accepted, candidate);
        Erase(objectKey);
        RETURN_IF_NOT_OK(rc);
        VLOG(1) << FormatString("[ObjectKey %s] Object will be end of life, accepted: %d", objectKey, accepted);
        return Status::OK();
    }
    Erase(objectKey);
    const bool hadCpuCopy = entry.Get() != nullptr && !entry->stateInfo.IsCacheInvalid();
    if (nextAction == Action::DELETE) {
        PerfPoint point(PerfKey::WORKER_EVICT_DELETE);
        uint64_t version = entry.Get()->GetCreateTime();
        // No need to call FreeResources as destructor will free the resources.
        RETURN_IF_NOT_OK(objectTable_->Erase(objectKey, entry));
        if (deletedObjects == nullptr) {
            EvictDeletedObjects objectKeyVersions = { { objectKey, version } };
            SubmitAsyncMasterTask(objectKeyVersions);
        } else {
            (*deletedObjects)[objectKey] = version;
        }
        point.Record();
        VLOG(1) << FormatString("[ObjectKey %s] Object delete success", objectKey);
        if (hadCpuCopy) {
            PublishKvRemovedEvent(kvEventPublisher_, objectKey, kKvEventMediumCpu);
        }
    } else if (nextAction == Action::FREE_MEMORY) {
        PerfPoint point(PerfKey::WORKER_EVICT_FREE);
        RETURN_IF_NOT_OK(entry->FreeResources());
        point.Record();
        VLOG(1) << FormatString("[ObjectKey %s] Object free success", objectKey);
        if (hadCpuCopy) {
            PublishKvRemovedEvent(kvEventPublisher_, objectKey, kKvEventMediumCpu);
        }
    } else if (nextAction == Action::SPILL) {
        VLOG(1) << FormatString("[ObjectKey %s] Object will be spill", objectKey);
    } else {
        RETURN_STATUS(K_NOT_READY, "No space in EvictObject");
    }
    return Status::OK();
}

Status WorkerOcEvictionManager::EvictClearObject(ObjectKV &objectKV)
{
    return EvictObject(objectKV, Action::DELETE);
}

uint64_t WorkerOcEvictionManager::GetLowWaterMark(CacheType cacheType)
{
    memory::CacheType memCacheType = static_cast<memory::CacheType>(cacheType);
    auto maxMemorySize = datasystem::memory::Allocator::Instance()->GetMaxMemorySize(ServiceType::OBJECT, memCacheType);
    auto usedMemorySize =
        datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT, memCacheType);
    auto lowWater = static_cast<std::uint64_t>(
        std::min(datasystem::memory::Allocator::Instance()->GetTotalRealMemoryFree(memCacheType) + usedMemorySize,
                 maxMemorySize)
        * GetEvictionLowWaterFactor());
    return lowWater;
}

bool WorkerOcEvictionManager::IsAboveLowWaterMark(uint64_t needSize, size_t pendingSpillSize, CacheType cacheType)
{
    uint64_t max = std::numeric_limits<uint64_t>::max();
    auto realMemoryUsage = datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(
        ServiceType::OBJECT, static_cast<memory::CacheType>(cacheType));
    realMemoryUsage = (realMemoryUsage > max - needSize) ? max : realMemoryUsage + needSize;
    auto lowWater = GetLowWaterMark(cacheType);
    lowWater = (lowWater > max - pendingSpillSize) ? max : lowWater + pendingSpillSize;
    return realMemoryUsage > lowWater;
}

void WorkerOcEvictionManager::EvictionTask(uint64_t needSize, CacheType cacheType)
{
    Raii finishTask([this]() {
        isDone_.store(true, std::memory_order_release);
        NotifyEvictionStopped();
    });
    std::shared_ptr<EvictionStrategy> strategy;
    EvictionList *evictionList = nullptr;
    {
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        if (policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE) {
            return;
        }
        strategy = policyRoute_.sourceStrategy;
        evictionList = policyRoute_.sourceList;
    }
    Timer evictionTaskTimer;
    EvictionRetryList evictFailedIds;
    EvictionRoundState evictionRound;
    // IMPORTANT — declaration order:
    // evictionAggregator MUST be declared before spillTasks.
    // spillTasks entries hold EvictionTrace objects whose aggregator_ pointer
    // points to evictionAggregator (set at line ~trace->aggregator_ = &evictionAggregator).
    // C++ destroys local variables in reverse declaration order, so spillTasks
    // (and the EvictionTrace objects inside it) are destroyed BEFORE evictionAggregator.
    // When an EvictionTrace is destroyed, its destructor calls
    // aggregator_->Add(*this), which appends data into evictionAggregator.
    // If evictionAggregator were destroyed first, this would be a use-after-free.
    // Additionally, async spill futures may hold EvictionTrace objects whose
    // destructors fire when the future completes. ReleaseSpillFutures(..., true)
    // at the end of this function blocks until ALL futures are ready, ensuring
    // every trace destructor runs before evictionAggregator goes out of scope.
    EvictionTraceAggregator evictionAggregator;
    std::unordered_map<std::string, SpillTask> spillTasks;
    EvictDeletedObjects deletedObjects;
    Timer deletedObjectsFlushTimer(BATCH_DELETE_META_MAX_DELAY_MS);
    auto flushDeletedObjects = [this, &deletedObjects, &deletedObjectsFlushTimer]() {
        if (deletedObjects.empty()) {
            return;
        }
        SubmitAsyncMasterTask(deletedObjects);
        deletedObjects.clear();
        deletedObjectsFlushTimer.Reset();
    };
    LOG(INFO) << "EvictionList size before evict: " << evictionList->Size();
    VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=eviction_summary, event=start, eviction_list_size="
                          << memEvictionList_.Size() << ", pressure=" << GetPrimaryEndLifePressure();
    size_t pendingSpillSize = 0;
    // The size of low water mark memory usage is not fixed. It varies on the size of shared memory available.
    // Share memory release is delayed due to asynchronous spill, so the pending spill data size needs to be counted to
    // prevent all objects from being spilled.
    while (!evictionCancelRequested_.load(std::memory_order_acquire)
           && IsAboveLowWaterMark(needSize, pendingSpillSize, cacheType) && evictionList->Size() != 0) {
        EvictionCandidate candidate;
        if (strategy->SelectCandidate(evictionRound, candidate).IsError()) {
            LOG(ERROR) << "FindEvictCandidate failed, EvictionList is empty.";
            continue;
        }
        const auto &candidateId = candidate.objectKey;
        if (evictionCancelRequested_.load(std::memory_order_acquire)) {
            break;
        }
        auto trace = std::make_unique<EvictionTrace>(candidateId);
        trace->aggregator_ = &evictionAggregator;
        std::shared_ptr<SafeObjType> entry;
        std::optional<EvictionList::Node> retrySnapshot;
        Status rc = GetAndLockEntry(candidateId, entry, retrySnapshot);
        if (rc.IsError()) {
            if (retrySnapshot.has_value()) {
                evictFailedIds.push_back(
                    { MakeEvictionCandidate(candidate.policy, *retrySnapshot), static_cast<uint8_t>(Q1) });
            }
            trace->rc = Status(rc.GetCode(), FormatString("GetAndLockEntry failed %s.", rc.GetMsg()));
            continue;
        }
        ObjectKV objectKV(candidateId, *entry);
        bool locked = true;
        Raii unLockRaii([entry, &locked]() {
            if (locked) {
                entry->WUnlock();
            }
        });
        // Heat selection consumes a bounded snapshot batch. Revalidate after taking the object write lock so a cache
        // hit, decay, reinsert, or same-key recreation cannot evict a stale snapshot.
        if (!strategy->ValidateCandidate(evictionRound, candidate)) {
            trace->rc = Status(K_NOT_READY, "Eviction candidate changed after selection");
            continue;
        }
        // Pair with RebalanceCandidateProvider, which marks candidates while holding the object read lock. A prior mark
        // is visible after this write lock is acquired; later rebalance validation waits until eviction ends.
        if (IsObjectBeingRebalanced(candidateId)) {
            trace->rc = Status(K_NOT_READY, "Object is being rebalanced");
            (void)evictionList->Erase(candidateId);
            evictFailedIds.push_back({ candidate, READD_COUNTER });
            continue;
        }
        trace->AddObjectKeySize(candidateId, (*entry)->GetDataSize());
        // This moment object key may not in EvictionList.
        // It may be erased in other place after we got candidateId.
        // So we need to check it before do evict.
        if (!IsObjectEvictable(objectKV)) {
            trace->rc = Status(K_RUNTIME_ERROR, "IsObjectEvictable return false");
            continue;
        }
        GetObjectNextAction(*entry, trace, pendingSpillSize);
        bool wasDeletedObjectsEmpty = deletedObjects.empty();
        rc = TryEvictObject(entry, std::move(trace), pendingSpillSize, spillTasks, locked, candidate, cacheType,
                            &deletedObjects, needSize);
        if (wasDeletedObjectsEmpty && !deletedObjects.empty()) {
            deletedObjectsFlushTimer.Reset();
        }
        if (deletedObjects.size() >= BATCH_DELETE_META_THRESHOLD
            || (!deletedObjects.empty() && deletedObjectsFlushTimer.IsTimeout())) {
            flushDeletedObjects();
        }
        if (rc.IsError()) {
            // K_TRY_AGAIN (e.g. primary end-life queue full) is a transient capacity issue;
            // Transient failures like end-life queue-full (K_TRY_AGAIN) use Q1 for fast
            // retry since the condition clears quickly. Persistent errors (e.g. master
            // RPC failure) use READD_COUNTER=5 as backoff to avoid a tight retry loop.
            // With push_back the object lands at the tail, so the 5-round backoff
            // (~list_size / write_rate per round) is a reasonable delay (issue #750).
            uint8_t counter = (rc.GetCode() == StatusCode::K_TRY_AGAIN) ? Q1 : READD_COUNTER;
            evictFailedIds.push_back({ candidate, counter });
        }
        auto spilledSize = ReleaseSpillFutures(spillTasks, evictFailedIds, false);
        pendingSpillSize -= std::min(pendingSpillSize, spilledSize);
        INJECT_POINT("worker.Evict", [&pendingSpillSize](size_t size) { pendingSpillSize = size; });
    }
    flushDeletedObjects();
    // Blocking wait (last=true) for ALL remaining async spill futures.
    // This must be blocking because evictionAggregator (declared above) will go
    // out of scope when this function returns. Futures that complete after the
    // aggregator is destroyed would call aggregator_->Add() on a dangling pointer
    // in ~EvictionTrace(), causing use-after-free.
    (void)ReleaseSpillFutures(spillTasks, evictFailedIds, true);

    for (const auto &retry : evictFailedIds) {
        strategy->ReaddCandidate(retry.candidate, retry.counter);
    }
    LOG(INFO) << "EvictionList size after evict:" << evictionList->Size() << ", failed size:" << evictFailedIds.size();
    auto evictionElapsedMs = evictionTaskTimer.ElapsedMilliSecond();
    if (evictionElapsedMs >= PRIMARY_END_LIFE_SLOW_LOG_THRESHOLD_MS) {
        LOG(WARNING) << "PRIMARY_END_LIFE_DIAG stage=eviction_summary, event=complete, elapsed_ms="
                     << evictionElapsedMs << ", eviction_list_size=" << evictionList->Size()
                     << ", failed_keys=" << evictFailedIds.size() << ", pressure=" << GetPrimaryEndLifePressure();
    } else {
        VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=eviction_summary, event=complete, elapsed_ms="
                              << evictionElapsedMs << ", eviction_list_size=" << evictionList->Size()
                              << ", failed_keys=" << evictFailedIds.size()
                              << ", pressure=" << GetPrimaryEndLifePressure();
    }
}

void WorkerOcEvictionManager::NotifyEvictionStopped()
{
    evictionStoppedCv_.notify_all();
}

Status WorkerOcEvictionManager::TryEvictObject(std::shared_ptr<SafeObjType> &entry,
                                               std::unique_ptr<EvictionTrace> trace, size_t &pendingSpillSize,
                                               std::unordered_map<std::string, SpillTask> &spillTasks, bool &locked,
                                               const EvictionCandidate &candidate, CacheType cacheType,
                                               EvictDeletedObjects *deletedObjects, uint64_t needSize)
{
    const auto &objectKey = trace->taskId;
    ObjectKV objectKV(objectKey, *entry);
    PerfPoint point(PerfKey::WORKER_EVICT_ONE_OBJECT);
    Status rc = EvictObject(objectKV, trace->action, deletedObjects, cacheType, needSize, &candidate);
    if (rc.IsError()) {
        trace->rc = rc;
        if (rc.GetCode() != K_NOT_READY) {
            trace->rc.AppendMsg("EvictObject failed");
        } else if ((*entry)->modeInfo.GetCacheType() == CacheType::DISK) {
            return Status::OK();
        }
        return rc;
    }
    if (trace->action == Action::SPILL) {
        auto objectSize = (*entry)->GetDataSize();
        auto version = (*entry)->GetCreateTime();
        // Ensure the spill task for the same object are not concurrent
        if (spillTasks.count(objectKey) > 0) {
            RETURN_STATUS(K_TRY_AGAIN, "Spill task is running.");
        }
        entry->WUnlock();
        locked = false;
        pendingSpillSize += objectSize;
        spillTasks.emplace(objectKey, SpillTask{ SubmitSpillTask(objectKey, version), std::move(trace), candidate });
    }
    return Status::OK();
}

void WorkerOcEvictionManager::Evict(uint64_t needSize, CacheType cacheType)
{
    LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL3) << "Eviction start.";
    if (policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE) {
        LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL3) << "Eviction is disabled during policy update.";
        return;
    }
    bool expected = true;
    if (isDone_.compare_exchange_strong(expected, false)) {
        if (policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE) {
            isDone_.store(true, std::memory_order_release);
            NotifyEvictionStopped();
            return;
        }
        std::unique_lock<std::mutex> lk(cvMutex_);
        auto traceID = Trace::Instance().GetTraceID();
        memEvictTaskThreadPool_->Execute([this, traceID, needSize, cacheType] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            EvictionTask(needSize, cacheType);
        });
    } else {
        LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL3) << "Evict is going on...";
    }
}

Status WorkerOcEvictionManager::GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest)
{
    StableRouteReadGuard stableRoute(*this);
    if (stableRoute) {
        return policyRoute_.sourceList->GetAllObjectsInfo(res, oldest);
    }
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    return policyRoute_.sourceList->GetAllObjectsInfo(res, oldest);
}

Status WorkerOcEvictionManager::GetObjectInfo(const std::string &objectKey, EvictionList::Node &node)
{
    StableRouteReadGuard stableRoute(*this);
    if (stableRoute) {
        return policyRoute_.sourceList->GetObjectInfo(objectKey, node);
    }
    std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
    const auto phase = policyUpdatePhase_.load(std::memory_order_acquire);
    if (phase != PolicyUpdatePhase::STABLE && phase != PolicyUpdatePhase::DRAINING) {
        Status rc = policyRoute_.targetList->GetObjectInfo(objectKey, node);
        if (rc.IsOk() || rc.GetCode() != K_NOT_FOUND) {
            return rc;
        }
    }
    return policyRoute_.sourceList->GetObjectInfo(objectKey, node);
}

Status WorkerOcEvictionManager::GetObjectsInfoFromOldest(size_t maxScanCount, std::vector<EvictionList::Node> &res)
{
    StableRouteReadGuard stableRoute(*this);
    if (!stableRoute) {
        RETURN_STATUS(K_NOT_READY, "Eviction candidates are unavailable during policy update");
    }
    return policyRoute_.sourceList->GetObjectsInfoFromOldest(maxScanCount, res);
}

Status WorkerOcEvictionManager::GetLowestHeatObjects(size_t maxCount, std::vector<EvictionList::Node> &res)
{
    StableRouteReadGuard stableRoute(*this);
    if (!stableRoute || policyRoute_.sourcePolicy != EvictionPolicy::HEAT) {
        RETURN_STATUS(K_NOT_READY, "Active eviction policy is not Heat");
    }
    // Rebalance consumes a bounded candidate batch and revalidates every selected object. A global full-list minimum
    // is not required, while holding listMutex_ across millions of nodes would stall eviction-list writers. Passing a
    // finite threshold activates the same 8 * maxCount scan bound used by normal heat eviction.
    return policyRoute_.sourceList->GetHeatCandidates(std::numeric_limits<double>::infinity(), maxCount, res, 0,
                                                      std::numeric_limits<double>::max());
}

void WorkerOcEvictionManager::SubmitAsyncMasterTask(const EvictDeletedObjects &objectKeyVersions)
{
    if (objectKeyVersions.empty()) {
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    masterTaskThreadPool_->Execute([this, objectKeyVersions, traceID] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        AsyncMasterTask(objectKeyVersions);
    });
}

void WorkerOcEvictionManager::AsyncMasterTask(const EvictDeletedObjects &objectKeyVersions)
{
    Status rc;
    int retryCount = 0;
    const int maxRetryNum = 3;
    Timer timer;
    EvictDeletedObjects failedObjects = objectKeyVersions;
    do {
        rc = RemoveMetaFromMasterForEviction(failedObjects);
    } while (rc.IsError() && !failedObjects.empty() && retryCount++ < maxRetryNum);
    if (rc.IsError()) {
        LOG_EVERY_T(ERROR, LOG_TIME_LIMIT_LEVEL2) << FormatString(
            "[Object count %zu] RemoveMetaFromMasterForEviction failed, %s", failedObjects.size(), rc.ToString());
    } else {
        auto elapsedMs = timer.ElapsedMilliSecond();
        auto logLevel = elapsedMs > 1 ? 0 : 1;
        VLOG(logLevel) << FormatString("[Object count %zu] RemoveMetaFromMasterForEviction took %f ms",
                                       objectKeyVersions.size(), elapsedMs);
    }
}

Status WorkerOcEvictionManager::SubmitPrimaryEndLifeTask(const ObjectKV &objectKV, CacheType cacheType,
                                                         uint64_t needSize, bool &accepted,
                                                         const EvictionCandidate *candidate)
{
    const auto &objectKey = objectKV.GetObjKey();
    PrimaryEndLifeTask task{ objectKey, objectKV.GetObjEntry()->GetCreateTime(), cacheType, needSize };
    task.queuedAtMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    if (candidate != nullptr) {
        task.evictionCandidate = *candidate;
    }
    RETURN_IF_NOT_OK(ReservePrimaryEndLifeTask(task, accepted));
    if (!accepted) {
        VLOG(DEBUG_LOG_LEVEL) << FormatString("[ObjectKey %s] Primary end-life task already pending.", objectKey);
        return Status::OK();
    }
    Status rc = EnqueuePrimaryEndLifeTask(task);
    if (rc.IsError()) {
        LOG(WARNING) << "[ObjectKey " << objectKey << "] Enqueue primary end-life task failed, " << rc.ToString()
                     << ".";
        ClearPrimaryEndLifePending(task);
    }
    return rc;
}

Status WorkerOcEvictionManager::ReservePrimaryEndLifeTask(PrimaryEndLifeTask &task, bool &accepted)
{
    size_t pendingSize = 0;
    size_t queueSize = 0;
    int activeWorkers = 0;
    std::unique_lock<std::mutex> lock(primaryEndLifeMutex_);
    task.metaDeleted = false;
    auto metaIter = metaDeletedPrimaryEndLifeObjects_.find(task.objectKey);
    if (metaIter != metaDeletedPrimaryEndLifeObjects_.end()) {
        if (metaIter->second == task.version) {
            task.metaDeleted = true;
        } else {
            metaDeletedPrimaryEndLifeObjects_.erase(metaIter);
        }
    }
    accepted = false;
    auto [iter, inserted] = pendingPrimaryEndLifeObjects_.emplace(task.objectKey, task.version);
    if (!inserted) {
        return Status::OK();
    }
    if (pendingPrimaryEndLifeObjects_.size() > PRIMARY_END_LIFE_PENDING_LIMIT) {
        pendingPrimaryEndLifeObjects_.erase(iter);
        primaryEndLifePendingFullCount_.fetch_add(1, std::memory_order_relaxed);
        pendingSize = pendingPrimaryEndLifeObjects_.size();
        queueSize = primaryEndLifeQueue_.size();
        activeWorkers = activeDrainWorkers_;
        lock.unlock();
        LOG_EVERY_T(WARNING, LOG_TIME_LIMIT_LEVEL2)
            << "Primary end-life pending queue is full, full count since last log: "
            << primaryEndLifePendingFullCount_.exchange(0, std::memory_order_relaxed)
            << ", pending size: " << pendingSize
            << ", queue size: " << queueSize << ", active drain workers: " << activeWorkers
            << ", limit: " << PRIMARY_END_LIFE_PENDING_LIMIT << ", rejected object: " << task.objectKey;

        RETURN_STATUS(K_TRY_AGAIN, "Primary end-life pending queue is full.");
    }
    accepted = true;
    return Status::OK();
}

Status WorkerOcEvictionManager::EnqueuePrimaryEndLifeTask(const PrimaryEndLifeTask &task)
{
    bool needStartWorker = false;
    {
        std::unique_lock<std::mutex> lock(primaryEndLifeMutex_);
        try {
            primaryEndLifeQueue_.emplace_back(task);
        } catch (const std::exception &e) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Enqueue primary end-life task failed: %s", e.what()));
        }
        // Spawn a new drain worker if the active count is below the thread limit. Existing
        // workers loop on PopPrimaryEndLifeTasks and will consume the queue, but spawning up
        // to PRIMARY_END_LIFE_THREAD_NUM workers lets end-life batches run in parallel.
        if (activeDrainWorkers_ < static_cast<int>(PRIMARY_END_LIFE_THREAD_NUM)) {
            activeDrainWorkers_++;
            needStartWorker = true;
        }
    }
    if (needStartWorker) {
        try {
            auto drainTraceID = Trace::Instance().GetTraceID();
            primaryEndLifeThreadPool_->Execute([this, drainTraceID]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(drainTraceID);
                DrainPrimaryEndLifeTasks();
            });
        } catch (const std::exception &e) {
            {
                std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
                activeDrainWorkers_--;
                // The task stays in primaryEndLifeQueue_ and will be drained by the next
                // worker (queue is non-empty, so the next Enqueue or the restart-guard in
                // DrainPrimaryEndLifeTasks spawns one). Do NOT pop_back: between emplace_back
                // (earlier lock) and here the lock was released, so another thread may have
                // appended its own task at the back - pop_back would discard that task.
            }
            primaryEndLifeDrainedCv_.notify_all();
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Submit primary end-life task failed: %s", e.what()));
        }
    }
    return Status::OK();
}

void WorkerOcEvictionManager::ClearPrimaryEndLifePending(const PrimaryEndLifeTask &task)
{
    {
        std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
        auto iter = pendingPrimaryEndLifeObjects_.find(task.objectKey);
        if (iter != pendingPrimaryEndLifeObjects_.end() && iter->second == task.version) {
            pendingPrimaryEndLifeObjects_.erase(iter);
        }
    }
    primaryEndLifeDrainedCv_.notify_all();
}

void WorkerOcEvictionManager::FinishPrimaryEndLifeTask(const PrimaryEndLifeTask &task, bool readd)
{
    {
        std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
        auto pendingIter = pendingPrimaryEndLifeObjects_.find(task.objectKey);
        if (pendingIter != pendingPrimaryEndLifeObjects_.end() && pendingIter->second == task.version) {
            pendingPrimaryEndLifeObjects_.erase(pendingIter);
        }
        auto metaIter = metaDeletedPrimaryEndLifeObjects_.find(task.objectKey);
        if (readd && task.metaDeleted) {
            metaDeletedPrimaryEndLifeObjects_[task.objectKey] = task.version;
        } else if (metaIter != metaDeletedPrimaryEndLifeObjects_.end() && metaIter->second == task.version) {
            metaDeletedPrimaryEndLifeObjects_.erase(metaIter);
        }
    }
    primaryEndLifeDrainedCv_.notify_all();
    if (readd) {
        bool trackMutation = policyUpdatePhase_.load(std::memory_order_acquire) != PolicyUpdatePhase::STABLE;
        if (trackMutation) {
            BeginTrackedPolicyMutation();
        }
        Raii mutationDone([this, &trackMutation]() {
            if (trackMutation) {
                EndTrackedPolicyMutation();
            }
        });
        std::shared_lock<std::shared_mutex> routeLock(policyRouteMutex_);
        const auto phase = policyUpdatePhase_.load(std::memory_order_acquire);
        if (!trackMutation && phase != PolicyUpdatePhase::STABLE) {
            BeginTrackedPolicyMutation();
            trackMutation = true;
        }
        EvictionCandidate retryCandidate;
        if (task.evictionCandidate.has_value()) {
            retryCandidate = *task.evictionCandidate;
        } else {
            retryCandidate.objectKey = task.objectKey;
        }
        if (phase == PolicyUpdatePhase::STABLE || phase == PolicyUpdatePhase::DRAINING) {
            policyRoute_.sourceStrategy->ReaddCandidate(retryCandidate, READD_COUNTER);
        } else {
            std::lock_guard<std::mutex> keyLock(GetPolicyMigrationLock(task.objectKey));
            policyRoute_.targetStrategy->ReaddCandidate(retryCandidate, READD_COUNTER);
        }
    }
}

void WorkerOcEvictionManager::ReaddPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks)
{
    for (const auto &task : tasks) {
        FinishPrimaryEndLifeTask(task, true);
    }
}

std::string WorkerOcEvictionManager::GetPrimaryEndLifePressure()
{
    size_t pendingSize;
    size_t queueSize;
    int activeDrainWorkers;
    {
        std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
        pendingSize = pendingPrimaryEndLifeObjects_.size();
        queueSize = primaryEndLifeQueue_.size();
        activeDrainWorkers = activeDrainWorkers_;
    }
    return FormatString("pending=%zu,queued=%zu,active_drains=%d,pending_limit=%zu", pendingSize, queueSize,
                        activeDrainWorkers, PRIMARY_END_LIFE_PENDING_LIMIT);
}

void WorkerOcEvictionManager::LogPrimaryEndLifeStage(const char *stage, double elapsedMs, size_t batchKeys,
                                                     uint64_t queueWaitMs, const char *event)
{
    if (elapsedMs >= PRIMARY_END_LIFE_SLOW_LOG_THRESHOLD_MS
        || queueWaitMs >= static_cast<uint64_t>(PRIMARY_END_LIFE_SLOW_LOG_THRESHOLD_MS)) {
        LOG(WARNING) << "PRIMARY_END_LIFE_DIAG stage=" << stage << ", elapsed_ms=" << elapsedMs
                     << ", event=" << event << ", queue_wait_ms=" << queueWaitMs << ", batch_keys=" << batchKeys
                     << ", pressure=" << GetPrimaryEndLifePressure();
        return;
    }
    VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=" << stage << ", elapsed_ms=" << elapsedMs
                          << ", event=" << event << ", queue_wait_ms=" << queueWaitMs << ", batch_keys=" << batchKeys
                          << ", pressure=" << GetPrimaryEndLifePressure();
}

void WorkerOcEvictionManager::LogPrimaryEndLifeRpcAttempt(const HostPort &masterAddr, uint32_t attempt,
                                                          double attemptElapsedMs, double totalElapsedMs,
                                                          size_t batchKeys, size_t failedKeys, const Status &rc)
{
    if (rc.IsError() || failedKeys > 0 || attemptElapsedMs >= PRIMARY_END_LIFE_SLOW_LOG_THRESHOLD_MS) {
        LOG(WARNING) << "PRIMARY_END_LIFE_DIAG stage=rpc_attempt, event=complete, master=" << masterAddr.ToString()
                     << ", attempt=" << attempt << "/" << PRIMARY_END_LIFE_DELETE_ALL_COPY_RETRY_TIMES
                     << ", elapsed_ms=" << attemptElapsedMs << ", total_elapsed_ms=" << totalElapsedMs
                     << ", batch_keys=" << batchKeys << ", failed_keys=" << failedKeys
                     << ", status=" << rc.ToString() << ", pressure=" << GetPrimaryEndLifePressure();
        return;
    }
    VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=rpc_attempt, event=complete, master="
                          << masterAddr.ToString() << ", attempt=" << attempt << "/"
                          << PRIMARY_END_LIFE_DELETE_ALL_COPY_RETRY_TIMES << ", elapsed_ms=" << attemptElapsedMs
                          << ", total_elapsed_ms=" << totalElapsedMs << ", batch_keys=" << batchKeys
                          << ", failed_keys=" << failedKeys << ", status=" << rc.ToString()
                          << ", pressure=" << GetPrimaryEndLifePressure();
}

void WorkerOcEvictionManager::DrainPrimaryEndLifeTasks()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(PRIMARY_END_LIFE_BATCH_MAX_DELAY_MS));
    while (true) {
        auto tasks = PopPrimaryEndLifeTasks();
        if (tasks.empty()) {
            OnDrainWorkerIdle();
            return;
        }
        auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
        auto queuedAtMs = tasks.front().queuedAtMs;
        auto queueWaitMs = queuedAtMs != 0 && nowMs > queuedAtMs ? nowMs - queuedAtMs : 0;
        LogPrimaryEndLifeStage("dequeue", 0, tasks.size(), queueWaitMs);
        Timer batchTimer;
        try {
            ProcessPrimaryEndLifeTasks(tasks);
        } catch (const std::exception &e) {
            // ProcessPrimaryEndLifeTasks takes a const&, so tasks is still intact.
            // Re-add the batch so pendingPrimaryEndLifeObjects_ entries that were not
            // yet processed by FinishPrimaryEndLifeTask are cleared and the objects
            // are returned to the eviction list for later retry.
            LOG(ERROR) << FormatString("ProcessPrimaryEndLifeTasks exception: %s", e.what());
            ReaddPrimaryEndLifeTasks(tasks);
        }
        LogPrimaryEndLifeStage("drain_batch", batchTimer.ElapsedMilliSecond(), tasks.size());
    }
}

void WorkerOcEvictionManager::OnDrainWorkerIdle()
{
    // Decrement the active count, then guard against the race where a new task was
    // enqueued between Pop and the count decrement: if the queue is non-empty but no
    // worker remains, restart one. Extracted from DrainPrimaryEndLifeTasks to keep
    // nesting depth within G.FUN.01-CPP limits.
    bool needRestart = false;
    {
        std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
        activeDrainWorkers_--;
        if (!primaryEndLifeQueue_.empty() && activeDrainWorkers_ == 0) {
            activeDrainWorkers_++;
            needRestart = true;
        }
    }
    primaryEndLifeDrainedCv_.notify_all();
    if (needRestart) {
        try {
            auto drainTraceID = Trace::Instance().GetTraceID();
            primaryEndLifeThreadPool_->Execute([this, drainTraceID]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(drainTraceID);
                DrainPrimaryEndLifeTasks();
            });
        } catch (const std::exception &e) {
            LOG(ERROR) << FormatString("Restart drain worker failed: %s", e.what());
            {
                std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
                activeDrainWorkers_--;
            }
            primaryEndLifeDrainedCv_.notify_all();
        }
    }
}

std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> WorkerOcEvictionManager::PopPrimaryEndLifeTasks()
{
    std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
    if (primaryEndLifeQueue_.empty()) {
        return {};
    }
    auto batchSize = std::min(primaryEndLifeQueue_.size(), PRIMARY_END_LIFE_BATCH_LIMIT);
    std::vector<PrimaryEndLifeTask> tasks;
    tasks.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        tasks.emplace_back(std::move(primaryEndLifeQueue_.front()));
        primaryEndLifeQueue_.pop_front();
    }
    return tasks;
}

void WorkerOcEvictionManager::ProcessPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks)
{
    auto traceGuard = Trace::Instance().SetTraceUUID();
    std::vector<std::string> objectKeys;
    std::unordered_map<std::string, PrimaryEndLifeTask> taskByKey;
    objectKeys.reserve(tasks.size());
    taskByKey.reserve(tasks.size());
    for (const auto &task : tasks) {
        objectKeys.emplace_back(task.objectKey);
        taskByKey.emplace(task.objectKey, task);
    }
    LogPrimaryEndLifeStage("route_group", 0, tasks.size(), 0, "start");
    Timer routeTimer;
    auto grouped = metadataRoute_.GroupOwners(objectKeys);
    LogPrimaryEndLifeStage("route_group", routeTimer.ElapsedMilliSecond(), tasks.size());
    std::unordered_set<std::string> routeFailedKeys;
    for (const auto &item : grouped.failures) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Skip primary end-life, master unavailable: %s.", item.first,
                                     item.second.ToString());
        routeFailedKeys.emplace(item.first);
    }
    std::vector<PrimaryEndLifeTask> failedTasks;
    failedTasks.reserve(routeFailedKeys.size());
    for (const auto &key : routeFailedKeys) {
        auto iter = taskByKey.find(key);
        if (iter != taskByKey.end()) {
            failedTasks.emplace_back(iter->second);
        }
    }
    ReaddPrimaryEndLifeTasks(failedTasks);

    for (const auto &item : grouped.groups) {
        HostPort masterAddr = item.first;
        std::vector<PrimaryEndLifeTask> masterTasks;
        masterTasks.reserve(item.second.size());
        for (const auto &objectKey : item.second) {
            if (routeFailedKeys.count(objectKey) == 0 && taskByKey.count(objectKey) > 0) {
                masterTasks.emplace_back(taskByKey.at(objectKey));
            }
        }
        if (masterTasks.empty()) {
            continue;
        }
        if (masterAddr.Empty()) {
            ReaddPrimaryEndLifeTasks(masterTasks);
            continue;
        }
        ProcessPrimaryEndLifeMasterBatch(masterAddr, std::move(masterTasks));
    }
}

void WorkerOcEvictionManager::ProcessPrimaryEndLifeMasterBatch(const HostPort &masterAddr,
                                                               std::vector<PrimaryEndLifeTask> tasks)
{
    LogPrimaryEndLifeStage("prepare", 0, tasks.size(), 0, "start");
    Timer prepareTimer;
    std::sort(tasks.begin(), tasks.end(),
              [](const auto &lhs, const auto &rhs) { return lhs.objectKey < rhs.objectKey; });
    std::vector<PrimaryEndLifeCandidate> candidates;
    std::vector<PrimaryEndLifeTask> skippedTasks;
    LOG_IF_ERROR(PreparePrimaryEndLifeCandidates(tasks, candidates, skippedTasks),
                 "Prepare primary end-life candidates failed.");
    ReaddPrimaryEndLifeTasks(skippedTasks);
    if (candidates.empty()) {
        LogPrimaryEndLifeStage("prepare", prepareTimer.ElapsedMilliSecond(), tasks.size());
        return;
    }
    std::vector<PrimaryEndLifeCandidate> needDeleteMetaCandidates;
    needDeleteMetaCandidates.reserve(candidates.size());
    for (const auto &candidate : candidates) {
        if (!candidate.task.metaDeleted) {
            needDeleteMetaCandidates.emplace_back(candidate);
        }
    }
    // Release W-lock before the master RPC so concurrent Get RLocks are not blocked for the RPC duration.
    UnlockPrimaryEndLifeCandidates(candidates);
    LogPrimaryEndLifeStage("prepare", prepareTimer.ElapsedMilliSecond(), tasks.size());
    std::unordered_set<std::string> failedKeys;
    Status rc = DeletePrimaryEndLifeMetadata(masterAddr, needDeleteMetaCandidates, failedKeys);
    // Re-acquire WLock per candidate for local erase only.
    LogPrimaryEndLifeStage("local_cleanup", 0, candidates.size(), 0, "start");
    Timer localCleanupTimer;
    ProcessPrimaryEndLifeLocalErase(candidates, rc, failedKeys);
    LogPrimaryEndLifeStage("local_cleanup", localCleanupTimer.ElapsedMilliSecond(), candidates.size());
}

Status WorkerOcEvictionManager::DeletePrimaryEndLifeMetadata(
    const HostPort &masterAddr, const std::vector<PrimaryEndLifeCandidate> &needDeleteMetaCandidates,
    std::unordered_set<std::string> &failedKeys)
{
    if (needDeleteMetaCandidates.empty()) {
        return Status::OK();
    }
    Timer rpcTotalTimer;
    Status rc;
    for (uint32_t attempt = 1; attempt <= PRIMARY_END_LIFE_DELETE_ALL_COPY_RETRY_TIMES; ++attempt) {
        failedKeys.clear();
        VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=rpc_attempt, event=start, master="
                              << masterAddr.ToString() << ", attempt=" << attempt << "/"
                              << PRIMARY_END_LIFE_DELETE_ALL_COPY_RETRY_TIMES
                              << ", batch_keys=" << needDeleteMetaCandidates.size()
                              << ", pressure=" << GetPrimaryEndLifePressure();
        Timer rpcAttemptTimer;
        rc = DeleteAllCopyMetaForPrimaryEndLife(masterAddr, needDeleteMetaCandidates, failedKeys);
        auto attemptElapsedMs = rpcAttemptTimer.ElapsedMilliSecond();
        LogPrimaryEndLifeRpcAttempt(masterAddr, attempt, attemptElapsedMs, rpcTotalTimer.ElapsedMilliSecond(),
                                    needDeleteMetaCandidates.size(), failedKeys.size(), rc);
        if (rc.IsOk() || !IsRetryableRpcError(rc)) {
            return rc;
        }
    }
    std::vector<std::string> forceDeleteKeys;
    forceDeleteKeys.reserve(needDeleteMetaCandidates.size());
    for (const auto &candidate : needDeleteMetaCandidates) {
        forceDeleteKeys.emplace_back(candidate.task.objectKey);
    }
    LOG(ERROR) << "Force deleting primary END_LIFE objects after DeleteAllCopyMeta RPC failed "
               << PRIMARY_END_LIFE_DELETE_ALL_COPY_RETRY_TIMES << " times, master=" << masterAddr.ToString()
               << ", total_elapsed_ms=" << rpcTotalTimer.ElapsedMilliSecond()
               << ", object_keys=" << JoinKeys(forceDeleteKeys) << ", last_status=" << rc.ToString()
               << ", pressure=" << GetPrimaryEndLifePressure();
    failedKeys.clear();
    return Status::OK();
}

void WorkerOcEvictionManager::ProcessPrimaryEndLifeLocalErase(std::vector<PrimaryEndLifeCandidate> &candidates,
                                                              Status &rc, std::unordered_set<std::string> &failedKeys)
{
    for (auto &candidate : candidates) {
        PrimaryEndLifeTask finishTask = candidate.task;
        bool failed = !candidate.task.metaDeleted && (rc.IsError() || failedKeys.count(candidate.task.objectKey) > 0);
        if (!failed) {
            Status reacquireRc = ReacquireAndValidateForLocalDelete(candidate);
            if (reacquireRc.IsError()) {
                candidate.entry.reset();
                failed = true;
                finishTask.metaDeleted = true;
            } else {
                Raii wUnlockRaii([&candidate]() {
                    if (candidate.entry != nullptr) {
                        candidate.entry->WUnlock();
                    }
                });
                bool removeAsyncSend = candidate.entry != nullptr && (*candidate.entry)->IsWriteBackL2CacheEvictMode();
                Status deleteRc = DeletePrimaryEndLifeLocal(candidate);
                failed = deleteRc.IsError();
                finishTask.metaDeleted = failed;
                if (!failed && removeAsyncSend) {
                    RemovePrimaryEndLifeAsyncSend(candidate.task.objectKey);
                }
            }
        }
        FinishPrimaryEndLifeTask(finishTask, failed);
    }
}

Status WorkerOcEvictionManager::ReacquireAndValidateForLocalDelete(PrimaryEndLifeCandidate &candidate)
{
    // Re-lookup entry from table (may have been erased during RPC window).
    std::shared_ptr<SafeObjType> entry;
    RETURN_IF_NOT_OK(objectTable_->Get(candidate.task.objectKey, entry));
    // TryWLock with bounded retry, matching TryLockPrimaryEndLifeTask's policy.
    Status rc;
    for (uint32_t i = 0; i < PRIMARY_END_LIFE_LOCK_RETRY_TIMES; ++i) {
        rc = entry->TryWLock();
        if (rc.IsOk() || rc.GetCode() != K_TRY_AGAIN) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(PRIMARY_END_LIFE_LOCK_RETRY_INTERVAL_MS));
    }
    RETURN_IF_NOT_OK(rc);
    // Version and rebalance admission can both change while the object lock is released for the metadata RPC.
    // Never erase a source object after a rebalance task has claimed it.
    if ((*entry)->GetCreateTime() != candidate.task.version || IsObjectBeingRebalanced(candidate.task.objectKey)) {
        entry->WUnlock();
        RETURN_STATUS(StatusCode::K_NOT_FOUND,
                      FormatString("[ObjectKey %s] Object changed or entered rebalance during end-life RPC window, "
                                   "skip local erase.",
                                   candidate.task.objectKey));
    }
    candidate.entry = entry;
    return Status::OK();
}

#ifdef WITH_TESTS
Status WorkerOcEvictionManager::ReacquirePrimaryEndLifeForTest(const std::string &objectKey, uint64_t version,
                                                               std::shared_ptr<SafeObjType> &entry)
{
    PrimaryEndLifeCandidate candidate{ { objectKey, version, CacheType::MEMORY }, nullptr };
    auto rc = ReacquireAndValidateForLocalDelete(candidate);
    entry = candidate.entry;
    return rc;
}
#endif

Status WorkerOcEvictionManager::PreparePrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeTask> &tasks,
                                                                std::vector<PrimaryEndLifeCandidate> &candidates,
                                                                std::vector<PrimaryEndLifeTask> &skippedTasks)
{
    std::unordered_map<int, uint64_t> selectedSizeByCache;
    const size_t candidateBegin = candidates.size();
    bool handoffCandidates = false;
    Raii unlockOnError([&candidates, &handoffCandidates, candidateBegin] {
        if (handoffCandidates) {
            return;
        }
        for (size_t i = candidateBegin; i < candidates.size(); ++i) {
            if (candidates[i].entry != nullptr) {
                candidates[i].entry->WUnlock();
            }
        }
        candidates.resize(candidateBegin);
    });
    for (const auto &task : tasks) {
        if (!IsAboveLowWaterMark(task.needSize, 0, task.cacheType)) {
            skippedTasks.emplace_back(task);
            continue;
        }
        std::shared_ptr<SafeObjType> entry;
        Status rc = TryLockPrimaryEndLifeTask(task, entry);
        if (rc.IsError()) {
            skippedTasks.emplace_back(task);
            continue;
        }
        bool entryLocked = true;
        Raii unlockEntry([&entry, &entryLocked] {
            if (entryLocked) {
                entry->WUnlock();
            }
        });
        if (!IsPrimaryEndLifeTaskStillEvictable(task, *entry)) {
            skippedTasks.emplace_back(task);
            continue;
        }
        auto cacheKey = static_cast<int>(task.cacheType);
        auto releaseSize = GetPrimaryEndLifeReleaseSize(*entry);
        auto budget = GetPrimaryEndLifeReleaseBudget(task.cacheType, task.needSize);
        auto selectedSize = selectedSizeByCache[cacheKey];
        // The first candidate is allowed so a single large object can still relieve pressure.
        if (selectedSize != 0 && releaseSize > budget - std::min(budget, selectedSize)) {
            skippedTasks.emplace_back(task);
            continue;
        }
        auto maxSize = std::numeric_limits<uint64_t>::max();
        selectedSizeByCache[cacheKey] = releaseSize > maxSize - selectedSize ? maxSize : selectedSize + releaseSize;
        candidates.emplace_back(PrimaryEndLifeCandidate{ task, entry });
        entryLocked = false;
    }
    handoffCandidates = true;
    return Status::OK();
}

Status WorkerOcEvictionManager::TryLockPrimaryEndLifeTask(const PrimaryEndLifeTask &task,
                                                          std::shared_ptr<SafeObjType> &entry)
{
    RETURN_IF_NOT_OK(objectTable_->Get(task.objectKey, entry));
    Status rc;
    for (uint32_t i = 0; i < PRIMARY_END_LIFE_LOCK_RETRY_TIMES; ++i) {
        rc = entry->TryWLock();
        if (rc.IsOk() || rc.GetCode() != K_TRY_AGAIN) {
            return rc;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(PRIMARY_END_LIFE_LOCK_RETRY_INTERVAL_MS));
    }
    return rc;
}

bool WorkerOcEvictionManager::IsPrimaryEndLifeTaskStillEvictable(const PrimaryEndLifeTask &task,
                                                                 const SafeObjType &entry)
{
    if (entry->GetCreateTime() != task.version) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey << "] Skip primary end-life, version changed, "
                              << "expected: " << task.version << ", current: " << entry->GetCreateTime();
        return false;
    }
    if (!entry->stateInfo.IsPrimaryCopy()) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey << "] Skip primary end-life, not primary copy.";
        return false;
    }
    bool isBinary = entry->IsBinary();
    if (!isBinary && !entry->HasL2Cache()) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                              << "] Skip primary end-life, non-binary object has no L2 cache.";
        return false;
    }
    if (isBinary && entry->GetShmUnit() == nullptr) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                              << "] Skip primary end-life, binary object has no shm.";
        return false;
    }
    bool hasL2Cache = IsObjectExistInL2Cache(entry);
    if (entry->modeInfo.GetCacheType() == CacheType::DISK) {
        bool evictable = !hasL2Cache && entry->IsNoneL2CacheEvictMode();
        if (!evictable) {
            VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                                  << "] Skip primary end-life, disk object is not none-L2 evictable.";
        }
        return evictable;
    }
    if (entry->IsWriteBackL2CacheEvictMode()) {
        return true;
    }
    bool evictable = !hasL2Cache && entry->IsNoneL2CacheEvictMode();
    if (!evictable) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                              << "] Skip primary end-life, memory object is not evictable.";
    }
    return evictable;
}

uint64_t WorkerOcEvictionManager::GetPrimaryEndLifeReleaseBudget(CacheType cacheType, uint64_t needSize)
{
    auto realUsage = datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(
        ServiceType::OBJECT, static_cast<memory::CacheType>(cacheType));
    auto max = std::numeric_limits<uint64_t>::max();
    realUsage = realUsage > max - needSize ? max : realUsage + needSize;
    auto lowWater = GetLowWaterMark(cacheType);
    return realUsage > lowWater ? realUsage - lowWater : 0;
}

Status WorkerOcEvictionManager::DeleteAllCopyMetaForPrimaryEndLife(
    const HostPort &masterAddr, const std::vector<PrimaryEndLifeCandidate> &candidates,
    std::unordered_set<std::string> &failedKeys)
{
    auto req = BuildPrimaryEndLifeDeleteReq(candidates, true);
    master::DeleteAllCopyMetaRspPb rsp;
    GetRequestContext()->reqTimeoutDuration.Init(PRIMARY_END_LIFE_DELETE_ALL_COPY_TIMEOUT_MS);
    ApiDeadline::Instance().Reset();
    Raii resetTimeout([] { GetRequestContext()->reqTimeoutDuration.Reset(); });
    RETURN_IF_NOT_OK(DeleteAllCopyMetaOnce(masterAddr, req, rsp));

    std::vector<PrimaryEndLifeRedirectGroup> redirectGroups;
    RETURN_IF_NOT_OK(CollectPrimaryEndLifeSourceResult(masterAddr, candidates, rsp, failedKeys, redirectGroups));
    ForwardPrimaryEndLifeRedirectGroups(masterAddr, redirectGroups, failedKeys);
    return Status::OK();
}

Status WorkerOcEvictionManager::DeleteAllCopyMetaOnce(const HostPort &masterAddr,
                                                      master::DeleteAllCopyMetaReqPb &req,
                                                      master::DeleteAllCopyMetaRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(!masterAddr.Empty(), K_NOT_FOUND, "Cannot find master for DeleteAllCopyMeta.");
    auto workerMasterApi =
        worker::WorkerMasterOCApi::CreateWorkerMasterOCApi(masterAddr, localAddress_, akSkManager_, masterOc_);
    RETURN_IF_NOT_OK(workerMasterApi->Init());
    return workerMasterApi->DeleteAllCopyMeta(req, rsp);
}

master::DeleteAllCopyMetaReqPb WorkerOcEvictionManager::BuildPrimaryEndLifeDeleteReq(
    const std::vector<PrimaryEndLifeCandidate> &candidates, bool allowRedirect) const
{
    master::DeleteAllCopyMetaReqPb req;
    req.set_address(localAddress_.ToString());
    req.set_redirect(allowRedirect);
    req.set_async_delete(true);
    for (const auto &candidate : candidates) {
        auto *objKeyVersionPb = req.add_ids_with_version();
        objKeyVersionPb->set_id(candidate.task.objectKey);
        objKeyVersionPb->set_version(candidate.task.version);
    }
    return req;
}

Status WorkerOcEvictionManager::CollectPrimaryEndLifeSourceResult(
    const HostPort &sourceMaster, const std::vector<PrimaryEndLifeCandidate> &candidates,
    const master::DeleteAllCopyMetaRspPb &rsp, std::unordered_set<std::string> &failedKeys,
    std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups)
{
    if (rsp.info().empty()) {
        return CollectPrimaryEndLifeDeleteResult(rsp, failedKeys);
    }
    if (rsp.meta_is_moving()) {
        RETURN_STATUS(K_TRY_AGAIN, "DeleteAllCopyMeta meta is moving.");
    }

    PrimaryEndLifeSourceClassification classification;
    CollectPrimaryEndLifeSourceKeyResults(candidates, rsp, failedKeys, classification);
    CollectPrimaryEndLifeRedirectResults(sourceMaster, rsp, failedKeys, classification);
    if (classification.malformedRedirectResponse) {
        for (const auto &candidate : candidates) {
            failedKeys.emplace(candidate.task.objectKey);
        }
        LOG(WARNING) << "PRIMARY_END_LIFE_DIAG stage=redirect_classify, source_master=" << sourceMaster.ToString()
                     << ", batch_keys=" << candidates.size() << ", event=malformed_redirect";
        return Status::OK();
    }

    BuildPrimaryEndLifeRedirectGroups(candidates, classification, redirectGroups);
    Status lastRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (lastRc.IsError() && classification.reportedFailures.empty()) {
        if (!classification.hasKnownRedirect) {
            return lastRc;
        }
        for (const auto &candidate : candidates) {
            const auto &objectKey = candidate.task.objectKey;
            bool classified = classification.terminalKeys.count(objectKey) > 0
                              || classification.invalidRedirectKeys.count(objectKey) > 0
                              || classification.redirectByKey.count(objectKey) > 0;
            if (!classified) {
                failedKeys.emplace(objectKey);
            }
        }
    }
    if (classification.unknownResponseKeys > 0) {
        LOG(WARNING) << "PRIMARY_END_LIFE_DIAG stage=redirect_classify, source_master=" << sourceMaster.ToString()
                     << ", batch_keys=" << candidates.size()
                     << ", unknown_response_keys=" << classification.unknownResponseKeys;
    }
    return Status::OK();
}

void WorkerOcEvictionManager::CollectPrimaryEndLifeSourceKeyResults(
    const std::vector<PrimaryEndLifeCandidate> &candidates, const master::DeleteAllCopyMetaRspPb &rsp,
    std::unordered_set<std::string> &failedKeys, PrimaryEndLifeSourceClassification &classification)
{
    classification.candidateKeys.reserve(candidates.size());
    for (const auto &candidate : candidates) {
        classification.candidateKeys.emplace(candidate.task.objectKey);
    }

    classification.reportedFailures.reserve(rsp.failed_object_keys().size());
    for (const auto &objectKey : rsp.failed_object_keys()) {
        if (classification.candidateKeys.count(objectKey) == 0) {
            ++classification.unknownResponseKeys;
            continue;
        }
        classification.reportedFailures.emplace(objectKey);
        failedKeys.emplace(objectKey);
    }

    classification.terminalKeys.reserve(rsp.objs_without_meta().size() + rsp.outdated_objs().size());
    auto collectTerminalKeys = [&classification](const auto &objectKeys) {
        for (const auto &objectKey : objectKeys) {
            if (classification.candidateKeys.count(objectKey) > 0) {
                classification.terminalKeys.emplace(objectKey);
            } else {
                ++classification.unknownResponseKeys;
            }
        }
    };
    collectTerminalKeys(rsp.objs_without_meta());
    collectTerminalKeys(rsp.outdated_objs());
}

void WorkerOcEvictionManager::CollectPrimaryEndLifeRedirectResults(
    const HostPort &sourceMaster, const master::DeleteAllCopyMetaRspPb &rsp,
    std::unordered_set<std::string> &failedKeys, PrimaryEndLifeSourceClassification &classification)
{
    classification.redirectByKey.reserve(classification.candidateKeys.size());
    for (const auto &redirectInfo : rsp.info()) {
        if (redirectInfo.change_meta_ids().empty()) {
            classification.malformedRedirectResponse = true;
            continue;
        }
        HostPort redirectMaster;
        Status parseRc = redirectMaster.ParseString(redirectInfo.redirect_meta_address());
        bool validTarget = parseRc.IsOk() && !redirectMaster.Empty() && redirectMaster != sourceMaster;
        for (const auto &objectKey : redirectInfo.change_meta_ids()) {
            if (classification.candidateKeys.count(objectKey) == 0) {
                ++classification.unknownResponseKeys;
                classification.malformedRedirectResponse = true;
                continue;
            }
            classification.hasKnownRedirect = true;
            if (classification.reportedFailures.count(objectKey) > 0) {
                continue;
            }
            if (!validTarget || classification.terminalKeys.count(objectKey) > 0) {
                classification.invalidRedirectKeys.emplace(objectKey);
                classification.redirectByKey.erase(objectKey);
                failedKeys.emplace(objectKey);
                continue;
            }
            auto [it, inserted] = classification.redirectByKey.emplace(
                objectKey, PrimaryEndLifeRedirectChoice{ redirectMaster, redirectInfo.topology_version() });
            if (!inserted && it->second.masterAddress != redirectMaster) {
                classification.invalidRedirectKeys.emplace(objectKey);
                classification.redirectByKey.erase(it);
                failedKeys.emplace(objectKey);
            } else if (!inserted) {
                it->second.topologyVersion = std::max(it->second.topologyVersion, redirectInfo.topology_version());
            }
        }
    }
}

void WorkerOcEvictionManager::BuildPrimaryEndLifeRedirectGroups(
    const std::vector<PrimaryEndLifeCandidate> &candidates,
    const PrimaryEndLifeSourceClassification &classification,
    std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups)
{
    std::map<HostPort, PrimaryEndLifeRedirectGroup> groupsByTarget;
    for (const auto &candidate : candidates) {
        const auto &objectKey = candidate.task.objectKey;
        if (classification.reportedFailures.count(objectKey) > 0
            || classification.invalidRedirectKeys.count(objectKey) > 0
            || classification.terminalKeys.count(objectKey) > 0) {
            continue;
        }
        auto redirectIt = classification.redirectByKey.find(objectKey);
        if (redirectIt == classification.redirectByKey.end()) {
            continue;
        }
        auto &group = groupsByTarget[redirectIt->second.masterAddress];
        group.masterAddress = redirectIt->second.masterAddress;
        group.topologyVersion = std::max(group.topologyVersion, redirectIt->second.topologyVersion);
        group.candidates.emplace_back(candidate);
    }
    redirectGroups.reserve(groupsByTarget.size());
    for (auto &item : groupsByTarget) {
        redirectGroups.emplace_back(std::move(item.second));
    }
}

void WorkerOcEvictionManager::ForwardPrimaryEndLifeRedirectGroups(
    const HostPort &sourceMaster, const std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups,
    std::unordered_set<std::string> &failedKeys)
{
    for (const auto &group : redirectGroups) {
        auto markGroupFailed = [&group, &failedKeys] {
            for (const auto &candidate : group.candidates) {
                failedKeys.emplace(candidate.task.objectKey);
            }
        };
        auto req = BuildPrimaryEndLifeDeleteReq(group.candidates, false);
        master::DeleteAllCopyMetaRspPb rsp;
        Status rc = DeleteAllCopyMetaOnce(group.masterAddress, req, rsp);
        size_t failedBefore = failedKeys.size();
        if (rc.IsError()) {
            markGroupFailed();
        } else {
            std::unordered_set<std::string> groupKeys;
            groupKeys.reserve(group.candidates.size());
            for (const auto &candidate : group.candidates) {
                groupKeys.emplace(candidate.task.objectKey);
            }
            bool malformedRedirect = false;
            for (const auto &redirectInfo : rsp.info()) {
                if (redirectInfo.change_meta_ids().empty()) {
                    malformedRedirect = true;
                    break;
                }
                for (const auto &objectKey : redirectInfo.change_meta_ids()) {
                    if (groupKeys.count(objectKey) == 0) {
                        malformedRedirect = true;
                        break;
                    }
                }
                if (malformedRedirect) {
                    break;
                }
            }
            std::unordered_set<std::string> targetFailedKeys;
            Status collectRc = CollectPrimaryEndLifeDeleteResult(rsp, targetFailedKeys);
            if (collectRc.IsError() || malformedRedirect) {
                rc = collectRc.IsError()
                         ? collectRc
                         : Status(K_TRY_AGAIN, "Forwarded DeleteAllCopyMeta returned invalid redirect.");
                markGroupFailed();
            } else {
                for (const auto &objectKey : targetFailedKeys) {
                    if (groupKeys.count(objectKey) > 0) {
                        failedKeys.emplace(objectKey);
                    }
                }
            }
        }
        VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=redirect_forward, source_master="
                              << sourceMaster.ToString() << ", target_master=" << group.masterAddress.ToString()
                              << ", topology_version=" << group.topologyVersion
                              << ", batch_keys=" << group.candidates.size()
                              << ", failed_keys=" << (failedKeys.size() - failedBefore)
                              << ", status=" << rc.ToString();
    }
}

Status WorkerOcEvictionManager::CollectPrimaryEndLifeDeleteResult(
    const master::DeleteAllCopyMetaRspPb &rsp, std::unordered_set<std::string> &failedKeys)
{
    Status lastRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    const bool hasReportedFailure = !rsp.failed_object_keys().empty();
    failedKeys.insert(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    for (const auto &redirectInfo : rsp.info()) {
        failedKeys.insert(redirectInfo.change_meta_ids().begin(), redirectInfo.change_meta_ids().end());
    }
    if (rsp.meta_is_moving()) {
        RETURN_STATUS(K_TRY_AGAIN, "DeleteAllCopyMeta meta is moving.");
    }
    if (lastRc.IsError() && !hasReportedFailure) {
        return lastRc;
    }
    return Status::OK();
}

Status WorkerOcEvictionManager::CollectDeleteAllCopyMetaResult(const master::DeleteAllCopyMetaRspPb &rsp,
                                                               std::unordered_set<std::string> &failedKeys)
{
    Status lastRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    failedKeys.insert(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    failedKeys.insert(rsp.outdated_objs().begin(), rsp.outdated_objs().end());
    failedKeys.insert(rsp.objs_without_meta().begin(), rsp.objs_without_meta().end());
    for (const auto &redirectInfo : rsp.info()) {
        failedKeys.insert(redirectInfo.change_meta_ids().begin(), redirectInfo.change_meta_ids().end());
    }
    if (rsp.meta_is_moving()) {
        RETURN_STATUS(K_TRY_AGAIN, "DeleteAllCopyMeta meta is moving.");
    }
    RETURN_IF_NOT_OK(lastRc);
    return Status::OK();
}

void WorkerOcEvictionManager::RemovePrimaryEndLifeAsyncSend(const std::string &objectKey)
{
    if (auto sp = asyncSendManager_.lock()) {
        sp->Remove(objectKey);
    }
}

Status WorkerOcEvictionManager::DeletePrimaryEndLifeLocal(const PrimaryEndLifeCandidate &candidate)
{
    const auto &objectKey = candidate.task.objectKey;
    auto &entry = *candidate.entry;
    const bool hadCpuCopy = entry.Get() != nullptr && !entry->stateInfo.IsCacheInvalid();
    if (entry->IsSpilled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WorkerOcSpill::Instance()->Delete(objectKey, true),
                                         FormatString("[ObjectKey %s] Delete from disk failed", objectKey));
    }
    entry->stateInfo.SetSpillState(false);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectTable_->Erase(objectKey, entry),
                                     FormatString("Failed to erase object %s from object table", objectKey));
    if (hadCpuCopy) {
        PublishKvRemovedEvent(kvEventPublisher_, objectKey, kKvEventMediumCpu);
    }
    return Status::OK();
}

#ifdef WITH_TESTS
Status WorkerOcEvictionManager::DeletePrimaryEndLifeLocalForTest(const std::string &objectKey,
                                                                 const std::shared_ptr<SafeObjType> &entry)
{
    PrimaryEndLifeTask task{ objectKey, 0, CacheType::MEMORY };
    return DeletePrimaryEndLifeLocal(PrimaryEndLifeCandidate{ std::move(task), entry });
}
#endif

uint64_t WorkerOcEvictionManager::GetPrimaryEndLifeReleaseSize(const SafeObjType &entry)
{
    auto dataSize = entry->GetDataSize();
    auto metaSize = entry->GetMetadataSize();
    auto max = std::numeric_limits<uint64_t>::max();
    return dataSize > max - metaSize ? max : dataSize + metaSize;
}

void WorkerOcEvictionManager::UnlockPrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeCandidate> &candidates)
{
    for (const auto &candidate : candidates) {
        if (candidate.entry != nullptr) {
            candidate.entry->WUnlock();
        }
    }
}

Status WorkerOcEvictionManager::SpillImpl(const std::string &objectKey, uint64_t version)
{
    INJECT_POINT("worker.SubmitSpillTask");
    // Retry case: 1. try lock failed; 2. Spill failed;
    // Ignore case: 1. object not exists; 2. Shm released; 3. version changed.
    std::shared_ptr<SafeObjType> entryPtr;
    {
        Status rc = GetAndLockEntry(objectKey, version, false, entryPtr);
        if (rc.IsError()) {
            return rc.GetCode() == K_TRY_AGAIN ? rc : Status::OK();
        }

        bool locked = true;
        Raii rUnlockRaii([entryPtr, &locked] {
            if (locked) {
                entryPtr->RUnlock();
            }
        });
        SafeObjType &entry = *entryPtr;

        auto dataSize = entry->GetDataSize();
        auto metaSize = entry->GetMetadataSize();
        TryEvictSpilledObjects(dataSize);
        ShmGuard shmGuard(entry->GetShmUnit(), dataSize, metaSize);
        if (WorkerOcServiceCrudCommonApi::ShmEnable() && !shmGuard.TryRLatch(false)) {
            return Status(K_TRY_AGAIN, "TryRLatch failed");
        }
        // ShmGuard will hold the shm unit.
        auto shmUnit = entry->GetShmUnit();
        RETURN_RUNTIME_ERROR_IF_NULL(shmUnit);
        const void *buffer = static_cast<uint8_t *>(shmUnit->GetPointer()) + metaSize;
        bool isNoneL2EvictType = entry->IsNoneL2CacheEvictMode();
        bool canEvict = entry->HasL2Cache() || isNoneL2EvictType;
        entryPtr->RUnlock();
        locked = false;
        (void)locked;
        rc = WorkerOcSpill::Instance()->Spill(objectKey, buffer, dataSize, canEvict);
        if (isNoneL2EvictType && rc.GetCode() == StatusCode::K_NO_SPACE) {
            // If we lock failed, we can do nothing but retry next time.
            Status s = GetAndLockEntry(objectKey, version, true, entryPtr);
            if (s.IsError()) {
                return rc;
            }
            Raii wUnlockRaii([entryPtr] { entryPtr->WUnlock(); });
            rc = DeleteNoneL2CacheEvictableObject({ objectKey, entry });
        }
        RETURN_IF_NOT_OK(rc);
    }

    Status rc = GetAndLockEntry(objectKey, version, true, entryPtr);
    if (rc.IsError()) {
        // Rollback if failed.
        LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey), "Delete failed");
        return rc.GetCode() == K_TRY_AGAIN ? rc : Status::OK();
    }

    Raii wUnlockRaii([entryPtr] { entryPtr->WUnlock(); });
    LOG_IF_ERROR((*entryPtr)->FreeResources(), "SafeObj free failed");
    (*entryPtr)->stateInfo.SetSpillState(true);

    return Status::OK();
}

std::future<WorkerOcEvictionManager::SpillResult> WorkerOcEvictionManager::SubmitSpillTask(const std::string &objectKey,
                                                                                           uint64_t version)
{
    auto traceId = Trace::Instance().GetTraceID();
    return spillTaskThreadPool_->Submit([this, objectKey, version, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Timer timer;
        auto rc = SpillImpl(objectKey, version);
        return SpillResult{ .rc = rc, .elapsed = timer.ElapsedMilliSecond() };
    });
}

size_t WorkerOcEvictionManager::ReleaseSpillFutures(std::unordered_map<std::string, SpillTask> &spillTasks,
                                                    EvictionRetryList &evictFailedIds, bool last)
{
    size_t spilledSize = 0;
    for (auto iter = spillTasks.begin(); iter != spillTasks.end();) {
        auto &future = iter->second.future;
        if (!future.valid()) {
            ++iter;
            continue;
        }
        auto &trace = iter->second.trace;
        if (!last) {
            std::future_status taskStatus = future.wait_for(std::chrono::microseconds(0));
            if (taskStatus != std::future_status::ready) {
                ++iter;
                continue;
            }
        } else {
            future.wait();
        }
        auto result = future.get();
        Status spillRc = result.rc;
        spilledSize += trace->objectSize;
        if (spillRc.IsError()) {
            // Transient spill lock contention uses Q1, persistent spill failure uses READD.
            auto counter = spillRc.GetCode() == StatusCode::K_TRY_AGAIN ? Q1 : READD_COUNTER;
            evictFailedIds.push_back({ iter->second.candidate, counter });
        }
        trace->rc = spillRc;
        trace->spillCost = result.elapsed;
        spillTasks.erase(iter++);
    }
    return spilledSize;
}

void WorkerOcEvictionManager::TryEvictSpilledObjects(uint64_t objectSize)
{
    if (!WorkerOcSpill::Instance()->IsSpaceExceedHWM(objectSize)) {
        return;
    }
    if (spillEvictTaskThreadPool_->GetRunningTasksNum() == 0) {
        auto spillTraceID = Trace::Instance().GetTraceID();
        spillEvictTaskThreadPool_->Execute([this, objectSize, spillTraceID]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(spillTraceID);
            EvictSpilledObjects(objectSize);
        });
    } else {
        LOG(INFO) << "Spill evict task running...";
    }
}

void WorkerOcEvictionManager::EvictSpilledObjects(uint64_t objectSize)
{
    EvictFailedList evictFailedIds;
    std::unordered_map<std::string, SpillTask> spillTasks;
    auto &spillEvictionList = WorkerOcSpill::Instance()->GetEvictionList();
    LOG(INFO) << "Spill eviction list size before evict: " << spillEvictionList.Size();

    size_t needSkipCount = 0;
    bool forceCompact = false;
    while (spillEvictionList.Size() != 0 && needSkipCount <= spillEvictionList.Size()
           && WorkerOcSpill::Instance()->IsActiveSpillSizeExceedLWM(objectSize)) {
        std::string candidateId;
        if (spillEvictionList.FindEvictCandidate(candidateId).IsError()) {
            LOG(ERROR) << "FindEvictCandidate failed, EvictionList is empty.";
            continue;
        }

        std::shared_ptr<SafeObjType> entry;
        std::optional<EvictionList::Node> retrySnapshot;
        Status rc = GetAndLockEntry(candidateId, entry, retrySnapshot);
        if (rc.IsError()) {
            if (retrySnapshot.has_value()) {
                evictFailedIds.emplace_back(candidateId, Q1);
            }
            needSkipCount++;
            continue;
        }
        Raii unLockRaii([entry]() { entry->WUnlock(); });

        if (!IsSpilledObjectEvictable(entry)) {
            needSkipCount++;
            continue;
        }

        needSkipCount = 0;
        if (entry->Get()->IsNoneL2CacheEvictMode()) {
            rc = DeleteNoneL2CacheEvictableObject(ObjectKV(candidateId, *entry));
        } else {
            rc = DeleteL2CacheEvictableObject(ObjectKV(candidateId, *entry));
        }

        if (rc.IsError()) {
            evictFailedIds.emplace_back(candidateId, READD_COUNTER);
        } else {
            forceCompact = true;
            (void)spillEvictionList.Erase(candidateId);
        }
    }

    FinishEvictSpilledObjects(spillEvictionList, evictFailedIds, objectSize, forceCompact);

    LOG(INFO) << "Spill eviction list size after evict:" << spillEvictionList.Size()
              << ", need retry size:" << evictFailedIds.size() << ", force compact: " << forceCompact;
}

void WorkerOcEvictionManager::FinishEvictSpilledObjects(
    EvictionList &spillEvictionList, const EvictFailedList &evictFailedIds, uint64_t objectSize, bool &forceCompact)
{
    for (const auto &objKeyCounter : evictFailedIds) {
        spillEvictionList.Add(objKeyCounter.first, objKeyCounter.second);
    }
    const double ratio =
        (WorkerOcSpill::Instance()->LowWaterFactor() + WorkerOcSpill::Instance()->HighWaterFactor()) / 2.0;
    forceCompact &= WorkerOcSpill::Instance()->IsSpaceExceed(ratio, objectSize);
    if (forceCompact) {
        WorkerOcSpill::Instance()->ForceCompact();
    }
}

bool WorkerOcEvictionManager::IsSpilledObjectEvictable(const std::shared_ptr<SafeObjType> &entry)
{
    auto entryPtr = entry->Get();
    return entryPtr->IsWriteThroughMode() || entryPtr->IsNoneL2CacheEvictMode()
           || (entryPtr->IsWriteBackMode() && entryPtr->stateInfo.IsWriteBackDone());
}

Status WorkerOcEvictionManager::DeleteNoneL2CacheEvictableObject(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    VLOG(DEBUG_LOG_LEVEL) << "DeleteNoneL2CacheEvictableObject start. ObjectKey: " << objectKey;
    HostPort masterAddr;
    RETURN_IF_NOT_OK(metadataRoute_.ResolveOwner(objectKey, masterAddr));

    auto buildReq = [this, &objectKey](bool allowRedirect) {
        master::DeleteAllCopyMetaReqPb request;
        request.add_object_keys(objectKey);
        request.set_address(localAddress_.ToString());
        request.set_redirect(allowRedirect);
        return request;
    };
    auto req = buildReq(true);
    master::DeleteAllCopyMetaRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(DeleteAllCopyMetaOnce(masterAddr, req, rsp),
                                     FormatString("DeleteAllCopyMeta failed, objectKey %s.", objectKey));

    if (!rsp.info().empty()) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            !rsp.meta_is_moving() && rsp.failed_object_keys().empty() && rsp.outdated_objs().empty()
                && rsp.objs_without_meta().empty(),
            K_TRY_AGAIN, "DeleteAllCopyMeta redirect response contains conflicting result.");
        HostPort redirectMaster;
        uint64_t topologyVersion = 0;
        size_t redirectKeyCount = 0;
        for (const auto &redirectInfo : rsp.info()) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!redirectInfo.change_meta_ids().empty(), K_TRY_AGAIN,
                                                 "DeleteAllCopyMeta returned an empty redirect.");
            for (const auto &redirectKey : redirectInfo.change_meta_ids()) {
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(redirectKey == objectKey, K_TRY_AGAIN,
                                                     "DeleteAllCopyMeta redirect response contains unknown key.");
                ++redirectKeyCount;
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(redirectKeyCount == 1, K_TRY_AGAIN,
                                                     "DeleteAllCopyMeta returned ambiguous redirects.");
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(redirectMaster.ParseString(redirectInfo.redirect_meta_address()),
                                                 "Parse DeleteAllCopyMeta redirect target failed.");
                topologyVersion = redirectInfo.topology_version();
            }
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(redirectKeyCount == 1 && !redirectMaster.Empty()
                                                 && redirectMaster != masterAddr,
                                             K_TRY_AGAIN, "DeleteAllCopyMeta returned invalid redirect target.");

        auto redirectReq = buildReq(false);
        master::DeleteAllCopyMetaRspPb redirectRsp;
        Status redirectRc = DeleteAllCopyMetaOnce(redirectMaster, redirectReq, redirectRsp);
        VLOG(DEBUG_LOG_LEVEL) << "PRIMARY_END_LIFE_DIAG stage=redirect_forward, source_master="
                              << masterAddr.ToString() << ", target_master=" << redirectMaster.ToString()
                              << ", topology_version=" << topologyVersion << ", batch_keys=1"
                              << ", status=" << redirectRc.ToString();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(redirectRc, "Forwarded DeleteAllCopyMeta failed.");
        for (const auto &redirectInfo : redirectRsp.info()) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!redirectInfo.change_meta_ids().empty(), K_TRY_AGAIN,
                                                 "Forwarded DeleteAllCopyMeta returned an empty redirect.");
        }
        rsp = std::move(redirectRsp);
    }

    std::unordered_set<std::string> failedKeys;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CollectDeleteAllCopyMetaResult(rsp, failedKeys), "Delete from master failed.");
    if (!failedKeys.empty()) {
        RETURN_STATUS_LOG_ERROR(K_TRY_AGAIN, FormatString("DeleteAllCopyMeta needs retry, objectKey %s.", objectKey));
    }

    if (objectKV.GetObjEntry()->IsSpilled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WorkerOcSpill::Instance()->Delete(objectKey, true),
                                         FormatString("[ObjectKey %s] Delete from disk failed", objectKey));
    }
    objectKV.GetObjEntry()->stateInfo.SetSpillState(false);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectTable_->Erase(objectKey, objectKV.GetObjEntry()),
                                     FormatString("Failed to erase object %s from object table", objectKey));
    VLOG(DEBUG_LOG_LEVEL) << "DeleteNoneL2CacheEvictableObject end. ObjectKey: " << objectKey;
    return Status::OK();
}

Status WorkerOcEvictionManager::DeleteL2CacheEvictableObject(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    auto &entry = objectKV.GetObjEntry();
    RETURN_IF_NOT_OK_EXCEPT(WorkerOcSpill::Instance()->Delete(objectKey, true), StatusCode::K_NOT_FOUND);
    entry->stateInfo.SetSpillState(false);
    return Status::OK();
}

Status WorkerOcEvictionManager::GetAndLockEntry(const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
                                                std::optional<EvictionList::Node> &retrySnapshot)
{
    retrySnapshot.reset();
    Status rc = objectTable_->Get(objectKey, entry);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object not in ObjectTable, %s.", objectKey, rc.ToString());
        Erase(objectKey);
        return rc;
    }
    rc = entry->TryWLock();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object TryWLock failed, %s.", objectKey, rc.ToString());
        EvictionList::Node erasedNode;
        Status eraseRc = ExtractEvictionNode(objectKey, erasedNode);
        if (rc.GetCode() == K_TRY_AGAIN && eraseRc.IsOk()) {
            // Transient lock contention: re-add with Q1 so the object does not gain 5x clock
            // protection that would invert LRU order (issue #750).
            retrySnapshot = std::move(erasedNode);
        }
    }
    return rc;
}

Status WorkerOcEvictionManager::GetAndLockEntry(const std::string &objectKey, uint64_t version, bool isWrite,
                                                std::shared_ptr<SafeObjType> &entryPtr)
{
    Status rc = objectTable_->Get(objectKey, entryPtr);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object not in ObjectTable, %s.", objectKey, rc.ToString());
        return rc;
    }
    if (isWrite) {
        rc = entryPtr->TryWLock();
    } else {
        rc = entryPtr->TryRLock();
    }
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] %s failed, %s.", objectKey, isWrite ? "TryWLock" : "TryRLock",
                                     rc.ToString());
        return rc;
    }

    bool success = false;
    Raii raii([entryPtr, isWrite, &success] {
        if (!success) {
            isWrite ? entryPtr->WUnlock() : entryPtr->RUnlock();
        }
    });
    SafeObjType &entry = *entryPtr;
    if (entry->GetShmUnit() == nullptr) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object's shm has been free.", objectKey);
        RETURN_STATUS(K_RUNTIME_ERROR, "ShmUnit is null");
    }

    if (entry->GetCreateTime() != version) {
        LOG(WARNING) << FormatString("[ObjectKey %s] version changed, expected:%zu, current:%zu.", objectKey, version,
                                     entry->GetCreateTime());
        RETURN_STATUS(K_RUNTIME_ERROR, "version changed");
    }
    success = true;
    (void)success;
    return Status::OK();
}

bool WorkerOcEvictionManager::IsObjectEvictable(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    const SafeObjType &entry = objectKV.GetObjEntry();
    EvictionList::Node evictionNode;
    if (GetObjectInfo(objectKey, evictionNode).IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object not in EvictionList.", objectKey);
        return false;
    }
    bool isBinary = entry->IsBinary();
    if (!isBinary && !entry->HasL2Cache()) {
        LOG(ERROR) << FormatString("[ObjectId %s] Object doesn't have L2 cache, it's wrong status.", objectKey);
        Erase(objectKey);
        return false;
    }
    if (isBinary && entry->GetShmUnit() == nullptr) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object's shm has been free.", objectKey);
        Erase(objectKey);
        return false;
    }
    return true;
}

bool WorkerOcEvictionManager::IsObjectExistInL2Cache(const SafeObjType &entry)
{
    return entry->IsWriteThroughMode() || (entry->IsWriteBackMode() && entry->stateInfo.IsWriteBackDone());
}

std::string WorkerOcEvictionManager::GetActionName(Action action)
{
    switch (action) {
        case Action::FREE_MEMORY:
            return "free memory";
        case Action::DELETE:
            return "delete";
        case Action::SPILL:
            return "spill";
        case Action::RETAIN:
            return "retain";
        case Action::END_LIFE:
            return "life end";
        default:
            return "unknown";
    }
}

bool EvictWhenMemoryExceedThrehold(const std::string &keyInfo, uint64_t needSize,
                                   const std::shared_ptr<WorkerOcEvictionManager> &evictionManager, ServiceType type,
                                   CacheType cacheType)
{
    uint64_t realMemoryUsed = 0;
    uint64_t memOccupied = 0;
    uint64_t maxAvailableMemorySize = 0;
    memory::CacheType memCacheType = static_cast<memory::CacheType>(cacheType);
    uint64_t memThreshold = 0;
    auto realObjMemoryUsed =
        datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT, memCacheType);
    auto getMemThresInitVal = [](uint64_t maxAvailableMemorySize, uint64_t evictionThresholdMB) {
        return std::max(static_cast<uint64_t>(maxAvailableMemorySize * GetEvictionHighWaterFactor()),
                        maxAvailableMemorySize > evictionThresholdMB * MB_TO_BYTES
                            ? maxAvailableMemorySize - evictionThresholdMB * MB_TO_BYTES
                            : 0);
    };
    if (UINT64_MAX - realMemoryUsed < needSize) {
        // If needSize + realMemoryUsed > UINT64_MAX, it means that the needSize is very large,
        // it could never be success, so skip evict.
        return false;
    }
    if (type == ServiceType::OBJECT) {
        realMemoryUsed = realObjMemoryUsed;
        memOccupied = realMemoryUsed + needSize;
        maxAvailableMemorySize = std::min(
            datasystem::memory::Allocator::Instance()->GetMaxMemorySize(type, memCacheType),
            (datasystem::memory::Allocator::Instance()->GetTotalRealMemoryFree(memCacheType) + realMemoryUsed));
        memThreshold =
            getMemThresInitVal(maxAvailableMemorySize, FLAGS_eviction_reserve_mem_threshold_mb);
    } else if (type == ServiceType::STREAM) {
        realMemoryUsed =
            datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::STREAM) + realObjMemoryUsed;
        memOccupied = realMemoryUsed + needSize;
        maxAvailableMemorySize = datasystem::memory::Allocator::Instance()->GetMaxMemoryLimit();
        memThreshold =
            getMemThresInitVal(maxAvailableMemorySize, FLAGS_eviction_reserve_mem_threshold_mb);
    }
    VLOG(1) << FormatString("Allocate memory for %s, size = %lu, memOccupied = %lu, memThreshold = %lu", keyInfo,
                            needSize, memOccupied, memThreshold);
    if (memOccupied >= memThreshold && realObjMemoryUsed > 0) {
        PerfPoint evictPoint(PerfKey::WORKER_EVICT_TASK);
        evictionManager->Evict(needSize, cacheType);
        evictPoint.Record();
        return true;
    }
    return false;
}

}  // namespace object_cache
}  // namespace datasystem
