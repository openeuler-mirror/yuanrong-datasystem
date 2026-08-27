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
 * Description: Defines EvictionList and WorkerOcEvictionManager Interface.
 */
#ifndef DATASYSTEM_WORKER_OC_EVICTION_MANAGER_H
#define DATASYSTEM_WORKER_OC_EVICTION_MANAGER_H

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <future>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/metadata_route_resolver.h"
#include "datasystem/worker/object_cache/eviction_list.h"
#include "datasystem/worker/object_cache/eviction_strategy.h"
#include "datasystem/worker/object_cache/kv_event/kv_event_publisher.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {
class AsyncSendManager;
}
}  // namespace datasystem

namespace datasystem {
namespace ut {
class EvictionManagerTest;
class SpillEvictionTest;
}
}  // namespace datasystem

namespace datasystem {

namespace master {
class MasterOCServiceImpl;
}
namespace object_cache {

class WorkerOcEvictionManager : public std::enable_shared_from_this<WorkerOcEvictionManager> {
public:
    enum class PolicyUpdatePhase : uint8_t { STABLE, DRAINING, MIGRATING, VERIFYING, ACTIVATING };

    struct PolicyStateSnapshot {
        PolicyUpdatePhase phase{ PolicyUpdatePhase::STABLE };
        EvictionPolicy activePolicy{ EvictionPolicy::CLOCK };
        uint64_t epoch{ 0 };
        EvictionPolicy targetPolicy{ EvictionPolicy::CLOCK };
    };

    struct CopyWatermarkStats {
        EvictionPolicy policy{ EvictionPolicy::CLOCK };
        uint64_t coldPrimaryCopyCount{ 0 };
        uint64_t warmPrimaryCopyCount{ 0 };
        uint64_t hotPrimaryCopyCount{ 0 };
        uint64_t totalPrimaryCopyCount{ 0 };
        uint64_t coldPrimaryCopyBytes{ 0 };
        uint64_t warmPrimaryCopyBytes{ 0 };
        uint64_t hotPrimaryCopyBytes{ 0 };
        uint64_t totalPrimaryCopyBytes{ 0 };
        double counterP50{ 0.0 };
        double counterP90{ 0.0 };
        double counterP99{ 0.0 };
        uint64_t cappedPrimaryCopyCount{ 0 };
    };
    using CopyWatermarkObserver = std::function<void(const CopyWatermarkStats &)>;

    struct PersistedPolicyState {
        EvictionPolicy activePolicy{ EvictionPolicy::CLOCK };
        uint64_t activeEpoch{ 0 };
        bool hasTransitionIntent{ false };
        EvictionPolicy targetPolicy{ EvictionPolicy::CLOCK };
        uint64_t transitionEpoch{ 0 };
    };
    using PolicyStateLoader = std::function<Status(PersistedPolicyState &state, bool &found)>;
    using PolicyStateStorer = std::function<Status(const PersistedPolicyState &state)>;

    /**
     * @brief Construct WorkerOcEvictionManager.
     * @param[in] objectTable The pointer to a ObjectTable.
     * @param[in] localAddress Address of the worker.
     * @param[in] masterAddress Address of the local master.
     * @param[in] metadataRoute Metadata owner resolver that outlives this manager.
     * @param[in] masterOc Pointer to the master object cache service.
     */
    WorkerOcEvictionManager(std::shared_ptr<ObjectTable> objectTable, HostPort localAddress, HostPort masterAddress,
                            const worker::MetadataRouteResolver &metadataRoute,
                            master::MasterOCServiceImpl *masterOc = nullptr);

    ~WorkerOcEvictionManager();

    /**
     * @brief Initialize the WorkerOcEvictionManager Object.
     * @param[in] gRefTable Global ref count table.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @return Status of the call.
     */
    Status Init(const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &gRefTable,
                std::shared_ptr<AkSkManager> akSkManager);

    /**
     * @brief Install the worker-local policy state store and restore last-good state.
     *
     * This must be called before object-cache services start adding eviction memberships.
     */
    Status InitPolicyStateStore(PolicyStateLoader loader, PolicyStateStorer storer);

    /**
     * @brief Add a object to EvictionManager.
     * @param[in] objectKey The objectKey to add.
     */
    void Add(const std::string &objectKey);

    /**
     * @brief Apply heat received with a migrated primary copy.
     * @param[in] objectKey The migrated object key.
     * @param[in] heat Point-in-time heat snapshot from the source.
     * @param[in] mergeExisting Preserve a higher heat already observed by an existing target replica.
     * @return Status of the call.
     */
    Status ApplyMigratedHeat(const std::string &objectKey, double heat, bool mergeExisting);

    /**
     * @brief Erase a object from EvictionManager.
     * @param[in] objectKey The object to erase.
     */
    void Erase(const std::string &objectKey);

    /**
     * @brief Trigger asynchronous eviction task.
     * @param[in] needSize The need size.
     * @param[in] cacheType The type of cache.
     */
    void Evict(uint64_t needSize = 0, CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Touch an object on a cache hit (Get memory hit). Dispatches to the active
     *        eviction strategy (clock: refill curCounter; heat: add size-normalized heat + lastAccess).
     * @param[in] objectKey The hit object key.
     */
    void OnCacheHit(const std::string &objectKey, uint64_t migratableSize = 0);

    /**
     * @brief Apply a cache hit immediately when the route is stably CLOCK and therefore does not need object size.
     * @return True if the CLOCK mutation was applied; false if the caller must resolve size and use OnCacheHit.
     */
    bool TryOnCacheHitWithoutSize(const std::string &objectKey);

    /**
     * @brief Touch an object loaded by a successful Get from spill, L2, or a remote worker.
     *        Heat records the current access and grants one size-normalized admission credit.
     */
    void OnRefill(const std::string &objectKey, uint64_t migratableSize = 0);

    /**
     * @brief Apply a refill immediately when the route is stably CLOCK and therefore does not need object size.
     * @return True if the CLOCK mutation was applied; false if the caller must resolve size and use OnRefill.
     */
    bool TryOnRefillWithoutSize(const std::string &objectKey);

    /**
     * @brief Whether foreground Get paths need allocator-accounted object size for the active policy route.
     * @return True while Heat is active or is the target of an in-progress policy update.
     */
    bool NeedsMigratableSize() const noexcept
    {
        return needsMigratableSize_.load(std::memory_order_acquire);
    }

    /**
     * @brief Start a worker-local Clock/Heat policy conversion.
     *
     * New eviction rounds are fenced before this call drains an in-flight round.
     * Object Value ownership is unchanged; only eviction metadata is copied to
     * the inactive list.
     */
    Status BeginPolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch);

    /**
     * @brief Convert at most maxKeys source memberships into the target list.
     * @param[out] done True when the source list is empty.
     */
    Status MigratePolicyBatch(size_t maxKeys, bool &done);

    /**
     * @brief Atomically publish the target list after migration completes.
     */
    Status CommitPolicyUpdate(uint64_t epoch);

    /**
     * @brief Execute one idempotent control-plane step for a master-delivered update.
     */
    Status HandlePolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch, size_t maxKeys, bool &complete);

    /**
     * @brief Validate a rollout without draining eviction or mutating either list.
     */
    Status PrecheckPolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch, size_t migrationBatchSize,
                                uint64_t minimumAvailableMemoryBytes, uint64_t maximumSourceObjects,
                                uint64_t deadlineUnixMs, uint64_t &sourceObjects);

    /**
     * @brief Return bounded progress counters for resource-report acknowledgements.
     */
    void GetPolicyUpdateProgress(uint64_t &totalObjects, uint64_t &migratedObjects) const;

    EvictionPolicy GetActiveEvictionPolicy() const;
    uint64_t GetPolicyUpdateEpoch() const;
    PolicyStateSnapshot GetPolicyStateSnapshot() const;
    Status ValidateRebalancePolicy(uint32_t policy, uint64_t epoch) const;

    /**
     * @brief Run heat decay and collect post-decay hot-primary statistics in one full-list pass.
     *        Used by heat rebalance resource reporting to avoid separate O(N) object-table scans.
     */
    Status MaintainHeatAndCollectHotPrimaryStats(CopyWatermarkStats &stats);

    /**
     * @brief Collect stable primary-copy count and allocator-accounted bytes without heat maintenance.
     *        The default clock path does not call this; workload telemetry enables it through a test inject point.
     */
    Status CollectPrimaryCopyStats(uint64_t &totalPrimaryCopyCount, uint64_t &totalPrimaryCopyBytes);

    /**
     * @brief Register the callback that publishes a read-only copy-watermark snapshot.
     *
     * Workload telemetry refreshes the snapshot immediately before a periodic worker resource report. Collection
     * never decays or otherwise mutates policy counters. Clock considers counter >= Q2 hot; Heat uses
     * rebalance_heat_hot_counter_threshold.
     */
    void SetCopyWatermarkObserver(CopyWatermarkObserver observer);

    /**
     * @brief Collect and publish the current copy-watermark snapshot through the registered observer.
     *        Called by the Worker resource-report control path; does not mutate policy counters.
     */
    void RefreshCopyWatermarkSnapshot();

    /**
     * @brief Register and refresh the read-only hot-primary snapshot used by master scheduling.
     *
     * A successful keep-local rebalance changes primary ownership without reducing source memory usage. Refreshing
     * this snapshot before reporting task completion prevents the master from scheduling against the previous
     * 30-second maintenance snapshot. Neither method performs heat decay.
     */
    void SetHotPrimaryReportObserver(CopyWatermarkObserver observer);
    void RefreshHotPrimaryReport();

    /**
     * @brief Get all object infos (for testing).
     * @param[out] res All objects info in EvictionManager.
     * @param[out] oldest The oldest object in EvictionManager.
     * @return Status of the call.
     */
    Status GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest);

    /**
     * @brief Snapshot one eviction node.
     */
    Status GetObjectInfo(const std::string &objectKey, EvictionList::Node &node);

    /**
     * @brief Get a bounded object info snapshot from eviction list oldest position.
     * @param[in] maxScanCount The maximum number of nodes to copy.
     * @param[out] res The bounded object info snapshot.
     * @return Status of the call.
     */
    Status GetObjectsInfoFromOldest(size_t maxScanCount, std::vector<EvictionList::Node> &res);

    /**
     * @brief Snapshot Heat nodes ordered from lowest to highest heat for memory-pressure rebalance.
     */
    Status GetLowestHeatObjects(size_t maxCount, std::vector<EvictionList::Node> &res);

    /**
     * @brief Setter function to assign the async send manager.
     * @param[in] asyncSendManager The async send manager pointer to assign
     */
    void SetAsyncSendManager(std::shared_ptr<AsyncSendManager> asyncSendManager)
    {
        asyncSendManager_ = asyncSendManager;
    }

    /**
     * @brief Setter function to assign the KV event publisher.
     * @param[in] kvEventPublisher The KV event publisher pointer to assign.
     */
    void SetKvEventPublisher(std::shared_ptr<KvEventPublisher> kvEventPublisher)
    {
        kvEventPublisher_ = kvEventPublisher;
    }

    /**
     * @brief Evict clear object.
     * @param[in] objectKV The object that need to evict.
     * @return Status of the call.
     */
    Status EvictClearObject(ObjectKV &objectKV);

    /**
     * @brief Whether evict is running.
     * @return true Evict is running.
     */
    bool IsRunning()
    {
        return !isDone_;
    }

    /**
     * @brief Try to mark an object as being rebalanced.
     * @param[in] objectKey The object key.
     * @return true if this call marks the object successfully, false if it has been marked already.
     */
    bool TryMarkRebalancingObject(const std::string &objectKey);

    /**
     * @brief Remove rebalance mark from one object.
     * @param[in] objectKey The object key.
     */
    void UnmarkRebalancingObject(const std::string &objectKey);

    /**
     * @brief Check whether an object is being rebalanced.
     * @param[in] objectKey The object key.
     * @return true if object is being rebalanced.
     */
    bool IsObjectBeingRebalanced(const std::string &objectKey) const;

#ifdef WITH_TESTS
    void EvictionTaskForTest(uint64_t needSize, CacheType cacheType = CacheType::MEMORY)
    {
        EvictionTask(needSize, cacheType);
    }

    Status EvictDeleteObjectForTest(ObjectKV &objectKV)
    {
        EvictDeletedObjects deletedObjects;
        return EvictObject(objectKV, Action::DELETE, &deletedObjects);
    }

    Status EvictFreeMemoryForTest(ObjectKV &objectKV)
    {
        return EvictObject(objectKV, Action::FREE_MEMORY);
    }

    Status DeletePrimaryEndLifeLocalForTest(const std::string &objectKey,
                                            const std::shared_ptr<SafeObjType> &entry);
    Status ReacquirePrimaryEndLifeForTest(const std::string &objectKey, uint64_t version,
                                          std::shared_ptr<SafeObjType> &entry);
    void AddHeatNodeForTest(const std::string &objectKey, double heat, uint64_t nowMs);
    Status CollectCopyWatermarkStatsForTest(CopyWatermarkStats &stats);
    void NotifyCopyWatermarkObserverForTest();
    void MarkPrimaryEndLifeTaskActiveForTest(const std::string &objectKey, uint64_t version);
    void FinishPrimaryEndLifeTaskAndWorkerForTest(const std::string &objectKey, uint64_t version);
    void HoldStableRouteReaderForTest(const std::function<void()> &callback);
#endif

private:
    enum class Action : int { UNKNOWN, DELETE, FREE_MEMORY, SPILL, END_LIFE, RETAIN };
    using EvictDeletedObjects = std::unordered_map<std::string, uint64_t>;

    struct EvictionTraceAggregator;  // forward declaration for EvictionTrace::aggregator_

    struct EvictionTrace {
        Timer timer;
        std::string taskId;
        uint64_t objectSize;
        std::unordered_map<std::string, uint64_t> objectKeySizeMap;
        Action action;
        std::string info;
        double spillCost;
        Status rc;
        // Per-task aggregator pointer. Set by EvictionTask before the trace is
        // consumed by TryEvictObject or spill futures. When non-null, the
        // destructor calls aggregator_->Add(*this) instead of using a shared
        // thread_local instance.
        //
        // Lifetime contract: the aggregator MUST outlive every EvictionTrace
        // that points to it. EvictionTask guarantees this via:
        // 1. Declaration order: aggregator declared before spillTasks (reverse
        //    destruction ensures aggregator outlives spillTasks' traces).
        // 2. ReleaseSpillFutures(..., true): blocking wait at end of function
        //    drains all async futures before aggregator goes out of scope.
        //
        // When nullptr (legacy/other code paths), the destructor falls back
        // to a static thread_local EvictionTraceAggregator.
        EvictionTraceAggregator* aggregator_ = nullptr;
        EvictionTrace(std::string id) : taskId(std::move(id)), objectSize(0), action(Action::UNKNOWN), spillCost(0)
        {
        }
        ~EvictionTrace();

        void AddObjectKeySize(const std::string &key, uint64_t size)
        {
            if (size == 0) {
                LOG(WARNING) << "The trace object key [" << key << "] is zero, skip it";
                return;
            }
            auto it = objectKeySizeMap.find(key);
            if (it == objectKeySizeMap.end()) {
                objectKeySizeMap.emplace(key, size);
                objectSize += size;
                return;
            }
            LOG(WARNING) << "The trace object key [" << key << "] is repeated, update it";
            objectSize += size;
            objectSize -= std::min(objectSize, it->second);
            it->second = size;
        }
    };

    struct ActionSummary {
        uint64_t lastLogTimeMs{ 0 };
        std::vector<std::string> successKeys;
        std::vector<std::string> failedKeys;
        uint64_t tryAgainCount{ 0 };
        uint64_t notReadyCount{ 0 };
    };

    struct EvictionTraceAggregator {
        ~EvictionTraceAggregator();

        void Add(const EvictionTrace &trace);

        const std::unordered_map<Action, ActionSummary> &GetSummaries() const;

    private:
        void Flush(Action action, ActionSummary &summary);
        void FlushIfNeeded(Action action, ActionSummary &summary, uint64_t nowMs);

        std::unordered_map<Action, ActionSummary> summaries_;
    };

    struct SpillResult {
        Status rc;
        double elapsed;
    };

    struct SpillTask {
        std::future<SpillResult> future;
        std::unique_ptr<EvictionTrace> trace;
        EvictionCandidate candidate;
    };

    struct PrimaryEndLifeTask {
        std::string objectKey;
        uint64_t version;
        CacheType cacheType;
        uint64_t needSize{ 0 };
        // True when master metadata was already deleted and only local cleanup should be retried.
        bool metaDeleted{ false };
        uint64_t queuedAtMs{ 0 };
        // Present when the task originated from an eviction selection. It carries Heat retry state across the async
        // primary-end-life lane without consulting strategy-global mutable bookkeeping.
        std::optional<EvictionCandidate> evictionCandidate{ std::nullopt };
        HostPort lastAttemptOwner{ "", 0 };
        uint64_t lastAttemptTopologyVersion{ 0 };
        uint8_t retryableFailureCount{ 0 };
        HostPort redirectTarget{ "", 0 };
        uint64_t redirectTopologyVersion{ 0 };
        uint64_t logicalAttemptDeadlineMs{ 0 };
    };

    struct DelayedPrimaryEndLifeTask {
        uint64_t readyAtMs{ 0 };
        uint64_t sequence{ 0 };
        PrimaryEndLifeTask task;
    };

    struct DelayedPrimaryEndLifeTaskCompare {
        bool operator()(const DelayedPrimaryEndLifeTask &lhs, const DelayedPrimaryEndLifeTask &rhs) const
        {
            return lhs.readyAtMs == rhs.readyAtMs ? lhs.sequence > rhs.sequence : lhs.readyAtMs > rhs.readyAtMs;
        }
    };

    struct PrimaryEndLifeOwnerLane {
        bool inFlight{ false };
        std::list<PrimaryEndLifeTask> waitingTasks;
    };

    struct PrimaryEndLifeOwnerBatch {
        HostPort owner;
        uint64_t topologyVersion{ 0 };
        bool redirectAttempt{ false };
        std::vector<PrimaryEndLifeTask> tasks;
        PrimaryEndLifeOwnerLane *ownerLane{ nullptr };
    };

    using PrimaryEndLifeTaskMap = std::unordered_map<std::string, PrimaryEndLifeTask>;
    using PrimaryEndLifeOwnerBatchMap = std::map<std::pair<HostPort, bool>, PrimaryEndLifeOwnerBatch>;
    using PrimaryEndLifeBatchLane = std::pair<PrimaryEndLifeOwnerBatch *, PrimaryEndLifeOwnerLane *>;

    struct StagedPrimaryEndLifeTasks {
        PrimaryEndLifeOwnerLane *lane;
        std::list<PrimaryEndLifeTask> tasks;
    };

    struct PrimaryEndLifeCandidate {
        PrimaryEndLifeTask task;
        std::shared_ptr<SafeObjType> entry;
    };

    struct PrimaryEndLifeRedirectGroup {
        HostPort masterAddress;
        uint64_t topologyVersion{ 0 };
        std::vector<PrimaryEndLifeCandidate> candidates;
    };

    struct PrimaryEndLifeRedirectChoice {
        HostPort masterAddress;
        uint64_t topologyVersion{ 0 };
    };

    struct PrimaryEndLifeSourceClassification {
        std::unordered_set<std::string> candidateKeys;
        std::unordered_set<std::string> reportedFailures;
        std::unordered_set<std::string> terminalKeys;
        std::unordered_map<std::string, PrimaryEndLifeRedirectChoice> redirectByKey;
        std::unordered_set<std::string> invalidRedirectKeys;
        size_t unknownResponseKeys{ 0 };
        bool hasKnownRedirect{ false };
        bool malformedRedirectResponse{ false };
    };

    using EvictFailedList = std::vector<std::pair<std::string, uint8_t>>;
    struct EvictionRetry {
        EvictionCandidate candidate;
        uint8_t counter{ 0 };
    };
    using EvictionRetryList = std::vector<EvictionRetry>;

    struct PolicyRoute {
        uint64_t epoch{ 0 };
        EvictionPolicy sourcePolicy{ EvictionPolicy::CLOCK };
        EvictionList *sourceList{ nullptr };
        std::shared_ptr<EvictionStrategy> sourceStrategy;
        HeatPolicyConfig sourceHeatConfig;
        EvictionPolicy targetPolicy{ EvictionPolicy::CLOCK };
        EvictionList *targetList{ nullptr };
        std::shared_ptr<EvictionStrategy> targetStrategy;
        HeatPolicyConfig targetHeatConfig;
    };

    static constexpr size_t POLICY_MIGRATION_LOCK_COUNT = 256;
    static constexpr size_t POLICY_MIGRATION_LOCK_ALIGNMENT = 64;
    struct alignas(POLICY_MIGRATION_LOCK_ALIGNMENT) PolicyMigrationLock {
        std::mutex mutex;
    };

    static constexpr size_t STABLE_ROUTE_READER_SLOT_COUNT = 64;
    static constexpr size_t STABLE_ROUTE_READER_SLOT_ALIGNMENT = 64;
    struct alignas(STABLE_ROUTE_READER_SLOT_ALIGNMENT) StableRouteReaderSlot {
        std::atomic<uint64_t> count{ 0 };
    };

    class StableRouteReadGuard {
    public:
        explicit StableRouteReadGuard(WorkerOcEvictionManager &manager);
        ~StableRouteReadGuard();
        explicit operator bool() const;

        StableRouteReadGuard(const StableRouteReadGuard &) = delete;
        StableRouteReadGuard &operator=(const StableRouteReadGuard &) = delete;

    private:
        WorkerOcEvictionManager &manager_;
        size_t slot_{ 0 };
        bool acquired_{ false };
    };

    enum class PolicyMutationKind : uint8_t { ADD, CACHE_HIT, REFILL, ERASE, EXTRACT };
    Status RoutePolicyMutation(const std::string &objectKey, PolicyMutationKind kind, uint64_t migratableSize = 0,
                               EvictionList::Node *snapshot = nullptr);
    bool TryApplyClockMutationWithoutSize(const std::string &objectKey, PolicyMutationKind kind);
    Status RoutePolicyMutationDuringUpdate(const std::string &objectKey, PolicyMutationKind kind,
                                           uint64_t migratableSize, EvictionList::Node *snapshot);
    static Status ApplyPolicyMutation(const PolicyRoute &route, const std::string &objectKey, PolicyMutationKind kind,
                                      uint64_t migratableSize, EvictionList::Node *snapshot);
    Status ConvertPolicySnapshot(const EvictionList::Node &source, EvictionPolicy sourcePolicy,
                                 EvictionPolicy targetPolicy, const HeatPolicyConfig &targetHeatConfig,
                                 EvictionList::Node &target);

    Status PersistPolicyState(const PersistedPolicyState &state) const;

    Status PersistTransitionIntent(EvictionPolicy activePolicy, uint64_t activeEpoch, EvictionPolicy targetPolicy,
                                   uint64_t epoch);

    Status PersistLastGood(EvictionPolicy activePolicy, uint64_t epoch);

    Status AuditPolicyUpdateMembership(uint64_t epoch, uint64_t &auditedGeneration);

    static bool IsEligibleEvictionMembership(const ObjectInterface &object);

    void BeginTrackedPolicyMutation();

    void EndTrackedPolicyMutation();
    Status MoveOnePolicyNode(const std::string &objectKey);
    std::mutex &GetPolicyMigrationLock(const std::string &objectKey);
    bool TryAcquireStableRouteReader(size_t &slot);
    void ReleaseStableRouteReader(size_t slot);
    bool StableRouteReadersDrained() const;
    void NotifyEvictionStopped();

    /**
     * @brief Get the evict action name.
     * @param[in] action The evict action.
     * @return The evict action name.
     */
    static std::string GetActionName(Action action);

    /**
     * @brief Remove meta in master.
     * @param[in,out] objectKeyVersions Evicted object versions, updated with per-owner removal results.
     * @return Status of the metadata removal pass.
     */
    Status RemoveMetaFromMasterForEviction(EvictDeletedObjects &objectKeyVersions);

    /**
     * @brief Remove eviction metadata for one metadata-owner group.
     * @param[in] masterAddr Metadata owner address.
     * @param[in] objectKeys Object keys routed to the owner.
     * @param[in] objectKeyVersions Object versions supplied by the eviction caller.
     * @param[out] failedObjects Objects that must remain pending.
     * @param[out] lastRc Last group error returned to the caller.
     */
    void RemoveEvictionMetaGroup(const HostPort &masterAddr, const std::vector<std::string> &objectKeys,
                                 const EvictDeletedObjects &objectKeyVersions, EvictDeletedObjects &failedObjects,
                                 Status &lastRc);

    /**
     * @brief Send one eviction remove-meta request without following its response redirects.
     * @param[in] masterAddr Metadata owner address.
     * @param[in] objectKeys Object keys routed to the owner.
     * @param[in] objectKeyVersions Object versions supplied by the eviction caller.
     * @param[in] allowRedirect Whether the contacted metadata owner may redirect the request.
     * @param[out] rsp Remove-meta response returned by the contacted owner.
     * @return Status of the request.
     */
    Status RemoveEvictionMetaOnce(const HostPort &masterAddr, const std::vector<std::string> &objectKeys,
                                  const EvictDeletedObjects &objectKeyVersions, bool allowRedirect,
                                  master::RemoveMetaRspPb &rsp);

    /**
     * @brief Build the remove-meta request used by eviction metadata cleanup.
     * @param[in] objectKeys Object keys routed to the metadata owner.
     * @param[in] objectKeyVersions Object versions supplied by the eviction caller.
     * @param[in] localAddress Local worker address.
     * @param[in] redirect Whether the contacted metadata owner may redirect the request.
     * @return Remove-meta request for eviction cleanup.
     */
    static master::RemoveMetaReqPb BuildEvictionRemoveMetaReq(const std::vector<std::string> &objectKeys,
                                                              const EvictDeletedObjects &objectKeyVersions,
                                                              const HostPort &localAddress, bool redirect);

    /**
     * @brief Execute the selected eviction action for a locked object.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @param[in] nextAction The next action.
     * @return Status of the call.
     */
    Status EvictObject(ObjectKV &objectKV, Action nextAction, EvictDeletedObjects *deletedObjects = nullptr,
                       CacheType cacheType = CacheType::MEMORY, uint64_t needSize = 0,
                       const EvictionCandidate *candidate = nullptr);

    /**
     * @brief Run eviction for one locked object and update async spill bookkeeping.
     * @param[in] entry The object entry that need to spill.
     * @param[in] trace The evict trace object.
     * @param[out] pendingSpillSize The size of data to be spill.
     * @param[out] spillTasks The spill task list.
     * @param[out] locked Still locked or not.
     * @return Status
     */
    Status TryEvictObject(std::shared_ptr<SafeObjType> &entry, std::unique_ptr<EvictionTrace> trace,
                          size_t &pendingSpillSize, std::unordered_map<std::string, SpillTask> &spillTasks,
                          bool &locked, const EvictionCandidate &candidate, CacheType cacheType = CacheType::MEMORY,
                          EvictDeletedObjects *deletedObjects = nullptr, uint64_t needSize = 0);

    /**
     * @brief Eviction task, asynchronous.
     * @param[in] needSize The need size.
     * @param[in] cacheType The type of cache.
     */
    void EvictionTask(uint64_t needSize, CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Indicate if now is above low water mark.
     * @param[in] needSize The need size in bytes.
     * @param[in] pendingSpillSize The pending spill size in bytes.
     * @return True if now is above low water mark.
     */
    bool IsAboveLowWaterMark(uint64_t needSize, size_t pendingSpillSize, CacheType cacheType);

    /**
     * @brief Get the next action in eviction task for an object.
     * @param[in] entry The object entry that need to get next action.
     * @param[in] trace The evict trace object.
     * @param[in] pendingSpillSize The size of data to be spill.
     */
    void GetObjectNextAction(SafeObjType &entry, std::unique_ptr<EvictionTrace> &trace, size_t pendingSpillSize);

    void SubmitAsyncMasterTask(const EvictDeletedObjects &objectKeyVersions);
    void AsyncMasterTask(const EvictDeletedObjects &objectKeyVersions);

    /**
     * @brief Submit a primary-copy END_LIFE object to the async primary end-life lane.
     * @param[in] objectKV The object selected by the memory eviction loop.
     * @param[in] cacheType The cache type being evicted.
     * @param[in] needSize The foreground memory demand that triggered this eviction round.
     * @param[out] accepted Whether this object is newly accepted into the pending set.
     * @return Status of the submit operation.
     */
    Status SubmitPrimaryEndLifeTask(const ObjectKV &objectKV, CacheType cacheType, uint64_t needSize, bool &accepted,
                                    const EvictionCandidate *candidate);

    Status StartPrimaryEndLifeWorkers();

    /**
     * @brief Reserve a primary end-life task in the pending set before enqueueing it.
     * @param[in,out] task The primary end-life task to reserve.
     * @param[out] accepted Whether the task was inserted into the pending set.
     * @return Status of the reservation.
     */
    Status ReservePrimaryEndLifeTask(PrimaryEndLifeTask &task, bool &accepted);

    /**
     * @brief Enqueue a reserved primary end-life task and start the drain worker when needed.
     * @param[in] task The primary end-life task to enqueue.
     * @return Status of the enqueue operation.
     */
    Status EnqueuePrimaryEndLifeTask(const PrimaryEndLifeTask &task);

    /**
     * @brief Remove a primary end-life task from the pending set.
     * @param[in] task The primary end-life task whose pending mark should be cleared.
     */
    void ClearPrimaryEndLifePending(const PrimaryEndLifeTask &task);

    /**
     * @brief Finish a primary end-life task and optionally readd it to the eviction list.
     * @param[in] task The primary end-life task that has finished one lane attempt.
     * @param[in] readd Whether the object should return to the eviction list for a later attempt.
     */
    void FinishPrimaryEndLifeTask(const PrimaryEndLifeTask &task, bool readd);

    /**
     * @brief Readd primary end-life tasks that could not be safely deleted in the lane attempt.
     * @param[in] tasks The skipped or failed tasks to readd.
     */
    void ReaddPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks);

    /**
     * @brief Persistently drain primary end-life tasks until manager shutdown.
     */
    void DrainPrimaryEndLifeTasks();

    /**
     * @brief Wait for ready work or the earliest delayed retry and pop one bounded raw batch.
     * @return A batch of queued primary end-life tasks.
     */
    std::vector<PrimaryEndLifeTask> WaitAndPopPrimaryEndLifeTasks();

    void PromoteReadyPrimaryEndLifeTasks(uint64_t nowMs);

    void ScheduleDelayedPrimaryEndLifeTasks(std::vector<PrimaryEndLifeTask> tasks);

    void EnqueueReadyPrimaryEndLifeTasks(std::vector<PrimaryEndLifeTask> tasks);

    void ReleasePrimaryEndLifeOwner(PrimaryEndLifeOwnerLane *ownerLane) noexcept;

    /**
     * @brief Route tasks, park busy owners, and process at most one owner batch.
     * @param[in] tasks The tasks popped from the primary end-life queue.
     */
    void ProcessPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks);

    void IndexPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks, std::vector<std::string> &objectKeys,
                                  PrimaryEndLifeTaskMap &taskByKey);

    void ReaddPrimaryEndLifeRouteFailures(const worker::MetaOwnerRouteGroups &grouped,
                                          const PrimaryEndLifeTaskMap &taskByKey);

    PrimaryEndLifeOwnerBatchMap BuildPrimaryEndLifeOwnerBatches(const worker::MetaOwnerRouteGroups &grouped,
                                                                 const PrimaryEndLifeTaskMap &taskByKey);

    PrimaryEndLifeBatchLane StagePrimaryEndLifeOwnerBatchesLocked(
        PrimaryEndLifeOwnerBatchMap &batches, std::vector<PrimaryEndLifeBatchLane> &batchLanes,
        std::vector<PrimaryEndLifeOwnerLane *> &insertedLanes,
        std::vector<StagedPrimaryEndLifeTasks> &stagedTasks);

    void CommitPrimaryEndLifeOwnerBatchLocked(const PrimaryEndLifeBatchLane &selectedBatchLane,
                                              std::vector<StagedPrimaryEndLifeTasks> &stagedTasks,
                                              std::optional<PrimaryEndLifeOwnerBatch> &selected);

    void RollbackPrimaryEndLifeOwnerLanesLocked(const std::vector<PrimaryEndLifeOwnerLane *> &insertedLanes);

    std::optional<PrimaryEndLifeOwnerBatch> AcquirePrimaryOwnerBatch(PrimaryEndLifeOwnerBatchMap &batches);

    /**
     * @brief Process one master batch by deleting remote metadata before local object erase.
     * @param[in] masterAddr The owner master address for this batch.
     * @param[in] tasks The primary end-life tasks routed to the same master.
     */
    void ProcessPrimaryEndLifeMasterBatch(const PrimaryEndLifeOwnerBatch &batch);

    bool PreparePrimaryEndLifeMasterBatch(const PrimaryEndLifeOwnerBatch &batch,
                                          std::vector<PrimaryEndLifeCandidate> &candidates,
                                          std::vector<PrimaryEndLifeCandidate> &needDeleteMetaCandidates);

    int64_t GetPrimaryEndLifeRpcTimeout(const PrimaryEndLifeOwnerBatch &batch,
                                        const std::vector<PrimaryEndLifeCandidate> &candidates);

    Status DeletePrimaryEndLifeMetadataForBatch(const PrimaryEndLifeOwnerBatch &batch,
                                                const std::vector<PrimaryEndLifeCandidate> &candidates,
                                                std::unordered_set<std::string> &failedKeys,
                                                std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups);

    void HandlePrimaryEndLifeBatchFailure(const PrimaryEndLifeOwnerBatch &batch,
                                          std::vector<PrimaryEndLifeCandidate> &candidates, const Status &rc);

    void FinishPrimaryEndLifeMasterBatch(std::vector<PrimaryEndLifeCandidate> &candidates, Status &rc,
                                         std::unordered_set<std::string> &failedKeys,
                                         const std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups);

    /**
     * @brief Send one primary end-life metadata deletion attempt.
     * @param[in] masterAddr The metadata owner.
     * @param[in] needDeleteMetaCandidates Candidates that still need metadata deletion.
     * @param[in] allowRedirect Whether this source attempt may return a redirect.
     * @param[in] topologyVersion Placement version used for this attempt.
     * @param[in] timeoutMs Remaining logical attempt timeout.
     * @param[out] failedKeys Keys rejected by the Master.
     * @param[out] redirectGroups Redirected candidates grouped by target owner.
     * @return Master acceptance or the single RPC attempt status.
     */
    Status DeletePrimaryEndLifeMetadata(const HostPort &masterAddr,
                                        const std::vector<PrimaryEndLifeCandidate> &needDeleteMetaCandidates,
                                        bool allowRedirect, uint64_t topologyVersion, int64_t timeoutMs,
                                        std::unordered_set<std::string> &failedKeys,
                                        std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups);

    void HandlePrimaryEndLifeRpcFailure(const PrimaryEndLifeOwnerBatch &batch,
                                        std::vector<PrimaryEndLifeCandidate> &candidates, const Status &rc);

    void ClassifyPrimaryEndLifeRpcFailure(const PrimaryEndLifeOwnerBatch &batch, PrimaryEndLifeCandidate &candidate,
                                          const Status &rc, std::vector<PrimaryEndLifeTask> &delayedTasks,
                                          std::vector<PrimaryEndLifeTask> &readdTasks,
                                          std::vector<PrimaryEndLifeCandidate> &forceDeleteCandidates);

    static void ResetPrimaryEndLifeRetryState(PrimaryEndLifeTask &task);

    void ForceDeletePrimaryEndLifeCandidates(const PrimaryEndLifeOwnerBatch &batch,
                                              std::vector<PrimaryEndLifeCandidate> &candidates, const Status &rc);

    void SchedulePrimaryEndLifeRedirects(const std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups,
                                         std::unordered_set<std::string> &redirectKeys);

    /**
     * @brief Get a snapshot of primary end-life queue and drain pressure.
     * @return A single-line pressure description.
     */
    std::string GetPrimaryEndLifePressure();

    /**
     * @brief Log primary end-life stage latency and queue pressure.
     * @param[in] stage The stage name.
     * @param[in] elapsedMs The stage elapsed time in milliseconds.
     * @param[in] batchKeys The number of keys in the stage.
     * @param[in] queueWaitMs The oldest task queue wait in milliseconds.
     * @param[in] event The stage event, either start or complete.
     */
    void LogPrimaryEndLifeStage(const char *stage, double elapsedMs, size_t batchKeys, uint64_t queueWaitMs = 0,
                                const char *event = "complete");

    /**
     * @brief Log one primary end-life metadata RPC attempt.
     * @param[in] masterAddr The metadata owner.
     * @param[in] attempt The current attempt number.
     * @param[in] attemptElapsedMs The current attempt elapsed time.
     * @param[in] totalElapsedMs The cumulative RPC elapsed time.
     * @param[in] batchKeys The number of keys in the RPC.
     * @param[in] failedKeys The number of keys rejected by the Master.
     * @param[in] rc The RPC or response status.
     */
    void LogPrimaryEndLifeRpcAttempt(const HostPort &masterAddr, uint64_t topologyVersion, bool redirectAttempt,
                                     bool deferred, uint32_t attempt, double attemptElapsedMs, double totalElapsedMs,
                                     size_t batchKeys, size_t failedKeys, const Status &rc);

    /**
     * @brief Revalidate tasks, acquire object write locks, and select candidates within the release budget.
     * @param[in] tasks The tasks routed to one master.
     * @param[out] candidates Locked candidates that may proceed to DeleteAllCopyMeta.
     * @param[out] skippedTasks Tasks that should be retried by the eviction list later.
     * @return Status of candidate preparation.
     */
    Status PreparePrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeTask> &tasks,
                                           std::vector<PrimaryEndLifeCandidate> &candidates,
                                           std::vector<PrimaryEndLifeTask> &skippedTasks);

    /**
     * @brief Get the object entry and acquire its write lock for a primary end-life attempt.
     * @param[in] task The primary end-life task being prepared.
     * @param[out] entry The locked object entry on success.
     * @return Status of lookup and lock acquisition.
     */
    Status TryLockPrimaryEndLifeTask(const PrimaryEndLifeTask &task, std::shared_ptr<SafeObjType> &entry);

    /**
     * @brief Check whether a locked object is still eligible for primary end-life deletion.
     * @param[in] task The primary end-life task carrying the expected version and cache type.
     * @param[in] entry The locked object entry to validate.
     * @return True if the object can still be deleted by the primary end-life lane.
     */
    bool IsPrimaryEndLifeTaskStillEvictable(const PrimaryEndLifeTask &task, const SafeObjType &entry);

    /**
     * @brief Compute how many bytes this lane may additionally release before reaching the low watermark.
     * @param[in] cacheType The cache type being evicted.
     * @param[in] needSize The foreground memory demand that triggered this eviction round.
     * @return The remaining release budget in bytes.
     */
    uint64_t GetPrimaryEndLifeReleaseBudget(CacheType cacheType, uint64_t needSize);

    /**
     * @brief Delete all-copy metadata for validated primary end-life candidates on one master.
     * @param[in] masterAddr The owner master address for this batch.
     * @param[in] candidates Candidates whose object locks were released before this RPC path.
     * @param[out] failedKeys Keys that must not be locally erased and should be readded.
     * @return Status of the metadata delete operation.
     */
    Status DeleteAllCopyMetaForPrimaryEndLife(const HostPort &masterAddr,
                                              const std::vector<PrimaryEndLifeCandidate> &candidates,
                                              bool allowRedirect, int64_t timeoutMs,
                                              std::unordered_set<std::string> &failedKeys,
                                              std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups);

    /**
     * @brief Send one DeleteAllCopyMeta request to a metadata owner.
     * @param[in] masterAddr The metadata owner address.
     * @param[in] req The prepared delete request.
     * @param[out] rsp The delete response.
     * @return Status of API initialization or the RPC.
     */
    Status DeleteAllCopyMetaOnce(const HostPort &masterAddr, master::DeleteAllCopyMetaReqPb &req,
                                 master::DeleteAllCopyMetaRspPb &rsp);

    /**
     * @brief Build the versioned DeleteAllCopyMeta request used by the primary end-life lane.
     * @param[in] candidates Candidates sent to one metadata owner.
     * @param[in] allowRedirect Whether this owner may redirect the request.
     * @return The prepared request.
     */
    master::DeleteAllCopyMetaReqPb BuildPrimaryEndLifeDeleteReq(
        const std::vector<PrimaryEndLifeCandidate> &candidates, bool allowRedirect) const;

    /**
     * @brief Classify the source-owner response and group valid redirects by target owner.
     * @param[in] sourceMaster The initially contacted metadata owner.
     * @param[in] candidates Candidates in the source request.
     * @param[in] rsp The source-owner response.
     * @param[out] failedKeys Keys that must remain local.
     * @param[out] redirectGroups Redirected candidates grouped by target metadata owner.
     * @return Status for an unclassified whole-batch source failure.
     */
    static Status CollectPrimaryEndLifeSourceResult(
        const HostPort &sourceMaster, const std::vector<PrimaryEndLifeCandidate> &candidates,
        const master::DeleteAllCopyMetaRspPb &rsp, std::unordered_set<std::string> &failedKeys,
        std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups);

    /**
     * @brief Classify explicit failures and terminal keys in a source-owner response.
     * @param[in] candidates Candidates in the source request.
     * @param[in] rsp The source-owner response.
     * @param[out] failedKeys Explicitly failed candidate keys.
     * @param[out] classification Accumulated source response classification.
     */
    static void CollectPrimaryEndLifeSourceKeyResults(
        const std::vector<PrimaryEndLifeCandidate> &candidates, const master::DeleteAllCopyMetaRspPb &rsp,
        std::unordered_set<std::string> &failedKeys, PrimaryEndLifeSourceClassification &classification);

    /**
     * @brief Validate and classify redirects returned by the source metadata owner.
     * @param[in] sourceMaster The initially contacted metadata owner.
     * @param[in] rsp The source-owner response.
     * @param[out] failedKeys Redirect keys that cannot be followed safely.
     * @param[out] classification Accumulated source response classification.
     */
    static void CollectPrimaryEndLifeRedirectResults(
        const HostPort &sourceMaster, const master::DeleteAllCopyMetaRspPb &rsp,
        std::unordered_set<std::string> &failedKeys, PrimaryEndLifeSourceClassification &classification);

    /**
     * @brief Group valid source redirects by target metadata owner.
     * @param[in] candidates Candidates in the source request.
     * @param[in] classification Classified source-owner response.
     * @param[out] redirectGroups Redirected candidates grouped by target metadata owner.
     */
    static void BuildPrimaryEndLifeRedirectGroups(
        const std::vector<PrimaryEndLifeCandidate> &candidates,
        const PrimaryEndLifeSourceClassification &classification,
        std::vector<PrimaryEndLifeRedirectGroup> &redirectGroups);

    /**
     * @brief Forward valid primary end-life redirects once with redirect disabled.
     * @param[in] sourceMaster The initially contacted metadata owner.
     * @param[in] redirectGroups Redirected candidates grouped by target owner.
     * @param[out] failedKeys Keys rejected by, or not safely classified at, the target owner.
     */
    /**
     * @brief Collect primary end-life failures while treating no-meta and outdated versions as completed.
     * @param[in] rsp The DeleteAllCopyMeta response.
     * @param[out] failedKeys Keys that should not proceed to local erase.
     * @return Batch error only when it cannot be attributed to specific failed keys.
     */
    static Status CollectPrimaryEndLifeDeleteResult(const master::DeleteAllCopyMetaRspPb &rsp,
                                                    std::unordered_set<std::string> &failedKeys);

    /**
     * @brief Collect failed keys from a DeleteAllCopyMeta response, including redirect and moving metadata cases.
     * @param[in] rsp The DeleteAllCopyMeta response.
     * @param[out] failedKeys Keys that should not proceed to local erase.
     * @return Status derived from the response.
     */
    static Status CollectDeleteAllCopyMetaResult(const master::DeleteAllCopyMetaRspPb &rsp,
                                                 std::unordered_set<std::string> &failedKeys);

    /**
     * @brief Cancel the write-back async send task only after local erase succeeded.
     * @param[in] objectKey The object key whose async send task should be canceled.
     */
    void RemovePrimaryEndLifeAsyncSend(const std::string &objectKey);

    /**
     * @brief Erase the locked primary end-life candidate from local disk state and object table.
     * @param[in] candidate The candidate to erase locally.
     * @return Status of local erase.
     */
    Status DeletePrimaryEndLifeLocal(const PrimaryEndLifeCandidate &candidate);

    /**
     * @brief Re-acquire W-lock after the master RPC window, re-validate version, and refresh
     *        candidate.entry for the local Erase in Phase 2.
     * @param[in,out] candidate The candidate to re-lock; entry is refreshed from the table.
     * @return K_OK on success; K_NOT_FOUND if version changed or object erased; K_TRY_AGAIN if
     *         TryWLock retries exhausted.
     */
    Status ReacquireAndValidateForLocalDelete(PrimaryEndLifeCandidate &candidate);

    /**
     * @brief Phase 2 per-candidate: re-acquire WLock and perform local Erase.
     */
    void ProcessPrimaryEndLifeLocalErase(std::vector<PrimaryEndLifeCandidate> &candidates, Status &rc,
                                         std::unordered_set<std::string> &failedKeys);

    /**
     * @brief Get the memory release size for a locked primary end-life candidate.
     * @param[in] entry The object entry to inspect.
     * @return Data plus metadata size in bytes, saturated on overflow.
     */
    static uint64_t GetPrimaryEndLifeReleaseSize(const SafeObjType &entry);

    /**
     * @brief Unlock all object entries held by primary end-life candidates.
     * @param[in] candidates The candidates whose entries are write locked.
     */
    static void UnlockPrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeCandidate> &candidates);

    /**
     * @brief Submit spill task to thread pool.
     * @param[in] objectKey The object key.
     * @param[in] version The object version.
     * @return The future of the async thread.
     */
    std::future<SpillResult> SubmitSpillTask(const std::string &objectKey, uint64_t version);

    /**
     * @brief Spill object to disk.
     * @param[in] objectKey The object key.
     * @param[in] version The object version.
     * @return Status
     */
    Status SpillImpl(const std::string &objectKey, uint64_t version);

    /**
     * @brief Release finished spill task.
     * @param[in/out] spillTasks The spill task list.
     * @param[out] evictFailedIds Object keys that spill failed.
     * @param[in] last Whether to wait for all spill threads to complete.
     * @return The spilled size.
     */
    size_t ReleaseSpillFutures(std::unordered_map<std::string, SpillTask> &spillTasks,
                               EvictionRetryList &evictFailedIds, bool last);

    /**
     * @brief Submit async evict task to evict spilled objects when spill happen.
     * @param[in] objectSize Object size.
     */
    void TryEvictSpilledObjects(uint64_t objectSize);

    /**
     * @brief Evict spilled obejcts.
     * @param[in] objectSize Object data size.
     */
    void EvictSpilledObjects(uint64_t objectSize);
    void FinishEvictSpilledObjects(EvictionList &spillEvictionList, const EvictFailedList &evictFailedIds,
                                   uint64_t objectSize, bool &forceCompact);

    /**
     * @brief Indicate the object is evitable or not.
     * @param[in] entry Safe object entry.
     * @return True if object is evictable.
     */
    bool IsSpilledObjectEvictable(const std::shared_ptr<SafeObjType> &entry);

    /**
     * @brief Delete write back/through object if evictable.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @return Status of the call.
     */
    Status DeleteNoneL2CacheEvictableObject(const ObjectKV &objectKV);

    /**
     * @brief Delete none l2 cache evictable object if evictable.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @return Status of the call.
     */
    Status DeleteL2CacheEvictableObject(const ObjectKV &objectKV);

    /**
     * @brief Get a object from ObjectTable and lock it.
     * @param[in] objectKey The ID of the object that need to get.
     * @param[out] entry The object entry that need to get.
     * @param[out] retrySnapshot Current eviction metadata when the entry cannot be locked temporarily.
     * @return Status of the call.
     */
    Status GetAndLockEntry(const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
                           std::optional<EvictionList::Node> &retrySnapshot);
    Status ExtractEvictionNode(const std::string &objectKey, EvictionList::Node &snapshot);

    /**
     * @brief Get a object from ObjectTable and lock it.
     * @param[in] objectKey The ID of the object that need to get.
     * @param[in] version The object version.
     * @param[in] isWrite Specifies to add a read lock or write lock to the object.
     * @param[out] entryPtr The object entry that need to get.
     * @return Status of the call.
     */
    Status GetAndLockEntry(const std::string &objectKey, uint64_t version, bool isWrite,
                           std::shared_ptr<SafeObjType> &entryPtr);

    /**
     * @brief Check whether an object can do evict.
     * @param[in] objectKV The object entry that need to check and its corresponding objectKey.
     * @return true if object can do evict.
     */
    bool IsObjectEvictable(const ObjectKV &objectKV);

    /**
     * @brief Check whether an object is exists in L2 cache.
     * @param[in] entry The object entry that need to check.
     * @return true if object exists in L2 cache.
     */
    static bool IsObjectExistInL2Cache(const SafeObjType &entry);

    /**
     * @brief Calculates the low water mark for shared memory dynamically.
     * @param[in] cacheType The type of cache.
     * @return the low water mark based on the available shared memory.
     */
    uint64_t GetLowWaterMark(CacheType cacheType = CacheType::MEMORY);

    EvictionList::HeatNodeMetadata ResolveHeatNodeMetadata(const std::string &objectKey) const;
    Status CollectCopyWatermarkStats(CopyWatermarkStats &stats, bool collectCounterDistribution = true);
    void NotifyCopyWatermarkObserver();
    Status EnsureTargetMembership(const std::string &objectKey);
    Status EnsureMigratedHeatTarget(const std::string &objectKey);
    Status PreparePolicyUpdate(EvictionPolicy targetPolicy, uint64_t epoch, bool &phaseAcquired);
    Status DrainPolicyUpdateActivity();
    Status InitializePolicyUpdateTarget(EvictionPolicy targetPolicy, uint64_t epoch);
    void ResetPolicyUpdatePhase();

    std::shared_ptr<ObjectTable> objectTable_;
    // Declared before policyRoute_ so ClockEvictionStrategy's reference to this slot remains valid through teardown.
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> gRefTable_{ nullptr };
    EvictionList memEvictionList_;
    EvictionList alternateEvictionList_;
    mutable std::shared_mutex policyRouteMutex_;
    PolicyRoute policyRoute_;
    std::array<PolicyMigrationLock, POLICY_MIGRATION_LOCK_COUNT> policyMigrationLocks_;
    std::atomic<PolicyUpdatePhase> policyUpdatePhase_{ PolicyUpdatePhase::STABLE };
    // Avoid an allocator metadata lookup on every CLOCK hit. This is published before a Clock->Heat transition can
    // route foreground mutations to the Heat target and remains true until Heat is no longer reachable.
    std::atomic<bool> needsMigratableSize_{ false };
    std::atomic<uint64_t> policyUpdateTotalObjects_{ 0 };
    std::atomic<uint64_t> policyUpdateRemainingObjects_{ 0 };
    std::atomic<uint64_t> policyMutationGeneration_{ 0 };
    std::atomic<uint64_t> policyMutationWriters_{ 0 };
    PolicyStateLoader policyStateLoader_;
    PolicyStateStorer policyStateStorer_;
    bool recoveredTransitionIntent_{ false };
    EvictionPolicy recoveredTargetPolicy_{ EvictionPolicy::CLOCK };
    uint64_t recoveredTransitionEpoch_{ 0 };
    std::atomic<bool> evictionCancelRequested_{ false };
    std::condition_variable evictionStoppedCv_;
    // STABLE cache operations are read-mostly but extremely frequent. Shard reader ownership by execution thread so
    // cache hits do not bounce one global counter cache line. Admission and policy close use seq_cst operations to
    // order the phase transition against reader registration; DRAINING waits until every slot reaches zero.
    std::array<StableRouteReaderSlot, STABLE_ROUTE_READER_SLOT_COUNT> stableRouteReaderSlots_;
    std::condition_variable stableRouteReadersCv_;
    // Rebalancing membership is sparse and has no cross-key invariant. A concurrent key map avoids serializing
    // eviction and migration workers that operate on unrelated objects.
    using RebalancingObjectTable = tbb::concurrent_hash_map<ImmutableString, bool>;
    mutable RebalancingObjectTable rebalancingObjects_;
    // Observer registration is rare and notification is read-mostly. Publish immutable callbacks atomically so
    // eviction and post-migration reporting do not contend on callback-registration mutexes.
    std::shared_ptr<const CopyWatermarkObserver> copyWatermarkObserver_;
    std::shared_ptr<const CopyWatermarkObserver> hotPrimaryReportObserver_;
    std::unique_ptr<ThreadPool> memEvictTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> primaryEndLifeThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> spillEvictTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> masterTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> spillTaskThreadPool_{ nullptr };
    std::mutex cvMutex_;  // To protect the eviction task
    HostPort localAddress_;
    HostPort masterAddress_;
    std::atomic<bool> isDone_;
    master::MasterOCServiceImpl *masterOc_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    const worker::MetadataRouteResolver &metadataRoute_;
    std::atomic<bool> scheduleEvictionRunning_{ false };
    std::unique_ptr<ThreadPool> scheduleEvictThreadPool_{ nullptr };
    std::weak_ptr<AsyncSendManager> asyncSendManager_{};
    std::mutex primaryEndLifeMutex_;
    std::condition_variable primaryEndLifeDrainedCv_;
    std::unordered_map<std::string, uint64_t> pendingPrimaryEndLifeObjects_;
    std::atomic<uint64_t> primaryEndLifePendingFullCount_{ 0 };
    // Tracks metadata-deleted objects whose local cleanup failed and must be retried locally.
    std::unordered_map<std::string, uint64_t> metaDeletedPrimaryEndLifeObjects_;
    std::list<PrimaryEndLifeTask> primaryEndLifeReadyQueue_;
    std::priority_queue<DelayedPrimaryEndLifeTask, std::vector<DelayedPrimaryEndLifeTask>,
                        DelayedPrimaryEndLifeTaskCompare>
        delayedPrimaryEndLifeQueue_;
    std::unordered_map<HostPort, PrimaryEndLifeOwnerLane> primaryEndLifeOwnerLanes_;
    std::condition_variable primaryEndLifeCv_;
    bool primaryEndLifeStopping_{ true };
    uint64_t primaryEndLifeDeferredSequence_{ 0 };
    int activeDrainWorkers_{ 0 };
    // Keep the publisher alive until eviction background tasks have drained in this manager's destructor.
    std::shared_ptr<KvEventPublisher> kvEventPublisher_{ nullptr };
    friend class ::datasystem::ut::EvictionManagerTest;
    friend class ::datasystem::ut::SpillEvictionTest;
};

/**
 * @brief Try to evict when memory size reach high water maker.
 * @param[in] keyInfo The ID of the object need to allocate.
 * @param[in] needSize The size need to allocate.
 * @param[in] evictionManager The class of eviction process.
 * @param[in] type The service type.
 * @param[in] cacheType The type of cache.
 * @return True if eviction is triggered.
 */
bool EvictWhenMemoryExceedThrehold(const std::string &keyInfo, uint64_t needSize,
                                   const std::shared_ptr<WorkerOcEvictionManager> &evictionManager,
                                   ServiceType type = ServiceType::OBJECT, CacheType cacheType = CacheType::MEMORY);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OC_EVICTION_MANAGER_H
