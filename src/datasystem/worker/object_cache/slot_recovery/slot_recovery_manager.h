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
 * Description: Slot recovery coordination manager.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_MANAGER_H

#include <atomic>
#include <deque>
#include <mutex>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/protos/slot_recovery.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"

namespace datasystem {
namespace object_cache {

class SlotRecoveryIncidentState {
public:
    ~SlotRecoveryIncidentState() = default;

    /**
     * @brief Check whether a recovery task has reached terminal state.
     * @param[in] task The task to inspect.
     * @return True if the task is COMPLETED or FAILED.
     */
    static bool IsTaskTerminal(const RecoveryTaskPb &task);

    /**
     * @brief Check whether an incident has no remaining unfinished slots.
     * @param[in] info The incident protobuf to inspect.
     * @return True if all slots are terminal.
     */
    static bool IsFullyTerminal(const SlotRecoveryInfoPb &info);

    /**
     * @brief Mark all unfinished tasks owned by failedWorker as FAILED.
     * @param[in] failedWorker The failed task owner.
     * @param[in,out] info The incident protobuf to update.
     * @return Status of the call.
     */
    static Status MarkTasksFailedByOwner(const std::string &failedWorker, SlotRecoveryInfoPb &info);

    /**
     * @brief Recompute incident counters from the task terminal states.
     * @param[in,out] info The incident protobuf to update.
     */
    static void RefreshCounters(SlotRecoveryInfoPb &info);
};

class SlotRecoveryPlanner {
public:
    ~SlotRecoveryPlanner() = default;

    /**
     * @brief Build the initial recovery tasks for a failed worker using a stable round-robin assignment.
     * @param[in] failedWorker The worker that has entered FAILED.
     * @param[in] totalSlots The total slot count to plan.
     * @param[in] activeWorkers Stable active workers used as task owners.
     * @param[in,out] info The incident protobuf to fill.
     * @return Status of the call.
     */
    static Status BuildInitialTasks(const std::string &failedWorker, uint32_t totalSlots,
                                    const std::vector<std::string> &activeWorkers, SlotRecoveryInfoPb &info);
    /**
     * @brief Collect unfinished raw tasks owned by a newly failed worker.
     * @param[in] failedWorker The newly failed task owner.
     * @param[in] sourceInfo The incident that currently contains tasks owned by failedWorker.
     * @param[out] tasks The raw inherited tasks. Worker field keeps the original owner.
     * @return Status of the call.
     */
    static Status CollectInheritedTasks(const std::string &failedWorker, const SlotRecoveryInfoPb &sourceInfo,
                                        std::vector<RecoveryTaskPb> &tasks);
    /**
     * @brief Reassign raw inherited tasks to active workers with global round-robin at task granularity.
     * @param[in] rawTasks Raw inherited tasks.
     * @param[in] activeWorkers Stable active workers that can inherit those tasks.
     * @param[out] tasks The inherited pending tasks after reassignment.
     * @return Status of the call.
     */
    static Status ReassignInheritedTasks(const std::vector<RecoveryTaskPb> &rawTasks,
                                         const std::vector<std::string> &activeWorkers,
                                         std::vector<RecoveryTaskPb> &tasks);
    /**
     * @brief Append recovery tasks into an incident.
     * @param[in] tasks Tasks to be appended.
     * @param[in,out] info The incident protobuf to update.
     * @return Status of the call.
     */
    static Status AppendRecoveryTasks(std::vector<RecoveryTaskPb> &&tasks, SlotRecoveryInfoPb &info);
};

class SlotRecoveryManager {
    struct DeferredMetaRetryTask {
        std::string incidentKey;
        std::string failedWorker;
        std::string ownerWorker;
        std::string sourceWorker;
        std::string slotsSummary;
        std::vector<ObjectMetaPb> pendingMetas;
        std::vector<std::string> failedIds;
        Status firstStatus = Status::OK();
        uint64_t enqueueTimeMs{ 0 };
        uint64_t attempt{ 0 };
    };

public:
    SlotRecoveryManager() = default;
    ~SlotRecoveryManager();

    /**
     * @brief Initialize the manager and subscribe to failed-worker notifications.
     * @param[in] localAddress Local worker address.
     * @param[in] etcdCM Cluster manager used to query stable active/failed workers.
     * @param[in] persistApi Persistence API reserved for later recovery execution.
     * @param[in] apiManager Worker-master API manager reserved for later recovery execution.
     * @param[in] etcdStore EtcdStore pointer used to construct default slot-recovery store.
     * @return Status of the call.
     */
    Status Init(const HostPort &localAddress, EtcdClusterManager *etcdCM, std::shared_ptr<PersistenceApi> persistApi,
                std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager,
                datasystem::EtcdStore *etcdStore, MetaDataRecoveryManager *metadataRecoveryManager = nullptr);

    /**
     * @brief Shutdown the manager.
     */
    void Shutdown();

    /**
     * @brief Handle a batch of failed workers produced in one demotion round.
     * @param[in] failedWorkers Workers that became FAILED in the same batch.
     * @return Status of the call.
     */
    Status HandleFailedWorkers(const std::vector<HostPort> &failedWorkers);

    /**
     * @brief Handle local in-place restart slot recovery takeover.
     * @return Status of the call.
     */
    Status HandleLocalRestart();

    /**
     * @brief Schedule local pending tasks from persisted slot-recovery incidents.
     * @return Status of the call.
     */
    Status ScheduleLocalPendingTasksFromStore();

protected:
    /**
     * @brief Plan the incident of one failed worker with its own tasks and inherited tasks in one CAS.
     *        If the incident already exists, the current worker reuses that published plan directly.
     * @param[in] failedWorker The worker key of the incident.
     * @param[in] activeWorkers Stable active workers computed once for the current failed-worker batch.
     * @param[in,out] incidents Reusable ETCD snapshot for the current batch.
     * @return Status of the call.
     */
    Status PlanIncident(const std::string &failedWorker, const std::vector<std::string> &activeWorkers,
                        std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents);

    /**
     * @brief Collect inherited tasks from older incidents for the newly failed worker.
     * @param[in] failedWorker The newly failed worker.
     * @param[in] activeWorkers Stable active workers computed once for the current failed-worker batch.
     * @param[in] incidents A reusable incident snapshot loaded from ETCD.
     * @param[out] inheritedTasks Pending tasks that should be appended into the new incident.
     * @return Status of the call.
     */
    Status CollectInheritedTasks(const std::string &failedWorker, const std::vector<std::string> &activeWorkers,
                                 const std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents,
                                 std::vector<RecoveryTaskPb> &inheritedTasks);

    /**
     * @brief Mark tasks owned by failedWorker as FAILED in all other incidents.
     * @param[in] failedWorker The failed task owner.
     * @param[in] incidents A reusable incident snapshot loaded from ETCD.
     * @return Status of the call.
     */
    Status MarkTasksFailedInOtherIncidents(const std::string &failedWorker,
                                           const std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents);

    /**
     * @brief Inspect the incident snapshot and schedule local tasks asynchronously.
     *        The local snapshot may be stale, so each task still needs a CAS claim before execution.
     * @param[in] incidentKey The ETCD key suffix of the incident.
     * @param[in] info The current incident snapshot.
     * @return Status of the call.
     */
    Status ScheduleLocalTasks(const std::string &incidentKey, const SlotRecoveryInfoPb &info);

    /**
     * @brief Claim a local task from PENDING to IN_PROGRESS with owner verification.
     * @param[in] incidentKey The ETCD key suffix of the incident.
     * @param[in] task The task identity captured from the snapshot.
     * @param[out] claimed True if the task can be executed locally.
     * @return Status of the call.
     */
    Status ClaimLocalTask(const std::string &incidentKey, const RecoveryTaskPb &task, bool &claimed);

    /**
     * @brief Complete a local task with owner verification after execution returns.
     * @param[in] incidentKey The ETCD key suffix of the incident.
     * @param[in] task The task identity captured from the snapshot.
     * @return Status of the call.
     */
    Status CompleteLocalTask(const std::string &incidentKey, const RecoveryTaskPb &task);

    /**
     * @brief Mark a local task FAILED with owner verification after execution returns an error.
     * @param[in] incidentKey The ETCD key suffix of the incident.
     * @param[in] task The task identity captured from the snapshot.
     * @return Status of the call.
     */
    Status FailLocalTask(const std::string &incidentKey, const RecoveryTaskPb &task);

    /**
     * @brief Return a stable active-worker list after filtering out known failed workers.
     * @return Stable active workers.
     */
    std::vector<std::string> GetStableActiveWorkers() const;

    /**
     * @brief Pick a deterministic subset of active workers to execute cross-incident ETCD updates.
     * @param[in] failedWorkers Newly failed workers in the same demotion batch.
     * @param[in] activeWorkers Stable active workers that can process the batch.
     * @param[in] maxWorkers Upper bound of selected process workers.
     * @return Deterministically selected process workers.
     */
    static std::vector<std::string> PickProcessWorkers(const std::vector<std::string> &failedWorkers,
                                                       const std::vector<std::string> &activeWorkers,
                                                       size_t maxWorkers);

    /**
     * @brief One-shot read model for planning local restart work.
     */
    struct LocalRestartPlan {
        bool localIncidentExists = false;
        std::unordered_map<std::string, std::set<uint32_t>> plannedSlotsBySource;
        std::vector<std::string> sourceIncidentKeys;
    };

    /**
     * @brief Collect the local restart plan in one ETCD scan.
     * @param[in] localWorker The local restarted worker.
     * @param[out] restartPlan Consolidated local/source restart state.
     * @return Status of the call.
     */
    Status CollectLocalRestartPlan(const std::string &localWorker, LocalRestartPlan &restartPlan);

    /**
     * @brief Mark takeover candidates in one source incident as FAILED and collect their slots.
     * @param[in] sourceIncidentKey Source incident key.
     * @param[in] localWorker The local restarted worker.
     * @param[out] takenSlotsBySource Slots successfully taken over from the source incident, grouped by preload
     *                                source worker.
     * @param[out] blockedSlots Slots that still belong to foreign IN_PROGRESS tasks after the CAS finishes.
     * @param[out] shouldDeleteSource True if the source incident becomes terminal after takeover.
     * @return Status of the call.
     */
    Status TakeOverPendingFromSourceIncident(const std::string &sourceIncidentKey, const std::string &localWorker,
                                             std::unordered_map<std::string, std::set<uint32_t>> &takenSlotsBySource,
                                             std::vector<uint32_t> &blockedSlots, bool &shouldDeleteSource);

    /**
     * @brief Compute the canonical local slot set after restart planning.
     *
     * When the local incident exists, restart only resumes/takes over slots that already belong to the local
     * recovery chain. When it does not exist, restart rebuilds a fresh local task covering all slots except slots
     * already protected by foreign IN_PROGRESS/COMPLETED work.
     *
     * @param[in] restartPlan Consolidated local/source restart state.
     * @param[in] takenSlotsBySource Slots taken over from source incidents during this restart.
     * @param[in] blockedSlots Slots blocked by foreign IN_PROGRESS/COMPLETED work after source takeover CAS finishes.
     * @param[out] plannedLocalSlots Canonical local slot set for rebuild.
     * @return Status of the call.
     */
    Status BuildPlannedLocalRestartTasks(const std::string &localWorker, const LocalRestartPlan &restartPlan,
                                         const std::unordered_map<std::string, std::set<uint32_t>> &takenSlotsBySource,
                                         const std::set<uint32_t> &blockedSlots,
                                         std::vector<RecoveryTaskPb> &plannedLocalTasks);

    /**
     * @brief Rewrite the local incident into the post-restart canonical shape.
     *
     * The rebuilt incident keeps:
     * 1. terminal tasks for failed_worker=localWorker, so finished history/counters are preserved;
     * 2. foreign IN_PROGRESS tasks for failed_worker=localWorker, because restart must not rewrite them;
     * 3. all tasks for other failed workers that happen to share the same incident key.
     *
     * All other tasks for failed_worker=localWorker are replaced by local PENDING tasks grouped by preload source.
     *
     * @param[in] localWorker The local restarted worker.
     * @param[in] plannedLocalTasks Canonical local tasks after restart planning.
     * @return Status of the call.
     */
    Status RebuildLocalRestartIncident(const std::string &localWorker,
                                       const std::vector<RecoveryTaskPb> &plannedLocalTasks);

    /**
     * @brief Load latest local incident and schedule local tasks.
     * @param[in] localWorker The local restarted worker.
     * @return Status of the call.
     */
    Status ScheduleLocalRestartTasks(const std::string &localWorker);

    /**
     * @brief On local restart, resume stale cross-incident local tasks that were left IN_PROGRESS by the previous
     *        process instance.
     *
     * These tasks belong to incidents keyed by other failed workers and may never be rescheduled if no new
     * failed-worker event arrives after restart. We downgrade such stale local IN_PROGRESS tasks back to PENDING and
     * immediately re-run local scheduling.
     *
     * @param[in] localWorker The local restarted worker.
     * @return Status of the call.
     */
    Status ResumeStaleCrossIncidentLocalTasksOnRestart(const std::string &localWorker);

    /**
     * @brief Execute the recovery task by preloading slot data locally and pushing recovered metadata to master.
     * @param[in] task The task being executed.
     * @return Status of the call.
     */
    Status ExecuteRecoveryTask(const RecoveryTaskPb &task);

    /**
     * @brief Enqueue async metadata retry when master access fails in the fast recovery path.
     * @param[in] incidentKey The incident key.
     * @param[in] task The recovery task context.
     * @param[in] recoveredMetas Recovered metadata candidates.
     * @param[in] failedIds Failed object ids from the first attempt.
     * @param[in] recoverRc Status returned by the first attempt.
     * @return OK if the failure is accepted for deferred retry and handled asynchronously.
     */
    Status EnqueueDeferredMetaRetry(const std::string &incidentKey, const RecoveryTaskPb &task,
                                    const std::vector<ObjectMetaPb> &recoveredMetas,
                                    const std::vector<std::string> &failedIds, const Status &recoverRc);

    /**
     * @brief Drain async metadata retry queue in single-threaded retry worker.
     */
    void DrainDeferredMetaRetryQueue();

    /**
     * @brief Retry one deferred metadata task in bounded retry window.
     * @param[in,out] retryTask Deferred task.
     * @return Status of the call.
     */
    Status RetryDeferredMetaTask(DeferredMetaRetryTask &retryTask);

    /**
     * @brief Detect whether metadata recovery failure is likely caused by master access or routing instability.
     * @param[in] recoverRc Recovery status.
     * @param[in] failedIds Failed ids from recovery.
     * @return True if failure should be retried asynchronously.
     */
    bool ShouldDeferMetaRetry(const Status &recoverRc, const std::vector<std::string> &failedIds) const;

    /**
     * @brief Create slot-recovery store instance.
     * @param[in] etcdStore EtcdStore pointer used by concrete store.
     * @return Slot-recovery store instance.
     */
    virtual std::shared_ptr<SlotRecoveryStore> CreateStore(datasystem::EtcdStore *etcdStore) const;

private:
    /**
     * @brief Check whether slot recovery coordination is enabled.
     * @return True if enabled.
     */
    bool IsFeatureEnabled() const;

    /**
     * @brief Build the slot preload callback used during recovery execution.
     *
     * The callback validates memory, rebuilds object metadata from preload records, writes local entries through
     * MetaDataRecoveryManager, and appends recovered metadata for a later master push.
     *
     * @param[in,out] recoveredMetas Output list; successful preloads append one ObjectMetaPb per object.
     * @return Callback suitable for PersistenceApi::PreloadSlot.
     */
    SlotPreloadCallback BuildRecoveryPreloadCallback(std::vector<ObjectMetaPb> &recoveredMetas);

    /**
     * @brief Preload every slot listed in the recovery task from the given source worker.
     *
     * Slot order is shuffled to spread load. Transient I/O errors are retried with a bounded timeout in the worker.
     *
     * @param[in] sourceWorker Worker address to read slot data from.
     * @param[in] task Recovery task whose slots are preloaded.
     * @param[in] callback Preload callback invoked for each loaded object.
     * @param[out] failedSlotIds Slot ids whose preload ultimately failed after retries.
     * @return First non-OK preload status if any slot failed, otherwise OK (remaining slots are still attempted).
     */
    Status PreloadRecoveryTaskSlots(const std::string &sourceWorker, const RecoveryTaskPb &task,
                                    const SlotPreloadCallback &callback, std::vector<uint32_t> &failedSlotIds);

    /**
     * @brief Push recovered metadata to master after preloads complete.
     *
     * On partial or retryable master failures, enqueues deferred metadata retry when eligible; otherwise fails fast
     * with the recovery status.
     *
     * @param[in] task Recovery task context (incident key and identity fields).
     * @param[in] recoveredMetas Metadata collected during preload; passed to RecoverMetadata / deferred retry.
     * @return Status of the call.
     */
    Status FinalizeRecoveryMetadataPush(const RecoveryTaskPb &task, std::vector<ObjectMetaPb> &recoveredMetas);

    HostPort localAddress_;
    EtcdClusterManager *etcdCM_;
    std::shared_ptr<PersistenceApi> persistenceApi_;
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> workerMasterApiManager_;
    MetaDataRecoveryManager *metadataRecoveryManager_{ nullptr };
    std::shared_ptr<SlotRecoveryStore> store_;
    std::shared_ptr<ThreadPool> recoveryTaskThreadPool_{ nullptr };
    std::shared_ptr<ThreadPool> deferredMetaRetryThreadPool_{ nullptr };
    mutable std::mutex deferredMetaRetryMutex_;
    std::deque<DeferredMetaRetryTask> deferredMetaRetryQueue_;
    bool deferredMetaRetryDraining_{ false };
    std::atomic<bool> shuttingDown_{ false };
};

}  // namespace object_cache
}  // namespace datasystem

#endif
