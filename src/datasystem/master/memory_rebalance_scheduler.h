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
 * Description: In-memory scheduler for object-cache memory rebalance tasks.
 */
#ifndef DATASYSTEM_MASTER_MEMORY_REBALANCE_SCHEDULER_H
#define DATASYSTEM_MASTER_MEMORY_REBALANCE_SCHEDULER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <bthread/mutex.h>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
#ifdef WITH_TESTS
namespace ut {
class MemoryRebalanceSchedulerTest;
}
#endif

namespace master {
class MemoryRebalanceScheduler {
public:
    ~MemoryRebalanceScheduler() = default;

    /**
     * @brief Bind the read-only topology membership view used to fence rebalance candidates.
     * @param[in] topologyMembership Non-owning view that outlives this scheduler.
     */
    void SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership);

    /**
     * @brief Try to assign one rebalance task to the reporting source worker.
     */
    Status Schedule(const master::ResourceReportReqPb &req, const std::unordered_map<std::string, NodeInfo> &snapshot,
                    master::ResourceReportRspPb &rsp);

    /**
     * @brief Fast path before the caller builds a full cluster snapshot.
     * @return True if the caller should build a snapshot and call Schedule to create a new task.
     */
    bool NeedSnapshotForSchedule(const master::ResourceReportReqPb &req, const NodeInfo &reportingNode,
                                 master::ResourceReportRspPb &rsp);

    /**
     * @brief Receive worker-side rebalance task result.
     */
    Status ReportResult(const master::ReportRebalanceResultReqPb &req, master::ReportRebalanceResultRspPb &rsp);

private:
    struct RunningTask {
        master::RebalanceTaskPb task;
        bool dispatched = false;
        // Stable target memory view captured at assignment. memoryCapacity == highWater line
        // (stable config value: usedMemory + availableMemory); memoryLimit is the hard limit.
        // ReportResult uses these with the fresh per-batch target_remain_bytes to derive the
        // target's current used memory (memoryCapacity - remain_bytes) without re-reading the
        // cluster snapshot, and to check the rebalance watermark ceiling.
        uint64_t targetMemoryLimit = 0;
        uint64_t targetMemoryCapacity = 0;
        // Total migration budget fixed at Schedule time (the 30s ResourceReport cycle): the
        // midpoint gap (usageGap/2) capped by the target's watermark headroom and eviction
        // headroom. Per-batch max_bytes (300MB) caps only each batch, not the total. The loop
        // migrates in batches until cumulativeMigrated reaches totalBudget OR a fresh target
        // safety stop fires. Fixed by construction: foreground source writes cannot grow it.
        uint64_t totalBudget = 0;
        uint64_t cumulativeMigrated = 0;
        // Steady-clock ms captured at Schedule time. BuildNextBatchTaskLocked caps the chain's
        // wall-clock duration to one ResourceReport cycle so the loop cannot starve the next
        // cycle's snapshot or foreground work. 0 only before the first Schedule fills it.
        uint64_t epochStartMs = 0;
        // task_id of the immediate predecessor that was erased and replaced by this successor.
        // When the worker retries the predecessor (response lost in flight), master sees a stale
        // task_id against this active entry; if it matches immediatePredecessorTaskId, master
        // replays this entry's task as the successor so the chain continues instead of breaking on
        // an empty OK. Empty for the first task in a chain (no predecessor to replay).
        std::string immediatePredecessorTaskId;
    };

    struct CandidatePair {
        const NodeInfo *source = nullptr;
        const NodeInfo *target = nullptr;
        uint64_t maxBytes = 0;
        uint64_t targetAvailableAfterInFlight = 0;
        uint64_t effectiveTargetUsed = 0;
        uint32_t usageGapRate = 0;
        uint32_t projectedTargetUsageRate = 0;
    };

    static bool IsTerminalStatus(master::RebalanceTaskStatusPb status);
    static bool IsFailedStatus(master::RebalanceTaskStatusPb status);
    static uint64_t CalculateUsageRate(uint64_t usedMemory, uint64_t memoryLimit);
    static uint64_t CalculateUsageRate(const NodeInfo &node);

    void ExpireTimeoutTasksLocked(uint64_t nowMs);
    void ExpireCooldownsLocked(uint64_t nowMs);
    void GcHeldInflightLocked(uint64_t nowMs);
    bool IsInCooldownLocked(const std::string &worker, uint64_t nowMs) const;
    bool IsPairInCooldownLocked(const std::string &source, const std::string &target, uint64_t nowMs) const;
    static uint64_t CalculateCooldownDeadlineMs(uint64_t nowMs);
    void AddCooldownLocked(const std::string &worker, uint64_t nowMs);
    void AddPairCooldownLocked(const std::string &source, const std::string &target, uint64_t nowMs);
    void ApplyFailureCooldownLocked(const master::RebalanceTaskPb &task,
                                    master::RebalanceFailureSidePb failureSide, uint64_t nowMs);
    void RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success,
                          master::RebalanceFailureSidePb failureSide = master::REBALANCE_FAILURE_UNKNOWN);
    void MarkTaskDispatchedLocked(RunningTask &runningTask);
    uint64_t GetTargetInflightBytesLocked(const std::string &targetWorker) const;
    // Release held in-flight for every target whose snapshot timestamp advanced since its
    // latest completion (merge-refreshed snapshot path).
    void ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot);
    // Decrement the target's total in-flight charge (active + held) and erase the FutureDelta
    // entry once the total reaches zero (heldBytes/holdSinceMs are 0 by then -- held is a subset
    // of the total). Mirrors the old DecreaseCounter(targetInflightBytes_, ...) erase-on-zero.
    void DecreaseInflightLocked(const std::string &targetWorker, uint64_t bytes);
    // Release a held charge: decrement inflight by heldBytes, then clear heldBytes/holdSinceMs
    // on the surviving entry (if inflight > 0 after decrement, i.e. an active task remains).
    // If inflight reaches zero, DecreaseInflightLocked erases the entry and the find returns end().
    // Encapsulates the decrease-then-clear pattern shared by GC, reporter-release, and
    // snapshot-release paths so callers cannot forget the clear.
    void ReleaseHeldLocked(const std::string &worker, uint64_t heldBytes);
    // Per-batch feedback loop: on a successful report with a fresh target_remain_bytes, release the
    // held charge (fresh signal supersedes #685 hold), accumulate migrated bytes into the fixed
    // budget, and build the next 300MB batch task if the budget is not exhausted and the target is
    // still below the watermark. Caller must hold mutex_.
    void ProcessFreshFeedbackLocked(RunningTask &prevTask, const master::ReportRebalanceResultReqPb &req,
                                    uint64_t nowMs, master::ReportRebalanceResultRspPb &rsp);
    std::shared_ptr<const cluster::TopologySnapshot> GetTopologySnapshot();
    bool IsWorkerActiveInTopology(const std::string &worker,
                                  const cluster::TopologySnapshot *topologySnapshot) const;
    bool IsSourceCandidateLocked(const NodeInfo &node, uint64_t nowMs,
                                 const cluster::TopologySnapshot *topologySnapshot) const;
    void CollectWorkerCandidatesLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                       const std::string &sourceWorker, uint64_t nowMs,
                                       const cluster::TopologySnapshot *topologySnapshot,
                                       std::vector<const NodeInfo *> &sources,
                                       std::vector<const NodeInfo *> &targets) const;
    void CollectCandidatePairsLocked(const std::vector<const NodeInfo *> &sources,
                                     const std::vector<const NodeInfo *> &targets,
                                     uint64_t nowMs, std::vector<CandidatePair> &targetPairs) const;
    void FillTaskProtoLocked(const std::string &sourceWorker, const std::string &targetWorker,
                             uint64_t maxBytes, uint64_t nowMs, master::RebalanceTaskPb &task) const;
    void FillTaskFromPairLocked(const CandidatePair &bestPair, uint64_t nowMs,
                                RunningTask &runningTask) const;
    Status TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                              const std::string &sourceWorker, uint64_t nowMs,
                              const cluster::TopologySnapshot *topologySnapshot, RunningTask &runningTask);
    // Build the next 300MB batch task for the same source->target pair from the fixed total
    // budget (set at Schedule) and the fresh per-batch target_remain_bytes. Returns true and
    // fills nextTask when another batch is needed (budget not exhausted and target below the
    // rebalance watermark / not full); returns false when the rebalance loop should stop.
    // Caller must hold mutex_ and have already recorded the previous task's result
    // (RemoveTaskLocked) and updated prevTask.cumulativeMigrated so futureView_ is current.
    bool BuildNextBatchTaskLocked(const RunningTask &prevTask, uint64_t targetRemainBytes,
                                  uint64_t nowMs, master::RebalanceTaskPb &nextTask) const;
    // Early stop checks (no target memory view, corrupt remain signal, epoch budget exhausted)
    // that don't depend on the batch-size computation. Caller must hold mutex_.
    bool ShouldStopChainEarlyLocked(const RunningTask &prevTask, uint64_t targetRemainBytes,
                                    uint64_t nowMs) const;
    // Idempotent replay: if the active entry's immediatePredecessorTaskId matches the retried
    // req.task_id(), replay the successor into rsp and return true. Otherwise log stale and
    // return false. Caller must hold mutex_.
    Status ReplayOrIgnoreStaleLocked(std::unordered_map<std::string, RunningTask>::iterator taskIt,
                                     const master::ReportRebalanceResultReqPb &req,
                                     master::ReportRebalanceResultRspPb &rsp);
    uint64_t CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                       uint64_t targetInflightBytes, uint64_t heldBytes,
                                       uint64_t freshUsedMemory) const;
    uint64_t CalculateProjectedTargetUsageRate(const NodeInfo &target, uint64_t targetInflightBytes,
                                                uint64_t heldBytes, uint64_t freshUsedMemory,
                                                uint64_t maxBytes) const;

    bthread::Mutex mutex_;
    // Non-owning read-only topology view. WorkerOCServer destroys ResourceManager before TopologyEngine.
    const cluster::MembershipEndpointView *topologyMembership_{ nullptr };
    std::unordered_map<std::string, RunningTask> activeTasksBySource_;
    std::unordered_map<std::string, uint64_t> cooldownUntilMs_;
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> pairCooldownUntilMs_;
    // Future table (FutureView): per-target in-flight migration deltas overlaid on the current
    // table (readSnapshot_) for projection decisions. Grouping the three legacy maps into one
    // struct keeps a single entry per target so active/held state cannot drift apart, and gives
    // a stable place to add future overlays (outbound delta, eviction reserve, ...).
    //   inflightBytes  = total charged (active in-progress + held-pending-release); used by
    //                    projection (CalculateProjectedTargetUsageRate).
    //   heldBytes      = subset of inflightBytes held past success until the target reports its
    //                    real post-receive memory (issue #685).
    //   holdSinceMs    = latest completion time of a held charge on this target (steady-clock ms).
    struct FutureDelta {
        uint64_t inflightBytes = 0;
        uint64_t heldBytes = 0;
        uint64_t holdSinceMs = 0;
        uint64_t freshUsedMemory = 0;
    };
    std::unordered_map<std::string, FutureDelta> futureView_;

#ifdef WITH_TESTS
    friend class ::datasystem::ut::MemoryRebalanceSchedulerTest;
#endif
};
}  // namespace master
}  // namespace datasystem
#endif
