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
#include <unordered_set>
#include <vector>

#include <bthread/mutex.h>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/rebalance_scheduler.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
#ifdef WITH_TESTS
namespace ut {
class MemoryRebalanceSchedulerTest;
}
#endif

namespace master {
class MemoryRebalanceScheduler : public RebalanceScheduler {
public:
    ~MemoryRebalanceScheduler() = default;

    /**
     * @brief Bind the read-only topology membership view used to fence rebalance candidates.
     * @param[in] topologyMembership Non-owning view that outlives this scheduler.
     */
    void SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership) override;

    /**
     * @brief Try to assign one rebalance task to the reporting source worker.
     */
    Status Schedule(const master::ResourceReportReqPb &req, const std::unordered_map<std::string, NodeInfo> &snapshot,
                    master::ResourceReportRspPb &rsp) override;

    /**
     * @brief Fast path before the caller builds a full cluster snapshot.
     * @return True if the caller should build a snapshot and call Schedule to create a new task.
     */
    bool NeedSnapshotForSchedule(const master::ResourceReportReqPb &req, const NodeInfo &reportingNode,
                                 master::ResourceReportRspPb &rsp) override;

    /**
     * @brief Receive worker-side rebalance task result.
     */
    Status ReportResult(const master::ReportRebalanceResultReqPb &req,
                        master::ReportRebalanceResultRspPb &rsp) override;

private:
    struct CandidatePair {
        const NodeInfo *source = nullptr;
        const NodeInfo *target = nullptr;
        uint64_t maxBytes = 0;
        uint64_t targetAvailableAfterInFlight = 0;
        uint64_t effectiveTargetUsed = 0;
        uint32_t usageGapRate = 0;
        uint32_t projectedTargetUsageRate = 0;
    };

    static bool IsFailedStatus(master::RebalanceTaskStatusPb status);
    static uint64_t CalculateUsageRate(uint64_t usedMemory, uint64_t memoryLimit);
    static uint64_t CalculateUsageRate(const NodeInfo &node);

    void ExpireTimeoutTasksLocked(uint64_t nowMs);
    void ExpireCooldownsLocked(uint64_t nowMs);
    void GcHeldInflightLocked(uint64_t nowMs);
    bool IsPairInCooldownLocked(const std::string &source, const std::string &target, uint64_t nowMs) const;
    static uint64_t CalculateCooldownDeadlineMs(uint64_t nowMs);
    void AddCooldownLocked(const std::string &worker, uint64_t nowMs);
    void AddPairCooldownLocked(const std::string &source, const std::string &target, uint64_t nowMs);
    void ApplyFailureCooldownLocked(const master::RebalanceTaskPb &task,
                                    master::RebalanceFailureSidePb failureSide, uint64_t nowMs);
    void RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success,
                          master::RebalanceFailureSidePb failureSide = master::REBALANCE_FAILURE_UNKNOWN);
    // Release held in-flight for every target whose snapshot timestamp advanced since its
    // latest completion (merge-refreshed snapshot path).
    void ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot);
    // Per-batch feedback loop: on a successful report with a fresh target_remain_bytes, release the
    // held charge (fresh signal supersedes #685 hold), accumulate migrated bytes into the fixed
    // budget, and build the next 300MB batch task if the budget is not exhausted and the target is
    // still below the watermark. Caller must hold mutex_.
    void ProcessFreshFeedbackLocked(RunningTask &prevTask, const master::ReportRebalanceResultReqPb &req,
                                    uint64_t nowMs, bool allowSuccessor,
                                    master::ReportRebalanceResultRspPb &rsp);
    std::shared_ptr<const cluster::TopologySnapshot> GetTopologySnapshot();
    bool AllowRebalanceForTopologyLocked(const cluster::TopologySnapshot *snapshot,
                                         const std::string &reportingWorker, uint64_t nowMs);
    void StartTopologyStabilizationLocked(uint64_t version, uint64_t stableSinceMs);
    void MarkActiveTasksTopologyStaleLocked();
    static uint64_t GetTopologyCooldownMs();
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
                                     bool allowReplay, master::ReportRebalanceResultRspPb &rsp);
    uint64_t CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                       uint64_t targetInflightBytes, uint64_t heldBytes,
                                       uint64_t freshUsedMemory) const;
    uint64_t CalculateProjectedTargetUsageRate(const NodeInfo &target, uint64_t targetInflightBytes,
                                                uint64_t heldBytes, uint64_t freshUsedMemory,
                                                uint64_t maxBytes) const;

    bthread::Mutex mutex_;
    // Non-owning read-only topology view. WorkerOCServer destroys ResourceManager before TopologyEngine.
    const cluster::MembershipEndpointView *topologyMembership_{ nullptr };
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> pairCooldownUntilMs_;
    bool topologyObserved_{ false };
    bool topologyStabilizationPending_{ false };
    uint64_t observedTopologyVersion_{ 0 };
    uint64_t topologyStableSinceMs_{ 0 };
    std::unordered_set<std::string> freshTopologyReportWorkers_;

#ifdef WITH_TESTS
    friend class ::datasystem::ut::MemoryRebalanceSchedulerTest;
#endif
};
}  // namespace master
}  // namespace datasystem
#endif
