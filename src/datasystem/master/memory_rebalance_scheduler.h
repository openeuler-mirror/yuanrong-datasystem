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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

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
    };

    struct CandidatePair {
        const NodeInfo *source = nullptr;
        const NodeInfo *target = nullptr;
        uint64_t maxBytes = 0;
        uint64_t targetAvailableAfterInFlight = 0;
        uint32_t usageGapRate = 0;
        uint32_t projectedTargetUsageRate = 0;
    };

    static bool IsTerminalStatus(master::RebalanceTaskStatusPb status);
    static bool IsFailedStatus(master::RebalanceTaskStatusPb status);
    static uint64_t CalculateUsageRate(uint64_t usedMemory, uint64_t memoryLimit);
    static uint64_t CalculateUsageRate(const NodeInfo &node);

    void ExpireTimeoutTasksLocked(uint64_t nowMs, const std::string &activeWorker);
    void GcHeldInflightLocked(uint64_t nowMs);
    bool IsInCooldownLocked(const std::string &worker, uint64_t nowMs) const;
    void AddCooldownLocked(const std::string &worker, uint64_t nowMs);
    void RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success);
    void MarkTaskDispatchedLocked(RunningTask &runningTask);
    uint64_t GetTargetInflightBytesLocked(const std::string &targetWorker) const;
    // Release the reporting worker's held in-flight: its own report timestamp proves its
    // snapshot now reflects post-receive memory, so the held charge can drop (issue #685).
    void ReleaseReporterHoldsLocked(const std::string &worker, uint64_t reportTimestamp);
    // Release held in-flight for every target whose snapshot timestamp advanced since its
    // latest completion (swap-lagged backup for non-reporting targets).
    void ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot);
    bool IsSourceCandidateLocked(const NodeInfo &node, uint64_t nowMs) const;
    void CollectWorkerCandidatesLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                       const std::string &sourceWorker, uint64_t nowMs,
                                       std::vector<const NodeInfo *> &sources,
                                       std::vector<const NodeInfo *> &targets) const;
    void CollectCandidatePairsLocked(const std::vector<const NodeInfo *> &sources,
                                     const std::vector<const NodeInfo *> &targets,
                                     std::vector<CandidatePair> &targetPairs) const;
    void FillTaskFromPairLocked(const CandidatePair &bestPair, uint64_t nowMs, master::RebalanceTaskPb &task) const;
    Status TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                              const std::string &sourceWorker, uint64_t nowMs, master::RebalanceTaskPb &task);
    uint64_t CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                      uint64_t targetInflightBytes) const;
    uint64_t CalculateProjectedTargetUsageRate(const NodeInfo &target, uint64_t targetInflightBytes,
                                               uint64_t maxBytes) const;

    std::mutex mutex_;
    std::unordered_map<std::string, RunningTask> activeTasksBySource_;
    std::unordered_map<std::string, uint64_t> targetInflightBytes_;
    std::unordered_map<std::string, uint64_t> cooldownUntilMs_;
    // issue #685: in-flight bytes held past completion until the target reports its real
    // post-receive memory. pendingReleaseBytes_[T] = bytes still charged & waiting to drop;
    // holdSinceMs_[T] = latest completion time of a held charge on T (steady-clock ms).
    std::unordered_map<std::string, uint64_t> pendingReleaseBytes_;
    std::unordered_map<std::string, uint64_t> holdSinceMs_;

#ifdef WITH_TESTS
    friend class ::datasystem::ut::MemoryRebalanceSchedulerTest;
#endif
};
}  // namespace master
}  // namespace datasystem
#endif
