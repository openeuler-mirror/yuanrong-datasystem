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
 * Description: Master-side scheduler for the heat-driven rebalance strategy.
 *
 * Trigger: a worker is a source when it meets EITHER path:
 *   Path A: usage > source_usage_percent AND at least one primary copy remains (memory-pressure fallback)
 *   Path B: usage > source_usage_percent_low AND hot primary copy bytes / capacity > source_hot_ratio_percent
 * Target eligible when usage < target_usage_percent AND hot primary copy bytes / capacity <
 * target_hot_ratio_percent. Migration bytes are a fixed 10% of the source's memory capacity, capped by
 * the target's remaining available space. Cooldown is added only on failure/timeout, not on success,
 * so a successful source may be re-selected next cycle if it still meets the trigger conditions.
 * RebalanceScheduler owns the lifecycle state shared with MemoryRebalanceScheduler; this class owns only Heat task
 * completion policy and source/target selection.
 */
#ifndef DATASYSTEM_MASTER_HEAT_REBALANCE_SCHEDULER_H
#define DATASYSTEM_MASTER_HEAT_REBALANCE_SCHEDULER_H

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <bthread/mutex.h>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/rebalance_scheduler.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace master {
class HeatRebalanceScheduler : public RebalanceScheduler {
public:
    ~HeatRebalanceScheduler() = default;

    void SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership) override;

    Status Schedule(const master::ResourceReportReqPb &req, const std::unordered_map<std::string, NodeInfo> &snapshot,
                    master::ResourceReportRspPb &rsp) override;

    bool NeedSnapshotForSchedule(const master::ResourceReportReqPb &req, const NodeInfo &reportingNode,
                                 master::ResourceReportRspPb &rsp) override;

    Status ReportResult(const master::ReportRebalanceResultReqPb &req,
                        master::ReportRebalanceResultRspPb &rsp) override;

#ifdef WITH_TESTS
    void SetActiveTaskDeadlineForTest(const std::string &sourceWorker, uint64_t deadlineMs);
#endif

private:
    struct CandidatePair {
        const NodeInfo *source = nullptr;
        const NodeInfo *target = nullptr;
        uint64_t maxBytes = 0;
        uint64_t targetAvailAfterInflight = 0;  // sort key 2 (descending): space after deducting in-flight
        uint32_t usageRate = 0;                  // sort key 1 (ascending): target usage percent
    };

    enum class SchedulerEventType : uint8_t { EXPIRE, HOLD, RELEASE_REPORTER, RELEASE_SNAPSHOT, RELEASE_TTL };

    struct SchedulerEvent {
        explicit SchedulerEvent(SchedulerEventType eventType) : type(eventType)
        {
        }

        SchedulerEventType type;
        std::string taskId;
        std::string target;
        uint64_t bytes = 0;
        uint64_t pendingBytes = 0;
        uint64_t minimumObservedUsedMemory = 0;
        uint64_t ttlSeconds = 0;
    };

    static uint64_t CalculateUsageRate(const NodeInfo &node);

    std::shared_ptr<const cluster::TopologySnapshot> GetTopologySnapshot(bool &topologyConfigured);

    static void EmitSchedulerEvents(const std::vector<SchedulerEvent> &events);
    void ExpireTimeoutTasksLocked(uint64_t nowMs, std::vector<SchedulerEvent> &events);
    void GcHeldInflightLocked(uint64_t nowMs, std::vector<SchedulerEvent> &events);
    void AddCooldownLocked(const std::string &worker, uint64_t nowMs);
    void RemoveTaskLocked(const std::string &sourceWorker, uint64_t nowMs, bool success,
                          std::vector<SchedulerEvent> &events, uint64_t migratedBytes = 0);
    void ReleaseReporterHoldsLocked(const NodeInfo &reportingNode, std::vector<SchedulerEvent> &events);
    void ReleaseSnapshotHoldsLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                                    std::vector<SchedulerEvent> &events);
    bool IsSourceCandidateLocked(const NodeInfo &node, uint64_t nowMs,
                                 const cluster::TopologySnapshot *topologySnapshot) const;
    bool IsTargetCandidateLocked(const NodeInfo &node, uint64_t nowMs,
                                 const cluster::TopologySnapshot *topologySnapshot) const;
    uint64_t CalculateTaskBytesLocked(const NodeInfo &source, const NodeInfo &target,
                                      uint64_t targetInflightBytes) const;
    void FillTaskFromPairLocked(const CandidatePair &bestPair, const std::string &taskId, uint64_t nowMs,
                                master::RebalanceTaskPb &task) const;
    Status TryBuildTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                              const std::string &sourceWorker, const std::string &taskId, uint64_t nowMs,
                              const cluster::TopologySnapshot *topologySnapshot, master::RebalanceTaskPb &task);
    Status ActivateTaskLocked(const std::unordered_map<std::string, NodeInfo> &snapshot,
                              const std::string &reportingWorker, const master::RebalanceTaskPb &task,
                              master::ResourceReportRspPb &rsp, std::vector<SchedulerEvent> &events,
                              std::unique_lock<bthread::Mutex> &lock);

    bthread::Mutex mutex_;
    const cluster::MembershipEndpointView *topologyMembership_{ nullptr };
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_HEAT_REBALANCE_SCHEDULER_H
