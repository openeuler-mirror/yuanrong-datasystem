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
 * Description: Abstract interface for master-scheduled rebalance schedulers.
 *
 * ResourceManager owns one RebalanceScheduler (memory or heat) selected by the
 * rebalance_strategy flag at construction. The scheduler owns task-lifecycle state
 * (active tasks, in-flight bytes, cooldowns, expiry) and the source/target selection
 * logic; ResourceManager drives it from the ResourceReport RPC path.
 */
#ifndef DATASYSTEM_MASTER_REBALANCE_SCHEDULER_H
#define DATASYSTEM_MASTER_REBALANCE_SCHEDULER_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace master {
class RebalanceScheduler {
public:
    virtual ~RebalanceScheduler() = default;

    /**
     * @brief Bind the read-only topology membership view used by schedulers that fence candidates by liveness.
     * @param[in] topologyMembership Non-owning view that outlives this scheduler.
     */
    virtual void SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership)
    {
        (void)topologyMembership;
    }

    /**
     * @brief Fast path before the caller builds a full cluster snapshot.
     * @return True if the caller should build a snapshot and call Schedule to create a new task.
     */
    virtual bool NeedSnapshotForSchedule(const master::ResourceReportReqPb &req, const NodeInfo &reportingNode,
                                         master::ResourceReportRspPb &rsp) = 0;

    /**
     * @brief Try to assign one rebalance task to the reporting source worker.
     */
    virtual Status Schedule(const master::ResourceReportReqPb &req,
                            const std::unordered_map<std::string, NodeInfo> &snapshot,
                            master::ResourceReportRspPb &rsp) = 0;

    /**
     * @brief Receive worker-side rebalance task result.
     */
    virtual Status ReportResult(const master::ReportRebalanceResultReqPb &req,
                                master::ReportRebalanceResultRspPb &rsp) = 0;

protected:
    /**
     * Shared task envelope. Memory rebalance uses the chain fields; Heat uses targetUsedBeforeDispatch for its
     * post-transfer visibility floor. Keeping lifecycle identity and accounting in one type prevents the schedulers
     * from growing independent active-task implementations again.
     */
    struct RunningTask {
        master::RebalanceTaskPb task;
        uint64_t targetUsedBeforeDispatch{ 0 };
        uint64_t targetMemoryLimit{ 0 };
        uint64_t targetMemoryCapacity{ 0 };
        uint64_t totalBudget{ 0 };
        uint64_t cumulativeMigrated{ 0 };
        uint64_t epochStartMs{ 0 };
        std::string immediatePredecessorTaskId;
        bool topologyStale{ false };
    };

    /** Target-side future memory delta. heldBytes is always a subset of inflightBytes. */
    struct FutureDelta {
        uint64_t inflightBytes{ 0 };
        uint64_t heldBytes{ 0 };
        uint64_t holdSinceMs{ 0 };
        uint64_t minimumObservedUsedMemory{ 0 };
    };

    static bool IsTerminalStatus(master::RebalanceTaskStatusPb status);
    static bool IsWorkerActiveInTopology(const std::string &worker, const cluster::TopologySnapshot *topologySnapshot);
    uint64_t GetTargetInflightBytesLocked(const std::string &targetWorker) const;
    void IncreaseTargetInflightLocked(const std::string &targetWorker, uint64_t bytes);
    void DecreaseInflightLocked(const std::string &targetWorker, uint64_t bytes);
    FutureDelta &HoldTargetInflightLocked(const std::string &targetWorker, uint64_t bytes, uint64_t nowMs,
                                          uint64_t minimumObservedUsedMemory = 0);
    void ReleaseHeldLocked(const std::string &targetWorker, uint64_t heldBytes);
    bool IsInCooldownLocked(const std::string &worker, uint64_t nowMs) const;
    void SetCooldownUntilLocked(const std::string &worker, uint64_t deadlineMs);
    void ExpireWorkerCooldownsLocked(uint64_t nowMs);
    std::vector<std::string> CollectExpiredTaskSourcesLocked(uint64_t nowMs) const;
    std::vector<std::pair<std::string, uint64_t>> CollectExpiredHoldsLocked(uint64_t nowMs, uint64_t holdTtlMs) const;
    std::vector<std::pair<std::string, uint64_t>> CollectReleasableHoldsLocked(
        const std::unordered_map<std::string, NodeInfo> &snapshot) const;
    const FutureDelta *GetReleasableReporterHoldLocked(const NodeInfo &reportingNode) const;

    // All helpers above are lock-free by contract: the concrete scheduler owns and must hold its mutex.
    std::unordered_map<std::string, RunningTask> activeTasksBySource_;
    std::unordered_map<std::string, uint64_t> cooldownUntilMs_;
    std::unordered_map<std::string, FutureDelta> futureView_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_REBALANCE_SCHEDULER_H
