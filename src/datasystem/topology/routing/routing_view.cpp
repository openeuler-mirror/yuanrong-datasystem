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
 * Description: R0 routing snapshot view.
 */
#include "datasystem/topology/routing/routing_view.h"

#include <mutex>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/algorithm/topology_algorithm.h"
#include "datasystem/topology/repository/topology_repository.h"

namespace datasystem {
namespace topology {
namespace {

bool IsKnownWorkerState(TopologyNodeState state)
{
    switch (state) {
        case TopologyNodeState::INITIAL:
        case TopologyNodeState::JOINING:
        case TopologyNodeState::ACTIVE:
        case TopologyNodeState::LEAVING:
        case TopologyNodeState::PRE_LEAVING:
            return true;
        default:
            return false;
    }
}

std::vector<RoutingOwnerEntry> ToRoutingOwners(const std::vector<AlgorithmOwnerRange> &ownerRanges)
{
    std::vector<RoutingOwnerEntry> owners;
    owners.reserve(ownerRanges.size());
    for (const auto &range : ownerRanges) {
        owners.emplace_back(RoutingOwnerEntry{ RoutingRange{ range.begin, range.end }, range.nodeId });
    }
    return owners;
}

Status BuildRoutingSnapshotFacts(const TopologyDescriptor &topology, ITopologyRoutingRepository &repository,
                                 RoutingSnapshotFacts &facts)
{
    facts = {};
    CHECK_FAIL_RETURN_STATUS(topology.version >= 0, K_INVALID, "topology version is invalid");
    CHECK_FAIL_RETURN_STATUS(topology.clusterHasInit, K_NOT_READY, "topology is not initialized");

    facts.nodeOrder.reserve(topology.members.size());
    std::unordered_set<TopologyNodeId> nodeIds;
    nodeIds.reserve(topology.members.size());
    for (const auto &worker : topology.members) {
        CHECK_FAIL_RETURN_STATUS(!worker.nodeId.empty(), K_INVALID, "topology worker id is empty");
        CHECK_FAIL_RETURN_STATUS(nodeIds.insert(worker.nodeId).second, K_INVALID,
                                 "topology worker id is duplicated");
        CHECK_FAIL_RETURN_STATUS(IsKnownWorkerState(worker.state), K_INVALID, "topology worker state is invalid");
        facts.validTopologyNodeIds.emplace(worker.nodeId);
        facts.nodeOrder.emplace_back(worker.nodeId);
        if (worker.state == TopologyNodeState::ACTIVE) {
            facts.activeTopologyNodeIds.emplace(worker.nodeId);
        }
    }

    std::vector<TransferTaskRecord> transferTasks;
    RETURN_IF_NOT_OK(repository.ListTransferTaskRecords({}, transferTasks));
    for (const auto &task : transferTasks) {
        // Backend task records are the migration-time authority. A worker may observe a transfer task before the
        // matching committed topology event, so keep non-stale hints even when the local topology version lags.
        if (task.targetTopologyVersion < topology.version) {
            continue;
        }
        for (const auto &range : task.ranges) {
            facts.redirectHints.emplace_back(
                RoutingRedirectHint{ RoutingRange{ range.begin, range.end }, task.targetNodeId, HostPort() });
        }
    }
    return Status::OK();
}

Status BuildRoutingSnapshotFromTopology(const TopologyDescriptor &topology, ITopologyRoutingRepository &repository,
                                        const IRoutingAlgorithm &algorithm,
                                        std::shared_ptr<const RoutingSnapshot> &snapshot)
{
    snapshot.reset();
    RoutingSnapshotFacts facts;
    RETURN_IF_NOT_OK(BuildRoutingSnapshotFacts(topology, repository, facts));
    if (topology.members.empty()) {
        snapshot = std::make_shared<RoutingSnapshot>(topology.version, std::vector<RoutingOwnerEntry>(),
                                                     std::move(facts));
        return Status::OK();
    }

    std::unique_ptr<AlgorithmRoutingState> routingState;
    RETURN_IF_NOT_OK(algorithm.BuildRoutingState(topology, routingState));
    CHECK_FAIL_RETURN_STATUS(routingState != nullptr, K_INVALID, "routing algorithm returned empty state");
    CHECK_FAIL_RETURN_STATUS(routingState->algorithmId == algorithm.GetAlgorithmId(), K_INVALID,
                             "routing algorithm returned mismatched state");
    CHECK_FAIL_RETURN_STATUS(routingState->topologyVersion == topology.version, K_INVALID,
                             "routing algorithm returned mismatched version");
    auto owners = ToRoutingOwners(routingState->ownerRanges);
    snapshot = std::make_shared<RoutingSnapshot>(topology.version, std::move(routingState), std::move(owners),
                                                 std::move(facts));
    return Status::OK();
}

}  // namespace

Status RoutingView::GetSnapshot(std::shared_ptr<const RoutingSnapshot> &snapshot) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    snapshot = snapshot_;
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_NOT_READY, "Routing snapshot is not published.");
    return Status::OK();
}

void RoutingView::Publish(std::shared_ptr<const RoutingSnapshot> snapshot)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    snapshot_ = std::move(snapshot);
}

Status RoutingView::ApplyCommittedTopology(ITopologyRoutingRepository &repository, const IRoutingAlgorithm &algorithm)
{
    TopologyDescriptor topology;
    Revision revision = 0;
    RETURN_IF_NOT_OK(repository.GetCommittedTopology(topology, revision));
    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(BuildRoutingSnapshotFromTopology(topology, repository, algorithm, snapshot));
    Publish(std::move(snapshot));
    return Status::OK();
}

Status RoutingView::ApplyCommittedTopologyEvent(ITopologyRoutingRepository &repository, const CoordinationEvent &event,
                                                const IRoutingAlgorithm &algorithm)
{
    TopologyWatchEvent typed;
    auto rc = repository.HandleCommittedTopologyEvent(event, typed);
    if (rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    if (typed.type == TopologyWatchEventType::DELETED) {
        return Status::OK();
    }
    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(BuildRoutingSnapshotFromTopology(typed.topology, repository, algorithm, snapshot));
    Publish(std::move(snapshot));
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
