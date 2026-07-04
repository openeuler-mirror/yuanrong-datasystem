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
 * Description: Background owner that rebuilds routing snapshots from committed topology facts.
 */
#include "datasystem/topology/runtime/topology_snapshot_rebuilder.h"

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

bool IsKnownNodeState(TopologyNodeState state)
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

Status FillMembershipFacts(const TopologyDescriptor &topology, RoutingSnapshotFacts &facts)
{
    facts.nodeOrder.reserve(topology.members.size());
    std::unordered_set<TopologyNodeId> nodeIds;
    nodeIds.reserve(topology.members.size());
    for (const auto &member : topology.members) {
        CHECK_FAIL_RETURN_STATUS(!member.nodeId.empty(), K_INVALID, "topology member id is empty");
        CHECK_FAIL_RETURN_STATUS(nodeIds.insert(member.nodeId).second, K_INVALID, "topology member id is duplicated");
        CHECK_FAIL_RETURN_STATUS(IsKnownNodeState(member.state), K_INVALID, "topology member state is invalid");
        facts.validTopologyNodeIds.emplace(member.nodeId);
        facts.nodeOrder.emplace_back(member.nodeId);
        if (member.state == TopologyNodeState::ACTIVE) {
            facts.activeTopologyNodeIds.emplace(member.nodeId);
        }
    }
    return Status::OK();
}

Status FillRedirectHints(const TopologyDescriptor &topology, ITopologyRoutingRepository &repository,
                         RoutingSnapshotFacts &facts)
{
    std::vector<TransferTaskRecord> transferTasks;
    RETURN_IF_NOT_OK(repository.ListTransferTaskRecords({}, transferTasks));
    for (const auto &task : transferTasks) {
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

Status BuildRoutingSnapshotFacts(const TopologyDescriptor &topology, ITopologyRoutingRepository &repository,
                                 RoutingSnapshotFacts &facts)
{
    facts = {};
    CHECK_FAIL_RETURN_STATUS(topology.version >= 0, K_INVALID, "topology version is invalid");
    CHECK_FAIL_RETURN_STATUS(topology.clusterHasInit, K_NOT_READY, "topology is not initialized");
    RETURN_IF_NOT_OK(FillMembershipFacts(topology, facts));
    return FillRedirectHints(topology, repository, facts);
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

TopologySnapshotRebuilder::TopologySnapshotRebuilder(RoutingView &routingView, ITopologyRoutingRepository &repository,
                                                     const IRoutingAlgorithm &routingAlgorithm)
    : routingView_(routingView), repository_(repository), routingAlgorithm_(routingAlgorithm)
{
}

Status TopologySnapshotRebuilder::RebuildFromCommittedTopology()
{
    TopologyDescriptor topology;
    Revision revision = 0;
    auto rc = repository_.GetCommittedTopology(topology, revision);
    if (rc.IsError()) {
        LOG(WARNING) << "Topology committed snapshot rebuild failed while reading topology, status: " << rc.ToString();
        RecordFailure(rc);
        return rc;
    }
    std::shared_ptr<const RoutingSnapshot> snapshot;
    rc = BuildRoutingSnapshotFromTopology(topology, repository_, routingAlgorithm_, snapshot);
    if (rc.IsError()) {
        LOG(WARNING) << "Topology committed snapshot rebuild failed, topologyVersion: " << topology.version
                     << ", revision: " << revision << ", status: " << rc.ToString();
        RecordFailure(rc);
        return rc;
    }
    if (routingView_.PublishIfNotOlder(std::move(snapshot))) {
        LOG(INFO) << "Topology routing snapshot rebuilt, topologyVersion: " << topology.version
                  << ", revision: " << revision;
        RecordSuccess(topology.version, revision);
    } else {
        RecordIgnoredEvent();
    }
    return Status::OK();
}

Status TopologySnapshotRebuilder::ApplyCommittedTopologyEvent(const CoordinationEvent &event)
{
    TopologyWatchEvent typed;
    auto rc = repository_.HandleCommittedTopologyEvent(event, typed);
    if (rc.GetCode() == K_NOT_FOUND) {
        RecordIgnoredEvent();
        return Status::OK();
    }
    if (rc.IsError()) {
        LOG(WARNING) << "Topology committed event decode failed, revision: " << event.revision
                     << ", status: " << rc.ToString();
        RecordFailure(rc);
        return rc;
    }
    if (typed.type == TopologyWatchEventType::DELETED) {
        RecordIgnoredEvent();
        return Status::OK();
    }
    std::shared_ptr<const RoutingSnapshot> snapshot;
    rc = BuildRoutingSnapshotFromTopology(typed.topology, repository_, routingAlgorithm_, snapshot);
    if (rc.IsError()) {
        LOG(WARNING) << "Topology committed event rebuild failed, topologyVersion: " << typed.topology.version
                     << ", revision: " << typed.revision << ", status: " << rc.ToString();
        RecordFailure(rc);
        return rc;
    }
    if (routingView_.PublishIfNotOlder(std::move(snapshot))) {
        LOG(INFO) << "Topology routing snapshot published from event, topologyVersion: " << typed.topology.version
                  << ", revision: " << typed.revision;
        RecordSuccess(typed.topology.version, typed.revision);
    } else {
        RecordIgnoredEvent();
    }
    return Status::OK();
}

TopologySnapshotRebuilderStats TopologySnapshotRebuilder::GetStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

void TopologySnapshotRebuilder::RecordSuccess(int64_t topologyVersion, Revision revision)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.successfulRebuilds;
    stats_.lastTopologyVersion = topologyVersion;
    stats_.lastTopologyRevision = revision;
    stats_.lastStatus = Status::OK();
}

void TopologySnapshotRebuilder::RecordFailure(const Status &status)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.failedRebuilds;
    stats_.lastStatus = status;
}

void TopologySnapshotRebuilder::RecordIgnoredEvent()
{
    std::lock_guard<std::mutex> lock(mutex_);
    ++stats_.ignoredEvents;
}

}  // namespace topology
}  // namespace datasystem
