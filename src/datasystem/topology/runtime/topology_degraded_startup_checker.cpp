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
 * Description: Degraded startup checker for topology snapshots.
 */
#include "datasystem/topology/runtime/topology_degraded_startup_checker.h"

#include <unordered_set>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/runtime/topology_state_digest.h"

namespace datasystem {
namespace topology {
namespace {

void Reject(const std::string &reason, DegradedStartupDecision &decision)
{
    decision.accepted = false;
    decision.reason = reason;
}

bool SameTaskSummary(const TopologyTaskSummary &left, const TopologyTaskSummary &right)
{
    return left.unfinishedTransferTasks == right.unfinishedTransferTasks
           && left.unfinishedRecoveryTasks == right.unfinishedRecoveryTasks;
}

std::unordered_set<TopologyNodeId> BuildMemberSet(const TopologyDescriptor &topology)
{
    std::unordered_set<TopologyNodeId> members;
    members.reserve(topology.members.size());
    for (const auto &member : topology.members) {
        members.emplace(member.nodeId);
    }
    return members;
}

bool HasPeerQuorum(const TopologyDescriptor &topology, size_t peerCount)
{
    if (topology.members.empty() || peerCount == 0) {
        return false;
    }
    const auto observedMembers = peerCount + 1;
    const auto majority = topology.members.size() / 2 + 1;
    return observedMembers >= majority;
}

Status ValidateLocalSnapshot(const LocalTopologySnapshot &localSnapshot)
{
    CHECK_FAIL_RETURN_STATUS(localSnapshot.topology.clusterHasInit, K_INVALID, "local snapshot is not initialized");
    CHECK_FAIL_RETURN_STATUS(!localSnapshot.digest.empty(), K_INVALID, "local snapshot digest is empty");
    std::string expectedDigest;
    RETURN_IF_NOT_OK(TopologyStateDigest::Build(localSnapshot, expectedDigest));
    CHECK_FAIL_RETURN_STATUS(localSnapshot.digest == expectedDigest, K_INVALID, "local snapshot digest mismatch");
    return Status::OK();
}

bool RejectIfPeerSetUnavailable(const LocalTopologySnapshot &localSnapshot,
                                const std::vector<PeerStateSnapshot> &peerStates,
                                DegradedStartupDecision &decision)
{
    if (peerStates.empty()) {
        Reject("no peer state is available", decision);
        return true;
    }
    if (!HasPeerQuorum(localSnapshot.topology, peerStates.size())) {
        Reject("peer quorum is not available", decision);
        return true;
    }
    return false;
}

bool AcceptPeerState(const LocalTopologySnapshot &localSnapshot, const std::unordered_set<TopologyNodeId> &members,
                     std::unordered_set<TopologyNodeId> &observedPeers, const PeerStateSnapshot &peer,
                     DegradedStartupDecision &decision)
{
    if (!observedPeers.insert(peer.nodeId).second) {
        Reject("duplicated peer state: " + peer.nodeId, decision);
        return false;
    }
    if (members.find(peer.nodeId) == members.end()) {
        Reject("peer is not a topology member: " + peer.nodeId, decision);
        return false;
    }
    if (peer.topologyVersion != localSnapshot.topology.version
        || peer.topologyRevision != localSnapshot.topologyRevision) {
        Reject("peer topology version mismatch: " + peer.nodeId, decision);
        return false;
    }
    if (!peer.ready) {
        Reject("peer is not ready: " + peer.nodeId, decision);
        return false;
    }
    if (peer.backendAvailable) {
        Reject("backend is available from peer: " + peer.nodeId, decision);
        return false;
    }
    if (peer.digest != localSnapshot.digest) {
        Reject("peer digest mismatch: " + peer.nodeId, decision);
        return false;
    }
    if (!SameTaskSummary(peer.taskSummary, localSnapshot.taskSummary)) {
        Reject("peer task summary mismatch: " + peer.nodeId, decision);
        return false;
    }
    return true;
}

}  // namespace

Status TopologyDegradedStartupChecker::Check(const LocalTopologySnapshot &localSnapshot,
                                             const std::vector<PeerStateSnapshot> &peerStates,
                                             DegradedStartupDecision &decision) const
{
    decision = {};
    RETURN_IF_NOT_OK(ValidateLocalSnapshot(localSnapshot));
    if (RejectIfPeerSetUnavailable(localSnapshot, peerStates, decision)) {
        return Status::OK();
    }
    const auto members = BuildMemberSet(localSnapshot.topology);
    std::unordered_set<TopologyNodeId> observedPeers;
    observedPeers.reserve(peerStates.size());
    for (const auto &peer : peerStates) {
        if (!AcceptPeerState(localSnapshot, members, observedPeers, peer, decision)) {
            return Status::OK();
        }
    }
    decision.accepted = true;
    decision.reason = "local snapshot matches all ready peers while backend is unavailable";
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
