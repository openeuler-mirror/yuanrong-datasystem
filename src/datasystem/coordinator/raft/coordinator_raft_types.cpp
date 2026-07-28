// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Description: Coordinator raft startup option validation.
 */
#include "datasystem/coordinator/raft/coordinator_raft_types.h"

#include <unordered_set>

#include "datasystem/coordinator/raft/coordinator_raft_peer.h"

namespace datasystem::coordinator {

Status ValidateCoordinatorRaftOptions(const CoordinatorRaftOptions &options, RaftMetadataState metadataState)
{
    braft::PeerId localPeer;
    const auto localPeerStatus = ParseCoordinatorRaftPeer(options.localPeer, localPeer);
    if (localPeerStatus.IsError()) {
        return Status(K_INVALID, std::string("localPeer is invalid: ") + localPeerStatus.GetMsg());
    }
    if (options.dataDir.empty()) {
        return Status(K_INVALID, "dataDir must not be empty");
    }
    if (options.electionTimeoutMs < kCoordinatorRaftMinElectionTimeoutMs
        || options.electionTimeoutMs > kCoordinatorRaftMaxElectionTimeoutMs) {
        return Status(K_INVALID, "electionTimeoutMs=" + std::to_string(options.electionTimeoutMs)
                                     + " is outside the valid inclusive range ["
                                     + std::to_string(kCoordinatorRaftMinElectionTimeoutMs) + ", "
                                     + std::to_string(kCoordinatorRaftMaxElectionTimeoutMs) + "]");
    }

    if (metadataState == RaftMetadataState::CORRUPT) {
        return Status(K_DATA_INCONSISTENCY, "Local raft metadata is corrupt and cannot be used for startup");
    }
    if (metadataState == RaftMetadataState::UNKNOWN) {
        return Status(K_NOT_READY, "Local raft metadata state is unknown, so raft startup is not ready");
    }

    if (const auto *bootstrap = std::get_if<BootstrapPlan>(&options.startPlan); bootstrap != nullptr) {
        if (metadataState != RaftMetadataState::ABSENT) {
            return Status(K_INVALID, "BOOTSTRAP requires ABSENT raft metadata");
        }
        if (bootstrap->initialPeers.empty()) {
            return Status(K_INVALID, "BOOTSTRAP requires at least one initial peer");
        }

        std::unordered_set<std::string> normalizedPeers;
        normalizedPeers.reserve(bootstrap->initialPeers.size());
        bool containsLocalPeer = false;
        const auto normalizedLocalPeer = CoordinatorRaftPeerAddress(localPeer);
        for (const auto &initialPeerAddress : bootstrap->initialPeers) {
            braft::PeerId initialPeer;
            const auto initialPeerStatus = ParseCoordinatorRaftPeer(initialPeerAddress, initialPeer);
            if (initialPeerStatus.IsError()) {
                return Status(K_INVALID, std::string("BOOTSTRAP initialPeers contains an invalid address: ")
                                             + initialPeerStatus.GetMsg());
            }

            const auto normalizedInitialPeer = CoordinatorRaftPeerAddress(initialPeer);
            if (!normalizedPeers.emplace(normalizedInitialPeer).second) {
                return Status(K_INVALID, "BOOTSTRAP initialPeers contains a duplicate normalized peer address");
            }
            containsLocalPeer = containsLocalPeer || normalizedInitialPeer == normalizedLocalPeer;
        }
        if (!containsLocalPeer) {
            return Status(K_INVALID, "BOOTSTRAP initialPeers must contain localPeer after address normalization");
        }
        return Status::OK();
    }

    if (std::holds_alternative<RecoverPlan>(options.startPlan)) {
        return metadataState == RaftMetadataState::VALID ? Status::OK()
                                                         : Status(K_INVALID, "RECOVER requires VALID raft metadata");
    }

    if (std::holds_alternative<WaitingToJoinPlan>(options.startPlan)) {
        return metadataState == RaftMetadataState::ABSENT
                   ? Status::OK()
                   : Status(K_INVALID, "WAITING_TO_JOIN requires ABSENT raft metadata");
    }

    return Status(K_INVALID, "Unsupported raft start plan");
}

}  // namespace datasystem::coordinator
