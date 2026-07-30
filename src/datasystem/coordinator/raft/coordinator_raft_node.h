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
 * Description: Coordinator-owned braft node lifecycle.
 */
#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_NODE_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_NODE_H

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <braft/raft.h>

#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::coordinator {

namespace detail {
class RaftOperationDrainState;
class CoordinatorRaftNodeTestAccessor;
}  // namespace detail

using RaftOperationCallback = std::function<void(Status)>;

inline constexpr int kCoordinatorFollowerFailureErrorThreshold = 5;

struct CoordinatorFollowerStatus {
    std::string peer;
    bool valid{ false };
    int consecutiveErrorTimes{ 0 };
};

struct CoordinatorRaftMembershipStatus {
    bool isLeader{ false };
    int64_t term{ 0 };
    int64_t configurationIndex{ 0 };
    std::vector<std::string> committedPeers;
    std::vector<CoordinatorFollowerStatus> stableFollowers;
};

// Lifecycle contract: one external owner destroys the Node. FSM and Add/Remove callbacks may only notify that owner;
// they must not destroy the Node or retain a strong reference that could become its last owner. The owner must destroy
// CoordinatorMembershipManager before the Node and destroy the shared brpc server after the Node.
class CoordinatorRaftNode final {
public:
    CoordinatorRaftNode(CoordinatorRaftOptions options, CoordinatorRaftEventCallbacks callbacks);
    // Sole synchronous shutdown entry, invoked by the external owner through destruction.
    ~CoordinatorRaftNode() noexcept;

    CoordinatorRaftNode(const CoordinatorRaftNode &) = delete;
    CoordinatorRaftNode &operator=(const CoordinatorRaftNode &) = delete;
    CoordinatorRaftNode(CoordinatorRaftNode &&) = delete;
    CoordinatorRaftNode &operator=(CoordinatorRaftNode &&) = delete;

    Status Start(RaftMetadataState metadataState);

    bool IsLeader() const;
    Status GetLeader(std::string &leaderAddress) const;
    Status GetCommittedConfiguration(std::vector<std::string> &peers, int64_t &index) const;
    Status GetMembershipStatus(CoordinatorRaftMembershipStatus &status) const;

    Status AddPeer(const std::string &peer, RaftOperationCallback callback);
    Status RemovePeer(const std::string &peer, RaftOperationCallback callback);

private:
    friend class detail::CoordinatorRaftNodeTestAccessor;

    enum class LifecycleState { CONSTRUCTED, STARTED, STOPPING, STOPPED };
    enum class PeerMembershipOperation : uint8_t { ADD, REMOVE };

    struct CommittedConfigurationSnapshot {
        std::vector<std::string> peers;
        int64_t index{ 0 };
    };

    CoordinatorRaftEventCallbacks MakeStateMachineCallbacks();
    void ShutdownInternal() noexcept;
    Status SubmitPeerMembershipChange(const std::string &peer, RaftOperationCallback &&callback,
                                      PeerMembershipOperation operation);
    void HandleConfigurationCommitted(
        std::vector<std::string> peers, int64_t index,
        const std::function<void(std::vector<std::string>, int64_t)> &onConfigurationCommitted,
        const std::function<void(Status)> &onError);

    CoordinatorRaftOptions options_;
    CoordinatorRaftEventCallbacks callbacks_;
    std::shared_ptr<detail::RaftOperationDrainState> operationDrainState_;
    braft::PeerId localPeer_;
    mutable std::mutex lifecycleMutex_;
    LifecycleState state_{ LifecycleState::CONSTRUCTED };
    mutable std::mutex committedConfigurationMutex_;
    std::optional<CommittedConfigurationSnapshot> committedConfiguration_;
    std::mutex configurationPublishMutex_;
    std::condition_variable configurationPublishCv_;
    bool configurationPublishInProgress_{ false };
    // Wrapped FSM callbacks borrow this and the Node borrows the FSM; declaration order destroys the Node first.
    std::unique_ptr<CoordinatorRaftStateMachine> stateMachine_;
    std::unique_ptr<braft::Node> node_;
};

namespace detail {

class CoordinatorRaftNodeTestAccessor final {
public:
    CoordinatorRaftNodeTestAccessor() = delete;
    ~CoordinatorRaftNodeTestAccessor() = delete;

    static void SetOperationDrainEntryObserver(CoordinatorRaftNode &node, std::function<void()> observer);
};

}  // namespace detail

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_NODE_H
