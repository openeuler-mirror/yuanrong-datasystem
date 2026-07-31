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
#include "datasystem/coordinator/raft/coordinator_raft_node.h"

#include <algorithm>
#include <exception>
#include <unordered_set>
#include <utility>

#include <gflags/gflags.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/coordinator/raft/coordinator_raft_operation.h"
#include "datasystem/coordinator/raft/coordinator_raft_peer.h"

namespace datasystem::coordinator {
namespace {
Status NotReadyStatus(const char *operation)
{
    return Status(K_NOT_READY, FormatString("Coordinator raft node cannot %s before raft startup", operation));
}

Status BuildBootstrapConfiguration(const CoordinatorRaftOptions &options, braft::Configuration &configuration)
{
    configuration.reset();
    const auto *bootstrap = std::get_if<BootstrapPlan>(&options.startPlan);
    if (bootstrap == nullptr) {
        return Status::OK();
    }

    for (const auto &peerAddress : bootstrap->initialPeers) {
        braft::PeerId peer;
        RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(peerAddress, peer));
        CHECK_FAIL_RETURN_STATUS(configuration.add_peer(peer), K_INVALID,
                                 "BOOTSTRAP initialPeers contains a duplicate normalized peer address");
    }
    return Status::OK();
}

Status CommittedConfigurationError(const char *reason)
{
    return Status(K_DATA_INCONSISTENCY,
                  FormatString("Committed configuration for group %s is invalid: %s", kCoordinatorRaftGroupId, reason));
}

Status InvalidFollowerIdentityStatus()
{
    return Status(K_DATA_INCONSISTENCY, "Coordinator raft stable follower has an invalid internal identity");
}

Status MakeRaftOperationResult(const butil::Status &raftStatus, const std::string &operation,
                               const std::string &localPeer, const std::string &targetPeer)
{
    if (raftStatus.ok()) {
        return Status::OK();
    }
    return Status(
        K_RUNTIME_ERROR,
        FormatString("Failed to %s peer for group %s, local peer %s, target peer %s: braft error code=%d, message=%s",
                     operation, kCoordinatorRaftGroupId, localPeer, targetPeer, raftStatus.error_code(),
                     raftStatus.error_cstr()));
}

class RaftOperationClosure final : public braft::Closure {
public:
    RaftOperationClosure(std::string operation, std::string localPeer, std::string targetPeer,
                         std::shared_ptr<detail::RaftOperationSubmissionGate> submissionGate)
        : operation_(std::move(operation)),
          localPeer_(std::move(localPeer)),
          targetPeer_(std::move(targetPeer)),
          submissionGate_(std::move(submissionGate))
    {
    }

    ~RaftOperationClosure() override = default;

    void Run() override
    {
        auto result = MakeRaftOperationResult(status(), operation_, localPeer_, targetPeer_);
        auto submissionGate = std::move(submissionGate_);
        delete this;
        submissionGate->DispatchOrDefer(std::move(result));
    }

private:
    std::string operation_;
    std::string localPeer_;
    std::string targetPeer_;
    std::shared_ptr<detail::RaftOperationSubmissionGate> submissionGate_;
};
}  // namespace

CoordinatorRaftNode::CoordinatorRaftNode(CoordinatorRaftOptions options, CoordinatorRaftEventCallbacks callbacks)
    : options_(std::move(options)),
      callbacks_(std::move(callbacks)),
      operationDrainState_(std::make_shared<detail::RaftOperationDrainState>())
{
}

CoordinatorRaftNode::~CoordinatorRaftNode() noexcept
{
    ShutdownInternal();
}

void detail::CoordinatorRaftNodeTestAccessor::SetOperationDrainEntryObserver(CoordinatorRaftNode &node,
                                                                             std::function<void()> observer)
{
    node.operationDrainState_->SetDrainEntryObserverForTest(std::move(observer));
}

CoordinatorRaftEventCallbacks CoordinatorRaftNode::MakeStateMachineCallbacks()
{
    const auto onConfigurationCommitted = callbacks_.onConfigurationCommitted;
    const auto onError = callbacks_.onError;

    CoordinatorRaftEventCallbacks wrappedCallbacks;
    wrappedCallbacks.onLeaderStart = callbacks_.onLeaderStart;
    wrappedCallbacks.onLeaderStop = callbacks_.onLeaderStop;
    wrappedCallbacks.onConfigurationCommitted = [this, onConfigurationCommitted, onError](
                                                    std::vector<std::string> peers, int64_t index) {
        HandleConfigurationCommitted(std::move(peers), index, onConfigurationCommitted, onError);
    };
    wrappedCallbacks.onError = callbacks_.onError;
    wrappedCallbacks.onShutdown = callbacks_.onShutdown;
    return wrappedCallbacks;
}

void CoordinatorRaftNode::HandleConfigurationCommitted(
    std::vector<std::string> peers, int64_t index,
    const std::function<void(std::vector<std::string>, int64_t)> &onConfigurationCommitted,
    const std::function<void(Status)> &onError)
{
    const auto reportError = [&onError](const char *reason) {
        if (!onError) {
            return;
        }
        try {
            onError(CommittedConfigurationError(reason));
        } catch (const std::exception &e) {
            LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
        } catch (...) {
            LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker;
        }
    };

    if (index < 0) {
        reportError("the committed index must not be negative");
        return;
    }
    if (peers.empty()) {
        reportError("the peer list must not be empty");
        return;
    }

    CommittedConfigurationSnapshot snapshot;
    snapshot.peers.reserve(peers.size());
    snapshot.index = index;
    std::unordered_set<std::string> normalizedIdentities;
    normalizedIdentities.reserve(peers.size());
    for (const auto &peerIdentity : peers) {
        braft::PeerId internalPeer;
        if (internalPeer.parse(peerIdentity) != 0 || internalPeer.is_empty() || internalPeer.idx != 0
            || internalPeer.to_string() != peerIdentity) {
            reportError("a peer has an invalid internal identity or nonzero braft index");
            return;
        }

        auto normalizedAddress = CoordinatorRaftPeerAddress(internalPeer);
        braft::PeerId normalizedPeer;
        if (normalizedAddress.empty() || ParseCoordinatorRaftPeer(normalizedAddress, normalizedPeer).IsError()) {
            reportError("a peer cannot be decoded by the Coordinator peer codec");
            return;
        }
        normalizedAddress = CoordinatorRaftPeerAddress(normalizedPeer);
        if (normalizedAddress.empty()) {
            reportError("a peer cannot be encoded by the Coordinator peer codec");
            return;
        }
        if (!normalizedIdentities.emplace(normalizedAddress).second) {
            reportError("multiple peers normalize to the same Coordinator identity");
            return;
        }
        snapshot.peers.emplace_back(std::move(normalizedAddress));
    }

    std::sort(snapshot.peers.begin(), snapshot.peers.end());
    auto callbackPeers = snapshot.peers;
    {
        std::lock_guard<std::mutex> lock(committedConfigurationMutex_);
        committedConfiguration_ = std::move(snapshot);
    }
    if (onConfigurationCommitted) {
        onConfigurationCommitted(std::move(callbackPeers), index);
    }
}

Status CoordinatorRaftNode::Start(RaftMetadataState metadataState)
{
    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    if (state_ != LifecycleState::CONSTRUCTED) {
        return Status(K_INVALID, "Coordinator raft node cannot be started more than once or after shutdown");
    }

    RETURN_IF_NOT_OK(ValidateCoordinatorRaftOptions(options_, metadataState));
    RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(options_.localPeer, localPeer_));
    RETURN_IF_NOT_OK(CreateDir(options_.dataDir, true));

    braft::Configuration initialConfiguration;
    RETURN_IF_NOT_OK(BuildBootstrapConfiguration(options_, initialConfiguration));

    auto stateMachine = std::make_unique<CoordinatorRaftStateMachine>(MakeStateMachineCallbacks());
    braft::NodeOptions nodeOptions;
    // braft v1.1.2 derives heartbeat from election_timeout_ms and the global heartbeat factor.
    const auto heartbeatFactor = std::to_string(options_.electionTimeoutMs / options_.heartbeatIntervalMs);
    (void)gflags::SetCommandLineOption("raft_election_heartbeat_factor", heartbeatFactor.c_str());
    nodeOptions.election_timeout_ms = options_.electionTimeoutMs;
    nodeOptions.initial_conf = std::move(initialConfiguration);
    nodeOptions.fsm = stateMachine.get();
    nodeOptions.node_owns_fsm = false;
    nodeOptions.log_uri = "local://" + options_.dataDir + "/log";
    nodeOptions.raft_meta_uri = "local://" + options_.dataDir + "/raft_meta";
    nodeOptions.snapshot_uri = "local://" + options_.dataDir + "/snapshot";
    nodeOptions.snapshot_interval_s = 0;
    nodeOptions.disable_cli = true;

    auto node = std::make_unique<braft::Node>(kCoordinatorRaftGroupId, localPeer_);
    const int rc = node->init(nodeOptions);
    if (rc != 0) {
        const auto status =
            Status(K_RUNTIME_ERROR,
                   FormatString("Failed to initialize braft node for group %s, local peer %s, data root %s, rc=%d",
                                kCoordinatorRaftGroupId, options_.localPeer, options_.dataDir, rc));
        state_ = LifecycleState::STOPPING;
        lock.unlock();
        // braft v1.1.2 Node destruction is the canonical partial-init cleanup path; do not call join directly here.
        node.reset();
        stateMachine.reset();
        {
            std::lock_guard<std::mutex> configurationLock(committedConfigurationMutex_);
            committedConfiguration_.reset();
        }
        lock.lock();
        state_ = LifecycleState::STOPPED;
        return status;
    }

    const bool publishBootstrapConfiguration = std::holds_alternative<BootstrapPlan>(options_.startPlan);
    CoordinatorRaftStateMachine *publishedStateMachine = nullptr;
    stateMachine_ = std::move(stateMachine);
    node_ = std::move(node);
    state_ = LifecycleState::STARTED;
    if (publishBootstrapConfiguration) {
        publishedStateMachine = stateMachine_.get();
        std::lock_guard<std::mutex> publishLock(configurationPublishMutex_);
        configurationPublishInProgress_ = true;
    }
    lock.unlock();

    if (publishBootstrapConfiguration) {
        Raii publishComplete([this] {
            {
                std::lock_guard<std::mutex> publishLock(configurationPublishMutex_);
                configurationPublishInProgress_ = false;
            }
            configurationPublishCv_.notify_all();
        });
        publishedStateMachine->on_configuration_committed(nodeOptions.initial_conf, 0);
    }
    return Status::OK();
}

void CoordinatorRaftNode::ShutdownInternal() noexcept
{
    std::unique_ptr<CoordinatorRaftStateMachine> stateMachine;
    std::unique_ptr<braft::Node> node;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        operationDrainState_->StopAcceptingNewTokens();
        if (state_ == LifecycleState::STOPPED) {
            return;
        }
        state_ = LifecycleState::STOPPING;
        lock.unlock();
        {
            std::unique_lock<std::mutex> publishLock(configurationPublishMutex_);
            configurationPublishCv_.wait(publishLock, [this] { return !configurationPublishInProgress_; });
        }
        lock.lock();
        node = std::move(node_);
        stateMachine = std::move(stateMachine_);
    }

    if (node != nullptr) {
        node->shutdown(nullptr);
        node->join();
    }
    operationDrainState_->WaitForDrain();
    node.reset();
    stateMachine.reset();
    {
        std::lock_guard<std::mutex> lock(committedConfigurationMutex_);
        committedConfiguration_.reset();
    }
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        state_ = LifecycleState::STOPPED;
    }
}

bool CoordinatorRaftNode::IsLeader() const
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    return state_ == LifecycleState::STARTED && node_ != nullptr && node_->is_leader();
}

bool CoordinatorRaftNode::UpdateObservedLeaderLocked(const std::string &leaderAddress,
                                                     std::string &previousLeader) const
{
    if (leaderAddress == lastObservedLeader_) {
        return false;
    }
    previousLeader = lastObservedLeader_;
    lastObservedLeader_ = leaderAddress;
    return true;
}

Status CoordinatorRaftNode::GetLeader(std::string &leaderAddress) const
{
    leaderAddress.clear();
    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    if (state_ != LifecycleState::STARTED || node_ == nullptr) {
        return NotReadyStatus("report a leader");
    }

    const auto leader = node_->leader_id();
    if (leader.is_empty()) {
        return Status(K_NOT_READY, "Coordinator raft leader is not known yet");
    }
    auto normalizedAddress = CoordinatorRaftPeerAddress(leader);
    if (normalizedAddress.empty()) {
        return Status(K_DATA_INCONSISTENCY, "Coordinator raft leader has an invalid internal identity");
    }
    std::string previousLeader;
    const bool leaderChanged = UpdateObservedLeaderLocked(normalizedAddress, previousLeader);
    leaderAddress = std::move(normalizedAddress);
    const auto currentLeader = leaderAddress;
    lock.unlock();
    if (leaderChanged) {
        LOG(INFO) << "COORDINATOR_RAFT_LEADER_CHANGED current_addr=" << options_.localPeer
                  << " old_leader=" << previousLeader << " new_leader=" << currentLeader << " reason=get_leader";
    }
    return Status::OK();
}

Status CoordinatorRaftNode::GetCommittedConfiguration(std::vector<std::string> &peers, int64_t &index) const
{
    peers.clear();
    index = 0;
    std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
    if (state_ != LifecycleState::STARTED || node_ == nullptr) {
        return NotReadyStatus("report a committed configuration");
    }

    std::lock_guard<std::mutex> configurationLock(committedConfigurationMutex_);
    if (!committedConfiguration_.has_value()) {
        return Status(K_NOT_READY, "Coordinator raft committed configuration is not known yet");
    }
    peers = committedConfiguration_->peers;
    index = committedConfiguration_->index;
    return Status::OK();
}

Status CoordinatorRaftNode::GetMembershipStatus(CoordinatorRaftMembershipStatus &status) const
{
    status = {};
    std::unique_lock<std::mutex> lifecycleLock(lifecycleMutex_);
    if (state_ != LifecycleState::STARTED || node_ == nullptr) {
        return NotReadyStatus("report membership status");
    }

    braft::NodeStatus raftStatus;
    node_->get_status(&raftStatus);
    const auto leader = node_->leader_id();
    std::string currentLeader;
    bool leaderObserveFailed = false;
    if (!leader.is_empty()) {
        currentLeader = CoordinatorRaftPeerAddress(leader);
        leaderObserveFailed = currentLeader.empty();
    }
    std::string previousLeader;
    const bool leaderChanged =
        !leader.is_empty() && !currentLeader.empty() && UpdateObservedLeaderLocked(currentLeader, previousLeader);

    CommittedConfigurationSnapshot committedConfiguration;
    {
        std::lock_guard<std::mutex> configurationLock(committedConfigurationMutex_);
        if (!committedConfiguration_.has_value()) {
            return Status(K_NOT_READY, "Coordinator raft committed configuration is not known yet");
        }
        committedConfiguration = *committedConfiguration_;
    }

    CoordinatorRaftMembershipStatus observedStatus;
    observedStatus.isLeader = raftStatus.state == braft::STATE_LEADER;
    observedStatus.term = raftStatus.term;
    observedStatus.configurationIndex = committedConfiguration.index;
    observedStatus.committedPeers = std::move(committedConfiguration.peers);
    observedStatus.stableFollowers.reserve(raftStatus.stable_followers.size());
    for (const auto &[internalPeer, peerStatus] : raftStatus.stable_followers) {
        auto normalizedAddress = CoordinatorRaftPeerAddress(internalPeer);
        braft::PeerId normalizedPeer;
        if (normalizedAddress.empty() || ParseCoordinatorRaftPeer(normalizedAddress, normalizedPeer).IsError()) {
            return InvalidFollowerIdentityStatus();
        }
        normalizedAddress = CoordinatorRaftPeerAddress(normalizedPeer);
        if (normalizedAddress.empty()) {
            return InvalidFollowerIdentityStatus();
        }
        if (!std::binary_search(observedStatus.committedPeers.begin(), observedStatus.committedPeers.end(),
                                normalizedAddress)) {
            continue;
        }
        observedStatus.stableFollowers.emplace_back(CoordinatorFollowerStatus{
            std::move(normalizedAddress), peerStatus.valid, peerStatus.consecutive_error_times });
    }
    std::sort(
        observedStatus.stableFollowers.begin(), observedStatus.stableFollowers.end(),
        [](const CoordinatorFollowerStatus &lhs, const CoordinatorFollowerStatus &rhs) { return lhs.peer < rhs.peer; });
    status = std::move(observedStatus);
    lifecycleLock.unlock();
    if (leaderObserveFailed) {
        LOG(WARNING) << "COORDINATOR_RAFT_LEADER_OBSERVE_FAILED current_addr=" << options_.localPeer
                     << " reason=membership_status";
    }
    if (leaderChanged) {
        LOG(INFO) << "COORDINATOR_RAFT_LEADER_CHANGED current_addr=" << options_.localPeer
                  << " old_leader=" << previousLeader << " new_leader=" << currentLeader << " reason=membership_status";
    }
    return Status::OK();
}

Status CoordinatorRaftNode::SubmitPeerMembershipChange(const std::string &peer, RaftOperationCallback &&callback,
                                                       PeerMembershipOperation operation)
{
    const char *operationName = nullptr;
    const char *notReadyOperation = nullptr;
    switch (operation) {
        case PeerMembershipOperation::ADD:
            operationName = "add";
            notReadyOperation = "add a peer";
            break;
        case PeerMembershipOperation::REMOVE:
            operationName = "remove";
            notReadyOperation = "remove a peer";
            break;
    }

    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    if (state_ != LifecycleState::STARTED || node_ == nullptr) {
        return NotReadyStatus(notReadyOperation);
    }
    if (!callback) {
        return Status(K_INVALID, FormatString("Coordinator raft %s peer callback must not be empty", operationName));
    }

    braft::PeerId targetPeer;
    RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(peer, targetPeer));
    if (operation == PeerMembershipOperation::REMOVE) {
        std::lock_guard<std::mutex> configurationLock(committedConfigurationMutex_);
        if (!committedConfiguration_.has_value()) {
            return Status(K_NOT_READY, "Coordinator raft committed configuration is not known yet");
        }
        const auto normalizedTargetPeer = CoordinatorRaftPeerAddress(targetPeer);
        if (committedConfiguration_->peers.size() == 1
            && committedConfiguration_->peers.front() == normalizedTargetPeer) {
            return Status(K_INVALID,
                          FormatString("Coordinator raft cannot remove the sole committed voter for group %s, "
                                       "local peer %s, target peer %s",
                                       kCoordinatorRaftGroupId, options_.localPeer, normalizedTargetPeer));
        }
    }
    if (!node_->is_leader()) {
        return Status(
            K_RUNTIME_ERROR,
            FormatString("Coordinator raft node is not leader and cannot %s peer for group %s, local peer %s, "
                         "target peer %s",
                         operationName, kCoordinatorRaftGroupId, options_.localPeer, peer));
    }

    auto inFlightToken = std::make_shared<detail::RaftOperationDrainToken>(operationDrainState_);
    if (!inFlightToken->IsAcquired()) {
        return NotReadyStatus(notReadyOperation);
    }
    RaftOperationCallback trackedCallback = [callback = std::move(callback),
                                             inFlightToken = std::move(inFlightToken)](Status result) mutable {
        detail::InvokeRaftOperationCallback(std::move(callback), std::move(result));
    };
    auto submissionGate = std::make_shared<detail::RaftOperationSubmissionGate>(std::move(trackedCallback));
    auto done = std::make_unique<RaftOperationClosure>(operationName, options_.localPeer, peer, submissionGate);
    switch (operation) {
        case PeerMembershipOperation::ADD:
            node_->add_peer(targetPeer, done.release());
            break;
        case PeerMembershipOperation::REMOVE:
            node_->remove_peer(targetPeer, done.release());
            break;
    }

    lock.unlock();
    submissionGate->MarkSubmissionComplete();
    return Status::OK();
}

Status CoordinatorRaftNode::AddPeer(const std::string &peer, RaftOperationCallback callback)
{
    return SubmitPeerMembershipChange(peer, std::move(callback), PeerMembershipOperation::ADD);
}

Status CoordinatorRaftNode::RemovePeer(const std::string &peer, RaftOperationCallback callback)
{
    return SubmitPeerMembershipChange(peer, std::move(callback), PeerMembershipOperation::REMOVE);
}

}  // namespace datasystem::coordinator
