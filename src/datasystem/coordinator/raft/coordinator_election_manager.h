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
 * Description: Sole owner of Coordinator raft bootstrap, election, and membership lifecycles.
 */
#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_ELECTION_MANAGER_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_ELECTION_MANAGER_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/coordinator/raft/coordinator_membership_manager.h"
#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

namespace datasystem::coordinator {

struct CoordinatorElectionOptions {
    CoordinatorRaftFlags raftFlags;
    CoordinatorMembershipOptions membershipOptions;
};

// One external owner serializes Start, StopMembership, Shutdown, leader queries, and destruction.
// Raft callbacks may only notify that independently owned lifecycle controller. They must not call StopMembership or
// Shutdown, destroy this Manager or its Node, or retain a strong reference that can become the final lifecycle owner.
// The shared RpcServer remains externally owned and must outlive complete Manager and Node destruction.
class CoordinatorElectionManager final {
public:
    CoordinatorElectionManager(CoordinatorElectionOptions options, CoordinatorRaftEventCallbacks callbacks,
                               std::shared_ptr<ICoordinatorDiscovery> discovery);
    ~CoordinatorElectionManager() noexcept;

    CoordinatorElectionManager(const CoordinatorElectionManager &) = delete;
    CoordinatorElectionManager &operator=(const CoordinatorElectionManager &) = delete;
    CoordinatorElectionManager(CoordinatorElectionManager &&) = delete;
    CoordinatorElectionManager &operator=(CoordinatorElectionManager &&) = delete;

    Status Start();
    Status StopMembership();
    Status Shutdown();

    Status GetBootstrapState(RaftBootstrapState &state) const;
    bool IsLeader() const;
    Status GetLeader(std::string &leaderAddress) const;

private:
    enum class LifecycleState { CONSTRUCTED, RUNNING, STOPPING, STOPPED };

    struct NodeHandle {
        std::unique_ptr<CoordinatorRaftNode> node;
        std::function<void()> onDestroyed;

        ~NodeHandle() noexcept;
    };

    struct MembershipHandle {
        std::unique_ptr<CoordinatorMembershipManager> membership;
        std::function<void()> onDestroyed;

        ~MembershipHandle() noexcept;
    };

    struct BootstrapObservation {
        std::string peer;
        RaftBootstrapState state;
        Status status;
    };

    struct Dependencies {
        std::function<Status(const std::string &, RaftMetadataState &)> probeLocalMetadata;
        std::function<Status(const std::shared_ptr<ICoordinatorDiscovery> &, std::vector<std::string> &)>
            discoverCandidates;
        std::function<Status(const std::string &, int32_t, RaftBootstrapState &)> probePeer;
        std::function<Status(const std::vector<std::string> &, std::string &)> digestCandidates;
        std::function<std::chrono::steady_clock::time_point()> now;
        std::function<std::chrono::milliseconds(std::chrono::milliseconds)> jitterBootstrapRetry;
        std::function<void()> onBootstrapWorkerExit;
        std::function<std::unique_ptr<NodeHandle>(const CoordinatorRaftOptions &,
                                                  const CoordinatorRaftEventCallbacks &)> createNode;
        std::function<Status(NodeHandle &, RaftMetadataState)> startNode;
        std::function<std::unique_ptr<MembershipHandle>(const CoordinatorMembershipOptions &, NodeHandle &,
                                                        const std::shared_ptr<ICoordinatorDiscovery> &)>
            createMembership;
        std::function<Status(MembershipHandle &)> startMembership;
        std::function<Status(MembershipHandle &)> shutdownMembership;
        std::function<bool(const NodeHandle &)> isLeader;
        std::function<Status(const NodeHandle &, std::string &)> getLeader;
    };

    CoordinatorElectionManager(CoordinatorElectionOptions options, CoordinatorRaftEventCallbacks callbacks,
                               std::shared_ptr<ICoordinatorDiscovery> discovery, Dependencies dependencies);

    static Dependencies MakeProductionDependencies();
    Status ValidateStartupInput() const;
    void RunBootstrapControl() noexcept;
    Status RefreshBootstrapObservation(RaftBootstrapState &localState,
                                       std::vector<std::string> &normalizedCandidates);
    Status TryBuildStartPlan(const RaftBootstrapState &localState,
                             const std::vector<std::string> &normalizedCandidates, RaftStartPlan &startPlan);
    Status CollectBootstrapObservations(const RaftBootstrapState &localState,
                                        const std::vector<std::string> &normalizedCandidates,
                                        std::vector<BootstrapObservation> &observations) const;
    Status DecideStartPlan(const RaftBootstrapState &localState,
                           const std::vector<std::string> &normalizedCandidates,
                           const std::vector<BootstrapObservation> &observations, RaftStartPlan &startPlan) const;
    Status StartOwnedComponents(RaftStartPlan startPlan, RaftMetadataState metadataState);
    Status ProbePeerBootstrapState(const std::string &peer, RaftBootstrapState &state) const;
    CoordinatorRaftEventCallbacks BuildManagedCallbacks();
    void PublishBootstrapState(RaftBootstrapState state);
    void RecordBootstrapTerminalStatus(Status status);
    bool GetBootstrapTerminalStatus(Status &status) const;
    bool WaitForBootstrapRetryOrStop(std::chrono::milliseconds &baseDelay);
    bool IsBootstrapStopRequested() const;
    void WarnBootstrapRetry(const Status &status, std::chrono::steady_clock::time_point &nextWarningAt) const;

    Status StopOwnedMembership(std::unique_ptr<MembershipHandle> membership);
    void RecordPendingCleanupStatus(const Status &status);
    Status LifecycleInterruptedStatus(const char *operation) const;
    Status WaitForShutdownResultLocked(std::unique_lock<std::mutex> &lock);
    void RecordShutdownCleanupStatusLocked(uint64_t generation, const Status &status);

    CoordinatorElectionOptions options_;
    CoordinatorRaftEventCallbacks callbacks_;
    std::shared_ptr<ICoordinatorDiscovery> discovery_;
    Dependencies dependencies_;

    mutable std::mutex bootstrapMutex_;
    std::condition_variable bootstrapCv_;
    RaftBootstrapState bootstrapState_;
    Status bootstrapStatus_;
    bool bootstrapStopRequested_{ false };
    uint64_t bootstrapWakeGeneration_{ 0 };
    size_t bootstrapRetryWaiters_{ 0 };
    std::thread bootstrapThread_;

    mutable std::mutex lifecycleMutex_;
    std::condition_variable lifecycleCv_;
    LifecycleState state_{ LifecycleState::CONSTRUCTED };
    bool lifecycleOperationInProgress_{ false };
    bool membershipStartDisabled_{ false };
    bool membershipStopInProgress_{ false };
    uint64_t membershipStopGeneration_{ 0 };
    uint64_t completedMembershipStopGeneration_{ 0 };
    Status membershipStopStatus_;
    size_t membershipStopWaiters_{ 0 };
    size_t stopMembershipLifecycleWaiters_{ 0 };
    size_t stopMembershipShutdownWaiters_{ 0 };
    bool shutdownInProgress_{ false };
    bool shutdownComplete_{ false };
    uint64_t shutdownGeneration_{ 0 };
    // First cleanup error for shutdownGeneration_; remains OK when every cleanup succeeds.
    Status shutdownStatus_;
    Status pendingCleanupStatus_;
    std::unique_ptr<NodeHandle> node_;
    std::unique_ptr<MembershipHandle> membership_;
};

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_ELECTION_MANAGER_H
