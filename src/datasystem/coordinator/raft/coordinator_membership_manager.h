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
 * Description: Standalone Coordinator raft membership management lifecycle.
 */
#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_MEMBERSHIP_MANAGER_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_MEMBERSHIP_MANAGER_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/utils/service_discovery.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class Thread;
}

namespace datasystem::coordinator {
class CoordinatorRaftNode;
struct CoordinatorRaftMembershipStatus;

struct CoordinatorMembershipOptions {
    size_t expectedMemberCount{ 0 };
    std::chrono::milliseconds healthCheckInterval{ 0 };
    std::chrono::milliseconds memberFailureGrace{ 0 };
    std::chrono::milliseconds discoveryRetryInterval{ 0 };

    bool IsValid() const noexcept;
};

class CoordinatorMembershipManager final {
public:
    // raftNode is non-owning and must outlive this Manager's Shutdown; discovery is owned by the Manager.
    CoordinatorMembershipManager(CoordinatorMembershipOptions options, CoordinatorRaftNode &raftNode,
                                 std::shared_ptr<ICoordinatorDiscovery> discovery);
    ~CoordinatorMembershipManager() noexcept;

    CoordinatorMembershipManager(const CoordinatorMembershipManager &) = delete;
    CoordinatorMembershipManager &operator=(const CoordinatorMembershipManager &) = delete;

    Status Start();
    Status Shutdown();

private:
    enum class LifecycleState : uint8_t { CONSTRUCTED, RUNNING, STOPPING, STOPPED };
    enum class MutationKind : uint8_t { ADD_VACANCY, ADD_REPLACEMENT, REMOVE_FAILED, ROLLBACK_CANDIDATE };

    using TimePoint = std::chrono::steady_clock::time_point;
    using MembershipOperationCallback = std::function<void(Status)>;
    struct Dependencies {
        std::function<bool()> hasInFlightMembershipOperation;
        std::function<Status(CoordinatorRaftMembershipStatus &)> getStatus;
        std::function<Status(const std::string &, MembershipOperationCallback)> addPeer;
        std::function<Status(const std::string &, MembershipOperationCallback)> removePeer;
    };
    using NowFunction = std::function<TimePoint()>;

    struct SubmissionSnapshot {
        int64_t term{ 0 };
        int64_t configurationIndex{ 0 };
        std::vector<std::string> committedPeers;
    };

    struct ReplacementIntent {
        std::string failedPeer;
        std::vector<std::string> originalPeers;
        std::set<std::string> attemptedCandidates;
    };

    struct HealthSummary {
        size_t knownHealthyMembers{ 0 };
        std::set<std::string> explicitlyHealthyPeers;
        std::vector<std::string> confirmedFailedPeers;
    };

    CoordinatorMembershipManager(CoordinatorMembershipOptions options, Dependencies dependencies,
                                 std::shared_ptr<ICoordinatorDiscovery> discovery, NowFunction now);

    void Run();
    Status ReconcileOnce();
    bool RefreshLeaderObservation(const CoordinatorRaftMembershipStatus &status);
    HealthSummary RefreshFollowerHealth(const CoordinatorRaftMembershipStatus &status, TimePoint now);
    void CleanupPolicyState(const CoordinatorRaftMembershipStatus &status);
    bool HasKnownQuorum(const CoordinatorRaftMembershipStatus &status, const HealthSummary &health) const;
    bool TryAdmitDiscovery();
    Status SelectCandidate(const CoordinatorRaftMembershipStatus &status, TimePoint now, std::string &candidate);
    void ReconcileReplacementIntent(const CoordinatorRaftMembershipStatus &status, const HealthSummary &health);
    std::optional<std::string> FindCommittedReplacementCandidate(const CoordinatorRaftMembershipStatus &status) const;
    static SubmissionSnapshot CaptureSubmissionSnapshot(const CoordinatorRaftMembershipStatus &status);
    static const char *MutationKindName(MutationKind kind) noexcept;
    Status RevalidateSubmissionPolicy(const SubmissionSnapshot &expected, MutationKind kind,
                                      const std::string &targetPeer, const std::string &failedPeer, TimePoint now);
    Status SubmitAdd(const SubmissionSnapshot &expected, const std::string &candidate, const std::string &failedPeer,
                     MutationKind kind, TimePoint now);
    Status SubmitRemove(const SubmissionSnapshot &expected, const std::string &targetPeer,
                        const std::string &failedPeer, MutationKind kind, TimePoint now);
    void LogUnsafeOverTargetConfiguration(const CoordinatorRaftMembershipStatus &status) const;
    static void LogMembershipOperationCompletion(const char *operation, const Status &status);

    CoordinatorMembershipOptions options_;
    Dependencies dependencies_;
    std::shared_ptr<ICoordinatorDiscovery> discovery_;
    NowFunction now_;
    std::string traceId_;
    std::mutex lifecycleMutex_;
    std::condition_variable lifecycleCv_;
    LifecycleState state_{ LifecycleState::CONSTRUCTED };
    std::unique_ptr<Thread> thread_;
    // Identifies the owned reconciliation thread even while thread_ is moved for join; protected by lifecycleMutex_.
    std::thread::id reconciliationThreadId_{};

    // Policy fields are owned exclusively by the reconciliation thread.
    std::optional<int64_t> observedTerm_;
    bool observingLeader_{ false };
    std::map<std::string, TimePoint> failureSince_;
    std::map<std::string, TimePoint> candidateLastAttemptAt_;
    TimePoint nextDiscoveryAt_;
    TimePoint nextMembershipSubmissionAt_;
    std::optional<ReplacementIntent> replacementIntent_;
};

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_MEMBERSHIP_MANAGER_H
