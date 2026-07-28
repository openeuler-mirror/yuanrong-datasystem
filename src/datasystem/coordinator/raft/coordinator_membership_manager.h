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
    std::chrono::milliseconds operationWarningTimeout{ 0 };
    std::chrono::milliseconds candidateRetryCooldown{ 0 };

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
    enum class OperationStage : uint8_t {
        IDLE,
        ADDING_VACANCY,
        ADDING_REPLACEMENT,
        REMOVING_FAILED,
        ROLLING_BACK,
        UNCERTAIN
    };
    enum class CompletionOrigin : uint8_t { CALLBACK, SUBMISSION_REJECTED };

    using TimePoint = std::chrono::steady_clock::time_point;
    using MembershipOperationCallback = std::function<void(Status)>;
    struct Dependencies {
        std::function<Status(CoordinatorRaftMembershipStatus &)> getStatus;
        std::function<Status(const std::string &, MembershipOperationCallback)> addPeer;
        std::function<Status(const std::string &, MembershipOperationCallback)> removePeer;
    };
    using NowFunction = std::function<TimePoint()>;

    struct CompletionResult {
        uint64_t generation{ 0 };
        Status status;
        CompletionOrigin origin{ CompletionOrigin::CALLBACK };
        bool retryWakeIssued{ false };
    };

    struct CompletionMailbox {
        std::mutex mutex;
        std::condition_variable cv;
        bool stopping{ false };
        uint64_t activeGeneration{ 0 };
        bool resultPublished{ false };
        bool wakeRequested{ false };
        std::optional<CompletionResult> result;
    };

    struct SubmissionSnapshot {
        int64_t term{ 0 };
        int64_t configurationIndex{ 0 };
        std::vector<std::string> committedPeers;
    };

    struct ActiveOperation {
        uint64_t generation{ 0 };
        OperationStage stage{ OperationStage::IDLE };
        OperationStage submittedStage{ OperationStage::IDLE };
        int64_t term{ 0 };
        int64_t startingConfigurationIndex{ 0 };
        std::string candidate;
        std::string failedPeer;
        std::string targetPeer;
        std::vector<std::string> startingCommittedPeers;
        TimePoint submittedAt;
        std::optional<Status> completionStatus;
        CompletionOrigin completionOrigin{ CompletionOrigin::CALLBACK };
        bool warningEmitted{ false };
    };

    struct OwnedReplacementIntent {
        std::string candidate;
        std::string failedPeer;
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
    void CleanupPolicyState(const CoordinatorRaftMembershipStatus &status, TimePoint now);
    bool HasKnownQuorum(const CoordinatorRaftMembershipStatus &status, const HealthSummary &health) const;
    Status SelectCandidate(const CoordinatorRaftMembershipStatus &status, TimePoint now, std::string &candidate);
    bool TryAdmitDiscovery();
    void ClaimCompletion(std::optional<CompletionResult> &completion) noexcept;
    Status ConsumeCompletion(const CoordinatorRaftMembershipStatus &status, const HealthSummary &health,
                             bool hasKnownQuorum, TimePoint now);
    void ReconcileOwnedReplacementIntent(const CoordinatorRaftMembershipStatus &status);
    bool HasProvenOwnedReplacementIntent(const CoordinatorRaftMembershipStatus &status) const;
    static SubmissionSnapshot CaptureSubmissionSnapshot(const CoordinatorRaftMembershipStatus &status);
    Status RevalidateSubmissionPolicy(const SubmissionSnapshot &expected, const ActiveOperation &operation,
                                      TimePoint now);
    Status SubmitAdd(SubmissionSnapshot expected, const std::string &candidate, const std::string &failedPeer,
                     OperationStage stage, TimePoint now);
    Status SubmitFailedPeerRemoval(SubmissionSnapshot expected, const std::string &failedPeer, TimePoint now);
    Status SubmitOwnedCandidateRollback(SubmissionSnapshot expected, const OwnedReplacementIntent &intent,
                                        TimePoint now);
    Status SubmitRemoveOperation(SubmissionSnapshot expected, ActiveOperation operation);
    uint64_t NextOperationGeneration();
    void LogUnsafeOverTargetConfiguration(const CoordinatorRaftMembershipStatus &status) const;
    void ClearActiveOperation();
    bool ArmCompletionMailbox(uint64_t generation);
    void InvalidateCompletionMailbox();
    void StopCompletionMailbox();
    std::optional<CompletionResult> TakeCompletion();
    void RestoreCompletion(CompletionResult completion);
    static const char *OperationStageName(OperationStage stage) noexcept;
    static void PublishCompletion(const std::weak_ptr<CompletionMailbox> &weakMailbox, uint64_t generation,
                                  Status status, CompletionOrigin origin);

    CoordinatorMembershipOptions options_;
    Dependencies dependencies_;
    std::shared_ptr<ICoordinatorDiscovery> discovery_;
    NowFunction now_;
    std::mutex lifecycleMutex_;
    std::condition_variable lifecycleCv_;
    LifecycleState state_{ LifecycleState::CONSTRUCTED };
    std::unique_ptr<Thread> thread_;
    // Identifies the owned reconciliation thread even while thread_ is moved for join; protected by lifecycleMutex_.
    std::thread::id reconciliationThreadId_{};

    // Policy fields are owned by the reconciliation thread. Callbacks only publish to completionMailbox_.
    // lifecycleMutex_ and CompletionMailbox::mutex are never held together.
    std::optional<int64_t> observedTerm_;
    bool observingLeader_{ false };
    std::map<std::string, TimePoint> failureSince_;
    std::map<std::string, TimePoint> candidateRetryAfter_;
    TimePoint nextDiscoveryAt_;
    TimePoint nextMembershipSubmissionAt_;
    uint64_t lastOperationGeneration_{ 0 };
    std::optional<ActiveOperation> activeOperation_;
    std::optional<OwnedReplacementIntent> ownedReplacementIntent_;
    std::shared_ptr<CompletionMailbox> completionMailbox_{ std::make_shared<CompletionMailbox>() };
};

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_MEMBERSHIP_MANAGER_H
