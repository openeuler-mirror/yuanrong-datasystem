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
#include "datasystem/coordinator/raft/coordinator_membership_manager.h"

#include <algorithm>
#include <exception>
#include <set>
#include <type_traits>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#include "datasystem/coordinator/raft/coordinator_raft_peer.h"

namespace datasystem::coordinator {
namespace {
constexpr char kCoordinatorMembershipThreadName[] = "coord-member";
constexpr char kReconcileExceptionMarker[] = "Coordinator membership reconciliation exception";
constexpr int kReconcileErrorLogIntervalSeconds = 10;
constexpr uint32_t kPolicyDiagnosticLogEveryCount = 100;
constexpr char kInvalidCandidateMarker[] = "Coordinator membership discovery returned an invalid candidate";
constexpr char kDiscoveryErrorMarker[] = "Coordinator membership discovery failed";
constexpr char kOperationWarningMarker[] = "Coordinator membership operation remains uncertain";
constexpr char kAddSubmissionExceptionMarker[] = "Coordinator membership AddPeer submission exception";
constexpr char kRemoveSubmissionExceptionMarker[] = "Coordinator membership RemovePeer submission exception";
constexpr char kUnsafeOverTargetMarker[] =
    "Coordinator membership over-target configuration has no safe removal target";

const char *InvalidCoordinatorMembershipOptionsReason(const CoordinatorMembershipOptions &options) noexcept
{
    const auto zero = std::chrono::milliseconds::zero();
    if (options.expectedMemberCount == 0) {
        return "Coordinator membership expectedMemberCount must be positive";
    }
    if (options.healthCheckInterval <= zero) {
        return "Coordinator membership healthCheckInterval must be positive";
    }
    if (options.memberFailureGrace <= zero) {
        return "Coordinator membership memberFailureGrace must be positive";
    }
    if (options.discoveryRetryInterval <= zero) {
        return "Coordinator membership discoveryRetryInterval must be positive";
    }
    if (options.operationWarningTimeout <= zero) {
        return "Coordinator membership operationWarningTimeout must be positive";
    }
    if (options.candidateRetryCooldown <= zero) {
        return "Coordinator membership candidateRetryCooldown must be positive";
    }
    if (options.healthCheckInterval >= options.memberFailureGrace) {
        return "Coordinator membership healthCheckInterval must be less than memberFailureGrace";
    }
    if (options.healthCheckInterval > options.discoveryRetryInterval) {
        return "Coordinator membership healthCheckInterval must not exceed discoveryRetryInterval";
    }
    return nullptr;
}

}  // namespace

bool CoordinatorMembershipOptions::IsValid() const noexcept
{
    return InvalidCoordinatorMembershipOptionsReason(*this) == nullptr;
}

CoordinatorMembershipManager::CoordinatorMembershipManager(CoordinatorMembershipOptions options,
                                                           CoordinatorRaftNode &raftNode,
                                                           std::shared_ptr<ICoordinatorDiscovery> discovery)
    : CoordinatorMembershipManager(
          options,
          Dependencies{
              [&raftNode](CoordinatorRaftMembershipStatus &status) { return raftNode.GetMembershipStatus(status); },
              [&raftNode](const std::string &peer, MembershipOperationCallback callback) {
                  return raftNode.AddPeer(peer, std::move(callback));
              },
              [&raftNode](const std::string &peer, MembershipOperationCallback callback) {
                  return raftNode.RemovePeer(peer, std::move(callback));
              } },
          std::move(discovery), [] { return std::chrono::steady_clock::now(); })
{
}

CoordinatorMembershipManager::CoordinatorMembershipManager(CoordinatorMembershipOptions options,
                                                           Dependencies dependencies,
                                                           std::shared_ptr<ICoordinatorDiscovery> discovery,
                                                           NowFunction now)
    : options_(options), dependencies_(std::move(dependencies)), discovery_(std::move(discovery)), now_(std::move(now))
{
}

CoordinatorMembershipManager::~CoordinatorMembershipManager() noexcept
{
    LOG_IF_ERROR(Shutdown(), "Shutdown Coordinator membership manager failed");
}

Status CoordinatorMembershipManager::Start()
{
    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    if (state_ != LifecycleState::CONSTRUCTED) {
        return Status(K_INVALID, "Coordinator membership manager can only be started once");
    }
    if (const auto *invalidReason = InvalidCoordinatorMembershipOptionsReason(options_); invalidReason != nullptr) {
        return Status(K_INVALID, invalidReason);
    }
    if (discovery_ == nullptr) {
        return Status(K_INVALID, "Coordinator membership manager discovery must not be null");
    }

    state_ = LifecycleState::RUNNING;
    try {
        thread_ = std::make_unique<Thread>(&CoordinatorMembershipManager::Run, this);
        reconciliationThreadId_ = thread_->get_id();
        thread_->set_name(kCoordinatorMembershipThreadName);
    } catch (const std::exception &error) {
        auto failedThread = std::move(thread_);
        state_ = failedThread == nullptr ? LifecycleState::STOPPED : LifecycleState::STOPPING;
        lock.unlock();
        StopCompletionMailbox();
        lifecycleCv_.notify_all();
        if (failedThread != nullptr && failedThread->joinable()) {
            failedThread->join();
        }
        lock.lock();
        state_ = LifecycleState::STOPPED;
        reconciliationThreadId_ = {};
        lifecycleCv_.notify_all();
        return Status(K_RUNTIME_ERROR,
                      std::string("Failed to start Coordinator membership manager thread: ") + error.what());
    } catch (...) {
        auto failedThread = std::move(thread_);
        state_ = failedThread == nullptr ? LifecycleState::STOPPED : LifecycleState::STOPPING;
        lock.unlock();
        StopCompletionMailbox();
        lifecycleCv_.notify_all();
        if (failedThread != nullptr && failedThread->joinable()) {
            failedThread->join();
        }
        lock.lock();
        state_ = LifecycleState::STOPPED;
        reconciliationThreadId_ = {};
        lifecycleCv_.notify_all();
        return Status(K_RUNTIME_ERROR, "Failed to start Coordinator membership manager thread");
    }
    return Status::OK();
}

Status CoordinatorMembershipManager::Shutdown()
{
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        if (reconciliationThreadId_ == std::this_thread::get_id()) {
            return Status(K_INVALID, "Coordinator membership manager Shutdown cannot run on its reconciliation thread");
        }
    }

    StopCompletionMailbox();

    std::unique_ptr<Thread> threadToJoin;
    bool stopConstructedManager = false;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        if (state_ == LifecycleState::STOPPING) {
            lifecycleCv_.wait(lock, [this] { return state_ == LifecycleState::STOPPED; });
            return Status::OK();
        }
        if (state_ == LifecycleState::STOPPED) {
            return Status::OK();
        }
        if (state_ == LifecycleState::CONSTRUCTED) {
            state_ = LifecycleState::STOPPED;
            stopConstructedManager = true;
        } else {
            state_ = LifecycleState::STOPPING;
            threadToJoin = std::move(thread_);
        }
    }

    lifecycleCv_.notify_all();
    if (stopConstructedManager) {
        activeOperation_.reset();
        ownedReplacementIntent_.reset();
        return Status::OK();
    }

    if (threadToJoin != nullptr && threadToJoin->joinable()) {
        threadToJoin->join();
    }
    activeOperation_.reset();
    ownedReplacementIntent_.reset();

    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        state_ = LifecycleState::STOPPED;
        reconciliationThreadId_ = {};
    }
    lifecycleCv_.notify_all();
    return Status::OK();
}

void CoordinatorMembershipManager::Run()
{
    while (true) {
        {
            std::lock_guard<std::mutex> lock(lifecycleMutex_);
            if (state_ != LifecycleState::RUNNING) {
                return;
            }
        }

        try {
            const auto status = ReconcileOnce();
            if (status.IsError()) {
                LOG_EVERY_T(WARNING, kReconcileErrorLogIntervalSeconds)
                    << "Coordinator membership status reconciliation failed: " << status;
            }
        } catch (const std::exception &e) {
            LOG(ERROR) << kReconcileExceptionMarker << ": " << e.what();
        } catch (...) {
            LOG(ERROR) << kReconcileExceptionMarker;
        }

        std::unique_lock<std::mutex> lock(completionMailbox_->mutex);
        if (!completionMailbox_->wakeRequested) {
            completionMailbox_->cv.wait_for(lock, options_.healthCheckInterval, [this] {
                return completionMailbox_->stopping || completionMailbox_->wakeRequested;
            });
        }
        if (completionMailbox_->stopping) {
            return;
        }
        completionMailbox_->wakeRequested = false;
    }
}

Status CoordinatorMembershipManager::ReconcileOnce()
{
    auto completion = TakeCompletion();
    CoordinatorRaftMembershipStatus status;
    Status getStatusResult;
    try {
        getStatusResult = dependencies_.getStatus(status);
    } catch (...) {
        if (completion.has_value()) {
            RestoreCompletion(std::move(*completion));
        }
        throw;
    }
    if (getStatusResult.IsError()) {
        if (completion.has_value()) {
            RestoreCompletion(std::move(*completion));
        }
        return getStatusResult;
    }
    ClaimCompletion(completion);
    const auto now = now_();

    if (!RefreshLeaderObservation(status)) {
        return Status::OK();
    }

    CleanupPolicyState(status, now);
    const auto health = RefreshFollowerHealth(status, now);
    const bool hasKnownQuorum = HasKnownQuorum(status, health);
    if (activeOperation_.has_value()) {
        RETURN_IF_NOT_OK(ConsumeCompletion(status, health, hasKnownQuorum, now));
        return Status::OK();
    }

    ReconcileOwnedReplacementIntent(status);
    if (!hasKnownQuorum) {
        return Status::OK();
    }

    const auto committedCount = status.committedPeers.size();
    if (committedCount < options_.expectedMemberCount) {
        if (now < nextDiscoveryAt_) {
            return Status::OK();
        }
        std::string candidate;
        RETURN_IF_NOT_OK(SelectCandidate(status, now, candidate));
        if (!candidate.empty()) {
            RETURN_IF_NOT_OK(
                SubmitAdd(CaptureSubmissionSnapshot(status), candidate, {}, OperationStage::ADDING_VACANCY, now));
        }
        return Status::OK();
    }

    if (committedCount > options_.expectedMemberCount) {
        if (!health.confirmedFailedPeers.empty()) {
            if (now >= nextMembershipSubmissionAt_) {
                RETURN_IF_NOT_OK(SubmitFailedPeerRemoval(CaptureSubmissionSnapshot(status),
                                                         health.confirmedFailedPeers.front(), now));
            }
            return Status::OK();
        }
        if (ownedReplacementIntent_.has_value()
            && health.explicitlyHealthyPeers.count(ownedReplacementIntent_->failedPeer) != 0) {
            if (now >= nextMembershipSubmissionAt_) {
                RETURN_IF_NOT_OK(
                    SubmitOwnedCandidateRollback(CaptureSubmissionSnapshot(status), *ownedReplacementIntent_, now));
            }
            return Status::OK();
        }
        LogUnsafeOverTargetConfiguration(status);
        return Status::OK();
    }

    if (health.confirmedFailedPeers.empty() || now < nextDiscoveryAt_) {
        return Status::OK();
    }
    std::string candidate;
    RETURN_IF_NOT_OK(SelectCandidate(status, now, candidate));
    if (!candidate.empty()) {
        RETURN_IF_NOT_OK(SubmitAdd(CaptureSubmissionSnapshot(status), candidate, health.confirmedFailedPeers.front(),
                                   OperationStage::ADDING_REPLACEMENT, now));
    }
    return Status::OK();
}

bool CoordinatorMembershipManager::RefreshLeaderObservation(const CoordinatorRaftMembershipStatus &status)
{
    if (!status.isLeader) {
        observingLeader_ = false;
        observedTerm_ = status.term;
        failureSince_.clear();
        nextDiscoveryAt_ = {};
        ReconcileOwnedReplacementIntent(status);
        ClearActiveOperation();
        return false;
    }

    if (!observingLeader_ || !observedTerm_.has_value() || *observedTerm_ != status.term) {
        observingLeader_ = true;
        observedTerm_ = status.term;
        failureSince_.clear();
        nextDiscoveryAt_ = {};
        ReconcileOwnedReplacementIntent(status);
        ClearActiveOperation();
    }
    return true;
}

CoordinatorMembershipManager::HealthSummary CoordinatorMembershipManager::RefreshFollowerHealth(
    const CoordinatorRaftMembershipStatus &status, TimePoint now)
{
    HealthSummary summary;
    if (!status.committedPeers.empty()) {
        summary.knownHealthyMembers = 1;
    }

    const std::set<std::string> committedPeers(status.committedPeers.begin(), status.committedPeers.end());
    std::set<std::string> observedFollowers;
    std::set<std::string> suspectedFollowers;
    for (const auto &follower : status.stableFollowers) {
        if (committedPeers.count(follower.peer) == 0 || !observedFollowers.emplace(follower.peer).second) {
            continue;
        }
        if (follower.consecutiveErrorTimes > kCoordinatorFollowerFailureErrorThreshold) {
            suspectedFollowers.emplace(follower.peer);
            const auto [failureIt, inserted] = failureSince_.try_emplace(follower.peer, now);
            if (!inserted && now - failureIt->second >= options_.memberFailureGrace) {
                summary.confirmedFailedPeers.emplace_back(follower.peer);
            }
            continue;
        }

        failureSince_.erase(follower.peer);
        if (follower.valid) {
            ++summary.knownHealthyMembers;
            summary.explicitlyHealthyPeers.emplace(follower.peer);
        }
    }

    for (auto it = failureSince_.begin(); it != failureSince_.end();) {
        if (suspectedFollowers.count(it->first) == 0) {
            it = failureSince_.erase(it);
        } else {
            ++it;
        }
    }
    std::sort(summary.confirmedFailedPeers.begin(), summary.confirmedFailedPeers.end(),
              [this](const std::string &lhs, const std::string &rhs) {
                  const auto lhsSince = failureSince_.at(lhs);
                  const auto rhsSince = failureSince_.at(rhs);
                  return lhsSince == rhsSince ? lhs < rhs : lhsSince < rhsSince;
              });
    return summary;
}

void CoordinatorMembershipManager::CleanupPolicyState(const CoordinatorRaftMembershipStatus &status, TimePoint now)
{
    const std::set<std::string> committedPeers(status.committedPeers.begin(), status.committedPeers.end());
    for (auto it = failureSince_.begin(); it != failureSince_.end();) {
        if (committedPeers.count(it->first) == 0) {
            it = failureSince_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = candidateRetryAfter_.begin(); it != candidateRetryAfter_.end();) {
        if (it->second <= now || committedPeers.count(it->first) != 0) {
            it = candidateRetryAfter_.erase(it);
        } else {
            ++it;
        }
    }
}

bool CoordinatorMembershipManager::HasKnownQuorum(const CoordinatorRaftMembershipStatus &status,
                                                  const HealthSummary &health) const
{
    const size_t requiredQuorum = (status.committedPeers.size() / 2) + 1;
    return health.knownHealthyMembers >= requiredQuorum;
}

Status CoordinatorMembershipManager::SelectCandidate(const CoordinatorRaftMembershipStatus &status, TimePoint now,
                                                     std::string &candidate)
{
    candidate.clear();
    nextDiscoveryAt_ = now + options_.discoveryRetryInterval;

    std::vector<std::string> discoveredCandidates;
    if (!TryAdmitDiscovery()) {
        return Status::OK();
    }
    const auto discoveryStatus = discovery_->GetCoordinators(discoveredCandidates);
    if (discoveryStatus.IsError()) {
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kDiscoveryErrorMarker;
        return Status::OK();
    }

    std::vector<std::string> normalizedCandidates;
    normalizedCandidates.reserve(discoveredCandidates.size());
    for (const auto &discoveredCandidate : discoveredCandidates) {
        braft::PeerId peer;
        if (ParseCoordinatorRaftPeer(discoveredCandidate, peer).IsError()) {
            LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kInvalidCandidateMarker;
            continue;
        }
        auto normalizedCandidate = CoordinatorRaftPeerAddress(peer);
        if (normalizedCandidate.empty()) {
            LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kInvalidCandidateMarker;
            continue;
        }
        normalizedCandidates.emplace_back(std::move(normalizedCandidate));
    }
    std::sort(normalizedCandidates.begin(), normalizedCandidates.end());
    normalizedCandidates.erase(std::unique(normalizedCandidates.begin(), normalizedCandidates.end()),
                               normalizedCandidates.end());

    const std::set<std::string> committedPeers(status.committedPeers.begin(), status.committedPeers.end());
    for (const auto &normalizedCandidate : normalizedCandidates) {
        if (committedPeers.count(normalizedCandidate) != 0
            || (activeOperation_.has_value() && activeOperation_->candidate == normalizedCandidate)) {
            continue;
        }
        const auto cooldownIt = candidateRetryAfter_.find(normalizedCandidate);
        if (cooldownIt != candidateRetryAfter_.end() && cooldownIt->second > now) {
            continue;
        }
        candidate = normalizedCandidate;
        break;
    }
    return Status::OK();
}

bool CoordinatorMembershipManager::TryAdmitDiscovery()
{
    std::lock_guard<std::mutex> lock(completionMailbox_->mutex);
    return !completionMailbox_->stopping;
}

void CoordinatorMembershipManager::ClaimCompletion(std::optional<CompletionResult> &completion) noexcept
{
    if (!completion.has_value() || !activeOperation_.has_value()
        || completion->generation != activeOperation_->generation) {
        return;
    }
    if (completion->origin == CompletionOrigin::SUBMISSION_REJECTED
        && activeOperation_->submittedStage == OperationStage::ADDING_REPLACEMENT) {
        ownedReplacementIntent_.reset();
    }
    activeOperation_->completionOrigin = completion->origin;
    activeOperation_->completionStatus.emplace(std::move(completion->status));
    completion.reset();
}

void CoordinatorMembershipManager::ReconcileOwnedReplacementIntent(const CoordinatorRaftMembershipStatus &status)
{
    if (ownedReplacementIntent_.has_value() && !HasProvenOwnedReplacementIntent(status)) {
        ownedReplacementIntent_.reset();
    }
}

bool CoordinatorMembershipManager::HasProvenOwnedReplacementIntent(const CoordinatorRaftMembershipStatus &status) const
{
    if (!ownedReplacementIntent_.has_value() || status.committedPeers.size() <= options_.expectedMemberCount) {
        return false;
    }
    const auto containsPeer = [&status](const std::string &peer) {
        return std::find(status.committedPeers.begin(), status.committedPeers.end(), peer)
               != status.committedPeers.end();
    };
    return containsPeer(ownedReplacementIntent_->candidate) && containsPeer(ownedReplacementIntent_->failedPeer);
}

Status CoordinatorMembershipManager::ConsumeCompletion(const CoordinatorRaftMembershipStatus &status,
                                                       const HealthSummary &health, bool hasKnownQuorum, TimePoint now)
{
    auto &operation = *activeOperation_;
    const bool configurationAdvanced = status.configurationIndex > operation.startingConfigurationIndex;
    const auto containsPeer = [&status](const std::string &peer) {
        return std::find(status.committedPeers.begin(), status.committedPeers.end(), peer)
               != status.committedPeers.end();
    };
    const bool operationFailed = operation.completionStatus.has_value() && operation.completionStatus->IsError();
    const bool submissionRejected =
        operationFailed && operation.completionOrigin == CompletionOrigin::SUBMISSION_REJECTED;
    const bool committedConfigurationUnchanged = status.committedPeers == operation.startingCommittedPeers;
    if (operation.submittedStage == OperationStage::REMOVING_FAILED
        || operation.submittedStage == OperationStage::ROLLING_BACK
        || (operation.submittedStage == OperationStage::ADDING_REPLACEMENT && configurationAdvanced
            && !containsPeer(operation.failedPeer))) {
        ReconcileOwnedReplacementIntent(status);
    }

    switch (operation.submittedStage) {
        case OperationStage::ADDING_VACANCY:
            if (submissionRejected) {
                candidateRetryAfter_[operation.candidate] = now + options_.candidateRetryCooldown;
                nextDiscoveryAt_ = now + options_.discoveryRetryInterval;
                ClearActiveOperation();
                return Status::OK();
            }
            if (configurationAdvanced && containsPeer(operation.candidate)) {
                ClearActiveOperation();
                return Status::OK();
            }
            if (operationFailed && committedConfigurationUnchanged) {
                candidateRetryAfter_[operation.candidate] = now + options_.candidateRetryCooldown;
                nextDiscoveryAt_ = now + options_.discoveryRetryInterval;
                ClearActiveOperation();
                return Status::OK();
            }
            break;
        case OperationStage::ADDING_REPLACEMENT:
            if (submissionRejected) {
                candidateRetryAfter_[operation.candidate] = now + options_.candidateRetryCooldown;
                nextDiscoveryAt_ = now + options_.discoveryRetryInterval;
                ownedReplacementIntent_.reset();
                ClearActiveOperation();
                return Status::OK();
            }
            if (configurationAdvanced && containsPeer(operation.candidate) && !containsPeer(operation.failedPeer)
                && status.committedPeers.size() == options_.expectedMemberCount) {
                ownedReplacementIntent_.reset();
                ClearActiveOperation();
                return Status::OK();
            }
            if (configurationAdvanced && containsPeer(operation.candidate)
                && status.committedPeers.size() == options_.expectedMemberCount + 1 && hasKnownQuorum
                && now >= nextMembershipSubmissionAt_) {
                const bool failedPeerStillConfirmed = std::find(health.confirmedFailedPeers.begin(),
                                                                health.confirmedFailedPeers.end(), operation.failedPeer)
                                                      != health.confirmedFailedPeers.end();
                if (failedPeerStillConfirmed) {
                    const auto failedPeer = operation.failedPeer;
                    return SubmitFailedPeerRemoval(CaptureSubmissionSnapshot(status), failedPeer, now);
                }
                if (health.explicitlyHealthyPeers.count(operation.failedPeer) != 0
                    && ownedReplacementIntent_.has_value()) {
                    const auto intent = *ownedReplacementIntent_;
                    return SubmitOwnedCandidateRollback(CaptureSubmissionSnapshot(status), intent, now);
                }
            }
            if (operationFailed && committedConfigurationUnchanged) {
                candidateRetryAfter_[operation.candidate] = now + options_.candidateRetryCooldown;
                nextDiscoveryAt_ = now + options_.discoveryRetryInterval;
                ownedReplacementIntent_.reset();
                ClearActiveOperation();
                return Status::OK();
            }
            break;
        case OperationStage::REMOVING_FAILED:
            if (configurationAdvanced && !operation.startingCommittedPeers.empty()
                && !containsPeer(operation.targetPeer)
                && status.committedPeers.size() == operation.startingCommittedPeers.size() - 1) {
                if (ownedReplacementIntent_.has_value()
                    && operation.targetPeer == ownedReplacementIntent_->failedPeer) {
                    ownedReplacementIntent_.reset();
                }
                ClearActiveOperation();
                ReconcileOwnedReplacementIntent(status);
                return Status::OK();
            }
            if (submissionRejected) {
                nextMembershipSubmissionAt_ = now + options_.discoveryRetryInterval;
                ClearActiveOperation();
                ReconcileOwnedReplacementIntent(status);
                return Status::OK();
            }
            break;
        case OperationStage::ROLLING_BACK:
            if (configurationAdvanced && !operation.startingCommittedPeers.empty()
                && !containsPeer(operation.targetPeer)
                && status.committedPeers.size() == operation.startingCommittedPeers.size() - 1) {
                ownedReplacementIntent_.reset();
                ClearActiveOperation();
                return Status::OK();
            }
            if (submissionRejected) {
                nextMembershipSubmissionAt_ = now + options_.discoveryRetryInterval;
                ClearActiveOperation();
                ReconcileOwnedReplacementIntent(status);
                return Status::OK();
            }
            break;
        case OperationStage::IDLE:
        case OperationStage::UNCERTAIN:
            break;
    }

    if (!operation.warningEmitted && now - operation.submittedAt >= options_.operationWarningTimeout) {
        operation.warningEmitted = true;
        LOG(WARNING) << kOperationWarningMarker << ", group=" << kCoordinatorRaftGroupId
                     << ", generation=" << operation.generation
                     << ", stage=" << OperationStageName(operation.submittedStage)
                     << ", candidate=" << (operation.candidate.empty() ? "none" : operation.candidate)
                     << ", failed_peer=" << (operation.failedPeer.empty() ? "none" : operation.failedPeer)
                     << ", target_peer=" << (operation.targetPeer.empty() ? "none" : operation.targetPeer)
                     << ", term=" << operation.term
                     << ", starting_configuration_index=" << operation.startingConfigurationIndex;
        operation.stage = OperationStage::UNCERTAIN;
    }
    return Status::OK();
}

CoordinatorMembershipManager::SubmissionSnapshot CoordinatorMembershipManager::CaptureSubmissionSnapshot(
    const CoordinatorRaftMembershipStatus &status)
{
    return SubmissionSnapshot{ status.term, status.configurationIndex, status.committedPeers };
}

Status CoordinatorMembershipManager::RevalidateSubmissionPolicy(const SubmissionSnapshot &expected,
                                                                const ActiveOperation &operation, TimePoint now)
{
    CoordinatorRaftMembershipStatus current;
    RETURN_IF_NOT_OK(dependencies_.getStatus(current));
    const bool snapshotMatches = current.isLeader && current.term == expected.term
                                 && current.configurationIndex == expected.configurationIndex
                                 && current.committedPeers == expected.committedPeers;
    if (!snapshotMatches) {
        ClearActiveOperation();
        ReconcileOwnedReplacementIntent(current);
        return Status(K_TRY_AGAIN, "Coordinator membership submission decision is stale for group "
                                       + std::string(kCoordinatorRaftGroupId)
                                       + ", expected={leader=true, term=" + std::to_string(expected.term)
                                       + ", configuration_index=" + std::to_string(expected.configurationIndex)
                                       + ", committed_peers=[" + VectorToString(expected.committedPeers)
                                       + "]}, current={leader=" + (current.isLeader ? "true" : "false")
                                       + ", term=" + std::to_string(current.term)
                                       + ", configuration_index=" + std::to_string(current.configurationIndex)
                                       + ", committed_peers=[" + VectorToString(current.committedPeers) + "]}");
    }

    CleanupPolicyState(current, now);
    const auto health = RefreshFollowerHealth(current, now);
    const bool hasKnownQuorum = HasKnownQuorum(current, health);
    const auto containsCommittedPeer = [&current](const std::string &peer) {
        return std::find(current.committedPeers.begin(), current.committedPeers.end(), peer)
               != current.committedPeers.end();
    };
    const auto isConfirmedFailed = [&health](const std::string &peer) {
        return std::find(health.confirmedFailedPeers.begin(), health.confirmedFailedPeers.end(), peer)
               != health.confirmedFailedPeers.end();
    };

    bool stageConditionHolds = false;
    switch (operation.submittedStage) {
        case OperationStage::ADDING_VACANCY:
            stageConditionHolds = current.committedPeers.size() < options_.expectedMemberCount;
            break;
        case OperationStage::ADDING_REPLACEMENT:
            stageConditionHolds = current.committedPeers.size() == options_.expectedMemberCount
                                  && isConfirmedFailed(operation.failedPeer);
            break;
        case OperationStage::REMOVING_FAILED:
            stageConditionHolds = current.committedPeers.size() > options_.expectedMemberCount
                                  && containsCommittedPeer(operation.targetPeer)
                                  && isConfirmedFailed(operation.targetPeer);
            break;
        case OperationStage::ROLLING_BACK:
            stageConditionHolds =
                current.committedPeers.size() > options_.expectedMemberCount
                && containsCommittedPeer(operation.candidate) && containsCommittedPeer(operation.failedPeer)
                && health.explicitlyHealthyPeers.count(operation.failedPeer) != 0 && ownedReplacementIntent_.has_value()
                && ownedReplacementIntent_->candidate == operation.candidate
                && ownedReplacementIntent_->failedPeer == operation.failedPeer;
            break;
        case OperationStage::IDLE:
        case OperationStage::UNCERTAIN:
            break;
    }
    if (hasKnownQuorum && stageConditionHolds) {
        return Status::OK();
    }

    ClearActiveOperation();
    ReconcileOwnedReplacementIntent(current);
    return Status(K_TRY_AGAIN, "Coordinator membership submission policy changed for group "
                                   + std::string(kCoordinatorRaftGroupId)
                                   + ", stage=" + OperationStageName(operation.submittedStage)
                                   + ", known_healthy_members=" + std::to_string(health.knownHealthyMembers)
                                   + ", committed_size=" + std::to_string(current.committedPeers.size())
                                   + ", confirmed_failed_peers=[" + VectorToString(health.confirmedFailedPeers) + "]");
}

Status CoordinatorMembershipManager::SubmitAdd(SubmissionSnapshot expected, const std::string &candidate,
                                               const std::string &failedPeer, OperationStage stage, TimePoint now)
{
    uint64_t generation = lastOperationGeneration_ + 1;
    if (generation == 0) {
        generation = 1;
    }
    std::optional<ActiveOperation> newActiveOperation{ ActiveOperation{ generation,
                                                                        stage,
                                                                        stage,
                                                                        expected.term,
                                                                        expected.configurationIndex,
                                                                        candidate,
                                                                        failedPeer,
                                                                        {},
                                                                        expected.committedPeers,
                                                                        now,
                                                                        std::nullopt,
                                                                        CompletionOrigin::CALLBACK,
                                                                        false } };
    const bool ownsReplacement = stage == OperationStage::ADDING_REPLACEMENT;
    std::optional<OwnedReplacementIntent> newOwnedReplacementIntent;
    if (ownsReplacement) {
        newOwnedReplacementIntent.emplace(OwnedReplacementIntent{ candidate, failedPeer });
    }

    static_assert(std::is_nothrow_move_constructible_v<ActiveOperation>);
    static_assert(std::is_nothrow_move_constructible_v<decltype(newActiveOperation)>);
    static_assert(std::is_nothrow_move_assignable_v<decltype(activeOperation_)>);
    static_assert(std::is_nothrow_move_constructible_v<OwnedReplacementIntent>);
    static_assert(std::is_nothrow_move_constructible_v<decltype(newOwnedReplacementIntent)>);
    static_assert(std::is_nothrow_move_assignable_v<decltype(ownedReplacementIntent_)>);

    RETURN_IF_NOT_OK(RevalidateSubmissionPolicy(expected, *newActiveOperation, now));
    lastOperationGeneration_ = generation;
    activeOperation_ = std::move(newActiveOperation);
    if (ownsReplacement) {
        ownedReplacementIntent_ = std::move(newOwnedReplacementIntent);
    }
    if (!ArmCompletionMailbox(generation)) {
        activeOperation_.reset();
        if (ownsReplacement) {
            ownedReplacementIntent_.reset();
        }
        return Status::OK();
    }

    const std::weak_ptr<CompletionMailbox> weakMailbox = completionMailbox_;
    Status submissionStatus;
    try {
        submissionStatus = dependencies_.addPeer(candidate, [weakMailbox, generation](Status completionStatus) {
            PublishCompletion(weakMailbox, generation, std::move(completionStatus), CompletionOrigin::CALLBACK);
        });
    } catch (const std::exception &e) {
        const auto error = FormatString("%s: %s", kAddSubmissionExceptionMarker, e.what());
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << error;
        submissionStatus = Status(K_RUNTIME_ERROR, error);
    } catch (...) {
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kAddSubmissionExceptionMarker;
        submissionStatus = Status(K_RUNTIME_ERROR, kAddSubmissionExceptionMarker);
    }
    if (submissionStatus.IsError()) {
        PublishCompletion(weakMailbox, generation, std::move(submissionStatus), CompletionOrigin::SUBMISSION_REJECTED);
    }
    return Status::OK();
}

Status CoordinatorMembershipManager::SubmitFailedPeerRemoval(SubmissionSnapshot expected, const std::string &failedPeer,
                                                             TimePoint now)
{
    std::string ownedCandidate;
    if (ownedReplacementIntent_.has_value() && ownedReplacementIntent_->failedPeer == failedPeer) {
        ownedCandidate = ownedReplacementIntent_->candidate;
    }
    return SubmitRemoveOperation(
        expected, ActiveOperation{ 0, OperationStage::REMOVING_FAILED, OperationStage::REMOVING_FAILED, expected.term,
                                   expected.configurationIndex, std::move(ownedCandidate), failedPeer, failedPeer,
                                   expected.committedPeers, now, std::nullopt, CompletionOrigin::CALLBACK, false });
}

Status CoordinatorMembershipManager::SubmitOwnedCandidateRollback(SubmissionSnapshot expected,
                                                                  const OwnedReplacementIntent &intent, TimePoint now)
{
    return SubmitRemoveOperation(
        expected, ActiveOperation{ 0, OperationStage::ROLLING_BACK, OperationStage::ROLLING_BACK, expected.term,
                                   expected.configurationIndex, intent.candidate, intent.failedPeer, intent.candidate,
                                   expected.committedPeers, now, std::nullopt, CompletionOrigin::CALLBACK, false });
}

Status CoordinatorMembershipManager::SubmitRemoveOperation(SubmissionSnapshot expected, ActiveOperation operation)
{
    RETURN_IF_NOT_OK(RevalidateSubmissionPolicy(expected, operation, operation.submittedAt));
    operation.generation = NextOperationGeneration();
    const auto generation = operation.generation;
    const auto targetPeer = operation.targetPeer;
    activeOperation_ = std::move(operation);
    if (!ArmCompletionMailbox(generation)) {
        activeOperation_.reset();
        return Status::OK();
    }

    const std::weak_ptr<CompletionMailbox> weakMailbox = completionMailbox_;
    Status submissionStatus;
    try {
        submissionStatus = dependencies_.removePeer(targetPeer, [weakMailbox, generation](Status completionStatus) {
            PublishCompletion(weakMailbox, generation, std::move(completionStatus), CompletionOrigin::CALLBACK);
        });
    } catch (const std::exception &e) {
        const auto error = FormatString("%s: %s", kRemoveSubmissionExceptionMarker, e.what());
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << error;
        submissionStatus = Status(K_RUNTIME_ERROR, error);
    } catch (...) {
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kRemoveSubmissionExceptionMarker;
        submissionStatus = Status(K_RUNTIME_ERROR, kRemoveSubmissionExceptionMarker);
    }
    if (submissionStatus.IsError()) {
        PublishCompletion(weakMailbox, generation, std::move(submissionStatus), CompletionOrigin::SUBMISSION_REJECTED);
    }
    return Status::OK();
}

uint64_t CoordinatorMembershipManager::NextOperationGeneration()
{
    ++lastOperationGeneration_;
    if (lastOperationGeneration_ == 0) {
        ++lastOperationGeneration_;
    }
    return lastOperationGeneration_;
}

void CoordinatorMembershipManager::LogUnsafeOverTargetConfiguration(const CoordinatorRaftMembershipStatus &status) const
{
    auto committedPeers = status.committedPeers;
    std::sort(committedPeers.begin(), committedPeers.end());
    LOG_FIRST_AND_EVERY_N(ERROR, kPolicyDiagnosticLogEveryCount)
        << kUnsafeOverTargetMarker << ", group=" << kCoordinatorRaftGroupId
        << ", committed_size=" << committedPeers.size() << ", expected_size=" << options_.expectedMemberCount
        << ", term=" << status.term << ", configuration_index=" << status.configurationIndex << ", committed_peers=["
        << VectorToString(committedPeers) << "]";
}

void CoordinatorMembershipManager::ClearActiveOperation()
{
    activeOperation_.reset();
    InvalidateCompletionMailbox();
}

bool CoordinatorMembershipManager::ArmCompletionMailbox(uint64_t generation)
{
    std::lock_guard<std::mutex> lock(completionMailbox_->mutex);
    if (completionMailbox_->stopping) {
        return false;
    }
    completionMailbox_->activeGeneration = generation;
    completionMailbox_->resultPublished = false;
    completionMailbox_->wakeRequested = false;
    completionMailbox_->result.reset();
    return true;
}

void CoordinatorMembershipManager::InvalidateCompletionMailbox()
{
    std::lock_guard<std::mutex> lock(completionMailbox_->mutex);
    completionMailbox_->activeGeneration = 0;
    completionMailbox_->resultPublished = false;
    completionMailbox_->wakeRequested = false;
    completionMailbox_->result.reset();
}

void CoordinatorMembershipManager::StopCompletionMailbox()
{
    std::lock_guard<std::mutex> lock(completionMailbox_->mutex);
    completionMailbox_->stopping = true;
    completionMailbox_->activeGeneration = 0;
    completionMailbox_->resultPublished = false;
    completionMailbox_->wakeRequested = false;
    completionMailbox_->result.reset();
    completionMailbox_->cv.notify_all();
}

std::optional<CoordinatorMembershipManager::CompletionResult> CoordinatorMembershipManager::TakeCompletion()
{
    std::lock_guard<std::mutex> lock(completionMailbox_->mutex);
    auto result = std::move(completionMailbox_->result);
    completionMailbox_->result.reset();
    if (result.has_value()) {
        completionMailbox_->wakeRequested = false;
    }
    return result;
}

void CoordinatorMembershipManager::RestoreCompletion(CompletionResult completion)
{
    bool notify = false;
    {
        std::lock_guard<std::mutex> lock(completionMailbox_->mutex);
        if (completionMailbox_->stopping || completionMailbox_->activeGeneration != completion.generation
            || completionMailbox_->result.has_value()) {
            return;
        }
        completionMailbox_->resultPublished = true;
        if (!completion.retryWakeIssued) {
            completion.retryWakeIssued = true;
            completionMailbox_->wakeRequested = true;
            notify = true;
        }
        completionMailbox_->result = std::move(completion);
    }
    if (notify) {
        completionMailbox_->cv.notify_all();
    }
}

const char *CoordinatorMembershipManager::OperationStageName(OperationStage stage) noexcept
{
    switch (stage) {
        case OperationStage::IDLE:
            return "IDLE";
        case OperationStage::ADDING_VACANCY:
            return "ADDING_VACANCY";
        case OperationStage::ADDING_REPLACEMENT:
            return "ADDING_REPLACEMENT";
        case OperationStage::REMOVING_FAILED:
            return "REMOVING_FAILED";
        case OperationStage::ROLLING_BACK:
            return "ROLLING_BACK";
        case OperationStage::UNCERTAIN:
            return "UNCERTAIN";
    }
    return "UNKNOWN";
}

void CoordinatorMembershipManager::PublishCompletion(const std::weak_ptr<CompletionMailbox> &weakMailbox,
                                                     uint64_t generation, Status status, CompletionOrigin origin)
{
    auto mailbox = weakMailbox.lock();
    if (mailbox == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mailbox->mutex);
        if (mailbox->stopping || mailbox->activeGeneration != generation || mailbox->resultPublished) {
            return;
        }
        mailbox->resultPublished = true;
        mailbox->wakeRequested = true;
        mailbox->result = CompletionResult{ generation, std::move(status), origin, false };
    }
    mailbox->cv.notify_all();
}

}  // namespace datasystem::coordinator
