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
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/uuid_generator.h"
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
constexpr char kAddSubmissionExceptionMarker[] = "Coordinator membership AddPeer submission exception";
constexpr char kRemoveSubmissionExceptionMarker[] = "Coordinator membership RemovePeer submission exception";
constexpr char kOperationCompletionErrorMarker[] = "Coordinator membership asynchronous operation failed";
constexpr char kUnsafeOverTargetMarker[] =
    "Coordinator membership over-target configuration has no safe removal target";
constexpr char K_COORDINATOR_MEMBERSHIP_TRACE_PREFIX[] = "CoordinatorMembership;";

std::string GetCoordinatorMembershipTraceId()
{
    auto traceId = Trace::Instance().GetTraceID();
    if (traceId.empty()) {
        traceId = std::string(K_COORDINATOR_MEMBERSHIP_TRACE_PREFIX) + GetStringUuid();
    }
    return traceId;
}

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
    if (options.healthCheckInterval >= options.memberFailureGrace) {
        return "Coordinator membership healthCheckInterval must be less than memberFailureGrace";
    }
    return nullptr;
}

}  // namespace

const char *CoordinatorMembershipManager::MutationKindName(MutationKind kind) noexcept
{
    switch (kind) {
        case MutationKind::ADD_VACANCY:
            return "ADD_VACANCY";
        case MutationKind::ADD_REPLACEMENT:
            return "ADD_REPLACEMENT";
        case MutationKind::REMOVE_FAILED:
            return "REMOVE_FAILED";
        case MutationKind::ROLLBACK_CANDIDATE:
            return "ROLLBACK_CANDIDATE";
    }
    return "UNKNOWN";
}

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
              [&raftNode] { return raftNode.HasInFlightMembershipOperation(); },
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
    : options_(options),
      dependencies_(std::move(dependencies)),
      discovery_(std::move(discovery)),
      now_(std::move(now)),
      traceId_(GetCoordinatorMembershipTraceId())
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
    if (!stopConstructedManager && threadToJoin != nullptr && threadToJoin->joinable()) {
        threadToJoin->join();
    }

    failureSince_.clear();
    candidateLastAttemptAt_.clear();
    replacementIntent_.reset();
    if (stopConstructedManager) {
        return Status::OK();
    }

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
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId_, true);
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

        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        lifecycleCv_.wait_for(lock, options_.healthCheckInterval, [this] { return state_ != LifecycleState::RUNNING; });
        if (state_ != LifecycleState::RUNNING) {
            return;
        }
    }
}

Status CoordinatorMembershipManager::ReconcileOnce()
{
    if (dependencies_.hasInFlightMembershipOperation()) {
        return Status::OK();
    }

    CoordinatorRaftMembershipStatus status;
    RETURN_IF_NOT_OK(dependencies_.getStatus(status));
    const auto now = now_();

    if (!RefreshLeaderObservation(status)) {
        return Status::OK();
    }

    CleanupPolicyState(status);
    const auto health = RefreshFollowerHealth(status, now);
    ReconcileReplacementIntent(status, health);
    if (!HasKnownQuorum(status, health)) {
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
                SubmitAdd(CaptureSubmissionSnapshot(status), candidate, {}, MutationKind::ADD_VACANCY, now));
        }
        return Status::OK();
    }

    if (committedCount > options_.expectedMemberCount) {
        if (!health.confirmedFailedPeers.empty()) {
            if (now >= nextMembershipSubmissionAt_) {
                const auto &failedPeer = health.confirmedFailedPeers.front();
                RETURN_IF_NOT_OK(SubmitRemove(CaptureSubmissionSnapshot(status), failedPeer, failedPeer,
                                              MutationKind::REMOVE_FAILED, now));
            }
            return Status::OK();
        }

        if (replacementIntent_.has_value()
            && health.explicitlyHealthyPeers.count(replacementIntent_->failedPeer) != 0) {
            const auto candidate = FindCommittedReplacementCandidate(status);
            if (candidate.has_value()) {
                if (now >= nextMembershipSubmissionAt_) {
                    RETURN_IF_NOT_OK(SubmitRemove(CaptureSubmissionSnapshot(status), *candidate,
                                                  replacementIntent_->failedPeer, MutationKind::ROLLBACK_CANDIDATE,
                                                  now));
                }
                return Status::OK();
            }
        }

        LogUnsafeOverTargetConfiguration(status);
        return Status::OK();
    }

    if (health.confirmedFailedPeers.empty()) {
        return Status::OK();
    }
    const auto &failedPeer = health.confirmedFailedPeers.front();
    if (!replacementIntent_.has_value() || replacementIntent_->failedPeer != failedPeer
        || replacementIntent_->originalPeers != status.committedPeers) {
        replacementIntent_.emplace(ReplacementIntent{ failedPeer, status.committedPeers, {} });
    }
    if (now < nextDiscoveryAt_) {
        return Status::OK();
    }

    std::string candidate;
    RETURN_IF_NOT_OK(SelectCandidate(status, now, candidate));
    if (!candidate.empty()) {
        RETURN_IF_NOT_OK(
            SubmitAdd(CaptureSubmissionSnapshot(status), candidate, failedPeer, MutationKind::ADD_REPLACEMENT, now));
    }
    return Status::OK();
}

bool CoordinatorMembershipManager::RefreshLeaderObservation(const CoordinatorRaftMembershipStatus &status)
{
    if (!status.isLeader) {
        observingLeader_ = false;
        observedTerm_ = status.term;
        failureSince_.clear();
        candidateLastAttemptAt_.clear();
        nextDiscoveryAt_ = {};
        nextMembershipSubmissionAt_ = {};
        replacementIntent_.reset();
        return false;
    }

    if (!observingLeader_ || !observedTerm_.has_value() || *observedTerm_ != status.term) {
        observingLeader_ = true;
        observedTerm_ = status.term;
        failureSince_.clear();
        candidateLastAttemptAt_.clear();
        nextDiscoveryAt_ = {};
        nextMembershipSubmissionAt_ = {};
        replacementIntent_.reset();
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

void CoordinatorMembershipManager::CleanupPolicyState(const CoordinatorRaftMembershipStatus &status)
{
    const std::set<std::string> committedPeers(status.committedPeers.begin(), status.committedPeers.end());
    for (auto it = failureSince_.begin(); it != failureSince_.end();) {
        if (committedPeers.count(it->first) == 0) {
            it = failureSince_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = candidateLastAttemptAt_.begin(); it != candidateLastAttemptAt_.end();) {
        if (committedPeers.count(it->first) != 0) {
            it = candidateLastAttemptAt_.erase(it);
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

bool CoordinatorMembershipManager::TryAdmitDiscovery()
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    return state_ == LifecycleState::CONSTRUCTED || state_ == LifecycleState::RUNNING;
}

Status CoordinatorMembershipManager::SelectCandidate(const CoordinatorRaftMembershipStatus &status, TimePoint now,
                                                     std::string &candidate)
{
    candidate.clear();
    if (!TryAdmitDiscovery()) {
        return Status::OK();
    }
    nextDiscoveryAt_ = now + options_.discoveryRetryInterval;

    std::vector<std::string> discoveredCandidates;
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
    std::set<std::string> eligibleCandidates;
    for (const auto &normalizedCandidate : normalizedCandidates) {
        if (committedPeers.count(normalizedCandidate) == 0) {
            eligibleCandidates.emplace(normalizedCandidate);
        }
    }
    for (auto it = candidateLastAttemptAt_.begin(); it != candidateLastAttemptAt_.end();) {
        if (eligibleCandidates.count(it->first) == 0) {
            it = candidateLastAttemptAt_.erase(it);
        } else {
            ++it;
        }
    }

    for (const auto &eligibleCandidate : eligibleCandidates) {
        if (candidate.empty()) {
            candidate = eligibleCandidate;
            continue;
        }
        const auto candidateAttempt = candidateLastAttemptAt_.find(candidate);
        const auto currentAttempt = candidateLastAttemptAt_.find(eligibleCandidate);
        if (candidateAttempt != candidateLastAttemptAt_.end() && currentAttempt == candidateLastAttemptAt_.end()) {
            candidate = eligibleCandidate;
            continue;
        }
        if (candidateAttempt != candidateLastAttemptAt_.end() && currentAttempt != candidateLastAttemptAt_.end()
            && currentAttempt->second < candidateAttempt->second) {
            candidate = eligibleCandidate;
        }
    }
    return Status::OK();
}

void CoordinatorMembershipManager::ReconcileReplacementIntent(const CoordinatorRaftMembershipStatus &status,
                                                              const HealthSummary &health)
{
    if (!replacementIntent_.has_value()) {
        return;
    }

    const auto failedPeer = replacementIntent_->failedPeer;
    const bool failedStillConfirmed =
        std::find(health.confirmedFailedPeers.begin(), health.confirmedFailedPeers.end(), failedPeer)
        != health.confirmedFailedPeers.end();
    if (status.committedPeers == replacementIntent_->originalPeers) {
        if (!failedStillConfirmed && replacementIntent_->attemptedCandidates.empty()) {
            replacementIntent_.reset();
        }
        return;
    }

    if (status.committedPeers.size() > options_.expectedMemberCount) {
        const auto candidate = FindCommittedReplacementCandidate(status);
        const bool failedStillCommitted =
            std::find(status.committedPeers.begin(), status.committedPeers.end(), failedPeer)
            != status.committedPeers.end();
        if (candidate.has_value() && failedStillCommitted) {
            return;
        }
    }
    replacementIntent_.reset();
}

std::optional<std::string> CoordinatorMembershipManager::FindCommittedReplacementCandidate(
    const CoordinatorRaftMembershipStatus &status) const
{
    if (!replacementIntent_.has_value() || status.committedPeers.size() <= options_.expectedMemberCount) {
        return std::nullopt;
    }
    const std::set<std::string> originalPeers(replacementIntent_->originalPeers.begin(),
                                              replacementIntent_->originalPeers.end());
    std::optional<std::string> candidate;
    for (const auto &peer : status.committedPeers) {
        if (originalPeers.count(peer) != 0) {
            continue;
        }
        if (candidate.has_value() || replacementIntent_->attemptedCandidates.count(peer) == 0) {
            return std::nullopt;
        }
        candidate = peer;
    }
    return candidate;
}

CoordinatorMembershipManager::SubmissionSnapshot CoordinatorMembershipManager::CaptureSubmissionSnapshot(
    const CoordinatorRaftMembershipStatus &status)
{
    return SubmissionSnapshot{ status.term, status.configurationIndex, status.committedPeers };
}

Status CoordinatorMembershipManager::RevalidateSubmissionPolicy(const SubmissionSnapshot &expected, MutationKind kind,
                                                                const std::string &targetPeer,
                                                                const std::string &failedPeer, TimePoint now)
{
    CoordinatorRaftMembershipStatus current;
    RETURN_IF_NOT_OK(dependencies_.getStatus(current));
    const bool snapshotMatches = current.isLeader && current.term == expected.term
                                 && current.configurationIndex == expected.configurationIndex
                                 && current.committedPeers == expected.committedPeers;
    if (!snapshotMatches) {
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

    CleanupPolicyState(current);
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

    bool policyStillHolds = false;
    switch (kind) {
        case MutationKind::ADD_VACANCY:
            policyStillHolds =
                current.committedPeers.size() < options_.expectedMemberCount && !containsCommittedPeer(targetPeer);
            break;
        case MutationKind::ADD_REPLACEMENT:
            policyStillHolds = current.committedPeers.size() == options_.expectedMemberCount
                               && !containsCommittedPeer(targetPeer) && isConfirmedFailed(failedPeer)
                               && replacementIntent_.has_value() && replacementIntent_->failedPeer == failedPeer
                               && replacementIntent_->originalPeers == current.committedPeers;
            break;
        case MutationKind::REMOVE_FAILED:
            policyStillHolds = current.committedPeers.size() > options_.expectedMemberCount
                               && containsCommittedPeer(targetPeer) && isConfirmedFailed(targetPeer);
            break;
        case MutationKind::ROLLBACK_CANDIDATE: {
            const auto committedCandidate = FindCommittedReplacementCandidate(current);
            policyStillHolds = current.committedPeers.size() > options_.expectedMemberCount
                               && containsCommittedPeer(targetPeer)
                               && health.explicitlyHealthyPeers.count(failedPeer) != 0 && committedCandidate.has_value()
                               && *committedCandidate == targetPeer;
            break;
        }
    }
    if (hasKnownQuorum && policyStillHolds) {
        return Status::OK();
    }

    return Status(K_TRY_AGAIN, "Coordinator membership submission policy changed for group "
                                   + std::string(kCoordinatorRaftGroupId) + ", mutation=" + MutationKindName(kind)
                                   + ", known_healthy_members=" + std::to_string(health.knownHealthyMembers)
                                   + ", committed_size=" + std::to_string(current.committedPeers.size())
                                   + ", confirmed_failed_peers=[" + VectorToString(health.confirmedFailedPeers) + "]");
}

Status CoordinatorMembershipManager::SubmitAdd(const SubmissionSnapshot &expected, const std::string &candidate,
                                               const std::string &failedPeer, MutationKind kind, TimePoint now)
{
    RETURN_IF_NOT_OK(RevalidateSubmissionPolicy(expected, kind, candidate, failedPeer, now));
    std::unique_lock<std::mutex> lifecycleLock(lifecycleMutex_);
    if (state_ == LifecycleState::STOPPING || state_ == LifecycleState::STOPPED) {
        return Status::OK();
    }
    candidateLastAttemptAt_[candidate] = now;

    std::set<std::string> *newOwnershipSet = nullptr;
    std::set<std::string>::iterator newOwnership;
    if (kind == MutationKind::ADD_REPLACEMENT && replacementIntent_.has_value()
        && replacementIntent_->failedPeer == failedPeer
        && replacementIntent_->originalPeers == expected.committedPeers) {
        auto &attemptedCandidates = replacementIntent_->attemptedCandidates;
        const auto [it, inserted] = attemptedCandidates.emplace(candidate);
        if (inserted) {
            newOwnershipSet = &attemptedCandidates;
            newOwnership = it;
        }
    }
    const auto rollbackNewOwnership = [&newOwnershipSet, &newOwnership] {
        if (newOwnershipSet != nullptr) {
            newOwnershipSet->erase(newOwnership);
            newOwnershipSet = nullptr;
        }
    };

    try {
        auto submissionStatus = dependencies_.addPeer(candidate, [](const Status &completionStatus) {
            LogMembershipOperationCompletion("AddPeer", completionStatus);
        });
        if (submissionStatus.IsError()) {
            rollbackNewOwnership();
        }
        return submissionStatus;
    } catch (const std::exception &e) {
        rollbackNewOwnership();
        const auto error = FormatString("%s: %s", kAddSubmissionExceptionMarker, e.what());
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << error;
        return Status(K_RUNTIME_ERROR, error);
    } catch (...) {
        rollbackNewOwnership();
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kAddSubmissionExceptionMarker;
        return Status(K_RUNTIME_ERROR, kAddSubmissionExceptionMarker);
    }
}

Status CoordinatorMembershipManager::SubmitRemove(const SubmissionSnapshot &expected, const std::string &targetPeer,
                                                  const std::string &failedPeer, MutationKind kind, TimePoint now)
{
    RETURN_IF_NOT_OK(RevalidateSubmissionPolicy(expected, kind, targetPeer, failedPeer, now));
    std::unique_lock<std::mutex> lifecycleLock(lifecycleMutex_);
    if (state_ == LifecycleState::STOPPING || state_ == LifecycleState::STOPPED) {
        return Status::OK();
    }
    nextMembershipSubmissionAt_ = now + options_.discoveryRetryInterval;

    try {
        return dependencies_.removePeer(targetPeer, [](const Status &completionStatus) {
            LogMembershipOperationCompletion("RemovePeer", completionStatus);
        });
    } catch (const std::exception &e) {
        const auto error = FormatString("%s: %s", kRemoveSubmissionExceptionMarker, e.what());
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << error;
        return Status(K_RUNTIME_ERROR, error);
    } catch (...) {
        LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount) << kRemoveSubmissionExceptionMarker;
        return Status(K_RUNTIME_ERROR, kRemoveSubmissionExceptionMarker);
    }
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

void CoordinatorMembershipManager::LogMembershipOperationCompletion(const char *operation, const Status &status)
{
    if (status.IsOk()) {
        return;
    }
    LOG_FIRST_AND_EVERY_N(WARNING, kPolicyDiagnosticLogEveryCount)
        << kOperationCompletionErrorMarker << ", operation=" << operation << ", status=" << status;
}

}  // namespace datasystem::coordinator
