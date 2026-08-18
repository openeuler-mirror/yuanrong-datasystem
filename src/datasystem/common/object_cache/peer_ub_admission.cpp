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

/** Description: Track process-local UB data-provider admission state. */

#include "datasystem/common/object_cache/peer_ub_admission.h"

#include <algorithm>
#include <exception>
#include <iterator>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {

Status PeerUbAdmission::CheckWriteTarget(const HostPort &peer, UbOperationKind op) const
{
    (void)op;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!IsGlobalWritableLocked(peer)) {
        INJECT_POINT_NO_RETURN("PeerUbAdmission.CheckWriteTarget.blocked");
        return BuildUnavailableStatus(peer, StatusCode::K_URMA_WORKER_UNAVAILABLE);
    }
    auto it = states_.find(peer);
    if (it == states_.end() || !ShouldBlock(it->second)) {
        return Status::OK();
    }
    INJECT_POINT_NO_RETURN("PeerUbAdmission.CheckWriteTarget.blocked");
    return BuildUnavailableStatus(peer, StatusCode::K_URMA_WORKER_UNAVAILABLE);
}

Status PeerUbAdmission::CheckReadSource(const HostPort &peer) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!IsGlobalWritableLocked(peer)) {
        return BuildUnavailableStatus(peer, StatusCode::K_URMA_DATA_WORKER_UNAVAILABLE);
    }
    auto it = states_.find(peer);
    if (it == states_.end() || !ShouldBlock(it->second)) {
        return Status::OK();
    }
    return BuildUnavailableStatus(peer, StatusCode::K_URMA_DATA_WORKER_UNAVAILABLE);
}

void PeerUbAdmission::SetSelfWorker(const HostPort &self)
{
    // Write-once during construction, read-only afterwards. The identity distinguishes the
    // process's local observation from the lease echo of its previously published summary.
    self_ = self;
}

void PeerUbAdmission::ReportOutcome(const UbOpOutcome &outcome)
{
    ReportOutcomeImpl(outcome, std::nullopt);
}

void PeerUbAdmission::ReportOutcomeImpl(const UbOpOutcome &outcome, std::optional<LateCompletionFence> fence)
{
    auto failureClass = classifier_.Classify(outcome);
    if (failureClass == UbFailureClass::SUCCESS || failureClass == UbFailureClass::LOCAL_RESOURCE_PRESSURE
        || failureClass == UbFailureClass::NON_UB_FAILURE) {
        return;
    }

    // issue #958: CONNECT_OR_PATH_FAILURE (e.g. post failure ret=4096) cannot tell a local send
    // fault from a peer receive fault, so hard-isolating on first sight over-blocks. Treat it as
    // SUSPECT and actively verify the path without closing admission.
    const auto nextState =
        (failureClass == UbFailureClass::TIMEOUT_SUSPECT
         || failureClass == UbFailureClass::CONNECT_OR_PATH_FAILURE)
            ? UbAdmissionState::SUSPECT
            : UbAdmissionState::UNAVAILABLE;
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        if ((fence.has_value() && !IsLateCompletionFenceCurrentLocked(outcome.peer, *fence))
            || !UpdatePathStateLocked(outcome, failureClass, nextState)) {
            return;
        }
    }
    if (nextState == UbAdmissionState::SUSPECT) {
        LOG(INFO) << "UB admission marked peer SUSPECT, peer=" << outcome.peer
                  << ", statusCode=" << outcome.status.GetCode();
    } else {
        LOG(WARNING) << "UB admission marked peer UNAVAILABLE, peer=" << outcome.peer
                     << ", statusCode=" << outcome.status.GetCode()
                     << ", failureClass=" << static_cast<int>(failureClass);
    }
}

void PeerUbAdmission::ReplaceGlobalSummaries(const std::vector<UbHealthSummary> &summaries)
{
    constexpr size_t MAX_RETIRED_INCARNATIONS_PER_WORKER = 8;
    std::lock_guard<std::shared_mutex> lock(mutex_);
    const auto nowMs = GetSteadyClockTimeStampMs();
    std::unordered_map<HostPort, UbHealthSummary> replacement;
    replacement.reserve(summaries.size());
    for (const auto &summary : summaries) {
        if (summary.worker.Empty() || summary.incarnation.empty()) {
            continue;
        }
        if ((topologyInitialized_ && topologyWorkers_.count(summary.worker) == 0)
            || IsReplayLocked(summary.worker, summary.incarnation)) {
            continue;
        }
        auto latest = latestGlobalIncarnations_.find(summary.worker);
        if (latest == latestGlobalIncarnations_.end()) {
            latestGlobalIncarnations_.emplace(summary.worker, summary.incarnation);
        } else if (latest->second != summary.incarnation) {
            auto &retired = retiredGlobalIncarnations_[summary.worker];
            if (retired.count(summary.incarnation) != 0) {
                auto current = globalSummaries_.find(summary.worker);
                if (current != globalSummaries_.end()) {
                    replacement.emplace(summary.worker, current->second);
                }
                continue;
            }
            retired.emplace(latest->second);
            if (retired.size() > MAX_RETIRED_INCARNATIONS_PER_WORKER) {
                retired.erase(retired.begin());
            }
            latest->second = summary.incarnation;
            // A trusted new incarnation supersedes process-local evidence learned from the old worker instance.
            states_.erase(summary.worker);
            AdvancePeerCompletionGenerationLocked(summary.worker);
        }

        auto candidate = summary;
        auto current = globalSummaries_.find(summary.worker);
        if (current != globalSummaries_.end() && current->second.incarnation == summary.incarnation
            && summary.epoch < current->second.epoch) {
            candidate = current->second;
        }
        auto [iter, inserted] = replacement.emplace(summary.worker, candidate);
        if (!inserted && iter->second.incarnation == candidate.incarnation && candidate.epoch > iter->second.epoch) {
            iter->second = candidate;
        }
        ApplyGlobalRecoveryTransitionLocked(iter->second, nowMs);
    }
    globalSummaries_ = std::move(replacement);
    INJECT_POINT_NO_RETURN("PeerUbAdmission.ReplaceGlobalSummaries.afterCommit");
}

void PeerUbAdmission::InitializeVerification(const HostPort &peer, uint64_t nowMs)
{
    if (peer.Empty()) {
        return;
    }
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto &state = states_[peer];
    state.state = UbAdmissionState::SUSPECT;
    state.lastStatus = Status(K_NOT_READY, "UB data plane requires verification probe");
    state.lastFailureClass = UbFailureClass::CONNECT_OR_PATH_FAILURE;
    state.backoffLevel = 0;
    state.backoffDeadlineMs = nowMs;
    state.probeInFlight = false;
    ++state.epoch;
}

std::optional<UbProbeToken> PeerUbAdmission::TryBeginProbe(const HostPort &peer, uint64_t nowMs)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto iter = states_.find(peer);
    if (iter == states_.end() || nowMs < iter->second.backoffDeadlineMs) {
        return std::nullopt;
    }
    auto &state = iter->second;
    if (state.state != UbAdmissionState::UNAVAILABLE && state.state != UbAdmissionState::SUSPECT
        && state.state != UbAdmissionState::PROBING) {
        return std::nullopt;
    }
    if (state.probeInFlight) {
        return std::nullopt;
    }
    if (state.state != UbAdmissionState::SUSPECT) {
        state.state = UbAdmissionState::PROBING;
    }
    state.probeInFlight = true;
    ++state.epoch;
    return UbProbeToken{ peer, state.epoch };
}

bool PeerUbAdmission::CancelProbe(const UbProbeToken &token, uint64_t nowMs)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto iter = states_.find(token.peer);
    if (iter == states_.end()
        || (iter->second.state != UbAdmissionState::PROBING
            && iter->second.state != UbAdmissionState::SUSPECT)
        || !iter->second.probeInFlight
        || iter->second.epoch != token.epoch) {
        return false;
    }
    auto &state = iter->second;
    const bool softFailure = state.lastFailureClass == UbFailureClass::TIMEOUT_SUSPECT
                             || state.lastFailureClass == UbFailureClass::CONNECT_OR_PATH_FAILURE;
    state.state = softFailure ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
    state.probeInFlight = false;
    state.backoffLevel = std::max(state.backoffLevel, 1U);
    state.backoffDeadlineMs = nowMs + ProbeBackoffMs(state.backoffLevel);
    ++state.epoch;
    return true;
}

bool PeerUbAdmission::CompleteProbe(const UbProbeToken &token, const Status &status, uint64_t nowMs,
                                    bool requireGlobalAvailable)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto iter = states_.find(token.peer);
    if (iter == states_.end()
        || (iter->second.state != UbAdmissionState::PROBING
            && iter->second.state != UbAdmissionState::SUSPECT)
        || !iter->second.probeInFlight
        || iter->second.epoch != token.epoch) {
        return false;
    }
    auto &state = iter->second;
    if (status.IsOk() && (!requireGlobalAvailable || IsGlobalWritableLocked(token.peer))) {
        state.state = UbAdmissionState::AVAILABLE;
        state.lastStatus = Status::OK();
        state.lastFailureClass = UbFailureClass::SUCCESS;
        state.backoffLevel = 0;
        state.backoffDeadlineMs = 0;
        state.probeInFlight = false;
        state.providerStatus.reset();
        state.cqeStatus.reset();
        ++state.epoch;
        if (token.peer == self_) {
            lateCompletionGeneration_.fetch_add(1, std::memory_order_acq_rel);
        }
        AdvancePeerCompletionGenerationLocked(token.peer);
        INJECT_POINT_NO_RETURN("PeerUbAdmission.CompleteProbe.success");
        return true;
    }
    const bool softVerification = state.state == UbAdmissionState::SUSPECT;
    state.state = softVerification ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
    state.probeInFlight = false;
    state.lastStatus = status.IsOk() ? Status(K_NOT_READY, "Global UB health still denies recovery") : status;
    state.backoffLevel = std::min(state.backoffLevel + 1, MAX_PROBE_BACKOFF_LEVEL);
    state.backoffDeadlineMs = nowMs + ProbeBackoffMs(state.backoffLevel);
    ++state.epoch;
    INJECT_POINT_NO_RETURN("PeerUbAdmission.CompleteProbe.failure");
    return false;
}

std::optional<HostPort> PeerUbAdmission::NextProbeCandidate(uint64_t nowMs) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto &[peer, state] : states_) {
        const bool recoverable = state.state == UbAdmissionState::UNAVAILABLE
                                 || state.state == UbAdmissionState::SUSPECT
                                 || state.state == UbAdmissionState::PROBING;
        if (recoverable && !state.probeInFlight && state.lastStatus.IsError()
            && nowMs >= state.backoffDeadlineMs) {
            return peer;
        }
    }
    return std::nullopt;
}

std::optional<uint64_t> PeerUbAdmission::NextProbeDeadlineMs() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::optional<uint64_t> deadline;
    for (const auto &[peer, state] : states_) {
        (void)peer;
        const bool recoverable = state.state == UbAdmissionState::UNAVAILABLE
                                 || state.state == UbAdmissionState::SUSPECT
                                 || state.state == UbAdmissionState::PROBING;
        if (recoverable && !state.probeInFlight && state.lastStatus.IsError()
            && (!deadline.has_value() || state.backoffDeadlineMs < *deadline)) {
            deadline = state.backoffDeadlineMs;
        }
    }
    return deadline;
}

void PeerUbAdmission::ReconcileTopologyWorkers(const std::unordered_set<HostPort> &workers, uint64_t nowMs,
                                               uint64_t cleanupGraceMs)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    topologyInitialized_ = true;
    topologyWorkers_ = workers;
    for (const auto &worker : workers) {
        departedWorkers_.erase(worker);
    }
    std::unordered_set<HostPort> trackedWorkers;
    for (const auto &[worker, state] : states_) {
        (void)state;
        trackedWorkers.emplace(worker);
    }
    for (const auto &[worker, summary] : globalSummaries_) {
        (void)summary;
        trackedWorkers.emplace(worker);
    }
    for (const auto &[worker, incarnation] : latestGlobalIncarnations_) {
        (void)incarnation;
        trackedWorkers.emplace(worker);
    }
    for (const auto &[worker, incarnations] : retiredGlobalIncarnations_) {
        (void)incarnations;
        trackedWorkers.emplace(worker);
    }
    for (const auto &[worker, generation] : peerCompletionGenerations_) {
        (void)generation;
        trackedWorkers.emplace(worker);
    }
    for (const auto &worker : trackedWorkers) {
        if (workers.count(worker) == 0) {
            departedWorkers_.try_emplace(worker, nowMs);
        }
    }
    std::vector<HostPort> expired;
    for (const auto &[worker, departedAt] : departedWorkers_) {
        if (nowMs >= departedAt && nowMs - departedAt >= cleanupGraceMs) {
            expired.emplace_back(worker);
        }
    }
    for (const auto &worker : expired) {
        RetireWorkerLocked(worker, nowMs, cleanupGraceMs);
        departedWorkers_.erase(worker);
    }
    PruneTombstonesLocked(nowMs);
}

void PeerUbAdmission::PruneExpiredTopologyState(uint64_t nowMs)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    if (nextTombstoneExpiryMs_ != 0 && nowMs >= nextTombstoneExpiryMs_) {
        PruneTombstonesLocked(nowMs);
    }
}

UbHealthSummary PeerUbAdmission::BuildSelfHealthSummary(const HostPort &self) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    UbHealthSummary summary;
    summary.worker = self;
    auto it = states_.find(self);
    if (it == states_.end()) {
        return summary;
    }
    const auto &state = it->second;
    summary.writable = !ShouldBlock(state);
    summary.state = state.state;
    summary.reason = state.lastFailureClass;
    summary.lastStatusCode = state.lastStatus.GetCode();
    summary.epoch = state.epoch;
    summary.backoffLevel = state.backoffLevel;
    summary.backoffDeadlineMs = state.backoffDeadlineMs;
    return summary;
}

std::optional<UbPathState> PeerUbAdmission::GetState(const HostPort &peer) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = states_.find(peer);
    return it == states_.end() ? std::nullopt : std::optional<UbPathState>{ it->second };
}

PeerUbAdmissionStats PeerUbAdmission::GetStats() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return PeerUbAdmissionStats{ states_.size(), globalSummaries_.size(), latestGlobalIncarnations_.size(),
                                 retiredGlobalIncarnations_.size(), departedWorkers_.size(),
                                 replayTombstones_.size(), peerCompletionGenerations_.size() };
}

void PeerUbAdmission::ClearLocalState(const HostPort &peer)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    states_.erase(peer);
    AdvancePeerCompletionGenerationLocked(peer);
    if (peer == self_) {
        lateCompletionGeneration_.fetch_add(1, std::memory_order_acq_rel);
    }
}

bool PeerUbAdmission::IsLateCompletionFenceCurrentLocked(const HostPort &peer,
                                                         const LateCompletionFence &fence) const
{
    if (fence.scope == LateCompletionScope::LOCAL_SENDER) {
        return fence.generation == lateCompletionGeneration_.load(std::memory_order_acquire);
    }
    auto generation = peerCompletionGenerations_.find(peer);
    if (generation == peerCompletionGenerations_.end() || generation->second != fence.generation) {
        return false;
    }
    return true;
}

bool PeerUbAdmission::UpdatePathStateLocked(const UbOpOutcome &outcome, UbFailureClass failureClass,
                                            UbAdmissionState nextState)
{
    auto &state = states_[outcome.peer];
    const bool softFailure = failureClass == UbFailureClass::TIMEOUT_SUSPECT
                             || failureClass == UbFailureClass::CONNECT_OR_PATH_FAILURE;
    // Soft evidence cannot downgrade an existing hard failure or invalidate an active recovery probe.
    if (softFailure && (state.state == UbAdmissionState::UNAVAILABLE || state.state == UbAdmissionState::PROBING)) {
        return false;
    }
    if (state.state == nextState) {
        return false;
    }
    state.lastStatus = outcome.status;
    state.lastFailureClass = failureClass;
    state.providerStatus = outcome.providerStatus;
    state.cqeStatus = outcome.cqeStatus;
    state.state = nextState;
    state.probeInFlight = false;
    state.backoffLevel = std::max(state.backoffLevel, 1U);
    state.backoffDeadlineMs = GetSteadyClockTimeStampMs() + ProbeBackoffMs(state.backoffLevel);
    ++state.epoch;
    return true;
}

std::optional<UrmaLateCompletionContext> PeerUbAdmission::BuildLateCompletionContext(
    UbOperationKind operation, const std::optional<HostPort> &remotePeer)
{
    auto observer = weak_from_this();
    if (observer.expired()) {
        return std::nullopt;
    }
    const auto operationValue = static_cast<uint64_t>(operation);
    if (operationValue > static_cast<uint64_t>(UbOperationKind::MIGRATION_WRITE)) {
        return std::nullopt;
    }
    const uint64_t ownerToken =
        (lateCompletionGeneration_.load(std::memory_order_acquire) << LATE_COMPLETION_OPERATION_BITS)
        | operationValue;
    uint64_t peerToken = 0;
    if (remotePeer.has_value() && !remotePeer->Empty()) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        peerToken = GetOrCreatePeerCompletionGenerationLocked(*remotePeer);
    }
    return UrmaLateCompletionContext{ observer, ownerToken, peerToken };
}

void PeerUbAdmission::OnLateUrmaCompletion(const UrmaLateCompletion &completion, uint64_t ownerToken,
                                           uint64_t peerToken) noexcept
{
    try {
        const bool localSenderFailure = completion.cqeStatus == URMA_PORT_UNAVAILABLE_STATUS;
        const bool remotePeerFailure = completion.cqeStatus == URMA_REMOTE_ACK_TIMEOUT_STATUS;
        if (!localSenderFailure && !remotePeerFailure) {
            return;
        }
        const auto operationValue = ownerToken & LATE_COMPLETION_OPERATION_MASK;
        if (operationValue > static_cast<uint64_t>(UbOperationKind::MIGRATION_WRITE)) {
            return;
        }
        const uint64_t generation = ownerToken >> LATE_COMPLETION_OPERATION_BITS;
        HostPort attributedPeer;
        LateCompletionFence fence{ LateCompletionScope::LOCAL_SENDER, generation };
        if (localSenderFailure) {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            if (self_.Empty()) {
                return;
            }
            attributedPeer = self_;
        } else {
            if (peerToken == 0 || attributedPeer.ParseString(completion.remoteAddress).IsError()) {
                return;
            }
            fence = { LateCompletionScope::REMOTE_PEER, peerToken };
        }
        UbOpOutcome outcome(
            attributedPeer, static_cast<UbOperationKind>(operationValue),
            Status(K_URMA_ERROR,
                   FormatString("Late URMA completion reports %s unavailable, requestId=%llu, remoteAddress=%s, "
                                "remoteInstanceId=%s, cqeStatus=%d",
                                localSenderFailure ? "local Worker sender" : "remote Worker peer",
                                completion.requestId, completion.remoteAddress.c_str(),
                                completion.remoteInstanceId.c_str(), completion.cqeStatus)));
        outcome.cqeStatus = completion.cqeStatus;
        outcome.learnedFrom = "late_urma_completion";
        ReportOutcomeImpl(outcome, std::move(fence));
    } catch (const std::exception &error) {
        LOG(ERROR) << "Failed to process late Worker URMA completion: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Failed to process late Worker URMA completion: unknown exception";
    }
}

bool UbHealthSummaryCache::Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    if (summary.worker.Empty() || summary.incarnation.empty() || summary.incarnation != expectedIncarnation) {
        return false;
    }
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto &retired = retiredIncarnations_[summary.worker];
    if (retired.count(summary.incarnation) != 0) {
        return false;
    }
    auto it = summaries_.find(summary.worker);
    if (it != summaries_.end()) {
        if (it->second.incarnation == summary.incarnation && summary.epoch < it->second.epoch) {
            return false;
        }
        if (it->second.incarnation != summary.incarnation) {
            retired.emplace(it->second.incarnation);
            if (retired.size() > MAX_RETIRED_INCARNATIONS_PER_WORKER) {
                retired.erase(retired.begin());
            }
        }
    }
    summaries_[summary.worker] = summary;
    return true;
}

void UbHealthSummaryCache::ReconcileWorkers(const std::unordered_set<HostPort> &workers)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    for (auto iter = summaries_.begin(); iter != summaries_.end();) {
        iter = workers.count(iter->first) == 0 ? summaries_.erase(iter) : std::next(iter);
    }
    for (auto iter = retiredIncarnations_.begin(); iter != retiredIncarnations_.end();) {
        iter = workers.count(iter->first) == 0 ? retiredIncarnations_.erase(iter) : std::next(iter);
    }
}

std::optional<UbHealthSummary> UbHealthSummaryCache::Get(const HostPort &worker) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = summaries_.find(worker);
    return it == summaries_.end() ? std::nullopt : std::optional<UbHealthSummary>{ it->second };
}

size_t UbHealthSummaryCache::Size() const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return summaries_.size();
}

bool PeerUbAdmission::ShouldBlock(const UbPathState &state)
{
    return state.state == UbAdmissionState::UNAVAILABLE || state.state == UbAdmissionState::PROBING;
}

Status PeerUbAdmission::BuildUnavailableStatus(const HostPort &peer, StatusCode code)
{
    return Status(code, FormatString("UB data plane unavailable for peer %s", peer.ToString()));
}

uint64_t PeerUbAdmission::ProbeBackoffMs(uint32_t level)
{
    const uint32_t bounded = std::clamp<uint32_t>(level, 1, MAX_PROBE_BACKOFF_LEVEL);
    return PROBE_BASE_DELAY_MS * (1ULL << (bounded - 1));
}

bool PeerUbAdmission::IsGlobalWritableLocked(const HostPort &peer) const
{
    // The lease contains this process's own last published summary. Treating that echo as an
    // external recovery fence creates a cycle: a local failure publishes writable=false, then
    // the same stale fact prevents the successful self probe from publishing writable=true.
    if (!self_.Empty() && peer == self_) {
        return true;
    }
    auto global = globalSummaries_.find(peer);
    return global == globalSummaries_.end() || global->second.writable;
}

bool PeerUbAdmission::IsReplayLocked(const HostPort &worker, const std::string &incarnation) const
{
    auto tombstone = replayTombstones_.find(worker);
    return tombstone != replayTombstones_.end() && tombstone->second.incarnations.count(incarnation) != 0;
}

void PeerUbAdmission::ApplyGlobalRecoveryTransitionLocked(const UbHealthSummary &summary, uint64_t nowMs)
{
    if (!self_.Empty() && summary.worker == self_) {
        // Lease sync publishes and then reads back this process's own
        // summary. It must not turn a local PROBING token into UNAVAILABLE
        // while consuming that self-summary.
        return;
    }
    auto local = states_.find(summary.worker);
    if (local == states_.end()) {
        return;
    }
    auto &state = local->second;
    if (!summary.writable) {
        if (state.state == UbAdmissionState::PROBING) {
            state.state = UbAdmissionState::UNAVAILABLE;
            state.probeInFlight = false;
            state.lastStatus = Status(K_NOT_READY, "Global UB health denies recovery");
            ++state.epoch;
        }
        return;
    }
    if (state.state == UbAdmissionState::UNAVAILABLE) {
        state.state = UbAdmissionState::PROBING;
        state.probeInFlight = false;
        state.backoffDeadlineMs = nowMs;
        ++state.epoch;
    }
}

void PeerUbAdmission::RetireWorkerLocked(const HostPort &worker, uint64_t nowMs, uint64_t tombstoneTtlMs)
{
    RetiredWorkerTombstone tombstone;
    auto latest = latestGlobalIncarnations_.find(worker);
    if (latest != latestGlobalIncarnations_.end()) {
        tombstone.incarnations.emplace(latest->second);
    }
    auto retired = retiredGlobalIncarnations_.find(worker);
    if (retired != retiredGlobalIncarnations_.end()) {
        tombstone.incarnations.insert(retired->second.begin(), retired->second.end());
    }
    if (!tombstone.incarnations.empty()) {
        tombstone.expiresAtMs = nowMs + tombstoneTtlMs;
        if (nextTombstoneExpiryMs_ == 0 || tombstone.expiresAtMs < nextTombstoneExpiryMs_) {
            nextTombstoneExpiryMs_ = tombstone.expiresAtMs;
        }
        replayTombstones_[worker] = std::move(tombstone);
    }
    states_.erase(worker);
    globalSummaries_.erase(worker);
    latestGlobalIncarnations_.erase(worker);
    retiredGlobalIncarnations_.erase(worker);
    peerCompletionGenerations_.erase(worker);
}

uint64_t PeerUbAdmission::GetOrCreatePeerCompletionGenerationLocked(const HostPort &peer)
{
    auto [iter, inserted] = peerCompletionGenerations_.try_emplace(peer, 0);
    if (inserted) {
        iter->second = ++nextPeerCompletionGeneration_;
    }
    return iter->second;
}

void PeerUbAdmission::AdvancePeerCompletionGenerationLocked(const HostPort &peer)
{
    peerCompletionGenerations_[peer] = ++nextPeerCompletionGeneration_;
}

void PeerUbAdmission::PruneTombstonesLocked(uint64_t nowMs)
{
    for (auto iter = replayTombstones_.begin(); iter != replayTombstones_.end();) {
        iter = iter->second.expiresAtMs <= nowMs ? replayTombstones_.erase(iter) : std::next(iter);
    }
    while (replayTombstones_.size() > MAX_REPLAY_TOMBSTONES) {
        replayTombstones_.erase(replayTombstones_.begin());
    }
    nextTombstoneExpiryMs_ = 0;
    for (const auto &[worker, tombstone] : replayTombstones_) {
        (void)worker;
        if (nextTombstoneExpiryMs_ == 0 || tombstone.expiresAtMs < nextTombstoneExpiryMs_) {
            nextTombstoneExpiryMs_ = tombstone.expiresAtMs;
        }
    }
}

}  // namespace datasystem
