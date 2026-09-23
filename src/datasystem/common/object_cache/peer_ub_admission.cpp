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
#include <array>
#include <exception>
#include <iterator>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace {

enum class GlobalSummaryTransition {
    QUARANTINE_APPLIED,
    QUARANTINE_UPDATED,
    RECOVERY_APPLIED,
    INCARNATION_REPLACED,
    SUMMARY_REMOVED
};

struct GlobalSummaryChange {
    GlobalSummaryTransition transition;
    HostPort worker;
    std::array<uint8_t, UUID_SIZE> incarnationBytes;
    size_t incarnationSize;
    uint64_t epoch;
    bool writable;
    UbAdmissionState state;
    UbFailureClass reason;
    StatusCode lastStatusCode;
};

struct GlobalSummarySyncStats {
    size_t input = 0;
    size_t effective = 0;
    size_t invalidIdentity = 0;
    size_t nonMember = 0;
    size_t replay = 0;
    size_t retiredIncarnation = 0;
    size_t staleEpoch = 0;
    size_t duplicate = 0;
};

const char *GlobalSummaryTransitionName(GlobalSummaryTransition transition)
{
    switch (transition) {
        case GlobalSummaryTransition::QUARANTINE_APPLIED:
            return "quarantine_applied";
        case GlobalSummaryTransition::QUARANTINE_UPDATED:
            return "quarantine_updated";
        case GlobalSummaryTransition::RECOVERY_APPLIED:
            return "recovery_applied";
        case GlobalSummaryTransition::INCARNATION_REPLACED:
            return "incarnation_replaced";
        case GlobalSummaryTransition::SUMMARY_REMOVED:
            return "summary_removed";
    }
    return "unknown";
}

bool IsSameGlobalAdmissionView(const UbHealthSummary &lhs, const UbHealthSummary &rhs)
{
    return lhs.incarnation == rhs.incarnation && lhs.writable == rhs.writable && lhs.reason == rhs.reason
           && lhs.lastStatusCode == rhs.lastStatusCode;
}

bool IsQueryAuthoritativePortHealthSummary(const UbHealthSummary &summary)
{
    if (!summary.portHealth.has_value() || !HasKnownUbPortHealth(*summary.portHealth)) {
        return false;
    }
    const bool isolated = summary.portHealth->badPortCount == summary.portHealth->totalPortCount;
    const bool recovered = summary.portHealth->badPortCount < summary.portHealth->totalPortCount;
    return (isolated && !summary.writable && summary.state == UbAdmissionState::UNAVAILABLE)
           || (recovered && summary.writable && summary.state == UbAdmissionState::AVAILABLE);
}

bool IsHardUnavailableFailure(UbFailureClass failureClass)
{
    return failureClass == UbFailureClass::PORT_UNAVAILABLE_ERROR4
           || failureClass == UbFailureClass::REMOTE_UNAVAILABLE_ERROR9;
}

bool IsPortHealthTrigger(UbFailureClass failureClass)
{
    return failureClass == UbFailureClass::PORT_UNAVAILABLE_ERROR4
           || failureClass == UbFailureClass::REMOTE_UNAVAILABLE_ERROR9;
}

void InvokeRemoteVerificationTrigger(
    const std::shared_ptr<const PeerUbAdmission::RemotePortHealthVerificationTrigger> &trigger,
    const HostPort &peer)
{
    if (trigger == nullptr) {
        return;
    }
    try {
        (*trigger)(peer);
    } catch (const std::exception &e) {
        LOG(ERROR) << "UB remote port-health verification trigger threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "UB remote port-health verification trigger threw an unknown exception";
    }
}

struct PortHealthApplyResult {
    bool applied = false;
    bool isolated = false;
    bool isolatedTransition = false;
    bool recovered = false;
    UbAdmissionState previousState = UbAdmissionState::AVAILABLE;
    std::optional<uint32_t> previousBadPortCount;
};

bool IsApplicablePassiveRecovery(UbPortHealthEvidenceSource source, const UbPathState *state,
                                 const UbPortHealthSummary &summary)
{
    return source != UbPortHealthEvidenceSource::PASSIVE_RECOVERY
           || (state != nullptr && state->portHealthGoverned && state->state == UbAdmissionState::UNAVAILABLE
               && state->portHealth.has_value() && CanApplyPassiveUbRecovery(*state->portHealth, summary));
}

bool CanApplyPortHealthUpdate(const UbPathState &state, const UbPortHealthSummary &summary,
                              UbAdmissionState nextState)
{
    if (!state.portHealth.has_value()) {
        return true;
    }
    const auto &previous = *state.portHealth;
    if (summary.healthEpoch != previous.healthEpoch) {
        return summary.healthEpoch > previous.healthEpoch;
    }
    const bool sameCounts = summary.valid == previous.valid && summary.totalPortCount == previous.totalPortCount
                            && summary.badPortCount == previous.badPortCount;
    return sameCounts && (summary.verificationPending != previous.verificationPending
                          || (!summary.verificationPending && state.state != nextState));
}

void LogPortHealthApply(const HostPort &subject, const UbPortHealthSummary &summary,
                        UbPortHealthEvidenceSource source, const PortHealthApplyResult &result)
{
    const char *sourceName = source == UbPortHealthEvidenceSource::QUERY_RESPONSE
                                 ? "query_response"
                                 : (source == UbPortHealthEvidenceSource::PASSIVE_RECOVERY
                                        ? "passive_recovery"
                                        : "local_snapshot");
    if (result.isolatedTransition) {
        LOG(WARNING) << "UB admission marked peer UNAVAILABLE, peer=" << subject.ToString()
                     << ", previous_state=" << static_cast<int>(result.previousState)
                     << ", state=" << static_cast<int>(UbAdmissionState::UNAVAILABLE)
                     << ", bad=" << summary.badPortCount << ", total=" << summary.totalPortCount
                     << ", health_epoch=" << summary.healthEpoch << ", source=" << sourceName;
    } else if (result.recovered) {
        LOG(INFO) << "UB admission marked peer AVAILABLE, peer=" << subject.ToString()
                  << ", previous_state=" << static_cast<int>(result.previousState)
                  << ", state=" << static_cast<int>(UbAdmissionState::AVAILABLE)
                  << ", previous_bad="
                  << (result.previousBadPortCount.has_value()
                          ? std::to_string(*result.previousBadPortCount)
                          : "unknown")
                  << ", bad=" << summary.badPortCount << ", total=" << summary.totalPortCount
                  << ", health_epoch=" << summary.healthEpoch << ", source=" << sourceName;
    } else if (result.applied) {
        LOG(INFO) << "UB admission applied port health, peer=" << subject.ToString()
                  << ", state=" << static_cast<int>(result.isolated ? UbAdmissionState::UNAVAILABLE
                                                                     : UbAdmissionState::AVAILABLE)
                  << ", bad=" << summary.badPortCount << ", total=" << summary.totalPortCount;
    }
}

GlobalSummaryChange CaptureGlobalSummaryChange(GlobalSummaryTransition transition, const UbHealthSummary &summary)
{
    std::array<uint8_t, UUID_SIZE> incarnationBytes{};
    const auto capturedSize = std::min(summary.incarnation.size(), incarnationBytes.size());
    std::copy_n(reinterpret_cast<const uint8_t *>(summary.incarnation.data()), capturedSize,
                incarnationBytes.begin());
    return GlobalSummaryChange{ transition, summary.worker, incarnationBytes, summary.incarnation.size(), summary.epoch,
                                summary.writable, summary.state, summary.reason, summary.lastStatusCode };
}

}  // namespace

Status PeerUbAdmission::CheckWriteTarget(const HostPort &peer, UbOperationKind op) const
{
    (void)op;
    bool blocked;
    {
        bthread::RWLockRdGuard lock(mutex_);
        auto it = states_.find(peer);
        blocked = !IsGlobalWritableLocked(peer) || (it != states_.end() && ShouldBlock(it->second));
    }
    if (!blocked) {
        return Status::OK();
    }
    INJECT_POINT_NO_RETURN("PeerUbAdmission.CheckWriteTarget.blocked");
    return BuildUnavailableStatus(peer, StatusCode::K_URMA_WORKER_UNAVAILABLE);
}

Status PeerUbAdmission::CheckReadSource(const HostPort &peer) const
{
    bool blocked;
    {
        bthread::RWLockRdGuard lock(mutex_);
        auto it = states_.find(peer);
        blocked = !IsGlobalWritableLocked(peer) || (it != states_.end() && ShouldBlock(it->second));
    }
    if (!blocked) {
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

void PeerUbAdmission::EnableVerifiedPortHealth()
{
    verificationMode_.store(UbPortHealthVerificationMode::VERIFIED_PORT_HEALTH, std::memory_order_release);
}

void PeerUbAdmission::SetSelfPortHealthRefreshTrigger(std::function<void()> trigger)
{
    bthread::RWLockWrGuard lock(mutex_);
    selfPortHealthRefreshTrigger_ = std::move(trigger);
}

void PeerUbAdmission::ReportOutcome(const UbOpOutcome &outcome)
{
    ReportOutcomeImpl(outcome, std::nullopt);
}

void PeerUbAdmission::SetRemotePortHealthCapability(const HostPort &peer, bool enabled,
                                                    const std::string &incarnation)
{
    if (peer.Empty()) {
        return;
    }
    bthread::RWLockWrGuard lock(mutex_);
    if (enabled) {
        const auto current = remotePortHealthPeers_.find(peer);
        const bool changed = current == remotePortHealthPeers_.end() || current->second != incarnation;
        remotePortHealthPeers_[peer] = incarnation;
        auto state = states_.find(peer);
        if (changed && state != states_.end() && state->second.probeInFlight) {
            state->second.probeInFlight = false;
            ++state->second.epoch;
        }
    } else {
        remotePortHealthPeers_.erase(peer);
    }
}

void PeerUbAdmission::ReconcileRemotePortHealthCapabilities(
    const std::unordered_map<HostPort, std::string> &incarnations)
{
    bthread::RWLockWrGuard lock(mutex_);
    for (auto iter = remotePortHealthPeers_.begin(); iter != remotePortHealthPeers_.end();) {
        auto current = incarnations.find(iter->first);
        iter = current == incarnations.end()
                       || (!iter->second.empty() && iter->second != current->second)
                   ? remotePortHealthPeers_.erase(iter)
                   : std::next(iter);
    }
}

void PeerUbAdmission::SetRemotePortHealthVerificationTrigger(RemotePortHealthVerificationTrigger trigger)
{
    auto callback = trigger ? std::make_shared<const RemotePortHealthVerificationTrigger>(std::move(trigger))
                            : nullptr;
    std::atomic_store(&remotePortHealthVerificationTrigger_, std::move(callback));
}

void PeerUbAdmission::ReportOutcomeImpl(const UbOpOutcome &outcome, std::optional<LateCompletionFence> fence)
{
    auto failureClass = classifier_.Classify(outcome);
    if (failureClass == UbFailureClass::SUCCESS || failureClass == UbFailureClass::LOCAL_RESOURCE_PRESSURE
        || failureClass == UbFailureClass::NON_UB_FAILURE) {
        return;
    }

    bool changed = false;
    UbAdmissionState nextState = UbAdmissionState::UNAVAILABLE;
    std::function<void()> selfPortHealthRefreshTrigger;
    std::shared_ptr<const RemotePortHealthVerificationTrigger> remoteVerificationTrigger;
    if (!PrepareOutcomeTransition(outcome, failureClass, fence, changed, nextState,
                                  selfPortHealthRefreshTrigger, remoteVerificationTrigger)) {
        return;
    }
    if (selfPortHealthRefreshTrigger) {
        try {
            selfPortHealthRefreshTrigger();
        } catch (const std::exception &e) {
            LOG(ERROR) << "UB self port-health refresh trigger threw: " << e.what();
        } catch (...) {
            LOG(ERROR) << "UB self port-health refresh trigger threw an unknown exception";
        }
    }
    InvokeRemoteVerificationTrigger(remoteVerificationTrigger, outcome.peer);
    if (!changed) {
        return;
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

bool PeerUbAdmission::PrepareOutcomeTransition(
    const UbOpOutcome &outcome, UbFailureClass failureClass,
    const std::optional<LateCompletionFence> &fence, bool &changed,
    UbAdmissionState &nextState, std::function<void()> &selfPortHealthRefreshTrigger,
    std::shared_ptr<const RemotePortHealthVerificationTrigger> &remoteVerificationTrigger)
{
    bthread::RWLockWrGuard lock(mutex_);
    if (fence.has_value() && !IsLateCompletionFenceCurrentLocked(outcome.peer, *fence)) {
        return false;
    }
    const bool portTrigger = IsPortHealthTrigger(failureClass);
    const bool selfPortTrigger = portTrigger && outcome.peer == self_ && UsesVerifiedPortHealth();
    const bool remotePortTrigger = portTrigger && outcome.peer != self_
                                   && HasRemotePortHealthCapabilityLocked(outcome.peer);
    const bool verifiedPortTrigger = selfPortTrigger || remotePortTrigger;
    nextState = (verifiedPortTrigger || failureClass == UbFailureClass::TIMEOUT_SUSPECT
                 || failureClass == UbFailureClass::CONNECT_OR_PATH_FAILURE)
                    ? UbAdmissionState::SUSPECT
                    : UbAdmissionState::UNAVAILABLE;
    changed = verifiedPortTrigger ? RecordPortVerificationTriggerLocked(outcome, failureClass)
                                  : UpdatePathStateLocked(outcome, failureClass, nextState);
    selfPortHealthRefreshTrigger = selfPortTrigger ? selfPortHealthRefreshTrigger_ : std::function<void()>{};
    remoteVerificationTrigger = remotePortTrigger ? std::atomic_load(&remotePortHealthVerificationTrigger_) : nullptr;
    return true;
}

bool PeerUbAdmission::IsApplicablePortHealth(const HostPort &subject, const UbPortHealthSummary &summary,
                                             UbPortHealthEvidenceSource source) const
{
    if (!HasKnownUbPortHealth(summary)) {
        return false;
    }
    const bool selfSubject = !self_.Empty() && subject == self_;
    if (selfSubject && !UsesVerifiedPortHealth()) {
        return false;
    }
    if (!selfSubject && !CanUpdateRemoteUbAdmission(source, summary)) {
        return false;
    }
    return true;
}

bool PeerUbAdmission::ApplyPortHealth(const HostPort &subject, const UbPortHealthSummary &summary,
                                      UbPortHealthEvidenceSource source)
{
    if (!IsApplicablePortHealth(subject, summary, source)) {
        return false;
    }
    PortHealthApplyResult result;
    {
        bthread::RWLockWrGuard lock(mutex_);
        auto stateIter = states_.find(subject);
        const auto *current = stateIter == states_.end() ? nullptr : &stateIter->second;
        if (!IsApplicablePassiveRecovery(source, current, summary)) {
            return false;
        }
        auto &state = stateIter == states_.end() ? states_[subject] : stateIter->second;
        const auto nextState = summary.badPortCount == summary.totalPortCount ? UbAdmissionState::UNAVAILABLE
                                                                               : UbAdmissionState::AVAILABLE;
        const auto previousState = state.state;
        result.previousState = previousState;
        if (state.portHealth.has_value()) {
            result.previousBadPortCount = state.portHealth->badPortCount;
        }
        if (!CanApplyPortHealthUpdate(state, summary, nextState)) {
            return false;
        }
        if (summary.verificationPending) {
            state.portHealth = summary;
            ++state.epoch;
            return true;
        }
        result.isolated = nextState == UbAdmissionState::UNAVAILABLE;
        result.applied = ApplyPortHealthFactLocked(subject, state, summary, nextState);
        result.isolatedTransition = result.applied && previousState != UbAdmissionState::UNAVAILABLE
                                    && nextState == UbAdmissionState::UNAVAILABLE;
        result.recovered = result.applied && previousState == UbAdmissionState::UNAVAILABLE
                           && nextState == UbAdmissionState::AVAILABLE;
    }
    LogPortHealthApply(subject, summary, source, result);
    return result.applied;
}

bool PeerUbAdmission::RecordPortVerificationTriggerLocked(const UbOpOutcome &outcome, UbFailureClass failureClass)
{
    auto &state = states_[outcome.peer];
    if (state.portHealthGoverned) {
        return false;
    }
    if (state.state == UbAdmissionState::UNAVAILABLE || state.state == UbAdmissionState::PROBING) {
        return false;
    }
    // Available or suspect subjects open or refresh a non-blocking verification window; fresh evidence bumps the
    // epoch so a stale in-flight legacy path probe cannot complete and clear the suspicion.
    state.lastStatus = outcome.status;
    state.lastFailureClass = failureClass;
    state.providerStatus = outcome.providerStatus;
    state.cqeStatus = outcome.cqeStatus;
    state.state = UbAdmissionState::SUSPECT;
    state.probeInFlight = false;
    state.backoffLevel = std::max(state.backoffLevel, 1U);
    state.backoffDeadlineMs = GetSteadyClockTimeStampMs() + ProbeBackoffMs(state.backoffLevel);
    ++state.epoch;
    return true;
}

bool PeerUbAdmission::ApplyPortHealthFactLocked(const HostPort &subject, UbPathState &state,
                                                const UbPortHealthSummary &summary, UbAdmissionState nextState)
{
    const bool stateChanged = state.state != nextState;
    state.lastStatus = Status::OK();
    state.lastFailureClass = UbFailureClass::SUCCESS;
    state.providerStatus.reset();
    state.cqeStatus.reset();
    state.state = nextState;
    state.probeInFlight = false;
    state.backoffLevel = 0;
    state.backoffDeadlineMs = 0;
    state.portHealth = summary;
    state.portHealthGoverned = summary.badPortCount == summary.totalPortCount;
    ++state.epoch;
    if (stateChanged && subject == self_) {
        lateCompletionGeneration_.fetch_add(1, std::memory_order_acq_rel);
    }
    if (stateChanged) {
        AdvancePeerCompletionGenerationLocked(subject);
    }
    return true;
}

void PeerUbAdmission::ReplaceGlobalSummaries(const std::vector<UbHealthSummary> &summaries)
{
    constexpr size_t MAX_RETIRED_INCARNATIONS_PER_WORKER = 8;
    std::vector<GlobalSummaryChange> changes;
    GlobalSummarySyncStats stats;
    stats.input = summaries.size();
    const HostPort receiver = self_;
    {
        bthread::RWLockWrGuard lock(mutex_);
        const auto nowMs = GetSteadyClockTimeStampMs();
        std::unordered_map<HostPort, UbHealthSummary> replacement;
        replacement.reserve(summaries.size());
        for (const auto &summary : summaries) {
            if (summary.worker.Empty() || summary.incarnation.empty()) {
                ++stats.invalidIdentity;
                continue;
            }
            if (topologyInitialized_ && topologyWorkers_.count(summary.worker) == 0) {
                ++stats.nonMember;
                continue;
            }
            if (IsReplayLocked(summary.worker, summary.incarnation)) {
                ++stats.replay;
                continue;
            }
            auto latest = latestGlobalIncarnations_.find(summary.worker);
            if (latest == latestGlobalIncarnations_.end()) {
                latestGlobalIncarnations_.emplace(summary.worker, summary.incarnation);
            } else if (latest->second != summary.incarnation) {
                auto &retired = retiredGlobalIncarnations_[summary.worker];
                if (retired.count(summary.incarnation) != 0) {
                    ++stats.retiredIncarnation;
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
                ++stats.staleEpoch;
                candidate = current->second;
            }
            auto [iter, inserted] = replacement.emplace(summary.worker, candidate);
            if (!inserted) {
                ++stats.duplicate;
                if (iter->second.incarnation == candidate.incarnation && candidate.epoch > iter->second.epoch) {
                    iter->second = candidate;
                }
            }
            ApplyGlobalRecoveryTransitionLocked(iter->second, nowMs);
        }

        for (const auto &[worker, summary] : replacement) {
            auto current = globalSummaries_.find(worker);
            if (current == globalSummaries_.end()) {
                if (!summary.writable) {
                    changes.emplace_back(
                        CaptureGlobalSummaryChange(GlobalSummaryTransition::QUARANTINE_APPLIED, summary));
                }
                continue;
            }
            if (current->second.incarnation != summary.incarnation) {
                changes.emplace_back(
                    CaptureGlobalSummaryChange(GlobalSummaryTransition::INCARNATION_REPLACED, summary));
            } else if (current->second.writable && !summary.writable) {
                changes.emplace_back(
                    CaptureGlobalSummaryChange(GlobalSummaryTransition::QUARANTINE_APPLIED, summary));
            } else if (!current->second.writable && summary.writable) {
                changes.emplace_back(CaptureGlobalSummaryChange(GlobalSummaryTransition::RECOVERY_APPLIED, summary));
            } else if (!summary.writable && !IsSameGlobalAdmissionView(current->second, summary)) {
                changes.emplace_back(CaptureGlobalSummaryChange(GlobalSummaryTransition::QUARANTINE_UPDATED, summary));
            }
        }
        for (const auto &[worker, summary] : globalSummaries_) {
            if (!summary.writable && replacement.count(worker) == 0) {
                changes.emplace_back(CaptureGlobalSummaryChange(GlobalSummaryTransition::SUMMARY_REMOVED, summary));
            }
        }
        globalSummaries_ = std::move(replacement);
        stats.effective = globalSummaries_.size();
        INJECT_POINT_NO_RETURN("PeerUbAdmission.ReplaceGlobalSummaries.afterCommit");
    }

    const auto receiverForLog = receiver.Empty() ? std::string("unknown") : receiver.ToString();
    for (const auto &change : changes) {
        const auto incarnationPrefix =
            FormatUbHealthIncarnationPrefix(change.incarnationBytes.data(), change.incarnationSize);
        LOG(INFO) << "UB_HEALTH_SUMMARY action=global_summary_applied receiver=" << receiverForLog
                  << " target=" << change.worker
                  << " transition=" << GlobalSummaryTransitionName(change.transition)
                  << " incarnation_prefix=" << incarnationPrefix.data() << " epoch=" << change.epoch
                  << " writable=" << change.writable << " state_code=" << static_cast<int>(change.state)
                  << " reason_code=" << static_cast<int>(change.reason)
                  << " status_name=" << Status::StatusCodeName(change.lastStatusCode);
    }
    VLOG(1) << "UB health summary snapshot synchronized, receiver=" << receiverForLog << ", input=" << stats.input
            << ", effective=" << stats.effective << ", changes=" << changes.size()
            << ", invalidIdentity=" << stats.invalidIdentity << ", nonMember=" << stats.nonMember
            << ", replay=" << stats.replay << ", retiredIncarnation=" << stats.retiredIncarnation
            << ", staleEpoch=" << stats.staleEpoch << ", duplicate=" << stats.duplicate;
}

void PeerUbAdmission::InitializeVerification(const HostPort &peer, uint64_t nowMs)
{
    if (peer.Empty()) {
        return;
    }
    bthread::RWLockWrGuard lock(mutex_);
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
    bthread::RWLockWrGuard lock(mutex_);
    auto iter = states_.find(peer);
    if (iter == states_.end()) {
        return std::nullopt;
    }
    auto &state = iter->second;
    if (IsPortHealthManagedState(peer, state)) {
        return std::nullopt;
    }
    if (state.state != UbAdmissionState::UNAVAILABLE && state.state != UbAdmissionState::SUSPECT
        && state.state != UbAdmissionState::PROBING) {
        return std::nullopt;
    }
    if (!TryBeginUbProbe(nowMs, state.backoffDeadlineMs, state.probeInFlight, state.epoch)) {
        return std::nullopt;
    }
    if (state.state != UbAdmissionState::SUSPECT) {
        state.state = UbAdmissionState::PROBING;
    }
    return UbProbeToken{ peer, state.epoch };
}

bool PeerUbAdmission::CancelProbe(const UbProbeToken &token, uint64_t nowMs)
{
    bthread::RWLockWrGuard lock(mutex_);
    auto iter = states_.find(token.peer);
    if (iter == states_.end()
        || (iter->second.state != UbAdmissionState::PROBING
            && iter->second.state != UbAdmissionState::SUSPECT)
        || !MatchesUbProbe(iter->second.probeInFlight, iter->second.epoch, token.epoch)) {
        return false;
    }
    auto &state = iter->second;
    const bool softFailure = state.lastFailureClass == UbFailureClass::TIMEOUT_SUSPECT
                             || state.lastFailureClass == UbFailureClass::CONNECT_OR_PATH_FAILURE
                             || IsPortHealthManagedState(token.peer, state);
    state.state = softFailure ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
    state.probeInFlight = false;
    ApplyProbeRetryBackoff(state, nowMs);
    ++state.epoch;
    return true;
}

bool PeerUbAdmission::CompleteProbe(const UbProbeToken &token, const Status &status, uint64_t nowMs,
                                    bool requireGlobalAvailable)
{
    // Diagnostic writes do not carry node-health authority for failures managed by port verification.
    std::shared_ptr<const RemotePortHealthVerificationTrigger> verifier;
    std::function<void()> refreshSelf;
    {
        bthread::RWLockWrGuard lock(mutex_);
        auto state = states_.find(token.peer);
        const bool portHealthManaged = state != states_.end()
                                       && IsPortHealthManagedState(token.peer, state->second);
        verifier = token.peer != self_ && portHealthManaged
                       ? std::atomic_load(&remotePortHealthVerificationTrigger_) : nullptr;
        if (token.peer == self_ && portHealthManaged && UsesVerifiedPortHealth()) {
            refreshSelf = selfPortHealthRefreshTrigger_;
        }
        if (verifier != nullptr || refreshSelf) {
            if (state == states_.end() ||
                !MatchesUbProbe(state->second.probeInFlight, state->second.epoch, token.epoch)) {
                return false;
            }
            state->second.probeInFlight = false;
            // A verifier-owned verdict yields no admission decision here; without the backoff
            // NextProbeCandidate re-arms on the next scheduler turn and the probe loop never sleeps.
            ApplyProbeRetryBackoff(state->second, nowMs);
            ++state->second.epoch;
        }
    }
    if (verifier != nullptr || refreshSelf) {
        InvokeRemoteVerificationTrigger(verifier, token.peer);
        if (refreshSelf) {
            refreshSelf();
        }
        return false;
    }
    bool recoveredFromHardUnavailable = false;
    StatusCode previousStatusCode = K_OK;
    UbFailureClass previousFailureClass = UbFailureClass::SUCCESS;
    {
        bthread::RWLockWrGuard lock(mutex_);
        auto iter = states_.find(token.peer);
        if (iter == states_.end()
            || (iter->second.state != UbAdmissionState::PROBING
                && iter->second.state != UbAdmissionState::SUSPECT)
            || !MatchesUbProbe(iter->second.probeInFlight, iter->second.epoch, token.epoch)) {
            return false;
        }
        auto &state = iter->second;
        if (status.IsOk() && (!requireGlobalAvailable || IsGlobalWritableLocked(token.peer))) {
            // Only a probe that escalated a port/path failure from UNAVAILABLE to PROBING is a hard-unavailable
            // recovery; port-fact SUSPECT windows and soft suspects recover silently.
            recoveredFromHardUnavailable = state.state == UbAdmissionState::PROBING
                                           && IsHardUnavailableFailure(state.lastFailureClass);
            if (recoveredFromHardUnavailable) {
                previousStatusCode = state.lastStatus.GetCode();
                previousFailureClass = state.lastFailureClass;
            }
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
        } else {
            const bool softVerification = state.state == UbAdmissionState::SUSPECT;
            state.state = softVerification ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
            state.probeInFlight = false;
            state.lastStatus = status.IsOk() ? Status(K_NOT_READY, "Global UB health still denies recovery") : status;
            state.backoffLevel = std::min(state.backoffLevel + 1, MAX_PROBE_BACKOFF_LEVEL);
            state.backoffDeadlineMs = UbProbeRetryAt(nowMs, ProbeBackoffMs(state.backoffLevel));
            ++state.epoch;
            INJECT_POINT_NO_RETURN("PeerUbAdmission.CompleteProbe.failure");
            return false;
        }
    }
    if (recoveredFromHardUnavailable) {
        LOG(INFO) << "UB admission marked peer AVAILABLE, peer=" << token.peer
                  << ", statusCode=" << status.GetCode() << ", recoveredFrom=UNAVAILABLE"
                  << ", previousStatusCode=" << previousStatusCode
                  << ", previousFailureClass=" << static_cast<int>(previousFailureClass);
    }
    return true;
}

std::optional<HostPort> PeerUbAdmission::NextProbeCandidate(uint64_t nowMs) const
{
    bthread::RWLockRdGuard lock(mutex_);
    for (const auto &[peer, state] : states_) {
        if (IsPortHealthManagedState(peer, state)) {
            continue;
        }
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
    bthread::RWLockRdGuard lock(mutex_);
    return NextUbProbeDeadline(states_, [this](const auto &entry) -> std::optional<uint64_t> {
        const auto &[peer, state] = entry;
        const bool recoverable = state.state == UbAdmissionState::UNAVAILABLE
                                 || state.state == UbAdmissionState::SUSPECT
                                 || state.state == UbAdmissionState::PROBING;
        return !IsPortHealthManagedState(peer, state) && recoverable && !state.probeInFlight
                       && state.lastStatus.IsError()
                   ? std::optional<uint64_t>{ state.backoffDeadlineMs } : std::nullopt;
    });
}

void PeerUbAdmission::ReconcileTopologyWorkers(const std::unordered_set<HostPort> &workers, uint64_t nowMs,
                                               uint64_t cleanupGraceMs)
{
    bthread::RWLockWrGuard lock(mutex_);
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
        remotePortHealthPeers_.erase(worker);
        departedWorkers_.erase(worker);
    }
    PruneTombstonesLocked(nowMs);
}

void PeerUbAdmission::PruneExpiredTopologyState(uint64_t nowMs)
{
    bthread::RWLockWrGuard lock(mutex_);
    if (nextTombstoneExpiryMs_ != 0 && nowMs >= nextTombstoneExpiryMs_) {
        PruneTombstonesLocked(nowMs);
    }
}

UbHealthSummary PeerUbAdmission::BuildSelfHealthSummary(const HostPort &self) const
{
    bthread::RWLockRdGuard lock(mutex_);
    UbHealthSummary summary;
    summary.worker = self;
    auto it = states_.find(self);
    if (it == states_.end()) {
        return summary;
    }
    const auto &state = it->second;
    summary.portHealth = state.portHealth;
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
    bthread::RWLockRdGuard lock(mutex_);
    auto it = states_.find(peer);
    return it == states_.end() ? std::nullopt : std::optional<UbPathState>{ it->second };
}

PeerUbAdmissionStats PeerUbAdmission::GetStats() const
{
    bthread::RWLockRdGuard lock(mutex_);
    return PeerUbAdmissionStats{ states_.size(), globalSummaries_.size(), latestGlobalIncarnations_.size(),
                                 retiredGlobalIncarnations_.size(), departedWorkers_.size(),
                                 replayTombstones_.size(), peerCompletionGenerations_.size() };
}

void PeerUbAdmission::ClearLocalState(const HostPort &peer)
{
    bthread::RWLockWrGuard lock(mutex_);
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
    // A confirmed all-down state can only move through a newer port-health fact.
    if (state.portHealthGoverned) {
        return false;
    }
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
        bthread::RWLockWrGuard lock(mutex_);
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
            bthread::RWLockRdGuard lock(mutex_);
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

bool MergeUbPortHealth(const std::optional<UbPortHealthSummary> &current,
                       const std::optional<UbPortHealthSummary> &incoming,
                       std::optional<UbPortHealthSummary> &merged)
{
    if (incoming.has_value() && incoming->valid && !HasKnownUbPortHealth(*incoming)) {
        return false;
    }
    if (!incoming.has_value() || (current.has_value() && current->valid && !incoming->valid)) {
        merged = current;
        return true;
    }
    if (current.has_value() && current->valid && incoming->valid) {
        if (incoming->healthEpoch < current->healthEpoch) {
            merged = current;
            return true;
        }
        if (incoming->healthEpoch == current->healthEpoch
            && (incoming->totalPortCount != current->totalPortCount
                || incoming->badPortCount != current->badPortCount)) {
            return false;
        }
    }
    merged = incoming;
    return true;
}

bool MergeUbHealthSummary(const UbHealthSummary *current, const UbHealthSummary &incoming,
                          UbHealthSummary &merged)
{
    if (incoming.worker.Empty() || incoming.incarnation.empty()) {
        return false;
    }
    const bool sameIncarnation = current != nullptr && current->worker == incoming.worker
                                 && current->incarnation == incoming.incarnation;
    std::optional<UbPortHealthSummary> ports;
    if (!MergeUbPortHealth(sameIncarnation ? current->portHealth : std::nullopt, incoming.portHealth, ports)) {
        return false;
    }
    merged = sameIncarnation && incoming.epoch < current->epoch ? *current : incoming;
    merged.portHealth = std::move(ports);
    return true;
}

bool UbHealthSummaryCache::Snapshot::Prepare(const UbHealthSummary &summary,
                                             const std::string &expectedIncarnation, UbHealthSummary &accepted) const
{
    if (summary.incarnation != expectedIncarnation) {
        return false;
    }
    auto retired = retiredIncarnations_.find(summary.worker);
    return (retired == retiredIncarnations_.end() || retired->second.count(summary.incarnation) == 0)
           && MergeUbHealthSummary(Find(summary.worker), summary, accepted);
}

const UbHealthSummary *UbHealthSummaryCache::Snapshot::Find(const HostPort &worker) const
{
    auto found = summaries_.find(worker);
    return found == summaries_.end() ? nullptr : &found->second;
}

void UbHealthSummaryCache::Snapshot::Retire(const HostPort &worker, const std::string &incarnation)
{
    auto &retired = retiredIncarnations_[worker];
    retired.emplace(incarnation);
    if (retired.size() > MAX_RETIRED_INCARNATIONS_PER_WORKER) {
        retired.erase(retired.begin());
    }
    auto current = summaries_.find(worker);
    if (current != summaries_.end() && current->second.incarnation == incarnation) {
        summaries_.erase(current);
    }
}

bool UbHealthSummaryCache::Snapshot::Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    UbHealthSummary accepted;
    if (!Prepare(summary, expectedIncarnation, accepted)) {
        return false;
    }
    const auto *current = Find(summary.worker);
    if (current != nullptr && IsSameUbHealthSummary(*current, accepted)) {
        return false;
    }
    if (current != nullptr && current->incarnation != accepted.incarnation) {
        Retire(summary.worker, current->incarnation);
    }
    summaries_[summary.worker] = std::move(accepted);
    return true;
}

bool UbHealthSummaryCache::Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    UbHealthSummary accepted;
    return Apply(summary, expectedIncarnation, accepted);
}

bool UbHealthSummaryCache::Apply(const UbHealthSummary &summary, const std::string &expectedIncarnation,
                                 UbHealthSummary &accepted)
{
    if (summary.worker.Empty() || summary.incarnation.empty() || summary.incarnation != expectedIncarnation) {
        return false;
    }
    {
        bthread::RWLockRdGuard lock(mutex_);
        const auto *current = state_.Find(summary.worker);
        if (current != nullptr && IsSameUbHealthSummary(*current, summary)) {
            return false;
        }
    }
    bthread::RWLockWrGuard lock(mutex_);
    const auto *current = state_.Find(summary.worker);
    // Standalone cache admission updates require a newer epoch; passive Registry/RPC observations do not.
    bool applied;
    if (current != nullptr && current->incarnation == summary.incarnation && current->epoch == summary.epoch) {
        auto retainedAdmission = *current;
        retainedAdmission.portHealth = summary.portHealth;
        applied = state_.Apply(retainedAdmission, expectedIncarnation);
    } else {
        applied = state_.Apply(summary, expectedIncarnation);
    }
    if (applied) {
        accepted = *state_.Find(summary.worker);
    }
    return applied;
}

void UbHealthSummaryCache::Snapshot::ReconcileWorkers(const std::unordered_set<HostPort> &workers)
{
    for (auto iter = summaries_.begin(); iter != summaries_.end();) {
        iter = workers.count(iter->first) == 0 ? summaries_.erase(iter) : std::next(iter);
    }
    for (auto iter = retiredIncarnations_.begin(); iter != retiredIncarnations_.end();) {
        iter = workers.count(iter->first) == 0 ? retiredIncarnations_.erase(iter) : std::next(iter);
    }
}

void UbHealthSummaryCache::ReconcileWorkers(const std::unordered_set<HostPort> &workers)
{
    bthread::RWLockWrGuard lock(mutex_);
    state_.ReconcileWorkers(workers);
}

std::optional<UbHealthSummary> UbHealthSummaryCache::Get(const HostPort &worker) const
{
    bthread::RWLockRdGuard lock(mutex_);
    const auto *summary = state_.Find(worker);
    return summary == nullptr ? std::nullopt : std::optional<UbHealthSummary>{ *summary };
}

size_t UbHealthSummaryCache::Size() const
{
    bthread::RWLockRdGuard lock(mutex_);
    return state_.Size();
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

void PeerUbAdmission::ApplyProbeRetryBackoff(UbPathState &state, uint64_t nowMs)
{
    state.backoffLevel = std::max(state.backoffLevel, 1U);
    state.backoffDeadlineMs = UbProbeRetryAt(nowMs, ProbeBackoffMs(state.backoffLevel));
}

bool PeerUbAdmission::UsesVerifiedPortHealth() const
{
    return verificationMode_.load(std::memory_order_acquire)
           == UbPortHealthVerificationMode::VERIFIED_PORT_HEALTH;
}

bool PeerUbAdmission::IsPortHealthManagedState(const HostPort &subject, const UbPathState &state) const
{
    if (state.portHealthGoverned || !IsPortHealthTrigger(state.lastFailureClass)) {
        return state.portHealthGoverned;
    }
    return (!self_.Empty() && subject == self_ && UsesVerifiedPortHealth())
           || (subject != self_ && HasRemotePortHealthCapabilityLocked(subject));
}

bool PeerUbAdmission::HasRemotePortHealthCapabilityLocked(const HostPort &peer) const
{
    // A bound verifier handles first-contact E4/E9 even before a passive summary advertises port facts.
    if (std::atomic_load(&remotePortHealthVerificationTrigger_) != nullptr
        || remotePortHealthPeers_.count(peer) != 0) {
        return true;
    }
    auto state = states_.find(peer);
    return state != states_.end() && state->second.portHealth.has_value();
}

bool PeerUbAdmission::IsGlobalWritableLocked(const HostPort &peer) const
{
    // The lease contains this process's own last published summary. Treating that echo as an
    // external recovery fence creates a cycle: a local failure publishes writable=false, then
    // the same stale fact prevents the successful self probe from publishing writable=true.
    if (!self_.Empty() && peer == self_) {
        return true;
    }
    if (std::atomic_load(&remotePortHealthVerificationTrigger_) != nullptr) {
        return true;
    }
    auto global = globalSummaries_.find(peer);
    return global == globalSummaries_.end() || IsQueryAuthoritativePortHealthSummary(global->second)
           || global->second.writable;
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
    if (std::atomic_load(&remotePortHealthVerificationTrigger_) != nullptr
        || IsQueryAuthoritativePortHealthSummary(summary)) {
        return;
    }
    auto local = states_.find(summary.worker);
    if (local == states_.end()) {
        return;
    }
    auto &state = local->second;
    if (state.portHealthGoverned) {
        // Recovery summaries only notify; a port-fact-isolated subject recovers through ApplyPortHealth.
        return;
    }
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
