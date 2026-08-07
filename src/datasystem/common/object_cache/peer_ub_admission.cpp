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
    auto global = globalSummaries_.find(peer);
    if (global != globalSummaries_.end() && !global->second.writable) {
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
    auto global = globalSummaries_.find(peer);
    if (global != globalSummaries_.end() && !global->second.writable) {
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
    // Write-once during construction, read-only afterwards: no lock is required.
    self_ = self;
}

void PeerUbAdmission::ReportOutcome(const UbOpOutcome &outcome)
{
    // self_ is write-once (set during construction), so this check does not need the admission lock.
    if (!self_.Empty() && outcome.peer == self_) {
        INJECT_POINT_NO_RETURN("PeerUbAdmission.ReportOutcome.self_skipped");
        return;
    }
    auto failureClass = classifier_.Classify(outcome);
    if (failureClass == UbFailureClass::SUCCESS || failureClass == UbFailureClass::LOCAL_RESOURCE_PRESSURE
        || failureClass == UbFailureClass::NON_UB_FAILURE) {
        return;
    }

    const auto nextState =
        failureClass == UbFailureClass::TIMEOUT_SUSPECT ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        auto &state = states_[outcome.peer];
        // A timeout is lower-confidence evidence than an explicit provider/CQE
        // failure. Once hard evidence (or its recovery probe) has quarantined a
        // peer, a late timeout from an older in-flight request must not reopen
        // the SUSPECT -> UNAVAILABLE oscillation or invalidate the probe token.
        if (failureClass == UbFailureClass::TIMEOUT_SUSPECT
            && (state.state == UbAdmissionState::UNAVAILABLE || state.state == UbAdmissionState::PROBING)) {
            return;
        }
        if (state.state == nextState) {
            return;
        }
        state.lastStatus = outcome.status;
        state.lastFailureClass = failureClass;
        state.providerStatus = outcome.providerStatus;
        state.cqeStatus = outcome.cqeStatus;
        state.state = nextState;
        if (state.backoffLevel == 0) {
            state.backoffLevel = 1;
        }
        state.backoffDeadlineMs = GetSteadyClockTimeStampMs() + ProbeBackoffMs(state.backoffLevel);
        ++state.epoch;
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

void PeerUbAdmission::InitializeProbing(const HostPort &peer, uint64_t nowMs)
{
    if (peer.Empty()) {
        return;
    }
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto &state = states_[peer];
    state.state = UbAdmissionState::PROBING;
    state.lastStatus = Status(K_NOT_READY, "UB data plane requires recovery probe");
    state.lastFailureClass = UbFailureClass::CONNECT_OR_PATH_FAILURE;
    state.backoffLevel = 0;
    state.backoffDeadlineMs = nowMs;
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
    if (state.state == UbAdmissionState::PROBING && state.lastStatus.IsOk()) {
        return std::nullopt;
    }
    state.state = UbAdmissionState::PROBING;
    state.lastStatus = Status::OK();
    ++state.epoch;
    return UbProbeToken{ peer, state.epoch };
}

bool PeerUbAdmission::CompleteProbe(const UbProbeToken &token, const Status &status, uint64_t nowMs,
                                    bool requireGlobalAvailable)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    auto iter = states_.find(token.peer);
    if (iter == states_.end() || iter->second.state != UbAdmissionState::PROBING
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
        state.providerStatus.reset();
        state.cqeStatus.reset();
        ++state.epoch;
        INJECT_POINT_NO_RETURN("PeerUbAdmission.CompleteProbe.success");
        return true;
    }
    state.state = UbAdmissionState::UNAVAILABLE;
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
        if (recoverable && state.lastStatus.IsError() && nowMs >= state.backoffDeadlineMs) {
            return peer;
        }
    }
    return std::nullopt;
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
                                 replayTombstones_.size() };
}

void PeerUbAdmission::ClearLocalState(const HostPort &peer)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    states_.erase(peer);
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
        if (state.state == UbAdmissionState::PROBING && state.lastStatus.IsOk()) {
            state.state = UbAdmissionState::UNAVAILABLE;
            state.lastStatus = Status(K_NOT_READY, "Global UB health denies recovery");
            ++state.epoch;
        }
        return;
    }
    if (state.state == UbAdmissionState::UNAVAILABLE) {
        state.state = UbAdmissionState::PROBING;
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
