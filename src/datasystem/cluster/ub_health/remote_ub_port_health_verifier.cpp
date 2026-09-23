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

#include "datasystem/cluster/ub_health/remote_ub_port_health_verifier.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <random>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/util/random_data.h"

namespace datasystem::cluster {
namespace {

constexpr auto SEED_WORD_BITS = std::numeric_limits<uint32_t>::digits;

bool IsValidRetryInterval(uint64_t retryMinMs, uint64_t retryMaxMs) noexcept
{
    return retryMinMs >= UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS
           && retryMaxMs <= UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS && retryMinMs <= retryMaxMs;
}

uint64_t RetryMinOrDefault(uint64_t retryMinMs, uint64_t retryMaxMs) noexcept
{
    return IsValidRetryInterval(retryMinMs, retryMaxMs) ? retryMinMs : UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS;
}

uint64_t RetryMaxOrDefault(uint64_t retryMinMs, uint64_t retryMaxMs) noexcept
{
    return IsValidRetryInterval(retryMinMs, retryMaxMs) ? retryMaxMs : UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS;
}

bool IsUsableQuerySummary(const RemoteUbQueryTicket &ticket, const UbHealthSummary &summary)
{
    return summary.worker == ticket.peer && summary.incarnation == ticket.incarnation
           && summary.portHealth.has_value() && HasKnownUbPortHealth(*summary.portHealth)
           && !summary.portHealth->verificationPending;
}

bool IsStaleOrConflicting(const std::optional<UbPortHealthSummary> &current,
                          const UbPortHealthSummary &incoming)
{
    if (!current.has_value()) {
        return false;
    }
    return incoming.healthEpoch < current->healthEpoch
           || (incoming.healthEpoch == current->healthEpoch
               && (incoming.totalPortCount != current->totalPortCount
                   || incoming.badPortCount != current->badPortCount));
}

Status ValidateQueryCompletion(const RemoteUbQueryTicket &ticket,
                               const std::optional<UbHealthSummary> &summary,
                               const Status &queryStatus,
                               const std::optional<UbPortHealthSummary> &lastPortHealth)
{
    if (queryStatus.IsError()) {
        return queryStatus;
    }
    if (!summary.has_value()) {
        return Status(K_INVALID, "UB port health query response has no summary");
    }
    if (!IsUsableQuerySummary(ticket, *summary)) {
        return Status(K_INVALID, "UB port health query response is unusable");
    }
    if (IsStaleOrConflicting(lastPortHealth, *summary->portHealth)) {
        return Status(K_NOT_READY, "UB port health query response is stale or conflicting");
    }
    return Status::OK();
}

void LogQueryRetry(const RemoteUbQueryTicket &ticket, const Status &status, uint64_t nextRetryMs)
{
    LOG(WARNING) << "UB_PORT_QUERY action=retry peer=" << ticket.peer.ToString()
                 << " incarnation_prefix=" << FormatUbHealthIncarnationPrefix(ticket.incarnation)
                 << " status_code=" << status.GetCode() << " status=" << status
                 << " next_retry_ms=" << nextRetryMs;
}

void LogQueryResponse(const RemoteUbQueryTicket &ticket, const UbPortHealthSummary &portHealth,
                      const char *decision)
{
    LOG(INFO) << "UB_PORT_QUERY action=response peer=" << ticket.peer.ToString()
              << " incarnation_prefix=" << FormatUbHealthIncarnationPrefix(ticket.incarnation)
              << " health_epoch=" << portHealth.healthEpoch << " bad=" << portHealth.badPortCount
              << " total=" << portHealth.totalPortCount << " decision=" << decision
              << " source=query_response";
}

}  // namespace

RemoteUbPortHealthVerifier::RemoteUbPortHealthVerifier()
    : RemoteUbPortHealthVerifier(RandomData::GetRandomSeed(), UB_REMOTE_PORT_HEALTH_RETRY_MIN_MS,
                                 UB_REMOTE_PORT_HEALTH_RETRY_MAX_MS)
{
}

RemoteUbPortHealthVerifier::RemoteUbPortHealthVerifier(uint64_t seed, uint64_t retryMinMs,
                                                       uint64_t retryMaxMs) noexcept
    : seed_(seed), retryMinMs_(RetryMinOrDefault(retryMinMs, retryMaxMs)),
      retryMaxMs_(RetryMaxOrDefault(retryMinMs, retryMaxMs))
{
}

bool RemoteUbPortHealthVerifier::RequestVerification(const HostPort &peer, const std::string &incarnation,
                                                     uint64_t nowMs)
{
    if (peer.Empty() || incarnation.empty()) {
        return false;
    }
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto [iter, inserted] = peers_.try_emplace(peer);
    if (inserted) {
        iter->second.incarnation = incarnation;
        iter->second.nextQueryMs = nowMs;
        iter->second.verificationPending = true;
        return true;
    }
    auto &state = iter->second;
    if (state.incarnation != incarnation) {
        state = PeerState{};
        state.incarnation = incarnation;
        state.nextQueryMs = nowMs;
        state.verificationPending = true;
        return true;
    }
    if (state.inFlight) {
        const bool firstPendingTrigger = !state.triggerPending;
        state.triggerPending = true;
        return firstPendingTrigger;
    }
    if (!state.inFlight && !state.isolated
        && state.nextQueryMs == std::numeric_limits<uint64_t>::max()) {
        state.verificationPending = true;
        state.nextQueryMs = state.lastQueryMs.has_value()
                                ? std::max(nowMs, UbProbeRetryAt(*state.lastQueryMs, retryMinMs_))
                                : nowMs;
        return true;
    }
    return false;
}

std::optional<RemoteUbQueryTicket> RemoteUbPortHealthVerifier::TryBeginDue(uint64_t nowMs)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto selected = peers_.end();
    for (auto iter = peers_.begin(); iter != peers_.end(); ++iter) {
        if (iter->second.inFlight || iter->second.nextQueryMs > nowMs) {
            continue;
        }
        if (selected == peers_.end() || iter->second.nextQueryMs < selected->second.nextQueryMs ||
            (iter->second.nextQueryMs == selected->second.nextQueryMs && iter->first < selected->first)) {
            selected = iter;
        }
    }
    if (selected == peers_.end()) {
        return std::nullopt;
    }
    auto &state = selected->second;
    if (state.inFlight || nowMs < state.nextQueryMs) {
        return std::nullopt;
    }
    state.inFlight = true;
    state.generation = NextGenerationLocked();
    state.lastQueryMs = nowMs;
    state.nextQueryMs = std::numeric_limits<uint64_t>::max();
    return RemoteUbQueryTicket{ selected->first, state.incarnation, state.generation };
}

uint64_t RemoteUbPortHealthVerifier::NextGenerationLocked()
{
    if (++nextGeneration_ == 0) {
        ++nextGeneration_;
    }
    return nextGeneration_;
}

uint64_t RemoteUbPortHealthVerifier::RetryDeadlineMs(const HostPort &peer, const PeerState &state, uint64_t nowMs) const
{
    const uint64_t address = std::hash<HostPort>{}(peer);
    const uint64_t incarnation = std::hash<std::string>{}(state.incarnation);
    std::seed_seq sequence{ static_cast<uint32_t>(seed_),
                            static_cast<uint32_t>(seed_ >> SEED_WORD_BITS),
                            static_cast<uint32_t>(address),
                            static_cast<uint32_t>(address >> SEED_WORD_BITS),
                            static_cast<uint32_t>(incarnation),
                            static_cast<uint32_t>(incarnation >> SEED_WORD_BITS),
                            static_cast<uint32_t>(state.generation),
                            static_cast<uint32_t>(state.generation >> SEED_WORD_BITS) };
    std::mt19937_64 generator(sequence);
    return UbProbeRetryAt(nowMs, std::uniform_int_distribution<uint64_t>(retryMinMs_, retryMaxMs_)(generator));
}

void RemoteUbPortHealthVerifier::ScheduleAfterCompletion(const HostPort &peer, PeerState &state, uint64_t nowMs,
                                                         RemoteUbQueryCompletion &completion) const
{
    const bool pending =
        state.isolated || state.verificationPending || state.triggerPending || state.summaryHintPending;
    const uint64_t next =
        pending ? RetryDeadlineMs(peer, state, nowMs) : std::numeric_limits<uint64_t>::max();
    state.verificationPending = pending;
    state.summaryHintPending = false;
    state.triggerPending = false;
    completion.retryScheduled = pending;
    state.nextQueryMs = next;
}

RemoteUbPortHealthVerifier::AcceptedSummaryTransition RemoteUbPortHealthVerifier::ApplyAcceptedSummaryLocked(
    const HostPort &peer, PeerState &state, const UbPortHealthSummary &portHealth, uint64_t nowMs,
    RemoteUbQueryCompletion &completion) const
{
    const bool healthChanged = !state.lastPortHealth.has_value()
                               || !IsSameUbPortHealth(*state.lastPortHealth, portHealth);
    const bool recoveredAfterRetry = state.lastLoggedRetryStatus.has_value();
    const bool wasIsolated = state.isolated;
    const bool triggerPending = state.triggerPending;
    state.triggerPending = false;
    state.summaryHintPending = false;
    state.lastLoggedRetryStatus.reset();
    state.lastPortHealth = portHealth;
    state.isolated = ShouldIsolateForUbPortHealth(portHealth);
    state.verificationPending = state.isolated || triggerPending;
    completion.evidenceAccepted = true;
    if (state.verificationPending) {
        ScheduleAfterCompletion(peer, state, nowMs, completion);
    } else {
        state.nextQueryMs = std::numeric_limits<uint64_t>::max();
    }
    const bool logResponse = healthChanged || recoveredAfterRetry || wasIsolated != state.isolated;
    const char *decision = state.isolated ? "ISOLATE" : (wasIsolated ? "RECOVER" : "ALLOW");
    return { logResponse, decision };
}

RemoteUbQueryCompletion RemoteUbPortHealthVerifier::Complete(
    const RemoteUbQueryTicket &ticket, const std::optional<UbHealthSummary> &summary,
    const Status &queryStatus, uint64_t nowMs)
{
    std::unique_lock<bthread::Mutex> lock(mutex_);
    auto iter = peers_.find(ticket.peer);
    if (iter == peers_.end() || iter->second.incarnation != ticket.incarnation
        || !MatchesUbProbe(iter->second.inFlight, iter->second.generation, ticket.generation)) {
        return {};
    }

    auto &state = iter->second;
    state.inFlight = false;
    RemoteUbQueryCompletion completion;
    auto completionStatus = ValidateQueryCompletion(ticket, summary, queryStatus, state.lastPortHealth);
    if (completionStatus.IsError()) {
        if (completionStatus.GetCode() == K_NOT_SUPPORTED) {
            state.summaryHintPending = false;
            state.triggerPending = false;
            state.nextQueryMs = UbProbeRetryAt(nowMs, REMOTE_UB_PORT_HEALTH_UNSUPPORTED_RETRY_INTERVAL_MS);
            completion.retryScheduled = true;
        } else {
            ScheduleAfterCompletion(ticket.peer, state, nowMs, completion);
        }
        const bool logRetry = completion.retryScheduled
                              && (!state.lastLoggedRetryStatus.has_value()
                                  || *state.lastLoggedRetryStatus != completionStatus.GetCode());
        if (logRetry) {
            state.lastLoggedRetryStatus = completionStatus.GetCode();
        }
        const uint64_t nextRetryMs = state.nextQueryMs > nowMs ? state.nextQueryMs - nowMs : 0;
        lock.unlock();
        if (logRetry) {
            LogQueryRetry(ticket, completionStatus, nextRetryMs);
        }
        return completion;
    }

    const auto portHealth = *summary->portHealth;
    const auto transition = ApplyAcceptedSummaryLocked(ticket.peer, state, portHealth, nowMs, completion);
    lock.unlock();
    if (transition.logResponse) {
        LogQueryResponse(ticket, portHealth, transition.decision);
    }
    return completion;
}

bool RemoteUbPortHealthVerifier::AcceptPassiveRecovery(const UbHealthSummary &summary)
{
    if (!summary.portHealth.has_value() || !ShouldRecoverFromUbIsolation(*summary.portHealth)) {
        return false;
    }
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto iter = peers_.find(summary.worker);
    if (iter == peers_.end() || iter->second.incarnation != summary.incarnation) {
        return false;
    }
    auto &state = iter->second;
    if (!state.isolated || !state.lastPortHealth.has_value()
        || !CanApplyPassiveUbRecovery(*state.lastPortHealth, *summary.portHealth)) {
        return false;
    }
    state.lastPortHealth = *summary.portHealth;
    state.isolated = false;
    state.summaryHintPending = false;
    state.triggerPending = false;
    state.verificationPending = false;
    state.inFlight = false;
    state.nextQueryMs = std::numeric_limits<uint64_t>::max();
    state.lastLoggedRetryStatus.reset();
    return true;
}

bool RemoteUbPortHealthVerifier::NotifySummaryHint(const UbHealthSummary &summary, uint64_t nowMs)
{
    if (!summary.portHealth.has_value() || !HasKnownUbPortHealth(*summary.portHealth)) {
        return false;
    }
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auto iter = peers_.find(summary.worker);
    if (iter == peers_.end() || iter->second.incarnation != summary.incarnation) {
        return false;
    }
    auto &state = iter->second;
    const bool newer =
        !state.lastPortHealth.has_value() || summary.portHealth->healthEpoch > state.lastPortHealth->healthEpoch;
    if (newer) {
        state.lastPortHealth = *summary.portHealth;
    }
    if (state.inFlight) {
        state.summaryHintPending = state.summaryHintPending || newer;
        return false;
    }
    if (state.nextQueryMs != std::numeric_limits<uint64_t>::max()
        || (!state.isolated && !(newer && ShouldIsolateForUbPortHealth(*summary.portHealth)))) {
        return false;
    }
    state.nextQueryMs =
        state.isolated || state.verificationPending ? RetryDeadlineMs(summary.worker, state, nowMs) : nowMs;
    state.verificationPending = true;
    return true;
}

void RemoteUbPortHealthVerifier::ReconcileTopology(
    const std::unordered_map<HostPort, std::string> &incarnations)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    for (auto iter = peers_.begin(); iter != peers_.end();) {
        auto current = incarnations.find(iter->first);
        iter = current == incarnations.end() || current->second != iter->second.incarnation
                   ? peers_.erase(iter)
                   : std::next(iter);
    }
}

std::optional<uint64_t> RemoteUbPortHealthVerifier::NextQueryDeadlineMs() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return NextUbProbeDeadline(peers_, [](const auto &entry) -> std::optional<uint64_t> {
        const auto &state = entry.second;
        return !state.inFlight && state.nextQueryMs != std::numeric_limits<uint64_t>::max()
                   ? std::optional<uint64_t>{ state.nextQueryMs } : std::nullopt;
    });
}

}  // namespace datasystem::cluster
