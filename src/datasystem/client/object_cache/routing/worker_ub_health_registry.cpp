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

#include "datasystem/client/object_cache/routing/worker_ub_health_registry.h"

#include <atomic>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/log.h"

namespace datasystem::client {
namespace {
struct RoutingHealthChange {
    HostPort worker;
    std::string incarnationPrefix;
    const char *source;
    UbPortHealthSummary previous;
    UbPortHealthSummary current;
    bool previouslyWritable;
    bool currentlyWritable;
};

void LogRoutingHealthChange(const RoutingHealthChange &change)
{
    LOG(INFO) << "UB_ROUTING_HEALTH action=updated peer=" << change.worker.ToString()
              << " source=" << change.source << " incarnation_prefix=" << change.incarnationPrefix
              << " old_valid=" << change.previous.valid << " old_bad=" << change.previous.badPortCount
              << " old_total=" << change.previous.totalPortCount << " new_valid=" << change.current.valid
              << " new_bad=" << change.current.badPortCount << " new_total=" << change.current.totalPortCount
              << " health_epoch=" << change.current.healthEpoch
              << " old_writable=" << change.previouslyWritable
              << " writable=" << change.currentlyWritable << " routing_visible=true";
}

bool IsRoutableMember(::datasystem::MembershipPb::StatePb state)
{
    return state == ::datasystem::MembershipPb::ACTIVE || state == ::datasystem::MembershipPb::LEAVING;
}

WorkerUbPortHealth BuildRoutingWorker(const std::string &incarnation, const UbHealthSummary &summary)
{
    return { incarnation, *summary.portHealth };
}

std::optional<bool> GetVerifiedUnavailable(const UbHealthSummary &summary, bool verified)
{
    if (!verified) {
        return std::nullopt;
    }
    if (!summary.portHealth.has_value()) {
        return !summary.writable;
    }
    if (CanUpdateRemoteUbAdmission(UbPortHealthEvidenceSource::QUERY_RESPONSE, *summary.portHealth)) {
        return ShouldIsolateForUbPortHealth(*summary.portHealth);
    }
    return std::nullopt;
}

bool IsPassiveRecoveryCandidate(bool verified, bool currentlyVerified, const UbHealthSummary *previous,
                                const UbHealthSummary &summary, const UbHealthSummary &accepted)
{
    return !verified && currentlyVerified && previous != nullptr && previous->portHealth.has_value()
           && summary.portHealth.has_value() && IsSameUbPortHealth(summary.portHealth, accepted.portHealth)
           && CanApplyPassiveUbRecovery(*previous->portHealth, *summary.portHealth);
}
}  // namespace

struct WorkerUbHealthRegistry::WorkerState {
    UbHealthSummaryCache::Snapshot health;
    std::unordered_map<HostPort, std::string> verifiedUnavailable;
};

struct WorkerUbHealthRegistry::State {
    using Incarnations = std::unordered_map<HostPort, std::string>;
    std::shared_ptr<const Incarnations> incarnations;
    std::shared_ptr<const WorkerState> workers = std::make_shared<const WorkerState>();
    UbRoutingHealthSnapshot routing;
};

WorkerUbHealthRegistry::WorkerUbHealthRegistry() : state_(std::make_shared<const State>())
{
}

void WorkerUbHealthRegistry::ReconcileTopology(const ::datasystem::ClusterTopologyPb &topology)
{
    std::shared_ptr<const State> current;
    std::lock_guard<bthread::Mutex> lock(writeMutex_);
    current = std::atomic_load(&state_);
    auto incarnations = std::make_shared<State::Incarnations>();
    incarnations->reserve(topology.members_size());
    std::unordered_set<HostPort> workers;
    workers.reserve(topology.members_size());
    std::vector<std::pair<HostPort, const ::datasystem::MembershipPb *>> routableMembers;
    routableMembers.reserve(topology.members_size());
    for (const auto &[endpoint, member] : topology.members()) {
        HostPort worker;
        if (!IsRoutableMember(member.state()) || member.id().empty()
            || worker.ParseString(endpoint).IsError()) {
            continue;
        }
        incarnations->emplace(worker, member.id());
        workers.emplace(worker);
        routableMembers.emplace_back(worker, &member);
    }
    if (current->incarnations != nullptr && *current->incarnations == *incarnations) {
        return;
    }

    auto next = std::make_shared<State>();
    auto workerState = std::make_shared<WorkerState>();
    workerState->health = current->workers->health;
    next->routing.localClient = current->routing.localClient;
    next->routing.workers.reserve(routableMembers.size());
    for (const auto &[worker, member] : routableMembers) {
        ReconcileTopologyMemberLocked(current, worker, *member, *next, *workerState);
    }
    workerState->health.ReconcileWorkers(workers);
    next->incarnations = std::move(incarnations);
    next->workers = std::move(workerState);
    std::atomic_store(&state_, std::shared_ptr<const State>(std::move(next)));
}

void WorkerUbHealthRegistry::ReconcileTopologyMemberLocked(
    const std::shared_ptr<const State> &current, const HostPort &worker,
    const ::datasystem::MembershipPb &member, State &next, WorkerState &workers) const
{
    if (current->incarnations != nullptr) {
        auto previous = current->incarnations->find(worker);
        if (previous != current->incarnations->end() && previous->second != member.id()) {
            workers.health.Retire(worker, previous->second);
        }
    }
    const auto *summary = workers.health.Find(worker);
    if (summary != nullptr && summary->incarnation != member.id()) {
        workers.health.Retire(worker, summary->incarnation);
        summary = nullptr;
    }
    if (summary != nullptr && summary->portHealth.has_value()) {
        next.routing.workers.emplace(worker, BuildRoutingWorker(member.id(), *summary));
    }
    auto verified = current->workers->verifiedUnavailable.find(worker);
    if (verified != current->workers->verifiedUnavailable.end() && verified->second == member.id()) {
        workers.verifiedUnavailable.emplace(worker, verified->second);
    }
}

bool WorkerUbHealthRegistry::ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    return ApplySummaryInternal(summary, expectedIncarnation, false).result == ApplyResult::UPDATED;
}

bool WorkerUbHealthRegistry::ApplySummary(const UbHealthSummary &summary,
                                          const std::string &expectedIncarnation,
                                          const PassiveRecoveryCommit &recoveryCommit, bool &recovered)
{
    const auto outcome = ApplySummaryInternal(summary, expectedIncarnation, false, recoveryCommit);
    recovered = outcome.recovered;
    return outcome.result == ApplyResult::UPDATED;
}

bool WorkerUbHealthRegistry::ApplyVerifiedSummary(const UbHealthSummary &summary,
                                                  const std::string &expectedIncarnation)
{
    return ApplySummaryInternal(summary, expectedIncarnation, true).result != ApplyResult::REJECTED;
}

WorkerUbHealthRegistry::ApplyOutcome WorkerUbHealthRegistry::ApplySummaryInternal(
    const UbHealthSummary &summary, const std::string &expectedIncarnation, bool verified,
    const PassiveRecoveryCommit &recoveryCommit)
{
    std::shared_ptr<const State> current;
    std::unique_lock<bthread::Mutex> lock(writeMutex_);
    current = std::atomic_load(&state_);
    std::string expected;
    if (!ResolveExpectedIncarnationLocked(*current, summary.worker, expectedIncarnation, expected)) {
        return {};
    }
    UbHealthSummary accepted;
    if (!current->workers->health.Prepare(summary, expected, accepted)) {
        return {};
    }
    const auto *previous = current->workers->health.Find(summary.worker);
    const bool evidenceAccepted = !verified || !summary.portHealth.has_value()
                                  || IsSameUbPortHealth(summary.portHealth, accepted.portHealth);
    const bool summaryChanged = previous == nullptr || !IsSameUbHealthSummary(*previous, accepted);
    auto marked = current->workers->verifiedUnavailable.find(summary.worker);
    const bool identityChanged = marked != current->workers->verifiedUnavailable.end()
                                 && marked->second != summary.incarnation;
    const bool currentlyVerified = marked != current->workers->verifiedUnavailable.end() && !identityChanged;
    const bool passiveRecoveryCandidate =
        IsPassiveRecoveryCandidate(verified, currentlyVerified, previous, summary, accepted);
    // Publish recovery only after the write admission accepts the same fact. A rejection keeps the previous
    // isolation evidence intact so a later passive response can retry the complete transition.
    if (passiveRecoveryCandidate && (!recoveryCommit || !recoveryCommit(accepted))) {
        return {};
    }
    const bool passiveRecovery = passiveRecoveryCandidate;
    const auto unavailable = passiveRecovery ? std::optional<bool>{ false }
                                             : GetVerifiedUnavailable(accepted, verified && evidenceAccepted);
    if (!summaryChanged && !identityChanged
        && (!unavailable.has_value() || *unavailable == currentlyVerified)) {
        return { verified && evidenceAccepted ? ApplyResult::ACCEPTED : ApplyResult::REJECTED, false };
    }
    auto next = std::make_shared<State>(*current);
    auto workers = std::make_shared<WorkerState>(*current->workers);
    workers->health.Apply(summary, expected);
    if (summaryChanged && accepted.portHealth.has_value()) {
        next->routing.workers[summary.worker] = BuildRoutingWorker(expected, accepted);
    }
    if (identityChanged || (unavailable.has_value() && !*unavailable)) {
        workers->verifiedUnavailable.erase(summary.worker);
    }
    if (unavailable.value_or(false)) {
        workers->verifiedUnavailable[summary.worker] = summary.incarnation;
    }
    next->workers = std::move(workers);
    std::atomic_store(&state_, std::shared_ptr<const State>(next));
    lock.unlock();
    const char *source = passiveRecovery ? "passive_recovery" : (verified ? "query_response" : "rpc_response");
    LogRoutingChange(*current, *next, summary.worker, source);
    return { evidenceAccepted ? ApplyResult::UPDATED : ApplyResult::REJECTED, passiveRecovery };
}

bool WorkerUbHealthRegistry::ResolveExpectedIncarnationLocked(const State &current, const HostPort &worker,
                                                              const std::string &fallback,
                                                              std::string &expected) const
{
    expected = fallback;
    if (current.incarnations == nullptr) {
        return true;
    }
    auto identity = current.incarnations->find(worker);
    if (identity == current.incarnations->end()) {
        return false;
    }
    expected = identity->second;
    return true;
}

void WorkerUbHealthRegistry::LogRoutingChange(const State &previous, const State &current,
                                              const HostPort &worker, const char *source) const
{
    auto before = previous.routing.workers.find(worker);
    auto after = current.routing.workers.find(worker);
    if (after == current.routing.workers.end()) {
        return;
    }
    UbPortHealthSummary oldPortHealth;
    bool wasUnavailable = false;
    if (before != previous.routing.workers.end()) {
        oldPortHealth = before->second.portHealth;
        auto oldMarked = previous.workers->verifiedUnavailable.find(worker);
        wasUnavailable = oldMarked != previous.workers->verifiedUnavailable.end()
                         && oldMarked->second == before->second.incarnation;
    }
    auto newMarked = current.workers->verifiedUnavailable.find(worker);
    const bool isUnavailable = newMarked != current.workers->verifiedUnavailable.end()
                               && newMarked->second == after->second.incarnation;
    if (before != previous.routing.workers.end()
        && IsSameUbPortHealth(oldPortHealth, after->second.portHealth) && wasUnavailable == isUnavailable) {
        return;
    }
    LogRoutingHealthChange({ worker, FormatUbHealthIncarnationPrefix(after->second.incarnation), source,
                             oldPortHealth, after->second.portHealth, !wasUnavailable, !isUnavailable });
}

bool WorkerUbHealthRegistry::ApplyLocalClientPortHealth(const UbPortHealthSummary &portHealth)
{
    // Keep the retired snapshot alive until the publication lock has been released.
    std::shared_ptr<const State> current;
    std::lock_guard<bthread::Mutex> lock(writeMutex_);
    current = std::atomic_load(&state_);
    std::optional<UbPortHealthSummary> merged;
    if (!MergeUbPortHealth(current->routing.localClient, portHealth, merged)
        || !merged.has_value() || IsSameUbPortHealth(current->routing.localClient, *merged)) {
        return false;
    }
    auto next = std::make_shared<State>(*current);
    next->routing.localClient = *merged;
    std::atomic_store(&state_, std::shared_ptr<const State>(std::move(next)));
    return true;
}

void WorkerUbHealthRegistry::OnUbPortHealthChanged(const UbPortHealthSummary &portHealth)
{
    (void)ApplyLocalClientPortHealth(portHealth);
}

std::optional<UbHealthSummary> WorkerUbHealthRegistry::GetSummary(const HostPort &worker) const
{
    auto state = std::atomic_load(&state_);
    const auto *summary = state->workers->health.Find(worker);
    return summary == nullptr ? std::nullopt : std::optional<UbHealthSummary>{ *summary };
}

bool WorkerUbHealthRegistry::IsVerifiedUnavailable(const HostPort &worker) const
{
    return std::atomic_load(&state_)->workers->verifiedUnavailable.count(worker) != 0;
}

std::shared_ptr<const UbRoutingHealthSnapshot> WorkerUbHealthRegistry::GetRoutingSnapshot() const
{
    if (!IsClientUbFaultIsolationEnabled()) {
        // Disabled: publish no port health at all, so scheduling treats every UB path as UNKNOWN.
        static const auto emptySnapshot = std::make_shared<const UbRoutingHealthSnapshot>();
        return emptySnapshot;
    }
    auto state = std::atomic_load(&state_);
    return std::shared_ptr<const UbRoutingHealthSnapshot>(state, &state->routing);
}
}  // namespace datasystem::client
