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

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"

namespace datasystem {

Status PeerUbAdmission::CheckWriteTarget(const HostPort &peer, UbOperationKind op) const
{
    (void)op;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto global = globalSummaries_.find(peer);
    if (global != globalSummaries_.end() && !global->second.writable) {
        return BuildUnavailableStatus(peer, StatusCode::K_URMA_WORKER_UNAVAILABLE);
    }
    auto it = states_.find(peer);
    if (it == states_.end() || !ShouldBlock(it->second)) {
        return Status::OK();
    }
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

void PeerUbAdmission::ReportOutcome(const UbOpOutcome &outcome)
{
    auto failureClass = classifier_.Classify(outcome);
    if (failureClass == UbFailureClass::SUCCESS || failureClass == UbFailureClass::LOCAL_RESOURCE_PRESSURE
        || failureClass == UbFailureClass::NON_UB_FAILURE) {
        return;
    }

    const auto nextState =
        failureClass == UbFailureClass::TIMEOUT_SUSPECT ? UbAdmissionState::SUSPECT : UbAdmissionState::UNAVAILABLE;
    bool stateChanged = false;
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        auto &state = states_[outcome.peer];
        stateChanged = state.state != nextState;
        state.lastStatus = outcome.status;
        state.lastFailureClass = failureClass;
        state.state = nextState;
        ++state.epoch;
    }
    if (!stateChanged) {
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

void PeerUbAdmission::ReplaceGlobalSummaries(const std::vector<UbHealthSummary> &summaries)
{
    constexpr size_t MAX_RETIRED_INCARNATIONS_PER_WORKER = 8;
    std::lock_guard<std::shared_mutex> lock(mutex_);
    std::unordered_map<HostPort, UbHealthSummary> replacement;
    replacement.reserve(summaries.size());
    for (const auto &summary : summaries) {
        if (summary.worker.Empty() || summary.incarnation.empty()) {
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
    }
    globalSummaries_ = std::move(replacement);
    INJECT_POINT_NO_RETURN("PeerUbAdmission.ReplaceGlobalSummaries.afterCommit");
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

}  // namespace datasystem
