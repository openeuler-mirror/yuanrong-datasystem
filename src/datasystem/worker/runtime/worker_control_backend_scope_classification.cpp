/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker control backend failure scope evidence classifier.
 */
#include "datasystem/worker/runtime/worker_control_backend_scope.h"

#include <chrono>
#include <unordered_map>
#include <unordered_set>

namespace datasystem::worker {
namespace {
constexpr auto BACKEND_EVIDENCE_MAX_AGE = std::chrono::seconds(5);

bool IsFreshControlBackendObservation(const cluster::ControlBackendObservation &observation,
                                      std::chrono::steady_clock::time_point now)
{
    return observation.observedAt != std::chrono::steady_clock::time_point{} && observation.observedAt <= now
           && now - observation.observedAt <= BACKEND_EVIDENCE_MAX_AGE;
}

bool SameControlBackendAuthorityStamp(const cluster::ControlBackendObservation &left,
                                      const cluster::ControlBackendObservation &right)
{
    return left.topologyVersion == right.topologyVersion && left.topologyRevision == right.topologyRevision
           && !left.topologyDigest.empty() && left.topologyDigest == right.topologyDigest;
}
}  // namespace

bool ConfirmsGlobalBackendOutage(const cluster::ControlBackendObservation &local,
                                 const std::vector<cluster::MemberIdentity> &targets,
                                 const std::vector<cluster::ControlBackendObservation> &observations)
{
    return ClassifyControlBackendFailureScope(local, targets, observations)
           == ControlBackendFailureScope::GLOBAL_OUTAGE;
}

ControlBackendFailureScope ClassifyControlBackendFailureScope(
    const cluster::ControlBackendObservation &local, const std::vector<cluster::MemberIdentity> &targets,
    const std::vector<cluster::ControlBackendObservation> &observations)
{
    if (targets.empty() || observations.size() != targets.size()) {
        return ControlBackendFailureScope::INCONCLUSIVE;
    }
    std::unordered_map<std::string, cluster::MemberIdentity> expected;
    expected.reserve(targets.size());
    for (const auto &target : targets) {
        expected.emplace(target.address, target);
    }
    std::unordered_set<std::string> accepted;
    accepted.reserve(targets.size());
    const auto now = std::chrono::steady_clock::now();
    bool peerAvailable = false;
    bool authorityMismatch = false;
    for (const auto &observation : observations) {
        auto target = expected.find(observation.reporter.address);
        if (target == expected.end() || !(target->second == observation.reporter)
            || !accepted.insert(observation.reporter.address).second
            || observation.state == cluster::ControlBackendState::UNKNOWN
            || !IsFreshControlBackendObservation(observation, now)) {
            return ControlBackendFailureScope::INCONCLUSIVE;
        }
        if (!SameControlBackendAuthorityStamp(local, observation)) {
            authorityMismatch = true;
        }
        peerAvailable = peerAvailable || observation.state == cluster::ControlBackendState::AVAILABLE;
    }
    if (accepted.size() != targets.size()) {
        return ControlBackendFailureScope::INCONCLUSIVE;
    }
    if (!peerAvailable && authorityMismatch) {
        return ControlBackendFailureScope::INCONCLUSIVE;
    }
    return peerAvailable ? ControlBackendFailureScope::LOCAL_ISOLATION : ControlBackendFailureScope::GLOBAL_OUTAGE;
}
}  // namespace datasystem::worker
