/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Control backend failure scope evidence classifier.
 */
#include "datasystem/cluster/runtime/control_backend_scope_classifier.h"

#include <chrono>
#include <unordered_map>
#include <unordered_set>

namespace datasystem::cluster {
namespace {
constexpr auto BACKEND_EVIDENCE_MAX_AGE = std::chrono::seconds(5);

bool IsFreshControlBackendObservation(const ControlBackendObservation &observation,
                                      std::chrono::steady_clock::time_point now)
{
    return observation.observedAt != std::chrono::steady_clock::time_point{} && observation.observedAt <= now
           && now - observation.observedAt <= BACKEND_EVIDENCE_MAX_AGE;
}

bool SameControlBackendAuthorityStamp(const ControlBackendObservation &left, const ControlBackendObservation &right)
{
    return left.topologyVersion == right.topologyVersion && left.topologyRevision == right.topologyRevision
           && !left.topologyDigest.empty() && left.topologyDigest == right.topologyDigest;
}
}  // namespace

ControlBackendFailureScope ClassifyControlBackendFailureScope(
    const ControlBackendObservation &local, const std::vector<MemberIdentity> &targets,
    const std::vector<ControlBackendObservation> &observations)
{
    if (targets.empty() || observations.empty()) {
        return ControlBackendFailureScope::INCONCLUSIVE;
    }
    std::unordered_map<std::string, MemberIdentity> expected;
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
            || observation.state == ControlBackendState::UNKNOWN
            || !IsFreshControlBackendObservation(observation, now)) {
            return ControlBackendFailureScope::INCONCLUSIVE;
        }
        if (!SameControlBackendAuthorityStamp(local, observation)) {
            authorityMismatch = true;
        }
        peerAvailable = peerAvailable || observation.state == ControlBackendState::AVAILABLE;
    }
    if (peerAvailable) {
        return ControlBackendFailureScope::LOCAL_ISOLATION;
    }
    if (accepted.size() != targets.size()) {
        return ControlBackendFailureScope::INCONCLUSIVE;
    }
    if (authorityMismatch) {
        return ControlBackendFailureScope::INCONCLUSIVE;
    }
    return ControlBackendFailureScope::GLOBAL_OUTAGE;
}

bool ConfirmsGlobalBackendOutage(const ControlBackendObservation &local, const std::vector<MemberIdentity> &targets,
                                 const std::vector<ControlBackendObservation> &observations)
{
    return ClassifyControlBackendFailureScope(local, targets, observations)
           == ControlBackendFailureScope::GLOBAL_OUTAGE;
}
}  // namespace datasystem::cluster
