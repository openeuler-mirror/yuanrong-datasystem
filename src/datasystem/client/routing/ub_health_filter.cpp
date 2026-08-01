/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "datasystem/client/routing/ub_health_filter.h"

#include <unordered_set>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"

namespace datasystem::client {
bool UbHealthFilter::ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    {
        std::lock_guard<std::mutex> lock(incarnationMutex_);
        auto expected = expectedIncarnation;
        if (topologyInitialized_) {
            auto trusted = trustedIncarnations_.find(summary.worker);
            if (trusted == trustedIncarnations_.end()) {
                return false;
            }
            expected = trusted->second;
        }
        if (!cache_.Apply(summary, expected)) {
            return false;
        }
        ReconcileLocalObservationWithTrustedIncarnationLocked(summary.worker, summary.incarnation);
        trustedIncarnations_[summary.worker] = summary.incarnation;
    }
    if (!summary.writable) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.global_unavailable_applied");
    }
    return true;
}

void UbHealthFilter::ApplyTopologyIncarnations(const ::datasystem::ClusterTopologyPb &ring)
{
    std::unordered_map<HostPort, std::string> replacement;
    std::unordered_set<HostPort> workers;
    replacement.reserve(ring.members_size());
    workers.reserve(ring.members_size());
    for (const auto &[endpoint, member] : ring.members()) {
        HostPort worker;
        if (worker.ParseString(endpoint).IsError() || member.id().empty()) {
            continue;
        }
        workers.emplace(worker);
        replacement.emplace(std::move(worker), member.id());
    }

    std::lock_guard<std::mutex> lock(incarnationMutex_);
    topologyInitialized_ = true;
    cache_.ReconcileWorkers(workers);
    for (const auto &[worker, incarnation] : replacement) {
        ReconcileLocalObservationWithTrustedIncarnationLocked(worker, incarnation);
    }
    for (auto iter = localObservationIncarnations_.begin(); iter != localObservationIncarnations_.end();) {
        if (workers.count(iter->first) == 0) {
            localAdmission_.ClearLocalState(iter->first);
            iter = localObservationIncarnations_.erase(iter);
        } else {
            ++iter;
        }
    }
    trustedIncarnations_ = std::move(replacement);
}

bool UbHealthFilter::ReportProviderFailure(const HostPort &provider, const ProviderUbFailureDetailPb &detail)
{
    auto outcome = DecodeProviderUbFailureDetail(detail, provider, UbOperationKind::CLIENT_GET_WRITEBACK,
                                                 "client_direct_get_provider_detail");
    if (!outcome.has_value()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(incarnationMutex_);
    localAdmission_.ReportOutcome(*outcome);
    const auto state = localAdmission_.GetState(provider);
    const bool unavailable = state.has_value() && state->state == UbAdmissionState::UNAVAILABLE;
    if (unavailable) {
        auto incarnation = trustedIncarnations_.find(provider);
        localObservationIncarnations_[provider] =
            incarnation == trustedIncarnations_.end() ? std::string{} : incarnation->second;
        INJECT_POINT_NO_RETURN("client.ub_health_filter.local_observation");
    }
    return unavailable;
}

void UbHealthFilter::ReconcileLocalObservationWithTrustedIncarnationLocked(const HostPort &worker,
                                                                           const std::string &incarnation)
{
    auto observation = localObservationIncarnations_.find(worker);
    if (observation == localObservationIncarnations_.end()
        || (!observation->second.empty() && observation->second == incarnation)) {
        return;
    }
    localAdmission_.ClearLocalState(worker);
    localObservationIncarnations_.erase(observation);
}

bool UbHealthFilter::IsAvailable(const HostPort &addr) const
{
    if (localAdmission_.CheckReadSource(addr).IsError()) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.local_read_denied");
        return false;
    }
    auto summary = cache_.Get(addr);
    if (summary.has_value() && !summary->writable) {
        INJECT_POINT_NO_RETURN("client.ub_health_filter.global_read_denied");
        return false;
    }
    return true;
}

std::optional<UbPathState> UbHealthFilter::GetLocalObservation(const HostPort &addr) const
{
    return localAdmission_.GetState(addr);
}
}  // namespace datasystem::client
