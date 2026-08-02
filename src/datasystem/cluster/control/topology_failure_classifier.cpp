/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Controller-local membership failure classification.
 */
#include "datasystem/cluster/control/topology_failure_classifier.h"

#include <algorithm>
#include <unordered_set>

#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
TopologyFailureClassifier::TopologyFailureClassifier(std::chrono::seconds nodeDeadTimeout)
    : nodeDeadTimeout_(nodeDeadTimeout)
{
}

Status TopologyFailureClassifier::Observe(const TopologySnapshot &topology,
                                          const std::vector<MembershipRecord> &members,
                                          std::chrono::steady_clock::time_point now,
                                          FailureClassification &classification)
{
    CHECK_FAIL_RETURN_STATUS(nodeDeadTimeout_.count() >= 0, K_INVALID, "node dead timeout must be non-negative");
    Resume(now);
    std::unordered_set<std::string> present;
    for (const auto &record : members) {
        CHECK_FAIL_RETURN_STATUS(!record.address.empty() && present.insert(record.address).second, K_INVALID,
                                 "membership observation contains an invalid or duplicate address");
    }
    FailureClassification observed;
    std::vector<MemberIdentity> retainedFailed;
    for (const auto &member : topology.Members()) {
        if (member.state == MemberState::FAILED) {
            retainedFailed.push_back(member.identity);
            missingSince_.erase(member.identity.address);
            continue;
        }
        if (present.count(member.identity.address) > 0) {
            auto missing = missingSince_.find(member.identity.address);
            if (missing != missingSince_.end()) {
                observed.restored.push_back({ member.identity, member.state, DurationMs(missing->second, now) });
                missingSince_.erase(missing);
            }
            continue;
        }
        if (member.state == MemberState::INITIAL) {
            observed.removeInitial.push_back(member.identity);
        } else if (member.state == MemberState::JOINING) {
            observed.removeJoining.push_back(member.identity);
        } else {
            auto [iter, inserted] = missingSince_.emplace(member.identity.address, now);
            const auto missingMs = inserted ? 0 : DurationMs(iter->second, now);
            if (inserted) {
                observed.newlyMissing.push_back({ member.identity, member.state, missingMs });
            }
            // timeout==0: confirm on the first successful observation after membership key absence.
            if (now - iter->second >= nodeDeadTimeout_) {
                observed.confirmedMissing.push_back({ member.identity, member.state, missingMs });
                observed.confirmedFailure.push_back(member.identity);
            }
        }
    }
    if (!observed.confirmedFailure.empty()) {
        observed.confirmedFailure.insert(observed.confirmedFailure.end(), retainedFailed.begin(), retainedFailed.end());
        std::sort(observed.confirmedFailure.begin(), observed.confirmedFailure.end(),
                  [](const auto &left, const auto &right) { return left.address < right.address; });
    }
    for (auto iter = missingSince_.begin(); iter != missingSince_.end();) {
        const Member *member = nullptr;
        if (topology.FindMemberByAddress(iter->first, member).IsError()) {
            iter = missingSince_.erase(iter);
        } else {
            ++iter;
        }
    }
    classification = std::move(observed);
    return Status::OK();
}

Status TopologyFailureClassifier::ObserveMissingSamples(
    const std::vector<MemberAbsenceSample> &samples, std::chrono::steady_clock::time_point now,
    std::vector<MemberAbsenceObservation> &confirmedMissing)
{
    CHECK_FAIL_RETURN_STATUS(nodeDeadTimeout_.count() >= 0, K_INVALID, "node dead timeout must be non-negative");
    std::unordered_set<std::string> sampledAddresses;
    sampledAddresses.reserve(samples.size());
    for (const auto &sample : samples) {
        CHECK_FAIL_RETURN_STATUS(!sample.identity.address.empty()
                                     && sampledAddresses.insert(sample.identity.address).second,
                                 K_INVALID, "missing-member samples contain an invalid or duplicate address");
    }
    for (auto iter = missingSince_.begin(); iter != missingSince_.end();) {
        if (sampledAddresses.count(iter->first) == 0) {
            iter = missingSince_.erase(iter);
        } else {
            ++iter;
        }
    }
    Resume(now);
    std::vector<MemberAbsenceObservation> observed;
    observed.reserve(samples.size());
    for (const auto &sample : samples) {
        auto [iter, inserted] = missingSince_.emplace(sample.identity.address, now);
        const auto missingMs = inserted ? 0 : DurationMs(iter->second, now);
        if (now - iter->second >= nodeDeadTimeout_) {
            observed.push_back({ sample.identity, sample.state, missingMs });
        }
    }
    confirmedMissing = std::move(observed);
    return Status::OK();
}

void TopologyFailureClassifier::Resume(std::chrono::steady_clock::time_point now) noexcept
{
    if (!pausedAt_.has_value()) {
        return;
    }
    const auto unreadableDuration = now - *pausedAt_;
    for (auto &[address, missingSince] : missingSince_) {
        (void)address;
        missingSince += unreadableDuration;
    }
    pausedAt_.reset();
}

void TopologyFailureClassifier::Pause(std::chrono::steady_clock::time_point now) noexcept
{
    if (!pausedAt_.has_value()) {
        pausedAt_ = now;
    }
}

void TopologyFailureClassifier::ResetMissing(const std::string &address) noexcept
{
    missingSince_.erase(address);
}

void TopologyFailureClassifier::Reset() noexcept
{
    missingSince_.clear();
    pausedAt_.reset();
}

}  // namespace datasystem::cluster
