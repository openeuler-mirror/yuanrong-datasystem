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
    if (pausedAt_.has_value()) {
        const auto unreadableDuration = now - *pausedAt_;
        for (auto &[address, missingSince] : missingSince_) {
            (void)address;
            missingSince += unreadableDuration;
        }
        pausedAt_.reset();
    }
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
            missingSince_.erase(member.identity.address);
            continue;
        }
        if (member.state == MemberState::INITIAL) {
            observed.removeInitial.push_back(member.identity);
        } else if (member.state == MemberState::JOINING) {
            observed.removeJoining.push_back(member.identity);
        } else {
            auto [iter, inserted] = missingSince_.emplace(member.identity.address, now);
            if (!inserted && now - iter->second >= nodeDeadTimeout_) {
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

void TopologyFailureClassifier::Pause(std::chrono::steady_clock::time_point now) noexcept
{
    if (!pausedAt_.has_value()) {
        pausedAt_ = now;
    }
}

void TopologyFailureClassifier::Reset() noexcept
{
    missingSince_.clear();
    pausedAt_.reset();
}

}  // namespace datasystem::cluster
