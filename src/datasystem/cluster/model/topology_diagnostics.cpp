/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Shared topology diagnostic formatting helpers.
 */
#include "datasystem/cluster/model/topology_diagnostics.h"

#include <algorithm>
#include <map>
#include <optional>
#include <sstream>
#include <string_view>
#include <utility>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::cluster {
namespace {
struct RingRange {
    uint32_t start;
    uint32_t end;
    bool full;
};

bool IsRingMember(const Member &member, const std::optional<ActiveBatch> &batch, TopologyRingView view)
{
    const bool committed = member.state == MemberState::ACTIVE || member.state == MemberState::PRE_LEAVING
                           || member.state == MemberState::LEAVING;
    if (view == TopologyRingView::COMMITTED || !batch.has_value()
        || batch->type == TopologyChangeType::FAILURE) {
        return committed;
    }
    if (batch->type == TopologyChangeType::SCALE_OUT) {
        return committed || member.state == MemberState::JOINING;
    }
    return committed && member.state != MemberState::LEAVING;
}

std::map<std::string_view, std::vector<RingRange>> BuildRingRanges(const TopologySnapshot &snapshot,
                                                                  TopologyRingView view)
{
    std::vector<std::pair<uint32_t, const Member *>> owners;
    for (const auto &member : snapshot.Members()) {
        if (!IsRingMember(member, snapshot.GetActiveBatch(), view)) {
            continue;
        }
        for (const auto token : member.tokens) {
            owners.emplace_back(token, &member);
        }
    }
    std::sort(owners.begin(), owners.end(),
              [](const auto &left, const auto &right) { return left.first < right.first; });
    std::map<std::string_view, std::vector<RingRange>> rangesByAddress;
    for (size_t index = 0; index < owners.size(); ++index) {
        const size_t previous = index == 0 ? owners.size() - 1 : index - 1;
        rangesByAddress[owners[index].second->identity.address].push_back(
            { owners[previous].first, owners[index].first, owners.size() == 1 });
    }
    return rangesByAddress;
}

void AppendTruncation(std::ostringstream &out, size_t total, size_t effectiveLimit)
{
    if (total > effectiveLimit) {
        out << (effectiveLimit == 0 ? "" : ",") << "...(truncated,total=" << total << ")";
    }
}

void AppendRingRanges(std::ostringstream &out, const std::vector<RingRange> &ranges)
{
    out << "[";
    for (size_t index = 0; index < ranges.size(); ++index) {
        if (index > 0) {
            out << ",";
        }
        if (ranges[index].full) {
            out << "full";
        } else {
            out << "(" << ranges[index].start << "," << ranges[index].end << "]";
        }
    }
    out << "]";
}

}  // namespace

std::string TopologyParticipantScopeForLog(const MemberIdentity &executor,
                                           const std::optional<MemberIdentity> &source,
                                           const std::optional<MemberIdentity> &target,
                                           const std::optional<MemberIdentity> &failed)
{
    const auto identityAddress = [](const std::optional<MemberIdentity> &identity) {
        return identity.has_value() ? identity->address : "";
    };
    const auto identityId = [](const std::optional<MemberIdentity> &identity) {
        return identity.has_value() ? MemberIdForLog(identity->id) : "";
    };
    std::ostringstream out;
    out << "executor=" << executor.address << " executor_id_prefix=" << MemberIdForLog(executor.id)
        << " source=" << identityAddress(source) << " source_id_prefix=" << identityId(source)
        << " target=" << identityAddress(target) << " target_id_prefix=" << identityId(target)
        << " failed=" << identityAddress(failed) << " failed_id_prefix=" << identityId(failed);
    return out.str();
}

const char *TopologyChangeTypeName(TopologyChangeType type)
{
    switch (type) {
        case TopologyChangeType::SCALE_OUT:
            return "SCALE_OUT";
        case TopologyChangeType::SCALE_IN:
            return "SCALE_IN";
        case TopologyChangeType::FAILURE:
            return "FAILURE";
        default:
            return "UNKNOWN";
    }
}

const char *MemberStateName(MemberState state)
{
    switch (state) {
        case MemberState::INITIAL:
            return "INITIAL";
        case MemberState::JOINING:
            return "JOINING";
        case MemberState::ACTIVE:
            return "ACTIVE";
        case MemberState::PRE_LEAVING:
            return "PRE_LEAVING";
        case MemberState::LEAVING:
            return "LEAVING";
        case MemberState::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN";
    }
}

const char *MemberLifecycleStateName(MemberLifecycleState state)
{
    switch (state) {
        case MemberLifecycleState::UNKNOWN:
            return "UNKNOWN";
        case MemberLifecycleState::STARTING:
            return "STARTING";
        case MemberLifecycleState::RESTARTING:
            return "RESTARTING";
        case MemberLifecycleState::RECOVERING:
            return "RECOVERING";
        case MemberLifecycleState::READY:
            return "READY";
        case MemberLifecycleState::EXITING:
            return "EXITING";
        case MemberLifecycleState::DOWNGRADE_RESTARTING:
            return "DOWNGRADE_RESTARTING";
        case MemberLifecycleState::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN";
    }
}

const char *TopologyCallbackStageName(TopologyCallbackPhase phase, bool scaleInDataDrain)
{
    if (phase == TopologyCallbackPhase::SCALE_OUT || (phase == TopologyCallbackPhase::SCALE_IN && !scaleInDataDrain)) {
        return "metadata_migration";
    }
    if (phase == TopologyCallbackPhase::SCALE_IN) {
        return "data_drain";
    }
    if (phase == TopologyCallbackPhase::SCALE_IN_CLEANUP) {
        return "cleanup";
    }
    return phase == TopologyCallbackPhase::FAILURE ? "failure_recovery" : "unknown";
}

std::string TopologyDiagnosticPrefix(const std::string &value, size_t prefixSize)
{
    return value.substr(0, std::min(value.size(), prefixSize));
}

std::string MemberIdForLog(const std::string &id, size_t prefixSize)
{
    if (id.empty()) {
        return "";
    }
    const auto printable = id.size() == UUID_SIZE ? BytesUuidToString(id) : id;
    return TopologyDiagnosticPrefix(printable, prefixSize);
}

std::string MemberIdentitySample(const std::vector<MemberIdentity> &members, size_t limit)
{
    std::ostringstream out;
    out << "[";
    const size_t effectiveLimit = std::min(members.size(), limit);
    for (size_t index = 0; index < effectiveLimit; ++index) {
        out << (index == 0 ? "" : ",") << members[index].address << ":" << MemberIdForLog(members[index].id);
    }
    AppendTruncation(out, members.size(), effectiveLimit);
    out << "]";
    return out.str();
}

std::string MembershipSample(const std::vector<MembershipRecord> &memberships, size_t limit)
{
    std::ostringstream out;
    out << "[";
    const size_t effectiveLimit = std::min(memberships.size(), limit);
    for (size_t index = 0; index < effectiveLimit; ++index) {
        out << (index == 0 ? "" : ",") << memberships[index].address << ":"
            << MemberLifecycleStateName(memberships[index].state);
    }
    AppendTruncation(out, memberships.size(), effectiveLimit);
    out << "]";
    return out.str();
}

std::string MembershipStateCounts(const std::vector<MembershipRecord> &memberships)
{
    size_t starting = 0;
    size_t restarting = 0;
    size_t recovering = 0;
    size_t ready = 0;
    size_t exiting = 0;
    size_t failed = 0;
    size_t other = 0;
    for (const auto &record : memberships) {
        if (record.state == MemberLifecycleState::STARTING) {
            ++starting;
        } else if (record.state == MemberLifecycleState::RESTARTING
                   || record.state == MemberLifecycleState::DOWNGRADE_RESTARTING) {
            ++restarting;
        } else if (record.state == MemberLifecycleState::RECOVERING) {
            ++recovering;
        } else if (record.state == MemberLifecycleState::READY) {
            ++ready;
        } else if (record.state == MemberLifecycleState::EXITING) {
            ++exiting;
        } else if (record.state == MemberLifecycleState::FAILED) {
            ++failed;
        } else {
            ++other;
        }
    }
    std::ostringstream out;
    out << "starting=" << starting << ",restarting=" << restarting << ",recovering=" << recovering;
    out << ",ready=" << ready << ",exiting=" << exiting << ",failed=" << failed << ",other=" << other;
    return out.str();
}

std::string TokenRangesForLog(const std::vector<TokenRange> &ranges)
{
    std::ostringstream out;
    out << "[";
    for (size_t index = 0; index < ranges.size(); ++index) {
        out << (index == 0 ? "" : ",") << "[" << ranges[index].from << "," << ranges[index].end << "]";
    }
    out << "]";
    return out.str();
}

std::string TopologyTaskScopeForLog(const TopologyExecutionFence &fence)
{
    std::ostringstream out;
    out << TopologyParticipantScopeForLog(fence.executor, fence.source, fence.target, fence.failed)
        << " range_count=" << fence.ranges.size() << " hash_ranges=" << TokenRangesForLog(fence.ranges);
    return out.str();
}

std::string TopologyRingForLog(const TopologySnapshot &snapshot, TopologyRingView view, size_t limit)
{
    const auto rangesByAddress = BuildRingRanges(snapshot, view);
    const auto &members = snapshot.Members();
    const size_t effectiveLimit = std::min(members.size(), limit);
    std::ostringstream out;
    out << "[";
    for (size_t index = 0; index < effectiveLimit; ++index) {
        const auto &member = members[index];
        out << (index == 0 ? "" : ",") << "{address=" << member.identity.address
            << ",id_prefix=" << MemberIdForLog(member.identity.id) << ",state=" << MemberStateName(member.state)
            << ",ranges=";
        const auto ranges = rangesByAddress.find(member.identity.address);
        if (ranges == rangesByAddress.end()) {
            out << "[]";
        } else {
            AppendRingRanges(out, ranges->second);
        }
        out << "}";
    }
    AppendTruncation(out, members.size(), effectiveLimit);
    out << "]";
    return out.str();
}

std::string TopologyRingViewsForLog(const TopologySnapshot &snapshot, size_t limit)
{
    std::ostringstream out;
    out << "committed_ring=" << TopologyRingForLog(snapshot, TopologyRingView::COMMITTED, limit)
        << " prospective_ring=" << TopologyRingForLog(snapshot, TopologyRingView::PROSPECTIVE, limit);
    return out.str();
}

int64_t DurationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}
}  // namespace datasystem::cluster
