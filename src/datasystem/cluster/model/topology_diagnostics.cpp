/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Shared topology diagnostic formatting helpers.
 */
#include "datasystem/cluster/model/topology_diagnostics.h"

#include <algorithm>
#include <sstream>

#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::cluster {
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
    if (members.size() > effectiveLimit) {
        out << (effectiveLimit == 0 ? "..." : ",...");
    }
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
    if (memberships.size() > effectiveLimit) {
        out << (effectiveLimit == 0 ? "..." : ",...");
    }
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

int64_t DurationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}
}  // namespace datasystem::cluster
