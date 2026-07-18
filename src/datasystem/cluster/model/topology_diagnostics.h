/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Shared topology diagnostic formatting helpers.
 */
#ifndef DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_DIAGNOSTICS_H
#define DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_DIAGNOSTICS_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {
constexpr size_t TOPOLOGY_DIGEST_DIAGNOSTIC_PREFIX_SIZE = 12;
constexpr size_t TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT = 8;
constexpr int TOPOLOGY_VERBOSE_LOG_LEVEL = 1;
constexpr uint64_t TOPOLOGY_NO_ACTIVE_BATCH_EPOCH = 0;

const char *TopologyChangeTypeName(TopologyChangeType type);
const char *MemberStateName(MemberState state);
const char *MemberLifecycleStateName(MemberLifecycleState state);

std::string TopologyDiagnosticPrefix(const std::string &value,
                                     size_t prefixSize = TOPOLOGY_DIGEST_DIAGNOSTIC_PREFIX_SIZE);
std::string MemberIdForLog(const std::string &id, size_t prefixSize = TOPOLOGY_DIGEST_DIAGNOSTIC_PREFIX_SIZE);
std::string MemberIdentitySample(const std::vector<MemberIdentity> &members,
                                 size_t limit = TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT);
std::string MembershipSample(const std::vector<MembershipRecord> &memberships,
                             size_t limit = TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT);
std::string MembershipStateCounts(const std::vector<MembershipRecord> &memberships);

int64_t DurationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end);
}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_DIAGNOSTICS_H
