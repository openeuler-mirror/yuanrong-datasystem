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
#include <optional>
#include <string>
#include <vector>

#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {
constexpr size_t TOPOLOGY_DIGEST_DIAGNOSTIC_PREFIX_SIZE = 12;
constexpr size_t TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT = 32;
constexpr int TOPOLOGY_VERBOSE_LOG_LEVEL = 1;
constexpr uint64_t TOPOLOGY_NO_ACTIVE_BATCH_EPOCH = 0;

class TopologySnapshot;

/**
 * @brief Select the owner ring represented in a topology log.
 */
enum class TopologyRingView : uint8_t {
    COMMITTED = 0,
    PROSPECTIVE = 1,
};

const char *TopologyChangeTypeName(TopologyChangeType type);
const char *MemberStateName(MemberState state);
const char *MemberLifecycleStateName(MemberLifecycleState state);

/**
 * @brief Return the stable diagnostic stage name for a callback invocation.
 */
const char *TopologyCallbackStageName(TopologyCallbackPhase phase, bool scaleInDataDrain);

std::string TopologyDiagnosticPrefix(const std::string &value,
                                     size_t prefixSize = TOPOLOGY_DIGEST_DIAGNOSTIC_PREFIX_SIZE);
std::string MemberIdForLog(const std::string &id, size_t prefixSize = TOPOLOGY_DIGEST_DIAGNOSTIC_PREFIX_SIZE);
std::string MemberIdentitySample(const std::vector<MemberIdentity> &members,
                                 size_t limit = TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT);
std::string MembershipSample(const std::vector<MembershipRecord> &memberships,
                             size_t limit = TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT);
std::string MembershipStateCounts(const std::vector<MembershipRecord> &memberships);

/**
 * @brief Format exact closed task ranges for structured logs.
 */
std::string TokenRangesForLog(const std::vector<TokenRange> &ranges);

/**
 * @brief Format topology task participants without depending on executor callback types.
 */
std::string TopologyParticipantScopeForLog(const MemberIdentity &executor,
                                           const std::optional<MemberIdentity> &source,
                                           const std::optional<MemberIdentity> &target,
                                           const std::optional<MemberIdentity> &failed);

/**
 * @brief Format the participants and exact ranges carried by an execution fence.
 */
std::string TopologyTaskScopeForLog(const TopologyExecutionFence &fence);

/**
 * @brief Format each member and its ranges in the selected ring view.
 */
std::string TopologyRingForLog(const TopologySnapshot &snapshot, TopologyRingView view,
                               size_t limit = TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT);

/**
 * @brief Format committed and prospective ring views with stable field names.
 */
std::string TopologyRingViewsForLog(const TopologySnapshot &snapshot,
                                    size_t limit = TOPOLOGY_MEMBER_LOG_SAMPLE_LIMIT);

int64_t DurationMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end);
}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_DIAGNOSTICS_H
