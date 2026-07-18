/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Controller-local membership failure classification.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_FAILURE_CLASSIFIER_H
#define DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_FAILURE_CLASSIFIER_H

#include <chrono>
#include <optional>
#include <unordered_map>
#include <vector>

#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/model/topology_snapshot.h"

namespace datasystem::cluster {

/**
 * @brief One member absence observation with elapsed monotonic duration.
 */
struct MemberAbsenceObservation {
    MemberIdentity identity;
    MemberState state{ MemberState::INITIAL };
    int64_t missingMs{ 0 };
};

/**
 * @brief Disjoint actions produced by one successful membership observation.
 */
struct FailureClassification {
    std::vector<MemberIdentity> removeInitial;
    std::vector<MemberIdentity> removeJoining;
    std::vector<MemberIdentity> confirmedFailure;
    std::vector<MemberAbsenceObservation> newlyMissing;
    std::vector<MemberAbsenceObservation> restored;
    std::vector<MemberAbsenceObservation> confirmedMissing;
};

/**
 * @brief Per-Controller serial in-memory missing timers.
 */
class TopologyFailureClassifier final {
public:
    /**
     * @brief Construct a classifier.
     * @param[in] nodeDeadTimeout Required continuous absence.
     */
    explicit TopologyFailureClassifier(std::chrono::seconds nodeDeadTimeout);

    /**
     * @brief Destroy process-local timers.
     */
    ~TopologyFailureClassifier() = default;

    /**
     * @brief Disable copying Controller-local timer state.
     */
    TopologyFailureClassifier(const TopologyFailureClassifier &) = delete;

    /**
     * @brief Disable copy assignment of Controller-local timer state.
     */
    TopologyFailureClassifier &operator=(const TopologyFailureClassifier &) = delete;

    /**
     * @brief Observe one successful membership read-back.
     * @param[in] topology Latest topology.
     * @param[in] members Membership records.
     * @param[in] now Monotonic time.
     * @param[out] classification Disjoint actions.
     * @return Operation status.
     */
    Status Observe(const TopologySnapshot &topology, const std::vector<MembershipRecord> &members,
                   std::chrono::steady_clock::time_point now, FailureClassification &classification);

    /**
     * @brief Freeze accumulated absence while authoritative membership cannot be read.
     * @param[in] now Monotonic time at the first failed read.
     */
    void Pause(std::chrono::steady_clock::time_point now) noexcept;

    /**
     * @brief Clear all process-local timers.
     */
    void Reset() noexcept;

private:
    std::chrono::seconds nodeDeadTimeout_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> missingSince_;
    std::optional<std::chrono::steady_clock::time_point> pausedAt_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_FAILURE_CLASSIFIER_H
