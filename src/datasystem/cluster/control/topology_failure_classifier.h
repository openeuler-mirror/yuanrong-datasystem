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
 * @brief One bounded member sample whose authoritative membership key is absent.
 */
struct MemberAbsenceSample {
    MemberIdentity identity;
    MemberState state{ MemberState::INITIAL };
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
     * @param[in] nodeDeadTimeout Required continuous absence after membership key deletion.
     *            Zero confirms on the first successful observation of absence.
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
     * @brief Observe a caller-bounded deterministic subset without scanning a full topology.
     * @param[in] samples Missing committed-member samples selected by the caller.
     * @param[in] now Monotonic time.
     * @param[out] confirmedMissing Samples whose continuous absence reached nodeDeadTimeout, in input order.
     * @return Operation status.
     * @note Timers outside the current subset are discarded; this path does not materialize or sort full failures.
     */
    Status ObserveMissingSamples(const std::vector<MemberAbsenceSample> &samples,
                                 std::chrono::steady_clock::time_point now,
                                 std::vector<MemberAbsenceObservation> &confirmedMissing);

    /**
     * @brief Freeze accumulated absence while authoritative membership cannot be read.
     * @param[in] now Monotonic time at the first failed read.
     */
    void Pause(std::chrono::steady_clock::time_point now) noexcept;

    /**
     * @brief Require a new continuous-absence window after direct liveness evidence.
     * @param[in] address Canonical member address whose missing timer must be cleared.
     */
    void ResetMissing(const std::string &address) noexcept;

    /**
     * @brief Clear all process-local timers.
     */
    void Reset() noexcept;

private:
    void Resume(std::chrono::steady_clock::time_point now) noexcept;

    std::chrono::seconds nodeDeadTimeout_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> missingSince_;
    std::optional<std::chrono::steady_clock::time_point> pausedAt_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_FAILURE_CLASSIFIER_H
