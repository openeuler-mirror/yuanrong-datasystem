/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Deterministic cluster topology task and notify materialization.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_MATERIALIZER_H
#define DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_MATERIALIZER_H

#include <map>

#include "datasystem/cluster/algorithm/topology_algorithm.h"
#include "datasystem/cluster/membership/membership_types.h"

namespace datasystem::cluster {

class TopologyTaskExecutor;

/**
 * @brief Complete canonical task/notify set for one active topology epoch.
 */
struct ExpectedDerivedState {
    std::vector<TopologyTask> tasks;
    std::map<std::string, TopologyTaskNotify> notifiesByAddress;
    std::vector<std::string> notifyRecipients;
    std::map<std::string, int64_t> restartTimestampsByAddress;
    // Canonical restart-only suffix encoded once and shared by every recipient in this generation.
    std::string canonicalRestartNotify;
};

/**
 * @brief Pure deterministic derivation from latest topology and placement diff.
 */
class TopologyTaskMaterializer final {
public:
    /**
     * @brief Construct the stateless materializer.
     */
    TopologyTaskMaterializer() = default;

    /**
     * @brief Destroy the stateless materializer.
     */
    ~TopologyTaskMaterializer() = default;

    /**
     * @brief Build an expected derived set.
     * @param[in] latest Latest active snapshot.
     * @param[in] plan Matching placement plan.
     * @param[out] expected Complete set.
     * @return Operation status.
     */
    Status BuildExpected(const TopologySnapshot &latest, const TopologyPlan &plan,
                         ExpectedDerivedState &expected) const;

    /**
     * @brief Rebuild an expected set from topology.
     * @param[in] latest Latest snapshot.
     * @param[in] algorithm Planning algorithm.
     * @param[out] expected Complete set.
     * @return Operation status.
     */
    Status RebuildExpected(const TopologySnapshot &latest, const IPlanningAlgorithm &algorithm,
                           ExpectedDerivedState &expected) const;

    /**
     * @brief Rebuild active tasks and optional centralized restart effects.
     * @param[in] latest Latest snapshot.
     * @param[in] algorithm Planning algorithm.
     * @param[in] memberships Memberships from the same reconciliation read.
     * @param[in] includeRestartFacts Whether to fan out current RESTARTING facts.
     * @param[out] expected Complete derived generation.
     * @return Operation status.
     */
    Status RebuildExpected(const TopologySnapshot &latest, const IPlanningAlgorithm &algorithm,
                           const std::vector<MembershipRecord> &memberships, bool includeRestartFacts,
                           ExpectedDerivedState &expected) const;

    /**
     * @brief Build one recipient's complete task and restart notification.
     * @param[in] expected Complete derived generation.
     * @param[in] address Canonical recipient address.
     * @param[out] notify Complete notification value.
     * @return Operation status.
     */
    Status BuildNotifyFor(const ExpectedDerivedState &expected, const std::string &address,
                          TopologyTaskNotify &notify) const;

    /**
     * @brief Build one recipient's canonical notify without re-encoding shared restart facts.
     * @param[in] expected Complete derived generation.
     * @param[in] address Canonical recipient address.
     * @param[out] value Canonical composite notify bytes.
     * @return Operation status.
     */
    Status BuildEncodedNotifyFor(const ExpectedDerivedState &expected, const std::string &address,
                                 std::string &value) const;

    /**
     * @brief Compute a cross-epoch business operation id.
     * @param[in] phase Callback phase.
     * @param[in] fence Complete execution fence.
     * @return Deterministic operation id.
     */
    static std::string BuildBusinessOperationId(TopologyCallbackPhase phase, const TopologyExecutionFence &fence);

private:
    friend class TopologyTaskExecutor;

    /**
     * @brief Compute an epoch-scoped task id.
     * @param[in] task Canonical task.
     * @return Deterministic task id.
     */
    static std::string BuildTaskId(const TopologyTask &task);
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_MATERIALIZER_H
