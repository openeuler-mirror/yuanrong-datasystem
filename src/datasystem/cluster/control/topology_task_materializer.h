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
    // Canonical restart-only suffix encoded once and shared by every recipient in this generation.
    std::string canonicalRestartNotify;
};

/**
 * @brief Pure deterministic derivation from latest topology and placement diff.
 */
class TopologyTaskMaterializer final {
public:
    TopologyTaskMaterializer() = default;
    ~TopologyTaskMaterializer() = default;

    Status BuildExpected(const TopologySnapshot &latest, const TopologyPlan &plan,
                         ExpectedDerivedState &expected) const;

    Status RebuildExpected(const TopologySnapshot &latest, const IPlanningAlgorithm &algorithm,
                           ExpectedDerivedState &expected) const;

    Status RebuildExpected(const TopologySnapshot &latest, const IPlanningAlgorithm &algorithm,
                           const std::vector<MembershipRecord> &memberships, bool includeRestartFacts,
                           ExpectedDerivedState &expected) const;

    Status BuildEncodedNotifyFor(const ExpectedDerivedState &expected, const std::string &address,
                                 std::string &value) const;

    static std::string BuildBusinessOperationId(TopologyCallbackPhase phase, const TopologyExecutionFence &fence);

private:
    friend class TopologyTaskExecutor;

    static std::string BuildTaskId(const TopologyTask &task);
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_TASK_MATERIALIZER_H
