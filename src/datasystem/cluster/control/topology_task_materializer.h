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

namespace datasystem::cluster {

class TopologyTaskExecutor;

/**
 * @brief Complete canonical task/notify set for one active topology epoch.
 */
struct ExpectedDerivedState {
    std::vector<TopologyTask> tasks;
    std::map<std::string, TopologyTaskNotify> notifiesByAddress;
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
