/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Pure cluster topology state-transition construction.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_PLAN_BUILDER_H
#define DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_PLAN_BUILDER_H

#include "datasystem/cluster/algorithm/topology_algorithm.h"

namespace datasystem::cluster {

/**
 * @brief Pure construction of start/final topology states; no IO and no task materialization.
 */
class TopologyPlanBuilder final {
public:
    /**
     * @brief Bind to a planning algorithm.
     * @param[in] algorithm Algorithm that outlives this builder.
     */
    explicit TopologyPlanBuilder(const IPlanningAlgorithm &algorithm);

    /**
     * @brief Destroy the plan builder.
     */
    ~TopologyPlanBuilder() = default;

    /**
     * @brief Bootstrap ready INITIAL members.
     * @param[in] latest Latest topology.
     * @param[in] ready Ready identities.
     * @param[out] next Next topology.
     * @return Operation status.
     */
    Status BuildBootstrap(const TopologyState &latest, const std::vector<MemberIdentity> &ready,
                          TopologyState &next) const;

    /**
     * @brief Start one ScaleOut batch.
     * @param[in] latest Latest topology.
     * @param[in] selected INITIAL identities.
     * @param[out] plan Next plan.
     * @return Operation status.
     */
    Status BuildScaleOutStart(const TopologyState &latest, const std::vector<MemberIdentity> &selected,
                              TopologyPlan &plan) const;

    /**
     * @brief Remove failed JOINING members and replan.
     * @param[in] latest Latest topology.
     * @param[in] failedJoining Failed identities.
     * @param[out] plan Next plan.
     * @return Operation status.
     */
    Status BuildScaleOutReplan(const TopologyState &latest, const std::vector<MemberIdentity> &failedJoining,
                               TopologyPlan &plan) const;

    /**
     * @brief Start one ScaleIn batch.
     * @param[in] latest Latest topology.
     * @param[in] selected PRE_LEAVING identities.
     * @param[out] plan Next plan.
     * @return Operation status.
     */
    Status BuildScaleInStart(const TopologyState &latest, const std::vector<MemberIdentity> &selected,
                             TopologyPlan &plan) const;

    /**
     * @brief Start or replan Failure.
     * @param[in] latest Latest topology.
     * @param[in] confirmed Complete failed set.
     * @param[out] plan Next plan.
     * @return Operation status.
     */
    Status BuildFailureStartOrReplan(const TopologyState &latest, const std::vector<MemberIdentity> &confirmed,
                                     TopologyPlan &plan) const;

    /**
     * @brief Finalize ScaleOut.
     * @param[in] latest Latest topology.
     * @param[out] next Next topology.
     * @return Operation status.
     */
    Status BuildScaleOutFinal(const TopologyState &latest, TopologyState &next) const;

    /**
     * @brief Finalize ScaleIn.
     * @param[in] latest Latest topology.
     * @param[out] next Next topology.
     * @return Operation status.
     */
    Status BuildScaleInFinal(const TopologyState &latest, TopologyState &next) const;

    /**
     * @brief Finalize Failure and re-anchor ordinary facts.
     * @param[in] latest Latest topology.
     * @param[out] next Next topology.
     * @return Operation status.
     */
    Status BuildFailureFinal(const TopologyState &latest, TopologyState &next) const;

    /**
     * @brief Clear an all-out topology.
     * @param[in] latest Latest topology.
     * @param[out] next Empty topology.
     * @return Operation status.
     */
    Status BuildClusterShutdownFinal(const TopologyState &latest, TopologyState &next) const;

private:
    const IPlanningAlgorithm &algorithm_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_PLAN_BUILDER_H
