/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Pure cluster topology routing and planning contracts.
 */
#ifndef DATASYSTEM_CLUSTER_ALGORITHM_TOPOLOGY_ALGORITHM_H
#define DATASYSTEM_CLUSTER_ALGORITHM_TOPOLOGY_ALGORITHM_H

#include <string>
#include <string_view>

#include "datasystem/cluster/model/topology_snapshot.h"

namespace datasystem::cluster {

using TopologyAlgorithmId = std::string;
struct ScaleOutPlanInput {
    TopologyState current;
    std::vector<MemberIdentity> joining;
    uint32_t tokensPerMember{ 4 };
};
struct ScaleInPlanInput {
    TopologyState current;
    std::vector<MemberIdentity> leaving;
};
struct FailurePlanInput {
    TopologyState current;
    std::vector<MemberIdentity> failed;
};

class ITopologyAlgorithm {
public:
    /**
     * @brief Destroy the algorithm interface.
     */
    virtual ~ITopologyAlgorithm() = default;

    /**
     * @brief Return the stable built-in id.
     * @return Algorithm id.
     */
    virtual TopologyAlgorithmId GetId() const = 0;
};

class IRoutingAlgorithm : public virtual ITopologyAlgorithm {
public:
    /**
     * @brief Destroy the routing facet.
     */
    ~IRoutingAlgorithm() override = default;

    /**
     * @brief Hash one binary-safe key.
     * @param[in] placementKey Key bytes.
     * @return 32-bit token.
     */
    virtual uint32_t Hash(std::string_view placementKey) const noexcept = 0;

    /**
     * @brief Locate a committed owner.
     * @param[in] snapshot Immutable Snapshot.
     * @param[in] token Placement token.
     * @param[out] owner Snapshot-lifetime member pointer.
     * @return K_OK, K_NOT_READY, or K_INVALID.
     */
    virtual Status LocateOwner(const TopologySnapshot &snapshot, uint32_t token, const Member *&owner) const = 0;

    /**
     * @brief Locate the owner after the current ordinary topology batch commits.
     * @param[in] snapshot Immutable Snapshot.
     * @param[in] token Placement token.
     * @param[out] owner Snapshot-lifetime prospective member pointer, or the committed owner outside ScaleOut/ScaleIn.
     * @return K_OK, K_NOT_READY, or K_INVALID.
     */
    virtual Status LocateProspectiveOwner(const TopologySnapshot &snapshot, uint32_t token,
                                          const Member *&owner) const = 0;
};

class IPlanningAlgorithm : public virtual ITopologyAlgorithm {
public:
    /**
     * @brief Destroy the planning facet.
     */
    ~IPlanningAlgorithm() override = default;

    /**
     * @brief Build bootstrap placement without migration.
     * @param[in] input Complete bootstrap input.
     * @param[out] plan Deterministic plan.
     * @return K_OK or K_INVALID.
     */
    virtual Status BuildInitialPlacement(const ScaleOutPlanInput &input, TopologyPlan &plan) const = 0;

    /**
     * @brief Plan one multi-member ScaleOut.
     * @param[in] input Complete ScaleOut input.
     * @param[out] plan Deterministic plan.
     * @return K_OK or K_INVALID.
     */
    virtual Status PlanScaleOut(const ScaleOutPlanInput &input, TopologyPlan &plan) const = 0;

    /**
     * @brief Plan one multi-member ScaleIn.
     * @param[in] input Complete ScaleIn input.
     * @param[out] plan Deterministic plan.
     * @return K_OK or K_INVALID.
     */
    virtual Status PlanScaleIn(const ScaleInPlanInput &input, TopologyPlan &plan) const = 0;

    /**
     * @brief Plan one confirmed multi-member Failure set.
     * @param[in] input Complete Failure input.
     * @param[out] plan Deterministic plan.
     * @return K_OK or K_INVALID.
     */
    virtual Status PlanFailure(const FailurePlanInput &input, TopologyPlan &plan) const = 0;

    /**
     * @brief Validate placement invariants only.
     * @param[in] state Domain topology state.
     * @return K_OK or K_INVALID.
     */
    virtual Status Validate(const TopologyState &state) const = 0;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ALGORITHM_TOPOLOGY_ALGORITHM_H
