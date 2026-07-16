/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Built-in MurmurHash3 cluster topology algorithm.
 */
#ifndef DATASYSTEM_CLUSTER_ALGORITHM_HASH_ALGORITHM_H
#define DATASYSTEM_CLUSTER_ALGORITHM_HASH_ALGORITHM_H

#include <unordered_map>

#include "datasystem/cluster/algorithm/topology_algorithm.h"

namespace datasystem::cluster {

class HashAlgorithm final : public IRoutingAlgorithm, public IPlanningAlgorithm {
public:
    /**
     * @brief Construct the stateless built-in algorithm.
     */
    HashAlgorithm() = default;

    /**
     * @brief Destroy the built-in algorithm.
     */
    ~HashAlgorithm() override = default;

    /**
     * @brief Return the built-in id.
     * @return Stable algorithm id.
     */
    TopologyAlgorithmId GetId() const override;

    /**
     * @brief Hash binary-safe key bytes.
     * @param[in] placementKey Key bytes.
     * @return 32-bit token.
     */
    uint32_t Hash(std::string_view placementKey) const noexcept override;

    /**
     * @brief Locate a committed owner.
     * @param[in] snapshot Snapshot.
     * @param[in] token Token.
     * @param[out] owner Snapshot-lifetime member pointer.
     * @return Lookup status.
     */
    Status LocateOwner(const TopologySnapshot &snapshot, uint32_t token, const Member *&owner) const override;

    /**
     * @brief Locate the owner after the active ScaleOut or ScaleIn batch commits.
     * @param[in] snapshot Snapshot.
     * @param[in] token Token.
     * @param[out] owner Snapshot-lifetime prospective member pointer.
     * @return Lookup status.
     */
    Status LocateProspectiveOwner(const TopologySnapshot &snapshot, uint32_t token,
                                  const Member *&owner) const override;

    /**
     * @brief Build bootstrap placement.
     * @param[in] input Input.
     * @param[out] plan Plan.
     * @return Status.
     */
    Status BuildInitialPlacement(const ScaleOutPlanInput &input, TopologyPlan &plan) const override;

    /**
     * @brief Plan ScaleOut.
     * @param[in] input Input.
     * @param[out] plan Plan.
     * @return Status.
     */
    Status PlanScaleOut(const ScaleOutPlanInput &input, TopologyPlan &plan) const override;

    /**
     * @brief Plan ScaleIn.
     * @param[in] input Input.
     * @param[out] plan Plan.
     * @return Status.
     */
    Status PlanScaleIn(const ScaleInPlanInput &input, TopologyPlan &plan) const override;

    /**
     * @brief Plan Failure.
     * @param[in] input Input.
     * @param[out] plan Plan.
     * @return Status.
     */
    Status PlanFailure(const FailurePlanInput &input, TopologyPlan &plan) const override;

    /**
     * @brief Validate placement.
     * @param[in] state State.
     * @return Status.
     */
    Status Validate(const TopologyState &state) const override;

private:
    /**
     * @brief Allocate unique deterministic tokens.
     * @param[in] members Members.
     * @param[in] tokensPerMember Count.
     * @param[out] tokens Tokens.
     * @return Status.
     */
    Status AllocateTokens(const std::vector<MemberIdentity> &members, uint32_t tokensPerMember,
                          std::unordered_map<std::string, std::vector<uint32_t>> &tokens) const;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ALGORITHM_HASH_ALGORITHM_H
