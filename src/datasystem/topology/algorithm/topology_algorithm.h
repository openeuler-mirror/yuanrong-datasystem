/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Topology algorithm routing and planning interfaces.
 */
#ifndef DATASYSTEM_TOPOLOGY_ALGORITHM_TOPOLOGY_ALGORITHM_H
#define DATASYSTEM_TOPOLOGY_ALGORITHM_TOPOLOGY_ALGORITHM_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

struct PlacementRange {
    uint32_t begin{ 0 };
    uint32_t end{ 0 };
};

struct OwnerChange {
    WorkerId fromWorkerId;
    WorkerId toWorkerId;
    std::vector<PlacementRange> ranges;
};

struct PlanInput {
    TopologyDescriptor current;
    std::vector<WorkerId> targetWorkerIds;
    std::string algorithmOptions;
};

struct PlanResult {
    AlgorithmId algorithmId;
    TopologyDescriptor next;
    std::vector<OwnerChange> ownerChanges;
    std::vector<std::string> diagnostics;
};

struct ValidateResult {
    bool valid{ false };
    std::vector<std::string> diagnostics;
};

class ITopologyAlgorithm {
public:
    virtual ~ITopologyAlgorithm() = default;

    /**
     * @brief Return the stable algorithm id used by placement policy rules and Coordinator planning.
     * @return Stable algorithm id.
     */
    virtual AlgorithmId GetAlgorithmId() const = 0;
};

class IRoutingAlgorithm : public virtual ITopologyAlgorithm {
public:
    ~IRoutingAlgorithm() override = default;

    /**
     * @brief Build immutable algorithm-owned routing state from committed topology.
     * @param[in] snapshot Committed topology descriptor.
     * @param[out] routing Built routing state owned by the caller.
     * @return K_OK on success; K_NOT_READY when no committed owner exists; K_INVALID for malformed topology.
     */
    virtual Status BuildRoutingState(const TopologyDescriptor &snapshot,
                                     std::unique_ptr<AlgorithmRoutingState> &routing) const = 0;

    /**
     * @brief Build one placement unit from route input and the selected policy rule.
     * @param[in] context Route input.
     * @param[in] policy Selected placement policy rule.
     * @param[out] unit Algorithm-owned placement unit.
     * @return K_OK on success; K_INVALID for malformed input or policy mismatch.
     */
    virtual Status BuildPlacementUnit(const RouteContext &context, const PlacementPolicyRule &policy,
                                      PlacementUnit &unit) const = 0;

    /**
     * @brief Route one placement unit to a logical owner using immutable routing state.
     * @param[in] routing Algorithm-owned routing state.
     * @param[in] unit Placement unit produced by BuildPlacementUnit.
     * @param[out] owner Logical owner before endpoint resolution.
     * @return K_OK on success; K_NOT_READY when no owner is available; K_INVALID for mismatched state/unit.
     */
    virtual Status Route(const AlgorithmRoutingState &routing, const PlacementUnit &unit,
                         LogicalOwner &owner) const = 0;
};

class IPlanningAlgorithm : public virtual ITopologyAlgorithm {
public:
    ~IPlanningAlgorithm() override = default;

    /**
     * @brief Build an initial placement when there is no committed topology.
     * @param[in] input Current topology and target member set. The current topology may be empty.
     * @param[out] result Pure planning result; no PB keys, task ids, notify targets, or CAS versions are returned.
     * @return K_OK on success; K_INVALID for invalid algorithm config or target members.
     */
    virtual Status InitPlacement(const PlanInput &input, PlanResult &result) const = 0;

    /**
     * @brief Plan a new topology from current topology and target member set.
     * @param[in] input Current committed topology and target member set.
     * @param[out] result Pure planning result with next topology and ownership changes.
     * @return K_OK on success; K_INVALID for invalid current topology or target members.
     */
    virtual Status PlanPlacement(const PlanInput &input, PlanResult &result) const = 0;

    /**
     * @brief Diff two topology descriptors and return only ownership changes.
     * @param[in] from Source topology descriptor.
     * @param[in] to Destination topology descriptor.
     * @param[out] changes Ownership changes grouped by source and target worker.
     * @return K_OK on success; K_INVALID for malformed topology descriptors.
     */
    virtual Status DiffPlacement(const TopologyDescriptor &from, const TopologyDescriptor &to,
                                 std::vector<OwnerChange> &changes) const = 0;

    /**
     * @brief Validate one topology descriptor without touching repository, PB writer, or Coordinator state.
     * @param[in] topology Topology descriptor to validate.
     * @param[out] result Validation result with diagnostics. K_OK means validation completed.
     * @return K_OK when validation completed.
     */
    virtual Status ValidatePlacement(const TopologyDescriptor &topology, ValidateResult &result) const = 0;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_ALGORITHM_TOPOLOGY_ALGORITHM_H
