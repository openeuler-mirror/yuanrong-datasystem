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
 * Description: Hash topology routing and planning algorithm.
 */
#ifndef DATASYSTEM_TOPOLOGY_ALGORITHM_HASH_ALGORITHM_H
#define DATASYSTEM_TOPOLOGY_ALGORITHM_HASH_ALGORITHM_H

#include "datasystem/topology/algorithm/topology_algorithm.h"

namespace datasystem {
namespace topology {

class HashAlgorithm final : public IRoutingAlgorithm, public IPlanningAlgorithm {
public:
    static constexpr const char *DEFAULT_ALGORITHM_ID = "hash";
    static constexpr uint32_t DEFAULT_VIRTUAL_TOKEN_NUM = 128;

    explicit HashAlgorithm(AlgorithmId algorithmId = DEFAULT_ALGORITHM_ID,
                           uint32_t virtualTokenNum = DEFAULT_VIRTUAL_TOKEN_NUM);
    ~HashAlgorithm() override = default;
    HashAlgorithm(const HashAlgorithm &) = default;
    HashAlgorithm &operator=(const HashAlgorithm &) = default;
    HashAlgorithm(HashAlgorithm &&) = default;
    HashAlgorithm &operator=(HashAlgorithm &&) = default;

    AlgorithmId GetAlgorithmId() const override;
    Status BuildRoutingState(const TopologyDescriptor &snapshot,
                             std::unique_ptr<AlgorithmRoutingState> &routing) const override;
    Status BuildPlacementUnit(const RouteContext &context, const PlacementPolicyRule &policy,
                              PlacementUnit &unit) const override;
    Status Route(const AlgorithmRoutingState &routing, const PlacementUnit &unit, LogicalOwner &owner) const override;
    Status InitPlacement(const PlanInput &input, PlanResult &result) const override;
    Status PlanPlacement(const PlanInput &input, PlanResult &result) const override;
    Status DiffPlacement(const TopologyDescriptor &from, const TopologyDescriptor &to,
                         std::vector<OwnerChange> &changes) const override;
    Status ValidatePlacement(const TopologyDescriptor &topology, ValidateResult &result) const override;

private:
    AlgorithmId algorithmId_;
    uint32_t virtualTokenNum_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_ALGORITHM_HASH_ALGORITHM_H
