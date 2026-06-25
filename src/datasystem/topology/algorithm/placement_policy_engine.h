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
 * Description: Placement policy selection engine.
 */
#ifndef DATASYSTEM_TOPOLOGY_ALGORITHM_PLACEMENT_POLICY_ENGINE_H
#define DATASYSTEM_TOPOLOGY_ALGORITHM_PLACEMENT_POLICY_ENGINE_H

#include <vector>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

class IPlacementPolicyEngine {
public:
    virtual ~IPlacementPolicyEngine() = default;

    /**
     * @brief Select one deterministic placement policy rule for a route context.
     * @param[in] context Route input.
     * @param[in] rules Candidate rules from routing snapshot.
     * @param[out] rule Selected rule.
     * @return K_OK on success; K_NOT_FOUND when no rule matches; K_INVALID for invalid or ambiguous rules.
     */
    virtual Status SelectPolicy(const RouteContext &context, const std::vector<PlacementPolicyRule> &rules,
                                PlacementPolicyRule &rule) const = 0;
};

class PlacementPolicyEngine final : public IPlacementPolicyEngine {
public:
    PlacementPolicyEngine() = default;
    ~PlacementPolicyEngine() override = default;
    PlacementPolicyEngine(const PlacementPolicyEngine &) = default;
    PlacementPolicyEngine &operator=(const PlacementPolicyEngine &) = default;
    PlacementPolicyEngine(PlacementPolicyEngine &&) = default;
    PlacementPolicyEngine &operator=(PlacementPolicyEngine &&) = default;

    Status SelectPolicy(const RouteContext &context, const std::vector<PlacementPolicyRule> &rules,
                        PlacementPolicyRule &rule) const override;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_ALGORITHM_PLACEMENT_POLICY_ENGINE_H
