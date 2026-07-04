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
 * Description: Backend-neutral topology transition plan builder.
 */
#ifndef DATASYSTEM_TOPOLOGY_ORCHESTRATION_TOPOLOGY_PLAN_BUILDER_H
#define DATASYSTEM_TOPOLOGY_ORCHESTRATION_TOPOLOGY_PLAN_BUILDER_H

#include "datasystem/topology/algorithm/topology_algorithm.h"

namespace datasystem {
namespace topology {

class TopologyPlanBuilder final {
public:
    explicit TopologyPlanBuilder(const IPlanningAlgorithm &algorithm);
    ~TopologyPlanBuilder() = default;

    /**
     * @brief Build a pure topology change plan from current facts and desired membership.
     * @param[in] request Ready, orderly leaving, and failed member facts.
     * @param[in] current Current committed topology, or empty when absent.
     * @param[in] currentRevision Store revision observed with current topology.
     * @param[in] hasCommittedTopology Whether current was read from repository.
     * @param[out] plan Computed plan with topology, tasks, and notify doorbells.
     * @return K_OK on success; K_INVALID for unsupported or unsafe transitions.
     */
    Status Build(const TopologyReconcileRequest &request, const TopologyDescriptor &current,
                 Revision currentRevision, bool hasCommittedTopology, TopologyChangePlan &plan) const;

private:
    const IPlanningAlgorithm &algorithm_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ORCHESTRATION_TOPOLOGY_PLAN_BUILDER_H
