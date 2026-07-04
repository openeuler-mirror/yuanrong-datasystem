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
 * Description: Backend-neutral topology reconcile entrypoint.
 */
#ifndef DATASYSTEM_TOPOLOGY_ORCHESTRATION_TOPOLOGY_CONTROL_LOOP_H
#define DATASYSTEM_TOPOLOGY_ORCHESTRATION_TOPOLOGY_CONTROL_LOOP_H

#include "datasystem/topology/orchestration/topology_mutation_applier.h"
#include "datasystem/topology/orchestration/topology_plan_builder.h"

namespace datasystem {
namespace topology {

class TopologyControlLoop final {
public:
    TopologyControlLoop(ITopologyControllerRepository &repository, const IPlanningAlgorithm &algorithm);
    ~TopologyControlLoop() = default;

    /**
     * @brief Run one reconcile tick from neutral membership facts.
     * @param[in] request Ready, leaving, and failed member facts.
     * @param[out] result Reconcile result.
     * @return K_OK on success; repository or planning errors otherwise.
     */
    Status Reconcile(const TopologyReconcileRequest &request, TopologyReconcileResult &result);

private:
    ITopologyControllerRepository &repository_;
    TopologyPlanBuilder planBuilder_;
    TopologyMutationApplier mutationApplier_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ORCHESTRATION_TOPOLOGY_CONTROL_LOOP_H
