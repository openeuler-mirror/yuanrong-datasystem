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
#include "datasystem/topology/orchestration/topology_control_loop.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

TopologyControlLoop::TopologyControlLoop(ITopologyControllerRepository &repository,
                                         const IPlanningAlgorithm &algorithm)
    : repository_(repository), planBuilder_(algorithm), mutationApplier_(repository)
{
}

Status TopologyControlLoop::Reconcile(const TopologyReconcileRequest &request, TopologyReconcileResult &result)
{
    result = {};
    TopologyDescriptor current;
    Revision revision = 0;
    bool hasCommittedTopology = true;
    auto rc = repository_.GetCommittedTopology(current, revision);
    if (rc.GetCode() == K_NOT_FOUND) {
        current.clusterHasInit = false;
        hasCommittedTopology = false;
    } else {
        RETURN_IF_NOT_OK(rc);
    }

    auto reconcileRequest = request;
    TopologyTaskSummary taskSummary;
    rc = repository_.GetTaskSummary(taskSummary);
    if (rc.IsOk()) {
        reconcileRequest.hasUnfinishedTasks =
            reconcileRequest.hasUnfinishedTasks || taskSummary.unfinishedTransferTasks > 0
            || taskSummary.unfinishedRecoveryTasks > 0;
    } else if (rc.GetCode() != K_NOT_FOUND) {
        RETURN_IF_NOT_OK(rc);
    }

    TopologyChangePlan plan;
    rc = planBuilder_.Build(reconcileRequest, current, revision, hasCommittedTopology, plan);
    if (rc.IsError()) {
        result.status = rc;
        return rc;
    }
    rc = mutationApplier_.Apply(plan, result);
    result.diagnostics = std::move(plan.diagnostics);
    result.status = rc;
    return rc;
}

}  // namespace topology
}  // namespace datasystem
