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
 * Description: Topology change request boundary for worker lifecycle.
 */
#include "datasystem/topology/runtime/topology_change_handler.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

TopologyChangeHandler::TopologyChangeHandler(ClusterMembership &membership, ITopologyChangeRequester &requester)
    : membership_(membership), requester_(requester)
{
}

Status TopologyChangeHandler::RequestScaleIn(ScaleInReason reason)
{
    ScaleInRequest request;
    Revision observedRevision = 0;
    RETURN_IF_NOT_OK(membership_.BuildLocalScaleInRequest(reason, request, observedRevision));
    return requester_.SubmitScaleInRequest(request, observedRevision);
}

}  // namespace topology
}  // namespace datasystem
