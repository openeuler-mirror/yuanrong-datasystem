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
 * Description: Topology change request boundary for member lifecycle.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_CHANGE_HANDLER_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_CHANGE_HANDLER_H

#include "datasystem/topology/membership/cluster_membership.h"
#include "datasystem/topology/membership/membership_types.h"

namespace datasystem {
namespace topology {

class ITopologyChangeRequester {
public:
    virtual ~ITopologyChangeRequester() = default;

    /**
     * @brief Submit a topology scale-in request to the Coordinator topology control boundary.
     * @param[in] request Scale-in request built after local membership validation.
     * @param[in] observedMembershipRevision Revision of the membership snapshot used for validation.
     * @return K_OK when accepted; K_INVALID for malformed request; K_NOT_READY when unavailable;
     * K_WRITE_BACK_QUEUE_FULL when the bounded request queue is full. Infrastructure errors from the channel are
     * preserved.
     */
    virtual Status SubmitScaleInRequest(const ScaleInRequest &request, Revision observedMembershipRevision) = 0;
};

class TopologyChangeHandler final {
public:
    TopologyChangeHandler(ClusterMembership &membership, ITopologyChangeRequester &requester);
    ~TopologyChangeHandler() = default;
    TopologyChangeHandler(const TopologyChangeHandler &) = delete;
    TopologyChangeHandler &operator=(const TopologyChangeHandler &) = delete;
    TopologyChangeHandler(TopologyChangeHandler &&) = delete;
    TopologyChangeHandler &operator=(TopologyChangeHandler &&) = delete;

    /**
     * @brief Request local scale-in from the process lifecycle/composition boundary.
     * @param[in] reason Reason for scale-in.
     * @return K_OK when accepted by Coordinator; validation and requester errors are returned unchanged.
     */
    Status RequestScaleIn(ScaleInReason reason);

private:
    ClusterMembership &membership_;
    ITopologyChangeRequester &requester_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_CHANGE_HANDLER_H
