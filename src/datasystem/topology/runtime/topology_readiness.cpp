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
 * Description: Readiness view for topology routing snapshots.
 */
#include "datasystem/topology/runtime/topology_readiness.h"

#include <memory>

#include "datasystem/topology/routing/routing_snapshot.h"
#include "datasystem/topology/routing/routing_view.h"

namespace datasystem {
namespace topology {

TopologyReadiness::TopologyReadiness(const IRoutingView &routingView) : routingView_(routingView)
{
}

Status TopologyReadiness::GetSnapshot(TopologyReadinessSnapshot &snapshot) const
{
    snapshot = {};
    std::shared_ptr<const RoutingSnapshot> routingSnapshot;
    auto rc = routingView_.GetSnapshot(routingSnapshot);
    snapshot.lastStatus = rc;
    if (rc.IsError()) {
        return Status::OK();
    }
    snapshot.state = TopologyReadinessState::READY;
    snapshot.ready = true;
    snapshot.routingVersion = routingSnapshot->Version();
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
