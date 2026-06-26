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
 * Description: B0/R0 placement boundary DTOs.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_TYPES_H
#define DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_TYPES_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/master/meta_addr_info.h"

namespace datasystem {
namespace topology {

struct RoutingRange {
    uint32_t rangeBegin = 0;
    uint32_t rangeEnd = 0;

    bool IsWrapped() const
    {
        return rangeBegin > rangeEnd;
    }

    bool Contains(uint32_t objectHash) const
    {
        if (rangeBegin == rangeEnd) {
            return false;
        }
        if (IsWrapped()) {
            return objectHash > rangeBegin || objectHash <= rangeEnd;
        }
        return objectHash > rangeBegin && objectHash <= rangeEnd;
    }
};

enum class WorkerAvailability {
    READY,
    NOT_READY,
    UNCONFIRMED,
};

struct PlacementEndpoint {
    PlacementEndpoint() : address()
    {
    }

    PlacementEndpoint(std::string workerId, HostPort address, WorkerAvailability availability)
        : workerId(std::move(workerId)), address(std::move(address)), availability(availability)
    {
    }

    std::string workerId;
    HostPort address;
    WorkerAvailability availability = WorkerAvailability::NOT_READY;

    bool Empty() const
    {
        return workerId.empty() && address.Empty();
    }
};

struct RouteOptions {
    bool requireAvailableTarget = false;
    bool centralizedMode = false;
    HostPort masterAddress;
};

struct RouteDecision {
    RouteDecision() = default;

    uint32_t objectKeyHash = 0;
    int64_t routingVersion = -1;
    RoutingRange placementUnit;
    std::string ownerWorkerId;
    PlacementEndpoint ownerEndpoint;

    MetaAddrInfo ToMetaAddrInfo() const
    {
        MetaAddrInfo info;
        info.SetAddress(ownerEndpoint.address);
        info.SetDbName(ownerWorkerId);
        return info;
    }
};

struct BatchRouteDecision {
    int64_t routingVersion = -1;
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> groupsByEndpoint;
    std::unordered_map<std::string, RouteDecision> perKeyDecision;
    std::unordered_map<std::string, Status> perKeyFailure;
    bool hasPartialFailure = false;
};

struct LocalPlacementQuery {
    std::string objectKey;
};

struct LocalPlacementDecision {
    uint32_t objectKeyHash = 0;
    int64_t routingVersion = -1;
    RoutingRange placementUnit;
};

enum class RedirectAction {
    SERVE_LOCAL,
    REDIRECT,
};

struct RedirectDecision {
    RedirectDecision() = default;

    RedirectAction action = RedirectAction::SERVE_LOCAL;
    PlacementEndpoint targetEndpoint;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_TYPES_H
