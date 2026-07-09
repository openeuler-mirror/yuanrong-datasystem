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
 * Description: R0 owner endpoint resolver.
 */
#include "datasystem/topology/routing/owner_endpoint_resolver.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "datasystem/topology/algorithm/topology_algorithm.h"

namespace datasystem {
namespace topology {

OwnerEndpointResolver::OwnerEndpointResolver(std::shared_ptr<IRoutingView> routingView,
                                             std::shared_ptr<IMembershipEndpointView> endpointView)
    : OwnerEndpointResolver(std::move(routingView), std::move(endpointView), std::make_shared<HashAlgorithm>())
{
}

OwnerEndpointResolver::OwnerEndpointResolver(std::shared_ptr<IRoutingView> routingView,
                                             std::shared_ptr<IMembershipEndpointView> endpointView,
                                             std::shared_ptr<const IRoutingAlgorithm> routingAlgorithm)
    : routingView_(std::move(routingView)),
      endpointView_(std::move(endpointView)),
      routingAlgorithm_(std::move(routingAlgorithm))
{
}

Status OwnerEndpointResolver::LocateMetaOwner(const std::string &key, const RouteOptions &options, IRoutingCache *cache,
                                              RouteDecision &decision) const
{
    decision = RouteDecision{};
    CHECK_FAIL_RETURN_STATUS(endpointView_ != nullptr, K_INVALID, "Owner endpoint resolver endpoint view is not set.");
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr, K_INVALID, "Owner endpoint resolver routing view is not set.");
    CHECK_FAIL_RETURN_STATUS(routingAlgorithm_ != nullptr, K_INVALID,
                             "Owner endpoint resolver routing algorithm is not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    PlacementUnit placementUnit;
    RETURN_IF_NOT_OK(BuildPlacementUnit(key, placementUnit));
    return LocateWithSnapshot(placementUnit, options, snapshot, cache, decision);
}

Status OwnerEndpointResolver::LocateMetaOwnersBatch(const std::vector<std::string> &keys, const RouteOptions &options,
                                                    BatchRouteDecision &decision) const
{
    decision = BatchRouteDecision{};
    if (keys.empty()) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(endpointView_ != nullptr, K_INVALID, "Owner endpoint resolver endpoint view is not set.");
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr, K_INVALID, "Owner endpoint resolver routing view is not set.");
    CHECK_FAIL_RETURN_STATUS(routingAlgorithm_ != nullptr, K_INVALID,
                             "Owner endpoint resolver routing algorithm is not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    decision.routingVersion = snapshot->Version();

    RoutingCache cache(keys.size());
    for (const auto &key : keys) {
        RouteDecision route;
        PlacementUnit placementUnit;
        Status rc = BuildPlacementUnit(key, placementUnit);
        if (rc.IsOk()) {
            rc = LocateWithSnapshot(placementUnit, options, snapshot, &cache, route);
        }
        if (rc.IsError()) {
            decision.hasPartialFailure = true;
            decision.perKeyFailure.emplace(key, rc);
            continue;
        }
        decision.perKeyDecision.emplace(key, route);
        decision.groupsByEndpoint[route.OwnerAddress()].emplace_back(key);
    }
    return Status::OK();
}

Status OwnerEndpointResolver::LocateWithSnapshot(const PlacementUnit &placementUnit, const RouteOptions &options,
                                                 const std::shared_ptr<const RoutingSnapshot> &snapshot,
                                                 IRoutingCache *cache, RouteDecision &decision) const
{
    const RouteCacheKey cacheKey{ snapshot->Version(), placementUnit };
    if (cache != nullptr && cache->Lookup(cacheKey, decision)) {
        return Status::OK();
    }

    decision = RouteDecision{};
    decision.routingVersion = snapshot->Version();
    const auto *routingState = snapshot->RoutingState();
    CHECK_FAIL_RETURN_STATUS(routingState != nullptr, K_NOT_READY, "Routing snapshot has no algorithm state.");

    LogicalOwner owner;
    RETURN_IF_NOT_OK(routingAlgorithm_->Route(*routingState, placementUnit, owner));
    decision.routingVersion = owner.topologyVersion;
    decision.ownerNodeId = owner.nodeId;
    RETURN_IF_NOT_OK(ResolveOwner(owner.nodeId, options, decision));
    if (cache != nullptr) {
        cache->Store(cacheKey, decision);
    }
    return Status::OK();
}

Status OwnerEndpointResolver::ResolveOwner(const TopologyNodeId &nodeId, const RouteOptions &options,
                                           RouteDecision &decision) const
{
    RETURN_IF_NOT_OK(endpointView_->ResolveEndpoint(nodeId, decision.ownerEndpoint));
    if (options.requireAvailableTarget && decision.ownerEndpoint.availability != MemberAvailability::READY) {
        RETURN_STATUS(K_RPC_UNAVAILABLE, "Route target is not available in local endpoint view.");
    }
    return Status::OK();
}

Status OwnerEndpointResolver::BuildPlacementUnit(const std::string &key, PlacementUnit &placementUnit) const
{
    RouteContext context;
    context.key = key;
    PlacementPolicyRule policy;
    policy.algorithmId = routingAlgorithm_->GetAlgorithmId();
    return routingAlgorithm_->BuildPlacementUnit(context, policy, placementUnit);
}

}  // namespace topology
}  // namespace datasystem
