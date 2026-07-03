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
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_OWNER_ENDPOINT_RESOLVER_H
#define DATASYSTEM_TOPOLOGY_ROUTING_OWNER_ENDPOINT_RESOLVER_H

#include <memory>
#include <vector>

#include "datasystem/topology/algorithm/topology_algorithm.h"
#include "datasystem/topology/routing/membership_endpoint_view.h"
#include "datasystem/topology/routing/routing_cache.h"
#include "datasystem/topology/routing/routing_view.h"

namespace datasystem {
namespace topology {

class IOwnerEndpointResolver {
public:
    virtual ~IOwnerEndpointResolver() = default;

    /**
     * @brief Locate the metadata owner endpoint for one business key.
     * @param[in] key Business business key.
     * @param[in] options Route options.
     * @param[in,out] cache Optional request-local route cache.
     * @param[out] decision Route decision.
     * @return K_OK if owner is located, K_NOT_READY/K_NOT_FOUND/K_RPC_UNAVAILABLE/K_INVALID otherwise.
     *
     * Request threads only read local immutable snapshots. This method must not perform repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status LocateMetaOwner(const std::string &key, const RouteOptions &options, IRoutingCache *cache,
                                   RouteDecision &decision) const = 0;

    /**
     * @brief Locate metadata owners for a batch of business keys using one routing snapshot.
     * @param[in] keys Business business keys.
     * @param[in] options Route options.
     * @param[out] decision Batch route decision.
     * @return K_OK if batch-level prerequisites are ready. Per-key failures are stored in decision.
     */
    virtual Status LocateMetaOwnersBatch(const std::vector<std::string> &keys, const RouteOptions &options,
                                         BatchRouteDecision &decision) const = 0;
};

class OwnerEndpointResolver final : public IOwnerEndpointResolver {
public:
    OwnerEndpointResolver(std::shared_ptr<IRoutingView> routingView,
                          std::shared_ptr<IMembershipEndpointView> endpointView);
    OwnerEndpointResolver(std::shared_ptr<IRoutingView> routingView,
                          std::shared_ptr<IMembershipEndpointView> endpointView,
                          std::shared_ptr<const IRoutingAlgorithm> routingAlgorithm);
    ~OwnerEndpointResolver() override = default;

    Status LocateMetaOwner(const std::string &key, const RouteOptions &options, IRoutingCache *cache,
                           RouteDecision &decision) const override;
    Status LocateMetaOwnersBatch(const std::vector<std::string> &keys, const RouteOptions &options,
                                 BatchRouteDecision &decision) const override;

private:
    /**
     * @brief Locate one object's meta owner against a loaded routing snapshot.
     * @param[in] placementUnit Algorithm-owned placement unit for one business key.
     * @param[in] options Route options.
     * @param[in] snapshot Immutable routing snapshot to search.
     * @param[in] cache Optional request-local route cache.
     * @param[out] decision Route decision.
     * @return K_OK on success, K_NOT_READY if the snapshot has no routing state, K_NOT_FOUND if no owner is available,
     *         K_RPC_UNAVAILABLE if requireAvailableTarget rejects the owner.
     *
     * Serves from cache on a (version, placement unit) hit; otherwise asks the routing algorithm for the logical owner,
     * resolves its endpoint and stores the resulting decision in the cache when present.
     */
    Status LocateWithSnapshot(const PlacementUnit &placementUnit, const RouteOptions &options,
                              const std::shared_ptr<const RoutingSnapshot> &snapshot, IRoutingCache *cache,
                              RouteDecision &decision) const;

    /**
     * @brief Resolve a logical owner endpoint into the route decision.
     * @param[in] nodeId Logical owner topology node id to resolve.
     * @param[in] options Route options.
     * @param[out] decision Route decision, owner endpoint filled in.
     * @return K_OK on success, K_NOT_FOUND if the owner is absent from the endpoint view, K_RPC_UNAVAILABLE when
     *         requireAvailableTarget rejects a non-READY owner.
     */
    Status ResolveOwner(const TopologyNodeId &nodeId, const RouteOptions &options, RouteDecision &decision) const;

    Status BuildPlacementUnit(const std::string &key, PlacementUnit &placementUnit) const;

    std::shared_ptr<IRoutingView> routingView_;
    std::shared_ptr<IMembershipEndpointView> endpointView_;
    std::shared_ptr<const IRoutingAlgorithm> routingAlgorithm_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_OWNER_ENDPOINT_RESOLVER_H
