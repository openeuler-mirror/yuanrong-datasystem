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
 * Description: R0 worker locator.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_WORKER_LOCATOR_H
#define DATASYSTEM_TOPOLOGY_ROUTING_WORKER_LOCATOR_H

#include <memory>
#include <vector>

#include "datasystem/topology/routing/placement_directory.h"
#include "datasystem/topology/routing/routing_cache.h"
#include "datasystem/topology/routing/routing_view.h"

namespace datasystem {
namespace topology {

class IWorkerLocator {
public:
    virtual ~IWorkerLocator() = default;

    /**
     * @brief Locate the metadata owner endpoint for one object key.
     * @param[in] objectKey Business object key.
     * @param[in] options Route options.
     * @param[in,out] cache Optional request-local route cache.
     * @param[out] decision Route decision.
     * @return K_OK if owner is located, K_NOT_READY/K_NOT_FOUND/K_RPC_UNAVAILABLE/K_INVALID otherwise.
     *
     * Request threads only read local immutable snapshots. This method must not perform repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status LocateMetaOwner(const std::string &objectKey, const RouteOptions &options, IRoutingCache *cache,
                                   RouteDecision &decision) const = 0;

    /**
     * @brief Locate metadata owners for a batch of object keys using one routing snapshot.
     * @param[in] objectKeys Business object keys.
     * @param[in] options Route options.
     * @param[out] decision Batch route decision.
     * @return K_OK if batch-level prerequisites are ready. Per-key failures are stored in decision.
     */
    virtual Status LocateMetaOwnersBatch(const std::vector<std::string> &objectKeys, const RouteOptions &options,
                                         BatchRouteDecision &decision) const = 0;
};

class WorkerLocator final : public IWorkerLocator {
public:
    WorkerLocator(std::shared_ptr<IRoutingView> routingView, std::shared_ptr<IPlacementDirectory> directory);
    ~WorkerLocator() override = default;

    Status LocateMetaOwner(const std::string &objectKey, const RouteOptions &options, IRoutingCache *cache,
                           RouteDecision &decision) const override;
    Status LocateMetaOwnersBatch(const std::vector<std::string> &objectKeys, const RouteOptions &options,
                                 BatchRouteDecision &decision) const override;

private:
    /**
     * @brief Locate one object's meta owner against a loaded routing snapshot.
     * @param[in] objectHash Hash value of the business key.
     * @param[in] options Route options.
     * @param[in] snapshot Immutable routing snapshot to search.
     * @param[in] cache Optional request-local route cache.
     * @param[out] decision Route decision.
     * @return K_OK on success, K_NOT_READY if the snapshot is empty, K_NOT_FOUND if no range covers the hash,
     *         K_RPC_UNAVAILABLE if requireAvailableTarget rejects the owner.
     *
     * Serves from cache on a (version, hash) hit; otherwise locates the owner entry, resolves its endpoint and stores
     * the resulting decision in the cache when present.
     */
    Status LocateWithSnapshot(uint32_t objectHash, const RouteOptions &options,
                              const std::shared_ptr<const RoutingSnapshot> &snapshot, IRoutingCache *cache,
                              RouteDecision &decision) const;

    /**
     * @brief Resolve a routing owner entry's worker endpoint into the route decision.
     * @param[in] entry Routing owner entry to resolve.
     * @param[in] options Route options.
     * @param[out] decision Route decision, owner endpoint filled in.
     * @return K_OK on success, K_NOT_FOUND if the owner is absent from the directory, K_RPC_UNAVAILABLE when
     *         requireAvailableTarget rejects a non-READY owner.
     */
    Status ResolveOwner(const RoutingOwnerEntry &entry, const RouteOptions &options, RouteDecision &decision) const;

    /**
     * @brief Build a route decision pointing at the centralized master for centralized mode.
     * @param[in] objectKey Business object key.
     * @param[in] options Route options; when requireAvailableTarget is set the master must be READY in the local
     * placement directory.
     * @param[out] decision Route decision with the master endpoint set as owner.
     * @return K_OK on success, K_RPC_UNAVAILABLE when requireAvailableTarget rejects a non-READY master, otherwise the
     * parse status of the master address.
     *
     * The master is resolved through the placement directory (published under its address string) so the centralized
     * and distributed paths share the same local availability semantics.
     */
    Status LocateCentralizedMaster(const std::string &objectKey, const RouteOptions &options,
                                   RouteDecision &decision) const;

    std::shared_ptr<IRoutingView> routingView_;
    std::shared_ptr<IPlacementDirectory> directory_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_WORKER_LOCATOR_H
