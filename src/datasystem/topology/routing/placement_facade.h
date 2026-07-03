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
 * Description: B0 placement facade.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_FACADE_H
#define DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_FACADE_H

#include <memory>
#include <vector>

#include "datasystem/topology/routing/redirect_policy.h"
#include "datasystem/topology/routing/owner_endpoint_resolver.h"

namespace datasystem {
namespace topology {

class IPlacementFacade {
public:
    virtual ~IPlacementFacade() = default;

    /**
     * @brief Locate metadata owner for one business key.
     * @param[in] key Business business key.
     * @param[in] options Route options.
     * @param[out] decision Route decision.
     * @return Status of route lookup.
     */
    virtual Status LocateMetaOwner(const std::string &key, const RouteOptions &options,
                                   RouteDecision &decision) const = 0;

    /**
     * @brief Locate metadata owners for a batch of business keys.
     * @param[in] keys Business business keys.
     * @param[in] options Route options.
     * @param[out] decision Batch route decision.
     * @return Batch-level status. Per-key failures are stored in decision.
     */
    virtual Status LocateMetaOwnersBatch(const std::vector<std::string> &keys, const RouteOptions &options,
                                         BatchRouteDecision &decision) const = 0;

    /**
     * @brief Evaluate whether the local node should serve or redirect one business key.
     * @param[in] key Business business key.
     * @param[in] options Route and response policy options.
     * @param[out] decision Redirect decision.
     * @return Status of redirect policy evaluation.
     */
    virtual Status EvaluateRedirect(const std::string &key, const RouteOptions &options,
                                    RedirectDecision &decision) const = 0;

    /**
     * @brief Query current placement metadata for an business key without returning an endpoint.
     * @param[in] query Local placement query.
     * @param[out] decision Local placement decision.
     * @return Status of local placement query.
     */
    virtual Status QueryLocalPlacement(const LocalPlacementQuery &query, LocalPlacementDecision &decision) const = 0;

    /**
     * @brief Check whether a business key hashes into a set of ranges.
     * @param[in] key Business key, stream name, or other business id to hash.
     * @param[in] ranges Task scope ranges. Empty ranges never match.
     * @return True if the hashed key falls inside any range, false otherwise.
     *
     * This predicate may be called by task and migration selectors; it must not perform repository/backend IO,
     * CAS/List/Watch, task scan, or mutation.
     */
    virtual bool IsInRange(const std::string &key, const std::vector<Range> &ranges) const = 0;
};

class PlacementFacade final : public IPlacementFacade {
public:
    PlacementFacade(std::shared_ptr<IOwnerEndpointResolver> locator, std::shared_ptr<IRedirectPolicy> redirectPolicy);
    PlacementFacade(std::shared_ptr<IOwnerEndpointResolver> locator, std::shared_ptr<IRedirectPolicy> redirectPolicy,
                    std::shared_ptr<IRoutingView> routingView);
    ~PlacementFacade() override = default;

    Status LocateMetaOwner(const std::string &key, const RouteOptions &options,
                           RouteDecision &decision) const override;
    Status LocateMetaOwnersBatch(const std::vector<std::string> &keys, const RouteOptions &options,
                                 BatchRouteDecision &decision) const override;
    Status EvaluateRedirect(const std::string &key, const RouteOptions &options,
                            RedirectDecision &decision) const override;
    Status QueryLocalPlacement(const LocalPlacementQuery &query, LocalPlacementDecision &decision) const override;
    bool IsInRange(const std::string &key, const std::vector<Range> &ranges) const override;

private:
    std::shared_ptr<IOwnerEndpointResolver> locator_;
    std::shared_ptr<IRedirectPolicy> redirectPolicy_;
    std::shared_ptr<IRoutingView> routingView_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_FACADE_H
