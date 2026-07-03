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
 * Description: Master metadata response redirect policy.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_REDIRECT_POLICY_H
#define DATASYSTEM_TOPOLOGY_ROUTING_REDIRECT_POLICY_H

#include <memory>

#include "datasystem/topology/routing/membership_endpoint_view.h"
#include "datasystem/topology/routing/owner_endpoint_resolver.h"
#include "datasystem/topology/routing/placement_types.h"
#include "datasystem/topology/routing/routing_view.h"

namespace datasystem {
namespace topology {

class IRedirectPolicy {
public:
    virtual ~IRedirectPolicy() = default;

    /**
     * @brief Evaluate redirect action for an business key using local R0 facts.
     * @param[in] key Business business key.
     * @param[in] options Route and response policy options.
     * @param[out] decision Redirect decision.
     * @return K_OK if a response action is generated, K_INVALID if context is malformed, K_NOT_READY if routing
     * snapshot or placement endpointView is not published, K_NOT_FOUND if local member or routing owner cannot be
     * resolved, K_RPC_UNAVAILABLE if the locator fallback rejects an unavailable target.
     *
     * Redirect policy only reads local immutable snapshots and placement endpointView facts. It must not scan tasks,
     * wait for topology progress, perform repository/backend IO, or execute migration/recovery/cleanup. Request paths
     * should not call this policy.
     */
    virtual Status Evaluate(const std::string &key, const RouteOptions &options,
                            RedirectDecision &decision) const = 0;
};

class RedirectPolicy final : public IRedirectPolicy {
public:
    RedirectPolicy(std::shared_ptr<IRoutingView> routingView,
                   std::shared_ptr<IMembershipEndpointView> endpointView,
                   std::shared_ptr<IOwnerEndpointResolver> locator);
    ~RedirectPolicy() override = default;

    Status Evaluate(const std::string &key, const RouteOptions &options,
                    RedirectDecision &decision) const override;

private:
    /**
     * @brief Resolve the member endpoint that should receive this object's request.
     * @param[in] key Business business key.
     * @param[out] targetEndpoint Resolved redirect target endpoint.
     * @return K_OK if a target is resolved, K_INVALID if dependencies are unset, K_NOT_READY if the snapshot is absent.
     *
     * Prefers a redirect hint for the object hash and falls back to the regular meta-owner lookup when no hint exists.
     */
    Status ResolveRedirectTarget(const std::string &key, MemberEndpoint &targetEndpoint) const;

    /**
     * @brief Resolve the redirect target from a routing redirect hint, if present.
     * @param[in] objectHash Hash value of the business key.
     * @param[in] snapshot Immutable routing snapshot to search.
     * @param[out] targetEndpoint Resolved hint target endpoint.
     * @param[out] hasHint True when a hint matched the hash, false otherwise.
     * @return K_OK on success, K_INVALID if the matched hint has an empty target address.
     *
     * When hasHint is false the caller must fall back to meta-owner lookup. A hint whose node id resolves in the
     * endpointView upgrades the endpoint to the endpointView's availability; otherwise the raw hint address is
     * NOT_READY.
     */
    Status ResolveHintTarget(uint32_t objectHash, const std::shared_ptr<const RoutingSnapshot> &snapshot,
                             MemberEndpoint &targetEndpoint, bool &hasHint) const;

    /**
     * @brief Finalize a redirect decision by comparing the target against the local member.
     * @param[in] targetEndpoint Redirect target endpoint.
     * @param[out] decision Redirect decision with action and target filled in.
     * @return K_OK on success, K_INVALID if the target address is empty or local member lookup fails.
     *
     * Sets SERVE_LOCAL when the target is the local member, REDIRECT otherwise.
     */
    Status FinishDecision(const MemberEndpoint &targetEndpoint, RedirectDecision &decision) const;

    std::shared_ptr<IRoutingView> routingView_;
    std::shared_ptr<IMembershipEndpointView> endpointView_;
    std::shared_ptr<IOwnerEndpointResolver> locator_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_REDIRECT_POLICY_H
