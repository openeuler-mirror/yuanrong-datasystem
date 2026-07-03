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
#include "datasystem/topology/routing/redirect_policy.h"

#include <utility>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {
bool IsSameWorker(const MemberEndpoint &lhs, const MemberEndpoint &rhs)
{
    if (!lhs.nodeId.empty() && !rhs.nodeId.empty()) {
        return lhs.nodeId == rhs.nodeId;
    }
    return !lhs.address.Empty() && !rhs.address.Empty() && lhs.address == rhs.address;
}
}  // namespace

RedirectPolicy::RedirectPolicy(std::shared_ptr<IRoutingView> routingView,
                               std::shared_ptr<IMembershipEndpointView> endpointView,
                               std::shared_ptr<IOwnerEndpointResolver> locator)
    : routingView_(std::move(routingView)), endpointView_(std::move(endpointView)), locator_(std::move(locator))
{
}

Status RedirectPolicy::Evaluate(const std::string &key, const RouteOptions &options,
                                RedirectDecision &decision) const
{
    (void)options;
    decision = RedirectDecision{};
    CHECK_FAIL_RETURN_STATUS(!key.empty(), K_INVALID, "Redirect business key is empty.");
    CHECK_FAIL_RETURN_STATUS(endpointView_ != nullptr, K_INVALID, "Redirect policy endpointView is not set.");

    MemberEndpoint targetEndpoint;
    RETURN_IF_NOT_OK(ResolveRedirectTarget(key, targetEndpoint));
    return FinishDecision(targetEndpoint, decision);
}

Status RedirectPolicy::ResolveRedirectTarget(const std::string &key, MemberEndpoint &targetEndpoint) const
{
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr && locator_ != nullptr, K_INVALID,
                             "Redirect policy dependencies are not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_NOT_READY, "Routing snapshot is not published.");

    const uint32_t objectHash = MurmurHash3_32(key);
    bool hasHint = false;
    RETURN_IF_NOT_OK(ResolveHintTarget(objectHash, snapshot, targetEndpoint, hasHint));
    if (hasHint) {
        return Status::OK();
    }

    RouteDecision route;
    RouteOptions options;
    RETURN_IF_NOT_OK(locator_->LocateMetaOwner(key, options, nullptr, route));
    targetEndpoint = route.ownerEndpoint;
    return Status::OK();
}

Status RedirectPolicy::ResolveHintTarget(uint32_t objectHash,
                                         const std::shared_ptr<const RoutingSnapshot> &snapshot,
                                         MemberEndpoint &targetEndpoint, bool &hasHint) const
{
    hasHint = false;
    RoutingRedirectHint hint;
    if (!snapshot->FindRedirectHint(objectHash, hint)) {
        return Status::OK();
    }

    targetEndpoint = MemberEndpoint{ hint.targetNodeId, hint.targetAddress, MemberAvailability::NOT_READY };
    if (!hint.targetNodeId.empty()) {
        MemberEndpoint resolvedEndpoint;
        if (endpointView_->ResolveEndpoint(hint.targetNodeId, resolvedEndpoint).IsOk()) {
            targetEndpoint = resolvedEndpoint;
        }
    }
    CHECK_FAIL_RETURN_STATUS(!targetEndpoint.address.Empty(), K_INVALID, "Redirect hint target address is empty.");
    hasHint = true;
    return Status::OK();
}

Status RedirectPolicy::FinishDecision(const MemberEndpoint &targetEndpoint, RedirectDecision &decision) const
{
    MemberEndpoint localNode;
    RETURN_IF_NOT_OK(endpointView_->GetLocalEndpoint(localNode));
    CHECK_FAIL_RETURN_STATUS(!targetEndpoint.address.Empty(), K_INVALID, "Redirect target address is empty.");
    decision.targetEndpoint = targetEndpoint;
    if (IsSameWorker(localNode, decision.targetEndpoint)) {
        decision.action = RedirectAction::SERVE_LOCAL;
        return Status::OK();
    }
    decision.action = RedirectAction::REDIRECT;
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
