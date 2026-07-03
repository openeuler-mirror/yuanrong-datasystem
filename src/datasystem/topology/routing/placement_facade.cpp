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
#include "datasystem/topology/routing/placement_facade.h"

#include <algorithm>
#include <utility>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {
bool ContainsHash(const Range &range, uint32_t hash)
{
    return RoutingRange{ range.first, range.second }.Contains(hash);
}

}  // namespace

PlacementFacade::PlacementFacade(std::shared_ptr<IOwnerEndpointResolver> locator,
                                 std::shared_ptr<IRedirectPolicy> redirectPolicy)
    : PlacementFacade(std::move(locator), std::move(redirectPolicy), nullptr)
{
}

PlacementFacade::PlacementFacade(std::shared_ptr<IOwnerEndpointResolver> locator,
                                 std::shared_ptr<IRedirectPolicy> redirectPolicy,
                                 std::shared_ptr<IRoutingView> routingView)
    : locator_(std::move(locator)), redirectPolicy_(std::move(redirectPolicy)), routingView_(std::move(routingView))
{
}

Status PlacementFacade::LocateMetaOwner(const std::string &key, const RouteOptions &options,
                                        RouteDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(locator_ != nullptr, K_INVALID, "Placement facade locator is not set.");
    return locator_->LocateMetaOwner(key, options, nullptr, decision);
}

Status PlacementFacade::LocateMetaOwnersBatch(const std::vector<std::string> &keys, const RouteOptions &options,
                                              BatchRouteDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(locator_ != nullptr, K_INVALID, "Placement facade locator is not set.");
    return locator_->LocateMetaOwnersBatch(keys, options, decision);
}

Status PlacementFacade::EvaluateRedirect(const std::string &key, const RouteOptions &options,
                                         RedirectDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(redirectPolicy_ != nullptr, K_INVALID, "Placement facade redirect policy is not set.");
    return redirectPolicy_->Evaluate(key, options, decision);
}

Status PlacementFacade::QueryLocalPlacement(const LocalPlacementQuery &query, LocalPlacementDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr, K_INVALID, "Placement facade routing view is not set.");
    decision = LocalPlacementDecision{};
    decision.keyHash = MurmurHash3_32(query.key);
    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    RoutingOwnerEntry entry;
    RETURN_IF_NOT_OK(snapshot->Locate(decision.keyHash, entry));
    decision.routingVersion = snapshot->Version();
    decision.placementUnit = entry.unit;
    return Status::OK();
}

bool PlacementFacade::IsInRange(const std::string &key, const std::vector<Range> &ranges) const
{
    const uint32_t keyHash = MurmurHash3_32(key);
    return std::any_of(ranges.begin(), ranges.end(),
                       [keyHash](const Range &range) { return ContainsHash(range, keyHash); });
}

}  // namespace topology
}  // namespace datasystem
