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
#include "datasystem/topology/routing/worker_locator.h"

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {
constexpr char LEGACY_HASH_ALGORITHM_ID[] = "hash";
constexpr char LEGACY_HASH_UNIT_TYPE[] = "hash-token";

RouteCacheKey BuildLegacyRouteCacheKey(int64_t version, uint32_t objectHash)
{
    PlacementUnit unit;
    unit.algorithmId = LEGACY_HASH_ALGORITHM_ID;
    unit.unitType = LEGACY_HASH_UNIT_TYPE;
    unit.opaqueUnit = std::to_string(objectHash);
    return RouteCacheKey{ version, unit };
}
}  // namespace

WorkerLocator::WorkerLocator(std::shared_ptr<IRoutingView> routingView, std::shared_ptr<IPlacementDirectory> directory)
    : routingView_(std::move(routingView)), directory_(std::move(directory))
{
}

Status WorkerLocator::LocateMetaOwner(const std::string &objectKey, const RouteOptions &options, IRoutingCache *cache,
                                      RouteDecision &decision) const
{
    decision = RouteDecision{};
    decision.keyHash = MurmurHash3_32(objectKey);
    CHECK_FAIL_RETURN_STATUS(directory_ != nullptr, K_INVALID, "Worker locator placement directory is not set.");
    if (options.centralizedMode) {
        return LocateCentralizedMaster(objectKey, options, decision);
    }
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr, K_INVALID, "Worker locator routing view is not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    return LocateWithSnapshot(decision.keyHash, options, snapshot, cache, decision);
}

Status WorkerLocator::LocateMetaOwnersBatch(const std::vector<std::string> &objectKeys, const RouteOptions &options,
                                            BatchRouteDecision &decision) const
{
    decision = BatchRouteDecision{};
    if (objectKeys.empty()) {
        return Status::OK();
    }
    if (options.centralizedMode) {
        RouteDecision route;
        RETURN_IF_NOT_OK(LocateCentralizedMaster(objectKeys.front(), options, route));
        const auto masterAddr = route.OwnerAddress();
        for (const auto &objectKey : objectKeys) {
            route.keyHash = MurmurHash3_32(objectKey);
            decision.perKeyDecision.emplace(objectKey, route);
            decision.groupsByEndpoint[masterAddr].emplace_back(objectKey);
        }
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(directory_ != nullptr, K_INVALID, "Worker locator placement directory is not set.");
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr, K_INVALID, "Worker locator routing view is not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    decision.routingVersion = snapshot->Version();

    RoutingCache cache(objectKeys.size());
    for (const auto &objectKey : objectKeys) {
        RouteDecision route;
        const uint32_t objectHash = MurmurHash3_32(objectKey);
        Status rc = LocateWithSnapshot(objectHash, options, snapshot, &cache, route);
        if (rc.IsError()) {
            decision.hasPartialFailure = true;
            decision.perKeyFailure.emplace(objectKey, rc);
            continue;
        }
        decision.perKeyDecision.emplace(objectKey, route);
        decision.groupsByEndpoint[route.OwnerAddress()].emplace_back(objectKey);
    }
    return Status::OK();
}

Status WorkerLocator::LocateWithSnapshot(uint32_t objectHash, const RouteOptions &options,
                                         const std::shared_ptr<const RoutingSnapshot> &snapshot, IRoutingCache *cache,
                                         RouteDecision &decision) const
{
    const auto cacheKey = BuildLegacyRouteCacheKey(snapshot->Version(), objectHash);
    if (cache != nullptr && cache->Lookup(cacheKey, decision)) {
        return Status::OK();
    }

    decision = RouteDecision{};
    decision.keyHash = objectHash;
    decision.routingVersion = snapshot->Version();
    RoutingOwnerEntry entry;
    Status rc = snapshot->Locate(objectHash, entry);
    if (rc.IsError()) {
        return rc;
    }
    decision.placementUnit = entry.unit;
    decision.ownerNodeId = entry.ownerNodeId;
    rc = ResolveOwner(entry, options, decision);
    if (rc.IsError()) {
        return rc;
    }
    if (cache != nullptr) {
        cache->Store(cacheKey, decision);
    }
    return Status::OK();
}

Status WorkerLocator::ResolveOwner(const RoutingOwnerEntry &entry, const RouteOptions &options,
                                   RouteDecision &decision) const
{
    PlacementEndpoint endpoint;
    RETURN_IF_NOT_OK(directory_->ResolveWorker(entry.ownerNodeId, endpoint));
    decision.ownerEndpoint = endpoint;
    if (options.requireAvailableTarget && decision.ownerEndpoint.availability != WorkerAvailability::READY) {
        RETURN_STATUS(K_RPC_UNAVAILABLE, "Route target is not available in local placement directory.");
    }
    return Status::OK();
}

Status WorkerLocator::LocateCentralizedMaster(const std::string &objectKey, const RouteOptions &options,
                                              RouteDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(directory_ != nullptr, K_INVALID, "Worker locator placement directory is not set.");
    decision = RouteDecision{};
    decision.keyHash = MurmurHash3_32(objectKey);
    CHECK_FAIL_RETURN_STATUS(!options.masterAddress.Empty(), K_INVALID, "Centralized master address is empty.");
    decision.ownerEndpoint.availability = WorkerAvailability::READY;
    decision.ownerEndpoint.address = options.masterAddress;
    INJECT_POINT("WorkerLocator.LocateCentralizedMaster.skipDirectoryCheck");
    decision.ownerNodeId = options.masterAddress.ToString();
    decision.ownerEndpoint.nodeId = decision.ownerNodeId;
    if (!decision.ownerNodeId.empty()) {
        PlacementEndpoint resolved;
        if (directory_->ResolveWorker(decision.ownerNodeId, resolved).IsOk()) {
            decision.ownerEndpoint.availability = resolved.availability;
        }
    }
    if (options.requireAvailableTarget && decision.ownerEndpoint.availability == WorkerAvailability::NOT_READY) {
        RETURN_STATUS(K_RPC_UNAVAILABLE, "Centralized master is not available in local placement directory.");
    }
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
