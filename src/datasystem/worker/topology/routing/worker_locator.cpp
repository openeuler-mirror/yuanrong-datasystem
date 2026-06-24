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
#include "datasystem/worker/topology/routing/worker_locator.h"

#include <utility>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(master_address);

namespace datasystem {
namespace topology {

WorkerLocator::WorkerLocator(std::shared_ptr<IRoutingView> routingView, std::shared_ptr<IWorkerDirectory> directory)
    : routingView_(std::move(routingView)), directory_(std::move(directory))
{
}

Status WorkerLocator::LocateMetaOwner(const std::string &objectKey, const RouteOptions &options, IRoutingCache *cache,
                                      RouteDecision &decision) const
{
    decision = RouteDecision{};
    decision.objectKeyHash = MurmurHash3_32(objectKey);
    if (options.centralizedMode) {
        return LocateCentralizedMaster(objectKey, decision);
    }
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr && directory_ != nullptr, K_INVALID,
                             "Worker locator dependencies are not set.");

    std::shared_ptr<const RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(routingView_->GetSnapshot(snapshot));
    return LocateWithSnapshot(decision.objectKeyHash, options, snapshot, cache, decision);
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
        RETURN_IF_NOT_OK(LocateCentralizedMaster(objectKeys.front(), route));
        const auto metaAddrInfo = route.ToMetaAddrInfo();
        for (const auto &objectKey : objectKeys) {
            route.objectKeyHash = MurmurHash3_32(objectKey);
            decision.perKeyDecision.emplace(objectKey, route);
            decision.groupsByEndpoint[metaAddrInfo].emplace_back(objectKey);
        }
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(routingView_ != nullptr && directory_ != nullptr, K_INVALID,
                             "Worker locator dependencies are not set.");

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
        decision.groupsByEndpoint[route.ToMetaAddrInfo()].emplace_back(objectKey);
    }
    return Status::OK();
}

Status WorkerLocator::LocateWithSnapshot(uint32_t objectHash, const RouteOptions &options,
                                         const std::shared_ptr<const RoutingSnapshot> &snapshot, IRoutingCache *cache,
                                         RouteDecision &decision) const
{
    const RouteCacheKey cacheKey{ snapshot->Version(), objectHash };
    if (cache != nullptr && cache->Lookup(cacheKey, decision)) {
        return Status::OK();
    }

    decision = RouteDecision{};
    decision.objectKeyHash = objectHash;
    decision.routingVersion = snapshot->Version();
    RoutingOwnerEntry entry;
    Status rc = snapshot->Locate(objectHash, entry);
    if (rc.IsError()) {
        return rc;
    }
    decision.placementUnit = entry.unit;
    decision.ownerWorkerId = entry.ownerWorkerId;
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
    RETURN_IF_NOT_OK(directory_->ResolveWorker(entry.ownerWorkerId, decision.ownerEndpoint));
    if (options.requireAvailableTarget && decision.ownerEndpoint.availability != WorkerAvailability::READY) {
        RETURN_STATUS(K_RPC_UNAVAILABLE, "Route target is not available in local worker directory.");
    }
    return Status::OK();
}

Status WorkerLocator::LocateCentralizedMaster(const std::string &objectKey, RouteDecision &decision) const
{
    decision = RouteDecision{};
    decision.objectKeyHash = MurmurHash3_32(objectKey);
    decision.ownerEndpoint.availability = WorkerAvailability::READY;
    RETURN_IF_NOT_OK(decision.ownerEndpoint.address.ParseString(FLAGS_master_address));
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
