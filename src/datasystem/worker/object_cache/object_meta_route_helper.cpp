/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Local helpers for object metadata-owner route grouping.
 */
#include "datasystem/worker/object_cache/object_meta_route_helper.h"

#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
namespace datasystem::object_cache {
namespace {
struct IndexedMetaRoute {
    size_t index;
    std::string_view key;
};

Status ResolveMetaOwnerRouteForIndex(const IndexedMetaRoute &route, const Status &batchRc,
                                     const cluster::BatchPlacementDecision &decision,
                                     const cluster::PlacementFacade &placement, HostPort &address)
{
    if (batchRc.IsError()) {
        cluster::PlacementDecision fallback;
        RETURN_IF_NOT_OK(placement.Locate(route.key, fallback));
        return address.ParseString(fallback.committedOwnerAddress);
    }
    CHECK_FAIL_RETURN_STATUS(route.index < decision.decisions.size(), K_NOT_FOUND,
                             "Placement decision is missing for object key.");
    return address.ParseString(decision.decisions[route.index].committedOwnerAddress);
}
}  // namespace

Status ResolveMetaOwner(const std::string &objectKey, const cluster::PlacementFacade *placement,
                        const worker::MetadataRouteOptions &routeOptions, HostPort &address)
{
    if (routeOptions.centralizedMode) {
        address = routeOptions.masterAddress;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(placement != nullptr, K_NOT_READY, "Topology placement facade is not provided.");
    cluster::PlacementDecision decision;
    RETURN_IF_NOT_OK(placement->Locate(objectKey, decision));
    return address.ParseString(decision.committedOwnerAddress);
}

MetaOwnerRouteGroups BuildMetaOwnerRouteGroups(const std::vector<std::string> &objectKeys,
                                               const cluster::PlacementFacade *placement,
                                               const worker::MetadataRouteOptions &routeOptions)
{
    MetaOwnerRouteGroups result;
    if (objectKeys.empty()) {
        return result;
    }
    if (routeOptions.centralizedMode) {
        result.groups[routeOptions.masterAddress] = objectKeys;
        auto &indexed = result.indexedGroups[routeOptions.masterAddress];
        indexed.reserve(objectKeys.size());
        for (size_t index = 0; index < objectKeys.size(); ++index) {
            indexed.emplace_back(objectKeys[index], index);
        }
        return result;
    }
    if (placement == nullptr) {
        Status rc(K_NOT_READY, "Topology placement facade is not provided.");
        for (const auto &objectKey : objectKeys) {
            (void)result.failures.emplace(objectKey, rc);
        }
        LOG(INFO) << "Group object keys by topology placement failed, first errInfo: key is "
                  << result.failures.begin()->first << ", status is " << result.failures.begin()->second.ToString();
        return result;
    }

    std::vector<std::string_view> keys;
    keys.reserve(objectKeys.size());
    for (const auto &objectKey : objectKeys) {
        keys.emplace_back(objectKey);
    }
    cluster::BatchPlacementDecision decision;
    Status rc = placement->LocateBatch(keys, decision);
    for (size_t index = 0; index < objectKeys.size(); ++index) {
        const auto &objectKey = objectKeys[index];
        HostPort address;
        Status routeRc = ResolveMetaOwnerRouteForIndex({ index, objectKey }, rc, decision, *placement, address);
        if (routeRc.IsOk()) {
            result.groups[address].emplace_back(objectKey);
            result.indexedGroups[std::move(address)].emplace_back(objectKey, index);
            continue;
        }
        (void)result.failures.emplace(objectKey, routeRc);
        VLOG(1) << FormatString("objKey[%s] can not find master, status: %s", objectKey, routeRc.ToString());
    }
    if (!result.failures.empty()) {
        LOG(INFO) << "Group object keys by topology placement failed, first errInfo: key is "
                  << result.failures.begin()->first << ", status is " << result.failures.begin()->second.ToString();
    }
    return result;
}

MetaOwnerRouteGroups BuildMetaOwnerRouteGroups(const std::vector<std::string> &objectKeys,
                                               const worker::WorkerTopologyReferences *references)
{
    worker::MetadataRouteOptions options;
    if (references != nullptr) {
        options.centralizedMode = references->centralizedMetadata;
        (void)options.masterAddress.ParseString(references->metadataAddress);
    }
    return BuildMetaOwnerRouteGroups(objectKeys, references == nullptr ? nullptr : references->placement, options);
}
}  // namespace datasystem::object_cache
