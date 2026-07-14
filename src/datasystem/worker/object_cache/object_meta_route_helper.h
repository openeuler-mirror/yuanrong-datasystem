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
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_META_ROUTE_HELPER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_META_ROUTE_HELPER_H

#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/metadata_route_options.h"
#include "datasystem/worker/worker_topology_references.h"

namespace datasystem::object_cache {
struct MetaOwnerRouteGroups {
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    std::unordered_map<HostPort, std::vector<std::pair<std::string, size_t>>> indexedGroups;
    std::unordered_map<std::string, Status> failures;

    void AppendFailuresToGroup(const HostPort &address = HostPort())
    {
        if (failures.empty()) {
            return;
        }
        auto &keys = groups[address];
        keys.reserve(keys.size() + failures.size());
        for (const auto &failure : failures) {
            keys.emplace_back(failure.first);
        }
    }
};

/**
 * @brief Resolve one metadata owner using Worker business mode and cluster placement.
 * @param[in] objectKey Object key used for placement.
 * @param[in] placement Cluster placement service; required in distributed metadata mode.
 * @param[in] routeOptions Worker metadata-routing mode and local endpoint facts.
 * @param[out] address Resolved metadata owner.
 * @return K_OK on success; an input, topology, or address parsing status otherwise.
 */
Status ResolveMetaOwner(const std::string &objectKey, const cluster::PlacementFacade *placement,
                        const worker::MetadataRouteOptions &routeOptions, HostPort &address);

/**
 * @brief Group object keys by resolved metadata owner while preserving per-key failures.
 * @param[in] objectKeys Object keys to group.
 * @param[in] placement Cluster placement service; required in distributed metadata mode.
 * @param[in] routeOptions Worker metadata-routing mode and local endpoint facts.
 * @return Successful owner groups, indexed groups, and per-key resolution failures.
 */
MetaOwnerRouteGroups BuildMetaOwnerRouteGroups(const std::vector<std::string> &objectKeys,
                                               const cluster::PlacementFacade *placement,
                                               const worker::MetadataRouteOptions &routeOptions);

/**
 * @brief Group keys using the Worker composition's narrow topology references.
 * @param[in] objectKeys Object keys to group.
 * @param[in] references Worker-owned topology dependencies and routing mode.
 * @return Successful owner groups, indexed groups, and per-key resolution failures.
 */
MetaOwnerRouteGroups BuildMetaOwnerRouteGroups(const std::vector<std::string> &objectKeys,
                                               const worker::WorkerTopologyReferences *references);
}  // namespace datasystem::object_cache
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_META_ROUTE_HELPER_H
