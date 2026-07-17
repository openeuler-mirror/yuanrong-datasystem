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
 * Description: Resolves Worker metadata owners from prebound deployment facts.
 */
#ifndef DATASYSTEM_WORKER_METADATA_ROUTE_RESOLVER_H
#define DATASYSTEM_WORKER_METADATA_ROUTE_RESOLVER_H

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/metadata_route_options.h"

namespace datasystem::worker {

/**
 * @brief Successful metadata-owner groups plus explicit per-key failures.
 */
struct MetaOwnerRouteGroups {
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    std::unordered_map<std::string, Status> failures;
};

/**
 * @brief Successful indexed metadata-owner groups plus explicit per-key failures.
 */
struct IndexedMetaOwnerRouteGroups {
    std::unordered_map<HostPort, std::vector<std::pair<std::string, size_t>>> groups;
    std::unordered_map<std::string, Status> failures;
};

/**
 * @brief Bind Worker metadata deployment facts to cluster placement once.
 */
class MetadataRouteResolver final {
public:
    /**
     * @brief Bind immutable metadata route options and a non-owned placement capability.
     * @param[in] placement Required only for distributed metadata mode; outlives this resolver.
     * @param[in] options Worker-owned metadata deployment facts to consume.
     */
    MetadataRouteResolver(const cluster::PlacementFacade *placement, MetadataRouteOptions options);

    /**
     * @brief Release non-owned route dependencies.
     */
    ~MetadataRouteResolver() = default;

    /**
     * @brief Disable copying a dependency-binding business adapter.
     */
    MetadataRouteResolver(const MetadataRouteResolver &) = delete;

    /**
     * @brief Disable copy assignment of a dependency-binding business adapter.
     */
    MetadataRouteResolver &operator=(const MetadataRouteResolver &) = delete;

    /**
     * @brief Resolve one metadata owner from a key using the prebound mode.
     * @param[in] key Binary-safe object or stream placement key.
     * @param[out] owner Resolved owner; unchanged on failure.
     * @return K_OK or topology/address status.
     */
    Status ResolveOwner(std::string_view key, HostPort &owner) const;

    /**
     * @brief Group keys using exactly one batch placement decision.
     * @param[in] keys Keys to group.
     * @return Successful groups plus per-key failures from the same topology version.
     * @note A batch placement failure is assigned to every key; it never falls back to per-key Locate.
     */
    MetaOwnerRouteGroups GroupOwners(const std::vector<std::string> &keys) const;

    /**
     * @brief Group keys and original indexes using exactly one batch placement decision.
     * @param[in] keys Keys to group without copying a second non-indexed projection.
     * @return Successful indexed groups plus per-key failures from the same topology version.
     */
    IndexedMetaOwnerRouteGroups GroupIndexedOwners(const std::vector<std::string> &keys) const;

private:
    const cluster::PlacementFacade *placement_;
    const MetadataRouteOptions options_;
};

}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_METADATA_ROUTE_RESOLVER_H
