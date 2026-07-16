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
 * Description: Object-cache endpoint availability policy over narrow topology capabilities.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_ENDPOINT_POLICY_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_ENDPOINT_POLICY_H

#include <string>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/worker/metadata_route_resolver.h"

namespace datasystem::object_cache {

/**
 * @brief Apply Object-specific endpoint fallback without changing cluster-core semantics.
 */
class ObjectEndpointPolicy final {
public:
    /**
     * @brief Bind the metadata route and endpoint capabilities once.
     * @param[in] resolver Metadata resolver that outlives this policy.
     * @param[in] membership Membership view that outlives this policy.
     */
    ObjectEndpointPolicy(const worker::MetadataRouteResolver &resolver,
                         const cluster::MembershipEndpointView &membership);

    /**
     * @brief Release non-owned Object endpoint dependencies.
     */
    ~ObjectEndpointPolicy() = default;

    /**
     * @brief Disable copying non-owned endpoint dependencies.
     */
    ObjectEndpointPolicy(const ObjectEndpointPolicy &) = delete;

    /**
     * @brief Disable copy assignment of non-owned endpoint dependencies.
     */
    ObjectEndpointPolicy &operator=(const ObjectEndpointPolicy &) = delete;

    /**
     * @brief Check one endpoint using Object's optional directory-lag fallback.
     * @param[in] address Remote Worker or metadata owner address.
     * @param[in] allowDirectoryLag Whether NOT_READY/NOT_FOUND may degrade to available.
     * @return K_OK, membership status, or K_MASTER_TIMEOUT for a known unreachable endpoint.
     */
    Status CheckEndpoint(const HostPort &address, bool allowDirectoryLag) const;

    /**
     * @brief Resolve one metadata key and check its current endpoint.
     * @param[in] key Binary-safe Object metadata key.
     * @param[in] allowDirectoryLag Whether directory lag may degrade to available.
     * @return K_OK or route/endpoint status.
     */
    Status CheckMetaOwner(std::string_view key, bool allowDirectoryLag) const;

    /**
     * @brief Return a printable topology member id for diagnostics.
     * @param[in] address Canonical member address.
     * @return Printable UUID/id, or empty when the member is unavailable.
     */
    std::string GetDiagnosticMemberId(const std::string &address) const;

private:
    const worker::MetadataRouteResolver &resolver_;
    const cluster::MembershipEndpointView &membership_;
};

/**
 * @brief Append per-key routing failures to one Object fallback owner group.
 * @param[in,out] grouped Resolved and failed Object keys; original per-key Status values are retained.
 * @param[in] failureOwner Owner used by the existing Object fallback workflow.
 */
void AppendRouteFailures(worker::MetaOwnerRouteGroups &grouped, const HostPort &failureOwner = HostPort());

}  // namespace datasystem::object_cache

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJECT_ENDPOINT_POLICY_H
