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
 * Description: R0 membership endpoint view snapshot.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_MEMBERSHIP_ENDPOINT_VIEW_H
#define DATASYSTEM_TOPOLOGY_ROUTING_MEMBERSHIP_ENDPOINT_VIEW_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/routing/placement_types.h"

namespace datasystem {
namespace topology {

struct MembershipEndpointSnapshot {
    MembershipEndpointSnapshot() : localAddress()
    {
    }

    int64_t version = -1;
    std::string localNodeId;
    HostPort localAddress;
    std::unordered_map<std::string, MemberEndpoint> members;
    std::unordered_map<std::string, std::string> nodeIdsByAddress;
};

class IMembershipEndpointView {
public:
    virtual ~IMembershipEndpointView() = default;

    /**
     * @brief Resolve a member endpoint from the local immutable endpoint view snapshot.
     * @param[in] nodeId Topology node id.
     * @param[out] endpoint Resolved endpoint.
     * @return K_OK if found, K_NOT_READY if endpoint view is not published, K_NOT_FOUND if the member is absent.
     *
     * Request threads only read local snapshot state. This method must not perform RPC probing, repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status ResolveEndpoint(const std::string &nodeId, MemberEndpoint &endpoint) const = 0;

    /**
     * @brief Resolve a member endpoint by address from the local immutable endpoint view snapshot.
     * @param[in] nodeAddress Endpoint address string.
     * @param[out] endpoint Resolved endpoint.
     * @return K_OK if found, K_NOT_READY if endpoint view is not published, K_NOT_FOUND if the member is absent.
     */
    virtual Status ResolveEndpointByAddress(const std::string &nodeAddress, MemberEndpoint &endpoint) const = 0;

    /**
     * @brief Return the local member endpoint from the local immutable endpoint view snapshot.
     * @param[out] endpoint Local member endpoint.
     * @return K_OK if found, K_NOT_READY if endpoint view is not published, K_NOT_FOUND if local member is absent.
     */
    virtual Status GetLocalEndpoint(MemberEndpoint &endpoint) const = 0;
};

class MembershipEndpointView final : public IMembershipEndpointView {
public:
    MembershipEndpointView() = default;
    ~MembershipEndpointView() override = default;

    Status ResolveEndpoint(const std::string &nodeId, MemberEndpoint &endpoint) const override;
    Status ResolveEndpointByAddress(const std::string &nodeAddress, MemberEndpoint &endpoint) const override;
    Status GetLocalEndpoint(MemberEndpoint &endpoint) const override;

    /**
     * @brief Publish an immutable membership endpoint snapshot.
     * @param[in] snapshot Fully constructed endpoint view snapshot.
     */
    void Publish(std::shared_ptr<const MembershipEndpointSnapshot> snapshot);

private:
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<const MembershipEndpointSnapshot> snapshot_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_MEMBERSHIP_ENDPOINT_VIEW_H
