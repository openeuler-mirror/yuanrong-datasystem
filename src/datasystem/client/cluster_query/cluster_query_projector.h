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
#ifndef DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_PROJECTOR_H
#define DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_PROJECTOR_H

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "datasystem/client/cluster_query/cluster_query_types.h"
#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/utils/status.h"

namespace datasystem::client::cluster_query {

class ClusterQueryProjector final {
public:
    ClusterQueryProjector() = default;
    ~ClusterQueryProjector() = default;
    ClusterQueryProjector(const ClusterQueryProjector &) = delete;
    ClusterQueryProjector &operator=(const ClusterQueryProjector &) = delete;

    Status BuildCluster(const ClusterSourceSnapshot &snapshot, ClusterQueryResult &result) const;
    Status Route(const ClusterSourceSnapshot &snapshot, const std::vector<std::string_view> &keys,
                 RouteQueryResult &result) const;
    static Status ValidateRouteKeys(const std::vector<std::string_view> &keys);

private:
    void BuildRanges(const cluster::TopologySnapshot &topology,
                     std::map<std::string, std::vector<QueryHashRange>> &ranges) const;
    ClusterNodeHealth DeriveHealth(const cluster::Member &member,
                                   const MembershipObservation *membership) const noexcept;
    cluster::HashAlgorithm algorithm_;
};

}  // namespace datasystem::client::cluster_query

#endif  // DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_PROJECTOR_H
