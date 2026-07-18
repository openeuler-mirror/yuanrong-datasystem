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
#ifndef DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_CLIENT_H
#define DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_CLIENT_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/client/cluster_query/cluster_query_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::client::cluster_query {

struct ClusterQueryOptions {
    std::string clusterName;
    std::string etcdAddress;
    std::string coordinatorAddress;
};

class ClusterQueryClient final {
public:
    explicit ClusterQueryClient(ClusterQueryOptions options);
    ~ClusterQueryClient();
    ClusterQueryClient(const ClusterQueryClient &) = delete;
    ClusterQueryClient &operator=(const ClusterQueryClient &) = delete;
    ClusterQueryClient(ClusterQueryClient &&) = delete;
    ClusterQueryClient &operator=(ClusterQueryClient &&) = delete;

    Status Init();
    Status QueryCluster(ClusterQueryResult &result);
    Status QueryRoutes(const std::vector<std::string> &keys, RouteQueryResult &result);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace datasystem::client::cluster_query

#endif  // DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_CLIENT_H
