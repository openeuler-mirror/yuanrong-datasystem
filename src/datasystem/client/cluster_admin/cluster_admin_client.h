/*
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
 * Description: Cluster admin client for removing stale per-address topology records by worker address.
 *              Backed by either EtcdStore (etcd/metastore) or CoordinatorServiceProxy (Coordinator).
 */
#ifndef DATASYSTEM_CLIENT_CLUSTER_ADMIN_CLUSTER_ADMIN_CLIENT_H
#define DATASYSTEM_CLIENT_CLUSTER_ADMIN_CLUSTER_ADMIN_CLIENT_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem::client::cluster_admin {

struct ClusterAdminOptions {
    std::string clusterName;
    std::string etcdAddress;
    std::string coordinatorAddress;
};

struct DeleteClusterMemberResult {
    std::string address;
    bool membershipDeleted = false;
    bool notifyDeleted = false;
    bool probeDeleted = false;
    bool ubHealthDeleted = false;
    bool topologyMemberRemoved = false;
    uint64_t topologyVersion = 0;
    std::string error;
};

class ClusterAdminClient final {
public:
    explicit ClusterAdminClient(ClusterAdminOptions options);
    ~ClusterAdminClient();
    ClusterAdminClient(const ClusterAdminClient &) = delete;
    ClusterAdminClient &operator=(const ClusterAdminClient &) = delete;
    ClusterAdminClient(ClusterAdminClient &&) = delete;
    ClusterAdminClient &operator=(ClusterAdminClient &&) = delete;

    Status Init();
    Status DeleteClusterMembers(const std::vector<std::string> &addresses,
                                 std::vector<DeleteClusterMemberResult> &results);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace datasystem::client::cluster_admin

#endif  // DATASYSTEM_CLIENT_CLUSTER_ADMIN_CLUSTER_ADMIN_CLIENT_H
