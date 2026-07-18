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
#ifndef DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_TYPES_H
#define DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_TYPES_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/model/topology_snapshot.h"

namespace datasystem::client::cluster_query {

inline constexpr int32_t CLUSTER_QUERY_TIMEOUT_MS = 5'000;
inline constexpr size_t MAX_QUERY_KEYS = 100'000;
inline constexpr size_t MAX_QUERY_KEY_BYTES = 1 * 1'024 * 1'024;
inline constexpr size_t MAX_QUERY_TOTAL_KEY_BYTES = 4 * 1'024 * 1'024;

enum class ClusterNodeHealth : uint8_t {
    HEALTHY = 0,
    DRAINING = 1,
    STARTING = 2,
    RECOVERING = 3,
    UNHEALTHY = 4
};

struct MembershipObservation {
    std::string address;
    cluster::MemberLifecycleState lifecycleState{ cluster::MemberLifecycleState::UNKNOWN };
};

struct ClusterSourceSnapshot {
    std::shared_ptr<const cluster::TopologySnapshot> topology;
    std::vector<MembershipObservation> memberships;
};

struct QueryHashRange {
    uint32_t start{ 0 };
    uint32_t end{ 0 };
    bool fullRing{ false };
};

struct ClusterNodeDiagnostic {
    std::string workerAddress;
    ClusterNodeHealth health{ ClusterNodeHealth::UNHEALTHY };
    cluster::MemberState state{ cluster::MemberState::INITIAL };
    std::vector<QueryHashRange> hashRanges;
};

struct ClusterQueryResult {
    uint64_t topologyVersion{ 0 };
    std::vector<ClusterNodeDiagnostic> nodes;
};

struct WorkerRouteDiagnostic {
    std::string workerAddress;
    std::vector<std::string> keys;
    ClusterNodeHealth health{ ClusterNodeHealth::UNHEALTHY };
};

struct RouteQueryResult {
    uint64_t topologyVersion{ 0 };
    std::vector<WorkerRouteDiagnostic> routes;
};

}  // namespace datasystem::client::cluster_query

#endif  // DATASYSTEM_CLIENT_CLUSTER_QUERY_CLUSTER_QUERY_TYPES_H
