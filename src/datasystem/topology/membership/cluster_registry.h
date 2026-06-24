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
 * Description: Cluster table registry for topology membership.
 */
#ifndef DATASYSTEM_COMMON_TOPOLOGY_MEMBERSHIP_CLUSTER_REGISTRY_H
#define DATASYSTEM_COMMON_TOPOLOGY_MEMBERSHIP_CLUSTER_REGISTRY_H

#include <string>

#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/membership/membership_types.h"

namespace datasystem {
namespace topology {

class ClusterRegistryKeyHelper {
public:
    ClusterRegistryKeyHelper() = delete;
    ~ClusterRegistryKeyHelper() = delete;

    /**
     * @brief Build the cluster-table key used by ICoordinationBackend for one worker.
     * @param[in] workerId Canonical worker address.
     * @param[out] key Built store key relative to ETCD_CLUSTER_TABLE.
     * @return K_OK on success; K_INVALID when workerId is malformed.
     */
    static Status BuildWorkerKey(const WorkerId &workerId, std::string &key);

    /**
     * @brief Parse a cluster-table event/list key.
     * @param[in] key Store key. Both relative keys and full ETCD event keys are accepted.
     * @param[out] workerId Canonical worker address in the key.
     * @return K_OK on success; K_INVALID when key is not a valid cluster worker key.
     */
    static Status ParseWorkerKey(const std::string &key, WorkerId &workerId);
};

class IClusterRegistry {
public:
    virtual ~IClusterRegistry() = default;

    /**
     * @brief List current worker records from the existing cluster table.
     * @param[out] snapshot Snapshot decoded from cluster-table records.
     * @return K_OK on success. Malformed records are logged and excluded from the snapshot.
     */
    virtual Status ListWorkers(MembershipSnapshot &snapshot) = 0;

    /**
     * @brief Decode one coordination-backend event into a typed membership event.
     * @param[in] event Raw coordination-backend event.
     * @param[out] typed Typed worker event.
     * @return K_OK when event belongs to the cluster table; K_NOT_FOUND for unrelated events; K_INVALID for malformed
     * cluster-table payload.
     */
    virtual Status HandleWorkerEvent(const CoordinationEvent &event, WorkerWatchEvent &typed) = 0;
};

class ClusterRegistry final : public IClusterRegistry {
public:
    explicit ClusterRegistry(ICoordinationBackend &store);
    ~ClusterRegistry() override = default;
    ClusterRegistry(const ClusterRegistry &) = delete;
    ClusterRegistry &operator=(const ClusterRegistry &) = delete;
    ClusterRegistry(ClusterRegistry &&) = delete;
    ClusterRegistry &operator=(ClusterRegistry &&) = delete;

    Status ListWorkers(MembershipSnapshot &snapshot) override;
    Status HandleWorkerEvent(const CoordinationEvent &event, WorkerWatchEvent &typed) override;

private:
    ICoordinationBackend &store_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TOPOLOGY_MEMBERSHIP_CLUSTER_REGISTRY_H
