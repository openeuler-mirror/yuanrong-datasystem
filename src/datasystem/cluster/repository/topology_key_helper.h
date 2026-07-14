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
 * Description: Validated cluster topology keyspace builder.
 */
#ifndef DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_KEY_HELPER_H
#define DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_KEY_HELPER_H

#include <memory>
#include <string>

#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief The only builder for `/datasystem[/cluster_name]/...` topology tables.
 */
class TopologyKeyHelper final {
public:
    /**
     * @brief Validate an optional cluster namespace and construct its only key helper.
     * @param[in] clusterName Optional cluster namespace to validate and consume.
     * @param[out] helper Constructed helper; unchanged on failure.
     * @return K_OK on success; K_INVALID when a non-empty clusterName violates the keyspace contract.
     */
    static Status Create(std::string clusterName, std::unique_ptr<TopologyKeyHelper> &helper);

    /**
     * @brief Destroy the validated key helper.
     */
    ~TopologyKeyHelper() = default;

    /**
     * @brief Return the validated cluster namespace.
     * @return Stable cluster-name reference.
     */
    const std::string &ClusterName() const noexcept;

    /**
     * @brief Return the logical singleton topology table.
     * @return Stable table-name reference.
     */
    const std::string &TopologyTable() const noexcept;

    /**
     * @brief Return the logical migrate-task collection.
     * @return Stable table-name reference.
     */
    const std::string &MigrateTaskTable() const noexcept;

    /**
     * @brief Return the logical delete-task collection.
     * @return Stable table-name reference.
     */
    const std::string &DeleteTaskTable() const noexcept;

    /**
     * @brief Return the logical per-address notify collection.
     * @return Stable table-name reference.
     */
    const std::string &NotifyTable() const noexcept;

    /**
     * @brief Return the logical membership-lease collection.
     * @return Stable table-name reference.
     */
    const std::string &MembershipTable() const noexcept;

    /**
     * @brief Return the singleton topology relative key.
     * @return Stable empty-key reference.
     */
    static const std::string &TopologyKey() noexcept;

    /**
     * @brief Validate a deterministic task ID and build its exact relative key.
     * @param[in] taskId Deterministic task ID.
     * @param[out] key Exact relative key; unchanged on failure.
     * @return K_OK on success; K_INVALID for an invalid task ID.
     */
    static Status TaskKey(const std::string &taskId, std::string &key);

    /**
     * @brief Validate a canonical address and build its exact notify key.
     * @param[in] address Canonical member address.
     * @param[out] key Exact relative key; unchanged on failure.
     * @return K_OK on success; K_INVALID for an invalid address.
     */
    static Status NotifyKey(const std::string &address, std::string &key);

    /**
     * @brief Validate a canonical address and build its exact membership key.
     * @param[in] address Canonical member address.
     * @param[out] key Exact relative key; unchanged on failure.
     * @return K_OK on success; K_INVALID for an invalid address.
     */
    static Status MembershipKey(const std::string &address, std::string &key);

private:
    /**
     * @brief Construct prevalidated logical table names.
     * @param[in] clusterName Validated optional cluster namespace to consume.
     */
    explicit TopologyKeyHelper(std::string clusterName);

    std::string clusterName_;
    std::string topologyTable_;
    std::string migrateTaskTable_;
    std::string deleteTaskTable_;
    std::string notifyTable_;
    std::string membershipTable_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_KEY_HELPER_H
