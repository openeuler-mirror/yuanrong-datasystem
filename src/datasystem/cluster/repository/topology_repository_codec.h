/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

/**
 * Description: Canonical protobuf boundary for cluster topology repository values.
 */
#ifndef DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H
#define DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H

#include <string>

#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

class TopologyRepositoryCodec final {
public:
    /**
     * @brief Deterministically encode schema version 1 topology.
     * @param[in] state Validated domain topology.
     * @param[out] value Canonical serialized bytes.
     * @return K_OK or K_INVALID.
     */
    static Status EncodeTopology(const TopologyState &state, std::string &value);

    /**
     * @brief Parse and validate one topology value.
     * @param[in] value Serialized topology bytes.
     * @param[out] state Decoded state; unchanged on failure.
     * @return K_OK or K_INVALID.
     */
    static Status DecodeTopology(const std::string &value, TopologyState &state);

    /**
     * @brief Encode one deterministic migrate task.
     * @param[in] task Validated migrate task.
     * @param[out] value Canonical serialized bytes.
     * @return K_OK or K_INVALID.
     */
    static Status EncodeMigrateTask(const TopologyMigrateTask &task, std::string &value);

    /**
     * @brief Decode a migrate task using exact key context.
     * @param[in] taskId Expected deterministic task id.
     * @param[in] type Expected batch type.
     * @param[in] epoch Expected batch epoch.
     * @param[in] value Serialized task bytes.
     * @param[out] task Decoded task; unchanged on failure.
     * @return K_OK or K_INVALID.
     */
    static Status DecodeMigrateTask(const std::string &taskId, TopologyChangeType type, uint64_t epoch,
                                    const std::string &value, TopologyMigrateTask &task);

    /**
     * @brief Encode one deterministic delete-member task.
     * @param[in] task Validated failure recovery task.
     * @param[out] value Canonical serialized bytes.
     * @return K_OK or K_INVALID.
     */
    static Status EncodeDeleteTask(const TopologyDeleteTask &task, std::string &value);

    /**
     * @brief Decode a delete-member task using exact key context.
     * @param[in] taskId Expected deterministic task id.
     * @param[in] epoch Expected batch epoch.
     * @param[in] value Serialized task bytes.
     * @param[out] task Decoded task; unchanged on failure.
     * @return K_OK or K_INVALID.
     */
    static Status DecodeDeleteTask(const std::string &taskId, uint64_t epoch, const std::string &value,
                                   TopologyDeleteTask &task);

    /**
     * @brief Encode one sorted and deduplicated notify.
     * @param[in] notify Complete notify value.
     * @param[out] value Canonical serialized bytes.
     * @return K_OK or K_INVALID.
     */
    static Status EncodeNotify(const TopologyTaskNotify &notify, std::string &value);

    /**
     * @brief Decode and validate one notify.
     * @param[in] value Serialized notify bytes.
     * @param[out] notify Decoded notify; unchanged on failure.
     * @return K_OK or K_INVALID.
     */
    static Status DecodeNotify(const std::string &value, TopologyTaskNotify &notify);

private:
    /**
     * @brief Static-only utility; construction is forbidden.
     */
    TopologyRepositoryCodec() = delete;

    /**
     * @brief Static-only utility; destruction is forbidden.
     */
    ~TopologyRepositoryCodec() = delete;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H
