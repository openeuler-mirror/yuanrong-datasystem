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
 * Description: Topology repository codec.
 */
#ifndef DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H
#define DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H

#include <string>

#include "datasystem/common/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

class TopologyRepositoryCodec {
public:
    ~TopologyRepositoryCodec() = default;

    /**
     * @brief Decode committed topology bytes.
     * @param[in] bytes Serialized topology record.
     * @param[out] topology Decoded topology descriptor.
     * @return K_OK on success; K_INVALID for parse or semantic errors.
     */
    Status DecodeTopology(const std::string &bytes, TopologyDescriptor &topology) const;

    /**
     * @brief Encode committed topology bytes.
     * @param[in] topology Topology descriptor to serialize.
     * @param[out] bytes Serialized topology record.
     * @return K_OK on success; K_INVALID when topology is malformed.
     */
    Status EncodeTopology(const TopologyDescriptor &topology, std::string &bytes) const;

    /**
     * @brief Decode transfer tasks from HashRingPb.add_node_info.
     * @param[in] ringBytes Serialized HashRingPb.
     * @param[out] tasks Worker-facing transfer task records grouped by target and source worker.
     * @return K_OK on success; K_INVALID for parse or semantic errors.
     */
    Status DecodeTransferTasksFromRing(const std::string &ringBytes, std::vector<TransferTaskRecord> &tasks) const;

    /**
     * @brief Decode recovery tasks from HashRingPb.del_node_info.
     * @param[in] ringBytes Serialized HashRingPb.
     * @param[out] tasks Worker-facing recovery task records grouped by failed and recovery worker.
     * @return K_OK on success; K_INVALID for parse or semantic errors.
     */
    Status DecodeRecoveryTasksFromRing(const std::string &ringBytes, std::vector<RecoveryTaskRecord> &tasks) const;

    /**
     * @brief Apply one transfer task progress update to HashRingPb.add_node_info.
     * @param[in] currentBytes Current serialized HashRingPb.
     * @param[in] update Progress update to merge.
     * @param[out] newBytes Serialized ring after merge.
     * @return K_OK on success; K_INVALID when update mismatches ring tasks.
     */
    Status ApplyTransferProgressToRing(const std::string &currentBytes, const TaskProgressUpdate &update,
                                       std::string &newBytes) const;

    /**
     * @brief Apply one recovery task progress update to HashRingPb.del_node_info.
     * @param[in] currentBytes Current serialized HashRingPb.
     * @param[in] update Progress update to merge.
     * @param[out] newBytes Serialized ring after merge.
     * @return K_OK on success; K_INVALID when update mismatches ring tasks.
     */
    Status ApplyRecoveryProgressToRing(const std::string &currentBytes, const TaskProgressUpdate &update,
                                       std::string &newBytes) const;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H
