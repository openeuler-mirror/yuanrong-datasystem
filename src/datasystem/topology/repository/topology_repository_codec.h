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
#ifndef DATASYSTEM_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H
#define DATASYSTEM_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H

#include <string>
#include <vector>

#include "datasystem/topology/model/topology_types.h"

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

    Status DecodeMigrateTask(const std::string &bytes, const TaskId &taskId, TransferTaskRecord &task) const;
    Status EncodeMigrateTask(const TransferTaskRecord &task, std::string &bytes) const;
    Status ApplyTransferProgressToTask(const std::string &currentBytes, const TaskProgressUpdate &update,
                                       std::string &newBytes) const;
    Status ApplyTransferProgressBatchToTask(const std::string &currentBytes,
                                            const std::vector<TaskProgressUpdate> &updates,
                                            std::string &newBytes) const;
    Status DecodeDeleteNodeTask(const std::string &bytes, const TaskId &taskId, RecoveryTaskRecord &task) const;
    Status EncodeDeleteNodeTask(const RecoveryTaskRecord &task, std::string &bytes) const;
    Status ApplyRecoveryProgressToTask(const std::string &currentBytes, const TaskProgressUpdate &update,
                                       std::string &newBytes) const;
    Status ApplyRecoveryProgressBatchToTask(const std::string &currentBytes,
                                            const std::vector<TaskProgressUpdate> &updates,
                                            std::string &newBytes) const;
    Status DecodeNotify(const std::string &bytes, const TopologyAddress &nodeAddress, TaskNotify &notify) const;
    Status EncodeNotify(const TaskNotify &notify, std::string &bytes) const;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_CODEC_H
