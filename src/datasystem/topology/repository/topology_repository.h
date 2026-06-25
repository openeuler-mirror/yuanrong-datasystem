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
 * Description: Worker-facing topology repository.
 */
#ifndef DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_H
#define DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_H

#include <vector>

#include "datasystem/topology/coordination_backend/i_coordination_backend.h"
#include "datasystem/topology/model/topology_types.h"
#include "datasystem/topology/repository/topology_repository_codec.h"

namespace datasystem {
namespace topology {

class ITopologyRepository {
public:
    virtual ~ITopologyRepository() = default;

    /**
     * @brief Read the committed topology fact from the existing hash-ring store entry.
     * @param[out] topology Decoded topology descriptor.
     * @param[out] revision Store revision observed by the read.
     * @return K_OK on success; K_NOT_FOUND when topology is absent; K_INVALID for malformed topology.
     */
    virtual Status GetCommittedTopology(TopologyDescriptor &topology, Revision &revision) = 0;

    /**
     * @brief List transfer task records derived from HashRingPb.add_node_info.
     * @param[in] filter Worker and unfinished filtering options.
     * @param[out] tasks Decoded transfer tasks.
     * @return K_OK on success; K_INVALID when HashRingPb is malformed.
     */
    virtual Status ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks) = 0;

    /**
     * @brief List recovery task records derived from HashRingPb.del_node_info.
     * @param[in] filter Worker and unfinished filtering options.
     * @param[out] tasks Decoded recovery tasks.
     * @return K_OK on success; K_INVALID when HashRingPb is malformed.
     */
    virtual Status ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks) = 0;

    /**
     * @brief Report finished transfer range progress by CAS-updating HashRingPb.add_node_info.
     * @param[in] taskId Task id derived as `<target>|<source>`.
     * @param[in] update Progress update to merge into HashRingPb.
     * @return K_OK on success; K_NOT_FOUND when task is absent; K_INVALID for semantic mismatch; K_TRY_AGAIN on CAS
     * conflict.
     */
    virtual Status ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update) = 0;

    /**
     * @brief Report finished recovery range progress by CAS-updating HashRingPb.del_node_info.
     * @param[in] taskId Task id derived as `<failed>|<recovery>`.
     * @param[in] update Progress update to merge into HashRingPb.
     * @return K_OK on success; K_NOT_FOUND when task is absent; K_INVALID for semantic mismatch; K_TRY_AGAIN on CAS
     * conflict.
     */
    virtual Status ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update) = 0;

    /**
     * @brief Decode one committed-topology event delivered by the shared ICoordinationBackend event dispatcher.
     * @param[in] event Raw coordination-backend event.
     * @param[out] typed Decoded topology event.
     * @return K_OK when the event is the committed topology key; K_NOT_FOUND for unrelated events; K_INVALID for
     * malformed topology payload.
     */
    virtual Status HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed) = 0;
};

class TopologyRepository final : public ITopologyRepository {
public:
    explicit TopologyRepository(ICoordinationBackend &store);
    ~TopologyRepository() override = default;
    TopologyRepository(const TopologyRepository &) = delete;
    TopologyRepository &operator=(const TopologyRepository &) = delete;
    TopologyRepository(TopologyRepository &&) = delete;
    TopologyRepository &operator=(TopologyRepository &&) = delete;

    Status GetCommittedTopology(TopologyDescriptor &topology, Revision &revision) override;
    Status ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks) override;
    Status ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks) override;
    Status ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update) override;
    Status ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update) override;
    Status HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed) override;

private:
    ICoordinationBackend &store_;
    TopologyRepositoryCodec codec_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_H
