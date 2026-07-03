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
 * Description: In-memory topology repository fake for module tests.
 */
#ifndef TESTS_UT_TOPOLOGY_TESTING_FAKE_TOPOLOGY_REPOSITORY_H
#define TESTS_UT_TOPOLOGY_TESTING_FAKE_TOPOLOGY_REPOSITORY_H

#include <mutex>
#include <vector>

#include "datasystem/topology/repository/topology_repository.h"

namespace datasystem {
namespace topology {

class FakeTopologyRepository final : public ITopologyRepository {
public:
    FakeTopologyRepository() = default;
    ~FakeTopologyRepository() override = default;
    FakeTopologyRepository(const FakeTopologyRepository &) = delete;
    FakeTopologyRepository &operator=(const FakeTopologyRepository &) = delete;
    FakeTopologyRepository(FakeTopologyRepository &&) = delete;
    FakeTopologyRepository &operator=(FakeTopologyRepository &&) = delete;

    /**
     * @brief Seed one committed topology snapshot for tests.
     * @param[in] topology Committed topology to expose through GetCommittedTopology.
     * @return K_OK on success; K_INVALID when topology is malformed for fake storage.
     */
    Status SeedCommittedTopology(const TopologyDescriptor &topology);

    /**
     * @brief Seed one transfer task record for tests.
     * @param[in] task Transfer task record.
     * @return K_OK on success; K_INVALID when the task is malformed.
     */
    Status SeedTransferTask(const TransferTaskRecord &task);

    /**
     * @brief Seed one recovery task record for tests.
     * @param[in] task Recovery task record.
     * @return K_OK on success; K_INVALID when the task is malformed.
     */
    Status SeedRecoveryTask(const RecoveryTaskRecord &task);

    /**
     * @brief Inject one transfer-progress CAS conflict for the next report call.
     */
    void InjectTransferProgressConflict();

    Status GetCommittedTopology(TopologyDescriptor &topology, Revision &revision) override;
    Status TryCreateCommittedTopology(const TopologyDescriptor &topology, Revision &revision) override;
    Status TryUpdateCommittedTopology(const TopologyDescriptor &expectedTopology, Revision expectedRevision,
                                      const TopologyDescriptor &nextTopology, Revision &revision) override;
    Status ClearEphemeralRecords() override;
    Status ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks) override;
    Status ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks) override;
    Status TryCreateTransferTaskRecord(const TransferTaskRecord &task, Revision &revision) override;
    Status DeleteTransferTaskRecord(const TaskId &taskId) override;
    Status TryCreateRecoveryTaskRecord(const RecoveryTaskRecord &task, Revision &revision) override;
    Status DeleteRecoveryTaskRecord(const TaskId &taskId) override;
    Status UpsertTaskNotify(const TaskNotify &notify, Revision &revision) override;
    Status ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update) override;
    Status ReportTransferProgressBatch(const TaskId &taskId, const std::vector<TaskProgressUpdate> &updates) override;
    Status ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update) override;
    Status ReportRecoveryProgressBatch(const TaskId &taskId, const std::vector<TaskProgressUpdate> &updates) override;
    Status HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed) override;

private:
    mutable std::mutex mutex_;
    TopologyDescriptor topology_;
    Revision revision_{ 0 };
    bool hasTopology_{ false };
    bool transferConflict_{ false };
    std::vector<TransferTaskRecord> transferTasks_;
    std::vector<RecoveryTaskRecord> recoveryTasks_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // TESTS_UT_TOPOLOGY_TESTING_FAKE_TOPOLOGY_REPOSITORY_H
