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
#ifndef DATASYSTEM_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_H
#define DATASYSTEM_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_H

#include <vector>

#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/model/topology_types.h"
#include "datasystem/topology/repository/topology_repository_codec.h"

namespace datasystem {
namespace topology {

class ITopologyReader {
public:
    virtual ~ITopologyReader() = default;

    /**
     * @brief Read the committed topology fact from the existing hash-ring store entry.
     * @param[out] topology Decoded topology descriptor.
     * @param[out] revision Store revision observed by the read.
     * @return K_OK on success; K_NOT_FOUND when topology is absent; K_INVALID for malformed topology.
     */
    virtual Status GetCommittedTopology(TopologyDescriptor &topology, Revision &revision) = 0;

    /**
     * @brief Decode one committed-topology event delivered by the shared ICoordinationBackend event dispatcher.
     */
    virtual Status HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed) = 0;
};

class ITopologyWriter {
public:
    virtual ~ITopologyWriter() = default;

    /**
     * @brief Create the committed topology fact only when the root topology key is absent.
     * @param[in] topology Topology descriptor to encode into the committed topology key.
     * @param[out] revision Store revision observed after the create succeeds.
     * @return K_OK on successful create; K_TRY_AGAIN when the committed topology already exists.
     */
    virtual Status TryCreateCommittedTopology(const TopologyDescriptor &topology, Revision &revision) = 0;

    /**
     * @brief Update the committed topology fact only when the expected topology snapshot and revision still match.
     * @param[in] expectedTopology Topology descriptor previously read by the caller.
     * @param[in] expectedRevision Store revision observed with expectedTopology.
     * @param[in] nextTopology Topology descriptor to publish.
     * @param[out] revision Store revision observed after the update succeeds.
     * @return K_OK on successful update; K_NOT_FOUND when topology is absent; K_TRY_AGAIN when expected state is stale.
     */
    virtual Status TryUpdateCommittedTopology(const TopologyDescriptor &expectedTopology, Revision expectedRevision,
                                              const TopologyDescriptor &nextTopology, Revision &revision) = 0;

    /**
     * @brief Clear topology task and notify records that are only meaningful for an existing committed topology.
     * @return K_OK when all known ephemeral records are absent.
     */
    virtual Status ClearEphemeralRecords() = 0;
};

class ITopologyTaskReader {
public:
    virtual ~ITopologyTaskReader() = default;

    /**
     * @brief List transfer task records stored under migrate_tasks child keys.
     */
    virtual Status ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks) = 0;

    /**
     * @brief List recovery task records stored under delete_node_tasks child keys.
     */
    virtual Status ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks) = 0;
};

class ITopologyTaskWriter {
public:
    virtual ~ITopologyTaskWriter() = default;

    /**
     * @brief Create one transfer task fact only when the migrate task key is absent.
     * @param[in] task Transfer task record to encode under migrate_tasks.
     * @param[out] revision Store revision observed after the create succeeds.
     * @return K_OK on successful create; K_TRY_AGAIN when the task key already exists.
     */
    virtual Status TryCreateTransferTaskRecord(const TransferTaskRecord &task, Revision &revision) = 0;

    /**
     * @brief Delete one transfer task fact by exact task id.
     * @param[in] taskId Transfer task id stored under migrate_tasks.
     * @return K_OK when the record is absent after the call; K_INVALID when taskId is malformed.
     */
    virtual Status DeleteTransferTaskRecord(const TaskId &taskId) = 0;

    /**
     * @brief Create one recovery task fact only when the delete-node task key is absent.
     * @param[in] task Recovery task record to encode under delete_node_tasks.
     * @param[out] revision Store revision observed after the create succeeds.
     * @return K_OK on successful create; K_TRY_AGAIN when the task key already exists.
     */
    virtual Status TryCreateRecoveryTaskRecord(const RecoveryTaskRecord &task, Revision &revision) = 0;

    /**
     * @brief Delete one recovery task fact by exact task id.
     * @param[in] taskId Recovery task id stored under delete_node_tasks.
     * @return K_OK when the record is absent after the call; K_INVALID when taskId is malformed.
     */
    virtual Status DeleteRecoveryTaskRecord(const TaskId &taskId) = 0;
};

class ITopologyNotifyWriter {
public:
    virtual ~ITopologyNotifyWriter() = default;

    /**
     * @brief Overwrite one notify doorbell for a worker.
     * @param[in] notify Notify payload to encode under notify.
     * @param[out] revision Store revision observed after the write succeeds.
     * @return K_OK on successful write; K_INVALID for malformed notify payload.
     */
    virtual Status UpsertTaskNotify(const TaskNotify &notify, Revision &revision) = 0;
};

class ITopologyProgressReporter {
public:
    virtual ~ITopologyProgressReporter() = default;

    /**
     * @brief Report finished transfer range progress by CAS-updating one migrate task key.
     * @param[in] update Progress update to merge into the migrate task payload.
     * @return K_OK on success; K_NOT_FOUND when task is absent; K_INVALID for semantic mismatch; K_TRY_AGAIN on CAS
     * conflict.
     */
    virtual Status ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update) = 0;

    /**
     * @brief Report several finished transfer range progresses with one CAS update to one migrate task key.
     * @param[in] updates Progress updates to merge into the migrate task payload.
     * @return K_OK on success; K_NOT_FOUND when task is absent; K_INVALID for semantic mismatch; K_TRY_AGAIN on CAS
     * conflict.
     */
    virtual Status ReportTransferProgressBatch(const TaskId &taskId,
                                               const std::vector<TaskProgressUpdate> &updates) = 0;

    /**
     * @brief Report finished recovery range progress by CAS-updating one delete-node task key.
     * @param[in] update Progress update to merge into the delete-node task payload.
     * @return K_OK on success; K_NOT_FOUND when task is absent; K_INVALID for semantic mismatch; K_TRY_AGAIN on CAS
     * conflict.
     */
    virtual Status ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update) = 0;

    /**
     * @brief Report several finished recovery range progresses with one CAS update to one delete-node task key.
     * @param[in] updates Progress updates to merge into the delete-node task payload.
     * @return K_OK on success; K_NOT_FOUND when task is absent; K_INVALID for semantic mismatch; K_TRY_AGAIN on CAS
     * conflict.
     */
    virtual Status ReportRecoveryProgressBatch(const TaskId &taskId,
                                               const std::vector<TaskProgressUpdate> &updates) = 0;
};

/**
 * Repository view used by task executors. It intentionally excludes topology and notify writers so executor code can
 * only observe work and report range progress.
 */
class ITopologyTaskExecutionRepository : public virtual ITopologyTaskReader,
                                         public virtual ITopologyProgressReporter {
public:
    ~ITopologyTaskExecutionRepository() override = default;
};

/**
 * Repository view used by routing rebuilds. Routing may read the committed topology and unfinished tasks, but must not
 * publish topology transitions or task notifications.
 */
class ITopologyRoutingRepository : public virtual ITopologyReader, public virtual ITopologyTaskReader {
public:
    ~ITopologyRoutingRepository() override = default;
};

/**
 * Repository view used by the active topology controller. This is the only non-backend role that can publish committed
 * topology changes, create/delete task records, and write task notify doorbells.
 */
class ITopologyControllerRepository : public virtual ITopologyReader,
                                      public virtual ITopologyWriter,
                                      public virtual ITopologyTaskReader,
                                      public virtual ITopologyTaskWriter,
                                      public virtual ITopologyNotifyWriter {
public:
    ~ITopologyControllerRepository() override = default;
};

/**
 * Concrete repository surface implemented by TopologyRepository. Production code should inject the narrow role
 * interface needed by the caller instead of depending on this aggregate.
 */
class ITopologyRepository : public ITopologyControllerRepository,
                            public ITopologyTaskExecutionRepository,
                            public ITopologyRoutingRepository {
public:
    ~ITopologyRepository() override = default;
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
    Status TryCreateCommittedTopology(const TopologyDescriptor &topology, Revision &revision) override;
    Status TryUpdateCommittedTopology(const TopologyDescriptor &expectedTopology, Revision expectedRevision,
                                      const TopologyDescriptor &nextTopology, Revision &revision) override;
    Status TryCreateTransferTaskRecord(const TransferTaskRecord &task, Revision &revision) override;
    Status DeleteTransferTaskRecord(const TaskId &taskId) override;
    Status TryCreateRecoveryTaskRecord(const RecoveryTaskRecord &task, Revision &revision) override;
    Status DeleteRecoveryTaskRecord(const TaskId &taskId) override;
    Status UpsertTaskNotify(const TaskNotify &notify, Revision &revision) override;
    Status ClearEphemeralRecords() override;
    Status ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks) override;
    Status ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks) override;
    Status ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update) override;
    Status ReportTransferProgressBatch(const TaskId &taskId, const std::vector<TaskProgressUpdate> &updates) override;
    Status ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update) override;
    Status ReportRecoveryProgressBatch(const TaskId &taskId, const std::vector<TaskProgressUpdate> &updates) override;
    Status HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed) override;

private:
    ICoordinationBackend &store_;
    TopologyRepositoryCodec codec_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_REPOSITORY_TOPOLOGY_REPOSITORY_H
