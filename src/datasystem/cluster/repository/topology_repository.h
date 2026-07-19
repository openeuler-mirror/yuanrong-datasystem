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
 * Description: Single-key semantic repository for cluster topology.
 */
#ifndef DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_REPOSITORY_H
#define DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_REPOSITORY_H

#include <optional>
#include <string>
#include <vector>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/cluster/repository/topology_key_helper.h"

namespace datasystem::cluster {

enum class TopologyCasOutcome : uint8_t { COMMITTED, CONFLICT, UNKNOWN };
struct TopologyCasResult {
    TopologyCasOutcome outcome{ TopologyCasOutcome::UNKNOWN };
    std::optional<TopologyState> observed;
};
enum class TaskProgressOutcome : uint8_t { UPDATED, ALREADY_FINISHED, STALE, UNKNOWN };
struct TaskJanitorCandidate {
    TopologyTaskKind kind{ TopologyTaskKind::MIGRATE };
    std::string taskId;
    std::string matchToken;
};
struct NotifyJanitorCandidate {
    std::string address;
    TopologyTaskNotify notify;
    std::string matchToken;
};
struct ScaleInMetadataDoneJanitorCandidate {
    std::string key;
    std::string matchToken;
};
struct ScaleInMetadataDoneRecord {
    uint64_t batchEpoch{ 0 };
    std::string sourceId;
    std::string taskId;
    std::string businessOperationId;
};

class TopologyRepository final {
public:
    /**
     * @brief Bind semantic operations to non-owned dependencies.
     * @param[in] backend Role-exclusive backend that outlives this repository.
     * @param[in] keys Validated key helper that outlives this repository.
     */
    TopologyRepository(ICoordinationBackend &backend, const TopologyKeyHelper &keys);

    /**
     * @brief Destroy without shutting down the non-owned backend.
     */
    ~TopologyRepository() = default;

    /**
     * @brief Disable copying bound dependencies.
     */
    TopologyRepository(const TopologyRepository &) = delete;

    /**
     * @brief Disable copy assignment of bound dependencies.
     */
    TopologyRepository &operator=(const TopologyRepository &) = delete;

    /**
     * @brief Exact-read authoritative topology.
     * @param[in] timeoutMs Positive backend timeout.
     * @param[out] state Decoded topology; unchanged on failure.
     * @param[out] authorityRevision Exact-read revision.
     * @return Backend, K_NOT_FOUND, or validation status.
     */
    Status ReadTopology(int32_t timeoutMs, TopologyState &state, int64_t &authorityRevision) const;

    /**
     * @brief CAS topology by expected version with exact unknown read-back.
     * @param[in] expectedVersion Required current version.
     * @param[in] desired Complete next-version topology.
     * @param[out] result Semantic CAS outcome and optional observation.
     * @return Backend/read-back status.
     */
    Status CompareAndSwapTopology(uint64_t expectedVersion, const TopologyState &desired, TopologyCasResult &result);

    /**
     * @brief Read the authoritative membership collection.
     * @param[out] members Decoded address-key projected records.
     * @return Backend or validation status.
     */
    Status ReadMemberships(std::vector<MembershipRecord> &members) const;

    /**
     * @brief Exact-read one task with active-batch context.
     * @param[in] kind Task collection kind.
     * @param[in] taskId Exact deterministic task id.
     * @param[in] type Expected batch type.
     * @param[in] epoch Expected batch epoch.
     * @param[out] task Decoded task; unchanged on failure.
     * @return Backend or validation status.
     */
    Status ReadTask(TopologyTaskKind kind, const std::string &taskId, TopologyChangeType type, uint64_t epoch,
                    TopologyTask &task) const;

    /**
     * @brief Idempotently create one deterministic task.
     * @param[in] task Complete deterministic task.
     * @return K_OK when created/identical; K_INVALID on byte mismatch.
     */
    Status CreateTaskIfAbsent(const TopologyTask &task);

    /**
     * @brief Mark the complete fenced scope false-to-true in one task CAS.
     * @param[in] fence Current-epoch task authorization.
     * @param[out] outcome Semantic progress result.
     * @return Backend or validation status.
     */
    Status MarkTaskScopeFinished(const TopologyExecutionFence &fence, TaskProgressOutcome &outcome);

    /**
     * @brief Idempotently mark one ScaleIn task's metadata migration complete.
     * @param[in] record Complete marker identity.
     * @return K_OK when created or already identical.
     */
    Status MarkScaleInMetadataDone(const ScaleInMetadataDoneRecord &record);

    /**
     * @brief Count metadata-done markers for one ScaleIn source in one batch.
     * @param[in] batchEpoch Active ScaleIn batch epoch.
     * @param[in] sourceId Stable source member ID.
     * @param[out] count Marker count for the source/batch scope.
     * @return Backend or validation status.
     */
    Status CountScaleInMetadataDone(uint64_t batchEpoch, const std::string &sourceId, size_t &count) const;

    /**
     * @brief Exact-read one member notify.
     * @param[in] address Canonical member address.
     * @param[out] notify Decoded notify.
     * @return Backend or validation status.
     */
    Status ReadNotify(const std::string &address, TopologyTaskNotify &notify) const;

    /**
     * @brief List a bounded ETCD-janitor task snapshot.
     * @param[in] kind Task collection kind.
     * @param[in] limit Maximum result count.
     * @param[out] tasks Raw byte-match candidates.
     * @return Backend status.
     */
    Status ListTaskCandidatesForJanitor(TopologyTaskKind kind, size_t limit,
                                        std::vector<TaskJanitorCandidate> &tasks) const;

    /**
     * @brief List a bounded ETCD-janitor notify snapshot.
     * @param[in] limit Maximum result count.
     * @param[out] notifies Decoded byte-match candidates.
     * @return Backend or validation status.
     */
    Status ListNotifyCandidatesForJanitor(size_t limit, std::vector<NotifyJanitorCandidate> &notifies) const;

    /**
     * @brief List a bounded ETCD-janitor ScaleIn metadata marker snapshot.
     * @param[in] limit Maximum result count.
     * @param[out] markers Raw byte-match candidates.
     * @return Backend status.
     */
    Status ListScaleInMetadataDoneCandidatesForJanitor(
        size_t limit, std::vector<ScaleInMetadataDoneJanitorCandidate> &markers) const;

    /**
     * @brief CAS-rewrite complete notify references.
     * @param[in] address Canonical member address.
     * @param[in] expected Complete sorted notify.
     * @return Backend or validation status.
     */
    Status RewriteNotify(const std::string &address, const TopologyTaskNotify &expected);

    /**
     * @brief Conditionally delete one byte-identical stale task.
     * @param[in] candidate Raw byte-match candidate.
     * @param[out] deleted True only when deleted by this call.
     * @return Backend status.
     */
    Status DeleteTaskIfMatches(const TaskJanitorCandidate &candidate, bool &deleted);

    /**
     * @brief Conditionally tombstone or rewrite one byte-identical stale notify.
     * @param[in] candidate Match token plus desired notify; empty task ids request a tombstone.
     * @param[out] deleted True only when this call installed a logical-delete tombstone.
     * @return Backend status.
     */
    Status DeleteNotifyIfMatches(const NotifyJanitorCandidate &candidate, bool &deleted);

    /**
     * @brief Conditionally delete one byte-identical stale ScaleIn metadata marker.
     * @param[in] candidate Match token plus exact marker key.
     * @param[out] deleted True only when deleted by this call.
     * @return Backend status.
     */
    Status DeleteScaleInMetadataDoneIfMatches(const ScaleInMetadataDoneJanitorCandidate &candidate, bool &deleted);

private:
    /**
     * @brief Select a fixed task table.
     * @param[in] kind Task kind.
     * @return Stable table reference.
     */
    const std::string &TaskTable(TopologyTaskKind kind) const;
    ICoordinationBackend &backend_;
    const TopologyKeyHelper &keys_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_REPOSITORY_TOPOLOGY_REPOSITORY_H
