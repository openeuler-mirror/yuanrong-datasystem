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
 * Description: Topology repository.
 */
#include "datasystem/topology/repository/topology_repository.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/repository/topology_key_helper.h"

namespace datasystem {
namespace topology {
namespace {

bool HasUnfinishedRange(const std::vector<TokenRange> &ranges)
{
    for (const auto &range : ranges) {
        if (!range.finished) {
            return true;
        }
    }
    return false;
}

bool HasRangeNode(const std::vector<TokenRange> &ranges, const TopologyNodeId &nodeId)
{
    for (const auto &range : ranges) {
        if (range.nodeId == nodeId) {
            return true;
        }
    }
    return false;
}

bool ShouldDeleteTransferTask(const std::string &bytes, const TaskId &taskId, bool hasCommittedTopology,
                              int64_t committedTopologyVersion)
{
    if (!hasCommittedTopology) {
        return true;
    }
    TopologyRepositoryCodec codec;
    TransferTaskRecord task;
    auto rc = codec.DecodeMigrateTask(bytes, taskId, task);
    if (rc.IsError()) {
        LOG(WARNING) << "Delete malformed migrate task during topology cleanup, taskId: " << taskId
                     << ", status: " << rc.ToString();
        return true;
    }
    return task.targetTopologyVersion < committedTopologyVersion
           || (task.targetTopologyVersion == committedTopologyVersion && HasUnfinishedRange(task.ranges));
}

bool ShouldDeleteRecoveryTask(const std::string &bytes, const TaskId &taskId, bool hasCommittedTopology,
                              int64_t committedTopologyVersion)
{
    if (!hasCommittedTopology) {
        return true;
    }
    TopologyRepositoryCodec codec;
    RecoveryTaskRecord task;
    auto rc = codec.DecodeDeleteNodeTask(bytes, taskId, task);
    if (rc.IsError()) {
        LOG(WARNING) << "Delete malformed delete-node task during topology cleanup, taskId: " << taskId
                     << ", status: " << rc.ToString();
        return true;
    }
    return task.targetTopologyVersion < committedTopologyVersion
           || (task.targetTopologyVersion == committedTopologyVersion && HasUnfinishedRange(task.ranges));
}

bool TopologyNodeLess(const TopologyNode &left, const TopologyNode &right)
{
    return left.nodeId < right.nodeId;
}

std::vector<TopologyNode> NormalizeTopologyNodes(const TopologyDescriptor &topology)
{
    auto members = topology.members;
    std::sort(members.begin(), members.end(), TopologyNodeLess);
    return members;
}

bool SameTopologyNode(const TopologyNode &left, const TopologyNode &right)
{
    return left.nodeId == right.nodeId && left.state == right.state && left.tokens == right.tokens;
}

bool SameTopology(const TopologyDescriptor &left, const TopologyDescriptor &right)
{
    if (left.version != right.version || left.clusterHasInit != right.clusterHasInit
        || left.members.size() != right.members.size()) {
        return false;
    }
    auto leftNodes = NormalizeTopologyNodes(left);
    auto rightNodes = NormalizeTopologyNodes(right);
    for (size_t i = 0; i < leftNodes.size(); ++i) {
        if (!SameTopologyNode(leftNodes[i], rightNodes[i])) {
            return false;
        }
    }
    return true;
}

Status NormalizeProgressBatch(const TaskId &taskId, const std::vector<TaskProgressUpdate> &updates,
                              std::vector<TaskProgressUpdate> &normalized)
{
    CHECK_FAIL_RETURN_STATUS(!updates.empty(), K_INVALID, "empty progress batch");
    normalized.clear();
    normalized.reserve(updates.size());
    for (const auto &update : updates) {
        auto next = update;
        if (next.taskId.empty()) {
            next.taskId = taskId;
        }
        CHECK_FAIL_RETURN_STATUS(next.taskId == taskId, K_INVALID, "task id mismatch");
        normalized.push_back(std::move(next));
    }
    return Status::OK();
}

bool MatchTransferTask(const TransferTaskRecord &task, const TaskFilter &filter)
{
    if (filter.executorNodeId.has_value() && task.executorNodeId != *filter.executorNodeId) {
        return false;
    }
    if (filter.nodeId.has_value() && task.executorNodeId != *filter.nodeId
        && task.sourceNodeId != *filter.nodeId && task.targetNodeId != *filter.nodeId
        && !HasRangeNode(task.ranges, *filter.nodeId)) {
        return false;
    }
    return !filter.unfinishedOnly || HasUnfinishedRange(task.ranges);
}

bool MatchRecoveryTask(const RecoveryTaskRecord &task, const TaskFilter &filter)
{
    if (filter.executorNodeId.has_value() && task.executorNodeId != *filter.executorNodeId) {
        return false;
    }
    if (filter.nodeId.has_value() && task.executorNodeId != *filter.nodeId
        && task.failedNodeId != *filter.nodeId && task.recoveryNodeId != *filter.nodeId
        && !HasRangeNode(task.ranges, *filter.nodeId)) {
        return false;
    }
    return !filter.unfinishedOnly || HasUnfinishedRange(task.ranges);
}

Status ReadRing(ICoordinationBackend &store, CoordinationStoreResult &res)
{
    return store.Get(COORDINATION_HASHRING_TABLE, "", res);
}

Status ReadRingChild(ICoordinationBackend &store, const std::string &key, CoordinationStoreResult &res)
{
    return store.Get(COORDINATION_HASHRING_TABLE, key, res);
}

}  // namespace

TopologyRepository::TopologyRepository(ICoordinationBackend &store) : store_(store)
{
}

Status TopologyRepository::GetCommittedTopology(TopologyDescriptor &topology, Revision &revision)
{
    topology = {};
    revision = 0;
    CoordinationStoreResult res;
    RETURN_IF_NOT_OK(ReadRing(store_, res));
    RETURN_IF_NOT_OK(codec_.DecodeTopology(res.value, topology));
    revision = res.modRevision;
    return Status::OK();
}

Status TopologyRepository::TryCreateCommittedTopology(const TopologyDescriptor &topology, Revision &revision)
{
    revision = 0;
    CoordinationStoreResult existing;
    auto getRc = ReadRing(store_, existing);
    if (getRc.IsOk()) {
        RETURN_STATUS(K_TRY_AGAIN, "committed topology already exists");
    }
    CHECK_FAIL_RETURN_STATUS(getRc.GetCode() == K_NOT_FOUND, getRc.GetCode(), getRc.GetMsg());

    std::string bytes;
    RETURN_IF_NOT_OK(codec_.EncodeTopology(topology, bytes));
    CoordinationStoreResult casRes;
    ICoordinationBackend::ProcessFunction createIfAbsent =
        [&bytes, &casRes](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            retry = false;
            (void)oldValue;
            CHECK_FAIL_RETURN_STATUS(casRes.version == 0, K_TRY_AGAIN, "committed topology already exists");
            newValue = std::make_unique<std::string>(bytes);
            return Status::OK();
        };
    RETURN_IF_NOT_OK(store_.CAS(COORDINATION_HASHRING_TABLE, "", createIfAbsent, casRes));

    CoordinationStoreResult res;
    RETURN_IF_NOT_OK(ReadRing(store_, res));
    revision = res.modRevision;
    return Status::OK();
}

Status TopologyRepository::TryUpdateCommittedTopology(const TopologyDescriptor &expectedTopology,
                                                      Revision expectedRevision, const TopologyDescriptor &nextTopology,
                                                      Revision &revision)
{
    revision = 0;
    CHECK_FAIL_RETURN_STATUS(expectedRevision > 0, K_INVALID, "expected committed topology revision is invalid");
    CHECK_FAIL_RETURN_STATUS(nextTopology.version > expectedTopology.version, K_INVALID,
                             "committed topology version must advance");

    std::string nextBytes;
    RETURN_IF_NOT_OK(codec_.EncodeTopology(nextTopology, nextBytes));

    CoordinationStoreResult casRes;
    ICoordinationBackend::ProcessFunction updateIfExpected = [expectedTopology, nextBytes](
                                                                 const std::string &oldValue,
                                                                 std::unique_ptr<std::string> &newValue, bool &retry) {
        retry = false;
        CHECK_FAIL_RETURN_STATUS(!oldValue.empty(), K_NOT_FOUND, "committed topology not found");

        TopologyRepositoryCodec codec;
        TopologyDescriptor currentTopology;
        RETURN_IF_NOT_OK(codec.DecodeTopology(oldValue, currentTopology));
        CHECK_FAIL_RETURN_STATUS(SameTopology(currentTopology, expectedTopology), K_TRY_AGAIN,
                                 "committed topology value changed");
        newValue = std::make_unique<std::string>(nextBytes);
        return Status::OK();
    };
    RETURN_IF_NOT_OK(store_.CAS(COORDINATION_HASHRING_TABLE, "", updateIfExpected, casRes));

    CoordinationStoreResult res;
    RETURN_IF_NOT_OK(ReadRing(store_, res));
    revision = res.modRevision;
    return Status::OK();
}

Status TopologyRepository::TryCreateTransferTaskRecord(const TransferTaskRecord &task, Revision &revision)
{
    revision = 0;
    std::string bytes;
    RETURN_IF_NOT_OK(codec_.EncodeMigrateTask(task, bytes));
    std::string taskKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildMigrateTaskKey(task.taskId, taskKey));

    CoordinationStoreResult casRes;
    ICoordinationBackend::ProcessFunction createIfAbsent =
        [&bytes](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            retry = false;
            CHECK_FAIL_RETURN_STATUS(oldValue.empty(), K_TRY_AGAIN, "migrate task already exists");
            newValue = std::make_unique<std::string>(bytes);
            return Status::OK();
        };
    RETURN_IF_NOT_OK(store_.CAS(COORDINATION_HASHRING_TABLE, taskKey, createIfAbsent, casRes));

    CoordinationStoreResult res;
    RETURN_IF_NOT_OK(ReadRingChild(store_, taskKey, res));
    revision = res.modRevision;
    return Status::OK();
}

Status TopologyRepository::DeleteTransferTaskRecord(const TaskId &taskId)
{
    std::string taskKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildMigrateTaskKey(taskId, taskKey));
    auto rc = store_.Delete(COORDINATION_HASHRING_TABLE, taskKey);
    CHECK_FAIL_RETURN_STATUS(rc.IsOk() || rc.GetCode() == K_NOT_FOUND, rc.GetCode(), rc.GetMsg());
    return Status::OK();
}

Status TopologyRepository::TryCreateRecoveryTaskRecord(const RecoveryTaskRecord &task, Revision &revision)
{
    revision = 0;
    std::string bytes;
    RETURN_IF_NOT_OK(codec_.EncodeDeleteNodeTask(task, bytes));
    std::string taskKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(task.taskId, taskKey));

    CoordinationStoreResult casRes;
    ICoordinationBackend::ProcessFunction createIfAbsent =
        [&bytes](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            retry = false;
            CHECK_FAIL_RETURN_STATUS(oldValue.empty(), K_TRY_AGAIN, "delete-node task already exists");
            newValue = std::make_unique<std::string>(bytes);
            return Status::OK();
        };
    RETURN_IF_NOT_OK(store_.CAS(COORDINATION_HASHRING_TABLE, taskKey, createIfAbsent, casRes));

    CoordinationStoreResult res;
    RETURN_IF_NOT_OK(ReadRingChild(store_, taskKey, res));
    revision = res.modRevision;
    return Status::OK();
}

Status TopologyRepository::DeleteRecoveryTaskRecord(const TaskId &taskId)
{
    std::string taskKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(taskId, taskKey));
    auto rc = store_.Delete(COORDINATION_HASHRING_TABLE, taskKey);
    CHECK_FAIL_RETURN_STATUS(rc.IsOk() || rc.GetCode() == K_NOT_FOUND, rc.GetCode(), rc.GetMsg());
    return Status::OK();
}

Status TopologyRepository::UpsertTaskNotify(const TaskNotify &notify, Revision &revision)
{
    revision = 0;
    std::string bytes;
    RETURN_IF_NOT_OK(codec_.EncodeNotify(notify, bytes));
    std::string notifyKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildNotifyKey(notify.nodeAddress, notifyKey));

    ICoordinationBackend::ProcessFunction upsert = [&bytes](const std::string &oldValue,
                                                            std::unique_ptr<std::string> &newValue, bool &retry) {
        retry = true;
        if (oldValue != bytes) {
            newValue = std::make_unique<std::string>(bytes);
        }
        return Status::OK();
    };
    CoordinationStoreResult casRes;
    RETURN_IF_NOT_OK(store_.CAS(COORDINATION_HASHRING_TABLE, notifyKey, upsert, casRes));

    CoordinationStoreResult res;
    auto rc = ReadRingChild(store_, notifyKey, res);
    if (rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    revision = res.modRevision;
    return Status::OK();
}

Status TopologyRepository::ClearEphemeralRecords()
{
    TopologyDescriptor committedTopology;
    Revision committedRevision = 0;
    bool hasCommittedTopology = false;
    auto topologyRc = GetCommittedTopology(committedTopology, committedRevision);
    if (topologyRc.IsOk()) {
        hasCommittedTopology = committedTopology.clusterHasInit;
    } else {
        CHECK_FAIL_RETURN_STATUS(topologyRc.GetCode() == K_NOT_FOUND, topologyRc.GetCode(), topologyRc.GetMsg());
    }

    std::vector<std::pair<std::string, std::string>> keyValues;
    RETURN_IF_NOT_OK(store_.GetAll(COORDINATION_HASHRING_TABLE, keyValues));
    for (const auto &keyValue : keyValues) {
        TopologyKeyParts parts;
        RETURN_IF_NOT_OK(TopologyKeyHelper::Parse(keyValue.first, parts));
        std::string key;
        switch (parts.type) {
            case TopologyKeyType::MIGRATE_TASK:
                if (!ShouldDeleteTransferTask(keyValue.second, parts.taskId, hasCommittedTopology,
                                              committedTopology.version)) {
                    continue;
                }
                RETURN_IF_NOT_OK(TopologyKeyHelper::BuildMigrateTaskKey(parts.taskId, key));
                break;
            case TopologyKeyType::DELETE_NODE_TASK:
                if (!ShouldDeleteRecoveryTask(keyValue.second, parts.taskId, hasCommittedTopology,
                                              committedTopology.version)) {
                    continue;
                }
                RETURN_IF_NOT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(parts.taskId, key));
                break;
            case TopologyKeyType::NOTIFY:
                RETURN_IF_NOT_OK(TopologyKeyHelper::BuildNotifyKey(parts.nodeAddress, key));
                break;
            default:
                continue;
        }
        auto rc = store_.Delete(COORDINATION_HASHRING_TABLE, key);
        CHECK_FAIL_RETURN_STATUS(rc.IsOk() || rc.GetCode() == K_NOT_FOUND, rc.GetCode(), rc.GetMsg());
    }
    return Status::OK();
}

Status TopologyRepository::ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks)
{
    tasks.clear();
    std::vector<std::pair<std::string, std::string>> keyValues;
    RETURN_IF_NOT_OK(store_.GetAll(COORDINATION_HASHRING_TABLE, keyValues));
    for (const auto &keyValue : keyValues) {
        TopologyKeyParts parts;
        RETURN_IF_NOT_OK(TopologyKeyHelper::Parse(keyValue.first, parts));
        if (parts.type != TopologyKeyType::MIGRATE_TASK) {
            continue;
        }
        TransferTaskRecord task;
        RETURN_IF_NOT_OK(codec_.DecodeMigrateTask(keyValue.second, parts.taskId, task));
        CHECK_FAIL_RETURN_STATUS(task.taskId == parts.taskId, K_INVALID, "migrate task key mismatch");
        if (MatchTransferTask(task, filter)) {
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status TopologyRepository::ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks)
{
    tasks.clear();
    std::vector<std::pair<std::string, std::string>> keyValues;
    RETURN_IF_NOT_OK(store_.GetAll(COORDINATION_HASHRING_TABLE, keyValues));
    for (const auto &keyValue : keyValues) {
        TopologyKeyParts parts;
        RETURN_IF_NOT_OK(TopologyKeyHelper::Parse(keyValue.first, parts));
        if (parts.type != TopologyKeyType::DELETE_NODE_TASK) {
            continue;
        }
        RecoveryTaskRecord task;
        RETURN_IF_NOT_OK(codec_.DecodeDeleteNodeTask(keyValue.second, parts.taskId, task));
        CHECK_FAIL_RETURN_STATUS(task.taskId == parts.taskId, K_INVALID, "delete-node task key mismatch");
        if (MatchRecoveryTask(task, filter)) {
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status TopologyRepository::GetTaskSummary(TopologyTaskSummary &summary)
{
    summary = {};
    TaskFilter filter;
    filter.unfinishedOnly = true;
    std::vector<TransferTaskRecord> transferTasks;
    RETURN_IF_NOT_OK(ListTransferTaskRecords(filter, transferTasks));
    std::vector<RecoveryTaskRecord> recoveryTasks;
    RETURN_IF_NOT_OK(ListRecoveryTaskRecords(filter, recoveryTasks));
    summary.unfinishedTransferTasks = transferTasks.size();
    summary.unfinishedRecoveryTasks = recoveryTasks.size();
    return Status::OK();
}

Status TopologyRepository::ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update)
{
    return ReportTransferProgressBatch(taskId, { update });
}

Status TopologyRepository::ReportTransferProgressBatch(const TaskId &taskId,
                                                       const std::vector<TaskProgressUpdate> &updates)
{
    std::vector<TaskProgressUpdate> updateCopies;
    RETURN_IF_NOT_OK(NormalizeProgressBatch(taskId, updates, updateCopies));
    std::string taskKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildMigrateTaskKey(taskId, taskKey));

    CoordinationStoreResult casRes;
    ICoordinationBackend::ProcessFunction updateProgress =
        [updateCopies](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            retry = false;
            CHECK_FAIL_RETURN_STATUS(!oldValue.empty(), K_NOT_FOUND, "migrate task not found");
            retry = true;
            TopologyRepositoryCodec codec;
            auto nextValue = std::make_unique<std::string>();
            RETURN_IF_NOT_OK(codec.ApplyTransferProgressBatchToTask(oldValue, updateCopies, *nextValue));
            if (*nextValue != oldValue) {
                newValue = std::move(nextValue);
            }
            return Status::OK();
        };
    return store_.CAS(COORDINATION_HASHRING_TABLE, taskKey, updateProgress, casRes);
}

Status TopologyRepository::ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update)
{
    return ReportRecoveryProgressBatch(taskId, { update });
}

Status TopologyRepository::ReportRecoveryProgressBatch(const TaskId &taskId,
                                                       const std::vector<TaskProgressUpdate> &updates)
{
    std::vector<TaskProgressUpdate> updateCopies;
    RETURN_IF_NOT_OK(NormalizeProgressBatch(taskId, updates, updateCopies));
    std::string taskKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(taskId, taskKey));

    CoordinationStoreResult casRes;
    ICoordinationBackend::ProcessFunction updateProgress =
        [updateCopies](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            retry = false;
            CHECK_FAIL_RETURN_STATUS(!oldValue.empty(), K_NOT_FOUND, "delete-node task not found");
            retry = true;
            TopologyRepositoryCodec codec;
            auto nextValue = std::make_unique<std::string>();
            RETURN_IF_NOT_OK(codec.ApplyRecoveryProgressBatchToTask(oldValue, updateCopies, *nextValue));
            if (*nextValue != oldValue) {
                newValue = std::move(nextValue);
            }
            return Status::OK();
        };
    return store_.CAS(COORDINATION_HASHRING_TABLE, taskKey, updateProgress, casRes);
}

Status TopologyRepository::HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed)
{
    typed = {};
    TopologyKeyParts parts;
    RETURN_IF_NOT_OK(TopologyKeyHelper::Parse(event.key, parts));
    CHECK_FAIL_RETURN_STATUS(parts.type == TopologyKeyType::COMMITTED_TOPOLOGY, K_NOT_FOUND,
                             "unrelated topology event");
    typed.revision = event.revision;
    if (event.type == CoordinationEventType::DELETE) {
        typed.type = TopologyWatchEventType::DELETED;
        return Status::OK();
    }
    typed.type = TopologyWatchEventType::UPDATED;
    RETURN_IF_NOT_OK(codec_.DecodeTopology(event.value, typed.topology));
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
