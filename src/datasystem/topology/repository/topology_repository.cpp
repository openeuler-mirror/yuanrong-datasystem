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
#include "datasystem/topology/repository/topology_repository.h"

#include <memory>
#include <utility>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
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

bool MatchTransferTask(const TransferTaskRecord &task, const TaskFilter &filter)
{
    if (filter.workerId.has_value() && task.sourceWorkerId != *filter.workerId
        && task.targetWorkerId != *filter.workerId) {
        return false;
    }
    return !filter.unfinishedOnly || HasUnfinishedRange(task.ranges);
}

bool MatchRecoveryTask(const RecoveryTaskRecord &task, const TaskFilter &filter)
{
    if (filter.workerId.has_value() && task.failedWorkerId != *filter.workerId
        && task.recoveryWorkerId != *filter.workerId) {
        return false;
    }
    return !filter.unfinishedOnly || HasUnfinishedRange(task.ranges);
}

Status ReadRing(ICoordinationBackend &store, RangeSearchResult &res)
{
    return store.Get(ETCD_RING_PREFIX, "", res);
}

}  // namespace

TopologyRepository::TopologyRepository(ICoordinationBackend &store) : store_(store)
{
}

Status TopologyRepository::GetCommittedTopology(TopologyDescriptor &topology, Revision &revision)
{
    topology = {};
    revision = 0;
    RangeSearchResult res;
    RETURN_IF_NOT_OK(ReadRing(store_, res));
    RETURN_IF_NOT_OK(codec_.DecodeTopology(res.value, topology));
    revision = res.modRevision;
    topology.version = revision;
    return Status::OK();
}

Status TopologyRepository::ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks)
{
    tasks.clear();
    RangeSearchResult res;
    RETURN_IF_NOT_OK(ReadRing(store_, res));
    std::vector<TransferTaskRecord> allTasks;
    RETURN_IF_NOT_OK(codec_.DecodeTransferTasksFromRing(res.value, allTasks));
    for (auto &task : allTasks) {
        task.ringRevision = res.modRevision;
        if (MatchTransferTask(task, filter)) {
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status TopologyRepository::ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks)
{
    tasks.clear();
    RangeSearchResult res;
    RETURN_IF_NOT_OK(ReadRing(store_, res));
    std::vector<RecoveryTaskRecord> allTasks;
    RETURN_IF_NOT_OK(codec_.DecodeRecoveryTasksFromRing(res.value, allTasks));
    for (auto &task : allTasks) {
        task.ringRevision = res.modRevision;
        if (MatchRecoveryTask(task, filter)) {
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status TopologyRepository::ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update)
{
    TaskProgressUpdate updateCopy = update;
    if (updateCopy.taskId.empty()) {
        updateCopy.taskId = taskId;
    }
    CHECK_FAIL_RETURN_STATUS(updateCopy.taskId == taskId, K_INVALID, "task id mismatch");

    return store_.CAS(
        ETCD_RING_PREFIX, "",
        [updateCopy](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            TopologyRepositoryCodec codec;
            auto nextValue = std::make_unique<std::string>();
            RETURN_IF_NOT_OK(codec.ApplyTransferProgressToRing(oldValue, updateCopy, *nextValue));
            if (*nextValue != oldValue) {
                newValue = std::move(nextValue);
            }
            return Status::OK();
        });
}

Status TopologyRepository::ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update)
{
    TaskProgressUpdate updateCopy = update;
    if (updateCopy.taskId.empty()) {
        updateCopy.taskId = taskId;
    }
    CHECK_FAIL_RETURN_STATUS(updateCopy.taskId == taskId, K_INVALID, "task id mismatch");

    return store_.CAS(
        ETCD_RING_PREFIX, "",
        [updateCopy](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            TopologyRepositoryCodec codec;
            auto nextValue = std::make_unique<std::string>();
            RETURN_IF_NOT_OK(codec.ApplyRecoveryProgressToRing(oldValue, updateCopy, *nextValue));
            if (*nextValue != oldValue) {
                newValue = std::move(nextValue);
            }
            return Status::OK();
        });
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
    typed.topology.version = event.revision;
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
