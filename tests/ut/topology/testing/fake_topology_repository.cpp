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
#include "tests/ut/topology/testing/fake_topology_repository.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "datasystem/topology/repository/topology_key_helper.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

bool HasUnfinishedRange(const std::vector<TokenRange> &ranges)
{
    return std::any_of(ranges.begin(), ranges.end(), [](const TokenRange &range) { return !range.finished; });
}

bool HasRangeNode(const std::vector<TokenRange> &ranges, const TopologyNodeId &nodeId)
{
    return std::any_of(ranges.begin(), ranges.end(), [&nodeId](const TokenRange &range) {
        return range.nodeId == nodeId;
    });
}

bool SameMembers(const std::vector<TopologyNode> &left, const std::vector<TopologyNode> &right)
{
    return left.size() == right.size() && std::equal(left.begin(), left.end(), right.begin(), [](const auto &a,
                                                                                                  const auto &b) {
        return a.nodeId == b.nodeId && a.state == b.state && a.tokens == b.tokens;
    });
}

bool SameTopology(const TopologyDescriptor &left, const TopologyDescriptor &right)
{
    return left.version == right.version && left.clusterHasInit == right.clusterHasInit
           && SameMembers(left.members, right.members);
}

bool MatchTransferTask(const TransferTaskRecord &task, const TaskFilter &filter)
{
    if (filter.executorNodeId.has_value() && task.executorNodeId != *filter.executorNodeId) {
        return false;
    }
    if (filter.nodeId.has_value() && task.sourceNodeId != *filter.nodeId
        && task.targetNodeId != *filter.nodeId) {
        return false;
    }
    return !filter.unfinishedOnly || HasUnfinishedRange(task.ranges);
}

bool MatchRecoveryTask(const RecoveryTaskRecord &task, const TaskFilter &filter)
{
    if (filter.executorNodeId.has_value() && task.executorNodeId != *filter.executorNodeId) {
        return false;
    }
    if (filter.nodeId.has_value() && task.failedNodeId != *filter.nodeId
        && task.recoveryNodeId != *filter.nodeId) {
        return false;
    }
    return !filter.unfinishedOnly || HasUnfinishedRange(task.ranges);
}

Status ValidateRange(const TokenRange &range)
{
    CHECK_FAIL_RETURN_STATUS(range.begin <= range.end, K_INVALID, "invalid fake token range");
    CHECK_FAIL_RETURN_STATUS(!range.nodeId.empty(), K_INVALID, "fake range node is empty");
    return Status::OK();
}

Status ValidateTransferTask(const TransferTaskRecord &task)
{
    CHECK_FAIL_RETURN_STATUS(!task.taskId.empty(), K_INVALID, "fake transfer task id is empty");
    CHECK_FAIL_RETURN_STATUS(!task.executorNodeId.empty(), K_INVALID, "fake transfer executor is empty");
    CHECK_FAIL_RETURN_STATUS(!task.sourceNodeId.empty(), K_INVALID, "fake transfer source is empty");
    CHECK_FAIL_RETURN_STATUS(!task.targetNodeId.empty(), K_INVALID, "fake transfer target is empty");
    CHECK_FAIL_RETURN_STATUS(task.createdTopologyVersion >= 0, K_INVALID, "fake transfer created version is invalid");
    CHECK_FAIL_RETURN_STATUS(task.targetTopologyVersion >= task.createdTopologyVersion, K_INVALID,
                             "fake transfer target version is invalid");
    CHECK_FAIL_RETURN_STATUS(!task.ranges.empty(), K_INVALID, "fake transfer ranges are empty");
    for (const auto &range : task.ranges) {
        RETURN_IF_NOT_OK(ValidateRange(range));
    }
    CHECK_FAIL_RETURN_STATUS(HasRangeNode(task.ranges, task.sourceNodeId), K_INVALID,
                             "fake transfer source is not in ranges");
    CHECK_FAIL_RETURN_STATUS(HasRangeNode(task.ranges, task.executorNodeId), K_INVALID,
                             "fake transfer executor is not in ranges");
    return Status::OK();
}

Status ValidateRecoveryTask(const RecoveryTaskRecord &task)
{
    CHECK_FAIL_RETURN_STATUS(!task.taskId.empty(), K_INVALID, "fake recovery task id is empty");
    CHECK_FAIL_RETURN_STATUS(!task.executorNodeId.empty(), K_INVALID, "fake recovery executor is empty");
    CHECK_FAIL_RETURN_STATUS(!task.failedNodeId.empty(), K_INVALID, "fake recovery failed node is empty");
    CHECK_FAIL_RETURN_STATUS(!task.recoveryNodeId.empty(), K_INVALID, "fake recovery node is empty");
    CHECK_FAIL_RETURN_STATUS(task.createdTopologyVersion >= 0, K_INVALID, "fake recovery created version is invalid");
    CHECK_FAIL_RETURN_STATUS(task.targetTopologyVersion >= task.createdTopologyVersion, K_INVALID,
                             "fake recovery target version is invalid");
    CHECK_FAIL_RETURN_STATUS(!task.ranges.empty(), K_INVALID, "fake recovery ranges are empty");
    for (const auto &range : task.ranges) {
        RETURN_IF_NOT_OK(ValidateRange(range));
    }
    CHECK_FAIL_RETURN_STATUS(HasRangeNode(task.ranges, task.recoveryNodeId), K_INVALID,
                             "fake recovery node is not in ranges");
    return Status::OK();
}

template <typename TaskT>
Status MarkProgress(std::vector<TaskT> &tasks, const TaskId &taskId, const TaskProgressUpdate &update,
                    const TopologyNodeId &expectedNodeId)
{
    CHECK_FAIL_RETURN_STATUS(update.taskId.empty() || update.taskId == taskId, K_INVALID, "fake task id mismatch");
    CHECK_FAIL_RETURN_STATUS(update.nodeId == expectedNodeId, K_INVALID, "fake node mismatch");
    CHECK_FAIL_RETURN_STATUS(update.range.finished, K_INVALID, "fake progress must be finished");
    auto taskIter =
        std::find_if(tasks.begin(), tasks.end(), [&taskId](const TaskT &task) { return task.taskId == taskId; });
    CHECK_FAIL_RETURN_STATUS(taskIter != tasks.end(), K_NOT_FOUND, "fake task not found");
    for (auto &range : taskIter->ranges) {
        if (range.nodeId == update.range.nodeId && range.begin == update.range.begin
            && range.end == update.range.end) {
            range.finished = true;
            return Status::OK();
        }
    }
    RETURN_STATUS(K_INVALID, "fake progress range not found");
}

}  // namespace

Status FakeTopologyRepository::SeedCommittedTopology(const TopologyDescriptor &topology)
{
    CHECK_FAIL_RETURN_STATUS(topology.version >= 0, K_INVALID, "fake topology version is invalid");
    CHECK_FAIL_RETURN_STATUS(!topology.members.empty(), K_INVALID, "fake topology members are empty");
    std::lock_guard<std::mutex> lock(mutex_);
    topology_ = topology;
    committedRevision_ = topology.version > 0 ? topology.version : writeRevision_ + 1;
    writeRevision_ = std::max(writeRevision_, committedRevision_);
    hasTopology_ = true;
    return Status::OK();
}

Status FakeTopologyRepository::SeedTransferTask(const TransferTaskRecord &task)
{
    RETURN_IF_NOT_OK(ValidateTransferTask(task));
    std::lock_guard<std::mutex> lock(mutex_);
    auto next = task;
    next.ringRevision = committedRevision_;
    next.taskRevision = ++writeRevision_;
    transferTasks_.push_back(std::move(next));
    return Status::OK();
}

Status FakeTopologyRepository::SeedRecoveryTask(const RecoveryTaskRecord &task)
{
    RETURN_IF_NOT_OK(ValidateRecoveryTask(task));
    std::lock_guard<std::mutex> lock(mutex_);
    auto next = task;
    next.ringRevision = committedRevision_;
    next.taskRevision = ++writeRevision_;
    recoveryTasks_.push_back(std::move(next));
    return Status::OK();
}

void FakeTopologyRepository::InjectTransferProgressConflict()
{
    std::lock_guard<std::mutex> lock(mutex_);
    transferConflict_ = true;
}

void FakeTopologyRepository::InjectNotifyFailure(Status status)
{
    std::lock_guard<std::mutex> lock(mutex_);
    notifyFailure_ = std::move(status);
}

Status FakeTopologyRepository::GetCommittedTopology(TopologyDescriptor &topology, Revision &revision)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(hasTopology_, K_NOT_FOUND, "fake topology is absent");
    topology = topology_;
    revision = committedRevision_;
    return Status::OK();
}

Status FakeTopologyRepository::TryCreateCommittedTopology(const TopologyDescriptor &topology, Revision &revision)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!hasTopology_, K_TRY_AGAIN, "fake topology already exists");
    topology_ = topology;
    committedRevision_ = ++writeRevision_;
    revision = committedRevision_;
    hasTopology_ = true;
    return Status::OK();
}

Status FakeTopologyRepository::TryUpdateCommittedTopology(const TopologyDescriptor &expectedTopology,
                                                          Revision expectedRevision,
                                                          const TopologyDescriptor &nextTopology, Revision &revision)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(expectedRevision > 0, K_INVALID, "fake expected topology revision is invalid");
    CHECK_FAIL_RETURN_STATUS(hasTopology_, K_NOT_FOUND, "fake topology is absent");
    (void)expectedRevision;
    CHECK_FAIL_RETURN_STATUS(SameTopology(topology_, expectedTopology), K_TRY_AGAIN, "fake topology value changed");
    topology_ = nextTopology;
    committedRevision_ = ++writeRevision_;
    revision = committedRevision_;
    return Status::OK();
}

Status FakeTopologyRepository::ClearEphemeralRecords()
{
    std::lock_guard<std::mutex> lock(mutex_);
    transferTasks_.clear();
    recoveryTasks_.clear();
    return Status::OK();
}

Status FakeTopologyRepository::ListTransferTaskRecords(const TaskFilter &filter, std::vector<TransferTaskRecord> &tasks)
{
    tasks.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto task : transferTasks_) {
        task.ringRevision = committedRevision_;
        if (MatchTransferTask(task, filter)) {
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status FakeTopologyRepository::ListRecoveryTaskRecords(const TaskFilter &filter, std::vector<RecoveryTaskRecord> &tasks)
{
    tasks.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto task : recoveryTasks_) {
        task.ringRevision = committedRevision_;
        if (MatchRecoveryTask(task, filter)) {
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status FakeTopologyRepository::GetTaskSummary(TopologyTaskSummary &summary)
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

Status FakeTopologyRepository::TryCreateTransferTaskRecord(const TransferTaskRecord &task, Revision &revision)
{
    RETURN_IF_NOT_OK(ValidateTransferTask(task));
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(std::none_of(transferTasks_.begin(), transferTasks_.end(),
                             [&task](const TransferTaskRecord &existing) {
                                 return existing.taskId == task.taskId;
                             }),
                             K_TRY_AGAIN, "fake transfer task already exists");
    auto next = task;
    next.taskRevision = ++writeRevision_;
    next.ringRevision = committedRevision_;
    revision = next.taskRevision;
    transferTasks_.push_back(std::move(next));
    return Status::OK();
}

Status FakeTopologyRepository::DeleteTransferTaskRecord(const TaskId &taskId)
{
    std::lock_guard<std::mutex> lock(mutex_);
    transferTasks_.erase(std::remove_if(transferTasks_.begin(), transferTasks_.end(),
                                        [&taskId](const TransferTaskRecord &task) {
                                            return task.taskId == taskId;
                                        }),
                         transferTasks_.end());
    return Status::OK();
}

Status FakeTopologyRepository::TryCreateRecoveryTaskRecord(const RecoveryTaskRecord &task, Revision &revision)
{
    RETURN_IF_NOT_OK(ValidateRecoveryTask(task));
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(std::none_of(recoveryTasks_.begin(), recoveryTasks_.end(),
                             [&task](const RecoveryTaskRecord &existing) {
                                 return existing.taskId == task.taskId;
                             }),
                             K_TRY_AGAIN, "fake recovery task already exists");
    auto next = task;
    next.taskRevision = ++writeRevision_;
    next.ringRevision = committedRevision_;
    revision = next.taskRevision;
    recoveryTasks_.push_back(std::move(next));
    return Status::OK();
}

Status FakeTopologyRepository::DeleteRecoveryTaskRecord(const TaskId &taskId)
{
    std::lock_guard<std::mutex> lock(mutex_);
    recoveryTasks_.erase(std::remove_if(recoveryTasks_.begin(), recoveryTasks_.end(),
                                        [&taskId](const RecoveryTaskRecord &task) {
                                            return task.taskId == taskId;
                                        }),
                         recoveryTasks_.end());
    return Status::OK();
}

Status FakeTopologyRepository::UpsertTaskNotify(const TaskNotify &notify, Revision &revision)
{
    CHECK_FAIL_RETURN_STATUS(!notify.nodeAddress.empty(), K_INVALID, "fake notify node is empty");
    std::lock_guard<std::mutex> lock(mutex_);
    if (notifyFailure_.IsError()) {
        auto status = notifyFailure_;
        notifyFailure_ = Status::OK();
        return status;
    }
    revision = ++writeRevision_;
    return Status::OK();
}

Status FakeTopologyRepository::ReportTransferProgress(const TaskId &taskId, const TaskProgressUpdate &update)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (transferConflict_) {
        transferConflict_ = false;
        RETURN_STATUS(K_TRY_AGAIN, "fake transfer CAS conflict");
    }
    return MarkProgress(transferTasks_, taskId, update, update.range.nodeId);
}

Status FakeTopologyRepository::ReportTransferProgressBatch(const TaskId &taskId,
                                                           const std::vector<TaskProgressUpdate> &updates)
{
    for (const auto &update : updates) {
        RETURN_IF_NOT_OK(ReportTransferProgress(taskId, update));
    }
    return Status::OK();
}

Status FakeTopologyRepository::ReportRecoveryProgress(const TaskId &taskId, const TaskProgressUpdate &update)
{
    std::lock_guard<std::mutex> lock(mutex_);
    return MarkProgress(recoveryTasks_, taskId, update, update.range.nodeId);
}

Status FakeTopologyRepository::ReportRecoveryProgressBatch(const TaskId &taskId,
                                                           const std::vector<TaskProgressUpdate> &updates)
{
    for (const auto &update : updates) {
        RETURN_IF_NOT_OK(ReportRecoveryProgress(taskId, update));
    }
    return Status::OK();
}

Status FakeTopologyRepository::HandleCommittedTopologyEvent(const CoordinationEvent &event, TopologyWatchEvent &typed)
{
    typed = {};
    TopologyKeyParts parts;
    RETURN_IF_NOT_OK(TopologyKeyHelper::Parse(event.key, parts));
    CHECK_FAIL_RETURN_STATUS(parts.type == TopologyKeyType::COMMITTED_TOPOLOGY, K_NOT_FOUND, "fake event is unrelated");
    typed.revision = event.revision;
    if (event.type == CoordinationEventType::DELETE) {
        typed.type = TopologyWatchEventType::DELETED;
        return Status::OK();
    }
    TopologyDescriptor topology;
    TopologyRepositoryCodec codec;
    RETURN_IF_NOT_OK(codec.DecodeTopology(event.value, topology));
    topology.version = event.revision;
    typed.type = TopologyWatchEventType::UPDATED;
    typed.topology = std::move(topology);
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
