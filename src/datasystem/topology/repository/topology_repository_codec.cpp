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
#include "datasystem/topology/repository/topology_repository_codec.h"

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring_v2.pb.h"

namespace datasystem {
namespace topology {
namespace topology_v2 = ::datasystem::v2;
namespace {

constexpr char TOPOLOGY_SCHEMA_VERSION_TEXT[] = "1";
static_assert(TOPOLOGY_SCHEMA_VERSION == 1, "update TOPOLOGY_SCHEMA_VERSION_TEXT when schema version changes");
constexpr char MIGRATE_TASK_VERSION_PREFIX[] = "migrate-v";
constexpr char DELETE_NODE_TASK_VERSION_PREFIX[] = "delete-v";
constexpr char TASK_VERSION_SEPARATOR[] = "-to-v";
constexpr char TASK_ID_SEGMENT_SEPARATOR = '-';
constexpr size_t MIGRATE_TASK_VERSION_PREFIX_SIZE = sizeof(MIGRATE_TASK_VERSION_PREFIX) - 1;
constexpr size_t DELETE_NODE_TASK_VERSION_PREFIX_SIZE = sizeof(DELETE_NODE_TASK_VERSION_PREFIX) - 1;
constexpr size_t TASK_VERSION_SEPARATOR_SIZE = sizeof(TASK_VERSION_SEPARATOR) - 1;
constexpr int64_t DECIMAL_BASE = 10;

bool ContainsAny(const std::string &value, const std::string &chars)
{
    return value.find_first_of(chars) != std::string::npos;
}

bool HasControlChar(const std::string &value)
{
    return std::any_of(value.begin(), value.end(), [](unsigned char ch) { return ch <= 0x1F || ch == 0x7F; });
}

Status ValidateAtom(const std::string &value, const std::string &field)
{
    CHECK_FAIL_RETURN_STATUS(!value.empty(), K_INVALID, field + " is empty");
    CHECK_FAIL_RETURN_STATUS(value != "..", K_INVALID, field + " must not be '..'");
    CHECK_FAIL_RETURN_STATUS(!ContainsAny(value, "|,=;/"), K_INVALID, field + " contains reserved char");
    CHECK_FAIL_RETURN_STATUS(!HasControlChar(value), K_INVALID, field + " contains control char");
    return Status::OK();
}

Status ValidateTaskId(const TaskId &taskId)
{
    CHECK_FAIL_RETURN_STATUS(!taskId.empty(), K_INVALID, "task id is empty");
    CHECK_FAIL_RETURN_STATUS(taskId != "..", K_INVALID, "task id must not be '..'");
    CHECK_FAIL_RETURN_STATUS(!ContainsAny(taskId, "|,=;/"), K_INVALID, "task id contains reserved char");
    CHECK_FAIL_RETURN_STATUS(!HasControlChar(taskId), K_INVALID, "task id contains control char");
    return Status::OK();
}

bool StartsWith(const std::string &value, const char *prefix, size_t prefixSize)
{
    return value.compare(0, prefixSize, prefix) == 0;
}

Status ParseNonNegativeVersion(const TaskId &taskId, size_t begin, size_t end, int64_t &version)
{
    CHECK_FAIL_RETURN_STATUS(begin < end && end <= taskId.size(), K_INVALID, "empty topology version in task id");
    int64_t value = 0;
    for (size_t i = begin; i < end; ++i) {
        const char ch = taskId[i];
        CHECK_FAIL_RETURN_STATUS(ch >= '0' && ch <= '9', K_INVALID, "invalid topology version in task id");
        const int64_t digit = ch - '0';
        CHECK_FAIL_RETURN_STATUS(value <= (std::numeric_limits<int64_t>::max() - digit) / DECIMAL_BASE, K_INVALID,
                                 "topology version in task id is too large");
        value = value * DECIMAL_BASE + digit;
    }
    version = value;
    return Status::OK();
}

Status DecodeTaskVersionsFromId(const TaskId &taskId, int64_t &createdTopologyVersion, int64_t &targetTopologyVersion)
{
    size_t versionBegin = std::string::npos;
    if (StartsWith(taskId, MIGRATE_TASK_VERSION_PREFIX, MIGRATE_TASK_VERSION_PREFIX_SIZE)) {
        versionBegin = MIGRATE_TASK_VERSION_PREFIX_SIZE;
    } else if (StartsWith(taskId, DELETE_NODE_TASK_VERSION_PREFIX, DELETE_NODE_TASK_VERSION_PREFIX_SIZE)) {
        versionBegin = DELETE_NODE_TASK_VERSION_PREFIX_SIZE;
    } else {
        return Status::OK();
    }

    const auto separatorPos = taskId.find(TASK_VERSION_SEPARATOR, versionBegin);
    if (separatorPos == std::string::npos) {
        return Status::OK();
    }
    const auto targetBegin = separatorPos + TASK_VERSION_SEPARATOR_SIZE;
    auto targetEnd = taskId.find(TASK_ID_SEGMENT_SEPARATOR, targetBegin);
    if (targetEnd == std::string::npos) {
        targetEnd = taskId.size();
    }

    int64_t created = 0;
    int64_t target = 0;
    RETURN_IF_NOT_OK(ParseNonNegativeVersion(taskId, versionBegin, separatorPos, created));
    RETURN_IF_NOT_OK(ParseNonNegativeVersion(taskId, targetBegin, targetEnd, target));
    CHECK_FAIL_RETURN_STATUS(target >= created, K_INVALID,
                             "task target topology version is older than created version");
    createdTopologyVersion = created;
    targetTopologyVersion = target;
    return Status::OK();
}

Status ValidateRange(const TokenRange &range)
{
    CHECK_FAIL_RETURN_STATUS(range.begin < range.end, K_INVALID, "invalid token range");
    RETURN_IF_NOT_OK(ValidateAtom(range.nodeId, "range worker id"));
    return Status::OK();
}

Status ValidateProgress(const TaskProgressUpdate &update)
{
    RETURN_IF_NOT_OK(ValidateTaskId(update.taskId));
    RETURN_IF_NOT_OK(ValidateAtom(update.nodeId, "worker id"));
    CHECK_FAIL_RETURN_STATUS(update.range.finished, K_INVALID, "progress update must be finished");
    return ValidateRange(update.range);
}

Status ValidateTerminalStatus(TaskTerminalStatus status)
{
    switch (status) {
        case TaskTerminalStatus::RUNNING:
        case TaskTerminalStatus::SUCCEEDED:
        case TaskTerminalStatus::FAILED:
        case TaskTerminalStatus::BLOCKED:
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid task terminal status");
    }
}

Status EncodeState(TopologyNodeState state, topology_v2::WorkerPb::StatePb &statePb)
{
    switch (state) {
        case TopologyNodeState::INITIAL:
            statePb = topology_v2::WorkerPb::INITIAL;
            return Status::OK();
        case TopologyNodeState::JOINING:
            statePb = topology_v2::WorkerPb::JOINING;
            return Status::OK();
        case TopologyNodeState::ACTIVE:
            statePb = topology_v2::WorkerPb::ACTIVE;
            return Status::OK();
        case TopologyNodeState::LEAVING:
            statePb = topology_v2::WorkerPb::LEAVING;
            return Status::OK();
        case TopologyNodeState::PRE_LEAVING:
            statePb = topology_v2::WorkerPb::PRE_LEAVING;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid topology worker state");
    }
}

Status DecodeState(topology_v2::WorkerPb::StatePb statePb, TopologyNodeState &state)
{
    switch (statePb) {
        case topology_v2::WorkerPb::INITIAL:
            state = TopologyNodeState::INITIAL;
            return Status::OK();
        case topology_v2::WorkerPb::JOINING:
            state = TopologyNodeState::JOINING;
            return Status::OK();
        case topology_v2::WorkerPb::ACTIVE:
            state = TopologyNodeState::ACTIVE;
            return Status::OK();
        case topology_v2::WorkerPb::LEAVING:
            state = TopologyNodeState::LEAVING;
            return Status::OK();
        case topology_v2::WorkerPb::PRE_LEAVING:
            state = TopologyNodeState::PRE_LEAVING;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid topology worker state");
    }
}

Status ValidateTopology(const TopologyDescriptor &topology)
{
    CHECK_FAIL_RETURN_STATUS(topology.version >= 0, K_INVALID, "invalid topology version");
    std::unordered_set<TopologyNodeId> nodeIds;
    nodeIds.reserve(topology.members.size());
    for (const auto &worker : topology.members) {
        RETURN_IF_NOT_OK(ValidateAtom(worker.nodeId, "worker id"));
        CHECK_FAIL_RETURN_STATUS(nodeIds.insert(worker.nodeId).second, K_INVALID, "duplicated topology worker id");
    }
    return Status::OK();
}

Status DecodeRing(const std::string &bytes, topology_v2::HashRingPb &ring)
{
    CHECK_FAIL_RETURN_STATUS(ring.ParseFromString(bytes), K_INVALID, "parse HashRingPb failed");
    CHECK_FAIL_RETURN_STATUS(ring.schema_version() == TOPOLOGY_SCHEMA_VERSION_TEXT, K_INVALID,
                             "unsupported HashRingPb schema version");
    CHECK_FAIL_RETURN_STATUS(ring.version() <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()), K_INVALID,
                             "invalid HashRingPb version");
    return Status::OK();
}

Status EncodeTaskRange(const TokenRange &range, topology_v2::TokenRangePb *rangePb)
{
    RETURN_IF_NOT_OK(ValidateRange(range));
    rangePb->set_worker(range.nodeId);
    rangePb->set_from(range.begin);
    rangePb->set_end(range.end);
    rangePb->set_finished(range.finished);
    return Status::OK();
}

Status DecodeTaskRange(const topology_v2::TokenRangePb &rangePb, TokenRange &range)
{
    range = {};
    RETURN_IF_NOT_OK(ValidateAtom(rangePb.worker(), "range worker id"));
    range.nodeId = rangePb.worker();
    range.begin = rangePb.from();
    range.end = rangePb.end();
    range.finished = rangePb.finished();
    return ValidateRange(range);
}

bool SameRangeIdentity(const TokenRange &left, const TokenRange &right)
{
    return left.nodeId == right.nodeId && left.begin == right.begin && left.end == right.end;
}

Status MarkProgressRangeFinished(std::vector<TokenRange> &ranges, const TaskProgressUpdate &update)
{
    bool found = false;
    for (auto &range : ranges) {
        if (SameRangeIdentity(range, update.range)) {
            found = true;
            range.finished = true;
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS(found, K_INVALID, "progress range not found");
    return Status::OK();
}

Status ApplyTransferProgress(TransferTaskRecord &task, const TaskProgressUpdate &update)
{
    RETURN_IF_NOT_OK(ValidateProgress(update));
    CHECK_FAIL_RETURN_STATUS(update.taskId == task.taskId, K_INVALID, "task id mismatch");
    CHECK_FAIL_RETURN_STATUS(update.nodeId == update.range.nodeId, K_INVALID, "worker mismatch");
    return MarkProgressRangeFinished(task.ranges, update);
}

Status ApplyRecoveryProgress(RecoveryTaskRecord &task, const TaskProgressUpdate &update)
{
    RETURN_IF_NOT_OK(ValidateProgress(update));
    CHECK_FAIL_RETURN_STATUS(update.taskId == task.taskId, K_INVALID, "task id mismatch");
    CHECK_FAIL_RETURN_STATUS(update.nodeId == task.recoveryNodeId, K_INVALID, "worker mismatch");
    return MarkProgressRangeFinished(task.ranges, update);
}

bool HasRangeWorker(const std::vector<TokenRange> &ranges, const TopologyNodeId &nodeId)
{
    return std::any_of(ranges.begin(), ranges.end(),
                       [&nodeId](const TokenRange &range) { return range.nodeId == nodeId; });
}

Status ValidateTransferTask(const TransferTaskRecord &task)
{
    RETURN_IF_NOT_OK(ValidateTaskId(task.taskId));
    RETURN_IF_NOT_OK(ValidateAtom(task.executorNodeId, "executor worker id"));
    RETURN_IF_NOT_OK(ValidateAtom(task.sourceNodeId, "source worker id"));
    RETURN_IF_NOT_OK(ValidateAtom(task.targetNodeId, "target worker id"));
    CHECK_FAIL_RETURN_STATUS(task.createdTopologyVersion >= 0, K_INVALID, "invalid created topology version");
    CHECK_FAIL_RETURN_STATUS(task.targetTopologyVersion >= 0, K_INVALID, "invalid target topology version");
    RETURN_IF_NOT_OK(ValidateTerminalStatus(task.status));
    CHECK_FAIL_RETURN_STATUS(!task.ranges.empty(), K_INVALID, "empty migrate task ranges");
    for (const auto &range : task.ranges) {
        RETURN_IF_NOT_OK(ValidateRange(range));
    }
    CHECK_FAIL_RETURN_STATUS(HasRangeWorker(task.ranges, task.sourceNodeId), K_INVALID,
                             "source worker is not present in migrate task ranges");
    CHECK_FAIL_RETURN_STATUS(HasRangeWorker(task.ranges, task.executorNodeId), K_INVALID,
                             "executor worker is not present in migrate task ranges");
    return Status::OK();
}

Status ValidateRecoveryTask(const RecoveryTaskRecord &task)
{
    RETURN_IF_NOT_OK(ValidateTaskId(task.taskId));
    RETURN_IF_NOT_OK(ValidateAtom(task.executorNodeId, "executor worker id"));
    RETURN_IF_NOT_OK(ValidateAtom(task.failedNodeId, "failed worker id"));
    RETURN_IF_NOT_OK(ValidateAtom(task.recoveryNodeId, "recovery worker id"));
    CHECK_FAIL_RETURN_STATUS(task.createdTopologyVersion >= 0, K_INVALID, "invalid created topology version");
    CHECK_FAIL_RETURN_STATUS(task.targetTopologyVersion >= 0, K_INVALID, "invalid target topology version");
    RETURN_IF_NOT_OK(ValidateTerminalStatus(task.status));
    CHECK_FAIL_RETURN_STATUS(!task.ranges.empty(), K_INVALID, "empty delete-node task ranges");
    for (const auto &range : task.ranges) {
        RETURN_IF_NOT_OK(ValidateRange(range));
    }
    CHECK_FAIL_RETURN_STATUS(HasRangeWorker(task.ranges, task.recoveryNodeId), K_INVALID,
                             "recovery worker is not present in delete-node task ranges");
    return Status::OK();
}

Status EncodeNotifyType(TaskNotifyType type, topology_v2::TaskNotifyPb::TypePb &typePb)
{
    switch (type) {
        case TaskNotifyType::SCALE_OUT:
            typePb = topology_v2::TaskNotifyPb::SCALE_OUT;
            return Status::OK();
        case TaskNotifyType::ACTIVE_SCALE_IN:
            typePb = topology_v2::TaskNotifyPb::ACTIVE_SCALE_IN;
            return Status::OK();
        case TaskNotifyType::PASSIVE_SCALE_IN:
            typePb = topology_v2::TaskNotifyPb::PASSIVE_SCALE_IN;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid task notify type");
    }
}

Status DecodeNotifyType(topology_v2::TaskNotifyPb::TypePb typePb, TaskNotifyType &type)
{
    switch (typePb) {
        case topology_v2::TaskNotifyPb::SCALE_OUT:
            type = TaskNotifyType::SCALE_OUT;
            return Status::OK();
        case topology_v2::TaskNotifyPb::ACTIVE_SCALE_IN:
            type = TaskNotifyType::ACTIVE_SCALE_IN;
            return Status::OK();
        case topology_v2::TaskNotifyPb::PASSIVE_SCALE_IN:
            type = TaskNotifyType::PASSIVE_SCALE_IN;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid task notify type");
    }
}

Status ValidateNotify(const TaskNotify &notify)
{
    RETURN_IF_NOT_OK(ValidateAtom(notify.nodeAddress, "notify worker address"));
    topology_v2::TaskNotifyPb::TypePb typePb = topology_v2::TaskNotifyPb::SCALE_OUT;
    RETURN_IF_NOT_OK(EncodeNotifyType(notify.type, typePb));
    for (const auto &taskId : notify.taskIds) {
        RETURN_IF_NOT_OK(ValidateTaskId(taskId));
    }
    return Status::OK();
}

}  // namespace

Status TopologyRepositoryCodec::DecodeTopology(const std::string &bytes, TopologyDescriptor &topology) const
{
    topology = {};
    topology_v2::HashRingPb ringPb;
    RETURN_IF_NOT_OK(DecodeRing(bytes, ringPb));
    topology.version = static_cast<int64_t>(ringPb.version());
    topology.clusterHasInit = ringPb.cluster_has_init();
    topology.members.reserve(ringPb.workers().size());
    for (const auto &entry : ringPb.workers()) {
        TopologyNode worker;
        RETURN_IF_NOT_OK(ValidateAtom(entry.first, "worker id"));
        worker.nodeId = entry.first;
        RETURN_IF_NOT_OK(DecodeState(entry.second.state(), worker.state));
        worker.tokens.assign(entry.second.hash_tokens().begin(), entry.second.hash_tokens().end());
        topology.members.push_back(std::move(worker));
    }
    std::sort(topology.members.begin(), topology.members.end(),
              [](const TopologyNode &left, const TopologyNode &right) { return left.nodeId < right.nodeId; });
    return Status::OK();
}

Status TopologyRepositoryCodec::EncodeTopology(const TopologyDescriptor &topology, std::string &bytes) const
{
    bytes.clear();
    RETURN_IF_NOT_OK(ValidateTopology(topology));

    topology_v2::HashRingPb ringPb;
    ringPb.set_schema_version(TOPOLOGY_SCHEMA_VERSION_TEXT);
    ringPb.set_version(static_cast<uint64_t>(topology.version));
    ringPb.set_cluster_has_init(topology.clusterHasInit);
    for (const auto &worker : topology.members) {
        auto &workerPb = (*ringPb.mutable_workers())[worker.nodeId];
        topology_v2::WorkerPb::StatePb statePb = topology_v2::WorkerPb::INITIAL;
        RETURN_IF_NOT_OK(EncodeState(worker.state, statePb));
        workerPb.set_state(statePb);
        for (auto token : worker.tokens) {
            workerPb.add_hash_tokens(token);
        }
    }
    CHECK_FAIL_RETURN_STATUS(ringPb.SerializeToString(&bytes), K_INVALID, "serialize HashRingPb failed");
    return Status::OK();
}

Status TopologyRepositoryCodec::DecodeMigrateTask(const std::string &bytes, const TaskId &taskId,
                                                  TransferTaskRecord &task) const
{
    task = {};
    RETURN_IF_NOT_OK(ValidateTaskId(taskId));
    topology_v2::MigrateTaskPb taskPb;
    CHECK_FAIL_RETURN_STATUS(taskPb.ParseFromString(bytes), K_INVALID, "parse MigrateTaskPb failed");
    task.taskId = taskId;
    task.targetNodeId = taskPb.target_worker();
    task.ranges.reserve(taskPb.source_ranges_size());
    for (const auto &rangePb : taskPb.source_ranges()) {
        TokenRange range;
        RETURN_IF_NOT_OK(DecodeTaskRange(rangePb, range));
        task.ranges.push_back(std::move(range));
    }
    if (!task.ranges.empty()) {
        task.executorNodeId = task.ranges.front().nodeId;
        task.sourceNodeId = task.ranges.front().nodeId;
    }
    RETURN_IF_NOT_OK(DecodeTaskVersionsFromId(taskId, task.createdTopologyVersion, task.targetTopologyVersion));
    return ValidateTransferTask(task);
}

Status TopologyRepositoryCodec::EncodeMigrateTask(const TransferTaskRecord &task, std::string &bytes) const
{
    bytes.clear();
    RETURN_IF_NOT_OK(ValidateTransferTask(task));
    topology_v2::MigrateTaskPb taskPb;
    taskPb.set_target_worker(task.targetNodeId);
    for (const auto &range : task.ranges) {
        RETURN_IF_NOT_OK(EncodeTaskRange(range, taskPb.add_source_ranges()));
    }
    CHECK_FAIL_RETURN_STATUS(taskPb.SerializeToString(&bytes), K_INVALID, "serialize MigrateTaskPb failed");
    return Status::OK();
}

Status TopologyRepositoryCodec::ApplyTransferProgressToTask(const std::string &currentBytes,
                                                            const TaskProgressUpdate &update,
                                                            std::string &newBytes) const
{
    return ApplyTransferProgressBatchToTask(currentBytes, { update }, newBytes);
}

Status TopologyRepositoryCodec::ApplyTransferProgressBatchToTask(const std::string &currentBytes,
                                                                 const std::vector<TaskProgressUpdate> &updates,
                                                                 std::string &newBytes) const
{
    newBytes.clear();
    CHECK_FAIL_RETURN_STATUS(!updates.empty(), K_INVALID, "empty transfer progress batch");
    TransferTaskRecord task;
    RETURN_IF_NOT_OK(DecodeMigrateTask(currentBytes, updates.front().taskId, task));
    for (const auto &update : updates) {
        RETURN_IF_NOT_OK(ApplyTransferProgress(task, update));
    }
    return EncodeMigrateTask(task, newBytes);
}

Status TopologyRepositoryCodec::DecodeDeleteNodeTask(const std::string &bytes, const TaskId &taskId,
                                                     RecoveryTaskRecord &task) const
{
    task = {};
    RETURN_IF_NOT_OK(ValidateTaskId(taskId));
    topology_v2::DeleteNodeTaskPb taskPb;
    CHECK_FAIL_RETURN_STATUS(taskPb.ParseFromString(bytes), K_INVALID, "parse DeleteNodeTaskPb failed");
    task.taskId = taskId;
    task.failedNodeId = taskPb.failed_worker();
    task.ranges.reserve(taskPb.recovery_ranges_size());
    for (const auto &rangePb : taskPb.recovery_ranges()) {
        TokenRange range;
        RETURN_IF_NOT_OK(DecodeTaskRange(rangePb, range));
        task.ranges.push_back(std::move(range));
    }
    if (!task.ranges.empty()) {
        task.executorNodeId = task.ranges.front().nodeId;
        task.recoveryNodeId = task.ranges.front().nodeId;
    }
    RETURN_IF_NOT_OK(DecodeTaskVersionsFromId(taskId, task.createdTopologyVersion, task.targetTopologyVersion));
    return ValidateRecoveryTask(task);
}

Status TopologyRepositoryCodec::EncodeDeleteNodeTask(const RecoveryTaskRecord &task, std::string &bytes) const
{
    bytes.clear();
    RETURN_IF_NOT_OK(ValidateRecoveryTask(task));
    CHECK_FAIL_RETURN_STATUS(task.executorNodeId == task.recoveryNodeId, K_INVALID,
                             "delete-node task executor must match recovery worker");
    topology_v2::DeleteNodeTaskPb taskPb;
    taskPb.set_failed_worker(task.failedNodeId);
    for (const auto &range : task.ranges) {
        RETURN_IF_NOT_OK(EncodeTaskRange(range, taskPb.add_recovery_ranges()));
    }
    CHECK_FAIL_RETURN_STATUS(taskPb.SerializeToString(&bytes), K_INVALID, "serialize DeleteNodeTaskPb failed");
    return Status::OK();
}

Status TopologyRepositoryCodec::ApplyRecoveryProgressToTask(const std::string &currentBytes,
                                                            const TaskProgressUpdate &update,
                                                            std::string &newBytes) const
{
    return ApplyRecoveryProgressBatchToTask(currentBytes, { update }, newBytes);
}

Status TopologyRepositoryCodec::ApplyRecoveryProgressBatchToTask(const std::string &currentBytes,
                                                                 const std::vector<TaskProgressUpdate> &updates,
                                                                 std::string &newBytes) const
{
    newBytes.clear();
    CHECK_FAIL_RETURN_STATUS(!updates.empty(), K_INVALID, "empty recovery progress batch");
    RecoveryTaskRecord task;
    RETURN_IF_NOT_OK(DecodeDeleteNodeTask(currentBytes, updates.front().taskId, task));
    for (const auto &update : updates) {
        RETURN_IF_NOT_OK(ApplyRecoveryProgress(task, update));
    }
    return EncodeDeleteNodeTask(task, newBytes);
}

Status TopologyRepositoryCodec::DecodeNotify(const std::string &bytes, const TopologyAddress &nodeAddress,
                                             TaskNotify &notify) const
{
    notify = {};
    RETURN_IF_NOT_OK(ValidateAtom(nodeAddress, "notify worker address"));
    topology_v2::TaskNotifyPb notifyPb;
    CHECK_FAIL_RETURN_STATUS(notifyPb.ParseFromString(bytes), K_INVALID, "parse TaskNotifyPb failed");
    notify.nodeAddress = nodeAddress;
    RETURN_IF_NOT_OK(DecodeNotifyType(notifyPb.type(), notify.type));
    notify.taskIds.reserve(notifyPb.task_ids_size());
    for (const auto &taskId : notifyPb.task_ids()) {
        RETURN_IF_NOT_OK(ValidateTaskId(taskId));
        notify.taskIds.push_back(taskId);
    }
    return ValidateNotify(notify);
}

Status TopologyRepositoryCodec::EncodeNotify(const TaskNotify &notify, std::string &bytes) const
{
    bytes.clear();
    RETURN_IF_NOT_OK(ValidateNotify(notify));
    topology_v2::TaskNotifyPb notifyPb;
    topology_v2::TaskNotifyPb::TypePb typePb = topology_v2::TaskNotifyPb::SCALE_OUT;
    RETURN_IF_NOT_OK(EncodeNotifyType(notify.type, typePb));
    notifyPb.set_type(typePb);
    for (const auto &taskId : notify.taskIds) {
        notifyPb.add_task_ids(taskId);
    }
    CHECK_FAIL_RETURN_STATUS(notifyPb.SerializeToString(&bytes), K_INVALID, "serialize TaskNotifyPb failed");
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
