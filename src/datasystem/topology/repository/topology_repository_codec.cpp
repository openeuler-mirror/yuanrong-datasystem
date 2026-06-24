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
#include <map>
#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char TASK_ID_SEPARATOR = '|';

bool ContainsAny(const std::string &value, const std::string &chars)
{
    return value.find_first_of(chars) != std::string::npos;
}

bool HasControlChar(const std::string &value)
{
    return std::any_of(value.begin(), value.end(), [](unsigned char ch) { return ch < ' '; });
}

Status ValidateAtom(const std::string &value, const std::string &field)
{
    CHECK_FAIL_RETURN_STATUS(!value.empty(), K_INVALID, field + " is empty");
    CHECK_FAIL_RETURN_STATUS(value != "..", K_INVALID, field + " must not be '..'");
    CHECK_FAIL_RETURN_STATUS(!ContainsAny(value, "|,=;/"), K_INVALID, field + " contains reserved char");
    CHECK_FAIL_RETURN_STATUS(!HasControlChar(value), K_INVALID, field + " contains control char");
    return Status::OK();
}

Status ValidateRange(const TokenRange &range)
{
    CHECK_FAIL_RETURN_STATUS(range.begin < range.end, K_INVALID, "invalid token range");
    RETURN_IF_NOT_OK(ValidateAtom(range.workerId, "range worker id"));
    return Status::OK();
}

Status ValidateProgress(const TaskProgressUpdate &update)
{
    CHECK_FAIL_RETURN_STATUS(!update.taskId.empty(), K_INVALID, "task id is empty");
    RETURN_IF_NOT_OK(ValidateAtom(update.workerId, "worker id"));
    CHECK_FAIL_RETURN_STATUS(update.range.finished, K_INVALID, "progress update must be finished");
    return ValidateRange(update.range);
}

Status EncodeState(WorkerTopologyState state, WorkerPb::StatePb &statePb)
{
    switch (state) {
        case WorkerTopologyState::INITIAL:
            statePb = WorkerPb::INITIAL;
            return Status::OK();
        case WorkerTopologyState::JOINING:
            statePb = WorkerPb::JOINING;
            return Status::OK();
        case WorkerTopologyState::ACTIVE:
            statePb = WorkerPb::ACTIVE;
            return Status::OK();
        case WorkerTopologyState::LEAVING:
            statePb = WorkerPb::LEAVING;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid topology worker state");
    }
}

bool IsKnownState(WorkerTopologyState state)
{
    switch (state) {
        case WorkerTopologyState::INITIAL:
        case WorkerTopologyState::JOINING:
        case WorkerTopologyState::ACTIVE:
        case WorkerTopologyState::LEAVING:
            return true;
        default:
            return false;
    }
}

Status DecodeState(WorkerPb::StatePb statePb, WorkerTopologyState &state)
{
    switch (statePb) {
        case WorkerPb::INITIAL:
            state = WorkerTopologyState::INITIAL;
            return Status::OK();
        case WorkerPb::JOINING:
            state = WorkerTopologyState::JOINING;
            return Status::OK();
        case WorkerPb::ACTIVE:
            state = WorkerTopologyState::ACTIVE;
            return Status::OK();
        case WorkerPb::LEAVING:
            state = WorkerTopologyState::LEAVING;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "invalid topology worker state");
    }
}

Status ValidateTopology(const TopologyDescriptor &topology)
{
    CHECK_FAIL_RETURN_STATUS(topology.version >= 0, K_INVALID, "invalid topology version");
    CHECK_FAIL_RETURN_STATUS(!topology.workers.empty(), K_INVALID, "empty topology workers");
    for (const auto &worker : topology.workers) {
        RETURN_IF_NOT_OK(ValidateAtom(worker.workerId, "worker id"));
        CHECK_FAIL_RETURN_STATUS(IsKnownState(worker.state), K_INVALID, "invalid topology worker state");
    }
    return Status::OK();
}

std::string BuildTaskId(const WorkerId &left, const WorkerId &right)
{
    return left + TASK_ID_SEPARATOR + right;
}

Status SplitTaskId(const TaskId &taskId, WorkerId &left, WorkerId &right)
{
    left.clear();
    right.clear();
    auto pos = taskId.find(TASK_ID_SEPARATOR);
    CHECK_FAIL_RETURN_STATUS(pos != std::string::npos && pos > 0 && pos + 1 < taskId.size(), K_INVALID,
                             "invalid task id");
    left = taskId.substr(0, pos);
    right = taskId.substr(pos + 1);
    RETURN_IF_NOT_OK(ValidateAtom(left, "task id left"));
    return ValidateAtom(right, "task id right");
}

Status DecodeRange(const ChangeNodePb::RangePb &rangePb, TokenRange &range)
{
    range = {};
    RETURN_IF_NOT_OK(ValidateAtom(rangePb.workerid(), "range worker id"));
    range.workerId = rangePb.workerid();
    range.begin = rangePb.from();
    range.end = rangePb.end();
    range.finished = rangePb.finished();
    return ValidateRange(range);
}

Status DecodeRing(const std::string &bytes, HashRingPb &ring)
{
    CHECK_FAIL_RETURN_STATUS(ring.ParseFromString(bytes), K_INVALID, "parse HashRingPb failed");
    CHECK_FAIL_RETURN_STATUS(!ring.workers().empty(), K_INVALID, "empty hash ring workers");
    return Status::OK();
}

}  // namespace

Status TopologyRepositoryCodec::DecodeTopology(const std::string &bytes, TopologyDescriptor &topology) const
{
    topology = {};
    HashRingPb ringPb;
    RETURN_IF_NOT_OK(DecodeRing(bytes, ringPb));
    topology.clusterHasInit = ringPb.cluster_has_init();
    topology.workers.reserve(ringPb.workers().size());
    for (const auto &entry : ringPb.workers()) {
        TopologyWorker worker;
        RETURN_IF_NOT_OK(ValidateAtom(entry.first, "worker id"));
        worker.workerId = entry.first;
        RETURN_IF_NOT_OK(DecodeState(entry.second.state(), worker.state));
        worker.tokens.assign(entry.second.hash_tokens().begin(), entry.second.hash_tokens().end());
        topology.workers.push_back(std::move(worker));
    }
    std::sort(topology.workers.begin(), topology.workers.end(),
              [](const TopologyWorker &left, const TopologyWorker &right) { return left.workerId < right.workerId; });
    return Status::OK();
}

Status TopologyRepositoryCodec::EncodeTopology(const TopologyDescriptor &topology, std::string &bytes) const
{
    bytes.clear();
    RETURN_IF_NOT_OK(ValidateTopology(topology));

    HashRingPb ringPb;
    ringPb.set_cluster_has_init(topology.clusterHasInit);
    for (const auto &worker : topology.workers) {
        auto &workerPb = (*ringPb.mutable_workers())[worker.workerId];
        WorkerPb::StatePb statePb = WorkerPb::INITIAL;
        RETURN_IF_NOT_OK(EncodeState(worker.state, statePb));
        workerPb.set_state(statePb);
        for (auto token : worker.tokens) {
            workerPb.add_hash_tokens(token);
        }
    }
    CHECK_FAIL_RETURN_STATUS(ringPb.SerializeToString(&bytes), K_INVALID, "serialize HashRingPb failed");
    return Status::OK();
}

Status TopologyRepositoryCodec::DecodeTransferTasksFromRing(const std::string &ringBytes,
                                                            std::vector<TransferTaskRecord> &tasks) const
{
    tasks.clear();
    HashRingPb ring;
    RETURN_IF_NOT_OK(DecodeRing(ringBytes, ring));
    for (const auto &entry : ring.add_node_info()) {
        const auto &target = entry.first;
        RETURN_IF_NOT_OK(ValidateAtom(target, "target worker id"));
        std::map<WorkerId, std::vector<TokenRange>> rangesBySource;
        for (const auto &rangePb : entry.second.changed_ranges()) {
            TokenRange range;
            RETURN_IF_NOT_OK(DecodeRange(rangePb, range));
            rangesBySource[range.workerId].push_back(range);
        }
        for (auto &sourceEntry : rangesBySource) {
            TransferTaskRecord task;
            task.taskId = BuildTaskId(target, sourceEntry.first);
            task.targetWorkerId = target;
            task.sourceWorkerId = sourceEntry.first;
            task.ranges = std::move(sourceEntry.second);
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status TopologyRepositoryCodec::DecodeRecoveryTasksFromRing(const std::string &ringBytes,
                                                            std::vector<RecoveryTaskRecord> &tasks) const
{
    tasks.clear();
    HashRingPb ring;
    RETURN_IF_NOT_OK(DecodeRing(ringBytes, ring));
    for (const auto &entry : ring.del_node_info()) {
        const auto &failed = entry.first;
        RETURN_IF_NOT_OK(ValidateAtom(failed, "failed worker id"));
        std::map<WorkerId, std::vector<TokenRange>> rangesByRecovery;
        for (const auto &rangePb : entry.second.changed_ranges()) {
            TokenRange range;
            RETURN_IF_NOT_OK(DecodeRange(rangePb, range));
            rangesByRecovery[range.workerId].push_back(range);
        }
        for (auto &recoveryEntry : rangesByRecovery) {
            RecoveryTaskRecord task;
            task.taskId = BuildTaskId(failed, recoveryEntry.first);
            task.failedWorkerId = failed;
            task.recoveryWorkerId = recoveryEntry.first;
            task.ranges = std::move(recoveryEntry.second);
            tasks.push_back(std::move(task));
        }
    }
    return Status::OK();
}

Status TopologyRepositoryCodec::ApplyTransferProgressToRing(const std::string &currentBytes,
                                                            const TaskProgressUpdate &update,
                                                            std::string &newBytes) const
{
    newBytes.clear();
    RETURN_IF_NOT_OK(ValidateProgress(update));
    WorkerId target;
    WorkerId source;
    RETURN_IF_NOT_OK(SplitTaskId(update.taskId, target, source));
    CHECK_FAIL_RETURN_STATUS(update.workerId == source, K_INVALID, "worker mismatch");

    HashRingPb ring;
    RETURN_IF_NOT_OK(DecodeRing(currentBytes, ring));
    auto targetIter = ring.mutable_add_node_info()->find(target);
    CHECK_FAIL_RETURN_STATUS(targetIter != ring.mutable_add_node_info()->end(), K_NOT_FOUND, "transfer task not found");
    bool found = false;
    for (auto &rangePb : *targetIter->second.mutable_changed_ranges()) {
        if (rangePb.workerid() == source && rangePb.from() == update.range.begin && rangePb.end() == update.range.end) {
            found = true;
            rangePb.set_finished(true);
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS(found, K_INVALID, "progress range not found");
    CHECK_FAIL_RETURN_STATUS(ring.SerializeToString(&newBytes), K_INVALID, "serialize HashRingPb failed");
    return Status::OK();
}

Status TopologyRepositoryCodec::ApplyRecoveryProgressToRing(const std::string &currentBytes,
                                                            const TaskProgressUpdate &update,
                                                            std::string &newBytes) const
{
    newBytes.clear();
    RETURN_IF_NOT_OK(ValidateProgress(update));
    WorkerId failed;
    WorkerId recovery;
    RETURN_IF_NOT_OK(SplitTaskId(update.taskId, failed, recovery));
    CHECK_FAIL_RETURN_STATUS(update.workerId == recovery, K_INVALID, "worker mismatch");

    HashRingPb ring;
    RETURN_IF_NOT_OK(DecodeRing(currentBytes, ring));
    auto failedIter = ring.mutable_del_node_info()->find(failed);
    CHECK_FAIL_RETURN_STATUS(failedIter != ring.mutable_del_node_info()->end(), K_NOT_FOUND, "recovery task not found");
    bool found = false;
    for (auto &rangePb : *failedIter->second.mutable_changed_ranges()) {
        if (rangePb.workerid() == recovery && rangePb.from() == update.range.begin
            && rangePb.end() == update.range.end) {
            found = true;
            rangePb.set_finished(true);
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS(found, K_INVALID, "progress range not found");
    CHECK_FAIL_RETURN_STATUS(ring.SerializeToString(&newBytes), K_INVALID, "serialize HashRingPb failed");
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
