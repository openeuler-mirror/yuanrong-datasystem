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
#include "datasystem/cluster/repository/topology_repository.h"

#include <algorithm>
#include <utility>

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr char TASK_DELETE_TOMBSTONE[] = "cluster-task-delete-tombstone-v1";

bool IsTaskDeleteTombstone(const std::string &bytes)
{
    return bytes == TASK_DELETE_TOMBSTONE;
}

Status EncodeTask(const TopologyTask &task, std::string &taskId, TopologyTaskKind &kind, std::string &bytes)
{
    if (std::holds_alternative<TopologyMigrateTask>(task)) {
        const auto &migrate = std::get<TopologyMigrateTask>(task);
        taskId = migrate.taskId;
        kind = TopologyTaskKind::MIGRATE;
        return TopologyRepositoryCodec::EncodeMigrateTask(migrate, bytes);
    }
    const auto &remove = std::get<TopologyDeleteTask>(task);
    taskId = remove.taskId;
    kind = TopologyTaskKind::DELETE_MEMBER;
    return TopologyRepositoryCodec::EncodeDeleteTask(remove, bytes);
}

bool CompatibleRangeProgress(const std::vector<TopologyTaskRange> &observed,
                             const std::vector<TopologyTaskRange> &expected)
{
    return observed.size() == expected.size() &&
           std::equal(observed.begin(), observed.end(), expected.begin(), [](const auto &first, const auto &second) {
               return first.ownerAddress == second.ownerAddress && first.range == second.range &&
                      (!second.finished || first.finished);
           });
}

Status ValidateCompatibleTask(const TopologyTask &expected, const std::string &observedBytes)
{
    if (std::holds_alternative<TopologyMigrateTask>(expected)) {
        const auto &value = std::get<TopologyMigrateTask>(expected);
        TopologyMigrateTask observed;
        RETURN_IF_NOT_OK(
            TopologyRepositoryCodec::DecodeMigrateTask(value.taskId, value.type, value.epoch, observedBytes, observed));
        CHECK_FAIL_RETURN_STATUS(observed.executorAddress == value.executorAddress
                                     && observed.targetAddress == value.targetAddress
                                     && CompatibleRangeProgress(observed.sourceRanges, value.sourceRanges),
                                 K_INVALID, "task id has different immutable migrate scope");
        return Status::OK();
    }
    const auto &value = std::get<TopologyDeleteTask>(expected);
    TopologyDeleteTask observed;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeDeleteTask(value.taskId, value.epoch, observedBytes, observed));
    CHECK_FAIL_RETURN_STATUS(observed.executorAddress == value.executorAddress
                                 && observed.failedAddress == value.failedAddress
                                 && CompatibleRangeProgress(observed.recoveryRanges, value.recoveryRanges),
                             K_INVALID, "task id has different immutable recovery scope");
    return Status::OK();
}

Status ResolveCompatibleTaskWrite(ICoordinationBackend &backend, const std::string &table, const std::string &key,
                                  const TopologyTask &expected, const Status &writeStatus)
{
    std::string observedBytes;
    auto readStatus = backend.Get(table, key, observedBytes);
    if (readStatus.IsOk()) {
        if (IsTaskDeleteTombstone(observedBytes)) {
            return writeStatus;
        }
        RETURN_IF_NOT_OK(ValidateCompatibleTask(expected, observedBytes));
        return Status::OK();
    }
    return writeStatus;
}

Status ResolveExactWrite(ICoordinationBackend &backend, const std::string &table, const std::string &key,
                         const std::string &desiredBytes, const Status &writeStatus)
{
    std::string observedBytes;
    auto readStatus = backend.Get(table, key, observedBytes);
    if (readStatus.IsOk()) {
        CHECK_FAIL_RETURN_STATUS(observedBytes == desiredBytes, K_INVALID,
                                 "persistent identity has different canonical bytes");
        return Status::OK();
    }
    return writeStatus;
}

Status ApplyFenceToTask(TopologyTask &task, const TopologyExecutionFence &fence, TaskProgressOutcome &outcome)
{
    std::vector<TopologyTaskRange> *taskRanges = nullptr;
    std::string taskId;
    uint64_t epoch = 0;
    std::string executor;
    if (std::holds_alternative<TopologyMigrateTask>(task)) {
        auto &migrate = std::get<TopologyMigrateTask>(task);
        CHECK_FAIL_RETURN_STATUS(fence.taskKind == TopologyTaskKind::MIGRATE, K_INVALID, "task kind mismatch");
        taskId = migrate.taskId;
        epoch = migrate.epoch;
        executor = migrate.executorAddress;
        taskRanges = &migrate.sourceRanges;
    } else {
        auto &remove = std::get<TopologyDeleteTask>(task);
        CHECK_FAIL_RETURN_STATUS(fence.taskKind == TopologyTaskKind::DELETE_MEMBER, K_INVALID, "task kind mismatch");
        taskId = remove.taskId;
        epoch = remove.epoch;
        executor = remove.executorAddress;
        taskRanges = &remove.recoveryRanges;
    }
    CHECK_FAIL_RETURN_STATUS(taskId == fence.taskId && epoch == fence.batchEpoch && executor == fence.executor.address
                                 && !fence.ranges.empty(),
                             K_INVALID, "task execution fence mismatch");
    std::vector<TokenRange> unfinished;
    std::vector<TokenRange> allRanges;
    for (const auto &range : *taskRanges) {
        allRanges.emplace_back(range.range);
        if (!range.finished) {
            unfinished.emplace_back(range.range);
        }
    }
    if (unfinished.empty()) {
        outcome = allRanges == fence.ranges ? TaskProgressOutcome::ALREADY_FINISHED : TaskProgressOutcome::STALE;
        return Status::OK();
    }
    if (unfinished != fence.ranges) {
        outcome = TaskProgressOutcome::STALE;
        return Status::OK();
    }
    for (auto &range : *taskRanges) {
        range.finished = true;
    }
    outcome = TaskProgressOutcome::UPDATED;
    return Status::OK();
}
}  // namespace

TopologyRepository::TopologyRepository(ICoordinationBackend &backend, const TopologyKeyHelper &keys)
    : backend_(backend), keys_(keys)
{
}

Status TopologyRepository::ReadTopology(int32_t timeoutMs, TopologyState &state, int64_t &authorityRevision) const
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "invalid topology read timeout");
    RangeSearchResult result;
    RETURN_IF_NOT_OK(backend_.Get(keys_.TopologyTable(), TopologyKeyHelper::TopologyKey(), result, timeoutMs));
    TopologyState decoded;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeTopology(result.value, decoded));
    state = std::move(decoded);
    authorityRevision = result.modRevision;
    return Status::OK();
}

Status TopologyRepository::CompareAndSwapTopology(uint64_t expectedVersion, const TopologyState &desired,
                                                  TopologyCasResult &result)
{
    result = {};
    CHECK_FAIL_RETURN_STATUS(desired.version == expectedVersion + 1, K_INVALID, "topology CAS version must advance");
    std::string desiredBytes;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(desired, desiredBytes));
    bool conflict = false;
    ICoordinationBackend::ProcessFunction process = [&](const std::string &current, std::unique_ptr<std::string> &next,
                                                        bool &retry) {
        retry = false;
        if (current.empty() && expectedVersion == 0) {
            next = std::make_unique<std::string>(desiredBytes);
            return Status::OK();
        }
        TopologyState observed;
        auto rc = TopologyRepositoryCodec::DecodeTopology(current, observed);
        if (rc.IsError() || observed.version != expectedVersion) {
            conflict = true;
            return Status::OK();
        }
        next = std::make_unique<std::string>(desiredBytes);
        return Status::OK();
    };
    auto rc = backend_.CAS(keys_.TopologyTable(), TopologyKeyHelper::TopologyKey(), process);
    if (rc.IsOk() && !conflict) {
        result.outcome = TopologyCasOutcome::COMMITTED;
        return Status::OK();
    }
    RangeSearchResult readResult;
    auto readRc = backend_.Get(keys_.TopologyTable(), TopologyKeyHelper::TopologyKey(), readResult);
    if (readRc.IsOk()) {
        TopologyState observed;
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeTopology(readResult.value, observed));
        result.observed = observed;
        if (readResult.value == desiredBytes) {
            result.outcome = TopologyCasOutcome::COMMITTED;
            return Status::OK();
        }
        result.outcome = TopologyCasOutcome::CONFLICT;
        CHECK_FAIL_RETURN_STATUS(observed.version != desired.version, K_INVALID,
                                 "same topology version has different canonical bytes");
        return Status::OK();
    }
    result.outcome = TopologyCasOutcome::UNKNOWN;
    return rc.IsError() ? rc : readRc;
}

Status TopologyRepository::ReadMemberships(std::vector<MembershipRecord> &members) const
{
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(keys_.MembershipTable(), values));
    std::vector<MembershipRecord> decoded;
    decoded.reserve(values.size());
    for (const auto &[address, bytes] : values) {
        MembershipValue value;
        RETURN_IF_NOT_OK(MembershipValueCodec::Decode(bytes, value));
        decoded.emplace_back(MembershipRecord{ address, value.lifecycleState, value.timestamp, value.hostId });
    }
    std::sort(decoded.begin(), decoded.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    members = std::move(decoded);
    return Status::OK();
}

const std::string &TopologyRepository::TaskTable(TopologyTaskKind kind) const
{
    return kind == TopologyTaskKind::MIGRATE ? keys_.MigrateTaskTable() : keys_.DeleteTaskTable();
}

Status TopologyRepository::ReadTask(TopologyTaskKind kind, const std::string &taskId, TopologyChangeType type,
                                    uint64_t epoch, TopologyTask &task) const
{
    std::string key;
    RETURN_IF_NOT_OK(TopologyKeyHelper::TaskKey(taskId, key));
    std::string bytes;
    RETURN_IF_NOT_OK(backend_.Get(TaskTable(kind), key, bytes));
    CHECK_FAIL_RETURN_STATUS(!IsTaskDeleteTombstone(bytes), K_NOT_FOUND, "topology task is logically deleted");
    if (kind == TopologyTaskKind::MIGRATE) {
        TopologyMigrateTask decoded;
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeMigrateTask(taskId, type, epoch, bytes, decoded));
        task = std::move(decoded);
    } else {
        TopologyDeleteTask decoded;
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeDeleteTask(taskId, epoch, bytes, decoded));
        task = std::move(decoded);
    }
    return Status::OK();
}

Status TopologyRepository::CreateTaskIfAbsent(const TopologyTask &task)
{
    std::string taskId;
    std::string bytes;
    TopologyTaskKind kind;
    RETURN_IF_NOT_OK(EncodeTask(task, taskId, kind, bytes));
    ICoordinationBackend::ProcessFunction process = [&bytes, &task](const std::string &current,
                                                                    std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        if (IsTaskDeleteTombstone(current)) {
            RETURN_STATUS(K_TRY_AGAIN, "topology task physical deletion is in progress");
        }
        if (current.empty()) {
            next = std::make_unique<std::string>(bytes);
        } else {
            RETURN_IF_NOT_OK(ValidateCompatibleTask(task, current));
        }
        return Status::OK();
    };
    auto rc = backend_.CAS(TaskTable(kind), taskId, process);
    return rc.IsOk() ? rc : ResolveCompatibleTaskWrite(backend_, TaskTable(kind), taskId, task, rc);
}

Status TopologyRepository::ReadNotify(const std::string &address, TopologyTaskNotify &notify) const
{
    std::string key;
    RETURN_IF_NOT_OK(TopologyKeyHelper::NotifyKey(address, key));
    std::string bytes;
    RETURN_IF_NOT_OK(backend_.Get(keys_.NotifyTable(), key, bytes));
    CHECK_FAIL_RETURN_STATUS(!bytes.empty(), K_NOT_FOUND, "topology notify is logically deleted");
    return TopologyRepositoryCodec::DecodeNotify(bytes, notify);
}

Status TopologyRepository::RewriteNotify(const std::string &address, const TopologyTaskNotify &expected)
{
    std::string key;
    std::string bytes;
    RETURN_IF_NOT_OK(TopologyKeyHelper::NotifyKey(address, key));
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeNotify(expected, bytes));
    ICoordinationBackend::ProcessFunction process = [&bytes](const std::string &current,
                                                             std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        if (current != bytes) {
            next = std::make_unique<std::string>(bytes);
        }
        return Status::OK();
    };
    auto rc = backend_.CAS(keys_.NotifyTable(), key, process);
    return rc.IsOk() ? rc : ResolveExactWrite(backend_, keys_.NotifyTable(), key, bytes, rc);
}

Status TopologyRepository::MarkTaskScopeFinished(const TopologyExecutionFence &fence, TaskProgressOutcome &outcome)
{
    outcome = TaskProgressOutcome::UNKNOWN;
    ICoordinationBackend::ProcessFunction process = [&](const std::string &current, std::unique_ptr<std::string> &next,
                                                        bool &retry) {
        retry = false;
        TopologyTask task;
        if (fence.taskKind == TopologyTaskKind::MIGRATE) {
            TopologyMigrateTask decoded;
            RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeMigrateTask(fence.taskId, fence.batchType, fence.batchEpoch,
                                                                        current, decoded));
            task = std::move(decoded);
        } else {
            TopologyDeleteTask decoded;
            RETURN_IF_NOT_OK(
                TopologyRepositoryCodec::DecodeDeleteTask(fence.taskId, fence.batchEpoch, current, decoded));
            task = std::move(decoded);
        }
        RETURN_IF_NOT_OK(ApplyFenceToTask(task, fence, outcome));
        if (outcome == TaskProgressOutcome::UPDATED) {
            std::string taskId;
            TopologyTaskKind kind;
            std::string bytes;
            RETURN_IF_NOT_OK(EncodeTask(task, taskId, kind, bytes));
            next = std::make_unique<std::string>(std::move(bytes));
        }
        return Status::OK();
    };
    auto rc = backend_.CAS(TaskTable(fence.taskKind), fence.taskId, process);
    if (rc.IsError()) {
        outcome = TaskProgressOutcome::UNKNOWN;
        TopologyTask observed;
        auto readRc = ReadTask(fence.taskKind, fence.taskId, fence.batchType, fence.batchEpoch, observed);
        if (readRc.IsOk()) {
            TaskProgressOutcome observedOutcome;
            RETURN_IF_NOT_OK(ApplyFenceToTask(observed, fence, observedOutcome));
            if (observedOutcome == TaskProgressOutcome::ALREADY_FINISHED) {
                outcome = observedOutcome;
                return Status::OK();
            }
        }
    }
    return rc;
}

Status TopologyRepository::ListTaskCandidatesForJanitor(TopologyTaskKind kind, size_t limit,
                                                        std::vector<TaskJanitorCandidate> &tasks) const
{
    CHECK_FAIL_RETURN_STATUS(limit > 0, K_INVALID, "task Janitor scan limit must be positive");
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(TaskTable(kind), values));
    std::sort(values.begin(), values.end());
    std::vector<TaskJanitorCandidate> candidates;
    candidates.reserve(std::min(limit, values.size()));
    for (const auto &[taskId, bytes] : values) {
        if (candidates.size() >= limit) {
            break;
        }
        candidates.push_back({ kind, taskId, bytes });
    }
    tasks = std::move(candidates);
    return Status::OK();
}

Status TopologyRepository::ListNotifyCandidatesForJanitor(size_t limit,
                                                          std::vector<NotifyJanitorCandidate> &notifies) const
{
    CHECK_FAIL_RETURN_STATUS(limit > 0, K_INVALID, "notify Janitor scan limit must be positive");
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(keys_.NotifyTable(), values));
    std::sort(values.begin(), values.end());
    std::vector<NotifyJanitorCandidate> candidates;
    candidates.reserve(std::min(limit, values.size()));
    for (const auto &[address, bytes] : values) {
        if (candidates.size() >= limit) {
            break;
        }
        if (bytes.empty()) {
            continue;
        }
        TopologyTaskNotify notify;
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeNotify(bytes, notify));
        candidates.push_back({ address, std::move(notify), bytes });
    }
    notifies = std::move(candidates);
    return Status::OK();
}

Status TopologyRepository::DeleteTaskIfMatches(const TaskJanitorCandidate &candidate, bool &deleted)
{
    deleted = false;
    if (!IsTaskDeleteTombstone(candidate.matchToken)) {
        bool matched = false;
        ICoordinationBackend::ProcessFunction process = [&](const std::string &current,
                                                            std::unique_ptr<std::string> &next, bool &retry) {
            retry = false;
            matched = current == candidate.matchToken;
            if (matched) {
                next = std::make_unique<std::string>(TASK_DELETE_TOMBSTONE);
            }
            return Status::OK();
        };
        RETURN_IF_NOT_OK(backend_.CAS(TaskTable(candidate.kind), candidate.taskId, process));
        if (!matched) {
            return Status::OK();
        }
    }
    RETURN_IF_NOT_OK(backend_.Delete(TaskTable(candidate.kind), candidate.taskId));
    deleted = true;
    return Status::OK();
}

Status TopologyRepository::DeleteNotifyIfMatches(const NotifyJanitorCandidate &candidate, bool &deleted)
{
    deleted = false;
    std::string key;
    RETURN_IF_NOT_OK(TopologyKeyHelper::NotifyKey(candidate.address, key));
    std::string replacement;
    if (!candidate.notify.taskIds.empty()) {
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeNotify(candidate.notify, replacement));
    }
    bool matched = false;
    ICoordinationBackend::ProcessFunction process = [&](const std::string &current, std::unique_ptr<std::string> &next,
                                                        bool &retry) {
        retry = false;
        matched = current == candidate.matchToken;
        if (matched) {
            next = std::make_unique<std::string>(replacement);
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(backend_.CAS(keys_.NotifyTable(), key, process));
    if (!matched) {
        return Status::OK();
    }
    deleted = replacement.empty();
    return Status::OK();
}

}  // namespace datasystem::cluster
