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
constexpr char TASK_DELETE_TOMBSTONE_PREFIX[] = "cluster-task-delete-tombstone-v1-";
constexpr char NOTIFY_DELETE_TOMBSTONE_PREFIX[] = "cluster-notify-delete-tombstone-v1-";
constexpr char SCALE_IN_METADATA_DONE_DELETE_TOMBSTONE_PREFIX[] = "cluster-scalein-metadata-delete-tombstone-v1-";
constexpr char DELETE_TOMBSTONE_FIRST_SUFFIX[] = "0";
constexpr char DELETE_TOMBSTONE_SECOND_SUFFIX[] = "1";

bool HasPrefix(const std::string &bytes, const char *prefix)
{
    return bytes.rfind(prefix, 0) == 0;
}

std::string NextDeleteTombstone(const char *prefix, const std::string &current)
{
    std::string first(prefix);
    first.append(DELETE_TOMBSTONE_FIRST_SUFFIX);
    if (current != first) {
        return first;
    }
    std::string second(prefix);
    second.append(DELETE_TOMBSTONE_SECOND_SUFFIX);
    return second;
}

bool IsTaskDeleteTombstone(const std::string &bytes)
{
    return HasPrefix(bytes, TASK_DELETE_TOMBSTONE_PREFIX);
}

bool IsNotifyDeleteTombstone(const std::string &bytes)
{
    return HasPrefix(bytes, NOTIFY_DELETE_TOMBSTONE_PREFIX);
}

bool IsScaleInMetadataDoneDeleteTombstone(const std::string &bytes)
{
    return HasPrefix(bytes, SCALE_IN_METADATA_DONE_DELETE_TOMBSTONE_PREFIX);
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
    CHECK_FAIL_RETURN_STATUS(!bytes.empty() && !IsNotifyDeleteTombstone(bytes), K_NOT_FOUND,
                             "topology notify is logically deleted");
    return TopologyRepositoryCodec::DecodeNotify(bytes, notify);
}

Status TopologyRepository::RewriteNotify(const std::string &address, const TopologyTaskNotify &expected)
{
    std::string bytes;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeNotify(expected, bytes));
    return RewriteEncodedNotify(address, bytes);
}

Status TopologyRepository::RewriteEncodedNotify(const std::string &address, const std::string &bytes)
{
    std::string key;
    RETURN_IF_NOT_OK(TopologyKeyHelper::NotifyKey(address, key));
    ICoordinationBackend::ProcessFunction process = [&bytes](const std::string &current,
                                                             std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        if (IsNotifyDeleteTombstone(current)) {
            RETURN_STATUS(K_TRY_AGAIN, "topology notify physical deletion is in progress");
        }
        if (current != bytes) {
            next = std::make_unique<std::string>(bytes);
        }
        return Status::OK();
    };
    auto rc = backend_.CAS(keys_.NotifyTable(), key, process);
    if (rc.IsOk() || rc.GetCode() == K_TRY_AGAIN) {
        return rc;
    }
    return ResolveExactWrite(backend_, keys_.NotifyTable(), key, bytes, rc);
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

Status TopologyRepository::MarkScaleInMetadataDone(const ScaleInMetadataDoneRecord &record)
{
    CHECK_FAIL_RETURN_STATUS(!record.businessOperationId.empty(), K_INVALID,
                             "empty ScaleIn metadata operation id");
    std::string key;
    RETURN_IF_NOT_OK(
        TopologyKeyHelper::ScaleInMetadataDoneKey(record.batchEpoch, record.sourceId, record.taskId, key));
    ICoordinationBackend::ProcessFunction process = [&record](const std::string &current,
                                                              std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        if (current.empty()) {
            next = std::make_unique<std::string>(record.businessOperationId);
            return Status::OK();
        }
        if (IsScaleInMetadataDoneDeleteTombstone(current)) {
            RETURN_STATUS(K_TRY_AGAIN, "ScaleIn metadata marker physical deletion is in progress");
        }
        CHECK_FAIL_RETURN_STATUS(current == record.businessOperationId, K_INVALID,
                                 "ScaleIn metadata marker has different operation id");
        return Status::OK();
    };
    auto rc = backend_.CAS(keys_.ScaleInMetadataDoneTable(), key, process);
    if (rc.IsOk() || rc.GetCode() == K_TRY_AGAIN) {
        return rc;
    }
    return ResolveExactWrite(backend_, keys_.ScaleInMetadataDoneTable(), key, record.businessOperationId, rc);
}

Status TopologyRepository::CountScaleInMetadataDone(uint64_t batchEpoch, const std::string &sourceId,
                                                    size_t &count) const
{
    std::string prefix;
    RETURN_IF_NOT_OK(TopologyKeyHelper::ScaleInMetadataDonePrefix(batchEpoch, sourceId, prefix));
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(keys_.ScaleInMetadataDoneTable(), values));
    count = static_cast<size_t>(std::count_if(values.begin(), values.end(), [&prefix](const auto &value) {
        return value.first.rfind(prefix, 0) == 0 && !value.second.empty()
               && !IsScaleInMetadataDoneDeleteTombstone(value.second);
    }));
    return Status::OK();
}

Status TopologyRepository::ListTaskCandidatesForJanitor(TopologyTaskKind kind, size_t limit,
                                                        std::vector<TaskJanitorCandidate> &tasks) const
{
    std::string cursor;
    return ListTaskCandidatesForJanitor(kind, limit, cursor, tasks);
}

template <typename Visit>
Status VisitRotatingPage(std::vector<std::pair<std::string, std::string>> &values, size_t limit,
                         std::string &cursor, Visit visit)
{
    std::sort(values.begin(), values.end(), [](const auto &left, const auto &right) {
        return left.first < right.first;
    });
    if (values.empty()) {
        cursor.clear();
        return Status::OK();
    }
    auto begin = std::upper_bound(values.begin(), values.end(), cursor, [](const auto &key, const auto &entry) {
        return key < entry.first;
    });
    size_t index = begin == values.end() ? 0 : static_cast<size_t>(std::distance(values.begin(), begin));
    const size_t count = std::min(limit, values.size());
    std::string nextCursor = cursor;
    for (size_t visited = 0; visited < count; ++visited) {
        const auto &entry = values[index];
        RETURN_IF_NOT_OK(visit(entry));
        nextCursor = entry.first;
        index = (index + 1) % values.size();
    }
    cursor = std::move(nextCursor);
    return Status::OK();
}

Status TopologyRepository::ListTaskCandidatesForJanitor(TopologyTaskKind kind, size_t limit,
                                                        std::string &cursor,
                                                        std::vector<TaskJanitorCandidate> &tasks) const
{
    CHECK_FAIL_RETURN_STATUS(limit > 0, K_INVALID, "task Janitor scan limit must be positive");
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(TaskTable(kind), values));
    std::vector<TaskJanitorCandidate> candidates;
    candidates.reserve(std::min(limit, values.size()));
    RETURN_IF_NOT_OK(VisitRotatingPage(values, limit, cursor, [&](const auto &entry) {
        const auto &[taskId, bytes] = entry;
        candidates.push_back({ kind, taskId, bytes });
        return Status::OK();
    }));
    tasks = std::move(candidates);
    return Status::OK();
}

Status TopologyRepository::ListNotifyCandidatesForJanitor(size_t limit,
                                                          std::vector<NotifyJanitorCandidate> &notifies) const
{
    std::string cursor;
    return ListNotifyCandidatesForJanitor(limit, cursor, notifies);
}

Status TopologyRepository::ListNotifyCandidatesForJanitor(size_t limit, std::string &cursor,
                                                          std::vector<NotifyJanitorCandidate> &notifies) const
{
    CHECK_FAIL_RETURN_STATUS(limit > 0, K_INVALID, "notify Janitor scan limit must be positive");
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(keys_.NotifyTable(), values));
    std::vector<NotifyJanitorCandidate> candidates;
    candidates.reserve(std::min(limit, values.size()));
    RETURN_IF_NOT_OK(VisitRotatingPage(values, limit, cursor, [&](const auto &entry) {
        const auto &[address, bytes] = entry;
        TopologyTaskNotify notify;
        if (!bytes.empty() && !IsNotifyDeleteTombstone(bytes)) {
            RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeNotify(bytes, notify));
        }
        candidates.push_back(
            { address, std::move(notify), bytes, bytes.empty() || IsNotifyDeleteTombstone(bytes) });
        return Status::OK();
    }));
    notifies = std::move(candidates);
    return Status::OK();
}

Status TopologyRepository::ListScaleInMetadataDoneCandidatesForJanitor(
    size_t limit, std::vector<ScaleInMetadataDoneJanitorCandidate> &markers) const
{
    std::string cursor;
    return ListScaleInMetadataDoneCandidatesForJanitor(limit, cursor, markers);
}

Status TopologyRepository::ListScaleInMetadataDoneCandidatesForJanitor(
    size_t limit, std::string &cursor, std::vector<ScaleInMetadataDoneJanitorCandidate> &markers) const
{
    CHECK_FAIL_RETURN_STATUS(limit > 0, K_INVALID, "ScaleIn metadata marker Janitor scan limit must be positive");
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend_.GetAll(keys_.ScaleInMetadataDoneTable(), values));
    std::vector<ScaleInMetadataDoneJanitorCandidate> candidates;
    candidates.reserve(std::min(limit, values.size()));
    RETURN_IF_NOT_OK(VisitRotatingPage(values, limit, cursor, [&](const auto &entry) {
        const auto &[key, bytes] = entry;
        candidates.push_back({ key, bytes });
        return Status::OK();
    }));
    markers = std::move(candidates);
    return Status::OK();
}

Status TopologyRepository::DeleteTaskIfMatches(const TaskJanitorCandidate &candidate, bool &deleted)
{
    deleted = false;
    bool matched = false;
    ICoordinationBackend::ProcessFunction process = [&](const std::string &current,
                                                        std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        matched = current == candidate.matchToken;
        if (matched) {
            next = std::make_unique<std::string>(NextDeleteTombstone(TASK_DELETE_TOMBSTONE_PREFIX, current));
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(backend_.CAS(TaskTable(candidate.kind), candidate.taskId, process));
    if (!matched) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(backend_.Delete(TaskTable(candidate.kind), candidate.taskId));
    deleted = true;
    return Status::OK();
}

Status TopologyRepository::ReconcileNotifyIfMatches(const NotifyJanitorCandidate &candidate, bool &changed)
{
    changed = false;
    std::string key;
    RETURN_IF_NOT_OK(TopologyKeyHelper::NotifyKey(candidate.address, key));
    std::string replacement;
    if (candidate.notify.activeBatch.has_value() || !candidate.notify.taskIds.empty()
        || !candidate.notify.restartTimestampsByAddress.empty()) {
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeNotify(candidate.notify, replacement));
    }
    const bool shouldDelete = replacement.empty();
    bool matched = false;
    ICoordinationBackend::ProcessFunction process = [&](const std::string &current, std::unique_ptr<std::string> &next,
                                                        bool &retry) {
        retry = false;
        matched = current == candidate.matchToken;
        if (matched) {
            next = std::make_unique<std::string>(
                shouldDelete ? NextDeleteTombstone(NOTIFY_DELETE_TOMBSTONE_PREFIX, current) : replacement);
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(backend_.CAS(keys_.NotifyTable(), key, process));
    if (!matched) {
        return Status::OK();
    }
    if (shouldDelete) {
        RETURN_IF_NOT_OK(backend_.Delete(keys_.NotifyTable(), key));
    }
    changed = true;
    return Status::OK();
}

Status TopologyRepository::DeleteScaleInMetadataDoneIfMatches(
    const ScaleInMetadataDoneJanitorCandidate &candidate, bool &deleted)
{
    deleted = false;
    bool matched = false;
    ICoordinationBackend::ProcessFunction process = [&](const std::string &current,
                                                        std::unique_ptr<std::string> &next, bool &retry) {
        retry = false;
        matched = current == candidate.matchToken;
        if (matched) {
            next = std::make_unique<std::string>(
                NextDeleteTombstone(SCALE_IN_METADATA_DONE_DELETE_TOMBSTONE_PREFIX, current));
        }
        return Status::OK();
    };
    RETURN_IF_NOT_OK(backend_.CAS(keys_.ScaleInMetadataDoneTable(), candidate.key, process));
    if (!matched) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(backend_.Delete(keys_.ScaleInMetadataDoneTable(), candidate.key));
    deleted = true;
    return Status::OK();
}

}  // namespace datasystem::cluster
