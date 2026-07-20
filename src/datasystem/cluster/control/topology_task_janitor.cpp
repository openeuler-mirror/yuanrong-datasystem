/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: ETCD-only stale cluster topology task cleanup.
 */
#include "datasystem/cluster/control/topology_task_janitor.h"

#include <algorithm>
#include <charconv>
#include <exception>
#include <optional>
#include <unordered_set>
#include <utility>

#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr int32_t JANITOR_READ_TIMEOUT_MS = 3'000;
constexpr size_t MAX_SCAN_LIMIT = 8'192;
constexpr size_t MAX_DELETE_BATCH = 128;
constexpr size_t TASK_EPOCH_OFFSET = 3;
constexpr size_t SCALE_IN_MARKER_EPOCH_OFFSET = 1;
constexpr size_t SCALE_IN_MARKER_MIN_KEY_SIZE = 3;
constexpr char SCALE_IN_MARKER_EPOCH_PREFIX = 'e';

std::string TaskId(const TopologyTask &task)
{
    return std::visit([](const auto &value) { return value.taskId; }, task);
}

bool SameNotify(const TopologyTaskNotify &left, const TopologyTaskNotify &right)
{
    return left.type == right.type && left.taskIds == right.taskIds;
}

std::optional<uint64_t> TaskEpoch(const std::string &taskId)
{
    const auto separator = taskId.find('-', TASK_EPOCH_OFFSET);
    if (separator == std::string::npos || separator == TASK_EPOCH_OFFSET) {
        return std::nullopt;
    }
    uint64_t epoch = 0;
    const auto *begin = taskId.data() + TASK_EPOCH_OFFSET;
    const auto *end = taskId.data() + separator;
    const auto result = std::from_chars(begin, end, epoch);
    if (result.ec != std::errc{} || result.ptr != end || epoch == 0) {
        return std::nullopt;
    }
    return epoch;
}

std::optional<uint64_t> ScaleInMarkerEpoch(const std::string &key)
{
    const auto separator = key.find('/');
    if (key.size() < SCALE_IN_MARKER_MIN_KEY_SIZE || key.front() != SCALE_IN_MARKER_EPOCH_PREFIX
        || separator == std::string::npos || separator <= SCALE_IN_MARKER_EPOCH_OFFSET) {
        return std::nullopt;
    }
    uint64_t epoch = 0;
    const auto *begin = key.data() + SCALE_IN_MARKER_EPOCH_OFFSET;
    const auto *end = key.data() + separator;
    const auto result = std::from_chars(begin, end, epoch);
    if (result.ec != std::errc{} || result.ptr != end || epoch == 0) {
        return std::nullopt;
    }
    return epoch;
}

struct StaleTaskCleanupPass {
    TopologyTaskKind kind;
    const std::unordered_set<std::string> &expectedIds;
    size_t scanLimit;
    size_t deleteLimit;
    uint64_t maximumStaleEpoch;
    size_t &deletedCount;
};

Status DeleteStaleTasks(TopologyRepository &repository, const StaleTaskCleanupPass &pass)
{
    std::vector<TaskJanitorCandidate> candidates;
    RETURN_IF_NOT_OK(repository.ListTaskCandidatesForJanitor(pass.kind, pass.scanLimit, candidates));
    for (const auto &candidate : candidates) {
        if (pass.deletedCount >= pass.deleteLimit) {
            break;
        }
        if (pass.expectedIds.count(candidate.taskId) > 0) {
            continue;
        }
        const auto epoch = TaskEpoch(candidate.taskId);
        if (!epoch.has_value() || *epoch > pass.maximumStaleEpoch) {
            continue;
        }
        bool deleted = false;
        ++pass.deletedCount;
        RETURN_IF_NOT_OK(repository.DeleteTaskIfMatches(candidate, deleted));
    }
    return Status::OK();
}

Status DeleteStaleScaleInMetadataMarkers(TopologyRepository &repository, size_t scanLimit, size_t deleteLimit,
                                         uint64_t maximumStaleEpoch, size_t &deletedCount)
{
    std::vector<ScaleInMetadataDoneJanitorCandidate> candidates;
    RETURN_IF_NOT_OK(repository.ListScaleInMetadataDoneCandidatesForJanitor(scanLimit, candidates));
    for (const auto &candidate : candidates) {
        if (deletedCount >= deleteLimit) {
            break;
        }
        const auto epoch = ScaleInMarkerEpoch(candidate.key);
        if (!epoch.has_value() || *epoch > maximumStaleEpoch) {
            continue;
        }
        bool deleted = false;
        RETURN_IF_NOT_OK(repository.DeleteScaleInMetadataDoneIfMatches(candidate, deleted));
        if (deleted) {
            ++deletedCount;
        }
    }
    return Status::OK();
}

Status ReconcileNotifies(TopologyRepository &repository,
                         const std::map<std::string, TopologyTaskNotify> &expectedNotifies, size_t scanLimit,
                         size_t deleteLimit, size_t &deletedCount)
{
    std::vector<NotifyJanitorCandidate> candidates;
    RETURN_IF_NOT_OK(repository.ListNotifyCandidatesForJanitor(scanLimit, candidates));
    for (auto &candidate : candidates) {
        if (deletedCount >= deleteLimit) {
            break;
        }
        const auto expected = expectedNotifies.find(candidate.address);
        if (expected != expectedNotifies.end() && SameNotify(candidate.notify, expected->second)) {
            continue;
        }
        candidate.notify = expected == expectedNotifies.end() ? TopologyTaskNotify{} : expected->second;
        bool deleted = false;
        RETURN_IF_NOT_OK(repository.DeleteNotifyIfMatches(candidate, deleted));
        ++deletedCount;
    }
    return Status::OK();
}
}  // namespace

TopologyTaskJanitor::TopologyTaskJanitor(TopologyRepository &repository, const IPlanningAlgorithm &algorithm,
                                         TopologyTaskMaterializer &materializer, TopologyTaskJanitorOptions options)
    : repository_(repository), algorithm_(algorithm), materializer_(materializer), options_(options)
{
}

bool TopologyTaskJanitorOptions::IsValid() const noexcept
{
    return interval.count() > 0 && scanLimit > 0 && scanLimit <= MAX_SCAN_LIMIT && deleteBatch > 0
           && deleteBatch <= MAX_DELETE_BATCH && deleteBatch <= scanLimit;
}

TopologyTaskJanitor::~TopologyTaskJanitor()
{
    LOG_IF_ERROR(Stop(std::chrono::steady_clock::time_point::max()),
                 "Stop cluster topology task Janitor during destruction");
}

Status TopologyTaskJanitor::Start()
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    CHECK_FAIL_RETURN_STATUS(!started_ && options_.IsValid(), K_INVALID,
                             "invalid or already started topology task Janitor");
    started_ = true;
    stopping_ = false;
    threadExited_ = false;
    try {
        thread_ = Thread(&TopologyTaskJanitor::Run, this);
        thread_.set_name("cluster-gc");
    } catch (const std::exception &error) {
        started_ = false;
        threadExited_ = true;
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start topology task Janitor failed: ") + error.what());
    }
    return Status::OK();
}

Status TopologyTaskJanitor::Stop(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    if (!started_) {
        return Status::OK();
    }
    stopping_ = true;
    wakeCv_.notify_all();
    if (!stoppedCv_.wait_until(lock, deadline, [this] { return threadExited_; })) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "topology task Janitor stop deadline exceeded");
    }
    lock.unlock();
    if (thread_.joinable()) {
        thread_.join();
    }
    lock.lock();
    started_ = false;
    stopping_ = false;
    return Status::OK();
}

Status TopologyTaskJanitor::RunOnce()
{
    std::lock_guard<std::mutex> passLock(passMutex_);
    TopologyReader reader(repository_);
    std::shared_ptr<const TopologySnapshot> latest;
    RETURN_IF_NOT_OK(reader.Read(JANITOR_READ_TIMEOUT_MS, latest));
    ExpectedDerivedState expected;
    RETURN_IF_NOT_OK(materializer_.RebuildExpected(*latest, algorithm_, expected));
    std::unordered_set<std::string> expectedIds;
    expectedIds.reserve(expected.tasks.size());
    for (const auto &task : expected.tasks) {
        expectedIds.insert(TaskId(task));
    }
    const auto &activeBatch = latest->GetActiveBatch();
    CHECK_FAIL_RETURN_STATUS(!activeBatch.has_value() || activeBatch->epoch > 0, K_INVALID,
                             "active topology batch has an invalid epoch");
    const uint64_t maximumStaleEpoch = activeBatch.has_value() ? activeBatch->epoch - 1 : latest->Version();
    size_t changed = 0;
    RETURN_IF_NOT_OK(DeleteStaleTasks(repository_, { TopologyTaskKind::MIGRATE, expectedIds, options_.scanLimit,
                                                     options_.deleteBatch, maximumStaleEpoch, changed }));
    RETURN_IF_NOT_OK(DeleteStaleTasks(repository_, { TopologyTaskKind::DELETE_MEMBER, expectedIds, options_.scanLimit,
                                                     options_.deleteBatch, maximumStaleEpoch, changed }));
    RETURN_IF_NOT_OK(DeleteStaleScaleInMetadataMarkers(repository_, options_.scanLimit, options_.deleteBatch,
                                                       maximumStaleEpoch, changed));
    return ReconcileNotifies(repository_, expected.notifiesByAddress, options_.scanLimit, options_.deleteBatch,
                             changed);
}

void TopologyTaskJanitor::Run()
{
    while (true) {
        {
            std::lock_guard<std::mutex> lock(lifecycleMutex_);
            if (stopping_) {
                break;
            }
        }
        auto rc = RunOnce();
        if (rc.IsError()) {
            LOG(WARNING) << "Cluster topology task Janitor pass failed: " << rc.ToString();
        }
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        if (wakeCv_.wait_for(lock, options_.interval, [this] { return stopping_; })) {
            break;
        }
    }
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    threadExited_ = true;
    stoppedCv_.notify_all();
}

}  // namespace datasystem::cluster
