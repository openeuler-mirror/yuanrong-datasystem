/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Atomic immutable cluster topology Snapshot publication.
 */
#include "datasystem/cluster/runtime/topology_snapshot_state.h"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <tuple>

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
struct TopologySnapshotState::PublicationSync {
    bthread::Mutex mutex;
    bthread::ConditionVariable published;
};

namespace {
std::atomic<uint64_t> g_snapshotStateInstanceId{ 1 };

struct ThreadSnapshotCache {
    const TopologySnapshotState *owner{ nullptr };
    uint64_t instanceId{ 0 };
    uint64_t generation{ 0 };
    std::weak_ptr<const TopologySnapshot> snapshot;
};

std::vector<TokenRange> MergeRanges(std::vector<TokenRange> ranges)
{
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) {
        return std::tie(left.from, left.end) < std::tie(right.from, right.end);
    });
    std::vector<TokenRange> merged;
    merged.reserve(ranges.size());
    for (const auto &range : ranges) {
        const bool separated =
            merged.empty()
            || (merged.back().end != std::numeric_limits<uint32_t>::max() && merged.back().end + 1 < range.from);
        if (separated) {
            merged.emplace_back(range);
        } else {
            merged.back().end = std::max(merged.back().end, range.end);
        }
    }
    return merged;
}
}  // namespace

TopologySnapshotState::TopologySnapshotState()
    : publicationSync_(std::make_unique<PublicationSync>()),
      instanceId_(g_snapshotStateInstanceId.fetch_add(1, std::memory_order_relaxed))
{
}

TopologySnapshotState::~TopologySnapshotState() = default;

Status TopologySnapshotState::Load(std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    thread_local ThreadSnapshotCache cache;
    const auto generation = publicationGeneration_.load(std::memory_order_acquire);
    std::shared_ptr<const TopologySnapshot> current;
    if (cache.owner == this && cache.instanceId == instanceId_ && cache.generation == generation) {
        current = cache.snapshot.lock();
    }
    if (current == nullptr) {
        current = std::atomic_load_explicit(&current_, std::memory_order_acquire);
        cache = { this, instanceId_, generation, current };
    }
    CHECK_FAIL_RETURN_STATUS(current != nullptr, K_NOT_READY, "cluster topology Snapshot is not ready");
    snapshot = std::move(current);
    return Status::OK();
}

Status TopologySnapshotState::Publish(std::shared_ptr<const TopologySnapshot> snapshot, SnapshotUpdateOutcome &outcome)
{
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_INVALID, "cannot publish a null cluster topology Snapshot");
    std::unique_lock<bthread::Mutex> lock(publicationSync_->mutex);
    auto current = std::atomic_load(&current_);
    if (current == nullptr || snapshot->Version() == current->Version() + 1) {
        ResetScaleOutHandoffIfBatchChanged(*snapshot);
        std::atomic_store_explicit(&current_, std::move(snapshot), std::memory_order_release);
        publicationGeneration_.fetch_add(1, std::memory_order_release);
        outcome = SnapshotUpdateOutcome::PUBLISHED;
        lock.unlock();
        publicationSync_->published.notify_all();
        return Status::OK();
    }
    if (snapshot->Version() < current->Version()) {
        outcome = SnapshotUpdateOutcome::VERSION_ROLLBACK;
    } else if (snapshot->Version() > current->Version()) {
        outcome = SnapshotUpdateOutcome::VERSION_GAP;
    } else if (snapshot->CanonicalDigest() == current->CanonicalDigest()) {
        outcome = SnapshotUpdateOutcome::IDEMPOTENT;
        return Status::OK();
    } else {
        outcome = SnapshotUpdateOutcome::CONFLICT;
    }
    RETURN_STATUS(K_INVALID, "cluster topology Snapshot version/digest rejected");
}

Status TopologySnapshotState::PublishAfterFullRebuild(std::shared_ptr<const TopologySnapshot> snapshot)
{
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_INVALID, "cannot rebuild from a null Snapshot");
    std::unique_lock<bthread::Mutex> lock(publicationSync_->mutex);
    auto current = std::atomic_load(&current_);
    CHECK_FAIL_RETURN_STATUS(current == nullptr || snapshot->Version() > current->Version(), K_INVALID,
                             "full rebuild must advance cluster topology version");
    ResetScaleOutHandoffIfBatchChanged(*snapshot);
    std::atomic_store_explicit(&current_, std::move(snapshot), std::memory_order_release);
    publicationGeneration_.fetch_add(1, std::memory_order_release);
    lock.unlock();
    publicationSync_->published.notify_all();
    return Status::OK();
}

Status TopologySnapshotState::WaitForVersion(uint64_t minimumVersion,
                                             std::chrono::steady_clock::time_point deadline,
                                             std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    CHECK_FAIL_RETURN_STATUS(minimumVersion > 0, K_INVALID, "minimum topology version must be positive");
    std::shared_ptr<const TopologySnapshot> current;
    std::unique_lock<bthread::Mutex> lock(publicationSync_->mutex);
    const auto ready = [&] {
        current = std::atomic_load_explicit(&current_, std::memory_order_acquire);
        return current != nullptr && current->Version() >= minimumVersion;
    };
    while (!ready()) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            RETURN_STATUS(K_TRY_AGAIN, "local cluster topology Snapshot has not reached the migration fence");
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::microseconds>(deadline - now).count();
        const int waitStatus = publicationSync_->published.wait_for(lock, remaining);
        if (waitStatus == ETIMEDOUT && !ready()) {
            RETURN_STATUS(K_TRY_AGAIN, "local cluster topology Snapshot has not reached the migration fence");
        }
        CHECK_FAIL_RETURN_STATUS(waitStatus == 0 || waitStatus == ETIMEDOUT, K_RUNTIME_ERROR,
                                 "wait for local cluster topology Snapshot failed");
    }
    snapshot = std::move(current);
    return Status::OK();
}

void TopologySnapshotState::RecordScaleOutHandoffCompletion(const TopologyExecutionFence &fence)
{
    if (fence.batchType != TopologyChangeType::SCALE_OUT || fence.phase != TopologyCallbackPhase::SCALE_OUT
        || fence.ranges.empty()) {
        return;
    }
    std::lock_guard<bthread::Mutex> lock(publicationSync_->mutex);
    const auto current = std::atomic_load_explicit(&current_, std::memory_order_acquire);
    if (current == nullptr || !current->GetActiveBatch().has_value()
        || current->GetActiveBatch()->type != TopologyChangeType::SCALE_OUT
        || current->GetActiveBatch()->epoch != fence.batchEpoch) {
        return;
    }
    auto previous = std::atomic_load_explicit(&scaleOutHandoffCompletion_, std::memory_order_acquire);
    std::vector<TokenRange> ranges = fence.ranges;
    if (previous != nullptr && previous->batchEpoch == fence.batchEpoch) {
        ranges.insert(ranges.end(), previous->ranges.begin(), previous->ranges.end());
    }
    auto completed = std::make_shared<const ScaleOutHandoffCompletion>(
        ScaleOutHandoffCompletion{ fence.batchEpoch, MergeRanges(std::move(ranges)) });
    std::atomic_store_explicit(&scaleOutHandoffCompletion_, std::move(completed), std::memory_order_release);
}

bool TopologySnapshotState::IsScaleOutHandoffComplete(uint64_t batchEpoch, uint32_t token) const noexcept
{
    const auto completed = std::atomic_load_explicit(&scaleOutHandoffCompletion_, std::memory_order_acquire);
    if (completed == nullptr || batchEpoch == 0 || completed->batchEpoch != batchEpoch) {
        return false;
    }
    const auto iter = std::lower_bound(completed->ranges.begin(), completed->ranges.end(), token,
                                       [](const auto &range, uint32_t value) { return range.end < value; });
    return iter != completed->ranges.end() && iter->from <= token;
}

Status TopologySnapshotState::AuthorizeCleanupIfCurrent(
    const TopologySnapshot &expected, const std::function<Status()> &authorize) const
{
    CHECK_FAIL_RETURN_STATUS(authorize != nullptr, K_INVALID, "empty cluster topology cleanup authorization");
    std::lock_guard<bthread::Mutex> lock(publicationSync_->mutex);
    const auto current = std::atomic_load_explicit(&current_, std::memory_order_acquire);
    CHECK_FAIL_RETURN_STATUS(current != nullptr && current->Version() == expected.Version()
                                 && current->CanonicalDigest() == expected.CanonicalDigest(),
                             K_INVALID, "cluster topology Snapshot changed before cleanup authorization");
    return authorize();
}

void TopologySnapshotState::Clear()
{
    std::lock_guard<bthread::Mutex> lock(publicationSync_->mutex);
    std::atomic_store_explicit(&current_, std::shared_ptr<const TopologySnapshot>{}, std::memory_order_release);
    std::atomic_store_explicit(&scaleOutHandoffCompletion_,
                               std::shared_ptr<const ScaleOutHandoffCompletion>{},
                               std::memory_order_release);
    publicationGeneration_.fetch_add(1, std::memory_order_release);
}

void TopologySnapshotState::ResetScaleOutHandoffIfBatchChanged(const TopologySnapshot &snapshot)
{
    const auto batch = snapshot.GetActiveBatch();
    const auto completion = std::atomic_load_explicit(&scaleOutHandoffCompletion_, std::memory_order_acquire);
    const bool stillCurrent = batch.has_value() && batch->type == TopologyChangeType::SCALE_OUT
                              && completion != nullptr && completion->batchEpoch == batch->epoch;
    if (!stillCurrent && completion != nullptr) {
        std::atomic_store_explicit(&scaleOutHandoffCompletion_,
                                   std::shared_ptr<const ScaleOutHandoffCompletion>{},
                                   std::memory_order_release);
    }
}

}  // namespace datasystem::cluster
