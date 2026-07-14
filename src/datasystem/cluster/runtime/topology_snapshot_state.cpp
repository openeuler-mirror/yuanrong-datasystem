/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Atomic immutable cluster topology Snapshot publication.
 */
#include "datasystem/cluster/runtime/topology_snapshot_state.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
std::atomic<uint64_t> g_snapshotStateInstanceId{ 1 };

struct ThreadSnapshotCache {
    const TopologySnapshotState *owner{ nullptr };
    uint64_t instanceId{ 0 };
    uint64_t generation{ 0 };
    std::weak_ptr<const TopologySnapshot> snapshot;
};
}  // namespace

TopologySnapshotState::TopologySnapshotState()
    : instanceId_(g_snapshotStateInstanceId.fetch_add(1, std::memory_order_relaxed))
{
}

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
    std::lock_guard<std::mutex> lock(publishMutex_);
    auto current = std::atomic_load(&current_);
    if (current == nullptr || snapshot->Version() == current->Version() + 1) {
        std::atomic_store_explicit(&current_, std::move(snapshot), std::memory_order_release);
        publicationGeneration_.fetch_add(1, std::memory_order_release);
        outcome = SnapshotUpdateOutcome::PUBLISHED;
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
    std::lock_guard<std::mutex> lock(publishMutex_);
    auto current = std::atomic_load(&current_);
    CHECK_FAIL_RETURN_STATUS(current == nullptr || snapshot->Version() > current->Version(), K_INVALID,
                             "full rebuild must advance cluster topology version");
    std::atomic_store_explicit(&current_, std::move(snapshot), std::memory_order_release);
    publicationGeneration_.fetch_add(1, std::memory_order_release);
    return Status::OK();
}

Status TopologySnapshotState::AuthorizeCleanupIfCurrent(
    const TopologySnapshot &expected, const std::function<Status()> &authorize) const
{
    CHECK_FAIL_RETURN_STATUS(authorize != nullptr, K_INVALID, "empty cluster topology cleanup authorization");
    std::lock_guard<std::mutex> lock(publishMutex_);
    const auto current = std::atomic_load_explicit(&current_, std::memory_order_acquire);
    CHECK_FAIL_RETURN_STATUS(current != nullptr && current->Version() == expected.Version()
                                 && current->CanonicalDigest() == expected.CanonicalDigest(),
                             K_INVALID, "cluster topology Snapshot changed before cleanup authorization");
    return authorize();
}

void TopologySnapshotState::Clear()
{
    std::lock_guard<std::mutex> lock(publishMutex_);
    std::atomic_store_explicit(&current_, std::shared_ptr<const TopologySnapshot>{}, std::memory_order_release);
    publicationGeneration_.fetch_add(1, std::memory_order_release);
}

}  // namespace datasystem::cluster
