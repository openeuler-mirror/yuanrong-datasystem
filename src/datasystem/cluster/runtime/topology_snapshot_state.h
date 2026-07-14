/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Atomic immutable cluster topology Snapshot publication.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_SNAPSHOT_STATE_H
#define DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_SNAPSHOT_STATE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "datasystem/cluster/model/topology_snapshot.h"

namespace datasystem::cluster {

class TopologyTaskExecutor;

enum class SnapshotUpdateOutcome : uint8_t { PUBLISHED, IDEMPOTENT, VERSION_GAP, CONFLICT, VERSION_ROLLBACK };

/**
 * @brief Thread-safe atomic shared-pointer holder; owns no backend or thread.
 */
class TopologySnapshotState final {
public:
    /**
     * @brief Construct an empty holder.
     */
    TopologySnapshotState();

    /**
     * @brief Destroy the current immutable reference.
     */
    ~TopologySnapshotState() = default;
    TopologySnapshotState(const TopologySnapshotState &) = delete;
    TopologySnapshotState &operator=(const TopologySnapshotState &) = delete;

    /**
     * @brief Atomically load the current Snapshot without retaining old generations in thread-local caches.
     * @param[out] snapshot Shared immutable Snapshot.
     * @return K_NOT_READY before first publish; K_OK otherwise.
     */
    Status Load(std::shared_ptr<const TopologySnapshot> &snapshot) const;

    /**
     * @brief Publish only an idempotent or next-version Snapshot.
     * @param[in] snapshot Candidate Snapshot.
     * @param[out] outcome Version/digest decision.
     * @return K_OK when accepted; K_INVALID otherwise.
     */
    Status Publish(std::shared_ptr<const TopologySnapshot> snapshot, SnapshotUpdateOutcome &outcome);

    /**
     * @brief Publish a complete authoritative rebuild after a version gap.
     * @param[in] snapshot Complete newer Snapshot.
     * @return K_OK when published; K_INVALID otherwise.
     */
    Status PublishAfterFullRebuild(std::shared_ptr<const TopologySnapshot> snapshot);

    /**
     * @brief Clear only after foreground admission and runtime threads stop.
     */
    void Clear();

private:
    friend class TopologyTaskExecutor;

    /**
     * @brief Authorize one short local cleanup only while the expected Snapshot is still current.
     * @param[in] expected Snapshot used by the latest exact fence validation.
     * @param[in] authorize No-IO local authorization run under the Snapshot publication lock.
     * @return K_OK on authorization; K_INVALID when the expected Snapshot is stale.
     */
    Status AuthorizeCleanupIfCurrent(const TopologySnapshot &expected,
                                     const std::function<Status()> &authorize) const;

    // Serializes current_ validation/publication/clearing, cleanup authorization, and publicationGeneration_ updates.
    mutable std::mutex publishMutex_;
    std::shared_ptr<const TopologySnapshot> current_;
    const uint64_t instanceId_;
    std::atomic<uint64_t> publicationGeneration_{ 0 };
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_SNAPSHOT_STATE_H
