/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Standalone exact cluster topology Observer.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_OBSERVER_H
#define DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_OBSERVER_H

#include <condition_variable>

#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {

struct TopologyObserverDiagnostics {
    bool running{ false };
    bool stopping{ false };
    uint64_t topologyVersion{ 0 };
    uint64_t readFailures{ 0 };
    uint64_t shutdownTimeouts{ 0 };
    bool resyncRequired{ false };
    std::string lastError;
};

/**
 * @brief Standalone exact-topology observer with one serial state thread.
 */
class TopologyObserver final {
public:
    /**
     * @brief Bind Observer-exclusive dependencies.
     * @param[in] backend Backend that outlives this Observer.
     * @param[in] reader Reader that outlives this Observer.
     * @param[in] keys Key helper that outlives this Observer.
     */
    TopologyObserver(ICoordinationBackend &backend, TopologyReader &reader, const TopologyKeyHelper &keys);

    /**
     * @brief Shut down and join the state thread before releasing Observer state.
     */
    ~TopologyObserver();

    /**
     * @brief Disable copying a state-thread owner.
     */
    TopologyObserver(const TopologyObserver &) = delete;

    /**
     * @brief Disable copy assignment of a state-thread owner.
     */
    TopologyObserver &operator=(const TopologyObserver &) = delete;

    /**
     * @brief Register the exact watch, read back, and start the serial loop.
     * @return K_OK on success; a backend, validation, or thread-start error otherwise.
     */
    Status Start();

    /**
     * @brief Load the last-good Snapshot without backend IO.
     * @param[out] snapshot Shared immutable Snapshot; unchanged when no Snapshot is available.
     * @return K_OK on success; K_NOT_READY before the first legal publish.
     */
    Status GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const;

    /**
     * @brief Stop ingress and join no later than deadline; timeout remains STOPPING.
     * @param[in] deadline Absolute monotonic shutdown deadline.
     * @return K_OK after join; K_RPC_DEADLINE_EXCEEDED when the deadline expires first.
     */
    Status Shutdown(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Return no-IO Observer diagnostics.
     * @return Current low-cardinality Observer diagnostics.
     */
    TopologyObserverDiagnostics GetDiagnostics() const;

private:
    static constexpr int32_t OBSERVER_READ_TIMEOUT_MS = 3'000;

    /**
     * @brief Consume topology doorbells and publish exact-read Snapshots on the Observer state thread.
     */
    void Run();

    /**
     * @brief Exact-read and publish one topology Snapshot.
     * @param[in] fullRebuildAllowed Whether a version gap may replace the local Snapshot.
     * @return Read, validation, or publish status.
     */
    Status Reload(bool fullRebuildAllowed);

    Status RegisterWatchEvents();

    Status ReloadInitialSnapshot();

    Status StartStateThread();

    void InstallEventHandler();

    void CleanupAfterStartFailure(const char *cleanupMessage);

    /**
     * @brief Record one exact-read failure in Observer diagnostics.
     * @param[in] status Failure status to retain.
     */
    void RecordReadFailure(const Status &status);

    ICoordinationBackend &backend_;
    TopologyReader &reader_;
    const TopologyKeyHelper &keys_;
    TopologySnapshotState snapshots_;
    CoordinationEventDispatcher dispatcher_{ 1024 };
    // Protects started_, stopping_, threadExited_, diagnostics_, and stateThread_ join coordination.
    mutable std::mutex stateMutex_;
    // Uses stateMutex_ and signals changes to threadExited_.
    std::condition_variable stoppedCv_;
    Thread stateThread_;
    bool started_{ false };
    bool stopping_{ false };
    bool threadExited_{ true };
    TopologyObserverDiagnostics diagnostics_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_OBSERVER_H
