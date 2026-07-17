/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker-role cluster topology runtime composition root.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_ENGINE_H
#define DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_ENGINE_H

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"
#include "datasystem/cluster/runtime/topology_runtime_types.h"
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {

/**
 * @brief Worker-role composition root; not a business mega-facade.
 */
class TopologyEngine final {
public:
    /**
     * @brief Validate options and construct all Worker-role components.
     * @param[in] options Worker-role identity and bounded runtime options.
     * @param[in] backend Worker-role backend that outlives the Engine.
     * @param[in] routingAlgorithm Routing algorithm that outlives the Engine.
     * @param[in] callbacks Business callbacks that outlive the Engine.
     * @param[out] engine Constructed composition root; unchanged on failure.
     * @return K_OK on success; K_INVALID or construction error otherwise.
     */
    static Status Create(TopologyEngineOptions options, ICoordinationBackend &backend,
                         const IRoutingAlgorithm &routingAlgorithm, ITopologyPhaseCallbacks &callbacks,
                         std::unique_ptr<TopologyEngine> &engine);

    /**
     * @brief Stop and join all Worker-role runtime components before releasing state.
     */
    ~TopologyEngine();

    /**
     * @brief Disable copying the Worker-role composition root.
     */
    TopologyEngine(const TopologyEngine &) = delete;

    /**
     * @brief Disable copy assignment of the Worker-role composition root.
     */
    TopologyEngine &operator=(const TopologyEngine &) = delete;

    /**
     * @brief Establish Worker watches/lease, exact-read topology and start one state thread once.
     * @return K_OK on successful startup; a backend, validation, or thread-start error otherwise.
     * @note A failed or stopped Engine cannot be restarted because backend event-source shutdown is one-shot.
     */
    Status Start();

    /**
     * @brief Cancel/drain callbacks, close ingress, join and clear Snapshot before returning.
     * @param[in] deadline Absolute stop deadline, additionally bounded by options.stopGrace.
     * @return K_OK after all runtime work is stopped; the first timeout or stop error after safe teardown otherwise.
     * @note The call may wait past deadline to preserve object lifetime safety; the process manager owns the outer
     *       bound.
     */
    Status Stop(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Return lifecycle state without IO.
     * @return Current Engine lifecycle state.
     */
    TopologyEngineState GetState() const noexcept;

    /**
     * @brief Return process-local availability without IO.
     * @return Current process-local topology availability.
     */
    TopologyAvailabilityLevel GetAvailability() const noexcept;

    /**
     * @brief Return the foreground placement facade.
     * @return Stable facade reference valid for this Engine's lifetime.
     */
    const PlacementFacade &Placement() const noexcept;

    /**
     * @brief Return the local member/endpoint view.
     * @return Stable view reference valid for this Engine's lifetime.
     */
    MembershipEndpointView &Membership() noexcept;

    /**
     * @brief Atomically read the current PB-free Snapshot.
     * @param[out] snapshot Shared immutable Snapshot; unchanged when no Snapshot is available.
     * @return K_OK on success; K_NOT_READY before the first legal publish.
     */
    Status GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const;

    /**
     * @brief Export the last-good topology as canonical repository bytes without backend IO.
     * @param[out] topologyVersion Snapshot topology version; unchanged when no Snapshot exists.
     * @param[out] canonicalTopology Canonical topology bytes; unchanged when no Snapshot exists.
     * @return K_OK on success or K_NOT_FOUND before the first legal Snapshot.
     */
    Status GetRecoveryTopology(uint64_t &topologyVersion, std::string &canonicalTopology) const;

    /**
     * @brief Return fresh Worker-role backend evidence for the peer-state RPC.
     * @return Identity- and version-bound observation; stale or incomplete evidence is UNKNOWN.
     */
    ControlBackendObservation GetControlBackendObservation() const;

    /**
     * @brief Return a read-only, no-IO aggregate diagnostic value.
     * @return Current low-cardinality Engine diagnostics.
     */
    TopologyDiagnostics GetDiagnostics() const;

private:
    static constexpr int32_t ENGINE_READ_TIMEOUT_MS = 3'000;

    /**
     * @brief Construct validated Worker-role components after KeyHelper creation.
     * @param[in] options Validated Worker-role runtime options.
     * @param[in] backend Worker-role backend that outlives the Engine.
     * @param[in] routingAlgorithm Routing algorithm that outlives the Engine.
     * @param[in] callbacks Business callbacks that outlive the Engine.
     * @param[in] keys Validated key helper consumed by the Engine.
     */
    TopologyEngine(TopologyEngineOptions options, ICoordinationBackend &backend,
                   const IRoutingAlgorithm &routingAlgorithm, ITopologyPhaseCallbacks &callbacks,
                   std::unique_ptr<TopologyKeyHelper> keys);

    /**
     * @brief Non-blockingly enqueue one backend doorbell.
     * @param[in] event Move-only backend event consumed only as a doorbell.
     * @return K_OK on enqueue or coalescing; a bounded-queue or lifecycle error otherwise.
     */
    Status EnqueueCoordinationEvent(CoordinationEvent &&event);

    /**
     * @brief Worker serial event and tick loop.
     */
    void Run();

    /**
     * @brief Start the Worker state thread after dependencies are ready.
     * @return K_OK on success; K_RUNTIME_ERROR when thread construction fails.
     */
    Status StartStateThread();

    /**
     * @brief Disable event ingress and restore lifecycle state after a failed Start step.
     */
    void CleanupAfterStartFailure();

    /**
     * @brief Exact-read topology and publish or rebuild the Snapshot.
     * @param[in] fullRebuildAllowed Whether a version gap may replace the local Snapshot.
     * @return Read, validation, publish, or evidence status.
     */
    Status ReloadTopology(bool fullRebuildAllowed);

    /**
     * @brief Exact-read topology and the local notify, then admit referenced tasks.
     * @return Read, publish, notify, or task admission status.
     */
    Status ReloadTopologyAndNotify();

    /**
     * @brief Publish identity-bound evidence after one successful exact read.
     * @param[in] snapshot Exact-read authoritative Snapshot used to derive the evidence.
     * @return K_OK on success; an identity or Snapshot validation error otherwise.
     */
    Status PublishBackendEvidence(const TopologySnapshot &snapshot);

    /**
     * @brief Dispatch one queued event on the Worker state thread.
     * @param[in] event Move-only runtime event to process.
     * @return Callback completion, topology reload, notify read, or task admission status.
     */
    Status HandleRuntimeEvent(RuntimeEvent event);

    /**
     * @brief Enter global degraded or local isolation after backend loss.
     * @return K_OK after publishing an availability decision; probe errors otherwise.
     */
    Status HandleBackendUnavailable();

    /**
     * @brief Re-probe failure scope while the local backend remains unavailable.
     * @return K_OK after publishing CONTROL_DEGRADED or ROLE_ISOLATED.
     */
    Status ReevaluateFailureScope();

    /**
     * @brief Periodically exact-read for recovery, otherwise refresh failure scope.
     * @return K_OK after recovery; the latest exact-read error while unavailable.
     */
    Status RefreshUnavailableBackend();

    /**
     * @brief Publish process-local availability and diagnostic reason.
     * @param[in] level New process-local availability level.
     * @param[in] reason Low-cardinality reason stored with the level.
     */
    void SetAvailability(TopologyAvailabilityLevel level, std::string reason);

    /**
     * @brief Apply the Host admission hook without allowing exceptions to escape the state thread.
     * @param[in] level Availability level delivered to the Host.
     */
    void NotifyAvailability(TopologyAvailabilityLevel level) noexcept;

    /**
     * @brief Record a runtime error without exposing high-cardinality payload.
     * @param[in] status Runtime error to retain and log.
     */
    void RecordError(const Status &status);

    TopologyEngineOptions options_;
    ICoordinationBackend &backend_;
    const IRoutingAlgorithm &routingAlgorithm_;
    std::unique_ptr<TopologyKeyHelper> keys_;
    TopologyRepository repository_;
    TopologyReader reader_;
    TopologySnapshotState snapshots_;
    CoordinationEventDispatcher dispatcher_;
    MembershipEndpointView membershipView_;
    PlacementFacade placement_;
    TopologyTaskExecutor executor_;
    // Protects the complete Start/Stop transaction, startAttempted_, and stateThread_ construction/join sequence.
    // The state thread never acquires this lock.
    std::mutex lifecycleMutex_;
    // Protects threadExited_, isolationReason_, lastError_, and backendObservation_.
    mutable std::mutex stateMutex_;
    // Serializes availability_, publishedAvailability_, isolationReason_, and Host admission callback transitions.
    std::mutex availabilityTransitionMutex_;
    // Uses stateMutex_ and signals changes to threadExited_.
    std::condition_variable stoppedCv_;
    Thread stateThread_;
    bool startAttempted_{ false };
    bool threadExited_{ true };
    std::atomic<TopologyEngineState> state_{ TopologyEngineState::STOPPED };
    std::atomic<TopologyAvailabilityLevel> availability_{ TopologyAvailabilityLevel::NOT_READY };
    // Exposes availability only after the corresponding Host admission transition has completed.
    std::atomic<TopologyAvailabilityLevel> publishedAvailability_{ TopologyAvailabilityLevel::NOT_READY };
    // Permanently fences this Engine lifetime after authoritative rollback or same-version content conflict.
    std::atomic<bool> authorityIsolated_{ false };
    std::string isolationReason_;
    std::string lastError_;
    ControlBackendObservation backendObservation_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_ENGINE_H
