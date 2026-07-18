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
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"
#include "datasystem/cluster/runtime/topology_runtime_types.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
class EtcdStore;
class ICoordinatorServiceProxy;
}

namespace datasystem::cluster {

class HashAlgorithm;
class ITopologyPhaseCallbacks;
class TopologyControllerRuntime;
class TopologyRecoveryReporter;

/**
 * @brief Bind and safely drain the Worker RPC ingress used by Coordinator-backed watches.
 */
struct CoordinatorWatchIngress {
    using Handler =
        std::function<Status(const std::string &, int64_t, CoordinationEvent &&)>;

    std::function<Status(Handler)> bind;
    std::function<Status(std::chrono::steady_clock::time_point)> unbindAndDrain;
};

/**
 * @brief Worker-role composition root; not a business mega-facade.
 */
class TopologyEngine final {
public:
    /**
     * @brief Collect the complete Worker topology composition without exposing its subcomponents.
     */
    class Builder final {
    public:
        /**
         * @brief Construct an empty one-shot Builder with bounded defaults.
         */
        Builder();

        /**
         * @brief Release resources retained by an unused or failed Builder.
         */
        ~Builder();

        /**
         * @brief Disable copying owned backend resources.
         */
        Builder(const Builder &) = delete;

        /**
         * @brief Disable copy assignment of owned backend resources.
         */
        Builder &operator=(const Builder &) = delete;

        /**
         * @brief Bind the cluster name; an empty name remains valid.
         * @param[in] clusterName Cluster name to consume.
         * @return This Builder.
         */
        Builder &SetClusterName(std::string clusterName);

        /**
         * @brief Bind the canonical local member address.
         * @param[in] localAddress Canonical local address to consume.
         * @return This Builder.
         */
        Builder &SetLocalAddress(std::string localAddress);

        /**
         * @brief Select ETCD using the two existing role-specific Store resources.
         * @param[in] memberStore Shared non-owned member-role Store.
         * @param[in] controllerStore Owned controller-role Store.
         * @return This Builder.
         */
        Builder &UseEtcd(EtcdStore &memberStore, std::unique_ptr<EtcdStore> controllerStore);

        /**
         * @brief Select Coordinator and bind its Worker watch RPC ingress.
         * @param[in] proxy Coordinator proxy that outlives the Engine.
         * @param[in] ingress Complete bind and unbind-and-drain contract.
         * @return This Builder.
         */
        Builder &UseCoordinator(ICoordinatorServiceProxy &proxy, CoordinatorWatchIngress ingress);

        /**
         * @brief Register the business topology phase callbacks.
         * @param[in] callbacks Callback owner that outlives the Engine.
         * @return This Builder.
         */
        Builder &SetPhaseCallbacks(ITopologyPhaseCallbacks &callbacks);

        /**
         * @brief Register the bounded control-backend failure-scope probe.
         * @param[in] probe Probe to consume.
         * @return This Builder.
         */
        Builder &SetControlBackendProbe(ControlBackendProbe probe);

        /**
         * @brief Register the non-blocking business-admission callback.
         * @param[in] handler Availability callback to consume.
         * @return This Builder.
         */
        Builder &SetAvailabilityHandler(std::function<void(TopologyAvailabilityLevel)> handler);

        /**
         * @brief Register the existing member-restart cleanup sink.
         * @param[in] handler Restart sink to consume.
         * @return This Builder.
         */
        Builder &SetMembershipRestartHandler(
            std::function<Status(const std::string &, int64_t)> handler);

        /**
         * @brief Register a non-blocking newly-published Snapshot callback.
         * @param[in] handler Callback that may only enqueue bounded work.
         * @return This Builder.
         */
        Builder &SetSnapshotPublishedHandler(
            std::function<void(std::shared_ptr<const TopologySnapshot>)> handler);

        /**
         * @brief Bind the configured confirmed-failure timeout.
         * @param[in] timeout Existing node-dead timeout.
         * @return This Builder.
         */
        Builder &SetNodeDeadTimeout(std::chrono::seconds timeout);

        /**
         * @brief Validate once, exact-read restart state, and construct every role component.
         * @param[out] engine Complete Engine; unchanged on failure.
         * @return K_OK on success; validation, read, or construction status otherwise.
         */
        Status Build(std::unique_ptr<TopologyEngine> &engine);

    private:
        friend class TopologyEngine;
        struct Config;

        /**
         * @brief Validate the complete one-shot Builder configuration.
         * @return K_OK when the configuration is complete; K_INVALID otherwise.
         */
        Status Validate() const;

        /**
         * @brief Register backend-local table mappings and construct two role backends plus one shared algorithm.
         * @return K_OK on success or the dependency construction status.
         */
        Status CreateOwnedDependencies();

        /**
         * @brief Exact-read the startup topology and derive the immutable restart fact.
         * @return K_OK on success or the exact-read/validation status.
         */
        Status ReadRestartFact();

        std::unique_ptr<Config> config_;
    };

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
     * @param[in] deadline Absolute stop deadline. Finite deadlines are additionally bounded by options.stopGrace;
     *                     time_point::max() requests the destructor-only final safe join.
     * @return K_OK after all runtime work is stopped; the first timeout or stop error after safe teardown otherwise.
     * @note The call may wait past deadline to preserve object lifetime safety; the process manager owns the outer
     *       bound.
     */
    Status Shutdown(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Publish the local membership as READY after the first lease is established.
     * @return K_OK on update; lifecycle or backend status otherwise.
     */
    Status MarkReady();

    /**
     * @brief Publish the local membership as EXITING for graceful ScaleIn.
     * @return K_OK on update; lifecycle or backend status otherwise.
     */
    Status MarkExiting();

    /**
     * @brief Inform the backend that restart reconciliation has completed.
     * @return K_OK on success; lifecycle, address, or backend status otherwise.
     */
    Status NotifyReconciliationDone();

    /**
     * @brief Report the immutable restart fact derived by the startup exact-read.
     * @return True only when the exact-read contained the local address.
     */
    bool IsRestart() const noexcept;

    /**
     * @brief Report whether the member-role backend established its first lease.
     * @return True after the first successful lease publication.
     */
    bool HasEstablishedMemberLease() const noexcept;

    /**
     * @brief Report whether the current member lease timed out.
     * @return True only for a confirmed local lease timeout.
     */
    bool IsMemberLeaseTimedOut() const noexcept;

    /**
     * @brief Read membership host identifiers for the existing routing response.
     * @param[out] hostIds Address-to-host-id map; unchanged on failure.
     * @return K_OK on success or backend/read/decode status otherwise.
     */
    Status GetRoutingHostIds(std::unordered_map<std::string, std::string> &hostIds) const;

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
    const MembershipEndpointView &Membership() const noexcept;

    /**
     * @brief Atomically read the current PB-free Snapshot.
     * @param[out] snapshot Shared immutable Snapshot; unchanged when no Snapshot is available.
     * @return K_OK on success; K_NOT_READY before the first legal publish.
     */
    Status GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const;

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
     * @brief Private Worker runtime settings hidden from business composition code.
     */
    struct RuntimeOptions {
        std::string clusterName;
        std::string localAddress;
        bool isRestart{ false };
        size_t eventQueueCapacity{ 1'024 };
        std::chrono::seconds scopeProbeDeadline{ 2 };
        std::chrono::seconds scopeProbeInterval{ 5 };
        std::chrono::seconds stopGrace{ 10 };
        ControlBackendProbe controlBackendProbe;
        std::function<void(TopologyAvailabilityLevel)> availabilityHandler;
        std::function<void(std::shared_ptr<const TopologySnapshot>)> snapshotPublishedHandler;
        TopologyTaskExecutorOptions executor;
    };

    /**
     * @brief Construct the Engine from one validated Builder configuration.
     * @param[in] config Validated configuration and owned role resources to consume.
     */
    explicit TopologyEngine(std::unique_ptr<Builder::Config> config);

    /**
     * @brief Move Worker-role runtime values out of a validated Builder configuration.
     * @param[in] config Validated Builder configuration.
     * @return Private runtime options consumed by the Engine constructor.
     */
    static RuntimeOptions ConsumeRuntimeOptions(Builder::Config &config);

    /**
     * @brief Construct Controller Runtime and optional recovery reporter after core members exist.
     * @param[in] membershipRestartHandler Existing Controller restart cleanup sink.
     * @param[in] nodeDeadTimeout Existing confirmed-failure timeout.
     * @return K_OK on success or component construction status otherwise.
     */
    Status InitializeOwnedComponents(
        std::function<Status(const std::string &, int64_t)> membershipRestartHandler,
        std::chrono::seconds nodeDeadTimeout);

    /**
     * @brief Route one Coordinator watch event to its unique role backend.
     * @param[in] coordinatorId Coordinator process identity.
     * @param[in] watchId Coordinator watch identity.
     * @param[in] event Event to consume.
     * @return K_OK on delivery or identity/lifecycle status otherwise.
     */
    Status RouteCoordinatorWatchEvent(const std::string &coordinatorId, int64_t watchId,
                                      CoordinationEvent &&event);

    /**
     * @brief Bind Coordinator ingress before role watches are established.
     * @return K_OK when absent or bound; ingress status otherwise.
     */
    Status BindCoordinatorIngress();

    /**
     * @brief Reject new Coordinator events and drain in-flight RPC handlers.
     * @param[in] deadline Absolute drain deadline.
     * @return K_OK when absent or drained; ingress status otherwise.
     */
    Status UnbindCoordinatorIngress(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Start only the member-role state machine after ingress is ready.
     * @return K_OK on success or member-role startup status otherwise.
     */
    Status StartMemberRole();

    /**
     * @brief Attempt every safe shutdown step and retain the first failure.
     * @param[in] deadline One absolute shutdown deadline.
     * @return First error after all safe steps are attempted.
     */
    Status ShutdownComponents(std::chrono::steady_clock::time_point deadline);

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
     * @return K_OK after complete rollback; the first cleanup error when Shutdown must be retried.
     */
    Status CleanupAfterStartFailure();

    /**
     * @brief Commit RUNNING and publish the latest traffic-allowing availability before releasing Start ownership.
     */
    void CommitSuccessfulStart();

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
     * @brief Export last-good canonical topology for the owned recovery reporter without backend IO.
     * @param[out] topologyVersion Snapshot version; unchanged when no Snapshot exists.
     * @param[out] canonicalTopology Canonical topology bytes; unchanged when no Snapshot exists.
     * @return K_OK on success or K_NOT_FOUND before the first legal Snapshot.
     */
    Status GetRecoveryTopology(uint64_t &topologyVersion, std::string &canonicalTopology) const;

    /**
     * @brief Publish identity-bound evidence after one successful exact read.
     * @param[in] snapshot Exact-read authoritative Snapshot used to derive the evidence.
     * @return K_OK on success; an identity or Snapshot validation error otherwise.
     */
    Status PublishBackendEvidence(const TopologySnapshot &snapshot);

    void LogAndNotifyPublishedSnapshot(std::shared_ptr<const TopologySnapshot> published);

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
     * @brief Invoke the non-blocking Snapshot publication hook.
     * @param[in] snapshot Newly published immutable Snapshot.
     */
    void NotifySnapshotPublished(std::shared_ptr<const TopologySnapshot> snapshot);

    /**
     * @brief Record a runtime error without exposing high-cardinality payload.
     * @param[in] status Runtime error to retain and log.
     */
    void RecordError(const Status &status);

    RuntimeOptions options_;
    std::unique_ptr<EtcdStore> controllerEtcdStore_;
    std::unique_ptr<ICoordinationBackend> memberBackend_;
    std::unique_ptr<ICoordinationBackend> controllerBackend_;
    std::unique_ptr<HashAlgorithm> algorithm_;
    ICoordinatorServiceProxy *coordinatorProxy_{ nullptr };  // Non-owning; outlives the Engine.
    CoordinatorWatchIngress coordinatorIngress_;
    std::unique_ptr<TopologyKeyHelper> keys_;
    TopologyRepository repository_;
    TopologyReader reader_;
    TopologySnapshotState snapshots_;
    CoordinationEventDispatcher dispatcher_;
    MembershipEndpointView membershipView_;
    PlacementFacade placement_;
    TopologyTaskExecutor executor_;
    std::unique_ptr<TopologyControllerRuntime> controllerRuntime_;
    std::unique_ptr<TopologyRecoveryReporter> recoveryReporter_;
    // Protects lifecycle transactions, startAttempted_, ingressBound_, and thread join sequencing.
    // Backend IO, callbacks, drain, component Start/Stop, and thread join execute without this lock.
    std::mutex lifecycleMutex_;
    // Protects threadExited_, isolationReason_, lastError_, and backendObservation_.
    mutable std::mutex stateMutex_;
    // Serializes availability_, publishedAvailability_, isolationReason_, and Host admission callback transitions.
    // The successful-Start commit may acquire lifecycleMutex_ while holding this mutex; lifecycle paths never acquire
    // this mutex while holding lifecycleMutex_.
    std::mutex availabilityTransitionMutex_;
    // Uses stateMutex_ and signals changes to threadExited_.
    std::condition_variable stoppedCv_;
    Thread stateThread_;
    bool startAttempted_{ false };
    bool ingressBound_{ false };
    bool lifecycleOperationInFlight_{ false };
    bool threadExited_{ true };
    std::atomic<TopologyEngineState> state_{ TopologyEngineState::STOPPED };
    // Latest internal candidate; traffic-allowing candidates remain unpublished until Start commits RUNNING.
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
