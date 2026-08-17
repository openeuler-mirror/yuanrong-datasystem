/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Multi-instance CAS cluster topology Controller.
 */
#ifndef DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_H
#define DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/cluster/control/topology_failure_classifier.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/cluster/runtime/control_backend_state.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/cluster/runtime/worker_liveness.h"
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {

inline constexpr int64_t MAX_SCALE_IN_COLLECT_WINDOW_MS = 5'000;
inline constexpr int64_t MAX_SCALE_OUT_COLLECT_WINDOW_MS = 5'000;
inline constexpr int64_t DEFAULT_ORDINARY_COLLECT_WINDOW_MS = 3'000;

/**
 * @brief Select watch ownership and external-event authority.
 */
enum class TopologyEventSourceMode : uint8_t {
    SELF_MANAGED,  // Controller owns the watch and exact-reads facts.
    EXTERNAL,      // Another owner sends doorbells; Controller exact-reads facts.
    EXTERNAL_ETCD  // Worker forwards revisioned ETCD values; Controller compensates with exact reads.
};

/**
 * @brief Fixed Controller budgets and bounded work limits.
 */
struct TopologyControllerOptions {
    /**
     * Extra continuous absence after membership key deletion before confirmed failure.
     * Zero confirms on the first successful observation (lease TTL already gated liveness).
     */
    std::chrono::seconds nodeDeadTimeout{ 300 };
    std::chrono::seconds failureBatchWindow{ 30 };
    std::chrono::minutes ordinaryBatchWindow{ 3 };
    std::chrono::milliseconds reconcileTick{ 1'000 };
    std::chrono::milliseconds scaleOutCollectWindow{ DEFAULT_ORDINARY_COLLECT_WINDOW_MS };
    std::chrono::milliseconds scaleInCollectWindow{ DEFAULT_ORDINARY_COLLECT_WINDOW_MS };
    // Absolute budget for one memberLivenessProbe call, including all targets and cleanup.
    std::chrono::seconds failureProbeTimeout{ 2 };
    std::chrono::seconds witnessProbeRoundTimeout{ 10 };
    size_t failureProbeWitnessCount{ 3 };
    // Existing Worker identity used to deterministically assign one cluster-wide probe owner per missing member.
    std::string localAddress;
    std::string probeEpoch;
    // First round ID reserved for this Controller runtime; Coordinator runtimes use disjoint generation ranges.
    uint64_t initialProbeRound{ 1 };
    size_t maxMembersPerBatch{ 2'500 };
    size_t maxDerivedOperationsPerTick{ 512 };
    size_t maxProgressReadsPerTick{ 512 };
    std::chrono::milliseconds derivedSliceBudget{ 5 };
    TopologyEventSourceMode eventSourceMode{ TopologyEventSourceMode::SELF_MANAGED };
    bool materializeRestartFacts{ false };

    /**
     * @brief Host hook for a newly observed RESTARTING member process.
     */
    std::function<Status(const std::string &, int64_t)> membershipRestartHandler;

    /**
     * @brief Promote this process's post-admission RECOVERING membership back to READY.
     */
    std::function<Status()> localMembershipRecoveryHandler;

    /**
     * @brief Directly probe all supplied members before the absolute deadline.
     * @return One structured result per target, including an optional direct observation, completion reason and elapsed
     *         time. Incomplete evidence still proves transport reachability; an exception aborts the current reconcile
     *         tick without changing topology and is retried safely by a later tick.
     */
    std::function<std::vector<ControlBackendProbeResult>(const std::vector<MemberIdentity> &,
                                                         std::chrono::steady_clock::time_point)>
        memberLivenessProbe;

    /**
     * @brief Return the current authority epoch for collective stale-topology recovery.
     *
     * An empty callback preserves the existing self-managed/ETCD behavior. A callback returning std::nullopt means
     * this Controller currently has no authority to probe or replace a stale topology.
     */
    std::function<std::optional<uint64_t>()> collectiveControlEpoch;

    /**
     * @brief Execute the final collective replacement commit only while the expected authority epoch remains current.
     */
    std::function<Status(uint64_t, const std::function<Status()> &)> collectiveReplacementFence;

    /**
     * @brief Host-supplied active failure candidates from multi-worker failure summaries.
     */
    std::function<std::vector<MemberIdentity>(const TopologySnapshot &, const std::vector<MembershipRecord> &,
                                              std::chrono::steady_clock::time_point)>
        failureSummaryCandidateProvider;

    /**
     * @brief Revalidate active failure candidates and run their topology mutation atomically.
     */
    std::function<Status(const TopologySnapshot &, const std::vector<MembershipRecord> &,
                         std::chrono::steady_clock::time_point, std::optional<uint64_t>,
                         const std::vector<MemberIdentity> &, const std::function<Status(int64_t)> &)>
        activeFailureCommitFence;

    /**
     * @brief Semantic policy clock; production uses steady time and tests may inject virtual time.
     */
    std::function<std::chrono::steady_clock::time_point()> now{ [] { return std::chrono::steady_clock::now(); } };

    /**
     * @brief Validate bounded Controller budgets before constructing a one-shot Runtime.
     * @return True when every required duration, work limit and clock is usable.
     */
    bool IsValid() const noexcept;
};

/**
 * @brief Low-cardinality, no-IO Controller diagnostic snapshot.
 */
struct TopologyControllerDiagnostics {
    bool running{ false };
    bool controlFrozen{ false };
    ControlBackendState backendState{ ControlBackendState::UNKNOWN };
    uint64_t topologyVersion{ 0 };
    int64_t topologyRevision{ 0 };
    std::optional<ActiveBatch> activeBatch;
    size_t queuedEvents{ 0 };
    size_t dirtyDerivedOperations{ 0 };
    std::string lastError;
};

class CoordinationEventDispatcher;

/**
 * @brief One independently owned topology control loop; instances cooperate through CAS.
 */
class TopologyController final {
public:
    /**
     * @brief Wire one Controller.
     * @param[in] backend Role backend.
     * @param[in] repository Repository.
     * @param[in] keys Key helper.
     * @param[in] algorithm Planning algorithm.
     * @param[in] dispatcher Role dispatcher.
     * @param[in] options Bounded options.
     */
    TopologyController(ICoordinationBackend &backend, TopologyRepository &repository, const TopologyKeyHelper &keys,
                       const IPlanningAlgorithm &algorithm, CoordinationEventDispatcher &dispatcher,
                       TopologyControllerOptions options);

    /**
     * @brief Stop and join the state thread before releasing Controller state.
     */
    ~TopologyController();

    /**
     * @brief Disable copying a state-thread owner.
     */
    TopologyController(const TopologyController &) = delete;

    /**
     * @brief Disable copy assignment of a state-thread owner.
     */
    TopologyController &operator=(const TopologyController &) = delete;

    /**
     * @brief Establish watches and start the state thread.
     * @return Operation status.
     */
    Status Start();

    /**
     * @brief Stop ingress and join by deadline.
     * @param[in] deadline Absolute deadline.
     * @return Operation status.
     */
    Status Stop(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Submit one externally owned watch doorbell.
     * @param[in] event Backend event.
     * @return Queue status.
     */
    Status SubmitCoordinationEvent(CoordinationEvent &&event);

    /**
     * @brief Enqueue one witness report for serial Controller processing.
     */
    Status SubmitWorkerLivenessReport(WorkerLivenessReport report);

    int64_t GetBootstrapRevision() const noexcept;  // Immutable revision for external watch registration.

    /**
     * @brief Return a no-IO diagnostic snapshot.
     * @return Current diagnostics.
     */
    TopologyControllerDiagnostics GetDiagnostics() const;

private:
    struct BatchDeadlineState {
        ActiveBatch batch;
        std::chrono::steady_clock::time_point deadline;
    };

    struct BatchCollectState {
        std::chrono::steady_clock::time_point deadline;
        bool started{ false };
    };

    Status EnqueueCoordinationEvent(CoordinationEvent &&event);

    Status PrepareMembershipRestartObservation();

    Status ObserveMembershipRestart(const CoordinationEvent &event);

    Status ResyncExternalFacts();
    Status ApplyExternalEvent(const CoordinationEvent &event);
    Status PublishExternalTopology(std::shared_ptr<const TopologySnapshot> candidate, bool fullRebuild);

    void ObserveMembershipRestarts(const std::vector<MembershipRecord> &memberships);

    void RecordMembershipRestart(const std::string &address, int64_t timestamp);

    void DrainMembershipRestarts();

    void Run();

    bool StopRequested() const;

    void ConsumeRuntimeEvent(const RuntimeEvent &event);

    Status WaitForReconcile(bool immediate, size_t &drained);

    void RecordReconcileResult(const Status &status, std::chrono::steady_clock::time_point now,
                               std::chrono::steady_clock::time_point startedAt, size_t drained);

    Status ReconcileOnce();

    Status RecoverFromLatestTopology();

    Status RestoreReadyAfterLocalRecovery(const TopologySnapshot &latest,
                                          const std::vector<MembershipRecord> &memberships);

    Status EnsureTopologyAuthority();

    Status ReconcileDerivedState(const TopologySnapshot &latest,
                                 const std::vector<MembershipRecord> &memberships);

    Status PrepareDerivedGeneration(const TopologySnapshot &latest,
                                    const std::vector<MembershipRecord> &memberships);

    Status ReconcileDerivedSlice();

    Status TryConfirmFailures(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    Status ProbeActiveFailureCandidates(const TopologySnapshot &latest, const std::vector<MemberIdentity> &targets,
                                        std::vector<MemberIdentity> &confirmed, std::optional<uint64_t> &controlEpoch);

    Status PrepareActiveFailureProbeRound(const std::vector<MemberIdentity> &targets,
                                          std::vector<MemberIdentity> &dueTargets,
                                          std::optional<uint64_t> &controlEpoch);

    void ResetActiveFailureProbeAuthorityState();
    Status ResolveActiveFailureControlEpoch(std::optional<uint64_t> &controlEpoch);

    void ApplyActiveFailureProbeResults(const TopologySnapshot &latest, const std::vector<MemberIdentity> &dueTargets,
                                        const std::vector<ControlBackendProbeResult> &results,
                                        std::vector<MemberIdentity> &confirmed, std::optional<uint64_t> &controlEpoch);

    Status PrepareCollectiveProbeContext(const TopologySnapshot &latest,
                                         const std::vector<MembershipRecord> &memberships,
                                         const std::vector<MemberIdentity> &samples, size_t &readyCount,
                                         std::optional<std::string> &owner, bool &hasControlAuthority);

    Status HandleCollectiveMembershipAbsence(const TopologySnapshot &latest,
                                             const std::vector<MembershipRecord> &memberships,
                                             const std::vector<MemberIdentity> &samples,
                                             const std::vector<MemberAbsenceObservation> &confirmedMissing);
    std::vector<MemberIdentity> SelectCollectiveProbeSamples(const TopologySnapshot &latest) const;
    Status ProbeCollectiveSample(const TopologySnapshot &latest, const std::vector<MemberIdentity> &samples,
                                 size_t membershipCount, size_t readyCount);
    Status BootstrapCollectiveReplacement(const TopologySnapshot &latest);
    void ResetCollectiveProbeProgress() noexcept;
    void SummarizeCollectiveReadyMemberships(const std::vector<MembershipRecord> &memberships,
                                             size_t &readyCount, std::optional<std::string> &owner) const;
    void LogCollectiveDecision(const TopologySnapshot &latest, size_t membershipCount, size_t readyCount,
                               const std::string &owner, size_t progress, size_t sampleCount, const char *action,
                               const char *decision, const char *reason, bool sampled,
                               const std::string &details = {}) const;
    Status ConfirmMissingMembersUnreachable(const TopologySnapshot &latest, FailureClassification &classification);

    struct SuspectProbeRound;

    Status RefreshWitnessProbes(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships,
                                const FailureClassification &classification);
    Status StartWitnessProbeRound(const TopologySnapshot &latest, const std::vector<std::string> &eligibleWitnesses,
                                  const MemberIdentity &target);
    Status PublishWitnessProbeEvent(const std::string &witness, const SuspectProbeRound &round);
    void ApplyWorkerLivenessReport(const WorkerLivenessReport &report);
    void ApplyWitnessFailureGate(FailureClassification &classification);

    Status CommitClusterShutdown(const TopologySnapshot &latest);

    Status CommitConfirmedFailures(const TopologySnapshot &latest, const FailureClassification &classification,
                                   int64_t expectedAuthorityRevision = 0);

    Status CommitUncommittedCleanup(const TopologySnapshot &latest, const FailureClassification &classification,
                                    int64_t expectedAuthorityRevision = 0);

    Status CommitAndLogMemberTransition(const TopologySnapshot &latest, const TopologyState &next,
                                        const std::vector<MemberIdentity> &members, const char *action,
                                        int64_t expectedAuthorityRevision = 0);

    Status CommitMembershipFacts(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    void CollectMembershipFacts(const std::vector<MembershipRecord> &memberships,
                                std::unordered_set<std::string> &exiting,
                                std::vector<MembershipRecord> &ready);

    void ApplyExitingMembershipFacts(TopologyState &next, const std::unordered_set<std::string> &exiting,
                                     std::unordered_set<std::string> &known,
                                     std::vector<MemberIdentity> &admittedLeaving, size_t &changed) const;

    Status ApplyReadyMembershipFacts(TopologyState &next, const std::vector<MembershipRecord> &ready,
                                     std::unordered_set<std::string> &known,
                                     std::vector<MemberIdentity> &admittedJoining, size_t &changed) const;

    void LogMembershipFactsCommit(uint64_t committedVersion, const std::vector<MemberIdentity> &admittedLeaving,
                                  const std::vector<MemberIdentity> &admittedJoining) const;

    Status TryFinalizeActiveBatch(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    Status InspectBatchProgress(const TopologySnapshot &latest, const ExpectedDerivedState &expected, bool &complete);

    void CollectFailedJoining(const TopologySnapshot &latest, const ExpectedDerivedState &expected,
                              std::vector<MemberIdentity> &failedJoining) const;

    Status RefreshTaskProgressCache(const ActiveBatch &batch, const ExpectedDerivedState &expected);

    /**
     * @brief Collect participants of the active topology batch for admission, quarantine and final cleanup.
     */
    std::vector<MemberIdentity> CollectBatchParticipants(const TopologySnapshot &latest,
                                                         TopologyChangeType type) const;

    void RememberQuarantinedReadyMembers(const std::vector<MemberIdentity> &participants,
                                         const std::vector<MembershipRecord> &memberships);

    Status CommitBatchFinal(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    Status CommitScaleOutExhaustion(const TopologySnapshot &latest, const std::vector<MemberIdentity> &failedJoining,
                                    const std::vector<MembershipRecord> &memberships);

    Status CommitExpiredBatch(const TopologySnapshot &latest, const std::vector<MemberIdentity> &failedJoining,
                              const std::vector<MembershipRecord> &memberships);

    Status TryStartNextBatch(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    Status TryStartBatchAfterCollection(const TopologySnapshot &latest, const TopologyState &state,
                                        const std::vector<MemberIdentity> &participants, TopologyChangeType type,
                                        bool bootstrap);

    void ClearBatchCollectState(TopologyChangeType type, const char *reason);

    void CollectNextBatchCandidates(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships,
                                    std::vector<MemberIdentity> &leaving,
                                    std::vector<MemberIdentity> &joining) const;

    Status CommitBatchStart(const TopologySnapshot &latest, const TopologyState &state,
                            const std::vector<MemberIdentity> &participants, TopologyChangeType type, bool bootstrap);

    void LogBatchStart(const TopologySnapshot &latest, const TopologySnapshot &committed,
                       const std::vector<MemberIdentity> &participants, const char *action) const;

    Status CommitAndReadBack(uint64_t expectedVersion, const TopologyState &desired,
                             std::shared_ptr<const TopologySnapshot> &committed,
                             int64_t expectedAuthorityRevision = 0);

    struct ProbeReport {
        WorkerLivenessResult result{ WorkerLivenessResult::UNKNOWN };
        std::chrono::steady_clock::time_point reportedAt;
    };

    struct SuspectProbeRound {
        MemberIdentity target;
        uint64_t probeRound{ 0 };
        std::unordered_set<std::string> witnesses;
        std::chrono::steady_clock::time_point startedAt;
        std::chrono::steady_clock::time_point deadline;
        std::unordered_map<std::string, ProbeReport> reports;
    };

    bool HasReachableWitness(const SuspectProbeRound &round) const;

    ICoordinationBackend &backend_;
    TopologyRepository &repository_;
    const TopologyKeyHelper &keys_;
    const IPlanningAlgorithm &algorithm_;
    TopologyControllerOptions options_;
    TopologyPlanBuilder planBuilder_;
    TopologyFailureClassifier failureClassifier_;
    TopologyTaskMaterializer materializer_;
    CoordinationEventDispatcher &dispatcher_;
    // Protects started_, stopping_, threadExited_, diagnostics_, and stateThread_ join coordination.
    mutable std::mutex stateMutex_;
    // Uses stateMutex_ and signals changes to threadExited_.
    std::condition_variable stoppedCv_;
    Thread stateThread_;
    bool started_{ false };
    bool stopping_{ false };
    bool threadExited_{ true };
    bool membershipDirty_{ true };
    bool topologyCommittedThisTick_{ false };
    uint32_t consecutiveReconcileFailures_{ 0 };
    std::chrono::steady_clock::time_point reconcileNotBefore_{};
    std::optional<BatchDeadlineState> batchDeadline_;
    uint64_t loggedExpiredBatchEpoch_{ 0 };
    uint64_t loggedScaleInWaitEpoch_{ 0 };
    // State-thread-owned, in-process and non-persistent collect deadline for ordinary SCALE_IN coalescing.
    // Not written to topology, not part of CAS, not shared across Controller instances or restarts.
    std::optional<BatchCollectState> scaleInCollect_;
    // State-thread-owned bounded quiet window that coalesces INITIAL members into one SCALE_OUT batch.
    std::optional<BatchCollectState> scaleOutCollect_;
    // The Controller state thread exclusively owns derived-generation and task-progress cursors/caches below.
    size_t admissionCursor_{ 0 };
    uint64_t derivedTopologyVersion_{ 0 };
    std::string derivedMembershipDigest_;
    ExpectedDerivedState expectedDerivedState_;
    bool derivedWorkPending_{ false };
    size_t progressReadCursor_{ 0 };
    size_t progressSweepRemaining_{ 0 };
    uint64_t progressTopologyVersion_{ 0 };
    uint64_t progressBatchEpoch_{ 0 };
    bool progressWorkPending_{ false };
    // State-thread-owned monotonic task progress cache for progress reads bounded across reconciliation ticks.
    std::unordered_set<std::string> finishedTaskIds_;
    // State-thread-owned, non-persistent stale-topology probe evidence.
    std::optional<uint64_t> collectiveProbeTopologyVersion_;
    std::optional<std::string> collectiveProbeOwner_;
    std::optional<uint64_t> collectiveProbeControlEpoch_;
    std::unordered_set<std::string> collectiveUnreachableSamples_;
    struct ActiveFailureProbeState {
        MemberIdentity target;
        std::optional<uint64_t> controlEpoch;
        std::chrono::steady_clock::time_point notBefore;
        uint32_t consecutiveUnreachable{ 0 };
    };
    std::unordered_map<std::string, ActiveFailureProbeState> activeFailureProbeStates_;
    size_t activeFailureProbeCursor_{ 0 };
    std::vector<MemberIdentity> activeFailureProbeCandidateSweep_;
    bool activeFailureProbeSweepInProgress_{ false };
    std::optional<std::chrono::steady_clock::time_point> activeFailureProbeWakeDeadline_;
    std::string membershipEventPrefix_;
    // Protects membershipEventPrefix_, latestRestartTimestampByAddress_, and pendingRestartTimestampByAddress_.
    std::mutex membershipRestartMutex_;
    std::unordered_map<std::string, int64_t> latestRestartTimestampByAddress_;
    std::unordered_map<std::string, int64_t> pendingRestartTimestampByAddress_;
    TopologySnapshotState externalTopology_;  // Synchronous bootstrap, then Controller state-thread-owned.
    std::map<std::string, MembershipRecord> externalMemberships_;
    std::unordered_map<std::string, int64_t> membershipEventRevisionByAddress_;
    int64_t bootstrapRevision_{ 0 };
    int64_t membershipRevisionFloor_{ 0 };
    int64_t topologyEventRevision_{ 0 };
    bool externalResyncRequired_{ true };
    bool externalEventSourceReady_{ false };
    // State-thread-owned admission quarantine for exhausted READY process generations.
    std::unordered_map<std::string, int64_t> quarantinedReadyTimestampByAddress_;
    std::unordered_map<std::string, SuspectProbeRound> suspectRoundsByTarget_;
    uint64_t nextProbeRound_{ 1 };
    std::string lastMembershipObservationDigest_;
    TopologyControllerDiagnostics diagnostics_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_H
