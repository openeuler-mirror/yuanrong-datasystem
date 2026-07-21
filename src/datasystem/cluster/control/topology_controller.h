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
#include "datasystem/common/util/thread.h"

namespace datasystem::cluster {

enum class TopologyEventSourceMode : uint8_t { SELF_MANAGED, EXTERNAL };

/**
 * @brief Fixed Controller budgets and bounded work limits.
 */
struct TopologyControllerOptions {
    std::chrono::seconds nodeDeadTimeout{ 300 };
    std::chrono::seconds failureBatchWindow{ 30 };
    std::chrono::minutes ordinaryBatchWindow{ 3 };
    std::chrono::milliseconds reconcileTick{ 1'000 };
    size_t maxMembersPerBatch{ 2'500 };
    size_t maxDerivedOperationsPerTick{ 256 };
    size_t maxProgressReadsPerTick{ 256 };
    TopologyEventSourceMode eventSourceMode{ TopologyEventSourceMode::SELF_MANAGED };

    /**
     * @brief Host hook for a newly observed RESTARTING member process.
     */
    std::function<Status(const std::string &, int64_t)> membershipRestartHandler;

    /**
     * @brief Host hook for a newly observed RECOVERING member process.
     */
    std::function<Status(const std::string &, int64_t)> membershipRecoveryHandler;

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
                       const TopologyControllerOptions &options);

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
     * @brief Return a no-IO diagnostic snapshot.
     * @return Current diagnostics.
     */
    TopologyControllerDiagnostics GetDiagnostics() const;

private:
    /**
     * @brief Enqueue a watch doorbell.
     * @param[in] event Backend event.
     * @return Queue status.
     */
    Status EnqueueCoordinationEvent(CoordinationEvent &&event);

    /**
     * @brief Resolve the membership event prefix and reset restart de-duplication.
     * @return K_OK when restart observation is disabled or initialized; backend or key validation status otherwise.
     */
    Status PrepareMembershipRestartObservation();

    /**
     * @brief Preserve a RESTARTING generation before generic doorbell coalescing.
     * @param[in] event Backend event to inspect without consuming it.
     * @return K_OK when ignored or recorded; key or membership value validation status otherwise.
     */
    Status ObserveMembershipRestart(const CoordinationEvent &event);

    /**
     * @brief Preserve restart generations recovered by a full membership read.
     * @param[in] memberships Successful authoritative membership read.
     */
    void ObserveMembershipRestarts(const std::vector<MembershipRecord> &memberships);

    /**
     * @brief Preserve recovery generations recovered by a full membership read.
     * @param[in] memberships Successful authoritative membership read.
     */
    void ObserveMembershipRecoveries(const std::vector<MembershipRecord> &memberships);

    /**
     * @brief De-duplicate one restarting address generation.
     * @param[in] address Canonical member address.
     * @param[in] timestamp Membership process-generation timestamp.
     */
    void RecordMembershipRestart(const std::string &address, int64_t timestamp);

    /**
     * @brief De-duplicate one recovering address generation.
     * @param[in] address Canonical member address.
     * @param[in] timestamp Membership process-generation timestamp.
     */
    void RecordMembershipRecovery(const std::string &address, int64_t timestamp);

    /**
     * @brief Deliver preserved restart generations on the serial state thread.
     */
    void DrainMembershipRestarts();

    /**
     * @brief Deliver preserved recovery generations on the serial state thread.
     */
    void DrainMembershipRecoveries();

    /**
     * @brief Run the serial state loop.
     */
    void Run();

    /**
     * @brief Advance one bounded reconciliation tick.
     * @return Operation status.
     */
    Status ReconcileOnce();

    /**
     * @brief Join and repair the committed epoch.
     * @return Operation status.
     */
    Status RecoverFromLatestTopology();

    /**
     * @brief Create the empty pre-bootstrap authority record if absent.
     * @return Operation status.
     */
    Status EnsureTopologyAuthority();

    /**
     * @brief Repair expected task and notify keys.
     * @param[in] latest Snapshot.
     * @return Operation status.
     */
    Status ReconcileDerivedState(const TopologySnapshot &latest);

    /**
     * @brief Confirm failures from the tick-consistent membership snapshot.
     * @param[in] latest Snapshot.
     * @param[in] memberships Memberships read once for this reconciliation tick.
     * @return Operation status.
     */
    Status TryConfirmFailures(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    /**
     * @brief Commit the cluster-shutdown special path.
     * @param[in] latest Snapshot.
     * @return Operation status.
     */
    Status CommitClusterShutdown(const TopologySnapshot &latest);

    /**
     * @brief Commit one complete confirmed Failure set.
     * @param[in] latest Snapshot.
     * @param[in] classification Complete classification.
     * @return Operation status.
     */
    Status CommitConfirmedFailures(const TopologySnapshot &latest, const FailureClassification &classification);

    /**
     * @brief Commit immediate INITIAL/JOINING cleanup.
     * @param[in] latest Snapshot.
     * @param[in] classification Complete classification.
     * @return Operation status.
     */
    Status CommitUncommittedCleanup(const TopologySnapshot &latest, const FailureClassification &classification);

    Status CommitScaleOutUncommittedCleanup(const TopologySnapshot &latest,
                                            const FailureClassification &classification);

    Status CommitFailureUncommittedCleanup(const TopologySnapshot &latest,
                                           const FailureClassification &classification);

    Status CommitInitialCleanup(const TopologySnapshot &latest, const FailureClassification &classification);

    Status CommitAndLogMemberTransition(const TopologySnapshot &latest, const TopologyState &next,
                                        const std::vector<MemberIdentity> &members, const char *action);

    /**
     * @brief Persist READY/EXITING membership facts as INITIAL/PRE_LEAVING.
     * @param[in] latest Snapshot.
     * @param[in] memberships Successful membership read-back.
     * @return Operation status.
     */
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

    /**
     * @brief Finalize a completed or expired batch.
     * @param[in] latest Snapshot.
     * @param[in] memberships Memberships read once for this reconciliation tick.
     * @return Operation status.
     */
    Status TryFinalizeActiveBatch(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    /**
     * @brief Read current task progress and identify incomplete ScaleOut generations.
     * @param[in] latest Snapshot.
     * @param[in] expected Expected derived state.
     * @param[out] complete True when every expected task is finished.
     * @param[out] failedJoining Incomplete ScaleOut member generations.
     * @return Operation status.
     */
    Status InspectBatchProgress(const TopologySnapshot &latest, const ExpectedDerivedState &expected, bool &complete,
                                std::vector<MemberIdentity> &failedJoining);

    /**
     * @brief Refresh a bounded slice of the state-thread-owned task progress cache.
     * @param[in] batch Active batch fence.
     * @param[in] expected Expected task identities.
     * @return Operation status.
     */
    Status RefreshTaskProgressCache(const ActiveBatch &batch, const ExpectedDerivedState &expected);

    /**
     * @brief Commit the normal or Failure final state.
     * @param[in] latest Snapshot.
     * @return Operation status.
     */
    Status CommitBatchFinal(const TopologySnapshot &latest);

    std::vector<MemberIdentity> CollectBatchParticipants(const TopologySnapshot &latest,
                                                         const ActiveBatch &batch) const;

    void LogBatchFinalized(const TopologySnapshot &latest, const ActiveBatch &batch,
                           const std::vector<MemberIdentity> &participants,
                           const std::shared_ptr<const TopologySnapshot> &committed) const;

    /**
     * @brief Remove incomplete ScaleOut generations and replan retained members.
     * @param[in] latest Snapshot.
     * @param[in] failedJoining Incomplete generations.
     * @param[in] memberships Memberships read once for this reconciliation tick.
     * @return Operation status.
     */
    Status CommitScaleOutExhaustion(const TopologySnapshot &latest, const std::vector<MemberIdentity> &failedJoining,
                                    const std::vector<MembershipRecord> &memberships);

    /**
     * @brief Apply the type-specific exhausted-batch terminal path.
     * @param[in] latest Snapshot.
     * @param[in] failedJoining Incomplete ScaleOut generations.
     * @param[in] memberships Memberships read once for this reconciliation tick.
     * @return Operation status.
     */
    Status CommitExpiredBatch(const TopologySnapshot &latest, const std::vector<MemberIdentity> &failedJoining,
                              const std::vector<MembershipRecord> &memberships);

    /**
     * @brief Admit the next stable ordinary batch.
     * @param[in] latest Snapshot.
     * @param[in] memberships Memberships read once for this reconciliation tick.
     * @return Operation status.
     */
    Status TryStartNextBatch(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships);

    void CollectNextBatchCandidates(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships,
                                    std::vector<MemberIdentity> &leaving,
                                    std::vector<MemberIdentity> &joining) const;

    Status CommitBootstrapBatchStart(const TopologySnapshot &latest, const TopologyState &state,
                                     const std::vector<MemberIdentity> &joining);

    Status CommitOrdinaryBatchStart(const TopologySnapshot &latest, const TopologyState &state,
                                    const std::vector<MemberIdentity> &participants, TopologyChangeType type);

    Status CommitStartedBatch(const TopologySnapshot &latest, const TopologyState &next,
                              const std::vector<MemberIdentity> &participants);

    void LogBatchStart(const TopologySnapshot &latest, const TopologySnapshot &committed,
                       const std::vector<MemberIdentity> &participants) const;

    /**
     * @brief CAS and exact-read the committed state.
     * @param[in] expectedVersion Expected version.
     * @param[in] desired Candidate.
     * @param[out] committed Read-back snapshot.
     * @return Operation status.
     */
    Status CommitAndReadBack(uint64_t expectedVersion, const TopologyState &desired,
                             std::shared_ptr<const TopologySnapshot> &committed);

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
    bool topologyDirty_{ true };
    bool membershipDirty_{ true };
    bool taskDirty_{ true };
    bool topologyCommittedThisTick_{ false };
    uint32_t consecutiveReconcileFailures_{ 0 };
    std::chrono::steady_clock::time_point reconcileNotBefore_{};
    std::optional<std::chrono::steady_clock::time_point> batchDeadline_;
    std::optional<TopologyChangeType> deadlineBatchType_;
    uint64_t deadlineBatchEpoch_{ 0 };
    uint64_t loggedExpiredBatchEpoch_{ 0 };
    uint64_t loggedScaleInWaitEpoch_{ 0 };
    size_t admissionCursor_{ 0 };
    uint64_t derivedBatchEpoch_{ 0 };
    size_t progressReadCursor_{ 0 };
    uint64_t progressBatchEpoch_{ 0 };
    // State-thread-owned monotonic task progress cache for progress reads bounded across reconciliation ticks.
    std::unordered_set<std::string> finishedTaskIds_;
    std::string membershipEventPrefix_;
    // Protects the membership event prefix and lifecycle generation queues.
    std::mutex membershipRestartMutex_;
    std::unordered_map<std::string, int64_t> latestRestartTimestampByAddress_;
    std::unordered_map<std::string, int64_t> pendingRestartTimestampByAddress_;
    std::unordered_map<std::string, int64_t> latestRecoveryTimestampByAddress_;
    std::unordered_map<std::string, int64_t> pendingRecoveryTimestampByAddress_;
    // State-thread-owned admission quarantine for exhausted READY process generations.
    std::unordered_map<std::string, int64_t> quarantinedReadyTimestampByAddress_;
    std::string lastMembershipObservationDigest_;
    TopologyControllerDiagnostics diagnostics_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_CONTROL_TOPOLOGY_CONTROLLER_H
