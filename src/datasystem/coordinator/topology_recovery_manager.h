/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
#ifndef DATASYSTEM_COORDINATOR_TOPOLOGY_RECOVERY_MANAGER_H
#define DATASYSTEM_COORDINATOR_TOPOLOGY_RECOVERY_MANAGER_H

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/utils/status.h"

namespace datasystem {
class CoordinatorStore;
class SteadyClock;
class ThreadPool;

namespace coordinator {

enum class TopologyRecoveryState : uint8_t { RECOVERING, INSTALLING, READY, BLOCKED };
enum class TopologyRecoveryReportResult : uint8_t { ACCEPTED, COORDINATOR_ID_MISMATCH, MEMBERSHIP_NOT_READY };
enum class TopologyCoordinationKeyKind : uint8_t { OTHER, TOPOLOGY, MIGRATE_TASK, DELETE_TASK, NOTIFY, MEMBERSHIP };

struct ParsedTopologyCoordinationKey {
    std::string clusterName;
    TopologyCoordinationKeyKind kind{ TopologyCoordinationKeyKind::OTHER };
    std::string relativeKey;
};

struct TopologyRecoveryCandidateReport {
    std::string reporterAddress;
    bool hasSnapshot{ false };
    uint64_t topologyVersion{ 0 };
    std::string canonicalDigest;
    std::string canonicalTopology;
};

struct TopologyRecoveryReportDecision {
    TopologyRecoveryState state{ TopologyRecoveryState::RECOVERING };
    TopologyRecoveryReportResult result{ TopologyRecoveryReportResult::ACCEPTED };
    bool payloadRequired{ false };
};

constexpr size_t DEFAULT_MAX_CANDIDATE_MEMORY_BYTES = 64 * 1'024 * 1'024;
constexpr size_t MAX_TOPOLOGY_RECOVERY_PAYLOAD_BYTES = 4 * 1'024 * 1'024;
struct TopologyRecoveryOptions {
    std::chrono::milliseconds discoveryWindow{ 10'000 };
    std::chrono::milliseconds validationWaitTimeout{ 500 };
    size_t minRecoveryThreads{ 4 };
    size_t maxRecoveryThreads{ 16 };
    size_t maxPendingRecoveryWork{ 10'000 };
    size_t maxClusters{ 1'024 };
    size_t maxMembersPerCluster{ 10'000 };
    size_t maxCandidateMemoryBytes{ DEFAULT_MAX_CANDIDATE_MEMORY_BYTES };
};

class TopologyRecoveryManager final {
public:
    /**
     * @brief Construct a recovery manager for one CoordinatorId.
     * @param[in] coordinatorId Immutable current CoordinatorId.
     * @param[in] store Store that outlives this manager.
     * @param[in] clock Monotonic clock used for deadlines.
     * @param[in] options Resource and deadline bounds.
     */
    TopologyRecoveryManager(std::string coordinatorId, CoordinatorStore &store,
                            std::shared_ptr<SteadyClock> clock, TopologyRecoveryOptions options = {});

    /**
     * @brief Stop and drain started recovery work.
     */
    ~TopologyRecoveryManager();

    TopologyRecoveryManager(const TopologyRecoveryManager &) = delete;
    TopologyRecoveryManager &operator=(const TopologyRecoveryManager &) = delete;

    /**
     * @brief Check one read against its cluster recovery gate.
     * @param[in] key Physical key.
     * @param[in] rangeEnd Physical range end; empty for exact read.
     * @return Gate or validation status.
     */
    Status CheckReadAllowed(const std::string &key, const std::string &rangeEnd);

    /**
     * @brief Check one mutation against its cluster recovery gate.
     * @param[in] key Physical key.
     * @param[in] rangeEnd Physical range end; empty for exact mutation.
     * @return Gate or validation status.
     */
    Status CheckMutationAllowed(const std::string &key, const std::string &rangeEnd);

    /**
     * @brief Validate one watch range without applying the recovery-state gate.
     * @param[in] key Physical watch start key.
     * @param[in] rangeEnd Physical watch range end; empty for an exact watch.
     * @return K_OK for one legal keyspace or K_INVALID for a crossing range.
     */
    Status ValidateWatchRange(const std::string &key, const std::string &rangeEnd) const;

    /**
     * @brief Parse one physical topology-coordination key.
     * @param[in] physicalKey Physical Coordinator key.
     * @param[out] parsed Parsed cluster, kind and relative key.
     * @return Validation status.
     */
    Status ParseKey(const std::string &physicalKey, ParsedTopologyCoordinationKey &parsed) const;

    /**
     * @brief Observe one committed membership presence change.
     * @param[in] physicalKey Exact membership key.
     * @param[in] present True for Put and false for Delete.
     */
    void ObserveMembershipChange(const std::string &physicalKey, bool present);

    /**
     * @brief Re-admit a committed member and schedule reconciliation.
     * @param[in] physicalKey Existing membership key.
     */
    void NotifyMembershipActivity(const std::string &physicalKey);

    /**
     * @brief Validate and record Worker-initiated evidence or payload.
     * @param[in] clusterName Cluster scope; empty is valid.
     * @param[in] requestCoordinatorId CoordinatorId seen by the reporter.
     * @param[in] report Candidate report to consume.
     * @param[out] decision Control result and payload request.
     * @return Validation, admission or shutdown status.
     */
    Status ReportCandidate(const std::string &clusterName, const std::string &requestCoordinatorId,
                           TopologyRecoveryCandidateReport report, TopologyRecoveryReportDecision &decision);

    /**
     * @brief Read one cluster state without creating a context.
     * @param[in] clusterName Cluster scope.
     * @return Current state or RECOVERING when unseen.
     */
    TopologyRecoveryState GetState(const std::string &clusterName) const;

    /**
     * @brief Stop new work and drain started work idempotently.
     * @return K_OK after shutdown.
     */
    Status Shutdown();

private:
    struct ClusterRecoveryContext;

    /**
     * @brief Validate one operation and apply its recovery gate.
     * @param[in] key Physical key.
     * @param[in] rangeEnd Physical range end.
     * @param[in] mutation True for a mutation.
     * @return Gate or validation status.
     */
    Status CheckAllowed(const std::string &key, const std::string &rangeEnd, bool mutation);

    /**
     * @brief Find or create one bounded context while mutex_ is held.
     * @param[in] clusterName Cluster scope.
     * @param[out] context Borrowed context protected by mutex_.
     * @return Admission status.
     */
    Status EnsureContext(const std::string &clusterName, ClusterRecoveryContext *&context);

    /**
     * @brief Add, refresh or remove one parsed member.
     * @param[in] parsed Parsed membership key.
     * @param[in] present True to admit or refresh the member.
     */
    void UpdateMembership(const ParsedTopologyCoordinationKey &parsed, bool present);

    /**
     * @brief Validate lightweight candidate evidence.
     * @param[in] clusterName Cluster scope.
     * @param[in] report Candidate evidence.
     * @return Validation status.
     */
    Status ValidateEvidence(const std::string &clusterName, const TopologyRecoveryCandidateReport &report) const;

    /**
     * @brief Decode, canonicalize and hash one payload outside manager locks.
     * @param[in] report Candidate payload.
     * @return Validation status.
     */
    Status ValidatePayload(const TopologyRecoveryCandidateReport &report) const;

    /**
     * @brief Record one payload-free evidence report.
     * @param[in] clusterName Cluster scope.
     * @param[in] report Candidate evidence to consume.
     * @param[out] decision Recovery decision.
     * @return Recording status.
     */
    Status RecordEvidence(const std::string &clusterName, TopologyRecoveryCandidateReport report,
                          TopologyRecoveryReportDecision &decision);

    /**
     * @brief Update one cluster's evidence while mutex_ is held.
     * @param[in,out] context Cluster recovery context.
     * @param[in] report Validated evidence-only report.
     * @param[out] decision Response decision.
     * @param[out] schedule Whether reconciliation should be scheduled.
     * @return Evidence admission status.
     */
    Status UpdateEvidenceLocked(ClusterRecoveryContext &context, TopologyRecoveryCandidateReport report,
                                TopologyRecoveryReportDecision &decision, bool &schedule);

    /**
     * @brief Reserve budgets and validate a requested payload asynchronously.
     * @param[in] clusterName Cluster scope.
     * @param[in] report Candidate payload to consume.
     * @param[out] decision Recovery decision.
     * @return Submission or validation status.
     */
    Status SubmitPayload(const std::string &clusterName, TopologyRecoveryCandidateReport report,
                         TopologyRecoveryReportDecision &decision);

    /**
     * @brief Commit a validated payload while its evidence remains selected.
     * @param[in] clusterName Cluster scope.
     * @param[in] report Validated payload to consume.
     * @return Recording status.
     */
    Status RecordPayload(const std::string &clusterName, TopologyRecoveryCandidateReport report);

    /**
     * @brief Release admission budget and block an invalid selected candidate.
     * @param[in] clusterName Cluster scope.
     * @param[in] report Rejected candidate payload.
     * @param[in] payloadBytes Reserved payload bytes.
     * @param[in] validationStatus Payload validation result.
     * @return Original validation status.
     */
    Status RejectPayload(const std::string &clusterName, const TopologyRecoveryCandidateReport &report,
                         size_t payloadBytes, const Status &validationStatus);

    /**
     * @brief Drop a retained payload that is no longer selected.
     * @param[in,out] context Context protected by mutex_.
     */
    void RefreshSelection(ClusterRecoveryContext &context);

    /**
     * @brief Submit one per-cluster deduplicated arbitration attempt.
     * @param[in] clusterName Cluster scope.
     */
    void ScheduleReconcile(const std::string &clusterName);

    /**
     * @brief Advance one cluster after its discovery deadline.
     * @param[in] clusterName Cluster scope.
     * @return Arbitration or Store status.
     */
    Status MaybeFinalize(const std::string &clusterName);

    /**
     * @brief Freeze an eligible candidate for installation while mutex_ is held.
     * @param[in] clusterName Logical cluster name.
     * @param[out] payload Frozen canonical topology payload.
     * @param[out] version Frozen topology version; zero means no installation is ready.
     * @return Arbitration status.
     */
    Status PrepareInstallationLocked(const std::string &clusterName,
                                     std::shared_ptr<const std::string> &payload, uint64_t &version);

    /**
     * @brief Apply an installation result while mutex_ is held.
     * @param[in] clusterName Logical cluster name.
     * @param[in] version Installed topology version.
     * @param[in] installStatus Store installation result.
     */
    void CompleteInstallationLocked(const std::string &clusterName, uint64_t version,
                                    const Status &installStatus);

    /**
     * @brief Create the selected topology only when its Store key is absent.
     * @param[in] clusterName Cluster scope.
     * @param[in] version Selected topology version.
     * @param[in] canonicalTopology Canonical repository bytes.
     * @return Store create-if-absent status.
     */
    Status InstallSelected(const std::string &clusterName, uint64_t version, const std::string &canonicalTopology);

    /**
     * @brief Release one retained payload and its memory budget.
     * @param[in,out] context Context protected by mutex_.
     */
    void ReleaseSelectedPayload(ClusterRecoveryContext &context);

    const std::string coordinatorId_;
    CoordinatorStore &store_;
    std::shared_ptr<SteadyClock> clock_;
    TopologyRecoveryOptions options_;
    std::unique_ptr<ThreadPool> recoveryPool_;
    // Protects lifecycle flags, work/byte counters and contexts_. No codec or Store operation is allowed while held.
    mutable std::mutex mutex_;
    std::condition_variable shutdownCv_;
    bool stopping_{ false };
    bool shutdownComplete_{ false };
    size_t pendingRecoveryWork_{ 0 };
    size_t retainedCandidateBytes_{ 0 };
    size_t admittedReportBytes_{ 0 };
    std::unordered_map<std::string, std::unique_ptr<ClusterRecoveryContext>> contexts_;
};

}  // namespace coordinator
}  // namespace datasystem

#endif  // DATASYSTEM_COORDINATOR_TOPOLOGY_RECOVERY_MANAGER_H
