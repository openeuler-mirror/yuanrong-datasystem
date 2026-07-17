/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Coordinator-backed Worker topology recovery reporter.
 */
#ifndef DATASYSTEM_CLUSTER_COORDINATION_BACKEND_TOPOLOGY_RECOVERY_REPORTER_H
#define DATASYSTEM_CLUSTER_COORDINATION_BACKEND_TOPOLOGY_RECOVERY_REPORTER_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem::cluster {

/**
 * @brief Bounded RPC, jitter and retry policy for one recovery reporter.
 */
struct TopologyRecoveryReporterOptions {
    /** Maximum duration of one report RPC. */
    std::chrono::milliseconds reportTimeout{ 500 };
    /** Initial retry delay for transient Coordinator failures. */
    std::chrono::milliseconds minRetryBackoff{ 100 };
    /** Maximum retry delay for transient Coordinator failures. */
    std::chrono::milliseconds maxRetryBackoff{ 5'000 };
    /** Maximum deterministic per-Worker startup jitter. */
    std::chrono::milliseconds maxInitialJitter{ 1'000 };
};

/**
 * @brief Asynchronously report one Worker's last-good topology after Coordinator restart.
 */
class TopologyRecoveryReporter final {
public:
    using SnapshotProvider = std::function<Status(uint64_t &, std::string &)>;

    /**
     * @brief Construct one Worker-scoped asynchronous recovery reporter.
     * @param[in] proxy Shared Coordinator proxy that outlives this reporter.
     * @param[in] clusterName Cluster scope; empty is valid.
     * @param[in] reporterAddress Canonical local Worker address.
     * @param[in] snapshotProvider PB-free last-good topology provider.
     * @param[in] options Report timeout and retry bounds.
     */
    TopologyRecoveryReporter(ICoordinatorServiceProxy &proxy, std::string clusterName,
                             std::string reporterAddress, SnapshotProvider snapshotProvider,
                             TopologyRecoveryReporterOptions options = {});

    /**
     * @brief Invoke idempotent Shutdown before releasing the reporter.
     */
    ~TopologyRecoveryReporter();

    /**
     * @brief Disable copying the reporter's thread and lifecycle state.
     */
    TopologyRecoveryReporter(const TopologyRecoveryReporter &) = delete;

    /**
     * @brief Disable copy assignment of the reporter's thread and lifecycle state.
     */
    TopologyRecoveryReporter &operator=(const TopologyRecoveryReporter &) = delete;

    /**
     * @brief Mark membership committed for the exact response CoordinatorId.
     * @param[in] coordinatorId Exact membership operation response CoordinatorId.
     */
    void NotifyMembershipReady(const std::string &coordinatorId);

    /**
     * @brief Mark the local TopologyEngine ready for candidate export.
     */
    void NotifyRuntimeReady();

    /**
     * @brief Stop scheduling and cooperatively drain report work.
     * @return K_OK after idempotent shutdown.
     */
    Status Shutdown();

private:
    /**
     * @brief Check all lifecycle gates while mutex_ is held.
     * @return True when a report round may start.
     */
    bool CanReportLocked() const;

    /**
     * @brief Schedule at most one report round.
     */
    void ScheduleReport();

    /**
     * @brief Export and hash one immutable last-good candidate.
     * @param[out] version Exported topology version.
     * @param[out] canonical Canonical topology bytes.
     * @param[out] digest SHA-256 digest of canonical bytes.
     * @param[out] hasSnapshot True when an exportable snapshot exists.
     * @return Snapshot provider or digest status.
     */
    Status LoadCandidate(uint64_t &version, std::string &canonical, std::string &digest, bool &hasSnapshot) const;

    /**
     * @brief Send one evidence or payload report.
     * @param[in] coordinatorId Target Coordinator process lifetime.
     * @param[in] version Candidate topology version.
     * @param[in] canonical Canonical topology bytes.
     * @param[in] digest Candidate digest.
     * @param[in] hasSnapshot True when the Worker owns a snapshot.
     * @param[in] includePayload True to include canonical bytes.
     * @param[out] response Coordinator recovery decision.
     * @return RPC or response status.
     */
    Status SendCandidate(const std::string &coordinatorId, uint64_t version, const std::string &canonical,
                         const std::string &digest, bool hasSnapshot, bool includePayload,
                         coordinator::ReportTopologyRecoveryCandidateRspPb &response);

    /**
     * @brief Run one CoordinatorId-bound report round.
     * @param[out] coordinatorId Identity used by this round.
     * @param[out] completed True when the Coordinator reached READY.
     */
    void RunReportLoop(std::string &coordinatorId, bool &completed);

    /**
     * @brief Interruptibly wait for a retry deadline.
     * @param[in] coordinatorId Identity that owns the current round.
     * @param[in] delay Bounded retry delay.
     * @return False after shutdown or identity change.
     */
    bool WaitRetry(const std::string &coordinatorId, std::chrono::milliseconds delay);

    /**
     * @brief Commit or discard one completed CoordinatorId round.
     * @param[in] coordinatorId Identity used by the completed round.
     * @param[in] completed True when Coordinator reported READY.
     */
    void FinishRound(const std::string &coordinatorId, bool completed);

    ICoordinatorServiceProxy &proxy_;
    const std::string clusterName_;
    const std::string reporterAddress_;
    const SnapshotProvider snapshotProvider_;
    const TopologyRecoveryReporterOptions options_;
    std::unique_ptr<ThreadPool> reportPool_;
    // Protects lifecycle, readiness, scheduling and CoordinatorId-bound report state.
    std::mutex mutex_;
    // Uses mutex_ to interrupt jitter/backoff during identity change or shutdown.
    std::condition_variable retryCv_;
    bool stopping_{ false };
    bool runtimeReady_{ false };
    bool scheduled_{ false };
    // Exact CoordinatorId returned by the successful membership operation that opens the report gate.
    std::string membershipCoordinatorId_;
    std::string completedCoordinatorId_;
    std::string jitteredCoordinatorId_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_COORDINATION_BACKEND_TOPOLOGY_RECOVERY_REPORTER_H
