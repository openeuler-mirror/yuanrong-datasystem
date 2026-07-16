/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Coordinator-backed Worker topology recovery reporter.
 */
#include "datasystem/cluster/coordination_backend/topology_recovery_reporter.h"

#include <algorithm>
#include <exception>
#include <functional>
#include <limits>
#include <stdexcept>
#include <utility>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr size_t REPORT_POOL_SIZE = 1;
constexpr int RETRY_BACKOFF_MULTIPLIER = 2;
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;

/**
 * @brief Derive deterministic per-Worker jitter without shared random state.
 * @param[in] reporter Canonical reporter address used as the deterministic seed.
 * @param[in] options Reporter timing options containing the jitter bound.
 * @return Deterministic initial report delay.
 */
std::chrono::milliseconds InitialJitter(const std::string &reporter,
                                        const TopologyRecoveryReporterOptions &options)
{
    const auto bound = options.maxInitialJitter.count();
    if (bound <= 0) {
        return std::chrono::milliseconds::zero();
    }
    return std::chrono::milliseconds(
        static_cast<int64_t>(std::hash<std::string>{}(reporter) % static_cast<uint64_t>(bound)));
}

/**
 * @brief Classify report statuses that remain safe to retry within one CoordinatorId.
 * @param[in] status Report operation status to classify.
 * @return True when the report may be retried for the same CoordinatorId.
 */
bool IsRetryableReportStatus(const Status &status)
{
    return status.GetCode() == K_TRY_AGAIN || status.GetCode() == K_NOT_READY
           || status.GetCode() == K_RPC_UNAVAILABLE || status.GetCode() == K_RPC_DEADLINE_EXCEEDED
           || status.GetCode() == K_RPC_CANCELLED;
}

/**
 * @brief Emit one low-volume transient report failure.
 * @param[in] cluster Cluster scope of the report.
 * @param[in] reporter Canonical reporter address.
 * @param[in] version Reported topology version.
 * @param[in] coordinatorId Coordinator generation that owns the report round.
 * @param[in] backoff Delay before the next retry.
 * @param[in] status Transient report failure.
 */
void LogReportRetry(const std::string &cluster, const std::string &reporter, uint64_t version,
                    const std::string &coordinatorId, std::chrono::milliseconds backoff, const Status &status)
{
    VLOG(1) << "CLUSTER_RECOVERY_REPORT_RETRY cluster=" << cluster << ", reporter=" << reporter
            << ", version=" << version
            << ", coordinator_id=" << coordinatorId.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
            << ", backoff_ms=" << backoff.count() << ", status=" << status.ToString();
}

/**
 * @brief Emit a Coordinator decision that intentionally ends the current report round.
 * @param[in] cluster Cluster scope of the report.
 * @param[in] reporter Canonical reporter address.
 * @param[in] coordinatorId Coordinator generation that owns the report round.
 * @param[in] response Coordinator recovery decision.
 */
void LogReportDeferred(const std::string &cluster, const std::string &reporter,
                       const std::string &coordinatorId,
                       const coordinator::ReportTopologyRecoveryCandidateRspPb &response)
{
    VLOG(1) << "CLUSTER_RECOVERY_REPORT_DEFERRED cluster=" << cluster << ", reporter=" << reporter
            << ", coordinator_id=" << coordinatorId.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
            << ", result=" << response.result() << ", state=" << response.recovery_state();
}

/**
 * @brief Emit successful completion for one CoordinatorId-bound report round.
 * @param[in] cluster Cluster scope of the report.
 * @param[in] reporter Canonical reporter address.
 * @param[in] version Reported topology version.
 * @param[in] coordinatorId Coordinator generation that accepted the report.
 */
void LogReportComplete(const std::string &cluster, const std::string &reporter, uint64_t version,
                       const std::string &coordinatorId)
{
    LOG(INFO) << "CLUSTER_RECOVERY_REPORT_COMPLETE cluster=" << cluster << ", reporter=" << reporter
              << ", version=" << version
              << ", coordinator_id=" << coordinatorId.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE);
}
}  // namespace

TopologyRecoveryReporter::TopologyRecoveryReporter(ICoordinatorServiceProxy &proxy, std::string clusterName,
                                                   std::string reporterAddress, SnapshotProvider snapshotProvider,
                                                   TopologyRecoveryReporterOptions options)
    : proxy_(proxy),
      clusterName_(std::move(clusterName)),
      reporterAddress_(std::move(reporterAddress)),
      snapshotProvider_(std::move(snapshotProvider)),
      options_(options),
      reportPool_(std::make_unique<ThreadPool>(REPORT_POOL_SIZE, REPORT_POOL_SIZE, "TopologyRecoveryReporter", true))
{
    if (reporterAddress_.empty() || snapshotProvider_ == nullptr || options_.reportTimeout.count() <= 0
        || options_.reportTimeout.count() > std::numeric_limits<int32_t>::max()
        || options_.minRetryBackoff.count() <= 0 || options_.maxRetryBackoff < options_.minRetryBackoff
        || options_.maxInitialJitter.count() < 0) {
        throw std::invalid_argument("invalid topology recovery reporter options");
    }
}

TopologyRecoveryReporter::~TopologyRecoveryReporter()
{
    LOG_IF_ERROR(Shutdown(), "CLUSTER_RECOVERY_REPORT_SHUTDOWN_FAILED");
}

bool TopologyRecoveryReporter::CanReportLocked() const
{
    return !stopping_ && runtimeReady_ && !membershipCoordinatorId_.empty()
           && membershipCoordinatorId_ != completedCoordinatorId_;
}

void TopologyRecoveryReporter::NotifyMembershipReady(const std::string &coordinatorId)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || coordinatorId.empty()) {
            return;
        }
        membershipCoordinatorId_ = coordinatorId;
    }
    retryCv_.notify_all();
    ScheduleReport();
}

void TopologyRecoveryReporter::NotifyRuntimeReady()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        runtimeReady_ = true;
    }
    ScheduleReport();
}

void TopologyRecoveryReporter::ScheduleReport()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!CanReportLocked() || scheduled_ || reportPool_ == nullptr) {
        return;
    }
    scheduled_ = true;
    try {
        // scheduled_ bounds the single-thread pool to the current round plus at most one successor round.
        reportPool_->Execute([this] {
            std::string coordinatorId;
            bool completed = false;
            try {
                RunReportLoop(coordinatorId, completed);
            } catch (const std::exception &error) {
                LOG(ERROR) << "CLUSTER_RECOVERY_REPORT_FAILED cluster=" << clusterName_
                           << " reporter=" << reporterAddress_ << " reason=exception error=" << error.what();
            } catch (...) {
                LOG(ERROR) << "CLUSTER_RECOVERY_REPORT_FAILED cluster=" << clusterName_
                           << " reporter=" << reporterAddress_ << " reason=unknown_exception";
            }
            FinishRound(coordinatorId, completed);
        });
    } catch (const std::exception &error) {
        scheduled_ = false;
        LOG(WARNING) << "CLUSTER_RECOVERY_REPORT_SCHEDULE_FAILED cluster=" << clusterName_
                     << " reporter=" << reporterAddress_ << " error=" << error.what();
    }
}

bool TopologyRecoveryReporter::WaitRetry(const std::string &coordinatorId, std::chrono::milliseconds delay)
{
    std::unique_lock<std::mutex> lock(mutex_);
    retryCv_.wait_for(lock, delay, [&] {
        return stopping_ || membershipCoordinatorId_ != coordinatorId;
    });
    return !stopping_ && membershipCoordinatorId_ == coordinatorId;
}

void TopologyRecoveryReporter::FinishRound(const std::string &coordinatorId, bool completed)
{
    bool reschedule = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (completed && membershipCoordinatorId_ == coordinatorId) {
            completedCoordinatorId_ = coordinatorId;
        }
        scheduled_ = false;
        reschedule = CanReportLocked() && membershipCoordinatorId_ != coordinatorId;
    }
    if (reschedule) {
        ScheduleReport();
    }
}

Status TopologyRecoveryReporter::LoadCandidate(uint64_t &version, std::string &canonical, std::string &digest,
                                               bool &hasSnapshot) const
{
    auto rc = snapshotProvider_(version, canonical);
    hasSnapshot = rc.IsOk();
    if (!hasSnapshot) {
        return rc.GetCode() == K_NOT_FOUND ? Status::OK() : rc;
    }
    Hasher hasher;
    return hasher.GetSha256Hex(canonical, digest);
}

Status TopologyRecoveryReporter::SendCandidate(
    const std::string &coordinatorId, uint64_t version, const std::string &canonical, const std::string &digest,
    bool hasSnapshot, bool includePayload, coordinator::ReportTopologyRecoveryCandidateRspPb &response)
{
    coordinator::ReportTopologyRecoveryCandidateReqPb request;
    request.set_cluster_name(clusterName_);
    request.set_coordinator_id(coordinatorId);
    request.set_reporter_address(reporterAddress_);
    request.set_result(hasSnapshot ? coordinator::TOPOLOGY_RECOVERY_SNAPSHOT
                                   : coordinator::TOPOLOGY_RECOVERY_NO_SNAPSHOT);
    request.set_topology_version(version);
    request.set_topology_digest(digest);
    if (includePayload) {
        request.set_canonical_topology(canonical);
    }
    return proxy_.ReportTopologyRecoveryCandidate(request, response,
                                                  static_cast<int32_t>(options_.reportTimeout.count()));
}

void TopologyRecoveryReporter::RunReportLoop(std::string &coordinatorId, bool &completed)
{
    bool applyJitter = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        coordinatorId = membershipCoordinatorId_;
        applyJitter = jitteredCoordinatorId_ != coordinatorId;
        jitteredCoordinatorId_ = coordinatorId;
    }
    const auto jitter = applyJitter ? InitialJitter(reporterAddress_, options_) : std::chrono::milliseconds::zero();
    if (!WaitRetry(coordinatorId, jitter)) {
        return;
    }
    uint64_t version = 0;
    std::string canonical;
    std::string digest;
    bool hasSnapshot = false;
    auto loadStatus = LoadCandidate(version, canonical, digest, hasSnapshot);
    if (loadStatus.IsError()) {
        LOG(WARNING) << "CLUSTER_RECOVERY_EXPORT_FAILED, cluster=" << clusterName_
                     << ", reporter=" << reporterAddress_ << ", status=" << loadStatus.ToString();
        return;
    }
    bool includePayload = false;
    auto backoff = options_.minRetryBackoff;
    while (WaitRetry(coordinatorId, std::chrono::milliseconds::zero())) {
        coordinator::ReportTopologyRecoveryCandidateRspPb response;
        auto rc = SendCandidate(coordinatorId, version, canonical, digest, hasSnapshot, includePayload, response);
        if (rc.IsError()) {
            if (!IsRetryableReportStatus(rc)) {
                LOG(ERROR) << "CLUSTER_RECOVERY_REPORT_REJECTED, cluster=" << clusterName_
                           << ", reporter=" << reporterAddress_ << ", version=" << version
                           << ", status=" << rc.ToString();
                return;
            }
            LogReportRetry(clusterName_, reporterAddress_, version, coordinatorId, backoff, rc);
            if (!WaitRetry(coordinatorId, backoff)) {
                return;
            }
            backoff = std::min(backoff * RETRY_BACKOFF_MULTIPLIER, options_.maxRetryBackoff);
            continue;
        }
        if (response.result() != coordinator::ReportTopologyRecoveryCandidateRspPb::ACCEPTED) {
            LogReportDeferred(clusterName_, reporterAddress_, coordinatorId, response);
            return;
        }
        if (response.recovery_state() == coordinator::COORDINATOR_READY) {
            LogReportComplete(clusterName_, reporterAddress_, version, coordinatorId);
            completed = true;
            return;
        }
        if (response.payload_required() && hasSnapshot && !includePayload) {
            VLOG(1) << "CLUSTER_RECOVERY_PAYLOAD_REQUESTED cluster=" << clusterName_
                    << ", reporter=" << reporterAddress_ << ", version=" << version;
            includePayload = true;
            continue;
        }
        return;
    }
}

Status TopologyRecoveryReporter::Shutdown()
{
    std::unique_ptr<ThreadPool> pool;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return Status::OK();
        }
        stopping_ = true;
        pool = std::move(reportPool_);
    }
    retryCv_.notify_all();
    pool.reset();
    return Status::OK();
}

}  // namespace datasystem::cluster
