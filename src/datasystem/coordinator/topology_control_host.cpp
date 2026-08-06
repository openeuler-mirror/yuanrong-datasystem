/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Process-local owner of per-cluster topology Controller runtimes.
 */
#include "datasystem/coordinator/topology_control_host.h"

#include <algorithm>
#include <exception>
#include <limits>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::coordinator {
namespace {
constexpr size_t MIN_ACTIVE_CLUSTERS = 2;
constexpr size_t MAX_ACTIVE_CLUSTERS = 32;
constexpr size_t MIN_EVENT_QUEUE_CAPACITY = 1;
constexpr size_t MAX_EVENT_QUEUE_CAPACITY = 1'024;
constexpr auto MIN_RECONCILE_INTERVAL = std::chrono::milliseconds(10);
constexpr auto MAX_RECONCILE_INTERVAL = std::chrono::seconds(1);
constexpr auto MIN_START_RETRY = std::chrono::milliseconds(10);
constexpr auto MAX_START_RETRY_INITIAL = std::chrono::seconds(1);
constexpr auto MAX_START_RETRY = std::chrono::seconds(30);
constexpr auto RUNTIME_STOP_SLICE = std::chrono::milliseconds(10);
constexpr auto COORDINATOR_JANITOR_INTERVAL = std::chrono::seconds(10);
constexpr size_t COORDINATOR_JANITOR_SCAN_LIMIT = 8'192;
constexpr size_t COORDINATOR_JANITOR_DELETE_BATCH = 8'192;
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;
constexpr size_t HOST_CAPACITY_LOG_INTERVAL = 128;
constexpr size_t HOST_LIFECYCLE_LOG_INTERVAL = 100;
constexpr size_t MAX_PENDING_LIVENESS_REPORTS = 1'024;
constexpr size_t MIN_ACTIVE_FAILURE_REPORTERS = 2;
constexpr int RETRY_BACKOFF_MULTIPLIER = 2;
constexpr int64_t RUNTIME_START_WARN_MS = 500;
constexpr int64_t RUNTIME_STOP_WARN_MS = 1'000;

bool IsControlMutation(const ParsedTopologyCoordinationKey &parsed)
{
    return parsed.kind == TopologyCoordinationKeyKind::TOPOLOGY
           || parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP
           || parsed.kind == TopologyCoordinationKeyKind::MIGRATE_TASK
           || parsed.kind == TopologyCoordinationKeyKind::DELETE_TASK;
}

void PreserveFirstError(const Status &candidate, Status &firstError)
{
    if (firstError.IsOk() && candidate.IsError()) {
        firstError = candidate;
    }
}
}  // namespace

bool TopologyControlHost::Options::IsValid() const noexcept
{
    return maxClusters >= MIN_ACTIVE_CLUSTERS && maxClusters <= MAX_ACTIVE_CLUSTERS
           && eventQueueCapacity >= MIN_EVENT_QUEUE_CAPACITY && eventQueueCapacity <= MAX_EVENT_QUEUE_CAPACITY
           && reconcileInterval >= MIN_RECONCILE_INTERVAL && reconcileInterval <= MAX_RECONCILE_INTERVAL
           && startRetryInitial >= MIN_START_RETRY && startRetryInitial <= MAX_START_RETRY_INITIAL
           && startRetryMaximum >= startRetryInitial && startRetryMaximum <= MAX_START_RETRY
           && activeFailureWindow.count() > 0 && controller.IsValid();
}

TopologyControlHost::ClusterEntry::ClusterEntry(std::string name) : clusterName(std::move(name))
{
}

TopologyControlHost::ClusterEntry::~ClusterEntry() = default;

TopologyControlHost::TopologyControlHost(std::string coordinatorId, CoordinatorStore &store,
                                         TopologyRecoveryManager &recovery)
    : TopologyControlHost(std::move(coordinatorId), store, recovery, Options{})
{
}

TopologyControlHost::TopologyControlHost(std::string coordinatorId, CoordinatorStore &store,
                                         TopologyRecoveryManager &recovery, Options options)
    : coordinatorId_(std::move(coordinatorId)), store_(store), recovery_(recovery), options_(std::move(options))
{
}

TopologyControlHost::~TopologyControlHost()
{
    LOG_IF_ERROR(Shutdown(std::chrono::steady_clock::time_point::max()),
                 "CLUSTER_CONTROL_HOST state=destructor_shutdown_failed");
}

Status TopologyControlHost::Start()
{
    CHECK_FAIL_RETURN_STATUS(coordinatorId_.size() == UUID_SIZE && options_.IsValid(), K_INVALID,
                             "invalid topology Control Host identity or options");
    std::unique_lock<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!started_ && !stopping_, K_INVALID, "topology Control Host Start is one-shot");
    started_ = true;
    threadExited_ = false;
    threadJoined_ = false;
    try {
        thread_ = Thread(&TopologyControlHost::Run, this);
        thread_.set_name("topology-host");
    } catch (const std::exception &error) {
        stopping_ = true;
        wakeCv_.notify_all();
        lock.unlock();
        if (thread_.joinable()) {
            thread_.join();
        }
        lock.lock();
        started_ = false;
        stopping_ = false;
        threadExited_ = true;
        threadJoined_ = true;
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start topology Control Host failed: ") + error.what());
    }
    LOG(INFO) << "CLUSTER_CONTROL_HOST state=started coordinator_id="
              << BytesUuidToString(coordinatorId_).substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
              << " cluster_limit=" << options_.maxClusters;
    return Status::OK();
}

Status TopologyControlHost::PrepareMembershipPut(const std::string &clusterName)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(started_ && !stopping_, K_SHUTTING_DOWN,
                             "topology Control Host does not accept membership admission");
    auto found = entries_.find(clusterName);
    if (found != entries_.end()) {
        ++found->second->mutationGeneration;
        ++found->second->pendingMembershipPuts;
        found->second->releaseAfterStop = false;
        return Status::OK();
    }
    if (entries_.size() >= options_.maxClusters) {
        LOG_FIRST_AND_EVERY_N(WARNING, HOST_CAPACITY_LOG_INTERVAL)
            << "CLUSTER_CONTROL_HOST cluster=" << clusterName
            << " action=admission_rejected reason=capacity_exhausted"
            << " active_clusters=" << entries_.size() << " cluster_limit=" << options_.maxClusters;
        RETURN_STATUS(K_TRY_AGAIN, "controller_capacity_exhausted");
    }
    auto entry = std::make_unique<ClusterEntry>(clusterName);
    ++entry->mutationGeneration;
    entry->pendingMembershipPuts = 1;
    entries_.emplace(clusterName, std::move(entry));
    wakeCv_.notify_all();
    return Status::OK();
}

void TopologyControlHost::CompleteMembershipPut(const std::string &clusterName, bool committed) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(clusterName);
    if (found == entries_.end()) {
        return;
    }
    auto &entry = *found->second;
    ++entry.mutationGeneration;
    if (entry.pendingMembershipPuts > 0) {
        --entry.pendingMembershipPuts;
    }
    entry.hasCommittedMembership = entry.hasCommittedMembership || committed;
    entry.storeDirty = entry.storeDirty || committed;
    if (entry.hasCommittedMembership && entry.state == EntryState::RESERVED) {
        entry.state = EntryState::WAITING_RECOVERY;
    }
    entry.releaseAfterStop = !entry.hasCommittedMembership && entry.pendingMembershipPuts == 0;
    wakeCv_.notify_all();
}

void TopologyControlHost::NotifyStoreMutation(WatchEvent::Type type,
                                              const ParsedTopologyCoordinationKey &parsed) noexcept
{
    if (!IsControlMutation(parsed)) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(parsed.clusterName);
    if (found == entries_.end()) {
        return;
    }
    ++found->second->mutationGeneration;
    if (found->second->storeDirty) {
        ++coalescedDoorbells_;
    }
    found->second->storeDirty = true;
    found->second->emptyCheckPending =
        found->second->emptyCheckPending || parsed.kind == TopologyCoordinationKeyKind::TOPOLOGY
        || (parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP && type == WatchEvent::Type::DELETE);
    const bool debounceMembershipPut = parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP
                                       && type == WatchEvent::Type::PUT
                                       && found->second->state == EntryState::RUNNING;
    if (!debounceMembershipPut) {
        wakeCv_.notify_all();
    }
}

Status TopologyControlHost::EnqueueWorkerLivenessReport(const std::string &clusterName,
                                                        cluster::WorkerLivenessReport report)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(started_ && !stopping_, K_SHUTTING_DOWN,
                             "topology Control Host does not accept liveness reports");
    auto found = entries_.find(clusterName);
    CHECK_FAIL_RETURN_STATUS(found != entries_.end() && found->second->state == EntryState::RUNNING, K_NOT_READY,
                             "cluster Controller runtime is not running");
    CHECK_FAIL_RETURN_STATUS(found->second->pendingLivenessReports.size()
                                     + found->second->deliveringLivenessReports
                                 < MAX_PENDING_LIVENESS_REPORTS,
                             K_TRY_AGAIN, "worker liveness report queue is full");
    found->second->pendingLivenessReports.emplace_back(std::move(report));
    wakeCv_.notify_all();
    return Status::OK();
}

void TopologyControlHost::RecordWorkerFailureSummaries(const std::string &clusterName, const std::string &reporter,
                                                       const std::vector<std::string> &targets)
{
    cluster::MembershipRecord reporterRecord{ reporter, cluster::MemberLifecycleState::READY, 0, "" };
    std::vector<cluster::MembershipRecord> targetRecords;
    targetRecords.reserve(targets.size());
    for (const auto &target : targets) {
        targetRecords.push_back({ target, cluster::MemberLifecycleState::READY, 0, "" });
    }
    RecordWorkerFailureSummaries(clusterName, reporterRecord, targetRecords);
}

void TopologyControlHost::RecordWorkerFailureSummaries(const std::string &clusterName,
                                                       const cluster::MembershipRecord &reporter,
                                                       const std::vector<cluster::MembershipRecord> &targets)
{
    if (reporter.address.empty() || targets.empty()) {
        return;
    }
    const auto now = options_.controller.now();
    std::lock_guard<std::mutex> lock(mutex_);
    auto &clusterReports = failureReportsByCluster_[clusterName];
    bool shouldWake = false;
    for (const auto &target : targets) {
        if (target.address.empty() || target.address == reporter.address) {
            continue;
        }
        auto &reporters = clusterReports[target.address];
        const auto oldSize = reporters.size();
        auto &state = reporters[reporter.address];
        const bool newReporter = reporters.size() != oldSize;
        state = FailureReportState{
            now,
            reporter.timestamp,
            reporter.hostId,
            target.timestamp,
            target.hostId
        };
        shouldWake = shouldWake || newReporter;
    }
    if (shouldWake) {
        auto entry = entries_.find(clusterName);
        if (entry != entries_.end()) {
            entry->second->storeDirty = true;
        }
        VLOG(1) << "CLUSTER_FAILURE_REPORT role=coordinator cluster=" << clusterName
                << " reporter=" << reporter.address;
        wakeCv_.notify_all();
    }
}

std::unordered_map<std::string, cluster::MembershipRecord> TopologyControlHost::BuildReadyActiveMemberships(
    const cluster::TopologySnapshot &latest, const std::vector<cluster::MembershipRecord> &memberships)
{
    std::unordered_set<std::string> activeMembers;
    activeMembers.reserve(latest.ActiveMembers().size());
    for (const auto *member : latest.ActiveMembers()) {
        activeMembers.emplace(member->identity.address);
    }
    std::unordered_map<std::string, cluster::MembershipRecord> readyMemberships;
    readyMemberships.reserve(memberships.size());
    for (const auto &membership : memberships) {
        if (membership.state == cluster::MemberLifecycleState::READY && activeMembers.count(membership.address) > 0) {
            readyMemberships.emplace(membership.address, membership);
        }
    }
    return readyMemberships;
}

size_t TopologyControlHost::ActiveFailureReporterThreshold(size_t totalWorkers)
{
    if (totalWorkers <= 1) {
        return std::numeric_limits<size_t>::max();
    }
    const auto percentThreshold = (totalWorkers + 19) / 20;
    const auto configuredThreshold = std::min(std::max<size_t>(percentThreshold, 5), totalWorkers - 1);
    return std::max<size_t>(configuredThreshold, MIN_ACTIVE_FAILURE_REPORTERS);
}

size_t TopologyControlHost::PruneAndCountFailureReporters(
    const cluster::TopologySnapshot &latest,
    const std::unordered_map<std::string, cluster::MembershipRecord> &readyMemberships, const std::string &target,
    std::unordered_map<std::string, FailureReportState> &reporters, std::chrono::steady_clock::time_point now) const
{
    size_t validReports = 0;
    const cluster::Member *targetMember = nullptr;
    const bool targetActive = latest.FindMemberByAddress(target, targetMember).IsOk() && targetMember != nullptr
                              && targetMember->state == cluster::MemberState::ACTIVE;
    const auto targetMembership = readyMemberships.find(target);
    const bool targetReady = targetMembership != readyMemberships.end();
    for (auto reporterIter = reporters.begin(); reporterIter != reporters.end();) {
        const auto &reporter = reporterIter->first;
        const bool expired = now - reporterIter->second.receiveTime > options_.activeFailureWindow;
        const auto reporterMembership = readyMemberships.find(reporter);
        const bool reporterActive = reporterMembership != readyMemberships.end();
        const auto &state = reporterIter->second;
        const bool reporterChanged =
            reporterActive && state.reporterTimestamp != 0
            && (state.reporterTimestamp != reporterMembership->second.timestamp
                || state.reporterHostId != reporterMembership->second.hostId);
        const bool targetChanged =
            targetReady
            && (state.targetTimestamp < 0
                || (state.targetTimestamp > 0
                    && (state.targetTimestamp != targetMembership->second.timestamp
                        || state.targetHostId != targetMembership->second.hostId)));
        if (expired || !targetActive || !reporterActive || reporter == target || reporterChanged || targetChanged) {
            reporterIter = reporters.erase(reporterIter);
            continue;
        }
        ++validReports;
        ++reporterIter;
    }
    return validReports;
}

std::vector<cluster::MemberIdentity> TopologyControlHost::GetIsolationCandidates(
    const std::string &clusterName, const cluster::TopologySnapshot &latest,
    const std::vector<cluster::MembershipRecord> &memberships, std::chrono::steady_clock::time_point now)
{
    const auto readyMemberships = BuildReadyActiveMemberships(latest, memberships);
    size_t totalWorkers = 0;
    for (const auto &member : latest.Members()) {
        if (member.state == cluster::MemberState::ACTIVE) {
            ++totalWorkers;
        }
    }
    if (totalWorkers <= 1) {
        return {};
    }
    const auto reporterThreshold = ActiveFailureReporterThreshold(totalWorkers);
    if (reporterThreshold > totalWorkers - 1) {
        return {};
    }
    std::vector<cluster::MemberIdentity> candidates;
    std::lock_guard<std::mutex> lock(mutex_);
    auto clusterIter = failureReportsByCluster_.find(clusterName);
    if (clusterIter == failureReportsByCluster_.end()) {
        return candidates;
    }
    auto &clusterReports = clusterIter->second;
    for (auto targetIter = clusterReports.begin(); targetIter != clusterReports.end();) {
        const auto &target = targetIter->first;
        auto &reporters = targetIter->second;
        const auto validReports = PruneAndCountFailureReporters(latest, readyMemberships, target, reporters, now);
        if (validReports >= reporterThreshold) {
            const cluster::Member *member = nullptr;
            if (latest.FindMemberByAddress(target, member).IsOk() && member != nullptr
                && member->state == cluster::MemberState::ACTIVE) {
                candidates.push_back(member->identity);
            }
        }
        if (reporters.empty()) {
            targetIter = clusterReports.erase(targetIter);
        } else {
            ++targetIter;
        }
    }
    if (clusterReports.empty()) {
        failureReportsByCluster_.erase(clusterIter);
    }
    return candidates;
}

void TopologyControlHost::Run() noexcept
{
    while (true) {
        try {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                wakeCv_.wait_for(lock, options_.reconcileInterval);
                if (stopping_) {
                    break;
                }
            }
            ReconcileEntries();
        } catch (const std::exception &error) {
            LOG_FIRST_AND_EVERY_N(ERROR, HOST_LIFECYCLE_LOG_INTERVAL)
                << "CLUSTER_CONTROL_HOST action=reconcile_exception status=" << error.what();
        } catch (...) {
            LOG(ERROR) << "CLUSTER_CONTROL_HOST action=reconcile_exception status=unknown";
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            break;
        }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    threadExited_ = true;
    wakeCv_.notify_all();
}

void TopologyControlHost::ReconcileEntries()
{
    INJECT_POINT_NO_RETURN("TopologyControlHost.ReconcileEntries.enter");
    INJECT_POINT_NO_RETURN("TopologyControlHost.ReconcileEntries.exception", [](const std::string &) {
        throw std::runtime_error("injected Host reconcile exception");
    });
    std::vector<std::string> clusterNames;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        clusterNames.reserve(entries_.size());
        for (const auto &item : entries_) {
            clusterNames.emplace_back(item.first);
        }
        if (!clusterNames.empty()) {
            reconcileCursor_ %= clusterNames.size();
            std::rotate(clusterNames.begin(), clusterNames.begin() + reconcileCursor_, clusterNames.end());
            reconcileCursor_ = (reconcileCursor_ + 1) % clusterNames.size();
        }
    }
    for (const auto &clusterName : clusterNames) {
        ReconcileCluster(clusterName);
    }
}

void TopologyControlHost::ReconcileCluster(const std::string &clusterName)
{
    ClusterEntry *entry = nullptr;
    EntryState state = EntryState::RESERVED;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = entries_.find(clusterName);
        if (found == entries_.end()) {
            return;
        }
        entry = found->second.get();
        state = entry->state;
        if (state == EntryState::RESERVED && entry->releaseAfterStop) {
            entries_.erase(found);
            return;
        }
    }
    if (state == EntryState::WAITING_RECOVERY) {
        ReconcileWaitingEntry(clusterName, *entry);
    } else if (state == EntryState::RUNNING) {
        ReconcileRunningEntry(clusterName, *entry);
    } else if (state == EntryState::STOPPING) {
        ReconcileStoppingEntry(clusterName, *entry);
    }
}

void TopologyControlHost::ReconcileWaitingEntry(const std::string &clusterName, ClusterEntry &entry)
{
    const auto recoveryState = recovery_.GetState(clusterName);
    const auto now = std::chrono::steady_clock::now();
    size_t pendingMembershipPuts = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingMembershipPuts = entry.pendingMembershipPuts;
    }
    if (recoveryState == TopologyRecoveryState::RECOVERING && pendingMembershipPuts == 0) {
        bool released = false;
        uint64_t observationGeneration = 0;
        const auto status = ReleaseClusterIfEmpty(entry, released, observationGeneration);
        if (status.IsOk() && released) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = entries_.find(clusterName);
            if (found != entries_.end() && found->second.get() == &entry
                && IsEmptyObservationCurrent(entry, observationGeneration)) {
                entries_.erase(found);
            }
            return;
        }
    }
    if (recoveryState != TopologyRecoveryState::READY || now < entry.retryAt) {
        return;
    }
    const auto startedAt = std::chrono::steady_clock::now();
    Status status;
    try {
        status = StartRuntime(entry);
    } catch (const std::exception &error) {
        status = Status(K_RUNTIME_ERROR, std::string("start topology Runtime threw: ") + error.what());
    } catch (...) {
        status = Status(K_RUNTIME_ERROR, "start topology Runtime threw an unknown exception");
    }
    const auto elapsedMs = cluster::DurationMs(startedAt, std::chrono::steady_clock::now());
    std::lock_guard<std::mutex> lock(mutex_);
    if (status.IsOk()) {
        entry.state = EntryState::RUNNING;
        entry.retryBackoff = options_.startRetryInitial;
        LOG(INFO) << "CLUSTER_CONTROL_HOST cluster=" << clusterName << " state=running coordinator_id="
                  << BytesUuidToString(coordinatorId_).substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << " start_elapsed_ms=" << elapsedMs;
        if (elapsedMs > RUNTIME_START_WARN_MS) {
            LOG(WARNING) << "CLUSTER_CONTROL_HOST cluster=" << clusterName
                         << " action=slow_runtime_start elapsed_ms=" << elapsedMs
                         << " threshold_ms=" << RUNTIME_START_WARN_MS;
        }
        return;
    }
    entry.state = EntryState::STOPPING;
    entry.stopReason = "runtime_start_failed";
    LOG(WARNING) << "CLUSTER_CONTROL_HOST cluster=" << clusterName
                 << " state=start_failed action=bounded_stop elapsed_ms=" << elapsedMs
                 << " status=" << status.ToString();
}

void TopologyControlHost::ReconcileRunningEntry(const std::string &clusterName, ClusterEntry &entry)
{
    const auto recoveryState = recovery_.GetState(clusterName);
    const auto diagnostics = entry.runtime->GetDiagnostics();
    const bool controllerMayRun = recoveryState == TopologyRecoveryState::READY
                                  || recoveryState == TopologyRecoveryState::RECOVERING;
    if (controllerMayRun && diagnostics.running) {
        bool inspectEmpty = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            inspectEmpty = entry.emptyCheckPending;
            entry.emptyCheckPending = false;
        }
        bool released = false;
        uint64_t observationGeneration = 0;
        const auto releaseStatus =
            inspectEmpty ? ReleaseClusterIfEmpty(entry, released, observationGeneration) : Status::OK();
        if (releaseStatus.IsError()) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                entry.emptyCheckPending = true;
            }
            LOG_FIRST_AND_EVERY_N(WARNING, HOST_LIFECYCLE_LOG_INTERVAL)
                << "CLUSTER_CONTROL_HOST cluster=" << clusterName
                << " action=inspect_empty_failed status=" << releaseStatus.ToString();
        } else if (released) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (IsEmptyObservationCurrent(entry, observationGeneration)) {
                entry.state = EntryState::STOPPING;
                entry.releaseAfterStop = true;
                entry.stopReason = "cluster_empty";
                LOG(INFO) << "CLUSTER_CONTROL_HOST cluster=" << clusterName
                          << " state=stopping reason=cluster_empty";
                return;
            }
            entry.emptyCheckPending = true;
        } else if (inspectEmpty && recoveryState == TopologyRecoveryState::RECOVERING) {
            LOG_FIRST_AND_EVERY_N(WARNING, HOST_LIFECYCLE_LOG_INTERVAL)
                << "CLUSTER_CONTROL_HOST cluster=" << clusterName
                << " action=retain_runtime reason=topology_authority_not_empty recovery_state=RECOVERING";
        }
        SubmitWorkerLivenessReports(entry);
        SubmitDoorbell(entry);
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    entry.state = EntryState::STOPPING;
    entry.releaseAfterStop = recoveryState == TopologyRecoveryState::RECOVERING;
    entry.stopReason = diagnostics.running ? "recovery_not_ready" : "runtime_not_running";
    LOG(WARNING) << "CLUSTER_CONTROL_HOST cluster=" << clusterName << " state=stopping reason=" << entry.stopReason
                 << " recovery_state=" << static_cast<int>(recoveryState)
                 << " runtime_error=" << diagnostics.lastError;
}

void TopologyControlHost::SubmitWorkerLivenessReports(ClusterEntry &entry)
{
    std::deque<cluster::WorkerLivenessReport> reports;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        reports.swap(entry.pendingLivenessReports);
        entry.deliveringLivenessReports += reports.size();
    }
    while (!reports.empty()) {
        auto report = std::move(reports.front());
        reports.pop_front();
        ++report.deliveryAttempts;
        auto status = entry.runtime->SubmitWorkerLivenessReport(report);
        const auto probeId = cluster::WorkerProbeIdForLog(report.probeEpoch, report.probeRound);
        if (status.IsOk()) {
            LOG(INFO) << "CLUSTER_CONTROL_HOST cluster=" << entry.clusterName
                      << " action=WITNESS_PROBE_REPORT_DELIVERED probe_id=" << probeId
                      << " witness=" << report.witnessAddress << " target=" << report.target.address
                      << " result=" << cluster::WorkerLivenessResultName(report.result)
                      << " attempts=" << report.deliveryAttempts;
            std::lock_guard<std::mutex> lock(mutex_);
            --entry.deliveringLivenessReports;
            continue;
        }
        LOG(WARNING) << "CLUSTER_CONTROL_HOST cluster=" << entry.clusterName
                     << " action=WITNESS_PROBE_REPORT_DELIVERY_FAILED probe_id=" << probeId
                     << " witness=" << report.witnessAddress << " target=" << report.target.address
                     << " result=" << cluster::WorkerLivenessResultName(report.result)
                     << " attempts=" << report.deliveryAttempts << " retry=false status=" << status.ToString();
        std::lock_guard<std::mutex> lock(mutex_);
        --entry.deliveringLivenessReports;
    }
}

void TopologyControlHost::ReconcileStoppingEntry(const std::string &clusterName, ClusterEntry &entry)
{
    const auto startedAt = std::chrono::steady_clock::now();
    const auto status = StopRuntime(entry, startedAt + RUNTIME_STOP_SLICE);
    if (status.GetCode() == K_RPC_DEADLINE_EXCEEDED || entry.runtime != nullptr) {
        if (status.IsError()) {
            LOG_FIRST_AND_EVERY_N(WARNING, HOST_LIFECYCLE_LOG_INTERVAL)
                << "CLUSTER_CONTROL_HOST cluster=" << clusterName
                << " state=stopping action=retry_same_runtime"
                << " elapsed_ms=" << cluster::DurationMs(startedAt, std::chrono::steady_clock::now())
                << " status=" << status.ToString();
        }
        return;
    }
    FinishStoppedEntry(clusterName, entry);
}

Status TopologyControlHost::StartRuntime(ClusterEntry &entry)
{
    INJECT_POINT("TopologyControlHost.StartRuntime");
    CHECK_FAIL_RETURN_STATUS(entry.backend == nullptr && entry.runtime == nullptr, K_INVALID,
                             "topology Runtime dependencies already exist");
    auto backend = std::make_unique<CoordinatorStoreBackend>(store_);
    cluster::TopologyControllerRuntime::Options runtimeOptions;
    runtimeOptions.clusterName = entry.clusterName;
    runtimeOptions.eventQueueCapacity = options_.eventQueueCapacity;
    runtimeOptions.controller = options_.controller;
    runtimeOptions.controller.eventSourceMode = cluster::TopologyEventSourceMode::EXTERNAL;
    runtimeOptions.controller.localAddress.clear();
    runtimeOptions.controller.probeEpoch = coordinatorId_;
    CHECK_FAIL_RETURN_STATUS(nextRuntimeGeneration_ <= std::numeric_limits<uint32_t>::max(), K_RUNTIME_ERROR,
                             "topology Runtime probe generation space is exhausted");
    runtimeOptions.controller.initialProbeRound = (nextRuntimeGeneration_++ << 32U) + 1U;
    runtimeOptions.controller.membershipRestartHandler = {};
    runtimeOptions.controller.materializeRestartFacts = true;
    runtimeOptions.controller.activeFailureCandidateProvider =
        [this, clusterName = entry.clusterName](const cluster::TopologySnapshot &latest,
                                                const std::vector<cluster::MembershipRecord> &memberships,
                                                std::chrono::steady_clock::time_point now) {
            return GetIsolationCandidates(clusterName, latest, memberships, now);
        };
    runtimeOptions.janitor = cluster::TopologyTaskJanitorOptions{
        COORDINATOR_JANITOR_INTERVAL, COORDINATOR_JANITOR_SCAN_LIMIT, COORDINATOR_JANITOR_DELETE_BATCH
    };
    std::unique_ptr<cluster::TopologyControllerRuntime> runtime;
    RETURN_IF_NOT_OK(cluster::TopologyControllerRuntime::Create(
        std::move(runtimeOptions), *backend, entry.algorithm, runtime));
    entry.backend = std::move(backend);
    entry.runtime = std::move(runtime);
    INJECT_POINT_NO_RETURN("TopologyControlHost.StartRuntime.afterPublish", [](const std::string &kind) {
        struct UnknownStartException {};
        if (kind == "std") {
            throw std::runtime_error("injected Runtime Start exception");
        }
        throw UnknownStartException{};
    });
    return entry.runtime->Start();
}

Status TopologyControlHost::StopRuntime(ClusterEntry &entry,
                                        std::chrono::steady_clock::time_point deadline)
{
    if (entry.runtime == nullptr) {
        entry.backend.reset();
        return Status::OK();
    }
    INJECT_POINT("TopologyControlHost.StopRuntime");
    const auto status = entry.runtime->Stop(deadline);
    if (status.IsOk()) {
        entry.runtime.reset();
        entry.backend.reset();
    }
    return status;
}

Status TopologyControlHost::ReleaseClusterIfEmpty(ClusterEntry &entry, bool &released,
                                                  uint64_t &observationGeneration)
{
    released = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        observationGeneration = entry.mutationGeneration;
        if (entry.pendingMembershipPuts > 0) {
            return Status::OK();
        }
    }
    CoordinatorStoreBackend backend(store_);
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(entry.clusterName, keys));
    std::string encodedTopology;
    const auto readStatus = backend.Get(keys->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(),
                                        encodedTopology);
    if (readStatus.IsOk()) {
        INJECT_POINT_NO_RETURN("TopologyControlHost.ReleaseClusterIfEmpty.afterTopologyRead");
        cluster::TopologyState topology;
        RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::DecodeTopology(encodedTopology, topology));
        if (!topology.members.empty() || topology.activeBatch.has_value()) {
            return Status::OK();
        }
    } else if (readStatus.GetCode() != K_NOT_FOUND) {
        return readStatus;
    }
    std::vector<std::pair<std::string, std::string>> memberships;
    RETURN_IF_NOT_OK(backend.GetAll(keys->MembershipTable(), memberships));
    released = memberships.empty();
    INJECT_POINT_NO_RETURN("TopologyControlHost.ReleaseClusterIfEmpty.afterRead");
    return Status::OK();
}

bool TopologyControlHost::IsEmptyObservationCurrent(const ClusterEntry &entry,
                                                    uint64_t observationGeneration) const noexcept
{
    return entry.mutationGeneration == observationGeneration && entry.pendingMembershipPuts == 0;
}

void TopologyControlHost::SubmitDoorbell(ClusterEntry &entry)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!entry.storeDirty || entry.runtime == nullptr) {
            return;
        }
        entry.storeDirty = false;
    }
    const auto status = entry.runtime->SubmitCoordinationEvent(
        { cluster::CoordinationEventType::RESET, "", "", 0, 0 });
    if (status.IsOk()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++runtimeResyncs_;
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        entry.storeDirty = true;
    }
    LOG_FIRST_AND_EVERY_N(WARNING, HOST_LIFECYCLE_LOG_INTERVAL)
        << "CLUSTER_CONTROL_HOST cluster=" << entry.clusterName
        << " action=retry_doorbell status=" << status.ToString();
}

void TopologyControlHost::FinishStoppedEntry(const std::string &clusterName, ClusterEntry &entry)
{
    bool released = false;
    uint64_t observationGeneration = 0;
    const auto releaseStatus = ReleaseClusterIfEmpty(entry, released, observationGeneration);
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(clusterName);
    if (found == entries_.end()) {
        return;
    }
    entry.pendingLivenessReports.clear();
    entry.deliveringLivenessReports = 0;
    if (releaseStatus.IsOk() && released && IsEmptyObservationCurrent(entry, observationGeneration)) {
        LOG(INFO) << "CLUSTER_CONTROL_HOST cluster=" << clusterName << " state=released";
        entries_.erase(found);
        return;
    }
    entry.state = EntryState::WAITING_RECOVERY;
    entry.releaseAfterStop = false;
    entry.retryBackoff =
        entry.retryBackoff.count() == 0
            ? options_.startRetryInitial
            : std::min(entry.retryBackoff * RETRY_BACKOFF_MULTIPLIER,
                       std::chrono::duration_cast<std::chrono::milliseconds>(options_.startRetryMaximum));
    entry.retryAt = std::chrono::steady_clock::now() + entry.retryBackoff;
}

Status TopologyControlHost::StopAllRuntimes(std::chrono::steady_clock::time_point deadline)
{
    Status firstError;
    for (auto &item : entries_) {
        const auto startedAt = std::chrono::steady_clock::now();
        auto status = StopRuntime(*item.second, deadline);
        const auto elapsedMs = cluster::DurationMs(startedAt, std::chrono::steady_clock::now());
        if (elapsedMs > RUNTIME_STOP_WARN_MS) {
            LOG(WARNING) << "CLUSTER_CONTROL_HOST cluster=" << item.first
                         << " action=slow_runtime_stop elapsed_ms=" << elapsedMs
                         << " threshold_ms=" << RUNTIME_STOP_WARN_MS << " status=" << status.ToString();
        }
        PreserveFirstError(status, firstError);
    }
    return firstError;
}

Status TopologyControlHost::Shutdown(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!started_ && entries_.empty()) {
        return Status::OK();
    }
    if (shutdownInProgress_) {
        const bool completed = wakeCv_.wait_until(lock, deadline, [this] { return !shutdownInProgress_; });
        CHECK_FAIL_RETURN_STATUS(completed, K_RPC_DEADLINE_EXCEEDED, "topology Control Host shutdown wait timed out");
        if (!started_ && entries_.empty()) {
            return Status::OK();
        }
    }
    shutdownInProgress_ = true;
    stopping_ = true;
    wakeCv_.notify_all();
    const bool exited = wakeCv_.wait_until(lock, deadline, [this] { return threadExited_; });
    if (!exited) {
        shutdownInProgress_ = false;
        wakeCv_.notify_all();
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "topology Control Host thread stop deadline exceeded");
    }
    lock.unlock();
    if (thread_.joinable()) {
        thread_.join();
    }
    lock.lock();
    threadJoined_ = true;
    lock.unlock();
    auto status = StopAllRuntimes(deadline);
    lock.lock();
    const bool allStopped =
        std::all_of(entries_.begin(), entries_.end(), [](const auto &item) { return item.second->runtime == nullptr; });
    if (allStopped) {
        LOG(INFO) << "CLUSTER_CONTROL_HOST state=stopped cluster_count=" << entries_.size()
                  << " coalesced_doorbells=" << coalescedDoorbells_
                  << " runtime_resyncs=" << runtimeResyncs_ << " status=" << status.ToString();
        entries_.clear();
        started_ = false;
    }
    shutdownInProgress_ = false;
    wakeCv_.notify_all();
    return status;
}

bool TopologyControlHost::IsStopped() const noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    return threadExited_ && threadJoined_ && entries_.empty() && !shutdownInProgress_;
}

}  // namespace datasystem::coordinator
