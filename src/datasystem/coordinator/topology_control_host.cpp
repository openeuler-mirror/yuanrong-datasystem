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

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/raii.h"
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
constexpr size_t TWO_WORKER_CLUSTER_SIZE = 2;
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

std::unordered_map<std::string, cluster::MembershipRecord> IndexMembershipsByAddress(
    const std::vector<cluster::MembershipRecord> &memberships)
{
    std::unordered_map<std::string, cluster::MembershipRecord> byAddress;
    byAddress.reserve(memberships.size());
    for (const auto &membership : memberships) {
        byAddress.emplace(membership.address, membership);
    }
    return byAddress;
}

size_t CountFailurePopulation(const cluster::TopologySnapshot &latest)
{
    const auto &activeBatch = latest.GetActiveBatch();
    // PRE_LEAVING cannot report, but an active ScaleIn still blocks its exit, so it remains in the quorum population.
    const auto isFailurePopulationMember = [&activeBatch](const cluster::Member &member) {
        const bool scaleInMember = activeBatch.has_value()
                                   && activeBatch->type == cluster::TopologyChangeType::SCALE_IN
                                   && (member.state == cluster::MemberState::PRE_LEAVING
                                       || member.state == cluster::MemberState::LEAVING);
        return member.state == cluster::MemberState::ACTIVE || scaleInMember;
    };
    return std::count_if(latest.Members().begin(), latest.Members().end(), isFailurePopulationMember);
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
        CHECK_FAIL_RETURN_STATUS(!found->second->activeFailureCommitInProgress, K_TRY_AGAIN,
                                 "active failure commit is in progress");
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
    entry->clusterGeneration = nextClusterGeneration_++;
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
    // Address-only compatibility callers cannot provide incarnation fences; production ingestion uses records below.
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
    if (reporter.address.empty()) {
        return;
    }
    uint64_t clusterGeneration = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto entry = entries_.find(clusterName);
        if (entry != entries_.end()) {
            clusterGeneration = entry->second->clusterGeneration;
        }
    }
    INJECT_POINT_NO_RETURN("TopologyControlHost.RecordWorkerFailureSummaries.afterGenerationCapture");
    // Eligibility is evaluated later against one current topology and membership snapshot.
    const auto now = options_.controller.now();
    std::unordered_set<std::string> reportedTargets;
    reportedTargets.reserve(targets.size());
    for (const auto &target : targets) {
        if (!target.address.empty() && target.address != reporter.address) {
            reportedTargets.emplace(target.address);
        }
    }
    std::vector<std::string> newTargets;
    std::vector<std::string> clearedTargets;
    const bool shouldWake = UpdateFailureReports(clusterName, reporter, targets, reportedTargets, clusterGeneration,
                                                 now, newTargets, clearedTargets);
    bool currentGeneration = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto entry = entries_.find(clusterName);
        currentGeneration = entry != entries_.end() && entry->second->clusterGeneration == clusterGeneration;
        if (currentGeneration && shouldWake) {
            entry->second->storeDirty = true;
            wakeCv_.notify_all();
        }
    }
    if (currentGeneration && shouldWake) {
        if (!newTargets.empty()) {
            LOG(INFO) << "CLUSTER_FAILURE_REPORT role=coordinator action=summary_received cluster=" << clusterName
                      << " reporter=" << reporter.address << " targets=" << VectorToString(newTargets);
        }
        if (!clearedTargets.empty()) {
            LOG(INFO) << "CLUSTER_FAILURE_REPORT role=coordinator action=summary_cleared cluster=" << clusterName
                      << " reporter=" << reporter.address << " targets=" << VectorToString(clearedTargets);
        }
    }
}

bool TopologyControlHost::UpdateFailureReports(const std::string &clusterName,
                                               const cluster::MembershipRecord &reporter,
                                               const std::vector<cluster::MembershipRecord> &targets,
                                               const std::unordered_set<std::string> &reportedTargets,
                                               uint64_t clusterGeneration, std::chrono::steady_clock::time_point now,
                                               std::vector<std::string> &newTargets,
                                               std::vector<std::string> &clearedTargets)
{
    bool shouldWake = false;
    auto clusterMutex = GetFailureReportClusterMutex(clusterName);
    if (clusterMutex == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> clusterLock(*clusterMutex);
    {
        std::lock_guard<std::mutex> hostLock(mutex_);
        const auto entry = entries_.find(clusterName);
        if (entry == entries_.end() || entry->second->state != EntryState::RUNNING
            || entry->second->clusterGeneration != clusterGeneration) {
            return false;
        }
    }
    std::lock_guard<std::mutex> lock(failureReportMutex_);
    auto clusterIter = failureReportsByCluster_.find(clusterName);
    if (clusterIter == failureReportsByCluster_.end()) {
        if (reportedTargets.empty()) {
            return false;
        }
        clusterIter =
            failureReportsByCluster_.emplace(clusterName, decltype(failureReportsByCluster_)::mapped_type{}).first;
    }
    auto &clusterReports = clusterIter->second;
    shouldWake = ClearUnreportedTargets(clusterReports, reporter, reportedTargets, clusterGeneration, clearedTargets);
    for (const auto &target : targets) {
        if (target.address.empty() || target.address == reporter.address) {
            continue;
        }
        auto &reporters = clusterReports.byTarget[target.address];
        auto reporterIter = reporters.find(reporter.address);
        if (reporterIter != reporters.end() && reporterIter->second.clusterGeneration > clusterGeneration) {
            continue;
        }
        const bool newReporter =
            reporterIter == reporters.end() || reporterIter->second.clusterGeneration != clusterGeneration;
        if (newReporter) {
            newTargets.emplace_back(target.address);
        }
        reporters[reporter.address] =
            FailureReportState{ now,           reporter.state,   reporter.timestamp, reporter.hostId, target.timestamp,
                                target.hostId, clusterGeneration };
        clusterReports.targetsByReporter[reporter.address].insert(target.address);
        shouldWake = shouldWake || newReporter;
    }
    if (clusterReports.empty()) {
        failureReportsByCluster_.erase(clusterIter);
    }
    return shouldWake;
}

bool TopologyControlHost::ClearUnreportedTargets(ClusterFailureReports &clusterReports,
                                                 const cluster::MembershipRecord &reporter,
                                                 const std::unordered_set<std::string> &reportedTargets,
                                                 uint64_t clusterGeneration, std::vector<std::string> &clearedTargets)
{
    bool cleared = false;
    auto reverseIter = clusterReports.targetsByReporter.find(reporter.address);
    if (reverseIter == clusterReports.targetsByReporter.end()) {
        return false;
    }
    for (auto previousIter = reverseIter->second.begin(); previousIter != reverseIter->second.end();) {
        const auto &target = *previousIter;
        if (reportedTargets.count(target) > 0) {
            ++previousIter;
            continue;
        }
        auto targetIter = clusterReports.byTarget.find(target);
        if (targetIter == clusterReports.byTarget.end()) {
            previousIter = reverseIter->second.erase(previousIter);
            continue;
        }
        auto reporterIter = targetIter->second.find(reporter.address);
        if (reporterIter != targetIter->second.end() && reporterIter->second.clusterGeneration == clusterGeneration) {
            targetIter->second.erase(reporterIter);
            cleared = true;
            clearedTargets.emplace_back(target);
            previousIter = reverseIter->second.erase(previousIter);
        } else {
            ++previousIter;
        }
        if (targetIter->second.empty()) {
            clusterReports.byTarget.erase(targetIter);
        }
    }
    if (reverseIter->second.empty()) {
        clusterReports.targetsByReporter.erase(reverseIter);
    }
    return cleared;
}

std::unordered_map<std::string, cluster::MembershipRecord> TopologyControlHost::BuildEligibleFailureReporters(
    const cluster::TopologySnapshot &latest, const std::vector<cluster::MembershipRecord> &memberships)
{
    std::unordered_set<std::string> activeMembers;
    std::unordered_set<std::string> leavingMembers;
    activeMembers.reserve(latest.ActiveMembers().size());
    leavingMembers.reserve(latest.Members().size());
    for (const auto *member : latest.ActiveMembers()) {
        activeMembers.emplace(member->identity.address);
    }
    for (const auto &member : latest.Members()) {
        if (member.state == cluster::MemberState::LEAVING) {
            leavingMembers.emplace(member.identity.address);
        }
    }
    std::unordered_map<std::string, cluster::MembershipRecord> eligibleReporters;
    eligibleReporters.reserve(memberships.size());
    for (const auto &membership : memberships) {
        const bool serving = membership.state == cluster::MemberLifecycleState::READY
                             && activeMembers.count(membership.address) > 0;
        const bool leaving = membership.state == cluster::MemberLifecycleState::EXITING
                             && leavingMembers.count(membership.address) > 0;
        if (serving || leaving) {
            eligibleReporters.emplace(membership.address, membership);
        }
    }
    return eligibleReporters;
}

size_t TopologyControlHost::FailureReporterThreshold(size_t totalWorkers, size_t eligibleReporterCount)
{
    if (totalWorkers <= 1) {
        return std::numeric_limits<size_t>::max();
    }
    if (totalWorkers == TWO_WORKER_CLUSTER_SIZE) {
        return 1;
    }
    const auto percentThreshold = (totalWorkers + 19) / 20;
    const auto configuredThreshold = std::min(std::max<size_t>(percentThreshold, 5), totalWorkers - 1);
    // Concurrent failures can make the configured threshold unattainable. In that case require every reporter that
    // remains eligible in the Coordinator's current membership view, while preserving the multi-reporter safety floor.
    const auto attainableThreshold = std::max<size_t>(eligibleReporterCount, MIN_ACTIVE_FAILURE_REPORTERS);
    return std::min(std::max<size_t>(configuredThreshold, MIN_ACTIVE_FAILURE_REPORTERS), attainableThreshold);
}

void TopologyControlHost::RemoveTargetFromReporterIndex(ClusterFailureReports &clusterReports,
                                                        const std::string &reporter, const std::string &target)
{
    auto reverseIter = clusterReports.targetsByReporter.find(reporter);
    if (reverseIter == clusterReports.targetsByReporter.end()) {
        return;
    }
    reverseIter->second.erase(target);
    if (reverseIter->second.empty()) {
        clusterReports.targetsByReporter.erase(reverseIter);
    }
}

size_t TopologyControlHost::PruneAndCountFailureReporters(
    const cluster::TopologySnapshot &latest,
    const std::unordered_map<std::string, cluster::MembershipRecord> &eligibleReporters,
    const std::unordered_map<std::string, cluster::MembershipRecord> &membershipsByAddress, const std::string &target,
    uint64_t clusterGeneration, ClusterFailureReports &clusterReports,
    std::unordered_map<std::string, FailureReportState> &reporters, std::chrono::steady_clock::time_point now) const
{
    size_t validReports = 0;
    const cluster::Member *targetMember = nullptr;
    const bool targetFound = latest.FindMemberByAddress(target, targetMember).IsOk() && targetMember != nullptr;
    const auto activeBatch = latest.GetActiveBatch();
    const bool targetEligible = targetFound
                                && (targetMember->state == cluster::MemberState::ACTIVE
                                    || (targetMember->state == cluster::MemberState::JOINING
                                        && activeBatch.has_value()
                                        && activeBatch->type == cluster::TopologyChangeType::SCALE_OUT));
    const auto targetMembership = membershipsByAddress.find(target);
    const bool targetPresent = targetMembership != membershipsByAddress.end();
    for (auto reporterIter = reporters.begin(); reporterIter != reporters.end();) {
        const auto &reporter = reporterIter->first;
        const bool expired = now - reporterIter->second.receiveTime > options_.activeFailureWindow;
        const auto reporterMembership = eligibleReporters.find(reporter);
        const bool reporterEligible = reporterMembership != eligibleReporters.end();
        const auto &state = reporterIter->second;
        const bool reporterChanged =
            reporterEligible
            && (state.reporterState != reporterMembership->second.state
                || (state.reporterTimestamp != 0
                    && (state.reporterTimestamp != reporterMembership->second.timestamp
                        || state.reporterHostId != reporterMembership->second.hostId)));
        const bool targetChanged =
            targetPresent
            && (state.targetTimestamp < 0
                || (state.targetTimestamp > 0
                    && (state.targetTimestamp != targetMembership->second.timestamp
                        || state.targetHostId != targetMembership->second.hostId)));
        if (state.clusterGeneration != clusterGeneration || expired || !targetEligible || !reporterEligible
            || reporter == target || reporterChanged || targetChanged) {
            RemoveTargetFromReporterIndex(clusterReports, reporter, target);
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
    uint64_t clusterGeneration = 0;
    auto clusterMutex = GetFailureReportClusterMutex(clusterName);
    if (clusterMutex == nullptr) {
        return {};
    }
    std::lock_guard<std::mutex> clusterLock(*clusterMutex);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto entry = entries_.find(clusterName);
        if (entry == entries_.end() || entry->second->state != EntryState::RUNNING) {
            return {};
        }
        clusterGeneration = entry->second->clusterGeneration;
    }
    const auto eligibleReporters = BuildEligibleFailureReporters(latest, memberships);
    const auto membershipsByAddress = IndexMembershipsByAddress(memberships);
    std::lock_guard<std::mutex> lock(failureReportMutex_);
    return GetIsolationCandidatesLocked(clusterName, latest, eligibleReporters, membershipsByAddress,
                                        CountFailurePopulation(latest), clusterGeneration, now);
}

std::shared_ptr<std::mutex> TopologyControlHost::GetFailureReportClusterMutex(const std::string &clusterName)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto entry = entries_.find(clusterName);
    return entry == entries_.end() ? nullptr : entry->second->failureReportMutex;
}

std::vector<cluster::MemberIdentity> TopologyControlHost::GetIsolationCandidatesLocked(
    const std::string &clusterName, const cluster::TopologySnapshot &latest,
    const std::unordered_map<std::string, cluster::MembershipRecord> &eligibleReporters,
    const std::unordered_map<std::string, cluster::MembershipRecord> &membershipsByAddress, size_t failurePopulation,
    uint64_t clusterGeneration, std::chrono::steady_clock::time_point now)
{
    std::vector<cluster::MemberIdentity> candidates;
    auto clusterIter = failureReportsByCluster_.find(clusterName);
    if (clusterIter == failureReportsByCluster_.end()) {
        return candidates;
    }
    auto &clusterReports = clusterIter->second;
    const auto activeBatch = latest.GetActiveBatch();
    for (auto targetIter = clusterReports.byTarget.begin(); targetIter != clusterReports.byTarget.end();) {
        const auto &target = targetIter->first;
        auto &reporters = targetIter->second;
        const auto validReports = PruneAndCountFailureReporters(latest, eligibleReporters, membershipsByAddress, target,
                                                                clusterGeneration, clusterReports, reporters, now);
        const cluster::Member *targetMember = nullptr;
        const bool joiningScaleOutTarget = activeBatch.has_value()
                                           && activeBatch->type == cluster::TopologyChangeType::SCALE_OUT
                                           && latest.FindMemberByAddress(target, targetMember).IsOk()
                                           && targetMember != nullptr
                                           && targetMember->state == cluster::MemberState::JOINING;
        const size_t totalWorkers = failurePopulation + (joiningScaleOutTarget ? 1 : 0);
        const auto targetIsEligibleReporter = eligibleReporters.count(target) > 0;
        const auto eligibleReporterCount = eligibleReporters.size() - static_cast<size_t>(targetIsEligibleReporter);
        const auto reporterThreshold = FailureReporterThreshold(totalWorkers, eligibleReporterCount);
        if (validReports >= reporterThreshold) {
            const cluster::Member *member = nullptr;
            if (latest.FindMemberByAddress(target, member).IsOk() && member != nullptr) {
                candidates.push_back(member->identity);
            }
        }
        if (reporters.empty()) {
            targetIter = clusterReports.byTarget.erase(targetIter);
        } else {
            ++targetIter;
        }
    }
    if (clusterReports.empty()) {
        failureReportsByCluster_.erase(clusterIter);
    }
    return candidates;
}

Status TopologyControlHost::RunUnderActiveFailureCommitFence(const std::string &clusterName,
                                                             const cluster::TopologySnapshot &latest,
                                                             const std::vector<cluster::MembershipRecord> &,
                                                             std::chrono::steady_clock::time_point,
                                                             std::optional<uint64_t> expectedControlEpoch,
                                                             const std::vector<cluster::MemberIdentity> &expected,
                                                             const std::function<Status(int64_t)> &mutation)
{
    ActiveFailureReservation reservation;
    if (!TryReserveActiveFailureCommit(clusterName, reservation)) {
        return Status::OK();
    }
    Raii releaseReservation(
        [this, &clusterName, &reservation] { ReleaseActiveFailureCommit(clusterName, reservation.clusterGeneration); });
    std::vector<cluster::MembershipRecord> currentMemberships;
    int64_t authorityRevision = 0;
    RETURN_IF_NOT_OK(ReadCurrentMemberships(clusterName, currentMemberships, authorityRevision));
    const auto eligibleReporters = BuildEligibleFailureReporters(latest, currentMemberships);
    const auto membershipsByAddress = IndexMembershipsByAddress(currentMemberships);
    const auto validateAndCommit = [&]() {
        auto clusterMutex = GetFailureReportClusterMutex(clusterName);
        CHECK_FAIL_RETURN_STATUS(clusterMutex != nullptr, K_NOT_READY, "Cluster failure report state is unavailable");
        std::lock_guard<std::mutex> clusterLock(*clusterMutex);
        std::vector<cluster::MemberIdentity> candidates;
        {
            std::lock_guard<std::mutex> lock(failureReportMutex_);
            candidates = GetIsolationCandidatesLocked(clusterName, latest, eligibleReporters, membershipsByAddress,
                                                      CountFailurePopulation(latest), reservation.clusterGeneration,
                                                      options_.controller.now());
        }
        if (!ActiveFailureCandidatesContainExpected(candidates, expected)) {
            LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << clusterName << " version=" << latest.Version()
                      << " action=active_summary_commit_fence decision=preserve expected_count=" << expected.size()
                      << " candidate_count=" << candidates.size();
            return Status::OK();
        }
        if (!IsActiveFailureReservationCurrent(clusterName, reservation)) {
            LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << clusterName << " version=" << latest.Version()
                      << " action=active_summary_commit_fence decision=preserve reason=control_mutated";
            return Status::OK();
        }
        return mutation(authorityRevision);
    };
    return options_.activeFailureAuthorityFence
               ? options_.activeFailureAuthorityFence(expectedControlEpoch, validateAndCommit)
               : validateAndCommit();
}

Status TopologyControlHost::ReadCurrentMemberships(const std::string &clusterName,
                                                   std::vector<cluster::MembershipRecord> &memberships,
                                                   int64_t &authorityRevision)
{
    CoordinatorStoreBackend backend(store_);
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
    std::vector<std::pair<std::string, std::string>> values;
    RETURN_IF_NOT_OK(backend.GetAll(keys->MembershipTable(), values, authorityRevision));
    memberships.clear();
    memberships.reserve(values.size());
    for (const auto &[address, bytes] : values) {
        cluster::MembershipValue value;
        RETURN_IF_NOT_OK(cluster::MembershipValueCodec::Decode(bytes, value));
        memberships.push_back({ address, value.lifecycleState, value.timestamp, value.hostId });
    }
    return Status::OK();
}

bool TopologyControlHost::TryReserveActiveFailureCommit(const std::string &clusterName,
                                                        ActiveFailureReservation &reservation)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = entries_.find(clusterName);
    if (entry == entries_.end() || entry->second->state != EntryState::RUNNING
        || entry->second->pendingMembershipPuts > 0 || entry->second->activeFailureCommitInProgress) {
        return false;
    }
    entry->second->activeFailureCommitInProgress = true;
    reservation = { entry->second->clusterGeneration, entry->second->mutationGeneration };
    return true;
}

bool TopologyControlHost::IsActiveFailureReservationCurrent(const std::string &clusterName,
                                                            const ActiveFailureReservation &reservation)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto entry = entries_.find(clusterName);
    return entry != entries_.end() && entry->second->state == EntryState::RUNNING
           && entry->second->activeFailureCommitInProgress && entry->second->pendingMembershipPuts == 0
           && entry->second->clusterGeneration == reservation.clusterGeneration
           && entry->second->mutationGeneration == reservation.mutationGeneration;
}

void TopologyControlHost::ReleaseActiveFailureCommit(const std::string &clusterName, uint64_t clusterGeneration)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto entry = entries_.find(clusterName);
    if (entry != entries_.end() && entry->second->clusterGeneration == clusterGeneration) {
        entry->second->activeFailureCommitInProgress = false;
        wakeCv_.notify_all();
    }
}

bool TopologyControlHost::ActiveFailureCandidatesContainExpected(const std::vector<cluster::MemberIdentity> &candidates,
                                                                 const std::vector<cluster::MemberIdentity> &expected)
{
    if (expected.size() > candidates.size()) {
        return false;
    }
    std::unordered_map<std::string, std::string> current;
    current.reserve(candidates.size());
    for (const auto &candidate : candidates) {
        current.emplace(candidate.address, candidate.id);
    }
    return std::all_of(expected.begin(), expected.end(), [&](const auto &identity) {
        const auto iter = current.find(identity.address);
        return iter != current.end() && iter->second == identity.id;
    });
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
    bool releaseReserved = false;
    uint64_t mutationGeneration = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = entries_.find(clusterName);
        if (found == entries_.end()) {
            return;
        }
        entry = found->second.get();
        state = entry->state;
        releaseReserved = state == EntryState::RESERVED && entry->releaseAfterStop;
        mutationGeneration = entry->mutationGeneration;
    }
    if (releaseReserved) {
        static_cast<void>(EraseClusterIfCurrent(clusterName, entry, mutationGeneration));
        return;
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
            bool observationCurrent = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto found = entries_.find(clusterName);
                observationCurrent = found != entries_.end() && found->second.get() == &entry
                                     && IsEmptyObservationCurrent(entry, observationGeneration);
            }
            if (observationCurrent) {
                static_cast<void>(EraseClusterIfCurrent(clusterName, &entry, observationGeneration));
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
    runtimeOptions.controller.failureSummaryCandidateProvider =
        [this, clusterName = entry.clusterName](const cluster::TopologySnapshot &latest,
                                                const std::vector<cluster::MembershipRecord> &memberships,
                                                std::chrono::steady_clock::time_point now) {
            return GetIsolationCandidates(clusterName, latest, memberships, now);
        };
    runtimeOptions.controller.activeFailureCommitFence =
        [this, clusterName = entry.clusterName](
            const cluster::TopologySnapshot &latest, const std::vector<cluster::MembershipRecord> &memberships,
            std::chrono::steady_clock::time_point now, std::optional<uint64_t> expectedControlEpoch,
            const std::vector<cluster::MemberIdentity> &expected,
            const std::function<Status(int64_t)> &mutation) {
            return RunUnderActiveFailureCommitFence(clusterName, latest, memberships, now, expectedControlEpoch,
                                                    expected, mutation);
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

bool TopologyControlHost::EraseClusterIfCurrent(const std::string &clusterName, const ClusterEntry *expected,
                                                uint64_t mutationGeneration)
{
    auto clusterMutex = GetFailureReportClusterMutex(clusterName);
    if (clusterMutex == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> clusterLock(*clusterMutex);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = entries_.find(clusterName);
        if (found == entries_.end() || found->second.get() != expected
            || found->second->mutationGeneration != mutationGeneration) {
            return false;
        }
        entries_.erase(found);
    }
    std::lock_guard<std::mutex> lock(failureReportMutex_);
    failureReportsByCluster_.erase(clusterName);
    return true;
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
    std::unique_lock<std::mutex> lock(mutex_);
    auto found = entries_.find(clusterName);
    if (found == entries_.end()) {
        return;
    }
    entry.pendingLivenessReports.clear();
    entry.deliveringLivenessReports = 0;
    if (releaseStatus.IsOk() && released && IsEmptyObservationCurrent(entry, observationGeneration)) {
        LOG(INFO) << "CLUSTER_CONTROL_HOST cluster=" << clusterName << " state=released";
        lock.unlock();
        static_cast<void>(EraseClusterIfCurrent(clusterName, &entry, observationGeneration));
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
