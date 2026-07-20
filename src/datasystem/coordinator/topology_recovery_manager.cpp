/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
#include "datasystem/coordinator/topology_recovery_manager.h"

#include <algorithm>
#include <array>
#include <future>
#include <optional>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::coordinator {
namespace {
constexpr size_t SHA256_HEX_SIZE = 64;
constexpr size_t DIGEST_LOG_PREFIX_SIZE = 8;
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;
constexpr size_t TOPOLOGY_KEYSPACE_KIND_COUNT = 5;
constexpr uint64_t MEMBER_LIMIT_LOG_INTERVAL = 1'024;
constexpr char PHYSICAL_ROOT[] = "/datasystem/";

TraceContext GetRecoveryTraceContext()
{
    auto traceContext = Trace::Instance().GetContext();
    if (traceContext.traceID.empty()) {
        traceContext.traceID = "CoordRec;" + GetStringUuid();
    }
    return traceContext;
}

struct TopologyCandidateEvidence {
    bool hasSnapshot{ false };
    uint64_t topologyVersion{ 0 };
    std::string canonicalDigest;
    bool payloadRequested{ false };
};

struct TopologyCandidateSelection {
    uint64_t highestVersion{ 0 };
    std::string highestDigest;
    bool hasSnapshot{ false };
    bool conflictingHighest{ false };
};

bool IsLowerHex(const std::string &value)
{
    return std::all_of(value.begin(), value.end(), [](char ch) {
        return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
    });
}

bool SameEvidence(const TopologyCandidateEvidence &evidence, const TopologyRecoveryCandidateReport &report)
{
    return evidence.hasSnapshot == report.hasSnapshot && evidence.topologyVersion == report.topologyVersion
           && evidence.canonicalDigest == report.canonicalDigest;
}

TopologyCandidateSelection SelectHighestCandidate(
    const std::unordered_map<std::string, TopologyCandidateEvidence> &evidenceByReporter)
{
    TopologyCandidateSelection selection;
    for (const auto &[address, evidence] : evidenceByReporter) {
        static_cast<void>(address);
        if (!evidence.hasSnapshot) {
            continue;
        }
        selection.hasSnapshot = true;
        if (evidence.topologyVersion > selection.highestVersion) {
            selection.highestVersion = evidence.topologyVersion;
            selection.highestDigest = evidence.canonicalDigest;
            selection.conflictingHighest = false;
        } else if (evidence.topologyVersion == selection.highestVersion
                   && evidence.canonicalDigest != selection.highestDigest) {
            selection.conflictingHighest = true;
        }
    }
    return selection;
}

bool HasPeerPayloadRequest(const std::unordered_map<std::string, TopologyCandidateEvidence> &evidenceByReporter,
                           const std::string &reporterAddress, const TopologyCandidateSelection &selection)
{
    return std::any_of(evidenceByReporter.begin(), evidenceByReporter.end(), [&](const auto &entry) {
        return entry.first != reporterAddress && entry.second.payloadRequested
               && entry.second.topologyVersion == selection.highestVersion
               && entry.second.canonicalDigest == selection.highestDigest;
    });
}

Status ValidateRelativeKey(TopologyCoordinationKeyKind kind, const std::string &relative)
{
    std::string canonical;
    if (kind == TopologyCoordinationKeyKind::TOPOLOGY) {
        CHECK_FAIL_RETURN_STATUS(relative.empty(), K_INVALID, "topology singleton key is not exact");
    } else if (kind == TopologyCoordinationKeyKind::MIGRATE_TASK && !relative.empty()) {
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::TaskKey(relative, canonical));
        CHECK_FAIL_RETURN_STATUS(relative.rfind("m-", 0) == 0, K_INVALID, "migrate task key kind mismatch");
    } else if (kind == TopologyCoordinationKeyKind::DELETE_TASK && !relative.empty()) {
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::TaskKey(relative, canonical));
        CHECK_FAIL_RETURN_STATUS(relative.rfind("d-", 0) == 0, K_INVALID, "delete task key kind mismatch");
    } else if (kind == TopologyCoordinationKeyKind::NOTIFY && !relative.empty()) {
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::NotifyKey(relative, canonical));
    } else if (kind == TopologyCoordinationKeyKind::MEMBERSHIP && !relative.empty()) {
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(relative, canonical));
    }
    return Status::OK();
}

Status MatchKeyspace(const cluster::TopologyKeyHelper &keys, const std::string &physicalKey,
                     ParsedTopologyCoordinationKey &parsed, bool &matched)
{
    const std::array<std::pair<const std::string *, TopologyCoordinationKeyKind>, TOPOLOGY_KEYSPACE_KIND_COUNT>
        tables = {
        std::make_pair(&keys.TopologyTable(), TopologyCoordinationKeyKind::TOPOLOGY),
        std::make_pair(&keys.MigrateTaskTable(), TopologyCoordinationKeyKind::MIGRATE_TASK),
        std::make_pair(&keys.DeleteTaskTable(), TopologyCoordinationKeyKind::DELETE_TASK),
        std::make_pair(&keys.NotifyTable(), TopologyCoordinationKeyKind::NOTIFY),
        std::make_pair(&keys.MembershipTable(), TopologyCoordinationKeyKind::MEMBERSHIP),
    };
    for (const auto &[table, kind] : tables) {
        if (physicalKey == *table) {
            RETURN_STATUS(K_INVALID, "topology coordination key lacks separator");
        }
        const std::string prefix = *table + "/";
        if (physicalKey.rfind(prefix, 0) != 0) {
            continue;
        }
        const std::string relative = physicalKey.substr(prefix.size());
        RETURN_IF_NOT_OK(ValidateRelativeKey(kind, relative));
        parsed = ParsedTopologyCoordinationKey{ keys.ClusterName(), kind, relative };
        matched = true;
        return Status::OK();
    }
    return Status::OK();
}

bool HasCapacity(size_t used, size_t requested, size_t limit)
{
    return used <= limit && requested <= limit - used;
}

bool RangeIntersectsTopologyRoot(const std::string &key, const std::string &rangeEnd)
{
    if (rangeEnd.empty()) {
        return false;
    }
    const std::string rootEnd = StringPlusOne(PHYSICAL_ROOT);
    return key < rootEnd && std::string(PHYSICAL_ROOT) < rangeEnd;
}

/**
 * @brief Wait for one payload-validation task and normalize asynchronous failures.
 * @param[in,out] result Started validation result.
 * @param[in] timeout Bounded RPC-side wait.
 * @return Validation result or retry/shutdown failure.
 */
Status AwaitPayloadValidation(std::future<Status> &result, std::chrono::milliseconds timeout)
{
    if (result.wait_for(timeout) != std::future_status::ready) {
        RETURN_STATUS(K_TRY_AGAIN, "candidate validation continues after report deadline");
    }
    try {
        return result.get();
    } catch (const std::future_error &error) {
        RETURN_STATUS(K_SHUTTING_DOWN, std::string("candidate validation cancelled: ") + error.what());
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("candidate validation failed: ") + error.what());
    } catch (...) {
        RETURN_STATUS(K_RUNTIME_ERROR, "candidate validation failed with an unknown exception");
    }
}
}  // namespace

struct TopologyRecoveryManager::ClusterRecoveryContext {
    TopologyRecoveryState state{ TopologyRecoveryState::RECOVERING };
    std::unordered_set<std::string> observedMembers;
    std::unordered_map<std::string, TopologyCandidateEvidence> reporterEvidence;
    std::shared_ptr<const std::string> selectedCanonicalTopology;
    std::string selectedCanonicalDigest;
    uint64_t selectedVersion{ 0 };
    std::optional<std::chrono::steady_clock::time_point> discoveryDeadline;
    bool payloadValidationPending{ false };
    bool reconcileQueued{ false };
};

TopologyRecoveryManager::TopologyRecoveryManager(std::string coordinatorId, CoordinatorStore &store,
                                                 std::shared_ptr<SteadyClock> clock,
                                                 TopologyRecoveryOptions options)
    : coordinatorId_(std::move(coordinatorId)), store_(store), clock_(std::move(clock)), options_(options)
{
    if (coordinatorId_.size() != UUID_SIZE || clock_ == nullptr || options_.discoveryWindow.count() <= 0
        || options_.validationWaitTimeout.count() <= 0 || options_.minRecoveryThreads == 0
        || options_.maxRecoveryThreads < options_.minRecoveryThreads || options_.maxPendingRecoveryWork == 0
        || options_.maxClusters == 0 || options_.maxMembersPerCluster == 0
        || options_.maxCandidateMemoryBytes == 0) {
        throw std::invalid_argument("invalid topology recovery options");
    }
    recoveryPool_ = std::make_unique<ThreadPool>(options_.minRecoveryThreads, options_.maxRecoveryThreads,
                                                 "TopologyRecovery", true);
}

TopologyRecoveryManager::~TopologyRecoveryManager()
{
    LOG_IF_ERROR(Shutdown(), "CLUSTER_RECOVERY_MANAGER_SHUTDOWN_FAILED");
}

Status TopologyRecoveryManager::ParseKey(const std::string &physicalKey,
                                         ParsedTopologyCoordinationKey &parsed) const
{
    parsed = ParsedTopologyCoordinationKey{};
    if (physicalKey.rfind(PHYSICAL_ROOT, 0) != 0) {
        return Status::OK();
    }
    const std::string suffix = physicalKey.substr(sizeof(PHYSICAL_ROOT) - 1);
    const size_t separator = suffix.find('/');
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    bool matched = false;
    if (separator != std::string::npos) {
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(suffix.substr(0, separator), keys));
        RETURN_IF_NOT_OK(MatchKeyspace(*keys, physicalKey, parsed, matched));
        if (matched) {
            return Status::OK();
        }
    }
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create("", keys));
    return MatchKeyspace(*keys, physicalKey, parsed, matched);
}

Status TopologyRecoveryManager::EnsureContext(const std::string &clusterName, ClusterRecoveryContext *&context)
{
    auto found = contexts_.find(clusterName);
    if (found != contexts_.end()) {
        context = found->second.get();
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(contexts_.size() < options_.maxClusters, K_TRY_AGAIN,
                             "topology recovery cluster admission limit reached");
    auto inserted = contexts_.emplace(clusterName, std::make_unique<ClusterRecoveryContext>());
    context = inserted.first->second.get();
    LOG(INFO) << "CLUSTER_RECOVERY_STATE cluster=" << clusterName << ", coordinator_id="
              << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
              << ", old=UNSEEN, new=RECOVERING, reason=membership_admitted";
    return Status::OK();
}

Status TopologyRecoveryManager::CheckAllowed(const std::string &key, const std::string &rangeEnd, bool mutation)
{
    ParsedTopologyCoordinationKey parsed;
    RETURN_IF_NOT_OK(ParseKey(key, parsed));
    if (parsed.kind == TopologyCoordinationKeyKind::OTHER) {
        CHECK_FAIL_RETURN_STATUS(rangeEnd.empty() || rangeEnd > key, K_INVALID, "invalid coordination range");
        CHECK_FAIL_RETURN_STATUS(!RangeIntersectsTopologyRoot(key, rangeEnd), K_INVALID,
                                 "coordination range crosses the topology keyspace boundary");
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(rangeEnd.empty() || (!mutation && parsed.relativeKey.empty()
                                                     && rangeEnd == StringPlusOne(key)),
                             K_INVALID, "topology coordination range crosses a keyspace boundary");
    if ((mutation && parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP)
        || (!mutation && parsed.kind != TopologyCoordinationKeyKind::TOPOLOGY)) {
        return Status::OK();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = contexts_.find(parsed.clusterName);
        if (found == contexts_.end()) {
            RETURN_STATUS(K_NOT_READY, "cluster membership is not established");
        }
        auto *context = found->second.get();
        if (context->state == TopologyRecoveryState::READY) {
            return Status::OK();
        }
    }
    ScheduleReconcile(parsed.clusterName);
    RETURN_STATUS(K_NOT_READY, "cluster topology is recovering");
}

Status TopologyRecoveryManager::CheckReadAllowed(const std::string &key, const std::string &rangeEnd)
{
    return CheckAllowed(key, rangeEnd, false);
}

Status TopologyRecoveryManager::CheckMutationAllowed(const std::string &key, const std::string &rangeEnd)
{
    return CheckAllowed(key, rangeEnd, true);
}

Status TopologyRecoveryManager::ValidateWatchRange(const std::string &key, const std::string &rangeEnd) const
{
    ParsedTopologyCoordinationKey parsed;
    RETURN_IF_NOT_OK(ParseKey(key, parsed));
    if (parsed.kind == TopologyCoordinationKeyKind::OTHER) {
        CHECK_FAIL_RETURN_STATUS(rangeEnd.empty() || rangeEnd > key, K_INVALID, "invalid coordination range");
        CHECK_FAIL_RETURN_STATUS(!RangeIntersectsTopologyRoot(key, rangeEnd), K_INVALID,
                                 "watch range crosses the topology keyspace boundary");
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(rangeEnd.empty()
                                 || (parsed.relativeKey.empty() && rangeEnd == StringPlusOne(key)),
                             K_INVALID, "watch range crosses a topology keyspace boundary");
    return Status::OK();
}

void TopologyRecoveryManager::UpdateMembership(const ParsedTopologyCoordinationKey &parsed, bool present)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ClusterRecoveryContext *context = nullptr;
    auto found = contexts_.find(parsed.clusterName);
    auto ensureStatus = present ? EnsureContext(parsed.clusterName, context) : Status::OK();
    if (ensureStatus.IsError()) {
        LOG_EVERY_N(ERROR, MEMBER_LIMIT_LOG_INTERVAL)
            << "CLUSTER_RECOVERY_CONTEXT_ADMISSION_FAILED, cluster=" << parsed.clusterName
            << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
            << ", status=" << ensureStatus.ToString();
        return;
    }
    context = present ? context : (found == contexts_.end() ? nullptr : found->second.get());
    if (context == nullptr) {
        return;
    }
    if (present) {
        if (context->observedMembers.count(parsed.relativeKey) != 0) {
            return;
        }
        if (context->observedMembers.size() >= options_.maxMembersPerCluster) {
            LOG_FIRST_AND_EVERY_N(WARNING, MEMBER_LIMIT_LOG_INTERVAL)
                << "CLUSTER_RECOVERY_MEMBER_LIMIT_REACHED, cluster=" << parsed.clusterName
                << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                << ", members=" << context->observedMembers.size()
                << ", limit=" << options_.maxMembersPerCluster;
            return;
        }
        context->observedMembers.insert(parsed.relativeKey);
        VLOG(1) << "CLUSTER_RECOVERY_MEMBER cluster=" << parsed.clusterName << ", address=" << parsed.relativeKey
                << ", present=true, member_count=" << context->observedMembers.size();
        return;
    }
    context->observedMembers.erase(parsed.relativeKey);
    VLOG(1) << "CLUSTER_RECOVERY_MEMBER cluster=" << parsed.clusterName << ", address=" << parsed.relativeKey
            << ", present=false, member_count=" << context->observedMembers.size();
    const auto selection = SelectHighestCandidate(context->reporterEvidence);
    auto evidence = context->reporterEvidence.find(parsed.relativeKey);
    const bool removedHighest = evidence != context->reporterEvidence.end() && evidence->second.hasSnapshot
                                && evidence->second.topologyVersion == selection.highestVersion;
    context->reporterEvidence.erase(parsed.relativeKey);
    if (context->state == TopologyRecoveryState::INSTALLING) {
        return;
    }
    if (context->state == TopologyRecoveryState::BLOCKED || removedHighest) {
        LOG(INFO) << "CLUSTER_RECOVERY_STATE cluster=" << parsed.clusterName << ", coordinator_id="
                  << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << ", old=BLOCKED_OR_STALE, new=RECOVERING, reason=membership_removed";
        context->state = TopologyRecoveryState::RECOVERING;
        context->discoveryDeadline = context->reporterEvidence.empty()
                                         ? std::nullopt
                                         : std::make_optional(clock_->Now() + options_.discoveryWindow);
    }
    RefreshSelection(*context);
    if (context->reporterEvidence.empty()) {
        context->discoveryDeadline.reset();
    }
    if (context->observedMembers.empty() && context->state != TopologyRecoveryState::INSTALLING) {
        ReleaseSelectedPayload(*context);
        contexts_.erase(parsed.clusterName);
    }
}

void TopologyRecoveryManager::ObserveMembershipChange(const std::string &physicalKey, bool present)
{
    ParsedTopologyCoordinationKey parsed;
    if (ParseKey(physicalKey, parsed).IsError() || parsed.kind != TopologyCoordinationKeyKind::MEMBERSHIP
        || parsed.relativeKey.empty()) {
        return;
    }
    UpdateMembership(parsed, present);
    ScheduleReconcile(parsed.clusterName);
}

void TopologyRecoveryManager::NotifyMembershipActivity(const std::string &physicalKey)
{
    ParsedTopologyCoordinationKey parsed;
    if (ParseKey(physicalKey, parsed).IsError() || parsed.kind != TopologyCoordinationKeyKind::MEMBERSHIP
        || parsed.relativeKey.empty()) {
        return;
    }
    UpdateMembership(parsed, true);
    ScheduleReconcile(parsed.clusterName);
}

Status TopologyRecoveryManager::ValidateEvidence(const std::string &clusterName,
                                                 const TopologyRecoveryCandidateReport &report) const
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
    std::string canonicalAddress;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(report.reporterAddress, canonicalAddress));
    if (!report.hasSnapshot) {
        CHECK_FAIL_RETURN_STATUS(report.topologyVersion == 0 && report.canonicalDigest.empty()
                                     && report.canonicalTopology.empty(),
                                 K_INVALID, "NO_SNAPSHOT report contains topology evidence");
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(report.topologyVersion > 0, K_INVALID, "snapshot topology version is zero");
    CHECK_FAIL_RETURN_STATUS(report.canonicalDigest.size() == SHA256_HEX_SIZE
                                 && IsLowerHex(report.canonicalDigest),
                             K_INVALID, "snapshot digest is not canonical SHA-256 hex");
    CHECK_FAIL_RETURN_STATUS(report.canonicalTopology.size() <= MAX_TOPOLOGY_RECOVERY_PAYLOAD_BYTES, K_INVALID,
                             "candidate topology payload exceeds limit");
    return Status::OK();
}

Status TopologyRecoveryManager::ValidatePayload(const TopologyRecoveryCandidateReport &report) const
{
    cluster::TopologyState state;
    RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::DecodeTopology(report.canonicalTopology, state));
    CHECK_FAIL_RETURN_STATUS(state.version == report.topologyVersion, K_INVALID,
                             "candidate topology version does not match evidence");
    std::string canonical;
    RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(state, canonical));
    CHECK_FAIL_RETURN_STATUS(canonical == report.canonicalTopology, K_INVALID,
                             "candidate topology payload is not canonical");
    std::string digest;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.GetSha256Hex(canonical, digest));
    CHECK_FAIL_RETURN_STATUS(digest == report.canonicalDigest, K_INVALID,
                             "candidate topology digest does not match evidence");
    return Status::OK();
}

Status TopologyRecoveryManager::ReportCandidate(const std::string &clusterName,
                                                const std::string &requestCoordinatorId,
                                                TopologyRecoveryCandidateReport report,
                                                TopologyRecoveryReportDecision &decision)
{
    decision = TopologyRecoveryReportDecision{};
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(!stopping_, K_SHUTTING_DOWN, "topology recovery manager is shutting down");
        if (requestCoordinatorId != coordinatorId_) {
            decision.result = TopologyRecoveryReportResult::COORDINATOR_ID_MISMATCH;
            return Status::OK();
        }
    }
    RETURN_IF_NOT_OK(ValidateEvidence(clusterName, report));
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(!stopping_, K_SHUTTING_DOWN, "topology recovery manager is shutting down");
        auto found = contexts_.find(clusterName);
        if (found == contexts_.end()) {
            decision.result = TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY;
            return Status::OK();
        }
        auto *context = found->second.get();
        decision.state = context->state;
        if (context->observedMembers.count(report.reporterAddress) == 0) {
            CHECK_FAIL_RETURN_STATUS(context->observedMembers.size() < options_.maxMembersPerCluster, K_TRY_AGAIN,
                                     "topology recovery member admission limit reached");
            decision.result = TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY;
            return Status::OK();
        }
        if (context->state == TopologyRecoveryState::READY) {
            return Status::OK();
        }
    }
    VLOG(1) << "CLUSTER_RECOVERY_CANDIDATE cluster=" << clusterName << ", coordinator_id="
            << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE) << ", reporter=" << report.reporterAddress
            << ", version=" << report.topologyVersion << ", has_snapshot=" << report.hasSnapshot
            << ", payload_bytes=" << report.canonicalTopology.size();
    if (report.canonicalTopology.empty()) {
        return RecordEvidence(clusterName, std::move(report), decision);
    }
    return SubmitPayload(clusterName, std::move(report), decision);
}

Status TopologyRecoveryManager::RecordEvidence(const std::string &clusterName,
                                               TopologyRecoveryCandidateReport report,
                                               TopologyRecoveryReportDecision &decision)
{
    bool schedule = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(!stopping_, K_SHUTTING_DOWN, "topology recovery manager is shutting down");
        auto found = contexts_.find(clusterName);
        CHECK_FAIL_RETURN_STATUS(found != contexts_.end(), K_TRY_AGAIN, "recovery context disappeared");
        RETURN_IF_NOT_OK(UpdateEvidenceLocked(*found->second, std::move(report), decision, schedule));
    }
    if (schedule) {
        ScheduleReconcile(clusterName);
    }
    return Status::OK();
}

Status TopologyRecoveryManager::UpdateEvidenceLocked(ClusterRecoveryContext &context,
                                                     TopologyRecoveryCandidateReport report,
                                                     TopologyRecoveryReportDecision &decision, bool &schedule)
{
    if (context.observedMembers.count(report.reporterAddress) == 0) {
        decision.result = TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY;
        return Status::OK();
    }
    if (context.state == TopologyRecoveryState::READY) {
        decision.state = context.state;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(context.state != TopologyRecoveryState::INSTALLING, K_TRY_AGAIN,
                             "topology candidate installation is in progress");
    const auto now = clock_->Now();
    auto old = context.reporterEvidence.find(report.reporterAddress);
    if (context.state == TopologyRecoveryState::BLOCKED && old != context.reporterEvidence.end()
        && SameEvidence(old->second, report)) {
        decision.state = context.state;
        return Status::OK();
    }
    if (context.state == TopologyRecoveryState::BLOCKED) {
        context.state = TopologyRecoveryState::RECOVERING;
        context.discoveryDeadline.reset();
    }
    if (context.discoveryDeadline.has_value() && now >= *context.discoveryDeadline
        && (old == context.reporterEvidence.end() || !SameEvidence(old->second, report))) {
        decision.state = context.state;
        schedule = true;
        return Status::OK();
    }
    const bool requested = old != context.reporterEvidence.end() && SameEvidence(old->second, report)
                           && old->second.payloadRequested;
    context.reporterEvidence[report.reporterAddress] = TopologyCandidateEvidence{
        report.hasSnapshot, report.topologyVersion, std::move(report.canonicalDigest), requested
    };
    if (!context.discoveryDeadline.has_value()) {
        context.discoveryDeadline = now + options_.discoveryWindow;
    }
    const auto selection = SelectHighestCandidate(context.reporterEvidence);
    RefreshSelection(context);
    auto &evidence = context.reporterEvidence.at(report.reporterAddress);
    if (evidence.hasSnapshot) {
        const bool alreadyRequested =
            HasPeerPayloadRequest(context.reporterEvidence, report.reporterAddress, selection);
        evidence.payloadRequested = evidence.topologyVersion == selection.highestVersion
                                    && evidence.canonicalDigest == selection.highestDigest
                                    && !selection.conflictingHighest && !alreadyRequested;
        decision.payloadRequired = evidence.payloadRequested
                                   && !context.payloadValidationPending
                                   && (context.selectedCanonicalTopology == nullptr
                                       || context.selectedVersion != evidence.topologyVersion
                                       || context.selectedCanonicalDigest != evidence.canonicalDigest);
    }
    decision.state = context.state;
    schedule = true;
    return Status::OK();
}

Status TopologyRecoveryManager::SubmitPayload(const std::string &clusterName,
                                              TopologyRecoveryCandidateReport report,
                                              TopologyRecoveryReportDecision &decision)
{
    const auto traceContext = GetRecoveryTraceContext();
    const size_t payloadBytes = report.canonicalTopology.size();
    std::future<Status> result;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(!stopping_, K_SHUTTING_DOWN, "topology recovery manager is shutting down");
        auto found = contexts_.find(clusterName);
        CHECK_FAIL_RETURN_STATUS(found != contexts_.end(), K_TRY_AGAIN, "recovery context disappeared");
        auto &context = *found->second;
        CHECK_FAIL_RETURN_STATUS(context.state != TopologyRecoveryState::INSTALLING, K_TRY_AGAIN,
                                 "topology candidate installation is in progress");
        auto evidence = context.reporterEvidence.find(report.reporterAddress);
        CHECK_FAIL_RETURN_STATUS(evidence != context.reporterEvidence.end()
                                     && SameEvidence(evidence->second, report) && evidence->second.payloadRequested,
                                 K_INVALID, "candidate payload was not requested for this evidence");
        CHECK_FAIL_RETURN_STATUS(!context.payloadValidationPending, K_TRY_AGAIN,
                                 "candidate payload validation is already in progress");
        const size_t usedBytes = admittedReportBytes_ + retainedCandidateBytes_;
        CHECK_FAIL_RETURN_STATUS(pendingRecoveryWork_ < options_.maxPendingRecoveryWork
                                     && HasCapacity(usedBytes, payloadBytes, options_.maxCandidateMemoryBytes),
                                 K_TRY_AGAIN, "topology recovery payload admission limit reached");
        ++pendingRecoveryWork_;
        admittedReportBytes_ += payloadBytes;
        context.payloadValidationPending = true;
        try {
            result = recoveryPool_->Submit(
                [this, clusterName, payloadBytes, report = std::move(report), traceContext]() mutable {
                    TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
                    Status status;
                    try {
                        status = ValidatePayload(report);
                    } catch (const std::exception &error) {
                        status = Status(K_RUNTIME_ERROR, std::string("candidate validation failed: ") + error.what());
                    } catch (...) {
                        status = Status(K_RUNTIME_ERROR, "candidate validation failed with an unknown exception");
                    }
                    if (status.IsOk()) {
                        return RecordPayload(clusterName, std::move(report));
                    }
                    return RejectPayload(clusterName, report, payloadBytes, status);
                });
        } catch (const std::exception &error) {
            --pendingRecoveryWork_;
            admittedReportBytes_ -= payloadBytes;
            context.payloadValidationPending = false;
            RETURN_STATUS(K_TRY_AGAIN, std::string("submit candidate validation failed: ") + error.what());
        }
    }
    RETURN_IF_NOT_OK(AwaitPayloadValidation(result, options_.validationWaitTimeout));
    decision.state = GetState(clusterName);
    decision.payloadRequired = false;
    return Status::OK();
}

Status TopologyRecoveryManager::RecordPayload(const std::string &clusterName,
                                              TopologyRecoveryCandidateReport report)
{
    const size_t payloadBytes = report.canonicalTopology.size();
    bool schedule = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        --pendingRecoveryWork_;
        admittedReportBytes_ -= payloadBytes;
        auto found = contexts_.find(clusterName);
        if (found == contexts_.end()) {
            RETURN_STATUS(K_TRY_AGAIN, "recovery context disappeared during payload validation");
        }
        auto &context = *found->second;
        context.payloadValidationPending = false;
        CHECK_FAIL_RETURN_STATUS(context.state != TopologyRecoveryState::INSTALLING, K_TRY_AGAIN,
                                 "topology candidate installation already started");
        auto evidence = context.reporterEvidence.find(report.reporterAddress);
        if (context.observedMembers.count(report.reporterAddress) == 0
            || evidence == context.reporterEvidence.end()) {
            RETURN_STATUS(K_TRY_AGAIN, "candidate membership disappeared during payload validation");
        }
        CHECK_FAIL_RETURN_STATUS(SameEvidence(evidence->second, report) && evidence->second.payloadRequested,
                                 K_TRY_AGAIN, "candidate evidence changed during payload validation");
        RefreshSelection(context);
        const auto selection = SelectHighestCandidate(context.reporterEvidence);
        if (report.topologyVersion == selection.highestVersion && !selection.conflictingHighest
            && selection.highestDigest == report.canonicalDigest) {
            evidence->second.payloadRequested = false;
            ReleaseSelectedPayload(context);
            context.selectedCanonicalTopology =
                std::make_shared<const std::string>(std::move(report.canonicalTopology));
            context.selectedCanonicalDigest = std::move(report.canonicalDigest);
            context.selectedVersion = report.topologyVersion;
            retainedCandidateBytes_ += payloadBytes;
            VLOG(1) << "CLUSTER_RECOVERY_PAYLOAD_ACCEPTED cluster=" << clusterName
                    << ", reporter=" << report.reporterAddress << ", version=" << report.topologyVersion
                    << ", payload_bytes=" << payloadBytes;
            schedule = true;
        }
    }
    if (schedule) {
        ScheduleReconcile(clusterName);
    }
    return Status::OK();
}

Status TopologyRecoveryManager::RejectPayload(const std::string &clusterName,
                                              const TopologyRecoveryCandidateReport &report,
                                              size_t payloadBytes, const Status &validationStatus)
{
    std::lock_guard<std::mutex> lock(mutex_);
    --pendingRecoveryWork_;
    admittedReportBytes_ -= payloadBytes;
    auto found = contexts_.find(clusterName);
    if (found == contexts_.end()) {
        return validationStatus;
    }
    auto &context = *found->second;
    context.payloadValidationPending = false;
    if (validationStatus.GetCode() != K_INVALID) {
        LOG(WARNING) << "CLUSTER_RECOVERY_PAYLOAD_RETRY cluster=" << clusterName
                     << ", reporter=" << report.reporterAddress << ", version=" << report.topologyVersion
                     << ", status=" << validationStatus.ToString();
        return validationStatus;
    }
    auto evidence = context.reporterEvidence.find(report.reporterAddress);
    if (context.state != TopologyRecoveryState::INSTALLING && evidence != context.reporterEvidence.end()
        && SameEvidence(evidence->second, report) && evidence->second.payloadRequested) {
        context.state = TopologyRecoveryState::BLOCKED;
        ReleaseSelectedPayload(context);
        LOG(WARNING) << "CLUSTER_RECOVERY_INVALID_HIGHEST_CANDIDATE, cluster=" << clusterName
                     << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                     << ", reporter=" << report.reporterAddress << ", version=" << report.topologyVersion
                     << ", status=" << validationStatus.ToString();
    }
    return validationStatus;
}

void TopologyRecoveryManager::ReleaseSelectedPayload(ClusterRecoveryContext &context)
{
    if (context.selectedCanonicalTopology != nullptr) {
        retainedCandidateBytes_ -= context.selectedCanonicalTopology->size();
    }
    context.selectedCanonicalTopology.reset();
    context.selectedCanonicalDigest.clear();
    context.selectedVersion = 0;
}

void TopologyRecoveryManager::RefreshSelection(ClusterRecoveryContext &context)
{
    const auto selection = SelectHighestCandidate(context.reporterEvidence);
    const bool payloadStillSelected = selection.hasSnapshot && !selection.conflictingHighest
                                      && context.selectedVersion == selection.highestVersion
                                      && context.selectedCanonicalDigest == selection.highestDigest;
    if (!payloadStillSelected) {
        ReleaseSelectedPayload(context);
    }
}

void TopologyRecoveryManager::ScheduleReconcile(const std::string &clusterName)
{
    const auto traceContext = GetRecoveryTraceContext();
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = contexts_.find(clusterName);
    if (stopping_ || found == contexts_.end() || found->second->state != TopologyRecoveryState::RECOVERING
        || found->second->reconcileQueued
        || pendingRecoveryWork_ >= options_.maxPendingRecoveryWork) {
        return;
    }
    found->second->reconcileQueued = true;
    ++pendingRecoveryWork_;
    bool accepted = false;
    try {
        accepted = recoveryPool_->ExecuteNoWait([this, clusterName, traceContext] {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            try {
                auto status = MaybeFinalize(clusterName);
                if (status.IsError()) {
                    LOG(WARNING) << "CLUSTER_RECOVERY_RECONCILE_FAILED, cluster=" << clusterName
                                 << ", status=" << status.ToString();
                }
            } catch (const std::exception &error) {
                LOG(ERROR) << "CLUSTER_RECOVERY_RECONCILE_EXCEPTION, cluster=" << clusterName
                           << ", error=" << error.what();
            } catch (...) {
                LOG(ERROR) << "CLUSTER_RECOVERY_RECONCILE_EXCEPTION, cluster=" << clusterName
                           << ", unknown error";
            }
            std::lock_guard<std::mutex> finishLock(mutex_);
            --pendingRecoveryWork_;
            auto current = contexts_.find(clusterName);
            if (current != contexts_.end()) {
                current->second->reconcileQueued = false;
            }
        });
    } catch (const std::exception &error) {
        LOG(WARNING) << "CLUSTER_RECOVERY_RECONCILE_SUBMIT_FAILED, cluster=" << clusterName
                     << ", error=" << error.what();
    }
    if (!accepted) {
        --pendingRecoveryWork_;
        found->second->reconcileQueued = false;
    }
}

Status TopologyRecoveryManager::MaybeFinalize(const std::string &clusterName)
{
    std::shared_ptr<const std::string> payload;
    uint64_t version = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        RETURN_IF_NOT_OK(PrepareInstallationLocked(clusterName, payload, version));
    }
    if (version == 0) {
        return Status::OK();
    }
    Status installStatus;
    try {
        installStatus = InstallSelected(clusterName, version, *payload);
    } catch (const std::exception &error) {
        installStatus = Status(K_RUNTIME_ERROR, std::string("topology installation failed: ") + error.what());
    } catch (...) {
        installStatus = Status(K_RUNTIME_ERROR, "topology installation failed with an unknown exception");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    CompleteInstallationLocked(clusterName, version, installStatus);
    return installStatus;
}

Status TopologyRecoveryManager::PrepareInstallationLocked(const std::string &clusterName,
                                                          std::shared_ptr<const std::string> &payload,
                                                          uint64_t &version)
{
    auto found = contexts_.find(clusterName);
    if (found == contexts_.end() || found->second->state != TopologyRecoveryState::RECOVERING) {
        return Status::OK();
    }
    auto &context = *found->second;
    if (!context.discoveryDeadline.has_value() || clock_->Now() < *context.discoveryDeadline) {
        return Status::OK();
    }
    const auto selected = SelectHighestCandidate(context.reporterEvidence);
    if (selected.hasSnapshot && selected.conflictingHighest) {
        context.state = TopologyRecoveryState::BLOCKED;
        ReleaseSelectedPayload(context);
        LOG(WARNING) << "CLUSTER_RECOVERY_CONFLICT, cluster=" << clusterName
                     << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                     << ", version=" << selected.highestVersion << ", members=" << context.observedMembers.size()
                     << ", evidence=" << context.reporterEvidence.size();
        return Status::OK();
    }
    if (!selected.hasSnapshot && !context.reporterEvidence.empty()) {
        LOG(INFO) << "CLUSTER_RECOVERY_READY_NO_SNAPSHOT, cluster=" << clusterName << ", coordinator_id="
                  << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << ", members=" << context.observedMembers.size()
                  << ", evidence=" << context.reporterEvidence.size();
        context.state = TopologyRecoveryState::READY;
        context.reporterEvidence.clear();
        context.discoveryDeadline.reset();
        ReleaseSelectedPayload(context);
        return Status::OK();
    }
    if (context.selectedCanonicalTopology == nullptr || context.selectedVersion != selected.highestVersion
        || selected.conflictingHighest || context.selectedCanonicalDigest != selected.highestDigest) {
        return Status::OK();
    }
    context.state = TopologyRecoveryState::INSTALLING;
    payload = context.selectedCanonicalTopology;
    version = selected.highestVersion;
    LOG(INFO) << "CLUSTER_RECOVERY_STATE cluster=" << clusterName << ", coordinator_id="
              << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
              << ", old=RECOVERING, new=INSTALLING, version=" << version
              << ", evidence=" << context.reporterEvidence.size();
    return Status::OK();
}

void TopologyRecoveryManager::CompleteInstallationLocked(const std::string &clusterName, uint64_t version,
                                                         const Status &installStatus)
{
    auto found = contexts_.find(clusterName);
    if (found == contexts_.end() || found->second->state != TopologyRecoveryState::INSTALLING
        || found->second->selectedVersion != version) {
        return;
    }
    auto &context = *found->second;
    if (installStatus.IsOk()) {
        context.state = TopologyRecoveryState::READY;
        context.reporterEvidence.clear();
        context.discoveryDeadline.reset();
        LOG(INFO) << "CLUSTER_RECOVERY_READY, cluster=" << clusterName << ", version=" << version
                  << ", digest=" << context.selectedCanonicalDigest.substr(0, DIGEST_LOG_PREFIX_SIZE)
                  << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << ", members=" << context.observedMembers.size()
                  << ", evidence=" << context.reporterEvidence.size();
        ReleaseSelectedPayload(context);
    } else if (installStatus.GetCode() == K_INVALID) {
        context.state = TopologyRecoveryState::BLOCKED;
        LOG(WARNING) << "CLUSTER_RECOVERY_INSTALL_BLOCKED, cluster=" << clusterName
                     << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                     << ", version=" << version << ", status=" << installStatus.ToString();
    } else {
        context.state = TopologyRecoveryState::RECOVERING;
        LOG(WARNING) << "CLUSTER_RECOVERY_INSTALL_RETRY, cluster=" << clusterName
                     << ", coordinator_id=" << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                     << ", version=" << version << ", status=" << installStatus.ToString();
        RefreshSelection(context);
        if (context.reporterEvidence.empty()) {
            context.discoveryDeadline.reset();
        }
    }
}

Status TopologyRecoveryManager::InstallSelected(const std::string &clusterName, uint64_t version,
                                                const std::string &canonicalTopology)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
    const std::string physicalKey = keys->TopologyTable() + "/" + cluster::TopologyKeyHelper::TopologyKey();
    int64_t storedVersion = 0;
    int64_t revision = 0;
    VLOG(1) << "CLUSTER_RECOVERY_INSTALL, cluster=" << clusterName << ", coordinator_id="
            << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE) << ", version=" << version;
    return store_.Put(physicalKey, canonicalTopology, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, storedVersion, revision);
}

TopologyRecoveryState TopologyRecoveryManager::GetState(const std::string &clusterName) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = contexts_.find(clusterName);
    return found == contexts_.end() ? TopologyRecoveryState::RECOVERING : found->second->state;
}

Status TopologyRecoveryManager::Shutdown()
{
    std::unique_ptr<ThreadPool> pool;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (shutdownComplete_) {
            return Status::OK();
        }
        if (stopping_) {
            shutdownCv_.wait(lock, [this] { return shutdownComplete_; });
            return Status::OK();
        }
        stopping_ = true;
        LOG(INFO) << "CLUSTER_RECOVERY_SHUTDOWN state=draining, coordinator_id="
                  << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE)
                  << ", clusters=" << contexts_.size() << ", pending_work=" << pendingRecoveryWork_;
        pool = std::move(recoveryPool_);
    }
    pool.reset();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdownComplete_ = true;
    }
    shutdownCv_.notify_all();
    LOG(INFO) << "CLUSTER_RECOVERY_SHUTDOWN state=complete, coordinator_id="
              << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE);
    return Status::OK();
}

}  // namespace datasystem::coordinator
