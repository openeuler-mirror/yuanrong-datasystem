/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Multi-instance CAS cluster topology Controller.
 */
#include "datasystem/cluster/control/topology_controller.h"

#include <algorithm>
#include <exception>
#include <iterator>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem::cluster {
namespace {
constexpr int32_t CONTROLLER_READ_TIMEOUT_MS = 3'000;
constexpr size_t MAX_DOORBELLS_PER_RECONCILE = 1'024;
constexpr uint32_t MAX_RECONCILE_BACKOFF_SHIFT = 5;
constexpr size_t FIRST_DRAINED_EVENT_COUNT = 1;
constexpr uint64_t BACKOFF_SHIFT_BASE = 1;
constexpr int TOPOLOGY_WATCH_EVENT_LOG_INTERVAL = 1'024;
constexpr int TOPOLOGY_RECONCILE_LOG_INTERVAL = 128;
constexpr int64_t DERIVED_SLICE_WARN_THRESHOLD_MS = 20;
constexpr size_t MAX_EXTERNAL_BOOTSTRAP_ATTEMPTS = 8;
constexpr auto ACTIVE_FAILURE_DIRECT_PROBE_TIMEOUT = std::chrono::milliseconds(250);
constexpr auto ACTIVE_FAILURE_DIRECT_PROBE_INTERVAL = std::chrono::milliseconds(750);
constexpr uint32_t MIN_ACTIVE_FAILURE_UNREACHABLE_PROBES = 2;
constexpr size_t MAX_ACTIVE_FAILURE_PROBES_PER_ROUND = 128;
constexpr size_t TWO_WORKER_CLUSTER_SIZE = 2;

void CollectTopologyMemberships(const std::vector<MembershipRecord> &memberships,
                                const TopologySnapshot &topology,
                                std::vector<MembershipRecord> &scoped)
{
    auto memberIter = topology.Members().begin();
    for (const auto &record : memberships) {
        while (memberIter != topology.Members().end() && memberIter->identity.address < record.address) {
            ++memberIter;
        }
        if (memberIter != topology.Members().end() && memberIter->identity.address == record.address) {
            scoped.push_back(record);
        }
    }
}

std::string MembershipDigest(const std::vector<MembershipRecord> &memberships,
                             const TopologySnapshot *topologyScope = nullptr)
{
    std::string seed;
    size_t memberIndex = 0;
    for (const auto &record : memberships) {
        if (topologyScope != nullptr) {
            const auto &topologyMembers = topologyScope->Members();
            while (memberIndex < topologyMembers.size()
                   && topologyMembers[memberIndex].identity.address < record.address) {
                ++memberIndex;
            }
            if (memberIndex == topologyMembers.size()
                || topologyMembers[memberIndex].identity.address != record.address) {
                continue;
            }
        }
        seed.append(record.address).push_back('\0');
        seed.append(std::to_string(static_cast<uint32_t>(record.state))).push_back('\0');
        seed.append(std::to_string(record.timestamp)).push_back('\0');
        seed.append(record.hostId).push_back('\0');
    }
    std::string digest;
    if (Hasher().GetSha256Hex(seed, digest).IsError()) {
        // Retain collision-free change detection even if the diagnostic hash implementation is unavailable.
        return seed;
    }
    return digest;
}

bool IsTransientReconcileStatus(StatusCode code)
{
    return code == K_TRY_AGAIN || code == K_NOT_READY;
}

const char *ControlBackendProbeOutcomeName(ControlBackendProbeOutcome outcome)
{
    switch (outcome) {
        case ControlBackendProbeOutcome::RESPONSE:
            return "response";
        case ControlBackendProbeOutcome::DEADLINE_EXCEEDED:
            return "deadline_exceeded";
        case ControlBackendProbeOutcome::UNAVAILABLE:
            return "unavailable";
        case ControlBackendProbeOutcome::CANCELLED:
            return "cancelled";
        case ControlBackendProbeOutcome::ERROR:
            return "error";
    }
    return "error";
}

const Member *FailureProbeTargetOwnedBy(const TopologySnapshot &latest, const std::string &localAddress)
{
    const auto &committed = latest.CommittedMembers();
    const auto local = std::find_if(committed.begin(), committed.end(),
                                    [&](const Member *member) { return member->identity.address == localAddress; });
    if (local == committed.end()) {
        return nullptr;
    }
    // A target's canonical successor is its only reporter; viewed locally, that target is our predecessor.
    const auto localIndex = static_cast<size_t>(std::distance(committed.begin(), local));
    return committed[(localIndex + committed.size() - 1) % committed.size()];
}

Status BuildMemberId(const MembershipRecord &membership, std::string &memberId)
{
    const auto seed = std::to_string(membership.address.size()) + ":" + membership.address + ":"
                      + std::to_string(membership.timestamp) + ":" + membership.hostId;
    std::unique_ptr<unsigned char[]> digest;
    unsigned int digestSize = 0;
    RETURN_IF_NOT_OK(Hasher().HashSHA256(seed.data(), seed.size(), digest, digestSize));
    CHECK_FAIL_RETURN_STATUS(digest != nullptr && digestSize >= UUID_SIZE, K_RUNTIME_ERROR,
                             "membership generation digest is too short");
    memberId.assign(reinterpret_cast<const char *>(digest.get()), UUID_SIZE);
    return Status::OK();
}

bool TaskFinished(const TopologyTask &task)
{
    return std::visit(
        [](const auto &value) {
            const auto &ranges = [&]() -> const std::vector<TopologyTaskRange> & {
                if constexpr (std::is_same_v<std::decay_t<decltype(value)>, TopologyMigrateTask>) {
                    return value.sourceRanges;
                } else {
                    return value.recoveryRanges;
                }
            }();
            return std::all_of(ranges.begin(), ranges.end(), [](const auto &range) { return range.finished; });
        },
        task);
}

std::string TaskId(const TopologyTask &task)
{
    return std::visit([](const auto &value) { return value.taskId; }, task);
}

void LimitMembers(std::vector<MemberIdentity> &members, size_t limit)
{
    std::sort(members.begin(), members.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    if (members.size() > limit) {
        members.resize(limit);
    }
}

void EraseMembers(TopologyState &state, const std::vector<MemberIdentity> &identities)
{
    std::unordered_set<std::string> addresses;
    for (const auto &identity : identities) {
        addresses.insert(identity.address);
    }
    state.members.erase(
        std::remove_if(state.members.begin(), state.members.end(),
                       [&](const auto &member) { return addresses.count(member.identity.address) > 0; }),
        state.members.end());
}

bool AllMembersExiting(const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships)
{
    if (latest.Members().empty()) {
        return false;
    }
    std::unordered_map<std::string, MemberLifecycleState> lifecycleByAddress;
    lifecycleByAddress.reserve(memberships.size());
    for (const auto &record : memberships) {
        lifecycleByAddress.emplace(record.address, record.state);
    }
    return std::all_of(latest.Members().begin(), latest.Members().end(), [&](const auto &member) {
        const auto lifecycle = lifecycleByAddress.find(member.identity.address);
        return lifecycle != lifecycleByAddress.end() && lifecycle->second == MemberLifecycleState::EXITING;
    });
}

bool IsCollectiveCommittedMembershipMissing(const TopologySnapshot &latest,
                                            const std::vector<MembershipRecord> &memberships)
{
    const auto &committed = latest.CommittedMembers();
    if (committed.empty()) {
        return false;
    }
    for (const auto &membership : memberships) {
        const Member *member = nullptr;
        if (latest.FindMemberByAddress(membership.address, member).IsOk() && member != nullptr
            && (member->state == MemberState::ACTIVE || member->state == MemberState::PRE_LEAVING
                || member->state == MemberState::LEAVING)) {
            return false;
        }
    }
    return true;
}

constexpr size_t COLLECTIVE_PROBE_SAMPLE_COUNT = 5;
static_assert(COLLECTIVE_PROBE_SAMPLE_COUNT > 1, "collective probe sampling requires at least two samples");
constexpr char COORDINATOR_COLLECTIVE_PROBE_OWNER[] = "coordinator";

void LogMemberTransition(const std::string &clusterName, const char *action, size_t count,
                         const std::vector<MemberIdentity> &members, uint64_t committedVersion)
{
    LOG(INFO) << "CLUSTER_MEMBER_TRANSITION cluster=" << clusterName << " action=" << action << " count=" << count
              << " sample=" << MemberIdentitySample(members) << " committed_version=" << committedVersion;
}

void LogDirectFailureConfirmation(const std::string &clusterName, uint64_t version,
                                  const std::vector<MemberAbsenceObservation> &observations,
                                  std::chrono::seconds nodeDeadTimeout)
{
    std::vector<MemberIdentity> members;
    members.reserve(observations.size());
    int64_t maximumMissingMs = 0;
    for (const auto &observation : observations) {
        members.emplace_back(observation.identity);
        maximumMissingMs = std::max(maximumMissingMs, observation.missingMs);
    }
    LOG_FIRST_AND_EVERY_N(WARNING, TOPOLOGY_RECONCILE_LOG_INTERVAL)
        << "CLUSTER_FAILURE_DETECT cluster=" << clusterName << " version=" << version
        << " action=absence_timeout_direct confirmed_count=" << observations.size()
        << " sample=" << MemberIdentitySample(members) << " maximum_missing_ms=" << maximumMissingMs
        << " node_dead_timeout_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(nodeDeadTimeout).count();
}
}  // namespace

bool TopologyControllerOptions::IsValid() const noexcept
{
    return nodeDeadTimeout.count() >= 0 && failureBatchWindow.count() > 0 && ordinaryBatchWindow.count() > 0
           && reconcileTick.count() > 0 && failureProbeTimeout.count() > 0
           && witnessProbeRoundTimeout > failureProbeTimeout && failureProbeWitnessCount > 0 && initialProbeRound > 0
           && maxDerivedOperationsPerTick > 0 && maxMembersPerBatch > 0 && maxProgressReadsPerTick > 0
           && derivedSliceBudget.count() > 0 && now && scaleOutCollectWindow.count() >= 0
           && scaleOutCollectWindow.count() <= MAX_SCALE_OUT_COLLECT_WINDOW_MS && scaleInCollectWindow.count() >= 0
           && scaleInCollectWindow.count() <= MAX_SCALE_IN_COLLECT_WINDOW_MS
           && (!memberLivenessProbe || !localAddress.empty() || eventSourceMode == TopologyEventSourceMode::EXTERNAL)
           && static_cast<bool>(collectiveControlEpoch) == static_cast<bool>(collectiveReplacementFence)
           && static_cast<bool>(failureSummaryCandidateProvider) == static_cast<bool>(activeFailureCommitFence)
           && (eventSourceMode != TopologyEventSourceMode::EXTERNAL || !probeEpoch.empty())
           && (eventSourceMode == TopologyEventSourceMode::SELF_MANAGED
               || eventSourceMode == TopologyEventSourceMode::EXTERNAL
               || eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD);
}

TopologyController::TopologyController(ICoordinationBackend &backend, TopologyRepository &repository,
                                       const TopologyKeyHelper &keys, const IPlanningAlgorithm &algorithm,
                                       CoordinationEventDispatcher &dispatcher, TopologyControllerOptions options)
    : backend_(backend),
      repository_(repository),
      keys_(keys),
      algorithm_(algorithm),
      options_(options),
      planBuilder_(algorithm),
      failureClassifier_(options.nodeDeadTimeout),
      dispatcher_(dispatcher),
      nextProbeRound_(options.initialProbeRound)
{
}

TopologyController::~TopologyController()
{
    LOG_IF_ERROR(Stop(std::chrono::steady_clock::time_point::max()),
                 "Stop cluster topology Controller during destruction");
}

Status TopologyController::Start()
{
    std::unique_lock<std::mutex> lock(stateMutex_);
    CHECK_FAIL_RETURN_STATUS(!started_ && options_.IsValid(), K_INVALID,
                             "invalid or already started topology Controller");
    std::vector<WatchKey> watches;
    if (options_.eventSourceMode == TopologyEventSourceMode::SELF_MANAGED) {
        RETURN_IF_NOT_OK(BuildTopologyRoleWatchPlan(TopologyRuntimeRole::CONTROLLER, "", keys_, 0, watches));
    }
    RETURN_IF_NOT_OK(PrepareMembershipRestartObservation());
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD) {
        RETURN_IF_NOT_OK(ResyncExternalFacts());
        bootstrapRevision_ = membershipRevisionFloor_;
    }
    RETURN_IF_NOT_OK(dispatcher_.Start());
    if (options_.eventSourceMode == TopologyEventSourceMode::SELF_MANAGED) {
        backend_.SetEventHandler(
            [this](CoordinationEvent &&event) { (void)EnqueueCoordinationEvent(std::move(event)); });
        auto rc = backend_.WatchEvents(watches);
        if (rc.IsError()) {
            dispatcher_.ShutdownIngress();
            LOG_IF_ERROR(backend_.ShutdownEventSources(),
                         "Shut down topology Controller event sources after Start failure");
            backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
            return rc;
        }
    }
    LOG(INFO) << "CLUSTER_WATCH cluster=" << keys_.ClusterName() << " role=controller scope_count=" << watches.size()
              << " revision=" << membershipRevisionFloor_ << " status="
              << (options_.eventSourceMode == TopologyEventSourceMode::SELF_MANAGED ? "registered" : "external");
    started_ = true;
    stopping_ = false;
    threadExited_ = false;
    diagnostics_.running = true;
    try {
        stateThread_ = Thread(&TopologyController::Run, this);
        stateThread_.set_name("cluster-ctrl");
    } catch (const std::exception &error) {
        stopping_ = true;
        dispatcher_.ShutdownIngress();
        lock.unlock();
        if (options_.eventSourceMode == TopologyEventSourceMode::SELF_MANAGED) {
            LOG_IF_ERROR(backend_.ShutdownEventSources(),
                         "Shut down topology Controller event sources after thread Start failure");
            backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
        }
        if (stateThread_.joinable()) {
            stateThread_.join();
        }
        lock.lock();
        started_ = false;
        stopping_ = false;
        threadExited_ = true;
        diagnostics_.running = false;
        stoppedCv_.notify_all();
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start topology Controller failed: ") + error.what());
    }
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << keys_.ClusterName() << " role=controller state=ready";
    return Status::OK();
}

Status TopologyController::Stop(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(stateMutex_);
    if (!started_) {
        return Status::OK();
    }
    stopping_ = true;
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << keys_.ClusterName() << " role=controller state=stopping";
    dispatcher_.ShutdownIngress();
    Status eventSourceStatus;
    if (options_.eventSourceMode == TopologyEventSourceMode::SELF_MANAGED) {
        eventSourceStatus = backend_.ShutdownEventSources();
        backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
    }
    if (!stoppedCv_.wait_until(lock, deadline, [this] { return threadExited_; })) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "topology Controller stop deadline exceeded");
    }
    lock.unlock();
    if (stateThread_.joinable()) {
        stateThread_.join();
    }
    lock.lock();
    started_ = false;
    stopping_ = false;
    if (eventSourceStatus.IsError()) {
        return eventSourceStatus;
    }
    return Status::OK();
}

Status TopologyController::EnqueueCoordinationEvent(CoordinationEvent &&event)
{
    LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
        << "CLUSTER_WATCH_EVENT cluster=" << keys_.ClusterName() << " role=controller event=" << event.ToString();
    LOG_IF_ERROR(ObserveMembershipRestart(event), "Failed to observe membership restart event");
    auto rc = dispatcher_.SubmitCoordination(std::move(event));
    if (rc.IsError()) {
        LOG(WARNING) << "CLUSTER_WATCH_QUEUE cluster=" << keys_.ClusterName()
                     << " role=controller action=submit_failed status=" << rc.ToString();
    }
    return rc;
}

Status TopologyController::SubmitCoordinationEvent(CoordinationEvent &&event)
{
    CHECK_FAIL_RETURN_STATUS(options_.eventSourceMode != TopologyEventSourceMode::SELF_MANAGED, K_INVALID,
                             "topology Controller does not accept an external event source");
    return EnqueueCoordinationEvent(std::move(event));
}

Status TopologyController::SubmitWorkerLivenessReport(WorkerLivenessReport report)
{
    CHECK_FAIL_RETURN_STATUS(options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL, K_INVALID,
                             "worker liveness reports require Coordinator event-source mode");
    return dispatcher_.SubmitWorkerLiveness(std::move(report));
}

int64_t TopologyController::GetBootstrapRevision() const noexcept
{
    return bootstrapRevision_;
}

Status TopologyController::PrepareMembershipRestartObservation()
{
    if (options_.eventSourceMode != TopologyEventSourceMode::EXTERNAL_ETCD
        && options_.membershipRestartHandler == nullptr) {
        return Status::OK();
    }
    std::string eventPrefix;
    RETURN_IF_NOT_OK(backend_.GetStorePrefix(keys_.MembershipTable(), eventPrefix));
    if (eventPrefix.empty() || eventPrefix.back() != '/') {
        eventPrefix.push_back('/');
    }
    std::lock_guard<std::mutex> lock(membershipRestartMutex_);
    membershipEventPrefix_ = std::move(eventPrefix);
    if (options_.membershipRestartHandler != nullptr) {
        latestRestartTimestampByAddress_.clear();
        pendingRestartTimestampByAddress_.clear();
    }
    return Status::OK();
}

Status TopologyController::ObserveMembershipRestart(const CoordinationEvent &event)
{
    if (event.type != CoordinationEventType::PUT || options_.membershipRestartHandler == nullptr) {
        return Status::OK();
    }
    std::string eventPrefix;
    {
        std::lock_guard<std::mutex> lock(membershipRestartMutex_);
        eventPrefix = membershipEventPrefix_;
    }
    if (event.key.rfind(eventPrefix, 0) != 0) {
        return Status::OK();
    }
    const std::string address = event.key.substr(eventPrefix.size());
    std::string canonicalKey;
    RETURN_IF_NOT_OK(TopologyKeyHelper::MembershipKey(address, canonicalKey));
    CHECK_FAIL_RETURN_STATUS(canonicalKey == address, K_INVALID, "membership restart event key is not exact");
    MembershipValue value;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(event.value, value));
    if (value.lifecycleState != MemberLifecycleState::RESTARTING) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(value.timestamp > 0, K_INVALID, "membership restart timestamp is invalid");
    RecordMembershipRestart(address, value.timestamp);
    return Status::OK();
}

Status TopologyController::PublishExternalTopology(std::shared_ptr<const TopologySnapshot> candidate, bool fullRebuild)
{
    SnapshotUpdateOutcome outcome;
    auto rc = externalTopology_.Publish(candidate, outcome);
    if (rc.IsOk()) {
        return rc;
    }
    if (outcome == SnapshotUpdateOutcome::VERSION_GAP && fullRebuild) {
        return externalTopology_.PublishAfterFullRebuild(std::move(candidate));
    }
    externalResyncRequired_ = true;
    return rc;
}

Status TopologyController::ResyncExternalFacts()
{
    for (size_t attempt = 0; attempt < MAX_EXTERNAL_BOOTSTRAP_ATTEMPTS; ++attempt) {
        std::vector<MembershipRecord> memberships;
        int64_t membershipRevision = 0;
        RETURN_IF_NOT_OK(repository_.ReadMemberships(memberships, &membershipRevision));
        CHECK_FAIL_RETURN_STATUS(membershipRevision > 0, K_INVALID,
                                 "external membership read returned an invalid revision");
        if (membershipRevisionFloor_ > 0 && membershipRevision < membershipRevisionFloor_) {
            RETURN_STATUS(K_INVALID, "external membership revision rolled back");
        }

        TopologyReader reader(repository_);
        std::shared_ptr<const TopologySnapshot> topology;
        auto rc = reader.Read(CONTROLLER_READ_TIMEOUT_MS, topology);
        if (rc.GetCode() == K_NOT_FOUND) {
            CHECK_FAIL_RETURN_STATUS(membershipRevisionFloor_ == 0, K_INVALID,
                                     "external topology authority disappeared");
            RETURN_IF_NOT_OK(EnsureTopologyAuthority());
            continue;
        }
        RETURN_IF_NOT_OK(rc);
        if (topology->AuthorityRevision() > membershipRevision) {
            continue;
        }

        std::map<std::string, MembershipRecord> facts;
        for (const auto &membership : memberships) {
            facts.emplace(membership.address, membership);
        }
        const auto topologyRevision = topology->AuthorityRevision();
        RETURN_IF_NOT_OK(PublishExternalTopology(std::move(topology), true));
        externalMemberships_.swap(facts);
        membershipEventRevisionByAddress_.clear();
        membershipRevisionFloor_ = membershipRevision;
        topologyEventRevision_ = topologyRevision;
        externalResyncRequired_ = false;
        return Status::OK();
    }
    RETURN_STATUS(K_TRY_AGAIN, "external topology changed throughout the bounded fact rebuild");
}

Status TopologyController::ApplyExternalEvent(const CoordinationEvent &event)
{
    if (event.type == CoordinationEventType::RESET) {
        RETURN_STATUS(K_NOT_READY, "external watch reset requires an exact rebuild");
    }
    const auto kind = keys_.ClassifyPhysicalKey(event.key, "");
    if (kind == TopologyPhysicalKeyKind::TOPOLOGY) {
        if (event.type != CoordinationEventType::PUT || event.value.empty() || event.revision <= 0
            || event.version <= 0) {
            RETURN_STATUS(K_NOT_READY, "external topology event requires an exact rebuild");
        }
        if (event.revision <= topologyEventRevision_) {
            return Status::OK();
        }
        std::shared_ptr<const TopologySnapshot> candidate;
        RETURN_IF_NOT_OK(TopologyReader::BuildFromEncodedTopology(event.value, event.revision, candidate));
        RETURN_IF_NOT_OK(PublishExternalTopology(std::move(candidate), false));
        topologyEventRevision_ = event.revision;
        return Status::OK();
    }
    if (kind == TopologyPhysicalKeyKind::MEMBERSHIP) {
        CHECK_FAIL_RETURN_STATUS(event.key.rfind(membershipEventPrefix_, 0) == 0, K_INVALID,
                                 "external membership event prefix is invalid");
        const std::string address = event.key.substr(membershipEventPrefix_.size());
        std::string canonicalAddress;
        RETURN_IF_NOT_OK(TopologyKeyHelper::MembershipKey(address, canonicalAddress));
        CHECK_FAIL_RETURN_STATUS(canonicalAddress == address, K_INVALID, "external membership event key is not exact");
        if (event.revision <= 0) {
            RETURN_STATUS(K_NOT_READY, "revisionless membership event requires an exact rebuild");
        }
        const auto watermark = membershipEventRevisionByAddress_.find(address);
        const auto appliedRevision =
            watermark == membershipEventRevisionByAddress_.end() ? membershipRevisionFloor_ : watermark->second;
        if (event.revision <= appliedRevision) {
            return Status::OK();
        }
        if (event.type == CoordinationEventType::DELETE) {
            RETURN_STATUS(K_NOT_READY, "membership deletion requires an exact prefix rebuild");
        }
        if (event.type != CoordinationEventType::PUT || event.version <= 0) {
            RETURN_STATUS(K_NOT_READY, "external membership event requires an exact rebuild");
        }
        MembershipValue value;
        RETURN_IF_NOT_OK(MembershipValueCodec::Decode(event.value, value));
        externalMemberships_[address] =
            MembershipRecord{ address, value.lifecycleState, value.timestamp, value.hostId };
        membershipEventRevisionByAddress_[address] = event.revision;
        membershipDirty_ = true;
        return Status::OK();
    }
    if (kind == TopologyPhysicalKeyKind::MIGRATE_TASK || kind == TopologyPhysicalKeyKind::DELETE_TASK) {
        return Status::OK();
    }
    RETURN_STATUS(K_INVALID, "external Controller received an unknown watch key");
}

void TopologyController::ObserveMembershipRestarts(const std::vector<MembershipRecord> &memberships)
{
    if (options_.membershipRestartHandler == nullptr) {
        return;
    }
    for (const auto &record : memberships) {
        if (record.state == MemberLifecycleState::RESTARTING && record.timestamp > 0) {
            RecordMembershipRestart(record.address, record.timestamp);
        }
    }
}

void TopologyController::RecordMembershipRestart(const std::string &address, int64_t timestamp)
{
    std::lock_guard<std::mutex> lock(membershipRestartMutex_);
    auto [iter, inserted] = latestRestartTimestampByAddress_.emplace(address, timestamp);
    if (!inserted && timestamp <= iter->second) {
        return;
    }
    iter->second = timestamp;
    pendingRestartTimestampByAddress_[address] = timestamp;
}

void TopologyController::DrainMembershipRestarts()
{
    std::unordered_map<std::string, int64_t> pending;
    {
        std::lock_guard<std::mutex> lock(membershipRestartMutex_);
        pending.swap(pendingRestartTimestampByAddress_);
    }
    for (const auto &[address, timestamp] : pending) {
        auto rc = options_.membershipRestartHandler(address, timestamp);
        if (rc.IsError()) {
            LOG(WARNING) << "CLUSTER_RESTART_NOTIFY cluster=" << keys_.ClusterName()
                         << " action=deliver_failed address=" << address << " timestamp=" << timestamp
                         << " status=" << rc.ToString();
            std::lock_guard<std::mutex> lock(membershipRestartMutex_);
            auto latest = latestRestartTimestampByAddress_.find(address);
            if (latest != latestRestartTimestampByAddress_.end() && latest->second == timestamp) {
                pendingRestartTimestampByAddress_[address] = timestamp;
            }
        }
    }
}

void TopologyController::Run()
{
    bool initialReconcile = true;
    while (true) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID("cont-" + GetStringUuid());
        if (StopRequested()) {
            break;
        }
        const auto startedAt = std::chrono::steady_clock::now();
        size_t drained = 0;
        const bool immediate =
            initialReconcile || topologyCommittedThisTick_ || derivedWorkPending_ || progressWorkPending_;
        auto rc = WaitForReconcile(immediate, drained);
        initialReconcile = false;
        if (rc.IsError()) {
            if (StopRequested()) {
                break;
            }
            {
                std::lock_guard<std::mutex> lock(stateMutex_);
                diagnostics_.lastError = rc.ToString();
            }
            LOG(WARNING) << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName()
                         << " action=event_wait_failed status=" << rc.ToString();
            continue;
        }
        if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD && !externalEventSourceReady_) {
            continue;
        }
        if (drained > 0) {
            activeFailureProbeCandidateSweep_.clear();
            activeFailureProbeSweepInProgress_ = false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (reconcileNotBefore_ != std::chrono::steady_clock::time_point{} && now < reconcileNotBefore_) {
            continue;
        }
        rc = ReconcileOnce();
        RecordReconcileResult(rc, now, startedAt, drained);
    }
    std::lock_guard<std::mutex> lock(stateMutex_);
    threadExited_ = true;
    diagnostics_.running = false;
    stoppedCv_.notify_all();
}

bool TopologyController::StopRequested() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    return stopping_;
}

void TopologyController::ConsumeRuntimeEvent(const RuntimeEvent &event)
{
    if (const auto *report = std::get_if<WorkerLivenessReport>(&event.payload)) {
        ApplyWorkerLivenessReport(*report);
        return;
    }
    if (options_.eventSourceMode != TopologyEventSourceMode::EXTERNAL_ETCD) {
        membershipDirty_ = true;
        return;
    }
    const auto *coordination = std::get_if<CoordinationEvent>(&event.payload);
    externalEventSourceReady_ = true;
    if (coordination == nullptr || ApplyExternalEvent(*coordination).IsError()) {
        externalResyncRequired_ = true;
        failureClassifier_.Pause(options_.now());
    }
}

Status TopologyController::WaitForReconcile(bool immediate, size_t &drained)
{
    drained = 0;
    RuntimeEvent event;
    auto wakeDeadline =
        immediate ? std::chrono::steady_clock::now() : std::chrono::steady_clock::now() + options_.reconcileTick;
    if (!activeBatchObserved_) {
        if (scaleInCollect_.has_value() && scaleInCollect_->deadline < wakeDeadline) {
            wakeDeadline = scaleInCollect_->deadline;
        }
        if (scaleOutCollect_.has_value() && scaleOutCollect_->deadline < wakeDeadline) {
            wakeDeadline = scaleOutCollect_->deadline;
        }
    }
    const auto steadyNow = std::chrono::steady_clock::now();
    if (activeFailureProbeWakeDeadline_.has_value()) {
        if (*activeFailureProbeWakeDeadline_ <= steadyNow) {
            activeFailureProbeWakeDeadline_.reset();
            wakeDeadline = steadyNow;
        } else if (*activeFailureProbeWakeDeadline_ < wakeDeadline) {
            wakeDeadline = *activeFailureProbeWakeDeadline_;
        }
    }
    auto rc = dispatcher_.WaitPop(wakeDeadline, event);
    if (rc.IsError() && rc.GetCode() != K_RPC_DEADLINE_EXCEEDED) {
        return rc;
    }
    DrainMembershipRestarts();
    if (rc.IsError()) {
        return Status::OK();
    }
    ConsumeRuntimeEvent(event);
    drained = FIRST_DRAINED_EVENT_COUNT;
    while (drained < MAX_DOORBELLS_PER_RECONCILE
           && dispatcher_.WaitPop(std::chrono::steady_clock::now(), event).IsOk()) {
        ConsumeRuntimeEvent(event);
        ++drained;
    }
    return Status::OK();
}

void TopologyController::RecordReconcileResult(const Status &status, std::chrono::steady_clock::time_point now,
                                               std::chrono::steady_clock::time_point startedAt, size_t drained)
{
    if (status.IsError()) {
        // Back off every bounded continuation after an error because the returned Status does not preserve its source.
        // This deliberately trades at most one reconcile tick for avoiding a busy loop on persistent Store failures.
        derivedWorkPending_ = false;
        progressWorkPending_ = false;
        activeFailureProbeCandidateSweep_.clear();
        activeFailureProbeSweepInProgress_ = false;
    }
    const bool externalResyncNeedsBackoff = status.GetCode() == K_TRY_AGAIN
                                            && options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD
                                            && externalResyncRequired_;
    if (status.GetCode() == K_TRY_AGAIN && !externalResyncNeedsBackoff) {
        consecutiveReconcileFailures_ = 0;
        reconcileNotBefore_ = {};
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL) << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName()
                                         << " action=cas_conflict backoff_ms=0 status=" << status.ToString();
    } else if (status.GetCode() == K_NOT_READY) {
        consecutiveReconcileFailures_ = 0;
        reconcileNotBefore_ = {};
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL) << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName()
                                         << " action=recovery_not_ready backoff_ms=0 status=" << status.ToString();
    } else if (status.IsError()) {
        const uint32_t shift = std::min(consecutiveReconcileFailures_, MAX_RECONCILE_BACKOFF_SHIFT);
        const auto backoff = options_.reconcileTick * (BACKOFF_SHIFT_BASE << shift);
        reconcileNotBefore_ = now + backoff;
        ++consecutiveReconcileFailures_;
        LOG(WARNING) << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName()
                     << " action=failed backoff_ms=" << backoff.count() << " status=" << status.ToString();
    } else {
        consecutiveReconcileFailures_ = 0;
        reconcileNotBefore_ = {};
    }
    if (status.IsOk() && (drained > 0 || topologyCommittedThisTick_)) {
        const auto stats = dispatcher_.GetStats();
        LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_RECONCILE_LOG_INTERVAL)
            << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName() << " role=controller drained_events=" << drained
            << " committed=" << topologyCommittedThisTick_
            << " elapsed_ms=" << DurationMs(startedAt, std::chrono::steady_clock::now())
            << " queued_events=" << stats.queueDepth << " coalesced_events=" << stats.coalesced
            << " overflow_events=" << stats.overflow;
    }
    std::lock_guard<std::mutex> lock(stateMutex_);
    diagnostics_.lastError = status.IsError() ? status.ToString() : "";
}

Status TopologyController::ReconcileOnce()
{
    if (dispatcher_.ConsumeResyncRequired()) {
        LOG(WARNING) << "CLUSTER_WATCH cluster=" << keys_.ClusterName()
                     << " role=controller scope=all status=resync queued_events=" << dispatcher_.GetStats().queueDepth;
        if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD) {
            externalResyncRequired_ = true;
        }
    }
    auto rc = RecoverFromLatestTopology();
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (rc.IsOk()) {
        membershipDirty_ = false;
    }
    const bool backendUnavailable = IsRetryableRpcError(rc) || IsNonRetryableRpcError(rc);
    diagnostics_.backendState = backendUnavailable ? ControlBackendState::UNAVAILABLE : ControlBackendState::AVAILABLE;
    diagnostics_.controlFrozen = rc.IsError() && !IsTransientReconcileStatus(rc.GetCode());
    return rc;
}

Status TopologyController::RecoverFromLatestTopology()
{
    topologyCommittedThisTick_ = false;
    std::shared_ptr<const TopologySnapshot> latest;
    std::vector<MembershipRecord> memberships;
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD) {
        if (externalResyncRequired_) {
            auto resyncStatus = ResyncExternalFacts();
            if (resyncStatus.IsError()) {
                failureClassifier_.Pause(options_.now());
                return resyncStatus;
            }
        }
        auto readStatus = externalTopology_.Load(latest);
        if (readStatus.IsError()) {
            failureClassifier_.Pause(options_.now());
            return readStatus;
        }
        memberships.reserve(externalMemberships_.size());
        for (const auto &[address, membership] : externalMemberships_) {
            (void)address;
            memberships.emplace_back(membership);
        }
    } else {
        TopologyReader reader(repository_);
        auto readStatus = reader.Read(CONTROLLER_READ_TIMEOUT_MS, latest);
        if (readStatus.GetCode() == K_NOT_FOUND) {
            readStatus = EnsureTopologyAuthority();
            if (readStatus.IsOk()) {
                readStatus = reader.Read(CONTROLLER_READ_TIMEOUT_MS, latest);
            }
        }
        if (readStatus.IsError()) {
            failureClassifier_.Pause(options_.now());
            return readStatus;
        }
        auto membershipStatus = repository_.ReadMemberships(memberships);
        if (membershipStatus.IsError()) {
            failureClassifier_.Pause(options_.now());
            return membershipStatus;
        }
    }
    activeBatchObserved_ = latest->GetActiveBatch().has_value();
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        diagnostics_.topologyVersion = latest->Version();
        diagnostics_.topologyRevision = latest->AuthorityRevision();
        diagnostics_.activeBatch = latest->GetActiveBatch();
    }
    if (membershipDirty_ || lastMembershipObservationDigest_.empty()) {
        const auto membershipDigest = MembershipDigest(memberships);
        if (membershipDigest != lastMembershipObservationDigest_) {
            lastMembershipObservationDigest_ = membershipDigest;
            LOG(INFO) << "CLUSTER_MEMBERSHIP_OBSERVED cluster=" << keys_.ClusterName()
                      << " topology_version=" << latest->Version()
                      << " topology_revision=" << latest->AuthorityRevision() << " member_count=" << memberships.size()
                      << " state_counts=" << MembershipStateCounts(memberships)
                      << " digest_prefix=" << TopologyDiagnosticPrefix(membershipDigest)
                      << " sample=" << MembershipSample(memberships);
        }
    }
    RETURN_IF_NOT_OK(RestoreReadyAfterLocalRecovery(*latest, memberships));
    RETURN_IF_NOT_OK(TryConfirmFailures(*latest, memberships));
    if (topologyCommittedThisTick_) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(ReconcileDerivedState(*latest, memberships));
    RETURN_IF_NOT_OK(TryFinalizeActiveBatch(*latest, memberships));
    if (topologyCommittedThisTick_) {
        return Status::OK();
    }
    return TryStartNextBatch(*latest, memberships);
}

Status TopologyController::RestoreReadyAfterLocalRecovery(
    const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships)
{
    if (options_.eventSourceMode != TopologyEventSourceMode::EXTERNAL_ETCD
        || options_.localMembershipRecoveryHandler == nullptr || options_.localAddress.empty()) {
        return Status::OK();
    }
    const auto membership = std::find_if(memberships.begin(), memberships.end(), [this](const auto &record) {
        return record.address == options_.localAddress;
    });
    if (membership == memberships.end() || membership->state != MemberLifecycleState::RECOVERING) {
        return Status::OK();
    }
    const Member *local = nullptr;
    if (latest.FindMemberByAddress(options_.localAddress, local).IsError() || local == nullptr
        || local->state != MemberState::ACTIVE) {
        return Status::OK();
    }
    auto rc = options_.localMembershipRecoveryHandler();
    if (rc.GetCode() == K_NOT_READY) {
        LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_RECONCILE_LOG_INTERVAL)
            << "CLUSTER_MEMBERSHIP cluster=" << keys_.ClusterName()
            << " role=controller action=restore_ready_after_local_recovery address=" << options_.localAddress
            << " status=" << rc.ToString();
        // Local admission can remain pending much longer than one Controller tick. It must not freeze failure
        // confirmation or unrelated topology batches, and no exact resync is needed because no membership changed.
        return Status::OK();
    } else {
        LOG(INFO) << "CLUSTER_MEMBERSHIP cluster=" << keys_.ClusterName()
                  << " role=controller action=restore_ready_after_local_recovery address=" << options_.localAddress
                  << " status=" << rc.ToString();
    }
    RETURN_IF_NOT_OK(rc);
    externalResyncRequired_ = true;
    RETURN_STATUS(K_NOT_READY, "local recovered membership was promoted; exact resync required");
}

Status TopologyController::EnsureTopologyAuthority()
{
    TopologyState initial;
    initial.version = 1;
    TopologyCasResult result;
    RETURN_IF_NOT_OK(repository_.CompareAndSwapTopology(0, initial, result));
    CHECK_FAIL_RETURN_STATUS(
        result.outcome == TopologyCasOutcome::COMMITTED || result.outcome == TopologyCasOutcome::CONFLICT, K_TRY_AGAIN,
        "Unable to establish initial topology authority.");
    return Status::OK();
}

Status TopologyController::ReconcileDerivedState(const TopologySnapshot &latest,
                                                 const std::vector<MembershipRecord> &memberships)
{
    RETURN_IF_NOT_OK(PrepareDerivedGeneration(latest, memberships));
    return ReconcileDerivedSlice();
}

Status TopologyController::PrepareDerivedGeneration(const TopologySnapshot &latest,
                                                    const std::vector<MembershipRecord> &memberships)
{
    const bool scopeMembershipsToTopology =
        options_.materializeRestartFacts && latest.GetActiveBatch().has_value();
    const auto membershipDigest = options_.materializeRestartFacts
                                      ? MembershipDigest(memberships, scopeMembershipsToTopology ? &latest : nullptr)
                                      : "";
    if (derivedTopologyVersion_ == latest.Version() && derivedMembershipDigest_ == membershipDigest) {
        return Status::OK();
    }
    std::vector<MembershipRecord> scopedMemberships;
    const auto *generationMemberships = &memberships;
    if (scopeMembershipsToTopology) {
        scopedMemberships.reserve(std::min(memberships.size(), latest.Members().size()));
        CollectTopologyMemberships(memberships, latest, scopedMemberships);
        generationMemberships = &scopedMemberships;
    }
    ExpectedDerivedState candidate;
    RETURN_IF_NOT_OK(materializer_.RebuildExpected(latest, algorithm_, *generationMemberships,
                                                   options_.materializeRestartFacts, candidate));
    expectedDerivedState_ = std::move(candidate);
    derivedTopologyVersion_ = latest.Version();
    derivedMembershipDigest_ = membershipDigest;
    admissionCursor_ = 0;
    derivedWorkPending_ = true;
    return Status::OK();
}

Status TopologyController::ReconcileDerivedSlice()
{
    const auto &expected = expectedDerivedState_;
    const size_t total = expected.tasks.size() + expected.notifyRecipients.size();
    if (total == 0) {
        admissionCursor_ = 0;
        derivedWorkPending_ = false;
        std::lock_guard<std::mutex> lock(stateMutex_);
        diagnostics_.dirtyDerivedOperations = 0;
        return Status::OK();
    }
    const auto startedAt = std::chrono::steady_clock::now();
    const bool wasPending = derivedWorkPending_;
    size_t operations = 0;
    std::string reusableNotify;
    while (admissionCursor_ < total && operations < options_.maxDerivedOperationsPerTick
           && std::chrono::steady_clock::now() - startedAt < options_.derivedSliceBudget) {
        const size_t index = admissionCursor_;
        if (index < expected.tasks.size()) {
            RETURN_IF_NOT_OK(repository_.CreateTaskIfAbsent(expected.tasks[index]));
        } else {
            const auto &address = expected.notifyRecipients[index - expected.tasks.size()];
            RETURN_IF_NOT_OK(materializer_.BuildEncodedNotifyFor(expected, address, reusableNotify));
            RETURN_IF_NOT_OK(repository_.RewriteEncodedNotify(address, reusableNotify));
        }
        ++admissionCursor_;
        ++operations;
    }
    derivedWorkPending_ = admissionCursor_ < total;
    const auto elapsedMs = DurationMs(startedAt, std::chrono::steady_clock::now());
    if (wasPending && !derivedWorkPending_) {
        LOG(INFO) << "CLUSTER_DERIVED_STATE cluster=" << keys_.ClusterName()
                  << " action=generation_complete version=" << derivedTopologyVersion_
                  << " task_count=" << expected.tasks.size() << " notify_count=" << expected.notifyRecipients.size()
                  << " elapsed_ms=" << elapsedMs;
    }
    if (elapsedMs > DERIVED_SLICE_WARN_THRESHOLD_MS) {
        LOG(WARNING) << "CLUSTER_DERIVED_SLICE cluster=" << keys_.ClusterName()
                     << " version=" << derivedTopologyVersion_ << " cursor=" << admissionCursor_ << " total=" << total
                     << " operations=" << operations << " elapsed_ms=" << elapsedMs
                     << " pending=" << derivedWorkPending_;
    } else {
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL) << "CLUSTER_DERIVED_SLICE cluster=" << keys_.ClusterName()
                                         << " version=" << derivedTopologyVersion_ << " cursor=" << admissionCursor_
                                         << " total=" << total << " operations=" << operations
                                         << " elapsed_ms=" << elapsedMs << " pending=" << derivedWorkPending_;
    }
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        diagnostics_.dirtyDerivedOperations = total - admissionCursor_;
    }
    return Status::OK();
}

Status TopologyController::TryConfirmFailures(const TopologySnapshot &latest,
                                              const std::vector<MembershipRecord> &memberships)
{
    ObserveMembershipRestarts(memberships);
    if (AllMembersExiting(latest, memberships)) {
        if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL && latest.ClusterHasInit()) {
            std::vector<MemberIdentity> leaving;
            std::vector<MemberIdentity> joining;
            RETURN_IF_NOT_OK(CollectNextBatchCandidates(latest, memberships, leaving, joining, false));
            if (!joining.empty()) {
                return Status::OK();
            }
        }
        return CommitClusterShutdown(latest);
    }
    if (IsCollectiveCommittedMembershipMissing(latest, memberships)) {
        const auto samples = SelectCollectiveProbeSamples(latest);
        std::vector<MemberAbsenceSample> missingSamples;
        missingSamples.reserve(samples.size());
        for (const auto &sample : samples) {
            const Member *member = nullptr;
            RETURN_IF_NOT_OK(latest.FindMemberByAddress(sample.address, member));
            CHECK_FAIL_RETURN_STATUS(member != nullptr, K_RUNTIME_ERROR,
                                     "collective probe sample missing from topology snapshot");
            missingSamples.push_back({ sample, member->state });
        }
        std::vector<MemberAbsenceObservation> confirmedMissing;
        RETURN_IF_NOT_OK(failureClassifier_.ObserveMissingSamples(missingSamples, options_.now(), confirmedMissing));
        return HandleCollectiveMembershipAbsence(latest, memberships, samples, confirmedMissing);
    }
    FailureClassification classification;
    RETURN_IF_NOT_OK(failureClassifier_.Observe(latest, memberships, options_.now(), classification));
    ResetCollectiveProbeProgress();
    for (const auto &observed : classification.newlyMissing) {
        LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                     << " address=" << observed.identity.address
                     << " member_id_prefix=" << MemberIdForLog(observed.identity.id)
                     << " state=" << MemberStateName(observed.state)
                     << " action=missing_first_observed missing_ms=" << observed.missingMs << " node_dead_timeout_ms="
                     << std::chrono::duration_cast<std::chrono::milliseconds>(options_.nodeDeadTimeout).count();
    }
    for (const auto &observed : classification.restored) {
        LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                  << " address=" << observed.identity.address
                  << " member_id_prefix=" << MemberIdForLog(observed.identity.id)
                  << " state=" << MemberStateName(observed.state)
                  << " action=missing_resolved missing_ms=" << observed.missingMs;
    }
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL) {
        RETURN_IF_NOT_OK(RefreshWitnessProbes(latest, memberships, classification));
    }
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL && !classification.confirmedMissing.empty()) {
        LogDirectFailureConfirmation(keys_.ClusterName(), latest.Version(), classification.confirmedMissing,
                                     options_.nodeDeadTimeout);
    }
    RETURN_IF_NOT_OK(ConfirmMissingMembersUnreachable(latest, classification));
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL) {
        ApplyWitnessFailureGate(classification);
    }
    std::vector<MemberIdentity> fencedSummaryFailures;
    std::optional<uint64_t> activeFailureControlEpoch;
    if (options_.failureSummaryCandidateProvider) {
        std::unordered_set<std::string> confirmedAddresses;
        confirmedAddresses.reserve(classification.confirmedFailure.size());
        for (const auto &identity : classification.confirmedFailure) {
            confirmedAddresses.emplace(identity.address);
        }
        std::unordered_set<std::string> joiningAddresses;
        joiningAddresses.reserve(classification.removeJoining.size());
        for (const auto &identity : classification.removeJoining) {
            joiningAddresses.emplace(identity.address);
        }
        std::vector<MemberIdentity> candidates;
        if (activeFailureProbeSweepInProgress_) {
            candidates = activeFailureProbeCandidateSweep_;
        } else {
            candidates = options_.failureSummaryCandidateProvider(latest, memberships, options_.now());
            activeFailureProbeCandidateSweep_ = candidates;
        }
        std::vector<MemberIdentity> probeCandidates;
        probeCandidates.reserve(candidates.size());
        size_t activeCandidateCount = 0;
        for (const auto &identity : candidates) {
            const Member *member = nullptr;
            if (latest.FindMemberByAddress(identity.address, member).IsError() || member == nullptr) {
                continue;
            }
            if (member->state == MemberState::ACTIVE) {
                probeCandidates.emplace_back(identity);
                ++activeCandidateCount;
                continue;
            }
            const auto activeBatch = latest.GetActiveBatch();
            if (member->state == MemberState::JOINING && activeBatch.has_value()
                && activeBatch->type == TopologyChangeType::SCALE_OUT
                && joiningAddresses.count(identity.address) == 0) {
                probeCandidates.emplace_back(identity);
            }
        }
        std::unordered_set<std::string> candidateAddresses;
        candidateAddresses.reserve(probeCandidates.size());
        for (const auto &identity : probeCandidates) {
            candidateAddresses.emplace(identity.address);
        }
        const bool ambiguousTwoWorkerCandidates =
            latest.ActiveMembers().size() == TWO_WORKER_CLUSTER_SIZE && activeCandidateCount != 1;
        if (ambiguousTwoWorkerCandidates && activeCandidateCount > 0) {
            LOG_FIRST_AND_EVERY_N(WARNING, TOPOLOGY_RECONCILE_LOG_INTERVAL)
                << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                << " action=active_summary_ambiguous candidate_count=" << activeCandidateCount << " decision=preserve";
        }
        std::vector<MemberIdentity> candidatesToProbe;
        for (const auto &identity : probeCandidates) {
            const Member *member = nullptr;
            if (latest.FindMemberByAddress(identity.address, member).IsError() || member == nullptr) {
                continue;
            }
            if (member->state == MemberState::ACTIVE) {
                if (!ambiguousTwoWorkerCandidates && confirmedAddresses.count(identity.address) == 0) {
                    candidatesToProbe.push_back(identity);
                }
            } else if (member->state == MemberState::JOINING && joiningAddresses.count(identity.address) == 0) {
                candidatesToProbe.push_back(identity);
            }
        }
        std::vector<MemberIdentity> probeConfirmed;
        RETURN_IF_NOT_OK(
            ProbeActiveFailureCandidates(latest, candidatesToProbe, probeConfirmed, activeFailureControlEpoch));
        for (const auto &identity : probeConfirmed) {
            const Member *member = nullptr;
            if (latest.FindMemberByAddress(identity.address, member).IsError() || member == nullptr) {
                continue;
            }
            if (member->state == MemberState::JOINING) {
                classification.removeJoining.push_back(identity);
                joiningAddresses.emplace(identity.address);
                LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                             << " version=" << latest.Version() << " address=" << identity.address
                             << " member_id_prefix=" << MemberIdForLog(identity.id)
                             << " action=joining_summary_confirmed";
            } else if (member->state == MemberState::ACTIVE) {
                classification.confirmedFailure.push_back(identity);
                confirmedAddresses.emplace(identity.address);
                LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                             << " version=" << latest.Version() << " address=" << identity.address
                             << " member_id_prefix=" << MemberIdForLog(identity.id)
                             << " action=active_summary_confirmed";
            }
            fencedSummaryFailures.emplace_back(identity);
        }
        for (auto iter = activeFailureProbeStates_.begin(); iter != activeFailureProbeStates_.end();) {
            iter = candidateAddresses.count(iter->first) == 0 ? activeFailureProbeStates_.erase(iter) : std::next(iter);
        }
        if (activeFailureProbeStates_.empty()) {
            activeFailureProbeWakeDeadline_.reset();
        }
    }
    if (!classification.confirmedFailure.empty()) {
        const auto commit = [&](int64_t expectedAuthorityRevision) {
            return CommitConfirmedFailures(latest, classification, expectedAuthorityRevision);
        };
        if (!fencedSummaryFailures.empty()) {
            try {
                return options_.activeFailureCommitFence(latest, memberships, options_.now(), activeFailureControlEpoch,
                                                         fencedSummaryFailures, commit);
            } catch (const std::exception &error) {
                RETURN_STATUS(K_RUNTIME_ERROR, std::string("active failure commit fence threw: ") + error.what());
            } catch (...) {
                RETURN_STATUS(K_RUNTIME_ERROR, "active failure commit fence threw an unknown exception");
            }
        }
        return commit(0);
    }
    if (!classification.removeInitial.empty() || !classification.removeJoining.empty()) {
        const auto commit = [&](int64_t expectedAuthorityRevision) {
            return CommitUncommittedCleanup(latest, classification, expectedAuthorityRevision);
        };
        if (!fencedSummaryFailures.empty()) {
            try {
                return options_.activeFailureCommitFence(latest, memberships, options_.now(), activeFailureControlEpoch,
                                                         fencedSummaryFailures, commit);
            } catch (const std::exception &error) {
                RETURN_STATUS(K_RUNTIME_ERROR, std::string("joining failure commit fence threw: ") + error.what());
            } catch (...) {
                RETURN_STATUS(K_RUNTIME_ERROR, "joining failure commit fence threw an unknown exception");
            }
        }
        return commit(0);
    }
    return CommitMembershipFacts(latest, memberships);
}

void TopologyController::ResetActiveFailureProbeAuthorityState()
{
    activeFailureProbeStates_.clear();
    activeFailureProbeWakeDeadline_.reset();
    activeFailureProbeCursor_ = 0;
    activeFailureProbeCandidateSweep_.clear();
    activeFailureProbeSweepInProgress_ = false;
}

Status TopologyController::ResolveActiveFailureControlEpoch(std::optional<uint64_t> &controlEpoch)
{
    if (!options_.collectiveControlEpoch) {
        return Status::OK();
    }
    try {
        controlEpoch = options_.collectiveControlEpoch();
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("active failure control epoch threw: ") + error.what());
    } catch (...) {
        RETURN_STATUS(K_RUNTIME_ERROR, "active failure control epoch threw an unknown exception");
    }
    if (!controlEpoch.has_value()) {
        ResetActiveFailureProbeAuthorityState();
    }
    return Status::OK();
}

Status TopologyController::PrepareActiveFailureProbeRound(const std::vector<MemberIdentity> &targets,
                                                          std::vector<MemberIdentity> &dueTargets,
                                                          std::optional<uint64_t> &controlEpoch)
{
    const auto now = options_.now();
    RETURN_IF_NOT_OK(ResolveActiveFailureControlEpoch(controlEpoch));
    if (options_.collectiveControlEpoch && !controlEpoch.has_value()) {
        return Status::OK();
    }
    std::vector<MemberIdentity> orderedTargets = targets;
    std::sort(orderedTargets.begin(), orderedTargets.end(),
              [](const auto &lhs, const auto &rhs) { return lhs.address < rhs.address; });
    auto nextProbeDelay = ACTIVE_FAILURE_DIRECT_PROBE_INTERVAL;
    for (const auto &target : orderedTargets) {
        ActiveFailureProbeState initial{ target, controlEpoch, now, 0 };
        auto [stateIter, inserted] = activeFailureProbeStates_.try_emplace(target.address, initial);
        auto &state = stateIter->second;
        if (!inserted && (!(state.target == target) || state.controlEpoch != controlEpoch)) {
            state = std::move(initial);
            inserted = true;
        }
        if (!inserted && now < state.notBefore) {
            nextProbeDelay =
                std::min(nextProbeDelay, std::chrono::duration_cast<std::chrono::milliseconds>(state.notBefore - now));
        }
    }
    if (!orderedTargets.empty()) {
        const size_t startIndex = activeFailureProbeCursor_ % orderedTargets.size();
        size_t inspected = 0;
        while (inspected < orderedTargets.size() && dueTargets.size() < MAX_ACTIVE_FAILURE_PROBES_PER_ROUND) {
            const size_t index = (startIndex + inspected) % orderedTargets.size();
            const auto &target = orderedTargets[index];
            auto &state = activeFailureProbeStates_.at(target.address);
            ++inspected;
            if (now < state.notBefore) {
                continue;
            }
            dueTargets.push_back(target);
            state.notBefore = now + ACTIVE_FAILURE_DIRECT_PROBE_INTERVAL;
        }
        activeFailureProbeCursor_ = (startIndex + inspected) % orderedTargets.size();
        activeFailureProbeSweepInProgress_ = inspected < orderedTargets.size();
        if (activeFailureProbeSweepInProgress_) {
            nextProbeDelay = std::chrono::milliseconds(0);
        } else {
            activeFailureProbeCandidateSweep_.clear();
        }
        activeFailureProbeWakeDeadline_ = std::chrono::steady_clock::now() + nextProbeDelay;
    } else {
        activeFailureProbeCandidateSweep_.clear();
        activeFailureProbeSweepInProgress_ = false;
    }
    return Status::OK();
}

void TopologyController::ApplyActiveFailureProbeResults(const TopologySnapshot &latest,
                                                        const std::vector<MemberIdentity> &dueTargets,
                                                        const std::vector<ControlBackendProbeResult> &results,
                                                        std::vector<MemberIdentity> &confirmed,
                                                        std::optional<uint64_t> &controlEpoch)
{
    std::unordered_map<std::string, const ControlBackendProbeResult *> resultsByAddress;
    resultsByAddress.reserve(results.size());
    for (const auto &result : results) {
        resultsByAddress[result.target.address] = &result;
    }
    for (const auto &target : dueTargets) {
        const auto result = resultsByAddress.find(target.address);
        const bool matched = result != resultsByAddress.end() && result->second->target == target;
        const auto outcome = matched ? result->second->outcome : ControlBackendProbeOutcome::CANCELLED;
        const bool unreachable = matched
                                 && (outcome == ControlBackendProbeOutcome::UNAVAILABLE
                                     || outcome == ControlBackendProbeOutcome::DEADLINE_EXCEEDED);
        auto &state = activeFailureProbeStates_.at(target.address);
        state.consecutiveUnreachable = unreachable ? state.consecutiveUnreachable + 1 : 0;
        const bool targetConfirmed = state.consecutiveUnreachable >= MIN_ACTIVE_FAILURE_UNREACHABLE_PROBES;
        if (targetConfirmed) {
            confirmed.push_back(target);
            controlEpoch = state.controlEpoch;
        }
        std::ostringstream probeLog;
        probeLog << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                 << " address=" << target.address << " member_id_prefix=" << MemberIdForLog(target.id)
                 << " action=active_summary_direct_probe probe_result="
                 << (matched ? ControlBackendProbeOutcomeName(outcome) : "mismatched")
                 << " probe_elapsed_ms=" << (matched ? result->second->elapsed.count() : 0)
                 << " consecutive_unreachable=" << state.consecutiveUnreachable
                 << " decision=" << (targetConfirmed ? "confirm" : "preserve");
        if (targetConfirmed) {
            LOG(WARNING) << probeLog.str();
        } else {
            VLOG(1) << probeLog.str();
        }
    }
}

Status TopologyController::ProbeActiveFailureCandidates(const TopologySnapshot &latest,
                                                        const std::vector<MemberIdentity> &targets,
                                                        std::vector<MemberIdentity> &confirmed,
                                                        std::optional<uint64_t> &controlEpoch)
{
    confirmed.clear();
    controlEpoch.reset();
    std::vector<MemberIdentity> dueTargets;
    RETURN_IF_NOT_OK(PrepareActiveFailureProbeRound(targets, dueTargets, controlEpoch));
    if (dueTargets.empty()) {
        return Status::OK();
    }
    if (!options_.memberLivenessProbe) {
        for (const auto &target : dueTargets) {
            activeFailureProbeStates_.at(target.address).consecutiveUnreachable = 0;
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                         << " address=" << target.address << " member_id_prefix=" << MemberIdForLog(target.id)
                         << " action=active_summary_direct_probe probe_result=missing decision=preserve";
        }
        return Status::OK();
    }
    std::vector<ControlBackendProbeResult> results;
    try {
        results = options_.memberLivenessProbe(dueTargets,
                                               std::chrono::steady_clock::now() + ACTIVE_FAILURE_DIRECT_PROBE_TIMEOUT);
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("active failure direct probe threw: ") + error.what());
    } catch (...) {
        RETURN_STATUS(K_RUNTIME_ERROR, "active failure direct probe threw an unknown exception");
    }
    ApplyActiveFailureProbeResults(latest, dueTargets, results, confirmed, controlEpoch);
    return Status::OK();
}

bool TopologyController::HasReachableWitness(const SuspectProbeRound &round) const
{
    return std::any_of(round.reports.begin(), round.reports.end(),
                       [](const auto &entry) { return entry.second.result == WorkerLivenessResult::REACHABLE; });
}

Status TopologyController::StartWitnessProbeRound(const TopologySnapshot &latest,
                                                  const std::vector<std::string> &eligibleWitnesses,
                                                  const MemberIdentity &target)
{
    std::vector<std::string> witnesses;
    witnesses.reserve(options_.failureProbeWitnessCount);
    const size_t start = eligibleWitnesses.empty() ? 0 : std::hash<std::string>{}(target.id) % eligibleWitnesses.size();
    for (size_t offset = 0; offset < eligibleWitnesses.size(); ++offset) {
        const auto &candidate = eligibleWitnesses[(start + offset) % eligibleWitnesses.size()];
        if (candidate != target.address) {
            witnesses.emplace_back(candidate);
            if (witnesses.size() == options_.failureProbeWitnessCount) {
                break;
            }
        }
    }
    const auto now = options_.now();
    const uint64_t roundId = nextProbeRound_++;
    if (nextProbeRound_ == 0) {
        nextProbeRound_ = 1;
    }
    SuspectProbeRound round;
    round.target = target;
    round.probeRound = roundId;
    round.witnesses.insert(witnesses.begin(), witnesses.end());
    round.startedAt = now;
    round.deadline = now + options_.witnessProbeRoundTimeout;
    suspectRoundsByTarget_[target.address] = round;
    const auto probeId = WorkerProbeIdForLog(options_.probeEpoch, roundId);
    LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                 << " target=" << target.address << " target_id_prefix=" << MemberIdForLog(target.id)
                 << " action=WITNESS_PROBE_ROUND_STARTED probe_id=" << probeId
                 << " reason=MEMBERSHIP_TTL_EXPIRED witness_count=" << witnesses.size() << " witnesses=["
                 << VectorToString(witnesses) << "]";
    for (const auto &witness : witnesses) {
        auto status = PublishWitnessProbeEvent(witness, round);
        if (status.IsError()) {
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                         << " target=" << target.address << " target_id_prefix=" << MemberIdForLog(target.id)
                         << " witness=" << witness << " action=WITNESS_PROBE_PUBLISH_FAILED probe_id=" << probeId
                         << " status=" << status.ToString();
        }
    }
    return Status::OK();
}

Status TopologyController::PublishWitnessProbeEvent(const std::string &witness, const SuspectProbeRound &round)
{
    CHECK_FAIL_RETURN_STATUS(!options_.probeEpoch.empty(), K_NOT_READY, "worker probe epoch is not configured");
    coordinator::WorkerProbeEventValuePb value;
    value.set_cluster_name(keys_.ClusterName());
    value.set_probe_round(round.probeRound);
    value.set_target_address(round.target.address);
    value.set_target_member_id(round.target.id);
    value.set_coordinator_id(options_.probeEpoch);
    std::string encoded;
    CHECK_FAIL_RETURN_STATUS(value.SerializeToString(&encoded), K_RUNTIME_ERROR,
                             "serialize worker probe event value failed");
    return repository_.PutProbeValue(witness, encoded);
}

Status TopologyController::RefreshWitnessProbes(const TopologySnapshot &latest,
                                                const std::vector<MembershipRecord> &memberships,
                                                const FailureClassification &classification)
{
    if (options_.probeEpoch.empty()) {
        return Status::OK();
    }
    std::unordered_set<std::string> present;
    present.reserve(memberships.size());
    for (const auto &membership : memberships) {
        present.emplace(membership.address);
    }
    std::vector<std::string> eligibleWitnesses;
    eligibleWitnesses.reserve(latest.CommittedMembers().size());
    for (const auto *member : latest.CommittedMembers()) {
        if (present.count(member->identity.address) > 0) {
            eligibleWitnesses.emplace_back(member->identity.address);
        }
    }
    std::sort(eligibleWitnesses.begin(), eligibleWitnesses.end());

    std::unordered_set<std::string> confirmedMissing;
    confirmedMissing.reserve(classification.confirmedMissing.size());
    for (const auto &observed : classification.confirmedMissing) {
        confirmedMissing.emplace(observed.identity.address);
    }
    for (const auto &observed : classification.newlyMissing) {
        auto found = suspectRoundsByTarget_.find(observed.identity.address);
        if (found == suspectRoundsByTarget_.end() || !(found->second.target == observed.identity)) {
            RETURN_IF_NOT_OK(StartWitnessProbeRound(latest, eligibleWitnesses, observed.identity));
        }
    }

    const auto now = options_.now();
    std::vector<MemberIdentity> restart;
    for (auto iter = suspectRoundsByTarget_.begin(); iter != suspectRoundsByTarget_.end();) {
        const Member *member = nullptr;
        if (present.count(iter->first) > 0 || latest.FindMemberByAddress(iter->first, member).IsError()
            || !(member->identity == iter->second.target)) {
            LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                      << " action=WITNESS_PROBE_ROUND_CLEARED probe_id="
                      << WorkerProbeIdForLog(options_.probeEpoch, iter->second.probeRound)
                      << " target=" << iter->second.target.address
                      << " target_id_prefix=" << MemberIdForLog(iter->second.target.id)
                      << " reason=TARGET_NO_LONGER_SUSPECT";
            iter = suspectRoundsByTarget_.erase(iter);
            continue;
        }
        if (now >= iter->second.deadline) {
            const bool confirmed = confirmedMissing.count(iter->first) > 0;
            const bool reachable = HasReachableWitness(iter->second);
            if (!confirmed || reachable) {
                LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                          << " action=WITNESS_PROBE_ROUND_RESTARTED probe_id="
                          << WorkerProbeIdForLog(options_.probeEpoch, iter->second.probeRound)
                          << " target=" << iter->second.target.address
                          << " target_id_prefix=" << MemberIdForLog(iter->second.target.id)
                          << " confirmed_missing=" << confirmed << " reachable=" << reachable;
                restart.emplace_back(iter->second.target);
            }
        }
        ++iter;
    }
    for (const auto &target : restart) {
        RETURN_IF_NOT_OK(StartWitnessProbeRound(latest, eligibleWitnesses, target));
    }
    return Status::OK();
}

void TopologyController::ApplyWorkerLivenessReport(const WorkerLivenessReport &report)
{
    const auto probeId = WorkerProbeIdForLog(report.probeEpoch, report.probeRound);
    auto logIgnored = [&](const char *reason) {
        LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                     << " action=WITNESS_PROBE_REPORT_IGNORED probe_id=" << probeId
                     << " witness=" << report.witnessAddress << " target=" << report.target.address
                     << " target_id_prefix=" << MemberIdForLog(report.target.id)
                     << " result=" << WorkerLivenessResultName(report.result) << " reason=" << reason;
    };
    auto found = suspectRoundsByTarget_.find(report.target.address);
    if (report.probeEpoch != options_.probeEpoch) {
        logIgnored("STALE_EPOCH");
        return;
    }
    if (found == suspectRoundsByTarget_.end()) {
        logIgnored("ROUND_NOT_FOUND");
        return;
    }
    if (!(found->second.target == report.target)) {
        logIgnored("TARGET_MISMATCH");
        return;
    }
    if (found->second.probeRound != report.probeRound) {
        logIgnored("ROUND_MISMATCH");
        return;
    }
    if (found->second.witnesses.count(report.witnessAddress) == 0) {
        logIgnored("WITNESS_NOT_SELECTED");
        return;
    }
    if (report.result == WorkerLivenessResult::UNKNOWN) {
        logIgnored("UNKNOWN_RESULT");
        return;
    }
    const auto now = options_.now();
    if (now > found->second.deadline) {
        logIgnored("DEADLINE_EXPIRED");
        return;
    }
    auto [evidence, inserted] = found->second.reports.emplace(report.witnessAddress, ProbeReport{ report.result, now });
    if (!inserted && report.result == WorkerLivenessResult::REACHABLE) {
        evidence->second = { report.result, now };
    }
    LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
              << " action=WITNESS_PROBE_REPORT_ACCEPTED probe_id=" << probeId << " witness=" << report.witnessAddress
              << " target=" << report.target.address << " target_id_prefix=" << MemberIdForLog(report.target.id)
              << " result=" << WorkerLivenessResultName(report.result) << " duplicate=" << !inserted;
}

void TopologyController::ApplyWitnessFailureGate(FailureClassification &classification)
{
    const auto now = options_.now();
    auto func = [&](const MemberIdentity &target) {
        auto found = suspectRoundsByTarget_.find(target.address);
        if (found == suspectRoundsByTarget_.end() || !(found->second.target == target)) {
            return false;
        }
        const bool waiting = now < found->second.deadline;
        const bool reachable = HasReachableWitness(found->second);
        const auto probeId = WorkerProbeIdForLog(options_.probeEpoch, found->second.probeRound);
        if (waiting || reachable) {
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " target=" << target.address
                         << " target_id_prefix=" << MemberIdForLog(target.id)
                         << " action=WITNESS_PROBE_FAILURE_BLOCKED probe_id=" << probeId << " waiting=" << waiting
                         << " reachable=" << reachable;
        } else {
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " target=" << target.address
                         << " target_id_prefix=" << MemberIdForLog(target.id)
                         << " action=WITNESS_PROBE_FAILURE_ALLOWED probe_id=" << probeId
                         << " waiting=false reachable=false";
        }
        return waiting || reachable;
    };
    classification.confirmedFailure.erase(
        std::remove_if(classification.confirmedFailure.begin(), classification.confirmedFailure.end(), std::move(func)),
        classification.confirmedFailure.end());
}

void TopologyController::ResetCollectiveProbeProgress() noexcept
{
    collectiveProbeTopologyVersion_.reset();
    collectiveProbeOwner_.reset();
    collectiveProbeControlEpoch_.reset();
    collectiveUnreachableSamples_.clear();
}

void TopologyController::SummarizeCollectiveReadyMemberships(
    const std::vector<MembershipRecord> &memberships, size_t &readyCount, std::optional<std::string> &owner) const
{
    readyCount = 0;
    owner.reset();
    for (const auto &record : memberships) {
        if (record.state != MemberLifecycleState::READY) {
            continue;
        }
        const auto quarantined = quarantinedReadyTimestampByAddress_.find(record.address);
        if (quarantined != quarantinedReadyTimestampByAddress_.end()
            && record.timestamp <= quarantined->second) {
            continue;
        }
        ++readyCount;
        if (!owner.has_value() || record.address < *owner) {
            owner = record.address;
        }
    }
    if (readyCount > 0 && options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL) {
        owner = COORDINATOR_COLLECTIVE_PROBE_OWNER;
    }
}

void TopologyController::LogCollectiveDecision(
    const TopologySnapshot &latest, size_t membershipCount, size_t readyCount, const std::string &owner,
    size_t progress, size_t sampleCount, const char *action, const char *decision, const char *reason,
    bool sampled, const std::string &details) const
{
    std::ostringstream message;
    message << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
            << " action=" << action << " committed=" << latest.CommittedMembers().size()
            << " membership=" << membershipCount << " ready=" << readyCount << " owner=" << owner
            << " progress=" << progress << "/" << sampleCount << " decision=" << decision
            << " reason=" << reason << details;
    if (sampled) {
        LOG_FIRST_AND_EVERY_N(WARNING, TOPOLOGY_RECONCILE_LOG_INTERVAL) << message.str();
    } else {
        LOG(WARNING) << message.str();
    }
}

std::vector<MemberIdentity> TopologyController::SelectCollectiveProbeSamples(const TopologySnapshot &latest) const
{
    const auto &committed = latest.CommittedMembers();  // Snapshot indexes preserve canonical address order.
    if (committed.size() <= COLLECTIVE_PROBE_SAMPLE_COUNT) {
        std::vector<MemberIdentity> samples;
        for (const auto *member : committed) {
            samples.push_back(member->identity);
        }
        return samples;
    }
    std::vector<MemberIdentity> samples;
    samples.reserve(COLLECTIVE_PROBE_SAMPLE_COUNT);
    for (size_t index = 0; index < COLLECTIVE_PROBE_SAMPLE_COUNT; ++index) {
        const auto position = index * (committed.size() - 1) / (COLLECTIVE_PROBE_SAMPLE_COUNT - 1);
        samples.push_back(committed[position]->identity);
    }
    return samples;
}

Status TopologyController::ProbeCollectiveSample(const TopologySnapshot &latest,
                                                 const std::vector<MemberIdentity> &samples,
                                                 size_t membershipCount, size_t readyCount)
{
    const auto target = std::find_if(samples.begin(), samples.end(), [&](const auto &sample) {
        return collectiveUnreachableSamples_.count(sample.address) == 0;
    });
    if (target == samples.end()) {
        return Status::OK();
    }
    std::vector<ControlBackendProbeResult> results;
    try {
        results = options_.memberLivenessProbe({ *target },
                                               std::chrono::steady_clock::now() + options_.failureProbeTimeout);
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("member liveness probe threw: ") + error.what());
    } catch (...) {
        RETURN_STATUS(K_RUNTIME_ERROR, "member liveness probe threw an unknown exception");
    }
    const bool matched = results.size() == 1 && results.front().target == *target;
    const auto outcome = matched ? results.front().outcome : ControlBackendProbeOutcome::CANCELLED;
    const auto owner = collectiveProbeOwner_.value_or("none");
    if (matched && (outcome == ControlBackendProbeOutcome::RESPONSE
                    || outcome == ControlBackendProbeOutcome::ERROR)) {
        failureClassifier_.ResetMissing(target->address);
        ResetCollectiveProbeProgress();
    } else if (matched && (outcome == ControlBackendProbeOutcome::DEADLINE_EXCEEDED
                           || outcome == ControlBackendProbeOutcome::UNAVAILABLE)) {
        collectiveUnreachableSamples_.emplace(target->address);
    }
    const bool allUnreachable = collectiveUnreachableSamples_.size() == samples.size();
    const bool neutral = !matched || (outcome != ControlBackendProbeOutcome::DEADLINE_EXCEEDED
                                      && outcome != ControlBackendProbeOutcome::UNAVAILABLE);
    auto details = " target=" + target->address + " result=" + std::to_string(static_cast<uint32_t>(outcome));
    if (matched) {
        details += " probe_elapsed_ms=" + std::to_string(results.front().elapsed.count());
    }
    LogCollectiveDecision(latest, membershipCount, readyCount, owner, collectiveUnreachableSamples_.size(),
                          samples.size(), "collective_direct_probe",
                          allUnreachable ? "exact_reread" : "preserve",
                          neutral ? "neutral_probe_result" : "probe_unreachable", neutral, details);
    return Status::OK();
}

Status TopologyController::BootstrapCollectiveReplacement(const TopologySnapshot &latest)
{
    const auto sampleCount = std::min(COLLECTIVE_PROBE_SAMPLE_COUNT, latest.CommittedMembers().size());
    const auto probeProgress = collectiveUnreachableSamples_.size();
    std::vector<MembershipRecord> exactMemberships;
    auto rc = repository_.ReadMemberships(exactMemberships);
    if (rc.IsError()) {
        failureClassifier_.Pause(options_.now());
        LogCollectiveDecision(latest, 0, 0, collectiveProbeOwner_.value_or("none"),
                              probeProgress, sampleCount,
                              "collective_exact_reread", "preserve", "read_error", true,
                              " evidence=unavailable status=" + rc.ToString());
        return rc;
    }
    size_t exactReadyCount = 0;
    std::optional<std::string> exactOwner;
    SummarizeCollectiveReadyMemberships(exactMemberships, exactReadyCount, exactOwner);
    bool oldMemberReturned = false;
    for (const auto &membership : exactMemberships) {
        const Member *member = nullptr;
        if (latest.FindMemberByAddress(membership.address, member).IsOk() && member != nullptr
            && (member->state == MemberState::ACTIVE || member->state == MemberState::PRE_LEAVING
                || member->state == MemberState::LEAVING)) {
            failureClassifier_.ResetMissing(membership.address);
            oldMemberReturned = true;
        }
    }
    const char *preserveReason = oldMemberReturned ? "old_member_returned"
                                 : exactReadyCount == 0 ? "no_ready"
                                 : exactOwner != collectiveProbeOwner_ ? "owner_mismatch"
                                                                       : nullptr;
    if (preserveReason != nullptr) {
        LogCollectiveDecision(latest, exactMemberships.size(), exactReadyCount, exactOwner.value_or("none"),
                              probeProgress, sampleCount, "collective_exact_reread", "preserve", preserveReason,
                              false, exactOwner != collectiveProbeOwner_
                                         ? " expected_owner=" + collectiveProbeOwner_.value_or("none")
                                         : "");
        ResetCollectiveProbeProgress();
        return Status::OK();
    }
    std::unordered_set<std::string> exiting;
    std::vector<MembershipRecord> ready;
    CollectMembershipFacts(exactMemberships, exiting, ready);
    TopologyState empty = latest.CopyState();
    empty.members.clear();
    empty.activeBatch.reset();
    std::unordered_set<std::string> known;
    std::vector<MemberIdentity> admitted;
    size_t changed = 0;
    RETURN_IF_NOT_OK(ApplyReadyMembershipFacts(empty, ready, known, admitted, changed));
    TopologyState next;
    RETURN_IF_NOT_OK(planBuilder_.BuildBootstrap(empty, admitted, next));
    std::shared_ptr<const TopologySnapshot> committed;
    const auto commit = [&] { return CommitAndReadBack(latest.Version(), next, committed); };
    if (options_.collectiveReplacementFence) {
        if (!collectiveProbeControlEpoch_.has_value()) {
            LogCollectiveDecision(latest, exactMemberships.size(), exactReadyCount, exactOwner.value_or("none"),
                                  probeProgress, sampleCount, "collective_replacement", "preserve",
                                  "control_epoch_unbound", true);
            RETURN_STATUS(K_NOT_READY, "collective replacement control epoch is not bound");
        }
        try {
            rc = options_.collectiveReplacementFence(*collectiveProbeControlEpoch_, commit);
        } catch (const std::exception &error) {
            RETURN_STATUS(K_RUNTIME_ERROR,
                          std::string("collective replacement fence threw: ") + error.what());
        } catch (...) {
            RETURN_STATUS(K_RUNTIME_ERROR, "collective replacement fence threw an unknown exception");
        }
    } else {
        rc = commit();
    }
    if (rc.IsOk() || rc.GetCode() == K_TRY_AGAIN) {
        ResetCollectiveProbeProgress();
    }
    if (rc.IsOk()) {
        LogCollectiveDecision(latest, exactMemberships.size(), exactReadyCount, exactOwner.value_or("none"),
                              probeProgress, sampleCount, "collective_replacement", "replace",
                              "exact_reread_confirmed", false,
                              " committed_version=" + std::to_string(committed->Version()));
    }
    return rc;
}

Status TopologyController::PrepareCollectiveProbeContext(
    const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships,
    const std::vector<MemberIdentity> &samples, size_t &readyCount, std::optional<std::string> &owner,
    bool &hasControlAuthority)
{
    SummarizeCollectiveReadyMemberships(memberships, readyCount, owner);
    hasControlAuthority = true;
    std::optional<uint64_t> controlEpoch;
    if (options_.collectiveControlEpoch) {
        try {
            controlEpoch = options_.collectiveControlEpoch();
        } catch (const std::exception &error) {
            ResetCollectiveProbeProgress();
            RETURN_STATUS(K_RUNTIME_ERROR, std::string("collective control epoch callback threw: ") + error.what());
        } catch (...) {
            ResetCollectiveProbeProgress();
            RETURN_STATUS(K_RUNTIME_ERROR, "collective control epoch callback threw an unknown exception");
        }
        if (!controlEpoch.has_value()) {
            const auto progress = collectiveUnreachableSamples_.size();
            ResetCollectiveProbeProgress();
            LogCollectiveDecision(latest, memberships.size(), readyCount, owner.value_or("none"), progress,
                                  samples.size(), "collective_probe_fence", "preserve", "no_control_authority",
                                  true);
            hasControlAuthority = false;
            return Status::OK();
        }
    }
    const bool topologyChanged = collectiveProbeTopologyVersion_ != latest.Version();
    const bool ownerChanged = collectiveProbeOwner_ != owner;
    const bool epochChanged = options_.collectiveControlEpoch && collectiveProbeControlEpoch_ != controlEpoch;
    if (topologyChanged || ownerChanged || epochChanged) {
        const auto oldOwner = collectiveProbeOwner_.value_or("none");
        collectiveUnreachableSamples_.clear();
        collectiveProbeTopologyVersion_ = latest.Version();
        collectiveProbeOwner_ = owner;
        collectiveProbeControlEpoch_ = controlEpoch;
        const char *reason = topologyChanged ? "start_or_topology_change"
                             : ownerChanged ? "owner_change"
                                            : "control_epoch_change";
        LogCollectiveDecision(latest, memberships.size(), readyCount, owner.value_or("none"), 0,
                              std::min(COLLECTIVE_PROBE_SAMPLE_COUNT, latest.CommittedMembers().size()),
                              "collective_probe_fence", "preserve", reason, false,
                              " previous_owner=" + oldOwner);
    }
    return Status::OK();
}

Status TopologyController::HandleCollectiveMembershipAbsence(
    const TopologySnapshot &latest, const std::vector<MembershipRecord> &memberships,
    const std::vector<MemberIdentity> &samples,
    const std::vector<MemberAbsenceObservation> &confirmedMissing)
{
    size_t readyCount = 0;
    std::optional<std::string> owner;
    bool hasControlAuthority = true;
    RETURN_IF_NOT_OK(
        PrepareCollectiveProbeContext(latest, memberships, samples, readyCount, owner, hasControlAuthority));
    if (!hasControlAuthority) {
        return Status::OK();
    }
    const auto waitDetails = [&](int64_t missingMs) {
        return " missing_ms=" + std::to_string(missingMs) + " timeout_ms="
               + std::to_string(
                   std::chrono::duration_cast<std::chrono::milliseconds>(options_.nodeDeadTimeout).count());
    };
    if (readyCount == 0) {
        LogCollectiveDecision(latest, memberships.size(), 0, "none", collectiveUnreachableSamples_.size(),
                              samples.size(), "collective_wait", "preserve", "no_ready", true, waitDetails(0));
        return Status::OK();
    }
    const bool allTimedOut = std::all_of(samples.begin(), samples.end(), [&](const auto &sample) {
        return std::any_of(confirmedMissing.begin(), confirmedMissing.end(),
                           [&](const auto &missing) { return missing.identity.address == sample.address; });
    });
    const bool ownsProbe = options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL
                           || owner == options_.localAddress;
    int64_t missingMs = 0;
    for (const auto &missing : confirmedMissing) {
        missingMs = std::max(missingMs, missing.missingMs);
    }
    const char *waitReason = !allTimedOut ? "absence_timeout_pending"
                             : !ownsProbe ? "not_probe_owner"
                             : !options_.memberLivenessProbe ? "probe_callback_missing"
                                                            : nullptr;
    if (waitReason != nullptr) {
        LogCollectiveDecision(latest, memberships.size(), readyCount, owner.value_or("none"),
                              collectiveUnreachableSamples_.size(), samples.size(), "collective_wait", "preserve",
                              waitReason, true, waitDetails(missingMs));
        return Status::OK();
    }
    RETURN_IF_NOT_OK(ProbeCollectiveSample(latest, samples, memberships.size(), readyCount));
    return collectiveUnreachableSamples_.size() == samples.size() ? BootstrapCollectiveReplacement(latest)
                                                                  : Status::OK();
}

Status TopologyController::ConfirmMissingMembersUnreachable(const TopologySnapshot &latest,
                                                            FailureClassification &classification)
{
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL) {
        return Status::OK();
    }
    if (!options_.memberLivenessProbe || classification.confirmedMissing.empty()) {
        return Status::OK();
    }
    std::vector<const MemberAbsenceObservation *> owned;
    const auto *ownedMember = FailureProbeTargetOwnedBy(latest, options_.localAddress);
    if (ownedMember != nullptr) {
        const auto ownedObservation =
            std::find_if(classification.confirmedMissing.begin(), classification.confirmedMissing.end(),
                         [&](const MemberAbsenceObservation &observed) {
                             return observed.identity.address == ownedMember->identity.address;
                         });
        if (ownedObservation != classification.confirmedMissing.end()) {
            owned.push_back(&*ownedObservation);
        }
    }
    std::vector<MemberIdentity> targets;
    targets.reserve(owned.size());
    for (const auto *entry : owned) {
        const auto &observed = *entry;
        targets.push_back(observed.identity);
        LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                     << " address=" << observed.identity.address
                     << " member_id_prefix=" << MemberIdForLog(observed.identity.id)
                     << " state=" << MemberStateName(observed.state)
                     << " action=absence_timeout missing_ms=" << observed.missingMs << " node_dead_timeout_ms="
                     << std::chrono::duration_cast<std::chrono::milliseconds>(options_.nodeDeadTimeout).count();
    }

    std::vector<ControlBackendProbeResult> probeResults;
    auto probeElapsed = std::chrono::milliseconds(0);
    if (!targets.empty()) {
        const auto probeStartedAt = std::chrono::steady_clock::now();
        try {
            probeResults =
                options_.memberLivenessProbe(targets, std::chrono::steady_clock::now() + options_.failureProbeTimeout);
        } catch (const std::exception &error) {
            RETURN_STATUS(K_RUNTIME_ERROR, std::string("member liveness probe threw: ") + error.what());
        } catch (...) {
            RETURN_STATUS(K_RUNTIME_ERROR, "member liveness probe threw an unknown exception");
        }
        probeElapsed =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - probeStartedAt);
    }

    std::unordered_map<std::string, const ControlBackendProbeResult *> resultsByAddress;
    resultsByAddress.reserve(probeResults.size());
    for (const auto &result : probeResults) {
        resultsByAddress.emplace(result.target.address, &result);
    }
    std::unordered_set<std::string> directlyUnreachable;
    directlyUnreachable.reserve(targets.size());
    for (const auto &target : targets) {
        const auto found = resultsByAddress.find(target.address);
        const auto *result = found == resultsByAddress.end() ? nullptr : found->second;
        const bool hasObservation = result != nullptr && result->observation.has_value();
        const bool observationMatchesTarget = hasObservation && result->observation->reporter.address == target.address;
        if (!observationMatchesTarget) {
            directlyUnreachable.emplace(target.address);
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                         << " address=" << target.address << " member_id_prefix=" << MemberIdForLog(target.id)
                         << " action="
                         << (hasObservation ? "direct_probe_invalid_response" : "direct_probe_no_response")
                         << " probe_result="
                         << (result == nullptr ? "missing" : ControlBackendProbeOutcomeName(result->outcome))
                         << " probe_elapsed_ms="
                         << (result == nullptr ? probeElapsed.count() : result->elapsed.count());
            continue;
        }
        const auto &evidence = *result->observation;
        const auto now = std::chrono::steady_clock::now();
        const bool matchingEvidence = evidence.reporter.id == target.id && evidence.topologyVersion == latest.Version()
                                      && evidence.topologyRevision == latest.AuthorityRevision()
                                      && evidence.topologyDigest == latest.CanonicalDigest()
                                      && evidence.observedAt != std::chrono::steady_clock::time_point{}
                                      && evidence.observedAt <= now
                                      && now - evidence.observedAt <= options_.failureProbeTimeout;
        // Any direct response proves transport reachability, even when its backend evidence is stale or unavailable.
        failureClassifier_.ResetMissing(target.address);
        LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                     << " address=" << target.address << " member_id_prefix=" << MemberIdForLog(target.id)
                     << " action=" << (matchingEvidence ? "direct_probe_reachable" : "direct_probe_inconclusive")
                     << " probe_result=" << ControlBackendProbeOutcomeName(result->outcome)
                     << " probe_elapsed_ms=" << result->elapsed.count()
                     << " evidence_version=" << evidence.topologyVersion
                     << " evidence_revision=" << evidence.topologyRevision;
    }

    if (!directlyUnreachable.empty()) {
        std::vector<MembershipRecord> exactMemberships;
        auto rc = repository_.ReadMemberships(exactMemberships);
        if (rc.IsError()) {
            failureClassifier_.Pause(options_.now());
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                         << " action=membership_exact_read_failed decision=skip_failure status=" << rc.ToString();
            return rc;
        }
        std::unordered_set<std::string> exactAddresses;
        exactAddresses.reserve(exactMemberships.size());
        for (const auto &membership : exactMemberships) {
            exactAddresses.emplace(membership.address);
        }
        for (const auto &target : targets) {
            if (directlyUnreachable.count(target.address) == 0) {
                continue;
            }
            if (exactAddresses.count(target.address) > 0) {
                directlyUnreachable.erase(target.address);
                failureClassifier_.ResetMissing(target.address);
                LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                          << " address=" << target.address << " member_id_prefix=" << MemberIdForLog(target.id)
                          << " action=membership_exact_read_recovered decision=skip_failure";
            } else {
                LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                             << " version=" << latest.Version() << " address=" << target.address
                             << " member_id_prefix=" << MemberIdForLog(target.id)
                             << " action=direct_probe_unreachable membership_exact_read=absent";
            }
        }
    }

    classification.confirmedFailure.erase(
        std::remove_if(classification.confirmedFailure.begin(), classification.confirmedFailure.end(),
                       [&](const MemberIdentity &identity) {
                           if (directlyUnreachable.count(identity.address) > 0) {
                               return false;
                           }
                           const Member *member = nullptr;
                           return latest.FindMemberByAddress(identity.address, member).IsError()
                                  || member->state != MemberState::FAILED;
                       }),
        classification.confirmedFailure.end());
    return Status::OK();
}

Status TopologyController::CommitClusterShutdown(const TopologySnapshot &latest)
{
    LOG(INFO) << "CLUSTER_SHUTDOWN cluster=" << keys_.ClusterName() << " member_count=" << latest.Members().size()
              << " gate=all_members_exiting contract_status=satisfied";
    TopologyState state = latest.CopyState();
    state.activeBatch.reset();
    for (auto &member : state.members) {
        member.state = MemberState::PRE_LEAVING;
    }
    TopologyState next;
    RETURN_IF_NOT_OK(planBuilder_.BuildClusterShutdownFinal(state, next));
    std::shared_ptr<const TopologySnapshot> committed;
    return CommitAndReadBack(latest.Version(), next, committed);
}

Status TopologyController::CommitConfirmedFailures(const TopologySnapshot &latest,
                                                   const FailureClassification &classification,
                                                   int64_t expectedAuthorityRevision)
{
    std::vector<MemberIdentity> confirmed;
    std::unordered_set<std::string> retainedAddresses;
    for (const auto &member : latest.Members()) {
        if (member.state == MemberState::FAILED) {
            confirmed.push_back(member.identity);
            retainedAddresses.insert(member.identity.address);
        }
    }
    const size_t retainedCount = confirmed.size();
    for (const auto &identity : classification.confirmedFailure) {
        if (confirmed.size() >= options_.maxMembersPerBatch) {
            break;
        }
        if (retainedAddresses.insert(identity.address).second) {
            confirmed.push_back(identity);
        }
    }
    if (confirmed.size() == retainedCount) {
        return Status::OK();
    }
    const bool replan =
        latest.GetActiveBatch().has_value() && latest.GetActiveBatch()->type == TopologyChangeType::FAILURE;
    LOG(WARNING) << "CLUSTER_FAILURE cluster=" << keys_.ClusterName() << " action=plan version=" << latest.Version()
                 << " confirmed_count=" << confirmed.size()
                 << " confirmed_missing_count=" << classification.confirmedMissing.size()
                 << " sample=" << MemberIdentitySample(confirmed)
                 << " outcome=" << (replan ? "replan_pending" : "start_pending");
    TopologyPlan plan;
    RETURN_IF_NOT_OK(planBuilder_.BuildFailureStartOrReplan(latest.CopyState(), confirmed, plan));
    EraseMembers(plan.next, classification.removeInitial);
    EraseMembers(plan.next, classification.removeJoining);
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), plan.next, committed, expectedAuthorityRevision);
    if (rc.IsOk()) {
        for (const auto &identity : classification.confirmedFailure) {
            suspectRoundsByTarget_.erase(identity.address);
        }
        LogBatchStart(latest, *committed, confirmed, replan ? "replan" : "start");
    }
    return rc;
}

Status TopologyController::CommitUncommittedCleanup(const TopologySnapshot &latest,
                                                    const FailureClassification &classification,
                                                    int64_t expectedAuthorityRevision)
{
    TopologyState next;
    const auto activeBatch = latest.GetActiveBatch();
    if (!classification.removeJoining.empty() && activeBatch.has_value()
        && (activeBatch->type == TopologyChangeType::SCALE_OUT || activeBatch->type == TopologyChangeType::FAILURE)) {
        TopologyPlan plan;
        const TopologyState state = latest.CopyState();
        if (activeBatch->type == TopologyChangeType::SCALE_OUT) {
            RETURN_IF_NOT_OK(planBuilder_.BuildScaleOutReplan(state, classification.removeJoining, plan));
        } else if (activeBatch->type == TopologyChangeType::FAILURE) {
            std::vector<MemberIdentity> failed;
            for (const auto &member : latest.Members()) {
                if (member.state == MemberState::FAILED) {
                    failed.push_back(member.identity);
                }
            }
            RETURN_IF_NOT_OK(planBuilder_.BuildFailureStartOrReplan(state, failed, plan));
            EraseMembers(plan.next, classification.removeJoining);
        }
        EraseMembers(plan.next, classification.removeInitial);
        return CommitAndLogMemberTransition(latest, plan.next, classification.removeJoining,
                                            "remove_uncommitted_joining", expectedAuthorityRevision);
    }
    if (classification.removeInitial.empty()) {
        return Status::OK();
    }
    next = latest.CopyState();
    ++next.version;
    EraseMembers(next, classification.removeInitial);
    return CommitAndLogMemberTransition(latest, next, classification.removeInitial, "remove_initial",
                                        expectedAuthorityRevision);
}

Status TopologyController::CommitAndLogMemberTransition(const TopologySnapshot &latest, const TopologyState &next,
                                                        const std::vector<MemberIdentity> &members, const char *action,
                                                        int64_t expectedAuthorityRevision)
{
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed, expectedAuthorityRevision);
    if (rc.IsOk()) {
        LogMemberTransition(keys_.ClusterName(), action, members.size(), members, committed->Version());
    }
    return rc;
}

void TopologyController::CollectMembershipFacts(const std::vector<MembershipRecord> &memberships,
                                                std::unordered_set<std::string> &exiting,
                                                std::vector<MembershipRecord> &ready)
{
    for (const auto &record : memberships) {
        if (record.state == MemberLifecycleState::EXITING) {
            exiting.insert(record.address);
        }
        if (record.state == MemberLifecycleState::READY || record.state == MemberLifecycleState::RESTARTING) {
            auto quarantined = quarantinedReadyTimestampByAddress_.find(record.address);
            if (quarantined != quarantinedReadyTimestampByAddress_.end()) {
                if (record.timestamp <= quarantined->second) {
                    continue;
                }
                quarantinedReadyTimestampByAddress_.erase(quarantined);
            }
            ready.push_back(record);
        }
    }
    std::sort(ready.begin(), ready.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
}

void TopologyController::ApplyExitingMembershipFacts(TopologyState &next,
                                                     const std::unordered_set<std::string> &exiting,
                                                     std::unordered_set<std::string> &known,
                                                     std::vector<MemberIdentity> &admittedLeaving,
                                                     size_t &changed) const
{
    for (auto &member : next.members) {
        known.insert(member.identity.address);
        if (member.state == MemberState::ACTIVE && exiting.count(member.identity.address) > 0
            && changed < options_.maxMembersPerBatch) {
            admittedLeaving.push_back(member.identity);
            member.state = MemberState::PRE_LEAVING;
            ++changed;
        }
    }
}

Status TopologyController::ApplyReadyMembershipFacts(TopologyState &next, const std::vector<MembershipRecord> &ready,
                                                     std::unordered_set<std::string> &known,
                                                     std::vector<MemberIdentity> &admittedJoining,
                                                     size_t &changed) const
{
    for (const auto &record : ready) {
        const auto &address = record.address;
        if (known.count(address) > 0 || changed >= options_.maxMembersPerBatch) {
            continue;
        }
        std::string membershipKey;
        RETURN_IF_NOT_OK(TopologyKeyHelper::MembershipKey(address, membershipKey));
        std::string memberId;
        RETURN_IF_NOT_OK(BuildMemberId(record, memberId));
        MemberIdentity identity{ std::move(memberId), address };
        admittedJoining.push_back(identity);
        next.members.push_back({ std::move(identity), MemberState::INITIAL, {} });
        known.insert(address);
        ++changed;
    }
    return Status::OK();
}

bool TopologyController::ShouldApplyReadyScaleOutAdmission(
    const TopologySnapshot &latest, const std::vector<MembershipRecord> &ready,
    const std::unordered_set<std::string> &known)
{
    const auto hasUnknownReady = [&ready, &known]() {
        return std::any_of(ready.begin(), ready.end(), [&known](const auto &record) {
            return known.count(record.address) == 0;
        });
    };
    if (latest.GetActiveBatch().has_value()) {
        if (!latest.ClusterHasInit()) {
            return true;
        }
        const bool unknownReadyObserved = hasUnknownReady();
        if (!unknownReadyObserved) {
            if (scaleOutCollect_.has_value() && scaleOutCollect_->awaitingAdmission) {
                ClearBatchCollectState(TopologyChangeType::SCALE_OUT, "deferred_candidate_gone");
            }
            return false;
        }
        if (options_.scaleOutCollectWindow.count() > 0
            && (!scaleOutCollect_.has_value() || !scaleOutCollect_->awaitingAdmission)) {
            scaleOutCollect_ =
                BatchCollectState{ options_.now() + options_.scaleOutCollectWindow, false, true };
        }
        return false;
    }
    if (!scaleOutCollect_.has_value()) {
        return true;
    }
    if (!hasUnknownReady()) {
        return false;
    }
    std::vector<MemberIdentity> leaving;
    std::vector<MemberIdentity> joining;
    CollectNextBatchCandidates(latest, ready, leaving, joining, false);
    const bool hasReadyInitial = !joining.empty();
    if (!scaleOutCollect_->awaitingAdmission && !hasReadyInitial) {
        ClearBatchCollectState(TopologyChangeType::SCALE_OUT, "previous_cohort_consumed");
    }
    if (!scaleOutCollect_.has_value() || options_.now() < scaleOutCollect_->deadline) {
        return true;
    }
    if (scaleOutCollect_->awaitingAdmission && hasReadyInitial) {
        scaleOutCollect_->awaitingAdmission = false;
    }
    if (scaleOutCollect_->awaitingAdmission) {
        return true;
    }
    if (!hasReadyInitial) {
        ClearBatchCollectState(TopologyChangeType::SCALE_OUT, "collected_candidate_gone");
    }
    return !hasReadyInitial;
}

void TopologyController::LogMembershipFactsCommit(uint64_t committedVersion,
                                                  const std::vector<MemberIdentity> &admittedLeaving,
                                                  const std::vector<MemberIdentity> &admittedJoining) const
{
    if (!admittedJoining.empty()) {
        LogMemberTransition(keys_.ClusterName(), "ready_to_initial", admittedJoining.size(), admittedJoining,
                            committedVersion);
    }
    if (!admittedLeaving.empty()) {
        LogMemberTransition(keys_.ClusterName(), "active_to_pre_leaving", admittedLeaving.size(), admittedLeaving,
                            committedVersion);
    }
}

Status TopologyController::CommitMembershipFacts(const TopologySnapshot &latest,
                                                 const std::vector<MembershipRecord> &memberships)
{
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL && latest.ClusterHasInit()) {
        return Status::OK();
    }
    std::unordered_set<std::string> exiting;
    std::vector<MembershipRecord> ready;
    CollectMembershipFacts(memberships, exiting, ready);
    TopologyState next = latest.CopyState();
    ++next.version;
    size_t changed = 0;
    std::unordered_set<std::string> known;
    std::vector<MemberIdentity> admittedLeaving;
    std::vector<MemberIdentity> admittedJoining;
    ApplyExitingMembershipFacts(next, exiting, known, admittedLeaving, changed);
    if (ShouldApplyReadyScaleOutAdmission(latest, ready, known)) {
        RETURN_IF_NOT_OK(ApplyReadyMembershipFacts(next, ready, known, admittedJoining, changed));
    }
    if (changed == 0) {
        return Status::OK();
    }
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed);
    if (rc.IsOk()) {
        LogMembershipFactsCommit(committed->Version(), admittedLeaving, admittedJoining);
        if (!admittedLeaving.empty() && options_.scaleInCollectWindow.count() > 0 && !scaleInCollect_.has_value()) {
            scaleInCollect_ = BatchCollectState{ options_.now() + options_.scaleInCollectWindow };
        }
        if (latest.ClusterHasInit() && !admittedJoining.empty() && options_.scaleOutCollectWindow.count() > 0
            && !scaleOutCollect_.has_value()) {
            scaleOutCollect_ = BatchCollectState{ options_.now() + options_.scaleOutCollectWindow };
        }
        if (!admittedJoining.empty() && scaleOutCollect_.has_value()) {
            scaleOutCollect_->awaitingAdmission = false;
        }
    }
    return rc;
}

Status TopologyController::TryFinalizeActiveBatch(const TopologySnapshot &latest,
                                                  const std::vector<MembershipRecord> &memberships)
{
    if (!latest.GetActiveBatch().has_value()) {
        batchDeadline_.reset();
        progressReadCursor_ = 0;
        progressSweepRemaining_ = 0;
        progressTopologyVersion_ = 0;
        progressBatchEpoch_ = 0;
        progressWorkPending_ = false;
        finishedTaskIds_.clear();
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(derivedTopologyVersion_ == latest.Version(), K_TRY_AGAIN,
                             "derived generation does not match active topology");
    // Missing tasks or notifies are not callback progress until the bounded generation is fully materialized.
    if (derivedWorkPending_) {
        return Status::OK();
    }
    const auto &expected = expectedDerivedState_;
    bool complete = false;
    RETURN_IF_NOT_OK(InspectBatchProgress(latest, expected, complete));
    const auto now = options_.now();
    const auto &batch = *latest.GetActiveBatch();
    const bool preserveFailureDeadline = batchDeadline_.has_value()
                                         && batchDeadline_->batch.type == TopologyChangeType::FAILURE
                                         && batch.type == TopologyChangeType::FAILURE;
    const bool sameEpoch = batchDeadline_.has_value() && batchDeadline_->batch == batch;
    if (!batchDeadline_.has_value() || (!sameEpoch && !preserveFailureDeadline)) {
        const auto window = latest.GetActiveBatch()->type == TopologyChangeType::FAILURE
                                ? options_.failureBatchWindow
                                : std::chrono::duration_cast<std::chrono::seconds>(options_.ordinaryBatchWindow);
        batchDeadline_ = BatchDeadlineState{ batch, now + window };
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=deadline_set batch_type=" << TopologyChangeTypeName(batch.type)
                  << " batch_epoch=" << batch.epoch << " version=" << latest.Version()
                  << " window_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(window).count()
                  << " task_count=" << expected.tasks.size() << " notify_count=" << expected.notifiesByAddress.size();
    }
    if (complete) {
        return CommitBatchFinal(latest, memberships);
    }
    if (progressWorkPending_) {
        return Status::OK();
    }
    if (now < batchDeadline_->deadline) {
        return Status::OK();
    }
    std::vector<MemberIdentity> failedJoining;
    if (batch.type == TopologyChangeType::SCALE_OUT) {
        CollectFailedJoining(latest, expected, failedJoining);
    }
    return CommitExpiredBatch(latest, failedJoining, memberships);
}

Status TopologyController::CommitExpiredBatch(const TopologySnapshot &latest,
                                              const std::vector<MemberIdentity> &failedJoining,
                                              const std::vector<MembershipRecord> &memberships)
{
    const auto &batch = *latest.GetActiveBatch();
    if (loggedExpiredBatchEpoch_ != batch.epoch) {
        loggedExpiredBatchEpoch_ = batch.epoch;
        LOG(WARNING) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                     << " action=deadline_expired batch_type=" << TopologyChangeTypeName(batch.type)
                     << " batch_epoch=" << batch.epoch << " version=" << latest.Version()
                     << " failed_joining_count=" << failedJoining.size();
    }
    if (batch.type == TopologyChangeType::SCALE_OUT) {
        return CommitScaleOutExhaustion(latest, failedJoining, memberships);
    }
    if (batch.type == TopologyChangeType::SCALE_IN) {
        if (loggedScaleInWaitEpoch_ != batch.epoch) {
            loggedScaleInWaitEpoch_ = batch.epoch;
            LOG(WARNING) << "CLUSTER_CHANGE cluster=" << keys_.ClusterName()
                         << " decision=scalein_wait_external_termination version=" << latest.Version()
                         << " batch_epoch=" << batch.epoch;
        }
        return Status::OK();
    }
    return CommitBatchFinal(latest, memberships);
}

Status TopologyController::InspectBatchProgress(const TopologySnapshot &latest, const ExpectedDerivedState &expected,
                                                bool &complete)
{
    const auto &batch = *latest.GetActiveBatch();
    RETURN_IF_NOT_OK(RefreshTaskProgressCache(batch, expected));
    complete = finishedTaskIds_.size() == expected.tasks.size();
    return Status::OK();
}

void TopologyController::CollectFailedJoining(const TopologySnapshot &latest, const ExpectedDerivedState &expected,
                                              std::vector<MemberIdentity> &failedJoining) const
{
    std::unordered_set<std::string> incompleteTargets;
    for (const auto &task : expected.tasks) {
        if (finishedTaskIds_.count(TaskId(task)) == 0 && std::holds_alternative<TopologyMigrateTask>(task)) {
            incompleteTargets.insert(std::get<TopologyMigrateTask>(task).targetAddress);
        }
    }
    failedJoining.clear();
    for (const auto &member : latest.Members()) {
        if (member.state == MemberState::JOINING && incompleteTargets.count(member.identity.address) > 0) {
            failedJoining.push_back(member.identity);
        }
    }
}

Status TopologyController::RefreshTaskProgressCache(const ActiveBatch &batch, const ExpectedDerivedState &expected)
{
    if (progressBatchEpoch_ != batch.epoch || progressTopologyVersion_ != derivedTopologyVersion_) {
        progressTopologyVersion_ = derivedTopologyVersion_;
        progressBatchEpoch_ = batch.epoch;
        progressReadCursor_ = 0;
        progressSweepRemaining_ = expected.tasks.size();
        progressWorkPending_ = !expected.tasks.empty();
        finishedTaskIds_.clear();
    }
    const size_t total = expected.tasks.size();
    if (total == 0) {
        progressReadCursor_ = 0;
        progressSweepRemaining_ = 0;
        progressWorkPending_ = false;
        return Status::OK();
    }
    progressSweepRemaining_ = progressSweepRemaining_ == 0 ? total : std::min(progressSweepRemaining_, total);
    const auto startedAt = std::chrono::steady_clock::now();
    const size_t start = progressReadCursor_ % total;
    size_t visited = 0;
    size_t reads = 0;
    while (visited < progressSweepRemaining_ && reads < options_.maxProgressReadsPerTick
           && std::chrono::steady_clock::now() - startedAt < options_.derivedSliceBudget) {
        const auto &task = expected.tasks[(start + visited) % total];
        const std::string taskId = TaskId(task);
        ++visited;
        if (finishedTaskIds_.count(taskId) > 0) {
            continue;
        }
        TopologyTask observed;
        auto rc = repository_.ReadTask(TaskKind(task), taskId, batch.type, batch.epoch, observed);
        ++reads;
        if (rc.IsError() && rc.GetCode() != K_NOT_FOUND) {
            return rc;
        }
        if (rc.IsOk() && TaskFinished(observed)) {
            finishedTaskIds_.insert(taskId);
        }
    }
    progressReadCursor_ = (start + visited) % total;
    progressSweepRemaining_ -= visited;
    progressWorkPending_ = progressSweepRemaining_ > 0;
    return Status::OK();
}

std::vector<MemberIdentity> TopologyController::CollectBatchParticipants(const TopologySnapshot &latest,
                                                                         TopologyChangeType type) const
{
    std::vector<MemberIdentity> participants;
    for (const auto &member : latest.Members()) {
        if ((type == TopologyChangeType::SCALE_OUT && member.state == MemberState::JOINING)
            || (type == TopologyChangeType::SCALE_IN && member.state == MemberState::LEAVING)
            || (type == TopologyChangeType::FAILURE && member.state == MemberState::FAILED)) {
            participants.push_back(member.identity);
        }
    }
    return participants;
}

void TopologyController::RememberQuarantinedReadyMembers(const std::vector<MemberIdentity> &participants,
                                                         const std::vector<MembershipRecord> &memberships)
{
    for (const auto &identity : participants) {
        auto record = std::find_if(memberships.begin(), memberships.end(), [&](const auto &membership) {
            return membership.address == identity.address && membership.state == MemberLifecycleState::READY;
        });
        if (record != memberships.end()) {
            std::string memberId;
            if (BuildMemberId(*record, memberId).IsOk() && memberId == identity.id) {
                quarantinedReadyTimestampByAddress_[record->address] = record->timestamp;
            }
        }
    }
}

Status TopologyController::CommitBatchFinal(const TopologySnapshot &latest,
                                            const std::vector<MembershipRecord> &memberships)
{
    TopologyState next;
    TopologyState state = latest.CopyState();
    const auto batch = *latest.GetActiveBatch();
    auto participants = CollectBatchParticipants(latest, batch.type);
    if (batch.type == TopologyChangeType::SCALE_OUT) {
        RETURN_IF_NOT_OK(planBuilder_.BuildScaleOutFinal(state, next));
    } else if (batch.type == TopologyChangeType::SCALE_IN) {
        RETURN_IF_NOT_OK(planBuilder_.BuildScaleInFinal(state, next));
    } else {
        RETURN_IF_NOT_OK(planBuilder_.BuildFailureFinal(state, next));
        LOG(INFO) << "CLUSTER_FAILURE cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                  << " outcome=finalizing";
    }
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed);
    if (rc.IsOk()) {
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=finalized batch_type=" << TopologyChangeTypeName(batch.type)
                  << " batch_epoch=" << batch.epoch << " previous_version=" << latest.Version()
                  << " committed_version=" << committed->Version() << " participant_count=" << participants.size()
                  << " sample=" << MemberIdentitySample(participants);
        if (batch.type == TopologyChangeType::SCALE_OUT) {
            LOG(INFO) << "CLUSTER_MEMBER_JOIN_SUMMARY cluster=" << keys_.ClusterName()
                      << " result=success batch_epoch=" << batch.epoch << " joined_count=" << participants.size()
                      << " sample=" << MemberIdentitySample(participants)
                      << " committed_version=" << committed->Version();
        } else if (batch.type == TopologyChangeType::SCALE_IN) {
            LOG(INFO) << "CLUSTER_MEMBER_LEAVE_SUMMARY cluster=" << keys_.ClusterName()
                      << " result=success batch_epoch=" << batch.epoch << " left_count=" << participants.size()
                      << " sample=" << MemberIdentitySample(participants)
                      << " committed_version=" << committed->Version();
        } else {
            RememberQuarantinedReadyMembers(participants, memberships);
            LOG(INFO) << "CLUSTER_FAILURE cluster=" << keys_.ClusterName()
                      << " outcome=finalized batch_epoch=" << batch.epoch << " failed_count=" << participants.size()
                      << " sample=" << MemberIdentitySample(participants)
                      << " committed_version=" << committed->Version();
        }
    }
    return rc;
}

Status TopologyController::CommitScaleOutExhaustion(const TopologySnapshot &latest,
                                                    const std::vector<MemberIdentity> &failedJoining,
                                                    const std::vector<MembershipRecord> &memberships)
{
    CHECK_FAIL_RETURN_STATUS(!failedJoining.empty(), K_INVALID,
                             "expired ScaleOut has no incomplete joining generation");
    for (const auto &identity : failedJoining) {
        auto record = std::find_if(memberships.begin(), memberships.end(), [&](const auto &membership) {
            return membership.address == identity.address && membership.state == MemberLifecycleState::READY;
        });
        if (record != memberships.end()) {
            std::string memberId;
            if (BuildMemberId(*record, memberId).IsOk() && memberId == identity.id) {
                quarantinedReadyTimestampByAddress_[record->address] = record->timestamp;
            }
        }
    }
    TopologyState state = latest.CopyState();
    TopologyPlan plan;
    RETURN_IF_NOT_OK(planBuilder_.BuildScaleOutReplan(state, failedJoining, plan));
    LOG(WARNING) << "CLUSTER_CHANGE cluster=" << keys_.ClusterName() << " decision=scaleout_exhausted"
                 << " version=" << latest.Version() << " failed_joining_count=" << failedJoining.size()
                 << " sample=" << MemberIdentitySample(failedJoining);
    std::shared_ptr<const TopologySnapshot> committed;
    return CommitAndReadBack(latest.Version(), plan.next, committed);
}

Status TopologyController::TryStartNextBatch(const TopologySnapshot &latest,
                                             const std::vector<MembershipRecord> &memberships)
{
    const auto now = options_.now();
    const bool hasActiveBatch = latest.GetActiveBatch().has_value();
    const bool scaleOutCanStart =
        !hasActiveBatch
        && (options_.scaleOutCollectWindow.count() == 0
            || (scaleOutCollect_.has_value() && now >= scaleOutCollect_->deadline));
    std::vector<MemberIdentity> leaving;
    std::vector<MemberIdentity> joining;
    RETURN_IF_NOT_OK(CollectNextBatchCandidates(latest, memberships, leaving, joining, scaleOutCanStart));
    const bool bootstrap = latest.CommittedMembers().empty() && !joining.empty();
    const bool scaleOutReady = UpdateBatchCollectState(latest, joining, TopologyChangeType::SCALE_OUT, bootstrap, now);
    const bool scaleInReady = UpdateBatchCollectState(latest, leaving, TopologyChangeType::SCALE_IN, false, now);
    if (hasActiveBatch) {
        return Status::OK();
    }
    if (!joining.empty()) {
        if (!scaleOutReady) {
            return Status::OK();
        }
        return CommitBatchStart(latest, joining, TopologyChangeType::SCALE_OUT, bootstrap);
    }
    if (!leaving.empty() && scaleInReady) {
        return CommitBatchStart(latest, leaving, TopologyChangeType::SCALE_IN, false);
    }
    return Status::OK();
}

bool TopologyController::UpdateBatchCollectState(const TopologySnapshot &latest,
                                                 const std::vector<MemberIdentity> &participants,
                                                 TopologyChangeType type, bool bootstrap,
                                                 std::chrono::steady_clock::time_point now)
{
    auto &collect = type == TopologyChangeType::SCALE_IN ? scaleInCollect_ : scaleOutCollect_;
    if (participants.empty()) {
        if (type != TopologyChangeType::SCALE_OUT || !collect.has_value() || !collect->awaitingAdmission) {
            ClearBatchCollectState(type, "no_candidate");
        }
        return false;
    }
    const auto window =
        type == TopologyChangeType::SCALE_IN ? options_.scaleInCollectWindow : options_.scaleOutCollectWindow;
    if (window.count() == 0) {
        return true;
    }
    if (!collect.has_value()) {
        collect = BatchCollectState{ now + window };
    }
    const auto *action = bootstrap ? "bootstrap" : type == TopologyChangeType::SCALE_IN ? "scalein" : "scaleout";
    if (!collect->started) {
        collect->started = true;
        const auto remainingMs = std::chrono::duration_cast<std::chrono::milliseconds>(collect->deadline - now).count();
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName() << " action=" << action
                  << "_collect_start window_ms=" << window.count() << " deadline_in_ms=" << remainingMs
                  << " candidate_count=" << participants.size() << " sample=" << MemberIdentitySample(participants)
                  << " topology_version=" << latest.Version();
    }
    return now >= collect->deadline;
}

void TopologyController::ClearBatchCollectState(TopologyChangeType type, const char *reason)
{
    auto &collect = type == TopologyChangeType::SCALE_IN ? scaleInCollect_ : scaleOutCollect_;
    if (!collect.has_value()) {
        return;
    }
    if (collect->started) {
        const auto *action = type == TopologyChangeType::SCALE_IN ? "scalein" : "scaleout";
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName() << " action=" << action
                  << "_collect_cancel reason=" << reason;
    }
    collect.reset();
}

Status TopologyController::CollectNextBatchCandidates(const TopologySnapshot &latest,
                                                      const std::vector<MembershipRecord> &memberships,
                                                      std::vector<MemberIdentity> &leaving,
                                                      std::vector<MemberIdentity> &joining,
                                                      bool materializeNewMemberIds)
{
    std::unordered_set<std::string> exiting;
    std::vector<MembershipRecord> ready;
    CollectMembershipFacts(memberships, exiting, ready);
    std::unordered_set<std::string> readyAddresses;
    for (const auto &record : ready) {
        readyAddresses.insert(record.address);
    }
    std::unordered_set<std::string> known;
    const bool directAdmission =
        options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL && latest.ClusterHasInit();
    for (const auto &member : latest.Members()) {
        known.insert(member.identity.address);
        if (member.state == MemberState::PRE_LEAVING) {
            leaving.push_back(member.identity);
        } else if (directAdmission && member.state == MemberState::ACTIVE
                   && exiting.count(member.identity.address) > 0) {
            leaving.push_back(member.identity);
        }
        if (member.state == MemberState::INITIAL && readyAddresses.count(member.identity.address) > 0) {
            joining.push_back(member.identity);
        }
    }
    if (directAdmission) {
        for (const auto &record : ready) {
            if (known.count(record.address) > 0 || joining.size() >= options_.maxMembersPerBatch) {
                continue;
            }
            std::string memberId;
            if (materializeNewMemberIds) {
                RETURN_IF_NOT_OK(BuildMemberId(record, memberId));
            }
            joining.push_back({ std::move(memberId), record.address });
        }
    }
    LimitMembers(leaving, options_.maxMembersPerBatch);
    LimitMembers(joining, options_.maxMembersPerBatch);
    return Status::OK();
}

void TopologyController::PrepareBatchStartState(const TopologySnapshot &latest,
                                                const std::vector<MemberIdentity> &participants,
                                                TopologyChangeType type, TopologyState &state) const
{
    state = latest.CopyState();
    if (options_.eventSourceMode != TopologyEventSourceMode::EXTERNAL || !latest.ClusterHasInit()) {
        return;
    }
    if (type == TopologyChangeType::SCALE_OUT) {
        std::unordered_set<std::string> known;
        for (const auto &member : state.members) {
            known.insert(member.identity.address);
        }
        for (const auto &identity : participants) {
            if (known.count(identity.address) == 0) {
                state.members.push_back({ identity, MemberState::INITIAL, {} });
                known.insert(identity.address);
            }
        }
    } else if (type == TopologyChangeType::SCALE_IN) {
        std::unordered_set<std::string> selected;
        for (const auto &identity : participants) {
            selected.insert(identity.address);
        }
        for (auto &member : state.members) {
            if (member.state == MemberState::ACTIVE && selected.count(member.identity.address) > 0) {
                member.state = MemberState::PRE_LEAVING;
            }
        }
    }
}

Status TopologyController::CommitBatchStart(const TopologySnapshot &latest,
                                            const std::vector<MemberIdentity> &participants,
                                            TopologyChangeType type, bool bootstrap)
{
    TopologyState state;
    PrepareBatchStartState(latest, participants, type, state);
    TopologyState next;
    if (bootstrap) {
        RETURN_IF_NOT_OK(planBuilder_.BuildBootstrap(state, participants, next));
    } else {
        TopologyPlan plan;
        if (type == TopologyChangeType::SCALE_OUT) {
            RETURN_IF_NOT_OK(planBuilder_.BuildScaleOutStart(state, participants, plan));
        } else {
            CHECK_FAIL_RETURN_STATUS(type == TopologyChangeType::SCALE_IN, K_INVALID, "unsupported batch start type");
            RETURN_IF_NOT_OK(planBuilder_.BuildScaleInStart(state, participants, plan));
        }
        next = std::move(plan.next);
    }
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed);
    if (rc.IsOk()) {
        auto &collect = type == TopologyChangeType::SCALE_IN ? scaleInCollect_ : scaleOutCollect_;
        if (collect.has_value()) {
            const auto window =
                type == TopologyChangeType::SCALE_IN ? options_.scaleInCollectWindow : options_.scaleOutCollectWindow;
            const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       options_.now() - (collect->deadline - window))
                                       .count();
            const auto *action =
                bootstrap ? "bootstrap" : type == TopologyChangeType::SCALE_IN ? "scalein" : "scaleout";
            LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName() << " action=" << action
                      << "_collect_finish elapsed_ms=" << elapsedMs << " participant_count=" << participants.size()
                      << " sample=" << MemberIdentitySample(participants) << " topology_version=" << latest.Version();
            collect.reset();
        }
        LogBatchStart(latest, *committed, participants, "start");
    }
    return rc;
}

void TopologyController::LogBatchStart(const TopologySnapshot &latest, const TopologySnapshot &committed,
                                       const std::vector<MemberIdentity> &participants, const char *action) const
{
    const auto activeBatch = committed.GetActiveBatch();
    if (!activeBatch.has_value()) {
        return;
    }
    LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName() << " action=" << action
              << " batch_type=" << TopologyChangeTypeName(activeBatch->type) << " batch_epoch=" << activeBatch->epoch
              << " previous_version=" << latest.Version() << " committed_version=" << committed.Version()
              << " participant_count=" << participants.size() << " sample=" << MemberIdentitySample(participants);
}

Status TopologyController::CommitAndReadBack(uint64_t expectedVersion, const TopologyState &desired,
                                             std::shared_ptr<const TopologySnapshot> &committed,
                                             int64_t expectedAuthorityRevision)
{
    TopologyCasResult result;
    RETURN_IF_NOT_OK(
        repository_.CompareAndSwapTopology(expectedVersion, desired, result, expectedAuthorityRevision));
    CHECK_FAIL_RETURN_STATUS(result.outcome == TopologyCasOutcome::COMMITTED, K_TRY_AGAIN,
                             "topology CAS lost to another Controller");
    TopologyReader reader(repository_);
    RETURN_IF_NOT_OK(reader.Read(CONTROLLER_READ_TIMEOUT_MS, committed));
    CHECK_FAIL_RETURN_STATUS(committed->Version() >= desired.version, K_TRY_AGAIN,
                             "topology exact read-back is older than committed candidate");
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD) {
        RETURN_IF_NOT_OK(PublishExternalTopology(committed, true));
        topologyEventRevision_ = std::max(topologyEventRevision_, committed->AuthorityRevision());
    }
    topologyCommittedThisTick_ = true;
    const auto batchType =
        desired.activeBatch.has_value() ? std::to_string(static_cast<uint32_t>(desired.activeBatch->type)) : "none";
    const auto batchTypeName =
        desired.activeBatch.has_value() ? TopologyChangeTypeName(desired.activeBatch->type) : "none";
    const auto batchEpoch =
        desired.activeBatch.has_value() ? desired.activeBatch->epoch : TOPOLOGY_NO_ACTIVE_BATCH_EPOCH;
    LOG(INFO) << "CLUSTER_RING cluster=" << keys_.ClusterName() << " role=controller status=cas_committed"
              << " version=" << committed->Version() << " authority_revision=" << committed->AuthorityRevision()
              << " digest_prefix=" << TopologyDiagnosticPrefix(committed->CanonicalDigest())
              << " member_count=" << committed->Members().size() << " " << TopologyRingViewsForLog(*committed);
    LOG(INFO) << "CLUSTER_CHANGE cluster=" << keys_.ClusterName() << " version=" << committed->Version()
              << " expected_version=" << expectedVersion << " authority_revision=" << committed->AuthorityRevision()
              << " digest_prefix=" << TopologyDiagnosticPrefix(committed->CanonicalDigest())
              << " batch_type=" << batchType << " batch_type_name=" << batchTypeName << " batch_epoch=" << batchEpoch
              << " member_count=" << desired.members.size() << " decision=committed";
    return Status::OK();
}

TopologyControllerDiagnostics TopologyController::GetDiagnostics() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    auto diagnostics = diagnostics_;
    diagnostics.queuedEvents = dispatcher_.GetStats().queueDepth;
    return diagnostics;
}

}  // namespace datasystem::cluster
