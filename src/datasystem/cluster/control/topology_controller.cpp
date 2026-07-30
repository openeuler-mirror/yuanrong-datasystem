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
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

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

Status BuildWatchedTopology(const CoordinationEvent &event,
                            std::shared_ptr<const TopologySnapshot> &snapshot)
{
    TopologyState state;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::DecodeTopology(event.value, state));
    std::string canonical;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(state, canonical));
    std::string digest;
    RETURN_IF_NOT_OK(Hasher().GetSha256Hex(canonical, digest));
    return TopologySnapshot::Create(std::move(state), event.revision, std::move(digest), snapshot);
}

std::string MembershipDigest(const std::vector<MembershipRecord> &memberships)
{
    std::string seed;
    for (const auto &record : memberships) {
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

const Member *FailureProbeTargetOwnedBy(const TopologySnapshot &latest, const std::string &localAddress)
{
    const auto &committed = latest.CommittedMembers();
    const auto local =
        std::find_if(committed.begin(), committed.end(), [&](const Member *member) {
            return member->identity.address == localAddress;
        });
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
            const auto &ranges = [&]() -> const std::vector<TopologyTaskRange> &{
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

TopologyTaskKind TaskKind(const TopologyTask &task)
{
    return std::holds_alternative<TopologyMigrateTask>(task) ? TopologyTaskKind::MIGRATE
                                                             : TopologyTaskKind::DELETE_MEMBER;
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
    if (committed.size() <= 1) {
        return false;
    }
    return std::none_of(committed.begin(), committed.end(), [&](const Member *member) {
        return std::any_of(memberships.begin(), memberships.end(), [&](const MembershipRecord &membership) {
            return membership.address == member->identity.address;
        });
    });
}

void LogMemberTransition(const std::string &clusterName, const char *action, size_t count,
                         const std::vector<MemberIdentity> &members, uint64_t committedVersion)
{
    LOG(INFO) << "CLUSTER_MEMBER_TRANSITION cluster=" << clusterName << " action=" << action
              << " count=" << count << " sample=" << MemberIdentitySample(members)
              << " committed_version=" << committedVersion;
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
        << " node_dead_timeout_ms="
        << std::chrono::duration_cast<std::chrono::milliseconds>(nodeDeadTimeout).count();
}
}  // namespace

bool TopologyControllerOptions::IsValid() const noexcept
{
    return nodeDeadTimeout.count() >= 0 && failureBatchWindow.count() > 0 && ordinaryBatchWindow.count() > 0
           && reconcileTick.count() > 0 && failureProbeTimeout.count() > 0 && maxDerivedOperationsPerTick > 0
           && maxMembersPerBatch > 0 && maxProgressReadsPerTick > 0 && derivedSliceBudget.count() > 0 && now
           && scaleOutCollectWindow.count() >= 0
           && scaleOutCollectWindow.count() <= MAX_SCALE_OUT_COLLECT_WINDOW_MS
           && scaleInCollectWindow.count() >= 0
           && scaleInCollectWindow.count() <= MAX_SCALE_IN_COLLECT_WINDOW_MS
           && (!memberLivenessProbe || !localAddress.empty())
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
      dispatcher_(dispatcher)
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
    CHECK_FAIL_RETURN_STATUS(!started_ && options_.IsValid(),
                             K_INVALID, "invalid or already started topology Controller");
    std::vector<WatchKey> watches;
    if (options_.eventSourceMode == TopologyEventSourceMode::SELF_MANAGED) {
        RETURN_IF_NOT_OK(TopologyRoleWatchPlan::Build(TopologyRuntimeRole::CONTROLLER, "", keys_, 0, watches));
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
        << "CLUSTER_WATCH_EVENT cluster=" << keys_.ClusterName() << " role=controller event="
        << event.ToString();
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

Status TopologyController::PublishExternalTopology(std::shared_ptr<const TopologySnapshot> candidate,
                                                   bool fullRebuild)
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
    const auto kind = keys_.ClassifyEtcdWatchKey(event.key, "");
    if (kind == TopologyEtcdKeyKind::TOPOLOGY) {
        if (event.type != CoordinationEventType::PUT || event.value.empty() || event.revision <= 0
            || event.version <= 0) {
            RETURN_STATUS(K_NOT_READY, "external topology event requires an exact rebuild");
        }
        if (event.revision <= topologyEventRevision_) {
            return Status::OK();
        }
        std::shared_ptr<const TopologySnapshot> candidate;
        RETURN_IF_NOT_OK(BuildWatchedTopology(event, candidate));
        RETURN_IF_NOT_OK(PublishExternalTopology(std::move(candidate), false));
        topologyEventRevision_ = event.revision;
        topologyDirty_ = true;
        return Status::OK();
    }
    if (kind == TopologyEtcdKeyKind::MEMBERSHIP) {
        CHECK_FAIL_RETURN_STATUS(event.key.rfind(membershipEventPrefix_, 0) == 0, K_INVALID,
                                 "external membership event prefix is invalid");
        const std::string address = event.key.substr(membershipEventPrefix_.size());
        std::string canonicalAddress;
        RETURN_IF_NOT_OK(TopologyKeyHelper::MembershipKey(address, canonicalAddress));
        CHECK_FAIL_RETURN_STATUS(canonicalAddress == address, K_INVALID,
                                 "external membership event key is not exact");
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
    if (kind == TopologyEtcdKeyKind::MIGRATE_TASK || kind == TopologyEtcdKeyKind::DELETE_TASK) {
        taskDirty_ = true;
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
                         << " action=deliver_failed address=" << address
                         << " timestamp=" << timestamp << " status=" << rc.ToString();
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
        if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD
            && !externalEventSourceReady_) {
            continue;
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

Status TopologyController::WaitForReconcile(bool immediate, size_t &drained)
{
    const auto consumeEvent = [this](RuntimeEvent &event) {
        if (options_.eventSourceMode != TopologyEventSourceMode::EXTERNAL_ETCD) {
            topologyDirty_ = true;
            membershipDirty_ = true;
            taskDirty_ = true;
            return;
        }
        const auto *coordination = std::get_if<CoordinationEvent>(&event.payload);
        externalEventSourceReady_ = true;
        if (coordination == nullptr || ApplyExternalEvent(*coordination).IsError()) {
            externalResyncRequired_ = true;
            failureClassifier_.Pause(options_.now());
        }
    };
    drained = 0;
    RuntimeEvent event;
    auto wakeDeadline =
        immediate ? std::chrono::steady_clock::now() : std::chrono::steady_clock::now() + options_.reconcileTick;
    if (scaleInCollectDeadline_.has_value() && *scaleInCollectDeadline_ < wakeDeadline) {
        wakeDeadline = *scaleInCollectDeadline_;
    }
    if (scaleOutCollectDeadline_.has_value() && *scaleOutCollectDeadline_ < wakeDeadline) {
        wakeDeadline = *scaleOutCollectDeadline_;
    }
    auto rc = dispatcher_.WaitPop(wakeDeadline, event);
    if (rc.IsError() && rc.GetCode() != K_RPC_DEADLINE_EXCEEDED) {
        return rc;
    }
    DrainMembershipRestarts();
    if (rc.IsError()) {
        return Status::OK();
    }
    consumeEvent(event);
    drained = FIRST_DRAINED_EVENT_COUNT;
    while (drained < MAX_DOORBELLS_PER_RECONCILE
           && dispatcher_.WaitPop(std::chrono::steady_clock::now(), event).IsOk()) {
        consumeEvent(event);
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
    }
    const bool externalResyncNeedsBackoff =
        status.GetCode() == K_TRY_AGAIN && options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL_ETCD
        && externalResyncRequired_;
    if (status.GetCode() == K_TRY_AGAIN && !externalResyncNeedsBackoff) {
        consecutiveReconcileFailures_ = 0;
        reconcileNotBefore_ = {};
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL)
            << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName()
            << " action=cas_conflict backoff_ms=0 status=" << status.ToString();
    } else if (status.GetCode() == K_NOT_READY) {
        consecutiveReconcileFailures_ = 0;
        reconcileNotBefore_ = {};
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL)
            << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName()
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
            << "CLUSTER_RECONCILE cluster=" << keys_.ClusterName() << " role=controller drained_events="
            << drained << " committed=" << topologyCommittedThisTick_
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
        topologyDirty_ = false;
        membershipDirty_ = false;
        taskDirty_ = false;
    }
    const bool backendUnavailable =
        rc.GetCode() == K_RPC_UNAVAILABLE || rc.GetCode() == K_RPC_DEADLINE_EXCEEDED || rc.GetCode() == K_RPC_CANCELLED;
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
                      << " topology_revision=" << latest->AuthorityRevision()
                      << " member_count=" << memberships.size()
                      << " state_counts=" << MembershipStateCounts(memberships)
                      << " digest_prefix=" << TopologyDiagnosticPrefix(membershipDigest)
                      << " sample=" << MembershipSample(memberships);
        }
    }
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
    const auto membershipDigest = options_.materializeRestartFacts ? MembershipDigest(memberships) : "";
    if (derivedTopologyVersion_ == latest.Version() && derivedMembershipDigest_ == membershipDigest) {
        return Status::OK();
    }
    ExpectedDerivedState candidate;
    RETURN_IF_NOT_OK(materializer_.RebuildExpected(
        latest, algorithm_, memberships, options_.materializeRestartFacts, candidate));
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
                  << " task_count=" << expected.tasks.size()
                  << " notify_count=" << expected.notifyRecipients.size()
                  << " elapsed_ms=" << elapsedMs;
    }
    if (elapsedMs > DERIVED_SLICE_WARN_THRESHOLD_MS) {
        LOG(WARNING) << "CLUSTER_DERIVED_SLICE cluster=" << keys_.ClusterName()
                     << " version=" << derivedTopologyVersion_ << " cursor=" << admissionCursor_
                     << " total=" << total << " operations=" << operations << " elapsed_ms=" << elapsedMs
                     << " pending=" << derivedWorkPending_;
    } else {
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL)
            << "CLUSTER_DERIVED_SLICE cluster=" << keys_.ClusterName()
            << " version=" << derivedTopologyVersion_ << " cursor=" << admissionCursor_
            << " total=" << total << " operations=" << operations << " elapsed_ms=" << elapsedMs
            << " pending=" << derivedWorkPending_;
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
        return CommitClusterShutdown(latest);
    }
    if (IsCollectiveCommittedMembershipMissing(latest, memberships)) {
        failureClassifier_.Reset();
        LOG_EVERY_N(WARNING, TOPOLOGY_RECONCILE_LOG_INTERVAL)
            << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
            << " action=collective_membership_absence_suppressed committed_count="
            << latest.CommittedMembers().size() << " membership_count=" << memberships.size();
        return Status::OK();
    }
    FailureClassification classification;
    RETURN_IF_NOT_OK(failureClassifier_.Observe(latest, memberships, options_.now(), classification));
    for (const auto &observed : classification.newlyMissing) {
        LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                     << " version=" << latest.Version() << " address=" << observed.identity.address
                     << " member_id_prefix=" << MemberIdForLog(observed.identity.id)
                     << " state=" << MemberStateName(observed.state)
                     << " action=missing_first_observed missing_ms=" << observed.missingMs
                     << " node_dead_timeout_ms="
                     << std::chrono::duration_cast<std::chrono::milliseconds>(options_.nodeDeadTimeout).count();
    }
    for (const auto &observed : classification.restored) {
        LOG(INFO) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                  << " address=" << observed.identity.address
                  << " member_id_prefix=" << MemberIdForLog(observed.identity.id)
                  << " state=" << MemberStateName(observed.state)
                  << " action=missing_resolved missing_ms=" << observed.missingMs;
    }
    if (options_.eventSourceMode == TopologyEventSourceMode::EXTERNAL
        && !classification.confirmedMissing.empty()) {
        LogDirectFailureConfirmation(
            keys_.ClusterName(), latest.Version(), classification.confirmedMissing, options_.nodeDeadTimeout);
    }
    RETURN_IF_NOT_OK(ConfirmMissingMembersUnreachable(latest, classification));
    if (!classification.confirmedFailure.empty()) {
        return CommitConfirmedFailures(latest, classification);
    }
    if (!classification.removeInitial.empty() || !classification.removeJoining.empty()) {
        return CommitUncommittedCleanup(latest, classification);
    }
    return CommitMembershipFacts(latest, memberships);
}

Status TopologyController::ConfirmMissingMembersUnreachable(const TopologySnapshot &latest,
                                                            FailureClassification &classification)
{
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

    std::vector<ControlBackendObservation> observations;
    if (!targets.empty()) {
        try {
            observations =
                options_.memberLivenessProbe(targets, std::chrono::steady_clock::now() + options_.failureProbeTimeout);
        } catch (const std::exception &error) {
            RETURN_STATUS(K_RUNTIME_ERROR, std::string("member liveness probe threw: ") + error.what());
        } catch (...) {
            RETURN_STATUS(K_RUNTIME_ERROR, "member liveness probe threw an unknown exception");
        }
    }

    std::unordered_map<std::string, const ControlBackendObservation *> observationsByAddress;
    observationsByAddress.reserve(observations.size());
    for (const auto &observation : observations) {
        observationsByAddress.emplace(observation.reporter.address, &observation);
    }
    std::unordered_set<std::string> directlyUnreachable;
    directlyUnreachable.reserve(targets.size());
    const auto unreachableConfirmationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               options_.nodeDeadTimeout + options_.failureProbeTimeout)
                                               .count();
    for (size_t index = 0; index < targets.size(); ++index) {
        const auto &target = targets[index];
        const auto observed = observationsByAddress.find(target.address);
        if (observed == observationsByAddress.end()) {
            if (owned[index]->missingMs < unreachableConfirmationMs) {
                LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName()
                             << " version=" << latest.Version() << " address=" << target.address
                             << " member_id_prefix=" << MemberIdForLog(target.id)
                             << " action=direct_probe_retry missing_ms="
                             << owned[index]->missingMs
                             << " confirmation_ms=" << unreachableConfirmationMs;
                continue;
            }
            directlyUnreachable.emplace(target.address);
            LOG(WARNING) << "CLUSTER_FAILURE_DETECT cluster=" << keys_.ClusterName() << " version=" << latest.Version()
                         << " address=" << target.address << " member_id_prefix=" << MemberIdForLog(target.id)
                         << " action=direct_probe_unreachable";
            continue;
        }
        const auto &evidence = *observed->second;
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
                     << " evidence_version=" << evidence.topologyVersion
                     << " evidence_revision=" << evidence.topologyRevision;
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
    TopologyState state{ latest.ClusterHasInit(), latest.Version(), latest.Members(), std::nullopt };
    for (auto &member : state.members) {
        member.state = MemberState::PRE_LEAVING;
    }
    TopologyState next;
    RETURN_IF_NOT_OK(planBuilder_.BuildClusterShutdownFinal(state, next));
    std::shared_ptr<const TopologySnapshot> committed;
    return CommitAndReadBack(latest.Version(), next, committed);
}

Status TopologyController::CommitConfirmedFailures(const TopologySnapshot &latest,
                                                   const FailureClassification &classification)
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
    const bool replan = latest.GetActiveBatch().has_value()
                        && latest.GetActiveBatch()->type == TopologyChangeType::FAILURE;
    LOG(WARNING) << "CLUSTER_FAILURE cluster=" << keys_.ClusterName() << " action=plan version=" << latest.Version()
                 << " confirmed_count=" << confirmed.size()
                 << " confirmed_missing_count=" << classification.confirmedMissing.size()
                 << " sample=" << MemberIdentitySample(confirmed)
                 << " outcome=" << (replan ? "replan_pending" : "start_pending");
    TopologyPlan plan;
    RETURN_IF_NOT_OK(planBuilder_.BuildFailureStartOrReplan(
        { latest.ClusterHasInit(), latest.Version(), latest.Members(), latest.GetActiveBatch() }, confirmed, plan));
    EraseMembers(plan.next, classification.removeInitial);
    EraseMembers(plan.next, classification.removeJoining);
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), plan.next, committed);
    if (rc.IsOk()) {
        LogBatchStart(latest, *committed, confirmed, replan ? "replan" : "start");
    }
    return rc;
}

Status TopologyController::CommitUncommittedCleanup(const TopologySnapshot &latest,
                                                    const FailureClassification &classification)
{
    if (!classification.removeJoining.empty() && latest.GetActiveBatch().has_value()
        && latest.GetActiveBatch()->type == TopologyChangeType::SCALE_OUT) {
        return CommitScaleOutUncommittedCleanup(latest, classification);
    }
    if (!classification.removeJoining.empty() && latest.GetActiveBatch().has_value()
        && latest.GetActiveBatch()->type == TopologyChangeType::FAILURE) {
        return CommitFailureUncommittedCleanup(latest, classification);
    }
    if (!classification.removeInitial.empty()) {
        return CommitInitialCleanup(latest, classification);
    }
    return Status::OK();
}

Status TopologyController::CommitScaleOutUncommittedCleanup(const TopologySnapshot &latest,
                                                            const FailureClassification &classification)
{
    TopologyPlan plan;
    RETURN_IF_NOT_OK(planBuilder_.BuildScaleOutReplan(
        { latest.ClusterHasInit(), latest.Version(), latest.Members(), latest.GetActiveBatch() },
        classification.removeJoining, plan));
    EraseMembers(plan.next, classification.removeInitial);
    return CommitAndLogMemberTransition(
        latest, plan.next, classification.removeJoining, "remove_uncommitted_joining");
}

Status TopologyController::CommitFailureUncommittedCleanup(const TopologySnapshot &latest,
                                                           const FailureClassification &classification)
{
    std::vector<MemberIdentity> failed;
    for (const auto &member : latest.Members()) {
        if (member.state == MemberState::FAILED) {
            failed.push_back(member.identity);
        }
    }
    TopologyPlan plan;
    RETURN_IF_NOT_OK(planBuilder_.BuildFailureStartOrReplan(
        { latest.ClusterHasInit(), latest.Version(), latest.Members(), latest.GetActiveBatch() }, failed, plan));
    EraseMembers(plan.next, classification.removeJoining);
    EraseMembers(plan.next, classification.removeInitial);
    return CommitAndLogMemberTransition(
        latest, plan.next, classification.removeJoining, "remove_uncommitted_joining");
}

Status TopologyController::CommitInitialCleanup(const TopologySnapshot &latest,
                                                const FailureClassification &classification)
{
    TopologyState next{ latest.ClusterHasInit(), latest.Version() + 1, latest.Members(), latest.GetActiveBatch() };
    EraseMembers(next, classification.removeInitial);
    return CommitAndLogMemberTransition(latest, next, classification.removeInitial, "remove_initial");
}

Status TopologyController::CommitAndLogMemberTransition(const TopologySnapshot &latest, const TopologyState &next,
                                                        const std::vector<MemberIdentity> &members, const char *action)
{
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed);
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
        if (record.state == MemberLifecycleState::READY) {
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

void TopologyController::LogMembershipFactsCommit(uint64_t committedVersion,
                                                  const std::vector<MemberIdentity> &admittedLeaving,
                                                  const std::vector<MemberIdentity> &admittedJoining) const
{
    if (!admittedJoining.empty()) {
        LogMemberTransition(keys_.ClusterName(), "ready_to_initial", admittedJoining.size(),
                            admittedJoining, committedVersion);
    }
    if (!admittedLeaving.empty()) {
        LogMemberTransition(keys_.ClusterName(), "active_to_pre_leaving", admittedLeaving.size(),
                            admittedLeaving, committedVersion);
    }
}

Status TopologyController::CommitMembershipFacts(const TopologySnapshot &latest,
                                                 const std::vector<MembershipRecord> &memberships)
{
    std::unordered_set<std::string> exiting;
    std::vector<MembershipRecord> ready;
    CollectMembershipFacts(memberships, exiting, ready);
    TopologyState next{ latest.ClusterHasInit(), latest.Version() + 1, latest.Members(), latest.GetActiveBatch() };
    size_t changed = 0;
    std::unordered_set<std::string> known;
    std::vector<MemberIdentity> admittedLeaving;
    std::vector<MemberIdentity> admittedJoining;
    ApplyExitingMembershipFacts(next, exiting, known, admittedLeaving, changed);
    RETURN_IF_NOT_OK(ApplyReadyMembershipFacts(next, ready, known, admittedJoining, changed));
    if (changed == 0) {
        return Status::OK();
    }
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed);
    if (rc.IsOk()) {
        LogMembershipFactsCommit(committed->Version(), admittedLeaving, admittedJoining);
        if (!admittedLeaving.empty() && options_.scaleInCollectWindow.count() > 0
            && !scaleInCollectDeadline_.has_value()) {
            scaleInCollectDeadline_ = options_.now() + options_.scaleInCollectWindow;
            scaleInCollectStarted_ = false;
        }
        if (latest.ClusterHasInit() && !admittedJoining.empty() && options_.scaleOutCollectWindow.count() > 0
            && !scaleOutCollectDeadline_.has_value()) {
            scaleOutCollectDeadline_ = options_.now() + options_.scaleOutCollectWindow;
            scaleOutCollectStarted_ = false;
        }
    }
    return rc;
}

Status TopologyController::TryFinalizeActiveBatch(const TopologySnapshot &latest,
                                                  const std::vector<MembershipRecord> &memberships)
{
    if (!latest.GetActiveBatch().has_value()) {
        batchDeadline_.reset();
        deadlineBatchType_.reset();
        deadlineBatchEpoch_ = 0;
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
    const bool preserveFailureDeadline =
        deadlineBatchType_ == TopologyChangeType::FAILURE && batch.type == TopologyChangeType::FAILURE;
    const bool sameEpoch = deadlineBatchType_ == batch.type && deadlineBatchEpoch_ == batch.epoch;
    if (!batchDeadline_.has_value() || (!sameEpoch && !preserveFailureDeadline)) {
        const auto window = latest.GetActiveBatch()->type == TopologyChangeType::FAILURE
                                ? options_.failureBatchWindow
                                : std::chrono::duration_cast<std::chrono::seconds>(options_.ordinaryBatchWindow);
        batchDeadline_ = now + window;
        deadlineBatchType_ = batch.type;
        deadlineBatchEpoch_ = batch.epoch;
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=deadline_set batch_type=" << TopologyChangeTypeName(batch.type)
                  << " batch_epoch=" << batch.epoch << " version=" << latest.Version()
                  << " window_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(window).count()
                  << " task_count=" << expected.tasks.size()
                  << " notify_count=" << expected.notifiesByAddress.size();
    }
    if (complete) {
        return CommitBatchFinal(latest);
    }
    if (progressWorkPending_) {
        return Status::OK();
    }
    if (now < *batchDeadline_) {
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
    return CommitBatchFinal(latest);
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
    progressSweepRemaining_ =
        progressSweepRemaining_ == 0 ? total : std::min(progressSweepRemaining_, total);
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

Status TopologyController::CommitBatchFinal(const TopologySnapshot &latest)
{
    TopologyState next;
    TopologyState state{ latest.ClusterHasInit(), latest.Version(), latest.Members(), latest.GetActiveBatch() };
    const auto batch = *latest.GetActiveBatch();
    std::vector<MemberIdentity> participants;
    for (const auto &member : latest.Members()) {
        if ((batch.type == TopologyChangeType::SCALE_OUT && member.state == MemberState::JOINING)
            || (batch.type == TopologyChangeType::SCALE_IN && member.state == MemberState::LEAVING)
            || (batch.type == TopologyChangeType::FAILURE && member.state == MemberState::FAILED)) {
            participants.push_back(member.identity);
        }
    }
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
                  << " committed_version=" << committed->Version()
                  << " participant_count=" << participants.size()
                  << " sample=" << MemberIdentitySample(participants);
        if (batch.type == TopologyChangeType::SCALE_OUT) {
            LOG(INFO) << "CLUSTER_MEMBER_JOIN_SUMMARY cluster=" << keys_.ClusterName()
                      << " result=success batch_epoch=" << batch.epoch
                      << " joined_count=" << participants.size()
                      << " sample=" << MemberIdentitySample(participants)
                      << " committed_version=" << committed->Version();
        } else if (batch.type == TopologyChangeType::SCALE_IN) {
            LOG(INFO) << "CLUSTER_MEMBER_LEAVE_SUMMARY cluster=" << keys_.ClusterName()
                      << " result=success batch_epoch=" << batch.epoch
                      << " left_count=" << participants.size()
                      << " sample=" << MemberIdentitySample(participants)
                      << " committed_version=" << committed->Version();
        } else {
            LOG(INFO) << "CLUSTER_FAILURE cluster=" << keys_.ClusterName()
                      << " outcome=finalized batch_epoch=" << batch.epoch
                      << " failed_count=" << participants.size()
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
            quarantinedReadyTimestampByAddress_[record->address] = record->timestamp;
        }
    }
    TopologyState state{ latest.ClusterHasInit(), latest.Version(), latest.Members(), latest.GetActiveBatch() };
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
    if (latest.GetActiveBatch().has_value()) {
        ClearScaleInCollectState("active_batch");
        ClearScaleOutCollectState("active_batch");
        return Status::OK();
    }
    std::vector<MemberIdentity> leaving;
    std::vector<MemberIdentity> joining;
    CollectNextBatchCandidates(latest, memberships, leaving, joining);
    TopologyState state{ latest.ClusterHasInit(), latest.Version(), latest.Members(), latest.GetActiveBatch() };
    if (latest.CommittedMembers().empty() && !joining.empty()) {
        ClearScaleInCollectState("bootstrap_candidate");
        return TryStartJoiningBatchAfterCollection(latest, state, joining, true);
    }
    if (!leaving.empty()) {
        ClearScaleOutCollectState("scale_in_candidate");
        return TryStartScaleInBatchAfterCollection(latest, state, leaving);
    }
    if (!joining.empty()) {
        ClearScaleInCollectState("scale_out_candidate");
        return TryStartJoiningBatchAfterCollection(latest, state, joining, false);
    }
    ClearScaleInCollectState("no_candidate");
    ClearScaleOutCollectState("no_candidate");
    return Status::OK();
}

Status TopologyController::TryStartJoiningBatchAfterCollection(const TopologySnapshot &latest,
                                                               const TopologyState &state,
                                                               const std::vector<MemberIdentity> &joining,
                                                               bool bootstrap)
{
    if (options_.scaleOutCollectWindow.count() == 0) {
        return bootstrap ? CommitBootstrapBatchStart(latest, state, joining)
                         : CommitOrdinaryBatchStart(latest, state, joining, TopologyChangeType::SCALE_OUT);
    }
    if (!scaleOutCollectDeadline_.has_value()) {
        scaleOutCollectDeadline_ = options_.now() + options_.scaleOutCollectWindow;
        scaleOutCollectStarted_ = false;
    }
    const auto now = options_.now();
    if (!scaleOutCollectStarted_) {
        scaleOutCollectStarted_ = true;
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=" << (bootstrap ? "bootstrap_collect_start" : "scaleout_collect_start")
                  << " window_ms=" << options_.scaleOutCollectWindow.count()
                  << " candidate_count=" << joining.size() << " sample=" << MemberIdentitySample(joining)
                  << " topology_version=" << latest.Version();
    }
    if (now < *scaleOutCollectDeadline_) {
        return Status::OK();
    }
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - (*scaleOutCollectDeadline_ - options_.scaleOutCollectWindow)).count();
    LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
              << " action=" << (bootstrap ? "bootstrap_collect_finish" : "scaleout_collect_finish")
              << " elapsed_ms=" << elapsedMs
              << " participant_count=" << joining.size() << " sample=" << MemberIdentitySample(joining)
              << " topology_version=" << latest.Version();
    scaleOutCollectDeadline_.reset();
    scaleOutCollectStarted_ = false;
    return bootstrap ? CommitBootstrapBatchStart(latest, state, joining)
                     : CommitOrdinaryBatchStart(latest, state, joining, TopologyChangeType::SCALE_OUT);
}

Status TopologyController::TryStartScaleInBatchAfterCollection(const TopologySnapshot &latest,
                                                               const TopologyState &state,
                                                               const std::vector<MemberIdentity> &leaving)
{
    // 0ms disables coalescing: start immediately, preserving the legacy one-reconcile admission path.
    if (options_.scaleInCollectWindow.count() == 0) {
        return CommitOrdinaryBatchStart(latest, state, leaving, TopologyChangeType::SCALE_IN);
    }
    if (!scaleInCollectDeadline_.has_value()) {
        scaleInCollectDeadline_ = options_.now() + options_.scaleInCollectWindow;
        scaleInCollectStarted_ = false;
    }
    const auto now = options_.now();
    if (!scaleInCollectStarted_) {
        scaleInCollectStarted_ = true;
        const auto windowMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(options_.scaleInCollectWindow).count();
        const auto remainingMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(*scaleInCollectDeadline_ - now).count();
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=scalein_collect_start window_ms=" << windowMs << " deadline_in_ms=" << remainingMs
                  << " candidate_count=" << leaving.size() << " sample=" << MemberIdentitySample(leaving)
                  << " topology_version=" << latest.Version();
    }
    if (now < *scaleInCollectDeadline_) {
        return Status::OK();
    }
    // Deadline reached. Log finish once, then drop the window state so a CAS loss (K_TRY_AGAIN) below
    // cannot leave a stale deadline for the next reconcile.
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - (*scaleInCollectDeadline_ - options_.scaleInCollectWindow)).count();
    LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
              << " action=scalein_collect_finish elapsed_ms=" << elapsedMs
              << " participant_count=" << leaving.size() << " sample=" << MemberIdentitySample(leaving)
              << " topology_version=" << latest.Version();
    scaleInCollectDeadline_.reset();
    scaleInCollectStarted_ = false;
    return CommitOrdinaryBatchStart(latest, state, leaving, TopologyChangeType::SCALE_IN);
}

void TopologyController::ClearScaleInCollectState(const char *reason)
{
    if (!scaleInCollectDeadline_.has_value() && !scaleInCollectStarted_) {
        return;
    }
    if (scaleInCollectStarted_) {
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=scalein_collect_cancel reason=" << reason;
    }
    scaleInCollectDeadline_.reset();
    scaleInCollectStarted_ = false;
}

void TopologyController::ClearScaleOutCollectState(const char *reason)
{
    if (!scaleOutCollectDeadline_.has_value() && !scaleOutCollectStarted_) {
        return;
    }
    if (scaleOutCollectStarted_) {
        LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
                  << " action=scaleout_collect_cancel reason=" << reason;
    }
    scaleOutCollectDeadline_.reset();
    scaleOutCollectStarted_ = false;
}

void TopologyController::CollectNextBatchCandidates(const TopologySnapshot &latest,
                                                    const std::vector<MembershipRecord> &memberships,
                                                    std::vector<MemberIdentity> &leaving,
                                                    std::vector<MemberIdentity> &joining) const
{
    std::unordered_set<std::string> ready;
    for (const auto &record : memberships) {
        if (record.state == MemberLifecycleState::READY) {
            ready.insert(record.address);
        }
    }
    for (const auto &member : latest.Members()) {
        if (member.state == MemberState::PRE_LEAVING) {
            leaving.push_back(member.identity);
        }
        if (member.state == MemberState::INITIAL && ready.count(member.identity.address) > 0) {
            joining.push_back(member.identity);
        }
    }
    LimitMembers(leaving, options_.maxMembersPerBatch);
    LimitMembers(joining, options_.maxMembersPerBatch);
}

Status TopologyController::CommitBootstrapBatchStart(const TopologySnapshot &latest, const TopologyState &state,
                                                     const std::vector<MemberIdentity> &joining)
{
    TopologyState next;
    RETURN_IF_NOT_OK(planBuilder_.BuildBootstrap(state, joining, next));
    return CommitStartedBatch(latest, next, joining);
}

Status TopologyController::CommitOrdinaryBatchStart(const TopologySnapshot &latest, const TopologyState &state,
                                                    const std::vector<MemberIdentity> &participants,
                                                    TopologyChangeType type)
{
    TopologyPlan plan;
    if (type == TopologyChangeType::SCALE_OUT) {
        RETURN_IF_NOT_OK(planBuilder_.BuildScaleOutStart(state, participants, plan));
    } else {
        CHECK_FAIL_RETURN_STATUS(type == TopologyChangeType::SCALE_IN, K_INVALID, "unsupported batch start type");
        RETURN_IF_NOT_OK(planBuilder_.BuildScaleInStart(state, participants, plan));
    }
    return CommitStartedBatch(latest, plan.next, participants);
}

Status TopologyController::CommitStartedBatch(const TopologySnapshot &latest, const TopologyState &next,
                                              const std::vector<MemberIdentity> &participants)
{
    std::shared_ptr<const TopologySnapshot> committed;
    auto rc = CommitAndReadBack(latest.Version(), next, committed);
    if (rc.IsOk()) {
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
    LOG(INFO) << "CLUSTER_CHANGE_BATCH cluster=" << keys_.ClusterName()
              << " action=" << action << " batch_type=" << TopologyChangeTypeName(activeBatch->type)
              << " batch_epoch=" << activeBatch->epoch << " previous_version=" << latest.Version()
              << " committed_version=" << committed.Version() << " participant_count=" << participants.size()
              << " sample=" << MemberIdentitySample(participants);
}

Status TopologyController::CommitAndReadBack(uint64_t expectedVersion, const TopologyState &desired,
                                             std::shared_ptr<const TopologySnapshot> &committed)
{
    TopologyCasResult result;
    RETURN_IF_NOT_OK(repository_.CompareAndSwapTopology(expectedVersion, desired, result));
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
              << " version=" << committed->Version()
              << " authority_revision=" << committed->AuthorityRevision()
              << " digest_prefix=" << TopologyDiagnosticPrefix(committed->CanonicalDigest())
              << " member_count=" << committed->Members().size()
              << " " << TopologyRingViewsForLog(*committed);
    LOG(INFO) << "CLUSTER_CHANGE cluster=" << keys_.ClusterName() << " version=" << committed->Version()
              << " expected_version=" << expectedVersion << " authority_revision=" << committed->AuthorityRevision()
              << " digest_prefix=" << TopologyDiagnosticPrefix(committed->CanonicalDigest())
              << " batch_type=" << batchType << " batch_type_name=" << batchTypeName
              << " batch_epoch=" << batchEpoch << " member_count=" << desired.members.size()
              << " decision=committed";
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
