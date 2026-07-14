/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker-role cluster topology runtime composition root.
 */
#include "datasystem/cluster/runtime/topology_engine.h"

#include <algorithm>
#include <exception>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr auto BACKEND_EVIDENCE_MAX_AGE = std::chrono::seconds(5);
constexpr auto MAX_SCOPE_PROBE_DEADLINE = std::chrono::seconds(2);
constexpr auto MAX_SCOPE_PROBE_INTERVAL = std::chrono::seconds(5);
constexpr auto MAX_STOP_GRACE = std::chrono::seconds(10);
constexpr size_t MAX_EVENT_QUEUE_CAPACITY = 1'024;
constexpr size_t DIGEST_DIAGNOSTIC_PREFIX_SIZE = 12;

bool ValidDuration(std::chrono::seconds duration)
{
    return duration.count() > 0;
}

bool IsCanonicalAddress(const std::string &address)
{
    HostPort endpoint;
    return !address.empty() && endpoint.ParseString(address).IsOk() && endpoint.ToString() == address;
}

bool IsFresh(const ControlBackendObservation &observation, std::chrono::steady_clock::time_point now)
{
    return observation.observedAt != std::chrono::steady_clock::time_point{} && observation.observedAt <= now
           && now - observation.observedAt <= BACKEND_EVIDENCE_MAX_AGE;
}

bool SameAuthorityStamp(const ControlBackendObservation &left, const ControlBackendObservation &right)
{
    return left.topologyVersion == right.topologyVersion && left.topologyRevision == right.topologyRevision
           && !left.topologyDigest.empty() && left.topologyDigest == right.topologyDigest;
}

bool IsCommitted(MemberState state)
{
    return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

bool AllowsBusinessTraffic(TopologyAvailabilityLevel level)
{
    return level == TopologyAvailabilityLevel::NORMAL || level == TopologyAvailabilityLevel::CONTROL_DEGRADED;
}

bool IsBackendAccessFailure(const Status &status)
{
    return status.GetCode() == K_RPC_UNAVAILABLE || status.GetCode() == K_RPC_DEADLINE_EXCEEDED
           || status.GetCode() == K_RPC_CANCELLED;
}

Status SelectQuorumProbeTargets(const TopologySnapshot &snapshot, const std::string &localAddress,
                                std::vector<MemberIdentity> &targets)
{
    std::vector<MemberIdentity> committed;
    bool localCommitted = false;
    for (const auto &member : snapshot.Members()) {
        if (!IsCommitted(member.state)) {
            continue;
        }
        if (member.identity.address == localAddress) {
            localCommitted = true;
        } else {
            committed.push_back(member.identity);
        }
    }
    CHECK_FAIL_RETURN_STATUS(localCommitted, K_NOT_READY, "local member is not committed for backend quorum");
    const size_t requiredPeers = (committed.size() + 1) / 2;
    CHECK_FAIL_RETURN_STATUS(committed.size() >= requiredPeers, K_NOT_READY,
                             "insufficient committed peers for backend quorum");
    std::sort(committed.begin(), committed.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    committed.resize(requiredPeers);
    targets = std::move(committed);
    return Status::OK();
}

bool ConfirmsGlobalOutage(const ControlBackendObservation &local, const std::vector<MemberIdentity> &targets,
                          const std::vector<ControlBackendObservation> &observations)
{
    if (observations.size() != targets.size()) {
        return false;
    }
    std::unordered_map<std::string, MemberIdentity> expected;
    expected.reserve(targets.size());
    for (const auto &target : targets) {
        expected.emplace(target.address, target);
    }
    std::unordered_set<std::string> accepted;
    accepted.reserve(targets.size());
    const auto now = std::chrono::steady_clock::now();
    for (const auto &observation : observations) {
        auto target = expected.find(observation.reporter.address);
        if (target == expected.end() || !(target->second == observation.reporter)
            || !accepted.insert(observation.reporter.address).second
            || observation.state != ControlBackendState::UNAVAILABLE || !SameAuthorityStamp(local, observation)
            || !IsFresh(observation, now)) {
            return false;
        }
    }
    return accepted.size() == targets.size();
}
}  // namespace

Status TopologyEngine::Create(TopologyEngineOptions options, ICoordinationBackend &backend,
                              const IRoutingAlgorithm &routingAlgorithm, ITopologyPhaseCallbacks &callbacks,
                              std::unique_ptr<TopologyEngine> &engine)
{
    CHECK_FAIL_RETURN_STATUS(
        IsCanonicalAddress(options.localAddress) && options.eventQueueCapacity > 0
            && options.eventQueueCapacity <= MAX_EVENT_QUEUE_CAPACITY && ValidDuration(options.scopeProbeDeadline)
            && options.scopeProbeDeadline <= MAX_SCOPE_PROBE_DEADLINE && ValidDuration(options.scopeProbeInterval)
            && options.scopeProbeInterval <= MAX_SCOPE_PROBE_INTERVAL && ValidDuration(options.stopGrace)
            && options.stopGrace <= MAX_STOP_GRACE,
        K_INVALID, "invalid cluster topology Engine options");
    std::unique_ptr<TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(TopologyKeyHelper::Create(options.clusterName, keys));
    try {
        auto candidate = std::unique_ptr<TopologyEngine>(
            new TopologyEngine(std::move(options), backend, routingAlgorithm, callbacks, std::move(keys)));
        engine = std::move(candidate);
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("construct cluster topology Engine failed: ") + error.what());
    }
    return Status::OK();
}

TopologyEngine::TopologyEngine(TopologyEngineOptions options, ICoordinationBackend &backend,
                               const IRoutingAlgorithm &routingAlgorithm, ITopologyPhaseCallbacks &callbacks,
                               std::unique_ptr<TopologyKeyHelper> keys)
    : options_(std::move(options)),
      backend_(backend),
      routingAlgorithm_(routingAlgorithm),
      keys_(std::move(keys)),
      repository_(backend_, *keys_),
      reader_(repository_),
      dispatcher_(options_.eventQueueCapacity),
      membershipView_(snapshots_),
      placement_(snapshots_, routingAlgorithm_),
      executor_(options_.localAddress, repository_, snapshots_, callbacks, dispatcher_, options_.executor)
{
}

TopologyEngine::~TopologyEngine()
{
    LOG_IF_ERROR(Stop(std::chrono::steady_clock::time_point::max()),
                 "Stop cluster topology Engine during destruction");
}

Status TopologyEngine::EnqueueCoordinationEvent(CoordinationEvent &&event)
{
    auto rc = dispatcher_.SubmitCoordination(std::move(event));
    if (rc.IsError() && rc.GetCode() != K_TRY_AGAIN && rc.GetCode() != K_NOT_READY) {
        RecordError(rc);
    }
    return rc;
}

Status TopologyEngine::Start()
{
    std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
    CHECK_FAIL_RETURN_STATUS(!startAttempted_, K_INVALID, "cluster topology Engine Start is one-shot");
    startAttempted_ = true;
    TopologyEngineState expected = TopologyEngineState::STOPPED;
    CHECK_FAIL_RETURN_STATUS(state_.compare_exchange_strong(expected, TopologyEngineState::STARTING), K_INVALID,
                             "cluster topology Engine already started");

    std::vector<WatchKey> watches;
    auto rc = TopologyRoleWatchPlan::Build(TopologyRuntimeRole::WORKER, options_.localAddress, *keys_, 0, watches);
    if (rc.IsError()) {
        state_.store(TopologyEngineState::STOPPED);
        return rc;
    }

    rc = dispatcher_.Start();
    if (rc.IsError()) {
        state_.store(TopologyEngineState::STOPPED);
        return rc;
    }
    backend_.SetEventHandler([this](CoordinationEvent &&event) { (void)EnqueueCoordinationEvent(std::move(event)); });

    rc = backend_.WatchEvents(watches);
    if (rc.IsError()) {
        CleanupAfterStartFailure();
        return rc;
    }
    LOG(INFO) << "CLUSTER_WATCH cluster=" << options_.clusterName << " role=worker scope_count=" << watches.size()
              << " revision=0 status=registered";

    rc = backend_.InitKeepAlive(keys_->MembershipTable(), options_.localAddress, options_.isRestart, true);
    if (rc.IsError()) {
        CleanupAfterStartFailure();
        return rc;
    }

    auto readStatus = ReloadTopology(true);
    if (readStatus.IsError()) {
        RecordError(readStatus);
        if (readStatus.GetCode() != K_NOT_FOUND) {
            CleanupAfterStartFailure();
            return readStatus;
        }
        LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                  << " role=worker state=waiting_for_topology_bootstrap";
    }

    rc = executor_.Start();
    if (rc.IsError()) {
        CleanupAfterStartFailure();
        return rc;
    }
    return StartStateThread();
}

void TopologyEngine::CleanupAfterStartFailure()
{
    dispatcher_.ShutdownIngress();
    LOG_IF_ERROR(backend_.ShutdownEventSources(), "Shut down topology Engine event sources after Start failure");
    backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
    LOG_IF_ERROR(executor_.Stop(std::chrono::steady_clock::now() + options_.executor.failureDrain),
                 "Stop topology task Executor after Engine Start failure");
    snapshots_.Clear();
    state_.store(TopologyEngineState::STOPPED);
}

Status TopologyEngine::StartStateThread()
{
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        threadExited_ = false;
    }
    try {
        stateThread_ = Thread(&TopologyEngine::Run, this);
        stateThread_.set_name("cluster-eng");
    } catch (const std::exception &error) {
        (void)executor_.Stop(std::chrono::steady_clock::now() + options_.executor.failureDrain);
        CleanupAfterStartFailure();
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start cluster topology Engine thread failed: ") + error.what());
    }
    state_.store(TopologyEngineState::RUNNING);
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName << " role=worker state=ready";
    return Status::OK();
}

Status TopologyEngine::Stop(std::chrono::steady_clock::time_point deadline)
{
    std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
    if (state_.load() == TopologyEngineState::STOPPED) {
        return Status::OK();
    }
    const auto effectiveDeadline = std::min(deadline, std::chrono::steady_clock::now() + options_.stopGrace);
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName << " role=worker state=stopping";
    state_.store(TopologyEngineState::STOPPING);
    SetAvailability(TopologyAvailabilityLevel::SHUTTING_DOWN, "shutdown");
    dispatcher_.ShutdownIngress();
    const auto eventSourceStatus = backend_.ShutdownEventSources();
    backend_.SetEventHandler(ICoordinationBackend::EventHandler{});
    const auto executorStatus = executor_.Stop(effectiveDeadline);
    Status stateThreadStatus;
    std::unique_lock<std::mutex> lock(stateMutex_);
    if (!stoppedCv_.wait_until(lock, effectiveDeadline, [this] { return threadExited_; })) {
        stateThreadStatus = Status(K_RPC_DEADLINE_EXCEEDED, "cluster topology Engine shutdown deadline exceeded");
    }
    lock.unlock();
    // Stop cannot return while stateThread_ still captures this. An external lifecycle manager enforces the process
    // termination budget when backend IO or a business callback violates cooperative cancellation.
    if (stateThread_.joinable()) {
        stateThread_.join();
    }
    snapshots_.Clear();
    {
        std::lock_guard<std::mutex> stateLock(stateMutex_);
        isolationReason_.clear();
    }
    state_.store(TopologyEngineState::STOPPED);
    availability_.store(TopologyAvailabilityLevel::NOT_READY);
    publishedAvailability_.store(TopologyAvailabilityLevel::NOT_READY);
    if (executorStatus.IsError()) {
        return executorStatus;
    }
    if (eventSourceStatus.IsError()) {
        return eventSourceStatus;
    }
    return stateThreadStatus;
}

TopologyEngineState TopologyEngine::GetState() const noexcept
{
    return state_.load();
}

TopologyAvailabilityLevel TopologyEngine::GetAvailability() const noexcept
{
    return publishedAvailability_.load();
}

const PlacementFacade &TopologyEngine::Placement() const noexcept
{
    return placement_;
}

MembershipEndpointView &TopologyEngine::Membership() noexcept
{
    return membershipView_;
}

Status TopologyEngine::GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    return snapshots_.Load(snapshot);
}

ControlBackendObservation TopologyEngine::GetControlBackendObservation() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    auto observation = backendObservation_;
    const auto now = std::chrono::steady_clock::now();
    const auto availability = availability_.load();
    const bool reportable = availability == TopologyAvailabilityLevel::NORMAL
                            || availability == TopologyAvailabilityLevel::CONTROL_DEGRADED;
    if (state_.load() != TopologyEngineState::RUNNING || !reportable || observation.reporter.id.empty()
        || observation.reporter.address.empty() || observation.topologyDigest.empty() || !IsFresh(observation, now)) {
        observation.state = ControlBackendState::UNKNOWN;
    }
    return observation;
}

TopologyDiagnostics TopologyEngine::GetDiagnostics() const
{
    TopologyDiagnostics diagnostics;
    diagnostics.state = state_.load();
    diagnostics.availability = publishedAvailability_.load();
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        diagnostics.topologyVersion = backendObservation_.topologyVersion;
        diagnostics.topologyRevision = backendObservation_.topologyRevision;
        diagnostics.topologyDigestPrefix = backendObservation_.topologyDigest.substr(0, DIGEST_DIAGNOSTIC_PREFIX_SIZE);
        diagnostics.backendHealthy = backendObservation_.state == ControlBackendState::AVAILABLE;
        diagnostics.controlBackendState = backendObservation_.state;
        diagnostics.isolationReason = isolationReason_;
        diagnostics.lastError = lastError_;
    }
    diagnostics.dispatcher = dispatcher_.GetStats();
    diagnostics.executor = executor_.GetDiagnostics();
    return diagnostics;
}

Status TopologyEngine::ReloadTopology(bool fullRebuildAllowed)
{
    std::shared_ptr<const TopologySnapshot> candidate;
    auto rc = reader_.Read(ENGINE_READ_TIMEOUT_MS, candidate);
    if (rc.IsError()) {
        if (rc.GetCode() == K_NOT_FOUND) {
            SetAvailability(TopologyAvailabilityLevel::NOT_READY, "topology_missing");
        }
        return rc;
    }
    SnapshotUpdateOutcome outcome;
    rc = snapshots_.Publish(candidate, outcome);
    if (rc.IsError() && outcome == SnapshotUpdateOutcome::VERSION_GAP && fullRebuildAllowed) {
        rc = snapshots_.PublishAfterFullRebuild(candidate);
    }
    RETURN_IF_NOT_OK(rc);
    std::shared_ptr<const TopologySnapshot> published;
    RETURN_IF_NOT_OK(snapshots_.Load(published));
    VLOG(1) << "CLUSTER_RING cluster=" << options_.clusterName << " version=" << published->Version()
            << " digest_prefix=" << published->CanonicalDigest().substr(0, DIGEST_DIAGNOSTIC_PREFIX_SIZE)
            << " status=published";
    return PublishBackendEvidence(*published);
}

Status TopologyEngine::ReloadTopologyAndNotify()
{
    RETURN_IF_NOT_OK(ReloadTopology(true));
    TopologyTaskNotify notify;
    auto rc = repository_.ReadNotify(options_.localAddress, notify);
    if (rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    return executor_.HandleNotify(notify);
}

Status TopologyEngine::PublishBackendEvidence(const TopologySnapshot &snapshot)
{
    const Member *local = nullptr;
    const auto findStatus = snapshot.FindMemberByAddress(options_.localAddress, local);
    if (findStatus.GetCode() == K_NOT_FOUND) {
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            backendObservation_ = {};
        }
        SetAvailability(TopologyAvailabilityLevel::NOT_READY, "local_member_missing");
        return Status::OK();
    }
    RETURN_IF_NOT_OK(findStatus);
    bool identityChanged = false;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        identityChanged =
            !backendObservation_.reporter.id.empty() && backendObservation_.reporter.id != local->identity.id;
        backendObservation_ = { local->identity,
                                ControlBackendState::AVAILABLE,
                                snapshot.Version(),
                                snapshot.AuthorityRevision(),
                                snapshot.CanonicalDigest(),
                                std::chrono::steady_clock::now() };
        lastError_.clear();
    }
    membershipView_.RemoveStaleObservations();
    if (identityChanged || local->state == MemberState::FAILED) {
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED,
                        identityChanged ? "local_identity_changed" : "local_member_failed");
    } else {
        SetAvailability(
            IsCommitted(local->state) ? TopologyAvailabilityLevel::NORMAL : TopologyAvailabilityLevel::NOT_READY,
            IsCommitted(local->state) ? "" : "local_member_not_committed");
    }
    return Status::OK();
}

Status TopologyEngine::HandleRuntimeEvent(RuntimeEvent event)
{
    if (auto *completion = std::get_if<TopologyCallbackCompletion>(&event.payload)) {
        return executor_.HandleCompletion(std::move(*completion));
    }
    auto rc = ReloadTopologyAndNotify();
    if (rc.IsError()) {
        if (IsBackendAccessFailure(rc)) {
            LOG_IF_ERROR(HandleBackendUnavailable(), "Classify topology backend failure after runtime event");
        }
    }
    return rc;
}

Status TopologyEngine::HandleBackendUnavailable()
{
    bool hasSnapshot = false;
    std::shared_ptr<const TopologySnapshot> snapshot;
    hasSnapshot = snapshots_.Load(snapshot).IsOk();
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        backendObservation_.state = ControlBackendState::UNAVAILABLE;
        backendObservation_.observedAt = std::chrono::steady_clock::now();
    }
    if (!hasSnapshot) {
        SetAvailability(TopologyAvailabilityLevel::NOT_READY, "backend_unavailable_without_snapshot");
        return Status::OK();
    }
    return ReevaluateFailureScope();
}

Status TopologyEngine::ReevaluateFailureScope()
{
    ControlBackendObservation local;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        local = backendObservation_;
    }
    if (!options_.controlBackendProbe) {
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "backend_scope_unknown");
        return Status::OK();
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    std::vector<MemberIdentity> targets;
    auto rc = snapshots_.Load(snapshot);
    if (rc.IsOk()) {
        rc = SelectQuorumProbeTargets(*snapshot, options_.localAddress, targets);
    }
    if (rc.IsError()) {
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "backend_quorum_unavailable");
        return Status::OK();
    }
    const auto deadline = std::chrono::steady_clock::now() + options_.scopeProbeDeadline;
    std::vector<ControlBackendObservation> observations;
    try {
        observations = options_.controlBackendProbe(local, targets, deadline);
    } catch (const std::exception &error) {
        LOG(ERROR) << "Cluster topology backend quorum probe threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Cluster topology backend quorum probe threw an unknown exception";
    }
    if (ConfirmsGlobalOutage(local, targets, observations)) {
        SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "control_backend_unavailable");
    } else {
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "backend_quorum_not_confirmed");
    }
    return Status::OK();
}

Status TopologyEngine::RefreshUnavailableBackend()
{
    auto rc = ReloadTopologyAndNotify();
    if (rc.IsOk()) {
        return Status::OK();
    }
    if (IsBackendAccessFailure(rc)) {
        LOG_IF_ERROR(HandleBackendUnavailable(), "Re-evaluate topology backend failure scope");
    }
    return rc;
}

void TopologyEngine::SetAvailability(TopologyAvailabilityLevel level, std::string reason)
{
    std::lock_guard<std::mutex> transitionLock(availabilityTransitionMutex_);
    TopologyAvailabilityLevel previous;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        previous = availability_.load();
        if (previous == level && isolationReason_ == reason) {
            return;
        }
    }
    if (!AllowsBusinessTraffic(level)) {
        NotifyAvailability(level);
    }
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        availability_.store(level);
        isolationReason_ = reason;
    }
    if (AllowsBusinessTraffic(level)) {
        NotifyAvailability(level);
    }
    publishedAvailability_.store(level);
    LOG(INFO) << "CLUSTER_DEGRADED cluster=" << options_.clusterName << " role=worker"
              << " previous_level=" << static_cast<uint32_t>(previous) << " level=" << static_cast<uint32_t>(level)
              << " reason=" << reason;
}

void TopologyEngine::NotifyAvailability(TopologyAvailabilityLevel level) noexcept
{
    if (options_.availabilityHandler == nullptr) {
        return;
    }
    try {
        options_.availabilityHandler(level);
    } catch (const std::exception &error) {
        LOG(ERROR) << "Cluster topology availability handler threw: " << error.what();
    } catch (...) {
        LOG(ERROR) << "Cluster topology availability handler threw an unknown exception";
    }
}

void TopologyEngine::RecordError(const Status &status)
{
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        lastError_ = status.ToString();
    }
    LOG(WARNING) << "Cluster topology Engine runtime operation failed: " << status.ToString();
}

void TopologyEngine::Run()
{
    while (state_.load() != TopologyEngineState::STOPPING) {
        RuntimeEvent event;
        auto rc = dispatcher_.WaitPop(std::chrono::steady_clock::now() + options_.scopeProbeInterval, event);
        const bool periodicWake = rc.GetCode() == K_RPC_DEADLINE_EXCEEDED;
        if (rc.IsOk()) {
            rc = HandleRuntimeEvent(std::move(event));
        }
        if (rc.GetCode() != K_RPC_DEADLINE_EXCEEDED && rc.GetCode() != K_NOT_READY && rc.IsError()) {
            RecordError(rc);
        }
        if (dispatcher_.ConsumeResyncRequired()) {
            LOG(WARNING) << "CLUSTER_WATCH cluster=" << options_.clusterName
                         << " role=worker scope=all status=resync";
            auto rebuild = ReloadTopology(true);
            if (rebuild.IsError()) {
                RecordError(rebuild);
            }
        }
        auto tick = executor_.HandleTick(std::chrono::steady_clock::now());
        if (tick.IsError() && tick.GetCode() != K_NOT_READY) {
            RecordError(tick);
        }
        if (periodicWake) {
            auto refresh = RefreshUnavailableBackend();
            if (refresh.IsError() && refresh.GetCode() != K_NOT_READY && refresh.GetCode() != K_TRY_AGAIN) {
                RecordError(refresh);
            }
        }
    }
    std::lock_guard<std::mutex> lock(stateMutex_);
    threadExited_ = true;
    stoppedCv_.notify_all();
}

}  // namespace datasystem::cluster
