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
#include <thread>
#include <unordered_map>
#include <utility>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_controller_runtime.h"
#include "datasystem/cluster/coordination_backend/topology_recovery_reporter.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/control_backend_scope_classifier.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr auto BACKEND_EVIDENCE_MAX_AGE = std::chrono::seconds(5);
constexpr auto BACKEND_SCOPE_POLL_INTERVAL = std::chrono::milliseconds(100);
constexpr int TOPOLOGY_WATCH_EVENT_LOG_INTERVAL = 1'024;
const std::string LOCAL_RECOVERY_RECONCILIATION_KEY = "__local_recovery_reconciliation__";

Status RegisterUnifiedTopologyTables(ICoordinationBackend &backend, const TopologyKeyHelper &keys)
{
    RETURN_IF_NOT_OK(backend.CreateTableWithExactPrefix(keys.TopologyTable(), keys.TopologyTable()));
    RETURN_IF_NOT_OK(backend.CreateTableWithExactPrefix(keys.MigrateTaskTable(), keys.MigrateTaskTable()));
    RETURN_IF_NOT_OK(backend.CreateTableWithExactPrefix(keys.DeleteTaskTable(), keys.DeleteTaskTable()));
    RETURN_IF_NOT_OK(backend.CreateTableWithExactPrefix(keys.NotifyTable(), keys.NotifyTable()));
    RETURN_IF_NOT_OK(
        backend.CreateTableWithExactPrefix(keys.ScaleInMetadataDoneTable(), keys.ScaleInMetadataDoneTable()));
    return backend.CreateTableWithExactPrefix(keys.MembershipTable(), keys.EtcdMembershipTablePrefix());
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

bool IsCommitted(MemberState state)
{
    return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

bool AllowsBusinessTraffic(TopologyAvailabilityLevel level)
{
    return level == TopologyAvailabilityLevel::NORMAL || level == TopologyAvailabilityLevel::CONTROL_DEGRADED;
}

void PreserveFirstError(const Status &candidate, Status &firstError)
{
    if (firstError.IsOk() && candidate.IsError()) {
        firstError = candidate;
    }
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

}  // namespace

struct TopologyEngine::Builder::Config {
    enum class BackendKind : uint8_t { NONE, UNIFIED, COORDINATOR };

    std::string clusterName;
    std::string localAddress;
    BackendKind backendKind{ BackendKind::NONE };
    ICoordinatorServiceProxy *coordinatorProxy{ nullptr };
    CoordinatorWatchIngress ingress;
    ITopologyPhaseCallbacks *callbacks{ nullptr };
    ControlBackendProbe controlBackendProbe;
    std::function<void(TopologyAvailabilityLevel)> availabilityHandler;
    std::function<Status(const std::string &, int64_t)> membershipRestartHandler;
    std::function<Status(const std::string &, int64_t)> membershipRecoveryHandler;
    ICoordinationBackend::LocalIsolationHandler localIsolationHandler;
    ICoordinationBackend::LocalRecoveryHandler localRecoveryHandler;
    std::function<void(std::shared_ptr<const TopologySnapshot>)> snapshotPublishedHandler;
    std::chrono::seconds nodeDeadTimeout{ TopologyControllerOptions{}.nodeDeadTimeout };
    bool buildAttempted{ false };
    bool backendSelectionInvalid{ false };
    bool isRestart{ false };
    std::unique_ptr<TopologyKeyHelper> keys;
    std::unique_ptr<ICoordinationBackend> memberBackend;
    std::unique_ptr<ICoordinationBackend> controllerBackend;
    std::unique_ptr<HashAlgorithm> algorithm;
};

TopologyEngine::Builder::Builder() : config_(std::make_unique<Config>())
{
}

TopologyEngine::Builder::~Builder() = default;

TopologyEngine::Builder &TopologyEngine::Builder::SetClusterName(std::string clusterName)
{
    if (config_ != nullptr) {
        config_->clusterName = std::move(clusterName);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetLocalAddress(std::string localAddress)
{
    if (config_ != nullptr) {
        config_->localAddress = std::move(localAddress);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::UseUnifiedCoordinationBackends(
    std::unique_ptr<ICoordinationBackend> memberBackend, std::unique_ptr<ICoordinationBackend> controllerBackend)
{
    if (config_ == nullptr) {
        return *this;
    }
    config_->backendSelectionInvalid = config_->backendKind != Config::BackendKind::NONE;
    config_->backendKind = Config::BackendKind::UNIFIED;
    config_->memberBackend = std::move(memberBackend);
    config_->controllerBackend = std::move(controllerBackend);
    config_->coordinatorProxy = nullptr;
    config_->ingress = {};
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::UseCoordinator(ICoordinatorServiceProxy &proxy,
                                                                 CoordinatorWatchIngress ingress)
{
    if (config_ == nullptr) {
        return *this;
    }
    config_->backendSelectionInvalid = config_->backendKind != Config::BackendKind::NONE;
    config_->backendKind = Config::BackendKind::COORDINATOR;
    config_->coordinatorProxy = &proxy;
    config_->ingress = std::move(ingress);
    config_->memberBackend.reset();
    config_->controllerBackend.reset();
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetPhaseCallbacks(ITopologyPhaseCallbacks &callbacks)
{
    if (config_ != nullptr) {
        config_->callbacks = &callbacks;
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetControlBackendProbe(ControlBackendProbe probe)
{
    if (config_ != nullptr) {
        config_->controlBackendProbe = std::move(probe);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetAvailabilityHandler(
    std::function<void(TopologyAvailabilityLevel)> handler)
{
    if (config_ != nullptr) {
        config_->availabilityHandler = std::move(handler);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetMembershipRestartHandler(
    std::function<Status(const std::string &, int64_t)> handler)
{
    if (config_ != nullptr) {
        config_->membershipRestartHandler = std::move(handler);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetMembershipRecoveryHandler(
    std::function<Status(const std::string &, int64_t)> handler)
{
    if (config_ != nullptr) {
        config_->membershipRecoveryHandler = std::move(handler);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetLocalIsolationHandler(
    ICoordinationBackend::LocalIsolationHandler handler)
{
    if (config_ != nullptr) {
        config_->localIsolationHandler = std::move(handler);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetLocalRecoveryHandler(
    ICoordinationBackend::LocalRecoveryHandler handler)
{
    if (config_ != nullptr) {
        config_->localRecoveryHandler = std::move(handler);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetSnapshotPublishedHandler(
    std::function<void(std::shared_ptr<const TopologySnapshot>)> handler)
{
    if (config_ != nullptr) {
        config_->snapshotPublishedHandler = std::move(handler);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetNodeDeadTimeout(std::chrono::seconds timeout)
{
    if (config_ != nullptr) {
        config_->nodeDeadTimeout = timeout;
    }
    return *this;
}

Status TopologyEngine::Builder::Validate() const
{
    CHECK_FAIL_RETURN_STATUS(config_ != nullptr && IsCanonicalAddress(config_->localAddress)
                                 && config_->callbacks != nullptr && config_->nodeDeadTimeout.count() > 0
                                 && !config_->backendSelectionInvalid,
                             K_INVALID, "invalid cluster topology Engine Builder settings");
    if (config_->backendKind == Config::BackendKind::UNIFIED) {
        CHECK_FAIL_RETURN_STATUS(config_->memberBackend != nullptr && config_->controllerBackend != nullptr, K_INVALID,
                                 "unified topology Engine requires complete coordination backends");
    } else if (config_->backendKind == Config::BackendKind::COORDINATOR) {
        CHECK_FAIL_RETURN_STATUS(config_->coordinatorProxy != nullptr && config_->ingress.bind != nullptr
                                     && config_->ingress.unbindAndDrain != nullptr,
                                 K_INVALID, "Coordinator topology Engine requires a complete ingress binding");
    } else {
        RETURN_STATUS(K_INVALID, "cluster topology Engine backend is not selected");
    }
    return Status::OK();
}

Status TopologyEngine::Builder::CreateOwnedDependencies()
{
    RETURN_IF_NOT_OK(TopologyKeyHelper::Create(config_->clusterName, config_->keys));
    if (config_->backendKind == Config::BackendKind::UNIFIED) {
        RETURN_IF_NOT_OK(RegisterUnifiedTopologyTables(*config_->memberBackend, *config_->keys));
    }
    config_->algorithm = std::make_unique<HashAlgorithm>();
    if (config_->backendKind == Config::BackendKind::COORDINATOR) {
        config_->memberBackend = CreateDsCoordinationBackend(config_->coordinatorProxy, config_->localAddress);
        config_->controllerBackend = CreateDsCoordinationBackend(config_->coordinatorProxy, config_->localAddress);
    }
    config_->memberBackend->SetLocalIsolationHandler(std::move(config_->localIsolationHandler));
    config_->memberBackend->SetLocalRecoveryHandler(std::move(config_->localRecoveryHandler));
    return Status::OK();
}

Status TopologyEngine::Builder::ReadRestartFact()
{
    TopologyRepository repository(*config_->controllerBackend, *config_->keys);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> snapshot;
    auto rc = reader.Read(TopologyEngine::ENGINE_READ_TIMEOUT_MS, snapshot);
    if (rc.GetCode() == K_NOT_FOUND) {
        config_->isRestart = false;
        return Status::OK();
    }
    if (rc.GetCode() == K_NOT_READY && config_->backendKind == Config::BackendKind::COORDINATOR) {
        config_->isRestart = false;
        LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << config_->clusterName
                  << " role=builder state=coordinator_recovering restart=unproven";
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    const Member *localMember = nullptr;
    rc = snapshot->FindMemberByAddress(config_->localAddress, localMember);
    config_->isRestart = rc.IsOk();
    return rc.GetCode() == K_NOT_FOUND ? Status::OK() : rc;
}

Status TopologyEngine::Builder::Build(std::unique_ptr<TopologyEngine> &engine)
{
    CHECK_FAIL_RETURN_STATUS(config_ != nullptr && !config_->buildAttempted, K_INVALID,
                             "cluster topology Engine Builder is one-shot");
    config_->buildAttempted = true;
    RETURN_IF_NOT_OK(Validate());
    try {
        RETURN_IF_NOT_OK(CreateOwnedDependencies());
        RETURN_IF_NOT_OK(ReadRestartFact());
        auto restartHandler = std::move(config_->membershipRestartHandler);
        auto recoveryHandler = std::move(config_->membershipRecoveryHandler);
        const auto nodeDeadTimeout = config_->nodeDeadTimeout;
        auto candidate = std::unique_ptr<TopologyEngine>(new TopologyEngine(std::move(config_)));
        RETURN_IF_NOT_OK(candidate->InitializeOwnedComponents(std::move(restartHandler), std::move(recoveryHandler),
                                                              nodeDeadTimeout));
        engine = std::move(candidate);
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("construct cluster topology Engine failed: ") + error.what());
    }
    return Status::OK();
}

TopologyEngine::RuntimeOptions TopologyEngine::ConsumeRuntimeOptions(Builder::Config &config)
{
    RuntimeOptions options;
    options.clusterName = config.clusterName;
    options.localAddress = config.localAddress;
    options.isRestart = config.isRestart;
    options.unifiedEtcdWatch = config.backendKind == Builder::Config::BackendKind::UNIFIED;
    options.controlBackendProbe = std::move(config.controlBackendProbe);
    options.availabilityHandler = std::move(config.availabilityHandler);
    options.snapshotPublishedHandler = std::move(config.snapshotPublishedHandler);
    return options;
}

TopologyEngine::TopologyEngine(std::unique_ptr<Builder::Config> config)
    : options_(ConsumeRuntimeOptions(*config)),
      memberBackend_(std::move(config->memberBackend)),
      controllerBackend_(std::move(config->controllerBackend)),
      algorithm_(std::move(config->algorithm)),
      coordinatorProxy_(config->coordinatorProxy),
      coordinatorIngress_(std::move(config->ingress)),
      keys_(std::move(config->keys)),
      repository_(*memberBackend_, *keys_),
      reader_(repository_),
      dispatcher_(options_.eventQueueCapacity),
      membershipView_(snapshots_),
      placement_(snapshots_, *algorithm_, options_.localAddress),
      executor_(options_.localAddress, repository_, snapshots_, *config->callbacks, dispatcher_, options_.executor)
{
    memberBackend_->SetCheckStoreStateWhenNetworkFailedHandler([this] { return IsControlBackendReachableFromPeers(); });
}

Status TopologyEngine::InitializeOwnedComponents(
    std::function<Status(const std::string &, int64_t)> membershipRestartHandler,
    std::function<Status(const std::string &, int64_t)> membershipRecoveryHandler, std::chrono::seconds nodeDeadTimeout)
{
    TopologyControllerRuntime::Options runtimeOptions;
    runtimeOptions.clusterName = options_.clusterName;
    runtimeOptions.controller.nodeDeadTimeout = nodeDeadTimeout;
    runtimeOptions.controller.membershipRestartHandler = std::move(membershipRestartHandler);
    runtimeOptions.controller.eventSourceMode =
        options_.unifiedEtcdWatch ? TopologyEventSourceMode::EXTERNAL : TopologyEventSourceMode::SELF_MANAGED;
    runtimeOptions.controller.membershipRecoveryHandler = std::move(membershipRecoveryHandler);
    runtimeOptions.janitor = TopologyTaskJanitorOptions{};
    RETURN_IF_NOT_OK(TopologyControllerRuntime::Create(std::move(runtimeOptions), *controllerBackend_, *algorithm_,
                                                       controllerRuntime_));
    if (coordinatorProxy_ != nullptr) {
        recoveryReporter_ = std::make_unique<TopologyRecoveryReporter>(
            *coordinatorProxy_, options_.clusterName, options_.localAddress,
            [this](uint64_t &version, std::string &canonical) { return GetRecoveryTopology(version, canonical); });
    }
    return Status::OK();
}

TopologyEngine::~TopologyEngine()
{
    auto status = Shutdown(std::chrono::steady_clock::time_point::max());
    if (status.IsError()) {
        LOG(WARNING) << "CLUSTER_LIFECYCLE role=engine state=destructor_shutdown_failed status=" << status.ToString();
        // A destructor cannot return while an owned thread still references this object. The external process
        // lifecycle manager supplies the hard termination bound when a business callback ignores cancellation.
        LOG_IF_ERROR(ShutdownComponents(std::chrono::steady_clock::time_point::max()),
                     "CLUSTER_LIFECYCLE role=engine state=destructor_final_join_failed");
    }
}

Status TopologyEngine::BindCoordinatorIngress()
{
    if (coordinatorProxy_ == nullptr) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(
        coordinatorIngress_.bind([this](const std::string &coordinatorId, int64_t watchId, CoordinationEvent &&event) {
            return RouteCoordinatorWatchEvent(coordinatorId, watchId, std::move(event));
        }));
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    ingressBound_ = true;
    return Status::OK();
}

Status TopologyEngine::UnbindCoordinatorIngress(std::chrono::steady_clock::time_point deadline)
{
    bool shouldUnbind = false;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        shouldUnbind = ingressBound_;
    }
    if (!shouldUnbind) {
        return Status::OK();
    }
    auto rc = coordinatorIngress_.unbindAndDrain(deadline);
    if (rc.IsOk()) {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        ingressBound_ = false;
    }
    return rc;
}

Status TopologyEngine::RouteCoordinatorWatchEvent(const std::string &coordinatorId, int64_t watchId,
                                                  CoordinationEvent &&event)
{
    auto *member = memberBackend_.get();
    auto *controller = controllerBackend_.get();
    bool memberOwns = member->OwnsWatchIdentity(coordinatorId, watchId);
    bool controllerOwns = controller->OwnsWatchIdentity(coordinatorId, watchId);
    if (memberOwns == controllerOwns
        && (member->IsWatchRegistrationInProgress() || controller->IsWatchRegistrationInProgress())) {
        RETURN_STATUS(K_NOT_READY, "Coordinator topology watch registration is in progress");
    }
    if (memberOwns == controllerOwns) {
        LOG_FIRST_AND_EVERY_N(WARNING, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
            << "CLUSTER_WATCH cluster=" << options_.clusterName << " watch_id=" << watchId
            << " owner_state=" << (memberOwns ? "multiple" : "missing") << " action=rewatch";
        member->InvalidateWatches();
        controller->InvalidateWatches();
        return Status::OK();
    }
    auto *owner = memberOwns ? member : controller;
    LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
        << "CLUSTER_WATCH_EVENT cluster=" << options_.clusterName
        << " role=worker ingress=coordinator owner_role=" << (memberOwns ? "member" : "controller")
        << " watch_id=" << watchId << " coordinator_id_prefix=" << TopologyDiagnosticPrefix(coordinatorId)
        << " event=" << event.ToString();
    owner->HandleWatchEvent(coordinatorId, watchId, std::move(event));
    return Status::OK();
}

Status TopologyEngine::EnqueueCoordinationEvent(CoordinationEvent &&event)
{
    LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
        << "CLUSTER_WATCH_EVENT cluster=" << options_.clusterName << " role=worker event=" << event.ToString();
    auto rc = dispatcher_.SubmitCoordination(std::move(event));
    if (rc.IsError() && rc.GetCode() != K_TRY_AGAIN && rc.GetCode() != K_NOT_READY) {
        RecordError(rc);
    }
    return rc;
}

Status TopologyEngine::RouteUnifiedEtcdWatchEvent(CoordinationEvent &&event)
{
    CHECK_FAIL_RETURN_STATUS(controllerRuntime_ != nullptr, K_NOT_READY,
                             "unified ETCD Controller runtime is not ready");
    const auto kind = keys_->ClassifyEtcdWatchKey(event.key, options_.localAddress);
    if (kind == TopologyEtcdKeyKind::TOPOLOGY) {
        CoordinationEvent controllerEvent = event;
        auto memberStatus = EnqueueCoordinationEvent(std::move(event));
        auto controllerStatus = controllerRuntime_->SubmitCoordinationEvent(std::move(controllerEvent));
        return memberStatus.IsError() ? memberStatus : controllerStatus;
    }
    if (kind == TopologyEtcdKeyKind::LOCAL_NOTIFY) {
        return EnqueueCoordinationEvent(std::move(event));
    }
    if (kind == TopologyEtcdKeyKind::MEMBERSHIP || kind == TopologyEtcdKeyKind::MIGRATE_TASK
        || kind == TopologyEtcdKeyKind::DELETE_TASK) {
        return controllerRuntime_->SubmitCoordinationEvent(std::move(event));
    }
    RETURN_STATUS(K_INVALID, "unified ETCD watch received an unregistered physical key");
}

Status TopologyEngine::RequestRecoveryReconciliation(const std::function<void()> &suspendServing)
{
    std::lock_guard<std::mutex> transitionLock(availabilityTransitionMutex_);
    if (suspendServing != nullptr) {
        suspendServing();
    }
    const auto generation = recoveryGenerationRequested_.fetch_add(1) + 1;
    auto rc = EnqueueCoordinationEvent(CoordinationEvent{ CoordinationEventType::PUT, LOCAL_RECOVERY_RECONCILIATION_KEY,
                                                          "", 0, static_cast<int64_t>(generation) });
    return rc;
}

Status TopologyEngine::Start()
{
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        CHECK_FAIL_RETURN_STATUS(!startAttempted_ && !lifecycleOperationInFlight_, K_INVALID,
                                 "cluster topology Engine Start is one-shot");
        startAttempted_ = true;
        lifecycleOperationInFlight_ = true;
        state_.store(TopologyEngineState::STARTING);
    }
    if (coordinatorProxy_ != nullptr) {
        memberBackend_->SetMembershipReadyHandler([this](const std::string &coordinatorId, bool watchesInvalidated) {
            if (recoveryReporter_ != nullptr) {
                recoveryReporter_->NotifyMembershipReady(coordinatorId);
            }
            if (watchesInvalidated) {
                controllerBackend_->InvalidateWatches();
            }
        });
    }
    auto rc = BindCoordinatorIngress();
    if (rc.IsOk()) {
        rc = StartMemberRole();
    }
    if (rc.IsOk() && !options_.unifiedEtcdWatch) {
        rc = controllerRuntime_->Start();
    }
    if (rc.IsError()) {
        const auto cleanupStatus = CleanupAfterStartFailure();
        if (cleanupStatus.IsError()) {
            LOG(WARNING) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                         << " role=engine state=start_rollback_incomplete action=retry_shutdown status="
                         << cleanupStatus.ToString();
        }
        return rc;
    }
    if (recoveryReporter_ != nullptr) {
        recoveryReporter_->NotifyRuntimeReady();
    }
    CommitSuccessfulStart();
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName << " role=worker state=ready";
    return Status::OK();
}

Status TopologyEngine::StartMemberRole()
{
    std::vector<WatchKey> watches;
    const auto role = options_.unifiedEtcdWatch ? TopologyRuntimeRole::UNIFIED_ETCD : TopologyRuntimeRole::WORKER;
    RETURN_IF_NOT_OK(TopologyRoleWatchPlan::Build(role, options_.localAddress, *keys_, 0, watches));
    RETURN_IF_NOT_OK(dispatcher_.Start());
    if (options_.unifiedEtcdWatch) {
        memberBackend_->SetEventHandler([this](CoordinationEvent &&event) {
            auto rc = RouteUnifiedEtcdWatchEvent(std::move(event));
            if (rc.IsError() && rc.GetCode() != K_TRY_AGAIN && rc.GetCode() != K_NOT_READY) {
                LOG(ERROR) << "Route unified ETCD topology watch event failed: " << rc.ToString();
            }
        });
    } else {
        memberBackend_->SetEventHandler(
            [this](CoordinationEvent &&event) { (void)EnqueueCoordinationEvent(std::move(event)); });
    }
    // A Coordinator rejects topology watches without a live owning membership, which also fences delayed watches
    // after lease deletion. Publish the lease first, then use watch initial snapshots and ReloadTopology to catch up.
    RETURN_IF_NOT_OK(
        memberBackend_->InitKeepAlive(keys_->MembershipTable(), options_.localAddress, options_.isRestart, true));
    if (options_.unifiedEtcdWatch) {
        RETURN_IF_NOT_OK(controllerRuntime_->Start());
    }
    auto rc = memberBackend_->WatchEvents(watches);
    if (rc.IsError()) {
        LOG_IF_ERROR(memberBackend_->Delete(keys_->MembershipTable(), options_.localAddress),
                     "CLUSTER_MEMBERSHIP_STARTUP_CLEANUP_FAILED");
        return rc;
    }
    LOG(INFO) << "CLUSTER_WATCH cluster=" << options_.clusterName
              << " role=" << (options_.unifiedEtcdWatch ? "unified_etcd" : "worker")
              << " scope_count=" << watches.size() << " revision=0 status=registered";

    auto readStatus = ReloadTopology(true);
    if (readStatus.IsError()) {
        RecordError(readStatus);
        if (readStatus.GetCode() != K_NOT_FOUND && readStatus.GetCode() != K_NOT_READY) {
            return readStatus;
        }
        LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                  << " role=worker state=waiting_for_topology_bootstrap";
    }

    rc = executor_.Start();
    return rc.IsOk() ? StartStateThread() : rc;
}

Status TopologyEngine::CleanupAfterStartFailure()
{
    state_.store(TopologyEngineState::STOPPING);
    SetAvailability(TopologyAvailabilityLevel::SHUTTING_DOWN, "start_rollback");
    dispatcher_.ShutdownIngress();
    const auto cleanupDeadline = std::chrono::steady_clock::now() + options_.stopGrace;
    const auto cleanupStatus = ShutdownComponents(cleanupDeadline);
    if (cleanupStatus.IsOk()) {
        snapshots_.Clear();
        SetAvailability(TopologyAvailabilityLevel::NOT_READY, "start_failed");
    }
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        if (cleanupStatus.IsOk()) {
            state_.store(TopologyEngineState::STOPPED);
        }
        lifecycleOperationInFlight_ = false;
    }
    return cleanupStatus;
}

void TopologyEngine::CommitSuccessfulStart()
{
    std::lock_guard<std::mutex> transitionLock(availabilityTransitionMutex_);
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        state_.store(TopologyEngineState::RUNNING);
    }
    const auto level = availability_.load();
    if (AllowsBusinessTraffic(level)) {
        NotifyAvailability(level);
        publishedAvailability_.store(level);
    }
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        lifecycleOperationInFlight_ = false;
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    if (snapshots_.Load(snapshot).IsOk()) {
        NotifySnapshotPublishedIfRunning(std::move(snapshot));
    }
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
        std::lock_guard<std::mutex> lock(stateMutex_);
        threadExited_ = true;
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start cluster topology Engine thread failed: ") + error.what());
    }
    return Status::OK();
}

Status TopologyEngine::ShutdownComponents(std::chrono::steady_clock::time_point deadline)
{
    Status firstError;
    PreserveFirstError(UnbindCoordinatorIngress(deadline), firstError);
    PreserveFirstError(memberBackend_->ShutdownEventSources(), firstError);
    if (recoveryReporter_ != nullptr) {
        PreserveFirstError(recoveryReporter_->Shutdown(), firstError);
    }
    dispatcher_.ShutdownIngress();
    PreserveFirstError(executor_.Stop(deadline), firstError);
    bool stateThreadExited = false;
    std::unique_lock<std::mutex> lock(stateMutex_);
    stateThreadExited = stoppedCv_.wait_until(lock, deadline, [this] { return threadExited_; });
    lock.unlock();
    if (!stateThreadExited) {
        PreserveFirstError(Status(K_RPC_DEADLINE_EXCEEDED, "cluster topology Engine shutdown deadline exceeded"),
                           firstError);
    } else if (stateThread_.joinable()) {
        stateThread_.join();
    }
    PreserveFirstError(controllerRuntime_->Stop(deadline), firstError);
    if (firstError.IsOk()) {
        PreserveFirstError(memberBackend_->Shutdown(), firstError);
        if (!options_.unifiedEtcdWatch) {
            PreserveFirstError(controllerBackend_->Shutdown(), firstError);
        }
    }
    return firstError;
}

Status TopologyEngine::Shutdown(std::chrono::steady_clock::time_point deadline)
{
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        if (state_.load() == TopologyEngineState::STOPPED) {
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS(!lifecycleOperationInFlight_, K_TRY_AGAIN,
                                 "cluster topology Engine lifecycle operation is in progress");
        lifecycleOperationInFlight_ = true;
    }
    const auto effectiveDeadline = deadline == std::chrono::steady_clock::time_point::max()
                                       ? deadline
                                       : std::min(deadline, std::chrono::steady_clock::now() + options_.stopGrace);
    state_.store(TopologyEngineState::STOPPING);
    SetAvailability(TopologyAvailabilityLevel::SHUTTING_DOWN, "shutdown");
    auto rc = ShutdownComponents(effectiveDeadline);
    if (rc.IsError()) {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        lifecycleOperationInFlight_ = false;
        return rc;
    }
    snapshots_.Clear();
    {
        std::lock_guard<std::mutex> stateLock(stateMutex_);
        isolationReason_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        state_.store(TopologyEngineState::STOPPED);
        availability_.store(TopologyAvailabilityLevel::NOT_READY);
        publishedAvailability_.store(TopologyAvailabilityLevel::NOT_READY);
        lifecycleOperationInFlight_ = false;
    }
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName << " role=engine state=stopped";
    return Status::OK();
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

const MembershipEndpointView &TopologyEngine::Membership() const noexcept
{
    return membershipView_;
}

Status TopologyEngine::MarkReady()
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    CHECK_FAIL_RETURN_STATUS(HasEstablishedMemberLease(), K_NOT_READY,
                             "cluster topology member lease is not established");
    auto rc = memberBackend_->UpdateNodeState(MemberLifecycleState::READY);
    LOG(INFO) << "CLUSTER_MEMBERSHIP cluster=" << options_.clusterName
              << " role=worker action=mark_ready address=" << options_.localAddress << " status=" << rc.ToString();
    return rc;
}

Status TopologyEngine::MarkRecovering()
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    CHECK_FAIL_RETURN_STATUS(HasEstablishedMemberLease(), K_NOT_READY,
                             "cluster topology member lease is not established");
    auto rc = memberBackend_->UpdateNodeState(MemberLifecycleState::RECOVERING);
    LOG(INFO) << "CLUSTER_MEMBERSHIP cluster=" << options_.clusterName
              << " role=worker action=mark_recovering address=" << options_.localAddress << " status=" << rc.ToString();
    return rc;
}

Status TopologyEngine::MarkExiting()
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    auto rc = memberBackend_->UpdateNodeState(MemberLifecycleState::EXITING);
    LOG(INFO) << "CLUSTER_MEMBERSHIP cluster=" << options_.clusterName
              << " role=worker action=mark_exiting address=" << options_.localAddress << " status=" << rc.ToString();
    return rc;
}

Status TopologyEngine::NotifyReconciliationDone()
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    HostPort localAddress;
    RETURN_IF_NOT_OK(localAddress.ParseString(options_.localAddress));
    auto rc = memberBackend_->InformReconciliationDone(localAddress);
    LOG(INFO) << "CLUSTER_MEMBERSHIP cluster=" << options_.clusterName
              << " role=worker action=reconciliation_done address=" << options_.localAddress
              << " status=" << rc.ToString();
    return rc;
}

bool TopologyEngine::IsRestart() const noexcept
{
    return options_.isRestart;
}

bool TopologyEngine::HasEstablishedMemberLease() const noexcept
{
    return memberBackend_->IsFirstKeepAliveSent();
}

bool TopologyEngine::IsMemberLeaseTimedOut() const noexcept
{
    return memberBackend_->IsKeepAliveTimeout();
}

Status TopologyEngine::GetRoutingHostIds(std::unordered_map<std::string, std::string> &hostIds) const
{
    std::vector<std::pair<std::string, std::string>> members;
    RETURN_IF_NOT_OK(memberBackend_->GetAll(keys_->MembershipTable(), members));
    std::unordered_map<std::string, std::string> candidate;
    candidate.reserve(members.size());
    for (const auto &entry : members) {
        MembershipValue membership;
        auto rc = MembershipValueCodec::Decode(entry.second, membership);
        if (rc.IsError()) {
            LOG(WARNING) << "CLUSTER_MEMBERSHIP skip invalid host-id record, address=" << entry.first
                         << ", status=" << rc.ToString();
            continue;
        }
        candidate.emplace(entry.first, std::move(membership.hostId));
    }
    hostIds = std::move(candidate);
    return Status::OK();
}

Status TopologyEngine::GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    return snapshots_.Load(snapshot);
}

Status TopologyEngine::GetRecoveryTopology(uint64_t &topologyVersion, std::string &canonicalTopology) const
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    auto rc = snapshots_.Load(snapshot);
    if (rc.GetCode() == K_NOT_READY) {
        RETURN_STATUS(K_NOT_FOUND, "cluster topology recovery Snapshot is not ready");
    }
    RETURN_IF_NOT_OK(rc);
    TopologyState state{ snapshot->ClusterHasInit(), snapshot->Version(), snapshot->Members(),
                         snapshot->GetActiveBatch() };
    std::string encoded;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(state, encoded));
    topologyVersion = snapshot->Version();
    canonicalTopology = std::move(encoded);
    return Status::OK();
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

ControlBackendObservation TopologyEngine::GetLocalControlBackendObservation() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    auto observation = backendObservation_;
    const auto now = std::chrono::steady_clock::now();
    if (state_.load() != TopologyEngineState::RUNNING || observation.reporter.id.empty()
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
        diagnostics.topologyDigestPrefix = TopologyDiagnosticPrefix(backendObservation_.topologyDigest);
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
        std::shared_ptr<const TopologySnapshot> lastGood;
        const bool hasLastGood = snapshots_.Load(lastGood).IsOk();
        if (rc.GetCode() == K_NOT_FOUND || (rc.GetCode() == K_NOT_READY && !hasLastGood)) {
            SetAvailability(TopologyAvailabilityLevel::NOT_READY,
                            rc.GetCode() == K_NOT_FOUND ? "topology_missing" : "topology_recovering");
        }
        return rc;
    }
    SnapshotUpdateOutcome outcome;
    rc = snapshots_.Publish(candidate, outcome);
    bool newlyPublished = rc.IsOk() && outcome == SnapshotUpdateOutcome::PUBLISHED;
    if (rc.IsError() && outcome == SnapshotUpdateOutcome::VERSION_GAP && fullRebuildAllowed) {
        rc = snapshots_.PublishAfterFullRebuild(candidate);
        newlyPublished = rc.IsOk();
    }
    if (rc.IsError()
        && (outcome == SnapshotUpdateOutcome::VERSION_ROLLBACK || outcome == SnapshotUpdateOutcome::CONFLICT)) {
        authorityIsolated_.store(true);
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "authority_version_or_digest_conflict");
    }
    RETURN_IF_NOT_OK(rc);
    std::shared_ptr<const TopologySnapshot> published;
    RETURN_IF_NOT_OK(snapshots_.Load(published));
    VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL) << "CLUSTER_RING cluster=" << options_.clusterName
                                     << " version=" << published->Version()
                                     << " digest_prefix=" << TopologyDiagnosticPrefix(published->CanonicalDigest())
                                     << " status=published";
    RETURN_IF_NOT_OK(PublishBackendEvidence(*published));
    if (newlyPublished) {
        LogAndNotifyPublishedSnapshot(std::move(published));
    }
    return Status::OK();
}

void TopologyEngine::LogAndNotifyPublishedSnapshot(std::shared_ptr<const TopologySnapshot> published)
{
    const auto activeBatch = published->GetActiveBatch();
    const auto batchType = activeBatch.has_value() ? TopologyChangeTypeName(activeBatch->type) : "none";
    const auto batchEpoch = activeBatch.has_value() ? activeBatch->epoch : TOPOLOGY_NO_ACTIVE_BATCH_EPOCH;
    const Member *local = nullptr;
    const auto localStatus = published->FindMemberByAddress(options_.localAddress, local);
    LOG(INFO) << "CLUSTER_RING cluster=" << options_.clusterName << " role=worker status=published"
              << " version=" << published->Version() << " authority_revision=" << published->AuthorityRevision()
              << " digest_prefix=" << TopologyDiagnosticPrefix(published->CanonicalDigest())
              << " batch_type=" << batchType << " batch_epoch=" << batchEpoch
              << " member_count=" << published->Members().size()
              << " active_count=" << published->ActiveMembers().size()
              << " failed_count=" << published->FailedMembers().size() << " local_member_found=" << localStatus.IsOk()
              << " local_state=" << (localStatus.IsOk() ? MemberStateName(local->state) : "missing")
              << " local_member_id_prefix=" << (localStatus.IsOk() ? MemberIdForLog(local->identity.id) : "");
    NotifySnapshotPublishedIfRunning(std::move(published));
}

Status TopologyEngine::ReloadTopologyAndNotify()
{
    return ReloadTopologyForRuntime(true);
}

Status TopologyEngine::ReloadTopologyForRuntime(bool readNotify)
{
    const auto generation = recoveryGenerationRequested_.load();
    const auto previous = GetAvailability();
    activeReloadGeneration_ = generation;
    runtimeReloadInProgress_ = true;
    auto rc = ReloadTopology(true);
    if (rc.IsOk() && readNotify) {
        TopologyTaskNotify notify;
        rc = repository_.ReadNotify(options_.localAddress, notify);
        if (rc.GetCode() == K_NOT_FOUND) {
            rc = Status::OK();
        } else if (rc.IsOk()) {
            rc = executor_.HandleNotify(notify);
        }
    }
    runtimeReloadInProgress_ = false;
    if (rc.IsOk()) {
        CompleteRecoveryReconciliation(generation, previous);
    }
    return rc;
}

Status TopologyEngine::PublishBackendEvidence(const TopologySnapshot &snapshot)
{
    const Member *local = nullptr;
    const auto findStatus = snapshot.FindMemberByAddress(options_.localAddress, local);
    if (findStatus.GetCode() == K_NOT_FOUND) {
        bool hadLocalMember = false;
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            hadLocalMember = !backendObservation_.reporter.id.empty();
            backendObservation_ = {};
        }
        const auto availability =
            hadLocalMember ? TopologyAvailabilityLevel::ROLE_ISOLATED : TopologyAvailabilityLevel::NOT_READY;
        SetAvailability(availability, hadLocalMember ? "local_member_removed" : "local_member_missing");
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
            LOG_IF_ERROR(HandleBackendUnavailable(), "CLUSTER_BACKEND_FAILURE_CLASSIFICATION_FAILED");
        }
    }
    return rc;
}

void TopologyEngine::CompleteRecoveryReconciliation(uint64_t generation, TopologyAvailabilityLevel previous)
{
    if (generation == 0 || generation <= recoveryGenerationCompleted_.load()) {
        return;
    }
    std::lock_guard<std::mutex> transitionLock(availabilityTransitionMutex_);
    if (recoveryGenerationRequested_.load() != generation) {
        recoveryGenerationCompleted_ = generation;
        return;
    }
    const auto current = publishedAvailability_.load();
    if (current != previous) {
        recoveryGenerationCompleted_ = generation;
        return;
    }
    if (!AllowsBusinessTraffic(current)) {
        return;
    }
    recoveryGenerationCompleted_ = generation;
    NotifyAvailability(current);
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
    // An injected control-plane probe must not unwind through the Engine state thread.
    try {
        observations = options_.controlBackendProbe(local, targets, deadline);
    } catch (const std::exception &error) {
        LOG(ERROR) << "CLUSTER_BACKEND_PROBE_FAILED reason=exception error=" << error.what();
    } catch (...) {
        LOG(ERROR) << "CLUSTER_BACKEND_PROBE_FAILED reason=unknown_exception";
    }
    if (ConfirmsGlobalBackendOutage(local, targets, observations)) {
        SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "control_backend_unavailable");
    } else {
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "backend_quorum_not_confirmed");
    }
    return Status::OK();
}

bool TopologyEngine::IsControlBackendReachableFromPeers()
{
    ControlBackendObservation local;
    if (!CopyUnavailableLocalBackendObservation(local) || !options_.controlBackendProbe) {
        return false;
    }
    std::vector<MemberIdentity> targets;
    if (!LoadControlBackendProbeTargets(targets)) {
        return false;
    }
    const auto deadline = std::chrono::steady_clock::now() + options_.scopeProbeDeadline;
    size_t observationCount = 0;
    do {
        bool globalOutage = false;
        bool probeFailed = false;
        if (ProbePeerBackendReachabilityOnce(local, targets, deadline, observationCount, globalOutage, probeFailed)) {
            METRIC_INC(metrics::KvMetricId::WORKER_CONTROL_BACKEND_SCOPE_LOCAL_TOTAL);
            LOG(INFO) << "Keepalive backend failure scope is local, probeTargets: " << targets.size()
                      << ", observations: " << observationCount;
            INJECT_POINT_NO_RETURN("WorkerOCServer.LocalBackendIsolationCandidate");
            return true;
        }
        if (probeFailed) {
            METRIC_INC(metrics::KvMetricId::WORKER_CONTROL_BACKEND_SCOPE_INCONCLUSIVE_TOTAL);
            return false;
        }
        if (globalOutage) {
            METRIC_INC(metrics::KvMetricId::WORKER_CONTROL_BACKEND_SCOPE_GLOBAL_TOTAL);
            LOG(INFO) << "Keepalive backend failure scope is global, probeTargets: " << targets.size()
                      << ", observations: " << observationCount;
            SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "control_backend_unavailable");
            INJECT_POINT_NO_RETURN("WorkerOCServer.GlobalBackendOutage");
            return false;
        }
        const auto nextProbe = std::min(deadline, std::chrono::steady_clock::now() + BACKEND_SCOPE_POLL_INTERVAL);
        std::this_thread::sleep_until(nextProbe);
    } while (std::chrono::steady_clock::now() < deadline);
    LOG(INFO) << "Keepalive backend failure scope is inconclusive, probeTargets: " << targets.size()
              << ", observations: " << observationCount;
    METRIC_INC(metrics::KvMetricId::WORKER_CONTROL_BACKEND_SCOPE_INCONCLUSIVE_TOTAL);
    return false;
}

bool TopologyEngine::CopyUnavailableLocalBackendObservation(ControlBackendObservation &local)
{
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        local = backendObservation_;
        local.state = ControlBackendState::UNAVAILABLE;
        local.observedAt = std::chrono::steady_clock::now();
    }
    if (local.reporter.address.empty() || local.topologyDigest.empty()) {
        LOG(WARNING) << "Skip keepalive local-isolation decision without local backend evidence";
        return false;
    }
    return true;
}

bool TopologyEngine::LoadControlBackendProbeTargets(std::vector<MemberIdentity> &targets)
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    auto rc = snapshots_.Load(snapshot);
    if (rc.IsOk()) {
        rc = SelectQuorumProbeTargets(*snapshot, options_.localAddress, targets);
    }
    if (rc.IsError()) {
        LOG(WARNING) << "Skip keepalive local-isolation decision because backend quorum cannot be selected: "
                     << rc.ToString();
        return false;
    }
    return true;
}

bool TopologyEngine::ProbePeerBackendReachabilityOnce(const ControlBackendObservation &local,
                                                      const std::vector<MemberIdentity> &targets,
                                                      std::chrono::steady_clock::time_point deadline,
                                                      size_t &observationCount, bool &globalOutage,
                                                      bool &probeFailed) const
{
    std::vector<ControlBackendObservation> observations;
    try {
        observations = options_.controlBackendProbe(local, targets, deadline);
    } catch (const std::exception &error) {
        LOG(ERROR) << "CLUSTER_BACKEND_PROBE_FAILED reason=exception error=" << error.what();
        probeFailed = true;
        return false;
    } catch (...) {
        LOG(ERROR) << "CLUSTER_BACKEND_PROBE_FAILED reason=unknown_exception";
        probeFailed = true;
        return false;
    }
    observationCount = observations.size();
    const auto scope = ClassifyControlBackendFailureScope(local, targets, observations);
    globalOutage = scope == ControlBackendFailureScope::GLOBAL_OUTAGE;
    return scope == ControlBackendFailureScope::LOCAL_ISOLATION;
}

Status TopologyEngine::RefreshUnavailableBackend()
{
    auto rc = ReloadTopologyAndNotify();
    if (rc.IsOk()) {
        return Status::OK();
    }
    if (IsBackendAccessFailure(rc)) {
        LOG_IF_ERROR(HandleBackendUnavailable(), "CLUSTER_BACKEND_FAILURE_REEVALUATION_FAILED");
    }
    return rc;
}

void TopologyEngine::SetAvailability(TopologyAvailabilityLevel level, std::string reason)
{
    if (authorityIsolated_.load() && level != TopologyAvailabilityLevel::ROLE_ISOLATED
        && level != TopologyAvailabilityLevel::SHUTTING_DOWN) {
        level = TopologyAvailabilityLevel::ROLE_ISOLATED;
        reason = "authority_version_or_digest_conflict";
    }
    std::lock_guard<std::mutex> transitionLock(availabilityTransitionMutex_);
    const bool publish = state_.load() == TopologyEngineState::RUNNING || !AllowsBusinessTraffic(level);
    if (AllowsBusinessTraffic(level) && runtimeReloadInProgress_.load()
        && recoveryGenerationRequested_.load() > activeReloadGeneration_.load()) {
        LOG(INFO) << "Suppress serving availability from an exact read superseded by a newer recovery generation.";
        return;
    }
    TopologyAvailabilityLevel previous;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        previous = availability_.load();
        if (previous == level && isolationReason_ == reason && (!publish || publishedAvailability_.load() == level)) {
            return;
        }
    }
    if (publish && !AllowsBusinessTraffic(level)) {
        NotifyAvailability(level);
    }
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        availability_.store(level);
        isolationReason_ = reason;
    }
    if (publish && AllowsBusinessTraffic(level)) {
        publishedAvailability_.store(level);
        NotifyAvailability(level);
    }
    if (publish && !AllowsBusinessTraffic(level)) {
        publishedAvailability_.store(level);
    }
    LOG(INFO) << "CLUSTER_DEGRADED cluster=" << options_.clusterName << " role=worker"
              << " previous_level=" << static_cast<uint32_t>(previous) << " level=" << static_cast<uint32_t>(level)
              << " reason=" << reason << " published=" << publish;
}

void TopologyEngine::NotifyAvailability(TopologyAvailabilityLevel level) const noexcept
{
    if (options_.availabilityHandler == nullptr) {
        return;
    }
    // An injected availability callback must not unwind through the Engine state thread.
    try {
        options_.availabilityHandler(level);
    } catch (const std::exception &error) {
        LOG(ERROR) << "CLUSTER_AVAILABILITY_HANDLER_FAILED reason=exception error=" << error.what();
    } catch (...) {
        LOG(ERROR) << "CLUSTER_AVAILABILITY_HANDLER_FAILED reason=unknown_exception";
    }
}

void TopologyEngine::NotifySnapshotPublished(std::shared_ptr<const TopologySnapshot> snapshot) const
{
    if (options_.snapshotPublishedHandler == nullptr) {
        return;
    }
    options_.snapshotPublishedHandler(std::move(snapshot));
}

void TopologyEngine::NotifySnapshotPublishedIfRunning(std::shared_ptr<const TopologySnapshot> snapshot)
{
    if (snapshot == nullptr || state_.load() != TopologyEngineState::RUNNING) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        if (lifecycleOperationInFlight_) {
            return;
        }
    }
    auto previous = lastNotifiedSnapshotVersion_.load(std::memory_order_acquire);
    while (snapshot->Version() > previous) {
        if (lastNotifiedSnapshotVersion_.compare_exchange_weak(previous, snapshot->Version(), std::memory_order_acq_rel,
                                                               std::memory_order_acquire)) {
            NotifySnapshotPublished(std::move(snapshot));
            return;
        }
    }
}

void TopologyEngine::RecordError(const Status &status)
{
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        lastError_ = status.ToString();
    }
    LOG(WARNING) << "CLUSTER_RUNTIME_OPERATION_FAILED status=" << status.ToString();
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
                         << " role=worker scope=all status=resync queued_events=" << dispatcher_.GetStats().queueDepth;
            auto rebuild = ReloadTopologyForRuntime(false);
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
