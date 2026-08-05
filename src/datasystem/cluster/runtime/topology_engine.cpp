/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker-role cluster topology runtime composition root.
 */
#include "datasystem/cluster/runtime/topology_engine.h"

#include <algorithm>
#include <csignal>
#include <exception>
#include <iterator>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_controller_runtime.h"
#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/coordination_backend/etcd_coordination_backend.h"
#include "datasystem/cluster/coordination_backend/topology_recovery_reporter.h"
#include "datasystem/cluster/coordination_backend/worker_leader_reconciler.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem::cluster {
namespace {
constexpr auto BACKEND_EVIDENCE_MAX_AGE = std::chrono::seconds(10);
constexpr uint32_t LOCAL_ISOLATION_CONFIRMATIONS = 3;
constexpr int TOPOLOGY_WATCH_EVENT_LOG_INTERVAL = 1'024;
constexpr int CONTROL_DEGRADED_ERROR_LOG_INTERVAL = 60;

Status RegisterEtcdTopologyTables(EtcdStore &store, const TopologyKeyHelper &keys)
{
    RETURN_IF_NOT_OK(store.CreateTableWithExactPrefix(keys.TopologyTable(), keys.TopologyTable()));
    RETURN_IF_NOT_OK(store.CreateTableWithExactPrefix(keys.MigrateTaskTable(), keys.MigrateTaskTable()));
    RETURN_IF_NOT_OK(store.CreateTableWithExactPrefix(keys.DeleteTaskTable(), keys.DeleteTaskTable()));
    RETURN_IF_NOT_OK(store.CreateTableWithExactPrefix(keys.NotifyTable(), keys.NotifyTable()));
    RETURN_IF_NOT_OK(
        store.CreateTableWithExactPrefix(keys.ScaleInMetadataDoneTable(), keys.ScaleInMetadataDoneTable()));
    return store.CreateTableWithExactPrefix(keys.MembershipTable(), keys.EtcdMembershipTablePrefix());
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
    return IsRetryableRpcError(status) || IsNonRetryableRpcError(status);
}

Status SelectProbeTargets(const TopologySnapshot &snapshot, const std::string &localAddress,
                          std::vector<MemberIdentity> &targets)
{
    std::vector<MemberIdentity> committed;
    bool localCommitted = false;
    for (const auto &member : snapshot.Members()) {
        if (!IsCommittedMemberState(member.state)) {
            continue;
        }
        if (member.identity.address == localAddress) {
            localCommitted = true;
        } else {
            committed.push_back(member.identity);
        }
    }
    CHECK_FAIL_RETURN_STATUS(localCommitted, K_NOT_READY, "local member is not committed for backend quorum");
    std::sort(committed.begin(), committed.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    targets = std::move(committed);
    return Status::OK();
}

enum class BackendFailureScope : uint8_t { GLOBAL_OUTAGE, LOCAL_BACKEND_PATH, INCONCLUSIVE };

BackendFailureScope ClassifyBackendFailure(const ControlBackendObservation &local,
                                           const std::vector<MemberIdentity> &targets,
                                           const std::vector<ControlBackendProbeResult> &results)
{
    const size_t quorum = (targets.size() + 1) / 2;
    if (quorum == 0) {
        return BackendFailureScope::INCONCLUSIVE;
    }
    std::unordered_map<std::string, MemberIdentity> expected;
    expected.reserve(targets.size());
    for (const auto &target : targets) {
        expected.emplace(target.address, target);
    }
    std::unordered_set<std::string> accepted;
    accepted.reserve(targets.size());
    const auto now = std::chrono::steady_clock::now();
    size_t available = 0;
    size_t unavailable = 0;
    for (const auto &result : results) {
        auto target = expected.find(result.target.address);
        if (target == expected.end() || !(target->second == result.target)
            || !accepted.insert(result.target.address).second) {
            continue;
        }
        if (result.outcome == ControlBackendProbeOutcome::RESPONSE && result.observation.has_value()) {
            const auto &observation = *result.observation;
            if (!(observation.reporter == result.target) || !SameAuthorityStamp(local, observation)
                || !IsFresh(observation, now)) {
                continue;
            }
            available += observation.state == ControlBackendState::AVAILABLE;
            unavailable += observation.state == ControlBackendState::UNAVAILABLE;
        }
    }
    if (available > 0) {
        return BackendFailureScope::LOCAL_BACKEND_PATH;
    }
    if (unavailable >= quorum) {
        return BackendFailureScope::GLOBAL_OUTAGE;
    }
    return BackendFailureScope::INCONCLUSIVE;
}
}  // namespace

struct TopologyEngine::Builder::Config {
    enum class BackendKind : uint8_t { NONE, ETCD, COORDINATOR };

    std::string clusterName;
    std::string localAddress;
    BackendKind backendKind{ BackendKind::NONE };
    EtcdStore *memberStore{ nullptr };
    ICoordinatorServiceProxy *coordinatorProxy{ nullptr };
    CoordinatorWatchIngress ingress;
    ITopologyPhaseCallbacks *callbacks{ nullptr };
    ControlBackendProbe controlBackendProbe;
    PeerTopologyRefresh peerTopologyRefresh;
    std::function<Status(WorkerProbeRequest)> workerProbeHandler;
    std::function<void(TopologyAvailabilityLevel)> availabilityHandler;
    std::function<Status()> membershipRecreateGate;
    std::function<Status(const std::map<std::string, int64_t> &, RestartEffectMode)> membershipRestartHandler;
    std::function<void(std::shared_ptr<const TopologySnapshot>)> snapshotPublishedHandler;
    std::chrono::seconds nodeDeadTimeout{ TopologyControllerOptions{}.nodeDeadTimeout };
    std::chrono::seconds localIsolationTimeout{ TopologyControllerOptions{}.nodeDeadTimeout };
    std::chrono::milliseconds scopeProbeInterval{ 5'000 };
    std::chrono::milliseconds scaleInCollectWindow{ TopologyControllerOptions{}.scaleInCollectWindow };
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

TopologyEngine::Builder &TopologyEngine::Builder::UseEtcd(EtcdStore &store)
{
    if (config_ == nullptr) {
        return *this;
    }
    config_->backendSelectionInvalid = config_->backendKind != Config::BackendKind::NONE;
    config_->backendKind = Config::BackendKind::ETCD;
    config_->memberStore = &store;
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
    config_->memberStore = nullptr;
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

TopologyEngine::Builder &TopologyEngine::Builder::SetPeerTopologyRefresh(PeerTopologyRefresh refresh)
{
    if (config_ != nullptr) {
        config_->peerTopologyRefresh = std::move(refresh);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetWorkerProbeHandler(
    std::function<Status(WorkerProbeRequest)> handler)
{
    if (config_ != nullptr) {
        config_->workerProbeHandler = std::move(handler);
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

TopologyEngine::Builder &TopologyEngine::Builder::SetMembershipRecreateGate(std::function<Status()> gate)
{
    if (config_ != nullptr) {
        config_->membershipRecreateGate = std::move(gate);
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetMembershipRestartHandler(
    std::function<Status(const std::map<std::string, int64_t> &, RestartEffectMode)> handler)
{
    if (config_ != nullptr) {
        config_->membershipRestartHandler = std::move(handler);
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

TopologyEngine::Builder &TopologyEngine::Builder::SetFailureScopeProbeInterval(std::chrono::milliseconds interval)
{
    if (config_ != nullptr) {
        config_->scopeProbeInterval = interval;
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetLocalIsolationTimeout(std::chrono::seconds timeout)
{
    if (config_ != nullptr) {
        config_->localIsolationTimeout = timeout;
    }
    return *this;
}

TopologyEngine::Builder &TopologyEngine::Builder::SetScaleInCollectWindow(std::chrono::milliseconds window)
{
    if (config_ != nullptr) {
        config_->scaleInCollectWindow = window;
    }
    return *this;
}

Status TopologyEngine::Builder::Validate() const
{
    CHECK_FAIL_RETURN_STATUS(config_ != nullptr && IsCanonicalAddress(config_->localAddress)
                                 && config_->callbacks != nullptr && config_->nodeDeadTimeout.count() >= 0
                                 && config_->localIsolationTimeout.count() >= 0
                                 && config_->scopeProbeInterval.count() > 0
                                 && config_->scaleInCollectWindow.count() >= 0
                                 && config_->scaleInCollectWindow.count() <= MAX_SCALE_IN_COLLECT_WINDOW_MS
                                 && !config_->backendSelectionInvalid,
                             K_INVALID, "invalid cluster topology Engine Builder settings");
    if (config_->backendKind == Config::BackendKind::ETCD) {
        CHECK_FAIL_RETURN_STATUS(config_->memberStore != nullptr && config_->controlBackendProbe != nullptr, K_INVALID,
                                 "ETCD topology Engine requires one shared Store and one Worker liveness probe");
    } else if (config_->backendKind == Config::BackendKind::COORDINATOR) {
        CHECK_FAIL_RETURN_STATUS(config_->coordinatorProxy != nullptr && config_->ingress.bind != nullptr
                                     && config_->ingress.unbindAndDrain != nullptr
                                     && config_->workerProbeHandler != nullptr,
                                 K_INVALID, "Coordinator topology Engine requires ingress and worker probe handlers");
    } else {
        RETURN_STATUS(K_INVALID, "cluster topology Engine backend is not selected");
    }
    return Status::OK();
}

Status TopologyEngine::Builder::CreateOwnedDependencies()
{
    RETURN_IF_NOT_OK(TopologyKeyHelper::Create(config_->clusterName, config_->keys));
    if (config_->backendKind == Config::BackendKind::ETCD) {
        RETURN_IF_NOT_OK(RegisterEtcdTopologyTables(*config_->memberStore, *config_->keys));
    }
    config_->algorithm = std::make_unique<HashAlgorithm>();
    if (config_->backendKind == Config::BackendKind::ETCD) {
        config_->memberBackend = std::make_unique<EtcdCoordinationBackend>(config_->memberStore);
        config_->controllerBackend = std::make_unique<EtcdCoordinationBackend>(config_->memberStore);
    } else {
        config_->memberBackend =
            std::make_unique<DsCoordinationBackend>(config_->coordinatorProxy, config_->localAddress);
        static_cast<DsCoordinationBackend *>(config_->memberBackend.get())
            ->SetMembershipRecreateGate(std::move(config_->membershipRecreateGate));
    }
    return Status::OK();
}

Status TopologyEngine::Builder::ReadRestartFact()
{
    TopologyRepository repository(*config_->memberBackend, *config_->keys);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> snapshot;
    auto rc = reader.Read(TopologyEngine::ENGINE_READ_TIMEOUT_MS, snapshot);
    if (rc.GetCode() == K_NOT_FOUND) {
        config_->isRestart = false;
        return Status::OK();
    }
    if (config_->backendKind == Config::BackendKind::COORDINATOR && rc.GetCode() == K_NOT_READY) {
        LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << config_->clusterName
                  << " role=worker action=skip_restart_fact reason=coordinator_recovery_not_ready";
        config_->isRestart = false;
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
        const auto nodeDeadTimeout = config_->nodeDeadTimeout;
        const auto scaleInCollectWindow = config_->scaleInCollectWindow;
        auto candidate = std::unique_ptr<TopologyEngine>(new TopologyEngine(std::move(config_)));
        RETURN_IF_NOT_OK(candidate->InitializeOwnedComponents(nodeDeadTimeout, scaleInCollectWindow));
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
    options.unifiedEtcdWatch = config.backendKind == Builder::Config::BackendKind::ETCD;
    options.nodeDeadTimeout = config.nodeDeadTimeout;
    options.localIsolationTimeout = config.localIsolationTimeout;
    options.scopeProbeInterval = config.scopeProbeInterval;
    options.controlBackendProbe = std::move(config.controlBackendProbe);
    options.peerTopologyRefresh = std::move(config.peerTopologyRefresh);
    options.workerProbeHandler = std::move(config.workerProbeHandler);
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
      membershipRestartHandler_(std::move(config->membershipRestartHandler)),
      keys_(std::move(config->keys)),
      repository_(*memberBackend_, *keys_),
      reader_(repository_),
      dispatcher_(options_.eventQueueCapacity),
      membershipView_(snapshots_),
      placement_(snapshots_, *algorithm_, options_.localAddress),
      executor_(options_.localAddress, repository_, snapshots_, *config->callbacks, dispatcher_,
                membershipRestartHandler_, options_.executor)
{
}

Status TopologyEngine::InitializeOwnedComponents(std::chrono::seconds nodeDeadTimeout,
                                                 std::chrono::milliseconds scaleInCollectWindow)
{
    if (options_.unifiedEtcdWatch) {
        TopologyControllerRuntime::Options runtimeOptions;
        runtimeOptions.clusterName = options_.clusterName;
        runtimeOptions.controller.nodeDeadTimeout = nodeDeadTimeout;
        runtimeOptions.controller.scaleInCollectWindow = scaleInCollectWindow;
        runtimeOptions.controller.failureProbeTimeout = options_.scopeProbeDeadline;
        runtimeOptions.controller.localAddress = options_.localAddress;
        if (membershipRestartHandler_ != nullptr) {
            runtimeOptions.controller.membershipRestartHandler =
                [handler = membershipRestartHandler_](const std::string &address, int64_t timestamp) {
                    return handler({ { address, timestamp } }, RestartEffectMode::EVENTUAL);
                };
        }
        runtimeOptions.controller.memberLivenessProbe =
            [probe = options_.controlBackendProbe, localAddress = options_.localAddress](
                const std::vector<MemberIdentity> &targets, std::chrono::steady_clock::time_point deadline) {
                std::vector<MemberIdentity> remoteTargets;
                std::vector<ControlBackendProbeResult> results;
                remoteTargets.reserve(targets.size());
                results.reserve(targets.size());
                for (const auto &target : targets) {
                    if (target.address == localAddress) {
                        results.push_back(
                            { target,
                              ControlBackendObservation{ target, ControlBackendState::UNKNOWN, 0, 0, "",
                                                         std::chrono::steady_clock::now() },
                              ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) });
                    } else {
                        remoteTargets.push_back(target);
                    }
                }
                if (!remoteTargets.empty()) {
                    auto remote = probe({}, remoteTargets, deadline);
                    results.insert(results.end(), std::make_move_iterator(remote.begin()),
                                   std::make_move_iterator(remote.end()));
                }
                return results;
            };
        runtimeOptions.controller.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
        runtimeOptions.janitor = TopologyTaskJanitorOptions{};
        RETURN_IF_NOT_OK(TopologyControllerRuntime::Create(
            std::move(runtimeOptions), *controllerBackend_, *algorithm_, controllerRuntime_));
    }
    InitializeCoordinatorComponents();
    return Status::OK();
}

void TopologyEngine::InitializeCoordinatorComponents()
{
    if (coordinatorProxy_ == nullptr) {
        return;
    }
    recoveryReporter_ = std::make_unique<TopologyRecoveryReporter>(
        *coordinatorProxy_, options_.clusterName, options_.localAddress,
        [this](uint64_t &version, std::string &canonical) { return GetRecoveryTopology(version, canonical); });
    if (coordinatorProxy_->GetLeaderRouteProvider() == nullptr) {
        return;
    }
    auto *member = static_cast<DsCoordinationBackend *>(memberBackend_.get());
    workerLeaderReconciler_ =
        std::make_unique<WorkerLeaderReconciler>(*coordinatorProxy_, *member, *recoveryReporter_, options_.clusterName);
    member->SetMembershipReconcileHandler(
        [this](bool waitForCompletion) { return workerLeaderReconciler_->Reconcile(waitForCompletion); });
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
    auto *member = static_cast<DsCoordinationBackend *>(memberBackend_.get());
    if (!member->OwnsWatchIdentity(coordinatorId, watchId) && member->IsWatchRegistrationInProgress()) {
        RETURN_STATUS(K_NOT_READY, "Coordinator topology watch registration is in progress");
    }
    if (!member->OwnsWatchIdentity(coordinatorId, watchId)) {
        LOG_FIRST_AND_EVERY_N(WARNING, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
            << "CLUSTER_WATCH cluster=" << options_.clusterName << " watch_id=" << watchId
            << " owner_state=missing action=rewatch";
        member->InvalidateWatches();
        return Status::OK();
    }
    LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
        << "CLUSTER_WATCH_EVENT cluster=" << options_.clusterName
        << " role=worker ingress=coordinator owner_role=member"
        << " watch_id=" << watchId << " coordinator_id_prefix=" << TopologyDiagnosticPrefix(coordinatorId)
        << " event=" << event.ToString();
    member->HandleWatchEvent(coordinatorId, watchId, std::move(event));
    return Status::OK();
}

Status TopologyEngine::EnqueueCoordinationEvent(CoordinationEvent &&event)
{
    LOG_FIRST_AND_EVERY_N(INFO, TOPOLOGY_WATCH_EVENT_LOG_INTERVAL)
        << "CLUSTER_WATCH_EVENT cluster=" << options_.clusterName << " role=worker event=" << event.ToString();
    const bool probeEvent = coordinatorProxy_ != nullptr && event.type != CoordinationEventType::RESET
                            && keys_->ClassifyPhysicalKey(event.key, options_.localAddress)
                                   == TopologyPhysicalKeyKind::LOCAL_PROBE;
    auto rc = probeEvent ? dispatcher_.SubmitCoordinationUncoalesced(std::move(event))
                         : dispatcher_.SubmitCoordination(std::move(event));
    if (probeEvent && rc.GetCode() == K_TRY_AGAIN && coordinatorProxy_ != nullptr) {
        static_cast<DsCoordinationBackend *>(memberBackend_.get())->InvalidateWatches();
    }
    if (rc.IsError() && rc.GetCode() != K_TRY_AGAIN && rc.GetCode() != K_NOT_READY) {
        RecordError(rc);
    }
    return rc;
}

Status TopologyEngine::RouteUnifiedEtcdWatchEvent(CoordinationEvent &&event)
{
    CHECK_FAIL_RETURN_STATUS(controllerRuntime_ != nullptr, K_NOT_READY,
                             "unified ETCD Controller runtime is not ready");
    const auto kind = event.type == CoordinationEventType::RESET
                          ? TopologyPhysicalKeyKind::TOPOLOGY
                          : keys_->ClassifyPhysicalKey(event.key, options_.localAddress);
    if (kind == TopologyPhysicalKeyKind::TOPOLOGY) {
        CoordinationEvent controllerEvent = event;
        auto memberStatus = EnqueueCoordinationEvent(std::move(event));
        auto controllerStatus = controllerRuntime_->SubmitCoordinationEvent(std::move(controllerEvent));
        return memberStatus.IsError() ? memberStatus : controllerStatus;
    }
    if (kind == TopologyPhysicalKeyKind::LOCAL_NOTIFY) {
        return EnqueueCoordinationEvent(std::move(event));
    }
    if (kind == TopologyPhysicalKeyKind::MEMBERSHIP || kind == TopologyPhysicalKeyKind::MIGRATE_TASK
        || kind == TopologyPhysicalKeyKind::DELETE_TASK) {
        return controllerRuntime_->SubmitCoordinationEvent(std::move(event));
    }
    RETURN_STATUS(K_INVALID, "unified ETCD watch received an unregistered physical key");
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
        auto *member = static_cast<DsCoordinationBackend *>(memberBackend_.get());
        member->SetMembershipReadyHandler([this](const std::string &coordinatorId, bool) {
            if (recoveryReporter_ == nullptr) {
                return;
            }
            if (workerLeaderReconciler_ != nullptr) {
                workerLeaderReconciler_->NotifyMembershipReady(coordinatorId);
            } else {
                recoveryReporter_->NotifyMembershipReady(coordinatorId);
            }
        });
    }
    auto rc = BindCoordinatorIngress();
    if (rc.IsOk()) {
        rc = StartMemberRole();
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
    // Start membership keepalive before the bootstrap read. Coordinator publishes synchronously; ETCD establishes its
    // first lease and membership Put asynchronously.
    RETURN_IF_NOT_OK(
        memberBackend_->InitKeepAlive(keys_->MembershipTable(), options_.localAddress, options_.isRestart, true));
    if (options_.unifiedEtcdWatch) {
        auto rc = controllerRuntime_->Start();
        if (rc.IsError()) {
            LOG_IF_ERROR(memberBackend_->ShutdownEventSources(),
                         "Stop membership event sources after Controller bootstrap failure");
            LOG_IF_ERROR(memberBackend_->Delete(keys_->MembershipTable(), options_.localAddress),
                         "CLUSTER_MEMBERSHIP_STARTUP_CLEANUP_FAILED");
            return rc;
        }
    }

    const int64_t controllerRevision =
        options_.unifiedEtcdWatch ? controllerRuntime_->GetBootstrapRevision() : 0;
    CHECK_FAIL_RETURN_STATUS(!options_.unifiedEtcdWatch || controllerRevision > 0, K_INVALID,
                             "unified ETCD Controller bootstrap revision is invalid");
    int64_t watchRevision = 0;
    auto readStatus = ReloadTopology(true);
    if (readStatus.IsError()) {
        RecordError(readStatus);
        if (readStatus.GetCode() != K_NOT_FOUND && readStatus.GetCode() != K_NOT_READY) {
            return readStatus;
        }
        if (options_.unifiedEtcdWatch) {
            watchRevision = WATCH_FROM_NOW;
        }
        LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                  << " role=worker state=waiting_for_topology_bootstrap";
    } else if (options_.unifiedEtcdWatch) {
        std::shared_ptr<const TopologySnapshot> published;
        RETURN_IF_NOT_OK(snapshots_.Load(published));
        watchRevision = published->AuthorityRevision();
    }

    std::vector<WatchKey> watches;
    const auto role = options_.unifiedEtcdWatch ? TopologyRuntimeRole::UNIFIED_ETCD : TopologyRuntimeRole::WORKER;
    const auto factRevision = options_.unifiedEtcdWatch ? controllerRevision : watchRevision;
    RETURN_IF_NOT_OK(BuildTopologyRoleWatchPlan(role, options_.localAddress, *keys_, factRevision, watches));
    if (options_.unifiedEtcdWatch) {
        for (auto &watch : watches) {
            if (watch.tableName == keys_->NotifyTable()) {
                watch.startRevision = watchRevision;
            }
        }
    }
    auto rc = memberBackend_->WatchEvents(watches);
    if (rc.IsError()) {
        LOG_IF_ERROR(memberBackend_->Delete(keys_->MembershipTable(), options_.localAddress),
                     "CLUSTER_MEMBERSHIP_STARTUP_CLEANUP_FAILED");
        return rc;
    }
    if (options_.unifiedEtcdWatch) {
        rc = controllerRuntime_->SubmitCoordinationEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
        if (rc.IsError()) {
            LOG_IF_ERROR(memberBackend_->Delete(keys_->MembershipTable(), options_.localAddress),
                         "CLUSTER_MEMBERSHIP_STARTUP_CLEANUP_FAILED");
            return rc;
        }
    }
    const bool watchFromNow = factRevision == WATCH_FROM_NOW;
    LOG(INFO) << "CLUSTER_WATCH cluster=" << options_.clusterName
              << " role=" << (options_.unifiedEtcdWatch ? "unified_etcd" : "worker")
              << " scope_count=" << watches.size()
              << " facts_start_mode=" << (watchFromNow ? "from_now" : "after_revision")
              << " facts_last_processed_revision=" << (watchFromNow ? "none" : std::to_string(factRevision))
              << " notify_revision=" << (watchRevision == WATCH_FROM_NOW ? "none" : std::to_string(watchRevision))
              << " status=registered";

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
    if (coordinatorProxy_ != nullptr) {
        auto *member = static_cast<DsCoordinationBackend *>(memberBackend_.get());
        // Keepalive can complete while its event sources drain; it must not enter engine-owned callbacks then.
        member->SetMembershipReadyHandler({});
        member->SetMembershipReconcileHandler({});
    }
    if (workerLeaderReconciler_ != nullptr) {
        workerLeaderReconciler_->Shutdown();
    }
    PreserveFirstError(memberBackend_->ShutdownEventSources(), firstError);
    workerLeaderReconciler_.reset();
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
    if (controllerRuntime_ != nullptr) {
        PreserveFirstError(controllerRuntime_->Stop(deadline), firstError);
    }
    if (firstError.IsOk()) {
        PreserveFirstError(memberBackend_->Shutdown(), firstError);
        if (controllerBackend_ != nullptr) {
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

bool TopologyEngine::RequiresMembershipRejoin() const noexcept
{
    return membershipRejoinRequired_.load(std::memory_order_relaxed);
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
              << " role=worker action=publish_ready_membership purpose=topology_admission address="
              << options_.localAddress << " status=" << rc.ToString();
    return rc;
}

Status TopologyEngine::MarkExiting()
{
    return MarkExiting(SEND_RPC_TIMEOUT_MS_DEFAULT);
}

Status TopologyEngine::MarkExiting(int32_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    localVoluntaryExitRequested_.store(true);
    auto rc = memberBackend_->UpdateNodeStateWithTimeout(MemberLifecycleState::EXITING, timeoutMs);
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

const std::string &TopologyEngine::GetMembershipTableName() const noexcept
{
    return keys_->MembershipTable();
}

Status TopologyEngine::PutWithMembershipLease(const std::string &tableName, const std::string &key,
                                              const std::string &value)
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    return memberBackend_->PutWithKeepAliveLease(tableName, key, value);
}

Status TopologyEngine::GetMembershipSidecar(
    const std::string &tableName, std::vector<std::pair<std::string, std::string>> &records) const
{
    CHECK_FAIL_RETURN_STATUS(state_.load() == TopologyEngineState::RUNNING, K_NOT_READY,
                             "cluster topology Engine is not running");
    return memberBackend_->GetAll(tableName, records);
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
        diagnostics.controlBackendState = backendObservation_.state;
        diagnostics.isolationReason = isolationReason_;
        diagnostics.lastError = lastError_;
    }
    diagnostics.peerObservedTopologyVersion = peerObservedTopologyVersion_.load(std::memory_order_relaxed);
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
              << " local_member_id_prefix=" << (localStatus.IsOk() ? MemberIdForLog(local->identity.id) : "")
              << " " << TopologyRingViewsForLog(*published);
    NotifySnapshotPublished(std::move(published));
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
        const bool localMemberExisted =
            localMemberExistedInPreviousSnapshot_.exchange(false, std::memory_order_relaxed);
        const bool localMemberWasLeaving =
            localMemberWasLeavingInPreviousSnapshot_.exchange(false, std::memory_order_relaxed);
        const bool localVoluntaryExitRequested = localVoluntaryExitRequested_.load();
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            backendObservation_ = {};
        }
        if (!localMemberExisted || localMemberWasLeaving || localVoluntaryExitRequested) {
            membershipRejoinRequired_.store(false, std::memory_order_relaxed);
            SetAvailability(TopologyAvailabilityLevel::NOT_READY, "local_member_missing");
            if (localMemberWasLeaving || localVoluntaryExitRequested) {
                LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                          << " role=worker state=local_member_missing action=continue_voluntary_exit address="
                          << options_.localAddress;
            }
            return Status::OK();
        }
        LOG(ERROR) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                   << " role=worker state=local_member_missing action=require_rejoin address=" << options_.localAddress;
        membershipRejoinRequired_.store(true, std::memory_order_relaxed);
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "local_member_missing");
        return Status::OK();
    }
    RETURN_IF_NOT_OK(findStatus);
    ResetLocalIsolationEvidence();
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
        membershipRejoinRequired_.store(true, std::memory_order_relaxed);
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED,
                        identityChanged ? "local_identity_changed" : "local_member_failed");
    } else {
        membershipRejoinRequired_.store(false, std::memory_order_relaxed);
        SetAvailability(IsCommittedMemberState(local->state) ? TopologyAvailabilityLevel::NORMAL
                                                             : TopologyAvailabilityLevel::NOT_READY,
                        IsCommittedMemberState(local->state) ? "" : "local_member_not_committed");
    }
    localMemberExistedInPreviousSnapshot_.store(true, std::memory_order_relaxed);
    localMemberWasLeavingInPreviousSnapshot_.store(
        local->state == MemberState::PRE_LEAVING || local->state == MemberState::LEAVING, std::memory_order_relaxed);
    return Status::OK();
}

Status TopologyEngine::HandleRuntimeEvent(RuntimeEvent event)
{
    if (auto *completion = std::get_if<TopologyCallbackCompletion>(&event.payload)) {
        return executor_.HandleCompletion(std::move(*completion));
    }
    auto coordination = std::get<CoordinationEvent>(std::move(event.payload));
    if (coordinatorProxy_ != nullptr && coordination.type == CoordinationEventType::PUT
        && keys_->ClassifyPhysicalKey(coordination.key, options_.localAddress)
               == TopologyPhysicalKeyKind::LOCAL_PROBE) {
        return HandleWorkerProbeEvent(coordination);
    }
    auto rc = ReloadTopologyAndNotify();
    if (rc.IsError()) {
        if (IsBackendAccessFailure(rc)) {
            LOG_IF_ERROR(HandleBackendUnavailable(), "CLUSTER_BACKEND_FAILURE_CLASSIFICATION_FAILED");
        }
    }
    return rc;
}

Status TopologyEngine::HandleWorkerProbeEvent(const CoordinationEvent &event)
{
    CHECK_FAIL_RETURN_STATUS(options_.workerProbeHandler != nullptr, K_NOT_SUPPORTED,
                             "worker probe handler is not configured");
    coordinator::WorkerProbeEventValuePb value;
    CHECK_FAIL_RETURN_STATUS(value.ParseFromString(event.value), K_INVALID, "invalid worker probe event value");
    CHECK_FAIL_RETURN_STATUS(value.cluster_name() == options_.clusterName && !value.coordinator_id().empty(), K_INVALID,
                             "worker probe event cluster or CoordinatorId is invalid");
    CHECK_FAIL_RETURN_STATUS(value.probe_round() > 0 && !value.target_address().empty()
                                 && !value.target_member_id().empty(),
                             K_INVALID, "invalid worker probe target fields");
    WorkerProbeRequest request{ value.cluster_name(), value.coordinator_id(), value.probe_round(),
                                { value.target_member_id(), value.target_address() } };
    const auto probeId = WorkerProbeIdForLog(request.probeEpoch, request.probeRound);
    LOG(INFO) << "CLUSTER_WORKER_PROBE cluster=" << request.clusterName
              << " action=WITNESS_PROBE_EVENT_RECEIVED probe_id=" << probeId
              << " witness=" << options_.localAddress << " target=" << request.target.address
              << " target_id_prefix=" << MemberIdForLog(request.target.id) << " revision=" << event.revision;
    auto status = options_.workerProbeHandler(std::move(request));
    if (status.IsError()) {
        LOG(WARNING) << "CLUSTER_WORKER_PROBE cluster=" << value.cluster_name()
                     << " action=WITNESS_PROBE_ENQUEUE_FAILED probe_id=" << probeId
                     << " witness=" << options_.localAddress << " target=" << value.target_address()
                     << " target_id_prefix=" << MemberIdForLog(value.target_member_id())
                     << " status=" << status.ToString();
    }
    return status;
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
    const auto current = availability_.load();
    if (current == TopologyAvailabilityLevel::ROLE_ISOLATED
        || current == TopologyAvailabilityLevel::CONTROL_DEGRADED) {
        return Status::OK();
    }
    ResetLocalIsolationEvidence();
    SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "control_backend_unavailable");
    return Status::OK();
}

Status TopologyEngine::ReevaluateFailureScope()
{
    ControlBackendObservation local;
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        local = backendObservation_;
    }
    if (!options_.controlBackendProbe) {
        ResetLocalIsolationEvidence();
        SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "backend_scope_unknown");
        return Status::OK();
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    std::vector<MemberIdentity> targets;
    auto rc = snapshots_.Load(snapshot);
    if (rc.IsOk()) {
        rc = SelectProbeTargets(*snapshot, options_.localAddress, targets);
    }
    if (rc.IsError()) {
        ResetLocalIsolationEvidence();
        SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "backend_scope_unknown");
        return Status::OK();
    }
    const auto deadline = std::chrono::steady_clock::now() + options_.scopeProbeDeadline;
    std::vector<ControlBackendProbeResult> results;
    // An injected control-plane probe must not unwind through the Engine state thread.
    try {
        results = options_.controlBackendProbe(local, targets, deadline);
    } catch (const std::exception &error) {
        LOG(ERROR) << "CLUSTER_BACKEND_PROBE_FAILED reason=exception error=" << error.what();
    } catch (...) {
        LOG(ERROR) << "CLUSTER_BACKEND_PROBE_FAILED reason=unknown_exception";
    }
    const auto scope = ClassifyBackendFailure(local, targets, results);
    if (scope == BackendFailureScope::GLOBAL_OUTAGE) {
        ResetLocalIsolationEvidence();
        SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "control_backend_unavailable");
    } else if (scope == BackendFailureScope::LOCAL_BACKEND_PATH) {
        if (localIsolationConfirmations_ == 0) {
            localIsolationStartedAt_ = std::chrono::steady_clock::now();
        }
        if (++localIsolationConfirmations_ < LOCAL_ISOLATION_CONFIRMATIONS) {
            SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "local_isolation_confirmation_pending");
            return Status::OK();
        }
        if (!isolationKillDeadline_.has_value()) {
            isolationKillDeadline_ = *localIsolationStartedAt_ + options_.localIsolationTimeout;
        }
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "local_backend_path_isolated");
    } else {
        ResetLocalIsolationEvidence();
        SetAvailability(TopologyAvailabilityLevel::CONTROL_DEGRADED, "backend_scope_inconclusive");
    }
    return Status::OK();
}

Status TopologyEngine::RefreshPeerTopology()
{
    if (!options_.peerTopologyRefresh) {
        return Status::OK();
    }
    std::shared_ptr<const TopologySnapshot> current;
    std::vector<MemberIdentity> targets;
    auto rc = snapshots_.Load(current);
    if (rc.IsOk()) {
        rc = SelectProbeTargets(*current, options_.localAddress, targets);
    }
    if (rc.IsError() || targets.empty()) {
        return Status::OK();
    }
    std::shared_ptr<const TopologySnapshot> peerSnapshot;
    rc = options_.peerTopologyRefresh(current->Version(), targets,
                                      std::chrono::steady_clock::now() + options_.scopeProbeDeadline, peerSnapshot);
    if (rc.IsError() || peerSnapshot == nullptr || peerSnapshot->Version() <= current->Version()) {
        VLOG_IF(1, rc.IsError()) << "CLUSTER_PEER_TOPOLOGY_REFRESH_IGNORED status=" << rc.ToString();
        return Status::OK();
    }
    peerObservedTopologyVersion_.store(peerSnapshot->Version(), std::memory_order_relaxed);
    const Member *local = nullptr;
    rc = peerSnapshot->FindMemberByAddress(options_.localAddress, local);
    const bool localMissing = rc.GetCode() == K_NOT_FOUND;
    if (!localMissing) {
        RETURN_IF_NOT_OK(rc);
    }
    if (localMissing || local->state == MemberState::FAILED) {
        const auto reason = localMissing ? "missing" : "failed";
        if (localVoluntaryExitRequested_.load()) {
            membershipRejoinRequired_.store(false, std::memory_order_relaxed);
            SetAvailability(TopologyAvailabilityLevel::NOT_READY, "peer_observed_local_member_unavailable");
            LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                      << " role=worker state=peer_observed_local_member_unavailable action=continue_voluntary_exit"
                      << " reason=" << reason << " address=" << options_.localAddress
                      << " peer_version=" << peerSnapshot->Version();
            return Status::OK();
        }
        LOG(ERROR) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
                   << " role=worker state=peer_observed_local_member_unavailable action=require_rejoin"
                   << " reason=" << reason << " address=" << options_.localAddress
                   << " peer_version=" << peerSnapshot->Version();
        membershipRejoinRequired_.store(true, std::memory_order_relaxed);
        SetAvailability(TopologyAvailabilityLevel::ROLE_ISOLATED, "peer_observed_local_member_unavailable");
        return Status::OK();
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
        const bool reevaluate = availability_.load() == TopologyAvailabilityLevel::CONTROL_DEGRADED;
        LOG_IF_ERROR(HandleBackendUnavailable(), "CLUSTER_BACKEND_FAILURE_REEVALUATION_FAILED");
        if (reevaluate) {
            LOG_IF_ERROR(ReevaluateFailureScope(), "CLUSTER_BACKEND_FAILURE_REEVALUATION_FAILED");
        }
        LOG_IF_ERROR(RefreshPeerTopology(), "CLUSTER_PEER_TOPOLOGY_REFRESH_FAILED");
    } else if (availability_.load() == TopologyAvailabilityLevel::CONTROL_DEGRADED) {
        ResetLocalIsolationEvidence();
    }
    return rc;
}

void TopologyEngine::ResetLocalIsolationEvidence()
{
    localIsolationConfirmations_ = 0;
    localIsolationStartedAt_.reset();
    isolationKillDeadline_.reset();
}

void TopologyEngine::KillSelfIfIsolationExpired()
{
    if (!isolationKillDeadline_.has_value() || std::chrono::steady_clock::now() < *isolationKillDeadline_
        || state_.load() == TopologyEngineState::STOPPING) {
        return;
    }
    LOG(ERROR) << "CLUSTER_LIFECYCLE cluster=" << options_.clusterName
               << " role=worker state=local_isolation action=keep_alive address=" << options_.localAddress;
    isolationKillDeadline_.reset();
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
        NotifyAvailability(level);
    }
    if (publish) {
        publishedAvailability_.store(level);
    }
    LOG(INFO) << "CLUSTER_DEGRADED cluster=" << options_.clusterName << " role=worker"
              << " previous_level=" << static_cast<uint32_t>(previous) << " level=" << static_cast<uint32_t>(level)
              << " reason=" << reason << " published=" << publish;
}

void TopologyEngine::NotifyAvailability(TopologyAvailabilityLevel level) noexcept
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

void TopologyEngine::NotifySnapshotPublished(std::shared_ptr<const TopologySnapshot> snapshot) noexcept
{
    if (options_.snapshotPublishedHandler == nullptr) {
        return;
    }
    const uint64_t version = snapshot == nullptr ? 0 : snapshot->Version();
    try {
        options_.snapshotPublishedHandler(std::move(snapshot));
    } catch (const std::exception &error) {
        LOG(ERROR) << "CLUSTER_SNAPSHOT_HANDLER_FAILED cluster=" << options_.clusterName
                   << " local_address=" << options_.localAddress << " version=" << version
                   << " reason=exception error=" << error.what();
    } catch (...) {
        LOG(ERROR) << "CLUSTER_SNAPSHOT_HANDLER_FAILED cluster=" << options_.clusterName
                   << " local_address=" << options_.localAddress << " version=" << version
                   << " reason=unknown_exception";
    }
}

void TopologyEngine::RecordError(const Status &status)
{
    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        lastError_ = status.ToString();
    }
    if (availability_.load() == TopologyAvailabilityLevel::CONTROL_DEGRADED && IsBackendAccessFailure(status)) {
        LOG_EVERY_N(WARNING, CONTROL_DEGRADED_ERROR_LOG_INTERVAL)
            << "CLUSTER_RUNTIME_OPERATION_FAILED status=" << status.ToString();
        return;
    }
    LOG(WARNING) << "CLUSTER_RUNTIME_OPERATION_FAILED status=" << status.ToString();
}

void TopologyEngine::Run()
{
    auto nextExactRefresh = std::chrono::steady_clock::now() + options_.scopeProbeInterval;
    while (state_.load() != TopologyEngineState::STOPPING) {
        RuntimeEvent event;
        const auto waitDeadline = isolationKillDeadline_.has_value()
                                      ? std::min(nextExactRefresh, *isolationKillDeadline_)
                                      : nextExactRefresh;
        auto rc = dispatcher_.WaitPop(waitDeadline, event);
        if (rc.IsOk()) {
            rc = HandleRuntimeEvent(std::move(event));
        }
        if (rc.GetCode() != K_RPC_DEADLINE_EXCEEDED && rc.GetCode() != K_NOT_READY && rc.IsError()) {
            RecordError(rc);
        }
        if (dispatcher_.ConsumeResyncRequired()) {
            LOG(WARNING) << "CLUSTER_WATCH cluster=" << options_.clusterName
                         << " role=worker scope=all status=resync queued_events=" << dispatcher_.GetStats().queueDepth;
            auto rebuild = ReloadTopology(true);
            if (rebuild.IsError()) {
                RecordError(rebuild);
            }
        }
        auto tick = executor_.HandleTick(std::chrono::steady_clock::now());
        if (tick.IsError() && tick.GetCode() != K_NOT_READY) {
            RecordError(tick);
        }
        if (std::chrono::steady_clock::now() >= nextExactRefresh) {
            auto refresh = RefreshUnavailableBackend();
            if (refresh.IsError() && refresh.GetCode() != K_NOT_READY && refresh.GetCode() != K_TRY_AGAIN) {
                RecordError(refresh);
            }
            nextExactRefresh = std::chrono::steady_clock::now() + options_.scopeProbeInterval;
        }
        KillSelfIfIsolationExpired();
    }
    std::lock_guard<std::mutex> lock(stateMutex_);
    threadExited_ = true;
    stoppedCv_.notify_all();
}

}  // namespace datasystem::cluster
