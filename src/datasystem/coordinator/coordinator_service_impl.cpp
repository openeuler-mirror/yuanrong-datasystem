/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/coordinator/coordinator_service_impl.h"

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <utility>

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/coordinator/topology_control_host.h"
#include "datasystem/coordinator/raft/coordinator_raft_service.h"
#include "datasystem/coordinator/topology_recovery_manager.h"

namespace {
constexpr char kDefaultCoordinatorRaftDataDir[] = "./datasystem/coordinator_raft";
constexpr int32_t kDefaultCoordinatorRaftHeartbeatIntervalMs = 100;
constexpr int32_t kDefaultCoordinatorRaftElectionTimeoutMs = 1'000;
constexpr uint32_t kDefaultCoordinatorDiscoveryRetryIntervalMs = 5'000;
constexpr uint32_t kDefaultCoordinatorMemberFailureGraceMs = 10'000;
}  // namespace

DS_DEFINE_uint64(coordinator_rpc_stub_cache_size, 4096, "Maximum coordinator RPC stub cache size.");
DS_DEFINE_uint32(coordinator_topology_max_active_clusters, 8,
                 "Maximum number of active cluster topology Controller runtimes.");
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(scale_in_collect_window_ms);
DS_DEFINE_string(coordinator_raft_initial_peers, "", "Coordinator Raft initial peers.");
DS_DEFINE_string(coordinator_raft_data_dir, kDefaultCoordinatorRaftDataDir,
                 "Exclusive local data root for Coordinator Raft term, vote, log, and snapshot state.");
DS_DEFINE_int32(coordinator_raft_heartbeat_interval_ms, kDefaultCoordinatorRaftHeartbeatIntervalMs,
                "Coordinator Raft heartbeat interval in milliseconds.");
DS_DEFINE_int32(coordinator_raft_election_timeout_ms, kDefaultCoordinatorRaftElectionTimeoutMs,
                "Coordinator Raft election timeout in milliseconds.");
DS_DEFINE_uint32(coordinator_member_failure_grace_ms, kDefaultCoordinatorMemberFailureGraceMs,
                 "Continuous Coordinator voting-member failure grace period in milliseconds.");
DS_DEFINE_uint32(coordinator_discovery_retry_interval_ms, kDefaultCoordinatorDiscoveryRetryIntervalMs,
                 "Minimum interval between Coordinator candidate Discovery retries in milliseconds.");

DS_DEFINE_validator(coordinator_raft_heartbeat_interval_ms, &Validator::ValidateInt32);
DS_DEFINE_validator(coordinator_raft_election_timeout_ms, &Validator::ValidateInt32);
DS_DEFINE_validator(coordinator_member_failure_grace_ms, &Validator::ValidateUint32);
DS_DEFINE_validator(coordinator_discovery_retry_interval_ms, &Validator::ValidateUint32);

namespace datasystem {
namespace coordinator {
namespace {
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;
constexpr size_t MAX_CLUSTER_RAW_SNAPSHOT_BYTES = 16 * 1'024 * 1'024;
constexpr size_t MAX_CLUSTER_RAW_MEMBERSHIPS = 10'000;
constexpr size_t MIN_ACTIVE_CLUSTERS = 2;
constexpr size_t MAX_ACTIVE_CLUSTERS = 32;
constexpr auto COORDINATOR_TOPOLOGY_SHUTDOWN_GRACE = std::chrono::seconds(5);

Status CheckCoordinatorStore(const std::shared_ptr<CoordinatorStore> &store)
{
    CHECK_FAIL_RETURN_STATUS(store != nullptr, StatusCode::K_NOT_READY, "coordinator store is not bound");
    return Status::OK();
}

void FillKeyValuePb(const KeyValueEntry &entry, KeyValue *kv)
{
    kv->set_key(entry.key);
    kv->set_value(entry.value);
    kv->set_version(entry.version);
    kv->set_mod_revision(entry.modRevision);
}

void FillKeyValuePbs(const std::vector<KeyValueEntry> &entries, google::protobuf::RepeatedPtrField<KeyValue> *output)
{
    for (const auto &entry : entries) {
        FillKeyValuePb(entry, output->Add());
    }
}

Status BuildClusterReadKeys(const std::string &clusterName, std::string &topologyKey, std::string &membershipKey,
                            std::string &membershipEnd)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
    topologyKey = keys->TopologyTable() + "/";
    membershipKey = keys->MembershipTable() + "/";
    membershipEnd = StringPlusOne(membershipKey);
    return Status::OK();
}

CoordinatorRecoveryStatePb ToPbRecoveryState(TopologyRecoveryState state)
{
    if (state == TopologyRecoveryState::READY) {
        return COORDINATOR_READY;
    }
    if (state == TopologyRecoveryState::BLOCKED) {
        return COORDINATOR_BLOCKED;
    }
    return COORDINATOR_RECOVERING;
}

RaftMetadataStatePb ToPbRaftMetadataState(RaftMetadataState state)
{
    switch (state) {
        case RaftMetadataState::ABSENT:
            return RAFT_METADATA_ABSENT;
        case RaftMetadataState::VALID:
            return RAFT_METADATA_VALID;
        case RaftMetadataState::CORRUPT:
            return RAFT_METADATA_CORRUPT;
        case RaftMetadataState::UNKNOWN:
            return RAFT_METADATA_UNKNOWN;
    }
    return RAFT_METADATA_UNKNOWN;
}

RaftBootstrapPhasePb ToPbRaftBootstrapPhase(RaftBootstrapPhase phase)
{
    switch (phase) {
        case RaftBootstrapPhase::OBSERVING:
            return RAFT_BOOTSTRAP_OBSERVING;
        case RaftBootstrapPhase::RETRYING:
            return RAFT_BOOTSTRAP_RETRYING;
        case RaftBootstrapPhase::STARTED:
            return RAFT_BOOTSTRAP_STARTED;
        case RaftBootstrapPhase::TERMINAL:
            return RAFT_BOOTSTRAP_TERMINAL;
    }
    return RAFT_BOOTSTRAP_TERMINAL;
}

ReportTopologyRecoveryCandidateRspPb::ResultPb ToPbReportResult(TopologyRecoveryReportResult result)
{
    switch (result) {
        case TopologyRecoveryReportResult::ACCEPTED:
            return ReportTopologyRecoveryCandidateRspPb::ACCEPTED;
        case TopologyRecoveryReportResult::COORDINATOR_ID_MISMATCH:
            return ReportTopologyRecoveryCandidateRspPb::COORDINATOR_ID_MISMATCH;
        case TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY:
            return ReportTopologyRecoveryCandidateRspPb::MEMBERSHIP_NOT_READY;
    }
    return ReportTopologyRecoveryCandidateRspPb::RESULT_UNSPECIFIED;
}
}  // namespace

CoordinatorServiceImpl::CoordinatorServiceImpl(const HostPort &localAddress,
                                               std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery,
                                               size_t expectedMemberCount, CoordinatorRaftFlags raftFlags)
    : CoordinatorService(localAddress),
      coordinatorAddr_(localAddress),
      coordinatorDiscovery_(std::move(coordinatorDiscovery)),
      expectedMemberCount_(expectedMemberCount),
      raftFlags_(std::move(raftFlags))
{
}

CoordinatorServiceImpl::~CoordinatorServiceImpl() noexcept
{
    (void)Shutdown();
    servingState_.store(ServingState::STOPPED, std::memory_order_release);
}

bool CoordinatorServiceImpl::IsElectionConfigured() const noexcept
{
    return coordinatorDiscovery_ != nullptr && expectedMemberCount_ > 1;
}

Status CoordinatorServiceImpl::ValidateElectionConfiguration() const
{
    const bool discoveryConfigured = coordinatorDiscovery_ != nullptr;
    const bool memberCountConfigured = expectedMemberCount_ > 0;
    CHECK_FAIL_RETURN_STATUS(discoveryConfigured == memberCountConfigured, K_INVALID,
                             "Coordinator election Discovery and expected member count must be configured together");
    return Status::OK();
}

Status CoordinatorServiceImpl::BuildElectionStartupContext(CoordinatorElectionOptions &options) const
{
    CHECK_FAIL_RETURN_STATUS(IsElectionConfigured(), K_INVALID, "Coordinator election is not configured");
    options.raftFlags = raftFlags_;
    options.membershipOptions =
        CoordinatorMembershipOptions{ expectedMemberCount_,
                                      std::chrono::milliseconds(raftFlags_.healthCheckIntervalMs),
                                      std::chrono::milliseconds(raftFlags_.memberFailureGraceMs),
                                      std::chrono::milliseconds(raftFlags_.discoveryRetryIntervalMs),
                                      std::chrono::milliseconds(raftFlags_.operationWarningTimeoutMs),
                                      std::chrono::milliseconds(raftFlags_.candidateRetryCooldownMs) };
    return Status::OK();
}

CoordinatorRaftEventCallbacks CoordinatorServiceImpl::BuildRaftEventCallbacks()
{
    CoordinatorRaftEventCallbacks callbacks;
    // The Service exclusively owns and synchronously drains the Manager/Node before its own destruction, so this raw
    // capture cannot outlive the atomic gate and does not create a shared-ownership cycle.
    callbacks.onLeaderStart = [this](int64_t) {
        const auto state = servingState_.load(std::memory_order_acquire);
        if (state != ServingState::STARTING && state != ServingState::RUNNING) {
            return;
        }
        raftServing_.store(true, std::memory_order_release);
        const auto publishedState = servingState_.load(std::memory_order_acquire);
        if (publishedState == ServingState::STOPPING || publishedState == ServingState::STOPPED) {
            raftServing_.store(false, std::memory_order_release);
        }
    };
    callbacks.onLeaderStop = [this](Status) { raftServing_.store(false, std::memory_order_release); };
    callbacks.onError = [this](Status) { raftServing_.store(false, std::memory_order_release); };
    callbacks.onShutdown = [this] { raftServing_.store(false, std::memory_order_release); };
    return callbacks;
}

Status CoordinatorServiceImpl::CheckServing() const
{
    switch (servingState_.load(std::memory_order_acquire)) {
        case ServingState::CREATED:
            return Status(K_NOT_READY, "Coordinator service is not initialized");
        case ServingState::INITIALIZED:
            return Status(K_NOT_READY, "Coordinator service is initialized but has not started");
        case ServingState::STARTING:
            return Status(K_NOT_READY, "Coordinator service is starting and not ready to serve requests");
        case ServingState::RUNNING:
            if (IsElectionConfigured() && !raftServing_.load(std::memory_order_acquire)) {
                return Status(K_NOT_READY, "Coordinator service is not the active Raft node");
            }
            return Status::OK();
        case ServingState::STOPPING:
            return Status(K_SHUTTING_DOWN, "Coordinator service is shutting down");
        case ServingState::STOPPED:
            return Status(K_SHUTTING_DOWN, "Coordinator service is stopped");
    }
    return Status(K_NOT_READY, "Coordinator service is in an unknown serving state");
}

Status CoordinatorServiceImpl::Init()
{
    return Init(false);
}

Status CoordinatorServiceImpl::Init(bool publishStarting)
{
    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    const auto state = servingState_.load(std::memory_order_acquire);
    if (state == ServingState::STOPPING || state == ServingState::STOPPED) {
        return Status(K_SHUTTING_DOWN, "Coordinator service cannot initialize after shutdown has started");
    }
    if (state != ServingState::CREATED) {
        return Status(K_INVALID, "Coordinator service can only be initialized once");
    }
    Status initStatus = InitInternal();
    if (initStatus.IsError()) {
        LOG_IF_ERROR(ShutdownInternal(lock), "Coordinator cleanup after initialization failure also failed");
        return initStatus;
    }
    servingState_.store(publishStarting ? ServingState::RUNNING : ServingState::INITIALIZED, std::memory_order_release);
    return initStatus;
}

Status CoordinatorServiceImpl::InitInternal()
{
    RETURN_IF_NOT_OK(ValidateElectionConfiguration());
    CHECK_FAIL_RETURN_STATUS(!IsElectionConfigured() || FLAGS_use_brpc, K_INVALID,
                             "Coordinator election requires use_brpc=true; ZMQ election startup is unsupported");
    CHECK_FAIL_RETURN_STATUS(!IsElectionConfigured() || raftFlags_.localAddress == coordinatorAddr_.ToString(),
                             K_INVALID,
                             "Coordinator Raft snapshot localAddress must match the Coordinator service address");
    Logging::GetInstance()->Start("datasystem_coordinator", LogProcessRole::COORDINATOR);
    coordinatorId_ = GetBytesUuid();
    LOG(INFO) << "CLUSTER_COORDINATOR_ID role=coordinator id="
              << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE) << " state=created";
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_coordinator_rpc_stub_cache_size, coordinatorAddr_));
    RETURN_IF_NOT_OK(BuildComponentTree());
    ConfigureRpcService();
    return Status::OK();
}

Status CoordinatorServiceImpl::BuildComponentTree()
{
    CHECK_FAIL_RETURN_STATUS(FLAGS_coordinator_topology_max_active_clusters >= MIN_ACTIVE_CLUSTERS
                                 && FLAGS_coordinator_topology_max_active_clusters <= MAX_ACTIVE_CLUSTERS,
                             K_INVALID, "invalid Coordinator topology active cluster limit");
    memStore_ = std::make_shared<MemoryKvStore>();
    watchRegistry_ = std::make_shared<WatchRegistry>();
    watchDispatcher_ = std::make_shared<WatchDispatcherImpl>(watchRegistry_.get(), coordinatorId_);
    clock_ = std::make_shared<SteadyClockReal>();
    ttlManager_ = std::make_shared<TtlManager>(clock_);
    store_ = std::make_shared<CoordinatorStore>(memStore_, watchRegistry_, watchDispatcher_, ttlManager_);
    TopologyRecoveryOptions recoveryOptions;
    recoveryOptions.maxClusters = FLAGS_coordinator_topology_max_active_clusters;
    topologyRecoveryManager_ =
        std::make_unique<TopologyRecoveryManager>(coordinatorId_, *store_, clock_, recoveryOptions);
    TopologyControlHost::Options hostOptions;
    hostOptions.maxClusters = FLAGS_coordinator_topology_max_active_clusters;
    hostOptions.controller.nodeDeadTimeout = std::chrono::seconds(FLAGS_node_dead_timeout_s);
    hostOptions.controller.scaleInCollectWindow = std::chrono::milliseconds(FLAGS_scale_in_collect_window_ms);
    topologyControlHost_ =
        std::make_unique<TopologyControlHost>(coordinatorId_, *store_, *topologyRecoveryManager_, hostOptions);
    RETURN_IF_NOT_OK(topologyControlHost_->Start());
    store_->SetCommittedMutationObserver(
        [this](WatchEvent::Type type, const std::string &key) { HandleCommittedMutation(type, key); });
    return Status::OK();
}

void CoordinatorServiceImpl::HandleCommittedMutation(WatchEvent::Type type, const std::string &key)
{
    if (topologyRecoveryManager_ == nullptr) {
        return;
    }
    ParsedTopologyCoordinationKey parsed;
    const auto parseStatus = topologyRecoveryManager_->ParseKey(key, parsed);
    if (parseStatus.IsError()) {
        LOG(WARNING) << "CLUSTER_COORDINATOR_OBSERVER action=parse_failed key_size=" << key.size()
                     << " status=" << parseStatus.ToString();
        return;
    }
    if (parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP) {
        HandleCommittedMembershipMutation(key);
    }
    if (topologyControlHost_ != nullptr) {
        topologyControlHost_->NotifyStoreMutation(type, parsed);
    }
}

void CoordinatorServiceImpl::HandleCommittedMembershipMutation(const std::string &key)
{
    if (topologyRecoveryManager_ == nullptr || store_ == nullptr) {
        return;
    }
    ParsedTopologyCoordinationKey parsed;
    if (topologyRecoveryManager_->ParseKey(key, parsed).IsError()
        || parsed.kind != TopologyCoordinationKeyKind::MEMBERSHIP) {
        return;
    }
    std::lock_guard<std::mutex> lock(membershipWatchMutex_);
    std::vector<KeyValueEntry> current;
    int64_t revision = 0;
    auto rangeStatus = store_->Range(key, "", current, revision);
    if (rangeStatus.IsError()) {
        LOG(WARNING) << "CLUSTER_MEMBERSHIP_OBSERVER_READ_FAILED, key=" << key << ", status=" << rangeStatus.ToString();
        return;
    }
    const bool present = !current.empty();
    topologyRecoveryManager_->ObserveMembershipChange(key, present);
    if (!present && watchDispatcher_ != nullptr) {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        if (cluster::TopologyKeyHelper::Create(parsed.clusterName, keys).IsOk()) {
            const std::vector<std::string> scopes = { keys->TopologyTable(),   keys->MigrateTaskTable(),
                                                      keys->DeleteTaskTable(), keys->NotifyTable(),
                                                      keys->MembershipTable(), keys->ScaleInMetadataDoneTable() };
            watchDispatcher_->RemoveChannelsByWatcherInScopes(parsed.relativeKey, scopes);
        }
    }
}

void CoordinatorServiceImpl::ConfigureRpcService()
{
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = FLAGS_rpc_thread_num;
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
    cfg.udsEnabled_ = false;

    if (FLAGS_use_brpc) {
        brpcAddr_ = coordinatorAddr_.Host();
        brpcPort_ = coordinatorAddr_.Port() + kBrpcPortOffset;
        builder_.SetUseBrpc(true).SetBrpcAddr(brpcAddr_, brpcPort_);
        builder_.AddService(this, cfg);
    } else {
        builder_.AddEndPoint(RpcChannel::TcpipEndPoint(coordinatorAddr_));
        builder_.AddService(this, cfg);
    }
}

Status CoordinatorServiceImpl::FinishSuccessfulStart()
{
    const auto listenAddress = coordinatorAddr_.ToString();
    const char *transport = rpcServer_ != nullptr && rpcServer_->IsBrpc() ? "brpc" : "ZMQ";
    Status readyStatus = Status::OK();
    LOG(INFO) << "datasystem coordinator started at " << listenAddress << " (" << transport << ")";
    servingState_.store(ServingState::RUNNING, std::memory_order_release);
    return readyStatus;
}

Status CoordinatorServiceImpl::Start()
{
    std::unique_lock<std::mutex> lock(lifecycleMutex_);
    const auto state = servingState_.load(std::memory_order_acquire);
    if (state == ServingState::CREATED) {
        return Status(K_NOT_READY, "Coordinator service must be initialized before it can start");
    }
    if (state == ServingState::STOPPING || state == ServingState::STOPPED) {
        return Status(K_SHUTTING_DOWN, "Coordinator service cannot start after shutdown has started");
    }
    if (state != ServingState::INITIALIZED) {
        return Status(K_INVALID, "Coordinator service is already starting or running");
    }
    servingState_.store(ServingState::STARTING, std::memory_order_release);

    Status startStatus = StartInternal();
    if (startStatus.IsOk()) {
        return IsElectionConfigured() ? startStatus : FinishSuccessfulStart();
    }
    LOG_IF_ERROR(ShutdownInternal(lock), "Coordinator cleanup after startup failure also failed");
    return startStatus;
}

Status CoordinatorServiceImpl::StartInternal()
{
    const auto configurationStatus = ValidateElectionConfiguration();
    if (configurationStatus.IsError()) {
        return configurationStatus;
    }
    if (IsElectionConfigured() && !FLAGS_use_brpc) {
        return Status(K_INVALID, "Coordinator election requires use_brpc=true; ZMQ election startup is unsupported");
    }
    CHECK_FAIL_RETURN_STATUS(!IsElectionConfigured() || raftFlags_.localAddress == coordinatorAddr_.ToString(),
                             K_INVALID,
                             "Coordinator Raft snapshot localAddress must match the Coordinator service address");

    auto status = builder_.Init(rpcServer_);
    if (status.IsError()) {
        return status;
    }
    status = builder_.BuildAndStart(rpcServer_);
    if (status.IsError()) {
        return status;
    }

    if (FLAGS_use_brpc && rpcServer_->IsBrpc()) {
        brpcAdapter_ = std::make_unique<CoordinatorServiceBrpcAdapter>(*this);
        status = rpcServer_->AddBrpcService(brpcAdapter_.get());
        if (status.IsError()) {
            return status;
        }

        if (IsElectionConfigured()) {
            status = RegisterCoordinatorRaftServices(*rpcServer_, raftFlags_.localAddress);
            if (status.IsError()) {
                return status;
            }
        }

        status = rpcServer_->StartBrpcServer(brpcAddr_, brpcPort_);
        if (status.IsError()) {
            return status;
        }
    } else if (IsElectionConfigured()) {
        return Status(K_INVALID, "Coordinator election requires a brpc-configured RPC server");
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::StartElectionManager()
{
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        const auto state = servingState_.load(std::memory_order_acquire);
        if (state == ServingState::STOPPING || state == ServingState::STOPPED) {
            return Status(K_SHUTTING_DOWN, "Coordinator election manager cannot start after shutdown has started");
        }
        if (!IsElectionConfigured()) {
            return Status::OK();
        }
        if (state == ServingState::CREATED || state == ServingState::INITIALIZED) {
            return Status(K_NOT_READY, "Coordinator RPC services must be started before the election manager");
        }
        if (state != ServingState::STARTING) {
            return Status(K_INVALID, "Coordinator election manager is already running");
        }
        if (electionStartAttempted_ || electionStartInProgress_) {
            return Status(K_INVALID, "Coordinator election manager startup can only be attempted once");
        }
        electionStartAttempted_ = true;
        electionStartInProgress_ = true;
    }

    Status startStatus;
    CoordinatorElectionOptions electionOptions;
    startStatus = BuildElectionStartupContext(electionOptions);
    if (startStatus.IsOk()) {
        auto electionManager = std::make_unique<CoordinatorElectionManager>(
            std::move(electionOptions), BuildRaftEventCallbacks(), coordinatorDiscovery_);
        auto *managerView = electionManager.get();
        {
            std::lock_guard<std::mutex> lock(lifecycleMutex_);
            electionManager_ = std::move(electionManager);
        }
#ifdef WITH_TESTS
        if (electionManagerPublishedHook_) {
            electionManagerPublishedHook_();
        }
#endif
        startStatus = managerView->Start();
    }

    if (startStatus.IsOk()) {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        startStatus = FinishSuccessfulStart();
    }

    if (startStatus.IsError()) {
        raftServing_.store(false, std::memory_order_release);
        std::unique_ptr<CoordinatorElectionManager> failedManager;
        {
            std::lock_guard<std::mutex> lock(lifecycleMutex_);
            failedManager = std::move(electionManager_);
        }
        const auto cleanupStatus = ShutdownElectionManager(std::move(failedManager));
        if (cleanupStatus.IsError()) {
            LOG(ERROR) << "Coordinator election manager cleanup after startup failure also failed, status="
                       << cleanupStatus.ToString();
        }
    }

    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        electionStartInProgress_ = false;
    }
    lifecycleCv_.notify_all();
    return startStatus;
}

bool CoordinatorServiceImpl::IsLeader() const
{
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    return servingState_.load(std::memory_order_acquire) == ServingState::RUNNING && electionManager_ != nullptr
           && electionManager_->IsLeader();
}

Status CoordinatorServiceImpl::GetLeader(std::string &leaderAddress) const
{
    leaderAddress.clear();
    std::lock_guard<std::mutex> lock(lifecycleMutex_);
    const auto state = servingState_.load(std::memory_order_acquire);
    if (state == ServingState::STOPPING || state == ServingState::STOPPED) {
        return Status(K_SHUTTING_DOWN, "Coordinator service cannot report a leader during shutdown");
    }
    if (!IsElectionConfigured()) {
        return Status(K_INVALID, "Coordinator election is disabled");
    }
    if (state != ServingState::RUNNING || electionManager_ == nullptr) {
        return Status(K_NOT_READY, "Coordinator election manager is not running");
    }
    return electionManager_->GetLeader(leaderAddress);
}

Status CoordinatorServiceImpl::Shutdown()
{
    raftServing_.store(false, std::memory_order_release);
    std::unique_ptr<CoordinatorElectionManager> electionManager;
    {
        std::unique_lock<std::mutex> lock(lifecycleMutex_);
        lifecycleCv_.wait(lock, [this] { return !electionStartInProgress_; });
        if (shutdownComplete_) {
            return shutdownStatus_;
        }
        if (shutdownInProgress_) {
            lifecycleCv_.wait(lock, [this] { return shutdownComplete_; });
            return shutdownStatus_;
        }
        if (servingState_.load(std::memory_order_acquire) == ServingState::STOPPED) {
            return Status::OK();
        }

        shutdownInProgress_ = true;
        servingState_.store(ServingState::STOPPING, std::memory_order_release);
        raftServing_.store(false, std::memory_order_release);
        electionManager = std::move(electionManager_);
    }

    auto firstError = ShutdownElectionManager(std::move(electionManager));
    auto result = ShutdownRemainingComponents(std::move(firstError));
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        servingState_.store(ServingState::STOPPED, std::memory_order_release);
        shutdownStatus_ = result;
        shutdownComplete_ = true;
        shutdownInProgress_ = false;
    }
    lifecycleCv_.notify_all();
    return result;
}

Status CoordinatorServiceImpl::ShutdownElectionManager(std::unique_ptr<CoordinatorElectionManager> electionManager)
{
    Status firstError = Status::OK();
    const auto preserveFirstError = [&firstError](const Status &status) {
        if (firstError.IsOk() && status.IsError()) {
            firstError = status;
        }
    };

    LOG(INFO) << "Coordinator process executing a shutdown.";

    if (electionManager != nullptr) {
#ifdef WITH_TESTS
        if (electionManagerShutdownHook_) {
            preserveFirstError(electionManagerShutdownHook_());
        }
#endif
        preserveFirstError(electionManager->Shutdown());
        electionManager.reset();
    }
    return firstError;
}

Status CoordinatorServiceImpl::ShutdownRemainingComponents(Status firstError)
{
    const auto preserveFirstError = [&firstError](const Status &status) {
        if (firstError.IsOk() && status.IsError()) {
            firstError = status;
        }
    };

    if (rpcServer_ != nullptr) {
#ifdef WITH_TESTS
        if (rpcServerShutdownHook_) {
            rpcServerShutdownHook_();
        }
#endif
        rpcServer_->Shutdown();
        rpcServer_.reset();
    }
    if (store_ != nullptr) {
        store_->StopTtl();
    }
    brpcAdapter_.reset();
    if (topologyRecoveryManager_ != nullptr) {
        const auto recoveryStatus = topologyRecoveryManager_->Shutdown();
        preserveFirstError(recoveryStatus);
        if (recoveryStatus.IsError()) {
            LOG(ERROR) << "CLUSTER_RECOVERY_MANAGER_SHUTDOWN_FAILED, status=" << recoveryStatus.ToString();
        }
    }
    if (topologyControlHost_ != nullptr) {
        auto status =
            topologyControlHost_->Shutdown(std::chrono::steady_clock::now() + COORDINATOR_TOPOLOGY_SHUTDOWN_GRACE);
        if (status.IsError()) {
            LOG(ERROR) << "CLUSTER_CONTROL_HOST state=shutdown_incomplete action=retain_dependencies status="
                       << status.ToString();
            return status;
        }
    }
    if (store_ != nullptr) {
        store_->SetCommittedMutationObserver({});
    }
    topologyControlHost_.reset();
    topologyRecoveryManager_.reset();
    if (store_ != nullptr) {
        store_->Shutdown();
    }
    store_.reset();
    ttlManager_.reset();
    clock_.reset();
    watchDispatcher_.reset();
    watchRegistry_.reset();
    memStore_.reset();
    coordinatorId_.clear();

    LOG(INFO) << "Coordinator shutdown finished, status=" << firstError.ToString();
    return firstError;
}

Status CoordinatorServiceImpl::ShutdownInternal(std::unique_lock<std::mutex> &lifecycleLock)
{
    raftServing_.store(false, std::memory_order_release);
    if (servingState_.load(std::memory_order_acquire) == ServingState::STOPPED) {
        return Status::OK();
    }

    shutdownInProgress_ = true;
    servingState_.store(ServingState::STOPPING, std::memory_order_release);
    auto electionManager = std::move(electionManager_);
    lifecycleLock.unlock();

    auto firstError = ShutdownElectionManager(std::move(electionManager));
    auto result = ShutdownRemainingComponents(std::move(firstError));

    lifecycleLock.lock();
    servingState_.store(ServingState::STOPPED, std::memory_order_release);
    shutdownStatus_ = result;
    shutdownComplete_ = true;
    shutdownInProgress_ = false;
    lifecycleCv_.notify_all();
    return result;
}

Status CoordinatorServiceImpl::Put(const PutReqPb &req, PutRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id().empty() || req.expected_coordinator_id() == coordinatorId_,
                             K_TRY_AGAIN, "Put CoordinatorId fence no longer matches this process");
    std::string clusterName;
    bool reserved = false;
    RETURN_IF_NOT_OK(PrepareTopologyMembershipPut(req.key(), clusterName, reserved));
    int64_t version = 0;
    int64_t revision = 0;
    Raii reservationCompletion([this, &clusterName, &reserved, &version, &revision] {
        if (reserved && topologyControlHost_ != nullptr) {
            topologyControlHost_->CompleteMembershipPut(clusterName, version > 0 && revision > 0);
        }
    });
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->CheckMutationAllowed(req.key(), ""));

    RETURN_IF_NOT_OK(store_->Put(req.key(), req.value(), req.ttl(), req.expected_version(), version, revision,
                                 req.expected_mod_revision()));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_version(version);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::PrepareTopologyMembershipPut(const std::string &key, std::string &clusterName,
                                                            bool &reserved)
{
    reserved = false;
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr && topologyControlHost_ != nullptr, K_NOT_READY,
                             "topology control components are not bound");
    ParsedTopologyCoordinationKey parsed;
    RETURN_IF_NOT_OK(topologyRecoveryManager_->ParseKey(key, parsed));
    if (parsed.kind != TopologyCoordinationKeyKind::MEMBERSHIP) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(topologyControlHost_->PrepareMembershipPut(parsed.clusterName));
    clusterName = std::move(parsed.clusterName);
    reserved = true;
    return Status::OK();
}

Status CoordinatorServiceImpl::Range(const RangeReqPb &req, RangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->CheckReadAllowed(req.key(), req.range_end()));

    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Range(req.key(), req.range_end(), kvs, revision));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_revision(revision);
    for (const auto &entry : kvs) {
        FillKeyValuePb(entry, rsp.add_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id().empty() || req.expected_coordinator_id() == coordinatorId_,
                             K_TRY_AGAIN, "DeleteRange CoordinatorId fence no longer matches this process");
    CHECK_FAIL_RETURN_STATUS(
        req.expected_mod_revision() == COORDINATOR_NO_MOD_REVISION_CHECK || req.range_end().empty(), K_INVALID,
        "DeleteRange modification revision fence only supports an exact key");
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->CheckMutationAllowed(req.key(), req.range_end()));

    int64_t deleted = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->DeleteRange(req.key(), req.range_end(), deleted, revision, req.expected_mod_revision()));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_deleted(deleted);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(!req.registration_id().empty(), K_INVALID, "watch registration ID is empty");
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->ValidateWatchRange(req.key(), req.range_end()));
    std::lock_guard<std::mutex> lock(membershipWatchMutex_);
    RETURN_IF_NOT_OK(CheckWatcherMembership(req));

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initialKvs;
    RETURN_IF_NOT_OK(
        store_->WatchRange(req.key(), req.range_end(), req.watcher_addr(), req.registration_id(), watchId, initialKvs));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_watch_id(watchId);
    for (const auto &entry : initialKvs) {
        FillKeyValuePb(entry, rsp.add_initial_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::CheckWatcherMembership(const WatchRangeReqPb &req)
{
    ParsedTopologyCoordinationKey parsed;
    RETURN_IF_NOT_OK(topologyRecoveryManager_->ParseKey(req.key(), parsed));
    if (parsed.kind == TopologyCoordinationKeyKind::OTHER) {
        return Status::OK();
    }
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(parsed.clusterName, keys));
    std::string memberKey;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(req.watcher_addr(), memberKey));
    const std::string physicalKey = keys->MembershipTable() + "/" + memberKey;
    std::vector<KeyValueEntry> members;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Range(physicalKey, "", members, revision));
    CHECK_FAIL_RETURN_STATUS(!members.empty(), K_NOT_FOUND, "watcher membership no longer exists");
    return Status::OK();
}

Status CoordinatorServiceImpl::CancelWatch(const CancelWatchReqPb &req, CancelWatchRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id() == coordinatorId_, K_TRY_AGAIN,
                             "CancelWatch CoordinatorId no longer owns these watch IDs");

    std::vector<int64_t> watchIds(req.watch_ids().begin(), req.watch_ids().end());
    RETURN_IF_NOT_OK(store_->CancelWatch(req.watcher_addr(), watchIds));
    FillResponseHeader(rsp.mutable_header());
    return Status::OK();
}

Status CoordinatorServiceImpl::KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id().empty() || req.expected_coordinator_id() == coordinatorId_,
                             K_TRY_AGAIN, "KeepAlive CoordinatorId fence no longer matches this process");

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    RETURN_IF_NOT_OK(store_->KeepAlive(req.key(), ttlMs, remainingTtlMs, req.expected_mod_revision()));
    if (topologyRecoveryManager_ != nullptr) {
        topologyRecoveryManager_->NotifyMembershipActivity(req.key());
    }
    FillResponseHeader(rsp.mutable_header());
    rsp.set_ttl(ttlMs);
    rsp.set_remaining_ttl(remainingTtlMs);
    return Status::OK();
}

Status CoordinatorServiceImpl::GetCoordinatorId(const GetCoordinatorIdReqPb &req, GetCoordinatorIdRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    (void)req;
    CHECK_FAIL_RETURN_STATUS(coordinatorId_.size() == UUID_SIZE, K_NOT_READY, "CoordinatorId is not initialized");
    FillResponseHeader(rsp.mutable_header());
    return Status::OK();
}

Status CoordinatorServiceImpl::GetRaftBootstrapState(const GetRaftBootstrapStateReqPb &req,
                                                     GetRaftBootstrapStateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(req.group_id() == kCoordinatorRaftGroupId, K_INVALID,
                             "Coordinator bootstrap request has the wrong Raft group id");
#ifdef WITH_TESTS
    if (raftBootstrapHandlerEnteredHook_) {
        raftBootstrapHandlerEnteredHook_();
    }
#endif

    RaftBootstrapState bootstrapState;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        const auto lifecycleState = servingState_.load(std::memory_order_acquire);
        if (lifecycleState == ServingState::STOPPING || lifecycleState == ServingState::STOPPED) {
            return Status(K_SHUTTING_DOWN, "Coordinator bootstrap state is unavailable during shutdown");
        }
    }
    CHECK_FAIL_RETURN_STATUS(IsElectionConfigured(), K_INVALID, "Coordinator election is disabled");
    CHECK_FAIL_RETURN_STATUS(electionManager_ != nullptr, K_NOT_READY,
                             "Coordinator election manager is not published");
    RETURN_IF_NOT_OK(electionManager_->GetBootstrapState(bootstrapState));
#ifdef WITH_TESTS
    if (raftBootstrapSnapshotCopiedHook_) {
        raftBootstrapSnapshotCopiedHook_();
    }
#endif

    GetRaftBootstrapStateRspPb localRsp;
    localRsp.set_probe_ready(bootstrapState.probeReady);
    localRsp.set_group_id(bootstrapState.groupId);
    localRsp.set_local_peer(bootstrapState.localPeer);
    localRsp.set_expected_member_count(static_cast<uint64_t>(bootstrapState.expectedMemberCount));
    localRsp.set_metadata_state(ToPbRaftMetadataState(bootstrapState.metadataState));
    localRsp.set_candidate_count(static_cast<uint64_t>(bootstrapState.candidateCount));
    localRsp.set_candidate_digest(bootstrapState.candidateDigest);
    localRsp.set_phase(ToPbRaftBootstrapPhase(bootstrapState.phase));
    localRsp.set_status_code(bootstrapState.statusCode);
    for (const auto &peer : bootstrapState.committedPeers) {
        localRsp.add_committed_peers(peer);
    }
    rsp = std::move(localRsp);
    return Status::OK();
}

Status CoordinatorServiceImpl::ReportTopologyRecoveryCandidate(const ReportTopologyRecoveryCandidateReqPb &req,
                                                               ReportTopologyRecoveryCandidateRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    CHECK_FAIL_RETURN_STATUS(
        req.result() == TOPOLOGY_RECOVERY_NO_SNAPSHOT || req.result() == TOPOLOGY_RECOVERY_SNAPSHOT, K_INVALID,
        "invalid topology recovery report result");
    CHECK_FAIL_RETURN_STATUS(req.canonical_topology().size() <= MAX_TOPOLOGY_RECOVERY_PAYLOAD_BYTES, K_INVALID,
                             "candidate topology payload exceeds limit");
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = req.reporter_address();
    report.hasSnapshot = req.result() == TOPOLOGY_RECOVERY_SNAPSHOT;
    report.topologyVersion = req.topology_version();
    report.canonicalDigest = req.topology_digest();
    report.canonicalTopology = req.canonical_topology();
    TopologyRecoveryReportDecision decision;
    RETURN_IF_NOT_OK(topologyRecoveryManager_->ReportCandidate(req.cluster_name(), req.coordinator_id(),
                                                               std::move(report), decision));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_result(ToPbReportResult(decision.result));
    rsp.set_recovery_state(ToPbRecoveryState(decision.state));
    rsp.set_payload_required(decision.payloadRequired);
    return Status::OK();
}

Status CoordinatorServiceImpl::GetClusterRawSnapshot(const GetClusterRawSnapshotReqPb &req,
                                                     GetClusterRawSnapshotRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckServing());
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    std::string topologyKey;
    std::string membershipKey;
    std::string membershipEnd;
    RETURN_IF_NOT_OK(BuildClusterReadKeys(req.cluster_name(), topologyKey, membershipKey, membershipEnd));
    GetClusterRawSnapshotRspPb localRsp;
    std::vector<KeyValueEntry> topologyKvs;
    std::vector<KeyValueEntry> membershipKvs;
    int64_t ignoredRevision = 0;
    // Diagnostics intentionally bypass recovery gating so operators can inspect the raw facts used during recovery.
    // This endpoint remains read-only and does not project health, hash ranges, or routes on the Coordinator.
    RETURN_IF_NOT_OK(store_->Range(topologyKey, "", topologyKvs, ignoredRevision));
    RETURN_IF_NOT_OK(store_->Range(membershipKey, membershipEnd, membershipKvs, ignoredRevision));
    CHECK_FAIL_RETURN_STATUS(membershipKvs.size() <= MAX_CLUSTER_RAW_MEMBERSHIPS, K_OUT_OF_RANGE,
                             "raw cluster membership count exceeds limit");
    FillKeyValuePbs(topologyKvs, localRsp.mutable_topology_kvs());
    FillKeyValuePbs(membershipKvs, localRsp.mutable_membership_kvs());
    FillResponseHeader(localRsp.mutable_header());
    CHECK_FAIL_RETURN_STATUS(localRsp.ByteSizeLong() <= MAX_CLUSTER_RAW_SNAPSHOT_BYTES, K_OUT_OF_RANGE,
                             "raw cluster snapshot exceeds response limit");
    rsp = std::move(localRsp);
    return Status::OK();
}

void CoordinatorServiceImpl::FillResponseHeader(ResponseHeader *header) const
{
    if (header == nullptr) {
        return;
    }
    header->clear_leader_address();
    if (!IsElectionConfigured()) {
        header->set_is_leader(true);
    } else {
        const bool leader = IsLeader();
        header->set_is_leader(leader);
        if (!leader) {
            std::string leaderAddress;
            const auto leaderStatus = GetLeader(leaderAddress);
            if (leaderStatus.IsOk()) {
                header->set_leader_address(std::move(leaderAddress));
            }
        }
    }
    header->set_coordinator_id(coordinatorId_);
}
}  // namespace coordinator
}  // namespace datasystem
