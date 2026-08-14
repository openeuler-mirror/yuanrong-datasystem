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
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_diagnostics.h"
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
DS_DECLARE_uint32(node_timeout_s);
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

Status ReadMembershipRecord(CoordinatorStore &store, const std::string &physicalKey, const std::string &address,
                            cluster::MembershipRecord &record)
{
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store.Range(physicalKey, "", entries, revision));
    CHECK_FAIL_RETURN_STATUS(entries.size() == 1, K_NOT_FOUND, "membership key is absent");
    cluster::MembershipValue value;
    RETURN_IF_NOT_OK(cluster::MembershipValueCodec::Decode(entries.front().value, value));
    record = cluster::MembershipRecord{ address, value.lifecycleState, value.timestamp, value.hostId };
    return Status::OK();
}

bool BuildEnsureMembershipPhysicalKey(const EnsureLeaderMembershipReqPb &req, std::string &physicalKey)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    HostPort reporter;
    cluster::MembershipValue membership;
    if (req.ttl_ms() <= 0 || reporter.ParseString(req.reporter_address()).IsError()
        || reporter.ToString() != req.reporter_address()
        || cluster::TopologyKeyHelper::Create(req.cluster_name(), keys).IsError()
        || cluster::MembershipValueCodec::Decode(req.membership_value(), membership).IsError()) {
        return false;
    }
    std::string membershipKey;
    if (cluster::TopologyKeyHelper::MembershipKey(req.reporter_address(), membershipKey).IsError()) {
        return false;
    }
    physicalKey = keys->MembershipTable() + "/" + membershipKey;
    return true;
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
        case TopologyRecoveryReportResult::STALE_LEADER_TERM:
            return ReportTopologyRecoveryCandidateRspPb::STALE_LEADER_TERM;
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
        CoordinatorMembershipOptions{ expectedMemberCount_, std::chrono::milliseconds(raftFlags_.healthCheckIntervalMs),
                                      std::chrono::milliseconds(raftFlags_.memberFailureGraceMs),
                                      std::chrono::milliseconds(raftFlags_.discoveryRetryIntervalMs) };
    return Status::OK();
}

CoordinatorRaftEventCallbacks CoordinatorServiceImpl::BuildRaftEventCallbacks()
{
    CoordinatorRaftEventCallbacks callbacks;
    // The Service exclusively owns and synchronously drains the Manager/Node before its own destruction, so this raw
    // capture cannot outlive the atomic gate and does not create a shared-ownership cycle.
    callbacks.onLeaderStart = [this](int64_t term) { OnLeaderStart(term); };
    callbacks.onLeaderStop = [this](Status status) { OnLeaderStop(status); };
    callbacks.onError = [this](Status status) { OnLeaderStop(status); };
    callbacks.onShutdown = [this] { OnLeaderStop(Status(K_SHUTTING_DOWN, "Raft node shut down")); };
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
        case ServingState::FOLLOWER_SERVING:
            return Status(K_NOT_READY, "Coordinator is not the active Leader");
        case ServingState::LEADER_RECOVERING:
            return Status(K_NOT_READY, "Coordinator Leader is recovering topology state");
        case ServingState::LEADER_SERVING: {
            // Raft can revoke local leadership before its stop callback closes the service state gate.
            std::lock_guard<std::mutex> lock(lifecycleMutex_);
            if (IsElectionConfigured() && (electionManager_ == nullptr || !electionManager_->IsLeader())) {
                return Status(K_NOT_READY, "Coordinator is not the active Leader");
            }
            return Status::OK();
        }
        case ServingState::STOPPING:
            return Status(K_SHUTTING_DOWN, "Coordinator service is shutting down");
        case ServingState::STOPPED:
            return Status(K_SHUTTING_DOWN, "Coordinator service is stopped");
    }
    return Status(K_NOT_READY, "Coordinator service is in an unknown serving state");
}

Status CoordinatorServiceImpl::PrepareRpcResponse(bool allowLeaderRecovering, ResponseHeader *header,
                                                  bool &businessAllowed) const
{
    FillResponseHeader(header);
    const auto servingStatus = CheckServing();
    businessAllowed = servingStatus.IsOk();
    if (businessAllowed) {
        return Status::OK();
    }

    // A recovering Leader is the recovery-control route. A Follower is routable only with an explicit redirect.
    // No known Leader and lifecycle states preserve their original failure status.
    const auto state = servingState_.load(std::memory_order_acquire);
    if (IsElectionConfigured() && state == ServingState::LEADER_RECOVERING) {
        if (allowLeaderRecovering) {
            businessAllowed = true;
        }
        return Status::OK();
    }
    if (IsElectionConfigured() && state == ServingState::FOLLOWER_SERVING && header != nullptr
        && !header->leader_address().empty()) {
        return Status::OK();
    }
    return servingStatus;
}

Status CoordinatorServiceImpl::RequireRecoveryLeader(uint64_t term, std::string_view coordinatorId) const
{
    CHECK_FAIL_RETURN_STATUS(coordinatorId == coordinatorId_, K_TRY_AGAIN,
                             "Coordinator recovery request has a stale process identity");
    if (!IsElectionConfigured()) {
        return Status::OK();
    }
    const auto state = servingState_.load(std::memory_order_acquire);
    CHECK_FAIL_RETURN_STATUS(state == ServingState::LEADER_RECOVERING || state == ServingState::LEADER_SERVING,
                             K_NOT_READY, "Coordinator is not the active Leader");
    CHECK_FAIL_RETURN_STATUS(leaderTerm_.load(std::memory_order_acquire) == term, K_TRY_AGAIN,
                             "Coordinator recovery request has a stale term");
    return Status::OK();
}

bool CoordinatorServiceImpl::IsCurrentLeaderRound(uint64_t term, std::string_view coordinatorId) const
{
    if (!IsElectionConfigured()) {
        return term == 0 && coordinatorId == coordinatorId_;
    }
    const auto state = servingState_.load(std::memory_order_acquire);
    return coordinatorId == coordinatorId_ && leaderTerm_.load(std::memory_order_acquire) == term
           && (state == ServingState::LEADER_RECOVERING || state == ServingState::LEADER_SERVING);
}

void CoordinatorServiceImpl::OnLeaderStart(int64_t term)
{
    if (term <= 0) {
        return;
    }
    const auto leaderTerm = static_cast<uint64_t>(term);
    {
        std::unique_lock<std::shared_mutex> operationLock(leaderOperationMutex_);
        const auto state = servingState_.load(std::memory_order_acquire);
        if (state != ServingState::STARTING && state != ServingState::FOLLOWER_SERVING) {
            return;
        }
        leaderTerm_.store(leaderTerm, std::memory_order_release);
        servingState_.store(ServingState::LEADER_RECOVERING, std::memory_order_release);
        recoveryTraceId_ = Trace::Instance().GetTraceID();
        if (recoveryTraceId_.empty()) {
            recoveryTraceId_ = "CoordinatorRecovery;" + GetStringUuid();
        }
        if (topologyRecoveryManager_ != nullptr) {
            topologyRecoveryManager_->BeginLeaderRound({ leaderTerm, coordinatorId_ });
        }
    }

    // The recovery timeout bounds late reports; it must not delay a round with no pending recovery work.
    CompleteRecoveryWindow(leaderTerm);
    recoveryGateCv_.notify_all();
}

void CoordinatorServiceImpl::OnLeaderStop(const Status &status)
{
    std::unique_lock<std::shared_mutex> operationLock(leaderOperationMutex_);
    const uint64_t leaderTerm = leaderTerm_.exchange(0, std::memory_order_acq_rel);
    const auto state = servingState_.load(std::memory_order_acquire);
    if (IsElectionConfigured() && (state == ServingState::LEADER_RECOVERING || state == ServingState::LEADER_SERVING)) {
        servingState_.store(ServingState::FOLLOWER_SERVING, std::memory_order_release);
    }
    if (topologyRecoveryManager_ != nullptr && leaderTerm != 0) {
        topologyRecoveryManager_->EndLeaderRound({ leaderTerm, coordinatorId_ });
    }
    recoveryTraceId_.clear();
    recoveryGateCv_.notify_all();
    LOG(WARNING) << "CLUSTER_COORDINATOR_LEADER_STOP status=" << status.ToString();
}

void CoordinatorServiceImpl::RunRecoveryGate()
{
    std::unique_lock<std::mutex> lock(recoveryGateMutex_);
    while (!recoveryGateStopping_) {
        recoveryGateCv_.wait(lock, [this] {
            return recoveryGateStopping_
                   || servingState_.load(std::memory_order_acquire) == ServingState::LEADER_RECOVERING;
        });
        if (recoveryGateStopping_) {
            break;
        }
        const uint64_t term = leaderTerm_.load(std::memory_order_acquire);
        auto delay = std::chrono::seconds(FLAGS_node_dead_timeout_s);
        while (!recoveryGateStopping_
               && servingState_.load(std::memory_order_acquire) == ServingState::LEADER_RECOVERING
               && leaderTerm_.load(std::memory_order_acquire) == term) {
            if (recoveryGateCv_.wait_for(lock, delay, [this, term] {
                    return recoveryGateStopping_
                           || servingState_.load(std::memory_order_acquire) != ServingState::LEADER_RECOVERING
                           || leaderTerm_.load(std::memory_order_acquire) != term;
                })) {
                break;
            }
            lock.unlock();
            CompleteRecoveryWindow(term);
            lock.lock();
            delay = std::chrono::seconds(1);
        }
    }
}

void CoordinatorServiceImpl::StopRecoveryGate()
{
    {
        std::lock_guard<std::mutex> lock(recoveryGateMutex_);
        recoveryGateStopping_ = true;
    }
    recoveryGateCv_.notify_all();
    if (recoveryGateThread_.joinable()) {
        recoveryGateThread_.join();
    }
}

void CoordinatorServiceImpl::CompleteRecoveryWindow(uint64_t term)
{
    std::unique_lock<std::shared_mutex> operationLock(leaderOperationMutex_);
    TraceGuard traceGuard(TraceGuardType::INVALID);
    if (Trace::Instance().GetTraceID().empty()) {
        traceGuard = Trace::Instance().SetTraceNewID(recoveryTraceId_);
    }
#ifdef WITH_TESTS
    if (recoveryWindowTraceHook_) {
        recoveryWindowTraceHook_();
    }
#endif
    if (topologyRecoveryManager_ == nullptr) {
        return;
    }
    const auto summary = topologyRecoveryManager_->GetRoundSummary();
    if (servingState_.load(std::memory_order_acquire) != ServingState::LEADER_RECOVERING
        || leaderTerm_.load(std::memory_order_acquire) != term) {
        return;
    }
    if (summary.AllDiscoveredClustersReady()) {
        servingState_.store(ServingState::LEADER_SERVING, std::memory_order_release);
        LOG(INFO) << "CLUSTER_COORDINATOR_RECOVERY_COMPLETE term=" << term
                  << ", discovered_clusters=" << summary.contextCount;
    } else {
        const int logTimeLimit = 30;
        LOG_EVERY_T(ERROR, logTimeLimit)
            << "CLUSTER_COORDINATOR_RECOVERY_BLOCKED term=" << term << ", recovering=" << summary.recoveringCount
            << ", installing=" << summary.installingCount << ", blocked=" << summary.blockedCount;
    }
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
    servingState_.store(publishStarting ? ServingState::FOLLOWER_SERVING : ServingState::INITIALIZED,
                        std::memory_order_release);
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
    coordinatorId_ = GetBytesUuid();
    LOG(INFO) << "CLUSTER_COORDINATOR_ID role=coordinator id="
              << BytesUuidToString(coordinatorId_).substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE) << " state=created";
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_coordinator_rpc_stub_cache_size, coordinatorAddr_));
    RETURN_IF_NOT_OK(BuildComponentTree());
    try {
        recoveryGateThread_ = Thread(&CoordinatorServiceImpl::RunRecoveryGate, this);
        recoveryGateThread_.set_name("coord-recovery");
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start Coordinator recovery gate failed: ") + error.what());
    }
    ConfigureRpcService();
    return Status::OK();
}

void CoordinatorServiceImpl::ConfigureTopologyHostOptions(TopologyControlHost::Options &options) const
{
    options.maxClusters = FLAGS_coordinator_topology_max_active_clusters;
    options.activeFailureWindow = std::chrono::seconds(FLAGS_node_timeout_s);
    const uint32_t classifierAbsenceS = FLAGS_node_dead_timeout_s > FLAGS_node_timeout_s
                                            ? FLAGS_node_dead_timeout_s - FLAGS_node_timeout_s
                                            : 1U;
    options.controller.nodeDeadTimeout = std::chrono::seconds(classifierAbsenceS);
    options.controller.scaleInCollectWindow = std::chrono::milliseconds(FLAGS_scale_in_collect_window_ms);
    options.controller.eventSourceMode = cluster::TopologyEventSourceMode::EXTERNAL;
    options.controller.probeEpoch = coordinatorId_;
    options.controller.collectiveControlEpoch = [this]() { return GetCollectiveControlEpoch(); };
    options.controller.collectiveReplacementFence =
        [this](uint64_t expectedEpoch, const std::function<Status()> &mutation) {
            return RunUnderCollectiveReplacementFence(expectedEpoch, mutation);
        };
    options.controller.memberLivenessProbe =
        [this](const std::vector<cluster::MemberIdentity> &targets, std::chrono::steady_clock::time_point deadline) {
            return ProbeMembersLiveness(targets, deadline);
        };
}

std::optional<uint64_t> CoordinatorServiceImpl::GetCollectiveControlEpoch() const
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    const auto state = servingState_.load(std::memory_order_acquire);
    if (!IsElectionConfigured()) {
        return state == ServingState::STOPPING || state == ServingState::STOPPED ? std::nullopt
                                                                                 : std::optional<uint64_t>{ 1 };
    }
    if ((state != ServingState::LEADER_RECOVERING && state != ServingState::LEADER_SERVING) || !IsLeader()) {
        return std::nullopt;
    }
    const auto term = leaderTerm_.load(std::memory_order_acquire);
    return term == 0 ? std::nullopt : std::optional<uint64_t>{ term };
}

Status CoordinatorServiceImpl::RunUnderCollectiveReplacementFence(
    uint64_t expectedEpoch, const std::function<Status()> &mutation) const
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    const auto state = servingState_.load(std::memory_order_acquire);
    if (!IsElectionConfigured()) {
        CHECK_FAIL_RETURN_STATUS(
            expectedEpoch == 1 && state != ServingState::STOPPING && state != ServingState::STOPPED, K_NOT_READY,
            "Coordinator collective control epoch is stale");
        return mutation();
    }
    CHECK_FAIL_RETURN_STATUS(
        expectedEpoch != 0 && leaderTerm_.load(std::memory_order_acquire) == expectedEpoch
            && (state == ServingState::LEADER_RECOVERING || state == ServingState::LEADER_SERVING) && IsLeader(),
        K_NOT_READY, "Coordinator collective control term is stale");
    return mutation();
}

std::vector<cluster::ControlBackendProbeResult> CoordinatorServiceImpl::ProbeMembersLiveness(
    const std::vector<cluster::MemberIdentity> &targets, std::chrono::steady_clock::time_point deadline) const
{
    std::vector<cluster::ControlBackendProbeResult> results;
    results.reserve(targets.size());
    for (const auto &target : targets) {
        results.push_back(
            { target, std::nullopt, cluster::ControlBackendProbeOutcome::CANCELLED, std::chrono::milliseconds(0) });
    }
    const auto expectedEpoch = GetCollectiveControlEpoch();
    if (!expectedEpoch.has_value()) {
        return results;
    }
    static_cast<void>(RunUnderCollectiveReplacementFence(*expectedEpoch, [&] {
        for (size_t index = 0; index < targets.size(); ++index) {
            const auto &target = targets[index];
            const auto startedAt = std::chrono::steady_clock::now();
            const auto probe = watchDispatcher_->ProbeWorkerReachable(target.address, deadline);
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startedAt);
            auto outcome = cluster::ControlBackendProbeOutcome::CANCELLED;
            if (probe.rpcDispatched) {
                outcome = cluster::ControlBackendProbeOutcome::ERROR;
                if (probe.status.IsOk()) {
                    outcome = cluster::ControlBackendProbeOutcome::RESPONSE;
                } else if (probe.status.GetCode() == K_RPC_DEADLINE_EXCEEDED) {
                    outcome = cluster::ControlBackendProbeOutcome::DEADLINE_EXCEEDED;
                } else if (probe.status.GetCode() == K_RPC_UNAVAILABLE || probe.status.GetCode() == K_RPC_PEER_DEAD) {
                    outcome = cluster::ControlBackendProbeOutcome::UNAVAILABLE;
                } else if (probe.status.GetCode() == K_RPC_CANCELLED) {
                    outcome = cluster::ControlBackendProbeOutcome::CANCELLED;
                }
            }
            results[index] = { target, std::nullopt, outcome, elapsed };
        }
        return Status::OK();
    }));
    return results;
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
    ConfigureTopologyHostOptions(hostOptions);
    topologyControlHost_ =
        std::make_unique<TopologyControlHost>(coordinatorId_, *store_, *topologyRecoveryManager_, hostOptions);
    RETURN_IF_NOT_OK(topologyControlHost_->Start());
    topologyRecoveryManager_->SetLeaderRoundFence(
        &leaderOperationMutex_, [this](const TopologyRecoveryRoundIdentity &identity) {
            return IsCurrentLeaderRound(identity.leaderTerm, identity.coordinatorId);
        });
    if (!IsElectionConfigured()) {
        topologyRecoveryManager_->BeginLeaderRound({ 0, coordinatorId_ });
    }
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
            const std::vector<std::string> scopes = {
                keys->TopologyTable(), keys->MigrateTaskTable(), keys->DeleteTaskTable(),         keys->NotifyTable(),
                keys->ProbeTable(),    keys->MembershipTable(),  keys->ScaleInMetadataDoneTable()
            };
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
        brpcPort_ = coordinatorAddr_.Port();
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
    if (IsElectionConfigured()) {
        auto expected = ServingState::STARTING;
        servingState_.compare_exchange_strong(expected, ServingState::FOLLOWER_SERVING, std::memory_order_acq_rel);
    } else {
        servingState_.store(ServingState::LEADER_SERVING, std::memory_order_release);
    }
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
    const auto state = servingState_.load(std::memory_order_acquire);
    return (state == ServingState::LEADER_RECOVERING || state == ServingState::LEADER_SERVING)
           && electionManager_ != nullptr && electionManager_->IsLeader();
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
    if ((state != ServingState::FOLLOWER_SERVING && state != ServingState::LEADER_RECOVERING
         && state != ServingState::LEADER_SERVING)
        || electionManager_ == nullptr) {
        return Status(K_NOT_READY, "Coordinator election manager is not running");
    }
    return electionManager_->GetLeader(leaderAddress);
}

Status CoordinatorServiceImpl::Shutdown()
{
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
        electionManager = std::move(electionManager_);
    }

    OnLeaderStop(Status(K_SHUTTING_DOWN, "Coordinator service is shutting down"));
    StopRecoveryGate();
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
    lifecycleLock.unlock();
    OnLeaderStop(Status(K_SHUTTING_DOWN, "Coordinator service is shutting down"));
    StopRecoveryGate();
    lifecycleLock.lock();
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
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
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
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->CheckReadAllowed(req.key(), req.range_end()));

    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Range(req.key(), req.range_end(), kvs, revision));
    rsp.set_revision(revision);
    for (const auto &entry : kvs) {
        FillKeyValuePb(entry, rsp.add_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp)
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    ParsedTopologyCoordinationKey parsed;
    const bool isMembershipRollback = servingState_.load(std::memory_order_acquire) == ServingState::LEADER_RECOVERING
                                      && req.range_end().empty() && !req.expected_coordinator_id().empty()
                                      && req.expected_mod_revision() != COORDINATOR_NO_MOD_REVISION_CHECK
                                      && topologyRecoveryManager_ != nullptr
                                      && topologyRecoveryManager_->ParseKey(req.key(), parsed).IsOk()
                                      && parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP
                                      && !parsed.relativeKey.empty();
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(isMembershipRollback, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
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
    if (isMembershipRollback) {
        const uint64_t leaderTerm = leaderTerm_.load(std::memory_order_acquire);
        leaderLock.unlock();
        CompleteRecoveryWindow(leaderTerm);
        FillResponseHeader(rsp.mutable_header());
    }
    rsp.set_deleted(deleted);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp)
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
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
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id() == coordinatorId_, K_TRY_AGAIN,
                             "CancelWatch CoordinatorId no longer owns these watch IDs");

    std::vector<int64_t> watchIds(req.watch_ids().begin(), req.watch_ids().end());
    RETURN_IF_NOT_OK(store_->CancelWatch(req.watcher_addr(), watchIds));
    return Status::OK();
}

Status CoordinatorServiceImpl::KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp)
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(true, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id().empty() || req.expected_coordinator_id() == coordinatorId_,
                             K_TRY_AGAIN, "KeepAlive CoordinatorId fence no longer matches this process");

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    RETURN_IF_NOT_OK(store_->KeepAlive(req.key(), ttlMs, remainingTtlMs, req.expected_mod_revision()));
    if (topologyRecoveryManager_ != nullptr) {
        topologyRecoveryManager_->NotifyMembershipActivity(req.key());
    }
    if (topologyRecoveryManager_ != nullptr && topologyControlHost_ != nullptr && req.failed_targets_size() > 0) {
        ParsedTopologyCoordinationKey parsed;
        RETURN_IF_NOT_OK(topologyRecoveryManager_->ParseKey(req.key(), parsed));
        if (parsed.kind == TopologyCoordinationKeyKind::MEMBERSHIP) {
            cluster::MembershipRecord reporter;
            auto reporterRc = ReadMembershipRecord(*store_, req.key(), parsed.relativeKey, reporter);
            if (reporterRc.IsError()) {
                LOG(WARNING) << "Skip worker failure summaries because reporter membership is unavailable, key="
                             << req.key() << ", rc=" << reporterRc.ToString();
                FillResponseHeader(rsp.mutable_header());
                rsp.set_ttl(ttlMs);
                rsp.set_remaining_ttl(remainingTtlMs);
                return Status::OK();
            }
            std::unique_ptr<cluster::TopologyKeyHelper> keys;
            RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(parsed.clusterName, keys));
            std::vector<cluster::MembershipRecord> failedTargets;
            failedTargets.reserve(req.failed_targets_size());
            for (const auto &target : req.failed_targets()) {
                std::string targetMembershipKey;
                if (cluster::TopologyKeyHelper::MembershipKey(target, targetMembershipKey).IsError()) {
                    continue;
                }
                cluster::MembershipRecord targetRecord;
                if (ReadMembershipRecord(*store_, keys->MembershipTable() + "/" + targetMembershipKey, target,
                                         targetRecord).IsOk()) {
                    failedTargets.emplace_back(std::move(targetRecord));
                } else {
                    failedTargets.push_back({ target, cluster::MemberLifecycleState::READY, -1, "" });
                }
            }
            topologyControlHost_->RecordWorkerFailureSummaries(parsed.clusterName, reporter, failedTargets);
        }
    }
    FillResponseHeader(rsp.mutable_header());
    rsp.set_ttl(ttlMs);
    rsp.set_remaining_ttl(remainingTtlMs);
    return Status::OK();
}

Status CoordinatorServiceImpl::GetCoordinatorId(const GetCoordinatorIdReqPb &req, GetCoordinatorIdRspPb &rsp)
{
    (void)req;
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
    CHECK_FAIL_RETURN_STATUS(coordinatorId_.size() == UUID_SIZE, K_NOT_READY, "CoordinatorId is not initialized");
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
    CHECK_FAIL_RETURN_STATUS(electionManager_ != nullptr, K_NOT_READY, "Coordinator election manager is not published");
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
    // Reject oversized wire payloads before identity and leadership handling so every request path is bounded.
    CHECK_FAIL_RETURN_STATUS(req.canonical_topology().size() <= MAX_TOPOLOGY_RECOVERY_PAYLOAD_BYTES, K_INVALID,
                             "candidate topology payload exceeds limit");
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    FillResponseHeader(rsp.mutable_header());
    if (req.coordinator_id() != coordinatorId_) {
        rsp.set_result(ReportTopologyRecoveryCandidateRspPb::COORDINATOR_ID_MISMATCH);
        return Status::OK();
    }
    if (RequireRecoveryLeader(req.leader_term(), req.coordinator_id()).IsError()) {
        rsp.set_result(ReportTopologyRecoveryCandidateRspPb::STALE_LEADER_TERM);
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    CHECK_FAIL_RETURN_STATUS(
        req.result() == TOPOLOGY_RECOVERY_NO_SNAPSHOT || req.result() == TOPOLOGY_RECOVERY_SNAPSHOT, K_INVALID,
        "invalid topology recovery report result");
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = req.reporter_address();
    report.hasSnapshot = req.result() == TOPOLOGY_RECOVERY_SNAPSHOT;
    report.topologyVersion = req.topology_version();
    report.canonicalDigest = req.topology_digest();
    report.canonicalTopology = req.canonical_topology();
    TopologyRecoveryReportDecision decision;
    RETURN_IF_NOT_OK(topologyRecoveryManager_->ReportCandidate(req.cluster_name(), req.leader_term(),
                                                               req.coordinator_id(), std::move(report), decision));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_result(ToPbReportResult(decision.result));
    rsp.set_recovery_state(ToPbRecoveryState(decision.state));
    rsp.set_payload_required(decision.payloadRequired);
    return Status::OK();
}

Status CoordinatorServiceImpl::EnsureLeaderMembership(const EnsureLeaderMembershipReqPb &req,
                                                      EnsureLeaderMembershipRspPb &rsp)
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    FillResponseHeader(rsp.mutable_header());
    if (RequireRecoveryLeader(req.leader_term(), req.coordinator_id()).IsError()) {
        rsp.set_result(EnsureLeaderMembershipRspPb::STALE_EPOCH);
        return Status::OK();
    }
    std::string physicalKey;
    if (!BuildEnsureMembershipPhysicalKey(req, physicalKey)) {
        rsp.set_result(EnsureLeaderMembershipRspPb::INVALID_MEMBERSHIP);
        return Status::OK();
    }
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    std::string clusterName;
    bool reserved = false;
    RETURN_IF_NOT_OK(PrepareTopologyMembershipPut(physicalKey, clusterName, reserved));
    int64_t version = 0;
    int64_t revision = 0;
    Raii reservationCompletion([this, &clusterName, &reserved, &version, &revision] {
        if (reserved && topologyControlHost_ != nullptr) {
            topologyControlHost_->CompleteMembershipPut(clusterName, version > 0 && revision > 0);
        }
    });
    RETURN_IF_NOT_OK(store_->Put(physicalKey, req.membership_value(), req.ttl_ms(), COORDINATOR_NO_VERSION_CHECK,
                                 version, revision));
    if (RequireRecoveryLeader(req.leader_term(), req.coordinator_id()).IsError()) {
        rsp.set_result(EnsureLeaderMembershipRspPb::STALE_EPOCH);
        return Status::OK();
    }
    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    RETURN_IF_NOT_OK(store_->KeepAlive(physicalKey, ttlMs, remainingTtlMs));
    if (RequireRecoveryLeader(req.leader_term(), req.coordinator_id()).IsError()) {
        rsp.set_result(EnsureLeaderMembershipRspPb::STALE_EPOCH);
        return Status::OK();
    }
    FillResponseHeader(rsp.mutable_header());
    rsp.set_result(EnsureLeaderMembershipRspPb::ACCEPTED);
    rsp.set_remaining_ttl_ms(remainingTtlMs);
    rsp.set_membership_mod_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::ReportWorkerLiveness(const ReportWorkerLivenessReqPb &req,
                                                    ReportWorkerLivenessRspPb &rsp)
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    if (!businessAllowed) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(topologyControlHost_ != nullptr, K_NOT_READY, "topology Control Host is not bound");
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(req.cluster_name(), keys));
    std::string canonical;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::ProbeKey(req.witness_address(), canonical));
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(req.target_address(), canonical));
    CHECK_FAIL_RETURN_STATUS(req.coordinator_id() == coordinatorId_, K_TRY_AGAIN,
                             "worker liveness report CoordinatorId is stale");
    CHECK_FAIL_RETURN_STATUS(!req.target_member_id().empty() && req.probe_round() > 0
                                 && (req.result() == WORKER_REACHABLE || req.result() == WORKER_UNREACHABLE),
                             K_INVALID, "invalid worker liveness report");
    const auto result = req.result() == WORKER_REACHABLE ? cluster::WorkerLivenessResult::REACHABLE
                                                         : cluster::WorkerLivenessResult::UNREACHABLE;
    const auto probeId = cluster::WorkerProbeIdForLog(req.coordinator_id(), req.probe_round());
    LOG(INFO) << "CLUSTER_WORKER_PROBE cluster=" << req.cluster_name()
              << " action=WITNESS_PROBE_REPORT_RECEIVED probe_id=" << probeId << " witness=" << req.witness_address()
              << " target=" << req.target_address()
              << " target_id_prefix=" << cluster::MemberIdForLog(req.target_member_id())
              << " result=" << cluster::WorkerLivenessResultName(result);
    auto status = topologyControlHost_->EnqueueWorkerLivenessReport(req.cluster_name(),
                                                                    { req.coordinator_id(),
                                                                      req.witness_address(),
                                                                      { req.target_member_id(), req.target_address() },
                                                                      req.probe_round(),
                                                                      result });
    if (status.IsError()) {
        LOG(WARNING) << "CLUSTER_WORKER_PROBE cluster=" << req.cluster_name()
                     << " action=WITNESS_PROBE_REPORT_ENQUEUE_FAILED probe_id=" << probeId
                     << " witness=" << req.witness_address() << " target=" << req.target_address()
                     << " result=" << cluster::WorkerLivenessResultName(result) << " status=" << status.ToString();
        return status;
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::GetClusterRawSnapshot(const GetClusterRawSnapshotReqPb &req,
                                                     GetClusterRawSnapshotRspPb &rsp)
{
    std::shared_lock<std::shared_mutex> leaderLock(leaderOperationMutex_);
    bool businessAllowed = false;
    RETURN_IF_NOT_OK(PrepareRpcResponse(false, rsp.mutable_header(), businessAllowed));
    RETURN_OK_IF_TRUE(!businessAllowed);
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
    *localRsp.mutable_header() = rsp.header();
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
    header->set_coordinator_id(coordinatorId_);
    if (!IsElectionConfigured()) {
        header->set_is_leader(true);
        header->set_leader_term(0);
        header->set_serving_state(ResponseHeader::LEADER_SERVING);
        header->clear_leader_address();
        return;
    }
    const auto state = servingState_.load(std::memory_order_acquire);
    header->set_leader_term(leaderTerm_.load(std::memory_order_acquire));
    if (state == ServingState::LEADER_SERVING) {
        header->set_is_leader(true);
        header->set_serving_state(ResponseHeader::LEADER_SERVING);
        header->clear_leader_address();
        return;
    }
    if (state == ServingState::LEADER_RECOVERING) {
        header->set_is_leader(false);
        header->set_serving_state(ResponseHeader::LEADER_RECOVERING);
        header->set_leader_address(coordinatorAddr_.ToString());
        return;
    }
    header->set_is_leader(false);
    std::string leaderAddress;
    if (GetLeader(leaderAddress).IsOk()) {
        header->set_leader_address(std::move(leaderAddress));
        header->set_serving_state(ResponseHeader::FOLLOWER_SERVING);
    } else {
        header->clear_leader_address();
        header->clear_serving_state();
    }
}
}  // namespace coordinator
}  // namespace datasystem
