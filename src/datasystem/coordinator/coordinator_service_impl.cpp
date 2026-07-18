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

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/coordinator/topology_recovery_manager.h"

DS_DEFINE_uint64(coordinator_rpc_stub_cache_size, 2048, "Maximum coordinator RPC stub cache size.");

namespace datasystem {
namespace coordinator {
namespace {
constexpr size_t COORDINATOR_ID_LOG_PREFIX_SIZE = 8;
constexpr size_t MAX_CLUSTER_RAW_SNAPSHOT_BYTES = 16 * 1'024 * 1'024;
constexpr size_t MAX_CLUSTER_RAW_MEMBERSHIPS = 10'000;

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

void FillKeyValuePbs(const std::vector<KeyValueEntry> &entries,
                     google::protobuf::RepeatedPtrField<KeyValue> *output)
{
    for (const auto &entry : entries) {
        FillKeyValuePb(entry, output->Add());
    }
}

Status BuildClusterReadKeys(const std::string &clusterName, std::string &topologyKey,
                            std::string &membershipKey, std::string &membershipEnd)
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

CoordinatorServiceImpl::CoordinatorServiceImpl(const HostPort &localAddress)
    : CoordinatorService(localAddress), coordinatorAddr_(localAddress)
{
}

CoordinatorServiceImpl::~CoordinatorServiceImpl()
{
    (void)Shutdown();
}

Status CoordinatorServiceImpl::Init()
{
    Logging::GetInstance()->Start("datasystem_coordinator");
    coordinatorId_ = GetBytesUuid();
    LOG(INFO) << "CLUSTER_COORDINATOR_ID role=coordinator id="
              << coordinatorId_.substr(0, COORDINATOR_ID_LOG_PREFIX_SIZE) << " state=created";
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_coordinator_rpc_stub_cache_size, coordinatorAddr_));
    BuildComponentTree();
    ConfigureRpcService();
    return Status::OK();
}

void CoordinatorServiceImpl::BuildComponentTree()
{
    memStore_ = std::make_shared<MemoryKvStore>();
    watchRegistry_ = std::make_shared<WatchRegistry>();
    watchDispatcher_ = std::make_shared<WatchDispatcherImpl>(watchRegistry_.get(), coordinatorId_);
    clock_ = std::make_shared<SteadyClockReal>();
    ttlManager_ = std::make_shared<TtlManager>(clock_);
    store_ = std::make_shared<CoordinatorStore>(memStore_, watchRegistry_, watchDispatcher_, ttlManager_);
    topologyRecoveryManager_ =
        std::make_unique<TopologyRecoveryManager>(coordinatorId_, *store_, clock_, TopologyRecoveryOptions{});
    store_->SetCommittedMutationObserver([this](WatchEvent::Type, const std::string &key) {
        HandleCommittedMembershipMutation(key);
    });
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
        LOG(WARNING) << "CLUSTER_MEMBERSHIP_OBSERVER_READ_FAILED, key=" << key
                     << ", status=" << rangeStatus.ToString();
        return;
    }
    const bool present = !current.empty();
    topologyRecoveryManager_->ObserveMembershipChange(key, present);
    if (!present && watchDispatcher_ != nullptr) {
        watchDispatcher_->RemoveChannelsByWatcher(parsed.relativeKey);
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

Status CoordinatorServiceImpl::Start()
{
    RETURN_IF_NOT_OK(builder_.Init(rpcServer_));
    RETURN_IF_NOT_OK(builder_.BuildAndStart(rpcServer_));

    if (FLAGS_use_brpc && rpcServer_->IsBrpc()) {
        brpcAdapter_ = std::make_unique<CoordinatorServiceBrpcAdapter>(*this);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcAdapter_.get()));
        RETURN_IF_NOT_OK(rpcServer_->StartBrpcServer(brpcAddr_, brpcPort_));
    }

    LOG(INFO) << "datasystem coordinator started at " << coordinatorAddr_.ToString()
              << (FLAGS_use_brpc ? " (brpc)" : " (ZMQ)");
    return Status::OK();
}

Status CoordinatorServiceImpl::Shutdown()
{
    LOG(INFO) << "Coordinator process executing a shutdown.";
    if (rpcServer_ != nullptr) {
        rpcServer_->Shutdown();   // Stops brpc (StopBrpcServer) + ZMQ internally.
        brpcAdapter_.reset();     // Safe: brpc server already stopped, no longer references adapter.
        rpcServer_.reset();
    }
    if (topologyRecoveryManager_ != nullptr) {
        LOG_IF_ERROR(topologyRecoveryManager_->Shutdown(), "CLUSTER_RECOVERY_MANAGER_SHUTDOWN_FAILED");
    }
    if (store_ != nullptr) {
        store_->StopTtl();
        store_->SetCommittedMutationObserver({});
    }
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
    LOG(INFO) << "Coordinator shutdown success.";
    return Status::OK();
}

Status CoordinatorServiceImpl::Put(const PutReqPb &req, PutRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id().empty()
                                 || req.expected_coordinator_id() == coordinatorId_,
                             K_TRY_AGAIN, "Put CoordinatorId fence no longer matches this process");
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->CheckMutationAllowed(req.key(), ""));

    int64_t version = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Put(req.key(), req.value(), req.ttl(), req.expected_version(), version, revision));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_version(version);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::Range(const RangeReqPb &req, RangeRspPb &rsp)
{
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
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(req.expected_coordinator_id().empty()
                                 || req.expected_coordinator_id() == coordinatorId_,
                             K_TRY_AGAIN, "DeleteRange CoordinatorId fence no longer matches this process");
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->CheckMutationAllowed(req.key(), req.range_end()));

    int64_t deleted = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->DeleteRange(req.key(), req.range_end(), deleted, revision));
    FillResponseHeader(rsp.mutable_header());
    rsp.set_deleted(deleted);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));
    CHECK_FAIL_RETURN_STATUS(!req.registration_id().empty(), K_INVALID, "watch registration ID is empty");
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    RETURN_IF_NOT_OK(topologyRecoveryManager_->ValidateWatchRange(req.key(), req.range_end()));
    std::lock_guard<std::mutex> lock(membershipWatchMutex_);
    RETURN_IF_NOT_OK(CheckWatcherMembership(req));

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initialKvs;
    RETURN_IF_NOT_OK(store_->WatchRange(req.key(), req.range_end(), req.watcher_addr(), req.registration_id(), watchId,
                                       initialKvs));
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
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    RETURN_IF_NOT_OK(store_->KeepAlive(req.key(), ttlMs, remainingTtlMs));
    if (topologyRecoveryManager_ != nullptr) {
        topologyRecoveryManager_->NotifyMembershipActivity(req.key());
    }
    FillResponseHeader(rsp.mutable_header());
    rsp.set_ttl(ttlMs);
    rsp.set_remaining_ttl(remainingTtlMs);
    return Status::OK();
}

Status CoordinatorServiceImpl::GetCoordinatorId(const GetCoordinatorIdReqPb &, GetCoordinatorIdRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(coordinatorId_.size() == UUID_SIZE, K_NOT_READY, "CoordinatorId is not initialized");
    FillResponseHeader(rsp.mutable_header());
    return Status::OK();
}

Status CoordinatorServiceImpl::ReportTopologyRecoveryCandidate(const ReportTopologyRecoveryCandidateReqPb &req,
                                                                ReportTopologyRecoveryCandidateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(topologyRecoveryManager_ != nullptr, K_NOT_READY, "recovery manager is not bound");
    CHECK_FAIL_RETURN_STATUS(req.result() == TOPOLOGY_RECOVERY_NO_SNAPSHOT
                                 || req.result() == TOPOLOGY_RECOVERY_SNAPSHOT,
                             K_INVALID, "invalid topology recovery report result");
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
    header->set_is_leader(true);
    header->clear_leader_address();
    header->set_coordinator_id(coordinatorId_);
}
}  // namespace coordinator
}  // namespace datasystem
