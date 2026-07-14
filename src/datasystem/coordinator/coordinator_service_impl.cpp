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

DS_DEFINE_uint64(coordinator_rpc_stub_cache_size, 2048, "Maximum coordinator RPC stub cache size.");

namespace datasystem {
namespace coordinator {
namespace {
Status CheckCoordinatorStore(const std::shared_ptr<CoordinatorStore> &store)
{
    CHECK_FAIL_RETURN_STATUS(store != nullptr, StatusCode::K_NOT_READY, "coordinator store is not bound");
    return Status::OK();
}

void MarkLeader(ResponseHeader *header)
{
    if (header == nullptr) {
        return;
    }
    header->set_is_leader(true);
    header->clear_leader_address();
}

void FillKeyValuePb(const KeyValueEntry &entry, KeyValue *kv)
{
    kv->set_key(entry.key);
    kv->set_value(entry.value);
    kv->set_version(entry.version);
    kv->set_mod_revision(entry.modRevision);
}
}  // namespace

CoordinatorServiceImpl::CoordinatorServiceImpl(const HostPort &localAddress)
    : CoordinatorService(localAddress), coordinatorAddr_(localAddress)
{
}

Status CoordinatorServiceImpl::Init()
{
    Logging::GetInstance()->Start("datasystem_coordinator");

    // RPC authentication setup
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_coordinator_rpc_stub_cache_size, coordinatorAddr_));

    // Build the internal component tree
    memStore_ = std::make_shared<MemoryKvStore>();
    watchRegistry_ = std::make_shared<WatchRegistry>();
    watchDispatcher_ = std::make_shared<WatchDispatcherImpl>(watchRegistry_.get());
    clock_ = std::make_shared<SteadyClockReal>();
    ttlManager_ = std::make_shared<TtlManager>(clock_);
    store_ = std::make_shared<CoordinatorStore>(memStore_, watchRegistry_, watchDispatcher_, ttlManager_);

    // Configure RPC service settings
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = FLAGS_rpc_thread_num;
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
    cfg.udsEnabled_ = false;

    if (FLAGS_use_brpc) {
        // brpc path: register ZMQ builder for common infra, brpc handles the RPC service.
        // CoordinatorServiceImpl : ICoordinatorService → CoordinatorServiceBrpcAdapter in Start().
        brpcAddr_ = coordinatorAddr_.Host();
        brpcPort_ = coordinatorAddr_.Port() + kBrpcPortOffset;
        builder_.SetUseBrpc(true).SetBrpcAddr(brpcAddr_, brpcPort_);
        builder_.AddService(this, cfg);
    } else {
        // ZMQ path (original)
        builder_.AddEndPoint(RpcChannel::TcpipEndPoint(coordinatorAddr_));
        builder_.AddService(this, cfg);
    }
    return Status::OK();
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
    // Reset in reverse dependency order
    store_.reset();
    ttlManager_.reset();
    clock_.reset();
    watchDispatcher_.reset();
    watchRegistry_.reset();
    memStore_.reset();
    LOG(INFO) << "Coordinator shutdown success.";
    return Status::OK();
}

Status CoordinatorServiceImpl::Put(const PutReqPb &req, PutRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t version = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Put(req.key(), req.value(), req.ttl(), req.expected_version(), version, revision));
    MarkLeader(rsp.mutable_header());
    rsp.set_version(version);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::Range(const RangeReqPb &req, RangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->Range(req.key(), req.range_end(), kvs, revision));
    MarkLeader(rsp.mutable_header());
    rsp.set_revision(revision);
    for (const auto &entry : kvs) {
        FillKeyValuePb(entry, rsp.add_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t deleted = 0;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(store_->DeleteRange(req.key(), req.range_end(), deleted, revision));
    MarkLeader(rsp.mutable_header());
    rsp.set_deleted(deleted);
    rsp.set_revision(revision);
    return Status::OK();
}

Status CoordinatorServiceImpl::WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initialKvs;
    RETURN_IF_NOT_OK(store_->WatchRange(req.key(), req.range_end(), req.watcher_addr(), watchId, initialKvs));
    MarkLeader(rsp.mutable_header());
    rsp.set_watch_id(watchId);
    for (const auto &entry : initialKvs) {
        FillKeyValuePb(entry, rsp.add_initial_kvs());
    }
    return Status::OK();
}

Status CoordinatorServiceImpl::CancelWatch(const CancelWatchReqPb &req, CancelWatchRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    std::vector<int64_t> watchIds(req.watch_ids().begin(), req.watch_ids().end());
    RETURN_IF_NOT_OK(store_->CancelWatch(req.watcher_addr(), watchIds));
    MarkLeader(rsp.mutable_header());
    return Status::OK();
}

Status CoordinatorServiceImpl::KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp)
{
    RETURN_IF_NOT_OK(CheckCoordinatorStore(store_));

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    RETURN_IF_NOT_OK(store_->KeepAlive(req.key(), ttlMs, remainingTtlMs));
    MarkLeader(rsp.mutable_header());
    rsp.set_ttl(ttlMs);
    rsp.set_remaining_ttl(remainingTtlMs);
    return Status::OK();
}
}  // namespace coordinator
}  // namespace datasystem
