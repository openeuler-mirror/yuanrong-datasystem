/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

/**
 * Description: The interface of file cache worker descriptor.
 */
#include "datasystem/master/stream_cache/master_worker_sc_api.h"

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/worker/stream_cache/master_worker_sc_service_impl.h"

namespace datasystem {
namespace master {

// Base class methods
MasterWorkerSCApi::MasterWorkerSCApi(HostPort localMasterAddress, std::shared_ptr<AkSkManager> akSkManager)
    : localMasterAddress_(std::move(localMasterAddress)), akSkManager_(std::move(akSkManager))
{
}

std::shared_ptr<MasterWorkerSCApi> MasterWorkerSCApi::CreateMasterWorkerSCApi(
    const HostPort &hostPort, const HostPort &localHostPort, const std::shared_ptr<AkSkManager> &akSkManager,
    worker::stream_cache::MasterWorkerSCServiceImpl *service)
{
    if (hostPort != localHostPort) {
        LOG(INFO) << "Master and worker are not collocated. Creating a MasterWorkerSCApi as RPC-based api.";
        return std::make_shared<MasterRemoteWorkerSCApi>(hostPort, localHostPort, akSkManager);
    }
    if (service == nullptr) {
        LOG(INFO) << "Master and worker are collocated but the worker service is not provided. Local bypass disabled.";
        return std::make_shared<MasterRemoteWorkerSCApi>(hostPort, localHostPort, akSkManager);
    }
    LOG(INFO) << "Master and worker are collocated. Creating a MasterWorkerOCApi with local bypass optimization.";
    return std::make_shared<MasterLocalWorkerSCApi>(service, localHostPort, akSkManager);
}

void MasterWorkerSCApi::ConstructSyncConsumerNodePb(const std::string &streamName,
                                                    const std::vector<ConsumerMetaPb> &consumerMetas,
                                                    const HostPort &src, const RetainDataState::State retainData,
                                                    bool isRecon, SyncConsumerNodeReqPb &req) noexcept
{
    req.set_stream_name(streamName);
    req.set_retain_data(retainData);
    req.set_is_reconciliation(isRecon);
    for (const auto &consumerMeta : consumerMetas) {
        ConsumerMetaPb *consumerMetaPb = req.add_consumer_meta_vector();
        if (consumerMetaPb == nullptr) {
            LOG(ERROR) << FormatString("[S:%s] add_consumer_meta_vector pointer does not exit", streamName);

            return;
        }
        // ConsumerMetaPb = (streamName, workerAddress, consumerId, subConfig, lastAckCursor).
        consumerMetaPb->set_stream_name(consumerMeta.stream_name());
        *consumerMetaPb->mutable_worker_address() = consumerMeta.worker_address();

        consumerMetaPb->set_consumer_id(consumerMeta.consumer_id());
        *consumerMetaPb->mutable_sub_config() = consumerMeta.sub_config();

        consumerMetaPb->set_last_ack_cursor(consumerMeta.last_ack_cursor());
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
        "Stream:<%s>, Pub node:<%s>, Operation:<Sync sub consumer nodes>, nodes size:<%d>", streamName, src.ToString(),
        req.consumer_meta_vector_size());
}

void MasterWorkerSCApi::ConstructSyncPubNodePb(const std::string &streamName, const std::set<HostPort> &pubTable,
                                               const HostPort &src, bool isRecon, SyncPubNodeReqPb &req) noexcept
{
    req.set_stream_name(streamName);
    req.set_is_reconciliation(isRecon);
    for (auto &pubNode : pubTable) {
        auto workerNodePb = req.add_worker_address_vector();
        if (workerNodePb == nullptr) {
            LOG(ERROR) << FormatString("[S:%s] add_worker_address_vector pointer does not exit", streamName);
            return;
        }
        workerNodePb->set_host(pubNode.Host());
        workerNodePb->set_port(pubNode.Port());
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Stream:<%s>, Dest:<%s>, Operation:<Sync pub worker nodes>, Size:<%d>",
                                              streamName, src.ToString(), req.worker_address_vector_size());
}

// MasterRemoteWorkerSCApi methods
MasterRemoteWorkerSCApi::MasterRemoteWorkerSCApi(HostPort workerAddress, const HostPort &localAddress,
                                                 std::shared_ptr<AkSkManager> akSkManager)
    : MasterWorkerSCApi(localAddress, std::move(akSkManager)), workerAddress_(std::move(workerAddress))
{
}

Status MasterRemoteWorkerSCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(
        RpcStubCacheMgr::Instance().GetStub(workerAddress_, StubType::MASTER_WORKER_SC_SVC, rpcStub));
    rpcSession_ = std::dynamic_pointer_cast<MasterWorkerSCService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    return Status::OK();
}

MasterWorkerSCApiType MasterRemoteWorkerSCApi::TypeId()
{
    return MasterWorkerSCApiType::MasterRemoteWorkerSCApi;
}

Status MasterRemoteWorkerSCApi::DelStreamContextBroadcast(const std::string &streamName, bool forceDelete)
{
    DelStreamContextReqPb req;
    req.set_stream_name(streamName);
    req.set_force_delete(forceDelete);
    INJECT_POINT("MasterRemoteWorkerSCApi.DelStreamContextBroadcast.sleep");
    DelStreamContextRspPb rsp;
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->DelStreamContext(opts, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream broadcast success.", LogPrefix(), streamName);
    return Status::OK();
}

Status MasterRemoteWorkerSCApi::DelStreamContextBroadcastAsyncWrite(const std::string &streamName, bool forceDelete,
                                                                    int64_t &tagId)
{
    DelStreamContextReqPb req;
    req.set_stream_name(streamName);
    req.set_force_delete(forceDelete);
    INJECT_POINT("MasterRemoteWorkerSCApi.DelStreamContextBroadcast.sleep");
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->DelStreamContextAsyncWrite(opts, req, tagId));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream async broadcast send success.", LogPrefix(),
                                              streamName);
    return Status::OK();
}

Status MasterRemoteWorkerSCApi::DelStreamContextBroadcastAsyncRead(int64_t tagId, RpcRecvFlags flags)
{
    DelStreamContextRspPb rsp;
    RETURN_IF_NOT_OK(rpcSession_->DelStreamContextAsyncRead(tagId, rsp, flags));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Delete stream async broadcast receive success.", LogPrefix());
    return Status::OK();
}

Status MasterRemoteWorkerSCApi::SyncPubNode(const std::string &streamName, const std::set<HostPort> &pubNodeSet,
                                            bool isRecon)
{
    SyncPubNodeReqPb req;
    ConstructSyncPubNodePb(streamName, pubNodeSet, workerAddress_, isRecon, req);

    SyncPubNodeRspPb rsp;
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->SyncPubNode(opts, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] SyncPubNode success, node size:[%d]", LogPrefix(), streamName,
                                              pubNodeSet.size());
    return Status::OK();
}

Status MasterRemoteWorkerSCApi::SyncConsumerNode(const std::string &streamName,
                                                 const std::vector<ConsumerMetaPb> &consumerMetas,
                                                 const RetainDataState::State retainData, bool isRecon)
{
    SyncConsumerNodeReqPb req;
    ConstructSyncConsumerNodePb(streamName, consumerMetas, workerAddress_, retainData, isRecon, req);

    SyncConsumerNodeRspPb rsp;
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->SyncConsumerNode(opts, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] SyncConsumer success, consumer size:[%d]", LogPrefix(),
                                              streamName, consumerMetas.size());
    return Status::OK();
}

Status MasterRemoteWorkerSCApi::ClearAllRemotePub(const std::string &streamName)
{
    ClearRemoteInfoReqPb req;
    req.set_stream_name(streamName);

    ClearRemoteInfoRspPb rsp;
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Send ClearAllRemotePub request to worker", LogPrefix(),
                                              streamName);
    RETURN_IF_NOT_OK(rpcSession_->ClearAllRemotePub(opts, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] ClearAllRemotePub to worker success", LogPrefix(),
                                              streamName);
    return Status::OK();
}

std::string MasterRemoteWorkerSCApi::LogPrefix() const
{
    return FormatString("MasterWorkerApi, EndPoint:%s", workerAddress_.ToString());
}

Status MasterRemoteWorkerSCApi::QueryMetadata(
    std::unique_ptr<ClientWriterReader<GetMetadataAllStreamReqPb, GetStreamMetadataRspPb>> &stream)
{
    return rpcSession_->QueryMetadata(&stream);
}

Status MasterRemoteWorkerSCApi::UpdateTopoNotification(UpdateTopoNotificationReq &req)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    INJECT_POINT("master.UpdateTopoNotification.setTimeout", [&opts](int timeout) {
        LOG(INFO) << "set rpc timeout to " << timeout;
        opts.SetTimeout(timeout);
        return Status::OK();
    });
    UpdateTopoNotificationRsp rsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->UpdateTopoNotification(opts, req, rsp);
}

Status MasterRemoteWorkerSCApi::ClearAllRemotePubAsynWrite(const std::string &streamName, int64_t &tagId)
{
    ClearRemoteInfoReqPb req;
    req.set_stream_name(streamName);

    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s]Asyn write ClearAllRemotePub request to worker", LogPrefix(),
                                              streamName);
    Status rc = rpcSession_->ClearAllRemotePubAsyncWrite(opts, req, tagId);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s]Asyn write ClearAllRemotePub to worker success", LogPrefix(),
                                              streamName);
    return rc;
}

Status MasterRemoteWorkerSCApi::ClearAllRemotePubAsynRead(int64_t tagId, RpcRecvFlags flags)
{
    ClearRemoteInfoRspPb rsp;
    Status rc = rpcSession_->ClearAllRemotePubAsyncRead(tagId, rsp, flags);
    return rc;
}

// MasterLocalWorkerSCApi methods
MasterLocalWorkerSCApi::MasterLocalWorkerSCApi(worker::stream_cache::MasterWorkerSCServiceImpl *service,
                                               const HostPort &localAddress, std::shared_ptr<AkSkManager> akSkManager)
    : MasterWorkerSCApi(localAddress, std::move(akSkManager)), workerSC_(service)
{
}

Status MasterLocalWorkerSCApi::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(workerSC_);
    return Status::OK();
}

MasterWorkerSCApiType MasterLocalWorkerSCApi::TypeId()
{
    return MasterWorkerSCApiType::MasterLocalWorkerSCApi;
}

Status MasterLocalWorkerSCApi::DelStreamContextBroadcast(const std::string &streamName, bool forceDelete)
{
    DelStreamContextReqPb req;
    req.set_stream_name(streamName);
    req.set_force_delete(forceDelete);
    INJECT_POINT("MasterLocalWorkerSCApi.DelStreamContextBroadcast.sleep");
    // We use timeout to avoid deadlock
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    INJECT_POINT("MasterLocalWorkerSCApi.DelStreamContextBroadcast.setTimeout", [&req](int timeout) {
        LOG(INFO) << "set rpc timeout to " << timeout;
        req.set_timeout(timeout);
        return Status::OK();
    });
    DelStreamContextRspPb rsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerSC_->DelStreamContext(req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream broadcast success.", LogPrefix(), streamName);
    return Status::OK();
}

Status MasterLocalWorkerSCApi::DelStreamContextBroadcastAsyncWrite(const std::string &streamName, bool forceDelete,
                                                                   int64_t &tagId)
{
    (void)forceDelete;
    (void)tagId;
    // AsyncWrite and AsynRead with local bypass is not supported.
    LOG(WARNING) << FormatString("[%s, S:%s] Async DelStreamContextBroadcast not supported for MasterLocalWorkerSCApi",
                                 LogPrefix(), streamName);
    return Status::OK();
}

Status MasterLocalWorkerSCApi::DelStreamContextBroadcastAsyncRead(int64_t tagId, RpcRecvFlags flags)
{
    (void)tagId;
    (void)flags;
    // AsyncWrite and AsynRead with local bypass is not supported.
    LOG(WARNING) << FormatString("[%s] Async DelStreamContextBroadcast not supported for MasterLocalWorkerSCApi",
                                 LogPrefix());
    return Status::OK();
}

Status MasterLocalWorkerSCApi::SyncPubNode(const std::string &streamName, const std::set<HostPort> &pubNodeSet,
                                           bool isRecon)
{
    SyncPubNodeReqPb req;
    ConstructSyncPubNodePb(streamName, pubNodeSet, localMasterAddress_, isRecon, req);

    SyncPubNodeRspPb rsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerSC_->SyncPubNode(req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] SyncPubNode success, node size:[%d]", LogPrefix(), streamName,
                                              pubNodeSet.size());
    return Status::OK();
}

Status MasterLocalWorkerSCApi::SyncConsumerNode(const std::string &streamName,
                                                const std::vector<ConsumerMetaPb> &consumerMetas,
                                                const RetainDataState::State retainData, bool isRecon)
{
    SyncConsumerNodeReqPb req;
    ConstructSyncConsumerNodePb(streamName, consumerMetas, localMasterAddress_, retainData, isRecon, req);

    SyncConsumerNodeRspPb rsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerSC_->SyncConsumerNode(req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] SyncConsumer success, consumer size:[%d]", LogPrefix(),
                                              streamName, consumerMetas.size());
    return Status::OK();
}

Status MasterLocalWorkerSCApi::ClearAllRemotePub(const std::string &streamName)
{
    ClearRemoteInfoReqPb req;
    req.set_stream_name(streamName);

    ClearRemoteInfoRspPb rsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Send ClearAllRemotePub request to worker", LogPrefix(),
                                              streamName);
    RETURN_IF_NOT_OK(workerSC_->ClearAllRemotePub(req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] ClearAllRemotePub to worker success", LogPrefix(),
                                              streamName);
    return Status::OK();
}

std::string MasterLocalWorkerSCApi::LogPrefix() const
{
    // local version of the api has worker and master as same address (the local one)
    return FormatString("MasterWorkerApi, EndPoint:%s", localMasterAddress_.ToString());
}

Status MasterLocalWorkerSCApi::QueryMetadata(
    std::unique_ptr<ClientWriterReader<GetMetadataAllStreamReqPb, GetStreamMetadataRspPb>> &stream)
{
    // The local version of the api should not call this one. It should use the other syntax for local-only calls.
    stream.reset();
    RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Local version of MasterWorkerApi was incorrectly used.");
}

Status MasterLocalWorkerSCApi::QueryMetadata(const GetMetadataAllStreamReqPb &req, GetMetadataAllStreamRspPb &rsp)
{
    return workerSC_->QueryMetadata(req, rsp);
}


Status MasterLocalWorkerSCApi::UpdateTopoNotification(UpdateTopoNotificationReq &req)
{
    UpdateTopoNotificationRsp rsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(workerSC_->UpdateTopoNotification(req, rsp));
    return Status::OK();
}
}  // namespace master
}  // namespace datasystem
