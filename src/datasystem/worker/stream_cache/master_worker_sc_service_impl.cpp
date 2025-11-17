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

#include "datasystem/worker/stream_cache/master_worker_sc_service_impl.h"

#include "datasystem/common/util/format.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
MasterWorkerSCServiceImpl::MasterWorkerSCServiceImpl(HostPort serverAddr, HostPort masterAddr,
                                                     ClientWorkerSCServiceImpl *clientSvc,
                                                     std::shared_ptr<AkSkManager> akSkManager)
    : localWorkerAddress_(std::move(serverAddr)),
      masterAddress_(std::move(masterAddr)),
      clientWorkerSCSvc_(clientSvc),
      akSkManager_(std::move(akSkManager))
{
}

Status MasterWorkerSCServiceImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS(clientWorkerSCSvc_ != nullptr, StatusCode::K_NOT_READY,
                             "ClientWorkerService must be initialized before MasterWorkerService construction");
    LOG(INFO) << FormatString("[%s, Master address:%s] Initialization success.", LogPrefix(true),
                              masterAddress_.ToString());
    return MasterWorkerSCService::Init();
}

Status MasterWorkerSCServiceImpl::AddRemoteConsumer(const std::shared_ptr<StreamManager> &streamManager,
                                                    const ConsumerMetaPb &meta)
{
    const auto &workerAddr = meta.worker_address();
    const auto &consumerId = meta.consumer_id();
    const auto &streamName = meta.stream_name();
    HostPort workerAddress(workerAddr.host(), workerAddr.port());

    SubscriptionConfig config(meta.sub_config().subscription_name(),
                              ToSubscriptionType(meta.sub_config().subscription_type()));
    uint64_t lastAckCursor;
    RETURN_IF_NOT_OK_EXCEPT(streamManager->AddRemoteSubNode(workerAddress, config, consumerId, lastAckCursor),
                            K_DUPLICATED);
    auto &remoteWorkerManager = clientWorkerSCSvc_->remoteWorkerManager_;
    RETURN_IF_NOT_OK(remoteWorkerManager->AddRemoteConsumer(streamManager, localWorkerAddress_, workerAddress,
                                                            streamName, config, consumerId, lastAckCursor));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] New Consumer <Sub:%s, C:%s, RW:%s> success", LogPrefix(),
                                              streamName, config.subscriptionName, consumerId,
                                              workerAddress.ToString());
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::DelRemoteConsumer(const std::shared_ptr<StreamManager> &streamManager,
                                                    const ConsumerMetaPb &consumerMeta)
{
    const auto &workerAddr = consumerMeta.worker_address();
    const auto &consumerId = consumerMeta.consumer_id();
    const auto &streamName = consumerMeta.stream_name();
    HostPort workerAddress(workerAddr.host(), workerAddr.port());

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] DelSubNode <Sub:%s, C:%s, Remote:%s> begin", LogPrefix(),
                                              streamName, consumerMeta.sub_config().subscription_name(), consumerId,
                                              workerAddress.ToString());

    RETURN_IF_NOT_OK_EXCEPT(streamManager->DelRemoteSubNode(workerAddress, consumerId), K_NOT_FOUND);
    auto &remoteWorkerManager = clientWorkerSCSvc_->remoteWorkerManager_;
    RETURN_IF_NOT_OK_EXCEPT(remoteWorkerManager->DelRemoteConsumer(workerAddress.ToString(), streamName, consumerId),
                            K_NOT_FOUND);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] DelSubNode <Sub:%s, C:%s, Remote:%s> success", LogPrefix(),
                                              streamName, consumerMeta.sub_config().subscription_name(), consumerId,
                                              workerAddress.ToString());
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::SyncPubNode(const SyncPubNodeReqPb &req, SyncPubNodeRspPb &rsp)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const std::string &streamName = req.stream_name();
    LOG(INFO) << FormatString("worker(%s) received SyncPubNode request, streamname: %s", localWorkerAddress_.ToString(),
                              streamName);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerSCSvc_->GetStreamManager(streamName, accessor),
                                     "worker get streamManager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;

    auto pubNodeNum = req.worker_address_vector_size();

    std::vector<HostPort> pubNodeSet;
    pubNodeSet.reserve(pubNodeNum);
    for (int i = 0; i < pubNodeNum; ++i) {
        HostPort pubWorkerNode(req.worker_address_vector(i).host(), req.worker_address_vector(i).port());
        pubNodeSet.emplace_back(std::move(pubWorkerNode));
    }
    Status rc = streamManager->SyncPubTable(pubNodeSet, req.is_reconciliation());
    LOG_IF(INFO, rc.IsError()) << "streamManager SyncPubTable failed: " << rc.ToString();
    RETURN_IF_NOT_OK(rc);
    LOG(INFO) << FormatString("[%s, S:%s] SyncPubNode table success, Table size:%d", LogPrefix(true), streamName,
                              pubNodeNum);
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::SyncConsumerNode(const SyncConsumerNodeReqPb &req, SyncConsumerNodeRspPb &rsp)
{
    INJECT_POINT("MasterWorkerSCServiceImpl.SyncConsumerNode.sleep");
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const std::string &streamName = req.stream_name();
    LOG(INFO) << FormatString("worker(%s) received SyncConsumerNode request, streamname: %s",
                              localWorkerAddress_.ToString(), streamName);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerSCSvc_->GetStreamManager(streamName, accessor),
                                     "worker get streamManager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    RETURN_IF_NOT_OK(streamManager->CheckIfStreamActive());

    auto consumerNum = req.consumer_meta_vector_size();

    // If pubTable has at least 1 consumer node, we do the following synchronize.
    std::vector<ConsumerMeta> consumerNodeSet;
    consumerNodeSet.reserve(consumerNum);
    for (int i = 0; i < consumerNum; ++i) {
        // ConsumerMeta = (consumerId, workerNode, subConfig, lastAckCursor).
        const auto &consumerMetaPb = req.consumer_meta_vector(i);
        const std::string &consumerId(consumerMetaPb.consumer_id());
        HostPort workerNode(consumerMetaPb.worker_address().host(), consumerMetaPb.worker_address().port());
        const auto &subconfigPb = consumerMetaPb.sub_config();
        SubscriptionConfig subConfig(subconfigPb.subscription_name(),
                                     ToSubscriptionType(subconfigPb.subscription_type()));

        consumerNodeSet.emplace_back(streamName, consumerId, workerNode, subConfig, consumerMetaPb.last_ack_cursor());
    }
    // Do not start tables from scratch if it is triggered by reconciliation code path,
    // duplicates are skipped instead.
    bool isRecon = req.is_reconciliation();
    uint64_t lastAckCursor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamManager->SyncSubTable(consumerNodeSet, isRecon, lastAckCursor),
                                     "streamManager SyncSubTable failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SyncRemoteConsumer(streamManager, consumerNodeSet, lastAckCursor),
                                     "worker SyncRemoteConsumer failed");
    streamManager->SetRetainData(req.retain_data());
    LOG(INFO) << FormatString("[%s, S:%s] SyncConsumerNode table success, Table size:%d", LogPrefix(true), streamName,
                              consumerNum);
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::ClearAllRemotePub(const ClearRemoteInfoReqPb &req, ClearRemoteInfoRspPb &rsp)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const std::string &streamName = req.stream_name();
    LOG(INFO) << FormatString("worker(%s) received ClearAllRemotePub request, streamname: %s",
                              localWorkerAddress_.ToString(), streamName);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerSCSvc_->GetStreamManager(streamName, accessor),
                                     "worker get streamManager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamManager->ClearAllRemotePub(), "streamManager ClearAllRemotePub failed");
    LOG(INFO) << "worker ClearAllRemotePub done, streamname: " << streamName;
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::ClearAllRemoteConsumer(const ClearRemoteInfoReqPb &req, ClearRemoteInfoRspPb &rsp)
{
    // ClearAllRemoteConsumer RPC deprecated.
    INJECT_POINT("MasterWorkerSCServiceImpl.ClearAllRemoteConsumer.sleep");
    (void)rsp;
    (void)req;
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::DelStreamContext(const DelStreamContextReqPb &req, DelStreamContextRspPb &rsp)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const auto &streamName = req.stream_name();
    bool forceDelete = req.force_delete();
    auto timeout = req.timeout();
    LOG(INFO) << FormatString("worker(%s) received DelStreamContext request, streamname: %s, force delete: %s",
                              localWorkerAddress_.ToString(), streamName, forceDelete ? "true" : "false");

    RETURN_IF_NOT_OK(clientWorkerSCSvc_->DeleteStreamContext(streamName, forceDelete, timeout));
    return Status::OK();
}

std::string MasterWorkerSCServiceImpl::LogPrefix(bool withAddress) const
{
    if (withAddress) {
        return FormatString("MasterWorkerSvc, Node:%s", localWorkerAddress_.ToString());
    } else {
        return "MasterWorkerSvc";
    }
}

Status MasterWorkerSCServiceImpl::SyncRemoteConsumer(const std::shared_ptr<StreamManager> &streamManager,
                                                     const std::vector<ConsumerMeta> &remoteConsumerSet,
                                                     uint64_t lastAckCursor)
{
    const std::string streamName = streamManager->GetStreamName();
    auto &remoteWorkerManager = clientWorkerSCSvc_->remoteWorkerManager_;
    for (const auto &remoteConsumer : remoteConsumerSet) {
        // Do sync part
        RETURN_IF_NOT_OK(remoteWorkerManager->AddRemoteConsumer(
            streamManager, localWorkerAddress_, remoteConsumer.WorkerAddress(), streamName, remoteConsumer.SubConfig(),
            remoteConsumer.ConsumerId(), lastAckCursor));
    }
    return Status::OK();
}

Status MasterWorkerSCServiceImpl::QueryMetadata(
    std::shared_ptr<ServerWriterReader<GetStreamMetadataRspPb, GetMetadataAllStreamReqPb>> stream)
{
    LOG(INFO) << "worker(" << localWorkerAddress_.ToString() << ") receive QueryMetaData for all streams from master";
    GetMetadataAllStreamReqPb req;
    RETURN_IF_NOT_OK(stream->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    Status status = clientWorkerSCSvc_->SendAllStreamMetadata(req, stream);
    if (status.IsError() && status.GetCode() != K_RPC_STREAM_END) {
        LOG(ERROR) << "Send response to stream failed: " << status.GetMsg();
        return status;
    }
    LOG(INFO) << "worker QueryMetaData for all streams done";
    return stream->Finish();
}

Status MasterWorkerSCServiceImpl::QueryMetadata(const GetMetadataAllStreamReqPb &req, GetMetadataAllStreamRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    return clientWorkerSCSvc_->GetAllStreamMetadata(req, rsp);
}

Status MasterWorkerSCServiceImpl::UpdateTopoNotification(const UpdateTopoNotificationReq &req,
                                                         UpdateTopoNotificationRsp &rsp)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const auto &streamName = req.stream_name();
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("Stream:<%s> UpdateTopoNotification req:%s", streamName,
                                                LogHelper::IgnoreSensitive(req));
    LOG(INFO) << FormatString("worker(%s) received UpdateTopoNotification request, streamname: %s",
                              localWorkerAddress_.ToString(), streamName);
    INJECT_POINT("MasterWorkerSCServiceImpl.UpdateTopoNotification.begin");
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerSCSvc_->GetStreamManager(streamName, accessor),
                                     "worker get streamManager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    for (const auto &pub : req.pubs()) {
        const auto &workerAddr = pub.worker_addr();
        if (pub.is_close()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamManager->HandleClosedRemotePubNode(pub.force_close()),
                                             "streamManager HandleClosedRemotePubNode failed");
        } else {
            Status rc = streamManager->AddRemotePubNode(workerAddr);
            LOG_IF(INFO, rc.IsError()) << "streamManager AddRemotePubNode failed: " << rc.ToString();
            if (rc.IsError() && rc.GetCode() != K_DUPLICATED) {
                RETURN_STATUS(rc.GetCode(), rc.GetMsg());
            }
            StreamFields streamFields(pub.max_stream_size(), pub.page_size(), pub.auto_cleanup(),
                                      pub.retain_num_consumer(), pub.encrypt_stream(), pub.reserve_size(),
                                      pub.stream_mode());
            if (!streamFields.Empty()) {
                RETURN_IF_NOT_OK(streamManager->UpdateStreamFields(streamFields, true));
                // There should be existing consumers for it to receive the notification request,
                // update the reserved memory if applicable according to the page size
                LOG_IF_ERROR(clientWorkerSCSvc_->ReserveMemoryFromUsageMonitor(streamName, streamFields.pageSize_), "");
            }
        }
    }

    for (const auto &sub : req.subs()) {
        if (sub.is_close()) {
            Status rc = DelRemoteConsumer(streamManager, sub.consumer());
            LOG_IF(INFO, rc.IsError()) << "worker DelRemoteConsumer failed: " << rc.ToString();
            RETURN_IF_NOT_OK(rc);
        } else {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AddRemoteConsumer(streamManager, sub.consumer()),
                                             "worker AddRemoteConsumer failed");
        }
    }

    // This request only comes when we have enough consumers
    // Retain state will be set when producer is created
    if (req.retain_data() == RetainDataState::State::NOT_RETAIN) {
        streamManager->SetRetainData(req.retain_data());
    }
    LOG(INFO) << "worker UpdateTopoNotification done, streamname: " << streamName;
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
