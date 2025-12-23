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
 * Description: Implement client to worker api.
 */
#include "datasystem/client/stream_cache/client_worker_api.h"
#include <cstdint>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/plugin_generator/zmq_rpc_generator.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/rpc_option.pb.h"
#include "datasystem/protos/stream_posix.pb.h"
#include "datasystem/stream/stream_config.h"

namespace datasystem {
namespace client {
namespace stream_cache {
ClientWorkerApi::ClientWorkerApi(const HostPort &hostPort, RpcCredential cred, SensitiveValue token,
                                 Signature *signature, std::string tenantId)
    : ClientWorkerCommonApi(hostPort, cred, HeartbeatType::RPC_HEARTBEAT, std::move(token), signature,
                            std::move(tenantId))
{
}

Status ClientWorkerApi::Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs)
{
    RETURN_IF_NOT_OK(ClientWorkerCommonApi::Init(requestTimeoutMs, connectTimeoutMs));
    std::shared_ptr<RpcChannel> channel;
    channel = std::make_shared<RpcChannel>(hostPort_, cred_);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Setting client-worker communication via Unix socket : %s",
                                              (GetShmEnabled() ? "true" : "false"));
    // We will enable uds after handshaking with the worker.
    if (GetShmEnabled()) {
        channel->SetServiceUdsEnabled(ClientWorkerSCService_Stub::FullServiceName(),
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
    }
    rpcSession_ = std::make_unique<ClientWorkerSCService_Stub>(channel);
    return Status::OK();
}

Status ClientWorkerApi::CreateProducer(const std::string &streamName, const std::string &producerId,
                                       const ProducerConf &producerConf, ShmView &outPageView,
                                       DataVerificationHeader::SenderProducerNo &senderProducerNo,
                                       bool &enableStreamDataVerification, uint64_t &streamNo,
                                       bool &enableSharedPage, uint64_t &sharedPageSize, ShmView &outStreamMetaView)
{
    CreateProducerReqPb req;
    req.set_stream_name(streamName);
    req.set_page_size(producerConf.pageSize);
    req.set_client_id(GetClientId());
    req.set_producer_id(producerId);
    req.set_max_stream_size(producerConf.maxStreamSize);
    req.set_auto_cleanup(producerConf.autoCleanup);
    req.set_retain_num_consumer(producerConf.retainForNumConsumers);
    req.set_encrypt_stream(producerConf.encryptStream);
    req.set_reserve_size(producerConf.reserveSize);
    req.set_stream_mode(producerConf.streamMode);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));

    PerfPoint point(PerfKey::RPC_WORKER_CREATE_PRODUCER);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    CreateProducerRspPb rsp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK_EXCEPT(rpcSession_->CreateProducer(opts, req, rsp), StatusCode::K_DUPLICATED);
    point.Record();

    outPageView.off = static_cast<ptrdiff_t>(rsp.page_view().offset());
    outPageView.sz = rsp.page_view().size();
    outPageView.mmapSz = rsp.page_view().mmap_size();
    outPageView.fd = rsp.page_view().fd();

    outStreamMetaView.sz = rsp.stream_meta_view().size();
    outStreamMetaView.mmapSz = rsp.stream_meta_view().mmap_size();
    outStreamMetaView.fd = rsp.stream_meta_view().fd();
    outStreamMetaView.off = static_cast<ptrdiff_t>(rsp.stream_meta_view().offset());

    senderProducerNo = rsp.sender_producer_no();
    enableStreamDataVerification = rsp.enable_data_verification();
    streamNo = rsp.stream_no();
    enableSharedPage = rsp.enable_shared_page();
    sharedPageSize = rsp.shared_page_size();

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, P:%s] Create producer success.", LogPrefix(), streamName,
                                              producerId);
    return Status::OK();
}

Status ClientWorkerApi::Subscribe(const std::string &streamName, const std::string &consumerId,
                                  const SubscriptionConfig &config, SubscribeRspPb &rsp)
{
    auto configReqPtr = std::make_unique<SubscriptionConfigPb>();
    configReqPtr->set_subscription_name(config.subscriptionName);
    configReqPtr->set_subscription_type(SubscriptionTypePb(config.subscriptionType));
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    SubscribeReqPb req;
    req.set_stream_name(streamName);
    req.set_allocated_subscription_config(configReqPtr.release());
    req.set_client_id(GetClientId());
    req.set_consumer_id(consumerId);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));

    PerfPoint point(PerfKey::RPC_WORKER_CREATE_SUBSCRIBE);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK_EXCEPT(rpcSession_->Subscribe(opts, req, rsp), StatusCode::K_DUPLICATED);
    point.Record();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s] Create consumer success.", LogPrefix(), streamName,
                                              consumerId);
    return Status::OK();
}

std::pair<int32_t, int32_t> ClientWorkerApi::GetRpcTimeout(int64_t requestedTimeout, int32_t defaultRpcTimeout)
{
    // User do not want to wait, but rpc request still takes time
    if (requestedTimeout == 0) {
        return { defaultRpcTimeout, requestedTimeout };
    }

    // Make sure the timeout value does not overflow
    if (requestedTimeout >= INT_MAX || requestedTimeout < 0) {
        requestedTimeout = INT_MAX;
    }

    // Adjust time already spent in the client, so less time for the worker
    auto adjustedTimeout = ClientGetRequestTimeout(requestedTimeout);

    // Include request processing because this is a rpc round trip
    auto rpcTimeout = (defaultRpcTimeout + adjustedTimeout < INT_MAX) ? defaultRpcTimeout + adjustedTimeout : INT_MAX;
    return { rpcTimeout, adjustedTimeout };
}

Status ClientWorkerApi::DeleteStream(const std::string &streamName)
{
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    DeleteStreamReqPb req;
    DeleteStreamRspPb rsp;
    req.set_stream_name(streamName);
    req.set_client_id(GetClientId());
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));

    PerfPoint point(PerfKey::RPC_WORKER_DELETE_STREAM);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->DeleteStream(opts, req, rsp));
    point.Record();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream success.", LogPrefix(), streamName);
    return Status::OK();
}

Status ClientWorkerApi::QueryGlobalProducersNum(const std::string &streamName, uint64_t &producerNum)
{
    LOG(INFO) << FormatString("[%s, Stream:%s], Start to query global producer count.", LogPrefix(), streamName);
    QueryGlobalNumReqPb req;
    QueryGlobalNumRsqPb rsp;
    req.set_stream_name(streamName);
    req.set_client_id(GetClientId());

    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->QueryGlobalProducersNum(req, rsp));
    producerNum = rsp.global_count();
    return Status::OK();
}

Status ClientWorkerApi::QueryGlobalConsumersNum(const std::string &streamName, uint64_t &consumerNum)
{
    LOG(INFO) << FormatString("[%s, Stream:%s], Start to query global consumer count.", LogPrefix(), streamName);
    QueryGlobalNumReqPb req;
    QueryGlobalNumRsqPb rsp;
    req.set_stream_name(streamName);

    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    req.set_client_id(GetClientId());
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->QueryGlobalConsumersNum(req, rsp));
    consumerNum = rsp.global_count();
    return Status::OK();
}

Status ClientWorkerApi::ResetStreams(const std::vector<std::string> &streamNames)
{
    ResetOrResumeStreamsReqPb req;
    ResetOrResumeStreamsRspPb rsp;

    RpcOptions opts;
    opts.SetTimeout(rpcTimeoutMs_);
    for (auto &streamName : streamNames) {
        req.add_stream_names(streamName);
    }
    req.set_client_id(GetClientId());

    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));

    PerfPoint point(PerfKey::RPC_WORKER_RESET_STREAM);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            requestTimeoutMs_,
            [this, &opts, &req, &rsp](int32_t) {
                RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
                return rpcSession_->ResetStreams(opts, req, rsp);
            },
            []() { return Status::OK(); }, { K_RPC_CANCELLED, K_RPC_DEADLINE_EXCEEDED, K_RPC_UNAVAILABLE }),
        "Reset streams on worker error.");
    point.Record();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Reset streams on worker success.", LogPrefix());
    return Status::OK();
}

Status ClientWorkerApi::ResumeStreams(const std::vector<std::string> &streamNames)
{
    ResetOrResumeStreamsReqPb req;
    ResetOrResumeStreamsRspPb rsp;

    RpcOptions opts;
    opts.SetTimeout(rpcTimeoutMs_);
    for (auto &streamName : streamNames) {
        req.add_stream_names(streamName);
    }
    req.set_client_id(GetClientId());
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));

    PerfPoint point(PerfKey::RPC_WORKER_RESUME_STREAM);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            requestTimeoutMs_,
            [this, &opts, &req, &rsp](int32_t) {
                RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
                return rpcSession_->ResumeStreams(opts, req, rsp);
            },
            []() { return Status::OK(); }, { K_RPC_CANCELLED, K_RPC_DEADLINE_EXCEEDED, K_RPC_UNAVAILABLE }),
        "Resume streams on worker error.");
    point.Record();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Resume streams on worker success.", LogPrefix());
    return Status::OK();
}

std::string ClientWorkerApi::LogPrefix() const
{
    return FormatString("ClientWorkerApi, EndPoint:%s", hostPort_.ToString());
}

std::string ClientWorkerApi::GetClientId()
{
    return ClientWorkerCommonApi::GetClientId();
}
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
