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

/** Description: Implements a reusable RPC client for one worker address. */

#include "datasystem/client/transport/rpc/worker_rpc_client.h"

#include <algorithm>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {

Status GetRpcTimeout(int64_t maxRpcTimeoutMs, int32_t &rpcTimeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(maxRpcTimeoutMs > 0, K_INVALID, "RPC timeout must be positive");
    const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    CHECK_FAIL_RETURN_STATUS(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("API deadline exceeded, remaining %ld us.", remainingUs));
    const int64_t remainingMs = TimeoutDuration::CeilUsToMs(remainingUs);
    rpcTimeoutMs = static_cast<int32_t>(
        std::min({ remainingMs, maxRpcTimeoutMs, static_cast<int64_t>(MAX_RPC_TIMEOUT_MS) }));
    return Status::OK();
}

}  // namespace

std::atomic<uint32_t> WorkerRpcClient::nextConnectionGeneration_{ 1 };

WorkerRpcClient::WorkerRpcClient(HostPort workerAddress, std::shared_ptr<Signature> signature,
                                 BrpcChannelConfig channelConfig)
    : workerAddress_(std::move(workerAddress)),
      signature_(std::move(signature)),
      channelConfig_(std::move(channelConfig))
{
}

WorkerRpcClient::~WorkerRpcClient()
{
    Close();
}

Status WorkerRpcClient::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    channelConfig_.endpoint = workerAddress_.ToString();
    channelConfig_.max_retry = 0;
    auto channel = std::shared_ptr<brpc::Channel>(BrpcChannelFactory::Create(channelConfig_));
    CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                             "Failed to create routed worker brpc channel");
    auto controlStub = std::make_shared<WorkerOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    auto transportStub = std::make_shared<WorkerWorkerTransportService_BrpcGenericStub>(
        channel.get(), channelConfig_.timeout_ms);
    auto dataStub = std::make_shared<WorkerWorkerOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    auto masterStub =
        std::make_shared<master::MasterOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(controlStub != nullptr && transportStub != nullptr && dataStub != nullptr
                                 && masterStub != nullptr,
                             K_RUNTIME_ERROR, "Failed to create routed worker RPC stubs");
    channel_ = std::move(channel);
    controlStub_ = std::move(controlStub);
    transportStub_ = std::move(transportStub);
    dataStub_ = std::move(dataStub);
    masterStub_ = std::move(masterStub);
    connectionGeneration_ = nextConnectionGeneration_.fetch_add(1, std::memory_order_relaxed);
    alive_.store(true, std::memory_order_release);
    return Status::OK();
}

Status WorkerRpcClient::DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request,
                                          GetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads)
{
    return dataStub_->GetObjectRemote(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeQueryAndGet(const RpcOptions &options, const master::QueryAndGetReqPb &request,
                                            master::QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads)
{
    return masterStub_->QueryAndGet(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeCreate(const RpcOptions &options, const CreateReqPb &request,
                                       CreateRspPb &response)
{
    return controlStub_->Create(options, request, response);
}

Status WorkerRpcClient::DoInvokeSet(const RpcOptions &options, const PublishReqPb &request,
                                    PublishRspPb &response, const std::vector<MemView> &payloads)
{
    return controlStub_->Publish(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeDecreaseReference(const RpcOptions &options,
                                                  const DecreaseReferenceRequest &request,
                                                  DecreaseReferenceResponse &response)
{
    return controlStub_->DecreaseReference(options, request, response);
}

Status WorkerRpcClient::DoInvokeGetHashRing(const RpcOptions &options, const GetHashRingReqPb &request,
                                            GetHashRingRspPb &response)
{
    return controlStub_->GetHashRing(options, request, response);
}

Status WorkerRpcClient::InvokeGetObject(GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &response,
                                        std::vector<RpcMessage> &payloads)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker data client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.get_object_remote", []() { return Status::OK(); });
    Status rc = DoInvokeGetObject(options, request, response, payloads);
    return rc.IsError() ? WithRpcDiag(rc, "GetObjectRemote", workerAddress_) : Status::OK();
}

Status WorkerRpcClient::InvokeQueryAndGet(master::QueryAndGetReqPb &request, master::QueryAndGetRspPb &response,
                                          std::vector<RpcMessage> &payloads)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed master RPC client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.query_and_get", []() { return Status::OK(); });
    Status rc = DoInvokeQueryAndGet(options, request, response, payloads);
    return rc.IsError() ? WithRpcDiag(rc, "QueryAndGet", workerAddress_) : Status::OK();
}

Status WorkerRpcClient::InvokeCreate(int64_t subTimeoutMs, CreateReqPb &request, CreateRspPb &response,
                                     uint32_t &workerVersion)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms), rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_CREATE_OBJECT);
    Status rc = DoInvokeCreate(options, request, response);
    if (rc.IsError()) {
        return WithRpcDiag(rc, "Create", workerAddress_);
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
}

Status WorkerRpcClient::InvokeSet(int64_t subTimeoutMs, PublishReqPb &request,
                                  const std::vector<MemView> &payloads, PublishRspPb &response,
                                  uint32_t &workerVersion)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms), rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_PUBLISH_OBJECT);
    INJECT_POINT("WorkerRpcClient.InvokeSet.beforeRpc");
    Status rc = DoInvokeSet(options, request, response, payloads);
    if (rc.IsError()) {
        if (request.is_retry() && request.is_seal() && rc.GetCode() == K_OC_ALREADY_SEALED) {
            workerVersion = connectionGeneration_;
            perfPoint.Record();
            return Status::OK();
        }
        return WithRpcDiag(rc, "Publish", workerAddress_);
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
}

Status WorkerRpcClient::InvokeDecreaseReference(const TransportRequestContext &context, const ShmKey &shmId)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    CHECK_FAIL_RETURN_STATUS(!context.clientId.empty(), K_INVALID, "DecreaseReference client ID must not be empty");
    CHECK_FAIL_RETURN_STATUS(!shmId.Empty(), K_INVALID, "DecreaseReference shm ID must not be empty");
    DecreaseReferenceRequest request;
    request.set_client_id(context.clientId);
    request.add_object_keys(shmId);
    request.set_token(context.token);
    request.set_tenant_id(context.tenantId);
    request.set_is_routed(true);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    DecreaseReferenceResponse response;
    Status rc = DoInvokeDecreaseReference(options, request, response);
    if (rc.IsError()) {
        return WithRpcDiag(rc, "DecreaseReference", workerAddress_);
    }
    return Status(static_cast<StatusCode>(response.error().error_code()), response.error().error_msg());
}

Status WorkerRpcClient::ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    UrmaHandshakeReqPb request;
    RETURN_IF_NOT_OK(ConstructHandshakePb(workerAddress_.ToString(), request, ""));
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = transportStub_->WorkerWorkerExchangeUrmaConnectInfo(options, request, response);
    return rc.IsError() ? WithRpcDiag(rc, "WorkerWorkerExchangeUrmaConnectInfo", workerAddress_) : Status::OK();
}

Status WorkerRpcClient::InvokeGetHashRing(uint64_t currentVersion, GetHashRingRspPb &response)
{
    if (!IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "Routed worker RPC client is not initialized");
    }
    CHECK_FAIL_RETURN_STATUS(channelConfig_.timeout_ms > 0, K_INVALID, "RPC timeout must be positive");
    GetHashRingReqPb request;
    request.set_version(currentVersion);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(static_cast<int32_t>(
        std::min<int64_t>(channelConfig_.timeout_ms, static_cast<int64_t>(MAX_RPC_TIMEOUT_MS))));
    Status rc = DoInvokeGetHashRing(options, request, response);
    return rc.IsError() ? WithRpcDiag(rc, "GetHashRing", workerAddress_) : Status::OK();
}

bool WorkerRpcClient::IsAlive() const
{
    return alive_.load(std::memory_order_acquire) && channel_ != nullptr && controlStub_ != nullptr
           && transportStub_ != nullptr && dataStub_ != nullptr && masterStub_ != nullptr;
}

void WorkerRpcClient::Close()
{
    alive_.store(false, std::memory_order_release);
    masterStub_.reset();
    dataStub_.reset();
    transportStub_.reset();
    controlStub_.reset();
    channel_.reset();
}

}  // namespace client
}  // namespace datasystem
