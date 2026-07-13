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

#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.brpc.stub.pb.h"
#include "datasystem/protos/worker_object.brpc.stub.pb.h"

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

WorkerRpcClient::WorkerRpcClient(HostPort workerAddress, std::shared_ptr<Signature> signature,
                                 BrpcChannelConfig channelConfig)
    : workerAddress_(std::move(workerAddress)), signature_(std::move(signature)),
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
    // TransportLayer owns the single reconnect retry and must re-authenticate before that retry.
    channelConfig_.max_retry = 0;
    auto channel = std::shared_ptr<brpc::Channel>(BrpcChannelFactory::Create(channelConfig_));
    CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                             "Failed to create routed worker brpc channel");
    auto transportStub = std::make_shared<WorkerWorkerTransportService_BrpcGenericStub>(
        channel.get(), channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(transportStub != nullptr, K_RUNTIME_ERROR,
                             "Failed to create routed worker transport stub");
    auto dataStub = std::make_shared<WorkerWorkerOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(dataStub != nullptr, K_RUNTIME_ERROR, "Failed to create routed worker data stub");
    auto masterStub =
        std::make_shared<master::MasterOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(masterStub != nullptr, K_RUNTIME_ERROR, "Failed to create routed master OC stub");
    channel_ = std::move(channel);
    transportStub_ = std::move(transportStub);
    dataStub_ = std::move(dataStub);
    masterStub_ = std::move(masterStub);
    alive_.store(true, std::memory_order_release);
    return Status::OK();
}

Status WorkerRpcClient::DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request,
                                          GetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads)
{
    return dataStub_->GetObjectRemote(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeQueryAndGet(const RpcOptions &options, const master::QueryAndGetReqPb &request,
                                            master::QueryAndGetRspPb &response)
{
    return masterStub_->QueryAndGet(options, request, response);
}

Status WorkerRpcClient::InvokeGetObject(GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &response,
                                        std::vector<RpcMessage> &payloads)
{
    if (!IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "Routed worker data client is not initialized");
    }
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = DoInvokeGetObject(options, request, response, payloads);
    return rc.IsError() ? WithRpcDiag(rc, "GetObjectRemote", workerAddress_) : Status::OK();
}

Status WorkerRpcClient::InvokeQueryAndGet(master::QueryAndGetReqPb &request, master::QueryAndGetRspPb &response)
{
    if (!IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "Routed master RPC client is not initialized");
    }
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = DoInvokeQueryAndGet(options, request, response);
    return rc.IsError() ? WithRpcDiag(rc, "QueryAndGet", workerAddress_) : Status::OK();
}

Status WorkerRpcClient::ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &response)
{
    if (!IsAlive() || transportStub_ == nullptr) {
        return Status(K_RPC_UNAVAILABLE, "Routed worker RPC client is not initialized");
    }
    UrmaHandshakeReqPb request;
    RETURN_IF_NOT_OK(ConstructHandshakePb(workerAddress_.ToString(), request, ""));
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = transportStub_->WorkerWorkerExchangeUrmaConnectInfo(options, request, response);
    return rc.IsError() ? WithRpcDiag(rc, "WorkerWorkerExchangeUrmaConnectInfo", workerAddress_) : Status::OK();
}

bool WorkerRpcClient::IsAlive() const
{
    return alive_.load(std::memory_order_acquire) && channel_ != nullptr && transportStub_ != nullptr
           && dataStub_ != nullptr && masterStub_ != nullptr;
}

void WorkerRpcClient::Close()
{
    alive_.store(false, std::memory_order_release);
    masterStub_.reset();
    dataStub_.reset();
    transportStub_.reset();
    channel_.reset();
}

}  // namespace client
}  // namespace datasystem
