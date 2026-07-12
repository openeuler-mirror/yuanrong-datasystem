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

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/object_posix.brpc.stub.pb.h"
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

std::atomic<uint32_t> WorkerRpcClient::nextConnectionGeneration_{ 1 };

WorkerRpcClient::WorkerRpcClient(HostPort workerAddress, std::shared_ptr<ClientRequestAuth> auth,
                                 BrpcChannelConfig channelConfig)
    : workerAddress_(std::move(workerAddress)), auth_(std::move(auth)), channelConfig_(std::move(channelConfig))
{
}

WorkerRpcClient::~WorkerRpcClient()
{
    Close();
}

Status WorkerRpcClient::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(auth_);
    channelConfig_.endpoint = workerAddress_.ToString();
    // TransportLayer owns the single reconnect retry and must re-authenticate before that retry.
    channelConfig_.max_retry = 0;
    auto channel = std::shared_ptr<brpc::Channel>(BrpcChannelFactory::Create(channelConfig_));
    CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                             "Failed to create routed worker brpc channel");
    auto stub = CreateControlStub(channel, channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(stub != nullptr, K_RUNTIME_ERROR, "Failed to create routed worker OC stub");
    auto transportStub = CreateTransportStub(channel, channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(transportStub != nullptr, K_RUNTIME_ERROR,
                             "Failed to create routed worker transport stub");
    channel_ = std::move(channel);
    stub_ = std::move(stub);
    transportStub_ = std::move(transportStub);
    connectionGeneration_ = nextConnectionGeneration_.fetch_add(1, std::memory_order_relaxed);
    alive_.store(true, std::memory_order_release);
    return Status::OK();
}

std::shared_ptr<WorkerOCService_BrpcGenericStub> WorkerRpcClient::CreateControlStub(
    const std::shared_ptr<brpc::Channel> &channel, int32_t timeoutMs)
{
    return std::make_shared<WorkerOCService_BrpcGenericStub>(channel.get(), timeoutMs);
}

std::shared_ptr<WorkerWorkerTransportService_BrpcGenericStub> WorkerRpcClient::CreateTransportStub(
    const std::shared_ptr<brpc::Channel> &channel, int32_t timeoutMs)
{
    return std::make_shared<WorkerWorkerTransportService_BrpcGenericStub>(channel.get(), timeoutMs);
}

Status WorkerRpcClient::DoQueryObjectSizes(const RpcOptions &options, const GetObjMetaInfoReqPb &request,
                                           GetObjMetaInfoRspPb &response)
{
    return stub_->GetObjMetaInfo(options, request, response);
}

Status WorkerRpcClient::DoInvokeGet(const RpcOptions &options, const GetReqPb &request, GetRspPb &response,
                                    std::vector<RpcMessage> &payloads)
{
    return stub_->Get(options, request, response, payloads);
}

Status WorkerRpcClient::QueryObjectSizes(const std::vector<std::string> &objectKeys,
                                         std::vector<uint64_t> &objectSizes)
{
    objectSizes.clear();
    if (!IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "Routed worker RPC client is not initialized");
    }
    GetObjMetaInfoReqPb request;
    *request.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(auth_->Authenticate(request));
    GetObjMetaInfoRspPb response;
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = DoQueryObjectSizes(options, request, response);
    if (rc.IsError()) {
        return WithRpcDiag(rc, "GetObjMetaInfo", workerAddress_);
    }
    objectSizes.reserve(response.objs_meta_info_size());
    for (const auto &meta : response.objs_meta_info()) {
        objectSizes.push_back(meta.obj_size());
    }
    return Status::OK();
}

Status WorkerRpcClient::InvokeGet(int64_t subTimeoutMs, GetReqPb &request, GetRspPb &response,
                                  std::vector<RpcMessage> &payloads, uint32_t &workerVersion)
{
    if (!IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "Routed worker RPC client is not initialized");
    }
    const int64_t requestedTimeout = std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms);
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(requestedTimeout, rpcTimeout));
    RETURN_IF_NOT_OK(auth_->Authenticate(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_GET_OBJECT);
    Status rc = DoInvokeGet(options, request, response, payloads);
    if (rc.IsError()) {
        return WithRpcDiag(rc, "Get", workerAddress_);
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
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
    return alive_.load(std::memory_order_acquire) && channel_ != nullptr && stub_ != nullptr;
}

void WorkerRpcClient::Close()
{
    alive_.store(false, std::memory_order_release);
    transportStub_.reset();
    stub_.reset();
    channel_.reset();
}

}  // namespace client
}  // namespace datasystem
