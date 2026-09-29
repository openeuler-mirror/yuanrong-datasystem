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

#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
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

void RecordRpcTotalLatency(LatencySummaryPhase phase, bool traceEnabled,
                           const std::chrono::steady_clock::time_point &start)
{
    (void)Trace::Instance().ConsumeLastRpcCommUs();
    if (traceEnabled) {
        const auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start).count();
        Trace::Instance().AddDownstreamPhase(phase, static_cast<uint64_t>(elapsedUs));
    }
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
    auto workerStub = std::make_shared<WorkerService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    auto controlStub = std::make_shared<WorkerOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    auto transportStub = std::make_shared<WorkerWorkerTransportService_BrpcGenericStub>(
        channel.get(), channelConfig_.timeout_ms);
    auto dataStub = std::make_shared<WorkerWorkerOCService_BrpcGenericStub>(channel.get(), channelConfig_.timeout_ms);
    CHECK_FAIL_RETURN_STATUS(workerStub != nullptr && controlStub != nullptr && transportStub != nullptr
                                 && dataStub != nullptr,
                             K_RUNTIME_ERROR, "Failed to create routed worker RPC stubs");
    channel_ = std::move(channel);
    workerStub_ = std::move(workerStub);
    controlStub_ = std::move(controlStub);
    transportStub_ = std::move(transportStub);
    dataStub_ = std::move(dataStub);
    connectionGeneration_ = nextConnectionGeneration_.fetch_add(1, std::memory_order_relaxed);
    alive_.store(true, std::memory_order_release);
    return Status::OK();
}

Status WorkerRpcClient::DoInvokeGetObject(const RpcOptions &options, const GetObjectRemoteReqPb &request,
                                          GetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads)
{
    return dataStub_->GetObjectRemote(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeClientGet(const RpcOptions &options, const GetReqPb &request, GetRspPb &response,
                                          std::vector<RpcMessage> &payloads)
{
    return controlStub_->Get(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeBatchGetObject(const RpcOptions &options, const BatchGetObjectRemoteReqPb &request,
                                               BatchGetObjectRemoteRspPb &response, std::vector<RpcMessage> &payloads)
{
    return dataStub_->BatchGetObjectRemote(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeQueryAndGet(const RpcOptions &options, const QueryAndGetReqPb &request,
                                            QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads)
{
    return controlStub_->QueryAndGet(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeExist(const RpcOptions &options, const ExistReqPb &request, ExistRspPb &response)
{
    return controlStub_->Exist(options, request, response);
}

Status WorkerRpcClient::DoInvokeGetHashRing(const RpcOptions &options, const GetHashRingReqPb &request,
                                            GetHashRingRspPb &response)
{
    return controlStub_->GetHashRing(options, request, response);
}

Status WorkerRpcClient::DoInvokeGetSocketPath(const RpcOptions &options, const GetSocketPathReqPb &request,
                                              GetSocketPathRspPb &response)
{
    return workerStub_->GetSocketPath(options, request, response);
}

Status WorkerRpcClient::DoInvokeRegisterShmClient(const RpcOptions &options, const RegisterClientReqPb &request,
                                                  RegisterClientRspPb &response)
{
    return workerStub_->RegisterClient(options, request, response);
}

Status WorkerRpcClient::DoInvokeGetClientFd(const RpcOptions &options, const GetClientFdReqPb &request,
                                            GetClientFdRspPb &response)
{
    return workerStub_->GetClientFd(options, request, response);
}

Status WorkerRpcClient::DoInvokeShmHeartbeat(const RpcOptions &options, const HeartbeatReqPb &request,
                                             HeartbeatRspPb &response)
{
    return workerStub_->Heartbeat(options, request, response);
}

Status WorkerRpcClient::DoInvokeDisconnectShmClient(const RpcOptions &options, const DisconnectClientReqPb &request,
                                                    DisconnectClientRspPb &response)
{
    return workerStub_->DisconnectClient(options, request, response);
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

Status WorkerRpcClient::DoInvokeMultiCreate(const RpcOptions &options, const MultiCreateReqPb &request,
                                            MultiCreateRspPb &response)
{
    return controlStub_->MultiCreate(options, request, response);
}

Status WorkerRpcClient::DoInvokeMultiSet(const RpcOptions &options, const MultiPublishReqPb &request,
                                         MultiPublishRspPb &response, const std::vector<MemView> &payloads)
{
    return controlStub_->MultiPublish(options, request, response, payloads);
}

Status WorkerRpcClient::DoInvokeDecreaseReference(const RpcOptions &options,
                                                  const DecreaseReferenceRequest &request,
                                                  DecreaseReferenceResponse &response)
{
    return controlStub_->DecreaseReference(options, request, response);
}

Status WorkerRpcClient::InvokeGetObject(GetObjectRemoteReqPb &request, GetObjectRemoteRspPb &response,
                                        std::vector<RpcMessage> &payloads)
{
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed worker data client is not initialized"),
            RpcDiagnosticInfo{ "GetObjectRemote", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    int32_t rpcTimeout;
    const auto timeoutRc = GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "GetObjectRemote", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.get_object_remote", []() { return Status::OK(); });
    Status rc = DoInvokeGetObject(options, request, response, payloads);
    if (response.has_ub_health_summary()) {
        ObserveUbHealthSummary(response.ub_health_summary());
    }
    if (rc.IsError()) {
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "GetObjectRemote", "", workerAddress_.ToString() },
            "rpc_return");
    }
    return Status::OK();
}

Status WorkerRpcClient::InvokeClientGet(GetReqPb &request, GetRspPb &response, std::vector<RpcMessage> &payloads)
{
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed WorkerOCService client is not initialized"),
            RpcDiagnosticInfo{ "WorkerOCService.Get", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    CHECK_FAIL_RETURN_STATUS(!request.client_id().empty(), K_INVALID, "WorkerOCService Get client ID is empty");
    int32_t rpcTimeout;
    const auto timeoutRc = GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "WorkerOCService.Get", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.worker_oc_get", []() { return Status::OK(); });
    Status rc = DoInvokeClientGet(options, request, response, payloads);
    if (response.has_ub_health_summary()) {
        ObserveUbHealthSummary(response.ub_health_summary());
    }
    if (rc.IsError()) {
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "WorkerOCService.Get", "", workerAddress_.ToString() },
            "rpc_return");
    }
    return Status::OK();
}

Status WorkerRpcClient::InvokeBatchGetObject(BatchGetObjectRemoteReqPb &request, BatchGetObjectRemoteRspPb &response,
                                             std::vector<RpcMessage> &payloads)
{
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed worker data client is not initialized"),
            RpcDiagnosticInfo{ "BatchGetObjectRemote", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    CHECK_FAIL_RETURN_STATUS(request.requests_size() > 0, K_INVALID, "BatchGetObjectRemote request is empty");
    int32_t rpcTimeout;
    const auto timeoutRc = GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "BatchGetObjectRemote", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.batch_get_object_remote", []() { return Status::OK(); });
    Status rc = DoInvokeBatchGetObject(options, request, response, payloads);
    if (response.has_ub_health_summary()) {
        ObserveUbHealthSummary(response.ub_health_summary());
    }
    if (rc.IsError()) {
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "BatchGetObjectRemote", "", workerAddress_.ToString() },
            "rpc_return");
    }
    return Status::OK();
}

Status WorkerRpcClient::InvokeQueryAndGet(QueryAndGetReqPb &request, QueryAndGetRspPb &response,
                                          std::vector<RpcMessage> &payloads, bool *rpcDispatched)
{
    if (rpcDispatched != nullptr) {
        *rpcDispatched = false;
    }
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed worker RPC client is not initialized"),
            RpcDiagnosticInfo{ "QueryAndGet", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    int32_t rpcTimeout;
    const auto timeoutRc = GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "QueryAndGet", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.query_and_get", []() { return Status::OK(); });
    if (rpcDispatched != nullptr) {
        *rpcDispatched = true;
    }
    INJECT_POINT("client.transport.query_and_get.after_dispatch");
    Status rc = DoInvokeQueryAndGet(options, request, response, payloads);
    if (response.has_ub_health_summary()) {
        ObserveUbHealthSummary(response.ub_health_summary());
    }
    if (rc.IsError()) {
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "QueryAndGet", "", workerAddress_.ToString() },
            "rpc_return");
    }
    return Status::OK();
}

Status WorkerRpcClient::InvokeExist(int64_t subTimeoutMs, ExistReqPb &request, ExistRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(subTimeoutMs, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_EXIST);
    Status rc = DoInvokeExist(options, request, response);
    perfPoint.Record();
    if (rc.IsError()) {
        return WithRpcDiag(rc, "Exist", workerAddress_);
    }
    if (!response.redirect_extra().empty()) {
        return Status(K_NOT_OWNER, "Exist keys redirected to new owners").WithExtra(response.redirect_extra());
    }
    return Status::OK();
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
    options.SetTimeout(channelConfig_.timeout_ms);
    return WithRpcDiag(DoInvokeGetHashRing(options, request, response), "GetHashRing", workerAddress_);
}

Status WorkerRpcClient::InvokeGetSocketPath(GetSocketPathReqPb &request, GetSocketPathRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed WorkerService client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    return WithRpcDiag(DoInvokeGetSocketPath(options, request, response), "GetSocketPath", workerAddress_);
}

Status WorkerRpcClient::InvokeRegisterShmClient(RegisterClientReqPb &request, RegisterClientRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed WorkerService client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.register_shm_client", []() { return Status::OK(); });
    return WithRpcDiag(DoInvokeRegisterShmClient(options, request, response), "RegisterClient", workerAddress_);
}

Status WorkerRpcClient::InvokeGetClientFd(GetClientFdReqPb &request, GetClientFdRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed WorkerService client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    INJECT_POINT("client.transport.get_client_fd", []() { return Status::OK(); });
    return WithRpcDiag(DoInvokeGetClientFd(options, request, response), "GetClientFd", workerAddress_);
}

Status WorkerRpcClient::InvokeShmHeartbeat(HeartbeatReqPb &request, HeartbeatRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed WorkerService client is not initialized");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    constexpr int32_t SHM_MAINTENANCE_TIMEOUT_MS = 1000;
    options.SetTimeout(std::clamp<int64_t>(channelConfig_.timeout_ms, 1, SHM_MAINTENANCE_TIMEOUT_MS));
    INJECT_POINT("client.transport.shm_heartbeat", []() { return Status::OK(); });
    return WithRpcDiag(DoInvokeShmHeartbeat(options, request, response), "Heartbeat", workerAddress_);
}

Status WorkerRpcClient::InvokeDisconnectShmClient(DisconnectClientReqPb &request, DisconnectClientRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed WorkerService client is not initialized");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    constexpr int32_t SHM_DISCONNECT_TIMEOUT_MS = 1000;
    options.SetTimeout(std::clamp<int64_t>(channelConfig_.timeout_ms, 1, SHM_DISCONNECT_TIMEOUT_MS));
    return WithRpcDiag(DoInvokeDisconnectShmClient(options, request, response), "DisconnectClient", workerAddress_);
}

Status WorkerRpcClient::InvokeCreate(int64_t subTimeoutMs, CreateReqPb &request, CreateRspPb &response,
                                     uint32_t &workerVersion)
{
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed worker RPC client is not initialized"),
            RpcDiagnosticInfo{ "Create", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    int32_t rpcTimeout;
    const auto timeoutRc =
        GetRpcTimeout(std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms), rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "Create", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_CREATE_OBJECT);
    const bool traceEnabled = IsClientLatencyTraceActive();
    const auto rpcStart = traceEnabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    Status rc = DoInvokeCreate(options, request, response);
    RecordRpcTotalLatency(LatencySummaryPhase::CLIENT_RPC_CREATE_TOTAL, traceEnabled, rpcStart);
    if (rc.IsError()) {
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "Create", "", workerAddress_.ToString() },
            "rpc_return");
    }
    if (response.has_worker_redirect()) {
        return Status(K_SCALE_DOWN, "Worker rejected write before execution")
            .WithExtra(response.worker_redirect().SerializeAsString());
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
}

Status WorkerRpcClient::InvokeSet(int64_t subTimeoutMs, PublishReqPb &request,
                                  const std::vector<MemView> &payloads, PublishRspPb &response,
                                  uint32_t &workerVersion)
{
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed worker RPC client is not initialized"),
            RpcDiagnosticInfo{ "Publish", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    int32_t rpcTimeout;
    const auto timeoutRc =
        GetRpcTimeout(std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms), rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "Publish", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_PUBLISH_OBJECT);
    INJECT_POINT("WorkerRpcClient.InvokeSet.beforeRpc");
    const bool traceEnabled = IsClientLatencyTraceActive();
    const auto rpcStart = traceEnabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{};
    Status rc = DoInvokeSet(options, request, response, payloads);
    if (response.has_ub_health_summary()) {
        ObserveUbHealthSummary(response.ub_health_summary());
    }
    RecordRpcTotalLatency(LatencySummaryPhase::CLIENT_RPC_PUBLISH_TOTAL, traceEnabled, rpcStart);
    if (rc.IsError()) {
        if (request.is_retry() && request.is_seal() && rc.GetCode() == K_OC_ALREADY_SEALED) {
            workerVersion = connectionGeneration_;
            perfPoint.Record();
            return Status::OK();
        }
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "Publish", "", workerAddress_.ToString() },
            "rpc_return");
    }
    if (response.has_worker_redirect()) {
        return Status(K_SCALE_DOWN, "Worker rejected write before execution")
            .WithExtra(response.worker_redirect().SerializeAsString());
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
}

Status WorkerRpcClient::InvokeMultiCreate(int64_t subTimeoutMs, MultiCreateReqPb &request,
                                          MultiCreateRspPb &response, uint32_t &workerVersion)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms), rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::CLIENT_MULTI_CREATE_IPC);
    INJECT_POINT("WorkerRpcClient.InvokeMultiCreate.beforeRpc");
    Status rc = DoInvokeMultiCreate(options, request, response);
    if (rc.IsError()) {
        return WithRpcDiag(rc, "MultiCreate", workerAddress_);
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
}

Status WorkerRpcClient::InvokeMultiSet(int64_t subTimeoutMs, MultiPublishReqPb &request,
                                       const std::vector<MemView> &payloads, MultiPublishRspPb &response,
                                       uint32_t &workerVersion)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(std::max<int64_t>(subTimeoutMs, channelConfig_.timeout_ms), rpcTimeout));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_MULTI_PUBLISH_OBJECT);
    INJECT_POINT("WorkerRpcClient.InvokeMultiSet.beforeRpc");
    Status rc = DoInvokeMultiSet(options, request, response, payloads);
    if (response.has_ub_health_summary()) {
        ObserveUbHealthSummary(response.ub_health_summary());
    }
    if (rc.IsError()) {
        return WithRpcDiag(rc, "MultiPublish", workerAddress_);
    }
    workerVersion = connectionGeneration_;
    perfPoint.Record();
    return Status::OK();
}

Status WorkerRpcClient::InvokeDecreaseReference(const TransportRequestContext &context, const ShmKey &shmId,
                                                bool delayRelease)
{
    return InvokeDecreaseReferences(context, { shmId }, delayRelease);
}

Status WorkerRpcClient::InvokeDecreaseReferences(const TransportRequestContext &context,
                                                 const std::vector<ShmKey> &shmIds, bool delayRelease)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Routed worker RPC client is not initialized");
    CHECK_FAIL_RETURN_STATUS(!context.clientId.empty(), K_INVALID, "DecreaseReference client ID must not be empty");
    CHECK_FAIL_RETURN_STATUS(!shmIds.empty(), K_INVALID, "DecreaseReference shm IDs must not be empty");
    DecreaseReferenceRequest request;
    request.set_client_id(context.clientId);
    for (const auto &shmId : shmIds) {
        CHECK_FAIL_RETURN_STATUS(!shmId.Empty(), K_INVALID, "DecreaseReference shm ID must not be empty");
        request.add_object_keys(shmId);
    }
    request.set_token(context.token);
    request.set_tenant_id(context.tenantId);
    request.set_is_routed(true);
    request.set_delay_release(delayRelease);
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
    if (!IsAlive()) {
        return WithRpcDiag(
            Status(K_RPC_UNAVAILABLE, __LINE__, __FILE__,
                   "Routed worker RPC client is not initialized"),
            RpcDiagnosticInfo{ "WorkerWorkerExchangeUrmaConnectInfo", "", workerAddress_.ToString() },
            "before_rpc.check_client");
    }

    UrmaHandshakeReqPb request;
    RETURN_IF_NOT_OK(ConstructHandshakePb(workerAddress_.ToString(), request, ""));
    int32_t rpcTimeout;
    const auto timeoutRc = GetRpcTimeout(channelConfig_.timeout_ms, rpcTimeout);
    if (timeoutRc.IsError()) {
        return WithRpcDiag(
            timeoutRc, RpcDiagnosticInfo{ "WorkerWorkerExchangeUrmaConnectInfo", "", workerAddress_.ToString() },
            "before_rpc.timeout");
    }

    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = transportStub_->WorkerWorkerExchangeUrmaConnectInfo(options, request, response);
    if (rc.IsError()) {
        return WithRpcDiag(
            rc, RpcDiagnosticInfo{ "WorkerWorkerExchangeUrmaConnectInfo", "", workerAddress_.ToString() },
            "rpc_return");
    }
    return Status::OK();
}

Status WorkerRpcClient::ProbeProviderUbRecovery(const std::string &expectedWorkerIncarnation,
                                                int32_t timeoutMs, ProviderUbRecoveryProbeRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed worker RPC client is not initialized");
#ifdef USE_URMA
    ProviderUbRecoveryProbeReqPb request;
    RETURN_IF_NOT_OK(ConstructRecoveryProbeHandshakePb(workerAddress_.ToString(), *request.mutable_hand_shake(),
                                                       *request.mutable_recovery_probe_addr()));
    request.set_expected_worker_incarnation(expectedWorkerIncarnation);

    int32_t rpcTimeout;
    RETURN_IF_NOT_OK(GetRpcTimeout(std::min<int64_t>(channelConfig_.timeout_ms, timeoutMs), rpcTimeout));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    Status rc = transportStub_->ProbeProviderUbRecovery(options, request, response);
    return rc.IsError() ? WithRpcDiag(rc, "ProbeProviderUbRecovery", workerAddress_) : Status::OK();
#else
    (void)expectedWorkerIncarnation;
    (void)timeoutMs;
    (void)response;
    return Status(K_NOT_SUPPORTED, "URMA Provider recovery probe is unavailable in this build");
#endif
}

Status WorkerRpcClient::QueryUbPortHealth(const std::string &expectedWorkerIncarnation,
                                          int32_t timeoutMs, QueryUbPortHealthRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Routed worker RPC client is not initialized");
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "UB port health query timeout must be positive");
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(expectedWorkerIncarnation);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    const auto configuredTimeout = channelConfig_.timeout_ms > 0 ? channelConfig_.timeout_ms : timeoutMs;
    const auto rpcTimeout = static_cast<int32_t>(
        std::min({ static_cast<int64_t>(timeoutMs), static_cast<int64_t>(configuredTimeout),
                   static_cast<int64_t>(MAX_RPC_TIMEOUT_MS) }));
    RpcOptions options;
    options.SetTimeout(rpcTimeout);
    auto rc = transportStub_->QueryUbPortHealth(options, request, response);
    return rc.IsError() ? WithRpcDiag(rc, "QueryUbPortHealth", workerAddress_) : Status::OK();
}

void WorkerRpcClient::SetUbHealthSummaryCallback(UbHealthSummaryApplyHook callback)
{
    std::lock_guard<bthread::Mutex> lock(ubHealthSummaryMutex_);
    ubHealthSummaryCallback_ = std::move(callback);
}

void WorkerRpcClient::ObserveUbHealthSummary(const UbHealthSummaryPb &encoded)
{
    UbHealthSummary summary;
    auto rc = DecodeUbHealthSummary(encoded, summary);
    if (rc.IsError() || summary.worker != workerAddress_) {
        const auto reason = rc.IsError() ? rc.ToString() : "Worker endpoint mismatch";
        LOG(WARNING) << "Ignore invalid business UB health sidecar from " << workerAddress_.ToString() << ": "
            << reason;
        return;
    }
    auto current = std::atomic_load(&lastUbHealthSummary_);
    if (current != nullptr && IsSameUbHealthSummary(*current, summary)) {
        return;
    }

    UbHealthSummaryApplyHook callback;
    {
        std::lock_guard<bthread::Mutex> lock(ubHealthSummaryMutex_);
        current = std::atomic_load(&lastUbHealthSummary_);
        if (current != nullptr && IsSameUbHealthSummary(*current, summary)) {
            return;
        }
        if (current != nullptr) {
            UbHealthSummary merged;
            if (!MergeUbHealthSummary(current.get(), summary, merged)) {
                LOG(WARNING) << "Ignore conflicting business UB health sidecar from " << workerAddress_.ToString()
                    << ": same health epoch carries different port counts";
                return;
            }
            summary = std::move(merged);
            if (IsSameUbHealthSummary(*current, summary)) {
                return;
            }
        }
        std::atomic_store(&lastUbHealthSummary_,
                          std::shared_ptr<const UbHealthSummary>(std::make_shared<UbHealthSummary>(summary)));
        callback = ubHealthSummaryCallback_;
    }
    if (callback) {
        try {
            callback(summary);
        } catch (const std::exception &error) {
            LOG(ERROR) << "Business UB health sidecar callback threw for " << workerAddress_.ToString()
                       << ": " << error.what();
        } catch (...) {
            LOG(ERROR) << "Business UB health sidecar callback threw for " << workerAddress_.ToString();
        }
    }
}

bool WorkerRpcClient::IsAlive() const
{
    return alive_.load(std::memory_order_acquire) && channel_ != nullptr && workerStub_ != nullptr
           && controlStub_ != nullptr
           && transportStub_ != nullptr && dataStub_ != nullptr;
}

void WorkerRpcClient::Close()
{
    alive_.store(false, std::memory_order_release);
    dataStub_.reset();
    controlStub_.reset();
    workerStub_.reset();
    transportStub_.reset();
    channel_.reset();
}

}  // namespace client
}  // namespace datasystem
