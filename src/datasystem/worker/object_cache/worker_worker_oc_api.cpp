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
 * Description: Defines the worker class to communicate with the worker service.
 */
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"

namespace datasystem {
namespace object_cache {
namespace {
// Upper bound for RPC timeout to prevent infinite wait on network failures.
constexpr int64_t kWorkerWorkerRpcMaxTimeoutMs = 120'000;
}  // namespace

WorkerLocalWorkerOCApi::WorkerLocalWorkerOCApi(WorkerWorkerOCServiceImpl *service,
                                               std::shared_ptr<AkSkManager> akSkManager)
    : service_(service), akSkManager_(std::move(akSkManager))
{
}

Status WorkerLocalWorkerOCApi::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(service_);
    return Status::OK();
}

Status WorkerLocalWorkerOCApi::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                               std::vector<RpcMessage> &payload)
{
    return service_->GetObjectRemote(req, rsp, payload);
}

Status WorkerLocalWorkerOCApi::GetObjectRemoteForQueryAndGet(GetObjectRemoteReqPb &req,
                                                             std::vector<RpcMessage> &payload)
{
    GetObjectRemoteRspPb rsp;
    return service_->GetObjectRemote(req, rsp, payload, true);
}

WorkerRemoteWorkerOCApi::WorkerRemoteWorkerOCApi(HostPort hostPort, HostPort localHostPort,
                                                 std::shared_ptr<AkSkManager> akSkManager)
    : hostPort_(std::move(hostPort)), localHostPort_(std::move(localHostPort)), akSkManager_(std::move(akSkManager))
{
}

Status WorkerRemoteWorkerOCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(hostPort_, StubType::WORKER_WORKER_OC_SVC, rpcStub));
    if (FLAGS_use_brpc) {
        brpcSession_ = std::dynamic_pointer_cast<WorkerWorkerOCService_BrpcGenericStub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcSession_);
    } else {
        rpcSession_ = std::dynamic_pointer_cast<WorkerWorkerOCService_Stub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    }
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                std::vector<RpcMessage> &payload)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_OUTBOUND_LATENCY);
    INJECT_POINT("worker.remote_get", []() {
        LOG(ERROR) << "DFX test, try again!";
        RETURN_STATUS(K_TRY_AGAIN, "DFX test, try again!");
    });
    RpcOptions opts;
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    if (remainingTime <= 0) {
        return WithRpcDiag(Status(K_RPC_DEADLINE_EXCEEDED, __LINE__, __FILE__,
                                  FormatString("Request timeout (%ld ms).", -remainingTime)),
                           "GetObjectRemote", localHostPort_, hostPort_);
    }
    // If timeout duration is too large, prevent from waiting too long when network errors.
    int64_t maxTimeoutMs = kWorkerWorkerRpcMaxTimeoutMs;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "GetObjectRemote",
                           localHostPort_, hostPort_);
    }
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Status rc = brpcSession_ ? brpcSession_->GetObjectRemote(opts, req, rsp, payload)
                             : rpcSession_->GetObjectRemote(opts, req, rsp, payload);
    return WithRpcDiag(rc, "GetObjectRemote", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::GetObjectRemote(
    std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> *clientApi)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_OUTBOUND_LATENCY);
    RpcOptions opts;
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    if (remainingTime <= 0) {
        return WithRpcDiag(Status(K_RPC_DEADLINE_EXCEEDED, __LINE__, __FILE__,
                                  FormatString("Request timeout (%lld ms).", -remainingTime)),
                           "GetObjectRemote", localHostPort_, hostPort_);
    }
    // If timeout duration is too large, prevent from waiting too long when network errors.
    int64_t maxTimeoutMs = kWorkerWorkerRpcMaxTimeoutMs;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "GetObjectRemote",
                           localHostPort_, hostPort_);
    }
    auto rc = brpcSession_ ? brpcSession_->GetObjectRemote(opts, clientApi)
                           : rpcSession_->GetObjectRemote(opts, clientApi);
    return WithRpcDiag(rc, "GetObjectRemote", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::GetObjectRemoteWrite(
    std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> &clientApi,
    GetObjectRemoteReqPb &req)
{
    INJECT_POINT("worker.remote_get", []() {
        LOG(ERROR) << "DFX test, try again!";
        RETURN_STATUS(K_TRY_AGAIN, "DFX test, try again!");
    });
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto rc = clientApi->Write(req);
    return WithRpcDiag(rc, "GetObjectRemote", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::BatchGetObjectRemote(
    std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> *clientApi)
{
    RpcOptions opts;
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    if (remainingTime <= 0) {
        return WithRpcDiag(Status(K_RPC_DEADLINE_EXCEEDED, __LINE__, __FILE__,
                                  FormatString("Request timeout (%ld ms).", -remainingTime)),
                           "BatchGetObjectRemote", localHostPort_, hostPort_);
    }
    // If timeout duration is too large, prevent from waiting too long when network errors.
    int64_t maxTimeoutMs = kWorkerWorkerRpcMaxTimeoutMs;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "BatchGetObjectRemote",
                           localHostPort_, hostPort_);
    }
    auto rc = brpcSession_ ? brpcSession_->BatchGetObjectRemote(opts, clientApi)
                           : rpcSession_->BatchGetObjectRemote(opts, clientApi);
    return WithRpcDiag(rc, "BatchGetObjectRemote", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::BatchGetObjectRemoteWrite(
    std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> &clientApi,
    BatchGetObjectRemoteReqPb &req)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto rc = clientApi->Write(req);
    return WithRpcDiag(rc, "BatchGetObjectRemote", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::CheckCoordinatorStateAsyncWrite(CheckCoordinatorStateReqPb &req, int64_t &tag)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr || brpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(brpcSession_ ? brpcSession_->CheckCoordinatorStateAsyncWrite(req, tag)
                                  : rpcSession_->CheckCoordinatorStateAsyncWrite(req, tag));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::CheckCoordinatorStateAsyncRead(int64_t tag, CheckCoordinatorStateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr || brpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(brpcSession_ ? brpcSession_->CheckCoordinatorStateAsyncRead(tag, rsp)
                                  : rpcSession_->CheckCoordinatorStateAsyncRead(tag, rsp));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::GetClusterStateAsyncWrite(GetClusterStateReqPb &req, int32_t timeoutMs,
                                                          int64_t &tag)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr || brpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_INVALID, "Cluster-state RPC timeout must be positive");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RpcOptions options;
    options.SetTimeout(timeoutMs);
    RETURN_IF_NOT_OK(brpcSession_ ? brpcSession_->GetClusterStateAsyncWrite(options, req, tag)
                                  : rpcSession_->GetClusterStateAsyncWrite(options, req, tag));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::GetClusterStateAsyncRead(int64_t tag, GetClusterStateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr || brpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(brpcSession_ ? brpcSession_->GetClusterStateAsyncRead(tag, rsp)
                                  : rpcSession_->GetClusterStateAsyncRead(tag, rsp));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::MigrateData(MigrateDataReqPb &req, const std::vector<MemView> &payloads,
                                            MigrateDataRspPb &rsp)
{
    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "MigrateData",
                           localHostPort_, hostPort_);
    }
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto rc = brpcSession_ ? brpcSession_->MigrateData(req, rsp, payloads)
                           : rpcSession_->MigrateData(req, rsp, payloads);
    return WithRpcDiag(rc, "MigrateData", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::MigrateDataProbe(MigrateDataReqPb &req, MigrateDataRspPb &rsp, int timeoutMs)
{
    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "MigrateDataProbe",
                           localHostPort_, hostPort_);
    }
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    constexpr int kMinProbeTimeoutMs = 100;
    if (timeoutMs < kMinProbeTimeoutMs) {
        timeoutMs = kMinProbeTimeoutMs;
    }
    RpcOptions opts;
    opts.SetTimeout(timeoutMs);
    std::vector<MemView> emptyPayloads;
    auto rc = brpcSession_ ? brpcSession_->MigrateData(opts, req, rsp, emptyPayloads)
                           : rpcSession_->MigrateData(opts, req, rsp, emptyPayloads);
    return WithRpcDiag(rc, "MigrateDataProbe", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::MigrateDataDirect(MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    RpcOptions opts;
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTime <= 0) {
        return WithRpcDiag(Status(K_RPC_DEADLINE_EXCEEDED, __LINE__, __FILE__,
                                  FormatString("Request timeout (%ld ms).", -remainingTime)),
                           "MigrateDataDirect", localHostPort_, hostPort_);
    }
    // Prevent waiting too long when network errors.
    constexpr int64_t maxTimeoutMs = 180'000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(static_cast<int32_t>(remainingTime));
    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "MigrateDataDirect",
                           localHostPort_, hostPort_);
    }
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto rc = brpcSession_ ? brpcSession_->MigrateDataDirect(opts, req, rsp)
                           : rpcSession_->MigrateDataDirect(opts, req, rsp);
    return WithRpcDiag(rc, "MigrateDataDirect", localHostPort_, hostPort_);
}

Status WorkerRemoteWorkerOCApi::NotifyRemoteGet(NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp)
{
    RpcOptions opts;
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTime <= 0) {
        return WithRpcDiag(Status(K_RPC_DEADLINE_EXCEEDED, __LINE__, __FILE__,
                                  FormatString("Request timeout (%ld ms).", -remainingTime)),
                           "NotifyRemoteGet", req.addr(), hostPort_);
    }

    // Prevent waiting too long when network errors.
    constexpr int64_t maxTimeoutMs = 180'000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(static_cast<int32_t>(remainingTime));

    if (rpcSession_ == nullptr && brpcSession_ == nullptr) {
        return WithRpcDiag(Status(K_RUNTIME_ERROR, __LINE__, __FILE__, "Rpc session is null"), "NotifyRemoteGet",
                           req.addr(), hostPort_);
    }
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto rc = brpcSession_ ? brpcSession_->NotifyRemoteGet(opts, req, rsp)
                           : rpcSession_->NotifyRemoteGet(opts, req, rsp);
    return WithRpcDiag(rc, "NotifyRemoteGet", req.addr(), hostPort_);
}

Status CreateRemoteWorkerApi(const std::string &endPoint, const HostPort &localHostPort,
                             const std::shared_ptr<AkSkManager> &akSkManager,
                             std::shared_ptr<WorkerRemoteWorkerOCApi> &workerOcApi)
{
    workerOcApi.reset();
    HostPort hostAddress;
    Status status = hostAddress.ParseString(endPoint);
    if (status.IsError()) {
        LOG(INFO) << "PARSEPROBLEM: Host and Port address can not be parsed for the give address: " << endPoint;
    }
    workerOcApi = std::make_shared<WorkerRemoteWorkerOCApi>(hostAddress, localHostPort, akSkManager);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerOcApi->Init(), "Create worker credential authentication failed.");
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
