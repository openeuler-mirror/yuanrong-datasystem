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

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"

namespace datasystem {
namespace object_cache {
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

WorkerRemoteWorkerOCApi::WorkerRemoteWorkerOCApi(HostPort hostPort, std::shared_ptr<AkSkManager> akSkManager)
    : hostPort_(std::move(hostPort)), akSkManager_(std::move(akSkManager))
{
}

Status WorkerRemoteWorkerOCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(hostPort_, StubType::WORKER_WORKER_OC_SVC, rpcStub));
    rpcSession_ = std::dynamic_pointer_cast<WorkerWorkerOCService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                std::vector<RpcMessage> &payload)
{
    INJECT_POINT("worker.remote_get", []() {
        LOG(ERROR) << "DFX test, try again!";
        RETURN_STATUS(K_TRY_AGAIN, "DFX test, try again!");
    });
    RpcOptions opts;
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    // If timeout duration is too large, prevent from waiting too long when network errors.
    int64_t maxTimeoutMs = 120000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->GetObjectRemote(opts, req, rsp, payload);
}

Status WorkerRemoteWorkerOCApi::GetObjectRemote(
    std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> *clientApi)
{
    RpcOptions opts;
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -remainingTime));
    // If timeout duration is too large, prevent from waiting too long when network errors.
    int64_t maxTimeoutMs = 120000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(rpcSession_->GetObjectRemote(opts, clientApi));
    return Status::OK();
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
    return clientApi->Write(req);
}

Status WorkerRemoteWorkerOCApi::BatchGetObjectRemote(
    std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> *clientApi)
{
    RpcOptions opts;
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    // If timeout duration is too large, prevent from waiting too long when network errors.
    int64_t maxTimeoutMs = 120000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(rpcSession_->BatchGetObjectRemote(opts, clientApi));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::BatchGetObjectRemoteWrite(
    std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> &clientApi,
    BatchGetObjectRemoteReqPb &req)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(clientApi->Write(req));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::CheckEtcdStateAsyncWrite(CheckEtcdStateReqPb &req, int64_t &tag)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->CheckEtcdStateAsyncWrite(req, tag));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::CheckEtcdStateAsyncRead(int64_t tag, CheckEtcdStateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(rpcSession_->CheckEtcdStateAsyncRead(tag, rsp));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::GetClusterStateAsyncWrite(GetClusterStateReqPb &req, int64_t &tag)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->GetClusterStateAsyncWrite(req, tag));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::GetClusterStateAsyncRead(int64_t tag, GetClusterStateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(rpcSession_->GetClusterStateAsyncRead(tag, rsp));
    return Status::OK();
}

Status WorkerRemoteWorkerOCApi::MigrateData(MigrateDataReqPb &req, const std::vector<MemView> &payloads,
                                            MigrateDataRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->MigrateData(req, rsp, payloads));
    return Status::OK();
}

Status CreateRemoteWorkerApi(const std::string &endPoint, const std::shared_ptr<AkSkManager> &akSkManager,
                             std::shared_ptr<WorkerRemoteWorkerOCApi> &workerOcApi)
{
    workerOcApi.reset();
    HostPort hostAddress;
    Status status = hostAddress.ParseString(endPoint);
    if (status.IsError()) {
        LOG(INFO) << "PARSEPROBLEM: Host and Port address can not be parsed for the give address: " << endPoint;
    }
    workerOcApi = std::make_shared<WorkerRemoteWorkerOCApi>(hostAddress, akSkManager);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerOcApi->Init(), "Create worker credential authentication failed.");
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
