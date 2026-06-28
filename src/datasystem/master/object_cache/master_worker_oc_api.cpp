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
#include "datasystem/master/object_cache/master_worker_oc_api.h"

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/master/object_cache/master_worker_oc_local_api.h"

namespace datasystem {
namespace master {

// Base class methods
MasterWorkerOCApi::MasterWorkerOCApi(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager)
    : masterHostPort_(localHostPort), akSkManager_(akSkManager)
{
}

std::shared_ptr<MasterWorkerOCApi> MasterWorkerOCApi::CreateMasterWorkerOCApi(
    const HostPort &hostPort, const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
    object_cache::MasterWorkerOCServiceImpl *service)
{
    if (hostPort != localHostPort) {
        VLOG(1) << "Master and worker are not collocated. Creating a MasterWorkerOCApi as RPC-based api.";
        return std::make_shared<MasterRemoteWorkerOCApi>(hostPort, localHostPort, akSkManager);
    }
    if (service == nullptr) {
        LOG(INFO) << "Master and worker are collocated but the worker service is not provided. Local bypass disabled.";
        return std::make_shared<MasterRemoteWorkerOCApi>(hostPort, localHostPort, akSkManager);
    }
    VLOG(1) << "Master and worker are collocated. Creating a MasterWorkerOCApi with local bypass optimization.";
    return std::make_shared<MasterLocalWorkerOCApi>(service, localHostPort, akSkManager);
}

// MasterRemoteWorkerOCApi methods

MasterRemoteWorkerOCApi::MasterRemoteWorkerOCApi(const HostPort &workerHostPort, const HostPort &localHostPort,
                                                 std::shared_ptr<AkSkManager> akSkManager)
    : MasterWorkerOCApi(localHostPort, akSkManager), workerHostPort_(workerHostPort)
{
}

Status MasterRemoteWorkerOCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(workerHostPort_, StubType::MASTER_WORKER_OC_SVC, rpcStub));
    if (FLAGS_use_brpc) {
        brpcSession_ = std::dynamic_pointer_cast<MasterWorkerOCService_BrpcGenericStub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcSession_);
    } else {
        rpcSession_ = std::dynamic_pointer_cast<MasterWorkerOCService_Stub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    }
    return Status::OK();
}

Status MasterRemoteWorkerOCApi::ClearData(ClearDataReqPb &req, ClearDataRspPb &rsp)
{
    RpcOptions opts;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    auto status = RetryOnErrorRepent(
        timeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &req, &rsp](int32_t rpcTimeout) {
            opts.SetTimeout(rpcTimeout);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            return brpcSession_ ? brpcSession_->ClearData(opts, req, rsp) : rpcSession_->ClearData(opts, req, rsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
    return WithRpcDiag(status, "ClearData", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::PublishMeta(PublishMetaReqPb &req, PublishMetaRspPb &resp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    auto status = RetryOnErrorRepent(
        timeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &req, &resp](int32_t rpcTimeout) {
            opts.SetTimeout(rpcTimeout);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            Timer timer;
            Status rc = brpcSession_ ? brpcSession_->PublishMeta(opts, req, resp)
                                       : rpcSession_->PublishMeta(opts, req, resp);
            GetMasterTimeCost().Append("Master to worker rpc PublishMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
    return WithRpcDiag(status, "PublishMeta", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::UpdateNotification(UpdateObjectReqPb &req, UpdateObjectRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    int64_t timeoutMs = timeoutDuration.CalcRealRemainingTime();
    INJECT_POINT("MasterWorkerOCApi.UpdateNotification.timeoutMs", [&timeoutMs](int time) {
        timeoutMs = time;
        return Status::OK();
    });
    auto status = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &req, &rsp](int32_t rpcTimeout) {
            opts.SetTimeout(rpcTimeout);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            Timer timer;
            Status rc = brpcSession_ ? brpcSession_->UpdateNotification(opts, req, rsp)
                                       : rpcSession_->UpdateNotification(opts, req, rsp);
            GetMasterTimeCost().Append("Master to worker rpc UpdateNotification", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
    return WithRpcDiag(status, "UpdateNotification", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::DeleteNotification(std::unique_ptr<DeleteObjectReqPb> req, DeleteObjectRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(*req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->DeleteNotification(opts, *req, rsp)
                               : rpcSession_->DeleteNotification(opts, *req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc DeleteNotification", timer.ElapsedMilliSecond());
    return WithRpcDiag(rc, "DeleteNotification", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::DeletePersistenceObject(std::unique_ptr<DeletePersistenceObjectReqPb> req,
                                                        DeletePersistenceObjectRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(*req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->DeletePersistenceObject(opts, *req, rsp)
                               : rpcSession_->DeletePersistenceObject(opts, *req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc DeletePersistenceObject", timer.ElapsedMilliSecond());
    return WithRpcDiag(rc, "DeletePersistenceObject", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::DeleteNotificationSend(std::unique_ptr<DeleteObjectReqPb> req, int64_t &tag)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(*req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->DeleteNotificationAsyncWrite(opts, *req, tag)
                               : rpcSession_->DeleteNotificationAsyncWrite(opts, *req, tag);
    GetMasterTimeCost().Append("Master to worker rpc DeleteNotificationSend", timer.ElapsedMilliSecond());
    return WithRpcDiag(rc, "DeleteNotificationSend", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::DeleteNotificationReceive(int64_t tag, DeleteObjectRspPb &rsp)
{
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->DeleteNotificationAsyncRead(tag, rsp)
                               : rpcSession_->DeleteNotificationAsyncRead(tag, rsp);
    GetMasterTimeCost().Append("Master to worker rpc DeleteNotificationReceive", timer.ElapsedMilliSecond());
    return WithRpcDiag(rc, "DeleteNotificationReceive", masterHostPort_, workerHostPort_);
}

Status MasterRemoteWorkerOCApi::QueryGlobalRefNumOnWorker(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    req.set_timeout(MasterGetRequestTimeout(timeoutDuration.CalcRemainingTime()));
    LOG(INFO) << "QueryGlobalRefNumOnWorker " << workerHostPort_.ToString() << " : " << LogHelper::IgnoreSensitive(req);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->QueryGlobalRefNumOnWorker(opts, req, rsp)
                               : rpcSession_->QueryGlobalRefNumOnWorker(opts, req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc QueryGlobalRefNumOnWorker", timer.ElapsedMilliSecond());
    if (rc.IsOk()) {
        LOG(INFO) << "QueryGlobalRefNumOnWorker " << workerHostPort_.ToString() << " Success "
                  << LogHelper::IgnoreSensitive(req);
    }
    return rc;
}

Status MasterRemoteWorkerOCApi::PushMetaToWorker(PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp)
{
    INJECT_POINT("SkipPushMetaToWorker");
    LOG(INFO) << "Send PushMetaToWorker to worker " << workerHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RpcOptions opts;
    timeoutDuration.Reset();
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->PushMetaToWorker(opts, req, rsp)
                               : rpcSession_->PushMetaToWorker(opts, req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc PushMetaToWorker", timer.ElapsedMilliSecond());
    if (rc.IsError()) {
        rc = WithRpcDiag(rc, "PushMetaToWorker", masterHostPort_, workerHostPort_);
    }
    LOG(INFO) << "Send PushMetaToWorker to worker " << workerHostPort_.ToString() << " " << rc.ToString()
              << LogHelper::IgnoreSensitive(rsp);
    return rc;
}

Status MasterRemoteWorkerOCApi::RequestMetaFromWorker(RequestMetaFromWorkerReqPb &req, RequestMetaFromWorkerRspPb &rsp)
{
    LOG(INFO) << "Send RequestMetaFromWorker to worker " << workerHostPort_.ToString();
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->RequestMetaFromWorker(opts, req, rsp)
                               : rpcSession_->RequestMetaFromWorker(opts, req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc RequestMetaFromWorker", timer.ElapsedMilliSecond());
    if (rc.IsError()) {
        rc = WithRpcDiag(rc, "RequestMetaFromWorker", masterHostPort_, workerHostPort_);
    }
    LOG(INFO) << "Send RequestMetaFromWorker to worker " << workerHostPort_.ToString() << " " << rc.ToString()
              << LogHelper::IgnoreSensitive(rsp);
    return rc;
}

Status MasterRemoteWorkerOCApi::ChangePrimaryCopy(ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp)
{
    LOG(INFO) << "Send ChangePrimaryCopy to worker " << workerHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->ChangePrimaryCopy(opts, req, rsp)
                               : rpcSession_->ChangePrimaryCopy(opts, req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc ChangePrimaryCopy", timer.ElapsedMilliSecond());
    if (rc.IsError()) {
        rc = WithRpcDiag(rc, "ChangePrimaryCopy", masterHostPort_, workerHostPort_);
    }
    LOG(INFO) << "Send ChangePrimaryCopy to worker " << workerHostPort_.ToString() << " " << rc.ToString()
              << LogHelper::IgnoreSensitive(rsp);
    return rc;
}

Status MasterRemoteWorkerOCApi::NotifyMasterIncNestedRefs(NotifyMasterIncNestedReqPb &req,
                                                          NotifyMasterIncNestedResPb &rsp)
{
    LOG(INFO) << "Send NotifyMasterIncNestedRefs to worker " << workerHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->NotifyMasterIncNestedRefs(opts, req, rsp)
                               : rpcSession_->NotifyMasterIncNestedRefs(opts, req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc NotifyMasterIncNestedRefs", timer.ElapsedMilliSecond());
    if (rc.IsError()) {
        rc = WithRpcDiag(rc, "NotifyMasterIncNestedRefs", masterHostPort_, workerHostPort_);
    }
    LOG(INFO) << "Send NotifyMasterIncNestedRefs to worker " << workerHostPort_.ToString() << " " << rc.ToString()
              << LogHelper::IgnoreSensitive(rsp);
    return rc;
}

Status MasterRemoteWorkerOCApi::NotifyMasterDecNestedRefs(NotifyMasterDecNestedReqPb &req,
                                                          NotifyMasterDecNestedResPb &rsp)
{
    LOG(INFO) << "Send NotifyMasterDecNestedRefs to worker " << workerHostPort_.ToString() << " : "
              << LogHelper::IgnoreSensitive(req);
    RpcOptions opts;
    SET_RPC_TIMEOUT(timeoutDuration, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = brpcSession_ ? brpcSession_->NotifyMasterDecNestedRefs(opts, req, rsp)
                               : rpcSession_->NotifyMasterDecNestedRefs(opts, req, rsp);
    GetMasterTimeCost().Append("Master to worker rpc NotifyMasterDecNestedRefs", timer.ElapsedMilliSecond());
    if (rc.IsError()) {
        rc = WithRpcDiag(rc, "NotifyMasterDecNestedRefs", masterHostPort_, workerHostPort_);
    }
    LOG(INFO) << "Send NotifyMasterDecNestedRefs to worker " << workerHostPort_.ToString() << " " << rc.ToString()
              << LogHelper::IgnoreSensitive(rsp);
    return rc;
}
}  // namespace master
}  // namespace datasystem
