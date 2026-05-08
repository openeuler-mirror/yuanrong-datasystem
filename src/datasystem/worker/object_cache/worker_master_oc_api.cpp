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
 * Description: Defines the worker client class to communicate with the meta master service.
 */
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <memory>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/master/object_cache/master_oc_service_impl.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/worker/object_cache/async_rpc_request_manager.h"
#include "datasystem/utils/status.h"

DS_DECLARE_uint32(node_timeout_s);

namespace datasystem {
namespace worker {
static constexpr int64_t WORKER_ADD_MILLISECOND = 5 * 1000;
static constexpr int64_t WORKER_TIMEOUT_MINUS_MILLISECOND = 5 * 1000;
static constexpr double WORKER_TIMEOUT_DESCEND_FACTOR = 0.9;
static constexpr int64_t RETRY_WAIT_MAX_TIME_MS = 2;
static constexpr int64_t RESOURCE_REPORT_RPC_TIMEOUT_MS = 3 * 1000;

#define CHECK_AND_SET_TIMEOUT(timeoutDuration_, request_, opts_)                               \
    do {                                                                                       \
        int64_t remainingTime_ = timeoutDuration_.CalcRemainingTime();                         \
        CHECK_FAIL_RETURN_STATUS(remainingTime_ > 0, K_RPC_DEADLINE_EXCEEDED,                  \
                                 FormatString("Request timeout (%lld ms).", -remainingTime_)); \
        request_.set_timeout(WorkerGetRequestTimeout(remainingTime_));                         \
        opts_.SetTimeout(remainingTime_);                                                      \
    } while (false)

inline int64_t WorkerGetRequestTimeout(int32_t timeout)
{
    return std::max(TimeoutDuration::ScaleTimeoutMs(timeout, WORKER_TIMEOUT_DESCEND_FACTOR),
                    timeout - WORKER_TIMEOUT_MINUS_MILLISECOND);
}

// Base class methods

WorkerMasterOCApi::WorkerMasterOCApi(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager)
    : localHostPort_(localHostPort), akSkManager_(akSkManager)
{
}

std::shared_ptr<WorkerMasterOCApi> WorkerMasterOCApi::CreateWorkerMasterOCApi(
    const HostPort &hostPort, const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
    datasystem::master::MasterOCServiceImpl *service)
{
    if (hostPort != localHostPort) {
        VLOG(1) << "Worker and master are not collocated. Creating a WorkerMasterOCApi as RPC-based api.";
        return std::make_shared<WorkerRemoteMasterOCApi>(hostPort, localHostPort, akSkManager);
    }

    if (service == nullptr) {
        LOG(INFO) << "Worker and master are collocated but the master service is not provided. Local bypass disabled.";
        return std::make_shared<WorkerRemoteMasterOCApi>(hostPort, localHostPort, akSkManager);
    }

    VLOG(1) << "Worker and master are collocated. Creating a WorkerMasterOCApi with local bypass optimization.";
    return std::make_shared<WorkerLocalMasterOCApi>(service, localHostPort, akSkManager);
}

// WorkerRemoteMasterOCApi methods

WorkerRemoteMasterOCApi::WorkerRemoteMasterOCApi(const HostPort &hostPort, const HostPort &localHostPort,
                                                 std::shared_ptr<AkSkManager> akSkManager)
    : WorkerMasterOCApi(localHostPort, std::move(akSkManager)), hostPort_(hostPort)
{
}

Status WorkerRemoteMasterOCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(hostPort_, StubType::WORKER_MASTER_OC_SVC, rpcStub));
    rpcSession_ = std::dynamic_pointer_cast<master::MasterOCService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    return Status::OK();
}

Status WorkerRemoteMasterOCApi::CreateMeta(master::CreateMetaReqPb &request, master::CreateMetaRspPb &response)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_CREATE_META_LATENCY);
    RpcOptions opts;
    // seal operation can't been retry.
    if (request.meta().life_state() == static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED)) {
        CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
        Timer sealTimer;
        Status rc = rpcSession_->CreateMeta(opts, request, response);
        workerOperationTimeCost.Append("Worker to master rpc Seal CreateMeta", sealTimer.ElapsedMilliSecond());
        return rc;
    }

    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    INJECT_POINT("WorkerMasterOCApi.CreateMeta.timeoutMs", [&timeoutMs](int time) {
        timeoutMs = time;
        return Status::OK();
    });
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->CreateMeta(opts, request, response);
            workerOperationTimeCost.Append("Worker to master rpc CreateMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::ReportResource(master::ResourceReportReqPb &request,
                                               master::ResourceReportRspPb &response)
{
    reqTimeoutDuration.Init();
    RpcOptions opts;
    int64_t timeoutMs =
        std::min(WorkerGetRequestTimeout(reqTimeoutDuration.CalcRealRemainingTime()), RESOURCE_REPORT_RPC_TIMEOUT_MS);
    Status status = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response](int32_t) {
            int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
            CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                                     FormatString("Request timeout (%ld ms).", -remainingTime));
            opts.SetTimeout(std::min(remainingTime, RESOURCE_REPORT_RPC_TIMEOUT_MS));
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            return rpcSession_->ReportResource(opts, request, response);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });

    return status;
}

Status WorkerRemoteMasterOCApi::CreateMultiMeta(master::CreateMultiMetaReqPb &request,
                                                master::CreateMultiMetaRspPb &response, bool retry)
{
    RpcOptions opts;
    if (!retry) {
        CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
        return rpcSession_->CreateMultiMeta(opts, request, response);
    }
    return RetryOnErrorRepent(
        reqTimeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &request, &response](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            return rpcSession_->CreateMultiMeta(opts, request, response);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::CreateMultiMetaPhaseTwo(master::CreateMultiMetaPhaseTwoReqPb &request,
                                                        master::CreateMultiMetaRspPb &response)
{
    RpcOptions opts;
    return RetryOnErrorRepent(
        reqTimeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &request, &response](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            return rpcSession_->CreateMultiMetaPhaseTwo(opts, request, response);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_WORKER_TIMEOUT });
}

Status WorkerRemoteMasterOCApi::CreateCopyMeta(master::CreateCopyMetaReqPb &request,
                                               master::CreateCopyMetaRspPb &response)
{
    RpcOptions opts;
    int64_t timeoutMs = WorkerGetRequestTimeout(reqTimeoutDuration.CalcRealRemainingTime());
    Status status = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response](int32_t) {
            int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
            CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                                     FormatString("Request timeout (%ld ms).", -remainingTime));
            opts.SetTimeout(remainingTime);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->CreateCopyMeta(opts, request, response);
            workerOperationTimeCost.Append("Worker to master rpc CreateCopyMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE }, false);

    return status;
}

Status WorkerRemoteMasterOCApi::CreateMultiCopyMeta(master::CreateMultiCopyMetaReqPb &request,
                                                   master::CreateMultiCopyMetaRspPb &response)
{
    RpcOptions opts;
    int64_t timeoutMs = WorkerGetRequestTimeout(reqTimeoutDuration.CalcRealRemainingTime());
    Status status = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response](int32_t) {
            int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
            CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                                     FormatString("Request timeout (%ld ms).", - remainingTime));
            opts.SetTimeout(remainingTime);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->CreateMultiCopyMeta(opts, request, response);
            workerOperationTimeCost.Append("Worker to master rpc CreateMultiCopyMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });

    return status;
}

Status WorkerRemoteMasterOCApi::QueryMeta(master::QueryMetaReqPb &request, uint64_t subTimeout,
                                          master::QueryMetaRspPb &response, std::vector<RpcMessage> &payloads)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_QUERY_META_LATENCY);
    INJECT_POINT("worker.remote_query_meta");
    PerfPoint point(PerfKey::WORKER_QUERY_META_REMOTE);
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -remainingTime));
    request.set_sub_timeout(std::min<int64_t>(subTimeout, remainingTime));
    RpcOptions opts;
    int64_t timeoutMs = WorkerGetRequestTimeout(reqTimeoutDuration.CalcRealRemainingTime());
    Status status = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response, &payloads](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->QueryMeta(opts, request, response, payloads);
            workerOperationTimeCost.Append("Worker to master rpc QueryMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });

    point.Record();
    return status;
}

Status WorkerRemoteMasterOCApi::RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response)
{
    RpcOptions opts;

    int64_t timeoutMs = WorkerGetRequestTimeout(reqTimeoutDuration.CalcRealRemainingTime());
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->RemoveMeta(opts, request, response);
            workerOperationTimeCost.Append("Worker to master rpc RemoveMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::GIncNestedRef(master::GIncNestedRefReqPb &request, master::GIncNestedRefRspPb &response)
{
    RpcOptions opts;
    request.set_address(localHostPort_.ToString());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    Timer timer;
    Status rc = rpcSession_->GIncNestedRef(opts, request, response);
    workerOperationTimeCost.Append("Worker to master rpc GIncNestedRef", timer.ElapsedMilliSecond());
    return rc;
}

Status WorkerRemoteMasterOCApi::GDecNestedRef(master::GDecNestedRefReqPb &request, master::GDecNestedRefRspPb &response)
{
    RpcOptions opts;
    request.set_address(localHostPort_.ToString());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    Timer timer;
    Status rc = rpcSession_->GDecNestedRef(opts, request, response);
    workerOperationTimeCost.Append("Worker to master rpc GDecNestedRef", timer.ElapsedMilliSecond());
    return rc;
}

Status WorkerRemoteMasterOCApi::UpdateMeta(master::UpdateMetaReqPb &request, master::UpdateMetaRspPb &response)
{
    RpcOptions opts;
    // seal operation can't been retry.
    if (request.life_state() == static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED)) {
        CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
        Timer sealTimer;
        Status rc = rpcSession_->UpdateMeta(opts, request, response);
        workerOperationTimeCost.Append("Worker to master rpc Seal UpdateMeta", sealTimer.ElapsedMilliSecond());
        return rc;
    }

    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    INJECT_POINT("WorkerMasterOCApi.UpdateMeta.timeoutMs", [&timeoutMs](int time) {
        timeoutMs = time;
        return Status::OK();
    });
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &request, &response](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->UpdateMeta(opts, request, response);
            workerOperationTimeCost.Append("Worker to master rpc UpdateMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::DeleteAllCopyMeta(master::DeleteAllCopyMetaReqPb &request,
                                                  master::DeleteAllCopyMetaRspPb &response)
{
    INJECT_POINT("worker.DeleteAllCopyMeta");
    RpcOptions opts;
    return RetryOnErrorRepent(
        reqTimeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &request, &response](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            Timer timer;
            Status rc = rpcSession_->DeleteAllCopyMeta(opts, request, response);
            workerOperationTimeCost.Append("Worker to master rpc DeleteAllCopyMeta", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::ReleaseGRefs(master::ReleaseGRefsReqPb &request, master::ReleaseGRefsRspPb &response)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return rpcSession_->ReleaseGRefs(opts, request, response);
}

Status WorkerRemoteMasterOCApi::GIncreaseMasterRef(master::GIncreaseReqPb &incReq, master::GIncreaseRspPb &incRsp)
{
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    if (remainingTime > INT_MAX) {
        remainingTime = INT_MAX;
    }
    RpcOptions opts;
    opts.SetTimeout(remainingTime);
    auto rc = RetryOnErrorRepent(
        reqTimeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &incReq, &incRsp](int32_t) {
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(incReq));
            return this->rpcSession_->GIncreaseRef(opts, incReq, incRsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
    return rc;
}

Status WorkerRemoteMasterOCApi::GDecreaseMasterRef(master::GDecreaseReqPb &decReq, master::GDecreaseRspPb &decRsp)
{
    RpcOptions opts;
    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    auto rc = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &decReq, &decRsp](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, decReq, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(decReq));
            return this->rpcSession_->GDecreaseRef(opts, decReq, decRsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
    return rc;
}

Status WorkerRemoteMasterOCApi::GDecreaseMasterRef(const std::vector<std::string> &finishDecIds,
                                                   std::unordered_set<std::string> &unAliveIds,
                                                   std::vector<std::string> &failDecIds,
                                                   const std::string &remoteClientId)
{
    INJECT_POINT("worker.gdecrease");
    RpcOptions opts;
    master::GDecreaseReqPb decReq;
    *decReq.mutable_object_keys() = { finishDecIds.begin(), finishDecIds.end() };
    decReq.set_address(localHostPort_.ToString());
    decReq.set_remote_client_id(remoteClientId);
    master::GDecreaseRspPb decRsp;

    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    INJECT_POINT("WorkerMasterOCApi.GDecreaseMasterRef.timeoutMs", [&timeoutMs](int time) {
        timeoutMs = time;
        return Status::OK();
    });
    auto rc = RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &decReq, &decRsp](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, decReq, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(decReq));
            Timer timer;
            Status rc = this->rpcSession_->GDecreaseRef(opts, decReq, decRsp);
            workerOperationTimeCost.Append("Worker to master rpc GDecreaseMasterRef", timer.ElapsedMilliSecond());
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
    if (rc.IsError()) {
        LOG(ERROR) << "GDecreaseMasterRef failed with " << rc.ToString();
        failDecIds = finishDecIds;
        return rc;
    }
    unAliveIds = { decRsp.no_ref_ids().begin(), decRsp.no_ref_ids().end() };
    Status recvRc(static_cast<StatusCode>(decRsp.last_rc().error_code()), decRsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "GDecreaseMasterRef response " << LogHelper::IgnoreSensitive(decRsp);
        failDecIds = { decRsp.failed_object_keys().begin(), decRsp.failed_object_keys().end() };
        return recvRc;
    }
    return Status::OK();
}

Status WorkerRemoteMasterOCApi::QueryGlobalRefNum(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, req, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Timer timer;
    Status rc = rpcSession_->QueryGlobalRefNum(opts, req, rsp);
    workerOperationTimeCost.Append("Worker to master rpc QueryGlobalRefNum", timer.ElapsedMilliSecond());
    return rc;
}

Status WorkerRemoteMasterOCApi::PushMetadataToMaster(master::PushMetaToMasterReqPb &req,
                                                     master::PushMetaToMasterRspPb &rsp)
{
    INJECT_POINT("WorkerRemote.PushMetadataToMaster");
    constexpr int retryTime = 3;  // retry time
    constexpr int64_t timeoutMs = RPC_TIMEOUT / retryTime;
    return RetryOnRPCError([this, &req, &rsp]() {
        RpcOptions opts;
        opts.SetTimeout(timeoutMs + WORKER_ADD_MILLISECOND);
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
        Timer timer;
        Status rc = rpcSession_->PushMetaToMaster(opts, req, rsp);
        workerOperationTimeCost.Append("Worker to master rpc PushMetadataToMaster", timer.ElapsedMilliSecond());
        return rc;
    });
}

Status WorkerRemoteMasterOCApi::RollbackSeal(const std::string &objectKey, uint32_t oldLifeState)
{
    master::RollbackSealReqPb req;
    master::RollbackSealRspPb rsp;
    req.set_object_key(objectKey);
    req.set_old_life_state(oldLifeState);
    req.set_address(localHostPort_.ToString());
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    RpcOptions opts;
    uint64_t timeoutMs = remainingTime > 0 ? remainingTime : WORKER_ADD_MILLISECOND;
    if (timeoutMs > INT32_MAX) {
        timeoutMs = INT32_MAX;
    }
    opts.SetTimeout(timeoutMs);
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &req, &rsp](int32_t) {
            opts.SetTimeout(std::min<int64_t>(opts.GetTimeout(), reqTimeoutDuration.CalcRemainingTime()));
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            Timer timer;
            Status rc = rpcSession_->RollbackSeal(opts, req, rsp);
            workerOperationTimeCost.Append("Worker to master rpc RollbackSeal", timer.ElapsedMilliSecond());
            return rc;
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::IfNeedTriggerReconciliation(master::ReconciliationQueryPb &req,
                                                            master::ReconciliationRspPb &rsp)
{
    LOG(INFO) << "worker on " << localHostPort_.ToString() << " sends a reconciliation request to the remote master on "
              << hostPort_.ToString();
    req.set_hostport(localHostPort_.ToString());
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateCredentials(WORKER_SERVER_NAME, cred));
    int retryTimeout = 60 * 1000;  // 1 minutes
    INJECT_POINT("WorkerRemoteMasterOCApi.IfNeedTriggerReconciliation.retryTimeout", [&retryTimeout](int timeout) {
        retryTimeout = timeout;
        return Status::OK();
    });
    const std::unordered_set<StatusCode> &retryOn = { StatusCode::K_NOT_FOUND, StatusCode::K_RPC_CANCELLED,
                                                      StatusCode::K_RPC_DEADLINE_EXCEEDED,
                                                      StatusCode::K_RPC_UNAVAILABLE };
    auto retryFun = [this, &req, &rsp](int32_t) {
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
        Timer timer;
        Status rc = rpcSession_->IfNeedTriggerReconciliation(req, rsp);
        workerOperationTimeCost.Append("Worker to master rpc IfNeedTriggerReconciliation", timer.ElapsedMilliSecond());
        return rc;
    };
    RETURN_IF_NOT_OK(RetryOnError(
        retryTimeout, retryFun, []() { return Status::OK(); }, retryOn));
    // If needed, reconciliation should have been done. Otherwise, no-op and OK was returned.
    return Status::OK();
}

std::string WorkerRemoteMasterOCApi::GetHostPort()
{
    return hostPort_.ToString();
}

Status WorkerRemoteMasterOCApi::PutP2PMeta(PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    RpcOptions opts;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    opts.SetTimeout(remainingTime);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    PerfPoint point(PerfKey::WORKER_REMOTE_PUT_P2P_META);
    return rpcSession_->PutP2PMeta(opts, req, resp);
}

Status WorkerRemoteMasterOCApi::SubscribeReceiveEvent(
    SubscribeReceiveEventReqPb &req,
    std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi,
    std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "GetDataInfo Send Status to client failed.");
        return rc;
    }

    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    using RemoteRpc = RemoteAsyncRpcRequest<SubscribeReceiveEventReqPb, SubscribeReceiveEventRspPb>;

    auto asyncWriteCallback = [remainingTime, req, serverApi](RemoteRpc &remoteRpc) {
        RpcOptions opts;
        opts.SetTimeout(remainingTime);
        int64_t tagId;
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        Status rc = session->SubscribeReceiveEventAsyncWrite(opts, req, tagId);
        if (rc.IsError()) {
            LOG(ERROR) << "failed to SubscribeReceiveEventAsyncWrite, error: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "SubscribeReceiveEvent Send Status to client failed.");
            return rc;
        }
        remoteRpc.SetResponseTag(tagId);
        return Status::OK();
    };

    auto returnCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = remoteRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "SubscribeReceiveEvent remote rpc failed with : " << pair.second.ToString();
            return remoteRpc.ReplyToClient(pair, serverApi);
        } else {
            for (const auto &npuEvent : pair.first.npuevents()) {
                VLOG(1) << FormatString("[SubscribeReceiveEvent] objkeys: %s, event-type: %d", npuEvent.object_key(),
                                        static_cast<int>(npuEvent.event_type()));
            }
            LOG_IF_ERROR(serverApi->Write(pair.first), "SubscribeReceiveEvent Writes reply to client failed.");
        }
        return Status::OK();
    };

    auto rpcRespFunc = [](RemoteRpc &remoteRpc, int64_t tagId, SubscribeReceiveEventRspPb &rsp, RpcRecvFlags flags) {
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        return session->SubscribeReceiveEventAsyncRead(tagId, rsp, flags);
    };

    auto timeoutCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair =
            std::make_pair(SubscribeReceiveEventRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return remoteRpc.ReplyToClient(pair, serverApi);
    };

    std::shared_ptr<RemoteRpc> remoteRpc = std::make_shared<RemoteRpc>(rpcSession_, remainingTime);
    remoteRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback, rpcRespFunc);

    RETURN_IF_NOT_OK(remoteRpc->AsyncWrite());
    asyncRpcManager->AddRequest(remoteRpc);
    return Status::OK();
}

Status WorkerRemoteMasterOCApi::GetP2PMeta(
    GetP2PMetaReqPb &req, std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi,
    std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "GetP2PMeta Send Status to client failed.");
        return rc;
    }

    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    using RemoteRpc = RemoteAsyncRpcRequest<GetP2PMetaReqPb, GetP2PMetaRspPb>;

    auto asyncWriteCallback = [remainingTime, req, serverApi](RemoteRpc &remoteRpc) {
        RpcOptions opts;
        opts.SetTimeout(remainingTime);
        int64_t tagId;
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        Status rc = session->GetP2PMetaAsyncWrite(opts, req, tagId);
        if (rc.IsError()) {
            LOG(ERROR) << "failed to GetP2PMetaAsyncWrite, error: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "GetP2PMeta Send Status to client failed.");
            return rc;
        }
        remoteRpc.SetResponseTag(tagId);
        return Status::OK();
    };

    auto returnCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = remoteRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "GetP2PMeta remote rpc failed with : " << pair.second.ToString();
            return remoteRpc.ReplyToClient(pair, serverApi);
        } else {
            LOG_IF_ERROR(serverApi->Write(pair.first), "GetP2PMeta Writes reply to client failed.");
        }
        return Status::OK();
    };

    auto rpcRespFunc = [](RemoteRpc &remoteRpc, int64_t tagId, GetP2PMetaRspPb &rsp, RpcRecvFlags flags) {
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        return session->GetP2PMetaAsyncRead(tagId, rsp, flags);
    };

    auto timeoutCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = std::make_pair(GetP2PMetaRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return remoteRpc.ReplyToClient(pair, serverApi);
    };

    std::shared_ptr<RemoteRpc> remoteRpc = std::make_shared<RemoteRpc>(rpcSession_, remainingTime);
    remoteRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback, rpcRespFunc);

    RETURN_IF_NOT_OK(remoteRpc->AsyncWrite());
    asyncRpcManager->AddRequest(remoteRpc);
    return Status::OK();
}

Status WorkerRemoteMasterOCApi::SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    RpcOptions opts;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    opts.SetTimeout(remainingTime);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->SendRootInfo(req, resp);
}

Status WorkerRemoteMasterOCApi::RecvRootInfo(
    RecvRootInfoReqPb &req, std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi,
    std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "RecvRootInfo Send Status to client failed.");
        return rc;
    }

    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    using RemoteRpc = RemoteAsyncRpcRequest<RecvRootInfoReqPb, RecvRootInfoRspPb>;

    auto asyncWriteCallback = [remainingTime, req, serverApi](RemoteRpc &remoteRpc) {
        RpcOptions opts;
        opts.SetTimeout(remainingTime);
        int64_t tagId;
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        Status rc = session->RecvRootInfoAsyncWrite(opts, req, tagId);
        if (rc.IsError()) {
            LOG(ERROR) << "failed to RecvRootInfoAsyncWrite, error: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "RecvRootInfo Send Status to client failed.");
            return rc;
        }
        remoteRpc.SetResponseTag(tagId);
        return Status::OK();
    };

    auto returnCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = remoteRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "SubscribeReceiveEvent remote rpc failed with : " << pair.second.ToString();
            return remoteRpc.ReplyToClient(pair, serverApi);
        } else {
            LOG_IF_ERROR(serverApi->Write(pair.first), "SubscribeReceiveEvent Writes reply to client failed.");
        }
        return Status::OK();
    };

    auto rpcRespFunc = [](RemoteRpc &remoteRpc, int64_t tagId, RecvRootInfoRspPb &rsp, RpcRecvFlags flags) {
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        return session->RecvRootInfoAsyncRead(tagId, rsp, flags);
    };

    auto timeoutCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = std::make_pair(RecvRootInfoRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return remoteRpc.ReplyToClient(pair, serverApi);
    };

    std::shared_ptr<RemoteRpc> remoteRpc = std::make_shared<RemoteRpc>(rpcSession_, remainingTime);
    remoteRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback, rpcRespFunc);

    RETURN_IF_NOT_OK(remoteRpc->AsyncWrite());
    asyncRpcManager->AddRequest(remoteRpc);
    return Status::OK();
}

Status WorkerRemoteMasterOCApi::AckRecvFinish(AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    RpcOptions opts;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    opts.SetTimeout(remainingTime);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->AckRecvFinish(req, resp);
}

Status WorkerRemoteMasterOCApi::GetDataInfo(
    GetDataInfoReqPb &req,
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
    const int64_t subTimeoutMs, std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    req.set_sub_timeout(std::min<int64_t>(subTimeoutMs, remainingTime));
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "GetDataInfo Send Status to client failed.");
        return rc;
    }

    using RemoteRpc = RemoteAsyncRpcRequest<GetDataInfoReqPb, GetDataInfoRspPb>;

    auto asyncWriteCallback = [remainingTime, req, serverApi](RemoteRpc &remoteRpc) {
        RpcOptions opts;
        opts.SetTimeout(remainingTime);
        int64_t tagId;
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        Status rc = session->GetDataInfoAsyncWrite(opts, req, tagId);
        if (rc.IsError()) {
            LOG(ERROR) << "failed to GetDataInfoAsyncWrite, error: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "GetDataInfo Send Status to client failed.");
            return rc;
        }
        remoteRpc.SetResponseTag(tagId);
        return Status::OK();
    };
    auto returnCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = remoteRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "GetDataInfo remote rpc failed with : " << pair.second.ToString();
            return remoteRpc.ReplyToClient(pair, serverApi);
        } else {
            LOG_IF_ERROR(serverApi->Write(pair.first), "SubscribeReceiveEvent Writes reply to client failed.");
        }
        return Status::OK();
    };
    auto rpcRespFunc = [](RemoteRpc &remoteRpc, int64_t tagId, GetDataInfoRspPb &rsp, RpcRecvFlags flags) {
        auto session = remoteRpc.GetServerApi();
        RETURN_RUNTIME_ERROR_IF_NULL(session);
        return session->GetDataInfoAsyncRead(tagId, rsp, flags);
    };
    auto timeoutCallback = [serverApi](RemoteRpc &remoteRpc) {
        auto pair = std::make_pair(GetDataInfoRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return remoteRpc.ReplyToClient(pair, serverApi);
    };

    std::shared_ptr<RemoteRpc> remoteRpc = std::make_shared<RemoteRpc>(rpcSession_, remainingTime);
    remoteRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback, rpcRespFunc);

    RETURN_IF_NOT_OK(remoteRpc->AsyncWrite());
    asyncRpcManager->AddRequest(remoteRpc);
    return Status::OK();
}

Status WorkerRemoteMasterOCApi::RemoveP2PLocation(RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    RpcOptions opts;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    opts.SetTimeout(remainingTime);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->RemoveP2PLocation(req, resp);
}

Status WorkerRemoteMasterOCApi::GetObjectLocations(master::GetObjectLocationsReqPb &req,
                                                   master::GetObjectLocationsRspPb &resp)
{
    return GetObjectLocations(req, resp, reqTimeoutDuration.CalcRealRemainingTime());
}

Status WorkerRemoteMasterOCApi::GetObjectLocations(master::GetObjectLocationsReqPb &req,
                                                   master::GetObjectLocationsRspPb &resp, int64_t timeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -timeoutMs));
    RpcOptions opts;
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &opts, &req, &resp](int32_t onceRpcRemainTime) {
            opts.SetTimeout(onceRpcRemainTime);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            Timer timer;
            Status rc = rpcSession_->GetObjectLocations(opts, req, resp);
            workerOperationTimeCost.Append("Worker to master rpc GetObjectLocations", timer.ElapsedMilliSecond());
            INJECT_POINT("WorkerRemoteMasterOCApi.GetObjectLocations");
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::ReleaseMetaData(ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp)
{
    RpcOptions opts;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    opts.SetTimeout(remainingTime);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->ReleaseMetaData(opts, req, resp);
}

Status WorkerRemoteMasterOCApi::ReplacePrimary(master::ReplacePrimaryReqPb &req, master::ReplacePrimaryRspPb &rsp)
{
    RpcOptions opts;
    opts.SetTimeout(RPC_TIMEOUT);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->ReplacePrimary(opts, req, rsp);
}

Status WorkerRemoteMasterOCApi::PureQueryMeta(master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    RpcOptions opts;
    opts.SetTimeout(RPC_TIMEOUT);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->PureQueryMeta(opts, req, rsp);
}

Status WorkerRemoteMasterOCApi::CheckObjectDataLocation(master::CheckObjectDataLocationReqPb &req,
                                                        master::CheckObjectDataLocationRspPb &rsp)
{
    RpcOptions opts;
    return RetryOnErrorRepent(
        RPC_TIMEOUT,
        [this, &opts, &req, &rsp](int32_t rpcTimeout) {
            opts.SetTimeout(rpcTimeout);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            return rpcSession_->CheckObjectDataLocation(opts, req, rsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::RollbackMultiMeta(master::RollbackMultiMetaReqPb &req,
                                                  master::RollbackMultiMetaRspPb &rsp)
{
    RpcOptions opts;
    return RetryOnErrorRepent(
        reqTimeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &req, &rsp](int32_t) {
            CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, req, opts);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            return rpcSession_->RollbackMultiMeta(opts, req, rsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}

Status WorkerRemoteMasterOCApi::Expire(master::ExpireReqPb &req, master::ExpireRspPb &rsp)
{
    RpcOptions opts;
    opts.SetTimeout(RPC_TIMEOUT);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->Expire(opts, req, rsp);
}

Status WorkerRemoteMasterOCApi::GetMetaInfo(GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    RpcOptions opts;
    opts.SetTimeout(RPC_TIMEOUT);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->GetMetaInfo(opts, req, rsp);
}
// WorkerLocalMasterOCApi methods

WorkerLocalMasterOCApi::WorkerLocalMasterOCApi(master::MasterOCServiceImpl *service, const HostPort &localHostPort,
                                               std::shared_ptr<AkSkManager> akSkManager)
    : WorkerMasterOCApi(localHostPort, akSkManager), masterOC_(service)
{
}

Status WorkerLocalMasterOCApi::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(masterOC_);
    return Status::OK();
}

Status WorkerLocalMasterOCApi::CreateMeta(master::CreateMetaReqPb &request, master::CreateMetaRspPb &response)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_CREATE_META_LATENCY);
    // Although this is not an RPC call, the timeout field in the CreateMetaReqPb itself has associated actions on the
    // server side.
    int64_t remainingTime_ = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime_ > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime_));
    request.set_timeout(remainingTime_);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    return RetryOnErrorRepent(
        timeoutMs, [this, &request, &response](int32_t) { return masterOC_->CreateMeta(request, response); },
        []() { return Status::OK(); }, { StatusCode::K_TRY_AGAIN });
}

Status WorkerLocalMasterOCApi::ReportResource(master::ResourceReportReqPb &request,
                                              master::ResourceReportRspPb &response)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->ReportResource(request, response);
}

Status WorkerLocalMasterOCApi::CreateMultiMeta(master::CreateMultiMetaReqPb &request,
                                               master::CreateMultiMetaRspPb &response, bool retry)
{
    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -timeoutMs));
    request.set_timeout(timeoutMs);
    if (!retry) {
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
        return masterOC_->CreateMultiMeta(request, response);
    }
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &request, &response](int32_t) {
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            return masterOC_->CreateMultiMeta(request, response);
        },
        []() { return Status::OK(); }, { StatusCode::K_TRY_AGAIN });
}

Status WorkerLocalMasterOCApi::CreateMultiMetaPhaseTwo(master::CreateMultiMetaPhaseTwoReqPb &request,
                                                       master::CreateMultiMetaRspPb &response)
{
    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    CHECK_FAIL_RETURN_STATUS(timeoutMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -timeoutMs));
    request.set_timeout(timeoutMs);
    return RetryOnErrorRepent(
        timeoutMs,
        [this, &request, &response](int32_t) {
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            return masterOC_->CreateMultiMetaPhaseTwo(request, response);
        },
        []() { return Status::OK(); }, { StatusCode::K_TRY_AGAIN, StatusCode::K_WORKER_TIMEOUT });
}

Status WorkerLocalMasterOCApi::CreateCopyMeta(master::CreateCopyMetaReqPb &request,
                                              master::CreateCopyMetaRspPb &response)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->CreateCopyMeta(request, response);
}

Status WorkerLocalMasterOCApi::CreateMultiCopyMeta(master::CreateMultiCopyMetaReqPb &request,
                                                  master::CreateMultiCopyMetaRspPb &response)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->CreateMultiCopyMeta(request, response);
}

Status WorkerLocalMasterOCApi::QueryMeta(master::QueryMetaReqPb &request, uint64_t subTimeout,
                                         master::QueryMetaRspPb &response, std::vector<RpcMessage> &payloads)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_QUERY_META_LATENCY);
    INJECT_POINT("worker.query_meta");
    PerfPoint point(PerfKey::WORKER_QUERY_META_LOCAL);
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    request.set_sub_timeout(
        std::min<int64_t>(subTimeout, WorkerGetRequestTimeout(reqTimeoutDuration.CalcRemainingTime())));
    request.set_timeout(remainingTime);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    Status rc = masterOC_->QueryMeta(request, response, payloads);
    point.Record();
    return rc;
}

Status WorkerLocalMasterOCApi::RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->RemoveMeta(request, response);
}

Status WorkerLocalMasterOCApi::GIncNestedRef(master::GIncNestedRefReqPb &request, master::GIncNestedRefRspPb &response)
{
    request.set_address(localHostPort_.ToString());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->GIncNestedRef(request, response);
}

Status WorkerLocalMasterOCApi::GDecNestedRef(master::GDecNestedRefReqPb &request, master::GDecNestedRefRspPb &response)
{
    request.set_address(localHostPort_.ToString());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->GDecNestedRef(request, response);
}

Status WorkerLocalMasterOCApi::UpdateMeta(master::UpdateMetaReqPb &request, master::UpdateMetaRspPb &response)
{
    RpcOptions opts;  // Useless. Define opts just to be able to call CHECK_AND_SET_TIMEOUT
    CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    return RetryOnErrorRepent(
        timeoutMs, [this, &request, &response](int32_t) { return masterOC_->UpdateMeta(request, response); },
        []() { return Status::OK(); }, { StatusCode::K_TRY_AGAIN });
}

Status WorkerLocalMasterOCApi::DeleteAllCopyMeta(master::DeleteAllCopyMetaReqPb &request,
                                                 master::DeleteAllCopyMetaRspPb &response)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->DeleteAllCopyMeta(request, response);
}

Status WorkerLocalMasterOCApi::ReleaseGRefs(master::ReleaseGRefsReqPb &request, master::ReleaseGRefsRspPb &response)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(reqTimeoutDuration, request, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
    return masterOC_->ReleaseGRefs(request, response);
}

Status WorkerLocalMasterOCApi::GIncreaseMasterRef(master::GIncreaseReqPb &incReq, master::GIncreaseRspPb &incRsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(incReq));
    return masterOC_->GIncreaseRef(incReq, incRsp);
}

Status WorkerLocalMasterOCApi::GDecreaseMasterRef(master::GDecreaseReqPb &decReq, master::GDecreaseRspPb &decRsp)
{
    int64_t remainingTime_ = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime_ > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime_));
    decReq.set_timeout(remainingTime_);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(decReq));
    return masterOC_->GDecreaseRef(decReq, decRsp);
}

Status WorkerLocalMasterOCApi::GDecreaseMasterRef(const std::vector<std::string> &finishDecIds,
                                                  std::unordered_set<std::string> &unAliveIds,
                                                  std::vector<std::string> &failDecIds,
                                                  const std::string &remoteClientId)
{
    INJECT_POINT("worker.gdecrease");
    master::GDecreaseReqPb decReq;
    int64_t remainingTime_ = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime_ > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime_));
    decReq.set_timeout(remainingTime_);
    *decReq.mutable_object_keys() = { finishDecIds.begin(), finishDecIds.end() };
    decReq.set_address(localHostPort_.ToString());
    decReq.set_remote_client_id(remoteClientId);
    master::GDecreaseRspPb decRsp;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(decReq));
    LOG(INFO) << "GDecreaseMasterRef";
    auto rc = masterOC_->GDecreaseRef(decReq, decRsp);
    if (rc.IsError()) {
        failDecIds = finishDecIds;
        return rc;
    }
    failDecIds = { decRsp.mutable_failed_object_keys()->begin(), decRsp.mutable_failed_object_keys()->end() };
    unAliveIds = { decRsp.mutable_no_ref_ids()->begin(), decRsp.mutable_no_ref_ids()->end() };
    Status recvRc(static_cast<StatusCode>(decRsp.last_rc().error_code()), decRsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "GDecreaseMasterRef response " << LogHelper::IgnoreSensitive(decRsp);
        failDecIds = { decRsp.failed_object_keys().begin(), decRsp.failed_object_keys().end() };
        return recvRc;
    }
    return rc;
}

Status WorkerLocalMasterOCApi::QueryGlobalRefNum(QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp)
{
    req.set_timeout(reqTimeoutDuration.CalcRemainingTime());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->QueryGlobalRefNum(req, rsp);
}

Status WorkerLocalMasterOCApi::PushMetadataToMaster(master::PushMetaToMasterReqPb &req,
                                                    master::PushMetaToMasterRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->PushMetaToMaster(req, rsp);
}

Status WorkerLocalMasterOCApi::RollbackSeal(const std::string &objectKey, uint32_t oldLifeState)
{
    master::RollbackSealReqPb req;
    master::RollbackSealRspPb rsp;
    req.set_object_key(objectKey);
    req.set_old_life_state(oldLifeState);
    req.set_address(localHostPort_.ToString());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->RollbackSeal(req, rsp);
}

Status WorkerLocalMasterOCApi::IfNeedTriggerReconciliation(master::ReconciliationQueryPb &req,
                                                           master::ReconciliationRspPb &rsp)
{
    LOG(INFO) << "worker(" << localHostPort_.ToString() << ") performs reconciliation with local master.";
    req.set_hostport(localHostPort_.ToString());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterOC_->IfNeedTriggerReconciliationImpl(req, rsp));
    return Status::OK();
}

std::string WorkerLocalMasterOCApi::GetHostPort()
{
    return localHostPort_.ToString();
}

WorkerMasterOcApiManager::WorkerMasterOcApiManager(HostPort &hostPort, std::shared_ptr<AkSkManager> akSkManager,
                                                   master::MasterOCServiceImpl *masterOCService)
    : WorkerMasterApiManagerBase<WorkerMasterOCApi>(hostPort, akSkManager), masterOCService_(masterOCService)
{
}

std::shared_ptr<WorkerMasterOCApi> WorkerMasterOcApiManager::CreateWorkerMasterApi(const HostPort &masterAddress)
{
    return WorkerMasterOCApi::CreateWorkerMasterOCApi(masterAddress, workerAddr_, akSkManager_, masterOCService_);
}

Status WorkerLocalMasterOCApi::PutP2PMeta(PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    PerfPoint point(PerfKey::WORKER_LOCAL_PUT_P2P_META);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterOC_->PutP2PMeta(req, resp));
    return Status::OK();
}

Status WorkerLocalMasterOCApi::SubscribeReceiveEvent(
    SubscribeReceiveEventReqPb &req,
    std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi,
    std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "SubscribeReceiveEvent Send Status to client failed.");
        return rc;
    }
    using LocalRpc = LocalAsyncRpcRequest<SubscribeReceiveEventReqPb, SubscribeReceiveEventRspPb>;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    auto asyncWriteCallback = [this, serverApi](LocalRpc &localRpc) -> Status {
        auto rc = masterOC_->SubscribeReceiveEvent(localRpc.GetServerApi());
        if (rc.IsError()) {
            LOG(ERROR) << "ERROR with SubscribeReceiveEvent: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "SubscribeReceiveEvent Send StatusG to client failed.");
            return rc;
        }
        return Status::OK();
    };
    auto returnCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = localRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "SubscribeReceiveEvent remote rpc failed with : " << pair.second.ToString();
            return localRpc.ReplyToClient(pair, serverApi);
        }
        for (const auto &npuEvent : pair.first.npuevents()) {
            VLOG(1) << FormatString("[SubscribeReceiveEvent] objkeys: %s, event-type: %d", npuEvent.object_key(),
                                    static_cast<int>(npuEvent.event_type()));
        }
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto timeoutCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair =
            std::make_pair(SubscribeReceiveEventRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return localRpc.ReplyToClient(pair, serverApi);
    };

    std::shared_ptr<LocalRpc> localRpc = std::make_shared<LocalRpc>(req, remainingTime);
    localRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback);

    RETURN_IF_NOT_OK(localRpc->AsyncWrite());
    asyncRpcManager->AddRequest(localRpc);
    return Status::OK();
}

Status WorkerLocalMasterOCApi::GetP2PMeta(
    GetP2PMetaReqPb &req, std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi,
    std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    PerfPoint point(PerfKey::WORKER_LOCAL_GET_P2P_META);
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "GetP2PMeta Send Status to client failed.");
        return rc;
    }
    using LocalRpc = LocalAsyncRpcRequest<GetP2PMetaReqPb, GetP2PMetaRspPb>;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    auto asyncWriteCallback = [this, serverApi](LocalRpc &localRpc) -> Status {
        auto rc = masterOC_->GetP2PMeta(localRpc.GetServerApi());
        if (rc.IsError()) {
            LOG(ERROR) << "ERROR with GetP2PMeta: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "GetP2PMeta Send StatusG to client failed.");
            return rc;
        }
        return Status::OK();
    };

    auto returnCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = localRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "GetP2PMeta remote rpc failed with : " << pair.second.ToString();
        }
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto timeoutCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = std::make_pair(GetP2PMetaRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto localRpc = std::make_shared<LocalRpc>(req, remainingTime);
    localRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback);

    RETURN_IF_NOT_OK(localRpc->AsyncWrite());
    asyncRpcManager->AddRequest(localRpc);
    return Status::OK();
}

Status WorkerLocalMasterOCApi::SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterOC_->SendRootInfo(req, resp));
    return Status::OK();
}

Status WorkerLocalMasterOCApi::RecvRootInfo(
    RecvRootInfoReqPb &req, std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi,
    std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "RecvRootInfo Send Status to client failed.");
        return rc;
    }

    using LocalRpc = LocalAsyncRpcRequest<RecvRootInfoReqPb, RecvRootInfoRspPb>;
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    auto asyncWriteCallback = [this, serverApi](LocalRpc &localRpc) -> Status {
        auto rc = masterOC_->RecvRootInfo(localRpc.GetServerApi());
        if (rc.IsError()) {
            LOG(ERROR) << "ERROR with RecvRootInfo: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "RecvRootInfo Send StatusG to client failed.");
            return rc;
        }
        return Status::OK();
    };

    auto returnCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = localRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "GetP2PMeta remote rpc failed with : " << pair.second.ToString();
        }
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto timeoutCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = std::make_pair(RecvRootInfoRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto localRpc = std::make_shared<LocalRpc>(req, remainingTime);
    localRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback);

    RETURN_IF_NOT_OK(localRpc->AsyncWrite());
    asyncRpcManager->AddRequest(localRpc);
    return Status::OK();
}

Status WorkerLocalMasterOCApi::AckRecvFinish(AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->AckRecvFinish(req, resp);
}

Status WorkerLocalMasterOCApi::GetDataInfo(
    GetDataInfoReqPb &req,
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
    const int64_t subTimeoutMs, std::shared_ptr<AsyncRpcRequestManager> &asyncRpcManager)
{
    auto remainingTime = reqTimeoutDuration.CalcRemainingTime();
    req.set_sub_timeout(std::min<int64_t>(subTimeoutMs, remainingTime));
    auto rc = akSkManager_->GenerateSignature(req);
    if (rc.IsError()) {
        LOG_IF_ERROR(serverApi->SendStatus(rc), "RecvRootInfo Send Status to client failed.");
        return rc;
    }

    using LocalRpc = LocalAsyncRpcRequest<GetDataInfoReqPb, GetDataInfoRspPb>;

    auto asyncWriteCallback = [this, serverApi, subTimeoutMs](LocalRpc &localRpc) -> Status {
        auto rc = masterOC_->GetDataInfo(localRpc.GetServerApi());
        if (rc.IsError()) {
            LOG(ERROR) << "ERROR with GetDataInfo: " << rc.ToString();
            LOG_IF_ERROR(serverApi->SendStatus(rc), "GetDataInfo Send StatusG to client failed.");
            return rc;
        }
        return Status::OK();
    };

    auto returnCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = localRpc.GetReply();
        if (pair.second.IsError()) {
            LOG(ERROR) << "GetDataInfo remote rpc failed with : " << pair.second.ToString();
        }
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto timeoutCallback = [serverApi](LocalRpc &localRpc) -> Status {
        auto pair = std::make_pair(GetDataInfoRspPb{}, Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc deadline exceeded." });
        return localRpc.ReplyToClient(pair, serverApi);
    };

    auto localRpc = std::make_shared<LocalRpc>(req, remainingTime);
    localRpc->SetCallback(asyncWriteCallback, returnCallback, timeoutCallback);

    RETURN_IF_NOT_OK(localRpc->AsyncWrite());
    asyncRpcManager->AddRequest(localRpc);
    return Status::OK();
}

Status WorkerLocalMasterOCApi::RemoveP2PLocation(RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->RemoveP2PLocation(req, resp);
}

Status WorkerLocalMasterOCApi::GetObjectLocations(master::GetObjectLocationsReqPb &req,
                                                  master::GetObjectLocationsRspPb &resp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->GetObjectLocations(req, resp);
}

Status WorkerLocalMasterOCApi::GetObjectLocations(master::GetObjectLocationsReqPb &req,
                                                  master::GetObjectLocationsRspPb &resp, int64_t timeoutMs)
{
    (void)timeoutMs;
    return GetObjectLocations(req, resp);
}

Status WorkerLocalMasterOCApi::ReleaseMetaData(ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->ReleaseMetaData(req, resp);
}

Status WorkerLocalMasterOCApi::ReplacePrimary(master::ReplacePrimaryReqPb &req, master::ReplacePrimaryRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->ReplacePrimary(req, rsp);
}

Status WorkerLocalMasterOCApi::PureQueryMeta(master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->PureQueryMeta(req, rsp);
}

Status WorkerLocalMasterOCApi::CheckObjectDataLocation(master::CheckObjectDataLocationReqPb &req,
                                                       master::CheckObjectDataLocationRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->CheckObjectDataLocation(req, rsp);
}

Status WorkerLocalMasterOCApi::RollbackMultiMeta(master::RollbackMultiMetaReqPb &req,
                                                 master::RollbackMultiMetaRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->RollbackMultiMeta(req, rsp);
}

void WorkerLocalMasterOCApi::AsyncNotifyCrossAzDelete(
    const std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify)
{
    masterOC_->AsyncNotifyCrossAzDelete(objsNeedAsyncNotify);
}

Status WorkerLocalMasterOCApi::Expire(master::ExpireReqPb &req, master::ExpireRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->Expire(req, rsp);
}
Status WorkerLocalMasterOCApi::GetMetaInfo(GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterOC_->GetMetaInfo(req, rsp);
}
}  // namespace worker
}  // namespace datasystem
