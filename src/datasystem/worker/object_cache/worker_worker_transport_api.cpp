/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#include "datasystem/worker/object_cache/worker_worker_transport_api.h"

#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/worker_worker_transport_service_impl.h"

namespace datasystem {
namespace object_cache {

Status WorkerWorkerTransportApi::ExecOnceParrallelExchange(UrmaHandshakeRspPb &rsp)
{
    std::unique_lock<std::mutex> lock(mtx_);

    if (globalStopFlag_) {
        cv_.wait(lock, [this]() { return !isExecuting_; });
        // reset flag for new thread
        globalStopFlag_ = false;
    }

    while (isExecuting_) {
        cv_.wait(lock);
        // wakeup until one thread succeed
        if (globalStopFlag_) {
            return Status::OK();
        }
    }

    // set executing flag
    isExecuting_ = true;
    lock.unlock();

    auto result = ExchangeUrmaConnectInfo(rsp);

    lock.lock();
    isExecuting_ = false;

    if (result.IsOk()) {
        // set all finish flag
        globalStopFlag_ = true;
        cv_.notify_all();
    } else {
        // wake up one thread to retury
        cv_.notify_one();
    }

    return result;
}

WorkerLocalWorkerTransApi::WorkerLocalWorkerTransApi(HostPort localHost, WorkerWorkerTransportServiceImpl *service)
    : localHost_(localHost), service_(service)
{
}

Status WorkerLocalWorkerTransApi::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(service_);
    return Status::OK();
}

Status WorkerLocalWorkerTransApi::ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &rsp)
{
    UrmaHandshakeReqPb req;
    std::string localHost = localHost_.ToString();
    RETURN_IF_NOT_OK(ConstructHandshakePb(localHost, req));
    return service_->WorkerWorkerExchangeUrmaConnectInfo(req, rsp);
}

WorkerRemoteWorkerTransApi::WorkerRemoteWorkerTransApi(HostPort hostPort) : hostPort_(std::move(hostPort))
{
}

Status WorkerRemoteWorkerTransApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(hostPort_, StubType::WORKER_WORKER_TRANS_SVC, rpcStub));
    rpcSession_ = std::dynamic_pointer_cast<WorkerWorkerTransportService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    return Status::OK();
}

Status WorkerRemoteWorkerTransApi::ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &rsp)
{
    RpcOptions opts;
    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    int64_t maxTimeoutMs = 60000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");

    UrmaHandshakeReqPb req;
    std::string senderAddStr = hostPort_.ToString();
    RETURN_IF_NOT_OK(ConstructHandshakePb(senderAddStr, req));
    RETURN_IF_NOT_OK(rpcSession_->WorkerWorkerExchangeUrmaConnectInfo(opts, req, rsp));
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
