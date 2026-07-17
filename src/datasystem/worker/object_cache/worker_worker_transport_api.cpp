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

#include <algorithm>
#include <cerrno>
#include <limits>
#include <utility>

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/meta_transport.pb.h"

namespace datasystem {
namespace object_cache {

namespace {
long ToBthreadWaitUs(int64_t remainingUs)
{
    return remainingUs > std::numeric_limits<long>::max() ? std::numeric_limits<long>::max()
                                                          : static_cast<long>(remainingUs);
}
}  // namespace

struct WorkerWorkerTransportApi::ExchangeState {
    Status WaitForStateChange(std::unique_lock<bthread::Mutex> &lock)
    {
        int rc = EINTR;
        while (rc == EINTR) {
            int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
            CHECK_FAIL_RETURN_STATUS(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
                                     FormatString("URMA reconnect wait timeout (%ld us).", remainingUs));
            rc = cv.wait_for(lock, ToBthreadWaitUs(remainingUs));
        }
        if (rc == 0) {
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS(rc != ETIMEDOUT, K_RPC_DEADLINE_EXCEEDED,
                                 "URMA reconnect single-flight wait timeout.");
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("URMA reconnect wait failed, errno=%d.", rc));
    }

    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool isExecuting = false;
    bool globalStopFlag = false;
};

WorkerWorkerTransportApi::WorkerWorkerTransportApi() : exchangeState_(new ExchangeState())
{
}

WorkerWorkerTransportApi::~WorkerWorkerTransportApi() = default;

Status WorkerWorkerTransportApi::ExecOnceParrallelExchange(UrmaHandshakeRspPb &rsp)
{
    auto &state = *exchangeState_;
    std::unique_lock<bthread::Mutex> lock(state.mtx);

    if (state.globalStopFlag) {
        while (state.isExecuting) {
#ifdef WITH_TESTS
            OnExchangeWaitForTest();
#endif
            RETURN_IF_NOT_OK(state.WaitForStateChange(lock));
        }
        state.globalStopFlag = false;
    }

    while (state.isExecuting) {
#ifdef WITH_TESTS
        OnExchangeWaitForTest();
#endif
        RETURN_IF_NOT_OK(state.WaitForStateChange(lock));
        if (state.globalStopFlag) {
            return Status::OK();
        }
    }

    state.isExecuting = true;
    lock.unlock();

    Status result(K_RUNTIME_ERROR, "URMA reconnect exchange did not finish.");
    {
        Timer timer;
        Raii finishExchange([&state, &lock, &timer, &result] {
            lock.lock();
            state.isExecuting = false;

            if (result.IsOk()) {
                state.globalStopFlag = true;
                LOG(INFO) << "[URMA_NEED_CONNECT] Worker-worker transport connection exchange success, elapsed ms: "
                          << timer.ElapsedMilliSecond();
                state.cv.notify_all();
            } else {
                LOG(WARNING) << "[URMA_NEED_CONNECT] Worker-worker transport connection exchange failed, elapsed ms: "
                             << timer.ElapsedMilliSecond() << ", status: " << result.ToString();
                state.cv.notify_one();
            }
        });
        result = ExchangeUrmaConnectInfo(rsp);
    }

    return result;
}

WorkerRemoteWorkerTransApi::WorkerRemoteWorkerTransApi(HostPort hostPort, const std::string &firstClientEntityId)
    : hostPort_(std::move(hostPort)), firstClientEntityId_(firstClientEntityId)
{
}

Status WorkerRemoteWorkerTransApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(hostPort_, StubType::WORKER_WORKER_TRANS_SVC, rpcStub));
    if (FLAGS_use_brpc) {
        brpcSession_ = std::dynamic_pointer_cast<WorkerWorkerTransportService_BrpcGenericStub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcSession_);
    } else {
        rpcSession_ = std::dynamic_pointer_cast<WorkerWorkerTransportService_Stub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    }
    return Status::OK();
}

Status WorkerRemoteWorkerTransApi::ExchangeUrmaConnectInfo(UrmaHandshakeRspPb &rsp)
{
    RpcOptions opts;
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    int64_t maxTimeoutMs = 60000;
    remainingTime = std::min(remainingTime, maxTimeoutMs);
    opts.SetTimeout(remainingTime);
    CHECK_FAIL_RETURN_STATUS(rpcSession_ != nullptr || brpcSession_ != nullptr, K_RUNTIME_ERROR, "Rpc session is null");

    UrmaHandshakeReqPb req;
    std::string senderAddStr = hostPort_.ToString();
    RETURN_IF_NOT_OK(ConstructHandshakePb(senderAddStr, req, firstClientEntityId_));
    RETURN_IF_NOT_OK(brpcSession_ ? brpcSession_->WorkerWorkerExchangeUrmaConnectInfo(opts, req, rsp)
                                  : rpcSession_->WorkerWorkerExchangeUrmaConnectInfo(opts, req, rsp));
    RETURN_IF_NOT_OK(FinalizeOutboundConnection(rsp));
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
