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

#include "datasystem/worker/stream_cache/worker_master_sc_api.h"

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/master/stream_cache/master_sc_service_impl.h"
#include "datasystem/utils/optional.h"

DS_DECLARE_string(unix_domain_socket_dir);

namespace datasystem {
namespace worker {
namespace stream_cache {
static constexpr int64_t WORKER_TIMEOUT_MINUS_MILLISECOND = 5 * 1000;
static constexpr float WORKER_TIMEOUT_DESCEND_FACTOR = 0.9;

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
    return std::max(int64_t(timeout * WORKER_TIMEOUT_DESCEND_FACTOR), timeout - WORKER_TIMEOUT_MINUS_MILLISECOND);
}

// Base class methods
WorkerMasterSCApi::WorkerMasterSCApi(const HostPort &localWorkerAddress, std::shared_ptr<AkSkManager> akSkManager)
    : localWorkerAddress_(localWorkerAddress), akSkManager_(std::move(akSkManager))
{
}

std::shared_ptr<WorkerMasterSCApi> WorkerMasterSCApi::CreateWorkerMasterSCApi(const HostPort &hostPort,
                                                                              const HostPort &localHostPort,
                                                                              std::shared_ptr<AkSkManager> akSkManager,
                                                                              master::MasterSCServiceImpl *service)
{
    if (hostPort != localHostPort) {
        LOG(INFO) << "Worker and master are not collocated. Creating a WorkerMasterSCApi as RPC-based api.";
        return std::make_shared<WorkerRemoteMasterSCApi>(hostPort, localHostPort, akSkManager);
    }

    if (service == nullptr) {
        LOG(INFO) << "Worker and master are collocated but the master service is not provided. Local bypass disabled.";
        return std::make_shared<WorkerRemoteMasterSCApi>(hostPort, localHostPort, akSkManager);
    }

    LOG(INFO) << "Worker and master are collocated. Creating a WorkerMasterSCApi with local bypass optimization.";
    return std::make_shared<WorkerLocalMasterSCApi>(service, localHostPort, akSkManager);
}

// WorkerRemoteMasterSCApi methods

WorkerRemoteMasterSCApi::WorkerRemoteMasterSCApi(const HostPort &masterAddress, const HostPort &localHostPort,
                                                 std::shared_ptr<AkSkManager> akSkManager)
    : WorkerMasterSCApi(localHostPort, akSkManager), masterAddress_(masterAddress)
{
}

Status WorkerRemoteMasterSCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(
        RpcStubCacheMgr::Instance().GetStub(masterAddress_, StubType::WORKER_MASTER_SC_SVC, rpcStub));
    rpcSession_ = std::dynamic_pointer_cast<master::MasterSCService_Stub>(rpcStub);
    RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    return Status::OK();
}

Status WorkerRemoteMasterSCApi::CreateProducer(master::CreateProducerReqPb &req, master::CreateProducerRspPb &rsp)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(scTimeoutDuration, req, opts);
    INJECT_POINT("worker.CreateProducer.beforeSendToMaster", [&opts](const std::string &code) {
        const int timeout = 10 * 1000;  // 10s;
        opts.SetTimeout(timeout);
        RETURN_STATUS(GetStatusCodeByName(code), "inject status");
    });
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->CreateProducer(opts, req, rsp));
    INJECT_POINT("worker.CreateProducer.afterSendToMaster");

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Add new pub node on master success", LogPrefix(),
                                              req.producer_meta().stream_name());
    return Status::OK();
}

Status WorkerRemoteMasterSCApi::CloseProducer(master::CloseProducerReqPb &req, master::CloseProducerRspPb &rsp)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(scTimeoutDuration, req, opts);
    INJECT_POINT("worker.CloseProducer.beforeSendToMaster", [&opts](const std::string &code) {
        const int timeout = 10 * 1000;  // 10s;
        opts.SetTimeout(timeout);
        RETURN_STATUS(GetStatusCodeByName(code), "inject status");
    });
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->CloseProducer(opts, req, rsp));
    INJECT_POINT("worker.CloseProducer.afterSendToMaster");

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Closing %d producers on master success", LogPrefix(),
                                              req.producer_infos_size());
    return Status::OK();
}

Status WorkerRemoteMasterSCApi::Subscribe(master::SubscribeReqPb &req, master::SubscribeRspPb &rsp)
{
    // Construct master::SubscribeReqPb req
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(scTimeoutDuration, req, opts);
    INJECT_POINT("worker.Subscribe.beforeSendToMaster", [&opts](const std::string &code) {
        const int timeout = 10 * 1000;  // 10s;
        opts.SetTimeout(timeout);
        RETURN_STATUS(GetStatusCodeByName(code), "inject status");
    });

    INJECT_POINT("worker.Subscribe.sleepReturnTimeout", [&opts](int sleepTimeMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        // Simulates timeout
        opts.SetTimeout(0);
        return Status::OK();
    });
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->Subscribe(opts, req, rsp));
    INJECT_POINT("worker.Subscribe.afterSendToMaster");
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s] Add new consumer on master succeeded", LogPrefix(),
                                              req.consumer_meta().stream_name(), req.consumer_meta().consumer_id());
    return Status::OK();
}

Status WorkerRemoteMasterSCApi::CloseConsumer(master::CloseConsumerReqPb &req, master::CloseConsumerRspPb &rsp)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(scTimeoutDuration, req, opts);
    INJECT_POINT("worker.CloseConsumer.beforeSendToMaster", [&opts](const std::string &code) {
        const int timeout = 10 * 1000;  // 10s;
        opts.SetTimeout(timeout);
        RETURN_STATUS(GetStatusCodeByName(code), "inject status");
    });
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->CloseConsumer(opts, req, rsp));
    INJECT_POINT("worker.CloseConsumer.afterSendToMaster");

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s] Delete consumer on master succeeded", LogPrefix(),
                                              req.consumer_meta().stream_name(), req.consumer_meta().consumer_id());
    return Status::OK();
}

Status WorkerRemoteMasterSCApi::DeleteStream(master::DeleteStreamReqPb &req, master::DeleteStreamRspPb &rsp)
{
    RpcOptions opts;
    CHECK_AND_SET_TIMEOUT(scTimeoutDuration, req, opts);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->DeleteStream(opts, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream succeeded.", LogPrefix(), req.stream_name());
    return Status::OK();
}

Status WorkerRemoteMasterSCApi::QueryGlobalProducersNum(master::QueryGlobalNumReqPb &req,
                                                        master::QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->QueryGlobalProducersNum(req, rsp);
}

Status WorkerRemoteMasterSCApi::QueryGlobalConsumersNum(master::QueryGlobalNumReqPb &req,
                                                        master::QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return rpcSession_->QueryGlobalConsumersNum(req, rsp);
}

std::string WorkerRemoteMasterSCApi::LogPrefix() const
{
    return FormatString("WorkerMasterApi, EndPoint:%s", masterAddress_.ToString());
}

// WorkerLocalMasterSCApi methods

WorkerLocalMasterSCApi::WorkerLocalMasterSCApi(master::MasterSCServiceImpl *service, const HostPort &localHostPort,
                                               std::shared_ptr<AkSkManager> akSkManager)
    : WorkerMasterSCApi(localHostPort, akSkManager), masterSC_(service)
{
}

Status WorkerLocalMasterSCApi::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(masterSC_);
    return Status::OK();
}

Status WorkerLocalMasterSCApi::CreateProducer(master::CreateProducerReqPb &req, master::CreateProducerRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    INJECT_POINT("worker.CreateProducer.beforeSendToMaster", [&opts](const std::string &code) {
        const int timeout = 10 * 1000;  // 10s;
        opts.SetTimeout(timeout);
        RETURN_STATUS(GetStatusCodeByName(code), "inject status");
    });
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterSC_->CreateProducerImpl(nullptr, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Add new pub node on master success", LogPrefix(),
                                              req.producer_meta().stream_name());
    return Status::OK();
}

Status WorkerLocalMasterSCApi::CloseProducer(master::CloseProducerReqPb &req, master::CloseProducerRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterSC_->CloseProducerImpl(nullptr, req, rsp));

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Closing %d producers on master success", LogPrefix(),
                                              req.producer_infos_size());
    return Status::OK();
}

Status WorkerLocalMasterSCApi::Subscribe(master::SubscribeReqPb &req, master::SubscribeRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterSC_->SubscribeImpl(nullptr, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s] Add new consumer on master succeeded", LogPrefix(),
                                              req.consumer_meta().stream_name(), req.consumer_meta().consumer_id());
    return Status::OK();
}

Status WorkerLocalMasterSCApi::CloseConsumer(master::CloseConsumerReqPb &req, master::CloseConsumerRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterSC_->CloseConsumerImpl(nullptr, req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s] Delete consumer on master succeeded", LogPrefix(),
                                              req.consumer_meta().stream_name(), req.consumer_meta().consumer_id());
    return Status::OK();
}

Status WorkerLocalMasterSCApi::DeleteStream(master::DeleteStreamReqPb &req, master::DeleteStreamRspPb &rsp)
{
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK(masterSC_->DeleteStream(req, rsp));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream succeeded.", LogPrefix(), req.stream_name());
    return Status::OK();
}

Status WorkerLocalMasterSCApi::QueryGlobalProducersNum(master::QueryGlobalNumReqPb &req,
                                                       master::QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterSC_->QueryGlobalProducersNum(req, rsp);
}

Status WorkerLocalMasterSCApi::QueryGlobalConsumersNum(master::QueryGlobalNumReqPb &req,
                                                       master::QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    return masterSC_->QueryGlobalConsumersNum(req, rsp);
}

std::string WorkerLocalMasterSCApi::LogPrefix() const
{
    // local version of the api, the endpoint is ourself! (localWorkerAddress)
    return FormatString("WorkerMasterApi, EndPoint:%s", localWorkerAddress_.ToString());
}

WorkerMasterSCApiManager::WorkerMasterSCApiManager(HostPort &hostPort, std::shared_ptr<AkSkManager> akSkManager,
                                                   master::MasterSCServiceImpl *masterSCService)
    : WorkerMasterApiManagerBase<WorkerMasterSCApi>(hostPort, akSkManager), masterSCService_(masterSCService)
{
}

std::shared_ptr<WorkerMasterSCApi> WorkerMasterSCApiManager::CreateWorkerMasterApi(const HostPort &masterAddress)
{
    return WorkerMasterSCApi::CreateWorkerMasterSCApi(masterAddress, workerAddr_, akSkManager_, masterSCService_);
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
