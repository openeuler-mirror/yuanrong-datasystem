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
 * Description: UcpWorker class that handles the RDMA put process and managed
 * by UcpWorkerPool. It handles the reuse and removal of resources to previously
 * connected nodes.
 */

#include "datasystem/common/rdma/ucp_worker.h"

#include <cstring>
#include <cassert>
#include <chrono>
#include <iostream>

#include "ucp/api/ucp_def.h"

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/ucp_manager.h"
#include "datasystem/common/rdma/ucp_endpoint.h"

namespace datasystem {

UcpWorker::UcpWorker(const ucp_context_h &ucpContext, UcpManager *manager, const uint32_t &workerId)
    : context_(ucpContext),
      manager_(manager),
      workerId_(workerId),
      errorMsgHead_("[UcpWorker " + std::to_string(workerId_) + "]")
{
}

UcpWorker::~UcpWorker()
{
    Clean();
}

Status UcpWorker::Init()
{
    PerfPoint point(PerfKey::RDMA_UCP_WORKER_INIT);
    ucp_worker_params_t workerParams = {};
    workerParams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    workerParams.thread_mode = UCS_THREAD_MODE_MULTI;

    ucs_status_t status = ucp_worker_create(context_, &workerParams, &worker_);
    if (status != UCS_OK) {
        return Status(K_RDMA_ERROR, errorMsgHead_ + " Failed to create worker.");
    }

    size_t workerAddrLen;

    status = ucp_worker_get_address(worker_, &localWorkerAddr_, &workerAddrLen);
    if (status != UCS_OK) {
        return Status(K_RDMA_ERROR, errorMsgHead_ + " Failed to get worker address.");
    }

    localWorkerAddrStr_ = std::string(reinterpret_cast<const char *>(localWorkerAddr_), workerAddrLen);

    StartProgressThread();

    point.Record();
    return Status::OK();
}

Status UcpWorker::Write(const std::string &remoteRkey, const uintptr_t &remoteSegAddr,
                        const std::string &remoteWorkerAddr, const std::string &ipAddr, const uintptr_t &localSegAddr,
                        size_t localSegSize, uint64_t requestID)
{
    PerfPoint point(PerfKey::RDMA_UCP_WORKER_WRITE);
    PerfPoint point1(PerfKey::RDMA_UCP_WORKER_GET_ENDPOINT);
    const auto &ucpEp = GetOrCreateEndpoint(ipAddr, remoteWorkerAddr);
    if (ucpEp == nullptr) {
        return Status(K_RDMA_ERROR, errorMsgHead_ + " Failed to create Endpoint.");
    }
    const ucp_ep_h &ep = ucpEp->GetEp();
    if (ep == nullptr) {
        return Status(K_RDMA_ERROR, errorMsgHead_ + " UcpEndpoint contained an empty endpoint?");
    }
    point1.Record();

    Status status = ucpEp->UnpackRkey(remoteRkey);
    if (status != Status::OK()) {
        return Status(K_RDMA_ERROR, errorMsgHead_ + " Failed to unpack rkey.");
    }
    const ucp_rkey_h &rkey = ucpEp->GetUnpackedRkey();

    if (rkey == nullptr) {
        return Status(K_RDMA_ERROR, errorMsgHead_ + " Failed to unpack rkey.");
    }

    PerfPoint point3(PerfKey::RDMA_UCP_WORKER_EXECUTE_PUT);
    ucp_request_param_t param;
    memset_s(&param, sizeof(param), 0, sizeof(param));
    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    param.cb.send = CallBack;

    CallbackContext *ctx = new CallbackContext{ this, requestID };
    param.user_data = ctx;
    param.datatype = ucp_dt_make_contig(1);

    void *request =
        ucp_put_nbx(ep, reinterpret_cast<const void *>(localSegAddr), localSegSize, remoteSegAddr, rkey, &param);

    point3.Record();
    point.Record();

    if (UCS_PTR_IS_ERR(request)) {
        ucs_status_t status = UCS_PTR_STATUS(request);
        CallBack(request, status, ctx);
        ucp_request_free(request);
        return Status(K_RDMA_ERROR, errorMsgHead_ + " Failed to send data immediately.");
    } else if (request == nullptr) {
        CallBack(nullptr, UCS_OK, ctx);
        return Status::OK();
    }

    return Status::OK();
}

Status UcpWorker::RmEpByIp(const std::string &ipAddr)
{
    std::unique_lock write_lock(mapLock_);
    auto it = remoteEndpointMap_.find(ipAddr);
    if (it != remoteEndpointMap_.end()) {
        remoteEndpointMap_.erase(it);
        return Status::OK();
    }
    return Status(K_RDMA_ERROR, errorMsgHead_ + " No endpoint found to IP " + ipAddr);
}

void UcpWorker::StartProgressThread()
{
    if (running_.load()) {
        return;
    }
    running_.store(true);
    progressThread_ = std::make_unique<std::thread>(&UcpWorker::ProgressLoop, this);
}

void UcpWorker::StopProgressThread()
{
    if (!running_.load()) {
        return;
    }

    running_.store(false);
    if (progressThread_ && progressThread_->joinable()) {
        progressThread_->join();
        progressThread_.reset();
    }
}

void UcpWorker::ProgressLoop()
{
    while (running_.load()) {
        ucp_worker_progress(worker_);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

UcpEndpoint *UcpWorker::GetOrCreateEndpoint(const std::string &ipAddr, const std::string &remoteWorkerAddr)
{
    {
        std::shared_lock read_lock(mapLock_);

        auto it = remoteEndpointMap_.find(ipAddr);
        if (it != remoteEndpointMap_.end()) {
            return it->second.get();
        }
    }

    std::unique_lock write_lock(mapLock_);

    std::unique_ptr<UcpEndpoint> ep = std::make_unique<UcpEndpoint>(worker_, remoteWorkerAddr);
    Status status = ep->Init();
    if (status != Status::OK()) {
        return nullptr;
    }
    remoteEndpointMap_.emplace(ipAddr, std::move(ep));

    return remoteEndpointMap_[ipAddr].get();
}

void UcpWorker::Clean()
{
    StopProgressThread();

    remoteEndpointMap_.clear();

    if (localWorkerAddr_) {
        ucp_worker_release_address(worker_, localWorkerAddr_);
        localWorkerAddr_ = nullptr;
    }

    if (worker_ != nullptr) {
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
    }
}

void UcpWorker::CallBack(void *request, ucs_status_t status, void *userData)
{
    CallbackContext *ctx = static_cast<CallbackContext *>(userData);

    if (request == nullptr) {
        ctx->worker->manager_->InsertSuccessfulEvent(ctx->request_id);
        return;
    }

    if (status == UCS_OK) {
        ctx->worker->manager_->InsertSuccessfulEvent(ctx->request_id);
    } else {
        ctx->worker->manager_->InsertFailedEvent(ctx->request_id);
    }

    delete ctx;
    ucp_request_free(request);

    return;
}
}  // namespace datasystem