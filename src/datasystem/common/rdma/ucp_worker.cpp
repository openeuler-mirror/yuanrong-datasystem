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

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <poll.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/ucp_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

UcpWorker::UcpWorker(const ucp_context_h &ucpContext, UcpManager *manager, const uint32_t workerId)
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

    ucs_status_t status = ds_ucp_worker_create(context_, &workerParams, &worker_);
    if (status != UCS_OK) {
        LOG(ERROR) << errorMsgHead_ << " Failed to create worker. Status: " << ds_ucs_status_string(status);
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to create worker.");
    }

    size_t workerAddrLen;

    status = ds_ucp_worker_get_address(worker_, &localWorkerAddr_, &workerAddrLen);
    if (status != UCS_OK) {
        LOG(ERROR) << errorMsgHead_ << " Failed to get worker address. Status: " << ds_ucs_status_string(status);
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to get worker address.");
    }

    localWorkerAddrStr_ = std::string(reinterpret_cast<const char *>(localWorkerAddr_), workerAddrLen);

    StartProgressThread();

    point.Record();
    return Status::OK();
}

Status UcpWorker::Write(const std::string &remoteRkey, const uintptr_t remoteSegAddr,
                        const std::string &remoteWorkerAddr, const std::string &ipAddr, const uintptr_t localSegAddr,
                        size_t localSegSize, uint64_t requestID)
{
    PerfPoint point(PerfKey::RDMA_UCP_WORKER_WRITE);
    const auto &ucpEp = GetOrCreateEndpoint(ipAddr, remoteWorkerAddr);
    if (ucpEp == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to create Endpoint.");
    }
    const ucp_ep_h &ep = ucpEp->GetEp();
    if (ep == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " UcpEndpoint contained an empty endpoint?");
    }
    point.RecordAndReset(PerfKey::RDMA_UCP_WORKER_WRITE);

    ucp_rkey_h rkey = ucpEp->GetOrUnpackRkey(remoteRkey);
    if (rkey == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to get an unpack rkey.");
    }

    ucp_request_param_t putParam{};

    void *putRequest =
        ds_ucp_put_nbx(ep, reinterpret_cast<const void *>(localSegAddr), localSegSize, remoteSegAddr, rkey, &putParam);

    point.RecordAndReset(PerfKey::RDMA_UCP_WORKER_WRITE);

    if (UCS_PTR_IS_ERR(putRequest)) {
        ucs_status_t status = UCS_PTR_STATUS(putRequest);
        LOG(ERROR) << errorMsgHead_ << " Failed to execute ucp_put_nbx. Status: " << ds_ucs_status_string(status);
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to send data immediately.");
    }

    ucp_request_param_t flushParam{};
    flushParam.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    flushParam.cb.send = CallBack;
    CallbackContext *flushCtx = new CallbackContext{ this, requestID, putRequest };
    flushParam.user_data = flushCtx;

    void *flushRequest = ds_ucp_ep_flush_nbx(ep, &flushParam);
    if (UCS_PTR_IS_ERR(flushRequest)) {
        ucs_status_t status = UCS_PTR_STATUS(flushRequest);
        LOG(ERROR) << errorMsgHead_ << " Failed to execute ucp_ep_flush_nbx. Status: " << ds_ucs_status_string(status);
        if (putRequest != nullptr) {
            ds_ucp_request_free(putRequest);
        }
        delete flushCtx;
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to flush ep immediately.");
    } else if (flushRequest == nullptr) {
        CallBack(flushRequest, UCS_OK, flushCtx);
    }

    return Status::OK();
}

Status UcpWorker::RemoveEndpointByIp(const std::string &ipAddr)
{
    std::unique_lock writeLock(mapLock_);
    if (remoteEndpointMap_.erase(ipAddr) > 0) {
        return Status::OK();
    }
    LOG(WARNING) << errorMsgHead_ << " No endpoint found to IP " << ipAddr;
    return Status::OK();
}

void UcpWorker::StartProgressThread()
{
    if (running_.load()) {
        return;
    }
    running_.store(true);
    progressThread_ = std::make_unique<Thread>(&UcpWorker::ProgressLoop, this);
    progressThread_->set_name("UcpWorker_" + std::to_string(workerId_) + "_Progress");
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
    int fd;
    ucs_status_t status = ds_ucp_worker_get_efd(worker_, &fd);
    if (status != UCS_OK) {
        LOG(ERROR) << errorMsgHead_ << " Failed to get efd. Status: " << ds_ucs_status_string(status);
    }

    while (running_.load()) {
        bool innerBreak = false;
        while (ds_ucp_worker_progress(worker_)) {
            if (!running_.load()) {
                innerBreak = true;
                break;
            }
        };

        if (innerBreak) {
            break;
        }

        status = ds_ucp_worker_arm(worker_);
        if (status == UCS_ERR_BUSY) {
            // meaning there are new jobs in QP again, just restart the while loop
            continue;
        }
        if (status != UCS_OK) {
            LOG(ERROR) << errorMsgHead_ << " Failed to arm worker. Status: " << ds_ucs_status_string(status);
        }

        struct pollfd pfd = { fd, POLLIN, 0 };
        int ret = poll(&pfd, 1, WORKER_PROGRESS_SLEEP_TIMEOUT_MS);
        if (ret < 0 && errno != EINTR) {
            LOG(ERROR) << errorMsgHead_ << " Progress thread failed to poll. Error: " << strerror(errno);
        }
    }
}

std::shared_ptr<UcpEndpoint> UcpWorker::GetOrCreateEndpoint(const std::string &ipAddr,
                                                            const std::string &remoteWorkerAddr)
{
    {
        std::shared_lock readLock(mapLock_);

        auto it = remoteEndpointMap_.find(ipAddr);
        if (it != remoteEndpointMap_.end()) {
            return it->second;
        }
    }

    std::unique_lock writeLock(mapLock_);

    auto it = remoteEndpointMap_.find(ipAddr);
    if (it != remoteEndpointMap_.end()) {
        return it->second;
    }

    std::shared_ptr<UcpEndpoint> ep = std::make_shared<UcpEndpoint>(worker_, remoteWorkerAddr);
    Status status = ep->Init();
    if (status.IsError()) {
        LOG(ERROR) << "In " << errorMsgHead_ << ": " << status.ToString();
        return nullptr;
    }
    remoteEndpointMap_.emplace(ipAddr, std::move(ep));

    return remoteEndpointMap_[ipAddr];
}

void UcpWorker::Clean()
{
    StopProgressThread();

    remoteEndpointMap_.clear();

    if (localWorkerAddr_) {
        ds_ucp_worker_release_address(worker_, localWorkerAddr_);
        localWorkerAddr_ = nullptr;
    }

    if (worker_ != nullptr) {
        ds_ucp_worker_destroy(worker_);
        worker_ = nullptr;
    }
}

void UcpWorker::CallBack(void *request, ucs_status_t status, void *userData)
{
    CallbackContext *ctx = static_cast<CallbackContext *>(userData);

    if (request == nullptr) {
        ctx->worker->manager_->InsertSuccessfulEvent(ctx->request_id);
        delete ctx;
        return;
    }

    if (status == UCS_OK) {
        ctx->worker->manager_->InsertSuccessfulEvent(ctx->request_id);
    } else {
        LOG(ERROR) << ctx->worker->errorMsgHead_ << " Put failed. Status: " << ds_ucs_status_string(status);
        ctx->worker->manager_->InsertFailedEvent(ctx->request_id);
    }

    if (ctx->put_request) {
        ds_ucp_request_free(ctx->put_request);
    }

    delete ctx;
    ds_ucp_request_free(request);
}
}  // namespace datasystem