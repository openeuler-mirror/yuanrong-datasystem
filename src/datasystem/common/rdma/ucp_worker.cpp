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

#include <chrono>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
constexpr uint32_t DEFAULT_COMM_SUBMIT_BUDGET = 128;
constexpr uint32_t COMM_IDLE_WAIT_US = 100;

uint64_t GetSteadyClockNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

uint32_t GetCommSubmitBudget()
{
    return DEFAULT_COMM_SUBMIT_BUDGET;
}

}  // namespace

UcpWorker::UcpWorker(const ucp_context_h &ucpContext, const uint32_t workerId)
    : context_(ucpContext),
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
    ucp_worker_params_t workerParams = {};
    workerParams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    workerParams.thread_mode = UCS_THREAD_MODE_SERIALIZED;

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

    StartSubmitThread();

    return Status::OK();
}

Status UcpWorker::Write(const std::string &remoteRkey, const uintptr_t remoteSegAddr,
                        const std::string &remoteWorkerAddr, const std::string &ipAddr, const uintptr_t localSegAddr,
                        size_t localSegSize, uint64_t requestID, std::shared_ptr<Event> event)
{
    auto req = std::make_shared<SubmitRequest>();
    req->remoteRkey = remoteRkey;
    req->remoteAddr = remoteSegAddr;
    req->remoteWorkerAddr = remoteWorkerAddr;
    req->ipAddr = ipAddr;
    req->localSegAddr = localSegAddr;
    req->localSegSize = localSegSize;
    req->requestID = requestID;
    req->event = std::move(event);
    req->enqueueNs = GetSteadyClockNs();
    return EnqueueSubmit(std::move(req));
}

Status UcpWorker::WriteDirect(const std::string &remoteRkey, uintptr_t remoteSegAddr,
                              const std::string &remoteWorkerAddr, const std::string &ipAddr, uintptr_t localSegAddr,
                              size_t localSegSize, uint64_t requestID, std::shared_ptr<Event> event)
{
    PerfPoint totalPoint(PerfKey::RDMA_UCP_WORKER_WRITE_TOTAL);
    const auto &ucpEp = GetOrCreateEndpoint(ipAddr, remoteWorkerAddr);
    if (ucpEp == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to create Endpoint.");
    }
    const ucp_ep_h &ep = ucpEp->GetEp();
    if (ep == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " UcpEndpoint contained an empty endpoint?");
    }

    ucp_rkey_h rkey = ucpEp->GetOrUnpackRkey(remoteRkey);
    if (rkey == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + ipAddr + " Failed to get an unpack rkey.");
    }

    ucp_request_param_t putParam{};

    void *putRequest =
        ds_ucp_put_nbx(ep, reinterpret_cast<const void *>(localSegAddr), localSegSize, remoteSegAddr, rkey, &putParam);

    if (UCS_PTR_IS_ERR(putRequest)) {
        ucs_status_t status = UCS_PTR_STATUS(putRequest);
        LOG(ERROR) << errorMsgHead_ << " Failed to execute ucp_put_nbx. Status: " << ds_ucs_status_string(status);
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to send data immediately.");
    }

    CallbackContext *flushCtx =
        new CallbackContext{ this, ucpEp, requestID, putRequest, nullptr, std::move(event), GetSteadyClockNs() };
    EnqueueFlush(flushCtx);
    totalPoint.Record();

    return Status::OK();
}

std::vector<ucp_dt_iov_t> *UcpWorker::PrepareIovBuffer(const std::vector<IovSegment> &segments)
{
    std::vector<ucp_dt_iov_t> *iov = new std::vector<ucp_dt_iov_t>(segments.size());
    for (size_t i = 0; i < segments.size(); ++i) {
        (*iov)[i].buffer = reinterpret_cast<void *>(segments[i].localAddr);
        (*iov)[i].length = segments[i].size;
    }
    return iov;
}

Status UcpWorker::WriteN(const std::string &remoteRkey, uintptr_t remoteBaseAddr, const std::string &remoteWorkerAddr,
                         const std::string &ipAddr, const std::vector<IovSegment> &segments, uint64_t requestID,
                         std::shared_ptr<Event> event)
{
    auto req = std::make_shared<SubmitRequest>();
    req->isIov = true;
    req->remoteRkey = remoteRkey;
    req->remoteAddr = remoteBaseAddr;
    req->remoteWorkerAddr = remoteWorkerAddr;
    req->ipAddr = ipAddr;
    req->segments = segments;
    req->requestID = requestID;
    req->event = std::move(event);
    req->enqueueNs = GetSteadyClockNs();
    return EnqueueSubmit(std::move(req));
}

Status UcpWorker::WriteNDirect(const std::string &remoteRkey, uintptr_t remoteBaseAddr,
                               const std::string &remoteWorkerAddr, const std::string &ipAddr,
                               const std::vector<IovSegment> &segments, uint64_t requestID,
                               std::shared_ptr<Event> event)
{
    if (segments.empty()) {
        return Status::OK();
    }

    const auto &ucpEp = GetOrCreateEndpoint(ipAddr, remoteWorkerAddr);
    if (ucpEp == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to create Endpoint.");
    }
    const ucp_ep_h &ep = ucpEp->GetEp();
    if (ep == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " UcpEndpoint contained an empty endpoint?");
    }

    ucp_rkey_h rkey = ucpEp->GetOrUnpackRkey(remoteRkey);
    if (rkey == nullptr) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + ipAddr + " Failed to get an unpack rkey.");
    }

    std::vector<ucp_dt_iov_t> *iov = PrepareIovBuffer(segments);
    ucp_request_param_t putParam{};
    putParam.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
    putParam.datatype = ucp_dt_make_iov();

    void *putRequest = ds_ucp_put_nbx(ep, iov->data(), segments.size(), remoteBaseAddr, rkey, &putParam);
    if (UCS_PTR_IS_ERR(putRequest)) {
        ucs_status_t status = UCS_PTR_STATUS(putRequest);
        LOG(ERROR) << errorMsgHead_
                   << " Failed to execute ucp_put_nbx with IOV. Status: " << ds_ucs_status_string(status);
        delete iov;
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Failed to send data immediately with IOV.");
    }

    EnqueueFlush(new CallbackContext{ this, ucpEp, requestID, putRequest, iov, std::move(event), GetSteadyClockNs() });

    return Status::OK();
}

Status UcpWorker::RemoveEndpointByIp(const std::string &ipAddr)
{
    std::unique_lock writeLock(mapLock_);
    if (remoteEndpointMap_.erase(ipAddr) > 0) {
        return Status::OK();
    }
    VLOG(1) << errorMsgHead_ << " Try to remove by IP but never sent to " << ipAddr;
    return Status::OK();
}

void UcpWorker::StartSubmitThread()
{
    if (submitRunning_.load()) {
        return;
    }
    submitRunning_.store(true);
    submitThread_ = std::make_unique<Thread>(&UcpWorker::SubmitLoop, this);
    submitThread_->set_name("UcpWorker_" + std::to_string(workerId_) + "_Submit");
}

void UcpWorker::StopSubmitThread()
{
    if (!submitRunning_.load()) {
        return;
    }

    submitRunning_.store(false);
    submitCv_.notify_all();
    if (submitThread_ && submitThread_->joinable()) {
        submitThread_->join();
        submitThread_.reset();
    }
}

void UcpWorker::EnqueueFlush(CallbackContext *ctx)
{
    {
        std::lock_guard<std::mutex> lock(submitMutex_);
        flushQueue_.emplace_back(ctx);
    }
    submitCv_.notify_one();
}

Status UcpWorker::EnqueueSubmit(std::shared_ptr<SubmitRequest> req)
{
    if (!submitRunning_.load()) {
        RETURN_STATUS(K_RDMA_ERROR, errorMsgHead_ + " Submit thread is not running.");
    }

    {
        std::lock_guard<std::mutex> lock(submitMutex_);
        submitQueue_.emplace_back(req);
    }
    submitCv_.notify_one();

    return Status::OK();
}

void UcpWorker::SubmitLoop()
{
    for (;;) {
        std::deque<std::shared_ptr<SubmitRequest>> pendingSubmits;
        {
            std::unique_lock<std::mutex> lock(submitMutex_);
            if (submitRunning_.load() && submitQueue_.empty() && flushQueue_.empty() && outstandingFlushes_ == 0) {
                submitCv_.wait_for(lock, std::chrono::microseconds(COMM_IDLE_WAIT_US));
            }
            if (!submitRunning_.load() && submitQueue_.empty() && flushQueue_.empty() && outstandingFlushes_ == 0) {
                break;
            }

            const uint32_t budget = GetCommSubmitBudget();
            while (!submitQueue_.empty() && pendingSubmits.size() < budget) {
                pendingSubmits.emplace_back(std::move(submitQueue_.front()));
                submitQueue_.pop_front();
            }
        }

        for (auto &req : pendingSubmits) {
            PerfPoint::RecordElapsed(PerfKey::RDMA_UCP_WORKER_SUBMIT_QUEUE_WAIT,
                                     GetSteadyClockNs() - req->enqueueNs);
            Status status = req->isIov
                                ? WriteNDirect(req->remoteRkey, req->remoteAddr, req->remoteWorkerAddr, req->ipAddr,
                                               req->segments, req->requestID, req->event)
                                : WriteDirect(req->remoteRkey, req->remoteAddr, req->remoteWorkerAddr, req->ipAddr,
                                              req->localSegAddr, req->localSegSize, req->requestID, req->event);
            if (status.IsError() && req->event != nullptr) {
                req->event->SetFailed();
                req->event->NotifyAll();
            }
        }

        std::deque<CallbackContext *> pendingFlushes;
        {
            std::lock_guard<std::mutex> lock(submitMutex_);
            pendingFlushes.swap(flushQueue_);
        }
        std::unordered_map<ucp_ep_h, std::vector<CallbackContext *>> batches;
        for (auto *ctx : pendingFlushes) {
            batches[ctx->ep].emplace_back(ctx);
        }
        for (auto &entry : batches) {
            auto *batchCtx = new FlushBatchContext{ this, std::move(entry.second), false };
            PerfPoint::RecordElapsed(PerfKey::RDMA_UCP_WORKER_FLUSH_BATCH_SIZE, batchCtx->contexts.size());
            ucp_request_param_t flushParam{};
            flushParam.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
            flushParam.cb.send = FlushCallBack;
            flushParam.user_data = batchCtx;

            void *flushRequest = ds_ucp_ep_flush_nbx(entry.first, &flushParam);
            if (UCS_PTR_IS_ERR(flushRequest)) {
                ucs_status_t status = UCS_PTR_STATUS(flushRequest);
                LOG(ERROR) << errorMsgHead_ << " Failed to execute ucp_ep_flush_nbx. Status: "
                           << ds_ucs_status_string(status);
                FinishBatch(batchCtx, true);
            } else if (flushRequest == nullptr) {
                FlushCallBack(flushRequest, UCS_OK, batchCtx);
            } else {
                batchCtx->inflight = true;
                ++outstandingFlushes_;
            }
        }

        bool progressed = false;
        while (ds_ucp_worker_progress(worker_)) {
            progressed = true;
        }
        if (!progressed && outstandingFlushes_ > 0) {
            std::unique_lock<std::mutex> lock(submitMutex_);
            submitCv_.wait_for(lock, std::chrono::microseconds(COMM_IDLE_WAIT_US), [this]() {
                return !submitRunning_.load() || !submitQueue_.empty() || !flushQueue_.empty();
            });
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
    StopSubmitThread();

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

void UcpWorker::FinishContext(CallbackContext *ctx, bool failed)
{
    if (ctx->flush_start_ns != 0) {
        PerfPoint::RecordElapsed(PerfKey::RDMA_UCP_WORKER_FLUSH_CALLBACK,
                                 GetSteadyClockNs() - ctx->flush_start_ns);
    }
    if (ctx->event != nullptr) {
        if (failed) {
            ctx->event->SetFailed();
        }
        ctx->event->NotifyAll();
    }

    if (ctx->put_request != nullptr) {
        ds_ucp_request_free(ctx->put_request);
    }
    if (ctx->iov != nullptr) {
        delete ctx->iov;
    }
    delete ctx;
}

void UcpWorker::FinishBatch(FlushBatchContext *batchCtx, bool failed)
{
    for (auto *ctx : batchCtx->contexts) {
        FinishContext(ctx, failed);
    }
    delete batchCtx;
}

void UcpWorker::FlushCallBack(void *request, ucs_status_t status, void *userData)
{
    auto *batchCtx = static_cast<FlushBatchContext *>(userData);
    const bool failed = status != UCS_OK;
    if (failed) {
        LOG(ERROR) << batchCtx->worker->errorMsgHead_ << " Flush failed. Status: " << ds_ucs_status_string(status);
    }
    if (batchCtx->inflight) {
        if (batchCtx->worker->outstandingFlushes_ == 0) {
            LOG(ERROR) << batchCtx->worker->errorMsgHead_ << " Outstanding flush counter underflow.";
        } else {
            --batchCtx->worker->outstandingFlushes_;
        }
    }
    batchCtx->worker->FinishBatch(batchCtx, failed);
    if (request != nullptr) {
        ds_ucp_request_free(request);
    }
}
}  // namespace datasystem
