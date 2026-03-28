/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Implement stream cache base class for producer and consumer.
 */

#include "datasystem/client/stream_cache/producer_consumer_worker_api.h"
#include <memory>
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace client {
namespace stream_cache {
ProducerConsumerWorkerApi::ProducerConsumerWorkerApi(const std::string tenantId,
                                                     std::shared_ptr<ClientWorkerApi> workerApi)
    : tenantId_(tenantId), workerApi_(std::move(workerApi)){};

Status ProducerConsumerWorkerApi::GetDataPage(GetDataPageReqPb &req, ShmView &outPage)
{
    int64_t timeoutMs = req.timeout_ms();
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    req.set_client_id(workerApi_->clientId_);
    RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
    GetDataPageRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            workerApi_->rpcTimeoutMs_,
            [this, &req, &rsp, timeoutMs](int32_t currDefaultRpcTimeout) {
                auto pair = workerApi_->GetRpcTimeout(timeoutMs, currDefaultRpcTimeout);
                RpcOptions opts;
                opts.SetTimeout(pair.first);
                reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(opts.GetTimeout()));
                req.set_timeout_ms(pair.second);
                RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
                return workerApi_->rpcSession_->GetDataPage(opts, req, rsp);
            },
            []() { return Status::OK(); }, retryCode_),
        FormatString("[%s, S:%s, C:%s] Get data page failed", workerApi_->LogPrefix(), req.stream_name(),
                     req.consumer_id()));
    outPage.off = static_cast<ptrdiff_t>(rsp.page_view().offset());
    outPage.sz = rsp.page_view().size();
    outPage.mmapSz = rsp.page_view().mmap_size();
    outPage.fd = rsp.page_view().fd();
    return Status::OK();
}

Status ProducerConsumerWorkerApi::AllocBigElementMemory(const std::string &streamName, const std::string &producerId,
                                                        size_t sizeNeeded, int64_t timeoutMs, ShmView &outView)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    CreateLobPageReqPb req;
    req.set_stream_name(streamName);
    req.set_producer_id(producerId);
    req.set_page_size(sizeNeeded);
    req.set_client_id(workerApi_->clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    CreateLobPageRspPb rsp;
    PerfPoint point(PerfKey::RPC_WORKER_CREATE_WRITE_PAGE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            workerApi_->rpcTimeoutMs_,
            [this, &req, &rsp, timeoutMs](int32_t currDefaultRpcTimeout) {
                auto pair = workerApi_->GetRpcTimeout(timeoutMs, currDefaultRpcTimeout);
                RpcOptions opts;
                opts.SetTimeout(pair.first);
                reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(opts.GetTimeout()));
                req.set_sub_timeout(pair.second);
                // Even without AKSK authentication, this field should still be set in this scenario because worker
                // relies on this field to determine the order of requests.
                req.set_timestamp(GetSystemClockTimeStampUs());
                RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
                return workerApi_->rpcSession_->AllocBigShmMemory(opts, req, rsp);
            },
            []() { return Status::OK(); }, retryCode_),
        FormatString("[%s, S:%s, P:%s] Create big element page failed", workerApi_->LogPrefix(), streamName,
                     producerId));
    point.Record();
    outView.off = static_cast<ptrdiff_t>(rsp.page_view().offset());
    outView.sz = rsp.page_view().size();
    outView.mmapSz = rsp.page_view().mmap_size();
    outView.fd = rsp.page_view().fd();
    LOG(INFO) << FormatString("[%s, S:%s, P:%s] Client created big element page success. ShmView %s",
                              workerApi_->LogPrefix(), streamName, producerId, outView.ToStr());
    return Status::OK();
}

Status ProducerConsumerWorkerApi::ReleaseBigElementMemory(const std::string &streamName, const std::string &producerId,
                                                          const ShmView &pageView)
{
    ReleaseLobPageReqPb req;
    req.set_stream_name(streamName);
    req.set_producer_id(producerId);
    req.set_client_id(workerApi_->clientId_);

    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    ShmViewPb pb;
    pb.set_fd(pageView.fd);
    pb.set_mmap_size(pageView.mmapSz);
    pb.set_offset(pageView.off);
    pb.set_size(pageView.sz);
    req.mutable_page_view()->CopyFrom(pb);
    RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
    ReleaseLobPageRspPb rsp;
    INJECT_POINT("ProducerConsumerWorkerApi.ReleaseBigElementMemory.preReleaseBigShmMemory");
    // Fixme: In this scenario, we don't even care about the timeout, but we still need to set this thread_local
    // variable.
    reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(RpcOptions().GetTimeout()));
    RETURN_IF_NOT_OK(workerApi_->rpcSession_->ReleaseBigShmMemory(req, rsp));
    LOG(INFO) << FormatString("[%s, S:%s, P:%s] Client release big element page success. ShmView %s",
                              workerApi_->LogPrefix(), streamName, producerId, pageView.ToStr());
    return Status::OK();
}

Status ProducerConsumerWorkerApi::CreateWritePage(const std::string &streamName, const std::string &producerId,
                                                  int64_t timeoutMs, const ShmView &curView, ShmView &outPage)
{
    CreateShmPageReqPb req;
    CreateShmPageRspPb rsp;
    req.set_stream_name(streamName);
    req.set_producer_id(producerId);
    req.set_client_id(workerApi_->clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    ShmViewPb pb;
    pb.set_fd(curView.fd);
    pb.set_mmap_size(curView.mmapSz);
    pb.set_offset(curView.off);
    pb.set_size(curView.sz);
    req.mutable_cur_view()->CopyFrom(pb);

    LOG(INFO) << "Client creating write page. Stream: " << streamName << " producer: " << producerId;
    INJECT_POINT_NO_RETURN("ProducerConsumerWorkerApi.CreateWritePage.adjustRpcTimeoutMs",
                           [this](int timeoutMs) { workerApi_->rpcTimeoutMs_ = timeoutMs; });
    PerfPoint point(PerfKey::RPC_WORKER_CREATE_WRITE_PAGE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            workerApi_->rpcTimeoutMs_,
            [this, &req, &rsp, timeoutMs](int32_t currDefaultRpcTimeout) {
                auto pair = workerApi_->GetRpcTimeout(timeoutMs, currDefaultRpcTimeout);
                RpcOptions opts;
                opts.SetTimeout(pair.first);
                reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(opts.GetTimeout()));
                req.set_sub_timeout(pair.second);
                // Even without AKSK authentication, this field should still be set in this scenario because worker
                // relies on this field to determine the order of requests.
                req.set_timestamp(GetSystemClockTimeStampUs());
                RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
                return workerApi_->rpcSession_->CreateShmPage(opts, req, rsp);
            },
            []() { return Status::OK(); }, retryCode_),
        FormatString("[%s, S:%s, C:%s] CreateShmPage request failed", workerApi_->LogPrefix(), streamName, producerId));
    point.Record();
    outPage.off = static_cast<ptrdiff_t>(rsp.last_page_view().offset());
    outPage.sz = rsp.last_page_view().size();
    outPage.mmapSz = rsp.last_page_view().mmap_size();
    outPage.fd = rsp.last_page_view().fd();
    LOG(INFO) << FormatString("[%s, S:%s, P:%s] Client created write page success. ShmView %s",
        workerApi_->LogPrefix(), streamName, producerId, outPage.ToStr());
    return Status::OK();
}

Status ProducerConsumerWorkerApi::CloseProducer(const std::string &streamName, const std::string &producerId)
{
    CloseProducerReqPb req;
    req.set_stream_name(streamName);
    req.set_producer_id(producerId);
    req.set_client_id(workerApi_->clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
    CloseProducerRspPb rsp;

    RpcOptions opts;
    opts.SetTimeout(workerApi_->requestTimeoutMs_);
    reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(workerApi_->requestTimeoutMs_));
    PerfPoint point(PerfKey::RPC_WORKER_CLOSE_PRODUCER);
    RETURN_IF_NOT_OK_EXCEPT(workerApi_->rpcSession_->CloseProducer(opts, req, rsp),
                            StatusCode::K_SC_PRODUCER_NOT_FOUND);
    point.Record();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, P:%s] Close producer success", workerApi_->LogPrefix(),
                                              streamName, producerId);
    return Status::OK();
}

Status ProducerConsumerWorkerApi::CloseConsumer(const std::string &streamName, const std::string &subscriptionName,
                                                const std::string &consumerId)
{
    CloseConsumerReqPb req;
    req.set_stream_name(streamName);
    req.set_subscription_name(subscriptionName);
    req.set_consumer_id(consumerId);
    req.set_client_id(workerApi_->clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
    CloseConsumerRspPb rsp;

    RpcOptions opts;
    opts.SetTimeout(workerApi_->requestTimeoutMs_);
    reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(workerApi_->requestTimeoutMs_));
    PerfPoint point(PerfKey::RPC_WORKER_CLOSE_CONSUMER);
    RETURN_IF_NOT_OK_EXCEPT(workerApi_->rpcSession_->CloseConsumer(opts, req, rsp),
                            StatusCode::K_SC_CONSUMER_NOT_FOUND);
    point.Record();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, P:%s] Close consumer success", workerApi_->LogPrefix(),
                                              streamName, consumerId);
    return Status::OK();
}

Status ProducerConsumerWorkerApi::GetLastAppendCursor(const std::string &streamName, uint64_t &lastAppendCursor)
{
    LastAppendCursorReqPb req;
    req.set_stream_name(streamName);
    req.set_client_id(workerApi_->clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(workerApi_->signature_->GenerateSignature(req));
    LastAppendCursorRspPb rsp;

    RpcOptions opts;
    opts.SetTimeout(workerApi_->rpcTimeoutMs_);
    reqTimeoutDuration.Init(workerApi_->ClientGetRequestTimeout(workerApi_->rpcTimeoutMs_));
    RETURN_IF_NOT_OK(workerApi_->rpcSession_->GetLastAppendCursor(opts, req, rsp));
    lastAppendCursor = rsp.last_append_cursor();
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
