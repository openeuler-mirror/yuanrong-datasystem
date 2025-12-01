/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

#include "datasystem/worker/stream_cache/worker_worker_sc_service_impl.h"

#include <functional>
#include <utility>

#include "datasystem/worker/stream_cache/stream_manager.h"
#include "datasystem/worker/stream_cache/usage_monitor.h"

DS_DECLARE_int32(sc_regular_socket_num);
DS_DECLARE_uint64(sc_local_cache_memory_size_mb);

namespace datasystem {
namespace worker {
namespace stream_cache {

std::string RecvElementView::StreamName() const
{
    return streamName_;
}

std::string RecvElementView::ProducerName() const
{
    // All local and remote producers write to the same page.
    // However, we still need to distinguish elements from different worker
    // because they can have the same seqNo
    return workerAddr_;
}

std::string RecvElementView::ProducerInstanceId() const
{
    // Sequence number will get reset if producer worker restarts
    return workerInstanceId_;
}

uint64_t RecvElementView::StreamHash() const
{
    // We will swap the position of stream and worker address so to hash differently
    StreamProducerKey key(ProducerName(), KeyName(), ProducerInstanceId());
    return std::hash<StreamProducerKey>{}(key);
}

Status RecvElementView::ReleasePage()
{
    return Status::OK();
}

void *RecvElementView::GetBufferPointer()
{
    void *ptr = decrypted_.load() ? localBuf_.get() : recvBuffer_.Data();
    return ptr;
}

WorkerWorkerSCServiceImpl::WorkerWorkerSCServiceImpl(ClientWorkerSCServiceImpl *impl,
                                                     std::shared_ptr<AkSkManager> akSkManager)
    : akSkManager_(std::move(akSkManager)),
      clientWorkerScService_(impl),
      usageMonitor_(impl, FLAGS_sc_local_cache_memory_size_mb * 1024 * 1024)
{
}

WorkerWorkerSCServiceImpl::~WorkerWorkerSCServiceImpl()
{
    if (dataMap_) {
        dataMap_->Stop();
    }
}

Status WorkerWorkerSCServiceImpl::Init()
{
    dataMap_ = std::make_unique<BufferPool>(FLAGS_sc_regular_socket_num, "ScCopyToShm",
                                            std::bind(&WorkerWorkerSCServiceImpl::BatchAsyncFlushEntry, this,
                                                      std::placeholders::_1, std::placeholders::_2));
    RETURN_IF_NOT_OK(dataMap_->Init());
    clientWorkerScService_->SetWorkerWorkerSCServiceImpl(weak_from_this());
    return Status::OK();
}

UsageMonitor &WorkerWorkerSCServiceImpl::GetUsageMonitor()
{
    return usageMonitor_;
}

Status WorkerWorkerSCServiceImpl::ProcessEndOfStream(const std::shared_ptr<StreamManager> &streamMgr,
                                                     std::list<BaseData> dataLst, const std::string &streamName,
                                                     const std::string &workerAddr)
{
    (void)streamMgr;
    // Clean up the usage in the UsageMonitor
    for (auto &ele : dataLst) {
        auto data = std::static_pointer_cast<RecvElementView>(ele.first);
        auto sz = data->recvBuffer_.Size();
        usageMonitor_.DecUsage(streamName, workerAddr, sz);
    }
    // Discard all the buffers.
    dataLst.clear();
    // Signal this job is done.
    return Status::OK();
}

Status WorkerWorkerSCServiceImpl::CheckStreamState(const std::string &streamName,
                                                   StreamManagerMap::const_accessor &accessor,
                                                   std::shared_ptr<StreamManager> &mgr)
{
    Status rc = clientWorkerScService_->GetStreamManager(streamName, accessor);
    if (rc.IsOk()) {
        mgr = accessor->second;
        // Check its state (delete, reset)
        return mgr->CheckIfStreamActive();
    } else if (rc.GetCode() == K_SC_STREAM_NOT_FOUND) {
        return Status::OK();
    }
    return rc;
}

Status WorkerWorkerSCServiceImpl::ProcessRecvElementView(std::shared_ptr<BaseBufferData> &baseBufferData,
                                                         const std::string &streamName,
                                                         const std::string &sendWorkerAddr, bool &isBlocked)
{
    auto data = std::static_pointer_cast<RecvElementView>(baseBufferData);
    auto workerAddr = data->workerAddr_;
    auto bufSz = data->recvBuffer_.Size();
    auto seqNo = data->seqNo_;
    auto count = data->sz_.size();
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(data->traceId_);
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
        "[S:%s, W:%s] Processing RecvElementView. Seq %zu, Count %zu, "
        "Size %zu",
        streamName, workerAddr, seqNo, count, bufSz);
    // Get stream manager. If it is gone, purge the buffers.
    StreamManagerMap::const_accessor accessor;
    std::shared_ptr<StreamManager> streamMgr;
    auto rc = CheckStreamState(data->StreamName(), accessor, streamMgr);
    if (streamMgr) {
        if (streamMgr->IsProducerBlocked(sendWorkerAddr)) {
            isBlocked = true;
            return Status::OK();
        }
        rc = streamMgr->CopyElementView(data, usageMonitor_, RPC_POLL_TIME);
    }
    return rc;
}

Status WorkerWorkerSCServiceImpl::BatchAsyncFlushEntry(int myId, const PendingFlushList &pendingFlushList)
{
    (void)myId;
    size_t numProducers = pendingFlushList.size();
    std::vector<Status> rc(numProducers);
    for (size_t i = 0; i < numProducers; ++i) {
        const StreamProducerKey &key = pendingFlushList.at(i).first;
        std::list<BaseData> &dataList = pendingFlushList.at(i).second;
        const std::string &streamName = key.firstKey_;
        const std::string &sendWorkerAddr = key.producerId_;
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s] Processing RecvElementsViews. Number of elements %zu",
                                                    streamName, dataList.size());
        auto it = dataList.begin();
        while (it != dataList.end()) {
            bool isBlocked = false;
            rc[i] = ProcessRecvElementView(it->first, streamName, sendWorkerAddr, isBlocked);
            if (rc[i].IsError() || isBlocked) {
                break;
            }
            it = dataList.erase(it);
        }
    }
    return ReturnFirstErrorStatus(rc);
}

Status WorkerWorkerSCServiceImpl::ReturnFirstErrorStatus(const std::vector<Status> &rc)
{
    // Return the first non-ok error
    for (const auto &status : rc) {
        if (status.IsError()) {
            return status;
        }
    }
    return Status::OK();
}

void WorkerWorkerSCServiceImpl::RemoveStream(const std::string &keyName, const std::string &sharedPageName)
{
    dataMap_->RemoveStream(keyName, sharedPageName);
}

void WorkerWorkerSCServiceImpl::PurgeBuffer(const std::shared_ptr<StreamManager> &streamManager)

{
    dataMap_->PurgeBuffer(streamManager->GetStreamName(),
                          std::bind(&WorkerWorkerSCServiceImpl::ProcessEndOfStream, this, streamManager,
                                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
}

Status WorkerWorkerSCServiceImpl::ParsePushData(const PushReqPb &pushReqPb, std::vector<RpcMessage> &payloads,
                                                std::shared_ptr<StreamManager> &streamManager,
                                                const std::string &workerAddr, PushRspPb &pushRspPb,
                                                std::vector<std::shared_ptr<BaseBufferData>> &flushList)
{
    const std::string streamName = streamManager->GetStreamName();
    auto streamAllocRatio = streamManager->GetStreamMemAllocRatio();
    auto streamPageSize = streamManager->GetStreamPageSize();
    int numPageViews = pushReqPb.seq_size();
    const std::string &producerId = pushReqPb.producer_id();
    const std::string &workerInstanceId = pushReqPb.worker_instance_id();
    Status allocRc;
    uint64_t totalSize = 0;
    for (int i = 0; i < numPageViews; ++i) {
        // Step 1: Check for OOM
        // Check if local memory is over used. If OOM, no need to continue
        // We can't use the payload size because it can be encrypted and different from the real size.
        size_t totalLength = 0;
        std::vector<size_t> sz;
        // Elements are packed in reverse order.
        auto &eleMeta = pushReqPb.element_meta(i);
        for (auto k = 0; k < eleMeta.element_sizes_size(); ++k) {
            auto eleSz = eleMeta.element_sizes(k);
            sz.emplace_back(eleSz);
            totalLength += eleSz;
        }
        allocRc = usageMonitor_.CheckNIncOverUsedForStream(streamName, workerAddr, streamPageSize, streamAllocRatio,
                                                           totalLength);
        if (allocRc.IsError()) {
            for (int k = i; k < numPageViews; ++k) {
                auto *rsp = pushRspPb.mutable_error(k);
                rsp->Clear();
                rsp->set_error_code(allocRc.GetCode());
                rsp->set_error_msg(allocRc.GetMsg());
                VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
                    "[RW:%s, S:%s, I:%s, seq:%zu, count: %d, sz:%zu] "
                    "Not enough memory to satisfy the request",
                    workerAddr, streamName, workerInstanceId, pushReqPb.seq(k), eleMeta.element_sizes_size(),
                    payloads[k].Size());
            }
            // No need to continue;
            break;
        }
        // Step 2: Parse meta data.
        // Will be freed by processing thread (threadpool_)
        uint64_t seqNo = pushReqPb.seq(i);
        auto recvElementView = std::make_shared<RecvElementView>();
        // Step 2: Parse payloads.
        recvElementView->streamName_ = streamName;
        recvElementView->workerAddr_ = workerAddr;
        recvElementView->recvBuffer_ = std::move(payloads[i]);
        recvElementView->traceId_ = Trace::Instance().GetTraceID();
        recvElementView->workerInstanceId_ = workerInstanceId;
        recvElementView->seqNo_ = seqNo;
        recvElementView->totalLength_ = totalLength;
        recvElementView->sz_ = sz;
        for (auto k = 0; k < eleMeta.header_bits_size(); ++k) {
            recvElementView->headerBits_.emplace_back(eleMeta.header_bits(k));
        }
        auto status = dataMap_->UnsortedInsert(recvElementView, pushReqPb.seq(i), pushReqPb.first_seq());
        // Rollback reserved memory for duplicate element
        if (status.GetCode() == K_DUPLICATED) {
            LOG_IF_ERROR(usageMonitor_.DecUsage(streamName, workerAddr, totalLength),
                         FormatString("%s:%s", __FUNCTION__, __LINE__));
            continue;
        }
        auto bufSz = recvElementView->recvBuffer_.Size();
        totalSize += bufSz;
        // Accept and add the pageviews for processing
        flushList.emplace_back(std::move(recvElementView));
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
            "Finished req, stream:[%s], worker:[%s], producer:[%s], Instance:[%s], seq:[%zu], count:[%d], size:[%zu], "
            "total size:[%zu]",
            streamName, workerAddr, producerId, workerInstanceId, seqNo, eleMeta.element_sizes_size(), bufSz,
            totalSize);
    }
    return Status::OK();
}

Status WorkerWorkerSCServiceImpl::PushElementsCursors(
    std::shared_ptr<ServerUnaryWriterReader<PushRspPb, PushReqPb>> serverApi)
{
    INJECT_POINT("PushElementsCursors.begin");
    PerfPoint point(PerfKey::PUSH_CURSOR_ALL);
    PushReqPb pushReqPb;
    PushRspPb pushRspPb;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(serverApi->Read(pushReqPb));
    RETURN_IF_NOT_OK(serverApi->ReceivePayload(payloads));
    VLOG(1) << FormatString("Preparing to receive pushed data. streamName: %s", pushReqPb.stream_name());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(pushReqPb), "AK/SK failed");
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReqPb.trace_id());
    RETURN_IF_NOT_OK(PushElementsCursorsHelper(pushReqPb, payloads, pushRspPb));
    point.RecordAndReset(PerfKey::PUSH_CURSOR_RESPONSE);
    VLOG(SC_DEBUG_LOG_LEVEL) << "worker PushElementsCursors done";
    // We reply to the client at this point.
    RETURN_IF_NOT_OK(serverApi->Write(pushRspPb));
    return Status::OK();
}

Status WorkerWorkerSCServiceImpl::PushElementsCursorsHelper(PushReqPb &pushReqPb, std::vector<RpcMessage> &payloads,
                                                            PushRspPb &pushRspPb)
{
    const auto &streamName = pushReqPb.stream_name();
    const auto &workerAddr = pushReqPb.worker_addr();
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(clientWorkerScService_->GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    auto streamPageSize = streamManager->GetStreamPageSize();
    CHECK_FAIL_RETURN_STATUS(streamPageSize > 0, K_TRY_AGAIN, "Uninitialized page size, try again later.");
    RETURN_IF_NOT_OK(streamManager->CheckConsumerExist(workerAddr));
    // Set up response. We may change the error code later.
    for (int i = 0; i < pushReqPb.seq_size(); ++i) {
        ErrorInfoPb rsp;
        rsp.set_error_code(StatusCode::K_OK);
        pushRspPb.mutable_error()->Add(std::move(rsp));
    }
    // Reject any new data if stream is already in reset mode
    // Send OK to remote producer so that it does not try to resend old data. But we have just
    // set up the response for each PV
    if (streamManager->CheckIfStreamActive().IsError()) {
        return Status::OK();
    }
    std::vector<std::shared_ptr<BaseBufferData>> flushList;
    RETURN_IF_NOT_OK(ParsePushData(pushReqPb, payloads, streamManager, workerAddr, pushRspPb, flushList));
    return Status::OK();
}

Status WorkerWorkerSCServiceImpl::PushSharedPageCursors(
    std::shared_ptr<ServerUnaryWriterReader<PushRspPb, SharedPagePushReqPb>> serverApi)
{
    INJECT_POINT("PushElementsCursors.begin");
    PerfPoint point(PerfKey::PUSH_CURSOR_ALL);
    VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("Preparing to receive pushed shared page data.");
    SharedPagePushReqPb pushReqPb;
    PushRspPb pushRspPb;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(serverApi->Read(pushReqPb));
    RETURN_IF_NOT_OK(serverApi->ReceivePayload(payloads));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(pushReqPb), "AK/SK failed");
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReqPb.trace_id());
    // Group requests by stream name, and also record the indexes so the response is still in order.
    std::unordered_map<std::string, PushSharedPageTuple> streamDataMap;
    std::vector<ErrorInfoPb> errorList(pushReqPb.metas_size());
    for (int i = 0; i < pushReqPb.metas_size(); ++i) {
        const std::string &streamName = pushReqPb.stream_names(pushReqPb.metas(i).stream_index());
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("PushSharedPageCursors req involves stream %s at iteration %d",
                                                    streamName, i);
        auto iter = streamDataMap.find(streamName);
        if (iter == streamDataMap.end()) {
            PushReqPb req;
            req.set_stream_name(streamName);
            req.set_worker_addr(pushReqPb.worker_addr());
            req.set_producer_id(pushReqPb.producer_id());
            // Force the first sequence number to be 1,
            // so that UnsortedInsert only waits for the expected sequence number.
            req.set_first_seq(1);
            req.set_worker_instance_id(pushReqPb.worker_instance_id());
            iter = streamDataMap.emplace(streamName, PushSharedPageTuple()).first;
            iter->second.req_ = std::move(req);
        }
        iter->second.index_.emplace_back(i);
        auto &req = iter->second.req_;
        *req.mutable_element_meta()->Add() = pushReqPb.metas(i).element_meta();
        *req.mutable_seq()->Add() = pushReqPb.metas(i).seq();
        iter->second.payload_.emplace_back(std::move(payloads[i]));
    }
    for (auto &tuple : streamDataMap) {
        PushReqPb &req = tuple.second.req_;
        std::vector<RpcMessage> &payload = tuple.second.payload_;
        PushRspPb rsp;
        auto rc = PushElementsCursorsHelper(req, payload, rsp);
        ErrorInfoPb err;
        if (rc.IsError()) {
            err.set_error_code(rc.GetCode());
            err.set_error_msg(rc.GetMsg());
        } else {
            uint64_t expectedSize = tuple.second.index_.size();
            uint64_t actualSize = static_cast<uint64_t>(rsp.error_size());
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                actualSize == expectedSize, K_RUNTIME_ERROR,
                FormatString("Unexpected number of error info, expected %zu, actual %zu", expectedSize, actualSize));
        }
        uint64_t rspIndex = 0;
        for (const auto &index : tuple.second.index_) {
            errorList[index] = rc.IsError() ? err : rsp.error(rspIndex++);
        }
    }
    *pushRspPb.mutable_error() = { errorList.begin(), errorList.end() };
    point.RecordAndReset(PerfKey::PUSH_CURSOR_RESPONSE);
    VLOG(SC_DEBUG_LOG_LEVEL) << "worker PushSharedPageCursors done";
    // We reply to the client at this point.
    RETURN_IF_NOT_OK(serverApi->Write(pushRspPb));
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
