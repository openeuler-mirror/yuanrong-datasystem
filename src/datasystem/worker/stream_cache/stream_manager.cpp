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

#include "datasystem/worker/stream_cache/stream_manager.h"

#include <utility>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/stream_posix.stub.rpc.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"
#include "datasystem/worker/stream_cache/page_queue/page_queue_handler.h"

DS_DECLARE_string(sc_encrypt_secret_key);

namespace datasystem {
namespace worker {
namespace stream_cache {
template Status StreamManager::HandleBlockedRequestImpl(
    std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> &&blockedReq, bool lock);
template Status StreamManager::HandleBlockedRequestImpl(
    std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> &&blockedReq, bool lock);

StreamManager::StreamManager(std::string streamName, RemoteWorkerManager *remoteWorkerManager,
                             std::string localWorkerAddr, std::shared_ptr<AkSkManager> akSkManager,
                             std::weak_ptr<ClientWorkerSCServiceImpl> scSvc,
                             std::shared_ptr<WorkerSCAllocateMemory> manager,
                             std::weak_ptr<WorkerWorkerSCServiceImpl> workerWorkerSCService, uint64_t localStreamNum)
    : workerAddr_(std::move(localWorkerAddr)),
      streamName_(std::move(streamName)),
      remoteWorkerManager_(remoteWorkerManager),
      akSkManager_(std::move(akSkManager)),
      scSvc_(std::move(scSvc)),
      lastAckCursor_(0),
      wakeupPendingRecvOnProdFault_(false),
      scAllocateManager_(std::move(manager)),
      workerWorkerSCService_(std::move(workerWorkerSCService)),
      localStreamNum_(localStreamNum)
{
    ackWp_.Set();
    reclaimWp_.Set();
}

StreamManager::~StreamManager()
{
    // Update stream metrics final time before exit
    if (scStreamMetrics_) {
        UpdateStreamMetrics();
    }
    // Remove stream number from the map at stream manager deletion.
    auto scSvc = scSvc_.lock();
    if (scSvc != nullptr) {
        scSvc->RemoveStreamNo(localStreamNum_);
        // Explicitly undo the memory reservation at stream deletion.
        scSvc->UndoReserveMemoryFromUsageMonitor(GetStreamName());
    }
    // remove stream info in BufferPool
    if (auto workerWorkerSCServicePtr = workerWorkerSCService_.lock()) {
        workerWorkerSCServicePtr->RemoveStream(GetStreamName(), pageQueueHandler_->GetSharedPageQueueId());
    }
    remoteWorkerManager_->RemoveStream(GetStreamName(), pageQueueHandler_->GetSharedPageQueueId());
}

Status StreamManager::CreatePageQueueHandler(Optional<StreamFields> cfg)
{
    pageQueueHandler_ = std::make_unique<PageQueueHandler>(this, cfg);
    // Reserve memory if not enable shared page.
    // if enable shared page
    // 1. The produer node will not reserve memory.
    // 2. The consumer node will reserve memory after StremFields update in UpdateStreamFields.
    if (cfg && !EnableSharedPage(cfg->streamMode_)) {
        auto pageQueue = GetExclusivePageQueue();
        Status rc = pageQueue->ReserveStreamMemory();
        if (rc.IsOk()) {
            LOG(INFO) << FormatString("[%s] %zu bytes of shared memory has been reserved", LogPrefix(),
                                      pageQueue->GetReserveSize());
        }
        // Page size unknown at this point is not an error.
        if (rc.GetCode() == K_NOT_READY) {
            rc = Status::OK();
        }
        return rc;
    }
    return Status::OK();
}

void StreamManager::BlockMemoryReclaim()
{
    reclaimMutex_.lock_shared();  // To be unlocked by the caller, not by this function.
    reclaimWp_.Wait();
}

void StreamManager::UnblockMemoryReclaim()
{
    reclaimMutex_.unlock();
}

Status StreamManager::AddCursorForProducer(const std::string &producerId, ShmView &shmView)
{
    std::shared_ptr<Cursor> cursor;
    RETURN_IF_NOT_OK(pageQueueHandler_->AddCursor(producerId, true, cursor, shmView));
    bool needRollback = true;
    Raii raii([this, &needRollback, &producerId]() {
        if (needRollback) {
            pageQueueHandler_->DeleteCursor(producerId);
        }
    });
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto iter = pubs_.find(producerId);
    CHECK_FAIL_RETURN_STATUS(iter != pubs_.end(), K_RUNTIME_ERROR,
                             FormatString("can not find producer[%s] when add cursor", producerId));
    needRollback = false;
    iter->second->SetCursor(std::move(cursor));
    iter->second->SetElementCount(0);
    return Status::OK();
}

Status StreamManager::AddProducer(const std::string &producerId,
                                  DataVerificationHeader::SenderProducerNo &senderProducerNo)
{
    PerfPoint point(PerfKey::MANAGER_ADD_PRODUCER);
    // Allocate a work area (in shared memory) to be shared between this worker and the client producer

    bool needRollback = true;
    Raii raii([this, &needRollback, &producerId]() {
        if (needRollback) {
            pubs_.erase(producerId);
        }
    });
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto ret = pubs_.emplace(producerId, std::make_shared<Producer>(producerId, GetStreamName(), nullptr));
    CHECK_FAIL_RETURN_STATUS(ret.second, StatusCode::K_DUPLICATED,
                             FormatString("Failed to add new producer <%s> into streamManager", producerId));

    // Assign the new producer with a locally unique number for data verification.
    senderProducerNo = ++lifetimeLocalProducerCount_;
    needRollback = false;
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumLocalProducers, pubs_.size());
    }
    return Status::OK();
}

void StreamManager::ForceUnlockByCursor(const std::string &cursorId, bool isProducer, uint32_t lockId)
{
    if (pageQueueHandler_) {
        pageQueueHandler_->ForceUnlockByCursor(cursorId, isProducer, lockId);
    }
}

void StreamManager::ForceUnlockMemViemForPages(uint32_t lockId)
{
    if (pageQueueHandler_) {
        pageQueueHandler_->ForceUnlockMemViemForPages(lockId);
    }
}

Status StreamManager::CloseProducer(const std::string &producerId, bool forceClose)
{
    INJECT_POINT("StreamManager.CloseProducer.timing");
    std::shared_ptr<Producer> producerPtr;
    bool isLastProducer = false;
    {
        WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        auto producer = pubs_.find(producerId);
        CHECK_FAIL_RETURN_STATUS(producer != pubs_.end(), StatusCode::K_SC_PRODUCER_NOT_FOUND,
                                 FormatString("Stream:<%s>, Producer:<%s> does not exist", streamName_, producerId));
        auto elementCount = producer->second->GetElementCountAndReset();
        LOG(INFO) << FormatString("[%s] Stream manager close producer: %s, element sent: %zu", LogPrefix(), producerId,
                                  elementCount);
        if (scStreamMetrics_) {
            scStreamMetrics_->IncrementMetric(StreamMetric::NumTotalElementsSent, elementCount);
            scStreamMetrics_->IncrementMetric(StreamMetric::NumSendRequests,
                                              producer->second->GetRequestCountAndReset());
        }
        pubs_.erase(producer);
        RETURN_IF_NOT_OK_EXCEPT(pageQueueHandler_->DeleteCursor(producerId), K_NOT_FOUND);
        // Process local ClearAllRemoteConsumer when it is the last producer on the worker for the stream.
        // This is to replace the ClearAllRemoteConsumer RPC.
        isLastProducer = pubs_.empty();
        if (isLastProducer) {
            RETURN_IF_NOT_OK_EXCEPT(remoteWorkerManager_->ClearAllRemoteConsumer(streamName_, forceClose),
                                    K_SC_STREAM_NOT_FOUND);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ClearAllRemoteConsumerUnlocked(forceClose),
                                             "streamManager ClearAllRemoteConsumer failed");
            LOG(INFO) << "worker ClearAllRemoteConsumer done, streamname: " << streamName_;
        }
    }
    if (pageQueueHandler_ && isLastProducer) {
        // At the local ClearAllRemoteConsumer handling, we no longer flush through FlushAllChanges,
        // but now still log the last append cursor at the last CloseProducer for diagnostic purposes.
        RETURN_IF_NOT_OK(pageQueueHandler_->MoveUpLastPage());
        uint64_t lastAppendCursor = GetExclusivePageQueue()->GetLastAppendCursor();
        LOG(INFO) << FormatString("[S:%s] Last append cursor at %zu when producer %s close", streamName_,
                                  lastAppendCursor, producerId);
        if (!IsRetainData()) {
            RETURN_IF_NOT_OK(EarlyReclaim());
        }
    }
    if (CheckIfStreamInState(StreamState::RESET_IN_PROGRESS)) {
        std::unique_lock<std::shared_timed_mutex> lock(resetMutex_);
        std::vector<std::string> prodList(1, producerId);
        (void)RemovePubSubFromResetList(prodList);
    }
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumLocalProducers, pubs_.size());
    }
    return Status::OK();
}

Status StreamManager::AddConsumer(const SubscriptionConfig &config, const std::string &consumerId,
                                  uint64_t &lastAckCursor, ShmView &waView)
{
    Raii resumeAck([this]() { ResumeAckThread(); });
    // We are adding a local consumer and on return we will set up a cursor to begin with.
    // We also need to ensure the garbage collector thread is not purging the required page
    // from memory.
    std::shared_ptr<Cursor> cursor;
    bool needRollback = true;
    // Force update last page for last append cursor.
    RETURN_IF_NOT_OK(pageQueueHandler_->MoveUpLastPage());
    RETURN_IF_NOT_OK(pageQueueHandler_->AddCursor(consumerId, false, cursor, waView));
    Raii raii([this, &needRollback, &consumerId]() {
        if (needRollback) {
            pageQueueHandler_->DeleteCursor(consumerId);
        }
    });
    // Trigger AckCursors to update last ack cursor.
    RETURN_IF_NOT_OK(AckCursors());
    PauseAckThread();
    PerfPoint point(PerfKey::MANAGER_ADD_CONSUMER);
    // Should the subscriber wakeup pending Receive() on getting notification about a Pub node crash/force close.
    // Implicitly create subscription for the target stream.
    RETURN_IF_NOT_OK(CreateSubscriptionIfMiss(config, lastAckCursor));
    // By now we can obtain the target subscription definitely.
    std::shared_ptr<Subscription> subscription;
    RETURN_IF_NOT_OK(GetSubscription(config.subscriptionName, subscription));
    RETURN_IF_NOT_OK(subscription->AddConsumer(config, consumerId, lastAckCursor, cursor));
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumLocalConsumers, subs_.size());
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, C:%s] AddConsumer success, lastAckCursor:%llu", LogPrefix(),
                                              consumerId, lastAckCursor);
    needRollback = false;
    return Status::OK();
}

Status StreamManager::CloseConsumer(const std::string &subName, const std::string &consumerId)
{
    std::shared_ptr<Subscription> subPtr;
    RETURN_IF_NOT_OK(GetSubscription(subName, subPtr));
    CHECK_FAIL_RETURN_STATUS(subPtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                             "Failed to get stream by name: " + subName);
    PerfPoint point(PerfKey::MANAGER_CLOSE_CONSUMER);
    if (scStreamMetrics_) {
        scStreamMetrics_->IncrementMetric(StreamMetric::NumReceiveRequests, subPtr->GetRequestCountAndReset());
    }
    RETURN_IF_NOT_OK(subPtr->RemoveConsumer(consumerId));
    RETURN_IF_NOT_OK_EXCEPT(pageQueueHandler_->DeleteCursor(consumerId), K_NOT_FOUND);
    bool isLastConsumer = false;
    if (!subPtr->HasConsumer()) {  // If this subscription has no consumer, we delete it from subs_ hash map.
        {
            WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, Sub:%s] Delete sub due to no consumer inside it",
                                                        LogPrefix(), subName);
            CHECK_FAIL_RETURN_STATUS(
                subs_.erase(subName) == 1, StatusCode::K_SC_CONSUMER_NOT_FOUND,
                FormatString("Consumer <%s> does not exist in Subscription <%s>", consumerId, subName));
            isLastConsumer = subs_.empty();
            if (isLastConsumer) {
                // Early reclaim of local cache memory reservation when consumers are all closed.
                auto scSvc = scSvc_.lock();
                if (scSvc != nullptr) {
                    scSvc->UndoReserveMemoryFromUsageMonitor(GetStreamName());
                }
                // If this stream has no more consumers clear all remote pubs.
                ClearAllRemotePubUnlocked();
            }
        }
        if (CheckIfStreamInState(StreamState::RESET_IN_PROGRESS)) {
            std::unique_lock<std::shared_timed_mutex> lock(resetMutex_);
            std::vector<std::string> conList(1, consumerId);
            (void)RemovePubSubFromResetList(conList);
        }
    }
    point.Record();
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumLocalConsumers, subs_.size());
    }
    if (isLastConsumer && !IsRetainData()) {
        RETURN_IF_NOT_OK(EarlyReclaim());
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, Sub:%s, C:%s] CloseConsumer success.", LogPrefix(), subName,
                                                consumerId);
    return Status::OK();
}

Status StreamManager::CheckDeleteStreamCondition()
{
    PerfPoint point(PerfKey::MANAGER_DELETE_STREAM);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    if (pubs_.empty() && subs_.empty() && remotePubWorkerDict_.empty() && remoteSubWorkerDict_.empty()) {
        return Status::OK();
    }
    if (!pubs_.empty() || !subs_.empty()) {
        LOG(ERROR) << "Not allowed to delete stream, pub count:" << pubs_.size() << ", sub count:" << subs_.size();
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Not allowed to delete stream when producer/consumer is running.");
    }
    if (!remotePubWorkerDict_.empty()) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Not allowed to delete stream when remote producer is running.\nList: [%s]",
                                   VectorToString(remotePubWorkerDict_)));
    }
    if (!remoteSubWorkerDict_.empty()) {
        std::stringstream ss;
        for (const auto &entry : remoteSubWorkerDict_) {
            ss << entry.first << " ";
        }
        RETURN_STATUS(
            StatusCode::K_RUNTIME_ERROR,
            FormatString(
                "Not allowed to delete stream when remote consumer is running\nList: [%s]\n Possibility:\n1. Remote "
                "Consumer not closed yet\n2. Sending data on local node to remote consumer.",
                ss.str()));
    }
    point.Record();
    return Status::OK();
}

Status StreamManager::AllocDataPage(BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb> *blockedReq)
{
    auto req = blockedReq->GetCreateRequest();
    const auto &producerId = req.producer_id();
    ShmView curView = { .fd = req.cur_view().fd(),
                        .mmapSz = req.cur_view().mmap_size(),
                        .off = static_cast<ptrdiff_t>(req.cur_view().offset()),
                        .sz = req.cur_view().size() };
    std::shared_ptr<StreamDataPage> lastPage;
    RETURN_IF_NOT_OK(CreateOrGetLastDataPage(producerId, RPC_TIMEOUT, curView, lastPage, false));
    CreateShmPageRspPb &rsp = blockedReq->rsp_;
    ShmView shmView = lastPage->GetShmView();
    ShmViewPb pb;
    pb.set_fd(shmView.fd);
    pb.set_mmap_size(shmView.mmapSz);
    pb.set_offset(shmView.off);
    pb.set_size(shmView.sz);
    rsp.mutable_last_page_view()->CopyFrom(pb);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(blockedReq->Write(), "Write reply to client stream failed");
    const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
    LOG_EVERY_N(INFO, logPerCount) << FormatString(
        "[%s, P:%s] CreateShmPage success. ProdId: %s, PageId: %s. Retry count: %zu", LogPrefix(), producerId,
        req.producer_id(), lastPage->GetPageId(), blockedReq->retryCount_.load());
    return Status::OK();
}

Status StreamManager::AllocDataPageInternalReq(uint64_t timeoutMs, const ShmView &curView, ShmView &outView)
{
    CreateShmPageReqPb req;
    req.set_stream_name(streamName_);
    // We need to fake a producer id as a unique key into MemAllocRequestList
    auto producerId = GetStringUuid();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, P:%s] Send an internal AllocDataPage request for size %zu.",
                                              LogPrefix(), producerId, GetStreamPageSize());
    req.set_producer_id(producerId);
    ShmViewPb pb;
    pb.set_fd(curView.fd);
    pb.set_mmap_size(curView.mmapSz);
    pb.set_offset(curView.off);
    pb.set_size(curView.sz);
    req.mutable_cur_view()->CopyFrom(pb);

    auto fn = std::bind(&StreamManager::AllocDataPage, shared_from_this(), std::placeholders::_1);
    auto blockedReq = std::make_shared<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>>(
        streamName_, req, GetStreamPageSize(), nullptr, fn);
    // Lock to compete with StreamManager::UnblockProducers
    auto scSvc = scSvc_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
    RETURN_IF_NOT_OK(AddBlockedCreateRequest(scSvc.get(), blockedReq, true));
    scSvc->AsyncSendMemReq<CreateShmPageRspPb, CreateShmPageReqPb>(streamName_);
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, P:%s] Wait for internal AllocDataPage reply.", LogPrefix(),
                                                blockedReq->req_.producer_id());
    RETURN_IF_NOT_OK(blockedReq->Wait(timeoutMs));
    CreateShmPageRspPb &rsp = blockedReq->rsp_;
    outView.off = static_cast<ptrdiff_t>(rsp.last_page_view().offset());
    outView.sz = rsp.last_page_view().size();
    outView.mmapSz = rsp.last_page_view().mmap_size();
    outView.fd = rsp.last_page_view().fd();
    return Status::OK();
}

Status StreamManager::CreateOrGetLastDataPage(const std::string &producerId, uint64_t timeoutMs,
                                              const ShmView &lastView, std::shared_ptr<StreamDataPage> &lastPage,
                                              bool retryOnOOM)
{
    PerfPoint point(PerfKey::MANAGER_CREATE_STREAM_PAGE);
    // Create new page or return existing one
    RETURN_IF_NOT_OK(pageQueueHandler_->CreateOrGetLastDataPage(timeoutMs, lastView, lastPage, retryOnOOM));
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, P:%s] LastPage %s", LogPrefix(), producerId,
                                                lastPage->GetPageId());
    // Notify a new page has been created
    TryWakeUpPendingReceive();
    return Status::OK();
}

Status StreamManager::AllocBigShmMemory(BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb> *blockedReq)
{
    auto req = blockedReq->GetCreateRequest();
    const auto &producerId = req.producer_id();
    std::shared_ptr<ShmUnitInfo> pageUnitInfo;
    size_t pageSize = req.page_size();
    Status allocRc = pageQueueHandler_->AllocMemory(pageSize, true, pageUnitInfo, false);
    if (allocRc.GetCode() == K_OUT_OF_MEMORY) {
        LOG_IF_ERROR(pageQueueHandler_->ReclaimAckedChain(blockedReq->req_.sub_timeout()), "Reclaim ack chain error");
        if (!CheckHadEnoughMem(pageSize)) {
            pageQueueHandler_->DumpPoolPages(FLAGS_v);
        }
    }
    RETURN_IF_NOT_OK(allocRc);
    CHECK_FAIL_RETURN_STATUS(pageUnitInfo != nullptr, K_RUNTIME_ERROR, "pageUnitInfo is nullptr");
    LOG(INFO) << FormatString("[%s, P:%s] AllocBigShmMemory success.", LogPrefix(), producerId);
    // From now on make sure we free the memory on error exit
    bool needRollback = true;
    Raii raii([this, &producerId, &needRollback, &pageUnitInfo]() {
        if (needRollback) {
            ShmView v{ .fd = pageUnitInfo->fd,
                       .mmapSz = pageUnitInfo->mmapSize,
                       .off = pageUnitInfo->offset,
                       .sz = pageUnitInfo->size };
            LOG(INFO) << FormatString("[%s, P:%s] Undo previous AllocBigShmMemory", LogPrefix(), producerId);
            (void)pageQueueHandler_->ReleaseMemory(v);
        }
    });
    ShmViewPb pb;
    pb.set_fd(pageUnitInfo->fd);
    pb.set_mmap_size(pageUnitInfo->mmapSize);
    pb.set_offset(pageUnitInfo->offset);
    pb.set_size(pageUnitInfo->size);
    CreateLobPageRspPb &rsp = blockedReq->rsp_;
    rsp.mutable_page_view()->CopyFrom(pb);
    RETURN_IF_NOT_OK(blockedReq->Write());
    // For internal request, we need to coordinate with the caller because we don't chain big element like
    // data page. If the requester has gone (due to timeout), the memory is stale. It is hard to check
    // for rpc requester, but we can check that for internal requester.
    INJECT_POINT("StreamManager.AllocBigShmMemory.NoHandShake1", [&needRollback]() {
        needRollback = false;
        return Status::OK();
    });
    INJECT_POINT("StreamManager.AllocBigShmMemory.NoHandShake2");
    RETURN_IF_NOT_OK(blockedReq->SenderHandShake());
    needRollback = false;
    return Status::OK();
}

Status StreamManager::AllocBigShmMemoryInternalReq(uint64_t timeoutMs, size_t sz, ShmView &outView)
{
    CreateLobPageReqPb req;
    req.set_stream_name(streamName_);
    // We need to fake a producer id as a unique key into MemAllocRequestList
    auto producerId = GetStringUuid();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, P:%s] Send an internal AllocBigShmMemory request for size %zu.",
                                              LogPrefix(), producerId, sz);
    req.set_producer_id(producerId);
    req.set_page_size(sz);
    auto fn = std::bind(&StreamManager::AllocBigShmMemory, shared_from_this(), std::placeholders::_1);
    auto blockedReq = std::make_shared<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>>(streamName_, req,
                                                                                                     sz, nullptr, fn);
    auto scSvc = scSvc_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
    RETURN_IF_NOT_OK(AddBlockedCreateRequest(scSvc.get(), blockedReq, true));
    scSvc->AsyncSendMemReq<CreateLobPageRspPb, CreateLobPageReqPb>(streamName_);
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, P:%s] Wait for internal AllocBigShmMemory reply.", LogPrefix(),
                                                blockedReq->req_.producer_id());
    auto waitTime = [timeoutMs]() {
        INJECT_POINT("StreamManager.AllocBigShmMemoryInternalReq.SetTimeoutMs", [](uint64_t val) { return val; });
        return timeoutMs;
    };
    INJECT_POINT("StreamManager.AllocBigShmMemoryInternalReq.sleep");
    RETURN_IF_NOT_OK(blockedReq->Wait(waitTime()));
    // Handshake with the MemPool thread
    INJECT_POINT("StreamManager.AllocBigShmMemoryInternalReq.NoHandShake");
    RETURN_IF_NOT_OK(blockedReq->ReceiverHandShake());
    CreateLobPageRspPb &rsp = blockedReq->rsp_;
    outView.off = static_cast<ptrdiff_t>(rsp.page_view().offset());
    outView.sz = rsp.page_view().size();
    outView.mmapSz = rsp.page_view().mmap_size();
    outView.fd = rsp.page_view().fd();
    return Status::OK();
}

Status StreamManager::ReleaseBigShmMemory(
    const std::shared_ptr<ServerUnaryWriterReader<ReleaseLobPageRspPb, ReleaseLobPageReqPb>> &serverApi,
    const ReleaseLobPageReqPb &req)
{
    ShmView v;
    v.fd = req.page_view().fd();
    v.mmapSz = req.page_view().mmap_size();
    v.off = static_cast<ptrdiff_t>(req.page_view().offset());
    v.sz = req.page_view().size();
    Status rc = pageQueueHandler_->ReleaseMemory(v);
    if (rc.IsError()) {
        return serverApi->SendStatus(rc);
    }
    ReleaseLobPageRspPb rsp;
    return serverApi->Write(rsp);
}

Status StreamManager::AddBlockedCreateRequest(
    ClientWorkerSCServiceImpl *scSvc,
    std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> blockedReq, bool lock)
{
    // Compete with UnblockCreators
    std::shared_lock<std::shared_timed_mutex> rlock(streamManagerBlockedListsMutex_, std::defer_lock);
    if (lock) {
        INJECT_POINT("StreamManager.AddBlockCreateRequest.sleep");
        rlock.lock();
    }
    return dataBlockedList_.AddBlockedCreateRequest(scSvc, std::move(blockedReq));
}

Status StreamManager::AddBlockedCreateRequest(
    ClientWorkerSCServiceImpl *scSvc,
    std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> blockedReq, bool lock)
{
    // Compete with UnblockCreators
    std::shared_lock<std::shared_timed_mutex> rlock(streamManagerBlockedListsMutex_, std::defer_lock);
    if (lock) {
        rlock.lock();
    }
    return lobBlockedList_.AddBlockedCreateRequest(scSvc, std::move(blockedReq));
}

Status StreamManager::GetBlockedCreateRequest(
    std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> &blockedReq)
{
    return dataBlockedList_.GetBlockedCreateRequest(blockedReq);
}

Status StreamManager::GetBlockedCreateRequest(
    std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> &blockedReq)
{
    return lobBlockedList_.GetBlockedCreateRequest(blockedReq);
}

Status StreamManager::UnblockCreators()
{
    // We want to clear as much as backlog as possible.
    // At the same time, block new requests coming in.

    // We will handle BigElement first.
    if (!lobBlockedList_.Empty()) {
        // Block AddBlockedCreateRequest
        std::unique_lock<std::shared_timed_mutex> xlock(streamManagerBlockedListsMutex_);
        LOG(INFO) << FormatString("[%s] Freed page result in unblocking a waiting AllocBigShmMemory.", LogPrefix());
        std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> blockedReq;
        RETURN_IF_NOT_OK_EXCEPT(lobBlockedList_.GetBlockedCreateRequest(blockedReq), K_TRY_AGAIN);
        if (blockedReq) {
            // To avoid deadlock with itself, don't lock the streamManagerBlockedListsMutex_ again
            RETURN_IF_NOT_OK_EXCEPT(HandleBlockedRequestImpl(std::move(blockedReq), false), K_OUT_OF_MEMORY);
        }
    }
    // Next we handle regular page
    if (!dataBlockedList_.Empty()) {
        // Block AddBlockedCreateRequest
        INJECT_POINT("UnblockCreators.sleep");
        std::unique_lock<std::shared_timed_mutex> xlock(streamManagerBlockedListsMutex_);
        LOG(INFO) << FormatString("[%s] Freed page result in unblocking a waiting CreateShmPage.", LogPrefix());
        // Because producers are sharing pages, we will need to keep popping.
        Status rc;
        while (rc.IsOk()) {
            std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> blockedReq;
            rc = dataBlockedList_.GetBlockedCreateRequest(blockedReq);
            if (rc.IsOk()) {
                // To avoid deadlock with itself, don't lock the streamManagerBlockedListsMutex_ again
                rc = HandleBlockedRequestImpl(std::move(blockedReq), false);
            }
            if (rc.GetCode() == K_OUT_OF_MEMORY) {
                break;
            }
            RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
        }
    }
    return Status::OK();
}

std::pair<size_t, bool> StreamManager::GetNextBlockedRequestSize()
{
    // Block AddBlockedCreateRequest
    std::unique_lock<std::shared_timed_mutex> xlock(streamManagerBlockedListsMutex_);
    // We have two lists, and we will look at the oldest one.
    if (lobBlockedList_.GetNextStartTime() < dataBlockedList_.GetNextStartTime()) {
        return std::make_pair(lobBlockedList_.GetNextBlockedRequestSize(), true);
    }
    return std::make_pair(dataBlockedList_.GetNextBlockedRequestSize(), false);
}

template <typename W, typename R>
Status StreamManager::HandleBlockedRequestImpl(std::shared_ptr<BlockedCreateRequest<W, R>> &&blockedReq,
                                               bool lockBeforeAdd)
{
    Status rc;
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(blockedReq->traceId_);
    auto retryCount = blockedReq->retryCount_.load(std::memory_order_relaxed);
    auto req = blockedReq->GetCreateRequest();
    const auto &producerId = req.producer_id();
    auto subTimeout = blockedReq->GetRemainingTimeMs();
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, P:%s] Allocating shared memory. subTimeout: %zu", LogPrefix(),
                                              producerId, subTimeout);
    // Check for stream state. If CreatePage goes to sleep for OOM,
    // it will return after waking up and seeing reset is going on.
    rc = CheckIfStreamActive();
    if (rc.IsError()) {
        return blockedReq->SendStatus(rc);
    }
    // The launch of this thread and creation of stream manager may have used up some time.
    // If this elapsed time was more than the initial sub-time, then return now with timeout error.
    if (retryCount > 0) {
        RETURN_IF_NOT_OK(blockedReq->HandleBlockedCreateTimeout());
    }
    // Invoke the call back to allocate the memory
    rc = (*blockedReq)();
    // Refresh how much time left
    subTimeout = blockedReq->GetRemainingTimeMs();
    INJECT_POINT("HandleBlockedRequestImpl.subTimeout", [&subTimeout]() mutable {
        subTimeout = 0;
        return Status::OK();
    });
    if (rc.GetCode() == K_OUT_OF_MEMORY && req.sub_timeout() > 0 && subTimeout > 0) {
        // Add this request to a queue of blocked requests and then return.
        // The client will remain waiting until a timer unblocks or another event (free pages) executes the request
        // and returns to the client.
        LOG(INFO) << FormatString(
            "OOM. retry a blocked request to the blocked queue for stream %s with producer %s and new timeout %zu. "
            "retry count %zu",
            streamName_, producerId, subTimeout, retryCount);

        auto scSvc = scSvc_.lock();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");

        Status blocked_rc = AddBlockedCreateRequest(scSvc.get(), std::move(blockedReq), lockBeforeAdd);
        // Log error if we can not block and return original OOM error to the user
        LOG_IF_ERROR(blocked_rc, "error while producer blocking");
        if (blocked_rc.IsError()) {
            return blockedReq->SendStatus(rc);
        }
        // Return OOM back to the caller so the caller can distinguish between a successful retry vs
        // a failed-but-requeue retry
        return rc;
    }
    if (rc.IsError()) {
        return blockedReq->SendStatus(rc);
    }
    return Status::OK();
}

Status StreamManager::GetDataPage(
    const GetDataPageReqPb &req, const std::shared_ptr<Subscription> &sub,
    const std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> &serverApi)
{
    const auto &consumerId = req.consumer_id();
    CHECK_FAIL_RETURN_STATUS(sub->GetSubscriptionType() == SubscriptionType::STREAM, StatusCode::K_INVALID,
                             "Only support STREAM mode.");
    std::shared_ptr<Consumer> consumer;
    RETURN_IF_NOT_OK(sub->GetConsumer(consumerId, consumer));
    RETURN_IF_NOT_OK(GetExclusivePageQueue()->GetDataPage(req, consumer, serverApi));
    return Status::OK();
}

void StreamManager::TryWakeUpPendingReceive()
{
    if (pageQueueHandler_->ExistsSharedPageQueue()) {
        return;
    }
    // Consumer node using exclusive page.
    uint64_t lastCursor = GetExclusivePageQueue()->GetLastAppendCursor();
    PerfPoint point(PerfKey::MANAGER_TRY_WAKE_UP_RECV_GET_LOCK);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    point.Record();
    PerfPoint point1(PerfKey::MANAGER_TRY_WAKE_UP_RECV_LOGIC);
    for (const auto &sub : subs_) {
        auto status = sub.second->TryWakeUpPendingReceive(lastCursor);
        if (status.IsError()) {
            LOG(WARNING) << "Failed to wake up pending recv for sub:" << sub.first << ", " << status.ToString();
        }
    }
}

uint64_t StreamManager::UpdateLastAckCursorUnlocked(uint64_t minSubsAckCursor)
{
    if (pageQueueHandler_ == nullptr || IsRetainData()) {
        return 0;
    }
    bool success = false;
    do {
        uint64_t val = lastAckCursor_.load();
        // Stream's lastAckCursor = min{sub0 lastAckCursor, sub1 lastAckCursor,..., subN lastAckCursor}.
        for (const auto &sub : subs_) {
            const auto &lastSubAck = sub.second->UpdateLastAckCursor();
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[Stream %s, subscription %s] lastAckCursor = %zu", streamName_,
                                                      sub.first, lastSubAck);
            minSubsAckCursor = std::min(minSubsAckCursor, lastSubAck);
        }
        // Also go through all remote consumers. We may in the process of sending elements
        // to the remote worker.
        if (!remoteSubWorkerDict_.empty() || remoteWorkerManager_->HasRemoteConsumers(streamName_)) {
            auto remoteAckCursor = remoteWorkerManager_->GetLastAckCursor(streamName_);
            minSubsAckCursor = std::min(minSubsAckCursor, remoteAckCursor);
        }
        if (minSubsAckCursor > val) {
            INJECT_POINT_NO_RETURN("UpdateLastAckCursorUnlocked.sleep");
            success = lastAckCursor_.compare_exchange_strong(val, minSubsAckCursor);
            if (success) {
                LOG(INFO) << FormatString("[%s] The last ack of stream update from %zu to %zu", LogPrefix(), val,
                                          minSubsAckCursor);
                return minSubsAckCursor;
            }
        } else {
            return minSubsAckCursor;
        }
    } while (true);
}

Status StreamManager::RemoteAck()
{
    auto lastAppendCursor = GetLastAppendCursor();
    uint64_t newAckCursor = 0;
    {
        INJECT_POINT("StreamManager.RemoteAck.delay");
        ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        RETURN_OK_IF_TRUE(pageQueueHandler_ == nullptr);
        // If local consumer exists, leave the ack to be done by the ack thread itself.
        RETURN_OK_IF_TRUE(!subs_.empty());
        newAckCursor = UpdateLastAckCursorUnlocked(lastAppendCursor);
        // Early release of the lock since StreamManager::mutex_ is mostly to protect pubs and subs structures.
    }
    RETURN_IF_NOT_OK(GetExclusivePageQueue()->Ack(newAckCursor));
    EarlyReclaim(true, lastAppendCursor, newAckCursor);
    return Status::OK();
}

Status StreamManager::AckCursors()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    ackWp_.Wait();
    RETURN_IF_NOT_OK(CheckIfStreamActive());
    RETURN_OK_IF_TRUE(pageQueueHandler_ == nullptr);
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] GC starts", LogPrefix());
    auto lastAppendCursor = GetLastAppendCursor();
    uint64_t newAckCursor;
    {
        ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        INJECT_POINT("StreamManager.AckCursors.delay");
        newAckCursor = UpdateLastAckCursorUnlocked(lastAppendCursor);
    }
    RETURN_IF_NOT_OK(GetExclusivePageQueue()->Ack(newAckCursor, GetStreamMetaShm()));
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] GC ends", LogPrefix());
    return Status::OK();
}

Status StreamManager::AddRemotePubNode(const std::string &pubWorkerAddr)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto ret = remotePubWorkerDict_.emplace(pubWorkerAddr);
    CHECK_FAIL_RETURN_STATUS(ret.second, StatusCode::K_DUPLICATED,
                             "One remote pub node can only make one one-time broadcast to all sub nodes");
    if (scStreamMetrics_) {
        scStreamMetrics_->IncrementMetric(StreamMetric::NumRemoteProducers, 1);
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s], Add remote pub node <%s> success", LogPrefix(), pubWorkerAddr);
    return Status::OK();
}

Status StreamManager::HandleClosedRemotePubNode(bool forceClose)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    // Do not actually remove the remote publisher. Even if the remote side has closed and the master has given this
    // notification, there is likely many elements still pending receive. They cannot be received unless there is a
    // remote publisher instance. The remote pubs will be cleaned up later when the stream is removed.
    Status rc;
    if (forceClose && wakeupPendingRecvOnProdFault_) {
        for (const auto &sub : subs_) {
            Status rc1 = sub.second->SetForceClose();
            if (rc.IsOk()) {
                rc = rc1;
            }
        }
    }
    return rc;
}

Status StreamManager::AddRemoteSubNode(const HostPort &subWorker, const SubscriptionConfig &subConfig,
                                       const std::string &consumerId, uint64_t &lastAckCursor)
{
    Raii resumeAck([this]() { ResumeAckThread(); });
    // We are adding a remote consumer and on return we will set up a cursor to begin with.
    // We also need to ensure the garbage collector thread is not purging the required page
    // from memory.
    PauseAckThread();
    {
        auto lastAppendCursor = GetLastAppendCursor();
        WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        lastAckCursor = UpdateLastAckCursorUnlocked(lastAppendCursor);
        // If a new worker node, we add it into remote subscription dict.
        const auto &subWorkerHost = subWorker.ToString();
        auto iter = remoteSubWorkerDict_.find(subWorkerHost);
        if (iter == remoteSubWorkerDict_.end()) {
            bool success;
            std::tie(iter, success) =
                remoteSubWorkerDict_.emplace(subWorkerHost, std::make_shared<SubWorkerDesc>(subWorker));
        }
        RETURN_IF_NOT_OK(iter->second->AddConsumer(subConfig, consumerId));
    }
    if (scStreamMetrics_) {
        scStreamMetrics_->IncrementMetric(StreamMetric::NumRemoteConsumers, 1);
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, RW:%s, C:%s], Add remote consumer succeeded", LogPrefix(),
                                                subWorker.ToString(), consumerId);
    return Status::OK();
}

Status StreamManager::DelRemoteSubNode(const HostPort &subWorker, const std::string &consumerId)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, RW:%s, C:%s], Delete remote consumer begin", LogPrefix(),
                                                subWorker.ToString(), consumerId);
    // key: subWorker value: SubWorkerDesc object. So if not exist, we raise runtime error.
    auto iter = remoteSubWorkerDict_.find(subWorker.ToString());
    CHECK_FAIL_RETURN_STATUS(iter != remoteSubWorkerDict_.end(), StatusCode::K_NOT_FOUND,
                             FormatString("[%s]-[%s] Remote Sub node:<%s> not exist on worker:<%s>'s remoteSubDict",
                                          streamName_, consumerId, subWorker.ToString(), workerAddr_));

    RETURN_IF_NOT_OK(iter->second->DelConsumer(consumerId));
    if (iter->second->ConsumerNum() == 0) {
        (void)remoteSubWorkerDict_.erase(iter);
    }
    if (scStreamMetrics_) {
        scStreamMetrics_->DecrementMetric(StreamMetric::NumRemoteConsumers, 1);
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, RW:%s, C:%s], Delete remote consumer succeeded", LogPrefix(),
                                                subWorker.ToString(), consumerId);
    return Status::OK();
}

Status StreamManager::SyncSubTable(const std::vector<ConsumerMeta> &subTable, bool isRecon, uint64_t &lastAckCursor)
{
    Raii resumeAck([this]() { ResumeAckThread(); });
    // We are adding a remote consumer and on return we will set up a cursor to begin with.
    // We also need to ensure the garbage collector thread is not purging the required page
    // from memory.
    PauseAckThread();
    {
        auto lastAppendCursor = GetLastAppendCursor();
        WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        lastAckCursor = UpdateLastAckCursorUnlocked(lastAppendCursor);
        // Definition: ConsumerMeta = (consumerId_, workerAddress_, subConfig_, lastAckCursor_).
        if (!isRecon) {
            remoteSubWorkerDict_.clear();
            if (scStreamMetrics_) {
                scStreamMetrics_->LogMetric(StreamMetric::NumRemoteConsumers, 0);
            }
        }
        for (const auto &sub : subTable) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, RW:%s, C:%s]", LogPrefix(),
                                                        sub.WorkerAddress().ToString(), sub.ConsumerId());
            auto iter = remoteSubWorkerDict_.find(sub.WorkerAddress().ToString());
            if (iter == remoteSubWorkerDict_.end()) {
                auto newSubWorkerDesc = std::make_shared<SubWorkerDesc>(sub.WorkerAddress());
                iter = remoteSubWorkerDict_.emplace(sub.WorkerAddress().ToString(), std::move(newSubWorkerDesc)).first;
            }
            auto &remoteWorkerDesc = iter->second;
            RETURN_IF_NOT_OK(remoteWorkerDesc->AddConsumer(sub.SubConfig(), sub.ConsumerId()));
            if (scStreamMetrics_) {
                scStreamMetrics_->IncrementMetric(StreamMetric::NumRemoteConsumers, 1);
            }
        }
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] SyncSubTable success, table size:%zu", LogPrefix(),
                                                remoteSubWorkerDict_.size());
    return Status::OK();
}

Status StreamManager::SyncPubTable(const std::vector<HostPort> &pubTable, bool isRecon)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    // Always clear the elder remote dict if we get a new first consumer on local node.
    if (!remotePubWorkerDict_.empty() && !isRecon) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Stream:<%s>, State:<First consumer so remotePubWorkerDict should be "
                                             "empty>",
                                             streamName_));
    }
    for (const auto &pub : pubTable) {
        auto ret = remotePubWorkerDict_.emplace(pub.ToString());
        CHECK_FAIL_RETURN_STATUS(ret.second, StatusCode::K_DUPLICATED,
                                 "Runtime error: Fail to add pub worker into dict");
    }
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumRemoteProducers, remotePubWorkerDict_.size());
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] SyncPubTable success, table size:%d", LogPrefix(),
                                                remotePubWorkerDict_.size());
    return Status::OK();
}

void StreamManager::GetLocalProducers(std::vector<std::string> &localProducers)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    for (const auto &kv : pubs_) {
        const auto &producer = kv.second;
        localProducers.emplace_back(producer->GetId());
    }
}

void StreamManager::GetLocalConsumers(std::vector<std::pair<std::string, SubscriptionConfig>> &localConsumers)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    localConsumers.clear();
    for (const auto &kv : subs_) {
        const auto &sub = kv.second;
        std::vector<std::string> consumerIds;
        sub->GetAllConsumers(consumerIds);
        for (auto &consumerId : consumerIds) {
            localConsumers.emplace_back(std::move(consumerId), kv.second->GetSubscriptionConfig());
        }
    }
}

Status StreamManager::CreateSubscriptionIfMiss(const SubscriptionConfig &config, uint64_t &lastAckCursor)
{
    auto lastAppendCursor = GetLastAppendCursor();
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    if (subs_.empty()) {
        // Reserve local cache memory for stream consumer using the batch size FLAGS_zmq_chunk_sz,
        // fail the request if memory is not available.
        auto scSvc = scSvc_.lock();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
        RETURN_IF_NOT_OK(scSvc->ReserveMemoryFromUsageMonitor(GetStreamName(), FLAGS_zmq_chunk_sz));
    }
    lastAckCursor = UpdateLastAckCursorUnlocked(lastAppendCursor);
    auto iter = subs_.find(config.subscriptionName);
    if (iter == subs_.end()) {
        auto ret = subs_.emplace(config.subscriptionName,
                                 std::make_shared<Subscription>(config, lastAckCursor, GetStreamName()));
        CHECK_FAIL_RETURN_STATUS(ret.second, StatusCode::K_DUPLICATED,
                                 "Failed to add subscription into stream manager");
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, Sub:%s] Create new subscription succeeded", LogPrefix(),
                                                  config.subscriptionName);
    } else {
        CHECK_FAIL_RETURN_STATUS(iter->second->GetSubscriptionType() == config.subscriptionType, StatusCode::K_INVALID,
                                 "The subscription type of request subscription is inconsistent with the type "
                                 "stored in subs_ dict");
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, Sub:%s] Subscription already exist.", LogPrefix(),
                                                  config.subscriptionName);
    }
    return Status::OK();
}

Status StreamManager::ClearAllRemotePub()
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    ClearAllRemotePubUnlocked();
    return Status::OK();
}

void StreamManager::ClearAllRemotePubUnlocked()
{
    remotePubWorkerDict_.clear();
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumRemoteProducers, 0);
    }
}

Status StreamManager::EarlyReclaim(bool remoteAck, uint64_t lastAppendCursor, uint64_t newAckCursor)
{
    {
        // If CreateProducer is running at this point, we will wait on the reclaimMutex_.
        WriteLockHelper reclaimLck(STREAM_COMMON_LOCK_ARGS(reclaimMutex_));
        reclaimWp_.Clear();
        // We don't need to hold reclaimMutex_. The reclaim wait post is on.
        // If CreateProducer is running at this point, it will be blocked on the WaitPost
        // until we finish.
    }
    Raii wpRaii([this]() { reclaimWp_.Set(); });
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    // If local consumer still exists, leave the ack to be done by the ack thread itself.
    RETURN_OK_IF_TRUE(!subs_.empty());
    // We will check if all remote consumers are gone. If it is empty, it means
    // (a) all the local producers are gone, or
    // (b) remote consumer has done an early exit.
    // Both cases are driven by the master rpc to this worker.
    // Either case there is no need to cache pages for the future.
    RETURN_OK_IF_TRUE(!remoteSubWorkerDict_.empty());
    if (remoteAck) {
        const uint64_t timeoutMs = 10;
        LOG(INFO) << FormatString("[%s] Reclaim memory. Last append %zu. Last ack %zu", LogPrefix(), lastAppendCursor,
                                  newAckCursor);
        RETURN_IF_NOT_OK_EXCEPT(GetExclusivePageQueue()->ReclaimAckedChain(timeoutMs), K_TRY_AGAIN);
    }
    // Finally if all elements have been acked, or all producers are gone, we release all the shared memory.
    RETURN_OK_IF_TRUE(!pubs_.empty());
    bool hasRemoteConsumers = remoteWorkerManager_->HasRemoteConsumers(streamName_);
    auto remoteAckCursor = remoteWorkerManager_->GetLastAckCursor(streamName_);
    // refresh last page and last append cursor since there can be producer inserted elements and closed since
    // the last time we get last append cursor.
    const bool updateLocalPubLastPage = false;
    RETURN_IF_NOT_OK(GetExclusivePageQueue()->MoveUpLastPage(updateLocalPubLastPage));
    lastAppendCursor = GetLastAppendCursor();
    LOG(INFO) << FormatString("[%s] HasRemoteConsumers = %s remoteAckCursor = %zu lastAppendCursor = %zu", LogPrefix(),
                              (hasRemoteConsumers ? "true" : "false"), remoteAckCursor, lastAppendCursor);
    if (!hasRemoteConsumers || remoteAckCursor == lastAppendCursor) {
        // Data should have been purged by this time at early reclaim logic, so stop to push new buffer into RW
        if (!pageQueueHandler_->ExistsSharedPageQueue()) {
            RETURN_IF_NOT_OK(remoteWorkerManager_->DoneScanning(streamName_));
        }
        RETURN_IF_NOT_OK(GetExclusivePageQueue()->ReleaseAllPages());
    }
    return Status::OK();
}

Status StreamManager::ClearAllRemoteConsumerUnlocked(bool forceClose)
{
    Status rc;
    if (forceClose && wakeupPendingRecvOnProdFault_) {
        // add current node (self node) in the list of forced nodes.
        for (const auto &sub : subs_) {
            Status rc1 = sub.second->SetForceClose();
            if (rc.IsOk()) {
                rc = rc1;
            }
        }
    }
    remoteSubWorkerDict_.clear();
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumRemoteConsumers, 0);
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Clear all remote consumer succeeded", LogPrefix());
    return rc;
}

std::string StreamManager::LogPrefix() const
{
    return FormatString("S:%s", streamName_);
}

Status StreamManager::GetSubscription(const std::string &subName, std::shared_ptr<Subscription> &subscription)
{
    PerfPoint point(PerfKey::MANAGE_GET_SUB);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto iter = subs_.find(subName);
    if (iter == subs_.end()) {
        RETURN_STATUS(StatusCode::K_SC_CONSUMER_NOT_FOUND, "Subscription not found" + subName);
    }
    RETURN_RUNTIME_ERROR_IF_NULL(iter->second);
    subscription = iter->second;
    point.Record();
    return Status::OK();
}

Status StreamManager::RemovePubSubFromResetList(std::vector<std::string> &prodConList)
{
    Status sc = Status::OK();
    for (auto pubSubId : prodConList) {
        bool found = false;
        for (auto iter = prodConResetList_.begin(); iter != prodConResetList_.end(); ++iter) {
            if (*iter == pubSubId) {
                prodConResetList_.erase(iter);
                found = true;
                break;
            }
        }
        if (!found) {
            sc = Status(K_NOT_FOUND, FormatString("%s Not found in the list of resetting pubs/subs", pubSubId));
            LOG(ERROR) << sc.GetMsg();
        }
    }
    if (prodConResetList_.empty() && CheckIfStreamInState(StreamState::RESET_IN_PROGRESS)) {
        return ResetStreamEnd();
    }
    return sc;
}

Status StreamManager::ResetStreamStart(std::vector<std::string> &prodConList)
{
    // Stop remote producer pushing more data with the stream status flag.
    std::unique_lock<std::shared_timed_mutex> lock(resetMutex_);
    {
        // protect create/close pubs_/subs_
        WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        if (CheckIfStreamInState(StreamState::ACTIVE)) {
            RETURN_IF_NOT_OK(SetNewState(StreamState::RESET_IN_PROGRESS));
            prodConResetList_.clear();
            for (const auto &prod : pubs_) {
                prodConResetList_.emplace_back(prod.first);
            }
            for (auto &sub : subs_) {
                std::vector<std::string> consumerIds;
                sub.second->GetAllConsumers(consumerIds);
                prodConResetList_.insert(prodConResetList_.end(), consumerIds.begin(), consumerIds.end());
            }
        } else if (CheckIfStreamInState(StreamState::DELETE_IN_PROGRESS)) {
            RETURN_STATUS(K_SC_STREAM_DELETE_IN_PROGRESS,
                          FormatString("Delete is in progress on Stream [%s].", streamName_));
        } else if (CheckIfStreamInState(StreamState::RESET_COMPLETE)) {
            LOG(WARNING) << "Reset is already completed for stream: " << streamName_;
            return Status::OK();
        }
    }
    return RemovePubSubFromResetList(prodConList);
}

Status StreamManager::ResetStreamEnd()
{
    // As we have one BufferPool per remote worker
    // We will be inserting EOS num of remoteWorkers x num of producers
    // Create a placeholder element for producer
    {
        remoteWorkerManager_->PurgeBuffer(shared_from_this());
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] ResetStreamStart for remote consumer", LogPrefix());
        if (auto workerWorkerSCServicePtr = workerWorkerSCService_.lock()) {
            workerWorkerSCServicePtr->PurgeBuffer(shared_from_this());
        }
    }

    // Check we got replies from all EOS messages that we inserted
    INJECT_POINT("worker.stream.sleep_while_reset");
    // If we got replies from all -> Start cleanup
    // Clear pointer and indexes of each producer
    Status rc = Status::OK();
    {
        // protect read pubs_/subs_
        ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        for (const auto &pub : pubs_) {
            Status status = pub.second->CleanupProducer();
            if (status.IsError()) {
                LOG(ERROR) << status.GetMsg();
                rc = status;
            }
        }

        // Clear pointer and indexes of each consumer
        for (const auto &sub : subs_) {
            sub.second->CleanupSubscription();
        }
    }

    // Clear all the indexes and maps in StreamDataObject
    RETURN_IF_NOT_OK(GetExclusivePageQueue()->Reset());
    RETURN_IF_NOT_OK(remoteWorkerManager_->ResetStreamScanList(streamName_));
    {
        WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        blockOnOOM_.clear();
    }
    Status status = UnblockCreators();
    if (status.IsError()) {
        LOG(ERROR) << status.GetMsg();
        rc = status;
    }
    // Notify the client
    rc = SetNewState(StreamState::RESET_COMPLETE);
    VLOG(SC_INTERNAL_LOG_LEVEL) << "Reset complete for " << streamName_;
    return rc;
}

void StreamManager::ForceCloseClients()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    for (const auto &pub : pubs_) {
        pub.second->SetForceClose();
    }

    for (const auto &sub : subs_) {
        sub.second->SetForceClose();
    }
}

Status StreamManager::GetSubType(const std::string &subName, SubscriptionType &type)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto iter = subs_.find(subName);
    if (iter == subs_.end()) {
        RETURN_STATUS(StatusCode::K_SC_CONSUMER_NOT_FOUND, "Subscription not found" + subName);
    }
    type = iter->second->GetSubscriptionType();
    return Status::OK();
}

int64_t StreamManager::GetStreamPageSize()
{
    return pageQueueHandler_->GetPageSize();
}

double StreamManager::GetStreamMemAllocRatio()
{
    auto maxAllocatedMemorySC = scAllocateManager_->GetTotalMaxStreamSHMSize();
    if (maxAllocatedMemorySC != 0) {
        return (GetExclusivePageQueue()->GetMaxStreamSize() / (double)maxAllocatedMemorySC);
    } else {
        return 1.0;
    }
}

Status StreamManager::CheckConsumerExist(const std::string &workerAddr)
{
    // No consumer on this node, deny data push.
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    CHECK_FAIL_RETURN_STATUS(!subs_.empty(), StatusCode::K_SC_CONSUMER_NOT_FOUND,
                             FormatString("No consumer on this node [%s - %s]", streamName_, workerAddr));
    return Status::OK();
}

Status StreamManager::SendBlockProducerReq(const std::string &remoteWorkerAddr)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    VLOG(SC_NORMAL_LOG_LEVEL) << "Blocking Producer for stream: " << streamName_
                              << " sending to remote worker: " << remoteWorkerAddr;
    HostPort workerHostPort;
    RETURN_IF_NOT_OK(workerHostPort.ParseString(remoteWorkerAddr));
    std::shared_ptr<ClientWorkerSCService_Stub> stub;
    auto scSvc = scSvc_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
    RETURN_IF_NOT_OK(scSvc->GetWorkerStub(workerHostPort, stub));
    std::unique_ptr<ClientUnaryWriterReader<BlockProducerReqPb, BlockProducerRspPb>> clientApi;
    RETURN_IF_NOT_OK(stub->BlockProducer(RpcOptions(), &clientApi));
    BlockProducerReqPb req;
    req.set_stream_name(streamName_);
    req.set_worker_addr(workerAddr_);
    RpcOptions opts;
    SET_RPC_TIMEOUT(scTimeoutDuration, opts);
    req.set_timeout(opts.GetTimeout());
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    // Send only. No need to wait for any reply, and let clientApi goes out of scope by itself.
    RETURN_IF_NOT_OK(clientApi->Write(req));
    // Wait for the reply to ensure ordering
    BlockProducerRspPb rsp;
    RETURN_IF_NOT_OK(clientApi->Read(rsp));

    if (scStreamMetrics_) {
        scStreamMetrics_->IncrementMetric(StreamMetric::NumRemoteProducersBlocked, 1);
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << "Blocking Producer for stream: " << streamName_
                              << " sent to remote worker: " << remoteWorkerAddr << " is Successful";
    INJECT_POINT("StreamManager.SendBlockProducerReq.delay");
    return Status::OK();
}

Status StreamManager::BlockProducer(const std::string &workerAddr, bool addCallBack)
{
    {
        WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        auto it = blockOnOOM_.find(workerAddr);
        if (it == blockOnOOM_.end()) {
            bool success;
            std::tie(it, success) = blockOnOOM_.emplace(workerAddr, false);
        }
        if (it->second) {
            return Status::OK();
        }
        it->second = true;
        LOG(INFO) << FormatString("[%s] BlockProducer from remote worker %s", LogPrefix(), workerAddr);
    }
    auto scSvc = scSvc_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
    auto streamName = streamName_;
    scSvc->GetThreadPool()->Execute([scSvc, streamName, workerAddr, addCallBack]() {
        // Send the request
        StreamManagerMap::const_accessor accessor;
        Status rc = scSvc->GetStreamManager(streamName, accessor);
        if (rc.IsError()) {
            return;
        }
        std::shared_ptr<StreamManager> streamMgr = accessor->second;
        LOG_IF_ERROR(streamMgr->SendBlockProducerReq(workerAddr), "block error");
        // Add a call back after send block request to maintain ordering
        std::weak_ptr<StreamManager> weakStreamMgr = streamMgr;
        if (!addCallBack) {
            return;
        }
        streamMgr->AddUnblockCallback(workerAddr, [weakStreamMgr, workerAddr, streamName]() {
            auto streamMgr = weakStreamMgr.lock();
            if (streamMgr != nullptr) {
                LOG_IF_ERROR(streamMgr->UnBlockProducer(workerAddr), "unblock error");
            } else {
                LOG(WARNING)
                    << "The StreamManager already destroy when execute UnBlockProducer callback for streamName "
                    << streamName;
            }
        });
    });
    return Status::OK();
}

Status StreamManager::SendUnBlockProducerReq(const std::string &remoteWorkerAddr)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << FormatString("[%s] UnBlockProducer from remote worker %s", LogPrefix(), remoteWorkerAddr);
    ResetOOMState(remoteWorkerAddr);  // Producer is unblocked
    HostPort workerHostPort;
    RETURN_IF_NOT_OK(workerHostPort.ParseString(remoteWorkerAddr));
    std::shared_ptr<ClientWorkerSCService_Stub> stub;
    auto scSvc = scSvc_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
    RETURN_IF_NOT_OK(scSvc->GetWorkerStub(workerHostPort, stub));
    std::unique_ptr<ClientUnaryWriterReader<UnblockProducerReqPb, UnblockProducerRspPb>> clientApi;
    RETURN_IF_NOT_OK(stub->UnblockProducer(RpcOptions(), &clientApi));
    UnblockProducerReqPb req;
    req.set_stream_name(streamName_);
    req.set_worker_addr(workerAddr_);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    // Wait for the reply to ensure ordering
    RETURN_IF_NOT_OK(clientApi->Write(req));
    UnblockProducerRspPb rsp;
    RETURN_IF_NOT_OK(clientApi->Read(rsp));
    if (scStreamMetrics_) {
        scStreamMetrics_->DecrementMetric(StreamMetric::NumRemoteProducersBlocked, 1);
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << "UnBlocking Producer for stream: " << streamName_
                              << " sent to remote worker: " << remoteWorkerAddr << " is Successful";
    return Status::OK();
}

void StreamManager::ResetOOMState(const std::string &remoteWorkerAddr)
{
    // Unblock call back is already set
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto it = blockOnOOM_.find(remoteWorkerAddr);
    if (it != blockOnOOM_.end()) {
        it->second = false;
    }
}

Status StreamManager::UnBlockProducer(const std::string &workerAddr)
{
    auto weakThis = weak_from_this();
    auto scSvc = scSvc_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(scSvc != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
    scSvc->GetThreadPool()->Execute([weakThis, workerAddr, streamName = streamName_]() {
        auto streamManager = weakThis.lock();
        if (streamManager != nullptr) {
            LOG_IF_ERROR(streamManager->SendUnBlockProducerReq(workerAddr), "unblock error");
        } else {
            LOG(WARNING) << "The StreamManager already destroy when async UnBlockProducer for streamName "
                         << streamName;
        }
    });
    return Status::OK();
}

bool StreamManager::IsProducerBlocked(const std::string &workerAddr)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    auto it = blockOnOOM_.find(workerAddr);
    if (it == blockOnOOM_.end()) {
        return false;
    }
    return it->second;
}

Status StreamManager::CopyElementView(std::shared_ptr<RecvElementView> &recvElementView, UsageMonitor &usageMonitor,
                                      uint64_t timeoutMs)
{
    // Decrypt failure is non-recoverable error.
    auto pageQueue = GetExclusivePageQueue();
    size_t totalLength = 0;
    // Close consumer can trigger memory reclaim, but at the same time,
    // remote elements might still get written into shm page.
    // So we block the potential reclaim before we finish the ongoing BatchInsert.
    // Later push requests will get K_SC_CONSUMER_NOT_FOUND if consumer is indeed closed.
    BlockMemoryReclaim();
    Raii raii([this]() { UnblockMemoryReclaim(); });
    std::vector<size_t> sz(recvElementView->sz_.begin() + recvElementView->idx_, recvElementView->sz_.end());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!sz.empty(), K_INVALID,
                                         FormatString("[%s] invalid idx %zu", LogPrefix(), recvElementView->idx_));
    // To reduce the chance of OOM and seal a page that is only partially filled, we will do few rows at a time
    // while competing with local producers. We can also be resuming from where we left off last time.
    std::pair<size_t, size_t> res(0, 0);
    auto rc = pageQueue->BatchInsert(recvElementView->GetBufferPointer(), sz, res, timeoutMs,
                                     recvElementView->headerBits_, GetStreamMetaShm());
    totalLength = res.second;
    recvElementView->idx_ += res.first;
    // PageView is processed and will be removed from local cache
    if (totalLength > 0) {
        usageMonitor.DecUsage(streamName_, recvElementView->workerAddr_, totalLength);
    }
    if (rc.IsOk()) {
        // Sanity check. If all successful, idx_ should now be at the end.
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(recvElementView->idx_ == recvElementView->sz_.size(), K_RUNTIME_ERROR,
                                             FormatString("[%s] Expect %zu but got %zu", LogPrefix(),
                                                          recvElementView->sz_.size(), recvElementView->idx_));
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
            "Page is copied successfully stream name: %s, worker addr: %s, "
            "seq: %zu, count: %zu, size: %zu, stream state: %s",
            recvElementView->StreamName(), recvElementView->workerAddr_, recvElementView->seqNo_,
            recvElementView->sz_.size(), totalLength, PrintStreamStatus());
    } else {
        switch (rc.GetCode()) {
            case K_OUT_OF_MEMORY:
                LOG(WARNING) << FormatString("Out of memory for stream: %s, status %s, Stream Status: %s", streamName_,
                                             rc.GetMsg(), PrintStreamStatus());
                LOG_IF_ERROR(BlockProducer(recvElementView->workerAddr_, true),
                             "Failed to block sender");  // Sends a Block RPC to other worker to wait
                return rc;
            case K_TRY_AGAIN:
                return rc;
            default:
                LOG(ERROR) << FormatString("[%s] Non-recoverable error. %s", LogPrefix(), rc.ToString());
        }
    }
    recvElementView.reset();  // We will release memory except for OOM case where we retry
    return Status::OK();
}

uint64_t StreamManager::GetLastAppendCursor() const
{
    if (pageQueueHandler_ != nullptr) {
        return GetExclusivePageQueue()->GetLastAppendCursor();
    }
    return 0;
}

void StreamManager::PauseAckThread()
{
    // To ensure the GC thread is paused. Not only we clear the wait post
    // we will hold the lock in exclusive in the same order GC thread is doing
    WriteLockHelper wlock(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    ackWp_.Clear();
}

void StreamManager::ResumeAckThread()
{
    ackWp_.Set();
}

uint64_t StreamManager::GetEleCount()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    uint64_t val = 0;
    for (auto &sub : subs_) {
        val += sub.second->GetElementCountReceived();
    }
    return val;
}

uint64_t StreamManager::GetEleCountAcked()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    uint64_t val = subs_.size() > 0 ? std::numeric_limits<uint64_t>::max() : 0;
    uint64_t count = 0;
    for (auto &sub : subs_) {
        count = sub.second->UpdateLastAckCursor();
        if (val > count) {
            val = count;
        }
    }
    return val;
}

uint64_t StreamManager::GetEleCountSentAndReset()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    uint64_t val = 0;
    for (auto &pub : pubs_) {
        val += pub.second->GetElementCountAndReset();
    }
    return val;
}

uint64_t StreamManager::GetEleCountReceived()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    uint64_t val = subs_.size() > 0 ? std::numeric_limits<uint64_t>::max() : 0;
    uint64_t count = 0;
    for (auto &sub : subs_) {
        count = sub.second->GetElementCountReceived();
        if (val > count) {
            val = count;
        }
    }
    return val;
}

uint64_t StreamManager::GetSendRequestCountAndReset()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    uint64_t val = 0;
    for (auto &pub : pubs_) {
        val += pub.second->GetRequestCountAndReset();
    }
    return val;
}

uint64_t StreamManager::GetReceiveRequestCountAndReset()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    uint64_t val = 0;
    for (auto &sub : subs_) {
        val += sub.second->GetRequestCountAndReset();
    }
    return val;
}

void StreamManager::AddUnblockCallback(const std::string &addr, std::function<void()> unblockCallback)
{
    if (pageQueueHandler_) {
        GetExclusivePageQueue()->AddUnblockCallback(addr, std::move(unblockCallback));
    }
}

bool StreamManager::AutoCleanup() const
{
    return GetExclusivePageQueue()->AutoCleanup();
}

std::vector<std::string> StreamManager::GetRemoteWorkers() const
{
    std::vector<std::string> remoteWorkers;
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    std::transform(remoteSubWorkerDict_.begin(), remoteSubWorkerDict_.end(), std::back_inserter(remoteWorkers),
                   [](auto &kv) { return kv.first; });
    return remoteWorkers;
}

bool StreamManager::IsRemotePubEmpty()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    return remotePubWorkerDict_.empty();
}

Status StreamManager::UpdateStreamFields(const StreamFields &streamFields, bool reserveShm)
{
    RETURN_IF_NOT_OK(pageQueueHandler_->UpdateStreamFields(streamFields));
    if (EnableSharedPage(streamFields.streamMode_)) {
        ShmView shmViewOfStreamMeta;
        RETURN_IF_NOT_OK(
            GetOrCreateShmMeta(TenantAuthManager::Instance()->ExtractTenantId(streamName_), shmViewOfStreamMeta));
    }
    if (reserveShm) {
        // Reserve shared memory if we haven't done it previously
        auto pageQueue = GetExclusivePageQueue();
        RETURN_IF_NOT_OK(pageQueue->ReserveStreamMemory());
        LOG(INFO) << FormatString("[%s] %zu bytes of shared memory has been reserved", LogPrefix(),
                                  pageQueue->GetReserveSize());
    }
    TryWakeUpPendingReceive();
    return Status::OK();
}

void StreamManager::GetStreamFields(StreamFields &streamFields)
{
    GetExclusivePageQueue()->GetStreamFields(streamFields);
}

Status StreamManager::InitStreamMetrics()
{
    return ScMetricsMonitor::Instance()->AddStream(streamName_, weak_from_this(), scStreamMetrics_);
}

bool StreamManager::CheckHadEnoughMem(size_t sz) const
{
    if (pageQueueHandler_->ExistsSharedPageQueue()) {
        // Fixme stage 2
        return true;
    }
    return GetExclusivePageQueue()->CheckHadEnoughMem(sz).IsOk();
}

void StreamManager::UpdateStreamMetrics()
{
    if (scStreamMetrics_) {
        scStreamMetrics_->IncrementMetric(StreamMetric::NumTotalElementsSent, GetEleCountSentAndReset());
        scStreamMetrics_->LogMetric(StreamMetric::NumTotalElementsReceived, GetEleCountReceived());
        scStreamMetrics_->LogMetric(StreamMetric::NumTotalElementsAcked, GetEleCountAcked());
        scStreamMetrics_->IncrementMetric(StreamMetric::NumSendRequests, GetSendRequestCountAndReset());
        scStreamMetrics_->IncrementMetric(StreamMetric::NumReceiveRequests, GetReceiveRequestCountAndReset());
        scStreamMetrics_->LogMetric(StreamMetric::NumLocalProducersBlocked,
                                    lobBlockedList_.Size() + dataBlockedList_.Size());

        // Make sure WorkerWorkerService is not destructed
        if (auto workerWorkerSCServicePtr = workerWorkerSCService_.lock()) {
            scStreamMetrics_->LogMetric(StreamMetric::LocalMemoryUsed,
                                        workerWorkerSCServicePtr->GetUsageMonitor().GetLocalMemoryUsed(streamName_));
        }
        auto pageQueue = GetExclusivePageQueue();
        if (pageQueue) {
            const int workAreaSize = 64;
            uint64_t workAreaMemUsed = (scStreamMetrics_->GetMetric(StreamMetric::NumLocalProducers)
                                        + scStreamMetrics_->GetMetric(StreamMetric::NumLocalConsumers))
                                       * workAreaSize;
            scStreamMetrics_->LogMetric(StreamMetric::SharedMemoryUsed,
                                        pageQueue->GetSharedMemoryUsed() + workAreaMemUsed);
            scStreamMetrics_->LogMetric(StreamMetric::NumPagesCreated, pageQueue->GetNumPagesCreated());
            scStreamMetrics_->LogMetric(StreamMetric::NumPagesReleased, pageQueue->GetNumPagesReleased());
            scStreamMetrics_->LogMetric(StreamMetric::NumPagesInUse, pageQueue->GetNumPagesInUse());
            scStreamMetrics_->LogMetric(StreamMetric::NumPagesCached, pageQueue->GetNumPagesCached());
            scStreamMetrics_->LogMetric(StreamMetric::NumBigPagesCreated, pageQueue->GetNumBigPagesCreated());
            scStreamMetrics_->LogMetric(StreamMetric::NumBigPagesReleased, pageQueue->GetNumBigPagesReleased());
        }
    }
}

void StreamManager::ClearBlockedList()
{
    std::unique_lock<std::shared_timed_mutex> xlock(streamManagerBlockedListsMutex_);
    dataBlockedList_.ClearBlockedList();
    lobBlockedList_.ClearBlockedList();
}

bool StreamManager::EnableSharedPage(StreamMode streamMode)
{
    return streamMode == StreamMode::MPSC || streamMode == StreamMode::SPSC;
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
