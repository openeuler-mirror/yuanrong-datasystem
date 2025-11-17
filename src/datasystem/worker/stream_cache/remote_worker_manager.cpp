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

#include <numeric>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/stream_posix.stub.rpc.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"
#include "datasystem/worker/stream_cache/page_queue/shared_page_queue.h"
#include "datasystem/worker/stream_cache/remote_worker_manager.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

DS_DEFINE_int32(remote_send_thread_num, 8, "The num of threads used to send elements to remote worker.");
DS_DEFINE_validator(remote_send_thread_num, &Validator::ValidateThreadNum);

namespace datasystem {
namespace worker {
namespace stream_cache {

std::string SendElementView::StreamName() const
{
    return streamName_;
}

std::string SendElementView::ProducerName() const
{
    return remoteWorker_;
}

std::string SendElementView::ProducerInstanceId() const
{
    // we only care about instance on recv side
    return "";
}

uint64_t SendElementView::StreamHash() const
{
    // We will swap the position of stream and worker address so to hash differently
    StreamProducerKey key(ProducerName(), KeyName(), ProducerInstanceId());
    return std::hash<StreamProducerKey>{}(key);
}

Status SendElementView::CreateSendElementView(const std::shared_ptr<StreamDataPage> &page,
                                              const std::string &remoteWorker, DataElement &dataElement,
                                              std::shared_ptr<PageQueueBase> obj,
                                              RemoteWorkerManager *remoteWorkerManager,
                                              std::shared_ptr<SendElementView> &out)
{
    bool isSharedPage = dataElement.GetStreamNo() != 0;
    std::shared_ptr<StreamElementView> elementView = std::make_shared<StreamElementView>();
    std::string streamName;

    if (isSharedPage) {
        streamName = "";
        Status rc = remoteWorkerManager->StreamNoToName(dataElement.GetStreamNo(), streamName);
        VLOG_IF(SC_NORMAL_LOG_LEVEL, rc.IsError()) << rc.ToString();
    } else {
        streamName = obj->GetStreamName();
    }

    elementView->page_ = page;
    elementView->streamName_ = streamName;
    elementView->begCursor_ = dataElement.id;
    elementView->remote_ = dataElement.IsRemote();
    elementView->bigElement_ = dataElement.IsBigElement();
    // Note the order of constructing elementView!! After "ExtractBigElement" is executed, "dataElement.size" will be
    // replaced with the actual size of the big element.
    if (elementView->bigElement_) {
        elementView->bigElementMetaSize_ = dataElement.size;
    }
    if (elementView->bigElement_ && !elementView->remote_) {
        RETURN_IF_NOT_OK(obj->ExtractBigElement(dataElement, elementView->bigElementPage_));
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[RW:%s, S:%s] Page<%s> Cursor %zu BigElement<%s>", remoteWorker,
                                                  elementView->streamName_, page->GetPageId(), dataElement.id,
                                                  elementView->bigElementPage_->GetPageId());
    }

    // If it is a big element, the pointer here will be the pointer to the real data of the big element.
    (void)elementView->PackDataElement(dataElement, true);
    elementView->dataObj_ = obj;

    if (isSharedPage) {
        auto sharedPageView = std::make_shared<SharedPageElementView>();
        sharedPageView->sharedPageName_ = obj->GetStreamName();
        sharedPageView->remoteWorker_ = remoteWorker;
        sharedPageView->traceId_ = Trace::Instance().GetTraceID();
        sharedPageView->dataObj_ = obj;
        sharedPageView->elementViews_.emplace_back(elementView);
        out = std::static_pointer_cast<SendElementView>(std::move(sharedPageView));
    } else {
        elementView->remoteWorker_ = remoteWorker;
        elementView->traceId_ = Trace::Instance().GetTraceID();
        out = std::move(elementView);
    }

    return Status::OK();
}

Status StreamElementView::ReleasePage()
{
    bool expected = true;
    if (ref_.compare_exchange_strong(expected, false)) {
        RETURN_IF_NOT_OK(page_->ReleasePage(FormatString("RW:%s, S:%s", remoteWorker_, StreamName())));
        if (bigElement_) {
            RETURN_IF_NOT_OK(dataObj_->DecBigElementPageRefCount(bigElementPage_->GetPageId()));
        }
    }
    return Status::OK();
}

Status StreamElementView::IncRefCount()
{
    RETURN_OK_IF_TRUE(ref_);
    bool bigElementLocked = false;
    Raii raii([&bigElementLocked, this]() {
        // Unlock in case of error.
        if (!ref_ && bigElementLocked) {
            std::string pageId = bigElementPage_->GetPageId();
            (void)dataObj_->DecBigElementPageRefCount(pageId);
        }
    });
    if (bigElement_) {
        std::string pageId = bigElementPage_->GetPageId();
        RETURN_IF_NOT_OK(dataObj_->IncBigElementPageRefCount(pageId));
        bigElementLocked = true;
    }
    RETURN_IF_NOT_OK(page_->RefPage(FormatString("RW:%s, S:%s", remoteWorker_, StreamName())));
    ref_ = true;
    return Status::OK();
}

Status StreamElementView::MoveBufToAlternateMemory()
{
    std::unique_lock<std::shared_timed_mutex> lock(mux_);
    // Check if someone beat us to do it already
    RETURN_OK_IF_TRUE(!shmEnabled_);
    // This function is only called when we hit OOM on sending. Without holding up the
    // shared memory page, we will save the data from shared memory to alternate place.
    size_t totalSize = std::accumulate(sz_.begin(), sz_.end(), 0ul);
    shmUnit_ = std::make_unique<ShmUnit>();
    // Acquire some shared memory which is already allocated from the Arena,
    // as using private memory can lead to worker getting OOMKilled.
    RETURN_IF_NOT_OK(shmUnit_->AllocateMemory(DEFAULT_TENANT_ID, totalSize, true, ServiceType::STREAM));
    secondaryAddr_ = reinterpret_cast<uint8_t *>(shmUnit_->pointer);
    RETURN_IF_NOT_OK(HugeMemoryCopy(secondaryAddr_, totalSize, buf_, totalSize));
    RETURN_IF_NOT_OK(ReleasePage());
    shmEnabled_ = false;
    localBufSize_ = totalSize;
    return Status::OK();
}

Status StreamElementView::MoveBufToShmUnit()
{
    std::unique_lock<std::shared_timed_mutex> lock(mux_);
    // Check if someone beat us to do it already
    RETURN_OK_IF_TRUE(!shmEnabled_);
    // This function is called when we hit OOM on sending or when the stream is already blocked before sending.
    // Without holding up the shared page, we will save the data from the shared page to alternate place.
    size_t totalSize = std::accumulate(sz_.begin(), sz_.end(), 0ul);
    shmUnit_ = std::make_unique<ShmUnit>();
    // Fixme: use shared page queue for the allocation,
    // deal with per stream memory limit, and also not skip retry.
    auto tenantId = TenantAuthManager::ExtractTenantId(streamName_);
    RETURN_IF_NOT_OK(shmUnit_->AllocateMemory(tenantId, totalSize, true, ServiceType::STREAM));
    secondaryAddr_ = reinterpret_cast<uint8_t *>(shmUnit_->pointer);
    RETURN_IF_NOT_OK(HugeMemoryCopy(secondaryAddr_, totalSize, buf_, totalSize));
    RETURN_IF_NOT_OK(ReleasePage());
    shmEnabled_ = false;
    localBufSize_ = totalSize;
    return Status::OK();
}

uint8_t *StreamElementView::GetBufferPointer()
{
    std::shared_lock<std::shared_timed_mutex> rlock(mux_);
    bool shmEnabled = shmEnabled_.load();
    uint8_t *ptr = shmEnabled ? buf_.load() : secondaryAddr_;
    return ptr;
}

RemoteAckInfo::AckRange StreamElementView::GetAckRange()
{
    return std::make_pair(begCursor_, sz_.size());
}

bool StreamElementView::IsSharedPage()
{
    return false;
}

bool StreamElementView::PackDataElement(const DataElement &dataElement, bool skipChecks,
                                        RemoteWorkerManager *remoteworkerManager)
{
    (void)remoteworkerManager;
    // The first element skips the checks.
    if (!skipChecks) {
        // We do not pack the next element into the element view if:
        // 1. the element view contains big element already
        // 2. the next element is big element
        // 3. the element is not with contiguous memory
        // 4. the remote field mismatches between the next element and the existing elements
        if (bigElement_ || dataElement.IsBigElement() || dataElement.ptr + dataElement.size != buf_
            || dataElement.IsRemote() != remote_) {
            return false;
        }
    }
    buf_ = dataElement.ptr;
    sz_.emplace_back(dataElement.size);
    headerBits_.emplace_back(dataElement.HasHeader());
    return true;
}

uint64_t StreamElementView::GetElementNum()
{
    return sz_.size();
}

void StreamElementView::DiscardBufferFromList(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter)
{
    auto p = GetAckRange();
    LOG(INFO) << FormatString("[S:%s] Discard range [%zu, %zu)", StreamName(), p.first, p.first + p.second);
    if (!ref_) {
        // This buffer has been released already
        iter = dataLst.erase(iter);
        return;
    }
    (void)dataObj_->UpdatePageRefIfExist(page_->GetShmView(), FormatString("S:%s", StreamName()), false);
    if (bigElement_) {
        (void)dataObj_->DecBigElementPageRefCount(bigElementPage_->GetPageId());
    }
    iter = dataLst.erase(iter);
}

std::string SharedPageElementView::KeyName() const
{
    // The name for StreamProducerKey purposes.
    // It is the unique identifier for the shared page queue instead of an actual stream name.
    return sharedPageName_;
}

bool SharedPageElementView::IsSharedPage()
{
    return true;
}

bool SharedPageElementView::PackDataElement(const DataElement &dataElement, bool skipChecks,
                                            RemoteWorkerManager *remoteWorkerManager)
{
    // For shared page scenario, elements to be packed should be from the same stream.
    // Fixme: allow different streams in a shared page stream view
    // Fixme: change to detect empty stream name
    auto isDifferentStream = [this, &dataElement, remoteWorkerManager]() {
        // stream number being 0 means shared page is not enabled for the stream, so it should be exclusive page.
        if (dataElement.GetStreamNo() == 0) {
            return false;
        }
        std::string streamNameFromNumber;
        (void)remoteWorkerManager->StreamNoToName(dataElement.GetStreamNo(), streamNameFromNumber);
        // If the stream number cannot be mapped, elements of the same nature can still be combined,
        // data will be eventually discarded.
        auto streamName = elementViews_.back()->StreamName();
        return streamNameFromNumber != streamName;
    };
    if (isDifferentStream()) {
        return false;
    }
    // Fixme: actually deal with list of element views.
    return elementViews_.back()->PackDataElement(dataElement, skipChecks);
}

uint64_t SharedPageElementView::RecordSeqNo(std::function<uint64_t(const std::string &)> fetchAddSeqNo)
{
    for (auto &view : elementViews_) {
        seqNums_.emplace_back(view->RecordSeqNo(fetchAddSeqNo));
    }
    return fetchAddSeqNo(sharedPageName_);
}

Status SharedPageElementView::ReleasePage()
{
    for (auto &view : elementViews_) {
        RETURN_IF_NOT_OK(view->ReleasePage());
    }
    return Status::OK();
}

Status SharedPageElementView::IncRefCount()
{
    for (auto &view : elementViews_) {
        RETURN_IF_NOT_OK(view->IncRefCount());
    }
    return Status::OK();
}

RemoteAckInfo::AckRange SharedPageElementView::GetAckRange()
{
    uint64_t begCursor = elementViews_.front()->begCursor_;
    return std::make_pair(begCursor, GetElementNum());
}

uint64_t SharedPageElementView::GetElementNum()
{
    uint64_t totalNum = 0;
    for (auto &view : elementViews_) {
        totalNum += view->sz_.size();
    }
    return totalNum;
}

void SharedPageElementView::DiscardBufferFromList(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter)
{
    // Fixme: actually deal with list of stream views
    elementViews_.front()->DiscardBufferFromList(dataLst, iter);
}

Status SharedPageElementView::MoveBufToShmUnit()
{
    // Fixme: actually deal with the list of the element views.
    RETURN_IF_NOT_OK(elementViews_.front()->MoveBufToShmUnit());
    return Status::OK();
}

// Class RemoteWorker part.
RemoteWorker::RemoteWorker(HostPort localAddress, HostPort remoteAddress, std::shared_ptr<AkSkManager> akSkManager,
                           ClientWorkerSCServiceImpl *scSvc, std::string &workerInstanceId,
                           std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager, RemoteWorkerManager *manager)
    : localWorkerAddr_(std::move(localAddress)),
      remoteWorkerAddr_(remoteAddress),
      akSkManager_(std::move(akSkManager)),
      scSvc_(scSvc),
      sharedPageGroup_(std::move(remoteAddress), std::move(scAllocateManager), scSvc),
      workerInstanceId_(workerInstanceId), remoteWorkerManager_(manager)
{
}

Status RemoteWorker::Init()
{
    return Status::OK();
}

RemoteWorker::~RemoteWorker()
{
    LOG(INFO) << "Start Destroy RemoteWorker for remote worker:" << remoteWorkerAddr_.ToString();
    auto pages = sharedPageGroup_.GetAllSharedPageName();
    for (auto &page : pages) {
        remoteWorkerManager_->RemoveStream(page, "");
    }
}

Status RemoteWorker::GetAccessor(const std::string &streamName, RemoteStreamInfoTbbMap::accessor &accessor)
{
    return remoteConsumers_.GetAccessor(streamName, accessor, LogPrefix());
}

Status RemoteWorker::AddRemoteConsumer(const std::string &streamName, const SubscriptionConfig &subConfig,
                                       const std::string &consumerId, uint64_t windowCount, uint64_t lastAckCursor)
{
    if (subConfig.subscriptionType != SubscriptionType::STREAM) {
        RETURN_STATUS(StatusCode::K_INVALID,
                      FormatString("Only support STREAM mode. <%s> mode not supported.", subConfig.subscriptionName));
    }
    // Register this consumer onto that remote worker, one remote worker contains a lot of related stream.
    RETURN_IF_NOT_OK_EXCEPT(remoteConsumers_.AddConsumer(streamName, consumerId, windowCount, lastAckCursor),
                            K_DUPLICATED);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s], Add remote consumer success", LogPrefix(), streamName,
                                              consumerId);
    return Status::OK();
}

Status RemoteWorker::DelRemoteConsumer(const std::string &streamName, const std::string &consumerId,
                                       Optional<bool> &mapEmpty)
{
    RETURN_IF_NOT_OK(remoteConsumers_.DeleteConsumer(streamName, consumerId, mapEmpty));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s] Delete remote consumer success", LogPrefix(),
                                              streamName, consumerId);
    return Status::OK();
}

bool RemoteWorker::HasRemoteConsumers(const std::string &streamName)
{
    return remoteConsumers_.HasRemoteConsumers(streamName);
}

bool RemoteWorker::IsStreamSendBlocked(const std::string &streamName)
{
    return remoteConsumers_.IsStreamSendBlocked(streamName);
}

uint64_t RemoteWorker::GetMaxWindowCount(const std::string &streamName) const
{
    return remoteConsumers_.GetMaxWindowCount(streamName);
}

Status RemoteWorker::DeleteStream(const std::string &streamName, Optional<bool> &mapEmpty)
{
    LOG(INFO) << FormatString("[%s] ClearAllRemoteConsumer for stream %s", LogPrefix(), streamName);
    return remoteConsumers_.DeleteStream(streamName, mapEmpty);
}

void RemoteWorker::PostRecvCleanup(const std::string &streamName, const Status &status,
                                   PendingFlushList &pendingFlushList, const PushReqPb &pushReq,
                                   const PushRspPb &pushRspPb, std::unordered_map<std::string, StreamRaii> &raii)
{
    // Here worker instance id is empty as its for sender worker
    const std::string &producerId = pushReq.producer_id();
    const std::string workerInstanceId = "";
    // TraceID of StreamProducerKey is stored inside request, set in ParseProducerPendingFlushList
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReq.trace_id());
    // Key name is equivalent to stream name in exclusive page case.
    StreamProducerKey key(streamName, producerId, workerInstanceId);
    // Iterate over elements in pendingFlushList that match to current request
    auto iter = std::find_if(pendingFlushList.begin(), pendingFlushList.end(),
                             [key](const auto &kv) { return kv.first == key; });
    if (status.GetCode() == K_SC_CONSUMER_NOT_FOUND) {
        // Discard the buffer when consumer does not exist, instead of putting to retry.
        // Note that currently one (batched) request is sent at a time,
        // so it is feasible to discard all the remaining buffers in the list.
        if (iter != pendingFlushList.end()) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << "No consumer found: Discarding buffers for stream: " << streamName;
            RemoteStreamInfoTbbMap::accessor accessor;
            if (GetAccessor(streamName, accessor).IsOk()) {
                std::for_each(iter->second.begin(), iter->second.end(), [this, &accessor](const auto &kv) {
                    auto remoteElementView = std::static_pointer_cast<StreamElementView>(kv.first);
                    auto p = remoteElementView->GetAckRange();
                    SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
                });
            }
            DiscardBuffers(iter->second);
        }
        return;
    }
    if (status.IsError() && status.GetCode() != K_OUT_OF_MEMORY) {
        RecordRemoteSendSuccess(false);  // Rpc error, all data of the request fails to be sent.
        return;
    }
    // Post cleanup
    if (iter != pendingFlushList.end()) {
        RemoteStreamInfoTbbMap::accessor accessor;
        auto rc = GetAccessor(streamName, accessor);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[%s, S:%s] Stream not found", LogPrefix(), streamName);
            return;
        }
        auto streamMgr = (*(raii.find(streamName)->second))->second;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReq.trace_id());
        PostRecvCleanup(streamName, pushReq, pushRspPb, accessor, iter->second, raii);
        accessor.release();
        // If all elements are received. Move up the last ack cursor.
        // We may also send a portion of the frames because of the window count and in which
        // case we have to check what is remaining.
        // Technically, we can also do partially ack by moving up to the first element on the list.
        LOG_IF_ERROR(streamMgr->RemoteAck(), FormatString("[%s, S:%s] Remote ack failed", LogPrefix(), streamName));
    }
}

void RemoteWorker::PostRecvCleanup(const std::string &streamName, const PushReqPb &rq, const PushRspPb &pushRspPb,
                                   RemoteStreamInfoTbbMap::accessor &accessor, std::list<BaseData> &dataLst,
                                   std::unordered_map<std::string, StreamRaii> &streamRaii)
{
    uint64_t releaseSize = 0;
    Raii raii([&releaseSize, &streamName, &streamRaii]() {
        if (releaseSize > 0) {
            auto itr = streamRaii.find(streamName);
            if (itr == streamRaii.end()) {
                LOG(ERROR) << FormatString(
                    "Decrease shared memory usage for stream[%s] failed because no worker area was found", streamName);
                return;
            }
            LOG_IF_ERROR(
                (*(itr->second))->second->TryDecUsage(releaseSize),
                FormatString("Decrease shared memory usage for stream[%s] failed during push element to remote worker",
                             streamName));
        }
    });
    for (auto i = 0; i < pushRspPb.error_size(); ++i) {
        auto &err = pushRspPb.error(i);
        auto seqNo = rq.seq(i);
        // Find the matching seqNo.
        auto it = std::find_if(dataLst.begin(), dataLst.end(), [seqNo](const auto &kv) { return kv.second == seqNo; });
        if (it == dataLst.end()) {
            LOG(ERROR) << FormatString("[%s, S:%s] Unable to find seqNo %zu", LogPrefix(), streamName, seqNo);
            continue;
        }
        auto status = Status(static_cast<StatusCode>(err.error_code()), err.error_msg());
        auto remoteElementView = std::static_pointer_cast<StreamElementView>(it->first);
        auto p = remoteElementView->GetAckRange();
        auto begCursor = p.first;
        auto endCursor = begCursor + p.second;
        if (status.IsOk()) {
            const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
            LOG_EVERY_N(INFO, logPerCount) << FormatString(
                "[%s, S:%s] Remote send elements [seq:%zu] [%zu, %zu) to remote worker %s is successful. Ref count %zu",
                LogPrefix(), streamName, seqNo, begCursor, endCursor, remoteWorkerAddr_.ToString(),
                remoteElementView.use_count());
            // Ack and release page. Same logic as in BatchFlushAsyncRead
            SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
            // If we have a reference on the page, decrement the count
            remoteElementView->ReleasePage();
            releaseSize = std::accumulate(rq.element_meta(i).element_sizes().begin(),
                                          rq.element_meta(i).element_sizes().end(), releaseSize);
            if (std::static_pointer_cast<StreamElementView>(it->first)->bigElement_) {
                releaseSize += std::static_pointer_cast<StreamElementView>(it->first)->bigElementMetaSize_;
            }
            it = dataLst.erase(it);
            // If the sending status is K_OK, the success rate is 1.
            RecordRemoteSendSuccess(true);
            continue;
        }
        // The rest is error handling
        LOG(INFO) << FormatString(
            "[%s, S:%s, I:%s] Remote send elements [seq:%zu] [%zu, %zu) to remote worker %s gave status: %s",
            LogPrefix(), streamName, workerInstanceId_, seqNo, begCursor, endCursor, remoteWorkerAddr_.ToString(),
            status.ToString());
        if (status.GetCode() != K_OUT_OF_MEMORY) {
            // If the sending status is not K_OK, the success rate is 0. Ignore OOM.
            RecordRemoteSendSuccess(false);
            continue;
        }
        Status allocRc = remoteElementView->MoveBufToAlternateMemory();
        if (allocRc.IsError()) {
            LOG(ERROR) << FormatString("[%s, S:%s] Cursor [%zu, %zu) MoveBufToAlternateMemory failed. %s", LogPrefix(),
                                       streamName, begCursor, endCursor, allocRc.ToString());
            continue;
        }
        // The function MoveBufToAlternateMemory can be called by other RemoteWorker on the same PV.
        // We need to check carefully if we need to do the ack or not.
        auto lastAckCursor = std::get<K_ACK>(accessor->second).GetStreamLastAckCursor();
        SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
        if (lastAckCursor < std::get<K_ACK>(accessor->second).GetStreamLastAckCursor()) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
                "[%s, S:%s] Cursor [%zu, %zu) MoveBufToAlternateMemory. Ref count %zu", LogPrefix(), streamName,
                begCursor, endCursor, remoteElementView.use_count());
        }
    }
}

void RemoteWorker::PostRecvCleanup(const std::string &keyName, const Status &status, PendingFlushList &pendingFlushList,
                                   const SharedPagePushReqPb &pushReq, const PushRspPb &pushRspPb,
                                   std::unordered_map<std::string, StreamRaii> &raii)
{
    (void)status;
    // Here worker instance id is empty as its for sender worker
    // Key name here is the shared page name
    const std::string &producerId = pushReq.producer_id();
    const std::string workerInstanceId = "";
    // TraceID of StreamProducerKey is stored inside request, set in ParseProducerPendingFlushList
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReq.trace_id());
    StreamProducerKey key(keyName, producerId, workerInstanceId);
    // Iterate over elements in pendingFlushList that match to current request
    auto iter = std::find_if(pendingFlushList.begin(), pendingFlushList.end(),
                             [key](const auto &kv) { return kv.first == key; });
    // Post cleanup
    if (iter != pendingFlushList.end()) {
        RemoteStreamInfoTbbMap::accessor accessor;
        auto rc = GetAccessor(keyName, accessor);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[%s, S:%s] Stream not found", LogPrefix(), keyName);
            return;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReq.trace_id());
        auto sharedPageQueue = std::static_pointer_cast<SharedPageQueue>(
            std::static_pointer_cast<SendElementView>(iter->second.front().first)->dataObj_);
        PostRecvCleanup(keyName, pushReq, pushRspPb, accessor, iter->second, raii);
        accessor.release();

        // If all elements are received. Move up the last ack cursor.
        // We may also send a portion of the frames because of the window count and in which
        // case we have to check what is remaining.
        // Technically, we can also do partially ack by moving up to the first element on the list.
        LOG_IF_ERROR(sharedPageQueue->RemoteAck(), FormatString("[%s, S:%s] Remote ack failed", LogPrefix(), keyName));
    }
}

void RemoteWorker::PostRecvCleanup(const std::string &keyName, const SharedPagePushReqPb &rq,
                                   const PushRspPb &pushRspPb, RemoteStreamInfoTbbMap::accessor &accessor,
                                   std::list<BaseData> &dataLst, std::unordered_map<std::string, StreamRaii> &raii)
{
    std::unordered_map<std::string, uint64_t> streamName2BytesNumSendSuccess;
    for (auto i = 0; i < pushRspPb.error_size(); ++i) {
        auto &err = pushRspPb.error(i);
        auto seqNo = rq.metas(i).seq();
        // Find the matching seqNo and also matching stream name.
        // Note that seqNo in shared page case is at actual stream name granularity.
        const std::string &streamName = rq.stream_names(rq.metas(i).stream_index());
        auto it = std::find_if(dataLst.begin(), dataLst.end(), [seqNo, &streamName](const auto &kv) {
            auto sharedPageView = std::static_pointer_cast<SharedPageElementView>(kv.first);
            for (auto seqIter = sharedPageView->seqNums_.begin(), viewIter = sharedPageView->elementViews_.begin();
                 seqIter != sharedPageView->seqNums_.end(); seqIter++, viewIter++) {
                if (*seqIter == seqNo && (*viewIter)->StreamName() == streamName) {
                    return true;
                }
            }
            return false;
        });
        if (it == dataLst.end()) {
            LOG(ERROR) << FormatString("[%s, S:%s] Unable to find seqNo %zu", LogPrefix(), keyName, seqNo);
            continue;
        }
        auto status = Status(static_cast<StatusCode>(err.error_code()), err.error_msg());
        auto remoteElementView = std::static_pointer_cast<SharedPageElementView>(it->first);
        auto p = remoteElementView->GetAckRange();
        auto begCursor = p.first;
        auto endCursor = begCursor + p.second;
        if (status.IsOk()) {
            const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
            LOG_EVERY_N(INFO, logPerCount) << FormatString(
                "[%s, S:%s] Remote send elements [seq:%zu] [%zu, %zu) to remote worker %s is successful. Ref count %zu",
                LogPrefix(), keyName, seqNo, begCursor, endCursor, remoteWorkerAddr_.ToString(),
                remoteElementView.use_count());
            streamName2BytesNumSendSuccess[streamName] = std::accumulate(
                rq.metas(i).element_meta().element_sizes().begin(), rq.metas(i).element_meta().element_sizes().end(),
                streamName2BytesNumSendSuccess[streamName]);
            if (remoteElementView->elementViews_.back()->bigElement_) {
                streamName2BytesNumSendSuccess[streamName] +=
                    remoteElementView->elementViews_.back()->bigElementMetaSize_;
            }
            // Ack and release page. Same logic as in BatchFlushAsyncRead
            SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
            // If we have a reference on the page, decrement the count
            remoteElementView->ReleasePage();
            it = dataLst.erase(it);
            // If the sending status is K_OK, the success rate is 1.
            RecordRemoteSendSuccess(true);
            continue;
        }
        // The rest is error handling
        LOG(INFO) << FormatString(
            "[%s, S:%s, I:%s] Remote send elements [seq:%zu] [%zu, %zu) to remote worker %s gave status: %s",
            LogPrefix(), keyName, workerInstanceId_, seqNo, begCursor, endCursor, remoteWorkerAddr_.ToString(),
            status.ToString());
        if (status.GetCode() == K_SC_CONSUMER_NOT_FOUND) {
            // Discard the buffer when consumer does not exist, instead of putting to retry.
            // Note that in shared page scenario, we can only discard the current buffer,
            // as the other buffers can be from different streams.
            VLOG(SC_INTERNAL_LOG_LEVEL) << "No consumer found: Discarding one buffer from shared page: " << keyName;
            SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
            DiscardBufferHelper(dataLst, it);
        }
        if (status.GetCode() != K_OUT_OF_MEMORY) {
            // If the sending status is not K_OK, the success rate is 0. Ignore OOM.
            RecordRemoteSendSuccess(false);
            continue;
        }
        Status allocRc = remoteElementView->MoveBufToShmUnit();
        if (allocRc.IsError()) {
            LOG(ERROR) << FormatString("[%s, S:%s] Cursor [%zu, %zu) MoveBufToShmUnit failed. %s", LogPrefix(),
                                       streamName, begCursor, endCursor, allocRc.ToString());
            continue;
        }
        // The function MoveBufToShmUnit is for shared page scenario, currently it only supports single consumer.
        // We allow ack so then the page would not be occupied in back-pressure case.
        SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
    }
    for (const auto &kv : streamName2BytesNumSendSuccess) {
        auto itr = raii.find(kv.first);
        if (itr == raii.end()) {
            LOG(ERROR) << FormatString(
                "Decrease shared memory usage for stream[%s] failed because no worker area was found", kv.first);
            continue;
        }
        LOG_IF_ERROR(
            (*(itr->second))->second->TryDecUsage(kv.second),
            FormatString("Decrease shared memory usage for stream[%s] failed during push element to remote worker",
                         kv.first));
    }
}

void RemoteWorker::DiscardBuffers(std::list<BaseData> &dataLst)
{
    // Discard all the buffers.
    auto iter = dataLst.begin();
    while (iter != dataLst.end()) {
        DiscardBufferHelper(dataLst, iter);
    }
    dataLst.clear();
}

void RemoteWorker::DiscardBufferHelper(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter)
{
    auto remoteElementView = std::static_pointer_cast<SendElementView>(iter->first);
    remoteElementView->DiscardBufferFromList(dataLst, iter);
}

Status RemoteWorker::ParseProducerPendingFlushList(const std::string &streamName, const std::string &producerId,
                                                   std::list<BaseData> &dataLst, std::vector<PushReq> &requests,
                                                   std::vector<std::vector<MemView>> &payloads,
                                                   std::unordered_map<std::string, StreamRaii> &raii,
                                                   std::list<std::shared_ptr<SharedPageElementView>> &moveList,
                                                   std::unordered_set<std::shared_ptr<SharedPageQueue>> &needAckList)
{
    RETURN_OK_IF_TRUE(dataLst.empty());
    uint64_t firstSeqNo = dataLst.begin()->second;
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("Processing pending send Flush List [%s, %s, %zu]", streamName,
                                                producerId, firstSeqNo);
    // Try not to send too many rpc that the worker can't handle
    uint64_t windowCount = GetMaxWindowCount(streamName);
    auto firstCursor = std::numeric_limits<uint64_t>::max();
    auto lastCursor = std::numeric_limits<uint64_t>::min();
    auto it = dataLst.begin();
    bool sharedPage = std::static_pointer_cast<StreamElementView>(it->first)->IsSharedPage();
    // back up shared page queue in case all elements are to be discarded.
    std::shared_ptr<SharedPageQueue> backupSharedPage =
        sharedPage ? std::static_pointer_cast<SharedPageQueue>(
            std::static_pointer_cast<StreamElementView>(it->first)->dataObj_)
                   : nullptr;
    bool needAck = true;
    while (it != dataLst.end() && windowCount-- > 0) {
        std::variant<PushReqPb, SharedPagePushReqPb> pushReq;
        std::vector<MemView> elements;
        if (!sharedPage) {
            PushReqPb pushReqPb;
            RETURN_IF_NOT_OK(FillExclusivePushReqHelper(streamName, producerId, firstSeqNo, dataLst, it, firstCursor,
                                                        lastCursor, pushReqPb, elements, raii));
            pushReq = std::move(pushReqPb);
        } else {
            SharedPagePushReqPb pushReqPb;
            Status rc = FillSharedPushReqHelper(producerId, dataLst, it, firstCursor, lastCursor, pushReqPb, elements,
                                                raii, moveList);
            if (rc.GetCode() == K_SC_STREAM_NOT_FOUND) {
                RemoteStreamInfoTbbMap::accessor accessor;
                if (GetAccessor(streamName, accessor).IsOk()) {
                    auto remoteElementView = std::static_pointer_cast<StreamElementView>(it->first);
                    auto p = remoteElementView->GetAckRange();
                    SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
                }
                DiscardBufferHelper(dataLst, it);
                continue;
            } else if (rc.GetCode() == K_NOT_READY) {
                // Request is not ready because element views are all skipped.
                // Note that this code should be internal to this function, it should not propagate further down.
                continue;
            }
            RETURN_IF_NOT_OK(rc);
            pushReq = std::move(pushReqPb);
        }
        requests.emplace_back(std::move(pushReq), streamName);
        payloads.push_back(std::move(elements));
        needAck = false;
    }
    // If elements are to be sent to remote, there is no need to ack up here.
    // Otherwise if elements are skipped due to blocking in shared page case,
    // or if elements are being discarded,
    // we need to perform ack so that elements do not occupy twice the shm.
    if (needAck) {
        needAckList.emplace(backupSharedPage);
    }
    // All cursors before firstSeqNo has been reclaimed by the remote worker. The next potential
    // one is the last element on the list.
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, S:%s] Ack range [%zu, %zu]. Window size %zu", LogPrefix(),
                                                streamName, firstCursor, lastCursor, requests.size());
    return Status::OK();
}

Status RemoteWorker::FillExclusivePushReqHelper(const std::string &streamName, const std::string &producerId,
                                                uint64_t firstSeqNo, std::list<BaseData> &dataLst,
                                                std::list<BaseData>::iterator &it, uint64_t &firstCursor,
                                                uint64_t &lastCursor, PushReqPb &pushReqPb,
                                                std::vector<MemView> &elements,
                                                std::unordered_map<std::string, StreamRaii> &raii)
{
    RETURN_IF_NOT_OK(LockStreamManagerHelper(streamName, raii));
    pushReqPb.set_stream_name(streamName);
    pushReqPb.set_producer_id(producerId);
    pushReqPb.set_worker_addr(localWorkerAddr_.ToString());
    pushReqPb.set_first_seq(firstSeqNo);
    pushReqPb.set_worker_instance_id(workerInstanceId_);
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(it->first->traceId_);
    pushReqPb.set_trace_id(Trace::Instance().GetTraceID());
    size_t chunkSz = 0;
    const size_t zmqChunkSz = static_cast<size_t>(FLAGS_zmq_chunk_sz);
    // Only batch up to FLAGS_zmq_chunk_sz. Make sure we send at least one PV
    do {
        auto seqNo = it->second;
        auto streamElementView = std::static_pointer_cast<StreamElementView>(it->first);
        auto &eleSzs = streamElementView->sz_;
        size_t payloadSz = std::accumulate(eleSzs.begin(), eleSzs.end(), 0ul);
        if ((chunkSz > 0 && ((payloadSz > zmqChunkSz) || chunkSz > zmqChunkSz - payloadSz))) {
            break;
        }
        chunkSz += payloadSz;
        auto *ele = pushReqPb.mutable_element_meta()->Add();
        ele->mutable_element_sizes()->Add(eleSzs.begin(), eleSzs.end());
        auto &headerBits = streamElementView->headerBits_;
        ele->mutable_header_bits()->Add(headerBits.begin(), headerBits.end());
        pushReqPb.mutable_seq()->Add(seqNo);
        elements.emplace_back(streamElementView->GetBufferPointer(), payloadSz);
        const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
        LOG_EVERY_N(INFO, logPerCount) << FormatString(
            "[%s, S:%s, I:%s] Remote send elements [seq:%zu] [%zu, %zu) to remote worker %s, page: %s", LogPrefix(),
            streamName, workerInstanceId_, seqNo, streamElementView->begCursor_,
            streamElementView->begCursor_ + streamElementView->sz_.size(), remoteWorkerAddr_.ToString(),
            streamElementView->page_->GetPageId());
        firstCursor = std::min(firstCursor, streamElementView->begCursor_);
        lastCursor = std::max(lastCursor, streamElementView->begCursor_ + streamElementView->sz_.size() - 1);
        ++it;
    } while (it != dataLst.end());
    pushReqPb.set_chunk_size(chunkSz);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(pushReqPb));
    return Status::OK();
}

Status RemoteWorker::FillSharedPushReqHelper(const std::string &producerId, std::list<BaseData> &dataLst,
                                             std::list<BaseData>::iterator &it, uint64_t &firstCursor,
                                             uint64_t &lastCursor, SharedPagePushReqPb &pushReqPb,
                                             std::vector<MemView> &elements,
                                             std::unordered_map<std::string, StreamRaii> &raii,
                                             std::list<std::shared_ptr<SharedPageElementView>> &moveList)
{
    std::unordered_map<std::string, uint64_t> streamIndexMapping;
    std::unordered_map<std::string, bool> streamBlockInfoMap;
    pushReqPb.set_producer_id(producerId);
    pushReqPb.set_worker_addr(localWorkerAddr_.ToString());
    pushReqPb.set_worker_instance_id(workerInstanceId_);
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(it->first->traceId_);
    pushReqPb.set_trace_id(Trace::Instance().GetTraceID());
    bool requestReady = false;
    size_t chunkSz = 0;
    const size_t zmqChunkSz = static_cast<size_t>(FLAGS_zmq_chunk_sz);
    // Only batch up to FLAGS_zmq_chunk_sz. Make sure we send at least one PV
    do {
        // Fixme: actually deal with list of element views.
        auto sharedPageElementView = std::static_pointer_cast<SharedPageElementView>(it->first);
        auto streamElementView = sharedPageElementView->elementViews_.front();
        auto seqNo = sharedPageElementView->seqNums_.front();
        const std::string &streamName = streamElementView->streamName_;
        RETURN_IF_NOT_OK(LockStreamManagerHelper(streamName, raii));
        auto &eleSzs = streamElementView->sz_;
        size_t payloadSz = std::accumulate(eleSzs.begin(), eleSzs.end(), 0ul);
        // Skip if blocked. Record the stream names so we get consistent results in this loop.
        auto streamBlockInfo = streamBlockInfoMap.find(streamName);
        if (streamBlockInfo == streamBlockInfoMap.end()) {
            streamBlockInfo = streamBlockInfoMap.emplace(streamName, IsStreamSendBlocked(streamName)).first;
        }
        if (streamBlockInfo->second) {
            ++it;
            // delay the move to after the requests are sent.
            moveList.emplace_back(sharedPageElementView);
            continue;
        }
        auto iter = streamIndexMapping.find(streamName);
        if (iter == streamIndexMapping.end()) {
            iter = streamIndexMapping.emplace(streamName, streamIndexMapping.size()).first;
            pushReqPb.mutable_stream_names()->Add(streamName.c_str());
        }
        if ((chunkSz > 0 && ((payloadSz > zmqChunkSz) || (chunkSz + payloadSz) > zmqChunkSz))) {
            break;
        }
        chunkSz += payloadSz;
        // Fill in StreamElementsMetaPb with stream index, sequence number and the actual view meta.
        auto *meta = pushReqPb.mutable_metas()->Add();
        meta->set_stream_index(iter->second);
        meta->set_seq(seqNo);
        auto *ele = meta->mutable_element_meta();
        ele->mutable_element_sizes()->Add(eleSzs.begin(), eleSzs.end());
        auto &headerBits = streamElementView->headerBits_;
        ele->mutable_header_bits()->Add(headerBits.begin(), headerBits.end());
        elements.emplace_back(streamElementView->GetBufferPointer(), payloadSz);
        const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
        LOG_EVERY_N(INFO, logPerCount) << FormatString(
            "[%s, S:%s, I:%s] Remote send elements [seq:%zu] [%zu, %zu) to remote worker %s, page: %s", LogPrefix(),
            streamName, workerInstanceId_, seqNo, streamElementView->begCursor_,
            streamElementView->begCursor_ + streamElementView->sz_.size(), remoteWorkerAddr_.ToString(),
            streamElementView->page_->GetPageId());
        firstCursor = std::min(firstCursor, streamElementView->begCursor_);
        lastCursor = std::max(lastCursor, streamElementView->begCursor_ + streamElementView->sz_.size() - 1);
        ++it;
        // If all views are skipped due to blocking, then request is not prepared.
        requestReady = true;
    } while (it != dataLst.end());
    CHECK_FAIL_RETURN_STATUS(requestReady, K_NOT_READY, "All element views are skipped, request is not ready");
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(pushReqPb));
    return Status::OK();
}

Status RemoteWorker::LockStreamManagerHelper(const std::string &streamName,
                                             std::unordered_map<std::string, StreamRaii> &raii)
{
    if (raii.find(streamName) == raii.end()) {
        // We can't allow the stream to be deleted while we are traversing the shared memory.
        StreamRaii rlock = std::make_unique<StreamManagerMap::const_accessor>();
        RETURN_IF_NOT_OK(scSvc_->GetStreamManager(streamName, *rlock));
        std::shared_ptr<StreamManager> streamMgr = (*rlock)->second;
        // Check the state (delete, reset, etc)
        RETURN_IF_NOT_OK(streamMgr->CheckIfStreamActive());
        raii.emplace(streamName, std::move(rlock));
    }
    return Status::OK();
}

Status RemoteWorker::ProcessEndOfStream(const std::shared_ptr<StreamManager> &streamMgr, std::list<BaseData> dataLst,
                                        const std::string &streamName, const std::string &producerId)
{
    (void)streamMgr;
    (void)producerId;
    // Move up the ack. We aren't going to send them.
    RemoteStreamInfoTbbMap::accessor accessor;
    Status rc = GetAccessor(streamName, accessor);
    if (rc.IsOk()) {
        std::get<K_ACK>(accessor->second).Reset();
        accessor.release();
    }
    // Discard all the buffers.
    DiscardBuffers(dataLst);
    // If the stream is blocked, unblock it.
    RETURN_IF_NOT_OK_EXCEPT(remoteConsumers_.ToggleStreamBlocking(streamName, false), K_SC_STREAM_NOT_FOUND);
    // Signal this job is done.
    return Status::OK();
}

Status RemoteWorker::ParsePendingFlushList(const PendingFlushList &pendingFlushList, std::vector<PushReq> &requests,
                                           std::vector<std::vector<MemView>> &payloads,
                                           std::unordered_map<std::string, StreamRaii> &raii,
                                           std::list<std::shared_ptr<SharedPageElementView>> &moveList,
                                           std::unordered_set<std::shared_ptr<SharedPageQueue>> &needAckList)
{
    for (const auto &ele : pendingFlushList) {
        const StreamProducerKey key = ele.first;
        std::list<BaseData> &dataLst = ele.second;
        const std::string &streamName = key.firstKey_;
        const std::string &producerId = key.producerId_;
        if (IsStreamSendBlocked(streamName)) {
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Ignore stream %s producer %s", LogPrefix(), streamName,
                                                      producerId);
            continue;
        }
        if (dataLst.empty()) {
            continue;
        }
        Status rc = ParseProducerPendingFlushList(streamName, producerId, dataLst, requests, payloads, raii, moveList,
                                                  needAckList);
        if (rc.GetCode() == K_SC_STREAM_NOT_FOUND || rc.GetCode() == K_SC_STREAM_DELETE_IN_PROGRESS
            || rc.GetCode() == K_SC_STREAM_IN_RESET_STATE) {
            continue;
        }
        RETURN_IF_NOT_OK(rc);
    }
    return Status::OK();
}

int RemoteWorker::BatchFlushAsyncWrite(const std::shared_ptr<WorkerWorkerSCService_Stub> &stub,
                                       std::vector<PushReq> &requests, std::vector<std::vector<MemView>> &payloads)
{
    int numReqSent = 0;
    for (size_t i = 0; i < requests.size(); ++i) {
        Status &status = requests.at(i).rc_;
        auto &pushReq = requests.at(i).req_;
        const auto visitor = [&](auto &&pushReqPb) {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReqPb.trace_id());
            if constexpr (std::is_same_v<std::decay_t<decltype(pushReqPb)>, PushReqPb>) {
                VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("Calling PushElementsCursorsAsyncWrite for %s with %zu PV",
                                                         pushReqPb.stream_name(), pushReqPb.element_meta_size());
                status = stub->PushElementsCursorsAsyncWrite(pushReqPb, requests.at(i).tag_, payloads.at(i));
            } else {
                VLOG(SC_DEBUG_LOG_LEVEL) << FormatString(
                    "Calling PushSharedPageCursorsAsyncWrite for shared page with %zu PV", pushReqPb.metas_size());
                status = stub->PushSharedPageCursorsAsyncWrite(pushReqPb, requests.at(i).tag_, payloads.at(i));
            }
        };

        PerfPoint point(PerfKey::REMOTE_WORKER_SEND_ONE_STREAM);
        std::visit(visitor, pushReq);
        // Need to count AsyncWrite even if the AsyncRead returns K_TRY_AGAIN
        numReqSent++;
    }
    VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("Number of outstanding PushElementsCursorsAsyncWrite request %d",
                                             numReqSent);
    return numReqSent;
}

void RemoteWorker::BatchFlushAsyncRead(const std::shared_ptr<WorkerWorkerSCService_Stub> &stub,
                                       PendingFlushList &pendingFlushList, std::vector<PushReq> &requests,
                                       std::unordered_map<std::string, StreamRaii> &raii)
{
    size_t numAsync = requests.size();
    for (size_t i = 0; i < numAsync; ++i) {
        Status &status = requests.at(i).rc_;
        // Check the return code from PushElementsCursorsAsyncWrite
        if (status.IsError()) {
            continue;
        }
        const auto visitor = [&](auto &&pushReq) {
            // TraceID of StreamProducerKey is stored inside request, set in ParseProducerPendingFlushList
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(pushReq.trace_id());
            PushRspPb pushRspPb;
            PerfPoint point(PerfKey::REMOTE_WORKER_MAIN_RECV);
            INJECT_POINT("RemoteWorker.SleepBeforeAsyncRead", [](uint64_t timeoutMs) -> void {
                std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
                return;
            });
            if constexpr (std::is_same_v<std::decay_t<decltype(pushReq)>, PushReqPb>) {
                status = stub->PushElementsCursorsAsyncRead(requests.at(i).tag_, pushRspPb, RpcRecvFlags::NONE);
            } else {
                status = stub->PushSharedPageCursorsAsyncRead(requests.at(i).tag_, pushRspPb, RpcRecvFlags::NONE);
            }
            point.Record();
            INJECT_POINT_NO_RETURN("RemoteWorker.BatchFlushAsyncRead.rpc.timeout", [&status]() {
                status = { K_RPC_UNAVAILABLE, "Fake worker not responding" };
            });
            VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("PushElementsCursorsAsyncRead rc for stream %s: %s",
                                                     requests.at(i).keyName_, status.ToString());
            PostRecvCleanup(requests.at(i).keyName_, status, pendingFlushList, pushReq, pushRspPb, raii);
        };
        std::visit(visitor, requests.at(i).req_);
    }
}

void RemoteWorker::HandleBlockedElements(std::list<std::shared_ptr<SharedPageElementView>> &moveList,
                                         std::unordered_set<std::shared_ptr<SharedPageQueue>> &needAckList)
{
    std::unordered_set<std::string> oomList;
    for (auto &sharedPageElementView : moveList) {
        // Avoid occupying the shared page when stream is blocked.
        // Move buf to shm when we are about to batch and send to remote.
        // And then allow page to be acked.
        const auto &keyName = sharedPageElementView->KeyName();
        // If we previously got OOM, skip it and continue to the next stream.
        // 1. we want to preserve the order so that we are more likely to ack pages in order
        // 2. it is likely that OOM will still happen at least for the same stream, so skip the stream to save the time.
        if (oomList.find(keyName) != oomList.end()) {
            continue;
        }
        // Fixme: Actually deal with partial of the StreamElementView that needs to be moved.
        Status allocRc = sharedPageElementView->MoveBufToShmUnit();
        if (allocRc.IsError()) {
            auto p = sharedPageElementView->GetAckRange();
            LOG(WARNING) << FormatString("[%s, S:%s] Cursor [%zu, %zu) MoveBufToShmUnit failed. %s", LogPrefix(),
                                         sharedPageElementView->streamName_, p.first, p.first + p.second,
                                         allocRc.ToString());
            if (allocRc.GetCode() == K_OUT_OF_MEMORY) {
                oomList.emplace(keyName);
            }
            continue;
        }
        RemoteStreamInfoTbbMap::accessor accessor;
        if (GetAccessor(keyName, accessor).IsOk()) {
            auto p = sharedPageElementView->GetAckRange();
            SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
        }
    }
    for (auto &sharedPageQueue : needAckList) {
        LOG_IF_ERROR(sharedPageQueue->RemoteAck(),
                     FormatString("[%s, S:%s] Remote ack failed", LogPrefix(), sharedPageQueue->GetPageQueueId()));
    }
}

Status RemoteWorker::BatchAsyncFlushEntry(PendingFlushList &pendingFlushList)
{
    INJECT_POINT("RemoteWorker.BatchAsyncFlushEntry.Sleep", [](int sleepSecond) {
        std::this_thread::sleep_for(std::chrono::seconds(sleepSecond));
        return Status::OK();
    });
    std::vector<PushReq> requests;
    std::vector<std::vector<MemView>> payloads;
    std::unordered_map<std::string, StreamRaii> raii;  // holds all the const_accessor
    std::list<std::shared_ptr<SharedPageElementView>> moveList;
    std::unordered_set<std::shared_ptr<SharedPageQueue>> needAckList;
    RETURN_IF_NOT_OK(ParsePendingFlushList(pendingFlushList, requests, payloads, raii, moveList, needAckList));
    std::shared_ptr<RpcStubBase> stub;
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().GetStub(remoteWorkerAddr_, StubType::WORKER_WORKER_SC_SVC, stub));
    auto derivedStub = std::dynamic_pointer_cast<WorkerWorkerSCService_Stub>(stub);
    RETURN_RUNTIME_ERROR_IF_NULL(derivedStub);
    // This code is driven by BufferPool async flush code path which will retry on error.
    auto numRequestSent = BatchFlushAsyncWrite(derivedStub, requests, payloads);
    // Handle the blocked elements in between async write and read.
    HandleBlockedElements(moveList, needAckList);
    // If nothing is sent out and there is no need to continue.
    RETURN_OK_IF_TRUE(numRequestSent == 0);
    BatchFlushAsyncRead(derivedStub, pendingFlushList, requests, raii);
    // Return the first non-ok error
    for (auto &req : requests) {
        if (req.rc_.IsError()) {
            return req.rc_;
        }
    }
    return Status::OK();
}

Status RemoteWorker::GetStreamLastAckCursor(const std::string &streamName, uint64_t &cursor)
{
    RETURN_IF_NOT_OK(remoteConsumers_.GetStreamLastAckCursor(streamName, cursor));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] remoteAck = %zu", LogPrefix(), streamName, cursor);
    return Status::OK();
}

void RemoteWorker::SyncStreamLastAckCursor(RemoteStreamInfoTbbMap::accessor &accessor,
                                           Optional<RemoteAckInfo::AckRange> ackRange)
{
    remoteConsumers_.SyncStreamLastAckCursor(accessor, ackRange,
                                             FormatString("%s, S:%s", LogPrefix(), accessor->first));
}

std::string RemoteWorker::LogPrefix() const
{
    return FormatString("RW:%s", remoteWorkerAddr_.ToString());
}

bool RemoteWorker::ExistsRemoteConsumer()
{
    return !remoteConsumers_.Empty();
}

void RemoteWorker::GetOrCreateSharedPageQueue(const std::string &namespaceUri,
                                              std::shared_ptr<SharedPageQueue> &pageQueue)
{
    sharedPageGroup_.GetOrCreateSharedPageQueue(namespaceUri, pageQueue);
}

// Class RemoteWorkerManager part
RemoteWorkerManager::RemoteWorkerManager(ClientWorkerSCServiceImpl *scSvc, std::shared_ptr<AkSkManager> akSkManager,
                                         std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager)
    : akSkManager_(std::move(akSkManager)), scSvc_(scSvc), scAllocateManager_(scAllocateManager)
{
}

RemoteWorkerManager::~RemoteWorkerManager()
{
    // The remoteWorkerDict_ keeps the pointer of RemoteWorkerManager, it needs to be cleared first.
    remoteWorkerDict_.clear();
    if (dataMap_) {
        dataMap_->Stop();
    }
}

Status RemoteWorkerManager::Init()
{
    // for remote send
    dataMap_ = std::make_unique<BufferPool>(
        FLAGS_remote_send_thread_num, "ScPushToRemote",
        std::bind(&RemoteWorkerManager::BatchAsyncFlushEntry, this, std::placeholders::_1, std::placeholders::_2));
    RETURN_IF_NOT_OK(dataMap_->Init());
    // for scan
    dataPool_ = std::make_unique<StreamDataPool>();
    RETURN_IF_NOT_OK(dataPool_->Init());
    // An unique id will be generated on each restart
    workerInstanceId_ = GetStringUuid();
    return Status::OK();
}

Status RemoteWorkerManager::GetRemoteWorker(const std::string &address, std::shared_ptr<RemoteWorker> &remoteWorker)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto iter = remoteWorkerDict_.find(address);
    CHECK_FAIL_RETURN_STATUS(iter != remoteWorkerDict_.end(), StatusCode::K_NOT_FOUND,
                             FormatString("Remote worker:<%s> does not exist", address));
    RETURN_RUNTIME_ERROR_IF_NULL(iter->second);
    remoteWorker = iter->second;
    return Status::OK();
}

uint64_t RemoteWorkerManager::GetLastAckCursor(const std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (remoteWorkerDict_.empty()) {
        return 0;
    }
    uint64_t lastAckCursor = std::numeric_limits<uint64_t>::max();
    for (auto &ele : remoteWorkerDict_) {
        auto &remoteWorker = ele.second;
        uint64_t cursor;
        Status rc = remoteWorker->GetStreamLastAckCursor(streamName, cursor);
        if (rc.IsOk()) {
            lastAckCursor = std::min<uint64_t>(lastAckCursor, cursor);
        }
    }
    const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
    LOG_EVERY_N(INFO, logPerCount) << FormatString("[S:%s] Remote consumer(s) lastAckCursor = %zu", streamName,
                                                   lastAckCursor);
    return lastAckCursor;
}

void RemoteWorkerManager::RemoveStream(const std::string &keyName, const std::string &sharedPageName)
{
    dataMap_->RemoveStream(keyName, sharedPageName);
}

void RemoteWorkerManager::PurgeBuffer(const std::shared_ptr<StreamManager> &streamMgr)
{
    dataMap_->PurgeBuffer(streamMgr->GetStreamName(),
                          std::bind(&RemoteWorkerManager::ProcessEndOfStream, this, streamMgr, std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3));
}

Status RemoteWorkerManager::ProcessEndOfStream(const std::shared_ptr<StreamManager> &streamMgr,
                                               std::list<BaseData> dataLst, const std::string &streamName,
                                               const std::string &producerId)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    RETURN_OK_IF_TRUE(remoteWorkerDict_.empty());
    std::vector<Status> status(remoteWorkerDict_.size());
    size_t i = 0;
    auto iter = remoteWorkerDict_.begin();
    while (iter != remoteWorkerDict_.end()) {
        auto rw = iter->second;
        status.at(i) = rw->ProcessEndOfStream(streamMgr, dataLst, streamName, producerId);
        ++iter;
        ++i;
    }
    auto rc = std::find_if(status.begin(), status.end(), [](auto &kv) { return kv.IsError(); });
    if (rc != status.end()) {
        return (*rc);
    }
    return Status::OK();
}

Status RemoteWorkerManager::StreamNoToName(uint64_t streamNo, std::string &streamName)
{
    return scSvc_->StreamNoToName(streamNo, streamName);
}

Status RemoteWorkerManager::SendElementsView(const std::shared_ptr<SendElementView> &eleView)
{
    const std::string &streamName = eleView->StreamName();
    auto &remoteWorker = eleView->remoteWorker_;
    std::shared_ptr<RemoteWorker> rw;
    if (!eleView->remote_) {
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[RW:%s, S:%s] Flush %zu elements remotely", remoteWorker,
                                                    streamName, eleView->GetElementNum());
        RETURN_IF_NOT_OK(eleView->IncRefCount());
        INJECT_POINT("RemoteWorkerManager.SendElementsView.PostIncRefCount");
        dataMap_->Insert(eleView);
    } else {
        // We don't send remote elements back to remote workers, or we will run into infinite loop.
        // But we need to keep track of the gap and move up the ack cursor accordingly.
        RETURN_IF_NOT_OK(GetRemoteWorker(remoteWorker, rw));
        RemoteStreamInfoTbbMap::accessor accessor;
        RETURN_IF_NOT_OK(rw->GetAccessor(streamName, accessor));
        RemoteAckInfo::AckRange p = eleView->GetAckRange();
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s, S:%s] Ack cursor [%zu, %zu)", rw->LogPrefix(), streamName,
                                                    p.first, p.first + p.second);
        // Move up the ack if necessary
        rw->SyncStreamLastAckCursor(accessor, Optional<RemoteAckInfo::AckRange>(p));
    }
    return Status::OK();
}

Status RemoteWorkerManager::BatchAsyncFlushEntry(int myId, const PendingFlushList &pendingFlushList)
{
    (void)myId;
    auto traceGuard = Trace::Instance().SetTraceUUID();
    std::unordered_map<std::string, std::pair<std::shared_ptr<RemoteWorker>, PendingFlushList>> flushMap;
    for (const auto &ele : pendingFlushList) {
        const StreamProducerKey key = ele.first;
        std::list<BaseData> &dataLst = ele.second;
        const std::string &streamName = key.firstKey_;
        const std::string &remoteWorker = key.producerId_;
        auto it = flushMap.find(remoteWorker);
        if (it == flushMap.end()) {
            std::shared_lock<std::shared_timed_mutex> lock(mutex_);
            auto iter = remoteWorkerDict_.find(remoteWorker);
            if (iter == remoteWorkerDict_.end()) {
                // Discard all the buffers.
                RemoteWorker::DiscardBuffers(dataLst);
                continue;
            }
            it = flushMap.emplace(remoteWorker, std::make_pair(iter->second, PendingFlushList())).first;
        }
        auto &rw = it->second.first;
        // Check again if we still have remote consumer.
        if (rw->HasRemoteConsumers(streamName)) {
            it->second.second.push_back(ele);
        } else {
            RemoteWorker::DiscardBuffers(dataLst);
            continue;
        }
    }
    RETURN_OK_IF_TRUE(flushMap.empty());
    std::vector<Status> status(flushMap.size());
    size_t i = 0;
    auto it = flushMap.begin();
    while (it != flushMap.end()) {
        auto &rw = it->second.first;
        status.at(i) = rw->BatchAsyncFlushEntry(it->second.second);
        ++it;
        ++i;
    }
    auto rc = std::find_if(status.begin(), status.end(), [](auto &kv) { return kv.IsError(); });
    if (rc != status.end()) {
        return (*rc);
    }
    return Status::OK();
}

bool RemoteWorkerManager::HasRemoteConsumers(const std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> rlock(mutex_);
    return std::any_of(remoteWorkerDict_.begin(), remoteWorkerDict_.end(),
                       [&streamName](const auto &kv) { return kv.second->HasRemoteConsumers(streamName); });
}

Status RemoteWorkerManager::DeleteStream(const std::string &streamName)
{
    // Stop to push new buffer into RW
    RETURN_IF_NOT_OK_EXCEPT(dataPool_->RemoveStreamObject(streamName, {}), K_SC_STREAM_NOT_FOUND);
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto iter = remoteWorkerDict_.begin();
    while (iter != remoteWorkerDict_.end()) {
        RETURN_RUNTIME_ERROR_IF_NULL(iter->second);
        auto &rw = iter->second;
        Optional<bool> mapEmpty(false);
        RETURN_IF_NOT_OK_EXCEPT(rw->DeleteStream(streamName, mapEmpty), K_SC_STREAM_NOT_FOUND);
        if (mapEmpty.value()) {
            LOG(INFO) << "Erase remote worker " << rw->remoteWorkerAddr_.ToString() << " from remoteWorkerDict_";
            iter = remoteWorkerDict_.erase(iter);
        } else {
            ++iter;
        }
    }
    return Status::OK();
}

Status RemoteWorkerManager::DoneScanning(const std::string &streamName)
{
    RETURN_IF_NOT_OK_EXCEPT(dataPool_->RemoveStreamObject(streamName, {}), K_SC_STREAM_NOT_FOUND);
    return Status::OK();
}

std::string RemoteWorkerManager::GetSCRemoteSendSuccessRate()
{
    return remoteSendRateVec_.BlockingGetRateToStringAndClean();
}

Status RemoteWorkerManager::ToggleStreamBlocking(const std::string &workerAddr, const std::string &streamName,
                                                 bool enable)
{
    if (enable) {
        INJECT_POINT("RemoteWorker.EnableStreamBlocking.sleep");
    }
    std::shared_ptr<RemoteWorker> rw;
    RETURN_IF_NOT_OK(GetRemoteWorker(workerAddr, rw));
    VLOG(SC_NORMAL_LOG_LEVEL) << (enable ? "Blocking" : "Unblocking") << " Producer for stream: " << streamName
                              << " From remote worker: " << workerAddr;
    RemoteStreamInfoTbbMap::accessor accessor;
    RETURN_IF_NOT_OK(rw->GetAccessor(streamName, accessor));
    std::get<K_BLOCKED>(accessor->second) = enable;
    // Update stream metrics if it is enabled
    if (ScMetricsMonitor::Instance()->IsEnabled()) {
        uint64_t numRemoteConsumers = std::get<K_CONSUMER_ID>(accessor->second).size();
        accessor.release();
        StreamManagerMap::const_accessor streamMgrAccessor;
        RETURN_IF_NOT_OK(scSvc_->GetStreamManager(streamName, streamMgrAccessor));
        if (enable) {
            streamMgrAccessor->second->GetSCStreamMetrics()->IncrementMetric(StreamMetric::NumRemoteConsumersBlocking,
                                                                             numRemoteConsumers);
        } else {
            streamMgrAccessor->second->GetSCStreamMetrics()->DecrementMetric(StreamMetric::NumRemoteConsumersBlocking,
                                                                             numRemoteConsumers);
        }
    }
    return Status::OK();
}

Status RemoteWorkerManager::DelRemoteConsumer(const std::string &workerAddr, const std::string &streamName,
                                              const std::string &consumerId)
{
    std::vector<std::string> dest;
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = remoteWorkerDict_.find(workerAddr);
        CHECK_FAIL_RETURN_STATUS(iter != remoteWorkerDict_.end(), K_NOT_FOUND,
                                 FormatString("Remote worker:<%s> does not exist", workerAddr));
        auto &rw = iter->second;
        Optional<bool> mapEmpty(false);
        RETURN_IF_NOT_OK(rw->DelRemoteConsumer(streamName, consumerId, mapEmpty));
        if (mapEmpty.value()) {
            LOG(INFO) << "Erase remote worker " << workerAddr << " from remoteWorkerDict_";
            (void)remoteWorkerDict_.erase(iter);
        }
        dest = GetRemoteWorkers(streamName);
    }
    // Update the scan list.
    RETURN_IF_NOT_OK_EXCEPT(dataPool_->RemoveStreamObject(streamName, dest), K_SC_STREAM_NOT_FOUND);
    return Status::OK();
}

Status RemoteWorkerManager::AddRemoteConsumer(const std::shared_ptr<StreamManager> &streamMgr,
                                              const HostPort &localWorkerAddress, const HostPort &remoteWorkerAddress,
                                              const std::string &streamName, const SubscriptionConfig &subConfig,
                                              const std::string &consumerId, uint64_t lastAckCursor)
{
    StreamFields streamFields;
    streamMgr->GetStreamFields(streamFields);
    std::shared_ptr<SharedPageQueue> sharedPage;
    std::vector<std::string> dest;
    {
        std::shared_ptr<RemoteWorker> rw;
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        auto iter = remoteWorkerDict_.find(remoteWorkerAddress.ToString());
        if (iter == remoteWorkerDict_.end()) {
            auto remoteWorker = std::make_shared<RemoteWorker>(localWorkerAddress, remoteWorkerAddress, akSkManager_,
                                                               scSvc_, workerInstanceId_, scAllocateManager_, this);
            RETURN_IF_NOT_OK(remoteWorker->Init());
            remoteWorker->RegisterRecordRemoteSendRateCallBack(
                [this](int successNum, int totalNum) { remoteSendRateVec_.BlockingEmplaceBack(successNum, totalNum); });

            iter = remoteWorkerDict_.emplace(remoteWorkerAddress.ToString(), remoteWorker).first;
        }
        rw = iter->second;
        RETURN_IF_NOT_OK(
            rw->AddRemoteConsumer(streamName, subConfig, consumerId, streamMgr->GetMaxWindowCount(), lastAckCursor));
        if (StreamManager::EnableSharedPage(streamFields.streamMode_)) {
            rw->GetOrCreateSharedPageQueue(streamName, sharedPage);
            streamMgr->SetSharedPageQueue(sharedPage);
            const std::string &keyName = sharedPage->GetStreamName();
            // Add fake consumer for shared page remote ack purposes.
            // Calculate the ack cursor in case of shared page queue re-added to scan list.
            auto lastAppendCursor = sharedPage->GetLastAppendCursor();
            lastAckCursor = lastAppendCursor;
            uint64_t cursor;
            // Shared page only supports single consumer, so only need to check this remote worker for ack cursor.
            Status rc = rw->GetStreamLastAckCursor(keyName, cursor);
            if (rc.IsOk()) {
                lastAckCursor = std::min<uint64_t>(lastAckCursor, cursor);
            }
            RETURN_IF_NOT_OK(rw->AddRemoteConsumer(keyName, SubscriptionConfig(), keyName, 1, 0));
        }
        dest = GetRemoteWorkers(streamName);
    }
    // Add the stream to the scan list, and update the destination.
    if (!StreamManager::EnableSharedPage(streamFields.streamMode_)) {
        RETURN_IF_NOT_OK_EXCEPT(dataPool_->AddStreamObject(streamMgr, streamName, dest, lastAckCursor), K_DUPLICATED);
    } else {
        RETURN_IF_NOT_OK_EXCEPT(dataPool_->AddSharedPageObject(sharedPage, streamName, dest, lastAckCursor),
                                K_DUPLICATED);
    }
    return Status::OK();
}

Status RemoteWorkerManager::ClearAllRemoteConsumer(const std::string &streamName, bool forceClose)
{
    if (forceClose) {
        LOG(INFO) << "Client has crashed cleaning up the stream: " << streamName;
        // If last producer is force closed due to client crash
        // Irrespective of Ack position just discard the data
        // by Stop scanning and remove stream from RemoteConsumerMap
        RETURN_IF_NOT_OK(DeleteStream(streamName));
    }
    return Status::OK();
}

Status RemoteWorkerManager::ResetStreamScanList(const std::string &streamName)
{
    RETURN_IF_NOT_OK_EXCEPT(dataPool_->ResetStreamScanPosition(streamName), K_SC_STREAM_NOT_FOUND);
    return Status::OK();
}

std::vector<std::string> RemoteWorkerManager::GetRemoteWorkers(const std::string &streamName)
{
    std::vector<std::string> v;
    std::for_each(remoteWorkerDict_.begin(), remoteWorkerDict_.end(), [&v, &streamName](const auto &kv) {
        auto &rw = kv.second;
        if (rw->HasRemoteConsumers(streamName)) {
            v.emplace_back(kv.first);
        }
    });
    return v;
}

void RemoteAckInfo::SyncStreamLastAckCursor(Optional<AckRange> ackRange, const std::string &logPrefix)
{
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Last remote ack cursor %zu", logPrefix, lastAckCursor_);
    if (!ackQue_.empty()) {
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Most recent ack cursor %zu", logPrefix, ackQue_.top().first);
    }
    if (ackRange) {
        auto begCursor = ackRange.value().first;
        if (lastAckCursor_ < begCursor) {
            ackQue_.push(ackRange.value());
        }
    }
    while (!ackQue_.empty() && lastAckCursor_ + 1 == ackQue_.top().first) {
        // We can pop the top and move up
        auto ele = ackQue_.top();
        ackQue_.pop();
        lastAckCursor_ = ele.first + ele.second - 1;
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Move up ack cursor to %zu", logPrefix, lastAckCursor_);
    }
}

uint64_t RemoteAckInfo::GetStreamLastAckCursor() const
{
    return lastAckCursor_;
}

void RemoteAckInfo::Reset()
{
    lastAckCursor_ = 0;
    // There is no clear() function call, and we will pop them all.
    while (!ackQue_.empty()) {
        ackQue_.pop();
    }
}

RemoteAckInfo::RemoteAckInfo(uint64_t cursor) : lastAckCursor_(cursor)
{
}

Status RemoteConsumerMap::AddConsumer(const std::string &streamName, const std::string &consumerId,
                                      uint64_t windowCount, uint64_t lastAckCursor)
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::accessor accessor;
    if (streamConsumers_.find(accessor, streamName)) {
        auto ret = std::get<K_CONSUMER_ID>(accessor->second).emplace(consumerId);
        CHECK_FAIL_RETURN_STATUS(
            ret.second, K_DUPLICATED,
            FormatString("[S:%s] Add remote consumer error. Duplicate consumer id %s", streamName, consumerId));
    } else {
        std::set<std::string> remoteConsumerId;
        remoteConsumerId.emplace(consumerId);
        auto RemoteConsumer =
            std::make_tuple(false, RemoteAckInfo(lastAckCursor), windowCount, std::move(remoteConsumerId));
        streamConsumers_.emplace(accessor, streamName, std::move(RemoteConsumer));
    }
    return Status::OK();
}

Status RemoteConsumerMap::DeleteConsumer(const std::string &streamName, const std::string &consumerId,
                                         Optional<bool> &mapEmpty)
{
    std::unique_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::accessor accessor;
    bool find = streamConsumers_.find(accessor, streamName);
    CHECK_FAIL_RETURN_STATUS(find, StatusCode::K_SC_STREAM_NOT_FOUND,
                             FormatString("[S:%s] Stream not found", streamName));
    CHECK_FAIL_RETURN_STATUS(std::get<K_CONSUMER_ID>(accessor->second).erase(consumerId) == 1,
                             StatusCode::K_SC_CONSUMER_NOT_FOUND,
                             FormatString("[S:%s, C:%s] Consumer not belong to Stream", streamName, consumerId));
    if (std::get<K_CONSUMER_ID>(accessor->second).empty()) {
        (void)streamConsumers_.erase(accessor);
    }
    if (mapEmpty) {
        *mapEmpty = streamConsumers_.empty();
    }
    return Status::OK();
}

Status RemoteConsumerMap::DeleteStream(const std::string &streamName, Optional<bool> &mapEmpty)
{
    std::unique_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::accessor accessor;
    bool find = streamConsumers_.find(accessor, streamName);
    CHECK_FAIL_RETURN_STATUS(find, StatusCode::K_SC_STREAM_NOT_FOUND,
                             FormatString("Can not find stream:<%s>", streamName));
    (void)streamConsumers_.erase(accessor);
    if (mapEmpty) {
        *mapEmpty = streamConsumers_.empty();
    }
    return Status::OK();
}

bool RemoteConsumerMap::HasRemoteConsumers(const std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::const_accessor accessor;
    return streamConsumers_.find(accessor, streamName);
}

Status RemoteConsumerMap::ToggleStreamBlocking(const std::string &streamName, bool enable)
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    LOG(INFO) << (enable ? "Block" : "Unblock") << " Producer for stream " << streamName;
    RemoteStreamInfoTbbMap::accessor accessor;
    bool find = streamConsumers_.find(accessor, streamName);
    CHECK_FAIL_RETURN_STATUS(find, StatusCode::K_SC_STREAM_NOT_FOUND,
                             FormatString("Can not find stream:<%s>", streamName));
    std::get<K_BLOCKED>(accessor->second) = enable;
    return Status::OK();
}

bool RemoteConsumerMap::IsStreamSendBlocked(const std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::const_accessor accessor;
    return streamConsumers_.find(accessor, streamName) && std::get<K_BLOCKED>(accessor->second);
}

Status RemoteConsumerMap::GetStreamLastAckCursor(const std::string &streamName, uint64_t &cursor)
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::accessor accessor;
    if (streamConsumers_.find(accessor, streamName)) {
        cursor = std::get<K_ACK>(accessor->second).GetStreamLastAckCursor();
        return Status::OK();
    }
    RETURN_STATUS(K_SC_CONSUMER_NOT_FOUND, FormatString("Can not find stream:<%s>", streamName));
}

void RemoteConsumerMap::SyncStreamLastAckCursor(RemoteStreamInfoTbbMap::accessor &accessor,
                                                Optional<RemoteAckInfo::AckRange> ackRange,
                                                const std::string &logPrefix)
{
    std::get<K_ACK>(accessor->second).SyncStreamLastAckCursor(ackRange, logPrefix);
}

bool RemoteConsumerMap::Empty() const
{
    std::unique_lock<std::shared_timed_mutex> lock(consumerMutex_);
    return streamConsumers_.empty();
}

uint64_t RemoteConsumerMap::GetMaxWindowCount(const std::string &streamName) const
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    RemoteStreamInfoTbbMap::accessor accessor;
    if (streamConsumers_.find(accessor, streamName)) {
        return std::get<K_WINDOW_COUNT>(accessor->second);
    }
    return 1;
}

Status RemoteConsumerMap::GetAccessor(const std::string &streamName, RemoteStreamInfoTbbMap::accessor &accessor,
                                      const std::string &logPrefix)
{
    std::shared_lock<std::shared_timed_mutex> lock(consumerMutex_);
    auto success = streamConsumers_.find(accessor, streamName);
    CHECK_FAIL_RETURN_STATUS(success, K_SC_STREAM_NOT_FOUND,
                             FormatString("[%s, S:%s] Stream not found", logPrefix, streamName));
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
