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
 * Description: ExclusivePageQueue
 */

#include "datasystem/worker/stream_cache/page_queue/exclusive_page_queue.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/stream_cache/stream_data_pool.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

DS_DEFINE_uint32(sc_cache_pages, 16, "Default number of cache pages");
DS_DECLARE_string(sc_encrypt_secret_key);
DS_DECLARE_string(encrypt_kit);
DS_DECLARE_uint64(shared_memory_size_mb);

namespace datasystem {
namespace worker {
namespace stream_cache {

Status ExclusivePageQueue::UnblockProducers()
{
    // If there is at least one page available, wake up all blocked producers.
    // BigElement size is always greater than one page and so when a BigElement
    // is released, we know there should be sufficient memory to create one data
    // page.
    {
        ReadLockHelper rlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        if (pendingFreePages_.empty() && ackChain_.empty()
            && streamFields_.maxStreamSize_ - usedMemBytes_ < static_cast<uint64_t>(streamFields_.pageSize_)) {
            return Status::OK();
        }
    }
    {
        // unblock remote stream send
        std::lock_guard<std::mutex> lock2(unblockMutex_);
        std::for_each(unblockCallbacks_.begin(), unblockCallbacks_.end(), [](auto &cb) { cb.second(); });
        // We need to clear the call backs after we sent them
        unblockCallbacks_.clear();
    }
    // Ask the parent stream manager to unblock any local CreateDataPage waiters
    streamMgr_->UnblockCreators();
    return Status::OK();
}

Status ExclusivePageQueue::GetDataPage(
    const GetDataPageReqPb &req, const std::shared_ptr<Consumer> &consumer,
    const std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> &serverApi)
{
    const auto &lastRecvCursor = req.last_recv_cursor();
    const auto timeoutMs = static_cast<uint64_t>(req.timeout_ms());
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Locate page. lastRecvCursor %zu. Timeout %u", LogPrefix(),
                                              lastRecvCursor, timeoutMs);
    std::shared_ptr<StreamDataPage> page;
    Status rc = LocatePage(lastRecvCursor, page);
    if (rc.GetCode() == K_NOT_FOUND && timeoutMs > 0) {
        // Worker execute the pending receive timer before add the pending receive task.
        TimerQueue::TimerImpl timer;
        auto traceID = Trace::Instance().GetTraceID();
        auto clientService = streamMgr_->GetClientService();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(clientService != nullptr, K_SHUTTING_DOWN, "worker shutting down.");
        auto streamName = streamMgr_->GetStreamName();
        GetDataPageReqPb rq = req;  // Pass a non-const copy to the lambda
        auto func = [clientService, streamName, rq, serverApi, traceID]() mutable {
            // Turn off the timer because we are going call itself. Otherwise, we run into loop.
            rq.set_timeout_ms(0);
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            // When wakes up, check if the stream has been deleted or not.
            StreamManagerMap::const_accessor accessor;
            RETURN_IF_NOT_OK(clientService->GetStreamManager(streamName, accessor));
            std::shared_ptr<StreamManager> streamMgr = accessor->second;
            RETURN_IF_NOT_OK(streamMgr->CheckIfStreamActive());
            std::shared_ptr<Subscription> sub;
            RETURN_IF_NOT_OK(streamMgr->GetSubscription(rq.subscription_name(), sub));
            const auto &consumerId = rq.consumer_id();
            CHECK_FAIL_RETURN_STATUS(sub->GetSubscriptionType() == SubscriptionType::STREAM, StatusCode::K_INVALID,
                                     "Only support STREAM mode.");
            std::shared_ptr<Consumer> consumer;
            RETURN_IF_NOT_OK(sub->GetConsumer(consumerId, consumer));
            Status rc;
            bool wakeupPendingRecvOnProdFault;
            consumer->RemovePendingReceive(wakeupPendingRecvOnProdFault);
            if (wakeupPendingRecvOnProdFault) {
                rc = { K_SC_PRODUCER_NOT_FOUND, "Consumer cannot continue due to producer being forced off." };
                serverApi->SendStatus(rc);
                return Status::OK();
            }
            rc = streamMgr->GetExclusivePageQueue()->GetDataPage(rq, consumer, serverApi);
            if (rc.IsError()) {
                serverApi->SendStatus(rc);
            }
            return Status::OK();
        };
        return consumer->AddPendingReceive(lastRecvCursor, timeoutMs, func, serverApi);
    }
    RETURN_IF_NOT_OK(rc);
    RETURN_RUNTIME_ERROR_IF_NULL(page);
    return ReturnGetPageRspPb(page->GetShmView(), serverApi);
}

Status ExclusivePageQueue::ReturnGetPageRspPb(
    const ShmView &shmView,
    const std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> &serverApi)
{
    GetDataPageRspPb rsp;
    ShmViewPb pb;
    pb.set_fd(shmView.fd);
    pb.set_mmap_size(shmView.mmapSz);
    pb.set_offset(shmView.off);
    pb.set_size(shmView.sz);
    rsp.mutable_page_view()->CopyFrom(pb);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "Write reply to client stream failed");
    // We do not have to increase the reference count. This call is made for the purpose of
    // consumers (both local and remote) and we won't ack any page consumers are still reading.
    return Status::OK();
}

Status ExclusivePageQueue::UpdateStreamFields(const StreamFields &streamFields)
{
    const static uint64_t shmMemSize = FLAGS_shared_memory_size_mb * 1024 * 1024;
    std::unique_lock<std::mutex> lock(cfgMutex_);
    // If the fields were empty, do not sanity check them and just set the fields to the new values.
    if (streamFields_.Empty()) {
        // The optional parameters are given, sanity check them first against the shared memory limits.
        // (Other checks are already done at the client side)
        CHECK_FAIL_RETURN_STATUS(
            streamFields.maxStreamSize_ <= shmMemSize, K_INVALID,
            FormatString("maxStreamSize exceeds shared memory size [stream size, shm size]: [%zu, %zu] ",
                         streamFields.maxStreamSize_, shmMemSize));
        CHECK_FAIL_RETURN_STATUS(static_cast<uint64_t>(streamFields.pageSize_) <= shmMemSize, K_INVALID,
                                 FormatString("pageSize exceeds shared memory size [page size, shm size]: [%zu, %zu] ",
                                              streamFields.pageSize_, shmMemSize));
        streamFields_ = streamFields;
        LOG(INFO) << FormatString(
            "[%s] Stream configuration updated with max stream size: %zu, page size: %zu, auto cleanup: %s, "
            "retain for num consumers: %zu, encrypt stream: %s, and reserve size: %zu",
            LogPrefix(), streamFields_.maxStreamSize_, streamFields_.pageSize_,
            streamFields_.autoCleanup_ ? "true" : "false", streamFields_.retainForNumConsumers_,
            streamFields_.encryptStream_ ? "true" : "false", streamFields_.reserveSize_);
        return Status::OK();
    }

    // otherwise, sanity check that the new settings are a match to the old ones (and do not change them)
    CHECK_FAIL_RETURN_STATUS(
        streamFields_ == streamFields, K_INVALID,
        FormatString("[%s] Changing stream config fields [max stream size, page size, auto cleanup, retain for num "
                     "consumers, encrypt stream, reserve size, stream mode] not supported: Current: [%zu, %zu, %s, "
                     "%zu, %s, %zu, %d] "
                     "Invalid: [%zu, %zu, %s, %zu, %s, %zu, %d]",
                     LogPrefix(), streamFields_.maxStreamSize_, streamFields_.pageSize_,
                     (streamFields_.autoCleanup_ ? "true" : "false"), streamFields_.retainForNumConsumers_,
                     streamFields_.encryptStream_ ? "true" : "false", streamFields_.reserveSize_,
                     streamFields_.streamMode_, streamFields.maxStreamSize_, streamFields.pageSize_,
                     (streamFields.autoCleanup_ ? "true" : "false"), streamFields.retainForNumConsumers_,
                     streamFields.encryptStream_ ? "true" : "false", streamFields.reserveSize_,
                     streamFields.streamMode_));
    return Status::OK();
}

size_t ExclusivePageQueue::GetPageSize() const
{
    return streamFields_.pageSize_;
}

bool ExclusivePageQueue::AutoCleanup() const
{
    return streamFields_.autoCleanup_;
}

uint64_t ExclusivePageQueue::GetReserveSize() const
{
    return streamFields_.reserveSize_ == 0 ? streamFields_.pageSize_ : streamFields_.reserveSize_;
}

std::string ExclusivePageQueue::LogPrefix() const
{
    return FormatString("S:%s", streamMgr_->GetStreamName());
}

Status ExclusivePageQueue::CheckHadEnoughMem(size_t memSize)
{
    PerfPoint point(PerfKey::PAGE_CHECK_MEM);
    INJECT_POINT("worker.CheckHadEnoughMem");
    auto maxStreamSize = streamFields_.maxStreamSize_;
    // We will use the other form to calculate a theoretical used size by taking
    // pendingFreePages_ into consideration.
    // The pages in the pendingFreePages_ will be released when they are safe to do
    // In a way, we are borrowing some bytes from the Arena but will return them as soon
    // as the pending free pages are released.
    // Note that the difference of two atomic variables does not always give accurate result.
    // But pendingFreeBytes_ should always be subset of usedMemBytes_
    auto usedMemBytes = std::min<size_t>(usedMemBytes_.load(std::memory_order_relaxed), maxStreamSize);
    auto pendingFreeBytes = pendingFreeBytes_.load(std::memory_order_relaxed);
    auto bytesUsed = (pendingFreeBytes <= usedMemBytes) ? usedMemBytes - pendingFreeBytes : 0;
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] max memory %zu, used %zu, need %zu", LogPrefix(), maxStreamSize,
                                                bytesUsed, memSize);
    if (maxStreamSize - bytesUsed < memSize) {
        RETURN_STATUS(
            StatusCode::K_OUT_OF_MEMORY,
            FormatString("[%s] The stream does not have enough memory, and max memory %zu, used %zu, need %zu",
                         LogPrefix(), maxStreamSize, bytesUsed, memSize));
    }
    return Status::OK();
}

Status ExclusivePageQueue::VerifyWhenAlloc() const
{
    std::unique_lock<std::mutex> lock(cfgMutex_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        !streamFields_.Empty(), StatusCode::K_RUNTIME_ERROR,
        FormatString("[%s] Uninitialized page or stream sizes in stream page owner", LogPrefix()));
    return Status::OK();
}

Status ExclusivePageQueue::AllocateMemoryImpl(size_t memSizeNeeded, ShmUnit &shmUnit, bool retryOnOOM)
{
    auto tenantId = TenantAuthManager::ExtractTenantId(streamMgr_->GetStreamName());
    return streamMgr_->GetAllocManager()->AllocateMemoryForStream(tenantId, streamMgr_->GetStreamName(), memSizeNeeded,
                                                                  true, shmUnit, retryOnOOM);
}

Status ExclusivePageQueue::ReserveStreamMemory()
{
    RETURN_OK_IF_TRUE(reserveState_.pageZeroCreated && reserveState_.freeListCreated);
    // This creates the initial first page when the object is created.
    // It is driven by StreamManager.
    {
        std::unique_lock<std::mutex> lock(cfgMutex_);
        // If we don't have the page size (yet), return K_NOT_READY.
        if (streamFields_.Empty()) {
            const std::string errMsg =
                FormatString("[%s] Uninitialized page or stream sizes in stream page owner", LogPrefix());
            LOG(INFO) << errMsg;
            RETURN_STATUS(K_NOT_READY, errMsg);
        }
    }
    // We will reserve stream memory up to streamFields_.reserveSize_
    // If reserveSize_ is 0, it defaults to one page size.
    // In other words, we always allocate at least the first page.
    auto createPageZero = [this]() {
        INJECT_POINT("CreatePageZero.AllocMemory");
        RETURN_OK_IF_TRUE(reserveState_.pageZeroCreated);
        std::shared_ptr<StreamDataPage> lastPage;
        // We pass a null ShmView as the last reference and the page creation will be a no-op
        // if the first page is already created.
        // We do not want to call the CreateOrGetLastDataPage version because the stream manager
        // is not fully created yet. It is not wise to call TryWakeUpPendingReceive() or update
        // any producer's mailbox.
        Status rc = CreateOrGetLastDataPageImpl(0, ShmView(), lastPage, true);
        if (rc.IsOk()) {
            reserveState_.pageZeroCreated = true;
        }
        return rc;
    };
    Status rc = createPageZero();
    // Map OOM to a new return code if we aren't able to reserve the initial page
    if (rc.GetCode() == K_OUT_OF_MEMORY) {
        RETURN_STATUS_LOG_ERROR(
            K_SC_STREAM_RESOURCE_ERROR,
            FormatString("[%s] Resource error. Unable to reserve the first shared memory page memory, detail: %s",
                         LogPrefix(), rc.ToString()));
    }
    if (streamFields_.reserveSize_ <= streamFields_.pageSize_) {
        reserveState_.freeListCreated = true;  // No need to continue;
        return rc;
    }
    // The rest of the code is when the reserveSize_ > page size
    RETURN_IF_NOT_OK(ReserveAdditionalMemory());
    return Status::OK();
}

Status ExclusivePageQueue::ReserveAdditionalMemory()
{
    // Switch to use signed integer
    auto pageSize = static_cast<int64_t>(streamFields_.pageSize_);
    // Page 0 has been created.
    int64_t remaining = static_cast<int64_t>(streamFields_.reserveSize_) - pageSize;
    // We must add the rest to the ackChain_, not idxChain_
    WriteLockHelper xlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    // Check again after we have the lock.
    RETURN_OK_IF_TRUE(reserveState_.freeListCreated);
    std::list<PageShmUnit> freeList;
    std::vector<ShmKey> undoList;
    bool needRollback = true;
    Raii raii([this, &needRollback, &undoList]() {
        if (needRollback) {
            // Undo the changes. We can leave the lastPage alone. It is more work to take it out.
            (void)FreePages(undoList, false);
        }
    });
    auto func = [this, remaining, pageSize, &freeList, &undoList]() mutable -> Status {
        while (remaining > 0) {
            auto f = []() {
                INJECT_POINT("ReserveAdditionalMemory.AllocMemory");
                return Status::OK();
            };
            RETURN_IF_NOT_OK(f());
            std::shared_ptr<StreamDataPage> page;
            RETURN_IF_NOT_OK(CreateNewPage(page, true));
            undoList.push_back(page->GetPageId());
            freeList.emplace_back(0, std::move(page));
            remaining -= pageSize;
        }
        LOG(INFO) << FormatString("[%s] Reserve additional %zu pages", LogPrefix(), freeList.size());
        return Status::OK();
    };
    Status rc = func();
    // As in other code path. Change the rc from OOM to RESOURCE_ERROR
    if (rc.GetCode() == K_OUT_OF_MEMORY) {
        RETURN_STATUS_LOG_ERROR(K_SC_STREAM_RESOURCE_ERROR,
                                FormatString("[%s] Resource error. Unable to reserve additional %zu bytes", LogPrefix(),
                                             static_cast<int64_t>(streamFields_.reserveSize_) - pageSize));
        return rc;
    }
    Optional<std::list<PageShmUnit>> reserveList(freeList);
    RETURN_IF_NOT_OK(AppendFreePagesImplNotLocked(0, reserveList, false));
    reserveState_.freeListCreated = true;
    needRollback = false;
    return Status::OK();
}

Status ExclusivePageQueue::InsertBigElement(void *buf, size_t sz, std::pair<size_t, size_t> &res, uint64_t timeoutMs,
                                            const bool headerBit, StreamMetaShm *streamMetaShm,
                                            const std::string &producerId)
{
    RaiiPlus raiiP;
    if (streamMetaShm) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamMetaShm->TryIncUsage(sz), "TryIncUsage failed");
        raiiP.AddTask([sz, streamMetaShm]() {
            LOG_IF_ERROR(streamMetaShm->TryDecUsage(sz), "");
        });
    }

    auto flags = InsertFlags::REMOTE_ELEMENT | InsertFlags::BIG_ELEMENT;
    ShmView outView;
    RETURN_IF_NOT_OK(streamMgr_->AllocBigShmMemoryInternalReq(timeoutMs, sz, outView, producerId));
    std::shared_ptr<ShmUnitInfo> pageUnitInfo;
    RETURN_IF_NOT_OK(LocatePage(outView, pageUnitInfo));
    // From now on make sure we free the memory on error exit
    bool needRollback = true;
    auto raii = std::make_unique<Raii>([this, &needRollback, &pageUnitInfo]() {
        if (needRollback) {
            std::vector<ShmKey> v;
            auto pageId = StreamPageBase::CreatePageId(pageUnitInfo);
            v.push_back(pageId);
            (void)FreePages(v, true);
        }
    });
    auto bigElementPage = std::make_shared<StreamLobPage>(pageUnitInfo, false);
    std::string pointerString;
    RETURN_IF_NOT_OK(bigElementPage->Init());
    // using ExclusivePage for consumer node, no need streamNo.
    HeaderAndData ele(reinterpret_cast<uint8_t *>(buf), sz, 0);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(StreamDataPage::SerializeToShmViewPb(bigElementPage->GetShmView(), pointerString),
                                     "Serialization error");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(bigElementPage->Insert(ele), "BigElement insert");

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Inserting a BigElement [S:%zu] into BigElement page: %s",
                                              streamMgr_->GetStreamName(), ele.size, bigElementPage->GetPageId());
    INJECT_POINT("InsertBigElement.Rollback");
    std::vector<size_t> v;
    v.push_back(pointerString.size());
    std::vector<bool> headerBits;
    headerBits.push_back(headerBit);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        BatchInsertImpl(pointerString.data(), v, res, flags, timeoutMs, headerBits, streamMetaShm, GetStringUuid()),
        "Insert pointer");
    res.second = sz;
    needRollback = false;
    raiiP.ClearAllTask();
    (void)needRollback;
    return Status::OK();
}

Status ExclusivePageQueue::BatchInsertImpl(void *buf, std::vector<size_t> &sz, std::pair<size_t, size_t> &res,
                                           InsertFlags flags, uint64_t timeoutMs, const std::vector<bool> &headerBits,
                                           StreamMetaShm *streamMetaShm, const std::string &producerId)
{
    // This is a special form of batch insert. The data is sent from a remote worker
    // and the elements are already in the correct reverse order.
    Status rc;
    std::shared_ptr<StreamDataPage> lastPage;
    auto handleNoSpace = [this, &lastPage, &timeoutMs]() {
        // As in local producer case, if there is a free page coming after,
        // seal the current page.
        // First of all we need to lock the page to block other producers.
        ShmView nextPage;
        bool isFreePage;
        lastPage->nextPage_->GetView(nextPage, isFreePage, std::numeric_limits<uint64_t>::max());
        // If there is no next page encoded on the page, just return no space
        if (nextPage.fd <= 0) {
            RETURN_STATUS(K_NO_SPACE, "No next page");
        }
        // There is a next page. Check if the current page has been sealed.
        if (isFreePage) {
            // Now we will seal the current page, acquire the next page.
            auto func = [this](const ShmView &v, std::shared_ptr<StreamDataPage> &out) { return LocatePage(v, out); };
            RETURN_IF_NOT_OK_EXCEPT(lastPage->Seal(nextPage, timeoutMs, func, LogPrefix()), K_DUPLICATED);
        }
        RETURN_STATUS(K_SC_END_OF_PAGE, "New empty page is created");
    };
    do {
        std::vector<size_t> remaining(sz.begin() + res.first, sz.end());
        ShmView outView;
        RETURN_IF_NOT_OK(streamMgr_->AllocDataPageInternalReq(
            timeoutMs, lastPage == nullptr ? ShmView() : lastPage->GetShmView(), outView, producerId));
        std::shared_ptr<ShmUnitInfo> pageUnitInfo;
        RETURN_IF_NOT_OK(LocatePage(outView, pageUnitInfo));
        lastPage = std::make_shared<StreamDataPage>(pageUnitInfo, 0, false, isSharedPage_);
        RETURN_IF_NOT_OK(lastPage->Init());
        RETURN_IF_NOT_OK(lastPage->RefPage(FormatString("%s:%s", __FUNCTION__, __LINE__)));
        std::pair<size_t, size_t> batchRes(0, 0);
        rc = lastPage->BatchInsert(buf, remaining, timeoutMs, batchRes, flags, headerBits, streamMetaShm);
        res.first += batchRes.first;
        res.second += batchRes.second;
        if (rc.GetCode() == K_NO_SPACE) {
            rc = handleNoSpace();
        }
        Status rc2 = lastPage->ReleasePage(FormatString("%s:%s", __FUNCTION__, __LINE__));
        if (rc.IsOk()) {
            RETURN_IF_NOT_OK(rc2);
            if (res.first < sz.size()) {
                // Need to find another page to continue
                std::string msg =
                    FormatString("[%s] Page can only insert %zu rows. Total %zu. Remaining %zu. Continue to next page",
                                 LogPrefix(), batchRes.first, sz.size(), sz.size() - res.first);
                VLOG(SC_NORMAL_LOG_LEVEL) << msg;
                rc = Status(K_TRY_AGAIN, msg);
            }
        }
    } while (rc.GetCode() == K_NO_SPACE || rc.GetCode() == K_SC_END_OF_PAGE || rc.GetCode() == K_TRY_AGAIN);
    return rc;
}

Status ExclusivePageQueue::BatchInsert(void *buf, std::vector<size_t> &sz, std::pair<size_t, size_t> &res,
                                       uint64_t timeoutMs, const std::vector<bool> &headerBits,
                                       StreamMetaShm *streamMetaShm, const std::string &producerId)
{
    // Check if it is a big element. If it is a big element, remote manager will send
    // it separately in its own PV.
    if (sz.size() == 1 && sz.at(0) > static_cast<size_t>(streamFields_.pageSize_ - StreamDataPage::PageOverhead())) {
        return InsertBigElement(buf, sz.at(0), res, timeoutMs, headerBits.at(0), streamMetaShm, producerId);
    } else {
        return BatchInsertImpl(buf, sz, res, InsertFlags::REMOTE_ELEMENT, timeoutMs, headerBits, streamMetaShm,
                               producerId);
    }
}

Status ExclusivePageQueue::ReleaseAllPages()
{
    auto lastAppendCursor = GetLastAppendCursor();
    WriteLockHelper xlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    lastPage_.reset();
    idxChain_.clear();
    ackChain_.clear();
    pendingFreePages_.clear();
    pendingFreeBytes_ = 0;
    {
        WriteLockHelper xlock4(STREAM_COMMON_LOCK_ARGS(poolMutex_));
        shmPool_.clear();
    }
    usedMemBytes_ = 0;
    lastAckCursor_ = lastAppendCursor;
    nextCursor_ = lastAppendCursor + 1;
    reserveState_.pageZeroCreated = false;
    reserveState_.freeListCreated = false;
    // Set release metrics equal to create metrics
    // Metrics safe since already protected by other locks
    scMetricPagesReleased_.store(scMetricPagesCreated_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    scMetricBigPagesReleased_.store(scMetricBigPagesCreated_.load(std::memory_order_relaxed),
                                    std::memory_order_relaxed);
    LOG(INFO) << FormatString("[%s] ExclusivePageQueue release all pages done", LogPrefix());
    return Status::OK();
}

Status ExclusivePageQueue::Reset()
{
    // Synchronize with the timer queue
    {
        std::unique_lock<std::mutex> lock(unblockMutex_);
        unblockCallbacks_.clear();
    }
    // Block any new page created
    WriteLockHelper xlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    // Block timer queue
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    // Drop the reference on the last page
    if (lastPage_) {
        RETURN_IF_NOT_OK(lastPage_->ReleasePage(LogPrefix()));
    }
    lastPage_.reset();
    idxChain_.clear();
    ackChain_.clear();
    pendingFreePages_.clear();
    pendingFreeBytes_ = 0;
    {
        WriteLockHelper xlock4(STREAM_COMMON_LOCK_ARGS(poolMutex_));
        shmPool_.clear();
    }
    usedMemBytes_ = 0;
    lastAckCursor_ = 0;
    nextCursor_ = 1;
    reserveState_.pageZeroCreated = false;
    reserveState_.freeListCreated = false;
    // Set release metrics equal to create metrics
    // Metrics safe since already protected by other locks
    scMetricPagesReleased_.store(scMetricPagesCreated_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    scMetricBigPagesReleased_.store(scMetricBigPagesCreated_.load(std::memory_order_relaxed),
                                    std::memory_order_relaxed);
    LOG(INFO) << FormatString("[%s] ExclusivePageQueue reset done", LogPrefix());
    return Status::OK();
}

void ExclusivePageQueue::RemoveUnblockCallback(const std::string &addr)
{
    std::lock_guard<std::mutex> lock(unblockMutex_);
    unblockCallbacks_.erase(addr);
}

void ExclusivePageQueue::AddUnblockCallback(const std::string &addr, const std::function<void()> &unblockCallback)
{
    std::lock_guard<std::mutex> lock(unblockMutex_);
    unblockCallbacks_.emplace(addr, unblockCallback);
}

ExclusivePageQueue::ExclusivePageQueue(StreamManager *mgr, Optional<StreamFields> cfg)
    : streamFields_(0, 0, false, 0, false, 0, StreamMode::MPMC),
      maxWindowCount_(1),
      streamMgr_(mgr),
      streamName_(streamMgr_ ? streamMgr_->GetStreamName() : "")
{
    if (cfg) {
        streamFields_ = cfg.value();
    }
}

bool ExclusivePageQueue::IsEncryptStream(const std::string &streamName) const
{
    (void)streamName;
    // Note: If FLAGS_sc_encrypt_secret_key is empty, or if FLAGS_encrypt_kit is set to plaintext,
    // or if authentication function between components is already enabled,
    // then stream data encryption/decryption is not applicable.
    // But it should not fail the stream creation, so the check is delayed.
    return streamFields_.encryptStream_ && !FLAGS_sc_encrypt_secret_key.empty()
           && FLAGS_encrypt_kit != ENCRYPT_KIT_PLAINTEXT;
}

RemoteWorkerManager *ExclusivePageQueue::GetRemoteWorkerManager() const
{
    return streamMgr_->GetRemoteWorkerManager();
}

uint64_t ExclusivePageQueue::GetNumPagesCached() const
{
    ReadLockHelper rlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    return ackChain_.size();
}

uint64_t ExclusivePageQueue::GetNumPagesInUse() const
{
    ReadLockHelper rlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    return idxChain_.size();
}

Status ExclusivePageQueue::UpdateLocalCursorLastDataPage(const ShmView &shmView)
{
    RETURN_RUNTIME_ERROR_IF_NULL(updateLastDataPageHandler_);
    return updateLastDataPageHandler_(shmView);
}

std::pair<size_t, bool> ExclusivePageQueue::GetNextBlockedRequestSize()
{
    return streamMgr_->GetNextBlockedRequestSize();
}

std::shared_ptr<PageQueueBase> ExclusivePageQueue::SharedFromThis()
{
    return std::static_pointer_cast<PageQueueBase>(shared_from_this());
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
