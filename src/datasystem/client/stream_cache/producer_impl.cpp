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

/**
 * Description: Implement stream cache producer.
 */
#include "datasystem/client/stream_cache/producer_impl.h"

#include <securec.h>

#include <utility>

#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/stream_cache/cursor.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
namespace stream_cache {

ProducerImpl::ProducerImpl(std::string streamName, std::string tenantId, std::string producerId, int64_t delayFlushTime,
                           int64_t pageSize, std::shared_ptr<ProducerConsumerWorkerApi> workerApi,
                           std::shared_ptr<StreamClientImpl> client, MmapManager *mmapManager,
                           std::shared_ptr<client::ListenWorker> listenWorker, const ShmView &workArea,
                           uint64_t maxStreamSize, const DataVerificationHeader::SenderProducerNo senderProducerNo,
                           const bool enableStreamDataVerification, const DataVerificationHeader::Address address,
                           const DataVerificationHeader::Port port, StreamMode streamMode, uint64_t streamNo,
                           bool enableSharedPage, uint64_t sharedPageSize, const ShmView &streamMetaView)
    : ClientBaseImpl(std::move(streamName), std::move(tenantId), std::move(workerApi), std::move(client), mmapManager,
                     std::move(listenWorker)),
      producerId_(std::move(producerId)),
      delayFlushTime_(delayFlushTime),
      pageSize_(pageSize),
      maxStreamSize_(maxStreamSize),
      senderProducerNo_(senderProducerNo),
      enableStreamDataVerification_(enableStreamDataVerification),
      address_(address),
      port_(port),
      streamMode_(streamMode),
      streamNo_(streamNo),
      enableSharedPage_(enableSharedPage),
      sharedPageSize_(sharedPageSize)
{
    workArea_ = workArea;
    streamMetaView_ = streamMetaView;
    maxElementSize_ = static_cast<size_t>(pageSize_) - StreamDataPage::PageOverhead(false);
    if (enableSharedPage) {
        uint64_t maxElementSizeForSharedPage = sharedPageSize - StreamDataPage::PageOverhead(true);
        maxElementSize_ = std::min(maxElementSize_, maxElementSizeForSharedPage);
    }
}

ProducerImpl::~ProducerImpl()
{
    if (delayFlushTimer_) {
        TimerQueue::GetInstance()->Cancel(*delayFlushTimer_);
    }
    if (unfixTimer_) {
        TimerQueue::GetInstance()->Cancel(*unfixTimer_);
    }
    client_->ClearProducer(producerId_);
}

Status ProducerImpl::Init()
{
    RETURN_IF_NOT_OK(ClientBaseImpl::Init());
    if (enableSharedPage_) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(streamMetaView_ != ShmView(), K_RUNTIME_ERROR,
                                             "streamMetaView_ not initialized");
        auto shmUnitInfo = std::make_shared<ShmUnitInfo>(streamMetaView_.fd, streamMetaView_.mmapSz);
        RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd(tenantId_, shmUnitInfo));
        streamMetaShm_ = std::make_unique<StreamMetaShm>(
            streamName_, static_cast<uint8_t *>(shmUnitInfo->GetPointer()) + streamMetaView_.off, streamMetaView_.sz,
            maxStreamSize_);
        RETURN_IF_NOT_OK(streamMetaShm_->Init(mmapManager_->GetMmapEntryByFd(shmUnitInfo->fd)));
    }

    unfixWaitPost_ = std::make_unique<WaitPost>();
    return Status::OK();
}

void ProducerImpl::ExecAndCancelTimer()
{
    std::unique_lock<std::mutex> flushLock(flushMutex_);
    if (delayFlushTimer_) {
        INJECT_POINT_NO_RETURN("ProducerImpl.ExecAndCancelTimer.sleep");
        TimerQueue::GetInstance()->Cancel(*delayFlushTimer_);
        delayFlushTimer_.reset();
        flushLock.unlock();
        // We do not use the function inside the timer because we can be in the destructor and the function in timer
        // can only be execute when producer still exist and not in destructor.
        ExecFlush();
    }
}

Status ProducerImpl::SetUnfixPageTimer()
{
    CHECK_FAIL_RETURN_STATUS(unfixTimer_ == nullptr, StatusCode::K_RUNTIME_ERROR, "Timer should be cancelled.");
    const int DEFAULT_UNFIX_INTERVAL = 1000;
    TimerQueue::TimerImpl timer;
    unfixWaitPost_->Clear();
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        DEFAULT_UNFIX_INTERVAL,
        [this]() {
            ExecAndCancelTimer();
            LOG_IF_ERROR(UnfixPage(), "");
            unfixWaitPost_->Set();
        },
        timer));
    unfixTimer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
    return Status::OK();
}

void ProducerImpl::UnsetUnfixPageTimer()
{
    // If timer is already set, cancel the timer
    if (unfixTimer_) {
        bool canceled = TimerQueue::GetInstance()->Cancel(*unfixTimer_);
        // Wait for callback to finish if it is already executing
        if (!canceled) {
            unfixWaitPost_->Wait();
        }
        unfixTimer_ = nullptr;
    }
}

void ProducerImpl::SetInactive()
{
    UnsetUnfixPageTimer();
    ExecAndCancelTimer();
    ClientBaseImpl::SetInactive();
}

Status ProducerImpl::CheckNormalState() const
{
    // If the worker's state is unknown, do not try to access the shared memory work area
    RETURN_IF_NOT_OK(ClientBaseImpl::CheckNormalState());
    if (cursor_ == nullptr || cursor_->ForceClose()) {
        RETURN_STATUS(K_SC_CONSUMER_NOT_FOUND, "Force close. No consumer");
    }
    return Status::OK();
}

Status ProducerImpl::HandleNoSpaceFromInsert(int64_t timeoutMs, const Status &rc)
{
    if (rc.GetCode() != K_NO_SPACE) {
        return rc;
    }
    ShmView nextPage;
    bool isFreePage;
    writePage_->nextPage_->GetView(nextPage, isFreePage, std::numeric_limits<uint64_t>::max());
    // If there is no next page encoded on the page, just return no space
    if (nextPage.fd <= 0) {
        return rc;
    }
    // There is a next page. Check if the current page has been sealed.
    if (isFreePage) {
        // Now we will seal the current page, acquire the next page.
        auto func = [this](const ShmView &nextPage, std::shared_ptr<StreamDataPage> &out) {
            std::shared_ptr<ShmUnitInfo> pageInfo;
            std::shared_ptr<client::MmapTableEntry> mmapEntry;
            RETURN_IF_NOT_OK(GetShmInfo(nextPage, pageInfo, mmapEntry));
            auto page = std::make_shared<StreamDataPage>(pageInfo, lockId_, true, false, mmapEntry);
            RETURN_IF_NOT_OK(page->Init());
            out = page;
            return Status::OK();
        };
        RETURN_IF_NOT_OK_EXCEPT(writePage_->Seal(nextPage, timeoutMs, func, LogPrefix()), K_DUPLICATED);
    }
    RETURN_STATUS(K_SC_END_OF_PAGE, "New empty page is created");
}

Status ProducerImpl::InsertBigElement(const HeaderAndData &element, Optional<int64_t> timeoutMs)
{
    int64_t rpcTimeoutMs = timeoutMs ? timeoutMs.value() : RPC_TIMEOUT;
    ShmView pageView;
    RETURN_IF_NOT_OK(
        workerApi_->AllocBigElementMemory(streamName_, producerId_, element.TotalSize(), rpcTimeoutMs, pageView));
    fixPageFromRpc_++;
    InsertFlags flag = InsertFlags::BIG_ELEMENT;
    // If we hit any error below, we will send a rpc to the worker to release the shared memory acquired above.
    RaiiPlus raiiP([this, &pageView, &flag]() {
        if (!TESTFLAG(flag, InsertFlags::INSERT_SUCCESS)) {
            LOG_IF_ERROR(workerApi_->ReleaseBigElementMemory(streamName_, producerId_, pageView),
                         "ReleaseBigElementMemory");
        }
    });
    std::shared_ptr<ShmUnitInfo> pageUnit;
    std::shared_ptr<client::MmapTableEntry> mmapEntry;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetShmInfo(pageView, pageUnit, mmapEntry), "GetShmInfo");
    auto page = std::make_shared<StreamLobPage>(pageUnit, true, mmapEntry);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(page->Init(), "Page Init");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(page->Insert(element), "Insert");
    std::string pointerString;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(StreamDataPage::SerializeToShmViewPb(pageView, pointerString), "Serialization");
    HeaderAndData ele(reinterpret_cast<uint8_t *>(pointerString.data()), pointerString.size(), streamNo_);
    if (enableSharedPage_) {
        auto eleSize = ele.size;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamMetaShm_->TryIncUsage(eleSize),
                                         "Failed to increase the usage of shared memory for stream: " + streamName_);
        raiiP.AddTask([this, eleSize]() {
            LOG_IF_ERROR(streamMetaShm_->TryDecUsage(eleSize),
                         "Failed to decrease the usage of shared memory for stream: " + streamName_);
        });
    }
    INJECT_POINT("ProducerImpl.ReleaseBigElementMemory");
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, %s, %zu] Inserting a BigElement [S:%zu] into BigElement page: %s",
                                              streamName_, producerId_, (cursor_->GetElementCount() + 1), element.size,
                                              page->GetPageId());
    flag |= (element.headerSize_ > 0) ? InsertFlags::HEADER : InsertFlags::NONE;
    RETURN_IF_NOT_OK(SendImpl(ele, flag, timeoutMs));
    raiiP.ClearAllTask();
    return Status::OK();
}

Status ProducerImpl::SendImpl(const HeaderAndData &element, InsertFlags &flag, Optional<int64_t> userTimeoutMs)
{
    PerfPoint point(PerfKey::CLIENT_SEND_ALL);
    // Check if the element is correct
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(element.size > 0, K_INVALID, "Element size should be greater than 0");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(element.ptr != nullptr, K_INVALID, "Element ptr should not be a nullptr");
    // Cancel the timer before Send, and reset the timer after Send to account for the producer idle case
    UnsetUnfixPageTimer();
    Raii unfixPage([this]() { LOG_IF_ERROR(SetUnfixPageTimer(), ""); });
    Status rc;
    int64_t timeoutMs = userTimeoutMs ? userTimeoutMs.value() : RPC_TIMEOUT;
    Timer t(timeoutMs);
    // Locate the page to insert which is most likely the last page.
    // The page can be the current page.
    INJECT_POINT("client.Producer.beforeCheckCursor", [] { return inject::Set("SharedMemViewImpl.GetView", "abort"); });
    RETURN_IF_NOT_OK(FixPage(userTimeoutMs ? Optional<int64_t>(t.GetRemainingTimeMs()) : Optional<int64_t>(), false));

    SETFLAG(flag, (delayFlushTime_ > 0) ? InsertFlags::DELAY_WAKE : InsertFlags::NONE);
    SETFLAG(flag, (element.headerSize_ > 0) ? InsertFlags::HEADER : InsertFlags::NONE);

    // Start the loop
    do {
        // The timeout here is the time we wait on a shared memory lock, not rpc
        rc = writePage_->Insert(element, t.GetRemainingTimeMs(), flag, LogPrefix());
        auto rcCode = rc.GetCode();
        if (rcCode == K_OK) {
            cursor_->IncrementElementCount();
            lastSendElementSeqNo_++;
            pageDirty_ = true;
            INJECT_POINT("ProducerImpl.SendImpl.postInsertSuccess");
            return DelayFlush();
        }
        Status status = HandleNoSpaceFromInsert(timeoutMs, rc);
        rcCode = status.GetCode();
        if (rcCode == K_SC_END_OF_PAGE || rcCode == K_NO_SPACE) {
            RETURN_IF_NOT_OK(DelayFlush(pageDirty_));  // Ensure we wake up reader before we move to a new page
            // If there is a next page, we follow the pointer. Otherwise, we send another rpc request.
            INJECT_POINT("client.Producer.beforeCheckNewPage",
                         [] { return inject::Set("SharedMemViewImpl.GetView", "abort"); });
            auto nextPage = writePage_->GetNextPage();
            if (nextPage.fd > 0) {
                RETURN_IF_NOT_OK(CreatePagePostProcessing(nextPage, fixPageFromNextPage_));
            } else {
                RETURN_IF_NOT_OK(
                    CreateWritePage(userTimeoutMs ? Optional<int64_t>(t.GetRemainingTimeMs()) : Optional<int64_t>()));
            }
            continue;
        } else if (rcCode == K_TRY_AGAIN) {  // If we can't get a lock on the page
            if (t.GetRemainingTimeMs() == 0) {
                break;
            }
            // See if there is a newer page to try.
            // Require to CreateWritePage if the cursor don't have a new page.
            RETURN_IF_NOT_OK(
                FixPage(userTimeoutMs ? Optional<int64_t>(t.GetRemainingTimeMs()) : Optional<int64_t>(), true));
            continue;
        }
        return rc;
    } while (true);
    RETURN_STATUS(K_OUT_OF_MEMORY,
                  FormatString("[%s] Producer unable to secure enough memory for the element within %zu ms",
                               LogPrefix(), timeoutMs));
}

Status ProducerImpl::Send(const Element &element, Optional<int64_t> userTimeoutMs)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    cursor_->IncrementRequestCount();
    RETURN_IF_NOT_OK(CheckNormalState());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        !userTimeoutMs || *userTimeoutMs >= 0, K_INVALID,
        FormatString("[%s] The send timeout must be greater than or equal to 0", LogPrefix()));
    DataVerificationHeader dataVerificationHeader;
    ElementHeader elementHeader;
    if (enableStreamDataVerification_) {
        dataVerificationHeader.Set(lastSendElementSeqNo_ + 1, senderProducerNo_, address_, port_);
        INJECT_POINT("DataVerificationOutOfOrder", [&dataVerificationHeader](int num) {
            dataVerificationHeader.hdr.seqNo += static_cast<DataVerificationHeader::SeqNo>(num);
            return Status::OK();
        });
        elementHeader.Set(reinterpret_cast<ElementHeader::Ptr>(&dataVerificationHeader),
                          dataVerificationHeader.HeaderSize(), DATA_VERIFICATION_HEADER);
    }
    HeaderAndData headerAndData(element, elementHeader, streamNo_);
    uint64_t finalElementSize = headerAndData.TotalSize();
    RaiiPlus raiiP;
    Status rc(K_UNKNOWN_ERROR, "To initialize an error status, no special meaning");
    if (enableSharedPage_) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamMetaShm_->TryIncUsage(finalElementSize),
                                         "Failed to increase the usage of shared memory for stream: " + streamName_);
        raiiP.AddTask([this, &rc, &finalElementSize]() {
            if (!rc) {
                LOG_IF_ERROR(streamMetaShm_->TryDecUsage(finalElementSize),
                             "Failed to decrease the usage of shared memory for stream: " + streamName_);
            }
        });
    }
    if (finalElementSize <= static_cast<uint64_t>(maxElementSize_)) {
        auto flag = InsertFlags::NONE;
        rc = SendImpl(headerAndData, flag, userTimeoutMs);
    } else {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            finalElementSize < maxStreamSize_, K_INVALID,
            FormatString("[%s] Element size must be smaller than Stream size. [element size, stream size] : [%zu, %zu]",
                         LogPrefix(), finalElementSize, maxStreamSize_));
        rc = InsertBigElement(headerAndData, userTimeoutMs);
    }
    if (rc) {
        raiiP.ClearAllTask();
    }
    if (sendTracer_.NeedWriteLog(rc.IsOk())) {
        LOG(INFO) << FormatString("[%s] Producer first send element with status %s", LogPrefix(), rc.ToString());
    }
    // A special return code K_OUT_OF_RANGE may indicate the worker has already restarted.
    // Go check the state.
    if (rc.GetCode() == K_OUT_OF_RANGE) {
        RETURN_IF_NOT_OK(CheckNormalState());
    }
    return rc;
}

Status ProducerImpl::DelayFlush(bool force)
{
    PerfPoint point(PerfKey::CLIENT_FLUSH_ELEMENT_ALL);
    RETURN_IF_NOT_OK(CheckNormalState());
    RETURN_OK_IF_TRUE(delayFlushTime_ == 0);
    // Synchronize with the timer queue which can be running at the same time
    std::unique_lock<std::mutex> flushLock(flushMutex_);
    RETURN_OK_IF_TRUE(!pageDirty_);
    if (force) {
        if (delayFlushTimer_) {
            TimerQueue::GetInstance()->Cancel(*delayFlushTimer_);
            delayFlushTimer_ = nullptr;
        }
        pageDirty_ = false;
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[Cursor %zu] Wake up consumers", cursor_->GetElementCount());
        return writePage_->WakeUpConsumers();
    }
    RETURN_OK_IF_TRUE(delayFlushTimer_ != nullptr);
    TimerQueue::TimerImpl timer;
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        delayFlushTime_,
        [w = this->weak_from_this()]() {
            INJECT_POINT_NO_RETURN("ProducerImpl.ExecFlush.sleep");
            // Producer may be deallocated, check if the producer still there through weak pointer.
            auto producerImpl = w.lock();
            if (producerImpl) {
                producerImpl->ExecFlush();
            }
        },
        timer));
    delayFlushTimer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
    return Status::OK();
}

Status ProducerImpl::Close()
{
    PerfPoint point(PerfKey::CLIENT_CLOSE_PRODUCER_ALL);
    // If producer is already closed, return OK
    RETURN_OK_IF_TRUE(state_ == State::CLOSE);
    // If the current state is RESET, it is allowed to close
    RETURN_IF_NOT_OK(CheckState());
    UnsetUnfixPageTimer();
    ExecAndCancelTimer();
    RETURN_IF_NOT_OK(UnfixPage());
    std::string logStr = FormatString("%zu Elements sent.", lastSendElementSeqNo_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi_->CloseProducer(streamName_, producerId_),
                                     FormatString("[%s] CloseProducer request error", LogPrefix()));
    RETURN_IF_NOT_OK(ChangeState(State::CLOSE));
    std::ostringstream oss;
    auto totalGetPageCall = fixPageFromNextPage_ + fixPageFromWorkArea_ + fixPageFromRpc_;
    oss << "[" << LogPrefix() << "] Close producer success! " << logStr << " Total get page count " << totalGetPageCall
        << ". rpc: " << fixPageFromRpc_.load() << ", work area: " << fixPageFromWorkArea_.load()
        << ", next pointer: " << fixPageFromNextPage_.load();
    LOG(INFO) << oss.str();
    return Status::OK();
}

Status ProducerImpl::SetStateToReset()
{
    RETURN_IF_NOT_OK(ChangeState(State::RESET));
    std::unique_lock<std::mutex> flushLock(flushMutex_);
    // Cancel any pending AutoFlush in the timer
    if (delayFlushTimer_ != nullptr) {
        TimerQueue::GetInstance()->Cancel(*delayFlushTimer_);
        // Set timer_ to nullptr after canceling it
        delayFlushTimer_ = nullptr;
    }
    return Status::OK();
}

Status ProducerImpl::Reset()
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        state_ == State::RESET, K_RUNTIME_ERROR,
        FormatString("[%s] The producer should be in reset state already to cleanup data and metadata", LogPrefix()));
    {
        std::unique_lock<std::mutex> flushLock(flushMutex_);  // Wait for in flight flush to finish
        if (delayFlushTimer_) {
            TimerQueue::GetInstance()->Cancel(*delayFlushTimer_);
        }
        delayFlushTimer_ = nullptr;
        pageDirty_ = false;
    }
    RETURN_IF_NOT_OK(UnfixPage());
    pageUnit_ = nullptr;
    return Status::OK();
}

Status ProducerImpl::Resume()
{
    return ChangeState(State::NORMAL);
}

Status ProducerImpl::CreateWritePage(Optional<int64_t> timeoutMs)
{
    INJECT_POINT("ProducerImpl.beforeCreateWritePage");
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::CLIENT_CREATE_WRITE_PAGE_ALL);
    RETURN_IF_NOT_OK(CheckNormalState());
    // If we have a page, decrement its count before we send out the rpc request
    if (writePage_) {
        RETURN_IF_NOT_OK(UnfixPage());
        writePage_ = nullptr;
        pageUnit_ = nullptr;
    }
    ShmView lastPageView;
    RETURN_IF_NOT_OK(workerApi_->CreateWritePage(streamName_, producerId_, timeoutMs ? timeoutMs.value() : 0, curView_,
                                                 lastPageView));
    INJECT_POINT("ProducerImpl.afterCreateWritePage");
    return CreatePagePostProcessing(lastPageView, fixPageFromRpc_);
}

Status ProducerImpl::CreatePagePostProcessing(const ShmView &lastPageView, std::atomic<int64_t> &fixPageCount)
{
    PerfPoint point(PerfKey::PAGE_POST_PROCESSING);
    if (writePage_ == nullptr || pageUnit_->fd != lastPageView.fd || pageUnit_->offset != lastPageView.off) {
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Next page ShmInfo: %s", LogPrefix(), lastPageView.ToStr());
        std::shared_ptr<client::MmapTableEntry> mmapEntry;
        RETURN_IF_NOT_OK(GetShmInfo(lastPageView, pageUnit_, mmapEntry));
        auto page = std::make_shared<StreamDataPage>(pageUnit_, lockId_, true, false, mmapEntry);
        RETURN_IF_NOT_OK(page->Init());
        RETURN_IF_NOT_OK(UnfixPage());
        writePage_ = std::move(page);
        RETURN_IF_NOT_OK(writePage_->RefPage(LogPrefix()));
        curView_ = writePage_->GetShmView();
        fixPageCount++;
        // We may (or may not) lock this page. But let's track it in the work area
        if (WorkAreaIsV2()) {
            RETURN_IF_NOT_OK(cursor_->SetLastLockedPage(curView_, DEFAULT_TIMEOUT_MS));
        }
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Acquire page id: %s, isSharedPage: %d, lastPageView: %s",
                                                LogPrefix(), writePage_->GetPageId(), writePage_->IsSharedPage(),
                                                lastPageView.ToStr());
    return Status::OK();
}

Status ProducerImpl::UnfixPage()
{
    RETURN_OK_IF_TRUE(writePage_ == nullptr);
    RETURN_IF_NOT_OK(writePage_->WakeUpConsumers());
    PerfPoint point(PerfKey::PAGE_UNFIX);
    RETURN_IF_NOT_OK(writePage_->ReleasePage(LogPrefix()));
    // Clear the entry in the work area since we don't have any lock on this page
    if (WorkAreaIsV2()) {
        RETURN_IF_NOT_OK(cursor_->SetLastLockedPage(ShmView(), DEFAULT_TIMEOUT_MS));
    }
    point.Record();
    writePage_.reset();
    return Status::OK();
}

Status ProducerImpl::GetLastPageView(ShmView &lastPageView, bool &switchToSharedPage)
{
    // Client will timeout when worker has crashed
    const uint64_t DEFAULT_TIMEOUT_MS = 1000;
    if (!enableSharedPage_) {
        return cursor_->GetLastPageView(lastPageView, DEFAULT_TIMEOUT_MS);
    }

    auto func = [this](const ShmView &lastPageRefView, std::shared_ptr<ShmUnitInfo> &shmUnitInfo,
                       std::shared_ptr<client::MmapTableEntry> &mmapEntry) {
        VLOG(SC_DEBUG_LOG_LEVEL) << LogPrefix() << " lastPageRefView:" << lastPageRefView.ToStr();
        return GetShmInfo(lastPageRefView, shmUnitInfo, mmapEntry);
    };
    return cursor_->GetLastPageViewByRef(lastPageView, switchToSharedPage, DEFAULT_TIMEOUT_MS, std::move(func));
}

Status ProducerImpl::FixPage(Optional<int64_t> timeoutMs, const bool requireNewPage)
{
    // Look into the work area is there is a page.
    // Otherwise, send a rpc request to the worker
    ShmView lastPageView;
    bool switchToSharedPage = false;
    RETURN_IF_NOT_OK(GetLastPageView(lastPageView, switchToSharedPage));
    if (requireNewPage && lastPageView == curView_) {
        // Request a new page from worker if the page does not changed.
        return CreateWritePage(timeoutMs);
    }
    if (lastPageView.fd > 0) {
        return CreatePagePostProcessing(lastPageView, fixPageFromWorkArea_);
    }
    // Check if the producer has already fixed a page and not switch to shared page.
    if (writePage_ && !switchToSharedPage) {
        return Status::OK();
    }
    return CreateWritePage(timeoutMs);
}

std::string ProducerImpl::LogPrefix() const
{
    return FormatString("S:%s, P:%s", streamName_, producerId_);
}

void ProducerImpl::ExecFlush()
{
    std::unique_lock<std::mutex> flushLock(flushMutex_);
    if (pageDirty_) {
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[Cursor %zu] Wake up consumers", cursor_->GetElementCount());
        pageDirty_ = false;
        if (writePage_ != nullptr) {
            writePage_->WakeUpConsumers();
        }
        delayFlushTimer_ = nullptr;
    }
}
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
