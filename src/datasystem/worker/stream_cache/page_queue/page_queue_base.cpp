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
 * Description: PageQueueBase
 */

#include <thread>
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/stream_cache/page_queue/page_queue_base.h"
#include "datasystem/worker/stream_cache/remote_worker_manager.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
PageQueueBase::PageQueueBase() : lastPage_(nullptr), usedMemBytes_(0), lastAckCursor_(0), nextCursor_(1)
{
}

uint64_t PageQueueBase::GetLastAppendCursor() const
{
    ReadLockHelper rlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    if (lastPage_ == nullptr) {
        return lastAckCursor_;
    }
    return lastPage_->GetLastCursor();
}

Status PageQueueBase::CreateOrGetLastDataPage(uint64_t timeoutMs, const ShmView &lastView,
                                              std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM)
{
    // This function can be called from two different threads.
    // Remote producers can call ExclusivePageQueue::BatchInsert and
    // local producers can call via rpc.
    // Any changes must ensure it is thread safe.
    RETURN_IF_NOT_OK(CreateOrGetLastDataPageImpl(timeoutMs, lastView, lastPage, retryOnOOM));
    return Status::OK();
}

Status PageQueueBase::CreateOrGetLastDataPageImpl(uint64_t timeoutMs, const ShmView &lastView,
                                                  std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM)
{
    WriteLockHelper xlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    // Deal with the easy case when the object is empty
    if (lastPage_ == nullptr) {
        RETURN_IF_NOT_OK(CreateNewPage(lastPage, retryOnOOM));
        auto nextCursor = nextCursor_.load(std::memory_order_relaxed);
        // Now we update the beginning cursor of the page
        __atomic_store_n(&lastPage->GetPageHeader()->begCursor_, nextCursor, __ATOMIC_SEQ_CST);
        // Add it to the chain
        WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
        idxChain_.emplace_back(nextCursor - 1, lastPage);
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Chain page<%s> [%zu, ) to index", LogPrefix(),
                                                    lastPage->GetPageId(), nextCursor);
        // Swap the page. Increment the ref count by 1.
        RETURN_IF_NOT_OK(lastPage->RefPage(FormatString("%s:%s", __FUNCTION__, __LINE__)));
        lastPage_ = lastPage;
        RETURN_IF_NOT_OK(VerifyLastPageRefCountNotLocked());
        // Update all local producers that a new page is added
        RETURN_IF_NOT_OK(UpdateLocalCursorLastDataPage(lastPage->GetShmView()));
        // Early exit
        return Status::OK();
    }
    // Update the lastPage_ because it can be stale due to the way producers can insert elements
    // past the lastPage_ into the logical free pages. A simpler way is append an empty free list with
    // no seal.
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    auto emptyPageList = Optional<std::list<PageShmUnit>>();
    RETURN_IF_NOT_OK(AppendFreePagesImplNotLocked(timeoutMs, emptyPageList, false));
    lastPage = lastPage_;
    // Now check if there is any free pages in the chain. If there is at least one, there is no
    // need to create any new page. We can just check what's next after the lastPage_.
    // We aren't going to lock this page to prevent any producer to insert.
    {
        ReadLockHelper rlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        RETURN_OK_IF_TRUE(!ackChain_.empty());
    }
    // Two producers (both local and remote) can see the page is full since they share the same page
    // and both sends create a new page request. We will serialize the requests and ensure we don't
    // create redundant new pages.
    // To tell the difference if it is a false alarm and a real request, each producer will tell
    // us the page info it currently has. If it is the same as our last page, we will create a new
    // one. Otherwise, we simply return our current last page.
    ShmView lastPageView = lastPage_->GetShmView();
    if (lastView != lastPageView) {
        // Producer's view is stale. Return our last page to this producer to try
        return Status::OK();
    }
    // At this point, we are going to create a new page. Due to the way we recycle used pages to
    // the chain, we are going to do the same but will seal the last page.
    std::shared_ptr<StreamDataPage> page;
    RETURN_IF_NOT_OK(CreateNewPage(page, retryOnOOM));
    std::list<PageShmUnit> freeList;
    freeList.emplace_back(0, std::move(page));
    auto freeListOptional = Optional<std::list<PageShmUnit>>(freeList);
    RETURN_IF_NOT_OK(AppendFreePagesImplNotLocked(timeoutMs, freeListOptional, true));
    lastPage = lastPage_;
    return Status::OK();
}

Status PageQueueBase::CreateNewPage(std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM)
{
    RETURN_IF_NOT_OK(VerifyWhenAlloc());
    auto pageSize = GetPageSize();
    // Check if there is a pending free page that we can steal
    {
        WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        if (!pendingFreePages_.empty()) {
            auto &list = std::get<K_FREE_LIST>(pendingFreePages_.front());
            lastPage = std::move(list.back());
            list.pop_back();
            pendingFreeBytes_ -= pageSize;
            if (list.empty()) {
                pendingFreePages_.pop_front();
            }
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Recycle Page<%s>", LogPrefix(), lastPage->GetPageId());
            // Make sure we clear the next page pointer. The code path ReclaimAckedChain can move a list
            // of free pages from ackChain which are chained to each other.
            RETURN_IF_NOT_OK(lastPage->GetSharedMemViewForNextPage()->Init(true));
            return Status::OK();
        }
    }
    std::shared_ptr<ShmUnitInfo> pageUnitInfo;
    // We now count metadata as part of the page size.
    RETURN_IF_NOT_OK(AllocMemory(pageSize, false, pageUnitInfo, retryOnOOM));
    // The lock id of worker is 0 by default.
    auto page = std::make_shared<StreamDataPage>(pageUnitInfo, 0, false, isSharedPage_);
    RETURN_IF_NOT_OK(page->Init());
    // Worker has the extra work to initialize the page
    RETURN_IF_NOT_OK(page->InitEmptyPage());
    lastPage = std::move(page);
    return Status::OK();
}

Status PageQueueBase::AllocMemory(size_t pageSize, bool bigElement, std::shared_ptr<ShmUnitInfo> &shmUnitInfo,
                                  bool retryOnOOM)
{
    PerfPoint point(PerfKey::PAGE_CREATE_NEW);
    auto pageUnit = std::make_unique<ShmUnit>();
    // maxStreamSize_ and pageSize_ are initialized to 0 in the constructor. They need to be set later
    // in order to make the CreateNewPage usable. This check ensures they are initialized properly.
    RETURN_IF_NOT_OK(VerifyWhenAlloc());
    {
        // Two threads can call AllocMemory at the same time and both think they have enough memory,
        // and can result in over exceeding the max stream size. So we will serialize the memory
        // allocation using the allocMutex_ lock.
        std::unique_lock<std::mutex> lock(allocMutex_);
        RETURN_IF_NOT_OK(CheckHadEnoughMem(pageSize));
        RETURN_IF_NOT_OK(AllocateMemoryImpl(pageSize, *pageUnit, retryOnOOM));
        usedMemBytes_ += pageSize;  // Track the used bytes now that the allocation is successful
    }
    // Create another ShmUnitInfo because the destructor of ShmUnit will release the memory
    auto pageUnitInfo = std::make_shared<ShmUnitInfo>();
    pageUnitInfo->fd = pageUnit->fd;
    pageUnitInfo->mmapSize = pageUnit->mmapSize;
    pageUnitInfo->size = pageUnit->size;
    pageUnitInfo->offset = pageUnit->offset;
    pageUnitInfo->pointer = pageUnit->pointer;
    auto page = std::make_shared<StreamLobPage>(pageUnitInfo, false);
    RETURN_IF_NOT_OK(page->Init());
    RETURN_IF_NOT_OK(AddPageToPool(page->GetPageId(), std::move(pageUnit), bigElement));
    if (bigElement) {
        scMetricBigPagesCreated_.fetch_add(1, std::memory_order_relaxed);
    } else {
        scMetricPagesCreated_.fetch_add(1, std::memory_order_relaxed);
    }
    LOG(INFO) << FormatString("[%s] Create shared memory page<%s> successful.", LogPrefix(), page->GetPageId());
    shmUnitInfo = pageUnitInfo;
    return Status::OK();
}

Status PageQueueBase::AddPageToPool(const ShmKey &pageId, std::unique_ptr<ShmUnit> &&pageUnit, bool bigElement)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    pageUnit->refCount = 1;  // The ref count is used in the case of BigElement page only.
    ShmPagesMap::accessor accessor;
    auto memInfo = std::make_shared<ShmMemInfo>();
    memInfo->pageUnit = std::move(pageUnit);
    memInfo->createTime = std::chrono::steady_clock::now();
    memInfo->bigElement = bigElement;
    bool success = shmPool_.emplace(accessor, pageId, std::move(memInfo));
    RETURN_OK_IF_TRUE(success);
    RETURN_STATUS(K_DUPLICATED, FormatString("[%s] Page %s already in the pool", LogPrefix(), pageId));
}

Status PageQueueBase::VerifyLastPageRefCountNotLocked() const
{
    auto refCount = lastPage_->GetRefCount();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(refCount > 1,  // at least 2
                                         K_RUNTIME_ERROR,
                                         FormatString("[%s] Unexpected ref count %zu on the last page<%s>", LogPrefix(),
                                                      refCount, lastPage_->GetPageId()));
    return Status::OK();
}

Status PageQueueBase::AppendFreePagesImplNotLocked(uint64_t timeoutMs,
                                                   Optional<std::list<PageShmUnit>> &optionalFreeList, bool seal,
                                                   const bool updateLocalPubLastPage)
{
    std::shared_ptr<StreamDataPage> lastPage;
    auto iter = idxChain_.begin();
    // Refresh lastPage_ if the producer(s) have reused some of the free pages in the ack chain
    RETURN_IF_NOT_OK_EXCEPT(RefreshLastPage(iter, lastPage), K_NOT_FOUND);
    RETURN_OK_IF_TRUE(lastPage == nullptr);
    if (optionalFreeList) {
        std::list<PageShmUnit> &freeList = optionalFreeList.value();
        WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        AddListToAckChain(freeList);
        if (!ackChain_.empty()) {
            // Link the next free page here to beginning of the ack chain.
            // We should lock the last page if any producer is trying to seal it at the same time.
            StreamPageLock pageLock(lastPage);
            RETURN_IF_NOT_OK(pageLock.Lock(timeoutMs));
            if (!lastPage->HasNextPage()) {
                auto &page = ackChain_.begin()->second;
                auto &smv = lastPage->GetSharedMemViewForNextPage();
                LOG_IF_ERROR(
                    smv->SetView(page->GetShmView(), true, std::numeric_limits<uint64_t>::max()),
                    FormatString("[%s] The page %s set next page view failed", LogPrefix(), lastPage->GetPageId()));
            }
        }
    }
    // If asked to seal the last page, make sure it is not empty and has a free page in the ack chain
    if (seal && !lastPage->Empty()) {
        WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        if (!ackChain_.empty()) {
            // Get a reference to the beginning of the free chain. It is possible a producer has already
            // sealed the page. So we will tolerate K_DUPLICATED return code.
            auto &ele = ackChain_.front();
            auto func = [&ele](const ShmView &v, std::shared_ptr<StreamDataPage> &out) {
                (void)v;
                out = ele.second;
                return Status::OK();
            };
            RETURN_IF_NOT_OK_EXCEPT(lastPage->Seal(ele.second->GetShmView(), timeoutMs, func, LogPrefix()),
                                    K_DUPLICATED);
            auto &page = ele.second;
            ele.first = page->GetBegCursor() - 1;  // Using the last cursor of lastPage as the key
            idxChain_.emplace_back(ele);
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Chain page<%s> [%zu, ) to index", LogPrefix(),
                                                        page->GetPageId(), page->GetBegCursor());
            nextCursor_.store(page->GetBegCursor(), std::memory_order_relaxed);
            std::swap(page, lastPage);
            iter = std::prev(idxChain_.end());
            ackChain_.pop_front();
        }
    }
    if (lastPage.get() != lastPage_.get()) {
        RETURN_IF_NOT_OK(VerifyLastPageRefCountNotLocked());
        RETURN_IF_NOT_OK(lastPage_->ReleasePage(FormatString("[%s] %s:%s", LogPrefix(), __FUNCTION__, __LINE__)));
        INJECT_POINT("AppendFreePagesImplNotLocked.sleep");
        // Swap the page. Increment the ref count by 1.
        RETURN_IF_NOT_OK(lastPage->RefPage(FormatString("[%s] %s:%s", LogPrefix(), __FUNCTION__, __LINE__)));
        lastPage_ = lastPage;
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Last page updated to %s. begCursor %zu", LogPrefix(),
                                                    lastPage_->GetPageId(), lastPage_->GetBegCursor());
        RETURN_IF_NOT_OK(VerifyLastPageRefCountNotLocked());
        // Update all local producers that a new page is added
        if (updateLocalPubLastPage) {
            RETURN_IF_NOT_OK(UpdateLocalCursorLastDataPage(lastPage->GetShmView()));
        }
    }
    return Status::OK();
}

Status PageQueueBase::RefreshLastPage(std::list<PageShmUnit>::iterator &out, std::shared_ptr<StreamDataPage> &lastPage)
{
    LogCursors();
    auto iter = idxChain_.begin();
    while (iter != idxChain_.end() && iter->second.get() != lastPage_.get()) {
        ++iter;
    }
    CHECK_FAIL_RETURN_STATUS(iter != idxChain_.end(), K_NOT_FOUND,
                             FormatString("[%s] Unable to locate lastPage_", LogPrefix()));
    lastPage = iter->second;
    // If the lastPage_ is sealed by the producers, move those pages that have been reused back to idx chain.
    while (lastPage->HasNextPage()) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            lastPage->GetBegCursor() > 0 && !lastPage->Empty(), K_OUT_OF_RANGE,
            FormatString("[%s] Invalid page<%s> begCursor %zu slotCount %zu", LogPrefix(), lastPage->GetPageId(),
                         lastPage->GetBegCursor(), lastPage->GetSlotCount()));
        uint64_t lastAppendCursor = lastPage->GetLastCursor();
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Page<%s> [%zu, %zu] is sealed by producer", LogPrefix(),
                                                    lastPage->GetPageId(), lastPage->GetBegCursor(), lastAppendCursor);
        // Previous run of this function may hit lock timeout while we are moving things from
        // ack chain to idx chain and leave the lastPage_ not pointing to the right place.
        // But it is easy to verify the position of the iterator.
        auto next = std::next(iter, 1);
        if (next != idxChain_.end()) {
            auto page = next->second;
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                lastAppendCursor + 1 == page->GetBegCursor(), K_OUT_OF_RANGE,
                FormatString("[%s] Expect page<%s> begCursor %zu but get %zu", LogPrefix(), page->GetPageId(),
                             lastAppendCursor + 1, page->GetBegCursor()));
            std::swap(page, lastPage);
            iter = next;
            continue;
        }
        WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        CHECK_FAIL_RETURN_STATUS(
            !ackChain_.empty(), K_NOT_FOUND,
            FormatString("[%s] Last page<%s> is sealed but there is no next page", LogPrefix(), lastPage->GetPageId()));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(lastAppendCursor + 1 == ackChain_.front().second->GetBegCursor(),
                                             K_OUT_OF_RANGE,
                                             FormatString("[%s] Expect page<%s> begCursor %zu but get %zu", LogPrefix(),
                                                          ackChain_.front().second->GetPageId(), lastAppendCursor + 1,
                                                          ackChain_.front().second->GetBegCursor()));
        auto ele = std::move(ackChain_.front());
        ackChain_.pop_front();
        ele.first = lastAppendCursor;
        auto page = ele.second;
        idxChain_.emplace_back(ele);
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Chain page<%s> [%zu, ) to index", LogPrefix(),
                                                    ele.second->GetPageId(), lastAppendCursor + 1);
        nextCursor_.store(lastAppendCursor + 1, std::memory_order_relaxed);
        std::swap(page, lastPage);
        iter = std::prev(idxChain_.end());
    }
    out = iter;
    return Status::OK();
}

Status PageQueueBase::VerifyWhenAlloc() const
{
    return Status::OK();
}

void PageQueueBase::LogCursors()
{
    uint64_t totalElements = 0;
    // Found from GetLastAppendCursor()
    totalElements = (lastPage_ != nullptr) ? lastPage_->GetLastCursor() : 0;

    const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
    LOG_EVERY_N(INFO, logPerCount) << FormatString("[%s] %d received elements, %d elements acked", LogPrefix(),
                                                   totalElements, lastAckCursor_);
}

void PageQueueBase::AddListToAckChain(std::list<PageShmUnit> &freeList)
{
    std::shared_ptr<StreamDataPage> prev;
    if (!ackChain_.empty()) {
        auto prevIt = std::prev(ackChain_.end());
        prev = prevIt->second;
    }
    auto iter = freeList.begin();
    while (iter != freeList.end()) {
        if (prev) {
            prev->GetSharedMemViewForNextPage()->SetView(iter->second->GetShmView(), true,
                                                         std::numeric_limits<uint64_t>::max());
        }
        ackChain_.emplace_back(0, iter->second);
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Chain page<%s> to free list", LogPrefix(),
                                                    iter->second->GetPageId());
        prev = iter->second;
        iter = freeList.erase(iter);
    }
}

Status PageQueueBase::Ack(uint64_t cursor, StreamMetaShm *streamMetaShm)
{
    static auto last = std::chrono::steady_clock::now();
    INJECT_POINT("ExclusivePageQueue.Ack.Start");
    std::list<PageShmUnit> freeList;
    // Producers can insert the elements past the lastPage_. We need it
    // updated before we process the Ack chain. The easiest way is pass
    // an empty freeList to AppendFreePages
    RETURN_IF_NOT_OK(AppendFreePages(freeList));
    // Now we traverse to Ack chain.
    std::vector<ShmView> bigElementPage;
    std::vector<Status> status;
    Status rc = AckImpl(cursor, freeList, bigElementPage, streamMetaShm);
    status.emplace_back(rc);
    if (freeList.empty() && bigElementPage.empty()) {
        // If nothing to reclaim. Periodically dump the pool stat. But not too much to flood the log
        const int dumpInterval = 10;
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last).count() >= dumpInterval) {
            last = now;
            DumpPoolPages(0);
        }
    }
    rc = ProcessBigElementPages(bigElementPage, streamMetaShm);
    status.emplace_back(rc);
    rc = ProcessAckedPages(cursor, freeList);
    status.emplace_back(rc);
    rc = AfterAck();
    status.emplace_back(rc);
    auto iter = std::find_if(status.begin(), status.end(), [](auto &kv) { return kv.IsError(); });
    if (iter != status.end()) {
        return (*iter);
    }
    return Status::OK();
}

Status PageQueueBase::AckImpl(uint64_t cursor, std::list<PageShmUnit> &freeList, std::vector<ShmView> &bigElementPages,
                              StreamMetaShm *streamMetaShm)
{
    PerfPoint point(PerfKey::MANAGER_ACK_CURSOR_GET_LOCK);
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    point.RecordAndReset(PerfKey::MANAGER_ACK_CURSOR_LOGIC);
    RETURN_OK_IF_TRUE(cursor <= lastAckCursor_);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Ack cursor %zu", LogPrefix(), cursor);
    auto it = idxChain_.begin();
    while (it != idxChain_.end()) {
        bool keepThisPageInChain = false;
        auto &page = it->second;
        auto id = page->GetPageId();
        auto slotCount = page->GetSlotCount();
        auto begCursor = page->GetBegCursor();
        auto lastCursor = it->first + slotCount;
        if (begCursor > cursor) {
            VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("[%s] Ack cursor %zu stops at page<%s> range [%zu, %zu)",
                                                     LogPrefix(), cursor, id, it->first + 1, lastCursor + 1);
            return Status::OK();
        }
        // For a data page, we can only wait until its reference count drops to 1 to recycle/release
        // the page. But that will take too long to release BigElement row. So we will browse through
        // the slot and release the BigElement memory prior to ack cursor
        RETURN_IF_NOT_OK(ReleaseBigElementsUpTo(cursor, page, bigElementPages, keepThisPageInChain));
        // Now we check the reference count. It is not 1, move on.
        // When a page is created, its reference count is 1. See StreamDataPage::InitEmptyPage().
        // Any worker/producer/client will increase/decrease the reference count for access.
        // Also, ExclusivePageQueue always maintains an additional reference to the last page.
        // If this page has some unfinished BigElement, move on to the next page regardless of the reference
        // count
        auto count = page->GetRefCount();
        if (count != 1 || keepThisPageInChain) {
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
                "[%s] Page<%s> pending ack. NextAckCursor = %zu. Reference count %d", LogPrefix(), id, cursor, count);
            // We can't free this page (yet) but move on to the next one.
            ++it;
            continue;
        }
        // it is the last page, won't do anything do it until the all producers/consumers are gone.
        auto nextIt = std::next(it, 1);
        if (nextIt == idxChain_.end()) {
            break;
        }
        if (lastCursor < cursor) {
            // Every row on this page has been processed by the clients.
            RETURN_IF_NOT_OK(page->WakeUpConsumers());
            // Move forward the lastAckCursor_, it is possible we still have a page with
            // begCursor < lastAckCursor_ due to reference count. So make sure we don't
            // move it backward.
            auto lastAckCursor = lastAckCursor_.load(std::memory_order_relaxed);
            lastAckCursor_.store(std::max(lastCursor, lastAckCursor), std::memory_order_relaxed);
            // Cache the free page
            auto size = page->GetTotalEleSize();
            freeList.emplace_back(std::move(*it));
            it = idxChain_.erase(it);
            if (streamMetaShm) {
                LOG_IF_ERROR(streamMetaShm->TryDecUsage(size), "TryDecUsage failed");
            }
            LogCursors();
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Ack page<%s> [%zu, %zu] success.", LogPrefix(), id,
                                                      begCursor, lastCursor);
            continue;
        } else {
            // begCursor <= cursor < lastCursor
            VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("[%s] Ack cursor %zu stops at page<%s> range [%zu, %zu)",
                                                     LogPrefix(), cursor, id, it->first + 1, lastCursor + 1);
            return Status::OK();
        }
    }
    return Status::OK();
}

Status PageQueueBase::ReleaseBigElementsUpTo(uint64_t cursor, std::shared_ptr<StreamDataPage> &page,
                                             std::vector<ShmView> &bigElementPages, bool &keepThisPageInChain)
{
    std::vector<std::pair<uint64_t, ShmView>> bigId;
    RETURN_IF_NOT_OK(page->ExtractBigElementsUpTo(cursor, bigId, false));
    // Pop (from the back) all those BigElement pages that are in still in use
    while (!bigId.empty()) {
        int32_t bigRefCount;
        auto pageInfo = std::make_shared<ShmUnitInfo>(ShmKey::Intern(""), bigId.back().second, nullptr);
        auto pageId = StreamPageBase::CreatePageId(pageInfo);
        RETURN_IF_NOT_OK(GetBigElementPageRefCount(pageId, bigRefCount));
        if (bigRefCount > 1) {
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] BigElementPage<%s> still in use", LogPrefix(), pageId);
            // We need to come back to this page to reclaim the big memory
            keepThisPageInChain = true;
            bigId.pop_back();
        } else {
            break;
        }
    }
    if (!bigId.empty()) {
        auto ackCursor = bigId.back().first;
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Release BigElement up to cursor %zu", LogPrefix(), ackCursor);
        bigId.clear();
        // Note: Here the attribute of the big element will be removed.
        RETURN_IF_NOT_OK(page->ExtractBigElementsUpTo(ackCursor, bigId, true));
        std::transform(bigId.begin(), bigId.end(), std::back_inserter(bigElementPages),
                       [](auto &kv) { return kv.second; });
    }
    return Status::OK();
}

Status PageQueueBase::GetBigElementPageRefCount(const ShmKey &pageId, int32_t &refCount)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    ShmPagesMap::const_accessor accessor;
    bool success = shmPool_.find(accessor, pageId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_NOT_FOUND,
                                         FormatString("[%s] Page<%s> not found", LogPrefix(), pageId));
    refCount = accessor->second->pageUnit->GetRefCount();
    return Status::OK();
}

Status PageQueueBase::AppendFreePages(std::list<PageShmUnit> &freeList, const bool updateLocalPubLastPage)
{
    // The page lock acquisition is not expected to fail in this case,
    // but there can be situation where the page and page lock need recovery.
    // In that case, idxMutex_ needs to be released for TryUnlockByLockId logic,
    // so we perform retry until success if error code is K_TRY_AGAIN from PageLock::Lock.
    // Note that lastPageMutex_ also needs to be released for the
    // CreateStreamManagerIfNotExist -> CreatePageZero code path deadlock.
    const int32_t RETRY_FOREVER = std::numeric_limits<int32_t>::max();
    auto freeListOptional = Optional<std::list<PageShmUnit>>(freeList);
    auto func = [this, &freeListOptional, updateLocalPubLastPage](int32_t) {
        // Block rpc and pause the idx and ack(in the correct order).
        WriteLockHelper xlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
        WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
        return AppendFreePagesImplNotLocked(DEFAULT_TIMEOUT_MS, freeListOptional, false, updateLocalPubLastPage);
    };
    RETURN_IF_NOT_OK(RetryOnError(RETRY_FOREVER, func, [] { return Status::OK(); }, { K_TRY_AGAIN }));
    return Status::OK();
}

void PageQueueBase::DumpPoolPages(int level) const
{
    // Sort (in reverse order) based on duration
    using second = std::chrono::duration<double, std::ratio<1>>;
    struct PageStrInfo {
        second key;
        std::string info;
    };
    struct Compare {
        bool operator()(const PageStrInfo &a, const PageStrInfo &b)
        {
            return a.key < b.key;
        }
    };
    std::priority_queue<PageStrInfo, std::vector<PageStrInfo>, Compare> bigEleQue;
    std::priority_queue<PageStrInfo, std::vector<PageStrInfo>, Compare> dataPageQue;
    std::ostringstream oss;
    size_t numRegularPages = 0;
    size_t numBigElements = 0;
    size_t totalDataPageSz = 0;
    size_t totalBigElementSz = 0;
    size_t poolSize = 0;

    WriteLockHelper xlock4(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    poolSize = shmPool_.size();
    auto now = std::chrono::steady_clock::now();
    for (auto &ele : shmPool_) {
        const auto &pageId = ele.first;
        auto &shmEleInfo = ele.second;
        if (shmEleInfo->bigElement) {
            ++numBigElements;
            totalBigElementSz += shmEleInfo->pageUnit->size;
            if (level >= SC_NORMAL_LOG_LEVEL) {
                auto key = std::chrono::duration_cast<second>(now - shmEleInfo->createTime);
                auto val = FormatString("[%s] Page<%s>, Duration: [%.6lf]s", LogPrefix(), pageId, key.count());
                auto info = PageStrInfo{ .key = key, .info = val };
                bigEleQue.emplace(std::move(info));
            }
        } else {
            ++numRegularPages;
            totalDataPageSz += GetPageSize();
            if (level >= SC_INTERNAL_LOG_LEVEL) {
                auto key = std::chrono::duration_cast<second>(now - shmEleInfo->createTime);
                auto val = FormatString("[%s] Page<%s>, Duration: [%.6lf]s", LogPrefix(), pageId, key.count());
                auto info = PageStrInfo{ .key = key, .info = val };
                dataPageQue.emplace(std::move(info));
            }
        }
    }

    // Let go of the lock. We have gathered what we need.
    xlock4.unlock();

    // Summary
    oss << FormatString(
        "[%s] Dump pool stat (v=%d). Size of pool: %zu. Number of data pages: %zu. Total size of data pages: %zu. "
        "Number of BigElement: %zu. Total size of BigElements: %zu\n",
        LogPrefix(), level, poolSize, numRegularPages, totalDataPageSz, numBigElements, totalBigElementSz);

    // Only dump the summary when v=0
    if (level < SC_NORMAL_LOG_LEVEL) {
        LOG(INFO) << oss.str();
        return;
    }

    auto func = [&oss](std::priority_queue<PageStrInfo, std::vector<PageStrInfo>, Compare> &que, size_t maxLines) {
        size_t i = 0;
        while (!que.empty()) {
            auto ele = que.top();
            que.pop();
            oss << FormatString("%s. seq:[%zu]\n", ele.info, ++i);
            if (i >= maxLines) {
                break;
            }
        }
    };

    // For v=1, dump the BigElements (page id only)
    if (level >= SC_NORMAL_LOG_LEVEL && numBigElements > 0) {
        oss << FormatString("[%s] Dump the first few BigElements (sort by duration)\n", LogPrefix());
        const size_t maxLines = 10;
        func(bigEleQue, level == SC_DEBUG_LOG_LEVEL ? numBigElements : maxLines);
    }

    // Dump the BigElements when v=1
    if (level < SC_INTERNAL_LOG_LEVEL) {
        LOG(INFO) << oss.str();
        return;
    }

    // Rest is v=2
    if (numRegularPages > 0) {
        oss << FormatString("[%s] Dump the first few data pages (sort by duration)\n", LogPrefix());
        const size_t maxLines = 5;
        func(dataPageQue, level == SC_DEBUG_LOG_LEVEL ? numRegularPages : maxLines);
    }

    LOG(INFO) << oss.str();
}

Status PageQueueBase::MoveUpLastPage(const bool updateLocalPubLastPage)
{
    std::list<PageShmUnit> freeList;
    // Producers can insert the elements past the lastPage_. We need it
    // updated before we process the Ack chain. The easiest way is pass
    // an empty freeList to AppendFreePages
    return AppendFreePages(freeList, updateLocalPubLastPage);
}

Status PageQueueBase::ReclaimAckedChain(uint64_t timeoutMs)
{
    // Block rpc and pause the idx and ack(in the correct order).
    WriteLockHelper xlock1(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    // Don't lock ack chain yet because we will call the function RefreshLastPage
    std::shared_ptr<StreamDataPage> lastPage;
    auto iter = idxChain_.begin();
    // Refresh lastPage_ if the producer(s) have reused some of the free pages in the ack chain
    RETURN_IF_NOT_OK_EXCEPT(RefreshLastPage(iter, lastPage), K_NOT_FOUND);
    RETURN_OK_IF_TRUE(lastPage == nullptr);
    // Now we can lock the ackChain
    WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    RETURN_OK_IF_TRUE(ackChain_.empty());
    // We have to be careful if we can take out a page from the ack chain
    // because any producer can freely traverse the chain. The only
    // way to block them is to lock the page.
    while (true) {
        StreamPageLock pageLock(lastPage);
        RETURN_IF_NOT_OK(pageLock.Lock(timeoutMs));
        ShmView nextPageView;
        std::shared_ptr<StreamDataPage> nextPage;
        bool isFreePage;
        RETURN_IF_NOT_OK(lastPage->GetSharedMemViewForNextPage()->GetView(nextPageView, isFreePage, timeoutMs));
        RETURN_OK_IF_TRUE(nextPageView.fd <= 0);
        RETURN_IF_NOT_OK(LocatePage(nextPageView, nextPage));
        if (!isFreePage) {
            std::swap(lastPage, nextPage);
            continue;
        }
        // We have the current page locked (all producers are blocked) and there is a pointer to
        // a free page. We can't free this free page but rather the one(s) follow it because
        // other producers may already spot the existence of this page and is trying to seal
        // (and wait on our page lock).
        iter = ackChain_.begin();
        while (iter != ackChain_.end()) {
            ShmView v = iter->second->GetShmView();
            if (v != nextPageView) {
                ++iter;
                continue;
            }
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Keeping Page<%s> in the free list", LogPrefix(),
                                                      nextPage->GetPageId());
            // As mentioned before, we can't free this one, and it is too dangerous.
            // But we will free the rest.
            ++iter;
            break;
        }
        RETURN_OK_IF_TRUE(iter == ackChain_.end());
        // Two more things to do.
        // Clear the next pointer
        nextPage->GetSharedMemViewForNextPage()->SetView(ShmView(), false, std::numeric_limits<uint64_t>::max());
        // Split the chain starting from iter
        std::list<PageShmUnit> freeList;
        freeList.splice(freeList.end(), ackChain_, iter, ackChain_.end());
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Move %zu pages out from ack chain", LogPrefix(),
                                                  freeList.size());
        return MoveFreeListToPendFree(0, freeList);
    }
    return Status::OK();
}

Status PageQueueBase::ReleaseMemory(const ShmView &pageView)
{
    std::shared_ptr<ShmUnitInfo> pageInfo;
    RETURN_IF_NOT_OK(LocatePage(pageView, pageInfo));
    auto bigElementPage = std::make_shared<StreamLobPage>(pageInfo, true);
    RETURN_IF_NOT_OK(bigElementPage->Init());
    LOG(INFO) << FormatString("[%s] Release big element page<%s>", LogPrefix(), bigElementPage->GetPageId());
    std::vector<ShmKey> list;
    list.push_back(bigElementPage->GetPageId());
    return FreePages(list, true);
}

void PageQueueBase::TryUnlockByLockId(uint32_t lockId)
{
    // This form of recovery is obsolete except for down level client.
    // The problem of this logic is the locks are acquired in the wrong
    // order (which can lead to deadlock). The page can be locked
    // due to client crash but the logic is trying to acquire idxMutex_
    // which is the opposite order of other code path where the idxMutex_
    // is locked first, and then the page lock. A better method is to
    // use the cursor_ info.
    WriteLockHelper xlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    auto it = idxChain_.begin();
    while (it != idxChain_.end()) {
        it->second->TryUnlockByLockId(lockId);
        ++it;
    }
}

void PageQueueBase::ForceUnlockMemViemForPages(uint32_t lockId)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    for (auto &ele : shmPool_) {
        if (ele.second->bigElement) {
            continue;
        }
        VLOG(1) << FormatString("Try unlock for stream: %s, page id: %s, lockId: %zu", LogPrefix(), ele.first, lockId);
        auto &pageUnit = ele.second->pageUnit;
        auto pageUnitInfo = std::make_shared<ShmUnitInfo>();
        pageUnitInfo->fd = pageUnit->fd;
        pageUnitInfo->mmapSize = pageUnit->mmapSize;
        pageUnitInfo->size = pageUnit->size;
        pageUnitInfo->offset = pageUnit->offset;
        pageUnitInfo->pointer = pageUnit->pointer;

        auto page = std::make_shared<StreamDataPage>(pageUnitInfo, WORKER_LOCK_ID, false, isSharedPage_);
        auto rc = page->Init();
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("%s, PageId: %s, page init failed: %s", LogPrefix(), ele.first, rc.ToString());
            continue;
        }

        auto &smv = page->GetSharedMemViewForNextPage();
        if (smv != nullptr) {
            auto msg = FormatString("%s, PageId: %s", LogPrefix(), ele.first);
            LOG_IF_ERROR(smv->ForceUnLock(lockId, msg), "ForceUnLock for page failed");
        }
    }
}

Status PageQueueBase::ProcessBigElementPages(std::vector<ShmView> &bigElementId, StreamMetaShm *streamMetaShm)
{
    RETURN_OK_IF_TRUE(bigElementId.empty());
    std::vector<ShmKey> freeList;
    auto func = [this, &freeList](const ShmView &v) {
        std::shared_ptr<ShmUnitInfo> shmInfo;
        RETURN_IF_NOT_OK(LocatePage(v, shmInfo));
        auto bigElementPage = std::make_shared<StreamLobPage>(shmInfo, true);
        RETURN_IF_NOT_OK(bigElementPage->Init());
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Release big element page<%s>", LogPrefix(),
                                                  bigElementPage->GetPageId());
        freeList.emplace_back(bigElementPage->GetPageId());
        return Status::OK();
    };
    for (auto &v : bigElementId) {
        (void)func(v);
    }
    return FreePages(freeList, true, streamMetaShm);
}

Status PageQueueBase::LocatePage(const ShmView &v, std::shared_ptr<ShmUnitInfo> &out)
{
    auto pageInfo = std::make_shared<ShmUnitInfo>(ShmKey::Intern(""), v, nullptr);
    auto pageId = StreamPageBase::CreatePageId(pageInfo);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    ShmPagesMap::accessor accessor;
    bool exist = shmPool_.find(accessor, pageId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(exist, K_NOT_FOUND,
                                         FormatString("[%s] Page %s not found", LogPrefix(), pageId));
    pageInfo->pointer = accessor->second->pageUnit->pointer;
    out = std::move(pageInfo);
    return Status::OK();
}

std::pair<size_t, bool> PageQueueBase::GetNextBlockedRequestSize()
{
    return std::make_pair(0, false);
}

Status PageQueueBase::ProcessAckedPages(uint64_t cursor, std::list<PageShmUnit> &freeList)
{
    // Some locking orders to consider
    // StreamManager::UnblockCreators can hold this locks in this order
    // (a) StreamManager::streamManagerBlockedListsMutex_
    // (b) Call StreamManager::AllocBigShmMemory
    // (c) Call ExclusivePageQueue::ReclaimAckedChain
    // (d) Hold ExclusivePageQueue::lastPageMutex_
    // (e) Hold ExclusivePageQueue::idxChain_;
    // (f) Wait for ExclusivePageQueue::ackMutex_

    // So we need to follow the same order.
    // Find out the next request's size to determine if we should cache or free

    size_t nextReqSz;
    bool bigElement;
    std::tie(nextReqSz, bigElement) = GetNextBlockedRequestSize();
    {
        WriteLockHelper xlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
        RETURN_OK_IF_TRUE(freeList.empty() && pendingFreePages_.empty());
        // Clear all pages back to empty
        for (auto &e : freeList) {
            RETURN_IF_NOT_OK(e.second->ResetToEmpty());
        }
        if (bigElement && (CheckHadEnoughMem(nextReqSz).GetCode() == K_OUT_OF_MEMORY)
            && (!freeList.empty() || !pendingFreePages_.empty())) {
            // We have a BigElement in the next request, and don't have enough stream memory to serve it.
            // We will stop the caching, and free as much as we can.
            LOG(WARNING) << FormatString(
                "[%s] Not enough stream memory to handle BigElement %zu bytes request, used %zu", LogPrefix(),
                nextReqSz, usedMemBytes_);
            return MoveFreeListToPendFree(cursor, freeList);
        }
        // The size of the Ack chain should <= FLAGS_sc_cache_pages.
        // That said, we may have reserved more pages than FLAGS_sc_cache_pages in the function
        // ReserveStreamMemory and we will then not touch anything already in the ackChain.
        // Before this function is called, we have already moved as many 'recycled' pages
        // back to the idx chain as possible. The producers may still continue to exhaust
        // the ack chain but we shouldn't block them just to maintain FLAGS_sc_cache_pages.
        // So the producers may end up sending rpc to ask for more free pages.
        // The value of FLAGS_sc_cache_pages should be carefully chosen.
        const size_t chainLength = ackChain_.size();
        const size_t sizeToKeep = std::max<size_t>(FLAGS_sc_cache_pages, 0);
        std::list<PageShmUnit> pendingList;
        while (chainLength + freeList.size() > sizeToKeep) {
            if (freeList.empty()) {
                break;
            }
            auto ele = std::move(freeList.back());
            freeList.pop_back();
            pendingList.emplace_back(std::move(ele));
        }
        // Return the memory back to the pool. We aren't going to free them right away
        // due to some racing condition between the worker and the producer. We only
        // free ack pages that are acked a while ago. That should give enough time
        // for the producers to move away the ack pages.
        if (!pendingList.empty() || !pendingFreePages_.empty()) {
            RETURN_IF_NOT_OK(MoveFreeListToPendFree(cursor, pendingList));
        }
    }
    // Link them to the chain as 'logical' page.
    // We're still calling the function even though the freeList can be empty.
    // This is to continue the rest of the flow to unblock producers and creator.
    // Also, we can do one more final update to the idx chain.
    return AppendFreePages(freeList);
}

Status PageQueueBase::FreePages(std::vector<ShmKey> &pages, bool bigElementPage, StreamMetaShm *streamMetaShm)
{
    PerfPoint point(PerfKey::PAGE_RELEASE);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    auto b4 = usedMemBytes_.load(std::memory_order_relaxed);
    size_t totalReleased = 0;
    while (!pages.empty()) {
        auto pageId = std::move(pages.back());
        pages.pop_back();
        ShmPagesMap::accessor accessor;
        bool exist = shmPool_.find(accessor, pageId);
        if (!exist) {
            LOG(ERROR) << FormatString("[%s] Page %s not found", LogPrefix(), pageId);
            continue;
        }
        auto &pageUnit = accessor->second->pageUnit;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            bigElementPage == accessor->second->bigElement, K_RUNTIME_ERROR,
            FormatString("[%s, Page<%s>] BigElement attribute doesn't match.", LogPrefix(), pageId));
        size_t memSizeToRelease = bigElementPage ? pageUnit->size : GetPageSize();
        RETURN_IF_NOT_OK(pageUnit->FreeMemory());
        if (bigElementPage) {
            if (streamMetaShm) {
                LOG_IF_ERROR(streamMetaShm->TryDecUsage(memSizeToRelease), "TryDecUsage failed");
            }
            scMetricBigPagesReleased_.fetch_add(1, std::memory_order_relaxed);
        } else {
            scMetricPagesReleased_.fetch_add(1, std::memory_order_relaxed);
        }
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Page<%s> is released", LogPrefix(), pageId);
        shmPool_.erase(accessor);
        usedMemBytes_ -= memSizeToRelease;
        totalReleased += memSizeToRelease;
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Total memory release %zu. Latest usedMemBytes %zu", LogPrefix(),
                                              totalReleased, b4 - totalReleased);
    return Status::OK();
}

Status PageQueueBase::MoveFreeListToPendFree(uint64_t cursor, std::list<PageShmUnit> &freeList)
{
    RETURN_IF_NOT_OK(FreePendingList());
    std::vector<std::shared_ptr<StreamDataPage>> pagesToFree;
    while (!freeList.empty()) {
        auto ele = std::move(freeList.back());
        freeList.pop_back();
        pagesToFree.push_back(std::move(ele.second));
    }
    if (!pagesToFree.empty()) {
        auto now = std::chrono::steady_clock::now();
        pendingFreeBytes_ += pagesToFree.size() * GetPageSize();
        auto ele = std::make_tuple(cursor, now, std::move(pagesToFree));
        pendingFreePages_.emplace_back(std::move(ele));
    }
    return Status::OK();
}

Status PageQueueBase::FreePendingList()
{
    // Return the memory back to the pool. We aren't going to free them right away
    // due to some racing condition between the worker and the producer. We only
    // free ack pages that are acked a while ago. That should give enough time
    // for the producers to move away the ack pages.
    auto now = std::chrono::steady_clock::now();
    if (!pendingFreePages_.empty()) {
        std::chrono::time_point<std::chrono::steady_clock> start;
        uint64_t begCursor;
        std::tie(begCursor, start, std::ignore) = pendingFreePages_.front();
        const int interval = 12;
        if (std::chrono::duration_cast<std::chrono::seconds>(now - start).count() >= interval) {
            auto ele = std::move(pendingFreePages_.front());
            pendingFreePages_.pop_front();
            std::vector<ShmKey> freePages;
            auto &list = std::get<K_FREE_LIST>(ele);
            std::transform(list.begin(), list.end(), std::back_inserter(freePages),
                           [](const auto &kv) { return kv->GetPageId(); });
            RETURN_IF_NOT_OK(FreePages(freePages));
            pendingFreeBytes_ -= list.size() * GetPageSize();
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Free pages from cursor %zu ack", LogPrefix(), begCursor);
        }
    }
    return Status::OK();
}

Status PageQueueBase::LocatePage(const ShmView &v, std::shared_ptr<StreamDataPage> &out)
{
    std::shared_ptr<ShmUnitInfo> pageInfo;
    RETURN_IF_NOT_OK(LocatePage(v, pageInfo));
    auto page = std::make_shared<StreamDataPage>(pageInfo, 0, false, isSharedPage_);
    RETURN_IF_NOT_OK(page->Init());
    out = std::move(page);
    return Status::OK();
}

ShmView PageQueueBase::GetLastPageShmView()
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(lastPageMutex_));
    if (lastPage_) {
        return lastPage_->GetShmView();
    }
    return ShmView();
}

Status PageQueueBase::LocatePage(uint64_t lastAppendCursor, std::shared_ptr<StreamDataPage> &out, bool incRef)
{
    // We are going to lock both chains in the correct order to avoid deadlock.
    ReadLockHelper rlock2(STREAM_COMMON_LOCK_ARGS(idxMutex_));
    ReadLockHelper rlock3(STREAM_COMMON_LOCK_ARGS(ackMutex_));
    // If both chains are empty, return K_NOT_FOUND
    if (idxChain_.empty() && ackChain_.empty()) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("[%s] Stream index is empty.", LogPrefix()));
    }
    // Now we want to locate the page that contains '1 + lastAppendCursor'
    // The way we build the index is using the cursor of the last slot of *previous* page.
    // Here is the tricky part. Producers can insert past the idxChain and continue onto
    // the ackChain to recycle the pages.
    CHECK_FAIL_RETURN_STATUS(lastAppendCursor >= lastAckCursor_, K_NOT_FOUND, "Page has been released already");
    auto funcName = __FUNCTION__;
    auto func = [this, incRef, &out, &funcName](const std::shared_ptr<StreamDataPage> &page) {
        // If asked to increase the reference, do it while we are holding the idx and ack chain mutex
        if (incRef) {
            RETURN_IF_NOT_OK(page->RefPage(FormatString("[%s] %s:%s", LogPrefix(), funcName, __LINE__)));
        }
        out = page;
        return Status::OK();
    };
    // We will start with the idxChain, then continue to the ackChain
    std::shared_ptr<StreamDataPage> cur = nullptr;
    std::shared_ptr<StreamDataPage> prev = nullptr;
    auto it = idxChain_.begin();
    while (it != idxChain_.end()) {
        uint64_t endCursor = it->first;
        cur = it->second;
        auto lastCursor = endCursor + cur->GetSlotCount();
        if (endCursor < lastCursor && lastCursor <= lastAckCursor_) {
            // This page is pending released. Move onto
            // the next one. No need to update prev.
            ++it;
            continue;
        }
        // Loop invariant:
        // endCursor < Every cursor on it->second
        if (lastAppendCursor == endCursor) {
            // cur is what we are looking for because
            // the begCursor of the page is 1+lastAppendCursor
            return func(cur);
        } else if (lastAppendCursor < endCursor) {
            // prev is what we are looking for unless it is null
            if (prev == nullptr) {
                RETURN_STATUS(K_NOT_FOUND,
                              FormatString("[%s] cursor %zu has been ack'ed already", LogPrefix(), lastAppendCursor));
            }
            return func(prev);
        } else {
            prev = cur;
            ++it;
        }
    }
    // If we get here, we have to continue onto the ack chain which however we can't use link
    // and must follow the next pointer on the page instead.
    CHECK_FAIL_RETURN_STATUS(prev != nullptr, K_NOT_FOUND,
                             FormatString("[%s, cursor %zu] prev is null.", LogPrefix(), lastAppendCursor));
    while (prev->HasNextPage()) {
        uint64_t endCursor = prev->GetLastCursor();
        // The logic is similar
        if (lastAppendCursor < endCursor) {
            return func(prev);
        }
        ShmView v = prev->GetNextPage();
        RETURN_IF_NOT_OK(LocatePage(v, cur));
        if (lastAppendCursor == endCursor) {
            // cur is what we are looking for because
            // the begCursor of the page is 1+lastAppendCursor
            return func(cur);
        }
        prev = cur;
    }
    // We do not have to increase the reference count. This call is made for the purpose of
    // consumers (both local and remote) and we won't ack any page consumers are still reading.
    return func(cur);
}

Status PageQueueBase::ScanAndEval(uint64_t &lastAckCursor, uint64_t timeoutMs,
                                  const std::vector<std::string> &remoteWorkers, ScanFlags flag)
{
    // By design, lastAckCursor_ is always on page boundary. Ensure lastAckCursor is within range.
    CHECK_FAIL_RETURN_STATUS(lastAckCursor_ <= lastAckCursor, K_INVALID,
                             FormatString("[%s] lastAckCursor [%zu] is invalid. Stream has been reclaimed to %zu",
                                          LogPrefix(), lastAckCursor, lastAckCursor_));
    auto funcName = __FUNCTION__;
    do {
        INJECT_POINT("ExclusivePageQueue.ScanAndEval.wait");
        std::shared_ptr<StreamDataPage> lastPage;
        // Usually we don't increase the reference for consumer because only us can ack the page.
        // But the page we get back isn't the page that contains the lastAckCursor+1, and it can
        // be the last page of the idxChain
        RETURN_IF_NOT_OK(LocatePage(lastAckCursor, lastPage, true));
        RETURN_RUNTIME_ERROR_IF_NULL(lastPage);
        const std::string logPrefix = FormatString("%s Page:%s", LogPrefix(), lastPage->GetPageId());
        Raii unfix([&lastPage, logPrefix, &funcName]() {
            // We asked to fix the page with an extra reference count.
            LOG_IF_ERROR(lastPage->ReleasePage(FormatString("[%s] %s:%s", logPrefix, funcName, __LINE__)),
                         "Page unfix");
        });
        std::vector<DataElement> dirtyElements;
        Status rc = lastPage->Receive(lastAckCursor, timeoutMs, dirtyElements);
        if (rc.GetCode() == K_SC_END_OF_PAGE) {
            // Ensure lastAckCursor is the last cursor on this page
            auto numElements = lastPage->GetSlotCount();
            auto begCursor = lastPage->GetBegCursor();
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                lastAckCursor + 1 == begCursor + numElements, K_OUT_OF_RANGE,
                FormatString("[%s] LastAppendCursor mismatch. lastAckCursor %zu, begCursor %zu, numSlots %zu",
                             logPrefix, lastAckCursor, begCursor, numElements));
            // New dirty data on the next page. Early exit if pageBreak is set
            if (TESTFLAG(flag, ScanFlags::PAGE_BREAK)) {
                return rc;
            }
            continue;
        }
        // Another possible error is K_TRY_AGAIN. Basically it means no new elements are detected
        // since the last check. No need to log error for this case
        if (rc.GetCode() == K_TRY_AGAIN) {
            return rc;
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("[S:%s]", logPrefix));
        RETURN_OK_IF_TRUE(dirtyElements.empty());
        // Ensure that the producer will not write new data
        RequestCounter::GetInstance().ResetLastArrivalTime("ExclusivePageQueue::ScanAndEval");
        INJECT_POINT("ExclusivePageQueue.ScanAndEval");
        uint64_t nextAppendCursor = lastAckCursor + dirtyElements.size();
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Eval cursor [%zu, %zu)", logPrefix, lastAckCursor + 1,
                                                  nextAppendCursor + 1);
        // Pass the dirty elements to the destination
        for (const auto &remoteWorker : remoteWorkers) {
            RETURN_IF_NOT_OK(
                SendElements(lastPage, lastAckCursor + 1, nextAppendCursor + 1, remoteWorker, dirtyElements));
        }
        // Return the next append cursor
        lastAckCursor = nextAppendCursor;
        if (TESTFLAG(flag, ScanFlags::EVAL_BREAK)) {
            break;
        }
    } while (true);
    return Status::OK();
}

Status PageQueueBase::SendElements(const std::shared_ptr<StreamDataPage> &page, uint64_t begCursor, uint64_t endCursor,
                                   const std::string &remoteWorker, std::vector<DataElement> recvElements)
{
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[RW:%s, %s] Flush element cursor:[%zu, %zu) PageId: %s", remoteWorker,
                                                LogPrefix(), begCursor, endCursor, page->GetPageId());
    // Filter out elements that are from remote producers. We should not send them again remotely.
    CHECK_FAIL_RETURN_STATUS(
        endCursor - begCursor == recvElements.size(), K_OUT_OF_RANGE,
        FormatString("[RW:%s, %s] Range not match. begCursor = %zu, endCursor = %zu, vector size = %zu", remoteWorker,
                     LogPrefix(), begCursor, endCursor, recvElements.size()));
    auto *remoteWorkerManager = GetRemoteWorkerManager();
    // Elements are written in reverse order. But we can still pack them together as long
    // as the receiving worker knows how to walk the payload.
    auto iter = recvElements.begin();
    while (iter != recvElements.end()) {
        std::shared_ptr<SendElementView> elementView;
        RETURN_IF_NOT_OK(SendElementView::CreateSendElementView(page, remoteWorker, *iter, SharedFromThis(),
                                                                remoteWorkerManager, elementView));
        ++iter;
        // BigElement should be sent in its own PV. Otherwise, pack element of the same nature in one PV.
        while (iter != recvElements.end()) {
            if (!elementView->PackDataElement(*iter, false, remoteWorkerManager)) {
                break;
            }
            ++iter;
        }
        // Pass the PV to RWM
        RETURN_IF_NOT_OK(remoteWorkerManager->SendElementsView(elementView));
    }
    return Status::OK();
}

Status PageQueueBase::IncBigElementPageRefCount(const ShmKey &pageId)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    ShmPagesMap::accessor accessor;
    bool success = shmPool_.find(accessor, pageId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_NOT_FOUND,
                                         FormatString("[%s] Page<%s> not found", LogPrefix(), pageId));
    auto &pageUnit = accessor->second->pageUnit;
    pageUnit->IncrementRefCount();
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] BigElement page<%s> ref count %zu", LogPrefix(), pageId,
                                                pageUnit->refCount);
    return Status::OK();
}

Status PageQueueBase::ExtractBigElement(DataElement &ele, std::shared_ptr<StreamLobPage> &bigElementPage)
{
    // Double check it is a big element. But we can simply tolerate it.
    RETURN_OK_IF_TRUE(!ele.IsBigElement());
    ShmView v;
    RETURN_IF_NOT_OK(StreamDataPage::ParseShmViewPb(ele.ptr, ele.size, v));
    std::shared_ptr<ShmUnitInfo> pageInfo;
    RETURN_IF_NOT_OK(LocatePage(v, pageInfo));
    auto page = std::make_shared<StreamLobPage>(pageInfo, false);
    RETURN_IF_NOT_OK(page->Init());
    // Replace the original pointer with the big element pointer
    ele.ptr = reinterpret_cast<uint8_t *>(page->GetPointer());
    ele.size = page->PageSize();
    bigElementPage = std::move(page);
    return Status::OK();
}

Status PageQueueBase::DecBigElementPageRefCount(const ShmKey &pageId)
{
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    ShmPagesMap::accessor accessor;
    bool success = shmPool_.find(accessor, pageId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_NOT_FOUND,
                                         FormatString("[%s] Page<%s> not found", LogPrefix(), pageId));
    auto &pageUnit = accessor->second->pageUnit;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        pageUnit->refCount >= 1, K_OUT_OF_RANGE,
        FormatString("[%s] Page<%s> ref count %zu unexpected", LogPrefix(), pageId, pageUnit->refCount));
    pageUnit->DecrementRefCount();
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] BigElement page<%s> ref count %zu", LogPrefix(), pageId,
                                                pageUnit->refCount);
    return Status::OK();
}

Status PageQueueBase::UpdatePageRefIfExist(const ShmView &v, const std::string &logPrefix, const bool toggle)
{
    auto pageInfo = std::make_shared<ShmUnitInfo>(ShmKey::Intern(""), v, nullptr);
    auto pageId = StreamPageBase::CreatePageId(pageInfo);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(poolMutex_));
    ShmPagesMap::accessor accessor;
    bool exist = shmPool_.find(accessor, pageId);
    if (!exist) {
        const std::string errMsg = FormatString("[%s] Page %s not found", LogPrefix(), pageId);
        LOG(INFO) << errMsg;
        return { K_NOT_FOUND, errMsg };
    }
    pageInfo->pointer = accessor->second->pageUnit->pointer;
    auto page = std::make_shared<StreamDataPage>(pageInfo, 0, false, isSharedPage_);
    RETURN_IF_NOT_OK(page->Init());
    if (toggle) {
        return page->RefPage(logPrefix);
    } else {
        return page->ReleasePage(logPrefix);
    }
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
