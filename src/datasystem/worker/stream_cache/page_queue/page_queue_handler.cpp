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
 * Description: PageQueueHandler
 */

#include "datasystem/worker/stream_cache/page_queue/page_queue_handler.h"
#include <mutex>
#include <shared_mutex>

#include "datasystem/common/constants.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/stream_cache/page_queue/exclusive_page_queue.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
PageQueueHandler::PageQueueHandler(StreamManager *mgr, Optional<StreamFields> cfg)
{
    exclusivePageQueue_ = CreateExclusivePageQueue(mgr, cfg);
    if (exclusivePageQueue_) {
        exclusivePageQueue_->RegisterUpdateLastDataPageHandler(
            [this](const ShmView &shmView) { return UpdateLocalCursorLastDataPage(shmView); });
    }
    enableSharedPage_ = cfg && StreamManager::EnableSharedPage(cfg->streamMode_);
    if (mgr != nullptr) {
        streamName_ = mgr->GetStreamName();
    }
}

std::shared_ptr<ExclusivePageQueue> PageQueueHandler::CreateExclusivePageQueue(StreamManager *mgr,
                                                                               Optional<StreamFields> cfg)
{
    return std::make_shared<ExclusivePageQueue>(mgr, cfg);
}

Status PageQueueHandler::UpdateStreamFields(const StreamFields &streamFields)
{
    RETURN_IF_NOT_OK(exclusivePageQueue_->UpdateStreamFields(streamFields));
    enableSharedPage_ = StreamManager::EnableSharedPage(streamFields.streamMode_);
    return Status::OK();
}

Status PageQueueHandler::CreateOrGetLastDataPage(uint64_t timeoutMs, const ShmView &lastView,
                                                 std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            return sharedPageQueue_->CreateOrGetLastDataPage(timeoutMs, lastView, lastPage, retryOnOOM);
        }
    }
    return exclusivePageQueue_->CreateOrGetLastDataPage(timeoutMs, lastView, lastPage, retryOnOOM);
}

bool PageQueueHandler::ExistsSharedPageQueue() const
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    return sharedPageQueue_ != nullptr;
}

std::string PageQueueHandler::LogPrefix() const
{
    return FormatString("S:%s", streamName_);
}

Status PageQueueHandler::AddCursor(const std::string &id, bool isProducer, std::shared_ptr<Cursor> &out, ShmView &view)
{
    auto lastPageShmView = exclusivePageQueue_->GetLastPageShmView();
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(cursorMutex_));
    INJECT_POINT("worker.AddCursor.afterLockCursorMutex");
    CHECK_FAIL_RETURN_STATUS(cursorMap_.find(id) == cursorMap_.end(), K_DUPLICATED,
                             FormatString("[%s Add:%s] already created", streamName_, id));
    auto func = [this, id](std::deque<std::unique_ptr<ShmUnit>> &cache, size_t sz, std::unique_ptr<ShmUnit> &shmUnit) {
        if (cache.empty()) {
            shmUnit = std::make_unique<ShmUnit>();
            shmUnit->SetHardFreeMemory();
            std::string tenantId = TenantAuthManager::ExtractTenantId(streamName_);
            RETURN_IF_NOT_OK(shmUnit->AllocateMemory(tenantId, sz, false, ServiceType::STREAM));
        } else {
            shmUnit = std::move(cache.front());
            cache.pop_front();
        }
        auto rc = memset_s(shmUnit->GetPointer(), sz, 0, sz);
        CHECK_FAIL_RETURN_STATUS(rc == 0, K_RUNTIME_ERROR,
                                 FormatString("[S:%s, Add:%s] Memset to 0 results in errno %d", streamName_, id, rc));
        return Status::OK();
    };
    std::unique_ptr<ShmUnit> shmUnit;
    const size_t cursorSize = Cursor::K_CURSOR_SIZE_V2;
    RETURN_IF_NOT_OK(func(cacheCursor_, cursorSize, shmUnit));

    auto cursor = std::make_shared<Cursor>(shmUnit->GetPointer(), cursorSize, WORKER_LOCK_ID);
    RETURN_IF_NOT_OK(cursor->Init());
    RETURN_IF_NOT_OK(cursor->SetWorkerVersion(Cursor::K_WORKER_EYECATCHER_V1));

    // last page ref
    std::unique_ptr<ShmUnit> lastPageRefShmUnit;
    std::shared_ptr<SharedMemViewImpl> lastPageRefShmViewImpl;
    if (enableSharedPage_ && isProducer) {
        ShmView lastPageRefShmView;
        bool usingSharedPageQueue;
        {
            std::shared_lock<std::shared_timed_mutex> locker(mutex_);
            usingSharedPageQueue = sharedPageQueue_ != nullptr;
        }
        if (usingSharedPageQueue) {
            RETURN_IF_NOT_OK(sharedPageQueue_->GetOrCreateLastPageRef(lastPageRefShmView, lastPageRefShmViewImpl));
        } else {
            const size_t lastPageRefSize = sizeof(SharedMemView);
            RETURN_IF_NOT_OK(func(cacheLastPageRef_, lastPageRefSize, lastPageRefShmUnit));
            lastPageRefShmView = lastPageRefShmUnit->GetShmView();
            lastPageRefShmViewImpl =
                std::make_shared<SharedMemViewImpl>(lastPageRefShmUnit->GetPointer(), lastPageRefSize, WORKER_LOCK_ID);
            RETURN_IF_NOT_OK(lastPageRefShmViewImpl->Init(false));
        }

        RETURN_IF_NOT_OK(
            cursor->SetLastPageRef(lastPageRefShmView, std::numeric_limits<uint64_t>::max(), usingSharedPageQueue));
        LOG(INFO) << FormatString("[%s, Add:%s] Update the last page ref to %s", LogPrefix(), id,
                                  lastPageRefShmView.ToStr());
    } else {
        RETURN_IF_NOT_OK(cursor->SetLastPage(lastPageShmView, std::numeric_limits<uint64_t>::max()));
        LOG(INFO) << FormatString("[%s, Add:%s] Update the last page to %s", LogPrefix(), id, lastPageShmView.ToStr());
    }

    out = cursor;
    view = shmUnit->GetShmView();
    // Add them to the cursor map
    auto cInfo = std::make_unique<CursorInfo>();
    cInfo->shmUnit = std::move(shmUnit);
    cInfo->cursor = std::move(cursor);
    cInfo->lastPageRefShmUnit = std::move(lastPageRefShmUnit);
    cInfo->lastPageRefShmViewImpl = std::move(lastPageRefShmViewImpl);
    (void)cursorMap_.emplace(id, std::move(cInfo));
    LOG(INFO) << FormatString("[%s, Add:%s] Cursor added. Number of cursors %zu", streamName_, id, cursorMap_.size());
    return Status::OK();
}

Status PageQueueHandler::DeleteCursor(const std::string &id)
{
    constexpr static size_t maxCache = 5;
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(cursorMutex_));
    auto iter = cursorMap_.find(id);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(iter != cursorMap_.end(), K_NOT_FOUND,
                                         FormatString("[%s] cursor for %s not found", streamName_, id));
    std::unique_ptr<CursorInfo> cInfo = std::move(iter->second);
    cursorMap_.erase(iter);
    if (cacheCursor_.size() < maxCache) {
        cacheCursor_.emplace_back(std::move(cInfo->shmUnit));
    }
    if (cacheLastPageRef_.size() < maxCache && cInfo->lastPageRefShmUnit != nullptr) {
        cacheLastPageRef_.emplace_back(std::move(cInfo->lastPageRefShmUnit));
    }
    LOG(INFO) << FormatString("[%s, Delete:%s] Cursor removed. Number of cursors %zu", streamName_, id,
                              cursorMap_.size());
    return Status::OK();
}

void PageQueueHandler::ForceUnlockByCursor(const std::string &cursorId, bool isProducer, uint32_t lockId)
{
    bool fallback = false;
    LOG_IF_ERROR(ForceUnlockByCursorImpl(cursorId, lockId, fallback),
                 FormatString("TryForceUnlockImpl for %s %s and lockId %zu failed",
                              (isProducer ? "producer" : "consumer"), cursorId, lockId));
    if (isProducer && fallback) {
        LOG(INFO) << FormatString("[%s, P:%s] Switch to use V1 client recovery logic.", LogPrefix(), cursorId);
        TryUnlockByLockId(lockId);
    }
}

Status PageQueueHandler::ForceUnlockByCursorImpl(const std::string &cursorId, uint32_t lockId, bool &fallback)
{
    WriteLockHelper xlock(STREAM_COMMON_LOCK_ARGS(cursorMutex_));
    auto iter = cursorMap_.find(cursorId);
    if (iter == cursorMap_.end() || iter->second == nullptr) {
        return Status::OK();
    }

    // Only V2 client will update the last locked page field. If it is a V1 client, then
    // we will fall back to the old method.
    auto &cursor = iter->second->cursor;
    if (cursor->GetClientVersion() < Cursor::K_CURSOR_SIZE_V2) {
        fallback = true;
        return Status::OK();
    }

    LOG(INFO) << FormatString("[%s, cursorId:%s] V2 client detected.", LogPrefix(), cursorId);
    auto unlock = [this, &cursor, lockId, &cursorId]() {
        // unlock SharedMemView in Cursor
        auto msg = FormatString("%s cursorId:%s", LogPrefix(), cursorId);
        LOG_IF_ERROR(cursor->ForceUnLock(lockId, LogPrefix()), "Cursor ForceUnLock failed");
        // Get the last page (potentially) locked by this producer.
        ShmView view;
        RETURN_IF_NOT_OK(cursor->GetLastLockedPageView(view, DEFAULT_TIMEOUT_MS));
        LOG(INFO) << FormatString("[%s, cursorId:%s] Last locked page<%s>", LogPrefix(), cursorId, view.ToStr());
        RETURN_OK_IF_TRUE(view == ShmView());  // No page is locked
        // Get the page
        std::shared_ptr<StreamDataPage> page;
        RETURN_IF_NOT_OK(LocatePage(view, page));
        LOG(INFO) << FormatString("[%s, cursorId:%s] Unlock page<%s>", LogPrefix(), cursorId, page->GetPageId());
        page->TryUnlockByLockId(lockId);
        return Status::OK();
    };
    auto status = unlock();
    fallback = status.IsError();
    return status;
}

void PageQueueHandler::TryUnlockByLockId(uint32_t lockId)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            sharedPageQueue_->TryUnlockByLockId(lockId);
        }
    }
    exclusivePageQueue_->TryUnlockByLockId(lockId);
}

void PageQueueHandler::ForceUnlockMemViemForPages(uint32_t lockId)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            sharedPageQueue_->ForceUnlockMemViemForPages(lockId);
        }
    }
    exclusivePageQueue_->ForceUnlockMemViemForPages(lockId);
}

Status PageQueueHandler::LocatePage(const ShmView &v, std::shared_ptr<StreamDataPage> &out)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            auto rc = sharedPageQueue_->LocatePage(v, out);
            // continue find from exclusive page queue if not exists.
            if (rc.IsOk() || rc.GetCode() != K_NOT_FOUND) {
                return rc;
            }
        }
    }
    return exclusivePageQueue_->LocatePage(v, out);
}

Status PageQueueHandler::UpdateLocalCursorLastDataPage(const ShmView &shmView)
{
    INJECT_POINT("worker.UpdateLocalCursorLastDataPage.beforeLockCursorMutex");
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(cursorMutex_));
    if (!enableSharedPage_) {
        for (auto &ele : cursorMap_) {
            // if a SetLastPage errors out go to next one
            // When crash handling happens, we will clean this up by deleting pub
            INJECT_POINT("UpdateLocalPubLastDataPage.skip");
            WARN_IF_ERROR(ele.second->cursor->SetLastPage(shmView, DEFAULT_TIMEOUT_MS),
                          FormatString("[%s] UpdateLocalCursorLastDataPage error", LogPrefix()));
        }
        return Status::OK();
    }
    for (auto &ele : cursorMap_) {
        if (ele.second->lastPageRefShmViewImpl == nullptr) {
            LOG(WARNING) << LogPrefix() << " lastPageRefShmViewImpl is nullptr";
            continue;
        }
        WARN_IF_ERROR(ele.second->lastPageRefShmViewImpl->SetView(shmView, false, DEFAULT_TIMEOUT_MS),
                      FormatString("[%s] UpdateLocalCursorLastDataPage error", LogPrefix()));
    }
    return Status::OK();
}

Status PageQueueHandler::MoveUpLastPage(const bool updateLocalPubLastPage)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            return sharedPageQueue_->MoveUpLastPage(updateLocalPubLastPage);
        }
    }
    return exclusivePageQueue_->MoveUpLastPage(updateLocalPubLastPage);
}

Status PageQueueHandler::AllocMemory(size_t pageSz, bool bigElement, std::shared_ptr<ShmUnitInfo> &pageUnitInfo,
                                     bool retryOnOOM)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            return sharedPageQueue_->AllocMemory(pageSz, bigElement, pageUnitInfo, retryOnOOM);
        }
    }
    return exclusivePageQueue_->AllocMemory(pageSz, bigElement, pageUnitInfo, retryOnOOM);
}

Status PageQueueHandler::ReclaimAckedChain(uint64_t timeoutMs)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            return sharedPageQueue_->ReclaimAckedChain(timeoutMs);
        }
    }
    return exclusivePageQueue_->ReclaimAckedChain(timeoutMs);
}

Status PageQueueHandler::ReleaseMemory(const ShmView &pageView)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            // the memory alloc from exclusive page queue,
            // but release after switch to share page queue.
            auto rc = sharedPageQueue_->ReleaseMemory(pageView);
            if (rc.GetCode() != K_NOT_FOUND) {
                return rc;
            }
        }
    }
    return exclusivePageQueue_->ReleaseMemory(pageView);
}

void PageQueueHandler::DumpPoolPages(int level) const
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            sharedPageQueue_->DumpPoolPages(level);
            return;
        }
    }
    exclusivePageQueue_->DumpPoolPages(level);
}

size_t PageQueueHandler::GetPageSize() const
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        if (sharedPageQueue_ != nullptr) {
            return sharedPageQueue_->GetPageSize();
        }
    }
    return exclusivePageQueue_->GetPageSize();
}

std::string PageQueueHandler::GetSharedPageQueueId() const
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    if (sharedPageQueue_ != nullptr) {
        return sharedPageQueue_->GetPageQueueId();
    }
    return "";
}

void PageQueueHandler::SetSharedPageQueue(std::shared_ptr<SharedPageQueue> &sharedPageQueue)
{
    LOG(INFO) << LogPrefix() << " update SharedPageQueue to " << sharedPageQueue->GetPageQueueId();
    {
        ShmView lastPageRefShmView;
        std::shared_ptr<SharedMemViewImpl> lastPageRefShmViewImpl;
        auto shmView = sharedPageQueue->GetOrCreateLastPageRef(lastPageRefShmView, lastPageRefShmViewImpl);
        ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(cursorMutex_));
        for (auto &ele : cursorMap_) {
            LOG_IF_ERROR(ele.second->cursor->SetLastPageRef(lastPageRefShmView, DEFAULT_TIMEOUT_MS, true),
                         FormatString("%s update last page ref failed.", LogPrefix()));
        }
    }
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    sharedPageQueue_ = sharedPageQueue;
}

Status PageQueueHandler::GetOrCreateShmMeta(const std::string &tenantId, ShmView &view)
{
    {
        std::shared_lock<std::shared_timed_mutex> lck(streamMetaShmMux_);
        if (shmUnitOfStreamMeta_) {
            view = shmUnitOfStreamMeta_->GetShmView();
            return Status::OK();
        }
    }
    {
        std::lock_guard<std::shared_timed_mutex> lck(streamMetaShmMux_);
        if (shmUnitOfStreamMeta_) {
            view = shmUnitOfStreamMeta_->GetShmView();
            return Status::OK();
        }
        auto shmUnitOfStreamMeta = std::make_unique<ShmUnit>();
        RETURN_IF_NOT_OK(shmUnitOfStreamMeta->AllocateMemory(tenantId, streamMetaShmSize_, false, ServiceType::STREAM));
        auto rc = memset_s(shmUnitOfStreamMeta->GetPointer(), streamMetaShmSize_, 0, streamMetaShmSize_);
        CHECK_FAIL_RETURN_STATUS(rc == 0, K_RUNTIME_ERROR, FormatString("Memset to 0 results in errno %d", rc));
        StreamFields streamFields;
        exclusivePageQueue_->GetStreamFields(streamFields);
        streamMetaShm_ = std::make_unique<StreamMetaShm>(streamName_, shmUnitOfStreamMeta->GetPointer(),
                                                         streamMetaShmSize_, streamFields.maxStreamSize_);
        RETURN_IF_NOT_OK(streamMetaShm_->Init());
        view = shmUnitOfStreamMeta->GetShmView();
        shmUnitOfStreamMeta_ = std::move(shmUnitOfStreamMeta);
    }
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
