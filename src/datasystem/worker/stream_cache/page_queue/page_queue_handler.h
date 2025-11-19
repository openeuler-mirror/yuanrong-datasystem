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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_PAGE_QUEUE_HANDLER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_PAGE_QUEUE_HANDLER_H

#include <memory>
#include <shared_mutex>

#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/stream_cache/stream_meta_shm.h"
#include "datasystem/utils/optional.h"
#include "datasystem/worker/stream_cache/page_queue/shared_page_queue.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class StreamManager;
class PageQueueHandler {
public:
    PageQueueHandler(StreamManager *mgr, Optional<StreamFields> cfg);
    ~PageQueueHandler() = default;

    static std::shared_ptr<ExclusivePageQueue> CreateExclusivePageQueue(StreamManager *mgr, Optional<StreamFields> cfg);

    auto GetExclusivePageQueue() const
    {
        return exclusivePageQueue_;
    }

    std::string GetSharedPageQueueId() const;

    bool ExistsSharedPageQueue() const;

    Status CreateOrGetLastDataPage(uint64_t timeoutMs, const ShmView &lastView,
                                   std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM);

    /**
     * @brief A shared memory work area that is shared between this worker and the producer/consumer
     * @param[in] id ProducerId or ConsumerId
     * @param[in] isProducer Add cursor for producer.
     * @param[out] out shared work area
     * @param[out] view ShmView to access this work area
     * @return Status
     */
    Status AddCursor(const std::string &id, bool isProducer, std::shared_ptr<Cursor> &out, ShmView &view);

    /**
     * @brief Delete a shared memory work area
     * @param[in] id ProducerId or ConsumerId
     * @return Status
     */
    Status DeleteCursor(const std::string &id);

    /**
     * @brief Crash recovery for lost client to unlock by cursor.
     * @param[in] cursorId The cursorId.
     * @param[in] isProducer Ture for producer.
     * @param[in] lockId The lock id.
     */
    void ForceUnlockByCursor(const std::string &cursorId, bool isProducer, uint32_t lockId);

    /**
     * @brief Crash recovery for lost client to unlock by cursor.
     * @param[in] cursorId The cursorId.
     * @param[in] lockId The lock id.
     * @param[out] fallback whether need fallback to old logic.
     * @return Status of this call.
     */
    Status ForceUnlockByCursorImpl(const std::string &cursorId, uint32_t lockId, bool &fallback);

    /**
     * @brief Unlock mem view on all pages for this stream.
     * @param[in] streams The stream name list.
     * @param[in] lockId The lock id.
     */
    void ForceUnlockMemViemForPages(uint32_t lockId);

    /**
     * Crash recovery based on lockId and producerId
     * @param[in] lockId The lock id.
     */
    void TryUnlockByLockId(uint32_t lockId);

    /**
     * @brief Force to update the last page location
     * @param[in] updateLocalPubLastPage If local producers need to get an update of the lastest last page.
     * @return Status object
     */
    Status MoveUpLastPage(const bool updateLocalPubLastPage = true);

    Status AllocMemory(size_t pageSz, bool bigElement, std::shared_ptr<ShmUnitInfo> &pageUnitInfo, bool retryOnOOM);

    Status ReclaimAckedChain(uint64_t timeoutMs);

    Status ReleaseMemory(const ShmView &pageView);

    void DumpPoolPages(int level) const;

    size_t GetPageSize() const;

    void SetSharedPageQueue(std::shared_ptr<SharedPageQueue> &sharedPageQueue);

    /**
     * @brief Get or create shm meta.
     * @param[in] tenantId The ID of tenant.
     * @param[out] view The view of shm meta.
     * @return Status of the call.
     */
    Status GetOrCreateShmMeta(const std::string &tenantId, ShmView &view);

    /**
     * @brief Try to decrease the usage of shared memory in this node for this stream.
     * @param[in] size The size to be increased.
     * @return Status of the call.
     */
    Status TryDecUsage(uint64_t size)
    {
        std::shared_lock<std::shared_timed_mutex> lck(streamMetaShmMux_);
        return streamMetaShm_ ? streamMetaShm_->TryDecUsage(size) : Status::OK();
    }

    /**
     * @brief Get stream meta shm.
     * @return The pointer to stream meta shm.
     */
    StreamMetaShm *GetStreamMetaShm()
    {
        std::shared_lock<std::shared_timed_mutex> lck(streamMetaShmMux_);
        return streamMetaShm_ ? streamMetaShm_.get() : nullptr;
    }

    /**
     * @brief Verifies the input stream fields match the existing setting.
     * If the existing settings are uninitialized, updates the values.
     * @param[in] streamFields The stream fields with page size and max stream size to check
     * @return Status of the call.
     */
    Status UpdateStreamFields(const StreamFields &streamFields);

private:
    std::string LogPrefix() const;
    Status LocatePage(const ShmView &v, std::shared_ptr<StreamDataPage> &out);
    Status UpdateLocalCursorLastDataPage(const ShmView &shmView);

    std::string streamName_;
    std::atomic<bool> enableSharedPage_;
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<ExclusivePageQueue> exclusivePageQueue_;
    std::shared_ptr<SharedPageQueue> sharedPageQueue_;

    std::shared_timed_mutex streamMetaShmMux_;
    std::unique_ptr<ShmUnit> shmUnitOfStreamMeta_;
    std::unique_ptr<StreamMetaShm> streamMetaShm_;
    const uint64_t streamMetaShmSize_ = 64;

    // WorkArea(s) for communicating with producers/consumers.
    mutable std::shared_timed_mutex cursorMutex_;
    struct CursorInfo {
        std::unique_ptr<ShmUnit> shmUnit;
        std::shared_ptr<Cursor> cursor;
        std::unique_ptr<ShmUnit> lastPageRefShmUnit;
        std::shared_ptr<SharedMemViewImpl> lastPageRefShmViewImpl;
    };
    std::unordered_map<std::string, std::unique_ptr<CursorInfo>> cursorMap_;
    std::deque<std::unique_ptr<ShmUnit>> cacheCursor_;
    std::deque<std::unique_ptr<ShmUnit>> cacheLastPageRef_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif
