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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_PAGE_QUEUE_BASE_H
#define DATASYSTEM_WORKER_STREAM_CACHE_PAGE_QUEUE_BASE_H

#include <chrono>
#include <list>
#include <memory>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/string_intern/string_ref.h"

DS_DECLARE_uint32(sc_cache_pages);

#define STREAM_COMMON_LOCK_ARGS(lockname)                                               \
    (lockname), [this, funName = __FUNCTION__] {                                        \
        return FormatString("%s %s, %s:%s", LogPrefix(), #lockname, funName, __LINE__); \
    }

namespace datasystem {
namespace worker {
namespace stream_cache {
class RemoteWorkerManager;
enum class ScanFlags : uint32_t { NONE = 0, SCAN_LOCK = 1u, PAGE_BREAK = 1u << 1, EVAL_BREAK = 1u << 2 };
ENABLE_BITMASK_ENUM_OPS(ScanFlags);

class PageQueueBase {
public:
    struct ShmMemInfo {
        std::unique_ptr<ShmUnit> pageUnit;
        std::chrono::time_point<std::chrono::steady_clock> createTime;
        bool bigElement;
    };
    using ShmPagesMap = tbb::concurrent_hash_map<ShmKey, std::shared_ptr<ShmMemInfo>>;
    using PageShmUnit = std::pair<uint64_t, std::shared_ptr<StreamDataPage>>;

    PageQueueBase();
    virtual ~PageQueueBase() = default;

    /**
     * @brief Create or Get the last data page
     * @param timeoutMs in millisecond
     * @param lastView ShmView of the caller's last page (if any)
     * @param lastPage output
     * @param retryOnOOM Retry on OOM if true.
     * @return OK if successful
     * @note If the caller's lastView is out dated, we will return the last page
     *       rather than creating a new one. Only the lastView matches the
     *       current last page, we will create a new page
     */
    Status CreateOrGetLastDataPage(uint64_t timeoutMs, const ShmView &lastView,
                                   std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM);

    /**
     * @brief Ack to see if any streamPageView already consumed by all and can be erased.
     * @param[in] cursor advanced ack cursor.
     * @param[in] streamMetaShm The pointer to streamMetaShm
     * @return K_OK on success; the error code otherwise.
     */
    Status Ack(uint64_t cursor, StreamMetaShm *streamMetaShm = nullptr);

    /**
     * @brief Locate a page that contains lastAppendCursor + 1
     * @param lastAppendCursor
     * @param out
     * @return OK if found
     */
    Status LocatePage(uint64_t lastAppendCursor, std::shared_ptr<StreamDataPage> &out, bool incRef = false);

    /**
     * @brief Allocate memory and append to PageQueue.
     * @param[in] pageSz The pageSize.
     * @param[in] bigElement Is big element or not.
     * @param[out] pageUnitInfo The shm page info.
     * @param[in] retryOnOOM Retry on OOM if true.
     * @return Status of this call
     */
    Status AllocMemory(size_t pageSz, bool bigElement, std::shared_ptr<ShmUnitInfo> &pageUnitInfo, bool retryOnOOM);

    /**
     * @brief Scan data from page queue and send to remote workers.
     * @param[in/out] lastAckCursor The last ack cursor.
     * @param[in] timeoutMs The scan timeout.
     * @param[in] remoteWorkers The remote worker address.
     * @param[in] flag The scan flags.
     * @return Status of this call
     */
    Status ScanAndEval(uint64_t &lastAckCursor, uint64_t timeoutMs, const std::vector<std::string> &remoteWorkers,
                       ScanFlags flag);

    /**
     * Dump memory pool to log for diagnostic
     */
    void DumpPoolPages(int level) const;

    /**
     * @brief Force to update the last page location
     * @param[in] updateLocalPubLastPage If local producers need to get an update of the lastest last page.
     * @return Status object
     */
    Status MoveUpLastPage(const bool updateLocalPubLastPage = true);

    Status ReclaimAckedChain(uint64_t timeoutMs);

    Status ReleaseMemory(const ShmView &pageView);

    /**
     * Crash recovery based on lockId
     * @param lockId
     */
    void TryUnlockByLockId(uint32_t lockId);

    /**
     * @brief Crash recovery for lost client to unlock mem view current page queue.
     * @param[in] lockId The lock id.
     */
    void ForceUnlockMemViemForPages(uint32_t lockId);

    /**
     * @brief Get the StreamDataPage by ShmView.
     * @param[in] v The ShmView instance.
     * @param[out] out The StreamDataPage instance.
     * @return Status of this call.
     */
    Status LocatePage(const ShmView &v, std::shared_ptr<StreamDataPage> &out);

    /**
     * @brief Get the ShmView of the last page
     * @return The ShmView of the last page.
     */
    ShmView GetLastPageShmView();

public:
    /**
     * @brief Debugging log prefix
     * @return the log profex.
     */
    virtual std::string LogPrefix() const = 0;

    /**
     * @brief Gets the page size
     * @return The page size value
     */
    virtual size_t GetPageSize() const = 0;

    /**
     * @brief Verify when alloc memory.
     * @return Status of this call.
     */
    virtual Status VerifyWhenAlloc() const;

    /**
     * @brief Check if it had enough memory for this stream.
     * @param[in] memSize The memory size.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status CheckHadEnoughMem(size_t memSize) = 0;

    /**
     * @brief The implement of allocate shared memory for stream cache.
     * @param[in] memSizeNeeded The page size.
     * @param[out] shmUnit The share unit instance.
     * @return Status of this call.
     */
    virtual Status AllocateMemoryImpl(size_t memSizeNeeded, ShmUnit &shmUnit, bool retryOnOOM) = 0;

    /**
     * @brief Update cursor to notify the last page changed.
     * @param[in] shmView The ShmView for the last page.
     * @return Status of this call
     */
    virtual Status UpdateLocalCursorLastDataPage(const ShmView &shmView) = 0;

    /**
     * @brief Call after ack finish.
     * @return Status of this call
     */
    virtual Status AfterAck() = 0;

    /**
     * @brief Get encryptStream of streamFields_, and apply sanity checks.
     * @return true if stream data encryption is applicable.
     */
    virtual bool IsEncryptStream(const std::string &streamName) const = 0;

    /**
     * @brief Get the stream name or the shared page identifier.
     * @return Stream name or the shared page identifier.
     */
    virtual std::string GetStreamName() const = 0;

    /**
     * @brief Get remote worker manager.
     * @return Raw ptr to remote worker manager.
     */
    virtual RemoteWorkerManager *GetRemoteWorkerManager() const = 0;

    virtual Status IncBigElementPageRefCount(const ShmKey &pageId);
    virtual Status ExtractBigElement(DataElement &ele, std::shared_ptr<StreamLobPage> &bigElementPage);
    virtual Status DecBigElementPageRefCount(const ShmKey &pageId);
    virtual Status UpdatePageRefIfExist(const ShmView &v, const std::string &logPrefix, bool toggle);

    /**
     * @brief Get the blocked request size info.
     * @return the blocked request size info
     */
    virtual std::pair<size_t, bool> GetNextBlockedRequestSize();

    /**
     * @brief Send element to remote worker.
     * @param[in] page The data page for the element
     * @param[in] begCursor The begin cursor
     * @param[in] endCursor The end cursor
     * @param[in] remoteWorker The remote worker address.
     * @param[in] recvElements The element need send to remote worker
     * @return Status of this call
     */
    virtual Status SendElements(const std::shared_ptr<StreamDataPage> &page, uint64_t begCursor, uint64_t endCursor,
                                const std::string &remoteWorker, std::vector<DataElement> recvElements);

    /**
     * @brief Getter of lastAppendCursor_.
     * @return lastAppendCursor_.
     */
    [[nodiscard]] uint64_t GetLastAppendCursor() const;

protected:
    virtual std::shared_ptr<PageQueueBase> SharedFromThis() = 0;
    void LogCursors();
    Status CreateOrGetLastDataPageImpl(uint64_t timeoutSec, const ShmView &lastView,
                                       std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM);

    Status CreateNewPage(std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM);
    Status AddPageToPool(const ShmKey &pageId, std::unique_ptr<ShmUnit> &&pageUnit, bool bigElement);
    Status VerifyLastPageRefCountNotLocked() const;
    Status AppendFreePagesImplNotLocked(uint64_t timeoutMs, Optional<std::list<PageShmUnit>> &freeList, bool seal,
                                        const bool updateLocalPubLastPage = true);
    Status RefreshLastPage(std::list<PageShmUnit>::iterator &iter, std::shared_ptr<StreamDataPage> &lastPage);
    void AddListToAckChain(std::list<PageShmUnit> &freeList);
    Status AckImpl(uint64_t cursor, std::list<PageShmUnit> &freeList, std::vector<ShmView> &bigElementPage,
                   StreamMetaShm *streamMetaShm = nullptr);
    Status ReleaseBigElementsUpTo(uint64_t cursor, std::shared_ptr<StreamDataPage> &page,
                                  std::vector<ShmView> &bigElementPages, bool &keepThisPageInChain);
    Status GetBigElementPageRefCount(const ShmKey &pageId, int32_t &refCount);
    Status AppendFreePages(std::list<PageShmUnit> &freeList, const bool updateLocalPubLastPage = true);
    Status ProcessBigElementPages(std::vector<ShmView> &bigElementId, StreamMetaShm *streamMetaShm);
    Status LocatePage(const ShmView &v, std::shared_ptr<ShmUnitInfo> &out);
    Status ProcessAckedPages(uint64_t cursor, std::list<PageShmUnit> freeList);
    Status FreePages(std::vector<ShmKey> &pages, bool bigElementPage = false,
                     StreamMetaShm *streamMetaShm = nullptr);
    Status FreePendingList();
    Status MoveFreeListToPendFree(uint64_t cursor, std::list<PageShmUnit> &freeList);

    mutable std::shared_timed_mutex ackMutex_;       // protect ackChain_/pendingFreePages_
    mutable std::shared_timed_mutex idxMutex_;       // protect idxChain_
    mutable std::shared_timed_mutex poolMutex_;      // protect shmPool_
    mutable std::shared_timed_mutex lastPageMutex_;  // protect lastPage_
    std::mutex allocMutex_;  // protect shm memory allocate to avoid exceed the max stream size limit.

    std::list<PageShmUnit> ackChain_;
    std::list<PageShmUnit> idxChain_;
    ShmPagesMap shmPool_;

    static constexpr int K_FREE_LIST = 2;
    std::deque<std::tuple<uint64_t, std::chrono::time_point<std::chrono::steady_clock>,
                          std::vector<std::shared_ptr<StreamDataPage>>>>
        pendingFreePages_;
    std::atomic<uint64_t> pendingFreeBytes_{ 0 };

    std::shared_ptr<StreamDataPage> lastPage_;  // last page. ref count > 0
    std::atomic<uint64_t> usedMemBytes_;
    std::atomic_uint64_t lastAckCursor_;
    std::atomic_uint64_t nextCursor_;

    // Stream metrics variables
    std::atomic<uint64_t> scMetricPagesCreated_{ 0 };
    std::atomic<uint64_t> scMetricPagesReleased_{ 0 };
    std::atomic<uint64_t> scMetricBigPagesCreated_{ 0 };
    std::atomic<uint64_t> scMetricBigPagesReleased_{ 0 };

    bool isSharedPage_{ false };
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif
