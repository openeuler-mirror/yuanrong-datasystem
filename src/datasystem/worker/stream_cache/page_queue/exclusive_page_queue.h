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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_EXCLUSIVE_PAGE_QUEUE_H
#define DATASYSTEM_WORKER_STREAM_CACHE_EXCLUSIVE_PAGE_QUEUE_H

#include <memory>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/stream_cache/stream_meta_shm.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/worker/stream_cache/consumer.h"
#include "datasystem/worker/stream_cache/page_queue/page_queue_base.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class RemoteWorkerManager;
class StreamManager;
/**
 * A class that manages the stream data pages of a stream
 */
class ExclusivePageQueue : public std::enable_shared_from_this<ExclusivePageQueue>, public PageQueueBase {
public:
    ExclusivePageQueue(StreamManager *mgr, Optional<StreamFields> cfg);
    ~ExclusivePageQueue() = default;

    /**
     * @brief Verifies the input stream fields match the existing setting.
     * If the existing settings are uninitialized, updates the values.
     * @param[in] streamFields The stream fields with page size and max stream size to check
     * @return Status of the call.
     */
    Status UpdateStreamFields(const StreamFields &streamFields);

    /**
     * @return The reserve memory size
     */
    uint64_t GetReserveSize() const;

    /**
     * @return T if auto cleanup is on
     */
    bool AutoCleanup() const;

    /**
     * @brief Reserve stream memory when the stream data object is created.
     * @return K_OK if successful or if the 1st page is already created.
     *         Possible error code return can be K_OUT_OF_MEMORY
     * @note If the page size of the stream is unknown, the function returns K_OK without allocating any memory.
     */
    Status ReserveStreamMemory();

    /**
     * Reset the object
     * @return
     */
    Status Reset();

    /**
     * @brief Getter of lastAckCursor_.
     * @return lastAckCursor_.
     */
    uint64_t GetLastAckCursor() const
    {
        return lastAckCursor_;
    }

    /**
     * @brief Add callback to unblock sending stream.
     * @param[in] addr The producer worker address.
     * @param[in] unblockCallback The callback functions to unblock the stream.
     */
    void AddUnblockCallback(const std::string &addr, const std::function<void()> &unblockCallback);

    /**
     * @brief Remove callback when remote producer is getting deleted.
     * @param[in] addr The producer worker address.
     */
    void RemoveUnblockCallback(const std::string &addr);

    /*
     * Link back to the stream manager
     */
    auto GetStreamManager()
    {
        return streamMgr_;
    }

    std::string GetStreamName() const override
    {
        return streamName_;
    }

    RemoteWorkerManager *GetRemoteWorkerManager() const override;

    /**
     * @brief Gets the max stream size
     * @return Max stream size
     */
    uint32_t GetMaxStreamSize() const
    {
        return streamFields_.maxStreamSize_;
    }

    /**
     * @brief Gets the shared memory used
     * @return Shared memory used
     */
    uint64_t GetSharedMemoryUsed() const
    {
        return usedMemBytes_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Gets the number of pages created
     */
    uint64_t GetNumPagesCreated() const
    {
        return scMetricPagesCreated_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Gets the number of pages released
     */
    uint64_t GetNumPagesReleased() const
    {
        return scMetricPagesReleased_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Gets the number of pages in use
     */
    uint64_t GetNumPagesInUse() const;

    /**
     * @brief Gets the number of pages cached
     */
    uint64_t GetNumPagesCached() const;

    /**
     * @brief Gets the number of big element pages created
     */
    uint64_t GetNumBigPagesCreated() const
    {
        return scMetricBigPagesCreated_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Gets the number of big element pages released
     */
    uint64_t GetNumBigPagesReleased() const
    {
        return scMetricBigPagesReleased_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Batch insert.
     * @param[in] buf contiguous payload of the elements in reverse order
     * @param[in] sz vector of the size of the elements
     * @param[in] headerBits Is the element's data contain header
     * @param[in] streamMetaShm The pointer to streamMetaShm
     * @return Status
     * @note This function is not thread safe, and/or lock safe.
     */
    Status BatchInsert(void *buf, std::vector<size_t> &sz, std::pair<size_t, size_t> &res, uint64_t timeoutMs,
                       const std::vector<bool> &headerBits, StreamMetaShm *StreamMetaShm);

    /**
     * @brief Used by the consumer to return the first starting page of the lastAckCursor
     * @param req
     * @param consumer
     * @param GetDataPage unary server writer handle
     * @return OK
     */
    Status GetDataPage(const GetDataPageReqPb &req, const std::shared_ptr<Consumer> &consumer,
                       const std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> &serverApi);

    /**
     * @brief Gets the stream fields
     * @param[out] streamFields Return the stream fields with page size and max stream size.
     */
    void GetStreamFields(StreamFields &streamFields)
    {
        streamFields = streamFields_;
    }

    /**
     * @brief Get encryptStream of streamFields_, and apply sanity checks.
     * @return true if stream data encryption is applicable.
     */
    bool IsEncryptStream(const std::string &streamName) const override;

    /**
     * @brief Get max window count
     */
    auto GetMaxWindowCount() const
    {
        return maxWindowCount_;
    }

    /**
     * @brief Logs the cursors
     * @return
     */
    void LogCursors();

    Status ReleaseAllPages();

    void RegisterUpdateLastDataPageHandler(std::function<Status(const ShmView &)> updateLastDataPageHandler)
    {
        updateLastDataPageHandler_ = std::move(updateLastDataPageHandler);
    }

public:
    std::string LogPrefix() const override;
    size_t GetPageSize() const override;
    Status CheckHadEnoughMem(size_t memSize) override;
    Status VerifyWhenAlloc() const override;
    Status AllocateMemoryImpl(size_t memSizeNeeded, ShmUnit &shmUnit, bool retryOnOOM) override;
    Status UpdateLocalCursorLastDataPage(const ShmView &shmView) override;
    Status AfterAck() override
    {
        return UnblockProducers();
    }

    virtual std::pair<size_t, bool> GetNextBlockedRequestSize() override;

private:
    std::shared_ptr<PageQueueBase> SharedFromThis() override;
    friend class StreamDataPool;
    mutable std::mutex cfgMutex_;  // protect streamFields_
    StreamFields streamFields_;
    uint64_t maxWindowCount_;
    mutable std::mutex unblockMutex_;  // unblockCallbacks_
    std::unordered_map<std::string, std::function<void()>> unblockCallbacks_;
    StreamManager *streamMgr_;  // Back pointer to parent class
    const std::string streamName_;
    struct {
        std::atomic<bool> pageZeroCreated{ false };
        std::atomic<bool> freeListCreated{ false };
    } reserveState_;

    std::function<Status(const ShmView &)> updateLastDataPageHandler_;

    static Status ReturnGetPageRspPb(
        const ShmView &shmView,
        const std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> &serverApi);
    Status InsertBigElement(void *buf, size_t sz, std::pair<size_t, size_t> &res, uint64_t timeoutMs,
                            const bool headerBit, StreamMetaShm *streamMetaShm);
    Status BatchInsertImpl(void *buf, std::vector<size_t> &sz, std::pair<size_t, size_t> &res, InsertFlags flags,
                           uint64_t timeoutMs, const std::vector<bool> &headerBits, StreamMetaShm *streamMetaShm);
    Status UnblockProducers();
    Status ReserveAdditionalMemory();
};

}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif
