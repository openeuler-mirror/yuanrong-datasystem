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

#ifndef DATASYSTEM_STREAM_CACHE_CURSOR_H
#define DATASYSTEM_STREAM_CACHE_CURSOR_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/client/mmap_manager.h"

namespace datasystem {
class SharedMemViewLock {
public:
    explicit SharedMemViewLock(uint32_t *lockWord);
    Status LockExclusiveAndExec(const std::function<void()> &writeFunc, uint64_t timeoutMs);
    Status LockSharedAndExec(const std::function<void()> &readFunc, uint64_t timeoutMs);

private:
    uint32_t *lockWord_;
    constexpr static const uint32_t WRITER = 1;
    constexpr static const uint32_t READER = 2;
    constexpr static const int TIMEOUT_WARNING_LIMIT_MS = 3000;
};

// We store ShmView on the page. To avoid the size changes on different platforms
// and avoid any gap between fields, we will mirror ShmView but enforce the whole
// structure to be 32 bytes in size without any gap
// Some fields will be updated atomically.
struct SharedMemView {
    uint32_t lock_;    // 4 bytes
    uint32_t fd_;      // 4 bytes (unlikely a file descriptor needs a full 8 byte to store)
    uint64_t mmapSz_;  // 8 bytes
    int64_t offset_;   // 8 bytes
    uint64_t sz_;      // 8 bytes
    ~SharedMemView() = default;
    SharedMemView();
    void CopyTo(ShmView &v) const;
    void CopyFrom(const std::shared_ptr<ShmUnitInfo> &shmInfo);
    void CopyFrom(const ShmView &v);
    std::string ToString()
    {
        std::stringstream ss;
        ss << "fd:" << fd_;
        ss << ", mmapSz:" << mmapSz_;
        ss << ", offset:" << offset_;
        ss << ", sz:" << sz_;
        return ss.str();
    }
};

class SharedMemViewImpl {
public:
    SharedMemViewImpl() : view_(nullptr), sz_(0)
    {
    }
    SharedMemViewImpl(void *ptr, size_t sz, uint32_t lockId)
        : view_(reinterpret_cast<SharedMemView *>(ptr)), sz_(sz), lockId_(lockId)
    {
    }
    ~SharedMemViewImpl() = default;

    /**
     * Init. Check size
     * @return
     */
    Status Init(bool clearFields = false);

    /**
     * Set the ShmView of the last page
     * @param[in] shm The shme view .
     * @param[in] isTagged The tag bit for shm page.
     * @param[in] timeoutMs The timeout in ms.
     * @return Status of this call.
     */
    Status SetView(const ShmView &shm, bool isTagged, uint64_t timeoutMs);

    /**
     * Get ShmView of the last page
     * @param[out] shm The shme view .
     * @param[out] isTagged The tag bit for shm page.
     * @param[in] timeoutMs The timeout in ms.
     * @return Status of this call.
     */
    Status GetView(ShmView &shm, bool &isTagged, uint64_t timeoutMs);

    /**
     * @brief Force unlock the shm view.
     * @param[in] lockId The lock id.
     * @param[in] msg The message for log.
     * @return Status of this call.
     */
    Status ForceUnLock(uint32_t lockId, const std::string &msg);

protected:
    // The high bit of fd_ field is used for different purpose.
    static uint32_t constexpr PAGE_VIEW_TAG = 0x80000000;

private:
    friend class StreamDataPage;
    SharedMemView *view_;
    size_t sz_;
    uint32_t lockId_{ 0 };
    Status LockExclusiveAndExec(const std::function<void()> &func, uint64_t timeoutMs);
    Status LockSharedAndExec(const std::function<void()> &func, uint64_t timeoutMs) const;
    Status LockAndExec(const std::function<void()> &func, uint64_t timeoutMs);
};

static constexpr uint64_t ONE_K = 1'000ul;
static constexpr uint64_t ONE_M = 1'000'000ul;
timespec inline MilliSecondsToTimeSpec(uint64_t timeoutMs)
{
    timespec t{ .tv_sec = static_cast<__time_t>(timeoutMs / ONE_K),
                .tv_nsec = static_cast<__syscall_slong_t>((timeoutMs % ONE_K) * ONE_M) };
    return t;
}

// A work area that is shared between
// (1) client::stream_cache::ConsumerImpl and the corresponding worker::stream_cache::Consumer
// (2) client::stream_cache::ProducerImpl and the corresponding worker::stream_cache::Producer
// sz is the size of this work area. For version 1, the size is 64 bytes.
// (a) The first 8 bytes is used for fetching the lastAckCursor from the client
// (b) Next 32 bytes is used storing ShmView of the last page (if it exists)
// (c) Next 4 bytes is used for futex
// (d) Next 4 bytes is used to signal force close
// (e) Next 8 bytes for element count (pushing/receiving)
// (f) Next 8 bytes for request count (send/receive requests called by client)
// (g) The size of the work area is 64 bytes, and we have 0 bytes left if this is version 1.
// Local producer/consumer should use the inherited function WorkAreaIsV2 (see client_base_impl.h)
// for compatibility if the worker is of lower level
// For version 2, another 64 bytes is allocated and the total structure size is now 128 bytes
// (h) Some eye catcher field (4 bytes) so that worker can tell if it is a V1 or V2 client.
//     V1 client will not write past the 64 bytes. So if this field is set, it is a V2 client
// (i) Next 4 bytes is for alignment and can be combined with futex area (c) above as a wait count area
//     and call the static function PageLock::FutexWake and PageLock::FutexWait to improve performance
// (j) Next 32 bytes is used to store ShmView of the last page locked by the producer
// (j) 24 bytes is left for future use.
class Cursor {
public:
    // V1 of Cursor area has a size of 64 bytes
    constexpr static size_t K_CURSOR_SIZE_V1 = 64;
    // V2 of Cursor area is extending to 128 bytes
    constexpr static size_t K_CURSOR_SIZE_V2 = 128;
    // EyeCatcher masks for client and worker version.
    const uint32_t CLIENT_EYECATCHER_MASK = static_cast<uint32_t>(0x0000FFFF);
    const uint32_t WORKER_EYECATCHER_MASK = static_cast<uint32_t>(0xFFFF0000);
    // Client EyeCatcher V2 is K_CURSOR_SIZE_V2.
    constexpr static uint32_t K_WORKER_EYECATCHER_V1 = static_cast<uint32_t>(0x00010000);
    enum AckVal : uint32_t { NONE = 0, DONE = 1 };
    constexpr static uint32_t SHIFT = 1;

    Cursor(void *ptr, size_t sz, uint32_t lockId);
    ~Cursor() = default;

    /**
     * @brief Get the last ack cursor from the work area
     * @return last ack cursor
     */
    uint64_t GetWALastAckCursor() const;

    /**
     * @brief Update the last ack cursor in work area
     * @param elementId
     */
    void UpdateWALastAckCursor(uint64_t elementId) const;

    /**
     * Initialization
     */
    Status Init(std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr);

    /**
     * Get ShmView of the last page
     */
    Status GetLastPageView(ShmView &shm, uint64_t timeoutMs) const;

    /**
     * @brief Get the last page if enable shared page.
     * @param[out] shm The shm view of ths last page.
     * @param[out] switchToSharedPage Whether switched to shared page.
     * @param[in] timeoutMs The timeout in ms.
     * @param[in] toShmInfo The shm mapping function
     * @return Status of this call
     */
    template <typename F>
    Status GetLastPageViewByRef(ShmView &shm, bool &switchToSharedPage, uint64_t timeoutMs, F &&toShmInfo)
    {
        switchToSharedPage = false;
        if (lastPageRefShmView_) {
            return Cursor::GetPageView(lastPageRefShmView_, shm, timeoutMs);
        }

        ShmView lastPageRefView;
        RETURN_IF_NOT_OK(lastPageShmView_->GetView(lastPageRefView, switchToSharedPage, timeoutMs));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(lastPageRefView.fd > 0 && lastPageRefView.sz == sizeof(SharedMemView),
                                             K_RUNTIME_ERROR,
                                             FormatString("Invalid lastPageRefView %s", lastPageRefView.ToStr()));
        std::shared_ptr<client::IMmapTableEntry> mmapEntry;
        std::shared_ptr<ShmUnitInfo> lastPageRefUnit;
        RETURN_IF_NOT_OK(toShmInfo(lastPageRefView, lastPageRefUnit, mmapEntry));
        SharedMemView *memView = reinterpret_cast<SharedMemView *>(
            reinterpret_cast<uint8_t *>(lastPageRefUnit->GetPointer()) + lastPageRefUnit->offset);
        auto lastPageRefShmView = std::make_shared<SharedMemViewImpl>(memView, sizeof(SharedMemView), lockId_);
        RETURN_IF_NOT_OK(lastPageRefShmView->Init(false));
        if (switchToSharedPage) {
            refMmapEntry_ = std::move(mmapEntry);
            lastPageRefShmView_ = lastPageRefShmView;
        }
        return Cursor::GetPageView(lastPageRefShmView, shm, timeoutMs);
    }

    /**
     * Set the ShmView of the last page
     * @param shm
     */
    Status SetLastPage(const ShmView &shm, uint64_t timeoutMs);

    /**
     * Set the ShmView of the last page
     * @param[in] shm The shm view of last page ref.
     * @param[in] timeoutMs The timeout in ms.
     * @param[in] isTagged The page has tag bit.
     * @return The status of this call.
     */
    Status SetLastPageRef(const ShmView &shm, uint64_t timeoutMs, bool isTagged);

    void InitFutexArea();

    /**
     * @brief Wait for event
     * @param timeoutMs timeout in milliseconds
     * @param val Updated value from futex word
     * @return
     */
    Status Wait(uint64_t timeoutMs, int32_t &val);

    /**
     * @brief Wake up waiters
     * @param val Value to write to the futex word
     * @return numWaiter Number of waiters on the futex word
     */
    Status Wake(int32_t val, size_t &numWaiter);

    /**
     * Check for interrupt
     * @return true is interrupted.
     * @note If a consumer is interrupted, it will return K_SC_NO_PRODUCER
     *       If a producer is interrupted, it will return K_SC_NO_CONSUMER
     */
    bool ForceClose() const;

    /**
     * Interrupt a consumer when a producer is gone, or
     * interrupt a producer when a consumer is gone
     */
    void SetForceClose();

    /**
     * Set the element count
     * @param val
     */
    void SetElementCount(uint64_t val);

    /**
     * Increment the element count
     * @param inc
     * @return value before increment
     */
    uint64_t IncrementElementCount(uint64_t inc = 1);

    /**
     * Get the element count
     */
    uint64_t GetElementCount() const;

    /**
     * @brief Get the element count and reset it to 0.
     * @return
     */
    uint64_t GetElementCountAndReset();

    /**
     * Increment the request count
     * @param inc
     * @return value before increment
     */
    uint64_t IncrementRequestCount();

    /**
     * Get the request count and reset it to 0
     */
    uint64_t GetRequestCountAndReset();

    /**
     * Get ShmView of the last locked page
     */
    Status GetLastLockedPageView(ShmView &shm, uint64_t timeoutMs) const;

    /**
     * Set the ShmView of the last locked page
     * @param shm
     */
    Status SetLastLockedPage(const ShmView &shm, uint64_t timeoutMs);

    /**
     * @brief Update the eye catcher field for client version.
     */
    Status SetClientVersion(uint32_t val);

    /**
     * @brief Update the eye catcher field for worker version.
     */
    Status SetWorkerVersion(uint32_t val);

    /**
     * @brief Retrieve the eye catcher version for client.
     */
    uint32_t GetClientVersion() const;

    /**
     * @brief Retrieve the eye catcher version for worker.
     */
    uint32_t GetWorkerVersion() const;

    Status ForceUnLock(uint32_t lockId, const std::string &msg);

private:
    uint8_t *ptr_;
    uint64_t *lastAckCursor_{ nullptr };
    SharedMemView *lastPage_{ nullptr };
    uint32_t *futexWord_{ nullptr };
    uint32_t *forceClose_{ nullptr };
    uint64_t *elementCount_{ nullptr };
    uint64_t *requestCount_{ nullptr };
    uint32_t *eyeCatcher_{ nullptr };
    uint32_t *waitCount_{ nullptr };
    SharedMemView *lastLockedPage_{ nullptr };
    const size_t sz_;
    const uint32_t lockId_;
    std::shared_ptr<SharedMemViewImpl> lastPageShmView_;
    std::shared_ptr<SharedMemViewImpl> lastLockedShmView_;
    std::shared_ptr<client::IMmapTableEntry> mmapEntry_;

    std::shared_ptr<SharedMemViewImpl> lastPageRefShmView_;
    std::shared_ptr<client::IMmapTableEntry> refMmapEntry_;

    /**
     * @brief Get ShmView from a SharedMemViewImpl
     */
    static Status GetPageView(const std::shared_ptr<SharedMemViewImpl> &impl, ShmView &shm, uint64_t timeoutMs);

    /**
     * @brief Set the ShmView to a SharedMemViewImpl
     */
    static Status SetPage(std::shared_ptr<SharedMemViewImpl> &impl, const ShmView &shm, uint64_t timeoutMs);

    /**
     * @brief Helper function to update the eye catcher field for client or worker depending on the mask given.
     */
    Status SetEyeCatcherHelper(uint32_t val, uint32_t mask);

    /**
     * @brief Retrieve the eye catcher
     */
    uint32_t GetEyeCatcher() const;
};
}  // namespace datasystem
#endif
