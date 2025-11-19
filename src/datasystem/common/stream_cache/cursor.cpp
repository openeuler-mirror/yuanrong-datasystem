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

#include "datasystem/common/stream_cache/cursor.h"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/util/safe_shm_lock.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
SharedMemView::SharedMemView() : lock_(0), fd_(0), mmapSz_(0), offset_(0), sz_(0)
{
}

void SharedMemView::CopyTo(ShmView &v) const
{
    v.mmapSz = mmapSz_;
    v.off = offset_;
    v.sz = sz_;
    if (fd_ == 0) {
        v.fd = -1;
    } else {
        v.fd = static_cast<decltype(v.fd)>(fd_);
    }
}

void SharedMemView::CopyFrom(const std::shared_ptr<ShmUnitInfo> &shmInfo)
{
    mmapSz_ = shmInfo->mmapSize;
    offset_ = shmInfo->offset;
    sz_ = shmInfo->size;
    if (shmInfo->fd < 0) {
        fd_ = 0;
    } else {
        fd_ = static_cast<decltype(fd_)>(shmInfo->fd);
    }
}

void SharedMemView::CopyFrom(const ShmView &v)
{
    mmapSz_ = v.mmapSz;
    offset_ = v.off;
    sz_ = v.sz;
    if (v.fd < 0) {
        fd_ = 0;
    } else {
        fd_ = static_cast<decltype(fd_)>(v.fd);
    }
}

SharedMemViewLock::SharedMemViewLock(uint32_t *lockWord) : lockWord_(lockWord)
{
}

Status SharedMemViewLock::LockExclusiveAndExec(const std::function<void()> &writeFunc, uint64_t timeoutMs)
{
    Timer timer;
    bool isFirstTimeout = false;
    Status rc;
    do {
        uint32_t val = __atomic_load_n(lockWord_, __ATOMIC_ACQUIRE);
        uint32_t expected = val & ~WRITER;
        if (!__atomic_compare_exchange_n(lockWord_, &expected, val | WRITER, true, __ATOMIC_ACQUIRE,
                                         __ATOMIC_RELAXED)) {
            if (timer.ElapsedMilliSecond() > TIMEOUT_WARNING_LIMIT_MS && !isFirstTimeout) {
                isFirstTimeout = true;
                LOG(WARNING) << "Fetching a write-lock on shared memory takes more than " << TIMEOUT_WARNING_LIMIT_MS
                             << " ms, waiting for writer to release the lock.";
            }
            // If timeout send an error
            CHECK_FAIL_RETURN_STATUS(timer.ElapsedMilliSecond() < timeoutMs, K_TRY_AGAIN,
                                     FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
            continue;
        }
        // Write bit has been set, we must unset the writer bit before going out of scope.
        while (val & ~WRITER) {
            // Wait for all readers to go away
            val = __atomic_load_n(lockWord_, __ATOMIC_ACQUIRE);
            if (timer.ElapsedMilliSecond() > TIMEOUT_WARNING_LIMIT_MS && !isFirstTimeout) {
                isFirstTimeout = true;
                LOG(WARNING) << "Fetching a write-lock on shared memory takes more than " << TIMEOUT_WARNING_LIMIT_MS
                             << " ms, waiting for readers to release the lock.";
            }
            // If timeout send an error
            if (timer.ElapsedMilliSecond() >= timeoutMs) {
                // Unset the writer bit before returning error.
                __atomic_fetch_sub(lockWord_, WRITER, __ATOMIC_RELEASE);
                RETURN_STATUS(K_TRY_AGAIN,
                              FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
            }
        }
        // cache exception to avoid the lock not released.
        try {
            // Execute the user function after we get the lock in X
            writeFunc();
        } catch (const std::exception &e) {
            auto msg = FormatString("Exception when execute writeFunc get: %s", e.what());
            rc = Status(K_RUNTIME_ERROR, msg);
        }
        __atomic_fetch_sub(lockWord_, WRITER, __ATOMIC_RELEASE);
        if (isFirstTimeout) {
            LOG(WARNING) << "Fetching a write-lock on shared memory takes " << timer.ElapsedMilliSecond() << " ms";
        }
        if (rc.IsError()) {
            LOG(ERROR) << rc.GetMsg();
        }
        return rc;
    } while (true);
}

Status SharedMemViewLock::LockSharedAndExec(const std::function<void()> &readFunc, uint64_t timeoutMs)
{
    Timer timer;
    bool isFirstTimeout = false;
    Status rc;
    do {
        while (__atomic_load_n(lockWord_, __ATOMIC_ACQUIRE) & WRITER) {
            // Block on writer
            if (timer.ElapsedMilliSecond() > TIMEOUT_WARNING_LIMIT_MS && !isFirstTimeout) {
                isFirstTimeout = true;
                LOG(WARNING) << "Fetching a read-lock on shared memory takes more than " << TIMEOUT_WARNING_LIMIT_MS
                             << " ms, waiting for writer to release the lock";
            }

            // If timeout send an error
            CHECK_FAIL_RETURN_STATUS(timer.ElapsedMilliSecond() < timeoutMs, K_TRY_AGAIN,
                                     FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
        }
        if ((__atomic_add_fetch(lockWord_, READER, __ATOMIC_ACQUIRE) & WRITER) == 0) {
            // cache exception to avoid the lock not released.
            try {
                // Execute user function after we get the lock in shared mode
                readFunc();
            } catch (const std::exception &e) {
                auto msg = FormatString("Exception when execute readFunc get: %s", e.what());
                rc = Status(K_RUNTIME_ERROR, msg);
            }

            __atomic_fetch_sub(lockWord_, READER, __ATOMIC_RELEASE);
            if (isFirstTimeout) {
                LOG(WARNING) << "Fetching a read-lock on shared memory takes " << timer.ElapsedMilliSecond() << " ms";
            }
            if (rc.IsError()) {
                LOG(ERROR) << rc.GetMsg();
            }
            return rc;
        }
        __atomic_fetch_sub(lockWord_, READER, __ATOMIC_RELEASE);  // A writer beats us. retry again
        // If timeout send an error
        CHECK_FAIL_RETURN_STATUS(timer.ElapsedMilliSecond() < timeoutMs, K_TRY_AGAIN,
                                 FormatString("[%s:%s] Timeout after %zu ms", __FUNCTION__, __LINE__, timeoutMs));
    } while (true);
}

uint64_t Cursor::GetWALastAckCursor() const
{
    if (lastAckCursor_) {
        return __atomic_load_n(lastAckCursor_, __ATOMIC_SEQ_CST);
    }
    return 0;
}

void Cursor::UpdateWALastAckCursor(uint64_t elementId) const
{
    if (lastAckCursor_) {
        __atomic_store_n(lastAckCursor_, elementId, __ATOMIC_SEQ_CST);
        return;
    }
    LOG(ERROR) << "Cursor not initialized";
}

Status Cursor::GetLastPageView(ShmView &shm, uint64_t timeoutMs) const
{
    return GetPageView(lastPageShmView_, shm, timeoutMs);
}

Status Cursor::SetLastPage(const ShmView &shm, uint64_t timeoutMs)
{
    return SetPage(lastPageShmView_, shm, timeoutMs);
}

Status Cursor::SetLastPageRef(const ShmView &shm, uint64_t timeoutMs, bool isTagged)
{
    return lastPageShmView_->SetView(shm, isTagged, timeoutMs);
}

Status Cursor::GetLastLockedPageView(ShmView &shm, uint64_t timeoutMs) const
{
    return GetPageView(lastLockedShmView_, shm, timeoutMs);
}

Status Cursor::SetLastLockedPage(const ShmView &shm, uint64_t timeoutMs)
{
    return SetPage(lastLockedShmView_, shm, timeoutMs);
}

void Cursor::InitFutexArea()
{
    __atomic_store_n(futexWord_, AckVal::NONE, __ATOMIC_RELAXED);
}

Status Cursor::Wait(uint64_t timeoutMs, int32_t &val)
{
    auto t = MilliSecondsToTimeSpec(timeoutMs);
    auto res = syscall(SYS_futex, futexWord_, FUTEX_WAIT, AckVal::NONE, &t, nullptr, 0);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        res != -1 || errno == EAGAIN || errno == ETIMEDOUT || errno == EINTR, K_RUNTIME_ERROR,
        FormatString("Futex wait error. Errno = %d. Message %s", errno, StrErr(errno)));
    if (res == 0 || errno == EAGAIN) {
        auto fetchVal = __atomic_load_n(futexWord_, __ATOMIC_RELAXED);
        uint32_t checkBit = fetchVal & Cursor::SHIFT;
        val = static_cast<int32_t>(fetchVal >> Cursor::SHIFT);
        CHECK_FAIL_RETURN_STATUS(checkBit == Cursor::AckVal::DONE, K_RUNTIME_ERROR,
                                 FormatString("Handshake error. Expect %d but get %d", Cursor::AckVal::DONE, checkBit));
        return Status::OK();
    }
    RETURN_STATUS(K_TRY_AGAIN, FormatString("Time out within allowed time %zu ms", timeoutMs));
}

Status Cursor::Wake(const int32_t val, size_t &numWaiter)
{
    __atomic_store_n(futexWord_, static_cast<uint32_t>(val) << SHIFT | AckVal::DONE, __ATOMIC_SEQ_CST);
    auto res = syscall(SYS_futex, futexWord_, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        res != -1, K_RUNTIME_ERROR, FormatString("Futex wake error. Errno = %d. Message %s", errno, StrErr(errno)));
    numWaiter = static_cast<uint64_t>(res);
    return Status::OK();
}

bool Cursor::ForceClose() const
{
    uint32_t val = __atomic_load_n(forceClose_, __ATOMIC_RELAXED);
    return val > 0;
}

void Cursor::SetForceClose()
{
    __atomic_store_n(forceClose_, 1, __ATOMIC_RELAXED);
}

void Cursor::SetElementCount(uint64_t val)
{
    __atomic_store_n(elementCount_, val, __ATOMIC_RELAXED);
}

uint64_t Cursor::IncrementElementCount(uint64_t inc)
{
    return __atomic_add_fetch(elementCount_, inc, __ATOMIC_RELAXED);
}

uint64_t Cursor::GetElementCountAndReset()
{
    uint64_t val = __atomic_load_n(elementCount_, __ATOMIC_RELAXED);
    while (!__atomic_compare_exchange_n(elementCount_, &val, 0, true, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED)) {
        val = __atomic_load_n(elementCount_, __ATOMIC_RELAXED);
    }
    return val;
}

uint64_t Cursor::GetElementCount() const
{
    return __atomic_load_n(elementCount_, __ATOMIC_RELAXED);
}

uint64_t Cursor::IncrementRequestCount()
{
    return __atomic_add_fetch(requestCount_, 1, __ATOMIC_RELAXED);
}

uint64_t Cursor::GetRequestCountAndReset()
{
    uint64_t val = __atomic_load_n(requestCount_, __ATOMIC_RELAXED);
    while (!__atomic_compare_exchange_n(requestCount_, &val, 0, true, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED)) {
        val = __atomic_load_n(requestCount_, __ATOMIC_RELAXED);
    }
    return val;
}

uint32_t Cursor::GetEyeCatcher() const
{
    return __atomic_load_n(eyeCatcher_, __ATOMIC_RELAXED);
}

uint32_t Cursor::GetClientVersion() const
{
    return GetEyeCatcher() & CLIENT_EYECATCHER_MASK;
}

uint32_t Cursor::GetWorkerVersion() const
{
    return GetEyeCatcher() & WORKER_EYECATCHER_MASK;
}

Status Cursor::ForceUnLock(uint32_t lockId, const std::string &msg)
{
    Status lastRc;
    if (lastPageShmView_ != nullptr) {
        lastRc = lastPageShmView_->ForceUnLock(lockId, msg);
    }

    if (lastLockedShmView_ != nullptr) {
        auto rc = lastLockedShmView_->ForceUnLock(lockId, msg);
        lastRc = rc.IsError() ? rc : lastRc;
    }
    return lastRc;
}

Status Cursor::SetClientVersion(uint32_t val)
{
    return SetEyeCatcherHelper(val, CLIENT_EYECATCHER_MASK);
}

Status Cursor::SetWorkerVersion(uint32_t val)
{
    return SetEyeCatcherHelper(val, WORKER_EYECATCHER_MASK);
}

Status Cursor::SetEyeCatcherHelper(uint32_t val, uint32_t mask)
{
    RETURN_OK_IF_TRUE(val == 0);
    CHECK_FAIL_RETURN_STATUS((val & mask) == val, K_RUNTIME_ERROR,
                             FormatString("Invalid eye catcher version %zu given mask %x", val, mask));
    uint32_t current;
    uint32_t newVal;
    do {
        current = __atomic_load_n(eyeCatcher_, __ATOMIC_RELAXED);
        RETURN_OK_IF_TRUE((current & mask) == val);
        CHECK_FAIL_RETURN_STATUS((current & mask) == 0, K_RUNTIME_ERROR,
                                 "Client or worker eye catcher version is to be set only once");
        newVal = current | val;
    } while (!__atomic_compare_exchange_n(eyeCatcher_, &current, newVal, true, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED));
    return Status::OK();
}

Status Cursor::GetPageView(const std::shared_ptr<SharedMemViewImpl> &impl, ShmView &shm, uint64_t timeoutMs)
{
    ShmView v;
    bool isTagged;
    RETURN_IF_NOT_OK(impl->GetView(v, isTagged, timeoutMs));
    // We never tag the view in the mailbox area. But for safety, just return null
    if (isTagged) {
        shm = {};
        return Status::OK();
    }
    shm = v;
    return Status::OK();
}

Status Cursor::SetPage(std::shared_ptr<SharedMemViewImpl> &impl, const ShmView &shm, uint64_t timeoutMs)
{
    return impl->SetView(shm, false, timeoutMs);
}

Cursor::Cursor(void *ptr, size_t sz, uint32_t lockId) : ptr_(reinterpret_cast<uint8_t *>(ptr)), sz_(sz), lockId_(lockId)
{
}

Status Cursor::Init(std::shared_ptr<client::MmapTableEntry> mmapEntry)
{
    RETURN_RUNTIME_ERROR_IF_NULL(ptr_);
#define CURSOR_INIT_FIELD(start, cur, field)                                                   \
    do {                                                                                       \
        (field) = reinterpret_cast<decltype(field)>(cur);                                      \
        (cur) += sizeof(*(field));                                                             \
        CHECK_FAIL_RETURN_STATUS(static_cast<size_t>((cur) - (start)) <= sz_, K_RUNTIME_ERROR, \
                                 "Work area size too small");                                  \
    } while (false)

    auto *data = ptr_;
    CURSOR_INIT_FIELD(ptr_, data, lastAckCursor_);
    CURSOR_INIT_FIELD(ptr_, data, lastPage_);
    CURSOR_INIT_FIELD(ptr_, data, futexWord_);
    CURSOR_INIT_FIELD(ptr_, data, forceClose_);
    CURSOR_INIT_FIELD(ptr_, data, elementCount_);
    CURSOR_INIT_FIELD(ptr_, data, requestCount_);
    lastPageShmView_ = std::make_shared<SharedMemViewImpl>(lastPage_, sizeof(*lastPage_), lockId_);
    RETURN_IF_NOT_OK(lastPageShmView_->Init(false));

    if (mmapEntry != nullptr) {
        mmapEntry_ = std::move(mmapEntry);
    }
    // Clear the wait area
    __atomic_store_n(futexWord_, 0, __ATOMIC_SEQ_CST);
    // Clear the stream state
    __atomic_store_n(forceClose_, 0, __ATOMIC_SEQ_CST);
    // Clear the request count
    __atomic_store_n(requestCount_, 0, __ATOMIC_SEQ_CST);

    // This starts V2 where another 64 bytes is added after requestCount
    RETURN_OK_IF_TRUE(sz_ == K_CURSOR_SIZE_V1);
    // Continue the new fields added in V2
    CURSOR_INIT_FIELD(ptr_, data, eyeCatcher_);
    CURSOR_INIT_FIELD(ptr_, data, waitCount_);
    CURSOR_INIT_FIELD(ptr_, data, lastLockedPage_);
    // Initialize the shm view
    lastLockedShmView_ = std::make_shared<SharedMemViewImpl>(lastLockedPage_, sizeof(*lastLockedPage_), lockId_);
    RETURN_IF_NOT_OK(lastLockedShmView_->Init(false));
    // Clear the wait area
    __atomic_store_n(waitCount_, 0, __ATOMIC_SEQ_CST);

    return Status::OK();
}

Status SharedMemViewImpl::Init(bool clearFields)
{
    RETURN_RUNTIME_ERROR_IF_NULL(view_);
    CHECK_FAIL_RETURN_STATUS(sz_ >= sizeof(SharedMemView), K_RUNTIME_ERROR,
                             FormatString("Not enough size. Need at least %zu", sizeof(SharedMemView)));
    if (clearFields) {
        auto rc = memset_s(view_, sz_, 0, sizeof(SharedMemView));
        CHECK_FAIL_RETURN_STATUS(rc == 0, K_RUNTIME_ERROR, FormatString("memset_s fails. Errno = %d", errno));
    }
    return Status::OK();
}

Status SharedMemViewImpl::LockExclusiveAndExec(const std::function<void()> &writeFunc, uint64_t timeoutMs)
{
    RETURN_RUNTIME_ERROR_IF_NULL(view_);
    SharedMemViewLock lock(&view_->lock_);
    return lock.LockExclusiveAndExec(writeFunc, timeoutMs);
}

Status SharedMemViewImpl::LockSharedAndExec(const std::function<void()> &readFunc, uint64_t timeoutMs) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(view_);
    SharedMemViewLock lock(&view_->lock_);
    return lock.LockSharedAndExec(readFunc, timeoutMs);
}

Status SharedMemViewImpl::LockAndExec(const std::function<void()> &func, uint64_t timeoutMs)
{
    RETURN_RUNTIME_ERROR_IF_NULL(view_);
    SafeShmLock xlocker(&view_->lock_, lockId_);
    RETURN_IF_NOT_OK(xlocker.Lock(timeoutMs));
    Status rc;
    try {
        func();
    } catch (const std::exception &e) {
        auto msg = FormatString("Exception when execute func get: %s", e.what());
        rc = Status(K_RUNTIME_ERROR, msg);
    }
    xlocker.UnLock();
    if (rc.IsError()) {
        LOG(ERROR) << rc.GetMsg();
    }
    return rc;
}

Status SharedMemViewImpl::SetView(const ShmView &shm, bool isTagged, uint64_t timeoutMs)
{
    auto func = [this, isTagged, &shm]() {
        INJECT_POINT("SharedMemViewImpl.SetView", [] { throw std::bad_function_call(); });
        view_->CopyFrom(shm);
        if (isTagged) {
            view_->fd_ |= PAGE_VIEW_TAG;
        } else {
            view_->fd_ &= ~PAGE_VIEW_TAG;
        }
    };
    INJECT_POINT("MemView.Lock.OldVersion", [&] { return LockExclusiveAndExec(func, timeoutMs); });
    return LockAndExec(func, timeoutMs);
}

Status SharedMemViewImpl::GetView(ShmView &shm, bool &isTagged, uint64_t timeoutMs)
{
    auto func = [this, &isTagged, &shm]() {
        INJECT_POINT("SharedMemViewImpl.GetView", [] { throw std::bad_function_call(); });
        isTagged = (view_->fd_ & PAGE_VIEW_TAG);
        if (isTagged) {
            auto fd = view_->fd_;
            fd &= ~PAGE_VIEW_TAG;
            shm = { .fd = static_cast<int>(fd), .mmapSz = view_->mmapSz_, .off = view_->offset_, .sz = view_->sz_ };
        } else {
            INJECT_POINT("producer_crash_getview", [] {});
            view_->CopyTo(shm);
        }
    };
    INJECT_POINT("MemView.Lock.OldVersion", [&] { return LockSharedAndExec(func, timeoutMs); });
    return LockAndExec(func, timeoutMs);
}

Status SharedMemViewImpl::ForceUnLock(uint32_t lockId, const std::string &msg)
{
    CHECK_FAIL_RETURN_STATUS(
        lockId_ == WORKER_LOCK_ID, K_RUNTIME_ERROR,
        FormatString("Only worker can call ForceUnLock, invalid lockId_ %zu, lockId %zu", lockId_, lockId));

    CHECK_FAIL_RETURN_STATUS(lockId > WORKER_LOCK_ID, K_RUNTIME_ERROR, FormatString("Invalid lockId", lockId));

    if (view_ != nullptr && SafeShmLock::ForceUnlock(&view_->lock_, lockId)) {
        LOG(INFO) << FormatString("[%s] ForceUnLock for lockId %zu, PageViewInfo: %s", msg, lockId, view_->ToString());
    }
    return Status::OK();
}

}  // namespace datasystem
