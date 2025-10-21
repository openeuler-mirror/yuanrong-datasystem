/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Based on shared memory circular queue management.
 */

#include "datasystem/common/util/queue/shm_circular_queue.h"

namespace datasystem {

Status ShmCircularQueue::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(circularQueueUnit_);
    uint8_t *data = (uint8_t *)(circularQueueUnit_->pointer);
    RETURN_RUNTIME_ERROR_IF_NULL(data);
    uint32_t shmCheckSize =
        cap_ * elementSize_ + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(circularQueueUnit_->mmapSize >= shmCheckSize, K_RUNTIME_ERROR,
                                         "Allocate shared memory is not enough!");
    if (isClient_) {
        data = data + circularQueueUnit_->offset;
    }
    uint64_t *lockPtr = (uint64_t *)data;
    queueLock_ = std::make_shared<object_cache::PackageLock>(lockPtr, lockId_);
    constexpr int uintStep = 2;
    queueSize_ = (uint32_t *)data + uintStep;
    queueHead_ = queueSize_ + 1;
    arr_ = (uint8_t *)queueHead_ + sizeof(uint32_t);
    return Status::OK();
}

Status ShmCircularQueue::SetGetDataHandler(std::function<void(uint8_t *)> callback)
{
    RETURN_RUNTIME_ERROR_IF_NULL(callback);
    dataHandler_ = std::move(callback);
    return Status::OK();
}

void ShmCircularQueue::UpdateQueueMeta()
{
    len_ = __atomic_load_n(queueSize_, __ATOMIC_SEQ_CST);
    head_ = __atomic_load_n(queueHead_, __ATOMIC_SEQ_CST);
}

void ShmCircularQueue::NotifyQueueNotFull()
{
    if (len_ < cap_) {
        long result;
        FUTEX_RETRY_ON_EINTR(result, syscall(SYS_futex, queueSize_, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0));
        (void)CheckFutexErrno(result);
    }
}

void ShmCircularQueue::NotifyNotEmpty()
{
    if (len_ > 0) {
        long result;
        FUTEX_RETRY_ON_EINTR(result, syscall(SYS_futex, queueSize_, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0));
        (void)CheckFutexErrno(result);
    }
}

void ShmCircularQueue::WakeUpQueueProcessAndFinish()
{
    int addStep = 1;
    __atomic_add_fetch(queueSize_, addStep, __ATOMIC_SEQ_CST);
    long result;
    FUTEX_RETRY_ON_EINTR(result, syscall(SYS_futex, queueSize_, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0));
    (void)CheckFutexErrno(result);
}

void ShmCircularQueue::WakeUpClientProcessAndFinish()
{
    // Set client reconnect flag, stop to wait futex or lock next time.
    destroyFlag_ = true;
    // If worker alive, wake up worker to handle msg, and then worker can notify client to finish wait.
    NotifyNotEmpty();
    NotifyQueueNotFull();
}

Status ShmCircularQueue::WaitForQueueEmpty(const timespec &timeoutSec)
{
    if (len_ == 0) {
        long result;
        FUTEX_RETRY_ON_EINTR(result, syscall(SYS_futex, queueSize_, FUTEX_WAIT, 0, &timeoutSec, nullptr, 0));
        RETURN_IF_NOT_OK(CheckFutexErrno(result));
    }
    return Status::OK();
}

Status ShmCircularQueue::WaitForQueueFull(const timespec &timeoutSec)
{
    long result;
    FUTEX_RETRY_ON_EINTR(result, syscall(SYS_futex, queueSize_, FUTEX_WAIT, cap_, &timeoutSec, nullptr, 0));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckFutexErrno(result), "Failed to wait for queue full");
    return Status::OK();
}

void ShmCircularQueue::Clear()
{
    head_ = 0;
    len_ = 0;
}

bool ShmCircularQueue::GetSlotUntilSuccess(uint32_t &slotIndex)
{
    bool getSlotSuccess = false;
    uint32_t queueSize = __atomic_load_n(queueSize_, __ATOMIC_SEQ_CST);

    while (queueSize < cap_) {
        auto isSuccess = __atomic_compare_exchange_n(queueSize_, &queueSize, queueSize + 1, true, __ATOMIC_SEQ_CST,
                                                     __ATOMIC_SEQ_CST);
        if (!isSuccess) {
            queueSize = __atomic_load_n(queueSize_, __ATOMIC_SEQ_CST);
            continue;
        } else {
            getSlotSuccess = true;
            slotIndex = queueSize;
            len_ = slotIndex + 1;
            break;
        }
    }
    return getSlotSuccess;
}

Status ShmCircularQueue::PushBySlot(uint32_t slotIndex, const char *value, uint32_t valueSize, uint8_t **outShmPtr)
{
    if (cap_ == 0) {
        auto errMsg = "cap_ cant be 0 as a divisor, push by slot failed";
        LOG(ERROR) << errMsg;
        RETURN_STATUS(K_RUNTIME_ERROR, errMsg);
    }
    auto ptr = arr_ + (head_ + slotIndex) % cap_ * elementSize_;
    int ret = memcpy_s(ptr, elementSize_, value, valueSize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == EOK, K_RUNTIME_ERROR,
                                         FormatString("Memory copy failed, the memcpy_s return: %d: ", ret));
    *outShmPtr = ptr;
    RETURN_RUNTIME_ERROR_IF_NULL(outShmPtr);
    return Status::OK();
}

bool ShmCircularQueue::Push(const char *value, uint32_t valueSize)
{
    if (len_ == cap_) {
        LOG(ERROR) << "circular queue is full";
        return false;
    }

    if (cap_ == 0) {
        LOG(ERROR) << "cap_ cant be 0 as a divisor, push failed";
        return false;
    }

    auto ptr = arr_ + (head_ + len_) % cap_ * elementSize_;

    int ret = memcpy_s(ptr, elementSize_, value, valueSize);
    if (ret != EOK) {
        LOG(ERROR) << FormatString("Memory copy failed, the memcpy_s return: %d: ", ret);
        return false;
    }

    // Increment tail and len_.
    len_++;

    return SetQueueSizeToShm();
}

uint32_t ShmCircularQueue::GetAndPopAll()
{
    uint32_t popSize = 0;
    if (len_ == 0) {
        LOG(ERROR) << "circular queue is empty";
        return popSize;
    }

    if (dataHandler_ == nullptr) {
        LOG(ERROR) << "No handler deal the queue data!";
        return popSize;
    }

    if (cap_ == 0) {
        LOG(ERROR) << "cap_ cant be 0 as a divisor, GetAndPopAll failed";
        return popSize;
    }

    uint32_t verifyHeadPos = head_;
    uint32_t verifyLen = len_;
    while (len_ > 0) {
        dataHandler_(arr_ + head_ * elementSize_);
        len_--;
        head_ = (head_ + 1) % cap_;
    }
    if (!SetPosToShm(true, verifyHeadPos, verifyLen)) {
        return popSize;
    }
    popSize = verifyLen;
    return popSize;
}

bool ShmCircularQueue::Front()
{
    if (len_ == 0) {
        LOG(ERROR) << "circular queue is empty";
        return false;
    }

    if (dataHandler_ == nullptr) {
        LOG(ERROR) << "No handler deal the queue data!";
        return false;
    }

    dataHandler_(arr_ + head_ * elementSize_);
    return true;
}

bool ShmCircularQueue::Back()
{
    if (len_ == 0) {
        LOG(ERROR) << "circular queue is empty";
        return false;
    }

    if (cap_ == 0) {
        LOG(ERROR) << "circular queue back failed, cap_ is 0";
        return false;
    }

    if (dataHandler_ == nullptr) {
        LOG(ERROR) << "No handler deal the queue data!";
        return false;
    }

    dataHandler_(arr_ + (head_ + len_ - 1) % cap_ * elementSize_);
    return true;
}

bool ShmCircularQueue::Pop(size_t count)
{
    // Error trying to pop an empty queue.
    if (len_ < count) {
        LOG(ERROR) << "shm circular queue is empty";
        return false;
    }
    if (cap_ == 0) {
        LOG(ERROR) << "increment head_ failed, cap_ cant be 0 as a divisor, pop failed";
        return false;
    }
    // Decrement len_ and increment head_.
    len_ -= count;
    uint32_t verifyHeadPos = head_;
    head_ = (head_ + count) % cap_;
    return SetPosToShm(true, verifyHeadPos);
}

Status ShmCircularQueue::SharedLock(uint64_t timeoutSec)
{
    return queueLock_->RLatch(timeoutSec);
}

void ShmCircularQueue::SharedUnlock()
{
    return queueLock_->UnRLatchWithLockId();
}

void ShmCircularQueue::TrySharedUnlockByLockId(uint32_t lockId)
{
    return queueLock_->UnRLatchWithLockId(lockId, true);
}

Status ShmCircularQueue::WriteLock(uint64_t timeoutSec)
{
    return queueLock_->WLatch(timeoutSec);
}

void ShmCircularQueue::WriteUnlock()
{
    return queueLock_->UnWLatch();
}

Status ShmCircularQueue::GetQueueShmUnit(int &fd, uint64_t &mmapSize, ptrdiff_t &offset, std::string &id)
{
    RETURN_RUNTIME_ERROR_IF_NULL(circularQueueUnit_);
    fd = circularQueueUnit_->GetFd();
    mmapSize = circularQueueUnit_->GetMmapSize();
    offset = static_cast<ptrdiff_t>(circularQueueUnit_->GetOffset());
    id = circularQueueUnit_->GetId();
    return Status::OK();
}

Status ShmCircularQueue::CheckFutexErrno(long res)
{
    static constexpr int FAILED = -1;
    if (res == FAILED && errno == ETIMEDOUT) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Futex wait time out!"));
    } else if (res == FAILED && errno != EAGAIN) {  // EAGAIN : wait compare failed
        auto err = StrErr(errno);
        RETURN_STATUS_LOG_ERROR(K_UNKNOWN_ERROR,
                                FormatString("Futex operate error, errno: %d, errMsg: %s", errno, err));
    }
    return Status::OK();
}

bool ShmCircularQueue::SetPosToShm(bool needSetHead, uint32_t verifyHeadPos, uint32_t skipOne)
{
    bool isAddChange = true;
    if (needSetHead) {
        auto result = SetHeadPosToShm(verifyHeadPos);
        if (!result) {
            return result;
        }
        isAddChange = false;
    }
    return SetQueueSizeToShm(isAddChange, skipOne);
}

bool ShmCircularQueue::SetQueueSizeToShm(bool isAddChange, uint32_t skipOne)
{
    auto verifyData = isAddChange ? len_ - 1 : len_ + 1;
    if (skipOne > 0) {
        verifyData = skipOne;
    }
    if (__atomic_compare_exchange_n(queueSize_, &verifyData, len_, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        return true;
    }
    LOG(ERROR) << "Other process/thread are changing the head, is not safe !" << *queueSize_ << " " << len_;
    return false;
}

bool ShmCircularQueue::SetHeadPosToShm(uint32_t verifyHeadPos)
{
    // Make sure no other threads or processes are changing shm head.
    if (__atomic_compare_exchange_n(queueHead_, &verifyHeadPos, head_, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        return true;
    }
    LOG(ERROR) << "Other process/thread are changing the head, is not safe!";
    return false;
}
}  // namespace datasystem