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
 * Description: Implementation of spin-lock based on shared memory.
 */
#include "datasystem/common/object_cache/lock.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

static constexpr int FAILED = -1;
static constexpr int NO_LOCK_NUM = 0;
static constexpr int WRITE_LOCK_NUM = 1;
static constexpr int READ_LOCK_NUM = 2;
static constexpr uint32_t BYTES = 0x8;
static constexpr int DEBUG_LOG_LEVEL = 2;

namespace datasystem {
namespace object_cache {
Status Lock::WLock(const uint64_t timeoutSec, uint32_t &lockFlag)
{
    (void)timeoutSec;
    (void)lockFlag;
#if 0
    Timer timer;
    uint64_t useTimeSec = 0;
    while (true) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckLatch(timeoutSec, lockFlag, useTimeSec, WRITE_LOCK_NUM), "");

        uint32_t expectedVal = NO_LOCK_NUM;  // Expectation: no lock.
        // Compare and set the write lock.
        if (__atomic_compare_exchange_n(&lockFlag, &expectedVal, WRITE_LOCK_NUM, true, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST)) {
            Update(std::this_thread::get_id(), WRITE_LOCK_NUM);
            return Status::OK();
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckTimeout(timer, useTimeSec, timeoutSec), "");
    }
#endif
    return Status::OK();
}

void Lock::UnWLock(uint32_t &lockFlag, std::function<void()> extendedOperation, std::thread::id tid)
{
    (void)lockFlag;
    (void)extendedOperation;
    (void)tid;
#if 0
    if ((__atomic_load_n(&lockFlag, __ATOMIC_RELAXED) & 1) && Find(tid)) {
        uint32_t expectedVal = WRITE_LOCK_NUM;
        if (__atomic_compare_exchange_n(&lockFlag, &expectedVal, NO_LOCK_NUM, false, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST)) {
            VLOG(DEBUG_LOG_LEVEL) << "Success to unlock the write lock";
            Update(tid, 0 - WRITE_LOCK_NUM);
            if (extendedOperation != nullptr) {
                extendedOperation();
            }
            FutexWake(&lockFlag);
        }
    }
#endif
}

Status Lock::RLock(uint64_t timeoutSec, uint32_t &lockFlag)
{
    (void)timeoutSec;
    (void)lockFlag;
#if 0
    Timer timer;
    uint64_t useTimeSec = 0;
    while (true) {
        RETURN_IF_NOT_OK(CheckLatch(timeoutSec, lockFlag, useTimeSec, READ_LOCK_NUM));
        uint32_t shmData = __atomic_load_n(&lockFlag, __ATOMIC_RELAXED);
        if (shmData & 1) {
            continue;
        }
        // to avoid write lock atomic change the lockFlag
        if (__atomic_compare_exchange_n(&lockFlag, &shmData, shmData + READ_LOCK_NUM, true, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST)) {
            Update(std::this_thread::get_id(), READ_LOCK_NUM);
            return Status::OK();
        }
        RETURN_IF_NOT_OK(CheckTimeout(timer, useTimeSec, timeoutSec));
    }
#endif
    return Status::OK();
}

bool Lock::TryRLock(uint32_t &lockFlag)
{
    (void)lockFlag;
#if 0
    if (__atomic_load_n(&lockFlag, __ATOMIC_SEQ_CST) == WRITE_LOCK_NUM) {
        return false;
    }
    if (!(__atomic_add_fetch(&lockFlag, READ_LOCK_NUM, __ATOMIC_SEQ_CST) & 1)) {
        Update(std::this_thread::get_id(), READ_LOCK_NUM);
        return true;
    }
    __atomic_sub_fetch(&lockFlag, READ_LOCK_NUM, __ATOMIC_SEQ_CST);
#endif
    return true;
}

void Lock::UnRLock(uint32_t &lockFlag, std::function<void()> extendedOperation, std::thread::id tid)
{
    (void)lockFlag;
    (void)extendedOperation;
    (void)tid;
#if 0
    uint32_t curLockVal = __atomic_load_n(&lockFlag, __ATOMIC_RELAXED);
    if (curLockVal > WRITE_LOCK_NUM && Find(tid)) {
        Update(tid, 0 - READ_LOCK_NUM);
        VLOG(DEBUG_LOG_LEVEL) << "Success to unlock the read lock";
        if (extendedOperation != nullptr) {
            extendedOperation();
        }
        // Only all readers have done and the lock value equal to zero, other waiters can wake up.
        if (__atomic_sub_fetch(&lockFlag, READ_LOCK_NUM, __ATOMIC_SEQ_CST) == NO_LOCK_NUM) {
            FutexWake(&lockFlag);
        }
    }
#endif
}

Status Lock::CheckFutexWaitAndLogError(uint32_t val, const struct timespec &timeout, uint32_t &flag)
{
    auto res = FutexWait(&flag, val, &timeout);
    // 1. FutexWait fail when timeout
    // 2. FutexWait fail in other cases, and retry WLatch
    if (res == FAILED && errno == ETIMEDOUT) {
        return Status(K_RUNTIME_ERROR, "futex wait timeout");
    } else if (res == FAILED) {
        auto err = StrErr(errno);
        // When the flag doesn't need to wait, this error is reported, so we need to ignore errno 11 (Resource
        // temporarily unavailable).
        if (errno != EAGAIN) {
            LOG(WARNING) << "Futex wait error, errno:" << errno << ", msg: " << err;
        }
    }
    return Status::OK();
}

Status Lock::CheckLatch(const uint64_t timeoutSec, uint32_t &lockFlag, uint64_t useTimeSec, uint32_t lockNum)
{
    struct timespec timeoutStruct = { .tv_sec = static_cast<long int>(timeoutSec - useTimeSec), .tv_nsec = 0 };
    uint32_t curLockVal = __atomic_load_n(&lockFlag, __ATOMIC_RELAXED);
    // case1: Wlatch to wait.
    // case2: Rlatch to wait.
    if ((curLockVal && lockNum == WRITE_LOCK_NUM) || (curLockVal == WRITE_LOCK_NUM && lockNum == READ_LOCK_NUM)) {
        RETURN_IF_NOT_OK_APPEND_MSG(CheckFutexWaitAndLogError(curLockVal, timeoutStruct, lockFlag), "WLatch timeout");
    }
    return Status::OK();
}

Status Lock::CheckTimeout(Timer &timer, uint64_t &useTimeSec, const uint64_t timeoutSec)
{
    useTimeSec = static_cast<uint64_t>(timer.ElapsedSecond());
    if (timeoutSec <= useTimeSec) {
        return Status(K_RUNTIME_ERROR, "Wlatch timeout");
    }
    return Status::OK();
}

long Lock::FutexWait(uint32_t *pointer, uint32_t val, const struct timespec *timeout)
{
    return syscall(SYS_futex, pointer, FUTEX_WAIT, val, timeout, nullptr, 0);
}

long Lock::FutexWake(uint32_t *pointer)
{
    return syscall(SYS_futex, pointer, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0);
}

void Lock::Update(const std::thread::id &id, int lockVal)
{
    std::unique_lock<std::shared_timed_mutex> writeLock(threadIDTableMutex_);
    if (threadIds_.count(id) > 0) {
        threadIds_[id] += lockVal;
        if (threadIds_[id] == 0) {
            threadIds_.erase(id);
        }
    } else {
        threadIds_.emplace(id, lockVal);
    }
}

bool Lock::Find(const std::thread::id &id)
{
    std::shared_lock<std::shared_timed_mutex> readLock(threadIDTableMutex_);
    return threadIds_.count(id) > 0;
}

CommonLock::CommonLock(uint32_t *lockFlag)
{
    if (lockFlag == nullptr) {
        lockFlag_ = &localFlag_;
    } else {
        lockFlag_ = lockFlag;
    }
}

Status CommonLock::WLatch(uint64_t timeoutSec)
{
    return WLock(timeoutSec, *lockFlag_);
}

void CommonLock::UnWLatch()
{
    return UnWLock(*lockFlag_);
}

Status CommonLock::RLatch(uint64_t timeoutSec)
{
    return RLock(timeoutSec, *lockFlag_);
}

bool CommonLock::TryRLatch()
{
    return TryRLock(*lockFlag_);
}

void CommonLock::UnRLatch()
{
    return UnRLock(*lockFlag_);
}

ShmLock::ShmLock(void *pointer, uint32_t size, uint32_t lockId)
    : Lock(), pointer_(pointer), size_(size), lockId_(lockId)
{
}

Status ShmLock::Init()
{
    uint8_t *tail = (uint8_t *)pointer_ + size_;
    flag_ = (uint32_t *)pointer_;
    // Client lockId_ start from 1, but worker lockId_ got 0.
    recorder_ = (uint8_t *)pointer_ + sizeof(uint32_t) + (lockId_ / BYTES);
    if (recorder_ >= tail) {
        RETURN_STATUS(K_RUNTIME_ERROR, "memory access failed");
    }
    addMask_ = 1 << (lockId_ % BYTES);
    subMask_ = 0xFF ^ addMask_;
    return Status::OK();
}

Status ShmLock::WLatch(uint64_t timeoutSec)
{
    RETURN_IF_NOT_OK(WLock(timeoutSec, *flag_));
    UpdateRecorder(true);
    return Status::OK();
}

void ShmLock::UnWLatch()
{
    return UnWLock(*flag_, std::bind(&ShmLock::UpdateRecorder, this, false));
}

void ShmLock::UnWLatch(std::thread::id tid)
{
    return UnWLock(*flag_, std::bind(&ShmLock::UpdateRecorder, this, false), tid);
}

Status ShmLock::RLatch(uint64_t timeoutSec)
{
    RETURN_IF_NOT_OK(RLock(timeoutSec, *flag_));
    UpdateRecorder(true);
    return Status::OK();
}

bool ShmLock::TryRLatch()
{
    if (TryRLock(*flag_)) {
        UpdateRecorder(true);
        return true;
    } else {
        return false;
    }
}

void ShmLock::UnRLatch()
{
    return UnRLock(*flag_, std::bind(&ShmLock::UpdateRecorder, this, false));
}

void ShmLock::UnRLatch(std::thread::id tid)
{
    return UnRLock(*flag_, std::bind(&ShmLock::UpdateRecorder, this, false), tid);
}

PackageLock::PackageLock(void *pointer, uint32_t lockId) : CommonLock((uint32_t *)pointer), lockId_(lockId)
{
    packagePtr = (uint64_t *)pointer;
    lockFlag = (uint32_t *)packagePtr;
    addMask_ = 1 << (lockId_ % BYTES);
    subMask_ = 0xFF ^ addMask_;
}

Status PackageLock::RLatch(uint64_t timeoutSec)
{
    Timer timer;
    uint64_t useTimeSec = 0;
    uint64_t exchangeData = 0;
    while (true) {
        RETURN_IF_NOT_OK(CheckLatch(timeoutSec, *lockFlag, useTimeSec, READ_LOCK_NUM));
        uint64_t shmData = __atomic_load_n(packagePtr, __ATOMIC_RELAXED);

        exchangeData = shmData;
        uint32_t *tmpFlag = (uint32_t *)(&exchangeData);
        *tmpFlag += READ_LOCK_NUM;
        uint8_t *tmpMap = (uint8_t *)(tmpFlag + 1);
        if (*tmpFlag & 1) {
            continue;
        }
        tmpMap = tmpMap + (lockId_ / BYTES);
        *tmpMap |= addMask_;

        // to avoid write lock atomic change the lockFlag
        if (__atomic_compare_exchange_n(packagePtr, &shmData, exchangeData, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            Update(std::this_thread::get_id(), READ_LOCK_NUM);
            return Status::OK();
        }
        RETURN_IF_NOT_OK(CheckTimeout(timer, useTimeSec, timeoutSec));
    }
}

void PackageLock::UnRLatchWithLockId(int64_t lockId, bool ignoreTid)
{
    uint32_t unlockId = lockId_;
    uint8_t addMask = addMask_;
    uint8_t subMask = subMask_;
    if (lockId > 0) {
        unlockId = static_cast<uint32_t>(lockId);
        addMask = 1 << (lockId % BYTES);
        subMask = 0xFF ^ addMask;
    }

    std::thread::id tid = std::this_thread::get_id();
    uint64_t retryNum = 0;
    do {
        uint64_t curShmData = __atomic_load_n(packagePtr, __ATOMIC_RELAXED);
        uint64_t exchangeData = curShmData;
        uint32_t *exchangeFlag = (uint32_t *)(&exchangeData);
        VLOG(1) << "Curr flag is : " << *exchangeFlag;
        bool checkTid = ignoreTid ? true : Find(tid);
        if (*exchangeFlag <= WRITE_LOCK_NUM || !checkTid) {
            VLOG(1) << "No shared lock find!";
            break;
        }
        uint8_t *exchangeMap = (uint8_t *)(exchangeFlag + 1);
        exchangeMap = exchangeMap + (unlockId / BYTES);  // find the world.
        if (!(*exchangeMap & addMask)) {
            VLOG(1) << "This process has not been locked!";
            break;
        }
        *exchangeFlag -= READ_LOCK_NUM;
        *exchangeMap &= subMask;
        if (__atomic_compare_exchange_n(packagePtr, &curShmData, exchangeData, true, __ATOMIC_SEQ_CST,
                                        __ATOMIC_SEQ_CST)) {
            FutexWake(lockFlag);
            Update(tid, 0 - READ_LOCK_NUM);
            break;
        }
        retryNum++;
    } while (true);
    VLOG(1) << FormatString("Unlock %lld finish, and CAS operate retry time is : %llu", lockId, retryNum);
}
}  // namespace object_cache
}  // namespace datasystem