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
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_LOCK_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_LOCK_H

#include <climits>
#include <cstdint>
#include <errno.h>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class Lock {
public:
    Lock() = default;

    virtual ~Lock() = default;

    /**
     * @brief Init lock.
     * @return Status of the call.
     */
    virtual Status Init()
    {
        return Status::OK();
    }

    /**
     * @brief Write lock, at most one writer at any time
     * @param[in] timeoutSec Timeout(seconds) for the lock operation
     * @return Status of the call.
     */
    virtual Status WLatch(uint64_t timeoutSec) = 0;

    /**
     * @brief Write unlock
     */
    virtual void UnWLatch() = 0;

    /**
     * @brief Read lock, can have multiple readers at a certain time
     * @param[in] timeoutSec Timeout(seconds) for the unlock operation
     * @return Status of the call.
     */
    virtual Status RLatch(uint64_t timeoutSec) = 0;

    /**
     * @brief Try to acquire read lock, can have multiple readers at a certain time
     * @return Return true if acquire read lock success
     */
    virtual bool TryRLatch() = 0;

    /**
     * @brief Read unlock
     */
    virtual void UnRLatch() = 0;

    /**
     * @brief Sleep to wait when cannot acquire the lock.
     * @param[in] pointer Store futex value which is used to indicate the lock.
     * @param[in] val When *pointer == val, go to sleep.
     * @param[in] timeout A structure used to set the timeout.
     * @return -1: error; 0: if the caller was woken up. Return of futex syscall.
     */
    static long FutexWait(uint32_t *pointer, uint32_t val, const struct timespec *timeout);

    /**
     * @brief Wake up the waiting processes/threads.
     * @param[in] pointer Store futex value which is used to indicate the lock.
     * @return The number of waiters that were woken up.
     */
    static long FutexWake(uint32_t *pointer);

protected:
    /**
     * @brief Write lock, at most one writer at any time.
     * @param[in] timeoutSec Timeout(seconds) for the lock operation.
     * @param[in] lockFlag Lock flag to check lock status.
     * @param[in] isShm isShm buffer or not.
     * @return Status of the call.
     */
    Status WLock(const uint64_t timeoutSec, uint32_t &lockFlag);

    /**
     * @brief Write unlock
     * @param[in] lockFlag Lock flag to check lock status.
     * @param[in] extendedOperation The extend operation..
     * @param[in] tid The thread id.
     */
    void UnWLock(uint32_t &lockFlag, std::function<void()> extendedOperation = nullptr,
                 std::thread::id tid = std::this_thread::get_id());

    /**
     * @brief Read lock, can have multiple readers at a certain time
     * @param[in] timeoutSec Timeout(seconds) for the unlock operation
     * @param[in] lockFlag Lock flag to check lock status.
     * @param[in] isShm isShm buffer or not.
     * @return Status of the call.
     */
    Status RLock(uint64_t timeoutSec, uint32_t &lockFlag);

    /**
     * @brief Try to acquire read lock, can have multiple readers at a certain time
     * @param[in] lockFlag Lock flag to check lock status.
     * @param[in] isShm isShm buffer or not.
     * @return Return true if acquire read lock success
     */
    bool TryRLock(uint32_t &lockFlag);

    /**
     * @brief Read unlock
     * @param[in] lockFlag Lock flag to check lock status.
     * @param[in] extendedOperation The extend operation.
     * @param[in] tid The thread id.
     */
    void UnRLock(uint32_t &lockFlag, std::function<void()> extendedOperation = nullptr,
                 std::thread::id tid = std::this_thread::get_id());

    /**
     * @brief Check futex wait and log error.
     * @param[in] val When futex wait *pointer == val, go to sleep.
     * @param[in] timeout A structure used to set the timeout.
     * @param[in] flag Lock flag to check lock status.
     * @return Status::K_RUNTIME_ERROR when timeout, log error and return ok in other cases.
     */
    Status CheckFutexWaitAndLogError(uint32_t val, const struct timespec &timeout, uint32_t &flag);

    /**
     * Check latch status to lock.
     * @param timeoutSec Timeout(seconds) for the lock operation
     * @param lockFlag Lock flag to check lock status.
     * @param useTimeSec Use time from lock to now.
     * @param lockNum Latch type 1 : wlatch, 2: rlatch.
     * @return Status of the call.
     */
    Status CheckLatch(const uint64_t timeoutSec, uint32_t &lockFlag, uint64_t useTimeSec, uint32_t lockNum);
    /**
     * Check timeout or not.
     * @param[in] timer A timepiece.
     * @param[out] useTimeSec Use time from lock to now.
     * @param[in] timeoutSec Timeout(seconds) for the lock operation
     * @return Status of the call.
     */
    Status CheckTimeout(Timer &timer, uint64_t &useTimeSec, const uint64_t timeoutSec);

    /**
     * @brief Update the value of the key, if its value equals to 0 (after the update), delete the key.
     * @param[in] id Current thread ID.
     * @param[in] lockval 1 or -1 for write lock, 2 or -2 for read lock.
     */
    void Update(const std::thread::id &id, int lockVal);

    /**
     * @brief Find if a certain key(thread ID) exists in the hash table.
     * @param[in] id Current thread ID.
     * @return True if the id exists, false otherwise.
     */
    bool Find(const std::thread::id &id);

    std::shared_timed_mutex threadIDTableMutex_;
    std::unordered_map<std::thread::id, int> threadIds_;  // map current thread ID to lock frame value
};

class CommonLock : public Lock {
public:
    /**
     * @brief Constructor for class CommonLock.
     * @param[in] lockFlag It can lock pointers to shared memory, and it can also construct flags for memory variables
     * with default values
     */
    explicit CommonLock(uint32_t *lockFlag = nullptr);

    ~CommonLock() override = default;

    /**
     * @brief Write lock, at most one writer at any time
     * @param[in] timeoutSec Timeout(seconds) for the lock operation
     * @return status of the operation
     */
    Status WLatch(uint64_t timeoutSec) override;

    /**
     * @brief Write unlock
     * @return status of the operation
     */
    void UnWLatch() override;

    /**
     * @brief Read lock, can have multiple readers at a certain time
     * @param[in] timeoutSec Timeout(seconds) for the unlock operation
     * @return status of the operation
     */
    Status RLatch(uint64_t timeoutSec) override;

    /**
     * @brief Try to acquire read lock, can have multiple readers at a certain time
     * @return Return true if acquire read lock success
     */
    bool TryRLatch() override;

    /**
     * @brief Read unlock
     * @return status of the operation
     */
    void UnRLatch() override;

private:
    uint32_t localFlag_ = 0;
    uint32_t *lockFlag_{ nullptr };
};

class ShmLock : public Lock {
public:
    ShmLock(void *pointer, uint32_t size, uint32_t lockId);

    ~ShmLock() override = default;

    /**
     * @brief Init lock.
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Write lock, at most one writer at any time.
     * @param[in] timeoutSec Timeout(seconds) for the lock operation.
     * @return Status of the operation.
     */
    Status WLatch(uint64_t timeoutSec) override;

    /**
     * @brief Write unlock.
     * @return Status of the operation.
     */
    void UnWLatch() override;

    /**
     * @brief Write unlock.
     * @param[in] tid The thread id.
     * @return Status of the operation.
     */
    void UnWLatch(std::thread::id tid);

    /**
     * @brief Read lock, can have multiple readers at a certain time.
     * @param[in] timeoutSec Timeout(seconds) for the unlock operation.
     * @return status of the operation.
     */
    Status RLatch(uint64_t timeoutSec) override;

    /**
     * @brief Try to acquire read lock, can have multiple readers at a certain time
     * @return Return true if acquire read lock success
     */
    bool TryRLatch() override;

    /**
     * @brief Read unlock.
     * @return Status of the operation.
     */
    void UnRLatch() override;

    /**
     * @brief Read unlock.
     * @param[in] tid The thread id.
     * @return Status of the operation.
     */
    void UnRLatch(std::thread::id tid);

private:
    void UpdateRecorder(bool add)
    {
        (void)add;
#if 0
        if (add) {
            __atomic_or_fetch(recorder_, addMask_, __ATOMIC_SEQ_CST);
        } else {
            __atomic_and_fetch(recorder_, subMask_, __ATOMIC_SEQ_CST);
        }
#endif
    }

    void *pointer_;

    uint32_t size_;

    uint32_t lockId_;

    // Value means: 0: no lock; 1: has write lock; even number: has read lock.
    // Increase by 2 each time a read lock pointer is set.
    uint32_t *flag_{ nullptr };

    // Record which client handle the lock.
    uint8_t *recorder_{ nullptr };

    uint8_t addMask_{ 0 };

    uint8_t subMask_{ 0 };
};

/**
 * Combination lock with locking information, combining flag and lockid through atomic, other processes can unlock flag
 * through lockid, the same process only supports holding one lock at the same time.
 */
class PackageLock : public CommonLock {
public:
    PackageLock(void *pointer, uint32_t lockId = 0);

    ~PackageLock() override = default;

    /**
     * @brief Read lock with lockId.
     * @param[in] timeoutSec The timeout of read lock.
     * @return
     */
    Status RLatch(uint64_t timeoutSec) override;

    /**
     * @brief Read unlock.
     * @param[in] lockId Marking the ID of a lock that exists.
     * @param[in] ignoreTid Whether or not to ignore the threadId that logged this lock.
     * @return
     */
    void UnRLatchWithLockId(int64_t lockId = -1, bool ignoreTid = false);

private:
    uint64_t *packagePtr;

    uint32_t *lockFlag;

    uint32_t lockId_;

    uint8_t addMask_{ 0 };

    uint8_t subMask_{ 0 };
};
}  // namespace object_cache
}  // namespace datasystem

#endif