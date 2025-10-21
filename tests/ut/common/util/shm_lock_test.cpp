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

#include <gtest/gtest.h>
#include <climits>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/safe_shm_lock.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"
#include "securec.h"

namespace datasystem {
namespace ut {

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

class ShmLockTest : public CommonTest {
protected:
    uint32_t lockWord = 0;

    void SetUp() override
    {
        lockWord = 0;
    }
};

TEST_F(ShmLockTest, BasicMutualExclusion)
{
    SafeShmLock lock1(&lockWord, 1);
    DS_ASSERT_OK(lock1.Lock(0));

    std::thread t([&]() {
        SafeShmLock lock2(&lockWord, 2);
        ASSERT_EQ(lock2.Lock(10).GetCode(), K_TRY_AGAIN);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    t.join();
    lock1.UnLock();
}

TEST_F(ShmLockTest, TimeoutFunctionality)
{
    SafeShmLock lock1(&lockWord, 1);
    DS_ASSERT_OK(lock1.Lock(1000));

    auto start = std::chrono::steady_clock::now();
    std::thread t([&]() {
        SafeShmLock lock2(&lockWord, 2);
        ASSERT_EQ(lock2.Lock(100).GetCode(), K_TRY_AGAIN);
    });

    t.join();
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    EXPECT_GE(duration.count(), 100);
    EXPECT_LT(duration.count(), 150);
    lock1.UnLock();
}

TEST_F(ShmLockTest, UnlockAllowsAcquisition)
{
    SafeShmLock lock1(&lockWord, 1);
    DS_ASSERT_OK(lock1.Lock(0));

    std::thread t([&]() {
        SafeShmLock lock2(&lockWord, 2);
        DS_ASSERT_OK(lock2.Lock(100));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    lock1.UnLock();
    t.join();
}

TEST_F(ShmLockTest, ForceUnlockWithCorrectId)
{
    SafeShmLock lock1(&lockWord, 1);
    DS_ASSERT_OK(lock1.Lock(0));

    std::thread t([&]() {
        SafeShmLock lock2(&lockWord, 2);
        DS_ASSERT_OK(lock2.Lock(100));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(SafeShmLock::ForceUnlock(&lockWord, 1));
    t.join();
}

TEST_F(ShmLockTest, ForceUnlockWithWrongIdFails)
{
    SafeShmLock lock1(&lockWord, 1);
    ASSERT_TRUE(lock1.Lock(1000));

    EXPECT_FALSE(SafeShmLock::ForceUnlock(&lockWord, 2));
    EXPECT_FALSE(SafeShmLock::ForceUnlock(&lockWord, 0));

    SafeShmLock lock2(&lockWord, 2);
    EXPECT_FALSE(lock2.Lock(50));

    lock1.UnLock();
}

TEST_F(ShmLockTest, NonBlockingLock)
{
    SafeShmLock lock1(&lockWord, 1);
    DS_ASSERT_OK(lock1.Lock(0));

    SafeShmLock lock2(&lockWord, 2);
    ASSERT_EQ(lock2.Lock(0).GetCode(), K_TRY_AGAIN);

    lock1.UnLock();

    SafeShmLock lock3(&lockWord, 3);
    DS_ASSERT_OK(lock3.Lock(0));
    lock3.UnLock();
}

TEST_F(ShmLockTest, MultiThreadCompetition)
{
    constexpr int threadCount = 5;
    constexpr int loopCount = 1000;
    std::vector<std::thread> threads;
    uint64_t counter = 0;
    std::atomic<bool> start(false);

    SafeShmLock mainLock(&lockWord, 0);
    DS_ASSERT_OK(mainLock.Lock(0));

    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([&, i]() {
            SafeShmLock lock(&lockWord, i + 1);

            while (!start.load()) {
                std::this_thread::yield();
            }
            for (int n = 0; n < loopCount; n++) {
                DS_ASSERT_OK(lock.Lock(INT_MAX));
                counter++;
                lock.UnLock();
            }
        });
    }

    start = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(counter, 0);

    mainLock.UnLock();

    for (auto &t : threads) {
        t.join();
    }

    EXPECT_EQ(counter, threadCount * loopCount);
}

TEST_F(ShmLockTest, UnlockAfterForceUnlock)
{
    SafeShmLock lock1(&lockWord, 1);
    ASSERT_TRUE(lock1.Lock(1000));

    EXPECT_TRUE(SafeShmLock::ForceUnlock(&lockWord, 1));

    lock1.UnLock();

    SafeShmLock lock2(&lockWord, 2);
    EXPECT_TRUE(lock2.Lock(0));
    lock2.UnLock();
}

TEST_F(ShmLockTest, LockStatePersistence)
{
    {
        SafeShmLock lock1(&lockWord, 1);
        DS_ASSERT_OK(lock1.Lock(1000));
    }

    SafeShmLock lock2(&lockWord, 2);
    DS_ASSERT_NOT_OK(lock2.Lock(0));
}

TEST_F(ShmLockTest, TestWakeupDelay)
{
    constexpr int threadCount = 10;
    std::vector<std::thread> threads;
    DS_ASSERT_OK(inject::Set("FutexWait.wake", "25%sleep(1)->25%sleep(2)->25%sleep(3)"));
    SafeShmLock mainLock(&lockWord, 0);
    DS_ASSERT_OK(mainLock.Lock(0));
    std::atomic<bool> exitFlag{ false };
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([&, i]() {
            SafeShmLock lock(&lockWord, i);
            while (!exitFlag) {
                DS_ASSERT_OK(lock.Lock(INT_MAX));
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                lock.UnLock();
            }
        });
    }
    int delayMs = 5;
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    mainLock.UnLock();

    int blockCount = 10;
    while (blockCount > 0) {
        LOG(INFO) << "remain:" << blockCount;
        blockCount--;
        DS_ASSERT_OK(mainLock.Lock(INT_MAX));
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        mainLock.UnLock();
    }
    exitFlag = true;
    for (auto &t : threads) {
        t.join();
    }
}

class ShmLockMixedTest : public CommonTest {
protected:
    uint32_t lockWord = 0;

    void SetUp() override
    {
        lockWord = 0;
    }
};

TEST_F(ShmLockMixedTest, RLockBlocksShmLock)
{
    std::atomic<bool> running{ false };
    std::thread t([&]() {
        running = true;
        SharedMemViewLock rwLock(&lockWord);
        DS_ASSERT_OK(rwLock.LockSharedAndExec([] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }, 0));
    });
    while (!running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SafeShmLock lock(&lockWord, 1);
    DS_ASSERT_NOT_OK(lock.Lock(50));
    DS_ASSERT_OK(lock.Lock(200));
    lock.UnLock();
    t.join();
}

TEST_F(ShmLockMixedTest, MultipleRLocksBlockShmLock)
{
    constexpr int readerCount = 5;
    std::vector<std::thread> readers;
    std::atomic<int> counter(0);
    std::atomic<int> readyCount(0);

    for (int i = 0; i < readerCount; i++) {
        readers.emplace_back([&]() {
            SharedMemViewLock rwLock(&lockWord);
            DS_ASSERT_OK(rwLock.LockSharedAndExec(
                [&]() {
                    readyCount++;
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    counter++;
                },
                0));
        });
    }

    while (readyCount.load() < readerCount) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SafeShmLock lock(&lockWord, 1);
    DS_ASSERT_NOT_OK(lock.Lock(50));
    Timer timer;
    int limitMs = 1000;
    DS_ASSERT_OK(lock.Lock(INT_MAX));
    ASSERT_LT(timer.ElapsedMilliSecond(), limitMs);
    lock.UnLock();
    for (auto &t : readers) {
        t.join();
    }
}

TEST_F(ShmLockMixedTest, WLockBlocksShmLock)
{
    std::atomic<bool> running{ false };
    std::thread t([&]() {
        running = true;
        SharedMemViewLock rwLock(&lockWord);
        DS_ASSERT_OK(
            rwLock.LockExclusiveAndExec([] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }, 0));
    });
    while (!running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SafeShmLock lock(&lockWord, 1);
    DS_ASSERT_NOT_OK(lock.Lock(50));
    Timer timer;
    int limitMs = 1000;
    DS_ASSERT_OK(lock.Lock(INT_MAX));
    ASSERT_LT(timer.ElapsedMilliSecond(), limitMs);
    lock.UnLock();
    t.join();
}

TEST_F(ShmLockMixedTest, ShmLockBlocksRLock)
{
    std::atomic<bool> running{ false };
    SafeShmLock lock(&lockWord, 1);
    DS_ASSERT_OK(lock.Lock(0));

    std::thread t([&]() {
        running = true;
        SharedMemViewLock rwLock(&lockWord);
        std::atomic<int> counter{ 0 };
        DS_ASSERT_NOT_OK(rwLock.LockSharedAndExec([&] { counter++; }, 50));
        DS_ASSERT_OK(rwLock.LockSharedAndExec([&] { counter++; }, 200));
        ASSERT_EQ(counter.load(), 1);
    });
    while (!running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    lock.UnLock();

    t.join();
}

TEST_F(ShmLockMixedTest, ShmLockBlocksWLock)
{
    std::atomic<bool> running{ false };
    SafeShmLock lock(&lockWord, 1);
    DS_ASSERT_OK(lock.Lock(0));

    std::thread t([&]() {
        running = true;
        SharedMemViewLock rwLock(&lockWord);
        std::atomic<int> counter{ 0 };
        DS_ASSERT_NOT_OK(rwLock.LockExclusiveAndExec([&] { counter++; }, 50));
        DS_ASSERT_OK(rwLock.LockExclusiveAndExec([&] { counter++; }, 200));
        ASSERT_EQ(counter.load(), 1);
    });
    while (!running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    lock.UnLock();

    t.join();
}

TEST_F(ShmLockMixedTest, ForceUnlockAllowsRWLock)
{
    SafeShmLock lock(&lockWord, 1);
    DS_ASSERT_OK(lock.Lock(0));

    std::atomic<int> counter(0);

    std::thread rThread([&]() {
        SharedMemViewLock rwLock(&lockWord);
        DS_ASSERT_OK(rwLock.LockExclusiveAndExec([&]() { counter++; }, 200));
    });

    std::thread wThread([&]() {
        SharedMemViewLock rwLock(&lockWord);
        DS_ASSERT_OK(rwLock.LockSharedAndExec([&]() { counter++; }, 200));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(SafeShmLock::ForceUnlock(&lockWord, 1));

    rThread.join();
    wThread.join();

    EXPECT_EQ(counter.load(), 2);
}

TEST_F(ShmLockMixedTest, ConcurrentMixedLockCompetition)
{
    constexpr int threadGroup = 5;
    constexpr int loopCount = 10000;
    constexpr uint32_t timeoutMs = 10000;
    constexpr size_t dataSize = 128;

    char sharedData[dataSize];
    (void)memset_s(sharedData, dataSize, 0, dataSize);

    uint64_t counter = 0;
    std::atomic<uint64_t> readCounter = 0;
    SafeShmLock mainLock(&lockWord, 1);
    DS_ASSERT_OK(mainLock.Lock(timeoutMs));

    std::vector<std::thread> threads;
    std::atomic<bool> start(false);
    for (int i = 0; i < threadGroup; i++) {
        // read locker
        threads.emplace_back([&]() {
            while (!start.load()) {
                std::this_thread::yield();
            }
            for (int n = 0; n < loopCount; n++) {
                SharedMemViewLock rwLock(&lockWord);
                DS_ASSERT_OK(rwLock.LockSharedAndExec(
                    [&]() {
                        readCounter++;
                        for (size_t n = 1; n < dataSize; n++) {
                            ASSERT_EQ(sharedData[0], sharedData[n]);
                        }
                    },
                    timeoutMs));
            }
        });
        // write locker
        threads.emplace_back([&, i]() {
            while (!start.load()) {
                std::this_thread::yield();
            }
            for (int n = 0; n < loopCount; n++) {
                SharedMemViewLock rwLock(&lockWord);
                DS_ASSERT_OK(rwLock.LockExclusiveAndExec(
                    [&]() {
                        counter++;
                        char ch = i + 100;
                        (void)memset_s(sharedData, dataSize, ch, dataSize);
                    },
                    timeoutMs));
            }
        });
        // Exclusive locker
        threads.emplace_back([&, i]() {
            while (!start.load()) {
                std::this_thread::yield();
            }
            uint32_t lockId = 10 + i;
            for (int n = 0; n < loopCount; n++) {
                SafeShmLock lock(&lockWord, lockId);
                DS_ASSERT_OK(lock.Lock(timeoutMs));
                counter++;
                char ch = i + 200;
                (void)memset_s(sharedData, dataSize, ch, dataSize);
                lock.UnLock();
            }
        });
    }

    start = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(counter, 0);
    ASSERT_EQ(readCounter, 0);

    mainLock.UnLock();

    for (auto &t : threads) {
        t.join();
    }

    ASSERT_EQ(counter + readCounter, threads.size() * loopCount);
}

TEST_F(ShmLockMixedTest, TestWakeupDelay)
{
    constexpr int threadCount = 10;
    std::vector<std::thread> threads;
    DS_ASSERT_OK(inject::Set("FutexWait.wake", "25%sleep(1)->25%sleep(2)->25%sleep(3)"));
    const int delayMs = 5;
    std::atomic<bool> exitFlag{ false };
    std::thread rThread([&]() {
        int blockCount = 10;
        while (blockCount > 0) {
            LOG(INFO) << "remain:" << blockCount;
            blockCount--;
            SharedMemViewLock rwLock(&lockWord);
            DS_ASSERT_OK(rwLock.LockExclusiveAndExec(
                [&]() { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); }, INT_MAX));
        }
        exitFlag = true;
    });

    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([&, i]() {
            SafeShmLock lock(&lockWord, i);
            while (!exitFlag) {
                DS_ASSERT_OK(lock.Lock(INT_MAX));
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                lock.UnLock();
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    for (auto &t : threads) {
        t.join();
    }
    rThread.join();
}
}  // namespace ut
}  // namespace datasystem
