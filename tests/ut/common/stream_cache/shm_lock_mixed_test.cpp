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
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/stream_cache/cursor.h"
#include "datasystem/common/util/safe_shm_lock.h"
#include "datasystem/utils/status.h"
#include "securec.h"

namespace datasystem {
namespace ut {

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
