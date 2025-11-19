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
 * Description: Usage Monitor test
 */

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include "common.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/lock_map.h"
#include <tbb/concurrent_hash_map.h>
namespace datasystem {
namespace ut {
static constexpr uint64_t DEFAULT_TOTAL_SIZE = 10;
class LockMapTest : public CommonTest {
public:
    LockMapTest()
    {
    }

    ~LockMapTest() override = default;

    void SetUp() override
    {
        FLAGS_v = 2;  // vlog is 2.
        lockMap_ = std::make_unique<LockMap<std::string, int>>();
        CommonTest::SetUp();
    }

    void TearDown() override
    {
        CommonTest::TearDown();
    }

    void JoinThreads()
    {
        if (!writerThreads_.empty()) {
            for (auto &writerThread : writerThreads_) {
                writerThread.join();
            }
        }

        if (!readerThreads_.empty()) {
            for (auto &readerThread : readerThreads_) {
                readerThread.join();
            }
        }
    }

    std::unique_ptr<LockMap<std::string, int>> lockMap_;
    std::vector<std::thread> writerThreads_;
    std::vector<std::thread> readerThreads_;
};

TEST_F(LockMapTest, TestWriteLock)
{
    const int numWriterThreads = 10;
    const int numLoops = 10;
    int count = 0;
    const int interval = 1000;

    for (int threadId = 0; threadId < numWriterThreads; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numLoops, &count]() {
            // Do numLoops iterations of writes from this thread.
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::Accessor lock;
                lockMap_->Insert(lock, "s");
                int temp = count + 1;
                usleep(interval);
                count = temp;
                lockMap_->TryErase(lock);
            }
        }));
    }
    JoinThreads();
    ASSERT_TRUE(count == numWriterThreads * numLoops);
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestPerEntryWriteLock)
{
    const int numWriterThreads = 15;
    const int numLoops = 10;
    const int entries = 3;
    std::vector<int> count(entries);
    const int interval = 1000;

    for (int threadId = 0; threadId < numWriterThreads; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numLoops, &count, &entries]() {
            // Do numLoops iterations of writes from this thread.
            int index = threadId % entries;
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::Accessor lock;
                lockMap_->Insert(lock, "s" + std::to_string(index));
                int temp = count[index] + 1;
                usleep(interval);
                count[index] = temp;
                lockMap_->TryErase(lock);
            }
        }));
    }
    JoinThreads();

    const int numContentionThreads = numWriterThreads / entries;
    for (auto &entry : count) {
        ASSERT_TRUE(entry == numContentionThreads * numLoops);
    }
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestConcurrentPerEntryReadWrite)
{
    const int numWriterThreads = 3;
    const int numReaderThreads = 15;
    const int numLoops = 10;
    const int entries = 3;
    std::vector<int> count(entries);
    const int interval = 1000;

    for (int threadId = 0; threadId < numWriterThreads; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numLoops, &count, &entries]() {
            // Do numLoops iterations of writes from this thread.
            int index = threadId % entries;
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::Accessor lock;
                lockMap_->Insert(lock, "s" + std::to_string(index));
                int temp = count[index] + 1;
                usleep(interval);
                count[index] = temp;
            }
        }));
    }
    for (int threadId = 0; threadId < numReaderThreads; ++threadId) {
        readerThreads_.push_back(std::thread([this, threadId, &numLoops, &count, &entries]() {
            // Do numLoops iterations of reads from this thread.
            int lastCount = 0;
            int index = threadId % entries;
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::ConstAccessor lock;
                if (lockMap_->Find(lock, "s" + std::to_string(index))) {
                    ASSERT_TRUE(count[index] >= lastCount);
                    lastCount = count[index];
                }
            }
        }));
    }
    JoinThreads();

    const int numContentionThreads = numWriterThreads / entries;
    for (auto &entry : count) {
        ASSERT_TRUE(entry == numContentionThreads * numLoops);
    }
    ASSERT_TRUE(lockMap_->Size() == entries);
    for (int i = 0; i < entries; i++) {
        LockMap<std::string, int>::Accessor lock;
        ASSERT_TRUE(lockMap_->Find(lock, "s" + std::to_string(i)));
        ASSERT_TRUE(lockMap_->TryErase(lock));
    }
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestConcurrentPerValueReadWrite)
{
    const int numWriterThreads = 3;
    const int numReaderThreads = 15;
    const int numLoops = 10;
    const int entries = 3;
    const int interval = 1000;

    for (int threadId = 0; threadId < numWriterThreads; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numLoops, &entries]() {
            // Do numLoops iterations of writes from this thread.
            int index = threadId % entries;
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::Accessor lock;
                lockMap_->Insert(lock, "s" + std::to_string(index));
                int temp = lock.entry->data + 1;
                usleep(interval);
                lock.entry->data = temp;
            }
        }));
    }
    for (int threadId = 0; threadId < numReaderThreads; ++threadId) {
        readerThreads_.push_back(std::thread([this, threadId, &numLoops, &entries]() {
            // Do numLoops iterations of reads from this thread.
            int lastCount = 0;
            int index = threadId % entries;
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::ConstAccessor lock;
                if (lockMap_->Find(lock, "s" + std::to_string(index))) {
                    ASSERT_TRUE(lock.entry->data >= lastCount);
                    lastCount = lock.entry->data;
                }
            }
        }));
    }
    JoinThreads();

    const int numContentionThreads = numWriterThreads / entries;
    for (auto &entry : *lockMap_) {
        ASSERT_TRUE(entry.second.data == numContentionThreads * numLoops);
    }
    ASSERT_TRUE(lockMap_->Size() == entries);
    for (int i = 0; i < entries; i++) {
        LockMap<std::string, int>::Accessor lock;
        ASSERT_TRUE(lockMap_->Find(lock, "s" + std::to_string(i)));
        ASSERT_TRUE(lockMap_->TryErase(lock));
    }
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestFind)
{
    LockMap<std::string, int>::Accessor lock;
    // Try erase with invalid accessor
    ASSERT_FALSE(lockMap_->Find(lock, "s"));
    lockMap_->Insert(lock, "s");
    lock.Release();
    ASSERT_TRUE(lockMap_->Find(lock, "s"));
    ASSERT_FALSE(lockMap_->Find(lock, "s1"));
    std::thread thread1([this]() {
        LockMap<std::string, int>::Accessor lock;
        ASSERT_TRUE(lockMap_->Find(lock, "s"));
    });
    lock.Release();
    thread1.join();
    ASSERT_TRUE(lockMap_->Find(lock, "s"));
    ASSERT_TRUE(lockMap_->TryErase(lock));
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestErase)
{
    const int interval = 100000;
    LockMap<std::string, int>::Accessor lock;
    // Try erase with invalid accessor
    ASSERT_FALSE(lockMap_->TryErase(lock));
    lockMap_->Insert(lock, "s");
    
    std::thread thread1([this]() {
        LockMap<std::string, int>::ConstAccessor lock;
        lockMap_->Insert(lock, "s");
    });
    std::thread thread2([this]() {
        LockMap<std::string, int>::Accessor lock;
        lockMap_->Insert(lock, "s");
    });
    std::thread thread3([this]() {
        LockMap<std::string, int>::ConstAccessor lock;
        ASSERT_TRUE(lockMap_->Find(lock, "s"));
    });
    std::thread thread4([this]() {
        LockMap<std::string, int>::Accessor lock;
        ASSERT_TRUE(lockMap_->Find(lock, "s"));
    });
    
    usleep(interval);
    ASSERT_FALSE(lockMap_->TryErase(lock));

    lock.Release();
    thread1.join();
    thread2.join();
    thread3.join();
    thread4.join();
    ASSERT_FALSE(lockMap_->TryErase(lock));
    lockMap_->Insert(lock, "s");
    ASSERT_TRUE(lockMap_->TryErase(lock));
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestBlockingEraseReaders)
{
    // Test BlockingErase will always erase the entry
    const int interval = 1000;
    const int numReaderThreads = 30;
    const int numLoops = 10;
    LockMap<std::string, int>::Accessor lock;
    lockMap_->Insert(lock, "s");
    for (int threadId = 0; threadId < numReaderThreads; ++threadId) {
        readerThreads_.push_back(std::thread([this, threadId, &numLoops]() {
            // Do numLoops iterations of reads from this thread.
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::ConstAccessor lock;
                if (lockMap_->Find(lock, "s")) {
                    usleep(interval);
                } else {
                    ASSERT_TRUE(lockMap_->Size() == 0);
                }
            }
        }));
    }
    usleep(interval);
    lockMap_->BlockingErase(lock);
    JoinThreads();
    ASSERT_TRUE(lockMap_->Size() == 0);
}

TEST_F(LockMapTest, TestBlockingEraseWriters)
{
    // Test BlockingErase will always erase the entry
    const int interval = 1000;
    const int numWriterThreads = 2;
    const int numLoops = 10;
    LockMap<std::string, int>::Accessor lock;
    lockMap_->Insert(lock, "s");
    for (int threadId = 0; threadId < numWriterThreads; ++threadId) {
        writerThreads_.push_back(std::thread([this, threadId, &numLoops]() {
            // Do numLoops iterations of reads from this thread.
            for (int i = 0; i < numLoops; ++i) {
                LockMap<std::string, int>::Accessor lock;
                if (lockMap_->Find(lock, "s")) {
                    usleep(interval);
                } else {
                    ASSERT_TRUE(lockMap_->Size() == 0);
                }
            }
        }));
    }
    usleep(interval);
    lockMap_->BlockingErase(lock);
    JoinThreads();
    ASSERT_TRUE(lockMap_->Size() == 0);
}
}  // namespace ut
}  // namespace datasystem
