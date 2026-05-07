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
 * Description: Testing read-write lock
 */
#include <cstdlib>
#include <string>
#include <vector>

#include <securec.h>

#include "datasystem/common/object_cache/lock.h"
#include "ut/common.h"

static constexpr char READ = 'r';
static constexpr char WRITE = 'w';

namespace datasystem {
namespace ut {
class RWLockTest : public CommonTest {};

class RWLockTestObject {
public:
    explicit RWLockTestObject(int incTimes)
        : incTimes_(incTimes), criticalVal_(0), readCounter_(0), lockFrame_(nullptr), lk_(nullptr)
    {
        size_t size = sizeof(uint32_t) + sizeof(uint8_t);
        lockFrame_ = malloc(size);
        memset_s(lockFrame_, size, 0, size);
        lk_ = std::make_shared<datasystem::object_cache::ShmLock>(lockFrame_, size, 0);
        lk_->Init();
    }

    RWLockTestObject(RWLockTestObject &&other) noexcept
        : incTimes_(other.incTimes_),
          criticalVal_(other.criticalVal_),
          readCounter_(other.readCounter_),
          lockFrame_(other.lockFrame_),
          lk_(other.lk_)
    {
    }

    RWLockTestObject(const RWLockTestObject &other) = delete;

    ~RWLockTestObject()
    {
        if (lockFrame_) {
            free(lockFrame_);
        }
    }

    void IncreaseCriticalVal()
    {
        for (int i = 0; i < incTimes_; i++) {
            DS_ASSERT_OK(lk_->WLatch(60));
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            criticalVal_++;
            lk_->UnWLatch();
        }
    }

    void ReadCriticalVal()
    {
        DS_ASSERT_OK(lk_->RLatch(60));
        __atomic_add_fetch(&readCounter_, 1, __ATOMIC_SEQ_CST);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        lk_->UnRLatch();
    }

    int GetCriticalValue()
    {
        return criticalVal_;
    }

    int GetReaderCounter()
    {
        return readCounter_;
    }

    int GetLockFrameVal()
    {
        return *(static_cast<int *>(lockFrame_));
    }

    void UnmatchedLockUnlock(char ops)
    {
        if (ops == WRITE) {
            DS_ASSERT_OK(lk_->WLatch(60));
            lk_->UnRLatch();
        }
        if (ops == READ) {
            lk_->UnWLatch();
            DS_ASSERT_OK(lk_->RLatch(60));
            lk_->UnWLatch();
        }
    }

    void LockValueOverflowFail(char ops)
    {
        if (ops == 'w') {
            DS_ASSERT_OK(lk_->RLatch(60));
            lk_->UnRLatch();
            lk_->UnRLatch();
        }
        if (ops == 'r') {
            DS_ASSERT_OK(lk_->WLatch(60));
            lk_->UnWLatch();
            lk_->UnWLatch();
        }
    }

    void ThreadALock()
    {
        for (int i = 0; i < incTimes_; i++) {
            DS_ASSERT_OK(lk_->RLatch(60));
        }
    }

    void ThreadBUnLock()
    {
        lk_->UnRLatch();
    }

private:
    int incTimes_;
    int criticalVal_;
    int readCounter_;
    void *lockFrame_;
    std::shared_ptr<datasystem::object_cache::Lock> lk_;
};

TEST_F(RWLockTest, ConcurrentWriteSuccess)
{
    LOG(INFO) << "Test whether the rwlock can protect the critical section from concurrent writing";
    int numWriters = 100, incTimes = 100;
    RWLockTestObject RWT(incTimes);
    std::vector<std::thread> writersPool;
    for (int i = 0; i < numWriters; ++i) {
        writersPool.push_back(std::move(std::thread(&RWLockTestObject::IncreaseCriticalVal, &RWT)));
    }
    for (auto &w : writersPool) {
        w.join();
    }
    int criticalVal = RWT.GetCriticalValue();
    int lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(criticalVal, numWriters * incTimes);
    EXPECT_EQ(lockVal, 0);
}

TEST_F(RWLockTest, ConcurrentReadSuccess)
{
    LOG(INFO) << "Test whether the rwlock can allow concurrent read of the critical section";
    int numReaders = 100, incTimes = 1;
    RWLockTestObject RWT(incTimes);
    std::vector<std::thread> readersPool;
    for (int i = 0; i < numReaders; ++i) {
        readersPool.push_back(std::move(std::thread(&RWLockTestObject::ReadCriticalVal, &RWT)));
    }
    for (auto &r : readersPool) {
        r.join();
    }
    int readCnt = RWT.GetReaderCounter();
    int lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(readCnt, numReaders);
    EXPECT_EQ(lockVal, 0);
}

TEST_F(RWLockTest, ConcurrentReadWriteSuccess)
{
    LOG(INFO) << "Test the effectiveness of rwlock in the scenario of concurrent read and write interleaving";
    int numReaders = 100, numWriters = 100, incTimes = 100;
    RWLockTestObject RWT(incTimes);
    std::vector<std::thread> readersWritersPool;
    for (int i = 0, j = 0; i < numWriters || j < numReaders; ++i, ++j) {
        if (i < numWriters) {
            readersWritersPool.push_back(std::move(std::thread(&RWLockTestObject::IncreaseCriticalVal, &RWT)));
        }
        if (j < numReaders) {
            readersWritersPool.push_back(std::move(std::thread(&RWLockTestObject::ReadCriticalVal, &RWT)));
        }
    }
    for (auto &rw : readersWritersPool) {
        rw.join();
    }
    int criticalVal = RWT.GetCriticalValue();
    int readCnt = RWT.GetReaderCounter();
    int lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(criticalVal, numWriters * incTimes);
    EXPECT_EQ(readCnt, numReaders);
    EXPECT_EQ(lockVal, 0);
}

TEST_F(RWLockTest, UnmatchedLockUnlockFail)
{
    LOG(INFO) << "Test unmatched Lock call fail";
    int incTimes = 1;
    RWLockTestObject RWT(incTimes);
    RWT.UnmatchedLockUnlock('w');
    int lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(lockVal, 1);
    RWT.UnmatchedLockUnlock('r');
    lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(lockVal, 2);
}

TEST_F(RWLockTest, LockValueOverflowFail)
{
    LOG(INFO) << "Test lock value overflow fail";
    int incTimes = 1;
    RWLockTestObject RWT(incTimes);
    RWT.LockValueOverflowFail(WRITE);
    int lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(lockVal, 0);
    RWT.LockValueOverflowFail(READ);
    lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(lockVal, 0);
}

TEST_F(RWLockTest, ThreadAUnlockThreadBFail)
{
    LOG(INFO) << "Test thread A unlocks thread B fail";
    int numReaders = 100, incTimes = 100;
    RWLockTestObject RWT(incTimes);
    std::thread t1(&RWLockTestObject::ThreadALock, &RWT);
    std::vector<std::thread> readersPool;
    for (int i = 0; i < numReaders; ++i) {
        readersPool.push_back(std::move(std::thread(&RWLockTestObject::ThreadBUnLock, &RWT)));
    }
    t1.join();
    for (auto &r : readersPool) {
        r.join();
    }
    int lockVal = RWT.GetLockFrameVal();
    EXPECT_EQ(lockVal, 2 * incTimes);
}

}  // namespace ut
}  // namespace datasystem
