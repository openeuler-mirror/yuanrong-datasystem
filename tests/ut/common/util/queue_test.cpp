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
 * Description: Test of queue
 */
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <thread>

#include "datasystem/common/util/queue/circular_queue.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/queue/shm_circular_queue.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/uuid_generator.h"
#include "common.h"

namespace datasystem {
namespace ut {
class QueueTest : public CommonTest {
};

TEST_F(QueueTest, TestConcurrency1)
{
    // Create an integer queue with capacity 5
    int kCapacity = 5;
    Queue<int64_t> q(kCapacity);
    WaitPost t1_wp;
    WaitPost full_wp;
    // Start a thread and push more elements than the queue capacity.
    // We should block until the main thread retrieve them.
    std::thread t1([kCapacity, &t1_wp, &full_wp, &q]() {
        // Wait for the main thread to signal to proceed.
        t1_wp.Wait();
        for (auto i = 0; i < kCapacity; ++i) {
            std::cout << "Push " << i << " into the queue" << std::endl;
            ASSERT_TRUE(q.Put(i));
        }
        // At this point, we should get error if we call the other form of Put.
        ASSERT_TRUE(q.Add(kCapacity).GetCode() == StatusCode::K_TRY_AGAIN);
        // Wake up the main thread before we block ourself to push one more element.
        full_wp.Set();
        ASSERT_TRUE(q.Put(kCapacity));
    });
    // The thread t1 will wait for us to signal. The queue should be empty.
    // Test the Remove api which should give us error.
    int64_t v;
    ASSERT_TRUE(q.Remove(&v).GetCode() == StatusCode::K_TRY_AGAIN);
    // Now tell t1 to proceed
    t1_wp.Set();
    // Wait until the queue is full
    full_wp.Wait();
    for (auto i = 0; i < kCapacity + 1; ++i) {
        ASSERT_TRUE(q.Take(&v));
        EXPECT_EQ(v, i);
    }
    t1.join();
    EXPECT_TRUE(q.Empty());
}

TEST_F(QueueTest, TestTimeout)
{
    // Create an integer queue with capacity 5
    int kCapacity = 5;
    Queue<int64_t> q(kCapacity);
    // It is empty.
    int64_t v;
    // Pop with a timeout 5s
    ASSERT_TRUE(q.Poll(&v, 5000).GetCode() == StatusCode::K_TRY_AGAIN);
    // Fill up the queue
    for (int i = 0; i < kCapacity; ++i) {
        ASSERT_TRUE(q.Put(i));
    }
    // We should get an error of putting one more if we use the Offer version
    ASSERT_TRUE(q.Offer(kCapacity, 5000).GetCode() == StatusCode::K_TRY_AGAIN);
}

TEST_F(QueueTest, TestSimpleString)
{
    // Create an string queue with capacity 5
    int kCapacity = 5;
    Queue<std::string> q(kCapacity);
    // Start a thread to push a string.
    std::thread t1([&q]() { ASSERT_TRUE(q.Put("Hello World")); });
    std::string v;
    EXPECT_TRUE(q.Take(&v));
    std::cout << v << std::endl;
    EXPECT_EQ(strcmp(v.data(), "Hello World"), 0);
    t1.join();
}

TEST_F(QueueTest, TestUniquePointer)
{
    // Create a queue with capacity 5 to hold unique pointers
    int kCapacity = 5;
    Queue<std::unique_ptr<int>> q(kCapacity);
    // Start a thread to push a unique pointer
    std::thread t1([&q]() {
        auto a = std::make_unique<int>(3);
        ASSERT_TRUE(q.Put(std::move(a)));
        EXPECT_TRUE(a == nullptr);
    });
    std::unique_ptr<int> b;
    ASSERT_TRUE(q.Take(&b));
    EXPECT_EQ(*b, 3);
    t1.join();
}

TEST_F(QueueTest, TestEmplace)
{
    // Create a queue with capacity 5 to hold Status object
    int kCapacity = 5;
    Queue<Status> q(kCapacity);
    std::thread t1([&q]() {
        // Invoke the constructor of Status directly.
        ASSERT_TRUE(q.EmplaceBack(StatusCode::K_OUT_OF_MEMORY, "Hello, run out of memory"));
    });
    Status v;
    ASSERT_TRUE(q.Take(&v));
    EXPECT_EQ(v.GetCode(), StatusCode::K_OUT_OF_MEMORY);
    std::cout << v.GetMsg() << std::endl;
    t1.join();
}

TEST_F(QueueTest, TestConcurrency2)
{
    // Create a queue with capacity 5 to hold integer
    int kCapacity = 5;
    Queue<int64_t> q(kCapacity);
    int num_insert = 1009 * kCapacity;
    // We will do a mix and match of all the combinations of pop and push.
    std::thread t1([num_insert, &q]() {
        RandomData rd;
        for (auto i = 0; i < num_insert; ++i) {
            Status rc;
            auto op = rd.GetRandomUint64(1, 3);
            switch (op) {
                case 1:
                    do {
                        rc = q.Add(i);
                    } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                    break;
                case 2:
                    do {
                        rc = q.Offer(i, 1000);
                    } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                    break;
                default:
                    rc = q.Put(i);
                    break;
            }
            ASSERT_TRUE(rc.IsOk());
        }
    });
    // Main thread
    RandomData rd;
    for (auto i = 0; i < num_insert; ++i) {
        Status rc;
        int64_t v;
        auto op = rd.GetRandomUint64(1, 3);
        switch (op) {
            case 1:
                do {
                    rc = q.Remove(&v);
                } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                break;
            case 2:
                do {
                    rc = q.Poll(&v, 1000);
                } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                break;
            default:
                rc = q.Take(&v);
                break;
        }
        ASSERT_TRUE(rc.IsOk());
    }
    t1.join();
}

TEST_F(QueueTest, TestConcurrency3)
{
    // Same test as TestConcurrency2 but we test rvalue
    int kCapacity = 5;
    Queue<std::unique_ptr<int64_t>> q(kCapacity);
    int num_insert = 1009 * kCapacity;
    // We will do a mix and match of all the combinations of pop and push.
    std::thread t1([num_insert, &q]() {
        RandomData rd;
        for (auto i = 0; i < num_insert; ++i) {
            Status rc;
            auto op = rd.GetRandomUint64(1, 3);
            switch (op) {
                case 1:
                    do {
                        rc = q.Add(std::make_unique<int64_t>(i));
                    } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                    break;
                case 2:
                    do {
                        rc = q.Offer(std::make_unique<int64_t>(i), 1000);
                    } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                    break;
                default:
                    rc = q.Put(std::make_unique<int64_t>(i));
                    break;
            }
            ASSERT_TRUE(rc.IsOk());
        }
    });
    // Main thread
    RandomData rd;
    for (auto i = 0; i < num_insert; ++i) {
        Status rc;
        std::unique_ptr<int64_t> v;
        auto op = rd.GetRandomUint64(1, 3);
        switch (op) {
            case 1:
                do {
                    rc = q.Remove(&v);
                } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                break;
            case 2:
                do {
                    rc = q.Poll(&v, 1000);
                } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                break;
            default:
                rc = q.Take(&v);
                break;
        }
        ASSERT_TRUE(rc.IsOk());
    }
    t1.join();
}

TEST_F(QueueTest, TestCirCularQueue)
{
    int capacity = 10;
    HeapCircularQ<int> circularQueue(capacity);
    circularQueue.Clear();
    ASSERT_FALSE(circularQueue.IsFull());

    for (int i = 1; i <= capacity; i++) {
        circularQueue.Push(i);
    }

    ASSERT_TRUE(circularQueue.IsFull());
    ASSERT_FALSE(circularQueue.Push(999));

    circularQueue.Print("QueueTest");
    ASSERT_EQ(circularQueue.Front(), circularQueue[0]);
    ASSERT_EQ(circularQueue.Front(), 1);
    ASSERT_TRUE(circularQueue.Pop(2));
    ASSERT_EQ(circularQueue.Back(), circularQueue[circularQueue.Length() - 1]);
    ASSERT_EQ(circularQueue.Back(), capacity);
    ASSERT_EQ(circularQueue.Front(), 3);

    circularQueue.Clear();
    ASSERT_EQ(circularQueue.Length(), size_t(0));
    ASSERT_TRUE(circularQueue.Push(321));
    ASSERT_EQ(circularQueue.Front(), 321);
    ASSERT_EQ(circularQueue.Length(), size_t(1));
}

template <typename F>
void ExecuteFuncWithOORException(F f, int lineNum)
{
    try {
        f();
        ASSERT_TRUE(false);
    } catch (std::out_of_range &ex) {
        LOG(ERROR) << ex.what() << "; lineNum: " << lineNum;
    }
}

TEST_F(QueueTest, TestCirCularQueueCornerCases)
{
    int capacity = 10;
    HeapCircularQ<int> circularQueue(capacity);
    circularQueue.Clear();
    ExecuteFuncWithOORException([&circularQueue]() { circularQueue.Front(); }, __LINE__);
    ExecuteFuncWithOORException([&circularQueue]() { circularQueue.Back(); }, __LINE__);

    const int ele = 1;
    const int numPopEles = 2;
    ASSERT_TRUE(circularQueue.Push(ele));
    ASSERT_FALSE(circularQueue.Pop(numPopEles));
    ASSERT_EQ(circularQueue[0], ele);

    ExecuteFuncWithOORException([&circularQueue]() { LOG(INFO) << circularQueue[1]; }, __LINE__);
}

class ShmQueueTest : public CommonTest {
public:
    ShmQueueTest() = default;

    void ResetShmUnit(ShmUnit &shmUnit)
    {
        shmUnit.pointer = nullptr;
        shmUnit.fd = -1;
        shmUnit.offset = 0;
        shmUnit.mmapSize = 0;
        shmUnit.size = 0;
    }

    void CreateShmPtr(ShmUnit &shmUnit, uint64_t maxSize, uint64_t needSize)
    {
        allocator_ = datasystem::memory::Allocator::Instance();
        DS_ASSERT_OK(allocator_->Init(maxSize));
        ASSERT_EQ(allocator_->GetMaxMemorySize(), size_t(maxSize));
        ASSERT_EQ(allocator_->GetMemoryUsage(), size_t(0));

        ResetShmUnit(shmUnit);
        DS_ASSERT_OK(allocator_->AllocateMemory("", needSize, false, shmUnit.pointer, shmUnit.fd, shmUnit.offset,
                                                shmUnit.mmapSize));
        ASSERT_EQ(allocator_->GetMemoryUsage(), needSize);
    }

    void FreeShm(ShmUnit &shmUnit)
    {
        if (allocator_) {
            DS_ASSERT_OK(allocator_->FreeMemory(shmUnit.pointer));
            ASSERT_EQ(allocator_->GetMemoryUsage(), size_t(0));
            allocator_->Shutdown();
        }
    }

    // Scenarios that simulate concurrent client operations
    void StartProducerThread(int dataNum, uint32_t capacity, uint32_t elementSize, std::shared_ptr<ShmUnit> shmUnit,
                             const std::vector<std::string> &uuidVec)
    {
        int result = lockIdTest_.fetch_add(1);
        std::shared_ptr<ShmCircularQueue> queue =
            std::make_shared<ShmCircularQueue>(capacity, elementSize, shmUnit, result);
        DS_ASSERT_OK(queue->Init());
        queue->UpdateQueueMeta();
        int time = 0;
        constexpr int timeoutSec = 60;
        const struct timespec timeoutStruct = { .tv_sec = static_cast<long int>(timeoutSec), .tv_nsec = 0 };
        for (int i = 0; i < dataNum; i++) {
            queue->WaitForQueueFull(timeoutStruct);
            int dataFlag = i % 2;
            std::string element = std::to_string(dataFlag) + uuidVec[i];
            uint32_t slotIndex;
            DS_ASSERT_OK(queue->SharedLock());
            queue->UpdateQueueMeta();
            if (!queue->GetSlotUntilSuccess(slotIndex)) {
                i--;
                time++;
                queue->SharedUnlock();
                continue;
            }
            uint8_t *shmPtr = nullptr;
            DS_ASSERT_OK(queue->PushBySlot(slotIndex, element.data(), elementSize, &shmPtr));
            queue->SharedUnlock();
            queue->NotifyNotEmpty();
        }
        LOG(INFO) << "Slot has been changed ! " << time;
    }

    // Scenarios that simulate concurrent worker operations
    void StartConsumerThread(int producerNum, int dataNum, uint32_t capacity, uint32_t elementSize,
                             std::shared_ptr<ShmUnit> shmUnit, const std::function<void(uint8_t *)> &funcFrontHandler)
    {
        std::shared_ptr<ShmCircularQueue> queue = std::make_shared<ShmCircularQueue>(capacity, elementSize, shmUnit);
        DS_ASSERT_OK(queue->Init());
        queue->UpdateQueueMeta();
        (void)queue->SetGetDataHandler(funcFrontHandler);
        constexpr int timeoutSec = 60;
        const struct timespec timeoutStruct = { .tv_sec = static_cast<long int>(timeoutSec), .tv_nsec = 0 };
        for (int i = 0; i < dataNum * producerNum;) {
            (void)queue->WaitForQueueEmpty(timeoutStruct);
            DS_ASSERT_OK(queue->WriteLock());
            queue->UpdateQueueMeta();
            // client -> notify
            // client getlock
            // worker -> wait lock
            // client release lock
            // worker get lock
            // worker release lock
            // worker wait
            // client continue send notify
            if (queue->Length() == 0) {
                i--;
                queue->WriteUnlock();
                continue;
            }
            int popSize = queue->GetAndPopAll();
            ASSERT_GT(popSize, 0);
            i += popSize;
            queue->WriteUnlock();
            queue->NotifyQueueNotFull();
        }
    }

private:
    datasystem::memory::Allocator *allocator_{ nullptr };
    std::atomic<int> lockIdTest_ = { 1 };
};

static void GenerateDataByUuid(std::vector<std::string> &vec, uint32_t vecSize = 100)
{
    for (uint32_t i = 0; i < vecSize; i++) {
        auto str = GetStringUuid();
        std::string realByte;
        DS_ASSERT_OK(StringUuidToBytes(str, realByte));
        vec.emplace_back(realByte);
    }
}

static void ResetShmPointer(void *pointer, uint64_t size)
{
    auto ret = memset_s(pointer, size, 0, size);
    if (ret != EOK) {
        LOG(ERROR) << FormatString("Memory set failed, the memset_s return: %d: ", ret);
    }
    ASSERT_EQ(ret, EOK);
}

static bool VectorSameCheck(std::vector<std::string> &v1, std::vector<std::string> &v2)
{
    if (v1.size() != v2.size()) {
        return false;
    }
    sort(v1.begin(), v1.end());
    sort(v2.begin(), v2.end());
    for (int i = 0; i < (int)v1.size(); i++) {
        if (v1[i] != v2[i]) {
            return false;
        }
    }
    return true;
}

TEST_F(ShmQueueTest, UpdateQueueMetaTest)
{
    uint32_t dataNum = 1;
    uint32_t elementSize = 16;
    uint64_t needSize = sizeof(uint32_t) * 4 + dataNum * elementSize;

    std::shared_ptr<ShmUnit> shmUnit = std::make_shared<ShmUnit>();
    CreateShmPtr(*shmUnit, 64 * 1024ul * 1024u, needSize);
    int capacity = 30;
    std::shared_ptr<ShmCircularQueue> queue = std::make_shared<ShmCircularQueue>(capacity, elementSize, shmUnit);

    DS_ASSERT_OK(queue->Init());
    auto funcSetMem = [](void *flag, void *data) {
        auto ret = memcpy_s(flag, sizeof(uint32_t), data, sizeof(uint32_t));
        if (ret != EOK) {
            LOG(ERROR) << FormatString("Memory copy failed, the memcpy_s return: %d: ", ret);
        }
        ASSERT_EQ(ret, EOK);
    };

    uint32_t data[] = { 1, 2, 3, 4 };  // flag - lockId - length - head
    uint32_t *flag = (uint32_t *)(shmUnit->pointer);
    funcSetMem(flag, data);
    funcSetMem(flag + 1, data + 1);
    funcSetMem(flag + 2, data + 2);

    queue->UpdateQueueMeta();
    ASSERT_EQ(queue->Length(), (uint32_t)data[2]);
}

TEST_F(ShmQueueTest, ShmCircularQFullTest)
{
    int dataNum = 50;
    std::vector<std::string> uuidVec;
    GenerateDataByUuid(uuidVec, dataNum);
    uint32_t flagSize = sizeof(uint32_t);
    uint32_t dataSize = uuidVec[0].length();
    uint32_t elementSize = flagSize + dataSize;
    uint64_t needSize = sizeof(uint32_t) * 4 + dataNum * elementSize;

    std::shared_ptr<ShmUnit> shmUnit = std::make_shared<ShmUnit>();
    CreateShmPtr(*shmUnit, 64 * 1024ul * 1024u, needSize);
    ResetShmPointer(shmUnit->pointer, needSize);

    int capacity = 30;
    std::shared_ptr<ShmCircularQueue> queue = std::make_shared<ShmCircularQueue>(capacity, elementSize, shmUnit);

    DS_ASSERT_OK(queue->Init());
    queue->UpdateQueueMeta();

    int index = 0;
    auto funcFrontHandler = [&index, &uuidVec, flagSize, dataSize](uint8_t *element) {
        char *futex = (char *)element;
        std::string flagsData(futex, flagSize);
        int dataFlag = index % 2;
        ASSERT_EQ(flagsData, "000" + std::to_string(dataFlag));
        std::string uuid(futex + flagSize, dataSize);
        ASSERT_EQ(uuid, uuidVec[index]);
    };
    (void)queue->SetGetDataHandler(funcFrontHandler);

    bool firstTime = true;
    while (index < dataNum) {
        for (int i = index; i < dataNum; i++) {
            int dataFlag = i % 2;
            std::ostringstream oss;
            oss << std::setw(flagSize) << std::setfill('0') << dataFlag;
            std::string str = oss.str();
            std::string element = str + uuidVec[i];
            if (i == capacity && firstTime) {
                firstTime = false;
                ASSERT_FALSE(queue->Push(element.data(), elementSize));
                break;
            } else {
                ASSERT_TRUE(queue->Push(element.data(), elementSize));
            }
        }
        while (queue->Length() > 0) {
            ASSERT_TRUE(queue->Front());
            ASSERT_TRUE(queue->Pop());
            index++;
        }
    }
    ASSERT_EQ(queue->Length(), (uint32_t)0);
    FreeShm(*shmUnit);
}

TEST_F(ShmQueueTest, ShmCircularPushPopTest)
{
    LOG(INFO) << "Test ShmCircularQueue push front and pop.";

    uint32_t dataNum = 500;
    std::vector<std::string> uuidVec;
    GenerateDataByUuid(uuidVec, dataNum);
    uint32_t flagSize = sizeof(uint32_t);
    uint32_t dataSize = uuidVec[0].length();
    uint32_t elementSize = flagSize + dataSize;
    uint64_t needSize = sizeof(uint32_t) * 4 + dataNum * elementSize;

    std::shared_ptr<ShmUnit> shmUnit = std::make_shared<ShmUnit>();
    CreateShmPtr(*shmUnit, 64 * 1024ul * 1024u, needSize);
    ResetShmPointer(shmUnit->pointer, needSize);

    int capacity = 30;
    std::shared_ptr<ShmCircularQueue> queue = std::make_shared<ShmCircularQueue>(capacity, elementSize, shmUnit);

    DS_ASSERT_OK(queue->Init());
    queue->UpdateQueueMeta();

    int index = 0;
    auto funcFrontHandler = [&index, &uuidVec, flagSize, dataSize](uint8_t *element) {
        char *futex = (char *)element;
        std::string flagsData(futex, flagSize);
        int dataFlag = index % 2;
        ASSERT_EQ(flagsData, "000" + std::to_string(dataFlag));
        char *data = futex + flagSize;
        std::string uuid(data, dataSize);
        ASSERT_EQ(uuid, uuidVec[index]);
    };
    (void)queue->SetGetDataHandler(funcFrontHandler);

    ASSERT_EQ(queue->Length(), (uint32_t)0);
    for (uint32_t dataIndex = 0; dataIndex < dataNum; dataIndex++) {
        int dataFlag = dataIndex % 2;
        std::ostringstream oss;
        oss << std::setw(flagSize) << std::setfill('0') << dataFlag;
        std::string str = oss.str();
        std::string element = str + uuidVec[dataIndex];
        ASSERT_TRUE(queue->Push(element.data(), elementSize));

        ASSERT_TRUE(queue->Front());
        ASSERT_TRUE(queue->Pop());
        index++;
    }
    ASSERT_EQ(queue->Length(), (uint32_t)0);
    FreeShm(*shmUnit);
}

TEST_F(ShmQueueTest, GetSlotForFullTest)
{
    int dataNum = 10000;
    std::vector<std::string> uuidVec;
    GenerateDataByUuid(uuidVec, dataNum);
    uint32_t flagSize = sizeof(uint32_t);
    uint32_t dataSize = uuidVec[0].length();
    uint32_t elementSize = flagSize + dataSize;
    uint64_t needSize = sizeof(uint32_t) * 4 + dataNum * elementSize;
    errno = 0;

    std::shared_ptr<ShmUnit> shmUnit = std::make_shared<ShmUnit>();
    CreateShmPtr(*shmUnit, 64 * 1024ul * 1024u, needSize);
    ResetShmPointer(shmUnit->pointer, needSize);

    int producerNum = 16;
    ThreadPool pool(producerNum + 1);
    std::vector<std::future<void>> pushFuts;
    std::future<void> popFuts;

    int capacity = 2048;
    for (int i = 0; i < producerNum; i++) {
        pushFuts.emplace_back(pool.Submit([this, dataNum, capacity, elementSize, shmUnit, &uuidVec] {
            StartProducerThread(dataNum, capacity, elementSize, shmUnit, uuidVec);
        }));
    }

    std::vector<std::string> resultList;
    resultList.reserve(dataNum);
    auto funcFrontHandler = [&resultList, flagSize, dataSize](uint8_t *element) {
        char *futex = (char *)element;
        std::string flagsData(futex, flagSize);
        char *data = futex + 1;
        std::string uuid(data, dataSize);
        resultList.emplace_back(uuid);
    };
    popFuts = pool.Submit([this, dataNum, capacity, elementSize, shmUnit, producerNum, &funcFrontHandler] {
        StartConsumerThread(producerNum, dataNum, capacity, elementSize, shmUnit, funcFrontHandler);
    });

    for (auto &future : pushFuts) {
        future.get();
    }
    popFuts.get();

    ASSERT_EQ((int)resultList.size(), dataNum * producerNum);
    auto tmpVec = uuidVec;
    for (int i = 1; i < producerNum; i++) {
        uuidVec.insert(uuidVec.end(), tmpVec.begin(), tmpVec.end());
    }
    LOG(INFO) << uuidVec.size();
    ASSERT_TRUE(VectorSameCheck(resultList, uuidVec));
    FreeShm(*shmUnit);
}

}  // namespace ut
}  // namespace datasystem
