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
 * Description: Test of priority queue
 */
#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include "datasystem/common/util/queue/priority_queue.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/wait_post.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
class PriorityQueueTest : public CommonTest {};

TEST_F(PriorityQueueTest, SingleThreadPushAndPop)
{
    size_t capacity = 6;
    PriorityQueue<std::string> pq(capacity);
    std::string str1 = "Hello world1!";
    std::string str2 = "Hello world2!";
    std::string str3 = "Hello world3!";
    std::string str4 = "Hello world4!";
    std::string str5 = "Hello world5!";
    std::string str6 = "Hello world6!";
    DS_ASSERT_OK(pq.Put(str1));
    DS_ASSERT_OK(pq.Put(std::move(str2)));
    DS_ASSERT_OK(pq.Add(str3));
    DS_ASSERT_OK(pq.Add(std::move(str4)));
    DS_ASSERT_OK(pq.Offer(str5, 10));
    DS_ASSERT_OK(pq.Offer(std::move(str6), 10));

    // full
    ASSERT_TRUE(pq.Full());
    std::string str7 = "Hello world7!";
    ASSERT_EQ(pq.Add(str7).GetCode(), StatusCode::K_TRY_AGAIN);
    ASSERT_EQ(pq.Add(std::move(str7)).GetCode(), StatusCode::K_TRY_AGAIN);
    ASSERT_EQ(pq.Offer(str7, 10).GetCode(), StatusCode::K_TRY_AGAIN);
    ASSERT_EQ(pq.Offer(std::move(str7), 10).GetCode(), StatusCode::K_TRY_AGAIN);

    std::string buffer;
    DS_ASSERT_OK(pq.Take(&buffer));
    ASSERT_EQ(buffer, "Hello world6!");
    DS_ASSERT_OK(pq.Take(&buffer));
    ASSERT_EQ(buffer, "Hello world5!");
    DS_ASSERT_OK(pq.Remove(&buffer));
    ASSERT_EQ(buffer, "Hello world4!");
    DS_ASSERT_OK(pq.Remove(&buffer));
    ASSERT_EQ(buffer, "Hello world3!");
    DS_ASSERT_OK(pq.Poll(&buffer, 10));
    ASSERT_EQ(buffer, "Hello world2!");
    DS_ASSERT_OK(pq.Poll(&buffer, 10));
    ASSERT_EQ(buffer, "Hello world1!");

    // empty
    ASSERT_TRUE(pq.Empty());
    ASSERT_EQ(pq.Remove(&buffer).GetCode(), StatusCode::K_TRY_AGAIN);
    ASSERT_EQ(pq.Poll(&buffer, 10).GetCode(), StatusCode::K_TRY_AGAIN);
}

TEST_F(PriorityQueueTest, MultiThreadPushAndPop1)
{
    size_t capacity = 8;
    PriorityQueue<std::string> pq(capacity);
    std::vector<std::thread> writerThreads;
    std::vector<std::string> strs = { "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8" };
    for (int i = 0; i < 2; ++i) {
        writerThreads.push_back(std::thread([&pq, &strs, i]() {
            for (int j = 0; j < 4; ++j) {
                DS_ASSERT_OK(pq.Put(strs[j + 4 * i]));
            }
        }));
    }

    // full
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_TRUE(pq.Full());
    for (int i = 0; i < 2; ++i) {
        writerThreads.push_back(std::thread([&pq]() { DS_ASSERT_OK(pq.Put("test9")); }));
    }

    // remove strings
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::string buffer;
    DS_ASSERT_OK(pq.Poll(&buffer, 10));
    ASSERT_EQ(buffer, "test8");
    // let one of the blocked thread put its string
    std::this_thread::sleep_for(std::chrono::seconds(1));
    DS_ASSERT_OK(pq.Poll(&buffer, 10));
    // "test9" has the highest priority
    ASSERT_EQ(buffer, "test9");
    std::this_thread::sleep_for(std::chrono::seconds(1));
    DS_ASSERT_OK(pq.Poll(&buffer, 10));
    ASSERT_EQ(buffer, "test9");
    // empty the pq
    for (int i = 6; i >= 0; --i) {
        DS_ASSERT_OK(pq.Poll(&buffer, 10));
        ASSERT_EQ(buffer, strs[i]);
    }
    ASSERT_TRUE(pq.Empty());

    for (int i = 0; i < static_cast<int>(writerThreads.size()); ++i) {
        writerThreads[i].join();
    }
}

TEST_F(PriorityQueueTest, MultiThreadPushAndPop2)
{
    size_t capacity = 8;
    PriorityQueue<std::string> pq(capacity);
    std::vector<std::thread> readerThreads;
    for (int i = 0; i < 2; ++i) {
        readerThreads.push_back(std::thread([&pq]() {
            std::string buffer;
            // blocking call
            DS_ASSERT_OK(pq.Take(&buffer));
        }));
    }

    std::string str = "Hello world!";
    for (int i = 0; i < 2; ++i) {
        DS_ASSERT_OK(pq.Put(str));
    }

    for (int i = 0; i < static_cast<int>(readerThreads.size()); ++i) {
        readerThreads[i].join();
    }
    ASSERT_TRUE(pq.Empty());
}

TEST_F(PriorityQueueTest, MultiThreadPushAndPop3)
{
    size_t capacity = 100;
    struct Comp {
        bool operator()(const std::unique_ptr<int> &p1, const std::unique_ptr<int> &p2)
        {
            return *p1 / 30 < *p2 / 30;
        }
    };
    PriorityQueue<std::unique_ptr<int>, Comp> pq(capacity);
    int numReaders = 10, numWriters = 10;
    std::vector<std::thread> readerThreads, writerThreads;
    // 10 writers each writing 30 elements
    for (int i = 0; i < numWriters; ++i) {
        writerThreads.push_back(std::thread([&pq, i]() {
            Status rc;
            for (int j = 0; j < 30; ++j) {
                int op = j % 3;
                switch (op) {
                    case 0:
                        do {
                            rc = pq.Add(std::make_unique<int>(i * 30 + j));
                        } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                        break;
                    case 1:
                        do {
                            rc = pq.Offer(std::make_unique<int>(i * 30 + j), 100);
                        } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                        break;
                    default:
                        rc = pq.Put(std::make_unique<int>(i * 30 + j));
                        break;
                }
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }

    // 10 readers each reading 30 elements
    for (int i = 0; i < numReaders; ++i) {
        readerThreads.push_back(std::thread([&pq]() {
            Status rc;
            for (int j = 0; j < 30; ++j) {
                int op = j % 3;
                std::unique_ptr<int> buffer;
                switch (op) {
                    case 0:
                        do {
                            rc = pq.Remove(&buffer);
                        } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                        break;
                    case 1:
                        do {
                            rc = pq.Poll(&buffer, 100);
                        } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                        break;
                    default:
                        rc = pq.Take(&buffer);
                        break;
                }
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }

    for (int i = 0; i < static_cast<int>(writerThreads.size()); ++i) {
        writerThreads[i].join();
    }
    for (int i = 0; i < static_cast<int>(readerThreads.size()); ++i) {
        readerThreads[i].join();
    }
    ASSERT_TRUE(pq.Empty());
}

TEST_F(PriorityQueueTest, MultiThreadPushAndPop4)
{
    size_t capacity = 300;
    struct Comp {
        bool operator()(const int &i1, const int &i2)
        {
            return i1 / 30 < i2 / 30;
        }
    };
    PriorityQueue<int, Comp> pq(capacity);
    int numWriters = 10;
    std::vector<std::thread> writerThreads;
    // 10 writers each writing 30 elements
    for (int i = 0; i < numWriters; ++i) {
        writerThreads.push_back(std::thread([&pq, i]() {
            Status rc;
            for (int j = 0; j < 30; ++j) {
                int op = j % 3;
                int k = i * 30 + j;
                switch (op) {
                    case 0:
                        do {
                            rc = pq.Add(k);
                        } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                        break;
                    case 1:
                        do {
                            rc = pq.Offer(k, 100);
                        } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                        break;
                    default:
                        rc = pq.Put(k);
                        break;
                }
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }

    for (int i = 0; i < static_cast<int>(writerThreads.size()); ++i) {
        writerThreads[i].join();
    }

    ASSERT_TRUE(pq.Full());

    // full
    // test priority
    int k = -1;
    for (int i = 9; i >= 0; --i) {
        for (int j = 0; j < 30; ++j) {
            DS_ASSERT_OK(pq.Poll(&k, 1000));
            ASSERT_TRUE(k >= i * 30);
            ASSERT_TRUE(k < (i + 1) * 30);
        }
    }
}

TEST_F(PriorityQueueTest, MultiThreadPushAndPop5)
{
    int capacity = 100;
    struct Comp {
        bool operator()(const std::shared_ptr<int> &p1, const std::shared_ptr<int> &p2)
        {
            return *p1 % 2 > *p2 % 2;
        }
    };
    PriorityQueue<std::shared_ptr<int>, Comp> pq(capacity);
    // writer
    auto writer = std::thread([&pq, capacity]() {
        for (int i = 0; i < capacity; ++i) {
            if (i % 2 == 0) {
                DS_ASSERT_OK(pq.Offer(std::make_shared<int>(i), 100));
            } else {
                auto sp = std::make_shared<int>(i);
                DS_ASSERT_OK(pq.Add(sp));
            }
        }
    });
    writer.join();
    ASSERT_TRUE(pq.Full());

    // two readers
    std::vector<std::thread> readers;
    std::mutex mux;
    Queue<std::shared_ptr<int>> queue(2);
    for (int j = 0; j < 2; ++j) {
        readers.push_back(std::thread([&pq, &queue, &mux, capacity]() {
            std::shared_ptr<int> buffer;
            for (int i = 0; i < capacity / 2; ++i) {  // atomic block
                std::unique_lock<std::mutex> lock(mux);
                DS_ASSERT_OK(pq.Poll(&buffer, 100));
                Status rc;
                do {
                    rc = queue.Offer(std::move(buffer), 100);
                } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }

    // check the order of output elements
    std::shared_ptr<int> buffer;
    Status rc;
    for (int j = 0; j < 2; ++j) {
        for (int i = j; i < capacity; i += 2) {
            do {
                rc = queue.Remove(&buffer);
            } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
            ASSERT_TRUE(rc.IsOk());
            ASSERT_EQ(*buffer, i);
        }
    }

    readers[1].join();
    readers[0].join();

    ASSERT_TRUE(pq.Empty());
    ASSERT_TRUE(queue.Empty());
}

TEST_F(PriorityQueueTest, TestNumerousThreads)
{
    int capacity = 10;
    struct Comp {
        bool operator()(const std::unique_ptr<int> &p1, const std::unique_ptr<int> &p2)
        {
            return *p1 / 100 < *p2 / 100;
        }
    };
    PriorityQueue<std::unique_ptr<int>, Comp> pq(capacity);
    int numWriters = 100, numReaders = 100;
    std::vector<std::thread> writers;
    uint16_t done = 0;
    std::atomic<uint16_t> toDo(10000);
    std::mutex mux1, mux2;
    // push to pq
    for (int i = 0; i < numWriters; ++i) {
        writers.push_back(std::thread([&pq, &mux1, &toDo, i]() {
            Status rc;
            for (int j = 0; j < 100; ++j) {
                do {
                    std::unique_lock<std::mutex> lock(mux1);
                    rc = pq.Offer(std::make_unique<int>(j + i * 100), 100);
                    if (rc.IsOk()) {
                        --toDo;
                    }
                } while (rc.GetCode() == StatusCode::K_TRY_AGAIN);
                ASSERT_TRUE(rc.IsOk());
            }
        }));
    }
    // pop from pq
    std::vector<std::thread> readers;
    for (int i = 0; i < numReaders; ++i) {
        readers.push_back(std::thread([&pq, &mux2, &toDo, &done] {
            std::unique_ptr<int> buffer;
            do {
                std::unique_lock<std::mutex> lock(mux2);
                Status rc = pq.Poll(&buffer, 100);
                if (rc.IsOk()) {
                    ++done;
                }
            } while (!pq.Empty() || toDo != 0);
        }));
    }

    for (int i = 0; i < numWriters; ++i) {
        writers[i].join();
    }
    for (int i = 0; i < numReaders; ++i) {
        readers[i].join();
    }
    ASSERT_EQ(done, 10000);
}

TEST_F(PriorityQueueTest, TestEmplaceBack)
{
    int capacity = 8;
    struct Comp {
        std::less<std::string> cmp;
        bool operator()(const Status &s1, const Status &s2)
        {
            return cmp(s1.GetMsg(), s2.GetMsg());
        }
    };
    PriorityQueue<Status, Comp> pq(capacity);
    for (int i = 0; i < capacity; ++i) {
        DS_ASSERT_OK(pq.EmplaceBack(StatusCode::K_TRY_AGAIN, "Test EmplaceBack" + std::to_string(i)));
    }
    ASSERT_TRUE(pq.Full());
    Status buffer;
    for (int i = capacity - 1; i >= 0; --i) {
        DS_ASSERT_OK(pq.Remove(&buffer));
        ASSERT_EQ(buffer.GetCode(), StatusCode::K_TRY_AGAIN);
        ASSERT_EQ(buffer.GetMsg(), "Test EmplaceBack" + std::to_string(i));
    }
    ASSERT_TRUE(pq.Empty());
}

TEST_F(PriorityQueueTest, TestStabilityGreaterCmp)
{
    size_t capacity = 12;
    struct Comp {
        bool operator()(const std::shared_ptr<int> &p1, const std::shared_ptr<int> &p2)
        {
            return *p1 / 100 > *p2 / 100;
        }
    };
    PriorityQueue<std::shared_ptr<int>, Comp> pq(capacity);
    for (int i = 0; i < 4; ++i) {
        DS_ASSERT_OK(pq.Offer(std::make_shared<int>(i), 100));
    }
    DS_ASSERT_OK(pq.Offer(std::make_shared<int>(100), 100));
    DS_ASSERT_OK(pq.Offer(std::make_shared<int>(110), 100));
    DS_ASSERT_OK(pq.Offer(std::make_shared<int>(120), 100));
    DS_ASSERT_OK(pq.Offer(std::make_shared<int>(130), 100));
    for (int i = 7; i >= 4; --i) {
        DS_ASSERT_OK(pq.Offer(std::make_shared<int>(i), 100));
    }
    ASSERT_TRUE(pq.Full());

    std::shared_ptr<int> buffer;
    for (int i = 0; i < 4; ++i) {
        DS_ASSERT_OK(pq.Remove(&buffer));
        ASSERT_EQ(*buffer, i);
    }
    for (int i = 7; i >= 4; --i) {
        DS_ASSERT_OK(pq.Remove(&buffer));
        ASSERT_EQ(*buffer, i);
    }
    DS_ASSERT_OK(pq.Remove(&buffer));
    ASSERT_EQ(*buffer, 100);
    DS_ASSERT_OK(pq.Remove(&buffer));
    ASSERT_EQ(*buffer, 110);
    DS_ASSERT_OK(pq.Remove(&buffer));
    ASSERT_EQ(*buffer, 120);
    DS_ASSERT_OK(pq.Remove(&buffer));
    ASSERT_EQ(*buffer, 130);
    ASSERT_TRUE(pq.Empty());
}

TEST_F(PriorityQueueTest, TestPeek)
{
    size_t capacity = 1;
    PriorityQueue<std::string> pq(capacity);
    std::string str = "Hello world!";
    DS_ASSERT_OK(pq.Put(str));

    // peek
    const std::string *ptr = nullptr;
    DS_ASSERT_OK(pq.Peek(ptr));
    ASSERT_EQ(*ptr, "Hello world!");

    std::string buffer;
    DS_ASSERT_OK(pq.Take(&buffer));
    ASSERT_EQ(buffer, "Hello world!");

    // empty
    ASSERT_TRUE(pq.Empty());
}
}  // namespace ut
}  // namespace datasystem
