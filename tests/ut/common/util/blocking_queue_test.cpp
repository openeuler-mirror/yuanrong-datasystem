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
 * Description: blocing queue test.
 */
#include <vector>

#include "ut/common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/random_data.h"

namespace datasystem {
namespace ut {
class BlockingQueueTest : public CommonTest {};

TEST_F(BlockingQueueTest, TestPushPop)
{
    int sendNum = 10;
    std::vector<int> nums(sendNum);
    RandomData rand;
    int gtSum = 0;
    for (auto &num : nums) {
        num = rand.GetRandomUint64(10, 1000);
        gtSum += num;
    }

    ThreadPool pool(1);
    ThreadPool pool2(1);
    BlockingQueue<int> q;

    // Test Size() and Empty().
    ASSERT_EQ(q.Size(), size_t(0));
    ASSERT_EQ(q.Empty(), true);

    // Test synchronous Pop and Push.
    double waitTime = 0;
    auto sumFut = pool.Submit([sendNum, &q, &waitTime]() {
        double tmpTime = 0;
        int sum = 0;
        for (int i = 0; i < sendNum; i++) {
            int num = 0;
            q.Pop(num, tmpTime);
            sum += num;
            waitTime += tmpTime;
        }
        return sum;
    });
    auto fut = pool2.Submit([&nums, &q]() {
        for (auto num : nums) {
            q.Push(num);
        }
    });
    auto sum = sumFut.get();
    fut.get();
    LOG(INFO) << FormatString("Wait time: [%.6lf]s, Sum: [%d]", waitTime, sum);
    ASSERT_EQ(sum, gtSum);

    // Test Pop without statistics.
    fut = pool.Submit([&q]() {
        int num;
        q.Pop(num);
    });
    q.Push(1);
    fut.get();
}

TEST_F(BlockingQueueTest, TestAbortQueue)
{
    auto func = [] {
        std::atomic<bool> start = { false };
        BlockingQueue<int> queue;
        std::thread t([&queue, &start] {
            int v;
            start = true;
            (void)queue.Pop(v);
        });
        while (!start.load()) {
            std::this_thread::yield();
        }
        queue.Abort();
        t.join();
    };

    int testCount = 1000;
    for (int i = 0; i < testCount; i++) {
        func();
    }
}
}  // namespace ut
}  // namespace datasystem
