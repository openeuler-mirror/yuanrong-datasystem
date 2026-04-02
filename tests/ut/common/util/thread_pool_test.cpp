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
 * Description: Test thread pool basic function.
 */
#include "datasystem/common/util/thread_pool.h"
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <thread>

#include "ut/common.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace ut {
class ThreadPoolTest : public CommonTest {
public:
    static Status InitThreadPool(size_t numThreads)
    {
        std::unique_ptr<ThreadPool> threadPool;
        RETURN_IF_EXCEPTION_OCCURS(threadPool = std::make_unique<ThreadPool>(numThreads));
        return Status::OK();
    }
};

void CommonFunc(std::atomic<int> *cnt)
{
    cnt->fetch_add(1);
}

class TestClass {
public:
    void MemberFunc()
    {
        cnt_.fetch_add(1);
    }

    int GetNum()
    {
        return cnt_;
    }

private:
    std::atomic<int> cnt_{ 0 };
};

TEST_F(ThreadPoolTest, TestExecuteBasicFunction)
{
    LOG(INFO) << "Test thread pool Execute api basic function";
    ThreadPool threadPool(4);
    {
        // common function.
        size_t num = 8;
        std::atomic<int> cnt{ 0 };
        for (size_t i = 0; i < num; ++i) {
            threadPool.Execute(std::bind(&CommonFunc, &cnt));
        }
        sleep(1);
        ASSERT_EQ(cnt, static_cast<int>(num));
    }
    {
        // lambda expression test.
        size_t num = 8;
        std::atomic<int> cnt{ 0 };
        auto lambdaFunc = [&cnt]() { cnt.fetch_add(1); };
        for (size_t i = 0; i < num; ++i) {
            threadPool.Execute(lambdaFunc);
        }
        sleep(1);
        ASSERT_EQ(cnt, static_cast<int>(num));
    }
    {
        // class member function.
        TestClass t;
        size_t num = 8;
        for (size_t i = 0; i < num; ++i) {
            threadPool.Execute(std::bind(&TestClass::MemberFunc, &t));
        }
        sleep(1);
        ASSERT_EQ(t.GetNum(), static_cast<int>(num));
    }
}

// we expect to get std::bad_alloc, but it causes an ASAN error.
TEST_F(ThreadPoolTest, DISABLED_ExceptionHandling)
{
    size_t numThreads = 1e15;
    Status rc = InitThreadPool(numThreads);
    ASSERT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    LOG(INFO) << rc.ToString();
}

TEST_F(ThreadPoolTest, DynamicThreadsNum)
{
    size_t minThreadsNum = 5;
    size_t maxThreadsNum = 8;
    int idleTimeout = 2000;
    int funcSleep = 4;
    ThreadPool threadpool(minThreadsNum, maxThreadsNum, "", false, idleTimeout);
    EXPECT_EQ(threadpool.GetThreadsNum(), minThreadsNum);
    for (size_t i = 0; i < minThreadsNum; i++) {
        threadpool.Execute([&funcSleep] { std::this_thread::sleep_for(std::chrono::seconds(funcSleep)); });
    }
    // wait threads accept task
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // when task lessequal minThreadsNum, won't add thread.
    EXPECT_EQ(threadpool.GetThreadsNum(), minThreadsNum);
    EXPECT_EQ(threadpool.GetRunningTasksNum(), minThreadsNum);
    for (size_t i = 0; i < maxThreadsNum; i++) {
        threadpool.Execute([&funcSleep] { std::this_thread::sleep_for(std::chrono::seconds(funcSleep)); });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // when task number great than maxThreadsNum, total threads won't great than maxThreadsNum
    EXPECT_EQ(threadpool.GetThreadsNum(), maxThreadsNum);
    EXPECT_EQ(threadpool.GetRunningTasksNum(), maxThreadsNum);

    std::this_thread::sleep_for(std::chrono::seconds(funcSleep));
    // some tasks is finished, waiting task will run, workers may idle partly.
    EXPECT_LE(threadpool.GetThreadsNum(), maxThreadsNum);
    EXPECT_LE(threadpool.GetRunningTasksNum(), threadpool.GetThreadsNum());

    std::this_thread::sleep_for(std::chrono::seconds(funcSleep));
    std::this_thread::sleep_for(std::chrono::milliseconds(idleTimeout));
    // all task finished
    EXPECT_LE(threadpool.GetThreadsNum(), minThreadsNum);
    EXPECT_EQ(threadpool.GetRunningTasksNum(), static_cast<size_t>(0));
}

TEST_F(ThreadPoolTest, GetTaskLastFinishTime)
{
    size_t minThreadsNum = 0;
    size_t maxThreadsNum = 3;
    int delayMs = 200;
    std::time_t currentTime = GetSteadyClockTimeStampUs();
    WaitPost wp;
    ThreadPool threadpool(minThreadsNum, maxThreadsNum, "", false);
    for (size_t i = 0; i < maxThreadsNum; i++) {
        threadpool.Execute([&wp] { wp.Wait(); });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));  // wait for task execute
    auto usage = threadpool.GetThreadPoolUsage();
    wp.Set();
    ASSERT_EQ(usage.threadPoolUsage, 1);
    ASSERT_TRUE(usage.taskLastFinishTime > currentTime) << usage.taskLastFinishTime << "," << currentTime;

    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));  // wait for last tasks finish
    usage = threadpool.GetThreadPoolUsage();
    ASSERT_EQ(usage.runningTasksNum, 0);
    currentTime = GetSteadyClockTimeStampUs();
    wp.Clear();
    for (size_t i = 0; i < maxThreadsNum; i++) {
        threadpool.Execute([&wp] { wp.Wait(); });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));  // wait for task execute
    usage = threadpool.GetThreadPoolUsage();
    wp.Set();
    ASSERT_EQ(usage.threadPoolUsage, 1);
    ASSERT_TRUE(usage.taskLastFinishTime > currentTime) << usage.taskLastFinishTime << "," << currentTime;
}

TEST_F(ThreadPoolTest, TestMaxThreads)
{
    const int spinLockSleep = 500;
    std::atomic<int> threadsIn = 0;
    int maxThreads = 4;
    bool workDone = false;

    ThreadPool threadPool(0, maxThreads, "", false);

    // deploy the max number of threads and givem them a task that sleeps.
    // Thus, these threads will all be in use.
    for (int i = 0; i < maxThreads; ++i) {
        threadPool.Execute([&threadsIn, &spinLockSleep] {
            LOG(INFO) << "User thread starting.";
            ++threadsIn;
            // Spin until parent wakes us up by setting to 0
            while (threadsIn != 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
            }
            LOG(INFO) << "User thread quitting now.";
        });

        // This sleep is just so that LOG(INFO) doesn't get jumbled due to concurrency
        std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
    }

    // Parent wait until all threads are in
    LOG(INFO) << "Parent waiting for child threads to get into the pool and use their thread";
    while (threadsIn != maxThreads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
    }

    ThreadPool::ThreadPoolUsage poolUsage = threadPool.GetThreadPoolUsage();
    LOG(INFO) << "Before extra thread added usage:  " << poolUsage.ToString();

    // Then, create one more thread. This will exceed the maximum. It is queued but it is
    // not executed yet until some other thread is freed
    threadPool.Execute([&threadsIn, &workDone] {
        LOG(INFO) << "Extra thread starting.";
        workDone = true;
        LOG(INFO) << "Extra thread quitting.";
    });

    poolUsage = threadPool.GetThreadPoolUsage();
    LOG(INFO) << "After extra thread added usage:  " << poolUsage.ToString();

    // unblock all the threads so they can quit. This frees up a thread that can be used
    // by the extra thread which then executes.
    threadsIn = 0;

    // After other threads quit, the new thread can get in and run the task. Wait for it.
    while (!workDone) {
        std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
    }

    LOG(INFO) << "Extra thread did work: " << workDone;
    ASSERT_EQ(workDone, true);
}

TEST_F(ThreadPoolTest, TestMaxThreadsNoWait)
{
    const int spinLockSleep = 500;
    std::atomic<int> threadsIn = 0;
    int maxThreads = 4;
    bool workDone = false;
    bool threadCreated = false;

    ThreadPool threadPool(0, maxThreads, "", false);

    // deploy the max number of threads and givem them a task that sleeps.
    // Thus, these threads will all be in use.
    for (int i = 0; i < maxThreads; ++i) {
        threadCreated =
            threadPool.ExecuteNoWait([&threadsIn, &spinLockSleep] {
                LOG(INFO) << "User thread starting.";
                ++threadsIn;
                // Spin until parent wakes us up by setting to 0
                while (threadsIn != 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
                }
                LOG(INFO) << "User thread quitting now.";
            });

        ASSERT_EQ(threadCreated, true);

        // This sleep is just so that LOG(INFO) doesn't get jumbled due to concurrency
        std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
    }

    // Parent wait until all threads are in
    LOG(INFO) << "Parent waiting for child threads to get into the pool and use their thread";
    while (threadsIn != maxThreads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
    }

    ThreadPool::ThreadPoolUsage poolUsage = threadPool.GetThreadPoolUsage();
    LOG(INFO) << "Before extra thread added usage:  " << poolUsage.ToString();

    // Then, create one more thread. This will exceed the maximum.
    // ExecuteNoWait will return false to indicate that it did not create the thread and
    // the task was not enqueued. Thus, workDone should remain false.
    threadCreated =
        threadPool.ExecuteNoWait([&threadsIn, &workDone] {
            LOG(INFO) << "Extra thread starting.";
            workDone = true;
            LOG(INFO) << "Extra thread quitting.";
        });

    poolUsage = threadPool.GetThreadPoolUsage();
    LOG(INFO) << "After extra thread added usage:  " << poolUsage.ToString();

    ASSERT_EQ(threadCreated, false);

    // unblock all the threads so they can quit. The extra thread/task should not have been enqueued at all.
    threadsIn = 0;

    LOG(INFO) << "Extra thread did work: " << workDone;
    ASSERT_EQ(workDone, false);
}

TEST_F(ThreadPoolTest, TestTaskDelay)
{
    std::atomic<int> threadsIn = 0;
    const int maxThreads = 128;
    const int taskCount = 10;
    const int taskDelayMs = 3000;

    ThreadPool threadPool(1, maxThreads);
    for (int i = 0; i < taskCount; ++i) {
        Timer timer;
        auto threadCreated = threadPool.ExecuteNoWait([&threadsIn, timer, taskDelayMs] {
            ++threadsIn;
            LOG(INFO) << "Thread delay " << timer.ElapsedMilliSecond() << "ms";
            std::this_thread::sleep_for(std::chrono::milliseconds(taskDelayMs));
        });
        ASSERT_TRUE(threadCreated);
    }

    const int waitMs = 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));

    ASSERT_EQ(threadsIn, taskCount);
}
}  // namespace ut
}  // namespace datasystem
