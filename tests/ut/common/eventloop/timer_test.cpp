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
 * Description: Timer
 */
#include <functional>
#include <iostream>
#include <chrono>
#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/random_data.h"
using namespace datasystem;
using namespace std::chrono;
namespace datasystem {
namespace ut {
uint64_t CurrentTime()
{
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

class TimerTest : public CommonTest {
public:
    void Init()
    {
        EXPECT_EQ(true, TimerQueue::GetInstance()->Initialize());
        testqueue_ = TimerQueue::GetInstance();
        changeFlag_ = false;
        duration_ = 0;
        counter_ = 0;
    }
    void ChangeFlag()
    {
        changeFlag_ = true;
    }
    void AddOneWithRandomSleep()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(random_.GetRandomUint64(1, 100)));
        counter_.fetch_add(1);
    }

    void AddOne()
    {
        counter_.fetch_add(1);
    }

    void SetDuration()
    {
        duration_ = CurrentTime();
    }
    std::atomic<uint64_t> duration_;
    bool changeFlag_;
    std::atomic<int> counter_;
    TimerQueue *testqueue_;
    RandomData random_;
};
TEST_F(TimerTest, DISABLED_AddNewTimer)
{
    Init();
    uint64_t start = CurrentTime();
    uint64_t interval = 200;
    TimerQueue::TimerImpl timer;
    DS_ASSERT_OK(testqueue_->AddTimer(interval, std::bind(&TimerTest::SetDuration, this), timer));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(true, (duration_ - start - interval) < 2);
}

TEST_F(TimerTest, MultiTimer)
{
    Init();
    int threadNum = 10;
    std::vector<std::thread> threads;
    for (int i = 0; i < threadNum; i++) {
        threads.emplace_back([this, i]() {
            std::this_thread::sleep_for(std::chrono::microseconds(random_.GetRandomUint64(1, 1000)));
            TimerQueue::TimerImpl timer;
            DS_ASSERT_OK(testqueue_->AddTimer(4000, std::bind(&TimerTest::AddOneWithRandomSleep, this), timer));
        });
    }
    for (int i = 0; i < threadNum; i++) {
        threads[i].join();
    }
    LOG(INFO) << "After add timer, current size is : " << testqueue_->Size();
    while (counter_.load() != threadNum) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        LOG(INFO) << "QUEUE SIZE IS " << testqueue_->Size();
    }
    ASSERT_EQ(testqueue_->Size(), size_t(0));
    ASSERT_EQ(counter_.load(), threadNum);
}

TEST_F(TimerTest, CancelTimer)
{
    Init();
    uint64_t interval = 20;
    TimerQueue::TimerImpl tmpTimer;
    DS_ASSERT_OK(testqueue_->AddTimer(interval, std::bind(&TimerTest::ChangeFlag, this), tmpTimer));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    EXPECT_EQ(testqueue_->Cancel(tmpTimer), true);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(false, changeFlag_);
}

TEST_F(TimerTest, EraseAndExecTimer)
{
    Init();
    uint64_t interval = 200;
    uint64_t start = CurrentTime();
    TimerQueue::TimerImpl tmpTimer;
    DS_ASSERT_OK(testqueue_->AddTimer(interval, std::bind(&TimerTest::SetDuration, this), tmpTimer));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(testqueue_->EraseAndExecTimer(tmpTimer));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_LT(duration_ - start, interval);
}

/**
 * @brief Timer callback for test.
 * @param[in/out] valRef Reference of value.
 * @param[in] i Value.
 */
void Dummy(int &valRef, uint64_t i)
{
    LOG(INFO) << FormatString("Exec Timer Callback of %zu, Dummy Code.", i);
    valRef = static_cast<int>(i) + 1;
}

/*
 * Construct a case where events are added to the same time.
 * Due to our millisecond-unit.
 */
TEST_F(TimerTest, ConcurrentEraseAndExecTimer)
{
    Init();
    uint64_t interval = 20;
    size_t numOfSuccessTimers = 3;
    ThreadPool pool(numOfSuccessTimers);
    int initVal = 0;
    std::atomic_int numOfEntries{ initVal };

    // Step 1: Construct three timers to add in the same time as canceled target.
    // We should guarantee success timers are executed normally if not canceled.
    std::vector<TimerQueue::TimerImpl> timerImpls(numOfSuccessTimers + 1);
    std::vector<int> integers(numOfSuccessTimers + 1, initVal);
    std::vector<std::future<Status>> succFuts;
    for (uint64_t i = 0; i < numOfSuccessTimers; i++) {
        succFuts.emplace_back(pool.Submit([interval, i, &timerImpls, &integers]() {
            return TimerQueue::GetInstance()->AddTimer(interval,
                                                       [i, &integers]() {
                                                           LOG(INFO) << FormatString("Exec Timer Callback of %zu", i);
                                                           Dummy(integers[i], i);
                                                       },
                                                       timerImpls[i]);
        }));
    }

    // Step 2: Add a timer which will be canceled.
    auto status = TimerQueue::GetInstance()->AddTimer(
        interval,
        [&integers, numOfSuccessTimers, &numOfEntries]() {
            LOG(INFO) << FormatString("Exec Timer Callback, which is to be canceled");
            Dummy(integers[numOfSuccessTimers], numOfSuccessTimers);
            numOfEntries++;
        },
        timerImpls.back());
    ASSERT_EQ(status, Status::OK());

    // Step 3: We concurrently cancel the timer.
    std::vector<std::future<bool>> futs;
    for (uint64_t i = 0; i < numOfSuccessTimers; i++) {
        futs.emplace_back(
            pool.Submit([&timerImpls]() { return TimerQueue::GetInstance()->EraseAndExecTimer(timerImpls.back()); }));
    }
    for (auto &statusFut : succFuts) {
        ASSERT_EQ(statusFut.get(), Status::OK());
    }
    for (auto &fut : futs) {
        fut.get();
    }

    // Step 4: We verify the number of re-execution.
    usleep(interval * 2'000);
    for (uint64_t i = 0; i < numOfSuccessTimers + 1; i++) {
        LOG(INFO) << "Check " << i << " th Result.";
        // Important: All executors are executed exactly once.
        ASSERT_EQ(integers[i], static_cast<int>(i + 1));
    }
    // Important: Erase and Callback only entry once.
    ASSERT_EQ(numOfEntries, 1);
}

TEST_F(TimerTest, MultipleTimer)
{
    Init();
    uint64_t interval = 20;
    TimerQueue::TimerImpl tmpTimer1;
    DS_ASSERT_OK(testqueue_->AddTimer(interval, std::bind(&TimerTest::AddOne, this), tmpTimer1));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    TimerQueue::TimerImpl tmpTimer2;
    DS_ASSERT_OK(testqueue_->AddTimer(interval, std::bind(&TimerTest::AddOne, this), tmpTimer2));
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(2, counter_);
}

TEST_F(TimerTest, Finalize)
{
    Init();
}
}  // namespace ut
}  // namespace datasystem