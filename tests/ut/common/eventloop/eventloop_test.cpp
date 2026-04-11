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
#include <chrono>
#include <functional>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include "ut/common.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/util/thread_pool.h"

using namespace datasystem;
namespace datasystem {
namespace ut {

class EventLoopTest : public CommonTest {
public:
    void Init()
    {
        counter_ = 0;
        DS_ASSERT_OK(testLoop_.Init());
    }
    void ReadCallBack()
    {
        counter_ += 1;
    }
    void TimerFdSetTime(const uint64_t &delay)
    {
        struct itimerspec it;
        it.it_interval.tv_sec = 0;
        it.it_interval.tv_nsec = 0;
        it.it_value.tv_sec = delay / 1000;
        it.it_value.tv_nsec = (delay % 1000) * 1000 * 1000;
        if (timerfd_settime(timer_fd_, 0, &it, nullptr) == -1) {
            close(timer_fd_);
            timer_fd_ = -1;
            return;
        }
    }

    int counter_;
    int timer_fd_;
    EventLoop testLoop_;
};

TEST_F(EventLoopTest, AddFdEvent)
{
    Init();
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    DS_ASSERT_OK(testLoop_.AddFdEvent(timer_fd_, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr));
    TimerFdSetTime(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    TimerFdSetTime(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(2, counter_);
}

TEST_F(EventLoopTest, ModifyFdEvent)
{
    Init();
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    testLoop_.AddFdEvent(timer_fd_, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr);
    TimerFdSetTime(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    testLoop_.ModifyFdEvent(timer_fd_, EPOLLOUT);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(0, counter_);
}

TEST_F(EventLoopTest, DelFdEvent)
{
    Init();
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    testLoop_.AddFdEvent(timer_fd_, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr);
    TimerFdSetTime(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    testLoop_.DelFdEvent(timer_fd_);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(0, counter_);
}

TEST_F(EventLoopTest, ModifyNonExistentFd)
{
    Init();
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    testLoop_.AddFdEvent(timer_fd_, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr);
    EXPECT_NE(testLoop_.ModifyFdEvent(-100, EPOLLOUT), Status::OK());
}

TEST_F(EventLoopTest, ConcurrentTest)
{
    Init();
    ThreadPool threadPool(2);
    auto fut1 = threadPool.Submit([&]() {
        for (int i = 0; i < 100; i++) {
            testLoop_.AddFdEvent(i, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr);
        }
    });
    auto fut2 = threadPool.Submit([&]() {
        for (int i = 0; i < 100; i++) {
            testLoop_.DelFdEvent(i);
        }
    });
    fut1.get();
    fut2.get();
}
}  // namespace ut
}  // namespace datasystem