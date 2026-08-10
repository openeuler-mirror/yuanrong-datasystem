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
#include <condition_variable>
#include <functional>
#include <sys/socket.h>
#include <thread>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include "ut/common.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/util/thread_pool.h"

using namespace datasystem;
namespace datasystem {
namespace ut {

class TestableEventLoop : public EventLoop {
public:
    using EventLoop::HandleEvent;

    uint64_t GetToken(int fd) const
    {
        return eventMap_.at(fd)->token;
    }
};

class TestableSockEventLoop : public SockEventLoop {
public:
    using SockEventLoop::HandleEvent;

    Status InitWithoutThread()
    {
        efd_ = epoll_create1(0);
        if (efd_ == -1) {
            return Status(K_RUNTIME_ERROR, "epoll_create1 failed");
        }
        return Status::OK();
    }

    int GetEpollFd() const
    {
        return efd_;
    }
};

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
    TestableEventLoop testLoop_;
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

TEST_F(EventLoopTest, IgnoreStaleEventAfterConcurrentDelete)
{
    Init();
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    DS_ASSERT_OK(testLoop_.AddFdEvent(timer_fd_, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr));
    const uint64_t staleToken = testLoop_.GetToken(timer_fd_);
    DS_ASSERT_OK(testLoop_.DelFdEvent(timer_fd_));

    struct epoll_event staleEvent {};
    staleEvent.events = EPOLLIN;
    staleEvent.data.u64 = staleToken;
    testLoop_.HandleEvent(&staleEvent, 1);

    EXPECT_EQ(0, counter_);
}

TEST_F(EventLoopTest, RejectDuplicateRegistrationWithoutReplacingActiveToken)
{
    Init();
    timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    DS_ASSERT_OK(testLoop_.AddFdEvent(timer_fd_, EPOLLIN, std::bind(&EventLoopTest::ReadCallBack, this), nullptr));
    const uint64_t token = testLoop_.GetToken(timer_fd_);

    auto status = testLoop_.AddFdEvent(timer_fd_, EPOLLIN, nullptr, nullptr);

    EXPECT_EQ(status.GetCode(), StatusCode::K_DUPLICATED);
    EXPECT_EQ(testLoop_.GetToken(timer_fd_), token);
}

TEST_F(EventLoopTest, SockEventLoopIgnoresStaleDisconnectEventAfterDeleteAndFdReuse)
{
    TestableSockEventLoop sockLoop;
    DS_ASSERT_OK(sockLoop.InitWithoutThread());
    int disconnectedSockets[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, disconnectedSockets), 0);
    const int reusedFd = disconnectedSockets[0];
    int staleCallbackCount = 0;
    DS_ASSERT_OK(sockLoop.AddFdEvent(reusedFd, EPOLLIN | EPOLLHUP, [&]() { ++staleCallbackCount; }, nullptr));

    std::mutex eventLock;
    std::condition_variable eventCaptured;
    std::condition_variable handleEvent;
    bool isEventCaptured = false;
    bool canHandleEvent = false;
    int epollResult = 0;
    struct epoll_event event {};
    std::thread eventThread([&]() {
        epollResult = epoll_wait(sockLoop.GetEpollFd(), &event, 1, 1000);
        {
            std::lock_guard<std::mutex> lock(eventLock);
            isEventCaptured = true;
        }
        eventCaptured.notify_one();
        std::unique_lock<std::mutex> lock(eventLock);
        handleEvent.wait(lock, [&]() { return canHandleEvent; });
        if (epollResult == 1) {
            sockLoop.HandleEvent(&event, 1);
        }
    });

    EXPECT_EQ(close(disconnectedSockets[1]), 0);
    {
        std::unique_lock<std::mutex> lock(eventLock);
        eventCaptured.wait(lock, [&]() { return isEventCaptured; });
    }
    EXPECT_EQ(epollResult, 1);

    EXPECT_TRUE(sockLoop.DelFdEvent(reusedFd).IsOk());
    EXPECT_EQ(close(reusedFd), 0);
    int replacementSockets[2];
    EXPECT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, replacementSockets), 0);
    EXPECT_EQ(dup2(replacementSockets[0], reusedFd), reusedFd);
    if (replacementSockets[0] != reusedFd) {
        EXPECT_EQ(close(replacementSockets[0]), 0);
    }
    int replacementCallbackCount = 0;
    EXPECT_TRUE(sockLoop.AddFdEvent(reusedFd, EPOLLIN | EPOLLHUP,
                                    [&]() { ++replacementCallbackCount; }, nullptr)
                    .IsOk());

    {
        std::lock_guard<std::mutex> lock(eventLock);
        canHandleEvent = true;
    }
    handleEvent.notify_one();
    eventThread.join();

    EXPECT_EQ(staleCallbackCount, 0);
    EXPECT_EQ(replacementCallbackCount, 0);
    DS_ASSERT_OK(sockLoop.DelFdEvent(reusedFd));
    EXPECT_EQ(close(reusedFd), 0);
    EXPECT_EQ(close(replacementSockets[1]), 0);
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
