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
 * Description: Timer queue declaration.
 */
#ifndef DATASYSTEM_COMMON_EVENTLOOP_TIMER_QUEUE_H
#define DATASYSTEM_COMMON_EVENTLOOP_TIMER_QUEUE_H

#include <atomic>
#include <chrono>
#include <ctime>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <shared_mutex>

#include <sys/timerfd.h>
#include <unistd.h>

#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
constexpr uint64_t MICRTONANO = 1000;
constexpr uint64_t MILLITOMICR = 1000;
constexpr uint64_t SECTOMILLI = 1000;

class TimerQueue {
public:
    /**
     * @brief Get the Singleton TimerQueue instance.
     * @return TimerQueue instance.
     */
    static TimerQueue *GetInstance();
    ~TimerQueue();

    class TimerImpl {
    public:
        TimerImpl() : id_(0), timestamp_(0), timeOutCallBack_(nullptr){};

        TimerImpl(uint64_t timerId, const uint64_t &timeWatch, const std::function<void()> &handler)
            : id_(timerId), timestamp_(timeWatch), timeOutCallBack_(handler){};

        ~TimerImpl() = default;

        bool operator==(const TimerImpl &that) const
        {
            return id_ == that.id_;
        }

        void ExecTimeOutCallBack() const
        {
            if (timeOutCallBack_) {
                timeOutCallBack_();
            }
        }

        uint64_t GetTimestamp() const
        {
            return timestamp_;
        }

        uint64_t GetId() const
        {
            return id_;
        }

    private:
        uint64_t id_;
        uint64_t timestamp_;
        std::function<void()> timeOutCallBack_;
    };

    /**
     * @brief AddTimer add a timer.
     * @param[in] durationMs The interval from now.
     * @param[in] timeOutCallBack The hook function for timeout event.
     * @param[out] timer TimerImpl of the added timer.
     * @return Status of the call.
     */
    Status AddTimer(const uint64_t &durationMs, const std::function<void()> timeOutCallBack, TimerImpl &timer);

    /**
     * @brief Cancel a timer.
     * @param[in] timer The TimerImpl instance.
     * @return Status of the call.
     */
    bool Cancel(const TimerImpl &timer);

    /**
     * @brief Erase a timer and exec its callback function immediately.
     * @param[in] timer The TimerImpl instance.
     * @return Status of the call.
     */
    bool EraseAndExecTimer(const TimerImpl &timer);

    /**
     * @brief Get the size of the timer pool.
     * @return Return size of the timer pool.
     */
    size_t Size() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(timersLock_);
        return timerPool_->size();
    }

    /**
     * @brief Get current time
     * @return Current time
     */
    uint64_t CurrentTimeMs();

    /**
     * @brief Initialize init the timerqueue instance.
     */
    bool Initialize();

private:
    TimerQueue() = default;
    //  For make_unique to access private/protected constructor.
    friend std::unique_ptr<TimerQueue> std::make_unique<TimerQueue>();
    std::atomic<bool> initInstanceFlag_ = { false };

    /**
     * @brief Close the timerfd in timerqueue.
     */
    void Finalize();

    uint64_t NextTick(const std::map<uint64_t, std::list<TimerImpl>> &timerPool);

    void ExecTimers(const std::list<TimerImpl> &timers);

    void TimerFdSetTime(const uint64_t delay);

    void ScheduleTick(const std::map<uint64_t, std::list<TimerImpl>> &timerPool);

    void ScanTimerPool();

    using TimerPoolType = std::map<uint64_t, std::list<TimerImpl>>;
    mutable std::shared_timed_mutex timersLock_;  // We need to hold the lock to protect timerPool_
    std::unique_ptr<TimerPoolType> timerPool_{ nullptr };
    int runTimerFD_{ 0 };
    std::unique_ptr<EventLoop> timerEvLoop_{ nullptr };
    std::unique_ptr<ThreadPool> asyncEraseAndRunTimer_{ nullptr };
    const int eraseThreadNum{ 1 };
};
}  // namespace datasystem

#endif
