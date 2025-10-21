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
 * Description: Timer queue implementation.
 */
#include "datasystem/common/eventloop/timer_queue.h"

#include <iostream>

#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/strings_util.h"

using namespace std::chrono;
namespace datasystem {
static constexpr int DEBUG_LOG_LEVEL = 2;

TimerQueue *TimerQueue::GetInstance()
{
    static TimerQueue instance;
    return &instance;
}

TimerQueue::~TimerQueue()
{
    if (Size() > 0) {
        LOG(ERROR) << "When ~TimerQueue() the size of timer queue is : " << Size();
    }
    Finalize();
}

uint64_t TimerQueue::NextTick(const std::map<uint64_t, std::list<TimerImpl>> &timerPool)
{
    if (!timerPool.empty()) {
        return timerPool.begin()->first;
    }
    return 0;
}

void TimerQueue::ExecTimers(const std::list<TimerImpl> &timers)
{
    for (const auto &timer : timers) {
        DLOG(INFO) << FormatString("ExecTimers with id %llu, timestamp %llu", timer.GetId(), timer.GetTimestamp());
        timer.ExecTimeOutCallBack();
    }
}

uint64_t TimerQueue::CurrentTimeMs()
{
    uint64_t now = static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
    return now;
}

void TimerQueue::TimerFdSetTime(const uint64_t delay)
{
    struct itimerspec it;
    it.it_interval.tv_sec = 0;
    it.it_interval.tv_nsec = 0;
    it.it_value.tv_sec = delay / SECTOMILLI;
    it.it_value.tv_nsec = (delay % SECTOMILLI) * MILLITOMICR * MICRTONANO;
    if (timerfd_settime(runTimerFD_, 0, &it, nullptr) == -1) {
        RETRY_ON_EINTR(close(runTimerFD_));
        return;
    }
}

void TimerQueue::ScheduleTick(const std::map<uint64_t, std::list<TimerImpl>> &timerPool)
{
    uint64_t nextTick = NextTick(timerPool);
    uint64_t nowTime = CurrentTimeMs();
    uint64_t delay;
    if (nextTick <= nowTime) {
        delay = 1;
    } else {
        delay = nextTick - nowTime;
    }
    TimerFdSetTime(delay);
}

// Select timeout timers.
void TimerQueue::ScanTimerPool()
{
    std::list<TimerImpl> outTimer;
    {
        std::lock_guard<std::shared_timed_mutex> lock(timersLock_);
        uint64_t now = CurrentTimeMs();

        for (auto it = timerPool_->begin(); it != timerPool_->end(); it++) {
            if (it->first > now) {
                break;
            }
            outTimer.splice(outTimer.end(), (*timerPool_)[it->first]);
        }
        // Delete timed out timer.
        (void)timerPool_->erase(timerPool_->begin(), timerPool_->upper_bound(now));
        ScheduleTick(*timerPool_);
    }
    ExecTimers(outTimer);
    outTimer.clear();
}

bool TimerQueue::Initialize()
{
    bool expected = false;
    if (initInstanceFlag_.compare_exchange_strong(expected, true)) {
        {
            std::lock_guard<std::shared_timed_mutex> lock(timersLock_);
            timerPool_ = std::make_unique<TimerPoolType>();
            timerEvLoop_ = std::make_unique<EventLoop>();
            auto status = timerEvLoop_->Init();
            if (status.IsError()) {
                LOG(ERROR) << "timerEvLoop_ init fail:" << status.ToString();
                timerEvLoop_ = nullptr;
                return false;
            }
        }
        runTimerFD_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (runTimerFD_ < 0) {
            LOG(ERROR) << "runTimerFD_ create fail, ret: " << runTimerFD_;
            runTimerFD_ = -1;
            return false;
        }
        auto status =
            timerEvLoop_->AddFdEvent(runTimerFD_, EPOLLIN, std::bind(&TimerQueue::ScanTimerPool, this), nullptr);
        if (status.IsError()) {
            RETRY_ON_EINTR(close(runTimerFD_));
            return false;
        }
        asyncEraseAndRunTimer_ = std::make_unique<ThreadPool>(eraseThreadNum, 0, "TimerQueue");
        return true;
    }
    return true;
}

void TimerQueue::Finalize()
{
    {
        std::lock_guard<std::shared_timed_mutex> lock(timersLock_);
        if (runTimerFD_ >= 0) {
            RETRY_ON_EINTR(close(runTimerFD_));
        }
    }
}

Status TimerQueue::AddTimer(const uint64_t &durationMs, const std::function<void()> timeOutCallBack, TimerImpl &timer)
{
    CHECK_FAIL_RETURN_STATUS(timeOutCallBack != nullptr, StatusCode::K_INVALID, "The timeOutCallBack is nullptr.");
    static std::atomic<uint64_t> id(1);
    uint64_t timeWatch = CurrentTimeMs() + durationMs;
    timer = TimerImpl(id.fetch_add(1), timeWatch, timeOutCallBack);
    VLOG(DEBUG_LOG_LEVEL) << FormatString("AddTimer with delay %llu at expire time %llu, with id %llu", durationMs,
                                          timeWatch, timer.GetId());
    if (durationMs == 0) {
        VLOG(DEBUG_LOG_LEVEL) << "ExecTimers with id " << timer.GetId();
        timeOutCallBack();
        return Status::OK();
    }
    {
        std::lock_guard<std::shared_timed_mutex> lock(timersLock_);
        if (timerPool_->empty() || timeWatch < timerPool_->begin()->first) {
            (*timerPool_)[timeWatch].push_back(timer);
            ScheduleTick(*timerPool_);
        } else {
            (*timerPool_)[timeWatch].push_back(timer);
        }
    }
    return Status::OK();
}

bool TimerQueue::Cancel(const TimerQueue::TimerImpl &timer)
{
    VLOG(DEBUG_LOG_LEVEL) << "Cancel timer with id " << timer.GetId();
    bool canceled = false;
    {
        std::lock_guard<std::shared_timed_mutex> lock(timersLock_);
        uint64_t timestamp = timer.GetTimestamp();
        // Step 1: Check if the timer is already canceled,
        // but we take care that same timestamp there are possibly other timers.
        if (timerPool_->count(timestamp) > 0) {
            auto &lst = timerPool_->at(timestamp);
            // Step 2: We check whether the timer is exactly in the list.
            auto iter = std::find(lst.begin(), lst.end(), timer);
            if (iter != lst.end()) {
                canceled = true;
                lst.erase(iter);
            }
            // Step 3: After erase the list may be empty, we erase it from the data structure.
            if (lst.empty()) {
                (void)(timerPool_->erase(timestamp));
            }
        } else {
            const int logPerCount = VLOG_IS_ON(DEBUG_LOG_LEVEL) ? 1 : 1000;
            LOG_EVERY_N(INFO, logPerCount) << "Not found Cancel timer with id " << timer.GetId();
        }
    }
    return canceled;
}

bool TimerQueue::EraseAndExecTimer(const TimerImpl &timer)
{
    if (!Cancel(timer)) {
        return false;
    }
    LOG(INFO) << "EraseAndExecTimer with id " << timer.GetId();
    auto traceID = Trace::Instance().GetTraceID();
    asyncEraseAndRunTimer_->Execute([timer, traceID]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        timer.ExecTimeOutCallBack();
    });
    return true;
}
}  // namespace datasystem
