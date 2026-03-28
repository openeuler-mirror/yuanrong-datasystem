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
 * Description: Thread pool.
 */
#include "datasystem/common/util/thread_pool.h"

#include <system_error>
#include <thread>
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/log/log.h"

namespace datasystem {

// If ThreadPool throw std::system_error when constructing, ThreadPool's destructor won't execute,
// thread will release without join and cause abort.
// Wrap ThreadWorkers and rewrite its destructor to keep all threads will join normally when ThreadPool construct fail.
ThreadWorkers::~ThreadWorkers()
{
    this->Join();
}

void ThreadWorkers::Join()
{
    for (auto &workerPair : *this) {
        auto thread = &workerPair.second;
        if (thread->joinable()) {
            thread->join();
        }
    }
}

void ThreadPool::DoThreadWork()
{
    while (true) {
        std::function<void()> task;
        {
            // 1st: Proceed Condition.
            std::unique_lock<std::mutex> lock(this->mtx_);
            // After threadIdleTimeoutMs_, if taskQ is still empty, try to destroy this thread.
            if (!this->proceedCV_.wait_for(lock, threadIdleTimeoutMs_,
                                           [this] { return this->shutDown_ || !this->taskQ_.empty(); })) {
                if (GetThreadsNum() > minThreadNum_) {
                    auto tid = std::this_thread::get_id();
                    DestroyUnuseWorker(tid);
                    return;
                }
                continue;
            }

            // ShutDown and Finished.
            if (this->shutDown_ && (droppable_ || this->taskQ_.empty())) {
                return;
            }

            // 2nd: Dequeue Task.
            task = std::move(this->taskQ_.front());
            this->taskQ_.pop();
        }
        {
            // 3rd: Execute Task.
            if (runningThreadsNum_ == 0) {
                taskLastFinishTime_ = GetSteadyClockTimeStampUs();
            }
            runningThreadsNum_++;
            task();
            taskLastFinishTime_ = GetSteadyClockTimeStampUs();
            runningThreadsNum_--;
        }
    }
}

void ThreadPool::AddThread()
{
    auto thread = Thread([this] { this->DoThreadWork(); });
    thread.set_name(name_);
    std::lock_guard<std::shared_timed_mutex> workerLock(workersMtx_);
    workers_[thread.get_id()] = std::move(thread);
}

void ThreadPool::DestroyUnuseWorker(std::thread::id tid)
{
    std::lock_guard<std::shared_timed_mutex> workerLock(workersMtx_);
    if (!shutDown_ && workers_.find(tid) != workers_.end()) {
        if (workers_[tid].joinable()) {
            workers_[tid].detach();
        }
        (void)workers_.erase(tid);
    }
}

bool ThreadPool::IsPoolFull()
{
    std::shared_lock<std::shared_timed_mutex> lock(workersMtx_);
    if (taskQ_.size() + runningThreadsNum_ >= maxThreadNum_) {
        return true;
    }
    return false;
}

void ThreadPool::TryToAddThreadIfNeeded()
{
    {
        std::shared_lock<std::shared_timed_mutex> lock(workersMtx_);
        auto threadNum = workers_.size();
        if (threadNum >= maxThreadNum_ || threadNum >= taskQ_.size() + runningThreadsNum_) {
            return;
        }
    }
    try {
        AddThread();
    } catch (std::system_error &sysErr) {
        // If no thread in thread pool, we should throw error because there are not remaining thread to run task.
        if (GetThreadsNum() == 0) {
            throw;
        } else {
            std::string errMsg = std::string(sysErr.what()) + ", cannot acquire resources";
            LOG(ERROR) << errMsg;
        }
    }
}

size_t ThreadPool::GetThreadsNum()
{
    std::shared_lock<std::shared_timed_mutex> workerLock(workersMtx_);
    return workers_.size();
}

size_t ThreadPool::GetRunningTasksNum()
{
    return runningThreadsNum_;
}

size_t ThreadPool::GetWaitingTasksNum()
{
    return taskQ_.size();
}

std::string ThreadPool::GetStatistics()
{
    auto totalNum = GetThreadsNum();
    auto idleNum = totalNum - GetRunningTasksNum();
    return FormatString("idle(%ld),total(%ld),wait(%ld)", idleNum, totalNum, GetWaitingTasksNum());
}

ThreadPool::ThreadPoolUsage ThreadPool::GetThreadPoolUsage()
{
    ThreadPoolUsage threadPoolUsage;
    threadPoolUsage.currentTotalNum = GetThreadsNum();
    threadPoolUsage.runningTasksNum = GetRunningTasksNum();
    threadPoolUsage.idleNum = threadPoolUsage.currentTotalNum - threadPoolUsage.runningTasksNum;
    threadPoolUsage.waitingTaskNum = GetWaitingTasksNum();
    threadPoolUsage.maxThreadNum = maxThreadNum_;
    threadPoolUsage.threadPoolUsage = threadPoolUsage.runningTasksNum / static_cast<float>(maxThreadNum_);
    threadPoolUsage.taskLastFinishTime = taskLastFinishTime_;
    return threadPoolUsage;
}

ThreadPool::ThreadPool(size_t minThreadNum, size_t maxThreadNum, std::string name, bool droppable,
                       int threadIdleTimeoutMs)
    : shutDown_(false),
      joined_(false),
      droppable_(droppable),
      minThreadNum_(minThreadNum),
      maxThreadNum_(maxThreadNum),
      name_(std::move(name)),
      threadIdleTimeoutMs_(threadIdleTimeoutMs),
      warnLevel_(WarnLevel::HIGH)
{
    if (maxThreadNum_ == 0) {
        if (minThreadNum_ == 0) {
            throw std::runtime_error("ThreadPool: minThreadNum == maxThreadNum == 0, won't create any thread.");
        }
        maxThreadNum_ = minThreadNum_;
    }
    if (minThreadNum_ > maxThreadNum_) {
        minThreadNum_ = maxThreadNum_;
        LOG(WARNING) << FormatString("ThreadPool: minThreadNum %zu > maxThreadNum, adjust minThreadNum to %zu",
                                     minThreadNum_, maxThreadNum_);
    }
    // create core workers when construct
    workers_.reserve(minThreadNum_);
    for (size_t i = 0; i < minThreadNum_; ++i) {
        AddThread();
    }
}

// The destructor joins all threads.
ThreadPool::~ThreadPool()
{
    bool isShutDown = false;
    bool isJoined = false;
    {
        std::unique_lock<std::mutex> lock(mtx_);
        isShutDown = shutDown_;
        isJoined = joined_;
    }
    if (!isShutDown) {
        ShutDown();
    }
    if (!isJoined) {
        Join();
    }
}

void ThreadPool::Join()
{
    workers_.Join();
    joined_ = true;
}

void ThreadPool::ShutDown()
{
    {
        std::unique_lock<std::mutex> lock(mtx_);
        shutDown_ = true;
    }
    // Here, either shutdown correctly checked or have already been blocking.
    // Thus, safe to unprotected by lock(mtx_).
    proceedCV_.notify_all();
}

void ThreadPool::WarnIfNeed()
{
    if (name_.empty() || warnLevel_ == WarnLevel::NO_WARN) {
        return;
    }
    const float oneHundredPercent = 100.0;
    const float sixtyPercentThreshold = 60.0;
    const float eightyPercentThreshold = 80.0;
    float ratio = static_cast<float>(GetRunningTasksNum()) / maxThreadNum_ * oneHundredPercent;
    std::string msg = FormatString("The thread of %s is: [%.3lf]", name_, ratio);
    // log uses static variables with line number to save the log write counter, so it is necessary to write repetitive
    // code to distinguish different warn level.
    if (warnLevel_ == WarnLevel::LOW) {
        const int oneHundredTime = 10;
        const int eightyPercentTime = 60;
        const int sixtyPercentTime = 100;
        if (ratio >= oneHundredPercent) {
            LOG_EVERY_T(WARNING, oneHundredTime) << msg << "%, thread full, still wait task: " << GetWaitingTasksNum();
        } else if (ratio >= eightyPercentThreshold) {
            LOG_EVERY_T(WARNING, eightyPercentTime)
                << msg << "%, reaches 80% threshold, still wait task: " << GetWaitingTasksNum();
        } else if (ratio >= sixtyPercentThreshold) {
            LOG_EVERY_T(WARNING, sixtyPercentTime)
                << msg << "%, reaches 60% threshold, still wait task: " << GetWaitingTasksNum();
        }
    } else {
        const int oneHundredFreq = 5;
        const int eightyPercentFreq = 50;
        const int sixtyPercentFreq = 100;
        if (ratio >= oneHundredPercent) {
            LOG_EVERY_N(WARNING, oneHundredFreq) << msg << "%, thread full, still wait task: " << GetWaitingTasksNum();
        } else if (ratio >= eightyPercentThreshold) {
            LOG_EVERY_N(WARNING, eightyPercentFreq)
                << msg << "%, reaches 80% threshold, still wait task: " << GetWaitingTasksNum();
        } else if (ratio >= sixtyPercentThreshold) {
            LOG_EVERY_N(WARNING, sixtyPercentFreq)
                << msg << "%, reaches 60% threshold, still wait task: " << GetWaitingTasksNum();
        }
    }
}
}  // namespace datasystem
