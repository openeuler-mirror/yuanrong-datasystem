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
    // Reset worker nice to the default value so thread-pool workers do not inherit elevated priority from
    // the thread that created the pool thread.
    if (!Thread::SetCurrentThreadNice(0)) {
        LOG(WARNING) << "Failed to set nice for thread pool [" << name_ << "], nice=" << 0 << ", errno=" << errno;
    }
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

            if (runningThreadsNum_ == 0) {
                taskLastFinishTime_ = GetSteadyClockTimeStampUs();
            }
            runningThreadsNum_++;
            UpdateMaxAtomic(maxRunningInPeriod_, runningThreadsNum_.load());

            // 2nd: Dequeue Task.
            task = std::move(this->taskQ_.front());
            this->taskQ_.pop();
        }
        {
            // 3rd: Execute Task.
            auto start = std::chrono::steady_clock::now();
            task();
            auto elapsed = std::chrono::steady_clock::now() - start;
            auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
            taskLastFinishTime_ = GetSteadyClockTimeStampUs();
            runningThreadsNum_--;
            tasksCompleted_.fetch_add(1, std::memory_order_relaxed);
            totalWorkTimeNs_.fetch_add(elapsedNs, std::memory_order_relaxed);
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
    threadPoolUsage.waitingTaskNum = GetWaitingTasksNum();
    threadPoolUsage.maxThreadNum = maxThreadNum_;
    threadPoolUsage.threadPoolUsage =
        maxThreadNum_ > 0 ? threadPoolUsage.runningTasksNum / static_cast<float>(maxThreadNum_) : 0.0f;
    threadPoolUsage.taskLastFinishTime = taskLastFinishTime_;
    return threadPoolUsage;
}

void ThreadPool::UpdateMaxAtomic(std::atomic<uint64_t> &counter, uint64_t value)
{
    uint64_t current = counter.load(std::memory_order_relaxed);
    while (value > current) {
        if (counter.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
            break;
        }
    }
}

ThreadPool::ThreadPoolUsage ThreadPool::GetAndResetIntervalStats()
{
    ThreadPoolUsage usage;
    usage.currentTotalNum = GetThreadsNum();
    usage.maxThreadNum = maxThreadNum_;
    usage.runningTasksNum = GetRunningTasksNum();
    usage.waitingTaskNum = GetWaitingTasksNum();
    usage.threadPoolUsage = maxThreadNum_ > 0 ? usage.runningTasksNum / static_cast<float>(maxThreadNum_) : 0.0f;
    // Seed next period with current running/waiting to handle cross-interval tasks.
    auto running = static_cast<uint64_t>(usage.runningTasksNum);
    auto waiting = static_cast<uint64_t>(usage.waitingTaskNum);
    usage.maxRunningInPeriod = std::max(maxRunningInPeriod_.exchange(running, std::memory_order_relaxed), running);
    usage.tasksCompletedDelta = tasksCompleted_.exchange(0, std::memory_order_relaxed);
    usage.maxWaitingInPeriod = std::max(maxWaitingInPeriod_.exchange(waiting, std::memory_order_relaxed), waiting);
    usage.totalWorkTimeNs = totalWorkTimeNs_.exchange(0, std::memory_order_relaxed);
    usage.taskLastFinishTime = taskLastFinishTime_;
    return usage;
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
    if (GetWaitingTasksNum() < 1) {
        return;
    }
    const float oneHundredPercent = 100.0;
    const float sixtyPercentThreshold = 60.0;
    const float eightyPercentThreshold = 80.0;
    float ratio = static_cast<float>(GetRunningTasksNum()) / maxThreadNum_ * oneHundredPercent;
    std::string msg =
        FormatString("Thread pool [%s] usage: %.1lf%%, waiting tasks: %d", name_, ratio, GetWaitingTasksNum());
    // log uses static variables with line number to save the log write counter, so it is necessary to write repetitive
    // code to distinguish different warn level.
    if (warnLevel_ == WarnLevel::LOW) {
        const int oneHundredTime = 10;
        const int eightyPercentTime = 60;
        const int sixtyPercentTime = 100;
        if (ratio >= oneHundredPercent) {
            LOG_EVERY_T(WARNING, oneHundredTime) << msg << ", thread full";
        } else if (ratio >= eightyPercentThreshold) {
            LOG_EVERY_T(WARNING, eightyPercentTime) << msg << ", exceeds 80%";
        } else if (ratio >= sixtyPercentThreshold) {
            LOG_EVERY_T(WARNING, sixtyPercentTime) << msg << ", exceeds 60%";
        }
    } else {
        const int oneHundredFreq = 50;
        const int eightyPercentFreq = 500;
        const int sixtyPercentFreq = 1000;
        if (ratio >= oneHundredPercent) {
            LOG_FIRST_AND_EVERY_N(WARNING, oneHundredFreq) << msg << ", thread full";
        } else if (ratio >= eightyPercentThreshold) {
            LOG_FIRST_AND_EVERY_N(WARNING, eightyPercentFreq) << msg << ", exceeds 80%";
        } else if (ratio >= sixtyPercentThreshold) {
            LOG_FIRST_AND_EVERY_N(WARNING, sixtyPercentFreq) << msg << ", exceeds 60%";
        }
    }
}
}  // namespace datasystem
