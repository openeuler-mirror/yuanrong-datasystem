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
#ifndef DATASYSTEM_COMMON_UTIL_THREAD_POOL_H
#define DATASYSTEM_COMMON_UTIL_THREAD_POOL_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {

class ThreadWorkers : public std::unordered_map<std::thread::id, Thread> {
public:
    ~ThreadWorkers();

    void Join();
};

class ThreadPool {
public:
    enum class WarnLevel : int { HIGH, LOW, NO_WARN };

    ThreadPool() = delete;

    ThreadPool(const ThreadPool &) = delete;

    ThreadPool(ThreadPool &&) = delete;

    ThreadPool &operator=(ThreadPool &&) = delete;

    ThreadPool &operator=(const ThreadPool &) = delete;

    explicit ThreadPool(size_t minThreadNum, size_t maxThreadNum = 0, std::string name = "", bool droppable = false,
                        int threadIdleTimeoutMs = 60 * 1000);

    // Using a variable in the return type that has not been declared yet
    // (because the return type declaration goes before the parameters type declaration).
    // Add new work item to the pool.
    template <class F, class... Args>
    auto Submit(F &&f, Args &&...args) -> std::future<typename std::result_of<F(Args...)>::type>
    {
        WarnIfNeed();
        using RetType = typename std::result_of<F(Args...)>::type;

        // Wrapper over promise, or single-element-blocking-queue.
        auto task =
            std::make_shared<std::packaged_task<RetType()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<RetType> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(mtx_);
            if (shutDown_) {
                const std::string error = "Submit after Shutdown Error.";
                throw std::runtime_error(error.c_str());
            }
            // Future is set after during (*task)(), a synchronous way to notify others waiting for it.
            taskQ_.emplace([task]() { (*task)(); });
            TryToAddThreadIfNeeded();
        }
        // Here, impossible to be empty; so no dead wait occurs.
        // Thus, safe to unprotected by lock(mtx_).
        proceedCV_.notify_one();
        return res;
    }

    template <class F, class... Args>
    void Execute(F &&f, Args &&...args)
    {
        WarnIfNeed();
        using RetType = typename std::result_of<F(Args...)>::type;
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        static_assert(std::is_void<RetType>::value, "Return value type must be void!");

        std::unique_lock<std::mutex> lock(mtx_);
        if (shutDown_) {
            throw std::runtime_error("Submit after Shutdown Error.");
        }
        taskQ_.emplace(std::move(task));
        TryToAddThreadIfNeeded();
        proceedCV_.notify_one();
    }

    /**
     * @brief Get the number of threads.
     * @return The number of threads created by ThreadPool.
     */
    size_t GetThreadsNum();

    /**
     * @brief Get the number of running tasks.
     * @return The number of running tasks which is called by threads.
     */
    size_t GetRunningTasksNum();

    /**
     * @brief Get the number of waiting tasks.
     * @return The number of waiting tasks which is in task queue.
     */
    size_t GetWaitingTasksNum();

    /**
     * @brief Get statistics.
     */
    std::string GetStatistics();

    struct ThreadPoolUsage {
        size_t currentTotalNum = 0;
        size_t runningTasksNum = 0;
        size_t idleNum = 0;
        size_t waitingTaskNum = 0;
        float threadPoolUsage = 0;
        size_t maxThreadNum = 0;
        double taskLastFinishTime;

        std::string ToString()
        {
            // Usage: idleNum/currentTotalNum/maxThreadNum/waitingTaskNum/threadPoolUsage
            if (currentTotalNum == 0) {
                return "";
            }
            auto threadPoolUsage = runningTasksNum / static_cast<float>(maxThreadNum);
            return FormatString("%ld/%ld/%ld/%ld/%.3f", idleNum, currentTotalNum, maxThreadNum, waitingTaskNum,
                                threadPoolUsage);
        }
    };

    /**
     * @brief Get the resource usage information of the thread pool.
     * @note idleNum:Number of idle threads in the thread pool
     *       currentTotalNum:Number of created threads
     *       maxThreadNum:Maximum number of threads that can be created in the thread pool.
     *       waitingTaskNum Number of waiting tasks.
     * @return Usage: "idleNum/currentTotalNum/maxThreadNum/waitingTaskNum/threadPoolUsage".
     */
    ThreadPoolUsage GetThreadPoolUsage();

    /**
     * @brief Set warnLevel.
     * @param[in] warnLevel The new warnLevel.
     */
    void SetWarnLevel(WarnLevel warnLevel)
    {
        warnLevel_ = warnLevel;
    }

    ~ThreadPool();

protected:
    void ShutDown();

    void Join();

    void DoThreadWork();

    /**
     * @brief Try to add thread if needed, will ignore error if threads resource is not enough.
     */
    void TryToAddThreadIfNeeded();

    /**
     * @brief Join and erase unused thread in workers_
     * @param[in] tid The Thread id ready to destroy.
     */
    void DestroyUnuseWorker(std::thread::id tid);

    /**
     * @brief Add thread directly, may throw system error if threads resource is not enough.
     */
    void AddThread();

    /**
     * @brief Log warning if need.
     */
    void WarnIfNeed();

private:
    using Task = std::function<void()>;
    ThreadWorkers workers_;

    std::queue<Task> taskQ_;

    std::mutex mtx_;

    // The mutext protecting workers_ get size, erase, add concurrently
    std::shared_timed_mutex workersMtx_;
    std::condition_variable proceedCV_;

    bool shutDown_;
    bool joined_;
    bool droppable_;

    size_t minThreadNum_;
    size_t maxThreadNum_;

    std::string name_;

    // The num of threads which is running task.
    std::atomic<size_t> runningThreadsNum_{ 0 };

    // If a threads wait for threadIdleTimeoutMs_ and no task need to execute, try to destroy it.
    std::chrono::milliseconds threadIdleTimeoutMs_;

    WarnLevel warnLevel_;

    std::time_t taskLastFinishTime_{ 0 };
};

/**
 * @brief Check if a std::future has finished or not.
 * @param[in] f The std::future or std::shared_future variable.
 * @param[in] timeout The timeout seconds value.
 * @return True for finished, false for not finished.
 */
template <typename R>
inline bool IsThreadFinished(std::future<R> const &f, const int &timeout)
{
    return f.wait_for(std::chrono::seconds(timeout)) == std::future_status::ready;
}

/**
 * @brief Check if a std::future has finished or not.
 * @param[in] f The std::shared_future variable.
 * @param[in] timeout The timeout seconds value.
 * @return True for finished, false for not finished.
 */
template <typename R>
inline bool IsThreadFinished(std::shared_future<R> const &f, const int &timeout)
{
    return f.wait_for(std::chrono::seconds(timeout)) == std::future_status::ready;
}
}  // namespace datasystem

class OrderedThreadPool {
public:
    explicit OrderedThreadPool(size_t threadCount)
        : taskQueues_(threadCount), queueMutexes_(threadCount), conditionVars_(threadCount), threadCount_(threadCount)
    {
        for (size_t i = 0; i < threadCount_; ++i) {
            workers_.emplace_back([this, i] { Run(i); });
        }
    }

    void Run(size_t index)
    {
        while (true) {
            std::shared_ptr<Task> task;
            {
                std::unique_lock<std::mutex> lock(queueMutexes_[index]);
                conditionVars_[index].wait(lock, [this, index] { return stop_.load() || !taskQueues_[index].empty(); });

                if (stop_.load() && taskQueues_[index].empty()) {
                    return;
                }

                task = taskQueues_[index].front();
                taskQueues_[index].pop();
            }

            try {
                task->func();
                task->promise.set_value();
            } catch (...) {
                task->promise.set_exception(std::current_exception());
            }
        }
    }

    ~OrderedThreadPool()
    {
        stop_.store(true);
        for (auto &cv : conditionVars_) {
            cv.notify_all();
        }
        for (auto &worker : workers_) {
            worker.join();
        }
    }

    std::future<void> Submit(const std::string &key, std::function<void()> func)
    {
        size_t index = GetQueueIndex(key);
        auto task = std::make_shared<Task>(std::move(func), key);
        auto future = task->promise.get_future();

        {
            std::lock_guard<std::mutex> lock(queueMutexes_[index]);
            taskQueues_[index].push(task);
        }
        conditionVars_[index].notify_one();

        return future;
    }

    /**
     * @brief Check whether some async tasks in the list.
     * @return True if all of async list is empty.
     */
    bool AreAllQueuesEmpty()
    {
        for (size_t i = 0; i < threadCount_; ++i) {
            std::lock_guard<std::mutex> lock(queueMutexes_[i]);
            if (!taskQueues_[i].empty()) {
                return false;
            }
        }
        return true;
    }

private:
    struct Task {
        std::function<void()> func;
        std::promise<void> promise;
        std::string key;

        Task(std::function<void()> f, const std::string &k) : func(std::move(f)), key(k)
        {
        }
    };

    std::vector<std::queue<std::shared_ptr<Task>>> taskQueues_;
    std::vector<std::mutex> queueMutexes_;
    std::vector<std::condition_variable> conditionVars_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{ false };
    size_t threadCount_;

    /**
     * @brief Calculate a index of list according to key.
     * @param[in] key The Id of the object need to be calculated.
     * @return Index of list.
     */
    size_t GetQueueIndex(const std::string &key)
    {
        return std::hash<std::string>{}(key) % threadCount_;
    }
};
#endif  // DATASYSTEM_COMMON_UTIL_THREAD_POOL_H
