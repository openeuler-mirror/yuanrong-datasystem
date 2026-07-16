/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_POST_SEND_MOCK_THREAD_POOL_H
#define DATASYSTEM_COMMON_URMA_MOCK_POST_SEND_MOCK_THREAD_POOL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace datasystem {
namespace urma_mock {
/**
 * @brief Bounded worker pool used to simulate asynchronous mock operation completion.
 * The pool lets PostSendWr enqueue and return before a worker copies data and posts the completion record. Queue size,
 * worker count, and optional latency injection are controlled by URMA_MOCK_QUEUE_CAP, URMA_MOCK_THREAD_POOL_SIZE, and
 * URMA_MOCK_LATENCY_US.
 */
class MockThreadPool {
public:
    /**
     * @brief Task run by a mock worker thread.
     */
    using Task = std::function<void()>;

    /**
     * @brief Create a worker pool.
     * @param[in] size Worker count. Zero resolves the count from environment defaults.
     */
    explicit MockThreadPool(size_t size = 0);

    /**
     * @brief Stop workers and release queued work.
     */
    ~MockThreadPool();

    /**
     * @brief Thread pools own worker threads and cannot be copied.
     */
    MockThreadPool(const MockThreadPool &) = delete;
    /**
     * @brief Thread pools own worker threads and cannot be copy-assigned.
     */
    MockThreadPool &operator=(const MockThreadPool &) = delete;

    /**
     * @brief Submit a task to run on a worker thread.
     * @param[in] task Task to run.
     * @param[in] timeoutMs Maximum time to wait for a bounded queue slot. Zero means do not wait.
     * @return false if the pool is stopped or the bounded queue has no slot before timeout.
     */
    bool Submit(Task task, int timeoutMs = 0);

    /**
     * @brief Stop accepting new tasks and let queued work drain before workers exit.
     */
    void Shutdown();

    /**
     * @brief Wait for queued and running tasks while keeping the pool reusable.
     */
    void Drain();

    /**
     * @brief Get the worker count.
     * @return Number of worker threads owned by the pool.
     */
    size_t Size() const;

    /**
     * @brief Get configured queue capacity.
     * @return Maximum queued task count.
     */
    size_t QueueCap() const;

    /**
     * @brief Check whether the pool has stopped accepting work.
     * @return true after Shutdown starts.
     */
    bool IsStopped() const;

    /**
     * @brief Resolve per-task latency from mock environment variables.
     * @return Latency in microseconds.
     */
    static uint64_t ResolveLatencyFromEnv();

private:
    /**
     * @brief Resolve queue capacity from mock environment variables.
     * @return Queue capacity, or the default when the environment value is absent or invalid.
     */
    static size_t ResolveQueueCapFromEnv();

    /**
     * @brief Resolve worker count from mock environment variables.
     * @return Worker count, or the default when the environment value is absent or invalid.
     */
    static size_t ResolveSizeFromEnv();

    /**
     * @brief Worker thread main loop.
     */
    void WorkerLoop();

    std::mutex mu_;
    std::condition_variable cv_;        ///< Workers wait on this for tasks.
    std::condition_variable cvSubmit_;  ///< Submitters wait on this for queue space.
    std::condition_variable cvIdle_;
    std::queue<Task> queue_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stopped_{ false };
    std::atomic<bool> shutdownDone_{ false };
    size_t activeTasks_ = 0;
    size_t size_;
    size_t queueCap_;
};

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_POST_SEND_MOCK_THREAD_POOL_H
