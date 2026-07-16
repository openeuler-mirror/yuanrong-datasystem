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

#include "datasystem/common/urma_mock/post_send/mock_thread_pool.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <string>
#include <system_error>
#include <thread>

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace urma_mock {

namespace {
constexpr size_t K_MIN_POOL_SIZE = 1;
constexpr size_t K_MAX_POOL_SIZE = 64;
constexpr size_t K_DEFAULT_SIZE = 32;
// bounded queue for mock async pool. Default 128 covers
// 4x the max thread count (64). Tunable via mock queue-cap envs.
constexpr size_t K_MIN_QUEUE_CAP = 64;
constexpr size_t K_MAX_QUEUE_CAP = 1024;
constexpr size_t K_DEFAULT_QUEUE_CAP = 128;

size_t ReadEnvSize(const std::string &name, size_t def)
{
    const char *raw = std::getenv(name.c_str());
    if (raw == nullptr) {
        return def;
    }
    std::string value(raw);
    size_t parsed = 0;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (ec != std::errc() || ptr != value.data() + value.size() || parsed == 0) {
        return def;
    }
    return parsed;
}

uint64_t ReadEnvU64(const std::string &name, uint64_t def)
{
    const char *raw = std::getenv(name.c_str());
    if (raw == nullptr) {
        return def;
    }
    std::string value(raw);
    uint64_t parsed = 0;
    auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (ec != std::errc() || ptr != value.data() + value.size()) {
        return def;
    }
    return parsed;
}
}  // namespace

size_t MockThreadPool::ResolveSizeFromEnv()
{
    size_t v = ReadEnvSize("URMA_MOCK_THREAD_POOL_SIZE", K_DEFAULT_SIZE);
    return std::clamp(v, K_MIN_POOL_SIZE, K_MAX_POOL_SIZE);
}

uint64_t MockThreadPool::ResolveLatencyFromEnv()
{
    return ReadEnvU64("URMA_MOCK_LATENCY_US", 0);
}

size_t MockThreadPool::ResolveQueueCapFromEnv()
{
    size_t v = ReadEnvSize("URMA_MOCK_QUEUE_CAP", K_DEFAULT_QUEUE_CAP);
    return std::clamp(v, K_MIN_QUEUE_CAP, K_MAX_QUEUE_CAP);
}

MockThreadPool::MockThreadPool(size_t size)
    : size_(std::clamp(size == 0 ? ResolveSizeFromEnv() : size, K_MIN_POOL_SIZE, K_MAX_POOL_SIZE)),
      queueCap_(ResolveQueueCapFromEnv())
{
    workers_.reserve(size_);
    for (size_t i = 0; i < size_; ++i) {
        workers_.emplace_back(&MockThreadPool::WorkerLoop, this);
    }
}

MockThreadPool::~MockThreadPool()
{
    Shutdown();
}

size_t MockThreadPool::Size() const
{
    return workers_.size();
}

bool MockThreadPool::Submit(Task task, int timeoutMs)
{
    std::unique_lock<std::mutex> lk(mu_);
    if (stopped_.load(std::memory_order_acquire)) {
        return false;
    }
    // Wait for queue space if a timeout was specified; otherwise fail fast.
    if (static_cast<size_t>(queue_.size()) >= queueCap_) {
        if (timeoutMs == 0) {
            return false;
        }
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        if (!cvSubmit_.wait_until(lk, deadline, [this] {
                return stopped_.load(std::memory_order_acquire) || static_cast<size_t>(queue_.size()) < queueCap_;
            })) {
            return false;  // timed out
        }
        if (stopped_.load(std::memory_order_acquire)) {
            return false;
        }
    }
    queue_.push(std::move(task));
    lk.unlock();
    cv_.notify_one();
    return true;
}

size_t MockThreadPool::QueueCap() const
{
    return queueCap_;
}

bool MockThreadPool::IsStopped() const
{
    return stopped_.load(std::memory_order_acquire);
}

void MockThreadPool::Drain()
{
    std::unique_lock<std::mutex> lk(mu_);
    cvIdle_.wait(lk, [this] { return queue_.empty() && activeTasks_ == 0; });
}

void MockThreadPool::Shutdown()
{
    bool shouldJoin = false;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!stopped_.load(std::memory_order_acquire)) {
            stopped_.store(true, std::memory_order_release);
            shouldJoin = true;
        }
    }
    if (!shouldJoin) {
        // Already shutting down; wait for the first caller to finish joining.
        while (!shutdownDone_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        return;
    }
    cv_.notify_all();
    cvSubmit_.notify_all();  // wake up any submitters waiting on cap
    for (auto &t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workers_.clear();
    shutdownDone_.store(true, std::memory_order_release);
}

void MockThreadPool::WorkerLoop()
{
    bool keepRunning = true;
    while (keepRunning) {
        Task task;
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [this] { return stopped_.load() || !queue_.empty(); });
            if (queue_.empty()) {
                if (stopped_.load()) {
                    keepRunning = false;
                }
                continue;
            }
            task = std::move(queue_.front());
            queue_.pop();
            ++activeTasks_;
        }
        // signal submitter that a slot has freed up.
        cvSubmit_.notify_one();
        try {
            task();
        } catch (const std::exception &e) {
            LOG(WARNING) << "[MockUrma] async task failed: " << e.what();
        } catch (...) {
            LOG(WARNING) << "[MockUrma] async task failed with unknown exception";
        }
        {
            std::lock_guard<std::mutex> lk(mu_);
            --activeTasks_;
            if (queue_.empty() && activeTasks_ == 0) {
                cvIdle_.notify_all();
            }
        }
    }
}

}  // namespace urma_mock
}  // namespace datasystem
