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

/**
 * MockThreadPool UTs cover pool sizing, shutdown, non-blocking submit,
 * async completion, caller buffer lifetime, and queue backpressure.
 */
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>
#include <vector>

#include "datasystem/common/urma_mock/post_send/mock_thread_pool.h"

using namespace datasystem::urma_mock;

namespace {

void ClearThreadPoolEnv()
{
    unsetenv("URMA_MOCK_THREAD_POOL_SIZE");
    unsetenv("URMA_MOCK_QUEUE_CAP");
    unsetenv("URMA_MOCK_LATENCY_US");
    unsetenv("URMA_MOCK_THREAD_POOL_SIZE");
    unsetenv("URMA_MOCK_QUEUE_CAP");
    unsetenv("URMA_MOCK_LATENCY_US");
}

}  // namespace

TEST(MockThreadPoolTest, ThreadPoolSize32Default)
{
    ClearThreadPoolEnv();
    MockThreadPool pool;
    EXPECT_EQ(pool.Size(), 32u);
}

TEST(MockThreadPoolTest, ThreadPoolSize64LegacyEnv)
{
    ClearThreadPoolEnv();
    setenv("URMA_MOCK_THREAD_POOL_SIZE", "64", 1);
    MockThreadPool pool;
    EXPECT_EQ(pool.Size(), 64u);
    ClearThreadPoolEnv();
}

TEST(MockThreadPoolTest, HardwareMockEnvOverridesLegacyEnv)
{
    ClearThreadPoolEnv();
    setenv("URMA_MOCK_THREAD_POOL_SIZE", "16", 1);
    setenv("URMA_MOCK_THREAD_POOL_SIZE", "8", 1);
    setenv("URMA_MOCK_QUEUE_CAP", "64", 1);
    setenv("URMA_MOCK_QUEUE_CAP", "256", 1);
    setenv("URMA_MOCK_LATENCY_US", "10", 1);
    setenv("URMA_MOCK_LATENCY_US", "20", 1);

    EXPECT_EQ(MockThreadPool::ResolveLatencyFromEnv(), 20u);
    MockThreadPool pool;
    EXPECT_EQ(pool.Size(), 8u);
    EXPECT_EQ(pool.QueueCap(), 256u);
    ClearThreadPoolEnv();
}

TEST(MockThreadPoolTest, ThreadPoolShutdownJoinsInflight)
{
    MockThreadPool pool(4);
    std::atomic<int> done{ 0 };
    constexpr int kTasks = 100;
    for (int i = 0; i < kTasks; ++i) {
        pool.Submit([&done]() {
            // Simulate small work.
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            done.fetch_add(1);
        });
    }
    pool.Shutdown();
    EXPECT_EQ(done.load(), kTasks);
}

TEST(MockThreadPoolTest, PostSendWrReturnsImmediately)
{
    // With latency=0 and an empty queue, Submit must return well under 100us.
    // This guards against accidentally re-introducing synchronous work in the
    // Submit path.
    ClearThreadPoolEnv();
    MockThreadPool pool(4);
    auto t0 = std::chrono::steady_clock::now();
    constexpr int kSubmits = 1000;
    for (int i = 0; i < kSubmits; ++i) {
        pool.Submit([]() { /* no-op */ });
    }
    auto t1 = std::chrono::steady_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    EXPECT_LT(us / kSubmits, 100) << "Submit avg " << us << "us / " << kSubmits << " submits";
    pool.Shutdown();
}

TEST(MockThreadPoolTest, PostSendWrCompletesAsync)
{
    // Stand-in for PostSendWr_CompletesAsync with latency injection.
    constexpr uint64_t kLatencyUs = 10;
    ClearThreadPoolEnv();
    setenv("URMA_MOCK_LATENCY_US", "10", 1);
    EXPECT_EQ(MockThreadPool::ResolveLatencyFromEnv(), kLatencyUs);

    MockThreadPool pool(4);
    std::atomic<int> done{ 0 };
    auto submitT = std::chrono::steady_clock::now();
    pool.Submit([&done, kLatencyUs]() {
        std::this_thread::sleep_for(std::chrono::microseconds(kLatencyUs));
        done.store(1);
    });
    // Submit must return immediately (before the worker has run).
    EXPECT_EQ(done.load(), 0);
    // Wait for completion with a generous budget.
    auto deadline = submitT + std::chrono::milliseconds(500);
    while (done.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    auto tDone = std::chrono::steady_clock::now();
    EXPECT_EQ(done.load(), 1);
    auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(tDone - submitT).count();
    // Worker should have completed within ~5-50us of submit + latency.
    EXPECT_GE(elapsedUs, static_cast<long long>(kLatencyUs));
    EXPECT_LT(elapsedUs, 50000);
    pool.Shutdown();
    ClearThreadPoolEnv();
}

TEST(MockThreadPoolTest, PostSendWrWrLifetimeIndependent)
{
    // Stand-in for PostSendWr_WrLifetimeIndependent: deep-copy task captures
    // the wr fields by value (here, ints) and runs after Submit returns.
    // The caller may free / mutate the source after Submit; the task must
    // still see the snapshotted values.
    //
    // Use the default pool size (32) so workers reliably drain below the
    // 128-task queue cap during the producer loop. Submit(timeoutMs=0) is
    // non-blocking by design as defined by MockThreadPool and a 2-worker
    // pool + 200 tasks is racy with respect to the queue cap.
    MockThreadPool pool(0);
    constexpr int kIters = 200;
    std::atomic<int> sum{ 0 };
    for (int i = 0; i < kIters; ++i) {
        int snap = i;  // deep copy into lambda
        pool.Submit([&sum, snap]() { sum.fetch_add(snap); });
    }
    pool.Shutdown();
    int expected = 0;
    for (int i = 0; i < kIters; ++i) {
        expected += i;
    }
    EXPECT_EQ(sum.load(), expected);
}

TEST(MockThreadPoolTest, PostSendWrBackpressureOnFullQueue)
{
    setenv("URMA_MOCK_QUEUE_CAP", "64", 1);
    MockThreadPool pool(1);
    std::atomic<int> done{ 0 };
    std::atomic<bool> workerSleeping{ false };
    // Block the only worker on a long sleep so the queue fills without drain.
    pool.Submit([&done, &workerSleeping]() {
        workerSleeping.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        done.fetch_add(1);
    });
    // Wait until the worker is sleeping. Without this sync point, a fast
    // producer can fill the queue before the worker has popped the first task,
    // making the full-queue assertion depend on scheduling.
    while (!workerSleeping.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    constexpr int kQueueCap = 64;
    for (int i = 0; i < kQueueCap; ++i) {
        EXPECT_TRUE(pool.Submit([&done]() { done.fetch_add(1); }));
    }
    EXPECT_FALSE(pool.Submit([&done]() { done.fetch_add(1); }));
    pool.Shutdown();
    EXPECT_EQ(done.load(), kQueueCap + 1);
    ClearThreadPoolEnv();
}

TEST(MockThreadPoolTest, SubmitWaitingForQueueSpaceFailsAfterShutdown)
{
    setenv("URMA_MOCK_QUEUE_CAP", "64", 1);
    MockThreadPool pool(1);
    std::atomic<bool> workerSleeping{ false };
    std::atomic<bool> submitReturned{ false };
    std::atomic<bool> submitAccepted{ true };
    std::atomic<int> done{ 0 };

    ASSERT_TRUE(pool.Submit([&workerSleeping]() {
        workerSleeping.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }));
    while (!workerSleeping.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    for (int i = 0; i < 64; ++i) {
        ASSERT_TRUE(pool.Submit([&done]() { done.fetch_add(1); }));
    }

    std::thread submitter([&]() {
        submitAccepted.store(pool.Submit([&done]() { done.fetch_add(1); }, 5000), std::memory_order_release);
        submitReturned.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(submitReturned.load(std::memory_order_acquire));

    pool.Shutdown();
    submitter.join();
    EXPECT_FALSE(submitAccepted.load(std::memory_order_acquire));
    EXPECT_EQ(done.load(), 64);
    ClearThreadPoolEnv();
}

TEST(MockThreadPoolTest, PostSendWrBackpressureWhenFull)
{
    // Submit more tasks than the bounded queue can accept at once. Accepted
    // tasks must still drain cleanly; rejected tasks are the backpressure
    // signal and must not be counted as completed work.
    MockThreadPool pool(4);
    constexpr int kTasks = 1000;
    std::atomic<int> done{ 0 };
    int accepted = 0;
    for (int i = 0; i < kTasks; ++i) {
        if (pool.Submit([&done]() { done.fetch_add(1); })) {
            ++accepted;
        }
    }
    pool.Shutdown();
    EXPECT_GT(accepted, 0);
    EXPECT_LE(accepted, kTasks);
    EXPECT_EQ(done.load(), accepted);
}
