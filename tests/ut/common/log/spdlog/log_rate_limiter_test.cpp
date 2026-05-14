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
 * Description: Test request-level log rate limiter.
 */

#include "datasystem/common/log/spdlog/log_rate_limiter.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace ut {
namespace {
void ClearRequestDecision()
{
    Trace::Instance().SetRequestSampleDecision(false, false);
}

int64_t Percentile99(std::vector<int64_t> values)
{
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    size_t index = values.size() * 99 / 100;
    if (index >= values.size()) {
        index = values.size() - 1;
    }
    return values[index];
}

std::vector<int64_t> MeasureDecisionBatches(int32_t rate, int batches, int batchSize)
{
    auto &limiter = LogRateLimiter::Instance();
    Trace::Instance().Invalidate();
    limiter.Reset();
    limiter.SetRate(rate);

    std::vector<int64_t> batchUs;
    batchUs.reserve(batches);
    uint64_t traceHash = 10000000;
    for (int batch = 0; batch < batches; ++batch) {
        auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < batchSize; ++i) {
            ClearRequestDecision();
            (void)limiter.ShouldLog(ds_spdlog::level::info, traceHash++);
        }
        auto finish = std::chrono::steady_clock::now();
        batchUs.emplace_back(std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count());
    }
    return batchUs;
}
}  // namespace

class LogRateLimiterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        Trace::Instance().Invalidate();
        LogRateLimiter::Instance().Reset();
    }

    void TearDown() override
    {
        Trace::Instance().Invalidate();
        LogRateLimiter::Instance().Reset();
    }
};

TEST_F(LogRateLimiterTest, NoLimitByDefault)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(0);

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, uint64_t(1000 + i)));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::warn, uint64_t(1000 + i)));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::err, uint64_t(1000 + i)));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::critical, uint64_t(1000 + i)));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info));
    }
}

TEST_F(LogRateLimiterTest, NonRequestLogsAreNeverSampled)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, uint64_t(0)));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::warn, uint64_t(0)));
    }
}

TEST_F(LogRateLimiterTest, RequestLogsAreSampledAcrossLevels)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, uint64_t(1001)));

    bool rejectedFound = false;
    uint64_t rejectedTrace = 0;
    for (uint64_t traceHash = 1002; traceHash < 1200; ++traceHash) {
        ClearRequestDecision();
        if (!limiter.ShouldLog(ds_spdlog::level::info, traceHash)) {
            rejectedFound = true;
            rejectedTrace = traceHash;
            break;
        }
    }
    ASSERT_TRUE(rejectedFound);

    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::info, rejectedTrace));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::warn, rejectedTrace));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::err, rejectedTrace));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::critical, rejectedTrace));
}

TEST_F(LogRateLimiterTest, AdmittedTracePrintsWholeChain)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    constexpr uint64_t traceHash = 2001;
    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::warn, traceHash));
    }
}

TEST_F(LogRateLimiterTest, PropagatedDecisionTakesPrecedence)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(0);

    Trace::Instance().SetRequestSampleDecision(true, false);
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::info, uint64_t(3001)));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::warn, uint64_t(3001)));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::err, uint64_t(3001)));

    Trace::Instance().SetRequestSampleDecision(true, true);
    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, uint64_t(3002)));
    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::warn, uint64_t(3002)));
}

TEST_F(LogRateLimiterTest, DynamicRateUpdate)
{
    auto &limiter = LogRateLimiter::Instance();

    limiter.SetRate(0);
    for (uint64_t traceHash = 4000; traceHash < 4100; ++traceHash) {
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));
    }

    limiter.SetRate(1);
    int passed = 0;
    for (uint64_t traceHash = 5000; traceHash < 5100; ++traceHash) {
        ClearRequestDecision();
        if (limiter.ShouldLog(ds_spdlog::level::info, traceHash)) {
            ++passed;
        }
    }
    EXPECT_GT(passed, 0);
    EXPECT_LT(passed, 100);

    limiter.SetRate(0);
    for (uint64_t traceHash = 6000; traceHash < 6100; ++traceHash) {
        ClearRequestDecision();
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));
    }
}

TEST_F(LogRateLimiterTest, ZeroRateDoesNotCreateLocalDecisionForRequestTrace)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(0);

    constexpr uint64_t traceHash = 666001;
    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));
    bool admitted = false;
    EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
}

TEST_F(LogRateLimiterTest, ConcurrentPropagatedTraceUsesOneDecision)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    constexpr int kThreads = 16;
    constexpr int kRounds = 50;
    TraceContext context;
    {
        TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
        bool admitted = false;
        ASSERT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
        ASSERT_TRUE(admitted);
        context = Trace::Instance().GetContext();
    }

    std::atomic<int> ready{ 0 };
    std::atomic<bool> start{ false };
    std::atomic<int> allowed{ 0 };
    std::atomic<int> dropped{ 0 };
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(context);
            uint64_t traceHash = Trace::Instance().GetCachedHash();
            for (int j = 0; j < kRounds; ++j) {
                if (limiter.ShouldLog(ds_spdlog::level::info, traceHash)) {
                    allowed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    dropped.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != kThreads) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(allowed.load(std::memory_order_relaxed), kThreads * kRounds);
    EXPECT_EQ(dropped.load(std::memory_order_relaxed), 0);
}

TEST_F(LogRateLimiterTest, ConcurrentDifferentTracesRespectQuota)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    constexpr int kThreads = 64;
    std::atomic<int> ready{ 0 };
    std::atomic<bool> start{ false };
    std::atomic<int> allowed{ 0 };

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&limiter, &allowed, &ready, &start, t]() {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            uint64_t traceHash = 900000 + static_cast<uint64_t>(t);
            Trace::Instance().SetRequestSampleDecision(false, false);
            if (limiter.ShouldLog(ds_spdlog::level::info, traceHash)) {
                allowed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != kThreads) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    for (auto &thread : threads) {
        thread.join();
    }

    // In one synchronized burst, admitted traces should be tightly bounded by per-second quota.
    EXPECT_LE(allowed.load(std::memory_order_relaxed), 2);
}

TEST_F(LogRateLimiterTest, LEVEL1_HighConcurrencyAdmittedTraceKeepsFullChain)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    TraceContext context;
    {
        TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
        bool admitted = false;
        ASSERT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
        ASSERT_TRUE(admitted);
        context = Trace::Instance().GetContext();
    }

    constexpr int kThreads = 64;
    constexpr int kRounds = 5000;
    std::atomic<int> ready{ 0 };
    std::atomic<bool> start{ false };
    std::atomic<int> nonErrorDrop{ 0 };
    std::atomic<int> errorDrop{ 0 };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(context);
            uint64_t traceHash = Trace::Instance().GetCachedHash();
            for (int i = 0; i < kRounds; ++i) {
                bool info = limiter.ShouldLog(ds_spdlog::level::info, traceHash);
                bool warn = limiter.ShouldLog(ds_spdlog::level::warn, traceHash);
                bool err = limiter.ShouldLog(ds_spdlog::level::err, traceHash);
                if (!info || !warn) {
                    nonErrorDrop.fetch_add(1, std::memory_order_relaxed);
                }
                if (!err) {
                    errorDrop.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != kThreads) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(nonErrorDrop.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(errorDrop.load(std::memory_order_relaxed), 0);
}

TEST_F(LogRateLimiterTest, LEVEL1_HighConcurrencyDecisionStablePerTrace)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    constexpr int kThreads = 48;
    constexpr int kTraceCount = 256;
    constexpr int kOpsPerThread = 8000;

    std::vector<TraceContext> contexts(kTraceCount);
    std::vector<int> expected(kTraceCount);
    for (int i = 0; i < kTraceCount; ++i) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID("trace-context-" + std::to_string(i));
        Trace::Instance().SetRequestLogTrace(true);
        bool admitted = i % 2 == 0;
        Trace::Instance().SetRequestSampleDecision(true, admitted);
        contexts[i] = Trace::Instance().GetContext();
        expected[i] = admitted ? 1 : 0;
    }
    Trace::Instance().Invalidate();

    std::atomic<int> ready{ 0 };
    std::atomic<bool> start{ false };
    std::atomic<int> levelMismatch{ 0 };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            uint64_t seed = 1469598103934665603ULL ^ static_cast<uint64_t>(t + 1);
            for (int i = 0; i < kOpsPerThread; ++i) {
                seed = seed * 1099511628211ULL + 0x9e3779b97f4a7c15ULL;
                int idx = static_cast<int>(seed % static_cast<uint64_t>(kTraceCount));

                TraceGuard traceGuard = Trace::Instance().SetTraceContext(contexts[idx]);
                uint64_t traceHash = Trace::Instance().GetCachedHash();
                bool info = limiter.ShouldLog(ds_spdlog::level::info, traceHash);
                bool warn = limiter.ShouldLog(ds_spdlog::level::warn, traceHash);
                bool err = limiter.ShouldLog(ds_spdlog::level::err, traceHash);

                int observed = info ? 1 : 0;
                if (observed != expected[idx] || warn != info || err != info) {
                    levelMismatch.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != kThreads) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(levelMismatch.load(std::memory_order_relaxed), 0);
}

TEST_F(LogRateLimiterTest, DecisionDoesNotDependOnGlobalTraceTable)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);

    constexpr uint64_t traceHash = 9900000;
    ASSERT_TRUE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));
    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::warn, traceHash));

    ClearRequestDecision();
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::info, traceHash));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::warn, traceHash));
    EXPECT_FALSE(limiter.ShouldLog(ds_spdlog::level::err, traceHash));
}

TEST_F(LogRateLimiterTest, LEVEL1_FastRequestDecisionP99IsBounded)
{
    constexpr int kBatches = 100;
    constexpr int kBatchSize = 1000;

    auto noRateBatchUs = MeasureDecisionBatches(0, kBatches, kBatchSize);
    auto rateLimitedBatchUs = MeasureDecisionBatches(100, kBatches, kBatchSize);
    int64_t noRateP99Us = Percentile99(noRateBatchUs);
    int64_t rateLimitedP99Us = Percentile99(rateLimitedBatchUs);

    std::cout << "LogRateLimiter batch p99(us), rate=0: " << noRateP99Us
              << ", rate=100: " << rateLimitedP99Us << ", batchSize: " << kBatchSize << std::endl;

    int64_t guardrailUs = std::max<int64_t>(noRateP99Us * 100 + 1000, 50000);
    EXPECT_LE(rateLimitedP99Us, guardrailUs);
}

}  // namespace ut
}  // namespace datasystem
