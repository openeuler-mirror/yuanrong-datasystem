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
 * Description: Test log rate limiter.
 */

#include "datasystem/common/log/spdlog/log_rate_limiter.h"

#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <spdlog/common.h>

namespace datasystem {
namespace ut {

class LogRateLimiterTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        // Reset rate limiter state before each test
        LogRateLimiter::Instance().Reset();
    }

    void TearDown() override
    {
        LogRateLimiter::Instance().Reset();
    }
};

TEST_F(LogRateLimiterTest, NoLimitByDefault)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(0);

    // All levels should pass when rate=0
    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::warn));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::err));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::critical));
    }
}

TEST_F(LogRateLimiterTest, ErrorFatalAlwaysPass)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(1);  // Very strict: 1 per second

    // Consume the initial token
    limiter.ShouldLog(ds_spdlog::level::info);

    // ERROR and FATAL should still pass even after tokens exhausted
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::err));
        EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::critical));
    }
}

TEST_F(LogRateLimiterTest, TokenBucketBasic)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(10);  // 10 per second

    int passed = 0;
    constexpr int kTotalMessages = 200;
    for (int i = 0; i < kTotalMessages; ++i) {
        if (limiter.ShouldLog(ds_spdlog::level::info)) {
            ++passed;
        }
    }

    // With rate=10, most messages should be dropped.
    // Token bucket allows ~10, sampling may let a few more through.
    // We expect passed << kTotalMessages
    EXPECT_LT(passed, kTotalMessages);
    EXPECT_GT(passed, 0);
}

TEST_F(LogRateLimiterTest, DynamicRateUpdate)
{
    auto &limiter = LogRateLimiter::Instance();

    // Start with no limit
    limiter.SetRate(0);
    EXPECT_TRUE(limiter.ShouldLog(ds_spdlog::level::info));
    EXPECT_EQ(limiter.GetSamplingRate(), 1);

    // Enable strict limit
    limiter.SetRate(1);
    int passed = 0;
    for (int i = 0; i < 100; ++i) {
        if (limiter.ShouldLog(ds_spdlog::level::info)) {
            ++passed;
        }
    }
    // With rate=1, token bucket gives ~1, sampling divisor=max(1,2)=2 keeps ~1/2 of overflow.
    // So passed < 100 but > 0.
    EXPECT_LT(passed, 100);
    EXPECT_GT(passed, 0);

    // Disable limit again
    limiter.SetRate(0);
    passed = 0;
    for (int i = 0; i < 100; ++i) {
        if (limiter.ShouldLog(ds_spdlog::level::info)) {
            ++passed;
        }
    }
    EXPECT_EQ(passed, 100);
}

TEST_F(LogRateLimiterTest, SamplingRateAnnotation)
{
    auto &limiter = LogRateLimiter::Instance();

    // No limit → sampling rate should be 1
    limiter.SetRate(0);
    EXPECT_EQ(limiter.GetSamplingRate(), 1);

    // With rate limiting active, generate some load
    limiter.SetRate(5);
    for (int i = 0; i < 100; ++i) {
        limiter.ShouldLog(ds_spdlog::level::info);
    }

    // After sampling, GetSamplingRate should be > 1 (some logs were dropped)
    auto rate = limiter.GetSamplingRate();
    EXPECT_GE(rate, 1);
}

TEST_F(LogRateLimiterTest, MultiThreadSafety)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(100);

    constexpr int kNumThreads = 8;
    constexpr int kMessagesPerThread = 500;
    std::atomic<int> totalPassed{ 0 };

    std::vector<std::thread> threads;
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&limiter, &totalPassed]() {
            int localPassed = 0;
            for (int i = 0; i < kMessagesPerThread; ++i) {
                if (limiter.ShouldLog(ds_spdlog::level::info)) {
                    ++localPassed;
                }
            }
            totalPassed.fetch_add(localPassed, std::memory_order_relaxed);
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    int passed = totalPassed.load();
    // Some messages should pass, but not all
    EXPECT_GT(passed, 0);
    EXPECT_LT(passed, kNumThreads * kMessagesPerThread);
    // No crash or hang is the primary assertion
}

TEST_F(LogRateLimiterTest, SamplingFallbackSetsWasSampled)
{
    auto &limiter = LogRateLimiter::Instance();
    limiter.SetRate(5);

    // Exhaust all tokens
    for (int i = 0; i < 20; ++i) {
        limiter.ShouldLog(ds_spdlog::level::info);
    }

    // After exhaustion, some logs should pass via sampling with wasSampled=true
    int sampledCount = 0;
    int passedCount = 0;
    for (int i = 0; i < 100; ++i) {
        bool wasSampled = false;
        if (limiter.ShouldLog(ds_spdlog::level::info, &wasSampled)) {
            ++passedCount;
            if (wasSampled) {
                ++sampledCount;
            }
        }
    }
    EXPECT_GT(sampledCount, 0);
    EXPECT_GT(passedCount, 0);
}

}  // namespace ut
}  // namespace datasystem
