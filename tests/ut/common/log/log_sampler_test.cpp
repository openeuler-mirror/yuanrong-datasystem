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
 * Description: Unit tests for LogSampler core component.
 */

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/access_recorder.h"

#include <atomic>
#include <cmath>
#include <cstdint>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/protos/share_memory.pb.h"

namespace datasystem {
namespace ut {

class LogSamplerTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        LogSampler::Instance().ResetForTest();
    }

    void TearDown() override
    {
        LogSampler::Instance().ResetForTest();
        CommonTest::TearDown();
    }
};

// Test #1: each rate independently controls its own category
TEST_F(LogSamplerTest, IndependentRates)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.2;
    cfg.accessSampleRate = 0.3;
    cfg.diagnosticSampleRate = 0.4;

    ASSERT_TRUE(s.UpdateConfigFromFlags(cfg));

    auto *snap = s.GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);

    EXPECT_EQ(snap->config.requestRate.ppm, 200000);
    EXPECT_EQ(snap->config.accessRate.ppm, 300000);
    EXPECT_EQ(snap->config.diagnosticRate.ppm, 400000);
    EXPECT_TRUE(snap->config.enabled);
}

// Test #3: only request set — access/diagnostic default to full retention (1.0)
TEST_F(LogSamplerTest, OnlyRequestSetOthersDefaultFull)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.2;

    ASSERT_TRUE(s.UpdateConfigFromFlags(cfg));

    auto *snap = s.GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);

    EXPECT_EQ(snap->config.requestRate.ppm, 200000);
    EXPECT_EQ(snap->config.accessRate.ppm, kSamplePpmBase);
    EXPECT_EQ(snap->config.diagnosticRate.ppm, kSamplePpmBase);
    EXPECT_TRUE(snap->config.enabled);
}

// Test #5: enabled归一化
TEST_F(LogSamplerTest, EnabledNormalization)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    // All 1.0 → enabled=false
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 1.0;
    cfg.accessSampleRate = 1.0;
    cfg.diagnosticSampleRate = 1.0;

    ASSERT_TRUE(s.UpdateConfigFromFlags(cfg));
    EXPECT_FALSE(s.IsSamplerEnabledFast());

    // Partial non-1.0 → enabled=true
    s.ResetForTest();
    cfg.requestSampleRate = 0.5;
    ASSERT_TRUE(s.UpdateConfigFromFlags(cfg));
    EXPECT_TRUE(s.IsSamplerEnabledFast());
}

// Test #6: 配置校验失败
TEST_F(LogSamplerTest, InvalidConfigRejected)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    // First: valid config
    LogSampleUserConfig validCfg;
    validCfg.requestSampleRate = 0.5;
    ASSERT_TRUE(s.UpdateConfigFromFlags(validCfg));

    auto *snap1 = s.GetSnapshotForTest();
    ASSERT_NE(snap1, nullptr);
    EXPECT_EQ(snap1->config.requestRate.ppm, 500000);

    // Second: invalid config (negative)
    LogSampleUserConfig invalidCfg;
    invalidCfg.requestSampleRate = -0.1;
    EXPECT_FALSE(s.UpdateConfigFromFlags(invalidCfg));

    // Previous-good config should remain
    auto *snap2 = s.GetSnapshotForTest();
    ASSERT_NE(snap2, nullptr);
    EXPECT_EQ(snap2->config.requestRate.ppm, 500000);

    // NaN
    s.ResetForTest();
    LogSampleUserConfig nanCfg;
    nanCfg.requestSampleRate = std::nan("");
    EXPECT_FALSE(s.UpdateConfigFromFlags(nanCfg));

    // > 1.0
    s.ResetForTest();
    LogSampleUserConfig overflowCfg;
    overflowCfg.requestSampleRate = 1.5;
    EXPECT_FALSE(s.UpdateConfigFromFlags(overflowCfg));
}

// Test #9: hash-threshold决策 (ppm ∈ (0, 1M))
TEST_F(LogSamplerTest, HashThresholdDecision)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();
    s.SetSaltForTest(0x123456789ABCDEF0ULL);

    // Configure ppm=500000 (50%)
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    ASSERT_TRUE(s.UpdateConfigFromFlags(cfg));

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    SampleRate halfRate;
    halfRate.ppm = 500000;
    halfRate.threshold = BuildThreshold(500000);

    // First call creates decision
    bool firstResult = s.IsCurrentRequestSampledIn(halfRate);
    bool admitted1 = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted1));
    EXPECT_EQ(admitted1, firstResult);

    // Second call reads cached decision (no new hash)
    (void)s.IsCurrentRequestSampledIn(halfRate);
    bool admitted2 = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted2));
    EXPECT_EQ(admitted2, firstResult);  // Same cached result
}

// Test #10: shared trace-hash threshold distribution (Mix64 + BuildThreshold)
TEST_F(LogSamplerTest, RandomDistribution)
{
    constexpr uint64_t kSalt = 42;

    SampleRate halfRate;
    halfRate.ppm = 500000;
    halfRate.threshold = BuildThreshold(500000);

    int hits = 0;
    constexpr int kNumKeys = 100000;
    for (int i = 0; i < kNumKeys; ++i) {
        uint64_t traceHash = static_cast<uint64_t>(i * 1000);
        if (Mix64(traceHash ^ kSalt) <= halfRate.threshold) {
            ++hits;
        }
    }

    double ratio = static_cast<double>(hits) / kNumKeys;
    EXPECT_NEAR(ratio, 0.5, 0.01);  // 50% ± 1% (LS-015b design spec)
}

// Test #12: FATAL bypass
TEST_F(LogSamplerTest, FatalAlwaysPass)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    // Configure sampler with non-zero rates
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.0;
    cfg.accessSampleRate = 0.0;
    cfg.diagnosticSampleRate = 0.0;
    ASSERT_TRUE(s.UpdateConfigFromFlags(cfg));

    EXPECT_TRUE(s.ShouldCreateRuntimeLog(LogSeverity::FATAL, false));
}

// Test #13: disabled快速路径
TEST_F(LogSamplerTest, DisabledFastPath)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    // Default state: no config → enabled=false
    EXPECT_FALSE(s.IsSamplerEnabledFast());

    // ShouldCreateRuntimeLog should return true when disabled
    EXPECT_TRUE(s.ShouldCreateRuntimeLog(LogSeverity::INFO, false));
    EXPECT_TRUE(s.ShouldCreateRuntimeLog(LogSeverity::ERROR, false));
}

// Test #14: Random distribution proportion 0.1 (LS-015a)
TEST_F(LogSamplerTest, RandomDistribution_0_1)
{
    constexpr uint64_t kSalt = 42;

    SampleRate rate;
    rate.ppm = 100000;
    rate.threshold = BuildThreshold(100000);

    int hits = 0;
    constexpr int kNumKeys = 100000;
    for (int i = 0; i < kNumKeys; ++i) {
        uint64_t traceHash = static_cast<uint64_t>(i * 1000);
        if (Mix64(traceHash ^ kSalt) <= rate.threshold) {
            ++hits;
        }
    }

    double ratio = static_cast<double>(hits) / kNumKeys;
    EXPECT_NEAR(ratio, 0.1, 0.01);  // 10% ± 1%
}

// Test #15: Random distribution proportion 0.9 (LS-015c)
TEST_F(LogSamplerTest, RandomDistribution_0_9)
{
    constexpr uint64_t kSalt = 42;

    SampleRate rate;
    rate.ppm = 900000;
    rate.threshold = BuildThreshold(900000);

    int hits = 0;
    constexpr int kNumKeys = 100000;
    for (int i = 0; i < kNumKeys; ++i) {
        uint64_t traceHash = static_cast<uint64_t>(i * 1000);
        if (Mix64(traceHash ^ kSalt) <= rate.threshold) {
            ++hits;
        }
    }

    double ratio = static_cast<double>(hits) / kNumKeys;
    EXPECT_NEAR(ratio, 0.9, 0.01);  // 90% ± 1%
}

// Test #16: 20-bucket distribution (LS-015d)
TEST_F(LogSamplerTest, RandomBucketDistribution)
{
    constexpr uint64_t kSalt = 42;

    SampleRate halfRate;
    halfRate.ppm = 500000;
    halfRate.threshold = BuildThreshold(500000);

    constexpr int kNumBuckets = 20;
    constexpr int kNumKeys = 100000;
    std::vector<int> bucketHits(kNumBuckets, 0);
    std::vector<int> bucketTotal(kNumBuckets, 0);

    for (int i = 0; i < kNumKeys; ++i) {
        uint64_t traceHash = static_cast<uint64_t>(i * 997);
        int bucket = static_cast<int>(i % kNumBuckets);
        bucketTotal[bucket]++;
        if (Mix64(traceHash ^ kSalt) <= halfRate.threshold) {
            bucketHits[bucket]++;
        }
    }

    int firstHalfHits = 0;
    int secondHalfHits = 0;
    for (int b = 0; b < kNumBuckets; ++b) {
        ASSERT_GT(bucketTotal[b], 0);
        double ratio = static_cast<double>(bucketHits[b]) / bucketTotal[b];
        EXPECT_NEAR(ratio, 0.5, 0.05);  // Each bucket: 50% ± 5%

        if (b < kNumBuckets / 2) {
            firstHalfHits += bucketHits[b];
        } else {
            secondHalfHits += bucketHits[b];
        }
    }

    EXPECT_GT(firstHalfHits, 0);   // Front half has samples
    EXPECT_GT(secondHalfHits, 0);  // Back half has samples
}

// Test #17: Concurrent config update + hot-path read (thread safety)
TEST_F(LogSamplerTest, ConcurrentConfigUpdateAndHotPathRead)
{
    LogSampler &s = LogSampler::Instance();
    s.ResetForTest();

    constexpr int kConfigThreads = 4;
    constexpr int kHotPathThreads = 8;
    constexpr int kConfigUpdatesPerThread = 30;
    constexpr int kHotPathIterations = 5000;

    std::atomic<bool> configDone{false};
    std::atomic<int> configUpdates{0};

    auto configWorker = [&](int threadIdx) {
        for (int i = 0; i < kConfigUpdatesPerThread; ++i) {
            if (i % 2 == 0) {
                LogSampleUserConfig cfg;
                double rate = 0.1 + 0.1 * ((threadIdx + i) % 10);
                cfg.requestSampleRate = rate;
                s.UpdateConfigFromFlags(cfg);
            } else {
                LogSampleConfigPb proto;
                proto.set_enabled(true);
                uint32_t ppm = 100000 + 100000 * ((threadIdx + i) % 9);
                proto.set_request_sample_ppm(ppm);
                proto.set_access_sample_ppm(kSamplePpmBase);
                proto.set_diagnostic_sample_ppm(ppm);
                s.UpdateConfigFromProto(proto);
            }
            configUpdates.fetch_add(1, std::memory_order_relaxed);
        }
    };

    auto hotPathReader = [&]() {
        for (int i = 0; i < kHotPathIterations; ++i) {
            s.IsSamplerEnabledFast();
            s.ShouldCreateRuntimeLog(LogSeverity::INFO, false);
            s.ShouldCreateRuntimeLog(LogSeverity::WARNING, true);
            s.IsCurrentRequestSampledIn();
            s.ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET);
            s.ShouldRecordAccessType(AccessKeyType::CLIENT);
            s.ShouldRecordAccessType(AccessKeyType::ACCESS);
            s.ShouldRecordAccessType(AccessKeyType::REQUEST_OUT);
        }
        while (!configDone.load(std::memory_order_acquire)) {
            s.IsSamplerEnabledFast();
            s.ShouldCreateRuntimeLog(LogSeverity::INFO, false);
            s.IsCurrentRequestSampledIn();
            s.ShouldRecordAccessType(AccessKeyType::CLIENT);
        }
    };

    std::vector<std::thread> configThreads;
    std::vector<std::thread> hotPathThreads;
    for (int t = 0; t < kConfigThreads; ++t) {
        configThreads.emplace_back(configWorker, t);
    }
    for (int t = 0; t < kHotPathThreads; ++t) {
        hotPathThreads.emplace_back(hotPathReader);
    }

    for (auto &t : configThreads) {
        if (t.joinable()) {
            t.join();
        }
    }
    configDone.store(true, std::memory_order_release);

    for (auto &t : hotPathThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    EXPECT_GT(configUpdates.load(), 0);
}

}  // namespace ut
}  // namespace datasystem