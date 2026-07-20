/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Fast deterministic tests for LogSampler and AccessRecorder facade.
 * These tests run quickly (no 10-second stress runs) and validate sampler
 * correctness: sampled-out fast-path, sampled-in full-path, mixed distribution,
 * and the ObjectKeyProvider deferred-evaluation contract.
 */
#include "datasystem/common/log/log.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/access_recorder.h"

DS_DECLARE_double(request_sample_rate);
DS_DECLARE_double(access_sample_rate);
DS_DECLARE_double(diagnostic_sample_rate);
DS_DECLARE_uint32(stderrthreshold);
DS_DECLARE_bool(log_async);

namespace datasystem {
namespace ut {
constexpr int OUTPUT_PRECISION = 2;

class LogSamplerFastTest : public CommonTest {
public:
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

    void StartLogging()
    {
        FLAGS_stderrthreshold = LogSeverity::FATAL;
        FLAGS_log_async = true;
        Logging::GetInstance()->Start("ds_llt", LogProcessRole::CLIENT, 1);
    }

    void StartLoggingWithSamplerConfig(double requestRate, double accessRate, double diagnosticRate)
    {
        FLAGS_request_sample_rate = requestRate;
        FLAGS_access_sample_rate = accessRate;
        FLAGS_diagnostic_sample_rate = diagnosticRate;
        LogSampler::Instance().ResetForTest();
        StartLogging();
        LogSampleUserConfig cfg;
        cfg.requestSampleRate = requestRate;
        cfg.requestSampleRateExplicit = true;
        cfg.accessSampleRate = accessRate;
        cfg.accessSampleRateExplicit = true;
        cfg.diagnosticSampleRate = diagnosticRate;
        cfg.diagnosticSampleRateExplicit = true;
        LogSampler::Instance().UpdateConfigFromFlags(cfg);
    }

    double CalculatePercentile(std::vector<double> latencies, double percentile)
    {
        if (latencies.empty()) {
            return 0.0;
        }
        std::sort(latencies.begin(), latencies.end());
        size_t index = static_cast<size_t>(latencies.size() * percentile);
        if (index >= latencies.size()) {
            index = latencies.size() - 1;
        }
        return latencies[index];
    }

    double CalculateAvgLatency(std::vector<double> &latencies)
    {
        if (latencies.empty()) {
            return 0.0;
        }
        double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
        return sum / latencies.size();
    }

    struct AccessRecorderResult {
        double avgUs = 0.0;
        double p50Us = 0.0;
        double p99Us = 0.0;
        int totalCalls = 0;
        int sampledInCalls = 0;
        int sampledOutCalls = 0;
    };

    AccessRecorderResult RunAccessRecorderWorkload(double accessRate, double requestRate,
                                                    int callsPerThread, int threadCount)
    {
        StartLoggingWithSamplerConfig(requestRate, accessRate, 1.0);
        std::atomic<int> sampledIn{ 0 };
        std::vector<std::vector<double>> threadLatencies(threadCount);
        std::vector<std::thread> threads;
        threads.reserve(threadCount);

        for (int t = 0; t < threadCount; t++) {
            threads.emplace_back([&, t]() {
                threadLatencies[t].reserve(callsPerThread);
                for (int i = 0; i < callsPerThread; i++) {
                    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
                    auto begin = std::chrono::high_resolution_clock::now();
                    {
                        auto ap = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
                        ap.ObjectKeyProvider([&]() {
                            sampledIn.fetch_add(1, std::memory_order_relaxed);
                            return std::string("bench_key");
                        })
                        .TimeoutMs(100).Result(0).DataSize(100).Record();
                    }
                    auto end = std::chrono::high_resolution_clock::now();
                    threadLatencies[t].push_back(
                        std::chrono::duration<double, std::micro>(end - begin).count());
                }
            });
        }

        for (auto &thread : threads) {
            thread.join();
        }

        std::vector<double> allLatencies;
        allLatencies.reserve(callsPerThread * threadCount);
        for (auto &latencies : threadLatencies) {
            allLatencies.insert(allLatencies.end(), latencies.begin(), latencies.end());
        }

        AccessRecorderResult result;
        result.totalCalls = callsPerThread * threadCount;
        result.sampledInCalls = sampledIn.load(std::memory_order_relaxed);
        result.sampledOutCalls = result.totalCalls - result.sampledInCalls;
        result.avgUs = CalculateAvgLatency(allLatencies);
        result.p50Us = CalculatePercentile(allLatencies, 0.50);
        result.p99Us = CalculatePercentile(allLatencies, 0.99);
        return result;
    }
};

TEST_F(LogSamplerFastTest, ACCESS_LEVEL1_SampledOutFastPath)
{
    auto result = RunAccessRecorderWorkload(0.0, 0.0, 10000, 1);
    std::cout << "ACCESS_SAMPLED_OUT" << " avgUs=" << std::fixed << std::setprecision(OUTPUT_PRECISION)
              << result.avgUs << " p50Us=" << result.p50Us << " p99Us=" << result.p99Us
              << " totalCalls=" << result.totalCalls << " sampledIn=" << result.sampledInCalls
              << " sampledOut=" << result.sampledOutCalls << std::endl;

    EXPECT_EQ(result.sampledInCalls, 0);
    EXPECT_GT(result.sampledOutCalls, 0);
    EXPECT_LT(result.p99Us, 2000.0);
}

TEST_F(LogSamplerFastTest, ACCESS_LEVEL1_SampledInFullPath)
{
    auto result = RunAccessRecorderWorkload(1.0, 1.0, 10000, 1);
    std::cout << "ACCESS_SAMPLED_IN" << " avgUs=" << std::fixed << std::setprecision(OUTPUT_PRECISION)
              << result.avgUs << " p50Us=" << result.p50Us << " p99Us=" << result.p99Us
              << " totalCalls=" << result.totalCalls << " sampledIn=" << result.sampledInCalls
              << " sampledOut=" << result.sampledOutCalls << std::endl;

    EXPECT_GT(result.sampledInCalls, 0);
    EXPECT_EQ(result.sampledOutCalls, 0);
    EXPECT_LT(result.p99Us, 5000.0);
}

TEST_F(LogSamplerFastTest, ACCESS_LEVEL1_SampledMixedDistribution)
{
    auto result = RunAccessRecorderWorkload(0.5, 0.0, 50000, 1);
    std::cout << "ACCESS_SAMPLED_MIXED" << " avgUs=" << std::fixed << std::setprecision(OUTPUT_PRECISION)
              << result.avgUs << " p50Us=" << result.p50Us << " p99Us=" << result.p99Us
              << " totalCalls=" << result.totalCalls << " sampledIn=" << result.sampledInCalls
              << " sampledOut=" << result.sampledOutCalls << std::endl;

    EXPECT_GT(result.sampledInCalls, 0);
    EXPECT_GT(result.sampledOutCalls, 0);
    double inRatio = static_cast<double>(result.sampledInCalls) / result.totalCalls;
    EXPECT_NEAR(inRatio, 0.5, 0.1);
}

TEST_F(LogSamplerFastTest, LEVEL1_AccessRecorderSampledOutDoesNotBuildFields)
{
    StartLoggingWithSamplerConfig(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);
    std::atomic<int> providerCalls{ 0 };
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    for (int i = 0; i < 10000; ++i) {
        auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_GET);
        access.ObjectKeyProvider([&] {
                  providerCalls.fetch_add(1, std::memory_order_relaxed);
                  return std::string("expensive-key");
              })
              .Result(Status::OK())
              .Record();
    }

    EXPECT_EQ(providerCalls.load(), 0);
}

}  // namespace ut
}  // namespace datasystem
