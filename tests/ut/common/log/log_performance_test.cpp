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
 * Description: Test logging performance.
 */
#include "datasystem/common/log/log.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/log_sampler.h"

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_bool(logtostderr);
DS_DECLARE_bool(log_async);
DS_DECLARE_double(request_sample_rate);
DS_DECLARE_double(access_sample_rate);
DS_DECLARE_double(diagnostic_sample_rate);
DS_DECLARE_string(log_dir);
DS_DECLARE_uint32(max_log_size);
DS_DECLARE_uint32(stderrthreshold);

namespace datasystem {
namespace ut {
// Estimated size of spdlog's default log prefix (timestamp + level + thread ID + delimiters)
// Example: "2025-08-06T02:57:13.517005 | I | log_performance_test.cpp:171 |  |   | 52385:52385 |  |  |  "
constexpr size_t LOG_PREFIX_ESTIMATED_SIZE = 85;  // Bytes
constexpr double MICROSEC_TO_MILLISEC = 1e3;
constexpr double MICROSEC_TO_SEC = 1e6;
constexpr int WARMUP_ITERATIONS = 1000;
constexpr int OUTPUT_PRECISION = 2;
constexpr int PURE_LOG_THREAD_COUNT = 32;
constexpr int PURE_LOG_PER_THREAD_QPS = 125;
constexpr int PURE_LOG_TARGET_QPS = PURE_LOG_THREAD_COUNT * PURE_LOG_PER_THREAD_QPS;
constexpr int PURE_LOG_DURATION_SEC = 10;
constexpr double PURE_LOG_SAMPLE_RATE = 0.5;

/**
 * Test log asynchronous mode performance testing
 * Sets up common test environment and utility functions
 */
class LogPerformanceTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        LogSampler::Instance().ResetForTest();
    }

    void TearDown() override
    {
        Provider::Instance().FlushLogs();
        LogSampler::Instance().ResetForTest();
        CommonTest::TearDown();
    }

    void StartLogging()
    {
        FLAGS_alsologtostderr = false;
        FLAGS_logtostderr = false;
        FLAGS_stderrthreshold = LogSeverity::FATAL;
        FLAGS_max_log_size = 2048;
        FLAGS_log_async = true;

        Logging::GetInstance()->Start("ds_llt", true, 1);
    }

    void StartLoggingWithSamplerRate(double requestRate)
    {
        FLAGS_request_sample_rate = requestRate;
        LogSampler::Instance().ResetForTest();
        StartLogging();
        LogSampleUserConfig cfg;
        cfg.requestSampleRate = requestRate;
        cfg.requestSampleRateExplicit = true;
        LogSampler::Instance().UpdateConfigFromFlags(cfg);
    }

    void StartLoggingWithoutSampler()
    {
        LogSampler::Instance().ResetForTest();
        StartLogging();
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

    /**
     * Generate test log content of specified payload length
     * @param payloadLength Length of the actual log message content (excluding prefix)
     * @return Generated string with exactly 'payloadLength' characters
     */
    std::string GenerateLogPayload(size_t payloadLength)
    {
        static const std::string base = "abcdefghijklmnopqrstuvwxyz1234567890";
        std::string content;
        while (content.size() < payloadLength) {
            content += base;
        }

        return content.substr(0, payloadLength);
    }

    /**
     * Calculate total log size including estimated prefix
     * @param payloadLength Length of the log message content
     * @return Total log size (payload + prefix) in bytes
     */
    size_t CalculateTotalLogSize(size_t payloadLength)
    {
        return payloadLength + LOG_PREFIX_ESTIMATED_SIZE;
    }

    /**
     * High-precision timer utility for measuring microsecond-level performance
     */
    class Timer {
    public:
        /** Start the timer */
        void Start()
        {
            startTime_ = std::chrono::high_resolution_clock::now();
        }

        /**
         * Calculate elapsed time since start() was called
         * @return Elapsed time in microseconds
         */
        double ElapsedUs()
        {
            auto endTime = std::chrono::high_resolution_clock::now();
            return std::chrono::duration<double, std::micro>(endTime - startTime_).count();
        }

    private:
        std::chrono::high_resolution_clock::time_point startTime_;
    };

    /**
     * Calculate P99 latency (99% of log operations complete faster than this value)
     * @param latencies Collection of latency data points
     * @return P99 latency value in microseconds
     */
    double CalculateP99(std::vector<double> &latencies)
    {
        if (latencies.empty())
            return 0.0;

        std::sort(latencies.begin(), latencies.end());
        size_t index = static_cast<size_t>(latencies.size() * 0.99);
        // Handle edge cases (when total count < 100, use last element)
        if (index >= latencies.size())
            index = latencies.size() - 1;

        return latencies[index];
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

    /**
     * Calculate average latency
     * @param latencies Collection of latency data points
     * @return Average latency in microseconds
     */
    double CalculateAvgLatency(std::vector<double> &latencies)
    {
        if (latencies.empty())
            return 0.0;

        double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
        return sum / latencies.size();
    }

    int CountInfoLogLinesWithMarker(const std::string &marker)
    {
        DIR *dp = opendir(FLAGS_log_dir.c_str());
        if (dp == nullptr) {
            return 0;
        }

        int count = 0;
        struct dirent *entry = nullptr;
        while ((entry = readdir(dp)) != nullptr) {
            std::string filename(entry->d_name);
            if (filename.find("ds_llt") == std::string::npos || filename.find(".INFO") == std::string::npos
                || filename.find(".log") == std::string::npos) {
                continue;
            }
            std::ifstream file(FLAGS_log_dir + "/" + filename);
            std::string line;
            while (std::getline(file, line)) {
                if (line.find(marker) != std::string::npos) {
                    ++count;
                }
            }
        }
        closedir(dp);
        return count;
    }

    struct PureLogResult {
        double rate = 0;
        double accessRate = 0;
        double diagnosticRate = 0;
        bool directLog = false;
        bool expensivePayload = false;
        bool samplerOnly = false;
        std::string samplerMode;
        int totalRequests = 0;
        double elapsedSec = 0.0;
        double achievedQps = 0.0;
        double avgUs = 0.0;
        double p50Us = 0.0;
        double p95Us = 0.0;
        double p99Us = 0.0;
        double maxUs = 0.0;
        int writtenSamples = 0;
        int payloadBuilds = 0;
    };

    std::string BuildExpensivePayload(const std::string &payload, int threadId, int seq,
                                      std::atomic<int> &payloadBuilds)
    {
        payloadBuilds.fetch_add(1, std::memory_order_relaxed);
        std::string message;
        message.reserve(payload.size() * 4 + 128);
        message.append(" thread=").append(std::to_string(threadId));
        message.append(" seq=").append(std::to_string(seq));
        message.append(" payload=");
        for (int i = 0; i < 4; ++i) {
            message.append(payload);
        }
        return message;
    }

    void EmitPureLogLine(bool directLog, bool expensivePayload, const std::string &marker, const std::string &payload,
                         int threadId, int seq, std::atomic<int> &payloadBuilds)
    {
        if (expensivePayload) {
            if (directLog) {
                LogMessage(LogSeverity::INFO, __FILE__, __LINE__).Stream()
                    << marker << BuildExpensivePayload(payload, threadId, seq, payloadBuilds);
            } else {
                LOG(INFO) << marker << BuildExpensivePayload(payload, threadId, seq, payloadBuilds);
            }
            return;
        }

        if (directLog) {
            LogMessage(LogSeverity::INFO, __FILE__, __LINE__).Stream()
                << marker << ", thread=" << threadId << ", seq=" << seq << ", payload=" << payload;
        } else {
            LOG(INFO) << marker << ", thread=" << threadId << ", seq=" << seq << ", payload=" << payload;
        }
    }

    PureLogResult RunPureLogWorkload(double requestRate, const std::string &marker, bool expensivePayload,
                                     bool directLog, const std::string &samplerMode = "")
    {
        StartLoggingWithSamplerRate(requestRate);

        constexpr int requestsPerThread = PURE_LOG_PER_THREAD_QPS * PURE_LOG_DURATION_SEC;
        constexpr int totalRequests = PURE_LOG_THREAD_COUNT * requestsPerThread;
        const std::string payload = GenerateLogPayload(64);

        std::atomic<int> ready{ 0 };
        std::atomic<bool> start{ false };
        std::atomic<int> payloadBuilds{ 0 };
        std::chrono::steady_clock::time_point scheduledStart;
        std::vector<std::vector<double>> threadLatencies(PURE_LOG_THREAD_COUNT);
        std::vector<std::thread> threads;
        threads.reserve(PURE_LOG_THREAD_COUNT);

        for (int threadId = 0; threadId < PURE_LOG_THREAD_COUNT; ++threadId) {
            threads.emplace_back([&, threadId]() {
                threadLatencies[threadId].reserve(requestsPerThread);
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                const auto period = std::chrono::microseconds(1000000 / PURE_LOG_PER_THREAD_QPS);
                for (int i = 0; i < requestsPerThread; ++i) {
                    std::this_thread::sleep_until(scheduledStart + period * i);

                    auto begin = std::chrono::high_resolution_clock::now();
                    {
                        TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                        EmitPureLogLine(directLog, expensivePayload, marker, payload, threadId, i, payloadBuilds);
                    }
                    auto end = std::chrono::high_resolution_clock::now();
                    threadLatencies[threadId].push_back(
                        std::chrono::duration<double, std::micro>(end - begin).count());
                }
                Trace::Instance().Invalidate();
            });
        }

        while (ready.load(std::memory_order_acquire) != PURE_LOG_THREAD_COUNT) {
            std::this_thread::yield();
        }
        scheduledStart = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        start.store(true, std::memory_order_release);

        for (auto &thread : threads) {
            thread.join();
        }
        const auto finish = std::chrono::steady_clock::now();
        Provider::Instance().FlushLogs();

        std::vector<double> allLatencies;
        allLatencies.reserve(totalRequests);
        for (auto &latencies : threadLatencies) {
            allLatencies.insert(allLatencies.end(), latencies.begin(), latencies.end());
        }
        EXPECT_EQ(allLatencies.size(), static_cast<size_t>(totalRequests));

        PureLogResult result;
        result.rate = requestRate;
        result.directLog = directLog;
        result.expensivePayload = expensivePayload;
        result.samplerMode = samplerMode.empty()
            ? (std::fabs(requestRate - 1.0) < 1e-9
                   ? "sampler-through"
                   : (std::fabs(requestRate) < 1e-9 ? "sampler-reject" : "sampler-half"))
            : samplerMode;
        result.samplerOnly = false;
        result.totalRequests = totalRequests;
        result.elapsedSec = std::chrono::duration<double>(finish - scheduledStart).count();
        result.achievedQps = totalRequests / result.elapsedSec;
        result.p50Us = CalculatePercentile(allLatencies, 0.50);
        result.p95Us = CalculatePercentile(allLatencies, 0.95);
        result.p99Us = CalculatePercentile(allLatencies, 0.99);
        result.avgUs = CalculateAvgLatency(allLatencies);
        result.maxUs = *std::max_element(allLatencies.begin(), allLatencies.end());
        result.writtenSamples = CountInfoLogLinesWithMarker(marker);
        result.payloadBuilds = payloadBuilds.load(std::memory_order_relaxed);
        return result;
    }

    PureLogResult RunPureLogWorkloadMultiRate(double requestRate, double accessRate, double diagnosticRate,
                                               const std::string &marker, bool expensivePayload, bool directLog,
                                               const std::string &samplerMode = "")
    {
        StartLoggingWithSamplerConfig(requestRate, accessRate, diagnosticRate);

        constexpr int requestsPerThread = PURE_LOG_PER_THREAD_QPS * PURE_LOG_DURATION_SEC;
        constexpr int totalRequests = PURE_LOG_THREAD_COUNT * requestsPerThread;
        const std::string payload = GenerateLogPayload(64);

        std::atomic<int> ready{ 0 };
        std::atomic<bool> start{ false };
        std::atomic<int> payloadBuilds{ 0 };
        std::chrono::steady_clock::time_point scheduledStart;
        std::vector<std::vector<double>> threadLatencies(PURE_LOG_THREAD_COUNT);
        std::vector<std::thread> threads;
        threads.reserve(PURE_LOG_THREAD_COUNT);

        for (int threadId = 0; threadId < PURE_LOG_THREAD_COUNT; ++threadId) {
            threads.emplace_back([&, threadId]() {
                threadLatencies[threadId].reserve(requestsPerThread);
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                const auto period = std::chrono::microseconds(1000000 / PURE_LOG_PER_THREAD_QPS);
                for (int i = 0; i < requestsPerThread; ++i) {
                    std::this_thread::sleep_until(scheduledStart + period * i);

                    auto begin = std::chrono::high_resolution_clock::now();
                    {
                        TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                        EmitPureLogLine(directLog, expensivePayload, marker, payload, threadId, i, payloadBuilds);
                    }
                    auto end = std::chrono::high_resolution_clock::now();
                    threadLatencies[threadId].push_back(
                        std::chrono::duration<double, std::micro>(end - begin).count());
                }
                Trace::Instance().Invalidate();
            });
        }

        while (ready.load(std::memory_order_acquire) != PURE_LOG_THREAD_COUNT) {
            std::this_thread::yield();
        }
        scheduledStart = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        start.store(true, std::memory_order_release);

        for (auto &thread : threads) {
            thread.join();
        }
        const auto finish = std::chrono::steady_clock::now();
        Provider::Instance().FlushLogs();

        std::vector<double> allLatencies;
        allLatencies.reserve(totalRequests);
        for (auto &latencies : threadLatencies) {
            allLatencies.insert(allLatencies.end(), latencies.begin(), latencies.end());
        }
        EXPECT_EQ(allLatencies.size(), static_cast<size_t>(totalRequests));

        PureLogResult result;
        result.rate = requestRate;
        result.accessRate = accessRate;
        result.diagnosticRate = diagnosticRate;
        result.directLog = directLog;
        result.expensivePayload = expensivePayload;
        result.samplerMode = samplerMode.empty() ? "multi-rate" : samplerMode;
        result.samplerOnly = false;
        result.totalRequests = totalRequests;
        result.elapsedSec = std::chrono::duration<double>(finish - scheduledStart).count();
        result.achievedQps = totalRequests / result.elapsedSec;
        result.p50Us = CalculatePercentile(allLatencies, 0.50);
        result.p95Us = CalculatePercentile(allLatencies, 0.95);
        result.p99Us = CalculatePercentile(allLatencies, 0.99);
        result.avgUs = CalculateAvgLatency(allLatencies);
        result.maxUs = *std::max_element(allLatencies.begin(), allLatencies.end());
        result.writtenSamples = CountInfoLogLinesWithMarker(marker);
        result.payloadBuilds = payloadBuilds.load(std::memory_order_relaxed);
        return result;
    }

    PureLogResult RunPureLogWorkloadNoSampler(const std::string &marker, bool expensivePayload)
    {
        StartLoggingWithoutSampler();

        constexpr int requestsPerThread = PURE_LOG_PER_THREAD_QPS * PURE_LOG_DURATION_SEC;
        constexpr int totalRequests = PURE_LOG_THREAD_COUNT * requestsPerThread;
        const std::string payload = GenerateLogPayload(64);

        std::atomic<int> ready{ 0 };
        std::atomic<bool> start{ false };
        std::atomic<int> payloadBuilds{ 0 };
        std::chrono::steady_clock::time_point scheduledStart;
        std::vector<std::vector<double>> threadLatencies(PURE_LOG_THREAD_COUNT);
        std::vector<std::thread> threads;
        threads.reserve(PURE_LOG_THREAD_COUNT);

        for (int threadId = 0; threadId < PURE_LOG_THREAD_COUNT; ++threadId) {
            threads.emplace_back([&, threadId]() {
                threadLatencies[threadId].reserve(requestsPerThread);
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                const auto period = std::chrono::microseconds(1000000 / PURE_LOG_PER_THREAD_QPS);
                for (int i = 0; i < requestsPerThread; ++i) {
                    std::this_thread::sleep_until(scheduledStart + period * i);

                    auto begin = std::chrono::high_resolution_clock::now();
                    {
                        TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
                        EmitPureLogLine(false, expensivePayload, marker, payload, threadId, i, payloadBuilds);
                    }
                    auto end = std::chrono::high_resolution_clock::now();
                    threadLatencies[threadId].push_back(
                        std::chrono::duration<double, std::micro>(end - begin).count());
                }
                Trace::Instance().Invalidate();
            });
        }

        while (ready.load(std::memory_order_acquire) != PURE_LOG_THREAD_COUNT) {
            std::this_thread::yield();
        }
        scheduledStart = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        start.store(true, std::memory_order_release);

        for (auto &thread : threads) {
            thread.join();
        }
        const auto finish = std::chrono::steady_clock::now();
        Provider::Instance().FlushLogs();

        std::vector<double> allLatencies;
        allLatencies.reserve(totalRequests);
        for (auto &latencies : threadLatencies) {
            allLatencies.insert(allLatencies.end(), latencies.begin(), latencies.end());
        }
        EXPECT_EQ(allLatencies.size(), static_cast<size_t>(totalRequests));

        PureLogResult result;
        result.rate = 0;
        result.accessRate = 0;
        result.diagnosticRate = 0;
        result.directLog = false;
        result.expensivePayload = expensivePayload;
        result.samplerMode = "no-sampler";
        result.samplerOnly = false;
        result.totalRequests = totalRequests;
        result.elapsedSec = std::chrono::duration<double>(finish - scheduledStart).count();
        result.achievedQps = totalRequests / result.elapsedSec;
        result.p50Us = CalculatePercentile(allLatencies, 0.50);
        result.p95Us = CalculatePercentile(allLatencies, 0.95);
        result.p99Us = CalculatePercentile(allLatencies, 0.99);
        result.avgUs = CalculateAvgLatency(allLatencies);
        result.maxUs = *std::max_element(allLatencies.begin(), allLatencies.end());
        result.writtenSamples = CountInfoLogLinesWithMarker(marker);
        result.payloadBuilds = payloadBuilds.load(std::memory_order_relaxed);
        return result;
    }

    void PrintPureLogResult(const std::string &tag, const PureLogResult &result)
    {
        std::cout << tag << " samplerMode=" << result.samplerMode << " samplerOnly=" << result.samplerOnly
                  << " rate=" << result.rate << " accessRate=" << result.accessRate
                  << " diagnosticRate=" << result.diagnosticRate << " directLog=" << result.directLog
                  << " expensivePayload=" << result.expensivePayload << " threads=" << PURE_LOG_THREAD_COUNT
                  << " targetQps=" << PURE_LOG_TARGET_QPS << " durationSec=" << PURE_LOG_DURATION_SEC
                  << " totalRequests=" << result.totalRequests << " elapsedSec=" << std::fixed
                  << std::setprecision(OUTPUT_PRECISION) << result.elapsedSec << " achievedQps=" << result.achievedQps
                  << " avgUs=" << result.avgUs << " p50Us=" << result.p50Us << " p95Us=" << result.p95Us
                  << " p99Us=" << result.p99Us << " maxUs=" << result.maxUs
                  << " writtenSamples=" << result.writtenSamples << " payloadBuilds=" << result.payloadBuilds
                  << std::endl;
    }

private:
    std::string loggerName_;
};

/**
 * Single-threaded throughput test
 * Measures performance with different log message sizes (including prefix)
 * Reports: throughput (messages/sec), throughput (MB/sec), average latency and P99 latency
 */
TEST_F(LogPerformanceTest, SingleThreadThroughput)
{
    std::cout << "\n[Single-thread] Spdlog throughput test\n";

    StartLogging();

    // Test cases: {payloadLengthBytes, messageCount}
    // Actual log size will be payloadLength + LOG_PREFIX_ESTIMATED_SIZE
    std::vector<std::pair<size_t, size_t>> testCases = {
        { 32, 1000000 },  // Short logs: 32 bytes, 1,000,000 messages
        { 512, 200000 },  // Medium logs: 512 bytes, 200,000 messages
        { 4096, 50000 }   // Long logs: 4096 bytes, 50,000 messages
    };

    for (auto &[payloadLength, count] : testCases) {
        size_t totalLogSize = CalculateTotalLogSize(payloadLength);
        std::string logPayload = GenerateLogPayload(payloadLength);
        Timer totalTimer;               // Total time timer (for TPS calculation)
        std::vector<double> latencies;  // Stores latency of each log entry (microseconds)
        latencies.reserve(count);       // Pre-allocate memory to avoid dynamic allocation overhead

        // Warm-up phase to stabilize measurements (avoids initialization overhead)
        for (int i = 0; i < WARMUP_ITERATIONS; ++i) {
            LOG(INFO) << logPayload;
        }

        Provider::Instance().FlushLogs();  // Ensure warm-up logs are processed

        // Performance test: record latency for each log entry
        totalTimer.Start();
        for (size_t i = 0; i < count; ++i) {
            // Record start time of single log operation
            auto start = std::chrono::high_resolution_clock::now();
            // Execute log writing
            LOG(INFO) << logPayload;
            // Record end time of single log operation
            // (enqueue time for async mode, completion time for sync mode)
            auto end = std::chrono::high_resolution_clock::now();
            // Calculate and store latency in microseconds
            double latencyUs = std::chrono::duration<double, std::micro>(end - start).count();
            latencies.push_back(latencyUs);
        }

        Provider::Instance().FlushLogs();  // Wait for all async logs to be processed
        double totalElapsedUs = totalTimer.ElapsedUs();
        double totalElapsedSec = totalElapsedUs / MICROSEC_TO_SEC;

        // Calculate performance metrics
        double totalBytes = static_cast<double>(totalLogSize * count);
        double tps = count / totalElapsedSec;                          // Throughput (transactions per second)
        double mbps = (totalBytes / (MB_TO_BYTES)) / totalElapsedSec;  // Throughput (MB per second)
        double avgLatency = CalculateAvgLatency(latencies);            // Average latency (microseconds)
        double p99Latency = CalculateP99(latencies);                   // P99 latency (microseconds)

        std::cout << "  Payload length: " << payloadLength << "B, Total log size: ~" << totalLogSize << "B\n";
        std::cout << "  Total messages: " << count << ", Total data: " << std::fixed
                  << std::setprecision(OUTPUT_PRECISION) << (totalBytes / (MB_TO_BYTES)) << "MB\n";
        std::cout << "  Total duration: " << std::fixed << std::setprecision(OUTPUT_PRECISION)
                  << (totalElapsedUs / MICROSEC_TO_MILLISEC) << "ms\n";
        std::cout << "  Throughput: " << std::fixed << std::setprecision(OUTPUT_PRECISION) << tps << " msg/sec ("
                  << mbps << " MB/sec)\n";
        std::cout << "  Latency: Avg = " << std::fixed << std::setprecision(OUTPUT_PRECISION) << avgLatency << "μs, "
                  << "P99 = " << p99Latency << "μs\n\n";
    }
}

/**
 * Multi-threaded throughput test
 * Measures performance with different log message sizes (including prefix)
 * Reports: throughput (messages/sec), throughput (MB/sec), average latency and P99 latency
 */
TEST_F(LogPerformanceTest, MultiThreadThroughput)
{
    std::cout << "\n[Multi-thread] Spdlog throughput test\n";

    StartLogging();

    // Test cases: {payloadLengthBytes, totalMessageCount}
    // Actual log size will be payloadLength + LOG_PREFIX_ESTIMATED_SIZE
    std::vector<std::pair<size_t, size_t>> testCases = {
        { 32, 1000000 },  // Short logs: 32 bytes, 1,000,000 total messages
        { 512, 200000 },  // Medium logs: 512 bytes, 200,000 total messages
        { 4096, 50000 }   // Long logs: 4096 bytes, 50,000 total messages
    };

    const size_t threadCount = 10;
    std::cout << "[Multi-thread] Using " << threadCount << " concurrent threads\n";

    for (auto &[payloadLength, totalCount] : testCases) {
        size_t perThreadCount = totalCount / threadCount;
        size_t totalLogSize = CalculateTotalLogSize(payloadLength);
        std::string logPayload = GenerateLogPayload(payloadLength);
        Timer totalTimer;                  // Total time timer
        std::vector<double> allLatencies;  // Stores latency data from all threads
        std::mutex latencyMutex;           // Thread-safe protection for latency data

        std::vector<std::thread> threads;
        // Multi-thread warm-up phase
        for (size_t t = 0; t < threadCount; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < WARMUP_ITERATIONS; ++i) {
                    LOG(INFO) << logPayload;
                }
            });
        }

        for (auto &th : threads) {
            th.join();
        }

        threads.clear();
        Provider::Instance().FlushLogs();

        // Multi-thread concurrent writing test
        totalTimer.Start();
        for (size_t t = 0; t < threadCount; ++t) {
            threads.emplace_back([&, threadId = t]() {
                std::vector<double> threadLatencies;  // Thread-local latency storage (reduces lock contention)
                threadLatencies.reserve(perThreadCount);

                for (size_t i = 0; i < perThreadCount; ++i) {
                    auto start = std::chrono::high_resolution_clock::now();
                    LOG(INFO) << logPayload;
                    auto end = std::chrono::high_resolution_clock::now();
                    double latencyUs = std::chrono::duration<double, std::micro>(end - start).count();
                    threadLatencies.push_back(latencyUs);
                }

                // Merge thread-local data to global collection (protected by lock)
                std::lock_guard<std::mutex> lock(latencyMutex);
                allLatencies.insert(allLatencies.end(), threadLatencies.begin(), threadLatencies.end());
            });
        }

        // Wait for all threads to complete and flush logs
        for (auto &th : threads) {
            th.join();
        }

        Provider::Instance().FlushLogs();
        double totalElapsedUs = totalTimer.ElapsedUs();
        double totalElapsedSec = totalElapsedUs / MICROSEC_TO_SEC;

        // Calculate performance metrics
        double totalBytes = static_cast<double>(totalLogSize * totalCount);
        double tps = totalCount / totalElapsedSec;                     // Total throughput (messages per second)
        double mbps = (totalBytes / (MB_TO_BYTES)) / totalElapsedSec;  // Throughput (MB per second)
        double avgLatency = CalculateAvgLatency(allLatencies);         // Average latency
        double p99Latency = CalculateP99(allLatencies);                // P99 latency

        std::cout << "  Payload length: " << payloadLength << "B, Total log size: ~" << totalLogSize << "B\n";
        std::cout << "  Total messages: " << totalCount << " (per thread: " << perThreadCount << ")\n";
        std::cout << "  Total data: " << std::fixed << std::setprecision(OUTPUT_PRECISION)
                  << (totalBytes / (MB_TO_BYTES)) << "MB, Duration: " << std::fixed
                  << std::setprecision(OUTPUT_PRECISION) << (totalElapsedUs / MICROSEC_TO_MILLISEC) << "ms\n";
        std::cout << "  Throughput: " << std::fixed << std::setprecision(OUTPUT_PRECISION) << tps << " msg/sec ("
                  << mbps << " MB/sec)\n";
        std::cout << "  Latency: Avg = " << std::fixed << std::setprecision(OUTPUT_PRECISION) << avgLatency << "μs, "
                  << "P99 = " << p99Latency << "μs\n\n";
    }
}

TEST_F(LogPerformanceTest, LEVEL1_RequestLogSampleRateHalfPureLoggingQps4000)
{
    const std::string marker = "pure-log-sample-rate-qps4000-rate-" + std::to_string(PURE_LOG_SAMPLE_RATE);
    auto result = RunPureLogWorkload(PURE_LOG_SAMPLE_RATE, marker, false, false, "sampler-half");
    PrintPureLogResult("PURE_LOG_SAMPLE_RATE_RESULT", result);

    EXPECT_GE(result.achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_LT(result.p99Us, 50000.0);
}

TEST_F(LogPerformanceTest, LEVEL1_RequestLogSampleRateHalfRejectedExpensivePayloadQps4000)
{
    const std::string marker = "pure-log-sample-rate-expensive-rate-qps4000-" + std::to_string(PURE_LOG_SAMPLE_RATE);
    auto result = RunPureLogWorkload(PURE_LOG_SAMPLE_RATE, marker, true, false, "sampler-half");
    PrintPureLogResult("PURE_LOG_SAMPLE_RATE_EXPENSIVE_RESULT", result);

    EXPECT_GE(result.achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_LT(result.p99Us, 50000.0);
    EXPECT_GT(result.payloadBuilds, 0);
    EXPECT_LT(result.payloadBuilds, result.totalRequests);
}

TEST_F(LogPerformanceTest, LEVEL1_SamplerPassThroughMacroOverheadQps4000)
{
    auto noSampler = RunPureLogWorkloadNoSampler("pure-log-nosampler-baseline", true);
    auto samplerThrough = RunPureLogWorkload(1.0, "pure-log-sampler-through", true, false, "sampler-through");
    PrintPureLogResult("PURE_LOG_NOSAMPLER_BASELINE_RESULT", noSampler);
    PrintPureLogResult("PURE_LOG_SAMPLER_THROUGH_RESULT", samplerThrough);

    EXPECT_GE(noSampler.achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_GE(samplerThrough.achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_EQ(samplerThrough.payloadBuilds, samplerThrough.totalRequests);

    EXPECT_LE(samplerThrough.p99Us, noSampler.p99Us * 1.01);
    EXPECT_LE(samplerThrough.avgUs, noSampler.avgUs * 1.01);
}

TEST_F(LogPerformanceTest, LEVEL1_RequestRejectFastPathQps4000)
{
    auto result = RunPureLogWorkloadMultiRate(0.0, 1.0, 1.0,
                                               "pure-log-reject-fast-path-qps4000", true, false, "sampler-reject");
    PrintPureLogResult("PURE_LOG_REJECT_FAST_PATH_RESULT", result);

    EXPECT_GE(result.achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_EQ(result.payloadBuilds, 0);
    EXPECT_LT(result.p99Us, 50000.0);
}

TEST_F(LogPerformanceTest, LEVEL1_SamplerOnlyDecisionCostQps4000)
{
    StartLoggingWithSamplerConfig(0.5, 0.5, 1.0);
    constexpr int totalDecisions = PURE_LOG_TARGET_QPS * PURE_LOG_DURATION_SEC;
    std::atomic<int> ready{ 0 };
    std::atomic<bool> start{ false };
    std::vector<std::vector<double>> threadLatencies(PURE_LOG_THREAD_COUNT);

    std::vector<std::thread> threads;
    threads.reserve(PURE_LOG_THREAD_COUNT);
    for (int threadId = 0; threadId < PURE_LOG_THREAD_COUNT; ++threadId) {
        threads.emplace_back([&, threadId]() {
            threadLatencies[threadId].reserve(totalDecisions / PURE_LOG_THREAD_COUNT);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
            for (int i = 0; i < totalDecisions / PURE_LOG_THREAD_COUNT; ++i) {
                auto beg = std::chrono::high_resolution_clock::now();
                volatile bool result =
                    LogSampler::Instance().ShouldCreateRuntimeLog(LogSeverity::INFO, false);
                (void)result;
                auto end = std::chrono::high_resolution_clock::now();
                threadLatencies[threadId].push_back(
                    std::chrono::duration<double, std::micro>(end - beg).count());
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < PURE_LOG_THREAD_COUNT) {
        std::this_thread::yield();
    }
    auto globalStart = std::chrono::high_resolution_clock::now();
    start.store(true, std::memory_order_release);

    for (auto &t : threads) {
        t.join();
    }
    auto globalEnd = std::chrono::high_resolution_clock::now();
    double totalSec = std::chrono::duration<double>(globalEnd - globalStart).count();
    double achievedQps = totalDecisions / totalSec;

    std::vector<double> allLatencies;
    for (auto &v : threadLatencies) {
        allLatencies.insert(allLatencies.end(), v.begin(), v.end());
    }
    std::sort(allLatencies.begin(), allLatencies.end());

    double p50 = allLatencies[allLatencies.size() / 2];
    double p99 = allLatencies[allLatencies.size() * 99 / 100];

    std::cout << "SAMPLER_ONLY_QPS=" << std::fixed << std::setprecision(OUTPUT_PRECISION)
              << achievedQps << " p50=" << p50 << " p99=" << p99
              << " samplerMode=sampler-only samplerOnly=true" << std::endl;
    EXPECT_GE(achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_LT(p99, 10.0);
}

TEST_F(LogPerformanceTest, LEVEL1_NoSamplerBaselineQps4000)
{
    auto result = RunPureLogWorkloadNoSampler("pure-log-nosampler-baseline-qps4000", true);
    PrintPureLogResult("PURE_LOG_NOSAMPLER_BASELINE_QPS4000_RESULT", result);

    EXPECT_GE(result.achievedQps, PURE_LOG_TARGET_QPS * 0.95);
    EXPECT_EQ(result.payloadBuilds, result.totalRequests);
    EXPECT_LT(result.p99Us, 50000.0);
}

}  // namespace ut
}  // namespace datasystem
