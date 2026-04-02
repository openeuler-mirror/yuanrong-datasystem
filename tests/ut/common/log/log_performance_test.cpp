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
#include <chrono>
#include <dirent.h>
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
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/logging.h"

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_bool(log_async);
DS_DECLARE_uint32(max_log_size);

namespace datasystem {
namespace ut {
// Estimated size of spdlog's default log prefix (timestamp + level + thread ID + delimiters)
// Example: "2025-08-06T02:57:13.517005 | I | log_performance_test.cpp:171 |  |   | 52385:52385 |  |  |  "
constexpr size_t LOG_PREFIX_ESTIMATED_SIZE = 85;  // Bytes
constexpr double MICROSEC_TO_MILLISEC = 1e3;
constexpr double MICROSEC_TO_SEC = 1e6;
constexpr int WARMUP_ITERATIONS = 1000;
constexpr int OUTPUT_PRECISION = 2;

/**
 * Test log asynchronous mode performance testing
 * Sets up common test environment and utility functions
 */
class LogPerformanceTest : public CommonTest {
public:
    void StartLogging()
    {
        FLAGS_alsologtostderr = false;
        FLAGS_max_log_size = 2048;  // 2048 MB
        FLAGS_log_async = true;

        Logging::GetInstance()->Start("ds_llt", true, 1);
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

}  // namespace ut
}  // namespace datasystem
