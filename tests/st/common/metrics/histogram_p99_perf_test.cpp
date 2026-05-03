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
 * Histogram P99 performance / concurrency smoke (ST, gtest).
 * Log dirs match ST CommonTest (cluster/common.cpp) without pulling //tests/st:st_common.
 *
 * bazel test //tests/st/common/metrics:histogram_p99_perf_test --define=enable_urma=false
 */

#include "datasystem/common/metrics/metrics.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"

// CMake: target_compile_definitions(_ds_st_other_obj … LLT_BIN_PATH=…).
// Bazel: copts on //tests/st/common/metrics:histogram_p99_perf_test.
#ifndef LLT_BIN_PATH
#define LLT_BIN_PATH "/tmp/ds_llt"
#endif

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_int32(log_monitor_interval_ms);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
namespace {

constexpr int kClearDirRetryMs = 500;

void GetCurTestName(std::string &caseName, std::string &name)
{
    const ::testing::TestInfo *curTest = ::testing::UnitTest::GetInstance()->current_test_info();
    caseName = std::string(curTest->test_case_name());
    name = std::string(curTest->name());
}

void ClearPerfTestCaseDir(const std::string &path)
{
    auto rc = datasystem::RemoveAll(path);
    if (rc.IsError()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kClearDirRetryMs));
        (void)datasystem::RemoveAll(path);
    }
}

constexpr uint16_t HIST_ID = 0;
const datasystem::metrics::MetricDesc DESCS[] = {
    { HIST_ID, "perf_histogram", datasystem::metrics::MetricType::HISTOGRAM, "us" },
};

std::string ExtractField(const std::string &json, const char *field)
{
    std::string key = "\"" + std::string(field) + "\":";
    size_t pos = json.find(key);
    if (pos == std::string::npos) {
        return "0";
    }
    std::string val = json.substr(pos + key.size());
    size_t end = val.find_first_of(",}");
    if (end == std::string::npos) {
        return "0";
    }
    return val.substr(0, end);
}

// Full metrics_summary JSON + readable histogram total (first total:{...} matches ExtractField order).
void LogPerfSummary(const char *caseTag, const std::string &summary)
{
    LOG(INFO) << "[metrics_summary] " << caseTag << " " << summary;
    LOG(INFO) << "[perf_histogram total] " << caseTag << " count=" << ExtractField(summary, "count")
              << " avg_us=" << ExtractField(summary, "avg_us") << " max_us=" << ExtractField(summary, "max_us")
              << " p50=" << ExtractField(summary, "p50") << " p90=" << ExtractField(summary, "p90")
              << " p99=" << ExtractField(summary, "p99");
}

void InitPerfHistogram()
{
    datasystem::metrics::ResetForTest();
    ASSERT_TRUE(datasystem::metrics::Init(DESCS, sizeof(DESCS) / sizeof(DESCS[0])).IsOk());
}

}  // namespace

class HistogramP99PerfTest : public ::testing::Test {
protected:
    std::string testCasePath_;

    HistogramP99PerfTest()
    {
        FLAGS_alsologtostderr = true;
        std::string caseName;
        std::string name;
        GetCurTestName(caseName, name);
        testCasePath_ = std::string(LLT_BIN_PATH) + "/ds/" + caseName + "." + name;
        FLAGS_log_dir = testCasePath_ + "/client";
        ClearPerfTestCaseDir(testCasePath_);
        (void)datasystem::CreateDir(FLAGS_log_dir, true);
    }

    void SetUp() override
    {
        FLAGS_log_monitor = false;
    }

    void TearDown() override
    {
        datasystem::metrics::ResetForTest();
        FLAGS_log_monitor = true;
        FLAGS_log_monitor_interval_ms = 10000;
    }
};

TEST_F(HistogramP99PerfTest, FixedValue100usLockContention)
{
    const int threads = 16;
    const int loopsPerThread = 1000000;
    const uint64_t fixedValue = 100;
    InitPerfHistogram();

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&] {
            for (int i = 0; i < loopsPerThread; ++i) {
                datasystem::metrics::GetHistogram(HIST_ID).Observe(fixedValue);
            }
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    auto totalOps = static_cast<int64_t>(threads) * loopsPerThread;
    auto nsPerOp = (duration * 1000) / totalOps;

    LOG(INFO) << "[Fixed 100us] threads=" << threads << " ops=" << totalOps << " duration_us=" << duration
              << " throughput=" << (totalOps * 1000000LL / duration) << "/s ns/op=" << nsPerOp;

    auto summary = datasystem::metrics::DumpSummaryForTest();
    LogPerfSummary("FixedValue100usLockContention", summary);
    auto count = std::stoull(ExtractField(summary, "count"));
    auto p99 = std::stoull(ExtractField(summary, "p99"));
    EXPECT_EQ(count, static_cast<uint64_t>(totalOps));
    EXPECT_EQ(p99, fixedValue);
}

TEST_F(HistogramP99PerfTest, RandomValuesFourBuckets)
{
    const int threads = 16;
    const int loopsPerThread = 1000000;
    InitPerfHistogram();

    std::atomic<int> counter{ 0 };
    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&] {
            uint64_t values[] = { 10, 100, 1000, 10000 };
            for (int i = 0; i < loopsPerThread; ++i) {
                datasystem::metrics::GetHistogram(HIST_ID).Observe(values[counter.fetch_add(1) % 4]);
            }
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    auto totalOps = static_cast<int64_t>(threads) * loopsPerThread;

    LOG(INFO) << "[Random 4 buckets] ops=" << totalOps << " duration_us=" << duration;

    auto summary = datasystem::metrics::DumpSummaryForTest();
    LogPerfSummary("RandomValuesFourBuckets", summary);
    auto p99 = std::stoull(ExtractField(summary, "p99"));
    auto maxUs = std::stoull(ExtractField(summary, "max_us"));
    EXPECT_EQ(maxUs, 10000u);
    EXPECT_EQ(p99, maxUs);
}

TEST_F(HistogramP99PerfTest, MultiThreaded99x10Plus1x10000)
{
    InitPerfHistogram();

    std::thread t1([&] {
        for (int i = 0; i < 99; ++i) {
            datasystem::metrics::GetHistogram(HIST_ID).Observe(10);
        }
    });
    std::thread t2([&] {
        datasystem::metrics::GetHistogram(HIST_ID).Observe(10000);
    });
    t1.join();
    t2.join();

    auto summary = datasystem::metrics::DumpSummaryForTest();
    LogPerfSummary("MultiThreaded99x10Plus1x10000", summary);
    auto count = std::stoull(ExtractField(summary, "count"));
    auto p99 = std::stoull(ExtractField(summary, "p99"));
    LOG(INFO) << "[99x10+1x10000] count=" << count << " p99=" << p99 << " (expect 100, 20)";
    EXPECT_EQ(count, 100u);
    EXPECT_EQ(p99, 20u);
}

TEST_F(HistogramP99PerfTest, ConcurrentObserveAndDump)
{
    const int threads = 16;
    InitPerfHistogram();

    std::atomic<bool> stop{ false };
    std::atomic<uint64_t> totalObserved{ 0 };

    std::vector<std::thread> writers;
    for (int t = 0; t < threads; ++t) {
        writers.emplace_back([&]() {
            while (!stop.load()) {
                datasystem::metrics::GetHistogram(HIST_ID).Observe(100);
                totalObserved.fetch_add(1);
            }
        });
    }

    std::atomic<uint64_t> dumpCount{ 0 };
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&]() {
            while (!stop.load()) {
                (void)datasystem::metrics::DumpSummaryForTest();
                dumpCount.fetch_add(1);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true);
    for (auto &w : writers) {
        w.join();
    }
    for (auto &r : readers) {
        r.join();
    }

    auto summary = datasystem::metrics::DumpSummaryForTest();
    LogPerfSummary("ConcurrentObserveAndDump", summary);
    auto count = std::stoull(ExtractField(summary, "count"));
    LOG(INFO) << "[Race] observed=" << totalObserved.load() << " dumps=" << dumpCount.load()
              << " summary_count=" << count;
    EXPECT_LE(count, totalObserved.load());
}

}  // namespace st
}  // namespace datasystem
