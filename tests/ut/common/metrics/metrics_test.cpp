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

#include "datasystem/common/metrics/metrics.h"

#include <atomic>
#include <thread>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "gtest/gtest.h"
#include "ut/common.h"

DS_DECLARE_bool(log_monitor);
DS_DECLARE_int32(log_monitor_interval_ms);

namespace datasystem {
namespace ut {
namespace {
enum MetricId : uint16_t { COUNTER_ID = 0, GAUGE_ID = 1, HISTOGRAM_ID = 2 };
const metrics::MetricDesc DESCS[] = {
    { COUNTER_ID, "test_counter", metrics::MetricType::COUNTER, "count" },
    { GAUGE_ID, "test_gauge", metrics::MetricType::GAUGE, "count" },
    { HISTOGRAM_ID, "test_histogram", metrics::MetricType::HISTOGRAM, "us" },
};

void InitMetrics()
{
    metrics::ResetForTest();
    DS_ASSERT_OK(metrics::Init(DESCS, sizeof(DESCS) / sizeof(DESCS[0])));
}

class MetricsTest : public CommonTest {
public:
    void TearDown() override
    {
        metrics::ResetForTest();
        FLAGS_log_monitor = true;
        FLAGS_log_monitor_interval_ms = 10000;
    }
};
}  // namespace

TEST_F(MetricsTest, counter_inc_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc();
    metrics::GetCounter(COUNTER_ID).Inc(7);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=8"), std::string::npos);
    EXPECT_NE(summary.find("test_counter=+8"), std::string::npos);
}

TEST_F(MetricsTest, gauge_set_inc_dec_test)
{
    InitMetrics();
    auto gauge = metrics::GetGauge(GAUGE_ID);
    gauge.Set(10);
    gauge.Inc(5);
    gauge.Dec(3);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_gauge=12"), std::string::npos);
    EXPECT_NE(summary.find("test_gauge=+12"), std::string::npos);
}

TEST_F(MetricsTest, histogram_observe_test)
{
    InitMetrics();
    auto hist = metrics::GetHistogram(HISTOGRAM_ID);
    hist.Observe(10);
    hist.Observe(30);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_histogram,count=2,avg=20us,max=30us"), std::string::npos);
    EXPECT_NE(summary.find("test_histogram,count=+2,avg=20us,max=30us"), std::string::npos);
}

TEST_F(MetricsTest, histogram_empty_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_histogram,count=0,avg=0us,max=0us"), std::string::npos);
    EXPECT_NE(summary.find("test_histogram,count=+0,avg=0us,max=0us"), std::string::npos);
}

TEST_F(MetricsTest, scoped_timer_test)
{
    InitMetrics();
    { metrics::ScopedTimer timer(HISTOGRAM_ID); }
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_histogram,count=1,"), std::string::npos);
    EXPECT_NE(summary.find("test_histogram,count=+1,"), std::string::npos);
}

TEST_F(MetricsTest, invalid_metric_id_test)
{
    InitMetrics();
    metrics::GetCounter(100).Inc();
    metrics::GetGauge(100).Set(10);
    metrics::GetHistogram(100).Observe(10);
    EXPECT_EQ(metrics::Init(nullptr, 0).GetCode(), StatusCode::K_OK);
    EXPECT_EQ(metrics::Init(nullptr, 1).GetCode(), StatusCode::K_INVALID);
}

TEST_F(MetricsTest, counter_concurrent_inc_test)
{
    InitMetrics();
    const int threads = 64;
    const int loops = 1000;
    std::vector<std::thread> workers;
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < loops; ++j) {
                metrics::GetCounter(COUNTER_ID).Inc();
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=64000"), std::string::npos);
}

TEST_F(MetricsTest, histogram_concurrent_observe_test)
{
    InitMetrics();
    const int threads = 64;
    const int loops = 1000;
    std::vector<std::thread> workers;
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < loops; ++j) {
                metrics::GetHistogram(HISTOGRAM_ID).Observe(10);
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_histogram,count=64000,avg=10us,max=10us"), std::string::npos);
}

TEST_F(MetricsTest, writer_summary_format_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("Metrics Summary, version=v0"), 0ul);
    EXPECT_NE(summary.find("Total:\n"), std::string::npos);
    EXPECT_NE(summary.find("Compare with 10000ms before:\n"), std::string::npos);
    EXPECT_EQ(summary.find('|'), std::string::npos);
    EXPECT_EQ(summary.find("type="), std::string::npos);
    EXPECT_EQ(summary.find("unit="), std::string::npos);
}

TEST_F(MetricsTest, writer_no_pipe_test)
{
    InitMetrics();
    EXPECT_EQ(metrics::DumpSummaryForTest().find('|'), std::string::npos);
}

TEST_F(MetricsTest, writer_no_type_unit_field_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("type="), std::string::npos);
    EXPECT_EQ(summary.find("unit="), std::string::npos);
}

TEST_F(MetricsTest, writer_header_once_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("cycle=1"), summary.rfind("cycle=1"));
    EXPECT_EQ(summary.find("interval=10000ms"), summary.rfind("interval=10000ms"));
}

TEST_F(MetricsTest, writer_total_and_delta_section_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_LT(summary.find("Total:\n"), summary.find("Compare with 10000ms before:\n"));
}

TEST_F(MetricsTest, writer_counter_delta_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    (void)metrics::DumpSummaryForTest();
    metrics::GetCounter(COUNTER_ID).Inc(3);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=8"), std::string::npos);
    EXPECT_NE(summary.find("test_counter=+3"), std::string::npos);
}

TEST_F(MetricsTest, writer_gauge_delta_test)
{
    InitMetrics();
    metrics::GetGauge(GAUGE_ID).Set(10);
    (void)metrics::DumpSummaryForTest();
    metrics::GetGauge(GAUGE_ID).Set(6);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_gauge=6"), std::string::npos);
    EXPECT_NE(summary.find("test_gauge=-4"), std::string::npos);
}

TEST_F(MetricsTest, writer_histogram_delta_test)
{
    InitMetrics();
    metrics::GetHistogram(HISTOGRAM_ID).Observe(10);
    (void)metrics::DumpSummaryForTest();
    metrics::GetHistogram(HISTOGRAM_ID).Observe(30);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_histogram,count=2,avg=20us,max=30us"), std::string::npos);
    EXPECT_NE(summary.find("test_histogram,count=+1,avg=30us,max=30us"), std::string::npos);
}

TEST_F(MetricsTest, writer_zero_delta_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    (void)metrics::DumpSummaryForTest();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=5"), std::string::npos);
    EXPECT_NE(summary.find("test_counter=+0"), std::string::npos);
    EXPECT_NE(summary.find("test_histogram,count=+0,avg=0us,max=0us"), std::string::npos);
}

TEST_F(MetricsTest, writer_tick_updates_delta_snapshot_test)
{
    InitMetrics();
    FLAGS_log_monitor_interval_ms = 0;
    metrics::GetCounter(COUNTER_ID).Inc(5);
    metrics::Tick();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=5"), std::string::npos);
    EXPECT_NE(summary.find("test_counter=+0"), std::string::npos);
}

TEST_F(MetricsTest, writer_print_summary_updates_delta_snapshot_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    metrics::PrintSummary();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=5"), std::string::npos);
    EXPECT_NE(summary.find("test_counter=+0"), std::string::npos);
}

TEST_F(MetricsTest, writer_disabled_test)
{
    InitMetrics();
    FLAGS_log_monitor = false;
    FLAGS_log_monitor_interval_ms = 0;
    metrics::GetCounter(COUNTER_ID).Inc(5);
    metrics::Tick();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("test_counter=5"), std::string::npos);
    EXPECT_NE(summary.find("test_counter=+5"), std::string::npos);
}

TEST_F(MetricsTest, writer_update_race_test)
{
    InitMetrics();
    std::atomic<bool> exit{ false };
    std::thread worker([&] {
        while (!exit.load()) {
            metrics::GetCounter(COUNTER_ID).Inc();
            metrics::GetHistogram(HISTOGRAM_ID).Observe(1);
        }
    });
    for (int i = 0; i < 20; ++i) {
        (void)metrics::DumpSummaryForTest();
    }
    exit = true;
    worker.join();
}

TEST_F(MetricsTest, writer_tick_race_test)
{
    InitMetrics();
    FLAGS_log_monitor_interval_ms = 0;
    std::thread worker([] {
        for (int i = 0; i < 1000; ++i) {
            metrics::GetCounter(COUNTER_ID).Inc();
        }
    });
    metrics::Tick();
    worker.join();
    EXPECT_NE(metrics::DumpSummaryForTest().find("test_counter="), std::string::npos);
}
}  // namespace ut
}  // namespace datasystem
