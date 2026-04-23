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
#include "datasystem/common/metrics/kv_metrics.h"

#include <algorithm>
#include <atomic>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
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

void InitKvMetricsForTest()
{
    metrics::ResetKvMetricsForTest();
    DS_ASSERT_OK(metrics::InitKvMetrics());
}

std::string ScalarMetricJson(const std::string &name, int64_t total, int64_t delta)
{
    std::ostringstream os;
    os << "{\"name\":\"" << name << "\",\"total\":" << total << ",\"delta\":" << delta << '}';
    return os.str();
}

std::string HistogramMetricJson(const std::string &name, uint64_t totalCount, uint64_t totalAvg, uint64_t totalMax,
                                uint64_t deltaCount, uint64_t deltaAvg, uint64_t deltaMax)
{
    std::ostringstream os;
    os << "{\"name\":\"" << name << "\",\"total\":{\"count\":" << totalCount << ",\"avg_us\":" << totalAvg
       << ",\"max_us\":" << totalMax << "},\"delta\":{\"count\":" << deltaCount << ",\"avg_us\":" << deltaAvg
       << ",\"max_us\":" << deltaMax << "}}";
    return os.str();
}

class MetricsTest : public CommonTest {
public:
    void TearDown() override
    {
        metrics::ResetKvMetricsForTest();
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
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 8, 8)), std::string::npos);
}

TEST_F(MetricsTest, gauge_set_inc_dec_test)
{
    InitMetrics();
    auto gauge = metrics::GetGauge(GAUGE_ID);
    gauge.Set(10);
    gauge.Inc(5);
    gauge.Dec(3);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_gauge", 12, 12)), std::string::npos);
}

TEST_F(MetricsTest, histogram_observe_test)
{
    InitMetrics();
    auto hist = metrics::GetHistogram(HISTOGRAM_ID);
    hist.Observe(10);
    hist.Observe(30);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 2, 20, 30, 2, 20, 30)), std::string::npos);
}

TEST_F(MetricsTest, histogram_empty_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("test_histogram"), std::string::npos);
}

TEST_F(MetricsTest, scoped_timer_test)
{
    InitMetrics();
    metrics::GetHistogram(HISTOGRAM_ID).Observe(1);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("\"name\":\"test_histogram\""), std::string::npos);
    InitMetrics();
    { metrics::ScopedTimer timer(HISTOGRAM_ID); }
    EXPECT_NE(summary.find("\"count\":1"), std::string::npos);
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
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 64000, 64000)), std::string::npos);
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
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 64000, 10, 10, 64000, 10, 10)), std::string::npos);
}

TEST_F(MetricsTest, writer_summary_format_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("{\"event\":\"metrics_summary\""), 0ul);
    EXPECT_EQ(summary.find('\n'), std::string::npos);
    EXPECT_NE(summary.find("\"metrics\":["), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 5, 5)), std::string::npos);
    EXPECT_EQ(summary.find("\"trace_id\":"), std::string::npos);
    EXPECT_NE(summary.find("\"part_index\":1"), std::string::npos);
    EXPECT_NE(summary.find("\"part_count\":1"), std::string::npos);
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

TEST_F(MetricsTest, print_summary_uses_metrics_trace_id_test)
{
    InitMetrics();
    testing::internal::CaptureStderr();
    metrics::PrintSummary();
    auto output = testing::internal::GetCapturedStderr();
    auto traceId = (Logging::PodName() + "-metrics").substr(0, Trace::TRACEID_MAX_SIZE);
    EXPECT_NE(output.find(" | " + traceId + " | "), std::string::npos);
    EXPECT_NE(output.find("{\"event\":\"metrics_summary\""), std::string::npos);
    EXPECT_EQ(output.find("\"trace_id\":"), std::string::npos);
}

TEST_F(MetricsTest, print_summary_splits_large_payload_test)
{
    metrics::ResetForTest();
    std::vector<std::string> names;
    std::vector<metrics::MetricDesc> descs;
    for (uint16_t i = 0; i < 80; ++i) {
        names.emplace_back("metric_" + std::to_string(i) + "_" + std::string(320, 'x'));
        descs.push_back({ i, names.back().c_str(), metrics::MetricType::COUNTER, "count" });
    }
    DS_ASSERT_OK(metrics::Init(descs.data(), descs.size()));
    for (uint16_t i = 0; i < 80; ++i) {
        METRIC_ADD(i, 6);
    }
    testing::internal::CaptureStderr();
    metrics::PrintSummary();
    auto output = testing::internal::GetCapturedStderr();
    EXPECT_GT(std::count(output.begin(), output.end(), '\n'), 1);
    EXPECT_NE(output.find("\"part_index\":1"), std::string::npos);
    EXPECT_NE(output.find("\"part_index\":2"), std::string::npos);
}

TEST_F(MetricsTest, writer_header_once_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("\"cycle\":1"), summary.rfind("\"cycle\":1"));
    EXPECT_EQ(summary.find("\"interval_ms\":10000"), summary.rfind("\"interval_ms\":10000"));
}

TEST_F(MetricsTest, writer_total_and_delta_section_test)
{
    InitMetrics();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("\"metrics\":"), std::string::npos);
}

TEST_F(MetricsTest, writer_counter_delta_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    (void)metrics::DumpSummaryForTest();
    metrics::GetCounter(COUNTER_ID).Inc(3);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 8, 3)), std::string::npos);
}

TEST_F(MetricsTest, writer_gauge_delta_test)
{
    InitMetrics();
    metrics::GetGauge(GAUGE_ID).Set(10);
    (void)metrics::DumpSummaryForTest();
    metrics::GetGauge(GAUGE_ID).Set(6);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_gauge", 6, -4)), std::string::npos);
}

TEST_F(MetricsTest, writer_histogram_delta_test)
{
    InitMetrics();
    metrics::GetHistogram(HISTOGRAM_ID).Observe(10);
    (void)metrics::DumpSummaryForTest();
    metrics::GetHistogram(HISTOGRAM_ID).Observe(30);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 2, 20, 30, 1, 30, 30)), std::string::npos);
}

TEST_F(MetricsTest, writer_zero_delta_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    (void)metrics::DumpSummaryForTest();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 5, 0)), std::string::npos);
    EXPECT_EQ(summary.find("test_histogram"), std::string::npos);
}

TEST_F(MetricsTest, writer_tick_updates_delta_snapshot_test)
{
    InitMetrics();
    FLAGS_log_monitor_interval_ms = 0;
    metrics::GetCounter(COUNTER_ID).Inc(5);
    metrics::Tick();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 5, 0)), std::string::npos);
}

TEST_F(MetricsTest, writer_print_summary_updates_delta_snapshot_test)
{
    InitMetrics();
    metrics::GetCounter(COUNTER_ID).Inc(5);
    metrics::PrintSummary();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 5, 0)), std::string::npos);
}

TEST_F(MetricsTest, writer_disabled_test)
{
    InitMetrics();
    FLAGS_log_monitor = false;
    FLAGS_log_monitor_interval_ms = 0;
    metrics::GetCounter(COUNTER_ID).Inc(5);
    metrics::Tick();
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("test_counter", 5, 5)), std::string::npos);
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
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"test_counter\""), std::string::npos);
}

TEST_F(MetricsTest, kv_metric_desc_test)
{
    size_t count = 0;
    auto descs = metrics::GetKvMetricDescs(count);
    ASSERT_NE(descs, nullptr);
    ASSERT_EQ(count, static_cast<size_t>(metrics::KvMetricId::KV_METRIC_END));
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(descs[i].id, i);
        EXPECT_NE(descs[i].name, nullptr);
        EXPECT_NE(descs[i].unit, nullptr);
    }
}

TEST_F(MetricsTest, kv_metric_name_unique_test)
{
    size_t count = 0;
    auto descs = metrics::GetKvMetricDescs(count);
    std::set<std::string> names;
    for (size_t i = 0; i < count; ++i) {
        EXPECT_TRUE(names.emplace(descs[i].name).second);
    }
}

TEST_F(MetricsTest, kv_metric_id_mapping_test)
{
    DS_ASSERT_OK(metrics::InitKvMetrics());
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_ALLOCATED_MEMORY_SIZE))
        .Set(10);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_request_total", 1, 1)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("worker_allocated_memory_size", 10, 10)), std::string::npos);
}

TEST_F(MetricsTest, kv_metric_helper_inc_test)
{
    DS_ASSERT_OK(metrics::InitKvMetrics());
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL, 6);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_request_total", 7, 7)), std::string::npos);
}

TEST_F(MetricsTest, kv_metric_helper_timer_test)
{
    DS_ASSERT_OK(metrics::InitKvMetrics());
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::CLIENT_RPC_GET_LATENCY)).Observe(10);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find("\"name\":\"client_rpc_get_latency\""), std::string::npos);
    EXPECT_NE(summary.find("\"count\":1"), std::string::npos);
}

TEST_F(MetricsTest, kv_metric_error_if_test)
{
    DS_ASSERT_OK(metrics::InitKvMetrics());
    METRIC_ERROR_IF(false, metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    METRIC_ERROR_IF(true, metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_error_total", 1, 1)), std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_init_idempotent_test)
{
    DS_ASSERT_OK(metrics::InitKvMetrics());
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    DS_ASSERT_OK(metrics::InitKvMetrics());
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_request_total", 1, 1)), std::string::npos);
}

TEST_F(MetricsTest, client_put_metrics_counter_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 1, 1)), std::string::npos);
}

TEST_F(MetricsTest, client_put_metrics_error_counter_test)
{
    InitKvMetricsForTest();
    METRIC_ERROR_IF(true, metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_error_total", 1, 1)), std::string::npos);
}

TEST_F(MetricsTest, client_get_metrics_counter_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_get_request_total", 1, 1)), std::string::npos);
}

TEST_F(MetricsTest, client_get_metrics_error_counter_test)
{
    InitKvMetricsForTest();
    METRIC_ERROR_IF(true, metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_get_error_total", 1, 1)), std::string::npos);
}

TEST_F(MetricsTest, client_rpc_create_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::CLIENT_RPC_CREATE_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"client_rpc_create_latency\""), std::string::npos);
}

TEST_F(MetricsTest, client_rpc_publish_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::CLIENT_RPC_PUBLISH_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"client_rpc_publish_latency\""), std::string::npos);
}

TEST_F(MetricsTest, client_rpc_get_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::CLIENT_RPC_GET_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"client_rpc_get_latency\""), std::string::npos);
}

TEST_F(MetricsTest, worker_process_create_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_PROCESS_CREATE_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_process_create_latency\""), std::string::npos);
}

TEST_F(MetricsTest, worker_process_publish_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_PROCESS_PUBLISH_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_process_publish_latency\""), std::string::npos);
}

TEST_F(MetricsTest, worker_process_get_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_PROCESS_GET_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_process_get_latency\""), std::string::npos);
}

TEST_F(MetricsTest, worker_create_meta_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_CREATE_META_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_rpc_create_meta_latency\""), std::string::npos);
}

TEST_F(MetricsTest, worker_query_meta_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_QUERY_META_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_rpc_query_meta_latency\""), std::string::npos);
}

TEST_F(MetricsTest, worker_remote_get_metrics_latency_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_GET_REMOTE_OBJECT_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_rpc_get_remote_object_latency\""), std::string::npos);
}

TEST_F(MetricsTest, transport_bytes_test)
{
    InitKvMetricsForTest();
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_URMA_WRITE_TOTAL_BYTES, 11);
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_TCP_WRITE_TOTAL_BYTES, 13);
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_URMA_READ_TOTAL_BYTES, 17);
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_TCP_READ_TOTAL_BYTES, 19);
    METRIC_ADD(metrics::KvMetricId::WORKER_TO_CLIENT_TOTAL_BYTES, 23);
    METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_TOTAL_BYTES, 29);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_urma_write_total_bytes", 11, 11)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_tcp_write_total_bytes", 13, 13)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_urma_read_total_bytes", 17, 17)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_tcp_read_total_bytes", 19, 19)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("worker_to_client_total_bytes", 23, 23)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("worker_from_client_total_bytes", 29, 29)), std::string::npos);
}

TEST_F(MetricsTest, no_sensitive_data_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("token"), std::string::npos);
    EXPECT_EQ(summary.find("secret"), std::string::npos);
    EXPECT_EQ(summary.find("access_key"), std::string::npos);
    EXPECT_EQ(summary.find("object_key"), std::string::npos);
}

TEST_F(MetricsTest, multi_client_put_metrics_test)
{
    InitKvMetricsForTest();
    const int threads = 8;
    const int loops = 1000;
    std::vector<std::thread> workers;
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < loops; ++j) {
                METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 8000, 8000)),
              std::string::npos);
}

TEST_F(MetricsTest, multi_client_get_metrics_test)
{
    InitKvMetricsForTest();
    const int threads = 8;
    const int loops = 1000;
    std::vector<std::thread> workers;
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < loops; ++j) {
                METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_get_request_total", 8000, 8000)),
              std::string::npos);
}

TEST_F(MetricsTest, mixed_put_get_metrics_test)
{
    InitKvMetricsForTest();
    for (int i = 0; i < 100; ++i) {
        METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
        METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    }
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_request_total", 100, 100)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_request_total", 100, 100)), std::string::npos);
}

TEST_F(MetricsTest, business_writer_race_test)
{
    InitKvMetricsForTest();
    std::atomic<bool> exit{ false };
    std::thread worker([&] {
        while (!exit.load()) {
            METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
            METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
            metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::CLIENT_RPC_GET_LATENCY)).Observe(1);
        }
    });
    for (int i = 0; i < 20; ++i) {
        (void)metrics::DumpSummaryForTest();
    }
    exit = true;
    worker.join();
}

TEST_F(MetricsTest, kv_metrics_mset_success_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 1, 1)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_mget_success_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_get_request_total", 1, 1)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_mset_error_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_ERROR_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_error_total", 1, 1)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_mget_error_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_GET_ERROR_TOTAL);
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_get_error_total", 1, 1)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_create_meta_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_CREATE_META_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find(HistogramMetricJson("worker_rpc_create_meta_latency", 1, 10, 10, 1,
                                                                     10, 10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_query_meta_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_QUERY_META_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find(HistogramMetricJson("worker_rpc_query_meta_latency", 1, 10, 10, 1, 10,
                                                                     10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_remote_get_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_GET_REMOTE_OBJECT_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find(HistogramMetricJson("worker_rpc_get_remote_object_latency", 1, 10, 10,
                                                                     1, 10, 10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_urma_path_test)
{
    InitKvMetricsForTest();
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_URMA_READ_TOTAL_BYTES, 32);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_URMA_WRITE_LATENCY)).Observe(10);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_urma_read_total_bytes", 32, 32)), std::string::npos);
    EXPECT_NE(summary.find(HistogramMetricJson("worker_urma_write_latency", 1, 10, 10, 1, 10, 10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_tcp_fallback_test)
{
    InitKvMetricsForTest();
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_TCP_READ_TOTAL_BYTES, 64);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_TCP_WRITE_LATENCY)).Observe(10);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_tcp_read_total_bytes", 64, 64)), std::string::npos);
    EXPECT_NE(summary.find(HistogramMetricJson("worker_tcp_write_latency", 1, 10, 10, 1, 10, 10)), std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_summary_format_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_EQ(summary.find("{\"event\":\"metrics_summary\""), 0ul);
    EXPECT_EQ(summary.find('\n'), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_request_total", 1, 1)), std::string::npos);
    EXPECT_EQ(summary.find("\"trace_id\":"), std::string::npos);
    EXPECT_EQ(summary.find('|'), std::string::npos);
    EXPECT_EQ(summary.find("type="), std::string::npos);
    EXPECT_EQ(summary.find("unit="), std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_disabled_test)
{
    InitKvMetricsForTest();
    FLAGS_log_monitor = false;
    FLAGS_log_monitor_interval_ms = 0;
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    metrics::Tick();
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 1, 1)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_long_running_test)
{
    InitKvMetricsForTest();
    for (int i = 0; i < 100; ++i) {
        METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
        (void)metrics::DumpSummaryForTest();
    }
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 100, 0)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_high_qps_test)
{
    InitKvMetricsForTest();
    const int threads = 16;
    const int loops = 1000;
    std::vector<std::thread> workers;
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < loops; ++j) {
                METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
                METRIC_INC(metrics::KvMetricId::CLIENT_GET_REQUEST_TOTAL);
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_put_request_total", 16000, 16000)), std::string::npos);
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_request_total", 16000, 16000)), std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_print_summary_test)
{
    InitKvMetricsForTest();
    METRIC_INC(metrics::KvMetricId::CLIENT_PUT_REQUEST_TOTAL);
    metrics::PrintSummary();
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 1, 0)),
              std::string::npos);
}
}  // namespace ut
}  // namespace datasystem
