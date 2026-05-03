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
#include <cmath>
#include <random>
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

// Rank formula matches PercentileFromBuckets: target = (n * p + 99) / 100 (ceil-style), then take
// the target-th smallest value (1-based rank into ascending order).
uint64_t GroundTruthPercentile(const std::vector<uint64_t> &samples, uint32_t percentile)
{
    if (samples.empty()) {
        return 0;
    }
    std::vector<uint64_t> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    const uint64_t n = static_cast<uint64_t>(sorted.size());
    uint64_t target = (n * static_cast<uint64_t>(percentile) + 99U) / 100U;
    if (target == 0) {
        target = 1;
    }
    if (target > n) {
        target = n;
    }
    return sorted[static_cast<size_t>(target - 1U)];
}

metrics::HistBuckets BuildBucketsFromSamples(const std::vector<uint64_t> &samples)
{
    metrics::HistBuckets buckets{};
    for (uint64_t v : samples) {
        buckets[metrics::BucketIndex(v)]++;
    }
    return buckets;
}

uint64_t HistogramPercentileFromSamples(const std::vector<uint64_t> &samples, uint32_t percentile)
{
    const auto buckets = BuildBucketsFromSamples(samples);
    const uint64_t count = static_cast<uint64_t>(samples.size());
    const uint64_t maxVal = *std::max_element(samples.begin(), samples.end());
    return metrics::PercentileFromBuckets(buckets, count, percentile, maxVal);
}

// Replay samples through the real histogram + DumpSummaryForTest (same JSON as production summary).
void ObserveSamplesAndLogMetricsSummary(const char *utCaseTag, const std::vector<uint64_t> &samples)
{
    InitMetrics();
    auto h = metrics::GetHistogram(HISTOGRAM_ID);
    for (uint64_t v : samples) {
        h.Observe(v);
    }
    LOG(INFO) << "[metrics_summary] " << utCaseTag << ' ' << metrics::DumpSummaryForTest();
}

std::string ScalarMetricJson(const std::string &name, int64_t total, int64_t delta)
{
    std::ostringstream os;
    os << "{\"name\":\"" << name << "\",\"total\":" << total << ",\"delta\":" << delta << '}';
    return os.str();
}

std::string HistogramMetricJson(const std::string &name, uint64_t totalCount, uint64_t totalAvg, uint64_t totalMax,
                                uint64_t totalP50, uint64_t totalP90, uint64_t totalP99, uint64_t deltaCount,
                                uint64_t deltaAvg, uint64_t deltaMax, uint64_t deltaP50, uint64_t deltaP90,
                                uint64_t deltaP99)
{
    std::ostringstream os;
    os << "{\"name\":\"" << name << "\",\"total\":{\"count\":" << totalCount << ",\"avg_us\":" << totalAvg
       << ",\"max_us\":" << totalMax << ",\"p50\":" << totalP50 << ",\"p90\":" << totalP90
       << ",\"p99\":" << totalP99 << "},\"delta\":{\"count\":" << deltaCount << ",\"avg_us\":" << deltaAvg
       << ",\"max_us\":" << deltaMax << ",\"p50\":" << deltaP50 << ",\"p90\":" << deltaP90
       << ",\"p99\":" << deltaP99 << "}}";
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
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 2, 20, 30, 30, 30, 30, 2, 20, 30, 30, 30, 30)),
              std::string::npos);
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
    { metrics::ScopedTimer timer(HISTOGRAM_ID); }
    summary = metrics::DumpSummaryForTest();
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
    // All 64000 samples in one us bucket (10,20]; P99 interpolates toward upper bound → 20 rather than 10.
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 64000, 10, 10, 10, 10, 10, 64000, 10, 10, 10, 10, 10)),
              std::string::npos);
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

// Exercises LogSummary → LOG(INFO): stderr carries the same single-line metrics_summary JSON as
// DumpSummaryForTest(), including histogram total/delta p50, p90, and p99 (PR / doc sample).
TEST_F(MetricsTest, print_summary_histogram_json_includes_p99)
{
    InitMetrics();
    metrics::GetHistogram(HISTOGRAM_ID).Observe(10);
    metrics::GetHistogram(HISTOGRAM_ID).Observe(30);
    testing::internal::CaptureStderr();
    metrics::PrintSummary();
    std::string err = testing::internal::GetCapturedStderr();
    EXPECT_NE(err.find("{\"event\":\"metrics_summary\""), std::string::npos) << err;
    EXPECT_NE(err.find("\"p99\":30"), std::string::npos) << err;
    EXPECT_NE(err.find(HistogramMetricJson("test_histogram", 2, 20, 30, 30, 30, 30, 2, 20, 30, 30, 30, 30)), std::string::npos)
        << err;
    EXPECT_NE(err.find("\"p90\":30"), std::string::npos) << err;
    EXPECT_NE(err.find("\"p50\":30"), std::string::npos) << err;
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
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 2, 20, 30, 30, 30, 30, 1, 30, 30, 30, 30, 30)),
              std::string::npos);
}

TEST_F(MetricsTest, histogram_p99_total_even_split_test)
{
    InitMetrics();
    auto h = metrics::GetHistogram(HISTOGRAM_ID);
    for (int i = 0; i < 50; ++i) {
        h.Observe(10);
    }
    for (int i = 0; i < 50; ++i) {
        h.Observe(30);
    }
    auto summary = metrics::DumpSummaryForTest();
    // Bucket interpolation can exceed observed max; summary clamps p90/p99 to max_us (30).
    EXPECT_NE(summary.find(HistogramMetricJson("test_histogram", 100, 20, 30, 20, 30, 30, 100, 20, 30, 20, 30, 30)),
              std::string::npos)
        << summary;
}

TEST_F(MetricsTest, histogram_p99_total_and_delta_across_windows_test)
{
    InitMetrics();
    auto h = metrics::GetHistogram(HISTOGRAM_ID);
    for (int i = 0; i < 100; ++i) {
        h.Observe(10);
    }
    (void)metrics::DumpSummaryForTest();
    for (int i = 0; i < 100; ++i) {
        h.Observe(10);
    }
    h.Observe(100);
    auto summary = metrics::DumpSummaryForTest();
    // total: 201 samples, max=100; 200×10µs share (10,20] bin so P99 interpolates to 20 (< max, not 100).
    // sum=2100, avg=10; dSum=1100, dAvg=10; delta P99 matches same bucket math.
    EXPECT_NE(
        summary.find(HistogramMetricJson("test_histogram", 201, 10, 100, 16, 20, 20, 101, 10, 100, 16, 20, 20)),
        std::string::npos)
        << summary;
}

// PercentileFromBuckets unit checks (same implementation as BuildSummary P99 path).
TEST_F(MetricsTest, percentile_from_buckets_sparse_two_values_returns_overflow_max)
{
    metrics::HistBuckets buckets{};
    buckets[metrics::BucketIndex(10)]++;
    buckets[metrics::BucketIndex(30)]++;
    EXPECT_EQ(metrics::PercentileFromBuckets(buckets, 2, 99, 30), 30u);
}

TEST_F(MetricsTest, percentile_from_buckets_single_ten)
{
    metrics::HistBuckets buckets{};
    buckets[metrics::BucketIndex(10)]++;
    EXPECT_EQ(metrics::PercentileFromBuckets(buckets, 1, 99, 10), 10u);
}

TEST_F(MetricsTest, percentile_from_buckets_single_thirty)
{
    metrics::HistBuckets buckets{};
    buckets[metrics::BucketIndex(30)]++;
    EXPECT_EQ(metrics::PercentileFromBuckets(buckets, 1, 99, 30), 30u);
}

TEST_F(MetricsTest, percentile_from_buckets_overflow_bucket_uses_overflow_max)
{
    metrics::HistBuckets buckets{};
    buckets[metrics::BucketIndex(100000000)]++;
    EXPECT_EQ(metrics::PercentileFromBuckets(buckets, 1, 99, 100000000), 100000000u);
}

TEST_F(MetricsTest, percentile_from_buckets_empty)
{
    metrics::HistBuckets buckets{};
    EXPECT_EQ(metrics::PercentileFromBuckets(buckets, 0, 99, 0), 0u);
}

TEST_F(MetricsTest, percentile_from_buckets_even_split_interpolation)
{
    metrics::HistBuckets buckets{};
    for (int i = 0; i < 50; ++i) {
        buckets[metrics::BucketIndex(10)]++;
    }
    for (int i = 0; i < 50; ++i) {
        buckets[metrics::BucketIndex(30)]++;
    }
    EXPECT_EQ(metrics::PercentileFromBuckets(buckets, 100, 99, 30), 30u);
}

// Ground-truth order statistic vs fixed-bucket histogram (always <= observed max after clamp).
TEST_F(MetricsTest, histogram_percentile_vs_ground_truth_all_identical)
{
    std::vector<uint64_t> samples(5000, 777U);
    ObserveSamplesAndLogMetricsSummary("MetricsTest.histogram_percentile_vs_ground_truth_all_identical", samples);
    EXPECT_EQ(GroundTruthPercentile(samples, 99), 777u);
    EXPECT_EQ(HistogramPercentileFromSamples(samples, 99), 777u);
    EXPECT_EQ(GroundTruthPercentile(samples, 90), 777u);
    EXPECT_EQ(HistogramPercentileFromSamples(samples, 90), 777u);
}

TEST_F(MetricsTest, histogram_percentile_vs_ground_truth_two_point_mass)
{
    std::vector<uint64_t> samples;
    samples.reserve(100);
    for (int i = 0; i < 50; ++i) {
        samples.push_back(10);
    }
    for (int i = 0; i < 50; ++i) {
        samples.push_back(30);
    }
    ObserveSamplesAndLogMetricsSummary("MetricsTest.histogram_percentile_vs_ground_truth_two_point_mass", samples);
    const uint64_t gt99 = GroundTruthPercentile(samples, 99);
    const uint64_t apx99 = HistogramPercentileFromSamples(samples, 99);
    EXPECT_EQ(gt99, 30u);
    EXPECT_EQ(apx99, 30u);
}

TEST_F(MetricsTest, histogram_percentile_vs_ground_truth_sparse_outlier_documents_bias)
{
    std::vector<uint64_t> samples;
    samples.reserve(100);
    for (int i = 0; i < 99; ++i) {
        samples.push_back(10);
    }
    samples.push_back(10000);
    ObserveSamplesAndLogMetricsSummary(
        "MetricsTest.histogram_percentile_vs_ground_truth_sparse_outlier_documents_bias", samples);
    const uint64_t gt99 = GroundTruthPercentile(samples, 99);
    const uint64_t apx99 = HistogramPercentileFromSamples(samples, 99);
    EXPECT_EQ(gt99, 10u);
    EXPECT_EQ(apx99, 20u);
    EXPECT_LE(apx99, *std::max_element(samples.begin(), samples.end()));
}

// Rounded i.i.d. Gaussian: symmetric unimodal — tail quantiles sit above the sample mean (contrast with
// point-mass + rare huge outlier, where mean ≫ p99 order statistic).
TEST_F(MetricsTest, histogram_percentile_vs_ground_truth_near_gaussian)
{
    std::mt19937 gen(20260504);
    std::normal_distribution<double> dist(2500.0, 320.0);
    constexpr int kN = 12000;
    std::vector<uint64_t> samples;
    samples.reserve(static_cast<size_t>(kN));
    const double cap = static_cast<double>(metrics::HIST_BUCKET_UPPER.back());
    for (int i = 0; i < kN; ++i) {
        double x = dist(gen);
        if (x < 1.0) {
            x = 1.0;
        }
        if (x > cap) {
            x = cap;
        }
        samples.push_back(static_cast<uint64_t>(std::llround(x)));
    }
    ObserveSamplesAndLogMetricsSummary("MetricsTest.histogram_percentile_vs_ground_truth_near_gaussian", samples);
    uint64_t sum = 0;
    for (uint64_t v : samples) {
        sum += v;
    }
    const uint64_t avg = sum / static_cast<uint64_t>(kN);
    const uint64_t gt99 = GroundTruthPercentile(samples, 99);
    const uint64_t gt90 = GroundTruthPercentile(samples, 90);
    const uint64_t apx99 = HistogramPercentileFromSamples(samples, 99);
    const uint64_t apx90 = HistogramPercentileFromSamples(samples, 90);
    const uint64_t mx = *std::max_element(samples.begin(), samples.end());

    EXPECT_GT(gt99, avg);
    EXPECT_GT(gt90, avg);
    EXPECT_LE(apx99, mx);
    EXPECT_LE(apx90, mx);

    uint64_t maxSpan = 0;
    for (uint64_t v : samples) {
        const size_t bi = metrics::BucketIndex(v);
        const uint64_t lo = bi == 0 ? 0ULL : metrics::HIST_BUCKET_UPPER[bi - 1];
        const uint64_t span = metrics::HIST_BUCKET_UPPER[bi] - lo;
        if (span > maxSpan) {
            maxSpan = span;
        }
    }
    const auto abs64 = [](int64_t x) -> int64_t { return x < 0 ? -x : x; };
    const uint64_t tol = 8U * maxSpan + 50U;
    EXPECT_LE(abs64(static_cast<int64_t>(apx99) - static_cast<int64_t>(gt99)), static_cast<int64_t>(tol));
    EXPECT_LE(abs64(static_cast<int64_t>(apx90) - static_cast<int64_t>(gt90)), static_cast<int64_t>(tol));
}

// Tight bulk (+ smooth spread) plus a single enormous sample: p99 must stay with the bulk, not track max.
// (If p99 ≈ max here, summary would misrepresent typical latency — max already exposes the spike.)
TEST_F(MetricsTest, histogram_percentile_vs_ground_truth_smooth_bulk_one_spike)
{
    constexpr int kBulk = 12000;
    constexpr uint64_t kSpike = 8000000ULL;
    std::mt19937 gen(20260505);
    std::uniform_int_distribution<uint64_t> bulkDist(240, 260);
    std::vector<uint64_t> samples;
    samples.reserve(static_cast<size_t>(kBulk + 1));
    for (int i = 0; i < kBulk; ++i) {
        samples.push_back(bulkDist(gen));
    }
    samples.push_back(kSpike);

    ObserveSamplesAndLogMetricsSummary(
        "MetricsTest.histogram_percentile_vs_ground_truth_smooth_bulk_one_spike", samples);

    const uint64_t mx = *std::max_element(samples.begin(), samples.end());
    const uint64_t gt99 = GroundTruthPercentile(samples, 99);
    const uint64_t apx99 = HistogramPercentileFromSamples(samples, 99);
    ASSERT_EQ(mx, kSpike);

    EXPECT_LE(gt99, 280u);
    EXPECT_GE(gt99, 240u);
    EXPECT_LE(apx99, mx);
    EXPECT_LT(gt99, mx / 1000U);
    EXPECT_LT(apx99, mx / 1000U);

    uint64_t maxSpan = 0;
    for (uint64_t v : samples) {
        if (v == kSpike) {
            continue;
        }
        const size_t bi = metrics::BucketIndex(v);
        const uint64_t lo = bi == 0 ? 0ULL : metrics::HIST_BUCKET_UPPER[bi - 1];
        const uint64_t span = metrics::HIST_BUCKET_UPPER[bi] - lo;
        if (span > maxSpan) {
            maxSpan = span;
        }
    }
    const auto abs64 = [](int64_t x) -> int64_t { return x < 0 ? -x : x; };
    const uint64_t tol = 8U * maxSpan + 50U;
    EXPECT_LE(abs64(static_cast<int64_t>(apx99) - static_cast<int64_t>(gt99)), static_cast<int64_t>(tol));
}

TEST_F(MetricsTest, histogram_percentile_vs_ground_truth_random_single_bucket_tight)
{
    std::mt19937 gen(20260503);
    std::uniform_int_distribution<uint64_t> dist(150, 180);
    const int kIters = 300;
    const int kN = 900;
    for (int iter = 0; iter < kIters; ++iter) {
        std::vector<uint64_t> samples;
        samples.reserve(static_cast<size_t>(kN));
        for (int i = 0; i < kN; ++i) {
            samples.push_back(dist(gen));
        }
        if (iter == 0) {
            ObserveSamplesAndLogMetricsSummary(
                "MetricsTest.histogram_percentile_vs_ground_truth_random_single_bucket_tight/iter0", samples);
        }
        const uint64_t mx = *std::max_element(samples.begin(), samples.end());
        const uint64_t gt99 = GroundTruthPercentile(samples, 99);
        const uint64_t apx99 = HistogramPercentileFromSamples(samples, 99);
        const uint64_t gt90 = GroundTruthPercentile(samples, 90);
        const uint64_t apx90 = HistogramPercentileFromSamples(samples, 90);
        EXPECT_LE(apx99, mx) << "iter " << iter;
        EXPECT_LE(apx90, mx) << "iter " << iter;
        const int64_t d99 = static_cast<int64_t>(apx99) - static_cast<int64_t>(gt99);
        const int64_t d90 = static_cast<int64_t>(apx90) - static_cast<int64_t>(gt90);
        const size_t bi = metrics::BucketIndex(mx);
        const uint64_t bucketLo = bi == 0 ? 0ULL : metrics::HIST_BUCKET_UPPER[bi - 1];
        const uint64_t bucketSpan = metrics::HIST_BUCKET_UPPER[bi] - bucketLo;
        const uint64_t tol = bucketSpan + 1U;
        const auto abs64 = [](int64_t x) -> int64_t { return x < 0 ? -x : x; };
        EXPECT_LE(abs64(d99), static_cast<int64_t>(tol)) << "iter " << iter;
        EXPECT_LE(abs64(d90), static_cast<int64_t>(tol)) << "iter " << iter;
    }
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

TEST_F(MetricsTest, kv_metric_urma_id_layout_test)
{
    size_t count = 0;
    auto descs = metrics::GetKvMetricDescs(count);
    ASSERT_NE(descs, nullptr);
    ASSERT_GT(count, static_cast<size_t>(metrics::KvMetricId::WORKER_GET_POST_QUERY_META_PHASE_LATENCY));
    EXPECT_EQ(static_cast<uint16_t>(metrics::KvMetricId::URMA_IMPORT_JFR), 62);
    EXPECT_EQ(static_cast<uint16_t>(metrics::KvMetricId::URMA_INFLIGHT_WR_COUNT), 63);
    EXPECT_EQ(static_cast<uint16_t>(metrics::KvMetricId::URMA_NANOSLEEP_LATENCY), 64);
    EXPECT_EQ(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY), 65);
    EXPECT_STREQ(descs[64].name, "urma_nanosleep_latency");
    EXPECT_STREQ(descs[65].name, "worker_rpc_remote_get_inbound_latency");
    EXPECT_STREQ(descs[66].name, "worker_get_threadpool_queue_latency");
    EXPECT_STREQ(descs[67].name, "worker_get_threadpool_exec_latency");
    EXPECT_STREQ(descs[68].name, "worker_get_meta_addr_hashring_latency");
    EXPECT_STREQ(descs[69].name, "worker_get_post_query_meta_phase_latency");
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
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_put_request_total", 1, 1)),
              std::string::npos);
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
    EXPECT_NE(metrics::DumpSummaryForTest().find(ScalarMetricJson("client_get_request_total", 1, 1)),
              std::string::npos);
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
    metrics::GetHistogram(
        static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_OUTBOUND_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest().find("\"name\":\"worker_rpc_remote_get_outbound_latency\""),
              std::string::npos);
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
    EXPECT_NE(metrics::DumpSummaryForTest()
                  .find(HistogramMetricJson("worker_rpc_create_meta_latency", 1, 10, 10, 10, 10, 10, 1, 10, 10, 10, 10,
                                          10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_query_meta_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_QUERY_META_LATENCY)).Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest()
                  .find(HistogramMetricJson("worker_rpc_query_meta_latency", 1, 10, 10, 10, 10, 10, 1, 10, 10, 10, 10,
                                          10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_remote_get_test)
{
    InitKvMetricsForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_OUTBOUND_LATENCY))
        .Observe(10);
    EXPECT_NE(metrics::DumpSummaryForTest()
                  .find(HistogramMetricJson("worker_rpc_remote_get_outbound_latency", 1, 10, 10, 10, 10, 10, 1, 10, 10,
                                          10, 10, 10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_urma_path_test)
{
    InitKvMetricsForTest();
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_URMA_READ_TOTAL_BYTES, 32);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_URMA_WRITE_LATENCY)).Observe(10);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_urma_read_total_bytes", 32, 32)), std::string::npos);
    EXPECT_NE(summary.find(HistogramMetricJson("worker_urma_write_latency", 1, 10, 10, 10, 10, 10, 1, 10, 10, 10, 10,
                                                 10)),
              std::string::npos);
}

TEST_F(MetricsTest, kv_metrics_tcp_fallback_test)
{
    InitKvMetricsForTest();
    METRIC_ADD(metrics::KvMetricId::CLIENT_GET_TCP_READ_TOTAL_BYTES, 64);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_TCP_WRITE_LATENCY)).Observe(10);
    auto summary = metrics::DumpSummaryForTest();
    EXPECT_NE(summary.find(ScalarMetricJson("client_get_tcp_read_total_bytes", 64, 64)), std::string::npos);
    EXPECT_NE(
        summary.find(HistogramMetricJson("worker_tcp_write_latency", 1, 10, 10, 10, 10, 10, 1, 10, 10, 10, 10, 10)),
        std::string::npos);
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
