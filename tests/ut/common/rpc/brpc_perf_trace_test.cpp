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

#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/rpc/brpc_perf_trace.h"

#include <string>
#include <thread>
#include <type_traits>

#include <nlohmann/json.hpp>
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

using json = nlohmann::json;

json DumpSummaryJson()
{
    auto parts = metrics::DumpSummariesForTest();
    if (parts.empty()) {
        return json();
    }
    json merged;
    for (size_t i = 0; i < parts.size(); ++i) {
        json j = json::parse(parts[i]);
        if (i == 0) {
            merged = std::move(j);
            continue;
        }
        if (j.contains("metrics") && j["metrics"].is_array()) {
            for (const auto &m : j["metrics"]) {
                merged["metrics"].push_back(m);
            }
        }
    }
    merged["part_index"] = 1;
    merged["part_count"] = 1;
    return merged;
}

const json *FindMetric(const json &summary, const std::string &name)
{
    if (!summary.contains("metrics")) {
        return nullptr;
    }
    for (const auto &metric : summary["metrics"]) {
        if (metric["name"] == name) {
            return &metric;
        }
    }
    return nullptr;
}

uint64_t HistogramField(const json &summary, const std::string &name, const char *section, const char *field)
{
    auto metric = FindMetric(summary, name);
    return metric == nullptr ? 0 : (*metric)[section][field].get<uint64_t>();
}

class BrpcPerfTraceTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        metrics::ResetKvMetricsForTest();
        DS_ASSERT_OK(metrics::InitKvMetrics());
    }

    void TearDown() override
    {
        metrics::ResetKvMetricsForTest();
    }
};

TEST_F(BrpcPerfTraceTest, metric_descs_register_brpc_rpc_phase_histograms)
{
    size_t count = 0;
    auto descs = metrics::GetKvMetricDescs(count);
    ASSERT_NE(descs, nullptr);
    ASSERT_EQ(count, static_cast<size_t>(metrics::KvMetricId::KV_METRIC_END));

    auto expectMetric = [descs, count](metrics::KvMetricId id, const std::string &name) {
        const auto idx = static_cast<size_t>(id);
        ASSERT_LT(idx, count);
        EXPECT_EQ(descs[idx].id, idx);
        EXPECT_EQ(descs[idx].name, name);
        EXPECT_EQ(descs[idx].type, metrics::MetricType::HISTOGRAM);
        EXPECT_STREQ(descs[idx].unit, "us");
    };
    expectMetric(metrics::KvMetricId::BRPC_CLIENT_REQ_FRAMEWORK_LATENCY, "brpc_client_req_framework_latency");
    expectMetric(metrics::KvMetricId::BRPC_REMOTE_PROCESSING_LATENCY, "brpc_remote_processing_latency");
    expectMetric(metrics::KvMetricId::BRPC_CLIENT_RSP_FRAMEWORK_LATENCY, "brpc_client_rsp_framework_latency");
    expectMetric(metrics::KvMetricId::BRPC_SERVER_REQ_QUEUE_LATENCY, "brpc_server_req_queue_latency");
    expectMetric(metrics::KvMetricId::BRPC_SERVER_EXEC_LATENCY, "brpc_server_exec_latency");
    expectMetric(metrics::KvMetricId::BRPC_SERVER_RSP_QUEUE_LATENCY, "brpc_server_rsp_queue_latency");
    expectMetric(metrics::KvMetricId::BRPC_RPC_E2E_LATENCY, "brpc_rpc_e2e_latency");
    expectMetric(metrics::KvMetricId::BRPC_RPC_NETWORK_RESIDUAL_LATENCY, "brpc_rpc_network_residual_latency");
}

TEST_F(BrpcPerfTraceTest, trace_timestamp_storage_is_not_copyable)
{
    EXPECT_FALSE(std::is_copy_constructible<BrpcPerfTrace>::value);
    EXPECT_FALSE(std::is_copy_assignable<BrpcPerfTrace>::value);
}

TEST_F(BrpcPerfTraceTest, destroy_fallback_records_only_started_rpc)
{
    BrpcPerfTrace neverStarted("trace-never-started", "WorkerService.StreamObjects");
    EXPECT_FALSE(ShouldRecordBrpcTraceOnDestroy(neverStarted));

    BrpcPerfTrace started("trace-started", "WorkerService.StreamObjects");
    started.MarkClientStart(1'000'000ULL);
    EXPECT_TRUE(ShouldRecordBrpcTraceOnDestroy(started));
}

TEST_F(BrpcPerfTraceTest, trace_clock_returns_monotonic_nanoseconds)
{
    auto before = BrpcTraceNowNs();
    std::this_thread::sleep_for(std::chrono::microseconds(1));
    auto after = BrpcTraceNowNs();
    EXPECT_GT(after, before);
    EXPECT_GT(after - before, 0u);
}

TEST_F(BrpcPerfTraceTest, record_unary_rpc_observes_zmq_equivalent_phase_metrics)
{
    BrpcPerfTrace trace("trace-1", "WorkerService.GetObject");
    trace.MarkClientStart(1'000'000'000ULL);
    trace.MarkClientSend(1'002'000'000ULL);
    trace.MarkServerRecv(9'000'000'000ULL);
    trace.MarkServerExecStart(9'001'000'000ULL);
    trace.MarkServerExecEnd(9'006'000'000ULL);
    trace.MarkServerSend(9'007'000'000ULL);
    trace.MarkClientRecv(1'012'000'000ULL);
    trace.MarkClientEnd(1'013'000'000ULL);

    RecordBrpcRpcTrace(trace);

    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "brpc_client_req_framework_latency", "total", "avg_us"), 2'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_remote_processing_latency", "total", "avg_us"), 10'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_client_rsp_framework_latency", "total", "avg_us"), 1'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_req_queue_latency", "total", "avg_us"), 1'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_exec_latency", "total", "avg_us"), 5'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_rsp_queue_latency", "total", "avg_us"), 1'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_rpc_e2e_latency", "total", "avg_us"), 13'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_rpc_network_residual_latency", "total", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "brpc_rpc_network_residual_latency", "total", "avg_us"), 3'000u);
}

TEST_F(BrpcPerfTraceTest, partial_trace_does_not_emit_huge_missing_phase_latency)
{
    BrpcPerfTrace trace("partial-stream-trace", "WorkerService.StreamObjects");
    trace.MarkClientStart(1'000'000ULL);
    trace.MarkClientSend(2'000'000ULL);
    trace.MarkClientEnd(5'000'000ULL);

    RecordBrpcRpcTrace(trace);

    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "brpc_client_req_framework_latency", "total", "avg_us"), 1'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_rpc_e2e_latency", "total", "avg_us"), 4'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_client_rsp_framework_latency", "total", "count"), 0u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_exec_latency", "total", "count"), 0u);
}

TEST_F(BrpcPerfTraceTest, stream_summary_trace_records_once_at_stream_end)
{
    BrpcPerfTrace trace("stream-trace", "WorkerService.StreamObjects");
    std::atomic<bool> recorded { false };
    auto recordOnce = [&trace, &recorded](uint64_t serverExecEndTs, uint64_t serverSendTs) {
        trace.MarkServerExecEnd(serverExecEndTs);
        if (!recorded.exchange(true, std::memory_order_acq_rel)) {
            trace.MarkServerSend(serverSendTs);
            RecordBrpcRpcTrace(trace);
        }
    };

    trace.MarkServerRecv(10'000'000ULL);
    trace.MarkServerExecStart(11'000'000ULL);
    recordOnce(20'000'000ULL, 21'000'000ULL);
    recordOnce(30'000'000ULL, 31'000'000ULL);

    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "brpc_server_exec_latency", "total", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_exec_latency", "total", "avg_us"), 9'000u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_rsp_queue_latency", "total", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "brpc_server_rsp_queue_latency", "total", "avg_us"), 1'000u);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
