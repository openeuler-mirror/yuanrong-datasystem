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
 * Description: Unit tests for ZMQ metrics definitions and instrumentation logic.
 *
 * Test categories:
 *   [BASIC]   Registration and zero-value verification
 *   [FAULT]   Fault counter increment and delta correctness
 *   [PERF]    Latency histogram observe and delta
 *   [ERRNO]   IsZmqSocketNetworkErrno classification
 *   [CONC]    Concurrent safety
 *   [SCENE]   End-to-end scenario simulations
 */

#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/rpc/zmq/zmq_network_errno.h"
#include "datasystem/protos/meta_zmq.pb.h"

#include <cerrno>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

using json = nlohmann::json;

json DumpSummaryJson()
{
    auto summary = metrics::DumpSummaryForTest();
    if (summary.empty()) {
        return json();
    }
    return json::parse(summary);
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

int64_t ScalarTotal(const json &summary, const std::string &name)
{
    auto metric = FindMetric(summary, name);
    return metric == nullptr ? 0 : (*metric)["total"].get<int64_t>();
}

int64_t ScalarDelta(const json &summary, const std::string &name)
{
    auto metric = FindMetric(summary, name);
    return metric == nullptr ? 0 : (*metric)["delta"].get<int64_t>();
}

uint64_t HistogramField(const json &summary, const std::string &name, const char *section, const char *field)
{
    auto metric = FindMetric(summary, name);
    return metric == nullptr ? 0 : (*metric)[section][field].get<uint64_t>();
}

class ZmqMetricsTest : public CommonTest {
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

// ── [BASIC] ──────────────────────────────────────────────────────────────────

// Case 1: All 13 metrics registered, initial values are zero
TEST_F(ZmqMetricsTest, all_metrics_registered_and_zero)
{
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_send_failure_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_receive_failure_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_send_try_again_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_receive_try_again_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_network_error_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_last_error_number"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_gateway_recreate_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_event_disconnect_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_event_handshake_failure_total"), 0);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "count"), 0u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "count"), 0u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_serialize_latency", "total", "count"), 0u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_deserialize_latency", "total", "count"), 0u);
}

// Case 2: Total metric count matches descriptor table
TEST_F(ZmqMetricsTest, metric_descs_count)
{
    size_t count = 0;
    (void)metrics::GetKvMetricDescs(count);
    EXPECT_GE(count, static_cast<size_t>(metrics::KvMetricId::ZMQ_RPC_DESERIALIZE_LATENCY) + 1);
}

// ── [FAULT] ──────────────────────────────────────────────────────────────────

// Case 3: send.fail counter increments correctly
TEST_F(ZmqMetricsTest, send_fail_counter_inc)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc();
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc();
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_send_failure_total"), 2);
    EXPECT_EQ(ScalarDelta(summary, "zmq_send_failure_total"), 2);
}

// Case 4: recv.fail + net_error + last_errno linked correctly
TEST_F(ZmqMetricsTest, recv_fail_net_error_last_errno_linked)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc();
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_NETWORK_ERROR_TOTAL)).Inc();
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(ECONNRESET);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_receive_failure_total"), 1);
    EXPECT_EQ(ScalarTotal(summary, "zmq_network_error_total"), 1);
    EXPECT_EQ(ScalarTotal(summary, "zmq_last_error_number"), ECONNRESET);
}

// Case 5: last_errno gauge overwrites correctly (only last Set wins)
TEST_F(ZmqMetricsTest, last_errno_gauge_overwrite)
{
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(ECONNREFUSED);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(EHOSTUNREACH);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_last_error_number"), EHOSTUNREACH);
}

// Case 6: delta between two dumps is correct
TEST_F(ZmqMetricsTest, fault_counter_delta_between_dumps)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc(5);
    (void)DumpSummaryJson();            // snapshot 1
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc(3);
    auto summary = DumpSummaryJson();         // snapshot 2
    EXPECT_EQ(ScalarTotal(summary, "zmq_receive_failure_total"), 8);  // total
    EXPECT_EQ(ScalarDelta(summary, "zmq_receive_failure_total"), 3);  // delta
}

// Case 7: zero delta — no new faults → all +0
TEST_F(ZmqMetricsTest, zero_delta_when_quiet)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc(1);
    (void)DumpSummaryJson();
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarDelta(summary, "zmq_send_failure_total"), 0);
    EXPECT_EQ(ScalarDelta(summary, "zmq_receive_failure_total"), 0);
    EXPECT_EQ(ScalarDelta(summary, "zmq_network_error_total"), 0);
}

// Case 8: Layer 2 connection event counters
TEST_F(ZmqMetricsTest, layer2_connection_event_counters)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_GATEWAY_RECREATE_TOTAL)).Inc();
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_EVENT_DISCONNECT_TOTAL)).Inc(3);
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_EVENT_HANDSHAKE_FAILURE_TOTAL)).Inc(2);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_gateway_recreate_total"), 1);
    EXPECT_EQ(ScalarTotal(summary, "zmq_event_disconnect_total"), 3);
    EXPECT_EQ(ScalarTotal(summary, "zmq_event_handshake_failure_total"), 2);
}

// ── [ERRNO] ──────────────────────────────────────────────────────────────────

// Case 9: IsZmqSocketNetworkErrno (same impl as zmq_socket_ref.cpp) — all 9 network errnos
TEST_F(ZmqMetricsTest, is_network_errno_true_cases)
{
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ECONNREFUSED));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ECONNRESET));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ECONNABORTED));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(EHOSTUNREACH));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ENETUNREACH));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ENETDOWN));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ETIMEDOUT));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(EPIPE));
    EXPECT_TRUE(IsZmqSocketNetworkErrno(ENOTCONN));
}

// Case 10: rejects non-network errnos
TEST_F(ZmqMetricsTest, is_network_errno_false_cases)
{
    EXPECT_FALSE(IsZmqSocketNetworkErrno(EAGAIN));
    EXPECT_FALSE(IsZmqSocketNetworkErrno(EINTR));
    EXPECT_FALSE(IsZmqSocketNetworkErrno(ENOMEM));
    EXPECT_FALSE(IsZmqSocketNetworkErrno(ENOENT));
    EXPECT_FALSE(IsZmqSocketNetworkErrno(EBADF));
    EXPECT_FALSE(IsZmqSocketNetworkErrno(0));
}

// ── [PERF] ───────────────────────────────────────────────────────────────────

// Case 11: I/O histogram observe and summary format
TEST_F(ZmqMetricsTest, io_histogram_observe)
{
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(100);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(200);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_IO_LATENCY)).Observe(500);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "count"), 2u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "avg_us"), 150u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "max_us"), 200u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "avg_us"), 500u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "max_us"), 500u);
}

// Case 12: Serialization histograms
TEST_F(ZmqMetricsTest, ser_deser_histogram_observe)
{
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_SERIALIZE_LATENCY)).Observe(10);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_SERIALIZE_LATENCY)).Observe(20);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_DESERIALIZE_LATENCY)).Observe(8);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_serialize_latency", "total", "count"), 2u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_serialize_latency", "total", "avg_us"), 15u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_serialize_latency", "total", "max_us"), 20u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_deserialize_latency", "total", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_deserialize_latency", "total", "avg_us"), 8u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_deserialize_latency", "total", "max_us"), 8u);
}

// Case 13: periodMax resets between dumps (delta max reflects only latest period)
TEST_F(ZmqMetricsTest, histogram_period_max_reset_between_dumps)
{
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(1000);
    (void)DumpSummaryJson();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(200);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "count"), 2u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "avg_us"), 600u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "max_us"), 1000u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "delta", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "delta", "avg_us"), 200u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "delta", "max_us"), 200u);
}

// Case 14: Histogram delta count increases correctly across dumps
TEST_F(ZmqMetricsTest, histogram_delta_count)
{
    for (int i = 0; i < 10; ++i) {
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_IO_LATENCY)).Observe(100);
    }
    (void)DumpSummaryJson();
    for (int i = 0; i < 5; ++i) {
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_IO_LATENCY)).Observe(100);
    }
    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "count"), 15u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "delta", "count"), 5u);
}

// ── [CONC] ───────────────────────────────────────────────────────────────────

// Case 15: Concurrent Counter and Histogram increments are race-free
TEST_F(ZmqMetricsTest, concurrent_counter_and_histogram)
{
    const int threads = 32;
    const int loops = 500;
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&] {
            for (int j = 0; j < loops; ++j) {
                metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(100);
                metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc();
            }
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    auto summary = DumpSummaryJson();
    const auto expected = threads * loops;
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "count"), static_cast<uint64_t>(expected));
    EXPECT_EQ(ScalarTotal(summary, "zmq_send_failure_total"), expected);
}

// ── [NOOP] ───────────────────────────────────────────────────────────────────

// Case 16: Operations before Init are safe no-ops
TEST_F(ZmqMetricsTest, noop_before_init_no_crash)
{
    metrics::ResetForTest();   // clear the SetUp init
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(100);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(42);
    auto summary = DumpSummaryJson();
    EXPECT_TRUE(summary.is_null());    // uninited → empty summary
}

// ── [SCENE] ──────────────────────────────────────────────────────────────────

// Case 17: Scenario — network card failure pattern
//   expect: send.fail↑, recv.fail↑, net_error↑, last_errno=EHOSTUNREACH, evt.disconn↑, gw_recreate↑
TEST_F(ZmqMetricsTest, scenario_network_card_failure)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc(5);
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc(3);
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_NETWORK_ERROR_TOTAL)).Inc(8);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(EHOSTUNREACH);
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_EVENT_DISCONNECT_TOTAL)).Inc(2);
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_GATEWAY_RECREATE_TOTAL)).Inc(1);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_network_error_total"), 8);
    EXPECT_EQ(ScalarTotal(summary, "zmq_last_error_number"), EHOSTUNREACH);
    EXPECT_EQ(ScalarTotal(summary, "zmq_event_disconnect_total"), 2);
    EXPECT_EQ(ScalarTotal(summary, "zmq_gateway_recreate_total"), 1);
}

// Case 18: Scenario — peer hang (only recv.eagain↑, net_error stays 0)
TEST_F(ZmqMetricsTest, scenario_peer_hang)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_TRY_AGAIN_TOTAL)).Inc(10);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_receive_try_again_total"), 10);
    EXPECT_EQ(ScalarTotal(summary, "zmq_network_error_total"), 0);
    EXPECT_EQ(ScalarTotal(summary, "zmq_send_failure_total"), 0);
}

// Case 19: Scenario — HWM backpressure (send.eagain↑, net_error stays 0)
TEST_F(ZmqMetricsTest, scenario_hwm_backpressure)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_TRY_AGAIN_TOTAL)).Inc(100);
    auto summary = DumpSummaryJson();
    EXPECT_EQ(ScalarTotal(summary, "zmq_send_try_again_total"), 100);
    EXPECT_EQ(ScalarTotal(summary, "zmq_network_error_total"), 0);
}

// Case 20: Scenario — RPC framework self-proof (I/O dominates, ser/deser small)
TEST_F(ZmqMetricsTest, scenario_self_prove_framework_innocent)
{
    // Simulate: 100 RPCs, io avg=500us, ser avg=10us, deser avg=8us
    for (int i = 0; i < 100; ++i) {
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(500);
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_IO_LATENCY)).Observe(800);
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_SERIALIZE_LATENCY)).Observe(10);
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_DESERIALIZE_LATENCY)).Observe(8);
    }
    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "count"), 100u);
    EXPECT_EQ(HistogramField(summary, "zmq_send_io_latency", "total", "avg_us"), 500u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "count"), 100u);
    EXPECT_EQ(HistogramField(summary, "zmq_receive_io_latency", "total", "avg_us"), 800u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_serialize_latency", "total", "count"), 100u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_serialize_latency", "total", "avg_us"), 10u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_deserialize_latency", "total", "count"), 100u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_deserialize_latency", "total", "avg_us"), 8u);
}

// Caller-clock e2e vs residual network estimate; SERVER_RPC_WINDOW_NS = SERVER_SEND - SERVER_RECV (server host).
TEST_F(ZmqMetricsTest, queue_flow_residual_network_matches_e2e_minus_framework)
{
    const uint64_t t0 = 1000000000000000ULL;
    MetaPb meta;
    auto addTs = [&](const char *tickName, uint64_t wallNs) {
        TickPb tk;
        tk.set_tick_name(tickName);
        tk.set_ts(wallNs);
        meta.mutable_ticks()->Add(std::move(tk));
    };
    addTs(TICK_CLIENT_ENQUEUE, t0);
    addTs(TICK_CLIENT_TO_STUB, t0 + 2'500'000ULL);     // client framework fragment = 2.5ms on caller clock
    addTs(TICK_SERVER_RECV, t0 + 5'500'000ULL);         // diagnostics only — not subtracted vs client timestamps
    addTs(TICK_SERVER_DEQUEUE, t0 + 5'520'000ULL);
    addTs(TICK_SERVER_EXEC_END, t0 + 5'570'000ULL);
    addTs(TICK_SERVER_SEND, t0 + 5'572'000ULL);         // SERVER_SEND − SERVER_RECV = 72000 ns on server clock
    addTs(TICK_CLIENT_RECV, t0 + 6'572'000ULL);         // e2e_ns = 6572000 ns
    TickPb winSynth;
    winSynth.set_tick_name(TICK_SERVER_RPC_WINDOW_NS);
    winSynth.set_ts(72'000ULL);
    meta.mutable_ticks()->Add(std::move(winSynth));

    RecordRpcLatencyMetrics(meta);

    auto summary = DumpSummaryJson();
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_e2e_latency", "total", "count"), 1u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_e2e_latency", "total", "avg_us"), 6572u);

    EXPECT_EQ(HistogramField(summary, "zmq_client_queuing_latency", "total", "avg_us"), 2500u);
    EXPECT_EQ(HistogramField(summary, "zmq_rpc_network_latency", "total", "avg_us"), 4000u);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
