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
#include "datasystem/common/rpc/zmq/zmq_network_errno.h"

#include <cerrno>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

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
    auto s = metrics::DumpSummaryForTest();
    // Fault counters
    EXPECT_NE(s.find("zmq_send_failure_total=0"),   std::string::npos);
    EXPECT_NE(s.find("zmq_receive_failure_total=0"),   std::string::npos);
    EXPECT_NE(s.find("zmq_send_try_again_total=0"), std::string::npos);
    EXPECT_NE(s.find("zmq_receive_try_again_total=0"), std::string::npos);
    EXPECT_NE(s.find("zmq_network_error_total=0"),   std::string::npos);
    EXPECT_NE(s.find("zmq_last_error_number=0"),  std::string::npos);
    EXPECT_NE(s.find("zmq_gateway_recreate_total=0"), std::string::npos);
    EXPECT_NE(s.find("zmq_event_disconnect_total=0"), std::string::npos);
    EXPECT_NE(s.find("zmq_event_handshake_failure_total=0"), std::string::npos);
    // Latency histograms
    EXPECT_NE(s.find("zmq_send_io_latency,count=0"),   std::string::npos);
    EXPECT_NE(s.find("zmq_receive_io_latency,count=0"),   std::string::npos);
    EXPECT_NE(s.find("zmq_rpc_serialize_latency,count=0"),   std::string::npos);
    EXPECT_NE(s.find("zmq_rpc_deserialize_latency,count=0"), std::string::npos);
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
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_send_failure_total=2"),  std::string::npos);
    EXPECT_NE(s.find("zmq_send_failure_total=+2"), std::string::npos);
}

// Case 4: recv.fail + net_error + last_errno linked correctly
TEST_F(ZmqMetricsTest, recv_fail_net_error_last_errno_linked)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc();
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_NETWORK_ERROR_TOTAL)).Inc();
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(ECONNRESET);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_receive_failure_total=1"), std::string::npos);
    EXPECT_NE(s.find("zmq_network_error_total=1"), std::string::npos);
    EXPECT_NE(s.find("zmq_last_error_number=" + std::to_string(ECONNRESET)), std::string::npos);
}

// Case 5: last_errno gauge overwrites correctly (only last Set wins)
TEST_F(ZmqMetricsTest, last_errno_gauge_overwrite)
{
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(ECONNREFUSED);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(EHOSTUNREACH);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_last_error_number=" + std::to_string(EHOSTUNREACH)), std::string::npos);
}

// Case 6: delta between two dumps is correct
TEST_F(ZmqMetricsTest, fault_counter_delta_between_dumps)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc(5);
    (void)metrics::DumpSummaryForTest();            // snapshot 1
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_FAILURE_TOTAL)).Inc(3);
    auto s = metrics::DumpSummaryForTest();         // snapshot 2
    EXPECT_NE(s.find("zmq_receive_failure_total=8"),  std::string::npos);  // total
    EXPECT_NE(s.find("zmq_receive_failure_total=+3"), std::string::npos);  // delta
}

// Case 7: zero delta — no new faults → all +0
TEST_F(ZmqMetricsTest, zero_delta_when_quiet)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc(1);
    (void)metrics::DumpSummaryForTest();
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_send_failure_total=+0"), std::string::npos);
    EXPECT_NE(s.find("zmq_receive_failure_total=+0"), std::string::npos);
    EXPECT_NE(s.find("zmq_network_error_total=+0"), std::string::npos);
}

// Case 8: Layer 2 connection event counters
TEST_F(ZmqMetricsTest, layer2_connection_event_counters)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_GATEWAY_RECREATE_TOTAL)).Inc();
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_EVENT_DISCONNECT_TOTAL)).Inc(3);
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_EVENT_HANDSHAKE_FAILURE_TOTAL)).Inc(2);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_gateway_recreate_total=1"), std::string::npos);
    EXPECT_NE(s.find("zmq_event_disconnect_total=3"), std::string::npos);
    EXPECT_NE(s.find("zmq_event_handshake_failure_total=2"), std::string::npos);
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
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_send_io_latency,count=2,avg=150us,max=200us"), std::string::npos);
    EXPECT_NE(s.find("zmq_receive_io_latency,count=1,avg=500us,max=500us"), std::string::npos);
}

// Case 12: Serialization histograms
TEST_F(ZmqMetricsTest, ser_deser_histogram_observe)
{
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_SERIALIZE_LATENCY)).Observe(10);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_SERIALIZE_LATENCY)).Observe(20);
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RPC_DESERIALIZE_LATENCY)).Observe(8);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_rpc_serialize_latency,count=2,avg=15us,max=20us"), std::string::npos);
    EXPECT_NE(s.find("zmq_rpc_deserialize_latency,count=1,avg=8us,max=8us"), std::string::npos);
}

// Case 13: periodMax resets between dumps (delta max reflects only latest period)
TEST_F(ZmqMetricsTest, histogram_period_max_reset_between_dumps)
{
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(1000);
    (void)metrics::DumpSummaryForTest();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(200);
    auto s = metrics::DumpSummaryForTest();
    // total: count=2, avg=(1000+200)/2=600, max=1000
    EXPECT_NE(s.find("zmq_send_io_latency,count=2,avg=600us,max=1000us"), std::string::npos);
    // delta: only the new observation
    EXPECT_NE(s.find("zmq_send_io_latency,count=+1,avg=200us,max=200us"), std::string::npos);
}

// Case 14: Histogram delta count increases correctly across dumps
TEST_F(ZmqMetricsTest, histogram_delta_count)
{
    for (int i = 0; i < 10; ++i) {
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_IO_LATENCY)).Observe(100);
    }
    (void)metrics::DumpSummaryForTest();
    for (int i = 0; i < 5; ++i) {
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_IO_LATENCY)).Observe(100);
    }
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_receive_io_latency,count=15,"), std::string::npos);   // total
    EXPECT_NE(s.find("zmq_receive_io_latency,count=+5,"), std::string::npos);   // delta
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
    auto s = metrics::DumpSummaryForTest();
    const auto expected = std::to_string(threads * loops);
    EXPECT_NE(s.find("zmq_send_io_latency,count=" + expected), std::string::npos);
    EXPECT_NE(s.find("zmq_send_failure_total=" + expected), std::string::npos);
}

// ── [NOOP] ───────────────────────────────────────────────────────────────────

// Case 16: Operations before Init are safe no-ops
TEST_F(ZmqMetricsTest, noop_before_init_no_crash)
{
    metrics::ResetForTest();   // clear the SetUp init
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_FAILURE_TOTAL)).Inc();
    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_IO_LATENCY)).Observe(100);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_LAST_ERROR_NUMBER)).Set(42);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_TRUE(s.empty());    // uninited → empty summary
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
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_network_error_total=8"), std::string::npos);
    EXPECT_NE(s.find("zmq_last_error_number=" + std::to_string(EHOSTUNREACH)), std::string::npos);
    EXPECT_NE(s.find("zmq_event_disconnect_total=2"), std::string::npos);
    EXPECT_NE(s.find("zmq_gateway_recreate_total=1"), std::string::npos);
}

// Case 18: Scenario — peer hang (only recv.eagain↑, net_error stays 0)
TEST_F(ZmqMetricsTest, scenario_peer_hang)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_RECEIVE_TRY_AGAIN_TOTAL)).Inc(10);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_receive_try_again_total=10"), std::string::npos);
    EXPECT_NE(s.find("zmq_network_error_total=0"),    std::string::npos);
    EXPECT_NE(s.find("zmq_send_failure_total=0"),    std::string::npos);
}

// Case 19: Scenario — HWM backpressure (send.eagain↑, net_error stays 0)
TEST_F(ZmqMetricsTest, scenario_hwm_backpressure)
{
    metrics::GetCounter(static_cast<uint16_t>(metrics::KvMetricId::ZMQ_SEND_TRY_AGAIN_TOTAL)).Inc(100);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("zmq_send_try_again_total=100"), std::string::npos);
    EXPECT_NE(s.find("zmq_network_error_total=0"),     std::string::npos);
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
    auto s = metrics::DumpSummaryForTest();
    // Verify I/O Histogram recorded
    EXPECT_NE(s.find("zmq_send_io_latency,count=100,avg=500us"), std::string::npos);
    EXPECT_NE(s.find("zmq_receive_io_latency,count=100,avg=800us"), std::string::npos);
    // Verify framework Histogram recorded (much smaller avg)
    EXPECT_NE(s.find("zmq_rpc_serialize_latency,count=100,avg=10us"), std::string::npos);
    EXPECT_NE(s.find("zmq_rpc_deserialize_latency,count=100,avg=8us"), std::string::npos);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
