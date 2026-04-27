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
 * ZMQ RPC queue-flow latency ST (gtest), same workload as the old REPL binary.
 * Loaded by CMake `ds_st` (no second main) and by Bazel target
 * //tests/st/common/rpc/zmq:zmq_rpc_queue_latency_repl.
 *
 * Env: ZMQ_RPC_QUEUE_LATENCY_SEC (default 5) — used by remote bazel_run.sh.
 *
 * bazel: bazel test //tests/st/common/rpc/zmq:zmq_rpc_queue_latency_repl --test_output=all
 */

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/zmq_test.stub.rpc.pb.h"
#include "datasystem/utils/status.h"

#include "zmq_test.h"

using json = nlohmann::json;

// CMake: target_compile_definitions(_ds_st_other_obj … LLT_BIN_PATH=…).
// Bazel: copts on //tests/st/common/rpc/zmq:zmq_rpc_queue_latency_repl.
#ifndef LLT_BIN_PATH
#define LLT_BIN_PATH "/tmp/ds_llt"
#endif

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_int32(log_monitor_interval_ms);
DS_DECLARE_int32(v);
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

void ClearCaseDir(const std::string &path)
{
    Status rc = datasystem::RemoveAll(path);
    if (rc.IsError()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kClearDirRetryMs));
        (void)datasystem::RemoveAll(path);
    }
}

int DurationSecFromEnv()
{
    const char *e = std::getenv("ZMQ_RPC_QUEUE_LATENCY_SEC");
    if (e != nullptr && *e != '\0') {
        const int n = std::atoi(e);
        return std::max(1, n);
    }
    return 5;
}

json DumpSummaryJson()
{
    auto summary = metrics::DumpSummaryForTest(2000);
    return summary.empty() ? json() : json::parse(summary);
}

uint64_t MetricTotalCount(const json &root, const char *histName)
{
    if (!root.contains("metrics") || !root["metrics"].is_array()) {
        return 0;
    }
    for (const auto &m : root["metrics"]) {
        if (!m.contains("name") || m["name"].get<std::string>() != histName) {
            continue;
        }
        const auto &total = m["total"];
        if (!total.is_object() || !total.contains("count")) {
            break;
        }
        const json &jc = total["count"];
        if (jc.is_number_unsigned()) {
            return jc.get<uint64_t>();
        }
        if (jc.is_number_integer()) {
            return static_cast<uint64_t>(jc.get<int64_t>());
        }
        break;
    }
    return 0;
}

}  // namespace

class ZmqRpcQueueLatencyTest : public ::testing::Test {
protected:
    std::string testCasePath_;

    ZmqRpcQueueLatencyTest()
    {
        FLAGS_alsologtostderr = true;
        FLAGS_v = 0;
        std::string caseName;
        std::string name;
        GetCurTestName(caseName, name);
        testCasePath_ = std::string(LLT_BIN_PATH) + "/ds/" + caseName + "." + name;
        FLAGS_log_dir = testCasePath_ + "/client";
        ClearCaseDir(testCasePath_);
        (void)datasystem::CreateDir(FLAGS_log_dir, true);
    }

    void SetUp() override { FLAGS_log_monitor = false; }

    void TearDown() override
    {
        metrics::ResetKvMetricsForTest();
        FLAGS_log_monitor = true;
        FLAGS_log_monitor_interval_ms = 10000;
    }
};

TEST_F(ZmqRpcQueueLatencyTest, QueueFlowSmoke)
{
    const int durationSec = DurationSecFromEnv();
    LOG(INFO) << "[zmq_queue_latency] duration_sec=" << durationSec
              << " (env ZMQ_RPC_QUEUE_LATENCY_SEC overrides default)";

    metrics::ResetKvMetricsForTest();
    auto stInit = metrics::InitKvMetrics();
    ASSERT_TRUE(stInit.IsOk()) << stInit.ToString();

    std::unique_ptr<datasystem::RpcServer> rpcServer;
    auto demo = std::make_unique<arbitrary::workspace::DemoServiceImpl>();

    datasystem::RpcServer::Builder bld;
    datasystem::RpcServiceCfg cfg;
    cfg.udsEnabled_ = false;
    constexpr int32_t kLightweightRpcThreads = 4;
    constexpr int32_t kDefaultStreamSocketNum = 8;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, kLightweightRpcThreads);
    cfg.numStreamSockets_ = kDefaultStreamSocketNum;
    cfg.hwm_ = datasystem::RPC_HEAVY_SERVICE_HWM;

    LOG(INFO) << "[zmq_queue_latency] RpcServiceCfg regular=" << cfg.numRegularSockets_
              << " stream=" << cfg.numStreamSockets_ << " hwm=" << cfg.hwm_
              << " FLAGS_rpc_thread_num=" << FLAGS_rpc_thread_num;

    bld.SetDebug().AddEndPoint("tcp://127.0.0.1:*").AddService(demo.get(), cfg);

    Status st = bld.Init(rpcServer);
    ASSERT_TRUE(st.IsOk()) << st.ToString();
    st = bld.BuildAndStart(rpcServer);
    ASSERT_TRUE(st.IsOk()) << st.ToString();

    std::vector<std::string> ports = rpcServer->GetListeningPorts();
    ASSERT_FALSE(ports.empty()) << "No listening port";
    const int port = std::stoi(ports.front());
    LOG(INFO) << "[zmq_queue_latency] server_tcp_port=" << port;

    datasystem::HostPort addr("127.0.0.1", port);
    auto channel = std::make_shared<datasystem::RpcChannel>(addr, datasystem::RpcCredential());
    auto stub = std::make_unique<arbitrary::workspace::DemoService::Stub>(channel);
    datasystem::RpcOptions opt;
    opt.SetTimeout(5000);

    for (int i = 0; i < 10; ++i) {
        arbitrary::workspace::SayHelloPb req;
        arbitrary::workspace::ReplyHelloPb rsp;
        req.set_msg("warmup");
        int64_t tagId = 0;
        auto wst = stub->SimpleGreetingAsyncWrite(opt, req, tagId);
        if (wst.IsOk()) {
            (void)stub->SimpleGreetingAsyncRead(tagId, rsp);
        }
    }

    LOG(INFO) << "[zmq_queue_latency] load_loop start for " << durationSec << "s";
    const auto start = std::chrono::steady_clock::now();
    int count = 0;
    uint64_t clientRpcRoundTripSumNs = 0;
    uint64_t clientRpcRoundTripOk = 0;
    while (true) {
        arbitrary::workspace::SayHelloPb req;
        arbitrary::workspace::ReplyHelloPb rsp;
        req.set_msg("Hello");
        int64_t tagId = 0;
        const auto rpcT0 = std::chrono::steady_clock::now();
        Status callSt = stub->SimpleGreetingAsyncWrite(opt, req, tagId);
        if (callSt.IsOk()) {
            callSt = stub->SimpleGreetingAsyncRead(tagId, rsp);
        }
        const auto rpcT1 = std::chrono::steady_clock::now();
        if (callSt.IsOk()) {
            clientRpcRoundTripSumNs += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(rpcT1 - rpcT0).count());
            ++clientRpcRoundTripOk;
        }
        ++count;
        if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() >=
            durationSec) {
            break;
        }
    }

    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    const double reqPerS =
        elapsedMs > 0 ? (count * 1000.0 / static_cast<double>(elapsedMs)) : 0.0;
    // Same line shape as legacy REPL; parse_repl_log.py consumes it.
    LOG(INFO) << "Completed " << count << " RPCs in " << elapsedMs << "ms (" << std::fixed
              << std::setprecision(1) << reqPerS << std::defaultfloat << " req/s)";
    LOG(INFO) << "[zmq_queue_latency] rpc_iters=" << count << " elapsed_ms=" << elapsedMs
              << " req_per_s=" << reqPerS;
    if (clientRpcRoundTripOk > 0U) {
        const double avgUs =
            static_cast<double>(clientRpcRoundTripSumNs) / static_cast<double>(clientRpcRoundTripOk) / 1000.0;
        LOG(INFO) << "[zmq_queue_latency] client_wall_async_ok=" << clientRpcRoundTripOk << " avg_us=" << std::fixed
                  << std::setprecision(3) << avgUs << std::defaultfloat;
    }

    json dump = DumpSummaryJson();
    ASSERT_FALSE(dump.empty()) << "metrics dump empty";
    // Marker for parse_repl_log.py (expects raw JSON object after the line).
    LOG(INFO) << "=== METRICS DUMP ===\n" << dump.dump();
    LOG(INFO) << "[zmq_queue_latency] metrics_summary (same as dump above)";

    const uint64_t qCnt = MetricTotalCount(dump, "zmq_client_queuing_latency");
    const uint64_t e2eCnt = MetricTotalCount(dump, "zmq_rpc_e2e_latency");
    LOG(INFO) << "[zmq_queue_latency] histogram_counts queuing=" << qCnt << " e2e=" << e2eCnt;

    EXPECT_GT(qCnt, 0u) << "expect zmq_client_queuing_latency samples after load";
    EXPECT_GT(e2eCnt, 0u) << "expect zmq_rpc_e2e_latency samples after load";

    stub.reset();
    channel.reset();
    rpcServer->Shutdown();
    rpcServer.reset();
    demo.reset();
}

}  // namespace st
}  // namespace datasystem
