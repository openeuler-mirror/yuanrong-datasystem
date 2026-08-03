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

/** Description: Tests deterministic transport Get slow-phase summaries. */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "datasystem/client/transport/transport_phase_latency_recorder.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_bool(alsologtostderr);

namespace datasystem {
namespace client {
namespace {
constexpr uint64_t PROCESS_THRESHOLD_US = 100;
constexpr uint64_t RPC_THRESHOLD_US = 200;
constexpr uint64_t PROCESS_BELOW_THRESHOLD_US = PROCESS_THRESHOLD_US - 1;
constexpr uint64_t RPC_BELOW_THRESHOLD_US = RPC_THRESHOLD_US - 1;
constexpr char PHASE_LOG_MARKER[] = "[TransportGet] Phase latency";

std::string CapturePhaseSummary(uint64_t processUs, uint64_t rpcUs)
{
    testing::internal::CaptureStderr();
    {
        TransportPhaseLatencyRecorder recorder(HostPort("127.0.0.1", 1));
        recorder.RecordPhase("connection_read_lock_wait", processUs, TransportLatencyThreshold::PROCESS);
        recorder.RecordPhase("data_transfer", rpcUs, TransportLatencyThreshold::RPC);
    }
    return testing::internal::GetCapturedStderr();
}

void ExpectSingleSummary(const std::string &output)
{
    const size_t first = output.find(PHASE_LOG_MARKER);
    ASSERT_NE(first, std::string::npos) << output;
    EXPECT_EQ(output.find(PHASE_LOG_MARKER, first + 1), std::string::npos) << output;
}
}  // namespace

TEST(TransportPhaseLatencyRecorderTest, AppliesThresholdsAndFormatsOneSummary)
{
    const uint64_t oldProcessThreshold = FLAGS_client_slow_log_process_slower_than;
    const uint64_t oldRpcThreshold = FLAGS_client_slow_log_rpc_slower_than;
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlags([oldProcessThreshold, oldRpcThreshold, oldAlsoLogToStderr] {
        FLAGS_client_slow_log_process_slower_than = oldProcessThreshold;
        FLAGS_client_slow_log_rpc_slower_than = oldRpcThreshold;
        FLAGS_alsologtostderr = oldAlsoLogToStderr;
    });
    FLAGS_client_slow_log_process_slower_than = PROCESS_THRESHOLD_US;
    FLAGS_client_slow_log_rpc_slower_than = RPC_THRESHOLD_US;
    FLAGS_alsologtostderr = true;

    const std::string rpcSlow = CapturePhaseSummary(PROCESS_BELOW_THRESHOLD_US, RPC_THRESHOLD_US);
    const std::string processSlow = CapturePhaseSummary(PROCESS_THRESHOLD_US, RPC_BELOW_THRESHOLD_US);
    const std::string allFast = CapturePhaseSummary(PROCESS_BELOW_THRESHOLD_US, RPC_BELOW_THRESHOLD_US);

    ExpectSingleSummary(rpcSlow);
    EXPECT_NE(rpcSlow.find("slowPhases=[data_transfer]"), std::string::npos) << rpcSlow;
    EXPECT_NE(rpcSlow.find("phasesUs={connection_read_lock_wait:99,data_transfer:200}"), std::string::npos) << rpcSlow;
    ExpectSingleSummary(processSlow);
    EXPECT_NE(processSlow.find("slowPhases=[connection_read_lock_wait]"), std::string::npos) << processSlow;
    EXPECT_NE(processSlow.find("phasesUs={connection_read_lock_wait:100,data_transfer:199}"), std::string::npos)
        << processSlow;
    EXPECT_EQ(allFast.find(PHASE_LOG_MARKER), std::string::npos) << allFast;
}

}  // namespace client
}  // namespace datasystem
