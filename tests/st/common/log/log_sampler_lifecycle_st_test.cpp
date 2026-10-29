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
 * Description: System-level guard that the SDK lifecycle Init API is excluded
 * from request log sampling (issue #1174).
 *
 * KVClient::Init() used to create its scope with SetRequestTraceUUID(), so the
 * RegisterClient RPC carried LOG_SAMPLE_UNDECIDED and the worker-side handler
 * made its own request_sample_rate decision. With request_sample_rate=0.01 the
 * worker INFO log "Register client ..." was rejected (~99% of the time) and
 * client onboarding became undiagnosable.
 *
 * Init now uses a plain SetTraceUUID() trace: the RPC carries
 * LOG_SAMPLE_NONE, the worker handler is background-classified, and the
 * Register client log must always be emitted regardless of sample rates. This
 * test pins request_sample_rate=0 so a regression back to request traces fails
 * deterministically. The ShutDown part of the test is a lifecycle-chain
 * completeness check (see the comment above the test body).
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/kv_client.h"

namespace datasystem {
namespace st {

namespace {
// The worker's async logger can hold an idle worker's log batch for ~5s before
// flushing it to file (observed: disconnect log lines generated at T were only
// flushed at T+5s when the next worker activity arrived). The retry window must
// comfortably exceed that idle-flush latency, otherwise the assertions below
// race the flush and fail spuriously.
constexpr int kLogRetryTimes = 150;
constexpr auto kLogRetryInterval = std::chrono::milliseconds(100);

bool FileContainsToken(const std::filesystem::path &path, const std::string &token)
{
    std::ifstream input(path);
    if (!input.is_open()) {
        return false;
    }
    std::string line;
    while (std::getline(input, line)) {
        if (line.find(token) != std::string::npos) {
            return true;
        }
    }
    return false;
}

bool WorkerLogDirContainsToken(const std::string &logDir, const std::string &token)
{
    std::error_code ec;
    if (!std::filesystem::exists(logDir, ec)) {
        return false;
    }
    for (const auto &entry : std::filesystem::directory_iterator(logDir, ec)) {
        if (ec || !entry.is_regular_file()) {
            continue;
        }
        if (FileContainsToken(entry.path(), token)) {
            return true;
        }
    }
    return false;
}

// Assert that the worker INFO log eventually contains `token`. Worker runtime
// logs are written by the worker's async logger, so poll until the flush lands.
void AssertWorkerLogContains(const std::string &logDir, const std::string &token)
{
    for (int i = 0; i < kLogRetryTimes; ++i) {
        if (WorkerLogDirContainsToken(logDir, token)) {
            SUCCEED();
            return;
        }
        std::this_thread::sleep_for(kLogRetryInterval);
    }
    FAIL() << "worker log dir " << logDir << " does not contain token '" << token
           << "' after " << kLogRetryTimes << " retries — lifecycle API logs were dropped by request log sampling";
}
}  // namespace

class LogSamplerLifecycleStTest : public OCClientCommon {
public:
    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client_);
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        // request_sample_rate=0 makes the pre-fix failure deterministic: a
        // request-sampled log is dropped with certainty, so no 1% random
        // false-pass like with the 0.01 rates of issue #1174. access and
        // diagnostic keep 0.01; they are not asserted here.
        opts.workerGflagParams = "-shared_memory_size_mb=25"
            " -log_monitor=true -log_monitor_interval_ms=1000"
            " -request_sample_rate=0 -access_sample_rate=0.01 -diagnostic_sample_rate=0.01";
    }

    std::string WorkerLogDir()
    {
        return FormatString("%s/worker0/log", cluster_->GetRootDir());
    }

    std::shared_ptr<KVClient> client_;
};

// LS-lifecycle guard for issue #1174.
//
// The "Register client" assertion is the sampling-fix guard: with the fix
// reverted, KVClient::Init() creates a request trace and the RegisterClient RPC
// carries UNDECIDED (no client-side log precedes the RPC), so the worker makes
// its own request_sample_rate decision. With request_sample_rate=0 that decision
// is REJECT, the worker-side "Register client" INFO log is dropped, and the
// assertion fails deterministically. With the fix (SetTraceUUID) the RPC carries
// LOG_SAMPLE_NONE and the log is always emitted.
//
// The "disconnect client:"/"Remove client" assertions are lifecycle-chain
// completeness checks, NOT sampling-fix guards: the ShutDown flow resets the
// thread's request-log state before the DisconnectClient RPC is sent (verified
// empirically — even with a SetRequestTraceUUID guard on ShutDown the RPC state
// is NONE), so these worker-side logs do not depend on the ShutDown guard.
// They verify the public ShutDown() entrypoint works end to end: the worker
// handler runs AfterClientLostHandler -> ClientManager::RemoveClient and emits
// its INFO logs.
TEST_F(LogSamplerLifecycleStTest, InitLogsSurviveRequestSampling)
{
    AssertWorkerLogContains(WorkerLogDir(), "Register client");

    // Exercise the public ShutDown() entrypoint (client_.reset() alone runs the
    // destructor chain, which skips the trace guard and cannot cover it).
    DS_ASSERT_OK(client_->ShutDown());
    AssertWorkerLogContains(WorkerLogDir(), "disconnect client:");
    AssertWorkerLogContains(WorkerLogDir(), "Remove client");
    client_.reset();
}

}  // namespace st
}  // namespace datasystem
