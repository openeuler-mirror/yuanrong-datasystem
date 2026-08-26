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
 * Description: System-level guard that brpc transport actually applies
 * request/access log sampling on the worker side.
 *
 * Regression guard for the bug where the generated brpc CallMethod prologue
 * only stripped the traceID from the request attachment and never restored
 * requestLogTrace / log_sample_state, so Trace::IsRequestLogTrace() stayed
 * false on the worker, LogSampler::ClassifyRuntime() returned BYPASS, and
 * 100% of access/request logs were recorded regardless of *_sample_rate.
 *
 * With request_sample_rate=0.0 + access_sample_rate=0.0, a
 * successful Set/Get must NOT produce an access.log line for that key on the
 * worker (the request is rejected and access is sampled out). Before the fix
 * this assertion fails because BYPASS forces every access log to be emitted.
 */

#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_manager.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/kv_client.h"


namespace datasystem {
namespace st {

namespace {
constexpr int kLogRetryTimes = 50;
constexpr auto kLogRetryInterval = std::chrono::milliseconds(100);

std::string ReadFileContent(const std::string &path)
{
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        return "";
    }
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}

bool LogFileContainsToken(const std::string &path, const std::string &token)
{
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        return false;
    }
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.find(token) != std::string::npos) {
            return true;
        }
    }
    return false;
}

// Assert that `path` does NOT contain any line with `token`, retrying while the
// access log buffer flushes. Fails if the token appears (i.e. sampling failed
// to drop the access record).
void AssertAccessLogDoesNotContain(const std::string &path, const std::string &token)
{
    for (int i = 0; i < kLogRetryTimes; ++i) {
        DS_ASSERT_OK(LogManager::DoLogMonitorWrite());
        if (LogFileContainsToken(path, token)) {
            FAIL() << "access log " << path << " unexpectedly contains token '" << token
                   << "' — sampling did not drop the record.\nfile content:\n"
                   << ReadFileContent(path);
        }
        std::this_thread::sleep_for(kLogRetryInterval);
    }
    SUCCEED();
}
}  // namespace

class LogSamplerBrpcStTest : public OCClientCommon {
public:
    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        // Client SDK uses brpc. The
        // client and worker speak the same transport (the worker is started
        // with below).
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
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        // All three sample rates = 0.0 so the request is rejected and access is
        // sampled out: a successful Set/Get must leave NO access.log line for that
        // key on the worker once the fix propagates log_sample_state over brpc.
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1"
            " -log_monitor=true -log_monitor_interval_ms=1000"
            " -request_sample_rate=0.0 -access_sample_rate=0.0 -diagnostic_sample_rate=0.0";
    }

    std::string WorkerAccessLogPath()
    {
        return FormatString("%s/worker0/log/access.log", cluster_->GetRootDir());
    }

    std::vector<std::string> workerAddress_;
    std::shared_ptr<KVClient> client_;
    bool prevUseBrpc_ = false;
};

// LS-brpc: under brpc + request/access_sample_rate=0.0, a successful Set+Get
// must not emit a worker access.log line carrying the (unique) object key.
// Before the fix this fails: requestLogTrace stays false -> BYPASS -> all
// access records emitted.
TEST_F(LogSamplerBrpcStTest, BrpcSamplingDropsWorkerAccessLog)
{
    const std::string key = ObjectKey();
    DS_ASSERT_OK(client_->Set(key, "v"));
    std::string out;
    DS_ASSERT_OK(client_->Get(key, out));

    // Worker access log is buffered; flush via graceful shutdown before
    // inspecting (same pattern as LatencySummaryStTest::WorkerAccessLog...).
    client_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));

    AssertAccessLogDoesNotContain(WorkerAccessLogPath(), key);
}

}  // namespace st
}  // namespace datasystem
