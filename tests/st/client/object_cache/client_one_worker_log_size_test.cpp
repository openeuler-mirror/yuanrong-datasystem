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

#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object_client.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
namespace {
uint64_t GetLogBytesByPattern(const std::string &pattern)
{
    std::vector<std::string> files;
    if (Glob(pattern, files).IsError()) {
        return 0;
    }

    uint64_t totalBytes = 0;
    for (const auto &file : files) {
        bool isDir = false;
        if (IsDirectory(file, isDir).IsOk() && isDir) {
            continue;
        }
        auto size = FileSize(file, false);
        if (size > 0) {
            totalBytes += static_cast<uint64_t>(size);
        }
    }
    return totalBytes;
}

uint64_t GetClientLogBytes()
{
    return GetLogBytesByPattern(FormatString("%s/*", FLAGS_log_dir.c_str()));
}

uint64_t GetWorkerLogBytes(const std::string &rootDir, uint32_t workerIndex)
{
    return GetLogBytesByPattern(FormatString("%s/worker%d/log/*", rootDir, workerIndex));
}
}  // namespace

class OCClientOneWorkerLogSizeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.masterIdx = 0;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = "-shared_memory_size_mb=128 -v=2 -log_monitor=true -log_rate_limit=0";
    }
};

TEST_F(OCClientOneWorkerLogSizeTest, DISABLED_OneMinuteMissingKeyGetForErrorWarningLogGrowth)
{
    constexpr uint32_t workerIndex = 0;
    constexpr int32_t connectTimeoutMs = 5000;
    constexpr int32_t getTimeoutMs = 200;
    constexpr double targetQps = 35.0;
    constexpr int64_t runSeconds = 60;
    constexpr int64_t flushWaitMs = 2000;

    std::shared_ptr<ObjectClient> client;
    InitTestClient(workerIndex, client, connectTimeoutMs);

    const std::string missingKey = "missing_key_" + GetStringUuid();
    const std::string rootDir = cluster_->GetRootDir();
    const uint64_t clientBefore = GetClientLogBytes();
    const uint64_t workerBefore = GetWorkerLogBytes(rootDir, workerIndex);

    Timer timer;
    const double intervalSec = 1.0 / targetQps;
    double nextTickSec = 0.0;
    uint64_t totalGets = 0;
    uint64_t failedGets = 0;

    while (timer.ElapsedSecond() < runSeconds) {
        TraceGuard requestTrace = Trace::Instance().SetRequestTraceUUID();
        std::vector<Optional<Buffer>> buffers;
        Status rc = client->Get({ missingKey }, getTimeoutMs, buffers);
        ++totalGets;
        if (rc.IsError()) {
            ++failedGets;
            LOG(WARNING) << FormatString("Expected get failure for missing key=%s, rc=%s", missingKey, rc.ToString());
        } else {
            LOG(ERROR) << FormatString("Unexpected get success for missing key=%s", missingKey);
        }
        nextTickSec += intervalSec;
        const double elapsedSec = timer.ElapsedMilliSecond() / 1000.0;
        const double sleepSec = nextTickSec - elapsedSec;
        if (sleepSec > 0) {
            std::this_thread::sleep_for(std::chrono::duration<double>(sleepSec));
        } else if (sleepSec < -intervalSec * 10) {
            nextTickSec = elapsedSec;
        }
    }

    EXPECT_GT(totalGets, 0UL);
    EXPECT_GT(failedGets, 0UL);

    LOG(ERROR) << FormatString("One-minute run done, totalGets=%ld, failedGets=%ld", totalGets, failedGets);
    std::this_thread::sleep_for(std::chrono::milliseconds(flushWaitMs));

    const uint64_t clientAfter = GetClientLogBytes();
    const uint64_t workerAfter = GetWorkerLogBytes(rootDir, workerIndex);
    const int64_t clientDelta = static_cast<int64_t>(clientAfter) - static_cast<int64_t>(clientBefore);
    const int64_t workerDelta = static_cast<int64_t>(workerAfter) - static_cast<int64_t>(workerBefore);

    LOG(INFO) << FormatString(
        "log_size_result client_before=%ldB client_after=%ldB client_delta=%ldB worker_before=%ldB worker_after=%ldB "
        "worker_delta=%ldB totalGets=%ld failedGets=%ld target_qps=%.2f actual_qps=%.2f",
        clientBefore, clientAfter, clientDelta, workerBefore, workerAfter, workerDelta, totalGets, failedGets,
        targetQps, runSeconds > 0 ? static_cast<double>(totalGets) / static_cast<double>(runSeconds) : 0.0);
}

}  // namespace st
}  // namespace datasystem
