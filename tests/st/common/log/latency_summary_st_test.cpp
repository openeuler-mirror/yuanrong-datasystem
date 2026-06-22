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
 * Description: System-level tests for latencySummary in access_log.
 * Verifies that client and worker access.log contain latencySummary
 * when slow_log thresholds are set to 1us (force all requests to print).
 * Client-side tests flush via DoLogMonitorWrite() (same process).
 * Worker-side test uses graceful shutdown to flush buffered access logs.
 */
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_manager.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/kv_client.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

static std::string ReadFileContent(const std::string &path)
{
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        return "";
    }
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}

static bool FindLogLineWithTokens(const std::string &path, const std::vector<std::string> &tokens,
                                  std::string &matchedLine)
{
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        return false;
    }
    std::string line;
    while (std::getline(ifs, line)) {
        bool allMatched = true;
        for (const auto &token : tokens) {
            if (line.find(token) == std::string::npos) {
                allMatched = false;
                break;
            }
        }
        if (allMatched) {
            matchedLine = line;
            return true;
        }
    }
    return false;
}

static void AssertAccessLogContains(const std::string &path, const std::vector<std::string> &tokens)
{
    constexpr int retryTimes = 50;
    constexpr auto retryInterval = std::chrono::milliseconds(100);
    std::string matchedLine;
    for (int i = 0; i < retryTimes; ++i) {
        DS_ASSERT_OK(LogManager::DoLogMonitorWrite());
        if (FindLogLineWithTokens(path, tokens, matchedLine)) {
            LOG(INFO) << "Found matched line: " << matchedLine;
            return;
        }
        std::this_thread::sleep_for(retryInterval);
    }
    FAIL() << "failed to find expected access log line in " << path << "\nexpected tokens: "
           << VectorToString(tokens) << "\nfile content:\n"
           << ReadFileContent(path);
}

static void AssertFileContains(const std::string &path, const std::vector<std::string> &tokens)
{
    constexpr int retryTimes = 5;
    constexpr auto retryInterval = std::chrono::milliseconds(100);
    std::string matchedLine;
    for (int i = 0; i < retryTimes; ++i) {
        if (FindLogLineWithTokens(path, tokens, matchedLine)) {
            LOG(INFO) << "Found matched line: " << matchedLine;
            return;
        }
        std::this_thread::sleep_for(retryInterval);
    }
    FAIL() << "failed to find expected line in " << path << "\nexpected tokens: "
           << VectorToString(tokens) << "\nfile content:\n"
           << ReadFileContent(path);
}

class LatencySummaryStTest : public OCClientCommon {
public:
    std::shared_ptr<KVClient> client_;
    static std::string clientLogDir_;

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        Logging::SetClientLogWithoutPid(true);
        if (clientLogDir_.empty()) {
            clientLogDir_ = FLAGS_log_dir;
        }
        std::string errMsg;
        SetCommandLineOption("slow_log_process_slower_than", std::string("1"), errMsg);
        SetCommandLineOption("slow_log_rpc_slower_than", std::string("1"), errMsg);
        InitTestKVClient(0, client_);
    }

    void TearDown() override
    {
        client_.reset();
        std::string errMsg;
        SetCommandLineOption("slow_log_process_slower_than", std::string("0"), errMsg);
        SetCommandLineOption("slow_log_rpc_slower_than", std::string("0"), errMsg);
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.skipWorkerPreShutdown = false;
        opts.workerGflagParams = "-slow_log_process_slower_than=1 -slow_log_rpc_slower_than=1 "
            "-log_monitor=true -log_monitor_interval_ms=1000 -shared_memory_size_mb=25 "
            "-node_timeout_s=5 -node_dead_timeout_s=8 -v=1";
    }

    std::string ClientAccessLogPath()
    {
        return clientLogDir_ + "/ds_client_access.log";
    }

    std::string WorkerAccessLogPath()
    {
        return FormatString("%s/worker0/log/access.log", cluster_->GetRootDir());
    }
};

std::string LatencySummaryStTest::clientLogDir_;

TEST_F(LatencySummaryStTest, GetAccessLogLatencySummary)
{
    const std::string key = ObjectKey();
    const std::string val = "value_for_get_test";
    DS_ASSERT_OK(client_->Set(key, val));

    std::string out;
    DS_ASSERT_OK(client_->Get(key, out));

    AssertAccessLogContains(ClientAccessLogPath(), { "latencySummary:{", "client.rpc.get:" });
}

TEST_F(LatencySummaryStTest, SetAccessLogLatencySummary)
{
    const std::string key = ObjectKey();
    const std::string val = "value_for_set_test";
    DS_ASSERT_OK(client_->Set(key, val));

    AssertAccessLogContains(ClientAccessLogPath(), { "latencySummary:{", "client.rpc.publish:" });
}

TEST_F(LatencySummaryStTest, CreateAccessLogLatencySummary)
{
    const std::string key = ObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client_->Create(key, 64, SetParam{}, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    const std::string val = "value_for_create_test";
    DS_ASSERT_OK(buffer->MemoryCopy(val.data(), val.size()));
    DS_ASSERT_OK(buffer->Seal());
    DS_ASSERT_OK(buffer->UnWLatch());

    AssertAccessLogContains(ClientAccessLogPath(), { "latencySummary:{", "client.process.create:" });
}

TEST_F(LatencySummaryStTest, ExistAccessLogLatencySummary)
{
    const std::string key = ObjectKey();
    const std::string val = "value_for_exist_test";
    DS_ASSERT_OK(client_->Set(key, val));

    std::vector<bool> exists;
    DS_ASSERT_OK(client_->Exist({ key }, exists));

    AssertAccessLogContains(ClientAccessLogPath(), { "latencySummary:{", "client.rpc.exist:" });
}

TEST_F(LatencySummaryStTest, WorkerAccessLogLatencySummary)
{
    const std::string setKey = ObjectKey();
    DS_ASSERT_OK(client_->Set(setKey, "value_for_worker_set"));

    std::string out;
    DS_ASSERT_OK(client_->Get(setKey, out));

    const std::string createKey = ObjectKey();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client_->Create(createKey, 64, SetParam{}, buffer));
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(buffer->MemoryCopy("value_for_worker_create", 22));
    DS_ASSERT_OK(buffer->Seal());
    DS_ASSERT_OK(buffer->UnWLatch());

    std::vector<bool> exists;
    DS_ASSERT_OK(client_->Exist({ setKey }, exists));

    client_.reset();

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));

    std::string workerLog = WorkerAccessLogPath();
    AssertFileContains(workerLog, { "latencySummary:{", "worker.process.publish:" });
    AssertFileContains(workerLog, { "latencySummary:{", "worker.process.get:" });
    AssertFileContains(workerLog, { "latencySummary:{", "worker.process.publish:" });
    AssertFileContains(workerLog, { "latencySummary:{", "worker.process.exist:" });
}

static void AssertAccessLogNotContains(const std::string &path, const std::vector<std::string> &tokens)
{
    constexpr int retryTimes = 50;
    constexpr auto retryInterval = std::chrono::milliseconds(100);
    for (int i = 0; i < retryTimes; ++i) {
        DS_ASSERT_OK(LogManager::DoLogMonitorWrite());
        std::ifstream ifs(path);
        if (!ifs.is_open()) {
            std::this_thread::sleep_for(retryInterval);
            continue;
        }
        std::string line;
        bool foundAnyKeyLine = false;
        bool foundForbiddenToken = false;
        while (std::getline(ifs, line)) {
            bool hasAllKeyTokens = true;
            for (size_t t = 0; t + 1 < tokens.size(); ++t) {
                if (line.find(tokens[t]) == std::string::npos) {
                    hasAllKeyTokens = false;
                    break;
                }
            }
            if (hasAllKeyTokens) {
                foundAnyKeyLine = true;
                if (line.find(tokens.back()) != std::string::npos) {
                    foundForbiddenToken = true;
                }
            }
        }
        if (foundAnyKeyLine && !foundForbiddenToken) {
            return;
        }
        if (foundAnyKeyLine && foundForbiddenToken) {
            FAIL() << "Found forbidden token in access log when it should not be present";
        }
        std::this_thread::sleep_for(retryInterval);
    }
}

TEST_F(LatencySummaryStTest, DefaultDisabledNoLatencySummaryInAccessLog)
{
    std::string errMsg;
    SetCommandLineOption("slow_log_process_slower_than", std::string("0"), errMsg);
    SetCommandLineOption("slow_log_rpc_slower_than", std::string("0"), errMsg);

    const std::string key = ObjectKey() + "_disabled";
    DS_ASSERT_OK(client_->Set(key, "val_disabled"));

    std::string out;
    DS_ASSERT_OK(client_->Get(key, out));

    AssertAccessLogNotContains(ClientAccessLogPath(), { key, "latencySummary:" });

    SetCommandLineOption("slow_log_process_slower_than", std::string("1"), errMsg);
    SetCommandLineOption("slow_log_rpc_slower_than", std::string("1"), errMsg);
}

TEST_F(LatencySummaryStTest, GetTimeoutWorkerAccessLogContainsLatencySummary)
{
    const std::string key = ObjectKey() + "_timeout";
    DS_ASSERT_OK(client_->Set(key, "val_timeout"));

    std::string out;
    client_->Get(key, out, 1);

    client_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));

    std::string workerLog = WorkerAccessLogPath();
    AssertFileContains(workerLog, { "latencySummary:{", "worker.process.get:" });
}

}  // namespace st
}  // namespace datasystem
