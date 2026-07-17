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
 * Description: Operation audit logger unit tests.
 */
#include "datasystem/common/log/operation_logger.h"

#include <algorithm>
#include <atomic>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(cluster_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);
DS_DECLARE_bool(log_async);

namespace datasystem {
namespace {

std::string MakeTempDir()
{
    std::string pattern = "/tmp/operation_logger_ut_XXXXXX";
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    char *dir = mkdtemp(buffer.data());
    EXPECT_NE(dir, nullptr);
    return dir == nullptr ? "" : std::string(dir);
}

std::string ReadFile(const std::string &path)
{
    std::ifstream file(path);
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

class OperationLoggerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        logDir_ = MakeTempDir();
        ASSERT_FALSE(logDir_.empty());
        oldClusterName_ = FLAGS_cluster_name;
        FLAGS_cluster_name = "operation-logger-test-cluster";
        FLAGS_log_dir = logDir_;
        FLAGS_log_filename = "test_worker";
        FLAGS_log_async = false;
    }

    void TearDown() override
    {
        OperationLogger::Instance().Shutdown();
        if (!logDir_.empty()) {
            (void)RemoveAll(logDir_);
        }
        FLAGS_cluster_name = oldClusterName_;
        FLAGS_log_dir.clear();
        FLAGS_log_filename.clear();
    }

    std::string logDir_;
    std::string oldClusterName_;
};

TEST_F(OperationLoggerTest, WritesConfigChangedLine)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
    OperationLogger::Instance().LogConfigChanged("minloglevel", "0", "1");
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: minloglevel=0 --> 1"));
}

TEST_F(OperationLoggerTest, MasksSensitiveValuesInConfigChanged)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
    OperationLogger::Instance().LogConfigChanged("etcd_password", "old-secret", "new-secret");
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: etcd_password=xxx --> xxx"));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr("old-secret")));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr("new-secret")));
}

TEST_F(OperationLoggerTest, WritesOperationStartStop)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("client"));
    OperationLogger::Instance().LogOperationStop();
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr("OPERATION_START: role=client"));
    EXPECT_THAT(content, testing::HasSubstr("OPERATION_STOP: role=client"));
}

TEST_F(OperationLoggerTest, UsesStandardLogContextFields)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID("operation-logger-test-trace");
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr(" | I | operation_logger.cpp:"));
    EXPECT_THAT(content, testing::HasSubstr(" | " + std::to_string(getpid()) + ":"));
    EXPECT_THAT(content, testing::HasSubstr(" | operation-logger-test-trace | operation-logger-test-cluster |  "
                                            "OPERATION_START: role=worker"));
}

TEST_F(OperationLoggerTest, WritesCoordinatorRole)
{
    ASSERT_TRUE(OperationLogger::Instance().Init(GetLogProcessRoleName(LogProcessRole::COORDINATOR)));
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr("OPERATION_START: role=coordinator"));
    EXPECT_THAT(content, testing::HasSubstr("OPERATION_STOP: role=coordinator"));
}

TEST_F(OperationLoggerTest, MasksSensitiveFlagsInConfigInit)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
    OperationLogger::Instance().LogConfigInit("--minloglevel=0\n"
                                              "--system_access_key=secret\n"
                                              "--etcd_password=etcd-secret\n"
                                              "--etcd_key=/tmp/etcd.key\n"
                                              "--yuanrong_iam_passphrase=iam-secret\n"
                                              "--yuanrong_iam_key=/tmp/iam.key\n"
                                              "--v=1\n");
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_INIT:"));
    EXPECT_THAT(content, testing::HasSubstr("--minloglevel=0"));
    EXPECT_THAT(content, testing::HasSubstr("--system_access_key=xxx"));
    EXPECT_THAT(content, testing::HasSubstr("--etcd_password=xxx"));
    EXPECT_THAT(content, testing::HasSubstr("--etcd_key=xxx"));
    EXPECT_THAT(content, testing::HasSubstr("--yuanrong_iam_passphrase=xxx"));
    EXPECT_THAT(content, testing::HasSubstr("--yuanrong_iam_key=xxx"));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr("secret")));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr("/tmp/etcd.key")));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr("/tmp/iam.key")));
}

TEST_F(OperationLoggerTest, WritesConfigFailedLine)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
    OperationLogger::Instance().LogConfigFailed("rpc_thread_num", "not modifiable");
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_FAILED: flag 'rpc_thread_num' not modifiable"));
}

TEST_F(OperationLoggerTest, RotatedOperationLogFilesMatchLogManagerGlob)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
    const std::string rotatedName = logDir_ + "/test_worker_operation.20260101120000.log";
    {
        std::ofstream out(rotatedName, std::ios::out);
        out << "rotated operation log";
    }
    std::vector<std::string> files;
    const std::string pattern = logDir_ + "/test_worker_operation." + "*[0-9].log";
    ASSERT_TRUE(Glob(pattern, files).IsOk());
    EXPECT_EQ(files.size(), 1u);
    OperationLogger::Instance().Shutdown();
}

TEST_F(OperationLoggerTest, OperationLoggerRoleFilter)
{
    // worker logs full snapshot; client keeps only allow-listed flags.
    const std::string workerOnly = "eviction_high_watermark_ratio";   // worker-only
    const std::string masterOnly = "master_sc_thread_num";            // master-only
    const std::string common = "log_dir";                             // client allow-listed
    const std::string clientOnly = "client_slow_log_rpc_slower_than"; // client allow-listed
    const std::string snapshot =
        "--" + workerOnly + "=0.9\n"
        "--" + masterOnly + "=128\n"
        "--" + common + "=/tmp/ds_logs\n"
        "--" + clientOnly + "=5000\n";

    auto findFlagLine = [](const std::string &content, const std::string &flagName) -> std::string {
        const std::string needle = "--" + flagName + "=";
        std::istringstream ss(content);
        std::string line;
        while (std::getline(ss, line)) {
            if (line.find(needle) != std::string::npos) {
                return line;
            }
        }
        return std::string();
    };

    auto runRole = [&](const std::string &role, const std::string &logBasename) -> std::string {
        FLAGS_log_filename = logBasename;
        if (!OperationLogger::Instance().Init(role)) {
            return std::string();
        }
        OperationLogger::Instance().LogConfigInit(snapshot);
        OperationLogger::Instance().Shutdown();
        const std::string path = logDir_ + "/" + logBasename + "_operation.log";
        return ReadFile(path);
    };

    // ---- worker role: full snapshot ----
    {
        const std::string content = runRole("worker", "op_filter_worker");
        ASSERT_FALSE(content.empty()) << "worker operation log not written";
        EXPECT_FALSE(findFlagLine(content, workerOnly).empty());
        EXPECT_FALSE(findFlagLine(content, masterOnly).empty());
        EXPECT_FALSE(findFlagLine(content, common).empty());
        EXPECT_FALSE(findFlagLine(content, clientOnly).empty());
    }

    // ---- client role: only allow-listed flags kept ----
    {
        const std::string content = runRole("client", "op_filter_client");
        ASSERT_FALSE(content.empty()) << "client operation log not written";
        EXPECT_TRUE(findFlagLine(content, workerOnly).empty());
        EXPECT_TRUE(findFlagLine(content, masterOnly).empty());
        EXPECT_FALSE(findFlagLine(content, common).empty());
        EXPECT_FALSE(findFlagLine(content, clientOnly).empty());
    }
}

TEST_F(OperationLoggerTest, OperationLoggerRoleChangeFilter)
{
    // worker records every change; client records only allow-listed changes.
    // client_slow_log_rpc_slower_than -> client allow-listed
    // eviction_high_watermark_ratio   -> worker-only, not in client allow-list
    // system_access_key               -> worker-only, sensitive (masked)
    const std::string clientFlag = "client_slow_log_rpc_slower_than";
    const std::string workerFlag = "eviction_high_watermark_ratio";
    const std::string sensitiveWorkerFlag = "system_access_key";

    auto contains = [](const std::string &content, const std::string &needle) -> bool {
        return content.find(needle) != std::string::npos;
    };

    auto runRoleChanges = [&](const std::string &role, const std::string &logBasename) -> std::string {
        FLAGS_log_filename = logBasename;
        if (!OperationLogger::Instance().Init(role)) {
            return std::string();
        }
        OperationLogger::Instance().LogConfigChanged(clientFlag, "5000", "8000");
        OperationLogger::Instance().LogConfigChanged(workerFlag, "0.9", "0.8");
        OperationLogger::Instance().LogConfigChanged(sensitiveWorkerFlag, "plain_secret", "rotated_secret");
        OperationLogger::Instance().LogConfigFailed(clientFlag, "invalid value");
        OperationLogger::Instance().LogConfigFailed(workerFlag, "out of range");
        OperationLogger::Instance().Shutdown();
        const std::string path = logDir_ + "/" + logBasename + "_operation.log";
        return ReadFile(path);
    };

    // ---- worker role: record all changes (sensitive values masked) ----
    {
        const std::string content = runRoleChanges("worker", "op_change_worker");
        ASSERT_FALSE(content.empty()) << "worker operation log not written";
        EXPECT_TRUE(contains(content, "CONFIG_CHANGED: " + clientFlag + "=5000 --> 8000"));
        EXPECT_TRUE(contains(content, "CONFIG_CHANGED: " + workerFlag + "=0.9 --> 0.8"));
        EXPECT_TRUE(contains(content, "CONFIG_CHANGED: " + sensitiveWorkerFlag + "=xxx --> xxx"));
        EXPECT_FALSE(contains(content, "plain_secret"));
        EXPECT_FALSE(contains(content, "rotated_secret"));
        EXPECT_TRUE(contains(content, "CONFIG_FAILED: flag '" + clientFlag + "' invalid value"));
        EXPECT_TRUE(contains(content, "CONFIG_FAILED: flag '" + workerFlag + "' out of range"));
    }

    // ---- client role: record only allow-listed changes, drop worker/master-only ----
    {
        const std::string content = runRoleChanges("client", "op_change_client");
        ASSERT_FALSE(content.empty()) << "client operation log not written";
        EXPECT_TRUE(contains(content, "CONFIG_CHANGED: " + clientFlag + "=5000 --> 8000"));
        EXPECT_FALSE(contains(content, "CONFIG_CHANGED: " + workerFlag));
        EXPECT_FALSE(contains(content, sensitiveWorkerFlag));
        EXPECT_FALSE(contains(content, "plain_secret"));
        EXPECT_FALSE(contains(content, "rotated_secret"));
        EXPECT_TRUE(contains(content, "CONFIG_FAILED: flag '" + clientFlag + "' invalid value"));
        EXPECT_FALSE(contains(content, "CONFIG_FAILED: flag '" + workerFlag));
    }
}

TEST_F(OperationLoggerTest, OperationLevelFailuresAlwaysRecordedOnClient)
{
    // Operation-level failures (name is an operation tag, not a gflag) must always be recorded,
    // even on the client role -- the client allow-list filter must not drop them. These mirror
    // the three real call sites: API disabled by file monitor, illegal JSON, monitor path conflict.
    ASSERT_TRUE(OperationLogger::Instance().Init("client"));
    OperationLogger::Instance().LogConfigApiFailed("UpdateConfig",
        "UpdateConfig: file monitor is enabled, API disabled");
    OperationLogger::Instance().LogConfigApiFailed("UpdateConfig", "UpdateConfig: invalid JSON: ...");
    OperationLogger::Instance().LogConfigApiFailed("UpdateConfig",
        "UpdateConfig: MonitorConfigPath must be empty when using UpdateConfig API");
    // A worker-only gflag failure is still filtered out on client (unchanged LogConfigFailed behavior).
    OperationLogger::Instance().LogConfigFailed("eviction_high_watermark_ratio", "out of range");
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    const std::string content = ReadFile(path);
    ASSERT_FALSE(content.empty());
    EXPECT_NE(content.find("CONFIG_FAILED: UpdateConfig UpdateConfig: file monitor is enabled, API disabled"),
              std::string::npos);
    EXPECT_NE(content.find("CONFIG_FAILED: UpdateConfig UpdateConfig: invalid JSON"), std::string::npos);
    EXPECT_NE(content.find("CONFIG_FAILED: UpdateConfig UpdateConfig: MonitorConfigPath must be empty"),
              std::string::npos);
    // worker-only gflag failure still filtered on client.
    EXPECT_EQ(content.find("CONFIG_FAILED: flag 'eviction_high_watermark_ratio'"), std::string::npos);
}

TEST_F(OperationLoggerTest, ConcurrentShutdownInitNoRoleLoggerCrossGeneration)
{
    ASSERT_TRUE(OperationLogger::Instance().Init("worker"));

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            OperationLogger::Instance().LogConfigChanged("client_slow_log_rpc_slower_than", "2000", "3000");
            OperationLogger::Instance().LogConfigChanged("eviction_high_watermark_ratio", "0.9", "0.8");
            OperationLogger::Instance().LogConfigFailed("eviction_high_watermark_ratio", "out of range");
            OperationLogger::Instance().LogConfigApiFailed("UpdateConfig", "stress");
        }
    });
    std::thread cycler([&] {
        for (int i = 0; i < 200; ++i) {
            OperationLogger::Instance().Shutdown();
            ASSERT_TRUE(OperationLogger::Instance().Init(i % 2 == 0 ? "worker" : "client"))
                << "Init failed at cycle " << i;
        }
        stop.store(true, std::memory_order_relaxed);
    });
    writer.join();
    cycler.join();
    OperationLogger::Instance().Shutdown();

    const std::string path = logDir_ + "/test_worker_operation.log";
    EXPECT_FALSE(ReadFile(path).empty());
}

}  // namespace
}  // namespace datasystem

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
