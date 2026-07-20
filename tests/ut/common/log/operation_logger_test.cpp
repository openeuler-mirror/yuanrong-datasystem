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

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"
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

}  // namespace
}  // namespace datasystem

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
