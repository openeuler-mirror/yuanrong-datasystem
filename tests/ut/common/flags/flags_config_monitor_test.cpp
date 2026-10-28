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
 * Description: Wiki §3 config file monitor and operation log unit tests.
 */
#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/cluster/coordination_timing.h"
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"
#include "datasystem/common/flags/dynamic_flag_config.h"

#include <chrono>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "ut/common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/log_manager.h"
#define private public
#define protected public
#include "datasystem/common/log/logging.h"
#undef protected
#undef private
#include "datasystem/common/util/file_util.h"

DS_DECLARE_int32(v);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);
DS_DECLARE_int32(logbufsecs);

namespace datasystem {
namespace ut {
namespace {

std::string ReadFile(const std::string &path)
{
    std::ifstream file(path);
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

size_t CountOccurrences(const std::string &content, const std::string &target)
{
    size_t count = 0;
    for (size_t pos = 0; (pos = content.find(target, pos)) != std::string::npos; pos += target.size()) {
        ++count;
    }
    return count;
}

void WriteConfigFile(const std::string &path, const std::string &content)
{
    std::ofstream out(path, std::ios::trunc);
    out << content;
    out.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

}  // namespace

class FlagsConfigMonitorTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        Logging::ResetLoggingRuntimeForTest();
        FLAGS_log_filename = "flags_cfg_mon";
        FLAGS_logbufsecs = 0;
        FLAGS_v = 0;
        FLAGS_minloglevel = 0;
        FLAGS_node_timeout_s = 60;
        configPath_ = FLAGS_log_dir + "/datasystem.config";
        Logging::GetInstance()->Start("flags_cfg_mon", LogProcessRole::WORKER);
    }

    void TearDown() override
    {
        Logging::ResetLoggingRuntimeForTest();
        CommonTest::TearDown();
    }

    std::string configPath_;
};

TEST_F(FlagsConfigMonitorTest, MissingConfigFileLogsWarningOnce)
{
    DynamicFlagConfig flagConfig;
    const std::string missingPath = FLAGS_log_dir + "/missing-datasystem.config";
    flagConfig.StartConfigFileHandle(missingPath, std::chrono::steady_clock::now());
    flagConfig.StartConfigFileHandle(missingPath, std::chrono::steady_clock::now());
    const std::string operationLog = OperationLogger::Instance().OperationLogPath();
    Logging::ResetLoggingRuntimeForTest();

    const std::string infoLog = FLAGS_log_dir + "/" + FLAGS_log_filename + ".INFO.log";
    const std::string content = ReadFile(infoLog);
    EXPECT_EQ(CountOccurrences(content, "Monitor config file does not exist"), 1ul);
    EXPECT_EQ(CountOccurrences(content, "missing-datasystem.config"), 1ul);

    if (!operationLog.empty()) {
        const std::string opContent = ReadFile(operationLog);
        EXPECT_THAT(opContent, testing::Not(testing::HasSubstr("CONFIG_FAILED")));
    }
}

TEST_F(FlagsConfigMonitorTest, ConfigFileChangeAppliesModifiableFlag)
{
    DynamicFlagConfig flagConfig;
    WriteConfigFile(configPath_, "-v=2\n");
    flagConfig.StartConfigFileHandle(configPath_, std::chrono::steady_clock::now());
    EXPECT_EQ(FLAGS_v, 2);

    WriteConfigFile(configPath_, "-v=3\n");
    flagConfig.StartConfigFileHandle(configPath_, std::chrono::steady_clock::now());
    EXPECT_EQ(FLAGS_v, 3);

    const std::string operationLog = OperationLogger::Instance().OperationLogPath();
    Logging::ResetLoggingRuntimeForTest();
    const std::string content = ReadFile(operationLog);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: v=2 --> 3"));
}

TEST_F(FlagsConfigMonitorTest, InvalidFlagValueRecordsConfigFailed)
{
    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-minloglevel=invalid");
    ASSERT_EQ(flagMap.count("minloglevel"), 1ul);
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsError());

    const std::string operationLog = OperationLogger::Instance().OperationLogPath();
    Logging::ResetLoggingRuntimeForTest();
    const std::string content = ReadFile(operationLog);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_FAILED: flag 'minloglevel'"));
}

TEST_F(FlagsConfigMonitorTest, NonModifiableFlagRecordsConfigFailed)
{
    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-node_timeout_s=32");
    EXPECT_EQ(flagMap.count("node_timeout_s"), 0ul);

    std::pair<std::string, std::string> kv;
    EXPECT_FALSE(flagConfig.SplitArgument("--node_timeout_s=32", kv));

    const std::string operationLog = OperationLogger::Instance().OperationLogPath();
    Logging::ResetLoggingRuntimeForTest();
    const std::string content = ReadFile(operationLog);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_FAILED: flag 'node_timeout_s' not modifiable"));
}

TEST_F(FlagsConfigMonitorTest, MultipleFlagsRecordedSeparately)
{
    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-v=1\n-minloglevel=2");
    EXPECT_EQ(flagMap.size(), 2ul);
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());

    const std::string operationLog = OperationLogger::Instance().OperationLogPath();
    Logging::ResetLoggingRuntimeForTest();
    const std::string content = ReadFile(operationLog);
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: v="));
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: minloglevel="));
}

TEST_F(FlagsConfigMonitorTest, KeepAliveIntervalUsesBoundedNodeTimeoutFraction)
{
    EXPECT_EQ(GetMembershipKeepAliveIntervalMs(0), 100);
    EXPECT_EQ(GetMembershipKeepAliveIntervalMs(3), 1'000);
    EXPECT_EQ(GetMembershipKeepAliveIntervalMs(15), 5'000);
    EXPECT_EQ(GetMembershipKeepAliveIntervalMs(60), 5'000);
    EXPECT_EQ(GetMembershipKeepAliveIntervalMs(std::numeric_limits<uint32_t>::max()), 5'000);
}

TEST_F(FlagsConfigMonitorTest, EtcdKeepAliveDerivesIntervalFromNodeTimeout)
{
    EtcdKeepAlive keepAlive("", 0);
    EXPECT_EQ(keepAlive.GetLeaseRenewIntervalMs(), 5'000);
}

}  // namespace ut
}  // namespace datasystem
