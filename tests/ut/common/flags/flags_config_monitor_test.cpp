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
#include "datasystem/common/kvstore/etcd/etcd_keep_alive.h"
#include "datasystem/common/flags/dynamic_flag_config.h"

#include <chrono>
#include <fstream>
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
#include "datasystem/common/log/spdlog/provider.h"

DS_DECLARE_int32(v);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_int32(heartbeat_interval_ms);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);
DS_DECLARE_bool(log_async);
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

void WriteConfigFile(const std::string &path, const std::string &content)
{
    std::ofstream out(path, std::ios::trunc);
    out << content;
    out.close();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

void ResetOperationLogger()
{
    OperationLogger::Instance().Shutdown();
}

}  // namespace

class FlagsConfigMonitorTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        ResetOperationLogger();
        if (Logging::GetInstance()->IsLoggingInitialized()) {
            Logging::GetInstance()->logManager_->Stop();
            Logging::GetInstance()->SetLoggingInitialized(false);
            Logging::GetInstance()->init_ = false;
        }
        FLAGS_log_filename = "flags_cfg_mon";
        FLAGS_log_async = false;
        FLAGS_logbufsecs = 0;
        FLAGS_v = 0;
        FLAGS_minloglevel = 0;
        FLAGS_heartbeat_interval_ms = 1000;
        FLAGS_node_timeout_s = 60;
        configPath_ = FLAGS_log_dir + "/datasystem.config";
        ASSERT_TRUE(OperationLogger::Instance().Init("worker"));
        Logging::GetInstance()->Start("flags_cfg_mon", LogProcessRole::WORKER);
    }

    void TearDown() override
    {
        OperationLogger::Instance().LogOperationStop();
        ResetOperationLogger();
        if (Logging::GetInstance()->IsLoggingInitialized()) {
            Logging::GetInstance()->logManager_->Stop();
            Logging::GetInstance()->SetLoggingInitialized(false);
            Logging::GetInstance()->init_ = false;
        }
        CommonTest::TearDown();
    }

    std::string configPath_;
};

TEST_F(FlagsConfigMonitorTest, MissingConfigFileLogsWarning)
{
    DynamicFlagConfig flagConfig;
    const std::string missingPath = FLAGS_log_dir + "/missing-datasystem.config";
    flagConfig.StartConfigFileHandle(missingPath, std::chrono::steady_clock::now());
    Provider::Instance().FlushLogs();

    const std::string infoLog = FLAGS_log_dir + "/" + FLAGS_log_filename + ".INFO.log";
    const std::string content = ReadFile(infoLog);
    EXPECT_THAT(content, testing::HasSubstr("Monitor config file does not exist"));
    EXPECT_THAT(content, testing::HasSubstr("missing-datasystem.config"));

    const std::string operationLog = OperationLogger::Instance().OperationLogPath();
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

    const std::string content = ReadFile(OperationLogger::Instance().OperationLogPath());
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: v=2 --> 3"));
}

TEST_F(FlagsConfigMonitorTest, InvalidFlagValueRecordsConfigFailed)
{
    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-minloglevel=invalid");
    ASSERT_EQ(flagMap.count("minloglevel"), 1ul);
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsError());

    const std::string content = ReadFile(OperationLogger::Instance().OperationLogPath());
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_FAILED: flag 'minloglevel'"));
}

TEST_F(FlagsConfigMonitorTest, NonModifiableFlagRecordsConfigFailed)
{
    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-rpc_thread_num=32");
    EXPECT_EQ(flagMap.count("rpc_thread_num"), 0ul);

    std::pair<std::string, std::string> kv;
    EXPECT_FALSE(flagConfig.SplitArgument("--rpc_thread_num=32", kv));

    const std::string content = ReadFile(OperationLogger::Instance().OperationLogPath());
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_FAILED: flag 'rpc_thread_num' not modifiable"));
}

TEST_F(FlagsConfigMonitorTest, MultipleFlagsRecordedSeparately)
{
    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-v=1\n-minloglevel=2\n-heartbeat_interval_ms=3000");
    EXPECT_EQ(flagMap.size(), 3ul);
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());

    const std::string content = ReadFile(OperationLogger::Instance().OperationLogPath());
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: v="));
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: minloglevel="));
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: heartbeat_interval_ms="));
}

TEST_F(FlagsConfigMonitorTest, ManifestFlagEndToEndViaConfigFile)
{
    ASSERT_TRUE(FlagManager::GetInstance()->IsModifiableFlag("heartbeat_interval_ms"));
    DynamicFlagConfig flagConfig;
    WriteConfigFile(configPath_, "-heartbeat_interval_ms=2500\n");
    flagConfig.StartConfigFileHandle(configPath_, std::chrono::steady_clock::now());
    EXPECT_EQ(FLAGS_heartbeat_interval_ms, 2500);

    const std::string content = ReadFile(OperationLogger::Instance().OperationLogPath());
    EXPECT_THAT(content, testing::HasSubstr("CONFIG_CHANGED: heartbeat_interval_ms="));
}

TEST_F(FlagsConfigMonitorTest, HeartbeatIntervalRuntimeUpdateChangesEtcdRenewInterval)
{
    DynamicFlagConfig flagConfig;
    WriteConfigFile(configPath_, "-heartbeat_interval_ms=2500\n");
    flagConfig.StartConfigFileHandle(configPath_, std::chrono::steady_clock::now());

    EtcdKeepAlive keepAlive("", 0);
    EXPECT_EQ(keepAlive.GetLeaseRenewIntervalMs(), 2500);
}

}  // namespace ut
}  // namespace datasystem
