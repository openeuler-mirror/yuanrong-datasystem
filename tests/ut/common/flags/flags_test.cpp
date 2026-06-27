/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: flags test.
 */

#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/gflag/common_gflags.h"

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/utils/embedded_config.h"
#include "datasystem/utils/kv_client_config.h"

#include "datasystem/common/constants.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

DS_DEFINE_bool(bool_flag, false, "boolean variable test");
DS_DEFINE_uint32(uint32_flag, 32, "uint32 variable test");
DS_DEFINE_int32(int32_flag, 32, "uint32 variable test");
DS_DEFINE_uint64(uint64_flag, 64, "uint32 variable test");
DS_DEFINE_int64(int64_flag, 64, "uint32 variable test");
DS_DEFINE_string(str_flag, "default", "uint32 variable test");
DS_DEFINE_string(empty_str_flag, "default", "string flag with no validator; accepts empty");
DS_DEFINE_double(double_flag, 1.0, "double variable test");
DS_DEFINE_double(invalid_double_flag, 1.0, "invalid double variable test");
DS_DEFINE_int32_dynamic(test_modifiable_flag, 7, "ut dynamic flag");

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_bool(log_async);
DS_DECLARE_bool(log_compress);
DS_DECLARE_bool(log_only_write_info_file);
DS_DECLARE_bool(logtostderr);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_uint32(log_async_queue_size);
DS_DECLARE_uint32(log_retention_day);
DS_DECLARE_uint32(max_log_file_num);
DS_DECLARE_uint32(max_log_size);
DS_DECLARE_uint32(stderrthreshold);
DS_DECLARE_string(monitor_config_file);

namespace {
constexpr uint32_t kDefaultSlowUs = 500;
constexpr char kClientSlowEnv[] = "DATASYSTEM_CLIENT_SLOW_US";
constexpr char kWorkerSlowEnv[] = "DATASYSTEM_WORKER_SLOW_US";
}

int modifyCount = 0;
int defaultCount = 0;

void ResetCount()
{
    modifyCount = 0;
    defaultCount = 0;
}

static bool ValidateBoolean(const char *name, bool value)
{
    if (std::string(name) != "bool_flag") {
        return false;
    }
    if (value) {
        modifyCount++;
    } else {
        defaultCount++;
    }
    return true;
}

static bool ValidateUint32(const char *name, uint32_t value)
{
    if (std::string(name) != "uint32_flag") {
        return false;
    }
    if (value != 32) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateInt32(const char *name, int32_t value)
{
    if (std::string(name) != "int32_flag") {
        return false;
    }
    if (value != 32) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateUint64(const char *name, uint64_t value)
{
    if (std::string(name) != "uint64_flag") {
        return false;
    }
    if (value != 64) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateInt64(const char *name, int64_t value)
{
    if (std::string(name) != "int64_flag") {
        return false;
    }
    if (value != 64) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateString(const char *name, const std::string &value)
{
    if (std::string(name) != "str_flag") {
        return false;
    }
    if (value.empty()) {
        return false;
    }
    if (value != "default") {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == "xxx") {
        return false;
    }
    return true;
}

DS_DEFINE_validator(bool_flag, &ValidateBoolean);
DS_DEFINE_validator(uint32_flag, &ValidateUint32);
DS_DEFINE_validator(int32_flag, &ValidateInt32);
DS_DEFINE_validator(uint64_flag, &ValidateUint64);
DS_DEFINE_validator(int64_flag, &ValidateInt64);
DS_DEFINE_validator(str_flag, &ValidateString);

#define ASSERT_ABNORMAL_VALUE(name, set_value, expect_value, contain_msg) \
    do {                                                                  \
        std::string errMsg;                                               \
        ASSERT_FALSE(SetCommandLineOption(#name, set_value, errMsg));     \
        ASSERT_TRUE(errMsg.find(contain_msg) != errMsg.npos);             \
        ASSERT_EQ(FLAGS_##name, expect_value);                            \
    } while (0)

namespace datasystem {
namespace ut {
namespace {
class EnvGuard {
public:
    explicit EnvGuard(std::vector<std::pair<std::string, std::string>> envs)
    {
        for (const auto &env : envs) {
            const char *oldValue = std::getenv(env.first.c_str());
            savedValues_.push_back(SavedEnv{ env.first, oldValue != nullptr, oldValue != nullptr ? oldValue : "" });
            setenv(env.first.c_str(), env.second.c_str(), 1);
        }
    }

    ~EnvGuard()
    {
        for (const auto &env : savedValues_) {
            if (env.wasSet) {
                setenv(env.name.c_str(), env.value.c_str(), 1);
            } else {
                unsetenv(env.name.c_str());
            }
        }
    }

private:
    struct SavedEnv {
        std::string name;
        bool wasSet;
        std::string value;
    };
    std::vector<SavedEnv> savedValues_;
};

class ClientLoggingFlagGuard {
public:
    ClientLoggingFlagGuard()
        : maxLogSize_(FLAGS_max_log_size),
          maxLogFileNum_(FLAGS_max_log_file_num),
          logCompress_(FLAGS_log_compress),
          v_(FLAGS_v),
          minLogLevel_(FLAGS_minloglevel),
          logRetentionDay_(FLAGS_log_retention_day),
          logToStderr_(FLAGS_logtostderr),
          alsoLogToStderr_(FLAGS_alsologtostderr),
          stderrThreshold_(FLAGS_stderrthreshold),
          logAsync_(FLAGS_log_async),
          logAsyncQueueSize_(FLAGS_log_async_queue_size),
          logOnlyWriteInfoFile_(FLAGS_log_only_write_info_file),
          zmqClientIoThread_(FLAGS_zmq_client_io_thread)
    {
    }

    ~ClientLoggingFlagGuard()
    {
        FLAGS_max_log_size = maxLogSize_;
        FLAGS_max_log_file_num = maxLogFileNum_;
        FLAGS_log_compress = logCompress_;
        FLAGS_v = v_;
        FLAGS_minloglevel = minLogLevel_;
        FLAGS_log_retention_day = logRetentionDay_;
        FLAGS_logtostderr = logToStderr_;
        FLAGS_alsologtostderr = alsoLogToStderr_;
        FLAGS_stderrthreshold = stderrThreshold_;
        FLAGS_log_async = logAsync_;
        FLAGS_log_async_queue_size = logAsyncQueueSize_;
        FLAGS_log_only_write_info_file = logOnlyWriteInfoFile_;
        FLAGS_zmq_client_io_thread = zmqClientIoThread_;
    }

private:
    uint32_t maxLogSize_;
    uint32_t maxLogFileNum_;
    bool logCompress_;
    int32_t v_;
    int32_t minLogLevel_;
    uint32_t logRetentionDay_;
    bool logToStderr_;
    bool alsoLogToStderr_;
    uint32_t stderrThreshold_;
    bool logAsync_;
    uint32_t logAsyncQueueSize_;
    bool logOnlyWriteInfoFile_;
    int32_t zmqClientIoThread_;
};

void ApplyKvClientLogConfigFromConfig(const KVClientConfig &clientConfig)
{
    const auto &args = clientConfig.GetArgs();
    auto logWithoutPid = args.find("client_log_without_pid");
    if (logWithoutPid != args.end()) {
        Logging::SetClientLogWithoutPid(ParseBoolFromString(logWithoutPid->second, false));
    }
    auto accessLogName = args.find("client_access_log_filename");
    if (accessLogName != args.end()) {
        Logging::SetClientAccessLogName(accessLogName->second);
    }
}

class ClientLogConfigPriorityTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        Logging::ResetClientLogConfigForTest();
    }
};
}  // namespace

class FlagsTest : public ::testing::Test {
public:
    void SetUp() override
    {
        ResetCount();
        // Reset all FLAGS to default values to prevent test pollution
        FLAGS_bool_flag = false;
        FLAGS_uint32_flag = 32;
        FLAGS_int32_flag = 32;
        FLAGS_uint64_flag = 64;
        FLAGS_int64_flag = 64;
        FLAGS_str_flag = "default";
        FLAGS_double_flag = 1.0;
        FLAGS_invalid_double_flag = 1.0;
    }
};

TEST_F(FlagsTest, TestDefaultValue)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);
    ASSERT_EQ(FLAGS_bool_flag, false);
    ASSERT_EQ(FLAGS_uint32_flag, 32u);
    ASSERT_EQ(FLAGS_int32_flag, 32);
    ASSERT_EQ(FLAGS_uint64_flag, 64ul);
    ASSERT_EQ(FLAGS_int64_flag, 64l);
    ASSERT_EQ(FLAGS_str_flag, "default");
    // Verify that validators were called (exact count may vary due to global state)
    // defaultCount should increase because all flags have default values
    ASSERT_GT(defaultCount, 0);
    // modifyCount should be 0 because no flags were modified
    ASSERT_EQ(modifyCount, 0);
}

TEST_F(FlagsTest, EmbeddedConfigKeepsArgs)
{
    EmbeddedConfig config;
    config.LogDir("/tmp/ds_logs").MinLogLevel(1);
    ASSERT_EQ(config.GetArgs().at("log_dir"), "/tmp/ds_logs");
    ASSERT_EQ(config.GetArgs().at("minloglevel"), "1");
}

TEST_F(FlagsTest, ParseCommandLineFlagsAcceptsEmbeddedConfig)
{
    EmbeddedConfig config;
    config.SetArg("str_flag", "configured");
    std::string errMsg;
    ASSERT_TRUE(ParseCommandLineFlags(config, errMsg));
    ASSERT_TRUE(errMsg.empty());
    ASSERT_EQ(FLAGS_str_flag, "configured");
}

TEST_F(FlagsTest, ParseCommandLineFlagsAcceptsArgMap)
{
    std::unordered_map<std::string, std::string> args = { { "str_flag", "from_map" } };
    std::string errMsg;
    ASSERT_TRUE(ParseCommandLineFlags(args, errMsg));
    ASSERT_TRUE(errMsg.empty());
    ASSERT_EQ(FLAGS_str_flag, "from_map");
}

TEST_F(FlagsTest, ExplicitLogMonitorConfigIsNotOverriddenByEnv)
{
    const char *envName = "DATASYSTEM_LOG_MONITOR_ENABLE";
    const char *oldValue = std::getenv(envName);
    std::string savedValue = oldValue != nullptr ? oldValue : "";
    setenv(envName, "true", 1);

    KVClientConfig config;
    Status status = KVClientConfig::Builder().LogMonitorEnable(false).Build(config);
    ASSERT_EQ(status, Status::OK());
    std::string errMsg;
    ASSERT_TRUE(ParseCommandLineFlags(config, errMsg));
    ASSERT_TRUE(errMsg.empty());
    ASSERT_FALSE(FLAGS_log_monitor);
    ASSERT_TRUE(WasCommandLineFlagSpecified("log_monitor"));

    Logging::GetInstance()->Start("kv_client_test", true);
    ASSERT_FALSE(FLAGS_log_monitor);

    if (oldValue != nullptr) {
        setenv(envName, savedValue.c_str(), 1);
    } else {
        unsetenv(envName);
    }
}

TEST_F(FlagsTest, ExplicitDefaultClientLogConfigIsNotOverriddenByEnv)
{
    ClientLoggingFlagGuard flagGuard;
    EnvGuard envGuard({
        { MAX_LOG_SIZE_ENV, "200" },
        { MAX_LOG_FILE_NUM_ENV, "7" },
        { LOG_COMPRESS_ENV, "false" },
        { LOG_V, "3" },
        { MIN_LOG_LEVEL, "2" },
        { LOG_RETENTION_DAY_ENV, "5" },
        { LOG_TO_STDERR_ENV, "true" },
        { ALSO_LOG_TO_STDERR_ENV, "true" },
        { STDERR_THRESHOLD_ENV, "3" },
        { LOG_ASYNC_ENABLE, "false" },
        { LOG_ASYNC_QUEUE_SIZE, "4096" },
        { LOG_ONLY_WRITE_INFO_FILE_ENV, "false" },
        { "DATASYSTEM_ZMQ_CLIENT_IO_THREAD", "3" },
    });

    KVClientConfig config;
    Status status = KVClientConfig::Builder()
                        .MaxLogSize(100)
                        .MaxLogFileNum(5)
                        .LogCompress(true)
                        .VLogLevel(0)
                        .MinLogLevel(0)
                        .LogRetentionDay(0)
                        .LogToStderr(false)
                        .AlsoLogToStderr(false)
                        .StderrThreshold(2)
                        .LogAsyncEnable(true)
                        .LogAsyncQueueSize(DEFAULT_LOG_ASYNC_QUEUE_SIZE)
                        .LogOnlyWriteInfoFile(true)
                        .ZmqClientIoThread(1)
                        .Build(config);
    ASSERT_EQ(status, Status::OK());
    std::string errMsg;
    ASSERT_TRUE(ParseCommandLineFlags(config, errMsg));
    ASSERT_TRUE(errMsg.empty());

    Logging::GetInstance()->Start("kv_client_default_config_test", true);

    EXPECT_EQ(FLAGS_max_log_size, 100u);
    EXPECT_EQ(FLAGS_max_log_file_num, 5u);
    EXPECT_TRUE(FLAGS_log_compress);
    EXPECT_EQ(FLAGS_v, 0);
    EXPECT_EQ(FLAGS_minloglevel, 0);
    EXPECT_EQ(FLAGS_log_retention_day, 0u);
    EXPECT_FALSE(FLAGS_logtostderr);
    EXPECT_FALSE(FLAGS_alsologtostderr);
    EXPECT_EQ(FLAGS_stderrthreshold, 2u);
    EXPECT_TRUE(FLAGS_log_async);
    EXPECT_EQ(FLAGS_log_async_queue_size, DEFAULT_LOG_ASYNC_QUEUE_SIZE);
    EXPECT_TRUE(FLAGS_log_only_write_info_file);
    EXPECT_EQ(FLAGS_zmq_client_io_thread, 1);
}

TEST_F(ClientLogConfigPriorityTest, ExplicitClientAccessLogNameIsNotOverriddenByEnv)
{
    const char *envName = ACCESS_LOG_NAME_ENV.c_str();
    const char *oldValue = std::getenv(envName);
    std::string savedValue = oldValue != nullptr ? oldValue : "";
    setenv(envName, "env_access_log", 1);

    KVClientConfig config;
    Status status = KVClientConfig::Builder().AccessLogName("kv_access_log").Build(config);
    ASSERT_EQ(status, Status::OK());
    ApplyKvClientLogConfigFromConfig(config);
    ASSERT_EQ(Logging::GetClientAccessLogName(), "kv_access_log");

    if (oldValue != nullptr) {
        setenv(envName, savedValue.c_str(), 1);
    } else {
        unsetenv(envName);
    }
}

TEST_F(ClientLogConfigPriorityTest, ExplicitEmptyClientAccessLogNameIsNotOverriddenByEnv)
{
    const char *envName = ACCESS_LOG_NAME_ENV.c_str();
    const char *oldValue = std::getenv(envName);
    std::string savedValue = oldValue != nullptr ? oldValue : "";
    setenv(envName, "env_access_log", 1);

    KVClientConfig config;
    Status status = KVClientConfig::Builder().AccessLogName("").Build(config);
    ASSERT_EQ(status, Status::OK());
    ApplyKvClientLogConfigFromConfig(config);
    ASSERT_EQ(Logging::GetClientAccessLogName(), "");

    if (oldValue != nullptr) {
        setenv(envName, savedValue.c_str(), 1);
    } else {
        unsetenv(envName);
    }
}

TEST_F(ClientLogConfigPriorityTest, ClientAccessLogNameFallsBackToEnvWhenKvClientNotSpecified)
{
    const char *envName = ACCESS_LOG_NAME_ENV.c_str();
    const char *oldValue = std::getenv(envName);
    std::string savedValue = oldValue != nullptr ? oldValue : "";
    setenv(envName, "env_access_log", 1);

    ASSERT_EQ(Logging::GetClientAccessLogName(), "env_access_log");

    if (oldValue != nullptr) {
        setenv(envName, savedValue.c_str(), 1);
    } else {
        unsetenv(envName);
    }
}

TEST_F(ClientLogConfigPriorityTest, ExplicitClientLogWithoutPidIsNotOverriddenByEnv)
{
    const char *envName = CLIENT_LOG_WITHOUT_PID_ENV.c_str();
    const char *oldValue = std::getenv(envName);
    std::string savedValue = oldValue != nullptr ? oldValue : "";
    setenv(envName, "true", 1);

    KVClientConfig config;
    Status status = KVClientConfig::Builder().LogWithoutPid(false).Build(config);
    ASSERT_EQ(status, Status::OK());
    ApplyKvClientLogConfigFromConfig(config);
    ASSERT_EQ(Logging::GetClientLogName(CLIENT_ACCESS_LOG_NAME, 12345), CLIENT_ACCESS_LOG_NAME + "_12345");

    if (oldValue != nullptr) {
        setenv(envName, savedValue.c_str(), 1);
    } else {
        unsetenv(envName);
    }
}

TEST_F(FlagsTest, KVClientConfigBuilderBuildsEmptyConfigWhenNoSetterUsed)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder().Build(config);
    ASSERT_EQ(status, Status::OK());
    ASSERT_TRUE(config.GetArgs().empty());
}

TEST_F(FlagsTest, KVClientConfigBuilderReplacesOutputConfig)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder().MaxLogSize(123).LogAsyncQueueSize(4096).Build(config);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(config.GetArgs().at("max_log_size"), "123");
    ASSERT_EQ(config.GetArgs().at("log_async_queue_size"), "4096");

    status = KVClientConfig::Builder().MaxLogFileNum(7).Build(config);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(config.GetArgs().size(), 1ul);
    ASSERT_EQ(config.GetArgs().at("max_log_file_num"), "7");
    ASSERT_EQ(config.GetArgs().count("max_log_size"), 0ul);
    ASSERT_EQ(config.GetArgs().count("log_async_queue_size"), 0ul);

    status = KVClientConfig::Builder().Build(config);
    ASSERT_EQ(status, Status::OK());
    ASSERT_TRUE(config.GetArgs().empty());
}

TEST_F(FlagsTest, KVClientConfigBuilderStoresExplicitValues)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder()
                        .LogDir("/tmp/ds_logs")
                        .LogName("client")
                        .LogWithoutPid(false)
                        .AccessLogName("client_access")
                        .MinLogLevel(1)
                        .VLogLevel(2)
                        .StderrThreshold(3)
                        .MaxLogSize(100)
                        .MaxLogFileNum(7)
                        .LogCompress(false)
                        .LogRetentionDay(3)
                        .LogToStderr(true)
                        .AlsoLogToStderr(true)
                        .LogOnlyWriteInfoFile(false)
                        .LogAsyncEnable(false)
                        .LogAsyncQueueSize(4096)
                        .LogMonitorEnable(false)
                        .MonitorConfigPath("/tmp/ds.config")
                        .ZmqClientIoContext(8)
                        .ZmqClientIoThread(2)
                        .Build(config);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(config.GetArgs().at("log_dir"), "/tmp/ds_logs");
    ASSERT_EQ(config.GetArgs().at("log_filename"), "client");
    ASSERT_EQ(config.GetArgs().at("client_log_without_pid"), "false");
    ASSERT_EQ(config.GetArgs().at("client_access_log_filename"), "client_access");
    ASSERT_EQ(config.GetArgs().at("minloglevel"), "1");
    ASSERT_EQ(config.GetArgs().at("v"), "2");
    ASSERT_EQ(config.GetArgs().at("stderrthreshold"), "3");
    ASSERT_EQ(config.GetArgs().at("max_log_size"), "100");
    ASSERT_EQ(config.GetArgs().at("max_log_file_num"), "7");
    ASSERT_EQ(config.GetArgs().at("log_compress"), "false");
    ASSERT_EQ(config.GetArgs().at("log_retention_day"), "3");
    ASSERT_EQ(config.GetArgs().at("logtostderr"), "true");
    ASSERT_EQ(config.GetArgs().at("alsologtostderr"), "true");
    ASSERT_EQ(config.GetArgs().at("log_only_write_info_file"), "false");
    ASSERT_EQ(config.GetArgs().at("log_async"), "false");
    ASSERT_EQ(config.GetArgs().at("log_async_queue_size"), "4096");
    ASSERT_EQ(config.GetArgs().at("log_monitor"), "false");
    ASSERT_EQ(config.GetArgs().at("monitor_config_file"), "/tmp/ds.config");
    ASSERT_EQ(config.GetArgs().at("zmq_client_io_context"), "8");
    ASSERT_EQ(config.GetArgs().at("zmq_client_io_thread"), "2");
}

TEST_F(FlagsTest, KVClientConfigBuilderAggregatesInvalidValues)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder()
                        .LogName("bad|name")
                        .MinLogLevel(4)
                        .MaxLogSize(0)
                        .LogAsyncQueueSize(255)
                        .ZmqClientIoThread(0)
                        .Build(config);
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("LogName"));
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("MinLogLevel"));
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("MaxLogSize"));
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("LogAsyncQueueSize"));
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("ZmqClientIoThread"));
    EXPECT_TRUE(config.GetArgs().empty());
}

TEST_F(FlagsTest, KVClientConfigBuilderRejectsEmptyGflagStrings)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder().LogDir("").Build(config);
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("LogDir"));

    status = KVClientConfig::Builder().LogName("").Build(config);
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("LogName"));
}

TEST_F(FlagsTest, KVClientConfigBuilderAcceptsEmptyMonitorConfigPath)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder().MonitorConfigPath("").Build(config);
    ASSERT_EQ(status, Status::OK());
    ASSERT_EQ(config.GetArgs().at("monitor_config_file"), "");
}

TEST_F(FlagsTest, ParseCommandLineFlagsAcceptsEmptyStringWhenValidatorAllows)
{
    KVClientConfig config;
    ASSERT_EQ(KVClientConfig::Builder().MonitorConfigPath("").Build(config), Status::OK());
    std::string errMsg;
    ASSERT_TRUE(ParseCommandLineFlags(config, errMsg)) << errMsg;
    ASSERT_EQ(FLAGS_monitor_config_file, "");

    std::unordered_map<std::string, std::string> args = { { "empty_str_flag", "" } };
    ASSERT_TRUE(ParseCommandLineFlags(args, errMsg)) << errMsg;
    ASSERT_TRUE(errMsg.empty());
    ASSERT_EQ(FLAGS_empty_str_flag, "");
}

TEST_F(FlagsTest, ParseCommandLineFlagsRejectsEmptyNonStringFlag)
{
    std::unordered_map<std::string, std::string> args = { { "uint32_flag", "" } };
    std::string errMsg;
    ASSERT_FALSE(ParseCommandLineFlags(args, errMsg));
    EXPECT_THAT(errMsg, testing::HasSubstr("uint32_flag"));
    EXPECT_THAT(errMsg, testing::HasSubstr("missing its argument"));
}

TEST_F(FlagsTest, ParseCommandLineFlagsRejectsEmptyStringWhenValidatorDisallows)
{
    std::unordered_map<std::string, std::string> args = { { "str_flag", "" } };
    std::string errMsg;
    ASSERT_FALSE(ParseCommandLineFlags(args, errMsg));
    EXPECT_THAT(errMsg, testing::HasSubstr("str_flag"));
    EXPECT_THAT(errMsg, testing::HasSubstr("illegal value"));
}

TEST_F(FlagsTest, KVClientConfigBuilderRejectsInvalidAccessLogName)
{
    KVClientConfig config;
    Status status = KVClientConfig::Builder().AccessLogName("client-access").Build(config);
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("AccessLogName"));
    EXPECT_TRUE(config.GetArgs().empty());
}

TEST_F(FlagsTest, ParseCommandLineFlagsClearsErrorStateOnRetry)
{
    std::unordered_map<std::string, std::string> badArgs = { { "str_flag", "" } };
    std::string errMsg;
    ASSERT_FALSE(ParseCommandLineFlags(badArgs, errMsg));
    ASSERT_FALSE(errMsg.empty());

    std::unordered_map<std::string, std::string> goodArgs = { { "str_flag", "retry_ok" } };
    ASSERT_TRUE(ParseCommandLineFlags(goodArgs, errMsg));
    ASSERT_TRUE(errMsg.empty());
    ASSERT_EQ(FLAGS_str_flag, "retry_ok");
}

TEST_F(FlagsTest, TestSetValue)
{
    const char *argv[] = { "./program", "-bool_flag",       "true",      "-uint32_flag=64",      "--int32_flag",
                           "-64",       "--int64_flag=-64", "-str_flag", "Hextech Flashtraption" };
    ParseCommandLineFlags(9, (char **)argv);
    ASSERT_EQ(FLAGS_bool_flag, true);
    ASSERT_EQ(FLAGS_uint32_flag, 64u);
    ASSERT_EQ(FLAGS_int32_flag, -64);
    ASSERT_EQ(FLAGS_uint64_flag, 64ul);
    ASSERT_EQ(FLAGS_int64_flag, -64l);
    ASSERT_EQ(FLAGS_str_flag, "Hextech Flashtraption");
    // Verify that validators were called
    // defaultCount may include validators for flags that kept default values
    // modifyCount should be > 0 because some flags were modified
    ASSERT_GT(defaultCount + modifyCount, 0);

    ResetCount();
    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("bool_flag", "f", errMsg));
    ASSERT_EQ(FLAGS_bool_flag, false);
    ASSERT_TRUE(errMsg.empty());
    // When setting to default value, defaultCount should increase
    ASSERT_EQ(defaultCount, 1);
    ASSERT_EQ(modifyCount, 0);

    ASSERT_TRUE(SetCommandLineOption("uint32_flag", "123 ", errMsg));
    ASSERT_EQ(FLAGS_uint32_flag, 123UL);
}

TEST_F(FlagsTest, TestOutOfRangeValue)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    // uint32_t
    ASSERT_ABNORMAL_VALUE(uint32_flag, "-1", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "5294967296", 32u, "illegal value");

    // int32_t
    ASSERT_ABNORMAL_VALUE(int32_flag, "-5294967296", 32, "illegal value");
    ASSERT_ABNORMAL_VALUE(int32_flag, "5294967296", 32, "illegal value");

    // uint64_t
    ASSERT_ABNORMAL_VALUE(uint64_flag, "-1", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "18446744073709551616", 64ul, "illegal value");

    // int64_t
    ASSERT_ABNORMAL_VALUE(int64_flag, "9223372036854775809", 64, "illegal value");
    ASSERT_ABNORMAL_VALUE(int64_flag, "-9223372036854775809", 64, "illegal value");
}

TEST_F(FlagsTest, TestSetAbsentFlag)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    std::string errMsg;
    ResetCount();
    ASSERT_FALSE(SetCommandLineOption("absent_flag", "who care", errMsg));
    ASSERT_TRUE(errMsg.find("not found") != errMsg.npos);
    ASSERT_EQ(defaultCount, 0);
    ASSERT_EQ(modifyCount, 0);
}

TEST_F(FlagsTest, TestSetIllegalValue)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    // Boolean
    ASSERT_ABNORMAL_VALUE(bool_flag, "xxx", false, "illegal value");
    ASSERT_ABNORMAL_VALUE(bool_flag, "", false, "illegal value");

    // uint32_t
    ASSERT_ABNORMAL_VALUE(uint32_flag, "xxx", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "-100", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "123abc", 32u, "illegal value");

    // int32_t
    ASSERT_ABNORMAL_VALUE(int32_flag, "xxx", 32, "illegal value");
    ASSERT_ABNORMAL_VALUE(int32_flag, "", 32, "illegal value");
    ASSERT_ABNORMAL_VALUE(int32_flag, "123abc", 32, "illegal value");

    // uint64_t
    ASSERT_ABNORMAL_VALUE(uint64_flag, "xxx", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "-100", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "123abc", 64ul, "illegal value");

    // int64_t
    ASSERT_ABNORMAL_VALUE(int64_flag, "xxx", 64, "illegal value");
    ASSERT_ABNORMAL_VALUE(int64_flag, "", 64, "illegal value");
    ASSERT_ABNORMAL_VALUE(int64_flag, "123abc", 64, "illegal value");
}

TEST_F(FlagsTest, TestFailValidation)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    ASSERT_ABNORMAL_VALUE(uint32_flag, "0", 32u, "failed validation");
    ASSERT_ABNORMAL_VALUE(int32_flag, "0", 32, "failed validation");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "0", 64ul, "failed validation");
    ASSERT_ABNORMAL_VALUE(int64_flag, "0", 64, "failed validation");
    ASSERT_ABNORMAL_VALUE(str_flag, "xxx", "default", "failed validation");
}

TEST_F(FlagsTest, TestGetAllFlags)
{
    const char *argv[] = { "./program", "-uint32_flag=32" };
    ParseCommandLineFlags(1, (char **)argv);

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    // Don't check exact size as other modules may register flags
    // Just ensure our test flags are present
    ASSERT_GE(output.size(), 7ul);

    // Check that our test flags are in the list
    bool found_bool_flag = false;
    bool found_uint32_flag = false;
    for (const auto &info : output) {
        if (info.name == "bool_flag" || info.name == "bool_flag") {
            found_bool_flag = true;
        }
        if (info.name == "uint32_flag" || info.name == "uint32_flag") {
            found_uint32_flag = true;
        }
    }
    ASSERT_TRUE(found_bool_flag) << "bool_flag should be registered";
    ASSERT_TRUE(found_uint32_flag) << "uint32_flag should be registered";

    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("uint32_flag", "1", errMsg));
    ASSERT_FALSE(SetCommandLineOption("uint64_flag", "-1", errMsg));
    ASSERT_TRUE(SetCommandLineOption("bool_flag", "true", errMsg));
    ASSERT_FALSE(SetCommandLineOption("str_flag", "xxx", errMsg));

    output.clear();
    GetAllFlags(output);
    ASSERT_GE(output.size(), 7ul);

    int modCount = 0;

    for (const auto &info : output) {
        if (!info.isDefault) {
            modCount++;
        }
    }
    // We modified 2 flags (uint32_flag and bool_flag)
    // But other tests may have modified other flags, so just check >= 2
    ASSERT_GE(modCount, 2);
}

TEST_F(FlagsTest, ParseBoolFromStringUsesGflagRules)
{
    ASSERT_TRUE(ParseBoolFromString("true", false));
    ASSERT_TRUE(ParseBoolFromString("TRUE", false));
    ASSERT_TRUE(ParseBoolFromString("1", false));
    ASSERT_TRUE(ParseBoolFromString("yes", false));
    ASSERT_FALSE(ParseBoolFromString("false", true));
    ASSERT_FALSE(ParseBoolFromString("FALSE", true));
    ASSERT_FALSE(ParseBoolFromString("0", true));
    ASSERT_FALSE(ParseBoolFromString("invalid", false));
    ASSERT_TRUE(ParseBoolFromString("invalid", true));
}

TEST_F(FlagsTest, TestGetBooleanEnv)
{
    std::vector<std::string> commanlines = {
        "bool1=false", "bool2=true", "bool3=f", "bool4=t",  "bool5=0", "bool6=1",
        "bool7=no",    "bool8=yes",  "bool9=n", "bool10=y", "str=xxx", "empty=",
    };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    // normal.
    ASSERT_FALSE(GetBoolFromEnv("bool1", true));
    ASSERT_TRUE(GetBoolFromEnv("bool2", false));
    ASSERT_FALSE(GetBoolFromEnv("bool3", true));
    ASSERT_TRUE(GetBoolFromEnv("bool4", false));
    ASSERT_FALSE(GetBoolFromEnv("bool5", true));
    ASSERT_TRUE(GetBoolFromEnv("bool6", false));
    ASSERT_FALSE(GetBoolFromEnv("bool7", true));
    ASSERT_TRUE(GetBoolFromEnv("bool8", false));
    ASSERT_FALSE(GetBoolFromEnv("bool9", true));
    ASSERT_TRUE(GetBoolFromEnv("bool10", false));

    // abnormal
    ASSERT_FALSE(GetBoolFromEnv("str", false));
    ASSERT_TRUE(GetBoolFromEnv("empty", true));
    ASSERT_TRUE(GetBoolFromEnv(nullptr, true));
}

TEST_F(FlagsTest, TestGet32BitEnv)
{
    std::vector<std::string> commanlines = {
        "num1=1",
        "uint32_max=4294967295",
        "uint32_min=0",
        "uint32_max_out=4294967296",
        "uint32_min_out=-1",
        "int32_max=2147483647",
        "int32_min=-2147483648",
        "int32_max_out=2147483648",
        "int32_min_out=-2147483649",
        "str=xxx",
        "empty=",
    };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    // normal
    ASSERT_EQ(GetUint32FromEnv("num1", 0), 1u);
    ASSERT_EQ(GetUint32FromEnv("uint32_max", 0), UINT32_MAX);
    ASSERT_EQ(GetUint32FromEnv("uint32_min", 1), 0u);

    ASSERT_EQ(GetInt32FromEnv("num1", 0), 1);
    ASSERT_EQ(GetInt32FromEnv("int32_max", 0), INT32_MAX);
    ASSERT_EQ(GetInt32FromEnv("int32_min", 1), INT32_MIN);

    // abnormal
    ASSERT_EQ(GetUint32FromEnv("uint32_max_out", 100u), 100u);
    ASSERT_EQ(GetUint32FromEnv("uint32_min_out", 100u), 100u);
    ASSERT_EQ(GetUint32FromEnv("empty", 0), 0u);
    ASSERT_EQ(GetUint32FromEnv("str", 0), 0u);
    ASSERT_EQ(GetUint32FromEnv("no_exist", 0), 0u);
    ASSERT_EQ(GetUint32FromEnv(nullptr, 0), 0u);

    ASSERT_EQ(GetInt32FromEnv("int32_max_out", 100), 100);
    ASSERT_EQ(GetInt32FromEnv("int32_min_out", 100), 100);
    ASSERT_EQ(GetInt32FromEnv("empty", 0), 0);
    ASSERT_EQ(GetInt32FromEnv("str", 0), 0);
    ASSERT_EQ(GetInt32FromEnv("no_exist", 0), 0);
    ASSERT_EQ(GetInt32FromEnv(nullptr, 0), 0);
}

TEST_F(FlagsTest, TestGet64BitEnv)
{
    std::vector<std::string> commanlines = {
        "num1=1",
        "uint64_max=18446744073709551615",
        "uint64_min=0",
        "uint64_max_out=18446744073709551616",
        "uint64_min_out=-1",
        "int64_max=9223372036854775807",
        "int64_min=-9223372036854775808",
        "int64_max_out=9223372036854775808",
        "int64_min_out=-9223372036854775809",
        "str=xxx",
        "empty=",
    };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    // normal
    ASSERT_EQ(GetUint64FromEnv("num1", 0), 1u);
    ASSERT_EQ(GetUint64FromEnv("uint64_max", 0), UINT64_MAX);
    ASSERT_EQ(GetUint64FromEnv("uint64_min", 1), 0ul);

    ASSERT_EQ(GetInt64FromEnv("num1", 0), 1);
    ASSERT_EQ(GetInt64FromEnv("int64_max", 0), INT64_MAX);
    ASSERT_EQ(GetInt64FromEnv("int64_min", 1), INT64_MIN);

    // abnormal
    ASSERT_EQ(GetUint64FromEnv("uint64_max_out", 100ul), 100ul);
    ASSERT_EQ(GetUint64FromEnv("uint64_min_out", 100ul), 100ul);
    ASSERT_EQ(GetUint64FromEnv("empty", 0), 0ul);
    ASSERT_EQ(GetUint64FromEnv("str", 0), 0ul);
    ASSERT_EQ(GetUint64FromEnv("no_exist", 0), 0ul);
    ASSERT_EQ(GetUint64FromEnv(nullptr, 0), 0ul);

    ASSERT_EQ(GetInt64FromEnv("int64_max_out", 100), 100);
    ASSERT_EQ(GetInt64FromEnv("int64_min_out", 100), 100);
    ASSERT_EQ(GetInt64FromEnv("empty", 0), 0);
    ASSERT_EQ(GetInt64FromEnv("str", 0), 0);
    ASSERT_EQ(GetInt64FromEnv("no_exist", 0), 0);
    ASSERT_EQ(GetInt64FromEnv(nullptr, 0), 0);
}

TEST_F(FlagsTest, TestSlowThresholdEnvDefaultsAndOverrides)
{
    unsetenv(kClientSlowEnv);
    unsetenv(kWorkerSlowEnv);

    ASSERT_EQ(GetUint64FromEnv(kClientSlowEnv, kDefaultSlowUs), kDefaultSlowUs);
    ASSERT_EQ(GetUint64FromEnv(kWorkerSlowEnv, kDefaultSlowUs), kDefaultSlowUs);

    ASSERT_EQ(setenv(kClientSlowEnv, "123", 1), 0);
    ASSERT_EQ(setenv(kWorkerSlowEnv, "456", 1), 0);

    ASSERT_EQ(GetUint64FromEnv(kClientSlowEnv, kDefaultSlowUs), 123u);
    ASSERT_EQ(GetUint64FromEnv(kWorkerSlowEnv, kDefaultSlowUs), 456u);
}

TEST_F(FlagsTest, TestGetStringEnv)
{
    std::vector<std::string> commanlines = { "num1=32", "num4=-9223372036854775809", "str=/tmp/xxx1/xxx2/xxx3",
                                             "empty=" };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    ASSERT_EQ(GetStringFromEnv("num1", "xxx"), "32");
    ASSERT_EQ(GetStringFromEnv("num4", "xxx"), "-9223372036854775809");
    ASSERT_EQ(GetStringFromEnv("str", "xxx"), "/tmp/xxx1/xxx2/xxx3");
    ASSERT_EQ(GetStringFromEnv("empty", "xxx"), "");
    ASSERT_EQ(GetStringFromEnv("no_exist", "xxx"), "xxx");
    ASSERT_EQ(GetStringFromEnv(nullptr, "xxx"), "xxx");
}

TEST_F(FlagsTest, TestDoubleFlagRejectsEmptyValue)
{
    std::string errMsg;
    ASSERT_FALSE(SetCommandLineOption("double_flag", "", errMsg));
    ASSERT_EQ(FLAGS_double_flag, 1.0);
    ASSERT_TRUE(SetCommandLineOption("double_flag", "0.5", errMsg));
    ASSERT_EQ(FLAGS_double_flag, 0.5);
    ASSERT_TRUE(SetCommandLineOption("double_flag", "0.0", errMsg));
    ASSERT_EQ(FLAGS_double_flag, 0.0);
}

// wasSpecified: flag set via SetCommandLineOption has wasSpecified=true
TEST_F(FlagsTest, WasSpecifiedAfterSetCommandLineOption)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("bool_flag", "true", errMsg));

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    for (const auto &fi : output) {
        if (fi.name == "bool_flag") {
            EXPECT_TRUE(fi.wasSpecified);
        }
    }
}

// wasSpecified: flag set via argv has wasSpecified=true
TEST_F(FlagsTest, WasSpecifiedAfterArgv)
{
    const char *argv[] = { "./program", "-bool_flag=true" };
    ParseCommandLineFlags(2, (char **)argv);

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    for (const auto &fi : output) {
        if (fi.name == "bool_flag") {
            EXPECT_TRUE(fi.wasSpecified);
        }
    }
}

// wasSpecified: isDefault tracks value != default, wasSpecified tracks explicit set
TEST_F(FlagsTest, WasSpecifiedDiffersFromIsDefault)
{
    const char *argv[] = { "./program", "-uint32_flag=32" }; // set to default value
    ParseCommandLineFlags(2, (char **)argv);

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    for (const auto &fi : output) {
        if (fi.name == "uint32_flag") {
            // Value equals default, but was explicitly specified
            EXPECT_TRUE(fi.isDefault);     // value == default -> isDefault=true
            EXPECT_TRUE(fi.wasSpecified);  // explicitly set -> wasSpecified=true
        }
    }
}

TEST_F(FlagsTest, TestDoubleFlagRejectsNan)
{
    std::string errMsg;
    ASSERT_FALSE(SetCommandLineOption("invalid_double_flag", "nan", errMsg));
    ASSERT_TRUE(errMsg.find("illegal value") != errMsg.npos);
    ASSERT_EQ(FLAGS_invalid_double_flag, 1.0);
    std::vector<FlagInfo> output;
    GetAllFlags(output);
    for (const auto &fi : output) {
        if (fi.name == "invalid_double_flag") {
            EXPECT_TRUE(fi.isDefault);
            EXPECT_FALSE(fi.wasSpecified);
        }
    }
}

TEST_F(FlagsTest, TestDoubleFlagRejectsInf)
{
    std::string errMsg;
    ASSERT_FALSE(SetCommandLineOption("invalid_double_flag", "inf", errMsg));
    ASSERT_TRUE(errMsg.find("illegal value") != errMsg.npos);
    ASSERT_EQ(FLAGS_invalid_double_flag, 1.0);
}

TEST_F(FlagsTest, TestDoubleFlagRejectsNegativeInf)
{
    std::string errMsg;
    ASSERT_FALSE(SetCommandLineOption("invalid_double_flag", "-inf", errMsg));
    ASSERT_TRUE(errMsg.find("illegal value") != errMsg.npos);
    ASSERT_EQ(FLAGS_invalid_double_flag, 1.0);
}

TEST_F(FlagsTest, TestDoubleFlagAcceptsScientificNotation)
{
    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("double_flag", "1.5e-2", errMsg));
    ASSERT_NEAR(FLAGS_double_flag, 0.015, 1e-12);
    FLAGS_double_flag = 1.0;
}

TEST_F(FlagsTest, GetExplicitDeclaredFlagsIncludesExplicitDefault)
{
    const char *argv[] = { "./program", "-uint32_flag=32" };
    ParseCommandLineFlags(2, (char **)argv);

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    std::ostringstream args;
    for (const auto &flag : output) {
        if (flag.wasSpecified || !flag.isDefault) {
            args << "--" << flag.name << '=' << flag.value << '\n';
        }
    }
    EXPECT_TRUE(args.str().find("--uint32_flag=32") != args.str().npos);
}

TEST_F(FlagsTest, GetExplicitDeclaredFlagsIncludesRuntimeModified)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("uint32_flag", "64", errMsg));

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    std::ostringstream args;
    for (const auto &flag : output) {
        if (flag.wasSpecified || !flag.isDefault) {
            args << "--" << flag.name << '=' << flag.value << '\n';
        }
    }
    EXPECT_TRUE(args.str().find("--uint32_flag=") != args.str().npos);
}

TEST_F(FlagsTest, ModifiableFlagRegistration)
{
    EXPECT_TRUE(FlagManager::GetInstance()->IsModifiableFlag("test_modifiable_flag"));
    EXPECT_FALSE(FlagManager::GetInstance()->IsModifiableFlag("int32_flag"));
    std::vector<std::string> names;
    FlagManager::GetInstance()->GetModifiableFlagNames(names);
    EXPECT_THAT(names, testing::Contains("test_modifiable_flag"));
}

TEST_F(FlagsTest, ValidateChangeRejectsInvalidValue)
{
    std::string errMsg;
    EXPECT_FALSE(FlagManager::GetInstance()->ValidateChange("test_modifiable_flag", "not_a_number", errMsg));
    EXPECT_FALSE(errMsg.empty());
    EXPECT_EQ(FLAGS_test_modifiable_flag, 7);
}

TEST_F(FlagsTest, ValidateChangeAcceptsValidValueWithoutMutatingFlag)
{
    std::string errMsg;
    EXPECT_TRUE(FlagManager::GetInstance()->ValidateChange("test_modifiable_flag", "42", errMsg));
    EXPECT_TRUE(errMsg.empty());
    EXPECT_EQ(FLAGS_test_modifiable_flag, 7);
}

TEST_F(FlagsTest, ValidateChangeRejectsNonModifiableFlag)
{
    std::string errMsg;
    EXPECT_FALSE(FlagManager::GetInstance()->ValidateChange("int32_flag", "64", errMsg));
    EXPECT_TRUE(errMsg.find("not modifiable") != errMsg.npos);
    EXPECT_EQ(FLAGS_int32_flag, 32);
}
}  // namespace ut
}  // namespace datasystem

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
