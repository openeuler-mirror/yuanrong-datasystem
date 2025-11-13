/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Perform unit tests on functions in the flags.cpp file.
 */

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/worker_update_flag_check.h"
#include "datasystem/common/util/gflag/flags.h"

DS_DEFINE_string(worker_address, "", "");

DS_DECLARE_int32(heartbeat_interval_ms);
DS_DECLARE_bool(log_compress);
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_uint32(log_async_queue_size);
DS_DECLARE_uint32(max_log_file_num);
DS_DECLARE_int32(logbufsecs);

namespace datasystem {
namespace ut {
class FlagsTest : public CommonTest {};

TEST_F(FlagsTest, TestTrimSpace)
{
    Flags flag;
    std::string command1 = " -FlagName=Value  ";
    flag.TrimSpace(command1);
    EXPECT_EQ(command1, "-FlagName=Value");

    std::string command2 = " -FlagName = Value  ";
    flag.TrimSpace(command2);
    EXPECT_EQ(command2, "-FlagName = Value");
}

TEST_F(FlagsTest, TestSplitArgument)
{
    Flags flag;
    // Valid parameter.
    std::string command1 = "--heartbeat_interval_ms=Value";
    std::pair<std::string, std::string> kv1;
    EXPECT_TRUE(flag.SplitArgument(command1.c_str(), kv1));
    EXPECT_EQ(kv1.first, "heartbeat_interval_ms");
    EXPECT_EQ(kv1.second, "Value");

    // Valid parameter. Spaces are not allowed before and after the equal sign (=).
    std::string command2 = "--heartbeat_interval_ms = Value";
    std::pair<std::string, std::string> kv2;
    EXPECT_FALSE(flag.SplitArgument(command2.c_str(), kv2));
    EXPECT_NE(kv2.first, "heartbeat_interval_ms");
    EXPECT_NE(kv2.second, "Value");

    // Invalid parameter.
    std::string command3 = "-xx=Value";
    std::pair<std::string, std::string> kv3;
    EXPECT_FALSE(flag.SplitArgument(command3.c_str(), kv3));
    EXPECT_NE(kv3.first, "xx");
    EXPECT_NE(kv3.second, "Value");
}

TEST_F(FlagsTest, TestIsToHandle)
{
    Flags flag;
    flag.SetIsToHandle(WorkerFlagIsToHandle);
    std::unordered_map<std::string, std::string> flagMap;
    EXPECT_FALSE(flag.IsToHandle(flagMap, "loglevel_only_for_workers"));
    EXPECT_TRUE(flag.IsToHandle(flagMap, "v"));

    FLAGS_worker_address = "127.0.0.1:8888";
    flagMap.emplace(std::pair<std::string, std::string>("loglevel_only_for_workers", "128.0.0.1"));
    EXPECT_FALSE(flag.IsToHandle(flagMap, "v"));
}

TEST_F(FlagsTest, TestUpdateFlagParameter)
{
    Flags flag;
    std::unordered_map<std::string, std::string> flagMap{ { "v", "3" },
                                                          { "max_log_file_num", "20" },
                                                          { "logbufsecs", "15" } };
    flag.UpdateFlagParameter(flagMap);
    EXPECT_EQ(FLAGS_v, 3);
    EXPECT_EQ(FLAGS_max_log_file_num, 20ul);
    EXPECT_EQ(FLAGS_logbufsecs, 15);  // 15s
}

TEST_F(FlagsTest, TestValidateFlagName)
{
    Flags flag;
    EXPECT_FALSE(flag.ValidateFlagName("logbufsecs"));
    EXPECT_TRUE(flag.ValidateFlagName("log_compress"));
}

TEST_F(FlagsTest, TestFindFirstSeparator)
{
    Flags flag;
    std::string context1 = " -flagName=value\r\n ";
    auto index1 = flag.FindFirstSeparator(context1);
    EXPECT_EQ(index1, 0);

    std::string context2 = "-flagName=value\r\n ";
    auto index2 = flag.FindFirstSeparator(context2);
    EXPECT_EQ(index2, 15);

    std::string context3 = "-flagName=value\n ";
    auto index3 = flag.FindFirstSeparator(context3);
    EXPECT_EQ(index3, 15);

    std::string context4 = "-flagName=value";
    auto index4 = flag.FindFirstSeparator(context3);
    EXPECT_EQ(index4, 15);

    std::string context5 = "-flagName=value\t";
    auto index5 = flag.FindFirstSeparator(context5);
    EXPECT_EQ(index5, 15);
}

TEST_F(FlagsTest, TestProcessOptions)
{
    Flags flag;
    std::string fileContext =
        "-heartbeat_interval_ms=10\n--v=3\r\n-log_async_queue_size=4096\r-log_compress=false\t-max_log_file_num=22 "
        "-arena_per_tenant=30";
    auto flagMap = flag.ProcessOptions(fileContext);
    flag.UpdateFlagParameter(flagMap);
    EXPECT_EQ(FLAGS_heartbeat_interval_ms, 10);
    EXPECT_EQ(FLAGS_v, 3);
    EXPECT_EQ(FLAGS_log_async_queue_size, static_cast<uint32_t>(4096));
    EXPECT_FALSE(FLAGS_log_compress);
    EXPECT_EQ(FLAGS_max_log_file_num, static_cast<uint32_t>(22));
    EXPECT_EQ(FLAGS_arena_per_tenant, static_cast<uint32_t>(30));
}

TEST_F(FlagsTest, TestWorkerFlagIsToHandle)
{
    std::unordered_map<std::string, std::string> flagMap;
    EXPECT_FALSE(WorkerFlagIsToHandle(flagMap, "loglevel_only_for_workers"));
    EXPECT_TRUE(WorkerFlagIsToHandle(flagMap, "v"));

    FLAGS_worker_address = "127.0.0.1:8888";
    flagMap.emplace(std::pair<std::string, std::string>("loglevel_only_for_workers", "128.0.0.1"));
    EXPECT_FALSE(WorkerFlagIsToHandle(flagMap, "v"));
}

TEST_F(FlagsTest, TestWorkerFlagValidateSpecial)
{
    std::string flagName = "node_dead_timeout_s";
    std::string newVal = "50";
    EXPECT_TRUE(WorkerFlagValidateSpecial(flagName, newVal));
    flagName = "heartbeat_interval_ms";
    newVal = "70000";
    EXPECT_TRUE(WorkerFlagValidateSpecial(flagName, newVal));
}

}  // namespace ut
}  // namespace datasystem
