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
#include <gmock/gmock.h>

#include "ut/common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/common_flags.h"
#define private public
#include "datasystem/common/flags/dynamic_flag_config.h"
#undef private

DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_bool(log_compress);
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_uint32(log_async_queue_size);
DS_DECLARE_uint32(max_log_file_num);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_double(request_sample_rate);

namespace datasystem {
namespace ut {
class FlagsTest : public CommonTest {};

TEST_F(FlagsTest, TestTrimSpace)
{
    DynamicFlagConfig flagConfig;
    std::string command1 = " -FlagName=Value  ";
    flagConfig.TrimSpace(command1);
    EXPECT_EQ(command1, "-FlagName=Value");

    std::string command2 = " -FlagName = Value  ";
    flagConfig.TrimSpace(command2);
    EXPECT_EQ(command2, "-FlagName = Value");
}

TEST_F(FlagsTest, TestSplitArgument)
{
    DynamicFlagConfig flagConfig;
    // Valid parameter.
    std::string command1 = "--v=Value";
    std::pair<std::string, std::string> kv1;
    EXPECT_TRUE(flagConfig.SplitArgument(command1.c_str(), kv1));
    EXPECT_EQ(kv1.first, "v");
    EXPECT_EQ(kv1.second, "Value");

    // Valid parameter. Spaces are not allowed before and after the equal sign (=).
    std::string command2 = "--v = Value";
    std::pair<std::string, std::string> kv2;
    EXPECT_FALSE(flagConfig.SplitArgument(command2.c_str(), kv2));
    EXPECT_NE(kv2.first, "v");
    EXPECT_NE(kv2.second, "Value");

    // Invalid parameter.
    std::string command3 = "-xx=Value";
    std::pair<std::string, std::string> kv3;
    EXPECT_FALSE(flagConfig.SplitArgument(command3.c_str(), kv3));
    EXPECT_NE(kv3.first, "xx");
    EXPECT_NE(kv3.second, "Value");
}

TEST_F(FlagsTest, TestUpdateFlagParameter)
{
    DynamicFlagConfig flagConfig;
    std::unordered_map<std::string, std::string> flagMap{ { "v", "3" },
                                                          { "max_log_file_num", "20" },
                                                          { "log_compress", "true" } };
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());
    EXPECT_EQ(FLAGS_v, 3);
    EXPECT_EQ(FLAGS_max_log_file_num, 20ul);
    EXPECT_TRUE(FLAGS_log_compress);
}

TEST_F(FlagsTest, TestUpdateFlagParameterRejectsWholeBatchOnInvalidValue)
{
    DynamicFlagConfig flagConfig;
    auto oldV = FLAGS_v;
    auto oldMaxLogFileNum = FLAGS_max_log_file_num;
    std::unordered_map<std::string, std::string> flagMap{ { "v", "3" },
                                                          { "max_log_file_num", "20" },
                                                          { "minloglevel", "invalid" } };
    auto status = flagConfig.UpdateFlagParameter(flagMap);
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("minloglevel"));
    EXPECT_EQ(FLAGS_v, oldV);
    EXPECT_EQ(FLAGS_max_log_file_num, oldMaxLogFileNum);
}

TEST_F(FlagsTest, TestUpdateFlagParameterSkipsUnchangedValue)
{
    DynamicFlagConfig flagConfig;
    auto oldLogMonitor = FLAGS_log_monitor;
    std::unordered_map<std::string, std::string> flagMap{
        { "log_monitor", oldLogMonitor ? "1" : "0" }
    };
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());
    EXPECT_EQ(FLAGS_log_monitor, oldLogMonitor);
}

TEST_F(FlagsTest, TestUpdateFlagParameterCommitFailureRollsBackBatch)
{
    DynamicFlagConfig flagConfig;
    flagConfig.SetBatchCommitHandler([](const auto &) { return false; });
    auto oldLogCompress = FLAGS_log_compress;
    auto oldRequestSampleRate = FLAGS_request_sample_rate;
    FLAGS_log_compress = false;
    FLAGS_request_sample_rate = 1.0;

    std::unordered_map<std::string, std::string> flagMap{ { "log_compress", "true" },
                                                          { "request_sample_rate", "0.7" } };
    auto status = flagConfig.UpdateFlagParameter(flagMap);
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("failed to update sampler flags"));
    EXPECT_FALSE(FLAGS_log_compress);
    EXPECT_DOUBLE_EQ(FLAGS_request_sample_rate, 1.0);

    FLAGS_log_compress = oldLogCompress;
    FLAGS_request_sample_rate = oldRequestSampleRate;
}

TEST_F(FlagsTest, TestUpdateFlagParameterSkipsFilteredFlags)
{
    DynamicFlagConfig flagConfig;
    flagConfig.SetIsToHandle([](const auto &, const std::string &flagName) { return flagName != "log_compress"; });
    auto oldLogCompress = FLAGS_log_compress;
    auto oldMaxLogFileNum = FLAGS_max_log_file_num;
    FLAGS_log_compress = false;
    FLAGS_max_log_file_num = 0;

    std::unordered_map<std::string, std::string> flagMap{ { "log_compress", "true" },
                                                          { "max_log_file_num", "5" } };
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());
    EXPECT_FALSE(FLAGS_log_compress);
    EXPECT_EQ(FLAGS_max_log_file_num, 5ul);

    FLAGS_log_compress = oldLogCompress;
    FLAGS_max_log_file_num = oldMaxLogFileNum;
}

TEST_F(FlagsTest, TestMaskFlagValueUsesOperationLoggerSensitiveList)
{
    DynamicFlagConfig flagConfig;
    EXPECT_EQ(flagConfig.MaskFlagValue("etcd_password", "secret"), "xxx");
    EXPECT_EQ(flagConfig.MaskFlagValue("log_compress", "1"), "1");
}

TEST_F(FlagsTest, TestEraseInfoCoversSensitiveFlagList)
{
    DynamicFlagConfig flagConfig;
    char arg0[] = "worker";
    char arg1[] = "--etcd_password=secret";
    char arg2[] = "-yuanrong_iam_key";
    char arg3[] = "iam-secret";
    char arg4[] = "--log_dir=/tmp";
    char *argv[] = { arg0, arg1, arg2, arg3, arg4 };
    EXPECT_TRUE(flagConfig.EraseInfo(5, argv).IsOk());
    EXPECT_STREQ(argv[1], "--etcd_password");
    EXPECT_STREQ(argv[2], "-yuanrong_iam_key");
    EXPECT_STREQ(argv[3], "");
    EXPECT_STREQ(argv[4], "--log_dir=/tmp");
}

TEST_F(FlagsTest, TestValidateFlagName)
{
    DynamicFlagConfig flagConfig;
    EXPECT_FALSE(flagConfig.ValidateFlagName("logbufsecs"));
    EXPECT_TRUE(flagConfig.ValidateFlagName("log_compress"));
    EXPECT_TRUE(flagConfig.ValidateFlagName("minloglevel"));
    EXPECT_TRUE(flagConfig.ValidateFlagName("log_monitor"));
}

TEST_F(FlagsTest, TestFindFirstSeparator)
{
    DynamicFlagConfig flagConfig;
    std::string context1 = " -flagName=value\r\n ";
    auto index1 = flagConfig.FindFirstSeparator(context1);
    EXPECT_EQ(index1, 0);

    std::string context2 = "-flagName=value\r\n ";
    auto index2 = flagConfig.FindFirstSeparator(context2);
    EXPECT_EQ(index2, 15);

    std::string context3 = "-flagName=value\n ";
    auto index3 = flagConfig.FindFirstSeparator(context3);
    EXPECT_EQ(index3, 15);

    std::string context4 = "-flagName=value";
    auto index4 = flagConfig.FindFirstSeparator(context3);
    EXPECT_EQ(index4, 15);

    std::string context5 = "-flagName=value\t";
    auto index5 = flagConfig.FindFirstSeparator(context5);
    EXPECT_EQ(index5, 15);
}

TEST_F(FlagsTest, TestProcessOptions)
{
    auto oldLogAsyncQueueSize = FLAGS_log_async_queue_size;
    DynamicFlagConfig flagConfig;
    std::string fileContext =
        "--v=3\r\n-log_async_queue_size=4096\r-log_compress=false\t-max_log_file_num=22 "
        "-arena_per_tenant=30";
    auto flagMap = flagConfig.ProcessOptions(fileContext);
    EXPECT_EQ(flagMap.count("log_async_queue_size"), 0ul);
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());
    EXPECT_EQ(FLAGS_v, 3);
    EXPECT_EQ(FLAGS_log_async_queue_size, oldLogAsyncQueueSize);
    EXPECT_FALSE(FLAGS_log_compress);
    EXPECT_EQ(FLAGS_max_log_file_num, static_cast<uint32_t>(22));
    EXPECT_EQ(FLAGS_arena_per_tenant, static_cast<uint32_t>(30));
}

TEST_F(FlagsTest, TestProcessOptionsUpdatesRuntimeLogControlFlags)
{
    auto oldMinLogLevel = FLAGS_minloglevel;
    auto oldLogMonitor = FLAGS_log_monitor;
    FLAGS_minloglevel = 0;
    FLAGS_log_monitor = true;

    DynamicFlagConfig flagConfig;
    auto flagMap = flagConfig.ProcessOptions("-minloglevel=2\n-log_monitor=false");
    EXPECT_EQ(flagMap.count("minloglevel"), 1ul);
    EXPECT_EQ(flagMap.count("log_monitor"), 1ul);
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(flagMap).IsOk());
    EXPECT_EQ(FLAGS_minloglevel, 2);
    EXPECT_FALSE(FLAGS_log_monitor);

    FLAGS_minloglevel = oldMinLogLevel;
    FLAGS_log_monitor = oldLogMonitor;
}

TEST_F(FlagsTest, TestNodeDeadTimeoutFlagValidateSpecial)
{
    DynamicFlagConfig flagConfig;
    flagConfig.SetValidateSpecial(NodeDeadTimeoutFlagValidateSpecial);
    auto oldNodeTimeoutS = FLAGS_node_timeout_s;
    auto oldNodeDeadTimeoutS = FLAGS_node_dead_timeout_s;

    FLAGS_node_timeout_s = 60;
    std::string flagName = "node_dead_timeout_s";
    EXPECT_TRUE(NodeDeadTimeoutFlagValidateSpecial(flagName, "50"));
    EXPECT_EQ(FLAGS_node_dead_timeout_s, oldNodeDeadTimeoutS);

    FLAGS_node_timeout_s = 1;
    EXPECT_TRUE(NodeDeadTimeoutFlagValidateSpecial(flagName, "2"));
    EXPECT_EQ(FLAGS_node_dead_timeout_s, oldNodeDeadTimeoutS);

    EXPECT_FALSE(NodeDeadTimeoutFlagValidateSpecial(flagName, "30"));
    EXPECT_EQ(FLAGS_node_dead_timeout_s, oldNodeDeadTimeoutS);

    FLAGS_node_dead_timeout_s = 4;
    std::unordered_map<std::string, std::string> validBatch{ { "node_dead_timeout_s", "30" } };
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(validBatch).IsOk());
    EXPECT_EQ(FLAGS_node_dead_timeout_s, 30ul);

    std::unordered_map<std::string, std::string> rejectBatch{ { "node_dead_timeout_s", "2" } };
    EXPECT_TRUE(flagConfig.UpdateFlagParameter(rejectBatch).IsError());
    EXPECT_EQ(FLAGS_node_dead_timeout_s, 30ul);

    FLAGS_node_timeout_s = oldNodeTimeoutS;
    FLAGS_node_dead_timeout_s = oldNodeDeadTimeoutS;
}

TEST_F(FlagsTest, TestNodeDeadTimeoutMustBeGreaterThanNodeTimeout)
{
    auto oldNodeTimeoutS = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;

    EXPECT_FALSE(ValidateNodeDeadTimeoutS(3));
    EXPECT_TRUE(ValidateNodeDeadTimeoutS(4));

    FLAGS_node_timeout_s = 1;
    EXPECT_FALSE(ValidateNodeDeadTimeoutS(2));

    FLAGS_node_timeout_s = oldNodeTimeoutS;
}

TEST_F(FlagsTest, TestValidateNodeTimeoutFlags)
{
    auto oldNodeTimeoutS = FLAGS_node_timeout_s;
    auto oldNodeDeadTimeoutS = FLAGS_node_dead_timeout_s;

    FLAGS_node_timeout_s = 3;
    FLAGS_node_dead_timeout_s = 4;
    EXPECT_TRUE(ValidateNodeTimeoutFlags());
    EXPECT_TRUE(ValidateNodeTimeoutS(3));
    EXPECT_FALSE(ValidateNodeTimeoutS(2));

    FLAGS_node_timeout_s = 60;
    FLAGS_node_dead_timeout_s = 300;
    EXPECT_TRUE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = 300;
    FLAGS_node_dead_timeout_s = 60;
    EXPECT_FALSE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = 2;
    FLAGS_node_dead_timeout_s = 300;
    EXPECT_FALSE(ValidateNodeTimeoutFlags());

    FLAGS_node_timeout_s = oldNodeTimeoutS;
    FLAGS_node_dead_timeout_s = oldNodeDeadTimeoutS;
}

}  // namespace ut
}  // namespace datasystem
