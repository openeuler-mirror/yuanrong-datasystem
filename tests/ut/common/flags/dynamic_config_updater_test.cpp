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
 * Description: DynamicConfigUpdater unit tests.
 */
#include "datasystem/common/flags/dynamic_config_updater.h"

#include <limits>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/utils/kv_client_config.h"
#include "datasystem/common/flags/config_monitor_state.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

DS_DECLARE_int32(v);
DS_DECLARE_double(request_sample_rate);

namespace datasystem {

class DynamicConfigUpdaterTest : public ::testing::Test {
protected:
    DynamicFlagConfig flagConfig_;

    void SetUp() override
    {
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
        FLAGS_request_sample_rate = 1.0;
        FLAGS_v = 0;
    }

    void TearDown() override
    {
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
        FLAGS_request_sample_rate = 1.0;
        FLAGS_v = 0;
    }
};

TEST(UrmaLogThresholdConfigTest, StartupFlagDefaultsTo500AndRejectsInvalidValues)
{
    LinkCommonFlagsValidators();
    const uint32_t original = FLAGS_urma_log_threshold_us;
    Raii restore([original] {
        std::string ignored;
        SetCommandLineOption("urma_log_threshold_us", std::to_string(original), ignored);
    });
    EXPECT_EQ(FLAGS_urma_log_threshold_us, 500u);
    EXPECT_EQ(GetUrmaLogThresholdUs(), 500u);
    std::string error;
    for (uint32_t value : { 1u, 250u, 500u, 1000u, std::numeric_limits<uint32_t>::max() }) {
        ASSERT_TRUE(SetCommandLineOption("urma_log_threshold_us", std::to_string(value), error)) << error;
        EXPECT_EQ(FLAGS_urma_log_threshold_us, value);
        EXPECT_EQ(GetUrmaLogThresholdUs(), value);
    }
    for (const std::string &value : { "0", "-1", "4294967296", "1.5", "bad" }) {
        EXPECT_FALSE(SetCommandLineOption("urma_log_threshold_us", value, error)) << value;
        EXPECT_EQ(FLAGS_urma_log_threshold_us, std::numeric_limits<uint32_t>::max());
        EXPECT_EQ(GetUrmaLogThresholdUs(), std::numeric_limits<uint32_t>::max());
    }
}

TEST(UrmaLogThresholdConfigTest, BuilderKeepsMissingFieldUnspecifiedAndPreservesConfigOnFailure)
{
    KVClientConfig config;
    ASSERT_TRUE(KVClientConfig::Builder().Build(config).IsOk());
    EXPECT_EQ(config.GetArgs().count("urma_log_threshold_us"), 0u);
    for (uint32_t value : { 1u, 250u, 500u, 1000u, std::numeric_limits<uint32_t>::max() }) {
        ASSERT_TRUE(KVClientConfig::Builder().UrmaLogThresholdUs(value).Build(config).IsOk());
        EXPECT_EQ(config.GetArgs().at("urma_log_threshold_us"), std::to_string(value));
    }
    const auto status = KVClientConfig::Builder().UrmaLogThresholdUs(0).Build(config);
    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("UrmaLogThresholdUs"));
    EXPECT_EQ(config.GetArgs().at("urma_log_threshold_us"), "4294967295");
}

TEST_F(DynamicConfigUpdaterTest, UrmaLogThresholdRejectsApiAndFileRuntimeUpdates)
{
    const uint32_t original = FLAGS_urma_log_threshold_us;
    EXPECT_FALSE(FlagManager::GetInstance()->IsModifiableFlag("urma_log_threshold_us"));
    DynamicConfigUpdater updater(flagConfig_);
    EXPECT_EQ(updater.ApplyJson(R"({"urma_log_threshold_us":"1000"})").GetCode(), K_INVALID);
    EXPECT_EQ(FLAGS_urma_log_threshold_us, original);
    EXPECT_FALSE(flagConfig_.ValidateFlagName("urma_log_threshold_us"));
    EXPECT_EQ(FLAGS_urma_log_threshold_us, original);
}

TEST_F(DynamicConfigUpdaterTest, ApplyValidJson)
{
    DynamicConfigUpdater updater(flagConfig_);
    auto status = updater.ApplyJson(R"({"v":"2"})");
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(FLAGS_v, 2);
}

TEST_F(DynamicConfigUpdaterTest, RejectNonStringValue)
{
    DynamicConfigUpdater updater(flagConfig_);
    auto status = updater.ApplyJson(R"({"v":2})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("invalid JSON"));
}

TEST_F(DynamicConfigUpdaterTest, AggregateMultipleErrors)
{
    DynamicConfigUpdater updater(flagConfig_);
    auto status = updater.ApplyJson(R"({"not_a_flag":"1","v":"bad"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("not in trust list"));
}

TEST_F(DynamicConfigUpdaterTest, RejectWhenFileMonitorEnabled)
{
    ConfigMonitorState::Instance().SetFileMonitorEnabled(true);
    DynamicConfigUpdater updater(flagConfig_);
    auto status = updater.ApplyJson(R"({"v":"2"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("file monitor is enabled"));
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, AllOrNothingOnValidationFailure)
{
    FLAGS_v = 0;
    DynamicConfigUpdater updater(flagConfig_);
    auto status = updater.ApplyJson(R"({"v":"3","not_a_flag":"1"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, RuntimeApplicabilityFilterRejectsUnsupportedFlagsBeforeCommit)
{
    DynamicConfigUpdater updater(flagConfig_, [](const std::string &flagName) {
        return flagName == "request_sample_rate";
    });

    const auto status = updater.ApplyJson(R"({"request_sample_rate":"0.5","v":"2"})");

    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("not runtime-applicable"));
    EXPECT_DOUBLE_EQ(FLAGS_request_sample_rate, 1.0);
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, RejectWhenSpecialValidationFailsBeforeCommit)
{
    flagConfig_.SetValidateSpecial([](const std::string &flagName, const std::string &newVal) {
        return flagName == "v" && newVal == "2";
    });
    DynamicConfigUpdater updater(flagConfig_);

    auto status = updater.ApplyJson(R"({"v":"2"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("special validation rejected"));
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, RejectSpecialValidationBeforePartialCommit)
{
    flagConfig_.SetValidateSpecial([](const std::string &flagName, const std::string &newVal) {
        return flagName == "v" && newVal == "2";
    });
    DynamicConfigUpdater updater(flagConfig_);

    auto status = updater.ApplyJson(R"({"request_sample_rate":"0.5","v":"2"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("special validation rejected"));
    EXPECT_DOUBLE_EQ(FLAGS_request_sample_rate, 1.0);
    EXPECT_EQ(FLAGS_v, 0);
}

}  // namespace datasystem
