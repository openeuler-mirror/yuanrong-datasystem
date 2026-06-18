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
#include "datasystem/common/util/gflag/dynamic_config_updater.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/gflag/config_monitor_state.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

DS_DECLARE_int32(v);
DS_DECLARE_double(request_sample_rate);

namespace datasystem {

class DynamicConfigUpdaterTest : public ::testing::Test {
protected:
    Flags flags_;

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

TEST_F(DynamicConfigUpdaterTest, ApplyValidJson)
{
    DynamicConfigUpdater updater(flags_);
    auto status = updater.ApplyJson(R"({"v":"2"})");
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(FLAGS_v, 2);
}

TEST_F(DynamicConfigUpdaterTest, RejectNonStringValue)
{
    DynamicConfigUpdater updater(flags_);
    auto status = updater.ApplyJson(R"({"v":2})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("invalid JSON"));
}

TEST_F(DynamicConfigUpdaterTest, AggregateMultipleErrors)
{
    DynamicConfigUpdater updater(flags_);
    auto status = updater.ApplyJson(R"({"not_a_flag":"1","v":"bad"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("not in trust list"));
}

TEST_F(DynamicConfigUpdaterTest, RejectWhenFileMonitorEnabled)
{
    ConfigMonitorState::Instance().SetFileMonitorEnabled(true);
    DynamicConfigUpdater updater(flags_);
    auto status = updater.ApplyJson(R"({"v":"2"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("file monitor is enabled"));
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, AllOrNothingOnValidationFailure)
{
    FLAGS_v = 0;
    DynamicConfigUpdater updater(flags_);
    auto status = updater.ApplyJson(R"({"v":"3","not_a_flag":"1"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, RejectWhenSpecialValidationFailsBeforeCommit)
{
    flags_.SetValidateSpecial([](const std::string &flagName, const std::string &newVal) {
        return flagName == "v" && newVal == "4";
    });
    DynamicConfigUpdater updater(flags_);

    auto status = updater.ApplyJson(R"({"v":"4"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("special validation rejected"));
    EXPECT_EQ(FLAGS_v, 0);
}

TEST_F(DynamicConfigUpdaterTest, RejectSpecialValidationBeforePartialCommit)
{
    flags_.SetValidateSpecial([](const std::string &flagName, const std::string &newVal) {
        return flagName == "v" && newVal == "4";
    });
    DynamicConfigUpdater updater(flags_);

    auto status = updater.ApplyJson(R"({"request_sample_rate":"0.5","v":"4"})");
    EXPECT_TRUE(status.IsError());
    EXPECT_THAT(status.GetMsg(), testing::HasSubstr("special validation rejected"));
    EXPECT_DOUBLE_EQ(FLAGS_request_sample_rate, 1.0);
    EXPECT_EQ(FLAGS_v, 0);
}

}  // namespace datasystem
