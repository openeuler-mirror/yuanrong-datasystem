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
 * Description: Config file monitor empty-path gate unit tests.
 */
#include "datasystem/client/client_flags_monitor.h"

#include "datasystem/common/flags/config_monitor_state.h"
#include "datasystem/common/flags/dynamic_config_updater.h"
#include "datasystem/common/flags/dynamic_flag_config.h"

#include "gtest/gtest.h"

DS_DECLARE_int32(v);
DS_DECLARE_string(monitor_config_file);

namespace datasystem {

class FlagsMonitorGateTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
        FLAGS_monitor_config_file = "";
        FLAGS_v = 0;
        unsetenv("DATASYSTEM_CLIENT_CONFIG_PATH");
    }

    void TearDown() override
    {
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
        FLAGS_v = 0;
        unsetenv("DATASYSTEM_CLIENT_CONFIG_PATH");
    }
};

TEST_F(FlagsMonitorGateTest, StartSkipsThreadWhenPathEmpty)
{
    setenv("DATASYSTEM_CLIENT_CONFIG_PATH", "", 1);
    FlagsMonitor::GetInstance()->Start();
    EXPECT_FALSE(FlagsMonitor::GetInstance()->IsMonitorThreadRunning());
    EXPECT_FALSE(ConfigMonitorState::Instance().IsFileMonitorEnabled());
}

TEST_F(FlagsMonitorGateTest, UpdateConfigAppliesViaFlagsMonitorFlags)
{
    DynamicConfigUpdater updater(FlagsMonitor::GetInstance()->GetDynamicFlagConfig());
    auto status = updater.ApplyJson(R"({"v":"2"})");
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(FLAGS_v, 2);
}

}  // namespace datasystem
