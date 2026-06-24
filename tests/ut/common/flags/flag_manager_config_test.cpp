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
 * Description: Tests for FlagManager config file parsing.
 */

#include "datasystem/common/flags/flag_manager.h"

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include "common.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(worker_address);

namespace datasystem {
namespace ut {

class FlagManagerConfigTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldWorkerAddress_ = FLAGS_worker_address;
        tempDir_ = MakeTempDir();
    }

    void TearDown() override
    {
        FLAGS_worker_address = oldWorkerAddress_;
        if (!tempDir_.empty()) {
            (void)RemoveAll(tempDir_);
        }
    }

    std::string WriteTempConfig(const std::string &content)
    {
        auto path = JoinPath(tempDir_, "worker_config.json");
        std::ofstream f(path);
        f << content;
        f.close();
        return path;
    }

    static std::string MakeTempDir()
    {
        auto pattern = std::string("/tmp/worker_config_ut_XXXXXX");
        std::vector<char> buf(pattern.begin(), pattern.end());
        buf.push_back('\0');
        auto *dir = mkdtemp(buf.data());
        return dir == nullptr ? "" : std::string(dir);
    }

protected:
    std::string tempDir_;
    std::string oldWorkerAddress_;
};

TEST_F(FlagManagerConfigTest, EmptyPathReturnsError)
{
    std::string errMsg;
    EXPECT_FALSE(FlagManager::GetInstance()->ParseConfigFile("", errMsg));
    EXPECT_FALSE(errMsg.empty());
}

TEST_F(FlagManagerConfigTest, FileNotFoundReturnsError)
{
    std::string errMsg;
    EXPECT_FALSE(FlagManager::GetInstance()->ParseConfigFile(
        "/nonexistent/path/worker_config.json", errMsg));
    EXPECT_FALSE(errMsg.empty());
}

TEST_F(FlagManagerConfigTest, InvalidJsonReturnsError)
{
    std::string errMsg;
    auto path = WriteTempConfig("not valid json {{{");
    EXPECT_FALSE(FlagManager::GetInstance()->ParseConfigFile(path, errMsg));
    EXPECT_FALSE(errMsg.empty());
}

TEST_F(FlagManagerConfigTest, EmptyObjectReturnsError)
{
    std::string errMsg;
    auto path = WriteTempConfig("{}");
    EXPECT_FALSE(FlagManager::GetInstance()->ParseConfigFile(path, errMsg));
    EXPECT_FALSE(errMsg.empty());
}

TEST_F(FlagManagerConfigTest, ValidConfigParsesCorrectly)
{
    std::string errMsg;
    auto path = WriteTempConfig(R"({"worker_address": {"value": "10.0.0.1:9999"}})");
    EXPECT_TRUE(FlagManager::GetInstance()->ParseConfigFile(path, errMsg)) << errMsg;
    EXPECT_EQ(FLAGS_worker_address, "10.0.0.1:9999");
}

TEST_F(FlagManagerConfigTest, ConfigWithNoValueFieldIsSkipped)
{
    std::string errMsg;
    auto path = WriteTempConfig(R"({"key_without_value": {"desc": "no value field"}})");
    EXPECT_FALSE(FlagManager::GetInstance()->ParseConfigFile(path, errMsg));
    EXPECT_NE(errMsg.find("no valid flags"), std::string::npos);
}

}  // namespace ut
}  // namespace datasystem
