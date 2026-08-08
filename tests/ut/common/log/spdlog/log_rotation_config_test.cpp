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
 * Description: Test log rotation max-files config and timestamp cleanup behavior.
 */
#include "datasystem/common/log/log_rotation_config.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

namespace datasystem {
namespace ut {
namespace {

std::string MakeTempDir()
{
    std::string pattern = "/tmp/log_rotation_config_ut_XXXXXX";
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    char *dir = ::mkdtemp(buffer.data());
    return dir == nullptr ? "" : std::string(dir);
}

std::size_t CountFilesWithPrefix(const std::string &dir, const std::string &prefix)
{
    std::size_t count = 0;
    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
        const auto filename = entry.path().filename().string();
        if (filename.rfind(prefix, 0) == 0 && filename.find(".log") != std::string::npos) {
            ++count;
        }
    }
    return count;
}

}  // namespace

class LogRotationConfigTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        testCasePath_ = MakeTempDir();
        ASSERT_FALSE(testCasePath_.empty());
    }

    void TearDown() override
    {
        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
    }

    std::string testCasePath_;
};

TEST_F(LogRotationConfigTest, ConfiguredLimitIsPassedToSpdlog)
{
    EXPECT_EQ(GetEffectiveSpdlogMaxFileNum(5), 5ul);
    EXPECT_EQ(GetEffectiveSpdlogMaxFileNum(0), HIGHEST_SPDLOG_MAX_FILE_NUM);
}

TEST_F(LogRotationConfigTest, TimestampRotationHonorsMaxFilesIncludingActiveFile)
{
    constexpr std::size_t maxFiles = 5;
    constexpr std::size_t maxSize = 256;
    const std::string prefix = "ds_llt_limit.INFO";
    const std::string logPath = testCasePath_ + "/" + prefix + ".log";

    auto sink = std::make_shared<ds_spdlog::sinks::rotating_file_sink_mt>(logPath, maxSize, maxFiles);
    auto logger = std::make_shared<ds_spdlog::logger>("test_max_files", sink);

    for (int i = 0; i < 20; ++i) {
        logger->info("{}", std::string(maxSize, static_cast<char>('A' + (i % 26))));
        logger->flush();
        ASSERT_LE(CountFilesWithPrefix(testCasePath_, prefix), maxFiles);
    }

    EXPECT_TRUE(std::filesystem::exists(logPath));
    EXPECT_LE(CountFilesWithPrefix(testCasePath_, prefix), maxFiles);
    ds_spdlog::drop("test_max_files");
}

}  // namespace ut
}  // namespace datasystem
