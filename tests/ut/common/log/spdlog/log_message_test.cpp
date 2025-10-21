/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test log message.
 */
#include "datasystem/common/log/log.h"

#include <string>
#include <thread>
#include <chrono>
#include <fstream>

#include <dirent.h>

#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/spdlog/logger_provider.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {
class LogMessageTest : public CommonTest {
public:
    void SetUp() override
    {
        GlobalLogParam globalLogParam;
        auto lp = std::make_shared<LoggerProvider>(globalLogParam);
        Provider::Instance().SetLoggerProvider(lp);

        CreateDsLogger();
    }

    void TearDown() override
    {
        DropDsLogger();

        Provider::Instance().SetLoggerProvider(nullptr);
    }

    void CreateDsLogger()
    {
        std::vector<std::string> fileNamePatterns = { "ds_llt.INFO", "ds_llt.WARNING", "ds_llt.ERROR" };
        LogParam loggerParam;
        loggerParam.logDir = FLAGS_log_dir;
        loggerParam.alsoLog2Stderr = true;
        loggerParam.fileNamePatterns = fileNamePatterns;

        auto lp = Provider::Instance().GetLoggerProvider();
        ASSERT_NE(lp, nullptr);
        auto logger = lp->InitDsLogger(loggerParam);
        ASSERT_NE(logger, nullptr);
    }

    void DropDsLogger()
    {
        auto lp = Provider::Instance().GetLoggerProvider();
        ASSERT_NE(lp, nullptr);
        lp->DropDsLogger();
    }

protected:
    std::vector<std::string> GetFilesInDirectory(const std::string &directory)
    {
        std::vector<std::string> files;
        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir(directory.c_str())) != nullptr) {
            while ((ent = readdir(dir)) != nullptr) {
                files.push_back(directory + "/" + ent->d_name);
            }

            closedir(dir);
        }

        return files;
    }

    bool FileContains(const std::string &filename, const std::string &content)
    {
        std::ifstream file(filename);
        if (!file.is_open()) {
            return false;
        }

        std::string line;
        while (std::getline(file, line)) {
            if (line.find(content) != std::string::npos) {
                return true;
            }
        }

        return false;
    }
};

TEST_F(LogMessageTest, BasicLogging)
{
    std::string infoLog = "This is info log.";
    std::string warningLog = "Warning: value is ";
    std::string errorLog = "Error code: ";

    LOG(INFO) << infoLog;
    LOG(WARNING) << warningLog << 42;  // WARNING_DISK_FULL 42
    LOG(ERROR) << errorLog << 404;     // HTTP-like "not found" error 404

    for (const auto &filename : GetFilesInDirectory(FLAGS_log_dir)) {
        if (filename.find("ds_llt") != std::string::npos) {
            if (filename.find(".INFO.log") != std::string::npos) {
                ASSERT_TRUE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".WARNING.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".ERROR.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_FALSE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            }
        }
    }
}

TEST_F(LogMessageTest, ConditionalLogging)
{
    std::string infoLog = "Debug mode is ON.";
    std::string warningLog = "Retrying operation, attempt: ";
    std::string errorLog = "Error code: ";
    bool isDebugEnabled = false;
    int retryCount = 3;
    int errorCode = 404;

    LOG_IF(INFO, isDebugEnabled) << infoLog;
    LOG_IF(WARNING, retryCount > 0) << warningLog << retryCount;
    LOG_IF(ERROR, errorCode != 0) << errorLog << errorCode;

    for (const auto &filename : GetFilesInDirectory(FLAGS_log_dir)) {
        if (filename.find("ds_llt") != std::string::npos) {
            if (filename.find(".INFO.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".WARNING.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".ERROR.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_FALSE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            }
        }
    }
}

TEST_F(LogMessageTest, VerboseLogging)
{
    FLAGS_v = 1;
    std::string vLog0 = "Verbose log at level 0.";
    std::string vLog1 = "Verbose log at level 1.";
    std::string vLog2 = "Verbose log at level 2.";

    VLOG(0) << vLog0;  // Basic info 0, always recorded
    VLOG(1) << vLog1;  // Debug info 1, recorded when --v=1
    VLOG(2) << vLog2;  // Trace-level 2, not recorded when --v=1

    for (const auto &filename : GetFilesInDirectory(FLAGS_log_dir)) {
        if (filename.find("ds_llt") != std::string::npos) {
            if (filename.find(".INFO.log") != std::string::npos) {
                ASSERT_TRUE(FileContains(filename, vLog0));
                ASSERT_TRUE(FileContains(filename, vLog1));
                ASSERT_FALSE(FileContains(filename, vLog2));
            } else if (filename.find(".WARNING.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, vLog0));
                ASSERT_FALSE(FileContains(filename, vLog1));
                ASSERT_FALSE(FileContains(filename, vLog2));
            } else if (filename.find(".ERROR.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, vLog0));
                ASSERT_FALSE(FileContains(filename, vLog1));
                ASSERT_FALSE(FileContains(filename, vLog2));
            }
        }
    }
}

TEST_F(LogMessageTest, DebugOnlyLogging)
{
    std::string infoLog = "This is info log.";
    std::string warningLog = "Warning: value is ";
    std::string errorLog = "Error code: ";

    DLOG(INFO) << infoLog;
    DLOG(WARNING) << warningLog << 42;            // WARNING_DISK_FULL 42
    DLOG(ERROR) << errorLog << 404 << std::endl;  // HTTP-like "not found" error 404

    for (const auto &filename : GetFilesInDirectory(FLAGS_log_dir)) {
        if (filename.find("ds_llt") != std::string::npos) {
            if (filename.find(".INFO.log") != std::string::npos) {
                ASSERT_TRUE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".WARNING.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".ERROR.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_FALSE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            }
        }
    }
}

TEST_F(LogMessageTest, FrequencyLogging)
{
    // Set verbosity level for VLOG tests
    FLAGS_v = 1;

    // Test: LOG_EVERY_N - logs every N occurrences
    constexpr int kEveryNCount = 5;
    constexpr int kNumIterationsEveryN = 10;
    for (int i = 0; i < kNumIterationsEveryN; ++i) {
        LOG_EVERY_N(INFO, kEveryNCount) << "This logs every " << kEveryNCount << " iterations. Iteration: " << i;
    }

    // Test: LOG_FIRST_N - logs only the first N occurrences
    constexpr int kFirstNCount = 3;
    constexpr int kNumIterationsFirstN = 10;
    for (int i = 0; i < kNumIterationsFirstN; ++i) {
        LOG_FIRST_N(INFO, kFirstNCount) << "This logs only first " << kFirstNCount << " times. Iteration: " << i;
    }

    // Test: LOG_EVERY_T - logs every T seconds
    constexpr int kEveryTSeconds = 2;
    constexpr int kNumIterationsEveryT = 30;
    constexpr auto kSleepIntervalMs = std::chrono::milliseconds(100);
    for (int i = 0; i < kNumIterationsEveryT; ++i) {
        LOG_EVERY_T(INFO, kEveryTSeconds) << "This logs every " << kEveryTSeconds << " seconds. Iteration: " << i;
        std::this_thread::sleep_for(kSleepIntervalMs);
    }

    // Test: LOG_IF_EVERY_N - logs every N occurrences when condition is true
    constexpr int kIfEveryNCount = 3;
    constexpr int kNumIterationsIfEveryN = 30;
    constexpr int kCheckEvenModulo = 2;
    for (int i = 0; i < kNumIterationsIfEveryN; ++i) {
        LOG_IF_EVERY_N(INFO, (i % kCheckEvenModulo == 0), kIfEveryNCount)
            << "Even number: " << i << " (logged every " << kIfEveryNCount << " even numbers)";
    }

    // Test: VLOG_EVERY_N at different verbosity levels
    constexpr int kVlogEveryNCount = 10;
    constexpr int kNumIterationsVlog = 30;

    constexpr int kVerboseLevelBasic = 1;
    // This will be visible when --v=1 or higher
    for (int i = 0; i < kNumIterationsVlog; ++i) {
        VLOG_EVERY_N(kVerboseLevelBasic, kVlogEveryNCount)
            << "Verbose log at level 1, every " << kVlogEveryNCount << " iterations. Iteration: " << i;
    }

    constexpr int kVerboseLevelDetailed = 2;
    // This will only be visible when --v>=2
    for (int i = 0; i < kNumIterationsVlog; ++i) {
        VLOG_EVERY_N(kVerboseLevelDetailed, kVlogEveryNCount)
            << "Verbose log at level 2, every " << kVlogEveryNCount << " iterations. Iteration: " << i;
    }
}

TEST_F(LogMessageTest, AllCheckMacrosPassWhenConditionTrue)
{
    // Test: CHECK(true) - basic unconditional pass
    CHECK(true) << "Basic CHECK(true) should not fail. This is a diagnostic message.";

    // Test: Equality check with meaningful values
    constexpr int expected_value = 100;
    constexpr int actual_value = 100;
    CHECK_EQ(expected_value, actual_value);

    // Test: Inequality check
    constexpr int left_value = 50;
    constexpr int right_value = 75;
    CHECK_NE(left_value, right_value);

    // Test: Less than
    constexpr int smaller = 30;
    constexpr int larger = 40;
    CHECK_LT(smaller, larger);

    // Test: Less than or equal
    constexpr int equal_val = 60;
    CHECK_LE(equal_val, equal_val);
    CHECK_LE(smaller, larger);

    // Test: Greater than
    CHECK_GT(larger, smaller);

    // Test: Greater than or equal
    CHECK_GE(equal_val, equal_val);
    CHECK_GE(larger, smaller);

    // Test: Pointer is not null
    int valid_data = 999;  // meaningful name
    int *valid_ptr = &valid_data;
    CHECK_NOTNULL(valid_ptr);

    // Test: String inequality
    const char *first_string = "apple";
    const char *second_string = "banana";
    CHECK_STRNE(first_string, second_string);
}

using LogMessageDeathTest = LogMessageTest;

TEST_F(LogMessageDeathTest, FatalCausesAbort)
{
    EXPECT_DEATH(
        {
            LOG(FATAL) << "Fatal message!";  // will not raise signal
        },
        ".*Fatal message!.*");
}

TEST_F(LogMessageDeathTest, CheckFailCausesAbort)
{
    EXPECT_DEATH(
        {
            CHECK(false);
        },
        ".*Check failed: false.*");
}

}  // namespace ut
}  // namespace datasystem
