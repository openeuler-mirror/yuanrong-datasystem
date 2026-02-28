/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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
 * Description: A very, very boring logging test.
 */
#include "datasystem/common/log/log.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <re2/re2.h>

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include "common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log_manager.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_uint32(stderrthreshold);
DS_DECLARE_bool(log_compress);
DS_DECLARE_string(log_filename);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_dir);
DS_DECLARE_int32(logbufsecs);
DS_DECLARE_uint32(max_log_file_num);
DS_DECLARE_uint32(max_log_size);

namespace datasystem {
namespace ut {
class LoggingTest : public CommonTest {
public:
    Status CreateTextFile(const std::string &filename, size_t len)
    {
        std::ofstream osf;
        std::string fullPath = FLAGS_log_dir + "/" + filename;
        osf.open(fullPath, std::ios::out);
        if (!osf.is_open()) {
            RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, "create txt file failed: " + fullPath);
        }

        std::string content = rand_.GetRandomString(len);
        osf << content << std::endl;
        osf.close();
        return Status::OK();
    }

    Status CreateLogFiles(int fileNum, int fileSize, bool enableCompress, bool isClient)
    {
        for (int i = 0; i < fileNum; ++i) {
            std::stringstream filename;
            filename << "ds_llt";
            if (isClient) {
                filename << "_" << getpid();
            }

            filename << ".INFO." << GetCurrentTimestamp();
            const std::string log_suffix = enableCompress ? ".log.gz" : ".log";
            filename << log_suffix;
            DS_EXPECT_OK(CreateTextFile(filename.str(), fileSize));
            auto interval = std::chrono::milliseconds(1000);
            std::this_thread::sleep_for(interval);
        }

        return Status::OK();
    }

    template <typename F, typename Rep, typename Period>
    bool Retry(F const &func, const int times, std::chrono::duration<Rep, Period> interval)
    {
        int count = 0;
        while (count < times) {
            ++count;
            if (count > 0) {
                LOG(INFO) << "Retry " << count << " times";
            }

            bool result = func();
            if (result) {
                return true;
            }
            std::this_thread::sleep_for(interval);
        }

        return false;
    }

    bool VerifyLogFiles(const std::string &dir_path, const std::string &pattern, int leftFileNum)
    {
        re2::RE2 re(pattern);
        DIR *dir = opendir(dir_path.c_str());
        if (!dir) {
            LOG(ERROR) << "Failed to open directory: " << dir_path << std::endl;
            return false;
        }

        int count = 0;
        struct dirent *entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string filename = entry->d_name;
            if (re2::RE2::FullMatch(filename, re)) {
                ++count;
                if (count > leftFileNum) {
                    return false;
                }
            }
        }

        closedir(dir);
        return count == leftFileNum;
    }

    void TestRollingFiles(bool enableCompress)
    {
        FLAGS_log_compress = enableCompress;
        FLAGS_log_filename = "ds_llt";
        FLAGS_max_log_size = 1;
        FLAGS_max_log_file_num = 5;
        DS_EXPECT_OK(CreateLogFiles(10, 1024 * 1024, enableCompress, false));
        std::string logPattern = "ds_llt\\.INFO\\.\\d{14}\\.log";

        if (enableCompress) {
            logPattern += ".gz";
            while (true) {
                bool execute = false;
                DS_ASSERT_OK(LogManager::DoLogFileCompress(execute));
                if (!execute) {
                    break;
                }
            }
        }

        DS_EXPECT_OK(LogManager::DoLogFileRolling());
        ASSERT_TRUE(VerifyLogFiles(FLAGS_log_dir, logPattern, FLAGS_max_log_file_num));
    }

    void MultiTimeCostLogger()
    {
        Logging::GetInstance()->Start("ds_llt", true, 1);
        LOG(INFO) << "Start TestMultipleLogAndCompress. ";

        auto handle = Logging::AccessRecorderManagerInstance();
        const int poolSize = 4;
        ThreadPool threadPool(poolSize);
        std::vector<std::future<void>> futures;
        for (int i = 0; i < poolSize; i++) {
            futures.emplace_back(threadPool.Submit([handle, i]() {
                const int logTimes = 100000;
                for (int times = 0; times < logTimes; times++) {
                    handle->LogPerformance("MonitorLogTest" + std::to_string(i), AccessKeyType::CLIENT, times);
                }
            }));
        }

        for (auto &future : futures) {
            future.get();
        }

        LOG(INFO) << "End TestMultipleLogAndCompress.";
    }

    void DeleteFilesMatching(const std::string &dir_path, const std::string &pattern)
    {
        re2::RE2 re(pattern);
        DIR *dir = opendir(dir_path.c_str());
        if (!dir) {
            LOG(ERROR) << "Failed to open directory: " << dir_path << std::endl;
            return;
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string filename = entry->d_name;
            if (re2::RE2::FullMatch(filename, re)) {
                std::string full_path = dir_path + "/" + filename;
                if (unlink(full_path.c_str()) != 0) {
                    LOG(ERROR) << "Failed to delete file: " << full_path << ", error: " << strerror(errno) << std::endl;
                }
            }
        }

        closedir(dir);
    }

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

protected:
    RandomData rand_;
};

TEST_F(LoggingTest, TestFlagsLogDirEmpty)
{
    FLAGS_log_dir = "";
    Logging::GetInstance()->Start("ds_llt", true, 1);
    std::string homeDir;
    DS_EXPECT_OK(Uri::GetHomeDir(homeDir));
    ASSERT_EQ(FLAGS_log_dir, homeDir + "/.datasystem/logs");
    DS_EXPECT_OK(DeleteFile(FLAGS_log_dir + "/" + FLAGS_log_filename + ".INFO.log"));
}

TEST_F(LoggingTest, TestCompressFiles)
{
    FLAGS_log_compress = true;
    FLAGS_log_filename = "compress_test";

    int num = 5;
    for (int i = 0; i < num; ++i) {
        std::stringstream filename;
        filename << "compress_test.INFO.2025102016493" << i << ".log";
        DS_EXPECT_OK(CreateTextFile(filename.str(), 1000 * 1000));
        auto interval = std::chrono::milliseconds(1000);
        std::this_thread::sleep_for(interval);
    }

    while (true) {
        bool execute = false;
        DS_ASSERT_OK(LogManager::DoLogFileCompress(execute));
        if (!execute) {
            break;
        }
    }

    std::string log_pattern = "compress_test\\.INFO\\.\\d{14}\\.log";
    std::string gz_pattern = "compress_test\\.INFO\\.\\d{14}\\.log.gz";
    EXPECT_FALSE(VerifyLogFiles(FLAGS_log_dir, log_pattern, num));
    EXPECT_TRUE(VerifyLogFiles(FLAGS_log_dir, gz_pattern, num));
    DeleteFilesMatching(FLAGS_log_dir, gz_pattern);
}

TEST_F(LoggingTest, TestRollingFiles)
{
    TestRollingFiles(false);
}

TEST_F(LoggingTest, TestRollingGZFiles)
{
    TestRollingFiles(true);
}

TEST_F(LoggingTest, TestEnvSucceed)
{
    constexpr int NUM_LOG_FILES_TO_CREATE = 10;
    constexpr size_t LOG_FILE_SIZE_BYTES = 1024 * 1024;
    DS_EXPECT_OK(CreateLogFiles(NUM_LOG_FILES_TO_CREATE, LOG_FILE_SIZE_BYTES, true, true));

    int replace = 1;
    (void)setenv(LOG_DIR_ENV.c_str(), FLAGS_log_dir.c_str(), replace);
    (void)setenv(MAX_LOG_SIZE_ENV.c_str(), "1", replace);
    (void)setenv(MAX_LOG_FILE_NUM_ENV.c_str(), "5", replace);
    (void)setenv(LOG_COMPRESS_ENV.c_str(), "true", replace);
    (void)setenv(LOG_RETENTION_DAY_ENV.c_str(), "0", replace);

    Logging::GetInstance()->Start("ds_llt", true, 1);
    std::string pattern = "ds_llt(_\\d+)?\\.INFO\\.\\d{14}\\.log\\.gz";
    auto interval = std::chrono::milliseconds(1000);
    constexpr int EXPECTED_LOG_FILE_COUNT = 5;
    constexpr int RETRY_TIMEOUT_SECONDS = 30;
    ASSERT_TRUE(
        Retry([this, &pattern]() -> bool { return VerifyLogFiles(FLAGS_log_dir, pattern, EXPECTED_LOG_FILE_COUNT); },
              RETRY_TIMEOUT_SECONDS, interval));
}

TEST_F(LoggingTest, DISABLED_TestMultiTimeCostLoggerRecord)
{
    FLAGS_log_monitor = true;
    FLAGS_max_log_size = 10;
    FLAGS_log_compress = false;
    (void)MultiTimeCostLogger();
    Logging::AccessRecorderManagerInstance()->ResetWriteLogger(false);

    std::stringstream ssTimeCostFile;
    ssTimeCostFile << FLAGS_log_dir.c_str() << "/" << CLIENT_ACCESS_LOG_NAME << "_[0-9]*\\.log*";
    std::string pattern = ssTimeCostFile.str();
    std::vector<std::string> files;
    DS_ASSERT_OK(Glob(pattern, files));

    sleep(10);
    int totalNum = 0;
    int expectNum = 100000 * 4;
    for (const auto &file : files) {
        FILE *fp = fopen(file.c_str(), "r");
        int c;
        do {
            c = fgetc(fp);
            if (c == '\n') {
                totalNum++;
            }
        } while (c != EOF);
        auto rc = fclose(fp);
        EXPECT_EQ(rc, 0);
    }
    ASSERT_EQ(expectNum, totalNum);
}

TEST_F(LoggingTest, TestMultiTimeCostLoggerCompress)
{
    FLAGS_log_compress = true;
    FLAGS_log_monitor = true;
    FLAGS_max_log_size = 10;

    (void)MultiTimeCostLogger();

    // wait compress success
    auto interval = std::chrono::milliseconds(10000);
    std::this_thread::sleep_for(interval);

    std::stringstream ssTimeCostFile;
    ssTimeCostFile << FLAGS_log_dir.c_str() << "/" << CLIENT_ACCESS_LOG_NAME << "_[0-9]*\\.log*gz";
    std::string pattern = ssTimeCostFile.str();
    std::vector<std::string> files;
    DS_ASSERT_OK(Glob(pattern, files));
    LOG(INFO) << VectorToString(files);
    ASSERT_GT(files.size(), 0ul);
}

TEST_F(LoggingTest, TestMonitorLogMaxLogFileNum)
{
    FLAGS_log_compress = true;
    FLAGS_log_monitor = true;
    FLAGS_max_log_size = 1;

    (void)MultiTimeCostLogger();

    // wait compress success
    std::stringstream ssTimeCostFile;
    ssTimeCostFile << FLAGS_log_dir.c_str() << "/" << CLIENT_ACCESS_LOG_NAME << "_[0-9]*\\.log*gz";
    std::string pattern = ssTimeCostFile.str();
    int timeout = 10;
    bool success = false;
    Timer timer;
    auto interval = std::chrono::milliseconds(500);
    while (timer.ElapsedSecond() < timeout) {
        std::vector<std::string> files;
        DS_ASSERT_OK(Glob(pattern, files));
        LOG(INFO) << VectorToString(files);
        if (files.size() == FLAGS_max_log_file_num) {
            success = true;
            break;
        }

        std::this_thread::sleep_for(interval);
    }

    ASSERT_TRUE(success);
}

TEST_F(LoggingTest, TestCostAutoWriteToLog)
{
    FLAGS_log_monitor = true;

    Logging::GetInstance()->Start("ds_llt", true, 1);
    auto handle = Logging::AccessRecorderManagerInstance();
    const int poolSize = 4;

    ThreadPool threadPool(poolSize);

    std::vector<std::future<void>> futures;
    for (int i = 0; i < poolSize; i++) {
        futures.emplace_back(threadPool.Submit([handle, i]() {
            const int logTimes = 100;
            auto interval = std::chrono::milliseconds(100);
            for (int times = 0; times < logTimes; times++) {
                std::this_thread::sleep_for(interval);
                handle->LogPerformance("MonitorLogTest" + std::to_string(i), AccessKeyType::CLIENT, times);
            }
        }));
    }

    auto interval = std::chrono::milliseconds(50 * 100);
    std::this_thread::sleep_for(interval);
    DS_ASSERT_OK(LogManager::DoLogMonitorWrite());
    interval = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(interval);

    std::stringstream ssTimeCostFile;
    ssTimeCostFile << FLAGS_log_dir.c_str() << "/" << CLIENT_ACCESS_LOG_NAME << "_[0-9]*\\.log";
    std::string pattern = ssTimeCostFile.str();
    std::vector<std::string> files;
    DS_ASSERT_OK(Glob(pattern, files));
    size_t fileSize = 1;
    ASSERT_EQ(files.size(), fileSize);
    auto firstSize = FileSize(files[0]);
    ASSERT_NE(firstSize, (size_t)0);

    for (auto &future : futures) {
        future.get();
    }

    DS_ASSERT_OK(LogManager::DoLogMonitorWrite());

    while (true) {
        bool execute = false;
        DS_ASSERT_OK(LogManager::DoLogFileCompress(execute));
        if (!execute) {
            break;
        }
    }
    std::this_thread::sleep_for(interval);
    auto lastSize = FileSize(files[0]);
    ASSERT_GT(lastSize, firstSize);
}

TEST_F(LoggingTest, TestShutdownHardDiskExporter)
{
    const int testCount = 100;
    for (int i = 0; i < testCount; i++) {
        HardDiskExporter exporter;
        exporter.Init("TestShutdownHardDiskExporter");
    }
}

TEST_F(LoggingTest, TestWriteLogToFile)
{
    std::string message = "This is mock message.";
    std::string filepath = FLAGS_log_dir + "/container.log";
    Logging::GetInstance()->Start("ds_llt", true, 1);
    Logging::GetInstance()->WriteLogToFile(__LINE__, __FILE__, filepath, 'E', message);
    ASSERT_TRUE(FileExist(filepath));
    std::ifstream ifs(filepath);
    ASSERT_TRUE(ifs.is_open());
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    std::string fileContent = buffer.str();
    ifs.close();
    ASSERT_TRUE(fileContent.find('E') != std::string::npos);
    ASSERT_TRUE(fileContent.find(message) != std::string::npos);
}

TEST_F(LoggingTest, TestWriteLogWhenChangeEnv)
{
    Logging::GetInstance()->Start("ds_llt", true);
    std::vector<std::thread> threads;
    const int testTimeSec = 2;
    threads.emplace_back([] {
        Timer timer;
        while (timer.ElapsedSecond() < testTimeSec) {
            LOG(INFO) << "hello";
        }
    });

    threads.emplace_back([] {
        Timer timer;
        while (timer.ElapsedSecond() < testTimeSec) {
            auto v = setenv("HOSTNAME", "val1", 1);
            (void)v;
            unsetenv("POD_NAME");
        }
    });

    threads.emplace_back([] {
        Timer timer;
        while (timer.ElapsedSecond() < testTimeSec) {
            auto v = setenv("POD_NAME", "val2", 1);
            (void)v;
            unsetenv("HOSTNAME");
        }
    });
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(LoggingTest, TestMinLogLevelNotWriteToFile)
{
    int replace = 1;
    (void)setenv("DATASYSTEM_LOG_ASYNC_ENABLE", "false", replace);
    FLAGS_logbufsecs = 0;
    (void)setenv("DATASYSTEM_MIN_LOG_LEVEL", "1", replace);
    Logging::GetInstance()->Start("ds_llt", true);
    std::string infoLog = "This is info log.";
    std::string warningLog = "This is warning log.";
    std::string errorLog = "This is error log.";

    LOG(INFO) << infoLog;
    LOG(WARNING) << warningLog;
    LOG(ERROR) << errorLog;

    bool isAccessLogExist = false;
    for (const auto &filename : GetFilesInDirectory(FLAGS_log_dir)) {
        if (filename.find("ds_llt") != std::string::npos) {
            if (filename.find(".INFO.log") != std::string::npos) {
                ASSERT_FALSE(FileContains(filename, infoLog));
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".WARNING.log") != std::string::npos) {
                ASSERT_TRUE(FileContains(filename, warningLog));
                ASSERT_TRUE(FileContains(filename, errorLog));
            } else if (filename.find(".ERROR.log") != std::string::npos) {
                ASSERT_TRUE(FileContains(filename, errorLog));
            }
        } else if (filename.find("ds_client_access") != std::string::npos) {
            isAccessLogExist = FileExist(filename);
        }
    }

    ASSERT_TRUE(isAccessLogExist);
}

TEST_F(LoggingTest, TestMinLogLevelNotCallFunction)
{
    auto expensiveCall = [] {
        sleep(1);
        return "hello";
    };
    FLAGS_minloglevel = 1;
    Timer timer;
    const int loopCount = 10;
    for (int i = 0; i < loopCount; i++) {
        LOG(INFO) << expensiveCall();
    }
    ASSERT_LT(timer.ElapsedSecond(), 1);
    LOG(ERROR) << "cost: " << timer.ElapsedMicroSecond();
}

TEST_F(LoggingTest, TestDisableClientLogMonitor)
{
    int replace = 1;
    (void)setenv("DATASYSTEM_LOG_MONITOR_ENABLE", "false", replace);
    Logging::GetInstance()->Start("ds_llt", true);

    for (const auto &filename : GetFilesInDirectory(FLAGS_log_dir)) {
        ASSERT_FALSE(filename.find("ds_client_access") != std::string::npos);
    }
}

TEST_F(LoggingTest, TestLogFileDeleted)
{
    FLAGS_max_log_size = 1;
    Logging::GetInstance()->Start("ds_llt", false, 1);
    auto interval = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(interval);

    std::string filename = FLAGS_log_dir + "/ds_llt.INFO.log";
    DS_EXPECT_OK(DeleteFile(filename));

    const std::string log_message = "xxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\n";
    const size_t log_len = log_message.size();
    const size_t target_size_mb = 1;  // Ensure log rolling is triggered
    const size_t approx_lines = (target_size_mb * MB_TO_BYTES) / log_len;
    interval = std::chrono::milliseconds(1);
    for (size_t i = 0; i < approx_lines; ++i) {
        LOG(INFO) << log_message;
        std::this_thread::sleep_for(interval);
    }

    ASSERT_TRUE(FileExist(filename));
}

TEST_F(LoggingTest, TestAlsoLogToStderr)
{
    testing::internal::CaptureStderr();

    FLAGS_alsologtostderr = false;
    Logging::GetInstance()->Start("ds_llt", true, 1);
    auto interval = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(interval);

    LOG(INFO) << "Test FLAGS_alsologtostderr" << std::endl;

    std::string output = testing::internal::GetCapturedStderr();
    EXPECT_EQ(output.find("Test FLAGS_alsologtostderr"), std::string::npos);
}

TEST_F(LoggingTest, TestStderrThreshold)
{
    int replace = 1;
    (void)setenv("DATASYSTEM_LOG_ASYNC_ENABLE", "false", replace);
    testing::internal::CaptureStderr();

    FLAGS_stderrthreshold = 1;  // LogSeverity::WARNING = 1
    Logging::GetInstance()->Start("ds_llt", true, 1);
    auto interval = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(interval);

    LOG(INFO) << "StderrThresholdTest: INFO level message" << std::endl;
    LOG(WARNING) << "StderrThresholdTest: WARNING level message" << std::endl;
    LOG(ERROR) << "StderrThresholdTest: ERROR level message" << std::endl;

    std::string output = testing::internal::GetCapturedStderr();
    EXPECT_EQ(output.find("StderrThresholdTest: INFO level message"), std::string::npos);
    EXPECT_NE(output.find("StderrThresholdTest: WARNING level message"), std::string::npos);
    EXPECT_NE(output.find("StderrThresholdTest: ERROR level message"), std::string::npos);
}

TEST_F(LoggingTest, TestMaxLogSize)
{
    int replace = 1;
    (void)setenv(MAX_LOG_SIZE_ENV.c_str(), "5", replace);
    Logging::GetInstance()->Start("ds_llt", true, 1);
    EXPECT_EQ(FLAGS_max_log_size, 5);  // 5 MB
}

TEST_F(LoggingTest, TestLogName)
{
    int replace = 1;
    (void)setenv(LOG_NAME_ENV.c_str(), "test_client", replace);
    (void)setenv(ACCESS_LOG_NAME_ENV.c_str(), "test_client_access", replace);
    Logging::GetInstance()->Start("ds_llt", true, 1);
    auto interval = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(interval);

    std::string filepath = FLAGS_log_dir + "/test_client_access.log";
    ASSERT_TRUE(FileExist(filepath));
    filepath = FLAGS_log_dir + "/test_client.INFO.log";
    ASSERT_TRUE(FileExist(filepath));
}
}  // namespace ut
}  // namespace datasystem
