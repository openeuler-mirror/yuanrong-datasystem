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
 * Description: Test rotating_file_sink behavior when reopening a log file fails with EMFILE.
 */
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include <spdlog/async.h>
#include <spdlog/details/file_helper.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

namespace datasystem {
namespace ut {
namespace {

std::string MakeTempDir()
{
    std::string pattern = "/tmp/rotating_file_sink_ut_XXXXXX";
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    char *dir = ::mkdtemp(buffer.data());
    return dir == nullptr ? "" : std::string(dir);
}

}  // namespace

class RotatingSinkReproTest : public ::testing::Test {
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

    static void FillFdsAndTriggerCrash(const std::string &logDir)
    {
        const std::string logPath = logDir + "/ds_llt_repro.INFO.log";
        const std::size_t kMaxSize = 512;

        struct rlimit limit;
        if (::getrlimit(RLIMIT_NOFILE, &limit) != 0) {
            ::_exit(1);
        }
        constexpr rlim_t kTargetSoft = 64;
        limit.rlim_cur = std::min<rlim_t>(limit.rlim_cur, kTargetSoft);
        if (::setrlimit(RLIMIT_NOFILE, &limit) != 0) {
            ::_exit(1);
        }

        ds_spdlog::file_event_handlers handlers;
        handlers.after_close = [](const ds_spdlog::filename_t & /*fn*/) {
            // Consume the FD released by file_helper::close() so that
            // reopen() hits EMFILE deterministically.
            (void)::open("/dev/null", O_RDONLY);
        };

        auto sink = std::make_shared<ds_spdlog::sinks::rotating_file_sink_mt>(logPath, kMaxSize, 1,
                                                                              false /*rotate_on_open*/, handlers);

        auto pool = std::make_shared<ds_spdlog::details::thread_pool>(256 /*queue_size*/, 1 /*one worker*/);
        auto logger = std::make_shared<ds_spdlog::async_logger>("test_repro", sink, pool,
                                                                ds_spdlog::async_overflow_policy::block);
        ds_spdlog::register_logger(logger);

        // Pre-fill sink to near max_size.
        {
            std::string preFill(200, 'A');
            logger->log(ds_spdlog::source_loc{ __FILE__, __LINE__, "" }, ds_spdlog::level::info, preFill);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        // Consume remaining FDs up to EMFILE.
        std::vector<int> fillerFds;
        while (true) {
            int fd = ::open("/dev/null", O_RDONLY);
            if (fd < 0) {
                break;
            }
            fillerFds.push_back(fd);
        }

        // Trigger rotation. The after_close callback takes the released FD, so reopen fails.
        {
            std::string triggerMsg(420, 'B');  // 200 + 420 > 512
            logger->log(ds_spdlog::source_loc{ __FILE__, __LINE__, "" }, ds_spdlog::level::info, triggerMsg);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // The poisoned state triggers another rotation attempt instead of writing through a null FILE pointer.
        {
            std::string bypassMsg(80, 'C');  // 200 + 80 <= 512
            logger->log(ds_spdlog::source_loc{ __FILE__, __LINE__, "" }, ds_spdlog::level::info, bypassMsg);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        ::_exit(0);
    }

    std::string testCasePath_;
};

TEST_F(RotatingSinkReproTest, WriteToClosedFileThrows)
{
    ds_spdlog::details::file_helper fileHelper;
    fileHelper.open(testCasePath_ + "/closed.log");
    fileHelper.close();

    ds_spdlog::memory_buf_t buffer;
    EXPECT_THROW(fileHelper.write(buffer), ds_spdlog::spdlog_ex);
}

TEST_F(RotatingSinkReproTest, EmfileReopenNoLongerCrashes)
{
    EXPECT_EXIT({ FillFdsAndTriggerCrash(testCasePath_); }, ::testing::ExitedWithCode(0), "");
}

}  // namespace ut
}  // namespace datasystem
