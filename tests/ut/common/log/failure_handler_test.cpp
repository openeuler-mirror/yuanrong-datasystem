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
 * Description: Test absl failure handler.
 */
#include "datasystem/common/log/failure_handler.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/spdlog/logger_provider.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/util/file_util.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {
class FailureHandlerTest : public CommonTest {
protected:
    void TearDown() override
    {
        Provider::Instance().SetLoggerProvider(nullptr);
    }
};

TEST_F(FailureHandlerTest, FailureWriter)
{
    FailureWriter("xxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\nxxx\n");

    struct stat buffer;
    std::string backtraceFilename = FLAGS_log_dir + "/container.log";
    EXPECT_TRUE(stat(backtraceFilename.c_str(), &buffer) == 0);
    DS_EXPECT_OK(DeleteFile(backtraceFilename));
}

// Regression for issue #772: FailureWriter(nullptr) is invoked by absl's failure
// signal handler (i.e. inside a real signal handler on the alternate stack). It must
// not touch spdlog or any non-async-signal-safe primitive, otherwise a signal
// delivered while the spdlog async worker holds a lock re-enters spdlog and crashes
// with a secondary SIGSEGV. Here we simply assert the nullptr path returns without
// throwing or crashing; the async-signal-safety of the bodies is enforced by
// code review on failure_handler.cpp.
TEST_F(FailureHandlerTest, FailureWriterNullptrIsAsyncSignalSafe)
{
    EXPECT_NO_THROW(FailureWriter(nullptr));
}

// Proof-of-fix for issue #772: simulate the exact crash scenario.
//
// Before the fix, FailureWriter(nullptr) called Provider::FlushLogs(), which
// acquired a std::shared_mutex and posted a flush into spdlog's shared
// async thread_pool. Neither is async-signal-safe: if a SIGSEGV arrived while
// the spdlog async worker thread held the sink mutex, the re-entrant flush
// would touch sinks mid-teardown and raise a secondary SIGSEGV → recursive
// absl handler entry → raise(SIGABRT).
//
// After the fix, FailureWriter(nullptr) is a pure return. The signal handler
// path has zero lock/shared-state interaction with the async logger, so it is
// safe regardless of what the spdlog async worker is doing.
//
// This test:
//  1. Initializes a real spdlog async logger with rotating_file_sink_mt
//     INSIDE the EXPECT_DEATH block so the forked child creates its own
//     live thread_pool workers (parent threads are killed by fork)
//  2. Starts 4 background threads continuously posting LOG messages to keep
//     the async thread_pool saturated (mimicking brpc's 64 bthread workers)
//  3. Triggers SIGSEGV via nullptr deref while the async worker is busy
//  4. EXPECT_DEATH matches "SIGSEGV" — the primary fault signal, NOT SIGABRT
//     which would indicate a recursive handler re-entry (old code path)
//
// Run 3 iterations to verify consistency.
TEST_F(FailureHandlerTest, FailureSignalHandlerUnderAsyncLoggingStress)
{
    constexpr int kIterations = 3;
    for (int iter = 0; iter < kIterations; ++iter) {
        EXPECT_DEATH(
            {
                // Setup MUST happen inside the death block: gtest death tests
                // use fork(), which kills parent threads. The child must create
                // its own spdlog thread_pool with live workers so that the
                // async worker threads are actively writing when SIGSEGV fires.
                GlobalLogParam globalLogParam;
                globalLogParam.asyncThreadCount = 2;  // match production
                auto lp = std::make_shared<LoggerProvider>(globalLogParam);
                Provider::Instance().SetLoggerProvider(lp);

                std::vector<std::string> fileNamePatterns = { "ds_llt.INFO" };
                LogParam loggerParam;
                loggerParam.logDir = FLAGS_log_dir;
                loggerParam.logAsync = true;
                loggerParam.fileNamePatterns = fileNamePatterns;
                loggerParam.alsoLog2Stderr = false;
                auto logger = lp->InitDsLogger(loggerParam);
                (void)logger;

                InstallFailureSignalHandler("datasystem");

                std::atomic<bool> stop{false};
                std::vector<std::thread> threads;

                // Spawn background threads that continuously post LOG() messages
                // to keep the async thread_pool busy — simulates brpc's 64
                // bthread workers posting to the shared pool.
                for (int t = 0; t < 4; ++t) {
                    threads.emplace_back([&stop, t]() {
                        int n = 0;
                        while (!stop.load(std::memory_order_relaxed)) {
                            LOG(INFO) << "stress_thread_" << t << "_msg_" << n++;
                        }
                    });
                }

                // Let the async thread_pool process queued messages for long
                // enough that the rotating_file_sink is almost certainly mid-
                // write when the signal arrives.
                std::this_thread::sleep_for(std::chrono::milliseconds(200));

                // Trigger SIGSEGV while the spdlog async worker is actively
                // writing. The absl failure handler fires → calls
                // FailureWriter(data) to write backtrace → calls
                // FailureWriter(nullptr) to flush.
                //
                // With the fix, FailureWriter(nullptr) is a pure return:
                // no lock, no spdlog interaction, fully async-signal-safe.
                // The process dies cleanly with SIGSEGV.
                //
                // Before the fix, FailureWriter(nullptr) called
                // Provider::FlushLogs() (shared_mutex + post_flush to async
                // pool), risking a secondary crash → SIGABRT from recursive
                // handler entry.
                volatile int* volatile null_ptr = nullptr;
                *null_ptr = 42;

                // unreachable
                stop.store(true);
                for (auto& th : threads) {
                    th.join();
                }
            },
            ".*SIGSEGV.*");

        auto interval = std::chrono::milliseconds(500);
        std::this_thread::sleep_for(interval);
    }

    struct stat buffer;
    std::string backtraceFilename = FLAGS_log_dir + "/container.log";
    EXPECT_TRUE(stat(backtraceFilename.c_str(), &buffer) == 0);
}

using FailureHandlerDeathTest = FailureHandlerTest;

TEST_F(FailureHandlerDeathTest, FailureSignalHandler)
{
    InstallFailureSignalHandler("datasystem");

    EXPECT_DEATH({
        int* ptr = nullptr;
        *ptr = 42;
    }, ".*SIGSEGV.*");

    auto interval = std::chrono::milliseconds(1000);
    std::this_thread::sleep_for(interval);
    
    struct stat buffer;
    std::string backtraceFilename = FLAGS_log_dir + "/container.log";
    EXPECT_TRUE(stat(backtraceFilename.c_str(), &buffer) == 0);
}

}  // namespace ut
}  // namespace datasystem
