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

#include <cstdlib>
#include <functional>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/spdlog/logger_provider.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/rdma/urma_manager.h"
#include "datasystem/common/urma_mock/abi/mock_abi.h"
#include "datasystem/common/util/file_util.h"

namespace datasystem {
class UrmaConnectionTestAccess {
public:
    UrmaConnectionTestAccess() = delete;
    ~UrmaConnectionTestAccess() = delete;

    static Status RegisterLog()
    {
        return UrmaManager::Instance().RegisterUrmaLog();
    }

    static Status UnregisterLog()
    {
        return UrmaManager::Instance().UnRegisterUrmaLog();
    }
};

namespace {
class UrmaLogTest : public testing::Test {
protected:
    void SetUp() override
    {
        oldMinLogLevel_ = FLAGS_minloglevel;
        FLAGS_minloglevel = LogSeverity::INFO;
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
        previousProvider_ = Provider::Instance().GetLoggerProvider();
        char logDir[] = "/tmp/ds-urma-log-XXXXXX";
        ASSERT_NE(mkdtemp(logDir), nullptr);
        logDir_ = logDir;
        loggerProvider_ = std::make_shared<LoggerProvider>(GlobalLogParam{});
        Provider::Instance().SetLoggerProvider(loggerProvider_);
        LogParam param;
        param.logDir = logDir_;
        param.fileNamePatterns = { "urma_log.INFO", "urma_log.WARNING", "urma_log.ERROR" };
        param.alsoLog2Stderr = true;
        param.stderrLogLevel = "INFO";
        ASSERT_NE(loggerProvider_->InitDsLogger(param), nullptr);
        urma_dlopen::Cleanup();
        ASSERT_TRUE(urma_dlopen::Init());
        ASSERT_TRUE(UrmaConnectionTestAccess::RegisterLog().IsOk());
        ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags({ 0.0, 0.0, 0.0 }));
    }

    void TearDown() override
    {
        EXPECT_TRUE(UrmaConnectionTestAccess::UnregisterLog().IsOk());
        char message[] = "unregistered callback";
        EXPECT_FALSE(ds_urma_mock_emit_log(URMA_VLOG_LEVEL_ERR, message));
        urma_dlopen::Cleanup();
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
        if (loggerProvider_) {
            loggerProvider_->DropDsLogger();
        }
        Provider::Instance().SetLoggerProvider(previousProvider_);
        FLAGS_minloglevel = oldMinLogLevel_;
        if (!logDir_.empty()) {
            EXPECT_TRUE(RemoveAll(logDir_).IsOk());
        }
    }

    std::string CaptureLogs(const std::function<void()> &action)
    {
        testing::internal::CaptureStderr();
        action();
        EXPECT_TRUE(loggerProvider_->ForceFlush());
        return testing::internal::GetCapturedStderr();
    }

    void EmitLog(int level, std::string message)
    {
        EXPECT_TRUE(ds_urma_mock_emit_log(level, &message[0]));
    }

private:
    int oldMinLogLevel_ = 0;
    std::string logDir_;
    std::shared_ptr<LoggerProvider> previousProvider_;
    std::shared_ptr<LoggerProvider> loggerProvider_;
};

TEST_F(UrmaLogTest, CallbacksBypassSamplingAndCountThrottle)
{
    auto guard = Trace::Instance().SetRequestTraceUUID();
    const auto traceId = Trace::Instance().GetTraceID();
    const auto output = CaptureLogs([&] {
        for (int i = 0; i < 3; ++i) {
            EmitLog(URMA_VLOG_LEVEL_ERR, "error " + std::to_string(i));
            EmitLog(URMA_VLOG_LEVEL_WARNING, "warning " + std::to_string(i));
        }
        EmitLog(URMA_VLOG_LEVEL_NOTICE, "notice");
        EmitLog(URMA_VLOG_LEVEL_INFO, "sampled info");
        EmitLog(URMA_VLOG_LEVEL_DEBUG, "sampled debug");
        EmitLog(URMA_VLOG_LEVEL_DEBUG + 1, "sampled other");
        EmitLog(URMA_VLOG_LEVEL_ERR, "no available thread slot");
    });
    for (int i = 0; i < 3; ++i) {
        EXPECT_NE(output.find("error " + std::to_string(i)), std::string::npos);
        EXPECT_NE(output.find("warning " + std::to_string(i)), std::string::npos);
    }
    EXPECT_NE(output.find("notice"), std::string::npos);
    EXPECT_NE(output.find(" | E | "), std::string::npos);
    EXPECT_NE(output.find(" | W | "), std::string::npos);
    EXPECT_NE(output.find(traceId), std::string::npos);
    EXPECT_EQ(output.find("sampled "), std::string::npos);
    EXPECT_EQ(output.find("no available thread slot"), std::string::npos);
    EXPECT_EQ(Trace::Instance().GetTraceID(), traceId);
}

}  // namespace
}  // namespace datasystem
