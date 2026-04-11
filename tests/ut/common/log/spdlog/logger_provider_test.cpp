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
 * Description: Test logger provider.
 */
#include "datasystem/common/log/spdlog/logger_provider.h"

#include <chrono>
#include <fstream>
#include <string>

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/spdlog/provider.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {

class LoggerProviderTest : public CommonTest {
protected:
    void TearDown() override
    {
        Provider::Instance().SetLoggerProvider(nullptr);
    }
};

TEST_F(LoggerProviderTest, AsyncInit)
{
    GlobalLogParam globalLogParam;
    EXPECT_EQ(globalLogParam.logBufSecs, 10);            // logBufSecs 10
    EXPECT_EQ(globalLogParam.maxAsyncQueueSize, 65536);  // maxQueueSize 65536
    EXPECT_EQ(globalLogParam.asyncThreadCount, 1);       // threadCount 1

    auto lp = std::make_shared<LoggerProvider>(globalLogParam);
    Provider::Instance().SetLoggerProvider(lp);
    auto provider = Provider::Instance().GetLoggerProvider();
    EXPECT_EQ(provider, lp);
    EXPECT_EQ(provider->GetDsLogger(), nullptr);
}

TEST_F(LoggerProviderTest, GetDsLogger)
{
    auto lp = std::make_shared<LoggerProvider>();
    Provider::Instance().SetLoggerProvider(lp);
    auto provider = Provider::Instance().GetLoggerProvider();
    EXPECT_EQ(provider, lp);
    EXPECT_EQ(provider->GetDsLogger(), nullptr);
}

TEST_F(LoggerProviderTest, DropLogger)
{
    auto lp = std::make_shared<LoggerProvider>();
    LogParam loggerParam;
    (void)lp->InitDsLogger(loggerParam);
    auto logger = lp->GetDsLogger();
    EXPECT_EQ(logger->name(), DS_LOGGER_NAME);

    lp->DropDsLogger();
    logger = lp->GetDsLogger();
    EXPECT_EQ(logger, nullptr);
}

TEST_F(LoggerProviderTest, FlushLogFiles)
{
    GlobalLogParam globalLogParam;
    auto lp = std::make_shared<LoggerProvider>(globalLogParam);
    Provider::Instance().SetLoggerProvider(lp);

    std::vector<std::string> fileNamePatterns = { "ds_llt.INFO", "ds_llt.WARNING", "ds_llt.ERROR" };
    LogParam loggerParam;
    loggerParam.logDir = FLAGS_log_dir;
    loggerParam.logAsync = true;
    loggerParam.fileNamePatterns = fileNamePatterns;
    auto logger = lp->InitDsLogger(loggerParam);
    ASSERT_NE(logger, nullptr);

    std::string infoLog = "This is an info log, ForceFlush!";
    LOG(INFO) << infoLog;
    Provider::Instance().FlushLogs();

    auto interval = std::chrono::milliseconds(100);
    std::this_thread::sleep_for(interval);

    std::string filepath = FLAGS_log_dir + "/" + "ds_llt.INFO.log";
    std::ifstream ifs(filepath);
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    std::string fileContent = buffer.str();
    ifs.close();
    ASSERT_TRUE(fileContent.find(infoLog) != std::string::npos);
}

}  // namespace ut
}  // namespace datasystem