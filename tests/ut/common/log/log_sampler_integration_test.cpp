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
 * Description: Integration test for LOG/PLOG macros with LogSampler.
 */
#include "datasystem/common/log/log.h"

#include "ut/common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/spdlog/logger_provider.h"
#include "datasystem/common/log/trace.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {

namespace {
int BuildSideEffectPayload(int &counter)
{
    ++counter;
    return counter;
}
}  // namespace

class LogSamplerIntegrationTest : public CommonTest {
public:
    void SetUp() override
    {
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();

        GlobalLogParam globalLogParam;
        auto lp = std::make_shared<LoggerProvider>(globalLogParam);
        Provider::Instance().SetLoggerProvider(lp);

        CreateDsLogger();
    }

    void TearDown() override
    {
        DropDsLogger();

        Provider::Instance().SetLoggerProvider(nullptr);
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
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

    LogSampleUserConfig MakeConfig(double requestRate, double diagnosticRate, double accessRate)
    {
        LogSampleUserConfig config;
        config.requestSampleRateExplicit = true;
        config.requestSampleRate = requestRate;
        config.diagnosticSampleRateExplicit = true;
        config.diagnosticSampleRate = diagnosticRate;
        config.accessSampleRateExplicit = true;
        config.accessSampleRate = accessRate;
        return config;
    }
};

TEST_F(LogSamplerIntegrationTest, FATALAlwaysPassMacroLevel)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    EXPECT_DEATH(
        {
            LOG(FATAL) << "Fatal message!";
        },
        ".*Fatal message!.*");
}

TEST_F(LogSamplerIntegrationTest, FATALAlwaysPassBackstop)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    EXPECT_DEATH(
        {
            LogMessage(LogSeverity::FATAL, "test.cc", 1).Stream() << "Direct FATAL";
        },
        ".*Direct FATAL.*");
}

TEST_F(LogSamplerIntegrationTest, RequestSampledInForcesDiagnostic)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(1.0, 0.0, 0.0)));

    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    int evaluations = 0;
    LOG(INFO) << "sampled-in info " << BuildSideEffectPayload(evaluations);
    LOG(WARNING) << "sampled-in warning " << BuildSideEffectPayload(evaluations);
    LOG(ERROR) << "sampled-in error " << BuildSideEffectPayload(evaluations);
    PLOG(INFO) << "sampled-in plog " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 4);
}

TEST_F(LogSamplerIntegrationTest, DiagnosticSupplementSampling)
{
    LogSampler::Instance().SetSaltForTest(0);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.5, 0.0)));

    int errorCount = 0;
    constexpr int kAttempts = 10000;
    for (int i = 0; i < kAttempts; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard tg = Trace::Instance().SetRequestTraceUUID();
        int evals = 0;
        LOG(ERROR) << "diag test " << BuildSideEffectPayload(evals);
        errorCount += evals;
    }
    double ratio = static_cast<double>(errorCount) / kAttempts;
    EXPECT_NEAR(ratio, 0.5, 0.05);
}

TEST_F(LogSamplerIntegrationTest, PlogClassifiedAsDiagnostic)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    int evaluations = 0;
    PLOG(INFO) << "plog rejected " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 0);
}

TEST_F(LogSamplerIntegrationTest, PlogDiagnosticSampling)
{
    LogSampler::Instance().SetSaltForTest(42);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.5, 0.0)));

    int plogCount = 0;
    constexpr int kAttempts = 1000;
    for (int i = 0; i < kAttempts; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard tg = Trace::Instance().SetRequestTraceUUID();
        int evals = 0;
        PLOG(INFO) << "plog diag test " << BuildSideEffectPayload(evals);
        plogCount += evals;
    }
    double ratio = static_cast<double>(plogCount) / kAttempts;
    EXPECT_NEAR(ratio, 0.5, 0.1);
}

TEST_F(LogSamplerIntegrationTest, ForceLogSkipsRejectAsDropButSamplerStillApplies)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    // PLOG bypasses request-reject-as-drop, but diagnostic_rate=0 still drops it
    int evaluations = 0;
    PLOG(INFO) << "plog with diagnostic=0 " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 0);

    // PLOG bypasses request-reject-as-drop, and diagnostic_rate=1 passes it
    LogSampler::Instance().ResetForTest();
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 1.0, 0.0)));

    Trace::Instance().Invalidate();
    TraceGuard tg2 = Trace::Instance().SetRequestTraceUUID();
    PLOG(INFO) << "plog with diagnostic=1 " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 1);
}

TEST_F(LogSamplerIntegrationTest, PlogIfOrVlogWorks)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(1.0, 1.0, 1.0)));

    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    FLAGS_v = 1;
    int evaluations = 0;
    PLOG_IF_OR_VLOG(INFO, true, 1,
                    "plog branch " << BuildSideEffectPayload(evaluations));
    EXPECT_EQ(evaluations, 1);

    PLOG_IF_OR_VLOG(INFO, false, 1,
                    "vlog branch " << BuildSideEffectPayload(evaluations));
    EXPECT_EQ(evaluations, 2);
}

TEST_F(LogSamplerIntegrationTest, VlogRespectsSampler)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    FLAGS_v = 1;
    int evaluations = 0;
    VLOG(1) << "vlog rejected " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 0);
}

TEST_F(LogSamplerIntegrationTest, CheckFatalBypass)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    EXPECT_DEATH(
        {
            CHECK(false);
        },
        ".*Check failed: false.*");
}

TEST_F(LogSamplerIntegrationTest, BackgroundLogNotSampled)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    int evaluations = 0;
    LOG(INFO) << "background info " << BuildSideEffectPayload(evaluations);
    LOG(WARNING) << "background warning " << BuildSideEffectPayload(evaluations);
    LOG(ERROR) << "background error " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 3);
}

// #38: Background PLOG bypasses sampler — no request context PLOG always passes
TEST_F(LogSamplerIntegrationTest, BackgroundPlogBypassesSampler)
{
    LogSampler::Instance().SetSaltForTest(12345);
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(MakeConfig(0.0, 0.0, 0.0)));

    Trace::Instance().Invalidate();
    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());

    int evaluations = 0;
    PLOG(INFO) << "background plog info " << BuildSideEffectPayload(evaluations);
    PLOG(WARNING) << "background plog warning " << BuildSideEffectPayload(evaluations);
    PLOG(ERROR) << "background plog error " << BuildSideEffectPayload(evaluations);
    EXPECT_EQ(evaluations, 3);
}

}  // namespace ut
}  // namespace datasystem
