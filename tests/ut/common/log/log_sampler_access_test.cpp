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
 * Description: Unit tests for LogSampler access guard (Step 4).
 */

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/access_recorder.h"

#include <cstdint>

#include "ut/common.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace ut {

class LogSamplerAccessTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        LogSampler::Instance().ResetForTest();
    }

    void TearDown() override
    {
        LogSampler::Instance().ResetForTest();
        CommonTest::TearDown();
    }

    void EnableSampler(double requestRate, double accessRate, double diagnosticRate,
                       bool reqExplicit = true, bool accExplicit = true, bool diagExplicit = true)
    {
        LogSampleUserConfig cfg;
        cfg.requestSampleRate = requestRate;
        cfg.requestSampleRateExplicit = reqExplicit;
        cfg.accessSampleRate = accessRate;
        cfg.accessSampleRateExplicit = accExplicit;
        cfg.diagnosticSampleRate = diagnosticRate;
        cfg.diagnosticSampleRateExplicit = diagExplicit;
        ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));
    }
};

// ShouldRecordAccess: sampler disabled → always pass
TEST_F(LogSamplerAccessTest, SamplerDisabledAccessPass)
{
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_POSIX_CREATE));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_ETCD_PUT));
}

// ShouldRecordAccess: accessRate=1.0 → pass without key→type mapping
TEST_F(LogSamplerAccessTest, AccessRateFullPassThrough)
{
    EnableSampler(0.5, 1.0, 1.0);
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
}

// ShouldRecordAccess: REQUEST_OUT always pass
TEST_F(LogSamplerAccessTest, RequestOutAlwaysPass)
{
    EnableSampler(0.0, 0.0, 0.0);
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_ETCD_PUT));
}

// ShouldRecordAccess: request sampled-in forces access output
TEST_F(LogSamplerAccessTest, RequestSampledInForcesAccess)
{
    EnableSampler(1.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    // request_sample_rate=1.0 → IsCurrentRequestSampledIn returns true (no decision created)
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_POSIX_CREATE));
}

// ShouldRecordAccess: accessRate=0 → false (no reject creation)
TEST_F(LogSamplerAccessTest, AccessRateZeroDrop)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    // request_rate=0 → not sampled-in, access_rate=0 → drop
    EXPECT_FALSE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
    EXPECT_FALSE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_POSIX_CREATE));

    // Verify: no reject decision in Trace (access 0.0 does not create reject)
    bool admitted = false;
    bool hasDecision = Trace::Instance().GetRequestSampleDecision(admitted);
    EXPECT_FALSE(hasDecision);
}

// ShouldRecordAccess: per-event access sampling when request not sampled-in
TEST_F(LogSamplerAccessTest, AccessPerEventSampling)
{
    EnableSampler(0.0, 0.5, 1.0);
    LogSampler::Instance().SetSaltForTest(42);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    int hits = 0;
    constexpr int kNumCalls = 1000;
    for (int i = 0; i < kNumCalls; ++i) {
        if (LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET)) {
            ++hits;
        }
    }

    double ratio = static_cast<double>(hits) / kNumCalls;
    EXPECT_NEAR(ratio, 0.5, 0.05);
}

// ShouldRecordAccess: no request context → per-event sampling only
TEST_F(LogSamplerAccessTest, NoRequestContextAccess)
{
    EnableSampler(0.5, 0.5, 1.0);
    LogSampler::Instance().SetSaltForTest(42);

    // No TraceGuard → no request context
    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());

    // IsCurrentRequestSampledIn returns false → access per-event sampling
    int hits = 0;
    constexpr int kNumCalls = 1000;
    for (int i = 0; i < kNumCalls; ++i) {
        if (LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET)) {
            ++hits;
        }
    }

    double ratio = static_cast<double>(hits) / kNumCalls;
    EXPECT_NEAR(ratio, 0.5, 0.05);
}

// ShouldRecordAccessType: CLIENT/ACCESS per-event
TEST_F(LogSamplerAccessTest, ShouldRecordAccessTypeClientAccess)
{
    EnableSampler(0.0, 0.5, 1.0);
    LogSampler::Instance().SetSaltForTest(42);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    int clientHits = 0;
    int accessHits = 0;
    constexpr int kNumCalls = 1000;
    for (int i = 0; i < kNumCalls; ++i) {
        if (LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::CLIENT)) {
            ++clientHits;
        }
        if (LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::ACCESS)) {
            ++accessHits;
        }
    }

    double clientRatio = static_cast<double>(clientHits) / kNumCalls;
    double accessRatio = static_cast<double>(accessHits) / kNumCalls;
    EXPECT_NEAR(clientRatio, 0.5, 0.05);
    EXPECT_NEAR(accessRatio, 0.5, 0.05);
}

// GetAccessKeyType: correct mapping
TEST_F(LogSamplerAccessTest, GetAccessKeyTypeMapping)
{
    EXPECT_EQ(GetAccessKeyType(AccessRecorderKey::DS_KV_CLIENT_SET), AccessKeyType::CLIENT);
    EXPECT_EQ(GetAccessKeyType(AccessRecorderKey::DS_POSIX_CREATE), AccessKeyType::ACCESS);
    EXPECT_EQ(GetAccessKeyType(AccessRecorderKey::DS_ETCD_PUT), AccessKeyType::REQUEST_OUT);
}

TEST_F(LogSamplerAccessTest, RecordBackstopClientReject)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("test_key").Result(0).DataSize(100).Record();
}

TEST_F(LogSamplerAccessTest, RecordBackstopAccessReject)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_CREATE);
    access.ObjectKeyRef("test_key").Result(0).DataSize(100).Record();
}

TEST_F(LogSamplerAccessTest, AccessGuardIntegrationPattern)
{
    EnableSampler(0.0, 0.5, 1.0);
    LogSampler::Instance().SetSaltForTest(42);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

    int recorded = 0;
    int skipped = 0;
    constexpr int kNumCalls = 1000;
    for (int i = 0; i < kNumCalls; ++i) {
        if (LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET)) {
            auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
            access.ObjectKeyRef("key").Result(0).DataSize(100).Record();
            ++recorded;
        } else {
            ++skipped;
        }
    }

    double ratio = static_cast<double>(recorded) / kNumCalls;
    EXPECT_NEAR(ratio, 0.5, 0.05);
    EXPECT_EQ(recorded + skipped, kNumCalls);
}

TEST_F(LogSamplerAccessTest, AccessGuardSkipsConstructionWhenDropped)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    EXPECT_FALSE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));

    EnableSampler(1.0, 1.0, 1.0);
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
}

// LS-003d: Access OR rule boundary — request_rate=0.5 + access_rate=0
TEST_F(LogSamplerAccessTest, AccessOrRuleBoundary)
{
    EnableSampler(0.5, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    constexpr int kAttempts = 1000;
    int sampledInCount = 0;
    int sampledOutCount = 0;
    for (int i = 0; i < kAttempts; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

        bool sampledIn = LogSampler::Instance().IsCurrentRequestSampledIn();
        bool accessAllowed = LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET);

        if (sampledIn) {
            ++sampledInCount;
            EXPECT_TRUE(accessAllowed);
        } else {
            ++sampledOutCount;
            EXPECT_FALSE(accessAllowed);
        }
    }

    EXPECT_GT(sampledInCount, 0);
    EXPECT_GT(sampledOutCount, 0);
    double ratio = static_cast<double>(sampledInCount) / kAttempts;
    EXPECT_NEAR(ratio, 0.5, 0.05);
}

// LS-014: logSampled:true marker — sampler disabled, request context → true
TEST_F(LogSamplerAccessTest, LogSampledMarkerSamplerDisabledWithRequestContext)
{
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    EXPECT_TRUE(IsCurrentRequestLogSampled(AccessKeyType::CLIENT));
    EXPECT_TRUE(IsCurrentRequestLogSampled(AccessKeyType::ACCESS));
}

// LS-014: logSampled:true marker — no request context → false (marker not applicable)
TEST_F(LogSamplerAccessTest, LogSampledMarkerNoRequestContext)
{
    EXPECT_FALSE(IsCurrentRequestLogSampled(AccessKeyType::CLIENT));
}

// LS-014: logSampled:true marker — request sampled-in → true
TEST_F(LogSamplerAccessTest, LogSampledMarkerRequestSampledIn)
{
    EnableSampler(1.0, 0.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    EXPECT_TRUE(IsCurrentRequestLogSampled(AccessKeyType::CLIENT));
}

// LS-014: logSampled:true marker — request rejected → false
TEST_F(LogSamplerAccessTest, LogSampledMarkerRequestRejected)
{
    EnableSampler(0.0, 0.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    EXPECT_FALSE(IsCurrentRequestLogSampled(AccessKeyType::CLIENT));
    EXPECT_FALSE(IsCurrentRequestLogSampled(AccessKeyType::ACCESS));
}

// LS-014: logSampled:true marker — REQUEST_OUT → false
TEST_F(LogSamplerAccessTest, LogSampledMarkerRequestOut)
{
    EXPECT_FALSE(IsCurrentRequestLogSampled(AccessKeyType::REQUEST_OUT));
}

// LS-014: FormatAccessReqMsg adds logSampled:true when request sampled-in
TEST_F(LogSamplerAccessTest, FormatAccessReqMsgSampledIn)
{
    EnableSampler(1.0, 0.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

    std::string result = FormatAccessReqMsg(AccessKeyType::CLIENT, "{objectKey:foo}");
    EXPECT_NE(result.find("logSampled:true"), std::string::npos);
}

// LS-014: FormatAccessReqMsg does NOT add logSampled:true when request rejected
TEST_F(LogSamplerAccessTest, FormatAccessReqMsgRequestRejected)
{
    EnableSampler(0.0, 0.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

    std::string result = FormatAccessReqMsg(AccessKeyType::CLIENT, "{objectKey:foo}");
    EXPECT_EQ(result.find("logSampled:true"), std::string::npos);
}

// LS-014: FormatAccessReqMsg does NOT add logSampled:true for REQUEST_OUT
TEST_F(LogSamplerAccessTest, FormatAccessReqMsgRequestOut)
{
    std::string result = FormatAccessReqMsg(AccessKeyType::REQUEST_OUT, "{objectKey:foo}");
    EXPECT_EQ(result.find("logSampled:true"), std::string::npos);
}

// #26: Access log layout stable — FormatAccessReqMsg field order/separators unchanged
TEST_F(LogSamplerAccessTest, AccessLogLayoutStable)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

    std::string plain = FormatAccessReqMsg(AccessKeyType::CLIENT, "{objectKey:foo,writeMode:0}");
    EXPECT_EQ(plain, "{objectKey:foo,writeMode:0,logSampled:true}");

    std::string emptyBrace = FormatAccessReqMsg(AccessKeyType::CLIENT, "{}");
    EXPECT_EQ(emptyBrace, "{logSampled:true}");

    std::string noBrace = FormatAccessReqMsg(AccessKeyType::CLIENT, "rawMsg");
    EXPECT_EQ(noBrace, "{rawMsg,logSampled:true}");

    std::string emptyInput = FormatAccessReqMsg(AccessKeyType::CLIENT, "");
    EXPECT_EQ(emptyInput, "{logSampled:true}");
}

TEST_F(LogSamplerAccessTest, ShouldRecordTrueSkipsAccessCheck)
{
    EnableSampler(1.0, 1.0, 1.0);
    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("test_key").Result(0).DataSize(100).Record();
}

TEST_F(LogSamplerAccessTest, RecordDefaultFallbackAccessCheck)
{
    EnableSampler(0.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
    access.ObjectKeyRef("test_key").Result(0).DataSize(100).Record();
}

}  // namespace ut
}  // namespace datasystem
