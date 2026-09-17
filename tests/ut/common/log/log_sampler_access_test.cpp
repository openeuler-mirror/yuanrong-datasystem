/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

    void EnableSampler(double requestRate, double accessRate, double diagnosticRate)
    {
        LogSampleUserConfig cfg;
        cfg.requestSampleRate = requestRate;
        cfg.accessSampleRate = accessRate;
        cfg.diagnosticSampleRate = diagnosticRate;
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

// ShouldRecordAccess: accessRate=0 drops access even when request sampling admits every trace
TEST_F(LogSamplerAccessTest, AccessRateZeroDropsEvenWhenRequestFull)
{
    EnableSampler(1.0, 0.0, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    EXPECT_FALSE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
    EXPECT_FALSE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_POSIX_CREATE));
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

// ShouldRecordAccess: per-trace access sampling independent of the request decision
TEST_F(LogSamplerAccessTest, AccessPerTraceSampling)
{
    EnableSampler(0.0, 0.5, 1.0);
    LogSampler::Instance().SetSaltForTest(42);

    int hits = 0;
    constexpr int kNumTraces = 1000;
    for (int i = 0; i < kNumTraces; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
        bool firstCall = LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET);
        // Per-trace decision: repeated calls within one trace are stable
        EXPECT_EQ(firstCall, LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
        if (firstCall) {
            ++hits;
        }
    }

    double ratio = static_cast<double>(hits) / kNumTraces;
    EXPECT_NEAR(ratio, 0.5, 0.08);
}

TEST_F(LogSamplerAccessTest, NoRequestContextAccessBypassSampler)
{
    EnableSampler(0.0, 0.0, 0.0);

    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());

    // ShouldRecordAccess: CLIENT/ACCESS/REQUEST_OUT all bypass sampler
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_POSIX_CREATE));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_ETCD_PUT));

    // ShouldRecordAccessType: all types bypass sampler
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::CLIENT));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::ACCESS));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::REQUEST_OUT));
}

// ShouldRecordAccessType: CLIENT/ACCESS per-trace (with request context)
TEST_F(LogSamplerAccessTest, ShouldRecordAccessTypeClientAccess)
{
    EnableSampler(0.0, 0.5, 1.0);
    LogSampler::Instance().SetSaltForTest(42);

    int clientHits = 0;
    int accessHits = 0;
    constexpr int kNumTraces = 1000;
    for (int i = 0; i < kNumTraces; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
        if (LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::CLIENT)) {
            ++clientHits;
        }
        if (LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::ACCESS)) {
            ++accessHits;
        }
    }

    double clientRatio = static_cast<double>(clientHits) / kNumTraces;
    double accessRatio = static_cast<double>(accessHits) / kNumTraces;
    EXPECT_NEAR(clientRatio, 0.5, 0.08);
    EXPECT_NEAR(accessRatio, 0.5, 0.08);
}

// LS-007b: LOG_SAMPLE_NONE receiving side — background cross-node RPC access bypass
TEST_F(LogSamplerAccessTest, LogSampleNoneReceivingSideAccessBypass)
{
    EnableSampler(0.0, 0.0, 0.0);
    LogSampler::Instance().SetSaltForTest(0);

    // Simulate full cross-node path: background thread → LOG_SAMPLE_NONE → receiving side
    TraceGuard guard = Trace::Instance().SetTraceNewID("bg_thread_trace");
    EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());

    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_POSIX_GINCREASEREF));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::CLIENT));
    EXPECT_TRUE(LogSampler::Instance().ShouldRecordAccessType(AccessKeyType::ACCESS));
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

    int recorded = 0;
    int skipped = 0;
    constexpr int kNumTraces = 1000;
    constexpr double kSamplingTolerance = 0.08;
    for (int i = 0; i < kNumTraces; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
        if (LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET)) {
            auto access = AccessRecorder::Object(AccessRecorderKey::DS_KV_CLIENT_SET);
            access.ObjectKeyRef("key").Result(0).DataSize(100).Record();
            ++recorded;
        } else {
            ++skipped;
        }
    }

    double ratio = static_cast<double>(recorded) / kNumTraces;
    EXPECT_NEAR(ratio, 0.5, kSamplingTolerance);
    EXPECT_EQ(recorded + skipped, kNumTraces);
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

// Nested thresholds: access_rate >= request_rate keeps access for every sampled-in trace
TEST_F(LogSamplerAccessTest, NestingAccessAboveRequest)
{
    EnableSampler(0.5, 0.8, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    constexpr int kAttempts = 1000;
    constexpr double kSamplingTolerance = 0.08;
    int sampledInCount = 0;
    int accessCount = 0;
    for (int i = 0; i < kAttempts; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

        bool sampledIn = LogSampler::Instance().IsCurrentRequestSampledIn();
        bool accessAllowed = LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET);
        if (sampledIn) {
            ++sampledInCount;
            EXPECT_TRUE(accessAllowed);
        }
        if (accessAllowed) {
            ++accessCount;
        }
    }

    EXPECT_GE(accessCount, sampledInCount);
    double reqRatio = static_cast<double>(sampledInCount) / kAttempts;
    double accRatio = static_cast<double>(accessCount) / kAttempts;
    EXPECT_NEAR(reqRatio, 0.5, kSamplingTolerance);
    EXPECT_NEAR(accRatio, 0.8, kSamplingTolerance);
}

// Nested thresholds: access_rate < request_rate — the whole access budget lands on
// sampled-in traces (access-kept ⊆ sampled-in); the broken band is the minimum
// under the volume constraint
TEST_F(LogSamplerAccessTest, AccessBelowRequestBreaksLink)
{
    EnableSampler(0.5, 0.2, 1.0);
    LogSampler::Instance().SetSaltForTest(0);

    constexpr int kAttempts = 1000;
    constexpr double kSamplingTolerance = 0.08;
    int sampledInCount = 0;
    int sampledInWithAccess = 0;
    int nonSampledInWithAccess = 0;
    for (int i = 0; i < kAttempts; ++i) {
        Trace::Instance().Invalidate();
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();

        bool sampledIn = LogSampler::Instance().IsCurrentRequestSampledIn();
        bool accessAllowed = LogSampler::Instance().ShouldRecordAccess(AccessRecorderKey::DS_KV_CLIENT_SET);
        if (sampledIn) {
            ++sampledInCount;
            if (accessAllowed) {
                ++sampledInWithAccess;
            }
        } else if (accessAllowed) {
            ++nonSampledInWithAccess;
        }
    }

    EXPECT_EQ(nonSampledInWithAccess, 0);
    EXPECT_GT(sampledInCount, sampledInWithAccess);
    double accRatio = static_cast<double>(sampledInWithAccess) / kAttempts;
    EXPECT_NEAR(accRatio, 0.2, kSamplingTolerance);
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
