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
 * Description: Unit tests for LogSampler config integration (Step 5 + Step 7).
 */

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_sampler.h"

#include <cstdint>

#include "ut/common.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/protos/share_memory.pb.h"

namespace datasystem {
namespace ut {

using ConfigUpdateResult = LogSampler::ConfigUpdateResult;

namespace {
int BuildSideEffectPayload(int &counter)
{
    ++counter;
    return counter;
}
}  // namespace

class LogSamplerConfigTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        LogSampler::Instance().ResetForTest();
        LogSampler::Instance().Init();
    }

    void TearDown() override
    {
        LogSampler::Instance().ResetForTest();
        CommonTest::TearDown();
    }

    LogSampleConfigPb MakeProto(bool enabled, uint32_t reqPpm, uint32_t accPpm, uint32_t diagPpm)
    {
        LogSampleConfigPb proto;
        proto.set_enabled(enabled);
        if (enabled) {
            proto.set_request_sample_ppm(reqPpm);
            proto.set_access_sample_ppm(accPpm);
            proto.set_diagnostic_sample_ppm(diagPpm);
        }
        return proto;
    }
};

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoDisabledPassThrough)
{
    auto proto = MakeProto(false, 0, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::UNCHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoEnabledValid)
{
    auto proto = MakeProto(true, 500000, 1000000, 200000);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::CHANGED);
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->config.requestRate.ppm, 500000);
    EXPECT_EQ(snap->config.accessRate.ppm, 1000000);
    EXPECT_EQ(snap->config.diagnosticRate.ppm, 200000);
    EXPECT_EQ(snap->config.requestRate.threshold, BuildThreshold(500000));
    EXPECT_EQ(snap->config.accessRate.threshold, kAlwaysSampleThreshold);
    EXPECT_EQ(snap->config.diagnosticRate.threshold, BuildThreshold(200000));
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoIllegalPpmRejected)
{
    auto proto = MakeProto(true, 1000001, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::INVALID);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());

    proto = MakeProto(true, 0, 1000001, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::INVALID);

    proto = MakeProto(true, 0, 0, 1000001);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::INVALID);
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoZeroPpm)
{
    auto proto = MakeProto(true, 0, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::CHANGED);
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->config.requestRate.ppm, 0);
    EXPECT_EQ(snap->config.requestRate.threshold, 0);
    EXPECT_EQ(snap->config.accessRate.ppm, 0);
    EXPECT_EQ(snap->config.diagnosticRate.ppm, 0);
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoAllPpmBaseFoldToDisabled)
{
    auto proto = MakeProto(true, kSamplePpmBase, kSamplePpmBase, kSamplePpmBase);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::CHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    EXPECT_FALSE(snap->config.enabled);
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoPreviousGoodRetainedOnFailure)
{
    auto goodProto = MakeProto(true, 500000, 500000, 500000);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(goodProto), ConfigUpdateResult::CHANGED);
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());

    auto *snapBefore = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snapBefore, nullptr);
    uint32_t ppmBefore = snapBefore->config.requestRate.ppm;

    auto badProto = MakeProto(true, 1000001, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(badProto), ConfigUpdateResult::INVALID);

    auto *snapAfter = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snapAfter, nullptr);
    EXPECT_EQ(snapAfter->config.requestRate.ppm, ppmBefore);
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoTransitionEnabledToDisabled)
{
    auto enabledProto = MakeProto(true, 500000, 500000, 500000);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(enabledProto), ConfigUpdateResult::CHANGED);
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());

    auto disabledProto = MakeProto(false, 0, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(disabledProto), ConfigUpdateResult::CHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoTransitionDisabledToEnabled)
{
    auto disabledProto = MakeProto(false, 0, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(disabledProto), ConfigUpdateResult::UNCHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());

    auto enabledProto = MakeProto(true, 500000, 500000, 500000);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(enabledProto), ConfigUpdateResult::CHANGED);
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());
}

TEST_F(LogSamplerConfigTest, LogSampleConfigPbDisabledNoPpm)
{
    LogSampleConfigPb proto;
    proto.set_enabled(false);
    EXPECT_EQ(proto.request_sample_ppm(), 0);
    EXPECT_EQ(proto.access_sample_ppm(), 0);
    EXPECT_EQ(proto.diagnostic_sample_ppm(), 0);

    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::UNCHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoBoundaryPpm)
{
    auto proto = MakeProto(true, 1, kSamplePpmBase - 1, kSamplePpmBase);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::CHANGED);

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->config.requestRate.ppm, 1);
    EXPECT_NE(snap->config.requestRate.threshold, 0);
    EXPECT_NE(snap->config.requestRate.threshold, kAlwaysSampleThreshold);
    EXPECT_EQ(snap->config.accessRate.ppm, kSamplePpmBase - 1);
    EXPECT_EQ(snap->config.diagnosticRate.ppm, kSamplePpmBase);
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoRejectsAfterPreviousGood)
{
    auto goodProto = MakeProto(true, 500000, 500000, 500000);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(goodProto), ConfigUpdateResult::CHANGED);

    auto badProto = MakeProto(true, kSamplePpmBase + 1, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(badProto), ConfigUpdateResult::INVALID);

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->config.requestRate.ppm, 500000);
}

TEST_F(LogSamplerConfigTest, UpdateConfigFromProtoNoPreviousGoodFallsBackToDisabled)
{
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());

    auto badProto = MakeProto(true, 1000001, 0, 0);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(badProto), ConfigUpdateResult::INVALID);

    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());
}

// --- Step 7 tests: PopulateConfigProto and IsCurrentRequestSampledIn no-arg ---

TEST_F(LogSamplerConfigTest, PopulateConfigProtoDisabled)
{
    LogSampleConfigPb proto;
    LogSampler::Instance().PopulateConfigProto(&proto);
    EXPECT_FALSE(proto.enabled());
    EXPECT_EQ(proto.request_sample_ppm(), 0);
    EXPECT_EQ(proto.access_sample_ppm(), 0);
    EXPECT_EQ(proto.diagnostic_sample_ppm(), 0);
}

TEST_F(LogSamplerConfigTest, PopulateConfigProtoEnabled)
{
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    cfg.accessSampleRate = 0.3;
    cfg.accessSampleRateExplicit = true;
    cfg.diagnosticSampleRate = 0.4;
    cfg.diagnosticSampleRateExplicit = true;
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));

    LogSampleConfigPb proto;
    LogSampler::Instance().PopulateConfigProto(&proto);
    EXPECT_TRUE(proto.enabled());
    EXPECT_EQ(proto.request_sample_ppm(), 500000);
    EXPECT_EQ(proto.access_sample_ppm(), 300000);
    EXPECT_EQ(proto.diagnostic_sample_ppm(), 400000);
}

TEST_F(LogSamplerConfigTest, PopulateConfigProtoNoSnapshot)
{
    LogSampler::Instance().ResetForTest();

    LogSampleConfigPb proto;
    LogSampler::Instance().PopulateConfigProto(&proto);
    EXPECT_FALSE(proto.enabled());
    EXPECT_EQ(proto.request_sample_ppm(), 0);
}

TEST_F(LogSamplerConfigTest, PopulateConfigProtoEnabledAllFullFoldsToDisabled)
{
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 1.0;
    cfg.accessSampleRate = 1.0;
    cfg.diagnosticSampleRate = 1.0;
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));

    LogSampleConfigPb proto;
    LogSampler::Instance().PopulateConfigProto(&proto);
    EXPECT_FALSE(proto.enabled());
}

TEST_F(LogSamplerConfigTest, IsCurrentRequestSampledInNoArgPassThrough)
{
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());
    EXPECT_TRUE(LogSampler::Instance().IsCurrentRequestSampledIn());
}

TEST_F(LogSamplerConfigTest, IsCurrentRequestSampledInNoArgRandom)
{
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));
    LogSampler::Instance().SetSaltForTest(UINT64_MAX);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    bool result = LogSampler::Instance().IsCurrentRequestSampledIn();
    bool admitted = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_EQ(admitted, result);
}

// LS-009b: Config failure retains previous-good config
TEST_F(LogSamplerConfigTest, ConfigFailureRetainsPreviousGood)
{
    LogSampleUserConfig goodCfg;
    goodCfg.requestSampleRate = 0.0;
    goodCfg.requestSampleRateExplicit = true;
    goodCfg.accessSampleRate = 0.0;
    goodCfg.accessSampleRateExplicit = true;
    goodCfg.diagnosticSampleRate = 0.0;
    goodCfg.diagnosticSampleRateExplicit = true;
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(goodCfg));
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());

    auto *snapBefore = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snapBefore, nullptr);
    EXPECT_EQ(snapBefore->config.requestRate.ppm, 0);

    TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());

    // Under previous-good config, request INFO is rejected
    int infoEvals = 0;
    LOG(INFO) << "rejected under previous-good " << BuildSideEffectPayload(infoEvals);
    EXPECT_EQ(infoEvals, 0);

    // Invalid dynamic update must fail and keep previous-good
    LogSampleUserConfig invalidCfg;
    invalidCfg.requestSampleRate = -0.5;
    invalidCfg.requestSampleRateExplicit = true;
    EXPECT_FALSE(LogSampler::Instance().UpdateConfigFromFlags(invalidCfg));

    // Previous-good config still active — INFO still rejected
    auto *snapAfter = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snapAfter, nullptr);
    EXPECT_EQ(snapAfter->config.requestRate.ppm, 0);

    int infoEvals2 = 0;
    LOG(INFO) << "still rejected after config failure " << BuildSideEffectPayload(infoEvals2);
    EXPECT_EQ(infoEvals2, 0);

    // Config failure LOG(ERROR) happens outside request context (in config thread),
    // so ClassifyRuntime → BYPASS → always passes. Verify this principle:
    // background ERROR (no request context) always passes regardless of sampler state
    Trace::Instance().Invalidate();
    int bgErrorEvals = 0;
    LOG(ERROR) << "background error bypasses sampler " << BuildSideEffectPayload(bgErrorEvals);
    EXPECT_EQ(bgErrorEvals, 1);
}

// #21: Missing log_sample_config in proto keeps client config unchanged
TEST_F(LogSamplerConfigTest, MissingLogSampleConfigKeepsClientConfig)
{
    LogSampleUserConfig goodCfg;
    goodCfg.requestSampleRate = 0.5;
    goodCfg.requestSampleRateExplicit = true;
    goodCfg.accessSampleRate = 0.3;
    goodCfg.accessSampleRateExplicit = true;
    goodCfg.diagnosticSampleRate = 0.4;
    goodCfg.diagnosticSampleRateExplicit = true;
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(goodCfg));
    EXPECT_TRUE(LogSampler::Instance().IsSamplerEnabledFast());

    uint32_t ppmBefore = LogSampler::Instance().GetSnapshotForTest()->config.requestRate.ppm;

    LogSampleConfigPb proto;
    proto.set_enabled(true);
    proto.set_request_sample_ppm(kSamplePpmBase);
    proto.set_access_sample_ppm(kSamplePpmBase);
    proto.set_diagnostic_sample_ppm(kSamplePpmBase);
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(proto), ConfigUpdateResult::CHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());

    LogSampleConfigPb emptyProto;
    ASSERT_EQ(LogSampler::Instance().UpdateConfigFromProto(emptyProto), ConfigUpdateResult::UNCHANGED);
    EXPECT_FALSE(LogSampler::Instance().IsSamplerEnabledFast());

    LogSampleUserConfig restoreCfg;
    restoreCfg.requestSampleRate = 0.5;
    restoreCfg.requestSampleRateExplicit = true;
    restoreCfg.accessSampleRate = 0.3;
    restoreCfg.accessSampleRateExplicit = true;
    restoreCfg.diagnosticSampleRate = 0.4;
    restoreCfg.diagnosticSampleRateExplicit = true;
    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(restoreCfg));
    EXPECT_EQ(LogSampler::Instance().GetSnapshotForTest()->config.requestRate.ppm, ppmBefore);
}

// Derivation end-to-end: only requestSampleRate explicit → access/diagnostic derive
TEST_F(LogSamplerConfigTest, DerivationOnlyRequestExplicit)
{
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.2;
    cfg.requestSampleRateExplicit = true;
    cfg.accessSampleRate = 1.0;
    cfg.accessSampleRateExplicit = false;
    cfg.diagnosticSampleRate = 1.0;
    cfg.diagnosticSampleRateExplicit = false;

    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);

    // kAccessDeriveMultiplier=3.0 → access=min(1.0, 0.2*3)=0.6 → ppm=600000
    EXPECT_EQ(snap->config.accessRate.ppm, 600000);
    // kDiagnosticDeriveMultiplier=4.0 → diagnostic=min(1.0, 0.2*4)=0.8 → ppm=800000
    EXPECT_EQ(snap->config.diagnosticRate.ppm, 800000);
}

// Derivation end-to-end: access explicitly set → no derivation for access
TEST_F(LogSamplerConfigTest, DerivationAccessExplicitStopsDerivation)
{
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    cfg.accessSampleRate = 0.3;
    cfg.accessSampleRateExplicit = true;
    cfg.diagnosticSampleRate = 1.0;
    cfg.diagnosticSampleRateExplicit = false;

    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg));

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);

    // access is explicit → use 0.3
    EXPECT_EQ(snap->config.accessRate.ppm, 300000);
    // diagnostic not explicit but access is explicit (which means derivation is disabled),
    // and there's no derivation for diagnostic either → default 1.0
    EXPECT_EQ(snap->config.diagnosticRate.ppm, kSamplePpmBase);
}

// Derivation: persistent explicit blocks subsequent derivation
TEST_F(LogSamplerConfigTest, DerivationPersistentExplicitBlocks)
{
    LogSampleUserConfig cfg1;
    cfg1.requestSampleRate = 0.5;
    cfg1.requestSampleRateExplicit = true;
    cfg1.accessSampleRate = 0.3;
    cfg1.accessSampleRateExplicit = true;  // explicitly set access
    cfg1.diagnosticSampleRate = 1.0;
    cfg1.diagnosticSampleRateExplicit = false;

    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg1));

    // Second update: request only explicit (access/diagnostic not explicit)
    LogSampleUserConfig cfg2;
    cfg2.requestSampleRate = 0.7;
    cfg2.requestSampleRateExplicit = true;
    cfg2.accessSampleRate = 1.0;
    cfg2.accessSampleRateExplicit = false;
    cfg2.diagnosticSampleRate = 1.0;
    cfg2.diagnosticSampleRateExplicit = false;

    ASSERT_TRUE(LogSampler::Instance().UpdateConfigFromFlags(cfg2));

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);

    // access was ever explicit → no derivation, falls back to 1.0
    EXPECT_EQ(snap->config.accessRate.ppm, kSamplePpmBase);
    // diagnostic was never explicit → but derivation is blocked because
    // access was ever explicit, so diagnostic also falls back to 1.0
    EXPECT_EQ(snap->config.diagnosticRate.ppm, kSamplePpmBase);
}

}  // namespace ut
}  // namespace datasystem
