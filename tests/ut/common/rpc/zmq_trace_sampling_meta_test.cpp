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
 * Description: Test request-log sampling metadata propagation in ZMQ MetaPb.
 */

#include "datasystem/common/rpc/zmq/zmq_common.h"

#include <gtest/gtest.h>

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
namespace ut {

class ZmqTraceSamplingMetaTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
        GetRequestContext()->reqTimeoutDuration.Init();
    }

    void TearDown() override
    {
        Trace::Instance().Invalidate();
        LogSampler::Instance().ResetForTest();
    }
};

TEST_F(ZmqTraceSamplingMetaTest, UpdateMetaSkipsDecisionForNonRequestTrace)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID("background-trace");
    Trace::Instance().SetRequestLogTrace(false);

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    LogSampler::Instance().UpdateConfigFromFlags(cfg);

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);

    EXPECT_EQ(meta.trace_id(), "background-trace");
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_NONE);
}

TEST_F(ZmqTraceSamplingMetaTest, UpdateMetaCarriesDecisionForRequestTrace)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    LogSampler::Instance().SetSaltForTest(0);

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    LogSampler::Instance().UpdateConfigFromFlags(cfg);

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);

    EXPECT_FALSE(meta.trace_id().empty());
    bool admitted = false;
    Trace::Instance().GetRequestSampleDecision(admitted);
    EXPECT_EQ(meta.log_sample_state(), admitted ? LOG_SAMPLE_ADMIT : LOG_SAMPLE_REJECT);
}

TEST_F(ZmqTraceSamplingMetaTest, UpdateMetaUsesUndecidedWhenSamplerDisabled)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);

    EXPECT_FALSE(meta.trace_id().empty());
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_UNDECIDED);
}

TEST_F(ZmqTraceSamplingMetaTest, SetTraceContextFromMetaRestoresRequestFields)
{
    MetaPb meta;
    meta.set_trace_id("rpc-trace-id");
    meta.set_log_sample_state(LOG_SAMPLE_REJECT);

    {
        TraceGuard traceGuard = SetTraceContextFromMeta(meta);
        EXPECT_EQ(Trace::Instance().GetTraceID(), "rpc-trace-id");
        EXPECT_TRUE(Trace::Instance().IsRequestLogTrace());
        bool admitted = true;
        EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
        EXPECT_FALSE(admitted);
    }
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
}

TEST_F(ZmqTraceSamplingMetaTest, CreateMetaDataCarriesThreadLocalRequestFlag)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    LogSampler::Instance().SetSaltForTest(0);

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    LogSampler::Instance().UpdateConfigFromFlags(cfg);

    MetaPb meta = CreateMetaData("svc", 1, -1, "client-id");
    EXPECT_NE(meta.log_sample_state(), LOG_SAMPLE_NONE);
}

TEST_F(ZmqTraceSamplingMetaTest, SetTraceContextFromMetaNoneClearsRequestFields)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    Trace::Instance().SetRequestSampleDecision(true, true);

    MetaPb meta;
    meta.set_trace_id("rpc-trace-none");
    meta.set_log_sample_state(LOG_SAMPLE_NONE);

    {
        TraceGuard innerGuard = SetTraceContextFromMeta(meta);
        EXPECT_EQ(Trace::Instance().GetTraceID(), "rpc-trace-none");
        EXPECT_FALSE(Trace::Instance().IsRequestLogTrace());
        bool admitted = true;
        EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
    }
}

TEST_F(ZmqTraceSamplingMetaTest, UndecidedMetaAllowsWorkerLocalSamplingDecision)
{
    LogSampler::Instance().SetSaltForTest(UINT64_MAX);

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.5;
    cfg.requestSampleRateExplicit = true;
    LogSampler::Instance().UpdateConfigFromFlags(cfg);

    MetaPb meta;
    meta.set_trace_id("rpc-trace-undecided");
    meta.set_log_sample_state(LOG_SAMPLE_UNDECIDED);

    TraceGuard traceGuard = SetTraceContextFromMeta(meta);
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_TRUE(admitted);
}

TEST_F(ZmqTraceSamplingMetaTest, UndecidedMetaRemainsUndecidedWhenReceiverSamplerDisabled)
{
    MetaPb meta;
    meta.set_trace_id("rpc-trace-undecided-no-rate");
    meta.set_log_sample_state(LOG_SAMPLE_UNDECIDED);

    TraceGuard traceGuard = SetTraceContextFromMeta(meta);
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
}

TEST_F(ZmqTraceSamplingMetaTest, SamplerDisabledReturnsUndecidedForRequestTrace)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_UNDECIDED);
}

TEST_F(ZmqTraceSamplingMetaTest, FullRateReturnsUndecidedNotAdmit)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 1.0;
    cfg.requestSampleRateExplicit = true;
    LogSampler::Instance().UpdateConfigFromFlags(cfg);

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_UNDECIDED);
}

TEST_F(ZmqTraceSamplingMetaTest, ZeroRateReturnsReject)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = 0.0;
    cfg.requestSampleRateExplicit = true;
    LogSampler::Instance().UpdateConfigFromFlags(cfg);

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_REJECT);
    bool admitted = true;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_FALSE(admitted);
}

}  // namespace ut
}  // namespace datasystem