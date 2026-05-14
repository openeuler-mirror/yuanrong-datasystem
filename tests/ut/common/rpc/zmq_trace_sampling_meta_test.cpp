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

#include "datasystem/common/log/spdlog/log_rate_limiter.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
namespace ut {

class ZmqTraceSamplingMetaTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        Trace::Instance().Invalidate();
        LogRateLimiter::Instance().Reset();
        reqTimeoutDuration.Init();
        g_MetaRocksDbName.clear();
    }

    void TearDown() override
    {
        Trace::Instance().Invalidate();
        LogRateLimiter::Instance().Reset();
        g_MetaRocksDbName.clear();
    }
};

TEST_F(ZmqTraceSamplingMetaTest, UpdateMetaSkipsDecisionForNonRequestTrace)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID("background-trace");
    Trace::Instance().SetRequestLogTrace(false);
    LogRateLimiter::Instance().SetRate(1);

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);

    EXPECT_EQ(meta.trace_id(), "background-trace");
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_NONE);
}

TEST_F(ZmqTraceSamplingMetaTest, UpdateMetaCarriesDecisionForRequestTrace)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    LogRateLimiter::Instance().SetRate(1);

    MetaPb meta;
    UpdateMetaByThreadLocalValue(meta);

    EXPECT_FALSE(meta.trace_id().empty());
    EXPECT_EQ(meta.log_sample_state(), LOG_SAMPLE_ADMIT);
}

TEST_F(ZmqTraceSamplingMetaTest, UpdateMetaUsesUndecidedWhenRequestHasNoLocalSampling)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    LogRateLimiter::Instance().SetRate(0);

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
    LogRateLimiter::Instance().SetRate(1);

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
    LogRateLimiter::Instance().SetRate(1);

    MetaPb meta;
    meta.set_trace_id("rpc-trace-undecided");
    meta.set_log_sample_state(LOG_SAMPLE_UNDECIDED);

    TraceGuard traceGuard = SetTraceContextFromMeta(meta);
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_TRUE(admitted);
    EXPECT_TRUE(LogRateLimiter::Instance().ShouldLog(ds_spdlog::level::info, Trace::Instance().GetCachedHash()));
    EXPECT_TRUE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_TRUE(admitted);
}

TEST_F(ZmqTraceSamplingMetaTest, UndecidedMetaRemainsUndecidedWhenReceiverHasNoLocalSampling)
{
    LogRateLimiter::Instance().SetRate(0);

    MetaPb meta;
    meta.set_trace_id("rpc-trace-undecided-no-rate");
    meta.set_log_sample_state(LOG_SAMPLE_UNDECIDED);

    TraceGuard traceGuard = SetTraceContextFromMeta(meta);
    ASSERT_TRUE(Trace::Instance().IsRequestLogTrace());
    bool admitted = false;
    EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
    EXPECT_TRUE(LogRateLimiter::Instance().ShouldLog(ds_spdlog::level::info, Trace::Instance().GetCachedHash()));
    EXPECT_FALSE(Trace::Instance().GetRequestSampleDecision(admitted));
}

}  // namespace ut
}  // namespace datasystem
