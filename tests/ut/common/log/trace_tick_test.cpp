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

#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/latency_phase.h"

#include <thread>
#include <type_traits>

#include "ut/common.h"

namespace datasystem {
namespace ut {
static_assert(std::is_trivially_destructible<Trace>::value,
              "thread-local Trace must remain safe during process-static client destruction");

class TraceTickTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        Trace::Instance().Invalidate();
    }
    void TearDown() override
    {
        Trace::Instance().Invalidate();
        CommonTest::TearDown();
    }
};

TEST_F(TraceTickTest, AddLatencyTickBasicAndOverflow)
{
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_START);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_END);
    ASSERT_EQ(Trace::Instance().GetLatencyTickCount(), 3u);
    const auto *ticks = Trace::Instance().GetLatencyTicks();
    ASSERT_NE(ticks, nullptr);
    EXPECT_EQ(ticks[0].key, LatencyTickKey::CLIENT_GET_START);
    EXPECT_EQ(ticks[1].key, LatencyTickKey::CLIENT_GET_RPC_START);
    EXPECT_EQ(ticks[2].key, LatencyTickKey::CLIENT_GET_RPC_END);
    EXPECT_GT(ticks[0].tick, 0u);
    EXPECT_GE(ticks[1].tick, ticks[0].tick);
    EXPECT_GE(ticks[2].tick, ticks[1].tick);
    EXPECT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 0u);

    for (uint16_t i = 3; i < LATENCY_TICK_MAX_NUM + 5; ++i) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::UNKNOWN);
    }
    EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), LATENCY_TICK_MAX_NUM);
    EXPECT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 5u);
}

TEST_F(TraceTickTest, ClearLatencyTicksReset)
{
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_END);
    for (uint16_t i = 0; i < LATENCY_TICK_MAX_NUM + 3; ++i) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::UNKNOWN);
    }
    ASSERT_EQ(Trace::Instance().GetLatencyTickCount(), LATENCY_TICK_MAX_NUM);
    ASSERT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 5u);

    Trace::Instance().ClearLatencyTicks();
    EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), 0u);
    EXPECT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 0u);
}

TEST_F(TraceTickTest, TraceContextPropagatesTicks)
{
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_START);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_END);
    for (uint16_t i = 3; i < LATENCY_TICK_MAX_NUM + 2; ++i) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::UNKNOWN);
    }
    uint16_t expectedDropped = 2u;
    ASSERT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), expectedDropped);

    TraceContext ctx = Trace::Instance().GetContext();
    std::thread t([ctx, expectedDropped]() {
        TraceGuard guard = Trace::Instance().SetTraceContext(ctx);
        EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), LATENCY_TICK_MAX_NUM);
        EXPECT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), expectedDropped);
        const auto *ticks = Trace::Instance().GetLatencyTicks();
        EXPECT_EQ(ticks[0].key, LatencyTickKey::CLIENT_GET_START);
        EXPECT_EQ(ticks[1].key, LatencyTickKey::CLIENT_GET_RPC_START);
        EXPECT_EQ(ticks[2].key, LatencyTickKey::CLIENT_GET_RPC_END);
    });
    t.join();
}

TEST_F(TraceTickTest, InvalidateClearsTicks)
{
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_END);
    for (uint16_t i = 2; i < LATENCY_TICK_MAX_NUM + 1; ++i) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::UNKNOWN);
    }
    ASSERT_EQ(Trace::Instance().GetLatencyTickCount(), LATENCY_TICK_MAX_NUM);
    ASSERT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 1u);

    Trace::Instance().Invalidate();
    EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), 0u);
    EXPECT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 0u);
}

TEST_F(TraceTickTest, LatencySummaryTruncatesAtInlineCapacity)
{
    const std::string summary(LATENCY_SUMMARY_MAX_SIZE + 1, 'x');
    Trace::Instance().SetLatencySummary(summary);

    EXPECT_EQ(Trace::Instance().GetLatencySummary(), summary.substr(0, LATENCY_SUMMARY_MAX_SIZE));
}

TEST_F(TraceTickTest, DownstreamPhaseBasicAndOverflow)
{
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 500u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_CREATE_META, 200u);
    auto &ds = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(ds.count, 2u);
    EXPECT_EQ(ds.entries[0].phase, LatencySummaryPhase::MASTER_PROCESS_QUERY_META);
    EXPECT_EQ(ds.entries[0].durationUs, 500u);
    EXPECT_EQ(ds.entries[1].phase, LatencySummaryPhase::MASTER_PROCESS_CREATE_META);
    EXPECT_EQ(ds.entries[1].durationUs, 200u);
    EXPECT_EQ(ds.phaseDroppedCount, 0u);
    EXPECT_EQ(ds.tickDroppedCount, 0u);

    Trace::Instance().ClearDownstreamPhases();
    auto &dsAfterClear = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(dsAfterClear.count, 0u);
    EXPECT_EQ(dsAfterClear.phaseDroppedCount, 0u);
    EXPECT_EQ(dsAfterClear.tickDroppedCount, 0u);

    for (uint16_t i = 0; i < DOWNSTREAM_PHASE_MAX_NUM; ++i) {
        Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META,
                                              static_cast<uint64_t>(i * 10));
    }
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_CREATE_META, 50u);
    auto &dsOverflow = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(dsOverflow.count, DOWNSTREAM_PHASE_MAX_NUM);
    EXPECT_EQ(dsOverflow.phaseDroppedCount, 1u);
    Trace::Instance().ClearDownstreamPhases();
}

TEST_F(TraceTickTest, ClearLatencyTicksAlsoClearsDownstream)
{
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 300u);
    Trace::Instance().AddDownstreamTickDroppedCount(5u);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    Trace::Instance().ClearLatencyTicks();
    EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), 0u);
    EXPECT_EQ(Trace::Instance().GetLatencyTickDroppedCount(), 0u);
    auto &ds = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(ds.count, 0u);
    EXPECT_EQ(ds.tickDroppedCount, 0u);
    EXPECT_EQ(ds.phaseDroppedCount, 0u);
}

TEST_F(TraceTickTest, TraceContextPropagatesDownstreamPhases)
{
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 500u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_CREATE_META, 200u);
    Trace::Instance().AddDownstreamTickDroppedCount(3u);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);

    TraceContext ctx = Trace::Instance().GetContext();
    std::thread t([ctx]() {
        TraceGuard guard = Trace::Instance().SetTraceContext(ctx);
        auto &ds = Trace::Instance().GetDownstreamPhases();
        EXPECT_EQ(ds.count, 2u);
        EXPECT_EQ(ds.entries[0].phase, LatencySummaryPhase::MASTER_PROCESS_QUERY_META);
        EXPECT_EQ(ds.entries[0].durationUs, 500u);
        EXPECT_EQ(ds.entries[1].phase, LatencySummaryPhase::MASTER_PROCESS_CREATE_META);
        EXPECT_EQ(ds.entries[1].durationUs, 200u);
        EXPECT_EQ(ds.tickDroppedCount, 3u);
        EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), 1u);
    });
    t.join();
    Trace::Instance().ClearLatencyTicks();
}

TEST_F(TraceTickTest, InvalidateClearsLatencySummary)
{
    Trace::Instance().SetLatencySummary("{client.rpc.get:300,client.process.get:200}");
    ASSERT_FALSE(Trace::Instance().GetLatencySummary().empty());

    Trace::Instance().Invalidate();
    EXPECT_TRUE(Trace::Instance().GetLatencySummary().empty());
}

TEST_F(TraceTickTest, TraceGuardLifetimeClearsLatencySummary)
{
    {
        TraceGuard guard = Trace::Instance().SetRequestTraceUUID();
        Trace::Instance().SetLatencySummary("{client.rpc.get:300}");
        ASSERT_FALSE(Trace::Instance().GetLatencySummary().empty());
    }
    EXPECT_TRUE(Trace::Instance().GetLatencySummary().empty());
}

TEST_F(TraceTickTest, ConsumeLatencySummaryMoveAndClear)
{
    Trace::Instance().SetLatencySummary("{client.rpc.get:500}");
    std::string consumed = Trace::Instance().ConsumeLatencySummary();
    EXPECT_EQ(consumed, "{client.rpc.get:500}");
    EXPECT_TRUE(Trace::Instance().GetLatencySummary().empty());
}
}  // namespace ut
}  // namespace datasystem
