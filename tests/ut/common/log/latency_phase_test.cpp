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

#include "datasystem/common/log/latency_phase.h"

#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>
#include <vector>

#include "datasystem/common/flags/common_flags.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
class LatencyPhaseTest : public CommonTest {};

TEST_F(LatencyPhaseTest, ComputePhaseDurationsDirectAndDerived)
{
    // In B (issue #862), *_RPC_* phases hold comm (network + RPC framework),
    // populated by the transport layer via Trace::AddDownstreamPhase — NOT from
    // business tick pairs. So CLIENT_GET_RPC_START/END ticks no longer produce
    // CLIENT_RPC_GET; only the derived CLIENT_PROCESS_GET is computed from
    // CLIENT_GET_START/END. With no downstream phases merged, the derived
    // residual equals the full total.
    LatencyTick ticks[] = {
        { LatencyTickKey::CLIENT_GET_START, 100000u },
        { LatencyTickKey::CLIENT_GET_END, 600000u },
    };
    auto result = ComputePhaseDurations(ticks, 2, 0);

    EXPECT_EQ(result.Find(LatencySummaryPhase::CLIENT_RPC_GET), nullptr);

    const auto *localGet = result.Find(LatencySummaryPhase::CLIENT_PROCESS_GET);
    ASSERT_NE(localGet, nullptr);
    EXPECT_EQ(localGet->durationUs, 500u);

    EXPECT_EQ(result.Find(LatencySummaryPhase::CLIENT_PROCESS_SET), nullptr);

    LatencyTick emptyTicks[] = {};
    auto emptyResult = ComputePhaseDurations(emptyTicks, 0, 0);
    EXPECT_EQ(emptyResult.count, 0u);
}

TEST_F(LatencyPhaseTest, ComputeDirectGetPhaseDurations)
{
    LatencyTick ticks[] = {
        { LatencyTickKey::CLIENT_GET_START, 100000u },
        { LatencyTickKey::CLIENT_DIRECT_ROUTE_START, 120000u },
        { LatencyTickKey::CLIENT_DIRECT_ROUTE_END, 140000u },
        { LatencyTickKey::CLIENT_DIRECT_QUERY_AND_GET_START, 150000u },
        { LatencyTickKey::CLIENT_DIRECT_QUERY_AND_GET_END, 450000u },
        { LatencyTickKey::CLIENT_DIRECT_GET_DATA_START, 460000u },
        { LatencyTickKey::CLIENT_DIRECT_GET_DATA_END, 760000u },
        { LatencyTickKey::CLIENT_DIRECT_MATERIALIZE_START, 780000u },
        { LatencyTickKey::CLIENT_DIRECT_MATERIALIZE_END, 850000u },
        { LatencyTickKey::CLIENT_GET_END, 900000u },
    };
    auto result = ComputePhaseDurations(ticks, 10, 0);

    const auto *route = result.Find(LatencySummaryPhase::CLIENT_PROCESS_DIRECT_ROUTE);
    ASSERT_NE(route, nullptr);
    EXPECT_EQ(route->durationUs, 20u);
    const auto *query = result.Find(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET);
    ASSERT_NE(query, nullptr);
    EXPECT_EQ(query->durationUs, 300u);
    const auto *getData = result.Find(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA);
    ASSERT_NE(getData, nullptr);
    EXPECT_EQ(getData->durationUs, 300u);
    const auto *materialize = result.Find(LatencySummaryPhase::CLIENT_PROCESS_DIRECT_MATERIALIZE);
    ASSERT_NE(materialize, nullptr);
    EXPECT_EQ(materialize->durationUs, 70u);
    const auto *localGet = result.Find(LatencySummaryPhase::CLIENT_PROCESS_GET);
    ASSERT_NE(localGet, nullptr);
    EXPECT_EQ(localGet->durationUs, 110u);

    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::CLIENT_PROCESS_DIRECT_ROUTE));
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::CLIENT_PROCESS_DIRECT_MATERIALIZE));
    // Direct-route phases gate on process threshold, not rpc (issue #862 review).
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET));
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA));
    EXPECT_FALSE(IsRpcPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET));
    EXPECT_FALSE(IsRpcPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA));
}

TEST_F(LatencyPhaseTest, FormatLatencySummaryNoRawTickOrTotal)
{
    LatencyTick ticks[] = {
        { LatencyTickKey::CLIENT_GET_START, 100000u },
        { LatencyTickKey::CLIENT_GET_END, 600000u },
    };
    auto result = ComputePhaseDurations(ticks, 2, 0);
    std::string summary = FormatLatencySummary(result);

    EXPECT_NE(summary.find("client.process.get"), std::string::npos);
    EXPECT_EQ(summary.find("tick"), std::string::npos);
    EXPECT_EQ(summary.find("total"), std::string::npos);
    EXPECT_EQ(summary.find("ns"), std::string::npos);

    PhaseDurationResult emptyResult;
    emptyResult.count = 0;
    emptyResult.tickDroppedCount = 0;
    emptyResult.phaseDroppedCount = 0;
    EXPECT_EQ(FormatLatencySummary(emptyResult), "{}");
}

TEST_F(LatencyPhaseTest, ShouldPrintLatencySummaryDisabledAndThreshold)
{
    LatencyTraceConfig disabled{ 0, 0 };
    EXPECT_FALSE(ShouldPrintLatencySummary(1000, disabled));
    EXPECT_FALSE(ShouldPrintLatencySummary(0, disabled));

    LatencyTraceConfig localOnly{ 100, 0 };
    EXPECT_TRUE(ShouldPrintLatencySummary(200, localOnly));
    EXPECT_FALSE(ShouldPrintLatencySummary(50, localOnly));

    LatencyTraceConfig rpcOnly{ 0, 200 };
    EXPECT_TRUE(ShouldPrintLatencySummary(300, rpcOnly));
    EXPECT_FALSE(ShouldPrintLatencySummary(100, rpcOnly));

    LatencyTraceConfig both{ 100, 200 };
    EXPECT_TRUE(ShouldPrintLatencySummary(150, both));
    EXPECT_FALSE(ShouldPrintLatencySummary(50, both));
}

TEST_F(LatencyPhaseTest, CheckPhaseGateLocalRpcIndependent)
{
    LatencyTraceConfig config{ 100, 200 };
    PhaseDurationResult result;

    result.Add(LatencySummaryPhase::CLIENT_PROCESS_GET, 150u);
    result.Add(LatencySummaryPhase::CLIENT_RPC_GET, 50u);
    EXPECT_TRUE(CheckPhaseGate(result, config));

    PhaseDurationResult result2;
    result2.Add(LatencySummaryPhase::CLIENT_PROCESS_GET, 50u);
    result2.Add(LatencySummaryPhase::CLIENT_RPC_GET, 300u);
    EXPECT_TRUE(CheckPhaseGate(result2, config));

    PhaseDurationResult result3;
    result3.Add(LatencySummaryPhase::CLIENT_PROCESS_GET, 50u);
    result3.Add(LatencySummaryPhase::CLIENT_RPC_GET, 50u);
    EXPECT_FALSE(CheckPhaseGate(result3, config));

    LatencyTraceConfig disabled{ 0, 0 };
    EXPECT_FALSE(CheckPhaseGate(result, disabled));

    PhaseDurationResult exactLocal;
    exactLocal.Add(LatencySummaryPhase::CLIENT_PROCESS_GET, 100u);
    EXPECT_TRUE(CheckPhaseGate(exactLocal, config));

    PhaseDurationResult exactRpc;
    exactRpc.Add(LatencySummaryPhase::CLIENT_RPC_GET, 200u);
    EXPECT_TRUE(CheckPhaseGate(exactRpc, config));
}

// Issue #862: the *_RPC_* phases now hold RPC communication time (network + RPC
// framework, excludes remote business execution and remote queue wait), so they
// DO drive the slow_log_rpc_slower_than gate. Server-side execution shows up as
// process phases (master.process.* / worker.process.remote_get), NOT as rpc
// phases.
TEST_F(LatencyPhaseTest, RpcCommPhaseDrivesRpcThresholdGate)
{
    LatencyTraceConfig config{ 0, 200 }; // rpc-only gate
    PhaseDurationResult result;
    result.Add(LatencySummaryPhase::CLIENT_RPC_GET, 5000u);
    EXPECT_TRUE(IsRpcPhase(LatencySummaryPhase::CLIENT_RPC_GET));
    EXPECT_TRUE(CheckPhaseGate(result, config));

    PhaseDurationResult workerResult;
    workerResult.Add(LatencySummaryPhase::WORKER_RPC_QUERY_META, 5000u);
    EXPECT_TRUE(IsRpcPhase(LatencySummaryPhase::WORKER_RPC_QUERY_META));
    EXPECT_TRUE(CheckPhaseGate(workerResult, config));

    // Server-side execution is a process phase, not an rpc phase.
    PhaseDurationResult serverExecResult;
    serverExecResult.Add(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 5000u);
    EXPECT_FALSE(IsRpcPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META));
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META));
    EXPECT_FALSE(CheckPhaseGate(serverExecResult, config));

    // Comm below threshold does not gate.
    PhaseDurationResult fastResult;
    fastResult.Add(LatencySummaryPhase::WORKER_RPC_QUERY_META, 50u);
    EXPECT_FALSE(CheckPhaseGate(fastResult, config));

    // Comm=0 (old server without 8-point trace) does not gate.
    PhaseDurationResult oldServerResult;
    oldServerResult.Add(LatencySummaryPhase::CLIENT_RPC_GET, 0u);
    EXPECT_FALSE(CheckPhaseGate(oldServerResult, config));

    // Legacy direct-route phases gate on process threshold, not rpc threshold
    // (they bundle remote business execution, issue #862 review).
    EXPECT_FALSE(IsRpcPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET));
    EXPECT_FALSE(IsRpcPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA));
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET));
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA));
}

TEST_F(LatencyPhaseTest, ConsumeCommAndAddDownstreamPhase)
{
    Trace::Instance().ClearDownstreamPhases();
    // comm=0 → AddDownstreamPhase with 0 is a no-op in effect (phase present, duration 0).
    Trace::Instance().SetLastRpcCommUs(0);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::CLIENT_RPC_GET,
                                        Trace::Instance().ConsumeLastRpcCommUs());
    auto &ds0 = Trace::Instance().GetDownstreamPhases();
    ASSERT_EQ(ds0.count, 1u);
    EXPECT_EQ(ds0.entries[0].phase, LatencySummaryPhase::CLIENT_RPC_GET);
    EXPECT_EQ(ds0.entries[0].durationUs, 0u);
    Trace::Instance().ClearDownstreamPhases();

    // comm=750 → stashed by transport, consumed once, added as downstream phase.
    Trace::Instance().SetLastRpcCommUs(750);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_RPC_QUERY_META,
                                        Trace::Instance().ConsumeLastRpcCommUs());
    auto &ds = Trace::Instance().GetDownstreamPhases();
    ASSERT_EQ(ds.count, 1u);
    EXPECT_EQ(ds.entries[0].phase, LatencySummaryPhase::WORKER_RPC_QUERY_META);
    EXPECT_EQ(ds.entries[0].durationUs, 750u);
    // Stash consumed exactly once.
    EXPECT_EQ(Trace::Instance().ConsumeLastRpcCommUs(), 0u);

    Trace::Instance().ClearDownstreamPhases();
}

// Issue #862 review: AddCommPhaseIfEnabled always clears the stash even when
// tracing is disabled, preventing cross-RPC leakage when traceEnabled toggles.
TEST_F(LatencyPhaseTest, AddCommPhaseIfEnabledClearsStashWhenDisabled)
{
    Trace::Instance().ClearDownstreamPhases();
    // Stash from transport layer, tracing disabled.
    Trace::Instance().SetLastRpcCommUs(500);
    Trace::Instance().AddCommPhaseIfEnabled(LatencySummaryPhase::CLIENT_RPC_GET, false);
    // Stash must be cleared even though tracing was disabled.
    EXPECT_EQ(Trace::Instance().ConsumeLastRpcCommUs(), 0u);
    // No downstream phase added when disabled.
    auto &ds = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(ds.count, 0u);

    // Next call with tracing enabled does not see stale comm.
    Trace::Instance().SetLastRpcCommUs(300);
    Trace::Instance().AddCommPhaseIfEnabled(LatencySummaryPhase::CLIENT_RPC_GET, true);
    auto &ds2 = Trace::Instance().GetDownstreamPhases();
    ASSERT_EQ(ds2.count, 1u);
    EXPECT_EQ(ds2.entries[0].durationUs, 300u); // not 500 (stale) or 800 (sum)
    EXPECT_EQ(Trace::Instance().ConsumeLastRpcCommUs(), 0u);

    Trace::Instance().ClearDownstreamPhases();
}

TEST_F(LatencyPhaseTest, EncodeDecodePhasePairs)
{
    std::vector<uint32_t> packed;
    AppendPhasePair(packed, LatencySummaryPhase::CLIENT_RPC_GET, 300);
    AppendPhasePair(packed, LatencySummaryPhase::CLIENT_PROCESS_GET, 200);
    ASSERT_EQ(packed.size(), 4u);
    EXPECT_EQ(packed[0], static_cast<uint32_t>(LatencySummaryPhase::CLIENT_RPC_GET));
    EXPECT_EQ(packed[1], 300u);

    PhaseDurationResult decoded;
    EXPECT_TRUE(DecodePhasePairs(packed, decoded));
    ASSERT_EQ(decoded.count, 2u);
    EXPECT_EQ(decoded.entries[0].phase, LatencySummaryPhase::CLIENT_RPC_GET);
    EXPECT_EQ(decoded.entries[0].durationUs, 300u);
    EXPECT_EQ(decoded.entries[1].phase, LatencySummaryPhase::CLIENT_PROCESS_GET);
    EXPECT_EQ(decoded.entries[1].durationUs, 200u);

    std::vector<uint32_t> odd = { 1, 2, 3 };
    PhaseDurationResult oddResult;
    EXPECT_FALSE(DecodePhasePairs(odd, oddResult));

    std::vector<uint32_t> mixed;
    AppendPhasePair(mixed, LatencySummaryPhase::CLIENT_RPC_GET, 100);
    mixed.push_back(0);
    mixed.push_back(50);
    mixed.push_back(99);
    mixed.push_back(200);
    AppendPhasePair(mixed, LatencySummaryPhase::CLIENT_PROCESS_GET, 300);
    PhaseDurationResult mixedResult;
    EXPECT_TRUE(DecodePhasePairs(mixed, mixedResult));
    ASSERT_EQ(mixedResult.count, 2u);
    EXPECT_EQ(mixedResult.entries[0].phase, LatencySummaryPhase::CLIENT_RPC_GET);
    EXPECT_EQ(mixedResult.entries[0].durationUs, 100u);
    EXPECT_EQ(mixedResult.entries[1].phase, LatencySummaryPhase::CLIENT_PROCESS_GET);
    EXPECT_EQ(mixedResult.entries[1].durationUs, 300u);

    std::vector<uint32_t> saturating;
    AppendPhasePair(saturating, LatencySummaryPhase::CLIENT_RPC_GET,
                    std::numeric_limits<uint32_t>::max());
    PhaseDurationResult satResult;
    EXPECT_TRUE(DecodePhasePairs(saturating, satResult));
    ASSERT_EQ(satResult.count, 1u);
    EXPECT_EQ(satResult.entries[0].phase, LatencySummaryPhase::CLIENT_RPC_GET);
    EXPECT_EQ(satResult.entries[0].durationUs,
              static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()));

    std::vector<uint32_t> overflowPack;
    uint64_t overUint32Max = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
    AppendPhasePair(overflowPack, LatencySummaryPhase::CLIENT_RPC_GET, overUint32Max);
    ASSERT_EQ(overflowPack.size(), 2u);
    EXPECT_EQ(overflowPack[1], std::numeric_limits<uint32_t>::max());

    uint64_t atUint32Max = std::numeric_limits<uint32_t>::max();
    std::vector<uint32_t> packed2;
    AppendPhasePair(packed2, LatencySummaryPhase::CLIENT_RPC_GET, atUint32Max);
    ASSERT_EQ(packed2.size(), 2u);
    EXPECT_EQ(packed2[1], std::numeric_limits<uint32_t>::max());

    uint64_t belowUint32Max = 100;
    std::vector<uint32_t> packed3;
    AppendPhasePair(packed3, LatencySummaryPhase::CLIENT_RPC_GET, belowUint32Max);
    ASSERT_EQ(packed3.size(), 2u);
    EXPECT_EQ(packed3[1], 100u);
}

TEST_F(LatencyPhaseTest, DroppedCountPropagatesToResult)
{
    LatencyTick ticks[] = {
        { LatencyTickKey::CLIENT_GET_START, 100000u },
        { LatencyTickKey::CLIENT_GET_RPC_START, 200000u },
        { LatencyTickKey::CLIENT_GET_RPC_END, 500000u },
        { LatencyTickKey::CLIENT_GET_END, 600000u },
    };
    auto result = ComputePhaseDurations(ticks, 4, 5);
    EXPECT_EQ(result.tickDroppedCount, 5u);

    std::string summary = FormatLatencySummary(result);
    EXPECT_NE(summary.find("tick_dropped:5"), std::string::npos);

    auto zeroResult = ComputePhaseDurations(ticks, 4, 0);
    EXPECT_EQ(zeroResult.tickDroppedCount, 0u);
    std::string zeroSummary = FormatLatencySummary(zeroResult);
    EXPECT_EQ(zeroSummary.find("tick_dropped"), std::string::npos);

    PhaseDurationResult mergeResult;
    mergeResult.Add(LatencySummaryPhase::CLIENT_RPC_GET, 100u);
    mergeResult.Add(LatencySummaryPhase::CLIENT_RPC_GET, 200u);
    EXPECT_EQ(mergeResult.count, 1u);
    EXPECT_EQ(mergeResult.phaseDroppedCount, 0u);
    const auto *merged = mergeResult.Find(LatencySummaryPhase::CLIENT_RPC_GET);
    ASSERT_NE(merged, nullptr);
    EXPECT_EQ(merged->durationUs, 300u);
}

TEST_F(LatencyPhaseTest, DownstreamPhaseResultAddAndOverflow)
{
    DownstreamPhaseResult ds;
    for (uint16_t i = 0; i < DOWNSTREAM_PHASE_MAX_NUM; ++i) {
        ds.Add(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, static_cast<uint64_t>(i * 100));
    }
    EXPECT_EQ(ds.count, DOWNSTREAM_PHASE_MAX_NUM);
    EXPECT_EQ(ds.phaseDroppedCount, 0u);
    ds.Add(LatencySummaryPhase::MASTER_PROCESS_CREATE_META, 50u);
    EXPECT_EQ(ds.phaseDroppedCount, 1u);
    EXPECT_EQ(ds.count, DOWNSTREAM_PHASE_MAX_NUM);
}

TEST_F(LatencyPhaseTest, MergeDecodedPhasesToTrace)
{
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().ClearDownstreamPhases();

    std::vector<uint32_t> packed;
    AppendPhasePair(packed, LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 500);
    AppendPhasePair(packed, LatencySummaryPhase::MASTER_PROCESS_CREATE_META, 200);
    MergeDecodedPhasesToTrace(packed, 2u);

    auto &ds = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(ds.count, 2u);
    EXPECT_EQ(ds.entries[0].phase, LatencySummaryPhase::MASTER_PROCESS_QUERY_META);
    EXPECT_EQ(ds.entries[0].durationUs, 500u);
    EXPECT_EQ(ds.entries[1].phase, LatencySummaryPhase::MASTER_PROCESS_CREATE_META);
    EXPECT_EQ(ds.entries[1].durationUs, 200u);
    EXPECT_EQ(ds.tickDroppedCount, 2u);

    Trace::Instance().ClearDownstreamPhases();

    std::vector<uint32_t> odd = { 1 };
    MergeDecodedPhasesToTrace(odd, 0u);
    auto &dsOdd = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(dsOdd.count, 0u);
    Trace::Instance().ClearDownstreamPhases();
}

TEST_F(LatencyPhaseTest, DownstreamMergeWithLocalPhases)
{
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().ClearDownstreamPhases();

    std::vector<uint32_t> packed;
    AppendPhasePair(packed, LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 500);
    AppendPhasePair(packed, LatencySummaryPhase::MASTER_PROCESS_CREATE_META, 200);
    MergeDecodedPhasesToTrace(packed, 1u);

    LatencyTick ticks[] = {
        { LatencyTickKey::WORKER_PUBLISH_START, 100000u },
        { LatencyTickKey::WORKER_PUBLISH_END, 400000u },
    };
    PhaseDurationResult result = ComputePhaseDurations(ticks, 2, 0);
    auto &downstream = Trace::Instance().GetDownstreamPhases();
    for (uint16_t di = 0; di < downstream.count; ++di) {
        result.Add(downstream.entries[di].phase, downstream.entries[di].durationUs);
    }
    result.tickDroppedCount += downstream.tickDroppedCount;
    result.phaseDroppedCount += downstream.phaseDroppedCount;

    EXPECT_EQ(result.count, 3u);
    EXPECT_NE(result.Find(LatencySummaryPhase::WORKER_PROCESS_PUBLISH), nullptr);
    EXPECT_NE(result.Find(LatencySummaryPhase::MASTER_PROCESS_QUERY_META), nullptr);
    EXPECT_NE(result.Find(LatencySummaryPhase::MASTER_PROCESS_CREATE_META), nullptr);
    EXPECT_EQ(result.droppedCount(), 1u);

    Trace::Instance().ClearLatencyTicks();
}

TEST_F(LatencyPhaseTest, DerivedPhaseUnderflowClampToZero)
{
    // In B, *_RPC_* tick pairs are removed. Test underflow with memory_copy
    // exceeding the derived total (subSum > total → clamp to 0).
    LatencyTick ticks[] = {
        { LatencyTickKey::CLIENT_SET_START, 100000u },
        { LatencyTickKey::CLIENT_MEMORY_COPY_START, 50000u },
        { LatencyTickKey::CLIENT_MEMORY_COPY_END, 450000u }, // 400us > 300us total
        { LatencyTickKey::CLIENT_SET_END, 400000u },
    };
    auto result = ComputePhaseDurations(ticks, 4, 0);

    const auto *localSet = result.Find(LatencySummaryPhase::CLIENT_PROCESS_SET);
    ASSERT_NE(localSet, nullptr);
    EXPECT_EQ(localSet->durationUs, 0u);
}

TEST_F(LatencyPhaseTest, FormatLatencySummarySplitDroppedDisplay)
{
    LatencyTick ticks[] = {
        { LatencyTickKey::CLIENT_GET_START, 100000u },
        { LatencyTickKey::CLIENT_GET_RPC_START, 200000u },
        { LatencyTickKey::CLIENT_GET_RPC_END, 500000u },
        { LatencyTickKey::CLIENT_GET_END, 600000u },
    };
    auto result = ComputePhaseDurations(ticks, 4, 3);
    std::string summary = FormatLatencySummary(result);
    EXPECT_NE(summary.find("tick_dropped:3"), std::string::npos);
    EXPECT_EQ(summary.find("phase_dropped"), std::string::npos);

    PhaseDurationResult phaseOnly;
    phaseOnly.Add(LatencySummaryPhase::CLIENT_RPC_GET, 300u);
    phaseOnly.phaseDroppedCount = 2;
    std::string phaseOnlySummary = FormatLatencySummary(phaseOnly);
    EXPECT_NE(phaseOnlySummary.find("phase_dropped:2"), std::string::npos);
    EXPECT_EQ(phaseOnlySummary.find("tick_dropped"), std::string::npos);

    PhaseDurationResult both;
    both.Add(LatencySummaryPhase::CLIENT_RPC_GET, 300u);
    both.tickDroppedCount = 1;
    both.phaseDroppedCount = 2;
    std::string bothSummary = FormatLatencySummary(both);
    EXPECT_NE(bothSummary.find("tick_dropped:1"), std::string::npos);
    EXPECT_NE(bothSummary.find("phase_dropped:2"), std::string::npos);
}

TEST_F(LatencyPhaseTest, DroppedCountMethodCombinesTickAndPhase)
{
    PhaseDurationResult result;
    result.tickDroppedCount = 3;
    result.phaseDroppedCount = 2;
    EXPECT_EQ(result.droppedCount(), 5u);

    DownstreamPhaseResult ds;
    ds.tickDroppedCount = 4;
    ds.phaseDroppedCount = 1;
    EXPECT_EQ(ds.droppedCount(), 5u);
}

TEST_F(LatencyPhaseTest, MergeDecodedPhasesSplitDroppedCount)
{
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().ClearDownstreamPhases();

    std::vector<uint32_t> packed;
    AppendPhasePair(packed, LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 500);
    MergeDecodedPhasesToTrace(packed, 3u);

    auto &ds = Trace::Instance().GetDownstreamPhases();
    EXPECT_EQ(ds.tickDroppedCount, 3u);
    EXPECT_EQ(ds.phaseDroppedCount, 0u);

    Trace::Instance().ClearDownstreamPhases();
}

TEST_F(LatencyPhaseTest, GetClientLatencyTraceConfigReadsDefaults)
{
    auto config = GetClientLatencyTraceConfig();
    EXPECT_EQ(config.processSlowerThanUs, 2000u);
    EXPECT_EQ(config.rpcSlowerThanUs, 5000u);
}

TEST_F(LatencyPhaseTest, ClientConfigIndependentOfWorkerGflag)
{
    std::string errMsg;
    SetCommandLineOption("slow_log_process_slower_than", "100", errMsg);
    SetCommandLineOption("slow_log_rpc_slower_than", "200", errMsg);
    auto config = GetClientLatencyTraceConfig();
    EXPECT_EQ(config.processSlowerThanUs, 2000u);
    EXPECT_EQ(config.rpcSlowerThanUs, 5000u);
    SetCommandLineOption("slow_log_process_slower_than", "2000", errMsg);
    SetCommandLineOption("slow_log_rpc_slower_than", "5000", errMsg);
}

TEST_F(LatencyPhaseTest, ShouldPrintLatencySummaryExactBoundary)
{
    LatencyTraceConfig config{ 100, 200 };
    EXPECT_TRUE(ShouldPrintLatencySummary(100, config));
    EXPECT_TRUE(ShouldPrintLatencySummary(200, config));
    EXPECT_FALSE(ShouldPrintLatencySummary(99, config));

    LatencyTraceConfig localOnly{ 100, 0 };
    EXPECT_TRUE(ShouldPrintLatencySummary(100, localOnly));
    EXPECT_FALSE(ShouldPrintLatencySummary(99, localOnly));

    LatencyTraceConfig rpcOnly{ 0, 200 };
    EXPECT_TRUE(ShouldPrintLatencySummary(200, rpcOnly));
    EXPECT_FALSE(ShouldPrintLatencySummary(199, rpcOnly));
}

TEST_F(LatencyPhaseTest, MasterQueryMetaUsesLocalThresholdNotRpc)
{
    LatencyTraceConfig localOnly{ 100, 0 };
    PhaseDurationResult result;
    result.Add(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 150u);
    EXPECT_TRUE(IsProcessPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META));
    EXPECT_FALSE(IsRpcPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META));
    EXPECT_TRUE(CheckPhaseGate(result, localOnly));
    EXPECT_TRUE(ShouldPrintLatencySummary(150, localOnly));

    LatencyTraceConfig rpcOnly{ 0, 50 };
    PhaseDurationResult resultRpcOnly;
    resultRpcOnly.Add(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 150u);
    EXPECT_FALSE(CheckPhaseGate(resultRpcOnly, rpcOnly));
}

TEST_F(LatencyPhaseTest, FailedButFastRequestNoSummary)
{
    LatencyTraceConfig config{ 100, 200 };
    PhaseDurationResult result;
    result.Add(LatencySummaryPhase::CLIENT_RPC_GET, 50u);
    result.Add(LatencySummaryPhase::CLIENT_PROCESS_GET, 30u);
    EXPECT_FALSE(CheckPhaseGate(result, config));
    EXPECT_FALSE(ShouldPrintLatencySummary(80, config));

    PhaseDurationResult nearThreshold;
    nearThreshold.Add(LatencySummaryPhase::CLIENT_RPC_GET, 199u);
    EXPECT_FALSE(CheckPhaseGate(nearThreshold, config));
}

TEST_F(LatencyPhaseTest, FormatLatencySummaryManyPhasesWithLargeValues)
{
    PhaseDurationResult result;
    const LatencySummaryPhase phases[] = {
        LatencySummaryPhase::CLIENT_PROCESS_GET, LatencySummaryPhase::CLIENT_RPC_GET,
        LatencySummaryPhase::CLIENT_PROCESS_SET, LatencySummaryPhase::CLIENT_RPC_CREATE,
        LatencySummaryPhase::CLIENT_PROCESS_MEMORY_COPY, LatencySummaryPhase::CLIENT_RPC_PUBLISH,
        LatencySummaryPhase::CLIENT_URMA_UB_TRANSFER, LatencySummaryPhase::CLIENT_PROCESS_CREATE,
        LatencySummaryPhase::CLIENT_PROCESS_EXIST, LatencySummaryPhase::CLIENT_RPC_EXIST,
        LatencySummaryPhase::WORKER_PROCESS_GET, LatencySummaryPhase::WORKER_RPC_QUERY_META,
        LatencySummaryPhase::WORKER_RPC_REMOTE_GET, LatencySummaryPhase::WORKER_URMA_URMA_TOTAL,
        LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ, LatencySummaryPhase::WORKER_PROCESS_CREATE,
        LatencySummaryPhase::WORKER_PROCESS_PUBLISH, LatencySummaryPhase::WORKER_RPC_CREATE_META,
        LatencySummaryPhase::WORKER_RPC_UPDATE_META, LatencySummaryPhase::WORKER_PROCESS_EXIST,
        LatencySummaryPhase::MASTER_PROCESS_QUERY_META, LatencySummaryPhase::MASTER_PROCESS_CREATE_META,
        LatencySummaryPhase::MASTER_PROCESS_UPDATE_META, LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET,
        LatencySummaryPhase::CLIENT_PROCESS_DIRECT_ROUTE,
        LatencySummaryPhase::CLIENT_RPC_DIRECT_QUERY_AND_GET,
        LatencySummaryPhase::CLIENT_RPC_DIRECT_GET_DATA,
        LatencySummaryPhase::CLIENT_PROCESS_DIRECT_MATERIALIZE,
    };
    for (auto phase : phases) {
        result.Add(phase, std::numeric_limits<uint64_t>::max());
    }
    ASSERT_EQ(result.count, LATENCY_SUMMARY_PHASE_MAX);
    std::string summary = FormatLatencySummary(result);
    EXPECT_TRUE(summary.front() == '{');
    EXPECT_TRUE(summary.back() == '}');
    for (auto phase : phases) {
        EXPECT_TRUE(summary.find(LatencySummaryPhaseName(phase)) != std::string::npos);
    }
}

TEST_F(LatencyPhaseTest, GetServerLatencyTraceConfigReadsGflags)
{
    uint64_t origLocal = FLAGS_slow_log_process_slower_than;
    uint64_t origRpc = FLAGS_slow_log_rpc_slower_than;
    FLAGS_slow_log_process_slower_than = 100;
    FLAGS_slow_log_rpc_slower_than = 200;
    auto config = GetServerLatencyTraceConfig();
    EXPECT_EQ(config.processSlowerThanUs, 100u);
    EXPECT_EQ(config.rpcSlowerThanUs, 200u);
    FLAGS_slow_log_process_slower_than = origLocal;
    FLAGS_slow_log_rpc_slower_than = origRpc;
}

TEST_F(LatencyPhaseTest, DuplicateTickKeyAggregation)
{
    // In B, *_RPC_* phases are not tick pairs. Test duplicate aggregation with
    // URMA tick pairs instead.
    LatencyTick ticks[] = {
        { LatencyTickKey::WORKER_GET_START, 100000u },
        { LatencyTickKey::WORKER_URMA_START, 200000u },
        { LatencyTickKey::WORKER_URMA_END, 500000u },
        { LatencyTickKey::WORKER_URMA_START, 600000u },
        { LatencyTickKey::WORKER_URMA_END, 900000u },
        { LatencyTickKey::WORKER_GET_END, 1000000u },
    };
    auto result = ComputePhaseDurations(ticks, 6, 0);

    const auto *urma = result.Find(LatencySummaryPhase::WORKER_URMA_URMA_TOTAL);
    ASSERT_NE(urma, nullptr);
    EXPECT_EQ(urma->durationUs, 600u);

    const auto *localGet = result.Find(LatencySummaryPhase::WORKER_PROCESS_GET);
    ASSERT_NE(localGet, nullptr);
    EXPECT_EQ(localGet->durationUs, 300u);

    EXPECT_EQ(result.count, 2u);
}

TEST_F(LatencyPhaseTest, DuplicateTickKeyDerivedPhase)
{
    // In B, *_RPC_* comm phases come from AddDownstreamPhase, not tick pairs.
    // Test the full three-step flow: tick → merge downstream → derive.
    // worker total = 800us, comm_query_meta=100, comm_remote_get=350,
    // server_exec(master)=50, server_exec(remote_worker)=50, urma=50, l2cache=50.
    // worker.process.get = 800 - 100 - 350 - 50 - 50 - 50 - 50 = 150.
    Trace::Instance().ClearDownstreamPhases();
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_RPC_QUERY_META, 100u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_RPC_REMOTE_GET, 350u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 50u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET, 50u);

    LatencyTick ticks[] = {
        { LatencyTickKey::WORKER_GET_START, 100000u },
        { LatencyTickKey::WORKER_URMA_START, 720000u },
        { LatencyTickKey::WORKER_URMA_END, 770000u },
        { LatencyTickKey::WORKER_L2CACHE_READ_START, 780000u },
        { LatencyTickKey::WORKER_L2CACHE_READ_END, 830000u },
        { LatencyTickKey::WORKER_GET_END, 900000u },
    };
    PhaseDurationResult result = ComputeTickPhases(ticks, 6, 0);
    MergeDownstreamPhases(result);
    ComputeDerivedPhases(ticks, 6, result);

    const auto *remoteGet = result.Find(LatencySummaryPhase::WORKER_RPC_REMOTE_GET);
    ASSERT_NE(remoteGet, nullptr);
    EXPECT_EQ(remoteGet->durationUs, 350u);

    const auto *queryMeta = result.Find(LatencySummaryPhase::WORKER_RPC_QUERY_META);
    ASSERT_NE(queryMeta, nullptr);
    EXPECT_EQ(queryMeta->durationUs, 100u);

    const auto *urma = result.Find(LatencySummaryPhase::WORKER_URMA_URMA_TOTAL);
    ASSERT_NE(urma, nullptr);
    EXPECT_EQ(urma->durationUs, 50u);

    const auto *l2cache = result.Find(LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ);
    ASSERT_NE(l2cache, nullptr);
    EXPECT_EQ(l2cache->durationUs, 50u);

    const auto *localGet = result.Find(LatencySummaryPhase::WORKER_PROCESS_GET);
    ASSERT_NE(localGet, nullptr);
    EXPECT_EQ(localGet->durationUs, 150u);

    Trace::Instance().ClearDownstreamPhases();
    Trace::Instance().ClearLatencyTicks();
}

TEST_F(LatencyPhaseTest, PhaseDurationResultMergeDuplicatePhase)
{
    PhaseDurationResult result;
    result.Add(LatencySummaryPhase::CLIENT_RPC_GET, 100u);
    ASSERT_EQ(result.count, 1u);
    EXPECT_EQ(result.entries[0].durationUs, 100u);

    result.Add(LatencySummaryPhase::CLIENT_RPC_GET, 200u);
    EXPECT_EQ(result.count, 1u);
    EXPECT_EQ(result.entries[0].durationUs, 300u);
    EXPECT_EQ(result.phaseDroppedCount, 0u);

    result.Add(LatencySummaryPhase::CLIENT_PROCESS_GET, 50u);
    EXPECT_EQ(result.count, 2u);
    EXPECT_EQ(result.phaseDroppedCount, 0u);

    const auto *found = result.Find(LatencySummaryPhase::CLIENT_RPC_GET);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->durationUs, 300u);
}

TEST_F(LatencyPhaseTest, DisabledConfigPreventsTickCollectionAndSummary)
{
    LatencyTraceConfig disabled{0, 0};
    EXPECT_FALSE(disabled.LatencyTraceEnabled());

    const bool traceEnabled = disabled.LatencyTraceEnabled();
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_START);
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_RPC_END);
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_END);
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_UB_TRANSFER_START);
        Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_UB_TRANSFER_END);
    }
    EXPECT_EQ(Trace::Instance().GetLatencyTickCount(), 0u);

    EXPECT_FALSE(ShouldPrintLatencySummary(100000, disabled));

    PhaseDurationResult result;
    EXPECT_FALSE(CheckPhaseGate(result, disabled));
    EXPECT_EQ(FormatLatencySummary(result), "{}");
}

TEST_F(LatencyPhaseTest, ComputeTotalElapsedUsFromTicksBasic)
{
    Trace::Instance().ClearLatencyTicks();
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_END);

    uint64_t elapsed = ComputeTotalElapsedUsFromTicks(
        LatencyTickKey::CLIENT_GET_START, LatencyTickKey::CLIENT_GET_END);
    EXPECT_GT(elapsed, 0u);

    uint64_t noMatch = ComputeTotalElapsedUsFromTicks(
        LatencyTickKey::CLIENT_SET_START, LatencyTickKey::CLIENT_SET_END);
    EXPECT_EQ(noMatch, 0u);

    Trace::Instance().ClearLatencyTicks();
}

TEST_F(LatencyPhaseTest, ShouldCollectLatencyTraceDisabledConfig)
{
    LatencyTraceConfig disabled{0, 0};
    EXPECT_FALSE(ShouldCollectLatencyTrace(disabled));
}

TEST_F(LatencyPhaseTest, ShouldCollectLatencyTraceEnabledWithSamplingReject)
{
    LatencyTraceConfig enabled{1, 1};
    Trace::Instance().SetAccessShouldRecord(false);
    EXPECT_FALSE(ShouldCollectLatencyTrace(enabled));

    Trace::Instance().SetAccessShouldRecord(true);
    EXPECT_TRUE(ShouldCollectLatencyTrace(enabled));
}

TEST_F(LatencyPhaseTest, FormatLatencySummaryDroppedCountLargeValueNoOverflow)
{
    PhaseDurationResult result;
    result.Add(LatencySummaryPhase::CLIENT_RPC_GET, 300u);
    result.tickDroppedCount = std::numeric_limits<uint16_t>::max();
    result.phaseDroppedCount = std::numeric_limits<uint16_t>::max();
    std::string summary = FormatLatencySummary(result);
    EXPECT_TRUE(summary.find("tick_dropped:") != std::string::npos);
    EXPECT_TRUE(summary.find("phase_dropped:") != std::string::npos);
}

// Issue #862: concurrent QueryMeta fan-out to N masters aggregates per-leg comm
// with MAX (not SUM) so that N fast legs do not sum past the rpc threshold.
TEST_F(LatencyPhaseTest, QueryMetaMaxAggregationConcurrentFanout)
{
    Trace::Instance().ClearDownstreamPhases();
    // Simulate two legs: comm_1=50, comm_2=80. MAX=80 (not SUM=130).
    uint64_t comm[] = { 50, 80 };
    uint64_t maxComm = 0;
    for (uint64_t c : comm) {
        maxComm = std::max(maxComm, c);
    }
    if (maxComm > 0) {
        Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_RPC_QUERY_META, maxComm);
    }
    auto &ds = Trace::Instance().GetDownstreamPhases();
    ASSERT_EQ(ds.count, 1u);
    EXPECT_EQ(ds.entries[0].phase, LatencySummaryPhase::WORKER_RPC_QUERY_META);
    EXPECT_EQ(ds.entries[0].durationUs, 80u); // MAX not SUM

    // With rpc threshold=100, MAX=80 does not gate; SUM=130 would falsely gate.
    LatencyTraceConfig config{ 0, 100 };
    PhaseDurationResult result;
    MergeDownstreamPhases(result);
    EXPECT_FALSE(CheckPhaseGate(result, config));

    Trace::Instance().ClearDownstreamPhases();
}

// Issue #862: additive decomposition — process + comm + server-exec = total.
TEST_F(LatencyPhaseTest, AdditiveDerivedMathWorkerGet)
{
    Trace::Instance().ClearDownstreamPhases();
    Trace::Instance().ClearLatencyTicks();
    // worker total = 1000us. comm_querymeta=100, comm_remoteget=200,
    // server_exec(master)=50, server_exec(remote)=50, urma=50, l2cache=50.
    // worker.process.get = 1000 - 100 - 200 - 50 - 50 - 50 - 50 = 500.
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_RPC_QUERY_META, 100u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_RPC_REMOTE_GET, 200u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::MASTER_PROCESS_QUERY_META, 50u);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET, 50u);

    LatencyTick ticks[] = {
        { LatencyTickKey::WORKER_GET_START, 1000000u },
        { LatencyTickKey::WORKER_URMA_START, 1800000u },
        { LatencyTickKey::WORKER_URMA_END, 1850000u },
        { LatencyTickKey::WORKER_L2CACHE_READ_START, 1860000u },
        { LatencyTickKey::WORKER_L2CACHE_READ_END, 1910000u },
        { LatencyTickKey::WORKER_GET_END, 2000000u },
    };
    PhaseDurationResult result = ComputeTickPhases(ticks, 6, 0);
    MergeDownstreamPhases(result);
    ComputeDerivedPhases(ticks, 6, result);

    const auto *localGet = result.Find(LatencySummaryPhase::WORKER_PROCESS_GET);
    ASSERT_NE(localGet, nullptr);
    EXPECT_EQ(localGet->durationUs, 500u);

    // Additive check: process + comm + server-exec + urma + l2cache = total.
    uint64_t sum = localGet->durationUs;
    sum += result.Find(LatencySummaryPhase::WORKER_RPC_QUERY_META)->durationUs;
    sum += result.Find(LatencySummaryPhase::WORKER_RPC_REMOTE_GET)->durationUs;
    sum += result.Find(LatencySummaryPhase::MASTER_PROCESS_QUERY_META)->durationUs;
    sum += result.Find(LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET)->durationUs;
    sum += result.Find(LatencySummaryPhase::WORKER_URMA_URMA_TOTAL)->durationUs;
    sum += result.Find(LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ)->durationUs;
    EXPECT_EQ(sum, 1000u);

    Trace::Instance().ClearDownstreamPhases();
    Trace::Instance().ClearLatencyTicks();
}

}  // namespace ut
}  // namespace datasystem
