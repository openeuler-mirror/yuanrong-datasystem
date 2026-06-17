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

#ifndef DATASYSTEM_COMMON_LOG_LATENCY_PHASE_H
#define DATASYSTEM_COMMON_LOG_LATENCY_PHASE_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/log/latency_phase_types.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {

PhaseDurationResult ComputePhaseDurations(const LatencyTick *ticks, uint16_t tickCount, uint16_t tickDroppedCount);

std::string FormatLatencySummary(const PhaseDurationResult &result);

bool ShouldPrintLatencySummary(uint64_t totalElapsedUs, const LatencyTraceConfig &config);

bool CheckPhaseGate(const PhaseDurationResult &result, const LatencyTraceConfig &config);

/**
 * @brief Append a phase-duration pair into a packed vector for proto encoding.
 * @param[out] phases Vector to append [phaseId, durationUs] pair into.
 * @param[in] phaseId LatencySummaryPhase enum value.
 * @param[in] durationUs Duration in microseconds; capped to uint32_t max on overflow.
 */
void AppendPhasePair(std::vector<uint32_t> &phases, LatencySummaryPhase phaseId, uint64_t durationUs);

/**
 * @brief Decode a packed phase-duration vector into a PhaseDurationResult.
 * @param[in] packed Flat vector of [phaseId, durationUs, ...] pairs.
 * @param[out] result Decoded phase durations.
 * @return true if packed size is even and decoding succeeded.
 */
bool DecodePhasePairs(const std::vector<uint32_t> &packed, PhaseDurationResult &result);

/**
 * @brief Merge downstream phase data from a packed proto field into the current Trace.
 * @param[in] packedPhases Packed [phaseId, durationUs] pairs from downstream response.
 * @param[in] protoDroppedCount Combined tick+phase dropped count from downstream proto.
 */
void MergeDecodedPhasesToTrace(const std::vector<uint32_t> &packedPhases, uint32_t protoDroppedCount);

LatencyTraceConfig GetServerLatencyTraceConfig();

LatencyTraceConfig GetClientLatencyTraceConfig();

inline bool ShouldCollectLatencyTrace(const LatencyTraceConfig &config)
{
    if (!config.LatencyTraceEnabled()) {
        return false;
    }
    if (!Trace::Instance().GetAccessShouldRecord()) {
        return false;
    }
    return true;
}

/**
 * @brief Merge downstream phase durations from Trace into the current result.
 * Adds each downstream phase entry into result if not already present, otherwise sums durations.
 * @param[out] result Phase durations to merge downstream data into.
 */
void MergeDownstreamPhases(PhaseDurationResult &result);

template <typename RspPb>
void EncodePhaseProto(RspPb &rsp, const PhaseDurationResult &result)
{
    auto *phases = rsp.mutable_latency_phase_us();
    for (uint16_t i = 0; i < result.count; ++i) {
        phases->Add(static_cast<unsigned int>(result.entries[i].phase));
        phases->Add(static_cast<unsigned int>(result.entries[i].durationUs));
    }
    if (result.droppedCount() > 0) {
        rsp.set_latency_tick_dropped_count(static_cast<uint32_t>(result.droppedCount()));
    }
}

uint64_t ComputeTotalElapsedUsFromTicks(LatencyTickKey startKey, LatencyTickKey endKey);

void EmitClientLatencySummary(LatencyTickKey startKey, LatencyTickKey endKey);

/**
 * @brief Finalize and encode latency summary for worker-to-worker remote-get RPC handlers.
 * Adds DATA_REMOTEGET_END tick, computes phase durations, merges downstream phases,
 * and encodes into the response proto if slow-log gate is hit.
 * @tparam RspPb Response protobuf type with latency_phase_us field.
 * @param[in] config Server latency trace configuration.
 * @param[in] traceEnabled Whether latency trace collection is enabled for this request.
 * @param[out] rsp Response protobuf to encode latency phase data into.
 */
template <typename RspPb>
void TryEncodeRemoteGetLatencySummary(const LatencyTraceConfig &config, bool traceEnabled, RspPb &rsp)
{
    if (!traceEnabled) {
        return;
    }
    Trace::Instance().AddLatencyTick(LatencyTickKey::DATA_REMOTEGET_END);
    auto ticks = Trace::Instance().GetLatencyTicks();
    auto tickCount = Trace::Instance().GetLatencyTickCount();
    auto droppedCount = Trace::Instance().GetLatencyTickDroppedCount();
    if (tickCount >= MIN_TICK_COUNT_FOR_SUMMARY) {
        uint64_t totalElapsedUs = (ticks[tickCount - 1].tick - ticks[0].tick) / NS_PER_US;
        if (ShouldPrintLatencySummary(totalElapsedUs, config)) {
            PhaseDurationResult result = ComputePhaseDurations(ticks, tickCount, droppedCount);
            bool hasDownstream = Trace::Instance().GetDownstreamPhases().count > 0;
            MergeDownstreamPhases(result);
            if (CheckPhaseGate(result, config) || hasDownstream) {
                EncodePhaseProto(rsp, result);
            }
        }
    }
    Trace::Instance().ClearLatencyTicks();
}

/**
 * @brief Finalize worker-side latency trace at request end.
 * Adds the END tick, computes phase durations, sets latencySummary on the Trace if
 * slow-log gate is hit, and optionally encodes phase data into the response proto.
 * @tparam RspPb Response protobuf type with latency_phase_us field.
 * @param[in] endKey The latency tick key for the request end boundary.
 * @param[in] config Server latency trace configuration.
 * @param[in] traceEnabled Whether latency trace collection is enabled for this request.
 * @param[in] totalUs Total elapsed time in microseconds for the request.
 * @param[in] encodeProto Whether to encode phase data into the response (true for normal
 *            completion, false for timeout/error early returns that don't carry proto phases).
 * @param[out] resp Response protobuf to encode latency phase data into when encodeProto is true.
 */
template <typename RspPb>
void FinalizeWorkerLatencyTrace(LatencyTickKey endKey, const LatencyTraceConfig &config,
                                bool traceEnabled, uint64_t totalUs, bool encodeProto, RspPb &resp)
{
    if (!traceEnabled) {
        return;
    }
    Trace::Instance().AddLatencyTick(endKey);
    if (!ShouldPrintLatencySummary(totalUs, config)) {
        return;
    }
    PhaseDurationResult result = ComputePhaseDurations(
        Trace::Instance().GetLatencyTicks(), Trace::Instance().GetLatencyTickCount(),
        Trace::Instance().GetLatencyTickDroppedCount());
    bool hasDownstream = Trace::Instance().GetDownstreamPhases().count > 0;
    MergeDownstreamPhases(result);
    bool gateHit = CheckPhaseGate(result, config);
    if (gateHit) {
        Trace::Instance().SetLatencySummary(FormatLatencySummary(result));
    }
    if (encodeProto && (gateHit || hasDownstream)) {
        EncodePhaseProto(resp, result);
    }
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LATENCY_PHASE_H
