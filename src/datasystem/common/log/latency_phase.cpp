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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/gflag/common_gflags.h"

namespace datasystem {

namespace {

struct TickPairMapping {
    LatencyTickKey startKey;
    LatencyTickKey endKey;
    LatencySummaryPhase phase;
};

constexpr TickPairMapping TICK_PAIR_TABLE[] = {
    { LatencyTickKey::CLIENT_GET_RPC_START, LatencyTickKey::CLIENT_GET_RPC_END,
      LatencySummaryPhase::CLIENT_RPC_GET },
    { LatencyTickKey::CLIENT_MEMORY_COPY_START, LatencyTickKey::CLIENT_MEMORY_COPY_END,
      LatencySummaryPhase::CLIENT_PROCESS_MEMORY_COPY },
    { LatencyTickKey::CLIENT_UB_TRANSFER_START, LatencyTickKey::CLIENT_UB_TRANSFER_END,
      LatencySummaryPhase::CLIENT_URMA_UB_TRANSFER },
    { LatencyTickKey::CLIENT_PUBLISH_RPC_START, LatencyTickKey::CLIENT_PUBLISH_RPC_END,
      LatencySummaryPhase::CLIENT_RPC_PUBLISH },
    { LatencyTickKey::CLIENT_EXIST_RPC_START, LatencyTickKey::CLIENT_EXIST_RPC_END,
      LatencySummaryPhase::CLIENT_RPC_EXIST },
    { LatencyTickKey::CLIENT_CREATE_RPC_START, LatencyTickKey::CLIENT_CREATE_RPC_END,
      LatencySummaryPhase::CLIENT_RPC_CREATE },
    { LatencyTickKey::WORKER_QUERYMETA_START, LatencyTickKey::WORKER_QUERYMETA_END,
      LatencySummaryPhase::WORKER_RPC_QUERY_META },
    { LatencyTickKey::WORKER_EXIST_QUERYMETA_START, LatencyTickKey::WORKER_EXIST_QUERYMETA_END,
      LatencySummaryPhase::WORKER_RPC_QUERY_META },
    { LatencyTickKey::WORKER_REMOTEGET_START, LatencyTickKey::WORKER_REMOTEGET_END,
      LatencySummaryPhase::WORKER_RPC_REMOTE_GET },
    { LatencyTickKey::WORKER_URMA_START, LatencyTickKey::WORKER_URMA_END,
      LatencySummaryPhase::WORKER_URMA_URMA_TOTAL },
    { LatencyTickKey::WORKER_L2CACHE_READ_START, LatencyTickKey::WORKER_L2CACHE_READ_END,
      LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ },
    { LatencyTickKey::WORKER_CREATE_META_RPC_START, LatencyTickKey::WORKER_CREATE_META_RPC_END,
      LatencySummaryPhase::WORKER_RPC_CREATE_META },
    { LatencyTickKey::WORKER_UPDATE_META_RPC_START, LatencyTickKey::WORKER_UPDATE_META_RPC_END,
      LatencySummaryPhase::WORKER_RPC_UPDATE_META },
    { LatencyTickKey::META_QUERYMETA_START, LatencyTickKey::META_QUERYMETA_END,
      LatencySummaryPhase::MASTER_PROCESS_QUERY_META },
    { LatencyTickKey::META_CREATE_META_START, LatencyTickKey::META_CREATE_META_END,
      LatencySummaryPhase::MASTER_PROCESS_CREATE_META },
    { LatencyTickKey::META_UPDATE_META_START, LatencyTickKey::META_UPDATE_META_END,
      LatencySummaryPhase::MASTER_PROCESS_UPDATE_META },
    { LatencyTickKey::DATA_REMOTEGET_START, LatencyTickKey::DATA_REMOTEGET_END,
      LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET },
};

constexpr size_t TICK_PAIR_TABLE_SIZE = sizeof(TICK_PAIR_TABLE) / sizeof(TICK_PAIR_TABLE[0]);

struct DerivedPhaseMapping {
    LatencyTickKey totalStartKey;
    LatencyTickKey totalEndKey;
    LatencySummaryPhase resultPhase;
    LatencySummaryPhase subPhases[4];
    uint8_t subPhaseCount;
};

constexpr DerivedPhaseMapping DERIVED_PHASE_TABLE[] = {
    { LatencyTickKey::CLIENT_GET_START, LatencyTickKey::CLIENT_GET_END,
      LatencySummaryPhase::CLIENT_PROCESS_GET,
      { LatencySummaryPhase::CLIENT_RPC_GET }, 1 },
    { LatencyTickKey::CLIENT_SET_START, LatencyTickKey::CLIENT_SET_END,
      LatencySummaryPhase::CLIENT_PROCESS_SET,
      { LatencySummaryPhase::CLIENT_RPC_CREATE, LatencySummaryPhase::CLIENT_PROCESS_MEMORY_COPY,
        LatencySummaryPhase::CLIENT_URMA_UB_TRANSFER, LatencySummaryPhase::CLIENT_RPC_PUBLISH }, 4 },
    { LatencyTickKey::CLIENT_CREATE_START, LatencyTickKey::CLIENT_CREATE_END,
      LatencySummaryPhase::CLIENT_PROCESS_CREATE,
      { LatencySummaryPhase::CLIENT_RPC_CREATE }, 1 },
    { LatencyTickKey::CLIENT_EXIST_START, LatencyTickKey::CLIENT_EXIST_END,
      LatencySummaryPhase::CLIENT_PROCESS_EXIST,
      { LatencySummaryPhase::CLIENT_RPC_EXIST }, 1 },
    { LatencyTickKey::WORKER_GET_START, LatencyTickKey::WORKER_GET_END,
      LatencySummaryPhase::WORKER_PROCESS_GET,
      { LatencySummaryPhase::WORKER_RPC_QUERY_META, LatencySummaryPhase::WORKER_RPC_REMOTE_GET,
        LatencySummaryPhase::WORKER_URMA_URMA_TOTAL, LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ }, 4 },
    { LatencyTickKey::WORKER_CREATE_START, LatencyTickKey::WORKER_CREATE_END,
      LatencySummaryPhase::WORKER_PROCESS_CREATE,
      {}, 0 },
    { LatencyTickKey::WORKER_PUBLISH_START, LatencyTickKey::WORKER_PUBLISH_END,
      LatencySummaryPhase::WORKER_PROCESS_PUBLISH,
      { LatencySummaryPhase::WORKER_RPC_CREATE_META, LatencySummaryPhase::WORKER_RPC_UPDATE_META }, 2 },
    { LatencyTickKey::WORKER_EXIST_START, LatencyTickKey::WORKER_EXIST_END,
      LatencySummaryPhase::WORKER_PROCESS_EXIST,
      { LatencySummaryPhase::WORKER_RPC_QUERY_META }, 1 },
};

constexpr size_t DERIVED_PHASE_TABLE_SIZE = sizeof(DERIVED_PHASE_TABLE) / sizeof(DERIVED_PHASE_TABLE[0]);

struct TickMatches {
    uint64_t ticks[LATENCY_TICK_MAX_NUM];
    uint16_t count = 0;
};

TickMatches FindAllTickNs(const LatencyTick *ticks, uint16_t tickCount, LatencyTickKey key)
{
    TickMatches result;
    for (uint16_t i = 0; i < tickCount; ++i) {
        if (ticks[i].key == key && result.count < LATENCY_TICK_MAX_NUM) {
            result.ticks[result.count] = ticks[i].tick;
            result.count++;
        }
    }
    return result;
}

struct PhaseNameEntry {
    LatencySummaryPhase phase;
    const char *name;
};

constexpr PhaseNameEntry PHASE_NAME_TABLE[] = {
    { LatencySummaryPhase::CLIENT_PROCESS_GET, "client.process.get" },
    { LatencySummaryPhase::CLIENT_RPC_GET, "client.rpc.get" },
    { LatencySummaryPhase::CLIENT_PROCESS_SET, "client.process.set" },
    { LatencySummaryPhase::CLIENT_RPC_CREATE, "client.rpc.create" },
    { LatencySummaryPhase::CLIENT_PROCESS_MEMORY_COPY, "client.process.memory_copy" },
    { LatencySummaryPhase::CLIENT_RPC_PUBLISH, "client.rpc.publish" },
    { LatencySummaryPhase::CLIENT_URMA_UB_TRANSFER, "client.urma.ub_transfer" },
    { LatencySummaryPhase::CLIENT_PROCESS_CREATE, "client.process.create" },
    { LatencySummaryPhase::CLIENT_PROCESS_EXIST, "client.process.exist" },
    { LatencySummaryPhase::CLIENT_RPC_EXIST, "client.rpc.exist" },
    { LatencySummaryPhase::WORKER_PROCESS_GET, "worker.process.get" },
    { LatencySummaryPhase::WORKER_RPC_QUERY_META, "worker.rpc.query_meta" },
    { LatencySummaryPhase::WORKER_RPC_REMOTE_GET, "worker.rpc.remote_get" },
    { LatencySummaryPhase::WORKER_URMA_URMA_TOTAL, "worker.urma.urma_total" },
    { LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ, "worker.process.l2cache_read" },
    { LatencySummaryPhase::WORKER_PROCESS_CREATE, "worker.process.create" },
    { LatencySummaryPhase::WORKER_PROCESS_PUBLISH, "worker.process.publish" },
    { LatencySummaryPhase::WORKER_RPC_CREATE_META, "worker.rpc.create_meta" },
    { LatencySummaryPhase::WORKER_RPC_UPDATE_META, "worker.rpc.update_meta" },
    { LatencySummaryPhase::WORKER_PROCESS_EXIST, "worker.process.exist" },
    { LatencySummaryPhase::MASTER_PROCESS_QUERY_META, "master.process.query_meta" },
    { LatencySummaryPhase::MASTER_PROCESS_CREATE_META, "master.process.create_meta" },
    { LatencySummaryPhase::MASTER_PROCESS_UPDATE_META, "master.process.update_meta" },
    { LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET, "worker.process.remote_get" },
};

constexpr size_t PHASE_NAME_TABLE_SIZE = sizeof(PHASE_NAME_TABLE) / sizeof(PHASE_NAME_TABLE[0]);

constexpr LatencySummaryPhase PROCESS_PHASES[] = {
    LatencySummaryPhase::CLIENT_PROCESS_GET,
    LatencySummaryPhase::CLIENT_PROCESS_SET,
    LatencySummaryPhase::CLIENT_PROCESS_MEMORY_COPY,
    LatencySummaryPhase::CLIENT_PROCESS_CREATE,
    LatencySummaryPhase::CLIENT_PROCESS_EXIST,
    LatencySummaryPhase::WORKER_PROCESS_GET,
    LatencySummaryPhase::WORKER_PROCESS_L2CACHE_READ,
    LatencySummaryPhase::WORKER_PROCESS_CREATE,
    LatencySummaryPhase::WORKER_PROCESS_PUBLISH,
    LatencySummaryPhase::WORKER_PROCESS_EXIST,
    LatencySummaryPhase::MASTER_PROCESS_QUERY_META,
    LatencySummaryPhase::MASTER_PROCESS_CREATE_META,
    LatencySummaryPhase::MASTER_PROCESS_UPDATE_META,
    LatencySummaryPhase::WORKER_PROCESS_REMOTE_GET,
};

constexpr size_t PROCESS_PHASES_SIZE = sizeof(PROCESS_PHASES) / sizeof(PROCESS_PHASES[0]);

constexpr LatencySummaryPhase RPC_PHASES[] = {
    LatencySummaryPhase::CLIENT_RPC_GET,
    LatencySummaryPhase::CLIENT_RPC_CREATE,
    LatencySummaryPhase::CLIENT_RPC_PUBLISH,
    LatencySummaryPhase::CLIENT_URMA_UB_TRANSFER,
    LatencySummaryPhase::CLIENT_RPC_EXIST,
    LatencySummaryPhase::WORKER_RPC_QUERY_META,
    LatencySummaryPhase::WORKER_RPC_REMOTE_GET,
    LatencySummaryPhase::WORKER_URMA_URMA_TOTAL,
    LatencySummaryPhase::WORKER_RPC_CREATE_META,
    LatencySummaryPhase::WORKER_RPC_UPDATE_META,
};

constexpr size_t RPC_PHASES_SIZE = sizeof(RPC_PHASES) / sizeof(RPC_PHASES[0]);

constexpr int UINT64_MAX_DECIMAL_DIGITS = 20;
constexpr int DECIMAL_BASE = 10;

void AppendUint64(std::string &buf, uint64_t value)
{
    if (value == 0) {
        buf += '0';
        return;
    }
    char tmp[UINT64_MAX_DECIMAL_DIGITS];
    int pos = 0;
    while (value > 0) {
        tmp[pos++] = '0' + static_cast<char>(value % DECIMAL_BASE);
        value /= DECIMAL_BASE;
    }
    for (int i = pos - 1; i >= 0; --i) {
        buf += tmp[i];
    }
}

void AppendDroppedCounts(std::string &buf, const PhaseDurationResult &result)
{
    if (result.tickDroppedCount == 0 && result.phaseDroppedCount == 0) {
        return;
    }
    if (result.count > 0) {
        buf += ",";
    }
    if (result.tickDroppedCount > 0) {
        buf += "tick_dropped:";
        AppendUint64(buf, static_cast<uint64_t>(result.tickDroppedCount));
    }
    if (result.phaseDroppedCount > 0) {
        if (result.tickDroppedCount > 0) {
            buf += ",";
        }
        buf += "phase_dropped:";
        AppendUint64(buf, static_cast<uint64_t>(result.phaseDroppedCount));
    }
}

}  // namespace

void PhaseDurationResult::Add(LatencySummaryPhase phase, uint64_t durationUs)
{
    for (uint16_t i = 0; i < count; ++i) {
        if (entries[i].phase == phase) {
            entries[i].durationUs += durationUs;
            return;
        }
    }
    if (count < LATENCY_SUMMARY_PHASE_MAX) {
        entries[count].phase = phase;
        entries[count].durationUs = durationUs;
        count++;
    } else {
        phaseDroppedCount++;
    }
}

void DownstreamPhaseResult::Add(LatencySummaryPhase phase, uint64_t durationUs)
{
    if (count < DOWNSTREAM_PHASE_MAX_NUM) {
        entries[count].phase = phase;
        entries[count].durationUs = durationUs;
        count++;
    } else {
        phaseDroppedCount++;
    }
}

const PhaseDuration *PhaseDurationResult::Find(LatencySummaryPhase phase) const
{
    for (uint16_t i = 0; i < count; ++i) {
        if (entries[i].phase == phase) {
            return &entries[i];
        }
    }
    return nullptr;
}

bool IsProcessPhase(LatencySummaryPhase phase)
{
    for (size_t i = 0; i < PROCESS_PHASES_SIZE; ++i) {
        if (PROCESS_PHASES[i] == phase) {
            return true;
        }
    }
    return false;
}

bool IsRpcPhase(LatencySummaryPhase phase)
{
    for (size_t i = 0; i < RPC_PHASES_SIZE; ++i) {
        if (RPC_PHASES[i] == phase) {
            return true;
        }
    }
    return false;
}

const char *LatencySummaryPhaseName(LatencySummaryPhase phase)
{
    for (size_t i = 0; i < PHASE_NAME_TABLE_SIZE; ++i) {
        if (PHASE_NAME_TABLE[i].phase == phase) {
            return PHASE_NAME_TABLE[i].name;
        }
    }
    return "unknown";
}

PhaseDurationResult ComputePhaseDurations(const LatencyTick *ticks, uint16_t tickCount, uint16_t tickDroppedCount)
{
    PhaseDurationResult result;
    result.tickDroppedCount = tickDroppedCount;

    for (size_t t = 0; t < TICK_PAIR_TABLE_SIZE; ++t) {
        TickMatches starts = FindAllTickNs(ticks, tickCount, TICK_PAIR_TABLE[t].startKey);
        TickMatches ends = FindAllTickNs(ticks, tickCount, TICK_PAIR_TABLE[t].endKey);
        uint64_t totalDurationUs = 0;
        bool anyValidPair = false;
        uint16_t pairCount = std::min(starts.count, ends.count);
        for (uint16_t p = 0; p < pairCount; ++p) {
            if (starts.ticks[p] > 0 && ends.ticks[p] > 0 && ends.ticks[p] >= starts.ticks[p]) {
                totalDurationUs += (ends.ticks[p] - starts.ticks[p]) / NS_PER_US;
                anyValidPair = true;
            }
        }
        if (anyValidPair) {
            result.Add(TICK_PAIR_TABLE[t].phase, totalDurationUs);
        }
    }

    for (size_t d = 0; d < DERIVED_PHASE_TABLE_SIZE; ++d) {
        TickMatches totalStarts = FindAllTickNs(ticks, tickCount, DERIVED_PHASE_TABLE[d].totalStartKey);
        TickMatches totalEnds = FindAllTickNs(ticks, tickCount, DERIVED_PHASE_TABLE[d].totalEndKey);
        if (totalStarts.count == 0 || totalEnds.count == 0) {
            continue;
        }
        uint64_t totalStartNs = totalStarts.ticks[0];
        uint64_t totalEndNs = totalEnds.ticks[totalEnds.count - 1];
        if (totalStartNs == 0 || totalEndNs == 0 || totalEndNs < totalStartNs) {
            continue;
        }
        uint64_t totalUs = (totalEndNs - totalStartNs) / NS_PER_US;
        uint64_t subSumUs = 0;
        for (uint8_t s = 0; s < DERIVED_PHASE_TABLE[d].subPhaseCount; ++s) {
            const PhaseDuration *sub = result.Find(DERIVED_PHASE_TABLE[d].subPhases[s]);
            if (sub != nullptr) {
                subSumUs += sub->durationUs;
            }
        }
        uint64_t derivedUs = (subSumUs >= totalUs) ? 0 : (totalUs - subSumUs);
        result.Add(DERIVED_PHASE_TABLE[d].resultPhase, derivedUs);
    }

    return result;
}

std::string FormatLatencySummary(const PhaseDurationResult &result)
{
    std::string buf = "{";
    for (uint16_t i = 0; i < result.count; ++i) {
        if (i > 0) {
            buf += ",";
        }
        buf += LatencySummaryPhaseName(result.entries[i].phase);
        buf += ":";
        AppendUint64(buf, result.entries[i].durationUs);
    }
    AppendDroppedCounts(buf, result);
    buf += "}";
    return buf;
}

bool ShouldPrintLatencySummary(uint64_t totalElapsedUs, const LatencyTraceConfig &config)
{
    if (!config.LatencyTraceEnabled()) {
        return false;
    }
    uint64_t minThreshold = 0;
    if (config.processSlowerThanUs > 0 && config.rpcSlowerThanUs > 0) {
        minThreshold = std::min(config.processSlowerThanUs, config.rpcSlowerThanUs);
    } else if (config.processSlowerThanUs > 0) {
        minThreshold = config.processSlowerThanUs;
    } else {
        minThreshold = config.rpcSlowerThanUs;
    }
    return totalElapsedUs >= minThreshold;
}

bool CheckPhaseGate(const PhaseDurationResult &result, const LatencyTraceConfig &config)
{
    if (!config.LatencyTraceEnabled()) {
        return false;
    }
    for (uint16_t i = 0; i < result.count; ++i) {
        if (IsProcessPhase(result.entries[i].phase) && config.processSlowerThanUs > 0
            && result.entries[i].durationUs >= config.processSlowerThanUs) {
            return true;
        }
        if (IsRpcPhase(result.entries[i].phase) && config.rpcSlowerThanUs > 0
            && result.entries[i].durationUs >= config.rpcSlowerThanUs) {
            return true;
        }
    }
    return false;
}

void AppendPhasePair(std::vector<uint32_t> &phases, LatencySummaryPhase phaseId, uint64_t durationUs)
{
    phases.push_back(static_cast<uint32_t>(phaseId));
    uint32_t cappedUs = (durationUs > std::numeric_limits<uint32_t>::max())
                         ? std::numeric_limits<uint32_t>::max()
                         : static_cast<uint32_t>(durationUs);
    phases.push_back(cappedUs);
}

bool DecodePhasePairs(const std::vector<uint32_t> &packed, PhaseDurationResult &result)
{
    if (packed.size() % PHASE_PAIR_SIZE != 0) {
        return false;
    }
    for (size_t i = 0; i + 1 < packed.size(); i += PHASE_PAIR_SIZE) {
        uint32_t phaseId = packed[i];
        uint32_t durationUs = packed[i + 1];
        if (phaseId < 1 || phaseId > static_cast<uint32_t>(LATENCY_SUMMARY_PHASE_MAX)) {
            continue;
        }
        result.Add(static_cast<LatencySummaryPhase>(phaseId), durationUs);
    }
    return true;
}

LatencyTraceConfig GetServerLatencyTraceConfig()
{
    LatencyTraceConfig config;
    config.processSlowerThanUs = FLAGS_slow_log_process_slower_than;
    config.rpcSlowerThanUs = FLAGS_slow_log_rpc_slower_than;
    return config;
}

namespace {
constexpr uint64_t DEFAULT_CLIENT_PROCESS_SLOWER_THAN_US = 2000;
constexpr uint64_t DEFAULT_CLIENT_RPC_SLOWER_THAN_US = 5000;
std::atomic<uint64_t> g_clientProcessSlowerThanUs{DEFAULT_CLIENT_PROCESS_SLOWER_THAN_US};
std::atomic<uint64_t> g_clientRpcSlowerThanUs{DEFAULT_CLIENT_RPC_SLOWER_THAN_US};

struct ClientLatencyTraceConfigInit {
    ClientLatencyTraceConfigInit() noexcept
    {
        g_clientProcessSlowerThanUs.store(GetUint64FromEnv("DATASYSTEM_CLIENT_SLOW_LOG_PROCESS_SLOWER_THAN",
                                                           DEFAULT_CLIENT_PROCESS_SLOWER_THAN_US),
                                          std::memory_order_relaxed);
        g_clientRpcSlowerThanUs.store(GetUint64FromEnv("DATASYSTEM_CLIENT_SLOW_LOG_RPC_SLOWER_THAN",
                                                       DEFAULT_CLIENT_RPC_SLOWER_THAN_US),
                                      std::memory_order_relaxed);
    }
};

static ClientLatencyTraceConfigInit g_clientLatencyTraceConfigInit;

bool ValidateSlowLogProcessSlowerThan(const char *, uint64_t value)
{
    g_clientProcessSlowerThanUs.store(value, std::memory_order_relaxed);
    return true;
}

bool ValidateSlowLogRpcSlowerThan(const char *, uint64_t value)
{
    g_clientRpcSlowerThanUs.store(value, std::memory_order_relaxed);
    return true;
}
}  // namespace

DS_DEFINE_validator(slow_log_process_slower_than, ValidateSlowLogProcessSlowerThan);
DS_DEFINE_validator(slow_log_rpc_slower_than, ValidateSlowLogRpcSlowerThan);

LatencyTraceConfig GetClientLatencyTraceConfig()
{
    LatencyTraceConfig config;
    config.processSlowerThanUs = g_clientProcessSlowerThanUs.load(std::memory_order_relaxed);
    config.rpcSlowerThanUs = g_clientRpcSlowerThanUs.load(std::memory_order_relaxed);
    return config;
}

void MergeDecodedPhasesToTrace(const std::vector<uint32_t> &packedPhases, uint32_t protoDroppedCount)
{
    PhaseDurationResult decoded;
    if (!DecodePhasePairs(packedPhases, decoded)) {
        return;
    }
    for (uint16_t i = 0; i < decoded.count; ++i) {
        Trace::Instance().AddDownstreamPhase(decoded.entries[i].phase, decoded.entries[i].durationUs);
    }
    Trace::Instance().AddDownstreamTickDroppedCount(static_cast<uint16_t>(protoDroppedCount));
    Trace::Instance().AddDownstreamPhaseDroppedCount(static_cast<uint16_t>(decoded.phaseDroppedCount));
}

void MergeDownstreamPhases(PhaseDurationResult &result)
{
    auto &downstream = Trace::Instance().GetDownstreamPhases();
    if (downstream.count == 0) {
        return;
    }
    for (uint16_t di = 0; di < downstream.count; ++di) {
        result.Add(downstream.entries[di].phase, downstream.entries[di].durationUs);
    }
    result.tickDroppedCount += downstream.tickDroppedCount;
    result.phaseDroppedCount += downstream.phaseDroppedCount;
}

uint64_t ComputeTotalElapsedUsFromTicks(LatencyTickKey startKey, LatencyTickKey endKey)
{
    auto ticks = Trace::Instance().GetLatencyTicks();
    auto tickCount = Trace::Instance().GetLatencyTickCount();
    TickMatches starts = FindAllTickNs(ticks, tickCount, startKey);
    TickMatches ends = FindAllTickNs(ticks, tickCount, endKey);
    if (starts.count == 0 || ends.count == 0) {
        return 0;
    }
    uint64_t startNs = starts.ticks[0];
    uint64_t endNs = ends.ticks[ends.count - 1];
    if (startNs > 0 && endNs > 0 && endNs >= startNs) {
        return (endNs - startNs) / NS_PER_US;
    }
    return 0;
}

void EmitClientLatencySummary(LatencyTickKey startKey, LatencyTickKey endKey)
{
    auto config = GetClientLatencyTraceConfig();
    uint64_t totalElapsedUs = ComputeTotalElapsedUsFromTicks(startKey, endKey);
    if (ShouldPrintLatencySummary(totalElapsedUs, config)) {
        auto ticks = Trace::Instance().GetLatencyTicks();
        auto tickCount = Trace::Instance().GetLatencyTickCount();
        auto droppedCount = Trace::Instance().GetLatencyTickDroppedCount();
        PhaseDurationResult result = ComputePhaseDurations(ticks, tickCount, droppedCount);
        MergeDownstreamPhases(result);
        if (CheckPhaseGate(result, config)) {
            Trace::Instance().SetLatencySummary(FormatLatencySummary(result));
        }
    }
    Trace::Instance().ClearLatencyTicks();
}

}  // namespace datasystem
