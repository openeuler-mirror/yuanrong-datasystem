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

#ifndef DATASYSTEM_COMMON_LOG_LATENCY_PHASE_TYPES_H
#define DATASYSTEM_COMMON_LOG_LATENCY_PHASE_TYPES_H

#include <cstdint>

namespace datasystem {

enum class LatencySummaryPhase : uint32_t {
    CLIENT_PROCESS_GET = 1,
    CLIENT_RPC_GET = 2,

    CLIENT_PROCESS_SET = 3,
    CLIENT_RPC_CREATE = 4,
    CLIENT_PROCESS_MEMORY_COPY = 5,
    CLIENT_RPC_PUBLISH = 6,
    CLIENT_URMA_UB_TRANSFER = 7,

    CLIENT_PROCESS_CREATE = 8,

    CLIENT_PROCESS_EXIST = 9,
    CLIENT_RPC_EXIST = 10,

    WORKER_PROCESS_GET = 11,
    WORKER_RPC_QUERY_META = 12,
    WORKER_RPC_REMOTE_GET = 13,
    WORKER_URMA_URMA_TOTAL = 14,
    WORKER_PROCESS_L2CACHE_READ = 15,

    WORKER_PROCESS_CREATE = 16,

    WORKER_PROCESS_PUBLISH = 17,
    WORKER_RPC_CREATE_META = 18,
    WORKER_RPC_UPDATE_META = 19,

    WORKER_PROCESS_EXIST = 20,

    MASTER_PROCESS_QUERY_META = 21,
    MASTER_PROCESS_CREATE_META = 22,
    MASTER_PROCESS_UPDATE_META = 23,

    WORKER_PROCESS_REMOTE_GET = 24,
};

constexpr uint16_t LATENCY_SUMMARY_PHASE_MAX = 24;

constexpr uint64_t NS_PER_US = 1000UL;
constexpr size_t PHASE_PAIR_SIZE = 2;
constexpr uint16_t MIN_TICK_COUNT_FOR_SUMMARY = 2;

struct LatencyTraceConfig {
    uint64_t processSlowerThanUs = 0;
    uint64_t rpcSlowerThanUs = 0;
    bool LatencyTraceEnabled() const
    {
        return processSlowerThanUs > 0 || rpcSlowerThanUs > 0;
    }
};

struct PhaseDuration {
    LatencySummaryPhase phase = LatencySummaryPhase::CLIENT_PROCESS_GET;
    uint64_t durationUs = 0;
};

struct PhaseDurationResult {
    PhaseDuration entries[LATENCY_SUMMARY_PHASE_MAX] = {};
    uint16_t count = 0;
    uint16_t tickDroppedCount = 0;
    uint16_t phaseDroppedCount = 0;

    uint16_t droppedCount() const
    {
        return tickDroppedCount + phaseDroppedCount;
    }

    void Add(LatencySummaryPhase phase, uint64_t durationUs);
    const PhaseDuration *Find(LatencySummaryPhase phase) const;
};

constexpr uint16_t DOWNSTREAM_PHASE_MAX_NUM = 12;

struct DownstreamPhaseResult {
    PhaseDuration entries[DOWNSTREAM_PHASE_MAX_NUM] = {};
    uint16_t count = 0;
    uint16_t tickDroppedCount = 0;
    uint16_t phaseDroppedCount = 0;

    uint16_t droppedCount() const
    {
        return tickDroppedCount + phaseDroppedCount;
    }

    void Add(LatencySummaryPhase phase, uint64_t durationUs);
    void Clear()
    {
        count = 0;
        tickDroppedCount = 0;
        phaseDroppedCount = 0;
    }
};

bool IsProcessPhase(LatencySummaryPhase phase);
bool IsRpcPhase(LatencySummaryPhase phase);
const char *LatencySummaryPhaseName(LatencySummaryPhase phase);

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LATENCY_PHASE_TYPES_H
