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

/** Description: Defines optional slow-phase recording for transport Get. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_PHASE_LATENCY_RECORDER_H
#define DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_PHASE_LATENCY_RECORDER_H

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

/** @brief Selects the client slow-log threshold used by a transport phase. */
enum class TransportLatencyThreshold : uint8_t { PROCESS, RPC };

/** @brief Records selected transport phases and emits one slow summary. */
class TransportPhaseLatencyRecorder {
public:
    /** @brief Monotonic clock point used to measure one phase. */
    using TimePoint = std::chrono::steady_clock::time_point;

    /**
     * @brief Construct a transport Get phase recorder.
     * @param[in] endpoint Target endpoint.
     */
    explicit TransportPhaseLatencyRecorder(const HostPort &endpoint);

    /** @brief Emit one summary when any recorded phase exceeds its threshold. */
    ~TransportPhaseLatencyRecorder();

    /** @brief Return a phase start point. */
    TimePoint StartPhase() const;

    /**
     * @brief Calculate a phase duration.
     * @param[in] begin Phase start point.
     * @return Elapsed time in microseconds.
     */
    uint64_t ElapsedUs(const TimePoint &begin) const;

    /**
     * @brief Record one phase duration from its start point.
     * @param[in] name Static phase name valid for the recorder lifetime.
     * @param[in] begin Phase start point.
     * @param[in] threshold Threshold category used by this phase.
     */
    void RecordPhase(const char *name, const TimePoint &begin, TransportLatencyThreshold threshold);

    /**
     * @brief Record one measured phase duration.
     * @param[in] name Static phase name valid for the recorder lifetime.
     * @param[in] elapsedUs Phase duration in microseconds.
     * @param[in] threshold Threshold category used by this phase.
     */
    void RecordPhase(const char *name, uint64_t elapsedUs, TransportLatencyThreshold threshold);

    TransportPhaseLatencyRecorder(const TransportPhaseLatencyRecorder &) = delete;
    TransportPhaseLatencyRecorder &operator=(const TransportPhaseLatencyRecorder &) = delete;
    TransportPhaseLatencyRecorder(TransportPhaseLatencyRecorder &&) = delete;
    TransportPhaseLatencyRecorder &operator=(TransportPhaseLatencyRecorder &&) = delete;

private:
    struct Phase {
        const char *name;
        uint64_t elapsedUs;
        TransportLatencyThreshold threshold;
    };

    bool HasSlowPhase(uint64_t processThresholdUs, uint64_t rpcThresholdUs) const;
    uint64_t ThresholdUs(const Phase &phase, uint64_t processThresholdUs, uint64_t rpcThresholdUs) const;
    std::string FormatSlowPhases(uint64_t processThresholdUs, uint64_t rpcThresholdUs) const;
    std::string FormatPhases() const;

    static constexpr size_t MAX_PHASE_COUNT = 16;
    HostPort endpoint_;
    std::array<Phase, MAX_PHASE_COUNT> phases_;
    size_t phaseCount_ = 0;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_PHASE_LATENCY_RECORDER_H
