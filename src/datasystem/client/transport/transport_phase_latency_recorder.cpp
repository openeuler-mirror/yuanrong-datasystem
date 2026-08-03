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

/** Description: Implements optional slow-phase recording for transport Get. */

#include "datasystem/client/transport/transport_phase_latency_recorder.h"

#include <sstream>

#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace client {
namespace {
constexpr char TRANSPORT_GET_LOG_PREFIX[] = "[TransportGet]";
constexpr char TRANSPORT_PHASE_LATENCY_EVENT[] = "Phase latency";
}  // namespace

TransportPhaseLatencyRecorder::TransportPhaseLatencyRecorder(const HostPort &endpoint) : endpoint_(endpoint)
{
}

TransportPhaseLatencyRecorder::~TransportPhaseLatencyRecorder()
{
    const auto config = GetClientLatencyTraceConfig();
    if (!HasSlowPhase(config.processSlowerThanUs, config.rpcSlowerThanUs)) {
        return;
    }
    SLOW_LOG(INFO) << TRANSPORT_GET_LOG_PREFIX << " " << TRANSPORT_PHASE_LATENCY_EVENT
                   << ", endpoint=" << endpoint_.ToString()
                   << ", slowPhases=" << FormatSlowPhases(config.processSlowerThanUs, config.rpcSlowerThanUs)
                   << ", phasesUs=" << FormatPhases();
}

TransportPhaseLatencyRecorder::TimePoint TransportPhaseLatencyRecorder::StartPhase() const
{
    return std::chrono::steady_clock::now();
}

uint64_t TransportPhaseLatencyRecorder::ElapsedUs(const TimePoint &begin) const
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin).count());
}

void TransportPhaseLatencyRecorder::RecordPhase(const char *name, const TimePoint &begin,
                                                TransportLatencyThreshold threshold)
{
    RecordPhase(name, ElapsedUs(begin), threshold);
}

void TransportPhaseLatencyRecorder::RecordPhase(const char *name, uint64_t elapsedUs,
                                                TransportLatencyThreshold threshold)
{
    if (phaseCount_ >= phases_.size()) {
        return;
    }
    phases_[phaseCount_++] = { name == nullptr ? "unknown" : name, elapsedUs, threshold };
}

uint64_t TransportPhaseLatencyRecorder::ThresholdUs(const Phase &phase, uint64_t processThresholdUs,
                                                    uint64_t rpcThresholdUs) const
{
    return phase.threshold == TransportLatencyThreshold::PROCESS ? processThresholdUs : rpcThresholdUs;
}

bool TransportPhaseLatencyRecorder::HasSlowPhase(uint64_t processThresholdUs, uint64_t rpcThresholdUs) const
{
    for (size_t i = 0; i < phaseCount_; ++i) {
        const uint64_t thresholdUs = ThresholdUs(phases_[i], processThresholdUs, rpcThresholdUs);
        if (thresholdUs > 0 && phases_[i].elapsedUs >= thresholdUs) {
            return true;
        }
    }
    return false;
}

std::string TransportPhaseLatencyRecorder::FormatSlowPhases(uint64_t processThresholdUs,
                                                            uint64_t rpcThresholdUs) const
{
    std::ostringstream stream;
    stream << "[";
    const char *separator = "";
    for (size_t i = 0; i < phaseCount_; ++i) {
        const uint64_t thresholdUs = ThresholdUs(phases_[i], processThresholdUs, rpcThresholdUs);
        if (thresholdUs > 0 && phases_[i].elapsedUs >= thresholdUs) {
            stream << separator << phases_[i].name;
            separator = ",";
        }
    }
    stream << "]";
    return stream.str();
}

std::string TransportPhaseLatencyRecorder::FormatPhases() const
{
    std::ostringstream stream;
    stream << "{";
    for (size_t i = 0; i < phaseCount_; ++i) {
        stream << (i == 0 ? "" : ",") << phases_[i].name << ":" << phases_[i].elapsedUs;
    }
    stream << "}";
    return stream.str();
}

}  // namespace client
}  // namespace datasystem
