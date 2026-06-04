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

#include "datasystem/common/log/log_sampler.h"

#include "datasystem/protos/share_memory.pb.h"

namespace datasystem {

LogSampler::ConfigUpdateResult LogSampler::UpdateConfigFromProto(const LogSampleConfigPb &proto)
{
    auto *current = snapshot_.load(std::memory_order_acquire);
    if (!proto.enabled()) {
        if (current == nullptr) {
            samplerEnabled_.store(false, std::memory_order_release);
            return ConfigUpdateResult::UNCHANGED;
        }
        if (!current->config.enabled) {
            return ConfigUpdateResult::UNCHANGED;
        }
        LogSamplerSnapshot *newSnap = new LogSamplerSnapshot();
        newSnap->config.enabled = false;
        LogSamplerSnapshot *oldSnap = snapshot_.exchange(newSnap, std::memory_order_acq_rel);
        if (oldSnap != nullptr) {
            std::lock_guard<std::mutex> lk(snapshotsMu_);
            oldSnapshots_.push_back(oldSnap);
        }
        samplerEnabled_.store(false, std::memory_order_release);
        return ConfigUpdateResult::CHANGED;
    }

    uint32_t reqPpm = proto.request_sample_ppm();
    uint32_t accPpm = proto.access_sample_ppm();
    uint32_t diagPpm = proto.diagnostic_sample_ppm();
    if (reqPpm > kSamplePpmBase || accPpm > kSamplePpmBase || diagPpm > kSamplePpmBase) {
        return ConfigUpdateResult::INVALID;
    }

    if (current != nullptr && current->config.enabled && current->config.requestRate.ppm == reqPpm
        && current->config.accessRate.ppm == accPpm && current->config.diagnosticRate.ppm == diagPpm) {
        return ConfigUpdateResult::UNCHANGED;
    }

    SampleRate requestRate;
    requestRate.ppm = reqPpm;
    requestRate.threshold = BuildThreshold(reqPpm);
    SampleRate accessRate;
    accessRate.ppm = accPpm;
    accessRate.threshold = BuildThreshold(accPpm);
    SampleRate diagnosticRate;
    diagnosticRate.ppm = diagPpm;
    diagnosticRate.threshold = BuildThreshold(diagPpm);

    bool configEnabled = (reqPpm != kSamplePpmBase || accPpm != kSamplePpmBase || diagPpm != kSamplePpmBase);

    LogSamplerSnapshot *newSnap = new LogSamplerSnapshot();
    newSnap->config.enabled = configEnabled;
    newSnap->config.requestRate = requestRate;
    newSnap->config.accessRate = accessRate;
    newSnap->config.diagnosticRate = diagnosticRate;

    LogSamplerSnapshot *oldSnap = snapshot_.exchange(newSnap, std::memory_order_acq_rel);
    if (oldSnap != nullptr) {
        std::lock_guard<std::mutex> lk(snapshotsMu_);
        oldSnapshots_.push_back(oldSnap);
    }
    samplerEnabled_.store(configEnabled, std::memory_order_release);
    return ConfigUpdateResult::CHANGED;
}

void LogSampler::PopulateConfigProto(LogSampleConfigPb *proto)
{
    auto *snap = snapshot_.load(std::memory_order_acquire);
    if (snap == nullptr || !snap->config.enabled) {
        proto->set_enabled(false);
        return;
    }
    proto->set_enabled(true);
    proto->set_request_sample_ppm(snap->config.requestRate.ppm);
    proto->set_access_sample_ppm(snap->config.accessRate.ppm);
    proto->set_diagnostic_sample_ppm(snap->config.diagnosticRate.ppm);
}

}  // namespace datasystem
