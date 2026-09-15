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

#include <cmath>

#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/access_recorder.h"

namespace datasystem {

namespace {
inline bool ShouldPassRandom(const SampleRate &rate, uint64_t key, uint64_t sampleSalt)
{
    if (rate.ppm == kSamplePpmBase) {
        return true;
    }
    if (rate.ppm == 0) {
        return false;
    }
    return Mix64(key ^ sampleSalt) <= rate.threshold;
}

bool IsValidRate(double rate)
{
    return std::isfinite(rate) && rate >= 0.0 && rate <= 1.0;
}
}  // namespace

uint64_t BuildThreshold(uint32_t ppm)
{
    if (ppm == 0) {
        return 0;
    }
    if (ppm == kSamplePpmBase) {
        return kAlwaysSampleThreshold;
    }
    return static_cast<uint64_t>(
        (static_cast<unsigned __int128>(ppm) * UINT64_MAX) / kSamplePpmBase);
}

uint64_t RateToPpm(double rate)
{
    if (rate <= 0.0) {
        return 0;
    }
    if (rate >= 1.0) {
        return kSamplePpmBase;
    }
    return static_cast<uint32_t>(std::round(rate * kSamplePpmBase));
}

LogSampler &LogSampler::Instance()
{
    static LogSampler *instance = new LogSampler();
    return *instance;
}

LogSampler::LogSampler() = default;

LogSampler::~LogSampler() = default;

void LogSampler::Shutdown()
{
    auto *current = snapshot_.exchange(nullptr, std::memory_order_acq_rel);
    delete current;
    {
        std::lock_guard<std::mutex> lk(snapshotsMu_);
        for (auto *snap : oldSnapshots_) {
            delete snap;
        }
        oldSnapshots_.clear();
    }
    samplerEnabled_.store(false, std::memory_order_release);
}

void LogSampler::SetSaltForTest(uint64_t salt)
{
    sampleSalt_.store(salt);
}

void LogSampler::ResetForTest()
{
    {
        std::lock_guard<std::mutex> lk(snapshotsMu_);
        for (auto *snap : oldSnapshots_) {
            delete snap;
        }
        oldSnapshots_.clear();
    }
    auto *current = snapshot_.exchange(nullptr, std::memory_order_acq_rel);
    delete current;
    samplerEnabled_.store(false, std::memory_order_release);
    sampleSalt_.store(0);
}

LogSamplerSnapshot *LogSampler::GetSnapshotForTest() const
{
    return snapshot_.load(std::memory_order_acquire);
}

bool LogSampler::UpdateConfigFromFlags(const LogSampleUserConfig &userConfig)
{
    if (!IsValidRate(userConfig.requestSampleRate) || !IsValidRate(userConfig.accessSampleRate)
        || !IsValidRate(userConfig.diagnosticSampleRate)) {
        return false;
    }
    bool enabled = BuildAndPublishSnapshot(userConfig);
    samplerEnabled_.store(enabled, std::memory_order_release);
    return true;
}

bool LogSampler::BuildAndPublishSnapshot(const LogSampleUserConfig &userConfig)
{
    bool enabled;
    {
        std::lock_guard<std::mutex> lk(snapshotsMu_);
        enabled = (std::abs(userConfig.requestSampleRate - 1.0) > 1e-12
                   || std::abs(userConfig.accessSampleRate - 1.0) > 1e-12
                   || std::abs(userConfig.diagnosticSampleRate - 1.0) > 1e-12);

        uint32_t requestPpm = RateToPpm(userConfig.requestSampleRate);
        uint32_t accessPpm = RateToPpm(userConfig.accessSampleRate);
        uint32_t diagnosticPpm = RateToPpm(userConfig.diagnosticSampleRate);

        auto *current = snapshot_.load(std::memory_order_acquire);
        if (current != nullptr && current->config.enabled
            && current->config.requestRate.ppm == requestPpm
            && current->config.accessRate.ppm == accessPpm
            && current->config.diagnosticRate.ppm == diagnosticPpm) {
            return enabled;
        }

        SampleRate requestRate{ requestPpm, BuildThreshold(requestPpm) };
        SampleRate accessRate{ accessPpm, BuildThreshold(accessPpm) };
        SampleRate diagnosticRate{ diagnosticPpm, BuildThreshold(diagnosticPpm) };

        LogSamplerSnapshot *newSnap = new LogSamplerSnapshot();
        newSnap->config.enabled = enabled;
        newSnap->config.requestRate = requestRate;
        newSnap->config.accessRate = accessRate;
        newSnap->config.diagnosticRate = diagnosticRate;

        LogSamplerSnapshot *oldSnap = snapshot_.exchange(newSnap, std::memory_order_acq_rel);
        if (oldSnap != nullptr) {
            oldSnapshots_.push_back(oldSnap);
        }
    }
    return enabled;
}

bool LogSampler::IsCurrentRequestSampledIn()
{
    auto *snap = snapshot_.load(std::memory_order_acquire);
    if (snap == nullptr || !snap->config.enabled) {
        return true;
    }
    return IsCurrentRequestSampledIn(snap->config.requestRate);
}

bool LogSampler::ShouldCreateRuntimeLog(LogSeverity severity, bool isPlog)
{
    auto *snap = snapshot_.load(std::memory_order_acquire);
    if (snap == nullptr || !snap->config.enabled) {
        return true;
    }
    if (severity == LogSeverity::FATAL) {
        return true;
    }

    auto kind = ClassifyRuntime(severity, isPlog);
    if (kind == LogSampleKind::BYPASS) {
        return true;
    }
    if (kind == LogSampleKind::REQUEST) {
        if (snap->config.requestRate.ppm == 0) {
            auto &trace = Trace::Instance();
            // ClassifyRuntime returned REQUEST → IsRequestLogTrace() guaranteed true
            trace.SetRequestSampleDecision(true, false);
            return false;
        }
        return IsCurrentRequestSampledIn(snap->config.requestRate);
    }

    // DIAGNOSTIC: independent per-trace threshold on the shared trace hash. A diagnostic
    // rate >= request_rate keeps diagnostics for all sampled-in traces by nesting.
    const SampleRate &rate = snap->config.diagnosticRate;
    if (rate.ppm == kSamplePpmBase) {
        return true;
    }
    if (rate.ppm == 0) {
        return false;
    }
    return ShouldPassRandom(rate, Trace::Instance().GetCachedHash(),
                            sampleSalt_.load(std::memory_order_relaxed));
}

bool LogSampler::IsCurrentRequestSampledIn(const SampleRate &requestRate)
{
    auto &trace = Trace::Instance();
    if (!trace.IsRequestLogTrace()) {
        return false;
    }

    bool admitted = false;
    if (trace.GetRequestSampleDecision(admitted)) {
        return admitted;
    }

    // No decision yet
    if (requestRate.ppm == kSamplePpmBase) {
        return true;
    }
    if (requestRate.ppm == 0) {
        // Do NOT create reject (hard per design 533)
        return false;
    }

    // ppm ∈ (0, kSamplePpmBase): GetOrCreate decision
    uint64_t traceHash = trace.GetCachedHash();
    bool result = ShouldPassRandom(requestRate, traceHash, sampleSalt_.load(std::memory_order_relaxed));
    trace.SetRequestSampleDecision(true, result);
    return result;
}

LogSampleKind LogSampler::ClassifyRuntime(LogSeverity severity, bool isPlog) const
{
    if (severity == LogSeverity::FATAL) {
        return LogSampleKind::BYPASS;
    }
    if (IsOutsideRequestTrace()) {
        return LogSampleKind::BYPASS;
    }
    if (isPlog || severity == LogSeverity::ERROR || severity == LogSeverity::WARNING) {
        return LogSampleKind::DIAGNOSTIC;
    }
    return LogSampleKind::REQUEST;
}

bool LogSampler::ShouldRecordAccess(AccessRecorderKey key)
{
    auto *snap = snapshot_.load(std::memory_order_acquire);
    if (snap == nullptr || !snap->config.enabled) {
        return true;
    }
    if (snap->config.accessRate.ppm == kSamplePpmBase) {
        return true;
    }

    AccessKeyType type = GetAccessKeyType(key);
    if (type == AccessKeyType::REQUEST_OUT) {
        return true;
    }

    if (IsOutsideRequestTrace()) {
        return true;
    }

    if (snap->config.accessRate.ppm == 0) {
        return false;
    }
    return ShouldPassRandom(snap->config.accessRate, Trace::Instance().GetCachedHash(),
                            sampleSalt_.load(std::memory_order_relaxed));
}

bool LogSampler::ShouldRecordAccessType(AccessKeyType type)
{
    auto *snap = snapshot_.load(std::memory_order_acquire);
    if (snap == nullptr || !snap->config.enabled) {
        return true;
    }
    if (type == AccessKeyType::REQUEST_OUT) {
        return true;
    }
    const auto &accessRate = snap->config.accessRate;
    if (accessRate.ppm == kSamplePpmBase) {
        return true;
    }

    if (IsOutsideRequestTrace()) {
        return true;
    }

    if (accessRate.ppm == 0) {
        return false;
    }
    return ShouldPassRandom(accessRate, Trace::Instance().GetCachedHash(),
                            sampleSalt_.load(std::memory_order_relaxed));
}

}  // namespace datasystem
