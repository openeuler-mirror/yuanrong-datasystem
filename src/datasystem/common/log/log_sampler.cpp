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
#include <random>
#include <unistd.h>
#include <chrono>

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

uint64_t GenerateSampleSalt()
{
    std::random_device rd;
    uint64_t salt = (static_cast<uint64_t>(rd()) << kSaltHighShift) | static_cast<uint64_t>(rd());
    if (salt == 0) {
        // Fallback: PID + steady_clock hash
        salt = static_cast<uint64_t>(getpid()) |
             (static_cast<uint64_t>(
                   std::chrono::steady_clock::now().time_since_epoch().count()) << kSaltHighShift);
        salt = Mix64(salt);
    }
    return salt;
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

void LogSampler::Init()
{
    sampleSalt_.store(GenerateSampleSalt());
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
    persistentExplicit_ = LogSamplerPersistentExplicitState{};
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
        bool canDeriveFromRequest = userConfig.requestSampleRateExplicit
            && !userConfig.accessSampleRateExplicit
            && !userConfig.diagnosticSampleRateExplicit
            && !persistentExplicit_.accessSampleRateEverExplicit
            && !persistentExplicit_.diagnosticSampleRateEverExplicit;

        double effectiveAccess;
        double effectiveDiagnostic;
        if (canDeriveFromRequest) {
            effectiveAccess = std::min(1.0, userConfig.requestSampleRate * kAccessDeriveMultiplier);
            effectiveDiagnostic = std::min(1.0, userConfig.requestSampleRate * kDiagnosticDeriveMultiplier);
        } else {
            effectiveAccess = userConfig.accessSampleRateExplicit ? userConfig.accessSampleRate : 1.0;
            effectiveDiagnostic = userConfig.diagnosticSampleRateExplicit ? userConfig.diagnosticSampleRate : 1.0;
        }

        if (userConfig.accessSampleRateExplicit) {
            persistentExplicit_.accessSampleRateEverExplicit = true;
        }
        if (userConfig.diagnosticSampleRateExplicit) {
            persistentExplicit_.diagnosticSampleRateEverExplicit = true;
        }

        enabled = (std::abs(userConfig.requestSampleRate - 1.0) > 1e-12
                   || std::abs(effectiveAccess - 1.0) > 1e-12
                   || std::abs(effectiveDiagnostic - 1.0) > 1e-12);

        uint32_t requestPpm = RateToPpm(userConfig.requestSampleRate);
        uint32_t accessPpm = RateToPpm(effectiveAccess);
        uint32_t diagnosticPpm = RateToPpm(effectiveDiagnostic);

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

    // DIAGNOSTIC
    const SampleRate &rate = GetRate(snap->config, kind);
    if (rate.ppm == kSamplePpmBase) {
        return true;
    }
    if (kind == LogSampleKind::DIAGNOSTIC && IsCurrentRequestSampledIn(snap->config.requestRate)) {
        return true;
    }
    if (rate.ppm == 0) {
        return false;
    }

    uint64_t traceHash = Trace::Instance().GetCachedHash();
    return ShouldSampleEvent(traceHash, kind, rate);
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

bool LogSampler::ShouldSampleEvent(uint64_t traceHash, LogSampleKind kind, const SampleRate &rate)
{
    static thread_local uint64_t tlSequence = 0;
    uint64_t key = traceHash ^ (static_cast<uint64_t>(kind) << 32) ^ (++tlSequence);
    return ShouldPassRandom(rate, key, sampleSalt_.load(std::memory_order_relaxed));
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

const SampleRate &LogSampler::GetRate(const LogSampleConfig &config, LogSampleKind kind) const
{
    switch (kind) {
        case LogSampleKind::REQUEST:
            return config.requestRate;
        case LogSampleKind::DIAGNOSTIC:
            return config.diagnosticRate;
        case LogSampleKind::ACCESS:
            return config.accessRate;
        default:
            return config.requestRate;
    }
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

    if (IsCurrentRequestSampledIn(snap->config.requestRate)) {
        return true;
    }
    if (snap->config.accessRate.ppm == 0) {
        return false;
    }
    uint64_t traceHash = Trace::Instance().GetCachedHash();
    return ShouldSampleEvent(traceHash, LogSampleKind::ACCESS, snap->config.accessRate);
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

    if (IsCurrentRequestSampledIn(snap->config.requestRate)) {
        return true;
    }
    if (accessRate.ppm == 0) {
        return false;
    }
    uint64_t traceHash = Trace::Instance().GetCachedHash();
    return ShouldSampleEvent(traceHash, LogSampleKind::ACCESS, accessRate);
}

}  // namespace datasystem
