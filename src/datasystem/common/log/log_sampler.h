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

/**
 * Description: LogSampler core component for unified random log sampling.
 */

#ifndef DATASYSTEM_COMMON_LOG_LOG_SAMPLER_H
#define DATASYSTEM_COMMON_LOG_LOG_SAMPLER_H

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

#include "datasystem/common/log/spdlog/log_param.h"

namespace datasystem {

constexpr uint32_t kSamplePpmBase = 1000000;
constexpr uint64_t kAlwaysSampleThreshold = UINT64_MAX;
constexpr uint64_t kSaltHighShift = 32;
constexpr double kAccessDeriveMultiplier = 3.0;
constexpr double kDiagnosticDeriveMultiplier = 4.0;
constexpr int kMix64Shift1 = 30;
constexpr int kMix64Shift2 = 27;
constexpr int kMix64Shift3 = 31;

enum class AccessRecorderKey : size_t;
enum class AccessKeyType : int;

class LogSampleConfigPb;

enum class LogSampleKind { BYPASS, REQUEST, DIAGNOSTIC, ACCESS };

// Precomputed sample rate with ppm and threshold
struct SampleRate {
    uint32_t ppm = kSamplePpmBase;
    uint64_t threshold = kAlwaysSampleThreshold;
};

// User configuration input (before derivation)
struct LogSampleUserConfig {
    bool requestSampleRateExplicit = false;
    bool accessSampleRateExplicit = false;
    bool diagnosticSampleRateExplicit = false;
    double requestSampleRate = 1.0;
    double accessSampleRate = 1.0;
    double diagnosticSampleRate = 1.0;
};

// Effective configuration (after derivation and normalization)
struct LogSampleConfig {
    bool enabled = false;
    SampleRate requestRate;
    SampleRate accessRate;
    SampleRate diagnosticRate;
};

// Immutable snapshot for hot path
struct LogSamplerSnapshot {
    LogSampleConfig config;
};

// Persistent explicit state (sticky across updates)
struct LogSamplerPersistentExplicitState {
    bool accessSampleRateEverExplicit = false;
    bool diagnosticSampleRateEverExplicit = false;
};

class LogSampler {
public:
    static LogSampler &Instance();

    // --- Configuration path (non-hot path) ---
    bool UpdateConfigFromFlags(const LogSampleUserConfig &userConfig);
    enum class ConfigUpdateResult { CHANGED, UNCHANGED, INVALID };
    ConfigUpdateResult UpdateConfigFromProto(const LogSampleConfigPb &proto);
    void PopulateConfigProto(LogSampleConfigPb *proto);
    void Init();
    void SetSaltForTest(uint64_t salt);
    void ResetForTest();
    LogSamplerSnapshot *GetSnapshotForTest() const;
    void Shutdown();

    // --- Hot path entrypoints ---
    inline bool IsSamplerEnabledFast() const
    {
        return samplerEnabled_.load(std::memory_order_relaxed);
    }
    bool ShouldCreateRuntimeLog(LogSeverity severity, bool isPlog);
    bool IsCurrentRequestSampledIn();
    bool IsCurrentRequestSampledIn(const SampleRate &requestRate);
    bool ShouldSampleEvent(uint64_t traceHash, LogSampleKind kind, const SampleRate &rate);
    bool ShouldRecordAccess(AccessRecorderKey key);
    bool ShouldRecordAccessType(AccessKeyType type);

    LogSampler(const LogSampler &) = delete;
    LogSampler &operator=(const LogSampler &) = delete;
    ~LogSampler();

private:
    LogSampler();

    LogSampleKind ClassifyRuntime(LogSeverity severity, bool isPlog) const;
    const SampleRate &GetRate(const LogSampleConfig &config, LogSampleKind kind) const;
    bool BuildAndPublishSnapshot(const LogSampleUserConfig &userConfig);

    std::atomic<LogSamplerSnapshot *> snapshot_{ nullptr };
    std::atomic<bool> samplerEnabled_{ false };
    std::vector<LogSamplerSnapshot *> oldSnapshots_;
    std::mutex snapshotsMu_;
    LogSamplerPersistentExplicitState persistentExplicit_;
    std::atomic<uint64_t> sampleSalt_{ 0 };
};

// Utility functions
uint64_t BuildThreshold(uint32_t ppm);
uint64_t RateToPpm(double rate);

// Internal hash function
inline uint64_t Mix64(uint64_t z)
{
    z = (z ^ (z >> kMix64Shift1)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> kMix64Shift2)) * 0x94d049bb133111ebULL;
    z = z ^ (z >> kMix64Shift3);
    return z;
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LOG_SAMPLER_H