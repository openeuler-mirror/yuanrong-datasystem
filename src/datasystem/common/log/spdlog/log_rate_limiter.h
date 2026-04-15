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
 * Description: Log rate limiter using token bucket + uniform-interval sampling.
 */
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_LOG_RATE_LIMITER_H
#define DATASYSTEM_COMMON_LOG_SPDLOG_LOG_RATE_LIMITER_H

#include <atomic>
#include <cstdint>

#include <spdlog/common.h>

namespace datasystem {

static constexpr int TRACE_WHITELIST_SIZE = 1024;    // 2^10, for bitwise modulo
static constexpr int TRACE_WHITELIST_PROBE = 4;      // Open addressing max probes
static constexpr int TRACE_MINI_BUCKET_RATE = 20;    // Per-trace mini-bucket cap (logs/sec)
static constexpr int CACHE_LINE_SIZE = 64;           // Cache line size for false-sharing avoidance

struct alignas(CACHE_LINE_SIZE) TraceSlot {
    std::atomic<uint64_t> hash{ 0 };           // FNV-1a hash of trace ID, 0 = empty
    std::atomic<int64_t> miniTokens{ 0 };      // Mini-bucket tokens
    std::atomic<int64_t> lastMiniRefillMs{ 0 }; // Mini-bucket last refill timestamp
};

class LogRateLimiter {
public:
    static LogRateLimiter &Instance();

    /**
     * @brief Determine whether the current log should be output.
     * @param level spdlog log level.
     * @return true to allow, false to drop.
     *
     * Rules:
     * - When rate_ == 0, no rate limiting, always returns true.
     * - ERROR(level=4) and FATAL(level=5) always return true.
     * - Other levels use token bucket + uniform-interval sampling.
     */
    bool ShouldLog(ds_spdlog::level::level_enum level, bool *wasSampled = nullptr);

    /**
     * @brief Trace-aware version of ShouldLog.
     * @param level spdlog log level.
     * @param traceHash FNV-1a hash of the current trace ID (0 = no trace).
     * @param wasSampled Output: true if log was kept through sampling fallback.
     * @return true to allow, false to drop.
     *
     * Rules (in addition to non-trace version):
     * - When traceHash != 0 and trace is in whitelist, consume from mini-bucket.
     * - Mini-bucket rate = min(rate_, 20) tokens/sec per trace.
     * - When mini-bucket exhausted, fall through to global sampling.
     */
    bool ShouldLog(ds_spdlog::level::level_enum level, uint64_t traceHash, bool *wasSampled = nullptr);

    /**
     * @brief Update the per-second log rate limit. 0 = unlimited.
     */
    void SetRate(int32_t ratePerSecond);

    /**
     * @brief Get the current sampling rate (1 out of N logs are kept).
     * @return 1 means full output, >1 means 1 out of N is kept.
     */
    int64_t GetSamplingRate() const;

    /**
     * @brief Reset all internal state. For testing only.
     */
    void Reset();

    LogRateLimiter(const LogRateLimiter &) = delete;
    LogRateLimiter &operator=(const LogRateLimiter &) = delete;
    LogRateLimiter(LogRateLimiter &&) = delete;
    LogRateLimiter &operator=(LogRateLimiter &&) = delete;

private:
    LogRateLimiter() = default;
    ~LogRateLimiter() = default;

    /**
     * @brief Refill tokens based on elapsed time.
     */
    void Refill();

    bool WhitelistContains(uint64_t h) const;

    void WhitelistAdd(uint64_t h);

    void MiniBucketRefill(TraceSlot &slot);

    bool MiniBucketConsume(TraceSlot &slot);

    /**
     * @brief Probe whitelist for traceHash. Returns slot index if found, -1 if not found.
     */
    int FindWhitelistSlot(uint64_t traceHash) const;

    /**
     * @brief Uniform-interval sampling fallback when tokens exhausted.
     * @return true if this log is the sampled survivor.
     */
    bool SamplingFallback(int32_t rate, bool *wasSampled);

    std::atomic<int64_t> tokens_{ 0 };
    std::atomic<int64_t> lastRefillMs_{ 0 };
    std::atomic<int32_t> rate_{ 0 };          // 0 = no rate limiting
    std::atomic<int64_t> totalLogged_{ 0 };   // Total allowed (including sampled)
    std::atomic<int64_t> totalDropped_{ 0 };  // Total dropped

    TraceSlot whitelist_[TRACE_WHITELIST_SIZE];
};

}  // namespace datasystem
#endif
