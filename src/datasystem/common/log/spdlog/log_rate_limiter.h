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
 * Description: Request-level log sampler by trace ID.
 */
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_LOG_RATE_LIMITER_H
#define DATASYSTEM_COMMON_LOG_SPDLOG_LOG_RATE_LIMITER_H

#include <atomic>
#include <cstdint>

#include <spdlog/common.h>

namespace datasystem {

class LogRateLimiter {
public:
    static LogRateLimiter &Instance();

    /**
     * @brief Determine whether the current log should be output.
     * @param level spdlog log level.
     * @return true to allow, false to drop.
     *
     * Rules:
     * - This overload is treated as non-request log and always returns true.
     */
    bool ShouldLog(ds_spdlog::level::level_enum level);

    /**
     * @brief Trace-aware version of ShouldLog.
     * @param level spdlog log level.
     * @param traceHash FNV-1a hash of the current request trace ID (0 = non-request log).
     * @return true to allow, false to drop.
     *
     * Rules:
     * - ERROR(level>=4) and FATAL(level=5) always return true.
     * - When rate_ == 0, no request sampling, always returns true.
     * - When traceHash == 0 (non-request log), always returns true.
     * - For request logs (traceHash != 0), sampling is request-level:
     *   the first decision is stored in current Trace context, and later logs
     *   in the same propagated request context follow the same decision.
     */
    bool ShouldLog(ds_spdlog::level::level_enum level, uint64_t traceHash);

    /**
     * @brief Update per-second request sampling limit. 0 = unlimited.
     */
    void SetRate(int32_t ratePerSecond);

    /**
     * @brief Get or create request sampling decision for a trace.
     * @param[in] traceHash FNV-1a hash of trace ID.
     * @param[out] admitted Whether trace is admitted when return is true.
     * @return true if decision is valid (sampling enabled and traceHash != 0), else false.
     */
    bool GetOrCreateRequestDecision(uint64_t traceHash, bool &admitted);

    bool IsEnabled() const
    {
        return rate_.load(std::memory_order_relaxed) > 0;
    }

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

    int64_t NowMs() const;
    void RefreshRequestWindow(int64_t nowMs);
    bool TryAdmitInCurrentSecond(int64_t nowMs);

    std::atomic<int32_t> rate_{ 0 };          // 0 = no rate limiting
    std::atomic<int64_t> windowSec_{ 0 };
    std::atomic<int32_t> admittedInWindow_{ 0 };
};

}  // namespace datasystem
#endif
