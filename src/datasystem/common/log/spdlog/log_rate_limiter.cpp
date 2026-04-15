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
 * Description: Log rate limiter implementation using token bucket + uniform-interval sampling.
 */

#include "datasystem/common/log/spdlog/log_rate_limiter.h"

#include <algorithm>
#include <chrono>

namespace datasystem {

LogRateLimiter &LogRateLimiter::Instance()
{
    static LogRateLimiter instance;
    return instance;
}

void LogRateLimiter::Refill()
{
    auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
    auto lastMs = lastRefillMs_.load(std::memory_order_relaxed);

    // First call or after Reset(): initialize timestamp without adding tokens
    if (lastMs == 0) {
        lastRefillMs_.compare_exchange_strong(lastMs, nowMs, std::memory_order_relaxed);
        return;
    }

    auto elapsed = nowMs - lastMs;
    if (elapsed <= 0) {
        return;
    }

    // CAS to update timestamp; only one thread performs the refill
    if (!lastRefillMs_.compare_exchange_strong(lastMs, nowMs, std::memory_order_relaxed)) {
        return;
    }

    int32_t r = rate_.load(std::memory_order_relaxed);
    if (r <= 0) {
        return;
    }

    // Refill tokens proportional to elapsed time, capped at rate_ (1 second worth)
    int64_t newTokens = elapsed * static_cast<int64_t>(r) / 1000;
    if (newTokens <= 0) {
        return;
    }

    // CAS loop to safely add tokens without losing concurrent updates
    int64_t current = tokens_.load(std::memory_order_relaxed);
    int64_t desired;
    do {
        desired = std::min(current + newTokens, static_cast<int64_t>(r));
        if (desired <= current) {
            break;  // Already at or above cap
        }
    } while (!tokens_.compare_exchange_weak(current, desired, std::memory_order_relaxed));
}

bool LogRateLimiter::ShouldLog(ds_spdlog::level::level_enum level, bool *wasSampled)
{
    if (wasSampled) {
        *wasSampled = false;
    }

    int32_t r = rate_.load(std::memory_order_relaxed);

    // No rate limiting
    if (r <= 0) {
        return true;
    }

    // ERROR(level=4) and above (FATAL=5) always pass
    if (level >= ds_spdlog::level::err) {
        return true;
    }

    Refill();

    // Try to consume a token
    int64_t t = tokens_.load(std::memory_order_relaxed);
    while (t > 0) {
        if (tokens_.compare_exchange_weak(t, t - 1, std::memory_order_relaxed)) {
            totalLogged_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }

    // Tokens exhausted -> uniform-interval sampling: keep 1 out of every rate_ dropped logs.
    // Use max(rate, 2) as divisor to prevent rate=1 edge case where mod 1 == 0 always.
    int64_t divisor = std::max(static_cast<int64_t>(r), INT64_C(2));
    int64_t dropped = totalDropped_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (divisor > 0 && dropped % divisor == 0) {
        totalLogged_.fetch_add(1, std::memory_order_relaxed);
        if (wasSampled) {
            *wasSampled = true;
        }
        return true;
    }

    return false;
}

bool LogRateLimiter::WhitelistContains(uint64_t h) const
{
    uint64_t idx = h & (TRACE_WHITELIST_SIZE - 1);
    for (int i = 0; i < TRACE_WHITELIST_PROBE; ++i) {
        uint64_t slotHash = whitelist_[idx].hash.load(std::memory_order_relaxed);
        if (slotHash == h) {
            return true;
        }
        if (slotHash == 0) {
            return false;
        }
        idx = (idx + 1) & (TRACE_WHITELIST_SIZE - 1);
    }
    return false;
}

void LogRateLimiter::WhitelistAdd(uint64_t h)
{
    uint64_t startIdx = h & (TRACE_WHITELIST_SIZE - 1);
    for (int i = 0; i < TRACE_WHITELIST_PROBE; ++i) {
        uint64_t idx = (startIdx + i) & (TRACE_WHITELIST_SIZE - 1);
        uint64_t expected = 0;
        if (whitelist_[idx].hash.compare_exchange_strong(expected, h, std::memory_order_relaxed)) {
            return;
        }
        if (expected == h) {
            return;
        }
    }
    // Full → evict last probed position
    uint64_t evictIdx = (startIdx + TRACE_WHITELIST_PROBE - 1) & (TRACE_WHITELIST_SIZE - 1);
    whitelist_[evictIdx].hash.store(h, std::memory_order_relaxed);
    whitelist_[evictIdx].miniTokens.store(0, std::memory_order_relaxed);
    whitelist_[evictIdx].lastMiniRefillMs.store(0, std::memory_order_relaxed);
}

void LogRateLimiter::MiniBucketRefill(TraceSlot &slot)
{
    auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
    auto lastMs = slot.lastMiniRefillMs.load(std::memory_order_relaxed);
    if (lastMs == 0) {
        slot.lastMiniRefillMs.compare_exchange_strong(lastMs, nowMs, std::memory_order_relaxed);
        return;
    }
    auto elapsed = nowMs - lastMs;
    if (elapsed <= 0) {
        return;
    }
    if (!slot.lastMiniRefillMs.compare_exchange_strong(lastMs, nowMs, std::memory_order_relaxed)) {
        return;
    }
    int32_t r = rate_.load(std::memory_order_relaxed);
    int32_t miniRate = std::min(r, TRACE_MINI_BUCKET_RATE);
    if (miniRate <= 0) {
        return;
    }
    int64_t newTokens = elapsed * static_cast<int64_t>(miniRate) / 1000;
    if (newTokens <= 0) {
        return;
    }
    int64_t current = slot.miniTokens.load(std::memory_order_relaxed);
    int64_t desired;
    do {
        desired = std::min(current + newTokens, static_cast<int64_t>(miniRate));
        if (desired <= current) {
            break;
        }
    } while (!slot.miniTokens.compare_exchange_weak(current, desired, std::memory_order_relaxed));
}

bool LogRateLimiter::MiniBucketConsume(TraceSlot &slot)
{
    MiniBucketRefill(slot);
    int64_t t = slot.miniTokens.load(std::memory_order_relaxed);
    while (t > 0) {
        if (slot.miniTokens.compare_exchange_weak(t, t - 1, std::memory_order_relaxed)) {
            totalLogged_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }
    return false;
}

int LogRateLimiter::FindWhitelistSlot(uint64_t traceHash) const
{
    uint64_t startIdx = traceHash & (TRACE_WHITELIST_SIZE - 1);
    for (int i = 0; i < TRACE_WHITELIST_PROBE; ++i) {
        uint64_t idx = (startIdx + i) & (TRACE_WHITELIST_SIZE - 1);
        uint64_t slotHash = whitelist_[idx].hash.load(std::memory_order_relaxed);
        if (slotHash == traceHash) {
            return static_cast<int>(idx);
        }
        if (slotHash == 0) {
            break;
        }
    }
    return -1;
}

bool LogRateLimiter::SamplingFallback(int32_t rate, bool *wasSampled)
{
    int64_t divisor = std::max(static_cast<int64_t>(rate), INT64_C(2));
    int64_t dropped = totalDropped_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (divisor > 0 && dropped % divisor == 0) {
        totalLogged_.fetch_add(1, std::memory_order_relaxed);
        if (wasSampled) {
            *wasSampled = true;
        }
        return true;
    }
    return false;
}

bool LogRateLimiter::ShouldLog(ds_spdlog::level::level_enum level, uint64_t traceHash, bool *wasSampled)
{
    if (wasSampled) {
        *wasSampled = false;
    }
    int32_t r = rate_.load(std::memory_order_relaxed);
    if (r <= 0 || level >= ds_spdlog::level::err) {
        return true;
    }
    if (traceHash == 0) {
        return ShouldLog(level, wasSampled);
    }

    // Whitelist hit → mini-bucket → sampling fallback
    int slotIdx = FindWhitelistSlot(traceHash);
    if (slotIdx >= 0) {
        return MiniBucketConsume(whitelist_[slotIdx]) || SamplingFallback(r, wasSampled);
    }

    // Not in whitelist → global token bucket
    Refill();
    int64_t t = tokens_.load(std::memory_order_relaxed);
    while (t > 0) {
        if (tokens_.compare_exchange_weak(t, t - 1, std::memory_order_relaxed)) {
            totalLogged_.fetch_add(1, std::memory_order_relaxed);
            WhitelistAdd(traceHash);
            return true;
        }
    }

    // Global tokens exhausted → sampling fallback
    WhitelistAdd(traceHash);
    return SamplingFallback(r, wasSampled);
}

void LogRateLimiter::SetRate(int32_t ratePerSecond)
{
    // Clamp negative values to 0 (no limit)
    rate_.store(std::max(ratePerSecond, static_cast<int32_t>(0)), std::memory_order_relaxed);
}

int64_t LogRateLimiter::GetSamplingRate() const
{
    int32_t r = rate_.load(std::memory_order_relaxed);
    if (r <= 0) {
        return 1;
    }

    int64_t logged = totalLogged_.load(std::memory_order_relaxed);
    int64_t dropped = totalDropped_.load(std::memory_order_relaxed);
    int64_t total = logged + dropped;
    if (total == 0 || logged == 0) {
        return 1;
    }

    return total / logged;
}

void LogRateLimiter::Reset()
{
    tokens_.store(0, std::memory_order_relaxed);
    auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
    lastRefillMs_.store(nowMs, std::memory_order_relaxed);
    rate_.store(0, std::memory_order_relaxed);
    totalLogged_.store(0, std::memory_order_relaxed);
    totalDropped_.store(0, std::memory_order_relaxed);
    for (int i = 0; i < TRACE_WHITELIST_SIZE; ++i) {
        whitelist_[i].hash.store(0, std::memory_order_relaxed);
        whitelist_[i].miniTokens.store(0, std::memory_order_relaxed);
        whitelist_[i].lastMiniRefillMs.store(0, std::memory_order_relaxed);
    }
}

}  // namespace datasystem
