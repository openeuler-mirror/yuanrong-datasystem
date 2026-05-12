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
 * Description: Request-level log sampler implementation by trace ID.
 */

#include "datasystem/common/log/spdlog/log_rate_limiter.h"

#include <algorithm>
#include <chrono>
#include <mutex>

#include "datasystem/common/log/trace.h"

namespace datasystem {

LogRateLimiter &LogRateLimiter::Instance()
{
    static LogRateLimiter instance;
    return instance;
}

int64_t LogRateLimiter::NowMs() const
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool LogRateLimiter::ShouldLog(ds_spdlog::level::level_enum level)
{
    return ShouldLog(level, 0);
}

bool LogRateLimiter::ShouldLog(ds_spdlog::level::level_enum level, uint64_t traceHash)
{
    // ERROR and FATAL are always logged.
    if (level >= ds_spdlog::level::err) {
        return true;
    }

    // Non-request logs (no trace) are never sampled.
    if (traceHash == 0) {
        return true;
    }

    // RPC-propagated decision takes precedence across processes.
    bool propagatedAdmit = false;
    if (Trace::Instance().GetRequestSampleDecision(propagatedAdmit)) {
        return propagatedAdmit;
    }

    // No sampling configured.
    if (rate_.load(std::memory_order_relaxed) <= 0) {
        return true;
    }

    return ShouldAdmitRequest(traceHash);
}

bool LogRateLimiter::ShouldAdmitRequest(uint64_t traceHash)
{
    int64_t nowMs = NowMs();

    bool admitted = false;
    {
        std::lock_guard<std::mutex> lock(decisionTableMutex_);
        CleanupExpiredDecisions(nowMs);
        if (GetDecisionFromTable(traceHash, nowMs, admitted)) {
            Trace::Instance().SetRequestSampleDecision(true, admitted);
            return admitted;
        }

        admitted = TryAdmitInCurrentSecond(nowMs);
        UpsertDecision(traceHash, admitted, nowMs);
    }

    Trace::Instance().SetRequestSampleDecision(true, admitted);
    return admitted;
}

bool LogRateLimiter::GetOrCreateRequestDecision(uint64_t traceHash, bool &admitted)
{
    if (traceHash == 0) {
        return false;
    }

    if (Trace::Instance().GetRequestSampleDecision(admitted)) {
        return true;
    }

    if (rate_.load(std::memory_order_relaxed) <= 0) {
        return false;
    }

    admitted = ShouldAdmitRequest(traceHash);
    return true;
}

void LogRateLimiter::RefreshRequestWindow(int64_t nowMs)
{
    int64_t currentSec = nowMs / 1000;
    int64_t oldSec = windowSec_.load(std::memory_order_relaxed);

    while (oldSec != currentSec) {
        if (windowSec_.compare_exchange_weak(oldSec, currentSec, std::memory_order_relaxed)) {
            admittedInWindow_.store(0, std::memory_order_relaxed);
            return;
        }
    }
}

bool LogRateLimiter::TryAdmitInCurrentSecond(int64_t nowMs)
{
    RefreshRequestWindow(nowMs);

    int32_t limit = rate_.load(std::memory_order_relaxed);
    if (limit <= 0) {
        return true;
    }

    int32_t admitted = admittedInWindow_.load(std::memory_order_relaxed);
    while (admitted < limit) {
        if (admittedInWindow_.compare_exchange_weak(admitted, admitted + 1, std::memory_order_relaxed)) {
            return true;
        }
    }

    return false;
}

bool LogRateLimiter::GetDecisionFromTable(uint64_t traceHash, int64_t nowMs, bool &admitted)
{
    auto it = traceDecisions_.find(traceHash);
    if (it == traceDecisions_.end()) {
        return false;
    }

    if (it->second.expireAtMs <= nowMs) {
        traceDecisions_.erase(it);
        return false;
    }

    admitted = it->second.admitted;
    it->second.expireAtMs = nowMs + TRACE_DECISION_TTL_MS;
    return true;
}

void LogRateLimiter::UpsertDecision(uint64_t traceHash, bool admitted, int64_t nowMs)
{
    traceDecisions_[traceHash] = TraceDecisionEntry{ admitted, nowMs + TRACE_DECISION_TTL_MS };
}

void LogRateLimiter::CleanupExpiredDecisions(int64_t nowMs)
{
    if (nowMs - lastCleanupMs_ < TRACE_DECISION_CLEANUP_INTERVAL_MS) {
        return;
    }
    lastCleanupMs_ = nowMs;

    for (auto it = traceDecisions_.begin(); it != traceDecisions_.end();) {
        if (it->second.expireAtMs <= nowMs) {
            it = traceDecisions_.erase(it);
            continue;
        }
        ++it;
    }
}

void LogRateLimiter::ClearDecisionTable()
{
    std::lock_guard<std::mutex> lock(decisionTableMutex_);
    traceDecisions_.clear();
    lastCleanupMs_ = 0;
}

void LogRateLimiter::SetRate(int32_t ratePerSecond)
{
    int32_t newRate = std::max(ratePerSecond, static_cast<int32_t>(0));
    int32_t oldRate = rate_.exchange(newRate, std::memory_order_relaxed);
    if (oldRate == newRate) {
        return;
    }

    windowSec_.store(NowMs() / 1000, std::memory_order_relaxed);
    admittedInWindow_.store(0, std::memory_order_relaxed);
    ClearDecisionTable();
}

void LogRateLimiter::Reset()
{
    rate_.store(0, std::memory_order_relaxed);
    windowSec_.store(NowMs() / 1000, std::memory_order_relaxed);
    admittedInWindow_.store(0, std::memory_order_relaxed);
    ClearDecisionTable();
}

}  // namespace datasystem
