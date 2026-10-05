/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Migrate data limiter implementation.
 */
#include "datasystem/worker/object_cache/limiter/data_limiter.h"

#include <algorithm>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace object_cache {
const uint MS2US = 1'000u;
const uint S2MS = 1'000u;
constexpr uint64_t RATE_SMOOTHING_DIVISOR = 2;

static inline std::time_t Now()
{
    return GetSteadyClockTimeStampUs() / MS2US;
}

DataLimiter::DataLimiter(uint64_t rate, uint64_t maxTokenSize)
    : rate_(rate), tokens_(rate), maxTokenSize_(maxTokenSize)
{
    timestamp_ = Now();
}

void DataLimiter::WaitAllow(uint64_t requiredSize)
{
    (void)WaitAllow(requiredSize, nullptr);
}

bool DataLimiter::WaitAllow(uint64_t requiredSize, const std::atomic<bool> *cancelled, uint64_t maxWaitMs)
{
    std::unique_lock<std::mutex> l(mtx_);
    uint64_t originalMax = maxTokenSize_;
    bool needRestore = false;

    if (requiredSize > maxTokenSize_) {
        maxTokenSize_ = requiredSize;
        needRestore = true;
    }
    constexpr uint64_t CANCEL_POLL_MS = 10;
    constexpr uint64_t STALL_LOG_INTERVAL_MS = 5000;
    uint64_t waitedMs = 0;
    uint64_t sinceLogMs = 0;
    while (tokens_ < requiredSize) {
        if (cancelled != nullptr && cancelled->load(std::memory_order_acquire)) {
            if (needRestore) {
                maxTokenSize_ = originalMax;
            }
            return false;
        }
        if (waitedMs >= maxWaitMs) {
            if (needRestore) {
                maxTokenSize_ = originalMax;
            }
            LOG(WARNING) << "event=LIMITER_WAIT_TIMEOUT waited_ms=" << waitedMs << " budget_ms=" << maxWaitMs
                         << " required_size=" << requiredSize << " tokens=" << tokens_ << " rate_bps=" << rate_;
            return false;
        }
        Refill();
        if (tokens_ < requiredSize) {
            // WaitMilliseconds returns UINT64_MAX at rate 0; cap the sleep by the remaining budget so
            // a finite budget can always elapse, while the legacy UINT64_MAX default stays unbounded.
            auto waitMs = std::min(WaitMilliseconds(requiredSize), maxWaitMs - waitedMs);
            if (cancelled != nullptr) {
                waitMs = std::min<uint64_t>(waitMs, CANCEL_POLL_MS);
            }
            cond_.wait_for(l, std::chrono::milliseconds(waitMs));
            waitedMs += waitMs;
            sinceLogMs += waitMs;
            if (sinceLogMs >= STALL_LOG_INTERVAL_MS) {
                // A long rate wait must never be silent again (2026-10-05 140s silent drain stall).
                LOG(WARNING) << "event=LIMITER_WAITING waited_ms=" << waitedMs
                             << " required_size=" << requiredSize << " tokens=" << tokens_
                             << " rate_bps=" << rate_ << " budget_ms=" << maxWaitMs;
                sinceLogMs = 0;
            }
        } else {
            break;
        }
    }
    tokens_ -= requiredSize;
    if (needRestore) {
        maxTokenSize_ = originalMax;
    }
    return true;
}

uint64_t DataLimiter::EstimateWaitMilliseconds(uint64_t requiredSize)
{
    std::unique_lock<std::mutex> l(mtx_);
    Refill();
    return WaitMilliseconds(requiredSize);
}

void DataLimiter::Refill()
{
    auto now = Now();
    uint64_t elapsed = now - timestamp_;
    INJECT_POINT("migrate.limiter.elapsed.longtime", [&elapsed] {
        uint64_t delayTimeS = 100;
        elapsed += delayTimeS * S2MS;
    });
    uint64_t newTokens;
    if (rate_ <= UINT64_MAX / (elapsed == 0 ? 1 : elapsed)) {
        newTokens = rate_ * elapsed;
    } else {
        newTokens = UINT64_MAX;
    }
    newTokens = newTokens / S2MS + 1;
    tokens_ = newTokens + tokens_ > tokens_ ? newTokens + tokens_ : UINT64_MAX;
    if (tokens_ > maxTokenSize_) {
        tokens_ = maxTokenSize_;
    }
    timestamp_ = now;
}

void DataLimiter::UpdateRate(uint64_t rate)
{
    {
        std::lock_guard<std::mutex> l(mtx_);
        rate_ = rate;
    }
    // A recovered rate refills tokens far faster than the sleep a waiter computed under the old
    // collapsed rate; without this wakeup the waiter idles until that stale deadline.
    cond_.notify_all();
}

uint64_t DataLimiter::WaitMilliseconds(uint64_t requiredSize) const
{
    if (requiredSize <= tokens_) {
        return 0;
    }
    if (rate_ == 0) {
        return UINT64_MAX;
    }
    return (requiredSize - tokens_) * S2MS / rate_ + 1;
}

bool DataLimiter::IsRemoteBusyNode() const
{
    INJECT_POINT("migrate.limiter.is_busy_node", []() { return true; });
    std::unique_lock<std::mutex> l(mtx_);
    return rate_ == 0;
}

void MigrateDataRateLimiter::SlidingWindowUpdateRate(const uint64_t &bytesReceived)
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto now = std::chrono::steady_clock::now();
    window.push_back({ now, bytesReceived });
    currentBandwidth += bytesReceived;

    PruneExpiredLocked(now);
}

MigrateDataRateController::MigrateDataRateController(uint64_t maxBandwidthBytes) : rateLimiter_(maxBandwidthBytes)
{
}

void MigrateDataRateController::SlidingWindowUpdateRate(uint64_t bytesReceived)
{
    rateLimiter_.SlidingWindowUpdateRate(bytesReceived);
}

uint64_t MigrateDataRateController::CalculateSmoothedRate(uint64_t lastRate, uint64_t availableBandwidth)
{
    if (availableBandwidth < lastRate) {
        return availableBandwidth;
    }
    return (lastRate + availableBandwidth) / RATE_SMOOTHING_DIVISOR;
}

uint64_t MigrateDataRateController::CalculateNewRate(const std::string &workerAddr)
{
    const uint64_t maxBandwidth = rateLimiter_.GetMaxBandwidth();
    const uint64_t availableBandwidth = rateLimiter_.GetAvailableBandwidth();
    const uint64_t timestampMs = GetSteadyClockTimeStampMs();
    uint64_t lastRate;
    uint64_t newRate;
    {
        RateTable::accessor record;
        const bool inserted = rateTable_.insert(record, workerAddr);
        lastRate = inserted ? maxBandwidth / RATE_SMOOTHING_DIVISOR : record->second.rate;
        newRate = CalculateSmoothedRate(lastRate, availableBandwidth);
        record->second = { newRate, timestampMs };
    }
    TimerQueue::TimerImpl timer;
    const uint32_t expireMs = RATE_RECORD_EXPIRE_MS;
    std::weak_ptr<MigrateDataRateController> weakPtr = weak_from_this();
    TimerQueue::GetInstance()->AddTimer(
        expireMs,
        [workerAddr, expireMs, timestampMs, weakPtr]() {
            auto sharedPtr = weakPtr.lock();
            if (sharedPtr == nullptr) {
                return;
            }
            sharedPtr->ClearExpiredRate(workerAddr, expireMs, timestampMs);
        },
        timer);
    constexpr uint64_t lowRateDivisor = 10;
    constexpr uint32_t lowRateLogEveryN = 100;
    if (newRate == 0 || newRate <= maxBandwidth / lowRateDivisor) {
        LOG_EVERY_N(INFO, lowRateLogEveryN)
            << "event=MIGRATE_RATE_HINT_LOW source=" << workerAddr << " last_rate_bps=" << lastRate
            << " available_bandwidth=" << availableBandwidth << " max_bandwidth=" << maxBandwidth
            << " new_rate_bps=" << newRate;
    }
    return newRate;
}

uint64_t MigrateDataRateController::PeekAvailableRate(const std::string &workerAddr)
{
    const uint64_t maxBandwidth = rateLimiter_.GetMaxBandwidth();
    uint64_t lastRate;
    {
        RateTable::const_accessor record;
        lastRate = rateTable_.find(record, workerAddr) ? record->second.rate : maxBandwidth / RATE_SMOOTHING_DIVISOR;
    }
    return CalculateSmoothedRate(lastRate, rateLimiter_.GetAvailableBandwidth());
}

void MigrateDataRateController::ClearExpiredRate(const std::string &workerAddr, uint64_t expireMs,
                                                 uint64_t lastUpdateTimeMs)
{
    RateTable::accessor record;
    if (!rateTable_.find(record, workerAddr) || record->second.timestampMs != lastUpdateTimeMs) {
        return;
    }
    if (GetSteadyClockTimeStampMs() - record->second.timestampMs >= expireMs) {
        rateTable_.erase(record);
    }
}
}  // namespace object_cache
}  // namespace datasystem
