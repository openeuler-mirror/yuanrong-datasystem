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

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace object_cache {
const uint ms2us = 1'000ul;
const uint s2ms = 1'000ul;
constexpr uint64_t RATE_SMOOTHING_DIVISOR = 2;

static inline std::time_t Now()
{
    return GetSteadyClockTimeStampUs() / ms2us;
}

DataLimiter::DataLimiter(uint64_t rate, uint64_t maxTokenSize)
    : rate_(rate), tokens_(rate), maxTokenSize_(maxTokenSize)
{
    timestamp_ = Now();
}

void DataLimiter::WaitAllow(uint64_t requiredSize)
{
    std::unique_lock<std::mutex> l(mtx_);
    uint64_t originalMax = maxTokenSize_;
    bool needRestore = false;

    if (requiredSize > maxTokenSize_) {
        maxTokenSize_ = requiredSize;
        needRestore = true;
    }
    while (tokens_ < requiredSize) {
        Refill();
        if (tokens_ < requiredSize) {
            cond_.wait_for(l, std::chrono::milliseconds(WaitMilliseconds(requiredSize)));
        } else {
            break;
        }
    }
    tokens_ -= requiredSize;
    if (needRestore) {
        maxTokenSize_ = originalMax;
    }
}

void DataLimiter::Refill()
{
    auto now = Now();
    uint64_t elapsed = now - timestamp_;
    INJECT_POINT("migrate.limiter.elapsed.longtime", [&elapsed] {
        uint64_t delayTimeS = 100;
        elapsed += delayTimeS * s2ms;
    });
    uint64_t newTokens;
    if (rate_ <= UINT64_MAX / (elapsed == 0 ? 1 : elapsed)) {
        newTokens = rate_ * elapsed;
    } else {
        newTokens = UINT64_MAX;
    }
    newTokens = newTokens / s2ms + 1;
    tokens_ = newTokens + tokens_ > tokens_ ? newTokens + tokens_ : UINT64_MAX;
    if (tokens_ > maxTokenSize_) {
        tokens_ = maxTokenSize_;
    }
    timestamp_ = now;
}

void DataLimiter::UpdateRate(uint64_t rate)
{
    std::unique_lock<std::mutex> l(mtx_);
    rate_ = rate;
}

std::time_t DataLimiter::WaitMilliseconds(uint64_t requiredSize)
{
    if (requiredSize <= tokens_) {
        return 0;
    }
    return (requiredSize - tokens_) * s2ms / rate_ + 1;
}

bool DataLimiter::IsRemoteBusyNode() const
{
    std::unique_lock<std::mutex> l(mtx_);
    return rate_ == 0;
}

void MigrateDataRateLimiter::SlidingWindowUpdateRate(const uint64_t &bytesReceived)
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto now = std::chrono::steady_clock::now();
    window.push_back({ now, bytesReceived });
    currentBandwidth += bytesReceived;

    while (!window.empty() && window.front().timestamp + std::chrono::seconds(1) < now) {
        currentBandwidth -= window.front().bytes;
        window.pop_front();
    }
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
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    uint64_t lastRate =
        rateMap_.count(workerAddr) ? rateMap_[workerAddr] : rateLimiter_.GetMaxBandwidth() / RATE_SMOOTHING_DIVISOR;
    uint64_t newRate = CalculateSmoothedRate(lastRate, rateLimiter_.GetAvailableBandwidth());
    uint64_t timestampMs = GetSteadyClockTimeStampMs();
    rateTimeStampMap_[workerAddr] = timestampMs;
    rateMap_[workerAddr] = newRate;
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
    return newRate;
}

void MigrateDataRateController::ClearExpiredRate(const std::string &workerAddr, uint64_t expireMs,
                                                 uint64_t lastUpdateTimeMs)
{
    std::lock_guard<std::shared_timed_mutex> l(mutex_);
    auto it = rateTimeStampMap_.find(workerAddr);
    if (it == rateTimeStampMap_.end() || it->second != lastUpdateTimeMs) {
        return;
    }
    if (GetSteadyClockTimeStampMs() - it->second >= expireMs) {
        rateMap_.erase(workerAddr);
        rateTimeStampMap_.erase(workerAddr);
    }
}
}  // namespace object_cache
}  // namespace datasystem
