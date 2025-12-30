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
#include "datasystem/worker/object_cache/data_migrator/basic/migrate_data_limiter.h"

#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace object_cache {
const uint ms2us = 1'000ul;
const uint s2ms = 1'000ul;

static inline std::time_t Now()
{
    return GetSteadyClockTimeStampUs() / ms2us;
}

MigrateDataLimiter::MigrateDataLimiter(uint64_t rate) : rate_(rate), tokens_(rate)
{
    timestamp_ = Now();
}

void MigrateDataLimiter::WaitAllow(uint64_t requiredSize)
{
    std::unique_lock<std::mutex> l(mtx_);
    while (tokens_ < requiredSize) {
        Refill();
        if (tokens_ < requiredSize) {
            cond_.wait_for(l, std::chrono::milliseconds(WaitMilliseconds(requiredSize)));
        } else {
            break;
        }
    }
    tokens_ -= requiredSize;
}

void MigrateDataLimiter::Refill()
{
    auto now = Now();
    uint64_t elapsed = now - timestamp_;
    uint64_t newTokens;
    if (rate_ <= UINT64_MAX / (elapsed == 0 ? 1 : elapsed)) {
        newTokens = rate_ * elapsed;
    } else {
        newTokens = UINT64_MAX;
    }
    newTokens = newTokens / s2ms + 1;
    tokens_ = newTokens + tokens_ > tokens_ ? newTokens + tokens_ : UINT64_MAX;
    timestamp_ = now;
}

void MigrateDataLimiter::UpdateRate(uint64_t rate)
{
    std::unique_lock<std::mutex> l(mtx_);
    rate_ = rate;
}

std::time_t MigrateDataLimiter::WaitMilliseconds(uint64_t requiredSize)
{
    if (requiredSize <= tokens_) {
        return 0;
    }
    return (requiredSize - tokens_) * s2ms / rate_ + 1;
}

bool MigrateDataLimiter::IsRemoteBusyNode() const
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

    while (!window.empty() && (window.front().timestamp - std::chrono::seconds(1)) < now) {
        currentBandwidth -= window.front().bytes;
        window.pop_front();
    }
}
}  // namespace object_cache
}  // namespace datasystem