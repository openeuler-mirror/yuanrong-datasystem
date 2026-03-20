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
 * Description: Migrate data limiter.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_MIGRATE_LIMITER_H
#define DATASYSTEM_MIGRATE_DATA_MIGRATE_LIMITER_H

#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <deque>
#include <mutex>
#include <shared_mutex>

namespace datasystem {
namespace object_cache {
class DataLimiter {
public:
    DataLimiter(uint64_t rate, uint64_t maxTokenSize = UINT64_MAX);

    ~DataLimiter() = default;

    /**
     * @brief Wait util tokens match the require size.
     * @param[in] requiredSize Required size.
     */
    void WaitAllow(uint64_t requiredSize);

    /**
     * @brief Update rate.
     * @param[in] rate Limit rate from response.
     */
    void UpdateRate(uint64_t rate);

    /**
     * @brief Check if remote is busy.
     * @return True if remote is busy, false otherwise.
     */
    bool IsRemoteBusyNode() const;

private:
    /**
     * @brief Refill tokens.
     */
    void Refill();

    /**
     * @brief Calculate need wait milliseconds via required size.
     * @param[in] requiredSize Required size.
     * @return Need wait milliseconds.
     */
    std::time_t WaitMilliseconds(uint64_t requiredSize);

    uint64_t rate_;

    uint64_t tokens_;

    std::time_t timestamp_;

    mutable std::mutex mtx_;

    std::condition_variable cond_;

    uint64_t maxTokenSize_ = UINT64_MAX;
};

class MigrateDataRateLimiter {
public:
    MigrateDataRateLimiter(uint64_t maxBandwidthBytes)
        : maxBandwidth(maxBandwidthBytes), currentBandwidth(0)
    {
    }

    ~MigrateDataRateLimiter() = default;

    /**
     * @brief Update current bandwidth by sliiding window.
     * @param[in] bytesReceived Bytes received of this reqeust.
     */
    void SlidingWindowUpdateRate(const uint64_t &bytesReceived);

    /**
     * @brief Get available bandwidth.
     * @return Available bandwidth of this node.
     */
    uint64_t GetAvailableBandwidth()
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        if (currentBandwidth >= maxBandwidth) {
            return 0;
        }
        return maxBandwidth - currentBandwidth;
    }

    /**
     * @brief Get max bandwidth.
     * @return Max bandwidth of this node.
     */
    uint64_t GetMaxBandwidth()
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        return maxBandwidth;
    }

private:
    struct TimestampedData {
        std::chrono::time_point<std::chrono::steady_clock> timestamp;
        uint64_t bytes;
    };

    std::shared_timed_mutex mutex_;
    std::deque<TimestampedData> window;
    const uint64_t maxBandwidth;
    uint64_t currentBandwidth;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
