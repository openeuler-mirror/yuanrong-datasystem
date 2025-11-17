/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Define api of stream cache
 */
#ifndef DATASYSTEM_STREAM_CACHE_STREAM_CONFIG_H
#define DATASYSTEM_STREAM_CACHE_STREAM_CONFIG_H

#include <string>
#include <utility>

#include "datasystem/utils/status.h"

namespace datasystem {
static constexpr int SC_NORMAL_LOG_LEVEL = 1;    // Normal output log level for stream cache module
static constexpr int SC_INTERNAL_LOG_LEVEL = 2;  // Internal output log level for stream cache module
static constexpr int SC_DEBUG_LOG_LEVEL = 3;     // Debug output log level for stream cache module

/**
 * @brief Subscription Types.
 * @details Stream Mode, Queue Mode (Round Robin and Key Partition).
 */
enum SubscriptionType { STREAM, ROUND_ROBIN, KEY_PARTITIONS, UNKNOWN };

/**
 * @brief Subscription configuration.
 * @details Consisting of subscription name and type. Optionally, the cache capacity can be adjusted, and the cache
 * prefetch low water mark can be enabled (non-zero value will turn prefetching on).
 */
struct SubscriptionConfig {
    static constexpr uint32_t SC_CACHE_CAPACITY = 32768;  // Default local subscription cache capacity
    static constexpr uint16_t SC_CACHE_LWM = 0;           // Default cache prefetch percent.
    std::string subscriptionName;
    SubscriptionType subscriptionType = SubscriptionType::STREAM;
    uint32_t cacheCapacity = SC_CACHE_CAPACITY;
    uint16_t cachePrefetchLWM = SC_CACHE_LWM;  // Enabled when value is greater than 0. Default is off.
    // Should the consumer receive notification about the fault of a producer. Default is false.

    SubscriptionConfig(std::string subName, const SubscriptionType subType)
        : subscriptionName(std::move(subName)), subscriptionType(subType)
    {
    }

    SubscriptionConfig(std::string subName, const SubscriptionType subType, uint32_t cacheMax,
                       uint16_t cachePrefetchPercent)
        : subscriptionName(std::move(subName)), subscriptionType(subType), cacheCapacity(cacheMax),
          cachePrefetchLWM(cachePrefetchPercent)
    {
    }

    SubscriptionConfig() = default;

    SubscriptionConfig(const SubscriptionConfig &other) = default;

    SubscriptionConfig &operator=(const SubscriptionConfig &other) = default;

    SubscriptionConfig(SubscriptionConfig &&other) noexcept
    {
        subscriptionName = std::move(other.subscriptionName);
        subscriptionType = other.subscriptionType;
        cacheCapacity = other.cacheCapacity;
        cachePrefetchLWM = other.cachePrefetchLWM;
    }

    SubscriptionConfig &operator=(SubscriptionConfig &&other) noexcept
    {
        subscriptionName = std::move(other.subscriptionName);
        subscriptionType = other.subscriptionType;
        cacheCapacity = other.cacheCapacity;
        cachePrefetchLWM = other.cachePrefetchLWM;
        return *this;
    }

    bool operator==(const SubscriptionConfig &config) const
    {
        return (subscriptionName == config.subscriptionName && subscriptionType == config.subscriptionType
                && cacheCapacity == config.cacheCapacity && cachePrefetchLWM == config.cachePrefetchLWM);
    }

    bool operator!=(const SubscriptionConfig &config) const
    {
        return !(*this == config);
    }
};

enum StreamMode : int32_t { MPMC = 0, MPSC, SPSC };

/**
 * @brief Producer configuration.
 * @details Auto flush time and page size.
 */
struct ProducerConf {
    // default auto flush time 5ms.
    int64_t delayFlushTime = 5;

    // default page size 1MB, must be a multiple of 4KB, must not greater than 16MB.
    int64_t pageSize = 1024 * 1024ul;

    // default max stream size 100MB, must greater then 64KB and less than the shared memory size.
    uint64_t maxStreamSize = 100 * 1024 * 1024ul;

    // auto stream clean up when the last producer/consumer exits.
    bool autoCleanup = false;

    // the number of consumers to retain data for, default to 0.
    // Notice: If a worker is voluntary scaled down, data will be lost if no remote consumer is created, even if
    // retainForNumConsumers is set.
    uint64_t retainForNumConsumers = 0;

    // enable stream data encryption between workers.
    bool encryptStream = false;

    // default reserve size to page size, must be a multiple of page size.
    uint64_t reserveSize = 0;

    // default stream mode MPMC.
    StreamMode streamMode = StreamMode::MPMC;
};
}  // namespace datasystem
#endif  // DATASYSTEM_STREAM_CACHE_STREAM_CONFIG_H
