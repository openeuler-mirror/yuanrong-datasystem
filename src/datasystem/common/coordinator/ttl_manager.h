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
 * Description: TTL manager for key expiration in coordinator KV store.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_TTL_MANAGER_H
#define DATASYSTEM_COMMON_COORDINATOR_TTL_MANAGER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class TtlManager {
public:
    explicit TtlManager(std::shared_ptr<SteadyClock> clock = nullptr);
    ~TtlManager();

    /**
     * @brief Schedule an expiry check for a key whose TTL metadata is stored in MemoryKvStore.
     * @param[in] key The key.
     * @param[in] ttlMs TTL in milliseconds. 0 means no TTL.
     * @param[in] revision The key revision associated with this schedule.
     * @param[in] ttlGeneration The TTL generation associated with this schedule.
     * @return Status of the operation.
     */
    Status Schedule(const std::string &key, int64_t ttlMs, int64_t revision, uint64_t ttlGeneration);

    /**
     * @brief Set callback invoked when a scheduled TTL check expires.
     * @param[in] callback Function receiving the expired key, revision, and TTL generation.
     */
    void SetExpireCallback(std::function<bool(const std::string &, int64_t, uint64_t)> callback);

    /**
     * @brief Start the expiration check thread.
     */
    void Start();

    /**
     * @brief Stop the expiration check thread.
     */
    void Stop();

private:
    /**
     * @brief Expiration check main loop.
     */
    void CheckExpiration();

    struct TtlScheduleRecord {
        std::string key;
        int64_t revision = 0;
        uint64_t ttlGeneration = 0;
        std::chrono::steady_clock::time_point expireTime;
    };

    using ExpiryMap = std::multimap<std::chrono::steady_clock::time_point, TtlScheduleRecord>;

    ExpiryMap expiryMap_;
    std::unordered_map<std::string, ExpiryMap::iterator> expiryIndex_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread expirationThread_;
    std::function<bool(const std::string &, int64_t, uint64_t)> expireCallback_;
    std::shared_ptr<SteadyClock> clock_;
    std::atomic<bool> running_{ false };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_TTL_MANAGER_H
