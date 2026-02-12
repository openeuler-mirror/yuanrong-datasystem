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
 * Description: Lease manager for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_LEASE_LEASE_MANAGER_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_LEASE_LEASE_MANAGER_H

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {

class LeaseManager {
public:
    LeaseManager() = default;
    ~LeaseManager();

    /**
     * @brief Grant a lease with TTL
     * @param ttl Time-to-live in seconds
     * @param leaseId Output lease ID
     * @param actualTtl Output actual TTL
     * @return Status of operation
     */
    Status Grant(int64_t ttl, int64_t *leaseId, int64_t *actualTtl);

    /**
     * @brief Revoke a lease
     * @param leaseId The lease ID
     * @return Status of operation
     */
    Status Revoke(int64_t leaseId);

    /**
     * @brief Keep a lease alive
     * @param leaseId The lease ID
     * @param ttl Output remaining TTL
     * @return Status of operation
     */
    Status KeepAlive(int64_t leaseId, int64_t *ttl);

    /**
     * @brief Attach a key to a lease
     * @param leaseId The lease ID
     * @param key The key
     * @return Status of operation
     */
    Status AttachKey(int64_t leaseId, const std::string &key);

    /**
     * @brief Detach a key from a lease
     * @param leaseId The lease ID
     * @param key The key
     * @return Status of operation
     */
    Status DetachKey(int64_t leaseId, const std::string &key);

    /**
     * @brief Check if a lease exists
     * @param leaseId The lease ID
     * @return true if lease exists
     */
    bool LeaseExists(int64_t leaseId) const;

    /**
     * @brief Start expiration check thread
     */
    void StartExpirationCheck();

    /**
     * @brief Stop expiration check thread
     */
    void StopExpirationCheck();

    /**
     * @brief Set delete key callback (called when lease expires)
     */
    void SetDeleteKeyCallback(std::function<Status(const std::string &)> callback)
    {
        deleteKeyCallback_ = callback;
    }

private:
    /**
     * @brief Check and revoke expired leases
     */
    void CheckExpiration();

    /**
     * @brief Generate a unique lease ID using timestamp + random number
     * @return Generated lease ID
     */
    int64_t GenerateLeaseId();

    struct LeaseInfo {
        int64_t leaseId;
        int64_t ttl;
        std::chrono::steady_clock::time_point expireTime;
        std::unordered_set<std::string> keys;
    };

    std::unordered_map<int64_t, LeaseInfo> leases_;
    std::atomic<bool> running_{ false };
    std::thread expirationThread_;
    mutable std::mutex mutex_;
    std::function<Status(const std::string &)> deleteKeyCallback_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_LEASE_LEASE_MANAGER_H
