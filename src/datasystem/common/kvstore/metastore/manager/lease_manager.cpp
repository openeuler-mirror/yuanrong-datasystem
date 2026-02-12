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
 * Description: Lease manager implementation.
 */
#include "datasystem/common/kvstore/metastore/manager/lease_manager.h"

#include <algorithm>
#include <chrono>
#include <random>
#include "datasystem/common/log/log.h"

namespace datasystem {

LeaseManager::~LeaseManager()
{
    StopExpirationCheck();
}

int64_t LeaseManager::GenerateLeaseId()
{
    // Use timestamp (in milliseconds) + random number for lease ID
    // Similar to etcd's implementation
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    // Get random 16-bit value
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint16_t> dis(0, 0xFFFF);
    uint16_t random = dis(gen);

    // (timestamp << 16) | random
    return (timestamp << 16) | random;
}

Status LeaseManager::Grant(int64_t ttl, int64_t *leaseId, int64_t *actualTtl)
{
    *leaseId = GenerateLeaseId();
    *actualTtl = ttl;

    LeaseInfo info;
    info.leaseId = *leaseId;
    info.ttl = ttl;
    info.expireTime = std::chrono::steady_clock::now() + std::chrono::seconds(ttl);

    std::lock_guard<std::mutex> lock(mutex_);
    leases_[*leaseId] = info;

    return Status::OK();
}

Status LeaseManager::Revoke(int64_t leaseId)
{
    LOG(INFO) << "Revoking lease: " << leaseId;
    std::vector<std::string> keys;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = leases_.find(leaseId);
        if (it == leases_.end()) {
            return Status(StatusCode::K_NOT_FOUND, "Lease not found: " + std::to_string(leaseId));
        }
        // Copy keys before erasing
        keys.assign(it->second.keys.begin(), it->second.keys.end());
        leases_.erase(it);
    }

    // Delete attached keys after releasing the lock
    if (deleteKeyCallback_) {
        for (const auto &key : keys) {
            deleteKeyCallback_(key);
        }
    }

    return Status::OK();
}

Status LeaseManager::KeepAlive(int64_t leaseId, int64_t *ttl)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = leases_.find(leaseId);
    if (it == leases_.end()) {
        return Status(StatusCode::K_NOT_FOUND, "Lease not found: " + std::to_string(leaseId));
    }

    // Update expiration time
    it->second.expireTime = std::chrono::steady_clock::now() + std::chrono::seconds(it->second.ttl);
    *ttl = it->second.ttl;

    return Status::OK();
}

Status LeaseManager::AttachKey(int64_t leaseId, const std::string &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = leases_.find(leaseId);
    if (it == leases_.end()) {
        return Status(StatusCode::K_NOT_FOUND, "Lease not found: " + std::to_string(leaseId) + ", key: " + key);
    }
    it->second.keys.insert(key);
    return Status::OK();
}

Status LeaseManager::DetachKey(int64_t leaseId, const std::string &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = leases_.find(leaseId);
    if (it == leases_.end()) {
        // Detach is idempotent: if lease doesn't exist, key is already detached
        return Status::OK();
    }
    it->second.keys.erase(key);
    return Status::OK();
}

bool LeaseManager::LeaseExists(int64_t leaseId) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return leases_.find(leaseId) != leases_.end();
}

void LeaseManager::StartExpirationCheck()
{
    if (running_.load()) {
        return;
    }
    running_.store(true);
    expirationThread_ = std::thread([this]() {
        while (running_.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            CheckExpiration();
        }
    });
}

void LeaseManager::StopExpirationCheck()
{
    running_.store(false);
    if (expirationThread_.joinable()) {
        expirationThread_.join();
    }
}

void LeaseManager::CheckExpiration()
{
    auto now = std::chrono::steady_clock::now();
    std::vector<int64_t> expiredLeases;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &[leaseId, info] : leases_) {
            if (now >= info.expireTime) {
                expiredLeases.push_back(leaseId);
            }
        }
    }

    // Revoke expired leases
    for (auto leaseId : expiredLeases) {
        Revoke(leaseId);
    }
}

}  // namespace datasystem
