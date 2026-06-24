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

#include "datasystem/common/coordinator/ttl_manager.h"

#include <utility>
#include <vector>

namespace datasystem {
namespace {
constexpr int64_t EXPIRY_POLL_INTERVAL_MS = 50;
constexpr size_t MAX_EXPIRED_KEYS_PER_CHECK = 1024;

struct ExpiredTtlRecord {
    std::string key;
    int64_t revision;
    uint64_t ttlGeneration;
};
}  // namespace
TtlManager::TtlManager(std::shared_ptr<SteadyClock> clock)
    : clock_(clock ? std::move(clock) : std::make_shared<SteadyClockReal>())
{
}

TtlManager::~TtlManager()
{
    Stop();
}

Status TtlManager::Schedule(const std::string &key, int64_t ttlMs, int64_t revision, uint64_t ttlGeneration)
{
    if (ttlMs <= 0) {
        return Status::OK();
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto expireTime = clock_->Now() + std::chrono::milliseconds(ttlMs);
    auto [indexIt, inserted] = expiryIndex_.try_emplace(key, expiryMap_.end());
    if (!inserted) {
        const auto &existing = indexIt->second->second;
        if (revision < existing.revision || (revision == existing.revision && ttlGeneration < existing.ttlGeneration)) {
            return Status::OK();
        }
        expiryMap_.erase(indexIt->second);
    }

    bool needNotify = expiryMap_.empty() || expireTime < expiryMap_.begin()->first;
    TtlScheduleRecord record{ key, revision, ttlGeneration, expireTime };
    indexIt->second = expiryMap_.emplace(expireTime, std::move(record));

    if (needNotify) {
        cv_.notify_one();
    }
    return Status::OK();
}

void TtlManager::SetExpireCallback(std::function<bool(const std::string &, int64_t, uint64_t)> callback)
{
    expireCallback_ = std::move(callback);
}

void TtlManager::Start()
{
    if (running_) {
        return;
    }
    running_ = true;
    expirationThread_ = std::thread(&TtlManager::CheckExpiration, this);
}

void TtlManager::Stop()
{
    if (!running_) {
        return;
    }
    running_ = false;
    cv_.notify_all();
    if (expirationThread_.joinable()) {
        expirationThread_.join();
    }
}

void TtlManager::CheckExpiration()
{
    while (running_) {
        std::vector<ExpiredTtlRecord> expiredKeys;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(EXPIRY_POLL_INTERVAL_MS), [this] {
                if (!running_) {
                    return true;
                }
                if (expiryMap_.empty()) {
                    return false;
                }
                return expiryMap_.begin()->first <= clock_->Now();
            });

            if (!running_) {
                break;
            }

            auto now = clock_->Now();
            while (!expiryMap_.empty() && expiryMap_.begin()->first <= now
                   && expiredKeys.size() < MAX_EXPIRED_KEYS_PER_CHECK) {
                auto it = expiryMap_.begin();
                const auto &record = it->second;
                auto indexIt = expiryIndex_.find(record.key);
                if (indexIt != expiryIndex_.end() && indexIt->second == it) {
                    expiryIndex_.erase(indexIt);
                }
                expiredKeys.push_back({ record.key, record.revision, record.ttlGeneration });
                expiryMap_.erase(it);
            }
        }

        if (!expireCallback_) {
            continue;
        }
        for (auto &record : expiredKeys) {
            (void)expireCallback_(record.key, record.revision, record.ttlGeneration);
        }
    }
}
}  // namespace datasystem
