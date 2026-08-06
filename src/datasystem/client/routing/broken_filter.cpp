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

#include "datasystem/client/routing/broken_filter.h"

#include <cstdint>
#include <thread>
#include <utility>

namespace datasystem {
namespace client {

BrokenFilter::BrokenFilter()
{
    std::atomic_store(&healthMap_, std::shared_ptr<const HealthMap>(std::make_shared<HealthMap>()));
}

bool BrokenFilter::IsAvailable(const HostPort &addr) const
{
    auto map = std::atomic_load(&healthMap_);
    auto it = map->find(addr.ToString());
    if (it == map->end()) {
        return true;  // Never observed a failure -> available
    }
    // Lazy TTL: available again once the broken window expires.
    return std::chrono::steady_clock::now() >= it->second.brokenUntil;
}

void BrokenFilter::OnWorkerStateChange(const HostPort &addr, StatusCode status)
{
    if (status != K_CLIENT_WORKER_DISCONNECT) {
        return;  // Only connection failures feed the eviction burst counter.
    }
    const std::string key = addr.ToString();
    const auto now = std::chrono::steady_clock::now();
    bool done = false;
    while (!done) {
        auto old = std::atomic_load(&healthMap_);
        auto existing = old->find(key);
        if (existing != old->end() && now < existing->second.brokenUntil) {
            done = true;  // Already broken within TTL; ignore further failure signals.
        } else {
            auto next = std::make_shared<HealthMap>(*old);
            // Lazy-expire entries that are neither broken nor tracking a fresh burst.
            for (auto it = next->begin(); it != next->end();) {
                const auto &entry = it->second;
                const bool brokenExpired = now >= entry.brokenUntil;
                const bool burstExpired = entry.consecutiveFailures == 0
                    || (now - entry.windowStart > FAILURE_BURST_WINDOW);
                if (brokenExpired && burstExpired) {
                    it = next->erase(it);
                } else {
                    ++it;
                }
            }
            auto &health = (*next)[key];
            if (health.consecutiveFailures == 0 || (now - health.windowStart > FAILURE_BURST_WINDOW)) {
                health.consecutiveFailures = 1;
                health.windowStart = now;
            } else {
                health.consecutiveFailures += 1;
            }
            if (health.consecutiveFailures >= EVICT_CONSECUTIVE_FAILURES) {
                health.brokenUntil = now + BROKEN_TTL;
                health.consecutiveFailures = 0;  // Reset; worker must fail N times again after TTL.
            }
            done = std::atomic_compare_exchange_weak(&healthMap_, &old,
                std::shared_ptr<const HealthMap>(std::move(next)));
            if (!done) {
                // CAS failed: another concurrent update won; yield and retry.
                std::this_thread::yield();
            }
        }
    }
}

}  // namespace client
}  // namespace datasystem
