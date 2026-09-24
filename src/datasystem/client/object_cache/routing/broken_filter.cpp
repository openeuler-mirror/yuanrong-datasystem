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

#include "datasystem/client/object_cache/routing/broken_filter.h"

#include <cstdint>
#include <thread>
#include <utility>

namespace datasystem {
namespace client {

BrokenFilter::BrokenFilter()
{
    std::atomic_store(&healthMap_, std::shared_ptr<const HealthMap>(std::make_shared<HealthMap>()));
}

bool BrokenFilter::IsAvailable(const HostPort &addr, WorkerAccessAction action) const
{
    (void)action;
    auto map = std::atomic_load(&healthMap_);
    auto it = map->find(addr.ToString());
    if (it == map->end()) {
        return true;  // Never observed a failure -> available
    }
    // Lazy TTL: available again once the broken window expires.
    return std::chrono::steady_clock::now() >= it->second.brokenUntil;
}

void BrokenFilter::EraseExpiredEntries(HealthMap &healthMap, std::chrono::steady_clock::time_point now)
{
    for (auto it = healthMap.begin(); it != healthMap.end();) {
        const auto &entry = it->second;
        const bool brokenExpired = now >= entry.brokenUntil;
        const bool burstExpired = entry.consecutiveFailures == 0 || (now - entry.windowStart > FAILURE_BURST_WINDOW);
        if (brokenExpired && burstExpired) {
            it = healthMap.erase(it);
        } else {
            ++it;
        }
    }
}

void BrokenFilter::OnWorkerStateChange(const HostPort &addr, StatusCode status)
{
    const bool scaleDown = status == K_SCALE_DOWN;
    const bool peerDead = status == K_RPC_PEER_DEAD;
    const bool peerFailure = status == K_CLIENT_WORKER_DISCONNECT || peerDead;
    if (!scaleDown && !peerFailure) {
        return;
    }
    const std::string key = addr.ToString();
    const auto now = std::chrono::steady_clock::now();
    const auto untilRingUpdate = std::chrono::steady_clock::time_point::max();
    bool done = false;
    while (!done) {
        auto old = std::atomic_load(&healthMap_);
        auto existing = old->find(key);
        if (existing != old->end() && now < existing->second.brokenUntil
            && (!scaleDown || existing->second.brokenUntil == untilRingUpdate)) {
            done = true;
        } else {
            auto next = std::make_shared<HealthMap>(*old);
            // Lazy-expire entries that are neither broken nor tracking a fresh burst.
            EraseExpiredEntries(*next, now);
            auto &health = (*next)[key];
            if (scaleDown) {
                health.brokenUntil = untilRingUpdate;
                health.consecutiveFailures = 0;
            } else if (peerDead) {
                health.brokenUntil = now + BROKEN_TTL;
                health.consecutiveFailures = 0;
            } else if (health.consecutiveFailures == 0 || (now - health.windowStart > FAILURE_BURST_WINDOW)) {
                health.consecutiveFailures = 1;
                health.windowStart = now;
            } else {
                health.consecutiveFailures += 1;
            }
            if (!scaleDown && health.consecutiveFailures >= EVICT_CONSECUTIVE_FAILURES) {
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

void BrokenFilter::OnHashRingUpdated(const ::datasystem::ClusterTopologyPb &)
{
    const auto now = std::chrono::steady_clock::now();
    const auto untilRingUpdate = std::chrono::steady_clock::time_point::max();
    bool done = false;
    while (!done) {
        auto old = std::atomic_load(&healthMap_);
        auto next = std::make_shared<HealthMap>();
        for (const auto &[worker, health] : *old) {
            if (health.brokenUntil != untilRingUpdate && now < health.brokenUntil) {
                next->emplace(worker, health);
            }
        }
        done = std::atomic_compare_exchange_weak(&healthMap_, &old, std::shared_ptr<const HealthMap>(std::move(next)));
        if (!done) {
            std::this_thread::yield();
        }
    }
}

}  // namespace client
}  // namespace datasystem
