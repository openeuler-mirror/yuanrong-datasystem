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

#include <thread>
#include <utility>

namespace datasystem {
namespace client {

BrokenFilter::BrokenFilter()
{
    std::atomic_store(&brokenMap_, std::shared_ptr<const BrokenMap>(std::make_shared<BrokenMap>()));
}

bool BrokenFilter::IsAvailable(const HostPort &addr) const
{
    auto map = std::atomic_load(&brokenMap_);
    auto it = map->find(addr.ToString());
    if (it == map->end()) {
        return true;  // Not marked -> available
    }
    // TTL expired -> available (lazy expiry)
    return std::chrono::steady_clock::now() >= it->second;
}

void BrokenFilter::OnWorkerStateChange(const HostPort &addr, StatusCode status)
{
    if (status != K_CLIENT_WORKER_DISCONNECT) {
        return;  // Only handle connection failures
    }
    auto old = std::atomic_load(&brokenMap_);
    bool updated = false;
    do {
        auto newMap = std::make_shared<BrokenMap>(*old);
        auto now = std::chrono::steady_clock::now();
        for (auto it = newMap->begin(); it != newMap->end();) {
            if (now >= it->second) {
                it = newMap->erase(it);
            } else {
                ++it;
            }
        }
        (*newMap)[addr.ToString()] = now + BROKEN_TTL;
        updated =
            std::atomic_compare_exchange_weak(&brokenMap_, &old, std::shared_ptr<const BrokenMap>(std::move(newMap)));
        if (!updated) {
            // CAS failed: yield to reduce contention under high concurrent MarkBroken.
            std::this_thread::yield();
        }
    } while (!updated);
}

}  // namespace client
}  // namespace datasystem
