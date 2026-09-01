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
 * Description: BrokenFilter - counts consecutive K_CLIENT_WORKER_DISCONNECT signals per worker;
 * only after EVICT_CONSECUTIVE_FAILURES (within FAILURE_BURST_WINDOW) is it marked broken for
 * BROKEN_TTL. A single blip no longer evicts (prevents code=37); a dead peer reaches the
 * threshold quickly. Recovery is lazy TTL.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_BROKEN_FILTER_H
#define DATASYSTEM_CLIENT_ROUTING_BROKEN_FILTER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

class BrokenFilter : public IWorkerFilter {
public:
    BrokenFilter();
    ~BrokenFilter() override = default;

    bool IsAvailable(const HostPort &addr) const override;
    void OnWorkerStateChange(const HostPort &addr, StatusCode status) override;
    void OnHashRingUpdated(const ::datasystem::ClusterTopologyPb &ring) override;

private:
    // Per-worker consecutive-failure count (within FAILURE_BURST_WINDOW) and broken-until (TTL).
    struct WorkerHealth {
        uint32_t consecutiveFailures{ 0 };
        std::chrono::steady_clock::time_point windowStart{};
        std::chrono::steady_clock::time_point brokenUntil{};
    };
    using HealthMap = std::unordered_map<std::string, WorkerHealth>;
    std::shared_ptr<const HealthMap> healthMap_;
    static constexpr std::chrono::seconds BROKEN_TTL{ 3 };
    static constexpr uint32_t EVICT_CONSECUTIVE_FAILURES{ 100 };
    static constexpr std::chrono::seconds FAILURE_BURST_WINDOW{ 5 };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_BROKEN_FILTER_H
