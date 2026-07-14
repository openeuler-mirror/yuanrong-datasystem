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
 * Description: Routing - the single facade for the routing module.
 * Combines WorkerRouter + HashRingRefresher.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_ROUTING_H
#define DATASYSTEM_CLIENT_ROUTING_ROUTING_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/client/routing/hash_ring_refresher.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

enum class DataPlacementPolicy {
    PREFERRED_SAME_NODE,  // Prefer a same-node worker, then fall back to the metadata owner.
    REQUIRED_SAME_NODE,   // Select only from same-node workers.
    PREFERRED_META_OWNER,  // Prefer the metadata owner selected by the hash ring.
};

class Routing {
public:
    Routing(std::shared_ptr<WorkerRouter> router, std::shared_ptr<HashRingRefresher> refresher,
            int64_t refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS);
    ~Routing();

    Status Init(const std::string &hostId, const HostPort &initialWorkerAddr);

    Status SelectWorker(const std::string &key, SelectStrategy strategy, HostPort &worker,
                         const std::vector<HostPort> &exclude = {});

    Status SelectWorkers(const std::vector<std::string> &keys, SelectStrategy strategy,
                          std::unordered_map<HostPort, std::vector<std::string>> &groups);

    void UpdateState(const HostPort &addr, StatusCode status);

    void Shutdown();

private:
    static constexpr int64_t DEFAULT_REFRESH_INTERVAL_MS = 5'000;
    std::shared_ptr<WorkerRouter> router_;
    std::shared_ptr<HashRingRefresher> refresher_;
    int64_t refreshIntervalMs_;
    std::atomic<bool> initialized_{ false };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_ROUTING_H
