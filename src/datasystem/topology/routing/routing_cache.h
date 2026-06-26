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
 * Description: Request-local R0 routing cache.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_CACHE_H
#define DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_CACHE_H

#include <cstddef>
#include <unordered_map>

#include "datasystem/topology/routing/placement_types.h"

namespace datasystem {
namespace topology {

struct RouteCacheKey {
    int64_t version = -1;
    uint32_t objectHash = 0;

    bool operator==(const RouteCacheKey &other) const
    {
        return version == other.version && objectHash == other.objectHash;
    }
};

struct RouteCacheKeyHash {
    size_t operator()(const RouteCacheKey &key) const
    {
        return std::hash<int64_t>()(key.version) ^ std::hash<uint32_t>()(key.objectHash);
    }
};

class IRoutingCache {
public:
    virtual ~IRoutingCache() = default;

    /**
     * @brief Lookup a route decision by snapshot version and key hash.
     * @param[in] key Cache key.
     * @param[out] decision Cached route decision.
     * @return True if a decision is cached.
     */
    virtual bool Lookup(const RouteCacheKey &key, RouteDecision &decision) = 0;

    /**
     * @brief Store a route decision for the current request only.
     * @param[in] key Cache key.
     * @param[in] decision Route decision.
     */
    virtual void Store(const RouteCacheKey &key, const RouteDecision &decision) = 0;
};

class RoutingCache final : public IRoutingCache {
public:
    explicit RoutingCache(size_t reserve = 0);
    ~RoutingCache() override = default;
    bool Lookup(const RouteCacheKey &key, RouteDecision &decision) override;
    void Store(const RouteCacheKey &key, const RouteDecision &decision) override;

private:
    std::unordered_map<RouteCacheKey, RouteDecision, RouteCacheKeyHash> cache_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_CACHE_H
