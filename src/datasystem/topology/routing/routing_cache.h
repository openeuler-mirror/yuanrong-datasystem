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
#include <functional>
#include <string>
#include <unordered_map>

#include "datasystem/topology/model/topology_types.h"
#include "datasystem/topology/routing/placement_types.h"

namespace datasystem {
namespace topology {

struct RouteCacheKey {
    int64_t version = -1;
    PlacementUnit unit;

    bool operator==(const RouteCacheKey &other) const
    {
        return version == other.version && unit.algorithmId == other.unit.algorithmId
               && unit.unitType == other.unit.unitType && unit.opaqueUnit == other.unit.opaqueUnit;
    }
};

struct RouteCacheKeyHash {
    size_t operator()(const RouteCacheKey &key) const
    {
        size_t seed = std::hash<int64_t>()(key.version);
        HashCombine(seed, std::hash<std::string>()(key.unit.algorithmId));
        HashCombine(seed, std::hash<std::string>()(key.unit.unitType));
        HashCombine(seed, std::hash<std::string>()(key.unit.opaqueUnit));
        return seed;
    }

private:
    static constexpr size_t HASH_COMBINE_MAGIC = 0x9e3779b9;
    static constexpr size_t HASH_COMBINE_LEFT_SHIFT = 6;
    static constexpr size_t HASH_COMBINE_RIGHT_SHIFT = 2;

    static void HashCombine(size_t &seed, size_t value)
    {
        seed ^= value + HASH_COMBINE_MAGIC + (seed << HASH_COMBINE_LEFT_SHIFT) + (seed >> HASH_COMBINE_RIGHT_SHIFT);
    }
};

class IRoutingCache {
public:
    virtual ~IRoutingCache() = default;

    /**
     * @brief Lookup a route decision by snapshot version and algorithm placement unit.
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
