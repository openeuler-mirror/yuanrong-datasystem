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
#include "datasystem/topology/routing/routing_cache.h"

namespace datasystem {
namespace topology {

RoutingCache::RoutingCache(size_t reserve)
{
    cache_.reserve(reserve);
}

bool RoutingCache::Lookup(const RouteCacheKey &key, RouteDecision &decision)
{
    auto iter = cache_.find(key);
    if (iter == cache_.end()) {
        return false;
    }
    decision = iter->second;
    return true;
}

void RoutingCache::Store(const RouteCacheKey &key, const RouteDecision &decision)
{
    cache_[key] = decision;
}

}  // namespace topology
}  // namespace datasystem
