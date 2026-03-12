/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: arena group key.
 */

#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_ARENA_GROUP_KEY_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_ARENA_GROUP_KEY_H

#include <functional>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace memory {
enum class CacheType : int {
    MEMORY = 0,
    DISK = 1,
    DEV_DEVICE = 2,
    DEV_HOST = 3,
    UB_TRANSPORT = 4,
};

static const std::unordered_map<CacheType, std::string> CACHE_TYPE_STR = { { CacheType::MEMORY, "Shared memory" },
                                                                           { CacheType::DISK, "Shared disk" },
                                                                           { CacheType::DEV_DEVICE, "Device pin mem" },
                                                                           { CacheType::DEV_HOST, "DevHost pin mem" } };

struct ArenaGroupKey {
    std::string tenantId;
    CacheType type;
    bool operator==(const ArenaGroupKey &other) const
    {
        return tenantId == other.tenantId && type == other.type;
    }
};

struct AllocatorFuncRegister {
    std::function<Status(void **, size_t)> createFunc;
    std::function<Status(void *, size_t)> destroyFunc;
};

}  // namespace memory
}  // namespace datasystem

namespace std {
template <>
struct hash<datasystem::memory::ArenaGroupKey> {
    size_t operator()(const datasystem::memory::ArenaGroupKey &key) const
    {
        size_t h1 = std::hash<std::string>{}(key.tenantId);
        size_t h2 = std::hash<int>{}(static_cast<int>(key.type));
        return h1 ^ (h2 << 1);
    }
};
}  // namespace std

#endif