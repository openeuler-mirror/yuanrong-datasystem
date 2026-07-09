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
 * Description: The node info for available memory resource.
 */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_NODE_INFO_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_NODE_INFO_H

#include <cstdint>
#include <string>

namespace datasystem {
struct NodeInfo {
    std::string nodeId;
    uint64_t availableMemory = 0;
    bool isReady = false;
    uint64_t timestamp = 0;
    uint64_t usedMemory = 0;
    uint64_t memoryCapacity = 0;
    uint64_t memoryLimit = 0;

    NodeInfo() = default;

    NodeInfo(const std::string& id, int64_t memory, bool ready, int64_t currentTime = 0)
        : nodeId(id),
          availableMemory(memory),
          isReady(ready),
          timestamp(currentTime),
          usedMemory(0),
          memoryCapacity(0),
          memoryLimit(0)
    {
    }

    NodeInfo(const std::string& id, int64_t memory, bool ready, int64_t currentTime, uint64_t used,
             uint64_t capacity, uint64_t limit)
        : nodeId(id),
          availableMemory(memory),
          isReady(ready),
          timestamp(currentTime),
          usedMemory(used),
          memoryCapacity(capacity),
          memoryLimit(limit)
    {
    }

    bool operator<(const NodeInfo& other) const
    {
        // If isReady is false, it's less than the nodeInfo which isReady is true;
        if (isReady != other.isReady) {
            return !isReady;
        }
        return availableMemory < other.availableMemory;
    }
};
}  // namespace datasystem
#endif
