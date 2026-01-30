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

#include <string>

namespace datasystem {
struct NodeInfo {
    std::string nodeId;
    uint64_t availableMemory;
    bool isReady;
    uint64_t timestamp;

    NodeInfo() = default;

    NodeInfo(const std::string& id, int64_t memory, bool ready, int64_t currentTime = 0)
        : nodeId(id), availableMemory(memory), isReady(ready), timestamp(currentTime) {}

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