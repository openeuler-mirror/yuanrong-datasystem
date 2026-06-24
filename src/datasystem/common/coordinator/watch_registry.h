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
 * Description: Watch registry with flat map and linear scan matching.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_WATCH_REGISTRY_H
#define DATASYSTEM_COMMON_COORDINATOR_WATCH_REGISTRY_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {
struct WatcherEntry {
    int64_t watchId = 0;
    std::string watcherAddr;
    bool active = true;
};

struct WatchRange {
    std::string key;
    std::string rangeEnd;
    std::unordered_set<int64_t> watchIds;
};

class WatchRegistry {
public:
    WatchRegistry() = default;
    ~WatchRegistry() = default;

    /**
     * @brief Register a watcher for a key or range.
     * @param[in] key Start key.
     * @param[in] rangeEnd End key (empty for single key watch).
     * @param[in] watcherAddr Address to notify.
     * @return Assigned watch ID.
     */
    int64_t Register(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr);

    /**
     * @brief Cancel a watcher.
     * @param[in] watchId The watch ID to cancel.
     * @return Status of the operation.
     */
    Status Cancel(int64_t watchId);

    /**
     * @brief Find all watchers matching a given mutation key.
     * @param[in] key The mutated key.
     * @param[out] matched Output matching watcher entries.
     */
    void MatchWatchers(const std::string &key, std::vector<std::shared_ptr<WatcherEntry>> &matched);

private:
    std::unordered_map<int64_t, std::shared_ptr<WatcherEntry>> watchers_;
    std::vector<WatchRange> watchRanges_;
    std::atomic<int64_t> nextWatchId_{ 1 };
    mutable std::shared_mutex mutex_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_WATCH_REGISTRY_H
