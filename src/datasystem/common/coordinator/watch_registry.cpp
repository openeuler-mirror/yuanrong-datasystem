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
 * Description: Watch registry with grouped key/range matching.
 */
#include "datasystem/common/coordinator/watch_registry.h"

#include <algorithm>
#include <mutex>

namespace datasystem {
namespace {
bool IsSameRange(const WatchRange &watchRange, const std::string &key, const std::string &rangeEnd)
{
    return watchRange.key == key && watchRange.rangeEnd == rangeEnd;
}

bool IsKeyInRange(const std::string &key, const WatchRange &watchRange)
{
    if (watchRange.rangeEnd.empty()) {
        return watchRange.key == key;
    }
    return watchRange.key <= key && key < watchRange.rangeEnd;
}
}  // namespace

int64_t WatchRegistry::Register(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr)
{
    auto entry = std::make_shared<WatcherEntry>();
    entry->watcherAddr = watcherAddr;
    entry->active = true;

    std::unique_lock<std::shared_mutex> lock(mutex_);
    int64_t watchId = nextWatchId_.fetch_add(1);
    entry->watchId = watchId;
    watchers_[watchId] = entry;

    auto groupIt = std::find_if(
        watchRanges_.begin(), watchRanges_.end(),
        [&key, &rangeEnd](const WatchRange &watchRange) { return IsSameRange(watchRange, key, rangeEnd); });
    if (groupIt == watchRanges_.end()) {
        WatchRange watchRange;
        watchRange.key = key;
        watchRange.rangeEnd = rangeEnd;
        watchRange.watchIds.insert(watchId);
        watchRanges_.push_back(std::move(watchRange));
    } else {
        groupIt->watchIds.insert(watchId);
    }
    return watchId;
}

Status WatchRegistry::Cancel(int64_t watchId)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = watchers_.find(watchId);
    if (it == watchers_.end()) {
        return Status(StatusCode::K_NOT_FOUND, "watch not found");
    }

    it->second->active = false;

    for (auto iter = watchRanges_.begin(); iter != watchRanges_.end();) {
        iter->watchIds.erase(watchId);
        if (iter->watchIds.empty()) {
            iter = watchRanges_.erase(iter);
        } else {
            ++iter;
        }
    }

    watchers_.erase(it);
    return Status::OK();
}

void WatchRegistry::MatchWatchers(const std::string &key, std::vector<std::shared_ptr<WatcherEntry>> &matched)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto &watchRange : watchRanges_) {
        if (!IsKeyInRange(key, watchRange)) {
            continue;
        }
        for (auto watchId : watchRange.watchIds) {
            auto it = watchers_.find(watchId);
            if (it != watchers_.end() && it->second->active) {
                matched.push_back(it->second);
            }
        }
    }
}
}  // namespace datasystem
