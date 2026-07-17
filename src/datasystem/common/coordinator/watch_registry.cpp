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

#include "datasystem/common/util/status_helper.h"

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
    int64_t watchId = 0;
    bool created = false;
    (void)Register(key, rangeEnd, watcherAddr, "", watchId, created);
    return watchId;
}

Status WatchRegistry::Register(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                               const std::string &registrationId, int64_t &watchId, bool &created)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!registrationId.empty()) {
        auto registered = watchIdsByRegistrationId_.find(registrationId);
        if (registered != watchIdsByRegistrationId_.end()) {
            auto watcher = watchers_.find(registered->second);
            CHECK_FAIL_RETURN_STATUS(watcher != watchers_.end() && watcher->second->watcherAddr == watcherAddr,
                                     K_INVALID, "watch registration ID belongs to another watcher");
            const bool sameRange = std::any_of(watchRanges_.begin(), watchRanges_.end(), [&](const auto &range) {
                return IsSameRange(range, key, rangeEnd) && range.watchIds.count(registered->second) != 0;
            });
            CHECK_FAIL_RETURN_STATUS(sameRange, K_INVALID,
                                     "watch registration ID belongs to another key range");
            watchId = registered->second;
            created = false;
            return Status::OK();
        }
    }
    auto entry = std::make_shared<WatcherEntry>();
    entry->watcherAddr = watcherAddr;
    entry->registrationId = registrationId;
    entry->active = true;
    watchId = nextWatchId_.fetch_add(1);
    entry->watchId = watchId;
    watchers_[watchId] = entry;
    if (!registrationId.empty()) {
        watchIdsByRegistrationId_[registrationId] = watchId;
    }

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
    created = true;
    return Status::OK();
}

Status WatchRegistry::Cancel(int64_t watchId, const std::string &watcherAddr)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = watchers_.find(watchId);
    if (it == watchers_.end()) {
        return Status(StatusCode::K_NOT_FOUND, "watch not found");
    }
    if (!watcherAddr.empty() && it->second->watcherAddr != watcherAddr) {
        return Status(StatusCode::K_INVALID, "watcher address does not match watch ID");
    }

    it->second->active = false;
    if (!it->second->registrationId.empty()) {
        watchIdsByRegistrationId_.erase(it->second->registrationId);
    }

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
