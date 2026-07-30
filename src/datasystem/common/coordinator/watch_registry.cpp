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
 * Description: Watch registry with exact-key indexing and grouped range matching.
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
    return watchRange.key <= key && key < watchRange.rangeEnd;
}

bool IsInTableScope(const std::string &key, const std::string &table)
{
    return key == table || (key.size() > table.size() && key.compare(0, table.size(), table) == 0
                            && key[table.size()] == '/');
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
            const auto scope = watchScopesById_.find(registered->second);
            const bool sameRange = scope != watchScopesById_.end()
                                   && scope->second.first == key && scope->second.second == rangeEnd;
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
    watchScopesById_[watchId] = { key, rangeEnd };

    if (rangeEnd.empty()) {
        exactWatchIdsByKey_[key].insert(watchId);
        created = true;
        return Status::OK();
    }
    auto groupIt = std::find_if(
        rangeWatches_.begin(), rangeWatches_.end(),
        [&key, &rangeEnd](const WatchRange &watchRange) { return IsSameRange(watchRange, key, rangeEnd); });
    if (groupIt == rangeWatches_.end()) {
        WatchRange watchRange;
        watchRange.key = key;
        watchRange.rangeEnd = rangeEnd;
        watchRange.watchIds.insert(watchId);
        rangeWatches_.push_back(std::move(watchRange));
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

    auto scope = watchScopesById_.find(watchId);
    if (scope != watchScopesById_.end() && scope->second.second.empty()) {
        auto exact = exactWatchIdsByKey_.find(scope->second.first);
        if (exact != exactWatchIdsByKey_.end()) {
            exact->second.erase(watchId);
            if (exact->second.empty()) {
                exactWatchIdsByKey_.erase(exact);
            }
        }
    } else {
        for (auto iter = rangeWatches_.begin(); iter != rangeWatches_.end();) {
            iter->watchIds.erase(watchId);
            if (iter->watchIds.empty()) {
                iter = rangeWatches_.erase(iter);
            } else {
                ++iter;
            }
        }
    }

    watchScopesById_.erase(watchId);
    watchers_.erase(it);
    return Status::OK();
}

void WatchRegistry::MatchWatchers(const std::string &key, std::vector<std::shared_ptr<WatcherEntry>> &matched)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto exact = exactWatchIdsByKey_.find(key);
    if (exact != exactWatchIdsByKey_.end()) {
        for (auto watchId : exact->second) {
            auto watcher = watchers_.find(watchId);
            if (watcher != watchers_.end() && watcher->second->active) {
                matched.push_back(watcher->second);
            }
        }
    }
    for (const auto &watchRange : rangeWatches_) {
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

bool WatchRegistry::IsWatchInScopes(int64_t watchId, const std::vector<std::string> &tableScopes) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto scope = watchScopesById_.find(watchId);
    if (scope == watchScopesById_.end()) {
        return false;
    }
    return std::any_of(tableScopes.begin(), tableScopes.end(),
                       [&](const auto &table) { return IsInTableScope(scope->second.first, table); });
}
}  // namespace datasystem
