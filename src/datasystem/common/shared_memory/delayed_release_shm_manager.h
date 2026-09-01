/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_DELAYED_RELEASE_SHM_MANAGER_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_DELAYED_RELEASE_SHM_MANAGER_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "datasystem/common/shared_memory/shm_unit.h"

namespace datasystem {

constexpr int64_t DEFAULT_SHM_DELAY_RELEASE_MS = 128;
constexpr int DELAY_RELEASE_LOG_INTERVAL_SEC = 10;

class DelayedReleaseShmManager {
public:
    ~DelayedReleaseShmManager();

    DelayedReleaseShmManager(const DelayedReleaseShmManager &) = delete;
    DelayedReleaseShmManager &operator=(const DelayedReleaseShmManager &) = delete;
    DelayedReleaseShmManager(DelayedReleaseShmManager &&) = delete;
    DelayedReleaseShmManager &operator=(DelayedReleaseShmManager &&) = delete;

    static DelayedReleaseShmManager &Instance();

    void Add(const std::shared_ptr<ShmUnit> &shmUnit, std::chrono::milliseconds delay);

private:
    DelayedReleaseShmManager();

    struct DelayedReleaseEntry {
        std::chrono::steady_clock::time_point releaseTime;
        std::shared_ptr<ShmUnit> shmUnit;
    };

    struct EntryCompare {
        bool operator()(const DelayedReleaseEntry &lhs, const DelayedReleaseEntry &rhs) const
        {
            return lhs.releaseTime > rhs.releaseTime;
        }
    };

    void Run();
    void Stop();

    std::mutex mutex_;
    std::condition_variable cv_;
    std::priority_queue<DelayedReleaseEntry, std::vector<DelayedReleaseEntry>, EntryCompare> delayReleaseQueue_;
    uint64_t pendingBytes_{ 0 };
    bool stopping_{ false };
    std::thread releaseThread_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_DELAYED_RELEASE_SHM_MANAGER_H
