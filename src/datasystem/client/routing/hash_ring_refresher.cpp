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

#include "datasystem/client/routing/hash_ring_refresher.h"

#include <utility>

namespace datasystem {
namespace client {

HashRingRefresher::HashRingRefresher(std::shared_ptr<WorkerRouter> router, FetchRpc fetchRpc)
    : router_(std::move(router)), fetchRpc_(std::move(fetchRpc)) {}

HashRingRefresher::~HashRingRefresher()
{
    Stop();
}

Status HashRingRefresher::InitialFetch(const HostPort &initialWorkerAddr)
{
    currentVersion_.store(0);
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workerList_.clear();
        workerList_.push_back(initialWorkerAddr);
    }
    return DoRefresh();
}

void HashRingRefresher::StartPeriodicRefresh(int64_t intervalMs)
{
    Stop();
    intervalMs_ = intervalMs;
    running_.store(true);
    refreshThread_ = std::thread(&HashRingRefresher::RefreshLoop, this);
}

void HashRingRefresher::Stop()
{
    if (!running_.exchange(false)) {
        return;
    }
    cv_.notify_all();
    if (refreshThread_.joinable()) {
        refreshThread_.join();
    }
}

void HashRingRefresher::ForceRefresh()
{
    forceRefresh_.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(cvMutex_);
    }
    cv_.notify_all();
}

Status HashRingRefresher::DoRefresh()
{
    // Clear forceRefresh_ at the start of refresh cycle, not after wait_for,
    // to avoid overwriting a new ForceRefresh that arrives during DoRefresh execution.
    forceRefresh_.store(false, std::memory_order_release);

    // Copy worker list under lock to avoid data race with InitialFetch
    std::vector<HostPort> workers;
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workers = workerList_;
    }

    for (const auto &worker : workers) {
        HashRingPb ring;
        std::string masterAddress;
        uint64_t newVersion = 0;
        bool changed = false;
        std::unordered_map<std::string, std::string> hostIdMap;

        Status st = fetchRpc_(worker, currentVersion_.load(), ring, masterAddress,
                              newVersion, changed, hostIdMap);
        if (st.IsError()) {
            continue;
        }

        if (changed) {
            currentVersion_.store(newVersion);
            auto ringPtr = std::make_shared<HashRingPb>(std::move(ring));
            auto hostIdMapPtr = std::make_shared<const std::unordered_map<std::string, std::string>>(
                std::move(hostIdMap));
            router_->UpdateHashRing(std::move(ringPtr), std::move(hostIdMapPtr));
        }
        return Status::OK();
    }
    return Status(K_NOT_FOUND, "No reachable worker for hash ring refresh");
}

void HashRingRefresher::RefreshLoop()
{
    while (running_.load()) {
        DoRefresh();

        std::unique_lock<std::mutex> lock(cvMutex_);
        // No predicate: ForceRefresh notify returns immediately (not swallowed by false predicate)
        cv_.wait_for(lock, std::chrono::milliseconds(intervalMs_));
        forceRefresh_.store(false, std::memory_order_release);
    }
}

}  // namespace client
}  // namespace datasystem
