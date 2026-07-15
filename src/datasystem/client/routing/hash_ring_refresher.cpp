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

#include <algorithm>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

HashRingRefresher::HashRingRefresher(std::shared_ptr<WorkerRouter> router, FetchRpc fetchRpc,
                                     RingUpdateHook ringUpdateHook)
    : router_(std::move(router)), fetchRpc_(std::move(fetchRpc)), ringUpdateHook_(std::move(ringUpdateHook))
{
}

HashRingRefresher::~HashRingRefresher()
{
    Stop();
}

Status HashRingRefresher::InitialFetch(const HostPort &initialWorkerAddr)
{
    RETURN_RUNTIME_ERROR_IF_NULL(router_);
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(fetchRpc_), K_INVALID, "Hash ring fetch callback must be set");
    CHECK_FAIL_RETURN_STATUS(!initialWorkerAddr.Empty(), K_INVALID, "Initial worker address must not be empty");
    currentVersion_.store(0);
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workerList_.clear();
        workerList_.push_back(initialWorkerAddr);
    }
    return DoRefresh();
}

Status HashRingRefresher::StartPeriodicRefresh(int64_t intervalMs)
{
    CHECK_FAIL_RETURN_STATUS(intervalMs > 0, K_INVALID, "Hash ring refresh interval must be positive");
    Stop();
    intervalMs_ = intervalMs;
    running_.store(true);
    refreshThread_ = std::thread(&HashRingRefresher::RefreshLoop, this);
    return Status::OK();
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
    cv_.notify_all();
}

Status HashRingRefresher::DoRefresh()
{
    // Copy worker list under lock to avoid data race with InitialFetch
    std::vector<HostPort> workers;
    {
        std::lock_guard<std::mutex> lock(workerListMutex_);
        workers = workerList_;
    }

    for (const auto &worker : workers) {
        ::datasystem::ClusterTopologyPb ring;
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
            if (ringUpdateHook_) {
                RETURN_IF_NOT_OK(ringUpdateHook_(newVersion, ring));
            }
            currentVersion_.store(newVersion);
            UpdateWorkerList(ring);
            auto ringPtr = std::make_shared<::datasystem::ClusterTopologyPb>(std::move(ring));
            auto hostIdMapPtr = std::make_shared<const std::unordered_map<std::string, std::string>>(
                std::move(hostIdMap));
            router_->UpdateHashRing(std::move(ringPtr), std::move(hostIdMapPtr));
        }
        return Status::OK();
    }
    return Status(K_NOT_FOUND, "No reachable worker for hash ring refresh");
}

void HashRingRefresher::UpdateWorkerList(const ::datasystem::ClusterTopologyPb &ring)
{
    std::vector<HostPort> updatedWorkers;
    updatedWorkers.reserve(ring.members_size());
    for (const auto &entry : ring.members()) {
        if (entry.second.state() != ::datasystem::MembershipPb::ACTIVE) {
            continue;
        }
        HostPort worker;
        if (worker.ParseString(entry.first).IsOk()) {
            updatedWorkers.emplace_back(std::move(worker));
        }
    }
    if (updatedWorkers.empty()) {
        return;
    }
    std::sort(updatedWorkers.begin(), updatedWorkers.end());
    std::lock_guard<std::mutex> lock(workerListMutex_);
    workerList_ = std::move(updatedWorkers);
}

void HashRingRefresher::RefreshLoop()
{
    while (running_.load()) {
        forceRefresh_.exchange(false, std::memory_order_acq_rel);
        DoRefresh();

        std::unique_lock<std::mutex> lock(cvMutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(intervalMs_), [this] {
            return !running_.load() || forceRefresh_.load(std::memory_order_acquire);
        });
    }
}

}  // namespace client
}  // namespace datasystem
