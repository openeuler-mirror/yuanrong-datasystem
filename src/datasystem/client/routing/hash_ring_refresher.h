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
 * Description: HashRingRefresher - background thread that periodically fetches
 * hash ring via GetHashRing RPC and writes to WorkerRouter.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_HASH_RING_REFRESHER_H
#define DATASYSTEM_CLIENT_ROUTING_HASH_RING_REFRESHER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class HashRingRefresher {
public:
    using FetchRpc = std::function<Status(const HostPort &workerAddr, uint64_t currentVersion,
                                          ::datasystem::ClusterTopologyPb &ring, std::string &masterAddress,
                                          uint64_t &newVersion, bool &changed,
                                          std::unordered_map<std::string, std::string> &hostIdMap)>;
    using RingUpdateHook = std::function<Status(uint64_t newVersion,
                                                const ::datasystem::ClusterTopologyPb &ring,
                                                const std::unordered_map<std::string, std::string> &hostIdMap)>;
    using WaitFn = std::function<void(std::condition_variable &cv, std::unique_lock<std::mutex> &lock,
                                      std::chrono::milliseconds duration,
                                      const std::function<bool()> &wakePredicate)>;

    HashRingRefresher(std::shared_ptr<WorkerRouter> router, FetchRpc fetchRpc, RingUpdateHook ringUpdateHook = {},
                      WaitFn waitFn = {});
    ~HashRingRefresher();

    Status InitialFetch(const HostPort &initialWorkerAddr);
    Status StartPeriodicRefresh(int64_t intervalMs);
    void Stop();
    void ForceRefresh();

private:
    void RefreshLoop();
    Status DoRefresh();
    void UpdateWorkerList(const ::datasystem::ClusterTopologyPb &ring);

    // Keep force refresh active for about 3s without turning worker disconnects into tight polling.
    static constexpr int FORCED_REFRESH_RETRY_COUNT = 6;
    static constexpr int64_t FORCED_REFRESH_RETRY_INTERVAL_MS = 500;

    std::shared_ptr<WorkerRouter> router_;
    FetchRpc fetchRpc_;
    RingUpdateHook ringUpdateHook_;
    WaitFn waitFn_;

    std::mutex workerListMutex_;
    std::vector<HostPort> workerList_;
    std::atomic<uint64_t> currentVersion_{ 0 };

    std::atomic<bool> running_{ false };
    std::atomic<bool> forceRefresh_{ false };
    // Remaining retry slots after a worker disconnect forces topology refresh.
    std::atomic<int> forceRefreshBudget_{ 0 };
    std::thread refreshThread_;
    std::mutex cvMutex_;
    std::condition_variable cv_;
    int64_t intervalMs_{ 5000 };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_HASH_RING_REFRESHER_H
