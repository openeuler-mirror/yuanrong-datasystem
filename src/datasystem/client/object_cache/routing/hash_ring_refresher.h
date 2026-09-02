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

#include "datasystem/client/object_cache/routing/worker_router.h"
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
    using TimedFetchRpc =
        std::function<Status(const HostPort &workerAddr, uint64_t currentVersion, ::datasystem::ClusterTopologyPb &ring,
                             std::string &masterAddress, uint64_t &newVersion, bool &changed,
                             std::unordered_map<std::string, std::string> &hostIdMap, int32_t timeoutMs)>;
    using RingUpdateHook = std::function<Status(uint64_t newVersion,
                                                const ::datasystem::ClusterTopologyPb &ring,
                                                const std::unordered_map<std::string, std::string> &hostIdMap)>;
    using WaitFn = std::function<void(std::condition_variable &cv, std::unique_lock<std::mutex> &lock,
                                      std::chrono::milliseconds duration,
                                      const std::function<bool()> &wakePredicate)>;

    HashRingRefresher(std::shared_ptr<WorkerRouter> router, FetchRpc fetchRpc, RingUpdateHook ringUpdateHook = {},
                      WaitFn waitFn = {});
    HashRingRefresher(std::shared_ptr<WorkerRouter> router, TimedFetchRpc fetchRpc, RingUpdateHook ringUpdateHook = {},
                      WaitFn waitFn = {});
    ~HashRingRefresher();

    Status InitialFetch(const HostPort &initialWorkerAddr);
    Status StartPeriodicRefresh(int64_t intervalMs);
    void Stop();
    bool ForceRefresh();

private:
    void RefreshLoop();
    Status DoRefresh(bool stopAware);
    Status PublishHashRing(uint64_t newVersion, ::datasystem::ClusterTopologyPb &&ring,
                           std::unordered_map<std::string, std::string> &&hostIdMap);
    void UpdateWorkerList(const ::datasystem::ClusterTopologyPb &ring);

    // Cover the online 3s isolation target plus reconciliation and publication margin.
    static constexpr int64_t FORCED_REFRESH_WINDOW_MS = 6'000;
    static constexpr int64_t FORCED_REFRESH_RETRY_INTERVAL_MS = 250;
    static constexpr int32_t BACKGROUND_REFRESH_RPC_TIMEOUT_MS = 250;
    static constexpr size_t MAX_BACKGROUND_PROBES_PER_ROUND = 4;

    std::shared_ptr<WorkerRouter> router_;
    TimedFetchRpc fetchRpc_;
    RingUpdateHook ringUpdateHook_;
    WaitFn waitFn_;

    std::mutex workerListMutex_;
    std::vector<HostPort> workerList_;
    size_t nextWorkerIndex_{ 0 };
    std::atomic<uint64_t> currentVersion_{ 0 };

    std::atomic<bool> running_{ false };
    std::atomic<bool> forceRefresh_{ false };
    // Steady-clock deadline extended by repeated failures; zero means inactive.
    std::atomic<int64_t> forceRefreshDeadlineMs_{ 0 };
    std::thread refreshThread_;
    std::mutex cvMutex_;
    std::condition_variable cv_;
    int64_t intervalMs_{ 5000 };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_HASH_RING_REFRESHER_H
