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
                                          ::datasystem::ClusterTopologyPb &ring, uint64_t &newVersion, bool &changed,
                                          std::unordered_map<std::string, std::string> &hostIdMap)>;
    using TimedFetchRpc =
        std::function<Status(const HostPort &workerAddr, uint64_t currentVersion, ::datasystem::ClusterTopologyPb &ring,
                             uint64_t &newVersion, bool &changed,
                             std::unordered_map<std::string, std::string> &hostIdMap, int32_t timeoutMs)>;
    using RingUpdateHook = std::function<Status(uint64_t newVersion,
                                                const ::datasystem::ClusterTopologyPb &ring,
                                                const std::unordered_map<std::string, std::string> &hostIdMap,
                                                bool epochResetConfirmed)>;
    using WaitFn = std::function<void(std::condition_variable &cv, std::unique_lock<std::mutex> &lock,
                                      std::chrono::milliseconds duration,
                                      const std::function<bool()> &wakePredicate)>;
    // Emergency address source for the refresh loop when every worker in the probe list is unreachable
    // (e.g. a full worker restart with new addresses). Queried at most once per refresh round.
    using ProbeListFallback = std::function<std::vector<HostPort>()>;

    HashRingRefresher(std::shared_ptr<WorkerRouter> router, FetchRpc fetchRpc, RingUpdateHook ringUpdateHook = {},
                      WaitFn waitFn = {}, ProbeListFallback probeFallback = {});
    HashRingRefresher(std::shared_ptr<WorkerRouter> router, TimedFetchRpc fetchRpc, RingUpdateHook ringUpdateHook = {},
                      WaitFn waitFn = {}, ProbeListFallback probeFallback = {});
    ~HashRingRefresher();

    Status InitialFetch(const HostPort &initialWorkerAddr);
    Status StartPeriodicRefresh(int64_t intervalMs);
    void Stop();
    bool ForceRefresh();

    std::string GetHostIdsDigest(uint64_t version);

    // Merge externally learned worker addresses (e.g. the post-switch bound worker) into the probe list.
    // Deduplicates and rotates the next probe round to the first newly added address. Returns the number
    // of addresses actually appended. The list stays bounded because every published ring rebuilds it.
    size_t MergeProbeWorkers(const std::vector<HostPort> &workers);

    // Cover the online 3s isolation target plus reconciliation and publication margin. Public for
    // callers that rate-limit their own forced-refresh triggers against the same window.
    static constexpr int64_t FORCED_REFRESH_WINDOW_MS = 6'000;

private:
    friend class HashRingRefresherTestPeer;

    void RefreshLoop();
    std::vector<HostPort> BeginRefreshRound(size_t &startIndex);
    Status DoRefresh(bool stopAware);
    Status ProbeRound(const std::vector<HostPort> &workers, size_t startIndex, size_t probeCount, bool stopAware,
                      int32_t timeoutMs, bool &reachedWorker);
    Status DoRefreshSafely(bool stopAware);
    Status PublishHashRing(uint64_t newVersion, ::datasystem::ClusterTopologyPb &&ring,
                           std::unordered_map<std::string, std::string> &&hostIdMap, bool epochResetConfirmed);
    void UpdateWorkerList(const ::datasystem::ClusterTopologyPb &ring);
    static std::string BuildRingDigest(const ::datasystem::ClusterTopologyPb &ring);
    bool RecordLowerVersionAndCheckConfirmation(const HostPort &worker, uint64_t newVersion,
                                                const std::string &digest);
    bool TryPublishEpochReset(const HostPort &worker, uint64_t requestedVersion, uint64_t newVersion,
                              ::datasystem::ClusterTopologyPb &ring,
                              std::unordered_map<std::string, std::string> &hostIdMap, Status &result);
    static constexpr int64_t FORCED_REFRESH_RETRY_INTERVAL_MS = 250;
    static constexpr int32_t BACKGROUND_REFRESH_RPC_TIMEOUT_MS = 250;
    static constexpr size_t MAX_BACKGROUND_PROBES_PER_ROUND = 4;
    // Foreground refresh (initial fetch) keeps its own window constant: same bound as background
    // today, but the two paths tighten independently.
    static constexpr size_t MAX_FOREGROUND_PROBES_PER_ROUND = 4;
    size_t ProbeListSize();

    // Lower-version responses observed in the current or previous refresh round, keyed by worker.
    // Two different workers reporting the same lower ring within the window confirm an epoch reset.
    struct LowerVersionObservation {
        uint64_t version{ 0 };
        std::string digest;
        uint64_t round{ 0 };
    };
    std::mutex lowerVersionObsMutex_;
    std::unordered_map<std::string, LowerVersionObservation> lowerVersionObs_;
    uint64_t refreshRound_{ 0 };

    std::shared_ptr<WorkerRouter> router_;
    TimedFetchRpc fetchRpc_;
    RingUpdateHook ringUpdateHook_;
    WaitFn waitFn_;
    ProbeListFallback probeFallback_;

    std::mutex workerListMutex_;
    std::vector<HostPort> workerList_;
    size_t nextWorkerIndex_{ 0 };
    std::atomic<uint64_t> currentVersion_{ 0 };
    // Published with currentVersion_ under workerListMutex_, only after both routing consumers accept the snapshot.
    std::string hostIdsDigest_;

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
