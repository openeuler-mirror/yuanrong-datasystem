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

/** Description: Client-side latency-aware worker selection using UB health snapshots. */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_ROUTING_CLIENT_READ_BANDWIDTH_SCHEDULER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_ROUTING_CLIENT_READ_BANDWIDTH_SCHEDULER_H

#include <cstdint>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/client/object_cache/routing/ub_routing_health.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

class ClientReadBandwidthScheduler {
public:
    struct Config {
        bool enabled{ false };
        uint32_t latencyClientTableSize{ 16384 };
        uint64_t latencyCandidateRefreshMs{ 60000 };
        uint64_t latencyWorkerRecycleCheckMs{ 60000 };
        uint64_t latencyWorkerInactiveRecycleMs{ 60000 };
        uint32_t latencyAvailabilityRetryCount{ 3 };
        uint64_t initialP50Us{ 200 };
        uint64_t initialP99Us{ 500 };
        uint64_t latencyClientStaleMs{ 100 };
        uint64_t latencyNonAffinityPenaltyUs{ 200 };
        uint64_t latencyStarvationProtectMs{ 2000 };
        uint64_t latencyHardLimitUs{ 20000 };
        uint32_t latencyCutInGuardPermille{ 600 };
        uint32_t latencyWeightScale{ 1000000 };
        uint64_t latencyWeightReferenceUs{ 1000 };
        std::string clientSalt{};

        static Config FromFlags();
    };

    struct WorkerStatus {
        uint32_t p50Ns{ 0 };
        uint32_t p99Ns{ 0 };
        uint64_t latencyVersion{ 0 };
        uint64_t affinityWeight{ 0 };
        uint64_t nonAffinityWeight{ 0 };
        bool exists{ false };
        bool fresh{ false };
        bool currentCandidate{ false };
    };

    explicit ClientReadBandwidthScheduler(Config config);
    ~ClientReadBandwidthScheduler();

    ClientReadBandwidthScheduler(const ClientReadBandwidthScheduler &) = delete;
    ClientReadBandwidthScheduler &operator=(const ClientReadBandwidthScheduler &) = delete;

    bool Enabled() const noexcept;

    // Observe stores latency only. UB health is read from the routing snapshot
    // during each selection and is never maintained by this scheduler.
    void Observe(const HostPort &worker, uint32_t p50Ns, uint32_t p99Ns, uint64_t latencyVersion,
                 const std::string &callFrom);

    // This is a control-path topology refresh. It does not aggregate weights.
    void RefreshCandidates(const std::vector<HostPort> &availableWorkers);

    // The scheduler does not cache UB faults. Pass the same immutable health
    // snapshot used by selection so the reported weights include live faults.
    bool GetWorkerStatus(const HostPort &worker,
                         const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot,
                         WorkerStatus &out) const;

    bool ShouldKeepAffinity(const HostPort &affinityWorker, const std::string &requestKey,
                            const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot,
                            std::uint64_t &taskId);

    bool SelectWorkerFast(const std::string &requestKey, const std::vector<HostPort> &exclude,
                          const HostPort &preferredWorker, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                          const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot,
                          std::uint64_t taskId, HostPort &selected, WorkerAccessAction action);

    bool SelectWorker(const std::string &requestKey, const std::vector<std::shared_ptr<IWorkerFilter>> &filters,
                      const std::vector<HostPort> &exclude, const HostPort &preferredWorker,
                      const std::shared_ptr<const UbRoutingHealthSnapshot> &healthSnapshot, std::uint64_t taskId,
                      HostPort &selected, WorkerAccessAction action);

    // Initialization-only setting. Call exactly once, in nanoseconds, before
    // Observe(), RefreshCandidates(), or any selection can run concurrently.
    void SetCutInGuardNs(std::uint64_t latencyHardLimitMs);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_ROUTING_CLIENT_READ_BANDWIDTH_SCHEDULER_H
