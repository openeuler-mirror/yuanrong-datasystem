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

/** Description: Defines the client transport selection policy. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H
#define DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "datasystem/client/object_cache/transport/transport_kind.h"
#include "datasystem/common/util/net_util.h"

#include <bthread/rwlock.h>

namespace datasystem {
namespace client {
class TransportAdvisor {
public:
    TransportAdvisor() = default;
    virtual ~TransportAdvisor() = default;

    /**
     * @brief Suggest a transport hint for the target worker.
     * @param[in] workerAddr Target worker address.
     * @return The suggested TransportHint.
     */
    virtual TransportHint GetTransportHint(const HostPort &workerAddr) const;

    /**
     * @brief Return the ordered fallback transports for an initial candidate.
     * @param[in] initial Initial transport hint.
     * @return UB then TCP for a same-host SHM candidate when URMA is enabled; otherwise TCP only.
     */
    std::vector<TransportHint> GetFallbackHints(TransportHint initial) const;

    /**
     * @brief Update the set of same-host workers eligible for SHM (from WorkerSnapshot::shmCandidateAddrs).
     * Called when the routing topology changes. Thread-safe.
     */
    void SetShmCandidateWorkers(const std::vector<HostPort> &workers);

    /** @brief Stop selecting SHM for a draining worker and allow one refresh per published snapshot. */
    bool ObserveDrainingShmFailure(const HostPort &workerAddr);

private:
    mutable bthread::RWLock mtx_;
    std::unordered_set<HostPort> shmCandidateWorkers_;
    // Protected by mtx_; refresh generations advance monotonically outside the lock.
    uint64_t snapshotGeneration_{ 1 };
    std::atomic<uint64_t> drainingRefreshGeneration_{ 0 };
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H
