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

/** Description: Implements the client transport selection policy. */

#include "datasystem/client/transport/transport_advisor.h"

#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif

namespace datasystem {
namespace client {

TransportHint TransportAdvisor::GetTransportHint(const HostPort &workerAddr) const
{
    // Same-host workers (identified by hostId from routing topology) use shm fd-passing.
    // Use unordered_set<HostPort> directly so the hot path does not allocate a ToString() key.
    if (!workerAddr.Empty()) {
        bthread::RWLockRdGuard lk(mtx_);
        if (shmCandidateWorkers_.count(workerAddr) > 0) {
            return TransportHint::SHM_CANDIDATE;
        }
    }
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        return TransportHint::UB_CANDIDATE;
    }
#endif
    return TransportHint::TCP_ONLY;
}

std::vector<TransportHint> TransportAdvisor::GetFallbackHints(TransportHint initial) const
{
    std::vector<TransportHint> hints;
    if (initial != TransportHint::SHM_CANDIDATE) {
        return hints;
    }
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        hints.emplace_back(TransportHint::UB_CANDIDATE);
    }
#endif
    hints.emplace_back(TransportHint::TCP_ONLY);
    return hints;
}

void TransportAdvisor::SetShmCandidateWorkers(const std::vector<HostPort> &workers)
{
    std::unordered_set<HostPort> updated;
    updated.reserve(workers.size());
    for (const auto &w : workers) {
        updated.insert(w);
    }
    bthread::RWLockWrGuard lk(mtx_);
    shmCandidateWorkers_ = std::move(updated);
    ++snapshotGeneration_;
}

bool TransportAdvisor::ObserveDrainingShmFailure(const HostPort &workerAddr)
{
    uint64_t snapshotGeneration;
    {
        bthread::RWLockWrGuard lk(mtx_);
        shmCandidateWorkers_.erase(workerAddr);
        snapshotGeneration = snapshotGeneration_;
    }
    auto refreshGeneration = drainingRefreshGeneration_.load(std::memory_order_acquire);
    while (refreshGeneration < snapshotGeneration) {
        if (drainingRefreshGeneration_.compare_exchange_weak(refreshGeneration, snapshotGeneration,
                                                             std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}
}  // namespace client
}  // namespace datasystem
