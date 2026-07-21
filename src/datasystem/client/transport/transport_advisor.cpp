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
        std::shared_lock<std::shared_mutex> lk(mtx_);
        if (sameHostWorkers_.count(workerAddr) > 0) {
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

void TransportAdvisor::SetSameHostWorkers(const std::vector<HostPort> &workers)
{
    std::unordered_set<HostPort> updated;
    updated.reserve(workers.size());
    for (const auto &w : workers) {
        updated.insert(w);
    }
    std::unique_lock<std::shared_mutex> lk(mtx_);
    sameHostWorkers_ = std::move(updated);
}
}  // namespace client
}  // namespace datasystem
