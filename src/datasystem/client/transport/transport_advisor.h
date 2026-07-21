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

#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {
class TransportAdvisor {
public:
    TransportAdvisor() = default;
    virtual ~TransportAdvisor() = default;

    /**
     * @brief Suggest a transport hint for the target worker. Same-host workers (populated via
     * SetSameHostWorkers from the routing snapshot) return SHM_CANDIDATE so the transport layer
     * builds shm fd-passing channels; cross-host workers fall through to UB_CANDIDATE or TCP_ONLY.
     * @param[in] workerAddr Target worker address.
     * @return The suggested TransportHint.
     */
    virtual TransportHint GetTransportHint(const HostPort &workerAddr) const;

    /**
     * @brief Update the set of same-host worker addresses (from BuildWorkerSnapshot sameHostAddrs).
     * Called when the routing topology changes. Thread-safe.
     */
    void SetSameHostWorkers(const std::vector<HostPort> &workers);

private:
    mutable std::shared_mutex mtx_;
    std::unordered_set<HostPort> sameHostWorkers_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H
