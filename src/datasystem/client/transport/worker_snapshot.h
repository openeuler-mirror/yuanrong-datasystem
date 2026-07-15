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

/** Description: Defines the worker snapshot used to reconcile transport connections. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_WORKER_SNAPSHOT_H
#define DATASYSTEM_CLIENT_TRANSPORT_WORKER_SNAPSHOT_H

#include <cstdint>
#include <vector>

#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

struct WorkerSnapshot {
    bool Empty() const
    {
        return sameHostAddrs.empty() && otherAddrs.empty();
    }

    uint64_t ringVersion = 0;
    std::vector<HostPort> sameHostAddrs;
    std::vector<HostPort> otherAddrs;
};

/**
 * @brief Build an all-or-nothing transport snapshot from the complete topology membership.
 * @param[in] ringVersion Version returned with the topology update.
 * @param[in] ring Complete cluster topology; every member state remains transport-admissible.
 * @param[out] snapshot Validated snapshot, left unchanged when any endpoint is malformed.
 * @return K_OK on success; K_INVALID when a member endpoint cannot be parsed.
 */
Status BuildWorkerSnapshot(uint64_t ringVersion, const ::datasystem::ClusterTopologyPb &ring,
                           WorkerSnapshot &snapshot);

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_WORKER_SNAPSHOT_H
