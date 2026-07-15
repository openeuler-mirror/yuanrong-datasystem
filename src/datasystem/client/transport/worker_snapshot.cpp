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

/** Description: Builds validated worker snapshots for transport reconciliation. */

#include "datasystem/client/transport/worker_snapshot.h"

#include <utility>

namespace datasystem {
namespace client {

Status BuildWorkerSnapshot(uint64_t ringVersion, const ::datasystem::ClusterTopologyPb &ring,
                           WorkerSnapshot &snapshot)
{
    WorkerSnapshot updated;
    updated.ringVersion = ringVersion;
    updated.otherAddrs.reserve(ring.members_size());
    for (const auto &member : ring.members()) {
        HostPort worker;
        Status rc = worker.ParseString(member.first);
        if (rc.IsError()) {
            return Status(K_INVALID, "Invalid worker endpoint in cluster topology: " + member.first);
        }
        updated.otherAddrs.emplace_back(std::move(worker));
    }
    snapshot = std::move(updated);
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
