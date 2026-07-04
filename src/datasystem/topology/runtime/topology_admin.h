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
 * Description: Administrative topology maintenance entrypoints.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_ADMIN_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_ADMIN_H

#include "datasystem/topology/runtime/topology_snapshot_rebuilder.h"

namespace datasystem {
namespace topology {

class TopologyAdmin final {
public:
    explicit TopologyAdmin(TopologySnapshotRebuilder &rebuilder);
    ~TopologyAdmin() = default;
    TopologyAdmin(const TopologyAdmin &) = delete;
    TopologyAdmin &operator=(const TopologyAdmin &) = delete;
    TopologyAdmin(TopologyAdmin &&) = delete;
    TopologyAdmin &operator=(TopologyAdmin &&) = delete;

    /**
     * @brief Rebuild the local routing snapshot from the committed topology fact.
     * @return K_OK when a new snapshot is published; repository and codec errors are returned unchanged.
     */
    Status RebuildFromBackend();

private:
    TopologySnapshotRebuilder &rebuilder_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_ADMIN_H
