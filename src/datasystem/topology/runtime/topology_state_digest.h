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
 * Description: Stable topology state digest helpers.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_STATE_DIGEST_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_STATE_DIGEST_H

#include <string>
#include <vector>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

class TopologyStateDigest {
public:
    TopologyStateDigest() = delete;
    ~TopologyStateDigest() = delete;

    /**
     * @brief Build a deterministic digest from topology and unfinished task summary.
     * @param[in] snapshot Local topology state to summarize.
     * @param[out] digest Digest string suitable for peer-state comparison.
     * @return K_OK on success; K_INVALID when snapshot topology cannot be encoded.
     */
    static Status Build(const LocalTopologySnapshot &snapshot, std::string &digest);

    /**
     * @brief Build a local snapshot with digest populated.
     * @param[in] topology Committed topology descriptor.
     * @param[in] revision Store revision observed with topology.
     * @param[in] taskSummary Unfinished task summary.
     * @param[out] snapshot Snapshot with digest.
     * @return K_OK on success.
     */
    static Status BuildSnapshot(const TopologyDescriptor &topology, Revision revision,
                                const TopologyTaskSummary &taskSummary,
                                LocalTopologySnapshot &snapshot);
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_STATE_DIGEST_H
