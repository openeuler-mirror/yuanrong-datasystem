/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: The interface of worker manager.
 */
#ifndef DATASYSTEM_MASTER_NODE_MANAGER_H
#define DATASYSTEM_MASTER_NODE_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/master/node_descriptor.h"
#include "datasystem/protos/master_heartbeat.pb.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {
class NodeManager {
public:
    /**
     * @brief Construct NodeManager.
     */
    NodeManager()
    {
    }

    virtual ~NodeManager() = default;

    /**
     * @brief Add a worker node. Update its info if it already exists.
     * @param[in] request The rpc request protobuf.
     * @param[out] desc The added worker descriptor.
     * @return Status of the call.
     */
    virtual Status AddNode(const HeartbeatReqPb &request, std::shared_ptr<NodeDescriptor> &desc) = 0;

    /**
     * @brief Get all worker descriptions.
     * @param[out] descs The worker descriptors.
     * @return Status of the call.
     */
    virtual Status GetAllNodesDesc(std::vector<std::shared_ptr<NodeDescriptor>> &descs) = 0;

    /**
     * @brief Get node type.
     * @return string of node type.
     */
    virtual std::string GetTypeStr() = 0;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_NODE_MANAGER_H
