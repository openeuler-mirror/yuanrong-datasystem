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
 * Description: Basic Descriptor class that records GCS information.
 */
#ifndef MASTER_NODE_DESCRIPTOR_H
#define MASTER_NODE_DESCRIPTOR_H

#include <memory>
#include <mutex>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/master_heartbeat.pb.h"

namespace datasystem {
namespace master {
// UNKNOWN is used for querying about node state. If a node was not found, we will say it is UNKNOWN.
// A node by itself won't have UNKNOWN state.
enum class NodeState { RUNNING = 0, TIMEOUT = 1, OFFLINE = 2, NEEDSREP = 3, RESET = 4, UNKNOWN = 5 };

class NodeDescriptor {
public:
    NodeDescriptor(NodeTypePb type, const HostPort nodeAddress);

    virtual ~NodeDescriptor() = default;

    /**
     * @brief Update the last heartbeat time.
     */
    void UpdateHeartbeatTime();

    /**
     * @brief Check if the node timed out via its last heartbeat time.
     * @return True if it is timeout, false otherwise.
     */
    bool EvaluateTimeout() const;

    /**
     * @brief Check if the node is dead via its last heartbeat time.
     * @return True if it is dead, false otherwise.
     */
    bool AssumeDead() const;

    /**
     * @brief Update the hostport of the node.
     * @param[in] hostPort The hostport of the node.
     * @return Status of the call.
     */
    Status UpdateHostPort(const HostPort &hostPort);

    /**
     * @brief Process heartbeat messages.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc respoonse protobuf.
     * @return Status of the call.
     */
    virtual Status ProcessHeartbeatMsg(const HeartbeatReqPb &req, HeartbeatRspPb &rsp);

    /**
     * @brief Process timeout.
     * @param[in] isDead Whether the node is dead.
     * @return Status of the call.
     */
    virtual Status ProcessTimeout(bool isDead)
    {
        (void)isDead;
        return Status::OK();
    }

    /**
     * @brief Get type of the node.
     * @return The type of the node.
     */
    NodeTypePb GetType() const
    {
        return type_;
    }

    /**
     * @brief Get HostPort of the node.
     * @return the HostPort of the node.
     */
    HostPort GetHostPort() const
    {
        return nodeAddress_;
    }

    /**
     * @brief Set this node's timeout status.
     * @param[in] timeOut The status for timeout, either True or False.
     */
    void SetTimeOut(bool timeOut)
    {
        timeOut_ = timeOut;
    }

    /**
     * @brief Return if this node has timed out.
     * @return True if this node has timed out.
     */
    bool TimeOut()
    {
        return timeOut_;
    }

    /**
     * @brief Output node address for Debugging.
     * @return The address of node in string.
     */
    virtual std::string ToString()
    {
        return "[Address:" + nodeAddress_.ToString() + "]";
    }

    /**
     * @brief Get the node state.
     * @return The current node state.
     */
    NodeState GetNodeState()
    {
        return state_;
    }

    /**
     * @brief Set the node state.
     */
    void SetNodeState(NodeState state)
    {
        state_ = state;
    }

    static std::string NodeStateToString(NodeState state)
    {
        std::string str = "Unknown";

        switch (state) {
            case NodeState::RUNNING:
                str = "Running";
                break;
            case NodeState::OFFLINE:
                str = "Offline";
                break;
            case NodeState::NEEDSREP:
                str = "Needsrep";
                break;
            case NodeState::RESET:
                str = "Reset";
                break;
            default:
                break;
        }

        return str;
    }

protected:
    NodeTypePb type_;
    HostPort nodeAddress_;  // Server address.
    Timer lastHeartbeat_;   // Time of the last received heartbeat.
    bool timeOut_;
    NodeState state_;
};
}  // namespace master
}  // namespace datasystem
#endif  // MASTER_NODE_DESCRIPTOR_H
