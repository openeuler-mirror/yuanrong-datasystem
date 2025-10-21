/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Cluster node structure
 */

#ifndef DATASYSTEM_CLUSTER_NODE_H
#define DATASYSTEM_CLUSTER_NODE_H

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
class EtcdClusterManager::ClusterNode {
public:
    /**
     * @brief Constructor
     * @param[in] timeEpoch The timestamp for when this node is started
     * @param[in] additionEventType The type of event for when this node is started
     */
    ClusterNode(const std::string &timeEpoch, const std::string &additionEventType);

    ClusterNode() = delete;

    /**
     * @brief Default destructor
     */
    ~ClusterNode() = default;

    /**
     * @brief return a string that represents the cluster node state
     * @return The string of the cluster node state
     */
    std::string StateToString() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::string stateStr;
        if (state_ == NodeState::ACTIVE) {
            stateStr = "ACTIVE";
        } else if (state_ == NodeState::FAILED) {
            stateStr = "FAILED";
        } else if (state_ == NodeState::TIMEOUT) {
            stateStr = "TIMEOUT";
        } else {
            stateStr = "UNKNOWN";
        }
        return stateStr;
    }

    /**
     * @brief Typically for debugging and logging, converts the ClusterNode fields into a string
     * @param hostPortKey the identifier of the cluster node is the key. Callers will want to see that also
     * @return ClusterNode info as a single string
     */
    std::string ToString(const HostPort &hostPortKey) const
    {
        std::string stateStr = this->StateToString();
        std::lock_guard<std::mutex> lock(mutex_);
        return hostPortKey.ToString() + ";" + timeEpoch_ + ";" + stateStr + ";" + additionEventType_;
    }

    std::string EventValue() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return timeEpoch_ + ";" + additionEventType_;
    }

    /**
     * @brief Check if node is active
     * @return True if active
     */
    bool IsActive() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_ == NodeState::ACTIVE;
    }

    /**
     * @brief Check if node is failed
     * @return True if failed
     */
    bool IsFailed() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_ == NodeState::FAILED;
    }

    /**
     * @brief Check if node is in timeout state
     * @return True if node is timed out
     */
    bool IsTimedOut() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_ == NodeState::TIMEOUT;
    }

    /**
     * @brief Setter for the node state to the failed state
     */
    void SetFailed()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = NodeState::FAILED;
    }

    /**
     * @brief Setter for the node state to the active state
     */
    void SetActive()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = NodeState::ACTIVE;
    }

    /**
     * @brief Setter for the node state to the active state
     */
    void SetTimedOut()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = NodeState::TIMEOUT;
        timeoutStamp_.Reset();
    }

    /**
     * @brief Whether the node restarted when it was added to etcd.
     * @return T/F true if restarted
     */
    bool NodeWasRestarted() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return (additionEventType_ == "restart");
    }

    bool NodeWasRecovered() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return (additionEventType_ == "recover");
    }

    bool NodeWasExiting() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return (additionEventType_ == ETCD_NODE_EXITING);
    }

    bool NodeWasReady() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return (additionEventType_ == ETCD_NODE_READY);
    }

    bool NodeWasDowngradeRestart() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return (additionEventType_ == ETCD_NODE_DOWNGRADE_RESTART);
    }

    /**
     * @brief Copies the timestamp and additionEventType fields from the input cluster node into the current one.
     * @param[in] a The input cluster node to copy from
     */
    void CopyInfoFrom(const ClusterNode &a)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timeEpoch_ = a.timeEpoch_;
        additionEventType_ = a.additionEventType_;
    }

    /**
     * @brief Performs a check if a timed out node needs to be demoted to a failed node, and executes the demotion
     * if the node meets the criteria.
     * @return T/F returns true if the node was demoted, and false if the node did not meet the criteria and remains
     * unchanged.
     */
    bool DemoteTimedOutNode();

    bool operator==(const ClusterNode &a) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return timeEpoch_ == a.timeEpoch_ && additionEventType_ == a.additionEventType_ && state_ == a.state_;
    }

    /**
     * @brief Return timeepoch/timestamp.
     */
    inline std::string GetTimeEpoch() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return timeEpoch_;
    }

private:
    enum class NodeState { ACTIVE = 0, FAILED = 1, TIMEOUT = 2 };
    mutable std::mutex mutex_;       // Protect cluster node
    std::string timeEpoch_;          // The timestamp when this cluster node was started
    std::string additionEventType_;  // The type of event when the node was added to etcd:
                                     // "start", "restart", "recover", "exitting", "ready", "d_rst"(Downgrade Restart)
    NodeState state_;                // The state of the node
    Timer timeoutStamp_;             // Save the timestamp when the NodeState is set to TIMEOUT
};
}  // namespace datasystem

#endif  // DATASYSTEM_CLUSTER_NODE_H
