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
#ifndef DATASYSTEM_MASTER_WORKER_MANAGER_H
#define DATASYSTEM_MASTER_WORKER_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/master/node_descriptor.h"
#include "datasystem/master/node_manager.h"
#include "datasystem/protos/master_heartbeat.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace master {
enum class ProcessState : uint32_t {
    FIRST_CONNECT,
    NORMAL,
    WORKER_RESTART,
    MASTER_RESTART,
    WORKER_MASTER_RESTART,
    NETWORK_RECOVERY
};

class WorkerStateProcess {
public:
    WorkerStateProcess() = default;
    virtual ~WorkerStateProcess() = default;

    /**
     * @brief Process worker heartbeat timeout
     * @param[in] address The worker address.
     * @param[in] isDead Whether the node is dead.
     * @return Status of the call.
     */
    virtual Status ProcessTimeout(const HostPort &address, bool isDead) = 0;

    /**
     * @brief Process heartbeat messages.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    virtual Status ProcessHeartbeatMsg(const HeartbeatReqPb &req, HeartbeatRspPb &rsp) = 0;

    /**
     * @brief Set the node state.
     * @param[in] nodeState The node state.
     */
    void SetNodeState(NodeState nodeState)
    {
        nodeState_ = nodeState;
    }

    /**
     * @brief Get the node state.
     * @return The current node state.
     */
    NodeState GetNodeState()
    {
        return nodeState_;
    }

    /**
     * @brief Set the process state.
     * @param[in] processState The process state.
     */
    void SetProcessState(ProcessState processState)
    {
        processState_ = processState;
    }

    /**
     * @brief Get the process state.
     * @return The current process state.
     */
    ProcessState GetProcessState()
    {
        return processState_;
    }

private:
    NodeState nodeState_ = NodeState::RUNNING;
    ProcessState processState_ = ProcessState::NORMAL;
};

class WorkerNodeDescriptor : public NodeDescriptor {
public:
    WorkerNodeDescriptor(NodeTypePb type, const HostPort &workerAddress, bool existsMeta = false);

    ~WorkerNodeDescriptor() override = default;

    /**
     * @brief Process heartbeat messages.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status ProcessHeartbeatMsg(const HeartbeatReqPb &req, HeartbeatRspPb &rsp) override;

    /**
     * @brief Process worker heartbeat timeout.
     * @param[in] isDead Whether the node is dead.
     * @return Status of the call.
     */
    Status ProcessTimeout(bool isDead) override;

    /**
     * @brief Register the worker state process.
     * @param[in] process The worker state process object.
     */
    void Register(std::shared_ptr<WorkerStateProcess> process);

private:
    std::vector<std::shared_ptr<WorkerStateProcess>> stateProcesses_;
    std::atomic<bool> firstHeartBeat_;
    bool existsMeta_;
};

class WorkerManager : public NodeManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Reference of WorkerManager.
     */
    static WorkerManager &Instance();

    ~WorkerManager() = default;

    /**
     * @brief Add a worker node. Update its info if it already exist.
     * @param[in] address The address of the worker node.
     * @param[out] desc The added node descriptor.
     * @return Status of the call.
     */
    Status AddWorkerNode(const std::string &address, std::shared_ptr<NodeDescriptor> &desc);

    /**
     * @brief Add a node. Update its info if it already exist.
     * @param[in] request The rpc request protobuf.
     * @param[out] desc The added node descriptor.
     * @return Status of the call.
     */
    Status AddNode(const HeartbeatReqPb &request, std::shared_ptr<NodeDescriptor> &desc) override;

    /**
     * @brief Add worker nodes when master restart.
     * @param[in] ipAddresses The IP address of workers.
     * @return Status of the call.
     */
    Status ReAddNodes(const std::set<std::string> &ipAddresses);

    /**
     * @brief Get a worker descriptor.
     * @param[in] ipAddress The IP address of the worker.
     * @param[out] desc The worker descriptor.
     * @return Status of the call.
     */
    Status GetWorkerDescriptor(const std::string &ipAddress, std::shared_ptr<NodeDescriptor> &desc);

    /**
     * @brief Get all nodes' descriptors.
     * @param[out] descs vector containing all NodeDescriptor in the map.
     * @return Status of the call.
     */
    Status GetAllNodesDesc(std::vector<std::shared_ptr<NodeDescriptor>> &descs) override;

    /**
     * @brief Get node type.
     * @return string of node type.
     */
    std::string GetTypeStr() override;

    /**
     * @brief Set the Register Function.
     * @param registerFunc The register function.
     */
    void SetRegisterFunction(std::function<void(std::shared_ptr<WorkerNodeDescriptor>)> registerFunc)
    {
        registerFunc_ = std::move(registerFunc);
    }

private:
    WorkerManager() = default;

    // The mutex is to protect workerNodes_
    mutable std::shared_timed_mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<NodeDescriptor>> workerNodes_;
    std::function<void(std::shared_ptr<WorkerNodeDescriptor>)> registerFunc_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_WORKER_MANAGER_H
