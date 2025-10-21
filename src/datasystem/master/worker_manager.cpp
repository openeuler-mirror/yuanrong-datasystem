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
#include "datasystem/master/worker_manager.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/version.h"

namespace datasystem {
namespace master {
WorkerNodeDescriptor::WorkerNodeDescriptor(NodeTypePb type, const HostPort &workerAddress, bool existsMeta)
    : NodeDescriptor(type, workerAddress), firstHeartBeat_(true), existsMeta_(existsMeta)
{
}

Status WorkerNodeDescriptor::ProcessHeartbeatMsg(const HeartbeatReqPb &req, HeartbeatRspPb &rsp)
{
    UpdateHeartbeatTime();
    ProcessState processState;
    NodeState nodeState = GetNodeState();
    VLOG(3) << "ProcessHeartbeatMsg: [worker send] first_msg=" << req.first_msg()
            << ", [master recv] firstHeartBeat=" << firstHeartBeat_ << ", node state:" << (uint32_t)nodeState;

    if (req.first_msg()) {
        std::string masterGitHash = GetGitHash();
        if (masterGitHash != req.git_hash()) {
            LOG(ERROR) << "Error: The commit id of worker does not match that of master. worker commit id is: "
                       << req.git_hash() << ", master commit id is: " << masterGitHash;
        }
        if (firstHeartBeat_) {
            if (existsMeta_) {
                processState = ProcessState::WORKER_MASTER_RESTART;
            } else {
                processState = ProcessState::FIRST_CONNECT;
            }
        } else {
            processState = ProcessState::WORKER_RESTART;
        }
    } else {
        if (firstHeartBeat_) {
            if (existsMeta_) {
                processState = ProcessState::MASTER_RESTART;
            } else {
                processState = ProcessState::NORMAL;
            }
        } else if (nodeState == NodeState::RUNNING) {
            processState = ProcessState::NORMAL;
        } else {
            processState = ProcessState::NETWORK_RECOVERY;
        }
    }

    Status status;
    for (auto &process : stateProcesses_) {
        process->SetNodeState(nodeState);
        process->SetProcessState(processState);
        Status rc = process->ProcessHeartbeatMsg(req, rsp);
        if (rc.IsError()) {
            status = rc;
            LOG(ERROR) << "Process Heartbeat failed:" << rc.GetMsg();
        }
    }
    SetNodeState(NodeState::RUNNING);
    firstHeartBeat_ = false;
    return status;
}

Status WorkerNodeDescriptor::ProcessTimeout(bool isDead)
{
    LOG(INFO) << "WorkerNodeDescriptor::ProcessTimeout " << isDead;
    auto nodeState = GetNodeState();

    // Only process one time for timeout and offline.
    if (isDead) {
        if (nodeState == NodeState::OFFLINE) {
            return Status::OK();
        }
        nodeState = NodeState::OFFLINE;
    } else {
        if (nodeState == NodeState::TIMEOUT) {
            return Status::OK();
        }
        nodeState = NodeState::TIMEOUT;
    }

    Status status;
    HostPort address = GetHostPort();
    for (auto &process : stateProcesses_) {
        Status rc = process->ProcessTimeout(address, isDead);
        if (rc.IsError()) {
            status = rc;
            LOG(ERROR) << "Process Heartbeat failed:" << rc.GetMsg();
        }
    }
    SetNodeState(nodeState);
    return status;
}

void WorkerNodeDescriptor::Register(std::shared_ptr<WorkerStateProcess> process)
{
    stateProcesses_.push_back(std::move(process));
}

WorkerManager &WorkerManager::Instance()
{
    static WorkerManager instance;
    return instance;
}

Status WorkerManager::AddWorkerNode(const std::string &address, std::shared_ptr<NodeDescriptor> &desc)
{
    HeartbeatReqPb req;
    req.set_ip_address(address);
    req.set_type(NodeTypePb::WORKER_NODE);
    return AddNode(req, desc);
}

Status WorkerManager::AddNode(const HeartbeatReqPb &request, std::shared_ptr<NodeDescriptor> &desc)
{
    std::string ipAddress = request.ip_address();
    NodeTypePb type = request.type();
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto iter = workerNodes_.find(ipAddress);
    if (iter == workerNodes_.end()) {
        LOG(INFO) << "Registering new worker in the master node management tracking. worker: " << ipAddress;
        HostPort host;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(registerFunc_, K_RUNTIME_ERROR, "not set register function!");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(host.ParseString(ipAddress), "Failed to parse ip address");
        auto workerDesc = std::make_shared<WorkerNodeDescriptor>(type, host);
        registerFunc_(workerDesc);
        desc = workerDesc;
        workerNodes_[ipAddress] = desc;
        LOG(INFO) << "Registered worker number is " << workerNodes_.size();
    } else {
        desc = workerNodes_[ipAddress];
    }
    return Status::OK();
}

Status WorkerManager::ReAddNodes(const std::set<std::string> &ipAddresses)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(registerFunc_, K_RUNTIME_ERROR, "not set register function!");
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    for (const auto &ipAddress : ipAddresses) {
        HostPort host;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(host.ParseString(ipAddress), "Failed to parse ip address");
        auto workerDesc = std::make_shared<WorkerNodeDescriptor>(NodeTypePb::WORKER_NODE, host, true);
        registerFunc_(workerDesc);
        workerNodes_[ipAddress] = workerDesc;
    }
    LOG(INFO) << "Registered worker number is " << workerNodes_.size();
    return Status::OK();
}

Status WorkerManager::GetWorkerDescriptor(const std::string &ipAddress, std::shared_ptr<NodeDescriptor> &desc)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto iter = workerNodes_.find(ipAddress);
    CHECK_FAIL_RETURN_STATUS(iter != workerNodes_.end(), StatusCode::K_RUNTIME_ERROR,
                             "The worker does not exist. ipAddress:" + ipAddress);
    desc = workerNodes_[ipAddress];
    return Status::OK();
}

Status WorkerManager::GetAllNodesDesc(std::vector<std::shared_ptr<NodeDescriptor>> &descs)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    VLOG(2) << "WorkerManager::GetAllNodesDesc: worker map size is " << workerNodes_.size();
    for (auto iter = workerNodes_.begin(); iter != workerNodes_.end(); iter++) {
        descs.push_back(iter->second);
    }
    return Status::OK();
}

std::string WorkerManager::GetTypeStr()
{
    return "worker";
}
}  // namespace master
}  // namespace datasystem
