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
 * Description: Defines client info class and client manager class.
 */
#include "datasystem/worker/client_manager/client_manager.h"

#include <atomic>
#include <cstdint>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"

DS_DECLARE_uint32(max_client_num);
constexpr uint32_t CLIENT_HEARTBEAT_INTERVAL_SECOND = 3;  // 3s

namespace datasystem {
namespace worker {

ClientManager &ClientManager::Instance()
{
    static ClientManager instance;
    return instance;
}

ClientManager::~ClientManager()
{
    healthThreadExit_ = true;
    cvLock_.Set();
    if (healthThread_ != nullptr && healthThread_->joinable()) {
        healthThread_->join();
    }
}

Status ClientManager::Init()
{
    auto traceId = Trace::Instance().GetTraceID();
    healthThread_ = std::make_unique<Thread>([this, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        uint32_t heartbeatIntervalMs = CLIENT_HEARTBEAT_INTERVAL_SECOND * SECS_TO_MS;
        INJECT_POINT("ClientManager.Init.heartbeatInterval", [&heartbeatIntervalMs](int timeMs) {
            heartbeatIntervalMs = timeMs;
            return Status::OK();
        });
        LOG(INFO) << "Start to check the client's alive, interval is " << heartbeatIntervalMs << " ms";
        while (!healthThreadExit_) {
            CheckClientHealth();
            cvLock_.WaitFor(heartbeatIntervalMs);
        }
        return Status::OK();
    });
    healthThread_->set_name("ClientHealthCheck");
    heartbeatEventLoop_ = std::make_shared<SockEventLoop>();
    return heartbeatEventLoop_->Init();
}

Status ClientManager::AddClient(const ClientKey &clientId, int socketFd, bool uniqueCount, const std::string &tenantId)
{
    LOG(INFO) << "Add client info, socketFd:" << socketFd << ", clientId:" << clientId << ", tenantId:" << tenantId;
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    auto clientInfo = std::make_shared<ClientInfo>(socketFd, clientId, uniqueCount, true, tenantId);
    return tbbClientTable_.emplace(clientId, std::move(clientInfo))
               ? Status::OK()
               : Status(StatusCode::K_RUNTIME_ERROR, FormatString("Failed to insert client %s to table", clientId));
}

Status ClientManager::AddClient(const ClientKey &clientId, bool shmEnabled, int socketFd, const std::string &tenantId,
                                bool enableCrossNode, const std::string &podName, std::string deviceId,
                                uint32_t &lockId)
{
    // Ensure callers never observe a stale lockId on failure paths.
    lockId = 0;
    if (shmEnabled) {
        RETURN_IF_NOT_OK(GetLockId(lockId));
    }
    Status status;
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    bool uniqueCount = true;
    auto clientInfo = std::make_shared<ClientInfo>(socketFd, clientId, uniqueCount, shmEnabled, tenantId,
                                                   enableCrossNode, podName, std::move(deviceId));
    clientInfo->SetLockId(lockId);
    bool insert = tbbClientTable_.emplace(clientId, std::move(clientInfo));
    if (!insert) {
        if (shmEnabled) {
            ReturnLockId(lockId);
            lockId = 0;
        }
        status = Status(StatusCode::K_RUNTIME_ERROR, FormatString("Failed to insert client %s to table", clientId));
    }
    return status;
}

void ClientManager::RemoveClient(const ClientKey &clientId)
{
    LOG(INFO) << "Remove client: " << clientId;
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientInfoTable::accessor accessor;
    if (!tbbClientTable_.find(accessor, clientId)) {
        LOG(WARNING) << "Client can not find, so we will not remove client info, clientId:" << clientId;
        return;
    }
    if (accessor->second->Removable()) {
        removableCount_.fetch_sub(1, std::memory_order_relaxed);
    }
    if (accessor->second->GetSocketFd() != INVALID_SOCKET_FD) {
        LOG(INFO) << "Close client[socket fd: " << accessor->second->GetSocketFd() << "]";
        (void)heartbeatEventLoop_->DelFdEvent(accessor->second->GetSocketFd());
        RETRY_ON_EINTR(close(accessor->second->GetSocketFd()));
    }
    uint32_t lockId;
    if (accessor->second->GetLockId(lockId).IsOk()) {
        ReturnLockId(lockId);
    }
    LOG(INFO) << FormatString("Remove client success, id: %s, pod: %s, shm: %d, cross node: %d", clientId,
                              accessor->second->PodName(), accessor->second->ShmEnabled(),
                              accessor->second->EnableCrossNode());
    (void)tbbClientTable_.erase(accessor);
    {
        std::lock_guard<std::shared_timed_mutex> lck(clientId2WorkerFdsMapMutex_);
        clientId2WorkerFdsMap_.erase(clientId);
    }
}

Status ClientManager::RegisterLostHandler(const ClientKey &clientId, std::function<void()> lostHandle,
                                          HeartbeatType type)
{
    RETURN_IF_NOT_OK(CheckClientId(clientId));
    auto clientInfo = GetClientInfo(clientId);
    if (type == HeartbeatType::UDS_HEARTBEAT) {
        Status status = heartbeatEventLoop_->AddFdEvent(clientInfo->GetSocketFd(), EPOLLIN | EPOLLHUP,
                                                        std::move(lostHandle), nullptr);
        CHECK_FAIL_RETURN_STATUS(status.IsOk(), K_RUNTIME_ERROR,
                                 FormatString("Register client %s failed: %s", clientId, status.GetMsg()));
    } else {
        clientInfo->SetLostHandler(std::move(lostHandle), type);
    }
    return Status::OK();
}

Status ClientManager::UpdateLastHeartbeat(const ClientKey &clientId, bool removable)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientInfoTable::accessor accessor;
    if (!tbbClientTable_.find(accessor, clientId) || accessor->second.get() == nullptr) {
        return { K_RUNTIME_ERROR, FormatString("Can not find the client %s", clientId) };
    }
    accessor->second->UpdateLastHeartbeat();
    bool prev = accessor->second->SetRemovable(removable);
    // If the removable flag changes from true to false, decrease the removable count.
    if (prev && !removable) {
        removableCount_.fetch_sub(1, std::memory_order_relaxed);
    }
    // If the removable flag changes from false to true, increase the removable count.
    if (!prev && removable) {
        removableCount_.fetch_add(1, std::memory_order_relaxed);
    }
    return Status::OK();
}

Status ClientManager::GetClientSocketFd(const ClientKey &clientId, int &socketFd) const
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientInfoTable::const_accessor accessor;
    CHECK_FAIL_RETURN_STATUS(tbbClientTable_.find(accessor, clientId), StatusCode::K_RUNTIME_ERROR,
                             FormatString("The client ID (%s) does not exist.", clientId));
    socketFd = accessor->second->GetSocketFd();
    return Status::OK();
}

Status ClientManager::AddShmUnit(const ClientKey &clientId, const std::shared_ptr<ShmUnit> &shmUnit)
{
    RETURN_RUNTIME_ERROR_IF_NULL(shmUnit);
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientInfoTable::accessor accessor;
    CHECK_FAIL_RETURN_STATUS(tbbClientTable_.find(accessor, clientId), StatusCode::K_RUNTIME_ERROR,
                             FormatString("The client id (%s) does not exist.", clientId));
    (void)accessor->second->AddShmUnit(shmUnit);
    return Status::OK();
}

Status ClientManager::RemoveShmUnit(const ClientKey &clientId, const std::shared_ptr<ShmUnit> &shmUnit)
{
    RETURN_RUNTIME_ERROR_IF_NULL(shmUnit);
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientInfoTable::accessor accessor;
    CHECK_FAIL_RETURN_STATUS(tbbClientTable_.find(accessor, clientId), StatusCode::K_RUNTIME_ERROR,
                             FormatString("The client id (%s) does not exist.", clientId));
    (void)accessor->second->RemoveShmUnit(shmUnit);
    return Status::OK();
}

Status ClientManager::RemoveShmUnit(const std::shared_ptr<ShmUnit> &shmUnit)
{
    RETURN_RUNTIME_ERROR_IF_NULL(shmUnit);
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    for (const auto &kv : tbbClientTable_) {
        if (kv.second->Contains(shmUnit->id)) {
            CHECK_FAIL_RETURN_STATUS(kv.second->RemoveShmUnit(shmUnit), StatusCode::K_RUNTIME_ERROR,
                                     FormatString("Failed to remove ShmUint %s", shmUnit->id));
        }
    }
    return Status::OK();
}

std::shared_ptr<ClientInfo> ClientManager::GetClientInfo(const ClientKey &clientId)
{
    INJECT_POINT("client_manager.GetClientInfo", []() {
        auto info = std::make_shared<ClientInfo>(0, ClientKey::Intern(""), false, true, "");
        return info;
    });
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientInfoTable::accessor accessor;
    if (tbbClientTable_.find(accessor, clientId)) {
        return accessor->second;
    }
    return nullptr;
}

std::string ClientManager::GetAuthTenantIdByClientId(const ClientKey &clientId, bool &clientExist)
{
    auto clientInfo = GetClientInfo(clientId);
    if (clientInfo == nullptr) {
        LOG(INFO) << "client info is nullptr, clientId: " << clientId;
        clientExist = false;
        return "";
    }
    clientExist = true;
    return clientInfo->GetTenantId();
}

Status ClientManager::CheckClientId(const ClientKey &clientId) const
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    CHECK_FAIL_RETURN_STATUS(tbbClientTable_.count(clientId) > 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("The client id (%s) does not exist.", clientId));
    return Status::OK();
}

ClientManager::ClientManager()
{
    uint32_t i = 0;  // index start from 1
    std::generate_n(std::inserter(freeLockIds_, freeLockIds_.begin()), FLAGS_max_client_num, [&i]() { return ++i; });
}

Status ClientManager::GetLockId(uint32_t &lockId)
{
    std::lock_guard<std::mutex> l(lockMutex_);
    if (freeLockIds_.empty()) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Client number upper to the limit");
    }
    lockId = *freeLockIds_.begin();
    INJECT_POINT("worker.clientManager.getLockId", [&lockId](uint32_t newLockId) {
        lockId = newLockId;
        return Status::OK();
    });
    (void)freeLockIds_.erase(lockId);
    return Status::OK();
}

void ClientManager::ReturnLockId(uint32_t lockId)
{
    // 0 is just for worker, client would not get it forever.
    if (lockId == 0) {
        return;
    }
    std::lock_guard<std::mutex> l(lockMutex_);
    if (lockId > FLAGS_max_client_num) {
        LOG(WARNING) << "lock id " << lockId << " is invalid, ignore it.";
        return;
    }
    (void)freeLockIds_.emplace(lockId);
}

void ClientManager::CheckClientHealth()
{
    Status status;
    std::vector<std::shared_ptr<ClientInfo>> lostClients;
    {
        // Notice: The lock needs to be released as soon as possible. Therefore, the LostHandler command is executed
        // separately.
        std::lock_guard<std::shared_timed_mutex> lck(mutex_);
        for (auto iter = tbbClientTable_.begin(); iter != tbbClientTable_.end(); iter++) {
            if (iter->second != nullptr) {
                if (iter->second->IsClientLost()) {
                    LOG(WARNING) << FormatString("Client %s lost heartbeat, worker will remove this client.",
                                                 iter->first);
                    lostClients.push_back(iter->second);
                }
            }
        }
    }

    for (const auto &clientInfo : lostClients) {
        if (clientInfo != nullptr) {
            clientInfo->LostHandler();
        }
    }
}

std::set<int> ClientManager::GetWorkerFdByClientId(const ClientKey &clientId)
{
    std::shared_lock<std::shared_timed_mutex> lck(clientId2WorkerFdsMapMutex_);
    auto iter = clientId2WorkerFdsMap_.find(clientId);
    if (iter != clientId2WorkerFdsMap_.end()) {
        return iter->second;
    }
    return {};
}

void ClientManager::DelClientId2WorkerFdMap(const ClientKey &clientId, const std::vector<int> &fds)
{
    std::lock_guard<std::shared_timed_mutex> lck(clientId2WorkerFdsMapMutex_);
    auto iter = clientId2WorkerFdsMap_.find(clientId);
    if (iter != clientId2WorkerFdsMap_.end()) {
        for (auto fd : fds) {
            iter->second.erase(fd);
        }
        if (iter->second.empty()) {
            clientId2WorkerFdsMap_.erase(iter);
        }
    }
}

bool ClientManager::IsAllWorkerFdsReleased(const std::vector<int> &workerFds)
{
    std::shared_lock<std::shared_timed_mutex> lck(clientId2WorkerFdsMapMutex_);
    for (auto workerFd : workerFds) {
        for (auto iter : clientId2WorkerFdsMap_) {
            if (iter.second.find(workerFd) != iter.second.end()) {
                return false;
            }
        }
    }
    return true;
}

void ClientManager::SetClientId2WorkerFdMap(const ClientKey &clientId, const std::vector<int> &fds)
{
    std::lock_guard<std::shared_timed_mutex> lck(clientId2WorkerFdsMapMutex_);
    clientId2WorkerFdsMap_[clientId].insert(fds.begin(), fds.end());
}

bool ClientManager::ExistClientsOnSameNode()
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    for (auto iter = tbbClientTable_.begin(); iter != tbbClientTable_.end(); iter++) {
        if (iter->second != nullptr) {
            // shmEnabled clients use the UDS heartbeat
            if ((iter->second->ShmEnabled() || iter->second->EnableCrossNode()) && !iter->second->Removable()) {
                constexpr int logInterval = 10;
                LOG_EVERY_T(INFO, logInterval)
                    << "[Graceful exit] client " << iter->first << ", pod name: " << iter->second->PodName()
                    << " still exists, shm enable: " << iter->second->ShmEnabled()
                    << ", enable cross node: " << iter->second->EnableCrossNode()
                    << ", removable: " << iter->second->Removable() << ", client size: " << tbbClientTable_.size();
                return true;
            } else {
                VLOG(1) << "[Graceful exit] client " << iter->first << " exit: "
                        << " still exists, shm enable: " << iter->second->ShmEnabled()
                        << ", enable cross node: " << iter->second->EnableCrossNode()
                        << ", removable: " << iter->second->Removable() << ", client size: " << tbbClientTable_.size();
            }
        } else {
            VLOG(1) << "[Graceful exit] client is nullptr";
        }
    }
    return false;
}
}  // namespace worker
}  // namespace datasystem
