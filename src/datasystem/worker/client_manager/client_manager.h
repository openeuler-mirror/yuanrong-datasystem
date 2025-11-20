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
#ifndef DATASYSTEM_WORKER_CLIENT_MANAGER_H
#define DATASYSTEM_WORKER_CLIENT_MANAGER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/worker/client_manager/client_info.h"

namespace datasystem {
namespace worker {
constexpr int INVALID_SOCKET_FD = -1;

using TbbClientInfoTable = tbb::concurrent_hash_map<ClientKey, std::shared_ptr<ClientInfo>>;
class ClientManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Reference of ClientManager
     */
    static ClientManager &Instance();

    ~ClientManager();

    /**
     * @brief Init a client manager.
     */
    Status Init();

    /**
     * @brief Add client information.
     * @param[in] clientId Uuid of the client
     * @param[in] socketFd Socket fd of the client.
     * @param[in] uniqueCount If the value is true, reference counting is not added when the data already exists.
     * @param[in] tenantId The tenant id
     * @return Status of the call.
     */
    Status AddClient(const ClientKey &clientId, int socketFd, bool uniqueCount = true,
                     const std::string &tenantId = "");

    /**
     * @brief Add client information with rpc heartbeat type.
     * @param[in] clientId Uuid of the client
     * @param[in] uniqueCount If the value is true, reference counting is not added when the data already exists.
     * @param[in] shmEnabled Indicates whether the client allows shared memory.
     * @param[in] tenantId The tenant id
     * @param[in] enableCrossNode Client is enable cross node connection or not.
     * @param[in] podName Client pod name.
     * @param[out] lockId Lock id for client.
     * @return Status of the call.
     */
    Status AddClient(const ClientKey &clientId, bool shmEnabled, int socketFd, const std::string &tenantId,
                     bool enableCrossNode, const std::string &podName, uint32_t &lockId);

    /**
     * @brief Remove client information.
     * @param[in] clientId Uuid of the client.
     */
    void RemoveClient(const ClientKey &clientId);

    /**
     * @brief Register a lost handler to process client lost.
     * @param[in] clientId The clientId.
     * @param[in] socketFd The unix domain socket Fd.
     * @return Status of the call.
     */
    Status RegisterLostHandler(const ClientKey &clientId, std::function<void()> lostHandle, HeartbeatType type);

    /**
     * @brief Update the client rpc heart time.
     * @param[in] clientId The clientId.
     * @param[in] removable Indicate the client is removable or not.
     * @return Status of the call.
     */
    Status UpdateLastHeartbeat(const ClientKey &clientId, bool removable = false);

    /**
     * @brief Get socket fd from client.
     * @param[in] clientId Uuid of the client.
     * @param[out] socketFd Socket fd of the client.
     * @return Status of the call.
     */
    Status GetClientSocketFd(const ClientKey &clientId, int &socketFd) const;

    /**
     * @brief Add shared memory unit to the client table.
     * @param[in] clientId Uuid of client.
     * @param[in] shmUnit Shared memory unit to be added.
     * @return Status of the call.
     */
    Status AddShmUnit(const ClientKey &clientId, const std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Remove shared memory unit from the client table.
     * @param[in] clientId Uuid of client.
     * @param[in] shmUnit Shared memory unit to be removed.
     * @return Status of the call.
     */
    Status RemoveShmUnit(const ClientKey &clientId, const std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Remove shared memory unit from the client table.
     * @param[in] shmUnit Shared memory unit to be removed.
     * @return Status of the call.
     */
    Status RemoveShmUnit(const std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Obtain the ClientInfo pointer.
     * @param[in] clientId Uuid of client.
     * @return Pointer of the ClientInfo.
     */
    std::shared_ptr<ClientInfo> GetClientInfo(const ClientKey &clientId);

    /**
     * @brief Check the UUID Validity.
     * @param[in] clientId The uuid of client connected with worker.
     * @return Status of the call.
     */
    Status CheckClientId(const ClientKey &clientId) const;

    /**
     * @brief Get the active client count.
     * @return size_t The client count.
     */
    size_t GetClientCount() const
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        return tbbClientTable_.size() - removableCount_;
    }

    /**
     * @brief After cleint obtains an FD from a worker, the worker records the cleint to the table.
     * @param[in] fds All FDs obtained by the client.
     * @param[in] clientId The client ID.
     */
    void SetClientId2WorkerFdMap(const ClientKey &clientId, const std::vector<int> &fds);

    /**
     * @brief Check whether client has expired worker fds.
     * @param[in] clientId The client ID.
     * @return The expired worker fds.
     */
    std::set<int> GetWorkerFdByClientId(const ClientKey &clientId);

    /**
     * @brief This function is invoked when some expired worker fds is released by client.
     * @param[in] clientId The client ID.
     * @param[in] fds The fds have been released.
     */
    void DelClientId2WorkerFdMap(const ClientKey &clientId, const std::vector<int> &fds);

    /**
     * @brief Check whether worker fds has been released by client.
     * @param[in] workerFds The worker fds which need to be released by client.
     * @return T/F
     */
    bool IsAllWorkerFdsReleased(const std::vector<int> &workerFds);

    /**
     * @brief Get tenant id by obtaining client info through client ID.
     * @param[in] clientId The client ID.
     * @return The tenant id.
     */
    std::string GetAuthTenantIdByClientId(const ClientKey &clientId, bool &clientExist);

    /**
     * @brief Check if exists clients on the same node with current worker by heartbeat type.
     * @return True if exists.
     */
    bool ExistClientsOnSameNode();

private:
    friend std::unique_ptr<ClientManager> std::make_unique<ClientManager>();
    ClientManager();

    /**
     * @brief Get lock id for client.
     * @param[out] lockId Lock id for client.
     * @return Status of the call.
     */
    Status GetLockId(uint32_t &lockId);

    /**
     * @brief Return unused lock id.
     * @param[in] lockId Lock id for client.
     */
    void ReturnLockId(uint32_t lockId);

    /**
     * @brief Check all clients health.
     */
    void CheckClientHealth();

    mutable std::shared_timed_mutex mutex_;  // Lock of 'tbbClientTable_'
    TbbClientInfoTable tbbClientTable_;      // Key is client uuid.

    mutable std::shared_timed_mutex clientId2WorkerFdsMapMutex_;
    std::unordered_map<std::string, std::set<int>> clientId2WorkerFdsMap_;

    // Protects 'freeLockIds_'.
    mutable std::mutex lockMutex_;

    // Free lock ID saved here to wait for assigning.
    // Data needs to be acquired in an orderly fashion to ensure that resources are reused as much as possible.
    std::set<uint32_t> freeLockIds_;

    std::shared_ptr<SockEventLoop> heartbeatEventLoop_{ nullptr };  // This event loop is used for uds type heartbeat
    std::unique_ptr<Thread> healthThread_{ nullptr };               // Pointer to the health-checking thread
    std::atomic<bool> healthThreadExit_{ false };
    WaitPost cvLock_;                            // wait for some second to check rpc type heartbeat timeout
    std::atomic<uint64_t> removableCount_{ 0 };  // removable count
};
}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_CLIENT_MANAGER_H
