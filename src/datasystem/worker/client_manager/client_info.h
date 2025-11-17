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
 * Description: Defines client info class and client manager class.
 */
#ifndef DATASYSTEM_WORKER_CLIENT_INFO_H
#define DATASYSTEM_WORKER_CLIENT_INFO_H

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace worker {

enum class HeartbeatType { NO_HEARTBEAT = 0, RPC_HEARTBEAT = 1, UDS_HEARTBEAT = 2 };

class ClientInfo {
public:
    /**
     * @brief Constructor.
     * @param[in] socketFd Fd of the socket channel between the client and worker.
     * @param[in] clientId Uuid of the client.
     * @param[in] uniqueCount The number of shmUnitId in the client is unique or not.
     * @param[in] shmEnabled Indicates whether the client allows shared memory.
     * @param[in] tenantId The tenant id.
     * @param[in] enableCrossNode Client is enable cross node connection or not.
     * @param[in] podName Client pod name.
     */
    ClientInfo(int32_t socketFd, std::string clientId, bool uniqueCount, bool shmEnabled = true,
               std::string tenantId = "", bool enableCrossNode = false, const std::string &podName = "")
        : socketFd_(socketFd),
          clientId_(std::move(clientId)),
          uniqueCount_(uniqueCount),
          lockId_(UINT32_MAX),
          shmEnabled_(shmEnabled),
          tenantId_(std::move(tenantId)),
          enableCrossNode_(enableCrossNode),
          podName_(podName)
    {
    }

    ~ClientInfo() = default;

    /**
     * @brief Set whether the number of shmUnitId is unique.
     * @param[in] uniqueCount Is number of shmUnitId unique.
     */
    void SetUniqueCount(bool uniqueCount);

    /**
     * @brief Get current socket fd.
     * @return The socket fd connected with worker.
     */
    int32_t GetSocketFd() const;

    /**
     * @brief Get the id of client.
     * @return The unique sign of client.
     */
    const std::string &GetClientId() const;

    /**
     * @brief Add the shared memory unit used by the client.
     * @param[in] shmUnit Shared memory unit pointer to add.
     * @return True if add shmUnit successfully.
     */
    bool AddShmUnit(const std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Remove the shared memory unit used by the client.
     * @param[in] shmUnit Shared memory unit pointer to remove.
     * @return True if remove shmUnit successfully.
     */
    bool RemoveShmUnit(const std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Check one object whether be referred by client.
     * @param[in] shmId Shared memory unit id of object.
     * @return True if contains uuid.
     */
    bool Contains(const ShmKey &shmId) const;

#ifdef WITH_TESTS
    /**
     * @brief Get all share memory unit ids referred by client.
     * @param[out] shmUnitIds Shared memory unit id of objects.
     */
    void GetShmUnitIds(std::unordered_map<ShmKey, uint32_t> &shmUnitIds);
#endif

    /**
     * @brief Add the reader session id used by the client.
     * @param[in] sessionId Id of a reader session.
     * @return True if add read session successfully.
     */
    bool AddReaderSessionId(const std::string &sessionId);

    /**
     * @brief Add the writer session id used by the client.
     * @param[in] sessionId Id of a writer session.
     * @return True if add write session successfully.
     */
    bool AddWriterSessionId(const std::string &sessionId);

    /**
     * @brief Remove the reader session id used by the client.
     * @param[in] sessionId Id of a reader session.
     * @return Status of the call.
     */
    Status RemoveReaderSessionId(const std::string &sessionId);

    /**
     * @brief Remove the writer session id used by the client.
     * @param[in] sessionId Id of a writer session.
     * @return Status of the call.
     */
    Status RemoveWriterSessionId(const std::string &sessionId);

    /**
     * @brief Get all reader sessions' ids used by the client.
     * @param[out] sessionIds Sessions' ids of all the readers registered.
     */
    void GetReaderSessionIds(std::unordered_set<std::string> &sessionIds) const;

    /**
     * @brief Get all writer sessions' ids used by the client.
     * @param[out] sessionIds Sessions' ids of all the writers registered.
     */
    void GetWriterSessionIds(std::unordered_set<std::string> &sessionIds) const;

    /**
     * @brief Set lock id for client info.
     * @param[in] Lock id
     */
    void SetLockId(uint32_t lockId);

    /**
     * @brief Get client lock id.
     * @param[out] Lock id.
     * @return Status of the call.
     */
    Status GetLockId(uint32_t &lockId) const;

    /**
     * @brief Reset the last heartbeat.
     */
    void UpdateLastHeartbeat();

    /**
     * @brief Run the client lost handler when client lost.
     */
    void LostHandler();

    /**
     * @brief Set the client lost handler and heartbeat type.
     * @param[in] lostHandler The client lost handler function.
     * @param[in] heartbeatType The client heartbeat type.
     */
    void SetLostHandler(std::function<void()> lostHandler, HeartbeatType heartbeatType);

    /**
     * @brief Check whether the client is deactivated.
     * @return return client status.
     */
    bool IsClientLost();

    /**
     * @brief Check whether the client is deactivated.
     * @return return client status.
     */
    bool ShmEnabled();

    /**
     * @brief Get the tenant id.
     * @return return tenant id.
     */
    const std::string &GetTenantId() const
    {
        return tenantId_;
    }

    /**
     * @brief Return true if client is enable cross node.
     * @return True if client is enable cross node.
     */
    bool EnableCrossNode() const
    {
        return enableCrossNode_;
    }

    const std::string &PodName() const
    {
        return podName_;
    }

    /**
     * @brief Set client removable.
     * @param[in] removable Removable or not.
     * @return Previous value.
     */
    bool SetRemovable(bool removable)
    {
        bool prev = removable_.exchange(removable, std::memory_order_relaxed);
        if (prev != removable) {
            LOG(INFO) << FormatString("client %s, pod: %s removable tag (%d) -> (%d)", clientId_, podName_, prev,
                                      removable);
        }
        return prev;
    }

    /**
     * @brief Return true if client is removable.
     * @return True if client is removable.
     */
    bool Removable() const
    {
        return removable_.load(std::memory_order_relaxed);
    }

private:
    std::mutex mutex_;
    int32_t socketFd_;
    std::string clientId_;
    bool uniqueCount_;
    uint32_t lockId_;
    bool shmEnabled_;
    std::function<void()> lostHandler_ = nullptr;  // Run this handler when client lost
    Timer lastHeartbeat_;                          // Time received the last heartbeat.
    std::atomic<HeartbeatType> heartbeatType_{ HeartbeatType::NO_HEARTBEAT };
    std::unordered_set<int> fds_;
    std::unordered_map<ShmKey, uint32_t> shmUnitIds_;
    std::unordered_set<std::string> readerSessionTable_;  // session id of readers
    std::unordered_set<std::string> writerSessionTable_;  // session id of writers
    std::string tenantId_;
    bool enableCrossNode_;
    std::atomic<bool> removable_{ false };
    const std::string podName_;
};

}  // namespace worker
}  // namespace datasystem

#endif