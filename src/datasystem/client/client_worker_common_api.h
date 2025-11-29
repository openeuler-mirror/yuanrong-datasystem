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
 * Description: Define the worker client class to communicate with the common worker service.
 */
#ifndef DATASYSTEM_CLIENT_CLIENT_WORKER_COMMON_API_H
#define DATASYSTEM_CLIENT_CLIENT_WORKER_COMMON_API_H

#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/share_memory.stub.rpc.pb.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
static constexpr int MIN_HEARTBEAT_TIMEOUT_MS = 15 * 1000;
static constexpr int MAX_HEARTBEAT_TIMEOUT_MS = 60 * 1000;  // 60s, Maintain compatibility of EDA scenarios.
static constexpr int MIN_HEARTBEAT_INTERVAL_MS = 3 * 1000;
static constexpr int MAX_HEARTBEAT_INTERVAL_MS = 30 * 1000;  // 30s, Maintain compatibility of EDA scenarios.
constexpr int INVALID_SOCKET_FD = -1;
enum class HeartbeatType { NO_HEARTBEAT = 0, RPC_HEARTBEAT = 1, UDS_HEARTBEAT = 2 };
namespace client {
class ClientWorkerCommonApi {
public:
    /**
     * @brief Construct ClientWorkerApi.
     * @param[in] hostPort The address of worker node.
     * @param[in] cred The authentication credentials.
     * @param[in] heartbeatType The type of heartbeat.
     * @param[in] signature Used to do AK/SK authenticate.
     * @param[in] tenantId TenantId of client user.
     * @param[in] enableCrossNodeConnection Indicates whether the client can connect to the standby node.
     */
    explicit ClientWorkerCommonApi(HostPort hostPort, RpcCredential cred = {},
                                   HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                                   Signature *signature = nullptr, std::string tenantId = "",
                                   bool enableCrossNodeConnection = false, bool enableExclusiveConnection = false);

    virtual ~ClientWorkerCommonApi();

    /**
     * @brief Initialize the ClientWorkerApi Object.
     * @param[in] requestTimeoutMs The request timeout.
     * @param[in] connectTimeoutMs The connec  timeout.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    virtual Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs);

    /**
     * @brief Send heartbeat messages periodically.
     * @param[out] workerReboot Check whether the worker is restarted.
     * @param[out] clientRemoved Check whether the worker deletes the current client.
     * @param[in] remainTime Check remain time to set client rpc timeout.
     * @param[out] isWorkerVoluntaryScaleDown Check whether the worker is valutary scale down.
     * @param[in] releasedFds The worker fds that has been released on client.
     * @param[out] expiredWorkerFds The expired worker fds that need to be released by client.
     * @return Status of the call.
     */
    Status SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTime, bool &isWorkerVoluntaryScaleDown,
                         const std::vector<int64_t> &releasedFds, std::vector<int64_t> &expiredWorkerFds);

    /**
     * @brief Registering the client with the worker
     * @param[in] req The register client request pb.
     * @param[in] timeoutMs Register request timeout interval.
     * @return Status of the call.
     */
    Status RegisterClient(RegisterClientReqPb &req, int32_t timeoutMs);

    /**
     * @brief Create a function of handshake.
     * @param[in] fd Unix sock fd.
     * @param[in] sockPath Sock path.
     * @param[in] serverFd The FD of the worker server.
     * @return Status of the call.
     */
    Status CreateHandShakeFunc(UnixSockFd &fd, std::string &sockPath, int32_t &serverFd);

    /**
     * @brief Create a unix domain socket.
     * @param[in] timeoutMs Register request timeout interval.
     * @param[out] isConnectUdsSuccess Check whether the UDS is successfully connected..
     * @param[out] serverFd Returns the FD of the worker server..
     * @param[out] socketFd FD of the UNIX socket.
     * @return Status of the call.
     */
    Status CreateUnixDomainSocket(int32_t timeoutMs, bool &isConnectUdsSuccess, int32_t &serverFd, int32_t &socketFd);

    /**
     * @brief Get the domain socket fd with worker.
     * @return The domain socket fd with worker.
     */
    int GetSocketFd() const;

    /**
     * @brief Get the client id.
     * @return Return the client id.
     */
    std::string GetClientId() const;

    /**
     * @brief Get the ShmEnabled parameter.
     * @return Return true if the worker allows using shared memory; false otherwise.
     */
    bool GetShmEnabled() const;

    /**
     * @brief Instructs the worker to send the mmap FD to the client.
     * @param[in] workerFds Specifies the worker Fds to be sent.
     * @param[out] clientFds Indicates the received client Fds.
     * @param tenantId if tenant id is empty use threadlocal tenantid.
     * @return Status of the call.
     */
    Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds, const std::string &tenantId);

    /**
     * @brief Client Send disconnect notification to worker before exit.
     * @return Status of the call.
     */
    Status Disconnect();

    /**
     * @brief Obtains the actual heartbeat type..
     * @return heartbeat type.
     */
    HeartbeatType GetHeartbeatType() const;

    /**
     * @brief Reconnect to worker.
     * @return Status of the call.
     */
    virtual Status Reconnect();

    /**
     * @brief Get worker version.
     * @return Worker version.
     */
    uint32_t GetWorkerVersion() const
    {
        return workerVersion_.load(std::memory_order_relaxed);
    }

    uint32_t GetLockId() const
    {
        return lockId_;
    }

    bool ShmCreateable(uint64_t size) const
    {
        return shmEnabled_ && size >= shmThreshold_;
    }

    uint64_t GetShmThreshold() const
    {
        return shmThreshold_;
    }

    std::string GetWorkerUuid() const
    {
        return workerUuid_;
    }

    /**
     * @brief Save standby worker.
     * @param[in] standbyWorker The standby worker.
     * @param[in] availableWorkers The available workers.
     */
    void SaveStandbyWorker(const std::string standbyWorker,
                           const ::google::protobuf::RepeatedPtrField<std::string> &availableWorkers);

    /**
     * @brief Get the standby worker.
     * @return HostPort The standby worker.
     */
    std::vector<HostPort> GetStandbyWorkers();

    /**
     * @brief Get the worker host.
     * @return The host address.
     */
    std::string GetWorkHost() const
    {
        return hostPort_.Host();
    }

    int GetWorkPort() const
    {
        return hostPort_.Port();
    }

    int GetWorkHostPortINETFamily() const
    {
        if (hostPort_.IsIPv6()) {
            return AF_INET6;
        }
        return AF_INET;
    }

    /**
     * @brief Set the isUseStandbyWorker
     * @param[in] isUseStandbyWorker
     */
    void SetIsUseStandbyWorker(bool isUseStandbyWorker)
    {
        isUseStandbyWorker_ = isUseStandbyWorker;
    }

    /**
     * @brief Get the HeartBeatInterval.
     * @return int64_t The HeartBeatInterval.
     */
    int64_t GetHeartBeatInterval();

    /**
     * @brief Get client dead timeout.
     * @return int64_t The client dead timeout.
     */
    int64_t GetClientDeadTimeoutMs();

    /**
     * @brief Get connect timeout ms.
     * @return int32_t timeoutMs.
     */
    int32_t GetConnectTimeoutMs();

    /**
     * @brief Get the shmUnit of communication queue.
     * @return Return true if the worker allows using shared memory; false otherwise.
     */
    bool GetShmQueueUnit(std::shared_ptr<ShmUnitInfo> &decShmUnit)
    {
        if (!shmEnabled_ || decShmUnit_->fd <= 0) {
            return false;
        }
        decShmUnit = decShmUnit_;
        return true;
    }

    /**
     * @brief Get the huge tlb switch.
     * @return Return true if the worker allows optimize; false otherwise.
     */
    bool IsEnableHugeTlb()
    {
        return enableHugeTlb_;
    }

    /**
     * @brief Set client is removable or not.
     * @param[in] removable Removable client.
     * @return Previous value.
     */
    bool SetRemovable(bool removable)
    {
        return removable_.exchange(removable, std::memory_order_relaxed);
    }

    /**
     * @brief Increase invoke count.
     */
    void IncreaseInvokeCount()
    {
        invokeCount_.fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * @brief Decrease invoke count.
     */
    void DecreaseInvokeCount()
    {
        if (invokeCount_.fetch_sub(1, std::memory_order_relaxed) == 0) {
            LOG(WARNING) << "invoke count error happen!";
            invokeCount_.store(0, std::memory_order_relaxed);
        }
    }

    /**
     * @brief Return true if stub have invoke count to be processed.
     * @return True if stub have invoke count to be processed.
     */
    bool HaveInvokeCount() const
    {
        return invokeCount_.load(std::memory_order_relaxed) > 0;
    }

    uint64_t InvokeCount() const
    {
        return invokeCount_.load(std::memory_order_relaxed);
    }

    bool IsHealthy() const
    {
        return healthy_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Return true if enable cross node is enable.
     * @return True if enable cross node is enable.
     */
    bool EnableCrossNodeConnection() const
    {
        return enableCrossNodeConnection_;
    }

    bool IsWorkerEnableP2Ptransfer()
    {
        return workerEnableP2Ptransfer_;
    }

protected:
    template <class ReqType>
    void SetTenantId(ReqType &req)
    {
        req.set_tenant_id(g_ContextTenantId);
    }

    template <class ReqType>
    Status SetTokenAndTenantId(ReqType &req)
    {
        SetTenantId(req);
        return Status::OK();
    }

    /**
     * @brief Create connection with worker and register the client.
     * @param[in] req Register request.
     * @param[in] timeoutMs Timeout milliseconds.
     * @param[in] reconnection Check whether the connection is reconnection..
     * @return Status of the call.
     */
    Status Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection = false);

    /**
     * @brief Used to receive the fd of the shared memory.
     */
    void RecvPageFd();

    /**
     * @param[in] timeout The original timeout to adjust
     * @brief Set the rpc timeout.
     */
    void SetRpcTimeout(int32_t timeout);

    /**
     * @brief Compute a timeout value that will result in a new timeout that is a bit smaller than the input timeout
     * based on an adjustment algorithm.
     * @param[in] timeout The original timeout to adjust
     * @return The adjusted/new timeout value
     */
    static int64_t ClientGetRequestTimeout(int32_t timeout)
    {
        const int64_t CLIENT_TIMEOUT_MINUS_MILLISECOND = 1000;
        const float CLIENT_TIMEOUT_DESCEND_FACTOR = 0.9;
        return std::max(int64_t(timeout * CLIENT_TIMEOUT_DESCEND_FACTOR), timeout - CLIENT_TIMEOUT_MINUS_MILLISECOND);
    }

    /**
     * @brief Construct decShmUnit_ after register.
     * @param[in] rsp Register respond.
     */
    void ConstructDecShmUnit(const RegisterClientRspPb &rsp);

    /**
     * @brief close socket fd.
     */
    void CloseSocketFd();

    /**
     * @brief Set healthy state.
     * @param[in] health state.
     */
    void SetHealthy(bool healthy);

    /**
     * @brief Receive client fds after notify worker sending fds.
     * @param[in] workerFds Specifies the worker Fds to be sent.
     * @param[in] requestId The unique id of this request.
     * @param[in] timer Records the request timeout.
     * @param[out] clientFds Indicates the received client Fds.
     */
    void RecvFdAfterNotify(const std::vector<int> &workerFds, uint64_t requestId, const Timer &timer,
                           std::vector<int> &clientFds);

    /**
     * @brief This is the logic for processing the received fd.
     * @param[in] Status The status of RecvPageFd.
     * @param[in] requestId The unique id of this request.
     * @param[in] pageFds Indicates the received client Fds.
     */
    void PostRecvPageFd(const Status &status, uint64_t requestId, const std::vector<int> &pageFds);

    /**
     * @brief Close expired fds.
     */
    void CloseExpiredFd();

    static constexpr int32_t retryTimes_ = 3;
    HostPort hostPort_;
    RpcCredential cred_;
    // Protect 'standbyWorkerAddrs_'
    std::shared_timed_mutex standbyWorkerMutex_;
    std::unordered_set<HostPort> standbyWorkerAddrs_;
    bool isUseStandbyWorker_ = false;
    uint32_t pageSize_{ 0 };  // The page size used when reading files.
    HostPort masterAddress_;
    std::string clientId_;
    std::string workerStartId_;  // To judge whether the worker is restarted.
    std::string workerUuid_;     // The uuid for worker.

    std::atomic<int32_t> socketFd_{ -1 };

    bool shmEnabled_{ false };
    uint32_t lockId_{ 0 };
    std::unique_ptr<WorkerService_Stub> commonWorkerSession_{ nullptr };
    double workerTimeoutMult_{ 0.0 };  // Used for changing the default timeout in some cases.
    HeartbeatType heartbeatType_;

    // Worker version, increases 1 each time the worker recovers from a disconnection.
    std::atomic<uint32_t> workerVersion_{ 0 };

    std::atomic_bool storeNotifyReboot_{ false };
    Thread recvPageThread_;
    uint64_t shmThreshold_{ 0 };
    Signature *signature_;
    std::string tenantId_;
    int32_t requestTimeoutMs_{ 0 };
    int32_t connectTimeoutMs_{ 0 };
    int32_t rpcTimeoutMs_{ 0 };
    bool enableCrossNodeConnection_{ false };
    bool enableExclusiveConnection_{ false };
    int64_t heartBeatTimeoutMs_{ 0 };
    int64_t heartBeatIntervalMs_{ MIN_HEARTBEAT_INTERVAL_MS };
    uint64_t clientDeadTimeoutMs_{ 0 };
    std::shared_ptr<ShmUnitInfo> decShmUnit_;
    bool enableHugeTlb_ = { false };
    std::atomic<uint64_t> invokeCount_{ 0 };
    std::atomic_bool removable_{ false };
    std::atomic_bool healthy_{ false };
    bool workerEnableP2Ptransfer_ = false;
    struct RecvClientFdState {
        std::unique_ptr<WaitPost> recvPageWaitPost{ nullptr };
        std::unique_ptr<WaitPost> recvPageNotify{ nullptr };
        std::mutex mutex;  // for requestId2UnmmapedClientFds.
        std::unordered_map<uint64_t, std::pair<std::vector<int>, uint64_t>> requestId2UnmmapedClientFds;
        std::atomic<uint64_t> requestId{ 0 };
        int32_t getClientFdTimeoutMs{ 0 };
        std::atomic_bool stopRecvPageFd{ false };

        RecvClientFdState()
        {
            recvPageWaitPost = std::make_unique<WaitPost>();
            recvPageNotify = std::make_unique<WaitPost>();
        }
    };
    RecvClientFdState recvClientFdState_;
    static std::atomic<int32_t> exclusiveIdGen_;
    std::optional<int32_t> exclusiveId_;
    std::string exclusiveConnSockPath_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_CLIENT_WORKER_COMMON_API_H
