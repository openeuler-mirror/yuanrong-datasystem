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
#include <functional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/token/client_access_token.h"
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

#include "datasystem/client/embedded_client_worker_api.h"

namespace datasystem {
static constexpr int MIN_HEARTBEAT_TIMEOUT_MS = 15 * 1000;
static constexpr int MAX_HEARTBEAT_TIMEOUT_MS = 60 * 1000;  // 60s, Maintain compatibility of EDA scenarios.
static constexpr int MIN_HEARTBEAT_INTERVAL_MS = 3 * 1000;
static constexpr int MAX_HEARTBEAT_INTERVAL_MS = 30 * 1000;  // 30s, Maintain compatibility of EDA scenarios.
constexpr int INVALID_SOCKET_FD = -1;
enum class HeartbeatType { NO_HEARTBEAT = 0, RPC_HEARTBEAT = 1, UDS_HEARTBEAT = 2 };
enum class ShmEnableType { NONE, UDS, SCMTCP };
namespace client {

struct ClientWorkerCommonApiAttribute {
    ClientWorkerCommonApiAttribute(HostPort hostPort, HeartbeatType heartbeatType, bool enableCrossNodeConnection,
                                   Signature *signature)
        : socketFd_(-1),
          heartbeatType_(heartbeatType),
          hostPort_(std::move(hostPort)),
          enableCrossNodeConnection_(enableCrossNodeConnection),
          signature_(signature),
          workerSupportMultiShmRefCount_(true)
    {
    }

    virtual ~ClientWorkerCommonApiAttribute();

    bool ShmCreateable(uint64_t size) const
    {
        return IsShmEnable() && size >= shmThreshold_;
    }

    bool IsShmEnable() const
    {
        return shmEnableType_ != ShmEnableType::NONE;
    }

    bool IsShmEnableByUDS() const
    {
        return shmEnableType_ == ShmEnableType::UDS;
    }

    int GetWorkHostPortINETFamily() const
    {
        if (hostPort_.IsIPv6()) {
            return AF_INET6;
        }
        return AF_INET;
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
        // Fixme: not atomic.
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

    /**
     * @brief Set healthy state.
     * @param[in] health state.
     */
    void SetHealthy(bool healthy)
    {
        bool prev = healthy_.exchange(healthy, std::memory_order_relaxed);
        if (prev != healthy) {
            LOG(INFO) << FormatString("client %s health state: (%d) -> (%d)", clientId_, prev, healthy);
        }
    }

    std::atomic<int32_t> socketFd_{ -1 };
    std::string clientId_;
    ShmEnableType shmEnableType_{ ShmEnableType::NONE };
    uint64_t shmThreshold_{ 0 };
    HeartbeatType heartbeatType_;
    // Worker version, increases 1 each time the worker recovers from a disconnection.
    std::atomic<uint32_t> workerVersion_{ 0 };
    uint32_t lockId_{ 0 };
    std::string workerId_;  // The ID of worker.
    HostPort hostPort_;
    bool isUseStandbyWorker_ = false;
    int64_t heartBeatIntervalMs_{ MIN_HEARTBEAT_INTERVAL_MS };
    uint64_t clientDeadTimeoutMs_{ 0 };
    int32_t connectTimeoutMs_{ 0 };
    bool enableHugeTlb_ = { false };
    std::atomic_bool removable_{ false };
    std::atomic_bool healthy_{ false };
    bool enableCrossNodeConnection_{ false };
    bool workerEnableP2Ptransfer_ = false;
    std::shared_ptr<ShmUnitInfo> decShmUnit_;
    Signature *signature_;
    bool workerSupportMultiShmRefCount_;

protected:
    void SetHeartbeatProperties(int32_t timeoutMs, const RegisterClientRspPb &rsp);

    int64_t heartBeatTimeoutMs_{ 0 };

private:
    std::atomic<uint64_t> invokeCount_{ 0 };
};

class IClientWorkerCommonApi : public ClientWorkerCommonApiAttribute {
public:
    IClientWorkerCommonApi(HostPort hostPort, HeartbeatType heartbeatType, bool enableCrossNodeConnection,
                           Signature *signature)
        : ClientWorkerCommonApiAttribute(std::move(hostPort), heartbeatType, enableCrossNodeConnection, signature)
    {
    }

    virtual ~IClientWorkerCommonApi();

    /**
     * @brief Initialize the ClientWorkerApi Object.
     * @param[in] requestTimeoutMs The request timeout.
     * @param[in] connectTimeoutMs The connec  timeout.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    virtual Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs) = 0;

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
    virtual Status SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTime,
                                 bool &isWorkerVoluntaryScaleDown, const std::vector<int64_t> &releasedFds,
                                 std::vector<int64_t> &expiredWorkerFds) = 0;

    /**
     * @brief Instructs the worker to send the mmap FD to the client.
     * @param[in] workerFds Specifies the worker Fds to be sent.
     * @param[out] clientFds Indicates the received client Fds.
     * @param tenantId if tenant id is empty use threadlocal tenantid.
     * @return Status of the call.
     */
    virtual Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                               const std::string &tenantId) = 0;

    /**
     * @brief Client Send disconnect notification to worker before exit.
     * @return Status of the call.
     */
    virtual Status Disconnect(bool isDestruct) = 0;

    /**
     * @brief Reconnect to worker.
     * @return Status of the call.
     */
    virtual Status Reconnect() = 0;

    /**
     * @brief Get the standby worker.
     * @return HostPort The standby worker.
     */
    virtual std::vector<HostPort> GetStandbyWorkers() = 0;

    /**
     * @brief Update token for yr iam
     * @param[in] Token message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status UpdateToken(SensitiveValue &token) = 0;

    /**
     * @brief Update aksk for yr iam
     * @param[in] acessKey message for auth certification
     * @param[in] secretKey message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey) = 0;

protected:
    virtual Status SetToken(std::string &token) = 0;
    virtual void SetTenantId(std::string &tenantId) = 0;
    template <class ReqType>
    Status SetTokenAndTenantId(ReqType &req)
    {
        std::string &tenantId = *req.mutable_tenant_id();
        std::string &token = *req.mutable_token();
        SetTenantId(tenantId);
        return SetToken(token);
    }

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
     * @brief Create connection with worker and register the client.
     * @param[in] req Register request.
     * @param[in] timeoutMs Timeout milliseconds.
     * @param[in] reconnection Check whether the connection is reconnection..
     * @return Status of the call.
     */
    virtual Status Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection = false) = 0;
};

class ClientWorkerLocalCommonApi : virtual public IClientWorkerCommonApi {
public:
    explicit ClientWorkerLocalCommonApi(HostPort hostPort,
                                        std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api,
                                        void *worker, HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                                        bool enableCrossNodeConnection = false, Signature *signature = nullptr);

    virtual ~ClientWorkerLocalCommonApi() = default;

    virtual Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs) override;
    Status SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTime, bool &isWorkerVoluntaryScaleDown,
                         const std::vector<int64_t> &releasedFds, std::vector<int64_t> &expiredWorkerFds) override;
    Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                       const std::string &tenantId) override;
    Status Disconnect(bool isDestruct) override;
    Status Reconnect() override;
    std::vector<HostPort> GetStandbyWorkers() override;
    Status UpdateToken(SensitiveValue &token) override;
    Status UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey) override;

protected:
    Status SetToken(std::string &tokenRef) override
    {
        (void)tokenRef;
        return Status::OK();
    }

    void SetTenantId(std::string &tenantId) override
    {
        (void)tenantId;
    }

    Status Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection = false) override;

    std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api_{ nullptr };
    void *worker_{ nullptr };

private:
    void *workerService_{ nullptr };
};

class ClientWorkerRemoteCommonApi : virtual public IClientWorkerCommonApi {
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
    explicit ClientWorkerRemoteCommonApi(HostPort hostPort, RpcCredential cred = {},
                                         HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                                         SensitiveValue token = "", Signature *signature = nullptr,
                                         std::string tenantId = "", bool enableCrossNodeConnection = false,
                                         bool enableExclusiveConnection = false);

    virtual ~ClientWorkerRemoteCommonApi();

    virtual Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs) override;
    Status SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTime, bool &isWorkerVoluntaryScaleDown,
                         const std::vector<int64_t> &releasedFds, std::vector<int64_t> &expiredWorkerFds) override;
    Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                       const std::string &tenantId) override;
    Status Disconnect(bool isDestruct) override;
    Status Reconnect() override;
    std::vector<HostPort> GetStandbyWorkers() override;
    Status UpdateToken(SensitiveValue &token) override;
    Status UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey) override;

    bool IsWorkerSupportMultiShmRefCount() const
    {
        return workerSupportMultiShmRefCount_;
    }

protected:
    template <class ReqType>
    Status SetToken(ReqType &req)
    {
        SensitiveValue token;
        RETURN_IF_NOT_OK(clientAccessToken_->UpdateAccessToken(token));
        if (!token.Empty()) {
            req.set_token(token.GetData(), token.GetSize());
        }
        return Status::OK();
    }

    Status SetToken(std::string &tokenRef) override
    {
        SensitiveValue token;
        RETURN_IF_NOT_OK(clientAccessToken_->UpdateAccessToken(token));
        if (!token.Empty()) {
            tokenRef = { token.GetData(), token.GetSize() };
        }
        return Status::OK();
    }

    void SetTenantId(std::string &tenantId) override
    {
        tenantId = g_ContextTenantId;
    }

    Status Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection = false) override;

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
     * @brief close socket fd.
     */
    void CloseSocketFd();

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
    RpcCredential cred_;
    // Protect 'standbyWorkerAddrs_'
    std::shared_timed_mutex standbyWorkerMutex_;
    std::unordered_set<HostPort> standbyWorkerAddrs_;
    HostPort masterAddress_;
    std::string workerStartId_;  // To judge whether the worker is restarted.

    std::unique_ptr<WorkerService_Stub> commonWorkerSession_{ nullptr };

    std::atomic_bool storeNotifyReboot_{ false };
    Thread recvPageThread_;
    std::string tenantId_;
    std::unique_ptr<ClientAccessToken> clientAccessToken_;
    int32_t requestTimeoutMs_{ 0 };
    int32_t rpcTimeoutMs_{ 0 };
    bool enableExclusiveConnection_{ false };
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
    uint32_t shmWorkerPort_{ 0 };

private:
    /**
     * @brief Called after RegisterClient succeeds.
     * @param[in] timeoutMs Timeout used for register.
     * @param[in] rsp Register response.
     */
    virtual void PostRegisterClient(int32_t timeoutMs, const RegisterClientRspPb &rsp);

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
    Status CreateHandShakeFunc(UnixSockFd &fd, const std::string &sockPath, int32_t &serverFd);

    /**
     * @brief Determine the endpoint and connection type for shared memory transfer.
     * @param[in] reply The response containing socket path or port.
     * @param[out] endpointStr The formatted endpoint string.
     * @param[out] shmEnableType The shared-memory connection type.
     * @return True if a shared memory endpoint was found; false otherwise.
     */
    bool PrepareShmTransferEndpoint(const GetSocketPathRspPb &reply, std::string &endpointStr,
                                    ShmEnableType &shmEnableType);

    /**
     * @brief Create a unix domain socket.
     * @param[in] timeoutMs Register request timeout interval.
     * @param[out] isConnectSuccess Check whether the UDS is successfully connected..
     * @param[out] serverFd Returns the FD of the worker server..
     * @param[out] socketFd FD of the UNIX socket.
     * @param[out] shmEnableType Shared-memory connection type.
     * @return Status of the call.
     */
    Status CreateConnectionForTransferShmFd(int32_t timeoutMs, bool &isConnectSuccess, int32_t &serverFd,
                                            int32_t &socketFd, ShmEnableType &shmEnableType);

    /**
     * @brief Construct decShmUnit_ after register.
     * @param[in] rsp Register respond.
     */
    void ConstructDecShmUnit(const RegisterClientRspPb &rsp);

    /**
     * @brief Save standby worker.
     * @param[in] standbyWorker The standby worker.
     * @param[in] availableWorkers The available workers.
     */
    void SaveStandbyWorker(const std::string standbyWorker,
                           const ::google::protobuf::RepeatedPtrField<std::string> &availableWorkers);

    Status FastTransportHandshake(const RegisterClientRspPb &rsp);
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_CLIENT_WORKER_COMMON_API_H
