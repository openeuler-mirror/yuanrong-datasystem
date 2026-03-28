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
 * Description: Defines the worker client class to communicate with the worker service.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_REMOTE_API_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_REMOTE_API_H

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "datasystem/client/embedded_client_worker_api.h"
#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/client/object_cache/client_worker_api/client_worker_base_api.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/hetero/device_common.h"
#include "datasystem/object/buffer.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ClientWorkerRemoteApi : public ClientWorkerBaseApi, public client::ClientWorkerRemoteCommonApi {
public:
    /**
     * @brief Construct ClientWorkerApi.
     * @param[in] hostPort The address of the worker node.
     * @param[in] cred The authentication credentials.
     * @param[in] heartbeatType The type of heartbeat.
     * @param[in] signature Used to do AK/SK authenticate.
     * @param[in] tenantId The tenant id.
     * @param[in] enableCrossNodeConnection Indicates whether the client can connect to the standby node.
     * @param[in] enableExclusiveConnection Indicates whether the client will use exclusive, per-thread connections
     */
    explicit ClientWorkerRemoteApi(HostPort hostPort, RpcCredential cred,
                                   HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                                   SensitiveValue token = "", Signature *signature = nullptr, std::string tenantId = "",
                                   bool enableCrossNodeConnection = false, bool enableExclusiveConnection = false);

    ~ClientWorkerRemoteApi() = default;

    /**
     * @brief Initialize ClientWorkerApi.
     * @param[in] requestTimeoutMs Request Timeout milliseconds.
     * @param[in] connectTimeoutMs Connect Timeout milliseconds.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs, uint64_t fastTransportSize = 0) override;

    std::shared_ptr<IClientWorkerApi> CloneWith(
        HostPort hostPort, RpcCredential cred, HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
        SensitiveValue token = "", Signature *signature = nullptr, std::string tenantId = "",
        bool enableCrossNodeConnection = false, bool enableExclusiveConnection = false,
        std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api = nullptr,
        void *worker = nullptr) const override
    {
        (void)api;
        (void)worker;
        return std::make_shared<ClientWorkerRemoteApi>(std::move(hostPort), cred, heartbeatType, std::move(token),
                                                       signature, tenantId, enableCrossNodeConnection,
                                                       enableExclusiveConnection);
    };

    Status Create(const std::string &objectKey, int64_t dataSize, uint32_t &version, uint64_t &metadataSize,
                  std::shared_ptr<ShmUnitInfo> &shmBuf, std::shared_ptr<UrmaRemoteAddrPb> &urmaDataInfo,
                  const CacheType &cacheType = CacheType::MEMORY) override;
    Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal,
                   const std::unordered_set<std::string> &nestedKeys = {}, uint32_t ttlSecond = 0,
                   int existence = 0) override;
    Status MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo, const PublishParam &param,
                        MultiPublishRspPb &rsp, const std::vector<std::vector<uint64_t>> &blobSizes = {}) override;
    Status DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys) override;
    Status Get(const GetParam &getParam, uint32_t &version, GetRspPb &rsp, std::vector<RpcMessage> &payloads) override;
    Status InvalidateBuffer(const std::string &objectKey) override;
    Status GIncreaseWorkerRef(const std::vector<std::string> &firstIncIds, std::vector<std::string> &failedObjectKeys,
                              const std::string &remoteClientId = "") override;
    Status ReleaseGRefs(const std::string &remoteClientId) override;
    Status GDecreaseWorkerRef(const std::vector<std::string> &finishDecIds, std::vector<std::string> &failedObjectKeys,
                              const std::string &remoteClientId = "") override;
    Status Delete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys,
                  bool areDeviceObjects = false) override;
    Status QueryGlobalRefNum(
        const std::vector<std::string> &objectKeys,
        std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap) override;
    Status PublishDeviceObject(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, size_t dataSize, bool isShm,
                               void *nonShmPointer) override;
    Status GetDeviceObject(const std::vector<std::string> &devObjKeys, uint64_t dataSize, int32_t timeoutMs,
                           GetDeviceObjectRspPb &rsp, std::vector<RpcMessage> &payloads) override;
    Status SubscribeReceiveEvent(int32_t deviceId, SubscribeReceiveEventRspPb &resp) override;
    Status PutP2PMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, const std::vector<Blob> &blobs) override;
    Status GetP2PMeta(std::vector<std::shared_ptr<DeviceBufferInfo>> &bufferInfoList,
                      std::vector<DeviceBlobList> &devBlobList, GetP2PMetaRspPb &resp,
                      int64_t subTimeoutMs = 500) override;
    Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp) override;
    Status RecvRootInfo(RecvRootInfoReqPb &req, RecvRootInfoRspPb &resp) override;
    Status GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs) override;
    Status AckRecvFinish(AckRecvFinishReqPb &req) override;
    Status RemoveP2PLocation(const std::string &objectKey, int32_t deviceId) override;
    Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                          std::vector<ObjMetaInfo> &objMetas) override;
    Status MultiCreate(bool skipCheckExistence, std::vector<MultiCreateParam> &createParams, uint32_t &version,
                       std::vector<bool> &exists, bool &useShmTransfer) override;
    Status QuerySize(const std::vector<std::string> &objectKeys, QuerySizeRspPb &rsp) override;
    Status HealthCheck(ServerState &state) override;
    Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache,
                 const bool isLocal) override;
    Status Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds,
                  std::vector<std::string> &failedKeys) override;
    Status GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey, GetMetaInfoRspPb &metaInfos) override;
    Status ReconnectWorker(const std::vector<std::string> &gRefIds) override;
    Status PrepairForDecreaseShmRef(
        std::function<Status(const std::string &, const std::shared_ptr<ShmUnitInfo> &)> mmapFunc) override;
    Status CleanUpForDecreaseShmRefAfterWorkerLost() override;
    Status DecreaseShmRef(const ShmKey &shmId, const std::function<Status()> &connectCheck,
                          std::shared_timed_mutex &mtx) override;

private:
#ifdef USE_URMA
    uint64_t ResolveUBGetSize(const GetParam &getParam, const std::string &tenantId);
#endif

    /**
     * @brief Try to get Shm lock for decrease buffer.
     * @param[in] timeoutStruct Time out for futex wait.
     * @param[out] retryCount Wait and retry using second intervals to avoid excessive time spent reconnecting to the
     * worker.And add retryCount to control retry nums.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddShmLockForClient(const struct timespec &timeoutStruct, int64_t &retryCount);

    /**
     * @brief Waiting to be woken up by worker, via shm futex
     * @param[in] waitFlag The flag ptr in shm.
     * @param[in] waitNum Wake up flag.
     * @param[in] timeoutStruct Time out for futex wait.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckShmFutexResult(uint32_t *waitFlag, uint32_t waitNum, struct timespec &timeoutStruct);

    /**
     * @brief Check if get response all failed.
     * @param[in] rsp The response getting from worker.
     * @return Return true if all get failed.
     */
    bool IsAllGetFailed(GetRspPb &rsp);

    /**
     * @brief Phase1 only: send Publish(use_ub=true), store urma_info in bufferInfo for Create+MemoryCopy+Publish path.
     * @param[in] bufferInfo Buffer information; on success, ubUrmaInfoOpaque is set to a new UrmaRemoteAddrPb.
     * @return K_OK on success; the error code otherwise.
     */
    Status PublishPhase1Only(const std::shared_ptr<ObjectBufferInfo> &bufferInfo);

    /**
     * @brief Send Publish phase2 only (publish_complete_ub=true). Used by UB path after data is sent via UrmaWrite.
     * @param[in] bufferInfo Buffer information.
     * @param[in] isSeal Is seal or not.
     * @param[in] nestedKeys Nested keys.
     * @param[in] ttlSecond TTL in seconds.
     * @param[in] existence Existence option for state api.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendPublishPhase2(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isSeal,
                             const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond, int existence);

    /**
     * @brief Create communication circular queue based on shared memory.
     * @return K_OK on success; the error code otherwise.
     */
    Status InitDecreaseQueue();

    // To protect the decreaseRPCQ_ and waitRespMap_ from being manipulated by different threads of the same client.
    mutable std::mutex mtx_;
    std::unordered_map<int, uint8_t *> waitRespMap_;
    std::shared_ptr<ShmCircularQueue> decreaseRPCQ_{ nullptr };
    std::unique_ptr<WorkerOCService::Stub> stub_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H
