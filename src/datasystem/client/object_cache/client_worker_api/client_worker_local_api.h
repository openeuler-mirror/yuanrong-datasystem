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
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_LOCAL_API_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_LOCAL_API_H

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
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ClientWorkerLocalApi : public ClientWorkerBaseApi, public client::ClientWorkerLocalCommonApi {
public:
    ClientWorkerLocalApi(HostPort hostPort, std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api,
                         void *worker, HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                         Signature *signature = nullptr, bool enableCrossNodeConnection = false);

    ~ClientWorkerLocalApi() = default;

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
        (void)cred;
        (void)token;
        (void)tenantId;
        (void)enableExclusiveConnection;
        return std::make_shared<ClientWorkerLocalApi>(std::move(hostPort), std::move(api), worker, heartbeatType,
                                                      signature, enableCrossNodeConnection);
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
    Status MemView2RpcMessage(const std::vector<MemView> &mvs, std::vector<RpcMessage> &rms);

    void *workerOCService_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H
