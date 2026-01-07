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
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/client/listen_worker.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/mmap/immap_table.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/fd_pass.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/queue/shm_circular_queue.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/hetero/device_common.h"
#include "datasystem/object/buffer.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.service.rpc.pb.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"

namespace datasystem {
namespace object_cache {

struct MultiCreateParam {
    MultiCreateParam(size_t index, const std::string &objectKey, size_t dataSize)
        : index(index), objectKey(objectKey), dataSize(dataSize)
    {
        shmBuf = std::make_shared<ShmUnitInfo>();
    }

    size_t index;
    const std::string &objectKey;
    size_t dataSize;
    size_t metadataSize;
    std::shared_ptr<ShmUnitInfo> shmBuf;
};

struct PublishParam {
    bool isTx;
    bool isReplica;
    ExistenceOpt existence;
    uint32_t ttlSecond;
};

struct GetParam {
    const std::vector<std::string> &objectKeys;
    int64_t subTimeoutMs;
    const std::vector<ReadParam> &readParams;
    bool queryL2Cache;
    bool isRH2DSupported = false;
};

class IClientWorkerApi : virtual public client::IClientWorkerCommonApi {
public:
    explicit IClientWorkerApi(HostPort hostPort, HeartbeatType heartbeatType, bool enableCrossNodeConnection)
        : client::IClientWorkerCommonApi(std::move(hostPort), heartbeatType, enableCrossNodeConnection)
    {
    }

    ~IClientWorkerApi() = default;

    virtual std::shared_ptr<IClientWorkerApi> CloneWith(HostPort hostPort, RpcCredential cred,
                                                        HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                                                        SensitiveValue token = "", Signature *signature = nullptr,
                                                        std::string tenantId = "",
                                                        bool enableCrossNodeConnection = false,
                                                        bool enableExclusiveConnection = false) const = 0;

    /**
     * @brief Create an object in the store-server, will only be called in shm case.
     * @param[in] objectKey The ID of the object to create.
     * @param[in] dataSize The size in bytes of the space to be allocated for this object.
     * @param[in] cacheType The cache type of the object to be allocted, defalut is MEMORY.
     * @param[out] version Worker version.
     * @param[out] metadataSize The metadata size.
     * @param[out] shmBuf The address of the newly created object will be written here.
     * @return K_OK on success; the error code otherwise.
     *         K_RUNTIME_ERROR: client fd mmap failed.
     *         K_DUPLICATED: the object already exists, no need to create.
     */
    virtual Status Create(const std::string &objectKey, int64_t dataSize, uint32_t &version, uint64_t &metadataSize,
                          std::shared_ptr<ShmUnitInfo> &shmBuf, const CacheType &cacheType = CacheType::MEMORY) = 0;

    /**
     * @brief Publish/Seal/Put an object in the store-server.
     * @param[in] bufferInfo Buffer information.
     * @param[in] isShm Is shared memory or not.
     * @param[in] isSeal Is seal or not.
     * @param[in] nestedKeys Nested keys.
     * @param[in] ttlSecond Used by state api, means how many seconds the key will be delete automatically.
     * @param[in] existence Used by state api, to determine whether to set or not set the key if it does already
     * exist.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal,
                           const std::unordered_set<std::string> &nestedKeys = {}, uint32_t ttlSecond = 0,
                           int existence = 0) = 0;

    /**
     * @brief Publish multiple objects in the store-server.
     * @param[in] bufferInfo Buffers information.
     * @param[in] param The publish param.
     * @param[out] rsp MultiPublishRspPb rsp.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo,
                                const PublishParam &param, MultiPublishRspPb &rsp,
                                const std::vector<std::vector<uint64_t>> &blobSizes = {}) = 0;

    /**
     * @brief Decrease the object worker reference count by one and if no client holds the object, release it.
     * @param[in] objectKey The ID of the object to decrease ref.
     * @return Status of the call.
     */
    virtual Status DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys) = 0;

    /**
     * @brief Send getting object rpc request to worker.
     * @param[in] getParam The params for get operation.
     * @param[out] version Worker version.
     * @param[out] rsp The response getting from worker.
     * @param[out] payloads The payload getting from worker.
     * @return Status of the call.
     */
    virtual Status Get(const GetParam &getParam, uint32_t &version, GetRspPb &rsp,
                       std::vector<RpcMessage> &payloads) = 0;

    /**
     * @brief Send invalidate buffer rpc request to worker.
     * @param[in] objectKey The object key which binds to the buffer.
     * @return Status of the call.
     */
    virtual Status InvalidateBuffer(const std::string &objectKey) = 0;

    /**
     * @brief Increase the global reference count to objects in worker.
     * @param[in] firstIncIds The object keys to increase in worker, it cannot be empty.
     * @param[out] failedObjectKeys Increase failed object keys.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status GIncreaseWorkerRef(const std::vector<std::string> &firstIncIds,
                                      std::vector<std::string> &failedObjectKeys,
                                      const std::string &remoteClientId = "") = 0;

    /**
     * @brief Release obj Ref of remote client id.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status ReleaseGRefs(const std::string &remoteClientId) = 0;

    /**
     * @brief Decrease the global reference count to objects in worker.
     * @param[in] finishDecIds The object keys to decrease  in worker, it cannot be empty.
     * @param[out] failedObjectKeys Decrease failed object keys.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status GDecreaseWorkerRef(const std::vector<std::string> &finishDecIds,
                                      std::vector<std::string> &failedObjectKeys,
                                      const std::string &remoteClientId = "") = 0;

    /**
     * @brief Delete the given objects.
     * @param[in] objectKeys The vector of the object key.
     * @param[out] failedObjectKeys The vector of the failed delete object key list.
     * @param[in] areDeviceObjects The objectKeys are device objects.
     * @return Status of the call.
     */
    virtual Status Delete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys,
                          bool areDeviceObjects = false) = 0;

    /**
     * @brief Query all objects global references in the cluster.
     * @param[in] objectKeys The specific objects id that to query.
     * @param[out] gRefMap The global references distribution.
     * @return Status K_OK on success; the error code otherwise.
     */
    virtual Status QueryGlobalRefNum(
        const std::vector<std::string> &objectKeys,
        std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap) = 0;

    /**
     * @brief Publish an device object in the worker.
     * @param[in] bufferInfo Device Buffer information.
     * @param[in] dataSize The size of data.
     * @param[in] isShm Is shared memory or not.
     * @param[in] nonShmPointer The data pointer if is not shared memory.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status PublishDeviceObject(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, size_t dataSize, bool isShm,
                                       void *nonShmPointer) = 0;

    /**
     * @brief Send device getting object rpc request to worker.
     * @param[in] devObjKeys The vector of the device object key.
     * @param[in] dataSize The size of the output data.
     * @param[in] timeoutMs The timeout of rpc.
     * @param[out] rsp The response getting from worker.
     * @param[out] payloads The payload getting from worker.
     * @return Status of the call.
     */
    virtual Status GetDeviceObject(const std::vector<std::string> &devObjKeys, uint64_t dataSize, int32_t timeoutMs,
                                   GetDeviceObjectRspPb &rsp, std::vector<RpcMessage> &payloads) = 0;

    /**
     * @brief Subscribe receive event from worker.
     * @param[in] deviceId The deviceid to which data belongs.
     * @param[out] resp The response getting from worker.
     * @return Status of the call
     */
    virtual Status SubscribeReceiveEvent(int32_t deviceId, SubscribeReceiveEventRspPb &resp) = 0;

    /**
     * @brief Put the p2p metadata to worker.
     * @param[in] bufferInfo The info of device buffer.
     * @param[in] blobs The list of device blob.
     * @return Status of the call
     */
    virtual Status PutP2PMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, const std::vector<Blob> &blobs) = 0;

    /**
     * @brief Get the p2p metadata from worker.
     * @param[in] bufferInfoList The info of device buffer.
     * @param[in] devBlobList The list of device blob.
     * @param[out] resp The response of the rpc call.
     * @param[in] subTimeoutMs The maximum time elapse of subscriptions.
     * @return Status of the call
     */
    virtual Status GetP2PMeta(std::vector<std::shared_ptr<DeviceBufferInfo>> &bufferInfoList,
                              std::vector<DeviceBlobList> &devBlobList, GetP2PMetaRspPb &resp,
                              int64_t subTimeoutMs = 500) = 0;

    /**
     * @brief Send the root info to worker.
     * @param[in] req The request of the call.
     * @param[out] resp The response of the call.
     * @return Status of the call.
     */
    virtual Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp) = 0;

    /**
     * @brief Receive the root info from worker.
     * @param[in] req The request of the call.
     * @param[out] resp The response of the call.
     * @return Status of the call.
     */
    virtual Status RecvRootInfo(RecvRootInfoReqPb &req, RecvRootInfoRspPb &resp) = 0;

    /**
     * @brief Obtains the BlobInfos, including the number of DataInfo, and the count and DataType of each DataInfo.
     * @param[in] devObjKey The object key.
     * @param[in] timeoutMs Waiting for the result return if object not ready. A positive integer number required.
     * 0 means no waiting time allowed. And the range is [0, INT32_MAX].
     * @param[out] blobs The list of blob info. (Include pointer、count and data type)
     * @return K_OK on any object success; the error code otherwise.
     */
    virtual Status GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs) = 0;

    /**
     * @brief Acknowledge the get operation is ok, and indicate whether worker as data providers
     * @param[in] req The AckRecvFinishReqPb request protobuf message.
     * @return Status of the call.
     */
    virtual Status AckRecvFinish(AckRecvFinishReqPb &req) = 0;

    /**
     * @brief Remove the location of device object
     * @param[in] objectKey The object key.
     * @param[in] deviceId The device id of this location.
     * @return K_OK on any object success; the error code otherwise.
     */
    virtual Status RemoveP2PLocation(const std::string &objectKey, int32_t deviceId) = 0;

    /**
     * @brief Get meta info of the given objects
     * @param[in] tenantId The tenant that the objs belong to.
     * @param[in] objectKeys The vector of the object key.
     * @param[out] objMetas The vector of the return metas.
     * @return K_OK on any object success.
     *         K_INVALID: the vector of keys is empty or include invalid key.
     *         K_RPC_UNAVAILABLE: Disconnect from worker or master.
     *         K_NOT_AUTHORIZED: The client is not authorized for the tenantId.
     */
    virtual Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                                  std::vector<ObjMetaInfo> &objMetas) = 0;

    /**
     * @brief Create multiple objects in the store-server.
     * @param[in]  skipCheckExistence Whether skip existence check.
     * @param[out] createParams The params for create operation.
     * @param[out] version Object version.
     * @param[out] exists The Key exist list.
     * @param[out] useShmTransfer Transfer by shm.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status MultiCreate(bool skipCheckExistence, std::vector<MultiCreateParam> &createParams, uint32_t &version,
                               std::vector<bool> &exists, bool &useShmTransfer) = 0;

    /**
     * @brief Invoke worker client to query the size of objectKeys (include the objectKeys of other AZ).
     * @param[in] objectKeys The objectKeys need to query size.
     * @param[out] rsp The response getting from worker.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: The objectKeys are empty or invalid.
     *         K_NOT_FOUND: All objectKeys not found.
     *         K_RPC_UNAVAILABLE: Network error.
     *         K_NOT_READY: Worker not ready.
     *         K_RUNTIME_ERROR: Can not get objectKey size from worker.
     */
    virtual Status QuerySize(const std::vector<std::string> &objectKeys, QuerySizeRspPb &rsp) = 0;

    /**
     * @brief Worker health check.
     * @param[out] state The state of ds service.
     * @return K_OK on any object success; the error code otherwise.
     */
    virtual Status HealthCheck(ServerState &state) = 0;

    /**
     * @brief Check whether the keys exist in the data system..
     * @param[in] keys keys The keys to be checked..
     * @param[in] exists The existence of the corresponding key.
     * @param[in] queryL2Cache  whether query L2Cache.
     * @param[in] isLocal Is a local query or not.
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    virtual Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache,
                         const bool isLocal) = 0;

    /**
     * @brief Sets expiration time for key list (in seconds)
     * @param[in] key The keys to set expiration for.
     * @param[in] ttlSeconds TTL in seconds.
     * @param[out] failedKeys Returns failed keys if setting failed.
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    virtual Status Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds,
                          std::vector<std::string> &failedKeys) = 0;

    /**
     * @brief Get device meta info of the keys.
     * @param[in] keys The keys to be queried. Constraint: The number of keys cannot exceed 10000.
     * @param[in] isDevKey It indicates whether the keys are used for D2H or D2D transmission, and when it is true, it
     * represents D2D transmission.
     * @param[out] metaInfos device meta info of the keys
     * @param[out] failKeys The failed keys
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    virtual Status GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey,
                               GetMetaInfoRspPb &metaInfos) = 0;
    
    /**
     * @brief Reconnect worker.
     * @param[in] gRefIds Global reference object keys.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    virtual Status ReconnectWorker(const std::vector<std::string> &gRefIds) = 0;

    virtual Status PrepairForDecreaseShmRef(
        std::function<Status(const std::string &, const std::shared_ptr<ShmUnitInfo> &)> mmapFunc) = 0;
    virtual Status CleanUpForDecreaseShmRefAfterWorkerLost() = 0;

    /**
     * @brief Decrease the object worker reference count by one and release the object if no client holds it, and
     * finally notifies the worker by shared memory.
     * @param[in] shmId The ID of the object to decrease ref.
     * @param[in] connectCheck the connect check with local server.
     * @return Status of the call.
     */
    virtual Status DecreaseShmRef(const ShmKey &shmId, const std::function<Status()> &connectCheck,
                                  std::shared_timed_mutex &shutdownMtx) = 0;

    bool EnableDecreaseShmRefByShmQueue()
    {
        return shmEnabled_ && decShmUnit_->fd > 0;
    }
};

class ClientWorkerRemoteApi : public IClientWorkerApi, public client::ClientWorkerRemoteCommonApi {
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
    Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs) override;

    std::shared_ptr<IClientWorkerApi> CloneWith(HostPort hostPort, RpcCredential cred,
                                                HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT,
                                                SensitiveValue token = "", Signature *signature = nullptr,
                                                std::string tenantId = "", bool enableCrossNodeConnection = false,
                                                bool enableExclusiveConnection = false) const override
    {
        return std::make_shared<ClientWorkerRemoteApi>(std::move(hostPort), cred, heartbeatType, std::move(token),
                                                       signature, tenantId, enableCrossNodeConnection,
                                                       enableExclusiveConnection);
    };

    Status Create(const std::string &objectKey, int64_t dataSize, uint32_t &version, uint64_t &metadataSize,
                  std::shared_ptr<ShmUnitInfo> &shmBuf, const CacheType &cacheType = CacheType::MEMORY) override;
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
    void ParseGlbRefPb(QueryGlobalRefNumRspCollectionPb &rsp,
                       std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap);

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
     * @brief Fill device object meta to Pb.
     * @param[in] bufferInfo The info of device buffer.
     * @param[in] blobs The list of blob info.
     * @param[out] metaPb The device object meta pb.
     */
    void FillDevObjMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, const std::vector<Blob> &blobs,
                        DeviceObjectMetaPb *metaPb);

    /**
     * @brief Prepare the put request.
     * @param[in] bufferInfo Buffer information.
     * @param[in] isSeal Is seal or not.
     * @param[in] nestedKeys Nested keys.
     * @param[in] ttlSecond Used by state api, means how many seconds the key will be delete automatically.
     * @param[in] existence Used by state api, to determine whether to set or not set the key if it does already
     * exist.
     * @param[out] req The protobuf req.
     * @return K_OK on success; the error code otherwise.
     */
    Status PreparePublishReq(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isSeal,
                             const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond, int existence,
                             PublishReqPb &req);

    /**
     * @brief Create communication circular queue based on shared memory.
     * @return K_OK on success; the error code otherwise.
     */
    Status InitDecreaseQueue();

    void PostMultiCreate(bool skipCheckExistence, const MultiCreateRspPb &rsp,
                         std::vector<MultiCreateParam> &createParams, bool &useShmTransfer, PerfPoint &point,
                         uint32_t &version, std::vector<bool> &exists);
    Status PreGet(const GetParam &getParam, int64_t subTimeoutMs, GetReqPb &req, int64_t &rpcTimeout);

    // To protect the decreaseRPCQ_ and waitRespMap_ from being manipulated by different threads of the same client.
    mutable std::mutex mtx_;
    std::unordered_map<int, uint8_t *> waitRespMap_;
    std::shared_ptr<ShmCircularQueue> decreaseRPCQ_{ nullptr };
    std::unique_ptr<WorkerOCService::Stub> stub_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H
