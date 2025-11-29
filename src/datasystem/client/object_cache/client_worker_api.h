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
#include "datasystem/client/mmap_table.h"
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
};

class ClientWorkerApi : public client::ClientWorkerCommonApi, public std::enable_shared_from_this<ClientWorkerApi> {
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
    explicit ClientWorkerApi(HostPort hostPort, RpcCredential cred,
                             HeartbeatType heartbeatType = HeartbeatType::RPC_HEARTBEAT, Signature *signature = nullptr,
                             std::string tenantId = "", bool enableCrossNodeConnection = false,
                             bool enableExclusiveConnection = false);

    /**
     * @brief Initialize ClientWorkerApi.
     * @param[in] requestTimeoutMs Request Timeout milliseconds.
     * @param[in] connectTimeoutMs Connect Timeout milliseconds.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs) override;

    /**
     * @brief Reconnect worker.
     * @param[in] gRefIds Global reference object keys.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the input ip or port is invalid.
     */
    Status ReconnectWorker(const std::vector<std::string> &gRefIds);

    /**
     * @brief Create communication circular queue based on shared memory.
     * @return K_OK on success; the error code otherwise.
     */
    Status InitDecreaseQueue();

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
    Status Create(const std::string &objectKey, int64_t dataSize, uint32_t &version, uint64_t &metadataSize,
                  std::shared_ptr<ShmUnitInfo> &shmBuf, const CacheType &cacheType = CacheType::MEMORY);

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
    Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal,
                   const std::unordered_set<std::string> &nestedKeys = {}, uint32_t ttlSecond = 0, int existence = 0);

    /**
     * @brief Publish multiple objects in the store-server.
     * @param[in] bufferInfo Buffers information.
     * @param[in] param The publish param.
     * @param[out] rsp MultiPublishRspPb rsp.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo, const PublishParam &param,
                        MultiPublishRspPb &rsp, const std::vector<std::vector<uint64_t>> &blobSizes = {});

    /**
     * @brief Decrease the object worker reference count by one and release the object if no client holds it, and
     * finally notifies the worker by shared memory.
     * @param[in] shmId The ID of the object to decrease ref.
     * @param[in] connectCheck the connect check with local server.
     * @return Status of the call.
     */
    Status DecreaseWorkerRefByShm(const ShmKey &shmId, const std::function<Status()> &connectCheck);

    /**
     * @brief Wakes up all waiting processes in the shared memory queue and clears the pointers to the queue.
     * @return Status of the call.
     */
    Status DisconnectWithShmQueue();

    /**
     * @brief Decrease the object worker reference count by one and if no client holds the object, release it.
     * @param[in] objectKey The ID of the object to decrease ref.
     * @return Status of the call.
     */
    Status DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys);

    /**
     * @brief Send getting object rpc request to worker.
     * @param[in] getParam The params for get operation.
     * @param[out] version Worker version.
     * @param[out] rsp The response getting from worker.
     * @param[out] payloads The payload getting from worker.
     * @return Status of the call.
     */
    Status Get(const GetParam &getParam, uint32_t &version, GetRspPb &rsp, std::vector<RpcMessage> &payloads);

    /**
     * @brief Send invalidate buffer rpc request to worker.
     * @param[in] objectKey The object key which binds to the buffer.
     * @return Status of the call.
     */
    Status InvalidateBuffer(const std::string &objectKey);

    /**
     * @brief Increase the global reference count to objects in worker.
     * @param[in] firstIncIds The object keys to increase in worker, it cannot be empty.
     * @param[out] failedObjectKeys Increase failed object keys.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseWorkerRef(const std::vector<std::string> &firstIncIds, std::vector<std::string> &failedObjectKeys,
                              const std::string &remoteClientId = "");

    /**
     * @brief Release obj Ref of remote client id.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReleaseGRefs(const std::string &remoteClientId);

    /**
     * @brief Decrease the global reference count to objects in worker.
     * @param[in] finishDecIds The object keys to decrease  in worker, it cannot be empty.
     * @param[out] failedObjectKeys Decrease failed object keys.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status GDecreaseWorkerRef(const std::vector<std::string> &finishDecIds, std::vector<std::string> &failedObjectKeys,
                              const std::string &remoteClientId = "");
    /**
     * @brief Delete the given objects.
     * @param[in] objectKeys The vector of the object key.
     * @param[out] failedObjectKeys The vector of the failed delete object key list.
     * @param[in] areDeviceObjects The objectKeys are device objects.
     * @return Status of the call.
     */
    Status Delete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys,
                  bool areDeviceObjects = false);

    /**
     * @brief Query all objects global references in the cluster.
     * @param[in] objectKeys The specific objects id that to query.
     * @param[out] gRefMap The global references distribution.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status QueryGlobalRefNum(const std::vector<std::string> &objectKeys,
                             std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap);
    /**
     * @brief Publish an device object in the worker.
     * @param[in] bufferInfo Device Buffer information.
     * @param[in] dataSize The size of data.
     * @param[in] isShm Is shared memory or not.
     * @param[in] nonShmPointer The data pointer if is not shared memory.
     * @return K_OK on success; the error code otherwise.
     */
    Status PublishDeviceObject(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, size_t dataSize, bool isShm,
                               void *nonShmPointer);

    /**
     * @brief Send device getting object rpc request to worker.
     * @param[in] devObjKeys The vector of the device object key.
     * @param[in] dataSize The size of the output data.
     * @param[in] timeoutMs The timeout of rpc.
     * @param[out] rsp The response getting from worker.
     * @param[out] payloads The payload getting from worker.
     * @return Status of the call.
     */
    Status GetDeviceObject(const std::vector<std::string> &devObjKeys, uint64_t dataSize, int32_t timeoutMs,
                           GetDeviceObjectRspPb &rsp, std::vector<RpcMessage> &payloads);

    /**
     * @brief Subscribe receive event from worker.
     * @param[in] deviceId The deviceid to which data belongs.
     * @param[out] resp The response getting from worker.
     * @return Status of the call
     */
    Status SubscribeReceiveEvent(int32_t deviceId, SubscribeReceiveEventRspPb &resp);

    /**
     * @brief Put the p2p metadata to worker.
     * @param[in] bufferInfo The info of device buffer.
     * @param[in] blobs The list of device blob.
     * @return Status of the call
     */
    Status PutP2PMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, const std::vector<Blob> &blobs);

    /**
     * @brief Get the p2p metadata from worker.
     * @param[in] bufferInfoList The info of device buffer.
     * @param[in] devBlobList The list of device blob.
     * @param[out] resp The response of the rpc call.
     * @param[in] subTimeoutMs The maximum time elapse of subscriptions.
     * @return Status of the call
     */
    Status GetP2PMeta(std::vector<std::shared_ptr<DeviceBufferInfo>> &bufferInfoList,
                      std::vector<DeviceBlobList> &devBlobList, GetP2PMetaRspPb &resp, int64_t subTimeoutMs = 500);

    /**
     * @brief Send the root info to worker.
     * @param[in] req The request of the call.
     * @param[out] resp The response of the call.
     * @return Status of the call.
     */
    Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp);

    /**
     * @brief Receive the root info from worker.
     * @param[in] req The request of the call.
     * @param[out] resp The response of the call.
     * @return Status of the call.
     */
    Status RecvRootInfo(RecvRootInfoReqPb &req, RecvRootInfoRspPb &resp);

    /**
     * @brief Obtains the BlobInfos, including the number of DataInfo, and the count and DataType of each DataInfo.
     * @param[in] devObjKey The object key.
     * @param[in] timeoutMs Waiting for the result return if object not ready. A positive integer number required.
     * 0 means no waiting time allowed. And the range is [0, INT32_MAX].
     * @param[out] blobs The list of blob info. (Include pointer、count and data type)
     * @return K_OK on any object success; the error code otherwise.
     */
    Status GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs);

    /**
     * @brief Acknowledge the get operation is ok, and indicate whether worker as data providers
     * @param[in] req The AckRecvFinishReqPb request protobuf message.
     * @return Status of the call.
     */
    Status AckRecvFinish(AckRecvFinishReqPb &req);

    /**
     * @brief Remove the location of device object
     * @param[in] objectKey The object key.
     * @param[in] deviceId The device id of this location.
     * @return K_OK on any object success; the error code otherwise.
     */
    Status RemoveP2PLocation(const std::string &objectKey, int32_t deviceId);

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
    Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                          std::vector<ObjMetaInfo> &objMetas);

    /**
     * @brief Create multiple objects in the store-server.
     * @param[in]  skipCheckExistence Whether skip existence check.
     * @param[out] createParams The params for create operation.
     * @param[out] version Object version.
     * @param[out] exists The Key exist list.
     * @param[out] useShmTransfer Transfer by shm.
     * @return K_OK on success; the error code otherwise.
     */
    Status MultiCreate(bool skipCheckExistence, std::vector<MultiCreateParam> &createParams, uint32_t &version,
                       std::vector<bool> &exists, bool &useShmTransfer);

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
    Status QuerySize(const std::vector<std::string> &objectKeys, QuerySizeRspPb &rsp);

    /**
     * @brief Worker health check.
     * @param[out] state The state of ds service.
     * @return K_OK on any object success; the error code otherwise.
     */
    Status HealthCheck(ServerState &state);

    /**
     * @brief Check whether the keys exist in the data system..
     * @param[in] keys keys The keys to be checked..
     * @param[in] exists The existence of the corresponding key.
     * @param[in] queryL2Cache  whether query L2Cache.
     * @param[in] isLocal Is a local query or not.
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache,
                 const bool isLocal);

    /**
     * @brief Sets expiration time for key list (in seconds)
     * @param[in] key The keys to set expiration for.
     * @param[in] ttlSeconds TTL in seconds.
     * @param[out] failedKeys Returns failed keys if setting failed.
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    Status Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys);

    /**
     * @brief Get device meta info of the keys.
     * @param[in] keys The keys to be queried. Constraint: The number of keys cannot exceed 10000.
     * @param[in] isDevKey It indicates whether the keys are used for D2H or D2D transmission, and when it is true, it
     * represents D2D transmission.
     * @param[out] metaInfos device meta info of the keys
     * @param[out] failKeys The failed keys
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    Status GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey, GetMetaInfoRspPb &metaInfos);

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

    // To protect the decreaseRPCQ_ and waitRespMap_ from being manipulated by different threads of the same client.
    mutable std::mutex mtx_;
    std::unordered_map<int, uint8_t *> waitRespMap_;
    std::shared_ptr<ShmCircularQueue> decreaseRPCQ_{ nullptr };
    std::unique_ptr<WorkerOCService::Stub> stub_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H
