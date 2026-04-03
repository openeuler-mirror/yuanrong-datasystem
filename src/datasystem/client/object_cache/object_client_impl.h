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
 * Description: Data system Object Client implementation.
 */

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CLIENT_IMPL_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_OBJECT_CLIENT_IMPL_H

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "re2/re2.h"

#include "datasystem/client/client_state_manager.h"
#include "datasystem/client/embedded_client_worker_api.h"
#include "datasystem/client/listen_worker.h"
#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/object_cache/client_memory_ref_table.h"
#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/client_worker_api/client_worker_local_api.h"
#include "datasystem/client/object_cache/client_worker_api/client_worker_remote_api.h"
#include "datasystem/client/object_cache/device/client_device_object_manager.h"
#include "datasystem/client/object_cache/device/p2p_subscribe.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/object_cache/object_base.h"
#ifdef BUILD_HETERO
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_helper.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/object_client.h"
#include "datasystem/hetero_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/kv_client.h"
#include "datasystem/common/rpc/rpc_auth_keys.h"
#include "datasystem/utils/embedded_config.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/utils/service_discovery.h"

namespace datasystem {
namespace object_cache {
using TbbGlobalRefTable = tbb::concurrent_hash_map<std::string, int>;
using GlobalRefInfo = std::pair<int, std::shared_ptr<TbbGlobalRefTable::accessor>>;

struct P2PPeer {
    void *devPointer;
    uint32_t *srcRank;
    uint32_t *destRank;
    DataType dataType;
    uint64_t count;
};

struct FullParam : public CreateParam {
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;
    uint32_t ttlSecond = 0;
};

using P2PPeerTable = tbb::concurrent_hash_map<std::string, P2PPeer>;

class __attribute((visibility("default"))) ObjectClientImpl : public std::enable_shared_from_this<ObjectClientImpl> {
public:
    explicit ObjectClientImpl(const ConnectOptions &connectOptions);

    ~ObjectClientImpl();

    /**
     * @brief Shutdown a object client instance.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     * @param[in] isDestruct Since shutdown will also be called during client's destruction,
     * this parameter is used to avoid redundant log printing in the destruction scenario.
     * @return K_OK on success; the error code otherwise.
     */
    Status ShutDown(bool &needRollbackState, bool isDestruct = false);

    /**
     * @brief Init embedded client.
     * @param[in] config Config for embedded client.
     * @param[in] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     * @return K_OK on success; the error code otherwise.
     */
    Status InitEmbedded(const EmbeddedConfig &config, bool &needRollbackState);

    /**
     * @brief Init a object client instance.
     * @param[in] enableHeartbeat If resources do not need to be released, set this parameter to false.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     * @return K_OK on success; the error code otherwise.
     */
    Status Init(bool &needRollbackState, bool enableHeartbeat = true);

    /**
     * @brief  Invoke worker client to seal the object.
     * @param[in] bufferInfo The info (data to be published) contained in the buffer.
     * @param[in] nestedObjectKeys Objects that depend on objectKey.
     * @param[in] isShm A flag indicating how the object will be published (shm or non-shm).
     * @return K_OK on success; the error code otherwise.
     */
    Status Seal(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                const std::unordered_set<std::string> &nestedObjectKeys, bool isShm);

    /**
     * @brief  Publish the object.
     * @param[in] bufferInfo The info (data to be published) contained in the buffer.
     * @param[in] nestedObjectKeys Objects that depend on objectKey.
     * @param[in] isShm A flag indicating how the object will be published (shm or non-shm).
     * @return K_OK on success; the error code otherwise.
     */
    Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                   const std::unordered_set<std::string> &nestedObjectKeys, bool isShm);

    /**
     * @brief Send buffer data via UB after MemoryCopy (Create+MemoryCopy+Publish path). No-op if UB disabled.
     * @param[in] bufferInfo The buffer info.
     * @param[in] data The data to be sent.
     * @param[in] length The length of the data to be sent.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data, uint64_t length);

    /**
     * @brief Invoke worker client to publish all buffers of all the given object keys.
     * @param[in] buffers The vector of the all the buffers.
     * @param[out]  failList The objects that are failed to be published
     * @return K_OK on any object success; the error code otherwise.
     */
    Status Publish(const std::vector<std::shared_ptr<DeviceBuffer>> &buffers, std::vector<std::string> &failList);

    /**
     * @brief Create the shared memory buffer of the data system.
     * @param[in] objectKey The ID of the object to create.
     * @param[in] dataSize The size in bytes of the space to be allocated for this object.
     * @param[in] param The param for create operation.
     * @param[out] buffer The address of the newly created object will be written here.
     * @return K_OK on success; the error code otherwise.
     *         K_RUNTIME_ERROR: client fd mmap failed.
     *         K_DUPLICATED: the object already exists, no need to create.
     */
    Status Create(const std::string &objectKey, uint64_t dataSize, const FullParam &param,
                  std::shared_ptr<Buffer> &buffer);

    /**
     * @brief Store the shared memory buffer created by the Create interface to the data system.
     *
     * @param[in] buffer The buffer to set.
     * @return K_OK on success; the error code otherwise.
     */
    Status Set(const std::shared_ptr<Buffer> &buffer);

    /**
     * @brief Batch create shared-memory Buffers in datasystem.
     *
     * The returned Buffers can be filled directly with data; subsequently call MSet()
     * to cache it. This interface avoids the need for temporary memory and reduces
     * one extra memory copy.
     *
     * @param[in] keys The ID of the object to create. ID should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
     * @param[in] size The size in bytes of object.
     * @param[in] param The create parameters.
     * @param[out] buffer The buffer for the object.
     * @return K_OK on success; the error code otherwise.
     *         K_RUNTIME_ERROR: client fd mmap failed.
     *         K_DUPLICATED: the object already exists, no need to create.
     */
    Status MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes, const FullParam &param,
                   std::vector<std::shared_ptr<Buffer>> &buffers);

    /**
     * @brief Batch setter for multiple buffers.
     *
     * This interface is used together with MCreate to cache a batch of
     * shared-memory Buffers into the data system.
     * @param[in] buffers The buffers to set.
     * @return K_OK on success; the error code otherwise.
     */
    Status MSet(const std::vector<std::shared_ptr<Buffer>> &buffers);

    /**
     * @brief Decrease the object reference count by one and if no one holds its ref, release it.
     * @param[in] shmId The ID of the object to decrease ref
     * @param[in] isShm A flag indicating how the object will be published (shm or non-shm).
     * @param[in] version Worker version.
     */
    void DecreaseReferenceCnt(const ShmKey &shmId, bool isShm, uint32_t version = 0);

    /**
     * @brief Increase the global reference count to objects in worker.
     * @param[in] firstIncIds The object keys to increase in worker, it cannot be empty.
     * @param[out] failedObjectKeys Increase failed object keys.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status GIncreaseRef(const std::vector<std::string> &firstIncIds, std::vector<std::string> &failedObjectKeys,
                        const std::string &remoteClientId = "");

    /**
     * @brief Release obj Ref of remote client id.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReleaseGRefs(const std::string &remoteClientId);

    /**
     * @brief Decrease the global reference count to objects in worker.
     * @param[in] finishDecIds The object keys to decrease in worker, it cannot be empty.
     * @param[out] failedObjectKeys Decrease failed object keys.
     * @param[in] remoteClientId The remote client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status GDecreaseRef(const std::vector<std::string> &finishDecIds, std::vector<std::string> &failedObjectKeys,
                        const std::string &remoteClientId = "");

    /**
     * @brief Query all objs global references.
     * @param[in] objectKey The selected object to be query.
     * @return The objects' global reference num; -1 in case of failure.
     */
    int QueryGlobalRefNum(const std::string &objectKey);

    /**
     * @brief Invoke worker client to put (publish) an object.
     * @param[in] objectKey The ID of the object to create.
     * @param[in] data The data pointer of the user.
     * @param[in] size The size in bytes of object.
     * @param[in] param The create parameters.
     * @param[in] nestedObjectKeys Objects that depend on objectKey.
     * @param[in] ttlSecond Used by state api, means how many seconds the key will be delete automatically.
     * @param[in] existence Used by state api, to determine whether to set or not set the key if it does already exist.
     * @return K_OK on success; the error code otherwise.
     */
    Status Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
               const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond = 0, int existence = 0);

    /**
     * @brief Invoke worker client to get an object.
     * @param[in] objectKeys The vector of the object key.
     * @param[in] vals The vector of the object values.
     * @param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer number
     * required. 0 means no waiting time allowed.
     * @param[out] buffers The return vector of the objects.
     * @param[out] dataSize The size of the objects.
     * @return Status of the result.
     */
    Status GetWithLatch(const std::vector<std::string> &objectKeys, std::vector<std::string> &vals,
                        int64_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers, size_t &dataSize);

    /**
     * @brief asyn initiate p2p data transfer process
     * @param[in] objKeys multiple keys support
     * @param[in] subTimeoutMs max waiting time of getting data
     * @param[out] buffers vector of device buffer
     * @param[out] failList object keys that failed for p2p transfer
     * @return status code
     */

    Status Get(const std::vector<std::string> &objKeys, int32_t subTimeoutMs,
               std::vector<std::shared_ptr<DeviceBuffer>> &buffers, std::vector<std::string> &failList);

    /**
     * @brief Invoke worker client to get an object.
     * @param[in] objectKeys The vector of the object key.
     * @param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer number
     * required. 0 means no waiting time allowed.
     * @param[out] buffers The return vector of the objects.
     * @param[in] queryL2Cache whether query l2cache.
     * @param[in] isRH2DSupported whether the get request supports RH2D.
     * @return Status of the result.
     */
    Status Get(const std::vector<std::string> &objectKeys, int64_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers,
               bool queryL2Cache = true, bool isRH2DSupported = false);

    /**
     * @brief Some data in an object can be read based on the specified key and parameters.
     *        In some scenarios, read amplification can be avoided.
     * @param[in] readParams The vector of the keys and offset.
     * @param[out] failedKeys The failed delete keys.
     * @return K_OK on any key success; the error code otherwise.
     *         K_INVALID: the vector of keys is empty or include empty key.
     *         K_NOT_FOUND: the key not found.
     *         K_RUNTIME_ERROR: Cannot get values from worker.
     * @verbatim
     * If some keys are not found, The Status OK will return,
     * and the existing keys will set the vals with the same index of keys.
     * @endverbatim
     */
    Status Read(const std::vector<ReadParam> &readParams, std::vector<Optional<Buffer>> &buffers);

    /**
     * @brief Delete the given objects.
     * @param[in] objectKeys The vector of the object key.
     * @param[out] failedObjectKeys The vector of the failed delete object key list.
     * @return Status of the call.
     */
    Status Delete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys);

    /**
     * @brief Invoke worker client to set the value of a key.
     * @param[in] key The key.
     * @param[in] val The value for the key.
     * @param[in] setParam The param for set operation.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the key or val is empty.
     */
    Status Set(const std::string &key, const StringView &val, const SetParam &setParam);

    /**
     * @brief Update token for yr iam
     * @param[in] Token message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateToken(SensitiveValue &token);

    /**
     * @brief Update aksk for yr iam
     * @param[in] acessKey message for auth certification
     * @param[in] secretKey message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey);

    /**
     * @brief Invoke worker client to set the value of a key.
     * @param[in] val The value for the key.
     * @param[in] setParam The param for set operation.
     * @param[out] val The generated key.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: the key or val is empty.
     */
    Status Set(const StringView &val, const SetParam &setParam, std::string &key);

    /**
     * @brief Invoke worker client to set the keys to their respective values.
     * @param[in] keys The keys.
     * @param[in] vals The values for the keys.
     * @param[in] param The set parameters.
     * @return K_OK on success; the error code otherwise.
     */
    Status MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals, const MSetParam &param);

    /**
     * @brief Invoke worker client to set the keys to their respective values.
     * @param[in] keys The keys.
     * @param[in] vals The values for the keys.
     * @param[in] param The set parameters.
     * @param[out] outFailedKeys
     * @return K_OK on success; the error code otherwise.
     */
    Status MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals, const MSetParam &param,
                std::vector<std::string> &outFailedKeys);

    /**
     * @brief Check if can create shared memory buffer.
     * @param[in] size Create size.
     * @return True if can create shared memory buffer.
     */
    bool ShmCreateable(uint64_t size) const;

    /**
     * @brief Check if can shared memory transfer.
     * @return True if can shared memory transfer.
     */
    bool ShmEnable() const;

    /**
     * @brief Get the memoryCopyThreadPool.
     * @return return memoryCopyThreadPool shared_ptr.
     */
    std::shared_ptr<ThreadPool> GetMemoryCopyThreadPool();

    /**
     * @brief Generate a key with workeId.
     * @param[in] prefixKey The user specified key prefix.
     * If the prefix is empty, return uuid;workerId
     * @return The key with workeId, if the key fails to be generated, an empty string is returned.
     */
    Status GenerateKey(std::string &key, const std::string &prefixKey = "");

    /**
     * @brief Get objectKey from a key with workerUuid.
     * @param[in] key The key with workerUuid.
     * @param[out] prefix The objectKey.
     * @return K_OK on any object success; the error code otherwise.
     */
    Status GetPrefix(const std::string &key, std::string &prefix);

    /**
     * @brief Publish device object to datasystem.
     * @param[in] buffer The device buffer ready to publish.
     * @return Status of the result.
     */
    Status PublishDeviceObject(std::shared_ptr<DeviceBuffer> buffer);

    /**
     * @brief Init/Shutdown complete handler.
     * @param[in] failed Init/Shutdown success or not.
     * @param[out] needRollbackState If the client status is successfully changed to INTERMEDIATE,
     * the status needs to be rolled back based on the completion status when the request is completed.
     */
    void CompleteHandler(bool failed, bool needRollbackState)
    {
        clientStateManager_->CompleteHandler(failed, needRollbackState);
    }

    /**
     * @brief Invoke worker client to create a device object with p2p.
     * @param[in] objectKey The ID of the device object to create. ID should not be empty and should only contains
     * english alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
     * @param[in] devBlobList The list of blob info.
     * @param[in] param The create param of device object.
     * @param[out] deviceBuffer The device buffer for the object.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status CreateDevBuffer(const std::string &devObjKey, const DeviceBlobList &devBlobList,
                           const CreateDeviceParam &param, std::shared_ptr<DeviceBuffer> &deviceBuffer);

    /**
     * @brief Invoke worker client to get the given device object keys and copy to the destinationdevice buffer.
     * @param[in] devObjKeys The vector of the object key. ID should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
     * Pass one object key to vector if you just want to get an device object. Don't support to pass multi object keys
     * now.
     * @param[out] dstDevBuffer The destination of device buffer.
     * @param[out] futureVec The deviceid to which data belongs.
     * @param[in] prefetchTimeoutMs Timeout(ms) of waiting for the result return if object not ready. A positive integer
     * number required. 0 means no waiting time allowed. And the range is [0, INT32_MAX].
     * @param[in] subTimeoutMs The maximum time elapse of subscriptions.
     * @return K_OK on any object success; the error code otherwise.
     *         K_INVALID: the vector of keys is empty or include empty key.
     *         K_NOT_FOUND: The objects not exists.
     *         K_RUNTIME_ERROR: Cannot get objects from worker.
     */
    Status AsyncGetDevBuffer(const std::vector<std::string> &devObjKey,
                             std::vector<std::shared_ptr<DeviceBuffer>> &dstDevBuffers, std::vector<Future> &futureVec,
                             int64_t prefetchTimeoutMs, int64_t subTimeoutMs = 20000);

    /**
     * @brief Gets the list of future in device memory sending, it only work in MOVE lifetime.
     * @param[out] futureVec The deviceid to which data belongs.
     * @return Status of the result.
     */
    Status GetSendStatus(const std::shared_ptr<DeviceBuffer> &buffer, std::vector<Future> &futureVec);

    /**
     * @brief Obtains the DBlobInfos, including the number of blobs, and the count
     * @param[in] devObjKey The object key. ID should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
     * @param[in] timeoutMs Waiting for the result return if object not ready. A positive integer number required.
     * 0 means no waiting time allowed. And the range is [0, INT32_MAX].
     * @param[out] blobs The list of data info. (Include pointer、count and data type)
     * @return K_OK on any object success; the error code otherwise.
     */
    Status GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs);

    /**
     * @brief Remove the location of device object
     * @param[in] devObjKey The object key.
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
     * @brief object detete, remove directory location, send notificatios
     * to associated clients and remove relevant tables in p2p subscribe.
     * @param[in] objKeys objects needed to be deleted
     * @return future of AsyncResult, describe delete status and failed list.
     */
    std::shared_future<AsyncResult> AsyncDeleteDevObjects(const std::vector<std::string> &objKeys);

    /**
     * @brief object detete, remove directory location, send notificatios
     * to associated clients and remove relevant tables in p2p subscribe.
     * @param[in] objKeys objects needed to be deleted
     * @param[out] failList failed ojbejcts to be deleted
     */
    Status DeleteDevObjects(const std::vector<std::string> &objKeys, std::vector<std::string> &failList);

    /**
     * @brief For device object, to sync get multiple objects
     * @param[in] objectKeys multiple keys support
     * @param[in] devBlobList vector of compose blobInfo
     * @param[out] failedKeys failed keys if retrieval fails
     * @param[in] timeoutMs max waiting time of getting data
     * @return Status of the result.
     */
    Status MGetH2D(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                   std::vector<std::string> &failedKeys, uint64_t timeoutMs);

    /**
     * @brief For device object, to async get multiple objects
     * @param[in] objectKeys multiple keys support
     * @param[in] devBlobList vector of compose blobInfo
     * @param[in] timeoutMs max waiting time of getting data
     * @return future of AsyncResult, describe get status and failed list.
     */
    std::shared_future<AsyncResult> AsyncMGetH2D(const std::vector<std::string> &objectKeys,
                                                 const std::vector<DeviceBlobList> &devBlobList, uint64_t timeout);

    /**
     * @brief For device object, to sync set multiple objects
     * @param[in] objectKeys multiple keys support
     * @param[in] devBlobList vector of compose data
     * @param[in] param set parameter
     * @return Status of the result.
     */
    Status MSetD2H(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                   const SetParam &param);

    /**
     * @brief For device object, to invoke worker client to create and async publish multiple objects
     * @param[in] objKeys multiple keys support
     * @param[in] devBlobList vector of compose data
     * @return future of AsyncResult, describe set status and failed list.
     */
    std::shared_future<AsyncResult> AsyncMSetD2H(const std::vector<std::string> &objectKeys,
                                                 const std::vector<DeviceBlobList> &devBlobList, const SetParam &param);

    /**
    @brief Publish device data to device.
    @param[in] keys A list of keys corresponding to the blob2dList.
    @param[in] blob2dList A list of structures describing the Device memory.
    @param[out] futureVec A list of futures to track the  operation.
    @return K_OK on when return all futures sucesss; the error code otherwise.
    */
    Status DevPublish(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blob2dList,
                      std::vector<Future> &futureVec);

    /**
    @brief Subscribe device data from device.
    @param[in] keys A list of keys corresponding to the blob2dList.
    @param[in] blob2dList A list of structures describing the Device memory.
    @param[out] futureVec A list of futures to track the  operation.
    @return K_OK on when return all futures sucesss; the error code otherwise.
    */
    Status DevSubscribe(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blob2dList,
                        std::vector<Future> &futureVec);

    /**
    @brief DevLocalDelete interface. After calling this interface, the data replica stored in the data system by the
    current client connection will be deleted.
    @param[in] objectKeys The objectKeys of the data expected to be deleted.
    @param[out] failedObjectKeys Partial failures will be returned through this parameter.
    @return K_OK on when return sucesss; the error code otherwise.
    */
    Status DevLocalDelete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys);

    /**
    @brief Retrieves data from the Device through the data system, storing it in the corresponding DeviceBlobList
    @param[in] keys Keys corresponding to blob2dList
    @param[in] blob2dList List describing the structure of Device memory
    @param[out] failedKeys Returns failed keys if retrieval fails
    @param[in] timeoutMs Provides a timeout time, defaulting to 0
    @return K_OK on when return sucesssfully; the error code otherwise.
     */
    Status DevMGet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blob2dList,
                   std::vector<std::string> &failedKeys, int32_t timeoutMs = 0);

    /**
    @brief Store Device cache through the data system, caching corresponding keys-blob2dList metadata to the data
    system
    @param[in] keys Keys corresponding to blob2dList
    @param[in] blob2dList List describing the structure of Device memory
    @param[out] failedKeys Returns failed keys if caching fails
    @return K_OK on when return sucesssfully; the error code otherwise.
     */
    Status DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &blob2dList,
                   std::vector<std::string> &failedKeys);

    /**
     * @brief Invoke worker client to query the size of objectKeys (include the objectKeys of other AZ).
     * @param[in] objectKeys The objectKeys need to query size.
     * @param[out] outSizes The size for the objectKeys in bytes.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: The objectKeys are empty or invalid.
     *         K_NOT_FOUND: All objectKeys not found.
     *         K_RPC_UNAVAILABLE: Network error.
     *         K_NOT_READY: Worker not ready.
     *         K_RUNTIME_ERROR: Can not get objectKey size from worker.
     */
    Status QuerySize(const std::vector<std::string> &objectKeys, std::vector<uint64_t> &outSizes);

    /**
     * @brief Check whether the keys exist in the data system.
     * @param[in] keys The keys to be checked. Constraint: The number of keys cannot exceed 10000.
     * @param[in] exists The existence of the corresponding key.
     * @param[in] queryL2Cache whether query L2Cache.
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
     * @param[in] isDevKeyThe query keys are prepared for p2p transfer
     * @param[out] metaInfos device meta info of the keys
     * @param[out] failKeys The failed keys
     * @return K_OK if at least one key is successfully processed; the error code otherwise.
     */
    Status GetMetaInfo(const std::vector<std::string> &keys, bool isDevKey, std::vector<MetaInfo> &metaInfos,
                       std::vector<std::string> &failKeys);

    /**
     * @brief Worker health check.
     * @param[out] state The state of ds service.
     * @return K_OK on any object success; the error code otherwise.
     */
    Status HealthCheck(ServerState &state);

private:
    friend Buffer;
    friend DeviceBuffer;
    friend ClientDeviceObjectManager;
    enum WorkerNode : uint32_t { LOCAL_WORKER = 0, STANDBY1_WORKER, STANDBY2_WORKER };

    /**
     * @brief Init ParallelFor thread pool.
     */
    void InitParallelFor();

    /**
     * @brief The core synchronous implementation shared by MGetH2D and AsyncMGetH2D.
     */
    Status MGetH2DImpl(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                       uint64_t timeoutMs, std::vector<std::string> &failedKeys);

    /**
     * @brief Validate MGetH2D input arguments.
     */
    Status CheckMGetH2DInput(const std::vector<std::string> &objectKeys,
                             const std::vector<DeviceBlobList> &devBlobList);

    /**
     * @brief The core synchronous implementation shared by MSetD2H and AsyncMSetD2H.
     */
    Status MSetD2HImpl(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                       const SetParam &setParam);

    /**
     * @brief Validate MSetD2H input arguments.
     */
    Status CheckMSetD2HInput(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                             const SetParam &setParam);

    /**
     * @brief Check and construct the multi createParam.
     * @param[in] objectKeyList The vector of the object key that needs to create.
     * @param[in] dataSizeList The object sizes.
     * @param[out] bufferList The buffer list needs to store data information.
     * @param[out] multiCreateParamList The list of objects create param.
     * @return Status of the result.
     */
    Status ConstructMultiCreateParam(const std::vector<std::string> &objectKeyList,
                                     const std::vector<uint64_t> &dataSizeList,
                                     std::vector<std::shared_ptr<Buffer>> &bufferList,
                                     std::vector<MultiCreateParam> &multiCreateParamList, uint64_t &dataSizeSum);

    /**
     * @brief For device object, to async get multiple objects
     * @param[in] devBlobList The user blobInfo list of device data.
     * @param[in] existBufferList The tmp buffer list which will be decrease after memory copy
     * @return Status of the result.
     */
    Status HostDataCopy2Device(std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &existBufferList);

    /**
     * @brief Multiple shared memory and copy data from device.
     * @param[in] objectKeys multiple keys support.
     * @param[in] devBlobList vector of compose data.
     * @param[out] bufferList The buffer list after create.
     * @param[out] destroyBufferList The tmp buffer list which will be decrease after memory copy.
     * @return Status of the result.
     */
    Status DeviceDataCreate(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                            const SetParam &setParam, std::vector<std::shared_ptr<Buffer>> &bufferList,
                            std::vector<bool> &exists);

    /**
     * @brief Create multiple objects at a time to the worker.
     * @param[in] objectKeyList The vector of the object key that needs to create.
     * @param[in] dataSizeList The object sizes.
     * @param[in] param The create param of device object.
     * @param[in] skipCheckExistence Whether skip check existence of key.
     * @param[out] bufferList The buffer list needs to store data information.
     * @param[out] exists The exist list of key.
     * @return Status of the result.
     */
    Status MultiCreate(const std::vector<std::string> &objectKeyList, const std::vector<uint64_t> &dataSizeList,
                       const FullParam &param, const bool skipCheckExistence,
                       std::vector<std::shared_ptr<Buffer>> &bufferList, std::vector<bool> &exists);

    /**
     * @brief Publish multiple objects at a time to the worker.
     * @param[in] bufferList The buffer list needs to store data information.
     * @param[in] setParam the set param of keys
     * @param[in] blobSizes the blob size list of keys
     * @return Status of the result.
     */
    Status MultiPublish(const std::vector<std::shared_ptr<Buffer>> &bufferList, const SetParam &setParam,
                        const std::vector<std::vector<uint64_t>> &blobSizes);

    /**
     * @brief Check the client is ready to execute any api
     * @return Status
     */
    inline Status IsClientReady()
    {
        uint16_t clientState = clientStateManager_->GetState();
        CHECK_FAIL_RETURN_STATUS(clientState == (uint16_t)ClientState::INITIALIZED, StatusCode::K_NOT_READY,
                                 clientStateManager_->ToStringForUser(clientState));
        return Status::OK();
    }

    /**
     * @brief Check if shm buffer still active.
     * @param[in] version Worker version.
     * @return True if alive.
     */
    bool IsBufferAlive(uint32_t version);

    /**
     * @brief Check socket network status between client and specify worker.
     * @param[in] id The node id of worker, default is LOCAL_WORKER.
     * @return Status of network.
     */
    Status CheckConnection(WorkerNode id = LOCAL_WORKER);

    /**
     * @brief Check if worker is scale down or not.
     * @return True if worker is scale down.
     */
    bool IsScaleDown(WorkerNode id = LOCAL_WORKER);

    /**
     * @brief Check if worker is healthy or not.
     * @return True if worker is healthy.
     */
    bool IsHealthy(WorkerNode id = LOCAL_WORKER);

    /**
     * @brief Check socket network status between client and LOCAL_WORKER.
     * @return Status of network.
     */
    Status CheckConnectionWhileShmModify();

    /**
     * @brief Get shared memory data buffers from worker.
     * @param[in] workerApi The worker that handles get request.
     * @param[in] getParam The parameters of get request.
     * @param[out] buffers The address of the objects.
     * @return Status of the result.
     */
    Status GetBuffersFromWorker(std::shared_ptr<IClientWorkerApi> workerApi, GetParam &getParam,
                                std::vector<std::shared_ptr<Buffer>> &buffers);

#ifdef USE_URMA
    /**
     * @brief Get buffers from worker in sub-batches when total UB data exceeds the max buffer size.
     * @param[in] workerApi The worker that handles get request.
     * @param[in] getParam The parameters of get request.
     * @param[out] buffers The address of the objects.
     * @param[in] objMetas Pre-fetched object metadata (sizes).
     * @param[in] ubMaxGetSize Maximum UB buffer size per Get RPC.
     * @return Status of the result.
     */
    Status GetBuffersFromWorkerBatched(std::shared_ptr<IClientWorkerApi> workerApi, const GetParam &getParam,
                                       std::vector<std::shared_ptr<Buffer>> &buffers,
                                       const std::vector<ObjMetaInfo> &objMetas, uint64_t ubMaxGetSize);
#endif

    /**
     * @brief Validate Get response counts and extract object buffers.
     * @param[in] objectKeys The object keys that were requested.
     * @param[in] readParams The read parameters.
     * @param[in,out] rsp The Get response.
     * @param[in] version The Worker version.
     * @param[in,out] payloads The payloads from worker.
     * @param[out] buffers The buffer of the objects.
     * @param[out] failedObjectKey The vector of failed object keys.
     * @return Status of the result.
     */
    Status ProcessGetResponse(const std::vector<std::string> &objectKeys, const std::vector<ReadParam> &readParams,
                              GetRspPb &rsp, uint32_t version, std::vector<RpcMessage> &payloads,
                              std::vector<std::shared_ptr<Buffer>> &buffers, std::vector<std::string> &failedObjectKey);

    /**
     * @brief Get shared memory data buffers from worker.
     * @param[in] objectsNeedToGet The vector of the object key that needs to get from worker.
     * @param[in] rsp The Get response.
     * @param[in] version The Worker version.
     * @param[out] payloads The payload getting from worker.
     * @param[out] buffers The buffer of the objects.
     * @param[out] failedObjectKey The vector of the failed to get object key list.
     * @return Status of the result.
     */
    Status GetObjectBuffers(const std::vector<std::string> &objectsNeedToGet, const GetRspPb &rsp, uint32_t version,
                            const std::vector<ReadParam> &readParams, std::vector<RpcMessage> &payloads,
                            std::vector<std::shared_ptr<Buffer>> &buffers, std::vector<std::string> &failedObjectKey);
    /**
     * @brief Fill in buffer for non-shm cases.
     * @param[in] objectKey The object key of the cache object.
     * @param[in] payloadInfo The protobuf object info.
     * @param[in] version Object version.
     * @param[in] payload The read buffers.
     * @param[out] bufferPtr The object buffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status SetNonShmObjectBuffer(const std::string &objectKey, const GetRspPb::PayloadInfoPb &payloadInfo, int version,
                                 std::vector<RpcMessage> &payloads, std::shared_ptr<Buffer> &bufferPtr);

    /**
     * @brief Set shm object buffer.
     * @param[in] objectKey The object key of the cache object.
     * @param[in] info The protobuf object info.
     * @param[in] version Object version.
     * @param[out] buffer The object buffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status SetShmObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info, uint32_t version,
                              std::shared_ptr<Buffer> &buffer);

    /**
     * @brief Set Remote H2D remote host object buffer.
     * @param[in] objectKey The object key of the cache object.
     * @param[in] info The protobuf object info.
     * @param[in] version Object version.
     * @param[out] buffer The object buffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status SetRemoteHostObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info, uint32_t version,
                                     std::shared_ptr<Buffer> &buffer);

    /**
     * @brief Batch release buffers.
     * @param[in] buffers The object buffers.
     */
    void BatchReleaseBufferPtr(const std::vector<Buffer *> &buffers);

    /**
     * @brief Batch release local memory ref by zmq RPC.
     * @param[in] shmInfos The shared memory info of buffers.
     */
    void BatchDecreaseRefCnt(const std::vector<std::pair<ShmKey, std::uint32_t>> &shmInfos);

    /**
     * @brief Set the offset read object buffer.
     * @param[in] objectKey The object key of the cache object.
     * @param[in] info The protobuf object info.
     * @param[in] version Object version.
     * @param[in] offset Offset position of the object.
     * @param[in] size Offset read size of an object.
     * @param[out] buffer The object buffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status SetOffsetReadObjectBuffer(const std::string &objectKey, const GetRspPb::ObjectInfoPb &info, uint32_t version,
                                     uint64_t offset, uint64_t size, std::shared_ptr<Buffer> &buffer);

    /**
     * @brief Fill in object buffer info.
     * @param[in] objectKey The id of the object.
     * @param[in] pointer The starting pointer of the buffer.
     * @param[in] size The data size of the buffer.
     * @param[in] metaSize The metadata size.
     * @param[in] param The creating parameter of the buffer.
     * @param[in] isSeal Indicate whether the object buffer has been sealed.
     * @param[in] payLoadPointer For non_shared memory, the Get interface will pass in a pointer to initialize
     * @param[in] mmapEntry For shared memory, keep mmap entry to avoid it unmap.
     * the bufferInfo->pointer; other cases will pass in a nullptr.
     * @param[in] remoteHostInfo The remote host info for RH2D data transfer.
     * @return ObjectBufferInfo The struct which stores buffer info.
     */
    static std::shared_ptr<ObjectBufferInfo> MakeObjectBufferInfo(
        const std::string &objectKey, uint8_t *pointer, uint64_t size, uint64_t metaSize, const FullParam &param,
        bool isSeal, uint32_t version, const ShmKey &shmId = {},
        const std::shared_ptr<RpcMessage> &payloadPointer = nullptr,
        std::shared_ptr<client::IMmapTableEntry> mmapEntry = nullptr,
        std::shared_ptr<RemoteH2DHostInfoPb> remoteHostInfo = nullptr);

    /**
     * @brief Make the buffer invalid.
     * @param[in] objectKey The object key which binds to the buffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status InvalidateBuffer(const std::string &objectKey);

    /**
     * @brief Add multiple locks by duplicate object keys for globalRefCount_.
     * @param[in] objectKeys The vector contains the ids that need to be locked.
     * @param[out] accessorTable The map of object key and relate accessor.
     */
    void AddTbbLockForGlobalRefIds(const std::vector<std::string> &objectKeys,
                                   std::map<std::string, GlobalRefInfo> &accessorTable,
                                   std::unordered_map<std::string, std::string> &objTenantIdsToObj);

    /**
     * @brief Remove global reference meta in globalRefCount_ when refCnt is zero.
     * @param[in] checkIds The ids to be checked.
     * @param[in/out] accessorTable The map of object key and relate accessor.
     */
    void RemoveZeroGlobalRefByRefTable(const std::vector<std::string> &checkIds,
                                       std::map<std::string, GlobalRefInfo> &accessorTable);

    /**
     * @brief Rollback for increase failed object keys.
     * @param[in] rollbackObjectKeys The object keys for rollback.
     * @param[in/out] accessorTable The map of object key and relate accessor.
     */
    void GIncreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                              std::map<std::string, GlobalRefInfo> &accessorTable);

    /**
     * @brief Rollback for decrease failed object keys.
     * @param[in] rollbackObjectKeys The object keys for rollback.
     * @param[in/out] accessorTable The map of object key and relate accessor.
     */
    void GDecreaseRefRollback(const std::vector<std::string> &rollbackObjectKeys,
                              std::map<std::string, GlobalRefInfo> &accessorTable);

    /**
     * @brief Check that the key is in the correct format.
     * @param[in] key The key to check.
     * @return K_OK on success; the error code otherwise.
     */
    static Status CheckValidObjectKey(const std::string &key);

    /**
     * @brief Check that the string inside the container is legitimate.
     * @param[in] vec Vector to check.
     * @param[in] nullable Allow empty vector.
     * @return K_OK on success; the error code otherwise.
     */
    template <typename Vec>
    static Status CheckValidObjectKeyVector(const Vec &vec, bool nullable = false)
    {
        CHECK_FAIL_RETURN_STATUS(nullable || !vec.empty(), K_INVALID, "The keys are empty");
        if (!vec.empty()) {
            CHECK_FAIL_RETURN_STATUS(!vec.begin()->empty(), K_INVALID, FormatString("keys[%d] can not be empty", 0));
            RETURN_IF_NOT_OK(CheckValidObjectKey(*vec.begin()));
        }
        return Status::OK();
    }

    /**
     * @brief Get worker version.
     * @return Worker version.
     */
    uint32_t GetWorkerVersion();

    /**
     * @brief Get lock id.
     * @return Lock id.
     */
    uint32_t GetLockId() const;

    /**
     * @brief If can not connect to worker, do some clean worker in this function.
     */
    void ProcessWorkerLost();

    /**
     * @brief Clean local shm and mmap state when local worker heartbeat times out.
     */
    void ProcessWorkerTimeout();

    /**
     * @brief Process standby worker lost.
     * @param[in] node Standby worker node index.
     */
    void ProcessStandbyWorkerLost(WorkerNode node);

    /**
     * @brief Stop listening standby worker.
     * @param[in] id The id of standby worker.
     */
    void StopStandbyWorkerListen(WorkerNode id);

    /**
     * @brief Switch available worker when current worker lost.
     * @param[in] node Worker node index.
     * @return True if switch success.
     */
    bool SwitchWorkerNode(WorkerNode node);

    /**
     * @brief Switch to standby worker impl.
     * @param[in] currentApi Current client worker api.
     * @param[in] next Next standby worker index.
     * @return True if switch success.
     */
    bool SwitchToStandbyWorkerImpl(const std::shared_ptr<IClientWorkerApi> &currentApi, WorkerNode next);

    /**
     * @brief Try switch back to local worker.
     */
    bool TrySwitchBackToLocalWorker();

    /**
     * @brief Rediscover local worker via ServiceDiscovery when IP changes (e.g., pod restart, rolling upgrade).
     * @return True if local worker was successfully rediscovered and reconnected.
     */
    bool RediscoverLocalWorker();

    /**
     * @brief Reconnect the local worker at a new address. Cleans up old state and re-establishes SHM.
     * @param[in] newAddress The new worker address.
     * @return True if reconnection succeeded.
     */
    bool ReconnectLocalWorkerAt(const HostPort &newAddress);

    /**
     * @brief Check node is ready to exit or not.
     * @return True if node ready to exit.
     */
    bool ReadyToExit(WorkerNode node);

    /**
     * @brief Wait standby worker ready.
     * @param[in] clientWorkerApi Standby worker stub handler.
     * @return True if worker is ready.
     */
    bool WaitStandbyWorkerReady(const std::shared_ptr<IClientWorkerApi> &clientWorkerApi);

    /**
     * @brief Get the next worker node.
     * @param[in] current The current worker node.
     * @return WorkerNode The next worker node.
     */
    WorkerNode GetNextWorkerNode(WorkerNode current);

    /**
     * @brief Get the available workerApi.
     * @param[out] workerApi The available workerApi.
     * @return Status of the call.
     */
    Status GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi);

    /**
     * @brief Get the available workerApi.
     * @param[out] workerApi The available workerApi.
     * @param[out] raii Raii for record the invoke count.
     * @return Status of the call.
     */
    Status GetAvailableWorkerApi(std::shared_ptr<IClientWorkerApi> &workerApi, std::unique_ptr<Raii> &raii);

    /**
     * @brief Mmap a ShmUnit to client.
     * @param[in] fd The ShmUnit store fd.
     * @param[in] mmapSize The size of mmap
     * @param[in] offset The offset of ShmUnit
     * @param[out] mmapEntry The mmap entry of mmap manager
     * @param[out] pointer The data pointer.
     * @return Status of the call.
     */
    Status MmapShmUnit(int64_t fd, uint64_t mmapSize, ptrdiff_t offset,
                       std::shared_ptr<client::IMmapTableEntry> &mmapEntry, uint8_t *&pointer);

    /**
     * @brief Get the buffer info from Buffer
     * @param[in] buffer The input buffer
     * @return The shared pointer of ObjectBuffer information.
     */
    static std::shared_ptr<ObjectBufferInfo> GetBufferInfo(std::shared_ptr<Buffer> buffer)
    {
        return buffer->bufferInfo_;
    }

    /**
     * @brief Process put in shm scenes.
     * @param[in] objectKey The ID of the object to create.
     * @param[in] data The data pointer of the user.
     * @param[in] size The size in bytes of object.
     * @param[in] param The create parameters.
     * @param[in] nestedObjectKeys Objects that depend on objectKey.
     * @param[in] ttlSecond Used by state api, means how many seconds the key will be delete automatically.
     * @param[in] workerApi Available worker api.
     * @param[in] existence Used by state api, to determine whether to set or not set the key if it does already exist.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessShmPut(const std::string &objectKey, const uint8_t *data, uint64_t size, const FullParam &param,
                         const std::unordered_set<std::string> &nestedObjectKeys, uint32_t ttlSecond,
                         const std::shared_ptr<IClientWorkerApi> &workerApi, int existence);

    /**
     * @brief Check the validation of the input parameter of the multiple set.
     * @param[in] keys The keys to be set.
     * @param[in] vals The values for the keys.
     * @param[in] existence Whether set if some key exists.
     * @param[out] kvs key and values for set.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckMultiSetInputParamValidation(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                                             const ExistenceOpt &existence, std::map<std::string, StringView> &kvs);

    /**
     * @brief construcs object key with tenant id
     * @param[in] objKey object key
     * @return std::string object key with tenant id
     */
    std::string ConstructObjKeyWithTenantId(const std::string &objKey);

    /**
     * @brief Check the validation of the input parameter of the multiple set.
     * @param[in] keys The keys to be set.
     * @param[in] vals The values for the keys.
     * @param[in] existence Whether set if some key exists.
     * @param[out] outFailedKeys out failed keys.
     * @param[out] deduplicateKeys the deduplicate key .
     * @param[out] deduplicateVals the deduplicate vals .
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckMultiSetInputParamValidationNtx(const std::vector<std::string> &keys,
                                                const std::vector<StringView> &vals,
                                                std::vector<std::string> &outFailedKeys,
                                                std::vector<std::string> &deduplicateKeys,
                                                std::vector<StringView> &deduplicateVals);
    /**
     * @brief Check the validation of the input parameter of the multiple set.
     * @param[in] keys The keys to be set.
     * @param[in] vals The values for the keys.
     * @param[in] writeMode Indicate write through or back mode.
     * @param[in] workerApi Available worker api.
     * @param[out] bufferInfo The buffers information for creating buffers.
     * @param[out] buffer The object buffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status AllocateMemoryForMSet(const std::map<std::string, StringView> &kv, const WriteMode &writeMode,
                                 const std::shared_ptr<IClientWorkerApi> &workerApi,
                                 std::vector<std::shared_ptr<Buffer>> &buffers,
                                 std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo,
                                 const CacheType &cacheType);

    /**
     * @brief Muti create buffer parallel
     * @param[in] skipCheckExistence is skip check existence
     * @param[in] param set param
     * @param[in] version buffer version
     * @param[out] exists exists list
     * @param[out] multiCreateParamList create param list
     * @param[out] bufferList buffer list
     */
    Status MutiCreateParallel(const bool skipCheckExistence, const FullParam &param, const uint32_t &version,
                              std::vector<bool> &exists, std::vector<MultiCreateParam> &multiCreateParamList,
                              std::vector<std::shared_ptr<Buffer>> &bufferList);

    Status CreateBufferForMultiCreateParamAtIndex(size_t index, bool skipCheckExistence, const FullParam &param,
                                                  uint32_t version, const std::vector<bool> &exists,
                                                  std::vector<MultiCreateParam> &multiCreateParamList,
                                                  std::vector<std::shared_ptr<Buffer>> &bufferList);

    /**
     * @brief Get clientId.
     * @return The ID of the current client.
     */
    std::string GetClientId()
    {
        return workerApi_[currentNode_]->clientId_;
    }

    /**
     * @brief Get client state.
     * @return Client state.
     */
    uint16_t GetState()
    {
        return clientStateManager_->GetState();
    }

    void ConstructTreadPool();

    Status InitClientWorkerConnect(bool enableHeartbeat, bool initWithWorker);

    Status InitListenWorker();

    /**
     * @brief Convert a list of devBlobList to a list of device buffer pointers.
     * @param[in] keys A list of keys, each corresponding to a 2D device blob.
     * @param[in] blob2dList The blob list of device data.
     * @param[in] createParam Parameters for creating device buffers.
     * @param[out] deviceBuffPtrList A list to store the device buffer pointers.
     * @return K_OK on success; an error code otherwise.
     */
    Status ConvertToDevBufferPtrList(const std::vector<std::string> &keys,
                                     const std::vector<DeviceBlobList> &blob2dList,
                                     const CreateDeviceParam &createParam,
                                     std::vector<std::shared_ptr<DeviceBuffer>> &deviceBuffPtrList);
    /**
     * @brief Check if device id is a valid value.
     * @param[in] deviceId The device id.
     * @return K_OK on success; an error code otherwise.
     */
    Status CheckDeviceValid(std::vector<uint32_t> deviceId);

    /**
     * @brief Update the client's remoteH2D config
     * @param[in] devId The device id used by the client
     * @return K_OK on success; an error code otherwise.
     */
    Status UpdateClientRemoteH2DConfig(int32_t devId);

    void StartPerfThread();
    void ShutdownPerfThread();
    /**
     * @brief Memory copy in parallel or serial mode.
     * @param[in] isParallel Enable parallel or not.
     * @param[in] keys Object keys.
     * @param[in] vals Object values.
     * @param[in] creatParam The creating parameter of the buffer.
     * @param[in] bufferList The buffer of the objects.
     * @param[out] bufferInfoList The buffers information for creating buffers..
     * @return K_OK on success; the error code otherwise.
     */
    Status MemoryCopyParallel(bool isParallel, const std::vector<std::string> &keys,
                              const std::vector<StringView> &vals, const FullParam &creatParam,
                              std::vector<std::shared_ptr<Buffer>> &bufferList,
                              std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfoList);

    /**
     * @brief Start periodic reconcile thread for client-worker shm refs.
     */
    void StartShmRefReconcileThread();

    /**
     * @brief Stop periodic reconcile thread for client-worker shm refs.
     */
    void ShutdownShmRefReconcileThread();

    /**
     * @brief Periodically reconcile maybe-expired shm ids with worker.
     */
    void ShmRefReconcileThreadFunc();

    /**
     * @brief Decrease the object reference count by one and if no one holds its ref, release it.
     * @param[in] shmId The ID of the object to decrease ref
     * @param[in] isShm A flag indicating how the object will be published (shm or non-shm).
     * @param[in] version Worker version.
     * @return K_OK on success; the error code otherwise.
     */
    Status DecreaseReferenceCntImpl(const ShmKey &shmId, bool isShm, uint32_t version);

    /**
     * @brief Handle the shared memory reference count after multi-publish.
     * @param[in] bufferList The buffer list of the objects.
     * @param[in] rsp The response of the multi-publish operation.
     * @return K_OK on success; the error code otherwise.
     */
    Status HandleShmRefCountAfterMultiPublish(const std::vector<std::shared_ptr<Buffer>> &bufferList,
                                              const MultiPublishRspPb &rsp);

    /**
     * @brief Parse embedded config.
     * @param[in] config Embedded config.
     * @return K_OK on success; the error code otherwise.
     */
    Status ParseEmbeddedConfig(const EmbeddedConfig &config);

    HostPort ipAddress_;
    RpcAuthKeys authKeys_;
    RpcCredential cred_;
    int32_t requestTimeoutMs_;
    int32_t connectTimeoutMs_;
    uint64_t fastTransportMemSize_;
    SensitiveValue token_;
    std::string tenantId_;
    bool enableCrossNodeConnection_ = false;
    bool enableExclusiveConnection_ = false;
    std::unique_ptr<Signature> signature_{ nullptr };
    std::vector<std::shared_ptr<IClientWorkerApi>> workerApi_;
    std::atomic<WorkerNode> currentNode_{ LOCAL_WORKER };
    std::mutex switchNodeMutex_;  // Protecting the process of switching workers.
    std::unique_ptr<client::MmapManager> mmapManager_{ nullptr };
    std::unique_ptr<ClientDeviceObjectManager> devOcImpl_{ nullptr };
    bool enableRemoteH2D_;
    int32_t devId_ = -1;

    // Protect tbb map globalRefCount_, use unique_lock in for/while loop.
    mutable std::shared_timed_mutex globalRefMutex_;

    mutable std::shared_timed_mutex shutdownMux_;  // Protecting the process of shutdown.

    ClientMemoryRefTable memoryRefCount_;
    TbbGlobalRefTable globalRefCount_;

    std::unique_ptr<ClientStateManager> clientStateManager_{ nullptr };
    std::shared_ptr<datasystem::client::EmbeddedClientWorkerApi> embeddedClientWorkerApi_{ nullptr };
    void *worker_;
    std::shared_ptr<ThreadPool> memoryCopyThreadPool_;
    std::shared_ptr<ThreadPool> asyncSetRPCPool_;
    std::shared_ptr<ThreadPool> asyncGetRPCPool_;
    std::shared_ptr<ThreadPool> asyncGetCopyPool_;
    std::shared_ptr<ThreadPool> asyncSwitchWorkerPool_;
    std::shared_ptr<ThreadPool> asyncDevDeletePool_;
    std::shared_ptr<ThreadPool> asyncReleasePool_;

    // Listenworker needs to be placed at the bottom to ensure that it is destructed first.
    std::vector<std::shared_ptr<client::ListenWorker>> listenWorker_;

    // verify remoteClientId format.
    re2::RE2 simpleIdRe_{ "^[a-zA-Z0-9_]*$" };
    std::mutex perfMutex_;
    std::condition_variable perfCv_;
    std::atomic<bool> perfExitFlag_{ false };
    std::unique_ptr<Thread> perfThread_{ nullptr };
    std::atomic<bool> shmRefReconcileExitFlag_{ false };
    WaitPost shmRefReconcileExitPost_;
    std::unique_ptr<Thread> shmRefReconcileThread_{ nullptr };
    WaitPost switchPost_;

    bool clientEnableP2Ptransfer_ = false;
    int parallismNum_ = 0;
    uint64_t memcpyParallelThreshold_ = 0;

    std::shared_ptr<ServiceDiscovery> serviceDiscovery_ = nullptr;
};
}  // namespace object_cache
}  // namespace datasystem
#endif
