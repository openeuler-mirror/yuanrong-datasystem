/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Data system Device Object Client implementation.
 */

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_CLIENT_DEVICE_OBJECT_MANAGER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_CLIENT_DEVICE_OBJECT_MANAGER_H

#include <memory>
#include <unordered_map>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/constants.h"
#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/client/object_cache/device/p2p_subscribe.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/hetero/device_common.h"
#include "datasystem/object_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ObjectClientImpl;

class ClientDeviceObjectManager {
public:
    ClientDeviceObjectManager(ObjectClientImpl *impl);
    ~ClientDeviceObjectManager() = default;

    Status Init();

    /**
     * @brief Publish device object to datasystem with host.
     * @param[in] buffer The device buffer ready to publish.
     * @return Status of the result.
     */
    Status PublishDeviceObjectWithHost(const std::shared_ptr<DeviceBuffer> &buffer);

    /**
     * @brief Publish device object to datasystem with p2p.
     * @param[in] buffer The device buffer ready to publish.
     * @return Status of the result.
     */
    Status PublishDeviceObjectWithP2P(const std::shared_ptr<DeviceBuffer> &buffer);

    /**
     * @brief Publish device object to datasystem.
     * @param[in] buffer The device buffer ready to publish.
     * @return Status of the result.
     */
    Status PublishDeviceObject(const std::shared_ptr<DeviceBuffer> &buffer);

    /**
     * @brief Invoke worker client to get the given device object keys and copy to the destinationdevice buffer.
     * @param[in] devObjKeys The vector of the object key. Key should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. Key length should less than 256.
     * Pass one object key to vector if you just want to get an device object. Don't support to pass multi object keys
     * now.
     * @param[out] dstDevBuffer The destination of device buffer.
     * @param[in] timeoutMs Timeout(ms) of waiting for the result return if object not ready. A positive integer number
     * required. 0 means no waiting time allowed. And the range is [0, INT32_MAX].
     * @return K_OK on any object success; the error code otherwise.
     *         K_INVALID: the vector of keys is empty or include empty key.
     *         K_NOT_FOUND: The objects not exists.
     *         K_RUNTIME_ERROR: Cannot get objects from worker.
     */
    Status GetDevBufferWithHost(const std::vector<std::string> &devObjKeys, const ReShardingMap &map, int32_t timeoutMs,
                                DeviceBuffer &dstDevBuffer);

    /**
     * @brief Get or create p2p subscribe object.
     * @param[in] deviceId The deviceid to which data belongs.
     * @param[out] p2pSubscribe The pointer of p2p subscribe object.
     * @return Status of call.
     */
    Status GetOrCreateP2PSubscribe(int32_t deviceId, std::shared_ptr<P2PSubscribe> &p2pSubscribe);

    /**
     * @brief Invoke worker client to create a device object with p2p.
     * @param[in] objectKey The Key of the device object to create. Key should not be empty and should only contains
     * english alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. Key length should less than 256.
     * @param[in] devBlobList The list of blob info.
     * @param[in] param The create param of device object.
     * @param[out] deviceBuffer The device buffer for the object.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status CreateDevBuffer(const std::string &devObjKey, const DeviceBlobList &devBlobList,
                           const CreateDeviceParam &param, std::shared_ptr<DeviceBuffer> &deviceBuffer);

    /**
     * @brief Invoke worker client to get the given device object keys and copy to the destinationdevice buffer.
     * @param[in] devObjKeys The vector of the object key. Key should not be empty and should only contains english
     * alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. Key length should less than 256.
     * Pass one object key to vector if you just want to get an device object. Don't support to pass multi object keys
     * now.
     * @param[in] timeoutMs Timeout(ms) of waiting for the result return if object not ready. A positive integer number
     * required. 0 means no waiting time allowed. And the range is [0, INT32_MAX].
     * @param[out] dstDevBuffer The destination of device buffer.
     * @param[in] subTimeoutMs The maximum time elapse of subscriptions.
     * @return K_OK on any object success; the error code otherwise.
     *         K_INVALID: the vector of keys is empty or include empty key.
     *         K_NOT_FOUND: The objects not exists.
     *         K_RUNTIME_ERROR: Cannot get objects from worker.
     */
    Status AsyncGetDevBuffer(const std::vector<std::string> &devObjKeys,
                             std::vector<std::shared_ptr<DeviceBuffer>> &dstDevBuffers, std::vector<Future> &futureVec,
                             int64_t prefetchTimeoutMs, int64_t subTimeoutMs);

    /**
     * @brief The implement of create device buffer.
     * @param[in] bufferInfo The info of device buffer.
     * @param[in] devBlobList The list of blob info.
     * @param[out] deviceBuffer The device buffer for the object.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status CreateDevBufferImpl(std::shared_ptr<DeviceBufferInfo> bufferInfo, const DeviceBlobList &devBlobList,
                               std::shared_ptr<DeviceBuffer> &deviceBuffer);

    /**
     * @brief Gets the list of future in device memory sending, it only work in MOVE lifetime.
     * @param[out] futureVec The deviceid to which data belongs.
     * @return Status of the result.
     */
    Status GetSendStatus(const std::shared_ptr<DeviceBuffer> &buffer, std::vector<Future> &futureVec);

    /**
     * @brief The memory copy between devBlobList and bufferList
     * @param[in] devBlobList The 2D list of blob info.
     * @param[in] bufferList The list of buffer.
     * @param[in] copyKind The memory copy kind.
     * @param[in] enableHugeTlb The memory is enable huge tlb.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status MemCopyBetweenDevAndHost(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList,
                                    MemcpyKind copyKind, bool enableHugeTlb);

    /**
     * @brief Set the interrupt flag of the thread to true.
     */
    void SetThreadInterruptFlag2True();

    /**
     * @brief Remove subscribe
     * @param[in] key The subscribe key.
     */
    void RemoveSubscribe(const std::string &key);

private:
    Status CheckDeviceRuntimeAvailable(const char *operation) const;

    DeviceManagerBase *devInterImpl_;
    ObjectClientImpl *objClientImpl_;
    std::unique_ptr<DeviceResourceManager> resourceMgr_;
    std::shared_ptr<CommFactory> commFactory_;
    tbb::concurrent_hash_map<int, std::shared_ptr<P2PSubscribe>> subscribeTable_;
    tbb::concurrent_hash_map<std::string, DeviceMemoryUnit> memUnitTable_;
    int32_t clientDevOJTimeoutMs_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif
