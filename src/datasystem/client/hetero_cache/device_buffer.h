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
 * Description: The DeviceBuffer to manager device memory.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_DEVICE_DEVICEBUFFER_H
#define DATASYSTEM_OBJECT_CACHE_DEVICE_DEVICEBUFFER_H

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/hetero/future.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ObjectClientImpl;
class ClientDeviceObjectManager;
}  // namespace object_cache
struct DeviceBufferInfo;
class DeviceMemoryUnit;
}  // namespace datasystem

namespace datasystem {

class DeviceBuffer : public std::enable_shared_from_this<DeviceBuffer> {
public:
    DeviceBuffer() = default;
    DeviceBuffer(DeviceBuffer &&other) noexcept;
    DeviceBuffer &operator=(DeviceBuffer &&other) noexcept;
    DeviceBuffer(const DeviceBuffer &other) = delete;
    DeviceBuffer &operator=(const DeviceBuffer &other) = delete;
    virtual ~DeviceBuffer();

    /// \brief Get the data size of the DeviceBuffer.
    ///
    /// \return The data size of the DeviceBuffer.
    uint64_t Size() const;

    /// \brief Publish device object to datasystem.
    ///
    /// \return Status of the result.
    Status Publish();

    /// \brief Gets the device memory pointer.
    ///
    /// \return A void * to the device memory.
    void *Data() const;

    /// \brief Gets the device idx of this device memory.
    ///
    /// \return The device index.
    int32_t GetDeviceIdx() const;

    /// \brief Gets the list of future in device memory sending, it only work in MOVE lifetime.
    ///
    /// \param[out] futureVec The deviceid to which data belongs.
    ///
    /// \return Status of the result. Return error if lifetime is not MOVE.
    Status GetSendStatus(std::vector<Future> &futureVec);

    /// \brief Gets the list of DataInfo.
    ///
    ///
    /// \return The list of DataInfo.
    std::vector<DataInfo> GetDataInfoList() const;

    /// \brief Detach a directory location
    ///
    void Detach();

    /// \brief Get object key from buffer
    ///
    /// \return object key
    std::string GetObjectKey();

private:
    friend class datasystem::object_cache::ObjectClientImpl;
    friend class datasystem::object_cache::ClientDeviceObjectManager;
    friend class P2PSubscribe;
    friend class WorkerOCServiceImpl;

    DeviceBuffer(std::shared_ptr<DeviceBufferInfo> bufferInfo, std::shared_ptr<DeviceMemoryUnit> devMemUnit,
                 std::shared_ptr<object_cache::ObjectClientImpl> clientImpl);

    /// \brief The only purpose of having this function is to encapsulate the above private DeviceBuffer constructor,
    ///         to make it work with std::make_shared<DeviceBuffer>. Directly use std::make_shared with a private
    ///         constructor will cause compilation errors.
    ///
    /// \param[in] DeviceBufferInfo The DeviceBuffer information for creating DeviceBuffer.
    /// \param[in] clientImpl Object client impl.
    ///
    /// \return New created DeviceBuffer.
    static std::shared_ptr<DeviceBuffer> CreateDeviceBuffer(
        std::shared_ptr<DeviceBufferInfo> bufferInfo, std::shared_ptr<DeviceMemoryUnit> devMemUnit,
        const std::shared_ptr<object_cache::ObjectClientImpl> &clientImpl);

    /// \brief Reset the DeviceBuffer, ignore the owned resource.
    void Reset();

    /// \brief Release the buffer owned resources.
    void Release();
    
    /// \brief Get the device memory unit.
    ///
    /// \return The device memory unit.
    std::shared_ptr<DeviceMemoryUnit> GetDeviceMemUnit();

    std::shared_ptr<DeviceBufferInfo> bufferInfo_;
    std::shared_ptr<DeviceMemoryUnit> deviceMemUnit_;
    std::shared_ptr<object_cache::ObjectClientImpl> clientImpl_;
};

struct Shard {
    uint64_t srcOffset = 0;
    uint64_t dstOffset = 0;
    uint64_t shardSz = 0;
};

using ReShardingMap = std::unordered_map<std::string, Shard>;
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_DeviceBuffer_H
