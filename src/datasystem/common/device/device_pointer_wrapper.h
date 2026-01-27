/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_POINTER_WRAPPER_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_POINTER_WRAPPER_H

#include <memory>

#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/device/ascend/cann_types.h"

namespace datasystem {
/**
 * @brief RAII wrapper for HCCL communicator
 */
class DevicePointerWrapper {
public:
    explicit DevicePointerWrapper(void *pointer) : pointer_(pointer)
    {
    }

    DevicePointerWrapper() : DevicePointerWrapper(nullptr)
    {
    }

    // Must not be copyable
    DevicePointerWrapper(const DevicePointerWrapper &) = delete;

    DevicePointerWrapper &operator=(const DevicePointerWrapper &) = delete;

    // Move constructable
    DevicePointerWrapper(DevicePointerWrapper &&other) noexcept
    {
        std::swap(pointer_, other.pointer_);
    }

    // Move assignable
    DevicePointerWrapper &operator=(DevicePointerWrapper &&other) noexcept
    {
        std::swap(pointer_, other.pointer_);
        return *this;
    }

    virtual void ShutDown()
    {
    }

    virtual ~DevicePointerWrapper() = default;

    /**
     * @brief Get the pointer
     * @return The pointer
     */
    [[nodiscard]] void *Get() const
    {
        return pointer_;
    }

    /**
     * @brief Get the pointer reference
     * @return The pointer reference
     */
    [[nodiscard]] void *&GetRef()
    {
        return pointer_;
    }

    void *pointer_;
};

class DeviceRtEventWrapper : public DevicePointerWrapper {
public:
    /**
     * @brief Create the aclRtEvent wrapper.
     * @param[out] event The aclRtEvent wrapper.
     * @return Status of the call.
     */
    static Status Create(std::shared_ptr<DeviceRtEventWrapper> &event)
    {
        if (event == nullptr) {
            event = std::make_shared<DeviceRtEventWrapper>();
        }
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        return deviceMgr->CreateEvent(&(event->GetRef()));
    }

    ~DeviceRtEventWrapper()
    {
        auto event = GetRef();
        if (event != nullptr) {
            auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
            deviceMgr->DestroyEvent(event);
            event = nullptr;
        }
    }

    /**
     * @brief Synchronize the event
     * @param[in] timeoutMs The timeout of the sync.
     * @return Status of the call.
     */
    Status SynchronizeEvent(int32_t timeoutMs = 0)
    {
        auto event = GetRef();
        CHECK_FAIL_RETURN_STATUS(event != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        if (timeoutMs != 0) {
            return deviceMgr->SynchronizeEventWithTimeout(event, timeoutMs);
        }
        return deviceMgr->SynchronizeEvent(event);
    }

    /**
     * @brief Record the event in this stream.
     * @param[in] stream The acl runtime stream.
     * @return Status of the call.
     */
    Status RecordEvent(aclrtStream stream)
    {
        auto event = Get();
        CHECK_FAIL_RETURN_STATUS(event != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        return deviceMgr->RecordEvent(event, stream);
    }

    /**
     * @brief Queries whether the events recorded by aclrtRecordEvent
     * @return Status of the call.
     */
    Status QueryEventStatus()
    {
        CHECK_FAIL_RETURN_STATUS(Get() != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        return deviceMgr->QueryEventStatus(Get());
    }
};
}  // namespace datasystem
#endif
