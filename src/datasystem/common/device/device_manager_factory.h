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

/**
 * Description: Device manager factory for selecting between NPU (ACL) and GPU (CUDA) backends.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_FACTORY_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_FACTORY_H

#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/device_config.h"

// Include the appropriate device manager header based on configuration
#if USE_NPU
#include "datasystem/common/device/ascend/acl_device_manager.h"
#endif

#if USE_GPU
#include "datasystem/common/device/nvidia/cuda_device_manager.h"
#endif

namespace datasystem {

/**
 * @brief Device Manager Factory
 * This factory provides a unified way to get the appropriate device manager
 * based on compile-time configuration (USE_NPU or USE_GPU).
 * Usage:
 *   DeviceManagerBase* manager = DeviceManagerFactory::GetDeviceManager();
 *   manager->Init(nullptr);
 *   // ... use manager ...
 * The returned pointer is a singleton managed by the underlying device manager class.
 * Do NOT delete the returned pointer.
 */
class DeviceManagerFactory {
public:
    /**
     * @brief Get the device manager instance based on compile-time configuration
     * @return Pointer to the device manager singleton (AclDeviceManager or CudaDeviceManager)
     * @note The returned pointer should NOT be deleted by the caller
     */
    static DeviceManagerBase *GetDeviceManager()
    {
#if USE_NPU
        return acl::AclDeviceManager::Instance();
#elif USE_GPU
        return cuda::CudaDeviceManager::Instance();
#endif
    }

    /**
     * @brief Check if NPU backend is enabled
     * @return true if USE_NPU is enabled
     */
    static constexpr bool IsNpuEnabled()
    {
#if USE_NPU
        return true;
#else
        return false;
#endif
    }

    /**
     * @brief Check if GPU backend is enabled
     * @return true if USE_GPU is enabled
     */
    static constexpr bool IsGpuEnabled()
    {
#if USE_GPU
        return true;
#else
        return false;
#endif
    }

    /**
     * @brief Get the name of the current device backend
     * @return "NPU" or "GPU"
     */
    static const char *GetBackendName()
    {
#if USE_NPU
        return "NPU";
#elif USE_GPU
        return "GPU";
#endif
    }

private:
    // Prevent instantiation - this is a static-only class
    DeviceManagerFactory() = delete;
    ~DeviceManagerFactory() = delete;
};

}  // namespace datasystem
#endif // DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_FACTORY_H