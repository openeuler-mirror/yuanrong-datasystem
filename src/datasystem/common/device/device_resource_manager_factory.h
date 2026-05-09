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
 * Description: Device resource manager factory.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_RESOURCE_MANAGER_FACTORY_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_RESOURCE_MANAGER_FACTORY_H

#include <memory>

#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/common/device/nvidia/cuda_resource_manager.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"

namespace datasystem {

class DeviceResourceManagerFactory {
public:
    static std::unique_ptr<DeviceResourceManager> Create()
    {
#if defined(WITH_TESTS) && !defined(BUILD_HETERO)
        return std::make_unique<AclResourceManager>();
#else
        switch (DeviceManagerFactory::ProbeBackend()) {
            case DeviceBackend::NPU:
                return std::make_unique<AclResourceManager>();
            case DeviceBackend::GPU:
                return std::make_unique<CudaResourceManager>();
            case DeviceBackend::UNKNOWN:
            default:
                LOG(INFO) << "No accelerator backend detected for DeviceResourceManagerFactory.";
                return nullptr;
        }
#endif
    }

private:
    DeviceResourceManagerFactory() = delete;
    ~DeviceResourceManagerFactory() = delete;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_DEVICE_RESOURCE_MANAGER_FACTORY_H
