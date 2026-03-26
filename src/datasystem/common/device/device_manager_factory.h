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
 * Description: Device manager factory with runtime device detection.
 * Both NPU and GPU backends are compiled in (when USE_NPU/USE_GPU defined).
 * The actual backend is selected at runtime by probing /dev/davinci* or /dev/nvidia*.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_FACTORY_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_FACTORY_H

#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/log/log.h"
#if defined(USE_NPU) || defined(USE_GPU)
#include "datasystem/common/util/file_util.h"
#endif

#ifdef USE_NPU
#include "datasystem/common/device/ascend/acl_device_manager.h"
#endif

#ifdef USE_GPU
#include "datasystem/common/device/nvidia/cuda_device_manager.h"
#endif

namespace datasystem {

enum class DeviceBackend { NPU, GPU, UNKNOWN };

/**
 * @brief Device Manager Factory with runtime device detection.
 *
 * When both USE_NPU and USE_GPU are defined (universal wheel build),
 * the factory probes /dev/davinci* and /dev/nvidia* at runtime to
 * decide which backend to use. Only the selected backend's plugin
 * is dlopen'd.
 *
 * Usage:
 *   DeviceManagerBase* mgr = DeviceManagerFactory::GetDeviceManager();
 */
class DeviceManagerFactory {
public:
    static DeviceManagerBase *GetDeviceManager()
    {
        static DeviceManagerBase *inst = Detect();
        return inst;
    }

    static DeviceBackend ProbeBackend()
    {
#ifdef USE_NPU
        if (HasDevNode("/dev/davinci[0-16]*")) return DeviceBackend::NPU;
#endif
#ifdef USE_GPU
        if (HasDevNode("/dev/nvidia[0-9]*")) return DeviceBackend::GPU;
#endif
        return DeviceBackend::UNKNOWN;
    }

private:
    DeviceManagerFactory() = delete;
    ~DeviceManagerFactory() = delete;

    static bool HasDevNode(const char *pattern)
    {
#if defined(USE_NPU) || defined(USE_GPU)
        std::vector<std::string> paths;
        Status rc = Glob(pattern, paths);
        if (rc.IsError()) {
            LOG(WARNING) << "Glob failed for pattern " << pattern << ", rc: " << rc.ToString();
            return false;
        }
        return !paths.empty();
#else
        (void)pattern;
        return false;
#endif
    }

    static DeviceManagerBase *Detect()
    {
        bool hasNpu = false;
        bool hasGpu = false;
#ifdef USE_NPU
        hasNpu = HasDevNode("/dev/davinci[0-16]*");
#endif
#ifdef USE_GPU
        hasGpu = HasDevNode("/dev/nvidia[0-9]*");
#endif
        if (hasNpu && hasGpu) {
            LOG(WARNING) << "Both NPU (/dev/davinci*) and GPU (/dev/nvidia*) devices detected. "
                            "By policy, NPU backend is preferred.";
#ifdef USE_NPU
            return acl::AclDeviceManager::Instance();
#endif
#ifdef USE_GPU
            return cuda::CudaDeviceManager::Instance();
#endif
            return nullptr;
        }
#ifdef USE_NPU
        if (hasNpu) {
            LOG(INFO) << "Detected NPU device (/dev/davinci*), using Ascend ACL backend.";
            return acl::AclDeviceManager::Instance();
        }
#endif
#ifdef USE_GPU
        if (hasGpu) {
            LOG(INFO) << "Detected GPU device (/dev/nvidia*), using CUDA backend.";
            return cuda::CudaDeviceManager::Instance();
        }
#endif
        LOG(ERROR) << "No accelerator device detected. "
                      "Checked:"
#ifdef USE_NPU
                      " /dev/davinci[0-16]*"
#endif
#ifdef USE_GPU
                      " /dev/nvidia[0-9]*"
#endif
                      ". Ensure the device driver is installed and "
                      "the device node exists.";
                      
        return nullptr;
    }
};

}  // namespace datasystem
#endif // DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_FACTORY_H