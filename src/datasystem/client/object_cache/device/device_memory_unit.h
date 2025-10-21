/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the device memory unit.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_DEVICE_MEMORY_UNIT
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_DEVICE_MEMORY_UNIT

#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class DeviceMemoryUnit {
public:
    explicit DeviceMemoryUnit(const std::string &devMemId, std::vector<DataInfo> dataInfoStorage);

    /**
     * @brief Malloc device memory if user not set the device pointer in data info list.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status MallocDeviceMemoryIfUserNotSet();

    /**
     * @brief Get the data info list.
     * @return The list of data info.
     */
    const std::vector<DataInfo> &GetDataInfoStorage() const;

    /**
     * @brief Check and get the single data info.
     * @param[out] dataInfo The data info.
     * @return Return ok only if memory unit have only one data info.
     */
    Status CheckAndGetSingleDataInfo(DataInfo &dataInfo) const;

    /**
     * @brief Check if the device pointer is nullptr in data info list.
     * @return The status of call.
     */
    Status CheckEmptyPointer() const;

    ~DeviceMemoryUnit();

private:
    std::string devMemId_;
    std::vector<DataInfo> dataInfoStorage_;
    std::deque<bool> dsAllocatedStorage_;
};
}  // namespace datasystem
#endif