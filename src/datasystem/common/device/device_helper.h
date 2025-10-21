/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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
 * Description: Defines some common functions in device object.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_HELPER_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_HELPER_H

#include <string>
#include <tuple>
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
static const std::string P2P_DEFAULT_MASTER = "DEFAULT";
static const int32_t ALL_DEVICE_ID = -1;
/**
 * @brief Concat the client and device id to identify an communicator.
 * @param[in] clientId The client id.
 * @param[in] deviceId The device id.
 * @return The concat result, i.e., npu id.
 */
inline std::string ConcatClientAndDeviceId(const std::string &clientId, int32_t deviceId)
{
    return clientId + ";" + std::to_string(deviceId);
}

inline std::tuple<std::string, int32_t> SplitNpuId(const std::string &npuId)
{
    size_t correctItemNum = 2;
    auto result = SplitToUniqueStr(npuId, ";");
    auto deviceId = -1;
    auto strIndex = result[0].size() > result[1].size() ? 0 : 1;
    if (result.size() == correctItemNum && StringToInt(result[1 - strIndex], deviceId)) {
        return std::make_tuple(result[strIndex], deviceId);
    }
    return std::make_tuple("", -1);
}

inline static std::string GetHcclPeerId(const std::string &srcClientId, int32_t srcDeviceId,
                                        const std::string &dstCilentId, int32_t dstDeviceId)
{
    return srcClientId + ";" + std::to_string(srcDeviceId) + "---" + dstCilentId + ";" + std::to_string(dstDeviceId);
}
}  // namespace datasystem
#endif