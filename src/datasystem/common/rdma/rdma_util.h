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

#ifndef DATASYSTEM_COMMON_RDMA_RDMA_UTIL_H
#define DATASYSTEM_COMMON_RDMA_RDMA_UTIL_H

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
/**
 * @brief Get the Ethernet device name from the destination ip.
 * @param[in] ipAddr The destination ip address.
 * @param[out] devName The ethernet device name.
 * @return Status of the call.
 */
Status GetDevNameFromDestIp(const std::string &ipAddr, std::string &devName);

/**
 * @brief Get the Ethernet device name from the local ip.
 * @param[in] ipAddr The local ip address.
 * @param[out] devName The ethernet device name.
 * @return Status of the call.
 */
int GetDevNameFromLocalIp(const std::string &ipAddr, std::string &devName);

/**
 * @brief Get the RDMA device name from the Ethernet device name.
 * @note If there is no RDMA device for the input device name, it will search and return other RDMA device if possible.
 * @param[in] ethDevName The Ethernet device name.
 * @param[out] rdmaDevName The RDMA device name.
 * @return Status of the call.
 */
Status EthToRdmaDevName(std::string ethDevName, std::string &rdmaDevName);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RDMA_RDMA_UTIL_H