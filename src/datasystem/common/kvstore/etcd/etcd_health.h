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
 * Description: Check etcd health
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_HEALTH_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_HEALTH_H

#include "datasystem/utils/status.h"

namespace datasystem {
/**
* @brief Check etcd health.
* @param[in] address Etcd IP address.
*/
Status CheckEtcdHealth(const std::string &address);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_HEALTH_H
