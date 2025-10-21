/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Defines operations related to gflag.
 */

#ifndef DATASYSTEM_COMMON_UTIL_VERSION_H
#define DATASYSTEM_COMMON_UTIL_VERSION_H

#include <string>

#include "datasystem/common/log/log.h"

namespace datasystem {
/**
 * @brief Compare the git hash of client and sever. If not equal, print log error.
 * @param[in] clientGitHash The git hash of client.
 */
void CheckClientGitHash(const std::string &clientGitHash);

/**
 * @brief Get the Git Hash string. This remove the developer name for privacy and security purpose.
 * @param[in] gitHash The git hash string to match.
 * @return bool. Return true if match or false if not.
 */
bool MatchGitHash(const std::string &gitHash);

/**
 * @brief Get the Git Hash string. This remove the developer name for privacy and security purpose.
 * @return std::string The git hash.
 */
std::string GetGitHash();
}

#endif // DATASYSTEM_COMMON_UTIL_VERSION_H