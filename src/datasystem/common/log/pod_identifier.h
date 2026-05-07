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
 * Description: Pod identifier util.
 */
#ifndef DATASYSTEM_COMMON_LOG_POD_IDENTIFIER_H
#define DATASYSTEM_COMMON_LOG_POD_IDENTIFIER_H

#include <cstdlib>
#include <string>

namespace datasystem {

inline std::string GetPodIdentifier()
{
    constexpr const char *envPriority[] = { "POD_IP", "POD_NAME", "HOSTNAME" };
    for (const auto &envName : envPriority) {
        const char *envValue = std::getenv(envName);
        if (envValue != nullptr && envValue[0] != '\0') {
            return envValue;
        }
    }
    return " ";
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_POD_IDENTIFIER_H
