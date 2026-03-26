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
 * Description: The flag used by command line.
 */
#ifndef DATASYSTEM_COMMON_FLAGS_STRING_TO_LONG_H
#define DATASYSTEM_COMMON_FLAGS_STRING_TO_LONG_H

#include <cstddef>
#include <cctype>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace datasystem {
/**
 * @brief Check if the string contains the negative sign.
 * @param[in] str string to be checked.
 * @return true if has negative sign, else false.
 */
inline bool IsNegative(const std::string &str)
{
    size_t idx = 0;
    while (idx < str.size() && std::isspace(str[idx])) {
        ++idx;
    }
    if (idx < str.size() && str[idx] == '-') {
        return true;
    }
    return false;
}

/**
 * @brief Convert string to unsigned long long, using stoull directly will interpret negative number as extremely
 * large positive values, therefore it’s necessary to check if the string contains a negative sign.
 * @param[in] str string to be interpreted.
 * @return Converted number.
 */
inline unsigned long long StrToUnsignedLongLong(const std::string &str, std::size_t *pos = nullptr)
{
    if (IsNegative(str)) {
        throw std::out_of_range("The string " + str + " is out of range for unsigned long long type.");
    }
    return std::stoull(str, pos);
}

/**
 * @brief Convert string to unsigned long, using stoull directly will interpret negative number as extremely large
 * positive values, therefore it’s necessary to check if the string contains a negative sign.
 * @param[in] str string to be interpreted.
 * @return Converted number.
 */
inline unsigned long StrToUnsignedLong(const std::string &str, std::size_t *pos = nullptr)
{
    if (IsNegative(str)) {
        throw std::out_of_range("The string " + str + " is out of range for unsigned long type.");
    }
    return std::stoul(str, pos);
}
}  // namespace datasystem

#endif
