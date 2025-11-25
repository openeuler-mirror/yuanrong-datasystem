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
 * Description: Hash algorithm
 */
#ifndef DATASYSTEM_COMMON_UTIL_HASH_ALGORITHM_H
#define DATASYSTEM_COMMON_UTIL_HASH_ALGORITHM_H

#include <string>

#include "datasystem/common/util/meta_route_tool.h"

namespace datasystem {

/**
 * @brief Get murmurhash3 result of input buffer.
 * @param[in] data Buffer to be hashed.
 * @param[in] len The size of buffer.
 * @param[in] seed Hash seed. Can be any value, zero by default.
 * @return uint32_t Hash result.
 */
uint32_t MurmurHash3_32(const uint8_t *data, size_t len, uint32_t seed = 0);

/**
 * @brief Get murmurhash3 result of input string.
 * @param[in] data String to be hashed.
 * @return uint32_t Hash result.
 */
inline uint32_t MurmurHash3_32(const std::string &data)
{
    return MurmurHash3_32(reinterpret_cast<const uint8_t *>(data.data()), data.size());
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_HASH_ALGORITHM_H