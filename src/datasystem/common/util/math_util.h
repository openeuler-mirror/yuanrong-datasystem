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

#ifndef DATASYSTEM_COMMON_UTIL_MATH_UTIL_H
#define DATASYSTEM_COMMON_UTIL_MATH_UTIL_H

#include <cstdint>
#include <limits>

namespace datasystem {

inline uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs)
{
    return std::numeric_limits<uint64_t>::max() - lhs < rhs ? std::numeric_limits<uint64_t>::max() : lhs + rhs;
}

inline uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs)
{
    if (rhs != 0 && lhs > std::numeric_limits<uint64_t>::max() / rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_MATH_UTIL_H
