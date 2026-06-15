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
 * Description: Some tools for routing meta data.
 */
#ifndef DATASYSTEM_COMMON_UTIL_META_ROUTE_TOOL_H
#define DATASYSTEM_COMMON_UTIL_META_ROUTE_TOOL_H

#include <cstdint>
#include <string>
#include <variant>

namespace datasystem {
inline const std::string &ExtractObjectId(const std::string &s)
{
    return s;
}

template <typename T>
inline const std::string &ExtractObjectId(const std::pair<std::string, T> &p)
{
    return p.first;
}

template <typename T>
inline std::string ExtractObjectId(std::pair<std::string, T> &&p)
{
    return std::move(p.first);
}

using HashPosition = uint32_t;
using Range = std::pair<uint32_t, uint32_t>;
struct RouteInfo {
    std::variant<std::monostate, Range> payload;  // hash range
    int64_t currHashRingVersion = -1;
};
}  // namespace datasystem
#endif
