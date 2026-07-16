/*
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

/**
 * Description: Shared coordination table and key names for cluster topology metadata.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_COORDINATION_KEYS_H
#define DATASYSTEM_COMMON_KVSTORE_COORDINATION_KEYS_H

#include <string>

namespace datasystem {
static constexpr char COORDINATION_CLUSTER_TABLE[] = "datasystem/cluster";  // Keep without leading '/'.
static constexpr char COORDINATION_MASTER_ADDRESS_TABLE[] = "/datasystem";
static constexpr char COORDINATION_MASTER_ADDRESS_KEY[] = "master_address";

inline std::string EtcdMembershipTable(const std::string &clusterName)
{
    const std::string table = "/" + std::string(COORDINATION_CLUSTER_TABLE);
    return clusterName.empty() ? table : "/" + clusterName + table;
}

inline bool IsCoordinationKeyUnderPrefix(const std::string &key, const std::string &prefix)
{
    return key == prefix || key.rfind(prefix + "/", 0) == 0;
}

inline std::string RemoveCoordinationTablePrefix(const std::string &key, const std::string &prefix)
{
    const std::string prefixWithSlash = prefix + "/";
    if (key.rfind(prefixWithSlash, 0) == 0) {
        return key.substr(prefixWithSlash.size());
    }
    return key == prefix ? "" : key;
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_COORDINATION_KEYS_H
