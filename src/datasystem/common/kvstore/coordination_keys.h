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

namespace datasystem {
static constexpr char COORDINATION_HASHRING_TABLE[] = "/datasystem/ring";
static constexpr char COORDINATION_CLUSTER_TABLE[] = "datasystem/cluster";  // Keep without leading '/'.
static constexpr char COORDINATION_MASTER_ADDRESS_TABLE[] = "/datasystem";
static constexpr char COORDINATION_MASTER_ADDRESS_KEY[] = "master_address";
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_COORDINATION_KEYS_H
