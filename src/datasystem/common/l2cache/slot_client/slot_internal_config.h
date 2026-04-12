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

/**
 * Description: Internal distributed-disk slot configuration.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_INTERNAL_CONFIG_H
#define DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_INTERNAL_CONFIG_H

#include <cstdint>

namespace datasystem {
constexpr uint32_t DISTRIBUTED_DISK_SLOT_NUM = 128;
constexpr uint64_t DISTRIBUTED_DISK_COMPACT_CUTOVER_BYTES = 1024UL * 1024UL;
constexpr uint32_t DISTRIBUTED_DISK_COMPACT_CUTOVER_RECORDS = 1024;
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_INTERNAL_CONFIG_H
