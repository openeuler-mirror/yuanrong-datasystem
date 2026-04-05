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
 * Description: l2 storage config.
 */
#ifndef DATASYSTEM_COMMON_L2CACHE_L2_STORAGE_H
#define DATASYSTEM_COMMON_L2CACHE_L2_STORAGE_H

#include <cstdint>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/bitmask_enum.h"

DS_DECLARE_string(l2_cache_type);

namespace datasystem {
enum class L2StorageType : uint32_t { NONE = 0, OBS = 1u, SFS = 1u << 2, DISTRIBUTED_DISK = 1u << 3 };
ENABLE_BITMASK_ENUM_OPS(L2StorageType);

L2StorageType GetCurrentStorageType();

bool IsSupportL2Storage(L2StorageType type);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_L2CACHE_L2_STORAGE_H
