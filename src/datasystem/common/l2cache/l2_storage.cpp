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
 * Description: l2 storage config.
 */

#include "datasystem/common/l2cache/l2_storage.h"

namespace datasystem {
L2StorageType GetCurrentStorageType()
{
    L2StorageType l2StorageType(L2StorageType::NONE);
    if (FLAGS_l2_cache_type == "obs") {
        SETFLAG(l2StorageType, L2StorageType::OBS);
    } else if (FLAGS_l2_cache_type == "sfs") {
        SETFLAG(l2StorageType, L2StorageType::SFS);
    } else if (FLAGS_l2_cache_type == "distributed_disk") {
        SETFLAG(l2StorageType, L2StorageType::DISTRIBUTED_DISK);
    }
    return l2StorageType;
}

bool IsSupportL2Storage(L2StorageType type)
{
    return TESTFLAG(type, L2StorageType::OBS) || TESTFLAG(type, L2StorageType::SFS)
           || TESTFLAG(type, L2StorageType::DISTRIBUTED_DISK);
}
}  // namespace datasystem
