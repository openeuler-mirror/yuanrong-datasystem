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
 * Description: Injected dependencies used by object-cache recovery paths.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_DEPENDENCIES_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_DEPENDENCIES_H

#include <memory>

#include "datasystem/worker/object_cache/service/object_metadata_reader.h"
#include "datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"

namespace datasystem {
namespace object_cache {

struct ObjectCacheRecoveryDependencies {
    std::shared_ptr<ObjectMetadataReader> metadataReader;
    std::shared_ptr<SlotRecoveryStore> slotRecoveryStore;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_DEPENDENCIES_H
