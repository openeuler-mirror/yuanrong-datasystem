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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SPILL_OBJECT_LOCATION_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SPILL_OBJECT_LOCATION_H

#include <cstdint>
#include <string>

namespace datasystem {
namespace object_cache {

struct ObjectLocation {
    std::string path;
    uint64_t offset = 0;
    uint64_t size = 0;
    uint64_t physicalOffset = 0;
    uint64_t physicalSize = 0;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_SPILL_OBJECT_LOCATION_H
