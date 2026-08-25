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
 * Description: Shared resource bounds for eviction-policy control.
 */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_EVICTION_POLICY_COMMON_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_EVICTION_POLICY_COMMON_H

#include <cstdint>

namespace datasystem {
// One batch runs synchronously on the resource-report control path. Keep it
// bounded so an operator request cannot turn one report into an O(N) scan.
constexpr uint32_t EVICTION_POLICY_MAX_MIGRATION_BATCH_SIZE = 4096;
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_EVICTION_POLICY_COMMON_H
