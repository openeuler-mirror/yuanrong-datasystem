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
 * Description: Key-value entry for coordinator KV store.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_KEY_VALUE_ENTRY_H
#define DATASYSTEM_COMMON_COORDINATOR_KEY_VALUE_ENTRY_H

#include <cstdint>
#include <string>

namespace datasystem {
static constexpr int64_t COORDINATOR_NO_VERSION_CHECK = -1;
static constexpr int64_t COORDINATOR_KEY_NOT_EXISTS_VERSION = 0;
static constexpr int64_t COORDINATOR_NO_MOD_REVISION_CHECK = 0;
static constexpr int64_t COORDINATOR_NO_GLOBAL_REVISION_CHECK = 0;

struct KeyValueEntry {
    std::string key;
    std::string value;
    int64_t version = 0;
    int64_t modRevision = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_KEY_VALUE_ENTRY_H
