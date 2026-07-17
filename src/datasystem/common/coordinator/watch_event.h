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
 * Description: Watch event for coordinator KV store.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_WATCH_EVENT_H
#define DATASYSTEM_COMMON_COORDINATOR_WATCH_EVENT_H

#include <cstddef>
#include <cstdint>

#include "datasystem/common/coordinator/key_value_entry.h"

namespace datasystem {
static constexpr size_t MAX_WATCH_EVENTS_PER_BATCH = 32;
static constexpr size_t MAX_WATCH_EVENT_BATCH_BYTES = 8 * 1'024 * 1'024;
static constexpr size_t WATCH_EVENT_WIRE_OVERHEAD_BYTES = 128;

struct WatchEvent {
    enum class Type { PUT, DELETE, REWATCH };
    Type type;
    KeyValueEntry entry;
    int64_t revision = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_WATCH_EVENT_H
