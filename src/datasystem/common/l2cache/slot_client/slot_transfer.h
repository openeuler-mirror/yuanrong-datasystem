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
 * Description: Slot transfer request/callback structures.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_TRANSFER_H
#define DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_TRANSFER_H

#include <cstdint>
#include <functional>
#include <memory>
#include <sstream>
#include <string>

#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class SlotTakeoverMode : uint8_t {
    MERGE = 0,
    PRELOAD = 1,
};

struct SlotPreloadMeta {
    std::string objectKey;
    uint64_t version{ 0 };
    WriteMode writeMode{ WriteMode::NONE_L2_CACHE };
    uint64_t size{ 0 };
    uint32_t ttlSecond{ 0 };
};

using SlotPreloadCallback =
    std::function<Status(const SlotPreloadMeta &meta, const std::shared_ptr<std::stringstream> &content)>;

struct SlotTakeoverRequest {
    SlotTakeoverMode mode{ SlotTakeoverMode::MERGE };
    SlotPreloadCallback callback;

    bool IsPreload() const
    {
        return mode == SlotTakeoverMode::PRELOAD;
    }
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SLOT_CLIENT_SLOT_TRANSFER_H
