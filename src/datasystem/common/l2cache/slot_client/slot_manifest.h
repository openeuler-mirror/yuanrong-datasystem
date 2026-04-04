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
 * Description: Slot manifest model and serialization helpers.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_MANIFEST_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_MANIFEST_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class SlotState : uint8_t {
    NORMAL = 0,
    IN_OPERATION = 1,
};

enum class SlotOperationType : uint8_t {
    NONE = 0,
    COMPACT = 1,
    TRANSFER = 2,
};

enum class SlotOperationPhase : uint8_t {
    NONE = 0,
    COMPACT_COMMITTING = 1,
    TRANSFER_FENCING = 2,
    TRANSFER_PREPARED = 3,
    TRANSFER_INDEX_PUBLISHED = 4,
    TRANSFER_SOURCE_RETIRED = 5,
};

enum class SlotOperationRole : uint8_t {
    NONE = 0,
    LOCAL = 1,
    SOURCE = 2,
    TARGET = 3,
};

/**
 * @brief Durable slot metadata stored in manifest.
 */
struct SlotManifestData {
    uint32_t version{ 1 };
    SlotState state{ SlotState::NORMAL };
    SlotOperationType opType{ SlotOperationType::NONE };
    SlotOperationPhase opPhase{ SlotOperationPhase::NONE };
    SlotOperationRole role{ SlotOperationRole::NONE };
    std::string txnId;
    uint64_t ownerEpoch{ 0 };
    std::string homeSlotPath;
    std::string recoverySlotPath;
    std::string peerSlotPath;
    std::string transferPlanPath;
    std::string transferFileMap;
    std::string activeIndex{ "index_active.log" };
    std::vector<std::string> activeData{ FormatDataFileName(1) };
    std::string pendingIndex;
    std::vector<std::string> pendingData;
    bool gcPending{ false };
    std::string obsoleteIndex;
    std::vector<std::string> obsoleteData;
    uint64_t lastCompactEpochMs{ 0 };
};

/**
 * @brief Serialize and atomically persist slot manifest state.
 */
class SlotManifest {
public:
    ~SlotManifest() = default;

    /**
     * @brief Build a default manifest for a new slot.
     * @return The default manifest data.
     */
    static SlotManifestData Bootstrap();

    /**
     * @brief Load and parse the manifest from disk.
     * @param[in] slotPath The slot directory path.
     * @param[out] manifest The loaded manifest data.
     * @return Status of the call.
     */
    static Status Load(const std::string &slotPath, SlotManifestData &manifest);

    /**
     * @brief Atomically store the manifest via temp-file replacement.
     * @param[in] slotPath The slot directory path.
     * @param[in] manifest The manifest data to persist.
     * @return Status of the call.
     */
    static Status StoreAtomic(const std::string &slotPath, const SlotManifestData &manifest);

    /**
     * @brief Convert a manifest to line-based text format.
     * @param[in] manifest The manifest data to encode.
     * @return The encoded manifest text.
     */
    static std::string Encode(const SlotManifestData &manifest);

    /**
     * @brief Parse manifest text into a structured representation.
     * @param[in] content The encoded manifest text.
     * @param[out] manifest The decoded manifest data.
     * @return Status of the call.
     */
    static Status Decode(const std::string &content, SlotManifestData &manifest);
};

/**
 * @brief Convert a slot state enum to its durable string value.
 * @param[in] state The slot state enum value.
 * @return The durable string representation.
 */
std::string ToString(SlotState state);

/**
 * @brief Parse the durable string value into a slot state enum.
 * @param[in] value The durable string representation.
 * @param[out] state The parsed slot state enum value.
 * @return Status of the call.
 */
Status SlotStateFromString(const std::string &value, SlotState &state);

std::string ToString(SlotOperationType opType);

Status SlotOperationTypeFromString(const std::string &value, SlotOperationType &opType);

std::string ToString(SlotOperationPhase opPhase);

Status SlotOperationPhaseFromString(const std::string &value, SlotOperationPhase &opPhase);

std::string ToString(SlotOperationRole role);

Status SlotOperationRoleFromString(const std::string &value, SlotOperationRole &role);
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_MANIFEST_H
