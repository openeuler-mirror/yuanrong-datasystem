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
 * Description: Slot takeover plan builder and serializer.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_TAKEOVER_PLANNER_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_TAKEOVER_PLANNER_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/common/l2cache/slot_client/slot_snapshot.h"
#include "datasystem/common/l2cache/slot_client/slot_transfer.h"
#include "datasystem/utils/status.h"

namespace datasystem {

struct SlotTakeoverDataFileMapping {
    uint32_t sourceFileId{ 0 };
    std::string sourceDataFile;
    uint32_t targetFileId{ 0 };
    std::string targetDataFile;
};

struct SlotTakeoverPlan {
    uint32_t version{ 1 };
    std::string txnId;
    std::string sourceHomeSlotPath;
    std::string sourceRecoverySlotPath;
    std::string targetSlotPath;
    std::string sourceIndex;
    std::string importIndexFile;
    bool loadToMemory{ false };
    std::vector<SlotTakeoverDataFileMapping> dataMappings;
};

class SlotTakeoverPlanner {
public:
    ~SlotTakeoverPlanner() = default;

    /**
     * @brief Build a durable takeover plan and the matching import log.
     * @param[in] sourceSlotPath The source slot directory path.
     * @param[in] sourceManifest The repaired source slot manifest.
     * @param[in] sourceSnapshot The visible source snapshot.
     * @param[in] targetSlotPath The target slot directory path.
     * @param[in] targetManifest The current target slot manifest.
     * @param[in] request The takeover request metadata.
     * @param[out] plan The built takeover plan.
     * @return Status of the call.
     */
    static Status BuildPlan(const std::string &sourceHomeSlotPath, const std::string &sourceRecoverySlotPath,
                            const SlotManifestData &sourceManifest, const SlotSnapshot &sourceSnapshot,
                            const std::string &targetSlotPath, const SlotManifestData &targetManifest,
                            const SlotTakeoverRequest &request, const std::string &txnId, SlotTakeoverPlan &plan);

    /**
     * @brief Persist the takeover plan into the target slot directory.
     * @param[in] targetSlotPath The target slot directory path.
     * @param[in] plan The takeover plan to persist.
     * @return Status of the call.
     */
    static Status DumpPlan(const std::string &targetSlotPath, const SlotTakeoverPlan &plan);

    /**
     * @brief Load and parse a takeover plan from the given plan file.
     * @param[in] planPath The absolute takeover plan path.
     * @param[out] plan The decoded takeover plan.
     * @return Status of the call.
     */
    static Status LoadPlan(const std::string &planPath, SlotTakeoverPlan &plan);

    /**
     * @brief Encode a takeover plan into a line-based text format.
     * @param[in] plan The takeover plan to encode.
     * @return The encoded takeover plan text.
     */
    static std::string Encode(const SlotTakeoverPlan &plan);

    /**
     * @brief Decode a takeover plan from a line-based text format.
     * @param[in] content The encoded takeover plan text.
     * @param[out] plan The decoded takeover plan.
     * @return Status of the call.
     */
    static Status Decode(const std::string &content, SlotTakeoverPlan &plan);
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_TAKEOVER_PLANNER_H
