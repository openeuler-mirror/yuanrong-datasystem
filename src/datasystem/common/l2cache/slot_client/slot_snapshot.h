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
 * Description: In-memory snapshot built from slot index replay.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_SNAPSHOT_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_SNAPSHOT_H

#include <map>
#include <string>
#include <unordered_map>

#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/l2cache/slot_client/slot_manifest.h"

namespace datasystem {

/**
 * @brief Resolved object location returned by slot lookup.
 */
struct SlotSnapshotValue {
    std::string key;
    uint32_t fileId{ 0 };
    uint64_t offset{ 0 };
    uint64_t size{ 0 };
    uint64_t version{ 0 };
    WriteMode writeMode{ WriteMode::NONE_L2_CACHE };
    bool deleted{ false };
};

/**
 * @brief Materialized view of visible versions inside one slot.
 */
class SlotSnapshot {
public:
    ~SlotSnapshot() = default;

    /**
     * @brief Apply one PUT record to the snapshot.
     * @param[in] record The PUT record to apply.
     */
    void ApplyPut(const SlotPutRecord &record);

    /**
     * @brief Apply one DELETE record to the snapshot.
     * @param[in] record The DELETE record to apply.
     */
    void ApplyDelete(const SlotDeleteRecord &record);

    /**
     * @brief Find an exact version if it is still visible.
     * @param[in] key The object key.
     * @param[in] version The exact version to query.
     * @param[out] value The resolved object location.
     * @return Status of the call.
     */
    Status FindExact(const std::string &key, uint64_t version, SlotSnapshotValue &value) const;

    /**
     * @brief Find the latest visible version strictly newer than minVersion.
     * @param[in] key The object key.
     * @param[in] minVersion The lower version bound that returned version must exceed.
     * @param[out] value The resolved object location.
     * @return Status of the call.
     */
    Status FindLatest(const std::string &key, uint64_t minVersion, SlotSnapshotValue &value) const;

    /**
     * @brief Replay the active index and build a new snapshot.
     * @param[in] slotPath The slot directory path.
     * @param[in] manifest The active slot manifest.
     * @param[out] snapshot The rebuilt in-memory snapshot.
     * @return Status of the call.
     */
    static Status Replay(const std::string &slotPath, const SlotManifestData &manifest, SlotSnapshot &snapshot);

    /**
     * @brief Collect all visible PUT records in deterministic order.
     * @param[out] records The visible PUT records.
     * @return Status of the call.
     */
    Status CollectVisiblePuts(std::vector<SlotPutRecord> &records) const;

private:
    static bool IsValidPut(const std::string &slotPath, const SlotPutRecord &record);

    std::unordered_map<std::string, std::map<uint64_t, SlotPutRecord>> putsByKey_;
    std::unordered_map<std::string, uint64_t> deletedUpToByKey_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_SNAPSHOT_H
