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
 * Description: Slot compact artifact builder.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_COMPACTOR_H
#define DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_COMPACTOR_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/l2cache/slot_client/slot_manifest.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/l2cache/slot_client/slot_snapshot.h"
#include "datasystem/utils/status.h"

namespace datasystem {

struct SlotCompactBuildResult {
    std::string indexFile;
    std::vector<std::string> dataFiles;
};

class SlotCompactor {
public:
    SlotCompactor(std::string slotPath, uint64_t maxDataFileBytes);

    ~SlotCompactor() = default;

    Status BuildArtifacts(const SlotManifestData &manifest, const SlotSnapshot &snapshot, uint64_t compactEpochMs,
                          SlotCompactBuildResult &result);

    Status ApplyDeltaRecords(const std::vector<SlotRecord> &records, SlotCompactBuildResult &result);

    Status CleanupArtifacts(const SlotCompactBuildResult &result);

private:
    Status CopyRecordToTargetFile(int sourceFd, uint64_t sourceOffset, uint64_t size, int targetFd,
                                  uint64_t targetOffset) const;
    Status AllocateCompactIndexFile(const SlotManifestData &manifest, uint64_t compactEpochMs,
                                    std::string &indexFile) const;
    Status GetNextDataFileId(const SlotManifestData &manifest, uint32_t &nextFileId) const;

    const std::string slotPath_;
    const uint64_t maxDataFileBytes_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_SFS_SLOT_STORE_SLOT_COMPACTOR_H
