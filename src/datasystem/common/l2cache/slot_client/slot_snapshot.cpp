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

#include "datasystem/common/l2cache/slot_client/slot_snapshot.h"

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"

#include <algorithm>

namespace datasystem {

void SlotSnapshot::ApplyPut(const SlotPutRecord &record)
{
    putsByKey_[record.key][record.version] = record;
}

void SlotSnapshot::ApplyDelete(const SlotDeleteRecord &record)
{
    auto &deletedVersion = deletedUpToByKey_[record.key];
    deletedVersion = std::max(deletedVersion, record.version);
}

Status SlotSnapshot::FindExact(const std::string &key, uint64_t version, SlotSnapshotValue &value) const
{
    auto it = putsByKey_.find(key);
    CHECK_FAIL_RETURN_STATUS(it != putsByKey_.end(), StatusCode::K_NOT_FOUND_IN_L2CACHE, "Key not found");
    auto pit = it->second.find(version);
    CHECK_FAIL_RETURN_STATUS(pit != it->second.end(), StatusCode::K_NOT_FOUND_IN_L2CACHE, "Version not found");
    auto dit = deletedUpToByKey_.find(key);
    CHECK_FAIL_RETURN_STATUS(dit == deletedUpToByKey_.end() || version > dit->second,
                             StatusCode::K_NOT_FOUND_IN_L2CACHE, "Version deleted");
    value = {
        key, pit->second.fileId, pit->second.offset, pit->second.size, pit->second.version, pit->second.writeMode, false
    };
    return Status::OK();
}

Status SlotSnapshot::FindLatest(const std::string &key, uint64_t minVersion, SlotSnapshotValue &value) const
{
    auto it = putsByKey_.find(key);
    CHECK_FAIL_RETURN_STATUS(it != putsByKey_.end(), StatusCode::K_NOT_FOUND_IN_L2CACHE, "Key not found");
    uint64_t deletedVersion = 0;
    auto dit = deletedUpToByKey_.find(key);
    if (dit != deletedUpToByKey_.end()) {
        deletedVersion = dit->second;
    }
    for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit) {
        if (rit->first > minVersion && rit->first > deletedVersion) {
            value = { key,
                      rit->second.fileId,
                      rit->second.offset,
                      rit->second.size,
                      rit->second.version,
                      rit->second.writeMode,
                      false };
            return Status::OK();
        }
    }
    RETURN_STATUS(StatusCode::K_NOT_FOUND_IN_L2CACHE, "No visible version found");
}

Status SlotSnapshot::Replay(const std::string &slotPath, const SlotManifestData &manifest, SlotSnapshot &snapshot)
{
    snapshot = SlotSnapshot{};
    std::vector<SlotRecord> records;
    size_t validBytes = 0;
    RETURN_IF_NOT_OK(SlotIndexCodec::ReadAllRecords(JoinPath(slotPath, manifest.activeIndex), records, validBytes));
    bool importBatchOpen = false;
    std::string importTxnId;
    std::vector<SlotRecord> stagedImportRecords;
    auto applyRecord = [&snapshot, &slotPath](const SlotRecord &record) {
        if (record.type == SlotRecordType::PUT) {
            if (IsValidPut(slotPath, record.put)) {
                snapshot.ApplyPut(record.put);
            }
        } else if (record.type == SlotRecordType::DELETE) {
            snapshot.ApplyDelete(record.del);
        }
    };
    for (const auto &record : records) {
        if (record.type == SlotRecordType::IMPORT_BEGIN) {
            importBatchOpen = true;
            importTxnId = record.import.txnId;
            stagedImportRecords.clear();
            continue;
        }
        if (record.type == SlotRecordType::IMPORT_END) {
            if (importBatchOpen && record.import.txnId == importTxnId) {
                for (const auto &staged : stagedImportRecords) {
                    applyRecord(staged);
                }
            }
            importBatchOpen = false;
            importTxnId.clear();
            stagedImportRecords.clear();
            continue;
        }
        if (importBatchOpen) {
            stagedImportRecords.emplace_back(record);
            continue;
        }
        applyRecord(record);
    }
    return Status::OK();
}

Status SlotSnapshot::CollectVisiblePuts(std::vector<SlotPutRecord> &records) const
{
    records.clear();
    for (const auto &putsEntry : putsByKey_) {
        uint64_t deletedVersion = 0;
        auto dit = deletedUpToByKey_.find(putsEntry.first);
        if (dit != deletedUpToByKey_.end()) {
            deletedVersion = dit->second;
        }
        for (const auto &versionEntry : putsEntry.second) {
            if (versionEntry.first > deletedVersion) {
                records.push_back(versionEntry.second);
            }
        }
    }
    std::sort(records.begin(), records.end(), [](const SlotPutRecord &lhs, const SlotPutRecord &rhs) {
        if (lhs.key != rhs.key) {
            return lhs.key < rhs.key;
        }
        return lhs.version < rhs.version;
    });
    return Status::OK();
}

bool SlotSnapshot::IsValidPut(const std::string &slotPath, const SlotPutRecord &record)
{
    auto dataPath = JoinPath(slotPath, FormatDataFileName(record.fileId));
    auto fileSize = FileSize(dataPath, false);
    if (fileSize < 0) {
        return false;
    }
    return record.offset <= static_cast<uint64_t>(fileSize) && record.size <= static_cast<uint64_t>(fileSize)
           && record.offset + record.size <= static_cast<uint64_t>(fileSize);
}
}  // namespace datasystem
