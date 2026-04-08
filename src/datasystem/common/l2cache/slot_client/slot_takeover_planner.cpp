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

#include "datasystem/common/l2cache/slot_client/slot_takeover_planner.h"

#include <algorithm>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/l2cache/slot_client/slot_index_codec.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
std::string JoinMappings(const std::vector<SlotTakeoverDataFileMapping> &mappings)
{
    std::ostringstream ss;
    for (size_t i = 0; i < mappings.size(); ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << mappings[i].sourceFileId << ":" << mappings[i].sourceDataFile << ":" << mappings[i].targetFileId << ":"
           << mappings[i].targetDataFile;
    }
    return ss.str();
}

Status ParseMappings(const std::string &value, std::vector<SlotTakeoverDataFileMapping> &mappings)
{
    mappings.clear();
    if (value.empty()) {
        return Status::OK();
    }
    for (const auto &item : Split(value, ",")) {
        auto fields = Split(item, ":");
        constexpr int fieldNum = 4;
        CHECK_FAIL_RETURN_STATUS(fields.size() == fieldNum, StatusCode::K_INVALID, "Invalid takeover mapping: " + item);
        SlotTakeoverDataFileMapping mapping;
        try {
            mapping.sourceFileId = static_cast<uint32_t>(std::stoul(fields[0]));
            constexpr int targetFiledIndex = 2;
            mapping.targetFileId = static_cast<uint32_t>(std::stoul(fields[targetFiledIndex]));
        } catch (const std::exception &e) {
            RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid takeover mapping: ") + e.what());
        }
        constexpr int dataFileIndex = 3;
        mapping.sourceDataFile = fields[1];
        mapping.targetDataFile = fields[dataFileIndex];
        mappings.emplace_back(std::move(mapping));
    }
    return Status::OK();
}

Status GetNextTargetFileId(const std::string &targetSlotPath, const SlotManifestData &targetManifest, uint32_t &fileId)
{
    fileId = 0;
    auto updateMaxFileId = [&fileId](const std::string &filename) {
        uint32_t parsedFileId = 0;
        if (ParseDataFileId(filename, parsedFileId).IsOk()) {
            fileId = std::max(fileId, parsedFileId);
        }
    };
    for (const auto &name : targetManifest.activeData) {
        updateMaxFileId(name);
    }
    for (const auto &name : targetManifest.pendingData) {
        updateMaxFileId(name);
    }
    for (const auto &name : targetManifest.obsoleteData) {
        updateMaxFileId(name);
    }
    std::vector<std::string> dataPaths;
    RETURN_IF_NOT_OK(Glob(JoinPath(targetSlotPath, "data_*.bin"), dataPaths));
    for (const auto &path : dataPaths) {
        auto pos = path.find_last_of('/');
        updateMaxFileId(pos == std::string::npos ? path : path.substr(pos + 1));
    }
    fileId = fileId == 0 ? 1 : fileId + 1;
    return Status::OK();
}
}  // namespace

Status SlotTakeoverPlanner::BuildPlan(const std::string &sourceHomeSlotPath, const std::string &sourceRecoverySlotPath,
                                      const SlotManifestData &sourceManifest, const SlotSnapshot &sourceSnapshot,
                                      const std::string &targetSlotPath, const SlotManifestData &targetManifest,
                                      const SlotTakeoverRequest &request, const std::string &txnId,
                                      SlotTakeoverPlan &plan)
{
    CHECK_FAIL_RETURN_STATUS(!txnId.empty(), StatusCode::K_INVALID, "txnId must not be empty");
    plan = SlotTakeoverPlan{};
    plan.txnId = txnId;
    plan.sourceHomeSlotPath = sourceHomeSlotPath;
    plan.sourceRecoverySlotPath = sourceRecoverySlotPath;
    plan.targetSlotPath = targetSlotPath;
    plan.sourceIndex = sourceManifest.activeIndex;
    plan.importIndexFile = FormatImportIndexFileName(txnId);
    plan.loadToMemory = request.IsPreload();

    uint32_t nextTargetFileId = 0;
    RETURN_IF_NOT_OK(GetNextTargetFileId(targetSlotPath, targetManifest, nextTargetFileId));

    std::vector<SlotPutRecord> visiblePuts;
    RETURN_IF_NOT_OK(sourceSnapshot.CollectVisiblePuts(visiblePuts));
    std::unordered_set<uint32_t> visibleSourceFileIds;
    for (const auto &record : visiblePuts) {
        visibleSourceFileIds.insert(record.fileId);
    }

    std::unordered_map<uint32_t, uint32_t> fileIdMapping;
    for (const auto &sourceDataFile : sourceManifest.activeData) {
        uint32_t sourceFileId = 0;
        RETURN_IF_NOT_OK(ParseDataFileId(sourceDataFile, sourceFileId));
        if (visibleSourceFileIds.find(sourceFileId) == visibleSourceFileIds.end()) {
            continue;
        }
        SlotTakeoverDataFileMapping mapping;
        mapping.sourceFileId = sourceFileId;
        mapping.sourceDataFile = sourceDataFile;
        mapping.targetFileId = nextTargetFileId++;
        mapping.targetDataFile = FormatDataFileName(mapping.targetFileId);
        fileIdMapping[mapping.sourceFileId] = mapping.targetFileId;
        plan.dataMappings.emplace_back(std::move(mapping));
    }

    auto importIndexPath = JoinPath(targetSlotPath, plan.importIndexFile);
    RETURN_IF_NOT_OK(SlotIndexCodec::EnsureIndexFile(importIndexPath));
    RETURN_IF_NOT_OK(SlotIndexCodec::TruncateTail(importIndexPath, SlotIndexCodec::HEADER_SIZE));

    std::string importPayload;
    importPayload.reserve(visiblePuts.size() * 64);
    for (const auto &record : visiblePuts) {
        auto it = fileIdMapping.find(record.fileId);
        CHECK_FAIL_RETURN_STATUS(it != fileIdMapping.end(), StatusCode::K_RUNTIME_ERROR,
                                 "Missing takeover file mapping for visible put");
        SlotPutRecord imported = record;
        imported.fileId = it->second;
        std::string encoded;
        RETURN_IF_NOT_OK(SlotIndexCodec::EncodePut(imported, encoded));
        importPayload.append(encoded);
    }
    RETURN_IF_NOT_OK(SlotIndexCodec::AppendEncodedRecords(importIndexPath, importPayload, true));
    return Status::OK();
}

Status SlotTakeoverPlanner::DumpPlan(const std::string &targetSlotPath, const SlotTakeoverPlan &plan)
{
    RETURN_IF_NOT_OK(CreateDir(targetSlotPath, true));
    auto planPath = JoinPath(targetSlotPath, FormatTakeoverPlanFileName(plan.txnId));
    RETURN_IF_NOT_OK(AtomicWriteTextFile(planPath, Encode(plan)));
    return Status::OK();
}

Status SlotTakeoverPlanner::LoadPlan(const std::string &planPath, SlotTakeoverPlan &plan)
{
    std::string content;
    RETURN_IF_NOT_OK(ReadWholeFile(planPath, content));
    RETURN_IF_NOT_OK(Decode(content, plan));
    return Status::OK();
}

std::string SlotTakeoverPlanner::Encode(const SlotTakeoverPlan &plan)
{
    std::ostringstream ss;
    ss << "version=" << plan.version << "\n";
    ss << "txn_id=" << plan.txnId << "\n";
    ss << "source_home_slot=" << plan.sourceHomeSlotPath << "\n";
    ss << "source_recovery_slot=" << plan.sourceRecoverySlotPath << "\n";
    ss << "target_slot=" << plan.targetSlotPath << "\n";
    ss << "source_index=" << plan.sourceIndex << "\n";
    ss << "import_index=" << plan.importIndexFile << "\n";
    ss << "load_to_memory=" << (plan.loadToMemory ? "true" : "false") << "\n";
    ss << "data_mappings=" << JoinMappings(plan.dataMappings) << "\n";
    return ss.str();
}

Status SlotTakeoverPlanner::Decode(const std::string &content, SlotTakeoverPlan &plan)
{
    plan = SlotTakeoverPlan{};
    std::istringstream ss(content);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.empty()) {
            continue;
        }
        auto pos = line.find('=');
        CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, StatusCode::K_INVALID, "Invalid takeover plan line");
        auto key = line.substr(0, pos);
        auto value = line.substr(pos + 1);
        if (key == "version") {
            try {
                plan.version = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception &e) {
                RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid takeover plan version: ") + e.what());
            }
        } else if (key == "txn_id") {
            plan.txnId = value;
        } else if (key == "source_home_slot") {
            plan.sourceHomeSlotPath = value;
        } else if (key == "source_recovery_slot") {
            plan.sourceRecoverySlotPath = value;
        } else if (key == "target_slot") {
            plan.targetSlotPath = value;
        } else if (key == "source_index") {
            plan.sourceIndex = value;
        } else if (key == "import_index") {
            plan.importIndexFile = value;
        } else if (key == "load_to_memory") {
            plan.loadToMemory = (value == "true");
        } else if (key == "data_mappings") {
            RETURN_IF_NOT_OK(ParseMappings(value, plan.dataMappings));
        }
    }
    CHECK_FAIL_RETURN_STATUS(!plan.txnId.empty(), StatusCode::K_INVALID, "takeover txn_id must not be empty");
    CHECK_FAIL_RETURN_STATUS(!plan.sourceHomeSlotPath.empty(), StatusCode::K_INVALID,
                             "takeover source_home_slot must not be empty");
    CHECK_FAIL_RETURN_STATUS(!plan.sourceRecoverySlotPath.empty(), StatusCode::K_INVALID,
                             "takeover source_recovery_slot must not be empty");
    CHECK_FAIL_RETURN_STATUS(!plan.targetSlotPath.empty(), StatusCode::K_INVALID,
                             "takeover target_slot must not be empty");
    CHECK_FAIL_RETURN_STATUS(!plan.importIndexFile.empty(), StatusCode::K_INVALID,
                             "takeover import_index must not be empty");
    return Status::OK();
}
}  // namespace datasystem
