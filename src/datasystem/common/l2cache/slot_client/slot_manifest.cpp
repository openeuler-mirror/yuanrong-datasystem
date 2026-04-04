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

#include "datasystem/common/l2cache/slot_client/slot_manifest.h"

#include <sstream>

#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace {
constexpr const char *MANIFEST_FILE = "manifest";

bool IsNormalManifest(const SlotManifestData &manifest)
{
    return manifest.state == SlotState::NORMAL && manifest.opType == SlotOperationType::NONE
           && manifest.opPhase == SlotOperationPhase::NONE && manifest.role == SlotOperationRole::NONE;
}

Status ValidateManifestStateCombination(const SlotManifestData &manifest)
{
    if (manifest.state == SlotState::NORMAL) {
        CHECK_FAIL_RETURN_STATUS(IsNormalManifest(manifest), StatusCode::K_INVALID,
                                 "NORMAL manifest must use NONE/NONE/NONE operation tuple");
        CHECK_FAIL_RETURN_STATUS(manifest.txnId.empty(), StatusCode::K_INVALID,
                                 "NORMAL manifest must not carry txn_id");
        return Status::OK();
    }

    CHECK_FAIL_RETURN_STATUS(manifest.state == SlotState::IN_OPERATION, StatusCode::K_INVALID,
                             "Unknown slot manifest state");
    if (manifest.opType == SlotOperationType::COMPACT) {
        CHECK_FAIL_RETURN_STATUS(manifest.opPhase == SlotOperationPhase::COMPACT_COMMITTING, StatusCode::K_INVALID,
                                 "COMPACT manifest must use COMPACT_COMMITTING phase");
        CHECK_FAIL_RETURN_STATUS(manifest.role == SlotOperationRole::LOCAL, StatusCode::K_INVALID,
                                 "COMPACT manifest must use LOCAL role");
        CHECK_FAIL_RETURN_STATUS(!manifest.txnId.empty(), StatusCode::K_INVALID, "COMPACT manifest must carry txn_id");
        return Status::OK();
    }

    CHECK_FAIL_RETURN_STATUS(manifest.opType == SlotOperationType::TRANSFER, StatusCode::K_INVALID,
                             "IN_OPERATION manifest must use COMPACT or TRANSFER op_type");
    CHECK_FAIL_RETURN_STATUS(manifest.role == SlotOperationRole::SOURCE || manifest.role == SlotOperationRole::TARGET,
                             StatusCode::K_INVALID, "TRANSFER manifest must use SOURCE or TARGET role");
    CHECK_FAIL_RETURN_STATUS(manifest.opPhase == SlotOperationPhase::TRANSFER_FENCING
                                 || manifest.opPhase == SlotOperationPhase::TRANSFER_PREPARED
                                 || manifest.opPhase == SlotOperationPhase::TRANSFER_INDEX_PUBLISHED
                                 || manifest.opPhase == SlotOperationPhase::TRANSFER_SOURCE_RETIRED,
                             StatusCode::K_INVALID, "TRANSFER manifest must use a transfer phase");
    CHECK_FAIL_RETURN_STATUS(!manifest.txnId.empty(), StatusCode::K_INVALID, "TRANSFER manifest must carry txn_id");
    CHECK_FAIL_RETURN_STATUS(!manifest.recoverySlotPath.empty(), StatusCode::K_INVALID,
                             "TRANSFER manifest must carry recovery_slot_path");
    if (manifest.role == SlotOperationRole::TARGET && manifest.opPhase != SlotOperationPhase::TRANSFER_FENCING) {
        CHECK_FAIL_RETURN_STATUS(!manifest.transferPlanPath.empty(), StatusCode::K_INVALID,
                                 "TRANSFER target manifest must carry transfer_plan_path after fencing");
    }
    return Status::OK();
}

std::string JoinVector(const std::vector<std::string> &values)
{
    std::ostringstream ss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << values[i];
    }
    return ss.str();
}
}  // namespace

std::string ToString(SlotState state)
{
    switch (state) {
        case SlotState::NORMAL:
            return "NORMAL";
        case SlotState::IN_OPERATION:
            return "IN_OPERATION";
        default:
            return "NORMAL";
    }
}

Status SlotStateFromString(const std::string &value, SlotState &state)
{
    if (value == "NORMAL") {
        state = SlotState::NORMAL;
    } else if (value == "IN_OPERATION") {
        state = SlotState::IN_OPERATION;
    } else {
        RETURN_STATUS(StatusCode::K_INVALID, "Unknown slot state: " + value);
    }
    return Status::OK();
}

std::string ToString(SlotOperationType opType)
{
    switch (opType) {
        case SlotOperationType::NONE:
            return "NONE";
        case SlotOperationType::COMPACT:
            return "COMPACT";
        case SlotOperationType::TRANSFER:
            return "TRANSFER";
        default:
            return "NONE";
    }
}

Status SlotOperationTypeFromString(const std::string &value, SlotOperationType &opType)
{
    if (value == "NONE") {
        opType = SlotOperationType::NONE;
    } else if (value == "COMPACT") {
        opType = SlotOperationType::COMPACT;
    } else if (value == "TRANSFER") {
        opType = SlotOperationType::TRANSFER;
    } else {
        RETURN_STATUS(StatusCode::K_INVALID, "Unknown slot op_type: " + value);
    }
    return Status::OK();
}

std::string ToString(SlotOperationPhase opPhase)
{
    switch (opPhase) {
        case SlotOperationPhase::NONE:
            return "NONE";
        case SlotOperationPhase::COMPACT_COMMITTING:
            return "COMPACT_COMMITTING";
        case SlotOperationPhase::TRANSFER_FENCING:
            return "TRANSFER_FENCING";
        case SlotOperationPhase::TRANSFER_PREPARED:
            return "TRANSFER_PREPARED";
        case SlotOperationPhase::TRANSFER_INDEX_PUBLISHED:
            return "TRANSFER_INDEX_PUBLISHED";
        case SlotOperationPhase::TRANSFER_SOURCE_RETIRED:
            return "TRANSFER_SOURCE_RETIRED";
        default:
            return "NONE";
    }
}

Status SlotOperationPhaseFromString(const std::string &value, SlotOperationPhase &opPhase)
{
    if (value == "NONE") {
        opPhase = SlotOperationPhase::NONE;
    } else if (value == "COMPACT_COMMITTING") {
        opPhase = SlotOperationPhase::COMPACT_COMMITTING;
    } else if (value == "TRANSFER_FENCING") {
        opPhase = SlotOperationPhase::TRANSFER_FENCING;
    } else if (value == "TRANSFER_PREPARED") {
        opPhase = SlotOperationPhase::TRANSFER_PREPARED;
    } else if (value == "TRANSFER_INDEX_PUBLISHED") {
        opPhase = SlotOperationPhase::TRANSFER_INDEX_PUBLISHED;
    } else if (value == "TRANSFER_SOURCE_RETIRED") {
        opPhase = SlotOperationPhase::TRANSFER_SOURCE_RETIRED;
    } else {
        RETURN_STATUS(StatusCode::K_INVALID, "Unknown slot op_phase: " + value);
    }
    return Status::OK();
}

std::string ToString(SlotOperationRole role)
{
    switch (role) {
        case SlotOperationRole::NONE:
            return "NONE";
        case SlotOperationRole::LOCAL:
            return "LOCAL";
        case SlotOperationRole::SOURCE:
            return "SOURCE";
        case SlotOperationRole::TARGET:
            return "TARGET";
        default:
            return "NONE";
    }
}

Status SlotOperationRoleFromString(const std::string &value, SlotOperationRole &role)
{
    if (value == "NONE") {
        role = SlotOperationRole::NONE;
    } else if (value == "LOCAL") {
        role = SlotOperationRole::LOCAL;
    } else if (value == "SOURCE") {
        role = SlotOperationRole::SOURCE;
    } else if (value == "TARGET") {
        role = SlotOperationRole::TARGET;
    } else {
        RETURN_STATUS(StatusCode::K_INVALID, "Unknown slot role: " + value);
    }
    return Status::OK();
}

SlotManifestData SlotManifest::Bootstrap()
{
    return SlotManifestData{};
}

Status SlotManifest::Load(const std::string &slotPath, SlotManifestData &manifest)
{
    auto manifestPath = JoinPath(slotPath, MANIFEST_FILE);
    bool exists = false;
    RETURN_IF_NOT_OK(CheckFileExists(&exists, manifestPath));
    CHECK_FAIL_RETURN_STATUS(exists, StatusCode::K_NOT_FOUND, "Manifest not found: " + manifestPath);
    std::string content;
    RETURN_IF_NOT_OK(ReadWholeFile(manifestPath, content));
    RETURN_IF_NOT_OK(Decode(content, manifest));
    auto tmpPath = manifestPath + ".tmp";
    if (FileExist(tmpPath)) {
        (void)DeleteFile(tmpPath);
    }
    return Status::OK();
}

Status SlotManifest::StoreAtomic(const std::string &slotPath, const SlotManifestData &manifest)
{
    RETURN_IF_NOT_OK(CreateDir(slotPath, true));
    auto manifestPath = JoinPath(slotPath, MANIFEST_FILE);
    RETURN_IF_NOT_OK(AtomicWriteTextFile(manifestPath, Encode(manifest)));
    return Status::OK();
}

std::string SlotManifest::Encode(const SlotManifestData &manifest)
{
    std::ostringstream ss;
    ss << "version=" << manifest.version << "\n";
    ss << "state=" << ToString(manifest.state) << "\n";
    ss << "op_type=" << ToString(manifest.opType) << "\n";
    ss << "op_phase=" << ToString(manifest.opPhase) << "\n";
    ss << "role=" << ToString(manifest.role) << "\n";
    ss << "txn_id=" << manifest.txnId << "\n";
    ss << "owner_epoch=" << manifest.ownerEpoch << "\n";
    ss << "home_slot_path=" << manifest.homeSlotPath << "\n";
    ss << "recovery_slot_path=" << manifest.recoverySlotPath << "\n";
    ss << "peer_slot_path=" << manifest.peerSlotPath << "\n";
    ss << "transfer_plan_path=" << manifest.transferPlanPath << "\n";
    ss << "transfer_file_map=" << manifest.transferFileMap << "\n";
    ss << "active_index=" << manifest.activeIndex << "\n";
    ss << "active_data=" << JoinVector(manifest.activeData) << "\n";
    ss << "pending_index=" << manifest.pendingIndex << "\n";
    ss << "pending_data=" << JoinVector(manifest.pendingData) << "\n";
    ss << "gc_pending=" << (manifest.gcPending ? "true" : "false") << "\n";
    ss << "obsolete_index=" << manifest.obsoleteIndex << "\n";
    ss << "obsolete_data=" << JoinVector(manifest.obsoleteData) << "\n";
    ss << "last_compact_epoch_ms=" << manifest.lastCompactEpochMs << "\n";
    return ss.str();
}

Status SlotManifest::Decode(const std::string &content, SlotManifestData &manifest)
{
    manifest = Bootstrap();
    bool activeIndexSeen = false;
    bool activeDataSeen = false;
    std::istringstream ss(content);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.empty()) {
            continue;
        }
        auto pos = line.find('=');
        CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, StatusCode::K_INVALID, "Invalid manifest line: " + line);
        auto key = line.substr(0, pos);
        auto value = line.substr(pos + 1);
        if (key == "version") {
            try {
                manifest.version = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception &e) {
                RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid manifest version: ") + e.what());
            }
        } else if (key == "state") {
            RETURN_IF_NOT_OK(SlotStateFromString(value, manifest.state));
        } else if (key == "op_type") {
            RETURN_IF_NOT_OK(SlotOperationTypeFromString(value, manifest.opType));
        } else if (key == "op_phase") {
            RETURN_IF_NOT_OK(SlotOperationPhaseFromString(value, manifest.opPhase));
        } else if (key == "role") {
            RETURN_IF_NOT_OK(SlotOperationRoleFromString(value, manifest.role));
        } else if (key == "txn_id") {
            manifest.txnId = value;
        } else if (key == "owner_epoch") {
            try {
                manifest.ownerEpoch = static_cast<uint64_t>(std::stoull(value));
            } catch (const std::exception &e) {
                RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid owner epoch: ") + e.what());
            }
        } else if (key == "home_slot_path") {
            manifest.homeSlotPath = value;
        } else if (key == "recovery_slot_path") {
            manifest.recoverySlotPath = value;
        } else if (key == "peer_slot_path") {
            manifest.peerSlotPath = value;
        } else if (key == "transfer_plan_path") {
            manifest.transferPlanPath = value;
        } else if (key == "transfer_file_map") {
            manifest.transferFileMap = value;
        } else if (key == "active_index") {
            activeIndexSeen = true;
            manifest.activeIndex = value;
        } else if (key == "active_data") {
            activeDataSeen = true;
            manifest.activeData = value.empty() ? std::vector<std::string>{} : Split(value, ",");
        } else if (key == "pending_index") {
            manifest.pendingIndex = value;
        } else if (key == "pending_data") {
            manifest.pendingData = value.empty() ? std::vector<std::string>{} : Split(value, ",");
        } else if (key == "gc_pending") {
            manifest.gcPending = (value == "true");
        } else if (key == "obsolete_index") {
            manifest.obsoleteIndex = value;
        } else if (key == "obsolete_data") {
            manifest.obsoleteData = value.empty() ? std::vector<std::string>{} : Split(value, ",");
        } else if (key == "last_compact_epoch_ms") {
            try {
                manifest.lastCompactEpochMs = static_cast<uint64_t>(std::stoull(value));
            } catch (const std::exception &e) {
                RETURN_STATUS(StatusCode::K_INVALID, std::string("Invalid compact epoch: ") + e.what());
            }
        }
    }
    RETURN_IF_NOT_OK(ValidateManifestStateCombination(manifest));
    if (!activeDataSeen && manifest.activeData.empty() && IsNormalManifest(manifest)) {
        manifest.activeData.push_back(FormatDataFileName(1));
    }
    if (!activeIndexSeen && manifest.activeIndex.empty() && IsNormalManifest(manifest)) {
        manifest.activeIndex = "index_active.log";
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!manifest.activeIndex.empty() || manifest.role == SlotOperationRole::SOURCE,
                                         StatusCode::K_INVALID, "active_index must not be empty");
    return Status::OK();
}
}  // namespace datasystem
