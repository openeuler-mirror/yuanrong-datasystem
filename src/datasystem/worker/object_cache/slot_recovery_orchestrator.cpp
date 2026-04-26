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
 * Description: Worker-side slot recovery orchestrator.
 */

#include "datasystem/worker/object_cache/slot_recovery_orchestrator.h"

#include <algorithm>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(cluster_name);

namespace datasystem {
namespace object_cache {

SlotRecoveryOrchestrator::SlotRecoveryOrchestrator(const std::string &rootPath) : rootPath_(rootPath)
{
}

Status SlotRecoveryOrchestrator::Init()
{
    slotClient_ = SlotClient::GetProcessSingleton(rootPath_);
    return slotClient_->Init();
}

Status SlotRecoveryOrchestrator::RepairLocalSlots()
{
    CHECK_FAIL_RETURN_STATUS(slotClient_ != nullptr, StatusCode::K_RUNTIME_ERROR, "slotClient_ is nullptr");
    const auto slotRoot = BuildSlotStoreRoot(rootPath_, FLAGS_cluster_name);
    if (!FileExist(slotRoot)) {
        LOG(INFO) << "Skip slot repair because local slot root does not exist, root=" << slotRoot;
        return Status::OK();
    }

    std::vector<std::string> slotDirs;
    RETURN_IF_NOT_OK(Glob(JoinPath(slotRoot, "slot_*"), slotDirs));
    std::sort(slotDirs.begin(), slotDirs.end());
    for (const auto &slotDir : slotDirs) {
        uint32_t slotId = 0;
        auto rc = ParseSlotId(BaseName(slotDir), slotId);
        if (rc.IsError()) {
            LOG(WARNING) << "Skip unexpected slot directory during startup repair, path=" << slotDir
                         << ", err=" << rc.ToString();
            continue;
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(slotClient_->RepairSlot(slotId),
                                         FormatString("Repair local slot failed, slotId=%u", slotId));
    }
    LOG(INFO) << "Finished local slot repair, repairedSlotCount=" << slotDirs.size() << ", root=" << slotRoot;
    return Status::OK();
}

Status SlotRecoveryOrchestrator::ParseSlotId(const std::string &slotDirName, uint32_t &slotId) const
{
    constexpr size_t prefixLen = 5;  // slot_
    CHECK_FAIL_RETURN_STATUS(slotDirName.rfind("slot_", 0) == 0 && slotDirName.size() > prefixLen,
                             StatusCode::K_INVALID, "Invalid slot directory name: " + slotDirName);
    try {
        size_t parsedChars = 0;
        slotId = static_cast<uint32_t>(std::stoul(slotDirName.substr(prefixLen), &parsedChars));
        CHECK_FAIL_RETURN_STATUS(parsedChars == slotDirName.size() - prefixLen, StatusCode::K_INVALID,
                                 "Invalid slot directory name: " + slotDirName);
    } catch (const std::exception &e) {
        RETURN_STATUS(StatusCode::K_INVALID,
                      std::string("Invalid slot directory name: ") + slotDirName + ", " + e.what());
    }
    return Status::OK();
}

std::string SlotRecoveryOrchestrator::BaseName(const std::string &path) const
{
    auto pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}
}  // namespace object_cache
}  // namespace datasystem
