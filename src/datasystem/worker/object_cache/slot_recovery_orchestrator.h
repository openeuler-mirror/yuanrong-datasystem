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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_ORCHESTRATOR_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_ORCHESTRATOR_H

#include <memory>
#include <string>

#include "datasystem/common/l2cache/slot_client/slot_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

/**
 * @brief Coordinate worker-local slot startup recovery and future takeover hooks.
 */
class SlotRecoveryOrchestrator {
public:
    /**
     * @brief Construct an orchestrator for one worker-scoped slot namespace.
     * @param[in] rootPath Distributed disk storage root path.
     */
    explicit SlotRecoveryOrchestrator(const std::string &rootPath);

    ~SlotRecoveryOrchestrator() = default;

    /**
     * @brief Initialize the underlying slot storage client.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Repair all existing local slots under the current worker namespace.
     * @return Status of the call.
     */
    Status RepairLocalSlots();

private:
    Status ParseSlotId(const std::string &slotDirName, uint32_t &slotId) const;
    std::string BaseName(const std::string &path) const;

    std::string rootPath_;
    std::unique_ptr<SlotClient> slotClient_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_ORCHESTRATOR_H
