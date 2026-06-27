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
 * Description: Stable task action contract for scale task business actions.
 */
#ifndef DATASYSTEM_COMMON_TASK_ACTION_TASK_ACTION_H
#define DATASYSTEM_COMMON_TASK_ACTION_TASK_ACTION_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class TransferTaskType : uint8_t {
    MIGRATE_SCALE_UP_METADATA = 0,
    MIGRATE_VOLUNTARY_METADATA,
    RECOVER_PASSIVE_METADATA,
    RECOVER_PASSIVE_ASYNC_TASK,
    RECOVER_VOLUNTARY_ASYNC_TASK,
    CLEANUP_PASSIVE_RESIDUAL_METADATA,
    CLEANUP_PASSIVE_LOCAL_DATA_WITHOUT_META,
    CLEANUP_DEVICE_CLIENT_META,
    CHECK_VOLUNTARY_READY,
    MIGRATE_VOLUNTARY_DATA,
    CLEANUP_VOLUNTARY_SOURCE,
};

const char *TransferTaskTypeToString(TransferTaskType type);

struct PlacementScope {
    std::vector<Range> ranges;
    uint64_t scopeVersion{ 0 };
};

struct TransferTask {
    std::string taskId;
    TransferTaskType type{ TransferTaskType::MIGRATE_SCALE_UP_METADATA };
    int64_t topologyVersion{ 0 };

    std::string localWorkerAddr;
    std::string sourceWorkerAddr;
    std::string targetWorkerAddr;
    std::string failedWorkerAddr;
    std::string leavingWorkerAddr;

    PlacementScope placementScope;
    std::vector<std::string> removedWorkers;
};

using TaskActionFn = std::function<Status(const TransferTask &)>;

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TASK_ACTION_TASK_ACTION_H
