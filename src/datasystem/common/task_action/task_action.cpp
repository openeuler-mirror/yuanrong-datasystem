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

#include "datasystem/common/task_action/task_action.h"

namespace datasystem {

const char *TransferTaskTypeToString(TransferTaskType type)
{
    switch (type) {
        case TransferTaskType::MIGRATE_SCALE_UP_METADATA:
            return "MIGRATE_SCALE_UP_METADATA";
        case TransferTaskType::MIGRATE_VOLUNTARY_METADATA:
            return "MIGRATE_VOLUNTARY_METADATA";
        case TransferTaskType::RECOVER_PASSIVE_METADATA:
            return "RECOVER_PASSIVE_METADATA";
        case TransferTaskType::RECOVER_PASSIVE_ASYNC_TASK:
            return "RECOVER_PASSIVE_ASYNC_TASK";
        case TransferTaskType::RECOVER_VOLUNTARY_ASYNC_TASK:
            return "RECOVER_VOLUNTARY_ASYNC_TASK";
        case TransferTaskType::CLEANUP_PASSIVE_RESIDUAL_METADATA:
            return "CLEANUP_PASSIVE_RESIDUAL_METADATA";
        case TransferTaskType::CLEANUP_PASSIVE_LOCAL_DATA_WITHOUT_META:
            return "CLEANUP_PASSIVE_LOCAL_DATA_WITHOUT_META";
        case TransferTaskType::CLEANUP_DEVICE_CLIENT_META:
            return "CLEANUP_DEVICE_CLIENT_META";
        case TransferTaskType::CHECK_VOLUNTARY_READY:
            return "CHECK_VOLUNTARY_READY";
        case TransferTaskType::MIGRATE_VOLUNTARY_DATA:
            return "MIGRATE_VOLUNTARY_DATA";
        case TransferTaskType::CLEANUP_VOLUNTARY_SOURCE:
            return "CLEANUP_VOLUNTARY_SOURCE";
    }
    return "UNKNOWN_TRANSFER_TASK";
}

}  // namespace datasystem
