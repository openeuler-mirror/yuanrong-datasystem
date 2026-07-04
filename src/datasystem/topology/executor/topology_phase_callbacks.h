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
 * Description: Topology task phase callback contract.
 */
#ifndef DATASYSTEM_TOPOLOGY_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H
#define DATASYSTEM_TOPOLOGY_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H

#include <vector>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

class ITopologyPhaseCallbacks {
public:
    virtual ~ITopologyPhaseCallbacks() = default;

    /**
     * @brief Execute one transfer task and return finished range progress.
     * @param[in] task Transfer task fact.
     * @param[out] progress Finished range progress to report.
     * @return K_OK on success; K_TRY_AGAIN for retryable failure; other errors block the task.
     */
    virtual Status ExecuteTransferTask(const TransferTaskRecord &task,
                                       std::vector<TaskProgressUpdate> &progress) = 0;

    /**
     * @brief Execute one recovery task and return finished range progress.
     * @param[in] task Recovery task fact.
     * @param[out] progress Finished range progress to report.
     * @return K_OK on success; K_TRY_AGAIN for retryable failure; other errors block the task.
     */
    virtual Status ExecuteRecoveryTask(const RecoveryTaskRecord &task,
                                       std::vector<TaskProgressUpdate> &progress) = 0;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H
