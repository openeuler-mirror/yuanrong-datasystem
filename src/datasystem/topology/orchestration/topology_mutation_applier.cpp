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
 * Description: Applies topology change plans to repository facts.
 */
#include "datasystem/topology/orchestration/topology_mutation_applier.h"

#include <vector>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

TopologyMutationApplier::TopologyMutationApplier(ITopologyControllerRepository &repository)
    : repository_(repository)
{
}

Status TopologyMutationApplier::Apply(const TopologyChangePlan &plan, TopologyReconcileResult &result)
{
    result.kind = plan.kind;
    if (plan.kind == TopologyChangeKind::NONE) {
        result.committedVersion = plan.expectedTopology.version;
        result.committedRevision = plan.expectedRevision;
        return Status::OK();
    }
    Revision revision = 0;
    if (plan.kind == TopologyChangeKind::FIRST_INIT) {
        RETURN_IF_NOT_OK(repository_.TryCreateCommittedTopology(plan.nextTopology, revision));
    } else {
        std::vector<TaskId> createdTransferTasks;
        std::vector<TaskId> createdRecoveryTasks;
        RETURN_IF_NOT_OK(CreateTasks(plan, result, createdTransferTasks, createdRecoveryTasks));
        auto rc = WriteNotifies(plan, result);
        if (rc.IsError()) {
            RollbackCreatedTasks(createdTransferTasks, createdRecoveryTasks);
            return rc;
        }
        rc = repository_.TryUpdateCommittedTopology(plan.expectedTopology, plan.expectedRevision,
                                                    plan.nextTopology, revision);
        if (rc.IsError()) {
            RollbackCreatedTasks(createdTransferTasks, createdRecoveryTasks);
            return rc;
        }
    }
    result.committedRevision = revision;
    result.committedVersion = plan.nextTopology.version;
    return Status::OK();
}

Status TopologyMutationApplier::CreateTasks(const TopologyChangePlan &plan, TopologyReconcileResult &result,
                                            std::vector<TaskId> &createdTransferTasks,
                                            std::vector<TaskId> &createdRecoveryTasks)
{
    Revision revision = 0;
    for (const auto &task : plan.transferTasks) {
        auto rc = repository_.TryCreateTransferTaskRecord(task, revision);
        CHECK_FAIL_RETURN_STATUS(rc.IsOk() || rc.GetCode() == K_TRY_AGAIN, rc.GetCode(), rc.GetMsg());
        if (rc.IsOk()) {
            createdTransferTasks.emplace_back(task.taskId);
            ++result.createdTransferTasks;
        }
    }
    for (const auto &task : plan.recoveryTasks) {
        auto rc = repository_.TryCreateRecoveryTaskRecord(task, revision);
        CHECK_FAIL_RETURN_STATUS(rc.IsOk() || rc.GetCode() == K_TRY_AGAIN, rc.GetCode(), rc.GetMsg());
        if (rc.IsOk()) {
            createdRecoveryTasks.emplace_back(task.taskId);
            ++result.createdRecoveryTasks;
        }
    }
    return Status::OK();
}

Status TopologyMutationApplier::WriteNotifies(const TopologyChangePlan &plan, TopologyReconcileResult &result)
{
    Revision revision = 0;
    for (const auto &notify : plan.notifies) {
        RETURN_IF_NOT_OK(repository_.UpsertTaskNotify(notify, revision));
        ++result.updatedNotifies;
    }
    return Status::OK();
}

void TopologyMutationApplier::RollbackCreatedTasks(const std::vector<TaskId> &createdTransferTasks,
                                                   const std::vector<TaskId> &createdRecoveryTasks)
{
    for (const auto &taskId : createdTransferTasks) {
        (void)repository_.DeleteTransferTaskRecord(taskId);
    }
    for (const auto &taskId : createdRecoveryTasks) {
        (void)repository_.DeleteRecoveryTaskRecord(taskId);
    }
}

}  // namespace topology
}  // namespace datasystem
