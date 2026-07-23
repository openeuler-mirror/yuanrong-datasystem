/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker admission facade.
 */
#include "datasystem/worker/runtime/worker_admission_facade.h"

namespace datasystem::worker {
WorkerAdmissionFacade::WorkerAdmissionFacade(const WorkerRuntimeStateManager &runtimeState)
    : runtimeState_(runtimeState), admission_(runtimeState)
{
}

Status WorkerAdmissionFacade::CheckNormalRead(const std::string &operation) const
{
    return admission_.Check(WorkerAdmissionKind::NORMAL_READ, operation);
}

Status WorkerAdmissionFacade::CheckNormalWrite(const std::string &operation) const
{
    return admission_.Check(WorkerAdmissionKind::NORMAL_WRITE, operation);
}

Status WorkerAdmissionFacade::CheckMigrationTarget(const std::string &operation) const
{
    return admission_.Check(WorkerAdmissionKind::MIGRATION_TARGET, operation);
}

Status WorkerAdmissionFacade::CheckRecoveryRpc(const std::string &operation) const
{
    const auto snapshot = runtimeState_.GetSnapshot();
    if (snapshot.mode != WorkerServiceMode::RUNNING && snapshot.mode != WorkerServiceMode::RECOVERING) {
        return Status(K_NOT_READY, "Worker is not accepting " + operation
                                       + ", kind=RECOVERY_RPC, mode=" + std::string(ToString(snapshot.mode))
                                       + ", reason=" + std::string(ToString(snapshot.reason))
                                       + ", phase=" + std::string(ToString(snapshot.recoveryPhase))
                                       + ", rejection=MODE_NOT_SERVING");
    }
    return admission_.Check(snapshot, WorkerAdmissionKind::RECOVERY_RPC, operation);
}

Status WorkerAdmissionFacade::Check(WorkerAdmissionKind kind, const std::string &operation) const
{
    if (kind == WorkerAdmissionKind::RECOVERY_RPC) {
        return CheckRecoveryRpc(operation);
    }
    return admission_.Check(kind, operation);
}

Status WorkerAdmissionFacade::AcquireGuard(WorkerAdmissionKind kind, const std::string &operation,
                                           std::optional<WorkerRuntimeStateReadGuard> &guard) const
{
    auto candidate = runtimeState_.TryAcquireReadGuard();
    if (!candidate.has_value()) {
        const auto rc = admission_.Check(kind, operation);
        if (rc.GetCode() == K_OUT_OF_MEMORY) {
            return rc;
        }
        return Status(K_NOT_READY, "Worker is not accepting " + operation + ", kind=" + std::string(ToString(kind))
                                       + ", runtime transition pending");
    }
    if (kind == WorkerAdmissionKind::RECOVERY_RPC) {
        const auto &snapshot = candidate->GetSnapshot();
        if (snapshot.mode != WorkerServiceMode::RUNNING && snapshot.mode != WorkerServiceMode::RECOVERING) {
            return Status(K_NOT_READY, "Worker is not accepting " + operation
                                           + ", kind=RECOVERY_RPC, mode=" + std::string(ToString(snapshot.mode))
                                           + ", reason=" + std::string(ToString(snapshot.reason))
                                           + ", phase=" + std::string(ToString(snapshot.recoveryPhase))
                                           + ", rejection=MODE_NOT_SERVING");
        }
    }
    RETURN_IF_NOT_OK(admission_.Check(candidate->GetSnapshot(), kind, operation));
    guard.emplace(std::move(*candidate));
    return Status::OK();
}

Status WorkerAdmissionFacade::AcquireNormalReadGuard(const std::string &operation,
                                                     std::optional<WorkerRuntimeStateReadGuard> &guard) const
{
    return AcquireGuard(WorkerAdmissionKind::NORMAL_READ, operation, guard);
}

std::optional<WorkerRuntimeStateReadGuard> WorkerAdmissionFacade::TryAcquireNormalGuard(
    const std::string &operation) const
{
    std::optional<WorkerRuntimeStateReadGuard> guard;
    auto rc = AcquireGuard(WorkerAdmissionKind::NORMAL_WRITE, operation, guard);
    if (rc.IsError()) {
        return std::nullopt;
    }
    return guard;
}
}  // namespace datasystem::worker
