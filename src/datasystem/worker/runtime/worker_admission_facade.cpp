/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker admission facade.
 */
#include "datasystem/worker/runtime/worker_admission_facade.h"

namespace datasystem::worker {
WorkerAdmissionFacade::WorkerAdmissionFacade(const WorkerRuntimeState &runtimeState)
    : runtimeState_(runtimeState), admission_(runtimeState)
{
}

Status WorkerAdmissionFacade::Check(WorkerAdmissionKind kind, const std::string &operation) const
{
    return admission_.Check(kind, operation);
}

Status WorkerAdmissionFacade::AcquireGuard(WorkerAdmissionKind kind, const std::string &operation,
                                           std::optional<WorkerRuntimeStateReadGuard> &guard) const
{
    guard.reset();
    auto readGuard = runtimeState_.TryAcquireReadGuard();
    if (!readGuard.has_value()) {
        return admission_.Check(kind, operation);
    }
    auto rc = admission_.Check(readGuard->GetSnapshot(), kind, operation);
    if (rc.IsOk()) {
        guard = std::move(readGuard);
    }
    return rc;
}
}  // namespace datasystem::worker
