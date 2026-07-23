/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery startup hooks.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_startup.h"

#include "datasystem/worker/object_cache/recovery/object_cache_recovery_state.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem {
namespace object_cache {
void MarkRestartReconciliationPending(worker::WorkerRuntimeFacade *runtime, ObjectCacheRecoveryState *recoveryState,
                                      bool isRestart, bool controlBackendAvailableAtStartup, bool enableReconciliation)
{
    if (!isRestart || !controlBackendAvailableAtStartup || !enableReconciliation) {
        return;
    }
    worker::WorkerRecoveryEvidenceBuilder builder;
    if (recoveryState != nullptr) {
        recoveryState->SetMetadataRecoveryEvidenceReport(builder.BuildReport("restart reconciliation pending"));
    }
    if (runtime == nullptr) {
        return;
    }
    runtime->MarkRecovering(worker::WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                            "restart reconciliation pending", worker::WorkerRecoveryPhase::METADATA);
}
}  // namespace object_cache
}  // namespace datasystem
