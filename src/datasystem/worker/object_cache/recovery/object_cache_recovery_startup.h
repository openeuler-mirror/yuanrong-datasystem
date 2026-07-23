/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache recovery startup hooks.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_STARTUP_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_STARTUP_H

namespace datasystem {
namespace worker {
class WorkerRuntimeFacade;
}  // namespace worker
namespace object_cache {
class ObjectCacheRecoveryState;

void MarkRestartReconciliationPending(worker::WorkerRuntimeFacade *runtime, ObjectCacheRecoveryState *recoveryState,
                                      bool isRestart, bool controlBackendAvailableAtStartup, bool enableReconciliation);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_STARTUP_H
