/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache action adapter for Worker topology callbacks.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_TOPOLOGY_OBJECT_CACHE_ACTIONS_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_TOPOLOGY_OBJECT_CACHE_ACTIONS_H

#include <functional>

#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/runtime/worker_topology_phase_callbacks.h"

namespace datasystem::object_cache {

class WorkerTopologyObjectCacheActions final : public worker::IWorkerTopologyObjectCacheActions {
public:
    using ServiceProvider = std::function<WorkerOCServiceImpl *()>;

    explicit WorkerTopologyObjectCacheActions(ServiceProvider serviceProvider);
    ~WorkerTopologyObjectCacheActions() override = default;

    Status DrainScaleInData(const cluster::TopologyCallbackContext &context) override;

    Status PrepareScaleInCleanup(const cluster::TopologyCallbackContext &context,
                                 std::unique_ptr<cluster::TopologyPreparedCleanup> &prepared) override;

    Status CleanupLocalData(const cluster::TopologyCallbackContext &context) override;

private:
    WorkerOCServiceImpl *GetService() const;

    ServiceProvider serviceProvider_;
};

}  // namespace datasystem::object_cache

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_TOPOLOGY_OBJECT_CACHE_ACTIONS_H
