/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Object-cache action adapter for Worker topology callbacks.
 */
#include "datasystem/worker/object_cache/worker_topology_object_cache_actions.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

namespace datasystem::object_cache {

WorkerTopologyObjectCacheActions::WorkerTopologyObjectCacheActions(ServiceProvider serviceProvider)
    : serviceProvider_(std::move(serviceProvider))
{
}

WorkerOCServiceImpl *WorkerTopologyObjectCacheActions::GetService() const
{
    return serviceProvider_ == nullptr ? nullptr : serviceProvider_();
}

Status WorkerTopologyObjectCacheActions::DrainScaleInData(const cluster::TopologyCallbackContext &context)
{
    auto *service = GetService();
    CHECK_FAIL_RETURN_STATUS(service != nullptr, K_NOT_READY, "Worker object-cache service is not initialized");
    return service->DrainTopologyScaleInData(context.action, context.businessOperationId, context.deadline,
                                             context.cancellation);
}

Status WorkerTopologyObjectCacheActions::PrepareScaleInCleanup(
    const cluster::TopologyCallbackContext &context, std::unique_ptr<cluster::TopologyPreparedCleanup> &prepared)
{
    auto *service = GetService();
    CHECK_FAIL_RETURN_STATUS(service != nullptr, K_NOT_READY, "Worker object-cache service is not initialized");
    std::function<Status()> authorize;
    cluster::TopologyCleanupEffect apply;
    RETURN_IF_NOT_OK(service->PrepareTopologyScaleInCleanup(context.action, context.keyFilter,
                                                            context.businessOperationId, context.deadline,
                                                            context.cancellation, authorize, apply));
    prepared = std::make_unique<cluster::TopologyPreparedCleanup>(std::move(authorize), std::move(apply));
    return Status::OK();
}

Status WorkerTopologyObjectCacheActions::CleanupLocalData(const cluster::TopologyCallbackContext &context)
{
    auto *service = GetService();
    CHECK_FAIL_RETURN_STATUS(service != nullptr, K_NOT_READY, "Worker object-cache service is not initialized");
    return service->SubmitTopologyFailureCleanup(context.action, context.keyFilter, context.businessOperationId,
                                                 context.deadline, context.cancellation);
}

}  // namespace datasystem::object_cache
