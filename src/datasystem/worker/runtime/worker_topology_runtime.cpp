/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Narrow worker topology runtime port.
 */
#include "datasystem/worker/runtime/worker_topology_runtime.h"

#include "datasystem/cluster/runtime/topology_engine.h"

namespace datasystem::worker {

WorkerTopologyRuntimeAdapter::WorkerTopologyRuntimeAdapter(cluster::TopologyEngine *engine) : engine_(engine)
{
}

bool WorkerTopologyRuntimeAdapter::HasEstablishedMemberLease() const
{
    return engine_ != nullptr && engine_->HasEstablishedMemberLease();
}

Status WorkerTopologyRuntimeAdapter::MarkReady()
{
    CHECK_FAIL_RETURN_STATUS(engine_ != nullptr, K_NOT_READY, "topology runtime is not initialized");
    return engine_->MarkReady();
}

Status WorkerTopologyRuntimeAdapter::MarkExiting()
{
    CHECK_FAIL_RETURN_STATUS(engine_ != nullptr, K_NOT_READY, "topology runtime is not initialized");
    return engine_->MarkExiting();
}

Status WorkerTopologyRuntimeAdapter::NotifyReconciliationDone()
{
    CHECK_FAIL_RETURN_STATUS(engine_ != nullptr, K_NOT_READY, "topology runtime is not initialized");
    return engine_->NotifyReconciliationDone();
}

Status WorkerTopologyRuntimeAdapter::GetRoutingHostIds(std::unordered_map<std::string, std::string> &hostIds) const
{
    CHECK_FAIL_RETURN_STATUS(engine_ != nullptr, K_NOT_READY, "topology runtime is not initialized");
    return engine_->GetRoutingHostIds(hostIds);
}

}  // namespace datasystem::worker
