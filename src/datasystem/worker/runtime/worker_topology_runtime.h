/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Narrow worker topology runtime port.
 */
#ifndef DATASYSTEM_WORKER_RUNTIME_WORKER_TOPOLOGY_RUNTIME_H
#define DATASYSTEM_WORKER_RUNTIME_WORKER_TOPOLOGY_RUNTIME_H

#include <string>
#include <unordered_map>

#include "datasystem/cluster/runtime/topology_engine.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::worker {

class IWorkerTopologyRuntime {
public:
    virtual ~IWorkerTopologyRuntime() = default;

    virtual bool HasEstablishedMemberLease() const = 0;
    virtual Status MarkReady() = 0;
    virtual Status MarkExiting() = 0;
    virtual Status NotifyReconciliationDone() = 0;
    virtual Status GetRoutingHostIds(std::unordered_map<std::string, std::string> &hostIds) const = 0;
};

class WorkerTopologyRuntimeAdapter final : public IWorkerTopologyRuntime {
public:
    explicit WorkerTopologyRuntimeAdapter(cluster::TopologyEngine *engine);
    ~WorkerTopologyRuntimeAdapter() override = default;

    bool HasEstablishedMemberLease() const override;
    Status MarkReady() override;
    Status MarkExiting() override;
    Status NotifyReconciliationDone() override;
    Status GetRoutingHostIds(std::unordered_map<std::string, std::string> &hostIds) const override;

private:
    cluster::TopologyEngine *engine_;
};

}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_TOPOLOGY_RUNTIME_H
