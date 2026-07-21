/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Metadata action adapter for Worker topology callbacks.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_TOPOLOGY_METADATA_ACTIONS_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_TOPOLOGY_METADATA_ACTIONS_H

#include "datasystem/worker/runtime/worker_topology_phase_callbacks.h"

namespace datasystem {
class MetadataManagerHolder;
}  // namespace datasystem

namespace datasystem::object_cache {

class WorkerTopologyMetadataActions final : public worker::IWorkerTopologyMetadataActions {
public:
    WorkerTopologyMetadataActions(bool centralizedMetadata, bool localMetadataMaster, bool streamMetadataEnabled,
                                  MetadataManagerHolder &metadataManagers);
    ~WorkerTopologyMetadataActions() override = default;

    Status MigrateMetadata(const cluster::TopologyCallbackContext &context) override;

    Status RecoverFailureMetadata(const cluster::TopologyCallbackContext &context) override;

    Status CleanupFailureMetadata(const cluster::TopologyCallbackContext &context) override;

    Status CleanupDeviceMetadata(const cluster::TopologyCallbackContext &context) override;

private:
    void RecordStep(const std::string &step, const Status &status, Status &firstError) const;

    const bool centralizedMetadata_;
    const bool localMetadataMaster_;
    const bool streamMetadataEnabled_;
    MetadataManagerHolder &metadataManagers_;
};

}  // namespace datasystem::object_cache

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_TOPOLOGY_METADATA_ACTIONS_H
