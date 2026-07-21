/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Metadata action adapter for Worker topology callbacks.
 */
#include "datasystem/worker/object_cache/worker_topology_metadata_actions.h"

#include "datasystem/common/log/log.h"
#include "datasystem/master/metadata_manager_holder.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"

namespace datasystem::object_cache {

WorkerTopologyMetadataActions::WorkerTopologyMetadataActions(bool centralizedMetadata, bool localMetadataMaster,
                                                             bool streamMetadataEnabled,
                                                             MetadataManagerHolder &metadataManagers)
    : centralizedMetadata_(centralizedMetadata),
      localMetadataMaster_(localMetadataMaster),
      streamMetadataEnabled_(streamMetadataEnabled),
      metadataManagers_(metadataManagers)
{
}

void WorkerTopologyMetadataActions::RecordStep(const std::string &step, const Status &status,
                                               Status &firstError) const
{
    if (status.IsOk()) {
        return;
    }
    LOG(ERROR) << "CLUSTER_FAILURE action=metadata_step step=" << step
               << " outcome=failed status=" << status.ToString();
    if (firstError.IsOk()) {
        firstError = status;
    }
}

Status WorkerTopologyMetadataActions::MigrateMetadata(const cluster::TopologyCallbackContext &context)
{
    if (centralizedMetadata_) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(master::OCMigrateMetadataManager::Instance().MigrateTopologyMetadata(
        context.action, context.keyFilter, context.businessOperationId, context.deadline, context.cancellation));
    if (!streamMetadataEnabled_) {
        return Status::OK();
    }
    return master::SCMigrateMetadataManager::Instance().MigrateTopologyMetadata(
        context.action, context.keyFilter, context.businessOperationId, context.deadline, context.cancellation);
}

Status WorkerTopologyMetadataActions::RecoverFailureMetadata(const cluster::TopologyCallbackContext &context)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadata;
    std::shared_ptr<master::SCMetadataManager> scMetadata;
    Status firstError;
    const bool localMetadata = !centralizedMetadata_ || localMetadataMaster_;
    if (localMetadata) {
        RecordStep("get-object-metadata", metadataManagers_.GetOcMetadataManager(ocMetadata), firstError);
    }
    if (localMetadata && streamMetadataEnabled_) {
        RecordStep("get-stream-metadata", metadataManagers_.GetScMetadataManager(scMetadata), firstError);
    }
    if (!centralizedMetadata_ && ocMetadata != nullptr) {
        RecordStep(
            "recover-object",
            ocMetadata->RecoverTopologyFailure(context.action, context.keyFilter, context.storageScanPlan,
                                               context.businessOperationId, context.deadline, context.cancellation),
            firstError);
    }
    if (!centralizedMetadata_ && streamMetadataEnabled_ && scMetadata != nullptr) {
        RecordStep(
            "recover-stream",
            scMetadata->RecoverTopologyFailure(context.action, context.keyFilter, context.businessOperationId,
                                               context.deadline, context.cancellation),
            firstError);
    }
    return firstError;
}

Status WorkerTopologyMetadataActions::CleanupFailureMetadata(const cluster::TopologyCallbackContext &context)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadata;
    std::shared_ptr<master::SCMetadataManager> scMetadata;
    Status firstError;
    const bool localMetadata = !centralizedMetadata_ || localMetadataMaster_;
    if (localMetadata) {
        RecordStep("get-object-metadata", metadataManagers_.GetOcMetadataManager(ocMetadata), firstError);
    }
    if (localMetadata && streamMetadataEnabled_) {
        RecordStep("get-stream-metadata", metadataManagers_.GetScMetadataManager(scMetadata), firstError);
    }
    if (streamMetadataEnabled_ && localMetadata && scMetadata != nullptr) {
        RecordStep("cleanup-stream",
                   scMetadata->CleanupTopologyFailedMember(context.action, context.businessOperationId,
                                                           context.deadline, context.cancellation),
                   firstError);
    }
    if (localMetadata && ocMetadata != nullptr) {
        RecordStep("cleanup-object",
                   ocMetadata->CleanupTopologyFailedMember(context.action, context.businessOperationId,
                                                           context.deadline, context.cancellation),
                   firstError);
    }
    return firstError;
}

Status WorkerTopologyMetadataActions::CleanupDeviceMetadata(const cluster::TopologyCallbackContext &context)
{
    if (centralizedMetadata_) {
        return Status::OK();
    }
    std::shared_ptr<master::OCMetadataManager> ocMetadata;
    RETURN_IF_NOT_OK(metadataManagers_.GetOcMetadataManager(ocMetadata));
    if (ocMetadata == nullptr) {
        return Status::OK();
    }
    return ocMetadata->CleanupTopologyDeviceClientMeta(context.action, context.businessOperationId, context.deadline,
                                                       context.cancellation);
}

}  // namespace datasystem::object_cache
