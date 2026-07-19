/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology business callback contract tests.
 */
#include <type_traits>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/executor/storage_scan_plan.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/worker_topology_phase_callbacks.h"

#include "gtest/gtest.h"

namespace datasystem::ut {
namespace {

using MetadataMigration = Status (master::OCMigrateMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using StreamMigration = Status (master::SCMigrateMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using ObjectFailureRecovery = Status (master::OCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const cluster::StorageScanPlan &,
    const std::string &, std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using StreamFailureRecovery = Status (master::SCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using ScaleInData = Status (object_cache::WorkerOCServiceImpl::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);
using ObjectMetaScan = Status (master::ObjectMetaStore::*)(
    const cluster::StorageScanPlan &, const cluster::IKeyFilter &,
    const std::function<Status(const std::string &, const std::string &)> &);
using ObjectFailureCleanup = Status (master::OCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);
using StreamFailureCleanup = Status (master::SCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);
using ScaleInCleanup = Status (object_cache::WorkerOCServiceImpl::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &, std::function<Status()> &,
    cluster::TopologyCleanupEffect &);
using FailureDataCleanup = Status (object_cache::WorkerOcServiceClearDataFlow::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using ScaleInWorkerDataDrain = Status (worker::WorkerTopologyPhaseCallbacks::*)(
    const cluster::TopologyCallbackContext &);

TEST(TopologyBusinessContractTest, ExposesOnlyOpaqueTaskLevelBusinessEntryPoints)
{
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<MetadataMigration>(
                                    &master::OCMigrateMetadataManager::MigrateTopologyMetadata)),
                                MetadataMigration>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamMigration>(
                                    &master::SCMigrateMetadataManager::MigrateTopologyMetadata)),
                                StreamMigration>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ObjectFailureRecovery>(
                                    &master::OCMetadataManager::RecoverTopologyFailure)),
                                ObjectFailureRecovery>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamFailureRecovery>(
                                    &master::SCMetadataManager::RecoverTopologyFailure)),
                                StreamFailureRecovery>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ScaleInData>(
                                    &object_cache::WorkerOCServiceImpl::DrainTopologyScaleInData)),
                                ScaleInData>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ObjectMetaScan>(
                                    &master::ObjectMetaStore::ScanTopologyScope)),
                                ObjectMetaScan>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ObjectFailureCleanup>(
                                    &master::OCMetadataManager::CleanupTopologyFailedMember)),
                                ObjectFailureCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamFailureCleanup>(
                                    &master::SCMetadataManager::CleanupTopologyFailedMember)),
                                StreamFailureCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ScaleInCleanup>(
                                    &object_cache::WorkerOCServiceImpl::PrepareTopologyScaleInCleanup)),
                                ScaleInCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<FailureDataCleanup>(
                                    &object_cache::WorkerOcServiceClearDataFlow::SubmitTopologyFailureCleanup)),
                                FailureDataCleanup>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<ScaleInWorkerDataDrain>(
                                    &worker::WorkerTopologyPhaseCallbacks::OnScaleInDataDrain)),
                                ScaleInWorkerDataDrain>));
}

TEST(TopologyBusinessContractTest, RemoveMetaCarriesTopologyOperationIdentity)
{
    master::RemoveMetaReqPb request;
    request.set_topology_operation_id("scale-in-operation");
    std::string bytes;
    ASSERT_TRUE(request.SerializeToString(&bytes));

    master::RemoveMetaReqPb decoded;
    ASSERT_TRUE(decoded.ParseFromString(bytes));
    EXPECT_EQ(decoded.topology_operation_id(), "scale-in-operation");
    EXPECT_EQ(master::RemoveMetaReqPb::descriptor()->FindFieldByName("topology_operation_id")->number(), 8);
}

}  // namespace
}  // namespace datasystem::ut
