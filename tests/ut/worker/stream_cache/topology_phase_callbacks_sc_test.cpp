/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
#include <type_traits>

#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "gtest/gtest.h"

namespace datasystem::ut {
namespace {
using StreamMigration = Status (master::SCMigrateMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using StreamFailureRecovery = Status (master::SCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const cluster::IKeyFilter &, const std::string &,
    std::chrono::steady_clock::time_point, const cluster::CancellationToken &);
using StreamFailureCleanup = Status (master::SCMetadataManager::*)(
    const cluster::TopologyPhaseAction &, const std::string &, std::chrono::steady_clock::time_point,
    const cluster::CancellationToken &);

TEST(TopologyStreamBusinessContractTest, ExposesOnlyOpaqueTaskLevelBusinessEntryPoints)
{
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamMigration>(
                                    &master::SCMigrateMetadataManager::MigrateTopologyMetadata)),
                                StreamMigration>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamFailureRecovery>(
                                    &master::SCMetadataManager::RecoverTopologyFailure)),
                                StreamFailureRecovery>));
    EXPECT_TRUE((std::is_same_v<decltype(static_cast<StreamFailureCleanup>(
                                    &master::SCMetadataManager::CleanupTopologyFailedMember)),
                                StreamFailureCleanup>));
}
}  // namespace
}  // namespace datasystem::ut
