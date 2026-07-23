/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Tests for object-cache ownership reconciliation planning.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_ownership_reconciliation.h"

#include <gtest/gtest.h>

namespace datasystem {
namespace object_cache {
namespace {

TEST(ObjectCacheOwnershipReconciliationTest, LocalIsolationUsesCommittedOwnersAndLocalWorker)
{
    ObjectCacheOwnershipReconciliationPlan plan;
    auto rc = BuildOwnershipReconciliationPlan(false, "master:1", "worker:1", { "worker:2", "worker:3" },
                                               OwnershipReconciliationKind::LOCAL_ISOLATION, plan);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(plan.metadataOwners, (std::set<std::string>{ "worker:1", "worker:2", "worker:3" }));
    EXPECT_EQ(plan.pendingEvidenceDetail, "local isolation ownership reconciliation pending");
    EXPECT_EQ(plan.emptyOwnersMessage, "No committed metadata owner is available for local-isolation handoff");
}

TEST(ObjectCacheOwnershipReconciliationTest, CentralizedNetworkRecoveryUsesMasterOnly)
{
    ObjectCacheOwnershipReconciliationPlan plan;
    auto rc = BuildOwnershipReconciliationPlan(true, "master:1", "worker:1", { "worker:2" },
                                               OwnershipReconciliationKind::NETWORK_RECOVERY, plan);

    ASSERT_TRUE(rc.IsOk());
    EXPECT_EQ(plan.metadataOwners, (std::set<std::string>{ "master:1" }));
    EXPECT_EQ(plan.pendingEvidenceDetail, "network recovery ownership reconciliation pending");
    EXPECT_EQ(plan.emptyOwnersMessage, "No committed metadata owner is available for network recovery");
}

TEST(ObjectCacheOwnershipReconciliationTest, EmptyDistributedOwnersReturnsNotReady)
{
    ObjectCacheOwnershipReconciliationPlan plan;
    auto rc = BuildOwnershipReconciliationPlan(false, "master:1", "", {}, OwnershipReconciliationKind::RESTART, plan);

    EXPECT_TRUE(rc.IsError());
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_EQ(plan.emptyOwnersMessage, "No committed metadata owner is available for restart reconciliation");
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
