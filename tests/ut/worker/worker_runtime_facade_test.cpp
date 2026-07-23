/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade tests.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
WorkerRunningEvidence CompleteEvidence()
{
    WorkerRunningEvidence evidence;
    evidence.membershipReady = true;
    evidence.topologyReady = true;
    evidence.metadataReady = true;
    evidence.slotReady = true;
    evidence.ownershipReady = true;
    evidence.resourceReady = true;
    return evidence;
}

TEST(WorkerRuntimeFacadeTest, CentralizesAdmissionRecoveryAndScaleDownTerminalState)
{
    WorkerRuntimeFacade runtime;

    EXPECT_TRUE(runtime.TryCompleteRecovery(CompleteEvidence(), "startup evidence ready"));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());

    runtime.MarkDraining("voluntary scale-down drain started");

    EXPECT_FALSE(runtime.TryCompleteRecovery(CompleteEvidence(), "late failure recovery evidence"));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Set").GetCode(), K_NOT_READY);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData").GetCode(), K_NOT_READY);
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::DRAINING);
}

TEST(WorkerRuntimeFacadeTest, AppliesTopologyAvailabilityWithObjectCacheEvidence)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");

    WorkerRecoveryEvidenceBuilder pendingBuilder;
    const auto pending = pendingBuilder.MarkMembershipReady().MarkTopologyReady().BuildReport("metadata pending");
    EXPECT_TRUE(runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, pending));

    WorkerRecoveryEvidenceBuilder completeBuilder;
    const auto complete =
        completeBuilder.MarkMetadataReady().MarkSlotReady().MarkOwnershipReady().MarkResourceReady().BuildReport(
            "object-cache recovery complete");
    EXPECT_TRUE(runtime.ApplyTopologyAvailability(cluster::TopologyAvailabilityLevel::NORMAL, &complete));
    EXPECT_FALSE(
        runtime.ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel::NORMAL, complete));
    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RUNNING);
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());
}
}  // namespace
}  // namespace datasystem::worker
