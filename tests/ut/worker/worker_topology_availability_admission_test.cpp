/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker topology availability to runtime-state admission tests.
 */
#include "datasystem/worker/runtime/worker_topology_availability_admission.h"

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
WorkerRecoveryEvidenceReport CompleteTopologyRecoveryReport()
{
    WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMembershipReady("membership ready")
        .MarkTopologyReady("topology ready")
        .MarkMetadataReady("metadata ready")
        .MarkSlotReady("slot ready")
        .MarkOwnershipReady("ownership ready")
        .MarkResourceReady("resource ready")
        .BuildReport("complete recovery evidence");
}

WorkerRecoveryEvidenceReport IncompleteObjectCacheRecoveryReport()
{
    WorkerRecoveryEvidenceBuilder builder;
    return builder.MarkMembershipReady("membership ready")
        .MarkTopologyReady("topology ready")
        .MarkMetadataReady("metadata ready")
        .MarkResourceReady("resource ready")
        .BuildReport("slot and ownership evidence missing");
}

TEST(WorkerTopologyAvailabilityAdmissionTest, TopologyAvailableOpensRuntimeWhenEvidenceCompletes)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &report);

    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.membershipReady);
    EXPECT_TRUE(snapshot.evidence.topologyReady);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);
    EXPECT_TRUE(snapshot.evidence.resourceReady);
    EXPECT_NE(snapshot.detail.find("ready=membership,topology,metadata,slot,ownership,resource"), std::string::npos);

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED, runtime, &report);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, TopologyAvailableWaitsForObjectCacheRecoveryEvidence)
{
    WorkerRuntimeFacade runtime;
    auto report = IncompleteObjectCacheRecoveryReport();

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &report);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_FALSE(snapshot.evidence.slotReady);
    EXPECT_FALSE(snapshot.evidence.ownershipReady);
    EXPECT_FALSE(ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel::NORMAL, snapshot));
}

TEST(WorkerTopologyAvailabilityAdmissionTest, TopologyAvailableWithoutEvidenceReportsMetadataPhase)
{
    WorkerRuntimeFacade runtime;

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::METADATA);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, RefreshTopologyAdmissionOpensAfterRecoveryEvidenceCompletes)
{
    WorkerRuntimeFacade runtime;
    auto incompleteReport = IncompleteObjectCacheRecoveryReport();
    EXPECT_FALSE(
        RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel::NORMAL, runtime, incompleteReport));

    auto completeReport = CompleteTopologyRecoveryReport();
    EXPECT_TRUE(
        RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel::NORMAL, runtime, completeReport));

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, TopologyRecoveryCanReopenLocalIsolationWithFreshEvidence)
{
    WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");

    auto completeReport = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &completeReport);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.metadataReady);
    EXPECT_TRUE(snapshot.evidence.slotReady);
    EXPECT_TRUE(snapshot.evidence.ownershipReady);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, StaleIncompleteTopologyRecoveryCannotDemoteRunningWorker)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryCompleteRecovery(CompleteTopologyRecoveryReport().evidence, "recovered"));

    auto incompleteReport = IncompleteObjectCacheRecoveryReport();
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &incompleteReport);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel::NORMAL, snapshot));
}

TEST(WorkerTopologyAvailabilityAdmissionTest, TopologyRecoveryCanReopenOutOfMemoryWithFreshEvidence)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryCompleteRecovery(CompleteTopologyRecoveryReport().evidence, "steady"));
    runtime.MarkOutOfMemory("allocation failed");

    auto completeReport = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &completeReport);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_TRUE(snapshot.evidence.resourceReady);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, TopologyCannotReopenOutOfMemoryWithoutAllocatorResourceEvidence)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryCompleteRecovery(CompleteTopologyRecoveryReport().evidence, "steady"));
    runtime.MarkOutOfMemory("allocation failed");
    auto report = CompleteTopologyRecoveryReport();
    report.evidence.resourceReady = false;

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &report);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::RESOURCE);
    EXPECT_FALSE(snapshot.evidence.resourceReady);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, LegacyServingGateRequiresRunningRuntimeState)
{
    WorkerRuntimeFacade runtime;
    auto report = CompleteTopologyRecoveryReport();
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &report);

    EXPECT_TRUE(ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel::NORMAL, runtime.GetSnapshot()));
    EXPECT_TRUE(ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED,
                                                   runtime.GetSnapshot()));
    EXPECT_FALSE(
        ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel::ROLE_ISOLATED, runtime.GetSnapshot()));
}

TEST(WorkerTopologyAvailabilityAdmissionTest, IsolatedAndNotReadyLevelsCloseRuntimeState)
{
    WorkerRuntimeFacade runtime;

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NOT_READY, runtime);
    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::JOINING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::STARTUP_NOT_READY);

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::ROLE_ISOLATED, runtime);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, HashRingSelfPassiveScaleDownDoesNotKillWorker)
{
    WorkerRuntimeFacade runtime;
    WorkerRecoveryEvidenceReport completeReport = CompleteTopologyRecoveryReport();

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &completeReport);
    ASSERT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RUNNING);

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::ROLE_ISOLATED, runtime,
                                            &completeReport);
    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN);
    EXPECT_NE(snapshot.mode, WorkerServiceMode::STOPPING);
    EXPECT_EQ(
        runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "legacy hash-ring passive scale-down").GetCode(),
        K_NOT_READY);

    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime, &completeReport);
    snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
}

TEST(WorkerTopologyAvailabilityAdmissionTest, ShuttingDownIsTerminal)
{
    WorkerRuntimeFacade runtime;
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::SHUTTING_DOWN, runtime);
    ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel::NORMAL, runtime);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::STOPPING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::PROCESS_STOPPING);
}
}  // namespace
}  // namespace datasystem::worker
