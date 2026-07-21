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
 * Description: Worker recovery evidence controller tests.
 */
#include "datasystem/worker/runtime/worker_recovery_controller.h"

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

TEST(WorkerRecoveryControllerTest, IncompleteEvidenceKeepsWorkerRecovering)
{
    WorkerRuntimeStateManager state;
    WorkerRecoveryController recovery(state);
    auto evidence = CompleteEvidence();
    evidence.ownershipReady = false;

    EXPECT_FALSE(recovery.TryCompleteRecovery(evidence, "ownership unresolved"));

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE);
    EXPECT_FALSE(snapshot.evidence.ownershipReady);
    EXPECT_NE(snapshot.detail.find("missing=ownership"), std::string::npos);
}

TEST(WorkerRecoveryControllerTest, CompleteEvidenceMarksWorkerRunning)
{
    WorkerRuntimeStateManager state;
    WorkerRecoveryController recovery(state);

    EXPECT_TRUE(recovery.TryCompleteRecovery(CompleteEvidence(), "all evidence ready"));

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::NONE);
    EXPECT_EQ(snapshot.detail, "all evidence ready");
}

TEST(WorkerRecoveryControllerTest, StoppingWorkerCannotReopen)
{
    WorkerRuntimeStateManager state;
    WorkerRecoveryController recovery(state);
    state.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "shutdown");

    EXPECT_FALSE(recovery.TryCompleteRecovery(CompleteEvidence(), "late evidence"));

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::STOPPING);
    EXPECT_EQ(snapshot.detail, "shutdown");
}

TEST(WorkerRecoveryControllerTest, SecondDisconnectDuringRecoveryKeepsAdmissionClosed)
{
    WorkerRuntimeStateManager state;
    WorkerRecoveryController recovery(state);
    state.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "metadata recovery running",
                         WorkerRecoveryPhase::METADATA);
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                            "backend disconnected during metadata recovery");

    EXPECT_FALSE(recovery.TryCompleteRecovery(CompleteEvidence(), "late complete evidence"));

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION);
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::NONE);
    EXPECT_EQ(snapshot.detail, "backend disconnected during metadata recovery");
}

TEST(WorkerRecoveryEvidenceBuilderTest, AggregatesIndependentEvidenceWithMissingDetail)
{
    WorkerRecoveryEvidenceBuilder builder;
    builder.MarkMembershipReady("lease renewed").MarkTopologyReady("topology active").MarkResourceReady("resource ok");

    const auto report = builder.BuildReport("recovery checkpoint");

    EXPECT_TRUE(report.evidence.membershipReady);
    EXPECT_TRUE(report.evidence.topologyReady);
    EXPECT_FALSE(report.evidence.metadataReady);
    EXPECT_FALSE(report.evidence.slotReady);
    EXPECT_FALSE(report.evidence.ownershipReady);
    EXPECT_TRUE(report.evidence.resourceReady);
    EXPECT_NE(report.detail.find("recovery checkpoint"), std::string::npos);
    EXPECT_NE(report.detail.find("ready=membership,topology,resource"), std::string::npos);
    EXPECT_NE(report.detail.find("missing=metadata,slot,ownership"), std::string::npos);
    EXPECT_NE(report.detail.find("membership=lease renewed"), std::string::npos);
    EXPECT_NE(report.detail.find("topology=topology active"), std::string::npos);
}

TEST(WorkerRecoveryEvidenceBuilderTest, CompleteEvidenceDrivesControllerToRunning)
{
    WorkerRecoveryEvidenceBuilder builder;
    const auto report = builder.MarkMembershipReady("lease renewed")
                            .MarkTopologyReady("topology active")
                            .MarkMetadataReady("metadata reconciled")
                            .MarkSlotReady("slot recovered")
                            .MarkOwnershipReady("ownership confirmed")
                            .MarkResourceReady("resource ok")
                            .BuildReport("all adapters ready");
    WorkerRuntimeStateManager state;
    WorkerRecoveryController recovery(state);

    EXPECT_TRUE(recovery.TryCompleteRecovery(report.evidence, report.detail));

    const auto snapshot = state.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RUNNING);
    EXPECT_EQ(snapshot.detail, report.detail);
}

TEST(WorkerRecoveryEvidenceBuilderTest, RedactsObjectKeysFromEvidenceDetail)
{
    WorkerRecoveryEvidenceBuilder builder;
    const auto report = builder.MarkMetadataReady("object_key=tenant/private_object_001 recovered")
                            .BuildReport("objectKey=tenant/private_object_001 recovery checkpoint");

    EXPECT_EQ(report.detail.find("tenant/private_object_001"), std::string::npos);
    EXPECT_NE(report.detail.find("object_key=<redacted>"), std::string::npos);
    EXPECT_NE(report.detail.find("objectKey=<redacted>"), std::string::npos);
}
}  // namespace
}  // namespace datasystem::worker
