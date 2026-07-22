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
 * Description: Worker service admission tests.
 */
#include "datasystem/worker/runtime/worker_service_admission.h"

#include <future>
#include <optional>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/worker/metadata_route_resolver.h"
#include "gtest/gtest.h"

DS_DECLARE_string(log_dir);

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

void InitKvMetricsForWorkerAdmissionTest()
{
    FLAGS_log_dir = "/tmp";
    metrics::ResetKvMetricsForTest();
    ASSERT_TRUE(metrics::InitKvMetrics().IsOk());
}

TEST(WorkerServiceAdmissionTest, RejectsUntilRuntimeStateIsRunning)
{
    WorkerRuntimeStateManager state;
    WorkerServiceAdmission admission(state);

    auto rc = admission.CheckServing("GetObject");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("mode=STARTING"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("reason=STARTUP_NOT_READY"), std::string::npos);

    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    EXPECT_TRUE(admission.CheckServing("GetObject").IsOk());
}

TEST(WorkerServiceAdmissionTest, ExplainsLocalIsolationRejection)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerServiceAdmission admission(state);

    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive lost quorum");
    auto rc = admission.CheckServing("Publish");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("Publish"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("mode=LOCAL_ISOLATED"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("reason=CONTROL_BACKEND_LOCAL_ISOLATION"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("phase=NONE"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("rejection=MODE_NOT_SERVING"), std::string::npos);
    EXPECT_EQ(rc.GetMsg().find("keepalive lost quorum"), std::string::npos);
}

TEST(WorkerServiceAdmissionTest, RejectResponseUsesFixedDfxFieldsWithoutFreeFormDetail)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                            "objectKey=tenant/private-key payload=secret");
    WorkerServiceAdmission admission(state);

    const auto rc = admission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create");

    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("kind=NORMAL_WRITE"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("mode=LOCAL_ISOLATED"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("reason=CONTROL_BACKEND_LOCAL_ISOLATION"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("phase=NONE"), std::string::npos);
    EXPECT_EQ(rc.GetMsg().find("private-key"), std::string::npos);
    EXPECT_EQ(rc.GetMsg().find("secret"), std::string::npos);
}

TEST(WorkerServiceAdmissionTest, RejectPublishesFixedModeCounter)
{
    Raii restoreMetrics([] {
        FLAGS_log_dir = "/tmp";
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    InitKvMetricsForWorkerAdmissionTest();
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerServiceAdmission admission(state);
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());
    std::string allowedSummary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        allowedSummary += part;
    }
    EXPECT_EQ(allowedSummary.find("\"name\":\"worker_admission_reject_latency\""), std::string::npos);

    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    EXPECT_EQ(admission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create").GetCode(), K_NOT_READY);

    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    EXPECT_NE(summary.find("{\"name\":\"worker_admission_reject_local_isolated_total\",\"total\":1,\"delta\":1}"),
              std::string::npos);
    EXPECT_NE(summary.find("\"name\":\"worker_admission_reject_latency\""), std::string::npos);
}

TEST(WorkerServiceAdmissionTest, RejectsOrdinaryTrafficWhileRuntimeTransitionIsPending)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    auto readGuard = state.TryAcquireReadGuard();
    ASSERT_TRUE(readGuard.has_value());
    WorkerServiceAdmission admission(state);

    auto isolation = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    });
    ASSERT_EQ(isolation.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    auto rc = admission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("phase=COMPLETE"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("rejection=TRANSITION_PENDING"), std::string::npos);
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::RECOVERY_RPC, "Recover").IsOk());

    readGuard.reset();
    EXPECT_EQ(isolation.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}

TEST(WorkerServiceAdmissionTest, AppliesServiceModeMatrix)
{
    struct Case {
        WorkerServiceMode mode;
        WorkerAdmissionKind kind;
        bool allowed;
        StatusCode expectedCode;
    };
    const std::vector<Case> cases = {
        { WorkerServiceMode::RUNNING, WorkerAdmissionKind::NORMAL_READ, true, K_OK },
        { WorkerServiceMode::RUNNING, WorkerAdmissionKind::NORMAL_WRITE, true, K_OK },
        { WorkerServiceMode::RUNNING, WorkerAdmissionKind::MIGRATION_TARGET, true, K_OK },
        { WorkerServiceMode::RUNNING, WorkerAdmissionKind::RECOVERY_RPC, true, K_OK },
        { WorkerServiceMode::STARTING, WorkerAdmissionKind::NORMAL_READ, false, K_NOT_READY },
        { WorkerServiceMode::STARTING, WorkerAdmissionKind::RECOVERY_RPC, true, K_OK },
        { WorkerServiceMode::JOINING, WorkerAdmissionKind::NORMAL_WRITE, false, K_NOT_READY },
        { WorkerServiceMode::JOINING, WorkerAdmissionKind::INTERNAL_JOINING_RPC, true, K_OK },
        { WorkerServiceMode::JOINING, WorkerAdmissionKind::RESOURCE_RECOVERY_RPC, false, K_NOT_READY },
        { WorkerServiceMode::LOCAL_ISOLATED, WorkerAdmissionKind::NORMAL_READ, false, K_NOT_READY },
        { WorkerServiceMode::LOCAL_ISOLATED, WorkerAdmissionKind::CLEANUP_RPC, true, K_OK },
        { WorkerServiceMode::LOCAL_ISOLATED, WorkerAdmissionKind::RESOURCE_RECOVERY_RPC, false, K_NOT_READY },
        { WorkerServiceMode::RECOVERING, WorkerAdmissionKind::MIGRATION_TARGET, false, K_NOT_READY },
        { WorkerServiceMode::RECOVERING, WorkerAdmissionKind::RECOVERY_RPC, true, K_OK },
        { WorkerServiceMode::RECOVERING, WorkerAdmissionKind::CLEANUP_RPC, true, K_OK },
        { WorkerServiceMode::RECOVERING, WorkerAdmissionKind::INTERNAL_JOINING_RPC, true, K_OK },
        { WorkerServiceMode::RECOVERING, WorkerAdmissionKind::RESOURCE_RECOVERY_RPC, true, K_OK },
        { WorkerServiceMode::DRAINING, WorkerAdmissionKind::NORMAL_READ, true, K_OK },
        { WorkerServiceMode::DRAINING, WorkerAdmissionKind::RESOURCE_RECOVERY_RPC, true, K_OK },
        { WorkerServiceMode::DRAINING, WorkerAdmissionKind::MIGRATION_TARGET, false, K_NOT_READY },
        { WorkerServiceMode::DRAINING, WorkerAdmissionKind::NORMAL_WRITE, false, K_NOT_READY },
        { WorkerServiceMode::DRAINING, WorkerAdmissionKind::CLEANUP_RPC, true, K_OK },
        { WorkerServiceMode::OUT_OF_MEMORY, WorkerAdmissionKind::NORMAL_READ, true, K_OK },
        { WorkerServiceMode::OUT_OF_MEMORY, WorkerAdmissionKind::NORMAL_WRITE, false, K_OUT_OF_MEMORY },
        { WorkerServiceMode::OUT_OF_MEMORY, WorkerAdmissionKind::MIGRATION_TARGET, false, K_OUT_OF_MEMORY },
        { WorkerServiceMode::OUT_OF_MEMORY, WorkerAdmissionKind::INTERNAL_JOINING_RPC, true, K_OK },
        { WorkerServiceMode::OUT_OF_MEMORY, WorkerAdmissionKind::RESOURCE_RECOVERY_RPC, true, K_OK },
        { WorkerServiceMode::OUT_OF_MEMORY, WorkerAdmissionKind::CLEANUP_RPC, true, K_OK },
        { WorkerServiceMode::STOPPING, WorkerAdmissionKind::NORMAL_READ, false, K_NOT_READY },
        { WorkerServiceMode::STOPPING, WorkerAdmissionKind::CLEANUP_RPC, true, K_OK },
    };

    for (const auto &item : cases) {
        WorkerRuntimeStateManager state;
        switch (item.mode) {
            case WorkerServiceMode::RUNNING:
                ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
                break;
            case WorkerServiceMode::JOINING:
                state.MarkJoining("joining");
                break;
            case WorkerServiceMode::LOCAL_ISOLATED:
                state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "isolated");
                break;
            case WorkerServiceMode::RECOVERING:
                state.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "recovering");
                break;
            case WorkerServiceMode::DRAINING:
                ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
                state.MarkDraining("draining");
                break;
            case WorkerServiceMode::OUT_OF_MEMORY:
                ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
                state.MarkOutOfMemory("oom");
                break;
            case WorkerServiceMode::STOPPING:
                state.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "stopping");
                break;
            case WorkerServiceMode::STARTING:
                break;
        }
        const WorkerServiceAdmission admission(state);
        auto rc = admission.Check(item.kind, "matrix-op");
        EXPECT_EQ(rc.IsOk(), item.allowed) << ToString(item.mode);
        EXPECT_EQ(rc.GetCode(), item.expectedCode) << ToString(item.mode);
    }
}

TEST(WorkerServiceAdmissionTest, ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindow)
{
    WorkerRuntimeStateManager scaleInSource;
    ASSERT_TRUE(scaleInSource.TryMarkRunning(CompleteEvidence(), "ready"));
    scaleInSource.MarkDraining("voluntary scale-down drain started");
    const WorkerServiceAdmission scaleInAdmission(scaleInSource);

    auto rc = scaleInAdmission.Check(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("mode=DRAINING"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("kind=MIGRATION_TARGET"), std::string::npos);
    EXPECT_TRUE(scaleInAdmission.Check(WorkerAdmissionKind::NORMAL_READ, "GetObjectRemote").IsOk());

    WorkerRuntimeStateManager isolatedPeer;
    ASSERT_TRUE(isolatedPeer.TryMarkRunning(CompleteEvidence(), "ready"));
    isolatedPeer.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "peer keepalive failed");
    const WorkerServiceAdmission isolatedAdmission(isolatedPeer);

    rc = isolatedAdmission.Check(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("mode=LOCAL_ISOLATED"), std::string::npos);
    EXPECT_TRUE(isolatedAdmission.Check(WorkerAdmissionKind::CLEANUP_RPC, "CleanupFailedMigration").IsOk());

    isolatedPeer.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                "topology available; waiting for metadata evidence");
    rc = isolatedAdmission.Check(WorkerAdmissionKind::MIGRATION_TARGET, "MigrateData");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("mode=RECOVERING"), std::string::npos);
    EXPECT_TRUE(isolatedAdmission.Check(WorkerAdmissionKind::RECOVERY_RPC, "RecoverMetadata").IsOk());
}

TEST(WorkerServiceAdmissionTest, StableRecoveringModeAllowsCleanupRpc)
{
    WorkerRuntimeStateManager state;
    WorkerServiceAdmission admission(state);
    WorkerRuntimeStateSnapshot snapshot;
    snapshot.mode = WorkerServiceMode::RECOVERING;
    snapshot.reason = WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE;
    snapshot.recoveryPhase = WorkerRecoveryPhase::TOPOLOGY;

    EXPECT_TRUE(admission.Check(snapshot, WorkerAdmissionKind::RECOVERY_RPC, "Recover").IsOk());
    EXPECT_TRUE(admission.Check(snapshot, WorkerAdmissionKind::CLEANUP_RPC, "DeleteNotification").IsOk());
    EXPECT_TRUE(admission.Check(snapshot, WorkerAdmissionKind::DIAGNOSTIC_RPC, "Inspect").IsOk());
    EXPECT_TRUE(admission.Check(snapshot, WorkerAdmissionKind::RESOURCE_RECOVERY_RPC, "ReleaseResource").IsOk());
    EXPECT_EQ(admission.Check(snapshot, WorkerAdmissionKind::NORMAL_READ, "Get").GetCode(), K_NOT_READY);
}

}  // namespace
}  // namespace datasystem::worker
