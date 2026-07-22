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
 * Description: Worker admission facade tests.
 */
#include "datasystem/worker/runtime/worker_admission_facade.h"

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

TEST(WorkerAdmissionFacadeTest, NormalGuardRejectsPendingTransition)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerAdmissionFacade facade(state);

    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local");

    EXPECT_FALSE(facade.TryAcquireNormalGuard("Put").has_value());
    EXPECT_FALSE(facade.CheckRecoveryRpc("RecoverMetadata").IsOk());
}

TEST(WorkerAdmissionFacadeTest, RecoveryRpcAllowedInRunningAndRecovering)
{
    WorkerRuntimeStateManager state;
    WorkerAdmissionFacade facade(state);

    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    EXPECT_TRUE(facade.CheckRecoveryRpc("PushMetaToWorker").IsOk());

    state.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "recovering");

    EXPECT_TRUE(facade.CheckRecoveryRpc("RecoverMetadata").IsOk());
    EXPECT_FALSE(facade.CheckNormalWrite("Put").IsOk());
}

TEST(WorkerAdmissionFacadeTest, NormalReadGuardUsesFacadeBoundary)
{
    WorkerRuntimeStateManager state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerAdmissionFacade facade(state);

    {
        std::optional<WorkerRuntimeStateReadGuard> guard;
        EXPECT_TRUE(facade.AcquireNormalReadGuard("GetObjectRemote", guard).IsOk());
        ASSERT_TRUE(guard.has_value());
        EXPECT_EQ(guard->GetSnapshot().mode, WorkerServiceMode::RUNNING);
    }

    std::optional<WorkerRuntimeStateReadGuard> blocked;
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local");
    auto rc = facade.AcquireNormalReadGuard("GetObjectRemote", blocked);
    EXPECT_FALSE(rc.IsOk());
    EXPECT_FALSE(blocked.has_value());
}
}  // namespace
}  // namespace datasystem::worker
