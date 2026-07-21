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
 * Description: Worker isolation coordinator tests.
 */
#include "datasystem/worker/runtime/worker_isolation_coordinator.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
TEST(WorkerIsolationCoordinatorTest, LocalIsolationClosesAdmissionAndKeepsProcessAlive)
{
    WorkerRuntimeFacade runtime;
    bool admissionOpen = true;
    bool localOwnershipReconciled = false;
    WorkerIsolationCoordinatorHooks hooks;
    hooks.setTopologyServingAdmission = [&](bool open) { admissionOpen = open; };
    hooks.reconcileLocalIsolationOwnership = [&] {
        localOwnershipReconciled = true;
        return Status::OK();
    };
    WorkerIsolationCoordinator coordinator(runtime, std::move(hooks));

    coordinator.OnLocalIsolation(Status(K_RUNTIME_ERROR, "keepalive timeout"));

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION);
    EXPECT_FALSE(admissionOpen);
    EXPECT_TRUE(localOwnershipReconciled);
}

TEST(WorkerIsolationCoordinatorTest, LocalRecoveryStartsRecoveringBeforeTopologyReconciliation)
{
    WorkerRuntimeFacade runtime;
    bool admissionOpen = true;
    bool membershipPublished = false;
    bool networkOwnershipReconciled = false;
    bool topologyReconciliationRequested = false;
    WorkerIsolationCoordinatorHooks hooks;
    hooks.setTopologyServingAdmission = [&](bool open) { admissionOpen = open; };
    hooks.isTopologyRuntimeReady = [] { return true; };
    hooks.publishReadyMembership = [&] {
        membershipPublished = true;
        return Status::OK();
    };
    hooks.reconcileNetworkRecoveryOwnership = [&] {
        networkOwnershipReconciled = true;
        return Status::OK();
    };
    hooks.requestRecoveryReconciliation = [&](const std::function<void()> &onReconciliationStarted) {
        topologyReconciliationRequested = true;
        auto snapshot = runtime.GetSnapshot();
        EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
        EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::MEMBERSHIP);
        EXPECT_FALSE(admissionOpen);
        onReconciliationStarted();
        return Status::OK();
    };
    WorkerIsolationCoordinator coordinator(runtime, std::move(hooks));

    coordinator.OnLocalRecovery();

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.reason, WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE);
    EXPECT_EQ(snapshot.recoveryPhase, WorkerRecoveryPhase::TOPOLOGY);
    EXPECT_FALSE(admissionOpen);
    EXPECT_TRUE(membershipPublished);
    EXPECT_TRUE(networkOwnershipReconciled);
    EXPECT_TRUE(topologyReconciliationRequested);
}
}  // namespace
}  // namespace datasystem::worker
