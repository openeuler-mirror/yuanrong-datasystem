/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Description: Worker UB fault -> Client isolation -> Worker recovery -> Client access restored cycle. */

#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "ut/common.h"
#define private public
#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#undef private
#include "datasystem/client/object_cache/routing/ub_health_filter.h"
#include "datasystem/client/object_cache/routing/worker_ub_health_registry.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem::client {
namespace {
const HostPort WORKER("127.0.0.1", 18485);
constexpr char INCARNATION[] = "worker-incarnation";
constexpr char NEXT_INCARNATION[] = "worker-incarnation-next";
const UbPortHealthSummary ALL_PORTS_BAD{ true, 4, 4, 1, false };
const UbPortHealthSummary ALL_PORTS_RECOVERED{ true, 4, 0, 2, false };

class FaultCycleDataPlaneManager final : public DataPlaneManager {
public:
    FaultCycleDataPlaneManager(UbHealthSummaryApplyHook passiveHook, UbHealthSummaryApplyHook verifiedHook,
                               std::function<void()> wakeHook)
        : DataPlaneManager(nullptr, 0, {}, nullptr, false, 1, nullptr, false, true, nullptr,
                           std::move(passiveHook), std::move(verifiedHook), std::move(wakeHook))
    {
    }

    ~FaultCycleDataPlaneManager() override { Shutdown(); }

    void RunAndWait()
    {
        RunDueUbPortHealthVerification();
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while ((ubPortHealthQueryPool_->GetWaitingTasksNum() != 0
                || ubPortHealthQueryPool_->GetRunningTasksNum() != 0)
               && std::chrono::steady_clock::now() < until) {
            std::this_thread::yield();
        }
        EXPECT_EQ(ubPortHealthQueryPool_->GetWaitingTasksNum(), 0u);
        EXPECT_EQ(ubPortHealthQueryPool_->GetRunningTasksNum(), 0u);
    }

    Status QueryUbPortHealth(const HostPort &workerAddr, const std::string &expectedIncarnation,
                             int32_t timeoutMs, UbHealthSummary &summary) override
    {
        (void)timeoutMs;
        ++queryCount;
        EXPECT_EQ(workerAddr, WORKER);
        EXPECT_EQ(expectedIncarnation, INCARNATION);
        summary.worker = workerAddr;
        summary.incarnation = expectedIncarnation;
        summary.portHealth = workerPortHealth;
        return Status::OK();
    }

    uint32_t queryCount = 0;
    UbPortHealthSummary workerPortHealth;
};

struct RecoveryHarness {
    std::shared_ptr<WorkerUbHealthRegistry> registry;
    std::unique_ptr<UbHealthFilter> filter;
    std::unique_ptr<FaultCycleDataPlaneManager> manager;
};

RecoveryHarness BuildHarness()
{
    RecoveryHarness harness;
    harness.registry = std::make_shared<WorkerUbHealthRegistry>();
    harness.filter = std::make_unique<UbHealthFilter>(harness.registry);
    UbHealthFilter &filter = *harness.filter;
    harness.manager = std::make_unique<FaultCycleDataPlaneManager>(
        [&filter](const UbHealthSummary &summary) { (void)filter.ObserveSummary(summary, summary.incarnation); },
        [&filter](const UbHealthSummary &summary) { (void)filter.ApplySummary(summary, summary.incarnation); },
        std::function<void()>{});
    FaultCycleDataPlaneManager &manager = *harness.manager;
    harness.filter->SetRemotePortHealthVerificationTrigger([&manager](const HostPort &worker) {
        (void)manager.RequestUbPortHealthVerification(worker);
    });
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 1;
    snapshot.remoteTransportAddrs.emplace_back(WORKER);
    snapshot.workerIncarnations.emplace(WORKER, INCARNATION);
    EXPECT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    return harness;
}

ClusterTopologyPb BuildRing(const std::string &incarnation)
{
    ClusterTopologyPb ring;
    auto &member = (*ring.mutable_members())[WORKER.ToString()];
    member.set_id(incarnation);
    member.set_state(MembershipPb::ACTIVE);
    return ring;
}

UbHealthSummary BuildSummary(const UbPortHealthSummary &portHealth)
{
    UbHealthSummary summary;
    summary.worker = WORKER;
    summary.incarnation = INCARNATION;
    summary.epoch = portHealth.healthEpoch;
    summary.portHealth = portHealth;
    return summary;
}

void IsolateWorkerViaWriteTargetFault(UbHealthFilter &filter, FaultCycleDataPlaneManager &manager)
{
    manager.workerPortHealth = ALL_PORTS_BAD;
    (void)filter.ReportWriteTargetFailure(WORKER, Status(K_URMA_ERROR, "remote ack timeout"), std::nullopt,
                                          URMA_REMOTE_ACK_TIMEOUT_STATUS);
    manager.RunAndWait();
    ASSERT_EQ(manager.queryCount, 1u);
    EXPECT_FALSE(filter.IsAvailable(WORKER));
    EXPECT_FALSE(filter.IsWriteTargetAvailable(WORKER));
}
}  // namespace

TEST(UbFaultRecoveryCycleTest, WorkerPortRecoveryRestoresClientAccess)
{
    auto harness = BuildHarness();
    auto &filter = *harness.filter;
    auto &manager = *harness.manager;
    const auto ring = BuildRing(INCARNATION);
    harness.registry->ReconcileTopology(ring);
    filter.ApplyTopologyIncarnations(ring);
    EXPECT_TRUE(filter.IsAvailable(WORKER));
    EXPECT_TRUE(filter.IsWriteTargetAvailable(WORKER));

    IsolateWorkerViaWriteTargetFault(filter, manager);
    auto deadline = manager.GetUbPortHealthQueryDeadline();
    ASSERT_TRUE(deadline.has_value());
    const auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
        *deadline - std::chrono::steady_clock::now());
    EXPECT_GT(delay.count(), 0);
    EXPECT_LE(delay, UB_REMOTE_PORT_HEALTH_QUERY_INTERVAL);

    manager.workerPortHealth = ALL_PORTS_RECOVERED;
    manager.ObserveUbHealthSummary(BuildSummary(ALL_PORTS_RECOVERED));
    EXPECT_FALSE(filter.IsAvailable(WORKER));

    manager.RunAndWait();
    EXPECT_EQ(manager.queryCount, 2u);
    EXPECT_TRUE(filter.IsAvailable(WORKER));
    EXPECT_TRUE(filter.IsWriteTargetAvailable(WORKER));
}

TEST(UbFaultRecoveryCycleTest, WorkerRestartIncarnationClearsIsolation)
{
    auto harness = BuildHarness();
    auto &filter = *harness.filter;
    auto &manager = *harness.manager;
    IsolateWorkerViaWriteTargetFault(filter, manager);

    const auto ring = BuildRing(NEXT_INCARNATION);
    harness.registry->ReconcileTopology(ring);
    filter.ApplyTopologyIncarnations(ring);
    EXPECT_TRUE(filter.IsAvailable(WORKER));
    EXPECT_TRUE(filter.IsWriteTargetAvailable(WORKER));
    EXPECT_EQ(manager.queryCount, 1u);
}

TEST(UbFaultRecoveryCycleTest, StalePortEpochRecoveryDoesNotRestoreAccess)
{
    auto harness = BuildHarness();
    auto &filter = *harness.filter;
    auto &manager = *harness.manager;
    IsolateWorkerViaWriteTargetFault(filter, manager);

    const UbPortHealthSummary staleRecovery{ true, 4, 3, ALL_PORTS_BAD.healthEpoch, false };
    manager.workerPortHealth = staleRecovery;
    manager.ObserveUbHealthSummary(BuildSummary(staleRecovery));
    manager.RunAndWait();
    EXPECT_EQ(manager.queryCount, 1u);
    EXPECT_FALSE(filter.IsAvailable(WORKER));
    EXPECT_FALSE(filter.IsWriteTargetAvailable(WORKER));
}

}  // namespace datasystem::client
