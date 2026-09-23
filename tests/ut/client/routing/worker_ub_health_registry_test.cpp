/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
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

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "tests/support/fake_ub_port_status_provider.h"

#include "datasystem/client/object_cache/routing/worker_ub_health_registry.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_bool(alsologtostderr);

namespace datasystem::ut {
namespace {
constexpr int WORKER_PORT_BASE = 20'000;
using FakeUbPortStatusProvider = ::datasystem::test::FakeUbPortStatusProvider;

ClusterTopologyPb BuildTopology(size_t activeWorkerCount)
{
    ClusterTopologyPb topology;
    for (size_t index = 0; index < activeWorkerCount; ++index) {
        const std::string endpoint = "127.0.0.1:" + std::to_string(WORKER_PORT_BASE + index);
        auto &member = (*topology.mutable_members())[endpoint];
        member.set_id("incarnation-" + std::to_string(index));
        member.set_state(MembershipPb::ACTIVE);
    }
    return topology;
}

UbHealthSummary BuildSummary(size_t workerIndex, std::optional<UbPortHealthSummary> portHealth,
                             uint64_t summaryEpoch = 1)
{
    UbHealthSummary summary;
    summary.worker = HostPort("127.0.0.1", WORKER_PORT_BASE + static_cast<int>(workerIndex));
    summary.incarnation = "incarnation-" + std::to_string(workerIndex);
    summary.epoch = summaryEpoch;
    summary.portHealth = std::move(portHealth);
    return summary;
}

UbPortHealthSummary KnownPortHealth(uint32_t total, uint32_t bad, uint64_t healthEpoch)
{
    return { true, total, bad, healthEpoch, false };
}
}  // namespace

TEST(WorkerUbHealthRegistryTest, PublishesOnlyObservedRoutableWorkersAndPrunesDepartedWorker)
{
    client::WorkerUbHealthRegistry registry;
    auto topology = BuildTopology(2);
    auto &leaving = (*topology.mutable_members())["127.0.0.1:29999"];
    leaving.set_id("leaving-incarnation");
    leaving.set_state(MembershipPb::LEAVING);

    registry.ReconcileTopology(topology);

    auto snapshot = registry.GetRoutingSnapshot();
    EXPECT_TRUE(snapshot->workers.empty());
    EXPECT_EQ(snapshot->workers.count(HostPort("127.0.0.1", 29999)), 0u);

    UbHealthSummary leavingSummary;
    leavingSummary.worker = HostPort("127.0.0.1", 29999);
    leavingSummary.incarnation = "leaving-incarnation";
    leavingSummary.epoch = 1;
    leavingSummary.portHealth = KnownPortHealth(4, 0, 1);
    ASSERT_TRUE(registry.ApplySummary(leavingSummary, leavingSummary.incarnation));
    EXPECT_EQ(registry.GetRoutingSnapshot()->workers.count(leavingSummary.worker), 1u);

    auto observed = BuildSummary(0, KnownPortHealth(4, 1, 1));
    ASSERT_TRUE(registry.ApplySummary(observed, observed.incarnation));
    snapshot = registry.GetRoutingSnapshot();
    ASSERT_EQ(snapshot->workers.size(), 2u);
    EXPECT_EQ(snapshot->workers.at(observed.worker).portHealth.badPortCount, 1u);
    auto publishedSnapshot = snapshot;

    topology.mutable_members()->erase(observed.worker.ToString());
    topology.mutable_members()->erase(leavingSummary.worker.ToString());
    registry.ReconcileTopology(topology);
    EXPECT_TRUE(registry.GetRoutingSnapshot()->workers.empty());
    EXPECT_EQ(publishedSnapshot->workers.count(observed.worker), 1u);
    EXPECT_FALSE(registry.GetSummary(observed.worker).has_value());
}

TEST(WorkerUbHealthRegistryTest, AcceptsMonitorSummaryFromInjectedPortFacts)
{
    auto provider = std::make_shared<FakeUbPortStatusProvider>(
        std::vector<UbPortStatus>{ { 2, UbPortState::BAD }, { 0, UbPortState::GOOD },
                                   { 1, UbPortState::BAD } });
    auto monitor = UbPortHealthMonitor::CreateForTest(provider, std::chrono::hours(1));
    ASSERT_TRUE(monitor->Start().IsOk());
    ASSERT_TRUE(monitor->EnsureFresh(std::chrono::hours(1)).IsOk());
    auto portHealth = monitor->GetSummary();
    ASSERT_TRUE(portHealth.has_value());
    monitor->Stop();

    client::WorkerUbHealthRegistry registry;
    registry.ReconcileTopology(BuildTopology(1));
    ASSERT_TRUE(registry.ApplySummary(BuildSummary(0, *portHealth), "incarnation-0"));

    auto snapshot = registry.GetRoutingSnapshot();
    const auto &health = snapshot->workers.at(HostPort("127.0.0.1", WORKER_PORT_BASE));
    EXPECT_TRUE(health.portHealth.valid);
    EXPECT_EQ(health.portHealth.totalPortCount, 3u);
    EXPECT_EQ(health.portHealth.badPortCount, 2u);
    EXPECT_EQ(health.portHealth.healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
}

TEST(WorkerUbHealthRegistryTest, FencesIdentityEpochAndDuplicateUpdates)
{
    client::WorkerUbHealthRegistry registry;
    registry.ReconcileTopology(BuildTopology(1));
    auto initial = BuildSummary(0, KnownPortHealth(4, 4, 3), 5);
    ASSERT_TRUE(registry.ApplySummary(initial, initial.incarnation));
    auto acceptedSnapshot = registry.GetRoutingSnapshot();

    EXPECT_FALSE(registry.ApplySummary(initial, initial.incarnation));
    EXPECT_EQ(registry.GetRoutingSnapshot(), acceptedSnapshot);

    auto staleSummary = initial;
    staleSummary.epoch = 4;
    staleSummary.portHealth = KnownPortHealth(4, 0, 2);
    EXPECT_FALSE(registry.ApplySummary(staleSummary, staleSummary.incarnation));
    EXPECT_EQ(registry.GetRoutingSnapshot(), acceptedSnapshot);

    auto wrongIdentity = initial;
    wrongIdentity.incarnation = "stale-incarnation";
    wrongIdentity.epoch = 6;
    EXPECT_FALSE(registry.ApplySummary(wrongIdentity, wrongIdentity.incarnation));
    EXPECT_EQ(registry.GetRoutingSnapshot(), acceptedSnapshot);
}

TEST(WorkerUbHealthRegistryTest, LogsOnlyRoutingHealthTransitionsWithSafeIncarnation)
{
    const bool oldAlsoLogToStderr = FLAGS_alsologtostderr;
    Raii restoreFlag([oldAlsoLogToStderr] { FLAGS_alsologtostderr = oldAlsoLogToStderr; });
    FLAGS_alsologtostderr = true;

    const std::array<char, 16> binaryIncarnationBytes{
        '\0', '\n', '\r', '\x1f', ' ', '\x7f', static_cast<char>(0x80), static_cast<char>(0xff),
        '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'
    };
    const std::string incarnation(binaryIncarnationBytes.data(), binaryIncarnationBytes.size());
    auto topology = BuildTopology(1);
    topology.mutable_members()->at("127.0.0.1:20000").set_id(incarnation);
    client::WorkerUbHealthRegistry registry;
    registry.ReconcileTopology(topology);

    auto partial = BuildSummary(0, KnownPortHealth(4, 1, 1));
    partial.incarnation = incarnation;
    testing::internal::CaptureStderr();
    ASSERT_TRUE(registry.ApplySummary(partial, incarnation));
    auto logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("UB_ROUTING_HEALTH action=updated"), std::string::npos) << logs;
    EXPECT_NE(logs.find("source=rpc_response"), std::string::npos) << logs;
    EXPECT_NE(logs.find("incarnation_prefix=" + FormatUbHealthIncarnationPrefix(incarnation)),
              std::string::npos) << logs;
    EXPECT_NE(logs.find("new_bad=1 new_total=4"), std::string::npos) << logs;
    EXPECT_NE(logs.find("writable=1 routing_visible=true"), std::string::npos) << logs;

    testing::internal::CaptureStderr();
    EXPECT_FALSE(registry.ApplySummary(partial, incarnation));
    logs = testing::internal::GetCapturedStderr();
    EXPECT_EQ(logs.find("UB_ROUTING_HEALTH action=updated"), std::string::npos) << logs;

    auto allDown = BuildSummary(0, KnownPortHealth(4, 4, 2), 2);
    allDown.incarnation = incarnation;
    testing::internal::CaptureStderr();
    ASSERT_TRUE(registry.ApplyVerifiedSummary(allDown, incarnation));
    logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("source=query_response"), std::string::npos) << logs;
    EXPECT_NE(logs.find("old_bad=1 old_total=4 new_valid=1 new_bad=4 new_total=4"), std::string::npos) << logs;
    EXPECT_NE(logs.find("old_writable=1 writable=0"), std::string::npos) << logs;
}

TEST(WorkerUbHealthRegistryTest, NewerPassiveRecoveryClearsVerifiedAdmission)
{
    client::WorkerUbHealthRegistry registry;
    registry.ReconcileTopology(BuildTopology(1));
    auto allDown = BuildSummary(0, KnownPortHealth(4, 4, 1));
    allDown.writable = false;

    ASSERT_TRUE(registry.ApplySummary(allDown, allDown.incarnation));
    EXPECT_FALSE(registry.IsVerifiedUnavailable(allDown.worker));
    ASSERT_TRUE(registry.ApplyVerifiedSummary(allDown, allDown.incarnation));
    EXPECT_TRUE(registry.IsVerifiedUnavailable(allDown.worker));

    bool recovered = false;
    auto pendingRecovery = BuildSummary(0, KnownPortHealth(4, 3, 2), 2);
    pendingRecovery.portHealth->verificationPending = true;
    const auto acceptRecovery = [](const UbHealthSummary &) { return true; };
    ASSERT_TRUE(registry.ApplySummary(pendingRecovery, pendingRecovery.incarnation, acceptRecovery, recovered));
    EXPECT_FALSE(recovered);
    EXPECT_TRUE(registry.IsVerifiedUnavailable(pendingRecovery.worker));

    auto anyUp = pendingRecovery;
    anyUp.portHealth->verificationPending = false;
    ASSERT_TRUE(registry.ApplySummary(anyUp, anyUp.incarnation, acceptRecovery, recovered));
    EXPECT_TRUE(recovered);
    EXPECT_FALSE(registry.IsVerifiedUnavailable(anyUp.worker));

    auto allDownAgain = BuildSummary(0, KnownPortHealth(4, 4, 3), 3);
    ASSERT_TRUE(registry.ApplySummary(allDownAgain, allDownAgain.incarnation, acceptRecovery, recovered));
    EXPECT_FALSE(recovered);
    EXPECT_FALSE(registry.IsVerifiedUnavailable(allDownAgain.worker));
}

TEST(WorkerUbHealthRegistryTest, RejectedPassiveRecoveryKeepsVerifiedAdmission)
{
    client::WorkerUbHealthRegistry registry;
    registry.ReconcileTopology(BuildTopology(1));
    auto allDown = BuildSummary(0, KnownPortHealth(4, 4, 1));
    allDown.writable = false;
    ASSERT_TRUE(registry.ApplyVerifiedSummary(allDown, allDown.incarnation));

    auto anyUp = BuildSummary(0, KnownPortHealth(4, 3, 2), 2);
    bool recovered = false;
    EXPECT_FALSE(registry.ApplySummary(anyUp, anyUp.incarnation,
                                      [](const UbHealthSummary &) { return false; }, recovered));

    EXPECT_FALSE(recovered);
    EXPECT_TRUE(registry.IsVerifiedUnavailable(anyUp.worker));
    auto retained = registry.GetSummary(anyUp.worker);
    ASSERT_TRUE(retained.has_value());
    ASSERT_TRUE(retained->portHealth.has_value());
    EXPECT_EQ(retained->portHealth->healthEpoch, 1u);
    EXPECT_EQ(retained->portHealth->badPortCount, 4u);

    ASSERT_TRUE(registry.ApplySummary(anyUp, anyUp.incarnation,
                                     [](const UbHealthSummary &) { return true; }, recovered));
    EXPECT_TRUE(recovered);
    EXPECT_FALSE(registry.IsVerifiedUnavailable(anyUp.worker));
}

TEST(WorkerUbHealthRegistryTest, StaleQueryCannotPromoteNewerPassiveFactToVerified)
{
    client::WorkerUbHealthRegistry registry;
    registry.ReconcileTopology(BuildTopology(1));
    auto observedAllDown = BuildSummary(0, KnownPortHealth(4, 4, 5), 5);
    ASSERT_TRUE(registry.ApplySummary(observedAllDown, observedAllDown.incarnation));
    auto staleQuery = BuildSummary(0, KnownPortHealth(4, 0, 4), 6);

    EXPECT_FALSE(registry.ApplyVerifiedSummary(staleQuery, staleQuery.incarnation));

    EXPECT_FALSE(registry.IsVerifiedUnavailable(observedAllDown.worker));
    auto retained = registry.GetSummary(observedAllDown.worker);
    ASSERT_TRUE(retained.has_value());
    ASSERT_TRUE(retained->portHealth.has_value());
    EXPECT_EQ(retained->portHealth->healthEpoch, 5u);
    EXPECT_EQ(retained->portHealth->badPortCount, 4u);
    ASSERT_TRUE(registry.ApplyVerifiedSummary(observedAllDown, observedAllDown.incarnation));
    EXPECT_TRUE(registry.IsVerifiedUnavailable(observedAllDown.worker));
}

TEST(WorkerUbHealthRegistryTest, UnchangedLargeTopologyKeepsPublishedSnapshot)
{
    client::WorkerUbHealthRegistry registry;
    auto topology = BuildTopology(1'024);
    registry.ReconcileTopology(topology);
    auto published = registry.GetRoutingSnapshot();

    registry.ReconcileTopology(topology);

    EXPECT_EQ(registry.GetRoutingSnapshot().get(), published.get());
}

}  // namespace datasystem::ut
