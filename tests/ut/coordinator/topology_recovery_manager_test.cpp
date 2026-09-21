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

/**
 * Description: Unit tests for Coordinator cluster topology recovery arbitration.
 */
#include "datasystem/coordinator/topology_recovery_manager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/raii.h"
#include "ut/common.h"

namespace datasystem::coordinator {
namespace {
constexpr char COORDINATOR_ID[] = "coordinator-id-1";
constexpr char MEMBER_A[] = "127.0.0.1:12001";
constexpr char MEMBER_B[] = "127.0.0.1:12002";
constexpr char MEMBER_C[] = "127.0.0.1:12003";
constexpr uint64_t TOPOLOGY_VERSION = 42;
constexpr uint64_t DISCOVERY_WINDOW_MS = 100;
constexpr uint64_t FAST_RECOVERY_WINDOW_MS = 3'000;
constexpr auto VALIDATION_TIMEOUT = std::chrono::seconds(1);
constexpr auto ROUND_TIMEOUT = std::chrono::seconds(10);
constexpr auto TEST_DEADLINE = std::chrono::seconds(2);
constexpr auto POLL_INTERVAL = std::chrono::milliseconds(1);
constexpr auto SHUTDOWN_OBSERVATION = std::chrono::milliseconds(100);
constexpr auto DELAYED_RECONCILE_WORKER_WINDOW = std::chrono::milliseconds(500);
constexpr auto DELAYED_RECONCILE_VALIDATION_TIMEOUT = std::chrono::milliseconds(50);
constexpr auto DELAYED_RECONCILE_WORKER_SETTLE = std::chrono::milliseconds(60);
constexpr auto HARD_DEADLINE_TIMEOUT = std::chrono::milliseconds(200);
constexpr auto REAL_CLOCK_HARD_DEADLINE_TIMEOUT = std::chrono::milliseconds(50);
constexpr size_t DELAYED_RECONCILE_WORKER_COUNT = 2;
constexpr uint64_t TEST_DOWNSTREAM_PHASE_US = 123;

class NoopWatchDispatcher final : public WatchDispatcher {
public:
    explicit NoopWatchDispatcher(WatchRegistry *registry) : WatchDispatcher(registry)
    {
    }

    ~NoopWatchDispatcher() override = default;

    Status DoNotify(int64_t, const std::string &, std::vector<std::shared_ptr<WatchEvent>> &) override
    {
        return Status::OK();
    }
};

class TopologyRecoveryManagerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        memoryStore_ = std::make_shared<MemoryKvStore>();
        registry_ = std::make_shared<WatchRegistry>();
        dispatcher_ = std::make_shared<NoopWatchDispatcher>(registry_.get());
        clock_ = std::make_shared<SteadyClockMock>();
        ttlManager_ = std::make_shared<TtlManager>(clock_);
        store_ = std::make_unique<CoordinatorStore>(memoryStore_, registry_, dispatcher_, ttlManager_);
        ASSERT_TRUE(store_->Start().IsOk());
        options_.discoveryWindow = std::chrono::milliseconds(DISCOVERY_WINDOW_MS);
        options_.validationWaitTimeout = VALIDATION_TIMEOUT;
        manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
        manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    }

    void TearDown() override
    {
        manager_.reset();
        store_.reset();
    }

    std::string MembershipKey(const std::string &clusterName, const std::string &address)
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        return keys->MembershipTable() + "/" + address;
    }

    std::string TopologyKey(const std::string &clusterName)
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        return keys->TopologyTable() + "/";
    }

    TopologyRecoveryCandidateReport SnapshotEvidence(const std::string &address, uint64_t version,
                                                     char identityByte)
    {
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = version;
        std::vector<uint32_t> tokens;
        for (uint32_t index = 0; index < 4; ++index) {
            tokens.emplace_back(cluster::HashAlgorithm::MakeToken(address, index, 0));
        }
        topology.members = { cluster::Member{ { std::string(16, identityByte), address },
                                               cluster::MemberState::ACTIVE, std::move(tokens) } };
        TopologyRecoveryCandidateReport report;
        report.reporterAddress = address;
        report.hasSnapshot = true;
        report.topologyVersion = version;
        EXPECT_TRUE(cluster::TopologyRepositoryCodec::EncodeTopology(topology, report.canonicalTopology).IsOk());
        Hasher hasher;
        EXPECT_TRUE(hasher.GetSha256Hex(report.canonicalTopology, report.canonicalDigest).IsOk());
        return report;
    }

    void ObserveMember(const std::string &clusterName, const std::string &address,
                       cluster::MemberLifecycleState state = cluster::MemberLifecycleState::STARTING)
    {
        manager_->ObserveMembershipChange(MembershipKey(clusterName, address), state);
    }

    void ReportEvidence(const std::string &clusterName, TopologyRecoveryCandidateReport report,
                        TopologyRecoveryReportDecision &decision)
    {
        report.canonicalTopology.clear();
        DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, std::move(report), decision));
    }

    bool DriveUntil(const std::string &clusterName, const std::string &address, TopologyRecoveryState expected)
    {
        const auto deadline = std::chrono::steady_clock::now() + TEST_DEADLINE;
        while (std::chrono::steady_clock::now() < deadline) {
            manager_->NotifyMembershipActivity(MembershipKey(clusterName, address));
            if (manager_->GetState(clusterName) == expected) {
                return true;
            }
            std::this_thread::sleep_for(POLL_INTERVAL);
        }
        return false;
    }

    bool WaitUntil(const std::function<bool()> &predicate) const
    {
        const auto deadline = std::chrono::steady_clock::now() + TEST_DEADLINE;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(POLL_INTERVAL);
        }
        return false;
    }

    TopologyRecoveryOptions options_;
    std::shared_ptr<MemoryKvStore> memoryStore_;
    std::shared_ptr<WatchRegistry> registry_;
    std::shared_ptr<NoopWatchDispatcher> dispatcher_;
    std::shared_ptr<SteadyClockMock> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> store_;
    std::unique_ptr<TopologyRecoveryManager> manager_;
};

TEST_F(TopologyRecoveryManagerTest, NoSnapshotMakesClusterReadyAfterFixedWindow)
{
    const std::string clusterName = "no-snapshot";
    ObserveMember(clusterName, MEMBER_A);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, std::move(report), decision));
    EXPECT_EQ(decision.state, TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_EQ(manager_->GetRpcAdmissionState(clusterName), TopologyRecoveryState::READY);
    EXPECT_TRUE(manager_->CheckReadAllowed(TopologyKey(clusterName), "").IsOk());
}

TEST_F(TopologyRecoveryManagerTest, StandaloneUnseenClusterOpensOnlyRpcAdmissionAfterHardDeadline)
{
    const std::string clusterName = "standalone-unseen";
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
    EXPECT_EQ(manager_->GetRpcAdmissionState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(ROUND_TIMEOUT.count() * 1'000 - 1);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
    EXPECT_EQ(manager_->GetRpcAdmissionState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(1);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
    EXPECT_EQ(manager_->GetRpcAdmissionState(clusterName), TopologyRecoveryState::READY);

    ObserveMember(clusterName, MEMBER_A);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
    EXPECT_EQ(manager_->GetRpcAdmissionState(clusterName), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, ElectionUnseenClusterRemainsRecoveringAfterHardDeadline)
{
    manager_->BeginLeaderRound({ 1, COORDINATOR_ID }, HARD_DEADLINE_TIMEOUT);
    clock_->AdvanceMs(HARD_DEADLINE_TIMEOUT.count());

    EXPECT_EQ(manager_->GetState("election-unseen"), TopologyRecoveryState::RECOVERING);
    EXPECT_EQ(manager_->GetRpcAdmissionState("election-unseen"), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, ClusterWithoutRecoveryReportBecomesReadyAfterFastWindow)
{
    const std::string clusterName = "fresh-cluster";
    ObserveMember(clusterName, MEMBER_A);

    clock_->AdvanceMs(FAST_RECOVERY_WINDOW_MS - 1);
    ObserveMember(clusterName, MEMBER_B);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(1);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    EXPECT_TRUE(entries.empty());
}

TEST_F(TopologyRecoveryManagerTest, ExistingMemberDoesNotEnableFastRecovery)
{
    const std::string clusterName = "existing-member";
    ObserveMember(clusterName, MEMBER_A, cluster::MemberLifecycleState::READY);

    clock_->AdvanceMs(FAST_RECOVERY_WINDOW_MS);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    std::this_thread::sleep_for(POLL_INTERVAL);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(
        std::chrono::duration_cast<std::chrono::milliseconds>(ROUND_TIMEOUT).count() - FAST_RECOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, ExistingMemberCancelsFastRecovery)
{
    const std::string clusterName = "mixed-members";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B, cluster::MemberLifecycleState::READY);

    clock_->AdvanceMs(FAST_RECOVERY_WINDOW_MS);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    std::this_thread::sleep_for(POLL_INTERVAL);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, KeepAliveDoesNotEnableFastRecovery)
{
    const std::string clusterName = "keepalive-only";
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));

    clock_->AdvanceMs(FAST_RECOVERY_WINDOW_MS);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    std::this_thread::sleep_for(POLL_INTERVAL);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, RecoveryReportCancelsFastWindow)
{
    const std::string clusterName = "reported-cluster";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, report, decision));

    clock_->AdvanceMs(FAST_RECOVERY_WINDOW_MS);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(ROUND_TIMEOUT.count() * 1'000 - FAST_RECOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, MissingReporterIsForcedReadyAtHardDeadlineWithoutStoreWrite)
{
    manager_->BeginLeaderRound({ 1, COORDINATOR_ID }, HARD_DEADLINE_TIMEOUT);
    const std::string clusterName = "missing-reporter";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 1, COORDINATOR_ID, report, decision));

    clock_->AdvanceMs(HARD_DEADLINE_TIMEOUT.count() - 1);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(1);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    EXPECT_TRUE(entries.empty());
}

TEST_F(TopologyRecoveryManagerTest, HardDeadlineDoesNotSlideAfterMembershipChange)
{
    manager_->BeginLeaderRound({ 1, COORDINATOR_ID }, HARD_DEADLINE_TIMEOUT);
    const std::string clusterName = "fixed-hard-deadline";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 1, COORDINATOR_ID, report, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS + 50);
    ObserveMember(clusterName, MEMBER_C);
    clock_->AdvanceMs(HARD_DEADLINE_TIMEOUT.count() - DISCOVERY_WINDOW_MS - 51);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(1);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, BlockedClusterIsForcedReadyAtHardDeadlineWithoutStoreWrite)
{
    manager_->BeginLeaderRound({ 1, COORDINATOR_ID }, HARD_DEADLINE_TIMEOUT);
    const std::string clusterName = "blocked-hard-deadline";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto first = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto second = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION, 'b');
    TopologyRecoveryReportDecision decision;
    first.canonicalTopology.clear();
    second.canonicalTopology.clear();
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 1, COORDINATOR_ID, first, decision));
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 1, COORDINATOR_ID, second, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::BLOCKED));
    clock_->AdvanceMs(HARD_DEADLINE_TIMEOUT.count() - DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));

    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    EXPECT_TRUE(entries.empty());
}

TEST_F(TopologyRecoveryManagerTest, HardDeadlineSchedulerConvergesWithoutFurtherActivity)
{
    manager_.reset();
    options_.discoveryWindow = std::chrono::milliseconds(500);
    auto realClock = std::make_shared<SteadyClockReal>();
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, realClock, options_);
    manager_->BeginLeaderRound({ 1, COORDINATOR_ID }, REAL_CLOCK_HARD_DEADLINE_TIMEOUT);
    const std::string clusterName = "automatic-hard-deadline";

    ObserveMember(clusterName, MEMBER_A);

    EXPECT_TRUE(WaitUntil([&] { return manager_->GetState(clusterName) == TopologyRecoveryState::READY; }));
}

TEST_F(TopologyRecoveryManagerTest, ParsesAllTopologyKeyKindsInDefaultAndNamedClusters)
{
    constexpr char migrateTaskId[] = "m-e7-0123456789abcdef0123456789abcdef";
    constexpr char deleteTaskId[] = "d-e7-0123456789abcdef0123456789abcdef";
    constexpr char sourceId[] = "source-member-id";
    struct KeyCase {
        std::string physicalKey;
        TopologyCoordinationKeyKind kind;
        std::string relativeKey;
    };

    for (const std::string &clusterName : { std::string(), std::string("blue") }) {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        DS_ASSERT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
        std::string metadataDoneKey;
        DS_ASSERT_OK(cluster::TopologyKeyHelper::ScaleInMetadataDoneKey(7, sourceId, migrateTaskId,
                                                                        metadataDoneKey));
        const std::vector<KeyCase> cases = {
            { keys->TopologyTable() + "/", TopologyCoordinationKeyKind::TOPOLOGY, "" },
            { keys->MigrateTaskTable() + "/", TopologyCoordinationKeyKind::MIGRATE_TASK, "" },
            { keys->DeleteTaskTable() + "/", TopologyCoordinationKeyKind::DELETE_TASK, "" },
            { keys->NotifyTable() + "/", TopologyCoordinationKeyKind::NOTIFY, "" },
            { keys->ProbeTable() + "/", TopologyCoordinationKeyKind::PROBE, "" },
            { keys->MembershipTable() + "/", TopologyCoordinationKeyKind::MEMBERSHIP, "" },
            { keys->UbHealthTable() + "/", TopologyCoordinationKeyKind::UB_HEALTH, "" },
            { keys->ScaleInMetadataDoneTable() + "/", TopologyCoordinationKeyKind::SCALE_IN_METADATA_DONE, "" },
            { keys->MigrateTaskTable() + "/" + migrateTaskId, TopologyCoordinationKeyKind::MIGRATE_TASK,
              migrateTaskId },
            { keys->DeleteTaskTable() + "/" + deleteTaskId, TopologyCoordinationKeyKind::DELETE_TASK,
              deleteTaskId },
            { keys->NotifyTable() + "/" + MEMBER_A, TopologyCoordinationKeyKind::NOTIFY, MEMBER_A },
            { keys->ProbeTable() + "/" + MEMBER_A, TopologyCoordinationKeyKind::PROBE, MEMBER_A },
            { keys->MembershipTable() + "/" + MEMBER_A, TopologyCoordinationKeyKind::MEMBERSHIP, MEMBER_A },
            { keys->UbHealthTable() + "/" + MEMBER_A, TopologyCoordinationKeyKind::UB_HEALTH, MEMBER_A },
            { keys->ScaleInMetadataDoneTable() + "/" + metadataDoneKey,
              TopologyCoordinationKeyKind::SCALE_IN_METADATA_DONE, metadataDoneKey },
            { keys->RolloutTable() + "/" + EVICTION_POLICY_ROLLOUT_KEY,
              TopologyCoordinationKeyKind::EVICTION_POLICY_ROLLOUT, EVICTION_POLICY_ROLLOUT_KEY },
        };
        for (const auto &keyCase : cases) {
            ParsedTopologyCoordinationKey parsed;
            DS_ASSERT_OK(manager_->ParseKey(keyCase.physicalKey, parsed));
            EXPECT_EQ(parsed.clusterName, clusterName);
            EXPECT_EQ(parsed.kind, keyCase.kind);
            EXPECT_EQ(parsed.relativeKey, keyCase.relativeKey);
        }
    }
}

TEST_F(TopologyRecoveryManagerTest, ParsesOnlyExactMasterAddressSingleton)
{
    const std::string key = std::string(COORDINATION_MASTER_ADDRESS_TABLE) + "/" + COORDINATION_MASTER_ADDRESS_KEY;
    ParsedTopologyCoordinationKey parsed;
    DS_ASSERT_OK(manager_->ParseKey(key, parsed));
    EXPECT_TRUE(parsed.clusterName.empty());
    EXPECT_EQ(parsed.kind, TopologyCoordinationKeyKind::MASTER_ADDRESS);
    EXPECT_EQ(parsed.relativeKey, COORDINATION_MASTER_ADDRESS_KEY);

    EXPECT_EQ(manager_->ParseKey(key + "/child", parsed).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->ParseKey("/datasystem/blue/master_address", parsed).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->CheckReadAllowed(key, key + "0").GetCode(), K_INVALID);
}

TEST_F(TopologyRecoveryManagerTest, RejectsReservedClusterNames)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    for (const std::string name :
         { "topology", "tasks", "notify", "probe", "cluster", "ub_health", "scale-in-metadata-done", "control" }) {
        EXPECT_EQ(cluster::TopologyKeyHelper::Create(name, keys).GetCode(), K_INVALID);
    }
}

TEST_F(TopologyRecoveryManagerTest, RejectsInvalidPhysicalKeysAndResetsParsedOutput)
{
    const std::vector<std::string> invalidKeys = {
        "/svc/key",
        "/datasystem/unknown/path",
        "/datasystem",
        "/datasystem/",
        "/datasystem/blue",
        "/datasystem/topology",
        "/datasystem/bad!/topology/",
        "/datasystem/cluster/not-an-address",
        "/datasystem/ub_health/not-an-address",
        "/datasystem/blue/ub_health/not-an-address",
        "/datasystem_ub_health/datasystem/cluster/" + std::string(MEMBER_A),
        "/datasystem/control/unknown",
        "/datasystem/blue/control/unknown",
    };
    for (const auto &physicalKey : invalidKeys) {
        ParsedTopologyCoordinationKey parsed{ "stale", TopologyCoordinationKeyKind::MEMBERSHIP, "stale" };
        EXPECT_EQ(manager_->ParseKey(physicalKey, parsed).GetCode(), K_INVALID);
        EXPECT_TRUE(parsed.clusterName.empty());
        EXPECT_EQ(parsed.kind, TopologyCoordinationKeyKind::TOPOLOGY);
        EXPECT_TRUE(parsed.relativeKey.empty());
    }
}

TEST_F(TopologyRecoveryManagerTest, ParseKeyAndGetStateHaveNoRecoveryOrStoreSideEffects)
{
    const auto summaryBefore = manager_->GetRoundSummary();
    const auto revisionBefore = memoryStore_->CurrentRevision();
    ParsedTopologyCoordinationKey parsed;

    DS_ASSERT_OK(manager_->ParseKey(TopologyKey("side-effect-free"), parsed));
    EXPECT_EQ(manager_->GetState("side-effect-free"), TopologyRecoveryState::RECOVERING);

    EXPECT_EQ(manager_->GetRoundSummary().contextCount, summaryBefore.contextCount);
    EXPECT_EQ(memoryStore_->CurrentRevision(), revisionBefore);
}

TEST_F(TopologyRecoveryManagerTest, ParsesExactWorkerProbeKeys)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create("blue", keys));
    const std::string physicalKey = keys->ProbeTable() + "/" + MEMBER_A;
    ParsedTopologyCoordinationKey parsed;
    DS_ASSERT_OK(manager_->ParseKey(physicalKey, parsed));
    EXPECT_EQ(parsed.clusterName, "blue");
    EXPECT_EQ(parsed.kind, TopologyCoordinationKeyKind::PROBE);
    EXPECT_EQ(parsed.relativeKey, MEMBER_A);
    DS_ASSERT_OK(manager_->ValidateWatchRange(physicalKey, ""));
    EXPECT_EQ(manager_->ValidateWatchRange(keys->ProbeTable() + "/invalid", "").GetCode(), K_INVALID);
}

TEST_F(TopologyRecoveryManagerTest, ParsesScaleInMetadataDoneMarkersInDefaultAndNamedClusters)
{
    constexpr char taskId[] = "m-e7-0123456789abcdef0123456789abcdef";
    constexpr char sourceId[] = "source-member-id";
    for (const std::string &clusterName : { std::string(), std::string("blue") }) {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        DS_ASSERT_OK(cluster::TopologyKeyHelper::Create(clusterName, keys));
        std::string relativeKey;
        DS_ASSERT_OK(cluster::TopologyKeyHelper::ScaleInMetadataDoneKey(7, sourceId, taskId, relativeKey));

        const std::string physicalKey = keys->ScaleInMetadataDoneTable() + "/" + relativeKey;
        ParsedTopologyCoordinationKey parsed;
        DS_ASSERT_OK(manager_->ParseKey(physicalKey, parsed));
        EXPECT_EQ(parsed.clusterName, clusterName);
        EXPECT_EQ(parsed.kind, TopologyCoordinationKeyKind::SCALE_IN_METADATA_DONE);
        EXPECT_EQ(parsed.relativeKey, relativeKey);

        ObserveMember(clusterName, MEMBER_A);
        TopologyRecoveryCandidateReport noSnapshot;
        noSnapshot.reporterAddress = MEMBER_A;
        TopologyRecoveryReportDecision decision;
        DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, noSnapshot, decision));
        clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
        ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
        DS_ASSERT_OK(manager_->CheckMutationAllowed(physicalKey, ""));
        int64_t version = 0;
        int64_t revision = 0;
        DS_ASSERT_OK(store_->Put(physicalKey, "operation-1", 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, version, revision));
        DS_ASSERT_OK(store_->Put(physicalKey, "operation-2", 0, version, version, revision));
        int64_t deleted = 0;
        DS_ASSERT_OK(store_->DeleteRange(physicalKey, "", deleted, revision));
        EXPECT_EQ(deleted, 1);
    }
}

TEST_F(TopologyRecoveryManagerTest, RejectsNonCanonicalScaleInMetadataDoneMarkers)
{
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create("blue", keys));
    const std::string prefix = keys->ScaleInMetadataDoneTable() + "/";
    constexpr char taskId[] = "m-e7-0123456789abcdef0123456789abcdef";
    ParsedTopologyCoordinationKey parsed;

    EXPECT_EQ(manager_->ParseKey(prefix + "e07/aa/" + taskId, parsed).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->ParseKey(prefix + "e7/AA/" + taskId, parsed).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->ParseKey(prefix + "e7/" + std::string(258, 'a') + "/" + taskId, parsed).GetCode(), K_INVALID);
}

TEST_F(TopologyRecoveryManagerTest, RejectsEvidenceFromAnotherLeaderTermWithoutChangingContext)
{
    const std::string clusterName = "term-fence";
    manager_->BeginLeaderRound({ 9, COORDINATOR_ID }, ROUND_TIMEOUT);
    ObserveMember(clusterName, MEMBER_A);
    const auto summaryBefore = manager_->GetRoundSummary();
    const auto revisionBefore = memoryStore_->CurrentRevision();
    auto report = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    report.canonicalTopology.clear();
    TopologyRecoveryReportDecision decision;

    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 8, COORDINATOR_ID, std::move(report), decision));

    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::STALE_LEADER_TERM);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
    EXPECT_EQ(manager_->GetRoundSummary().contextCount, summaryBefore.contextCount);
    EXPECT_EQ(memoryStore_->CurrentRevision(), revisionBefore);
}

TEST_F(TopologyRecoveryManagerTest, RoundSummaryReportsReadyAndBlockedContexts)
{
    const std::string readyCluster = "round-ready";
    const std::string blockedCluster = "round-blocked";
    ObserveMember(readyCluster, MEMBER_A);
    ObserveMember(blockedCluster, MEMBER_A);
    auto readyReport = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto blockedFirst = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'b');
    auto blockedSecond = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION, 'c');
    ObserveMember(blockedCluster, MEMBER_B);
    TopologyRecoveryReportDecision decision;
    ReportEvidence(readyCluster, readyReport, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(readyCluster, 0, COORDINATOR_ID, readyReport, decision));
    ReportEvidence(blockedCluster, blockedFirst, decision);
    ReportEvidence(blockedCluster, blockedSecond, decision);

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(readyCluster, MEMBER_A, TopologyRecoveryState::READY));
    ASSERT_TRUE(DriveUntil(blockedCluster, MEMBER_A, TopologyRecoveryState::BLOCKED));
    const auto summary = manager_->GetRoundSummary();
    EXPECT_EQ(summary.contextCount, 2U);
    EXPECT_EQ(summary.readyCount, 1U);
    EXPECT_EQ(summary.blockedCount, 1U);
    EXPECT_FALSE(summary.AllDiscoveredClustersReady());
}

TEST_F(TopologyRecoveryManagerTest, CommittedStoreMembershipMutationsDriveAdmission)
{
    const std::string clusterName = "observer";
    const auto membershipKey = MembershipKey(clusterName, MEMBER_A);
    store_->SetCommittedMutationObserver([this](WatchEvent::Type type, const std::string &key) {
        manager_->ObserveMembershipChange(
            key, type == WatchEvent::Type::PUT
                     ? std::make_optional(cluster::MemberLifecycleState::STARTING)
                     : std::nullopt);
    });
    int64_t storedVersion = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(membershipKey, "membership", 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, storedVersion,
                             revision));
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::ACCEPTED);

    int64_t deleted = 0;
    DS_ASSERT_OK(store_->DeleteRange(membershipKey, "", deleted, revision));
    ASSERT_EQ(deleted, 1);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY);
    store_->SetCommittedMutationObserver({});
}

TEST_F(TopologyRecoveryManagerTest, InstallsUniqueHighestCanonicalPayload)
{
    const std::string clusterName = "blue";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, payload.canonicalTopology);
}

TEST_F(TopologyRecoveryManagerTest, AcceptedPayloadInstallsAfterDiscoveryWindowWithoutNewReport)
{
    const std::string clusterName = "delayed-install";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(WaitUntil([&] { return manager_->GetState(clusterName) == TopologyRecoveryState::READY; }));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, payload.canonicalTopology);
}

TEST_F(TopologyRecoveryManagerTest, RepeatedEvidenceAfterAcceptedPayloadDoesNotBlockInstall)
{
    const std::string clusterName = "repeat-evidence";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    ReportEvidence(clusterName, payload, decision);
    EXPECT_FALSE(decision.payloadRequired);
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, RecoveryTasksPreserveSubmittingTraceContext)
{
    const std::string clusterName = "trace-context";
    const std::string requestTraceId = "coordinator-request-trace";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    {
        TraceGuard evidenceTraceGuard = Trace::Instance().SetTraceNewID("evidence-only-trace");
        ReportEvidence(clusterName, payload, decision);
    }
    ASSERT_TRUE(decision.payloadRequired);

    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(requestTraceId);
    Trace::Instance().SetRequestLogTrace(true);
    Trace::Instance().SetRequestSampleDecision(true, true);
    Trace::Instance().AddLatencyTick(LatencyTickKey::CLIENT_GET_START);
    Trace::Instance().AddDownstreamPhase(LatencySummaryPhase::CLIENT_PROCESS_GET, TEST_DOWNSTREAM_PHASE_US);
    const TraceContext expectedTrace = Trace::Instance().GetContext();
    const std::string topologyKey = TopologyKey(clusterName);
    auto observedTrace = std::make_shared<std::promise<TraceContext>>();
    auto observedTraceFuture = observedTrace->get_future();
    store_->SetCommittedMutationObserver([observedTrace, topologyKey](WatchEvent::Type type, const std::string &key) {
        if (type == WatchEvent::Type::PUT && key == topologyKey) {
            observedTrace->set_value(Trace::Instance().GetContext());
        }
    });

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));
    ASSERT_EQ(observedTraceFuture.wait_for(TEST_DEADLINE), std::future_status::ready);
    const TraceContext actualTrace = observedTraceFuture.get();
    EXPECT_EQ(actualTrace.traceID, expectedTrace.traceID);
    EXPECT_EQ(actualTrace.requestLogTrace, expectedTrace.requestLogTrace);
    EXPECT_EQ(actualTrace.requestSampleDecisionValid, expectedTrace.requestSampleDecisionValid);
    EXPECT_EQ(actualTrace.requestSampleDecisionAdmitted, expectedTrace.requestSampleDecisionAdmitted);
    ASSERT_EQ(actualTrace.latencyTickCount, expectedTrace.latencyTickCount);
    EXPECT_EQ(actualTrace.latencyTicks[0].key, expectedTrace.latencyTicks[0].key);
    ASSERT_EQ(actualTrace.downstreamPhases.count, expectedTrace.downstreamPhases.count);
    EXPECT_EQ(actualTrace.downstreamPhases.entries[0].phase, expectedTrace.downstreamPhases.entries[0].phase);
    EXPECT_EQ(actualTrace.downstreamPhases.entries[0].durationUs,
              expectedTrace.downstreamPhases.entries[0].durationUs);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    store_->SetCommittedMutationObserver({});
}

TEST_F(TopologyRecoveryManagerTest, MembershipDeleteDoesNotBreakAnInstallingCandidate)
{
    const std::string clusterName = "installing";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    std::atomic<StatusCode> readStatus{ K_OK };
    store_->SetCommittedMutationObserver([&](WatchEvent::Type type, const std::string &key) {
        if (type == WatchEvent::Type::PUT && key == TopologyKey(clusterName)) {
            readStatus.store(manager_->CheckReadAllowed(key, "").GetCode());
            manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_A), std::nullopt);
        }
    });
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_EQ(readStatus.load(), K_NOT_READY);
    store_->SetCommittedMutationObserver({});
}

TEST_F(TopologyRecoveryManagerTest, InstallingCandidateAcceptsIdenticalRetryAndRejectsConflict)
{
    std::promise<void> installationEntered;
    auto installationEnteredFuture = installationEntered.get_future();
    std::promise<void> releaseInstallation;
    auto releaseInstallationFuture = releaseInstallation.get_future().share();
    std::atomic<uint32_t> installAttempts{ 0 };
    std::atomic<bool> installationReleased{ false };
    auto release = [&] {
        if (!installationReleased.exchange(true)) {
            releaseInstallation.set_value();
        }
    };
    manager_.reset();
    options_.beforeInstall = [&] {
        if (installAttempts.fetch_add(1) == 0) {
            installationEntered.set_value();
        }
        releaseInstallationFuture.wait();
    };
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    auto stopManager = Raii([&] {
        release();
        manager_.reset();
    });

    const std::string clusterName = "installing-idempotency";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    const auto revisionBeforeInstall = memoryStore_->CurrentRevision();
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    ASSERT_EQ(installationEnteredFuture.wait_for(TEST_DEADLINE), std::future_status::ready);
    ASSERT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::INSTALLING);

    TopologyRecoveryReportDecision retryDecision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, retryDecision));
    EXPECT_EQ(retryDecision.state, TopologyRecoveryState::INSTALLING);

    auto conflictingPayload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'b');
    TopologyRecoveryReportDecision conflictDecision;
    EXPECT_EQ(manager_
                  ->ReportCandidate(clusterName, 0, COORDINATOR_ID, std::move(conflictingPayload), conflictDecision)
                  .GetCode(),
              K_TRY_AGAIN);

    release();
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_EQ(installAttempts.load(), 1U);
    EXPECT_EQ(memoryStore_->CurrentRevision(), revisionBeforeInstall + 1);
}

TEST_F(TopologyRecoveryManagerTest, CommittedObserverExceptionDoesNotStrandInstallation)
{
    const std::string clusterName = "observer-exception";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    store_->SetCommittedMutationObserver([](WatchEvent::Type, const std::string &) {
        throw std::runtime_error("injected committed observer failure");
    });
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, payload.canonicalTopology);
    store_->SetCommittedMutationObserver({});
}

TEST_F(TopologyRecoveryManagerTest, NewerEvidenceWithinDiscoveryWindowReplacesRetainedCandidate)
{
    const std::string clusterName = "newer";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto version41 = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION - 1, 'a');
    auto version42 = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION, 'b');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, version41, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, version41, decision));
    ReportEvidence(clusterName, version42, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, version42, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, version42.canonicalTopology);
}

TEST_F(TopologyRecoveryManagerTest, ConflictingHighestDigestBlocksWithoutRequestingPayload)
{
    const std::string clusterName = "red";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto first = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto second = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION, 'b');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, first, decision);
    EXPECT_TRUE(decision.payloadRequired);
    ReportEvidence(clusterName, second, decision);
    EXPECT_FALSE(decision.payloadRequired);

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::BLOCKED));
}

TEST_F(TopologyRecoveryManagerTest, WaitsForEveryObservedMemberBeforeInstallingHighestEvidence)
{
    const std::string clusterName = "complete-evidence";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto accepted = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, accepted, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, accepted, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);

    auto late = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION + 1, 'b');
    ReportEvidence(clusterName, late, decision);
    EXPECT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, late, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));

    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, late.canonicalTopology);
}

TEST_F(TopologyRecoveryManagerTest, MembershipDeleteReopensBlockedArbitration)
{
    const std::string clusterName = "healing";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto retained = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto removed = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION, 'b');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, retained, decision);
    ReportEvidence(clusterName, removed, decision);
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::BLOCKED));

    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_B), std::nullopt);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, retained, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, RecoveryGatesAndMemberLimitAreClusterScoped)
{
    const std::string readyCluster = "ready";
    ObserveMember(readyCluster, MEMBER_A);
    TopologyRecoveryCandidateReport emptyReport;
    emptyReport.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(readyCluster, 0, COORDINATOR_ID, emptyReport, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(readyCluster, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_EQ(manager_->CheckReadAllowed(TopologyKey("recovering"), "").GetCode(), K_NOT_READY);
    EXPECT_TRUE(manager_->CheckReadAllowed(TopologyKey(readyCluster), "").IsOk());

    manager_.reset();
    options_.maxMembersPerCluster = 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    ObserveMember("bounded", MEMBER_A);
    ObserveMember("bounded", MEMBER_B);
    emptyReport.reporterAddress = MEMBER_B;
    EXPECT_EQ(manager_->ReportCandidate("bounded", 0, COORDINATOR_ID, emptyReport, decision).GetCode(), K_TRY_AGAIN);
}

TEST_F(TopologyRecoveryManagerTest, RejectsNonTopologyAndCrossKeyspaceRanges)
{
    std::unique_ptr<cluster::TopologyKeyHelper> defaultKeys;
    std::unique_ptr<cluster::TopologyKeyHelper> namedKeys;
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create("", defaultKeys));
    DS_ASSERT_OK(cluster::TopologyKeyHelper::Create("blue", namedKeys));
    const std::string deletePrefix = defaultKeys->DeleteTaskTable() + "/";
    const std::string migrateRangeEnd = defaultKeys->MigrateTaskTable() + "0";
    const std::string namedTopologyKey = namedKeys->TopologyTable() + "/";
    const std::string crossClusterRangeEnd = "/datasystem/green/topology0";

    EXPECT_EQ(manager_->CheckReadAllowed("/svc/key", "").GetCode(), K_INVALID);
    EXPECT_EQ(manager_->CheckMutationAllowed("/svc/key", "").GetCode(), K_INVALID);
    EXPECT_EQ(manager_->ValidateWatchRange("/svc/key", "").GetCode(), K_INVALID);
    EXPECT_EQ(manager_->CheckReadAllowed(deletePrefix, migrateRangeEnd).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->ValidateWatchRange(deletePrefix, migrateRangeEnd).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->CheckReadAllowed(namedTopologyKey, crossClusterRangeEnd).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->ValidateWatchRange(namedTopologyKey, crossClusterRangeEnd).GetCode(), K_INVALID);
}

TEST_F(TopologyRecoveryManagerTest, RejectsOversizedAndNonCanonicalCandidatePayloads)
{
    const std::string clusterName = "invalid-payload";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);

    auto nonCanonical = payload;
    nonCanonical.canonicalTopology = "not-a-cluster-topology";
    EXPECT_EQ(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, nonCanonical, decision).GetCode(), K_INVALID);

    auto oversized = payload;
    oversized.canonicalTopology.assign(MAX_TOPOLOGY_RECOVERY_PAYLOAD_BYTES + 1, 'x');
    EXPECT_EQ(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, oversized, decision).GetCode(), K_INVALID);
}

TEST_F(TopologyRecoveryManagerTest, RejectsCanonicalPayloadThatDoesNotMatchEvidenceDigest)
{
    const std::string clusterName = "digest-mismatch";
    ObserveMember(clusterName, MEMBER_A);
    auto evidence = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto mismatchedPayload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'b');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, evidence, decision);
    ASSERT_TRUE(decision.payloadRequired);
    mismatchedPayload.canonicalDigest = evidence.canonicalDigest;

    EXPECT_EQ(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, mismatchedPayload, decision).GetCode(),
              K_INVALID);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::BLOCKED);
}

TEST_F(TopologyRecoveryManagerTest, ReadyReportDoesNotWriteTopologyAgain)
{
    const std::string clusterName = "ready-idempotency";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> before;
    int64_t beforeRevision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", before, beforeRevision));

    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    std::vector<KeyValueEntry> after;
    int64_t afterRevision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", after, afterRevision));
    ASSERT_EQ(before.size(), 1);
    ASSERT_EQ(after.size(), 1);
    EXPECT_EQ(after.front().version, before.front().version);
    EXPECT_EQ(afterRevision, beforeRevision);
}

TEST_F(TopologyRecoveryManagerTest, ShutdownWaitsForStartedTopologyInstallation)
{
    const std::string clusterName = "shutdown-install";
    const std::string topologyKey = TopologyKey(clusterName);
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    std::promise<void> installationStarted;
    auto installationFuture = installationStarted.get_future();
    std::promise<void> releaseInstallation;
    auto releaseFuture = releaseInstallation.get_future().share();
    std::atomic<bool> installationSignaled{ false };
    store_->SetCommittedMutationObserver([&](WatchEvent::Type, const std::string &key) {
        if (key == topologyKey) {
            bool expected = false;
            if (installationSignaled.compare_exchange_strong(expected, true)) {
                installationStarted.set_value();
            }
            releaseFuture.wait();
        }
    });
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    manager_->NotifyMembershipActivity(MembershipKey(clusterName, MEMBER_A));
    if (installationFuture.wait_for(TEST_DEADLINE) != std::future_status::ready) {
        ADD_FAILURE() << "topology installation did not start";
        releaseInstallation.set_value();
        EXPECT_TRUE(manager_->Shutdown().IsOk());
        store_->SetCommittedMutationObserver({});
        return;
    }

    auto shutdown = std::async(std::launch::async, [this] { return manager_->Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(SHUTDOWN_OBSERVATION), std::future_status::timeout);
    releaseInstallation.set_value();

    ASSERT_EQ(shutdown.wait_for(TEST_DEADLINE), std::future_status::ready);
    EXPECT_TRUE(shutdown.get().IsOk());
    store_->SetCommittedMutationObserver({});
}

TEST_F(TopologyRecoveryManagerTest, InvalidHighestCandidateBlocksUntilItsMemberDisappears)
{
    const std::string clusterName = "invalid-highest";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto validLower = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto invalidHigher = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION + 1, 'b');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, validLower, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, validLower, decision));
    ReportEvidence(clusterName, invalidHigher, decision);
    ASSERT_TRUE(decision.payloadRequired);
    invalidHigher.canonicalTopology = "invalid-topology";

    EXPECT_EQ(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, invalidHigher, decision).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::BLOCKED);

    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_B), std::nullopt);
    ReportEvidence(clusterName, validLower, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, validLower, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    EXPECT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, CandidateMemoryBudgetRejectsPayloadBeforeQueueingValidation)
{
    const std::string clusterName = "memory-budget";
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    manager_.reset();
    options_.maxCandidateMemoryBytes = payload.canonicalTopology.size() - 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    ObserveMember(clusterName, MEMBER_A);
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);

    EXPECT_EQ(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision).GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, ClusterAdmissionLimitRejectsAnAdditionalCluster)
{
    manager_.reset();
    options_.maxClusters = 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    ObserveMember("first", MEMBER_A);
    ObserveMember("second", MEMBER_B);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_B;
    TopologyRecoveryReportDecision decision;

    DS_ASSERT_OK(manager_->ReportCandidate("second", 0, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY);
    EXPECT_EQ(manager_->GetState("first"), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, UnboundRequestsDoNotConsumeClusterAdmission)
{
    manager_.reset();
    options_.maxClusters = 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate("unbound", 0, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY);
    EXPECT_EQ(manager_->CheckReadAllowed(TopologyKey("another-unbound"), "").GetCode(), K_NOT_READY);

    ObserveMember("real", MEMBER_A);
    DS_ASSERT_OK(manager_->ReportCandidate("real", 0, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::ACCEPTED);
}

TEST_F(TopologyRecoveryManagerTest, RequestsOnePayloadForIdenticalHighestEvidence)
{
    const std::string clusterName = "single-flight";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto first = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    auto second = first;
    second.reporterAddress = MEMBER_B;
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, first, decision);
    EXPECT_TRUE(decision.payloadRequired);
    ReportEvidence(clusterName, second, decision);
    EXPECT_FALSE(decision.payloadRequired);
}

TEST_F(TopologyRecoveryManagerTest, DelayedReconcileTimerDoesNotBlockPayloadValidationWorker)
{
    manager_.reset();
    options_.discoveryWindow = DELAYED_RECONCILE_WORKER_WINDOW;
    options_.validationWaitTimeout = DELAYED_RECONCILE_VALIDATION_TIMEOUT;
    options_.minRecoveryThreads = DELAYED_RECONCILE_WORKER_COUNT;
    options_.maxRecoveryThreads = DELAYED_RECONCILE_WORKER_COUNT;
    auto realClock = std::make_shared<SteadyClockReal>();
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, realClock, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);

    TopologyRecoveryReportDecision decision;
    for (size_t i = 0; i < DELAYED_RECONCILE_WORKER_COUNT; ++i) {
        const std::string delayedCluster = "delayed-worker-" + std::to_string(i);
        ObserveMember(delayedCluster, MEMBER_A);
        auto delayedPayload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, static_cast<char>('d' + i));
        ReportEvidence(delayedCluster, delayedPayload, decision);
        ASSERT_TRUE(decision.payloadRequired);
        DS_ASSERT_OK(manager_->ReportCandidate(delayedCluster, 0, COORDINATOR_ID, std::move(delayedPayload), decision));
        std::this_thread::sleep_for(DELAYED_RECONCILE_WORKER_SETTLE);
    }

    const std::string payloadCluster = "payload-single-worker";
    ObserveMember(payloadCluster, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    ReportEvidence(payloadCluster, payload, decision);
    EXPECT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(payloadCluster, 0, COORDINATOR_ID, std::move(payload), decision));
}

TEST_F(TopologyRecoveryManagerTest, ShutdownCancelsDelayedReconcile)
{
    manager_.reset();
    options_.discoveryWindow = std::chrono::seconds(1);
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    manager_->BeginLeaderRound({ 0, COORDINATOR_ID }, ROUND_TIMEOUT);
    const std::string clusterName = "shutdown-delayed";
    ObserveMember(clusterName, MEMBER_A);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::ACCEPTED);

    const auto start = std::chrono::steady_clock::now();
    DS_ASSERT_OK(manager_->Shutdown());
    EXPECT_LT(std::chrono::steady_clock::now() - start, SHUTDOWN_OBSERVATION);
}

TEST_F(TopologyRecoveryManagerTest, ExistingTopologyWinsCreateOnceInstallFence)
{
    const std::string clusterName = "store-exists";
    constexpr char EXISTING_TOPOLOGY[] = "existing-topology";
    int64_t storedVersion = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(TopologyKey(clusterName), EXISTING_TOPOLOGY, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION,
                             storedVersion, revision));
    const int64_t existingRevision = revision;
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, payload, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::BLOCKED));
    std::vector<KeyValueEntry> entries;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, EXISTING_TOPOLOGY);
    EXPECT_EQ(entries.front().modRevision, existingRevision);
}

TEST_F(TopologyRecoveryManagerTest, ReturningMemberReusesCurrentProcessTopologyAuthority)
{
    const std::string clusterName = "membership-return";
    ObserveMember(clusterName, MEMBER_A);
    TopologyRecoveryCandidateReport noSnapshot;
    noSnapshot.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, noSnapshot, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));

    const auto authority = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a').canonicalTopology;
    int64_t storedVersion = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(TopologyKey(clusterName), authority, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION,
                             storedVersion, revision));
    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_A), std::nullopt);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);

    ObserveMember(clusterName, MEMBER_A);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_TRUE(manager_->CheckMutationAllowed(TopologyKey(clusterName), "").IsOk());
    std::vector<KeyValueEntry> entries;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, authority);
}

TEST_F(TopologyRecoveryManagerTest, StaleStoredAuthorityReadCannotPublishIntoRecreatedContext)
{
    constexpr char injectPoint[] = "TopologyRecoveryManager.AdoptStoredAuthority.afterRead";
    const std::string clusterName = "authority-aba";
    const auto authority = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a').canonicalTopology;
    int64_t storedVersion = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(TopologyKey(clusterName), authority, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION,
                             storedVersion, revision));
    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    ObserveMember(clusterName, MEMBER_A);
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));

    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_A), std::nullopt);
    int64_t deleted = 0;
    DS_ASSERT_OK(store_->DeleteRange(TopologyKey(clusterName), "", deleted, revision));
    ASSERT_EQ(deleted, 1);
    ObserveMember(clusterName, MEMBER_B);
    DS_ASSERT_OK(inject::Clear(injectPoint));

    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
    TopologyRecoveryCandidateReport noSnapshot;
    noSnapshot.reporterAddress = MEMBER_B;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, noSnapshot, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    EXPECT_TRUE(DriveUntil(clusterName, MEMBER_B, TopologyRecoveryState::READY));
}

TEST_F(TopologyRecoveryManagerTest, StalePayloadValidationCannotPublishIntoRecreatedContext)
{
    constexpr char injectPoint[] = "TopologyRecoveryManager.ValidatePayload";
    const std::string clusterName = "payload-aba";
    ObserveMember(clusterName, MEMBER_A);
    auto stalePayload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, stalePayload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });

    auto validation = std::async(std::launch::async, [&] {
        TopologyRecoveryReportDecision staleDecision;
        return manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, stalePayload, staleDecision);
    });
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));
    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_A), std::nullopt);
    ObserveMember(clusterName, MEMBER_B);
    TopologyRecoveryCandidateReport noSnapshot;
    noSnapshot.reporterAddress = MEMBER_B;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, 0, COORDINATOR_ID, noSnapshot, decision));

    DS_ASSERT_OK(inject::Clear(injectPoint));
    EXPECT_EQ(validation.get().GetCode(), K_TRY_AGAIN);
    EXPECT_TRUE(DriveUntil(clusterName, MEMBER_B, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    EXPECT_TRUE(entries.empty());
}

TEST_F(TopologyRecoveryManagerTest, RejectsNewReportsAfterShutdown)
{
    ObserveMember("shutdown", MEMBER_A);
    DS_ASSERT_OK(manager_->Shutdown());
    DS_ASSERT_OK(manager_->Shutdown());
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    EXPECT_EQ(manager_->ReportCandidate("shutdown", 0, COORDINATOR_ID, report, decision).GetCode(), K_SHUTTING_DOWN);
}

}  // namespace
}  // namespace datasystem::coordinator
