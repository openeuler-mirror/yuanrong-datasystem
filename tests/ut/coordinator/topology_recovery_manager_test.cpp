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

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/log/trace.h"
#include "ut/common.h"

namespace datasystem::coordinator {
namespace {
constexpr char COORDINATOR_ID[] = "coordinator-id-1";
constexpr char MEMBER_A[] = "127.0.0.1:12001";
constexpr char MEMBER_B[] = "127.0.0.1:12002";
constexpr uint64_t TOPOLOGY_VERSION = 42;
constexpr uint64_t DISCOVERY_WINDOW_MS = 100;
constexpr auto VALIDATION_TIMEOUT = std::chrono::seconds(1);
constexpr auto TEST_DEADLINE = std::chrono::seconds(2);
constexpr auto POLL_INTERVAL = std::chrono::milliseconds(1);
constexpr auto SHUTDOWN_OBSERVATION = std::chrono::milliseconds(100);
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
        options_.discoveryWindow = std::chrono::milliseconds(DISCOVERY_WINDOW_MS);
        options_.validationWaitTimeout = VALIDATION_TIMEOUT;
        manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
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
        topology.members = { cluster::Member{ { std::string(16, identityByte), address },
                                               cluster::MemberState::ACTIVE, { 10, 20, 30, 40 } } };
        TopologyRecoveryCandidateReport report;
        report.reporterAddress = address;
        report.hasSnapshot = true;
        report.topologyVersion = version;
        EXPECT_TRUE(cluster::TopologyRepositoryCodec::EncodeTopology(topology, report.canonicalTopology).IsOk());
        Hasher hasher;
        EXPECT_TRUE(hasher.GetSha256Hex(report.canonicalTopology, report.canonicalDigest).IsOk());
        return report;
    }

    void ObserveMember(const std::string &clusterName, const std::string &address)
    {
        manager_->ObserveMembershipChange(MembershipKey(clusterName, address), true);
    }

    void ReportEvidence(const std::string &clusterName, TopologyRecoveryCandidateReport report,
                        TopologyRecoveryReportDecision &decision)
    {
        report.canonicalTopology.clear();
        DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, std::move(report), decision));
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

    TopologyRecoveryOptions options_;
    std::shared_ptr<MemoryKvStore> memoryStore_;
    std::shared_ptr<WatchRegistry> registry_;
    std::shared_ptr<NoopWatchDispatcher> dispatcher_;
    std::shared_ptr<SteadyClockMock> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> store_;
    std::unique_ptr<TopologyRecoveryManager> manager_;
};

TEST_F(TopologyRecoveryManagerTest, NoSnapshotMakesReservedNameClusterReadyAfterFixedWindow)
{
    const std::string clusterName = "cluster";
    ObserveMember(clusterName, MEMBER_A);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, std::move(report), decision));
    EXPECT_EQ(decision.state, TopologyRecoveryState::RECOVERING);

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_TRUE(manager_->CheckReadAllowed(TopologyKey(clusterName), "").IsOk());
}

TEST_F(TopologyRecoveryManagerTest, ParsesEmptyAndReservedClusterNamesWithoutAmbiguity)
{
    ParsedTopologyCoordinationKey parsed;
    DS_ASSERT_OK(manager_->ParseKey(TopologyKey(""), parsed));
    EXPECT_TRUE(parsed.clusterName.empty());
    EXPECT_EQ(parsed.kind, TopologyCoordinationKeyKind::TOPOLOGY);

    DS_ASSERT_OK(manager_->ParseKey(TopologyKey("cluster"), parsed));
    EXPECT_EQ(parsed.clusterName, "cluster");
    EXPECT_EQ(parsed.kind, TopologyCoordinationKeyKind::TOPOLOGY);
}

TEST_F(TopologyRecoveryManagerTest, CommittedStoreMembershipMutationsDriveAdmission)
{
    const std::string clusterName = "observer";
    const auto membershipKey = MembershipKey(clusterName, MEMBER_A);
    store_->SetCommittedMutationObserver([this](WatchEvent::Type type, const std::string &key) {
        manager_->ObserveMembershipChange(key, type == WatchEvent::Type::PUT);
    });
    int64_t storedVersion = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(membershipKey, "membership", 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, storedVersion,
                             revision));
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::ACCEPTED);

    int64_t deleted = 0;
    DS_ASSERT_OK(store_->DeleteRange(membershipKey, "", deleted, revision));
    ASSERT_EQ(deleted, 1);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, report, decision));
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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, payload.canonicalTopology);
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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));
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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));
    std::atomic<StatusCode> readStatus{ K_OK };
    store_->SetCommittedMutationObserver([&](WatchEvent::Type type, const std::string &key) {
        if (type == WatchEvent::Type::PUT && key == TopologyKey(clusterName)) {
            readStatus.store(manager_->CheckReadAllowed(key, "").GetCode());
            manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_A), false);
        }
    });

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_EQ(readStatus.load(), K_NOT_READY);
    store_->SetCommittedMutationObserver({});
}

TEST_F(TopologyRecoveryManagerTest, CommittedObserverExceptionDoesNotStrandInstallation)
{
    const std::string clusterName = "observer-exception";
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));
    store_->SetCommittedMutationObserver([](WatchEvent::Type, const std::string &) {
        throw std::runtime_error("injected committed observer failure");
    });

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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, version41, decision));
    ReportEvidence(clusterName, version42, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, version42, decision));

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

TEST_F(TopologyRecoveryManagerTest, LateHigherEvidenceDoesNotChangeTheFrozenWindow)
{
    const std::string clusterName = "frozen";
    ObserveMember(clusterName, MEMBER_A);
    ObserveMember(clusterName, MEMBER_B);
    auto accepted = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, accepted, decision);
    ASSERT_TRUE(decision.payloadRequired);
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);

    auto late = SnapshotEvidence(MEMBER_B, TOPOLOGY_VERSION + 1, 'b');
    ReportEvidence(clusterName, late, decision);
    EXPECT_FALSE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, accepted, decision));
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));

    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, accepted.canonicalTopology);
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

    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_B), false);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, retained, decision));
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
    DS_ASSERT_OK(manager_->ReportCandidate(readyCluster, COORDINATOR_ID, emptyReport, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(readyCluster, MEMBER_A, TopologyRecoveryState::READY));
    EXPECT_EQ(manager_->CheckReadAllowed(TopologyKey("recovering"), "").GetCode(), K_NOT_READY);
    EXPECT_TRUE(manager_->CheckReadAllowed(TopologyKey(readyCluster), "").IsOk());

    manager_.reset();
    options_.maxMembersPerCluster = 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    ObserveMember("bounded", MEMBER_A);
    ObserveMember("bounded", MEMBER_B);
    emptyReport.reporterAddress = MEMBER_B;
    EXPECT_EQ(manager_->ReportCandidate("bounded", COORDINATOR_ID, emptyReport, decision).GetCode(), K_TRY_AGAIN);
}

TEST_F(TopologyRecoveryManagerTest, BroadRangeCannotCrossIntoTopologyKeyspaceDuringRecovery)
{
    const std::string broadRangeEnd(1, static_cast<char>(0x7f));
    EXPECT_EQ(manager_->CheckReadAllowed("/", broadRangeEnd).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->CheckMutationAllowed("/", broadRangeEnd).GetCode(), K_INVALID);
    EXPECT_TRUE(manager_->CheckReadAllowed("/alpha", "/beta").IsOk());
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
    EXPECT_EQ(manager_->ReportCandidate(clusterName, COORDINATOR_ID, nonCanonical, decision).GetCode(), K_INVALID);

    auto oversized = payload;
    oversized.canonicalTopology.assign(MAX_TOPOLOGY_RECOVERY_PAYLOAD_BYTES + 1, 'x');
    EXPECT_EQ(manager_->ReportCandidate(clusterName, COORDINATOR_ID, oversized, decision).GetCode(), K_INVALID);
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

    EXPECT_EQ(manager_->ReportCandidate(clusterName, COORDINATOR_ID, mismatchedPayload, decision).GetCode(),
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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));
    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::READY));
    std::vector<KeyValueEntry> before;
    int64_t beforeRevision = 0;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", before, beforeRevision));

    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));

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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));
    std::promise<void> installationStarted;
    auto installationFuture = installationStarted.get_future();
    std::promise<void> releaseInstallation;
    auto releaseFuture = releaseInstallation.get_future().share();
    store_->SetCommittedMutationObserver([&](WatchEvent::Type, const std::string &key) {
        if (key == topologyKey) {
            installationStarted.set_value();
            releaseFuture.wait();
        }
    });
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
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, validLower, decision));
    ReportEvidence(clusterName, invalidHigher, decision);
    ASSERT_TRUE(decision.payloadRequired);
    invalidHigher.canonicalTopology = "invalid-topology";

    EXPECT_EQ(manager_->ReportCandidate(clusterName, COORDINATOR_ID, invalidHigher, decision).GetCode(), K_INVALID);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::BLOCKED);

    manager_->ObserveMembershipChange(MembershipKey(clusterName, MEMBER_B), false);
    ReportEvidence(clusterName, validLower, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, validLower, decision));
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
    ObserveMember(clusterName, MEMBER_A);
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);

    EXPECT_EQ(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision).GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(manager_->GetState(clusterName), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, ClusterAdmissionLimitRejectsAnAdditionalCluster)
{
    manager_.reset();
    options_.maxClusters = 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    ObserveMember("first", MEMBER_A);
    ObserveMember("second", MEMBER_B);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_B;
    TopologyRecoveryReportDecision decision;

    DS_ASSERT_OK(manager_->ReportCandidate("second", COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY);
    EXPECT_EQ(manager_->GetState("first"), TopologyRecoveryState::RECOVERING);
}

TEST_F(TopologyRecoveryManagerTest, UnboundRequestsDoNotConsumeClusterAdmission)
{
    manager_.reset();
    options_.maxClusters = 1;
    manager_ = std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, options_);
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    DS_ASSERT_OK(manager_->ReportCandidate("unbound", COORDINATOR_ID, report, decision));
    EXPECT_EQ(decision.result, TopologyRecoveryReportResult::MEMBERSHIP_NOT_READY);
    EXPECT_EQ(manager_->CheckReadAllowed(TopologyKey("another-unbound"), "").GetCode(), K_NOT_READY);

    ObserveMember("real", MEMBER_A);
    DS_ASSERT_OK(manager_->ReportCandidate("real", COORDINATOR_ID, report, decision));
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

TEST_F(TopologyRecoveryManagerTest, ExistingTopologyWinsCreateOnceInstallFence)
{
    const std::string clusterName = "store-exists";
    constexpr char EXISTING_TOPOLOGY[] = "existing-topology";
    int64_t storedVersion = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(TopologyKey(clusterName), EXISTING_TOPOLOGY, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION,
                             storedVersion, revision));
    ObserveMember(clusterName, MEMBER_A);
    auto payload = SnapshotEvidence(MEMBER_A, TOPOLOGY_VERSION, 'a');
    TopologyRecoveryReportDecision decision;
    ReportEvidence(clusterName, payload, decision);
    ASSERT_TRUE(decision.payloadRequired);
    DS_ASSERT_OK(manager_->ReportCandidate(clusterName, COORDINATOR_ID, payload, decision));

    clock_->AdvanceMs(DISCOVERY_WINDOW_MS);
    ASSERT_TRUE(DriveUntil(clusterName, MEMBER_A, TopologyRecoveryState::BLOCKED));
    std::vector<KeyValueEntry> entries;
    DS_ASSERT_OK(store_->Range(TopologyKey(clusterName), "", entries, revision));
    ASSERT_EQ(entries.size(), 1);
    EXPECT_EQ(entries.front().value, EXISTING_TOPOLOGY);
}

TEST_F(TopologyRecoveryManagerTest, RejectsNewReportsAfterShutdown)
{
    ObserveMember("shutdown", MEMBER_A);
    DS_ASSERT_OK(manager_->Shutdown());
    DS_ASSERT_OK(manager_->Shutdown());
    TopologyRecoveryCandidateReport report;
    report.reporterAddress = MEMBER_A;
    TopologyRecoveryReportDecision decision;
    EXPECT_EQ(manager_->ReportCandidate("shutdown", COORDINATOR_ID, report, decision).GetCode(), K_SHUTTING_DOWN);
}

}  // namespace
}  // namespace datasystem::coordinator
