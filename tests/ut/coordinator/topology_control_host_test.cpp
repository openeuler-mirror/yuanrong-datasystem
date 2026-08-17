/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Multi-cluster Coordinator topology Control Host tests.
 */
#include "datasystem/coordinator/topology_control_host.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "ut/common.h"

namespace datasystem::coordinator {

class TopologyControlHostTestPeer {
public:
    static bool ActiveFailureCandidatesContainExpected(const std::vector<cluster::MemberIdentity> &candidates,
                                                       const std::vector<cluster::MemberIdentity> &expected)
    {
        return TopologyControlHost::ActiveFailureCandidatesContainExpected(candidates, expected);
    }

    static void AddRunningEntry(TopologyControlHost &host, const std::string &clusterName)
    {
        std::lock_guard<std::mutex> lock(host.mutex_);
        auto entry = std::make_unique<TopologyControlHost::ClusterEntry>(clusterName);
        entry->state = TopologyControlHost::EntryState::RUNNING;
        entry->clusterGeneration = host.nextClusterGeneration_++;
        ++entry->mutationGeneration;
        host.entries_.emplace(clusterName, std::move(entry));
    }

    static Status RunUnderActiveFailureCommitFence(TopologyControlHost &host, const std::string &clusterName,
                                                   const cluster::TopologySnapshot &latest,
                                                   const std::vector<cluster::MembershipRecord> &memberships,
                                                   const std::vector<cluster::MemberIdentity> &expected,
                                                   const std::function<Status(int64_t)> &mutation)
    {
        return host.RunUnderActiveFailureCommitFence(clusterName, latest, memberships, host.options_.controller.now(),
                                                     std::nullopt, expected, mutation);
    }

    static uint64_t ClusterGeneration(TopologyControlHost &host, const std::string &clusterName)
    {
        std::lock_guard<std::mutex> lock(host.mutex_);
        return host.entries_.at(clusterName)->clusterGeneration;
    }

    static void ReleaseCluster(TopologyControlHost &host, const std::string &clusterName)
    {
        TopologyControlHost::ClusterEntry *entry = nullptr;
        uint64_t mutationGeneration = 0;
        {
            std::lock_guard<std::mutex> lock(host.mutex_);
            entry = host.entries_.at(clusterName).get();
            mutationGeneration = entry->mutationGeneration;
        }
        static_cast<void>(host.EraseClusterIfCurrent(clusterName, entry, mutationGeneration));
    }

    static bool HasFailureReports(TopologyControlHost &host, const std::string &clusterName)
    {
        std::lock_guard<std::mutex> lock(host.failureReportMutex_);
        return host.failureReportsByCluster_.count(clusterName) > 0;
    }
};

namespace {
constexpr char COORDINATOR_ID[] = "coordinator-id-1";
constexpr char MEMBER_A[] = "127.0.0.1:12001";
constexpr auto TEST_DEADLINE = std::chrono::seconds(2);
constexpr auto TEST_RECONCILE_INTERVAL = std::chrono::milliseconds(10);
constexpr auto TEST_DISCOVERY_WINDOW = std::chrono::milliseconds(10);
constexpr auto TEST_PURE_CONTROL_BUDGET = std::chrono::seconds(3);
constexpr auto TEST_LARGE_BATCH_DEADLINE = std::chrono::seconds(5);
constexpr size_t TEST_CLUSTER_LIMIT = 2;
constexpr size_t TEST_JOINING_MEMBER_COUNT = 500;

TEST(TopologyControlHostCandidateTest, ActiveOnlyExpectationAllowsAdditionalPassiveCandidate)
{
    const cluster::MemberIdentity passiveCandidate{ "passive", "127.0.0.1:12001" };
    const cluster::MemberIdentity activeCandidate{ "active", "127.0.0.1:12002" };
    EXPECT_TRUE(TopologyControlHostTestPeer::ActiveFailureCandidatesContainExpected(
        { passiveCandidate, activeCandidate }, { activeCandidate }));
}

std::shared_ptr<const cluster::TopologySnapshot> MakeActiveSnapshot(size_t memberCount)
{
    cluster::TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    for (size_t index = 0; index < memberCount; ++index) {
        state.members.push_back(cluster::Member{
            { std::string(15, 'a') + static_cast<char>('a' + index), "127.0.0.1:" + std::to_string(12001 + index) },
            cluster::MemberState::ACTIVE,
            { static_cast<uint32_t>(index + 1) },
        });
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    const auto rc = cluster::TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    return snapshot;
}

std::shared_ptr<const cluster::TopologySnapshot> MakeSnapshot(
    const std::vector<cluster::MemberState> &states,
    std::optional<cluster::TopologyChangeType> activeBatch = std::nullopt)
{
    cluster::TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    for (size_t index = 0; index < states.size(); ++index) {
        state.members.push_back(cluster::Member{
            { std::string(15, 'a') + static_cast<char>('a' + index), "127.0.0.1:" + std::to_string(12001 + index) },
            states[index],
            { static_cast<uint32_t>(index + 1) },
        });
    }
    if (activeBatch.has_value()) {
        state.activeBatch = cluster::ActiveBatch{ *activeBatch, 1 };
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    const auto rc = cluster::TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    return snapshot;
}

std::vector<cluster::MembershipRecord> MakeReadyMemberships(const cluster::TopologySnapshot &snapshot)
{
    std::vector<cluster::MembershipRecord> memberships;
    for (const auto &member : snapshot.Members()) {
        memberships.push_back({ member.identity.address, cluster::MemberLifecycleState::READY, 1, "" });
    }
    return memberships;
}

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

class TopologyControlHostTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        memoryStore_ = std::make_shared<MemoryKvStore>();
        registry_ = std::make_shared<WatchRegistry>();
        dispatcher_ = std::make_shared<NoopWatchDispatcher>(registry_.get());
        clock_ = std::make_shared<SteadyClockMock>();
        ttlManager_ = std::make_shared<TtlManager>(clock_);
        store_ = std::make_unique<CoordinatorStore>(memoryStore_, registry_, dispatcher_, ttlManager_);
        TopologyRecoveryOptions recoveryOptions;
        recoveryOptions.discoveryWindow = TEST_DISCOVERY_WINDOW;
        recoveryOptions.maxClusters = TEST_CLUSTER_LIMIT;
        recovery_ =
            std::make_unique<TopologyRecoveryManager>(COORDINATOR_ID, *store_, clock_, recoveryOptions);
        recovery_->BeginLeaderRound({ 0, COORDINATOR_ID });
    }

    void TearDown() override
    {
        host_.reset();
        recovery_.reset();
        store_.reset();
    }

    TopologyControlHost::Options MakeOptions() const
    {
        TopologyControlHost::Options options;
        options.maxClusters = TEST_CLUSTER_LIMIT;
        options.reconcileInterval = TEST_RECONCILE_INTERVAL;
        options.startRetryInitial = TEST_RECONCILE_INTERVAL;
        options.controller.reconcileTick = TEST_RECONCILE_INTERVAL;
        options.controller.nodeDeadTimeout = std::chrono::seconds(1);
        options.controller.now = [this] { return clock_->Now(); };
        return options;
    }

    std::string PhysicalMembershipKey(const std::string &clusterName) const
    {
        return PhysicalMembershipKey(clusterName, MEMBER_A);
    }

    std::string PhysicalMembershipKey(const std::string &clusterName, const std::string &address) const
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        return keys->MembershipTable() + "/" + address;
    }

    std::string PhysicalTopologyKey(const std::string &clusterName) const
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        return keys->TopologyTable() + "/";
    }

    void CommitMembership(const std::string &clusterName)
    {
        CommitMembership(clusterName, MEMBER_A, 1);
    }

    void CommitMembership(
        const std::string &clusterName, const std::string &address, int64_t timestamp,
        cluster::MemberLifecycleState state = cluster::MemberLifecycleState::READY)
    {
        DS_ASSERT_OK(host_->PrepareMembershipPut(clusterName));
        StoreMembership(clusterName, address, timestamp, state);
        host_->CompleteMembershipPut(clusterName, true);
    }

    void StoreMembership(const std::string &clusterName, const std::string &address, int64_t timestamp,
                         cluster::MemberLifecycleState state = cluster::MemberLifecycleState::READY)
    {
        cluster::MembershipValue membership{ timestamp, state, "", "" };
        std::string encoded;
        DS_ASSERT_OK(cluster::MembershipValueCodec::Encode(membership, encoded));
        int64_t version = 0;
        int64_t revision = 0;
        const auto key = PhysicalMembershipKey(clusterName, address);
        DS_ASSERT_OK(store_->Put(key, encoded, 0, COORDINATOR_NO_VERSION_CHECK, version, revision));
        recovery_->ObserveMembershipChange(key, true);
        NotifyHost(key, WatchEvent::Type::PUT);
    }

    void MakeRecoveryReady(const std::string &clusterName)
    {
        TopologyRecoveryCandidateReport report;
        report.reporterAddress = MEMBER_A;
        TopologyRecoveryReportDecision decision;
        DS_ASSERT_OK(recovery_->ReportCandidate(clusterName, 0, COORDINATOR_ID, std::move(report), decision));
        clock_->AdvanceMs(TEST_DISCOVERY_WINDOW.count());
        ASSERT_TRUE(WaitUntil([&] {
            recovery_->NotifyMembershipActivity(PhysicalMembershipKey(clusterName));
            return recovery_->GetState(clusterName) == TopologyRecoveryState::READY;
        }));
    }

    bool WaitUntil(const std::function<bool()> &predicate,
                   std::chrono::steady_clock::duration timeout = TEST_DEADLINE) const
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    }

    bool HasTopology(const std::string &clusterName) const
    {
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        if (store_->Range(PhysicalTopologyKey(clusterName), "", entries, revision).IsError()
            || entries.size() != 1) {
            return false;
        }
        cluster::TopologyState topology;
        return cluster::TopologyRepositoryCodec::DecodeTopology(entries.front().value, topology).IsOk()
               && topology.version > 0;
    }

    bool HasEmptyTopology(const std::string &clusterName) const
    {
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        if (store_->Range(PhysicalTopologyKey(clusterName), "", entries, revision).IsError()
            || entries.size() != 1) {
            return false;
        }
        cluster::TopologyState topology;
        return cluster::TopologyRepositoryCodec::DecodeTopology(entries.front().value, topology).IsOk()
               && topology.members.empty();
    }

    bool TopologyHasMember(const std::string &clusterName, const std::string &address) const
    {
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        if (store_->Range(PhysicalTopologyKey(clusterName), "", entries, revision).IsError()
            || entries.size() != 1) {
            return false;
        }
        cluster::TopologyState topology;
        return cluster::TopologyRepositoryCodec::DecodeTopology(entries.front().value, topology).IsOk()
               && std::any_of(topology.members.begin(), topology.members.end(), [&address](const auto &member) {
                      return member.identity.address == address;
                  });
    }

    bool TopologyHasStateCount(const std::string &clusterName, cluster::MemberState state, size_t expected) const
    {
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        if (store_->Range(PhysicalTopologyKey(clusterName), "", entries, revision).IsError()
            || entries.size() != 1) {
            return false;
        }
        cluster::TopologyState topology;
        return cluster::TopologyRepositoryCodec::DecodeTopology(entries.front().value, topology).IsOk()
               && static_cast<size_t>(std::count_if(
                      topology.members.begin(), topology.members.end(),
                      [state](const auto &member) { return member.state == state; }))
                      == expected;
    }

    bool HasScaleOutNotify(const std::string &clusterName, const std::string &address) const
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        std::string notifyKey;
        EXPECT_TRUE(cluster::TopologyKeyHelper::NotifyKey(address, notifyKey).IsOk());
        std::vector<KeyValueEntry> entries;
        int64_t revision = 0;
        if (store_->Range(keys->NotifyTable() + "/" + notifyKey, "", entries, revision).IsError()
            || entries.size() != 1) {
            return false;
        }
        cluster::TopologyTaskNotify notify;
        return cluster::TopologyRepositoryCodec::DecodeNotify(entries.front().value, notify).IsOk()
               && notify.activeBatch.has_value()
               && notify.activeBatch->type == cluster::TopologyChangeType::SCALE_OUT
               && !notify.taskIds.empty();
    }

    size_t NotifyRecordCount(const std::string &clusterName) const
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        CoordinatorStoreBackend backend(*store_);
        std::vector<std::pair<std::string, std::string>> notifies;
        EXPECT_TRUE(backend.GetAll(keys->NotifyTable(), notifies).IsOk());
        return notifies.size();
    }

    size_t MigrateTaskRecordCount(const std::string &clusterName) const
    {
        std::unique_ptr<cluster::TopologyKeyHelper> keys;
        EXPECT_TRUE(cluster::TopologyKeyHelper::Create(clusterName, keys).IsOk());
        CoordinatorStoreBackend backend(*store_);
        std::vector<std::pair<std::string, std::string>> tasks;
        EXPECT_TRUE(backend.GetAll(keys->MigrateTaskTable(), tasks).IsOk());
        return tasks.size();
    }

    void NotifyHost(const std::string &physicalKey, WatchEvent::Type type)
    {
        ParsedTopologyCoordinationKey parsed;
        DS_ASSERT_OK(recovery_->ParseKey(physicalKey, parsed));
        host_->NotifyStoreMutation(type, parsed);
    }

    void CommitEmptyTopology(const std::string &clusterName)
    {
        cluster::TopologyState topology;
        topology.version = 2;
        topology.clusterHasInit = true;
        std::string encoded;
        DS_ASSERT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(topology, encoded));
        int64_t version = 0;
        int64_t revision = 0;
        const auto topologyKey = PhysicalTopologyKey(clusterName);
        DS_ASSERT_OK(store_->Put(topologyKey, encoded, 0, COORDINATOR_NO_VERSION_CHECK, version, revision));
        int64_t deleted = 0;
        const auto membershipKey = PhysicalMembershipKey(clusterName);
        DS_ASSERT_OK(store_->DeleteRange(membershipKey, "", deleted, revision));
        NotifyHost(topologyKey, WatchEvent::Type::PUT);
        NotifyHost(membershipKey, WatchEvent::Type::DELETE);
    }

    std::shared_ptr<MemoryKvStore> memoryStore_;
    std::shared_ptr<WatchRegistry> registry_;
    std::shared_ptr<NoopWatchDispatcher> dispatcher_;
    std::shared_ptr<SteadyClockMock> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> store_;
    std::unique_ptr<TopologyRecoveryManager> recovery_;
    std::unique_ptr<TopologyControlHost> host_;
};

TEST_F(TopologyControlHostTest, ValidatesOptionsAndKeepsStartOneShot)
{
    auto invalidOptions = MakeOptions();
    invalidOptions.maxClusters = 1;
    TopologyControlHost invalidHost(COORDINATOR_ID, *store_, *recovery_, invalidOptions);
    EXPECT_EQ(invalidHost.Start().GetCode(), K_INVALID);
    EXPECT_TRUE(invalidHost.IsStopped());

    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    EXPECT_EQ(host_->Start().GetCode(), K_INVALID);
    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, WorkerFailureSummariesRequireReporterThreshold)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "");
    auto snapshot = MakeActiveSnapshot(6);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = snapshot->Members()[5].identity.address;
    for (size_t index = 0; index < 4; ++index) {
        host_->RecordWorkerFailureSummaries("", snapshot->Members()[index].identity.address, { target });
    }
    EXPECT_TRUE(host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now()).empty());

    host_->RecordWorkerFailureSummaries("", snapshot->Members()[4].identity.address, { target });
    const auto candidates = host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates.front().address, target);
}

TEST_F(TopologyControlHostTest, TwoWorkerFailureSummaryProducesProbeCandidate)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "");
    auto snapshot = MakeActiveSnapshot(2);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto reporter = snapshot->Members()[0].identity.address;
    const auto target = snapshot->Members()[1].identity.address;

    host_->RecordWorkerFailureSummaries("", reporter, { target });

    const auto candidates = host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1);
    EXPECT_EQ(candidates.front().address, target);
}

TEST_F(TopologyControlHostTest, ConcurrentFailuresUseAllRemainingEligibleReporters)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "");
    auto snapshot = MakeActiveSnapshot(4);
    auto memberships = MakeReadyMemberships(*snapshot);
    memberships.resize(2);
    const auto target = snapshot->Members()[2].identity.address;

    host_->RecordWorkerFailureSummaries("", memberships[0].address, { target });
    EXPECT_TRUE(host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now()).empty());

    host_->RecordWorkerFailureSummaries("", memberships[1].address, { target });
    const auto candidates = host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1UL);
    EXPECT_EQ(candidates.front().address, target);
}

TEST_F(TopologyControlHostTest, ConcurrentFailuresNeverUseOneReporterInLargerCluster)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "");
    auto snapshot = MakeActiveSnapshot(4);
    auto memberships = MakeReadyMemberships(*snapshot);
    memberships.resize(1);
    const auto target = snapshot->Members()[2].identity.address;

    host_->RecordWorkerFailureSummaries("", memberships[0].address, { target });

    EXPECT_TRUE(host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, WorkerFailureSummaryResetRemovesProbeCandidate)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "");
    auto snapshot = MakeActiveSnapshot(2);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto reporter = snapshot->Members()[0].identity.address;
    const auto target = snapshot->Members()[1].identity.address;

    host_->RecordWorkerFailureSummaries("", reporter, { target });
    ASSERT_EQ(host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now()).size(), 1UL);

    host_->RecordWorkerFailureSummaries("", reporter, {});
    EXPECT_TRUE(host_->GetIsolationCandidates("", *snapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, ActiveFailureCommitDoesNotBlockOtherClusterReports)
{
    auto options = MakeOptions();
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    const std::string committingCluster = "blue";
    const std::string reportingCluster = "green";
    TopologyControlHostTestPeer::AddRunningEntry(*host_, committingCluster);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, reportingCluster);
    auto snapshot = MakeActiveSnapshot(2);
    const auto memberships = MakeReadyMemberships(*snapshot);
    for (const auto &membership : memberships) {
        StoreMembership(committingCluster, membership.address, membership.timestamp);
    }
    const auto &reporter = memberships.front();
    const auto &target = memberships.back();
    host_->RecordWorkerFailureSummaries(committingCluster, reporter, { target });

    std::promise<void> mutationEntered;
    std::promise<void> releaseMutation;
    auto releaseFuture = releaseMutation.get_future().share();
    auto commit = std::async(std::launch::async, [&] {
        return TopologyControlHostTestPeer::RunUnderActiveFailureCommitFence(
            *host_, committingCluster, *snapshot, memberships, { snapshot->Members().back().identity }, [&](int64_t) {
                mutationEntered.set_value();
                releaseFuture.wait();
                return Status::OK();
            });
    });
    ASSERT_EQ(mutationEntered.get_future().wait_for(TEST_DEADLINE), std::future_status::ready);

    auto report = std::async(std::launch::async,
                             [&] { host_->RecordWorkerFailureSummaries(reportingCluster, reporter, { target }); });
    const auto reportStatus = report.wait_for(std::chrono::milliseconds(100));
    releaseMutation.set_value();

    EXPECT_EQ(reportStatus, std::future_status::ready);
    report.get();
    DS_ASSERT_OK(commit.get());
}

TEST_F(TopologyControlHostTest, WorkerFailureSummariesIgnoreStaleTargetIncarnation)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(6);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships[5];
    for (size_t index = 0; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { target });
    }
    for (auto &membership : memberships) {
        if (membership.address == target.address) {
            ++membership.timestamp;
        }
    }

    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, WorkerFailureSummariesCanIsolateActiveTargetAfterTargetMembershipExpired)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(3);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    memberships.pop_back();

    cluster::MembershipRecord expiredTarget{ target.address, cluster::MemberLifecycleState::READY, -1, "" };
    for (size_t index = 0; index < memberships.size(); ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { expiredTarget });
    }

    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1UL);
    EXPECT_EQ(candidates.front().address, target.address);
}

TEST_F(TopologyControlHostTest, WorkerFailureSummariesIgnoreUnknownIncarnationAfterTargetMembershipReturns)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(3);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    cluster::MembershipRecord unknownTarget{ target.address, cluster::MemberLifecycleState::READY, -1, "" };
    for (size_t index = 0; index + 1 < memberships.size(); ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { unknownTarget });
    }

    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, WorkerFailureSummariesExpireAndIgnoreInactiveMembers)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(6);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = snapshot->Members()[5].identity.address;
    for (size_t index = 0; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", snapshot->Members()[index].identity.address, { target });
    }

    clock_->AdvanceMs(std::chrono::duration_cast<std::chrono::milliseconds>(options.activeFailureWindow).count() + 1);
    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());

    for (size_t index = 0; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", snapshot->Members()[index].identity.address, { target });
    }
    memberships.resize(1);
    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, WorkerFailureSummariesSurviveOneReporterFailureWhenQuorumRemains)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(7);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = snapshot->Members()[6].identity.address;
    for (size_t index = 0; index < 6; ++index) {
        host_->RecordWorkerFailureSummaries("blue", snapshot->Members()[index].identity.address, { target });
    }

    memberships.erase(memberships.begin());
    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());

    ASSERT_EQ(candidates.size(), 1UL);
    EXPECT_EQ(candidates.front().address, target);
}

TEST_F(TopologyControlHostTest, WorkerFailureSummaryRefreshDoesNotCountDuplicateReporter)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(6);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = snapshot->Members()[5].identity.address;

    host_->RecordWorkerFailureSummaries("blue", snapshot->Members()[0].identity.address, { target });
    host_->RecordWorkerFailureSummaries("blue", snapshot->Members()[0].identity.address, { target });
    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());

    for (size_t index = 1; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", snapshot->Members()[index].identity.address, { target });
    }
    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1UL);
    EXPECT_EQ(candidates.front().address, target);
}

TEST_F(TopologyControlHostTest, ReporterTimestampChangeInvalidatesFailureSummaryEvidence)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(6);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    host_->RecordWorkerFailureSummaries("blue", memberships[0], { target });
    ++memberships[0].timestamp;
    for (size_t index = 1; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { target });
    }

    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());

    host_->RecordWorkerFailureSummaries("blue", memberships[0], { target });
    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1U);
    EXPECT_EQ(candidates.front().address, target.address);
}

TEST_F(TopologyControlHostTest, ReporterHostIdChangeInvalidatesFailureSummaryEvidence)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(6);
    auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    host_->RecordWorkerFailureSummaries("blue", memberships[0], { target });
    memberships[0].hostId = "restarted-host";
    for (size_t index = 1; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { target });
    }

    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());

    host_->RecordWorkerFailureSummaries("blue", memberships[0], { target });
    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());
    ASSERT_EQ(candidates.size(), 1U);
    EXPECT_EQ(candidates.front().address, target.address);
}

TEST_F(TopologyControlHostTest, ExitingLeavingReportersCanIsolateActiveTarget)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeSnapshot({ cluster::MemberState::LEAVING, cluster::MemberState::LEAVING,
                                   cluster::MemberState::LEAVING, cluster::MemberState::LEAVING,
                                   cluster::MemberState::LEAVING, cluster::MemberState::ACTIVE },
                                 cluster::TopologyChangeType::SCALE_IN);
    auto memberships = MakeReadyMemberships(*snapshot);
    for (size_t index = 0; index < 5; ++index) {
        memberships[index].state = cluster::MemberLifecycleState::EXITING;
    }
    const auto target = memberships.back();
    memberships.pop_back();
    cluster::MembershipRecord expiredTarget{ target.address, cluster::MemberLifecycleState::READY, -1, "" };
    for (const auto &reporter : memberships) {
        host_->RecordWorkerFailureSummaries("blue", reporter, { expiredTarget });
    }

    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());

    ASSERT_EQ(candidates.size(), 1U);
    EXPECT_EQ(candidates.front().address, target.address);
}

TEST_F(TopologyControlHostTest, ReadyEvidenceDoesNotBecomeEligibleAfterReporterStartsExiting)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto activeSnapshot = MakeActiveSnapshot(6);
    auto memberships = MakeReadyMemberships(*activeSnapshot);
    const auto target = memberships.back();
    memberships.pop_back();
    cluster::MembershipRecord expiredTarget{ target.address, cluster::MemberLifecycleState::READY, -1, "" };
    for (const auto &reporter : memberships) {
        host_->RecordWorkerFailureSummaries("blue", reporter, { expiredTarget });
    }
    auto leavingSnapshot = MakeSnapshot({ cluster::MemberState::LEAVING, cluster::MemberState::LEAVING,
                                          cluster::MemberState::LEAVING, cluster::MemberState::LEAVING,
                                          cluster::MemberState::LEAVING, cluster::MemberState::ACTIVE },
                                        cluster::TopologyChangeType::SCALE_IN);
    for (auto &membership : memberships) {
        membership.state = cluster::MemberLifecycleState::EXITING;
    }

    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *leavingSnapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, ActiveReportersCanRemoveJoiningScaleOutTarget)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeSnapshot({ cluster::MemberState::ACTIVE, cluster::MemberState::ACTIVE,
                                   cluster::MemberState::ACTIVE, cluster::MemberState::ACTIVE,
                                   cluster::MemberState::ACTIVE, cluster::MemberState::JOINING },
                                 cluster::TopologyChangeType::SCALE_OUT);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    for (size_t index = 0; index < 5; ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { target });
    }

    const auto candidates = host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now());

    ASSERT_EQ(candidates.size(), 1U);
    EXPECT_EQ(candidates.front().address, target.address);
}

TEST_F(TopologyControlHostTest, ReservesOneSlotPerClusterAndReleasesFailedReservations)
{
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    DS_ASSERT_OK(host_->PrepareMembershipPut("blue"));
    DS_ASSERT_OK(host_->PrepareMembershipPut("blue"));
    DS_ASSERT_OK(host_->PrepareMembershipPut("green"));
    EXPECT_EQ(host_->PrepareMembershipPut("red").GetCode(), K_TRY_AGAIN);

    host_->CompleteMembershipPut("blue", false);
    host_->CompleteMembershipPut("blue", false);
    ASSERT_TRUE(WaitUntil([&] { return host_->PrepareMembershipPut("red").IsOk(); }));
    host_->CompleteMembershipPut("red", false);
    host_->CompleteMembershipPut("green", false);

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, ReleasingClusterDiscardsFailureEvidence)
{
    auto options = MakeOptions();
    options.activeFailureWindow = std::chrono::seconds(3);
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(6);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    for (size_t index = 0; index + 1 < memberships.size(); ++index) {
        host_->RecordWorkerFailureSummaries("blue", memberships[index], { target });
    }
    ASSERT_EQ(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).size(), 1U);

    TopologyControlHostTestPeer::ReleaseCluster(*host_, "blue");

    EXPECT_TRUE(host_->GetIsolationCandidates("blue", *snapshot, memberships, clock_->Now()).empty());
}

TEST_F(TopologyControlHostTest, DelayedSummaryCannotRecreateReleasedClusterEvidence)
{
    constexpr char injectPoint[] = "TopologyControlHost.RecordWorkerFailureSummaries.afterGenerationCapture";
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    const auto generation = TopologyControlHostTestPeer::ClusterGeneration(*host_, "blue");
    const cluster::MembershipRecord reporter{ "127.0.0.1:12001", cluster::MemberLifecycleState::READY, 1, "" };
    const cluster::MembershipRecord target{ "127.0.0.1:12002", cluster::MemberLifecycleState::READY, 1, "" };
    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    auto delayedReport =
        std::async(std::launch::async, [&] { host_->RecordWorkerFailureSummaries("blue", reporter, { target }); });
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));

    TopologyControlHostTestPeer::ReleaseCluster(*host_, "blue");
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    EXPECT_NE(TopologyControlHostTestPeer::ClusterGeneration(*host_, "blue"), generation);
    DS_ASSERT_OK(inject::Clear(injectPoint));
    delayedReport.get();

    EXPECT_FALSE(TopologyControlHostTestPeer::HasFailureReports(*host_, "blue"));
}

TEST_F(TopologyControlHostTest, ActiveFailureFenceRejectsReplacedMembershipIncarnation)
{
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(3);
    const auto staleMemberships = MakeReadyMemberships(*snapshot);
    const auto target = staleMemberships.back();
    host_->RecordWorkerFailureSummaries("blue", staleMemberships[0], { target });
    host_->RecordWorkerFailureSummaries("blue", staleMemberships[1], { target });
    for (const auto &membership : staleMemberships) {
        StoreMembership("blue", membership.address, membership.address == target.address ? 2 : 1);
    }
    bool mutated = false;

    DS_ASSERT_OK(TopologyControlHostTestPeer::RunUnderActiveFailureCommitFence(
        *host_, "blue", *snapshot, staleMemberships, { snapshot->Members().back().identity }, [&](int64_t) {
            mutated = true;
            return Status::OK();
        }));

    EXPECT_FALSE(mutated);
}

TEST_F(TopologyControlHostTest, ActiveFailureFenceRejectsMembershipMutationBeforeTopologyCas)
{
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    TopologyControlHostTestPeer::AddRunningEntry(*host_, "blue");
    auto snapshot = MakeActiveSnapshot(3);
    const auto memberships = MakeReadyMemberships(*snapshot);
    const auto target = memberships.back();
    host_->RecordWorkerFailureSummaries("blue", memberships[0], { target });
    host_->RecordWorkerFailureSummaries("blue", memberships[1], { target });
    for (const auto &membership : memberships) {
        StoreMembership("blue", membership.address, membership.timestamp);
    }
    cluster::TopologyState current{ true, snapshot->Version(), snapshot->Members(), snapshot->GetActiveBatch() };
    std::string encoded;
    DS_ASSERT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(current, encoded));
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put(PhysicalTopologyKey("blue"), encoded, 0, COORDINATOR_KEY_NOT_EXISTS_VERSION, version,
                             revision));

    const auto rc = TopologyControlHostTestPeer::RunUnderActiveFailureCommitFence(
        *host_, "blue", *snapshot, memberships, { snapshot->Members().back().identity }, [&](int64_t authorityRevision) {
            StoreMembership("blue", target.address, target.timestamp + 1);
            std::unique_ptr<cluster::TopologyKeyHelper> keys;
            RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create("blue", keys));
            CoordinatorStoreBackend backend(*store_);
            cluster::TopologyRepository repository(backend, *keys);
            auto desired = current;
            ++desired.version;
            cluster::TopologyCasResult result;
            RETURN_IF_NOT_OK(repository.CompareAndSwapTopology(current.version, desired, result, authorityRevision));
            CHECK_FAIL_RETURN_STATUS(result.outcome == cluster::TopologyCasOutcome::COMMITTED, K_TRY_AGAIN,
                                     "membership snapshot changed before topology commit");
            return Status::OK();
        });

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    std::vector<KeyValueEntry> entries;
    DS_ASSERT_OK(store_->Range(PhysicalTopologyKey("blue"), "", entries, revision));
    ASSERT_EQ(entries.size(), 1U);
    cluster::TopologyState preserved;
    DS_ASSERT_OK(cluster::TopologyRepositoryCodec::DecodeTopology(entries.front().value, preserved));
    EXPECT_EQ(preserved.version, current.version);
}

TEST_F(TopologyControlHostTest, StartsIndependentRuntimesAfterRecoveryReadiness)
{
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    CommitMembership("green");
    MakeRecoveryReady("blue");
    MakeRecoveryReady("green");

    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue") && HasTopology("green"); }));
    ParsedTopologyCoordinationKey parsed;
    DS_ASSERT_OK(recovery_->ParseKey(PhysicalTopologyKey("blue"), parsed));
    host_->NotifyStoreMutation(WatchEvent::Type::PUT, parsed);

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, RuntimeStartFailureRetriesWithoutReleasingClusterSlots)
{
    constexpr char injectPoint[] = "TopologyControlHost.StartRuntime";
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(inject::Set(injectPoint, "return(K_RUNTIME_ERROR)"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    MakeRecoveryReady("blue");
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));
    DS_ASSERT_OK(host_->PrepareMembershipPut("green"));
    EXPECT_EQ(host_->PrepareMembershipPut("red").GetCode(), K_TRY_AGAIN);

    DS_ASSERT_OK(inject::Clear(injectPoint));
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue"); }));
    EXPECT_EQ(host_->PrepareMembershipPut("red").GetCode(), K_TRY_AGAIN);
    host_->CompleteMembershipPut("green", false);
    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
}

TEST_F(TopologyControlHostTest, RuntimeStartExceptionsStopPublishedDependenciesBeforeRetry)
{
    constexpr char startInjectPoint[] = "TopologyControlHost.StartRuntime.afterPublish";
    constexpr char stopInjectPoint[] = "TopologyControlHost.StopRuntime";
    DS_ASSERT_OK(inject::Set(startInjectPoint, "1*call(std)->1*call(unknown)"));
    Raii clearStartInject([&] { (void)inject::Clear(startInjectPoint); });
    DS_ASSERT_OK(inject::Set(stopInjectPoint, "call()"));
    Raii clearStopInject([&] { (void)inject::Clear(stopInjectPoint); });
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());

    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    MakeRecoveryReady("blue");
    ASSERT_TRUE(WaitUntil([&] {
        return inject::GetExecuteCount(startInjectPoint) >= 2
               && inject::GetExecuteCount(stopInjectPoint) >= 2;
    }));
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue"); }));

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
}

TEST_F(TopologyControlHostTest, ReconcileExceptionDoesNotTerminateHostOrBlockShutdown)
{
    constexpr char injectPoint[] = "TopologyControlHost.ReconcileEntries.exception";
    DS_ASSERT_OK(inject::Set(injectPoint, "1*call()"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());

    DS_ASSERT_OK(host_->Start());
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));
    CommitMembership("blue");
    MakeRecoveryReady("blue");
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue"); }));

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, ShutdownRetainsTimedOutRuntimeForRetry)
{
    constexpr char injectPoint[] = "TopologyControlHost.StopRuntime";
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    MakeRecoveryReady("blue");
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue"); }));
    DS_ASSERT_OK(inject::Set(injectPoint, "return(K_RPC_DEADLINE_EXCEEDED)"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });

    EXPECT_EQ(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE).GetCode(),
              K_RPC_DEADLINE_EXCEEDED);
    EXPECT_FALSE(host_->IsStopped());
    DS_ASSERT_OK(inject::Clear(injectPoint));
    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, Burst500MembershipsReachOneMaterializedScaleOutWithinControlBudget)
{
    auto options = MakeOptions();
    options.controller.now = [] { return std::chrono::steady_clock::now(); };
    // The controller collection policy has dedicated clock-driven tests. Keep this host/store performance test focused
    // on the materialization path instead of charging the default three-second policy window to its control budget.
    options.controller.scaleOutCollectWindow = TEST_RECONCILE_INTERVAL;
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, options);
    DS_ASSERT_OK(host_->Start());
    CommitMembership("burst");
    MakeRecoveryReady("burst");
    ASSERT_TRUE(WaitUntil([&] { return TopologyHasStateCount("burst", cluster::MemberState::ACTIVE, 1); }));
    CommitMembership("burst", MEMBER_A, TEST_JOINING_MEMBER_COUNT + 2,
                     cluster::MemberLifecycleState::RESTARTING);
    const auto startedAt = std::chrono::steady_clock::now();
    for (size_t index = 0; index < TEST_JOINING_MEMBER_COUNT; ++index) {
        const auto address = "127.0.0.1:" + std::to_string(20'000 + index);
        CommitMembership("burst", address, static_cast<int64_t>(index + 2));
    }
    ASSERT_TRUE(WaitUntil(
        [&] { return TopologyHasStateCount("burst", cluster::MemberState::INITIAL, TEST_JOINING_MEMBER_COUNT); },
        TEST_LARGE_BATCH_DEADLINE));
    ASSERT_TRUE(WaitUntil(
        [&] { return HasScaleOutNotify("burst", MEMBER_A); }, TEST_LARGE_BATCH_DEADLINE));
    ASSERT_TRUE(WaitUntil(
        [&] { return MigrateTaskRecordCount("burst") == TEST_JOINING_MEMBER_COUNT; },
        TEST_LARGE_BATCH_DEADLINE));
    ASSERT_TRUE(WaitUntil(
        [&] { return NotifyRecordCount("burst") == TEST_JOINING_MEMBER_COUNT + 1; },
        TEST_LARGE_BATCH_DEADLINE));
    ASSERT_TRUE(WaitUntil(
        [&] { return TopologyHasStateCount("burst", cluster::MemberState::JOINING, TEST_JOINING_MEMBER_COUNT); },
        TEST_LARGE_BATCH_DEADLINE));
    const auto elapsed = std::chrono::steady_clock::now() - startedAt;
    std::cout << "CLUSTER_PERF scope=host_store joining=" << TEST_JOINING_MEMBER_COUNT
              << " elapsed_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
              << std::endl;
    EXPECT_LT(elapsed, TEST_PURE_CONTROL_BUDGET);
    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
}

TEST_F(TopologyControlHostTest, KeepsEmptyAndNamedClusterScopesIndependent)
{
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    DS_ASSERT_OK(host_->PrepareMembershipPut(""));
    DS_ASSERT_OK(host_->PrepareMembershipPut("named"));
    host_->CompleteMembershipPut("", false);
    host_->CompleteMembershipPut("named", false);

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, DoesNotReleaseClusterWhileTopologyStillOwnsMember)
{
    constexpr char injectPoint[] = "TopologyControlHost.ReleaseClusterIfEmpty.afterTopologyRead";
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    CommitMembership("green");
    MakeRecoveryReady("blue");
    MakeRecoveryReady("green");
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue") && HasTopology("green"); }));

    int64_t deleted = 0;
    int64_t revision = 0;
    const auto membershipKey = PhysicalMembershipKey("blue");
    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(store_->DeleteRange(membershipKey, "", deleted, revision));
    recovery_->ObserveMembershipChange(membershipKey, false);
    NotifyHost(membershipKey, WatchEvent::Type::DELETE);
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));
    EXPECT_EQ(host_->PrepareMembershipPut("red").GetCode(), K_TRY_AGAIN);
    DS_ASSERT_OK(inject::Clear(injectPoint));
    clock_->AdvanceMs(std::chrono::duration_cast<std::chrono::milliseconds>(
                          MakeOptions().controller.nodeDeadTimeout)
                          .count());
    NotifyHost(membershipKey, WatchEvent::Type::DELETE);
    ASSERT_TRUE(WaitUntil([&] { return HasEmptyTopology("blue"); }));

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
}

TEST_F(TopologyControlHostTest, StopsEmptyRuntimeBeforeReusingClusterSlot)
{
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    CommitMembership("green");
    MakeRecoveryReady("blue");
    MakeRecoveryReady("green");
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue") && HasTopology("green"); }));

    CommitEmptyTopology("blue");
    ASSERT_TRUE(WaitUntil([&] { return host_->PrepareMembershipPut("red").IsOk(); }));
    host_->CompleteMembershipPut("red", false);

    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

TEST_F(TopologyControlHostTest, EmptyObservationCannotEraseConcurrentMembershipAdmission)
{
    constexpr char injectPoint[] = "TopologyControlHost.ReleaseClusterIfEmpty.afterRead";
    host_ = std::make_unique<TopologyControlHost>(COORDINATOR_ID, *store_, *recovery_, MakeOptions());
    DS_ASSERT_OK(host_->Start());
    CommitMembership("blue");
    CommitMembership("green");
    MakeRecoveryReady("blue");
    MakeRecoveryReady("green");
    ASSERT_TRUE(WaitUntil([&] { return HasTopology("blue") && HasTopology("green"); }));

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    CommitEmptyTopology("blue");
    ASSERT_TRUE(WaitUntil([&] { return inject::GetExecuteCount(injectPoint) > 0; }));

    DS_ASSERT_OK(host_->PrepareMembershipPut("blue"));
    cluster::MembershipValue membership{ 2, cluster::MemberLifecycleState::READY, "", "" };
    std::string encoded;
    DS_ASSERT_OK(cluster::MembershipValueCodec::Encode(membership, encoded));
    int64_t version = 0;
    int64_t revision = 0;
    const auto membershipKey = PhysicalMembershipKey("blue");
    DS_ASSERT_OK(store_->Put(membershipKey, encoded, 0, COORDINATOR_NO_VERSION_CHECK, version, revision));
    recovery_->ObserveMembershipChange(membershipKey, true);
    NotifyHost(membershipKey, WatchEvent::Type::PUT);
    host_->CompleteMembershipPut("blue", true);
    DS_ASSERT_OK(inject::Clear(injectPoint));

    ASSERT_TRUE(WaitUntil([&] { return TopologyHasMember("blue", MEMBER_A); }));
    EXPECT_EQ(host_->PrepareMembershipPut("red").GetCode(), K_TRY_AGAIN);
    DS_ASSERT_OK(host_->Shutdown(std::chrono::steady_clock::now() + TEST_DEADLINE));
    EXPECT_TRUE(host_->IsStopped());
}

}  // namespace
}  // namespace datasystem::coordinator
