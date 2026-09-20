/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology one-shot reader and Snapshot publication tests.
 */
#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "ut/cluster/testing/fake_coordination_backend.h"
#include "ut/cluster/testing/fake_coordinator_service_proxy.h"

#include <future>
#include <limits>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

std::shared_ptr<const TopologySnapshot> MakeSnapshot(uint64_t version, char digestByte)
{
    TopologyState state;
    state.version = version;
    std::shared_ptr<const TopologySnapshot> snapshot;
    EXPECT_TRUE(TopologySnapshot::Create(std::move(state), version, std::string(64, digestByte), snapshot).IsOk());
    return snapshot;
}

class CoordinatorTopologyReaderTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        DS_ASSERT_OK(TopologyKeyHelper::Create("reader-authority", keys_));
        repository_ = std::make_unique<TopologyRepository>(backend_, *keys_);
        reader_ = std::make_unique<TopologyReader>(*repository_);
    }

    Status WriteTopology(uint64_t version)
    {
        TopologyState topology;
        topology.version = version;
        std::string encoded;
        RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(topology, encoded));
        int64_t keyVersion = 0;
        int64_t revision = 0;
        ICoordinatorServiceProxy &proxy = proxy_;
        return proxy.Put(TopologyKey(), encoded, 0, COORDINATOR_NO_VERSION_CHECK, keyVersion, revision);
    }

    std::string TopologyKey() const
    {
        return keys_->TopologyTable() + "/" + TopologyKeyHelper::TopologyKey();
    }

    Status RegisterWatch()
    {
        return backend_.WatchEvents({ { keys_->TopologyTable(), TopologyKeyHelper::TopologyKey(), 0, true } });
    }

    testing::FakeCoordinatorServiceProxy proxy_;
    DsCoordinationBackend backend_{ &proxy_, "127.0.0.1:1" };
    std::unique_ptr<TopologyKeyHelper> keys_;
    std::unique_ptr<TopologyRepository> repository_;
    std::unique_ptr<TopologyReader> reader_;
};

TEST_F(CoordinatorTopologyReaderTest, RewatchReconcilesEqualRevisionFromAnotherCoordinator)
{
    DS_ASSERT_OK(WriteTopology(1));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(reader_->Read(100, snapshot));
    const auto previous = snapshot;
    DS_ASSERT_OK(RegisterWatch());
    size_t resets = 0;
    backend_.SetEventHandler([&](CoordinationEvent &&event) {
        EXPECT_EQ(event.type, CoordinationEventType::RESET);
        ++resets;
    });

    proxy_.ResetCoordinatorStore("coordinator-b");
    DS_ASSERT_OK(WriteTopology(2));
    DS_ASSERT_OK(RegisterWatch());
    ASSERT_EQ(resets, 1U);
    bool unchanged = true;
    DS_ASSERT_OK(reader_->ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_FALSE(unchanged);
    EXPECT_EQ(snapshot->Version(), 2U);
    EXPECT_EQ(snapshot->AuthorityRevision(), previous->AuthorityRevision());
    EXPECT_EQ(snapshot->CoordinatorId(), "coordinator-b");
    EXPECT_EQ(previous->CoordinatorId(), "coordinator-test");
    backend_.SetEventHandler({});
}

TEST_F(CoordinatorTopologyReaderTest, FailedReconciliationRetainsOldEvidenceForRetry)
{
    DS_ASSERT_OK(WriteTopology(1));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(reader_->Read(100, snapshot));
    const auto previous = snapshot;
    proxy_.ResetCoordinatorStore("coordinator-b");
    DS_ASSERT_OK(WriteTopology(2));
    DS_ASSERT_OK(RegisterWatch());
    proxy_.FailRangeForKeyTimes(TopologyKey(), K_RUNTIME_ERROR, 1);
    bool unchanged = true;
    EXPECT_EQ(reader_->ReadIfChanged(100, *snapshot, snapshot, unchanged).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(snapshot, previous);

    DS_ASSERT_OK(reader_->ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_FALSE(unchanged);
    EXPECT_EQ(snapshot->Version(), 2U);
    EXPECT_EQ(snapshot->AuthorityRevision(), previous->AuthorityRevision());
    EXPECT_EQ(snapshot->CoordinatorId(), "coordinator-b");
}

TEST_F(CoordinatorTopologyReaderTest, MembershipReadCannotRelabelTopologyResponseAuthority)
{
    DS_ASSERT_OK(WriteTopology(1));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(reader_->Read(100, snapshot));
    proxy_.ResetCoordinatorStore("coordinator-b");
    DS_ASSERT_OK(WriteTopology(2));
    DS_ASSERT_OK(RegisterWatch());
    proxy_.SetRangeEntryInterceptor([&](const std::string &key) {
        if (key == keys_->MembershipTable() + "/") {
            proxy_.SetRangeEntryInterceptor({});
            proxy_.ResetCoordinatorStore("coordinator-c");
            DS_ASSERT_OK(WriteTopology(3));
        }
    });

    bool unchanged = true;
    DS_ASSERT_OK(reader_->ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_FALSE(unchanged);
    EXPECT_EQ(snapshot->Version(), 2U);
    EXPECT_EQ(snapshot->CoordinatorId(), "coordinator-b");
    DS_ASSERT_OK(reader_->ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_FALSE(unchanged);
    EXPECT_EQ(snapshot->Version(), 3U);
    EXPECT_EQ(snapshot->CoordinatorId(), "coordinator-c");
}

TEST_F(CoordinatorTopologyReaderTest, SameAuthorityAndRevisionKeepCachedSnapshot)
{
    DS_ASSERT_OK(WriteTopology(1));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(reader_->Read(100, snapshot));
    const auto previous = snapshot;
    DS_ASSERT_OK(RegisterWatch());
    bool unchanged = false;
    DS_ASSERT_OK(reader_->ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_TRUE(unchanged);
    EXPECT_EQ(snapshot, previous);
    std::vector<KeyValueEntry> values;
    int64_t revision = 0;
    DS_ASSERT_OK(proxy_.RangeIfChanged(TopologyKey(), snapshot->AuthorityRevision(), snapshot->CoordinatorId(),
                                       values, revision, unchanged));
    EXPECT_TRUE(unchanged);
    EXPECT_TRUE(values.empty());
}

TEST(TopologyReaderTest, ExactReadsCanonicalSnapshotWithAuthorityEvidence)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("reader", keys));
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    std::shared_ptr<const TopologySnapshot> snapshot;

    DS_ASSERT_OK(reader.Read(100, snapshot));
    EXPECT_EQ(snapshot->Version(), 1);
    EXPECT_GT(snapshot->AuthorityRevision(), 0);
    EXPECT_EQ(snapshot->CanonicalDigest().size(), 64);
}

TEST(TopologyReaderTest, TopologyOnlyReadDoesNotReadMembership)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("reader-topology-only", keys));
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    backend.FailNextGetAll();
    std::shared_ptr<const TopologySnapshot> snapshot;

    DS_ASSERT_OK(reader.ReadTopologyOnly(100, snapshot));
    EXPECT_EQ(snapshot->Version(), 1U);
    EXPECT_EQ(snapshot->HostIdsRevision(), 0);
    EXPECT_TRUE(snapshot->HostIds().empty());
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), 0U);
    std::unordered_map<std::string, std::string> hostIds;
    EXPECT_TRUE(repository.ReadHostIds(hostIds).IsError());
}

TEST(TopologyReaderTest, ReadCarriesMembershipHostIds)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("reader-host-ids", keys));
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyState state;
    state.version = 1;
    state.members.push_back(Member{ { std::string(16, 'a'), "127.0.0.1:10001" }, MemberState::ACTIVE, { 0 } });
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);

    MembershipValue membership{ 0, MemberLifecycleState::READY, "host-a", "v1" };
    std::string bytes;
    DS_ASSERT_OK(MembershipValueCodec::Encode(membership, bytes));
    backend.PutBytes(keys->MembershipTable(), "127.0.0.1:10001", std::move(bytes));

    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(reader.Read(100, snapshot));
    const auto &hostIds = snapshot->HostIds();
    ASSERT_EQ(hostIds.size(), 1UL);
    EXPECT_EQ(hostIds.at("127.0.0.1:10001"), "host-a");
}

TEST(TopologyReaderTest, MembershipProjectionRecoversAndChangesWithoutTopologyAdvance)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("reader-host-recovery", keys));
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyState topology;
    topology.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    std::string bytes;
    DS_ASSERT_OK(MembershipValueCodec::Encode({ 0, MemberLifecycleState::READY, "host-a", "v1" }, bytes));
    backend.PutBytes(keys->MembershipTable(), "127.0.0.1:10001", bytes);
    backend.FailNextGetAll();
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(reader.Read(100, snapshot));
    EXPECT_EQ(snapshot->HostIdsRevision(), 0);
    TopologySnapshotState published;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(published.Publish(snapshot, outcome));

    bool unchanged = true;
    DS_ASSERT_OK(reader.ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_FALSE(unchanged);
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    DS_ASSERT_OK(published.Load(snapshot));
    EXPECT_EQ(snapshot->HostIds().at("127.0.0.1:10001"), "host-a");
    const auto first = snapshot;

    DS_ASSERT_OK(MembershipValueCodec::Encode({ 0, MemberLifecycleState::READY, "host-b", "v1" }, bytes));
    backend.PutBytes(keys->MembershipTable(), "127.0.0.1:10001", bytes);
    DS_ASSERT_OK(reader.ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_FALSE(unchanged);
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    DS_ASSERT_OK(published.Load(snapshot));
    EXPECT_EQ(snapshot->Version(), first->Version());
    EXPECT_EQ(snapshot->CanonicalDigest(), first->CanonicalDigest());
    EXPECT_EQ(snapshot->HostIds().at("127.0.0.1:10001"), "host-b");
    EXPECT_GT(snapshot->HostIdsRevision(), first->HostIdsRevision());
    EXPECT_EQ(first->HostIds().at("127.0.0.1:10001"), "host-a");

    backend.FailNextGetAll();
    DS_ASSERT_OK(reader.ReadIfChanged(100, *snapshot, snapshot, unchanged));
    EXPECT_TRUE(unchanged);
    EXPECT_EQ(snapshot->HostIds().at("127.0.0.1:10001"), "host-b");
}

TEST(TopologySnapshotStateTest, HostIdFailurePreservesOnlyUnchangedMemberIdentities)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 100 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot,
                                          { { "127.0.0.1:1", "host-a" }, { "127.0.0.1:2", "host-b" } }, 10));
    TopologySnapshotState published;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    topology.version = 2;
    topology.members[1].identity.id = std::string(16, 'c');
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), snapshot));
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    DS_ASSERT_OK(published.Load(snapshot));
    EXPECT_EQ(snapshot->Version(), 2U);
    EXPECT_EQ(snapshot->HostIds().size(), 1U);
    EXPECT_EQ(snapshot->HostIds().at("127.0.0.1:1"), "host-a");

    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), snapshot, {}, 11));
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    DS_ASSERT_OK(published.Load(snapshot));
    EXPECT_TRUE(snapshot->HostIds().empty());
    EXPECT_EQ(snapshot->HostIdsRevision(), 11);

    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), snapshot,
                                          { { "127.0.0.1:1", "host-after-recovery" } }, 1));
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    DS_ASSERT_OK(published.Load(snapshot));
    EXPECT_EQ(snapshot->HostIdsRevision(), 1);
    EXPECT_EQ(snapshot->HostIds().at("127.0.0.1:1"), "host-after-recovery");
}

TEST(TopologySnapshotStateTest, IdempotentPublicationRefreshesAuthorityAndRetainsHostIds)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 7, std::string(64, 'a'), snapshot,
                                          { { "127.0.0.1:1", "host-a" } }, 8, "coordinator-a"));
    const auto previous = snapshot;
    TopologySnapshotState published;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot, {}, 0, "coordinator-b"));
    DS_ASSERT_OK(published.Publish(snapshot, outcome));
    EXPECT_EQ(outcome, SnapshotUpdateOutcome::IDEMPOTENT);
    DS_ASSERT_OK(published.Load(snapshot));
    EXPECT_EQ(snapshot->CoordinatorId(), "coordinator-b");
    EXPECT_EQ(snapshot->AuthorityRevision(), 1);
    EXPECT_EQ(snapshot->HostIds(), previous->HostIds());
    EXPECT_EQ(previous->CoordinatorId(), "coordinator-a");
    EXPECT_EQ(previous->AuthorityRevision(), 7);
}

TEST(TopologySnapshotStateTest, RejectsGapRollbackAndSameVersionConflict)
{
    TopologySnapshotState state;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(state.Publish(MakeSnapshot(1, 'a'), outcome));
    EXPECT_EQ(outcome, SnapshotUpdateOutcome::PUBLISHED);
    DS_ASSERT_OK(state.Publish(MakeSnapshot(1, 'a'), outcome));
    EXPECT_EQ(outcome, SnapshotUpdateOutcome::IDEMPOTENT);
    EXPECT_EQ(state.Publish(MakeSnapshot(1, 'b'), outcome).GetCode(), K_INVALID);
    EXPECT_EQ(outcome, SnapshotUpdateOutcome::CONFLICT);
    EXPECT_EQ(state.Publish(MakeSnapshot(3, 'c'), outcome).GetCode(), K_INVALID);
    EXPECT_EQ(outcome, SnapshotUpdateOutcome::VERSION_GAP);
    DS_ASSERT_OK(state.PublishAfterFullRebuild(MakeSnapshot(3, 'c')));
    EXPECT_EQ(state.Publish(MakeSnapshot(2, 'd'), outcome).GetCode(), K_INVALID);
    EXPECT_EQ(outcome, SnapshotUpdateOutcome::VERSION_ROLLBACK);
}

TEST(TopologySnapshotStateTest, ThreadCacheDoesNotRetainReplacedSnapshot)
{
    TopologySnapshotState state;
    SnapshotUpdateOutcome outcome;
    auto first = MakeSnapshot(1, 'a');
    std::weak_ptr<const TopologySnapshot> oldGeneration = first;
    DS_ASSERT_OK(state.Publish(first, outcome));
    first.reset();
    std::shared_ptr<const TopologySnapshot> loaded;
    DS_ASSERT_OK(state.Load(loaded));
    loaded.reset();

    DS_ASSERT_OK(state.Publish(MakeSnapshot(2, 'b'), outcome));

    EXPECT_TRUE(oldGeneration.expired());
}

TEST(TopologySnapshotStateTest, WaitForVersionWakesWhenRequiredSnapshotIsPublished)
{
    TopologySnapshotState state;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(state.Publish(MakeSnapshot(1, 'a'), outcome));
    std::promise<void> entered;
    auto waiter = std::async(std::launch::async, [&] {
        entered.set_value();
        std::shared_ptr<const TopologySnapshot> observed;
        auto rc = state.WaitForVersion(2, std::chrono::steady_clock::now() + std::chrono::seconds(1), observed);
        return std::make_pair(rc, observed);
    });
    entered.get_future().wait();

    DS_ASSERT_OK(state.Publish(MakeSnapshot(2, 'b'), outcome));

    auto [status, observed] = waiter.get();
    DS_ASSERT_OK(status);
    ASSERT_NE(observed, nullptr);
    EXPECT_EQ(observed->Version(), 2U);
}

TEST(TopologySnapshotStateTest, WaitForVersionReturnsRetryableTimeout)
{
    TopologySnapshotState state;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(state.Publish(MakeSnapshot(1, 'a'), outcome));
    std::shared_ptr<const TopologySnapshot> observed;

    EXPECT_EQ(state.WaitForVersion(2, std::chrono::steady_clock::now(), observed).GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(observed, nullptr);
}

TEST(TopologySnapshotStateTest, ScaleOutHandoffCompletionIsEpochBoundAndRangeScoped)
{
    TopologyState topology;
    topology.version = 2;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 100 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    TopologySnapshotState state;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(state.Publish(snapshot, outcome));
    TopologyExecutionFence fence;
    fence.phase = TopologyCallbackPhase::SCALE_OUT;
    fence.batchEpoch = 2;
    fence.ranges = { { 10, 20 }, { 30, 40 } };

    state.RecordScaleOutHandoffCompletion(fence);

    EXPECT_TRUE(state.IsScaleOutHandoffComplete(2, 10));
    EXPECT_TRUE(state.IsScaleOutHandoffComplete(2, 35));
    EXPECT_FALSE(state.IsScaleOutHandoffComplete(2, 25));
    EXPECT_FALSE(state.IsScaleOutHandoffComplete(3, 35));
    fence.ranges = { { 0, std::numeric_limits<uint32_t>::max() } };
    state.RecordScaleOutHandoffCompletion(fence);
    EXPECT_TRUE(state.IsScaleOutHandoffComplete(2, std::numeric_limits<uint32_t>::max()));

    topology.version = 3;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 3 };
    topology.members[1].state = MemberState::FAILED;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 3, std::string(64, 'b'), snapshot));
    DS_ASSERT_OK(state.Publish(snapshot, outcome));
    EXPECT_FALSE(state.IsScaleOutHandoffComplete(2, 35));
}

}  // namespace
}  // namespace datasystem::cluster
