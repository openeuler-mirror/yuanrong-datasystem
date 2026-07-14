/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology one-shot reader and Snapshot publication tests.
 */
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

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

}  // namespace
}  // namespace datasystem::cluster
