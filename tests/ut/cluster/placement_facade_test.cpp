/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Foreground immutable cluster placement facade tests.
 */
#include "datasystem/cluster/routing/placement_facade.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <future>
#include <limits>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

class FailingProspectiveAlgorithm final : public IRoutingAlgorithm {
public:
    ~FailingProspectiveAlgorithm() override = default;

    TopologyAlgorithmId GetId() const override
    {
        return "failing-prospective";
    }

    uint32_t Hash(std::string_view) const noexcept override
    {
        return 0;
    }

    Status LocateOwner(const TopologySnapshot &snapshot, uint32_t, const Member *&owner) const override
    {
        owner = snapshot.CommittedMembers().front();
        return Status::OK();
    }

    Status LocateProspectiveOwner(const TopologySnapshot &, uint32_t, const Member *&) const override
    {
        return Status(K_RUNTIME_ERROR, "planned prospective placement failure");
    }
};

class PartiallyFailingRoutingAlgorithm final : public IRoutingAlgorithm {
public:
    PartiallyFailingRoutingAlgorithm()
    {
        owner_.identity = { std::string(16, 'a'), "127.0.0.1:1" };
        owner_.state = MemberState::ACTIVE;
    }

    ~PartiallyFailingRoutingAlgorithm() override = default;

    TopologyAlgorithmId GetId() const override
    {
        return "partially-failing";
    }

    uint32_t Hash(std::string_view key) const noexcept override
    {
        return key == "found" ? 1U : 0U;
    }

    Status LocateOwner(const TopologySnapshot &, uint32_t token, const Member *&owner) const override
    {
        if (token != 1U) {
            return Status(K_NOT_FOUND, "planned missing placement");
        }
        owner = &owner_;
        return Status::OK();
    }

    Status LocateProspectiveOwner(const TopologySnapshot &snapshot, uint32_t token, const Member *&owner) const override
    {
        return LocateOwner(snapshot, token, owner);
    }

private:
    Member owner_;
};

class FirstLocateHookRoutingAlgorithm final : public IRoutingAlgorithm {
public:
    explicit FirstLocateHookRoutingAlgorithm(std::function<void()> firstLocateHook)
        : firstLocateHook_(std::move(firstLocateHook))
    {
    }

    ~FirstLocateHookRoutingAlgorithm() override = default;

    TopologyAlgorithmId GetId() const override
    {
        return delegate_.GetId();
    }

    uint32_t Hash(std::string_view key) const noexcept override
    {
        return delegate_.Hash(key);
    }

    Status LocateOwner(const TopologySnapshot &snapshot, uint32_t token, const Member *&owner) const override
    {
        if (!firstLocateCalled_) {
            firstLocateCalled_ = true;
            firstLocateHook_();
        }
        return delegate_.LocateOwner(snapshot, token, owner);
    }

    Status LocateProspectiveOwner(const TopologySnapshot &snapshot, uint32_t token,
                                  const Member *&owner) const override
    {
        return delegate_.LocateProspectiveOwner(snapshot, token, owner);
    }

private:
    std::function<void()> firstLocateHook_;
    HashAlgorithm delegate_;
    mutable bool firstLocateCalled_{ false };
};

class DecimalTokenRoutingAlgorithm final : public IRoutingAlgorithm {
public:
    ~DecimalTokenRoutingAlgorithm() override = default;

    TopologyAlgorithmId GetId() const override
    {
        return "decimal-token";
    }

    uint32_t Hash(std::string_view placementKey) const noexcept override
    {
        uint32_t token = 0;
        for (char digit : placementKey) {
            token = token * 10U + static_cast<uint32_t>(digit - '0');
        }
        return token;
    }

    Status LocateOwner(const TopologySnapshot &snapshot, uint32_t token, const Member *&owner) const override
    {
        return delegate_.LocateOwner(snapshot, token, owner);
    }

    Status LocateProspectiveOwner(const TopologySnapshot &snapshot, uint32_t token,
                                  const Member *&owner) const override
    {
        return delegate_.LocateProspectiveOwner(snapshot, token, owner);
    }

private:
    HashAlgorithm delegate_;
};

TopologyState MakeDrainingRingTopology(uint64_t version, MemberState thirdState)
{
    TopologyState topology;
    topology.version = version;
    topology.clusterHasInit = true;
    topology.activeBatch = thirdState == MemberState::LEAVING
                               ? std::optional<ActiveBatch>(ActiveBatch{ TopologyChangeType::SCALE_IN, version })
                               : std::nullopt;
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 50, 150 } },
        Member{ { std::string(16, 'c'), "127.0.0.1:3" }, thirdState, { 75, 200 } },
    };
    return topology;
}

Status PublishForTest(TopologySnapshotState &snapshots, const TopologyState &state, char digest)
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(state, static_cast<int64_t>(state.version), std::string(64, digest),
                                              snapshot));
    SnapshotUpdateOutcome outcome;
    return snapshots.Publish(std::move(snapshot), outcome);
}

const std::vector<std::string> &DrainProbeKeys()
{
    static const std::vector<std::string> keys{ "0", "9", "10", "30", "49", "50", "60", "74", "75", "85", "99",
                                                "100", "125", "149", "150", "175", "199", "200", "300" };
    return keys;
}

TEST(PlacementFacadeTest, UsesOneSnapshotForTokenBatchAndRedirectDecisions)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 50, 150 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    PlacementDecision placement;
    DS_ASSERT_OK(facade.LocateToken(40, placement));
    EXPECT_EQ(placement.committedOwnerAddress, "127.0.0.1:2");
    RedirectDecision redirect;
    DS_ASSERT_OK(facade.EvaluateRedirect("key", redirect));
    EXPECT_EQ(redirect.topologyVersion, 1);
    std::vector<std::string_view> keys{ "a", "b", "c" };
    BatchPlacementDecision batch;
    DS_ASSERT_OK(facade.LocateBatch(keys, batch));
    EXPECT_EQ(batch.items.size(), keys.size());
    EXPECT_EQ(batch.topologyVersion, 1);
}

TEST(PlacementFacadeTest, KeepsPerKeyFailuresWithoutReloadingTheBatchSnapshot)
{
    TopologyState topology;
    topology.version = 1;
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    PartiallyFailingRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    BatchPlacementDecision batch;

    DS_ASSERT_OK(facade.LocateBatch({ "found", "missing" }, batch));
    ASSERT_EQ(batch.items.size(), 2U);
    EXPECT_TRUE(batch.items[0].status.IsOk());
    EXPECT_EQ(batch.items[0].decision.committedOwnerAddress, "127.0.0.1:1");
    EXPECT_EQ(batch.items[1].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(batch.items[1].decision.committedOwnerAddress.empty());
    EXPECT_EQ(batch.topologyVersion, 1U);
}

TEST(PlacementFacadeTest, AcceptsLargestForegroundBatchAndRejectsLargerBatch)
{
    constexpr size_t kLargestForegroundBatch = 100'000;
    TopologyState topology;
    topology.version = 1;
    topology.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    std::vector<std::string_view> keys(kLargestForegroundBatch, "key");
    BatchPlacementDecision batch;

    DS_ASSERT_OK(facade.LocateBatch(keys, batch));
    EXPECT_EQ(batch.items.size(), kLargestForegroundBatch);
    keys.emplace_back("key");
    EXPECT_EQ(facade.LocateBatch(keys, batch).GetCode(), K_INVALID);
}

TEST(PlacementFacadeTest, PreservesEveryOutputBeforeFirstSnapshot)
{
    TopologySnapshotState snapshots;
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    PlacementDecision single{ 99, "sentinel-owner" };
    BatchPlacementDecision batch{ 99, { BatchPlacementItem{ single, Status::OK() } } };
    RedirectDecision redirect{ 99, RedirectAction::REDIRECT, "sentinel-owner", "sentinel-target" };
    BatchRedirectDecision redirectBatch{ 99, { redirect } };
    bool isLocal = true;

    EXPECT_EQ(facade.Locate("key", single).GetCode(), K_NOT_READY);
    EXPECT_EQ(single.topologyVersion, 99);
    EXPECT_EQ(single.committedOwnerAddress, "sentinel-owner");
    EXPECT_EQ(facade.LocateBatch({ "key" }, batch).GetCode(), K_NOT_READY);
    EXPECT_EQ(batch.topologyVersion, 99);
    ASSERT_EQ(batch.items.size(), 1);
    EXPECT_EQ(batch.items.front().decision.committedOwnerAddress, "sentinel-owner");
    EXPECT_EQ(facade.EvaluateRedirect("key", redirect).GetCode(), K_NOT_READY);
    EXPECT_EQ(redirect.redirectTargetAddress, "sentinel-target");
    EXPECT_EQ(facade.EvaluateRedirectBatch({ "key" }, redirectBatch).GetCode(), K_NOT_READY);
    EXPECT_EQ(redirectBatch.topologyVersion, 99);
    ASSERT_EQ(redirectBatch.decisions.size(), 1);
    EXPECT_EQ(redirectBatch.decisions.front().redirectTargetAddress, "sentinel-target");
    EXPECT_EQ(facade.IsLocalOwner("key", isLocal).GetCode(), K_NOT_READY);
    EXPECT_TRUE(isLocal);
}

TEST(PlacementFacadeTest, WaitsConcurrentWritesOnScaleOutTransferRanges)
{
    TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } },
        Member{
            { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { std::numeric_limits<uint32_t>::max() } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    RedirectDecision decision;
    DS_ASSERT_OK(facade.EvaluateRedirect("key", decision));
    EXPECT_EQ(decision.action, RedirectAction::WAIT);
    EXPECT_EQ(decision.committedOwnerAddress, "127.0.0.1:1");
    EXPECT_EQ(decision.redirectTargetAddress, "127.0.0.1:2");
    EXPECT_EQ(decision.GetRedirectTargetAddress(), "127.0.0.1:2");
}

TEST(PlacementFacadeTest, RedirectsMissingScaleInMetadataToProspectiveOwner)
{
    TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 2 };
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::LEAVING,
                { std::numeric_limits<uint32_t>::max() } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 0 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    const Member *prospective = nullptr;
    DS_ASSERT_OK(algorithm.LocateProspectiveOwner(*snapshot, algorithm.Hash("key"), prospective));
    ASSERT_NE(prospective, nullptr);
    EXPECT_EQ(prospective->identity.address, "127.0.0.1:2");

    RedirectDecision decision;
    DS_ASSERT_OK(facade.EvaluateRedirect("key", decision));
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.committedOwnerAddress, "127.0.0.1:1");
    EXPECT_EQ(decision.redirectTargetAddress, "127.0.0.1:2");
    EXPECT_EQ(decision.GetRedirectTargetAddress(), "127.0.0.1:2");

    bool isLocal = false;
    DS_ASSERT_OK(facade.IsLocalOwner("key", isLocal));
    EXPECT_TRUE(isLocal);
}

TEST(PlacementFacadeTest, PreservesRedirectOutputWhenProspectivePlacementFails)
{
    TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 1 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    FailingProspectiveAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    RedirectDecision decision{ 99, RedirectAction::REDIRECT, "sentinel-owner", "sentinel-target" };

    EXPECT_EQ(facade.EvaluateRedirect("key", decision).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(decision.topologyVersion, 99);
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.committedOwnerAddress, "sentinel-owner");
    EXPECT_EQ(decision.redirectTargetAddress, "sentinel-target");
}

TEST(PlacementFacadeTest, BatchUsesOneSnapshotWhenANewerVersionPublishesDuringRouting)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } } };
    std::shared_ptr<const TopologySnapshot> first;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), first));
    topology.version = 2;
    std::shared_ptr<const TopologySnapshot> second;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), second));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(first, outcome));
    FirstLocateHookRoutingAlgorithm algorithm([&snapshots, second] {
        SnapshotUpdateOutcome published;
        EXPECT_TRUE(snapshots.Publish(second, published).IsOk());
    });
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    BatchPlacementDecision batch;

    DS_ASSERT_OK(facade.LocateBatch({ "a", "b", "c" }, batch));
    ASSERT_EQ(batch.items.size(), 3);
    EXPECT_EQ(batch.topologyVersion, 1);
    EXPECT_TRUE(std::all_of(batch.items.begin(), batch.items.end(), [](const BatchPlacementItem &item) {
        return item.status.IsOk() && item.decision.topologyVersion == 1;
    }));
}

TEST(PlacementFacadeTest, RedirectBatchUsesOneSnapshotWhenANewerVersionPublishesDuringRouting)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } } };
    std::shared_ptr<const TopologySnapshot> first;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), first));
    topology.version = 2;
    std::shared_ptr<const TopologySnapshot> second;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), second));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(first, outcome));
    FirstLocateHookRoutingAlgorithm algorithm([&snapshots, second] {
        SnapshotUpdateOutcome published;
        EXPECT_TRUE(snapshots.Publish(second, published).IsOk());
    });
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    BatchRedirectDecision batch;

    DS_ASSERT_OK(facade.EvaluateRedirectBatch({ "a", "b", "c" }, batch));
    ASSERT_EQ(batch.decisions.size(), 3);
    EXPECT_EQ(batch.topologyVersion, 1);
    EXPECT_TRUE(std::all_of(batch.decisions.begin(), batch.decisions.end(),
                            [](const RedirectDecision &item) { return item.topologyVersion == 1; }));
}

TEST(PlacementFacadeTest, ConcurrentPublishDoesNotMixBatchSnapshotVersions)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } } };
    std::shared_ptr<const TopologySnapshot> first;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), first));
    topology.version = 2;
    std::shared_ptr<const TopologySnapshot> second;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), second));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(first, outcome));
    std::promise<void> firstLocateStarted;
    auto firstLocateSignal = firstLocateStarted.get_future();
    std::promise<void> continueRouting;
    auto continueSignal = continueRouting.get_future().share();
    FirstLocateHookRoutingAlgorithm algorithm([&firstLocateStarted, continueSignal] {
        firstLocateStarted.set_value();
        continueSignal.wait();
    });
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    BatchPlacementDecision batch;
    auto routeStatus = std::async(std::launch::async, [&facade, &batch] {
        return facade.LocateBatch({ "a", "b", "c" }, batch);
    });

    const bool routingPaused = firstLocateSignal.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    auto publishStatus = routingPaused ? snapshots.Publish(second, outcome)
                                       : Status(K_RUNTIME_ERROR, "routing did not reach the test barrier");
    continueRouting.set_value();
    auto status = routeStatus.get();

    ASSERT_TRUE(routingPaused);
    DS_ASSERT_OK(publishStatus);
    DS_ASSERT_OK(status);
    ASSERT_EQ(batch.items.size(), 3U);
    EXPECT_EQ(batch.topologyVersion, 1U);
    EXPECT_TRUE(std::all_of(batch.items.begin(), batch.items.end(), [](const BatchPlacementItem &item) {
        return item.status.IsOk() && item.decision.topologyVersion == 1;
    }));
}

TEST(PlacementFacadeTest, SurvivingOwnerMatchesLocateWhenOwnerActive)
{
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, MakeDrainingRingTopology(1, MemberState::ACTIVE), 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    for (const auto &key : DrainProbeKeys()) {
        PlacementDecision committed;
        PlacementDecision surviving;
        DS_ASSERT_OK(facade.Locate(key, committed));
        DS_ASSERT_OK(facade.LocateSurvivingOwner(key, surviving));
        EXPECT_EQ(surviving.committedOwnerAddress, committed.committedOwnerAddress) << key;
    }
}

TEST(PlacementFacadeTest, FallsBackToProspectiveDuringScaleInDrain)
{
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, MakeDrainingRingTopology(2, MemberState::LEAVING), 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(snapshots.Load(snapshot));

    const Member *prospective = nullptr;
    DS_ASSERT_OK(algorithm.LocateProspectiveOwner(*snapshot, 75, prospective));
    ASSERT_NE(prospective, nullptr);
    PlacementDecision decision;
    DS_ASSERT_OK(facade.Locate("75", decision));
    ASSERT_EQ(decision.committedOwnerAddress, "127.0.0.1:3");
    DS_ASSERT_OK(facade.LocateSurvivingOwner("75", decision));
    EXPECT_EQ(decision.committedOwnerAddress, prospective->identity.address);
    EXPECT_EQ(decision.topologyVersion, 2U);
}

TEST(PlacementFacadeTest, SurvivingOwnerEqualsPostBatchCommittedRing)
{
    TopologyState draining = MakeDrainingRingTopology(2, MemberState::LEAVING);
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, draining, 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    std::vector<std::string> survivingOwners;
    survivingOwners.reserve(DrainProbeKeys().size());
    for (const auto &key : DrainProbeKeys()) {
        PlacementDecision decision;
        DS_ASSERT_OK(facade.LocateSurvivingOwner(key, decision));
        survivingOwners.emplace_back(decision.committedOwnerAddress);
    }
    PlacementDecision committed;
    DS_ASSERT_OK(facade.Locate("75", committed));
    ASSERT_EQ(committed.committedOwnerAddress, "127.0.0.1:3");
    PlacementDecision moved;
    DS_ASSERT_OK(facade.LocateSurvivingOwner("75", moved));
    EXPECT_NE(moved.committedOwnerAddress, "127.0.0.1:3");

    TopologyState finalized = draining;
    finalized.version = 3;
    finalized.activeBatch.reset();
    finalized.members.pop_back();
    DS_ASSERT_OK(PublishForTest(snapshots, finalized, 'b'));
    size_t index = 0;
    for (const auto &key : DrainProbeKeys()) {
        PlacementDecision finalPlacement;
        DS_ASSERT_OK(facade.Locate(key, finalPlacement));
        EXPECT_EQ(survivingOwners[index++], finalPlacement.committedOwnerAddress) << key;
    }
}

TEST(PlacementFacadeTest, SurvivingOwnerBatchMatchesPostBatchCommittedRing)
{
    TopologyState draining = MakeDrainingRingTopology(2, MemberState::LEAVING);
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, draining, 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    std::vector<std::string_view> keys(DrainProbeKeys().begin(), DrainProbeKeys().end());
    BatchPlacementDecision batch;
    DS_ASSERT_OK(facade.LocateSurvivingOwnerBatch(keys, batch));
    ASSERT_EQ(batch.items.size(), keys.size());
    for (const auto &item : batch.items) {
        ASSERT_OK(item.status);
        EXPECT_EQ(item.decision.topologyVersion, 2U);
    }

    TopologyState finalized = draining;
    finalized.version = 3;
    finalized.activeBatch.reset();
    finalized.members.pop_back();
    DS_ASSERT_OK(PublishForTest(snapshots, finalized, 'b'));
    size_t index = 0;
    for (const auto &key : DrainProbeKeys()) {
        PlacementDecision finalPlacement;
        DS_ASSERT_OK(facade.Locate(key, finalPlacement));
        EXPECT_EQ(batch.items[index++].decision.committedOwnerAddress, finalPlacement.committedOwnerAddress) << key;
    }
}

TEST(PlacementFacadeTest, SurvivingOwnerBatchRejectsInvalidBatchSize)
{
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, MakeDrainingRingTopology(1, MemberState::ACTIVE), 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    std::vector<std::string_view> tooManyKeys(PlacementFacade::kMaxBatchKeys + 1, "k");
    BatchPlacementDecision batch;
    ASSERT_EQ(facade.LocateSurvivingOwnerBatch(tooManyKeys, batch).GetCode(), K_INVALID);
}

TEST(PlacementFacadeTest, SurvivingOwnerBatchKeepsPerKeyFailures)
{
    TopologyState topology;
    topology.version = 1;
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    PartiallyFailingRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");
    BatchPlacementDecision batch;

    DS_ASSERT_OK(facade.LocateSurvivingOwnerBatch({ "found", "missing" }, batch));
    ASSERT_EQ(batch.items.size(), 2U);
    EXPECT_TRUE(batch.items[0].status.IsOk());
    EXPECT_EQ(batch.items[0].decision.committedOwnerAddress, "127.0.0.1:1");
    EXPECT_EQ(batch.items[1].status.GetCode(), K_NOT_FOUND);
    EXPECT_TRUE(batch.items[1].decision.committedOwnerAddress.empty());
    EXPECT_EQ(batch.topologyVersion, 1U);
}

TEST(PlacementFacadeTest, DegenerateAllLeavingKeepsCommittedAnswer)
{
    TopologyState topology = MakeDrainingRingTopology(2, MemberState::LEAVING);
    for (auto &member : topology.members) {
        member.state = MemberState::LEAVING;
    }
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, topology, 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    for (const auto &key : DrainProbeKeys()) {
        PlacementDecision committed;
        PlacementDecision surviving;
        DS_ASSERT_OK(facade.Locate(key, committed));
        DS_ASSERT_OK(facade.LocateSurvivingOwner(key, surviving));
        EXPECT_EQ(surviving.committedOwnerAddress, committed.committedOwnerAddress) << key;
    }
}

TEST(PlacementFacadeTest, KeepsPreLeavingOwnerWithoutBatch)
{
    TopologySnapshotState snapshots;
    DS_ASSERT_OK(PublishForTest(snapshots, MakeDrainingRingTopology(1, MemberState::PRE_LEAVING), 'a'));
    DecimalTokenRoutingAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm, "127.0.0.1:1");

    PlacementDecision decision;
    DS_ASSERT_OK(facade.LocateSurvivingOwner("75", decision));
    EXPECT_EQ(decision.committedOwnerAddress, "127.0.0.1:3");
    DS_ASSERT_OK(facade.LocateSurvivingOwner("50", decision));
    EXPECT_EQ(decision.committedOwnerAddress, "127.0.0.1:2");
}

}  // namespace
}  // namespace datasystem::cluster
