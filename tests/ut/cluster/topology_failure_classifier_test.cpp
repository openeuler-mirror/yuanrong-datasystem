/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Controller-local authoritative membership failure classification tests.
 */
#include "datasystem/cluster/control/topology_failure_classifier.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

TEST(TopologyFailureClassifierTest, ConfirmsMultipleCommittedFailuresOnlyAfterContinuousAbsence)
{
    TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                      Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyFailureClassifier classifier(std::chrono::seconds(5));
    const auto now = std::chrono::steady_clock::now();
    FailureClassification result;
    DS_ASSERT_OK(classifier.Observe(*snapshot, {}, now, result));
    EXPECT_TRUE(result.confirmedFailure.empty());
    DS_ASSERT_OK(classifier.Observe(*snapshot, {}, now + std::chrono::seconds(5), result));
    EXPECT_EQ(result.confirmedFailure.size(), 2);
}

TEST(TopologyFailureClassifierTest, RemovesInitialAndJoiningImmediatelyAndClearsTimerOnReappearance)
{
    TopologyState state;
    state.version = 2;
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                      Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 2 } },
                      Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::INITIAL, {} } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyFailureClassifier classifier(std::chrono::seconds(5));
    FailureClassification result;
    DS_ASSERT_OK(classifier.Observe(*snapshot, {}, std::chrono::steady_clock::now(), result));
    EXPECT_EQ(result.removeJoining.size(), 1);
    EXPECT_EQ(result.removeInitial.size(), 1);
}

TEST(TopologyFailureClassifierTest, ReplanIncludesExistingFailedMembersWithNewConfirmedFailure)
{
    TopologyState state;
    state.version = 4;
    state.clusterHasInit = true;
    state.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 4 };
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::FAILED, { 1 } },
                      Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } },
                      Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::ACTIVE, { 3 } } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyFailureClassifier classifier(std::chrono::seconds(5));
    const auto now = std::chrono::steady_clock::now();
    FailureClassification result;
    const std::vector<MembershipRecord> memberships{ { "127.0.0.1:1", MemberLifecycleState::READY, 0, "" },
                                                     { "127.0.0.1:3", MemberLifecycleState::READY, 0, "" } };
    DS_ASSERT_OK(classifier.Observe(*snapshot, memberships, now, result));
    DS_ASSERT_OK(classifier.Observe(*snapshot, memberships, now + std::chrono::seconds(5), result));
    ASSERT_EQ(result.confirmedFailure.size(), 2);
    EXPECT_EQ(result.confirmedFailure.front().address, "127.0.0.1:1");
    EXPECT_EQ(result.confirmedFailure.back().address, "127.0.0.1:2");
}

TEST(TopologyFailureClassifierTest, ExcludesUnreadableIntervalsFromMissingBudget)
{
    TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    std::shared_ptr<const TopologySnapshot> topology;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), topology));
    TopologyFailureClassifier classifier(std::chrono::seconds(5));
    FailureClassification classification;
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));

    DS_ASSERT_OK(classifier.Observe(*topology, {}, start, classification));
    classifier.Pause(start + std::chrono::seconds(2));
    DS_ASSERT_OK(classifier.Observe(*topology, {}, start + std::chrono::seconds(20), classification));
    EXPECT_TRUE(classification.confirmedFailure.empty());
    DS_ASSERT_OK(classifier.Observe(*topology, {}, start + std::chrono::seconds(23), classification));
    ASSERT_EQ(classification.confirmedFailure.size(), 1);
    EXPECT_EQ(classification.confirmedFailure.front().address, "127.0.0.1:1");
}

}  // namespace
}  // namespace datasystem::cluster
