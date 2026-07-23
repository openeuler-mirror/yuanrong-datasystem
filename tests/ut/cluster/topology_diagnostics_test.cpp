/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology diagnostic formatting tests.
 */
#include "datasystem/cluster/model/topology_diagnostics.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "gtest/gtest.h"

namespace datasystem::cluster {
namespace {
constexpr size_t CURRENT_OPERATIONAL_MEMBER_COUNT = 32;

std::vector<MemberIdentity> MakeIdentities(size_t count)
{
    std::vector<MemberIdentity> identities;
    identities.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        identities.push_back({ "id-" + std::to_string(index), "worker-" + std::to_string(index) });
    }
    return identities;
}

std::shared_ptr<const TopologySnapshot> MakeScaleOutSnapshot()
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 2;
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    state.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 200 } },
        Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::ACTIVE, { 300 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    EXPECT_TRUE(TopologySnapshot::Create(std::move(state), 2, std::string(64, 'a'), snapshot).IsOk());
    return snapshot;
}

std::shared_ptr<const TopologySnapshot> MakeScaleInSnapshot()
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 3;
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 3 };
    state.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::LEAVING, { 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 300 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    EXPECT_TRUE(TopologySnapshot::Create(std::move(state), 3, std::string(64, 'b'), snapshot).IsOk());
    return snapshot;
}
}  // namespace

TEST(TopologyDiagnosticsTest, PrintsAllCurrentOperationalMembersByDefault)
{
    const auto members = MakeIdentities(CURRENT_OPERATIONAL_MEMBER_COUNT);
    const auto output = MemberIdentitySample(members);
    EXPECT_NE(output.find("worker-31:id-31"), std::string::npos);
    EXPECT_EQ(output.find("truncated"), std::string::npos);
}

TEST(TopologyDiagnosticsTest, PrintsAllCurrentMembershipsByDefault)
{
    std::vector<MembershipRecord> memberships;
    for (size_t index = 0; index < CURRENT_OPERATIONAL_MEMBER_COUNT; ++index) {
        memberships.push_back(
            { "worker-" + std::to_string(index), MemberLifecycleState::READY, static_cast<int64_t>(index), "" });
    }
    const auto output = MembershipSample(memberships);
    EXPECT_NE(output.find("worker-31:READY"), std::string::npos);
    EXPECT_EQ(output.find("truncated"), std::string::npos);
}

TEST(TopologyDiagnosticsTest, MarksMemberOutputTruncatedAboveOperationalSize)
{
    const auto members = MakeIdentities(CURRENT_OPERATIONAL_MEMBER_COUNT + 1);
    const auto output = MemberIdentitySample(members);
    EXPECT_NE(output.find("worker-31:id-31"), std::string::npos);
    EXPECT_EQ(output.find("worker-32:id-32"), std::string::npos);
    EXPECT_NE(output.find("truncated,total=33"), std::string::npos);
}

TEST(TopologyDiagnosticsTest, PrintsRangesForAllCurrentOperationalMembers)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 1;
    for (size_t index = 0; index < CURRENT_OPERATIONAL_MEMBER_COUNT; ++index) {
        auto id = std::string(16, '\0');
        id.back() = static_cast<char>(index);
        state.members.push_back(
            Member{ { std::move(id), "127.0.0.1:" + std::to_string(index + 1) }, MemberState::ACTIVE,
                    { static_cast<uint32_t>(index) } });
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    ASSERT_TRUE(TopologySnapshot::Create(std::move(state), 1, std::string(64, 'c'), snapshot).IsOk());

    const auto output = TopologyRingForLog(*snapshot, TopologyRingView::COMMITTED);
    EXPECT_NE(output.find("address=127.0.0.1:32"), std::string::npos);
    EXPECT_EQ(output.find("truncated"), std::string::npos);
}

TEST(TopologyDiagnosticsTest, FormatsClosedTaskRangesAndParticipants)
{
    TopologyExecutionFence fence;
    fence.executor = { "executor-id", "127.0.0.1:1" };
    fence.source = MemberIdentity{ "source-id", "127.0.0.1:2" };
    fence.target = MemberIdentity{ "target-id", "127.0.0.1:3" };
    fence.ranges = { TokenRange{ 0, 9 }, TokenRange{ 20, 29 } };
    const std::string participantScope =
        "executor=127.0.0.1:1 executor_id_prefix=executor-id source=127.0.0.1:2 "
        "source_id_prefix=source-id target=127.0.0.1:3 target_id_prefix=target-id failed= "
        "failed_id_prefix=";

    EXPECT_EQ(TokenRangesForLog(fence.ranges), "[[0,9],[20,29]]");
    EXPECT_EQ(TopologyTaskScopeForLog(fence), participantScope + " range_count=2 hash_ranges=[[0,9],[20,29]]");
    EXPECT_EQ(TopologyParticipantScopeForLog(fence.executor, fence.source, fence.target, fence.failed),
              participantScope);
    EXPECT_STREQ(TopologyCallbackStageName(TopologyCallbackPhase::SCALE_IN, false), "metadata_migration");
    EXPECT_STREQ(TopologyCallbackStageName(TopologyCallbackPhase::SCALE_IN, true), "data_drain");
}

TEST(TopologyDiagnosticsTest, FormatsCommittedAndProspectiveRingRanges)
{
    const auto snapshot = MakeScaleOutSnapshot();
    ASSERT_NE(snapshot, nullptr);

    const auto committed = TopologyRingForLog(*snapshot, TopologyRingView::COMMITTED);
    EXPECT_NE(committed.find("address=127.0.0.1:1"), std::string::npos);
    EXPECT_NE(committed.find("state=ACTIVE,ranges=[(300,100]]"), std::string::npos);
    EXPECT_NE(committed.find("address=127.0.0.1:2"), std::string::npos);
    EXPECT_NE(committed.find("state=JOINING,ranges=[]"), std::string::npos);
    EXPECT_NE(committed.find("state=ACTIVE,ranges=[(100,300]]"), std::string::npos);

    const auto prospective = TopologyRingForLog(*snapshot, TopologyRingView::PROSPECTIVE);
    EXPECT_NE(prospective.find("state=ACTIVE,ranges=[(300,100]]"), std::string::npos);
    EXPECT_NE(prospective.find("state=JOINING,ranges=[(100,200]]"), std::string::npos);
    EXPECT_NE(prospective.find("state=ACTIVE,ranges=[(200,300]]"), std::string::npos);

    const auto views = TopologyRingViewsForLog(*snapshot);
    EXPECT_NE(views.find("committed_ring=" + committed), std::string::npos);
    EXPECT_NE(views.find("prospective_ring=" + prospective), std::string::npos);
}

TEST(TopologyDiagnosticsTest, ExcludesLeavingMemberFromProspectiveScaleInRing)
{
    const auto snapshot = MakeScaleInSnapshot();
    ASSERT_NE(snapshot, nullptr);

    const auto committed = TopologyRingForLog(*snapshot, TopologyRingView::COMMITTED);
    EXPECT_NE(committed.find("state=LEAVING,ranges=[(300,100]]"), std::string::npos);
    EXPECT_NE(committed.find("state=ACTIVE,ranges=[(100,300]]"), std::string::npos);

    const auto prospective = TopologyRingForLog(*snapshot, TopologyRingView::PROSPECTIVE);
    EXPECT_NE(prospective.find("state=LEAVING,ranges=[]"), std::string::npos);
    EXPECT_NE(prospective.find("state=ACTIVE,ranges=[full]"), std::string::npos);
}

}  // namespace datasystem::cluster
