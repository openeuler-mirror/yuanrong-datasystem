/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Manual performance gates for centralized cluster topology control.
 */
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr size_t INITIAL_MEMBER_COUNT = 2'000;
constexpr size_t JOINING_MEMBER_COUNT = 500;
constexpr size_t TOTAL_MEMBER_COUNT = INITIAL_MEMBER_COUNT + JOINING_MEMBER_COUNT;
constexpr uint32_t TOKENS_PER_MEMBER = 4;
constexpr size_t PLAN_SAMPLE_COUNT = 30;
constexpr size_t MATERIALIZE_SAMPLE_COUNT = 30;
constexpr size_t PLACEMENT_SAMPLE_COUNT = 100'000;
constexpr auto PLAN_P99_BUDGET = std::chrono::milliseconds(200);
constexpr auto SCALE_IN_PLAN_P99_BUDGET = std::chrono::milliseconds(150);
constexpr auto MATERIALIZE_P99_BUDGET = std::chrono::milliseconds(600);

std::string MemberId(size_t index)
{
    std::ostringstream output;
    output << std::hex << std::setw(16) << std::setfill('0') << index;
    return output.str();
}

MemberIdentity MemberIdentityAt(size_t index)
{
    return { MemberId(index), "127.0.0.1:" + std::to_string(10'000 + index) };
}

std::vector<MemberIdentity> BuildIdentities(size_t begin, size_t count)
{
    std::vector<MemberIdentity> identities;
    identities.reserve(count);
    for (size_t index = begin; index < begin + count; ++index) {
        identities.emplace_back(MemberIdentityAt(index));
    }
    return identities;
}

TopologyState BuildCommittedTopology(const HashAlgorithm &algorithm, size_t memberCount = INITIAL_MEMBER_COUNT)
{
    TopologyPlan bootstrap;
    EXPECT_TRUE(algorithm.BuildInitialPlacement(
        { {}, BuildIdentities(0, memberCount), TOKENS_PER_MEMBER }, bootstrap).IsOk());
    bootstrap.next.clusterHasInit = true;
    bootstrap.next.version = 1;
    for (auto &member : bootstrap.next.members) {
        member.state = MemberState::ACTIVE;
    }
    return bootstrap.next;
}

template <typename Duration>
Duration Percentile(std::vector<Duration> samples, size_t percentile)
{
    std::sort(samples.begin(), samples.end());
    const size_t index = (samples.size() * percentile + 99) / 100 - 1;
    return samples[index];
}

TopologyPlan MeasureScaleOutPlan(const HashAlgorithm &algorithm, const TopologyState &current,
                                 std::vector<std::chrono::microseconds> &samples)
{
    const auto joining = BuildIdentities(INITIAL_MEMBER_COUNT, JOINING_MEMBER_COUNT);
    TopologyPlan latest;
    samples.reserve(PLAN_SAMPLE_COUNT);
    for (size_t sample = 0; sample < PLAN_SAMPLE_COUNT; ++sample) {
        const auto start = std::chrono::steady_clock::now();
        EXPECT_TRUE(algorithm.PlanScaleOut({ current, joining, TOKENS_PER_MEMBER }, latest).IsOk());
        samples.emplace_back(
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start));
    }
    latest.next.version = current.version + 1;
    latest.next.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, latest.next.version };
    return latest;
}

TopologyPlan MeasureScaleInPlan(const HashAlgorithm &algorithm, const TopologyState &current,
                                std::vector<std::chrono::microseconds> &samples)
{
    const auto leaving = BuildIdentities(INITIAL_MEMBER_COUNT, JOINING_MEMBER_COUNT);
    TopologyPlan latest;
    samples.reserve(PLAN_SAMPLE_COUNT);
    for (size_t sample = 0; sample < PLAN_SAMPLE_COUNT; ++sample) {
        const auto start = std::chrono::steady_clock::now();
        EXPECT_TRUE(algorithm.PlanScaleIn({ current, leaving }, latest).IsOk());
        samples.emplace_back(
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start));
    }
    return latest;
}

std::vector<MembershipRecord> BuildMemberships(const std::vector<Member> &members)
{
    std::vector<MembershipRecord> memberships;
    memberships.reserve(members.size());
    for (size_t index = 0; index < members.size(); ++index) {
        const bool restarting = index >= INITIAL_MEMBER_COUNT;
        memberships.push_back({ members[index].identity.address,
                                restarting ? MemberLifecycleState::RESTARTING : MemberLifecycleState::READY,
                                restarting ? static_cast<int64_t>(index + 1) : 0, "" });
    }
    return memberships;
}

size_t MaterializeAndSerialize(const TopologySnapshot &snapshot, const HashAlgorithm &algorithm)
{
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    EXPECT_TRUE(materializer.RebuildExpected(snapshot, algorithm, BuildMemberships(snapshot.Members()), true, expected)
                    .IsOk());
    if (expected.notifyRecipients.empty()) {
        ADD_FAILURE() << "materialized generation has no notify recipients";
        return 0;
    }
    std::string regularBytes;
    std::string optimizedBytes;
    EXPECT_TRUE(materializer.BuildEncodedNotifyFor(expected, expected.notifyRecipients.front(), optimizedBytes).IsOk());
    TopologyTaskNotify regularNotify;
    EXPECT_TRUE(TopologyRepositoryCodec::DecodeNotify(optimizedBytes, regularNotify).IsOk());
    EXPECT_EQ(regularNotify.restartTimestampsByAddress.size(), JOINING_MEMBER_COUNT);
    EXPECT_TRUE(TopologyRepositoryCodec::EncodeNotify(regularNotify, regularBytes).IsOk());
    EXPECT_EQ(optimizedBytes, regularBytes);
    size_t serializedBytes = 0;
    for (const auto &address : expected.notifyRecipients) {
        std::string bytes;
        EXPECT_TRUE(materializer.BuildEncodedNotifyFor(expected, address, bytes).IsOk());
        serializedBytes += bytes.size();
    }
    return serializedBytes;
}

size_t MeasureMaterialization(const TopologySnapshot &snapshot, const HashAlgorithm &algorithm,
                              std::vector<std::chrono::milliseconds> &samples)
{
    size_t serializedBytes = 0;
    samples.reserve(MATERIALIZE_SAMPLE_COUNT);
    for (size_t sample = 0; sample < MATERIALIZE_SAMPLE_COUNT; ++sample) {
        const auto startedAt = std::chrono::steady_clock::now();
        serializedBytes = MaterializeAndSerialize(snapshot, algorithm);
        samples.emplace_back(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startedAt));
    }
    return serializedBytes;
}

TEST(TopologyControlPerfTest, ScaleOutPlanAndMaterializationStayWithinPhase3Budgets)
{
    HashAlgorithm algorithm;
    const TopologyState current = BuildCommittedTopology(algorithm);
    ASSERT_EQ(current.members.size(), INITIAL_MEMBER_COUNT);
    std::vector<std::chrono::microseconds> planSamples;
    TopologyPlan plan = MeasureScaleOutPlan(algorithm, current, planSamples);
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'a'), snapshot));
    std::vector<std::chrono::milliseconds> materializeSamples;
    const size_t serializedBytes = MeasureMaterialization(*snapshot, algorithm, materializeSamples);
    const auto planP50 = Percentile(planSamples, 50);
    const auto planP95 = Percentile(planSamples, 95);
    const auto planP99 = Percentile(planSamples, 99);
    const auto planMax = *std::max_element(planSamples.begin(), planSamples.end());
    const auto materializeP50 = Percentile(materializeSamples, 50);
    const auto materializeP95 = Percentile(materializeSamples, 95);
    const auto materializeP99 = Percentile(materializeSamples, 99);
    const auto materializeMax = *std::max_element(materializeSamples.begin(), materializeSamples.end());

    std::cout << "CLUSTER_PERF members=" << TOTAL_MEMBER_COUNT << " joining=" << JOINING_MEMBER_COUNT
              << " plan_p50_us=" << planP50.count() << " plan_p95_us=" << planP95.count()
              << " plan_p99_us=" << planP99.count() << " materialize_p50_ms=" << materializeP50.count()
              << " plan_max_us=" << planMax.count()
              << " materialize_p95_ms=" << materializeP95.count()
              << " materialize_p99_ms=" << materializeP99.count()
              << " materialize_max_ms=" << materializeMax.count()
              << " notify_bytes=" << serializedBytes << std::endl;
    EXPECT_LE(planP99, std::chrono::duration_cast<std::chrono::microseconds>(PLAN_P99_BUDGET));
    EXPECT_LE(materializeP99, std::chrono::duration_cast<std::chrono::milliseconds>(MATERIALIZE_P99_BUDGET));
}

TEST(TopologyControlPerfTest, ScaleInPlanStaysWithinPhase3Budget)
{
    HashAlgorithm algorithm;
    const TopologyState current = BuildCommittedTopology(algorithm, TOTAL_MEMBER_COUNT);
    ASSERT_EQ(current.members.size(), TOTAL_MEMBER_COUNT);
    std::vector<std::chrono::microseconds> samples;
    const TopologyPlan plan = MeasureScaleInPlan(algorithm, current, samples);
    const auto p50 = Percentile(samples, 50);
    const auto p95 = Percentile(samples, 95);
    const auto p99 = Percentile(samples, 99);
    const auto maximum = *std::max_element(samples.begin(), samples.end());

    EXPECT_EQ(std::count_if(plan.next.members.begin(), plan.next.members.end(),
                            [](const auto &member) { return member.state == MemberState::PRE_LEAVING; }),
              JOINING_MEMBER_COUNT);
    EXPECT_FALSE(plan.ownerChanges.empty());
    std::cout << "CLUSTER_PERF members=" << TOTAL_MEMBER_COUNT << " leaving=" << JOINING_MEMBER_COUNT
              << " scale_in_plan_p50_us=" << p50.count() << " scale_in_plan_p95_us=" << p95.count()
              << " scale_in_plan_p99_us=" << p99.count() << " scale_in_plan_max_us=" << maximum.count()
              << std::endl;
    EXPECT_LE(p99, std::chrono::duration_cast<std::chrono::microseconds>(SCALE_IN_PLAN_P99_BUDGET));
}

TEST(TopologyControlPerfTest, PlacementLookupReportsForegroundLatency)
{
    HashAlgorithm algorithm;
    TopologyState current = BuildCommittedTopology(algorithm);
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(current, 1, std::string(64, 'b'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    PlacementFacade placement(snapshots, algorithm, current.members.front().identity.address);
    std::vector<std::chrono::nanoseconds> samples;
    samples.reserve(PLACEMENT_SAMPLE_COUNT);
    for (size_t index = 0; index < PLACEMENT_SAMPLE_COUNT; ++index) {
        const auto start = std::chrono::steady_clock::now();
        PlacementDecision decision;
        DS_ASSERT_OK(placement.Locate(std::to_string(index), decision));
        samples.emplace_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start));
    }
    const auto p50 = Percentile(samples, 50);
    const auto p95 = Percentile(samples, 95);
    const auto p99 = Percentile(samples, 99);
    std::cout << "CLUSTER_PERF placement_samples=" << PLACEMENT_SAMPLE_COUNT << " placement_p50_ns=" << p50.count()
              << " placement_p95_ns=" << p95.count() << " placement_p99_ns=" << p99.count() << std::endl;
}
}  // namespace
}  // namespace datasystem::cluster
