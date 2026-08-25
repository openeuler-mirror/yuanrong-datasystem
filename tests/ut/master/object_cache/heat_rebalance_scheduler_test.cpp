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
 * Description: Test the heat-driven rebalance scheduler.
 */

#include "datasystem/master/heat_rebalance_scheduler.h"

#include <initializer_list>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/model/topology_snapshot.h"
#include "ut/common.h"

DS_DECLARE_bool(enable_memory_rebalance);
DS_DECLARE_uint32(rebalance_heat_source_usage_percent);
DS_DECLARE_uint32(rebalance_heat_source_hot_ratio_percent);
DS_DECLARE_uint32(rebalance_heat_source_usage_percent_low);
DS_DECLARE_uint32(rebalance_heat_target_usage_percent);
DS_DECLARE_uint32(rebalance_heat_target_hot_ratio_percent);
DS_DECLARE_uint32(rebalance_task_report_grace_ms);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);

using namespace datasystem::master;

namespace datasystem {
namespace ut {
namespace {
constexpr uint64_t MEMORY_CAPACITY = 1'000;
constexpr size_t TOPOLOGY_MEMBER_ID_SIZE = 16;
constexpr size_t TOPOLOGY_DIGEST_SIZE = 64;
const std::string WORKER_S = "127.0.0.1:31001";
const std::string WORKER_T1 = "127.0.0.1:31002";
const std::string WORKER_T2 = "127.0.0.1:31003";

// avail is derived as capacity - used so usage math stays exact.
NodeInfo MakeHotNode(const std::string &worker, uint64_t used, uint64_t hotCount, uint64_t totalCount,
                     uint64_t hotBytes, bool isReady = true)
{
    return NodeInfo(worker, MEMORY_CAPACITY - used, isReady, 0, used, MEMORY_CAPACITY, MEMORY_CAPACITY,
                    hotCount, totalCount, hotBytes, master::EVICTION_POLICY_HEAT, 0);
}

std::unordered_map<std::string, NodeInfo> MakeSnapshot(std::initializer_list<NodeInfo> nodes)
{
    std::unordered_map<std::string, NodeInfo> snapshot;
    snapshot.reserve(nodes.size());
    for (const auto &node : nodes) {
        snapshot.emplace(node.nodeId, node);
    }
    return snapshot;
}

void PublishTopology(cluster::TopologySnapshotState &snapshots, cluster::MemberState sourceState,
                     cluster::MemberState targetState)
{
    cluster::TopologyState topology;
    topology.clusterHasInit = true;
    topology.version = 1;
    if (sourceState != cluster::MemberState::ACTIVE || targetState != cluster::MemberState::ACTIVE) {
        topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 1 };
    }
    topology.members = {
        cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 's'), WORKER_S }, sourceState, { 0 } },
        cluster::Member{ { std::string(TOPOLOGY_MEMBER_ID_SIZE, 't'), WORKER_T1 }, targetState, { 1 } }
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(
        std::move(topology), 1, std::string(TOPOLOGY_DIGEST_SIZE, 'd'), snapshot));
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(std::move(snapshot), outcome));
}

master::ResourceReportReqPb MakeResourceReq(const std::string &reportingWorker)
{
    master::ResourceReportReqPb req;
    req.mutable_stat()->set_address(reportingWorker);
    return req;
}

master::ReportRebalanceResultReqPb MakeResultReq(const master::RebalanceTaskPb &task,
                                                 master::RebalanceTaskStatusPb status)
{
    master::ReportRebalanceResultReqPb req;
    req.set_task_id(task.task_id());
    req.set_source_worker(task.source_worker());
    req.set_target_worker(task.target_worker());
    req.set_status(status);
    req.set_migrated_bytes(task.max_bytes());
    req.set_migrated_objects(1);
    return req;
}
}  // namespace

class HeatRebalanceSchedulerTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldEnable_ = FLAGS_enable_memory_rebalance;
        oldSrcUsage_ = FLAGS_rebalance_heat_source_usage_percent;
        oldSrcHot_ = FLAGS_rebalance_heat_source_hot_ratio_percent;
        oldSrcUsageLow_ = FLAGS_rebalance_heat_source_usage_percent_low;
        oldTgtUsage_ = FLAGS_rebalance_heat_target_usage_percent;
        oldTgtHot_ = FLAGS_rebalance_heat_target_hot_ratio_percent;
        oldGrace_ = FLAGS_rebalance_task_report_grace_ms;
        oldRate_ = FLAGS_data_migrate_rate_limit_mb;

        FLAGS_enable_memory_rebalance = true;
        FLAGS_rebalance_heat_source_usage_percent = 60;
        FLAGS_rebalance_heat_source_hot_ratio_percent = 40;
        FLAGS_rebalance_heat_source_usage_percent_low = 50;
        FLAGS_rebalance_heat_target_usage_percent = 50;
        FLAGS_rebalance_heat_target_hot_ratio_percent = 30;
        FLAGS_rebalance_task_report_grace_ms = 300;
        FLAGS_data_migrate_rate_limit_mb = 500;
        RefreshRebalanceHeatFactors();
    }

    void TearDown() override
    {
        FLAGS_enable_memory_rebalance = oldEnable_;
        FLAGS_rebalance_heat_source_usage_percent = oldSrcUsage_;
        FLAGS_rebalance_heat_source_hot_ratio_percent = oldSrcHot_;
        FLAGS_rebalance_heat_source_usage_percent_low = oldSrcUsageLow_;
        FLAGS_rebalance_heat_target_usage_percent = oldTgtUsage_;
        FLAGS_rebalance_heat_target_hot_ratio_percent = oldTgtHot_;
        FLAGS_rebalance_task_report_grace_ms = oldGrace_;
        FLAGS_data_migrate_rate_limit_mb = oldRate_;
        RefreshRebalanceHeatFactors();
        CommonTest::TearDown();
    }

protected:
    master::ResourceReportRspPb ScheduleAndGetRsp(HeatRebalanceScheduler &scheduler, const std::string &reportingWorker,
                                                   const std::unordered_map<std::string, NodeInfo> &snapshot)
    {
        master::ResourceReportRspPb rsp;
        auto req = MakeResourceReq(reportingWorker);
        DS_EXPECT_OK(scheduler.Schedule(req, snapshot, rsp));
        return rsp;
    }

private:
    bool oldEnable_ = false;
    uint32_t oldSrcUsage_ = 0;
    uint32_t oldSrcHot_ = 0;
    uint32_t oldSrcUsageLow_ = 0;
    uint32_t oldTgtUsage_ = 0;
    uint32_t oldTgtHot_ = 0;
    uint32_t oldGrace_ = 0;
    uint32_t oldRate_ = 0;
};

// A hot, high-usage source gets a task targeting the lowest-usage eligible worker.
TEST_F(HeatRebalanceSchedulerTest, HotHighUsageSourceGetsTaskToLowUsageTarget)
{
    HeatRebalanceScheduler scheduler;
    // source: usage 80% (>60%), hotBytesRatio 50% (>40%), hotBytes 500. Triggers via path A.
    // target1: usage 20% (<50%, eligible), hotBytesRatio 0%.
    // target2: usage 90% is ineligible even though hotBytesRatio is low.
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 200, 0, 100, 0),
        MakeHotNode(WORKER_T2, 900, 0, 100, 0),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);

    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().source_worker(), WORKER_S);
    // usage-low first: T1 (20%) before T2 (90%).
    EXPECT_EQ(rsp.rebalance_task().target_worker(), WORKER_T1);
    // bytes = min(10%*1000=100, target avail 800, cap) = 100.
    EXPECT_EQ(rsp.rebalance_task().max_bytes(), 100ul);
    EXPECT_EQ(rsp.rebalance_task().source_eviction_policy(), master::EVICTION_POLICY_HEAT);
    EXPECT_EQ(rsp.rebalance_task().source_eviction_policy_epoch(), 0u);
    EXPECT_EQ(rsp.rebalance_task().target_eviction_policy(), master::EVICTION_POLICY_HEAT);
    EXPECT_EQ(rsp.rebalance_task().target_eviction_policy_epoch(), 0u);
}

TEST_F(HeatRebalanceSchedulerTest, ConfiguredTopologyWithoutSnapshotFailsClosed)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    HeatRebalanceScheduler scheduler;
    scheduler.SetTopologyMembership(&membership);
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 200, 0, 100, 0),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
}

TEST_F(HeatRebalanceSchedulerTest, TopologyRejectsLeavingSourceOrTarget)
{
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 200, 0, 100, 0),
    });
    for (const auto &[sourceState, targetState] :
         std::vector<std::pair<cluster::MemberState, cluster::MemberState>>{
             { cluster::MemberState::LEAVING, cluster::MemberState::ACTIVE },
             { cluster::MemberState::ACTIVE, cluster::MemberState::LEAVING } }) {
        cluster::TopologySnapshotState snapshots;
        cluster::MembershipEndpointView membership(snapshots);
        PublishTopology(snapshots, sourceState, targetState);
        HeatRebalanceScheduler scheduler;
        scheduler.SetTopologyMembership(&membership);
        auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
        EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
    }
}

TEST_F(HeatRebalanceSchedulerTest, MatchingTerminalResultWinsAfterMasterDeadline)
{
    // Admit a target with exactly one task worth of free space so the successful-transfer hold is directly visible:
    // retaining the hold blocks a second task, while losing it would allow immediate redispatch.
    FLAGS_rebalance_heat_target_usage_percent = 101;
    RefreshRebalanceHeatFactors();
    HeatRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 900, 0, 100, 0),
    });
    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    scheduler.SetActiveTaskDeadlineForTest(WORKER_S, 0);

    auto resultReq = MakeResultReq(rsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED);
    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(resultReq, resultRsp));

    // A fresh target report is required to release a successful transfer hold. Keep the snapshot stale even on
    // platforms where the steady clock has sub-millisecond resolution.
    snapshot.at(WORKER_T1).timestamp = 0;
    auto nextRsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    EXPECT_TRUE(nextRsp.rebalance_task().task_id().empty())
        << "successful late terminal result must retain the target capacity hold";
}

TEST_F(HeatRebalanceSchedulerTest, RejectsHighUsageTargetEvenWhenItsHotRatioIsLow)
{
    HeatRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 900, 0, 100, 0),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
}

TEST_F(HeatRebalanceSchedulerTest, EqualTargetsUseStableNodeIdTieBreak)
{
    HeatRebalanceScheduler scheduler;
    const std::string firstTarget = "a-target";
    const std::string secondTarget = "z-target";
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(secondTarget, 200, 0, 100, 0),
        MakeHotNode(firstTarget, 200, 0, 100, 0),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);

    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().target_worker(), firstTarget);
}

// Source trigger has two OR paths:
//   Path A (memory pressure): usage > 60% AND at least one primary remains
//   Path B (moderate-load + high-heat): usage > 50% AND hotBytesRatio > 40%
TEST_F(HeatRebalanceSchedulerTest, SourceTriggerTwoPathsOrSemantics)
{
    // Path A only: high usage still progresses after hot primary bytes fall to zero.
    {
        HeatRebalanceScheduler scheduler;
        auto snapPathA = MakeSnapshot({
            MakeHotNode(WORKER_S, 800, 0, 100, 0),
            MakeHotNode(WORKER_T1, 200, 0, 100, 0),
        });
        auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapPathA);
        ASSERT_FALSE(rsp.rebalance_task().task_id().empty())
            << "path A should trigger: usage 80%>60% and primary copies remain";
    }

    // Path B only: usage 55% (>50% but not >60%), hotBytesRatio 50% (>40%) -> triggers.
    {
        HeatRebalanceScheduler scheduler;
        auto snapPathB = MakeSnapshot({
            MakeHotNode(WORKER_S, 550, 50, 100, 500),
            MakeHotNode(WORKER_T1, 200, 0, 100, 0),
        });
        auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapPathB);
        ASSERT_FALSE(rsp.rebalance_task().task_id().empty())
            << "path B should trigger: usage 55%>50% and hotBytesRatio 50%>40%";
    }

    // Moderate usage without enough heat satisfies neither path.
    {
        HeatRebalanceScheduler scheduler;
        auto snapNeither = MakeSnapshot({
            MakeHotNode(WORKER_S, 550, 50, 100, 350),
            MakeHotNode(WORKER_T1, 200, 0, 100, 0),
        });
        auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapNeither);
        EXPECT_TRUE(rsp.rebalance_task().task_id().empty())
            << "neither path should trigger: usage 55%<=60% and hotBytesRatio 35%<=40%";
    }
}

TEST_F(HeatRebalanceSchedulerTest, DoesNotPairWorkersAcrossEvictionPolicies)
{
    HeatRebalanceScheduler scheduler;
    auto source = MakeHotNode(WORKER_S, 800, 50, 100, 500);
    auto target = MakeHotNode(WORKER_T1, 200, 0, 100, 0);
    target.evictionPolicy = master::EVICTION_POLICY_CLOCK;

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, MakeSnapshot({ source, target }));

    EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
}

// Migration bytes are capped by the target's remaining available space when it's less than 10% of source capacity.
TEST_F(HeatRebalanceSchedulerTest, BytesCappedByTargetAvailableSpace)
{
    // Relax only this test's admission watermark so the remaining-space cap can be exercised independently.
    FLAGS_rebalance_heat_target_usage_percent = 99;
    RefreshRebalanceHeatFactors();
    HeatRebalanceScheduler scheduler;
    // source: hotBytes 1000 -> hotBytesRatio 100% (>40%, path A). capacity 1000 -> 10% budget = 100.
    // target: used 950 -> avail 50 < 100, so bytes capped at 50.
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 1000),
        MakeHotNode(WORKER_T1, 950, 0, 100, 0),
    });
    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().max_bytes(), 50ul);
}

// A SUCCEEDED task does NOT add cooldown — the source may be re-selected on the next report.
TEST_F(HeatRebalanceSchedulerTest, SuccessDoesNotBlockReselection)
{
    HeatRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 200, 0, 100, 0),
        // A second eligible target ensures the source itself is not blocked by target cooldown.
        MakeHotNode(WORKER_T2, 300, 0, 100, 0),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().source_worker(), WORKER_S);

    // Report success; success no longer adds cooldown.
    auto resultReq = MakeResultReq(rsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED);
    master::ReportRebalanceResultRspPb resultRsp;
    DS_EXPECT_OK(scheduler.ReportResult(resultReq, resultRsp));

    // Same hot/high-usage source, success cooldown removed -> a new task is dispatched.
    auto rsp2 = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    ASSERT_FALSE(rsp2.rebalance_task().task_id().empty())
        << "source should be re-selectable after success (no success cooldown)";
}

TEST_F(HeatRebalanceSchedulerTest, SuccessfulTargetInflightHeldUntilFreshReport)
{
    // Relax only this test's admission watermark so the in-flight hold can be exercised independently.
    FLAGS_rebalance_heat_target_usage_percent = 101;
    RefreshRebalanceHeatFactors();
    HeatRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeHotNode(WORKER_S, 800, 50, 100, 500),
        MakeHotNode(WORKER_T1, 900, 0, 100, 0),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().max_bytes(), 100ul);

    auto resultReq = MakeResultReq(rsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED);
    master::ReportRebalanceResultRspPb resultRsp;
    DS_ASSERT_OK(scheduler.ReportResult(resultReq, resultRsp));

    // The stale target snapshot still advertises the pre-migration 100-byte room. The held in-flight charge must
    // consume that room so the same target cannot be overcommitted before its next report.
    auto staleRsp = ScheduleAndGetRsp(scheduler, WORKER_S, snapshot);
    EXPECT_TRUE(staleRsp.rebalance_task().task_id().empty());

    auto lateOldSnapshot = snapshot;
    lateOldSnapshot.at(WORKER_T1).timestamp = std::numeric_limits<uint64_t>::max();
    auto lateOldRsp = ScheduleAndGetRsp(scheduler, WORKER_S, lateOldSnapshot);
    EXPECT_TRUE(lateOldRsp.rebalance_task().task_id().empty())
        << "a late report with pre-migration used memory must not release the hold";

    auto freshSnapshot = lateOldSnapshot;
    freshSnapshot.at(WORKER_T1).usedMemory = 1'000;
    // Keep advertised room non-zero so releasing the hold is observable as a new task. The used-memory watermark,
    // rather than this synthetic availability value, is the property under test.
    freshSnapshot.at(WORKER_T1).availableMemory = 100;
    auto freshRsp = ScheduleAndGetRsp(scheduler, WORKER_S, freshSnapshot);
    EXPECT_FALSE(freshRsp.rebalance_task().task_id().empty());
}

}  // namespace ut
}  // namespace datasystem
