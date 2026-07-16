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
 * Description: Test memory rebalance scheduler.
 */

#include "datasystem/master/memory_rebalance_scheduler.h"

#include <initializer_list>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/timer.h"
#include "ut/common.h"

DS_DECLARE_bool(enable_memory_rebalance);
DS_DECLARE_uint32(rebalance_source_usage_percent);
DS_DECLARE_uint32(rebalance_usage_gap_percent);
DS_DECLARE_uint32(rebalance_cooldown_s);
DS_DECLARE_uint32(rebalance_task_report_grace_ms);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DECLARE_uint32(node_dead_timeout_s);

using namespace datasystem::master;

namespace datasystem {
namespace ut {
namespace {
constexpr uint64_t MEMORY_CAPACITY = 1'000;
constexpr uint64_t MS_PER_SECOND = 1'000;
constexpr uint64_t TRANSFER_TIME_MULTIPLIER = 2;
const std::string WORKER_92 = "127.0.0.1:9200";
const std::string WORKER_78 = "127.0.0.1:7800";
const std::string WORKER_15 = "127.0.0.1:1500";
const std::string WORKER_10 = "127.0.0.1:1000";

NodeInfo MakeNode(const std::string &worker, uint64_t usedMemory, uint64_t availableMemory, bool isReady = true,
                  uint64_t memoryCapacity = MEMORY_CAPACITY, uint64_t memoryLimit = MEMORY_CAPACITY)
{
    return NodeInfo(worker, availableMemory, isReady, 0, usedMemory, memoryCapacity, memoryLimit);
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

class MemoryRebalanceSchedulerTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        oldEnableMemoryRebalance_ = FLAGS_enable_memory_rebalance;
        oldSourceUsagePercent_ = FLAGS_rebalance_source_usage_percent;
        oldUsageGapPercent_ = FLAGS_rebalance_usage_gap_percent;
        oldCooldownS_ = FLAGS_rebalance_cooldown_s;
        oldTaskTimeoutS_ = FLAGS_rebalance_task_report_grace_ms;
        oldDataMigrateRate_ = FLAGS_data_migrate_rate_limit_mb;
        oldNodeDeadTimeoutS_ = FLAGS_node_dead_timeout_s;

        FLAGS_enable_memory_rebalance = true;
        FLAGS_rebalance_source_usage_percent = 70;
        FLAGS_rebalance_usage_gap_percent = 30;
        FLAGS_rebalance_cooldown_s = 60;
        FLAGS_rebalance_task_report_grace_ms = 300;
        FLAGS_data_migrate_rate_limit_mb = 500;
        FLAGS_node_dead_timeout_s = 0;  // TTL = max(0, HOLD_TTL_MIN_S) = 60s; flaky-safe with relative backdate
    }

    void TearDown() override
    {
        FLAGS_enable_memory_rebalance = oldEnableMemoryRebalance_;
        FLAGS_rebalance_source_usage_percent = oldSourceUsagePercent_;
        FLAGS_rebalance_usage_gap_percent = oldUsageGapPercent_;
        FLAGS_rebalance_cooldown_s = oldCooldownS_;
        FLAGS_rebalance_task_report_grace_ms = oldTaskTimeoutS_;
        FLAGS_data_migrate_rate_limit_mb = oldDataMigrateRate_;
        FLAGS_node_dead_timeout_s = oldNodeDeadTimeoutS_;
        CommonTest::TearDown();
    }

protected:
    master::ResourceReportRspPb ScheduleAndGetRsp(MemoryRebalanceScheduler &scheduler,
                                                  const std::string &reportingWorker,
                                                  const std::unordered_map<std::string, NodeInfo> &snapshot)
    {
        master::ResourceReportRspPb rsp;
        auto req = MakeResourceReq(reportingWorker);
        DS_EXPECT_OK(scheduler.Schedule(req, snapshot, rsp));
        return rsp;
    }

    // The scheduler grants this fixture friend access (memory_rebalance_scheduler.h). The fixture
    // is the friend, so its own methods can touch the private hold maps; TEST_F bodies run in a
    // gtest-derived subclass and cannot, so they go through these protected static wrappers.
    static bool HasInflight(const MemoryRebalanceScheduler &s)
    {
        return !s.targetInflightBytes_.empty();
    }
    static bool HasPendingRelease(const MemoryRebalanceScheduler &s)
    {
        return !s.pendingReleaseBytes_.empty();
    }
    static bool HasHold(const MemoryRebalanceScheduler &s)
    {
        return !s.holdSinceMs_.empty();
    }
    static void BackdateHold(MemoryRebalanceScheduler &s, const std::string &worker, uint64_t ts)
    {
        s.holdSinceMs_[worker] = ts;
    }
    static uint64_t GetHoldTs(const MemoryRebalanceScheduler &s, const std::string &worker)
    {
        return s.holdSinceMs_.at(worker);
    }

private:
    bool oldEnableMemoryRebalance_ = false;
    uint32_t oldSourceUsagePercent_ = 0;
    uint32_t oldUsageGapPercent_ = 0;
    uint32_t oldCooldownS_ = 0;
    uint32_t oldTaskTimeoutS_ = 0;
    uint32_t oldDataMigrateRate_ = 0;
    uint32_t oldNodeDeadTimeoutS_ = 0;
};

TEST_F(MemoryRebalanceSchedulerTest, SelectBestSourceTargetPairFromFourWorkers)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_78, 780, 220),
        MakeNode(WORKER_15, 150, 850),
        MakeNode(WORKER_10, 100, 900),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);

    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().source_worker(), WORKER_92);
    EXPECT_EQ(rsp.rebalance_task().target_worker(), WORKER_10);
    EXPECT_EQ(rsp.rebalance_task().max_bytes(), 410ul);
}

TEST_F(MemoryRebalanceSchedulerTest, UsageGapThresholdControlsWhetherTaskIsCreated)
{
    auto verify = [this](uint64_t targetUsed, bool expectTask) {
        MemoryRebalanceScheduler scheduler;
        const std::string source = "127.0.0.1:7000";
        const std::string target = "127.0.0.1:" + std::to_string(targetUsed);
        auto snapshot = MakeSnapshot({
            MakeNode(source, 700, 300),
            MakeNode(target, targetUsed, MEMORY_CAPACITY - targetUsed),
        });

        auto rsp = ScheduleAndGetRsp(scheduler, source, snapshot);
        EXPECT_EQ(!rsp.rebalance_task().task_id().empty(), expectTask);
    };

    verify(410, false);  // 70% - 41% = 29%
    verify(400, true);   // 70% - 40% = 30%
    verify(390, true);   // 70% - 39% = 31%
}

TEST_F(MemoryRebalanceSchedulerTest, UsageRateUsesMemoryLimitInsteadOfHighWaterCapacity)
{
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:8000";
    const std::string target = "127.0.0.1:5000";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 800, 100, true, 800, MEMORY_CAPACITY),
        MakeNode(target, 500, 300, true, 500, MEMORY_CAPACITY),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, source, snapshot);

    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    EXPECT_EQ(rsp.rebalance_task().source_worker(), source);
    EXPECT_EQ(rsp.rebalance_task().target_worker(), target);
    EXPECT_EQ(rsp.rebalance_task().max_bytes(), 150ul);
}

TEST_F(MemoryRebalanceSchedulerTest, CalculateMaxBytesAndSkipNonPositiveBudget)
{
    {
        MemoryRebalanceScheduler scheduler;
        const std::string source = "127.0.0.1:9000";
        const std::string target = "127.0.0.1:3000";
        auto snapshot = MakeSnapshot({
            MakeNode(source, 900, 100),
            MakeNode(target, 300, 100),
        });

        auto rsp = ScheduleAndGetRsp(scheduler, source, snapshot);
        ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
        EXPECT_EQ(rsp.rebalance_task().max_bytes(), 100ul);
    }
    {
        MemoryRebalanceScheduler scheduler;
        const std::string source = "127.0.0.1:9100";
        const std::string target = "127.0.0.1:3100";
        auto snapshot = MakeSnapshot({
            MakeNode(source, 900, 100),
            MakeNode(target, 300, 0),
        });

        auto rsp = ScheduleAndGetRsp(scheduler, source, snapshot);
        EXPECT_TRUE(rsp.rebalance_task().task_id().empty());
    }
}

TEST_F(MemoryRebalanceSchedulerTest, DeadlineUsesConfiguredTaskTimeout)
{
    FLAGS_rebalance_task_report_grace_ms = 7;
    FLAGS_data_migrate_rate_limit_mb = 5;
    MemoryRebalanceScheduler scheduler;
    const std::string source = "127.0.0.1:9001";
    const std::string target = "127.0.0.1:1001";
    auto snapshot = MakeSnapshot({
        MakeNode(source, 900, 100),
        MakeNode(target, 100, 900),
    });

    auto rsp = ScheduleAndGetRsp(scheduler, source, snapshot);

    auto rate_bytes_per_sec = static_cast<uint64_t>(FLAGS_data_migrate_rate_limit_mb) * 1024 * 1024;
    auto estimated_transfer_ms =
        ((rsp.rebalance_task().max_bytes() + rate_bytes_per_sec - 1) / rate_bytes_per_sec) * MS_PER_SECOND;
    ASSERT_FALSE(rsp.rebalance_task().task_id().empty());
    auto expectedTimeoutMs = estimated_transfer_ms * TRANSFER_TIME_MULTIPLIER + FLAGS_rebalance_task_report_grace_ms;
    EXPECT_EQ(rsp.rebalance_task().timeout_ms(), expectedTimeoutMs);
    EXPECT_EQ(rsp.rebalance_task().deadline_ms() - rsp.rebalance_task().create_time_ms(), expectedTimeoutMs);
}

TEST_F(MemoryRebalanceSchedulerTest, TargetInflightBytesPreventsOverAssigningTarget)
{
    MemoryRebalanceScheduler scheduler;
    const std::string sourceA = "127.0.0.1:9002";
    const std::string sourceB = "127.0.0.1:8502";
    const std::string targetBusy = "127.0.0.1:1002";
    const std::string targetFree = "127.0.0.1:2002";
    auto firstSnapshot = MakeSnapshot({
        MakeNode(sourceA, 900, 100),
        MakeNode(targetBusy, 100, 400),
    });

    auto firstRsp = ScheduleAndGetRsp(scheduler, sourceA, firstSnapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), targetBusy);
    ASSERT_EQ(firstRsp.rebalance_task().max_bytes(), 400ul);

    auto secondSnapshot = MakeSnapshot({
        MakeNode(sourceA, 900, 100),
        MakeNode(sourceB, 850, 150),
        MakeNode(targetBusy, 100, 400),
        MakeNode(targetFree, 200, 800),
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, sourceB, secondSnapshot);

    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().source_worker(), sourceB);
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), targetFree);
}

TEST_F(MemoryRebalanceSchedulerTest, DoesNotCreateTaskWhenReportingWorkerIsNotSource)
{
    MemoryRebalanceScheduler scheduler;
    const std::string sourceA = "127.0.0.1:9004";
    const std::string sourceB = "127.0.0.1:8504";
    const std::string target = "127.0.0.1:1004";
    auto targetReportSnapshot = MakeSnapshot({
        MakeNode(sourceA, 900, 100),
        MakeNode(target, 100, 400),
    });

    auto targetRsp = ScheduleAndGetRsp(scheduler, target, targetReportSnapshot);
    EXPECT_TRUE(targetRsp.rebalance_task().task_id().empty());

    auto sourceReportSnapshot = MakeSnapshot({
        MakeNode(sourceB, 850, 150),
        MakeNode(target, 100, 400),
    });
    auto sourceRsp = ScheduleAndGetRsp(scheduler, sourceB, sourceReportSnapshot);
    ASSERT_FALSE(sourceRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(sourceRsp.rebalance_task().source_worker(), sourceB);
    EXPECT_EQ(sourceRsp.rebalance_task().target_worker(), target);
}

TEST_F(MemoryRebalanceSchedulerTest, CooldownWorkerAndRunningSourceAreNotSelected)
{
    const std::string runningSource = "127.0.0.1:9203";
    const std::string otherSource = "127.0.0.1:8503";
    const std::string targetBusy = "127.0.0.1:1003";
    const std::string targetFree = "127.0.0.1:2003";
    {
        MemoryRebalanceScheduler scheduler;
        auto firstSnapshot = MakeSnapshot({
            MakeNode(runningSource, 920, 80),
            MakeNode(targetBusy, 100, 400),
        });
        auto firstRsp = ScheduleAndGetRsp(scheduler, runningSource, firstSnapshot);
        ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

        auto secondSnapshot = MakeSnapshot({
            MakeNode(runningSource, 920, 80),
            MakeNode(otherSource, 850, 150),
            MakeNode(targetBusy, 100, 400),
            MakeNode(targetFree, 200, 800),
        });
        auto secondRsp = ScheduleAndGetRsp(scheduler, otherSource, secondSnapshot);

        ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
        EXPECT_EQ(secondRsp.rebalance_task().source_worker(), otherSource);
    }
    {
        MemoryRebalanceScheduler scheduler;
        auto firstSnapshot = MakeSnapshot({
            MakeNode(runningSource, 920, 80),
            MakeNode(targetBusy, 100, 900),
        });
        auto firstRsp = ScheduleAndGetRsp(scheduler, runningSource, firstSnapshot);
        ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

        master::ReportRebalanceResultRspPb reportRsp;
        auto failedReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED);
        DS_ASSERT_OK(scheduler.ReportResult(failedReq, reportRsp));

        auto secondSnapshot = MakeSnapshot({
            MakeNode(runningSource, 920, 80),
            MakeNode(otherSource, 850, 150),
            MakeNode(targetBusy, 100, 900),
            MakeNode(targetFree, 200, 800),
        });
        auto secondRsp = ScheduleAndGetRsp(scheduler, otherSource, secondSnapshot);

        ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
        EXPECT_EQ(secondRsp.rebalance_task().source_worker(), otherSource);
        EXPECT_EQ(secondRsp.rebalance_task().target_worker(), targetFree);
    }
    {
        MemoryRebalanceScheduler scheduler;
        auto firstSnapshot = MakeSnapshot({
            MakeNode(runningSource, 920, 80),
            MakeNode(targetBusy, 100, 900),
        });
        auto firstRsp = ScheduleAndGetRsp(scheduler, runningSource, firstSnapshot);
        ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

        master::ReportRebalanceResultRspPb reportRsp;
        auto failedReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED);
        DS_ASSERT_OK(scheduler.ReportResult(failedReq, reportRsp));

        auto secondSnapshot = MakeSnapshot({
            MakeNode(runningSource, 920, 80),
            MakeNode(targetBusy, 100, 900),
            MakeNode(targetFree, 200, 800),
        });
        auto secondRsp = ScheduleAndGetRsp(scheduler, runningSource, secondSnapshot);

        ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
        EXPECT_EQ(secondRsp.rebalance_task().source_worker(), runningSource);
        EXPECT_EQ(secondRsp.rebalance_task().target_worker(), targetFree);
    }
}

TEST_F(MemoryRebalanceSchedulerTest, SuccessfulResultHoldsInflightToBlockImmediateRepick)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());

    // Success no longer clears the in-flight charge (issue #685): the target's snapshot is
    // still stale-low, so the held in-flight makes a re-pick's projected usage (100 + 410 +
    // 410 = 92%) exceed the source threshold and the reject guard blocks it.
    master::ReportRebalanceResultRspPb reportRsp;
    auto successReq = MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED);
    DS_ASSERT_OK(scheduler.ReportResult(successReq, reportRsp));

    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    EXPECT_TRUE(secondRsp.rebalance_task().task_id().empty());
}

// ============================================================================
// Issue #685: Memory Rebalance target over-receive + oscillation.
// https://gitcode.com/openeuler/yuanrong-datasystem/issues/685
//
// Root cause (verified against logs685 + source): the in-flight accounting
// (targetInflightBytes_, since the feature's first commit 6df8eb07) EXISTS but
// is INSUFFICIENT to prevent two sources converging on the same target:
//   1. it only REDUCES the second budget (min, not reject) -- a target with
//      headroom is still selected even when its projected post-migration usage
//      would cross the 70% source threshold (logs685 W2: master projected
//      .103 "37% -> 85%" and still dispatched -- the projection was sort-only,
//      there was NO reject-on-overflow guard);
//   2. the availableMemory used for the reduction is the STALE snapshot value
//      (worker report every 30s + master swap every 10s => up to ~40s lag, so
//      stale-low usedMemory => stale-high available => budget stays > 0);
//   3. RemoveTaskLocked cleared targetInflightBytes_ at completion BEFORE the
//      target's snapshot reflected the received bytes, so the just-received
//      target was re-pickable in the next window.
//
// Fix (this PR) = A + C (no fixed cooldown):
//   A. Reject guard in CollectCandidatePairsLocked -- skip a target whose projected
//      post-migration usage >= source threshold (don't create a future source).
//   C. Hold the in-flight charge past success until the target reports its real
//      post-receive memory (ReleaseReporterHoldsLocked on the target's own report +
//      ReleaseSnapshotHoldsLocked as a swap-lagged backup). Self-timing (<= one
//      30s report cycle), no blind timeout. This keeps the projection high so A
//      also blocks the sequential H1 re-pick (not just the concurrent H3 case).
// H2 (migrated_bytes payload-vs-salcx) is the separate #1346 fix; the reject guard's
// projection is salcx-accurate only with #1346 -- without it a ~6% mid-band still leaks.
// ============================================================================

// C+A (sequential, H1): after S1->T1 succeeds, the in-flight charge is held. S2
// reports while T1's snapshot is still stale-low; the held in-flight makes S2's
// projected post-migration usage for T1 (100 + 400 + 400 = 90%) exceed the 70%
// source threshold, so A rejects T1 and S2 is redirected to T2.
TEST_F(MemoryRebalanceSchedulerTest, SuccessHoldsInflightToPreventRepickToJustReceivedTarget)
{
    const std::string source1 = "127.0.0.1:9301";
    const std::string source2 = "127.0.0.1:8301";
    const std::string target1 = "127.0.0.1:1301";
    const std::string target2 = "127.0.0.1:2301";
    MemoryRebalanceScheduler scheduler;

    auto firstSnapshot = MakeSnapshot({
        MakeNode(source1, 900, 100),
        MakeNode(target1, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, source1, firstSnapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), target1);
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(
        scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED), reportRsp));

    // T1's snapshot is still stale-low (T1 actually received ~400 bytes). The held
    // in-flight makes S2's projection for T1 exceed 70% -> A rejects -> redirect to T2.
    auto secondSnapshot = MakeSnapshot({
        MakeNode(source1, 500, 500),  // S1 drained in reality
        MakeNode(source2, 900, 100),  // S2 is the reporting source
        MakeNode(target1, 100, 900),  // STALE: still looks 10% / 900 available
        MakeNode(target2, 300, 700),
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, source2, secondSnapshot);

    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().source_worker(), source2);
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), target2);
}

// A (concurrent, H3 / logs685 W2): while S1->T is still active (in-flight[T]=445),
// S2 reports; S2's only viable target is T and its projected post-migration usage for
// T is 92% (>= 70%). A must reject it -- do not dispatch a migration that would itself
// create a future source.
TEST_F(MemoryRebalanceSchedulerTest, ConcurrentSourceRejectsOverflowTarget)
{
    const std::string source1 = "127.0.0.1:9302";
    const std::string source2 = "127.0.0.1:8302";
    const std::string target = "127.0.0.1:1302";
    MemoryRebalanceScheduler scheduler;

    auto snap = MakeSnapshot({
        MakeNode(source1, 990, 10),
        MakeNode(source2, 850, 150),
        MakeNode(target, 100, 900),
    });
    // S1 (99%) -> T (10%): midpoint 445, in-flight[T]=445 (projected T = 54.5% < 70% -> ok).
    auto rsp1 = ScheduleAndGetRsp(scheduler, source1, snap);
    ASSERT_FALSE(rsp1.rebalance_task().task_id().empty());
    ASSERT_EQ(rsp1.rebalance_task().target_worker(), target);
    ASSERT_EQ(rsp1.rebalance_task().max_bytes(), 445ul);

    // S2 (85%) reports while S1 is active. T's projected post-migration usage
    // = 100 + 445(in-flight) + 375(max) = 920 = 92% >= 70% -> A rejects T.
    auto rsp2 = ScheduleAndGetRsp(scheduler, source2, snap);
    EXPECT_TRUE(rsp2.rebalance_task().task_id().empty());
}

// Baseline (passes now): when the snapshot is kept fresh (each schedule reflects the
// actual post-migration usage), no re-pick or oscillation occurs -- the source drops
// below the 70% threshold and no second task is built. Proves staleness (not the
// pair-selection) is the root cause of the #685 oscillation.
TEST_F(MemoryRebalanceSchedulerTest, FreshSnapshotConvergesWithoutRepick)
{
    const std::string source = "127.0.0.1:9304";
    const std::string target = "127.0.0.1:1304";
    MemoryRebalanceScheduler scheduler;

    auto snap1 = MakeSnapshot({ MakeNode(source, 900, 100), MakeNode(target, 100, 900) });
    auto rsp1 = ScheduleAndGetRsp(scheduler, source, snap1);
    ASSERT_FALSE(rsp1.rebalance_task().task_id().empty());
    ASSERT_EQ(rsp1.rebalance_task().max_bytes(), 400ul);
    master::ReportRebalanceResultRspPb rr;
    DS_ASSERT_OK(scheduler.ReportResult(MakeResultReq(rsp1.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED), rr));

    // Fresh snapshot: source actually drained to 50% (< 70% threshold) => not a
    // source candidate => no task. Contrast with the held-inflight tests above where
    // the stale-low target caused a reject / redirect.
    auto snap2 = MakeSnapshot({ MakeNode(source, 500, 500), MakeNode(target, 500, 500) });
    auto rsp2 = ScheduleAndGetRsp(scheduler, source, snap2);
    EXPECT_TRUE(rsp2.rebalance_task().task_id().empty());
}

// M1 (issue #685): the TTL GC reclaims a held in-flight charge when the target never reports
// back (e.g. target died after a success). Without GC the three hold maps would grow without
// bound across worker churn. The TTL is max(node_dead_timeout_s, HOLD_TTL_MIN_S); a held charge
// older than the TTL is DecreaseCounter'd and erased on the next schedule cycle.
TEST_F(MemoryRebalanceSchedulerTest, TTLReleasesHeldInflightWhenTargetNeverReportsBack)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), WORKER_10);

    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(
        scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED), reportRsp));
    // Success holds the in-flight charge on WORKER_10 (no DecreaseCounter on success).
    ASSERT_TRUE(HasInflight(scheduler));
    ASSERT_TRUE(HasPendingRelease(scheduler));
    ASSERT_TRUE(HasHold(scheduler));

    // Simulate the target dying before its next report: backdate the hold so it is older than the
    // TTL (max(node_dead_timeout_s, HOLD_TTL_MIN_S)). Real nowMs is far greater than
    // holdSinceMs(1) + TTL, so the next schedule's ExpireTimeoutTasksLocked GC fires.
    BackdateHold(scheduler, WORKER_10, GetSteadyClockTimeStampMs() - 70000);

    auto secondSnapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, secondSnapshot);
    // TTL GC released the held charge: pendingRelease + holdSinceMs are gone. We do NOT assert
    // targetInflightBytes_ empty here -- the second dispatch (below) just added the new task's
    // charge back into it. The GC itself is proven by pendingRelease/holdSinceMs being empty and
    // by the second task being assigned instead of A-rejected (projected 51% < 70%).
    EXPECT_FALSE(HasPendingRelease(scheduler));
    EXPECT_FALSE(HasHold(scheduler));
    // After GC the target's in-flight is 0, so it is selectable again (projected 51% < 70%).
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_10);
}

// M2 (issue #685): ReleaseSnapshotHoldsLocked is the swap-lagged backup release path. When a
// non-reporting target's snapshot timestamp advances past its held completion time (the master
// swapped in a newer snapshot), the held charge is released even though that target did not just
// report. The TTL must NOT fire here -- the schedule happens right after completion, so
// nowMs - holdSinceMs is well below the TTL.
TEST_F(MemoryRebalanceSchedulerTest, SnapshotTimestampAdvanceReleasesHeldInflight)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), WORKER_10);
    master::ReportRebalanceResultRspPb reportRsp;
    DS_ASSERT_OK(
        scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED), reportRsp));
    ASSERT_TRUE(HasPendingRelease(scheduler));

    // The target's snapshot timestamp advances past the held completion time, proving its snapshot
    // now reflects post-receive memory. ReleaseSnapshotHoldsLocked (called at the start of
    // Schedule) releases the hold. The TTL must NOT fire here.
    const uint64_t holdTs = GetHoldTs(scheduler, WORKER_10);
    NodeInfo target(WORKER_10, 900, true, holdTs + 1, 100, MEMORY_CAPACITY, MEMORY_CAPACITY);
    auto advancedSnapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        target,
    });
    auto secondRsp = ScheduleAndGetRsp(scheduler, WORKER_92, advancedSnapshot);
    // Snapshot-based release cleared the held charge: pendingRelease + holdSinceMs are gone.
    // (targetInflightBytes_ is not empty -- the second dispatch just added its charge back; the
    // release itself is proven by pendingRelease/holdSinceMs being empty and by the second task
    // being assigned instead of A-rejected, projected 51% < 70%.)
    EXPECT_FALSE(HasPendingRelease(scheduler));
    EXPECT_FALSE(HasHold(scheduler));
    // After release the target is selectable again (projected 51% < 70%).
    ASSERT_FALSE(secondRsp.rebalance_task().task_id().empty());
    EXPECT_EQ(secondRsp.rebalance_task().target_worker(), WORKER_10);
}

// M3 (issue #685, reporter path): the primary release path -- the target reports its own resource,
// its snapshot timestamp advances past the held completion, and ReleaseReporterHoldsLocked (called
// from NeedSnapshotForSchedule) drops the charge. This is the ~30s happy path every held target
// follows; M2 covered only the swap-lagged backup.
TEST_F(MemoryRebalanceSchedulerTest, ReporterReportReleasesHeldInflight)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), WORKER_10);
    master::ReportRebalanceResultRspPb rr;
    DS_ASSERT_OK(
        scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_SUCCEEDED), rr));
    ASSERT_TRUE(HasPendingRelease(scheduler));
    ASSERT_TRUE(HasHold(scheduler));

    // WORKER_10 reports itself: its NodeInfo timestamp advances past the held completion time, so
    // NeedSnapshotForSchedule -> ReleaseReporterHoldsLocked releases the charge.
    const uint64_t holdTs = GetHoldTs(scheduler, WORKER_10);
    NodeInfo targetReport(WORKER_10, 900, true, holdTs + 1, 100, MEMORY_CAPACITY, MEMORY_CAPACITY);
    master::ResourceReportRspPb rsp;
    auto req = MakeResourceReq(WORKER_10);
    (void)scheduler.NeedSnapshotForSchedule(req, targetReport, rsp);
    EXPECT_FALSE(HasPendingRelease(scheduler));
    EXPECT_FALSE(HasHold(scheduler));
}

// M4 (issue #685, failure path): a FAILED/EXPIRED result must DecreaseCounter + cooldown and must
// NOT create a pendingRelease/holdSinceMs entry (data never landed on the target, so the in-flight
// charge is bogus). Guards against a regression that moves the hold into the failure branch.
TEST_F(MemoryRebalanceSchedulerTest, FailureDoesNotHoldInflightCharge)
{
    MemoryRebalanceScheduler scheduler;
    auto snapshot = MakeSnapshot({
        MakeNode(WORKER_92, 920, 80),
        MakeNode(WORKER_10, 100, 900),
    });
    auto firstRsp = ScheduleAndGetRsp(scheduler, WORKER_92, snapshot);
    ASSERT_FALSE(firstRsp.rebalance_task().task_id().empty());
    ASSERT_EQ(firstRsp.rebalance_task().target_worker(), WORKER_10);
    master::ReportRebalanceResultRspPb rr;
    DS_ASSERT_OK(scheduler.ReportResult(MakeResultReq(firstRsp.rebalance_task(), master::REBALANCE_TASK_FAILED), rr));
    EXPECT_FALSE(HasPendingRelease(scheduler));
    EXPECT_FALSE(HasHold(scheduler));
    // Failure DecreaseCounter'd the in-flight charge, so the dispatch-time charge is gone.
    EXPECT_FALSE(HasInflight(scheduler));
}

}  // namespace ut
}  // namespace datasystem
